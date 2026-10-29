package pubsub_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"io"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/inbox"
	"github.com/Penthious/go-foundation/postgres"
	"github.com/Penthious/go-foundation/pubsub"
)

func TestConsumeTransactional_Validation(t *testing.T) {
	client, err := pubsub.New(pubsub.Config{Logger: newLogger(t, io.Discard)})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = client.Shutdown(context.Background()) })
	store, err := inbox.New(&sql.DB{}, inbox.Config{})
	if err != nil {
		t.Fatal(err)
	}

	cfg := pubsub.ConsumerConfig{Topic: "orders.created", Group: "billing"}
	handler := func(context.Context, *sql.Tx, pubsub.Message) error { return nil }
	if err := client.ConsumeTransactional(t.Context(), cfg, nil, handler); err == nil {
		t.Fatal("nil inbox accepted")
	}

	if err := client.ConsumeTransactional(t.Context(), cfg, store, nil); err == nil {
		t.Fatal("nil handler accepted")
	}

	if err := client.ConsumeTransactional(t.Context(), pubsub.ConsumerConfig{}, store, handler); err == nil || errors.Is(err, pubsub.ErrNotStarted) {
		t.Fatalf("consumer validation was bypassed: %v", err)
	}

	if err := client.ConsumeTransactional(t.Context(), cfg, store, handler); !errors.Is(err, pubsub.ErrNotStarted) {
		t.Fatalf("client lifecycle was bypassed: %v", err)
	}
}

func TestConsumeTransactional_CommitBeforeAckAndReplay(t *testing.T) {
	db, store := inboxDB(t)
	broker := startBroker(t, nil)
	client := startClient(t, pubsub.Config{Endpoints: []string{broker.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}})
	js := brokerAPI(t, broker)
	cfg := pubsub.ConsumerConfig{Topic: "orders.created", Group: "billing", AckTimeout: 300 * time.Millisecond}
	entered := make(chan struct{}, 1)
	proceed := make(chan struct{})
	var calls atomic.Int32
	handler := func(ctx context.Context, tx *sql.Tx, message pubsub.Message) error {
		calls.Add(1)
		if _, err := tx.ExecContext(ctx, "INSERT INTO effects VALUES ($1, $2, $3)", cfg.Group, message.Topic, message.ID); err != nil {
			return err
		}

		entered <- struct{}{}
		select {
		case <-proceed:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	stop := runTransactionalConsumer(t, client, cfg, store, handler)
	if err := client.Publish(t.Context(), pubsub.Message{ID: "event-1", Topic: cfg.Topic}); err != nil {
		t.Fatal(err)
	}

	receive(t, entered)
	if inboxCount(t, db, "effects") != 0 || inboxCount(t, db, "public.gof_inbox") != 0 {
		t.Fatal("transactional changes became visible before commit")
	}

	stream, err := js.StreamNameBySubject(t.Context(), cfg.Topic)
	if err != nil {
		t.Fatal(err)
	}

	consumer, err := js.Consumer(t.Context(), stream, cfg.Group)
	if err != nil {
		t.Fatal(err)
	}

	info, err := consumer.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if info.AckFloor.Stream != 0 || info.NumAckPending != 1 {
		t.Fatal("broker acknowledged work before the database transaction committed")
	}

	close(proceed)
	waitAcknowledged(t, js, cfg.Topic, cfg.Group, 1)
	stop()
	if inboxCount(t, db, "effects") != 1 || inboxCount(t, db, "public.gof_inbox") != 1 {
		t.Fatal("commit did not persist business effects and marker together")
	}

	// Recreate the durable consumer to replay retained messages as after lost
	// broker state. A new inbox instance must still skip the committed event.
	if err := js.DeleteConsumer(t.Context(), stream, cfg.Group); err != nil {
		t.Fatal(err)
	}

	restarted, err := inbox.New(db, inbox.Config{})
	if err != nil {
		t.Fatal(err)
	}

	stopReplay := runTransactionalConsumer(t, client, cfg, restarted, handler)
	waitAcknowledged(t, js, cfg.Topic, cfg.Group, 1)
	stopReplay()
	if calls.Load() != 1 || inboxCount(t, db, "effects") != 1 {
		t.Fatal("broker replay repeated committed business changes")
	}
}

func TestConsumeTransactional_RetryRollsBack(t *testing.T) {
	for _, mode := range []string{"error", "panic"} {
		t.Run(mode, func(t *testing.T) {
			db, store := inboxDB(t)
			broker := startBroker(t, nil)
			client := startClient(t, pubsub.Config{Endpoints: []string{broker.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}})
			cfg := pubsub.ConsumerConfig{Topic: "orders.created", Group: "billing", RetryDelay: 10 * time.Millisecond}
			var calls atomic.Int32
			stop := runTransactionalConsumer(t, client, cfg, store, func(ctx context.Context, tx *sql.Tx, message pubsub.Message) error {
				if _, err := tx.ExecContext(ctx, "INSERT INTO effects VALUES ($1, $2, $3)", cfg.Group, message.Topic, message.ID); err != nil {
					return err
				}

				if calls.Add(1) == 1 {
					if mode == "panic" {
						panic("handler failed")
					}

					return errors.New("handler failed")
				}

				return nil
			})
			if err := client.Publish(t.Context(), pubsub.Message{ID: "retry-event", Topic: cfg.Topic}); err != nil {
				t.Fatal(err)
			}

			waitAcknowledged(t, brokerAPI(t, broker), cfg.Topic, cfg.Group, 1)
			stop()
			if calls.Load() != 2 || inboxCount(t, db, "effects") != 1 || inboxCount(t, db, "public.gof_inbox") != 1 {
				t.Fatal("redelivery did not roll back the failed attempt")
			}
		})
	}
}

func TestConsumeTransactional_ConsumerAndTopicScopes(t *testing.T) {
	db, store := inboxDB(t)
	broker := startBroker(t, nil)
	client := startClient(t, pubsub.Config{Endpoints: []string{broker.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}, {Name: "orders.updated"}}})
	js := brokerAPI(t, broker)
	for _, topic := range []string{"orders.created", "orders.updated"} {
		if err := client.Publish(t.Context(), pubsub.Message{ID: "same-id", Topic: topic}); err != nil {
			t.Fatal(err)
		}
	}

	for _, cfg := range []pubsub.ConsumerConfig{
		{Topic: "orders.created", Group: "billing"},
		{Topic: "orders.created", Group: "shipping"},
		{Topic: "orders.updated", Group: "billing"},
	} {
		stop := runTransactionalConsumer(t, client, cfg, store, func(ctx context.Context, tx *sql.Tx, message pubsub.Message) error {
			_, err := tx.ExecContext(ctx, "INSERT INTO effects VALUES ($1, $2, $3)", cfg.Group, message.Topic, message.ID)
			return err
		})
		waitAcknowledged(t, js, cfg.Topic, cfg.Group, 1)
		stop()
	}

	if inboxCount(t, db, "effects") != 3 || inboxCount(t, db, "public.gof_inbox") != 3 {
		t.Fatal("independent groups or topics shared an inbox namespace")
	}
}

func runTransactionalConsumer(t *testing.T, client *pubsub.Client, cfg pubsub.ConsumerConfig, store *inbox.Store, handler pubsub.TransactionalHandler) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- client.ConsumeTransactional(ctx, cfg, store, handler) }()

	stopped := false
	stop := func() {
		if stopped {
			return
		}

		stopped = true
		cancel()
		if err := receive(t, done); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, pubsub.ErrClosed) {
			t.Error(err)
		}
	}
	t.Cleanup(stop)

	return stop
}

func inboxDB(t *testing.T) (*sql.DB, *inbox.Store) {
	t.Helper()

	dsn := os.Getenv("GO_FOUNDATION_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set GO_FOUNDATION_POSTGRES_TEST_DSN to a PostgreSQL URL with CREATEDB permission")
	}

	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Fatal("integration DSN must be a PostgreSQL URL")
	}

	admin, err := postgres.New(postgres.Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = admin.Close() })
	name := "gof_inbox_pubsub_" + strings.ToLower(rand.Text())
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if _, err := admin.ExecContext(ctx, "DROP DATABASE "+name+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
	})
	u.Path, u.RawPath = "/"+name, ""
	db, err := postgres.New(postgres.Config{DSN: u.String()})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, inbox.Schema()); err != nil {
		t.Fatal(err)
	}

	if _, err := db.ExecContext(ctx, "CREATE TABLE effects (group_name text, topic text, message_id text)"); err != nil {
		t.Fatal(err)
	}

	store, err := inbox.New(db, inbox.Config{})
	if err != nil {
		t.Fatal(err)
	}

	return db, store
}

func inboxCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()

	var count int
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM "+table).Scan(&count); err != nil {
		t.Fatal(err)
	}

	return count
}
