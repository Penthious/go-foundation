package pubsub_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/inbox"
	"github.com/Penthious/go-foundation/logger"
	"github.com/Penthious/go-foundation/pubsub"
)

func TestFailedStore_Validation(t *testing.T) {
	for _, cfg := range []pubsub.FailedStoreConfig{
		{Timeout: -time.Second}, {ReplayTimeout: -time.Second},
		{ReplayTimeout: time.Nanosecond}, {ReplayTimeout: time.Duration(1<<63 - 1)},
	} {
		if store, err := pubsub.NewFailedStore(&sql.DB{}, cfg); err == nil || store != nil {
			t.Fatal("invalid store config accepted")
		}
	}

	if _, err := pubsub.NewFailedStore(nil, pubsub.FailedStoreConfig{}); err == nil {
		t.Fatal("nil database accepted")
	}

	store, err := pubsub.NewFailedStore(&sql.DB{}, pubsub.FailedStoreConfig{})
	if err != nil {
		t.Fatal(err)
	}

	for _, filter := range []pubsub.FailedFilter{
		{Topic: "bad.*", Group: "billing"}, {Topic: "orders.created", Group: ""},
		{Topic: "orders.created", Group: "billing", State: "bad"},
		{Topic: "orders.created", Group: "billing", Limit: -1},
		{Topic: "orders.created", Group: "billing", Limit: 501},
		{Topic: "orders.created", Group: "billing", After: "invalid"},
	} {
		if _, err := store.ListFailed(t.Context(), filter); err == nil {
			t.Fatal("invalid failure filter accepted")
		}
	}

	if _, err := store.GetFailed(t.Context(), "invalid"); err == nil {
		t.Fatal("invalid failure ID accepted")
	}
}

func TestFailedStore_LimitsInspectionAndTargetedReplay(t *testing.T) {
	db, store, _ := failedDB(t)
	broker := startBroker(t, nil)
	client := startClient(t, pubsub.Config{Endpoints: []string{broker.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}})
	js := brokerAPI(t, broker)
	cfg := pubsub.ConsumerConfig{Topic: "orders.created", Group: "billing", MaxDeliveries: 2, Failures: store, RetryBackoff: []time.Duration{10 * time.Millisecond}}
	var calls atomic.Int32
	stop := runConsumer(t, client, cfg, func(context.Context, pubsub.Message) error {
		calls.Add(1)
		return errors.New("dependency unavailable")
	})
	var otherCalls atomic.Int32
	stopOther := runConsumer(t, client, pubsub.ConsumerConfig{Topic: cfg.Topic, Group: "shipping"}, func(context.Context, pubsub.Message) error {
		otherCalls.Add(1)
		return nil
	})
	if err := client.Publish(t.Context(), pubsub.Message{ID: "event-1", Topic: cfg.Topic, Data: []byte{0, 1, 255}}); err != nil {
		t.Fatal(err)
	}

	waitAcknowledged(t, js, cfg.Topic, cfg.Group, 1)
	waitAcknowledged(t, js, cfg.Topic, "shipping", 1)
	stop()
	stopOther()
	if calls.Load() != 2 {
		t.Fatalf("handler calls = %d, want delivery limit 2", calls.Load())
	}

	page, err := store.ListFailed(t.Context(), pubsub.FailedFilter{Topic: cfg.Topic, Group: cfg.Group, Limit: 1})
	if err != nil || len(page) != 1 {
		t.Fatalf("failure page = %v, %v", page, err)
	}

	record := page[0]
	if record.Message.ID != "event-1" || record.Group != cfg.Group || record.Reason != "dependency unavailable" || record.Deliveries != 2 || record.State != pubsub.FailureStored || len(record.Raw) == 0 {
		t.Fatalf("failure metadata = %+v", record)
	}

	page, err = store.ListFailed(t.Context(), pubsub.FailedFilter{Topic: cfg.Topic, Group: cfg.Group, After: record.ID})
	if err != nil || len(page) != 0 {
		t.Fatalf("pagination = %v, %v", page, err)
	}

	if err := store.DeleteFailed(t.Context(), record.ID); err == nil {
		t.Fatal("unprocessed failed record was deleted")
	}

	for range 2 {
		if err := store.ReplayFailed(t.Context(), record.ID); err != nil {
			t.Fatal(err)
		}
	}

	restarted, err := pubsub.NewFailedStore(db, pubsub.FailedStoreConfig{})
	if err != nil {
		t.Fatal(err)
	}

	cfg.Failures = restarted
	var replayCalls atomic.Int32
	stopReplay := runConsumer(t, client, cfg, func(_ context.Context, message pubsub.Message) error {
		if message.ID != record.Message.ID || message.Topic != cfg.Topic || string(message.Data) != string(record.Message.Data) {
			return errors.New("replay changed the logical event")
		}

		replayCalls.Add(1)
		return nil
	})
	waitFailureState(t, restarted, record.ID, pubsub.FailureReplayed)
	stopReplay()
	if replayCalls.Load() != 1 || otherCalls.Load() != 1 {
		t.Fatal("replay was duplicated or broadcast to another group")
	}

	replayed, err := restarted.GetFailed(t.Context(), record.ID)
	if err != nil || replayed.ReplayCount != 1 || replayed.ReplayAttempts != 1 || replayed.ReplayedAt.IsZero() {
		t.Fatalf("replay metadata = %+v, %v", replayed, err)
	}

	if err := restarted.ReplayFailed(t.Context(), record.ID); !errors.Is(err, pubsub.ErrAlreadyReplayed) {
		t.Fatalf("already-replayed request = %v", err)
	}

	// Re-delivering the retained original must not reset an operator's completed
	// replay, as can happen after ambiguous acknowledgments or broker state repair.
	stream, err := js.StreamNameBySubject(t.Context(), cfg.Topic)
	if err != nil {
		t.Fatal(err)
	}

	if err := js.DeleteConsumer(t.Context(), stream, cfg.Group); err != nil {
		t.Fatal(err)
	}

	stopOldDelivery := runConsumer(t, client, cfg, func(context.Context, pubsub.Message) error { return errors.New("old delivery") })
	waitAcknowledged(t, js, cfg.Topic, cfg.Group, 1)
	stopOldDelivery()
	preserved, err := restarted.GetFailed(t.Context(), record.ID)
	if err != nil || preserved.State != pubsub.FailureReplayed || preserved.ReplayCount != 1 || preserved.Reason != record.Reason {
		t.Fatalf("original redelivery overwrote replay state: %+v, %v", preserved, err)
	}

	if err := restarted.DeleteFailed(t.Context(), record.ID); err != nil {
		t.Fatal(err)
	}

	if _, err := restarted.GetFailed(t.Context(), record.ID); !errors.Is(err, pubsub.ErrFailedNotFound) {
		t.Fatalf("deleted record = %v", err)
	}
}

func TestFailedStore_UnavailableArchiveDoesNotAck(t *testing.T) {
	db, store, _ := failedDB(t)
	broker := startBroker(t, nil)
	observed := &failureLogSignal{match: "quarantine failed", signal: make(chan struct{})}
	log, err := logger.New(logger.Config{Service: "failed-test", Output: observed})
	if err != nil {
		t.Fatal(err)
	}

	client := startClient(t, pubsub.Config{Logger: log, Endpoints: []string{broker.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}})
	js := brokerAPI(t, broker)
	cfg := pubsub.ConsumerConfig{Topic: "orders.created", Group: "billing", MaxDeliveries: 1, Failures: store, RetryDelay: 10 * time.Millisecond}
	entered, proceed := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	stop := runConsumer(t, client, cfg, func(ctx context.Context, _ pubsub.Message) error {
		calls.Add(1)
		close(entered)
		select {
		case <-proceed:
			return errors.New("handler failed")
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err := client.Publish(t.Context(), pubsub.Message{ID: "retained", Topic: cfg.Topic}); err != nil {
		t.Fatal(err)
	}

	receive(t, entered)
	if _, err := db.ExecContext(t.Context(), "ALTER TABLE public.gof_failed_messages RENAME TO unavailable_failures"); err != nil {
		t.Fatal(err)
	}

	close(proceed)
	receive(t, observed.signal)
	stream, err := js.StreamNameBySubject(t.Context(), cfg.Topic)
	if err != nil {
		t.Fatal(err)
	}

	consumer, err := js.Consumer(t.Context(), stream, cfg.Group)
	if err != nil {
		t.Fatal(err)
	}

	info, err := consumer.Info(t.Context())
	if err != nil || info.AckFloor.Stream != 0 {
		t.Fatalf("original was acknowledged without durable storage: %+v, %v", info, err)
	}

	if _, err := db.ExecContext(t.Context(), "ALTER TABLE public.unavailable_failures RENAME TO gof_failed_messages"); err != nil {
		t.Fatal(err)
	}

	waitAcknowledged(t, js, cfg.Topic, cfg.Group, 1)
	stop()
	page, err := store.ListFailed(t.Context(), pubsub.FailedFilter{Topic: cfg.Topic, Group: cfg.Group})
	if err != nil || len(page) != 1 || calls.Load() != 1 {
		t.Fatalf("archive recovery = %+v, %v, handler calls = %d", page, err, calls.Load())
	}
}

func TestFailedStore_MalformedEnvelopePreservesRaw(t *testing.T) {
	_, store, _ := failedDB(t)
	broker := startBroker(t, nil)
	client := startClient(t, pubsub.Config{Endpoints: []string{broker.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}})
	js := brokerAPI(t, broker)
	cfg := pubsub.ConsumerConfig{Topic: "orders.created", Group: "billing", MaxDeliveries: 1, Failures: store}
	var called atomic.Int32
	stop := runConsumer(t, client, cfg, func(context.Context, pubsub.Message) error {
		called.Add(1)
		return nil
	})
	for _, raw := range [][]byte{[]byte("{bad"), []byte(`{"id":"bad\u0000","trace":{"x":"\u0000"}}`), []byte(`{"id":"partial","data":123}`)} {
		if _, err := js.Publish(t.Context(), cfg.Topic, raw); err != nil {
			t.Fatal(err)
		}
	}

	waitAcknowledged(t, js, cfg.Topic, cfg.Group, 3)
	stop()
	page, err := store.ListFailed(t.Context(), pubsub.FailedFilter{Topic: cfg.Topic, Group: cfg.Group})
	if err != nil || len(page) != 3 || called.Load() != 0 {
		t.Fatalf("malformed archive = %+v, %v", page, err)
	}

	for _, record := range page {
		if len(record.Raw) == 0 || record.Message.ID != "" {
			t.Fatal("malformed transport data was discarded or treated as replayable")
		}

		if err := store.ReplayFailed(t.Context(), record.ID); err == nil {
			t.Fatal("malformed envelope was replayed as a different event")
		}
	}
}

func TestFailedStore_TransactionalFailureAndReplay(t *testing.T) {
	db, failures, store := failedDB(t)
	broker := startBroker(t, nil)
	client := startClient(t, pubsub.Config{Endpoints: []string{broker.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}})
	cfg := pubsub.ConsumerConfig{Topic: "orders.created", Group: "billing", MaxDeliveries: 1, Failures: failures}
	stop := runTransactionalConsumer(t, client, cfg, store, func(ctx context.Context, tx *sql.Tx, message pubsub.Message) error {
		if _, err := tx.ExecContext(ctx, "INSERT INTO effects VALUES ($1, $2, $3)", cfg.Group, cfg.Topic, message.ID); err != nil {
			return err
		}

		return errors.New("business failure")
	})
	if err := client.Publish(t.Context(), pubsub.Message{ID: "transactional", Topic: cfg.Topic}); err != nil {
		t.Fatal(err)
	}

	waitAcknowledged(t, brokerAPI(t, broker), cfg.Topic, cfg.Group, 1)
	stop()
	if inboxCount(t, db, "effects") != 0 || inboxCount(t, db, "public.gof_inbox") != 0 {
		t.Fatal("failed transaction was committed before quarantine")
	}

	page, err := failures.ListFailed(t.Context(), pubsub.FailedFilter{Topic: cfg.Topic, Group: cfg.Group})
	if err != nil || len(page) != 1 {
		t.Fatalf("transactional failure = %+v, %v", page, err)
	}

	if err := failures.ReplayFailed(t.Context(), page[0].ID); err != nil {
		t.Fatal(err)
	}

	stopReplay := runTransactionalConsumer(t, client, cfg, store, func(ctx context.Context, tx *sql.Tx, message pubsub.Message) error {
		_, err := tx.ExecContext(ctx, "INSERT INTO effects VALUES ($1, $2, $3)", cfg.Group, cfg.Topic, message.ID)
		return err
	})
	waitFailureState(t, failures, page[0].ID, pubsub.FailureReplayed)
	stopReplay()
	if inboxCount(t, db, "effects") != 1 || inboxCount(t, db, "public.gof_inbox") != 1 {
		t.Fatal("transactional replay did not commit exactly one business result")
	}
}

func failedDB(t *testing.T) (*sql.DB, *pubsub.FailedStore, *inbox.Store) {
	t.Helper()

	db, store := inboxDB(t)
	if _, err := db.ExecContext(t.Context(), pubsub.FailedSchema()); err != nil {
		t.Fatal(err)
	}

	failures, err := pubsub.NewFailedStore(db, pubsub.FailedStoreConfig{Timeout: time.Second, ReplayTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}

	return db, failures, store
}

func waitFailureState(t *testing.T, store *pubsub.FailedStore, id string, state pubsub.FailureState) {
	t.Helper()

	eventually(t, func() bool {
		record, err := store.GetFailed(t.Context(), id)
		return err == nil && record.State == state
	})
}

type failureLogSignal struct {
	match  string
	signal chan struct{}
	once   sync.Once
}

func (w *failureLogSignal) Write(data []byte) (int, error) {
	if strings.Contains(string(data), w.match) {
		w.once.Do(func() { close(w.signal) })
	}

	return len(data), nil
}

var _ io.Writer = (*failureLogSignal)(nil)

func TestFailedStore_MissingSchema(t *testing.T) {
	db, _, _ := failedDB(t)
	if _, err := db.ExecContext(t.Context(), "DROP TABLE public.gof_failed_messages"); err != nil {
		t.Fatal(err)
	}

	store, err := pubsub.NewFailedStore(db, pubsub.FailedStoreConfig{})
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Ping(t.Context()); err == nil {
		t.Fatal("missing failed-message schema passed readiness")
	}

	broker := startBroker(t, nil)
	client := startClient(t, pubsub.Config{Endpoints: []string{broker.ClientURL()}})
	err = client.Consume(t.Context(), pubsub.ConsumerConfig{Topic: "orders.created", Group: "billing", MaxDeliveries: 1, Failures: store}, func(context.Context, pubsub.Message) error { return nil })
	if err == nil || !strings.Contains(fmt.Sprint(err), "schema") {
		t.Fatalf("consumption started without failed storage: %v", err)
	}
}
