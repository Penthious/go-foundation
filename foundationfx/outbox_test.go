package foundationfx_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"io"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/foundationfx"
	"github.com/Penthious/go-foundation/health"
	"github.com/Penthious/go-foundation/logger"
	"github.com/Penthious/go-foundation/outbox"
	"github.com/Penthious/go-foundation/postgres"
	"github.com/Penthious/go-foundation/pubsub"

	"go.uber.org/fx"
)

func TestOutboxModule_InvalidConfig(t *testing.T) {
	db, err := postgres.New(postgres.Config{DSN: "postgres://localhost/unused"})
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()
	app := fx.New(
		foundationfx.Module, foundationfx.OutboxModule, fx.NopLogger,
		fx.Supply(db, logger.Config{Service: "outbox-test", Output: io.Discard}),
		fx.Supply(outbox.Config{PollInterval: -time.Second}),
	)

	if app.Err() == nil || db.Stats().OpenConnections != 0 {
		t.Fatal("invalid outbox config accepted or construction opened connections")
	}
}

func TestOutboxModule_LifecycleAndHealth(t *testing.T) {
	dsn := outboxDatabase(t, true)
	broker := startPubSubBroker(t)
	var db *sql.DB
	var bus *pubsub.Client
	var checker *health.Checker
	app := fx.New(
		foundationfx.Module, foundationfx.PostgresModule(), foundationfx.OutboxModule, fx.NopLogger,
		fx.Supply(logger.Config{Service: "outbox-test", Output: io.Discard}),
		fx.Supply(postgres.Config{DSN: dsn}),
		fx.Supply(pubsub.Config{Endpoints: []string{broker.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}}),
		fx.Supply(outbox.Config{PollInterval: 5 * time.Millisecond}),
		fx.Provide(func(relay *outbox.Relay) (*health.Checker, error) {
			return health.New(health.Required("outbox", relay.Ping))
		}),
		fx.Populate(&db, &bus, &checker),
	)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}

	if db.Stats().OpenConnections != 0 || broker.NumClients() != 0 {
		t.Fatal("constructing the graph opened connections")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if err := app.Start(ctx); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := app.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	if !checker.Readiness(ctx).Ready {
		t.Fatal("started outbox failed its health check")
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer tx.Rollback()
	if err := outbox.Enqueue(ctx, tx, pubsub.Message{ID: "fx-event", Topic: "orders.created", Data: []byte("order")}); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		var pending int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM public.gof_outbox").Scan(&pending); err != nil {
			t.Fatal(err)
		}

		if pending == 0 {
			break
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("Fx relay did not deliver the queued event")
		}
	}

	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	assertClosedDB(t, db)
	if err := bus.Ping(ctx); !errors.Is(err, pubsub.ErrClosed) {
		t.Fatalf("pubsub did not stop after the relay: %v", err)
	}
}

func TestOutboxModule_StartupFailureAndRollback(t *testing.T) {
	for _, missingSchema := range []bool{true, false} {
		t.Run(map[bool]string{true: "missing schema", false: "later failure"}[missingSchema], func(t *testing.T) {
			dsn := outboxDatabase(t, !missingSchema)
			broker := startPubSubBroker(t)
			var db *sql.DB
			var bus *pubsub.Client
			opts := []fx.Option{
				foundationfx.Module, foundationfx.PostgresModule(), foundationfx.OutboxModule, fx.NopLogger,
				fx.Supply(logger.Config{Service: "outbox-test", Output: io.Discard}),
				fx.Supply(postgres.Config{DSN: dsn}),
				fx.Supply(pubsub.Config{Endpoints: []string{broker.ClientURL()}}),
				fx.Populate(&db, &bus),
			}
			if !missingSchema {
				opts = append(opts, fx.Invoke(func(lifecycle fx.Lifecycle, _ *outbox.Relay) {
					lifecycle.Append(fx.Hook{OnStart: func(context.Context) error {
						return errors.New("later startup failed")
					}})
				}))
			}

			app := fx.New(opts...)
			if err := app.Err(); err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			if err := app.Start(ctx); err == nil {
				t.Fatal("expected startup failure")
			}

			assertClosedDB(t, db)
			if err := bus.Ping(ctx); !errors.Is(err, pubsub.ErrClosed) {
				t.Fatalf("startup rollback did not close pubsub: %v", err)
			}
		})
	}
}

func outboxDatabase(t *testing.T, migrate bool) string {
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
	name := "gof_outbox_fx_" + strings.ToLower(rand.Text())
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
	if migrate {
		db, err := postgres.New(postgres.Config{DSN: u.String()})
		if err != nil {
			t.Fatal(err)
		}

		defer db.Close()
		if _, err := db.ExecContext(ctx, outbox.Schema()); err != nil {
			t.Fatal(err)
		}
	}

	return u.String()
}
