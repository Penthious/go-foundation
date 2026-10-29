package inbox_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/inbox"
	"github.com/Penthious/go-foundation/outbox"
	"github.com/Penthious/go-foundation/postgres"
	"github.com/Penthious/go-foundation/pubsub"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestNew_ValidationAndLazyConstruction(t *testing.T) {
	for _, tt := range []struct {
		name string
		db   *sql.DB
		cfg  inbox.Config
	}{
		{name: "nil database"},
		{name: "negative timeout", db: &sql.DB{}, cfg: inbox.Config{Timeout: -time.Second}},
		{name: "unsupported isolation", db: &sql.DB{}, cfg: inbox.Config{Isolation: sql.LevelLinearizable}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if store, err := inbox.New(tt.db, tt.cfg); store != nil || err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}

	db, err := postgres.New(postgres.Config{DSN: "postgres://localhost/unused"})
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()
	if _, err := inbox.New(db, inbox.Config{}); err != nil {
		t.Fatal(err)
	}

	if db.Stats().OpenConnections != 0 {
		t.Fatal("construction opened database connections")
	}
}

func TestStore_ProcessValidation(t *testing.T) {
	store, err := inbox.New(&sql.DB{}, inbox.Config{})
	if err != nil {
		t.Fatal(err)
	}

	for _, key := range []inbox.Key{
		{MessageID: "event"},
		{Consumer: "  ", MessageID: "event"},
		{Consumer: "billing"},
		{Consumer: strings.Repeat("c", 513), MessageID: "event"},
		{Consumer: "billing", MessageID: strings.Repeat("e", 257)},
		{Consumer: "billing", MessageID: "secret\x00id"},
		{Consumer: "billing\n", MessageID: "event"},
		{Consumer: "billing", MessageID: string([]byte{255})},
	} {
		if processed, err := store.Process(t.Context(), key, func(context.Context, *sql.Tx) error { return nil }); processed || err == nil {
			t.Fatal("invalid key accepted")
		} else if strings.Contains(err.Error(), "secret") {
			t.Fatal("validation error disclosed the key")
		}
	}

	if processed, err := store.Process(t.Context(), inbox.Key{Consumer: "billing", MessageID: "event"}, nil); processed || err == nil {
		t.Fatal("nil handler accepted")
	}
}

func TestStore_ProcessDeduplicationAndAtomicOutbox(t *testing.T) {
	db := testDB(t, true)
	if _, err := db.ExecContext(t.Context(), outbox.Schema()); err != nil {
		t.Fatal(err)
	}

	store := testStore(t, db, inbox.Config{})
	key := inbox.Key{Consumer: "billing:orders.created", MessageID: "order-1"}
	called := 0
	handler := func(ctx context.Context, tx *sql.Tx) error {
		called++
		if _, err := tx.ExecContext(ctx, "INSERT INTO effects VALUES (1)"); err != nil {
			return err
		}

		return outbox.Enqueue(ctx, tx, pubsub.Message{ID: "invoice-1", Topic: "invoices.created"})
	}
	if processed, err := store.Process(t.Context(), key, handler); !processed || err != nil {
		t.Fatalf("first processing = %v, %v", processed, err)
	}

	// A fresh store has no local memory of the earlier delivery.
	restarted := testStore(t, db, inbox.Config{})
	if processed, err := restarted.Process(t.Context(), key, handler); processed || err != nil {
		t.Fatalf("committed duplicate = %v, %v", processed, err)
	}

	if called != 1 || count(t, db, "effects") != 1 || count(t, db, "public.gof_inbox") != 1 || count(t, db, "public.gof_outbox") != 1 {
		t.Fatal("duplicate repeated business changes or outgoing events")
	}

	for _, consumer := range []string{"shipping:orders.created", "Billing:orders.created"} {
		if processed, err := store.Process(t.Context(), inbox.Key{Consumer: consumer, MessageID: key.MessageID}, func(context.Context, *sql.Tx) error { return nil }); !processed || err != nil {
			t.Fatalf("independent consumer = %v, %v", processed, err)
		}
	}

	if count(t, db, "public.gof_inbox") != 3 {
		t.Fatal("consumer namespaces were not independent")
	}
}

func TestStore_ProcessRollback(t *testing.T) {
	for _, mode := range []string{"error", "panic", "nil panic", "cancel", "timeout", "closed transaction"} {
		t.Run(mode, func(t *testing.T) {
			db := testDB(t, true)
			if _, err := db.ExecContext(t.Context(), outbox.Schema()); err != nil {
				t.Fatal(err)
			}

			store := testStore(t, db, inbox.Config{Timeout: time.Second})
			key := inbox.Key{Consumer: "billing", MessageID: "retry-event"}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			failure := errors.New("business operation failed")
			processed, err := store.Process(ctx, key, func(ctx context.Context, tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx, "INSERT INTO effects VALUES (1)"); err != nil {
					return err
				}

				if err := outbox.Enqueue(ctx, tx, pubsub.Message{ID: "outgoing", Topic: "invoices.created"}); err != nil {
					return err
				}

				switch mode {
				case "error":
					return failure
				case "panic":
					panic("sensitive panic value")
				case "nil panic":
					panic(nil)
				case "cancel":
					cancel()
				case "timeout":
					<-ctx.Done()
					return ctx.Err()
				case "closed transaction":
					return tx.Rollback()
				}

				return nil
			})
			if processed || err == nil {
				t.Fatal("failed handler was committed")
			}

			if mode == "error" && !errors.Is(err, failure) {
				t.Fatal("handler error cause was lost")
			}

			if strings.Contains(err.Error(), "sensitive panic value") {
				t.Fatal("panic value escaped into the error")
			}

			if count(t, db, "effects") != 0 || count(t, db, "public.gof_inbox") != 0 || count(t, db, "public.gof_outbox") != 0 {
				t.Fatal("failure left business writes, inbox marker, or outbox event committed")
			}

			if processed, err := store.Process(t.Context(), key, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, "INSERT INTO effects VALUES (2)")
				return err
			}); !processed || err != nil {
				t.Fatalf("rolled-back key could not be retried: %v, %v", processed, err)
			}
		})
	}
}

func TestStore_ProcessConcurrentDuplicates(t *testing.T) {
	db := testDB(t, true)
	store := testStore(t, db, inbox.Config{})
	key := inbox.Key{Consumer: "billing", MessageID: "shared"}
	entered := make(chan struct{})
	proceed := make(chan struct{})
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	go func() {
		_, err := store.Process(ctx, key, func(ctx context.Context, tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, "INSERT INTO effects VALUES (1)"); err != nil {
				return err
			}

			close(entered)
			select {
			case <-proceed:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		done <- err
	}()

	receive(t, entered)
	if count(t, db, "effects") != 0 || count(t, db, "public.gof_inbox") != 0 {
		t.Fatal("uncommitted processing was visible")
	}

	waitCtx, stopWaiting := context.WithTimeout(ctx, 100*time.Millisecond)
	defer stopWaiting()

	secondCalled := false
	if processed, err := store.Process(waitCtx, key, func(context.Context, *sql.Tx) error {
		secondCalled = true
		return nil
	}); processed || !errors.Is(err, context.DeadlineExceeded) || secondCalled {
		t.Fatalf("concurrent duplicate did not wait for the first transaction: %v, %v", processed, err)
	}

	close(proceed)
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}

	if processed, err := store.Process(ctx, key, func(context.Context, *sql.Tx) error {
		secondCalled = true
		return nil
	}); processed || err != nil || secondCalled {
		t.Fatalf("committed winner was not deduplicated: %v, %v", processed, err)
	}

	if count(t, db, "effects") != 1 || count(t, db, "public.gof_inbox") != 1 {
		t.Fatal("duplicate changed the committed result")
	}
}

func TestStore_PingRequiresSchema(t *testing.T) {
	db := testDB(t, false)
	store := testStore(t, db, inbox.Config{})
	if err := store.Ping(t.Context()); err == nil {
		t.Fatal("missing schema passed readiness")
	}

	called := false
	if processed, err := store.Process(t.Context(), inbox.Key{Consumer: "billing", MessageID: "event"}, func(context.Context, *sql.Tx) error {
		called = true
		return nil
	}); processed || err == nil || called {
		t.Fatal("missing schema allowed business processing")
	}

	if _, err := db.ExecContext(t.Context(), inbox.Schema()); err != nil {
		t.Fatal(err)
	}

	if err := store.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := store.Ping(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("health check ignored cancellation: %v", err)
	}
}

func TestStore_ProcessTelemetry(t *testing.T) {
	db := testDB(t, true)
	oldTraces, oldMetrics := otel.GetTracerProvider(), otel.GetMeterProvider()
	recorder := tracetest.NewSpanRecorder()
	traces := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	reader := sdkmetric.NewManualReader()
	metrics := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetTracerProvider(traces)
	otel.SetMeterProvider(metrics)
	t.Cleanup(func() {
		otel.SetTracerProvider(oldTraces)
		otel.SetMeterProvider(oldMetrics)
		_ = traces.Shutdown(context.Background())
		_ = metrics.Shutdown(context.Background())
	})

	store := testStore(t, db, inbox.Config{})
	ctx, parent := traces.Tracer("service").Start(t.Context(), "consume event")
	key := inbox.Key{Consumer: "billing", MessageID: "event"}
	handler := func(ctx context.Context, _ *sql.Tx) error {
		if trace.SpanContextFromContext(ctx).TraceID() != parent.SpanContext().TraceID() {
			return errors.New("trace context was lost")
		}

		return nil
	}
	for range 2 {
		if _, err := store.Process(ctx, key, handler); err != nil {
			t.Fatal(err)
		}
	}

	key.MessageID = "failure"
	if _, err := store.Process(ctx, key, func(context.Context, *sql.Tx) error { return errors.New("handler failed") }); err == nil {
		t.Fatal("expected handler failure")
	}

	parent.End()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}

	outcomes := make(map[string]int64)
	for _, scope := range data.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			if measurement.Name == "inbox.messages" {
				for _, point := range measurement.Data.(metricdata.Sum[int64]).DataPoints {
					result, _ := point.Attributes.Value("result")
					outcomes[result.AsString()] += point.Value
				}
			}
		}
	}

	if outcomes["processed"] != 1 || outcomes["duplicate"] != 1 || outcomes["failure"] != 1 {
		t.Fatalf("inbox outcomes = %v", outcomes)
	}

	spans, failed := 0, false
	for _, span := range recorder.Ended() {
		if span.Name() != "inbox process" {
			continue
		}

		spans++
		if span.Parent().SpanID() != parent.SpanContext().SpanID() {
			t.Fatal("inbox span lost its parent")
		}

		failed = failed || span.Status().Code == codes.Error
	}

	if spans != 3 || !failed {
		t.Fatalf("inbox spans = %d, failure recorded = %v", spans, failed)
	}
}

func testDB(t *testing.T, migrate bool) *sql.DB {
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
	name := "gof_inbox_" + strings.ToLower(rand.Text())
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
	if migrate {
		if _, err := db.ExecContext(ctx, inbox.Schema()); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := db.ExecContext(ctx, "CREATE TABLE effects (value integer NOT NULL)"); err != nil {
		t.Fatal(err)
	}

	return db
}

func testStore(t *testing.T, db *sql.DB, cfg inbox.Config) *inbox.Store {
	t.Helper()

	store, err := inbox.New(db, cfg)
	if err != nil {
		t.Fatal(err)
	}

	return store
}

func count(t *testing.T, db *sql.DB, table string) int {
	t.Helper()

	var count int
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM "+table).Scan(&count); err != nil {
		t.Fatal(err)
	}

	return count
}

func receive[T any](t *testing.T, channel <-chan T) T {
	t.Helper()

	select {
	case value := <-channel:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for transaction")
	}

	var zero T
	return zero
}
