package outbox

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/logger"
	"github.com/Penthious/go-foundation/pubsub"

	"github.com/nats-io/nats-server/v2/server"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestNew_Validation(t *testing.T) {
	log := testLogger(t)
	publish := func(context.Context, pubsub.Message) error { return nil }
	for _, tt := range []struct {
		name    string
		db      *sql.DB
		publish func(context.Context, pubsub.Message) error
		cfg     Config
	}{
		{name: "nil database", publish: publish, cfg: Config{Logger: log}},
		{name: "nil publisher", db: &sql.DB{}, cfg: Config{Logger: log}},
		{name: "nil logger", db: &sql.DB{}, publish: publish},
		{name: "negative poll", db: &sql.DB{}, publish: publish, cfg: Config{Logger: log, PollInterval: -time.Second}},
		{name: "negative retry", db: &sql.DB{}, publish: publish, cfg: Config{Logger: log, RetryDelay: -time.Second}},
		{name: "negative timeout", db: &sql.DB{}, publish: publish, cfg: Config{Logger: log, Timeout: -time.Second}},
		{name: "tiny timeout", db: &sql.DB{}, publish: publish, cfg: Config{Logger: log, Timeout: time.Nanosecond}},
		{name: "overflowing lease", db: &sql.DB{}, publish: publish, cfg: Config{Logger: log, Timeout: time.Duration(1<<63 - 1)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if relay, err := New(tt.db, tt.publish, tt.cfg); err == nil || relay != nil {
				t.Fatal("invalid relay configuration accepted")
			}
		})
	}
}

func TestRelay_RetryAndPanicRecovery(t *testing.T) {
	for _, panicPublisher := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "panic"}[panicPublisher], func(t *testing.T) {
			db := testDB(t, true)
			queue(t, db, "retry-event")
			var ids []string
			relay := testRelay(t, db, func(_ context.Context, message pubsub.Message) error {
				ids = append(ids, message.ID)
				if len(ids) == 1 {
					if panicPublisher {
						panic("failed publisher")
					}

					return errors.New("broker unavailable")
				}

				return nil
			})

			if found, err := relay.dispatch(t.Context()); !found || err == nil {
				t.Fatalf("failed publish = %v, %v", found, err)
			}

			var attempts int
			var released, delayed bool
			if err := db.QueryRowContext(t.Context(), `SELECT attempts, lease_token IS NULL,
				available_at > clock_timestamp() FROM public.gof_outbox`).Scan(&attempts, &released, &delayed); err != nil {
				t.Fatal(err)
			}

			if attempts != 1 || !released || !delayed {
				t.Fatal("failed event was lost or not scheduled for retry")
			}

			if found, err := relay.dispatch(t.Context()); found || err != nil {
				t.Fatalf("retried before backoff expired: %v, %v", found, err)
			}

			expire(t, db)
			if found, err := relay.dispatch(t.Context()); !found || err != nil {
				t.Fatalf("recovery publish = %v, %v", found, err)
			}

			if count(t, db, "public.gof_outbox") != 0 || len(ids) != 2 || ids[0] != ids[1] {
				t.Fatal("delivery did not remove the acknowledged event or preserve its logical ID")
			}
		})
	}
}

func TestRelay_ConcurrentClaimsReleaseDatabaseLocks(t *testing.T) {
	db := testDB(t, true)
	queue(t, db, "first")
	queue(t, db, "second")
	claimed := make(chan string, 2)
	proceed := make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	relay := testRelay(t, db, func(ctx context.Context, message pubsub.Message) error {
		claimed <- message.ID
		select {
		case <-proceed:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := relay.dispatch(ctx)
			results <- err
		}()
	}

	first, second := receive(t, claimed), receive(t, claimed)
	if first == second {
		t.Fatal("concurrent workers claimed the same live lease")
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, "SELECT id FROM public.gof_outbox FOR UPDATE NOWAIT")
	if err != nil {
		t.Fatalf("publishing held database locks: %v", err)
	}

	_ = rows.Close()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	close(proceed)
	for range 2 {
		if err := receive(t, results); err != nil {
			t.Fatal(err)
		}
	}

	if count(t, db, "public.gof_outbox") != 0 {
		t.Fatal("concurrent deliveries left acknowledged events behind")
	}
}

func TestRelay_ExpiredLeaseFencesPreviousOwner(t *testing.T) {
	db := testDB(t, true)
	queue(t, db, "crashed-event")
	relay := testRelay(t, db, func(context.Context, pubsub.Message) error { return nil })
	old, err := relay.claim(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	expire(t, db)
	reclaimed, err := relay.claim(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if old.token == reclaimed.token || old.message.ID != reclaimed.message.ID || reclaimed.attempt != 2 {
		t.Fatal("expired event was not reclaimed with a fresh token")
	}

	if err := relay.deliver(t.Context(), old); err == nil {
		t.Fatal("old owner deleted a new owner's event")
	}

	if err := relay.release(t.Context(), old); err == nil {
		t.Fatal("old owner changed a new owner's retry schedule")
	}

	if count(t, db, "public.gof_outbox") != 1 {
		t.Fatal("fenced owner lost the event")
	}

	if err := relay.deliver(t.Context(), reclaimed); err != nil {
		t.Fatal(err)
	}

	if count(t, db, "public.gof_outbox") != 0 {
		t.Fatal("reclaimed event was not acknowledged")
	}
}

func TestRelay_LockedRowsAreSkipped(t *testing.T) {
	db := testDB(t, true)
	queue(t, db, "locked")
	queue(t, db, "available")
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	defer tx.Rollback()
	if _, err := tx.ExecContext(t.Context(), "SELECT id FROM public.gof_outbox WHERE id = 'locked' FOR UPDATE"); err != nil {
		t.Fatal(err)
	}

	var delivered string
	relay := testRelay(t, db, func(_ context.Context, message pubsub.Message) error {
		delivered = message.ID
		return nil
	})
	if found, err := relay.dispatch(t.Context()); !found || err != nil || delivered != "available" {
		t.Fatalf("locked row blocked delivery: %v, %v, %q", found, err, delivered)
	}
}

func TestRelay_TimeoutRetainsEvent(t *testing.T) {
	db := testDB(t, true)
	queue(t, db, "timed-out-event")
	var logs bytes.Buffer
	log, err := logger.New(logger.Config{Service: "outbox-test", Output: &logs})
	if err != nil {
		t.Fatal(err)
	}

	relay, err := New(db, func(ctx context.Context, _ pubsub.Message) error {
		<-ctx.Done()
		return ctx.Err()
	}, Config{Logger: log, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}

	if found, err := relay.dispatch(t.Context()); !found || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed out publication = %v, %v", found, err)
	}

	if count(t, db, "public.gof_outbox") != 1 || !strings.Contains(logs.String(), "outbox delivery failed") {
		t.Fatal("timed-out event was lost or its failure was not logged")
	}

	expire(t, db)
	restarted := testRelay(t, db, func(context.Context, pubsub.Message) error { return nil })
	if found, err := restarted.dispatch(t.Context()); !found || err != nil {
		t.Fatalf("timed-out event was not recoverable: %v, %v", found, err)
	}
}

func TestRelay_RunCancellationAndMissingSchema(t *testing.T) {
	t.Run("missing schema", func(t *testing.T) {
		db := testDB(t, false)
		relay := testRelay(t, db, func(context.Context, pubsub.Message) error { return nil })
		if err := relay.Ping(t.Context()); err == nil {
			t.Fatal("health check accepted missing schema")
		}

		if err := relay.Run(t.Context()); err == nil {
			t.Fatal("worker started without its table")
		}
	})

	t.Run("cancel active publication", func(t *testing.T) {
		db := testDB(t, true)
		queue(t, db, "cancel-event")
		started := make(chan struct{})
		relay := testRelay(t, db, func(ctx context.Context, _ pubsub.Message) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- relay.Run(ctx) }()

		receive(t, started)
		cancel()
		if err := receive(t, done); !errors.Is(err, context.Canceled) {
			t.Fatalf("worker cancellation = %v", err)
		}

		if count(t, db, "public.gof_outbox") != 1 {
			t.Fatal("canceled delivery lost an event")
		}

		expire(t, db)
		restarted := testRelay(t, db, func(context.Context, pubsub.Message) error { return nil })
		if found, err := restarted.dispatch(t.Context()); !found || err != nil {
			t.Fatalf("new worker could not recover canceled delivery: %v, %v", found, err)
		}
	})
}

func TestRelay_RealBrokerAndTelemetry(t *testing.T) {
	db := testDB(t, true)
	oldTraces, oldMetrics, oldPropagation := otel.GetTracerProvider(), otel.GetMeterProvider(), otel.GetTextMapPropagator()
	recorder := tracetest.NewSpanRecorder()
	traces := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	reader := sdkmetric.NewManualReader()
	metrics := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetTracerProvider(traces)
	otel.SetMeterProvider(metrics)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() {
		otel.SetTracerProvider(oldTraces)
		otel.SetMeterProvider(oldMetrics)
		otel.SetTextMapPropagator(oldPropagation)
		_ = traces.Shutdown(context.Background())
		_ = metrics.Shutdown(context.Background())
	})

	broker, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}

	go broker.Start()
	t.Cleanup(func() {
		broker.Shutdown()
		broker.WaitForShutdown()
	})
	if !broker.ReadyForConnections(5 * time.Second) {
		t.Fatal("broker did not start")
	}

	bus, err := pubsub.New(pubsub.Config{
		Logger: testLogger(t), Endpoints: []string{broker.ClientURL()}, Timeout: 100 * time.Millisecond,
		Topics: []pubsub.TopicConfig{{Name: "orders.created"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := bus.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = bus.Shutdown(context.Background()) })
	tenant, err := baggage.NewMember("tenant", "tenant-123")
	if err != nil {
		t.Fatal(err)
	}

	values, err := baggage.New(tenant)
	if err != nil {
		t.Fatal(err)
	}

	ctx, parent := traces.Tracer("service").Start(baggage.ContextWithBaggage(t.Context(), values), "create order")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer tx.Rollback()
	if err := Enqueue(ctx, tx, pubsub.Message{ID: "broker-event", Topic: "orders.created", Data: []byte("payload")}); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	parent.End()
	relay := testRelay(t, db, bus.Publish)
	if found, err := relay.dispatch(t.Context()); !found || err != nil {
		t.Fatalf("broker publish = %v, %v", found, err)
	}

	consumerCtx, stop := context.WithCancel(t.Context())
	defer stop()

	got := make(chan trace.SpanContext, 1)
	done := make(chan error, 1)
	go func() {
		done <- bus.Consume(consumerCtx, pubsub.ConsumerConfig{Topic: "orders.created", Group: "outbox-test"}, func(ctx context.Context, message pubsub.Message) error {
			if message.ID != "broker-event" || string(message.Data) != "payload" {
				return errors.New("event changed")
			}

			if baggage.FromContext(ctx).Member("tenant").Value() != "tenant-123" {
				return errors.New("persisted baggage was lost")
			}

			got <- trace.SpanContextFromContext(ctx)
			return nil
		})
	}()

	consumed := receive(t, got)
	stop()
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	if consumed.TraceID() != parent.SpanContext().TraceID() || count(t, db, "public.gof_outbox") != 0 {
		t.Fatal("outbox lost trace context or did not remove a durable publication")
	}

	queue(t, db, "broker-down")
	broker.Shutdown()
	broker.WaitForShutdown()
	if found, err := relay.dispatch(t.Context()); !found || err == nil || count(t, db, "public.gof_outbox") != 1 {
		t.Fatalf("broker outage lost an event: %v, %v", found, err)
	}

	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}

	var attempts int64
	for _, scope := range data.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			if measurement.Name == "outbox.publish.attempts" {
				for _, point := range measurement.Data.(metricdata.Sum[int64]).DataPoints {
					attempts += point.Value
				}
			}
		}
	}

	if attempts != 2 {
		t.Fatalf("publication attempt metric = %d", attempts)
	}

	foundRelay := false
	for _, span := range recorder.Ended() {
		if span.Name() == "outbox relay" && span.SpanContext().TraceID() == parent.SpanContext().TraceID() {
			foundRelay = true
		}
	}

	if !foundRelay {
		t.Fatal("outbox relay span missing")
	}
}

func testLogger(t *testing.T) *logger.Logger {
	t.Helper()

	log, err := logger.New(logger.Config{Service: "outbox-test", Output: io.Discard})
	if err != nil {
		t.Fatal(err)
	}

	return log
}

func testRelay(t *testing.T, db *sql.DB, publish func(context.Context, pubsub.Message) error) *Relay {
	t.Helper()

	relay, err := New(db, publish, Config{Logger: testLogger(t), PollInterval: 5 * time.Millisecond, RetryDelay: time.Minute, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}

	return relay
}

func expire(t *testing.T, db *sql.DB) {
	t.Helper()

	if _, err := db.ExecContext(t.Context(), "UPDATE public.gof_outbox SET available_at = clock_timestamp() - interval '1 second'"); err != nil {
		t.Fatal(err)
	}
}

func receive[T any](t *testing.T, channel <-chan T) T {
	t.Helper()

	select {
	case value := <-channel:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for worker")
	}

	var zero T
	return zero
}
