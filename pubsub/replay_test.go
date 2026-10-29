package pubsub_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/logger"
	"github.com/Penthious/go-foundation/pubsub"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestReplay_FailureBudgetAndCancellationRecovery(t *testing.T) {
	db, store, _ := failedDB(t)
	broker := startBroker(t, nil)
	client := startClient(t, pubsub.Config{Endpoints: []string{broker.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}})
	cfg := pubsub.ConsumerConfig{Topic: "orders.created", Group: "billing", MaxDeliveries: 1, Failures: store, RetryDelay: 10 * time.Millisecond}
	id := archiveOne(t, client, cfg, brokerAPI(t, broker))
	if err := store.ReplayFailed(t.Context(), id); err != nil {
		t.Fatal(err)
	}

	cfg.MaxDeliveries = 2
	var attempts atomic.Int32
	stop := runConsumer(t, client, cfg, func(context.Context, pubsub.Message) error {
		attempts.Add(1)
		return errors.New("still failing")
	})
	waitFailureState(t, store, id, pubsub.FailureStored)
	stop()
	record, err := store.GetFailed(t.Context(), id)
	if err != nil || attempts.Load() != 2 || record.ReplayAttempts != 2 || record.LastError != "still failing" {
		t.Fatalf("replay failure budget = %+v, %v, attempts %d", record, err, attempts.Load())
	}

	if err := store.ReplayFailed(t.Context(), id); err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	stopCanceled := runConsumer(t, client, cfg, func(ctx context.Context, _ pubsub.Message) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	})
	receive(t, entered)
	stopCanceled()
	record, err = store.GetFailed(t.Context(), id)
	if err != nil || record.State != pubsub.FailureReplayPending {
		t.Fatalf("canceled replay was lost: %+v, %v", record, err)
	}

	if _, err := db.ExecContext(t.Context(), "UPDATE public.gof_failed_messages SET available_at = clock_timestamp() - interval '1 second' WHERE id = $1", id); err != nil {
		t.Fatal(err)
	}

	stopRecovered := runConsumer(t, client, cfg, func(context.Context, pubsub.Message) error { return nil })
	waitFailureState(t, store, id, pubsub.FailureReplayed)
	stopRecovered()
}

func TestReplay_ExpiredLeaseFencesStaleWorker(t *testing.T) {
	db, initial, _ := failedDB(t)
	store, err := pubsub.NewFailedStore(db, pubsub.FailedStoreConfig{ReplayTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}

	broker := startBroker(t, nil)
	observed := &failureLogSignal{match: "replay lease no longer owned", signal: make(chan struct{})}
	log, err := logger.New(logger.Config{Service: "replay-test", Output: observed})
	if err != nil {
		t.Fatal(err)
	}

	client := startClient(t, pubsub.Config{Logger: log, Endpoints: []string{broker.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}})
	cfg := pubsub.ConsumerConfig{Topic: "orders.created", Group: "billing", MaxDeliveries: 1, Failures: initial}
	id := archiveOne(t, client, cfg, brokerAPI(t, broker))
	if err := store.ReplayFailed(t.Context(), id); err != nil {
		t.Fatal(err)
	}

	cfg.Failures, cfg.MaxDeliveries = store, 5
	firstEntered, firstProceed := make(chan struct{}), make(chan struct{})
	secondEntered, secondProceed := make(chan struct{}), make(chan struct{})
	blocked := func(entered, proceed chan struct{}) pubsub.Handler {
		return func(ctx context.Context, _ pubsub.Message) error {
			close(entered)
			select {
			case <-proceed:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	stopFirst := runConsumer(t, client, cfg, blocked(firstEntered, firstProceed))
	receive(t, firstEntered)
	if _, err := db.ExecContext(t.Context(), "UPDATE public.gof_failed_messages SET available_at = clock_timestamp() - interval '1 second' WHERE id = $1", id); err != nil {
		t.Fatal(err)
	}

	stopSecond := runConsumer(t, client, cfg, blocked(secondEntered, secondProceed))
	receive(t, secondEntered)
	if err := store.ReplayFailed(t.Context(), id); err != nil {
		t.Fatal(err)
	}

	close(firstProceed)
	receive(t, observed.signal)
	record, err := store.GetFailed(t.Context(), id)
	if err != nil || record.State != pubsub.FailureReplayPending || record.ReplayAttempts != 2 || record.ReplayCount != 1 {
		t.Fatalf("stale worker or duplicate request overwrote ownership: %+v, %v", record, err)
	}

	close(secondProceed)
	waitFailureState(t, store, id, pubsub.FailureReplayed)
	stopFirst()
	stopSecond()
}

func TestReplay_PreservesTraceAndBaggage(t *testing.T) {
	_, store, _ := failedDB(t)
	oldProvider, oldPropagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	provider := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() {
		otel.SetTracerProvider(oldProvider)
		otel.SetTextMapPropagator(oldPropagator)
		_ = provider.Shutdown(context.Background())
	})
	broker := startBroker(t, nil)
	client := startClient(t, pubsub.Config{Endpoints: []string{broker.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}})
	cfg := pubsub.ConsumerConfig{Topic: "orders.created", Group: "billing", MaxDeliveries: 1, Failures: store}
	stop := runConsumer(t, client, cfg, func(context.Context, pubsub.Message) error { return errors.New("failure") })
	member, err := baggage.NewMember("tenant", "example")
	if err != nil {
		t.Fatal(err)
	}

	values, err := baggage.New(member)
	if err != nil {
		t.Fatal(err)
	}

	ctx, root := provider.Tracer("test").Start(baggage.ContextWithBaggage(t.Context(), values), "publish")
	if err := client.Publish(ctx, pubsub.Message{ID: "traced", Topic: cfg.Topic}); err != nil {
		t.Fatal(err)
	}

	root.End()
	waitAcknowledged(t, brokerAPI(t, broker), cfg.Topic, cfg.Group, 1)
	stop()
	page, err := store.ListFailed(t.Context(), pubsub.FailedFilter{Topic: cfg.Topic, Group: cfg.Group})
	if err != nil || len(page) != 1 {
		t.Fatalf("traced failure = %+v, %v", page, err)
	}

	if err := store.ReplayFailed(t.Context(), page[0].ID); err != nil {
		t.Fatal(err)
	}

	stopReplay := runConsumer(t, client, cfg, func(ctx context.Context, message pubsub.Message) error {
		if trace.SpanContextFromContext(ctx).TraceID() != root.SpanContext().TraceID() || baggage.FromContext(ctx).Member("tenant").Value() != "example" || message.ID != "traced" {
			return errors.New("replay lost context or identity")
		}

		return nil
	})
	waitFailureState(t, store, page[0].ID, pubsub.FailureReplayed)
	stopReplay()
}

func archiveOne(t *testing.T, client *pubsub.Client, cfg pubsub.ConsumerConfig, js jetstream.JetStream) string {
	t.Helper()

	stop := runConsumer(t, client, cfg, func(context.Context, pubsub.Message) error { return errors.New("initial failure") })
	if err := client.Publish(t.Context(), pubsub.Message{ID: "event", Topic: cfg.Topic}); err != nil {
		t.Fatal(err)
	}

	waitAcknowledged(t, js, cfg.Topic, cfg.Group, 1)
	stop()
	page, err := cfg.Failures.ListFailed(t.Context(), pubsub.FailedFilter{Topic: cfg.Topic, Group: cfg.Group})
	if err != nil || len(page) != 1 {
		t.Fatalf("initial failure = %+v, %v", page, err)
	}

	return page[0].ID
}
