package pubsub_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/pubsub"

	"github.com/nats-io/nats.go/jetstream"
)

func TestConsume_FanoutAndResume(t *testing.T) {
	s := startBroker(t, nil)
	client := startClient(t, pubsub.Config{Endpoints: []string{s.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}})
	js := brokerAPI(t, s)
	message := pubsub.Message{ID: "event-1", Topic: "orders.created", Data: []byte("order-123")}
	if err := client.Publish(t.Context(), message); err != nil {
		t.Fatal(err)
	}

	for _, group := range []string{"billing", "shipping"} {
		got := make(chan pubsub.Message, 1)
		stop := runConsumer(t, client, pubsub.ConsumerConfig{Topic: message.Topic, Group: group}, func(_ context.Context, msg pubsub.Message) error {
			got <- msg
			return nil
		})
		if received := receive(t, got); received.ID != message.ID || string(received.Data) != "order-123" || received.Topic != message.Topic {
			t.Errorf("unexpected message: %+v", received)
		}

		waitAcknowledged(t, js, message.Topic, group, 1)
		stop()
	}

	message.ID = "event-2"
	if err := client.Publish(t.Context(), message); err != nil {
		t.Fatal(err)
	}

	got := make(chan string, 2)
	runConsumer(t, client, pubsub.ConsumerConfig{Topic: message.Topic, Group: "billing"}, func(_ context.Context, msg pubsub.Message) error {
		got <- msg.ID
		return nil
	})

	if id := receive(t, got); id != "event-2" {
		t.Errorf("consumer did not resume: got %q", id)
	}
}

func TestConsume_RetryBackoffAndPolicyValidation(t *testing.T) {
	client, err := pubsub.New(pubsub.Config{Logger: newLogger(t, io.Discard)})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = client.Shutdown(context.Background()) })
	for _, cfg := range []pubsub.ConsumerConfig{
		{Topic: "orders.created", Group: "billing", MaxDeliveries: -1},
		{Topic: "orders.created", Group: "billing", MaxDeliveries: 1},
		{Topic: "orders.created", Group: "billing", RetryBackoff: []time.Duration{0}},
		{Topic: "orders.created", Group: "billing", RetryBackoff: []time.Duration{-time.Second}},
	} {
		if err := client.Consume(t.Context(), cfg, func(context.Context, pubsub.Message) error { return nil }); err == nil || errors.Is(err, pubsub.ErrNotStarted) {
			t.Fatalf("invalid policy accepted: %v", err)
		}
	}

	broker := startBroker(t, nil)
	started := startClient(t, pubsub.Config{Endpoints: []string{broker.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}})
	delays := []time.Duration{20 * time.Millisecond, 60 * time.Millisecond}
	cfg := pubsub.ConsumerConfig{Topic: "orders.created", Group: "backoff", RetryDelay: 5 * time.Second, RetryBackoff: delays}
	times := make(chan time.Time, 4)
	proceed := make(chan struct{})
	var calls atomic.Int32
	stop := runConsumer(t, started, cfg, func(ctx context.Context, _ pubsub.Message) error {
		call := calls.Add(1)
		times <- time.Now()
		if call == 1 {
			select {
			case <-proceed:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if call < 4 {
			return errors.New("retry")
		}

		return nil
	})
	if err := started.Publish(t.Context(), pubsub.Message{ID: "backoff", Topic: cfg.Topic}); err != nil {
		t.Fatal(err)
	}

	previous := receive(t, times)
	delays[0], delays[1] = 5*time.Second, 5*time.Second
	close(proceed)
	for _, minimum := range []time.Duration{20 * time.Millisecond, 60 * time.Millisecond, 60 * time.Millisecond} {
		next := receive(t, times)
		if next.Sub(previous) < minimum-time.Millisecond {
			t.Fatalf("retry occurred before its backoff: %s < %s", next.Sub(previous), minimum)
		}

		previous = next
	}

	waitAcknowledged(t, brokerAPI(t, broker), cfg.Topic, cfg.Group, 1)
	stop()
}

func TestConsume_SharedGroupDividesWork(t *testing.T) {
	s := startBroker(t, nil)
	cfg := pubsub.Config{Endpoints: []string{s.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}}
	first := startClient(t, cfg)
	second := startClient(t, cfg)
	got := make(chan string, 2)
	release := make(chan struct{})
	defer close(release)
	handler := func(ctx context.Context, msg pubsub.Message) error {
		got <- msg.ID
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	consumer := pubsub.ConsumerConfig{Topic: "orders.created", Group: "billing"}
	runConsumer(t, first, consumer, handler)
	runConsumer(t, second, consumer, handler)

	for _, id := range []string{"first", "second"} {
		if err := first.Publish(t.Context(), pubsub.Message{ID: id, Topic: consumer.Topic}); err != nil {
			t.Fatal(err)
		}
	}

	a, b := receive(t, got), receive(t, got)
	if a == b {
		t.Errorf("two workers received the same event: %q", a)
	}
}

func TestConsume_RedeliversFailuresAndPanics(t *testing.T) {
	for _, mode := range []string{"error", "panic"} {
		t.Run(mode, func(t *testing.T) {
			s := startBroker(t, nil)
			client := startClient(t, pubsub.Config{Endpoints: []string{s.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}})
			var attempts atomic.Int32
			got := make(chan string, 1)
			runConsumer(t, client, pubsub.ConsumerConfig{Topic: "orders.created", Group: "billing", RetryDelay: 10 * time.Millisecond}, func(_ context.Context, msg pubsub.Message) error {
				if attempts.Add(1) == 1 {
					if mode == "panic" {
						panic("handler failed")
					}

					return errors.New("handler failed")
				}

				got <- msg.ID
				return nil
			})

			if err := client.Publish(t.Context(), pubsub.Message{ID: "retry-me", Topic: "orders.created"}); err != nil {
				t.Fatal(err)
			}

			if id := receive(t, got); id != "retry-me" || attempts.Load() != 2 {
				t.Errorf("unexpected redelivery: %s, attempts %d", id, attempts.Load())
			}
		})
	}
}

func TestConsume_RenewsLeaseWhileHandlerRuns(t *testing.T) {
	s := startBroker(t, nil)
	client := startClient(t, pubsub.Config{Endpoints: []string{s.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}})
	js := brokerAPI(t, s)
	got := make(chan string, 4)
	release := make(chan struct{})
	defer close(release)
	consumer := pubsub.ConsumerConfig{Topic: "orders.created", Group: "billing", AckTimeout: 150 * time.Millisecond}
	handler := func(ctx context.Context, msg pubsub.Message) error {
		got <- msg.ID
		if msg.ID == "slow" {
			select {
			case <-release:
			case <-ctx.Done():
			}
		}

		return ctx.Err()
	}
	runConsumer(t, client, consumer, handler)
	runConsumer(t, client, consumer, handler)

	for _, id := range []string{"slow", "probe"} {
		if err := client.Publish(t.Context(), pubsub.Message{ID: id, Topic: consumer.Topic}); err != nil {
			t.Fatal(err)
		}
	}

	receive(t, got)
	receive(t, got)
	leaseWindow := time.NewTimer(4 * consumer.AckTimeout)
	defer leaseWindow.Stop()

	select {
	case id := <-got:
		t.Fatalf("message %q redelivered while its lease was renewed", id)
	case <-leaseWindow.C:
	}

	stream, err := js.StreamNameBySubject(t.Context(), consumer.Topic)
	if err != nil {
		t.Fatal(err)
	}

	view, err := js.Consumer(t.Context(), stream, consumer.Group)
	if err != nil {
		t.Fatal(err)
	}

	info, err := view.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if info.NumRedelivered != 0 {
		t.Errorf("redelivered messages = %d", info.NumRedelivered)
	}
}

func TestConsume_ShutdownCancelsAndWaits(t *testing.T) {
	s := startBroker(t, nil)
	client := startClient(t, pubsub.Config{Endpoints: []string{s.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}})
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	runConsumer(t, client, pubsub.ConsumerConfig{Topic: "orders.created", Group: "billing"}, func(ctx context.Context, _ pubsub.Message) error {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release

		return ctx.Err()
	})
	if err := client.Publish(t.Context(), pubsub.Message{ID: "blocking", Topic: "orders.created"}); err != nil {
		t.Fatal(err)
	}

	receive(t, entered)
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() { stopped <- client.Shutdown(ctx) }()
	receive(t, canceled)
	cancel()

	if err := receive(t, stopped); !errors.Is(err, context.Canceled) {
		t.Errorf("shutdown of blocked handler = %v", err)
	}
}

func TestConsume_Validation(t *testing.T) {
	client, err := pubsub.New(pubsub.Config{Logger: newLogger(t, io.Discard)})
	if err != nil {
		t.Fatal(err)
	}

	tests := []pubsub.ConsumerConfig{
		{Topic: "orders.*", Group: "billing"},
		{Topic: "orders.created"},
		{Topic: "orders.created", Group: "billing.bad"},
		{Topic: "orders.created", Group: strings.Repeat("a", 201)},
		{Topic: "orders.created", Group: "billing", AckTimeout: -time.Second},
		{Topic: "orders.created", Group: "billing", AckTimeout: time.Millisecond},
		{Topic: "orders.created", Group: "billing", RetryDelay: -time.Second},
	}
	for _, cfg := range tests {
		err := client.Consume(t.Context(), cfg, func(context.Context, pubsub.Message) error { return nil })
		if err == nil || errors.Is(err, pubsub.ErrNotStarted) {
			t.Errorf("invalid consumer config bypassed validation: %+v: %v", cfg, err)
		}
	}

	if err := client.Consume(t.Context(), pubsub.ConsumerConfig{Topic: "orders.created", Group: "billing"}, nil); err == nil || errors.Is(err, pubsub.ErrNotStarted) {
		t.Errorf("nil handler bypassed validation: %v", err)
	}
}

func TestConsume_DeletedSubscriptionReturnsError(t *testing.T) {
	s := startBroker(t, nil)
	client := startClient(t, pubsub.Config{Endpoints: []string{s.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}})
	js := brokerAPI(t, s)
	name, err := js.StreamNameBySubject(t.Context(), "orders.created")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- client.Consume(ctx, pubsub.ConsumerConfig{Topic: "orders.created", Group: "billing"}, func(context.Context, pubsub.Message) error { return nil })
	}()
	eventually(t, func() bool {
		consumer, err := js.Consumer(t.Context(), name, "billing")
		if err != nil {
			return false
		}

		info, err := consumer.Info(t.Context())
		return err == nil && info.NumWaiting > 0
	})

	if err := js.DeleteConsumer(t.Context(), name, "billing"); err != nil {
		t.Fatal(err)
	}

	if err := receive(t, done); err == nil || errors.Is(err, context.Canceled) {
		t.Errorf("deleted subscription was not reported: %v", err)
	}
}

func TestConsume_InvalidEnvelopeIsRetriedWithoutCallingHandler(t *testing.T) {
	s := startBroker(t, nil)
	client := startClient(t, pubsub.Config{Endpoints: []string{s.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}})
	js := brokerAPI(t, s)
	if _, err := js.Publish(t.Context(), "orders.created", []byte("invalid-envelope")); err != nil {
		t.Fatal(err)
	}

	got := make(chan string, 1)
	runConsumer(t, client, pubsub.ConsumerConfig{Topic: "orders.created", Group: "billing", RetryDelay: 10 * time.Millisecond}, func(_ context.Context, message pubsub.Message) error {
		got <- message.ID
		return nil
	})
	name, err := js.StreamNameBySubject(t.Context(), "orders.created")
	if err != nil {
		t.Fatal(err)
	}

	eventually(t, func() bool {
		consumer, err := js.Consumer(t.Context(), name, "billing")
		if err != nil {
			return false
		}

		info, err := consumer.Info(t.Context())
		return err == nil && info.NumRedelivered > 0
	})
	if err := client.Publish(t.Context(), pubsub.Message{ID: "valid-event", Topic: "orders.created"}); err != nil {
		t.Fatal(err)
	}

	if id := receive(t, got); id != "valid-event" {
		t.Errorf("invalid envelope reached handler: %q", id)
	}
}

func runConsumer(t *testing.T, client *pubsub.Client, cfg pubsub.ConsumerConfig, handler pubsub.Handler) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- client.Consume(ctx, cfg, handler) }()
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

func waitAcknowledged(t *testing.T, js jetstream.JetStream, topic, group string, sequence uint64) {
	t.Helper()

	stream, err := js.StreamNameBySubject(t.Context(), topic)
	if err != nil {
		t.Fatal(err)
	}

	eventually(t, func() bool {
		consumer, err := js.Consumer(t.Context(), stream, group)
		if err != nil {
			return false
		}

		info, err := consumer.Info(t.Context())
		return err == nil && info.AckFloor.Stream >= sequence
	})
}
