package pubsub_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/Penthious/go-foundation/pubsub"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestMessage_Validate(t *testing.T) {
	if err := (pubsub.Message{ID: "event-1", Topic: "orders.created"}).Validate(); err != nil {
		t.Fatal(err)
	}

	for _, message := range []pubsub.Message{
		{ID: "event-1", Topic: "orders.*"},
		{Topic: "orders.created"},
		{ID: "bad\x00id", Topic: "orders.created"},
	} {
		if err := message.Validate(); err == nil {
			t.Fatalf("invalid message accepted: %+v", message)
		}
	}
}

func TestPublish_ValidationAndCancellation(t *testing.T) {
	client, err := pubsub.New(pubsub.Config{Logger: newLogger(t, io.Discard)})
	if err != nil {
		t.Fatal(err)
	}

	for _, msg := range []pubsub.Message{
		{ID: "event-1", Topic: "orders.*"},
		{Topic: "orders.created"},
		{ID: "event\r\ninjected", Topic: "orders.created"},
		{ID: strings.Repeat("a", 257), Topic: "orders.created"},
		{ID: string([]byte{255}), Topic: "orders.created"},
	} {
		if err := client.Publish(t.Context(), msg); err == nil || errors.Is(err, pubsub.ErrNotStarted) {
			t.Errorf("invalid message bypassed validation: %+v: %v", msg, err)
		}
	}

	s := startBroker(t, nil)
	started := startClient(t, pubsub.Config{Endpoints: []string{s.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := started.Publish(ctx, pubsub.Message{ID: "canceled", Topic: "orders.created"}); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled publish = %v", err)
	}
}

func TestPublish_TracePropagationAndMetadataOnlyLogs(t *testing.T) {
	oldProvider, oldPropagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() {
		otel.SetTracerProvider(oldProvider)
		otel.SetTextMapPropagator(oldPropagator)
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	s := startBroker(t, nil)
	var output lockedBuffer
	client := startClient(t, pubsub.Config{
		Logger: newLogger(t, &output), Endpoints: []string{s.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}},
	})
	got := make(chan trace.SpanContext, 1)
	stop := runConsumer(t, client, pubsub.ConsumerConfig{Topic: "orders.created", Group: "billing"}, func(ctx context.Context, msg pubsub.Message) error {
		got <- trace.SpanContextFromContext(ctx)
		if string(msg.Data) != "secret-payload" {
			return errors.New("payload changed")
		}

		return nil
	})
	ctx, root := provider.Tracer("service").Start(t.Context(), "create order")
	if err := client.Publish(ctx, pubsub.Message{ID: "trace-event", Topic: "orders.created", Data: []byte("secret-payload")}); err != nil {
		t.Fatal(err)
	}

	consumed := receive(t, got)
	waitAcknowledged(t, brokerAPI(t, s), "orders.created", "billing", 1)
	stop()
	root.End()

	var producer, consumer tracetest.SpanStub
	for _, span := range exporter.GetSpans() {
		switch span.SpanKind {
		case trace.SpanKindProducer:
			producer = span
		case trace.SpanKindConsumer:
			consumer = span
		}
	}

	if !consumed.IsValid() || consumed.TraceID() != root.SpanContext().TraceID() || producer.Parent.SpanID() != root.SpanContext().SpanID() || consumer.Parent.SpanID() != producer.SpanContext.SpanID() {
		t.Errorf("trace chain was not propagated: root=%v producer=%v consumer=%v", root.SpanContext(), producer, consumer)
	}

	if consumer.Status.Code == codes.Error {
		t.Errorf("successful consumer span recorded error: %+v", consumer.Status)
	}

	logs := output.String()
	if strings.Contains(logs, "secret-payload") || !strings.Contains(logs, "trace-event") || !strings.Contains(logs, "trace_id") {
		t.Errorf("logs omitted correlation or exposed payload: %s", logs)
	}
}

func TestPublish_DeduplicatesLogicalEvent(t *testing.T) {
	s := startBroker(t, nil)
	client := startClient(t, pubsub.Config{Endpoints: []string{s.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}})
	message := pubsub.Message{ID: "same-event", Topic: "orders.created", Data: []byte{0, 1, 255}}
	for range 2 {
		if err := client.Publish(t.Context(), message); err != nil {
			t.Fatal(err)
		}
	}

	js := brokerAPI(t, s)
	name, err := js.StreamNameBySubject(t.Context(), message.Topic)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := js.Stream(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}

	info, err := stream.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if info.State.Msgs != 1 {
		t.Errorf("duplicate event persisted %d times", info.State.Msgs)
	}
}

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buffer.Write(data)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buffer.String()
}
