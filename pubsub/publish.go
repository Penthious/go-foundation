package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Penthious/go-foundation/logger"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Message contains an application payload. ID must identify the logical event;
// reuse it when retrying an ambiguous publication. Data is opaque bytes.
type Message struct {
	ID    string
	Topic string
	Data  []byte
}

// Validate checks the message's topic and logical event ID without publishing.
func (m Message) Validate() error {
	if err := validateTopic(m.Topic); err != nil {
		return err
	}

	return validateID(m.ID)
}

type envelope struct {
	ID    string            `json:"id"`
	Data  []byte            `json:"data"`
	Trace map[string]string `json:"trace,omitempty"`
}

// Publish waits for confirmation that the broker stored the message. A timeout
// can mean the message was stored but its acknowledgment was lost. The broker
// deduplicates IDs within its configured window; handlers must still be idempotent.
// The topic must have been provisioned by this client or another service.
func (c *Client) Publish(ctx context.Context, message Message) (err error) {
	if err := message.Validate(); err != nil {
		return err
	}

	ctx, js, finish, err := c.operation(ctx)
	if err != nil {
		return err
	}

	defer finish()
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	ctx, span := otel.Tracer("go-foundation/pubsub").Start(ctx, message.Topic+" publish",
		trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(messageAttributes(message)...))
	defer func() { finishSpan(span, err) }()

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	data, err := json.Marshal(envelope{ID: message.ID, Data: message.Data, Trace: carrier})
	if err != nil {
		return fmt.Errorf("pubsub: encode message: %w", err)
	}

	_, err = js.PublishMsg(ctx, &nats.Msg{Subject: message.Topic, Data: data},
		jetstream.WithMsgID(message.ID), jetstream.WithExpectStream(streamName(message.Topic)))
	if err != nil {
		return fmt.Errorf("pubsub: publish to %q: %w", message.Topic, err)
	}

	c.cfg.Logger.Debug(ctx, "pubsub published", logger.String("topic", message.Topic), logger.String("message_id", message.ID))

	return nil
}

func messageAttributes(message Message) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("messaging.system", "nats"),
		attribute.String("messaging.destination.name", message.Topic),
		attribute.String("messaging.message.id", message.ID),
	}
}

func finishSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	span.End()
}
