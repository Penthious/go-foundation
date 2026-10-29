// Package outbox atomically queues pubsub messages in PostgreSQL transactions
// and relays committed events with at-least-once delivery.
package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Penthious/go-foundation/internal/schema"
	"github.com/Penthious/go-foundation/pubsub"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Schema returns the initial migration for public.gof_outbox. Apply it through
// the service's migration process before starting a relay. No API applies DDL
// automatically. The service's transaction and relay must use the same database.
// The migrate package can include this schema through Config.OutboxVersion.
func Schema() string {
	return schema.Outbox()
}

// Enqueue inserts an event using the caller's transaction, preserving trace
// context and baggage. It never commits, rolls back, or publishes. Commit the
// business change and event together. An ID must be unique among queued events;
// reuse the same logical event ID on retries and never recycle it for new events.
func Enqueue(ctx context.Context, tx *sql.Tx, message pubsub.Message) (err error) {
	if tx == nil {
		return errors.New("outbox: transaction is required")
	}

	if err := message.Validate(); err != nil {
		return fmt.Errorf("outbox: invalid message: %w", err)
	}

	ctx, span := otel.Tracer("go-foundation/outbox").Start(ctx, "outbox enqueue", trace.WithAttributes(eventAttributes(message)...))
	defer func() { endSpan(span, err) }()

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	encoded, err := json.Marshal(carrier)
	if err != nil {
		return fmt.Errorf("outbox: encode trace context: %w", err)
	}

	data := message.Data
	if data == nil {
		data = []byte{}
	}

	_, err = tx.ExecContext(ctx, `INSERT INTO public.gof_outbox (id, topic, data, trace_context)
		VALUES ($1, $2, $3, $4::jsonb)`, message.ID, message.Topic, data, string(encoded))
	if err != nil {
		return fmt.Errorf("outbox: enqueue: %w", err)
	}

	return nil
}

func eventAttributes(message pubsub.Message) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("messaging.message.id", message.ID),
		attribute.String("messaging.destination.name", message.Topic),
	}
}

func endSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	span.End()
}
