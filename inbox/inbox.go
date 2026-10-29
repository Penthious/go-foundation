// Package inbox commits consumer deduplication markers and database changes in
// one PostgreSQL transaction. It does not make external side effects atomic.
package inbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Penthious/go-foundation/internal/schema"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Schema returns the initial migration for public.gof_inbox. Apply it through
// service migrations before processing messages. No API runs DDL automatically.
// The migrate package can include this schema through Config.InboxVersion.
func Schema() string {
	return schema.Inbox()
}

// Key identifies one logical event for one logical consumer. Consumer is a stable
// namespace, not an instance/pod ID. Keys are compared exactly, including case.
type Key struct {
	Consumer  string
	MessageID string
}

// Handler writes business changes using the supplied transaction and context.
// It must not commit, roll back, or retain the transaction. Use that transaction
// for all protected writes, including outbox events. Return errors to roll back.
type Handler func(context.Context, *sql.Tx) error

// Store processes events through a deduplication ledger and is safe for concurrent
// use. Construct it with New. It borrows its database and global OTel providers;
// the service must keep them alive while processing messages.
type Store struct {
	db       *sql.DB
	cfg      Config
	messages metric.Int64Counter
	duration metric.Float64Histogram
}

// New validates dependencies without querying the database or starting workers.
// Initialize telemetry before New. Closing the database remains the caller's job.
func New(db *sql.DB, cfg Config) (*Store, error) {
	if db == nil {
		return nil, errors.New("inbox: database is required")
	}

	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}

	meter := otel.Meter("go-foundation/inbox")
	messages, err := meter.Int64Counter("inbox.messages", metric.WithDescription("Inbox processing results"))
	if err != nil {
		return nil, fmt.Errorf("inbox: create message counter: %w", err)
	}

	duration, err := meter.Float64Histogram("inbox.process.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, fmt.Errorf("inbox: create duration histogram: %w", err)
	}

	return &Store{db: db, cfg: cfg, messages: messages, duration: duration}, nil
}

// Process returns true only after committing a new marker and the handler's
// writes. False with nil error means an earlier transaction already processed
// the key; the handler was skipped. At default isolation, concurrent duplicates
// wait for the first transaction, then skip after commit or proceed after rollback.
// Stronger isolation may return serialization errors for the caller to retry.
// Errors, cancellation, and handler panics roll back both marker and writes.
// An ambiguous commit returns an error; retrying the same key resolves its outcome.
func (s *Store) Process(ctx context.Context, key Key, handler Handler) (processed bool, err error) {
	if err := validateKey(key); err != nil {
		return false, err
	}

	if handler == nil {
		return false, errors.New("inbox: handler is required")
	}

	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	ctx, span := otel.Tracer("go-foundation/inbox").Start(ctx, "inbox process", trace.WithAttributes(
		attribute.String("messaging.consumer.id", key.Consumer), attribute.String("messaging.message.id", key.MessageID)))
	started := time.Now()
	defer func() { s.observe(ctx, span, started, processed, err) }()

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: s.cfg.Isolation})
	if err != nil {
		return false, fmt.Errorf("inbox: begin transaction: %w", err)
	}

	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `INSERT INTO public.gof_inbox (consumer, message_id)
		VALUES ($1, $2) ON CONFLICT (consumer, message_id) DO NOTHING`, key.Consumer, key.MessageID)
	if err != nil {
		return false, fmt.Errorf("inbox: record event: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("inbox: count recorded events: %w", err)
	}

	if count == 0 {
		return false, nil
	}

	if err := callHandler(ctx, tx, handler); err != nil {
		return false, fmt.Errorf("inbox: handler: %w", err)
	}

	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("inbox: processing canceled: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("inbox: commit: %w", err)
	}

	return true, nil
}

// Ping checks connectivity and required table columns under Timeout. It can be
// registered with health.Required. It does not verify write permissions.
func (s *Store) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	var count int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM
		(SELECT consumer, message_id, recorded_at FROM public.gof_inbox LIMIT 0) AS markers`).Scan(&count)
	if err != nil {
		return fmt.Errorf("inbox: check schema: %w", err)
	}

	return nil
}

func validateKey(key Key) error {
	if strings.TrimSpace(key.Consumer) == "" {
		return errors.New("inbox: consumer namespace is required")
	}

	for _, field := range []struct {
		name  string
		value string
		max   int
	}{{"consumer", key.Consumer, 512}, {"message ID", key.MessageID, 256}} {
		if len(field.value) == 0 || len(field.value) > field.max || !utf8.ValidString(field.value) || strings.ContainsAny(field.value, "\r\n\x00") {
			return fmt.Errorf("inbox: invalid %s", field.name)
		}
	}

	return nil
}

func callHandler(ctx context.Context, tx *sql.Tx, handler Handler) (err error) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			err = errors.New("handler panicked")
		}
	}()

	err = handler(ctx, tx)
	returned = true

	return err
}

func (s *Store) observe(ctx context.Context, span trace.Span, started time.Time, processed bool, err error) {
	result := "processed"
	if err != nil {
		result = "failure"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else if !processed {
		result = "duplicate"
	}

	span.SetAttributes(attribute.String("inbox.result", result))
	span.End()

	attributes := metric.WithAttributes(attribute.String("result", result))
	s.messages.Add(ctx, 1, attributes)
	s.duration.Record(ctx, time.Since(started).Seconds(), attributes)
}
