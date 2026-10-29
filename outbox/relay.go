package outbox

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Penthious/go-foundation/logger"
	"github.com/Penthious/go-foundation/pubsub"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Relay publishes committed events. It borrows the database, publisher, logger,
// and global OTel providers; their lifetimes must include Run. Publishers must
// honor context cancellation and return nil only after durable acceptance.
type Relay struct {
	db       *sql.DB
	publish  func(context.Context, pubsub.Message) error
	cfg      Config
	attempts metric.Int64Counter
	duration metric.Float64Histogram
}

// New validates dependencies without querying the database or starting workers.
// Pass bus.Publish as publish to use GOF pubsub. Apply Schema before Run.
func New(db *sql.DB, publish func(context.Context, pubsub.Message) error, cfg Config) (*Relay, error) {
	if db == nil || publish == nil {
		return nil, errors.New("outbox: database and publisher are required")
	}

	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}

	meter := otel.Meter("go-foundation/outbox")
	attempts, err := meter.Int64Counter("outbox.publish.attempts", metric.WithDescription("Publication attempts by result"))
	if err != nil {
		return nil, fmt.Errorf("outbox: create attempt counter: %w", err)
	}

	duration, err := meter.Float64Histogram("outbox.publish.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, fmt.Errorf("outbox: create duration histogram: %w", err)
	}

	return &Relay{db: db, publish: publish, cfg: cfg, attempts: attempts, duration: duration}, nil
}

// Ping checks connectivity and required table columns under Timeout. It can be
// passed to health.Required; it checks storage availability, not queue freshness.
func (r *Relay) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
	defer cancel()

	rows, err := r.db.QueryContext(ctx, `SELECT id, topic, data, trace_context, created_at,
		available_at, attempts, lease_token FROM public.gof_outbox LIMIT 0`)
	if err != nil {
		return fmt.Errorf("outbox: check schema: %w", err)
	}

	defer rows.Close()

	return rows.Err()
}

// Run verifies storage, then relays until cancellation, returning ctx.Err().
// Database/publish failures are logged and retried. Multiple Run invocations or
// service replicas may share a queue. Delivery is at least once and unordered;
// consumers must be idempotent. Cancel and join Run before closing dependencies.
func (r *Relay) Run(ctx context.Context) error {
	if err := r.Ping(ctx); err != nil {
		return err
	}

	for ctx.Err() == nil {
		found, err := r.dispatch(ctx)
		if !found || err != nil {
			timer := time.NewTimer(r.cfg.PollInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
	}

	return ctx.Err()
}

type event struct {
	message pubsub.Message
	trace   []byte
	attempt int64
	token   string
}

func (r *Relay) dispatch(ctx context.Context) (bool, error) {
	runCtx := ctx
	ctx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
	defer cancel()

	event, err := r.claim(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}

	if err != nil {
		if runCtx.Err() == nil {
			r.cfg.Logger.Warn(ctx, "outbox claim failed", logger.Err(err))
		}

		return false, fmt.Errorf("outbox: claim: %w", err)
	}

	carrier := propagation.MapCarrier{}
	if err := json.Unmarshal(event.trace, &carrier); err != nil {
		r.cfg.Logger.Warn(ctx, "outbox invalid trace context", logger.String("message_id", event.message.ID), logger.Err(err))
		return true, errors.Join(fmt.Errorf("outbox: decode trace context: %w", err), r.release(ctx, event))
	}

	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)
	ctx, span := otel.Tracer("go-foundation/outbox").Start(ctx, "outbox relay", trace.WithAttributes(eventAttributes(event.message)...))
	err = r.deliver(ctx, event)
	if err != nil && runCtx.Err() == nil {
		r.cfg.Logger.Warn(ctx, "outbox delivery failed", logger.String("message_id", event.message.ID),
			logger.String("topic", event.message.Topic), logger.Int64("attempt", event.attempt), logger.Err(err))
	}

	endSpan(span, err)

	return true, err
}

func (r *Relay) claim(ctx context.Context) (event, error) {
	event := event{token: rand.Text()}
	err := r.db.QueryRowContext(ctx, `UPDATE public.gof_outbox
		SET lease_token = $1, available_at = clock_timestamp() + $2::bigint * interval '1 microsecond', attempts = attempts + 1
		WHERE id = (SELECT id FROM public.gof_outbox WHERE available_at <= clock_timestamp()
			ORDER BY available_at, created_at, id FOR UPDATE SKIP LOCKED LIMIT 1)
		RETURNING id, topic, data, trace_context, attempts`, event.token, (2*r.cfg.Timeout).Microseconds()).Scan(
		&event.message.ID, &event.message.Topic, &event.message.Data, &event.trace, &event.attempt)

	return event, err
}

func (r *Relay) deliver(ctx context.Context, event event) error {
	started := time.Now()
	err := callPublisher(ctx, r.publish, event.message)
	result := "success"
	if err != nil {
		result = "failure"
	}

	attributes := metric.WithAttributes(attribute.String("result", result))
	r.attempts.Add(ctx, 1, attributes)
	r.duration.Record(ctx, time.Since(started).Seconds(), attributes)
	log := r.cfg.Logger.With(logger.String("message_id", event.message.ID), logger.String("topic", event.message.Topic), logger.Int64("attempt", event.attempt))
	if err != nil {
		return errors.Join(err, r.release(ctx, event))
	}

	deleted, err := r.db.ExecContext(ctx, "DELETE FROM public.gof_outbox WHERE id = $1 AND lease_token = $2", event.message.ID, event.token)
	if err != nil {
		return fmt.Errorf("outbox: acknowledge: %w", err)
	}

	if err := affected(deleted); err != nil {
		return err
	}

	log.Debug(ctx, "outbox delivered")

	return nil
}

func (r *Relay) release(ctx context.Context, event event) error {
	result, err := r.db.ExecContext(ctx, `UPDATE public.gof_outbox
		SET lease_token = NULL, available_at = clock_timestamp() + $3::bigint * interval '1 microsecond'
		WHERE id = $1 AND lease_token = $2`, event.message.ID, event.token, r.cfg.RetryDelay.Microseconds())
	if err != nil {
		return fmt.Errorf("outbox: schedule retry: %w", err)
	}

	return affected(result)
}

func affected(result sql.Result) error {
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("outbox: count updated events: %w", err)
	}

	if count != 1 {
		return errors.New("outbox: lease no longer owned")
	}

	return nil
}

func callPublisher(ctx context.Context, publish func(context.Context, pubsub.Message) error, message pubsub.Message) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("outbox: publisher panicked")
		}
	}()

	return publish(ctx, message)
}
