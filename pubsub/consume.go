package pubsub

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/Penthious/go-foundation/logger"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ConsumerConfig identifies a durable subscription. Different groups each get
// every retained event. Instances sharing a group divide its work.
type ConsumerConfig struct {
	Topic string
	// Group is stable across restarts; use letters, numbers, underscores, hyphens.
	Group string
	// AckTimeout defaults to 30s and must be at least 30ms. GOF renews the lease
	// while a handler runs. Instances sharing a group must use the same timeout.
	AckTimeout time.Duration
	// RetryDelay defaults to 1s after a failed handler or fetch.
	RetryDelay time.Duration
	// RetryBackoff overrides RetryDelay for message failures. Entries apply to
	// successive failed deliveries; the last repeats. It is copied on Consume.
	RetryBackoff []time.Duration
	// MaxDeliveries defaults to zero (unlimited) for compatibility. A positive
	// limit requires Failures. Exhausted messages are stored before acknowledgment.
	MaxDeliveries int
	// Failures enables durable quarantine and targeted replay for this topic/group.
	// Apply FailedSchema first. All replicas must use the same store and policy.
	Failures *FailedStore
}

// Handler processes a message. Return nil only after the work is committed.
// Errors and panics cause redelivery. Honor ctx cancellation and tolerate duplicates.
type Handler func(context.Context, Message) error

// Consume blocks, processing one message at a time, until ctx is canceled or the
// client shuts down. Run it in a service-owned goroutine. It resumes the group's
// position across restarts; a new group starts at the oldest retained message.
// Configuration errors return immediately. Transient fetch errors are retried.
func (c *Client) Consume(ctx context.Context, cfg ConsumerConfig, handler Handler) error {
	cfg, err := normalizeConsumer(cfg, handler)
	if err != nil {
		return err
	}

	ctx, js, finish, err := c.operation(ctx)
	if err != nil {
		return err
	}

	defer finish()
	if cfg.Failures != nil {
		if err := cfg.Failures.Ping(ctx); err != nil {
			return err
		}
	}

	setupCtx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	consumer, err := js.CreateConsumer(setupCtx, streamName(cfg.Topic), jetstream.ConsumerConfig{
		Name: cfg.Group, Durable: cfg.Group, FilterSubject: cfg.Topic,
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: cfg.AckTimeout,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		// Application limits must not strand a message when durable quarantine fails.
		MaxDeliver: -1,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("pubsub: create consumer for %q: %w", cfg.Topic, err)
	}

	for ctx.Err() == nil {
		if cfg.Failures != nil {
			found, err := c.processReplay(ctx, cfg, handler)
			if err != nil {
				c.cfg.Logger.Warn(ctx, "pubsub replay failed", logger.Err(err))
				if !wait(ctx, cfg.RetryDelay) {
					break
				}
			}

			if found || err != nil {
				continue
			}
		}

		fetchCtx := ctx
		cancelFetch := func() {}
		if cfg.Failures != nil {
			fetchCtx, cancelFetch = context.WithTimeout(ctx, time.Second)
		}

		message, err := consumer.Next(jetstream.FetchContext(fetchCtx))
		cancelFetch()
		if err != nil {
			if ctx.Err() != nil {
				break
			}

			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}

			if errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) {
				return fmt.Errorf("pubsub: subscription no longer exists: %w", err)
			}

			if errors.Is(err, nats.ErrConnectionClosed) {
				return fmt.Errorf("pubsub: connection closed: %w", err)
			}

			c.cfg.Logger.Warn(ctx, "pubsub receive failed", logger.String("topic", cfg.Topic), logger.String("group", cfg.Group), logger.Err(err))
			if !wait(ctx, cfg.RetryDelay) {
				break
			}

			continue
		}

		c.process(ctx, cfg, message, handler)
	}

	return ctx.Err()
}

func normalizeConsumer(cfg ConsumerConfig, handler Handler) (ConsumerConfig, error) {
	if err := validateTopic(cfg.Topic); err != nil {
		return ConsumerConfig{}, err
	}

	if !validGroup(cfg.Group) {
		return ConsumerConfig{}, errors.New("pubsub: group must contain 1-200 letters, numbers, underscores, or hyphens")
	}

	if handler == nil || cfg.AckTimeout < 0 || cfg.RetryDelay < 0 {
		return ConsumerConfig{}, errors.New("pubsub: handler is required and consumer durations must not be negative")
	}

	if cfg.AckTimeout == 0 {
		cfg.AckTimeout = 30 * time.Second
	}

	if cfg.AckTimeout < 30*time.Millisecond {
		return ConsumerConfig{}, errors.New("pubsub: ack timeout must be at least 30ms")
	}

	if cfg.RetryDelay == 0 {
		cfg.RetryDelay = time.Second
	}

	if cfg.MaxDeliveries < 0 || (cfg.MaxDeliveries > 0 && cfg.Failures == nil) {
		return ConsumerConfig{}, errors.New("pubsub: a finite delivery limit requires a failed-message store")
	}

	cfg.RetryBackoff = slices.Clone(cfg.RetryBackoff)
	for _, delay := range cfg.RetryBackoff {
		if delay <= 0 {
			return ConsumerConfig{}, errors.New("pubsub: retry backoff delays must be positive")
		}
	}

	return cfg, nil
}

func (c *Client) process(ctx context.Context, cfg ConsumerConfig, raw jetstream.Msg, handler Handler) {
	metadata, err := raw.Metadata()
	if err != nil {
		c.cfg.Logger.Warn(ctx, "pubsub delivery metadata unavailable", logger.Err(err))
		_ = raw.NakWithDelay(cfg.RetryDelay)
		return
	}

	var event envelope
	err = json.Unmarshal(raw.Data(), &event)
	if err == nil {
		err = validateID(event.ID)
	}

	messageID := event.ID
	if validateID(messageID) != nil {
		messageID = ""
	}

	message := Message{ID: messageID, Topic: cfg.Topic, Data: event.Data}
	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(event.Trace))
	attrs := append(messageAttributes(message), attribute.String("messaging.consumer.group.name", cfg.Group))
	ctx, span := otel.Tracer("go-foundation/pubsub").Start(ctx, cfg.Topic+" process",
		trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(attrs...))
	log := c.cfg.Logger.With(logger.String("topic", cfg.Topic), logger.String("group", cfg.Group), logger.String("message_id", message.ID))
	heartbeatCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(cfg.AckTimeout / 3)
		defer ticker.Stop()

		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				if err := raw.InProgress(); err != nil && !errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
					log.Warn(ctx, "pubsub lease renewal failed", logger.Err(err))
				}
			}
		}
	}()

	if err == nil && cfg.MaxDeliveries > 0 && metadata.NumDelivered > uint64(cfg.MaxDeliveries) {
		err = errors.New("pubsub: delivery limit exceeded before acknowledgment; prior outcome may be ambiguous")
	}

	if err == nil {
		err = callHandler(ctx, handler, message)
	}
	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}

	if err != nil {
		log.Warn(ctx, "pubsub handler failed", logger.Err(err))
		if ctx.Err() == nil && cfg.MaxDeliveries > 0 && metadata.NumDelivered >= uint64(cfg.MaxDeliveries) {
			digest := sha256.Sum256(raw.Data())
			id := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%s:%x", metadata.Stream, metadata.Sequence.Stream, cfg.Group, digest))))
			if storeErr := cfg.Failures.archive(ctx, id, cfg, event, raw.Data(), metadata.NumDelivered, err); storeErr != nil {
				log.Error(ctx, "pubsub quarantine failed; original remains unacknowledged", logger.Err(storeErr))
				err = errors.Join(err, storeErr, raw.NakWithDelay(cfg.retryDelay(metadata.NumDelivered)))
			} else {
				log.Warn(ctx, "pubsub message quarantined", logger.String("failure_id", id))
				ackCtx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
				ackErr := raw.DoubleAck(ackCtx)
				cancel()
				if ackErr != nil {
					log.Warn(ctx, "pubsub quarantine acknowledgment failed", logger.Err(ackErr))
				}

				err = errors.Join(err, ackErr)
			}
		} else {
			err = errors.Join(err, raw.NakWithDelay(cfg.retryDelay(metadata.NumDelivered)))
		}
	} else {
		ackCtx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
		err = raw.DoubleAck(ackCtx)
		cancel()
		if err != nil {
			log.Warn(ctx, "pubsub acknowledgment failed", logger.Err(err))
		} else {
			log.Debug(ctx, "pubsub processed")
		}
	}

	stop()
	<-done
	finishSpan(span, err)
}

func callHandler(ctx context.Context, handler Handler, message Message) (err error) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			err = errors.New("pubsub: handler panicked")
		}
	}()

	err = handler(ctx, message)
	returned = true

	return err
}

func validGroup(group string) bool {
	return len(group) <= 200 && groupPattern.MatchString(group)
}

func (cfg ConsumerConfig) retryDelay(delivery uint64) time.Duration {
	if len(cfg.RetryBackoff) == 0 {
		return cfg.RetryDelay
	}

	index := uint64(0)
	if delivery > 0 {
		index = delivery - 1
	}

	return cfg.RetryBackoff[min(index, uint64(len(cfg.RetryBackoff)-1))]
}

func wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
