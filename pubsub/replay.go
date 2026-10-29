package pubsub

import (
	"context"
	"errors"

	"github.com/Penthious/go-foundation/logger"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func (c *Client) processReplay(ctx context.Context, cfg ConsumerConfig, handler Handler) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.Failures.cfg.ReplayTimeout)
	defer cancel()

	entry, err := cfg.Failures.claim(ctx, cfg)
	if errors.Is(err, ErrFailedNotFound) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(entry.trace))
	ctx, span := otel.Tracer("go-foundation/pubsub").Start(ctx, cfg.Topic+" replay",
		trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(messageAttributes(entry.Message)...))
	if cfg.MaxDeliveries > 0 && entry.ReplayAttempts > int64(cfg.MaxDeliveries) {
		err = errors.New("pubsub: replay delivery limit exceeded after an ambiguous attempt")
	} else {
		err = callHandler(ctx, handler, entry.Message)
	}

	if err == nil {
		err = ctx.Err()
	}

	state := FailureReplayed
	if err != nil {
		state = FailureReplayPending
		if cfg.MaxDeliveries > 0 && entry.ReplayAttempts >= int64(cfg.MaxDeliveries) {
			state = FailureStored
		}
	}

	finalErr := cfg.Failures.finishReplay(ctx, entry, state, cfg.retryDelay(uint64(entry.ReplayAttempts)), err)
	finishSpan(span, errors.Join(err, finalErr))
	if err != nil {
		c.cfg.Logger.Warn(ctx, "pubsub replay handler failed", logger.String("failure_id", entry.ID), logger.Err(err))
	}

	return true, finalErr
}
