package ratelimit

import (
	"context"
	"time"

	"github.com/Penthious/go-foundation/logger"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

func (l *Limiter) begin(ctx context.Context) (context.Context, func(Decision, error)) {
	ctx, cancel := context.WithTimeout(ctx, l.cfg.Timeout)
	attributes := []attribute.KeyValue{attribute.String("ratelimit.name", l.cfg.Namespace), attribute.String("ratelimit.backend", string(l.cfg.Backend))}
	ctx, span := l.tracer.Start(ctx, "ratelimit.allow", trace.WithAttributes(attributes...))
	started := time.Now()

	return ctx, func(decision Decision, err error) {
		defer cancel()
		defer span.End()

		result := "allowed"
		fields := []logger.Field{logger.String("ratelimit_name", l.cfg.Namespace), logger.String("ratelimit_backend", string(l.cfg.Backend))}
		switch {
		case err != nil:
			result = "error"
			span.SetStatus(codes.Error, "rate limit check failed")
			l.cfg.Logger.Warn(ctx, "rate limit check failed", append(fields, logger.Err(err))...)
		case !decision.Allowed:
			result = "denied"
			l.cfg.Logger.Warn(ctx, "rate limit exceeded", append(fields, logger.Duration("retry_after", decision.RetryAfter))...)
		}

		attributes = append(attributes, attribute.String("ratelimit.result", result))
		span.SetAttributes(attributes...)
		options := metric.WithAttributes(attributes...)
		l.checks.Add(ctx, 1, options)
		l.duration.Record(ctx, time.Since(started).Seconds(), options)
	}
}
