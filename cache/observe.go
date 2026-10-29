package cache

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

func (c *Client) begin(ctx context.Context, operation string) (context.Context, func(error)) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	attributes := []attribute.KeyValue{
		attribute.String("cache.backend", string(c.cfg.Backend)),
		attribute.String("cache.operation", operation),
	}
	ctx, span := c.tracer.Start(ctx, "cache "+operation, trace.WithAttributes(attributes...))
	started := time.Now()

	return ctx, func(err error) {
		defer cancel()
		defer span.End()

		result := "success"
		switch {
		case errors.Is(err, ErrMiss):
			result = "miss"
		case err != nil:
			result = "error"
			span.SetStatus(codes.Error, "cache operation failed")
		case operation == "get":
			result = "hit"
		}

		attributes = append(attributes, attribute.String("cache.result", result))
		span.SetAttributes(attributes...)
		options := metric.WithAttributes(attributes...)
		c.operations.Add(ctx, 1, options)
		c.duration.Record(ctx, time.Since(started).Seconds(), options)
	}
}
