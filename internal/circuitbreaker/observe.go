package circuitbreaker

import (
	"context"
	"errors"
	"fmt"

	"github.com/Penthious/go-foundation/logger"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type observer struct {
	log         *logger.Logger
	attributes  []attribute.KeyValue
	transitions metric.Int64Counter
	rejections  metric.Int64Counter
}

func newObserver(cfg Config) (*observer, error) {
	if cfg.Logger == nil {
		return nil, errors.New("circuit breaker: logger is required")
	}

	if cfg.Name == "" {
		cfg.Name = "default"
	}

	meter := otel.Meter("go-foundation/circuitbreaker")
	transitions, err := meter.Int64Counter("circuit_breaker.transitions", metric.WithUnit("{transition}"),
		metric.WithDescription("Circuit breaker state transitions by client and state"))
	if err != nil {
		return nil, fmt.Errorf("circuit breaker: create transition counter: %w", err)
	}

	rejections, err := meter.Int64Counter("circuit_breaker.rejections", metric.WithUnit("{rejection}"),
		metric.WithDescription("Calls rejected by an open circuit or active recovery probe"))
	if err != nil {
		return nil, fmt.Errorf("circuit breaker: create rejection counter: %w", err)
	}

	return &observer{
		log:         cfg.Logger.With(logger.String("client_name", cfg.Name), logger.String("client_type", cfg.Protocol)),
		attributes:  []attribute.KeyValue{attribute.String("client.name", cfg.Name), attribute.String("client.type", cfg.Protocol)},
		transitions: transitions, rejections: rejections,
	}, nil
}

func (o *observer) transition(ctx context.Context, previous, current state, generation uint64) {
	attributes := append([]attribute.KeyValue{}, o.attributes...)
	attributes = append(attributes, attribute.String("from_state", previous.String()), attribute.String("to_state", current.String()))
	o.transitions.Add(ctx, 1, metric.WithAttributes(attributes...))

	fields := []logger.Field{logger.String("from_state", previous.String()), logger.String("to_state", current.String()), logger.Int64("generation", int64(generation))}
	if current == open {
		o.log.Warn(ctx, "circuit breaker state changed", fields...)
	} else {
		o.log.Info(ctx, "circuit breaker state changed", fields...)
	}
}

func (o *observer) rejected(ctx context.Context, current state, generation uint64) {
	attributes := append([]attribute.KeyValue{}, o.attributes...)
	attributes = append(attributes, attribute.String("state", current.String()))
	o.rejections.Add(ctx, 1, metric.WithAttributes(attributes...))
	o.log.Warn(ctx, "circuit breaker call rejected", logger.String("state", current.String()), logger.Int64("generation", int64(generation)))
}

func (s state) String() string {
	switch s {
	case closed:
		return "closed"
	case open:
		return "open"
	case halfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}
