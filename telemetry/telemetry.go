// Package telemetry configures OTLP logging, tracing, and metrics for a service.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Config identifies the service and configures its OTLP/gRPC exporters.
type Config struct {
	// Service is required and sets service.name on all three signals.
	Service string
	// Environment and Version override matching resource attributes when nonempty.
	Environment string
	Version     string
	// Endpoint is a collector URL, such as https://collector.example.com:4317.
	// Use http:// explicitly for a plaintext collector. If empty, standard OTLP
	// endpoint environment variables apply, defaulting to localhost:4317 with TLS.
	Endpoint string
	// Headers override exporter headers when non-nil. Otherwise standard OTLP
	// header environment variables apply. Prefer environment variables for secrets.
	Headers map[string]string
}

// SDK owns a service's telemetry providers and exposes ready-to-use instruments.
// Create it with New; its zero value is not usable. Tracer and Meter use the
// standard OpenTelemetry APIs. Application loggers are constructed separately.
type SDK struct {
	Tracer trace.Tracer
	Meter  metric.Meter

	logs    *sdklog.LoggerProvider
	traces  *sdktrace.TracerProvider
	metrics *sdkmetric.MeterProvider
	once    sync.Once
	stopErr error
}

// New creates batch log/span exporters and a periodic metric exporter. It
// installs the OTel providers and W3C trace-context/baggage propagation globally
// only after initialization succeeds. Call once at service startup. It does not
// create an application logger. No connection readiness check is performed;
// export errors are reported through the OTel error handler.
func New(ctx context.Context, cfg Config) (_ *SDK, err error) {
	if err := validateConfig(ctx, cfg); err != nil {
		return nil, err
	}

	res, err := newResource(ctx, cfg)
	if err != nil {
		return nil, err
	}

	var cleanup []func(context.Context) error
	defer func() {
		if err == nil {
			return
		}

		// A failed or canceled startup must still release previously started providers.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()

		for _, shutdown := range slices.Backward(cleanup) {
			err = errors.Join(err, shutdown(cleanupCtx))
		}
	}()

	logs, err := newLogProvider(ctx, cfg, res)
	if err != nil {
		return nil, err
	}

	cleanup = append(cleanup, logs.Shutdown)

	traces, err := newTraceProvider(ctx, cfg, res)
	if err != nil {
		return nil, err
	}

	cleanup = append(cleanup, traces.Shutdown)

	metrics, err := newMetricProvider(ctx, cfg, res)
	if err != nil {
		return nil, err
	}

	sdk := &SDK{
		Tracer: traces.Tracer(cfg.Service), Meter: metrics.Meter(cfg.Service),
		logs: logs, traces: traces, metrics: metrics,
	}

	sdk.installGlobals()

	return sdk, nil
}

// LoggerProvider returns the OTel Logs API provider for wiring an application
// logger. The SDK owns its lifecycle; use SDK.Shutdown to flush and close it.
func (s *SDK) LoggerProvider() otellog.LoggerProvider {
	return s.logs
}

func validateConfig(ctx context.Context, cfg Config) error {
	if ctx == nil {
		return errors.New("telemetry: context is required")
	}

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("telemetry: setup: %w", err)
	}

	if strings.TrimSpace(cfg.Service) == "" {
		return errors.New("telemetry: service is required")
	}

	if cfg.Endpoint != "" {
		u, err := url.Parse(cfg.Endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") ||
			u.Host == "" || (u.Path != "" && u.Path != "/") ||
			u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return errors.New("telemetry: endpoint must be an HTTP(S) collector URL without a path, credentials, query, or fragment")
		}
	}

	return nil
}

func newResource(ctx context.Context, cfg Config) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{attribute.String("service.name", cfg.Service)}
	if cfg.Environment != "" {
		attrs = append(attrs, attribute.String("deployment.environment.name", cfg.Environment))
	}

	if cfg.Version != "" {
		attrs = append(attrs, attribute.String("service.version", cfg.Version))
	}

	res, err := resource.New(ctx,
		resource.WithFromEnv(), resource.WithTelemetrySDK(), resource.WithAttributes(attrs...),
	)
	if err != nil {
		return nil, fmt.Errorf("telemetry: create resource: %w", err)
	}

	return res, nil
}

func newLogProvider(ctx context.Context, cfg Config, res *resource.Resource) (*sdklog.LoggerProvider, error) {
	var opts []otlploggrpc.Option
	if cfg.Endpoint != "" {
		opts = append(opts, otlploggrpc.WithEndpointURL(cfg.Endpoint))
	}

	if cfg.Headers != nil {
		opts = append(opts, otlploggrpc.WithHeaders(maps.Clone(cfg.Headers)))
	}

	exporter, err := otlploggrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("telemetry: create log exporter: %w", err)
	}

	return sdklog.NewLoggerProvider(
		sdklog.WithResource(res), sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
	), nil
}

func newTraceProvider(ctx context.Context, cfg Config, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	var opts []otlptracegrpc.Option
	if cfg.Endpoint != "" {
		opts = append(opts, otlptracegrpc.WithEndpointURL(cfg.Endpoint))
	}

	if cfg.Headers != nil {
		opts = append(opts, otlptracegrpc.WithHeaders(maps.Clone(cfg.Headers)))
	}

	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("telemetry: create trace exporter: %w", err)
	}

	return sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithBatcher(exporter)), nil
}

func newMetricProvider(ctx context.Context, cfg Config, res *resource.Resource) (*sdkmetric.MeterProvider, error) {
	var opts []otlpmetricgrpc.Option
	if cfg.Endpoint != "" {
		opts = append(opts, otlpmetricgrpc.WithEndpointURL(cfg.Endpoint))
	}

	if cfg.Headers != nil {
		opts = append(opts, otlpmetricgrpc.WithHeaders(maps.Clone(cfg.Headers)))
	}

	exporter, err := otlpmetricgrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("telemetry: create metric exporter: %w", err)
	}

	return sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res), sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)),
	), nil
}

func (s *SDK) installGlobals() {
	otel.SetLoggerProvider(s.logs)
	otel.SetTracerProvider(s.traces)
	otel.SetMeterProvider(s.metrics)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
}

// ForceFlush exports buffered logs and ended spans and collects metrics now.
// All providers are attempted, even if one fails. Supply a bounded context.
func (s *SDK) ForceFlush(ctx context.Context) error {
	return errors.Join(s.logs.ForceFlush(ctx), s.traces.ForceFlush(ctx), s.metrics.ForceFlush(ctx))
}

// Shutdown flushes and closes every provider. Stop service work first, end any
// active spans, and supply a fresh bounded context. It is safe to call more than
// once or concurrently; subsequent calls return the first shutdown result.
// Global providers are not restored. No further events should be emitted.
func (s *SDK) Shutdown(ctx context.Context) error {
	s.once.Do(func() {
		s.stopErr = errors.Join(s.logs.Shutdown(ctx), s.traces.Shutdown(ctx), s.metrics.Shutdown(ctx))
	})

	return s.stopErr
}
