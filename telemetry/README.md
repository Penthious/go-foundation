# telemetry

[Overview](../README.md) · [Development and testing](../testsupport/README.md)

## Telemetry setup

Call `telemetry.New` once at startup to configure the log, trace, and metric
providers. It exposes a tracer and meter, with batch processors for logs and spans
and a periodic metric reader. The service creates its own logger with `logger.New`
and connects it to `sdk.LoggerProvider()`. All three exported signals share the
service's resource metadata.

```go
func run(ctx context.Context) (err error) {
	cfg := telemetry.Config{
		Service:     "orders",
		Environment: "production",
		Version:     "1.0.0",
	}

	sdk, err := telemetry.New(ctx, cfg)
	if err != nil {
		return err
	}

	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err = errors.Join(err, sdk.Shutdown(shutdownCtx))
	}()

	log, err := logger.New(logger.Config{
		Service:        cfg.Service,
		Environment:    cfg.Environment,
		Version:        cfg.Version,
		Level:          logger.LevelInfo,
		LoggerProvider: sdk.LoggerProvider(),
	})
	if err != nil {
		return err
	}

	ctx, span := sdk.Tracer.Start(ctx, "receive order")
	defer span.End()

	ordersReceived, err := sdk.Meter.Int64Counter("orders.received")
	if err != nil {
		return err
	}

	ordersReceived.Add(ctx, 1)
	log.Info(ctx, "order received", logger.String("order_id", "ord_123"))

	return nil
}
```

This example uses `context`, `errors`, `time`, and this module's `logger` and
`telemetry` packages. In a service, create metric instruments once and pass
`log`, `sdk.Tracer`, and `sdk.Meter` to the components that need them. Keep the
service identity consistent between telemetry resources and logger metadata.
The logger's API hides `slog`; tracing and metrics use the standard OTel APIs.
Services can also use `logger.New` independently, without importing `telemetry`.

### Collector configuration

The exporters use **OTLP/gRPC**, normally on port **4317**. They default to
`localhost:4317` with TLS. Set an endpoint URL in the deployment environment:

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=https://collector.example.com:4317
```

For an explicitly plaintext local collector:

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
```

The URL scheme controls transport security; `http://` here still means gRPC,
not OTLP/HTTP. Collector URLs must not contain signal paths. The transport is
fixed to gRPC; `OTEL_EXPORTER_OTLP_PROTOCOL` does not switch it to HTTP.

`Config.Endpoint` overrides all three exporter endpoints when provided. With it
omitted, `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT`, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`,
and `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` override the common environment endpoint
for their respective signals. TLS certificates, client certificates, exporter
timeouts, and compression use the exporters' standard OTLP environment settings.
The exporters retain their standard bounded queues, timeouts, and retry policies.

Use `OTEL_EXPORTER_OTLP_HEADERS` or the corresponding signal-specific headers
variables for collector authentication. `Config.Headers`, when non-nil,
overrides environment headers for all three exporters; an empty map clears them.
Headers are copied at setup. Keep deployment credentials out of source control.

`Service` is required and overrides `service.name`. Nonempty `Environment` and
`Version` override `deployment.environment.name` and `service.version`.
`OTEL_RESOURCE_ATTRIBUTES` supplies additional resource attributes. Sampling,
batching, and metric collection use the OTel SDK defaults and supported settings,
including `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG`,
`OTEL_BSP_*`, `OTEL_BLRP_*`, and `OTEL_METRIC_EXPORT_INTERVAL`.

### Lifecycle and instrumentation

New installs the OTel log, tracer, and meter providers and W3C trace-context and
baggage propagation globally, so instrumented libraries can use them. It does not
create an application logger. Globals change only after initialization succeeds;
partial failures close the providers already created. The service owns its logger
and configures its level, output, and error handling through `logger.Config`.

`sdk.LoggerProvider()` returns the OTel Logs API provider for connecting that
logger explicitly. Telemetry owns the provider's lifecycle; use `sdk.Shutdown`
to flush and close it. Logger creation does not take ownership of the provider.

Services still need to record spans and metrics or use instrumented libraries.
New does not add HTTP/database middleware or instrument application operations
automatically. Continue passing request contexts so logs correlate with spans.

After stopping service work and ending spans, call `sdk.Shutdown` with a fresh,
bounded context to flush and close all providers. It attempts every provider and
joins errors; concurrent or repeated calls return the first shutdown result.
Providers are not restored globally on shutdown. `sdk.ForceFlush` exports pending
logs/ended spans and collects metrics without shutting down; it is useful for
explicit checkpoints and tests, not after every event.

New initializes exporters without checking collector readiness. Async export
failures go to OTel's error handler. A successful setup does not guarantee that a
collector is reachable or that its pipelines accept all three signals.
