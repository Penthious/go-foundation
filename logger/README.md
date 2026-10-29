# logger

[Overview](../README.md) · [Development and testing](../testsupport/README.md)

## Logging

Create one logger at startup, pass it into service constructors, and store it on
the components that need it. Pass the operation's context to each logging call
for trace correlation.

```go
log, err := logger.New(logger.Config{
	Service:     "orders",
	Environment: "production",
	Version:     "1.0.0",
	Level:       logger.LevelInfo,
})
if err != nil {
	return err
}

orders := NewOrderService(log)
```

The service adds its own reusable fields:

```go
type OrderService struct {
	log *logger.Logger
}

func NewOrderService(log *logger.Logger) *OrderService {
	return &OrderService{
		log: log.With(logger.String("component", "orders")),
	}
}

func (s *OrderService) Receive(ctx context.Context, orderID string) {
	s.log.Info(ctx, "order received",
		logger.String("order_id", orderID),
		logger.Bool("retry", true),
	)
}
```

Import the logger as `github.com/Penthious/go-foundation/logger`.

### API and field behavior

- `New(Config)` returns a logger or a configuration error. `Service` must be
  nonblank. Environment and version are optional.
- `Debug`, `Info`, `Warn`, and `Error` take a context, message, and optional fields.
  Every call emits an event immediately when its level is enabled.
- `With` creates an independent child logger. Parent and child loggers are safe
  for concurrent use. Later `With` fields override inherited fields; event fields
  override child fields. Duplicate keys produce one value.
- Fields are constructed with `String`, `Bool`, `Int`, `Int64`, `Duration`, and
  `Err`. Empty keys and zero-value fields are ignored. `Err(nil)` is omitted;
  other errors become an `error` string field.
- `Duration` writes Go duration strings, such as `"42ms"`. Use
  `Int64("duration_ms", elapsed.Milliseconds())` for numeric milliseconds.
- The default threshold is `LevelInfo`. `LevelDebug`, `LevelInfo`, `LevelWarn`,
  and `LevelError` apply to both outputs. Unsupported levels are rejected.
- The logger does not install a global application logger, exit, or panic.
  Log an error where it is handled rather than logging it at every return site.

### JSON output

Output is one JSON object per line, written to stdout by default. Supply
`Config.Output` to capture or redirect it; the caller owns that writer's lifecycle.
Writes through a logger and its children are serialized. Application fields are
nested so they cannot overwrite event metadata. Key order is unspecified.

```json
{"time":"2026-10-28T02:15:30.123Z","level":"INFO","msg":"order received","service":"orders","environment":"production","version":"1.0.0","fields":{"component":"orders","order_id":"ord_123","retry":true}}
```

Timestamps use UTC and RFC3339 with fractional seconds as needed. Valid OTel span
contexts add top-level `trace_id` and `span_id`; otherwise these keys are omitted.
An event without application fields omits `fields`. A canceled context does not
suppress local logging. A nil context is treated as a background context.

### OpenTelemetry output

Every enabled event is also passed through
[`otelslog`](https://pkg.go.dev/go.opentelemetry.io/contrib/bridges/otelslog).
The bridge preserves the message, severity, timestamp, metadata, and typed fields.
The OTel SDK correlates the record with the span in the logging context.

After creating telemetry, pass its provider explicitly when constructing the
service's root logger:

```go
log, err := logger.New(logger.Config{
	Service:        "orders",
	Environment:    "production",
	Version:        "1.0.0",
	LoggerProvider: sdk.LoggerProvider(),
})
if err != nil {
	return err
}
```

`LoggerProvider` is the OTel Logs API provider, independent of the private `slog`
backend. If omitted, the bridge uses the provider configured through
`otel.SetLoggerProvider`. Until a provider with a processor and exporter is
configured, the OTel output performs no export; JSON output still works.

The service owns logger creation and injection. Use [`telemetry.New`](../telemetry/README.md#telemetry-setup) to
configure exporters and manage their lifecycle, or supply a provider managed by
the service. `logger.New` can also be used alone; JSON output works without
initializing telemetry. The logger itself opens no collector connection.

`Config.OnError` receives synchronous output failures and defaults to the OTel
error handler (`otel.Handle`). It must be safe for concurrent calls and must not
log recursively through this logger. A JSON write failure does not prevent OTel
delivery. Asynchronous exporter failures use OTel's error handling instead.
