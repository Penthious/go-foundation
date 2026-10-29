# go-foundation

Shared Go libraries for services. `telemetry` sets up OTLP/gRPC logs, traces, and
metrics. `logger` provides structured logging with a private `slog` backend.
`config` loads service-owned structs from environment variables; `buildinfo`
exposes version, commit, and build time stamped into the service executable.
`httpclient` provides instrumented HTTP clients with DEBUG curl logging.
`httpserver` serves standard handlers with request logs, OTel, and health checks.
`grpcserver` serves generated RPC handlers with auth, OTel, health, and Fx lifecycle.
`grpcclient` connects generated RPC clients with telemetry, credentials, and deadlines.
`auth` protects handlers using JWT access tokens, API keys, and required scopes.
`ratelimit` applies authenticated identity quotas using memory or private Redis
storage, with HTTP middleware, gRPC interceptors, telemetry, and Fx lifecycle.
`health` runs arbitrary required/optional dependency functions and reports their status.
`pubsub` provides durable messaging through GOF types, with NATS JetStream kept private.
`postgres` provides an instrumented `*sql.DB` with pgx kept private.
`cache` provides expiring byte values through Redis or a bounded in-memory LRU.
`scheduler` runs cron/interval jobs locally or coordinates them across replicas.
`outbox` queues events in PostgreSQL transactions and relays them through pubsub.
`inbox` commits consumer deduplication markers with the handler's database writes.
`migrate` runs service-owned embedded SQL with Goose kept internal.
Pubsub optionally stores exhausted deliveries in PostgreSQL and replays them to
their original topic/group without exposing broker APIs.
`foundationfx` optionally wires these components into Uber Fx applications.

## Installation

Requires Go 1.27.1 or newer. From your service module:

```sh
go get github.com/Penthious/go-foundation@main
```

Go records the resolved commit as a version in your service's `go.mod`.

## Service configuration

Services define their own configuration types. `config.Load[T]()` reads a fresh
startup snapshot, applies tagged defaults, parses typed values, then calls the
top-level `Validate() error` method when present. It returns the zero `T` on any
failure; partially populated config is never returned. No global config holder,
background watcher, or live reload is involved.

```go
type AppConfig struct {
	Service     string        `env:"SERVICE,required,notEmpty"`
	Environment string        `env:"ENVIRONMENT" envDefault:"local"`
	Port        int           `env:"PORT" envDefault:"8080"`
	Timeout     time.Duration `env:"TIMEOUT" envDefault:"5s"`
}

func (c *AppConfig) Validate() error {
	if strings.TrimSpace(c.Service) == "" {
		return errors.New("service must not be blank")
	}

	if c.Port < 1 || c.Port > 65535 {
		return errors.New("port must be between 1 and 65535")
	}

	if c.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}

	return nil
}

cfg, err := config.Load[AppConfig](config.Options{Prefix: "ORDERS_"})
if err != nil {
	return err
}
```

This uses `errors`, `strings`, `time`, and `github.com/Penthious/go-foundation/config`. The prefix is
prepended verbatim, producing names such as `ORDERS_SERVICE` and `ORDERS_PORT`.
Use exported fields; `T` must be a struct, not a pointer. Supported conversions
include strings/named scalars, booleans, numbers, `time.Duration`, slices, maps,
optional pointers, and types implementing `encoding.TextUnmarshaler`.

`envDefault` applies when the variable is missing **or empty**. Explicit `false`
and `0` values override defaults and can be rejected by service validation.
`required` rejects an absent variable after defaults; combine it with `notEmpty`
to reject an explicitly empty value. Use `env:"-"` to skip a field. Untagged
scalar fields are ignored. Nested structs use their own field tags and can add
`envPrefix:"HTTP_"`; `envSeparator` and `envKeyValSeparator` configure collection
separators. Nested `Validate` methods are service-owned and must be called from
the top-level validator if needed.

The parser dependency is private; services use GOF's `Options` and return values.
Parser errors report field/variable names without values or underlying parser
causes, including when a custom text parser embeds its input in an error. Errors
for multiple fields are combined. Service validation errors preserve their
causes for `errors.Is`/`errors.As`; keep those messages free of secrets. Do not
log the entire loaded config or put credentials in source-controlled defaults.

For tests, pass `config.Options{Environment: map[string]string{...}}`. A non-nil
map completely replaces the process environment; an empty map selects defaults
without process-variable fallback. Nil uses the process environment. Options
and the map must not be mutated concurrently with loading. `Load` accepts zero
or one `Options` value. Values injected into running services should be treated
as immutable; changing the process environment later does not update them.

### Config in Fx

```go
app := fx.New(
	foundationfx.Module,
	foundationfx.Config[AppConfig](config.Options{Prefix: "ORDERS_"}),
	fx.Provide(func(cfg AppConfig, build buildinfo.Info) logger.Config {
		return logger.Config{
			Service: cfg.Service, Environment: cfg.Environment, Version: build.Version,
		}
	}),
	// Add service constructors that receive AppConfig and *logger.Logger.
)
```

`foundationfx.Config[T]` loads and validates `T` once during `fx.New`, even if no
other constructor consumes it. Invalid config appears through `app.Err()` before
any lifecycle hook starts. Services receive the concrete config value by type.
The base module also provides `buildinfo.Info`; logger and telemetry metadata
remain explicit so each service chooses how to compose them.

When telemetry must initialize before Fx/HTTP construction, load config once
first, initialize telemetry using that config and `buildinfo.Current()`, then
use `fx.Supply(cfg, sdk)` rather than registering `foundationfx.Config[T]` again.
The complete startup example below follows that pattern. Configuration loading
does not create loggers, telemetry providers, clients, or listeners.

## Docker build metadata

`buildinfo.Current()` returns `Info{Version, Commit, BuildTime}` describing the
running service executable. The defaults are `dev`, `unknown`, and `unknown`.
Build time is informational text, conventionally a UTC RFC3339 timestamp. GOF
does not invoke git, inspect the container, or infer a build date at runtime.
Runtime environment variables cannot override embedded metadata.

Use Go linker string variables when building each service image. This fragment
belongs in the service's Go builder stage, after copying dependencies and source:

```dockerfile
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown

RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w \
      -X github.com/Penthious/go-foundation/buildinfo.version=${VERSION} \
      -X github.com/Penthious/go-foundation/buildinfo.commit=${COMMIT} \
      -X github.com/Penthious/go-foundation/buildinfo.buildTime=${BUILD_TIME}" \
    -o /out/service ./cmd/service
```

Replace `./cmd/service` with the service's main package. The `-X` targets use
**GOF's full module import path**, not the service's `main` package or an import
alias. Use whitespace-free versions/commit IDs and an
RFC3339 timestamp for these flags. See the official [Go linker options](https://pkg.go.dev/cmd/link).

```sh
docker build -t orders:1.2.3 \
  --build-arg VERSION=1.2.3 \
  --build-arg COMMIT=abc123 \
  --build-arg BUILD_TIME=2026-10-28T12:00:00Z .

docker run --rm -p 8080:8080 \
  -e ORDERS_SERVICE=orders \
  -e ORDERS_ENVIRONMENT=production \
  orders:1.2.3
```

Deployment environment stays runtime configuration, so the same image can move
from staging to production without changing its build identity. Pass version to
`logger.Config.Version` and `telemetry.Config.Version`; commit and build time can
be recorded in a startup log using the logger's string fields. Do not put secrets
in build metadata, `-ldflags`, or Docker build arguments: those values are part
of the artifact/build history. See [Docker build variables](https://docs.docker.com/build/building/variables/).

## Dependency injection with Fx

Use `foundationfx.Module` for runtime dependency injection. It provides one
`*logger.Logger` and `*http.Client` per Fx app. Supply a `logger.Config`; supplying
`httpclient.Config` is optional and retains the HTTP client's defaults when absent.
An HTTP config without its own logger receives the app's injected logger.
Add `foundationfx.NamedHTTPClientModule("orders")` for each additional upstream,
with a matching named `httpclient.Config`; see [multiple named HTTP clients](#multiple-named-http-clients-in-fx).

When the service supplies an initialized `*telemetry.SDK`, the module connects
its log provider to the logger unless `logger.Config.LoggerProvider` was explicitly
set. It also exposes the SDK's `trace.Tracer` and `metric.Meter` for injection.
Logger/HTTP-only apps can omit the SDK; requesting a tracer or meter requires it.
Providers are lazy and are constructed only when consumed.

Requesting `*pubsub.Client` also registers broker startup and shutdown hooks.
Supply `pubsub.Config` to configure endpoints and topics; its logger defaults to
the app's injected logger. Constructing the graph opens no broker connections.
If the client is unused, no broker is needed. See the pubsub section for its
separate `Start`/`Shutdown` lifecycle when using manual injection.

HTTP listeners are an explicit opt-in: add `foundationfx.HTTPServerModule` and
provide `httpserver.Config`. It starts the listener automatically; the base
`foundationfx.Module` never starts an HTTP listener by itself.

PostgreSQL is also opt-in: call `foundationfx.PostgresModule()` for each app,
provide `postgres.Config`, and consume `*sql.DB`. It verifies connectivity at
startup and closes the pool at shutdown. See the PostgreSQL section below.

```go
func run(ctx context.Context) (err error) {
	appCfg, err := config.Load[AppConfig](config.Options{Prefix: "ORDERS_"})
	if err != nil {
		return err
	}

	build := buildinfo.Current()
	cfg := telemetry.Config{Service: appCfg.Service, Environment: appCfg.Environment, Version: build.Version}
	sdk, err := telemetry.New(ctx, cfg)
	if err != nil {
		return err
	}

	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err = errors.Join(err, sdk.Shutdown(shutdownCtx))
	}()

	app := fx.New(
		foundationfx.Module,
		fx.Supply(appCfg, sdk),
		fx.Supply(logger.Config{
			Service: cfg.Service, Environment: cfg.Environment, Version: cfg.Version,
		}),
		fx.Supply(httpclient.Config{Timeout: appCfg.Timeout}),
		fx.Invoke(func(lifecycle fx.Lifecycle, log *logger.Logger) {
			lifecycle.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					log.Info(ctx, "service started", logger.String("commit", build.Commit), logger.String("build_time", build.BuildTime))
					return nil
				},
			})
		}),
	)
	if err := app.Err(); err != nil {
		return err
	}

	startCtx, cancelStart := context.WithTimeout(ctx, 10*time.Second)
	startErr := app.Start(startCtx)
	cancelStart()
	if startErr != nil {
		return startErr
	}

	select {
	case <-ctx.Done():
	case signal := <-app.Wait():
		if signal.ExitCode != 0 {
			err = fmt.Errorf("application stopped with exit code %d", signal.ExitCode)
		}
	}

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelStop()

	return errors.Join(err, app.Stop(stopCtx))
}
```

This example uses `context`, `errors`, `fmt`, `time`, `go.uber.org/fx`, and the module's
`buildinfo`, `config`, `foundationfx`, `logger`, `httpclient`, and `telemetry`
packages, plus `AppConfig` from the configuration example. Pass a context
from `signal.NotifyContext` in the service's main function. Add service constructors
with `fx.Provide` and register routes or lifecycle hooks with `fx.Invoke`. Business
constructors take normal dependencies such as `*logger.Logger`, `*http.Client`,
`trace.Tracer`, and `metric.Meter`; they do not need to take an SDK or use Fx types.

The service initializes telemetry before creating the Fx app and shuts it down
after stopping the app. This also cleans up exporters if dependency construction
or startup fails. Use explicit `Start`/`Stop` error handling as shown, so telemetry
cleanup is not bypassed by process exit. The module borrows the SDK and never
shuts it down. It registers an HTTP-client `OnStop` hook to close idle connections;
service hooks should stop active work before that cleanup runs.

Fx supplies `context.Context` to lifecycle hooks; request contexts still flow
through normal method parameters. Put background work and server startup in
`OnStart`, not in constructors. Fx validates the consumed graph during app
construction; missing dependencies and constructor errors appear through `app.Err()`.
Its internal framework events retain Fx's default logging; application logs use
our injected logger.

The module supplies one unqualified HTTP client. For multiple internal/external
clients, register service-owned factories with `fx.Annotate`/named results and
call `httpclient.New` with each upstream's config. Existing packages have no Fx
imports and remain usable with manual constructor injection.

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

The service owns logger creation and injection. Use `telemetry.New` above to
configure exporters and manage their lifecycle, or supply a provider managed by
the service. `logger.New` can also be used alone; JSON output works without
initializing telemetry. The logger itself opens no collector connection.

`Config.OnError` receives synchronous output failures and defaults to the OTel
error handler (`otel.Handle`). It must be safe for concurrent calls and must not
log recursively through this logger. A JSON write failure does not prevent OTel
delivery. Asynchronous exporter failures use OTel's error handling instead.

## HTTP clients

Create and reuse a client for each upstream configuration. Both internal services
in Proxmox and third-party APIs use `httpclient.New`; the service passes in its own
logger. The returned value is a standard `*http.Client` and uses standard
`*http.Request` and `*http.Response` values.

```go
client, err := httpclient.New(httpclient.Config{
	Name:                  "orders",
	Logger:                log.With(logger.String("upstream", "orders")),
	Timeout:               10 * time.Second,
	PropagateTraceContext: true,
})
if err != nil {
	return err
}

defer client.CloseIdleConnections() // At service shutdown, after active requests stop.

req, err := http.NewRequestWithContext(ctx, http.MethodPost,
	"https://orders.internal/orders", strings.NewReader(`{"item":"book"}`),
)
if err != nil {
	return err
}

req.Header.Set("Content-Type", "application/json")
req.Header.Set("Authorization", "Bearer "+token)

resp, err := client.Do(req)
if err != nil {
	return err
}

defer resp.Body.Close()

// Check resp.StatusCode and read/decode resp.Body here.
```

This example also uses `net/http`, `strings`, and this module's `httpclient`
package. Always use `http.NewRequestWithContext` to carry the operation's context
and cancellation into the call. Read and close response bodies so connections
can be reused and HTTP spans complete.

### Internal and external calls

Create clients after initializing telemetry when using it. The official
`otelhttp` transport records outgoing spans and HTTP metrics using the configured
global OTel providers. The client also works without telemetry initialization.

`PropagateTraceContext` defaults to false: outgoing spans and metrics are still
recorded, but the client does not automatically inject trace headers or baggage.
Enable it for trusted internal services to continue distributed traces. Leave it
false for third-party APIs unless you want to share that context. Headers added
explicitly to a request are preserved, including explicitly supplied trace headers.

Timeout defaults to 30 seconds and covers connection setup, redirects, all retry
attempts, backoff waits, and reading the response body. Negative timeouts are
rejected. The client keeps Go's standard redirect policy and status-code behavior:
the final 4xx/5xx response is returned with its body readable, not converted into
an error merely because retries were exhausted.

### Retries

Retries are enabled by default through `go-retryablehttp`. With `Config.Retry`
omitted, its base settings apply: up to four retries after the initial attempt,
exponential backoff starting at one second and capped at 30 seconds, and
`Retry-After` handling. The overall client timeout or earlier request deadline
can stop the operation before that limit. A server's `Retry-After` can exceed
`WaitMax`; the overall deadline still bounds the wait.

The SDK conservatively retries only GET, HEAD, OPTIONS, and TRACE. A body must be
absent or replayable through `Request.GetBody`, and requests with trailers are
sent only once. POST, PUT, PATCH, DELETE, and other methods are not retried by the
SDK, even if an idempotency header is present. Header presence alone cannot prove
that an upstream implements deduplication. The standard transport may still
perform its own eligible connection retries.

For eligible requests, HashiCorp's default policy retries transient connection
errors, 429, and 5xx except 501. Certificate-verification failures, canceled
contexts, and expired deadlines are not retried. Each actual attempt goes through
our curl logger and OTel transport, producing its own DEBUG event and client span
under the original parent. Retry-library logging is disabled to avoid a separate
logger or duplicate messages.

Configure retry limits per client:

```go
client, err := httpclient.New(httpclient.Config{
	Logger: log,
	Retry: &httpclient.RetryConfig{
		MaxRetries: 2,
		WaitMin:    200 * time.Millisecond,
		WaitMax:    2 * time.Second,
	},
})
if err != nil {
	return err
}
```

When `Retry` is non-nil, `MaxRetries` is explicit: zero disables retries for that
client. Zero `WaitMin`/`WaitMax` retain the library defaults; positive values
override them. Negative limits or waits, and an effective maximum wait below the
minimum, are rejected. For example, `Retry: &httpclient.RetryConfig{MaxRetries: 0}`
disables the retry layer. Non-replayable streaming uploads are never pre-buffered
to make them retryable; they retain single-attempt behavior.

### Supplying your own client

Pass an existing `*http.Client`, including `httptest.Server.Client()`:

```go
client, err := httpclient.New(httpclient.Config{
	Logger: log,
	Client: server.Client(),
	Retry:  &httpclient.RetryConfig{MaxRetries: 0},
})
if err != nil {
	return err
}
```

The SDK copies the client configuration and preserves its redirect callback,
cookie jar, and nonzero timeout. `Config.Timeout` overrides that timeout when
positive; otherwise a client without a timeout receives the 30-second default.
Standard `*http.Transport` values are cloned, preserving test-server/private-CA
TLS trust and avoiding mutation of the original transport. Custom RoundTrippers
are reused, so their concurrency and lifecycle behavior still apply. Cookie jars
are shared as configured. An explicit `Config.Transport` takes precedence over
the supplied client's transport.

The selected transport is wrapped with instrumentation, curl logging, and safe
retries. Redirects and cookies are handled by the outer client, and the retry
layer shares its request deadline, so a retry does not reset the timeout budget.

TLS verification is enabled by default for internal and external calls. For
private Proxmox CAs, pass a `Config.Transport` with the appropriate
`TLSClientConfig.RootCAs`; configure `Certificates` for mutual TLS when required.
Use CA trust instead of disabling certificate verification. The transport is
cloned, retains standard connection pooling and proxy support, and its idle
connections are closed through the returned client's `CloseIdleConnections`.

### Client authentication

Set one of `Config.APIKey`, `Config.BearerToken`, or `Config.TokenProvider` to
attach credentials to every request made through that client. `APIKey` sends
`X-API-Key`; bearer settings take a raw token without the `Bearer ` prefix.
These settings also work without Fx. Request headers are copied before adding
credentials; the caller's request is not modified. With no configured auth,
services can continue setting credentials on individual requests.

`TokenProvider` has signature `func(context.Context) (string, error)`. It is
called once per request or redirect hop, before retries, so attempts for that hop
use the same token. It must be safe for concurrent calls and honor cancellation;
the client's total timeout includes the lookup. Refreshing and caching tokens
remain service-owned. Provider failures and panics return a generic credential
error; cancellation/deadline errors retain their causes. Failed lookups send no
HTTP request. API keys are limited to 4 KiB and bearer tokens to 16 KiB; values
must contain visible ASCII without whitespace or commas.

Combining credential sources, or combining configured auth with a request's
`Authorization`, `X-API-Key`, or URL user information, is rejected. Configured
credentials apply to every initial URL passed to the client: use each client
only for its intended upstream. Authenticated clients follow redirects only
within the original scheme, host, and port. A redirect elsewhere returns its
response and readable body without following it. Supplied redirect callbacks
still run, but cannot opt into another origin. Clients without configured auth
retain Go's normal redirect behavior.

Configured credentials appear in the existing full DEBUG curl logs, including
each retry attempt. TLS verification, trace propagation settings, request
timeouts, and safe retry rules continue to apply.

### Multiple named HTTP clients in Fx

Use one named module and matching named config per upstream. Each provides a
standard `*http.Client` under that Fx value name:

```go
type UpstreamClients struct {
	fx.In

	Orders    *http.Client `name:"orders"`
	Inventory *http.Client `name:"inventory"`
}

app := fx.New(
	foundationfx.Module,
	fx.Supply(logger.Config{Service: "checkout"}),
	foundationfx.NamedHTTPClientModule("orders"),
	foundationfx.NamedHTTPClientModule("inventory"),
	fx.Supply(
		fx.Annotate(httpclient.Config{
			Timeout: 2 * time.Second,
			APIKey: cfg.OrdersAPIKey,
			PropagateTraceContext: true,
			Retry: &httpclient.RetryConfig{
				MaxRetries: 2,
				WaitMin: 100 * time.Millisecond,
				WaitMax: time.Second,
			},
			CircuitBreaker: &httpclient.CircuitBreakerConfig{
				FailureThreshold: 3,
				OpenTimeout: 20 * time.Second,
			},
		}, fx.ResultTags(`name:"orders"`)),
		fx.Annotate(httpclient.Config{
			Timeout: 5 * time.Second,
			TokenProvider: inventoryToken, // Service-owned token lookup.
			PropagateTraceContext: true,
			Retry: &httpclient.RetryConfig{MaxRetries: 0},
		}, fx.ResultTags(`name:"inventory"`)),
	),
	fx.Provide(func(clients UpstreamClients) *CheckoutService {
		return NewCheckoutService(clients.Orders, clients.Inventory)
	}),
	fx.Invoke(func(*CheckoutService) {}),
)
app.Run()
```

`CheckoutService`, its constructor, runtime `cfg`, and `inventoryToken` belong
to the service. Alternatively, annotate a service-owned constructor with
``fx.ParamTags(`name:"orders"`)`` to consume one client, or annotate a config constructor
with `fx.ResultTags` instead of supplying the config directly.

Names are case-sensitive, nonblank, and cannot contain backquotes. Duplicate
names fail graph construction. An unused named module needs no config or logger;
a consumed named client requires its matching named config and never falls back
to the unnamed config. Supply a named `httpclient.Config{}` to retain all base
timeout/retry defaults. The shared app logger is injected unless `Config.Logger`
supplies an override. Each config can also supply its own base `Client`, including
an `httptest` client, or TLS `Transport`.

The unnamed client from `foundationfx.Module` remains available alongside named
clients. Each named binding is constructed lazily, opens no startup connection,
and closes its idle connections on Fx shutdown, graph errors, or startup rollback.
Call the module factory once per name per app; create fresh options for each app.
Active requests and response bodies remain service-owned. Standard transports
are cloned independently; custom transports and cookie jars are shared if the
service explicitly reuses them between configs.
`CircuitBreaker` is also configured independently per named client; see
[client circuit breakers](#client-circuit-breakers) for failure and recovery rules.

### Replayable curl debug logs

Set the supplied logger's `Level` to `logger.LevelDebug`. Each outgoing request
then produces an `http request` event with `method`, `url`, and `curl` fields.
When DEBUG is disabled, no curl command is constructed and no body is copied for
logging. `Logger.Enabled(ctx, level)` is also available for other expensive
diagnostic work.

The command includes the full URL, headers, credentials, cookies, and body, with
shell quoting that keeps request values as data. Bodies containing binary bytes
or a leading `@` use a base64 pipeline to preserve their bytes. Commands target
POSIX shells; binary replay also requires the `base64` utility. Extract the raw
command from newline-delimited JSON logs with `jq -r '.fields.curl // empty'`
rather than copying JSON escape sequences into a shell.

Replayable bodies are read through their independent `GetBody` function without
consuming the outgoing body. Streaming bodies without `GetBody` are captured as
the transport sends them, so logging does not pre-drain or delay the upload until
it finishes. Their debug event may arrive after the HTTP response. Each redirect
request is logged separately, after trace-header injection.

Commands reproduce application-level method, URL, headers, and body. HTTP framing,
TLS negotiation, proxy settings, custom DNS/dial behavior, and TLS client keys are
not embedded in the command. Run it from a machine with equivalent network access
and CA trust, adding curl's TLS/proxy options when necessary. The command does not
follow redirects automatically; each logged command represents one request hop.

If body capture fails, an upload is interrupted, or the request uses trailers,
the event contains `curl_error` instead of a misleading partial curl command.
Logging failures do not fail or replace the HTTP request. Curl logging captures
the whole body without truncation and buffers it in memory, so DEBUG has a memory
cost for large uploads. Full credentials and body values also reach the configured
log collector; enable DEBUG only where that storage and access are appropriate.

## gRPC clients

Use a reusable `grpcclient.Client` for each trusted internal upstream. It
implements the standard `grpc.ClientConnInterface` required by generated
protobuf clients while keeping the underlying connection private. No new
protobuf contract or custom code generation is required.

```go
channel, err := grpcclient.New(grpcclient.Config{
	Name: "orders",
	Target: cfg.OrdersGRPCTarget,
	Logger: log,
	APIKey: cfg.OrdersAPIKey,
	Timeout: 10 * time.Second,
})
if err != nil {
	return err
}

defer channel.Close()

orders := orderspb.NewOrdersClient(channel)
callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
defer cancel()

response, err := orders.GetOrder(callCtx, request)
if err != nil {
	return err
}

_ = response // Use your service-owned response type.
```

This uses `context`, `time`, GOF's `grpcclient`, and the service's generated
`orderspb` package. `request`, application config, logger, and business context
are service-owned. Constructing the channel performs no network I/O; connection
establishment is lazy and RPC deadlines include connection waits. Generated
clients and channels can be reused concurrently. Stop/join application callers
before closing the channel, then shut down borrowed logging/telemetry providers.

### Client transport and timeouts

`Target` is required. Normal gRPC targets such as `orders.internal:9090`,
`dns:///orders.internal:9090`, and registered resolver targets are accepted.
Targets cannot contain credentials, query strings, fragments, or whitespace.
Supply credentials in dedicated fields instead. gRPC's native name resolution,
connection reuse/reconnection, and load-balancing behavior remain in effect.

TLS certificate/hostname verification is enabled by default, using system
roots and a TLS 1.2 minimum. Set `TLS: &tls.Config{RootCAs: internalRoots}` for a
private CA; standard client certificates and callbacks support mutual TLS.
Custom TLS settings are cloned, with certificate/root pools and callbacks
remaining caller-owned and concurrency-safe as required by Go TLS.

For a trusted plaintext network, explicitly set `Insecure: true`; this cannot
be combined with `TLS`. Configure matching transports on the server and client.
The GOF server's default plaintext listener requires this explicit client option
unless TLS terminates at a trusted ingress. Credential metadata is not encrypted
on a plaintext hop. There is no automatic fallback from failed TLS to plaintext.

`Timeout` defaults to **30s for the whole unary RPC**, including credential
lookup, connection waits, and response decoding. `StreamTimeout` defaults to
**5m for the complete stream**, including credential lookup, establishment,
blocked sends/receives, and all response messages. Earlier caller deadlines win;
negative durations are rejected. Configure a longer stream lifetime where needed,
and cancel streams when abandoning them. `CloseSend` alone does not finish a
bidirectional/server response stream: receive to EOF/error or cancel the context.
Stream completion releases its timeout timer through gRPC's finish callback;
GOF adds no watcher goroutine per stream.

`MaxRecvMessageBytes` and `MaxSendMessageBytes` default to 4 MiB each. Standard
per-call `grpc.MaxCallRecvMsgSize`/`grpc.MaxCallSendMsgSize` options may override
these defaults. Standard generated-client call options and remote status codes
remain available. Direct `Invoke`/`NewStream` calls require a valid protobuf
full-method path; use generated clients or fixed method names, never arbitrary
untrusted strings as RPC methods.

GOF disables application retries and hedging, including retry policies supplied
by resolver service configuration. It cannot infer whether a generated method
is safe to repeat. Native gRPC transparent retries can still happen before a
call reaches the application or when the server reports it unprocessed. A
deadline or connection failure does not undo a remote business operation;
services own idempotency and any deliberate retry policy.

### RPC credentials and metadata

Configure at most one of:

- `APIKey`: sends `x-api-key` metadata.
- `BearerToken`: sends a fixed raw token as `authorization: Bearer <token>`.
- `TokenProvider func(context.Context) (string, error)`: obtains a fresh/cached
  raw bearer token for each unary RPC or stream establishment.

For rotating service tokens, use a concurrency-safe provider that honors its
context and owns token acquisition/caching. Credential lookup shares the RPC's
deadline, and channel closure cancels pending lookups. Provider errors, panic
values (including `panic(nil)`), and invalid/empty tokens are replaced with a
generic Unauthenticated status; cancellation/deadline/closed-channel outcomes
retain their appropriate codes. GOF does not issue tokens, perform OAuth login,
or automatically retry credential lookup. Stream credentials are attached once,
not refreshed during that stream's lifetime.

If no credential source is configured, callers may supply a single credential
through outgoing metadata using `metadata.Pairs`, `Set`, or `Add`. Non-auth
outgoing metadata is preserved and copied. Duplicate/mixed auth metadata, malformed
credentials, or caller auth metadata combined with configured credentials fail
before an RPC is sent. Use these fields or outgoing metadata as the auth source;
do not combine them with separate native `PerRPCCredentials` call options.
Incoming request metadata/credentials are **not forwarded automatically**.

Initialize `telemetry.New` before creating clients. Official gRPC OTel
instrumentation injects global trace context/baggage, records client spans and
RPC duration metrics, and connects calls to GOF server spans. Propagation is
enabled for this internal client; ensure baggage is appropriate for the target.
Local credential/method validation failures do not send a network RPC.

GOF logs completed RPCs using target, full method, status code, duration, and
trace context. It does not log payloads, credential values, metadata, provider
errors, or raw RPC error messages. Native OTel status descriptions can contain
the remote service's explicit gRPC status messages, so keep those messages free
of secrets. Closing a client never shuts down its borrowed logger or global
telemetry providers.

### Client testing, health, and Fx

`Dialer` accepts `func(context.Context, string) (net.Conn, error)` for private
networks or in-memory `bufconn` tests. It must honor cancellation. This preserves
the SDK's auth, timeout and telemetry pipeline instead of bypassing it with a
preconstructed connection.

`Ping(ctx)` waits for a ready transport under `Timeout` and the caller's deadline.
It checks connection/TLS establishment, not business readiness or RPC permissions.
Register `health.Optional("orders_rpc", channel.Ping)` for transport diagnostics,
or explicitly call the upstream's generated health client to check SERVING.
The channel may keep its shared connection attempts alive after a Ping wait
ends; `Close` terminates them. Close is idempotent and cancels active streams
and pending credential lookups. Later RPCs return `grpcclient.ErrClosed`
(Unavailable), also recognizable with `errors.Is`.

```go
func ordersClient(channel *grpcclient.Client) orderspb.OrdersClient {
	return orderspb.NewOrdersClient(channel)
}

app := fx.New(
	foundationfx.Module,
	foundationfx.GRPCClientModule(),
	fx.Supply(logger.Config{Service: "billing"}),
	fx.Supply(grpcclient.Config{
		Target: cfg.OrdersGRPCTarget,
		APIKey: cfg.OrdersAPIKey,
	}),
	fx.Provide(ordersClient, NewBillingService),
)
```

`GRPCClientModule()` is opt-in and lazy, supplies one `*grpcclient.Client`, injects
the application logger, and closes the channel on shutdown, construction failure,
or startup rollback. Call the module factory once per Fx app so ownership state
is not shared. An unused module needs no config and creates no channel. It makes
no startup connection/health check, avoiding dependency cycles between services
starting together. Register any required readiness/startup checks explicitly.

### Multiple named gRPC clients in Fx

Use `NamedGRPCClientModule(name)` for each upstream. Each module consumes a
`grpcclient.Config` and provides a `*grpcclient.Client` with the **same Fx name**.
Targets, auth sources, unary/stream timeouts, TLS settings, and connection
ownership remain independent. The existing unnamed `GRPCClientModule()` can
coexist with named modules.

```go
app := fx.New(
	foundationfx.Module,
	foundationfx.NamedGRPCClientModule("orders"),
	foundationfx.NamedGRPCClientModule("inventory"),
	fx.Supply(logger.Config{Service: "billing"}),
	fx.Supply(
		fx.Annotate(grpcclient.Config{
			Target: cfg.OrdersGRPCTarget,
			APIKey: cfg.OrdersAPIKey,
			Timeout: 2 * time.Second,
			StreamTimeout: 30 * time.Second,
			CircuitBreaker: &grpcclient.CircuitBreakerConfig{
				FailureThreshold: 5,
				OpenTimeout: 30 * time.Second,
			},
		}, fx.ResultTags(`name:"orders"`)),
		fx.Annotate(grpcclient.Config{
			Target: cfg.InventoryGRPCTarget,
			TokenProvider: inventoryTokens.AccessToken,
			Timeout: 5 * time.Second,
			StreamTimeout: 2 * time.Minute,
		}, fx.ResultTags(`name:"inventory"`)),
	),
	fx.Provide(
		fx.Annotate(func(channel *grpcclient.Client) orderspb.OrdersClient {
			return orderspb.NewOrdersClient(channel)
		}, fx.ParamTags(`name:"orders"`)),
		fx.Annotate(func(channel *grpcclient.Client) inventorypb.InventoryClient {
			return inventorypb.NewInventoryClient(channel)
		}, fx.ParamTags(`name:"inventory"`)),
		NewBillingService, // Receives the generated OrdersClient and InventoryClient.
	),
)
```

This uses normal Fx annotations; `inventoryTokens.AccessToken` is a service-owned,
concurrency-safe token provider. For configs built through DI instead of supplied
values, annotate the config constructor with the corresponding `fx.ResultTags`.
The shared application logger is injected unless a client's config supplies its
own logger. Verified TLS remains the default; configure each target's private CA
or explicitly select plaintext for a trusted network as described above.

To receive multiple raw channels directly, use standard Fx parameter objects:

```go
type RPCClients struct {
	fx.In

	Orders    *grpcclient.Client `name:"orders"`
	Inventory *grpcclient.Client `name:"inventory"`
}
```

Named bindings stay lazy and make no startup connection check. A consumed client
requires its matching named config; an unnamed config is never a fallback.
Duplicate names fail graph construction. Names must be nonblank and cannot
contain backquotes; matching is exact and case-sensitive. Use a fresh module
factory call once per name per app. No client registry or name lookup API is
required.

Each constructed channel closes on normal shutdown, graph failure, or startup
rollback. Closing one manually does not close the others. Keep each target's
credentials in its own config; no global auth or timeout setting is applied to
the named clients.

## Client circuit breakers

Both `httpclient.Config` and `grpcclient.Config` accept an optional
`CircuitBreaker` using their package's own `CircuitBreakerConfig` type. The
implementation is shared internally and services import no breaker library.
These settings work with manual construction, unnamed Fx clients, and named Fx
clients. The named examples above show different thresholds and open timeouts.

Nil disables the breaker, preserving existing behavior. A non-nil empty config
enables defaults: five consecutive availability failures and a 30-second open
timeout. Override `FailureThreshold` and `OpenTimeout` per client; zero fields
retain defaults and negative fields are rejected during construction.

When the threshold is reached, subsequent calls fail immediately without auth
lookups, HTTP attempts, or RPCs. After the open timeout, one call is admitted as a
recovery probe. A successful probe closes the circuit, a failed probe starts a
new open timeout, and an ignored probe (quota, cancellation, or local failure)
releases its slot for another probe without extending the timeout. Other calls
fail fast while the probe is in flight. In-flight calls are not canceled when a
circuit opens, and their old results cannot change a newer circuit generation.
No background probe runs.

State belongs to each constructed client within its process. Named clients
never share state, even if their targets or config pointers match. A client's
state covers all its methods and destinations, so keep using one HTTP client
per upstream. Each service replica has its own circuit state; restarting or
recreating a client resets it. `CloseIdleConnections` does not reset a circuit.

### HTTP failure policy

Each request/redirect hop counts once after its retry sequence finishes.
Transport errors, including network/deadline failures, and all 5xx responses
count as failures. HTTP 429 neither increments nor resets consecutive failures,
so one user's quota cannot open the shared upstream circuit. Other HTTP responses
reset consecutive failures. Explicit cancellation and local credential errors,
including a deadline during token lookup, also leave the count unchanged. The
triggering response is returned normally with its body readable. A deadline while
waiting to retry a 429 is also ignored; a subsequent attempt's network failure or
timeout still counts as an availability failure. Classification happens at
response headers; later body-reading errors remain caller-owned and do not affect
the breaker. A recovery probe retains the configured safe retry policy.

Check rejection using `errors.Is(err, httpclient.ErrCircuitOpen)`;
`http.Client.Do` wraps it in a standard `*url.Error`. Rejection closes the
unsent request body and performs no retry. Actual attempts retain their curl
logs, tracing, and metrics; rejected calls create no network-attempt telemetry.

### gRPC failure policy

Completed RPCs with `Unavailable`, `Internal`, `Unknown`, `DataLoss`, or
`DeadlineExceeded` count as availability failures. `ResourceExhausted` is ignored
because it can describe one user's quota or a message size limit. It neither
increments nor resets consecutive failures. Other completed application statuses
reset consecutive failures. Local validation/auth errors, explicit cancellation,
and client closure are ignored.
Request/stream deadlines reached during an RPC count as failures, including
earlier caller deadlines. Deadlines already expired before admission are ignored.

Unary calls count at completion. Streams count when gRPC reports their final
result, not when the stream is created or receives its first response. A recovery
stream holds the probe for its entire lifetime under `StreamTimeout`; receive to
completion or cancel it. Client, server, and bidirectional streams keep the same
generated protobuf API. No gRPC application retries are added.

Check `errors.Is(err, grpcclient.ErrCircuitOpen)`. It also has gRPC status
`Unavailable`, while the actual failure that opened the circuit retains its
original status. Closing the client and already-canceled contexts retain their
normal errors. `Ping` bypasses the circuit because it checks transport readiness,
not application availability, and neither opens nor resets the circuit. Actual
RPCs retain their logs, traces, and metrics; rejection creates no RPC attempt.

### Circuit breaker telemetry

Enabled breakers emit OpenTelemetry counters under instrumentation scope
`go-foundation/circuitbreaker`:

- `circuit_breaker.transitions` counts actual state changes, with `from_state`
  and `to_state` values `closed`, `open`, or `half_open`.
- `circuit_breaker.rejections` counts each call rejected by an open circuit or
  active recovery probe, tagged with its rejection-time `state`.

Both include `client.name` and `client.type` (`http` or `grpc`). Named Fx modules
set the client name to the exact binding name, overriding `Config.Name` for that
binding. No extra telemetry wiring is needed in the service. For manually
constructed clients or an unnamed Fx client, set `Config.Name` to a stable,
unique upstream name; an empty name is reported as `default`. Avoid names derived
from request IDs or URLs so metric dimensions remain bounded. Clients sharing
the same name/type within a process have their counters combined.

The supplied/injected GOF logger emits `circuit breaker state changed` with
`from_state` and `to_state`, and `circuit breaker call rejected` with `state`.
Both include `client_name`, `client_type`, and a per-client `generation`.
Transitions to `open` and all rejections use WARN; transitions to `half_open` or
`closed` use INFO. Each rejected call produces one event when that level is
enabled; log filtering does not disable the counters. Events use the call's
context for trace correlation and reach both JSON output and the logger's OTel
log provider. They add no request headers, bodies, raw errors, or target URLs.

Only actual transitions are recorded: construction in `closed`, ordinary
successful calls, duplicate completions, and results from an earlier circuit
generation do not create transition events. Canceled/local-failure recovery
probes still produce their actual `half_open` to `open` transition. Events are
emitted after releasing the circuit lock. Concurrent events can arrive out of
order; use `generation` to order transitions for one constructed client. The
generation is not a metric dimension. There are no synthetic HTTP/RPC attempts
or spans for rejected calls; the dedicated rejection telemetry records them.

Initialize telemetry before constructing clients or the Fx app. Breakers use
the configured global OTel meter provider, matching the HTTP/gRPC instrumentation;
the service continues to own provider flushing and shutdown. Disabled breakers
create no circuit instruments or events. No exporter connection is opened by
client construction.

## gRPC servers

Services own their `.proto` contracts and generated Go handlers. GOF owns the
listener, unary/stream middleware, request logging, OpenTelemetry, limits,
optional authentication, standard gRPC health service, and shutdown. The raw
`*grpc.Server` stays private. Generated registration uses the standard
`grpc.ServiceRegistrar` interface, which lets services keep normal protobuf
tooling and clients.

```go
server, err := grpcserver.New(grpcserver.Config{
	Logger: log,
	Address: ":9090",
	Register: []func(grpc.ServiceRegistrar){
		func(registrar grpc.ServiceRegistrar) {
			orderspb.RegisterOrdersServer(registrar, ordersHandler)
		},
	},
	Auth: gate,
	RequireScopes: map[string][]string{
		orderspb.Orders_GetOrder_FullMethodName: {"orders:read"},
	},
})
if err != nil {
	return err
}

if err := server.Start(ctx); err != nil {
	return err
}

// Observe server.Wait() for unexpected listener failures. During service
// shutdown, use a fresh context to drain RPCs before closing dependencies.
if err := server.Shutdown(shutdownCtx); err != nil {
	return err
}
```

This uses `google.golang.org/grpc`, `github.com/Penthious/go-foundation/grpcserver`, and the service's
generated `orderspb` package. `gate` is a GOF `*auth.Authenticator`; handlers get
their authenticated principal with `auth.FromContext(ctx)`, including the context
returned by a generated stream's `Context()` method.

`New` validates and copies registrations/policies without opening a listener or
constructing the gRPC runtime. Call only generated registration functions inside
`Register`; do not retain the registrar or start background work there. At least
one application service is required. Duplicate/reserved services, incorrect or
nil handler implementations, invalid methods, and registration panics return
constructor errors instead of gRPC's process-terminating registration errors.
Keep handler dependencies and generated descriptors valid and immutable.

`Start` binds synchronously, so an occupied address fails startup. Repeated
successful calls reuse the listener. `Address()` returns the actual bound address
(use `127.0.0.1:0` in tests), and `Wait()` reports listener termination/errors.
Unsuccessful binds create no gRPC runtime. The default listener address is
`:9090`; a stopped server cannot restart.

### RPC authentication and errors

With `Config.Auth` set, all registered application methods require credentials
unless explicitly listed in `PublicMethods`. With no authenticator, methods are
public; method policies without an authenticator fail construction. Policies use
exact `/package.Service/Method` names; generated full-method constants avoid typos.
Unknown names, invalid scopes, and methods listed as both public and scoped are
rejected. A required-scope list uses exact matching and requires every scope.

Clients send either `authorization: Bearer <access-token>` or
`x-api-key: <secret>` in gRPC metadata. Missing, invalid, duplicate or mixed
credentials produce `codes.Unauthenticated`; insufficient scopes produce
`codes.PermissionDenied`. Both use the existing auth package's validation and
GOF identities. No method handler runs after failed authorization.

Streaming authentication happens once when a stream opens. Tokens are not
revalidated during that stream's lifetime; use appropriate client deadlines and
application stream-lifetime policies. Service-specific tenant/ownership checks
remain in handlers. Unary, server-streaming, client-streaming, and bidirectional
RPCs share these policies.

Panics, including `panic(nil)`, return `codes.Internal` with a generic message.
Plain Go errors also become generic Internal errors. Return explicit
`status.Error(codes.InvalidArgument, "safe client message")` or equivalent
to define a public error; explicit gRPC statuses/details are preserved.
Context cancellation and deadline errors become their matching gRPC codes.
Keep explicit status messages free of secrets: clients and OTel status records
can see them. GOF request logs include method, code, duration, and trace context,
without request/response bodies, metadata, credentials, or raw error messages.

Other transports can use `gate.Authenticate(ctx, headers)` to reuse credential
validation and receive an authenticated context; it returns
`auth.ErrUnauthenticated` on failure and does not perform scope checks. Map only
the credential metadata to standard `http.Header` values using `Set`/`Add`;
`grpcserver` performs this mapping automatically.

### Limits, telemetry, and TLS

`UnaryTimeout` defaults to 30s; earlier client deadlines win. Handlers must honor
cancellation and return. Streams follow their original client deadlines and
handler lifecycle; GOF does not impose a context-only stream timeout that could
leave blocked receives running. Set client deadlines when opening finite streams
and handle stream cancellation/errors in the service.

`MaxRecvMessageBytes` and `MaxSendMessageBytes` each default to 4 MiB.
`MaxHeaderBytes` defaults to 64 KiB of metadata, `MaxConcurrentStreams` to 100
per connection, and `ConnectionTimeout` to 5s for connection handshakes. Exceeding
message limits returns `codes.ResourceExhausted`; outbound limit failures can
occur after handler business work has already completed. Stream limits are per
connection, not a process-wide request quota.

Initialize `telemetry.New` before constructing/starting the server. Official
OTel gRPC instrumentation propagates incoming trace context and records server
spans and RPC duration metrics for registered methods, including failed calls.
Request bodies and metadata are not recorded. Unknown methods are logged under
the fixed name `unknown` and excluded from RPC tracing/metrics to avoid arbitrary
client paths becoming high-cardinality labels. The package borrows global OTel
providers and never shuts them down.

`Config.TLS` accepts a standard `*tls.Config`, cloned before use; certificate/root
pools and callbacks remain caller-owned and must be concurrency-safe/immutable
as required by Go TLS. Certificates or a certificate callback are required;
the default minimum TLS version is 1.2. Use normal verified TLS, optionally with
client-certificate verification configured through `ClientAuth` and `ClientCAs`.
Without `TLS`, the listener is plaintext, matching the HTTP-server model for a
trusted ingress/internal network. Protect credential transport accordingly.

Reflection is disabled by default. Set `Reflection: true` only when service
discovery by clients/tools is desired. When auth is enabled, both reflection
protocol versions require authentication unless their full method names are
explicitly public. Register your own application services only; GOF reserves
the standard health and reflection service names.

### gRPC health and shutdown

GOF implements standard `grpc.health.v1.Health` **Check**, **List**, and **Watch**.
The empty service name represents the whole server; each registered service has
the same readiness result. A listening server with healthy required dependencies
reports SERVING. Optional dependencies do not affect readiness, and dependency
details are not exposed in health responses. An unknown Check returns NotFound;
an unknown Watch reports SERVICE_UNKNOWN.

Supply the existing `*health.Checker` as `Config.Health`. Checks run on demand;
Watch refreshes under `HealthTimeout` (default 2s) every `HealthWatchInterval`
(default 5s), emitting only status changes. Callbacks must honor cancellation.
Health Check/Watch remain public for probes, including when auth is enabled.
List follows the normal method auth policy. Restrict probe exposure at ingress
when needed.

`server.Ping(ctx)` checks listener readiness only. Add
`health.Required("grpc", server.Ping)` to your HTTP health checker to show the
listener in `/health` and the embedded Health UI. It deliberately avoids calling
dependencies again, so sharing the checker does not create recursive checks.
The existing HTTP health endpoints retain build information and the full
dependency breakdown; standard gRPC health responses carry serving status.

`Shutdown(ctx)` marks the server unready, ends health watches, and drains active
RPCs. A deadline force-closes connections and cancels contexts, then returns the
deadline error. Go cannot forcibly terminate a noncooperative handler: dependencies
may still be in use after a shutdown timeout. Keep them alive and call Shutdown
again to join once handlers return. Successful Shutdown joins drained handlers;
Wait alone only observes listener termination.

### gRPC in Fx

```go
func rpcConfig(handler *OrdersHandler, gate *auth.Authenticator) grpcserver.Config {
	return grpcserver.Config{
		Address: ":9090", Auth: gate,
		Register: []func(grpc.ServiceRegistrar){func(registrar grpc.ServiceRegistrar) {
			orderspb.RegisterOrdersServer(registrar, handler)
		}},
		RequireScopes: map[string][]string{
			orderspb.Orders_GetOrder_FullMethodName: {"orders:read"},
		},
	}
}

app := fx.New(
	foundationfx.Module,
	foundationfx.AuthModule,
	foundationfx.GRPCServerModule,
	fx.Supply(logger.Config{Service: "orders"}),
	fx.Supply(authConfig), // Service-owned auth.Config loaded from environment/secrets.
	fx.Provide(NewOrdersHandler, rpcConfig),
	// Optionally provide your shared *health.Checker.
)
```

`GRPCServerModule` is opt-in, injects the application logger and an optional
shared health checker, and starts/stops the listener with Fx. An explicitly
configured checker takes precedence. Auth remains explicit in the config
constructor. Handler/config dependencies start before the server and stop after
it. Later startup failures drain the listener during rollback; unexpected serving
errors request Fx shutdown with exit code 1. Use the explicit Start/Wait/Stop
application runner pattern shown earlier so those shutdown requests trigger
cleanup. HTTP and gRPC listeners can run together on separate addresses.

## Scheduling

Services register `scheduler.Job` values with a GOF scheduler. Each job chooses
independent local execution or PostgreSQL coordination; both can run in the same
process. The cron parser stays private. Services own job handlers and business
rules; GOF owns scheduling, timeouts, concurrency, logs, metrics, and shutdown.

```go
jobs, err := scheduler.New(scheduler.Config{
	Namespace: "orders",
	Logger: log,
	DB: db, // Borrowed *sql.DB; omit when every job is local.
}, scheduler.Job{
	Name: "refresh-local-state",
	Mode: scheduler.Local,
	Every: 5 * time.Minute,
	Timeout: 30 * time.Second,
	Handler: func(ctx context.Context, run scheduler.Run) error {
		return refreshLocalState(ctx)
	},
}, scheduler.Job{
	Name: "daily-reconciliation",
	Mode: scheduler.Coordinated,
	Cron: "0 9 * * *",
	Timezone: "America/Denver",
	Timeout: 10 * time.Minute,
	Handler: func(ctx context.Context, run scheduler.Run) error {
		return reconcileOrders(ctx, run.ID)
	},
})
if err != nil {
	return err
}

if err := jobs.Start(ctx); err != nil {
	return err
}

// On service shutdown, use a fresh shutdown context, cancel and join jobs,
// then close the database/logger/telemetry dependencies.
if err := jobs.Shutdown(shutdownCtx); err != nil {
	return err
}
```

This uses `context`, `time`, and GOF's `scheduler`; `db` is a standard `*sql.DB`.
The handler functions are service-owned. `New` validates and copies the jobs
without network I/O or goroutines. `Start` registers shared jobs transactionally
before starting workers; startup failures can be retried. Repeated successful
`Start` calls do not create extra workers. The startup context only bounds
startup; it does not become the lifetime context for job handlers.

### Schedules and execution

- `Name` must be unique within a scheduler. Names and the required `Namespace`
  use 1–128 ASCII letters, digits, dots, underscores or hyphens. Coordinate
  replicas using the same database, namespace, and job names.
- Choose exactly one of `Cron` or `Every`. Cron has five fields (minute, hour,
  day of month, month, day of week), with standard descriptors such as `@hourly`
  and `@daily`. Seconds and inline `@every`/`CRON_TZ` directives are not accepted;
  use the explicit fields instead.
- `Timezone` defaults to **UTC**; explicit IANA names work in minimal Docker
  images because the package includes Go's timezone data. OS-dependent `Local`
  is rejected. Calendar schedules follow the selected timezone and daylight
  saving rules, so a nonexistent local wall-clock time may be skipped.
- `Every` must be at least 1ms. Intervals align to fixed absolute time boundaries
  and do not drift from handler completion; timezone does not affect them.
  New jobs first run at their next occurrence, not immediately at startup.
- `Timeout` defaults to 5m, must be at least 1ms, and must fit a doubled lease.
  Handlers must honor their context, return, and join work they start. A timeout
  cannot forcibly terminate a Go function or an external effect.
- A local job does not overlap itself. `MaxConcurrentJobs` defaults to 10 per
  scheduler instance, capped at the number of jobs. There is no unbounded work
  queue: local occurrences are skipped when capacity is exhausted or while the
  same job is still running. A shared due job remains claimable by another
  replica, or is coalesced when capacity becomes available.
- Handler errors, panics (including `panic(nil)`), and timeouts are recorded;
  the next scheduled occurrence can still run. There is no automatic retry of
  the failed occurrence. Panic values are omitted; returned handler errors are
  logged, so keep secrets out of error messages.

`Run` provides namespace, job name, scheduled time in UTC, and an ID. Coordinated
run IDs are stable for the same namespace/name/occurrence across replicas. Local
IDs also include scheduler instance identity, so independent replicas do not
accidentally share an idempotency key. Use these IDs where useful, while keeping
business-specific idempotency and resource checks in the service.

### PostgreSQL coordination and migrations

Apply `scheduler.Schema()` using a service migration before `Start`, or enable
the built-in with `migrate.Config.SchedulerVersion` at a **new unused positive
version**. For example, if inbox/outbox/service/failure migrations already occupy
versions 1–4, add `SchedulerVersion: 5` while retaining those existing sources
and version assignments. This schema is forward-only. It is optional; local jobs
need neither PostgreSQL nor a migration. No scheduling API runs DDL automatically.

`public.gof_schedules` holds one row per namespace/job, including the normalized
schedule, timezone, timeout, next run time, and active lease. Registration uses
a stable lock order so replicas can start together. Existing rows retain their
next occurrence on restart. Replicas with conflicting schedule/timezone/timeout
configuration fail startup with `scheduler.ErrScheduleConflict`; they never
silently overwrite each other's definitions. Handler implementation changes do
not change the persisted definition.

Claims use PostgreSQL's clock and a short row-locking transaction. The winner
advances the next occurrence and commits **before** invoking the handler, so
another replica cannot duplicate a fast occurrence after the first finishes.
No SQL transaction or connection is held throughout the handler. A lease lasts
twice the job timeout, and release checks an ownership token so a stale worker
cannot release a replacement's lease. Leases are not renewed. Runtime database
errors pause shared claims and are logged/retried on the polling interval;
independent local jobs continue.

`PollInterval` defaults to 1s for each shared job; `StorageTimeout` defaults to 5s
per registration, claim, health query, or lease release. Both accept durations
of at least 1ms. Set polling appropriately for the interval and job count.
Global handler capacity is per instance; shared per-job leases coordinate across
replicas, but there is no cluster-wide limit on different jobs running together.

Missed local occurrences are skipped. Shared schedules coalesce overdue work
into one pending occurrence: after downtime or a long run, one overdue run can
execute, then the next time is calculated from the database's current clock.
There is no replay of every elapsed interval. `ScheduledAt` identifies that
pending occurrence and may be older than the actual start time.

The committed claim is not a durable execution queue. A process crash or
ambiguous claim response can lose that occurrence; it is not retried, and a
later due occurrence can run after the lease expires. A noncooperative handler
may still be executing when a replacement acquires an expired lease. External
effects must be idempotent; scheduling does not guarantee exactly-once effects.
Use the outbox/pubsub path for work that needs durable delivery after enqueue.

To change an existing coordinated definition, stop its replicas and wait for
handlers, then explicitly update or replace its row through service-owned SQL
before restarting with matching config. Do not clear active leases or assume a
renamed job shares the previous job's coordination. Removed job rows remain
until an operator removes them; there is no run-history table or automatic purge.

### Scheduling in Fx and health checks

Register jobs in the `foundation_jobs` Fx value group:

```go
func reconciliationJob(service *OrderService) scheduler.Job {
	return scheduler.Job{
		Name: "reconcile-orders", Mode: scheduler.Coordinated,
		Cron: "*/5 * * * *", Timeout: time.Minute,
		Handler: func(ctx context.Context, run scheduler.Run) error {
			return service.Reconcile(ctx, run.ID)
		},
	}
}

app := fx.New(
	foundationfx.Module,
	foundationfx.PostgresModule(),
	foundationfx.SchedulerModule,
	fx.Supply(logger.Config{Service: "orders"}),
	fx.Supply(postgres.Config{DSN: cfg.DatabaseURL}),
	fx.Provide(func(db *sql.DB) scheduler.Config {
		return scheduler.Config{Namespace: "orders", DB: db}
	}),
	fx.Provide(NewOrderService),
	fx.Provide(fx.Annotate(reconciliationJob,
		fx.ResultTags(`group:"foundation_jobs"`),
	)),
)
```

The scheduler module injects the application logger, starts registered jobs, and
cancels/joins them before their injected dependencies stop. Supply the database
through the scheduler config constructor for shared jobs. Local-only services
can omit PostgresModule and provide a config without `DB`. An empty job group
or invalid configuration fails construction; missing schemas or conflicting
shared definitions fail startup. Later startup-hook failures roll back the
scheduler by shutting it down.

Add `health.Required("scheduler", jobs.Ping)` (or `health.Optional`) to your
existing health checker to include it in `/health` and the embedded Health UI.
`Ping` checks lifecycle/storage, not whether recent business jobs succeeded.
Shutdown cancels handlers and waits for workers, including bounded lease cleanup.
If the shutdown deadline expires, handlers may still be running; keep their
dependencies alive and call `Shutdown` again after they finish.

Initialize telemetry before constructing the scheduler. Job spans carry the
handler context; `scheduler.runs` and `scheduler.run.duration` (seconds) report
job/mode/result, including failures, timeouts, cancellation, and capacity skips.
Run IDs are logged, not used as metric labels. Job duration covers the handler;
claim/release errors are logged separately. The scheduler borrows global OTel
providers and never shuts them down.

## Caching

The `cache` package exposes one concrete `*cache.Client` for both backends.
Services use `Get`, `Set`, `Delete`, `Ping`, and `Close`; Redis types and commands
stay private. Values are opaque bytes, so services own JSON or other encoding.
There is no automatic backend fallback or two-level cache.

```go
store, err := cache.New(cache.Config{
	Backend:   cache.Redis,
	Namespace: "orders-v1",
	DefaultTTL: 5 * time.Minute,
	Redis: &cache.RedisConfig{
		Address: cfg.RedisAddress, Password: cfg.RedisPassword,
	},
})
if err != nil {
	return err
}

defer store.Close()

if err := store.Ping(ctx); err != nil {
	return err
}

if err := store.Set(ctx, "order:123", []byte(`{"id":"123"}`), time.Minute); err != nil {
	return err
}

value, err := store.Get(ctx, "order:123")
switch {
case errors.Is(err, cache.ErrMiss):
	// Fetch from the database, then populate the cache with an appropriate TTL.
case err != nil:
	// The service decides whether to bypass the cache or fail the operation.
	return err
default:
	// Decode value into your service-owned type.
	_ = value
}

if err := store.Delete(ctx, "order:123"); err != nil {
	return err
}
```

This uses `errors`, `time`, and `github.com/Penthious/go-foundation/cache`. `cfg` and `ctx` are owned
by the service. `New` validates configuration without network I/O. Reuse one
client per configured cache; stop its users before calling `Close`.

For an in-process cache, switch the constructor configuration:

```go
store, err := cache.New(cache.Config{
	Backend: cache.Memory,
	Namespace: "orders-v1",
	Memory: cache.MemoryConfig{
		MaxEntries: 10_000,
		MaxBytes:   64 << 20,
	},
})
if err != nil {
	return err
}
```

The same service code works with either client. Omitting `Backend` selects
memory, but a namespace is always required; supplying Redis config without
selecting `cache.Redis` fails validation. Each memory client is independent,
including clients with the same namespace. Replicas do not share memory entries.
Redis clients share data when address, database, and namespace match.

### Cache behavior and limits

- `Get` returns an independent byte slice. Missing and expired entries return
  `cache.ErrMiss`; a stored empty/nil value is an empty **hit**.
- `Set` replaces the value and its TTL. Zero TTL selects `DefaultTTL` (5m by
  default). Explicit TTLs must be at least 1ms and round up to whole milliseconds;
  negative TTLs and durations that cannot be represented safely are rejected.
  There is no non-expiring mode. Redis controls expiry using its server clock.
- `Delete` is idempotent, and removes only the exact namespaced key. GOF exposes
  no flush-database, namespace scan, or wildcard deletion operation.
- `Namespace` is 1–128 ASCII letters, digits, dots, underscores or hyphens.
  Stored keys are `gof:cache:<namespace>:<key>`. Keys must be nonblank, valid UTF-8,
  at most 1024 bytes, and contain no NUL/CR/LF. Include service, schema version,
  tenant, and authorization scope in your key design where appropriate.
- `MaxValueBytes` defaults to 1 MiB and may be configured up to 512 MiB. Oversized
  values return `cache.ErrValueTooLarge`. Redis reads return at most the limit
  plus one byte, including values written by other clients, and never return a
  truncated value as a hit. Oversized writes do not replace an existing entry.
- Memory defaults to 10,000 entries and 64 MiB of retained **key/value bytes**,
  including the stored prefix. Map/list metadata and temporary input/result
  copies add memory overhead. Least recently used entries are evicted under
  pressure; expired entries are reclaimed before live entries. Expiry cleanup
  happens on access and capacity pressure, with no sweeper goroutine.
- Clients are safe for concurrent use. Do not mutate input slices while `Set`
  is running. `Close` is idempotent, releases local resources, and makes later
  operations return `cache.ErrClosed`. It never deletes shared Redis entries.

`Timeout` defaults to 2s and bounds each operation, including pool waits and
Redis I/O; an earlier caller deadline wins. Canceled contexts are passed through
to the client. A cancellation during socket I/O can take until the active socket
deadline to return. Context cancellation/deadline errors remain classifiable;
other Redis failures return `cache.ErrUnavailable`, without driver errors,
keys, values, addresses, or credentials. A failed write can still have reached
Redis; no distributed transaction with your database is implied.

### Redis configuration and telemetry

`RedisConfig` supports a standalone Redis host/port, ACL username/password,
database number, pool size, and standard `*tls.Config`. Address defaults to
`localhost:6379`, database to 0, and the pool to 10 connections. The TLS config
is cloned; configure custom roots and verified server names before construction.
Use TLS on untrusted networks. Plain TCP is available for a trusted local setup.
Cluster and Sentinel are not configured through this initial adapter.

Command retries are disabled to avoid replaying ambiguous writes over concurrent
updates. Subsequent service calls can reconnect after an outage; GOF does not
silently switch to memory. Redis ACLs need `PING`, `SET`, `DEL`, `EVALSHA`, `EVAL`,
`GETRANGE`, and `EXISTS` for cache operations, plus connection authentication and
database selection as applicable. Reads use a short script to atomically preserve
empty/missing semantics while bounding the response. A successful `Ping` checks
connectivity/authentication, not every data-command ACL permission.

Initialize telemetry before `cache.New`. The cache records operation spans,
`cache.operations`, and `cache.operation.duration` (seconds), using only backend,
operation, and result attributes. Results distinguish hits, misses, successes,
and errors; a miss is not a failed span. Keys, namespaces, values, credentials,
and raw backend errors are not added to GOF telemetry. Standard tracer/meter
provider overrides are optional and borrowed; closing the cache never closes
those providers. The cache does not duplicate application error logging.

### Cache in Fx and health checks

```go
app := fx.New(
	foundationfx.CacheModule(),
	fx.Supply(cache.Config{
		Backend: cache.Redis, Namespace: "orders-v1",
		Redis: &cache.RedisConfig{
			Address: cfg.RedisAddress, Password: cfg.RedisPassword,
		},
	}),
	fx.Provide(NewOrderService), // Receives *cache.Client alongside other dependencies.
)
```

`CacheModule()` is opt-in and works without `foundationfx.Module`. Call it once
per Fx app; it validates config when consumed, pings on startup, and closes on
shutdown, construction failure, or startup rollback. Startup fails if a consumed
Redis cache cannot be reached. An unused module creates no client and needs no
config. If cache availability must not gate startup, provide `cache.New` through
your own lifecycle wiring instead and choose the service's failure policy.

Register `Ping` with the existing health checker to show the cache in `/health`
and the embedded Health UI:

```go
func serviceHealth(store *cache.Client) (*health.Checker, error) {
	return health.New(health.Optional("cache", store.Ping))
}
```

Provide this function with `fx.Provide(serviceHealth)` when using
`foundationfx.HTTPServerModule`; use `health.Required` if a failed cache should
make the running service unready. Optional health checks do not override the
cache module's startup policy.

Keep authoritative state in your database. The service owns cache invalidation,
cache-aside loading, and handling concurrent misses/stale repopulation. This
initial API does not add distributed locks, singleflight, or background refresh.
Memory restarts and eviction lose entries; Redis can also evict entries according
to its server policy, so callers must always handle misses.

## Authentication and authorization

Services create an `auth.Authenticator` and explicitly wrap protected handlers.
JWT and API-key backends stay private; handlers receive a GOF `Principal` from
the request context. Either configured method can authenticate a wrapped route;
the identity must have **every** scope required by that route.

```go
gate, err := auth.New(auth.Config{
	JWT: &auth.JWTConfig{
		Issuer:   cfg.AuthIssuer,
		Audience: "orders-api",
		JWKSURL:  cfg.AuthJWKSURL,
	},
	APIKeys: []auth.APIKey{{
		ID: "inventory-service", Secret: cfg.InventoryAPIKey,
		Scopes: []string{"orders:read"},
	}},
})
if err != nil {
	return err
}

orderHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	principal, ok := auth.FromContext(r.Context())
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// Apply resource ownership/tenant checks before accessing this order.
	log.Info(r.Context(), "fetching order", logger.String("subject", principal.Subject))
	w.WriteHeader(http.StatusNoContent)
})

server, err := httpserver.New(httpserver.Config{
	Logger: log,
	Routes: []httpserver.Route{{
		Pattern: "GET /orders/{id}",
		Handler: gate.Protect(orderHandler, "orders:read"),
	}},
})
if err != nil {
	return err
}
```

This uses `net/http` and GOF's `auth`, `httpserver`, and `logger` packages.
`cfg` is the service's loaded configuration. Run the server using its normal
startup/shutdown lifecycle. Omit `JWT` or `APIKeys` to enable just one method;
at least one method is required. `New` copies configuration and does no network
I/O. `Protect(handler)` without scopes still requires authentication.
Scope checks are exact and case-sensitive; invalid/empty required scope strings
deny access. Route requirements are copied when the wrapper is constructed.

Clients send **one** of:

```http
Authorization: Bearer <JWT-access-token>
```

```http
X-API-Key: <random-secret>
```

Missing/invalid credentials, duplicate authentication headers, or both methods
in the same request return `401 {"error":"unauthorized"}` with challenges for
the configured methods. API keys use the `ApiKey` challenge. Credentials never
fall back to a different method after verification fails. Query strings, cookies,
and form fields are not credential sources. An authenticated identity missing a
required scope returns `403 {"error":"forbidden"}`; JWT responses also include
`Bearer error="insufficient_scope"`. Responses default to `Cache-Control: no-store`.
Failures never invoke the protected handler. Auth adds no credential logging;
existing HTTP request logs/traces/metrics still capture the response status.

### JWT access tokens

Configure a trusted, absolute HTTPS issuer and JWKS URL plus the API's audience.
The implementation uses private `go-oidc`/`go-jose` verification with additional
access-token checks: signature, exact issuer, audience membership, required
expiration, nonblank subject, protected token type, and optional `nbf`/`iat`.
Expiration is strict; future `nbf`/`iat` allow 30 seconds of clock skew. Keep
service and identity-provider clocks synchronized. The default allowed algorithm
is `RS256`; explicitly set `Algorithms` for other supported RSA, RSA-PSS, ECDSA,
or EdDSA algorithms. HMAC and unsigned tokens cannot be enabled.

The default protected `typ` header is **`at+jwt`**. For a provider whose access
tokens use `typ: JWT`, explicitly set `TokenType: "JWT"` and use a dedicated
resource-server audience **never used for ID tokens**. Do not use a browser
login client ID as the API audience. GOF does not issue tokens, perform login,
or introspect opaque tokens. Other provider-specific token-purpose checks remain
service-owned. Scopes come from OAuth's space-separated `scope` string and/or
the `scp` string array; role/group claims do not automatically grant scopes.

JWKS keys are fetched lazily and cached by a shared verifier. A missing key or
failed signature triggers a refresh, limited to one HTTP fetch per second per
authenticator, with concurrent fetches coalesced. During that cooldown a new key
can receive 401 until a subsequent request refreshes it; publish replacement
keys before using them. Cached keys continue working during provider outages;
uncached/unverifiable keys deny access. Cached keys have no periodic eviction,
so removing a key from the provider alone is **not immediate revocation**.
Use short-lived access tokens and replace/restart the authenticator when urgent
cache invalidation is required. No token revocation list is maintained.

`JWTConfig.Timeout` defaults to 5 seconds for verification and JWKS requests.
Requests honor cancellation; a shared key fetch may finish after one caller
cancels, within the HTTP timeout. Tokens are limited to 16 KiB and JWKS responses
to 1 MiB. Redirects are rejected. Token-supplied key URLs are never followed;
GOF does not perform OIDC discovery. An optional standard `HTTPClient` is copied
and its transport borrowed, allowing private CAs and local TLS test servers;
the shorter of its existing timeout and the configured timeout applies. Callers
own custom transport cleanup. Transports must honor contexts. Plaintext URLs
require explicit `AllowInsecureHTTP: true`, intended only for development.

### API keys and identities

Load API-key secrets from your service's environment/secret store. Generate at
least 32 random bytes (using `crypto/rand.Read`, then hex-encoding); the SDK requires
32–4096 printable ASCII bytes without spaces or commas. Length validation does
not establish randomness. IDs must be unique, nonblank, and at most 256 bytes;
secrets must be unique too. GOF retains SHA-256 digests and compares digests in
constant time. Caller-owned configuration still contains the original secret.

API-key principals have `Method: "api_key"`, `Subject` equal to the configured
key ID, and the configured scopes. JWT principals have `Method: "jwt"`, issuer,
subject, and validated scopes. Identify JWT users by **issuer plus subject**;
different issuers can use the same subject. `auth.FromContext(ctx)` returns an
independent scope slice; `principal.HasScopes(...)` can check additional
permissions in service code. The service owns resource/tenant authorization.

Configuration is a startup snapshot: rotate keys by deploying overlapping old
and new entries under distinct IDs, migrating callers, then removing the old
entry and redeploying. API keys have no built-in expiry or persistent registry.
Use HTTPS between callers and the ingress, and protect ingress-to-service
traffic. Existing HTTP-client DEBUG curl logging includes full credentials by
design, including these headers; use it only with appropriately protected logs.

### Auth in Fx

```go
func protectedHTTPConfig(log *logger.Logger, gate *auth.Authenticator) httpserver.Config {
	return httpserver.Config{
		Logger: log,
		Routes: []httpserver.Route{{
			Pattern: "GET /orders",
			Handler: gate.Protect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}), "orders:read"),
		}},
	}
}

app := fx.New(
	foundationfx.Module,
	foundationfx.AuthModule,
	foundationfx.HTTPServerModule,
	fx.Supply(logger.Config{Service: "orders"}),
	fx.Supply(auth.Config{APIKeys: []auth.APIKey{{
		ID: "inventory-service", Secret: cfg.InventoryAPIKey,
		Scopes: []string{"orders:read"},
	}}}),
	fx.Provide(protectedHTTPConfig),
)
```

`AuthModule` is opt-in and lazy; consuming its authenticator validates config
during graph construction. It owns no lifecycle hooks and changes no routes.
Wrap each protected route, or mount a protected standard router to cover a whole
application. Unwrapped routes remain public. The server's reserved health
endpoints remain public for probes; restrict their network exposure at ingress
when needed, especially `/health` and `/health/ui`.

## Rate limiting

`ratelimit` implements an immediate, burst-capable quota. It refills `Limit`
tokens evenly over `Window`, with at most `Burst` tokens immediately available.
Every admitted request consumes one token, including requests whose business
handler later fails. Checks never queue or wait for tokens.

```go
limiter, err := ratelimit.New(ratelimit.Config{
	Namespace: "orders-api",
	Backend: ratelimit.Memory,
	Logger: log,
	Limit: 60,
	Window: time.Minute,
	Burst: 10,
})
if err != nil {
	return err
}

defer limiter.Close() // After HTTP/RPC servers and application callers stop.

protected := gate.Protect(limiter.Middleware(orderHandler), "orders:write")
routes := []httpserver.Route{{Pattern: "POST /orders", Handler: protected}}
```

`gate` is a GOF `*auth.Authenticator`. Authentication and scope checks must run
outside/before the limiter. JWT quotas use the validated issuer/subject pair;
API-key quotas use the configured key ID. Rotating a token or key secret does not
reset a stable identity's quota. Identity components have unambiguous boundaries,
and only their SHA-256 digest is stored. Raw authorization headers, cookies,
forwarded headers, or user-ID headers are never used as trusted identities.
The combined principal identity is limited to 4 KiB.

All routes and RPC methods using the same limiter share each identity's quota.
Use separate limiters/namespaces for independent policies. Direct application
code can call `decision, err := limiter.Allow(ctx, stableIdentity)` with a
service-owned identity of 1–4096 nonblank bytes. `Decision` exposes `Allowed`, a
snapshot of `Remaining`, and a positive `RetryAfter` on denial. A denial has no
operational error. Standalone keys are caller-owned; use a consistent identity
scheme when intentionally sharing them with other callers.

### Defaults and backends

Zero settings select memory storage, 100 tokens/second, a burst of 100, a
two-second operation timeout, and at most 10,000 active memory identities.
`Namespace` and `Logger` are required. Namespaces contain 1–128 ASCII letters,
digits, dots, underscores, or hyphens. Negative settings are rejected.
`Window` must be 1ms–24h in whole microseconds. Refill intervals round up to
microseconds, conservatively approximating rates that do not divide evenly.
Rates above one token/microsecond and bursts taking over 24h to refill are rejected.

Memory quotas belong to one limiter instance. They are independent across
replicas and reset on restart. Fully refilled entries expire lazily using a
bounded map and expiry heap, with no background cleanup worker. Active quotas
are never evicted to admit new identities: a full store returns `ErrCapacity`.
Changing `MaxKeys` changes the maximum retained identity count.

To share quotas across replicas, configure Redis:

```go
ratelimit.Config{
	Namespace: "orders-api",
	Backend: ratelimit.Redis,
	Logger: log,
	Limit: 60, Window: time.Minute, Burst: 10,
	Redis: &ratelimit.RedisConfig{
		Address: cfg.RedisAddress,
		Username: cfg.RedisUsername,
		Password: cfg.RedisPassword,
		PoolSize: 10,
	},
}
```

The Redis driver stays private. The standalone pool defaults to ten connections;
`Address` defaults to `localhost:6379` and must be host:port, not a URL. Supply
`TLS` with verified roots for networks requiring TLS; its config is cloned, with
TLS 1.2 minimum by default. Root pools/callbacks remain caller-owned and immutable.
There is no automatic fallback from Redis to memory.

A single-key Lua script atomically checks and consumes quota using Redis server
time. Entries expire after their complete refill time, rounded up for Redis's
millisecond expiry precision. Denied checks do not extend the stored debt/TTL.
Clients sharing a namespace must use the same effective refill interval and
burst: a mismatch returns `ErrPolicyMismatch` until the old entry expires.
Coordinate policy rollouts or explicitly version the namespace when resetting
quotas is intended. Redis key deletion, eviction, or loss of persisted state can
reset quotas; configure the backing Redis instance accordingly.

Redis checks are not automatically retried after ambiguous failures. A canceled
or failed check may already have consumed a token; it is not refunded. Backend,
capacity, policy, and closed-limiter errors deny requests. Errors retain context
cancellation/deadline classification while hiding driver/server error details.
`Ping(ctx)` checks connectivity/authentication without consuming tokens; Redis
ACLs must also permit the script's `EVALSHA`/`EVAL`, `TIME`, `GETRANGE`, `EXISTS`,
and `SET` commands. `Close()` is idempotent and never deletes shared Redis quotas.

### HTTP and gRPC behavior

`limiter.Middleware(handler)` requires an already authenticated principal.
Missing principals receive JSON `401`; quota denial returns JSON `429` with
`Retry-After` rounded up to seconds; operational failures return JSON `503`.
Rejected responses use `Cache-Control: no-store`. Wrap selected application
routes, leaving the server's built-in health endpoints outside the limiter.
Apply edge/gateway limits separately for unauthenticated traffic, since these
identity quotas run after authentication.

For GOF gRPC servers, set `grpcserver.Config{Auth: gate, RateLimiter: limiter, ...}`.
The server enforces auth, then scopes, then quota. All protected application
methods share the limiter. Health methods and explicit `PublicMethods` bypass
quota checks while retaining their existing authentication policies. Configuring
a rate limiter without `Auth` is rejected. The server borrows the limiter and
does not close it.

Denial returns `ResourceExhausted` with standard `google.rpc.RetryInfo` details.
Backend failures return `Unavailable`; missing principals return
`Unauthenticated`; context cancellation/deadlines retain their corresponding
gRPC statuses. Streams consume one token at establishment, with no additional
charges per message or refund on completion. Rate limiting does not limit the
number of simultaneously active calls/streams.

`UnaryServerInterceptor()` and `StreamServerInterceptor()` are also available for
service-owned native gRPC servers. Chain them after authentication that installs
a validated GOF principal; the standalone interceptors do not parse credentials
or automatically exempt health/public methods.

Existing GOF client policies still apply: eligible HTTP requests may retry 429
under the configured deadline/backoff. HTTP 429 and gRPC `ResourceExhausted` leave
an enabled upstream circuit breaker unchanged, so quota rejections for one user
do not block other users of that client. This applies to all responses with those
statuses; service-wide availability failures should use HTTP 503 or gRPC
`Unavailable` to affect the shared breaker.

### Fx lifecycle and telemetry

Add `foundationfx.RateLimitModule()` and supply `ratelimit.Config`. The module
injects the app logger unless the config supplies its own. The provider is lazy;
consumed limiters are checked during startup and closed on normal shutdown,
graph failure, or startup rollback. Create fresh module options once per app.

```go
app := fx.New(
	foundationfx.Module,
	foundationfx.AuthModule,
	foundationfx.RateLimitModule(),
	foundationfx.HTTPServerModule,
	foundationfx.GRPCServerModule,
	fx.Supply(loggerConfig, authConfig, ratelimit.Config{
		Namespace: "orders-api", Limit: 60, Window: time.Minute, Burst: 10,
	}),
	fx.Provide(func(limit *ratelimit.Limiter) (*health.Checker, error) {
		return health.New(health.Required("rate_limit", limit.Ping))
	}),
	fx.Provide(func(gate *auth.Authenticator, limit *ratelimit.Limiter) httpserver.Config {
		return httpserver.Config{Routes: []httpserver.Route{{
			Pattern: "POST /orders",
			Handler: gate.Protect(limit.Middleware(orderHandler), "orders:write"),
		}}}
	}),
	fx.Provide(func(gate *auth.Authenticator, limit *ratelimit.Limiter) grpcserver.Config {
		return grpcserver.Config{
			Auth: gate, RateLimiter: limit,
			Register: []func(grpc.ServiceRegistrar){func(registrar grpc.ServiceRegistrar) {
				orderspb.RegisterOrdersServer(registrar, orderRPCHandler)
			}},
		}
	}),
)
app.Run()
```

Configuration, handlers and generated protobuf contracts belong to the service.
The shared health checker is injected by both server modules. Constructor
dependencies ensure limiter startup precedes serving and server shutdown precedes
closing the limiter. Standalone users own startup checks and shutdown ordering.

Initialize telemetry first. `ratelimit.checks` and `ratelimit.check.duration`
(seconds) use instrumentation scope `go-foundation/ratelimit`, with only
`ratelimit.name`, `ratelimit.backend`, and `ratelimit.result` (`allowed`, `denied`,
or `error`) dimensions. Each check creates a child `ratelimit.allow` span;
expected denials are not span errors. Denials and operational failures produce
WARN logs with namespace/backend and retry delay or a sanitized error. No user
IDs, key IDs, identity hashes, credentials, or request content become telemetry
fields. Missing-authentication requests never access quota storage. Providers
default to globals, can be supplied in config, and remain service-owned.

## HTTP servers

Services supply standard Go handlers and ServeMux patterns. GOF owns the listener,
request logging, tracing/metrics, health endpoints, and shutdown. The server uses
plain HTTP; terminate TLS at your reverse proxy or deployment ingress.

### Fx setup

```go
func httpConfig(log *logger.Logger) httpserver.Config {
	return httpserver.Config{
		Address: ":8080",
		Routes: []httpserver.Route{
			{Pattern: "GET /orders/{id}", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				log.Info(r.Context(), "fetching order", logger.String("order_id", r.PathValue("id")))
				_, _ = io.WriteString(w, r.PathValue("id"))
			})},
		},
	}
}

app := fx.New(
	foundationfx.Module,
	foundationfx.HTTPServerModule,
	fx.Supply(logger.Config{Service: "orders"}),
	fx.Provide(httpConfig),
)
```

This example uses `context`, `io`, `net/http`, `go.uber.org/fx`, and the module's
`foundationfx`, `httpserver`, and `logger` packages. Register a `*health.Checker`
provider as shown below to add dependency checks. With none, readiness follows
the listener lifecycle. Provide your own handler constructors and let
`httpConfig` receive them as dependencies instead of writing handlers inline.
For an existing router, mount it with `httpserver.Route{Pattern: "/", Handler: router}`.
Middleware can wrap each supplied handler using ordinary `http.Handler` functions.

Use the explicit app `Start`/`Stop` pattern from the Fx example above, and select
on `app.Wait()` as well as your signal context so unexpected listener failures
trigger cleanup. `HTTPServerModule` validates routes during graph construction,
binds during `app.Start`, and drains during `app.Stop`. Bind errors fail startup;
if a later hook fails, Fx rolls back and closes the listener. Unexpected serving
errors are logged and request Fx shutdown with exit code 1. A custom runner must
honor the shutdown signal's exit code when choosing its process exit status.

Initialize telemetry before building the app. Supplying the initialized SDK as in
the main Fx example connects logs, traces, and metrics to the collector. The
server module does not create or shut down telemetry. Keep HTTP-handler
dependencies in the constructor graph so they start before HTTP and remain
available until the HTTP shutdown hook finishes draining requests.

### Manual setup

```go
func serve(ctx context.Context, log *logger.Logger, orderHandler http.Handler) (err error) {
	server, err := httpserver.New(httpserver.Config{
		Logger: log,
		Address: ":8080",
		Routes: []httpserver.Route{
			{Pattern: "GET /orders/{id}", Handler: orderHandler},
		},
	})
	if err != nil {
		return err
	}

	if err := server.Start(ctx); err != nil {
		return err
	}

	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err = errors.Join(err, server.Shutdown(shutdownCtx))
	}()

	done := make(chan error, 1)
	go func() { done <- server.Wait() }()

	select {
	case <-ctx.Done():
		return nil
	case err := <-done:
		return err
	}
}
```

This uses `context`, `errors`, `net/http`, `time`, and the module's `httpserver`
and `logger` packages. `Start` returns after binding; the service waits for its
stop signal or `server.Wait()` before draining requests. `New` opens no sockets.
`Address()` exposes the actual bound address, useful with `127.0.0.1:0` in tests.
`ServeHTTP` implements the standard handler interface for middleware and `httptest`.
Readiness is false until this server's `Start` succeeds; using it as a handler
in another server does not start its owned listener or mark it ready.
`Start` is idempotent after success; startup failures can be retried. A shut-down
server cannot restart (`httpserver.ErrClosed`); construct a new one.

### Health checks and embedded UI

Register any context-aware function returning an error. There is no required
database, broker, or health-provider interface. A service using `database/sql`
and GOF pubsub can define:

```go
func serviceHealth(db *sql.DB, bus *pubsub.Client) (*health.Checker, error) {
	return health.New(
		health.Required("database", db.PingContext),
		health.Required("pubsub", bus.Ping),
	)
}
```

Use `health.Optional("cache", checkCache)` for a function that should not block
traffic. Closures work too: `health.Required("custom", func(ctx context.Context)
error { return checkMySystem(ctx) })`. Names must be nonblank and unique, and
functions must be non-nil, concurrency-safe, side-effect-free, and context-aware.
`health.New(checks ...health.Check)` copies registrations and opens no resources.
`pubsub.Client.Ping(ctx)` performs a read-only round trip to verify that durable
messaging is reachable; it does not publish a test event or expose NATS types.

With Fx, add `fx.Provide(serviceHealth)` after supplying the service's DB and
pubsub configuration/providers. `foundationfx.HTTPServerModule` automatically
injects the `*health.Checker`. Its dependencies start before HTTP and remain
available until HTTP drains. With manual wiring, set `httpserver.Config.Health`
to the checker. An explicitly configured checker takes precedence over the Fx
provider. The old `ReadinessChecks` map still works as required checks but is
deprecated; do not combine it with `Config.Health`.

- `GET /livez`: 200 with `{"status":"ok","build":{...}}`, including version,
  commit, and build time from `buildinfo.Current()`. It never calls dependencies.
- `GET /readyz`: runs required checks only. Returns 200 with `{"status":"ready"}`
  after startup and successful required checks; otherwise 503 with
  `{"status":"not_ready"}`. Optional checks are never called here.
- `GET /health`: full JSON report with build info, readiness, per-dependency
  status, required/optional policy, duration in milliseconds, safe error summary,
  and check timestamp. Required failure yields 503; optional-only failure yields
  200 with overall `degraded` status and `ready: true`.
- `GET /health/ui`: embedded, server-rendered Health UI with build metadata,
  overall readiness, and the same full dependency breakdown. It refreshes every
  ten seconds and provides manual refresh and endpoint links. The diagnostic page
  returns 200 even when dependencies are down; use `/readyz` for traffic probes.

All four paths support `HEAD` without a body, use `Cache-Control: no-store`, and
reject other methods with 405 and `Allow: GET, HEAD`. The paths are reserved and
cannot be shadowed by a catch-all or host-specific service router. The UI is
embedded in the Go executable; it needs no JavaScript, frontend build, external
assets, or separate UI server. Build info comes from the running executable's
linker values, including on liveness responses.

Each HTTP report uses one two-second default budget (`HealthTimeout`). Required
checks run before optional checks and all results are listed, including checks
left `unknown` after the budget expires. A panic fails that check without
preventing the rest from running. Optional timeouts/panics do not change readiness
when all required checks succeeded. There are no background polling goroutines
or cached results: each health request takes a fresh snapshot. Standalone callers
can use `checker.Check(ctx)` or `checker.Readiness(ctx)` with their own deadline.

Callbacks must honor cancellation; GOF cannot stop arbitrary code that ignores
its context. Full error causes remain available in Go results (`Result.Err`) and
server logs. JSON and HTML expose only safe summaries, never raw driver errors,
connection strings, or panic values. The full report and UI intentionally expose
build metadata and dependency names. Restrict diagnostic endpoints to internal
networks or apply access control at your ingress; GOF does not add authentication.
Keep liveness independent of a temporarily unavailable database or broker.

### Request behavior and defaults

The default bind address is `:8080`. Timeouts default to `ReadHeaderTimeout: 5s`,
`ReadTimeout: 30s`, `WriteTimeout: 30s`, and `IdleTimeout: 60s`. These are socket
deadlines, not a forced handler deadline; use request contexts for downstream
work. Zero selects defaults and negative values are invalid. Streaming handlers
can use `http.NewResponseController` to manage per-request deadlines explicitly.
`MaxHeaderBytes` and `MaxBodyBytes` default to 1 MiB each. Increase them for larger
headers/uploads. A known oversized body is rejected with 413 before calling the
handler. For unknown/chunked sizes, handlers receive `*http.MaxBytesError` while
reading; use `errors.As` and return 413. GOF limits reads instead of buffering the
entire upload, so a handler that ignores body-read errors owns its response.

Requests log method, escaped path, matched route, status, response byte count,
and duration. Successes use INFO, 4xx use WARN, and 5xx/panics use ERROR. Query
strings, request headers, and bodies are omitted; paths and application panic
values/stacks can still contain application data. Health requests are included.
Informational responses, flushing, `io.Copy`, and standard optional writer
interfaces retain their behavior. Hijacked responses use status 0 in request
logs if no HTTP status was written through the wrapper; raw socket bytes are
not counted.

Incoming trace context and baggage use the global OTel propagator. Request logs
and handler logs using `r.Context()` correlate with the server span. Route
templates label spans and HTTP duration/size metrics; concrete IDs do not become
metric route labels. Unmatched requests have no route label. Metrics use the
configured OTel meter/exporter; GOF does not add a Prometheus `/metrics` endpoint.
Configure telemetry before server construction so HTTP metrics use that provider.

Panics before a response starts produce a generic 500, with details/stacks only
in logs and an error span. A panic after headers/streaming starts aborts the
response instead of appending an invalid error body. Tracing and metrics finish
before the abort; the recorded status reflects any headers already sent.
`http.ErrAbortHandler` keeps standard abort semantics.

Shutdown immediately marks readiness false, stops accepting connections, and
drains ordinary active requests. If its context expires, GOF closes remaining
connections and returns the context error. Handlers must honor cancellation;
forced connection closure cannot stop arbitrary handler code. WebSockets and
other hijacked connections remain service-owned and need their own cleanup.
Shut HTTP down before flushing telemetry to retain final request spans/logs.

## PostgreSQL

`postgres.New(postgres.Config)` returns a standard `*sql.DB`. The pgx driver and
SQL instrumentation dependency are internal; service code uses `database/sql`
types, contexts, prepared statements, and transactions. Each service owns its
schema, SQL queries, and migrations. Initialize telemetry before constructing
the pool to use the providers installed by `telemetry.New`.

### Manual setup and health

Load the connection string into a service-owned config, for example:

```go
type AppConfig struct {
	DatabaseURL string `env:"DATABASE_URL,required,notEmpty"`
}
```

After loading `cfg`, construct one pool and reuse it for the service lifetime:

```go
db, err := postgres.New(postgres.Config{
	DSN: cfg.DatabaseURL,
	MaxOpenConns: 10,
	MaxIdleConns: 3,
})
if err != nil {
	return err
}

defer db.Close()

startupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()

if err := db.PingContext(startupCtx); err != nil {
	return fmt.Errorf("connect database: %w", err)
}

checks, err := health.New(health.Required("postgres", db.PingContext))
if err != nil {
	return err
}

// Pass checks through httpserver.Config.Health for readiness and the Health UI.
```

`New` validates configuration and allocates a lazy pool; it does not verify a
connection. The caller owns `db.Close()`. Closing also unregisters pool metric
callbacks. `db.PingContext` verifies a database round trip and can be passed
directly to the existing variadic `health.New`, alongside DBs, pubsub, or other
dependencies. No separate Postgres health API or background polling is needed.

### Connection settings and telemetry

`DSN` is required. Both PostgreSQL URLs and keyword/value strings are accepted.
Standard pgx/libpq-style connection options and `PG*` environment defaults apply,
including `sslmode`, `sslrootcert`, `sslcert`, `sslkey`, and `application_name`.
For verified TLS, use a URL such as
`postgres://orders@database:5432/orders?sslmode=verify-full&sslrootcert=/certs/ca.pem`;
provide credentials through your service's secret configuration. Use
`sslmode=disable` explicitly for a local plaintext test database. Invalid DSN
errors omit parser details because those can contain credentials. Runtime SQL
errors retain their original causes; avoid logging the entire config or DSN.

Zero-valued pool settings use these defaults:

- `MaxOpenConns`: 25 per service instance.
- `MaxIdleConns`: 5, capped at `MaxOpenConns`; `-1` disables idle connections.
- `ConnMaxLifetime`: 30 minutes.
- `ConnMaxIdleTime`: 5 minutes.
- `ConnectTimeout`: 5 seconds for each complete connection attempt, including
  multi-host fallback; Fx applies the same budget to its startup ping.

Negative durations, negative open limits, idle limits below `-1`, and explicit
idle limits above the open limit are rejected. Pool sizes should fit the database's
connection budget across **all services and replicas**, leaving capacity for
administration and migrations. Query, transaction, and pool-wait deadlines come
from their caller's context; use `QueryContext`, `ExecContext`, and `BeginTx`.
Close rows and finish transactions before shutdown. GOF adds no query or
transaction retry policy.

Operation spans follow the caller's trace context and record failures. Metrics
include `db.client.operation.duration` plus `db.sql.connection.*` measurements for
open/in-use/idle connections, limits, pool waits, and recycled connections.
SQL text, DSNs, and query parameters are not added as telemetry attributes.
Driver errors are recorded on failed spans and can contain server-provided data.
`Config.TracerProvider` and `Config.MeterProvider` can override global providers,
for example in tests. The pool borrows providers; shut down your telemetry SDK
after stopping services and closing their pools.

### Fx setup

```go
func serviceHealth(db *sql.DB, bus *pubsub.Client) (*health.Checker, error) {
	return health.New(
		health.Required("postgres", db.PingContext),
		health.Required("pubsub", bus.Ping),
	)
}

app := fx.New(
	foundationfx.Module,
	foundationfx.PostgresModule(),
	foundationfx.HTTPServerModule,
	fx.Supply(logger.Config{Service: "orders"}),
	fx.Supply(postgres.Config{DSN: cfg.DatabaseURL}),
	fx.Supply(pubsub.Config{Endpoints: []string{cfg.BrokerURL}}),
	fx.Supply(httpserver.Config{Address: ":8080"}),
	fx.Provide(serviceHealth),
)
```

`PostgresModule()` provides one lazy `*sql.DB` when a constructor needs it; an
unused module requires no database or config. Unlike the stateless module values,
call this function separately for each app so failure cleanup belongs to that
app. It can also be used without the base `foundationfx.Module` or a logger.
Graph construction opens no database connections. The startup hook pings the
database, fails startup if unavailable, and releases a failed pool. Successful
startup is paired with pool closure on shutdown or later startup rollback;
graph invocation failures also close a constructed pool.

Services receive `*sql.DB` directly in their constructors. Their lifecycle hooks
should depend on it so database startup precedes their work and shutdown follows
their cleanup. In the example, the provided `*health.Checker` is automatically
injected into the HTTP server. `/readyz`, `/health`, and `/health/ui` then include
the registered database check. `/livez` remains independent of dependencies.

## Database migrations

Use `migrate` from a **service-owned migration command or deployment job** before
starting the service. It accepts `*sql.DB` and standard `fs.FS`, including
`embed.FS`; no Goose types, registration calls, global configuration, or SQL
annotations are exposed. Constructing a runner and starting an Fx app never
apply migrations automatically.

Keep SQL files in one service-owned directory, with positive, unique numeric
versions. Each migration has a required `VERSION_name.up.sql` and an optional
matching `VERSION_name.down.sql`, for example:

```text
cmd/migrate/
  main.go
  migrations/
    003_create_orders.up.sql
    003_create_orders.down.sql
```

The up file contains plain PostgreSQL SQL:

```sql
CREATE TABLE public.orders (
    id text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
```

An explicitly reversible service migration can supply a down file:

```sql
DROP TABLE public.orders;
```

The command embeds its files and opts into the SDK schemas at versions chosen
by the service. Here versions 1 and 2 are reserved for inbox and outbox, so the
first application migration is version 3:

```go
//go:embed migrations/*.sql
var migrations embed.FS

func applyMigrations(ctx context.Context, db *sql.DB) error {
	runner, err := migrate.New(db, migrate.Config{
		FS: migrations,
		Dir: "migrations",
		InboxVersion: 1,
		OutboxVersion: 2,
	})
	if err != nil {
		return err
	}

	return runner.Up(ctx)
}
```

This uses `embed`, `context`, `database/sql`, and `github.com/Penthious/go-foundation/migrate`. `FS`
may be nil for a runner containing only SDK schemas. `Dir` defaults to `"."` and
is relative to that filesystem; only its immediate files are collected. Non-SQL
files and subdirectories are ignored. SQL files with invalid names, duplicate
versions, unmatched down files, empty content, and collisions with SDK versions
are rejected during `New`. Sources are copied at construction, so later changes
to a mutable filesystem require a new runner.

### Versioning and operations

- `Up(ctx)` applies pending versions in ascending order and is a no-op when up to date.
- `Status(ctx)` returns GOF `Status` values with version, name, applied timestamp,
  and reversibility. Pending timestamps are zero.
- `Version(ctx)` returns the highest applied version, or zero before migration.
- `Down(ctx)` rolls back **one** highest applied version using its explicit down
  file. This can destroy service data. A missing down file returns
  `migrate.ErrIrreversible`, without changing that migration or its version.

History lives in `public.gof_schema_version`. Status and version use read-only
snapshots; they do not create a history table in a fresh database. Down at version
zero is a no-op. All operations honor the caller's context and `Config.Timeout`
(default 5m), including pool acquisition and migration-lock waits. The runner
borrows the pool; the command owns `db.Close()`.

Each complete SQL file and its version record execute in one transaction.
Multiple statements and PostgreSQL dollar-quoted function bodies work without
vendor-specific statement markers. A failing file rolls back its DDL, data, and
version record; earlier successful files remain applied. Files must not contain
top-level `BEGIN`, `COMMIT`, `ROLLBACK`, or other transaction-control statements;
procedural blocks inside dollar-quoted functions are supported. Operations
that PostgreSQL forbids inside transactions, such as `CREATE INDEX CONCURRENTLY`,
require a separately managed deployment step; this API deliberately provides
transactional migrations only.

Migration jobs serialize with a PostgreSQL session advisory lock, including
across runner instances and processes. Canceled waits and failed operations
release the lock; an ambiguous/failed lock cleanup discards the physical session.
Cleanup has a separate budget of up to 5s after cancellation. Use a direct
database endpoint or session pooling. Transaction pooling proxies cannot preserve
the session lock's guarantees. A pool size of one is supported.

Treat applied migration files and configured SDK versions as immutable. Append
new versions for changes; never renumber, remove, or edit an applied migration.
Missing applied versions and newly introduced versions below the database's
current version cause errors. The history tracks versions, not SQL checksums;
editing an applied file is not detected or reapplied. Choose the same versions
and sources in every deployment of a service.

### Inbox/outbox schemas and existing databases

Positive `InboxVersion` and `OutboxVersion` values include the initial SDK schemas
in that **same service migration history**. Zero disables each schema. Their
versions must be distinct and must not collide with service SQL. The built-ins
are forward-only: Down never drops inbox deduplication records or pending outbox
events. Any intentional removal requires a service-owned, reviewed migration.

`FailedMessagesVersion` similarly opts into pubsub failure/replay storage at an
explicit unused version. For example, use version 4 after the command example's
versions 1–3. Its schema is also forward-only. Keep existing applied versions
unchanged; the example command does not enable this optional schema by default.

`SchedulerVersion` opts into coordinated scheduling state at another unused
version, with the same forward-only policy. Local scheduling does not need it.
The migration command example leaves this optional schema disabled too.

The SDK's initial SQL files live in `internal/schema`, shared by the migration
runner and the runtime packages' schema helpers. Migration commands do not import
the messaging or scheduler implementations to obtain their SQL. Moving these
files does not change the SQL, applied versions, or migration history.

`inbox.Schema()` and `outbox.Schema()` remain available as plain SQL for existing
migration workflows. A service can instead snapshot that SQL into its own
versioned up file; do not also enable the corresponding built-in version.
This runner does not automatically adopt existing tables or another tool's
history. Enabling a built-in for an existing untracked table fails rather than
silently claiming it matches the expected schema. Existing services should keep
their established migration history or plan an explicit, verified transition;
do not delete inbox/outbox data to bootstrap this runner.

### Service command and container builds

[The complete Go command example](examples/migrations/main.go) embeds paired SQL
files and supports `up`, `down`, `status`, and `version`. Copy it into a service
and adapt its application migrations and SDK version choices. It loads the
connection string from `DATABASE_URL`, supports cancellation, and closes its pool:

```sh
go run ./examples/migrations status
go run ./examples/migrations up
go run ./examples/migrations version
```

Supply `DATABASE_URL` from the deployment's secret configuration. Run the command
against the intended service database using a migration role with the necessary
DDL/history permissions. Runtime roles can retain narrower permissions.

In each service's Go builder stage, compile its command and embed the SQL into
the migration executable:

```dockerfile
RUN CGO_ENABLED=0 go build -trimpath -o /out/migrate ./cmd/migrate
```

Run `migrate up` as one explicit deployment step before starting the new service
version. No runtime filesystem mount or Goose binary is required. For rolling
deployments, use backward-compatible expand/contract schema changes so old and
new service versions can coexist during the rollout.

## Transactional outbox

Use `outbox.Enqueue` when a business write must commit together with an event.
It accepts the service's existing `*sql.Tx` and a GOF `pubsub.Message`. A relay
publishes committed rows and deletes them only after the publisher confirms
durable acceptance. PostgreSQL and broker types remain behind the existing GOF
and standard-library APIs. The pattern avoids the failure gap between committing
business data and separately publishing an event; see the
[transactional outbox pattern](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html).

### Migration and transactional writes

Apply [the initial SQL migration](internal/schema/001_create_outbox.sql) through
each service's migration process before running a relay. `outbox.Schema()` returns
the same SQL for services that embed migrations in Go. It creates the fixed
`public.gof_outbox` table and its polling index. Apply it once as a versioned
migration; the SDK never runs DDL automatically. Grant the runtime role SELECT,
INSERT, UPDATE, and DELETE on the table. The business transaction and relay must
connect to the **same service database**.

With the migration runner, opt in through `migrate.Config.OutboxVersion` at a
unique service-chosen version instead of copying this schema into a SQL file.

```go
func createOrder(ctx context.Context, db *sql.DB, orderID string, payload []byte) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "INSERT INTO orders (id) VALUES ($1)", orderID); err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	if err := outbox.Enqueue(ctx, tx, pubsub.Message{
		ID: "order.created:" + orderID,
		Topic: "orders.created",
		Data: payload,
	}); err != nil {
		return err
	}

	return tx.Commit()
}
```

`Enqueue` validates the topic and event ID using `pubsub.Message.Validate`, copies
the payload into PostgreSQL, and persists trace context/baggage. It never commits,
rolls back, or publishes. Roll back the business transaction on any error. Rolled
back and uncommitted rows are invisible to the relay. Nil payloads become empty
bytes; otherwise binary bytes are preserved. Marshal JSON in the service if the
event contract uses JSON.

Use a stable ID for the **logical event**, including on retries after an ambiguous
commit or publish. Concurrent queued events cannot share an ID; duplicate inserts
return a database error, without overwriting or silently discarding a message.
Successfully delivered rows are deleted, so this table is not a permanent
deduplication ledger or event history. Never recycle IDs for different events.

### Relay and Fx lifecycle

Add the opt-in relay module to an app with Postgres and pubsub configured:

```go
app := fx.New(
	foundationfx.Module,
	foundationfx.PostgresModule(),
	foundationfx.OutboxModule,
	fx.Supply(logger.Config{Service: "orders"}),
	fx.Supply(postgres.Config{DSN: cfg.DatabaseURL}),
	fx.Supply(pubsub.Config{
		Endpoints: []string{cfg.BrokerURL},
		Topics: []pubsub.TopicConfig{{Name: "orders.created"}},
	}),
	fx.Supply(outbox.Config{
		PollInterval: time.Second,
		RetryDelay: 5 * time.Second,
		Timeout: 10 * time.Second,
	}),
)
```

`outbox.Config` is optional in Fx. Its logger defaults to the injected service
logger. The module constructs one `*outbox.Relay`, checks its schema during
startup, and runs it with a service-lifetime context. Shutdown and startup rollback
cancel and join the worker **before** closing the database or pubsub connection.
An unexpected worker exit requests Fx shutdown. No worker starts during graph
construction. Broker startup still follows pubsub's connectivity check; after
startup, transient failures leave events queued for retry.

Producer-only services can use `Enqueue` with their database and omit the relay
module entirely. A separate process can run the relay against that same database,
allowing producers to start and queue work while the broker is unavailable.

Without Fx, construct a relay from borrowed dependencies and call its blocking
`Run(ctx)` method in a service-owned goroutine:

```go
relay, err := outbox.New(db, bus.Publish, outbox.Config{Logger: log})
if err != nil {
	return err
}

// Run blocks until canceled; the service supervises this goroutine.
err = relay.Run(ctx)
if err != nil && !errors.Is(err, context.Canceled) {
	return err
}
```

Cancel and join `Run` before shutting down its database, broker client, logger,
or telemetry SDK. The publisher function must respect cancellation and return
nil only after durable acceptance. `bus.Publish` satisfies that contract; a custom
backend can supply the same function signature without changing the producer.

### Retries, concurrency, and observability

Each worker claims one due event atomically using `FOR UPDATE SKIP LOCKED`, with
a random lease token, then releases database locks **before** publishing. Multiple
workers or replicas can share the table. Acknowledgments and retry scheduling
require the same token, so a stale worker cannot delete or change an event that
another worker has reclaimed. This does not prevent duplicate publications.

Defaults are a 1s polling interval after an empty queue or error, a 5s retry delay,
and a 10s timeout covering a complete claim/publish/acknowledgment cycle. Durations
must be at least 1ms. The database lease lasts twice `Timeout`; configure the
timeout to fit your broker and database latency. Failed publications and publisher
panics retain the row. If cancellation, a crash, or an ambiguous database result
prevents retry scheduling, the lease expires and makes the event eligible again.
Leases use the database's clock; workers do not compare local wall clocks.

Delivery is **at least once**. A broker may accept a message just before the worker
crashes or loses the database acknowledgment. The event will be retried with its
original ID. NATS ID deduplication has a finite window; consumers must be
idempotent independently of that window. Event order is not guaranteed across
retries, concurrent transactions, or workers; include aggregate versions in event
contracts when consumers need to detect stale/out-of-order changes.

Retries continue indefinitely; events are never silently dropped or moved to a
dead-letter queue. Monitor failures and backlog, and repair failed topics,
credentials, oversized payloads, or invalid event data. Broker message retention
still applies after publication. For example, queue depth and oldest pending age
can be inspected with:

```sql
SELECT count(*) AS pending,
       coalesce(extract(epoch FROM clock_timestamp() - min(created_at)), 0) AS oldest_seconds
FROM public.gof_outbox;
```

Initialize telemetry before constructing the relay. Enqueue and relay spans
preserve the originating trace through PostgreSQL and into pubsub; persisted
baggage follows the same configured propagator. `outbox.publish.attempts` and
`outbox.publish.duration` record publisher results (`success`/`failure`). A successful
publication metric means the publisher accepted the event; a later database
acknowledgment can still fail. Logs include IDs, topics, attempt numbers, and
errors without directly logging `Data`. Keep custom publisher errors free of
payloads and secrets. The table contains application payloads and propagated
baggage until delivery, so use the same access controls as business data.

To include storage availability in `/readyz`, `/health`, and the Health UI, add
`health.Required("outbox", relay.Ping)` to the service's checker factory. This
checks connectivity and table columns, not worker activity, broker health, queue
age, or write permissions. Services that can continue accepting queued writes
during a broker outage can make the broker's health check optional.

## Transactional inbox

Use `pubsub.Client.ConsumeTransactional` for handlers whose database changes must
run once per logical event and consumer. The inbox inserts a unique marker,
executes the handler, and commits both in **one PostgreSQL transaction**. The
pubsub consumer acknowledges only after commit. Committed duplicates skip the
handler and are acknowledged normally. Other errors and handler panics roll back
the transaction and follow pubsub's existing redelivery behavior.

Apply [the initial inbox migration](internal/schema/001_create_inbox.sql) through
the service's migration process first. `inbox.Schema()` returns the same SQL.
It creates `public.gof_inbox` with a primary key on `(consumer, message_id)`;
the SDK never runs DDL automatically. The runtime role needs INSERT and SELECT.
Only consumer/event identifiers and a timestamp are stored; payloads are not
copied into the ledger.

With the migration runner, opt in through `migrate.Config.InboxVersion` at a
unique service-chosen version instead of copying this schema into a SQL file.

### Pubsub integration

```go
store, err := inbox.New(db, inbox.Config{})
if err != nil {
	return err
}

if err := store.Ping(ctx); err != nil {
	return err
}

err = bus.ConsumeTransactional(ctx, pubsub.ConsumerConfig{
	Topic: "orders.created",
	Group: "billing",
}, store, func(ctx context.Context, tx *sql.Tx, message pubsub.Message) error {
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO invoices (event_id, payload) VALUES ($1, $2)",
		message.ID, message.Data,
	); err != nil {
		return fmt.Errorf("create invoice: %w", err)
	}

	return outbox.Enqueue(ctx, tx, pubsub.Message{
		ID: "invoice.created:" + message.ID,
		Topic: "invoices.created",
		Data: message.Data,
	})
})
if err != nil && !errors.Is(err, context.Canceled) {
	return err
}
```

The example's invoice schema and event payload contract belong to the service.
Provision both topics before their publishers run, and apply the outbox migration
when using `outbox.Enqueue`. This commits the invoice, inbox marker, and outgoing
event together. The inbox owns commit/rollback: the handler must use the supplied
`*sql.Tx` for all protected writes, must not finish or retain that transaction,
and must use the supplied context for its queries.

`ConsumeTransactional` is blocking, like `Consume`; supervise it in a service-owned
goroutine, handle unexpected exits, and cancel/join it before shutting down the
database and pubsub client. Its existing heartbeat renews the delivery lease while
the transaction runs. Check `store.Ping` at startup, or use the Fx module below;
missing-schema errors during processing otherwise follow normal pubsub retries.
Regular `Consume` remains available for handlers that do not use an inbox.

### Consumer identity and transaction behavior

The adapter uses `Group + ":" + Topic` as its stable inbox consumer namespace.
For example, `billing:orders.created` and `shipping:orders.created` process
independently, as do the same group on different topics. Service instances sharing
a group/topic share its ledger. Renaming the group creates a new namespace and
can reprocess retained events. Keep event IDs stable across redeliveries and do
not reuse an ID for different content; the inbox deduplicates by key, without
comparing payloads. Keys are case-sensitive and compared exactly.

Concurrent inserts of the same key wait on PostgreSQL's unique constraint. If
the first transaction commits, the waiting delivery skips the handler; if it
rolls back, a later transaction can process it. A worker crash before commit
leaves no committed marker or business writes. A crash or lost broker acknowledgment
after commit can cause redelivery, which the persisted marker skips. An ambiguous
database commit returns an error; retry the **same key** to resolve its outcome.
These semantics protect writes in this PostgreSQL transaction. HTTP calls,
emails, and other external effects need their own idempotency or an outbox event.

`inbox.Config.Timeout` defaults to 30s, covering pool acquisition, duplicate-key
waits, the handler, and commit. Earlier caller deadlines still apply. Handlers
must honor cancellation; GOF cannot terminate arbitrary callback code that ignores
its context. `Isolation` defaults to `sql.LevelReadCommitted`; repeatable-read
and serializable transactions are also supported. Serialization/deadlock errors
return to the caller for retry; the inbox does not rerun a handler internally.

Markers **do not expire automatically**. Deleting one makes the event eligible
for processing again. Keep markers for the full redelivery/manual-replay horizon
your service must protect, and plan ledger retention explicitly. Broker-side
deduplication windows do not replace this ledger.

### Direct processing and Fx

The storage API is independent of pubsub and has no broker imports:

```go
processed, err := store.Process(ctx, inbox.Key{
	Consumer: "billing:orders.created",
	MessageID: eventID,
}, func(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", amount, accountID)
	return err
})
if err != nil {
	return err
}

if !processed {
	return nil // Already committed by an earlier delivery.
}
```

`true, nil` means a new event committed; `false, nil` means a committed duplicate
was skipped. Any error means the caller should handle/retry the event with the
same key. Consumer namespaces must contain 1–512 valid UTF-8 bytes and cannot be
blank; message IDs contain 1–256 bytes. Neither may contain CR, LF, or NUL.
`New` validates configuration without querying the database or starting workers;
the store borrows `*sql.DB` and has no separate shutdown method.

Add `foundationfx.InboxModule` alongside `foundationfx.PostgresModule()` and
request `*inbox.Store` in your service constructor. `inbox.Config` is optional:

```go
foundationfx.InboxModule,
fx.Supply(inbox.Config{Timeout: 15 * time.Second}),
```

When consumed, the module checks the schema during startup, after its database
starts. It starts no consumer goroutines. A service constructor receiving the
store and pubsub client should register its consumer lifecycle hooks, starting
`ConsumeTransactional` with a service-lifetime context and joining it on shutdown.
An unused inbox provider needs no database configuration. Register
`health.Required("inbox", store.Ping)` with your checker to include storage
availability in readiness and the Health UI. Ping checks connectivity and table
columns, not write permissions or consumer activity.

Initialize telemetry before creating the store. `inbox process` spans preserve
the consumer trace and record the outcome. `inbox.messages` and
`inbox.process.duration` use `result=processed|duplicate|failure`; a processed
result is recorded only after a successful commit. Errors retain their causes
for callers; handler panic values are omitted. The inbox itself does not log
errors, so pubsub or the service can report them once.

## Durable pubsub

Services import `github.com/Penthious/go-foundation/pubsub` and use `*pubsub.Client`, `Message`,
`ConsumerConfig`, and `Handler`. No public signatures, aliases, options, messages,
or connection accessors expose NATS types. The initial implementation uses NATS
JetStream; changing it belongs inside GOF. Broker deployment, credentials,
stored messages, and consumer positions would still require a migration when
changing providers. The abstraction does not make those operational differences
disappear.

The public contract is durable, **at-least-once** delivery. Publishing waits for
the broker's storage acknowledgment. A timeout is ambiguous: the event may have
been stored. Give every logical event a stable ID and reuse it on retries.
Broker deduplication has a bounded window (currently two minutes by default),
so handlers must tolerate duplicate messages even when publishers reuse IDs.
For database writes plus events, use a transactional outbox in the owning
service; publishing alone is not atomic with a database transaction.

### Manual injection

```go
func consumeOrders(ctx context.Context, log *logger.Logger) (err error) {
	bus, err := pubsub.New(pubsub.Config{
		Logger:    log,
		Name:      "billing",
		Endpoints: []string{"nats://localhost:4222"},
		Topics: []pubsub.TopicConfig{{
			Name: "orders.created", MaxAge: 24 * time.Hour, MaxBytes: 64 << 20,
		}},
	})
	if err != nil {
		return err
	}

	if err := bus.Start(ctx); err != nil {
		return err
	}

	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err = errors.Join(err, bus.Shutdown(shutdownCtx))
	}()

	err = bus.Consume(ctx, pubsub.ConsumerConfig{
		Topic: "orders.created", Group: "billing",
	}, func(ctx context.Context, message pubsub.Message) error {
		// Commit idempotent business work before returning nil.
		log.Info(ctx, "order event received", logger.String("message_id", message.ID))
		return nil
	})
	if errors.Is(err, context.Canceled) {
		return nil
	}

	return err
}
```

This uses `context`, `errors`, `time`, and the module's `logger` and `pubsub`
packages. `New` validates configuration and opens no connections. `Start` connects,
checks that JetStream is enabled, and provisions configured topics. Startup
failures close their connection; topics already created remain on the broker.
`Start` is idempotent after success. Shutdown is terminal; create a new client
instead of restarting a closed one. `ErrNotStarted` and `ErrClosed` are GOF errors.

Publishing from a service with an injected client:

```go
func publishOrder(ctx context.Context, bus *pubsub.Client, eventID, orderID string) error {
	payload, err := json.Marshal(map[string]string{"order_id": orderID})
	if err != nil {
		return err
	}

	return bus.Publish(ctx, pubsub.Message{
		ID: eventID, Topic: "orders.created", Data: payload,
	})
}
```

Payloads are opaque bytes; JSON is an application choice. Topics are exact,
dot-separated names using letters, numbers, underscores, and hyphens. Wildcard
subscriptions are intentionally absent. IDs must contain 1–256 valid UTF-8 bytes
with no CR, LF, or NUL. Consumer groups use 1–200 letters, numbers, underscores, or hyphens.

### Retention and delivery

Each topic has its own disk-backed stream. Defaults are 24 hours and 64 MiB per
topic, including broker metadata, with one storage replica. Both limits apply:
expired events are removed even when unprocessed, and a full topic rejects new
publications instead of evicting older events to make room. Size retention for
your longest consumer outage. Provisioning is idempotent for matching settings;
conflicting settings fail startup rather than overwriting existing retention.
Consumers can omit `Topics` when another service or deployment provisions them.
Topic/consumer creation requires broker permissions; use separate accounts for
environments and namespace topic names where needed. GOF uses deterministic
private stream names derived from the topic; services never construct them.

Different groups each receive their own copy of every retained event. Instances
sharing the same topic and group divide that group's messages. A new group starts
at the oldest retained event; an existing group resumes its saved position.
`Consume` blocks and processes one message at a time. Run additional instances
with the same group for concurrent processing; global completion order is not
guaranteed. Configuration/setup failures, deleted subscriptions, and permanently
closed connections return to the caller. Other fetch failures retry with a delay;
normal idle fetch timeouts are not logged as errors.

Return nil from a handler only after its business work commits; GOF then waits
for the acknowledgment to be confirmed. Errors and panics cause delayed
redelivery. `RetryDelay` defaults to one second. `RetryBackoff` can supply a delay
sequence for failed deliveries; after the final entry, its last delay repeats.
The slice is copied when consumption starts. Fetch/storage errors still use
`RetryDelay`, and broker timeout redelivery uses `AckTimeout`. Zero
`MaxDeliveries` retains unlimited retries for compatibility. A positive limit
requires a `FailedStore`, as described below. Invalid envelopes follow the same
policy and are retained with their raw bytes if quarantined. Topic retention
expiry can still remove unacknowledged originals.

`AckTimeout` defaults to 30 seconds, with a minimum of 30ms. GOF renews the lease
while processing and acknowledging; it does not impose a handler deadline.
Instances sharing a group must use matching acknowledgment settings. Lease
renewal reduces concurrent redelivery but cannot eliminate duplicates during
failures. Handlers must honor cancellation and remain idempotent.

### Failed-message storage and targeted replay

Failed-message storage is optional and uses the service's PostgreSQL database.
Ordinary pubsub consumers still need no database. Apply
`migrate.Config.FailedMessagesVersion` at a new service-chosen version, or apply
[the SQL schema](internal/schema/001_failed_messages.sql) through your existing
migration workflow. `pubsub.FailedSchema()` returns the same SQL. The SDK never
creates this table automatically; the runtime role needs SELECT, INSERT, UPDATE,
and DELETE on `public.gof_failed_messages`.

```go
failures, err := pubsub.NewFailedStore(db, pubsub.FailedStoreConfig{})
if err != nil {
	return err
}

err = bus.Consume(ctx, pubsub.ConsumerConfig{
	Topic: "orders.created",
	Group: "billing",
	MaxDeliveries: 5,
	RetryBackoff: []time.Duration{
		time.Second, 5 * time.Second, 30 * time.Second,
	},
	Failures: failures,
}, handler)
```

The same config works with `ConsumeTransactional`, preserving inbox atomicity.
All workers sharing a topic/group must use the same database, delivery limit,
and backoff policy. Consumption checks failed-message schema availability at
startup when a store is supplied. PostgreSQL failures while polling replay work
pause that consumer with a retry delay.

At the configured limit, a failed delivery is stored before its broker
acknowledgment. Records contain the logical message, original raw envelope,
topic/group, failure reason, observed delivery count, timestamp, trace context,
and replay state. Archival IDs are deterministic for the original delivery, so
retries after an ambiguous insert or acknowledgment do not overwrite an existing
record or its replay state. Reasons are UTF-8-safe and bounded to 4096 runes.

If storage fails, GOF leaves the original unacknowledged and retries archival;
it does not execute the handler again once the observed delivery limit has been
exceeded. Broker-level delivery exhaustion is deliberately disabled so a storage
failure cannot strand a final delivery. The configured limit counts **deliveries**,
including crash/acknowledgment recovery, not guaranteed completed handler calls.
An exhausted record can therefore have an ambiguous earlier business outcome.
Inbox idempotency remains necessary.

Original topic retention still applies independently: recover failed storage
before unacknowledged originals expire. Once archived, the PostgreSQL record does
not expire automatically and is independent of broker message retention.

Inspect and queue a replay explicitly:

```go
page, err := failures.ListFailed(ctx, pubsub.FailedFilter{
	Topic: "orders.created",
	Group: "billing",
	State: pubsub.FailureStored,
	Limit: 50,
})
if err != nil {
	return err
}

if len(page) != 0 {
	if err := failures.ReplayFailed(ctx, page[0].ID); err != nil {
		return err
	}
}
```

`ListFailed` orders by record ID, with a default page size of 50 and maximum 500.
Use the final ID as `FailedFilter.After` to fetch the next page. `State` is optional;
without it, stored, pending, and successfully replayed records are all included.
`GetFailed(ctx, id)` returns a complete record. Both inspection APIs include bodies
and diagnostics; protect access like other application data. Custom handler error
messages can include business data, and raw envelopes include propagated baggage.

`ReplayFailed` changes a stored record to pending work for its **original topic
and group**. It does not publish to the original topic or notify other subscriber
groups. A running `Consume` or `ConsumeTransactional` with the same store and
topic/group processes it, including when that consumer has no new broker messages.
Idle replay polling is approximately once per second. Keep a consumer running
for the target group; queuing work does not start one.

Replays preserve the original event ID, body, trace context, and baggage. Inbox
handlers use the same group/topic key, so already committed work is skipped and
the replay can complete without repeating database changes. Malformed transport
envelopes or invalid event IDs remain inspectable through `Raw` but cannot be
replayed unchanged; repair their producer/transport instead of treating partial
decoded data as a valid event.

Pending replay requests are idempotent and do not reset an active lease or its
attempt count. Workers claim one record with `FOR UPDATE SKIP LOCKED`, release
database locks before calling the handler, and fence finalization with a random
lease token. Crashes/cancellation leave the record eligible after lease expiry.
`FailedStoreConfig.Timeout` defaults to 5s for SQL operations; `ReplayTimeout`
defaults to 30s for a complete replay attempt, with a lease of twice that value.
Handlers must honor cancellation. Duplicate execution remains possible after an
ambiguous result or lease expiry; external side effects need their own idempotency.

Each replay request gets a fresh attempt budget under the consumer's current
delivery policy. Failures retry with its backoff; exhaustion returns the record
to `FailureStored`, retaining the initial `Reason` and updating `LastError` and
`ReplayAttempts`. `ReplayCount` counts explicitly queued replay rounds. Successful
work becomes `FailureReplayed` with `ReplayedAt` populated, and further replay
requests return `pubsub.ErrAlreadyReplayed`. State can be queried after queuing
because `ReplayFailed` does not wait for execution.

`DeleteFailed(ctx, id)` permits deleting only successfully replayed records.
Unprocessed/pending records cannot be discarded through this API. Plan database
capacity and retention explicitly; retain resolved records through the relevant
source redelivery horizon before cleanup. The store is an operational quarantine,
not a permanent exactly-once ledger or audit log.

For Fx, add `foundationfx.FailedMessagesModule` alongside Postgres wiring and
inject `*pubsub.FailedStore` into the service that builds its `ConsumerConfig`.
`pubsub.FailedStoreConfig` is optional. The lazy module checks schema availability
on startup when consumed, starts no workers itself, and borrows the database
lifecycle. Consumers must stop before their database closes. Register
`health.Required("failed_messages", failures.Ping)` for storage availability in
readiness and the Health UI; it does not measure replay progress or backlog.

### Connection lifecycle and observability

The default endpoint is `nats://localhost:4222`. `Timeout` defaults to five seconds
and bounds startup and individual broker operations; caller deadlines can shorten
it. Initial connection failure returns an error. After a successful connection,
the client reconnects indefinitely. Publications while disconnected are not
buffered for a later send; errors return to the caller. The timeout/acknowledgment
ambiguity still applies, so retry using the same logical event ID.

Use `tls://` endpoints and/or `TLSConfig` for certificate-verified TLS, private
CAs, or mutual TLS. Use `Token` or `Username`/`Password` for authentication. URLs
cannot contain credentials. Configuration slices and the TLS config are cloned;
as with `tls.Config.Clone`, nested objects such as certificate pools must not be
mutated concurrently. Credentials and payloads are not written by GOF's pubsub
diagnostic logs. Application handler errors are logged as provided.

Producer and consumer spans use the configured OTel providers. W3C trace context
and baggage propagate using the global propagator installed by `telemetry.New`.
Consumer contexts carry the remote trace into business work and logger calls.
DEBUG logs contain topic, group, and event ID, without payload contents. Connection
changes, handler errors, and broker errors are logged through the injected logger.

`Shutdown` cancels active consumption/publications and waits for handlers before
closing the connection. With an expired shutdown context, it forces the connection
closed and returns the context error. A handler that ignores cancellation can
outlive shutdown. Do not call shutdown from the client's own handler. Shut down
pubsub before telemetry so final logs and spans can be exported.

### Fx wiring

Add configuration to the existing Fx app and request `*pubsub.Client` from your
service constructor:

```go
fx.Supply(pubsub.Config{
	Endpoints: []string{"nats://broker:4222"},
	Topics:    []pubsub.TopicConfig{{Name: "orders.created"}},
})
```

`foundationfx.Module` injects the logger and registers `Start`/`Shutdown` hooks.
Fx opens the connection during startup, closes it during shutdown, and closes it
if a later startup hook fails. Invalid dependency graphs open no connection.
Start your service's `Consume` goroutine in its own `OnStart`, after pubsub starts.
Use a service-owned context, not the temporary startup-hook context. Cancel and
join that goroutine in the service's `OnStop`; handle unexpected `Consume` errors
by reporting the failure and stopping the app. Constructors only wire dependencies.

For a local broker, run `nats-server -js -sd /path/to/persistent/data`; the data
directory must survive container/VM recreation. The SDK does not deploy a broker
or configure backups, authentication, or clustering. See the official
[JetStream documentation](https://docs.nats.io/concepts/jetstream) and
[Go client documentation](https://pkg.go.dev/github.com/nats-io/nats.go/jetstream).

## Development

[GitHub Actions CI](.github/workflows/ci.yml) runs on pull requests, pushes to
`main`, and manual dispatches. It uses the Go version declared in `go.mod`,
runs `go vet ./...`, and runs `go test -race -count=1 -timeout=10m ./...` against
fresh PostgreSQL 18 and Redis 7.4 service containers. Health checks wait for both
services before the job starts. The PostgreSQL test user can create the isolated
databases used by the integration tests. The database connection environment
variables are supplied so those tests run in CI rather than being skipped.

The containers are disposable test infrastructure; their credentials are not
production secrets. CI enables Redis password authentication before the tests
so the invalid-credential cases exercise a protected server. NATS servers, HTTP services,
and telemetry collectors are started locally by the Go tests themselves.

Use the Go version declared in `go.mod`:

```sh
go test -race ./...
go vet ./...
```

Rate-limit tests cover deterministic refill/burst boundaries, identity isolation,
API-key/JWT rotation, spoofed headers, capacity exhaustion, telemetry privacy,
HTTP 429/Retry-After, gRPC RetryInfo, auth/scope ordering, stream charging, health
exemptions, and shared HTTP/RPC quotas in Fx. Set `GO_FOUNDATION_REDIS_TEST_ADDRESS`
and `GO_FOUNDATION_REDIS_TEST_PASSWORD` for the real Redis atomicity, replica
sharing, expiry, policy mismatch, corrupt-state, authentication, and deadline tests:

```sh
GO_FOUNDATION_REDIS_TEST_ADDRESS=127.0.0.1:6379 \
GO_FOUNDATION_REDIS_TEST_PASSWORD=foundation-test \
  go test -race ./ratelimit ./grpcserver ./foundationfx
```

Use a dedicated test Redis instance. Tests create randomly namespaced quota keys
that expire; they do not flush the database. No Redis driver types are exposed
to service code.

Logger tests cover JSON encoding, field isolation, concurrency, level filtering,
error reporting, and trace correlation. Telemetry tests run local gRPC collector
services and verify logs, traces, and metrics over plaintext and TLS, headers,
environment configuration, shared resources, propagation, collector rejection,
export timeouts, and shutdown. No external collector or credentials are required;
the tests need permission to bind local loopback ports.
HTTP client tests make real requests to local HTTP/TLS services and run generated
curl commands back against them, verifying replayed methods, URLs, headers, and
bodies. They also check propagation, metrics, pooling, cancellation, timeouts,
redirects, and streaming uploads. Curl replay tests require `curl` on PATH.
Retry tests cover transient statuses and connection failures, safe-method guards,
body rewind failures, final responses, per-attempt logs/spans, and the shared
timeout budget. Injected-client tests verify TLS, redirects, cookies, timeout
overrides, custom transports, and preservation of the supplied client.
Pubsub tests run real local NATS servers with temporary storage, covering durable
fan-out, consumer resumption, shared workers, redelivery, lease renewal, storage
limits, broker restart/reconnect, TLS/authentication, trace propagation, lifecycle,
and the absence of broker types in public signatures. Fx tests verify startup,
shutdown, rollback, and graph failures. No installed NATS binary is needed.
HTTP server tests exercise real listeners, route parameters/methods, health states,
request logs, propagation, route metrics, body limits, streaming/hijacking, panic
recovery/aborts, startup failures, and graceful/forced shutdown. Server Fx tests
cover automatic opt-in startup, binding failure, graph failure, and rollback.
Health tests cover required/optional policy, deadline ordering, panic isolation,
JSON error safety, concurrent checks, build info, full reports, UI rendering and
escaping, Fx injection, and a real broker ping during availability and failure.
Config tests cover defaults, typed fields, nested prefixes, isolated environments,
required/empty values, validation, error redaction, and concurrent reads. Build
metadata tests compile and execute consumer binaries with linker flags, then run
the same executable with different deployment environments. They need the Go
compiler on PATH; no Docker daemon or external configuration service is required.

Postgres unit tests cover validation, DSN error safety, lazy construction,
connection timeouts, pool metric cleanup, and Fx graph/startup failure cleanup.
Integration cases are skipped unless `GO_FOUNDATION_POSTGRES_TEST_DSN` is set.
Point it at a dedicated test database; those cases exercise real SQL, prepared
statements, transaction commit/rollback, pool reuse and limits, cancellation,
health checks, traces, metrics, and Fx startup/shutdown/rollback:

```sh
GO_FOUNDATION_POSTGRES_TEST_DSN='postgres://foundation:foundation-test@127.0.0.1:5432/foundation?sslmode=disable' \
  go test -race ./postgres ./foundationfx
```

Tests create only a connection-local temporary table; they do not create or alter
service schemas. PostgreSQL must already be running; the tests do not start Docker.

Outbox integration tests use the same `GO_FOUNDATION_POSTGRES_TEST_DSN`, which
must be a PostgreSQL **URL** with permission to create/drop test databases. They
create isolated, randomly named databases, apply the outbox migration there, and
drop only those databases afterward. Local NATS servers verify durable delivery,
trace continuity, and broker outages. Tests also cover atomic rollback/commit,
invalid and duplicate events, retries, publisher panics, expired/stale leases,
parallel workers, lock release, cancellation, metrics, health checks, and Fx
startup/shutdown/rollback. These cases are skipped when the variable is unset.

Inbox integration tests also use `GO_FOUNDATION_POSTGRES_TEST_DSN` with CREATEDB
permission and isolated databases. They verify commit/rollback of business writes,
markers, and outgoing outbox events, duplicate-key contention, cancellation and
timeouts, handler panics, error preservation, namespace isolation, schema checks,
traces/metrics, and Fx startup checks. Real NATS tests verify acknowledgments
after database commit, retained-message replay with persisted deduplication,
handler retries, and independent topic/group scopes.

Migration tests use the same isolated PostgreSQL test databases and CREATEDB
permission. They cover source validation and snapshots, read-only status/version,
SDK and service schemas, transactional multi-statement/function SQL, rollback,
idempotent up, explicit down, irreversible schemas, missing/out-of-order history,
legacy-schema refusal, single-connection pools, concurrent jobs, cancellation,
and advisory-lock cleanup. The service-owned example command is compiled by the
suite; its embedded migrations can also be run against a dedicated test database.

Failed-message tests use isolated PostgreSQL databases and real NATS brokers.
They cover delivery limits, copied/repeating retry backoff, storage-before-ack,
storage outages, invalid envelopes, bounded inspection/pagination, explicit
targeted replay, failed replay budgets, cancellation recovery, stale lease fencing,
trace/baggage continuity, inbox rollback and replay, migration support, and Fx
schema/lifecycle checks. PostgreSQL integration cases skip when the DSN is unset.
