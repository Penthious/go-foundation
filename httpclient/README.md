# httpclient

[Overview](../README.md) · [Development and testing](../testsupport/README.md) · [Circuit breakers](../docs/circuit-breakers.md)

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
[client circuit breakers](../docs/circuit-breakers.md#client-circuit-breakers) for failure and recovery rules.

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
