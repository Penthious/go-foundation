# grpcclient

[Overview](../README.md) · [Development and testing](../testsupport/README.md) · [Circuit breakers](../docs/circuit-breakers.md)

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
