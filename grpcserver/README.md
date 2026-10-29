# grpcserver

[Overview](../README.md) · [Development and testing](../testsupport/README.md)

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
[application runner pattern](../foundationfx/README.md#dependency-injection-with-fx) so those shutdown requests trigger
cleanup. HTTP and gRPC listeners can run together on separate addresses.
