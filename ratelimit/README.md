# ratelimit

[Overview](../README.md) · [Development and testing](../testsupport/README.md)

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
