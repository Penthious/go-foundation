# health

[Overview](../README.md) · [Development and testing](../testsupport/README.md) · [HTTP server](../httpserver/README.md) · [gRPC health](../grpcserver/README.md#grpc-health-and-shutdown)

## Health checks and embedded UI

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
