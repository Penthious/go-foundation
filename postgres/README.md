# postgres

[Overview](../README.md) · [Development and testing](../testsupport/README.md)

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
