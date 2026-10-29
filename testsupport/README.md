# Development and testing

[Overview](../README.md)

## Development

[GitHub Actions CI](../.github/workflows/ci.yml) runs on pull requests, pushes to
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

For the full local integration suite, start Docker and run from the repository
root:

```sh
go -C testsupport/integration run .
```

The runner uses Testcontainers to start PostgreSQL 18 and password-protected
Redis 7.4 on temporary ports, waits for readiness, then runs
`go test -race -count=1 -timeout=10m ./...` in the SDK module. It removes the
containers and their volumes afterward, including when tests fail or startup
partially fails. Docker startup failures fail the command rather than skipping
integration tests. The first run may download container images.

Existing `GO_FOUNDATION_POSTGRES_TEST_DSN`, `GO_FOUNDATION_REDIS_TEST_ADDRESS`,
and `GO_FOUNDATION_REDIS_TEST_PASSWORD` settings are preserved. The runner starts
only a database whose connection setting is missing or empty; it never stops
externally supplied databases. GitHub Actions uses the same runner with both
connections supplied, so it reuses CI service containers without starting more.
Use dedicated test databases; the PostgreSQL user needs CREATEDB permission.

To select packages or tests, pass ordinary `go test` arguments:

```sh
go -C testsupport/integration run . ./inbox ./outbox
go -C testsupport/integration run . -run TestClient ./cache
```

Testcontainers is isolated in the `testsupport/integration` module and does not
add dependencies to the SDK module. CI also runs that module's own tests and
`go vet`. Plain `go test ./...` retains its existing behavior: database integration
cases skip when their connection variables are unset.

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
