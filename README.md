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

## Packages

| Package | Guide |
| --- | --- |
| [`config`](config/README.md) | Environment loading, defaults, and validation |
| [`buildinfo`](buildinfo/README.md) | Version, commit, and Docker linker flags |
| [`foundationfx`](foundationfx/README.md) | Application composition and lifecycle wiring |
| [`telemetry`](telemetry/README.md) | OTLP/gRPC logs, traces, and metrics |
| [`logger`](logger/README.md) | Structured logging and OpenTelemetry integration |
| [`httpclient`](httpclient/README.md) | HTTP requests, retries, auth, and replayable curl logs |
| [`grpcclient`](grpcclient/README.md) | RPC calls, auth metadata, timeouts, and named clients |
| [`grpcserver`](grpcserver/README.md) | RPC handlers, authentication, health, and shutdown |
| [`scheduler`](scheduler/README.md) | Local and PostgreSQL-coordinated jobs |
| [`cache`](cache/README.md) | In-memory and Redis caching |
| [`auth`](auth/README.md) | JWT access tokens, API keys, and scopes |
| [`ratelimit`](ratelimit/README.md) | Per-identity HTTP and gRPC quotas |
| [`httpserver`](httpserver/README.md) | Routes, request telemetry, and graceful shutdown |
| [`health`](health/README.md) | Dependency checks, health endpoints, and embedded UI |
| [`postgres`](postgres/README.md) | Connection pooling and instrumented database/sql |
| [`migrate`](migrate/README.md) | Service-owned embedded SQL and SDK schemas |
| [`outbox`](outbox/README.md) | Transactional event writes and background delivery |
| [`inbox`](inbox/README.md) | Consumer idempotency in the business transaction |
| [`pubsub`](pubsub/README.md) | Messaging, delivery limits, failed messages, and replay |

## Start a service

The [Fx startup example](foundationfx/README.md#dependency-injection-with-fx)
combines configuration, build metadata, logging, telemetry, and an HTTP client.
For manual wiring, start with the [telemetry and logger example](telemetry/README.md#telemetry-setup),
then add the packages your service needs. Package guides include their own Fx examples.

See [client circuit breakers](docs/circuit-breakers.md) for shared HTTP/gRPC
failure policies and [health checks](health/README.md) for readiness and the Health UI.

## Development

Run the full race-enabled integration suite from the repository root with Docker running:

```sh
go -C testsupport/integration run .
```

See [development and testing](testsupport/README.md) for connection overrides,
targeted tests, CI, prerequisites, and detailed package coverage.
