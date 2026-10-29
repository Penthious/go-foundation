# Client circuit breakers

[Overview](../README.md) · [Development and testing](../testsupport/README.md)

## Client circuit breakers

Both `httpclient.Config` and `grpcclient.Config` accept an optional
`CircuitBreaker` using their package's own `CircuitBreakerConfig` type. The
implementation is shared internally and services import no breaker library.
These settings work with manual construction, unnamed Fx clients, and named Fx
clients. The [named HTTP](../httpclient/README.md#multiple-named-http-clients-in-fx) and [named gRPC](../grpcclient/README.md#multiple-named-grpc-clients-in-fx) examples show different thresholds and open timeouts.

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
