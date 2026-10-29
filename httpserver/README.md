# httpserver

[Overview](../README.md) · [Development and testing](../testsupport/README.md)

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
provider as shown in the [health guide](../health/README.md) to add dependency checks. With none, readiness follows
the listener lifecycle. Provide your own handler constructors and let
`httpConfig` receive them as dependencies instead of writing handlers inline.
For an existing router, mount it with `httpserver.Route{Pattern: "/", Handler: router}`.
Middleware can wrap each supplied handler using ordinary `http.Handler` functions.

Use the explicit app `Start`/`Stop` pattern from the [Fx example](../foundationfx/README.md#dependency-injection-with-fx), and select
on `app.Wait()` as well as your signal context so unexpected listener failures
trigger cleanup. `HTTPServerModule` validates routes during graph construction,
binds during `app.Start`, and drains during `app.Stop`. Bind errors fail startup;
if a later hook fails, Fx rolls back and closes the listener. Unexpected serving
errors are logged and request Fx shutdown with exit code 1. A custom runner must
honor the shutdown signal's exit code when choosing its process exit status.

Initialize telemetry before building the app. Supplying the initialized SDK as in
the [main Fx example](../foundationfx/README.md#dependency-injection-with-fx) connects logs, traces, and metrics to the collector. The
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

See the [health guide](../health/README.md) for dependency registration, endpoints,
readiness policies, and the embedded Health UI.

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
