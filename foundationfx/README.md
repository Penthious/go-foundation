# foundationfx

[Overview](../README.md) · [Development and testing](../testsupport/README.md) · [Service configuration](../config/README.md)

## Dependency injection with Fx

Use `foundationfx.Module` for runtime dependency injection. It provides one
`*logger.Logger` and `*http.Client` per Fx app. Supply a `logger.Config`; supplying
`httpclient.Config` is optional and retains the HTTP client's defaults when absent.
An HTTP config without its own logger receives the app's injected logger.
Add `foundationfx.NamedHTTPClientModule("orders")` for each additional upstream,
with a matching named `httpclient.Config`; see [multiple named HTTP clients](../httpclient/README.md#multiple-named-http-clients-in-fx).

When the service supplies an initialized `*telemetry.SDK`, the module connects
its log provider to the logger unless `logger.Config.LoggerProvider` was explicitly
set. It also exposes the SDK's `trace.Tracer` and `metric.Meter` for injection.
Logger/HTTP-only apps can omit the SDK; requesting a tracer or meter requires it.
Providers are lazy and are constructed only when consumed.

Requesting `*pubsub.Client` also registers broker startup and shutdown hooks.
Supply `pubsub.Config` to configure endpoints and topics; its logger defaults to
the app's injected logger. Constructing the graph opens no broker connections.
If the client is unused, no broker is needed. See the [pubsub guide](../pubsub/README.md) for its
separate `Start`/`Shutdown` lifecycle when using manual injection.

HTTP listeners are an explicit opt-in: add `foundationfx.HTTPServerModule` and
provide `httpserver.Config`. It starts the listener automatically; the base
`foundationfx.Module` never starts an HTTP listener by itself.

PostgreSQL is also opt-in: call `foundationfx.PostgresModule()` for each app,
provide `postgres.Config`, and consume `*sql.DB`. It verifies connectivity at
startup and closes the pool at shutdown. See the [PostgreSQL guide](../postgres/README.md).

```go
func run(ctx context.Context) (err error) {
	appCfg, err := config.Load[AppConfig](config.Options{Prefix: "ORDERS_"})
	if err != nil {
		return err
	}

	build := buildinfo.Current()
	cfg := telemetry.Config{Service: appCfg.Service, Environment: appCfg.Environment, Version: build.Version}
	sdk, err := telemetry.New(ctx, cfg)
	if err != nil {
		return err
	}

	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err = errors.Join(err, sdk.Shutdown(shutdownCtx))
	}()

	app := fx.New(
		foundationfx.Module,
		fx.Supply(appCfg, sdk),
		fx.Supply(logger.Config{
			Service: cfg.Service, Environment: cfg.Environment, Version: cfg.Version,
		}),
		fx.Supply(httpclient.Config{Timeout: appCfg.Timeout}),
		fx.Invoke(func(lifecycle fx.Lifecycle, log *logger.Logger) {
			lifecycle.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					log.Info(ctx, "service started", logger.String("commit", build.Commit), logger.String("build_time", build.BuildTime))
					return nil
				},
			})
		}),
	)
	if err := app.Err(); err != nil {
		return err
	}

	startCtx, cancelStart := context.WithTimeout(ctx, 10*time.Second)
	startErr := app.Start(startCtx)
	cancelStart()
	if startErr != nil {
		return startErr
	}

	select {
	case <-ctx.Done():
	case signal := <-app.Wait():
		if signal.ExitCode != 0 {
			err = fmt.Errorf("application stopped with exit code %d", signal.ExitCode)
		}
	}

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelStop()

	return errors.Join(err, app.Stop(stopCtx))
}
```

This example uses `context`, `errors`, `fmt`, `time`, `go.uber.org/fx`, and the module's
`buildinfo`, `config`, `foundationfx`, `logger`, `httpclient`, and `telemetry`
packages, plus `AppConfig` from the [configuration example](../config/README.md#service-configuration). Pass a context
from `signal.NotifyContext` in the service's main function. Add service constructors
with `fx.Provide` and register routes or lifecycle hooks with `fx.Invoke`. Business
constructors take normal dependencies such as `*logger.Logger`, `*http.Client`,
`trace.Tracer`, and `metric.Meter`; they do not need to take an SDK or use Fx types.

The service initializes telemetry before creating the Fx app and shuts it down
after stopping the app. This also cleans up exporters if dependency construction
or startup fails. Use explicit `Start`/`Stop` error handling as shown, so telemetry
cleanup is not bypassed by process exit. The module borrows the SDK and never
shuts it down. It registers an HTTP-client `OnStop` hook to close idle connections;
service hooks should stop active work before that cleanup runs.

Fx supplies `context.Context` to lifecycle hooks; request contexts still flow
through normal method parameters. Put background work and server startup in
`OnStart`, not in constructors. Fx validates the consumed graph during app
construction; missing dependencies and constructor errors appear through `app.Err()`.
Its internal framework events retain Fx's default logging; application logs use
our injected logger.

The module supplies one unqualified HTTP client. For multiple internal/external
clients, register service-owned factories with `fx.Annotate`/named results and
call `httpclient.New` with each upstream's config. Existing packages have no Fx
imports and remain usable with manual constructor injection.
