# config

[Overview](../README.md) · [Development and testing](../testsupport/README.md)

## Service configuration

Services define their own configuration types. `config.Load[T]()` reads a fresh
startup snapshot, applies tagged defaults, parses typed values, then calls the
top-level `Validate() error` method when present. It returns the zero `T` on any
failure; partially populated config is never returned. No global config holder,
background watcher, or live reload is involved.

```go
type AppConfig struct {
	Service     string        `env:"SERVICE,required,notEmpty"`
	Environment string        `env:"ENVIRONMENT" envDefault:"local"`
	Port        int           `env:"PORT" envDefault:"8080"`
	Timeout     time.Duration `env:"TIMEOUT" envDefault:"5s"`
}

func (c *AppConfig) Validate() error {
	if strings.TrimSpace(c.Service) == "" {
		return errors.New("service must not be blank")
	}

	if c.Port < 1 || c.Port > 65535 {
		return errors.New("port must be between 1 and 65535")
	}

	if c.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}

	return nil
}

cfg, err := config.Load[AppConfig](config.Options{Prefix: "ORDERS_"})
if err != nil {
	return err
}
```

This uses `errors`, `strings`, `time`, and `github.com/Penthious/go-foundation/config`. The prefix is
prepended verbatim, producing names such as `ORDERS_SERVICE` and `ORDERS_PORT`.
Use exported fields; `T` must be a struct, not a pointer. Supported conversions
include strings/named scalars, booleans, numbers, `time.Duration`, slices, maps,
optional pointers, and types implementing `encoding.TextUnmarshaler`.

`envDefault` applies when the variable is missing **or empty**. Explicit `false`
and `0` values override defaults and can be rejected by service validation.
`required` rejects an absent variable after defaults; combine it with `notEmpty`
to reject an explicitly empty value. Use `env:"-"` to skip a field. Untagged
scalar fields are ignored. Nested structs use their own field tags and can add
`envPrefix:"HTTP_"`; `envSeparator` and `envKeyValSeparator` configure collection
separators. Nested `Validate` methods are service-owned and must be called from
the top-level validator if needed.

The parser dependency is private; services use GOF's `Options` and return values.
Parser errors report field/variable names without values or underlying parser
causes, including when a custom text parser embeds its input in an error. Errors
for multiple fields are combined. Service validation errors preserve their
causes for `errors.Is`/`errors.As`; keep those messages free of secrets. Do not
log the entire loaded config or put credentials in source-controlled defaults.

For tests, pass `config.Options{Environment: map[string]string{...}}`. A non-nil
map completely replaces the process environment; an empty map selects defaults
without process-variable fallback. Nil uses the process environment. Options
and the map must not be mutated concurrently with loading. `Load` accepts zero
or one `Options` value. Values injected into running services should be treated
as immutable; changing the process environment later does not update them.

### Config in Fx

```go
app := fx.New(
	foundationfx.Module,
	foundationfx.Config[AppConfig](config.Options{Prefix: "ORDERS_"}),
	fx.Provide(func(cfg AppConfig, build buildinfo.Info) logger.Config {
		return logger.Config{
			Service: cfg.Service, Environment: cfg.Environment, Version: build.Version,
		}
	}),
	// Add service constructors that receive AppConfig and *logger.Logger.
)
```

`foundationfx.Config[T]` loads and validates `T` once during `fx.New`, even if no
other constructor consumes it. Invalid config appears through `app.Err()` before
any lifecycle hook starts. Services receive the concrete config value by type.
The base module also provides `buildinfo.Info`; logger and telemetry metadata
remain explicit so each service chooses how to compose them.

When telemetry must initialize before Fx/HTTP construction, load config once
first, initialize telemetry using that config and `buildinfo.Current()`, then
use `fx.Supply(cfg, sdk)` rather than registering `foundationfx.Config[T]` again.
The complete [Fx startup example](../foundationfx/README.md#dependency-injection-with-fx) follows that pattern. Configuration loading
does not create loggers, telemetry providers, clients, or listeners.
