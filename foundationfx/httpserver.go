package foundationfx

import (
	"context"

	"github.com/Penthious/go-foundation/health"
	"github.com/Penthious/go-foundation/httpserver"
	"github.com/Penthious/go-foundation/logger"

	"go.uber.org/fx"
)

// HTTPServerModule starts one HTTP server automatically. Include Module (or
// provide your own *logger.Logger) and provide an httpserver.Config with routes.
// A provided *health.Checker is injected unless config supplies its own checks.
// Keeping this module opt-in prevents existing applications from opening a port.
var HTTPServerModule = fx.Module("go-foundation/httpserver",
	fx.Provide(newHTTPServer),
	fx.Invoke(func(*httpserver.Server) {}),
)

type httpServerParams struct {
	fx.In

	Lifecycle fx.Lifecycle
	Shutdown  fx.Shutdowner
	Logger    *logger.Logger
	Config    httpserver.Config
	Health    *health.Checker `optional:"true"`
}

func newHTTPServer(p httpServerParams) (*httpserver.Server, error) {
	cfg := p.Config
	if cfg.Logger == nil {
		cfg.Logger = p.Logger
	}

	if cfg.Health == nil && len(cfg.ReadinessChecks) == 0 {
		cfg.Health = p.Health
	}

	server, err := httpserver.New(cfg)
	if err != nil {
		return nil, err
	}

	p.Lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := server.Start(ctx); err != nil {
				return err
			}

			go func() {
				if err := server.Wait(); err != nil {
					if err := p.Shutdown.Shutdown(fx.ExitCode(1)); err != nil {
						cfg.Logger.Error(context.Background(), "http server could not stop app", logger.Err(err))
					}
				}
			}()

			return nil
		},
		OnStop: server.Shutdown,
	})

	return server, nil
}
