package foundationfx

import (
	"context"

	"github.com/Penthious/go-foundation/grpcserver"
	"github.com/Penthious/go-foundation/health"
	"github.com/Penthious/go-foundation/logger"

	"go.uber.org/fx"
)

// GRPCServerModule starts an opt-in gRPC listener from grpcserver.Config. It
// injects the application logger and an optional shared health checker, drains
// RPCs during shutdown, and requests app shutdown on unexpected listener failure.
var GRPCServerModule = fx.Module("go-foundation/grpcserver",
	fx.Provide(newGRPCServer),
	fx.Invoke(func(*grpcserver.Server) {}),
)

type grpcServerParams struct {
	fx.In

	Lifecycle fx.Lifecycle
	Shutdown  fx.Shutdowner
	Logger    *logger.Logger
	Config    grpcserver.Config
	Health    *health.Checker `optional:"true"`
}

func newGRPCServer(p grpcServerParams) (*grpcserver.Server, error) {
	cfg := p.Config
	if cfg.Logger == nil {
		cfg.Logger = p.Logger
	}

	if cfg.Health == nil {
		cfg.Health = p.Health
	}

	server, err := grpcserver.New(cfg)
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
						cfg.Logger.Error(context.Background(), "grpc server could not stop app", logger.Err(err))
					}
				}
			}()

			return nil
		},
		OnStop: server.Shutdown,
	})

	return server, nil
}
