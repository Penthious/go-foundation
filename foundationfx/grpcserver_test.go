package foundationfx_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/foundationfx"
	"github.com/Penthious/go-foundation/grpcserver"
	"github.com/Penthious/go-foundation/health"
	"github.com/Penthious/go-foundation/logger"

	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	testpb "google.golang.org/grpc/interop/grpc_testing"
)

func TestGRPCServerModule_LifecycleAndHealth(t *testing.T) {
	var server *grpcserver.Server
	app := fx.New(
		foundationfx.GRPCServerModule, fx.NopLogger,
		fx.Supply(grpcConfig()),
		fx.Provide(grpcLogger),
		fx.Provide(func() (*health.Checker, error) {
			return health.New(health.Required("database", func(context.Context) error { return errors.New("unavailable") }))
		}),
		fx.Populate(&server),
	)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}

	if err := app.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = app.Stop(context.Background()) })
	connection, err := grpc.NewClient("passthrough:///"+server.Address(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}

	defer connection.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	response, err := healthpb.NewHealthClient(connection).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || response.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("shared health checker not injected: %v, %v", response, err)
	}

	if err := server.Ping(ctx); err != nil {
		t.Fatal("listener-only health callback recursively checked dependencies:", err)
	}

	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	if !errors.Is(server.Ping(ctx), grpcserver.ErrClosed) {
		t.Fatal("Fx did not close the gRPC server")
	}
}

func TestGRPCServerModule_ConfigurationAndStartupRollback(t *testing.T) {
	for _, scenario := range []string{"missing config", "bad config", "rollback"} {
		t.Run(scenario, func(t *testing.T) {
			var server *grpcserver.Server
			options := []fx.Option{foundationfx.GRPCServerModule, fx.NopLogger, fx.Provide(grpcLogger), fx.Populate(&server)}
			if scenario == "bad config" {
				options = append(options, fx.Supply(grpcserver.Config{}))
			} else if scenario == "rollback" {
				options = append(options, fx.Supply(grpcConfig()), fx.Invoke(func(lifecycle fx.Lifecycle, _ *grpcserver.Server) {
					lifecycle.Append(fx.Hook{OnStart: func(context.Context) error { return errors.New("later startup failed") }})
				}))
			}

			app := fx.New(options...)
			if scenario != "rollback" {
				if app.Err() == nil {
					t.Fatal("invalid RPC config accepted")
				}

				return
			}

			if err := app.Err(); err != nil {
				t.Fatal(err)
			}

			if err := app.Start(t.Context()); err == nil {
				t.Fatal("later startup failure was hidden")
			}

			if !errors.Is(server.Ping(t.Context()), grpcserver.ErrClosed) {
				t.Fatal("startup rollback did not drain gRPC")
			}
		})
	}
}

func grpcConfig() grpcserver.Config {
	return grpcserver.Config{Address: "127.0.0.1:0", Register: []func(grpc.ServiceRegistrar){func(registrar grpc.ServiceRegistrar) {
		testpb.RegisterTestServiceServer(registrar, &testpb.UnimplementedTestServiceServer{})
	}}}
}

func grpcLogger() (*logger.Logger, error) {
	return logger.New(logger.Config{Service: "grpc-fx-test", Output: io.Discard})
}
