package foundationfx_test

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/auth"
	"github.com/Penthious/go-foundation/foundationfx"
	"github.com/Penthious/go-foundation/grpcclient"
	"github.com/Penthious/go-foundation/grpcserver"
	"github.com/Penthious/go-foundation/logger"

	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestGRPCClientModule_LazyAndInvalidConfig(t *testing.T) {
	app := fx.New(foundationfx.GRPCClientModule(), fx.NopLogger)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}

	if err := app.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := app.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}

	for _, options := range [][]fx.Option{nil, {fx.Supply(grpcclient.Config{})}} {
		var client *grpcclient.Client
		base := []fx.Option{foundationfx.GRPCClientModule(), fx.NopLogger, fx.Provide(grpcClientLogger), fx.Populate(&client)}
		app := fx.New(append(base, options...)...)

		if app.Err() == nil || client != nil {
			t.Fatal("missing/invalid client config accepted")
		}
	}
}

func TestGRPCClientModule_CleanupAndNoStartupNetworkDependency(t *testing.T) {
	for _, scenario := range []string{"normal", "graph failure", "earlier hook", "later hook"} {
		t.Run(scenario, func(t *testing.T) {
			var client, same *grpcclient.Client
			options := []fx.Option{foundationfx.GRPCClientModule(), fx.NopLogger,
				fx.Provide(grpcClientLogger), fx.Supply(grpcclient.Config{Target: "127.0.0.1:1", Insecure: true}),
			}
			if scenario == "earlier hook" {
				options = append(options, fx.Invoke(func(lifecycle fx.Lifecycle) {
					lifecycle.Append(fx.Hook{OnStart: func(context.Context) error { return errors.New("earlier startup failure") }})
				}))
			}

			options = append(options, fx.Populate(&client, &same))
			if scenario == "graph failure" {
				options = append(options, fx.Invoke(func() error { return errors.New("graph failure") }))
			} else if scenario == "later hook" {
				options = append(options, fx.Invoke(func(lifecycle fx.Lifecycle) {
					lifecycle.Append(fx.Hook{OnStart: func(context.Context) error { return errors.New("later startup failure") }})
				}))
			}

			app := fx.New(options...)
			if scenario == "graph failure" {
				if app.Err() == nil || client == nil || !errors.Is(client.Ping(t.Context()), grpcclient.ErrClosed) {
					t.Fatal("graph failure did not close the channel")
				}

				return
			}

			if err := app.Err(); err != nil {
				t.Fatal(err)
			}

			if client != same {
				t.Fatal("client is not a singleton within app")
			}

			err := app.Start(t.Context())
			if scenario == "normal" {
				if err != nil {
					t.Fatal("unreachable upstream prevented startup:", err)
				}
			} else if err == nil {
				t.Fatal("startup hook error was ignored")
			}

			if err := app.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}

			if !errors.Is(client.Ping(t.Context()), grpcclient.ErrClosed) {
				t.Fatal("channel not closed after shutdown/startup rollback")
			}
		})
	}
}

func TestGRPCClientModule_GeneratedClientDependency(t *testing.T) {
	log, err := grpcClientLogger()
	if err != nil {
		t.Fatal(err)
	}

	server, err := grpcserver.New(grpcserver.Config{Logger: log, Address: "127.0.0.1:0", Register: []func(grpc.ServiceRegistrar){func(r grpc.ServiceRegistrar) {
		testpb.RegisterTestServiceServer(r, &testServiceClientFixture{})
	}}})
	if err != nil {
		t.Fatal(err)
	}

	if err := server.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = server.Shutdown(context.Background()) })
	var generated testpb.TestServiceClient
	app := fx.New(foundationfx.GRPCClientModule(), fx.NopLogger,
		fx.Supply(log, grpcclient.Config{Target: server.Address(), Insecure: true}),
		fx.Provide(func(connection *grpcclient.Client) testpb.TestServiceClient {
			return testpb.NewTestServiceClient(connection)
		}),
		fx.Populate(&generated),
	)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}

	if err := app.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = app.Stop(context.Background()) })
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	if _, err := generated.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatal("generated client DI failed:", err)
	}

	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

type testServiceClientFixture struct {
	testpb.UnimplementedTestServiceServer
	empty func(context.Context) error
	unary func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error)
}

func (s *testServiceClientFixture) EmptyCall(ctx context.Context, _ *testpb.Empty) (*testpb.Empty, error) {
	if s.empty != nil {
		if err := s.empty(ctx); err != nil {
			return nil, err
		}
	}

	return &testpb.Empty{}, nil
}

func (s *testServiceClientFixture) UnaryCall(ctx context.Context, request *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
	if s.unary != nil {
		return s.unary(ctx, request)
	}

	return &testpb.SimpleResponse{}, nil
}

func grpcClientLogger() (*logger.Logger, error) {
	return logger.New(logger.Config{Service: "grpc-client-fx-test", Output: io.Discard})
}

type namedGRPCConnections struct {
	fx.In

	Orders    *grpcclient.Client `name:"orders"`
	Inventory *grpcclient.Client `name:"inventory"`
	Default   *grpcclient.Client `optional:"true"`
}

type namedGeneratedClients struct {
	fx.In

	Orders    testpb.TestServiceClient `name:"orders"`
	Inventory testpb.TestServiceClient `name:"inventory"`
}

func TestNamedGRPCClientModule_IndependentUpstreamsAndTimeouts(t *testing.T) {
	ordersKey := strings.Repeat("o", 32)
	gate, err := auth.New(auth.Config{APIKeys: []auth.APIKey{{ID: "orders-caller", Secret: ordersKey}}})
	if err != nil {
		t.Fatal(err)
	}

	ordersEntered, inventoryEntered := make(chan struct{}), make(chan struct{})
	releaseInventory := make(chan struct{})
	var released bool
	defer func() {
		if !released {
			close(releaseInventory)
		}
	}()
	orders := namedGRPCServer(t, &testServiceClientFixture{
		empty: func(ctx context.Context) error {
			principal, ok := auth.FromContext(ctx)
			if !ok || principal.Subject != "orders-caller" {
				return status.Error(codes.Unauthenticated, "wrong orders identity")
			}

			return nil
		},
		unary: func(ctx context.Context, _ *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
			close(ordersEntered)
			<-ctx.Done()

			return nil, ctx.Err()
		},
	}, gate)
	inventory := namedGRPCServer(t, &testServiceClientFixture{
		empty: func(ctx context.Context) error {
			values, _ := metadata.FromIncomingContext(ctx)
			if len(values.Get("x-api-key")) != 0 || len(values.Get("authorization")) != 1 || values.Get("authorization")[0] != "Bearer inventory-token" {
				return status.Error(codes.Unauthenticated, "wrong inventory credentials")
			}

			return nil
		},
		unary: func(ctx context.Context, _ *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
			close(inventoryEntered)
			select {
			case <-releaseInventory:
				return &testpb.SimpleResponse{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}, nil)
	var connections namedGRPCConnections
	var generated namedGeneratedClients
	app := fx.New(
		foundationfx.GRPCClientModule(),
		foundationfx.NamedGRPCClientModule("orders"),
		foundationfx.NamedGRPCClientModule("inventory"),
		fx.NopLogger, fx.Provide(grpcClientLogger),
		fx.Supply(grpcclient.Config{Target: orders.Address(), Insecure: true, APIKey: ordersKey}),
		namedGRPCConfig("orders", grpcclient.Config{Target: orders.Address(), Insecure: true, APIKey: ordersKey, Timeout: 80 * time.Millisecond}),
		namedGRPCConfig("inventory", grpcclient.Config{Target: inventory.Address(), Insecure: true, BearerToken: "inventory-token", Timeout: time.Second}),
		fx.Provide(
			fx.Annotate(func(client *grpcclient.Client) testpb.TestServiceClient { return testpb.NewTestServiceClient(client) }, fx.ParamTags(`name:"orders"`), fx.ResultTags(`name:"orders"`)),
			fx.Annotate(func(client *grpcclient.Client) testpb.TestServiceClient { return testpb.NewTestServiceClient(client) }, fx.ParamTags(`name:"inventory"`), fx.ResultTags(`name:"inventory"`)),
		),
		fx.Invoke(func(c namedGRPCConnections, g namedGeneratedClients) { connections, generated = c, g }),
	)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}

	if err := app.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = app.Stop(context.Background()) })
	if connections.Orders == connections.Inventory || connections.Default == nil || connections.Default == connections.Orders || connections.Default == connections.Inventory {
		t.Fatal("named/default bindings shared a channel")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	for _, client := range []testpb.TestServiceClient{generated.Orders, generated.Inventory, testpb.NewTestServiceClient(connections.Default)} {
		if _, err := client.EmptyCall(ctx, &testpb.Empty{}); err != nil {
			t.Fatal("upstream-specific authentication failed:", err)
		}
	}

	ordersResult, inventoryResult := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := generated.Orders.UnaryCall(ctx, &testpb.SimpleRequest{})
		ordersResult <- err
	}()
	go func() {
		_, err := generated.Inventory.UnaryCall(ctx, &testpb.SimpleRequest{})
		inventoryResult <- err
	}()
	for _, entered := range []<-chan struct{}{ordersEntered, inventoryEntered} {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("named call did not reach its upstream")
		}
	}

	select {
	case err := <-ordersResult:
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("orders deadline not applied: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("orders deadline did not end the RPC")
	}

	select {
	case err := <-inventoryResult:
		t.Fatalf("inventory inherited orders' deadline: %v", err)
	default:
	}

	close(releaseInventory)
	released = true
	select {
	case err := <-inventoryResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("inventory did not complete")
	}

	if err := connections.Orders.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := generated.Inventory.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatal("closing orders affected inventory:", err)
	}

	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	for _, client := range []*grpcclient.Client{connections.Orders, connections.Inventory, connections.Default} {
		if !errors.Is(client.Ping(ctx), grpcclient.ErrClosed) {
			t.Fatal("app shutdown left a channel open")
		}
	}
}

func TestNamedGRPCClientModule_ValidationAndLazyBindings(t *testing.T) {
	for _, name := range []string{"", " ", "bad`name"} {
		if app := fx.New(foundationfx.NamedGRPCClientModule(name), fx.NopLogger); app.Err() == nil {
			t.Fatal("invalid client name accepted")
		}
	}

	app := fx.New(foundationfx.NamedGRPCClientModule("orders"), foundationfx.NamedGRPCClientModule("inventory"), fx.NopLogger)
	if err := app.Err(); err != nil {
		t.Fatal("unused named clients required config:", err)
	}

	if err := app.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := app.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}

	missing := fx.New(foundationfx.NamedGRPCClientModule("orders"), fx.NopLogger,
		fx.Provide(grpcClientLogger), fx.Supply(grpcclient.Config{Target: "127.0.0.1:1", Insecure: true}),
		fx.Invoke(fx.Annotate(func(*grpcclient.Client) {}, fx.ParamTags(`name:"orders"`))),
	)
	if missing.Err() == nil {
		t.Fatal("named client silently used an unnamed config")
	}

	duplicate := fx.New(foundationfx.NamedGRPCClientModule("orders"), foundationfx.NamedGRPCClientModule("orders"), fx.NopLogger)
	if duplicate.Err() == nil {
		t.Fatal("duplicate client name accepted")
	}

	name := `orders"quoted`
	var quoted *grpcclient.Client
	escaped := fx.New(foundationfx.NamedGRPCClientModule(name), fx.NopLogger, fx.Provide(grpcClientLogger),
		namedGRPCConfig(name, grpcclient.Config{Target: "127.0.0.1:1", Insecure: true}),
		fx.Invoke(fx.Annotate(func(client *grpcclient.Client) { quoted = client }, fx.ParamTags("name:"+strconv.Quote(name)))),
	)
	if err := escaped.Err(); err != nil {
		t.Fatal("valid quoted Fx name was not escaped:", err)
	}

	if err := escaped.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := escaped.Stop(t.Context()); err != nil || quoted == nil || !errors.Is(quoted.Ping(t.Context()), grpcclient.ErrClosed) {
		t.Fatal("escaped named binding failed lifecycle cleanup")
	}
}

func TestNamedGRPCClientModule_FailureClosesEveryConstructedChannel(t *testing.T) {
	for _, scenario := range []string{"graph failure", "earlier hook", "later hook"} {
		t.Run(scenario, func(t *testing.T) {
			var clients namedGRPCConnections
			options := []fx.Option{foundationfx.NamedGRPCClientModule("orders"), foundationfx.NamedGRPCClientModule("inventory"), fx.NopLogger, fx.Provide(grpcClientLogger),
				namedGRPCConfig("orders", grpcclient.Config{Target: "127.0.0.1:1", Insecure: true}),
				namedGRPCConfig("inventory", grpcclient.Config{Target: "127.0.0.1:1", Insecure: true}),
			}
			if scenario == "earlier hook" {
				options = append(options, fx.Invoke(func(lifecycle fx.Lifecycle) {
					lifecycle.Append(fx.Hook{OnStart: func(context.Context) error { return errors.New("earlier startup failure") }})
				}))
			}

			options = append(options, fx.Invoke(func(c namedGRPCConnections) error {
				clients = c
				if scenario == "graph failure" {
					return errors.New("graph failed after constructing named clients")
				}

				return nil
			}))
			if scenario == "later hook" {
				options = append(options, fx.Invoke(func(lifecycle fx.Lifecycle) {
					lifecycle.Append(fx.Hook{OnStart: func(context.Context) error { return errors.New("later startup failure") }})
				}))
			}

			app := fx.New(options...)
			if scenario == "graph failure" {
				if app.Err() == nil {
					t.Fatal("graph failure was ignored")
				}
			} else {
				if err := app.Err(); err != nil {
					t.Fatal(err)
				}

				if err := app.Start(t.Context()); err == nil {
					t.Fatal("startup failure was ignored")
				}
			}

			for _, client := range []*grpcclient.Client{clients.Orders, clients.Inventory} {
				if client == nil || !errors.Is(client.Ping(t.Context()), grpcclient.ErrClosed) {
					t.Fatal("failure left a named channel open")
				}
			}
		})
	}
}

func namedGRPCConfig(name string, cfg grpcclient.Config) fx.Option {
	return fx.Supply(fx.Annotate(cfg, fx.ResultTags("name:"+strconv.Quote(name))))
}

func TestNamedGRPCClientModule_IndependentCircuitBreakers(t *testing.T) {
	reader, output := newCircuitObservations(t)
	var calls atomic.Int32
	server := namedGRPCServer(t, &testServiceClientFixture{unary: func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		calls.Add(1)

		return nil, status.Error(codes.Unavailable, "upstream down")
	}}, nil)
	var clients namedGRPCConnections
	startApp(t, foundationfx.NamedGRPCClientModule("orders"), foundationfx.NamedGRPCClientModule("inventory"),
		fx.Supply(logger.Config{Service: "test", Output: output}),
		namedGRPCConfig("orders", grpcclient.Config{Name: "wrong-name", Target: server.Address(), Insecure: true,
			CircuitBreaker: &grpcclient.CircuitBreakerConfig{FailureThreshold: 1, OpenTimeout: time.Hour}}),
		namedGRPCConfig("inventory", grpcclient.Config{Target: server.Address(), Insecure: true,
			CircuitBreaker: &grpcclient.CircuitBreakerConfig{FailureThreshold: 2, OpenTimeout: time.Hour}}),
		fx.Populate(&clients),
	)
	for _, call := range []struct {
		client *grpcclient.Client
		open   bool
	}{{clients.Orders, false}, {clients.Orders, true}, {clients.Inventory, false}, {clients.Inventory, false}, {clients.Inventory, true}} {
		_, err := testpb.NewTestServiceClient(call.client).UnaryCall(t.Context(), &testpb.SimpleRequest{})
		if status.Code(err) != codes.Unavailable || errors.Is(err, grpcclient.ErrCircuitOpen) != call.open {
			t.Fatal("named circuits shared state or ignored their individual thresholds:", err)
		}
	}

	if calls.Load() != 3 {
		t.Fatal("named circuits did not independently control upstream RPCs")
	}

	assertCircuitObservations(t, reader, output, "grpc")
}

func namedGRPCServer(t *testing.T, fixture *testServiceClientFixture, gate *auth.Authenticator) *grpcserver.Server {
	t.Helper()

	log, err := grpcClientLogger()
	if err != nil {
		t.Fatal(err)
	}

	server, err := grpcserver.New(grpcserver.Config{Logger: log, Address: "127.0.0.1:0", Auth: gate,
		Register: []func(grpc.ServiceRegistrar){func(registrar grpc.ServiceRegistrar) { testpb.RegisterTestServiceServer(registrar, fixture) }},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := server.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_ = server.Shutdown(ctx)
	})

	return server
}
