package foundationfx_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/auth"
	"github.com/Penthious/go-foundation/foundationfx"
	"github.com/Penthious/go-foundation/grpcclient"
	"github.com/Penthious/go-foundation/grpcserver"
	"github.com/Penthious/go-foundation/health"
	"github.com/Penthious/go-foundation/httpserver"
	"github.com/Penthious/go-foundation/logger"
	"github.com/Penthious/go-foundation/ratelimit"

	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestRateLimitModule_LazyConfigAndCleanup(t *testing.T) {
	unused := fx.New(foundationfx.RateLimitModule(), fx.NopLogger)
	if err := unused.Err(); err != nil {
		t.Fatal("unused limiter required config/logger:", err)
	}

	if err := unused.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := unused.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}

	for _, scenario := range []string{"normal", "missing config", "invalid config", "graph failure", "earlier hook", "later hook", "ping failure"} {
		t.Run(scenario, func(t *testing.T) {
			var limiter, same *ratelimit.Limiter
			options := []fx.Option{foundationfx.Module, foundationfx.RateLimitModule(), fx.NopLogger,
				fx.Supply(logger.Config{Service: "rate-fx-test", Output: io.Discard})}
			cfg := ratelimit.Config{Namespace: "orders"}
			if scenario == "invalid config" {
				cfg.Limit = -1
			}

			if scenario == "ping failure" {
				cfg.Backend, cfg.Redis, cfg.Timeout = ratelimit.Redis, &ratelimit.RedisConfig{Address: "127.0.0.1:1"}, 100*time.Millisecond
			}

			if scenario != "missing config" {
				options = append(options, fx.Supply(cfg))
			}

			if scenario == "earlier hook" {
				options = append(options, fx.Invoke(func(lifecycle fx.Lifecycle) {
					lifecycle.Append(fx.Hook{OnStart: func(context.Context) error {
						return errors.New("earlier failure")
					}})
				}))
			}

			options = append(options, fx.Populate(&limiter, &same))
			if scenario == "graph failure" {
				options = append(options, fx.Invoke(func() error { return errors.New("graph failure") }))
			}

			if scenario == "later hook" {
				options = append(options, fx.Invoke(func(lifecycle fx.Lifecycle) {
					lifecycle.Append(fx.Hook{OnStart: func(context.Context) error {
						return errors.New("later failure")
					}})
				}))
			}

			app := fx.New(options...)
			if scenario == "missing config" || scenario == "invalid config" || scenario == "graph failure" {
				if app.Err() == nil {
					t.Fatal("invalid graph was accepted")
				}

				if scenario == "graph failure" && (limiter == nil || !errors.Is(limiter.Ping(t.Context()), ratelimit.ErrClosed)) {
					t.Fatal("graph failure leaked a constructed limiter")
				}

				return
			}

			if err := app.Err(); err != nil {
				t.Fatal(err)
			}

			if limiter == nil || limiter != same {
				t.Fatal("limiter was not an app singleton")
			}

			err := app.Start(t.Context())
			if (scenario == "normal") != (err == nil) {
				t.Fatal("unexpected startup outcome:", err)
			}

			if err := app.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}

			if !errors.Is(limiter.Ping(t.Context()), ratelimit.ErrClosed) {
				t.Fatal("shutdown/startup failure did not close limiter")
			}
		})
	}
}

func TestRateLimitModule_RedisStartup(t *testing.T) {
	address := os.Getenv("GO_FOUNDATION_REDIS_TEST_ADDRESS")
	if address == "" {
		t.Skip("set GO_FOUNDATION_REDIS_TEST_ADDRESS to test against Redis")
	}

	var limiter *ratelimit.Limiter
	app := startApp(t, foundationfx.RateLimitModule(),
		fx.Supply(logger.Config{Service: "rate-fx-test", Output: io.Discard}),
		fx.Supply(ratelimit.Config{Backend: ratelimit.Redis, Namespace: "fx-rate-startup",
			Redis: &ratelimit.RedisConfig{Address: address, Password: os.Getenv("GO_FOUNDATION_REDIS_TEST_PASSWORD")}}),
		fx.Populate(&limiter),
	)
	if err := limiter.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := app.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}

	if !errors.Is(limiter.Ping(t.Context()), ratelimit.ErrClosed) {
		t.Fatal("Redis pool was not closed by Fx")
	}
}

func TestRateLimitModule_HTTPAndRPCShareAuthenticatedQuota(t *testing.T) {
	alice, bob := strings.Repeat("a", 32), strings.Repeat("b", 32)
	var httpServer *httpserver.Server
	var rpcServer *grpcserver.Server
	var limiter *ratelimit.Limiter
	var log *logger.Logger
	app := startApp(t, foundationfx.RateLimitModule(), foundationfx.AuthModule, foundationfx.HTTPServerModule, foundationfx.GRPCServerModule,
		fx.Supply(logger.Config{Service: "rate-fx-test", Output: io.Discard}),
		fx.Supply(ratelimit.Config{Namespace: "shared-transport", Limit: 1, Window: time.Hour, Burst: 1}),
		fx.Supply(auth.Config{APIKeys: []auth.APIKey{{ID: "alice", Secret: alice}, {ID: "bob", Secret: bob}}}),
		fx.Provide(func(limit *ratelimit.Limiter) (*health.Checker, error) {
			return health.New(health.Required("rate_limit", limit.Ping))
		}),
		fx.Provide(func(gate *auth.Authenticator, limit *ratelimit.Limiter) httpserver.Config {
			return httpserver.Config{Address: "127.0.0.1:0", Routes: []httpserver.Route{{Pattern: "GET /limited", Handler: gate.Protect(limit.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			})))}}}
		}),
		fx.Provide(func(gate *auth.Authenticator, limit *ratelimit.Limiter) grpcserver.Config {
			return grpcserver.Config{Address: "127.0.0.1:0", Auth: gate, RateLimiter: limit,
				Register: []func(grpc.ServiceRegistrar){func(registrar grpc.ServiceRegistrar) {
					testpb.RegisterTestServiceServer(registrar, &testServiceClientFixture{})
				}}}
		}),
		fx.Populate(&httpServer, &rpcServer, &limiter, &log),
	)
	client, err := grpcclient.New(grpcclient.Config{Target: rpcServer.Address(), Logger: log, Insecure: true})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+httpServer.Address()+"/limited", nil)
	if err != nil {
		t.Fatal(err)
	}

	request.Header.Set("X-API-Key", alice)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}

	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}

	if response.StatusCode != http.StatusNoContent {
		t.Fatal("HTTP middleware did not admit first request")
	}

	generated := testpb.NewTestServiceClient(client)
	_, err = generated.EmptyCall(metadata.NewOutgoingContext(ctx, metadata.Pairs("x-api-key", alice)), &testpb.Empty{})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatal("HTTP and RPC did not share identity quota:", err)
	}

	if _, err := generated.EmptyCall(metadata.NewOutgoingContext(ctx, metadata.Pairs("x-api-key", bob)), &testpb.Empty{}); err != nil {
		t.Fatal("one identity consumed another's quota:", err)
	}

	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	if !errors.Is(limiter.Ping(ctx), ratelimit.ErrClosed) {
		t.Fatal("shared limiter survived app shutdown")
	}
}
