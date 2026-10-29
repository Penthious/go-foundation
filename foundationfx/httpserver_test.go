package foundationfx_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/foundationfx"
	"github.com/Penthious/go-foundation/health"
	"github.com/Penthious/go-foundation/httpserver"
	"github.com/Penthious/go-foundation/logger"

	"go.uber.org/fx"
)

func TestHTTPServerModule_AutomaticLifecycle(t *testing.T) {
	var server *httpserver.Server
	app := fx.New(
		foundationfx.Module, foundationfx.HTTPServerModule, fx.NopLogger,
		fx.Supply(logger.Config{Service: "orders", Output: io.Discard}),
		fx.Supply(httpserver.Config{Address: "127.0.0.1:0", Routes: []httpserver.Route{{Pattern: "GET /orders/{id}", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, r.PathValue("id"))
		})}}}),
		fx.Decorate(func(s *httpserver.Server) *httpserver.Server {
			server = s
			return s
		}),
	)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}

	if server == nil || server.Address() != "127.0.0.1:0" {
		t.Fatal("module did not construct a server or opened its listener too early")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if err := app.Start(ctx); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := app.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Get("http://" + server.Address() + "/orders/123")
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != http.StatusOK || string(body) != "123" {
		t.Errorf("service handler = %d %q", resp.StatusCode, body)
	}

	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	if err := server.Start(ctx); !errors.Is(err, httpserver.ErrClosed) {
		t.Errorf("Fx did not stop the server: %v", err)
	}
}

func TestHTTPServerModule_StartupRollbackAndGraphFailure(t *testing.T) {
	for _, phase := range []string{"construction", "startup"} {
		t.Run(phase, func(t *testing.T) {
			var server *httpserver.Server
			opts := []fx.Option{
				foundationfx.Module, foundationfx.HTTPServerModule, fx.NopLogger,
				fx.Supply(logger.Config{Service: "orders", Output: io.Discard}),
				fx.Supply(httpserver.Config{Address: "127.0.0.1:0"}),
				fx.Populate(&server),
			}
			if phase == "construction" {
				opts = append(opts, fx.Invoke(func() error { return errors.New("construction failed") }))
			} else {
				opts = append(opts, fx.Invoke(func(lifecycle fx.Lifecycle) {
					lifecycle.Append(fx.Hook{OnStart: func(context.Context) error { return errors.New("startup failed") }})
				}))
			}

			app := fx.New(opts...)
			if phase == "construction" {
				if app.Err() == nil || server.Address() != "127.0.0.1:0" {
					t.Fatal("failed graph opened a listener or did not fail")
				}

				return
			}

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			if err := app.Start(ctx); err == nil {
				t.Fatal("expected startup failure")
			}

			if err := server.Start(ctx); !errors.Is(err, httpserver.ErrClosed) {
				t.Errorf("rollback left server running: %v", err)
			}
		})
	}
}

func TestHTTPServerModule_BindFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	defer listener.Close()
	app := fx.New(
		foundationfx.Module, foundationfx.HTTPServerModule, fx.NopLogger,
		fx.Supply(logger.Config{Service: "orders", Output: io.Discard}),
		fx.Supply(httpserver.Config{Address: listener.Addr().String()}),
	)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if err := app.Start(ctx); err == nil {
		t.Fatal("app accepted an occupied listen address")
	}
}

func TestHTTPServerModule_InjectsHealthChecks(t *testing.T) {
	var server *httpserver.Server
	app := fx.New(
		foundationfx.Module, foundationfx.HTTPServerModule, fx.NopLogger,
		fx.Supply(logger.Config{Service: "orders", Output: io.Discard}),
		fx.Supply(httpserver.Config{Address: "127.0.0.1:0"}),
		fx.Provide(func() (*health.Checker, error) {
			return health.New(health.Required("database", func(context.Context) error { return errors.New("unavailable") }))
		}),
		fx.Populate(&server),
	)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if err := app.Start(ctx); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := app.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Get("http://" + server.Address() + "/readyz")
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("injected health check did not affect readiness: %d", resp.StatusCode)
	}
}
