package foundationfx_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/foundationfx"
	"github.com/Penthious/go-foundation/httpclient"
	"github.com/Penthious/go-foundation/logger"

	"go.uber.org/fx"
)

func TestModule_DefaultHTTPConfig(t *testing.T) {
	var client, sameClient *http.Client
	startApp(t,
		fx.Supply(logger.Config{Service: "orders", Output: io.Discard}),
		fx.Populate(&client, &sameClient),
	)

	if client == nil || client != sameClient || client.Timeout != 30*time.Second {
		t.Errorf("unexpected default HTTP client: %v, singleton=%v", client, client == sameClient)
	}
}

type namedHTTPClients struct {
	fx.In

	Orders    *http.Client `name:"orders"`
	Inventory *http.Client `name:"inventory"`
	Default   *http.Client `optional:"true"`
}

func TestNamedHTTPClientModule_IndependentUpstreams(t *testing.T) {
	var ordersAttempts, inventoryAttempts atomic.Int32
	orders := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != "orders-key" || r.Header.Get("Authorization") != "" {
			t.Error("orders received the wrong client credentials")
		}

		if r.URL.Path == "/slow" {
			<-r.Context().Done()
			return
		}

		ordersAttempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer orders.Close()

	inventory := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer inventory-token" || r.Header.Get("X-API-Key") != "" {
			t.Error("inventory received the wrong client credentials")
		}

		inventoryAttempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer inventory.Close()

	var clients, same namedHTTPClients
	app := startApp(t,
		foundationfx.NamedHTTPClientModule("orders"), foundationfx.NamedHTTPClientModule("inventory"),
		fx.Supply(logger.Config{Service: "http-fx-test", Output: io.Discard}),
		namedHTTPConfig("orders", httpclient.Config{Client: orders.Client(), Timeout: 80 * time.Millisecond, APIKey: "orders-key",
			Retry: &httpclient.RetryConfig{MaxRetries: 1, WaitMin: time.Millisecond, WaitMax: time.Millisecond}}),
		namedHTTPConfig("inventory", httpclient.Config{Client: inventory.Client(), Timeout: time.Second,
			Retry: &httpclient.RetryConfig{}, TokenProvider: func(context.Context) (string, error) { return "inventory-token", nil }}),
		fx.Populate(&clients, &same),
	)
	if clients.Orders == clients.Inventory || clients.Orders == clients.Default || clients.Inventory == clients.Default || clients.Orders != same.Orders || clients.Inventory != same.Inventory {
		t.Fatal("named bindings were not independent singletons alongside the default client")
	}

	if clients.Orders.Timeout != 80*time.Millisecond || clients.Inventory.Timeout != time.Second || clients.Default.Timeout != 30*time.Second {
		t.Fatal("client timeout configuration was mixed between bindings")
	}

	for _, upstream := range []struct {
		client *http.Client
		url    string
	}{{clients.Orders, orders.URL}, {clients.Inventory, inventory.URL}} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, upstream.url, nil)
		if err != nil {
			t.Fatal(err)
		}

		resp, err := upstream.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}

		if err := resp.Body.Close(); err != nil {
			t.Fatal(err)
		}
	}

	if ordersAttempts.Load() != 2 || inventoryAttempts.Load() != 1 {
		t.Fatal("named clients did not retain their independent retry settings")
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, orders.URL+"/slow", nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := clients.Orders.Do(req); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("orders did not apply its request timeout:", err)
	}

	clients.Orders.CloseIdleConnections()
	req, err = http.NewRequestWithContext(t.Context(), http.MethodGet, inventory.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := clients.Inventory.Do(req)
	if err != nil {
		t.Fatal("closing orders idle connections affected inventory:", err)
	}

	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}

	if err := app.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestNamedHTTPClientModule_ValidationAndLazyBindings(t *testing.T) {
	for _, name := range []string{"", " ", "bad`name"} {
		if app := fx.New(foundationfx.NamedHTTPClientModule(name), fx.NopLogger); app.Err() == nil {
			t.Fatal("invalid client name accepted")
		}
	}

	app := fx.New(foundationfx.NamedHTTPClientModule("orders"), foundationfx.NamedHTTPClientModule("inventory"), fx.NopLogger)
	if err := app.Err(); err != nil {
		t.Fatal("unused clients required configuration:", err)
	}

	if err := app.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := app.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}

	for _, options := range [][]fx.Option{
		{fx.Supply(httpclient.Config{Timeout: time.Second})},
		{namedHTTPConfig("orders", httpclient.Config{Timeout: -time.Second})},
		{namedHTTPConfig("orders", httpclient.Config{APIKey: "key", BearerToken: "token"})},
		{namedHTTPConfig("orders", httpclient.Config{Retry: &httpclient.RetryConfig{MaxRetries: -1}})},
		{namedHTTPConfig("orders", httpclient.Config{CircuitBreaker: &httpclient.CircuitBreakerConfig{FailureThreshold: -1}})},
	} {
		base := []fx.Option{foundationfx.Module, foundationfx.NamedHTTPClientModule("orders"), fx.NopLogger,
			fx.Supply(logger.Config{Service: "test", Output: io.Discard}),
			fx.Invoke(fx.Annotate(func(*http.Client) {}, fx.ParamTags(`name:"orders"`))),
		}
		if fx.New(append(base, options...)...).Err() == nil {
			t.Fatal("missing/invalid named config accepted or fell back to unnamed config")
		}
	}

	if fx.New(foundationfx.NamedHTTPClientModule("orders"), foundationfx.NamedHTTPClientModule("orders"), fx.NopLogger).Err() == nil {
		t.Fatal("duplicate named client accepted")
	}

	name := `orders"quoted`
	var quoted *http.Client
	startApp(t, foundationfx.NamedHTTPClientModule(name), fx.Supply(logger.Config{Service: "test", Output: io.Discard}),
		namedHTTPConfig(name, httpclient.Config{}),
		fx.Invoke(fx.Annotate(func(client *http.Client) { quoted = client }, fx.ParamTags("name:"+strconv.Quote(name)))),
	)
	if quoted == nil || quoted.Timeout != 30*time.Second {
		t.Fatal("escaped named binding did not retain base defaults")
	}
}

func TestNamedHTTPClientModule_Cleanup(t *testing.T) {
	for _, scenario := range []string{"normal", "graph failure", "earlier hook", "later hook"} {
		t.Run(scenario, func(t *testing.T) {
			orders, inventory := &httpCleanupTransport{}, &httpCleanupTransport{}
			options := []fx.Option{foundationfx.Module, fx.NopLogger,
				foundationfx.NamedHTTPClientModule("orders"), foundationfx.NamedHTTPClientModule("inventory"),
				fx.Supply(logger.Config{Service: "test", Output: io.Discard}),
				namedHTTPConfig("orders", httpclient.Config{Client: &http.Client{Transport: orders}, APIKey: "key", CircuitBreaker: &httpclient.CircuitBreakerConfig{}}),
				namedHTTPConfig("inventory", httpclient.Config{Client: &http.Client{Transport: inventory}, CircuitBreaker: &httpclient.CircuitBreakerConfig{}}),
			}
			if scenario == "earlier hook" {
				options = append(options, fx.Invoke(func(lifecycle fx.Lifecycle) {
					lifecycle.Append(fx.Hook{OnStart: func(context.Context) error { return errors.New("earlier failure") }})
				}))
			}

			options = append(options, fx.Invoke(func(clients namedHTTPClients) error {
				if scenario == "graph failure" {
					return errors.New("graph failure")
				}

				return nil
			}))
			if scenario == "later hook" {
				options = append(options, fx.Invoke(func(lifecycle fx.Lifecycle) {
					lifecycle.Append(fx.Hook{OnStart: func(context.Context) error { return errors.New("later failure") }})
				}))
			}

			app := fx.New(options...)
			if scenario == "graph failure" {
				if app.Err() == nil {
					t.Fatal("graph failure ignored")
				}
			} else {
				if err := app.Err(); err != nil {
					t.Fatal(err)
				}

				err := app.Start(t.Context())
				if (scenario == "normal") != (err == nil) {
					t.Fatal("unexpected startup result:", err)
				}

				if err := app.Stop(t.Context()); err != nil {
					t.Fatal(err)
				}
			}

			if orders.closes.Load() != 1 || inventory.closes.Load() != 1 {
				t.Fatalf("cleanup calls = orders %d, inventory %d; want one each", orders.closes.Load(), inventory.closes.Load())
			}
		})
	}
}

func namedHTTPConfig(name string, cfg httpclient.Config) fx.Option {
	return fx.Supply(fx.Annotate(cfg, fx.ResultTags("name:"+strconv.Quote(name))))
}

func TestNamedHTTPClientModule_IndependentCircuitBreakers(t *testing.T) {
	reader, output := newCircuitObservations(t)
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	var clients namedHTTPClients
	startApp(t, foundationfx.NamedHTTPClientModule("orders"), foundationfx.NamedHTTPClientModule("inventory"),
		fx.Supply(logger.Config{Service: "test", Output: output}),
		namedHTTPConfig("orders", httpclient.Config{Name: "wrong-name", Client: server.Client(), Retry: &httpclient.RetryConfig{},
			CircuitBreaker: &httpclient.CircuitBreakerConfig{FailureThreshold: 1, OpenTimeout: time.Hour}}),
		namedHTTPConfig("inventory", httpclient.Config{Client: server.Client(), Retry: &httpclient.RetryConfig{},
			CircuitBreaker: &httpclient.CircuitBreakerConfig{FailureThreshold: 2, OpenTimeout: time.Hour}}),
		fx.Populate(&clients),
	)
	for _, call := range []struct {
		client *http.Client
		open   bool
	}{{clients.Orders, false}, {clients.Orders, true}, {clients.Inventory, false}, {clients.Inventory, false}, {clients.Inventory, true}} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}

		resp, err := call.client.Do(req)
		if call.open {
			if !errors.Is(err, httpclient.ErrCircuitOpen) {
				t.Fatal("named circuit did not open at its configured threshold:", err)
			}
		} else {
			if err != nil {
				t.Fatal("another named client's circuit blocked this client:", err)
			}

			if err := resp.Body.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}

	if calls.Load() != 3 {
		t.Fatal("named circuits did not independently control upstream calls")
	}

	assertCircuitObservations(t, reader, output, "http")
}

type httpCleanupTransport struct {
	http.RoundTripper
	closes atomic.Int32
}

func (t *httpCleanupTransport) CloseIdleConnections() {
	t.closes.Add(1)
}

func TestModule_HTTPConfigAndLifecycle(t *testing.T) {
	closed := make(chan struct{}, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			closed <- struct{}{}
		}
	}
	server.StartTLS()
	defer server.Close()
	base := server.Client()
	var client *http.Client
	app := startApp(t,
		fx.Supply(logger.Config{Service: "orders", Output: io.Discard}),
		fx.Supply(httpclient.Config{Client: base, Timeout: 2 * time.Second, Retry: &httpclient.RetryConfig{MaxRetries: 0}}),
		fx.Populate(&client),
	)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}

	if client.Timeout != 2*time.Second || client == base {
		t.Error("HTTP configuration was not applied independently of the base client")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("Fx shutdown did not close the HTTP client's idle connection")
	}
}
