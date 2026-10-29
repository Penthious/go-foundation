package foundationfx_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Penthious/go-foundation/auth"
	"github.com/Penthious/go-foundation/foundationfx"
	"github.com/Penthious/go-foundation/httpserver"
	"github.com/Penthious/go-foundation/logger"

	"go.uber.org/fx"
)

func TestAuthModule_LazyAndInvalidConfig(t *testing.T) {
	app := fx.New(foundationfx.AuthModule, fx.NopLogger)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}

	for _, options := range [][]fx.Option{nil, {fx.Supply(auth.Config{})}} {
		var authenticator *auth.Authenticator
		base := []fx.Option{foundationfx.AuthModule, fx.NopLogger, fx.Populate(&authenticator)}
		app := fx.New(append(base, options...)...)

		if app.Err() == nil || authenticator != nil {
			t.Fatal("missing/invalid auth config accepted")
		}
	}
}

func TestAuthModule_HTTPServerComposition(t *testing.T) {
	secret := strings.Repeat("s", 32)
	var authenticator, same *auth.Authenticator
	var server *httpserver.Server
	app := fx.New(
		foundationfx.AuthModule, foundationfx.HTTPServerModule, fx.NopLogger,
		fx.Supply(auth.Config{APIKeys: []auth.APIKey{{ID: "inventory", Secret: secret, Scopes: []string{"orders:read"}}}}),
		fx.Provide(func() (*logger.Logger, error) {
			return logger.New(logger.Config{Service: "orders-test", Output: io.Discard})
		}),
		fx.Provide(func(a *auth.Authenticator) httpserver.Config {
			return httpserver.Config{
				Address: "127.0.0.1:0",
				Routes: []httpserver.Route{
					{Pattern: "GET /orders", Handler: a.Protect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						p, _ := auth.FromContext(r.Context())
						_, _ = io.WriteString(w, p.Subject)
					}), "orders:read")},
					{Pattern: "GET /public", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(http.StatusNoContent)
					})},
				},
			}
		}),
		fx.Populate(&authenticator, &same, &server),
	)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}

	if authenticator != same {
		t.Fatal("authenticator is not shared within app")
	}

	if err := app.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = app.Stop(t.Context()) })
	for _, tt := range []struct {
		path   string
		key    string
		status int
	}{
		{"/orders", "", 401},
		{"/orders", secret, 200},
		{"/public", "", 204},
		{"/livez", "", 200},
		{"/readyz", "", 200},
	} {
		req := httptest.NewRequest("GET", tt.path, nil)
		if tt.key != "" {
			req.Header.Set("X-API-Key", tt.key)
		}

		response := httptest.NewRecorder()

		server.ServeHTTP(response, req)

		if response.Code != tt.status {
			t.Errorf("%s status = %d, want %d", tt.path, response.Code, tt.status)
		}

		if tt.key != "" && response.Body.String() != "inventory" {
			t.Error("handler did not receive authenticated principal")
		}
	}

	if err := app.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}
