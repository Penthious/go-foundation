package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestNew_APIKeyValidation(t *testing.T) {
	key := APIKey{ID: "billing", Secret: strings.Repeat("k", 32), Scopes: []string{"orders:read"}}
	for _, tt := range []struct {
		name string
		cfg  Config
	}{
		{"no methods", Config{}},
		{"blank ID", Config{APIKeys: []APIKey{{Secret: key.Secret}}}},
		{"invalid ID", Config{APIKeys: []APIKey{{ID: "bad\n", Secret: key.Secret}}}},
		{"short secret", Config{APIKeys: []APIKey{{ID: "billing", Secret: "short"}}}},
		{"whitespace secret", Config{APIKeys: []APIKey{{ID: "billing", Secret: strings.Repeat(" ", 32)}}}},
		{"unicode secret", Config{APIKeys: []APIKey{{ID: "billing", Secret: strings.Repeat("é", 32)}}}},
		{"duplicate ID", Config{APIKeys: []APIKey{key, {ID: key.ID, Secret: strings.Repeat("x", 32)}}}},
		{"duplicate secret", Config{APIKeys: []APIKey{key, {ID: "other", Secret: key.Secret}}}},
		{"empty scope", Config{APIKeys: []APIKey{{ID: key.ID, Secret: key.Secret, Scopes: []string{""}}}}},
		{"whitespace scope", Config{APIKeys: []APIKey{{ID: key.ID, Secret: key.Secret, Scopes: []string{"read write"}}}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a, err := New(tt.cfg)

			if err == nil || a != nil || strings.Contains(err.Error(), key.Secret) {
				t.Fatalf("invalid config accepted or secret exposed: %v", err)
			}
		})
	}
}

func TestAuthenticator_ProtectAPIKeys(t *testing.T) {
	secret := strings.Repeat("k", 32)
	config := Config{APIKeys: []APIKey{
		{ID: "reader", Secret: secret, Scopes: []string{"orders:read"}},
		{ID: "writer", Secret: strings.Repeat("w", 32), Scopes: []string{"orders:write"}},
	}}
	a, err := New(config)
	if err != nil {
		t.Fatal(err)
	}

	config.APIKeys[0].Scopes[0] = "orders:write"
	config.APIKeys[0].Secret = "changed"
	for _, tt := range []struct {
		name    string
		headers http.Header
		scopes  []string
		status  int
	}{
		{"missing", http.Header{}, nil, 401},
		{"query ignored", http.Header{}, nil, 401},
		{"empty", http.Header{"X-Api-Key": {""}}, nil, 401},
		{"invalid", http.Header{"X-Api-Key": {"wrong"}}, nil, 401},
		{"duplicate", http.Header{"X-Api-Key": {secret, secret}}, nil, 401},
		{"joined", http.Header{"X-Api-Key": {secret + "," + secret}}, nil, 401},
		{"disabled bearer", http.Header{"Authorization": {"Bearer " + secret}}, nil, 401},
		{"mixed", http.Header{"X-Api-Key": {secret}, "Authorization": {"Bearer ignored"}}, nil, 401},
		{"reader", http.Header{"X-Api-Key": {secret}}, []string{"orders:read"}, 204},
		{"no requirements", http.Header{"X-Api-Key": {secret}}, nil, 204},
		{"missing permission", http.Header{"X-Api-Key": {secret}}, []string{"orders:write"}, 403},
		{"all required", http.Header{"X-Api-Key": {secret}}, []string{"orders:read", "orders:write"}, 403},
		{"case sensitive", http.Header{"X-Api-Key": {secret}}, []string{"Orders:read"}, 403},
		{"invalid requirement", http.Header{"X-Api-Key": {secret}}, []string{""}, 403},
	} {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			handler := a.Protect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				p, ok := FromContext(r.Context())
				if !ok || p.Subject != "reader" || p.Method != "api_key" || p.Issuer != "" || !p.HasScopes("orders:read") {
					t.Errorf("wrong identity: %+v, %v", p, ok)
				}

				p.Scopes[0] = "changed"
				again, _ := FromContext(r.Context())
				if !again.HasScopes("orders:read") {
					t.Error("context principal mutated through returned slice")
				}

				w.WriteHeader(http.StatusNoContent)
			}), tt.scopes...)
			req := httptest.NewRequest("GET", "/orders?api_key="+secret, nil)
			req.Header = tt.headers
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, req)

			if response.Code != tt.status || called != (tt.status == 204) {
				t.Fatalf("status = %d, called = %v, want %d", response.Code, called, tt.status)
			}

			if response.Header().Get("Cache-Control") != "no-store" || strings.Contains(response.Body.String(), secret) {
				t.Fatal("response may cache or expose credentials")
			}

			if tt.status == 401 && response.Header().Get("WWW-Authenticate") != "ApiKey" {
				t.Error("missing authentication challenge")
			}
		})
	}
}

func TestAuthenticator_ConcurrentRequestsAndCancellation(t *testing.T) {
	secret := strings.Repeat("s", 32)
	a, err := New(Config{APIKeys: []APIKey{{ID: "worker", Secret: secret, Scopes: []string{"run"}}}})
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := FromContext(context.Background()); ok {
		t.Fatal("unauthenticated context has an identity")
	}

	required := []string{"run"}
	handler := a.Protect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, _ := FromContext(r.Context())
		p.Scopes[0] = "changed"
		w.WriteHeader(204)
	}), required...)
	required[0] = "changed"
	var wg sync.WaitGroup
	for range 30 {
		wg.Go(func() {
			req := httptest.NewRequest("GET", "/", nil)
			req.Header.Set("X-API-Key", secret)
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, req)

			if response.Code != 204 {
				t.Errorf("concurrent authentication failed: %d", response.Code)
			}
		})
	}

	wg.Wait()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	req := httptest.NewRequest("GET", "/", nil).WithContext(ctx)
	req.Header.Set("X-API-Key", secret)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, req)

	if response.Code != 401 {
		t.Fatal("canceled request authenticated")
	}
}

func TestAuthenticator_AuthenticateTransportContext(t *testing.T) {
	secret := strings.Repeat("s", 32)
	a, err := New(Config{APIKeys: []APIKey{{ID: "service", Secret: secret, Scopes: []string{"read"}}}})
	if err != nil {
		t.Fatal(err)
	}

	headers := make(http.Header)
	headers.Set("x-api-key", secret)
	ctx, err := a.Authenticate(t.Context(), headers)
	if err != nil {
		t.Fatal(err)
	}

	principal, ok := FromContext(ctx)
	if !ok || principal.Subject != "service" || !principal.HasScopes("read") {
		t.Fatal("transport authentication did not install its principal")
	}

	headers.Add("authorization", "Bearer ambiguous")
	if _, err := a.Authenticate(t.Context(), headers); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("mixed transport credentials accepted")
	}

	if _, ok := FromContext(t.Context()); ok {
		t.Fatal("caller context was modified")
	}
}
