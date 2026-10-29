package ratelimit

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/auth"
)

func TestMiddleware_IdentityIsolationAuthOrderAndHTTPContract(t *testing.T) {
	cfg := testConfig(t, Memory)
	cfg.Limit, cfg.Window, cfg.Burst = 1, time.Hour, 1
	limiter := testLimiter(t, cfg)
	alice, bob := strings.Repeat("a", 32), strings.Repeat("b", 32)
	gate, err := auth.New(auth.Config{APIKeys: []auth.APIKey{{ID: "alice", Secret: alice, Scopes: []string{"read"}}, {ID: "bob", Secret: bob, Scopes: []string{"read"}}}})
	if err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int32
	handler := limiter.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := auth.FromContext(r.Context()); !ok {
			t.Error("handler lost authenticated principal")
		}

		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	mux := http.NewServeMux()
	mux.Handle("/normal", gate.Protect(handler, "read"))
	mux.Handle("/scoped", gate.Protect(handler, "write"))
	server := httptest.NewServer(mux)
	defer server.Close()

	for i, call := range []struct {
		path   string
		key    string
		status int
	}{{"/normal", "", 401}, {"/normal", "invalid", 401}, {"/scoped", alice, 403}, {"/normal", alice, 204}, {"/normal", alice, 429}, {"/normal", bob, 204}} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+call.path, nil)
		if err != nil {
			t.Fatal(err)
		}

		req.Header.Set("X-API-Key", call.key)
		req.Header.Set("X-User-ID", "forged-user-"+strconv.Itoa(i))
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}

		if err := resp.Body.Close(); err != nil {
			t.Fatal(err)
		}

		if resp.StatusCode != call.status || strings.Contains(string(body), alice) || strings.Contains(string(body), bob) {
			t.Fatal("unexpected status or credentials leaked:", resp.StatusCode, string(body))
		}

		if call.status == 429 {
			seconds, err := strconv.Atoi(resp.Header.Get("Retry-After"))
			if err != nil || seconds < 1 || resp.Header.Get("Cache-Control") != "no-store" || string(body) != "{\"error\":\"rate_limit_exceeded\"}\n" {
				t.Fatal("invalid 429 contract:", resp.Header, string(body))
			}
		}
	}

	if calls.Load() != 2 {
		t.Fatal("rejected checks reached the handler")
	}
}

func TestMiddleware_MissingAuthAndUnavailableFailClosed(t *testing.T) {
	limiter := testLimiter(t, testConfig(t, Memory))
	handler := limiter.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("denied request reached handler")
	}))
	for _, authenticated := range []bool{false, true} {
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		request.Header.Set("X-API-Key", strings.Repeat("a", 32))
		want := http.StatusUnauthorized
		if authenticated {
			request = request.WithContext(apiContext(t, "alice", strings.Repeat("a", 32)))
			if err := limiter.Close(); err != nil {
				t.Fatal(err)
			}

			want = http.StatusServiceUnavailable
		}

		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != want {
			t.Fatal("missing auth/unavailable backend was allowed:", response.Code)
		}
	}
}
