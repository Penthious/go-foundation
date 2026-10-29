package httpclient_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/httpclient"
	"github.com/Penthious/go-foundation/logger"
)

func TestAuth_Configuration(t *testing.T) {
	log, _ := newLogger(t, logger.LevelInfo)
	for _, cfg := range []httpclient.Config{
		{APIKey: "key", BearerToken: "token"},
		{APIKey: "key", TokenProvider: func(context.Context) (string, error) { return "token", nil }},
		{BearerToken: "token", TokenProvider: func(context.Context) (string, error) { return "token", nil }},
		{APIKey: "bad\r\nkey"}, {BearerToken: "Bearer token"},
		{APIKey: strings.Repeat("k", 4097)}, {BearerToken: strings.Repeat("t", (16<<10)+1)},
	} {
		cfg.Logger = log
		client, err := httpclient.New(cfg)

		if client != nil || err == nil {
			t.Fatal("invalid or conflicting credentials accepted")
		}
	}
}

func TestAuth_RetriesAndCurlIncludeCredentials(t *testing.T) {
	for _, mode := range []string{"API key", "bearer"} {
		t.Run(mode, func(t *testing.T) {
			var attempts atomic.Int32
			header, value := "X-API-Key", "orders-key"
			cfg := httpclient.Config{APIKey: value}
			if mode == "bearer" {
				header, value = "Authorization", "Bearer inventory-token"
				cfg = httpclient.Config{BearerToken: "inventory-token"}
			}

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get(header) != value {
					t.Errorf("received credentials = %q, want %q", r.Header.Get(header), value)
				}

				if attempts.Add(1) == 1 {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}

				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()

			log, sink := newLogger(t, logger.LevelDebug)
			cfg.Logger = log
			cfg.Retry = &httpclient.RetryConfig{MaxRetries: 1, WaitMin: time.Millisecond, WaitMax: time.Millisecond}
			client := newClient(t, cfg)
			req := newRequest(t, http.MethodGet, server.URL, nil)

			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}

			closeResponse(t, resp)
			if resp.StatusCode != http.StatusNoContent || attempts.Load() != 2 || len(req.Header) != 0 {
				t.Fatal("auth changed retry behavior or mutated the caller's headers")
			}

			records := sink.records(t)
			if len(records) != 2 {
				t.Fatalf("curl records = %d, want two attempts", len(records))
			}

			for _, record := range records {
				curl, _ := record.Fields["curl"].(string)
				if !strings.Contains(strings.ToLower(curl), strings.ToLower(header+": "+value)) {
					t.Fatal("curl omitted configured credentials")
				}
			}
		})
	}
}

func TestAuth_TokenProviderRefreshAndFailure(t *testing.T) {
	var lookups, attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		want := "Bearer token-1"
		if attempt == 3 {
			want = "Bearer token-2"
		}

		if r.Header.Get("Authorization") != want {
			t.Errorf("attempt %d authorization = %q, want %q", attempt, r.Header.Get("Authorization"), want)
		}

		if attempt == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	log, _ := newLogger(t, logger.LevelInfo)
	client := newClient(t, httpclient.Config{Logger: log,
		Retry: &httpclient.RetryConfig{MaxRetries: 1, WaitMin: time.Millisecond, WaitMax: time.Millisecond},
		TokenProvider: func(context.Context) (string, error) {
			return fmt.Sprintf("token-%d", lookups.Add(1)), nil
		},
	})
	for range 2 {
		resp, err := client.Do(newRequest(t, http.MethodGet, server.URL, nil))
		if err != nil {
			t.Fatal(err)
		}

		closeResponse(t, resp)
	}

	if lookups.Load() != 2 || attempts.Load() != 3 {
		t.Fatal("token was not refreshed per request or changed between retries")
	}

	for _, scenario := range []string{"error", "panic", "panic nil", "empty", "invalid", "timeout"} {
		t.Run(scenario, func(t *testing.T) {
			provider := func(ctx context.Context) (string, error) {
				switch scenario {
				case "error":
					return "", errors.New("secret-provider-detail")
				case "panic":
					panic("secret-provider-detail")
				case "panic nil":
					panic(nil)
				case "invalid":
					return "bad\ntoken", nil
				case "timeout":
					<-ctx.Done()
					return "", ctx.Err()
				default:
					return "", nil
				}
			}
			client := newClient(t, httpclient.Config{Logger: log, Timeout: 50 * time.Millisecond, TokenProvider: provider})
			var closes atomic.Int32
			req := newRequest(t, http.MethodPost, server.URL, &trackedBody{Reader: strings.NewReader("payload"), closes: &closes})

			resp, err := client.Do(req)
			if err == nil || resp != nil || strings.Contains(err.Error(), "secret-provider-detail") || closes.Load() != 1 {
				t.Fatal("provider failure was not safely returned with request body closed")
			}

			if scenario == "timeout" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("provider lookup escaped the request timeout:", err)
			}
		})
	}

	if attempts.Load() != 3 {
		t.Fatal("failed token lookup sent an HTTP request")
	}
}

func TestAuth_RejectsRequestCredentialConflicts(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
	}))
	defer server.Close()

	log, _ := newLogger(t, logger.LevelInfo)
	client := newClient(t, httpclient.Config{Logger: log, APIKey: "configured"})
	for _, scenario := range []string{"Authorization", "authorization", "X-API-Key", "URL user"} {
		req := newRequest(t, http.MethodGet, server.URL, nil)
		if scenario == "URL user" {
			req.URL.User = url.UserPassword("username", "password")
		} else {
			req.Header[scenario] = []string{"caller-secret"}
		}

		resp, err := client.Do(req)
		if resp != nil || err == nil || strings.Contains(err.Error(), "caller-secret") {
			t.Fatal("request credential conflict accepted")
		}
	}

	if calls.Load() != 0 {
		t.Fatal("conflicting credentials reached the upstream")
	}
}

func TestAuth_RedirectsStayWithinOriginalOrigin(t *testing.T) {
	var foreignCalls, sameOriginCalls, policyCalls atomic.Int32
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		foreignCalls.Add(1)
	}))
	defer foreign.Close()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sameOriginCalls.Add(1)
		if r.Header.Get("X-API-Key") != "orders-key" {
			t.Error("same-origin request lost its credentials")
		}

		switch r.URL.Path {
		case "/local":
			http.Redirect(w, r, "/ok", http.StatusFound)
		case "/foreign":
			http.Redirect(w, r, foreign.URL, http.StatusFound)
		case "/loop":
			http.Redirect(w, r, "/loop", http.StatusFound)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()

	log, _ := newLogger(t, logger.LevelInfo)
	client := newClient(t, httpclient.Config{Logger: log, APIKey: "orders-key"})
	for _, path := range []string{"/local", "/foreign"} {
		resp, err := client.Do(newRequest(t, http.MethodGet, server.URL+path, nil))
		if err != nil {
			t.Fatal(err)
		}

		closeResponse(t, resp)
		want := http.StatusNoContent
		if path == "/foreign" {
			want = http.StatusFound
		}

		if resp.StatusCode != want {
			t.Fatalf("%s status = %d, want %d", path, resp.StatusCode, want)
		}
	}

	if foreignCalls.Load() != 0 || sameOriginCalls.Load() != 3 {
		t.Fatal("redirect policy sent credentials to another origin")
	}

	_, err := client.Do(newRequest(t, http.MethodGet, server.URL+"/loop", nil))
	if err == nil || !strings.Contains(err.Error(), "10 redirects") {
		t.Fatal("default redirect limit was lost")
	}

	base := &http.Client{CheckRedirect: func(req *http.Request, via []*http.Request) error {
		policyCalls.Add(1)
		req.URL.Host = strings.TrimPrefix(foreign.URL, "http://")

		return nil
	}}
	custom := newClient(t, httpclient.Config{Logger: log, APIKey: "orders-key", Client: base})
	resp, err := custom.Do(newRequest(t, http.MethodGet, server.URL+"/local", nil))
	if err != nil {
		t.Fatal(err)
	}

	closeResponse(t, resp)
	if policyCalls.Load() != 1 || foreignCalls.Load() != 0 || resp.StatusCode != http.StatusFound {
		t.Fatal("custom redirect callback bypassed credential origin restrictions")
	}

	policyErr := errors.New("service redirect policy")
	base.CheckRedirect = func(*http.Request, []*http.Request) error {
		return policyErr
	}
	custom = newClient(t, httpclient.Config{Logger: log, APIKey: "orders-key", Client: base})
	if _, err := custom.Do(newRequest(t, http.MethodGet, server.URL+"/local", nil)); !errors.Is(err, policyErr) {
		t.Fatal("supplied redirect policy error was lost:", err)
	}
}
