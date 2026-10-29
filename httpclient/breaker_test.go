package httpclient_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/auth"
	"github.com/Penthious/go-foundation/httpclient"
	"github.com/Penthious/go-foundation/logger"
	"github.com/Penthious/go-foundation/ratelimit"
)

func TestCircuitBreaker_ValidationAndDisabledDefaults(t *testing.T) {
	log, _ := newLogger(t, logger.LevelInfo)
	for _, settings := range []*httpclient.CircuitBreakerConfig{{FailureThreshold: -1}, {OpenTimeout: -time.Second}} {
		client, err := httpclient.New(httpclient.Config{Logger: log, CircuitBreaker: settings})
		if err == nil || client != nil {
			t.Fatal("invalid breaker settings accepted")
		}
	}

	for _, enabled := range []bool{false, true} {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer server.Close()

		cfg := httpclient.Config{Logger: log, Retry: &httpclient.RetryConfig{}}
		if enabled {
			cfg.CircuitBreaker = &httpclient.CircuitBreakerConfig{}
		}

		client := newClient(t, cfg)
		for i := range 7 {
			resp, err := client.Do(newRequest(t, http.MethodGet, server.URL, nil))
			if enabled && i >= 5 {
				if !errors.Is(err, httpclient.ErrCircuitOpen) || resp != nil {
					t.Fatal("default breaker did not open after five failures")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}

				closeResponse(t, resp)
			}
		}

		want := int32(7)
		if enabled {
			want = 5
		}

		if calls.Load() != want {
			t.Fatalf("upstream calls = %d, want %d", calls.Load(), want)
		}
	}
}

func TestCircuitBreaker_HTTPStatusPolicy(t *testing.T) {
	for _, code := range []int{200, 400, 401, 403, 404, 408, 409, 429, 500, 501, 503} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
			}))
			defer server.Close()

			log, _ := newLogger(t, logger.LevelInfo)
			client := newClient(t, httpclient.Config{Logger: log, Retry: &httpclient.RetryConfig{},
				CircuitBreaker: &httpclient.CircuitBreakerConfig{FailureThreshold: 1, OpenTimeout: time.Hour},
			})
			resp, err := client.Do(newRequest(t, http.MethodGet, server.URL, nil))
			if err != nil || resp.StatusCode != code {
				t.Fatal("breaker replaced the triggering HTTP response:", err)
			}

			closeResponse(t, resp)
			resp, err = client.Do(newRequest(t, http.MethodGet, server.URL, nil))
			if code >= 500 {
				if !errors.Is(err, httpclient.ErrCircuitOpen) || resp != nil {
					t.Fatal("availability failure did not open circuit")
				}
			} else {
				if err != nil {
					t.Fatal("application response opened circuit:", err)
				}

				closeResponse(t, resp)
			}
		})
	}
}

func TestCircuitBreaker_QuotaDoesNotBlockOtherUsers(t *testing.T) {
	log, _ := newLogger(t, logger.LevelInfo)
	alice, bob := strings.Repeat("a", 32), strings.Repeat("b", 32)
	gate, err := auth.New(auth.Config{APIKeys: []auth.APIKey{{ID: "alice", Secret: alice}, {ID: "bob", Secret: bob}}})
	if err != nil {
		t.Fatal(err)
	}

	limiter, err := ratelimit.New(ratelimit.Config{Namespace: "breaker-test", Logger: log, Limit: 1, Window: time.Hour, Burst: 1})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = limiter.Close() })
	server := httptest.NewServer(gate.Protect(limiter.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))))
	t.Cleanup(server.Close)

	client := newClient(t, httpclient.Config{Logger: log, Retry: &httpclient.RetryConfig{},
		CircuitBreaker: &httpclient.CircuitBreakerConfig{FailureThreshold: 2, OpenTimeout: time.Hour}})
	for _, call := range []struct {
		secret string
		status int
	}{
		{alice, http.StatusNoContent},
		{alice, http.StatusTooManyRequests},
		{alice, http.StatusTooManyRequests},
		{alice, http.StatusTooManyRequests},
		{bob, http.StatusNoContent},
	} {
		req := newRequest(t, http.MethodGet, server.URL, nil)
		req.Header.Set("X-API-Key", call.secret)

		resp, err := client.Do(req)
		if err != nil {
			t.Fatal("quota rejection opened the shared circuit:", err)
		}

		closeResponse(t, resp)
		if resp.StatusCode != call.status {
			t.Fatalf("status = %d, want %d", resp.StatusCode, call.status)
		}

		if call.status == http.StatusTooManyRequests && resp.Header.Get("Retry-After") == "" {
			t.Fatal("quota response lost its retry guidance")
		}
	}
}

func TestCircuitBreaker_QuotaPreservesAvailabilityFailures(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}

		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)
	log, _ := newLogger(t, logger.LevelInfo)
	client := newClient(t, httpclient.Config{Logger: log, Retry: &httpclient.RetryConfig{},
		CircuitBreaker: &httpclient.CircuitBreakerConfig{FailureThreshold: 2, OpenTimeout: time.Hour}})

	for _, code := range []int{http.StatusServiceUnavailable, http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		resp, err := client.Do(newRequest(t, http.MethodGet, server.URL, nil))
		if err != nil {
			t.Fatal("quota rejection counted as an availability failure:", err)
		}

		closeResponse(t, resp)
		if resp.StatusCode != code {
			t.Fatalf("status = %d, want %d", resp.StatusCode, code)
		}
	}

	if _, err := client.Do(newRequest(t, http.MethodGet, server.URL, nil)); !errors.Is(err, httpclient.ErrCircuitOpen) {
		t.Fatal("quota rejection reset earlier availability failures:", err)
	}
}

func TestCircuitBreaker_QuotaBackoffDeadline(t *testing.T) {
	for _, scenario := range []string{"quota wait", "server wait", "quota then timeout"} {
		t.Run(scenario, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempt := calls.Add(1)
				if r.Header.Get("X-Test-User") == "bob" {
					w.WriteHeader(http.StatusNoContent)
					return
				}

				if scenario == "quota then timeout" && attempt > 1 {
					<-r.Context().Done()
					return
				}

				w.Header().Set("Retry-After", "60")
				if scenario == "quota then timeout" {
					w.Header().Set("Retry-After", "0")
				}

				if scenario == "server wait" {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}

				w.WriteHeader(http.StatusTooManyRequests)
			}))
			t.Cleanup(server.Close)
			log, _ := newLogger(t, logger.LevelInfo)
			client := newClient(t, httpclient.Config{Logger: log, Timeout: 200 * time.Millisecond,
				CircuitBreaker: &httpclient.CircuitBreakerConfig{FailureThreshold: 1, OpenTimeout: time.Hour}})

			if _, err := client.Do(newRequest(t, http.MethodGet, server.URL, nil)); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("retry sequence did not preserve its deadline:", err)
			}

			req := newRequest(t, http.MethodGet, server.URL, nil)
			req.Header.Set("X-Test-User", "bob")
			resp, err := client.Do(req)
			if scenario != "quota wait" {
				if !errors.Is(err, httpclient.ErrCircuitOpen) {
					t.Fatal("availability timeout did not open the circuit:", err)
				}

				return
			}

			if err != nil {
				t.Fatal("waiting for a quota retry opened the shared circuit:", err)
			}

			closeResponse(t, resp)
			if resp.StatusCode != http.StatusNoContent || calls.Load() != 2 {
				t.Fatal("another user could not reach the upstream")
			}
		})
	}
}

func TestCircuitBreaker_CountsFinalRetryOutcomeAndRejectsBeforeAuth(t *testing.T) {
	var attempts, lookups atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) != 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
		}

		_, _ = io.WriteString(w, "body")
	}))
	defer server.Close()

	log, sink := newLogger(t, logger.LevelDebug)
	client := newClient(t, httpclient.Config{Logger: log,
		Retry:          &httpclient.RetryConfig{MaxRetries: 1, WaitMin: time.Millisecond, WaitMax: time.Millisecond},
		CircuitBreaker: &httpclient.CircuitBreakerConfig{FailureThreshold: 2, OpenTimeout: time.Hour},
		TokenProvider: func(context.Context) (string, error) {
			lookups.Add(1)

			return "token", nil
		},
	})
	for range 3 {
		resp, err := client.Do(newRequest(t, http.MethodGet, server.URL, nil))
		if err != nil {
			t.Fatal("retry attempts were counted as separate breaker failures:", err)
		}

		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}

		closeResponse(t, resp)
		if string(data) != "body" {
			t.Fatal("breaker consumed a response body")
		}
	}

	var closes atomic.Int32
	resp, err := client.Do(newRequest(t, http.MethodPost, server.URL, &trackedBody{Reader: strings.NewReader("payload"), closes: &closes}))
	if resp != nil || !errors.Is(err, httpclient.ErrCircuitOpen) || closes.Load() != 1 {
		t.Fatal("open circuit did not reject and close the request body:", err)
	}

	curlRecords := 0
	for _, record := range sink.records(t) {
		if _, ok := record.Fields["curl"]; ok {
			curlRecords++
		}
	}

	if attempts.Load() != 6 || lookups.Load() != 3 || curlRecords != 6 {
		t.Fatal("open circuit performed auth lookup, transport attempt or curl logging")
	}
}

func TestCircuitBreaker_TimeoutsAndLocalAuthFailures(t *testing.T) {
	for _, scenario := range []string{"upstream timeout", "auth error", "auth timeout", "canceled caller"} {
		t.Run(scenario, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if scenario == "upstream timeout" {
					<-r.Context().Done()
					return
				}

				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()

			log, _ := newLogger(t, logger.LevelInfo)
			var first atomic.Bool
			cfg := httpclient.Config{Logger: log, Timeout: 50 * time.Millisecond, Retry: &httpclient.RetryConfig{},
				CircuitBreaker: &httpclient.CircuitBreakerConfig{FailureThreshold: 1, OpenTimeout: time.Hour}}
			if strings.HasPrefix(scenario, "auth") {
				cfg.TokenProvider = func(ctx context.Context) (string, error) {
					if first.CompareAndSwap(false, true) {
						if scenario == "auth timeout" {
							<-ctx.Done()
						}

						return "", errors.New("provider unavailable")
					}

					return "token", nil
				}
			}

			client := newClient(t, cfg)
			req := newRequest(t, http.MethodGet, server.URL, nil)
			if scenario == "canceled caller" {
				ctx, cancel := context.WithCancel(req.Context())
				cancel()
				req = req.WithContext(ctx)
			}

			if _, err := client.Do(req); err == nil {
				t.Fatal("expected request failure")
			}

			resp, err := client.Do(newRequest(t, http.MethodGet, server.URL, nil))
			if scenario == "upstream timeout" {
				if !errors.Is(err, httpclient.ErrCircuitOpen) || calls.Load() != 1 {
					t.Fatal("upstream timeout did not open the circuit:", err)
				}
			} else {
				if err != nil || calls.Load() != 1 {
					t.Fatal("local auth/cancellation failure opened the circuit:", err)
				}

				closeResponse(t, resp)
			}
		})
	}
}

func TestCircuitBreaker_RecoveryAllowsOneHTTPProbe(t *testing.T) {
	var calls atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) {
		case 1:
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		case 2:
			close(entered)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	log, _ := newLogger(t, logger.LevelInfo)
	client := newClient(t, httpclient.Config{Logger: log, Retry: &httpclient.RetryConfig{},
		CircuitBreaker: &httpclient.CircuitBreakerConfig{FailureThreshold: 1, OpenTimeout: time.Nanosecond}})
	resp, err := client.Do(newRequest(t, http.MethodGet, server.URL, nil))
	if err != nil {
		t.Fatal(err)
	}

	closeResponse(t, resp)
	type result struct {
		response *http.Response
		err      error
	}
	finished := make(chan result, 1)
	probeReq := newRequest(t, http.MethodGet, server.URL, nil).WithContext(ctx)
	go func() {
		response, err := client.Do(probeReq)
		finished <- result{response, err}
	}()

	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("recovery probe did not reach upstream")
	}

	if _, err := client.Do(newRequest(t, http.MethodGet, server.URL, nil)); !errors.Is(err, httpclient.ErrCircuitOpen) {
		t.Fatal("concurrent request bypassed the recovery probe:", err)
	}

	close(release)
	select {
	case completed := <-finished:
		if completed.err != nil {
			t.Fatal(completed.err)
		}

		closeResponse(t, completed.response)
	case <-ctx.Done():
		t.Fatal("recovery probe did not complete")
	}

	resp, err = client.Do(newRequest(t, http.MethodGet, server.URL, nil))
	if err != nil {
		t.Fatal("successful probe did not close circuit:", err)
	}

	closeResponse(t, resp)
	if calls.Load() != 3 {
		t.Fatal("unexpected HTTP attempts during circuit recovery")
	}
}
