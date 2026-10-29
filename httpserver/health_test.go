package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/buildinfo"
	"github.com/Penthious/go-foundation/health"
)

func TestHealth_ReadinessAndLiveness(t *testing.T) {
	var healthy atomic.Bool
	var checked atomic.Int32
	s := newServer(t, Config{
		ReadinessChecks: map[string]func(context.Context) error{"database": func(context.Context) error {
			checked.Add(1)
			if !healthy.Load() {
				return errors.New("private-connection-details")
			}

			return nil
		}},
		Routes: []Route{{Pattern: "/", Handler: http.NotFoundHandler()}},
	})
	before := httptest.NewRecorder()
	s.ServeHTTP(before, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if before.Code != http.StatusServiceUnavailable {
		t.Error("server was ready before startup")
	}

	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	resp, _ := request(t, s, http.MethodGet, "/livez", nil)
	if resp.StatusCode != http.StatusOK || checked.Load() != 0 {
		t.Error("liveness depends on external health")
	}

	resp, body := request(t, s, http.MethodGet, "/readyz", nil)
	if resp.StatusCode != http.StatusServiceUnavailable || strings.Contains(body, "private") || strings.Contains(body, "database") || !strings.Contains(body, "not_ready") {
		t.Errorf("unexpected failed readiness response: %d %q", resp.StatusCode, body)
	}

	healthy.Store(true)
	resp, body = request(t, s, http.MethodGet, "/readyz", nil)
	if resp.StatusCode != http.StatusOK || body != "{\"status\":\"ready\"}\n" || resp.Header.Get("Cache-Control") != "no-store" {
		t.Errorf("unexpected healthy readiness: %d %q", resp.StatusCode, body)
	}

	resp, body = request(t, s, http.MethodHead, "/readyz", nil)
	if resp.StatusCode != http.StatusOK || body != "" {
		t.Errorf("HEAD readiness = %d %q", resp.StatusCode, body)
	}

	resp, _ = request(t, s, http.MethodPost, "/livez", nil)
	if resp.StatusCode != http.StatusMethodNotAllowed || resp.Header.Get("Allow") != "GET, HEAD" {
		t.Error("health endpoint accepted an unsupported method")
	}
}

func TestHealth_CheckTimeout(t *testing.T) {
	canceled := make(chan struct{}, 1)
	s := startServer(t, Config{
		HealthTimeout: 10 * time.Millisecond,
		ReadinessChecks: map[string]func(context.Context) error{"slow": func(ctx context.Context) error {
			<-ctx.Done()
			canceled <- struct{}{}

			return nil
		}},
	})

	resp, _ := request(t, s, http.MethodGet, "/readyz", nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Error("check returned ready after exceeding its context budget")
	}

	receive(t, canceled)
}

func TestHealth_CannotBeShadowedByHostSpecificRouter(t *testing.T) {
	s := newServer(t, Config{Routes: []Route{{Pattern: "example.com/{path...}", Handler: http.NotFoundHandler()}}})
	req := httptest.NewRequest(http.MethodGet, "http://example.com/livez", nil)
	response := httptest.NewRecorder()

	s.ServeHTTP(response, req)

	if response.Code != http.StatusOK {
		t.Error("service router shadowed liveness")
	}
}

func TestHealth_BuildInfoAndDependencyBreakdown(t *testing.T) {
	var requiredUp atomic.Bool
	var requiredCalls, optionalCalls atomic.Int32
	requiredUp.Store(true)
	checks, err := health.New(
		health.Required("database", func(context.Context) error {
			requiredCalls.Add(1)
			if !requiredUp.Load() {
				return errors.New("private-database-password")
			}

			return nil
		}),
		health.Optional("cache", func(context.Context) error { optionalCalls.Add(1); return errors.New("private-cache-address") }),
	)
	if err != nil {
		t.Fatal(err)
	}

	s := startServer(t, Config{Health: checks})
	resp, body := request(t, s, http.MethodGet, "/livez", nil)
	var live struct {
		Status string         `json:"status"`
		Build  buildinfo.Info `json:"build"`
	}
	if err := json.Unmarshal([]byte(body), &live); err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != 200 || live.Status != "ok" || live.Build != buildinfo.Current() || requiredCalls.Load() != 0 || optionalCalls.Load() != 0 {
		t.Errorf("liveness ran checks or omitted metadata: %d %s", resp.StatusCode, body)
	}

	resp, _ = request(t, s, http.MethodGet, "/readyz", nil)
	if resp.StatusCode != 200 || requiredCalls.Load() != 1 || optionalCalls.Load() != 0 {
		t.Error("readiness ran optional checks or failed on an optional dependency")
	}

	resp, body = request(t, s, http.MethodGet, "/health", nil)
	var full healthResponse
	if err := json.Unmarshal([]byte(body), &full); err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != 200 || !full.Ready || full.Status != health.Degraded || len(full.Checks) != 2 || full.Build != live.Build || full.Checks[1].Required || strings.Contains(body, "private-") {
		t.Errorf("unexpected full health report: %d %s", resp.StatusCode, body)
	}

	requiredUp.Store(false)
	for _, path := range []string{"/readyz", "/health"} {
		resp, body = request(t, s, http.MethodGet, path, nil)
		if resp.StatusCode != http.StatusServiceUnavailable || strings.Contains(body, "private-") {
			t.Errorf("required failure on %s = %d %s", path, resp.StatusCode, body)
		}
	}

	resp, _ = request(t, s, http.MethodGet, "/livez", nil)
	if resp.StatusCode != http.StatusOK {
		t.Error("dependency failure affected liveness")
	}
}
