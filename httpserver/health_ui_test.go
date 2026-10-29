package httpserver

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/Penthious/go-foundation/buildinfo"
	"github.com/Penthious/go-foundation/health"
)

func TestHealthUI_FullReportAndEscaping(t *testing.T) {
	checks, err := health.New(
		health.Required("database", func(context.Context) error { return nil }),
		health.Optional("<script>alert(1)</script>", func(context.Context) error { return errors.New("private-password") }),
	)
	if err != nil {
		t.Fatal(err)
	}

	s := startServer(t, Config{Health: checks})
	resp, body := request(t, s, http.MethodGet, "/health/ui", nil)

	for _, text := range []string{"Service health", "Some systems degraded", "Ready to accept traffic", "database", "Required", "Optional", "check failed", "&lt;script&gt;", buildinfo.Current().Version, "Full JSON report"} {
		if !strings.Contains(body, text) {
			t.Errorf("UI missing %q", text)
		}
	}

	if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/html") || resp.Header.Get("Cache-Control") != "no-store" || resp.Header.Get("Content-Security-Policy") == "" || strings.Contains(body, "<script>") || strings.Contains(body, "private-password") {
		t.Errorf("unsafe or invalid UI response: %d", resp.StatusCode)
	}

	resp, body = request(t, s, http.MethodHead, "/health/ui", nil)
	if resp.StatusCode != 200 || body != "" {
		t.Error("HEAD UI did not preserve standard body semantics")
	}
}

func TestHealthUI_EmptyAndUnhealthy(t *testing.T) {
	for _, requiredDown := range []bool{false, true} {
		var registrations []health.Check
		if requiredDown {
			registrations = append(registrations, health.Required("database", func(context.Context) error { return errors.New("down") }))
		}

		checks, err := health.New(registrations...)
		if err != nil {
			t.Fatal(err)
		}

		s := startServer(t, Config{Health: checks})
		resp, body := request(t, s, http.MethodGet, "/health/ui", nil)
		if resp.StatusCode != http.StatusOK {
			t.Error("diagnostic UI should render even when dependencies fail")
		}

		if requiredDown && !strings.Contains(body, "Not ready to accept traffic") {
			t.Error("UI hid required dependency failure")
		}

		if !requiredDown && !strings.Contains(body, "No dependency checks registered") {
			t.Error("UI omitted empty state")
		}
	}
}
