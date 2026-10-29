package httpserver

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/health"
)

func TestNew_DefaultsAndValidation(t *testing.T) {
	log := testLogger(t, io.Discard)
	s, err := New(Config{Logger: log})
	if err != nil {
		t.Fatal(err)
	}

	if s.Address() != ":8080" || s.listener != nil || s.cfg.MaxBodyBytes != 1<<20 || s.cfg.ReadHeaderTimeout != 5*time.Second {
		t.Error("unexpected defaults or constructor opened a listener")
	}

	tests := []struct {
		name string
		edit func(*Config)
	}{
		{"logger", func(cfg *Config) { cfg.Logger = nil }},
		{"health timeout", func(cfg *Config) { cfg.HealthTimeout = -1 }},
		{"read header timeout", func(cfg *Config) { cfg.ReadHeaderTimeout = -1 }},
		{"read timeout", func(cfg *Config) { cfg.ReadTimeout = -1 }},
		{"write timeout", func(cfg *Config) { cfg.WriteTimeout = -1 }},
		{"idle timeout", func(cfg *Config) { cfg.IdleTimeout = -1 }},
		{"header size", func(cfg *Config) { cfg.MaxHeaderBytes = -1 }},
		{"body size", func(cfg *Config) { cfg.MaxBodyBytes = -1 }},
		{"empty check name", func(cfg *Config) {
			cfg.ReadinessChecks = map[string]func(context.Context) error{"": func(context.Context) error { return nil }}
		}},
		{"nil check", func(cfg *Config) { cfg.ReadinessChecks = map[string]func(context.Context) error{"db": nil} }},
		{"nil handler", func(cfg *Config) { cfg.Routes = []Route{{Pattern: "/orders"}} }},
		{"invalid pattern", func(cfg *Config) { cfg.Routes = []Route{{Pattern: "bad pattern", Handler: http.NotFoundHandler()}} }},
		{"conflict", func(cfg *Config) {
			cfg.Routes = []Route{{Pattern: "/orders", Handler: http.NotFoundHandler()}, {Pattern: "/orders", Handler: http.NotFoundHandler()}}
		}},
		{"reserved liveness", func(cfg *Config) { cfg.Routes = []Route{{Pattern: "GET /livez", Handler: http.NotFoundHandler()}} }},
		{"reserved readiness host", func(cfg *Config) {
			cfg.Routes = []Route{{Pattern: "POST example.com/readyz", Handler: http.NotFoundHandler()}}
		}},
		{"reserved full health", func(cfg *Config) { cfg.Routes = []Route{{Pattern: "GET /health", Handler: http.NotFoundHandler()}} }},
		{"reserved health UI", func(cfg *Config) { cfg.Routes = []Route{{Pattern: "GET /health/ui", Handler: http.NotFoundHandler()}} }},
		{"conflicting health sources", func(cfg *Config) {
			cfg.Health = &health.Checker{}
			cfg.ReadinessChecks = map[string]func(context.Context) error{"db": func(context.Context) error { return nil }}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{Logger: log}
			tt.edit(&cfg)

			if _, err := New(cfg); err == nil {
				t.Fatal("accepted invalid configuration")
			}
		})
	}
}

func TestNew_CopiesConfiguration(t *testing.T) {
	cfg := Config{
		Routes: []Route{{Pattern: "GET /orders", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})}},
		ReadinessChecks: map[string]func(context.Context) error{"db": func(context.Context) error { return nil }},
	}
	s := newServer(t, cfg)
	cfg.Routes[0].Handler = http.NotFoundHandler()
	cfg.ReadinessChecks["db"] = nil
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{"/orders", "/readyz"} {
		resp, _ := request(t, s, http.MethodGet, path, nil)
		if resp.StatusCode >= 400 {
			t.Errorf("caller mutation affected %s: %d", path, resp.StatusCode)
		}
	}
}
