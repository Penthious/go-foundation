package httpserver

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/Penthious/go-foundation/health"
	"github.com/Penthious/go-foundation/logger"
)

// Route pairs a Go ServeMux pattern, such as GET /orders/{id}, with a handler.
// Mount another standard router with the / pattern if needed.
type Route struct {
	Pattern string
	Handler http.Handler
}

// Config configures one HTTP listener and its handlers.
type Config struct {
	// Logger is required. Request logs omit query strings, headers, and bodies.
	Logger *logger.Logger
	// Address defaults to :8080. Use 127.0.0.1:0 for an ephemeral local port.
	Address string
	Routes  []Route
	// Health supplies required and optional dependencies for readiness and the UI.
	Health *health.Checker
	// ReadinessChecks registers legacy required checks. Do not combine with Health.
	// Deprecated: use Health with health.New and health.Required.
	ReadinessChecks map[string]func(context.Context) error
	// HealthTimeout defaults to 2s, shared by all checks in one request.
	// Callbacks must honor cancellation. Liveness never calls dependency checks.
	HealthTimeout time.Duration
	// Timeouts default to 5s, 30s, 30s, and 60s respectively. These are socket
	// deadlines, not handler context deadlines. Negative values are rejected.
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	// MaxHeaderBytes defaults to 1 MiB; MaxBodyBytes defaults to 1 MiB.
	// Larger streaming/upload limits must be configured explicitly.
	MaxHeaderBytes int
	MaxBodyBytes   int64
}

func normalizeConfig(cfg Config) (Config, error) {
	if cfg.Logger == nil {
		return Config{}, errors.New("httpserver: logger is required")
	}

	if cfg.MaxHeaderBytes < 0 || cfg.MaxBodyBytes < 0 {
		return Config{}, errors.New("httpserver: size limits must not be negative")
	}

	durations := []struct {
		value    *time.Duration
		fallback time.Duration
	}{
		{&cfg.HealthTimeout, 2 * time.Second},
		{&cfg.ReadHeaderTimeout, 5 * time.Second},
		{&cfg.ReadTimeout, 30 * time.Second},
		{&cfg.WriteTimeout, 30 * time.Second},
		{&cfg.IdleTimeout, 60 * time.Second},
	}
	for _, duration := range durations {
		if *duration.value < 0 {
			return Config{}, errors.New("httpserver: timeouts must not be negative")
		}

		if *duration.value == 0 {
			*duration.value = duration.fallback
		}
	}

	if cfg.Address == "" {
		cfg.Address = ":8080"
	}

	if cfg.MaxHeaderBytes == 0 {
		cfg.MaxHeaderBytes = 1 << 20
	}

	if cfg.MaxBodyBytes == 0 {
		cfg.MaxBodyBytes = 1 << 20
	}

	cfg.Routes = slices.Clone(cfg.Routes)
	cfg.ReadinessChecks = maps.Clone(cfg.ReadinessChecks)
	for name, check := range cfg.ReadinessChecks {
		if strings.TrimSpace(name) == "" || check == nil {
			return Config{}, errors.New("httpserver: readiness checks require a name and function")
		}
	}

	return cfg, nil
}

func routeMux(routes []Route) (mux *http.ServeMux, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("httpserver: invalid or conflicting routes: %v", recovered)
		}
	}()

	mux = http.NewServeMux()
	for _, route := range routes {
		if route.Handler == nil {
			return nil, fmt.Errorf("httpserver: handler is required for %q", route.Pattern)
		}

		parts := strings.Fields(route.Pattern)
		if len(parts) > 0 {
			path := parts[len(parts)-1]
			if slash := strings.IndexByte(path, '/'); slash >= 0 {
				path = path[slash:]
			}

			if isHealthPath(path) {
				return nil, fmt.Errorf("httpserver: %s is reserved for health checks", path)
			}
		}

		mux.Handle(route.Pattern, route.Handler)
	}

	return mux, nil
}
