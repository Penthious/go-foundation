package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"slices"
	"time"

	"github.com/Penthious/go-foundation/buildinfo"
	"github.com/Penthious/go-foundation/health"
	"github.com/Penthious/go-foundation/logger"
)

type healthResponse struct {
	health.Report
	Build buildinfo.Info `json:"build"`
}

func isHealthPath(path string) bool {
	return path == "/livez" || path == "/readyz" || path == "/health" || path == "/health/ui"
}

func newHealth(cfg Config) (*health.Checker, error) {
	if cfg.Health != nil {
		if len(cfg.ReadinessChecks) != 0 {
			return nil, errors.New("httpserver: use Health or ReadinessChecks, not both")
		}

		return cfg.Health, nil
	}

	checks := make([]health.Check, 0, len(cfg.ReadinessChecks))
	for _, name := range slices.Sorted(maps.Keys(cfg.ReadinessChecks)) {
		checks = append(checks, health.Required(name, cfg.ReadinessChecks[name]))
	}

	return health.New(checks...)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if r.URL.Path == "/health/ui" {
		s.healthUI(w, r)
		return
	}

	var body any
	code := http.StatusOK
	switch r.URL.Path {
	case "/livez":
		body = struct {
			Status string         `json:"status"`
			Build  buildinfo.Info `json:"build"`
		}{Status: "ok", Build: buildinfo.Current()}
	case "/readyz":
		status := "ready"
		if !s.snapshot(r.Context(), true).Ready {
			status, code = "not_ready", http.StatusServiceUnavailable
		}

		body = struct {
			Status string `json:"status"`
		}{Status: status}
	case "/health":
		report := s.snapshot(r.Context(), false)
		if !report.Ready {
			code = http.StatusServiceUnavailable
		}

		body = report
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	if r.Method != http.MethodHead {
		_ = json.NewEncoder(w).Encode(body)
	}
}

func (s *Server) snapshot(ctx context.Context, requiredOnly bool) healthResponse {
	if requiredOnly && !s.ready.Load() {
		return healthResponse{
			Report: health.Report{Status: health.Down, CheckedAt: time.Now().UTC(), Checks: []health.Result{}},
			Build:  buildinfo.Current(),
		}
	}

	ctx, cancel := context.WithTimeout(ctx, s.cfg.HealthTimeout)
	defer cancel()

	var report health.Report
	if requiredOnly {
		report = s.healthChecks.Readiness(ctx)
	} else {
		report = s.healthChecks.Check(ctx)
	}

	for _, result := range report.Checks {
		if result.Err != nil {
			s.cfg.Logger.Warn(ctx, "http health check failed", logger.String("check", result.Name), logger.Bool("required", result.Required), logger.Err(result.Err))
		}
	}

	if !s.ready.Load() {
		report.Status, report.Ready = health.Down, false
	}

	return healthResponse{Report: report, Build: buildinfo.Current()}
}
