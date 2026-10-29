package httpserver

import (
	"bytes"
	_ "embed"
	"html/template"
	"net/http"

	"github.com/Penthious/go-foundation/health"
	"github.com/Penthious/go-foundation/logger"
)

//go:embed health_ui.html
var healthHTML string

var healthTemplate = template.Must(template.New("health").Parse(healthHTML))

func (s *Server) healthUI(w http.ResponseWriter, r *http.Request) {
	report := s.snapshot(r.Context(), false)
	healthy := 0
	for _, result := range report.Checks {
		if result.Status == health.Up {
			healthy++
		}
	}

	view := struct {
		healthResponse
		Host    string
		Healthy int
	}{report, r.Host, healthy}
	var body bytes.Buffer
	if err := healthTemplate.Execute(&body, view); err != nil {
		s.cfg.Logger.Error(r.Context(), "health UI render failed", logger.Err(err))
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'")
	if r.Method != http.MethodHead {
		_, _ = body.WriteTo(w)
	}
}
