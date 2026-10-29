package httpserver

import (
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/Penthious/go-foundation/logger"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		state := r.Context().Value(responseStateKey{}).(*responseState)
		w = state.wrap(w)
		var requestErr error
		var stack string
		defer func() {
			aborted := false
			if recovered := recover(); recovered != nil {
				if err, ok := recovered.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					requestErr = http.ErrAbortHandler
					aborted = true
				} else {
					requestErr = fmt.Errorf("httpserver: handler panic: %v", recovered)
					stack = string(debug.Stack())
					if !state.committed && state.hijacked == nil {
						// Recovery writes outside the route's compression middleware.
						w.Header().Del("Content-Encoding")
						http.Error(w, "internal server error", http.StatusInternalServerError)
					} else {
						aborted = true
					}
				}
			}

			if requestErr != nil {
				span := trace.SpanFromContext(r.Context())
				span.RecordError(requestErr)
				span.SetStatus(codes.Error, "request failed")
			}

			status := state.status
			if status == 0 && state.hijacked == nil && !aborted {
				status = http.StatusOK
			}

			fields := []logger.Field{
				logger.String("method", r.Method), logger.String("path", r.URL.EscapedPath()),
				logger.String("route", r.Pattern), logger.Int("status", status),
				logger.Int64("response_bytes", state.written), logger.Duration("duration", time.Since(started)),
				logger.Err(requestErr),
			}
			if stack != "" {
				fields = append(fields, logger.String("stack", stack))
			}

			switch {
			case status >= 500 || requestErr != nil:
				s.cfg.Logger.Error(r.Context(), "http request", fields...)
			case status >= 400:
				s.cfg.Logger.Warn(r.Context(), "http request", fields...)
			default:
				s.cfg.Logger.Info(r.Context(), "http request", fields...)
			}

			if aborted {
				if state.hijacked != nil {
					_ = state.hijacked.Close()
				}

				state.aborted = true
			}
		}()

		if r.ContentLength > s.cfg.MaxBodyBytes {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}

		if r.Body != nil && r.Body != http.NoBody {
			r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)
		}

		next.ServeHTTP(w, r)
	})
}
