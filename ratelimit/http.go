package ratelimit

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/Penthious/go-foundation/auth"
)

// Middleware limits an already authenticated identity. Compose auth outside it:
// gate.Protect(limiter.Middleware(handler)). Missing principals receive 401;
// quota denial receives 429 and Retry-After; backend errors receive 503.
// Quotas are shared across all routes using this limiter, including HTTP/gRPC.
func (l *Limiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		decision, err := l.allowPrincipal(r.Context())
		if err == nil && decision.Allowed {
			next.ServeHTTP(w, r)
			return
		}

		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case errors.Is(err, auth.ErrUnauthenticated):
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte("{\"error\":\"unauthorized\"}\n"))
		case err != nil:
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("{\"error\":\"rate_limiter_unavailable\"}\n"))
		default:
			seconds := (decision.RetryAfter + time.Second - 1) / time.Second
			w.Header().Set("Retry-After", strconv.FormatInt(int64(seconds), 10))
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("{\"error\":\"rate_limit_exceeded\"}\n"))
		}
	})
}
