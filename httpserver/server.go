// Package httpserver serves standard HTTP handlers with logging, OTel, and health checks.
package httpserver

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Penthious/go-foundation/health"
	"github.com/Penthious/go-foundation/logger"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// ErrClosed indicates that a server has shut down and cannot restart.
var ErrClosed = errors.New("httpserver: server is closed")

// Server owns one HTTP listener. Its zero value is not usable; construct it with New.
// It implements http.Handler for standard test servers and middleware composition.
type Server struct {
	cfg          Config
	healthChecks *health.Checker
	server       *http.Server
	mu           sync.Mutex
	listener     net.Listener
	closed       bool
	serveErr     error
	done         chan struct{}
	ready        atomic.Bool
}

// New validates routes and configuration without opening a listener. Configure
// telemetry before constructing the server to bind HTTP metrics to its provider.
func New(cfg Config) (*Server, error) {
	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}

	mux, err := routeMux(cfg.Routes)
	if err != nil {
		return nil, err
	}

	checks, err := newHealth(cfg)
	if err != nil {
		return nil, err
	}

	s := &Server{cfg: cfg, healthChecks: checks, done: make(chan struct{})}
	dispatch := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/livez", "/readyz", "/health", "/health/ui":
			r.Pattern = r.URL.Path
			s.health(w, r)
		default:
			mux.ServeHTTP(w, r)
		}
	})
	handler := otelhttp.NewHandler(s.observe(dispatch), "HTTP",
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			if r.Pattern != "" {
				return r.Pattern
			}

			return "HTTP"
		}),
	)
	observed := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state := &responseState{}
		ctx := context.WithValue(r.Context(), responseStateKey{}, state)
		handler.ServeHTTP(w, r.WithContext(ctx))
		// Let OTel finish recording before net/http aborts a partial response.
		if state.aborted {
			panic(http.ErrAbortHandler)
		}
	})
	s.server = &http.Server{
		Addr: cfg.Address, Handler: observed,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout, ReadTimeout: cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout, IdleTimeout: cfg.IdleTimeout,
		MaxHeaderBytes: cfg.MaxHeaderBytes,
		ErrorLog:       log.New(errorWriter{log: cfg.Logger}, "", 0),
	}

	return s, nil
}

// ServeHTTP serves a request through the same middleware as the owned listener.
// Readiness remains false until Start succeeds, including when using httptest.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.server.Handler.ServeHTTP(w, r)
}

// Start binds synchronously, so address conflicts fail startup, then serves in
// the background. It is idempotent after success. Observe Wait for later failures.
func (s *Server) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}

	if s.listener != nil {
		return nil
	}

	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", s.cfg.Address)
	if err != nil {
		return err
	}

	s.listener = listener
	s.ready.Store(true)
	go func() {
		err := s.server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}

		s.mu.Lock()
		s.serveErr = err
		s.closed = true
		s.ready.Store(false)
		s.mu.Unlock()
		if err != nil {
			s.cfg.Logger.Error(context.Background(), "http server failed", logger.Err(err))
		}

		close(s.done)
	}()
	s.cfg.Logger.Info(ctx, "http server started", logger.String("address", listener.Addr().String()))

	return nil
}

// Address returns the bound address after Start, otherwise the configured address.
func (s *Server) Address() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listener != nil {
		return s.listener.Addr().String()
	}

	return s.cfg.Address
}

// Wait blocks until the listener stops, returning nil for normal shutdown.
// Call it after successful Start; Shutdown before Start also unblocks it.
func (s *Server) Wait() error {
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.serveErr
}

// Shutdown marks the server unready and drains ordinary HTTP connections. If ctx
// expires, remaining connections are force-closed. Handlers must honor request
// cancellation. Hijacked connections, such as WebSockets, remain service-owned.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.ready.Store(false)
	if s.listener == nil && !s.closed {
		close(s.done)
	}

	s.closed = true
	s.mu.Unlock()

	err := s.server.Shutdown(ctx)
	if err != nil {
		return errors.Join(err, s.server.Close())
	}

	return nil
}

type errorWriter struct {
	log *logger.Logger
}

func (w errorWriter) Write(data []byte) (int, error) {
	w.log.Error(context.Background(), "http server diagnostic", logger.String("detail", strings.TrimSpace(string(data))))

	return len(data), nil
}
