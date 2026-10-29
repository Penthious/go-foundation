package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/logger"
)

func TestServer_RoutesAndLifecycle(t *testing.T) {
	s := startServer(t, Config{Routes: []Route{{Pattern: "GET /orders/{id}", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, r.PathValue("id"))
	})}}})
	address := s.Address()
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	if address != s.Address() {
		t.Error("Start opened another listener")
	}

	tests := []struct {
		method string
		path   string
		status int
		body   string
	}{
		{http.MethodGet, "/orders/123", http.StatusAccepted, "123"},
		{http.MethodHead, "/orders/123", http.StatusAccepted, ""},
		{http.MethodPost, "/orders/123", http.StatusMethodNotAllowed, ""},
		{http.MethodGet, "/missing", http.StatusNotFound, ""},
	}
	for _, tt := range tests {
		t.Run(tt.method+tt.path, func(t *testing.T) {
			resp, body := request(t, s, tt.method, tt.path, nil)

			if resp.StatusCode != tt.status || (tt.status == http.StatusAccepted && body != tt.body) {
				t.Errorf("response = %d %q", resp.StatusCode, body)
			}
		})
	}

	if err := s.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := s.Wait(); err != nil {
		t.Fatal(err)
	}

	if err := s.Start(t.Context()); !errors.Is(err, ErrClosed) {
		t.Errorf("restart = %v", err)
	}
}

func TestServer_BindFailureAndCanceledStartup(t *testing.T) {
	s := startServer(t, Config{})
	other := newServer(t, Config{Address: s.Address()})
	if err := other.Start(t.Context()); err == nil {
		t.Fatal("accepted occupied address")
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := other.Start(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled startup = %v", err)
	}

	if err := other.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := other.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestServer_ShutdownDrainsRequests(t *testing.T) {
	entered, release, stopping := make(chan struct{}), make(chan struct{}), make(chan struct{})
	s := startServer(t, Config{Routes: []Route{{Pattern: "GET /work", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-release:
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
		}
	})}}})
	var shutdownSignal sync.Once
	s.server.RegisterOnShutdown(func() { shutdownSignal.Do(func() { close(stopping) }) })
	response := make(chan int, 1)
	go func() {
		resp, err := http.Get("http://" + s.Address() + "/work")
		if err != nil {
			response <- 0
			return
		}

		defer resp.Body.Close()
		response <- resp.StatusCode
	}()
	receive(t, entered)
	stopped := make(chan error, 1)
	go func() { stopped <- s.Shutdown(t.Context()) }()
	receive(t, stopping)
	recorder := httptest.NewRecorder()
	s.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Error("server remained ready during shutdown")
	}

	close(release)
	if status := receive(t, response); status != http.StatusNoContent {
		t.Errorf("active request was interrupted: %d", status)
	}

	if err := receive(t, stopped); err != nil {
		t.Fatal(err)
	}
}

func TestServer_ShutdownDeadlineClosesActiveConnections(t *testing.T) {
	entered, canceled := make(chan struct{}), make(chan struct{})
	s := startServer(t, Config{Routes: []Route{{Pattern: "GET /work", Handler: http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(canceled)
	})}}})
	done := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + s.Address() + "/work")
		if resp != nil {
			resp.Body.Close()
		}

		done <- err
	}()
	receive(t, entered)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := s.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("forced shutdown = %v", err)
	}

	receive(t, canceled)
	if err := receive(t, done); err == nil {
		t.Error("forced connection close unexpectedly returned a successful response")
	}
}

func TestServer_UnexpectedListenerFailure(t *testing.T) {
	s := startServer(t, Config{})
	if err := s.listener.Close(); err != nil {
		t.Fatal(err)
	}

	if err := s.Wait(); err == nil {
		t.Error("listener failure was hidden")
	}

	if s.ready.Load() {
		t.Error("failed server remained ready")
	}
}

func TestServer_StartupContextDoesNotCancelRequests(t *testing.T) {
	s := newServer(t, Config{Routes: []Route{{Pattern: "GET /", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Context().Err() != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})}}})
	ctx, cancel := context.WithCancel(t.Context())
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}

	cancel()
	resp, _ := request(t, s, http.MethodGet, "/", nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Error("startup context leaked into request lifetime")
	}
}

func TestServer_HijackPreservesStandardHandlerSupport(t *testing.T) {
	s := startServer(t, Config{Routes: []Route{{Pattern: "GET /upgrade", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, buffer, err := http.NewResponseController(w).Hijack()
		if err != nil {
			panic(err)
		}

		defer conn.Close()
		if _, err := buffer.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\nready"); err != nil {
			return
		}

		_ = buffer.Flush()
	})}}})

	resp, body := request(t, s, http.MethodGet, "/upgrade", nil)
	if resp.StatusCode != http.StatusSwitchingProtocols || body != "ready" {
		t.Errorf("hijacked response = %d %q", resp.StatusCode, body)
	}
}

func newServer(t *testing.T, cfg Config) *Server {
	t.Helper()

	if cfg.Logger == nil {
		cfg.Logger = testLogger(t, io.Discard)
	}

	if cfg.Address == "" {
		cfg.Address = "127.0.0.1:0"
	}

	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := s.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})

	return s
}

func startServer(t *testing.T, cfg Config) *Server {
	t.Helper()

	s := newServer(t, cfg)
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	return s
}

func testLogger(t *testing.T, output io.Writer) *logger.Logger {
	t.Helper()

	log, err := logger.New(logger.Config{Service: "server-test", Output: output})
	if err != nil {
		t.Fatal(err)
	}

	return log
}

func request(t *testing.T, s *Server, method, path string, body io.Reader) (*http.Response, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, "http://"+s.Address()+path, body)
	if err != nil {
		t.Fatal(err)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	return resp, string(data)
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()

	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for test signal")
		var zero T
		return zero
	}
}

type logBuffer struct {
	mu   sync.Mutex
	data bytes.Buffer
}

func (b *logBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.data.Write(data)
}

func (b *logBuffer) records(t *testing.T) []map[string]any {
	t.Helper()

	b.mu.Lock()
	defer b.mu.Unlock()

	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(b.data.String()), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}

		if record["msg"] == "http request" {
			records = append(records, record)
		}
	}

	return records
}
