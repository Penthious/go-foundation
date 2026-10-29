package httpclient_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/httpclient"
	"github.com/Penthious/go-foundation/logger"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/log/noop"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestNew_Configuration(t *testing.T) {
	log, _ := newLogger(t, logger.LevelInfo)
	tests := []struct {
		name string
		cfg  httpclient.Config
	}{
		{name: "missing logger"},
		{name: "negative timeout", cfg: httpclient.Config{Logger: log, Timeout: -time.Second}},
		{name: "negative base timeout", cfg: httpclient.Config{Logger: log, Client: &http.Client{Timeout: -time.Second}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := httpclient.New(tt.cfg)

			if err == nil || client != nil {
				t.Fatalf("New = (%v, %v), want nil client and an error", client, err)
			}
		})
	}

	client := newClient(t, httpclient.Config{Logger: log})
	if client.Timeout != 30*time.Second {
		t.Errorf("timeout = %v, want 30s", client.Timeout)
	}
}

func TestClient_DebugDisabledDoesNotCopyBody(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}

		if string(data) != "payload" {
			t.Errorf("received %q, want payload", data)
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	log, sink := newLogger(t, logger.LevelInfo)
	client := newClient(t, httpclient.Config{Logger: log})
	req := newRequest(t, http.MethodPost, server.URL, strings.NewReader("payload"))
	req.GetBody = func() (io.ReadCloser, error) {
		calls.Add(1)
		return nil, errors.New("must not be called for logging")
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	closeResponse(t, resp)
	if calls.Load() != 0 || len(sink.records(t)) != 0 {
		t.Error("disabled DEBUG logging copied the body or produced a log")
	}
}

func TestClient_TracePropagationAndMetrics(t *testing.T) {
	for _, propagate := range []bool{false, true} {
		name := "external"
		if propagate {
			name = "internal"
		}

		t.Run(name, func(t *testing.T) {
			exporter := tracetest.NewInMemoryExporter()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
			reader := sdkmetric.NewManualReader()
			metrics := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			oldTrace, oldMeter, oldProp := otel.GetTracerProvider(), otel.GetMeterProvider(), otel.GetTextMapPropagator()
			otel.SetTracerProvider(provider)
			otel.SetMeterProvider(metrics)
			otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
			t.Cleanup(func() {
				otel.SetTracerProvider(oldTrace)
				otel.SetMeterProvider(oldMeter)
				otel.SetTextMapPropagator(oldProp)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				if err := errors.Join(provider.Shutdown(ctx), metrics.Shutdown(ctx)); err != nil {
					t.Error(err)
				}
			})
			incoming := make(chan http.Header, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				incoming <- r.Header.Clone()
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			log, sink := newLogger(t, logger.LevelDebug)
			client := newClient(t, httpclient.Config{Logger: log, PropagateTraceContext: propagate})
			ctx, parent := provider.Tracer("test").Start(t.Context(), "parent")
			defer parent.End()

			member, err := baggage.NewMember("region", "west")
			if err != nil {
				t.Fatal(err)
			}

			bag, err := baggage.New(member)
			if err != nil {
				t.Fatal(err)
			}

			req, err := http.NewRequestWithContext(baggage.ContextWithBaggage(ctx, bag), http.MethodGet, server.URL, nil)
			if err != nil {
				t.Fatal(err)
			}

			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}

			closeResponse(t, resp)
			headers := <-incoming
			if propagate {
				extracted := otel.GetTextMapPropagator().Extract(t.Context(), propagation.HeaderCarrier(headers))
				if trace.SpanContextFromContext(extracted).TraceID() != parent.SpanContext().TraceID() ||
					baggage.FromContext(extracted).Member("region").Value() != "west" {
					t.Error("internal request did not propagate trace and baggage")
				}
			} else if headers.Get("traceparent") != "" || headers.Get("baggage") != "" {
				t.Error("external request automatically leaked trace context or baggage")
			}

			spans := exporter.GetSpans()
			if len(spans) != 1 || spans[0].SpanKind != trace.SpanKindClient || spans[0].Parent.SpanID() != parent.SpanContext().SpanID() {
				t.Fatalf("unexpected HTTP spans: %v", spans)
			}

			record := sink.wait(t)
			if record.TraceID != parent.SpanContext().TraceID().String() || record.SpanID != spans[0].SpanContext.SpanID().String() {
				t.Error("curl debug log did not correlate with the HTTP client span")
			}

			var data metricdata.ResourceMetrics
			if err := reader.Collect(t.Context(), &data); err != nil {
				t.Fatal(err)
			}

			if len(data.ScopeMetrics) == 0 || len(data.ScopeMetrics[0].Metrics) == 0 {
				t.Error("HTTP metrics were not recorded")
			}
		})
	}
}

func TestClient_StatusCodesDoNotRetry(t *testing.T) {
	for _, code := range []int{http.StatusTooManyRequests, http.StatusInternalServerError} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(code)
				_, _ = io.WriteString(w, "upstream response")
			}))
			defer server.Close()
			log, _ := newLogger(t, logger.LevelInfo)
			client := newClient(t, httpclient.Config{Logger: log})

			resp, err := client.Do(newRequest(t, http.MethodPost, server.URL, nil))
			if err != nil {
				t.Fatal(err)
			}

			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}

			if resp.StatusCode != code || string(body) != "upstream response" || calls.Load() != 1 {
				t.Errorf("response = %d %q, calls = %d", resp.StatusCode, body, calls.Load())
			}
		})
	}
}

func TestClient_TimeoutAndCancellation(t *testing.T) {
	for _, cancelRequest := range []bool{false, true} {
		name := "timeout"
		if cancelRequest {
			name = "cancellation"
		}

		t.Run(name, func(t *testing.T) {
			started := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(started)
				<-r.Context().Done()
			}))
			defer server.Close()
			log, _ := newLogger(t, logger.LevelInfo)
			client := newClient(t, httpclient.Config{Logger: log, Timeout: 100 * time.Millisecond})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if cancelRequest {
				go func() {
					select {
					case <-started:
						cancel()
					case <-ctx.Done():
					}
				}()
			}

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
			if err != nil {
				t.Fatal(err)
			}

			resp, err := client.Do(req)
			if resp != nil {
				closeResponse(t, resp)
			}

			want := context.DeadlineExceeded
			if cancelRequest {
				want = context.Canceled
			}

			if !errors.Is(err, want) {
				t.Errorf("Do error = %v, want %v", err, want)
			}
		})
	}
}

func TestClient_PrivateCAAndTransportClone(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	log, _ := newLogger(t, logger.LevelInfo)
	untrusted := newClient(t, httpclient.Config{Logger: log})

	if resp, err := untrusted.Do(newRequest(t, http.MethodGet, server.URL, nil)); err == nil {
		closeResponse(t, resp)
		t.Fatal("untrusted self-signed certificate was accepted")
	}

	base := server.Client().Transport.(*http.Transport)
	trusted := newClient(t, httpclient.Config{Logger: log, Transport: base})
	base.TLSClientConfig = &tls.Config{ServerName: "wrong.example"}

	resp, err := trusted.Do(newRequest(t, http.MethodGet, server.URL, nil))
	if err != nil {
		t.Fatalf("private CA or transport clone did not work: %v", err)
	}

	closeResponse(t, resp)
}

func TestClient_InjectedBaseClient(t *testing.T) {
	var redirects atomic.Int32
	cookies := make(chan string, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.SetCookie(w, &http.Cookie{Name: "session", Value: "test", Path: "/"})
			http.Redirect(w, r, "/target", http.StatusTemporaryRedirect)
			return
		}

		cookies <- r.Header.Get("Cookie")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	base := server.Client()
	base.Timeout = 2 * time.Second
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}

	base.Jar = jar
	base.CheckRedirect = func(*http.Request, []*http.Request) error {
		redirects.Add(1)
		return http.ErrUseLastResponse
	}
	baseTransport := base.Transport
	log, _ := newLogger(t, logger.LevelInfo)
	client := newClient(t, httpclient.Config{Logger: log, Client: base})

	resp, err := client.Do(newRequest(t, http.MethodGet, server.URL+"/redirect", nil))
	if err != nil {
		t.Fatal(err)
	}

	closeResponse(t, resp)
	if resp.StatusCode != http.StatusTemporaryRedirect || redirects.Load() != 1 {
		t.Error("injected redirect policy was not preserved")
	}

	resp, err = client.Do(newRequest(t, http.MethodGet, server.URL+"/target", nil))
	if err != nil {
		t.Fatal(err)
	}

	closeResponse(t, resp)
	cookie := <-cookies
	if cookie != "session=test" || client.Jar != jar || client.Timeout != base.Timeout {
		t.Error("injected cookie jar or timeout was not preserved")
	}

	if client == base || base.Transport != baseTransport || base.Timeout != 2*time.Second {
		t.Error("SDK mutated the supplied client")
	}

	override := newClient(t, httpclient.Config{Logger: log, Client: base, Timeout: time.Second})
	if override.Timeout != time.Second || base.Timeout != 2*time.Second {
		t.Error("explicit timeout override did not preserve the base client")
	}
}

func TestClient_InjectedCustomTransport(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	base := &countedTransport{RoundTripper: http.DefaultTransport.(*http.Transport).Clone()}
	log, _ := newLogger(t, logger.LevelInfo)
	client := newClient(t, httpclient.Config{Logger: log, Client: &http.Client{Transport: base}})

	resp, err := client.Do(newRequest(t, http.MethodGet, server.URL, nil))
	if err != nil {
		t.Fatal(err)
	}

	closeResponse(t, resp)
	client.CloseIdleConnections()
	if base.calls.Load() != 1 || base.closes.Load() == 0 {
		t.Error("custom transport did not receive the request or idle cleanup")
	}
}

type countedTransport struct {
	http.RoundTripper
	calls  atomic.Int32
	closes atomic.Int32
}

func (t *countedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.calls.Add(1)
	return t.RoundTripper.RoundTrip(req)
}

func (t *countedTransport) CloseIdleConnections() {
	t.closes.Add(1)
	t.RoundTripper.(interface{ CloseIdleConnections() }).CloseIdleConnections()
}

func TestClient_ReusesAndClosesConnections(t *testing.T) {
	closed := make(chan struct{}, 2)
	var mu sync.Mutex
	var addresses []string
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		addresses = append(addresses, r.RemoteAddr)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			closed <- struct{}{}
		}
	}
	server.Start()
	defer server.Close()
	log, _ := newLogger(t, logger.LevelInfo)
	client := newClient(t, httpclient.Config{Logger: log})

	for range 2 {
		resp, err := client.Do(newRequest(t, http.MethodGet, server.URL, nil))
		if err != nil {
			t.Fatal(err)
		}

		closeResponse(t, resp)
	}

	client.CloseIdleConnections()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("CloseIdleConnections did not close the pooled connection")
	}

	mu.Lock()
	defer mu.Unlock()

	if len(addresses) != 2 || addresses[0] != addresses[1] {
		t.Errorf("connection was not reused: %v", addresses)
	}
}

func newClient(t *testing.T, cfg httpclient.Config) *http.Client {
	t.Helper()

	client, err := httpclient.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(client.CloseIdleConnections)

	return client
}

func newRequest(t *testing.T, method, url string, body io.Reader) *http.Request {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, url, body)
	if err != nil {
		t.Fatal(err)
	}

	return req
}

func closeResponse(t *testing.T, resp *http.Response) {
	t.Helper()

	_, readErr := io.Copy(io.Discard, resp.Body)
	closeErr := resp.Body.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		t.Fatal(err)
	}
}

type logRecord struct {
	TraceID string         `json:"trace_id"`
	SpanID  string         `json:"span_id"`
	Fields  map[string]any `json:"fields"`
}

type logSink struct {
	mu   sync.Mutex
	data bytes.Buffer
	done chan struct{}
}

func (s *logSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	n, err := s.data.Write(p)
	s.mu.Unlock()
	s.done <- struct{}{}

	return n, err
}

func (s *logSink) records(t *testing.T) []logRecord {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()

	var records []logRecord
	decoder := json.NewDecoder(bytes.NewReader(s.data.Bytes()))
	for decoder.More() {
		var record logRecord
		if err := decoder.Decode(&record); err != nil {
			t.Fatal(err)
		}

		records = append(records, record)
	}

	return records
}

func (s *logSink) wait(t *testing.T) logRecord {
	t.Helper()

	select {
	case <-s.done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for curl debug log")
	}

	records := s.records(t)
	return records[len(records)-1]
}

func newLogger(t *testing.T, level logger.Level) (*logger.Logger, *logSink) {
	t.Helper()

	sink := &logSink{done: make(chan struct{}, 32)}
	log, err := logger.New(logger.Config{
		Service: "httpclient-test", Level: level, Output: sink, LoggerProvider: noop.NewLoggerProvider(),
	})
	if err != nil {
		t.Fatal(err)
	}

	return log, sink
}
