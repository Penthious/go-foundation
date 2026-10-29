package httpclient_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/httpclient"
	"github.com/Penthious/go-foundation/logger"

	"github.com/hashicorp/go-retryablehttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestRetry_DefaultsAndFinalResponse(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, "final response is still readable")
	}))
	defer server.Close()
	log, _ := newLogger(t, logger.LevelInfo)
	client := newClient(t, httpclient.Config{Logger: log})

	resp, err := client.Do(newRequest(t, http.MethodGet, server.URL, nil))
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	wantCalls := int32(retryablehttp.NewClient().RetryMax + 1)
	if calls.Load() != wantCalls || resp.StatusCode != http.StatusServiceUnavailable || string(body) != "final response is still readable" {
		t.Errorf("calls=%d want=%d, final response=%d %q", calls.Load(), wantCalls, resp.StatusCode, body)
	}
}

func TestRetry_SafeMethods(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace} {
		t.Run(method, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}

				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			log, _ := newLogger(t, logger.LevelInfo)
			client := newClient(t, httpclient.Config{Logger: log, Retry: fastRetries(2)})

			resp, err := client.Do(newRequest(t, method, server.URL, nil))
			if err != nil {
				t.Fatal(err)
			}

			closeResponse(t, resp)
			if calls.Load() != 2 || resp.StatusCode != http.StatusNoContent {
				t.Errorf("calls=%d status=%d, want two attempts and success", calls.Load(), resp.StatusCode)
			}
		})
	}
}

func TestRetry_WritesAreNotRepeated(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				calls.Add(1)
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			defer server.Close()
			log, _ := newLogger(t, logger.LevelInfo)
			client := newClient(t, httpclient.Config{Logger: log, Retry: fastRetries(2)})
			req := newRequest(t, method, server.URL, strings.NewReader("write"))
			req.Header.Set("Idempotency-Key", "a-header-alone-is-not-a-server-guarantee")

			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}

			closeResponse(t, resp)
			if calls.Load() != 1 {
				t.Errorf("unsafe method %s was sent %d times", method, calls.Load())
			}
		})
	}
}

func TestRetry_StatusPolicy(t *testing.T) {
	for _, code := range []int{429, 500, 502, 503, 504, 400, 401, 403, 404, 501} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.WriteHeader(code)
					return
				}

				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			log, _ := newLogger(t, logger.LevelInfo)
			client := newClient(t, httpclient.Config{Logger: log, Retry: fastRetries(1)})

			resp, err := client.Do(newRequest(t, http.MethodGet, server.URL, nil))
			if err != nil {
				t.Fatal(err)
			}

			closeResponse(t, resp)
			wantCalls, wantStatus := int32(1), code
			if code == 429 || code >= 500 && code != 501 {
				wantCalls, wantStatus = 2, http.StatusNoContent
			}

			if calls.Load() != wantCalls || resp.StatusCode != wantStatus {
				t.Errorf("calls=%d status=%d, want calls=%d status=%d", calls.Load(), resp.StatusCode, wantCalls, wantStatus)
			}
		})
	}
}

func TestRetry_PerClientDisabled(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	log, _ := newLogger(t, logger.LevelInfo)
	client := newClient(t, httpclient.Config{Logger: log, Retry: &httpclient.RetryConfig{MaxRetries: 0}})

	resp, err := client.Do(newRequest(t, http.MethodGet, server.URL, nil))
	if err != nil {
		t.Fatal(err)
	}

	closeResponse(t, resp)
	if calls.Load() != 1 {
		t.Errorf("disabled retries sent %d requests", calls.Load())
	}
}

func TestRetry_ReplaysBodyWithoutPrebuffering(t *testing.T) {
	var calls, replays atomic.Int32
	var originalCloses atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}

		if string(body) != "payload" {
			t.Errorf("attempt body=%q, want payload", body)
		}

		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	log, _ := newLogger(t, logger.LevelInfo)
	client := newClient(t, httpclient.Config{Logger: log, Retry: fastRetries(1)})
	original := &trackedBody{Reader: strings.NewReader("payload"), closes: &originalCloses}
	req := newRequest(t, http.MethodGet, server.URL, original)
	req.ContentLength = int64(len("payload"))
	req.GetBody = func() (io.ReadCloser, error) {
		replays.Add(1)
		return io.NopCloser(strings.NewReader("payload")), nil
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	closeResponse(t, resp)
	if calls.Load() != 2 || replays.Load() != 1 || originalCloses.Load() == 0 {
		t.Errorf("calls=%d replays=%d original closes=%d, want 2, 1, and a closed original body", calls.Load(), replays.Load(), originalCloses.Load())
	}
}

func TestRetry_NonReplayableBodyIsSentOnce(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	log, _ := newLogger(t, logger.LevelInfo)
	client := newClient(t, httpclient.Config{Logger: log, Retry: fastRetries(2)})
	req := newRequest(t, http.MethodGet, server.URL, io.NopCloser(strings.NewReader("stream")))

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	closeResponse(t, resp)
	if calls.Load() != 1 {
		t.Errorf("non-replayable request was sent %d times", calls.Load())
	}
}

func TestRetry_RewindFailure(t *testing.T) {
	for _, nilBody := range []bool{false, true} {
		name := "factory error"
		if nilBody {
			name = "nil body"
		}

		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				calls.Add(1)
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			defer server.Close()
			log, _ := newLogger(t, logger.LevelInfo)
			client := newClient(t, httpclient.Config{Logger: log, Retry: fastRetries(2)})
			req := newRequest(t, http.MethodGet, server.URL, strings.NewReader("body"))
			copyErr := errors.New("cannot reopen body")
			req.GetBody = func() (io.ReadCloser, error) {
				if nilBody {
					return nil, nil
				}

				return nil, copyErr
			}

			resp, err := client.Do(req)

			if resp != nil || err == nil || calls.Load() != 1 {
				t.Errorf("response=%v error=%v attempts=%d, want an error after first attempt", resp, err, calls.Load())
			}

			if !nilBody && !errors.Is(err, copyErr) {
				t.Errorf("error lost its cause: %v", err)
			}
		})
	}
}

func TestRetry_ConnectionFailure(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}

			if err := conn.Close(); err != nil {
				t.Error(err)
			}

			return
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	log, _ := newLogger(t, logger.LevelInfo)
	client := newClient(t, httpclient.Config{Logger: log, Retry: fastRetries(1)})

	resp, err := client.Do(newRequest(t, http.MethodGet, server.URL, nil))
	if err != nil {
		t.Fatal(err)
	}

	closeResponse(t, resp)
	if calls.Load() != 2 || resp.StatusCode != http.StatusNoContent {
		t.Errorf("connection failure did not recover: calls=%d status=%d", calls.Load(), resp.StatusCode)
	}
}

func TestRetry_TotalTimeoutIncludesBackoff(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	log, _ := newLogger(t, logger.LevelInfo)
	client := newClient(t, httpclient.Config{Logger: log, Timeout: 100 * time.Millisecond})

	resp, err := client.Do(newRequest(t, http.MethodGet, server.URL, nil))
	if resp != nil {
		closeResponse(t, resp)
	}

	if !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 1 {
		t.Errorf("error=%v attempts=%d, want deadline while waiting after first attempt", err, calls.Load())
	}
}

func TestRetry_CurlAndTraceForEachAttempt(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	oldTrace, oldProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(oldTrace)
		otel.SetTextMapPropagator(oldProp)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := provider.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	log, sink := newLogger(t, logger.LevelDebug)
	client := newClient(t, httpclient.Config{Logger: log, Retry: fastRetries(2), PropagateTraceContext: true})
	ctx, parent := provider.Tracer("test").Start(t.Context(), "operation")
	defer parent.End()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	closeResponse(t, resp)
	spans := exporter.GetSpans()
	records := sink.records(t)
	if len(spans) != 3 || len(records) != 3 {
		t.Fatalf("spans=%d curl logs=%d, want 3 of each", len(spans), len(records))
	}

	ids := make(map[string]bool)
	for _, span := range spans {
		if span.Parent.SpanID() != parent.SpanContext().SpanID() || ids[span.SpanContext.SpanID().String()] {
			t.Error("attempts must have distinct spans under the original parent")
		}

		ids[span.SpanContext.SpanID().String()] = true
	}

	for _, record := range records {
		command, ok := record.Fields["curl"].(string)
		if !ok || !ids[record.SpanID] || !strings.Contains(command, record.SpanID) {
			t.Errorf("curl log missing its attempt span or injected trace header: %+v", record)
		}
	}
}

func TestRetry_InvalidConfiguration(t *testing.T) {
	log, _ := newLogger(t, logger.LevelInfo)
	for _, cfg := range []httpclient.RetryConfig{
		{MaxRetries: -1},
		{WaitMin: -time.Second},
		{WaitMax: -time.Second},
		{WaitMin: 2 * time.Second, WaitMax: time.Second},
	} {
		if client, err := httpclient.New(httpclient.Config{Logger: log, Retry: &cfg}); err == nil || client != nil {
			t.Errorf("invalid retry config %+v returned client=%v err=%v", cfg, client, err)
		}
	}
}

func fastRetries(max int) *httpclient.RetryConfig {
	return &httpclient.RetryConfig{MaxRetries: max, WaitMin: time.Nanosecond, WaitMax: time.Nanosecond}
}

type trackedBody struct {
	*strings.Reader
	closes *atomic.Int32
}

func (b *trackedBody) Close() error {
	b.closes.Add(1)
	return nil
}
