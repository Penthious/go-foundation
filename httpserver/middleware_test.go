package httpserver

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestMiddleware_RequestLogs(t *testing.T) {
	var output logBuffer
	s := startServer(t, Config{Logger: testLogger(t, &output), Routes: []Route{{Pattern: "POST /orders/{id}", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			http.Error(w, "read failed", http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, "created")
	})}}})
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+s.Address()+"/orders/123?secret=private-query", strings.NewReader("private-body"))
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Authorization", "private-header")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatal(err)
	}

	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	request(t, s, http.MethodGet, "/missing", nil)
	records := output.records(t)
	if len(records) != 2 {
		t.Fatalf("request logs = %d", len(records))
	}

	fields := records[0]["fields"].(map[string]any)
	if fields["route"] != "POST /orders/{id}" || fields["method"] != "POST" || fields["status"] != float64(201) || fields["response_bytes"] != float64(7) || fields["path"] != "/orders/123" || fields["duration"] == nil {
		t.Errorf("unexpected request fields: %v", fields)
	}

	if records[1]["level"] != "WARN" {
		t.Error("4xx request was not logged at WARN")
	}

	output.mu.Lock()
	defer output.mu.Unlock()

	for _, secret := range []string{"private-query", "private-body", "private-header"} {
		if strings.Contains(output.data.String(), secret) {
			t.Errorf("request log exposed %s", secret)
		}
	}
}

func TestMiddleware_BodyLimits(t *testing.T) {
	s := startServer(t, Config{MaxBodyBytes: 4, Routes: []Route{{Pattern: "POST /upload", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
				return
			}

			http.Error(w, "read failed", http.StatusBadRequest)
			return
		}

		_, _ = w.Write(data)
	})}}})
	tests := []struct {
		name   string
		body   io.Reader
		status int
	}{
		{"exact limit", strings.NewReader("1234"), http.StatusOK},
		{"known length", strings.NewReader("12345"), http.StatusRequestEntityTooLarge},
		{"chunked", io.NopCloser(strings.NewReader("12345")), http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, _ := request(t, s, http.MethodPost, "/upload", tt.body)

			if resp.StatusCode != tt.status {
				t.Errorf("status = %d", resp.StatusCode)
			}
		})
	}
}

func TestMiddleware_PanicRecovery(t *testing.T) {
	for _, value := range []any{"private-panic", map[string]string{"error": "private-panic"}} {
		t.Run("panic", func(t *testing.T) {
			var output logBuffer
			s := startServer(t, Config{Logger: testLogger(t, &output), Routes: []Route{{Pattern: "GET /panic", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Encoding", "gzip")
				panic(value)
			})}}})

			resp, body := request(t, s, http.MethodGet, "/panic", nil)
			if resp.StatusCode != http.StatusInternalServerError || strings.Contains(body, "private-panic") {
				t.Errorf("panic response = %d %q", resp.StatusCode, body)
			}

			records := output.records(t)
			if len(records) != 1 || records[0]["level"] != "ERROR" || records[0]["fields"].(map[string]any)["stack"] == nil {
				t.Errorf("panic did not produce a correlated error log: %v", records)
			}
		})
	}
}

func TestMiddleware_StreamingAndPartialResponsePanic(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	s := startServer(t, Config{Routes: []Route{{Pattern: "GET /stream", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "first\n")
		if err := http.NewResponseController(w).Flush(); err != nil {
			panic(err)
		}

		close(entered)
		select {
		case <-release:
			panic("stream failed")
		case <-r.Context().Done():
		}
	})}}})
	resp, err := http.Get("http://" + s.Address() + "/stream")
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()
	first := make([]byte, 6)
	if _, err := io.ReadFull(resp.Body, first); err != nil {
		t.Fatal(err)
	}

	receive(t, entered)
	close(release)
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Fatal("panic after a flushed response did not abort the connection")
	}

	if string(first) != "first\n" {
		t.Errorf("stream changed: %q", first)
	}
}

func TestMiddleware_TracePropagationAndMetrics(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	reader := sdkmetric.NewManualReader()
	metrics := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	oldTracer, oldMeter, oldProp := otel.GetTracerProvider(), otel.GetMeterProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetMeterProvider(metrics)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() {
		otel.SetTracerProvider(oldTracer)
		otel.SetMeterProvider(oldMeter)
		otel.SetTextMapPropagator(oldProp)
		if err := errors.Join(provider.Shutdown(context.Background()), metrics.Shutdown(context.Background())); err != nil {
			t.Error(err)
		}
	})
	var output logBuffer
	seen := make(chan trace.SpanContext, 2)
	s := startServer(t, Config{Logger: testLogger(t, &output), Routes: []Route{
		{Pattern: "GET /orders/{id}", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seen <- trace.SpanContextFromContext(r.Context())
			if baggage.FromContext(r.Context()).Member("region").Value() != "west" {
				http.Error(w, "missing baggage", http.StatusBadRequest)
				return
			}

			w.WriteHeader(http.StatusNoContent)
		})},
		{Pattern: "GET /panic", Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("test") })},
		{Pattern: "GET /abort", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, "partial")
			w.(http.Flusher).Flush()
			panic("stream failed")
		})},
	}})
	ctx, root := provider.Tracer("service").Start(t.Context(), "upstream")
	member, err := baggage.NewMember("region", "west")
	if err != nil {
		t.Fatal(err)
	}

	bag, err := baggage.New(member)
	if err != nil {
		t.Fatal(err)
	}

	ctx = baggage.ContextWithBaggage(ctx, bag)
	for _, id := range []string{"123", "456"} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+s.Address()+"/orders/"+id, nil)
		if err != nil {
			t.Fatal(err)
		}

		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}

		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent || receive(t, seen).TraceID() != root.SpanContext().TraceID() {
			t.Error("server did not propagate trace and baggage")
		}
	}

	request(t, s, http.MethodGet, "/panic", nil)
	aborted, err := http.Get("http://" + s.Address() + "/abort")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := io.ReadAll(aborted.Body); err == nil {
		t.Error("partial response did not abort")
	}

	aborted.Body.Close()
	root.End()
	spans := exporter.GetSpans()
	serverSpans, failures := 0, 0
	for _, span := range spans {
		if span.SpanKind != trace.SpanKindServer {
			continue
		}

		serverSpans++
		if strings.Contains(span.Name, "orders") && (span.Name != "GET /orders/{id}" || span.Parent.SpanID() != root.SpanContext().SpanID()) {
			t.Errorf("incorrect server span: %+v", span)
		}

		if span.Status.Code == codes.Error {
			failures++
		}
	}

	if serverSpans != 4 || failures != 2 {
		t.Errorf("server spans=%d failures=%d", serverSpans, failures)
	}

	records := output.records(t)
	if records[0]["trace_id"] != root.SpanContext().TraceID().String() || records[0]["span_id"] == nil {
		t.Error("request log did not correlate with server span")
	}

	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}

	var orderCount, panicCount, abortedCount uint64
	for _, scope := range data.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != "http.server.request.duration" {
				continue
			}

			histogram, ok := metric.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("unexpected duration metric: %T", metric.Data)
			}

			for _, point := range histogram.DataPoints {
				route, _ := point.Attributes.Value("http.route")
				switch route.AsString() {
				case "/orders/{id}":
					orderCount += point.Count
				case "/panic":
					panicCount += point.Count
				case "/abort":
					abortedCount += point.Count
				default:
					t.Errorf("unexpected metric route: %v", route)
				}
			}
		}
	}

	if orderCount != 2 || panicCount != 1 || abortedCount != 1 {
		t.Errorf("route metrics orders=%d panic=%d aborted=%d", orderCount, panicCount, abortedCount)
	}
}
