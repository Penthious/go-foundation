package cache

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestClient_TelemetryWithoutCacheContents(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = tp.Shutdown(t.Context())
		_ = mp.Shutdown(t.Context())
	})
	client := testClient(t, Config{Namespace: "sensitive-namespace", MaxValueBytes: 4, TracerProvider: tp, MeterProvider: mp})
	ctx, parent := tp.Tracer("service").Start(t.Context(), "request")
	if err := client.Set(ctx, "sensitive-key", []byte("data"), 0); err != nil {
		t.Fatal(err)
	}

	if _, err := client.Get(ctx, "sensitive-key"); err != nil {
		t.Fatal(err)
	}

	if _, err := client.Get(ctx, "missing"); !errors.Is(err, ErrMiss) {
		t.Fatal(err)
	}

	if err := client.Set(ctx, "sensitive-key", []byte("sensitive-value"), 0); !errors.Is(err, ErrValueTooLarge) {
		t.Fatal(err)
	}

	parent.End()
	spans := recorder.Ended()
	if len(spans) != 5 {
		t.Fatalf("span count = %d, want 5", len(spans))
	}

	for i, span := range spans[:4] {
		if span.Parent().SpanID() != parent.SpanContext().SpanID() {
			t.Fatal("cache span not parented to request")
		}

		if span.Status().Code == codes.Error && i != 3 {
			t.Fatal("successful operation or cache miss marked as an error")
		}

		encoded, err := json.Marshal(span.Attributes())
		if err != nil {
			t.Fatal(err)
		}

		if strings.Contains(string(encoded), "sensitive") || len(span.Events()) != 0 {
			t.Fatal("cache contents leaked into telemetry")
		}
	}

	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}

	results := make(map[string]int64)
	histograms := uint64(0)
	for _, scope := range data.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			switch value := measurement.Data.(type) {
			case metricdata.Sum[int64]:
				for _, point := range value.DataPoints {
					for _, attribute := range point.Attributes.ToSlice() {
						if string(attribute.Key) == "cache.result" {
							results[attribute.Value.AsString()] += point.Value
						}
					}
				}
			case metricdata.Histogram[float64]:
				for _, point := range value.DataPoints {
					histograms += point.Count
				}
			}
		}
	}

	if results["hit"] != 1 || results["miss"] != 1 || results["success"] != 1 || results["error"] != 1 || histograms != 4 {
		t.Fatalf("cache measurements = %v, histogram count = %d", results, histograms)
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal("cache closed a borrowed provider")
	}
}
