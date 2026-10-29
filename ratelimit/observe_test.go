package ratelimit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/logger"

	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestLimiter_TelemetryPrivacyAndBorrowedProviders(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	metrics := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	recorder := tracetest.NewSpanRecorder()
	traces := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() {
		_ = metrics.Shutdown(context.Background())
		_ = traces.Shutdown(context.Background())
	})
	var output bytes.Buffer
	log, err := logger.New(logger.Config{Service: "rate-telemetry", Output: &output})
	if err != nil {
		t.Fatal(err)
	}

	limiter := testLimiter(t, Config{Namespace: "orders", Logger: log, Limit: 1, Window: time.Hour, Burst: 1, MeterProvider: metrics, TracerProvider: traces})
	ctx, parent := traces.Tracer("service").Start(t.Context(), "request")
	decision, err := limiter.Allow(ctx, "sensitive-user-secret")
	if err != nil || !decision.Allowed {
		t.Fatal(err)
	}

	decision, err = limiter.Allow(ctx, "sensitive-user-secret")
	if err != nil || decision.Allowed {
		t.Fatal("expected quota denial:", decision, err)
	}

	if err := limiter.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := limiter.Allow(ctx, "sensitive-user-secret"); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}

	parent.End()
	spans := recorder.Ended()
	if len(spans) != 4 {
		t.Fatalf("span count = %d, want four", len(spans))
	}

	for i, span := range spans[:3] {
		if span.Parent().SpanID() != parent.SpanContext().SpanID() || (span.Status().Code == codes.Error) != (i == 2) {
			t.Fatal("incorrect span parent or denial/error classification")
		}

		data, err := json.Marshal(span.Attributes())
		if err != nil {
			t.Fatal(err)
		}

		if strings.Contains(string(data), "sensitive") || len(span.Events()) != 0 {
			t.Fatal("rate identity leaked into spans")
		}
	}

	if strings.Contains(output.String(), "sensitive") || !strings.Contains(output.String(), "rate limit exceeded") || !strings.Contains(output.String(), "trace_id") {
		t.Fatal("rate logs leaked identities or lost trace context")
	}

	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal("limiter closed borrowed meter provider:", err)
	}

	results := make(map[string]int64)
	var durations uint64
	for _, scope := range data.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			switch values := measurement.Data.(type) {
			case metricdata.Sum[int64]:
				for _, point := range values.DataPoints {
					if point.Attributes.Len() != 3 {
						t.Fatal("metrics contain identity dimensions")
					}

					name, _ := point.Attributes.Value("ratelimit.name")
					backend, _ := point.Attributes.Value("ratelimit.backend")
					result, _ := point.Attributes.Value("ratelimit.result")
					if name.AsString() != "orders" || backend.AsString() != "memory" {
						t.Fatal("metrics lost limiter identity")
					}

					results[result.AsString()] += point.Value
				}
			case metricdata.Histogram[float64]:
				for _, point := range values.DataPoints {
					durations += point.Count
				}
			}
		}
	}

	if results["allowed"] != 1 || results["denied"] != 1 || results["error"] != 1 || durations != 3 {
		t.Fatal("missing rate limit metrics:", results, durations)
	}
}
