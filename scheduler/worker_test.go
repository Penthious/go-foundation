package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestWorker_RecoveryTimeoutAndTelemetry(t *testing.T) {
	oldTraces, oldMetrics := otel.GetTracerProvider(), otel.GetMeterProvider()
	recorder := tracetest.NewSpanRecorder()
	traces := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	reader := sdkmetric.NewManualReader()
	meters := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetTracerProvider(traces)
	otel.SetMeterProvider(meters)
	t.Cleanup(func() {
		otel.SetTracerProvider(oldTraces)
		otel.SetMeterProvider(oldMetrics)
		_ = traces.Shutdown(t.Context())
		_ = meters.Shutdown(t.Context())
	})

	var calls atomic.Int64
	completed := make(chan struct{})
	worker, err := New(testConfig(t), Job{Name: "resilient", Every: time.Millisecond, Timeout: 10 * time.Millisecond, Handler: func(ctx context.Context, run Run) error {
		if !trace.SpanContextFromContext(ctx).IsValid() {
			t.Error("handler has no trace context")
		}

		switch calls.Add(1) {
		case 1:
			panic("sensitive panic value")
		case 2:
			panic(nil)
		case 3:
			return errors.New("handler failure")
		case 4:
			<-ctx.Done()
			return ctx.Err()
		case 6:
			close(completed)
			<-ctx.Done()
			return ctx.Err()
		}

		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}

	if err := worker.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	_ = receive(t, completed)
	if err := worker.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}

	results := make(map[string]bool)
	for _, span := range recorder.Ended() {
		for _, attr := range span.Attributes() {
			if attr.Key == "result" {
				results[attr.Value.AsString()] = true
			}
		}

		data, err := json.Marshal(span.Attributes())
		if err != nil {
			t.Fatal(err)
		}

		if strings.Contains(string(data), "sensitive") || len(span.Events()) != 0 {
			t.Fatal("handler/panic details leaked into spans")
		}
	}

	if !results["success"] || !results["failure"] || !results["timeout"] {
		t.Fatalf("missing job outcomes: %v", results)
	}

	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &metrics); err != nil {
		t.Fatal(err)
	}

	var attempts int64
	var durations uint64
	for _, scope := range metrics.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			switch value := measurement.Data.(type) {
			case metricdata.Sum[int64]:
				for _, point := range value.DataPoints {
					attempts += point.Value
				}
			case metricdata.Histogram[float64]:
				for _, point := range value.DataPoints {
					durations += point.Count
				}
			}
		}
	}

	if attempts < 5 || durations != uint64(attempts) {
		t.Fatal("job metrics missing")
	}
}

func TestWorker_CanceledOccurrenceDoesNotInvokeHandler(t *testing.T) {
	called := false
	worker, err := New(testConfig(t), Job{Name: "canceled", Every: time.Hour, Handler: func(context.Context, Run) error {
		called = true

		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err = worker.execute(ctx, worker.jobs[0], Run{})

	if !errors.Is(err, context.Canceled) || called {
		t.Fatalf("canceled occurrence invoked handler: %v, %v", called, err)
	}
}

func TestWorker_CapacityAndNoOverlap(t *testing.T) {
	old := otel.GetMeterProvider()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(old)
		_ = provider.Shutdown(t.Context())
	})
	entered := make(chan string, 2)
	handler := func(ctx context.Context, run Run) error {
		entered <- run.Name
		<-ctx.Done()

		return ctx.Err()
	}
	cfg := testConfig(t)
	cfg.MaxConcurrentJobs = 1
	worker, err := New(cfg, Job{Name: "first", Every: time.Millisecond, Handler: handler}, Job{Name: "second", Every: time.Millisecond, Handler: handler})
	if err != nil {
		t.Fatal(err)
	}

	worker.slots <- struct{}{}
	if err := worker.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	skipped := waitForSkips(t, reader, 2)
	select {
	case <-entered:
		t.Fatal("handler started while capacity was exhausted")
	default:
	}

	<-worker.slots
	_ = receive(t, entered)
	_ = waitForSkips(t, reader, skipped+3)
	select {
	case <-entered:
		t.Fatal("overlapping job exceeded global concurrency")
	default:
	}

	if err := worker.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func waitForSkips(t *testing.T, reader *sdkmetric.ManualReader, minimum int64) int64 {
	t.Helper()

	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		var data metricdata.ResourceMetrics
		if err := reader.Collect(t.Context(), &data); err != nil {
			t.Fatal(err)
		}

		var skipped int64
		for _, scope := range data.ScopeMetrics {
			for _, measurement := range scope.Metrics {
				if total, ok := measurement.Data.(metricdata.Sum[int64]); ok {
					for _, point := range total.DataPoints {
						for _, attr := range point.Attributes.ToSlice() {
							if attr.Key == "result" && attr.Value.AsString() == "skipped_capacity" {
								skipped += point.Value
							}
						}
					}
				}
			}
		}

		if skipped >= minimum {
			return skipped
		}

		select {
		case <-deadline.C:
			t.Fatal("scheduler did not report capacity skips")
		case <-ticker.C:
		}
	}
}
