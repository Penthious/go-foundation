package circuitbreaker

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/logger"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
)

func TestObserver_StateChangesAndRejections(t *testing.T) {
	fixture := newObservationTest(t)
	b, err := New(Config{FailureThreshold: 1, OpenTimeout: 30 * time.Second, Logger: fixture.log, Name: "orders", Protocol: "http"})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Unix(1000, 0)
	b.now = func() time.Time { return now }
	span := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled})
	ctx := trace.ContextWithSpanContext(t.Context(), span)
	first, _ := b.Allow(ctx)
	stale, _ := b.Allow(ctx)
	first.Done(Failure)
	first.Done(Failure)
	stale.Done(Success)
	for range 2 {
		if _, ok := b.Allow(ctx); ok {
			t.Fatal("expected open-circuit rejection")
		}
	}

	now = now.Add(30 * time.Second)
	probe, _ := b.Allow(ctx)
	if _, ok := b.Allow(ctx); ok {
		t.Fatal("expected recovery-probe rejection")
	}

	probe.Done(Ignored)
	probe, _ = b.Allow(ctx)
	probe.Done(Failure)
	if _, ok := b.Allow(ctx); ok {
		t.Fatal("expected failed-probe rejection")
	}

	now = now.Add(30 * time.Second)
	probe, _ = b.Allow(ctx)
	probe.Done(Success)
	admit(t, b).Done(Success)

	want := map[string]int64{
		"circuit_breaker.transitions/closed/open":      1,
		"circuit_breaker.transitions/open/half_open":   3,
		"circuit_breaker.transitions/half_open/open":   2,
		"circuit_breaker.transitions/half_open/closed": 1,
		"circuit_breaker.rejections/open":              3,
		"circuit_breaker.rejections/half_open":         1,
	}
	if got := collectObservations(t, fixture.reader, "orders", "http"); !reflect.DeepEqual(got, want) {
		t.Fatalf("circuit counters = %v, want %v", got, want)
	}

	records := fixture.exporter.snapshot()
	if len(records) != 11 {
		t.Fatalf("OTel log records = %d, want eleven actual transitions/rejections", len(records))
	}

	generations := make(map[int64]bool)
	for _, record := range records {
		if record.TraceID() != span.TraceID() || record.SpanID() != span.SpanID() {
			t.Fatal("circuit event lost request trace correlation")
		}

		fields := make(map[string]any)
		record.WalkAttributes(func(kv attribute.KeyValue) bool {
			if kv.Key == "fields" {
				for _, field := range kv.Value.AsMap() {
					fields[string(field.Key)] = field.Value.AsInterface()
				}
			}

			return true
		})
		if fields["client_name"] != "orders" || fields["client_type"] != "http" || fields["generation"] == nil {
			t.Fatal("OTel log lost client identity or generation:", fields)
		}

		if record.Body().AsString() == "circuit breaker state changed" {
			generation, ok := fields["generation"].(int64)
			if !ok || generation < 1 || generation > 7 || generations[generation] {
				t.Fatal("state transition generation was missing or duplicated:", fields)
			}

			generations[generation] = true
		}

		if record.Body().AsString() == "circuit breaker call rejected" || fields["to_state"] == "open" {
			if record.Severity() != otellog.SeverityWarn {
				t.Fatal("open/rejection event was not WARN")
			}
		} else if record.Severity() != otellog.SeverityInfo {
			t.Fatal("recovery event was not INFO")
		}
	}

	decoder := json.NewDecoder(bytes.NewReader(fixture.output.Bytes()))
	count := 0
	for decoder.More() {
		var record struct {
			Fields map[string]any `json:"fields"`
		}
		if err := decoder.Decode(&record); err != nil {
			t.Fatal(err)
		}

		if record.Fields["client_name"] != "orders" || record.Fields["client_type"] != "http" {
			t.Fatal("JSON event lost client identity")
		}

		count++
	}

	if count != len(records) {
		t.Fatal("JSON and OTel did not receive the same circuit events")
	}
}

func TestObserver_ConcurrentCountsAndDefaultName(t *testing.T) {
	fixture := newObservationTest(t)
	b, err := New(Config{FailureThreshold: 1, Logger: fixture.log, Protocol: "grpc"})
	if err != nil {
		t.Fatal(err)
	}

	permits := make([]*Permit, 100)
	for i := range permits {
		permits[i] = admit(t, b)
	}

	var callers sync.WaitGroup
	for _, permit := range permits {
		callers.Go(func() {
			permit.Done(Failure)
			permit.Done(Failure)
			_, _ = b.Allow(t.Context())
		})
	}

	callers.Wait()
	want := map[string]int64{"circuit_breaker.transitions/closed/open": 1, "circuit_breaker.rejections/open": 100}
	if got := collectObservations(t, fixture.reader, "default", "grpc"); !reflect.DeepEqual(got, want) {
		t.Fatalf("concurrent counters = %v, want %v", got, want)
	}

	if got := len(fixture.exporter.snapshot()); got != 101 {
		t.Fatalf("concurrent OTel events = %d, want 101", got)
	}
}

func TestObserver_EmitsOutsideBreakerLock(t *testing.T) {
	var b *Breaker
	var locked atomic.Bool
	log, err := logger.New(logger.Config{Service: "breaker-lock-test", Output: callbackWriter(func(data []byte) (int, error) {
		if !b.mu.TryLock() {
			locked.Store(true)
		} else {
			b.mu.Unlock()
		}

		return len(data), nil
	})})
	if err != nil {
		t.Fatal(err)
	}

	b, err = New(Config{FailureThreshold: 1, Logger: log, Protocol: "http"})
	if err != nil {
		t.Fatal(err)
	}

	admit(t, b).Done(Failure)
	_, _ = b.Allow(t.Context())
	if locked.Load() {
		t.Fatal("logger/user output was called while holding the circuit lock")
	}
}

type observationTest struct {
	reader   *sdkmetric.ManualReader
	exporter *logExporter
	output   *lockedBuffer
	log      *logger.Logger
}

func newObservationTest(t *testing.T) observationTest {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	metrics := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(metrics)
	exporter := &logExporter{}
	logs := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exporter)))
	t.Cleanup(func() {
		otel.SetMeterProvider(previous)
		_ = metrics.Shutdown(context.Background())
		_ = logs.Shutdown(context.Background())
	})
	output := &lockedBuffer{}
	log, err := logger.New(logger.Config{Service: "breaker-test", Output: output, LoggerProvider: logs})
	if err != nil {
		t.Fatal(err)
	}

	return observationTest{reader: reader, exporter: exporter, output: output, log: log}
}

func collectObservations(t *testing.T, reader *sdkmetric.ManualReader, name, protocol string) map[string]int64 {
	t.Helper()

	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}

	values := make(map[string]int64)
	for _, scope := range data.ScopeMetrics {
		if scope.Scope.Name != "go-foundation/circuitbreaker" {
			continue
		}

		for _, measurement := range scope.Metrics {
			sum, ok := measurement.Data.(metricdata.Sum[int64])
			if !ok || !sum.IsMonotonic {
				t.Fatal("circuit metric was not a monotonic integer counter")
			}

			for _, point := range sum.DataPoints {
				client, _ := point.Attributes.Value("client.name")
				kind, _ := point.Attributes.Value("client.type")
				if client.AsString() != name || kind.AsString() != protocol {
					t.Fatal("metric lost client identity:", point.Attributes)
				}

				key := measurement.Name
				attributeCount := 3
				if measurement.Name == "circuit_breaker.transitions" {
					attributeCount = 4
					from, _ := point.Attributes.Value("from_state")
					to, _ := point.Attributes.Value("to_state")
					key += "/" + from.AsString() + "/" + to.AsString()
				} else {
					state, _ := point.Attributes.Value("state")
					key += "/" + state.AsString()
				}

				if point.Attributes.Len() != attributeCount {
					t.Fatal("metric contains unexpected dimensions:", point.Attributes)
				}

				values[key] += point.Value
			}
		}
	}

	return values
}

type logExporter struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (e *logExporter) Export(_ context.Context, records []sdklog.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, record := range records {
		e.records = append(e.records, record.Clone())
	}

	return nil
}

func (e *logExporter) Shutdown(context.Context) error {
	return nil
}

func (e *logExporter) ForceFlush(context.Context) error {
	return nil
}

func (e *logExporter) snapshot() []sdklog.Record {
	e.mu.Lock()
	defer e.mu.Unlock()

	return append([]sdklog.Record{}, e.records...)
}

type lockedBuffer struct {
	mu   sync.Mutex
	data bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.data.Write(data)
}

func (b *lockedBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	return bytes.Clone(b.data.Bytes())
}

type callbackWriter func([]byte) (int, error)

func (f callbackWriter) Write(data []byte) (int, error) {
	return f(data)
}
