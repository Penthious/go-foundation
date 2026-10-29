package logger_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/logger"

	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/trace"
)

func TestNew_Validation(t *testing.T) {
	tests := []struct {
		name string
		cfg  logger.Config
	}{
		{name: "missing service"},
		{name: "blank service", cfg: logger.Config{Service: " \t\n"}},
		{name: "invalid level", cfg: logger.Config{Service: "orders", Level: "verbose"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log, err := logger.New(tt.cfg)

			if err == nil || log != nil {
				t.Fatalf("New(%+v) = (%v, %v), want nil logger and an error", tt.cfg, log, err)
			}
		})
	}
}

func TestLogger_Enabled(t *testing.T) {
	log := newLogger(t, logger.Config{Service: "orders", Level: logger.LevelWarn, Output: io.Discard})
	tests := []struct {
		level logger.Level
		want  bool
	}{
		{level: "", want: false},
		{level: logger.LevelDebug, want: false},
		{level: logger.LevelInfo, want: false},
		{level: logger.LevelWarn, want: true},
		{level: logger.LevelError, want: true},
		{level: "invalid", want: false},
	}

	for _, tt := range tests {
		t.Run(string(tt.level), func(t *testing.T) {
			if got := log.Enabled(t.Context(), tt.level); got != tt.want {
				t.Errorf("Enabled(%q) = %v, want %v", tt.level, got, tt.want)
			}
		})
	}

	if !log.Enabled(nil, logger.LevelError) {
		t.Error("nil context should be accepted")
	}
}

func TestNew_DefaultOutput(t *testing.T) {
	output, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatal(err)
	}

	original := os.Stdout
	os.Stdout = output
	t.Cleanup(func() {
		os.Stdout = original
		if err := output.Close(); err != nil {
			t.Errorf("close output: %v", err)
		}
	})
	log := newLogger(t, logger.Config{Service: "orders"})

	log.Info(t.Context(), "startup")

	data, err := os.ReadFile(output.Name())
	if err != nil {
		t.Fatal(err)
	}

	if records := readRecords(t, bytes.NewBuffer(data)); len(records) != 1 || records[0].Message != "startup" {
		t.Errorf("default output did not receive event: %+v", records)
	}
}

func TestLogger_JSON(t *testing.T) {
	var output bytes.Buffer
	log := newLogger(t, logger.Config{
		Service: "orders", Environment: "production", Version: "1.0.0", Output: &output,
	})
	before := time.Now()

	log.Info(t.Context(), "order received\nwith a newline",
		logger.String("order_id", "ord_123"),
		logger.Bool("retry", true),
		logger.String("service", "cannot replace metadata"),
		logger.Int64("duration_ms", 42),
	)

	records := readRecords(t, &output)
	if len(records) != 1 {
		t.Fatalf("got %d JSON lines, want 1", len(records))
	}

	r := records[0]
	if r.Level != "INFO" || r.Message != "order received\nwith a newline" ||
		r.Service != "orders" || r.Environment != "production" || r.Version != "1.0.0" {
		t.Fatalf("unexpected record metadata: %+v", r)
	}

	timestamp, err := time.Parse(time.RFC3339Nano, r.Time)
	if err != nil {
		t.Fatalf("invalid timestamp %q: %v", r.Time, err)
	}

	if !strings.HasSuffix(r.Time, "Z") || timestamp.Before(before) || timestamp.After(time.Now()) {
		t.Errorf("timestamp %q must be current and in UTC", r.Time)
	}

	wantFields := map[string]any{
		"order_id": "ord_123", "retry": true,
		"service": "cannot replace metadata", "duration_ms": json.Number("42"),
	}
	if !reflect.DeepEqual(r.Fields, wantFields) {
		t.Errorf("fields = %#v, want %#v", r.Fields, wantFields)
	}

	if r.TraceID != "" || r.SpanID != "" {
		t.Errorf("unexpected trace identifiers without a span: %+v", r)
	}
}

func TestLogger_With(t *testing.T) {
	var output bytes.Buffer
	log := newLogger(t, logger.Config{Service: "orders", Output: &output})
	parent := log.With(logger.String("component", "orders"), logger.String("order_id", "parent"))
	child := parent.With(logger.String("order_id", "child"), logger.Bool("retry", true))
	sibling := parent.With(logger.String("order_id", "sibling"))

	child.Info(t.Context(), "event override", logger.String("order_id", "event"))
	child.Info(t.Context(), "child")
	sibling.Info(t.Context(), "sibling")
	parent.Info(t.Context(), "parent")
	log.Info(t.Context(), "root")

	records := readRecords(t, &output)
	want := []map[string]any{
		{"component": "orders", "order_id": "event", "retry": true},
		{"component": "orders", "order_id": "child", "retry": true},
		{"component": "orders", "order_id": "sibling"},
		{"component": "orders", "order_id": "parent"},
		nil,
	}
	if len(records) != len(want) {
		t.Fatalf("got %d records, want %d", len(records), len(want))
	}

	for i, record := range records {
		if !reflect.DeepEqual(record.Fields, want[i]) {
			t.Errorf("record %d fields = %#v, want %#v", i, record.Fields, want[i])
		}
	}
}

func TestLogger_LevelFiltersBothOutputs(t *testing.T) {
	tests := []struct {
		level logger.Level
		want  []string
	}{
		{level: "", want: []string{"INFO", "WARN", "ERROR"}},
		{level: logger.LevelDebug, want: []string{"DEBUG", "INFO", "WARN", "ERROR"}},
		{level: logger.LevelInfo, want: []string{"INFO", "WARN", "ERROR"}},
		{level: logger.LevelWarn, want: []string{"WARN", "ERROR"}},
		{level: logger.LevelError, want: []string{"ERROR"}},
	}

	for _, tt := range tests {
		t.Run(string(tt.level), func(t *testing.T) {
			var output bytes.Buffer
			provider, exporter := newProvider(t)
			log := newLogger(t, logger.Config{
				Service: "orders", Level: tt.level, Output: &output, LoggerProvider: provider,
			})

			log.Debug(t.Context(), "debug")
			log.Info(t.Context(), "info")
			log.Warn(t.Context(), "warn")
			log.Error(t.Context(), "error")

			var jsonLevels, otelLevels []string
			for _, r := range readRecords(t, &output) {
				jsonLevels = append(jsonLevels, r.Level)
			}

			for _, r := range exporter.snapshot() {
				otelLevels = append(otelLevels, r.SeverityText())
			}

			if !reflect.DeepEqual(jsonLevels, tt.want) || !reflect.DeepEqual(otelLevels, tt.want) {
				t.Errorf("JSON levels = %v, OTel levels = %v; want both %v", jsonLevels, otelLevels, tt.want)
			}
		})
	}
}

func TestLogger_OpenTelemetry(t *testing.T) {
	var output bytes.Buffer
	provider, exporter := newProvider(t)
	log := newLogger(t, logger.Config{
		Service: "orders", Environment: "production", Version: "1.0.0",
		Output: &output, LoggerProvider: provider,
	})
	span := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(t.Context(), span)
	child := log.With(logger.String("order_id", "ord_123"))

	child.Error(ctx, "order creation failed",
		logger.Err(errors.New("database unavailable")),
		logger.Bool("retry", true),
		logger.Int("attempt", 2),
		logger.Int64("duration_ms", 42),
		logger.Duration("duration", 42*time.Millisecond),
	)

	jsonRecords := readRecords(t, &output)
	if len(jsonRecords) != 1 {
		t.Fatalf("got %d JSON records, want 1", len(jsonRecords))
	}

	j := jsonRecords[0]
	if j.TraceID != span.TraceID().String() || j.SpanID != span.SpanID().String() {
		t.Errorf("JSON trace identifiers = (%q, %q), want (%s, %s)", j.TraceID, j.SpanID, span.TraceID(), span.SpanID())
	}

	records := exporter.snapshot()
	if len(records) != 1 {
		t.Fatalf("got %d OTel records, want 1", len(records))
	}

	r := records[0]
	if r.Body().AsString() != "order creation failed" || r.Severity() != otellog.SeverityError {
		t.Errorf("unexpected OTel body or severity: %v, %v", r.Body(), r.Severity())
	}

	if r.TraceID() != span.TraceID() || r.SpanID() != span.SpanID() || r.TraceFlags() != span.TraceFlags() {
		t.Errorf("OTel trace identifiers did not propagate from context")
	}

	if r.InstrumentationScope().Name != "go-foundation/logger" || r.Timestamp().IsZero() {
		t.Errorf("unexpected OTel scope or timestamp: %v, %v", r.InstrumentationScope(), r.Timestamp())
	}

	attrs := make(map[string]any)
	r.WalkAttributes(func(kv attribute.KeyValue) bool {
		if kv.Key == "fields" {
			fields := make(map[string]any)
			for _, field := range kv.Value.AsMap() {
				fields[string(field.Key)] = field.Value.AsInterface()
			}

			attrs["fields"] = fields
			return true
		}

		attrs[string(kv.Key)] = kv.Value.AsInterface()
		return true
	})
	want := map[string]any{
		"service": "orders", "environment": "production", "version": "1.0.0",
		"trace_id": span.TraceID().String(), "span_id": span.SpanID().String(),
		"fields": map[string]any{
			"order_id": "ord_123", "error": "database unavailable", "retry": true,
			"attempt": int64(2), "duration_ms": int64(42), "duration": "42ms",
		},
	}
	if !reflect.DeepEqual(attrs, want) {
		t.Errorf("OTel attributes = %#v, want %#v", attrs, want)
	}
}

func TestLogger_OutputFailureStillEmitsToOpenTelemetry(t *testing.T) {
	provider, exporter := newProvider(t)
	writeErr := errors.New("output unavailable")
	var reported error
	log := newLogger(t, logger.Config{
		Service: "orders", Output: failingWriter{err: writeErr}, LoggerProvider: provider,
		OnError: func(err error) { reported = err },
	})

	log.Info(t.Context(), "order received")

	if !errors.Is(reported, writeErr) {
		t.Errorf("reported error = %v, want %v", reported, writeErr)
	}

	if records := exporter.snapshot(); len(records) != 1 || records[0].Body().AsString() != "order received" {
		t.Errorf("OTel did not receive event after JSON failure: %v", records)
	}
}

func TestLogger_ConcurrentChildren(t *testing.T) {
	var output bytes.Buffer
	provider, exporter := newProvider(t)
	log := newLogger(t, logger.Config{Service: "orders", Output: &output, LoggerProvider: provider})
	shared := log.With(logger.String("component", "orders"))
	const workers = 24
	var wg sync.WaitGroup

	for id := range workers {
		wg.Go(func() {
			child := shared.With(logger.Int("worker_id", id))
			child.Info(t.Context(), "worker completed", logger.Bool("success", true))
		})
	}

	wg.Wait()
	log.Info(t.Context(), "root")

	records := readRecords(t, &output)
	if len(records) != workers+1 || len(exporter.snapshot()) != workers+1 {
		t.Fatalf("both outputs must receive %d records; JSON=%d, OTel=%d", workers+1, len(records), len(exporter.snapshot()))
	}

	seen := make(map[json.Number]bool)
	for _, r := range records[:workers] {
		id, ok := r.Fields["worker_id"].(json.Number)
		if !ok || seen[id] || r.Fields["success"] != true || r.Fields["component"] != "orders" || len(r.Fields) != 3 {
			t.Fatalf("fields leaked between concurrent children: %#v", r.Fields)
		}

		seen[id] = true
	}

	if records[workers].Fields != nil {
		t.Errorf("child fields leaked to root: %#v", records[workers].Fields)
	}
}

func TestLogger_Context(t *testing.T) {
	t.Run("nil context", func(t *testing.T) {
		var output bytes.Buffer
		log := newLogger(t, logger.Config{Service: "orders", Output: &output})

		log.Info(nil, "startup")

		if records := readRecords(t, &output); len(records) != 1 {
			t.Errorf("got %d records, want 1", len(records))
		}
	})

	t.Run("canceled context retains trace", func(t *testing.T) {
		var output bytes.Buffer
		log := newLogger(t, logger.Config{Service: "orders", Output: &output})
		span := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{3}, SpanID: trace.SpanID{4}})
		ctx, cancel := context.WithCancel(trace.ContextWithSpanContext(t.Context(), span))
		cancel()

		log.Error(ctx, "request canceled")

		records := readRecords(t, &output)
		if len(records) != 1 || records[0].TraceID != span.TraceID().String() {
			t.Errorf("canceled context lost event or trace: %+v", records)
		}
	})
}

func ExampleLogger_With() {
	log, err := logger.New(logger.Config{Service: "orders", Output: io.Discard})
	if err != nil {
		panic(err)
	}

	orderLog := log.With(logger.String("order_id", "ord_123"))
	orderLog.Info(context.Background(), "order received", logger.Bool("retry", true))
	// Output:
}

type jsonRecord struct {
	Time        string         `json:"time"`
	Level       string         `json:"level"`
	Message     string         `json:"msg"`
	Service     string         `json:"service"`
	Environment string         `json:"environment"`
	Version     string         `json:"version"`
	TraceID     string         `json:"trace_id"`
	SpanID      string         `json:"span_id"`
	Fields      map[string]any `json:"fields"`
}

func readRecords(t *testing.T, output *bytes.Buffer) []jsonRecord {
	t.Helper()

	var records []jsonRecord
	scanner := bufio.NewScanner(bytes.NewReader(output.Bytes()))
	for scanner.Scan() {
		var record jsonRecord
		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.UseNumber()

		if err := decoder.Decode(&record); err != nil {
			t.Fatalf("invalid JSON record %s: %v", scanner.Bytes(), err)
		}

		records = append(records, record)
	}

	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}

	return records
}

func newLogger(t *testing.T, cfg logger.Config) *logger.Logger {
	t.Helper()

	log, err := logger.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	return log
}

func newProvider(t *testing.T) (*sdklog.LoggerProvider, *recordingExporter) {
	t.Helper()

	exporter := &recordingExporter{}
	provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exporter)))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := provider.Shutdown(ctx); err != nil {
			t.Errorf("shutdown provider: %v", err)
		}
	})

	return provider, exporter
}

type recordingExporter struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (e *recordingExporter) Export(ctx context.Context, records []sdklog.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	for _, record := range records {
		e.records = append(e.records, record.Clone())
	}

	return nil
}

func (e *recordingExporter) Shutdown(context.Context) error {
	return nil
}

func (e *recordingExporter) ForceFlush(context.Context) error {
	return nil
}

func (e *recordingExporter) snapshot() []sdklog.Record {
	e.mu.Lock()
	defer e.mu.Unlock()

	records := make([]sdklog.Record, len(e.records))
	for i, record := range e.records {
		records[i] = record.Clone()
	}

	return records
}

type failingWriter struct {
	err error
}

func (w failingWriter) Write([]byte) (int, error) {
	return 0, w.err
}
