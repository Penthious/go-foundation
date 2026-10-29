package telemetry_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/logger"
	"github.com/Penthious/go-foundation/telemetry"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/log/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestNew_ValidationPreservesGlobals(t *testing.T) {
	cleanEnvironment(t)
	originalLogs := otel.GetLoggerProvider()
	originalTraces := otel.GetTracerProvider()
	originalMetrics := otel.GetMeterProvider()
	originalPropagation := otel.GetTextMapPropagator()
	tests := []struct {
		name string
		cfg  telemetry.Config
	}{
		{name: "missing service"},
		{name: "blank service", cfg: telemetry.Config{Service: " \t"}},
	}
	for _, endpoint := range []string{
		"collector:4317", "ftp://collector:4317", "http://", "http://user:secret@collector:4317",
		"http://collector:4317/path", "http://collector:4317?x=y", "http://collector:4317#fragment", "%",
	} {
		tests = append(tests, struct {
			name string
			cfg  telemetry.Config
		}{name: "invalid endpoint " + endpoint, cfg: telemetry.Config{Service: "orders", Endpoint: endpoint}})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sdk, err := telemetry.New(t.Context(), tt.cfg)

			if err == nil || sdk != nil {
				t.Fatalf("New returned (%v, %v), want nil SDK and an error", sdk, err)
			}

			if otel.GetLoggerProvider() != originalLogs || otel.GetTracerProvider() != originalTraces ||
				otel.GetMeterProvider() != originalMetrics || !reflect.DeepEqual(otel.GetTextMapPropagator(), originalPropagation) {
				t.Fatal("failed setup changed global providers or propagation")
			}
		})
	}

	if _, err := telemetry.New(nil, telemetry.Config{Service: "orders"}); err == nil {
		t.Error("New accepted a nil context")
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := telemetry.New(ctx, telemetry.Config{Service: "orders"}); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled setup error = %v, want context.Canceled", err)
	}
}

func TestNew_ExportsAllSignals(t *testing.T) {
	for _, mode := range []string{"explicit", "environment", "signal environment", "TLS"} {
		t.Run(mode, func(t *testing.T) {
			cleanEnvironment(t)
			restoreGlobals(t)
			var options []grpc.ServerOption
			if mode == "TLS" {
				options = append(options, collectorTLS(t))
			}

			collector, endpoint := newCollector(t, options...)
			if mode == "TLS" {
				endpoint = "https" + strings.TrimPrefix(endpoint, "http")
			}

			var output bytes.Buffer
			cfg := telemetry.Config{
				Service: "orders", Environment: "test", Version: "1.2.3",
			}
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "team=platform,service.name=wrong")
			switch mode {
			case "explicit", "TLS":
				cfg.Endpoint = endpoint
				cfg.Headers = map[string]string{"authorization": "Bearer explicit"}
				t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
				t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "authorization=wrong")
			case "environment":
				t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
				t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "authorization=Bearer%20explicit")
			case "signal environment":
				t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
				for _, signal := range []string{"LOGS", "TRACES", "METRICS"} {
					t.Setenv("OTEL_EXPORTER_OTLP_"+signal+"_ENDPOINT", endpoint)
					t.Setenv("OTEL_EXPORTER_OTLP_"+signal+"_HEADERS", "authorization=Bearer%20explicit")
				}
			}

			sdk := newSDK(t, cfg)
			log := newLogger(t, logger.Config{
				Service: cfg.Service, Environment: cfg.Environment, Version: cfg.Version,
				Output: &output, LoggerProvider: sdk.LoggerProvider(),
			})
			if cfg.Headers != nil {
				cfg.Headers["authorization"] = "changed after setup"
			}

			spanContext := emitSignals(t, sdk, log)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			if err := sdk.ForceFlush(ctx); err != nil {
				t.Fatalf("flush: %v", err)
			}

			collector.mu.Lock()
			defer collector.mu.Unlock()

			if len(collector.logs) != 1 || len(collector.traces) != 1 || len(collector.metrics) != 1 {
				t.Fatalf("export counts: logs=%d traces=%d metrics=%d, want one of each",
					len(collector.logs), len(collector.traces), len(collector.metrics))
			}

			logs := collector.logs[0].ResourceLogs[0]
			traces := collector.traces[0].ResourceSpans[0]
			metrics := collector.metrics[0].ResourceMetrics[0]
			for _, attrs := range [][]*commonpb.KeyValue{logs.Resource.Attributes, traces.Resource.Attributes, metrics.Resource.Attributes} {
				got := stringAttributes(attrs)
				for key, want := range map[string]string{
					"service.name": "orders", "service.version": "1.2.3",
					"deployment.environment.name": "test", "team": "platform",
				} {
					if got[key] != want {
						t.Errorf("resource %s = %q, want %q", key, got[key], want)
					}
				}
			}

			logRecord := logs.ScopeLogs[0].LogRecords[0]
			span := traces.ScopeSpans[0].Spans[0]
			if logRecord.Body.GetStringValue() != "order received" || span.Name != "receive order" {
				t.Errorf("unexpected log or span: %v, %v", logRecord, span)
			}

			if !bytes.Equal(logRecord.TraceId, span.TraceId) || !bytes.Equal(logRecord.SpanId, span.SpanId) ||
				hex.EncodeToString(span.TraceId) != spanContext.TraceID().String() {
				t.Error("exported logs and traces did not correlate")
			}

			metric := metrics.ScopeMetrics[0].Metrics[0]
			if metric.Name != "orders.received" || metric.GetSum().DataPoints[0].GetAsInt() != 1 {
				t.Errorf("unexpected metric: %v", metric)
			}

			for _, auth := range collector.auth {
				if auth != "Bearer explicit" {
					t.Errorf("collector authorization = %q, want configured header", auth)
				}
			}

			var jsonLog map[string]any
			if err := json.Unmarshal(output.Bytes(), &jsonLog); err != nil {
				t.Fatal(err)
			}

			if jsonLog["trace_id"] != spanContext.TraceID().String() {
				t.Errorf("JSON output missing correlated trace: %v", jsonLog)
			}
		})
	}
}

func TestNew_GlobalInstrumentationAndPropagation(t *testing.T) {
	cleanEnvironment(t)
	restoreGlobals(t)
	collector, endpoint := newCollector(t)
	sdk := newSDK(t, telemetry.Config{Service: "orders", Endpoint: endpoint})
	member, err := baggage.NewMember("region", "west")
	if err != nil {
		t.Fatal(err)
	}

	bag, err := baggage.New(member)
	if err != nil {
		t.Fatal(err)
	}

	ctx, span := otel.Tracer("library").Start(baggage.ContextWithBaggage(t.Context(), bag), "global span")
	counter, err := otel.Meter("library").Int64Counter("global.counter")
	if err != nil {
		t.Fatal(err)
	}

	log, err := logger.New(logger.Config{Service: "orders", Output: io.Discard})
	if err != nil {
		t.Fatal(err)
	}

	counter.Add(ctx, 1)
	log.Info(ctx, "global log")
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	extracted := otel.GetTextMapPropagator().Extract(t.Context(), carrier)
	span.End()

	if got := trace.SpanContextFromContext(extracted); got.TraceID() != span.SpanContext().TraceID() || !got.IsRemote() {
		t.Errorf("W3C trace context did not round trip: %v", got)
	}

	if got := baggage.FromContext(extracted).Member("region").Value(); got != "west" {
		t.Errorf("baggage = %q, want west", got)
	}

	flushCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if err := sdk.ForceFlush(flushCtx); err != nil {
		t.Fatal(err)
	}

	collector.mu.Lock()
	defer collector.mu.Unlock()

	if len(collector.logs) != 1 || len(collector.traces) != 1 || len(collector.metrics) != 1 {
		t.Fatal("global instruments did not export all three signals")
	}
}

func TestSDK_ShutdownFlushesAfterCancellation(t *testing.T) {
	cleanEnvironment(t)
	restoreGlobals(t)
	collector, endpoint := newCollector(t)
	ctx, cancel := context.WithCancel(t.Context())
	sdk, err := telemetry.New(ctx, telemetry.Config{Service: "orders", Endpoint: endpoint})
	if err != nil {
		t.Fatal(err)
	}

	log := newLogger(t, logger.Config{Service: "orders", Output: io.Discard, LoggerProvider: sdk.LoggerProvider()})

	emitSignals(t, sdk, log)
	cancel()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	var wg sync.WaitGroup
	shutdownErrors := make(chan error, 4)

	for range 4 {
		wg.Go(func() { shutdownErrors <- sdk.Shutdown(stopCtx) })
	}

	wg.Wait()
	close(shutdownErrors)
	for err := range shutdownErrors {
		if err != nil {
			t.Errorf("shutdown: %v", err)
		}
	}

	collector.mu.Lock()
	defer collector.mu.Unlock()

	if len(collector.logs) != 1 || len(collector.traces) != 1 || len(collector.metrics) != 1 {
		t.Errorf("shutdown exports: logs=%d traces=%d metrics=%d, want exactly one of each",
			len(collector.logs), len(collector.traces), len(collector.metrics))
	}
}

func TestSDK_CollectorFailureStillAttemptsAllSignals(t *testing.T) {
	cleanEnvironment(t)
	restoreGlobals(t)
	collector, endpoint := newCollector(t, grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, err := handler(ctx, req); err != nil {
			return nil, err
		}

		return nil, status.Error(codes.InvalidArgument, "collector rejected event")
	}))
	sdk, err := telemetry.New(t.Context(), telemetry.Config{Service: "orders", Endpoint: endpoint})
	if err != nil {
		t.Fatal(err)
	}

	log := newLogger(t, logger.Config{Service: "orders", Output: io.Discard, LoggerProvider: sdk.LoggerProvider()})

	emitSignals(t, sdk, log)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	flushErr := sdk.ForceFlush(ctx)
	stopErr := sdk.Shutdown(ctx)

	if flushErr == nil && stopErr == nil {
		t.Error("collector rejection was not reported")
	}

	collector.mu.Lock()
	defer collector.mu.Unlock()

	if len(collector.logs) == 0 || len(collector.traces) == 0 || len(collector.metrics) == 0 {
		t.Error("one failed signal prevented attempts to export the others")
	}
}

func TestSDK_ExporterTimeout(t *testing.T) {
	cleanEnvironment(t)
	restoreGlobals(t)
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "50")
	_, endpoint := newCollector(t, grpc.UnaryInterceptor(func(ctx context.Context, _ any, _ *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}))
	sdk, err := telemetry.New(t.Context(), telemetry.Config{Service: "orders", Endpoint: endpoint})
	if err != nil {
		t.Fatal(err)
	}

	log := newLogger(t, logger.Config{Service: "orders", Output: io.Discard, LoggerProvider: sdk.LoggerProvider()})

	emitSignals(t, sdk, log)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	flushErr := sdk.ForceFlush(ctx)
	stopErr := sdk.Shutdown(ctx)

	if flushErr == nil {
		t.Error("hung collector did not produce an export timeout")
	}

	if ctx.Err() != nil {
		t.Errorf("exporter timeout did not bound flush and shutdown: flush=%v shutdown=%v", flushErr, stopErr)
	}
}

func TestSDK_LoggerProvider(t *testing.T) {
	cleanEnvironment(t)
	restoreGlobals(t)
	collector, endpoint := newCollector(t)
	sdk := newSDK(t, telemetry.Config{Service: "orders", Endpoint: endpoint})
	otel.SetLoggerProvider(noop.NewLoggerProvider())
	var output bytes.Buffer
	log := newLogger(t, logger.Config{
		Service: "orders", Level: logger.LevelWarn, Output: &output, LoggerProvider: sdk.LoggerProvider(),
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	log.Info(ctx, "filtered")
	log.Error(ctx, "service-owned logger")

	if err := sdk.ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}

	collector.mu.Lock()
	defer collector.mu.Unlock()

	if len(collector.logs) != 1 {
		t.Fatalf("collector received %d log batches, want 1", len(collector.logs))
	}

	records := collector.logs[0].ResourceLogs[0].ScopeLogs[0].LogRecords
	if len(records) != 1 || records[0].Body.GetStringValue() != "service-owned logger" {
		t.Errorf("explicit provider did not receive the service's filtered logs: %v", records)
	}

	var jsonLog map[string]any
	if err := json.Unmarshal(output.Bytes(), &jsonLog); err != nil {
		t.Fatal(err)
	}

	if jsonLog["msg"] != "service-owned logger" || jsonLog["level"] != "ERROR" {
		t.Errorf("unexpected service-owned JSON output: %v", jsonLog)
	}
}

func emitSignals(t *testing.T, sdk *telemetry.SDK, log *logger.Logger) trace.SpanContext {
	t.Helper()

	ctx, span := sdk.Tracer.Start(t.Context(), "receive order")
	defer span.End()

	counter, err := sdk.Meter.Int64Counter("orders.received")
	if err != nil {
		t.Fatal(err)
	}

	counter.Add(ctx, 1)
	log.Info(ctx, "order received", logger.String("order_id", "ord_123"))

	return span.SpanContext()
}

func newLogger(t *testing.T, cfg logger.Config) *logger.Logger {
	t.Helper()

	log, err := logger.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	return log
}

func newSDK(t *testing.T, cfg telemetry.Config) *telemetry.SDK {
	t.Helper()

	sdk, err := telemetry.New(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := sdk.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})

	return sdk
}

func cleanEnvironment(t *testing.T) {
	t.Helper()

	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "OTEL_") {
			t.Setenv(key, "")
		}
	}

	t.Setenv("OTEL_TRACES_SAMPLER", "always_on")
	t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "3600000")
	t.Setenv("OTEL_BSP_SCHEDULE_DELAY", "3600000")
	t.Setenv("OTEL_BLRP_SCHEDULE_DELAY", "3600000")
}

func restoreGlobals(t *testing.T) {
	t.Helper()

	logs, traces, metrics := otel.GetLoggerProvider(), otel.GetTracerProvider(), otel.GetMeterProvider()
	propagator := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetLoggerProvider(logs)
		otel.SetTracerProvider(traces)
		otel.SetMeterProvider(metrics)
		otel.SetTextMapPropagator(propagator)
	})
}

type collector struct {
	mu      sync.Mutex
	logs    []*logspb.ExportLogsServiceRequest
	traces  []*tracepb.ExportTraceServiceRequest
	metrics []*metricspb.ExportMetricsServiceRequest
	auth    []string
}

func newCollector(t *testing.T, options ...grpc.ServerOption) (*collector, string) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	c := &collector{}
	server := grpc.NewServer(options...)
	logspb.RegisterLogsServiceServer(server, &logReceiver{collector: c})
	tracepb.RegisterTraceServiceServer(server, &traceReceiver{collector: c})
	metricspb.RegisterMetricsServiceServer(server, &metricReceiver{collector: c})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		if err := <-done; err != nil {
			t.Errorf("collector: %v", err)
		}
	})

	return c, "http://" + listener.Addr().String()
}

type logReceiver struct {
	logspb.UnimplementedLogsServiceServer
	collector *collector
}

func (r *logReceiver) Export(ctx context.Context, req *logspb.ExportLogsServiceRequest) (*logspb.ExportLogsServiceResponse, error) {
	r.collector.mu.Lock()
	defer r.collector.mu.Unlock()

	r.collector.logs = append(r.collector.logs, req)
	r.collector.auth = append(r.collector.auth, incomingAuthorization(ctx))

	return &logspb.ExportLogsServiceResponse{}, nil
}

type traceReceiver struct {
	tracepb.UnimplementedTraceServiceServer
	collector *collector
}

func (r *traceReceiver) Export(ctx context.Context, req *tracepb.ExportTraceServiceRequest) (*tracepb.ExportTraceServiceResponse, error) {
	r.collector.mu.Lock()
	defer r.collector.mu.Unlock()

	r.collector.traces = append(r.collector.traces, req)
	r.collector.auth = append(r.collector.auth, incomingAuthorization(ctx))

	return &tracepb.ExportTraceServiceResponse{}, nil
}

type metricReceiver struct {
	metricspb.UnimplementedMetricsServiceServer
	collector *collector
}

func (r *metricReceiver) Export(ctx context.Context, req *metricspb.ExportMetricsServiceRequest) (*metricspb.ExportMetricsServiceResponse, error) {
	r.collector.mu.Lock()
	defer r.collector.mu.Unlock()

	r.collector.metrics = append(r.collector.metrics, req)
	r.collector.auth = append(r.collector.auth, incomingAuthorization(ctx))

	return &metricspb.ExportMetricsServiceResponse{}, nil
}

func incomingAuthorization(ctx context.Context) string {
	md, _ := metadata.FromIncomingContext(ctx)
	return strings.Join(md.Get("authorization"), ",")
}

func stringAttributes(attrs []*commonpb.KeyValue) map[string]string {
	result := make(map[string]string)
	for _, attr := range attrs {
		result[attr.Key] = attr.Value.GetStringValue()
	}

	return result
}

func collectorTLS(t *testing.T) grpc.ServerOption {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, BasicConstraintsValid: true, IsCA: true,
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "collector-ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", path)

	return grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
	}))
}
