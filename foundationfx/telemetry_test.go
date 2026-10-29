package foundationfx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/logger"
	"github.com/Penthious/go-foundation/telemetry"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/log/noop"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

func TestModule_BorrowedTelemetry(t *testing.T) {
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "OTEL_") {
			t.Setenv(key, "")
		}
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	collector := &logCollector{}
	server := grpc.NewServer()
	logspb.RegisterLogsServiceServer(server, collector)
	metricspb.RegisterMetricsServiceServer(server, &metricCollector{})
	tracepb.RegisterTraceServiceServer(server, &traceCollector{})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	oldLogs, oldTraces, oldMetrics := otel.GetLoggerProvider(), otel.GetTracerProvider(), otel.GetMeterProvider()
	oldProp := otel.GetTextMapPropagator()
	sdk, err := telemetry.New(t.Context(), telemetry.Config{Service: "orders", Endpoint: "http://" + listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := sdk.Shutdown(ctx); err != nil {
			t.Error(err)
		}

		otel.SetLoggerProvider(oldLogs)
		otel.SetTracerProvider(oldTraces)
		otel.SetMeterProvider(oldMetrics)
		otel.SetTextMapPropagator(oldProp)
	})
	otel.SetLoggerProvider(noop.NewLoggerProvider())
	var log *logger.Logger
	var tracer trace.Tracer
	var meter metric.Meter
	app := startApp(t,
		fx.Supply(sdk, logger.Config{Service: "orders", Output: io.Discard}),
		fx.Populate(&log, &tracer, &meter),
	)

	if tracer != sdk.Tracer || meter != sdk.Meter {
		t.Error("Fx instruments did not come from the supplied SDK")
	}

	log.Info(t.Context(), "service log")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if err := sdk.ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}

	if collector.records.Load() != 1 {
		t.Error("logger did not use the supplied SDK log provider")
	}

	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	log.Info(t.Context(), "SDK still belongs to the service")
	if err := sdk.ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}

	if collector.records.Load() != 2 {
		t.Error("Fx incorrectly shut down the borrowed telemetry SDK")
	}
}

type logCollector struct {
	logspb.UnimplementedLogsServiceServer
	records atomic.Int32
}

func (c *logCollector) Export(_ context.Context, req *logspb.ExportLogsServiceRequest) (*logspb.ExportLogsServiceResponse, error) {
	for _, resource := range req.ResourceLogs {
		for _, scope := range resource.ScopeLogs {
			c.records.Add(int32(len(scope.LogRecords)))
		}
	}

	return &logspb.ExportLogsServiceResponse{}, nil
}

type metricCollector struct {
	metricspb.UnimplementedMetricsServiceServer
}

func (*metricCollector) Export(context.Context, *metricspb.ExportMetricsServiceRequest) (*metricspb.ExportMetricsServiceResponse, error) {
	return &metricspb.ExportMetricsServiceResponse{}, nil
}

type traceCollector struct {
	tracepb.UnimplementedTraceServiceServer
}

func (*traceCollector) Export(context.Context, *tracepb.ExportTraceServiceRequest) (*tracepb.ExportTraceServiceResponse, error) {
	return &tracepb.ExportTraceServiceResponse{}, nil
}

func newCircuitObservations(t *testing.T) (*sdkmetric.ManualReader, *circuitOutput) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(previous)
		_ = provider.Shutdown(context.Background())
	})

	return reader, &circuitOutput{}
}

func assertCircuitObservations(t *testing.T, reader *sdkmetric.ManualReader, output *circuitOutput, protocol string) {
	t.Helper()

	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}

	counts := make(map[string]int64)
	for _, scope := range data.ScopeMetrics {
		if scope.Scope.Name != "go-foundation/circuitbreaker" {
			continue
		}

		for _, measurement := range scope.Metrics {
			sum, ok := measurement.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatal("circuit measurement was not an integer counter")
			}

			for _, point := range sum.DataPoints {
				name, _ := point.Attributes.Value("client.name")
				kind, _ := point.Attributes.Value("client.type")
				if kind.AsString() != protocol || name.AsString() != "orders" && name.AsString() != "inventory" {
					t.Fatal("Fx binding identity was not used:", point.Attributes)
				}

				counts[measurement.Name+"/"+name.AsString()] += point.Value
			}
		}
	}

	if len(counts) != 4 {
		t.Fatal("missing transition/rejection measurements for both clients:", counts)
	}

	for _, name := range []string{"orders", "inventory"} {
		for _, metric := range []string{"circuit_breaker.transitions", "circuit_breaker.rejections"} {
			if counts[metric+"/"+name] != 1 {
				t.Fatal("named circuit events were double-counted or combined:", counts)
			}
		}
	}

	decoder := json.NewDecoder(bytes.NewReader(output.snapshot()))
	logs := make(map[string]int)
	for decoder.More() {
		var record struct {
			Message string         `json:"msg"`
			Fields  map[string]any `json:"fields"`
		}
		if err := decoder.Decode(&record); err != nil {
			t.Fatal(err)
		}

		if !strings.HasPrefix(record.Message, "circuit breaker ") {
			continue
		}

		name, _ := record.Fields["client_name"].(string)
		if record.Fields["client_type"] != protocol || name != "orders" && name != "inventory" {
			t.Fatal("circuit log lost its named binding:", record)
		}

		logs[record.Message+"/"+name]++
	}

	for _, name := range []string{"orders", "inventory"} {
		for _, message := range []string{"circuit breaker state changed", "circuit breaker call rejected"} {
			if logs[message+"/"+name] != 1 {
				t.Fatal("missing named client circuit logs:", logs)
			}
		}
	}
}

type circuitOutput struct {
	mu   sync.Mutex
	data bytes.Buffer
}

func (o *circuitOutput) Write(data []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.data.Write(data)
}

func (o *circuitOutput) snapshot() []byte {
	o.mu.Lock()
	defer o.mu.Unlock()

	return bytes.Clone(o.data.Bytes())
}
