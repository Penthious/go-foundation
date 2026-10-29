package grpcserver

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/logger"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
)

func TestStats_TracePropagationMetricsAndPrivateLogging(t *testing.T) {
	oldT, oldM, oldP := otel.GetTracerProvider(), otel.GetMeterProvider(), otel.GetTextMapPropagator()
	recorder := tracetest.NewSpanRecorder()
	traces := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	reader := sdkmetric.NewManualReader()
	meters := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetTracerProvider(traces)
	otel.SetMeterProvider(meters)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(oldT)
		otel.SetMeterProvider(oldM)
		otel.SetTextMapPropagator(oldP)
		_ = traces.Shutdown(t.Context())
		_ = meters.Shutdown(t.Context())
	})
	logs := &lockedBuffer{}
	log, err := logger.New(logger.Config{Service: "rpc-test", Output: logs})
	if err != nil {
		t.Fatal(err)
	}

	var received trace.SpanContext
	cfg := testConfig(t, &testService{unary: func(ctx context.Context, request *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		received = trace.SpanContextFromContext(ctx)

		return &testpb.SimpleResponse{Payload: request.Payload}, nil
	}})
	cfg.Logger = log
	server := startServer(t, cfg)
	connection := connect(t, server)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	ctx, parent := traces.Tracer("caller").Start(ctx, "client work")
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	values := metadata.New(map[string]string(carrier))
	values.Set("authorization", "sensitive-credential")
	request := &testpb.SimpleRequest{Payload: &testpb.Payload{Body: []byte("sensitive-payload")}}
	if _, err := testpb.NewTestServiceClient(connection).UnaryCall(metadata.NewOutgoingContext(ctx, values), request); err != nil {
		t.Fatal(err)
	}

	_ = connection.Invoke(ctx, "/private-unknown/secret-method", &testpb.Empty{}, &testpb.Empty{})
	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	parent.End()
	if !received.IsValid() || received.TraceID() != parent.SpanContext().TraceID() {
		t.Fatal("trace context did not propagate into RPC handler")
	}

	var serverSpan bool
	for _, span := range recorder.Ended() {
		if span.SpanKind() == trace.SpanKindServer {
			serverSpan = true
			if span.Parent().SpanID() != parent.SpanContext().SpanID() {
				t.Fatal("RPC span not parented to incoming context")
			}
		}

		data, err := json.Marshal(span.Attributes())
		if err != nil {
			t.Fatal(err)
		}

		if strings.Contains(string(data), "sensitive") || strings.Contains(string(data), "private-unknown") {
			t.Fatal("sensitive payload, metadata or unknown path entered telemetry")
		}
	}

	if !serverSpan || !strings.Contains(logs.String(), "grpc request completed") || !strings.Contains(logs.String(), "trace_id") || strings.Contains(logs.String(), "sensitive") || strings.Contains(logs.String(), "private-unknown") {
		t.Fatal("RPC logging or data privacy guarantees failed")
	}

	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &metrics); err != nil {
		t.Fatal(err)
	}

	var observations uint64
	for _, scope := range metrics.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			if histogram, ok := measurement.Data.(metricdata.Histogram[float64]); ok {
				for _, point := range histogram.DataPoints {
					observations += point.Count
				}
			}
		}
	}

	if observations == 0 {
		t.Fatal("no RPC duration metrics recorded")
	}
}
