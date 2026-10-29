package grpcclient

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/logger"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	testpb "google.golang.org/grpc/interop/grpc_testing"
)

func TestClient_TelemetryAndMetadataPrivacy(t *testing.T) {
	oldT, oldM, oldP := otel.GetTracerProvider(), otel.GetMeterProvider(), otel.GetTextMapPropagator()
	recorder := tracetest.NewSpanRecorder()
	traces := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	reader := sdkmetric.NewManualReader()
	meters := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetTracerProvider(traces)
	otel.SetMeterProvider(meters)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() {
		otel.SetTracerProvider(oldT)
		otel.SetMeterProvider(oldM)
		otel.SetTextMapPropagator(oldP)
		_ = traces.Shutdown(t.Context())
		_ = meters.Shutdown(t.Context())
	})
	serverReceived := make(chan trace.SpanContext, 1)
	server := testServer(t, &testService{unary: func(ctx context.Context, request *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		serverReceived <- trace.SpanContextFromContext(ctx)
		if baggage.FromContext(ctx).Member("tenant").Value() != "tenant-1" {
			t.Error("baggage did not propagate")
		}

		return &testpb.SimpleResponse{Payload: request.Payload}, nil
	}}, nil)
	logs := &safeBuffer{}
	log, err := logger.New(logger.Config{Service: "client-telemetry-test", Output: logs})
	if err != nil {
		t.Fatal(err)
	}

	cfg := testConfig(t, server.Address())
	cfg.Logger, cfg.APIKey = log, "sensitive-key"
	client := testClient(t, cfg)
	member, err := baggage.NewMember("tenant", "tenant-1")
	if err != nil {
		t.Fatal(err)
	}

	values, err := baggage.New(member)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(baggage.ContextWithBaggage(t.Context(), values), 3*time.Second)
	defer cancel()

	ctx, parent := traces.Tracer("business").Start(ctx, "order operation")
	if _, err := testpb.NewTestServiceClient(client).UnaryCall(ctx, &testpb.SimpleRequest{Payload: &testpb.Payload{Body: []byte("sensitive-body")}}); err != nil {
		t.Fatal(err)
	}

	remote := receive(t, serverReceived)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	parent.End()
	var clientSpan, serverSpan sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		switch span.SpanKind() {
		case trace.SpanKindClient:
			clientSpan = span
		case trace.SpanKindServer:
			serverSpan = span
		}

		attributes, err := json.Marshal(span.Attributes())
		if err != nil {
			t.Fatal(err)
		}

		if strings.Contains(string(attributes), "sensitive") {
			t.Fatal("credentials/body leaked into span attributes")
		}
	}

	if clientSpan == nil || serverSpan == nil || clientSpan.Parent().SpanID() != parent.SpanContext().SpanID() || serverSpan.Parent().SpanID() != clientSpan.SpanContext().SpanID() || remote.TraceID() != parent.SpanContext().TraceID() {
		t.Fatal("business → client → server trace hierarchy was not preserved")
	}

	if strings.Contains(logs.String(), "sensitive") || !strings.Contains(logs.String(), "grpc client request completed") || !strings.Contains(logs.String(), "trace_id") {
		t.Fatal("RPC logging or credential privacy failed")
	}

	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}

	var clientMeasurements uint64
	for _, scope := range data.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			if strings.Contains(measurement.Name, "client") {
				if histogram, ok := measurement.Data.(metricdata.Histogram[float64]); ok {
					for _, point := range histogram.DataPoints {
						clientMeasurements += point.Count
					}
				}
			}
		}
	}

	if clientMeasurements == 0 {
		t.Fatal("client RPC duration metrics missing")
	}
}

type safeBuffer struct {
	mu    sync.Mutex
	value bytes.Buffer
}

func (b *safeBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.value.Write(value)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.value.String()
}
