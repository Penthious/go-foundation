package grpcserver

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/health"

	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"
)

func TestHealth_ReadinessWatchAndShutdown(t *testing.T) {
	var down atomic.Bool
	var optional atomic.Int64
	checks, err := health.New(health.Required("database", func(context.Context) error {
		if down.Load() {
			return errors.New("private backend details")
		}

		return nil
	}), health.Optional("cache", func(context.Context) error {
		optional.Add(1)

		return errors.New("optional outage")
	}))
	if err != nil {
		t.Fatal(err)
	}

	cfg := testConfig(t, &testService{})
	cfg.Health, cfg.HealthWatchInterval = checks, 5*time.Millisecond
	server := startServer(t, cfg)
	client := healthpb.NewHealthClient(connect(t, server))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	response, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || response.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("readiness: %v, %v", response, err)
	}

	if optional.Load() != 0 {
		t.Fatal("optional dependencies affected readiness")
	}

	if _, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: "unknown"}); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown service: %v", err)
	}

	listed, err := client.List(ctx, &healthpb.HealthListRequest{})
	if err != nil || len(listed.GetStatuses()) != 3 {
		t.Fatalf("health listing: %v, %v", listed, err)
	}

	watch, err := client.Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}

	response, err = watch.Recv()
	if err != nil || response.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatal("watch did not send initial status")
	}

	down.Store(true)
	response, err = watch.Recv()
	if err != nil || response.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatal("watch missed dependency failure")
	}

	down.Store(false)
	response, err = watch.Recv()
	if err != nil || response.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatal("watch missed dependency recovery")
	}

	unknown, err := client.Watch(ctx, &healthpb.HealthCheckRequest{Service: "unknown"})
	if err != nil {
		t.Fatal(err)
	}

	response, err = unknown.Recv()
	if err != nil || response.GetStatus() != healthpb.HealthCheckResponse_SERVICE_UNKNOWN {
		t.Fatal("unknown watch did not report SERVICE_UNKNOWN")
	}

	if err := server.Shutdown(ctx); err != nil {
		t.Fatal("health watch prevented graceful shutdown:", err)
	}

	response, err = watch.Recv()
	if err != nil || response.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatal("watch did not report draining status")
	}

	if _, err := watch.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("watch remained open after shutdown: %v", err)
	}
}

func TestHealth_DependencyTimeout(t *testing.T) {
	checks, err := health.New(health.Required("slow", func(ctx context.Context) error {
		<-ctx.Done()

		return ctx.Err()
	}))
	if err != nil {
		t.Fatal(err)
	}

	cfg := testConfig(t, &testService{})
	cfg.Health, cfg.HealthTimeout = checks, 20*time.Millisecond
	server := startServer(t, cfg)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	response, err := healthpb.NewHealthClient(connect(t, server)).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || response.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("timed-out dependency: %v, %v", response, err)
	}
}

func TestReflection_ExplicitOptIn(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(t *testing.T) {
			cfg := testConfig(t, &testService{})
			cfg.Reflection = enabled
			server := startServer(t, cfg)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()

			stream, err := reflectionpb.NewServerReflectionClient(connect(t, server)).ServerReflectionInfo(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if err := stream.Send(&reflectionpb.ServerReflectionRequest{MessageRequest: &reflectionpb.ServerReflectionRequest_ListServices{ListServices: ""}}); err != nil && (enabled || !errors.Is(err, io.EOF)) {
				t.Fatal(err)
			}

			response, err := stream.Recv()
			if enabled && (err != nil || len(response.GetListServicesResponse().GetService()) < 2) {
				t.Fatalf("enabled reflection failed: %v", err)
			}

			if !enabled && status.Code(err) != codes.Unimplemented {
				t.Fatalf("reflection unexpectedly available: %v", err)
			}
		})
	}
}
