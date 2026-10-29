package grpcserver

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type probeServer struct {
	healthpb.UnimplementedHealthServer
	server *Server
}

func (p *probeServer) serving(ctx context.Context) healthpb.HealthCheckResponse_ServingStatus {
	if !p.server.ready.Load() {
		return healthpb.HealthCheckResponse_NOT_SERVING
	}

	if p.server.cfg.Health != nil {
		ctx, cancel := context.WithTimeout(ctx, p.server.cfg.HealthTimeout)
		defer cancel()

		if !p.server.cfg.Health.Readiness(ctx).Ready {
			return healthpb.HealthCheckResponse_NOT_SERVING
		}
	}

	return healthpb.HealthCheckResponse_SERVING
}

func (p *probeServer) Check(ctx context.Context, request *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if !p.server.services[request.Service] {
		return nil, status.Error(codes.NotFound, "unknown service")
	}

	return &healthpb.HealthCheckResponse{Status: p.serving(ctx)}, nil
}

func (p *probeServer) List(ctx context.Context, _ *healthpb.HealthListRequest) (*healthpb.HealthListResponse, error) {
	state := p.serving(ctx)
	result := &healthpb.HealthListResponse{Statuses: make(map[string]*healthpb.HealthCheckResponse)}
	for service := range p.server.services {
		result.Statuses[service] = &healthpb.HealthCheckResponse{Status: state}
	}

	return result, nil
}

func (p *probeServer) Watch(request *healthpb.HealthCheckRequest, stream grpc.ServerStreamingServer[healthpb.HealthCheckResponse]) error {
	ticker := time.NewTicker(p.server.cfg.HealthWatchInterval)
	defer ticker.Stop()

	last := healthpb.HealthCheckResponse_UNKNOWN
	for {
		current := healthpb.HealthCheckResponse_SERVICE_UNKNOWN
		if p.server.services[request.Service] {
			current = p.serving(stream.Context())
		}

		if current != last {
			if err := stream.Send(&healthpb.HealthCheckResponse{Status: current}); err != nil {
				return err
			}

			last = current
		}

		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-p.server.draining:
			if p.server.services[request.Service] && last != healthpb.HealthCheckResponse_NOT_SERVING {
				return stream.Send(&healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_NOT_SERVING})
			}

			return nil
		case <-ticker.C:
		}
	}
}
