package grpcserver

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/Penthious/go-foundation/auth"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func (s *Server) configurePolicies() error {
	s.public[healthpb.Health_Check_FullMethodName] = true
	s.public[healthpb.Health_Watch_FullMethodName] = true
	for _, method := range s.cfg.PublicMethods {
		if !s.methods[method] {
			return errors.New("grpcserver: public policy references an unknown method")
		}

		s.public[method] = true
	}

	for method := range s.cfg.RequireScopes {
		if !s.methods[method] || s.public[method] {
			return errors.New("grpcserver: scoped policy references an unknown or public method")
		}
	}

	return nil
}

func (s *Server) authorize(ctx context.Context, method string) (context.Context, error) {
	if s.cfg.Auth == nil || s.public[method] {
		return ctx, nil
	}

	values, _ := metadata.FromIncomingContext(ctx)
	headers := http.Header{"Authorization": values.Get("authorization"), "X-Api-Key": values.Get("x-api-key")}
	ctx, err := s.cfg.Auth.Authenticate(ctx, headers)
	if err != nil {
		return ctx, status.Error(codes.Unauthenticated, "unauthenticated")
	}

	principal, _ := auth.FromContext(ctx)
	if !principal.HasScopes(s.cfg.RequireScopes[method]...) {
		return ctx, status.Error(codes.PermissionDenied, "permission denied")
	}

	return ctx, nil
}

func (s *Server) unary(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response any, err error) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			response, err = nil, status.Error(codes.Internal, "internal server error")
		}

		err = rpcError(err)
	}()
	ctx, cancel := context.WithTimeout(ctx, s.cfg.UnaryTimeout)
	defer cancel()

	ctx, err = s.authorize(ctx, info.FullMethod)
	if contextErr := ctx.Err(); contextErr != nil {
		err = contextErr
	}

	if err == nil {
		if s.limited(info.FullMethod) {
			response, err = s.cfg.RateLimiter.UnaryServerInterceptor()(ctx, request, info, handler)
		} else {
			response, err = handler(ctx, request)
		}

		if err == nil {
			err = ctx.Err()
		}
	}

	returned = true

	return response, err
}

type contextualStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *contextualStream) Context() context.Context {
	return s.ctx
}

func (s *Server) stream(service any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			err = status.Error(codes.Internal, "internal server error")
		}

		err = rpcError(err)
	}()
	ctx, err := s.authorize(stream.Context(), info.FullMethod)
	if contextErr := ctx.Err(); contextErr != nil {
		err = contextErr
	}

	if err == nil {
		stream = &contextualStream{ServerStream: stream, ctx: ctx}
		if s.limited(info.FullMethod) {
			err = s.cfg.RateLimiter.StreamServerInterceptor()(service, stream, info, handler)
		} else {
			err = handler(service, stream)
		}

		if err == nil {
			err = ctx.Err()
		}
	}

	returned = true

	return err
}

func rpcError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}

	if _, ok := status.FromError(err); ok {
		return err
	}

	return status.Error(codes.Internal, "internal server error")
}

func (s *Server) limited(method string) bool {
	return s.cfg.RateLimiter != nil && !s.public[method] && !strings.HasPrefix(method, "/grpc.health.v1.Health/")
}
