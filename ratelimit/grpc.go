package ratelimit

import (
	"context"
	"errors"

	"github.com/Penthious/go-foundation/auth"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// UnaryServerInterceptor limits one RPC after a preceding authentication
// interceptor installs a GOF principal. No raw credential metadata is trusted.
func (l *Limiter) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		decision, err := l.allowPrincipal(ctx)
		if err := grpcDecision(decision, err); err != nil {
			return nil, err
		}

		return handler(ctx, request)
	}
}

// StreamServerInterceptor charges once at stream establishment, not per message
// or active stream. Authentication must precede it. It does not wrap the stream.
func (l *Limiter) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(service any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		decision, err := l.allowPrincipal(stream.Context())
		if err := grpcDecision(decision, err); err != nil {
			return err
		}

		return handler(service, stream)
	}
}

func grpcDecision(decision Decision, err error) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, auth.ErrUnauthenticated):
		return status.Error(codes.Unauthenticated, "unauthenticated")
	case err != nil:
		return status.Error(codes.Unavailable, "rate limiter unavailable")
	case decision.Allowed:
		return nil
	}

	result := status.New(codes.ResourceExhausted, "rate limit exceeded")
	withRetry, err := result.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(decision.RetryAfter)})
	if err != nil {
		return result.Err()
	}

	return withRetry.Err()
}
