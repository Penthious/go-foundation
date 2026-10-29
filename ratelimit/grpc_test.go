package ratelimit

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestInterceptors_QuotaSharingRetryDetailsAndFailClosed(t *testing.T) {
	cfg := testConfig(t, Memory)
	cfg.Limit, cfg.Window, cfg.Burst = 1, time.Hour, 1
	limiter := testLimiter(t, cfg)
	ctx := apiContext(t, "alice", strings.Repeat("a", 32))
	calls := 0
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Call"}
	handler := func(context.Context, any) (any, error) {
		calls++

		return "response", nil
	}
	response, err := limiter.UnaryServerInterceptor()(ctx, "request", info, handler)
	if err != nil || response != "response" {
		t.Fatal("first unary call failed:", err)
	}

	_, err = limiter.UnaryServerInterceptor()(ctx, "request", info, handler)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatal("second call was not rate limited:", err)
	}

	details := status.Convert(err).Details()
	if len(details) != 1 {
		t.Fatal("denial omitted RetryInfo")
	}

	retry, ok := details[0].(*errdetails.RetryInfo)
	if !ok || retry.RetryDelay.AsDuration() <= 0 || retry.RetryDelay.AsDuration() > time.Hour {
		t.Fatal("invalid retry delay:", details)
	}

	streamHandler := func(any, grpc.ServerStream) error {
		calls++

		return nil
	}
	err = limiter.StreamServerInterceptor()(nil, &testServerStream{ctx: ctx}, &grpc.StreamServerInfo{}, streamHandler)
	if status.Code(err) != codes.ResourceExhausted || calls != 1 {
		t.Fatal("stream did not share the unary quota:", err)
	}

	for _, stream := range []bool{false, true} {
		forged := metadata.NewIncomingContext(t.Context(), metadata.Pairs("x-api-key", strings.Repeat("a", 32)))
		if stream {
			err = limiter.StreamServerInterceptor()(nil, &testServerStream{ctx: forged}, &grpc.StreamServerInfo{}, streamHandler)
		} else {
			_, err = limiter.UnaryServerInterceptor()(forged, nil, info, handler)
		}

		if status.Code(err) != codes.Unauthenticated {
			t.Fatal("raw credential metadata was trusted as a principal:", err)
		}
	}

	if err := limiter.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = limiter.UnaryServerInterceptor()(ctx, nil, info, handler)
	if status.Code(err) != codes.Unavailable {
		t.Fatal("unavailable limiter allowed an RPC:", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()

	_, err = limiter.UnaryServerInterceptor()(canceled, nil, info, handler)
	if status.Code(err) != codes.Canceled {
		t.Fatal("cancellation was not preserved:", err)
	}
}

type testServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *testServerStream) Context() context.Context {
	return s.ctx
}
