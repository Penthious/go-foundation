package grpcserver

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/auth"
	"github.com/Penthious/go-foundation/ratelimit"

	jose "github.com/go-jose/go-jose/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestInterceptors_APIKeyPoliciesAndStreamIdentity(t *testing.T) {
	secret := strings.Repeat("k", 32)
	gate, err := auth.New(auth.Config{APIKeys: []auth.APIKey{{ID: "billing", Secret: secret, Scopes: []string{"read"}}}})
	if err != nil {
		t.Fatal(err)
	}

	service := &testService{unary: func(ctx context.Context, _ *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		principal, ok := auth.FromContext(ctx)
		if !ok || principal.Subject != "billing" {
			t.Error("unary handler did not receive principal")
		}

		return &testpb.SimpleResponse{}, nil
	}, bidi: func(stream grpc.BidiStreamingServer[testpb.StreamingOutputCallRequest, testpb.StreamingOutputCallResponse]) error {
		principal, ok := auth.FromContext(stream.Context())
		if !ok || principal.Subject != "billing" {
			t.Error("stream handler did not receive principal")
		}

		return stream.Send(&testpb.StreamingOutputCallResponse{})
	}}
	cfg := testConfig(t, service)
	cfg.Auth = gate
	cfg.PublicMethods = []string{testpb.TestService_EmptyCall_FullMethodName}
	cfg.RequireScopes = map[string][]string{
		testpb.TestService_UnaryCall_FullMethodName:           {"read"},
		testpb.TestService_StreamingOutputCall_FullMethodName: {"write"},
	}
	server := startServer(t, cfg)
	connection := connect(t, server)
	client := testpb.NewTestServiceClient(connection)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	for _, tt := range []struct {
		name     string
		metadata metadata.MD
		code     codes.Code
	}{
		{"missing", nil, codes.Unauthenticated},
		{"invalid", metadata.Pairs("x-api-key", "wrong"), codes.Unauthenticated},
		{"valid", metadata.Pairs("x-api-key", secret), codes.OK},
		{"duplicate", metadata.Pairs("x-api-key", secret, "x-api-key", secret), codes.Unauthenticated},
		{"mixed", metadata.Pairs("x-api-key", secret, "authorization", "Bearer ignored"), codes.Unauthenticated},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := client.UnaryCall(metadata.NewOutgoingContext(ctx, tt.metadata), &testpb.SimpleRequest{})

			if status.Code(err) != tt.code {
				t.Fatalf("authentication status = %v, want %v", err, tt.code)
			}
		})
	}

	if _, err := client.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatal("explicit public method was protected")
	}

	if _, err := healthpb.NewHealthClient(connection).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal("public health probe was protected")
	}

	if _, err := healthpb.NewHealthClient(connection).List(ctx, &healthpb.HealthListRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatal("health service listing bypassed authentication")
	}

	authenticated := metadata.NewOutgoingContext(ctx, metadata.Pairs("x-api-key", secret))
	stream, err := client.FullDuplexCall(authenticated)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := stream.Recv(); err != nil {
		t.Fatal("authenticated stream failed:", err)
	}

	denied, err := client.StreamingOutputCall(authenticated, &testpb.StreamingOutputCallRequest{})
	if err == nil {
		_, err = denied.Recv()
	}

	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("scope check failed for stream: %v", err)
	}
}

func TestInterceptors_RateLimitAuthScopeAndProbeOrdering(t *testing.T) {
	secret := strings.Repeat("k", 32)
	gate, err := auth.New(auth.Config{APIKeys: []auth.APIKey{{ID: "caller", Secret: secret, Scopes: []string{"read"}}}})
	if err != nil {
		t.Fatal(err)
	}

	cfg := testConfig(t, &testService{})
	limiter, err := ratelimit.New(ratelimit.Config{Namespace: "grpc-ordering", Logger: cfg.Logger, Limit: 1, Window: time.Hour, Burst: 1})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = limiter.Close() })
	cfg.Auth, cfg.RateLimiter = gate, limiter
	cfg.PublicMethods = []string{testpb.TestService_EmptyCall_FullMethodName}
	cfg.RequireScopes = map[string][]string{testpb.TestService_StreamingOutputCall_FullMethodName: {"write"}}
	server := startServer(t, cfg)
	connection := connect(t, server)
	client := testpb.NewTestServiceClient(connection)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	if _, err := client.UnaryCall(ctx, &testpb.SimpleRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatal("rate limiter ran before authentication:", err)
	}

	authenticated := metadata.NewOutgoingContext(ctx, metadata.Pairs("x-api-key", secret))
	denied, err := client.StreamingOutputCall(authenticated, &testpb.StreamingOutputCallRequest{})
	if err == nil {
		_, err = denied.Recv()
	}

	if status.Code(err) != codes.PermissionDenied {
		t.Fatal("rate limiter ran before scope checks:", err)
	}

	if _, err := client.UnaryCall(authenticated, &testpb.SimpleRequest{}); err != nil {
		t.Fatal("auth/scope failures consumed quota:", err)
	}

	if _, err := client.UnaryCall(authenticated, &testpb.SimpleRequest{}); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("authenticated RPC was not rate limited:", err)
	}

	for range 2 {
		if _, err := client.EmptyCall(ctx, &testpb.Empty{}); err != nil {
			t.Fatal("public method was rate limited:", err)
		}

		if _, err := healthpb.NewHealthClient(connection).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
			t.Fatal("health check was rate limited:", err)
		}

		if _, err := healthpb.NewHealthClient(connection).List(authenticated, &healthpb.HealthListRequest{}); err != nil {
			t.Fatal("authenticated health listing consumed quota:", err)
		}
	}

	if err := limiter.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := client.UnaryCall(authenticated, &testpb.SimpleRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatal("limiter outage did not fail closed:", err)
	}
}

func TestInterceptors_RateLimitChargesStreamOnce(t *testing.T) {
	secret := strings.Repeat("k", 32)
	gate, err := auth.New(auth.Config{APIKeys: []auth.APIKey{{ID: "caller", Secret: secret}}})
	if err != nil {
		t.Fatal(err)
	}

	cfg := testConfig(t, &testService{})
	limiter, err := ratelimit.New(ratelimit.Config{Namespace: "grpc-stream", Logger: cfg.Logger, Limit: 1, Window: time.Hour, Burst: 1})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = limiter.Close() })
	cfg.Auth, cfg.RateLimiter = gate, limiter
	server := startServer(t, cfg)
	client := testpb.NewTestServiceClient(connect(t, server))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("x-api-key", secret))
	stream, err := client.FullDuplexCall(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for range 3 {
		if err := stream.Send(&testpb.StreamingOutputCallRequest{}); err != nil {
			t.Fatal(err)
		}

		if _, err := stream.Recv(); err != nil {
			t.Fatal("stream messages consumed more tokens:", err)
		}
	}

	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}

	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}

	if _, err := client.EmptyCall(ctx, &testpb.Empty{}); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("stream did not share the unary quota:", err)
	}
}

func TestInterceptors_JWTMetadata(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	keys := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: "test", Algorithm: "ES256", Use: "sig"}}})
	}))
	defer keys.Close()

	gate, err := auth.New(auth.Config{JWT: &auth.JWTConfig{Issuer: keys.URL, Audience: "rpc-api", JWKSURL: keys.URL, Algorithms: []string{"ES256"}, HTTPClient: keys.Client()}})
	if err != nil {
		t.Fatal(err)
	}

	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.ES256, Key: jose.JSONWebKey{Key: key, KeyID: "test"}}, (&jose.SignerOptions{}).WithType("at+jwt"))
	if err != nil {
		t.Fatal(err)
	}

	claims, err := json.Marshal(map[string]any{"iss": keys.URL, "aud": "rpc-api", "sub": "alice", "exp": time.Now().Add(time.Hour).Unix(), "scope": "orders:read"})
	if err != nil {
		t.Fatal(err)
	}

	signed, err := signer.Sign(claims)
	if err != nil {
		t.Fatal(err)
	}

	token, err := signed.CompactSerialize()
	if err != nil {
		t.Fatal(err)
	}

	cfg := testConfig(t, &testService{unary: func(ctx context.Context, _ *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		principal, ok := auth.FromContext(ctx)
		if !ok || principal.Subject != "alice" || principal.Method != "jwt" || principal.Issuer != keys.URL {
			t.Error("JWT principal not propagated")
		}

		return &testpb.SimpleResponse{}, nil
	}})
	cfg.Auth = gate
	cfg.RequireScopes = map[string][]string{testpb.TestService_UnaryCall_FullMethodName: {"orders:read"}}
	server := startServer(t, cfg)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	_, err = testpb.NewTestServiceClient(connect(t, server)).UnaryCall(metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+token)), &testpb.SimpleRequest{})

	if err != nil {
		t.Fatal("JWT RPC failed:", err)
	}
}

func TestInterceptors_ErrorMappingAndPanicRecovery(t *testing.T) {
	for _, tt := range []struct {
		name string
		code codes.Code
		fn   func() error
	}{
		{"panic", codes.Internal, func() error { panic("sensitive panic") }},
		{"nil panic", codes.Internal, func() error { panic(nil) }},
		{"plain error", codes.Internal, func() error { return errors.New("sensitive error") }},
		{"explicit status", codes.InvalidArgument, func() error { return status.Error(codes.InvalidArgument, "invalid order") }},
		{"canceled", codes.Canceled, func() error { return context.Canceled }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(t, &testService{unary: func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
				return nil, tt.fn()
			}, bidi: func(grpc.BidiStreamingServer[testpb.StreamingOutputCallRequest, testpb.StreamingOutputCallResponse]) error {
				return tt.fn()
			}})
			server := startServer(t, cfg)
			client := testpb.NewTestServiceClient(connect(t, server))
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()

			_, unaryErr := client.UnaryCall(ctx, &testpb.SimpleRequest{})
			stream, err := client.FullDuplexCall(ctx)
			if err != nil {
				t.Fatal(err)
			}

			_, streamErr := stream.Recv()
			for _, err := range []error{unaryErr, streamErr} {
				if status.Code(err) != tt.code || strings.Contains(err.Error(), "sensitive") {
					t.Fatalf("unsafe/wrong RPC error: %v", err)
				}
			}

			if _, err := client.EmptyCall(ctx, &testpb.Empty{}); err != nil {
				t.Fatal("server stopped after failed handler")
			}
		})
	}
}

func TestInterceptors_DeadlinesAndStreamingCancellation(t *testing.T) {
	finished := make(chan struct{}, 1)
	cfg := testConfig(t, &testService{unary: func(ctx context.Context, _ *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		<-ctx.Done()
		finished <- struct{}{}

		return nil, ctx.Err()
	}})
	cfg.UnaryTimeout = 30 * time.Millisecond
	server := startServer(t, cfg)
	client := testpb.NewTestServiceClient(connect(t, server))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	if _, err := client.UnaryCall(ctx, &testpb.SimpleRequest{}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("default unary deadline not applied: %v", err)
	}

	_ = receive(t, finished)
	streamCtx, stop := context.WithCancel(ctx)
	stream, err := client.FullDuplexCall(streamCtx)
	if err != nil {
		t.Fatal(err)
	}

	stop()
	if _, err := stream.Recv(); status.Code(err) != codes.Canceled {
		t.Fatalf("stream cancellation lost: %v", err)
	}
}
