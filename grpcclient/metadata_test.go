package grpcclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/auth"
	"github.com/Penthious/go-foundation/grpcserver"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestClient_APIKeyAndCallerMetadata(t *testing.T) {
	secret := strings.Repeat("k", 32)
	gate, err := auth.New(auth.Config{APIKeys: []auth.APIKey{{ID: "billing", Secret: secret, Scopes: []string{"read"}}}})
	if err != nil {
		t.Fatal(err)
	}

	server := testServer(t, &testService{unary: func(ctx context.Context, _ *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		principal, ok := auth.FromContext(ctx)
		values, _ := metadata.FromIncomingContext(ctx)
		if !ok || principal.Subject != "billing" || values.Get("x-request-id")[0] != "request-1" {
			t.Error("auth identity or service metadata not received")
		}

		return &testpb.SimpleResponse{}, nil
	}}, func(c *grpcserver.Config) {
		c.Auth = gate
		c.RequireScopes = map[string][]string{testpb.TestService_UnaryCall_FullMethodName: {"read"}}
	})
	cfg := testConfig(t, server.Address())
	cfg.APIKey = secret
	client := testClient(t, cfg)
	values := metadata.Pairs("x-request-id", "request-1")
	ctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(t.Context(), values), 3*time.Second)
	defer cancel()

	_, err = testpb.NewTestServiceClient(client).UnaryCall(ctx, &testpb.SimpleRequest{})
	if err != nil {
		t.Fatal(err)
	}

	if len(values.Get("x-api-key")) != 0 || len(values.Get("authorization")) != 0 {
		t.Fatal("caller metadata was modified")
	}

	for _, supplied := range []metadata.MD{
		metadata.Pairs("x-api-key", secret),
		metadata.Pairs("authorization", "Bearer caller-token"),
		metadata.Pairs("x-api-key", secret, "authorization", "Bearer caller-token"),
	} {
		_, err := testpb.NewTestServiceClient(client).UnaryCall(metadata.NewOutgoingContext(ctx, supplied), &testpb.SimpleRequest{})

		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("conflicting credentials accepted: %v", err)
		}
	}
}

func TestClient_TokenProviderRotationAndStreaming(t *testing.T) {
	var sequence atomic.Int64
	seen := make(chan string, 20)
	server := testServer(t, &testService{unary: func(ctx context.Context, _ *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		values, _ := metadata.FromIncomingContext(ctx)
		seen <- values.Get("authorization")[0]

		return &testpb.SimpleResponse{}, nil
	}, bidi: func(stream grpc.BidiStreamingServer[testpb.StreamingOutputCallRequest, testpb.StreamingOutputCallResponse]) error {
		values, _ := metadata.FromIncomingContext(stream.Context())
		seen <- values.Get("authorization")[0]

		return stream.Send(&testpb.StreamingOutputCallResponse{})
	}}, nil)
	cfg := testConfig(t, server.Address())
	cfg.TokenProvider = func(ctx context.Context) (string, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("token provider had no deadline")
		}

		return fmt.Sprintf("token-%d", sequence.Add(1)), nil
	}
	client := testClient(t, cfg)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	for i := 1; i <= 2; i++ {
		if _, err := testpb.NewTestServiceClient(client).UnaryCall(ctx, &testpb.SimpleRequest{}); err != nil {
			t.Fatal(err)
		}

		if token := receive(t, seen); token != fmt.Sprintf("Bearer token-%d", i) {
			t.Fatalf("provider did not refresh: %s", token)
		}
	}

	stream, err := testpb.NewTestServiceClient(client).FullDuplexCall(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}

	if token := receive(t, seen); token != "Bearer token-3" {
		t.Fatalf("stream credentials not set: %s", token)
	}

	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("credentialed stream did not finish: %v", err)
	}

	var calls sync.WaitGroup
	for range 10 {
		calls.Go(func() {
			if _, err := testpb.NewTestServiceClient(client).UnaryCall(ctx, &testpb.SimpleRequest{}); err != nil {
				t.Error(err)
			}
		})
	}

	calls.Wait()
	if sequence.Load() != 13 {
		t.Fatalf("unexpected provider invocations: %d", sequence.Load())
	}
}

func TestClient_CallerCredentialsAndIncomingMetadataIsolation(t *testing.T) {
	seen := make(chan metadata.MD, 5)
	server := testServer(t, &testService{unary: func(ctx context.Context, _ *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		values, _ := metadata.FromIncomingContext(ctx)
		seen <- values

		return &testpb.SimpleResponse{}, nil
	}}, nil)
	client := testClient(t, testConfig(t, server.Address()))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	for _, values := range []metadata.MD{metadata.Pairs("authorization", "Bearer caller-token"), metadata.Pairs("x-api-key", "caller-key")} {
		if _, err := testpb.NewTestServiceClient(client).UnaryCall(metadata.NewOutgoingContext(ctx, values), &testpb.SimpleRequest{}); err != nil {
			t.Fatal(err)
		}

		received := receive(t, seen)
		for key, entries := range values {
			if len(received.Get(key)) != 1 || received.Get(key)[0] != entries[0] {
				t.Fatal("explicit caller credential lost")
			}
		}
	}

	incoming := metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer incoming-user-secret", "x-private-header", "incoming-only"))
	if _, err := testpb.NewTestServiceClient(client).UnaryCall(incoming, &testpb.SimpleRequest{}); err != nil {
		t.Fatal(err)
	}

	values := receive(t, seen)
	if len(values.Get("authorization")) != 0 || len(values.Get("x-private-header")) != 0 {
		t.Fatal("incoming credentials/metadata were forwarded implicitly")
	}

	for _, values := range []metadata.MD{
		metadata.Pairs("authorization", "Basic secret"),
		metadata.Pairs("authorization", "Bearer one", "authorization", "Bearer two"),
		metadata.Pairs("x-api-key", "one", "authorization", "Bearer two"),
		metadata.Pairs("x-api-key", ""),
	} {
		_, err := testpb.NewTestServiceClient(client).UnaryCall(metadata.NewOutgoingContext(ctx, values), &testpb.SimpleRequest{})

		if status.Code(err) != codes.Unauthenticated {
			t.Fatal("invalid caller credentials accepted")
		}
	}
}

func TestClient_CredentialErrorsTimeoutAndClose(t *testing.T) {
	for _, scenario := range []string{"error", "panic", "nil panic", "empty", "invalid", "timeout", "close"} {
		t.Run(scenario, func(t *testing.T) {
			entered := make(chan struct{})
			cfg := testConfig(t, "127.0.0.1:1")
			cfg.Timeout = 30 * time.Millisecond
			if scenario == "close" {
				cfg.Timeout = 3 * time.Second
			}

			cfg.TokenProvider = func(ctx context.Context) (string, error) {
				switch scenario {
				case "error":
					return "", errors.New("sensitive credential failure")
				case "panic":
					panic("sensitive credential panic")
				case "nil panic":
					panic(nil)
				case "invalid":
					return "bad token\n", nil
				case "timeout", "close":
					close(entered)
					<-ctx.Done()
					return "", ctx.Err()
				}

				return "", nil
			}
			client := testClient(t, cfg)
			result := make(chan error, 1)
			go func() {
				_, err := testpb.NewTestServiceClient(client).UnaryCall(t.Context(), &testpb.SimpleRequest{})
				result <- err
			}()
			if scenario == "close" {
				_ = receive(t, entered)
				if err := client.Close(); err != nil {
					t.Fatal(err)
				}
			}

			err := receive(t, result)
			want := codes.Unauthenticated
			if scenario == "timeout" {
				want = codes.DeadlineExceeded
			} else if scenario == "close" {
				want = codes.Unavailable
			}

			if status.Code(err) != want || strings.Contains(err.Error(), "sensitive") {
				t.Fatalf("unsafe/wrong credential failure: %v", err)
			}
		})
	}
}
