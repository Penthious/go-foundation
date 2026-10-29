package grpcclient

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/auth"
	"github.com/Penthious/go-foundation/grpcserver"
	"github.com/Penthious/go-foundation/ratelimit"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestCircuitBreaker_DefaultsAndDisabled(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		var calls, lookups atomic.Int32
		server := testServer(t, &testService{unary: func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
			calls.Add(1)

			return nil, status.Error(codes.Unavailable, "upstream down")
		}}, nil)
		cfg := testConfig(t, server.Address())
		cfg.TokenProvider = func(context.Context) (string, error) {
			lookups.Add(1)

			return "token", nil
		}
		if enabled {
			cfg.CircuitBreaker = &CircuitBreakerConfig{}
		}

		client := testClient(t, cfg)
		generated := testpb.NewTestServiceClient(client)
		for i := range 7 {
			_, err := generated.UnaryCall(t.Context(), &testpb.SimpleRequest{})
			if status.Code(err) != codes.Unavailable || errors.Is(err, ErrCircuitOpen) != (enabled && i >= 5) {
				t.Fatal("unexpected failure or circuit state:", err)
			}
		}

		want := int32(7)
		if enabled {
			want = 5
		}

		if calls.Load() != want || lookups.Load() != want {
			t.Fatal("open circuit performed an RPC or credential lookup")
		}

		if err := client.Ping(t.Context()); err != nil {
			t.Fatal("open circuit prevented a transport health probe:", err)
		}

		if err := client.Close(); err != nil {
			t.Fatal(err)
		}

		if _, err := generated.EmptyCall(t.Context(), &testpb.Empty{}); !errors.Is(err, ErrClosed) {
			t.Fatal("open circuit hid client closure:", err)
		}
	}
}

func TestCircuitBreaker_StatusPolicy(t *testing.T) {
	for _, code := range []codes.Code{codes.OK, codes.Canceled, codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.PermissionDenied, codes.Unauthenticated, codes.FailedPrecondition, codes.Aborted, codes.OutOfRange, codes.Unimplemented, codes.Unknown, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Internal, codes.Unavailable, codes.DataLoss} {
		t.Run(code.String(), func(t *testing.T) {
			server := testServer(t, &testService{unary: func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
				return &testpb.SimpleResponse{}, status.Error(code, "upstream result")
			}}, nil)
			cfg := testConfig(t, server.Address())
			cfg.CircuitBreaker = &CircuitBreakerConfig{FailureThreshold: 1, OpenTimeout: time.Hour}
			client := testClient(t, cfg)
			generated := testpb.NewTestServiceClient(client)

			if _, err := generated.UnaryCall(t.Context(), &testpb.SimpleRequest{}); status.Code(err) != code {
				t.Fatal("breaker replaced the triggering RPC status:", err)
			}

			_, err := generated.EmptyCall(t.Context(), &testpb.Empty{})
			failure := code == codes.Unknown || code == codes.DeadlineExceeded || code == codes.Internal || code == codes.Unavailable || code == codes.DataLoss
			if failure {
				if !errors.Is(err, ErrCircuitOpen) || status.Code(err) != codes.Unavailable {
					t.Fatal("availability failure did not open circuit:", err)
				}
			} else if err != nil {
				t.Fatal("application result opened the circuit:", err)
			}
		})
	}
}

func TestCircuitBreaker_QuotaDoesNotBlockOtherUsers(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		name := "unary"
		if streaming {
			name = "stream"
		}

		t.Run(name, func(t *testing.T) {
			alice, bob := strings.Repeat("a", 32), strings.Repeat("b", 32)
			gate, err := auth.New(auth.Config{APIKeys: []auth.APIKey{{ID: "alice", Secret: alice}, {ID: "bob", Secret: bob}}})
			if err != nil {
				t.Fatal(err)
			}

			limiter, err := ratelimit.New(ratelimit.Config{Namespace: "breaker-test", Logger: testConfig(t, "unused:9090").Logger,
				Limit: 1, Window: time.Hour, Burst: 1})
			if err != nil {
				t.Fatal(err)
			}

			t.Cleanup(func() { _ = limiter.Close() })
			server := testServer(t, &testService{}, func(cfg *grpcserver.Config) {
				cfg.Auth, cfg.RateLimiter = gate, limiter
			})
			cfg := testConfig(t, server.Address())
			cfg.CircuitBreaker = &CircuitBreakerConfig{FailureThreshold: 2, OpenTimeout: time.Hour}
			generated := testpb.NewTestServiceClient(testClient(t, cfg))
			ctx := metadata.AppendToOutgoingContext(t.Context(), "x-api-key", alice)

			if _, err := generated.EmptyCall(ctx, &testpb.Empty{}); err != nil {
				t.Fatal(err)
			}

			for range 3 {
				var err error
				if streaming {
					var stream grpc.BidiStreamingClient[testpb.StreamingOutputCallRequest, testpb.StreamingOutputCallResponse]
					stream, err = generated.FullDuplexCall(ctx)
					if err == nil {
						_, err = stream.Recv()
					}
				} else {
					_, err = generated.EmptyCall(ctx, &testpb.Empty{})
				}

				if status.Code(err) != codes.ResourceExhausted {
					t.Fatal("quota rejection was replaced by a circuit failure:", err)
				}

				details := status.Convert(err).Details()
				if len(details) != 1 {
					t.Fatal("quota response lost its retry guidance")
				}

				if retry, ok := details[0].(*errdetails.RetryInfo); !ok || retry.RetryDelay.AsDuration() <= 0 {
					t.Fatal("quota response lost its retry delay")
				}
			}

			other := metadata.AppendToOutgoingContext(t.Context(), "x-api-key", bob)
			if _, err := generated.EmptyCall(other, &testpb.Empty{}); err != nil {
				t.Fatal("one user's quota blocked another user:", err)
			}
		})
	}
}

func TestCircuitBreaker_QuotaPreservesAvailabilityFailures(t *testing.T) {
	var calls atomic.Int32
	server := testServer(t, &testService{unary: func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		if calls.Add(1) == 2 {
			return nil, status.Error(codes.ResourceExhausted, "quota exceeded")
		}

		return nil, status.Error(codes.Unavailable, "upstream down")
	}}, nil)
	cfg := testConfig(t, server.Address())
	cfg.CircuitBreaker = &CircuitBreakerConfig{FailureThreshold: 2, OpenTimeout: time.Hour}
	generated := testpb.NewTestServiceClient(testClient(t, cfg))

	for _, code := range []codes.Code{codes.Unavailable, codes.ResourceExhausted, codes.Unavailable} {
		_, err := generated.UnaryCall(t.Context(), &testpb.SimpleRequest{})
		if status.Code(err) != code || errors.Is(err, ErrCircuitOpen) {
			t.Fatal("quota rejection counted as an availability failure:", err)
		}
	}

	if _, err := generated.EmptyCall(t.Context(), &testpb.Empty{}); !errors.Is(err, ErrCircuitOpen) {
		t.Fatal("quota rejection reset earlier availability failures:", err)
	}
}

func TestCircuitBreaker_LocalFailuresAndDeadlines(t *testing.T) {
	for _, scenario := range []string{"invalid method", "nil stream descriptor", "auth error", "auth timeout", "canceled caller", "upstream timeout"} {
		t.Run(scenario, func(t *testing.T) {
			server := testServer(t, &testService{unary: func(ctx context.Context, _ *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
				<-ctx.Done()

				return nil, ctx.Err()
			}}, nil)
			cfg := testConfig(t, server.Address())
			cfg.Timeout = 50 * time.Millisecond
			cfg.CircuitBreaker = &CircuitBreakerConfig{FailureThreshold: 1, OpenTimeout: time.Hour}
			var first atomic.Bool
			if scenario == "auth error" || scenario == "auth timeout" {
				cfg.TokenProvider = func(ctx context.Context) (string, error) {
					if first.CompareAndSwap(false, true) {
						if scenario == "auth timeout" {
							<-ctx.Done()
						}

						return "", errors.New("provider unavailable")
					}

					return "token", nil
				}
			}

			client := testClient(t, cfg)
			if err := client.Ping(t.Context()); err != nil {
				t.Fatal(err)
			}

			generated := testpb.NewTestServiceClient(client)
			var err error
			switch scenario {
			case "invalid method":
				err = client.Invoke(t.Context(), "invalid", &testpb.Empty{}, &testpb.Empty{})
			case "nil stream descriptor":
				_, err = client.NewStream(t.Context(), nil, "/grpc.testing.TestService/FullDuplexCall")
			case "canceled caller":
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				_, err = generated.EmptyCall(ctx, &testpb.Empty{})
			case "upstream timeout":
				_, err = generated.UnaryCall(t.Context(), &testpb.SimpleRequest{})
			default:
				_, err = generated.EmptyCall(t.Context(), &testpb.Empty{})
			}

			if err == nil {
				t.Fatal("expected the initial operation to fail")
			}

			_, err = generated.EmptyCall(t.Context(), &testpb.Empty{})
			if scenario == "upstream timeout" {
				if !errors.Is(err, ErrCircuitOpen) {
					t.Fatal("RPC timeout did not open circuit:", err)
				}
			} else if err != nil {
				t.Fatal("local failure opened circuit:", err)
			}
		})
	}
}

func TestCircuitBreaker_StreamFailuresTripAtCompletion(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded} {
		t.Run(code.String(), func(t *testing.T) {
			server := testServer(t, &testService{bidi: func(stream grpc.BidiStreamingServer[testpb.StreamingOutputCallRequest, testpb.StreamingOutputCallResponse]) error {
				if code == codes.DeadlineExceeded {
					<-stream.Context().Done()

					return stream.Context().Err()
				}

				return status.Error(codes.Unavailable, "upstream down")
			}}, nil)
			cfg := testConfig(t, server.Address())
			cfg.StreamTimeout = 50 * time.Millisecond
			cfg.CircuitBreaker = &CircuitBreakerConfig{FailureThreshold: 1, OpenTimeout: time.Hour}
			client := testClient(t, cfg)
			generated := testpb.NewTestServiceClient(client)

			stream, err := generated.FullDuplexCall(t.Context())
			if err != nil {
				t.Fatal(err)
			}

			if _, err := stream.Recv(); status.Code(err) != code {
				t.Fatal("stream failure was not preserved:", err)
			}

			if _, err := generated.EmptyCall(t.Context(), &testpb.Empty{}); !errors.Is(err, ErrCircuitOpen) {
				t.Fatal("completed stream failure did not open circuit:", err)
			}

			if _, err := generated.FullDuplexCall(t.Context()); !errors.Is(err, ErrCircuitOpen) {
				t.Fatal("open circuit admitted a new stream:", err)
			}
		})
	}
}

func TestCircuitBreaker_RecoveryProbeWaitsForWholeStream(t *testing.T) {
	for _, completion := range []string{"success", "cancel"} {
		t.Run(completion, func(t *testing.T) {
			server := testServer(t, &testService{unary: func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
				return nil, status.Error(codes.Unavailable, "trip circuit")
			}}, nil)
			cfg := testConfig(t, server.Address())
			cfg.CircuitBreaker = &CircuitBreakerConfig{FailureThreshold: 1, OpenTimeout: time.Nanosecond}
			client := testClient(t, cfg)
			generated := testpb.NewTestServiceClient(client)
			if _, err := generated.UnaryCall(t.Context(), &testpb.SimpleRequest{}); status.Code(err) != codes.Unavailable {
				t.Fatal(err)
			}

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			stream, err := generated.FullDuplexCall(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if err := stream.Send(&testpb.StreamingOutputCallRequest{}); err != nil {
				t.Fatal(err)
			}

			if _, err := stream.Recv(); err != nil {
				t.Fatal(err)
			}

			if _, err := generated.EmptyCall(t.Context(), &testpb.Empty{}); !errors.Is(err, ErrCircuitOpen) {
				t.Fatal("stream establishment/first response prematurely closed recovery circuit:", err)
			}

			if completion == "success" {
				if err := stream.CloseSend(); err != nil {
					t.Fatal(err)
				}

				if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
					t.Fatal(err)
				}
			} else {
				cancel()
				if _, err := stream.Recv(); status.Code(err) != codes.Canceled {
					t.Fatal(err)
				}
			}

			if _, err := generated.EmptyCall(t.Context(), &testpb.Empty{}); err != nil {
				t.Fatal("completed/canceled stream did not release recovery probe:", err)
			}
		})
	}
}
