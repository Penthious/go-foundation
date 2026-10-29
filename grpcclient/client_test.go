package grpcclient

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/grpcserver"
	"github.com/Penthious/go-foundation/logger"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestClient_GeneratedUnaryAndStreamingCalls(t *testing.T) {
	server := testServer(t, &testService{}, nil)
	cfg := testConfig(t, server.Address())
	cfg.CircuitBreaker = &CircuitBreakerConfig{}
	client := testClient(t, cfg)
	generated := testpb.NewTestServiceClient(client)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	if err := client.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	response, err := generated.UnaryCall(ctx, &testpb.SimpleRequest{Payload: &testpb.Payload{Body: []byte("unary")}})
	if err != nil || string(response.GetPayload().GetBody()) != "unary" {
		t.Fatalf("generated unary: %v, %v", response, err)
	}

	bidi, err := generated.FullDuplexCall(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if err := bidi.Send(&testpb.StreamingOutputCallRequest{Payload: &testpb.Payload{Body: []byte("bidi")}}); err != nil {
		t.Fatal(err)
	}

	item, err := bidi.Recv()
	if err != nil || string(item.GetPayload().GetBody()) != "bidi" {
		t.Fatalf("generated bidi: %v, %v", item, err)
	}

	if err := bidi.CloseSend(); err != nil {
		t.Fatal(err)
	}

	if _, err := bidi.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("bidi completion: %v", err)
	}

	if !errors.Is(bidi.Context().Err(), context.Canceled) {
		t.Fatal("completed stream retained its context timer")
	}

	upload, err := generated.StreamingInputCall(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if err := upload.Send(&testpb.StreamingInputCallRequest{Payload: &testpb.Payload{Body: []byte("123")}}); err != nil {
		t.Fatal(err)
	}

	total, err := upload.CloseAndRecv()
	if err != nil || total.GetAggregatedPayloadSize() != 3 {
		t.Fatalf("generated upload: %v, %v", total, err)
	}

	download, err := generated.StreamingOutputCall(ctx, &testpb.StreamingOutputCallRequest{Payload: &testpb.Payload{Body: []byte("download")}})
	if err != nil {
		t.Fatal(err)
	}

	item, err = download.Recv()
	if err != nil || string(item.GetPayload().GetBody()) != "download" {
		t.Fatalf("generated download: %v, %v", item, err)
	}

	if _, err := download.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("download completion: %v", err)
	}

	for range 2 {
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := generated.UnaryCall(ctx, &testpb.SimpleRequest{}); !errors.Is(err, ErrClosed) || status.Code(err) != codes.Unavailable {
		t.Fatalf("closed unary: %v", err)
	}

	if _, err := generated.FullDuplexCall(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed stream: %v", err)
	}

	if err := client.Ping(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed ping: %v", err)
	}
}

func TestClient_DeadlinesBoundUnaryAndIdleStream(t *testing.T) {
	server := testServer(t, &testService{unary: func(ctx context.Context, _ *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		<-ctx.Done()

		return nil, ctx.Err()
	}}, nil)
	cfg := testConfig(t, server.Address())
	cfg.Timeout, cfg.StreamTimeout = 50*time.Millisecond, 50*time.Millisecond
	client := testClient(t, cfg)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	if err := client.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	_, err := testpb.NewTestServiceClient(client).UnaryCall(ctx, &testpb.SimpleRequest{})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("unary timeout: %v", err)
	}

	stream, err := testpb.NewTestServiceClient(client).FullDuplexCall(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := stream.Recv(); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("idle receive escaped stream deadline: %v", err)
	}

	earlier, stop := context.WithTimeout(ctx, 10*time.Millisecond)
	defer stop()

	_, err = testpb.NewTestServiceClient(client).UnaryCall(earlier, &testpb.SimpleRequest{})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("earlier deadline: %v", err)
	}
}

func TestClient_LazyDialerAndInMemoryServer(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	testpb.RegisterTestServiceServer(server, &testService{})
	serving := make(chan error, 1)
	go func() { serving <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		_ = receive(t, serving)
	})
	var dials atomic.Int64
	cfg := testConfig(t, "passthrough:///memory:9090")
	cfg.Dialer = func(ctx context.Context, _ string) (net.Conn, error) {
		dials.Add(1)

		return listener.DialContext(ctx)
	}
	client := testClient(t, cfg)
	if dials.Load() != 0 {
		t.Fatal("New performed network I/O")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	if _, err := testpb.NewTestServiceClient(client).EmptyCall(ctx, &testpb.Empty{}); err != nil || dials.Load() == 0 {
		t.Fatalf("test dialer was not used: %v", err)
	}
}

func TestClient_VerifiedTLSAndMessageLimits(t *testing.T) {
	fixture := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer fixture.Close()

	server := testServer(t, &testService{}, func(c *grpcserver.Config) {
		c.TLS = &tls.Config{Certificates: fixture.TLS.Certificates}
	})
	for _, send := range []bool{false, true} {
		t.Run(map[bool]string{false: "receive", true: "send"}[send], func(t *testing.T) {
			cfg := testConfig(t, server.Address())
			cfg.Insecure = false
			cfg.TLS = fixture.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
			if cfg.TLS.InsecureSkipVerify {
				t.Fatal("TLS fixture disabled certificate verification")
			}

			if send {
				cfg.MaxSendMessageBytes = 32
			} else {
				cfg.MaxRecvMessageBytes = 32
			}

			client := testClient(t, cfg)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()

			if _, err := testpb.NewTestServiceClient(client).EmptyCall(ctx, &testpb.Empty{}); err != nil {
				t.Fatal("verified TLS failed:", err)
			}

			_, err := testpb.NewTestServiceClient(client).UnaryCall(ctx, &testpb.SimpleRequest{Payload: &testpb.Payload{Body: make([]byte, 1024)}})
			if status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("message limit failed: %v", err)
			}
		})
	}
}

func TestClient_CloseCancelsOpenStream(t *testing.T) {
	server := testServer(t, &testService{}, nil)
	client := testClient(t, testConfig(t, server.Address()))
	stream, err := testpb.NewTestServiceClient(client).FullDuplexCall(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = stream.Recv()
	if code := status.Code(err); code != codes.Canceled && code != codes.Unavailable {
		t.Fatalf("closed channel did not cancel stream: %v", err)
	}
}

func TestClient_DisablesResolverSuppliedApplicationRetries(t *testing.T) {
	var calls atomic.Int64
	server := testServer(t, &testService{unary: func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		calls.Add(1)

		return nil, status.Error(codes.Unavailable, "operation failed")
	}}, nil)
	resolver.Register(&retryResolverBuilder{address: server.Address()})
	client := testClient(t, testConfig(t, "gof-client-retry-test:///service"))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	_, err := testpb.NewTestServiceClient(client).UnaryCall(ctx, &testpb.SimpleRequest{})

	if status.Code(err) != codes.Unavailable || calls.Load() != 1 {
		t.Fatalf("application RPC was retried despite client policy: calls=%d, err=%v", calls.Load(), err)
	}
}

type retryResolverBuilder struct {
	address string
}

func (b *retryResolverBuilder) Scheme() string {
	return "gof-client-retry-test"
}

func (b *retryResolverBuilder) Build(_ resolver.Target, connection resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	config := connection.ParseServiceConfig(`{"methodConfig":[{"name":[{"service":"grpc.testing.TestService"}],"retryPolicy":{"maxAttempts":4,"initialBackoff":"0.01s","maxBackoff":"0.01s","backoffMultiplier":1,"retryableStatusCodes":["UNAVAILABLE"]}}]}`)
	if config.Err != nil {
		return nil, config.Err
	}

	if err := connection.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: b.address}}, ServiceConfig: config}); err != nil {
		return nil, err
	}

	return retryResolver{}, nil
}

type retryResolver struct{}

func (retryResolver) ResolveNow(resolver.ResolveNowOptions) {
}

func (retryResolver) Close() {
}

type testService struct {
	testpb.UnimplementedTestServiceServer
	unary func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error)
	bidi  func(grpc.BidiStreamingServer[testpb.StreamingOutputCallRequest, testpb.StreamingOutputCallResponse]) error
}

func (s *testService) EmptyCall(context.Context, *testpb.Empty) (*testpb.Empty, error) {
	return &testpb.Empty{}, nil
}

func (s *testService) UnaryCall(ctx context.Context, request *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
	if s.unary != nil {
		return s.unary(ctx, request)
	}

	return &testpb.SimpleResponse{Payload: request.Payload}, nil
}

func (s *testService) FullDuplexCall(stream grpc.BidiStreamingServer[testpb.StreamingOutputCallRequest, testpb.StreamingOutputCallResponse]) error {
	if s.bidi != nil {
		return s.bidi(stream)
	}

	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return err
		}

		if err := stream.Send(&testpb.StreamingOutputCallResponse{Payload: request.Payload}); err != nil {
			return err
		}
	}
}

func (s *testService) StreamingInputCall(stream grpc.ClientStreamingServer[testpb.StreamingInputCallRequest, testpb.StreamingInputCallResponse]) error {
	var size int32
	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&testpb.StreamingInputCallResponse{AggregatedPayloadSize: size})
		}

		if err != nil {
			return err
		}

		size += int32(len(request.GetPayload().GetBody()))
	}
}

func (s *testService) StreamingOutputCall(request *testpb.StreamingOutputCallRequest, stream grpc.ServerStreamingServer[testpb.StreamingOutputCallResponse]) error {
	return stream.Send(&testpb.StreamingOutputCallResponse{Payload: request.Payload})
}

func testConfig(t *testing.T, target string) Config {
	t.Helper()

	log, err := logger.New(logger.Config{Service: "grpc-client-test", Output: io.Discard})
	if err != nil {
		t.Fatal(err)
	}

	return Config{Target: target, Logger: log, Insecure: true}
}

func testClient(t *testing.T, cfg Config) *Client {
	t.Helper()

	client, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = client.Close() })

	return client
}

func testServer(t *testing.T, service *testService, change func(*grpcserver.Config)) *grpcserver.Server {
	t.Helper()

	log := testConfig(t, "unused:9090").Logger
	cfg := grpcserver.Config{Logger: log, Address: "127.0.0.1:0", Register: []func(grpc.ServiceRegistrar){func(registrar grpc.ServiceRegistrar) { testpb.RegisterTestServiceServer(registrar, service) }}}
	if change != nil {
		change(&cfg)
	}

	server, err := grpcserver.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := server.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_ = server.Shutdown(ctx)
	})

	return server
}

func receive[T any](t *testing.T, values <-chan T) T {
	t.Helper()

	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for RPC client")
	}

	var zero T

	return zero
}
