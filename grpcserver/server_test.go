package grpcserver

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/logger"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/status"
)

func TestServer_UnaryAndStreaming(t *testing.T) {
	server := startServer(t, testConfig(t, &testService{}))
	client := testpb.NewTestServiceClient(connect(t, server))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	response, err := client.UnaryCall(ctx, &testpb.SimpleRequest{Payload: &testpb.Payload{Body: []byte("hello")}})
	if err != nil || string(response.GetPayload().GetBody()) != "hello" {
		t.Fatalf("unary echo: %v, %v", response, err)
	}

	bidi, err := client.FullDuplexCall(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if err := bidi.Send(&testpb.StreamingOutputCallRequest{Payload: &testpb.Payload{Body: []byte("stream")}}); err != nil {
		t.Fatal(err)
	}

	streamed, err := bidi.Recv()
	if err != nil || string(streamed.GetPayload().GetBody()) != "stream" {
		t.Fatalf("bidirectional echo: %v, %v", streamed, err)
	}

	if err := bidi.CloseSend(); err != nil {
		t.Fatal(err)
	}

	if _, err := bidi.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("stream close: %v", err)
	}

	upload, err := client.StreamingInputCall(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if err := upload.Send(&testpb.StreamingInputCallRequest{Payload: &testpb.Payload{Body: []byte("abc")}}); err != nil {
		t.Fatal(err)
	}

	total, err := upload.CloseAndRecv()
	if err != nil || total.GetAggregatedPayloadSize() != 3 {
		t.Fatalf("client stream: %v, %v", total, err)
	}

	download, err := client.StreamingOutputCall(ctx, &testpb.StreamingOutputCallRequest{Payload: &testpb.Payload{Body: []byte("download")}})
	if err != nil {
		t.Fatal(err)
	}

	item, err := download.Recv()
	if err != nil || string(item.GetPayload().GetBody()) != "download" {
		t.Fatalf("server stream: %v, %v", item, err)
	}
}

func TestServer_LifecycleBindFailureAndUnexpectedStop(t *testing.T) {
	server := startServer(t, testConfig(t, &testService{}))
	address := server.Address()
	if err := server.Start(t.Context()); err != nil || server.Address() != address {
		t.Fatal("Start was not idempotent")
	}

	cfg := testConfig(t, &testService{})
	cfg.Address = address
	other, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := other.Start(t.Context()); err == nil || other.grpc != nil {
		t.Fatal("occupied address accepted or runtime leaked on bind failure")
	}

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := other.Start(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled startup was accepted")
	}

	if err := other.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := other.Wait(); err != nil {
		t.Fatal(err)
	}

	if err := server.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := server.Wait(); err != nil {
		t.Fatal(err)
	}

	if err := server.Start(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatal("closed server restarted")
	}

	broken := startServer(t, testConfig(t, &testService{}))
	if err := broken.listener.Close(); err != nil {
		t.Fatal(err)
	}

	if err := broken.Wait(); err == nil {
		t.Fatal("unexpected listener failure was hidden")
	}
}

func TestServer_GracefulAndForcedShutdown(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(map[bool]string{false: "graceful", true: "forced"}[force], func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			server := startServer(t, testConfig(t, &testService{unary: func(ctx context.Context, request *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
				close(entered)
				<-release

				return &testpb.SimpleResponse{}, nil
			}}))
			client := testpb.NewTestServiceClient(connect(t, server))
			result := make(chan error, 1)
			go func() {
				_, err := client.UnaryCall(t.Context(), &testpb.SimpleRequest{})
				result <- err
			}()
			_ = receive(t, entered)
			stopped := make(chan error, 1)
			ctx := t.Context()
			if force {
				deadline, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
				defer cancel()

				ctx = deadline
			}

			go func() { stopped <- server.Shutdown(ctx) }()
			_ = receive(t, server.draining)
			if err := server.Ping(t.Context()); !errors.Is(err, ErrClosed) {
				t.Fatal("draining listener remained ready")
			}

			if force {
				if err := receive(t, stopped); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("forced stop did not honor deadline: %v", err)
				}
			} else {
				select {
				case <-stopped:
					t.Fatal("graceful shutdown did not wait for active handler")
				default:
				}
			}

			close(release)
			if !force {
				if err := receive(t, result); err != nil {
					t.Fatal(err)
				}

				if err := receive(t, stopped); err != nil {
					t.Fatal(err)
				}
			} else {
				_ = receive(t, result)
			}

			if err := server.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestServer_TLS(t *testing.T) {
	fixture := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer fixture.Close()

	cfg := testConfig(t, &testService{})
	cfg.TLS = &tls.Config{Certificates: fixture.TLS.Certificates}
	server := startServer(t, cfg)
	clientTLS := fixture.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	if clientTLS.InsecureSkipVerify {
		t.Fatal("TLS fixture unexpectedly disabled certificate verification")
	}

	connection, err := grpc.NewClient("passthrough:///"+server.Address(), grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)))
	if err != nil {
		t.Fatal(err)
	}

	defer connection.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	if _, err := testpb.NewTestServiceClient(connection).EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatal("verified TLS RPC failed:", err)
	}

	if cfg.TLS.MinVersion != 0 || server.cfg.TLS.MinVersion != tls.VersionTLS12 {
		t.Fatal("TLS config was mutated or defaults not applied")
	}
}

func TestServer_MessageLimits(t *testing.T) {
	for _, send := range []bool{false, true} {
		t.Run(map[bool]string{false: "receive", true: "send"}[send], func(t *testing.T) {
			cfg := testConfig(t, &testService{})
			if send {
				cfg.MaxSendMessageBytes = 32
			} else {
				cfg.MaxRecvMessageBytes = 32
			}

			server := startServer(t, cfg)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()

			_, err := testpb.NewTestServiceClient(connect(t, server)).UnaryCall(ctx, &testpb.SimpleRequest{Payload: &testpb.Payload{Body: make([]byte, 1024)}})

			if status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("message size limit failed: %v", err)
			}
		})
	}
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

func testConfig(t *testing.T, service *testService) Config {
	t.Helper()

	log, err := logger.New(logger.Config{Service: "grpc-test", Output: io.Discard})
	if err != nil {
		t.Fatal(err)
	}

	return Config{Address: "127.0.0.1:0", Logger: log, Register: []func(grpc.ServiceRegistrar){func(r grpc.ServiceRegistrar) { testpb.RegisterTestServiceServer(r, service) }}}
}

func startServer(t *testing.T, cfg Config) *Server {
	t.Helper()

	server, err := New(cfg)
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

func connect(t *testing.T, server *Server) *grpc.ClientConn {
	t.Helper()

	connection, err := grpc.NewClient("passthrough:///"+server.Address(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = connection.Close() })

	return connection
}

func receive[T any](t *testing.T, values <-chan T) T {
	t.Helper()

	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for RPC server")
	}

	var zero T

	return zero
}

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buffer.Write(value)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buffer.String()
}
