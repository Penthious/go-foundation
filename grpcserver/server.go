// Package grpcserver serves generated gRPC services with GOF observability,
// optional authentication, health checks, limits and lifecycle management.
package grpcserver

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"github.com/Penthious/go-foundation/logger"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	reflectionalpha "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
)

// Listener lifecycle errors.
var (
	ErrClosed     = errors.New("grpcserver: server is closed")
	ErrNotStarted = errors.New("grpcserver: server is not started")
)

// Server owns a listener and its gRPC runtime. Construct with New and stop it
// before closing dependencies used by handlers. The raw gRPC server stays private.
type Server struct {
	cfg           Config
	grpc          *grpc.Server
	registrations []registration
	methods       map[string]bool
	services      map[string]bool
	public        map[string]bool
	mu            sync.Mutex
	listener      net.Listener
	closed        bool
	serveErr      error
	done          chan struct{}
	doneOnce      sync.Once
	stopOnce      sync.Once
	forceOnce     sync.Once
	draining      chan struct{}
	stopped       chan struct{}
	ready         atomic.Bool
}

// New validates/copies config and registers handlers without opening a listener.
// Initialize telemetry first. Registrations must call generated RegisterXServer
// functions using the supplied registrar, without retaining it or starting work.
func New(cfg Config) (*Server, error) {
	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}

	registrations, err := collect(cfg.Register)
	if err != nil {
		return nil, err
	}

	s := &Server{cfg: cfg, registrations: registrations, methods: make(map[string]bool), services: map[string]bool{"": true}, public: make(map[string]bool), done: make(chan struct{}), draining: make(chan struct{}), stopped: make(chan struct{})}
	descriptors := []grpc.ServiceDesc{healthpb.Health_ServiceDesc}
	for _, registration := range registrations {
		descriptors = append(descriptors, registration.desc)
	}

	if cfg.Reflection {
		descriptors = append(descriptors, reflectionv1.ServerReflection_ServiceDesc, reflectionalpha.ServerReflection_ServiceDesc)
	}

	for _, descriptor := range descriptors {
		s.services[descriptor.ServiceName] = true
		for _, method := range descriptor.Methods {
			s.methods["/"+descriptor.ServiceName+"/"+method.MethodName] = true
		}

		for _, method := range descriptor.Streams {
			s.methods["/"+descriptor.ServiceName+"/"+method.StreamName] = true
		}
	}

	if err := s.configurePolicies(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Server) newRuntime() *grpc.Server {
	cfg := s.cfg
	options := []grpc.ServerOption{
		grpc.StatsHandler(newStats(cfg.Logger, s.methods)),
		grpc.ChainUnaryInterceptor(s.unary), grpc.ChainStreamInterceptor(s.stream),
		grpc.MaxRecvMsgSize(cfg.MaxRecvMessageBytes), grpc.MaxSendMsgSize(cfg.MaxSendMessageBytes),
		grpc.MaxHeaderListSize(cfg.MaxHeaderBytes), grpc.MaxConcurrentStreams(cfg.MaxConcurrentStreams),
		grpc.ConnectionTimeout(cfg.ConnectionTimeout),
	}
	if cfg.TLS != nil {
		options = append(options, grpc.Creds(credentials.NewTLS(cfg.TLS)))
	}

	runtime := grpc.NewServer(options...)
	for _, registration := range s.registrations {
		runtime.RegisterService(&registration.desc, registration.implementation)
	}

	healthpb.RegisterHealthServer(runtime, &probeServer{server: s})
	if cfg.Reflection {
		reflection.Register(runtime)
	}

	return runtime
}

// Start binds synchronously, then serves in the background. It is idempotent
// after success. Call Wait to observe unexpected serving failures.
func (s *Server) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}

	if s.listener != nil {
		return nil
	}

	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", s.cfg.Address)
	if err != nil {
		return err
	}

	s.listener = listener
	s.grpc = s.newRuntime()
	s.ready.Store(true)
	go func() {
		err := s.grpc.Serve(listener)
		if errors.Is(err, grpc.ErrServerStopped) {
			err = nil
		}

		s.mu.Lock()
		s.serveErr, s.closed = err, true
		s.ready.Store(false)
		s.mu.Unlock()
		if err != nil {
			s.cfg.Logger.Error(context.Background(), "grpc server failed", logger.Err(err))
		}

		s.doneOnce.Do(func() { close(s.done) })
	}()
	s.cfg.Logger.Info(ctx, "grpc server started", logger.String("address", listener.Addr().String()))

	return nil
}

// Address returns the actual bound address after Start, otherwise the config value.
func (s *Server) Address() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listener != nil {
		return s.listener.Addr().String()
	}

	return s.cfg.Address
}

// Wait waits for the listener to stop, returning nil on normal shutdown. It does
// not join noncooperative handlers; successful Shutdown joins drained handlers.
func (s *Server) Wait() error {
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.serveErr
}

// Ping reports listener readiness for a shared health.Checker/Health UI. It does
// not call configured dependencies again, avoiding recursive readiness checks.
func (s *Server) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return ErrClosed
	}

	if !s.ready.Load() {
		return ErrNotStarted
	}

	return nil
}

// Shutdown marks probes unready, ends health watches and drains active RPCs. On
// deadline it force-closes connections, canceling handler contexts. Handlers
// must return; dependencies may still be in use after a shutdown timeout. Call
// Shutdown again to join them once they return. A closed server cannot restart.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.ready.Store(false)
	if s.listener == nil {
		s.doneOnce.Do(func() { close(s.done) })
	}

	s.mu.Unlock()
	s.stopOnce.Do(func() {
		close(s.draining)
		if s.grpc == nil {
			close(s.stopped)
			return
		}

		go func() {
			s.grpc.GracefulStop()
			close(s.stopped)
		}()
	})

	select {
	case <-s.stopped:
		return nil
	default:
	}

	select {
	case <-s.stopped:
		return nil
	case <-ctx.Done():
		if s.grpc != nil {
			s.forceOnce.Do(func() { go s.grpc.Stop() })
		}

		return ctx.Err()
	}
}
