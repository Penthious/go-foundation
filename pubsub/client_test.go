package pubsub_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/logger"
	"github.com/Penthious/go-foundation/pubsub"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestClient_Lifecycle(t *testing.T) {
	s := startBroker(t, nil)
	client, err := pubsub.New(pubsub.Config{Logger: newLogger(t, io.Discard), Endpoints: []string{s.ClientURL()}})
	if err != nil {
		t.Fatal(err)
	}

	message := pubsub.Message{ID: "event-1", Topic: "orders.created"}
	if err := client.Publish(t.Context(), message); !errors.Is(err, pubsub.ErrNotStarted) {
		t.Fatalf("publish before Start = %v", err)
	}

	for range 2 {
		if err := client.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
	}

	if s.NumClients() != 1 {
		t.Errorf("Start opened %d connections", s.NumClients())
	}

	for range 2 {
		if err := client.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
	}

	if err := client.Publish(t.Context(), message); !errors.Is(err, pubsub.ErrClosed) {
		t.Errorf("publish after shutdown = %v", err)
	}

	if err := client.Start(t.Context()); !errors.Is(err, pubsub.ErrClosed) {
		t.Errorf("restart after shutdown = %v", err)
	}
}

func TestClient_ShutdownBeforeStart(t *testing.T) {
	client, err := pubsub.New(pubsub.Config{Logger: newLogger(t, io.Discard)})
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := client.Start(t.Context()); !errors.Is(err, pubsub.ErrClosed) {
		t.Errorf("Start after shutdown = %v", err)
	}
}

func TestClient_PingVerifiesBroker(t *testing.T) {
	s := startBroker(t, nil)
	client, err := pubsub.New(pubsub.Config{Logger: newLogger(t, io.Discard), Endpoints: []string{s.ClientURL()}, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Ping(t.Context()); !errors.Is(err, pubsub.ErrNotStarted) {
		t.Errorf("ping before startup = %v", err)
	}

	if err := client.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := client.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := client.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := client.Ping(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled ping = %v", err)
	}

	s.Shutdown()
	s.WaitForShutdown()
	if err := client.Ping(t.Context()); err == nil {
		t.Error("ping succeeded while broker was unavailable")
	}

	if err := client.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := client.Ping(t.Context()); !errors.Is(err, pubsub.ErrClosed) {
		t.Errorf("ping after shutdown = %v", err)
	}
}

func TestClient_StartupFailures(t *testing.T) {
	t.Run("canceled context", func(t *testing.T) {
		client, err := pubsub.New(pubsub.Config{Logger: newLogger(t, io.Discard)})
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		if err := client.Start(ctx); !errors.Is(err, context.Canceled) {
			t.Errorf("Start = %v", err)
		}

		if err := client.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("durability unavailable", func(t *testing.T) {
		s := startBroker(t, func(opts *server.Options) { opts.JetStream = false })
		client, err := pubsub.New(pubsub.Config{Logger: newLogger(t, io.Discard), Endpoints: []string{s.ClientURL()}})
		if err != nil {
			t.Fatal(err)
		}

		if err := client.Start(t.Context()); err == nil {
			t.Fatal("accepted broker without durable storage")
		}

		eventually(t, func() bool { return s.NumClients() == 0 })
	})

	t.Run("cancel during handshake", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}

		defer listener.Close()
		accepted := make(chan net.Conn, 1)
		go func() {
			conn, _ := listener.Accept()
			accepted <- conn
		}()
		client, err := pubsub.New(pubsub.Config{
			Logger: newLogger(t, io.Discard), Endpoints: []string{"nats://" + listener.Addr().String()}, Timeout: 500 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- client.Start(ctx) }()
		conn := receive(t, accepted)
		defer conn.Close()
		cancel()

		if err := receive(t, done); !errors.Is(err, context.Canceled) {
			t.Errorf("Start = %v", err)
		}
	})
}

func TestClient_AuthenticationAndTLS(t *testing.T) {
	certificateServer := httptest.NewTLSServer(nil)
	certificate := certificateServer.TLS.Certificates[0]
	certificateServer.Close()
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	s := startBroker(t, func(opts *server.Options) {
		opts.Authorization = "private-token"
		opts.TLS = true
		opts.TLSConfig = &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	})
	base := pubsub.Config{
		Logger: newLogger(t, io.Discard), Endpoints: []string{s.ClientURL()}, Token: "private-token",
		Topics: []pubsub.TopicConfig{{Name: "orders.created"}},
	}
	client, err := pubsub.New(base)
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Start(t.Context()); err == nil {
		t.Fatal("trusted a private certificate without its CA")
	}

	base.TLSConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	trusted := startClient(t, base)
	if err := trusted.Publish(t.Context(), pubsub.Message{ID: "secure-event", Topic: "orders.created"}); err != nil {
		t.Fatal(err)
	}

	base.Token = "wrong-token"
	denied, err := pubsub.New(base)
	if err != nil {
		t.Fatal(err)
	}

	if err := denied.Start(t.Context()); err == nil {
		t.Fatal("accepted invalid credentials")
	}
}

func TestClient_ShutdownCancelsPendingStartup(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, _ := listener.Accept()
		accepted <- conn
	}()
	client, err := pubsub.New(pubsub.Config{
		Logger: newLogger(t, io.Discard), Endpoints: []string{"nats://" + listener.Addr().String()}, Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan error, 1)
	go func() { started <- client.Start(t.Context()) }()
	conn := receive(t, accepted)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	if err := client.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown failed to cancel startup: %v", err)
	}

	if err := receive(t, started); !errors.Is(err, context.Canceled) && !errors.Is(err, pubsub.ErrClosed) {
		t.Errorf("pending startup = %v", err)
	}
}

func TestClient_PublicAPIHidesBrokerTypes(t *testing.T) {
	seen := map[reflect.Type]bool{}
	var inspect func(reflect.Type)
	inspect = func(typ reflect.Type) {
		if seen[typ] {
			return
		}

		seen[typ] = true
		if strings.Contains(typ.PkgPath(), "nats-io") {
			t.Errorf("broker type exposed: %v", typ)
		}

		if typ.PkgPath() != "" && typ.PkgPath() != "go-foundation/pubsub" {
			return
		}

		for i := 0; i < typ.NumMethod(); i++ {
			inspect(typ.Method(i).Type)
		}

		switch typ.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array:
			inspect(typ.Elem())
		case reflect.Map:
			inspect(typ.Key())
			inspect(typ.Elem())
		case reflect.Func:
			for i := 0; i < typ.NumIn(); i++ {
				inspect(typ.In(i))
			}

			for i := 0; i < typ.NumOut(); i++ {
				inspect(typ.Out(i))
			}
		case reflect.Struct:
			for i := 0; i < typ.NumField(); i++ {
				if typ.Field(i).IsExported() {
					inspect(typ.Field(i).Type)
				}
			}
		}
	}

	inspect(reflect.TypeOf(pubsub.New))
	inspect(reflect.TypeFor[pubsub.Handler]())
}

func startBroker(t *testing.T, configure func(*server.Options)) *server.Server {
	t.Helper()

	opts := &server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoSigs: true}
	if configure != nil {
		configure(opts)
	}

	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}

	go s.Start()
	t.Cleanup(func() {
		s.Shutdown()
		s.WaitForShutdown()
	})
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("broker did not become ready")
	}

	return s
}

func newLogger(t *testing.T, output io.Writer) *logger.Logger {
	t.Helper()

	log, err := logger.New(logger.Config{Service: "pubsub-test", Level: logger.LevelDebug, Output: output})
	if err != nil {
		t.Fatal(err)
	}

	return log
}

func startClient(t *testing.T, cfg pubsub.Config) *pubsub.Client {
	t.Helper()

	if cfg.Logger == nil {
		cfg.Logger = newLogger(t, io.Discard)
	}

	client, err := pubsub.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := client.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := client.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	return client
}

func brokerAPI(t *testing.T, s *server.Server) jetstream.JetStream {
	t.Helper()

	conn, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}

	return js
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()

	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for test signal")
		var zero T
		return zero
	}
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for !condition() {
		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatal("condition did not become true")
		}
	}
}
