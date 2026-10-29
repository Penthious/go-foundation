package foundationfx_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/foundationfx"
	"github.com/Penthious/go-foundation/logger"
	"github.com/Penthious/go-foundation/pubsub"

	"github.com/nats-io/nats-server/v2/server"
	"go.uber.org/fx"
)

func TestModule_PubSubLifecycle(t *testing.T) {
	s := startPubSubBroker(t)
	var client, sameClient *pubsub.Client
	app := fx.New(
		foundationfx.Module, fx.NopLogger,
		fx.Supply(logger.Config{Service: "orders", Output: io.Discard}),
		fx.Supply(pubsub.Config{Endpoints: []string{s.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}}),
		fx.Populate(&client, &sameClient),
	)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}

	if client != sameClient || s.NumClients() != 0 {
		t.Fatal("pubsub must be a singleton and connect only during startup")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if err := app.Start(ctx); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := app.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	message := pubsub.Message{ID: "event-1", Topic: "orders.created"}
	if err := client.Publish(t.Context(), message); err != nil {
		t.Fatal(err)
	}

	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	if err := client.Publish(t.Context(), message); !errors.Is(err, pubsub.ErrClosed) {
		t.Errorf("Fx did not shut down pubsub: %v", err)
	}
}

func TestModule_PubSubStartupRollback(t *testing.T) {
	s := startPubSubBroker(t)
	var client *pubsub.Client
	app := fx.New(
		foundationfx.Module, fx.NopLogger,
		fx.Supply(logger.Config{Service: "orders", Output: io.Discard}),
		fx.Supply(pubsub.Config{Endpoints: []string{s.ClientURL()}}),
		fx.Populate(&client),
		fx.Invoke(func(lifecycle fx.Lifecycle) {
			lifecycle.Append(fx.Hook{OnStart: func(context.Context) error {
				return errors.New("service startup failed")
			}})
		}),
	)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if err := app.Start(ctx); err == nil {
		t.Fatal("expected service startup failure")
	}

	if err := client.Start(ctx); !errors.Is(err, pubsub.ErrClosed) {
		t.Errorf("startup rollback did not shut down pubsub: %v", err)
	}
}

func TestModule_PubSubConstructionFailureOpensNoConnections(t *testing.T) {
	s := startPubSubBroker(t)
	app := fx.New(
		foundationfx.Module, fx.NopLogger,
		fx.Supply(logger.Config{Service: "orders", Output: io.Discard}),
		fx.Supply(pubsub.Config{Endpoints: []string{s.ClientURL()}}),
		fx.Invoke(func(*pubsub.Client) error { return errors.New("graph construction failed") }),
	)

	if app.Err() == nil || s.NumClients() != 0 {
		t.Fatal("invalid dependency graph accepted or opened broker connections")
	}
}

func startPubSubBroker(t *testing.T) *server.Server {
	t.Helper()

	s, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoSigs: true,
	})
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
