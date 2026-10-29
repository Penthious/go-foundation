package pubsub_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/pubsub"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
)

func TestTopic_DefaultsAndConfigurationConflict(t *testing.T) {
	s := startBroker(t, nil)
	cfg := pubsub.Config{Endpoints: []string{s.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}}
	startClient(t, cfg)
	startClient(t, cfg)
	js := brokerAPI(t, s)
	name, err := js.StreamNameBySubject(t.Context(), "orders.created")
	if err != nil {
		t.Fatal(err)
	}

	stream, err := js.Stream(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}

	info, err := stream.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if info.Config.Storage != jetstream.FileStorage || info.Config.MaxAge != 24*time.Hour || info.Config.MaxBytes != 64<<20 || info.Config.Discard != jetstream.DiscardNew {
		t.Errorf("unexpected topic defaults: %+v", info.Config)
	}

	cfg.Logger = newLogger(t, io.Discard)
	cfg.Topics[0].MaxAge = time.Hour
	conflict, err := pubsub.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := conflict.Start(t.Context()); err == nil {
		t.Fatal("silently replaced existing retention config")
	}

	info, err = stream.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if info.Config.MaxAge != 24*time.Hour {
		t.Error("startup conflict modified existing topic")
	}
}

func TestTopic_FullStorageRejectsNewMessages(t *testing.T) {
	s := startBroker(t, nil)
	client := startClient(t, pubsub.Config{Endpoints: []string{s.ClientURL()}, Topics: []pubsub.TopicConfig{{Name: "orders.created", MaxBytes: 2048}}})
	rejected := false
	for i := range 10 {
		err := client.Publish(t.Context(), pubsub.Message{ID: fmt.Sprint(i), Topic: "orders.created", Data: make([]byte, 512)})
		if err != nil {
			rejected = true
			break
		}
	}

	if !rejected {
		t.Fatal("full topic did not reject new publications")
	}

	got := make(chan string, 10)
	runConsumer(t, client, pubsub.ConsumerConfig{Topic: "orders.created", Group: "billing"}, func(_ context.Context, message pubsub.Message) error {
		got <- message.ID
		return nil
	})

	if id := receive(t, got); id != "0" {
		t.Errorf("oldest retained message evicted: got %q", id)
	}
}

func TestTopic_SurvivesBrokerRestartAndReconnects(t *testing.T) {
	s := startBroker(t, nil)
	address := s.Addr().(*net.TCPAddr)
	store := filepath.Dir(s.StoreDir())
	client := startClient(t, pubsub.Config{Endpoints: []string{s.ClientURL()}, Timeout: time.Second, Topics: []pubsub.TopicConfig{{Name: "orders.created"}}})
	if err := client.Publish(t.Context(), pubsub.Message{ID: "before-restart", Topic: "orders.created"}); err != nil {
		t.Fatal(err)
	}

	s.Shutdown()
	s.WaitForShutdown()
	if err := client.Publish(t.Context(), pubsub.Message{ID: "after-restart", Topic: "orders.created"}); err == nil {
		t.Fatal("unavailable broker acknowledged a publication")
	}

	startBroker(t, func(opts *server.Options) {
		opts.Port = address.Port
		opts.StoreDir = store
	})
	eventually(t, func() bool {
		return client.Publish(t.Context(), pubsub.Message{ID: "after-restart", Topic: "orders.created"}) == nil
	})
	got := make(chan string, 2)
	runConsumer(t, client, pubsub.ConsumerConfig{Topic: "orders.created", Group: "billing"}, func(_ context.Context, message pubsub.Message) error {
		got <- message.ID
		return nil
	})

	if first, second := receive(t, got), receive(t, got); first != "before-restart" || second != "after-restart" {
		t.Errorf("messages lost across restart: %q, %q", first, second)
	}
}
