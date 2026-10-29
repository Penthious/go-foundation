package pubsub_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/pubsub"
)

func TestNew_Validation(t *testing.T) {
	log := newLogger(t, io.Discard)
	tests := []struct {
		name   string
		change func(*pubsub.Config)
	}{
		{"missing logger", func(cfg *pubsub.Config) { cfg.Logger = nil }},
		{"negative timeout", func(cfg *pubsub.Config) { cfg.Timeout = -time.Second }},
		{"bad endpoint", func(cfg *pubsub.Config) { cfg.Endpoints = []string{"localhost:4222"} }},
		{"empty endpoint", func(cfg *pubsub.Config) { cfg.Endpoints = []string{""} }},
		{"HTTP endpoint", func(cfg *pubsub.Config) { cfg.Endpoints = []string{"http://localhost:4222"} }},
		{"embedded credentials", func(cfg *pubsub.Config) { cfg.Endpoints = []string{"nats://user:secret@localhost:4222"} }},
		{"query", func(cfg *pubsub.Config) { cfg.Endpoints = []string{"nats://localhost:4222?token=secret"} }},
		{"mixed auth", func(cfg *pubsub.Config) { cfg.Token, cfg.Username = "secret", "user" }},
		{"password without user", func(cfg *pubsub.Config) { cfg.Password = "secret" }},
		{"wildcard topic", func(cfg *pubsub.Config) { cfg.Topics = []pubsub.TopicConfig{{Name: "orders.*"}} }},
		{"long topic", func(cfg *pubsub.Config) { cfg.Topics = []pubsub.TopicConfig{{Name: strings.Repeat("a", 201)}} }},
		{"duplicate topic", func(cfg *pubsub.Config) { cfg.Topics = []pubsub.TopicConfig{{Name: "orders"}, {Name: "orders"}} }},
		{"negative retention", func(cfg *pubsub.Config) { cfg.Topics = []pubsub.TopicConfig{{Name: "orders", MaxAge: -time.Second}} }},
		{"negative storage", func(cfg *pubsub.Config) { cfg.Topics = []pubsub.TopicConfig{{Name: "orders", MaxBytes: -1}} }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := pubsub.Config{Logger: log}
			tt.change(&cfg)

			if _, err := pubsub.New(cfg); err == nil {
				t.Fatal("accepted invalid config")
			} else if strings.Contains(err.Error(), "secret") {
				t.Error("validation exposed credentials")
			}
		})
	}
}

func TestNew_CopiesConfiguration(t *testing.T) {
	s := startBroker(t, nil)
	cfg := pubsub.Config{
		Logger: newLogger(t, io.Discard), Endpoints: []string{s.ClientURL()},
		Topics: []pubsub.TopicConfig{{Name: "orders.created"}},
	}
	client, err := pubsub.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	cfg.Endpoints[0] = "invalid"
	cfg.Topics[0].Name = "invalid.*"
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

	if err := client.Publish(t.Context(), pubsub.Message{ID: "event-1", Topic: "orders.created"}); err != nil {
		t.Fatal(err)
	}
}
