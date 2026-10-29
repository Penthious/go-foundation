package pubsub

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// TopicConfig sets disk-backed retention. Both limits apply independently.
// Expired messages are removed even if unprocessed. A full topic rejects new
// publications rather than evicting older messages to make room.
type TopicConfig struct {
	Name string
	// MaxAge defaults to 24 hours. Negative values are rejected.
	MaxAge time.Duration
	// MaxBytes defaults to 64 MiB, including broker metadata.
	MaxBytes int64
}

func normalizeTopic(cfg TopicConfig) (TopicConfig, error) {
	if err := validateTopic(cfg.Name); err != nil {
		return TopicConfig{}, err
	}

	if cfg.MaxAge < 0 || cfg.MaxBytes < 0 {
		return TopicConfig{}, errors.New("pubsub: topic retention limits must not be negative")
	}

	if cfg.MaxAge == 0 {
		cfg.MaxAge = 24 * time.Hour
	}

	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = 64 << 20
	}

	return cfg, nil
}

func createTopic(ctx context.Context, js jetstream.JetStream, cfg TopicConfig) error {
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: streamName(cfg.Name), Subjects: []string{cfg.Name},
		Storage: jetstream.FileStorage, Retention: jetstream.LimitsPolicy,
		Discard: jetstream.DiscardNew, MaxAge: cfg.MaxAge, MaxBytes: cfg.MaxBytes,
	})
	if err != nil {
		return fmt.Errorf("pubsub: provision topic %q: %w", cfg.Name, err)
	}

	return nil
}

func streamName(topic string) string {
	return fmt.Sprintf("gof_%x", sha256.Sum256([]byte(topic)))
}
