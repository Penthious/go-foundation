package pubsub

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Penthious/go-foundation/logger"
)

// Config configures a reusable durable messaging client.
type Config struct {
	// Logger is required.
	Logger *logger.Logger
	// Endpoints defaults to nats://localhost:4222. Credentials belong in the
	// fields below, not URLs. Only nats:// and tls:// endpoints are accepted.
	Endpoints []string
	Name      string
	Username  string
	Password  string
	Token     string
	// TLSConfig is cloned. Use it for private CAs or mutual TLS.
	TLSConfig *tls.Config
	// Timeout bounds connection attempts and broker operations; defaults to 5s.
	Timeout time.Duration
	// Topics are created at startup. Existing topics must match; they are never
	// overwritten. Consumers can omit topics provisioned by another service.
	Topics []TopicConfig
}

var topicPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$`)
var groupPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func normalizeConfig(cfg Config) (Config, error) {
	if cfg.Logger == nil {
		return Config{}, errors.New("pubsub: logger is required")
	}

	if cfg.Timeout < 0 {
		return Config{}, errors.New("pubsub: timeout must not be negative")
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}

	if cfg.Token != "" && (cfg.Username != "" || cfg.Password != "") {
		return Config{}, errors.New("pubsub: use token or username/password authentication")
	}

	if cfg.Password != "" && cfg.Username == "" {
		return Config{}, errors.New("pubsub: password requires username")
	}

	cfg.Endpoints = slices.Clone(cfg.Endpoints)
	if len(cfg.Endpoints) == 0 {
		cfg.Endpoints = []string{"nats://localhost:4222"}
	}

	for i, endpoint := range cfg.Endpoints {
		u, err := url.Parse(endpoint)
		if err != nil || u.Hostname() == "" || (u.Scheme != "nats" && u.Scheme != "tls") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
			return Config{}, fmt.Errorf("pubsub: invalid endpoint at index %d", i)
		}
	}

	if cfg.TLSConfig != nil {
		cfg.TLSConfig = cfg.TLSConfig.Clone()
	}

	cfg.Topics = slices.Clone(cfg.Topics)
	seen := make(map[string]bool, len(cfg.Topics))
	for i, topic := range cfg.Topics {
		normalized, err := normalizeTopic(topic)
		if err != nil {
			return Config{}, err
		}

		if seen[topic.Name] {
			return Config{}, fmt.Errorf("pubsub: duplicate topic %q", topic.Name)
		}

		seen[topic.Name] = true
		cfg.Topics[i] = normalized
	}

	return cfg, nil
}

func validateTopic(topic string) error {
	if len(topic) > 200 || !topicPattern.MatchString(topic) {
		return errors.New("pubsub: topic must contain dot-separated letters, numbers, underscores, or hyphens (maximum 200 bytes)")
	}

	return nil
}

func validateID(id string) error {
	if id == "" || len(id) > 256 || !utf8.ValidString(id) || strings.ContainsAny(id, "\r\n\x00") {
		return errors.New("pubsub: message ID must contain 1-256 valid UTF-8 bytes with no CR, LF, or NUL")
	}

	return nil
}
