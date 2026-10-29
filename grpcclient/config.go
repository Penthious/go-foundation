package grpcclient

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/Penthious/go-foundation/logger"
)

// Config configures one reusable channel to a trusted internal RPC service.
type Config struct {
	// Name identifies this client in circuit-breaker telemetry; empty means
	// "default". Named Fx bindings set it to their binding name automatically.
	Name string
	// Target is a gRPC target, such as orders.internal:9090 or dns:///orders:9090.
	// Credentials belong in the fields below, never in the target URI.
	Target string
	Logger *logger.Logger
	// TLS is cloned. Nil uses verified TLS with system roots and TLS 1.2 minimum.
	// Certificate/root pools and certificate callbacks remain caller-owned.
	TLS *tls.Config
	// Insecure explicitly selects plaintext for a trusted internal network.
	// It cannot be combined with TLS.
	Insecure bool
	// Timeout defaults to 30s for a whole unary RPC, including credential lookup
	// and connection waits. StreamTimeout defaults to 5m for a whole stream.
	// Earlier caller deadlines win; negative values are rejected.
	Timeout       time.Duration
	StreamTimeout time.Duration
	// CircuitBreaker optionally fails fast during an upstream outage. Its state
	// belongs to this client and is shared by all unary and streaming methods.
	CircuitBreaker *CircuitBreakerConfig
	// Configure at most one credential source. Tokens are raw values, without
	// the Bearer prefix. Empty static fields mean no configured credential.
	APIKey      string
	BearerToken string
	// TokenProvider is called for each RPC/stream establishment. It must honor
	// cancellation and be concurrency-safe. Errors/panics are never exposed.
	TokenProvider func(context.Context) (string, error)
	// Dialer optionally supplies a standard context-aware connection dialer,
	// useful for private networks and in-memory gRPC test listeners.
	Dialer func(context.Context, string) (net.Conn, error)
	// Message limits default to 4 MiB each. Native per-call size options may
	// override these defaults when a specific RPC needs a different limit.
	MaxRecvMessageBytes int
	MaxSendMessageBytes int
}

func normalizeConfig(cfg Config) (Config, error) {
	if cfg.Logger == nil || strings.TrimSpace(cfg.Target) == "" || strings.TrimSpace(cfg.Target) != cfg.Target {
		return Config{}, errors.New("grpcclient: logger and target are required")
	}

	if strings.ContainsAny(cfg.Target, "@?#") {
		return Config{}, errors.New("grpcclient: target must not contain credentials, query parameters or fragments")
	}

	for _, char := range cfg.Target {
		if char <= ' ' || char == 127 {
			return Config{}, errors.New("grpcclient: invalid target")
		}
	}

	if strings.Contains(cfg.Target, "://") {
		u, err := url.Parse(cfg.Target)
		if err != nil || u.User != nil || u.Scheme == "" || u.Path == "" {
			return Config{}, errors.New("grpcclient: invalid target URI")
		}
	}

	if cfg.Insecure && cfg.TLS != nil {
		return Config{}, errors.New("grpcclient: plaintext and TLS settings cannot be combined")
	}

	if cfg.Timeout < 0 || cfg.StreamTimeout < 0 || cfg.MaxRecvMessageBytes < 0 || cfg.MaxSendMessageBytes < 0 {
		return Config{}, errors.New("grpcclient: timeouts and message limits must not be negative")
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}

	if cfg.StreamTimeout == 0 {
		cfg.StreamTimeout = 5 * time.Minute
	}

	if cfg.MaxRecvMessageBytes == 0 {
		cfg.MaxRecvMessageBytes = 4 << 20
	}

	if cfg.MaxSendMessageBytes == 0 {
		cfg.MaxSendMessageBytes = 4 << 20
	}

	sources := 0
	for _, configured := range []bool{cfg.APIKey != "", cfg.BearerToken != "", cfg.TokenProvider != nil} {
		if configured {
			sources++
		}
	}

	if sources > 1 || cfg.APIKey != "" && !validCredential(cfg.APIKey, 4096) || cfg.BearerToken != "" && !validCredential(cfg.BearerToken, 16<<10) {
		return Config{}, errors.New("grpcclient: configure one valid API key, bearer token or token provider")
	}

	if !cfg.Insecure {
		if cfg.TLS == nil {
			cfg.TLS = &tls.Config{}
		} else {
			cfg.TLS = cfg.TLS.Clone()
		}

		if cfg.TLS.MinVersion == 0 {
			cfg.TLS.MinVersion = tls.VersionTLS12
		}
	}

	return cfg, nil
}
