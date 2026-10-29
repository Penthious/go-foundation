package grpcserver

import (
	"crypto/tls"
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/Penthious/go-foundation/auth"
	"github.com/Penthious/go-foundation/health"
	"github.com/Penthious/go-foundation/logger"
	"github.com/Penthious/go-foundation/ratelimit"

	"google.golang.org/grpc"
)

// Config supplies service-owned generated registrations and listener settings.
type Config struct {
	Logger *logger.Logger
	// Address defaults to :9090; use 127.0.0.1:0 for a local ephemeral listener.
	Address string
	// Register calls generated RegisterXServer functions with their handlers.
	// At least one application service is required. Health/reflection are reserved.
	Register []func(grpc.ServiceRegistrar)
	// TLS is cloned. Nil serves plaintext, suitable behind a trusted TLS ingress.
	TLS *tls.Config
	// Auth, when set, protects every method except health Check/Watch and explicit
	// PublicMethods. Authentication is checked once at stream establishment.
	Auth          *auth.Authenticator
	PublicMethods []string
	// RequireScopes maps exact /package.Service/Method names to required scopes.
	// Every listed scope is required; unknown methods or conflicting policies fail New.
	RequireScopes map[string][]string
	// RateLimiter optionally limits authenticated application RPCs after auth
	// and scope checks. Auth is required. Health methods and PublicMethods bypass
	// it. A stream is charged once at establishment. The limiter is borrowed.
	RateLimiter *ratelimit.Limiter
	// Reflection defaults to false. When enabled it follows the same auth policy.
	Reflection bool
	// Health supplies readiness dependencies. Check/List are evaluated on demand;
	// Watch polls under HealthTimeout at HealthWatchInterval (defaults 2s and 5s).
	Health              *health.Checker
	HealthTimeout       time.Duration
	HealthWatchInterval time.Duration
	// UnaryTimeout defaults to 30s. Earlier client deadlines win. Stream lifetimes
	// follow client deadlines and handler policy; no fake stream deadline is imposed.
	UnaryTimeout time.Duration
	// ConnectionTimeout defaults to 5s for connection handshakes.
	ConnectionTimeout time.Duration
	// Message limits default to 4 MiB each; header metadata defaults to 64 KiB.
	MaxRecvMessageBytes int
	MaxSendMessageBytes int
	MaxHeaderBytes      uint32
	// MaxConcurrentStreams defaults to 100 per connection.
	MaxConcurrentStreams uint32
}

func normalizeConfig(cfg Config) (Config, error) {
	if cfg.Logger == nil || len(cfg.Register) == 0 {
		return Config{}, errors.New("grpcserver: logger and service registrations are required")
	}

	if cfg.MaxRecvMessageBytes < 0 || cfg.MaxSendMessageBytes < 0 {
		return Config{}, errors.New("grpcserver: message limits must not be negative")
	}

	if cfg.Auth == nil && (len(cfg.PublicMethods) > 0 || len(cfg.RequireScopes) > 0) {
		return Config{}, errors.New("grpcserver: method policies require an authenticator")
	}

	if cfg.RateLimiter != nil && cfg.Auth == nil {
		return Config{}, errors.New("grpcserver: rate limiting requires authentication")
	}

	for _, duration := range []struct {
		value    *time.Duration
		fallback time.Duration
	}{
		{&cfg.HealthTimeout, 2 * time.Second},
		{&cfg.HealthWatchInterval, 5 * time.Second},
		{&cfg.UnaryTimeout, 30 * time.Second},
		{&cfg.ConnectionTimeout, 5 * time.Second},
	} {
		if *duration.value < 0 {
			return Config{}, errors.New("grpcserver: timeouts must not be negative")
		}

		if *duration.value == 0 {
			*duration.value = duration.fallback
		}
	}

	if cfg.Address == "" {
		cfg.Address = ":9090"
	}

	if cfg.MaxRecvMessageBytes == 0 {
		cfg.MaxRecvMessageBytes = 4 << 20
	}

	if cfg.MaxSendMessageBytes == 0 {
		cfg.MaxSendMessageBytes = 4 << 20
	}

	if cfg.MaxHeaderBytes == 0 {
		cfg.MaxHeaderBytes = 64 << 10
	}

	if cfg.MaxConcurrentStreams == 0 {
		cfg.MaxConcurrentStreams = 100
	}

	if cfg.TLS != nil {
		cfg.TLS = cfg.TLS.Clone()
		if len(cfg.TLS.Certificates) == 0 && cfg.TLS.GetCertificate == nil && cfg.TLS.GetConfigForClient == nil {
			return Config{}, errors.New("grpcserver: TLS requires a server certificate or certificate callback")
		}

		if cfg.TLS.MinVersion == 0 {
			cfg.TLS.MinVersion = tls.VersionTLS12
		}
	}

	cfg.Register = slices.Clone(cfg.Register)
	cfg.PublicMethods = slices.Clone(cfg.PublicMethods)
	cfg.RequireScopes = maps.Clone(cfg.RequireScopes)
	for method, scopes := range cfg.RequireScopes {
		cfg.RequireScopes[method] = slices.Clone(scopes)
		if !(auth.Principal{Scopes: scopes}).HasScopes(scopes...) {
			return Config{}, errors.New("grpcserver: invalid required scopes")
		}
	}

	return cfg, nil
}
