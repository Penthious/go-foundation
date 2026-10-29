package ratelimit

import (
	"crypto/tls"
	"errors"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/Penthious/go-foundation/logger"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Backend selects local or replica-shared quota storage. There is no fallback.
type Backend string

// Supported rate-limit backends.
const (
	Memory Backend = "memory"
	Redis  Backend = "redis"
)

// Config defines one burst-capable quota shared by all uses of this limiter.
type Config struct {
	// Namespace is required: 1–128 ASCII letters, digits, dots, underscores or
	// hyphens. Redis replicas sharing a quota must use the same namespace/policy.
	Namespace string
	Backend   Backend // Defaults to Memory.
	Logger    *logger.Logger
	// Limit tokens refill evenly over Window (defaults: 100 per second).
	// Burst defaults to Limit and is the maximum immediately available tokens.
	// Window must be 1ms–24h in whole microseconds. Refill intervals round up to
	// whole microseconds; at most one token/microsecond and 24h of debt are allowed.
	Limit  int
	Window time.Duration
	Burst  int
	// MaxKeys bounds active memory identities, default 10,000. Full stores deny
	// new identities with ErrCapacity rather than evicting active quotas.
	MaxKeys int
	// Timeout defaults to 2s, including Redis connection/pool waits.
	Timeout time.Duration
	Redis   *RedisConfig
	// Providers default to globals and are borrowed, never shut down.
	MeterProvider  metric.MeterProvider
	TracerProvider trace.TracerProvider
}

// RedisConfig configures an owned standalone Redis pool; driver types stay private.
type RedisConfig struct {
	Address  string // Defaults to localhost:6379; host:port, never a URL.
	Username string
	Password string
	DB       int
	PoolSize int // Defaults to ten.
	// TLS is cloned; root pools and callbacks remain caller-owned and immutable.
	TLS *tls.Config
}

func normalizeConfig(cfg Config) (Config, time.Duration, error) {
	if cfg.Logger == nil || len(cfg.Namespace) == 0 || len(cfg.Namespace) > 128 {
		return Config{}, 0, errors.New("ratelimit: logger and 1–128 character namespace are required")
	}

	for _, c := range cfg.Namespace {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
			return Config{}, 0, errors.New("ratelimit: invalid namespace")
		}
	}

	if cfg.Backend == "" {
		cfg.Backend = Memory
	}

	if cfg.Backend != Memory && cfg.Backend != Redis || cfg.Limit < 0 || cfg.Window < 0 || cfg.Burst < 0 || cfg.MaxKeys < 0 || cfg.Timeout < 0 {
		return Config{}, 0, errors.New("ratelimit: invalid backend or negative limits/timeouts")
	}

	if cfg.Limit == 0 {
		cfg.Limit = 100
	}

	if cfg.Window == 0 {
		cfg.Window = time.Second
	}

	if cfg.Burst == 0 {
		cfg.Burst = cfg.Limit
	}

	if cfg.Window < time.Millisecond || cfg.Window > 24*time.Hour || cfg.Window%time.Microsecond != 0 || int64(cfg.Limit) > cfg.Window.Microseconds() {
		return Config{}, 0, errors.New("ratelimit: invalid window or refill rate")
	}

	interval := (cfg.Window.Microseconds() + int64(cfg.Limit) - 1) / int64(cfg.Limit)
	if int64(cfg.Burst) > (24*time.Hour).Microseconds()/interval {
		return Config{}, 0, errors.New("ratelimit: burst refill time must not exceed 24h")
	}

	if cfg.MaxKeys == 0 {
		cfg.MaxKeys = 10_000
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = 2 * time.Second
	}

	if cfg.Backend == Memory {
		if cfg.Redis != nil {
			return Config{}, 0, errors.New("ratelimit: Redis config requires the Redis backend")
		}

		return cfg, time.Duration(interval) * time.Microsecond, nil
	}

	if cfg.Redis == nil {
		return Config{}, 0, errors.New("ratelimit: Redis config is required")
	}

	r := *cfg.Redis
	if r.Address == "" {
		r.Address = "localhost:6379"
	}

	host, port, err := net.SplitHostPort(r.Address)
	if err != nil || host == "" || strings.ContainsAny(r.Address, "@/?#\\ \t\r\n") {
		return Config{}, 0, errors.New("ratelimit: Redis address must be host:port")
	}

	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 || r.DB < 0 || r.PoolSize < 0 || r.PoolSize > math.MaxInt32 {
		return Config{}, 0, errors.New("ratelimit: invalid Redis port, database or pool size")
	}

	if r.PoolSize == 0 {
		r.PoolSize = 10
	}

	if r.TLS != nil {
		r.TLS = r.TLS.Clone()
		if r.TLS.MinVersion == 0 {
			r.TLS.MinVersion = tls.VersionTLS12
		}
	}

	cfg.Redis = &r

	return cfg, time.Duration(interval) * time.Microsecond, nil
}
