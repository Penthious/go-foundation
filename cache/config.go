package cache

import (
	"crypto/tls"
	"errors"
	"math"
	"net"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Backend selects one cache implementation. There is no automatic fallback.
type Backend string

// Supported cache backends.
const (
	Memory Backend = "memory"
	Redis  Backend = "redis"
)

// Config configures one namespaced cache. Zero Backend selects Memory.
type Config struct {
	Backend Backend
	// Namespace is required: 1–128 ASCII letters, digits, dots, underscores or
	// hyphens. Use the same namespace across replicas that should share entries.
	Namespace string
	// DefaultTTL defaults to 5m; Set with ttl zero uses it. Entries always expire.
	DefaultTTL time.Duration
	// Timeout defaults to 2s for each operation, including Redis pool waits.
	Timeout time.Duration
	// MaxValueBytes defaults to 1 MiB and cannot exceed 512 MiB.
	MaxValueBytes int
	Memory        MemoryConfig
	// Redis is required for the Redis backend and rejected for the Memory backend.
	Redis *RedisConfig
	// Providers default to the global providers and are borrowed, never closed.
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
}

// MemoryConfig bounds one instance's in-process LRU cache.
type MemoryConfig struct {
	// MaxEntries defaults to 10,000.
	MaxEntries int
	// MaxBytes defaults to 64 MiB of stored key/value bytes. Go map/list metadata
	// and temporary request/result copies consume additional memory.
	MaxBytes int64
}

// RedisConfig configures a standalone Redis connection pool. Driver types stay
// private. Configure a verified TLS connection for traffic on untrusted networks.
type RedisConfig struct {
	// Address defaults to localhost:6379 and must be host:port, not a URL.
	Address  string
	Username string
	Password string
	DB       int
	// PoolSize defaults to 10, bounding concurrent Redis connections.
	PoolSize int
	// TLS is cloned; custom root pools/callbacks remain caller-owned and immutable.
	TLS *tls.Config
}

func normalizeConfig(cfg Config) (Config, error) {
	if cfg.Backend == "" {
		cfg.Backend = Memory
	}

	if cfg.Backend != Memory && cfg.Backend != Redis {
		return Config{}, errors.New("cache: unsupported backend")
	}

	if len(cfg.Namespace) == 0 || len(cfg.Namespace) > 128 {
		return Config{}, errors.New("cache: namespace must contain 1–128 ASCII identifier characters")
	}

	for _, c := range cfg.Namespace {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
			return Config{}, errors.New("cache: invalid namespace")
		}
	}

	if cfg.DefaultTTL == 0 {
		cfg.DefaultTTL = 5 * time.Minute
	}

	ttl, err := normalizeTTL(cfg.DefaultTTL)
	if err != nil {
		return Config{}, err
	}

	cfg.DefaultTTL = ttl
	if cfg.Timeout < 0 || cfg.MaxValueBytes < 0 || cfg.MaxValueBytes > 512<<20 {
		return Config{}, errors.New("cache: invalid timeout or value size limit")
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = 2 * time.Second
	}

	if cfg.MaxValueBytes == 0 {
		cfg.MaxValueBytes = 1 << 20
	}

	if cfg.Memory.MaxEntries < 0 || cfg.Memory.MaxBytes < 0 {
		return Config{}, errors.New("cache: memory limits must not be negative")
	}

	if cfg.Memory.MaxEntries == 0 {
		cfg.Memory.MaxEntries = 10_000
	}

	if cfg.Memory.MaxBytes == 0 {
		cfg.Memory.MaxBytes = 64 << 20
	}

	if cfg.Backend == Memory {
		if cfg.Redis != nil {
			return Config{}, errors.New("cache: Redis config requires the Redis backend")
		}

		return cfg, nil
	}

	if cfg.Redis == nil {
		return Config{}, errors.New("cache: Redis config is required")
	}

	r := *cfg.Redis
	if r.Address == "" {
		r.Address = "localhost:6379"
	}

	host, port, err := net.SplitHostPort(r.Address)
	if err != nil || host == "" {
		return Config{}, errors.New("cache: Redis address must be host:port")
	}

	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 || r.DB < 0 || r.PoolSize < 0 || r.PoolSize > math.MaxInt32 {
		return Config{}, errors.New("cache: invalid Redis port, database or pool size")
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

	return cfg, nil
}

func normalizeTTL(ttl time.Duration) (time.Duration, error) {
	if ttl < time.Millisecond || ttl > time.Duration(math.MaxInt64/int64(time.Millisecond))*time.Millisecond {
		return 0, errors.New("cache: TTL must be at least 1ms and fit in whole milliseconds")
	}

	if remainder := ttl % time.Millisecond; remainder != 0 {
		ttl += time.Millisecond - remainder
	}

	return ttl, nil
}
