// Package ratelimit supplies burst-capable per-identity quotas with private
// memory/Redis implementations, HTTP middleware and gRPC interceptors.
package ratelimit

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Operational errors never include identities, keys, credentials or driver errors.
var (
	ErrClosed         = errors.New("ratelimit: limiter is closed")
	ErrUnavailable    = errors.New("ratelimit: backend unavailable")
	ErrCapacity       = errors.New("ratelimit: memory identity capacity reached")
	ErrPolicyMismatch = errors.New("ratelimit: shared quota policy mismatch")
	ErrInvalidKey     = errors.New("ratelimit: identity must contain 1–4096 nonblank bytes")
)

// Decision describes one immediate check. Denial is not an operational error.
// Remaining is a snapshot; RetryAfter is positive only when denied.
type Decision struct {
	Allowed    bool
	Remaining  int
	RetryAfter time.Duration
}

// Limiter is safe for concurrent use. Reuse one per policy/namespace and close
// it after servers stop. Its zero value is not usable.
type Limiter struct {
	cfg       Config
	interval  time.Duration
	memory    *memoryStore
	redis     *redis.Client
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
	tracer    trace.Tracer
	checks    metric.Int64Counter
	duration  metric.Float64Histogram
}

// New validates and snapshots config without network I/O. Initialize telemetry
// first. Each new memory limiter has fresh quotas; Redis state survives Close.
func New(cfg Config) (*Limiter, error) {
	cfg, interval, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}

	mp, tp := cfg.MeterProvider, cfg.TracerProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}

	if tp == nil {
		tp = otel.GetTracerProvider()
	}

	meter := mp.Meter("go-foundation/ratelimit")
	checks, err := meter.Int64Counter("ratelimit.checks", metric.WithDescription("Rate limit decisions by namespace, backend and result"))
	if err != nil {
		return nil, fmt.Errorf("ratelimit: create check counter: %w", err)
	}

	duration, err := meter.Float64Histogram("ratelimit.check.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, fmt.Errorf("ratelimit: create duration histogram: %w", err)
	}

	l := &Limiter{cfg: cfg, interval: interval, tracer: tp.Tracer("go-foundation/ratelimit"), checks: checks, duration: duration}
	if cfg.Backend == Memory {
		l.memory = &memoryStore{entries: make(map[[sha256.Size]byte]*memoryEntry), now: time.Now}
	} else {
		l.redis = newRedisClient(*cfg.Redis, cfg.Timeout)
	}

	return l, nil
}

// Allow consumes one token for a service-owned stable identity. The identity is
// hashed before storage and never logged or added to telemetry. HTTP/gRPC
// adapters derive identities from an already authenticated GOF principal.
// Checks never wait for tokens or retry ambiguous Redis writes.
func (l *Limiter) Allow(ctx context.Context, identity string) (decision Decision, err error) {
	ctx, finish := l.begin(ctx)
	defer func() { finish(decision, err) }()

	if l.closed.Load() {
		return Decision{}, ErrClosed
	}

	if err := ctx.Err(); err != nil {
		return Decision{}, err
	}

	if len(identity) == 0 || len(identity) > 4096 || strings.TrimSpace(identity) == "" {
		return Decision{}, ErrInvalidKey
	}

	key := sha256.Sum256([]byte(identity))
	if l.memory != nil {
		return l.memoryAllow(ctx, key)
	}

	return l.redisAllow(ctx, key)
}

// Ping checks connectivity/authentication without consuming tokens. Redis PING
// does not verify every script command's ACL permission. Use as a health callback.
func (l *Limiter) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, l.cfg.Timeout)
	defer cancel()

	if l.closed.Load() {
		return ErrClosed
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if l.redis != nil {
		if err := l.redis.Ping(ctx).Err(); err != nil {
			return l.redisError(ctx, err)
		}
	}

	return nil
}

// Close releases owned resources without deleting shared Redis quotas or closing
// borrowed telemetry/loggers. In-flight operations may fail. It is idempotent.
func (l *Limiter) Close() error {
	l.closeOnce.Do(func() {
		l.closed.Store(true)
		if l.memory != nil {
			l.memory.mu.Lock()
			l.memory.entries, l.memory.expiry = nil, nil
			l.memory.mu.Unlock()
		}

		if l.redis != nil {
			if err := l.redis.Close(); err != nil {
				l.closeErr = ErrUnavailable
			}
		}
	})

	return l.closeErr
}
