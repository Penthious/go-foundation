// Package cache provides expiring byte values through interchangeable Redis and
// bounded in-memory backends. Services own serialization; driver types stay private.
package cache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Stable cache outcomes; inspect them with errors.Is.
var (
	ErrMiss          = errors.New("cache: entry not found")
	ErrClosed        = errors.New("cache: client is closed")
	ErrUnavailable   = errors.New("cache: backend unavailable")
	ErrValueTooLarge = errors.New("cache: value exceeds configured size limit")
)

// Client is safe for concurrent use; construct it with New and close it after
// stopping callers. It uses one backend and never silently falls back to another.
type Client struct {
	cfg        Config
	memory     *memoryStore
	redis      *redis.Client
	closed     atomic.Bool
	closeOnce  sync.Once
	closeErr   error
	tracer     trace.Tracer
	operations metric.Int64Counter
	duration   metric.Float64Histogram
}

// New validates config and creates a lazy client. It performs no network I/O.
// Initialize telemetry before New. Call Ping at startup to check Redis access.
func New(cfg Config) (*Client, error) {
	cfg, err := normalizeConfig(cfg)
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

	meter := mp.Meter("go-foundation/cache")
	operations, err := meter.Int64Counter("cache.operations", metric.WithDescription("Cache operations by backend, operation and result"))
	if err != nil {
		return nil, fmt.Errorf("cache: create operation counter: %w", err)
	}

	duration, err := meter.Float64Histogram("cache.operation.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, fmt.Errorf("cache: create duration histogram: %w", err)
	}

	c := &Client{cfg: cfg, tracer: tp.Tracer("go-foundation/cache"), operations: operations, duration: duration}
	if cfg.Backend == Memory {
		c.memory = newMemoryStore(cfg.Memory)
	} else {
		c.redis = newRedisClient(*cfg.Redis, cfg.Timeout)
	}

	return c, nil
}

// Get returns an independent byte slice, or ErrMiss for an absent/expired entry.
// An empty stored value is a hit. Errors never include keys, values or driver errors.
func (c *Client) Get(ctx context.Context, key string) (value []byte, err error) {
	ctx, finish := c.begin(ctx, "get")
	defer func() { finish(err) }()

	fullKey, err := c.key(ctx, key)
	if err != nil {
		return nil, err
	}

	if c.memory != nil {
		return c.memory.get(ctx, fullKey)
	}

	return c.redisGet(ctx, fullKey)
}

// Set replaces one value and its TTL. Zero TTL uses DefaultTTL; other TTLs must
// be at least 1ms and are rounded up to whole milliseconds on both backends.
// Nil and empty values both store an empty value. Values are copied before return.
func (c *Client) Set(ctx context.Context, key string, value []byte, ttl time.Duration) (err error) {
	ctx, finish := c.begin(ctx, "set")
	defer func() { finish(err) }()

	fullKey, err := c.key(ctx, key)
	if err != nil {
		return err
	}

	if len(value) > c.cfg.MaxValueBytes {
		return ErrValueTooLarge
	}

	if ttl == 0 {
		ttl = c.cfg.DefaultTTL
	}

	ttl, err = normalizeTTL(ttl)
	if err != nil {
		return err
	}

	if c.memory != nil {
		return c.memory.set(ctx, fullKey, value, ttl)
	}

	if err := c.redis.Set(ctx, fullKey, value, ttl).Err(); err != nil {
		return c.redisError(ctx, err)
	}

	return nil
}

// Delete removes one entry. Deleting an absent or expired entry succeeds.
func (c *Client) Delete(ctx context.Context, key string) (err error) {
	ctx, finish := c.begin(ctx, "delete")
	defer func() { finish(err) }()

	fullKey, err := c.key(ctx, key)
	if err != nil {
		return err
	}

	if c.memory != nil {
		return c.memory.delete(ctx, fullKey)
	}

	if err := c.redis.Del(ctx, fullKey).Err(); err != nil {
		return c.redisError(ctx, err)
	}

	return nil
}

// Ping checks the client's health without writing values. For Redis it checks
// connectivity and authentication, not every data command's ACL permission.
// Register with health.Optional for a cache the service can operate without.
func (c *Client) Ping(ctx context.Context) (err error) {
	ctx, finish := c.begin(ctx, "ping")
	defer func() { finish(err) }()

	if c.closed.Load() {
		return ErrClosed
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if c.redis != nil {
		if err := c.redis.Ping(ctx).Err(); err != nil {
			return c.redisError(ctx, err)
		}
	}

	return nil
}

// Close is idempotent, releases memory and Redis connections, and rejects future
// operations. In-flight Redis operations may fail. It never deletes Redis data
// or shuts down borrowed telemetry providers.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		if c.memory != nil {
			c.memory.close()
		}

		if c.redis != nil {
			if err := c.redis.Close(); err != nil {
				c.closeErr = ErrUnavailable
			}
		}
	})

	return c.closeErr
}

func (c *Client) key(ctx context.Context, key string) (string, error) {
	if c.closed.Load() {
		return "", ErrClosed
	}

	if err := ctx.Err(); err != nil {
		return "", err
	}

	if strings.TrimSpace(key) == "" || len(key) > 1024 || !utf8.ValidString(key) || strings.ContainsAny(key, "\r\n\x00") {
		return "", errors.New("cache: key must contain 1–1024 UTF-8 bytes without control delimiters")
	}

	return "gof:cache:" + c.cfg.Namespace + ":" + key, nil
}
