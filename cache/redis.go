package cache

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
)

// Read at most limit+1 bytes while atomically distinguishing a missing key from
// an empty value. A value written by another client cannot force an unbounded reply.
var readValue = redis.NewScript(`
local value = redis.call('GETRANGE', KEYS[1], 0, ARGV[1])
if #value == 0 and redis.call('EXISTS', KEYS[1]) == 0 then return nil end
return value
`)

func newRedisClient(cfg RedisConfig, timeout time.Duration) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: cfg.Address, Username: cfg.Username, Password: cfg.Password, DB: cfg.DB,
		TLSConfig: cfg.TLS, PoolSize: cfg.PoolSize, MaxActiveConns: cfg.PoolSize,
		PipelinePoolSize: -1, MaxRetries: -1, DialerRetries: 1,
		DialTimeout: timeout, ReadTimeout: timeout, WriteTimeout: timeout, PoolTimeout: timeout,
		ContextTimeoutEnabled: true,
	})
}

func (c *Client) redisGet(ctx context.Context, key string) ([]byte, error) {
	value, err := readValue.Run(ctx, c.redis, []string{key}, c.cfg.MaxValueBytes).Text()
	if errors.Is(err, redis.Nil) {
		return nil, ErrMiss
	}

	if err != nil {
		return nil, c.redisError(ctx, err)
	}

	if len(value) > c.cfg.MaxValueBytes {
		return nil, ErrValueTooLarge
	}

	return []byte(value), nil
}

func (c *Client) redisError(ctx context.Context, cause error) error {
	if c.closed.Load() {
		return ErrClosed
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	var timeout net.Error
	if errors.Is(cause, context.DeadlineExceeded) || errors.As(cause, &timeout) && timeout.Timeout() {
		return context.DeadlineExceeded
	}

	// Driver errors can contain server-supplied keys, commands or credentials.
	return ErrUnavailable
}
