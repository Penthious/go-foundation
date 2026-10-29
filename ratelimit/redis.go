package ratelimit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
)

// GCRA stores bounded integer microseconds, using Redis time for every replica.
// Policy mismatches fail closed until the old bucket fully refills/expires.
var takeToken = redis.NewScript(`
local interval, burst = tonumber(ARGV[1]), tonumber(ARGV[2])
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000000 + tonumber(clock[2])
local tat = now
local raw = redis.call('GETRANGE', KEYS[1], 0, 95)
if #raw > 0 then
  if #raw >= 96 then return {-2, 0, 0} end
  local oldInterval, oldBurst, oldTat = string.match(raw, '^(%d+),(%d+),(%d+)$')
  if not oldTat then return {-2, 0, 0} end
  if tonumber(oldInterval) ~= interval or tonumber(oldBurst) ~= burst then return {-1, 0, 0} end
  tat = tonumber(oldTat)
  if not tat or tat > 9007199254740991 then return {-2, 0, 0} end
  if tat > now and tat - now > interval * burst then return {-2, 0, 0} end
  tat = math.max(tat, now)
elseif redis.call('EXISTS', KEYS[1]) ~= 0 then
  return {-2, 0, 0}
end
local next = tat + interval
local debt = next - now
local capacity = interval * burst
if debt > capacity then return {0, 0, debt - capacity} end
local value = ARGV[1] .. ',' .. ARGV[2] .. ',' .. string.format('%.0f', next)
redis.call('SET', KEYS[1], value, 'PX', math.ceil(debt / 1000) + 1)
return {1, math.floor((capacity - debt) / interval), 0}
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

func (l *Limiter) redisKey(key [sha256.Size]byte) string {
	return "gof:ratelimit:v1:" + l.cfg.Namespace + ":" + hex.EncodeToString(key[:])
}

func (l *Limiter) redisAllow(ctx context.Context, key [sha256.Size]byte) (Decision, error) {
	values, err := takeToken.Run(ctx, l.redis, []string{l.redisKey(key)}, l.interval.Microseconds(), l.cfg.Burst).Int64Slice()
	if err != nil {
		return Decision{}, l.redisError(ctx, err)
	}

	if len(values) != 3 || values[0] < -1 || values[0] > 1 || values[1] < 0 || values[1] >= int64(l.cfg.Burst) || values[2] < 0 || values[2] > (24*time.Hour).Microseconds() {
		return Decision{}, ErrUnavailable
	}

	if values[0] == -1 {
		return Decision{}, ErrPolicyMismatch
	}

	return Decision{Allowed: values[0] == 1, Remaining: int(values[1]), RetryAfter: time.Duration(values[2]) * time.Microsecond}, nil
}

func (l *Limiter) redisError(ctx context.Context, cause error) error {
	if l.closed.Load() {
		return ErrClosed
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	var timeout net.Error
	if errors.Is(cause, context.DeadlineExceeded) || errors.As(cause, &timeout) && timeout.Timeout() {
		return context.DeadlineExceeded
	}

	return ErrUnavailable
}
