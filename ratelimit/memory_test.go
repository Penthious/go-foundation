package ratelimit

import (
	"crypto/sha256"
	"errors"
	"testing"
	"time"
)

func TestMemory_RefillBurstAndDeniedChecks(t *testing.T) {
	cfg := testConfig(t, Memory)
	cfg.Limit, cfg.Window, cfg.Burst = 2, time.Second, 3
	limiter := testLimiter(t, cfg)
	now := time.Unix(1000, 0)
	limiter.memory.now = func() time.Time { return now }
	for remaining := 2; remaining >= 0; remaining-- {
		decision, err := limiter.Allow(t.Context(), "alice")
		if err != nil || !decision.Allowed || decision.Remaining != remaining || decision.RetryAfter != 0 {
			t.Fatal("unexpected burst decision:", decision, err)
		}
	}

	for range 3 {
		decision, err := limiter.Allow(t.Context(), "alice")
		if err != nil || decision.Allowed || decision.Remaining != 0 || decision.RetryAfter != 500*time.Millisecond {
			t.Fatal("denial changed the quota/refill time:", decision, err)
		}
	}

	now = now.Add(500*time.Millisecond - time.Nanosecond)
	decision, err := limiter.Allow(t.Context(), "alice")
	if err != nil || decision.Allowed || decision.RetryAfter != time.Nanosecond {
		t.Fatal("refilled a token too early:", decision, err)
	}

	now = now.Add(time.Nanosecond)
	decision, err = limiter.Allow(t.Context(), "alice")
	if err != nil || !decision.Allowed || decision.Remaining != 0 {
		t.Fatal("did not refill exactly one token:", decision, err)
	}

	now = now.Add(3 * time.Second)
	decision, err = limiter.Allow(t.Context(), "alice")
	if err != nil || !decision.Allowed || decision.Remaining != 2 {
		t.Fatal("idle bucket did not fully refill:", decision, err)
	}
}

func TestMemory_CapacityNeverResetsActiveQuotas(t *testing.T) {
	cfg := testConfig(t, Memory)
	cfg.Limit, cfg.Window, cfg.Burst, cfg.MaxKeys = 1, time.Second, 2, 2
	limiter := testLimiter(t, cfg)
	now := time.Unix(1000, 0)
	limiter.memory.now = func() time.Time { return now }
	for _, identity := range []string{"alice", "bob", "alice"} {
		decision, err := limiter.Allow(t.Context(), identity)
		if err != nil || !decision.Allowed {
			t.Fatal(err)
		}
	}

	if _, err := limiter.Allow(t.Context(), "charlie"); !errors.Is(err, ErrCapacity) {
		t.Fatal("active quota evicted to admit a new identity:", err)
	}

	now = now.Add(time.Second)
	decision, err := limiter.Allow(t.Context(), "charlie")
	if err != nil || !decision.Allowed || len(limiter.memory.entries) != 2 {
		t.Fatal("fully refilled identity was not expired:", decision, err)
	}

	if limiter.memory.entries[sha256.Sum256([]byte("alice"))] == nil {
		t.Fatal("still-active Alice quota was evicted")
	}

	decision, err = limiter.Allow(t.Context(), "alice")
	if err != nil || !decision.Allowed || decision.Remaining != 0 {
		t.Fatal("active quota received a fresh burst:", decision, err)
	}

	other := testLimiter(t, cfg)
	decision, err = other.Allow(t.Context(), "alice")
	if err != nil || !decision.Allowed || decision.Remaining != 1 {
		t.Fatal("memory instances unexpectedly shared state")
	}
}
