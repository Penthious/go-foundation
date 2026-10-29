package cache

import (
	"errors"
	"testing"
	"time"
)

func TestClient_MemoryExpiryAndTTLReplacement(t *testing.T) {
	client := testClient(t, Config{Namespace: "expiry", DefaultTTL: time.Minute})
	now := time.Now()
	client.memory.now = func() time.Time { return now }
	if err := client.Set(t.Context(), "key", []byte("first"), 0); err != nil {
		t.Fatal(err)
	}

	now = now.Add(30 * time.Second)
	if err := client.Set(t.Context(), "key", []byte("replacement"), 2*time.Minute); err != nil {
		t.Fatal(err)
	}

	now = now.Add(time.Minute)
	value, err := client.Get(t.Context(), "key")
	if err != nil || string(value) != "replacement" {
		t.Fatal("Set did not replace the previous value and expiration")
	}

	now = now.Add(time.Minute)
	if _, err := client.Get(t.Context(), "key"); !errors.Is(err, ErrMiss) {
		t.Fatal("entry survived exact expiration")
	}

	if client.memory.bytes != 0 || client.memory.lru.Len() != 0 {
		t.Fatal("expired entry storage not reclaimed on Get")
	}

	if err := client.Set(t.Context(), "precision", nil, 1500*time.Microsecond); err != nil {
		t.Fatal(err)
	}

	now = now.Add(1500 * time.Microsecond)
	if _, err := client.Get(t.Context(), "precision"); err != nil {
		t.Fatal("TTL was rounded down instead of up")
	}

	now = now.Add(500 * time.Microsecond)
	if _, err := client.Get(t.Context(), "precision"); !errors.Is(err, ErrMiss) {
		t.Fatal("rounded TTL did not expire")
	}
}

func TestClient_MemoryLRUEvictionAndExpiredReclamation(t *testing.T) {
	client := testClient(t, Config{Namespace: "lru", Memory: MemoryConfig{MaxEntries: 2}})
	now := time.Now()
	client.memory.now = func() time.Time { return now }
	for _, key := range []string{"first", "second"} {
		if err := client.Set(t.Context(), key, nil, time.Minute); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := client.Get(t.Context(), "first"); err != nil {
		t.Fatal(err)
	}

	if err := client.Set(t.Context(), "third", nil, time.Minute); err != nil {
		t.Fatal(err)
	}

	if _, err := client.Get(t.Context(), "second"); !errors.Is(err, ErrMiss) {
		t.Fatal("least recently used entry was not evicted")
	}

	if err := client.Set(t.Context(), "first", nil, time.Millisecond); err != nil {
		t.Fatal(err)
	}

	now = now.Add(time.Second)
	if err := client.Set(t.Context(), "fourth", nil, time.Minute); err != nil {
		t.Fatal(err)
	}

	if _, err := client.Get(t.Context(), "third"); err != nil {
		t.Fatal("live LRU entry evicted while an expired MRU entry remained")
	}
}

func TestClient_MemoryByteBudgetAndInstanceIsolation(t *testing.T) {
	client := testClient(t, Config{Namespace: "bytes", Memory: MemoryConfig{MaxBytes: 40}})
	if err := client.Set(t.Context(), "a", []byte("small"), 0); err != nil {
		t.Fatal(err)
	}

	if err := client.Set(t.Context(), "a", make([]byte, 40), 0); !errors.Is(err, ErrValueTooLarge) {
		t.Fatal("oversized entry accepted")
	}

	value, err := client.Get(t.Context(), "a")
	if err != nil || string(value) != "small" {
		t.Fatal("oversized replacement destroyed old entry")
	}

	if err := client.Set(t.Context(), "b", []byte("small"), 0); err != nil {
		t.Fatal(err)
	}

	if client.memory.bytes > 40 || client.memory.lru.Len() != 1 {
		t.Fatal("byte budget did not evict an entry")
	}

	other := testClient(t, Config{Namespace: "bytes"})
	if _, err := other.Get(t.Context(), "b"); !errors.Is(err, ErrMiss) {
		t.Fatal("memory cache unexpectedly shared between clients")
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	if client.memory.bytes != 0 || client.memory.lru.Len() != 0 || client.memory.entries != nil {
		t.Fatal("Close did not release memory")
	}
}
