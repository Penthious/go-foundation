package ratelimit

import (
	"container/heap"
	"context"
	"crypto/sha256"
	"sync"
	"time"
)

type memoryStore struct {
	mu      sync.Mutex
	entries map[[sha256.Size]byte]*memoryEntry
	expiry  expiryHeap
	now     func() time.Time
}

type memoryEntry struct {
	key   [sha256.Size]byte
	tat   time.Time
	index int
}

func (l *Limiter) memoryAllow(ctx context.Context, key [sha256.Size]byte) (Decision, error) {
	m := l.memory
	m.mu.Lock()
	defer m.mu.Unlock()

	if l.closed.Load() {
		return Decision{}, ErrClosed
	}

	if err := ctx.Err(); err != nil {
		return Decision{}, err
	}

	now := m.now()
	for len(m.expiry) > 0 && !now.Before(m.expiry[0].tat) {
		if err := ctx.Err(); err != nil {
			return Decision{}, err
		}

		entry := heap.Pop(&m.expiry).(*memoryEntry)
		delete(m.entries, entry.key)
	}

	entry := m.entries[key]
	if entry == nil {
		if len(m.entries) >= l.cfg.MaxKeys {
			return Decision{}, ErrCapacity
		}

		entry = &memoryEntry{key: key, tat: now}
		m.entries[key] = entry
		heap.Push(&m.expiry, entry)
	}

	next := entry.tat.Add(l.interval)
	debt := next.Sub(now)
	capacity := time.Duration(l.cfg.Burst) * l.interval
	if debt > capacity {
		return Decision{RetryAfter: debt - capacity}, nil
	}

	entry.tat = next
	heap.Fix(&m.expiry, entry.index)

	return Decision{Allowed: true, Remaining: int((capacity - debt) / l.interval)}, nil
}

// Entries expire only when fully refilled. Active quotas are never evicted.
type expiryHeap []*memoryEntry

func (h expiryHeap) Len() int {
	return len(h)
}

func (h expiryHeap) Less(i, j int) bool {
	return h[i].tat.Before(h[j].tat)
}

func (h expiryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

func (h *expiryHeap) Push(value any) {
	entry := value.(*memoryEntry)
	entry.index = len(*h)
	*h = append(*h, entry)
}

func (h *expiryHeap) Pop() any {
	last := len(*h) - 1
	entry := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]

	return entry
}
