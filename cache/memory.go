package cache

import (
	"container/list"
	"context"
	"sync"
	"time"
)

type memoryEntry struct {
	key       string
	value     []byte
	expiresAt time.Time
}

type memoryStore struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	lru     list.List
	bytes   int64
	limits  MemoryConfig
	closed  bool
	now     func() time.Time
}

func newMemoryStore(cfg MemoryConfig) *memoryStore {
	return &memoryStore{entries: make(map[string]*list.Element), limits: cfg, now: time.Now}
}

func (s *memoryStore) get(ctx context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.check(ctx); err != nil {
		return nil, err
	}

	element := s.entries[key]
	if element == nil {
		return nil, ErrMiss
	}

	entry := element.Value.(*memoryEntry)
	if !s.now().Before(entry.expiresAt) {
		s.remove(element)
		return nil, ErrMiss
	}

	s.lru.MoveToFront(element)

	return append([]byte{}, entry.value...), nil
}

func (s *memoryStore) set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.check(ctx); err != nil {
		return err
	}

	size := int64(len(key)) + int64(len(value))
	if size > s.limits.MaxBytes {
		return ErrValueTooLarge
	}

	if existing := s.entries[key]; existing != nil {
		s.remove(existing)
	}

	now := s.now()
	if len(s.entries) >= s.limits.MaxEntries || s.bytes > s.limits.MaxBytes-size {
		for _, element := range s.entries {
			if !now.Before(element.Value.(*memoryEntry).expiresAt) {
				s.remove(element)
			}
		}
	}

	for len(s.entries) >= s.limits.MaxEntries || s.bytes > s.limits.MaxBytes-size {
		s.remove(s.lru.Back())
	}

	entry := &memoryEntry{key: key, value: append([]byte{}, value...), expiresAt: now.Add(ttl)}
	s.entries[key] = s.lru.PushFront(entry)
	s.bytes += size

	return nil
}

func (s *memoryStore) delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.check(ctx); err != nil {
		return err
	}

	if element := s.entries[key]; element != nil {
		s.remove(element)
	}

	return nil
}

func (s *memoryStore) check(ctx context.Context) error {
	if s.closed {
		return ErrClosed
	}

	return ctx.Err()
}

func (s *memoryStore) remove(element *list.Element) {
	entry := element.Value.(*memoryEntry)
	s.bytes -= int64(len(entry.key)) + int64(len(entry.value))
	delete(s.entries, entry.key)
	s.lru.Remove(element)
}

func (s *memoryStore) close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	s.entries = nil
	s.lru.Init()
	s.bytes = 0
}
