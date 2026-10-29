// Package circuitbreaker owns the shared, process-local client breaker state.
package circuitbreaker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Penthious/go-foundation/logger"
)

// Outcome distinguishes availability failures from local/canceled operations.
type Outcome uint8

const (
	Ignored Outcome = iota
	Success
	Failure
)

type state uint8

const (
	closed state = iota
	open
	halfOpen
)

// Breaker admits calls while healthy and a single probe after an open timeout.
type Breaker struct {
	mu               sync.Mutex
	state            state
	generation       uint64
	failures         int
	failureThreshold int
	openTimeout      time.Duration
	openUntil        time.Time
	now              func() time.Time
	observer         *observer
}

// Config includes stable client identity for circuit telemetry.
type Config struct {
	FailureThreshold int
	OpenTimeout      time.Duration
	Name             string
	Protocol         string
	Logger           *logger.Logger
}

// New validates settings; zero values select five failures and 30 seconds.
func New(cfg Config) (*Breaker, error) {
	if cfg.FailureThreshold < 0 || cfg.OpenTimeout < 0 {
		return nil, errors.New("circuit breaker: failure threshold and open timeout must not be negative")
	}

	if cfg.FailureThreshold == 0 {
		cfg.FailureThreshold = 5
	}

	if cfg.OpenTimeout == 0 {
		cfg.OpenTimeout = 30 * time.Second
	}

	observed, err := newObserver(cfg)
	if err != nil {
		return nil, err
	}

	return &Breaker{failureThreshold: cfg.FailureThreshold, openTimeout: cfg.OpenTimeout, now: time.Now, observer: observed}, nil
}

// Permit records one admitted call. Done is safe to call more than once.
type Permit struct {
	breaker    *Breaker
	generation uint64
	done       atomic.Bool
	ctx        context.Context
}

// Allow reserves a call. A nil breaker is disabled and admits without a permit.
func (b *Breaker) Allow(ctx context.Context) (*Permit, bool) {
	if b == nil {
		return nil, true
	}

	b.mu.Lock()

	if b.state == halfOpen || b.state == open && b.now().Before(b.openUntil) {
		current, generation := b.state, b.generation
		b.mu.Unlock()
		b.observer.rejected(ctx, current, generation)

		return nil, false
	}

	previous := b.state
	if b.state == open {
		b.state = halfOpen
		b.generation++
	}

	permit := &Permit{breaker: b, generation: b.generation, ctx: ctx}
	b.unlockAndObserve(ctx, previous)

	return permit, true
}

// Done ignores stale calls from before a state transition and nil permits.
func (p *Permit) Done(outcome Outcome) {
	if p == nil || !p.done.CompareAndSwap(false, true) {
		return
	}

	b := p.breaker
	b.mu.Lock()

	if p.generation != b.generation {
		b.mu.Unlock()

		return
	}

	previous := b.state
	if b.state == halfOpen {
		switch outcome {
		case Failure:
			b.trip()
		case Success:
			b.state, b.failures = closed, 0
			b.generation++
		case Ignored:
			// A canceled/local probe releases its slot without extending cooldown.
			b.state, b.openUntil = open, time.Time{}
			b.generation++
		}
	} else {
		switch outcome {
		case Success:
			b.failures = 0
		case Failure:
			b.failures++
			if b.failures >= b.failureThreshold {
				b.trip()
			}
		}
	}

	b.unlockAndObserve(p.ctx, previous)
}

// Snapshot under the lock, then release it before calling telemetry or user I/O.
func (b *Breaker) unlockAndObserve(ctx context.Context, previous state) {
	current, generation := b.state, b.generation
	b.mu.Unlock()

	if previous != current {
		b.observer.transition(ctx, previous, current, generation)
	}
}

func (b *Breaker) trip() {
	b.state = open
	b.openUntil = b.now().Add(b.openTimeout)
	b.generation++
}
