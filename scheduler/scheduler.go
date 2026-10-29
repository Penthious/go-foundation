// Package scheduler runs service-owned jobs locally or with PostgreSQL coordination.
package scheduler

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// Lifecycle and coordination failures, suitable for errors.Is.
var (
	ErrClosed           = errors.New("scheduler: scheduler is closed")
	ErrNotStarted       = errors.New("scheduler: scheduler is not started")
	ErrScheduleConflict = errors.New("scheduler: coordinated schedule configuration differs")
	ErrLeaseLost        = errors.New("scheduler: run lease no longer belongs to this worker")
)

// Job describes one immutable schedule. Names use 1–128 ASCII identifier
// characters (letters, digits, dot, underscore, hyphen). Cron uses five fields;
// Every uses fixed, globally aligned intervals. Set exactly one.
type Job struct {
	Name  string
	Mode  Mode
	Cron  string
	Every time.Duration
	// Timezone defaults to UTC and affects calendar schedules, not fixed intervals.
	Timezone string
	// Timeout defaults to 5m. Handlers must honor cancellation and return; Go
	// cannot forcibly stop them. Do not retain contexts or start unjoined work.
	Timeout time.Duration
	Handler func(context.Context, Run) error
}

// Run identifies an occurrence. Coordinated IDs are stable across replicas for
// the same namespace, job and scheduled time. Local IDs also include instance
// identity. External effects still require service-owned idempotency.
type Run struct {
	ID          string
	Namespace   string
	Name        string
	ScheduledAt time.Time
}

type activation struct {
	ready   chan struct{}
	stopped chan struct{}
	cancel  context.CancelFunc
	err     error
}

// Scheduler owns its workers, but borrows the logger, database and global OTel
// providers. Construct with New; call Shutdown before closing dependencies.
type Scheduler struct {
	cfg      Config
	jobs     []job
	instance string
	slots    chan struct{}
	mu       sync.Mutex
	active   *activation
	started  bool
	closed   bool
	runs     metric.Int64Counter
	duration metric.Float64Histogram
}

// New snapshots and validates jobs without network I/O or starting workers.
// Initialize telemetry first. Apply Schema before starting coordinated jobs.
func New(cfg Config, jobs ...Job) (*Scheduler, error) {
	cfg, parsed, err := normalizeConfig(cfg, jobs)
	if err != nil {
		return nil, err
	}

	meter := otel.Meter("go-foundation/scheduler")
	runs, err := meter.Int64Counter("scheduler.runs", metric.WithDescription("Scheduled job outcomes and capacity skips"))
	if err != nil {
		return nil, fmt.Errorf("scheduler: create run counter: %w", err)
	}

	duration, err := meter.Float64Histogram("scheduler.run.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, fmt.Errorf("scheduler: create duration histogram: %w", err)
	}

	return &Scheduler{cfg: cfg, jobs: parsed, instance: rand.Text(), slots: make(chan struct{}, cfg.MaxConcurrentJobs), runs: runs, duration: duration}, nil
}

// Start registers coordinated schedules transactionally, then launches workers.
// It is idempotent after success. The startup context does not own worker lifetime.
// Registration errors allow retrying Start. Shutdown permanently closes the scheduler.
func (s *Scheduler) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}

	if current := s.active; current != nil {
		s.mu.Unlock()
		select {
		case <-current.ready:
			return current.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	runCtx, cancel := context.WithCancel(context.Background())
	current := &activation{ready: make(chan struct{}), stopped: make(chan struct{}), cancel: cancel}
	s.active = current
	s.mu.Unlock()

	startupCtx, stopStartup := context.WithCancel(ctx)
	stop := context.AfterFunc(runCtx, stopStartup)
	err := s.register(startupCtx)
	stop()
	stopStartup()

	s.mu.Lock()
	defer s.mu.Unlock()

	if err == nil {
		err = ctx.Err()
	}

	if s.closed && err == nil {
		err = ErrClosed
	}

	current.err = err
	if err != nil {
		cancel()
		s.active = nil
		close(current.ready)
		close(current.stopped)
		return err
	}

	s.started = true
	var workers sync.WaitGroup
	for _, entry := range s.jobs {
		workers.Go(func() { s.worker(runCtx, entry) })
	}

	go func() {
		workers.Wait()
		close(current.stopped)
	}()
	close(current.ready)

	return nil
}

// Shutdown prevents new runs, cancels handlers, and joins workers. If ctx expires
// it returns ctx.Err(); handlers may still be running and dependencies must remain
// usable until they return. Shutdown can be called again to wait for completion.
func (s *Scheduler) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closed, s.started = true, false
	current := s.active
	if current != nil {
		current.cancel()
	}

	s.mu.Unlock()
	if current == nil {
		return nil
	}

	select {
	case <-current.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Ping checks lifecycle state and, when needed, coordination storage. It is a
// read-only health callback; it does not indicate that recent handlers succeeded.
func (s *Scheduler) Ping(ctx context.Context) error {
	s.mu.Lock()
	closed, started := s.closed, s.started
	s.mu.Unlock()
	if closed {
		return ErrClosed
	}

	if !started {
		return ErrNotStarted
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if s.coordinated() {
		return s.checkStorage(ctx)
	}

	return nil
}

func (s *Scheduler) coordinated() bool {
	for _, entry := range s.jobs {
		if entry.Mode == Coordinated {
			return true
		}
	}

	return false
}

func (s *Scheduler) occurrence(entry job, at time.Time) Run {
	identity := s.cfg.Namespace + ":" + entry.Name + ":" + at.UTC().Format(time.RFC3339Nano)
	if entry.Mode == Local {
		identity += ":" + s.instance
	}

	return Run{ID: fmt.Sprintf("%x", sha256.Sum256([]byte(identity))), Namespace: s.cfg.Namespace, Name: entry.Name, ScheduledAt: at.UTC()}
}
