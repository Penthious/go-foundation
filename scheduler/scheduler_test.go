package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestScheduler_LifecycleAndConfigurationSnapshot(t *testing.T) {
	entered, finished := make(chan Run, 1), make(chan struct{})
	jobs := []Job{{Name: "cleanup", Every: 5 * time.Millisecond, Handler: func(ctx context.Context, run Run) error {
		entered <- run
		<-ctx.Done()
		close(finished)

		return ctx.Err()
	}}}
	worker, err := New(testConfig(t), jobs...)
	if err != nil {
		t.Fatal(err)
	}

	jobs[0].Handler = func(context.Context, Run) error { panic("mutated") }
	if err := worker.Ping(t.Context()); !errors.Is(err, ErrNotStarted) {
		t.Fatal("unstarted scheduler is healthy")
	}

	startup, cancel := context.WithCancel(t.Context())
	var starts sync.WaitGroup
	for range 10 {
		starts.Go(func() {
			if err := worker.Start(startup); err != nil {
				t.Error(err)
			}
		})
	}

	starts.Wait()
	cancel()
	run := receive(t, entered)
	if run.Name != "cleanup" || run.Namespace != "service" || len(run.ID) != 64 || run.ScheduledAt.IsZero() {
		t.Fatalf("invalid run metadata: %+v", run)
	}

	select {
	case <-finished:
		t.Fatal("startup context canceled a service-lifetime handler")
	default:
	}

	if err := worker.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := worker.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}

	_ = receive(t, finished)
	if err := worker.Start(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatal("closed scheduler restarted")
	}

	if err := worker.Ping(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatal("closed scheduler is healthy")
	}

	if err := worker.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestScheduler_ShutdownDeadlineAndNeverStarted(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	worker, err := New(testConfig(t), Job{Name: "blocked", Every: time.Millisecond, Handler: func(context.Context, Run) error {
		close(entered)
		<-release

		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}

	if err := worker.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	_ = receive(t, entered)
	deadline, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()

	if err := worker.Shutdown(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("noncooperative handler ignored shutdown deadline: %v", err)
	}

	close(release)
	if err := worker.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}

	other, err := New(testConfig(t), Job{Name: "unused", Every: time.Hour, Handler: func(context.Context, Run) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}

	if err := other.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := other.Start(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatal("shutdown before Start did not close scheduler")
	}
}

func receive[T any](t *testing.T, channel <-chan T) T {
	t.Helper()

	select {
	case value := <-channel:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for scheduler")
	}

	var zero T

	return zero
}
