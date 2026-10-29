package circuitbreaker

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/logger"
)

func TestNew_ValidationAndDefaults(t *testing.T) {
	if b, err := New(Config{}); b != nil || err == nil {
		t.Fatal("missing logger accepted")
	}

	for _, settings := range []struct {
		threshold int
		timeout   time.Duration
	}{{-1, 0}, {0, -time.Second}} {
		if b, err := New(Config{FailureThreshold: settings.threshold, OpenTimeout: settings.timeout, Logger: testLogger(t)}); b != nil || err == nil {
			t.Fatal("invalid breaker settings accepted")
		}
	}

	b, err := New(Config{Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}

	if b.failureThreshold != 5 || b.openTimeout != 30*time.Second {
		t.Fatal("unexpected defaults")
	}

	var disabled *Breaker
	permit, allowed := disabled.Allow(t.Context())
	if !allowed || permit != nil {
		t.Fatal("disabled breaker rejected a call")
	}

	permit.Done(Failure)
}

func TestBreaker_ConsecutiveFailuresAndRecovery(t *testing.T) {
	b, now := testBreaker(t, 2)
	admit(t, b).Done(Failure)
	admit(t, b).Done(Success)
	admit(t, b).Done(Failure)
	admit(t, b).Done(Ignored)
	admit(t, b).Done(Failure)

	if _, ok := b.Allow(t.Context()); ok {
		t.Fatal("two consecutive failures did not open the circuit")
	}

	*now = now.Add(30*time.Second - time.Nanosecond)
	if _, ok := b.Allow(t.Context()); ok {
		t.Fatal("circuit admitted a call before its timeout")
	}

	*now = now.Add(time.Nanosecond)
	probe := admit(t, b)
	if _, ok := b.Allow(t.Context()); ok {
		t.Fatal("circuit admitted another call during its probe")
	}

	probe.Done(Failure)
	*now = now.Add(29 * time.Second)
	if _, ok := b.Allow(t.Context()); ok {
		t.Fatal("failed probe did not start another full timeout")
	}

	*now = now.Add(time.Second)
	admit(t, b).Done(Ignored)
	admit(t, b).Done(Success)
	admit(t, b).Done(Failure)
	admit(t, b).Done(Success)
}

func TestBreaker_StaleAndRepeatedCompletions(t *testing.T) {
	for _, outcome := range []Outcome{Success, Failure, Ignored} {
		b, now := testBreaker(t, 1)
		first, stale := admit(t, b), admit(t, b)
		first.Done(Failure)
		first.Done(Success)
		if _, ok := b.Allow(t.Context()); ok {
			t.Fatal("repeated completion closed an open circuit")
		}

		*now = now.Add(30 * time.Second)
		probe := admit(t, b)
		stale.Done(outcome)
		if _, ok := b.Allow(t.Context()); ok {
			t.Fatal("stale completion changed the recovery probe")
		}

		probe.Done(Success)
		first.Done(Failure)
		stale.Done(Failure)
		admit(t, b).Done(Success)
	}
}

func TestBreaker_ConcurrentProbeAdmissionAndCompletion(t *testing.T) {
	b, now := testBreaker(t, 1)
	admit(t, b).Done(Failure)
	*now = now.Add(30 * time.Second)
	permits := make(chan *Permit, 100)
	var callers sync.WaitGroup
	for range 100 {
		callers.Go(func() {
			if permit, ok := b.Allow(t.Context()); ok {
				permits <- permit
			}
		})
	}

	callers.Wait()
	close(permits)
	if len(permits) != 1 {
		t.Fatalf("concurrent recovery probes = %d, want one", len(permits))
	}

	probe := <-permits
	for range 100 {
		callers.Go(func() { probe.Done(Success) })
	}

	callers.Wait()
	admit(t, b).Done(Success)
}

func testBreaker(t *testing.T, threshold int) (*Breaker, *time.Time) {
	t.Helper()

	b, err := New(Config{FailureThreshold: threshold, OpenTimeout: 30 * time.Second, Logger: testLogger(t), Protocol: "test"})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Unix(1000, 0)
	b.now = func() time.Time { return now }

	return b, &now
}

func admit(t *testing.T, b *Breaker) *Permit {
	t.Helper()

	permit, allowed := b.Allow(t.Context())
	if !allowed {
		t.Fatal("circuit rejected an expected call")
	}

	return permit
}

func testLogger(t *testing.T) *logger.Logger {
	t.Helper()

	log, err := logger.New(logger.Config{Service: "breaker-test", Output: io.Discard})
	if err != nil {
		t.Fatal(err)
	}

	return log
}
