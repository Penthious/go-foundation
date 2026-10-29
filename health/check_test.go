package health_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/health"
)

func TestChecker_RequiredAndOptional(t *testing.T) {
	for _, scenario := range []struct {
		name          string
		requiredError error
		optionalError error
		wantStatus    health.Status
		wantReady     bool
	}{
		{"healthy", nil, nil, health.Up, true},
		{"optional down", nil, errors.New("optional down"), health.Degraded, true},
		{"required down", errors.New("required down"), nil, health.Down, false},
		{"both down", errors.New("required down"), errors.New("optional down"), health.Down, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var optionalCalls atomic.Int32
			checker, err := health.New(
				health.Optional("cache", func(context.Context) error { optionalCalls.Add(1); return scenario.optionalError }),
				health.Required("database", func(context.Context) error { return scenario.requiredError }),
			)
			if err != nil {
				t.Fatal(err)
			}

			ready := checker.Readiness(t.Context())
			if ready.Ready != scenario.wantReady || optionalCalls.Load() != 0 || len(ready.Checks) != 1 {
				t.Errorf("readiness ran optional checks or returned wrong readiness: %+v", ready)
			}

			report := checker.Check(t.Context())
			if report.Status != scenario.wantStatus || report.Ready != scenario.wantReady || len(report.Checks) != 2 || report.Checks[0].Name != "database" || optionalCalls.Load() != 1 || report.CheckedAt.IsZero() {
				t.Errorf("unexpected full report: %+v", report)
			}
		})
	}
}

func TestChecker_TimeoutPreservesRequiredReadinessAndListsUnchecked(t *testing.T) {
	checker, err := health.New(
		health.Optional("slow", func(ctx context.Context) error { <-ctx.Done(); return nil }),
		health.Required("database", func(context.Context) error { return nil }),
		health.Optional("later", func(context.Context) error { t.Error("called after deadline"); return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	report := checker.Check(ctx)

	if !report.Ready || report.Status != health.Degraded || report.Checks[0].Name != "database" || report.Checks[1].Error != "timed out" || report.Checks[2].Status != health.Unknown {
		t.Errorf("optional timeout altered required readiness or hid remaining checks: %+v", report)
	}
}

func TestChecker_CancellationAndPanic(t *testing.T) {
	var calls atomic.Int32
	checker, err := health.New(
		health.Required("panic", func(context.Context) error { panic("private-panic") }),
		health.Required("later", func(context.Context) error { calls.Add(1); return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}

	report := checker.Check(t.Context())
	if report.Ready || report.Checks[0].Error != "check panicked" || calls.Load() != 1 {
		t.Errorf("panic prevented the remaining checks: %+v", report)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	canceled := checker.Readiness(ctx)
	if canceled.Ready || canceled.Checks[0].Status != health.Unknown || calls.Load() != 1 {
		t.Errorf("canceled request still ran checks: %+v", canceled)
	}
}

func TestChecker_JSONHidesRawErrors(t *testing.T) {
	private := errors.New("password=private-password")
	checker, err := health.New(health.Required("database", func(context.Context) error { return private }))
	if err != nil {
		t.Fatal(err)
	}

	report := checker.Check(t.Context())
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(string(data), "private-password") || !errors.Is(report.Checks[0].Err, private) || report.Checks[0].Error != "check failed" {
		t.Errorf("raw error lost or exposed publicly: %s", data)
	}
}

func TestNew_ValidationAndCopies(t *testing.T) {
	up := func(context.Context) error { return nil }
	for _, checks := range [][]health.Check{
		{health.Required("", up)},
		{health.Required("db", nil)},
		{health.Check{}},
		{health.Required("db", up), health.Optional("db", up)},
	} {
		if _, err := health.New(checks...); err == nil {
			t.Error("accepted invalid check registrations")
		}
	}

	checks := []health.Check{health.Required("db", up)}
	checker, err := health.New(checks...)
	if err != nil {
		t.Fatal(err)
	}

	checks[0] = health.Required("changed", func(context.Context) error { return errors.New("down") })
	if report := checker.Check(t.Context()); !report.Ready || report.Checks[0].Name != "db" {
		t.Error("caller mutation changed registrations")
	}

	var empty health.Checker
	if report := empty.Check(t.Context()); !report.Ready || report.Status != health.Up || report.Checks == nil {
		t.Errorf("zero value report = %+v", report)
	}
}

func TestChecker_ConcurrentReports(t *testing.T) {
	var calls atomic.Int32
	checker, err := health.New(health.Required("db", func(context.Context) error { calls.Add(1); return nil }))
	if err != nil {
		t.Fatal(err)
	}

	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			if report := checker.Check(t.Context()); !report.Ready {
				t.Error("unexpected unhealthy report")
			}
		})
	}

	workers.Wait()
	if calls.Load() != 8 {
		t.Errorf("check calls = %d", calls.Load())
	}
}
