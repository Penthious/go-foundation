// Package health checks arbitrary service dependencies through context-aware functions.
package health

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Status describes a dependency or an aggregate health report.
type Status string

var errCheckPanic = errors.New("health: check panicked")

// Health statuses used in reports and individual check results.
const (
	Up       Status = "up"
	Down     Status = "down"
	Degraded Status = "degraded"
	Unknown  Status = "unknown"
)

// Check is a named function registered with Required or Optional.
type Check struct {
	name     string
	required bool
	run      func(context.Context) error
}

// Required registers a dependency whose failure prevents readiness.
func Required(name string, run func(context.Context) error) Check {
	return Check{name: name, required: true, run: run}
}

// Optional registers a dependency whose failure degrades health without blocking readiness.
func Optional(name string, run func(context.Context) error) Check {
	return Check{name: name, run: run}
}

// Result contains a check's safe public status and duration. Err retains its
// original error for server-side diagnostics only; it is never JSON-encoded.
type Result struct {
	Name       string  `json:"name"`
	Required   bool    `json:"required"`
	Status     Status  `json:"status"`
	DurationMS float64 `json:"duration_ms"`
	Error      string  `json:"error,omitempty"`
	Err        error   `json:"-"`
}

// Report summarizes a snapshot. Ready depends only on required checks. Optional
// failures produce Degraded; failed or unchecked required dependencies produce Down.
type Report struct {
	Status    Status    `json:"status"`
	Ready     bool      `json:"ready"`
	CheckedAt time.Time `json:"checked_at"`
	Checks    []Result  `json:"checks"`
}

// Checker holds immutable check registrations and is safe to share. Check
// functions must be concurrency-safe, honor cancellation, and avoid side effects.
// The zero value is usable with no dependencies.
type Checker struct {
	checks []Check
}

// New validates and copies check registrations. Required checks run first so
// optional checks cannot consume the caller's timeout before readiness is checked.
func New(checks ...Check) (*Checker, error) {
	checks = slices.Clone(checks)
	names := make(map[string]bool, len(checks))
	for _, check := range checks {
		if strings.TrimSpace(check.name) == "" || check.run == nil {
			return nil, errors.New("health: each check needs a name and function")
		}

		if names[check.name] {
			return nil, fmt.Errorf("health: duplicate check %q", check.name)
		}

		names[check.name] = true
	}

	slices.SortStableFunc(checks, func(a, b Check) int {
		if a.required == b.required {
			return 0
		}

		if a.required {
			return -1
		}

		return 1
	})

	return &Checker{checks: checks}, nil
}

// Check runs every dependency sequentially under ctx's shared budget. A panic is
// recorded as a failed check; remaining checks still run. Callers set deadlines.
func (c *Checker) Check(ctx context.Context) Report {
	return c.run(ctx, false)
}

// Readiness runs only required dependencies. Optional functions are never called.
func (c *Checker) Readiness(ctx context.Context) Report {
	return c.run(ctx, true)
}

func (c *Checker) run(ctx context.Context, requiredOnly bool) Report {
	report := Report{Status: Up, Ready: true, Checks: make([]Result, 0, len(c.checks))}
	for _, check := range c.checks {
		if requiredOnly && !check.required {
			continue
		}

		result := Result{Name: check.name, Required: check.required, Status: Up}
		if err := ctx.Err(); err != nil {
			result.Status, result.Error, result.Err = Unknown, "not checked: context expired", err
		} else {
			started := time.Now()
			err := runCheck(ctx, check.run)
			if err == nil {
				err = ctx.Err()
			}

			result.DurationMS = float64(time.Since(started)) / float64(time.Millisecond)
			if err != nil {
				result.Status, result.Err, result.Error = Down, err, "check failed"
				switch {
				case errors.Is(err, context.DeadlineExceeded):
					result.Error = "timed out"
				case errors.Is(err, context.Canceled):
					result.Error = "canceled"
				case errors.Is(err, errCheckPanic):
					result.Error = "check panicked"
				}
			}
		}

		if result.Status != Up {
			if check.required {
				report.Status, report.Ready = Down, false
			} else if report.Status == Up {
				report.Status = Degraded
			}
		}

		report.Checks = append(report.Checks, result)
	}

	report.CheckedAt = time.Now().UTC()

	return report
}

func runCheck(ctx context.Context, run func(context.Context) error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: %v", errCheckPanic, recovered)
		}
	}()

	return run(ctx)
}
