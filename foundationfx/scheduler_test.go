package foundationfx_test

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/foundationfx"
	"github.com/Penthious/go-foundation/logger"
	"github.com/Penthious/go-foundation/scheduler"

	"go.uber.org/fx"
)

func TestSchedulerModule_LocalJobsAndShutdown(t *testing.T) {
	entered, finished := make(chan struct{}), make(chan struct{})
	var active atomic.Bool
	var worker *scheduler.Scheduler
	app := fx.New(
		foundationfx.SchedulerModule, fx.NopLogger,
		fx.Supply(scheduler.Config{Namespace: "orders"}),
		fx.Provide(func() (*logger.Logger, error) {
			return logger.New(logger.Config{Service: "scheduler-test", Output: io.Discard})
		}),
		fx.Provide(fx.Annotate(func() scheduler.Job {
			return scheduler.Job{Name: "cleanup", Every: time.Millisecond, Handler: func(ctx context.Context, run scheduler.Run) error {
				active.Store(true)
				close(entered)
				<-ctx.Done()
				active.Store(false)
				close(finished)

				return ctx.Err()
			}}
		}, fx.ResultTags(`group:"foundation_jobs"`))),
		fx.Populate(&worker),
	)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}

	if err := app.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = app.Stop(context.Background()) })
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("Fx job was not started")
	}

	if err := worker.Ping(t.Context()); err != nil || !active.Load() {
		t.Fatal("running scheduler is not healthy")
	}

	if err := app.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}

	select {
	case <-finished:
	default:
		t.Fatal("Fx shutdown returned before joining handlers")
	}

	if active.Load() || !errors.Is(worker.Ping(t.Context()), scheduler.ErrClosed) {
		t.Fatal("scheduler remained active after shutdown")
	}
}

func TestSchedulerModule_ValidationAndStartupRollback(t *testing.T) {
	for _, scenario := range []string{"no jobs", "invalid config", "startup rollback"} {
		t.Run(scenario, func(t *testing.T) {
			var worker *scheduler.Scheduler
			cfg := scheduler.Config{Namespace: "orders"}
			if scenario == "invalid config" {
				cfg.Namespace = ""
			}

			options := []fx.Option{foundationfx.SchedulerModule, fx.NopLogger,
				fx.Supply(cfg),
				fx.Provide(func() (*logger.Logger, error) {
					return logger.New(logger.Config{Service: "scheduler-test", Output: io.Discard})
				}),
				fx.Populate(&worker),
			}
			if scenario != "no jobs" {
				options = append(options, fx.Provide(fx.Annotate(func() scheduler.Job {
					return scheduler.Job{Name: "unused", Every: time.Hour, Handler: func(context.Context, scheduler.Run) error { return nil }}
				}, fx.ResultTags(`group:"foundation_jobs"`))))
			}

			if scenario == "startup rollback" {
				options = append(options, fx.Invoke(func(lifecycle fx.Lifecycle, _ *scheduler.Scheduler) {
					lifecycle.Append(fx.Hook{OnStart: func(context.Context) error { return errors.New("later startup error") }})
				}))
			}

			app := fx.New(options...)
			if scenario != "startup rollback" {
				if app.Err() == nil {
					t.Fatal("invalid scheduler graph accepted")
				}

				return
			}

			if err := app.Err(); err != nil {
				t.Fatal(err)
			}

			if err := app.Start(t.Context()); err == nil {
				t.Fatal("later startup failure accepted")
			}

			if !errors.Is(worker.Ping(t.Context()), scheduler.ErrClosed) {
				t.Fatal("startup rollback did not close scheduler")
			}
		})
	}
}
