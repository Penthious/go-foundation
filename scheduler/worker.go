package scheduler

import (
	"context"
	"errors"
	"time"

	"github.com/Penthious/go-foundation/logger"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

func (s *Scheduler) worker(ctx context.Context, entry job) {
	for ctx.Err() == nil {
		var due time.Time
		delay := s.cfg.PollInterval
		if entry.Mode == Local {
			due = entry.next(time.Now())
			if due.IsZero() {
				return
			}

			delay = time.Until(due)
		}

		if !wait(ctx, delay) {
			return
		}

		select {
		case s.slots <- struct{}{}:
			s.dispatch(ctx, entry, due)
			<-s.slots
		default:
			s.runs.Add(ctx, 1, metric.WithAttributes(attribute.String("job.name", entry.Name), attribute.String("scheduler.mode", string(entry.Mode)), attribute.String("result", "skipped_capacity")))
		}
	}
}

func (s *Scheduler) dispatch(ctx context.Context, entry job, due time.Time) {
	if ctx.Err() != nil {
		return
	}

	token := ""
	if entry.Mode == Coordinated {
		at, lease, found, err := s.claim(ctx, entry)
		if err != nil {
			if ctx.Err() == nil {
				s.cfg.Logger.Warn(ctx, "scheduled job claim failed", logger.String("job", entry.Name), logger.Err(err))
			}

			return
		}

		if !found {
			return
		}

		due, token = at, lease
	}

	run := s.occurrence(entry, due)
	err := s.execute(ctx, entry, run)
	if token != "" {
		if releaseErr := s.release(ctx, entry, token); releaseErr != nil {
			s.cfg.Logger.Warn(ctx, "scheduled job lease release failed", logger.String("job", entry.Name), logger.String("run_id", run.ID), logger.Err(releaseErr))
		}
	}

	if err != nil && ctx.Err() == nil {
		s.cfg.Logger.Warn(ctx, "scheduled job failed", logger.String("job", entry.Name), logger.String("run_id", run.ID), logger.Err(err))
	}
}

func (s *Scheduler) execute(parent context.Context, entry job, run Run) error {
	ctx, cancel := context.WithTimeout(parent, entry.Timeout)
	defer cancel()

	attributes := []attribute.KeyValue{attribute.String("job.name", entry.Name), attribute.String("scheduler.mode", string(entry.Mode))}
	ctx, span := otel.Tracer("go-foundation/scheduler").Start(ctx, "scheduled job "+entry.Name, trace.WithAttributes(attributes...))
	defer span.End()

	started := time.Now()
	err := ctx.Err()
	if err == nil {
		err = callHandler(ctx, entry.Handler, run)
	}

	if err == nil {
		err = ctx.Err()
	}

	result := "success"
	switch {
	case errors.Is(err, context.Canceled):
		result = "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		result = "timeout"
	case err != nil:
		result = "failure"
	}

	if err != nil {
		span.SetStatus(codes.Error, "scheduled job failed")
	}

	span.SetAttributes(attribute.String("result", result))
	options := metric.WithAttributes(append(attributes, attribute.String("result", result))...)
	s.runs.Add(ctx, 1, options)
	s.duration.Record(ctx, time.Since(started).Seconds(), options)
	if err == nil {
		s.cfg.Logger.Info(ctx, "scheduled job completed", logger.String("job", entry.Name), logger.String("run_id", run.ID), logger.Duration("duration", time.Since(started)))
	}

	return err
}

func callHandler(ctx context.Context, handler func(context.Context, Run) error, run Run) (err error) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			err = errors.New("scheduler: handler panicked")
		}
	}()
	err = handler(ctx, run)
	returned = true

	return err
}

func wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}
