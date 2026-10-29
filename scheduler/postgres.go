package scheduler

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Penthious/go-foundation/internal/schema"
)

// Schema returns the initial coordination schema. Apply it through service-owned
// migrations, or migrate.Config.SchedulerVersion. No scheduler API runs DDL.
func Schema() string {
	return schema.Scheduler()
}

func (s *Scheduler) checkStorage(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.StorageTimeout)
	defer cancel()

	rows, err := s.cfg.DB.QueryContext(ctx, `SELECT namespace, name, schedule, timezone,
		timeout_ns, next_run_at, lease_until, lease_token FROM public.gof_schedules LIMIT 0`)
	if err != nil {
		return fmt.Errorf("scheduler: check schema: %w", err)
	}

	defer rows.Close()

	return rows.Err()
}

func (s *Scheduler) register(ctx context.Context) error {
	if !s.coordinated() {
		return ctx.Err()
	}

	ctx, cancel := context.WithTimeout(ctx, s.cfg.StorageTimeout)
	defer cancel()

	tx, err := s.cfg.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("scheduler: begin registration: %w", err)
	}

	defer tx.Rollback()
	var now time.Time
	if err := tx.QueryRowContext(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return fmt.Errorf("scheduler: read clock: %w", err)
	}

	for _, entry := range s.jobs {
		if entry.Mode != Coordinated {
			continue
		}

		_, err := tx.ExecContext(ctx, `INSERT INTO public.gof_schedules
			(namespace, name, schedule, timezone, timeout_ns, next_run_at)
			VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (namespace, name) DO NOTHING`,
			s.cfg.Namespace, entry.Name, entry.spec, entry.Timezone, int64(entry.Timeout), entry.next(now))
		if err != nil {
			return fmt.Errorf("scheduler: register job: %w", err)
		}

		var spec, zone string
		var timeout int64
		err = tx.QueryRowContext(ctx, `SELECT schedule, timezone, timeout_ns FROM public.gof_schedules
			WHERE namespace = $1 AND name = $2`, s.cfg.Namespace, entry.Name).Scan(&spec, &zone, &timeout)
		if err != nil {
			return fmt.Errorf("scheduler: verify registered job: %w", err)
		}

		if spec != entry.spec || zone != entry.Timezone || timeout != int64(entry.Timeout) {
			return ErrScheduleConflict
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("scheduler: commit registration: %w", err)
	}

	return nil
}

func (s *Scheduler) claim(ctx context.Context, entry job) (time.Time, string, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.StorageTimeout)
	defer cancel()

	tx, err := s.cfg.DB.BeginTx(ctx, nil)
	if err != nil {
		return time.Time{}, "", false, fmt.Errorf("scheduler: begin claim: %w", err)
	}

	defer tx.Rollback()
	var spec, zone string
	var timeout int64
	var next, now time.Time
	var lease sql.NullTime
	err = tx.QueryRowContext(ctx, `SELECT schedule, timezone, timeout_ns, next_run_at,
		lease_until, clock_timestamp() FROM public.gof_schedules
		WHERE namespace = $1 AND name = $2 FOR UPDATE SKIP LOCKED`, s.cfg.Namespace, entry.Name).Scan(
		&spec, &zone, &timeout, &next, &lease, &now)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, "", false, nil
	}

	if err != nil {
		return time.Time{}, "", false, fmt.Errorf("scheduler: read due job: %w", err)
	}

	if spec != entry.spec || zone != entry.Timezone || timeout != int64(entry.Timeout) {
		return time.Time{}, "", false, ErrScheduleConflict
	}

	if now.Before(next) || lease.Valid && now.Before(lease.Time) {
		return time.Time{}, "", false, nil
	}

	token := rand.Text()
	_, err = tx.ExecContext(ctx, `UPDATE public.gof_schedules SET next_run_at = $3,
		lease_token = $4, lease_until = clock_timestamp() + $5::bigint * interval '1 microsecond'
		WHERE namespace = $1 AND name = $2`, s.cfg.Namespace, entry.Name, entry.next(now), token, (2 * entry.Timeout).Microseconds())
	if err != nil {
		return time.Time{}, "", false, fmt.Errorf("scheduler: reserve occurrence: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return time.Time{}, "", false, fmt.Errorf("scheduler: commit claim: %w", err)
	}

	return next.UTC(), token, true, nil
}

func (s *Scheduler) release(ctx context.Context, entry job, token string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.StorageTimeout)
	defer cancel()

	result, err := s.cfg.DB.ExecContext(ctx, `UPDATE public.gof_schedules
		SET lease_token = NULL, lease_until = NULL
		WHERE namespace = $1 AND name = $2 AND lease_token = $3`, s.cfg.Namespace, entry.Name, token)
	if err != nil {
		return fmt.Errorf("scheduler: release lease: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("scheduler: count released lease: %w", err)
	}

	if count != 1 {
		return ErrLeaseLost
	}

	return nil
}
