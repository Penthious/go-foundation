package scheduler

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Penthious/go-foundation/postgres"
)

func TestScheduler_CoordinatedClaimAndRestart(t *testing.T) {
	db := testDB(t, true)
	cfg := testConfig(t)
	cfg.DB = db
	var calls atomic.Int64
	runs := make(chan Run, 4)
	job := Job{Name: "shared", Mode: Coordinated, Every: time.Hour, Handler: func(ctx context.Context, run Run) error {
		calls.Add(1)
		runs <- run

		return nil
	}}
	one, two := testScheduler(t, cfg, job), testScheduler(t, cfg, job)
	for _, worker := range []*Scheduler{one, two} {
		if err := worker.register(t.Context()); err != nil {
			t.Fatal(err)
		}
	}

	makeDue(t, db, cfg.Namespace, job.Name, false)
	var racers sync.WaitGroup
	for _, worker := range []*Scheduler{one, two} {
		racers.Go(func() { worker.dispatch(t.Context(), worker.jobs[0], time.Time{}) })
	}

	racers.Wait()
	if calls.Load() != 1 {
		t.Fatalf("replicas invoked the same fast occurrence %d times", calls.Load())
	}

	run := receive(t, runs)
	if one.occurrence(one.jobs[0], run.ScheduledAt).ID != two.occurrence(two.jobs[0], run.ScheduledAt).ID {
		t.Fatal("coordinated run identity differs across replicas")
	}

	restarted := testScheduler(t, cfg, job)
	if err := restarted.register(t.Context()); err != nil {
		t.Fatal(err)
	}

	restarted.dispatch(t.Context(), restarted.jobs[0], time.Time{})
	if calls.Load() != 1 {
		t.Fatal("restart reset shared schedule state")
	}

	otherCfg := cfg
	otherCfg.Namespace = "another-service"
	other := testScheduler(t, otherCfg, job)
	if err := other.register(t.Context()); err != nil {
		t.Fatal(err)
	}

	makeDue(t, db, otherCfg.Namespace, job.Name, false)
	other.dispatch(t.Context(), other.jobs[0], time.Time{})
	if calls.Load() != 2 {
		t.Fatal("namespaces interfered")
	}
}

func TestScheduler_ConcurrentBatchRegistration(t *testing.T) {
	db := testDB(t, true)
	cfg := testConfig(t)
	cfg.DB = db
	forward, reverse := make([]Job, 50), make([]Job, 50)
	for i := range forward {
		forward[i] = Job{Name: fmt.Sprintf("job-%02d", i), Mode: Coordinated, Every: time.Hour, Handler: func(context.Context, Run) error { return nil }}
		reverse[len(reverse)-1-i] = forward[i]
	}

	one, two := testScheduler(t, cfg, forward...), testScheduler(t, cfg, reverse...)
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, worker := range []*Scheduler{one, two} {
		go func() {
			<-start
			results <- worker.register(t.Context())
		}()
	}

	close(start)
	for range 2 {
		if err := receive(t, results); err != nil {
			t.Fatalf("replicas with differently ordered jobs could not register together: %v", err)
		}
	}
}

func TestScheduler_CoordinatedWorkersPollAndShutdown(t *testing.T) {
	db := testDB(t, true)
	cfg := testConfig(t)
	cfg.DB = db
	entered := make(chan Run, 2)
	entry := Job{Name: "polled", Mode: Coordinated, Every: time.Hour, Handler: func(ctx context.Context, run Run) error {
		entered <- run
		<-ctx.Done()

		return ctx.Err()
	}}
	one, two := testScheduler(t, cfg, entry), testScheduler(t, cfg, entry)
	for _, worker := range []*Scheduler{one, two} {
		if err := worker.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
	}

	makeDue(t, db, cfg.Namespace, entry.Name, false)
	_ = receive(t, entered)
	for _, worker := range []*Scheduler{one, two} {
		if err := worker.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case <-entered:
		t.Fatal("replicas overlapped the same occurrence")
	default:
	}

	if err := db.PingContext(t.Context()); err != nil {
		t.Fatal("scheduler closed borrowed storage")
	}
}

func TestScheduler_LeaseRecoveryAndStaleRelease(t *testing.T) {
	db := testDB(t, true)
	cfg := testConfig(t)
	cfg.DB = db
	entry := Job{Name: "leased", Mode: Coordinated, Every: time.Hour, Handler: func(context.Context, Run) error { return nil }}
	one, two := testScheduler(t, cfg, entry), testScheduler(t, cfg, entry)
	if err := one.register(t.Context()); err != nil {
		t.Fatal(err)
	}

	makeDue(t, db, cfg.Namespace, entry.Name, false)
	_, first, found, err := one.claim(t.Context(), one.jobs[0])
	if err != nil || !found {
		t.Fatalf("initial claim: %v, %v", found, err)
	}

	makeDue(t, db, cfg.Namespace, entry.Name, false)
	if _, _, found, err := two.claim(t.Context(), two.jobs[0]); err != nil || found {
		t.Fatalf("active lease allowed overlapping claim: %v, %v", found, err)
	}

	makeDue(t, db, cfg.Namespace, entry.Name, true)
	_, second, found, err := two.claim(t.Context(), two.jobs[0])
	if err != nil || !found || first == second {
		t.Fatalf("expired lease not recovered: %v, %v", found, err)
	}

	if err := one.release(t.Context(), one.jobs[0], first); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale owner released replacement lease: %v", err)
	}

	var token string
	if err := db.QueryRowContext(t.Context(), "SELECT lease_token FROM public.gof_schedules").Scan(&token); err != nil || token != second {
		t.Fatal("replacement lease changed")
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := two.release(ctx, two.jobs[0], second); err != nil {
		t.Fatalf("canceled parent prevented bounded lease cleanup: %v", err)
	}
}

func TestScheduler_RegistrationConflictAndNoAutoDDL(t *testing.T) {
	db := testDB(t, false)
	cfg := testConfig(t)
	cfg.DB = db
	entry := Job{Name: "shared", Mode: Coordinated, Every: time.Hour, Handler: func(context.Context, Run) error { return nil }}
	worker := testScheduler(t, cfg, entry)
	if err := worker.Start(t.Context()); err == nil {
		t.Fatal("missing schema accepted")
	}

	if _, err := db.ExecContext(t.Context(), Schema()); err != nil {
		t.Fatal(err)
	}

	if err := worker.Start(t.Context()); err != nil {
		t.Fatal("failed startup was not retryable:", err)
	}

	if err := worker.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}

	for _, change := range []func(*Job){
		func(j *Job) { j.Every = 2 * time.Hour },
		func(j *Job) { j.Timeout = time.Second },
		func(j *Job) { j.Timezone = "America/Denver" },
	} {
		modified := entry
		change(&modified)
		conflicting := testScheduler(t, cfg, modified)
		if err := conflicting.Start(t.Context()); !errors.Is(err, ErrScheduleConflict) {
			t.Fatalf("conflicting replica configuration accepted: %v", err)
		}
	}

	newEntry := entry
	newEntry.Name = "new-job"
	entry.Every = 2 * time.Hour
	conflicting := testScheduler(t, cfg, newEntry, entry)
	if err := conflicting.register(t.Context()); !errors.Is(err, ErrScheduleConflict) {
		t.Fatalf("conflicting batch registration accepted: %v", err)
	}

	var count int
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM public.gof_schedules WHERE name = 'new-job'").Scan(&count); err != nil || count != 0 {
		t.Fatal("failed registration did not roll back the whole batch")
	}
}

func TestScheduler_HandlerReleasesSQLConnectionAndDoesNotRetryOccurrence(t *testing.T) {
	db := testDB(t, true)
	db.SetMaxOpenConns(1)
	cfg := testConfig(t)
	cfg.DB = db
	calls := 0
	worker := testScheduler(t, cfg, Job{Name: "sql", Mode: Coordinated, Every: time.Hour, Handler: func(ctx context.Context, run Run) error {
		calls++
		var value int
		if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&value); err != nil {
			return err
		}

		return errors.New("job failed")
	}})
	if err := worker.register(t.Context()); err != nil {
		t.Fatal(err)
	}

	makeDue(t, db, cfg.Namespace, "sql", false)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	worker.dispatch(ctx, worker.jobs[0], time.Time{})
	worker.dispatch(ctx, worker.jobs[0], time.Time{})
	if calls != 1 || ctx.Err() != nil {
		t.Fatal("handler retained a SQL connection or retried a failed occurrence")
	}

	var released bool
	if err := db.QueryRowContext(ctx, "SELECT lease_token IS NULL FROM public.gof_schedules").Scan(&released); err != nil || !released {
		t.Fatal("failed handler retained its lease")
	}
}

func TestScheduler_ShutdownCancelsBlockedStartup(t *testing.T) {
	db := testDB(t, true)
	cfg := testConfig(t)
	cfg.DB = db
	worker := testScheduler(t, cfg, Job{Name: "blocked", Mode: Coordinated, Every: time.Hour, Handler: func(context.Context, Run) error {
		t.Error("handler ran during canceled startup")

		return nil
	}})
	blocker, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	defer blocker.Rollback()
	if _, err := blocker.ExecContext(t.Context(), "LOCK TABLE public.gof_schedules IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}

	started := make(chan error, 1)
	go func() { started <- worker.Start(t.Context()) }()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		var waiting bool
		err := db.QueryRowContext(t.Context(), `SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock'
			AND query LIKE 'INSERT INTO public.gof_schedules%')`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}

		if waiting {
			break
		}

		select {
		case <-deadline.C:
			t.Fatal("startup did not reach controlled SQL lock")
		case <-ticker.C:
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	if err := worker.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	if err := receive(t, started); err == nil {
		t.Fatal("canceled registration reported successful startup")
	}
}

func testScheduler(t *testing.T, cfg Config, jobs ...Job) *Scheduler {
	t.Helper()

	worker, err := New(cfg, jobs...)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		_ = worker.Shutdown(ctx)
	})

	return worker
}

func testDB(t *testing.T, apply bool) *sql.DB {
	t.Helper()

	dsn := os.Getenv("GO_FOUNDATION_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set GO_FOUNDATION_POSTGRES_TEST_DSN for PostgreSQL coordination tests")
	}

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("invalid test database URL")
	}

	admin, err := postgres.New(postgres.Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = admin.Close() })
	name := "gof_scheduler_" + strings.ToLower(rand.Text())
	if _, err := admin.ExecContext(t.Context(), "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if _, err := admin.ExecContext(ctx, "DROP DATABASE "+name+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
	})
	u.Path, u.RawPath = "/"+name, ""
	db, err := postgres.New(postgres.Config{DSN: u.String()})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = db.Close() })
	if apply {
		if _, err := db.ExecContext(t.Context(), Schema()); err != nil {
			t.Fatal(err)
		}
	}

	return db
}

func makeDue(t *testing.T, db *sql.DB, namespace, name string, expireLease bool) {
	t.Helper()

	query := "UPDATE public.gof_schedules SET next_run_at = clock_timestamp() - interval '1 second'"
	if expireLease {
		query += ", lease_until = clock_timestamp() - interval '1 second'"
	}

	if _, err := db.ExecContext(t.Context(), query+" WHERE namespace = $1 AND name = $2", namespace, name); err != nil {
		t.Fatal(err)
	}
}
