package migrate_test

import (
	"context"
	"errors"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Penthious/go-foundation/migrate"
)

func TestRunner_ConcurrentJobsAndCanceledLockWait(t *testing.T) {
	db := testDB(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	blocker, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	defer blocker.Close()
	if _, err := blocker.ExecContext(ctx, "SELECT pg_advisory_lock(12345678)"); err != nil {
		t.Fatal(err)
	}

	files := fstest.MapFS{"1_wait.up.sql": {Data: []byte("SELECT pg_advisory_xact_lock(12345678);")}}
	runner := newRunner(t, db, migrate.Config{FS: files})
	done := make(chan error, 1)
	go func() { done <- runner.Up(ctx) }()

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		var waiting bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory'
			AND objid = 12345678 AND NOT granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database()))`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}

		if waiting {
			break
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("first migration did not reach the controlled database lock")
		}
	}

	waitCtx, stopWaiting := context.WithTimeout(ctx, 100*time.Millisecond)
	defer stopWaiting()

	if err := runner.Up(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("concurrent lock wait ignored cancellation: %v", err)
	}

	if _, err := blocker.ExecContext(ctx, "SELECT pg_advisory_unlock(12345678)"); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("migration did not finish after releasing its SQL lock")
	}

	if err := runner.Up(ctx); err != nil {
		t.Fatalf("runner did not recover after canceled initialization: %v", err)
	}

	version(t, runner, 1)
	var locks int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'
		AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`).Scan(&locks); err != nil {
		t.Fatal(err)
	}

	if locks != 0 {
		t.Fatalf("pooled sessions retained %d advisory locks", locks)
	}
}
