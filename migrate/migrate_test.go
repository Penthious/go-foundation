package migrate_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"io"
	"net/url"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Penthious/go-foundation/inbox"
	"github.com/Penthious/go-foundation/logger"
	"github.com/Penthious/go-foundation/migrate"
	"github.com/Penthious/go-foundation/outbox"
	"github.com/Penthious/go-foundation/postgres"
	"github.com/Penthious/go-foundation/pubsub"
	"github.com/Penthious/go-foundation/scheduler"
)

func TestNew_DoesNotConnect(t *testing.T) {
	db, err := postgres.New(postgres.Config{DSN: "postgres://localhost/unused"})
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()
	if _, err := migrate.New(db, migrate.Config{InboxVersion: 1, OutboxVersion: 2}); err != nil {
		t.Fatal(err)
	}

	if db.Stats().OpenConnections != 0 {
		t.Fatal("runner construction opened a connection")
	}
}

func TestRunner_SchedulerSchema(t *testing.T) {
	db := testDB(t)
	runner := newRunner(t, db, migrate.Config{SchedulerVersion: 1})
	if err := runner.Up(t.Context()); err != nil {
		t.Fatal(err)
	}

	log, err := logger.New(logger.Config{Service: "migration-test", Output: io.Discard})
	if err != nil {
		t.Fatal(err)
	}

	worker, err := scheduler.New(scheduler.Config{Namespace: "orders", Logger: log, DB: db}, scheduler.Job{
		Name: "reconcile", Mode: scheduler.Coordinated, Every: time.Hour,
		Handler: func(context.Context, scheduler.Run) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := worker.Start(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := worker.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := db.PingContext(t.Context()); err != nil {
		t.Fatal("scheduler closed its borrowed pool")
	}

	if err := runner.Down(t.Context()); !errors.Is(err, migrate.ErrIrreversible) {
		t.Fatalf("scheduler schema is not forward-only: %v", err)
	}
}

func TestRunner_EmbeddedSQLAndFoundationSchemas(t *testing.T) {
	db := testDB(t)
	db.SetMaxOpenConns(1)
	files := fstest.MapFS{
		"sql/003_widgets.up.sql": {Data: []byte(`CREATE TABLE widgets (id integer PRIMARY KEY);
			CREATE FUNCTION plus_one(n integer) RETURNS integer LANGUAGE plpgsql AS $$
			BEGIN RETURN n + 1; END; $$;
			INSERT INTO widgets VALUES (1);`)},
		"sql/003_widgets.down.sql": {Data: []byte("DROP FUNCTION plus_one(integer); DROP TABLE widgets;")},
	}
	runner := newRunner(t, db, migrate.Config{FS: files, Dir: "sql", InboxVersion: 1, OutboxVersion: 2})
	statuses, err := runner.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if len(statuses) != 3 || statuses[0].Applied || statuses[2].Applied || !statuses[2].Reversible {
		t.Fatalf("pending status = %+v", statuses)
	}

	version(t, runner, 0)
	if err := runner.Down(t.Context()); err != nil {
		t.Fatal(err)
	}

	if exists(t, db, "public.gof_schema_version") {
		t.Fatal("status/version/down created a history table in an unmigrated database")
	}

	// Execution uses New's source snapshot, not the subsequently edited FS.
	files["sql/003_widgets.up.sql"].Data = []byte("SELECT missing_function();")
	if err := runner.Up(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := runner.Up(t.Context()); err != nil {
		t.Fatal(err)
	}

	version(t, runner, 3)
	statuses, err = runner.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	for _, status := range statuses {
		if !status.Applied || status.AppliedAt.IsZero() {
			t.Fatalf("applied status missing: %+v", status)
		}
	}

	var value int
	if err := db.QueryRowContext(t.Context(), "SELECT plus_one(count(*)::integer) FROM widgets").Scan(&value); err != nil || value != 2 {
		t.Fatalf("multi-statement/dollar-quoted SQL = %d, %v", value, err)
	}

	store, err := inbox.New(db, inbox.Config{})
	if err != nil {
		t.Fatal(err)
	}

	if processed, err := store.Process(t.Context(), inbox.Key{Consumer: "billing", MessageID: "event"}, func(context.Context, *sql.Tx) error { return nil }); !processed || err != nil {
		t.Fatalf("migrated inbox = %v, %v", processed, err)
	}

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	defer tx.Rollback()
	if err := outbox.Enqueue(t.Context(), tx, pubsub.Message{ID: "queued", Topic: "orders.created"}); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := runner.Down(t.Context()); err != nil {
		t.Fatal(err)
	}

	version(t, runner, 2)
	if exists(t, db, "public.widgets") {
		t.Fatal("service down migration did not execute")
	}

	if err := runner.Down(t.Context()); !errors.Is(err, migrate.ErrIrreversible) {
		t.Fatalf("built-in down = %v", err)
	}

	version(t, runner, 2)
	for _, table := range []string{"public.gof_inbox", "public.gof_outbox"} {
		var rows int
		if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM "+table).Scan(&rows); err != nil || rows != 1 {
			t.Fatalf("durable SDK data was removed: %s = %d, %v", table, rows, err)
		}
	}
}

func TestRunner_FailedFileRollsBackAndCanBeRetried(t *testing.T) {
	db := testDB(t)
	files := fstest.MapFS{
		"1_stable.up.sql": {Data: []byte("CREATE TABLE stable (id integer);")},
		"2_partial.up.sql": {Data: []byte(`CREATE TABLE partial (id integer);
			INSERT INTO stable VALUES (1); SELECT nonexistent_function();`)},
	}
	runner := newRunner(t, db, migrate.Config{FS: files})
	if err := runner.Up(t.Context()); err == nil {
		t.Fatal("invalid SQL succeeded")
	}

	version(t, runner, 1)
	if exists(t, db, "public.partial") {
		t.Fatal("failed file left DDL committed")
	}

	var rows int
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM stable").Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("failed file left data committed: %d, %v", rows, err)
	}

	files["2_partial.up.sql"].Data = []byte("CREATE TABLE partial (id integer); INSERT INTO stable VALUES (1);")
	fixed := newRunner(t, db, migrate.Config{FS: files})
	if err := fixed.Up(t.Context()); err != nil {
		t.Fatal(err)
	}

	version(t, fixed, 2)
	if err := fixed.Down(t.Context()); !errors.Is(err, migrate.ErrIrreversible) {
		t.Fatalf("missing down file was silently versioned: %v", err)
	}

	version(t, fixed, 2)
}

func TestRunner_RejectsMissingAndOutOfOrderHistory(t *testing.T) {
	db := testDB(t)
	files := fstest.MapFS{
		"1_first.up.sql": {Data: []byte("CREATE TABLE items (id integer);")},
		"3_third.up.sql": {Data: []byte("INSERT INTO items VALUES (3);")},
	}
	runner := newRunner(t, db, migrate.Config{FS: files})
	if err := runner.Up(t.Context()); err != nil {
		t.Fatal(err)
	}

	files["2_late.up.sql"] = &fstest.MapFile{Data: []byte("INSERT INTO items VALUES (2);")}
	outOfOrder := newRunner(t, db, migrate.Config{FS: files})
	if err := outOfOrder.Up(t.Context()); err == nil {
		t.Fatal("out-of-order migration was accepted")
	}

	version(t, outOfOrder, 3)
	delete(files, "3_third.up.sql")
	missing := newRunner(t, db, migrate.Config{FS: files})
	if _, err := missing.Status(t.Context()); err == nil || !strings.Contains(err.Error(), "version 3") {
		t.Fatalf("missing-source status = %v", err)
	}

	if err := missing.Up(t.Context()); err == nil {
		t.Fatal("up accepted a removed applied migration")
	}

	if err := missing.Down(t.Context()); err == nil {
		t.Fatal("down accepted a removed applied migration")
	}
}

func TestRunner_DoesNotAdoptExistingSchemas(t *testing.T) {
	db := testDB(t)
	if _, err := db.ExecContext(t.Context(), inbox.Schema()); err != nil {
		t.Fatal(err)
	}

	runner := newRunner(t, db, migrate.Config{InboxVersion: 1})
	if err := runner.Up(t.Context()); err == nil {
		t.Fatal("existing untracked schema was silently adopted")
	}

	version(t, runner, 0)
	if !exists(t, db, "public.gof_inbox") {
		t.Fatal("existing schema was changed")
	}
}

func TestRunner_FailedMessagesSchema(t *testing.T) {
	db := testDB(t)
	runner := newRunner(t, db, migrate.Config{FailedMessagesVersion: 1})
	if err := runner.Up(t.Context()); err != nil {
		t.Fatal(err)
	}

	store, err := pubsub.NewFailedStore(db, pubsub.FailedStoreConfig{})
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}

	if err := runner.Down(t.Context()); !errors.Is(err, migrate.ErrIrreversible) {
		t.Fatalf("failed-message schema down = %v", err)
	}

	version(t, runner, 1)
}

func testDB(t *testing.T) *sql.DB {
	t.Helper()

	dsn := os.Getenv("GO_FOUNDATION_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set GO_FOUNDATION_POSTGRES_TEST_DSN to a PostgreSQL URL with CREATEDB permission")
	}

	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Fatal("integration DSN must be a PostgreSQL URL")
	}

	admin, err := postgres.New(postgres.Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = admin.Close() })
	name := "gof_migrate_" + strings.ToLower(rand.Text())
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
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

	return db
}

func newRunner(t *testing.T, db *sql.DB, cfg migrate.Config) *migrate.Runner {
	t.Helper()

	runner, err := migrate.New(db, cfg)
	if err != nil {
		t.Fatal(err)
	}

	return runner
}

func version(t *testing.T, runner *migrate.Runner, want int64) {
	t.Helper()

	got, err := runner.Version(t.Context())
	if err != nil || got != want {
		t.Fatalf("version = %d, %v; want %d", got, err, want)
	}
}

func exists(t *testing.T, db *sql.DB, table string) bool {
	t.Helper()

	var exists bool
	if err := db.QueryRowContext(t.Context(), "SELECT to_regclass($1) IS NOT NULL", table).Scan(&exists); err != nil {
		t.Fatal(err)
	}

	return exists
}
