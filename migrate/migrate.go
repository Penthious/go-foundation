// Package migrate runs service-owned PostgreSQL migrations with a private Goose
// backend. SQL files use VERSION_name.up.sql and optional matching .down.sql files.
package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/pressly/goose/v3/lock"
)

const historyTable = "public.gof_schema_version"

// ErrIrreversible means the highest applied migration has no down SQL. Built-in
// SDK schemas are forward-only to preserve durable data.
var ErrIrreversible = errors.New("migrate: migration is irreversible")

// Config supplies immutable migration sources. No migrations run automatically.
type Config struct {
	// FS is normally a service's embed.FS. Dir defaults to "." and selects one
	// flat directory. Files are read and copied during New. FS may be nil when
	// only built-in schemas are enabled.
	FS  fs.FS
	Dir string
	// InboxVersion and OutboxVersion opt into the initial SDK schemas at explicit
	// positive versions in the service's history. Zero disables each schema.
	// Versions must not collide with service files or change after application.
	InboxVersion  int64
	OutboxVersion int64
	// FailedMessagesVersion opts into durable pubsub failures and replay storage.
	// Zero disables it. Like inbox/outbox, this initial schema is forward-only.
	FailedMessagesVersion int64
	// SchedulerVersion opts into coordinated job state. Zero disables it.
	// Choose a new positive version; the initial schema is forward-only.
	SchedulerVersion int64
	// Timeout defaults to 5m for an operation, including connection and lock waits.
	// A separate cleanup budget of up to 5s releases the session lock afterward.
	Timeout time.Duration
}

// Status describes a migration in ascending version order. AppliedAt is zero
// for pending migrations. Reversible indicates a service-owned down file exists.
type Status struct {
	Version    int64
	Name       string
	Applied    bool
	AppliedAt  time.Time
	Reversible bool
}

// Runner borrows a database and snapshots its sources. It is safe for concurrent
// use. PostgreSQL advisory locks serialize Up/Down across runners and processes.
// Use a direct database connection or session pool, not a transaction pool proxy.
type Runner struct {
	db      *sql.DB
	sources []source
	history database.StoreExtender
	timeout time.Duration
}

// New validates and snapshots sources without connecting or executing SQL.
// The caller owns db.Close. Execute migrations explicitly from a deployment job
// or service-owned command, before starting services that require their schemas.
func New(db *sql.DB, cfg Config) (*Runner, error) {
	if db == nil {
		return nil, errors.New("migrate: database is required")
	}

	if cfg.Timeout < 0 || cfg.InboxVersion < 0 || cfg.OutboxVersion < 0 || cfg.FailedMessagesVersion < 0 || cfg.SchedulerVersion < 0 {
		return nil, errors.New("migrate: timeout and built-in versions must not be negative")
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Minute
	}

	if cfg.Dir == "" {
		cfg.Dir = "."
	}

	if !fs.ValidPath(cfg.Dir) || (cfg.FS == nil && cfg.Dir != ".") {
		return nil, errors.New("migrate: Dir must be a valid filesystem-relative directory with an FS")
	}

	sources, err := loadSources(cfg)
	if err != nil {
		return nil, err
	}

	history, err := database.NewStore(goose.DialectPostgres, historyTable)
	if err != nil {
		return nil, fmt.Errorf("migrate: create history store: %w", err)
	}

	extended, ok := history.(database.StoreExtender)
	if !ok {
		return nil, errors.New("migrate: history store lacks table existence checks")
	}

	return &Runner{db: db, sources: sources, history: extended, timeout: cfg.Timeout}, nil
}

// Up applies pending migrations in order, each in its own transaction with its
// version record. A failure rolls back that file; earlier versions stay applied.
// All SQL must be transaction-compatible and must not contain transaction control.
func (r *Runner) Up(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	provider, err := r.provider()
	if err != nil {
		return err
	}

	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("migrate: up: %w", err)
	}

	return nil
}

// Down rolls back the highest applied migration using its explicit down file.
// It is a no-op at version zero. Missing down files return ErrIrreversible without
// changing that migration or its version. This operation can destroy service data.
func (r *Runner) Down(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	exists, err := r.history.TableExists(ctx, r.db)
	if err != nil {
		return fmt.Errorf("migrate: check history: %w", err)
	}

	if !exists {
		return nil
	}

	provider, err := r.provider()
	if err != nil {
		return err
	}

	if _, err := provider.Down(ctx); err != nil {
		return fmt.Errorf("migrate: down: %w", err)
	}

	return nil
}

// Status reads history in a read-only snapshot without creating tables or
// applying migrations. Applied versions missing from the sources return an error.
func (r *Runner) Status(ctx context.Context) ([]Status, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, fmt.Errorf("migrate: read history: %w", err)
	}

	defer tx.Rollback()
	if err := r.validateHistory(ctx, tx); err != nil {
		return nil, err
	}

	exists, err := r.history.TableExists(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("migrate: check history: %w", err)
	}

	statuses := make([]Status, 0, len(r.sources))
	for _, migration := range r.sources {
		status := Status{Version: migration.version, Name: migration.name, Reversible: migration.down != ""}
		if exists {
			stored, err := r.history.GetMigration(ctx, tx, migration.version)
			if err != nil && !errors.Is(err, database.ErrVersionNotFound) {
				return nil, fmt.Errorf("migrate: read version %d: %w", migration.version, err)
			}

			if stored != nil && stored.IsApplied {
				status.Applied, status.AppliedAt = true, stored.Timestamp
			}
		}

		statuses = append(statuses, status)
	}

	return statuses, nil
}

// Version returns the highest applied version, or zero before any migrations.
// It has the same read-only behavior as Status.
func (r *Runner) Version(ctx context.Context) (int64, error) {
	statuses, err := r.Status(ctx)
	if err != nil {
		return 0, err
	}

	var version int64
	for _, status := range statuses {
		if status.Applied {
			version = status.Version
		}
	}

	return version, nil
}

func (r *Runner) provider() (*goose.Provider, error) {
	migrations := make([]*goose.Migration, 0, len(r.sources))
	for _, migration := range r.sources {
		up := sqlFunction(migration.up)
		down := &goose.GoFunc{RunTx: func(context.Context, *sql.Tx) error {
			return fmt.Errorf("%w: version %d (%s)", ErrIrreversible, migration.version, migration.name)
		}}
		if migration.down != "" {
			down = sqlFunction(migration.down)
		}

		registered := goose.NewGoMigration(migration.version, up, down)
		registered.Source = fmt.Sprintf("%d_%s.go", migration.version, migration.name)
		migrations = append(migrations, registered)
	}

	locker, err := lock.NewPostgresSessionLocker(lock.WithLockID(migrationLockID),
		lock.WithLockTimeout(1, uint64(r.timeout/time.Second)+1), lock.WithUnlockTimeout(1, 5))
	if err != nil {
		return nil, fmt.Errorf("migrate: create locker: %w", err)
	}

	// A fresh provider avoids shared global registration, uninterruptible local
	// mutex waits, and cached initialization failures after canceled operations.
	provider, err := goose.NewProvider(goose.DialectPostgres, r.db, nil,
		goose.WithTableName(historyTable), goose.WithDisableGlobalRegistry(true),
		goose.WithGoMigrations(migrations...), goose.WithLogger(goose.NopLogger()),
		goose.WithSessionLocker(&sessionLocker{delegate: locker, runner: r}))
	if err != nil {
		return nil, fmt.Errorf("migrate: create runner: %w", err)
	}

	return provider, nil
}

func sqlFunction(body string) *goose.GoFunc {
	return &goose.GoFunc{RunTx: func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, body)
		return err
	}}
}

func (r *Runner) validateHistory(ctx context.Context, db database.DBTxConn) error {
	exists, err := r.history.TableExists(ctx, db)
	if err != nil {
		return fmt.Errorf("migrate: check history: %w", err)
	}

	if !exists {
		return nil
	}

	applied, err := r.history.ListMigrations(ctx, db)
	if err != nil {
		return fmt.Errorf("migrate: list history: %w", err)
	}

	known := make(map[int64]bool, len(r.sources))
	for _, migration := range r.sources {
		known[migration.version] = true
	}

	for _, migration := range applied {
		if migration.IsApplied && migration.Version != 0 && !known[migration.Version] {
			return fmt.Errorf("migrate: applied version %d is missing from configured sources", migration.Version)
		}
	}

	return nil
}
