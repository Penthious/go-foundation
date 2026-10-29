# migrate

[Overview](../README.md) · [Development and testing](../testsupport/README.md)

## Database migrations

Use `migrate` from a **service-owned migration command or deployment job** before
starting the service. It accepts `*sql.DB` and standard `fs.FS`, including
`embed.FS`; no Goose types, registration calls, global configuration, or SQL
annotations are exposed. Constructing a runner and starting an Fx app never
apply migrations automatically.

Keep SQL files in one service-owned directory, with positive, unique numeric
versions. Each migration has a required `VERSION_name.up.sql` and an optional
matching `VERSION_name.down.sql`, for example:

```text
cmd/migrate/
  main.go
  migrations/
    003_create_orders.up.sql
    003_create_orders.down.sql
```

The up file contains plain PostgreSQL SQL:

```sql
CREATE TABLE public.orders (
    id text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
```

An explicitly reversible service migration can supply a down file:

```sql
DROP TABLE public.orders;
```

The command embeds its files and opts into the SDK schemas at versions chosen
by the service. Here versions 1 and 2 are reserved for inbox and outbox, so the
first application migration is version 3:

```go
//go:embed migrations/*.sql
var migrations embed.FS

func applyMigrations(ctx context.Context, db *sql.DB) error {
	runner, err := migrate.New(db, migrate.Config{
		FS: migrations,
		Dir: "migrations",
		InboxVersion: 1,
		OutboxVersion: 2,
	})
	if err != nil {
		return err
	}

	return runner.Up(ctx)
}
```

This uses `embed`, `context`, `database/sql`, and `github.com/Penthious/go-foundation/migrate`. `FS`
may be nil for a runner containing only SDK schemas. `Dir` defaults to `"."` and
is relative to that filesystem; only its immediate files are collected. Non-SQL
files and subdirectories are ignored. SQL files with invalid names, duplicate
versions, unmatched down files, empty content, and collisions with SDK versions
are rejected during `New`. Sources are copied at construction, so later changes
to a mutable filesystem require a new runner.

### Versioning and operations

- `Up(ctx)` applies pending versions in ascending order and is a no-op when up to date.
- `Status(ctx)` returns GOF `Status` values with version, name, applied timestamp,
  and reversibility. Pending timestamps are zero.
- `Version(ctx)` returns the highest applied version, or zero before migration.
- `Down(ctx)` rolls back **one** highest applied version using its explicit down
  file. This can destroy service data. A missing down file returns
  `migrate.ErrIrreversible`, without changing that migration or its version.

History lives in `public.gof_schema_version`. Status and version use read-only
snapshots; they do not create a history table in a fresh database. Down at version
zero is a no-op. All operations honor the caller's context and `Config.Timeout`
(default 5m), including pool acquisition and migration-lock waits. The runner
borrows the pool; the command owns `db.Close()`.

Each complete SQL file and its version record execute in one transaction.
Multiple statements and PostgreSQL dollar-quoted function bodies work without
vendor-specific statement markers. A failing file rolls back its DDL, data, and
version record; earlier successful files remain applied. Files must not contain
top-level `BEGIN`, `COMMIT`, `ROLLBACK`, or other transaction-control statements;
procedural blocks inside dollar-quoted functions are supported. Operations
that PostgreSQL forbids inside transactions, such as `CREATE INDEX CONCURRENTLY`,
require a separately managed deployment step; this API deliberately provides
transactional migrations only.

Migration jobs serialize with a PostgreSQL session advisory lock, including
across runner instances and processes. Canceled waits and failed operations
release the lock; an ambiguous/failed lock cleanup discards the physical session.
Cleanup has a separate budget of up to 5s after cancellation. Use a direct
database endpoint or session pooling. Transaction pooling proxies cannot preserve
the session lock's guarantees. A pool size of one is supported.

Treat applied migration files and configured SDK versions as immutable. Append
new versions for changes; never renumber, remove, or edit an applied migration.
Missing applied versions and newly introduced versions below the database's
current version cause errors. The history tracks versions, not SQL checksums;
editing an applied file is not detected or reapplied. Choose the same versions
and sources in every deployment of a service.

### Inbox/outbox schemas and existing databases

Positive `InboxVersion` and `OutboxVersion` values include the initial SDK schemas
in that **same service migration history**. Zero disables each schema. Their
versions must be distinct and must not collide with service SQL. The built-ins
are forward-only: Down never drops inbox deduplication records or pending outbox
events. Any intentional removal requires a service-owned, reviewed migration.

`FailedMessagesVersion` similarly opts into pubsub failure/replay storage at an
explicit unused version. For example, use version 4 after the command example's
versions 1–3. Its schema is also forward-only. Keep existing applied versions
unchanged; the example command does not enable this optional schema by default.

`SchedulerVersion` opts into coordinated scheduling state at another unused
version, with the same forward-only policy. Local scheduling does not need it.
The migration command example leaves this optional schema disabled too.

The SDK's initial SQL files live in `internal/schema`, shared by the migration
runner and the runtime packages' schema helpers. Migration commands do not import
the messaging or scheduler implementations to obtain their SQL. Moving these
files does not change the SQL, applied versions, or migration history.

`inbox.Schema()` and `outbox.Schema()` remain available as plain SQL for existing
migration workflows. A service can instead snapshot that SQL into its own
versioned up file; do not also enable the corresponding built-in version.
This runner does not automatically adopt existing tables or another tool's
history. Enabling a built-in for an existing untracked table fails rather than
silently claiming it matches the expected schema. Existing services should keep
their established migration history or plan an explicit, verified transition;
do not delete inbox/outbox data to bootstrap this runner.

### Service command and container builds

[The complete Go command example](../examples/migrations/main.go) embeds paired SQL
files and supports `up`, `down`, `status`, and `version`. Copy it into a service
and adapt its application migrations and SDK version choices. It loads the
connection string from `DATABASE_URL`, supports cancellation, and closes its pool:

```sh
go run ./examples/migrations status
go run ./examples/migrations up
go run ./examples/migrations version
```

Supply `DATABASE_URL` from the deployment's secret configuration. Run the command
against the intended service database using a migration role with the necessary
DDL/history permissions. Runtime roles can retain narrower permissions.

In each service's Go builder stage, compile its command and embed the SQL into
the migration executable:

```dockerfile
RUN CGO_ENABLED=0 go build -trimpath -o /out/migrate ./cmd/migrate
```

Run `migrate up` as one explicit deployment step before starting the new service
version. No runtime filesystem mount or Goose binary is required. For rolling
deployments, use backward-compatible expand/contract schema changes so old and
new service versions can coexist during the rollout.
