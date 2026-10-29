# scheduler

[Overview](../README.md) · [Development and testing](../testsupport/README.md)

## Scheduling

Services register `scheduler.Job` values with a GOF scheduler. Each job chooses
independent local execution or PostgreSQL coordination; both can run in the same
process. The cron parser stays private. Services own job handlers and business
rules; GOF owns scheduling, timeouts, concurrency, logs, metrics, and shutdown.

```go
jobs, err := scheduler.New(scheduler.Config{
	Namespace: "orders",
	Logger: log,
	DB: db, // Borrowed *sql.DB; omit when every job is local.
}, scheduler.Job{
	Name: "refresh-local-state",
	Mode: scheduler.Local,
	Every: 5 * time.Minute,
	Timeout: 30 * time.Second,
	Handler: func(ctx context.Context, run scheduler.Run) error {
		return refreshLocalState(ctx)
	},
}, scheduler.Job{
	Name: "daily-reconciliation",
	Mode: scheduler.Coordinated,
	Cron: "0 9 * * *",
	Timezone: "America/Denver",
	Timeout: 10 * time.Minute,
	Handler: func(ctx context.Context, run scheduler.Run) error {
		return reconcileOrders(ctx, run.ID)
	},
})
if err != nil {
	return err
}

if err := jobs.Start(ctx); err != nil {
	return err
}

// On service shutdown, use a fresh shutdown context, cancel and join jobs,
// then close the database/logger/telemetry dependencies.
if err := jobs.Shutdown(shutdownCtx); err != nil {
	return err
}
```

This uses `context`, `time`, and GOF's `scheduler`; `db` is a standard `*sql.DB`.
The handler functions are service-owned. `New` validates and copies the jobs
without network I/O or goroutines. `Start` registers shared jobs transactionally
before starting workers; startup failures can be retried. Repeated successful
`Start` calls do not create extra workers. The startup context only bounds
startup; it does not become the lifetime context for job handlers.

### Schedules and execution

- `Name` must be unique within a scheduler. Names and the required `Namespace`
  use 1–128 ASCII letters, digits, dots, underscores or hyphens. Coordinate
  replicas using the same database, namespace, and job names.
- Choose exactly one of `Cron` or `Every`. Cron has five fields (minute, hour,
  day of month, month, day of week), with standard descriptors such as `@hourly`
  and `@daily`. Seconds and inline `@every`/`CRON_TZ` directives are not accepted;
  use the explicit fields instead.
- `Timezone` defaults to **UTC**; explicit IANA names work in minimal Docker
  images because the package includes Go's timezone data. OS-dependent `Local`
  is rejected. Calendar schedules follow the selected timezone and daylight
  saving rules, so a nonexistent local wall-clock time may be skipped.
- `Every` must be at least 1ms. Intervals align to fixed absolute time boundaries
  and do not drift from handler completion; timezone does not affect them.
  New jobs first run at their next occurrence, not immediately at startup.
- `Timeout` defaults to 5m, must be at least 1ms, and must fit a doubled lease.
  Handlers must honor their context, return, and join work they start. A timeout
  cannot forcibly terminate a Go function or an external effect.
- A local job does not overlap itself. `MaxConcurrentJobs` defaults to 10 per
  scheduler instance, capped at the number of jobs. There is no unbounded work
  queue: local occurrences are skipped when capacity is exhausted or while the
  same job is still running. A shared due job remains claimable by another
  replica, or is coalesced when capacity becomes available.
- Handler errors, panics (including `panic(nil)`), and timeouts are recorded;
  the next scheduled occurrence can still run. There is no automatic retry of
  the failed occurrence. Panic values are omitted; returned handler errors are
  logged, so keep secrets out of error messages.

`Run` provides namespace, job name, scheduled time in UTC, and an ID. Coordinated
run IDs are stable for the same namespace/name/occurrence across replicas. Local
IDs also include scheduler instance identity, so independent replicas do not
accidentally share an idempotency key. Use these IDs where useful, while keeping
business-specific idempotency and resource checks in the service.

### PostgreSQL coordination and migrations

Apply `scheduler.Schema()` using a service migration before `Start`, or enable
the built-in with `migrate.Config.SchedulerVersion` at a **new unused positive
version**. For example, if inbox/outbox/service/failure migrations already occupy
versions 1–4, add `SchedulerVersion: 5` while retaining those existing sources
and version assignments. This schema is forward-only. It is optional; local jobs
need neither PostgreSQL nor a migration. No scheduling API runs DDL automatically.

`public.gof_schedules` holds one row per namespace/job, including the normalized
schedule, timezone, timeout, next run time, and active lease. Registration uses
a stable lock order so replicas can start together. Existing rows retain their
next occurrence on restart. Replicas with conflicting schedule/timezone/timeout
configuration fail startup with `scheduler.ErrScheduleConflict`; they never
silently overwrite each other's definitions. Handler implementation changes do
not change the persisted definition.

Claims use PostgreSQL's clock and a short row-locking transaction. The winner
advances the next occurrence and commits **before** invoking the handler, so
another replica cannot duplicate a fast occurrence after the first finishes.
No SQL transaction or connection is held throughout the handler. A lease lasts
twice the job timeout, and release checks an ownership token so a stale worker
cannot release a replacement's lease. Leases are not renewed. Runtime database
errors pause shared claims and are logged/retried on the polling interval;
independent local jobs continue.

`PollInterval` defaults to 1s for each shared job; `StorageTimeout` defaults to 5s
per registration, claim, health query, or lease release. Both accept durations
of at least 1ms. Set polling appropriately for the interval and job count.
Global handler capacity is per instance; shared per-job leases coordinate across
replicas, but there is no cluster-wide limit on different jobs running together.

Missed local occurrences are skipped. Shared schedules coalesce overdue work
into one pending occurrence: after downtime or a long run, one overdue run can
execute, then the next time is calculated from the database's current clock.
There is no replay of every elapsed interval. `ScheduledAt` identifies that
pending occurrence and may be older than the actual start time.

The committed claim is not a durable execution queue. A process crash or
ambiguous claim response can lose that occurrence; it is not retried, and a
later due occurrence can run after the lease expires. A noncooperative handler
may still be executing when a replacement acquires an expired lease. External
effects must be idempotent; scheduling does not guarantee exactly-once effects.
Use the outbox/pubsub path for work that needs durable delivery after enqueue.

To change an existing coordinated definition, stop its replicas and wait for
handlers, then explicitly update or replace its row through service-owned SQL
before restarting with matching config. Do not clear active leases or assume a
renamed job shares the previous job's coordination. Removed job rows remain
until an operator removes them; there is no run-history table or automatic purge.

### Scheduling in Fx and health checks

Register jobs in the `foundation_jobs` Fx value group:

```go
func reconciliationJob(service *OrderService) scheduler.Job {
	return scheduler.Job{
		Name: "reconcile-orders", Mode: scheduler.Coordinated,
		Cron: "*/5 * * * *", Timeout: time.Minute,
		Handler: func(ctx context.Context, run scheduler.Run) error {
			return service.Reconcile(ctx, run.ID)
		},
	}
}

app := fx.New(
	foundationfx.Module,
	foundationfx.PostgresModule(),
	foundationfx.SchedulerModule,
	fx.Supply(logger.Config{Service: "orders"}),
	fx.Supply(postgres.Config{DSN: cfg.DatabaseURL}),
	fx.Provide(func(db *sql.DB) scheduler.Config {
		return scheduler.Config{Namespace: "orders", DB: db}
	}),
	fx.Provide(NewOrderService),
	fx.Provide(fx.Annotate(reconciliationJob,
		fx.ResultTags(`group:"foundation_jobs"`),
	)),
)
```

The scheduler module injects the application logger, starts registered jobs, and
cancels/joins them before their injected dependencies stop. Supply the database
through the scheduler config constructor for shared jobs. Local-only services
can omit PostgresModule and provide a config without `DB`. An empty job group
or invalid configuration fails construction; missing schemas or conflicting
shared definitions fail startup. Later startup-hook failures roll back the
scheduler by shutting it down.

Add `health.Required("scheduler", jobs.Ping)` (or `health.Optional`) to your
existing health checker to include it in `/health` and the embedded Health UI.
`Ping` checks lifecycle/storage, not whether recent business jobs succeeded.
Shutdown cancels handlers and waits for workers, including bounded lease cleanup.
If the shutdown deadline expires, handlers may still be running; keep their
dependencies alive and call `Shutdown` again after they finish.

Initialize telemetry before constructing the scheduler. Job spans carry the
handler context; `scheduler.runs` and `scheduler.run.duration` (seconds) report
job/mode/result, including failures, timeouts, cancellation, and capacity skips.
Run IDs are logged, not used as metric labels. Job duration covers the handler;
claim/release errors are logged separately. The scheduler borrows global OTel
providers and never shuts them down.
