# outbox

[Overview](../README.md) · [Development and testing](../testsupport/README.md)

## Transactional outbox

Use `outbox.Enqueue` when a business write must commit together with an event.
It accepts the service's existing `*sql.Tx` and a GOF `pubsub.Message`. A relay
publishes committed rows and deletes them only after the publisher confirms
durable acceptance. PostgreSQL and broker types remain behind the existing GOF
and standard-library APIs. The pattern avoids the failure gap between committing
business data and separately publishing an event; see the
[transactional outbox pattern](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html).

### Migration and transactional writes

Apply [the initial SQL migration](../internal/schema/001_create_outbox.sql) through
each service's migration process before running a relay. `outbox.Schema()` returns
the same SQL for services that embed migrations in Go. It creates the fixed
`public.gof_outbox` table and its polling index. Apply it once as a versioned
migration; the SDK never runs DDL automatically. Grant the runtime role SELECT,
INSERT, UPDATE, and DELETE on the table. The business transaction and relay must
connect to the **same service database**.

With the migration runner, opt in through `migrate.Config.OutboxVersion` at a
unique service-chosen version instead of copying this schema into a SQL file.

```go
func createOrder(ctx context.Context, db *sql.DB, orderID string, payload []byte) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "INSERT INTO orders (id) VALUES ($1)", orderID); err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	if err := outbox.Enqueue(ctx, tx, pubsub.Message{
		ID: "order.created:" + orderID,
		Topic: "orders.created",
		Data: payload,
	}); err != nil {
		return err
	}

	return tx.Commit()
}
```

`Enqueue` validates the topic and event ID using `pubsub.Message.Validate`, copies
the payload into PostgreSQL, and persists trace context/baggage. It never commits,
rolls back, or publishes. Roll back the business transaction on any error. Rolled
back and uncommitted rows are invisible to the relay. Nil payloads become empty
bytes; otherwise binary bytes are preserved. Marshal JSON in the service if the
event contract uses JSON.

Use a stable ID for the **logical event**, including on retries after an ambiguous
commit or publish. Concurrent queued events cannot share an ID; duplicate inserts
return a database error, without overwriting or silently discarding a message.
Successfully delivered rows are deleted, so this table is not a permanent
deduplication ledger or event history. Never recycle IDs for different events.

### Relay and Fx lifecycle

Add the opt-in relay module to an app with Postgres and pubsub configured:

```go
app := fx.New(
	foundationfx.Module,
	foundationfx.PostgresModule(),
	foundationfx.OutboxModule,
	fx.Supply(logger.Config{Service: "orders"}),
	fx.Supply(postgres.Config{DSN: cfg.DatabaseURL}),
	fx.Supply(pubsub.Config{
		Endpoints: []string{cfg.BrokerURL},
		Topics: []pubsub.TopicConfig{{Name: "orders.created"}},
	}),
	fx.Supply(outbox.Config{
		PollInterval: time.Second,
		RetryDelay: 5 * time.Second,
		Timeout: 10 * time.Second,
	}),
)
```

`outbox.Config` is optional in Fx. Its logger defaults to the injected service
logger. The module constructs one `*outbox.Relay`, checks its schema during
startup, and runs it with a service-lifetime context. Shutdown and startup rollback
cancel and join the worker **before** closing the database or pubsub connection.
An unexpected worker exit requests Fx shutdown. No worker starts during graph
construction. Broker startup still follows pubsub's connectivity check; after
startup, transient failures leave events queued for retry.

Producer-only services can use `Enqueue` with their database and omit the relay
module entirely. A separate process can run the relay against that same database,
allowing producers to start and queue work while the broker is unavailable.

Without Fx, construct a relay from borrowed dependencies and call its blocking
`Run(ctx)` method in a service-owned goroutine:

```go
relay, err := outbox.New(db, bus.Publish, outbox.Config{Logger: log})
if err != nil {
	return err
}

// Run blocks until canceled; the service supervises this goroutine.
err = relay.Run(ctx)
if err != nil && !errors.Is(err, context.Canceled) {
	return err
}
```

Cancel and join `Run` before shutting down its database, broker client, logger,
or telemetry SDK. The publisher function must respect cancellation and return
nil only after durable acceptance. `bus.Publish` satisfies that contract; a custom
backend can supply the same function signature without changing the producer.

### Retries, concurrency, and observability

Each worker claims one due event atomically using `FOR UPDATE SKIP LOCKED`, with
a random lease token, then releases database locks **before** publishing. Multiple
workers or replicas can share the table. Acknowledgments and retry scheduling
require the same token, so a stale worker cannot delete or change an event that
another worker has reclaimed. This does not prevent duplicate publications.

Defaults are a 1s polling interval after an empty queue or error, a 5s retry delay,
and a 10s timeout covering a complete claim/publish/acknowledgment cycle. Durations
must be at least 1ms. The database lease lasts twice `Timeout`; configure the
timeout to fit your broker and database latency. Failed publications and publisher
panics retain the row. If cancellation, a crash, or an ambiguous database result
prevents retry scheduling, the lease expires and makes the event eligible again.
Leases use the database's clock; workers do not compare local wall clocks.

Delivery is **at least once**. A broker may accept a message just before the worker
crashes or loses the database acknowledgment. The event will be retried with its
original ID. NATS ID deduplication has a finite window; consumers must be
idempotent independently of that window. Event order is not guaranteed across
retries, concurrent transactions, or workers; include aggregate versions in event
contracts when consumers need to detect stale/out-of-order changes.

Retries continue indefinitely; events are never silently dropped or moved to a
dead-letter queue. Monitor failures and backlog, and repair failed topics,
credentials, oversized payloads, or invalid event data. Broker message retention
still applies after publication. For example, queue depth and oldest pending age
can be inspected with:

```sql
SELECT count(*) AS pending,
       coalesce(extract(epoch FROM clock_timestamp() - min(created_at)), 0) AS oldest_seconds
FROM public.gof_outbox;
```

Initialize telemetry before constructing the relay. Enqueue and relay spans
preserve the originating trace through PostgreSQL and into pubsub; persisted
baggage follows the same configured propagator. `outbox.publish.attempts` and
`outbox.publish.duration` record publisher results (`success`/`failure`). A successful
publication metric means the publisher accepted the event; a later database
acknowledgment can still fail. Logs include IDs, topics, attempt numbers, and
errors without directly logging `Data`. Keep custom publisher errors free of
payloads and secrets. The table contains application payloads and propagated
baggage until delivery, so use the same access controls as business data.

To include storage availability in `/readyz`, `/health`, and the Health UI, add
`health.Required("outbox", relay.Ping)` to the service's checker factory. This
checks connectivity and table columns, not worker activity, broker health, queue
age, or write permissions. Services that can continue accepting queued writes
during a broker outage can make the broker's health check optional.
