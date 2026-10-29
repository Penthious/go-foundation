# inbox

[Overview](../README.md) · [Development and testing](../testsupport/README.md)

## Transactional inbox

Use `pubsub.Client.ConsumeTransactional` for handlers whose database changes must
run once per logical event and consumer. The inbox inserts a unique marker,
executes the handler, and commits both in **one PostgreSQL transaction**. The
pubsub consumer acknowledges only after commit. Committed duplicates skip the
handler and are acknowledged normally. Other errors and handler panics roll back
the transaction and follow pubsub's existing redelivery behavior.

Apply [the initial inbox migration](../internal/schema/001_create_inbox.sql) through
the service's migration process first. `inbox.Schema()` returns the same SQL.
It creates `public.gof_inbox` with a primary key on `(consumer, message_id)`;
the SDK never runs DDL automatically. The runtime role needs INSERT and SELECT.
Only consumer/event identifiers and a timestamp are stored; payloads are not
copied into the ledger.

With the migration runner, opt in through `migrate.Config.InboxVersion` at a
unique service-chosen version instead of copying this schema into a SQL file.

### Pubsub integration

```go
store, err := inbox.New(db, inbox.Config{})
if err != nil {
	return err
}

if err := store.Ping(ctx); err != nil {
	return err
}

err = bus.ConsumeTransactional(ctx, pubsub.ConsumerConfig{
	Topic: "orders.created",
	Group: "billing",
}, store, func(ctx context.Context, tx *sql.Tx, message pubsub.Message) error {
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO invoices (event_id, payload) VALUES ($1, $2)",
		message.ID, message.Data,
	); err != nil {
		return fmt.Errorf("create invoice: %w", err)
	}

	return outbox.Enqueue(ctx, tx, pubsub.Message{
		ID: "invoice.created:" + message.ID,
		Topic: "invoices.created",
		Data: message.Data,
	})
})
if err != nil && !errors.Is(err, context.Canceled) {
	return err
}
```

The example's invoice schema and event payload contract belong to the service.
Provision both topics before their publishers run, and apply the outbox migration
when using `outbox.Enqueue`. This commits the invoice, inbox marker, and outgoing
event together. The inbox owns commit/rollback: the handler must use the supplied
`*sql.Tx` for all protected writes, must not finish or retain that transaction,
and must use the supplied context for its queries.

`ConsumeTransactional` is blocking, like `Consume`; supervise it in a service-owned
goroutine, handle unexpected exits, and cancel/join it before shutting down the
database and pubsub client. Its existing heartbeat renews the delivery lease while
the transaction runs. Check `store.Ping` at startup, or use the Fx module below;
missing-schema errors during processing otherwise follow normal pubsub retries.
Regular `Consume` remains available for handlers that do not use an inbox.

### Consumer identity and transaction behavior

The adapter uses `Group + ":" + Topic` as its stable inbox consumer namespace.
For example, `billing:orders.created` and `shipping:orders.created` process
independently, as do the same group on different topics. Service instances sharing
a group/topic share its ledger. Renaming the group creates a new namespace and
can reprocess retained events. Keep event IDs stable across redeliveries and do
not reuse an ID for different content; the inbox deduplicates by key, without
comparing payloads. Keys are case-sensitive and compared exactly.

Concurrent inserts of the same key wait on PostgreSQL's unique constraint. If
the first transaction commits, the waiting delivery skips the handler; if it
rolls back, a later transaction can process it. A worker crash before commit
leaves no committed marker or business writes. A crash or lost broker acknowledgment
after commit can cause redelivery, which the persisted marker skips. An ambiguous
database commit returns an error; retry the **same key** to resolve its outcome.
These semantics protect writes in this PostgreSQL transaction. HTTP calls,
emails, and other external effects need their own idempotency or an outbox event.

`inbox.Config.Timeout` defaults to 30s, covering pool acquisition, duplicate-key
waits, the handler, and commit. Earlier caller deadlines still apply. Handlers
must honor cancellation; GOF cannot terminate arbitrary callback code that ignores
its context. `Isolation` defaults to `sql.LevelReadCommitted`; repeatable-read
and serializable transactions are also supported. Serialization/deadlock errors
return to the caller for retry; the inbox does not rerun a handler internally.

Markers **do not expire automatically**. Deleting one makes the event eligible
for processing again. Keep markers for the full redelivery/manual-replay horizon
your service must protect, and plan ledger retention explicitly. Broker-side
deduplication windows do not replace this ledger.

### Direct processing and Fx

The storage API is independent of pubsub and has no broker imports:

```go
processed, err := store.Process(ctx, inbox.Key{
	Consumer: "billing:orders.created",
	MessageID: eventID,
}, func(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", amount, accountID)
	return err
})
if err != nil {
	return err
}

if !processed {
	return nil // Already committed by an earlier delivery.
}
```

`true, nil` means a new event committed; `false, nil` means a committed duplicate
was skipped. Any error means the caller should handle/retry the event with the
same key. Consumer namespaces must contain 1–512 valid UTF-8 bytes and cannot be
blank; message IDs contain 1–256 bytes. Neither may contain CR, LF, or NUL.
`New` validates configuration without querying the database or starting workers;
the store borrows `*sql.DB` and has no separate shutdown method.

Add `foundationfx.InboxModule` alongside `foundationfx.PostgresModule()` and
request `*inbox.Store` in your service constructor. `inbox.Config` is optional:

```go
foundationfx.InboxModule,
fx.Supply(inbox.Config{Timeout: 15 * time.Second}),
```

When consumed, the module checks the schema during startup, after its database
starts. It starts no consumer goroutines. A service constructor receiving the
store and pubsub client should register its consumer lifecycle hooks, starting
`ConsumeTransactional` with a service-lifetime context and joining it on shutdown.
An unused inbox provider needs no database configuration. Register
`health.Required("inbox", store.Ping)` with your checker to include storage
availability in readiness and the Health UI. Ping checks connectivity and table
columns, not write permissions or consumer activity.

Initialize telemetry before creating the store. `inbox process` spans preserve
the consumer trace and record the outcome. `inbox.messages` and
`inbox.process.duration` use `result=processed|duplicate|failure`; a processed
result is recorded only after a successful commit. Errors retain their causes
for callers; handler panic values are omitted. The inbox itself does not log
errors, so pubsub or the service can report them once.
