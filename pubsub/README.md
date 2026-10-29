# pubsub

[Overview](../README.md) · [Development and testing](../testsupport/README.md)

## Durable pubsub

Services import `github.com/Penthious/go-foundation/pubsub` and use `*pubsub.Client`, `Message`,
`ConsumerConfig`, and `Handler`. No public signatures, aliases, options, messages,
or connection accessors expose NATS types. The initial implementation uses NATS
JetStream; changing it belongs inside GOF. Broker deployment, credentials,
stored messages, and consumer positions would still require a migration when
changing providers. The abstraction does not make those operational differences
disappear.

The public contract is durable, **at-least-once** delivery. Publishing waits for
the broker's storage acknowledgment. A timeout is ambiguous: the event may have
been stored. Give every logical event a stable ID and reuse it on retries.
Broker deduplication has a bounded window (currently two minutes by default),
so handlers must tolerate duplicate messages even when publishers reuse IDs.
For database writes plus events, use a transactional outbox in the owning
service; publishing alone is not atomic with a database transaction.

### Manual injection

```go
func consumeOrders(ctx context.Context, log *logger.Logger) (err error) {
	bus, err := pubsub.New(pubsub.Config{
		Logger:    log,
		Name:      "billing",
		Endpoints: []string{"nats://localhost:4222"},
		Topics: []pubsub.TopicConfig{{
			Name: "orders.created", MaxAge: 24 * time.Hour, MaxBytes: 64 << 20,
		}},
	})
	if err != nil {
		return err
	}

	if err := bus.Start(ctx); err != nil {
		return err
	}

	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err = errors.Join(err, bus.Shutdown(shutdownCtx))
	}()

	err = bus.Consume(ctx, pubsub.ConsumerConfig{
		Topic: "orders.created", Group: "billing",
	}, func(ctx context.Context, message pubsub.Message) error {
		// Commit idempotent business work before returning nil.
		log.Info(ctx, "order event received", logger.String("message_id", message.ID))
		return nil
	})
	if errors.Is(err, context.Canceled) {
		return nil
	}

	return err
}
```

This uses `context`, `errors`, `time`, and the module's `logger` and `pubsub`
packages. `New` validates configuration and opens no connections. `Start` connects,
checks that JetStream is enabled, and provisions configured topics. Startup
failures close their connection; topics already created remain on the broker.
`Start` is idempotent after success. Shutdown is terminal; create a new client
instead of restarting a closed one. `ErrNotStarted` and `ErrClosed` are GOF errors.

Publishing from a service with an injected client:

```go
func publishOrder(ctx context.Context, bus *pubsub.Client, eventID, orderID string) error {
	payload, err := json.Marshal(map[string]string{"order_id": orderID})
	if err != nil {
		return err
	}

	return bus.Publish(ctx, pubsub.Message{
		ID: eventID, Topic: "orders.created", Data: payload,
	})
}
```

Payloads are opaque bytes; JSON is an application choice. Topics are exact,
dot-separated names using letters, numbers, underscores, and hyphens. Wildcard
subscriptions are intentionally absent. IDs must contain 1–256 valid UTF-8 bytes
with no CR, LF, or NUL. Consumer groups use 1–200 letters, numbers, underscores, or hyphens.

### Retention and delivery

Each topic has its own disk-backed stream. Defaults are 24 hours and 64 MiB per
topic, including broker metadata, with one storage replica. Both limits apply:
expired events are removed even when unprocessed, and a full topic rejects new
publications instead of evicting older events to make room. Size retention for
your longest consumer outage. Provisioning is idempotent for matching settings;
conflicting settings fail startup rather than overwriting existing retention.
Consumers can omit `Topics` when another service or deployment provisions them.
Topic/consumer creation requires broker permissions; use separate accounts for
environments and namespace topic names where needed. GOF uses deterministic
private stream names derived from the topic; services never construct them.

Different groups each receive their own copy of every retained event. Instances
sharing the same topic and group divide that group's messages. A new group starts
at the oldest retained event; an existing group resumes its saved position.
`Consume` blocks and processes one message at a time. Run additional instances
with the same group for concurrent processing; global completion order is not
guaranteed. Configuration/setup failures, deleted subscriptions, and permanently
closed connections return to the caller. Other fetch failures retry with a delay;
normal idle fetch timeouts are not logged as errors.

Return nil from a handler only after its business work commits; GOF then waits
for the acknowledgment to be confirmed. Errors and panics cause delayed
redelivery. `RetryDelay` defaults to one second. `RetryBackoff` can supply a delay
sequence for failed deliveries; after the final entry, its last delay repeats.
The slice is copied when consumption starts. Fetch/storage errors still use
`RetryDelay`, and broker timeout redelivery uses `AckTimeout`. Zero
`MaxDeliveries` retains unlimited retries for compatibility. A positive limit
requires a `FailedStore`, as described below. Invalid envelopes follow the same
policy and are retained with their raw bytes if quarantined. Topic retention
expiry can still remove unacknowledged originals.

`AckTimeout` defaults to 30 seconds, with a minimum of 30ms. GOF renews the lease
while processing and acknowledging; it does not impose a handler deadline.
Instances sharing a group must use matching acknowledgment settings. Lease
renewal reduces concurrent redelivery but cannot eliminate duplicates during
failures. Handlers must honor cancellation and remain idempotent.

### Failed-message storage and targeted replay

Failed-message storage is optional and uses the service's PostgreSQL database.
Ordinary pubsub consumers still need no database. Apply
`migrate.Config.FailedMessagesVersion` at a new service-chosen version, or apply
[the SQL schema](../internal/schema/001_failed_messages.sql) through your existing
migration workflow. `pubsub.FailedSchema()` returns the same SQL. The SDK never
creates this table automatically; the runtime role needs SELECT, INSERT, UPDATE,
and DELETE on `public.gof_failed_messages`.

```go
failures, err := pubsub.NewFailedStore(db, pubsub.FailedStoreConfig{})
if err != nil {
	return err
}

err = bus.Consume(ctx, pubsub.ConsumerConfig{
	Topic: "orders.created",
	Group: "billing",
	MaxDeliveries: 5,
	RetryBackoff: []time.Duration{
		time.Second, 5 * time.Second, 30 * time.Second,
	},
	Failures: failures,
}, handler)
```

The same config works with `ConsumeTransactional`, preserving inbox atomicity.
All workers sharing a topic/group must use the same database, delivery limit,
and backoff policy. Consumption checks failed-message schema availability at
startup when a store is supplied. PostgreSQL failures while polling replay work
pause that consumer with a retry delay.

At the configured limit, a failed delivery is stored before its broker
acknowledgment. Records contain the logical message, original raw envelope,
topic/group, failure reason, observed delivery count, timestamp, trace context,
and replay state. Archival IDs are deterministic for the original delivery, so
retries after an ambiguous insert or acknowledgment do not overwrite an existing
record or its replay state. Reasons are UTF-8-safe and bounded to 4096 runes.

If storage fails, GOF leaves the original unacknowledged and retries archival;
it does not execute the handler again once the observed delivery limit has been
exceeded. Broker-level delivery exhaustion is deliberately disabled so a storage
failure cannot strand a final delivery. The configured limit counts **deliveries**,
including crash/acknowledgment recovery, not guaranteed completed handler calls.
An exhausted record can therefore have an ambiguous earlier business outcome.
Inbox idempotency remains necessary.

Original topic retention still applies independently: recover failed storage
before unacknowledged originals expire. Once archived, the PostgreSQL record does
not expire automatically and is independent of broker message retention.

Inspect and queue a replay explicitly:

```go
page, err := failures.ListFailed(ctx, pubsub.FailedFilter{
	Topic: "orders.created",
	Group: "billing",
	State: pubsub.FailureStored,
	Limit: 50,
})
if err != nil {
	return err
}

if len(page) != 0 {
	if err := failures.ReplayFailed(ctx, page[0].ID); err != nil {
		return err
	}
}
```

`ListFailed` orders by record ID, with a default page size of 50 and maximum 500.
Use the final ID as `FailedFilter.After` to fetch the next page. `State` is optional;
without it, stored, pending, and successfully replayed records are all included.
`GetFailed(ctx, id)` returns a complete record. Both inspection APIs include bodies
and diagnostics; protect access like other application data. Custom handler error
messages can include business data, and raw envelopes include propagated baggage.

`ReplayFailed` changes a stored record to pending work for its **original topic
and group**. It does not publish to the original topic or notify other subscriber
groups. A running `Consume` or `ConsumeTransactional` with the same store and
topic/group processes it, including when that consumer has no new broker messages.
Idle replay polling is approximately once per second. Keep a consumer running
for the target group; queuing work does not start one.

Replays preserve the original event ID, body, trace context, and baggage. Inbox
handlers use the same group/topic key, so already committed work is skipped and
the replay can complete without repeating database changes. Malformed transport
envelopes or invalid event IDs remain inspectable through `Raw` but cannot be
replayed unchanged; repair their producer/transport instead of treating partial
decoded data as a valid event.

Pending replay requests are idempotent and do not reset an active lease or its
attempt count. Workers claim one record with `FOR UPDATE SKIP LOCKED`, release
database locks before calling the handler, and fence finalization with a random
lease token. Crashes/cancellation leave the record eligible after lease expiry.
`FailedStoreConfig.Timeout` defaults to 5s for SQL operations; `ReplayTimeout`
defaults to 30s for a complete replay attempt, with a lease of twice that value.
Handlers must honor cancellation. Duplicate execution remains possible after an
ambiguous result or lease expiry; external side effects need their own idempotency.

Each replay request gets a fresh attempt budget under the consumer's current
delivery policy. Failures retry with its backoff; exhaustion returns the record
to `FailureStored`, retaining the initial `Reason` and updating `LastError` and
`ReplayAttempts`. `ReplayCount` counts explicitly queued replay rounds. Successful
work becomes `FailureReplayed` with `ReplayedAt` populated, and further replay
requests return `pubsub.ErrAlreadyReplayed`. State can be queried after queuing
because `ReplayFailed` does not wait for execution.

`DeleteFailed(ctx, id)` permits deleting only successfully replayed records.
Unprocessed/pending records cannot be discarded through this API. Plan database
capacity and retention explicitly; retain resolved records through the relevant
source redelivery horizon before cleanup. The store is an operational quarantine,
not a permanent exactly-once ledger or audit log.

For Fx, add `foundationfx.FailedMessagesModule` alongside Postgres wiring and
inject `*pubsub.FailedStore` into the service that builds its `ConsumerConfig`.
`pubsub.FailedStoreConfig` is optional. The lazy module checks schema availability
on startup when consumed, starts no workers itself, and borrows the database
lifecycle. Consumers must stop before their database closes. Register
`health.Required("failed_messages", failures.Ping)` for storage availability in
readiness and the Health UI; it does not measure replay progress or backlog.

### Connection lifecycle and observability

The default endpoint is `nats://localhost:4222`. `Timeout` defaults to five seconds
and bounds startup and individual broker operations; caller deadlines can shorten
it. Initial connection failure returns an error. After a successful connection,
the client reconnects indefinitely. Publications while disconnected are not
buffered for a later send; errors return to the caller. The timeout/acknowledgment
ambiguity still applies, so retry using the same logical event ID.

Use `tls://` endpoints and/or `TLSConfig` for certificate-verified TLS, private
CAs, or mutual TLS. Use `Token` or `Username`/`Password` for authentication. URLs
cannot contain credentials. Configuration slices and the TLS config are cloned;
as with `tls.Config.Clone`, nested objects such as certificate pools must not be
mutated concurrently. Credentials and payloads are not written by GOF's pubsub
diagnostic logs. Application handler errors are logged as provided.

Producer and consumer spans use the configured OTel providers. W3C trace context
and baggage propagate using the global propagator installed by `telemetry.New`.
Consumer contexts carry the remote trace into business work and logger calls.
DEBUG logs contain topic, group, and event ID, without payload contents. Connection
changes, handler errors, and broker errors are logged through the injected logger.

`Shutdown` cancels active consumption/publications and waits for handlers before
closing the connection. With an expired shutdown context, it forces the connection
closed and returns the context error. A handler that ignores cancellation can
outlive shutdown. Do not call shutdown from the client's own handler. Shut down
pubsub before telemetry so final logs and spans can be exported.

### Fx wiring

Add configuration to the existing Fx app and request `*pubsub.Client` from your
service constructor:

```go
fx.Supply(pubsub.Config{
	Endpoints: []string{"nats://broker:4222"},
	Topics:    []pubsub.TopicConfig{{Name: "orders.created"}},
})
```

`foundationfx.Module` injects the logger and registers `Start`/`Shutdown` hooks.
Fx opens the connection during startup, closes it during shutdown, and closes it
if a later startup hook fails. Invalid dependency graphs open no connection.
Start your service's `Consume` goroutine in its own `OnStart`, after pubsub starts.
Use a service-owned context, not the temporary startup-hook context. Cancel and
join that goroutine in the service's `OnStop`; handle unexpected `Consume` errors
by reporting the failure and stopping the app. Constructors only wire dependencies.

For a local broker, run `nats-server -js -sd /path/to/persistent/data`; the data
directory must survive container/VM recreation. The SDK does not deploy a broker
or configure backups, authentication, or clustering. See the official
[JetStream documentation](https://docs.nats.io/concepts/jetstream) and
[Go client documentation](https://pkg.go.dev/github.com/nats-io/nats.go/jetstream).
