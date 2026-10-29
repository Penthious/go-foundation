# cache

[Overview](../README.md) · [Development and testing](../testsupport/README.md)

## Caching

The `cache` package exposes one concrete `*cache.Client` for both backends.
Services use `Get`, `Set`, `Delete`, `Ping`, and `Close`; Redis types and commands
stay private. Values are opaque bytes, so services own JSON or other encoding.
There is no automatic backend fallback or two-level cache.

```go
store, err := cache.New(cache.Config{
	Backend:   cache.Redis,
	Namespace: "orders-v1",
	DefaultTTL: 5 * time.Minute,
	Redis: &cache.RedisConfig{
		Address: cfg.RedisAddress, Password: cfg.RedisPassword,
	},
})
if err != nil {
	return err
}

defer store.Close()

if err := store.Ping(ctx); err != nil {
	return err
}

if err := store.Set(ctx, "order:123", []byte(`{"id":"123"}`), time.Minute); err != nil {
	return err
}

value, err := store.Get(ctx, "order:123")
switch {
case errors.Is(err, cache.ErrMiss):
	// Fetch from the database, then populate the cache with an appropriate TTL.
case err != nil:
	// The service decides whether to bypass the cache or fail the operation.
	return err
default:
	// Decode value into your service-owned type.
	_ = value
}

if err := store.Delete(ctx, "order:123"); err != nil {
	return err
}
```

This uses `errors`, `time`, and `github.com/Penthious/go-foundation/cache`. `cfg` and `ctx` are owned
by the service. `New` validates configuration without network I/O. Reuse one
client per configured cache; stop its users before calling `Close`.

For an in-process cache, switch the constructor configuration:

```go
store, err := cache.New(cache.Config{
	Backend: cache.Memory,
	Namespace: "orders-v1",
	Memory: cache.MemoryConfig{
		MaxEntries: 10_000,
		MaxBytes:   64 << 20,
	},
})
if err != nil {
	return err
}
```

The same service code works with either client. Omitting `Backend` selects
memory, but a namespace is always required; supplying Redis config without
selecting `cache.Redis` fails validation. Each memory client is independent,
including clients with the same namespace. Replicas do not share memory entries.
Redis clients share data when address, database, and namespace match.

### Cache behavior and limits

- `Get` returns an independent byte slice. Missing and expired entries return
  `cache.ErrMiss`; a stored empty/nil value is an empty **hit**.
- `Set` replaces the value and its TTL. Zero TTL selects `DefaultTTL` (5m by
  default). Explicit TTLs must be at least 1ms and round up to whole milliseconds;
  negative TTLs and durations that cannot be represented safely are rejected.
  There is no non-expiring mode. Redis controls expiry using its server clock.
- `Delete` is idempotent, and removes only the exact namespaced key. GOF exposes
  no flush-database, namespace scan, or wildcard deletion operation.
- `Namespace` is 1–128 ASCII letters, digits, dots, underscores or hyphens.
  Stored keys are `gof:cache:<namespace>:<key>`. Keys must be nonblank, valid UTF-8,
  at most 1024 bytes, and contain no NUL/CR/LF. Include service, schema version,
  tenant, and authorization scope in your key design where appropriate.
- `MaxValueBytes` defaults to 1 MiB and may be configured up to 512 MiB. Oversized
  values return `cache.ErrValueTooLarge`. Redis reads return at most the limit
  plus one byte, including values written by other clients, and never return a
  truncated value as a hit. Oversized writes do not replace an existing entry.
- Memory defaults to 10,000 entries and 64 MiB of retained **key/value bytes**,
  including the stored prefix. Map/list metadata and temporary input/result
  copies add memory overhead. Least recently used entries are evicted under
  pressure; expired entries are reclaimed before live entries. Expiry cleanup
  happens on access and capacity pressure, with no sweeper goroutine.
- Clients are safe for concurrent use. Do not mutate input slices while `Set`
  is running. `Close` is idempotent, releases local resources, and makes later
  operations return `cache.ErrClosed`. It never deletes shared Redis entries.

`Timeout` defaults to 2s and bounds each operation, including pool waits and
Redis I/O; an earlier caller deadline wins. Canceled contexts are passed through
to the client. A cancellation during socket I/O can take until the active socket
deadline to return. Context cancellation/deadline errors remain classifiable;
other Redis failures return `cache.ErrUnavailable`, without driver errors,
keys, values, addresses, or credentials. A failed write can still have reached
Redis; no distributed transaction with your database is implied.

### Redis configuration and telemetry

`RedisConfig` supports a standalone Redis host/port, ACL username/password,
database number, pool size, and standard `*tls.Config`. Address defaults to
`localhost:6379`, database to 0, and the pool to 10 connections. The TLS config
is cloned; configure custom roots and verified server names before construction.
Use TLS on untrusted networks. Plain TCP is available for a trusted local setup.
Cluster and Sentinel are not configured through this initial adapter.

Command retries are disabled to avoid replaying ambiguous writes over concurrent
updates. Subsequent service calls can reconnect after an outage; GOF does not
silently switch to memory. Redis ACLs need `PING`, `SET`, `DEL`, `EVALSHA`, `EVAL`,
`GETRANGE`, and `EXISTS` for cache operations, plus connection authentication and
database selection as applicable. Reads use a short script to atomically preserve
empty/missing semantics while bounding the response. A successful `Ping` checks
connectivity/authentication, not every data-command ACL permission.

Initialize telemetry before `cache.New`. The cache records operation spans,
`cache.operations`, and `cache.operation.duration` (seconds), using only backend,
operation, and result attributes. Results distinguish hits, misses, successes,
and errors; a miss is not a failed span. Keys, namespaces, values, credentials,
and raw backend errors are not added to GOF telemetry. Standard tracer/meter
provider overrides are optional and borrowed; closing the cache never closes
those providers. The cache does not duplicate application error logging.

### Cache in Fx and health checks

```go
app := fx.New(
	foundationfx.CacheModule(),
	fx.Supply(cache.Config{
		Backend: cache.Redis, Namespace: "orders-v1",
		Redis: &cache.RedisConfig{
			Address: cfg.RedisAddress, Password: cfg.RedisPassword,
		},
	}),
	fx.Provide(NewOrderService), // Receives *cache.Client alongside other dependencies.
)
```

`CacheModule()` is opt-in and works without `foundationfx.Module`. Call it once
per Fx app; it validates config when consumed, pings on startup, and closes on
shutdown, construction failure, or startup rollback. Startup fails if a consumed
Redis cache cannot be reached. An unused module creates no client and needs no
config. If cache availability must not gate startup, provide `cache.New` through
your own lifecycle wiring instead and choose the service's failure policy.

Register `Ping` with the existing health checker to show the cache in `/health`
and the embedded Health UI:

```go
func serviceHealth(store *cache.Client) (*health.Checker, error) {
	return health.New(health.Optional("cache", store.Ping))
}
```

Provide this function with `fx.Provide(serviceHealth)` when using
`foundationfx.HTTPServerModule`; use `health.Required` if a failed cache should
make the running service unready. Optional health checks do not override the
cache module's startup policy.

Keep authoritative state in your database. The service owns cache invalidation,
cache-aside loading, and handling concurrent misses/stale repopulation. This
initial API does not add distributed locks, singleflight, or background refresh.
Memory restarts and eviction lose entries; Redis can also evict entries according
to its server policy, so callers must always handle misses.
