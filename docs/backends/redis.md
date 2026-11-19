# Redis

```toml
[storage]
url = "redis://localhost:6379/0"
# url = "rediss://user:password@redis.internal:6380/0?prefix=myapp:dally:"
```

Redis is a good fit if you already run it and want dally to be fast without
adding a database. Any number of dally servers can share one Redis.

## Durability: turn on AOF

By default Redis keeps data in memory and only saves snapshots every few
minutes. If it crashes, everything since the last snapshot is gone: scheduled
messages, retries in progress, acks. For a job queue that means lost jobs.

Run Redis with the append-only file and an fsync every second:

```
appendonly yes
appendfsync everysec
```

With that, a crash loses at most about one second of writes. `appendfsync
always` loses nothing but is much slower. Managed Redis services usually call
this "persistence" or "AOF"; check that it is on.

## Key prefix

Every key dally writes starts with `dally:`. Set another prefix with the
`prefix` URL parameter, for example to run two dally setups on one Redis:

```
redis://localhost:6379/0?prefix=staging:dally:
```

The parameter is removed from the URL before connecting, so it never reaches
Redis.

## How it is stored

Each message is a hash. Each queue has a few sorted sets: messages waiting
for their time, messages due now in delivery order, running messages by lease
expiry, finished messages by completion time, and one index per status.
Every operation that touches more than one key is a Lua script, so it is
atomic. Scripts use Redis's own clock (`TIME`), so several dally servers agree
on what is due. The module docs in `crates/dally-core/src/storage/redis/mod.rs`
describe the keys in detail.

Scripts build key names from the data they read, so dally supports a single
Redis server (with replicas, or Sentinel for failover) but not Redis Cluster.

## Memory

Everything lives in memory. As a rough guide, a message takes a few hundred
bytes plus its payload. Finished messages are kept for the queue's retention
period (7 days by default) and then deleted, so memory use is about:

```
(messages per day × retention in days + messages waiting) × (300 B + payload)
```

Lower `retention` on busy queues if that adds up. Set `maxmemory-policy
noeviction`, the default, so Redis refuses writes when full rather than
silently evicting messages.

## Long polling

Scheduling a message publishes on the `<prefix>scheduled` channel, and every
dally server subscribes to it, so waiting workers are woken right away. If
the subscription drops, polls still find due messages within a second.

## Metrics

The Redis backend does not report `dally_queue_oldest_due_seconds`: once a due
message moves to the ready set, its score no longer holds its `run_at`, so the
answer would cost a scan. Every other metric works as with the other backends.
