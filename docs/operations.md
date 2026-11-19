# Running dally in production

## Storage

Pick a backend by URL in `storage.url` (or `DALLY_STORAGE_URL`):

| Backend    | Good for                              | Notes                                  |
|------------|---------------------------------------|----------------------------------------|
| PostgreSQL | Production, several servers           | 13 or later. The reference backend.    |
| SQLite     | Local development, one small server   | One server process per file. See [backends/sqlite.md](backends/sqlite.md). |
| Redis      | Teams that already run Redis          | Needs `appendonly yes`. See [backends/redis.md](backends/redis.md). |

Migrations run on startup. To run them as a separate deploy step instead,
start one server with migrations on, and the rest with
`storage.migrate = false`.

## API keys

Auth is off until you configure a key. With auth off, anyone who can reach
the server can read and change every queue, and dally logs a warning at
startup if it listens on anything other than localhost.

```toml
[[auth.keys]]
name = "web-app"
role = "client"            # schedule, poll, ack, nack, read
key_env = "DALLY_WEB_KEY"  # read the key from the environment

[[auth.keys]]
name = "ops"
role = "admin"             # also change queues, pause, retry dead messages
key_env = "DALLY_ADMIN_KEY"
```

Clients send `Authorization: Bearer <key>`. Keys must be at least 16
characters; `openssl rand -hex 32` makes a good one. `key = "..."` works too,
but keeps the secret in the file. Key names show up in logs and traces; keys
never do.

`/healthz` and `/readyz` are always open, so load balancers can use them.
`/metrics` is open unless `auth.protect_metrics = true`, in which case it needs
a key of any role.

## Health checks

- `GET /healthz` returns 200 while the process is up. Use it for liveness.
- `GET /readyz` returns 200 when storage answers, and 503 otherwise. Use it for
  readiness, so traffic only goes to servers that can do work.

## Metrics

`GET /metrics` serves Prometheus metrics:

| Metric                                  | Type      | Labels                    |
|-----------------------------------------|-----------|---------------------------|
| `dally_messages_scheduled_total`        | counter   | `queue`                   |
| `dally_messages_leased_total`           | counter   | `queue`                   |
| `dally_messages_acked_total`            | counter   | `queue`                   |
| `dally_messages_nacked_total`           | counter   | `queue`                   |
| `dally_messages_dead_total`             | counter   | `queue`                   |
| `dally_http_requests_total`             | counter   | `method`, `route`, `status` |
| `dally_http_request_duration_seconds`   | histogram | `method`, `route`         |
| `dally_queue_messages`                  | gauge     | `queue`, `status`         |
| `dally_queue_oldest_due_seconds`        | gauge     | `queue`                   |

Counters are per server, so `sum` them across servers. The gauges read shared
storage, so every server reports the same values; use `max`. They are cached
for 5 seconds. `dally_messages_dead_total` counts messages that died on a nack;
messages whose last lease expired show up in `dally_queue_messages{status="dead"}`.
The Redis backend does not report `dally_queue_oldest_due_seconds`.

Two alerts worth having:

```yaml
- alert: DallyQueueFallingBehind
  expr: max by (queue) (dally_queue_oldest_due_seconds) > 300
  for: 10m
- alert: DallyDeadMessages
  expr: increase(max by (queue) (dally_queue_messages{status="dead"})[1h:]) > 0
```

## Tracing

Build with the `otel` feature (`cargo install dally --features otel`) and set
`OTEL_EXPORTER_OTLP_ENDPOINT`, for example `http://otel-collector:4318`. Spans
go out over OTLP/HTTP with protobuf. The standard `OTEL_*` variables apply;
the service name defaults to `dally`.

## Logs

Logs go to stderr. `--log-format json` (or `DALLY_LOG_FORMAT=json`, the default
in the Docker image) writes one JSON object per line. `RUST_LOG` sets the
level, for example `RUST_LOG=dally_server=debug,info`.

## Shutting down

On SIGTERM or Ctrl-C, dally stops accepting connections, lets in-flight
requests finish (long polls end at their wait deadline, at most 30 seconds),
waits for push requests in flight, and exits. Give it at least 35 seconds
before a hard kill, for example `terminationGracePeriodSeconds: 40`.
