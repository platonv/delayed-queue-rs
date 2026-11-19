# dally design

dally is a job queue server. Applications in any language schedule work over
HTTP, and dally hands it back to workers when it is due, either by letting
workers poll for it or by pushing it to a webhook.

This document describes what dally does and the decisions behind it. It is the
reference for contributors. User-facing docs live in the README and the other
files in `docs/`.

## Goals

- **Language-agnostic.** Anything that can make an HTTP request can use dally.
  Adding background jobs to an existing web app should take one endpoint and a
  few lines of code.
- **Boring storage.** Run it on the database you already have: PostgreSQL,
  SQLite or Redis.
- **Honest guarantees.** At-least-once delivery, clearly documented, with no
  hidden ways to lose a message.
- **Easy to run.** One binary. With no configuration it starts on a local
  SQLite file.
- **Stateless servers.** Any number of `dally server` processes can share one
  database. All coordination happens in storage.

Non-goals: exactly-once delivery, pub/sub fan-out, and ordered streams (use
Kafka or NATS for those).

## Concepts

**Queue.** A named stream of messages with its own settings. A queue is
created on first use with default settings, and can be configured later.

| Setting              | Default  | Meaning                                                   |
|----------------------|----------|-----------------------------------------------------------|
| `visibility_timeout` | 30s      | How long a lease lasts before the message is handed out again |
| `max_attempts`       | 5        | Attempts before the message is marked `dead`              |
| `backoff`            | exp, 5s base, 1h cap | Delay before a retry after a failed attempt    |
| `retention`          | 7d       | How long finished messages are kept                       |
| `paused`             | false    | A paused queue accepts messages but delivers none         |
| `push`               | none     | Optional webhook endpoint (see [Push delivery](#push-delivery)) |

**Message.** A JSON payload scheduled to run at a point in time. A message has
a server-generated `id`, an optional `idempotency_key`, a `priority` (higher
runs first, default 0) and optional per-message overrides for
`visibility_timeout` and `max_attempts`.

**Lease.** A worker that takes a message gets a lease: a random token that
expires after the visibility timeout. Only the holder of a valid lease can ack
or nack the message. This guards against the classic bug where a slow worker
acks a message that was already handed to someone else.

### Message lifecycle

```
                 schedule
                    │
                    ▼
   ┌──────────► scheduled ──── cancel ────► cancelled
   │                │
   │  nack / lease  │ run_at reached, polled
   │  expired       ▼
   │  (attempts  running ───── ack ───────► succeeded
   │   left)        │
   └────────────────┤
                    │ nack / lease expired, no attempts left
                    ▼
                  dead ──── retry (manual) ──► scheduled
```

`succeeded`, `cancelled` and `dead` messages are deleted after the queue's
retention period. A message counts as `failed` in stats when it reached `dead`.

## Delivery semantics

dally delivers **at least once**. A message can be delivered more than once
when:

- a worker crashes or exceeds the visibility timeout before acking,
- a worker finishes the work but its ack never reaches the server.

Handlers must therefore be safe to run twice. The docs say this up front, and
show the usual pattern of storing a "done" marker under the message id.

When a message is due, it is delivered by priority, highest first, then by
`run_at`. dally does not promise strict ordering beyond that.

### Retries

A nack ends the current attempt. If the message has attempts left, it goes
back to `scheduled` with `run_at = now + backoff(attempt)`.

A lease that expires also ends the attempt, but without backoff: the message
can be leased again right away, as in SQS. The worker probably crashed, so
waiting longer only adds latency. If it was the last attempt, the message moves
to `dead` with the error `lease expired`. The backoff is
`min(base * 2^(attempt - 1), cap)` with ±20% jitter. A nack may also give its
own `retry_in` delay. The error text from a nack is stored on the message so
the UI can show why it failed.

### Idempotency

`idempotency_key` is unique per queue for as long as a message with that key
exists, which is until it is pruned after the retention period. Scheduling with
a key that already exists does not create a new message. It returns the
existing message with `200 OK` instead of `201 Created`. Rescheduling and
cancelling are separate operations, never a side effect of reusing a key.

### Time

All timestamps come from the storage clock (`now()` in Postgres, `TIME` in
Redis), never from the server process. That way several servers with slightly
different clocks still agree. SQLite only ever runs with a single server
process, so it uses that process's clock.

## Architecture

```
            ┌────────────┐   ┌───────────┐   ┌──────────────┐
            │ your app   │   │ dally CLI │   │  dally-ui    │  (optional, feature "ui")
            └─────┬──────┘   └─────┬─────┘   └──────┬───────┘
                  │ HTTP           │ dally-client   │ dally-client
                  ▼                ▼                ▼
            ┌───────────────────────────────────────────────┐
            │ dally-server   HTTP (axum)  · gRPC later      │
            │                push dispatcher · cron · janitor│
            ├───────────────────────────────────────────────┤
            │ dally-core     Service: the rules              │
            │                Storage trait                   │
            ├──────────────┬──────────────┬─────────────────┤
            │ postgres     │ sqlite       │ redis           │  (cargo features)
            └──────────────┴──────────────┴─────────────────┘
```

### Crates

| Crate          | What it holds                                                       |
|----------------|---------------------------------------------------------------------|
| `dally-core`   | Domain types, the `Service` (validation, lifecycle rules), the `Storage` trait and the backends behind cargo features |
| `dally-server` | HTTP API, push dispatcher, cron ticker, janitor, config, metrics    |
| `dally-client` | Rust HTTP client and a `Worker` that runs a handler with bounded concurrency |
| `dally-ui`     | Web UI: askama templates + htmx. Talks to the server only through `dally-client` |
| `dally`        | The binary: `dally server`, `dally ui`, and the client subcommands  |

Two rules keep this honest:

1. **Transports are thin.** HTTP handlers parse the request, call `Service`
   and serialize the result. All behaviour lives in `dally-core`, so a gRPC
   transport is a new thin layer later, not a rewrite.
2. **The UI and CLI are clients.** They use the public API and nothing else.
   If the UI needs something, the API grows it, and every user gets it too.

### Storage trait

The trait is small and works in terms of the operations dally needs, not
generic CRUD. Each backend implements every method atomically. The shared
conformance test suite (`dally-core/tests/conformance`) runs the same tests
against every backend.

In brief (see `crates/dally-core/src/storage/mod.rs` for the documented
version):

```rust
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    async fn ensure_queue(&self, name: &QueueName, defaults: &QueueSettings) -> Result<Queue>;
    async fn put_queue(&self, name: &QueueName, settings: &QueueSettings) -> Result<Queue>;
    async fn get_queue(&self, name: &QueueName) -> Result<Option<QueueSummary>>;
    async fn list_queues(&self) -> Result<Vec<QueueSummary>>;

    async fn schedule(&self, queue: &QueueName, messages: Vec<NewMessage>)
        -> Result<Vec<Scheduled>>;
    async fn lease(&self, queue: &QueueName, limit: u32) -> Result<Vec<Leased>>;
    async fn ack(&self, id: MessageId, lease: &LeaseToken) -> Result<Option<Message>>;
    async fn nack(&self, id: MessageId, lease: &LeaseToken, retry: Retry)
        -> Result<Option<Message>>;

    async fn get_message(&self, id: MessageId) -> Result<Option<Message>>;
    async fn list_messages(&self, filter: &MessageFilter) -> Result<Page<Message>>;
    async fn cancel(&self, id: MessageId) -> Result<Option<Message>>;
    async fn reschedule(&self, id: MessageId, run_at: RunAt) -> Result<Option<Message>>;
    async fn retry_dead(&self, queue: &QueueName, ids: Option<&[MessageId]>) -> Result<u64>;
    async fn prune(&self) -> Result<u64>;

    /// Optional; used for the oldest-due metric.
    async fn oldest_due(&self) -> Result<Option<BTreeMap<QueueName, Duration>>> { Ok(None) }

    fn subscribe(&self, queue: &QueueName) -> Subscription;
    async fn now(&self) -> Result<DateTime<Utc>>;
}
```

`None` from ack, nack, cancel and reschedule means "nothing matched"; the
`Service` then looks the message up to say whether it is missing, held under
another lease, or in the wrong status. Backoff, defaults and validation live in
the `Service`, so backends only store and fetch.

### Backends

**PostgreSQL** (13 or later) is the reference backend. `lease` is one statement using
`FOR UPDATE SKIP LOCKED`, so concurrent pollers never block each other or take
the same row. `LISTEN/NOTIFY` wakes long-polling requests as soon as a message
is scheduled. A partial index on `(queue, priority DESC, run_at)` over
`status = 'scheduled'` keeps polling fast.

**SQLite** runs in WAL mode with one writer. It supports a single server
process only, and the server refuses to start a second process against the
same file. Notifications go through an in-process channel. It is meant for
local development and small deployments.

**Redis** keeps each message in a hash, due messages in a sorted set per queue
scored by `run_at` and priority, and leases in a second sorted set scored by
expiry. Every operation that touches more than one key is a Lua script, so it
is atomic. Durability depends on Redis persistence: with default settings,
recent messages can be lost on a crash. The docs require `appendonly yes` for
production use.

## Delivery

### Pull

`POST /v1/queues/{queue}/poll` with `{"limit": 10, "wait_seconds": 20}`
returns up to `limit` leased messages. If none are due, the request waits up to
`wait_seconds` for one to arrive (long polling), so idle workers send very few
requests. The `dally-client` `Worker` is built on this.

### Push delivery

A queue may have one push endpoint:

```toml
[queues.emails.push]
url = "https://app.example.com/jobs/emails"
secret = "${EMAILS_WEBHOOK_SECRET}"
timeout = "30s"
concurrency = 8
```

The push dispatcher in each server leases messages from push queues and POSTs
them to the endpoint. A `2xx` response acks the message. Anything else,
including a timeout, nacks it with the response status and the first 1 KiB of
the body as the error text. Each request carries:

- `Dally-Message-Id`, `Dally-Queue`, `Dally-Attempt`
- `Dally-Signature: t=<unix seconds>,v1=<hex hmac-sha256(secret, "<t>.<body>")>`

The docs include a short "verify the signature" snippet for common languages.
Push is one endpoint per queue by design. Fan-out to many subscribers is
pub/sub, which dally does not do.

## Cron

Recurring jobs are defined per queue:

```toml
[[queues.reports.cron]]
name = "nightly"
schedule = "0 3 * * *"
timezone = "Europe/Bucharest"
payload = { kind = "nightly-report" }
```

Every server runs a ticker that works out the next run time for each entry and
schedules it with `idempotency_key = "cron:{name}:{run_at}"`. When several
servers do this at once, idempotency means only one message is created. No
leader election is needed.

## HTTP API

All endpoints are under `/v1` and use JSON. Errors have the shape
`{"error": {"code": "lease_expired", "message": "..."}}`. The OpenAPI spec is
generated from the code with `utoipa` and served at `/v1/openapi.json`.

| Method & path                          | Purpose                                     |
|----------------------------------------|---------------------------------------------|
| `POST /v1/queues/{queue}/messages`     | Schedule one message or a batch             |
| `POST /v1/queues/{queue}/poll`         | Lease due messages, optionally long polling |
| `POST /v1/messages/{id}/ack`           | Finish a message (`{"lease": "..."}`)       |
| `POST /v1/messages/{id}/nack`          | Fail an attempt (`lease`, `error`, `retry_in`) |
| `GET /v1/messages/{id}`                | Inspect a message                           |
| `PATCH /v1/messages/{id}`              | Reschedule                                  |
| `DELETE /v1/messages/{id}`             | Cancel                                      |
| `GET /v1/queues`                       | List queues with counts per status          |
| `GET /v1/queues/{queue}`               | Queue settings and stats                    |
| `PUT /v1/queues/{queue}`               | Update settings                             |
| `POST /v1/queues/{queue}/pause`, `/resume` | Pause or resume delivery                |
| `GET /v1/queues/{queue}/messages`      | List messages, filterable by status         |
| `POST /v1/queues/{queue}/dead/retry`   | Move dead messages back to `scheduled`      |
| `GET /v1/messages`                     | List messages across queues                 |
| `GET /v1/openapi.json`                 | This API as OpenAPI 3.1                     |
| `GET /healthz`, `GET /readyz`          | Liveness and readiness                      |
| `GET /metrics`                         | Prometheus metrics                          |

Scheduling example:

```http
POST /v1/queues/emails/messages
{
  "payload": {"to": "ana@example.com", "template": "welcome"},
  "delay": "10m",
  "idempotency_key": "welcome-email-user-42",
  "priority": 0
}
```

`delay` (a duration) and `run_at` (RFC 3339) are mutually exclusive. If
neither is given, the message runs now.

## Auth

Auth is off by default. When API keys are configured, every request needs
`Authorization: Bearer <key>`. Keys have one of two roles:

- **client**: schedule, poll, ack, nack, and read messages.
- **admin**: everything, including queue settings, retrying dead messages and
  the UI.

Keys stored in the database with a management UI may come later.

## Web UI

`dally-ui` is optional. It is compiled only with the `ui` cargo feature, and
when it is compiled in it can run:

- inside the server: `dally server --ui` mounts it under `/ui`,
- on its own: `dally ui --server https://dally.internal --api-key ...`.

It shows queues with counts and throughput, lets you browse and filter
messages, inspect payloads and errors, retry or delete dead messages, pause
queues and edit settings. It is server-rendered with askama and htmx, so the
build needs no JavaScript toolchain.

## CLI

```
dally server [--config dally.toml] [--ui]
dally ui --server URL
dally queues ls | show NAME | pause NAME | resume NAME
dally schedule QUEUE --payload JSON [--delay 10m] [--key K]
dally inspect ID
dally messages ls [--queue Q] [--status S]
dally dead retry QUEUE [ID...]
```

The client subcommands go through `dally-client`, never straight to storage.

## Configuration

A TOML file plus environment variables. `DALLY_*` variables override the file
(`DALLY_STORAGE_URL`, `DALLY_LISTEN`, ...). Storage is chosen by URL scheme:
`postgres://`, `sqlite://`, `redis://`. Migrations run on startup unless
`storage.migrate = false`. With no configuration, dally listens on
`127.0.0.1:7070` and stores data in `./dally.sqlite`.

## Background tasks

Each server runs three loops. All of them are safe to run on many servers at
once:

- **push dispatcher**: delivers push queues, as described above.
- **cron ticker**: schedules recurring jobs through idempotency keys.
- **janitor**: prunes finished messages past their retention period. Expired
  leases need no loop of their own, because they are reclaimed when a message
  is leased.

## Observability

- Logs come from `tracing`, as JSON or pretty-printed text.
- Spans can be exported with OpenTelemetry (OTLP) behind the `otel` feature.
- Prometheus metrics at `/metrics`: messages scheduled, leased, acked, nacked
  and dead per queue; queue depth by status; age of the oldest due message;
  push latency and status codes.

## Quality

- Edition 2024, with the minimum supported Rust version (MSRV) set in
  `Cargo.toml` and tested in CI.
- CI runs `cargo fmt --check`, `cargo clippy -- -D warnings`, unit tests, and
  the conformance suite against real Postgres and Redis (service containers)
  plus SQLite.
- Library crates use `thiserror` for errors and the binary uses `anyhow`.
  There is no `unwrap()` outside tests.
- Every public item has rustdoc. `docs/` holds architecture notes, the
  delivery semantics, and one page per backend describing its guarantees.
- Dual licensed under MIT or Apache-2.0.

## Out of scope for now

- The gRPC transport. The design leaves room for it.
- Hand-written SDKs for languages other than Rust. The OpenAPI spec lets people
  generate clients in the meantime.
- API keys managed in the database.
- Workflows (job dependencies, chains). These may come later.

## Moving from the prototype

The prototype keeps its git history. What carries over is the idea from
[the original article][article]: a due-time column that doubles as the lock.
The rename from `kind` to `queue`, the lease token, statuses and per-queue
config replace the rest. There are no users yet, so the database schema is
written fresh rather than migrated.

[article]: https://alexn.org/blog/2022/10/21/modeling-queue-for-delayed-messages-via-rdbms/
