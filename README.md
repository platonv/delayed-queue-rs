# dally

[![CI](https://github.com/platonv/delayed-queue-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/platonv/delayed-queue-rs/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

A job queue server written in Rust. You schedule work over HTTP, and dally
hands it back when it is due, either to workers that poll for it or by POSTing
it to your app.

```sh
curl -X POST localhost:7070/v1/queues/emails/messages \
  -d '{"payload": {"to": "ana@example.com"}, "delay": "10m"}'
```

- **Any language.** If it can make an HTTP request, it can use dally.
- **Your database.** PostgreSQL, SQLite or Redis, all three passing one
  shared conformance test suite.
- **Delayed and recurring jobs.** Run in ten minutes, at 3 a.m. tomorrow, or
  every night from a cron expression.
- **Retries that make sense.** Exponential backoff, a dead-letter state, and
  the error from every failed attempt.
- **No duplicates from retried requests.** Idempotency keys make scheduling
  safe to retry.
- **Push or pull.** Let dally call your webhook, or run workers that long-poll.
- **A web UI.** Optional, and built on the same public API.
- **One binary.** With no configuration it starts on a local SQLite file.

> **Status:** dally works end to end and is covered by about 170 tests, but
> it has not been released or run in production yet. The API may change
> before 1.0. See [Status and roadmap](#status-and-roadmap).

## Why dally

Most job queues tie you to a language (Sidekiq, Celery, Oban) or to a broker
you have to run next to your database. dally is a small, separate server with
an HTTP API, so any app can use it, and it keeps its state in a database you
probably already run.

It started as `delayed-queue-rs`, a prototype of the approach in Alexandru
Nedelcu's article
[Modeling a queue for delayed messages via an RDBMS](https://alexn.org/blog/2022/10/21/modeling-queue-for-delayed-messages-via-rdbms/):
one row per message with a due-time column that also works as the lock, so a
plain SQL table makes a reliable delayed queue. dally keeps that idea and adds
leases, retries, idempotency, per-queue settings and more backends around it.
The name means "to linger, to delay".

## How it works

Each message is a row (or a Redis hash) with a `status`, a `run_at` time and a
JSON payload. Scheduling inserts a `scheduled` row with `run_at` set to now
plus the delay. Nothing moves the message until a worker asks for it.

When a worker polls, dally takes the due messages (`scheduled` with
`run_at <= now()`, or `running` with an expired lease) in priority order,
marks them `running`, and gives each a random lease token that expires after
the queue's visibility timeout. Only the holder of a valid lease can ack or
nack the message, so a slow worker cannot ack a message that was already
handed to someone else.

```mermaid
sequenceDiagram
    participant App
    participant dally as dally server
    participant DB as Storage
    participant Worker

    App->>dally: POST /v1/queues/emails/messages (delay 10m)
    dally->>DB: insert status=scheduled, run_at=now()+10m
    Worker->>dally: POST /v1/queues/emails/poll (wait 20s)
    Note over dally,DB: long poll: wait until a message is due
    dally->>DB: lease due rows (FOR UPDATE SKIP LOCKED)
    DB-->>dally: status=running, lease token, lease_expires_at
    dally-->>Worker: message + lease
    Worker->>dally: POST /v1/messages/{id}/ack (lease)
    dally->>DB: status=succeeded
```

If the worker nacks, the message goes back to `scheduled` with
`run_at = now + backoff(attempt)`. If the lease expires, the message can be
leased again right away. After the last attempt it becomes `dead` and stays
there until you retry it. Finished messages are pruned after the queue's
retention period.

How each backend does this:

- **PostgreSQL** (the reference backend) leases in one `UPDATE ... FROM
  (SELECT ... FOR UPDATE SKIP LOCKED)` statement, so any number of servers can
  poll the same queue without blocking each other or taking the same row.
  `LISTEN/NOTIFY` wakes long polls on every server as soon as a message is
  scheduled, and a partial index over `status = 'scheduled'` keeps polling
  fast.
- **SQLite** runs in WAL mode for a single server process, for local
  development and small deployments.
- **Redis** keeps due messages in a sorted set per queue and leases in a
  second one scored by expiry. Every multi-key operation is a Lua script, so
  it is atomic.

All timestamps come from the storage clock, never the server process, so
several servers with skewed clocks still agree on what is due. Servers are
stateless: all coordination happens in storage.

The code is a Cargo workspace:

| Crate | What it does |
|---|---|
| `dally-core` | Queue rules (`Service`), the `Storage` trait and the three backends |
| `dally-server` | HTTP API (axum), push dispatcher, cron ticker, janitor, auth, metrics |
| `dally-client` | Rust client and a `Worker` that polls and runs a handler |
| `dally-ui` | Optional web UI, a client of the public API |
| `dally` | The binary: `dally server`, `dally ui` and client subcommands |

[docs/design.md](docs/design.md) goes into the details and the reasons behind
them.

## Quickstart

You need Rust 1.94 or later. dally is not on crates.io yet, so build it from
this repository:

```sh
git clone https://github.com/platonv/delayed-queue-rs dally
cd dally
```

The quickest start uses a local SQLite file and needs nothing else:

```sh
cargo run --release -p dally -- server --ui
```

To use PostgreSQL, start one in Docker and point dally at it. Migrations run
automatically when the server starts:

```sh
docker run --rm -d --name dally-postgres \
  -e POSTGRES_PASSWORD=dally -p 5432:5432 postgres:17

DALLY_STORAGE_URL=postgres://postgres:dally@localhost:5432/postgres \
  cargo run --release -p dally -- server --ui
```

dally listens on `127.0.0.1:7070`, and the web UI is at
<http://127.0.0.1:7070/ui>. To install the binary instead, run
`cargo install --git https://github.com/platonv/delayed-queue-rs dally`. There
is also a [Dockerfile](Dockerfile).

## Usage

### Over HTTP

Schedule a message. The queue is created on first use:

```sh
curl -X POST localhost:7070/v1/queues/emails/messages \
  -H 'content-type: application/json' \
  -d '{"payload": {"to": "ana@example.com"}, "delay": "10m", "idempotency_key": "welcome-42"}'
```

Take it, do the work, and ack it:

```sh
curl -X POST localhost:7070/v1/queues/emails/poll -d '{"wait": "20s"}'
# {"messages": [{"id": "0193...", "payload": {...}, "lease": "4f1c...", ...}]}

curl -X POST localhost:7070/v1/messages/0193.../ack -d '{"lease": "4f1c..."}'
```

If the worker crashes or does not ack within the queue's visibility timeout
(30 seconds by default), the message is handed out again. If the worker
reports a failure with `/nack`, dally retries it with backoff, up to five
attempts, and then marks it dead.

The whole API is described in [docs/openapi.json](docs/openapi.json), which a
running server also serves at `/v1/openapi.json`.

### From Rust

```toml
[dependencies]
dally-client = { git = "https://github.com/platonv/delayed-queue-rs" }
```

```rust
use std::time::Duration;

use dally_client::{Client, Job, Worker};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Welcome {
    user_id: u64,
}

#[tokio::main]
async fn main() -> dally_client::Result<()> {
    let client = Client::new("http://127.0.0.1:7070")?;

    // Send the welcome email in ten minutes. Signing up twice schedules it once.
    client
        .schedule("emails", &Welcome { user_id: 42 })
        .delay(Duration::from_secs(600))
        .idempotency_key("welcome-42")
        .send()
        .await?;

    // Run jobs from the queue until Ctrl+C. Ok acks, an error or panic nacks.
    Worker::new(client, "emails")
        .concurrency(8)
        .run_until(
            async {
                let _ = tokio::signal::ctrl_c().await;
            },
            |job: Job<Welcome>| async move {
                println!("welcome, user {}", job.payload.user_id);
                Ok::<(), std::io::Error>(())
            },
        )
        .await
}
```

### From the command line

The `dally` binary is also a client for a running server:

```sh
dally schedule emails --payload '{"to": "ana@example.com"}' --delay 10m
dally queues ls
dally messages ls --queue emails --status dead
dally dead retry emails
```

## Two ways to deliver

**Pull.** Workers call `POST /v1/queues/{queue}/poll` and ack or nack each
message. Long polling means an idle worker makes one request every 20 seconds
or so, not hundreds.

**Push.** Give a queue an endpoint and dally POSTs each message to it. A 2xx
response acks the message; anything else is retried. Requests are signed with
HMAC-SHA256 so your app can check they came from dally.

```toml
[queues.emails.push]
url = "https://app.example.com/jobs/emails"
secret = "..."
```

Push is the quickest way to add background jobs to an existing web app: the
job handler is just another endpoint. See [docs/push.md](docs/push.md).

## Recurring jobs

```toml
[[queues.reports.cron]]
name = "nightly"
schedule = "0 3 * * *"
timezone = "Europe/Bucharest"
payload = { kind = "nightly-report" }
```

Every server schedules each run with the idempotency key
`cron:{name}:{run_at}`, so running several servers creates each run once,
without leader election.

## Web UI

```sh
dally server --ui                          # served at http://127.0.0.1:7070/ui
dally ui --server https://dally.internal   # or on its own, for a remote server
```

The UI shows queues and their counts, lets you browse messages by status,
inspect payloads and errors, retry dead messages, cancel or run waiting ones,
and pause queues. It talks to the server through the public API like any
other client. When the server has API keys, you log in with yours, and the UI
can do exactly what that key allows. Build without the `ui` feature to leave
it out entirely.

## Configuration

Settings come from a TOML file (`dally server --config dally.toml`) and
`DALLY_*` environment variables, which win:

```toml
listen = "0.0.0.0:7070"

[storage]
url = "postgres://dally@db/dally"   # or sqlite://dally.sqlite, redis://cache:6379

[defaults]                           # for queues created on first use
visibility_timeout = "30s"
max_attempts = 5
retention = "7d"

[queues.reports]
max_attempts = 3
backoff = { base = "1m", cap = "1h" }
```

```sh
DALLY_STORAGE_URL=postgres://dally@db/dally dally server
```

[.env.example](.env.example) lists the environment variables used in
development. API keys, Prometheus metrics and OpenTelemetry tracing are
covered in [docs/operations.md](docs/operations.md).

## Guarantees

dally delivers each message **at least once**. A message can be delivered
twice if a worker finishes the work but its ack is lost, so make handlers safe
to run twice. [docs/semantics.md](docs/semantics.md) explains exactly when this
happens and how to deal with it.

## Status and roadmap

Implemented and tested:

- Queues with per-queue visibility timeout, max attempts, backoff, retention,
  and pause/resume
- Delays, priorities, idempotency keys, leases, retries and a dead-letter
  state
- PostgreSQL, SQLite and Redis backends behind one conformance suite
- HTTP API with long polling, push delivery with HMAC signatures, and cron
- API keys with client and admin roles, Prometheus metrics, OpenAPI, and
  optional OpenTelemetry tracing
- Rust client with a `Worker`, the `dally` CLI, and the web UI

Not done yet:

- A first release on crates.io and a published Docker image
- Production use. dally has only been run in tests and local setups so far
- A gRPC transport (the design leaves room for it)
- Client SDKs for other languages. Until then, generate one from the OpenAPI
  spec
- API keys managed in the database instead of the config file
- Workflows: job dependencies and chains

Not planned: exactly-once delivery, pub/sub fan-out, and ordered streams. Use
Kafka or NATS for those.

## Development

```sh
docker compose up -d   # Postgres on 55432, Redis on 56379
export DALLY_TEST_POSTGRES_URL=postgres://postgres:dally@localhost:55432/postgres
export DALLY_TEST_REDIS_URL=redis://localhost:56379
cargo test --workspace --all-features
```

Without those variables, the Postgres and Redis tests are skipped. CI runs
`cargo fmt`, `cargo clippy` with warnings as errors, and the full test suite
against real Postgres and Redis. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Documentation

- [Design](docs/design.md): how dally works and why
- [Delivery semantics](docs/semantics.md)
- [Push delivery](docs/push.md)
- [Running in production](docs/operations.md): storage, API keys, metrics, tracing
- [SQLite](docs/backends/sqlite.md) and [Redis](docs/backends/redis.md) backend notes
- [Changelog](CHANGELOG.md)

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT), at your option.

Unless you explicitly state otherwise, any contribution you submit for
inclusion in dally shall be dual licensed as above, without any additional
terms or conditions.
