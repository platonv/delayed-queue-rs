# dally

A job queue server. Schedule work over HTTP, and dally hands it back when it
is due, either to workers that poll for it or by POSTing it to your app.

```sh
curl -X POST localhost:7070/v1/queues/emails/messages \
  -d '{"payload": {"to": "ana@example.com"}, "delay": "10m"}'
```

- **Any language.** If it can make an HTTP request, it can use dally.
- **Your database.** PostgreSQL, SQLite or Redis.
- **Delayed and recurring jobs.** Run in ten minutes, at 3 a.m. tomorrow, or
  every night.
- **Retries that make sense.** Exponential backoff, a dead-letter state, and
  the error from every failed attempt.
- **No duplicates from retried requests.** Idempotency keys make scheduling
  safe to retry.
- **Push or pull.** Let dally call your webhook, or run workers that long-poll.
- **One binary.** It starts with zero configuration on a local SQLite file.

> dally is young. The API may change before 1.0.

## Quickstart

```sh
cargo install dally
dally server
```

dally listens on `127.0.0.1:7070` and stores everything in `./dally.sqlite`.

Schedule a message:

```sh
curl -X POST localhost:7070/v1/queues/emails/messages \
  -H 'content-type: application/json' \
  -d '{"payload": {"to": "ana@example.com"}, "idempotency_key": "welcome-42"}'
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
job handler is just another endpoint.

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

## Guarantees

dally delivers each message **at least once**. A message can be delivered
twice if a worker finishes the work but its ack is lost, so make handlers safe
to run twice. [docs/semantics.md](docs/semantics.md) explains exactly when this
happens and how to deal with it.

## Documentation

- [Design](docs/design.md): how dally works and why
- [Delivery semantics](docs/semantics.md)
- [Push delivery](docs/push.md)
- [Contributing](CONTRIBUTING.md)

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT), at your option.

Unless you explicitly state otherwise, any contribution you submit for
inclusion in dally shall be dual licensed as above, without any additional
terms or conditions.
