# Changelog

All notable changes to dally are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

The first release. Highlights:

- Queues with per-queue settings: visibility timeout, max attempts, backoff,
  retention, pause and resume.
- Messages with delays, priorities, idempotency keys, leases, retries with
  exponential backoff, and a dead-letter state.
- PostgreSQL, SQLite and Redis backends, all passing one conformance suite.
- HTTP API with long polling, push delivery to webhooks with HMAC signatures,
  and recurring jobs from cron expressions.
- API keys with client and admin roles, Prometheus metrics, an OpenAPI spec,
  and optional OpenTelemetry tracing.
- A Rust client with a `Worker`, and a `dally` CLI.
- An optional web UI.
