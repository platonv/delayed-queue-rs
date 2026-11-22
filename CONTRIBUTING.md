# Contributing to dally

Thanks for helping. Bug reports, docs fixes and code are all welcome.

## Before you start

For anything bigger than a small fix, open an issue first and describe what
you want to change. `docs/design.md` explains how dally is put together and
why. A change that goes against it is not ruled out, but it needs a discussion.

## Layout

| Path                  | What it is                                              |
|-----------------------|---------------------------------------------------------|
| `crates/dally-core`   | Domain types, the `Service` rules, the `Storage` trait and backends |
| `crates/dally-server` | HTTP API and background tasks                           |
| `crates/dally-client` | Rust client and `Worker`                                |
| `crates/dally-ui`     | Web UI, a client of the HTTP API                        |
| `crates/dally`        | The `dally` binary                                      |
| `docs/`               | Design notes and user documentation                     |

## Running the tests

The SQLite tests and unit tests need nothing. The PostgreSQL and Redis tests
need running databases and are skipped without them:

```sh
docker compose up -d
export DALLY_TEST_POSTGRES_URL=postgres://postgres:dally@localhost:55432/postgres
export DALLY_TEST_REDIS_URL=redis://localhost:56379
cargo test --workspace --all-features
```

Before sending a pull request, run what CI runs:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

After changing the HTTP API, regenerate the committed OpenAPI spec:

```sh
UPDATE_OPENAPI=1 cargo test -p dally-server openapi
```

## Adding a storage backend

Implement `dally_core::Storage` behind a cargo feature, then add a test file
that calls `conformance_tests!(setup)` (see `crates/dally-core/tests/postgres.rs`).
The conformance suite is the contract: a backend that passes it behaves like
every other backend. If a test seems wrong for your backend, raise it in the
pull request rather than skipping it.

## Style

- Write code for the next reader. Small functions, clear names, and comments
  that explain *why* rather than *what*.
- Public items get doc comments. The workspace warns on missing docs.
- No `unwrap()` outside tests. Library crates use `thiserror`; only the binary
  uses `anyhow`.
- Behaviour changes come with tests, and with an update to `docs/` if users
  would notice.
- Commit messages: a short summary line in the imperative ("Add Redis
  backend"), then a body explaining why, if it is not obvious.

## License

By contributing, you agree that your contributions will be dual licensed under
the MIT and Apache 2.0 licenses, as described in the README.
