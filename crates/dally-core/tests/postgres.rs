//! Runs the conformance suite against PostgreSQL.
//!
//! Set `DALLY_TEST_POSTGRES_URL` to a database the tests may create schemas
//! in, for example:
//!
//! ```sh
//! docker run -d --name dally-test-postgres -p 55432:5432 \
//!     -e POSTGRES_PASSWORD=dally postgres:17
//! export DALLY_TEST_POSTGRES_URL=postgres://postgres:dally@localhost:55432/postgres
//! ```
//!
//! Each test runs in its own schema, so tests run in parallel without seeing
//! each other's queues.

#![cfg(feature = "postgres")]

mod conformance;

use std::sync::Arc;

use dally_core::storage::postgres::PostgresStorage;
use dally_core::{ConnectOptions, Storage};
use sqlx::Connection;
use sqlx::postgres::{PgConnectOptions, PgConnection};

async fn setup() -> Option<Arc<dyn Storage>> {
    let url = std::env::var("DALLY_TEST_POSTGRES_URL").ok()?;
    let options: PgConnectOptions = url.parse().expect("valid DALLY_TEST_POSTGRES_URL");

    let schema = format!("dally_test_{}", uuid::Uuid::new_v4().simple());
    let mut admin = PgConnection::connect_with(&options).await.expect("connect");
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&mut admin)
        .await
        .expect("create schema");
    admin.close().await.expect("close");

    let options = options.options([("search_path", schema.as_str())]);
    let storage = PostgresStorage::connect_with(options, &ConnectOptions::default())
        .await
        .expect("connect storage");
    Some(Arc::new(storage))
}

conformance_tests!(setup);
