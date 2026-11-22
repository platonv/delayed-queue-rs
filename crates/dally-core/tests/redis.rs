//! Runs the conformance suite against Redis.
//!
//! Set `DALLY_TEST_REDIS_URL`, for example:
//!
//! ```sh
//! docker compose up -d redis
//! export DALLY_TEST_REDIS_URL=redis://localhost:56379
//! ```
//!
//! Each test uses its own key prefix, so tests run in parallel without seeing
//! each other's queues.

#![cfg(feature = "redis")]

mod conformance;

use std::sync::Arc;

use dally_core::storage::redis::RedisStorage;
use dally_core::{ConnectOptions, Storage};

async fn setup() -> Option<Arc<dyn Storage>> {
    let url = std::env::var("DALLY_TEST_REDIS_URL").ok()?;
    let separator = if url.contains('?') { '&' } else { '?' };
    let prefix = format!("dally-test-{}:", uuid::Uuid::new_v4().simple());
    let url = format!("{url}{separator}prefix={prefix}");

    let storage = RedisStorage::connect(&url, &ConnectOptions::default())
        .await
        .expect("connect storage");
    Some(Arc::new(storage))
}

conformance_tests!(setup);
