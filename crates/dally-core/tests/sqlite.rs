//! Runs the conformance suite against SQLite, plus the checks that only make
//! sense for a file-based backend.
//!
//! Every test gets a fresh database file in its own directory under Cargo's
//! per-target scratch directory (`target/tmp`). The directories are kept
//! rather than deleted, because the storage outlives the setup function that
//! creates them; `cargo clean` removes them.

#![cfg(feature = "sqlite")]

mod conformance;

use std::path::PathBuf;
use std::sync::Arc;

use dally_core::storage::sqlite::SqliteStorage;
use dally_core::{ConnectOptions, Error, QueueSettings, ScheduleRequest, Service, Storage};
use serde_json::json;

fn database_path() -> PathBuf {
    let dir = tempfile::Builder::new()
        .prefix("dally-sqlite-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("create temp dir")
        .keep();
    dir.join("dally.sqlite")
}

async fn setup() -> Option<Arc<dyn Storage>> {
    let storage = SqliteStorage::open(database_path(), &ConnectOptions::default())
        .await
        .expect("open storage");
    Some(Arc::new(storage))
}

conformance_tests!(setup);

#[tokio::test]
async fn second_storage_on_the_same_file_is_refused() {
    let path = database_path();
    let first = SqliteStorage::open(&path, &ConnectOptions::default())
        .await
        .expect("first open");

    let err = SqliteStorage::open(&path, &ConnectOptions::default())
        .await
        .err()
        .expect("second open should fail");
    assert!(matches!(err, Error::InvalidInput(_)), "got {err:?}");
    assert!(
        err.to_string().contains("another dally server"),
        "got {err}"
    );

    drop(first);
    SqliteStorage::open(&path, &ConnectOptions::default())
        .await
        .expect("open after the first one is gone");
}

#[tokio::test]
async fn messages_survive_reopening() {
    let path = database_path();
    let service =
        |storage: SqliteStorage| Service::new(Arc::new(storage), QueueSettings::default());
    let emails = dally_core::QueueName::new("emails").expect("valid name");

    let first = service(
        SqliteStorage::open(&path, &ConnectOptions::default())
            .await
            .expect("open"),
    );
    let scheduled = first
        .schedule(&emails, vec![ScheduleRequest::new(json!({"to": "ana"}))])
        .await
        .expect("schedule");
    drop(first);

    let second = service(
        SqliteStorage::open(&path, &ConnectOptions::default())
            .await
            .expect("reopen"),
    );
    let stored = second
        .get_message(scheduled[0].message.id)
        .await
        .expect("get");
    assert_eq!(stored, scheduled[0].message);
}

#[tokio::test]
async fn connect_accepts_file_and_memory_urls() {
    let path = database_path();
    let url = format!("sqlite://{}", path.display());
    assert!(path.is_absolute(), "the URL above is sqlite:///...");

    for url in [url.as_str(), "sqlite::memory:"] {
        let storage = dally_core::connect(url, &ConnectOptions::default())
            .await
            .unwrap_or_else(|err| panic!("connect to {url}: {err}"));
        let service = Service::new(storage, QueueSettings::default());
        let emails = dally_core::QueueName::new("emails").expect("valid name");
        service
            .schedule(&emails, vec![ScheduleRequest::new(json!(1))])
            .await
            .expect("schedule");
        assert_eq!(
            service
                .get_queue(&emails)
                .await
                .expect("queue")
                .stats
                .scheduled,
            1,
            "{url}"
        );
    }
    assert!(path.exists(), "the database file was created");
}
