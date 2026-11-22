//! The worker against a real server.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use dally_client::{Client, Job, MessageId, Status, Worker};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use common::wait_for_status;

#[derive(Debug, Serialize, Deserialize)]
struct Task {
    fail: bool,
    panic: bool,
}

impl Task {
    const OK: Task = Task {
        fail: false,
        panic: false,
    };
}

/// Starts a worker in the background. Dropping or sending on the returned
/// sender stops it; the handle resolves when it has shut down.
fn start<T, F, Fut>(
    worker: Worker,
    handler: F,
) -> (oneshot::Sender<()>, JoinHandle<dally_client::Result<()>>)
where
    T: serde::de::DeserializeOwned + Send + 'static,
    F: Fn(Job<T>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), String>> + Send + 'static,
{
    let (stop, stopped) = oneshot::channel::<()>();
    let handle = tokio::spawn(worker.run_until(
        async {
            let _ = stopped.await;
        },
        handler,
    ));
    (stop, handle)
}

async fn schedule(client: &Client, queue: &str, task: &Task) -> MessageId {
    client
        .schedule(queue, task)
        .max_attempts(1)
        .send()
        .await
        .expect("schedule")
        .message
        .id
}

async fn handle(job: Job<Task>) -> Result<(), String> {
    if job.payload.panic {
        panic!("boom");
    }
    if job.payload.fail {
        return Err(format!(
            "attempt {} of {} failed",
            job.attempt(),
            job.max_attempts()
        ));
    }
    Ok(())
}

#[tokio::test]
async fn acks_and_nacks() {
    let client = serve_or_skip!();
    let ok = schedule(&client, "tasks", &Task::OK).await;
    let failing = schedule(
        &client,
        "tasks",
        &Task {
            fail: true,
            panic: false,
        },
    )
    .await;
    let bad_payload = client
        .schedule("tasks", &"not a task")
        .max_attempts(1)
        .send()
        .await
        .expect("schedule")
        .message
        .id;

    let worker = Worker::new(client.clone(), "tasks")
        .concurrency(2)
        .poll_wait(Duration::from_secs(1));
    let (stop, handle) = start(worker, handle);

    wait_for_status(&client, ok, Status::Succeeded).await;
    let failed = wait_for_status(&client, failing, Status::Dead).await;
    assert_eq!(failed.last_error.as_deref(), Some("attempt 1 of 1 failed"));
    let invalid = wait_for_status(&client, bad_payload, Status::Dead).await;
    let error = invalid.last_error.unwrap_or_default();
    assert!(error.starts_with("invalid payload: "), "{error}");

    let _ = stop.send(());
    handle.await.expect("join").expect("worker");
}

#[tokio::test]
async fn survives_a_panicking_handler() {
    let client = serve_or_skip!();
    let panicking = schedule(
        &client,
        "tasks",
        &Task {
            fail: false,
            panic: true,
        },
    )
    .await;

    let worker = Worker::new(client.clone(), "tasks").poll_wait(Duration::from_secs(1));
    let (stop, handle) = start(worker, handle);

    let dead = wait_for_status(&client, panicking, Status::Dead).await;
    assert_eq!(dead.last_error.as_deref(), Some("handler panicked: boom"));

    let after = schedule(&client, "tasks", &Task::OK).await;
    wait_for_status(&client, after, Status::Succeeded).await;

    let _ = stop.send(());
    handle.await.expect("join").expect("worker");
}

#[tokio::test]
async fn respects_concurrency() {
    let client = serve_or_skip!();
    for _ in 0..12 {
        schedule(&client, "tasks", &Task::OK).await;
    }

    let running = Arc::new(AtomicUsize::new(0));
    let most = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicUsize::new(0));
    let (tx, mut finished) = mpsc::unbounded_channel();

    let worker = Worker::new(client.clone(), "tasks")
        .concurrency(4)
        .poll_wait(Duration::from_secs(1));
    let handler = {
        let (running, most, done) = (running.clone(), most.clone(), done.clone());
        move |_: Job<Task>| {
            let (running, most, done, tx) =
                (running.clone(), most.clone(), done.clone(), tx.clone());
            async move {
                let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                most.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(100)).await;
                running.fetch_sub(1, Ordering::SeqCst);
                if done.fetch_add(1, Ordering::SeqCst) + 1 == 12 {
                    let _ = tx.send(());
                }
                Ok::<_, String>(())
            }
        }
    };
    let (stop, handle) = start(worker, handler);

    tokio::time::timeout(Duration::from_secs(10), finished.recv())
        .await
        .expect("all jobs ran in time");
    assert_eq!(most.load(Ordering::SeqCst), 4);

    let _ = stop.send(());
    handle.await.expect("join").expect("worker");
}

#[tokio::test]
async fn shutdown_waits_for_running_jobs() {
    let client = serve_or_skip!();
    let mut ids = Vec::new();
    for _ in 0..3 {
        ids.push(schedule(&client, "tasks", &Task::OK).await);
    }

    let (started_tx, mut started) = mpsc::unbounded_channel();
    let finished = Arc::new(AtomicUsize::new(0));
    let handler = {
        let finished = finished.clone();
        move |_: Job<Task>| {
            let (started_tx, finished) = (started_tx.clone(), finished.clone());
            async move {
                let _ = started_tx.send(());
                tokio::time::sleep(Duration::from_millis(500)).await;
                finished.fetch_add(1, Ordering::SeqCst);
                Ok::<_, String>(())
            }
        }
    };
    let worker = Worker::new(client.clone(), "tasks")
        .concurrency(3)
        .poll_wait(Duration::from_secs(1));
    let (stop, handle) = start(worker, handler);

    for _ in 0..3 {
        tokio::time::timeout(Duration::from_secs(10), started.recv())
            .await
            .expect("jobs start");
    }
    let _ = stop.send(());
    handle.await.expect("join").expect("worker");

    assert_eq!(finished.load(Ordering::SeqCst), 3);
    for id in ids {
        let message = client.get_message(id).await.expect("get");
        assert_eq!(message.status, Status::Succeeded, "acked before returning");
    }
}

#[tokio::test]
async fn returns_errors_that_will_not_fix_themselves() {
    let client = serve_or_skip!();
    let result = Worker::new(client, "bad name")
        .run(|_: Job<Task>| async { Ok::<_, String>(()) })
        .await;
    let err = result.expect_err("an invalid queue name stops the worker");
    assert!(err.is_invalid_input(), "{err:?}");
}

#[tokio::test]
async fn keeps_retrying_while_the_server_is_down() {
    let client = Client::new("http://127.0.0.1:9").expect("client");
    // The first retry delay is a second, so stopping after half of one also
    // checks that shutdown interrupts the delay.
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        Worker::new(client, "tasks").run_until(
            tokio::time::sleep(Duration::from_millis(500)),
            |_: Job<Task>| async { Ok::<_, String>(()) },
        ),
    )
    .await
    .expect("shutdown interrupts the retry delay");
    result.expect("retryable errors do not stop the worker");
}
