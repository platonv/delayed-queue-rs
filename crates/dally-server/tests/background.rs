//! Tests for push delivery and cron, against SQLite so they need no external
//! services.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use dally_core::storage::sqlite::SqliteStorage;
use dally_core::{
    Backoff, ConnectOptions, MessageFilter, PushSettings, QueueName, QueueSettings,
    ScheduleRequest, Service, Status,
};
use dally_server::config::{CronJob, PushConfig, QueueConfig};
use dally_server::{CancellationToken, Config, Server, signature};
use serde_json::json;
use tokio::sync::mpsc;

const SECRET: &str = "test-secret";

async fn service() -> Service {
    let dir = tempfile::Builder::new()
        .prefix("dally-server-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("temp dir")
        .keep();
    let storage = SqliteStorage::open(dir.join("dally.sqlite"), &ConnectOptions::default())
        .await
        .expect("open storage");
    Service::new(Arc::new(storage), QueueSettings::default())
}

/// A config whose background tasks react within a fraction of a second.
fn fast_config() -> Config {
    Config {
        push: PushConfig {
            refresh_interval: Duration::from_millis(100),
            poll_wait: Duration::from_millis(200),
            ..PushConfig::default()
        },
        ..Config::default()
    }
}

fn push_queue(url: String, concurrency: u32, max_attempts: u32) -> QueueSettings {
    QueueSettings {
        max_attempts,
        backoff: Backoff {
            base: Duration::from_millis(50),
            cap: Duration::from_millis(50),
        },
        push: Some(PushSettings {
            url,
            secret: Some(SECRET.to_owned()),
            timeout: Duration::from_secs(5),
            concurrency,
        }),
        ..QueueSettings::default()
    }
}

/// Serves `app` on a random port and returns its URL.
async fn serve(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("address");
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    format!("http://{address}/jobs")
}

/// Waits until `check` holds, or fails after five seconds.
async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !check().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn status_of(service: &Service, id: dally_core::MessageId) -> Status {
    service.get_message(id).await.expect("get message").status
}

#[tokio::test]
async fn success_acks_with_payload_headers_and_signature() {
    let (sender, mut received) = mpsc::unbounded_channel::<(HeaderMap, Bytes)>();
    let app = Router::new()
        .route(
            "/jobs",
            post(
                |State(sender): State<mpsc::UnboundedSender<_>>,
                 headers: HeaderMap,
                 body: Bytes| async move {
                    let _ = sender.send((headers, body));
                    StatusCode::NO_CONTENT
                },
            ),
        )
        .with_state(sender);
    let url = serve(app).await;

    let service = service().await;
    let emails = QueueName::new("emails").expect("valid");
    service
        .put_queue(&emails, push_queue(url, 4, 5))
        .await
        .expect("put queue");
    let scheduled = service
        .schedule(
            &emails,
            vec![ScheduleRequest::new(json!({"to": "ana@example.com"}))],
        )
        .await
        .expect("schedule");
    let id = scheduled[0].message.id;

    let stop = CancellationToken::new();
    let tasks = Server::new(service.clone(), fast_config())
        .expect("server")
        .start_background_tasks(stop.clone())
        .expect("start");

    let (headers, body) = tokio::time::timeout(Duration::from_secs(5), received.recv())
        .await
        .expect("delivery in time")
        .expect("a request");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).ok(),
        Some(json!({"to": "ana@example.com"}))
    );
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    };
    assert_eq!(header("content-type"), "application/json");
    assert_eq!(header("dally-message-id"), id.to_string());
    assert_eq!(header("dally-queue"), "emails");
    assert_eq!(header("dally-attempt"), "1");
    let now = chrono::Utc::now().timestamp();
    signature::verify(
        SECRET.as_bytes(),
        &header(signature::HEADER),
        &body,
        now,
        Duration::from_secs(60),
    )
    .expect("valid signature");

    eventually("the ack", || async {
        status_of(&service, id).await == Status::Succeeded
    })
    .await;
    stop.cancel();
    tasks.await.expect("background tasks");
}

#[tokio::test]
async fn failures_are_retried_until_dead() {
    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route(
            "/jobs",
            post(|State(calls): State<Arc<AtomicUsize>>| async move {
                calls.fetch_add(1, Ordering::SeqCst);
                (StatusCode::INTERNAL_SERVER_ERROR, "database is down")
            }),
        )
        .with_state(Arc::clone(&calls));
    let url = serve(app).await;

    let service = service().await;
    let emails = QueueName::new("emails").expect("valid");
    service
        .put_queue(&emails, push_queue(url, 1, 3))
        .await
        .expect("put queue");
    let scheduled = service
        .schedule(&emails, vec![ScheduleRequest::new(json!(1))])
        .await
        .expect("schedule");
    let id = scheduled[0].message.id;

    let stop = CancellationToken::new();
    let tasks = Server::new(service.clone(), fast_config())
        .expect("server")
        .start_background_tasks(stop.clone())
        .expect("start");

    eventually("the message to die", || async {
        status_of(&service, id).await == Status::Dead
    })
    .await;
    let message = service.get_message(id).await.expect("get");
    assert_eq!(message.attempts, 3);
    assert_eq!(
        message.last_error.as_deref(),
        Some("HTTP 500: database is down")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);

    stop.cancel();
    tasks.await.expect("background tasks");
}

#[tokio::test]
async fn concurrency_is_respected() {
    #[derive(Default)]
    struct Load {
        current: AtomicUsize,
        peak: AtomicUsize,
    }
    let load = Arc::new(Load::default());
    let app = Router::new()
        .route(
            "/jobs",
            post(|State(load): State<Arc<Load>>| async move {
                let now = load.current.fetch_add(1, Ordering::SeqCst) + 1;
                load.peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(200)).await;
                load.current.fetch_sub(1, Ordering::SeqCst);
                StatusCode::OK
            }),
        )
        .with_state(Arc::clone(&load));
    let url = serve(app).await;

    let service = service().await;
    let emails = QueueName::new("emails").expect("valid");
    service
        .put_queue(&emails, push_queue(url, 2, 5))
        .await
        .expect("put queue");
    let requests = (0..6).map(|i| ScheduleRequest::new(json!(i))).collect();
    service.schedule(&emails, requests).await.expect("schedule");

    let stop = CancellationToken::new();
    let tasks = Server::new(service.clone(), fast_config())
        .expect("server")
        .start_background_tasks(stop.clone())
        .expect("start");

    eventually("every message to be acked", || async {
        service
            .get_queue(&emails)
            .await
            .expect("queue")
            .stats
            .succeeded
            == 6
    })
    .await;
    assert_eq!(load.peak.load(Ordering::SeqCst), 2);

    stop.cancel();
    tasks.await.expect("background tasks");
}

#[tokio::test]
async fn two_cron_tickers_schedule_each_occurrence_once() {
    let service = service().await;
    let mut config = fast_config();
    config.queues.insert(
        "reports".to_owned(),
        QueueConfig {
            settings: QueueSettings::default(),
            cron: vec![CronJob {
                name: "every-second".to_owned(),
                schedule: "* * * * * *".to_owned(),
                timezone: None,
                payload: json!({"kind": "tick"}),
            }],
        },
    );
    let server = Server::new(service.clone(), config).expect("server");
    server.apply_queues().await.expect("apply queues");

    let stop = CancellationToken::new();
    let first = server
        .start_background_tasks(stop.clone())
        .expect("start first");
    let second = server
        .start_background_tasks(stop.clone())
        .expect("start second");
    tokio::time::sleep(Duration::from_millis(2500)).await;
    stop.cancel();
    first.await.expect("first");
    second.await.expect("second");

    let messages = service
        .list_messages(MessageFilter {
            queue: QueueName::new("reports").ok(),
            ..MessageFilter::default()
        })
        .await
        .expect("list")
        .items;
    let mut keys: Vec<_> = messages
        .iter()
        .filter_map(|m| m.idempotency_key.clone())
        .collect();
    assert!(
        keys.len() >= 2,
        "expected at least two occurrences, got {keys:?}"
    );
    assert_eq!(keys.len(), messages.len(), "every cron message has a key");
    keys.sort();
    keys.dedup();
    assert_eq!(
        keys.len(),
        messages.len(),
        "no occurrence was scheduled twice"
    );
    assert!(keys.iter().all(|key| key.starts_with("cron:every-second:")));
}
