//! The client against a real server.

mod common;

use std::time::Duration;

use dally_client::{
    Client, Error, MessageFilter, MessageId, NackRequest, QueueName, QueueSettings, RunAt,
    ScheduleRequest, Status,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Welcome {
    user_id: u64,
}

#[tokio::test]
async fn schedule_poll_ack() {
    let client = serve_or_skip!();

    let scheduled = client
        .schedule("emails", &Welcome { user_id: 42 })
        .idempotency_key("welcome-42")
        .priority(3)
        .send()
        .await
        .expect("schedule");
    assert!(scheduled.created);
    assert_eq!(scheduled.message.status, Status::Scheduled);
    assert_eq!(scheduled.message.priority, 3);
    assert_eq!(scheduled.message.payload, json!({"user_id": 42}));

    let again = client
        .schedule("emails", &Welcome { user_id: 0 })
        .idempotency_key("welcome-42")
        .send()
        .await
        .expect("schedule again");
    assert!(!again.created, "a repeated key returns the existing message");
    assert_eq!(again.message.id, scheduled.message.id);

    let polled = client
        .poll("emails", 10, Duration::from_secs(1))
        .await
        .expect("poll");
    assert_eq!(polled.len(), 1);
    assert_eq!(polled[0].message.id, scheduled.message.id);
    assert_eq!(polled[0].message.status, Status::Running);

    let acked = client
        .ack(polled[0].message.id, &polled[0].lease)
        .await
        .expect("ack");
    assert_eq!(acked.status, Status::Succeeded);
}

#[tokio::test]
async fn schedule_options_reach_the_server() {
    let client = serve_or_skip!();

    let delayed = client
        .schedule("emails", &json!({"to": "ana"}))
        .delay(Duration::from_secs(3600))
        .max_attempts(2)
        .visibility_timeout(Duration::from_secs(90))
        .send()
        .await
        .expect("schedule delayed");
    assert_eq!(delayed.message.max_attempts, 2);
    assert_eq!(delayed.message.visibility_timeout, Duration::from_secs(90));
    assert!(delayed.message.run_at > delayed.message.created_at + chrono::Duration::minutes(59));

    let at = chrono::Utc::now() + chrono::Duration::days(1);
    let timed = client
        .schedule("emails", &1)
        .delay(Duration::from_secs(5))
        .run_at(at)
        .send()
        .await
        .expect("run_at replaces delay");
    let drift = (timed.message.run_at - at).num_milliseconds().abs();
    assert!(drift < 1000, "run_at should be kept, drift {drift}ms");

    let nothing_due = client
        .poll("emails", 10, Duration::ZERO)
        .await
        .expect("poll");
    assert!(nothing_due.is_empty());
}

#[tokio::test]
async fn schedule_batch_keeps_order() {
    let client = serve_or_skip!();

    let requests = [
        ScheduleRequest::new(json!(1)),
        ScheduleRequest {
            idempotency_key: Some("two".into()),
            ..ScheduleRequest::new(json!(2))
        },
    ];
    let results = client
        .schedule_batch("reports", &requests)
        .await
        .expect("batch");
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|result| result.created));
    assert_eq!(results[0].message.payload, json!(1));
    assert_eq!(results[1].message.payload, json!(2));

    let again = client
        .schedule_batch("reports", &requests[1..])
        .await
        .expect("batch again");
    assert!(!again[0].created);
}

#[tokio::test]
async fn nack_retries_and_dies() {
    let client = serve_or_skip!();

    let id = client
        .schedule("emails", &1)
        .max_attempts(2)
        .send()
        .await
        .expect("schedule")
        .message
        .id;

    for expected in [Status::Scheduled, Status::Dead] {
        let leased = client
            .poll("emails", 1, Duration::from_secs(1))
            .await
            .expect("poll")
            .pop()
            .expect("a due message");
        let nacked = client
            .nack(
                id,
                &NackRequest {
                    lease: leased.lease,
                    error: Some("smtp is down".into()),
                    retry_in: Some(Duration::ZERO),
                },
            )
            .await
            .expect("nack");
        assert_eq!(nacked.status, expected);
        assert_eq!(nacked.last_error.as_deref(), Some("smtp is down"));
    }

    assert_eq!(
        client.retry_dead("emails", None).await.expect("retry all"),
        1
    );
    let message = client.get_message(id).await.expect("get");
    assert_eq!(message.status, Status::Scheduled);
}

#[tokio::test]
async fn retry_dead_by_id() {
    let client = serve_or_skip!();
    let mut dead = Vec::new();
    for _ in 0..2 {
        let id = client
            .schedule("jobs", &0)
            .max_attempts(1)
            .send()
            .await
            .expect("schedule")
            .message
            .id;
        let leased = client
            .poll("jobs", 1, Duration::from_secs(1))
            .await
            .expect("poll")
            .pop()
            .expect("a due message");
        client
            .nack(
                id,
                &NackRequest {
                    lease: leased.lease,
                    ..NackRequest::default()
                },
            )
            .await
            .expect("nack");
        dead.push(id);
    }

    let retried = client
        .retry_dead("jobs", Some(&dead[..1]))
        .await
        .expect("retry one");
    assert_eq!(retried, 1);
    let still_dead = client.get_message(dead[1]).await.expect("get");
    assert_eq!(still_dead.status, Status::Dead);
}

#[tokio::test]
async fn reschedule_and_cancel() {
    let client = serve_or_skip!();
    let id = client
        .schedule("emails", &1)
        .delay(Duration::from_secs(3600))
        .send()
        .await
        .expect("schedule")
        .message
        .id;

    let later = client
        .reschedule(id, RunAt::After(Duration::from_secs(7200)))
        .await
        .expect("reschedule after");
    assert!(later.run_at > later.created_at + chrono::Duration::minutes(119));

    let at = chrono::Utc::now() + chrono::Duration::days(2);
    let timed = client
        .reschedule(id, RunAt::At(at))
        .await
        .expect("reschedule at");
    assert!((timed.run_at - at).num_milliseconds().abs() < 1000);

    client
        .reschedule(id, RunAt::Now)
        .await
        .expect("reschedule now");
    let polled = client
        .poll("emails", 1, Duration::from_secs(1))
        .await
        .expect("poll");
    assert_eq!(polled.len(), 1, "a message rescheduled to now is due");

    let err = client
        .cancel(id)
        .await
        .expect_err("a running message cannot be cancelled");
    assert!(err.is_invalid_status(), "{err:?}");

    let other = client
        .schedule("emails", &2)
        .delay(Duration::from_secs(60))
        .send()
        .await
        .expect("schedule");
    let cancelled = client.cancel(other.message.id).await.expect("cancel");
    assert_eq!(cancelled.status, Status::Cancelled);
}

#[tokio::test]
async fn list_messages_filters_and_pages() {
    let client = serve_or_skip!();
    for n in 0..3 {
        client.schedule("a", &n).send().await.expect("schedule");
    }
    let in_b = client
        .schedule("b", &"b")
        .delay(Duration::from_secs(60))
        .send()
        .await
        .expect("schedule");
    client
        .cancel(in_b.message.id)
        .await
        .expect("cancel");

    let all = client
        .list_messages(&MessageFilter::default())
        .await
        .expect("list all");
    assert_eq!(all.items.len(), 4);

    let first = client
        .list_messages(&MessageFilter {
            queue: Some(QueueName::new("a").expect("valid name")),
            limit: 2,
            ..MessageFilter::default()
        })
        .await
        .expect("first page");
    assert_eq!(first.items.len(), 2);
    let next = first.next.expect("a second page");

    let second = client
        .list_messages(&MessageFilter {
            queue: Some(QueueName::new("a").expect("valid name")),
            before: Some(next),
            limit: 2,
            ..MessageFilter::default()
        })
        .await
        .expect("second page");
    assert_eq!(second.items.len(), 1);

    let cancelled = client
        .list_messages(&MessageFilter {
            status: Some(Status::Cancelled),
            ..MessageFilter::default()
        })
        .await
        .expect("list cancelled");
    assert_eq!(cancelled.items.len(), 1);
    assert_eq!(cancelled.items[0].id, in_b.message.id);
}

#[tokio::test]
async fn queues() {
    let client = serve_or_skip!();

    let settings = QueueSettings {
        max_attempts: 9,
        visibility_timeout: Duration::from_secs(120),
        ..QueueSettings::default()
    };
    let queue = client.put_queue("billing", &settings).await.expect("put");
    assert_eq!(queue.settings, settings);

    client.schedule("billing", &1).send().await.expect("schedule");

    let summary = client.get_queue("billing").await.expect("get");
    assert_eq!(summary.queue.settings.max_attempts, 9);
    assert_eq!(summary.stats.scheduled, 1);

    let paused = client.pause("billing").await.expect("pause");
    assert!(paused.settings.paused);
    let polled = client
        .poll("billing", 1, Duration::ZERO)
        .await
        .expect("poll");
    assert!(polled.is_empty(), "a paused queue delivers nothing");

    let resumed = client.resume("billing").await.expect("resume");
    assert!(!resumed.settings.paused);

    let queues = client.list_queues().await.expect("list");
    let names: Vec<&str> = queues
        .iter()
        .map(|summary| summary.queue.name.as_str())
        .collect();
    assert!(names.contains(&"billing"), "{names:?}");
}

#[tokio::test]
async fn health() {
    let client = serve_or_skip!();
    client.health().await.expect("healthz");
    client.ready().await.expect("readyz");
}

#[tokio::test]
async fn errors_carry_the_server_code() {
    let client = serve_or_skip!();

    let missing = MessageId::generate();
    let err = client
        .get_message(missing)
        .await
        .expect_err("no such message");
    assert!(err.is_not_found());
    assert_eq!(err.code(), Some("message_not_found"));
    assert_eq!(err.status(), Some(dally_client::reqwest::StatusCode::NOT_FOUND));
    assert!(!err.is_retryable());

    let err = client.get_queue("nope").await.expect_err("no such queue");
    assert!(err.is_not_found());
    assert_eq!(err.code(), Some("queue_not_found"));

    let scheduled = client.schedule("emails", &1).send().await.expect("schedule");
    let wrong = dally_client::LeaseToken::from("not-the-lease".to_owned());
    let err = client
        .ack(scheduled.message.id, &wrong)
        .await
        .expect_err("wrong lease");
    assert!(err.is_lease_mismatch(), "{err:?}");
    assert_eq!(err.status(), Some(dally_client::reqwest::StatusCode::CONFLICT));

    let err = client
        .schedule("bad name", &1)
        .send()
        .await
        .expect_err("invalid queue name");
    assert!(err.is_invalid_input(), "{err:?}");
    assert!(matches!(err, Error::Api { .. }));
}

#[tokio::test]
async fn transport_errors_are_retryable() {
    // Nothing listens on port 9 (discard) on a test machine.
    let client = Client::builder("http://127.0.0.1:9")
        .timeout(Duration::from_secs(2))
        .build()
        .expect("client");
    let err = client.health().await.expect_err("nothing listens there");
    assert!(matches!(err, Error::Transport(_)), "{err:?}");
    assert!(err.is_retryable());
}

#[tokio::test]
async fn sends_the_api_key() {
    use axum::http::HeaderMap;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("local addr");
    let app = axum::Router::new().route(
        "/healthz",
        axum::routing::get(|headers: HeaderMap| async move {
            let auth = headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_owned();
            if auth == "Bearer s3cret" {
                Ok("ok")
            } else {
                Err((
                    axum::http::StatusCode::UNAUTHORIZED,
                    axum::Json(json!({"error": {"code": "unauthorized", "message": auth}})),
                ))
            }
        }),
    );
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });

    let base = format!("http://{address}");
    let with_key = Client::builder(&base)
        .api_key("s3cret")
        .build()
        .expect("client");
    with_key.health().await.expect("the key is sent");

    let without = Client::new(&base).expect("client");
    let err = without.health().await.expect_err("no key");
    assert_eq!(err.code(), Some("unauthorized"));
}
