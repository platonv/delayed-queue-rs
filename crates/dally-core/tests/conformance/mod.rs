//! Behaviour every storage backend must have.
//!
//! Each test gets a fresh, empty storage and drives it through [`Service`],
//! so it checks both the backend and the rules on top of it. A backend's test
//! file only provides a setup function and calls [`conformance_tests!`]:
//!
//! ```ignore
//! mod conformance;
//!
//! async fn setup() -> Option<Arc<dyn Storage>> { ... }
//!
//! conformance_tests!(setup);
//! ```
//!
//! Setup returns `None` when the backend is not available (for example, no
//! database URL in the environment), and the tests are skipped with a note.

#![allow(dead_code)]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use dally_core::{
    Error, Leased, MessageFilter, NackRequest, PollRequest, PushSettings, QueueName, QueueSettings,
    RunAt, ScheduleRequest, Service, Status, Storage,
};
use serde_json::json;

/// Generates one `#[tokio::test]` per conformance check, each with its own
/// storage from `$setup`.
#[macro_export]
macro_rules! conformance_tests {
    ($setup:path) => {
        $crate::conformance_tests!(@each $setup;
            schedule_then_get,
            idempotency_key_returns_existing_message,
            idempotency_key_is_scoped_to_queue,
            duplicate_keys_within_one_batch,
            delayed_messages_are_not_due,
            lease_orders_by_priority_then_run_at,
            lease_respects_limit,
            concurrent_polls_never_share_a_message,
            ack_needs_the_current_lease,
            nack_retries_until_dead,
            nack_uses_backoff_by_default,
            expired_lease_is_handed_out_again,
            expired_lease_on_last_attempt_is_dead,
            cancel_only_scheduled_messages,
            reschedule_changes_run_at,
            retry_dead_gives_fresh_attempts,
            paused_queue_delivers_nothing,
            list_messages_pages_newest_first,
            queue_stats_count_each_status,
            put_queue_round_trips_settings,
            long_poll_wakes_on_schedule,
            long_poll_times_out_empty,
            prune_deletes_only_old_finished_messages,
            storage_clock_is_sane,
            oldest_due_measures_waiting_time,
        );
    };
    (@each $setup:path; $($name:ident),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                let Some(storage) = $setup().await else {
                    eprintln!("skipping {}: backend not configured", stringify!($name));
                    return;
                };
                conformance::$name(storage).await;
            }
        )*
    };
}

fn service(storage: Arc<dyn Storage>) -> Service {
    Service::new(storage, QueueSettings::default())
}

fn queue(name: &str) -> QueueName {
    QueueName::new(name).expect("valid queue name")
}

fn message(payload: serde_json::Value) -> ScheduleRequest {
    ScheduleRequest::new(payload)
}

fn keyed(payload: serde_json::Value, key: &str) -> ScheduleRequest {
    ScheduleRequest {
        idempotency_key: Some(key.to_owned()),
        ..ScheduleRequest::new(payload)
    }
}

async fn schedule_one(
    service: &Service,
    queue: &QueueName,
    request: ScheduleRequest,
) -> dally_core::Message {
    let mut scheduled = service
        .schedule(queue, vec![request])
        .await
        .expect("schedule");
    scheduled.remove(0).message
}

async fn poll(service: &Service, queue: &QueueName, limit: u32) -> Vec<Leased> {
    let request = PollRequest {
        limit,
        wait: Duration::ZERO,
    };
    service.poll(queue, request).await.expect("poll")
}

async fn poll_one(service: &Service, queue: &QueueName) -> Leased {
    let mut leased = poll(service, queue, 1).await;
    assert_eq!(leased.len(), 1, "expected one due message");
    leased.remove(0)
}

fn nack_now(leased: &Leased, error: &str) -> NackRequest {
    NackRequest {
        lease: leased.lease.clone(),
        error: Some(error.to_owned()),
        retry_in: Some(Duration::ZERO),
    }
}

/// Settings with the shortest visibility timeout the service allows, for tests
/// that wait for a lease to expire.
fn short_leases(max_attempts: u32) -> QueueSettings {
    QueueSettings {
        visibility_timeout: Duration::from_secs(1),
        max_attempts,
        ..QueueSettings::default()
    }
}

const LEASE_EXPIRY_WAIT: Duration = Duration::from_millis(1100);

pub async fn schedule_then_get(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let emails = queue("emails");

    let scheduled = service
        .schedule(&emails, vec![message(json!({"to": "ana"}))])
        .await
        .expect("schedule");
    assert_eq!(scheduled.len(), 1);
    assert!(scheduled[0].created);

    let stored = service
        .get_message(scheduled[0].message.id)
        .await
        .expect("get");
    assert_eq!(stored, scheduled[0].message);
    assert_eq!(stored.queue, emails);
    assert_eq!(stored.payload, json!({"to": "ana"}));
    assert_eq!(stored.status, Status::Scheduled);
    assert_eq!(stored.attempts, 0);
    assert_eq!(stored.max_attempts, QueueSettings::default().max_attempts);
    assert_eq!(
        stored.visibility_timeout,
        QueueSettings::default().visibility_timeout
    );

    let summary = service
        .get_queue(&emails)
        .await
        .expect("queue was created implicitly");
    assert_eq!(summary.queue.settings, QueueSettings::default());
}

pub async fn idempotency_key_returns_existing_message(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let emails = queue("emails");

    let first = service
        .schedule(&emails, vec![keyed(json!(1), "welcome-42")])
        .await
        .expect("first schedule");
    let second = service
        .schedule(&emails, vec![keyed(json!(2), "welcome-42")])
        .await
        .expect("second schedule");

    assert!(first[0].created);
    assert!(!second[0].created);
    assert_eq!(second[0].message.id, first[0].message.id);
    assert_eq!(
        second[0].message.payload,
        json!(1),
        "the original payload is kept"
    );
}

pub async fn idempotency_key_is_scoped_to_queue(storage: Arc<dyn Storage>) {
    let service = service(storage);

    let a = service
        .schedule(&queue("a"), vec![keyed(json!(1), "same-key")])
        .await
        .expect("schedule in a");
    let b = service
        .schedule(&queue("b"), vec![keyed(json!(1), "same-key")])
        .await
        .expect("schedule in b");

    assert!(a[0].created);
    assert!(b[0].created);
    assert_ne!(a[0].message.id, b[0].message.id);
}

pub async fn duplicate_keys_within_one_batch(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let emails = queue("emails");

    let results = service
        .schedule(
            &emails,
            vec![
                keyed(json!(1), "k"),
                message(json!(2)),
                keyed(json!(3), "k"),
            ],
        )
        .await
        .expect("schedule");

    assert_eq!(results.len(), 3);
    assert!(results[0].created);
    assert!(results[1].created);
    assert!(!results[2].created);
    assert_eq!(results[2].message.id, results[0].message.id);
    assert_eq!(
        results[1].message.payload,
        json!(2),
        "results keep input order"
    );
}

pub async fn delayed_messages_are_not_due(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let emails = queue("emails");

    let later = ScheduleRequest {
        delay: Some(Duration::from_secs(3600)),
        ..message(json!("later"))
    };
    let future = ScheduleRequest {
        run_at: Some(Utc::now() + chrono::Duration::hours(1)),
        ..message(json!("future"))
    };
    service
        .schedule(&emails, vec![later, future])
        .await
        .expect("schedule");
    let now = schedule_one(&service, &emails, message(json!("now"))).await;

    let leased = poll(&service, &emails, 10).await;
    assert_eq!(leased.len(), 1);
    assert_eq!(leased[0].message.id, now.id);
}

pub async fn lease_orders_by_priority_then_run_at(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let emails = queue("emails");
    let at = |minutes_ago: i64| Some(Utc::now() - chrono::Duration::minutes(minutes_ago));

    let requests = vec![
        ScheduleRequest {
            run_at: at(1),
            ..message(json!("low, newer"))
        },
        ScheduleRequest {
            run_at: at(2),
            ..message(json!("low, older"))
        },
        ScheduleRequest {
            run_at: at(1),
            priority: 10,
            ..message(json!("high"))
        },
    ];
    service.schedule(&emails, requests).await.expect("schedule");

    let order: Vec<_> = poll(&service, &emails, 10)
        .await
        .into_iter()
        .map(|leased| leased.message.payload)
        .collect();
    assert_eq!(
        order,
        vec![json!("high"), json!("low, older"), json!("low, newer")]
    );
}

pub async fn lease_respects_limit(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let emails = queue("emails");
    let requests = (0..5).map(|i| message(json!(i))).collect();
    service.schedule(&emails, requests).await.expect("schedule");

    assert_eq!(poll(&service, &emails, 2).await.len(), 2);
    assert_eq!(poll(&service, &emails, 10).await.len(), 3);
    assert_eq!(poll(&service, &emails, 10).await.len(), 0);
}

pub async fn concurrent_polls_never_share_a_message(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let emails = queue("emails");
    let requests = (0..50).map(|i| message(json!(i))).collect();
    service.schedule(&emails, requests).await.expect("schedule");

    let pollers = (0..10).map(|_| {
        let service = service.clone();
        let emails = emails.clone();
        tokio::spawn(async move { poll(&service, &emails, 7).await })
    });

    let mut seen = HashSet::new();
    for poller in pollers {
        for leased in poller.await.expect("poller task") {
            assert!(seen.insert(leased.message.id), "message leased twice");
        }
    }
    assert_eq!(seen.len(), 50);
}

pub async fn ack_needs_the_current_lease(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let emails = queue("emails");
    schedule_one(&service, &emails, message(json!(1))).await;
    let leased = poll_one(&service, &emails).await;

    let wrong = dally_core::LeaseToken::generate();
    let err = service
        .ack(leased.message.id, &wrong)
        .await
        .expect_err("wrong lease");
    assert!(matches!(err, Error::LeaseMismatch(_)), "got {err:?}");

    let acked = service
        .ack(leased.message.id, &leased.lease)
        .await
        .expect("ack");
    assert_eq!(acked.status, Status::Succeeded);
    assert!(acked.finished_at.is_some());
    assert!(acked.lease_expires_at.is_none());

    let err = service
        .ack(leased.message.id, &leased.lease)
        .await
        .expect_err("acked twice");
    assert!(matches!(err, Error::LeaseMismatch(_)), "got {err:?}");

    let missing = dally_core::MessageId::generate();
    let err = service
        .ack(missing, &leased.lease)
        .await
        .expect_err("unknown message");
    assert!(matches!(err, Error::MessageNotFound(_)), "got {err:?}");
}

pub async fn nack_retries_until_dead(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let emails = queue("emails");
    let request = ScheduleRequest {
        max_attempts: Some(2),
        ..message(json!(1))
    };
    schedule_one(&service, &emails, request).await;

    let first = poll_one(&service, &emails).await;
    assert_eq!(first.message.attempts, 1);
    let retried = service
        .nack(first.message.id, nack_now(&first, "smtp timeout"))
        .await
        .expect("nack");
    assert_eq!(retried.status, Status::Scheduled);
    assert_eq!(retried.last_error.as_deref(), Some("smtp timeout"));
    assert!(retried.finished_at.is_none());

    let second = poll_one(&service, &emails).await;
    assert_eq!(second.message.attempts, 2);
    assert_ne!(second.lease, first.lease, "each lease gets a fresh token");
    let dead = service
        .nack(second.message.id, nack_now(&second, "smtp down"))
        .await
        .expect("nack");
    assert_eq!(dead.status, Status::Dead);
    assert_eq!(dead.last_error.as_deref(), Some("smtp down"));
    assert!(dead.finished_at.is_some());

    assert!(poll(&service, &emails, 10).await.is_empty());
}

pub async fn nack_uses_backoff_by_default(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let emails = queue("emails");
    schedule_one(&service, &emails, message(json!(1))).await;
    let leased = poll_one(&service, &emails).await;

    let request = NackRequest {
        lease: leased.lease.clone(),
        error: None,
        retry_in: None,
    };
    let retried = service
        .nack(leased.message.id, request)
        .await
        .expect("nack");

    // The default backoff is 5s for the first retry, give or take 20%.
    let delay = retried.run_at - retried.updated_at;
    assert!(delay >= chrono::Duration::seconds(3), "delay was {delay}");
    assert!(delay <= chrono::Duration::seconds(7), "delay was {delay}");
    assert!(poll(&service, &emails, 1).await.is_empty());
}

pub async fn expired_lease_is_handed_out_again(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let emails = queue("emails");
    service
        .put_queue(&emails, short_leases(3))
        .await
        .expect("put queue");
    schedule_one(&service, &emails, message(json!(1))).await;

    let first = poll_one(&service, &emails).await;
    assert!(poll(&service, &emails, 1).await.is_empty(), "still leased");

    tokio::time::sleep(LEASE_EXPIRY_WAIT).await;
    let second = poll_one(&service, &emails).await;
    assert_eq!(second.message.id, first.message.id);
    assert_eq!(second.message.attempts, 2);
    assert_eq!(second.message.last_error.as_deref(), Some("lease expired"));

    let err = service
        .ack(first.message.id, &first.lease)
        .await
        .expect_err("stale lease");
    assert!(matches!(err, Error::LeaseMismatch(_)), "got {err:?}");
    service
        .ack(second.message.id, &second.lease)
        .await
        .expect("current lease");
}

pub async fn expired_lease_on_last_attempt_is_dead(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let emails = queue("emails");
    service
        .put_queue(&emails, short_leases(1))
        .await
        .expect("put queue");
    let scheduled = schedule_one(&service, &emails, message(json!(1))).await;

    poll_one(&service, &emails).await;
    tokio::time::sleep(LEASE_EXPIRY_WAIT).await;
    assert!(poll(&service, &emails, 1).await.is_empty());

    let dead = service.get_message(scheduled.id).await.expect("get");
    assert_eq!(dead.status, Status::Dead);
    assert_eq!(dead.last_error.as_deref(), Some("lease expired"));
}

pub async fn cancel_only_scheduled_messages(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let emails = queue("emails");
    let waiting = schedule_one(
        &service,
        &emails,
        ScheduleRequest {
            delay: Some(Duration::from_secs(3600)),
            ..message(json!("waiting"))
        },
    )
    .await;
    schedule_one(&service, &emails, message(json!("running"))).await;
    let running = poll_one(&service, &emails).await;

    let cancelled = service.cancel(waiting.id).await.expect("cancel");
    assert_eq!(cancelled.status, Status::Cancelled);
    assert!(cancelled.finished_at.is_some());

    let err = service
        .cancel(running.message.id)
        .await
        .expect_err("running");
    assert!(
        matches!(
            err,
            Error::InvalidStatus {
                status: Status::Running,
                ..
            }
        ),
        "got {err:?}"
    );
}

pub async fn reschedule_changes_run_at(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let emails = queue("emails");
    let later = schedule_one(
        &service,
        &emails,
        ScheduleRequest {
            delay: Some(Duration::from_secs(3600)),
            ..message(json!(1))
        },
    )
    .await;
    assert!(poll(&service, &emails, 1).await.is_empty());

    service
        .reschedule(later.id, RunAt::Now)
        .await
        .expect("reschedule");
    let leased = poll_one(&service, &emails).await;
    assert_eq!(leased.message.id, later.id);

    let err = service
        .reschedule(later.id, RunAt::After(Duration::from_secs(60)))
        .await
        .expect_err("running");
    assert!(matches!(err, Error::InvalidStatus { .. }), "got {err:?}");
}

pub async fn retry_dead_gives_fresh_attempts(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let emails = queue("emails");
    let requests = (0..3)
        .map(|i| ScheduleRequest {
            max_attempts: Some(1),
            ..message(json!(i))
        })
        .collect();
    service.schedule(&emails, requests).await.expect("schedule");
    for leased in poll(&service, &emails, 3).await {
        service
            .nack(leased.message.id, nack_now(&leased, "boom"))
            .await
            .expect("nack");
    }
    assert_eq!(
        service.get_queue(&emails).await.expect("queue").stats.dead,
        3
    );

    let dead = service
        .list_messages(MessageFilter {
            queue: Some(emails.clone()),
            status: Some(Status::Dead),
            ..MessageFilter::default()
        })
        .await
        .expect("list");
    let one = [dead.items[0].id];
    assert_eq!(
        service
            .retry_dead(&emails, Some(&one))
            .await
            .expect("retry one"),
        1
    );
    assert_eq!(
        service.retry_dead(&emails, None).await.expect("retry rest"),
        2
    );

    let leased = poll(&service, &emails, 10).await;
    assert_eq!(leased.len(), 3);
    assert!(leased.iter().all(|l| l.message.attempts == 1));
}

pub async fn paused_queue_delivers_nothing(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let emails = queue("emails");
    schedule_one(&service, &emails, message(json!(1))).await;

    service.set_paused(&emails, true).await.expect("pause");
    schedule_one(&service, &emails, message(json!(2))).await;
    assert!(poll(&service, &emails, 10).await.is_empty());

    service.set_paused(&emails, false).await.expect("resume");
    assert_eq!(poll(&service, &emails, 10).await.len(), 2);
}

pub async fn list_messages_pages_newest_first(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let emails = queue("emails");
    for i in 0..5 {
        schedule_one(&service, &emails, message(json!(i))).await;
    }
    schedule_one(&service, &queue("other"), message(json!("other"))).await;

    let mut filter = MessageFilter {
        queue: Some(emails.clone()),
        limit: 2,
        ..MessageFilter::default()
    };
    let mut payloads = Vec::new();
    loop {
        let page = service.list_messages(filter.clone()).await.expect("list");
        assert!(page.items.len() <= 2);
        payloads.extend(page.items.into_iter().map(|m| m.payload));
        match page.next {
            Some(next) => filter.before = Some(next),
            None => break,
        }
    }
    assert_eq!(
        payloads,
        vec![json!(4), json!(3), json!(2), json!(1), json!(0)]
    );

    let all = service
        .list_messages(MessageFilter::default())
        .await
        .expect("list all");
    assert_eq!(all.items.len(), 6);
}

pub async fn queue_stats_count_each_status(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let emails = queue("emails");
    let requests = (0..4)
        .map(|i| ScheduleRequest {
            max_attempts: Some(1),
            ..message(json!(i))
        })
        .collect();
    let scheduled = service.schedule(&emails, requests).await.expect("schedule");
    service
        .cancel(scheduled[3].message.id)
        .await
        .expect("cancel");

    let leased = poll(&service, &emails, 3).await;
    service
        .ack(leased[0].message.id, &leased[0].lease)
        .await
        .expect("ack");
    service
        .nack(leased[1].message.id, nack_now(&leased[1], "boom"))
        .await
        .expect("nack");
    schedule_one(&service, &emails, message(json!("new"))).await;

    let stats = service.get_queue(&emails).await.expect("queue").stats;
    assert_eq!(stats.scheduled, 1);
    assert_eq!(stats.running, 1);
    assert_eq!(stats.succeeded, 1);
    assert_eq!(stats.dead, 1);
    assert_eq!(stats.cancelled, 1);

    schedule_one(&service, &queue("another"), message(json!(1))).await;
    let queues = service.list_queues().await.expect("list queues");
    let names: Vec<_> = queues.iter().map(|q| q.queue.name.as_str()).collect();
    assert_eq!(names, vec!["another", "emails"]);
    assert_eq!(queues[1].stats, stats);
}

pub async fn put_queue_round_trips_settings(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let webhooks = queue("webhooks");
    let settings = QueueSettings {
        visibility_timeout: Duration::from_secs(90),
        max_attempts: 12,
        backoff: dally_core::Backoff {
            base: Duration::from_millis(1500),
            cap: Duration::from_secs(600),
        },
        retention: Duration::from_secs(3600),
        paused: true,
        push: Some(PushSettings {
            url: "https://example.com/jobs".to_owned(),
            secret: Some("s3cret".to_owned()),
            timeout: Duration::from_secs(5),
            concurrency: 3,
        }),
    };

    let created = service
        .put_queue(&webhooks, settings.clone())
        .await
        .expect("put");
    assert_eq!(created.settings, settings);
    let fetched = service.get_queue(&webhooks).await.expect("get");
    assert_eq!(fetched.queue.settings, settings);

    let updated = QueueSettings {
        max_attempts: 1,
        push: None,
        ..settings
    };
    service
        .put_queue(&webhooks, updated.clone())
        .await
        .expect("update");
    assert_eq!(
        service
            .get_queue(&webhooks)
            .await
            .expect("get")
            .queue
            .settings,
        updated
    );

    let invalid = QueueSettings {
        max_attempts: 0,
        ..QueueSettings::default()
    };
    assert!(service.put_queue(&webhooks, invalid).await.is_err());
}

pub async fn long_poll_wakes_on_schedule(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let emails = queue("emails");
    // Create the queue first, so the poller and the producer race on nothing
    // but the message.
    assert!(poll(&service, &emails, 1).await.is_empty());

    let poller = {
        let service = service.clone();
        let emails = emails.clone();
        tokio::spawn(async move {
            let started = tokio::time::Instant::now();
            let request = PollRequest {
                limit: 1,
                wait: Duration::from_secs(10),
            };
            let leased = service.poll(&emails, request).await.expect("poll");
            (leased, started.elapsed())
        })
    };

    tokio::time::sleep(Duration::from_millis(200)).await;
    schedule_one(&service, &emails, message(json!("wake up"))).await;

    let (leased, elapsed) = poller.await.expect("poller task");
    assert_eq!(leased.len(), 1);
    // Without a notification the poll would only find it on its 1s recheck.
    assert!(elapsed < Duration::from_millis(900), "took {elapsed:?}");
}

pub async fn long_poll_times_out_empty(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let started = tokio::time::Instant::now();
    let request = PollRequest {
        limit: 1,
        wait: Duration::from_millis(300),
    };

    let leased = service.poll(&queue("empty"), request).await.expect("poll");

    assert!(leased.is_empty());
    assert!(started.elapsed() >= Duration::from_millis(300));
}

pub async fn prune_deletes_only_old_finished_messages(storage: Arc<dyn Storage>) {
    let service = service(storage);
    let short = queue("short");
    let long = queue("long");
    let no_retention = QueueSettings {
        retention: Duration::ZERO,
        ..QueueSettings::default()
    };
    service
        .put_queue(&short, no_retention)
        .await
        .expect("put short");

    for queue in [&short, &long] {
        schedule_one(&service, queue, message(json!("done"))).await;
        let leased = poll_one(&service, queue).await;
        service
            .ack(leased.message.id, &leased.lease)
            .await
            .expect("ack");
        schedule_one(&service, queue, message(json!("pending"))).await;
    }
    // Retention is measured from finished_at, which has to be strictly in
    // the past.
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert_eq!(service.prune().await.expect("prune"), 1);
    assert_eq!(
        service
            .get_queue(&short)
            .await
            .expect("short")
            .stats
            .succeeded,
        0
    );
    assert_eq!(
        service
            .get_queue(&short)
            .await
            .expect("short")
            .stats
            .scheduled,
        1
    );
    assert_eq!(
        service
            .get_queue(&long)
            .await
            .expect("long")
            .stats
            .succeeded,
        1
    );
}

pub async fn storage_clock_is_sane(storage: Arc<dyn Storage>) {
    let now = storage.now().await.expect("now");
    let skew = (now - Utc::now()).abs();
    assert!(
        skew < chrono::Duration::seconds(5),
        "storage clock is {skew} off"
    );
}

/// `oldest_due` is optional: `None` means the backend does not support it.
pub async fn oldest_due_measures_waiting_time(storage: Arc<dyn Storage>) {
    let service = service(Arc::clone(&storage));
    let emails = queue("emails");
    let later = queue("later");
    let a_minute_ago = Utc::now() - chrono::Duration::seconds(60);
    let overdue = ScheduleRequest {
        run_at: Some(a_minute_ago),
        ..message(json!(1))
    };
    let delayed = ScheduleRequest {
        delay: Some(Duration::from_secs(3600)),
        ..message(json!(3))
    };
    schedule_one(&service, &emails, overdue).await;
    schedule_one(&service, &emails, message(json!(2))).await;
    schedule_one(&service, &later, delayed).await;

    let Some(oldest) = storage.oldest_due().await.expect("oldest due") else {
        return;
    };
    let waited = oldest.get(&emails).copied().unwrap_or_default();
    assert!(
        waited >= Duration::from_secs(55) && waited < Duration::from_secs(70),
        "oldest due message in emails waited {waited:?}, expected about 60s"
    );
    assert!(
        !oldest.contains_key(&later),
        "nothing in `later` is due yet"
    );
}
