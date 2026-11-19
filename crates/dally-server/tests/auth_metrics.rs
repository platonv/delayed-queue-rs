//! API keys and Prometheus metrics, against SQLite.

use std::sync::Arc;

use dally_core::storage::sqlite::SqliteStorage;
use dally_core::{ConnectOptions, QueueSettings, Service};
use dally_server::auth::{AuthConfig, KeyConfig, Role};
use dally_server::{Config, Server};
use reqwest::StatusCode;
use serde_json::json;

const CLIENT_KEY: &str = "client-key-0123456789";
const ADMIN_KEY: &str = "admin-key-0123456789";

/// Serves a server built from `config` on a random port and returns its URL.
async fn serve(config: Config) -> String {
    let dir = tempfile::Builder::new()
        .prefix("dally-auth-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("temp dir")
        .keep();
    let storage = SqliteStorage::open(dir.join("dally.sqlite"), &ConnectOptions::default())
        .await
        .expect("open storage");
    let service = Service::new(Arc::new(storage), QueueSettings::default());
    let router = Server::new(service, config).expect("server").router();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    tokio::spawn(async move { axum::serve(listener, router).await.expect("serve") });
    format!("http://{address}")
}

fn with_keys(protect_metrics: bool) -> Config {
    let key = |name: &str, role, secret: &str| KeyConfig {
        name: name.to_owned(),
        role,
        key: Some(secret.to_owned()),
        key_env: None,
    };
    Config {
        auth: AuthConfig {
            keys: vec![
                key("worker", Role::Client, CLIENT_KEY),
                key("ops", Role::Admin, ADMIN_KEY),
            ],
            protect_metrics,
        },
        ..Config::default()
    }
}

async fn status(request: reqwest::RequestBuilder) -> StatusCode {
    request.send().await.expect("request").status()
}

#[tokio::test]
async fn without_keys_everything_is_open() {
    let base = serve(Config::default()).await;
    let http = reqwest::Client::new();

    let put = http.put(format!("{base}/v1/queues/emails")).json(&json!({}));
    assert_eq!(status(put).await, StatusCode::OK);
    assert_eq!(status(http.get(format!("{base}/metrics"))).await, StatusCode::OK);
}

#[tokio::test]
async fn keys_and_roles() {
    let base = serve(with_keys(false)).await;
    let http = reqwest::Client::new();
    let poll = || http.post(format!("{base}/v1/queues/emails/poll"));
    let put = || http.put(format!("{base}/v1/queues/emails")).json(&json!({}));

    let response = poll().send().await.expect("request");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers().get("www-authenticate").and_then(|v| v.to_str().ok()),
        Some("Bearer")
    );
    let body: serde_json::Value = response.json().await.expect("json");
    assert_eq!(body["error"]["code"], "unauthorized");

    assert_eq!(status(poll().bearer_auth("not-a-key")).await, StatusCode::UNAUTHORIZED);
    assert_eq!(status(poll().bearer_auth(CLIENT_KEY)).await, StatusCode::OK);
    assert_eq!(status(put().bearer_auth(CLIENT_KEY)).await, StatusCode::FORBIDDEN);
    assert_eq!(status(put().bearer_auth(ADMIN_KEY)).await, StatusCode::OK);
    assert_eq!(status(poll().bearer_auth(ADMIN_KEY)).await, StatusCode::OK);

    // Health checks and metrics stay open for probes and scrapers.
    assert_eq!(status(http.get(format!("{base}/healthz"))).await, StatusCode::OK);
    assert_eq!(status(http.get(format!("{base}/metrics"))).await, StatusCode::OK);
}

#[tokio::test]
async fn metrics_can_require_a_key() {
    let base = serve(with_keys(true)).await;
    let http = reqwest::Client::new();
    let metrics = || http.get(format!("{base}/metrics"));

    assert_eq!(status(metrics()).await, StatusCode::UNAUTHORIZED);
    assert_eq!(status(metrics().bearer_auth(CLIENT_KEY)).await, StatusCode::OK);
}

#[tokio::test]
async fn metrics_count_messages_and_requests() {
    let base = serve(Config::default()).await;
    let http = reqwest::Client::new();

    for i in 0..3 {
        let request = http
            .post(format!("{base}/v1/queues/emails/messages"))
            .json(&json!({"payload": i, "max_attempts": 1}));
        assert_eq!(status(request).await, StatusCode::CREATED);
    }
    let polled: serde_json::Value = http
        .post(format!("{base}/v1/queues/emails/poll"))
        .json(&json!({"limit": 2}))
        .send()
        .await
        .expect("poll")
        .json()
        .await
        .expect("json");
    let [first, second] = [&polled["messages"][0], &polled["messages"][1]];
    let ack = http
        .post(format!("{base}/v1/messages/{}/ack", first["id"].as_str().expect("id")))
        .json(&json!({"lease": first["lease"]}));
    assert_eq!(status(ack).await, StatusCode::OK);
    let nack = http
        .post(format!("{base}/v1/messages/{}/nack", second["id"].as_str().expect("id")))
        .json(&json!({"lease": second["lease"], "error": "boom"}));
    assert_eq!(status(nack).await, StatusCode::OK);

    let text = http
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics")
        .text()
        .await
        .expect("text");
    let has = |line: &str| text.lines().any(|l| l == line);

    assert!(has(r#"dally_messages_scheduled_total{queue="emails"} 3"#), "{text}");
    assert!(has(r#"dally_messages_leased_total{queue="emails"} 2"#), "{text}");
    assert!(has(r#"dally_messages_acked_total{queue="emails"} 1"#), "{text}");
    assert!(has(r#"dally_messages_nacked_total{queue="emails"} 1"#), "{text}");
    assert!(has(r#"dally_messages_dead_total{queue="emails"} 1"#), "{text}");
    assert!(has(r#"dally_queue_messages{queue="emails",status="scheduled"} 1"#), "{text}");
    assert!(has(r#"dally_queue_messages{queue="emails",status="succeeded"} 1"#), "{text}");
    assert!(has(r#"dally_queue_messages{queue="emails",status="dead"} 1"#), "{text}");
    assert!(text.contains(r#"dally_queue_oldest_due_seconds{queue="emails"}"#), "{text}");

    // Routes are labelled by template, never by the raw path.
    assert!(
        text.contains(r#"route="/v1/messages/{id}/ack""#),
        "expected a templated route label: {text}"
    );
    assert!(!text.contains("/v1/messages/0"), "raw ids leaked into labels: {text}");
}
