//! End-to-end tests of the HTTP API against PostgreSQL. They need
//! `DALLY_TEST_POSTGRES_URL`; see `dally-core/tests/postgres.rs`.

use std::sync::Arc;

use dally_core::storage::postgres::PostgresStorage;
use dally_core::{ConnectOptions, QueueSettings, Service};
use reqwest::StatusCode;
use serde_json::{Value, json};
use sqlx::Connection;
use sqlx::postgres::{PgConnectOptions, PgConnection};

/// Serves the API on a random port, backed by a fresh schema. Returns the
/// base URL, or `None` when no test database is configured.
async fn serve() -> Option<String> {
    let url = std::env::var("DALLY_TEST_POSTGRES_URL").ok()?;
    let options: PgConnectOptions = url.parse().expect("valid DALLY_TEST_POSTGRES_URL");

    let schema = format!("dally_test_{}", uuid::Uuid::new_v4().simple());
    let mut admin = PgConnection::connect_with(&options).await.expect("connect");
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&mut admin)
        .await
        .expect("create schema");

    let options = options.options([("search_path", schema.as_str())]);
    let storage = PostgresStorage::connect_with(options, &ConnectOptions::default())
        .await
        .expect("connect storage");
    let service = Service::new(Arc::new(storage), QueueSettings::default());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, dally_server::router(service))
            .await
            .expect("serve");
    });
    Some(format!("http://{address}"))
}

macro_rules! serve_or_skip {
    () => {
        match serve().await {
            Some(base) => (base, reqwest::Client::new()),
            None => {
                eprintln!("skipping: DALLY_TEST_POSTGRES_URL is not set");
                return;
            }
        }
    };
}

async fn send(request: reqwest::RequestBuilder) -> (StatusCode, Value) {
    let response = request.send().await.expect("request");
    let status = response.status();
    let body = response.json().await.unwrap_or(Value::Null);
    (status, body)
}

#[tokio::test]
async fn schedule_poll_ack() {
    let (base, client) = serve_or_skip!();

    let (status, message) = send(
        client
            .post(format!("{base}/v1/queues/emails/messages"))
            .json(&json!({"payload": {"to": "ana@example.com"}, "idempotency_key": "welcome-42"})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(message["status"], "scheduled");
    assert_eq!(message["visibility_timeout"], "30s");

    let (status, again) = send(
        client
            .post(format!("{base}/v1/queues/emails/messages"))
            .json(&json!({"payload": {}, "idempotency_key": "welcome-42"})),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a repeated key returns the existing message"
    );
    assert_eq!(again["id"], message["id"]);

    let (status, polled) = send(
        client
            .post(format!("{base}/v1/queues/emails/poll"))
            .json(&json!({"limit": 5, "wait": "1s"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let leased = &polled["messages"][0];
    assert_eq!(leased["id"], message["id"]);
    assert_eq!(leased["payload"]["to"], "ana@example.com");

    let id = leased["id"].as_str().expect("id");
    let (status, acked) = send(
        client
            .post(format!("{base}/v1/messages/{id}/ack"))
            .json(&json!({"lease": leased["lease"]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(acked["status"], "succeeded");

    let (status, queue) = send(client.get(format!("{base}/v1/queues/emails"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(queue["stats"]["succeeded"], 1);
}

#[tokio::test]
async fn batch_schedule_and_empty_poll_body() {
    let (base, client) = serve_or_skip!();

    let (status, body) = send(
        client
            .post(format!("{base}/v1/queues/reports/messages"))
            .json(&json!([{"payload": 1}, {"payload": 2, "delay": "1h"}])),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["messages"].as_array().map(Vec::len), Some(2));
    assert_eq!(body["messages"][0]["created"], true);

    let (status, polled) = send(client.post(format!("{base}/v1/queues/reports/poll"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(polled["messages"].as_array().map(Vec::len), Some(1));
}

#[tokio::test]
async fn errors_have_codes() {
    let (base, client) = serve_or_skip!();

    let (status, body) = send(
        client
            .post(format!("{base}/v1/queues/bad%20name/messages"))
            .json(&json!({"payload": 1})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "invalid_input");

    let (status, body) = send(
        client
            .post(format!("{base}/v1/queues/emails/messages"))
            .json(&json!({"payload": 1, "dely": "1m"})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let message = body["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("dely"),
        "message should name the bad field: {message}"
    );

    let missing = uuid::Uuid::now_v7();
    let (status, body) = send(client.get(format!("{base}/v1/messages/{missing}"))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "message_not_found");

    let (status, body) = send(client.get(format!("{base}/v1/queues/nope"))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "queue_not_found");

    let (_, scheduled) = send(
        client
            .post(format!("{base}/v1/queues/emails/messages"))
            .json(&json!({"payload": 1})),
    )
    .await;
    let id = scheduled["id"].as_str().expect("id");
    let (status, body) = send(
        client
            .post(format!("{base}/v1/messages/{id}/ack"))
            .json(&json!({"lease": "not-the-lease"})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], "lease_mismatch");
}

#[tokio::test]
async fn health_endpoints() {
    let (base, client) = serve_or_skip!();
    for path in ["healthz", "readyz"] {
        let response = client
            .get(format!("{base}/{path}"))
            .send()
            .await
            .expect("request");
        assert_eq!(response.status(), StatusCode::OK, "{path}");
    }
}
