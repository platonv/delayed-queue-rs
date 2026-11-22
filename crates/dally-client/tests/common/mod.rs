//! A real server for the integration tests: the dally server router on a random
//! port, backed by PostgreSQL with a fresh schema per test. The tests need
//! `DALLY_TEST_POSTGRES_URL` and skip themselves without it.

// Each test binary compiles this module separately and uses only part of it.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use dally_client::{Client, Message, MessageId, Status};
use dally_core::storage::postgres::PostgresStorage;
use dally_core::{ConnectOptions, QueueSettings, Service};
use sqlx::Connection;
use sqlx::postgres::{PgConnectOptions, PgConnection};

/// Serves the API and returns a client for it, or `None` when no test
/// database is configured.
pub async fn serve() -> Option<Client> {
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
        axum::serve(
            listener,
            dally_server::Server::new(service, dally_server::Config::default())
                .expect("server")
                .router(),
        )
        .await
        .expect("serve");
    });
    Some(Client::new(format!("http://{address}")).expect("client"))
}

/// Starts a test server, or returns from the test when no database is
/// configured.
#[macro_export]
macro_rules! serve_or_skip {
    () => {
        match common::serve().await {
            Some(client) => client,
            None => {
                eprintln!("skipping: DALLY_TEST_POSTGRES_URL is not set");
                return;
            }
        }
    };
}

/// Waits until the message reaches `status`, failing the test after a few
/// seconds.
pub async fn wait_for_status(client: &Client, id: MessageId, status: Status) -> Message {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let message = client.get_message(id).await.expect("get message");
        if message.status == status {
            return message;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "message {id} stayed {} instead of becoming {status}",
            message.status
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
