//! The published OpenAPI spec matches the routes the server actually serves.

use std::sync::Arc;

use dally_core::storage::sqlite::SqliteStorage;
use dally_core::{ConnectOptions, QueueSettings, Service};
use dally_server::{Config, Server};
use reqwest::{Method, StatusCode};

#[tokio::test]
async fn every_documented_route_is_served() {
    let storage = SqliteStorage::connect("sqlite::memory:", &ConnectOptions::default())
        .await
        .expect("open storage");
    let service = Service::new(Arc::new(storage), QueueSettings::default());
    let router = Server::new(service, Config::default())
        .expect("server")
        .router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let base = format!("http://{}", listener.local_addr().expect("address"));
    tokio::spawn(async move { axum::serve(listener, router).await.expect("serve") });

    let http = reqwest::Client::new();
    let spec: serde_json::Value = http
        .get(format!("{base}/v1/openapi.json"))
        .send()
        .await
        .expect("spec")
        .json()
        .await
        .expect("spec is JSON");
    let paths = spec["paths"].as_object().expect("paths");
    assert!(
        paths.len() >= 13,
        "expected the whole API, got {} paths",
        paths.len()
    );

    for (path, operations) in paths {
        let url = format!("{base}{path}")
            .replace("{queue}", "emails")
            .replace("{id}", "00000000-0000-7000-8000-000000000000");
        for method in operations.as_object().expect("operations").keys() {
            let method: Method = method.to_uppercase().parse().expect("HTTP method");
            let response = http
                .request(method.clone(), &url)
                .send()
                .await
                .expect("request");
            let status = response.status();
            let body = response.text().await.unwrap_or_default();

            // A route that exists answers with its own JSON error at worst;
            // a missing one gets axum's empty 404 or a 405.
            let unrouted = status == StatusCode::METHOD_NOT_ALLOWED
                || (status == StatusCode::NOT_FOUND && !body.contains("\"error\""));
            assert!(
                !unrouted,
                "{method} {path} is documented but not served ({status})"
            );
        }
    }
}
