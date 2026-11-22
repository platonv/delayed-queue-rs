//! The UI against a real server on SQLite, mounted at `/ui` like
//! `dally server --ui` does.

use std::sync::Arc;

use dally_core::storage::sqlite::SqliteStorage;
use dally_core::{ConnectOptions, QueueSettings, Service};
use dally_server::auth::{AuthConfig, KeyConfig, Role};
use dally_server::{Config, Server};
use dally_ui::UiConfig;
use reqwest::StatusCode;
use reqwest::redirect::Policy;
use serde_json::json;

const CLIENT_KEY: &str = "client-key-0123456789";
const ADMIN_KEY: &str = "admin-key-0123456789";

/// Serves the API and the UI on one random port, and returns its URL.
async fn serve(config: Config) -> String {
    let storage = SqliteStorage::connect("sqlite::memory:", &ConnectOptions::default())
        .await
        .expect("open storage");
    let service = Service::new(Arc::new(storage), QueueSettings::default());
    let api = Server::new(service, config).expect("server").router();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let base = format!("http://{}", listener.local_addr().expect("address"));
    let ui = dally_ui::mount(UiConfig {
        server: base.clone(),
        base_path: "/ui".to_owned(),
    });
    tokio::spawn(async move { axum::serve(listener, api.merge(ui)).await.expect("serve") });
    base
}

fn browser() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(Policy::none())
        .build()
        .expect("client")
}

async fn page(http: &reqwest::Client, url: String) -> (StatusCode, String) {
    let response = http.get(url).send().await.expect("request");
    (response.status(), response.text().await.expect("body"))
}

async fn json(request: reqwest::RequestBuilder) -> serde_json::Value {
    request
        .send()
        .await
        .expect("request")
        .json()
        .await
        .expect("json")
}

fn with_keys() -> Config {
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
            protect_metrics: false,
        },
        ..Config::default()
    }
}

#[tokio::test]
async fn pages_show_queues_and_messages() {
    let base = serve(Config::default()).await;
    let http = browser();

    let scheduled = json(
        http.post(format!("{base}/v1/queues/emails/messages"))
            .json(&json!({"payload": {"to": "ana"}, "max_attempts": 1})),
    )
    .await;
    let id = scheduled["id"].as_str().expect("id").to_owned();
    let polled = json(http.post(format!("{base}/v1/queues/emails/poll"))).await;
    json(
        http.post(format!("{base}/v1/messages/{id}/nack"))
            .json(&json!({"lease": polled["messages"][0]["lease"], "error": "smtp timeout"})),
    )
    .await;

    let (status, html) = page(&http, format!("{base}/ui")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains(r#"<a href="/ui/queues/emails">emails</a>"#),
        "{html}"
    );

    let slash = http
        .get(format!("{base}/ui/"))
        .send()
        .await
        .expect("request");
    assert_eq!(slash.status(), StatusCode::PERMANENT_REDIRECT);

    let (status, html) = page(&http, format!("{base}/ui/queues/emails?status=dead")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("smtp timeout"), "{html}");
    assert!(html.contains("Retry 1 dead"), "{html}");

    let (status, html) = page(&http, format!("{base}/ui/messages/{id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains(r#"<span class="status dead">dead</span>"#),
        "{html}"
    );

    let retried = http
        .post(format!("{base}/ui/queues/emails/retry-dead"))
        .send()
        .await
        .expect("retry");
    assert_eq!(retried.status(), StatusCode::SEE_OTHER);
    let queue = json(http.get(format!("{base}/v1/queues/emails"))).await;
    assert_eq!(queue["stats"]["scheduled"], 1);

    let missing = format!("{base}/ui/messages/00000000-0000-7000-8000-000000000000");
    let (status, html) = page(&http, missing).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(html.contains("<h1>Not found</h1>"), "{html}");
}

#[tokio::test]
async fn login_uses_the_visitors_own_key() {
    let base = serve(with_keys()).await;
    let http = browser();

    let anonymous = http
        .get(format!("{base}/ui"))
        .send()
        .await
        .expect("request");
    assert_eq!(anonymous.status(), StatusCode::SEE_OTHER);
    assert_eq!(anonymous.headers()["location"], "/ui/login");

    let rejected = http
        .post(format!("{base}/ui/login"))
        .form(&[("key", "not-a-key")])
        .send()
        .await
        .expect("login");
    let body = rejected.text().await.expect("body");
    assert!(body.contains("did not accept that key"), "{body}");

    let login = async |key: &str| {
        let response = http
            .post(format!("{base}/ui/login"))
            .form(&[("key", key)])
            .send()
            .await
            .expect("login");
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        let cookie = response.headers()["set-cookie"]
            .to_str()
            .expect("cookie")
            .to_owned();
        assert!(cookie.contains("HttpOnly"), "{cookie}");
        assert!(cookie.contains("SameSite=Strict"), "{cookie}");
        assert!(cookie.contains("Path=/ui"), "{cookie}");
        cookie.split(';').next().expect("name=value").to_owned()
    };
    let client_cookie = login(CLIENT_KEY).await;
    let admin_cookie = login(ADMIN_KEY).await;

    json(
        http.put(format!("{base}/v1/queues/emails"))
            .bearer_auth(ADMIN_KEY)
            .json(&json!({})),
    )
    .await;

    let dashboard = http
        .get(format!("{base}/ui"))
        .header("cookie", &client_cookie)
        .send()
        .await
        .expect("dashboard");
    assert_eq!(dashboard.status(), StatusCode::OK);

    let pause = async |cookie: &str| {
        http.post(format!("{base}/ui/queues/emails/pause"))
            .header("cookie", cookie)
            .send()
            .await
            .expect("pause")
            .status()
    };
    assert_eq!(pause(&client_cookie).await, StatusCode::FORBIDDEN);
    assert_eq!(pause(&admin_cookie).await, StatusCode::SEE_OTHER);
}

#[tokio::test]
async fn assets_are_served() {
    let base = serve(Config::default()).await;
    let http = browser();
    for (path, content_type) in [
        ("htmx.min.js", "text/javascript"),
        ("style.css", "text/css"),
    ] {
        let response = http
            .get(format!("{base}/ui/assets/{path}"))
            .send()
            .await
            .expect("asset");
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert_eq!(response.headers()["content-type"], content_type, "{path}");
    }
}
