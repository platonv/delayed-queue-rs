//! A web UI for [dally](https://github.com/platonv/dally).
//!
//! The UI is a client of the server like any other: every page is built from
//! calls to the public HTTP API through `dally-client`, and it never touches
//! storage. It can run inside the server process (`dally server --ui`) or on
//! its own (`dally ui --server URL`).
//!
//! The UI holds no key of its own. When the server has auth on, you log in
//! with your API key, which is kept in an `HttpOnly`, `SameSite=Strict`
//! cookie and sent with each API call, so the UI can do exactly what your key
//! can and nothing more.
//!
//! Pages are rendered on the server with askama. htmx refreshes the live
//! parts, but every page and button also works without JavaScript.

mod pages;
mod view;

use std::time::Duration;

use axum::Router;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::{HeaderValue, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use dally_client::Client;

/// Where the UI finds the server and where it is mounted.
#[derive(Debug, Clone)]
pub struct UiConfig {
    /// The server's base URL, such as `http://127.0.0.1:7070`.
    pub server: String,
    /// The path the UI is mounted under, such as `/ui`, or empty when it is
    /// served at the root. Links and cookies are scoped to it.
    pub base_path: String,
}

/// Builds the UI's routes. Mount them at `config.base_path` with
/// [`Router::nest`]; [`mount`] does that, including the trailing-slash
/// redirect.
pub fn router(config: UiConfig) -> Router {
    let ui = Ui::new(config);
    Router::new()
        .route("/", get(pages::queues))
        .route("/partials/queues", get(pages::queues_table))
        .route("/queues/{queue}", get(pages::queue))
        .route("/queues/{queue}/pause", post(pages::pause))
        .route("/queues/{queue}/resume", post(pages::resume))
        .route("/queues/{queue}/retry-dead", post(pages::retry_dead))
        .route("/messages/{id}", get(pages::message))
        .route("/messages/{id}/cancel", post(pages::cancel))
        .route("/messages/{id}/run-now", post(pages::run_now))
        .route("/login", get(pages::login_form).post(pages::login))
        .route("/logout", post(pages::logout))
        .route("/assets/htmx.min.js", get(htmx))
        .route("/assets/style.css", get(stylesheet))
        .with_state(ui)
}

/// The UI nested at `config.base_path`, ready to merge into another router.
/// `{base}/` redirects to `{base}`, which a nested router alone would 404.
pub fn mount(config: UiConfig) -> Router {
    let base = config.base_path.trim_end_matches('/').to_owned();
    if base.is_empty() {
        return router(config);
    }
    let home = base.clone();
    Router::new()
        .route(
            &format!("{base}/"),
            get(move || async move { axum::response::Redirect::permanent(&home) }),
        )
        .nest(&base, router(config))
}

/// What every handler shares.
#[derive(Clone)]
struct Ui {
    server: String,
    base: String,
    http: dally_client::reqwest::Client,
}

impl Ui {
    fn new(config: UiConfig) -> Self {
        let base = config.base_path.trim_end_matches('/').to_owned();
        Self {
            server: config.server,
            base,
            // One connection pool for every visitor; each request only adds
            // its own key.
            http: dally_client::reqwest::Client::new(),
        }
    }

    /// A client that calls the API with the visitor's key.
    fn client(&self, key: &ApiKey) -> Result<Client, dally_client::Error> {
        let mut builder = Client::builder(&self.server)
            .http_client(self.http.clone())
            .timeout(Duration::from_secs(10));
        if let Some(key) = &key.0 {
            builder = builder.api_key(key);
        }
        builder.build()
    }

    /// A path inside the UI, such as `/queues/emails` under `/ui`. The UI's
    /// home is the mount point itself (`/ui`), since a nested router does
    /// not answer `/ui/`.
    fn link(&self, path: &str) -> String {
        match (self.base.as_str(), path) {
            ("", path) => path.to_owned(),
            (base, "/") => base.to_owned(),
            (base, path) => format!("{base}{path}"),
        }
    }

    fn base(&self) -> Base<'_> {
        Base(&self.base)
    }

    fn cookie_path(&self) -> &str {
        if self.base.is_empty() {
            "/"
        } else {
            &self.base
        }
    }
}

/// The mount point, as templates use it: `{{ base }}/queues/...` for links
/// and `{{ base.home() }}` for the UI's front page.
#[derive(Clone, Copy)]
struct Base<'a>(&'a str);

impl Base<'_> {
    fn home(&self) -> &str {
        if self.0.is_empty() { "/" } else { self.0 }
    }
}

impl std::fmt::Display for Base<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

const KEY_COOKIE: &str = "dally_key";

/// The visitor's API key, from the login cookie.
struct ApiKey(Option<String>);

impl<S: Send + Sync> FromRequestParts<S> for ApiKey {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let key = parts
            .headers
            .get_all(header::COOKIE)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|cookies| cookies.split(';'))
            .find_map(|cookie| cookie.trim().strip_prefix(KEY_COOKIE)?.strip_prefix('='))
            .and_then(from_hex);
        Ok(ApiKey(key))
    }
}

/// The key is hex-encoded in the cookie, so any characters survive.
fn to_hex(text: &str) -> String {
    text.bytes().map(|byte| format!("{byte:02x}")).collect()
}

fn from_hex(hex: &str) -> Option<String> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    let bytes = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect::<Option<Vec<u8>>>()?;
    String::from_utf8(bytes).ok()
}

fn key_cookie(ui: &Ui, key: Option<&str>) -> HeaderValue {
    let (value, max_age) = match key {
        Some(key) => (to_hex(key), ""),
        None => (String::new(), "; Max-Age=0"),
    };
    let cookie = format!(
        "{KEY_COOKIE}={value}; Path={}; HttpOnly; SameSite=Strict{max_age}",
        ui.cookie_path()
    );
    HeaderValue::from_str(&cookie).unwrap_or_else(|_| HeaderValue::from_static(""))
}

async fn htmx() -> impl IntoResponse {
    asset("text/javascript", include_str!("../assets/htmx.min.js"))
}

async fn stylesheet() -> impl IntoResponse {
    asset("text/css", include_str!("../assets/style.css"))
}

fn asset(content_type: &'static str, body: &'static str) -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        body,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_round_trip_through_hex() {
        for key in ["admin-key-0123456789", "with spaces; and=signs", "ключ"] {
            assert_eq!(from_hex(&to_hex(key)).as_deref(), Some(key));
        }
        assert_eq!(from_hex("abc"), None);
        assert_eq!(from_hex("zz"), None);
    }
}
