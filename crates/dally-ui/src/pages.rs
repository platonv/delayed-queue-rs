//! The pages and the form actions behind their buttons.

use askama::Template;
use axum::Form;
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Redirect, Response};
use chrono::Utc;
use dally_client::{MessageFilter, MessageId, QueueName, RunAt, Status};
use serde::Deserialize;

use crate::view::{self, MessageRow, QueueRow};
use crate::{ApiKey, Base, Ui, key_cookie};

/// How many messages a queue page lists at once.
const PAGE_SIZE: u32 = 50;

/// Why a page could not be shown.
enum Failure {
    Api(dally_client::Error),
    Render(askama::Error),
    BadInput(String),
}

impl From<dally_client::Error> for Failure {
    fn from(err: dally_client::Error) -> Self {
        Failure::Api(err)
    }
}

impl From<askama::Error> for Failure {
    fn from(err: askama::Error) -> Self {
        Failure::Render(err)
    }
}

type PageResult = Result<Response, Failure>;

impl Ui {
    /// Runs a page handler and turns its failure into a page: the login form
    /// for a missing or rejected key, an error page for everything else.
    async fn respond(&self, key: &ApiKey, page: impl Future<Output = PageResult>) -> Response {
        let failure = match page.await {
            Ok(response) => return response,
            Err(failure) => failure,
        };

        let (status, title, detail) = match failure {
            Failure::Api(err) if err.status() == Some(StatusCode::UNAUTHORIZED) => {
                return Redirect::to(&self.link("/login")).into_response();
            }
            Failure::Api(err) if err.status() == Some(StatusCode::FORBIDDEN) => (
                StatusCode::FORBIDDEN,
                "Not allowed",
                "Your API key cannot do this. Log in with an admin key.".to_owned(),
            ),
            Failure::Api(err) if err.is_not_found() => {
                (StatusCode::NOT_FOUND, "Not found", err.to_string())
            }
            Failure::Api(err) if err.status().is_none() => (
                StatusCode::BAD_GATEWAY,
                "Cannot reach the server",
                format!("The UI could not reach dally at {}: {err}", self.server),
            ),
            Failure::Api(err) => (
                StatusCode::BAD_GATEWAY,
                "Something went wrong",
                err.to_string(),
            ),
            Failure::BadInput(detail) => (StatusCode::BAD_REQUEST, "Bad request", detail),
            Failure::Render(err) => {
                tracing::error!(error = %err, "cannot render page");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Something went wrong",
                    "The page could not be rendered.".to_owned(),
                )
            }
        };

        let page = ErrorPage {
            base: self.base(),
            signed_in: key.0.is_some(),
            title,
            detail,
        };
        match page.render() {
            Ok(html) => (status, Html(html)).into_response(),
            Err(_) => (status, format!("dally: {title}")).into_response(),
        }
    }
}

fn render(page: impl Template) -> PageResult {
    Ok(Html(page.render()?).into_response())
}

fn queue_name(name: &str) -> Result<QueueName, Failure> {
    QueueName::new(name).map_err(|err| Failure::BadInput(err.to_string()))
}

#[derive(Template)]
#[template(path = "error.html")]
struct ErrorPage<'a> {
    base: Base<'a>,
    signed_in: bool,
    title: &'a str,
    detail: String,
}

#[derive(Template)]
#[template(path = "queues.html")]
struct QueuesPage<'a> {
    base: Base<'a>,
    signed_in: bool,
    rows: Vec<QueueRow>,
}

#[derive(Template)]
#[template(path = "queues_table.html")]
struct QueuesTable<'a> {
    base: Base<'a>,
    rows: Vec<QueueRow>,
}

/// `GET /`: every queue with its counts.
pub async fn queues(State(ui): State<Ui>, key: ApiKey) -> Response {
    ui.respond(&key, async {
        let queues = ui.client(&key)?.list_queues().await?;
        render(QueuesPage {
            base: ui.base(),
            signed_in: key.0.is_some(),
            rows: queues.iter().map(QueueRow::from).collect(),
        })
    })
    .await
}

/// `GET /partials/queues`: the queue table alone, for htmx to refresh.
pub async fn queues_table(State(ui): State<Ui>, key: ApiKey) -> Response {
    ui.respond(&key, async {
        let queues = ui.client(&key)?.list_queues().await?;
        render(QueuesTable {
            base: ui.base(),
            rows: queues.iter().map(QueueRow::from).collect(),
        })
    })
    .await
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct QueueParams {
    status: Option<Status>,
    before: Option<MessageId>,
}

/// A status filter tab on the queue page.
struct Tab {
    label: &'static str,
    count: u64,
    active: bool,
}

#[derive(Template)]
#[template(path = "queue.html")]
struct QueuePage<'a> {
    base: Base<'a>,
    signed_in: bool,
    name: String,
    paused: bool,
    settings: Vec<(&'static str, String)>,
    tabs: Vec<Tab>,
    status: &'static str,
    dead: u64,
    rows: Vec<MessageRow>,
    next: Option<String>,
}

/// `GET /queues/{queue}?status=&before=`: settings, counts and messages.
pub async fn queue(
    State(ui): State<Ui>,
    key: ApiKey,
    Path(name): Path<String>,
    Query(params): Query<QueueParams>,
) -> Response {
    ui.respond(&key, async {
        let queue = queue_name(&name)?;
        let client = ui.client(&key)?;
        let summary = client.get_queue(queue.as_str()).await?;
        let status = params.status.unwrap_or(Status::Scheduled);
        let page = client
            .list_messages(&MessageFilter {
                queue: Some(queue.clone()),
                status: Some(status),
                before: params.before,
                limit: PAGE_SIZE,
            })
            .await?;

        let settings = &summary.queue.settings;
        let stats = summary.stats;
        let duration = |d| format_duration(d);
        let now = Utc::now();
        render(QueuePage {
            base: ui.base(),
            signed_in: key.0.is_some(),
            name: queue.to_string(),
            paused: settings.paused,
            settings: vec![
                ("Visibility timeout", duration(settings.visibility_timeout)),
                ("Max attempts", settings.max_attempts.to_string()),
                (
                    "Backoff",
                    format!(
                        "{} doubling up to {}",
                        duration(settings.backoff.base),
                        duration(settings.backoff.cap)
                    ),
                ),
                ("Retention", duration(settings.retention)),
                (
                    "Push",
                    settings
                        .push
                        .as_ref()
                        .map_or_else(|| "off, workers poll".to_owned(), |push| push.url.clone()),
                ),
            ],
            tabs: view::STATUSES
                .iter()
                .map(|&tab| Tab {
                    label: tab.as_str(),
                    count: match tab {
                        Status::Scheduled => stats.scheduled,
                        Status::Running => stats.running,
                        Status::Succeeded => stats.succeeded,
                        Status::Cancelled => stats.cancelled,
                        Status::Dead => stats.dead,
                    },
                    active: tab == status,
                })
                .collect(),
            status: status.as_str(),
            dead: stats.dead,
            rows: page
                .items
                .iter()
                .map(|message| MessageRow::new(message, now))
                .collect(),
            next: page.next.map(|id| id.to_string()),
        })
    })
    .await
}

/// Durations as the server's config files write them: `30s`, `5m`, `7days`.
fn format_duration(duration: std::time::Duration) -> String {
    let seconds = duration.as_secs();
    match seconds {
        0 => format!("{}ms", duration.as_millis()),
        s if s % 86_400 == 0 => format!("{}d", s / 86_400),
        s if s % 3_600 == 0 => format!("{}h", s / 3_600),
        s if s % 60 == 0 => format!("{}m", s / 60),
        s => format!("{s}s"),
    }
}

/// `POST /queues/{queue}/pause`.
pub async fn pause(State(ui): State<Ui>, key: ApiKey, Path(name): Path<String>) -> Response {
    ui.respond(&key, async {
        ui.client(&key)?.pause(queue_name(&name)?.as_str()).await?;
        Ok(Redirect::to(&ui.link(&format!("/queues/{name}"))).into_response())
    })
    .await
}

/// `POST /queues/{queue}/resume`.
pub async fn resume(State(ui): State<Ui>, key: ApiKey, Path(name): Path<String>) -> Response {
    ui.respond(&key, async {
        ui.client(&key)?.resume(queue_name(&name)?.as_str()).await?;
        Ok(Redirect::to(&ui.link(&format!("/queues/{name}"))).into_response())
    })
    .await
}

/// `POST /queues/{queue}/retry-dead`: retries every dead message.
pub async fn retry_dead(State(ui): State<Ui>, key: ApiKey, Path(name): Path<String>) -> Response {
    ui.respond(&key, async {
        ui.client(&key)?
            .retry_dead(queue_name(&name)?.as_str(), None)
            .await?;
        Ok(Redirect::to(&ui.link(&format!("/queues/{name}?status=scheduled"))).into_response())
    })
    .await
}

#[derive(Template)]
#[template(path = "message.html")]
struct MessagePage<'a> {
    base: Base<'a>,
    signed_in: bool,
    id: String,
    queue: String,
    status: &'static str,
    scheduled: bool,
    fields: Vec<(&'static str, String)>,
    last_error: Option<String>,
    payload: String,
}

/// `GET /messages/{id}`.
pub async fn message(State(ui): State<Ui>, key: ApiKey, Path(id): Path<String>) -> Response {
    ui.respond(&key, async {
        let id: MessageId = id
            .parse()
            .map_err(|_| Failure::BadInput(format!("{id:?} is not a message id")))?;
        let message = ui.client(&key)?.get_message(id).await?;
        let now = Utc::now();
        let optional = |value: Option<String>| value.unwrap_or_else(|| "-".to_owned());

        render(MessagePage {
            base: ui.base(),
            signed_in: key.0.is_some(),
            id: message.id.to_string(),
            queue: message.queue.to_string(),
            status: message.status.as_str(),
            scheduled: message.status == Status::Scheduled,
            fields: vec![
                (
                    "Attempts",
                    format!("{} of {}", message.attempts, message.max_attempts),
                ),
                ("Priority", message.priority.to_string()),
                (
                    "Run at",
                    format!(
                        "{} ({})",
                        view::timestamp(message.run_at),
                        view::relative(message.run_at, now)
                    ),
                ),
                ("Idempotency key", optional(message.idempotency_key.clone())),
                ("Created", view::timestamp(message.created_at)),
                (
                    "Finished",
                    optional(message.finished_at.map(view::timestamp)),
                ),
                (
                    "Lease expires",
                    optional(message.lease_expires_at.map(view::timestamp)),
                ),
            ],
            last_error: message.last_error.clone(),
            payload: serde_json::to_string_pretty(&message.payload).unwrap_or_default(),
        })
    })
    .await
}

/// `POST /messages/{id}/cancel`.
pub async fn cancel(State(ui): State<Ui>, key: ApiKey, Path(id): Path<String>) -> Response {
    ui.respond(&key, async {
        let id: MessageId = id
            .parse()
            .map_err(|_| Failure::BadInput(format!("{id:?} is not a message id")))?;
        ui.client(&key)?.cancel(id).await?;
        Ok(Redirect::to(&ui.link(&format!("/messages/{id}"))).into_response())
    })
    .await
}

/// `POST /messages/{id}/run-now`: reschedules a waiting message to now.
pub async fn run_now(State(ui): State<Ui>, key: ApiKey, Path(id): Path<String>) -> Response {
    ui.respond(&key, async {
        let id: MessageId = id
            .parse()
            .map_err(|_| Failure::BadInput(format!("{id:?} is not a message id")))?;
        ui.client(&key)?.reschedule(id, RunAt::Now).await?;
        Ok(Redirect::to(&ui.link(&format!("/messages/{id}"))).into_response())
    })
    .await
}

#[derive(Template)]
#[template(path = "login.html")]
struct LoginPage<'a> {
    base: Base<'a>,
    signed_in: bool,
    error: Option<&'a str>,
}

/// `GET /login`.
pub async fn login_form(State(ui): State<Ui>, key: ApiKey) -> Response {
    ui.respond(&key, async {
        render(LoginPage {
            base: ui.base(),
            signed_in: false,
            error: None,
        })
    })
    .await
}

#[derive(Debug, Deserialize)]
pub struct LoginForm {
    key: String,
}

/// `POST /login`: checks the key against the server, then keeps it in a
/// cookie.
pub async fn login(State(ui): State<Ui>, Form(form): Form<LoginForm>) -> Response {
    let key = ApiKey(Some(form.key.trim().to_owned()));
    let checked = match ui.client(&key) {
        Ok(client) => client.list_queues().await.map(|_| ()),
        Err(err) => Err(err),
    };

    match checked {
        Ok(()) => (
            [(header::SET_COOKIE, key_cookie(&ui, key.0.as_deref()))],
            Redirect::to(&ui.link("/")),
        )
            .into_response(),
        Err(err) if err.status() == Some(StatusCode::UNAUTHORIZED) => {
            let page = LoginPage {
                base: ui.base(),
                signed_in: false,
                error: Some("The server did not accept that key."),
            };
            ui.respond(&ApiKey(None), async { render(page) }).await
        }
        Err(err) => {
            ui.respond(&ApiKey(None), async { Err(Failure::Api(err)) })
                .await
        }
    }
}

/// `POST /logout`.
pub async fn logout(State(ui): State<Ui>) -> Response {
    (
        [(header::SET_COOKIE, key_cookie(&ui, None))],
        Redirect::to(&ui.link("/login")),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn durations_use_the_largest_whole_unit() {
        assert_eq!(format_duration(Duration::from_secs(30)), "30s");
        assert_eq!(format_duration(Duration::from_secs(300)), "5m");
        assert_eq!(format_duration(Duration::from_secs(3_600)), "1h");
        assert_eq!(format_duration(Duration::from_secs(7 * 86_400)), "7d");
        assert_eq!(format_duration(Duration::from_secs(90)), "90s");
        assert_eq!(format_duration(Duration::from_millis(250)), "250ms");
    }
}
