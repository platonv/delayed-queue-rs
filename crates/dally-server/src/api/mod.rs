//! The HTTP API. Handlers stay thin: parse, call [`Service`], serialize.
//! Everything they do is documented in `docs/design.md`.

mod error;
mod messages;
mod queues;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{FromRequest, FromRequestParts, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use dally_core::{MessageId, QueueName, Service};
use tower_http::trace::TraceLayer;

pub use error::ApiError;

/// A JSON body whose errors come back in the API's error format, not as
/// axum's plain-text rejections.
#[derive(FromRequest)]
#[from_request(via(axum::Json), rejection(ApiError))]
struct Json<T>(T);

impl<T: serde::Serialize> axum::response::IntoResponse for Json<T> {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self.0).into_response()
    }
}

/// Query parameters with errors in the API's error format.
#[derive(FromRequestParts)]
#[from_request(via(axum::extract::Query), rejection(ApiError))]
struct Query<T>(T);

type ApiResult<T> = Result<T, ApiError>;

/// Builds the router for the whole API. Mount it in your own axum app, or
/// serve it with [`crate::run`].
pub fn router(service: Service) -> Router {
    let v1 = Router::new()
        .route("/queues", get(queues::list))
        .route("/queues/{queue}", get(queues::get).put(queues::put))
        .route("/queues/{queue}/pause", post(queues::pause))
        .route("/queues/{queue}/resume", post(queues::resume))
        .route(
            "/queues/{queue}/messages",
            post(messages::schedule).get(messages::list_in_queue),
        )
        .route("/queues/{queue}/poll", post(messages::poll))
        .route("/queues/{queue}/dead/retry", post(queues::retry_dead))
        .route("/messages", get(messages::list))
        .route(
            "/messages/{id}",
            get(messages::get)
                .patch(messages::reschedule)
                .delete(messages::cancel),
        )
        .route("/messages/{id}/ack", post(messages::ack))
        .route("/messages/{id}/nack", post(messages::nack));

    Router::new()
        .nest("/v1", v1)
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .layer(TraceLayer::new_for_http())
        .with_state(service)
}

/// The process is up.
async fn healthz() -> &'static str {
    "ok"
}

/// The process is up and can reach storage.
async fn readyz(State(service): State<Service>) -> (StatusCode, &'static str) {
    match service.storage().now().await {
        Ok(_) => (StatusCode::OK, "ok"),
        Err(err) => {
            tracing::warn!(error = %err, "readiness check failed");
            (StatusCode::SERVICE_UNAVAILABLE, "storage unavailable")
        }
    }
}

/// Parses a body that may be left out entirely, as in `POST .../poll` with
/// no options.
fn optional_json<T: serde::de::DeserializeOwned + Default>(body: &Bytes) -> ApiResult<T> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(T::default());
    }
    serde_json::from_slice(body).map_err(|err| ApiError::invalid(err.to_string()))
}

fn queue_name(name: String) -> ApiResult<QueueName> {
    Ok(QueueName::new(name)?)
}

fn message_id(id: &str) -> ApiResult<MessageId> {
    Ok(id.parse()?)
}
