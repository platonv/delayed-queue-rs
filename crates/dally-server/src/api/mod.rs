//! The HTTP API. Handlers stay thin: parse, call [`Service`], serialize.
//! Everything they do is documented in `docs/design.md`.

mod error;
mod messages;
mod queues;

use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{FromRequest, FromRequestParts, State};
use axum::http::StatusCode;
use axum::middleware::from_fn_with_state;
use axum::routing::{get, post};
use dally_core::{MessageId, QueueName, Service};
use tower_http::trace::TraceLayer;

pub use error::ApiError;

use crate::auth::{Auth, authenticate};
use crate::metrics::{self, Metrics, MetricsState};

/// A JSON body whose errors come back in the API's error format, not as
/// axum's plain-text rejections.
///
/// Unlike `axum::Json`, it does not insist on `Content-Type:
/// application/json`, so `curl -d '{...}'` works without extra flags. The
/// body is JSON on every endpoint, so the header adds nothing.
struct Json<T>(T);

impl<S, T> FromRequest<S> for Json<T>
where
    S: Send + Sync,
    T: serde::de::DeserializeOwned,
{
    type Rejection = ApiError;

    async fn from_request(request: axum::extract::Request, state: &S) -> ApiResult<Self> {
        let body = Bytes::from_request(request, state)
            .await
            .map_err(|rejection| ApiError::invalid(rejection.body_text()))?;
        serde_json::from_slice(&body)
            .map(Json)
            .map_err(|err| ApiError::invalid(format!("invalid JSON body: {err}")))
    }
}

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

/// Builds the router for the whole API. [`crate::Server::router`] is the
/// public way in.
pub fn router(
    service: Service,
    auth: Arc<Auth>,
    metrics: Arc<Metrics>,
    protect_metrics: bool,
) -> Router {
    // Admin-only endpoints say so with an `Admin` argument in their handler.
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
        .route("/messages/{id}/nack", post(messages::nack))
        .route_layer(from_fn_with_state(Arc::clone(&auth), authenticate));

    let mut metrics_route = Router::new()
        .route("/metrics", get(metrics::handler))
        .with_state(MetricsState {
            metrics: Arc::clone(&metrics),
            service: service.clone(),
        });
    if protect_metrics {
        metrics_route = metrics_route.route_layer(from_fn_with_state(auth, authenticate));
    }

    Router::new()
        .nest("/v1", v1)
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(service)
        .merge(metrics_route)
        .layer(from_fn_with_state(metrics, metrics::track_requests))
        .layer(TraceLayer::new_for_http())
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
