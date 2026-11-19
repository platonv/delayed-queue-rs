use axum::body::Bytes;
use axum::extract::{Path, State};
use dally_core::{MessageId, Queue, QueueSettings, QueueSummary, Service};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::auth::Admin;

use super::openapi::ErrorBody;
use super::{ApiResult, Json, optional_json, queue_name};

#[derive(Debug, Serialize, ToSchema)]
pub struct QueueList {
    queues: Vec<QueueSummary>,
}

/// `GET /v1/queues`: every queue with its message counts.
#[utoipa::path(
    get, path = "/v1/queues", tag = "queues",
    responses(
        (status = 200, description = "Every queue with its message counts", body = QueueList),
        (status = 401, description = "Missing or unknown API key", body = ErrorBody), (status = 503, description = "Storage is unavailable", body = ErrorBody)
    )
)]
pub async fn list(State(service): State<Service>) -> ApiResult<Json<QueueList>> {
    let queues = service.list_queues().await?;
    Ok(Json(QueueList { queues }))
}

/// `GET /v1/queues/{queue}`: settings and message counts.
#[utoipa::path(
    get, path = "/v1/queues/{queue}", tag = "queues",
    params(("queue" = String, Path, description = "Queue name")),
    responses(
        (status = 200, description = "Settings and message counts", body = QueueSummary),
        (status = 400, description = "Invalid input", body = ErrorBody), (status = 401, description = "Missing or unknown API key", body = ErrorBody), (status = 404, description = "Not found", body = ErrorBody), (status = 503, description = "Storage is unavailable", body = ErrorBody)
    )
)]
pub async fn get(
    State(service): State<Service>,
    Path(queue): Path<String>,
) -> ApiResult<Json<QueueSummary>> {
    Ok(Json(service.get_queue(&queue_name(queue)?).await?))
}

/// `PUT /v1/queues/{queue}`: creates the queue or replaces its settings.
/// Fields left out get their defaults, not their current values.
#[utoipa::path(
    put, path = "/v1/queues/{queue}", tag = "queues",
    params(("queue" = String, Path, description = "Queue name")),
    request_body = QueueSettings,
    responses(
        (status = 200, description = "Created or updated", body = Queue),
        (status = 400, description = "Invalid input", body = ErrorBody), (status = 401, description = "Missing or unknown API key", body = ErrorBody), (status = 403, description = "Needs an admin key", body = ErrorBody), (status = 503, description = "Storage is unavailable", body = ErrorBody)
    )
)]
pub async fn put(
    _admin: Admin,
    State(service): State<Service>,
    Path(queue): Path<String>,
    Json(settings): Json<QueueSettings>,
) -> ApiResult<Json<Queue>> {
    Ok(Json(
        service.put_queue(&queue_name(queue)?, settings).await?,
    ))
}

/// `POST /v1/queues/{queue}/pause`.
#[utoipa::path(
    post, path = "/v1/queues/{queue}/pause", tag = "queues",
    params(("queue" = String, Path, description = "Queue name")),
    responses(
        (status = 200, description = "Paused", body = Queue),
        (status = 400, description = "Invalid input", body = ErrorBody), (status = 401, description = "Missing or unknown API key", body = ErrorBody), (status = 403, description = "Needs an admin key", body = ErrorBody), (status = 404, description = "Not found", body = ErrorBody), (status = 503, description = "Storage is unavailable", body = ErrorBody)
    )
)]
pub async fn pause(
    _admin: Admin,
    State(service): State<Service>,
    Path(queue): Path<String>,
) -> ApiResult<Json<Queue>> {
    Ok(Json(service.set_paused(&queue_name(queue)?, true).await?))
}

/// `POST /v1/queues/{queue}/resume`.
#[utoipa::path(
    post, path = "/v1/queues/{queue}/resume", tag = "queues",
    params(("queue" = String, Path, description = "Queue name")),
    responses(
        (status = 200, description = "Resumed", body = Queue),
        (status = 400, description = "Invalid input", body = ErrorBody), (status = 401, description = "Missing or unknown API key", body = ErrorBody), (status = 403, description = "Needs an admin key", body = ErrorBody), (status = 404, description = "Not found", body = ErrorBody), (status = 503, description = "Storage is unavailable", body = ErrorBody)
    )
)]
pub async fn resume(
    _admin: Admin,
    State(service): State<Service>,
    Path(queue): Path<String>,
) -> ApiResult<Json<Queue>> {
    Ok(Json(service.set_paused(&queue_name(queue)?, false).await?))
}

#[derive(Debug, Default, Deserialize, ToSchema)]
#[serde(default, deny_unknown_fields)]
pub struct RetryBody {
    /// Only these messages. Without it, every dead message in the queue.
    #[schema(value_type = Option<Vec<uuid::Uuid>>)]
    ids: Option<Vec<MessageId>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Retried {
    retried: u64,
}

/// `POST /v1/queues/{queue}/dead/retry` with an optional `{"ids": [...]}`.
/// Without ids, retries every dead message in the queue.
#[utoipa::path(
    post, path = "/v1/queues/{queue}/dead/retry", tag = "queues",
    params(("queue" = String, Path, description = "Queue name")),
    request_body(content = Option<RetryBody>, description = "Optional; without ids, every dead message is retried"),
    responses(
        (status = 200, description = "How many messages were retried", body = Retried),
        (status = 400, description = "Invalid input", body = ErrorBody), (status = 401, description = "Missing or unknown API key", body = ErrorBody), (status = 403, description = "Needs an admin key", body = ErrorBody), (status = 404, description = "Not found", body = ErrorBody), (status = 503, description = "Storage is unavailable", body = ErrorBody)
    )
)]
pub async fn retry_dead(
    _admin: Admin,
    State(service): State<Service>,
    Path(queue): Path<String>,
    body: Bytes,
) -> ApiResult<Json<Retried>> {
    let body: RetryBody = optional_json(&body)?;
    let retried = service
        .retry_dead(&queue_name(queue)?, body.ids.as_deref())
        .await?;
    Ok(Json(Retried { retried }))
}
