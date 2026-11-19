use axum::body::Bytes;
use axum::extract::{Path, State};
use dally_core::{MessageId, Queue, QueueSettings, QueueSummary, Service};
use serde::{Deserialize, Serialize};

use crate::auth::Admin;

use super::{ApiResult, Json, optional_json, queue_name};

#[derive(Debug, Serialize)]
pub struct QueueList {
    queues: Vec<QueueSummary>,
}

/// `GET /v1/queues`: every queue with its message counts.
pub async fn list(State(service): State<Service>) -> ApiResult<Json<QueueList>> {
    let queues = service.list_queues().await?;
    Ok(Json(QueueList { queues }))
}

/// `GET /v1/queues/{queue}`: settings and message counts.
pub async fn get(
    State(service): State<Service>,
    Path(queue): Path<String>,
) -> ApiResult<Json<QueueSummary>> {
    Ok(Json(service.get_queue(&queue_name(queue)?).await?))
}

/// `PUT /v1/queues/{queue}`: creates the queue or replaces its settings.
/// Fields left out get their defaults, not their current values.
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
pub async fn pause(
    _admin: Admin,
    State(service): State<Service>,
    Path(queue): Path<String>,
) -> ApiResult<Json<Queue>> {
    Ok(Json(service.set_paused(&queue_name(queue)?, true).await?))
}

/// `POST /v1/queues/{queue}/resume`.
pub async fn resume(
    _admin: Admin,
    State(service): State<Service>,
    Path(queue): Path<String>,
) -> ApiResult<Json<Queue>> {
    Ok(Json(service.set_paused(&queue_name(queue)?, false).await?))
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetryBody {
    ids: Option<Vec<MessageId>>,
}

#[derive(Debug, Serialize)]
pub struct Retried {
    retried: u64,
}

/// `POST /v1/queues/{queue}/dead/retry` with an optional `{"ids": [...]}`.
/// Without ids, retries every dead message in the queue.
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
