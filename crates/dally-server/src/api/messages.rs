use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use dally_core::{
    LeaseToken, Leased, Message, MessageFilter, MessageId, NackRequest, Page, PollRequest, RunAt,
    ScheduleRequest, Scheduled, Service, Status,
};
use serde::{Deserialize, Serialize};

use super::{ApiError, ApiResult, Json, Query, message_id, optional_json, queue_name};

/// `POST /v1/queues/{queue}/messages` takes one message or an array of them.
///
/// One message: `201 Created` with the message, or `200 OK` with the existing
/// message if its idempotency key was already used.
///
/// An array: `200 OK` with `{"messages": [{"created": bool, "message": ...}]}`
/// in the same order.
pub async fn schedule(
    State(service): State<Service>,
    Path(queue): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> ApiResult<Response> {
    let queue = queue_name(queue)?;

    // Parsing by hand rather than with an untagged enum keeps serde's error
    // messages useful ("unknown field `dely`") instead of "did not match any
    // variant".
    let is_batch = body.is_array();
    let requests: Vec<ScheduleRequest> = if is_batch {
        serde_json::from_value(body)
    } else {
        serde_json::from_value(body).map(|request| vec![request])
    }
    .map_err(|err| ApiError::invalid(err.to_string()))?;

    let mut results = service.schedule(&queue, requests).await?;

    if is_batch {
        return Ok(Json(Batch { messages: results }).into_response());
    }
    let Scheduled { message, created } = results
        .pop()
        .ok_or_else(|| ApiError::invalid("nothing was scheduled"))?;
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(message)).into_response())
}

#[derive(Debug, Serialize)]
pub struct Batch {
    messages: Vec<Scheduled>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PollBody {
    limit: Option<u32>,
    #[serde(with = "humantime_serde")]
    wait: Option<Duration>,
}

#[derive(Debug, Serialize)]
pub struct Polled {
    messages: Vec<Leased>,
}

/// `POST /v1/queues/{queue}/poll` with an optional body
/// `{"limit": 10, "wait": "20s"}`.
pub async fn poll(
    State(service): State<Service>,
    Path(queue): Path<String>,
    body: Bytes,
) -> ApiResult<Json<Polled>> {
    let queue = queue_name(queue)?;
    let body: PollBody = optional_json(&body)?;
    let request = PollRequest {
        limit: body.limit.unwrap_or(1),
        wait: body.wait.unwrap_or_default(),
    };

    let messages = service.poll(&queue, request).await?;
    Ok(Json(Polled { messages }))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AckBody {
    lease: LeaseToken,
}

/// `POST /v1/messages/{id}/ack` with `{"lease": "..."}`.
pub async fn ack(
    State(service): State<Service>,
    Path(id): Path<String>,
    Json(body): Json<AckBody>,
) -> ApiResult<Json<Message>> {
    let message = service.ack(message_id(&id)?, &body.lease).await?;
    Ok(Json(message))
}

/// `POST /v1/messages/{id}/nack` with `{"lease": "...", "error": "...",
/// "retry_in": "30s"}`. Only `lease` is required.
pub async fn nack(
    State(service): State<Service>,
    Path(id): Path<String>,
    Json(body): Json<NackRequest>,
) -> ApiResult<Json<Message>> {
    let message = service.nack(message_id(&id)?, body).await?;
    Ok(Json(message))
}

/// `GET /v1/messages/{id}`.
pub async fn get(
    State(service): State<Service>,
    Path(id): Path<String>,
) -> ApiResult<Json<Message>> {
    Ok(Json(service.get_message(message_id(&id)?).await?))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RescheduleBody {
    #[serde(default, with = "humantime_serde")]
    delay: Option<Duration>,
    #[serde(default)]
    run_at: Option<DateTime<Utc>>,
}

/// `PATCH /v1/messages/{id}` with `{"delay": "1h"}` or `{"run_at": "..."}`.
/// An empty object runs it now.
pub async fn reschedule(
    State(service): State<Service>,
    Path(id): Path<String>,
    Json(body): Json<RescheduleBody>,
) -> ApiResult<Json<Message>> {
    let run_at = match (body.delay, body.run_at) {
        (Some(_), Some(_)) => {
            return Err(ApiError::invalid("use either delay or run_at, not both"));
        }
        (Some(delay), None) => RunAt::After(delay),
        (None, Some(at)) => RunAt::At(at),
        (None, None) => RunAt::Now,
    };
    Ok(Json(service.reschedule(message_id(&id)?, run_at).await?))
}

/// `DELETE /v1/messages/{id}` cancels a scheduled message.
pub async fn cancel(
    State(service): State<Service>,
    Path(id): Path<String>,
) -> ApiResult<Json<Message>> {
    Ok(Json(service.cancel(message_id(&id)?).await?))
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ListParams {
    queue: Option<String>,
    status: Option<Status>,
    before: Option<MessageId>,
    limit: Option<u32>,
}

/// `GET /v1/messages?queue=&status=&before=&limit=`, newest first. Pass the
/// response's `next` as `before` to get the following page.
pub async fn list(
    State(service): State<Service>,
    Query(params): Query<ListParams>,
) -> ApiResult<Json<Page<Message>>> {
    let filter = MessageFilter {
        queue: params.queue.map(queue_name).transpose()?,
        status: params.status,
        before: params.before,
        limit: params.limit.unwrap_or_default(),
    };
    Ok(Json(service.list_messages(filter).await?))
}

/// `GET /v1/queues/{queue}/messages?status=&before=&limit=`.
pub async fn list_in_queue(
    State(service): State<Service>,
    Path(queue): Path<String>,
    Query(mut params): Query<ListParams>,
) -> ApiResult<Json<Page<Message>>> {
    params.queue = Some(queue);
    list(State(service), Query(params)).await
}
