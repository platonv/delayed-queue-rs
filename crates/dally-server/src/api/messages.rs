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
use utoipa::{IntoParams, ToSchema};

use super::openapi::{ErrorBody, ScheduleBody, ScheduleResponse};
use super::{ApiError, ApiResult, Json, Query, message_id, optional_json, queue_name};

/// `POST /v1/queues/{queue}/messages` takes one message or an array of them.
///
/// One message: `201 Created` with the message, or `200 OK` with the existing
/// message if its idempotency key was already used.
///
/// An array: `200 OK` with `{"messages": [{"created": bool, "message": ...}]}`
/// in the same order.
#[utoipa::path(
    post, path = "/v1/queues/{queue}/messages", tag = "messages",
    params(("queue" = String, Path, description = "Queue name")),
    request_body = ScheduleBody,
    responses(
        (status = 201, description = "Scheduled", body = Message),
        (status = 200, description = "The existing message, if its idempotency key was already used; or the results of a batch", body = ScheduleResponse),
        (status = 400, description = "Invalid input", body = ErrorBody), (status = 401, description = "Missing or unknown API key", body = ErrorBody), (status = 503, description = "Storage is unavailable", body = ErrorBody)
    )
)]
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

#[derive(Debug, Serialize, ToSchema)]
pub struct Batch {
    messages: Vec<Scheduled>,
}

#[derive(Debug, Default, Deserialize, ToSchema)]
#[serde(default, deny_unknown_fields)]
pub struct PollBody {
    /// Lease at most this many messages, 1 to 100. Defaults to 1.
    limit: Option<u32>,
    /// If nothing is due, wait up to this long (at most 30s) for a message.
    #[serde(with = "humantime_serde")]
    #[schema(value_type = Option<String>, example = "20s")]
    wait: Option<Duration>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Polled {
    messages: Vec<Leased>,
}

/// `POST /v1/queues/{queue}/poll` with an optional body
/// `{"limit": 10, "wait": "20s"}`.
#[utoipa::path(
    post, path = "/v1/queues/{queue}/poll", tag = "messages",
    params(("queue" = String, Path, description = "Queue name")),
    request_body(content = Option<PollBody>, description = "Optional; defaults to one message and no waiting"),
    responses(
        (status = 200, description = "Leased messages, possibly none", body = Polled),
        (status = 400, description = "Invalid input", body = ErrorBody), (status = 401, description = "Missing or unknown API key", body = ErrorBody), (status = 503, description = "Storage is unavailable", body = ErrorBody)
    )
)]
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

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AckBody {
    /// The lease token from the poll.
    lease: LeaseToken,
}

/// `POST /v1/messages/{id}/ack` with `{"lease": "..."}`.
#[utoipa::path(
    post, path = "/v1/messages/{id}/ack", tag = "messages",
    params(("id" = uuid::Uuid, Path, description = "Message id")),
    request_body = AckBody,
    responses(
        (status = 200, description = "Succeeded", body = Message),
        (status = 400, description = "Invalid input", body = ErrorBody), (status = 401, description = "Missing or unknown API key", body = ErrorBody), (status = 404, description = "Not found", body = ErrorBody), (status = 409, description = "The lease or status does not allow this", body = ErrorBody), (status = 503, description = "Storage is unavailable", body = ErrorBody)
    )
)]
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
#[utoipa::path(
    post, path = "/v1/messages/{id}/nack", tag = "messages",
    params(("id" = uuid::Uuid, Path, description = "Message id")),
    request_body = NackRequest,
    responses(
        (status = 200, description = "Scheduled for a retry, or dead", body = Message),
        (status = 400, description = "Invalid input", body = ErrorBody), (status = 401, description = "Missing or unknown API key", body = ErrorBody), (status = 404, description = "Not found", body = ErrorBody), (status = 409, description = "The lease or status does not allow this", body = ErrorBody), (status = 503, description = "Storage is unavailable", body = ErrorBody)
    )
)]
pub async fn nack(
    State(service): State<Service>,
    Path(id): Path<String>,
    Json(body): Json<NackRequest>,
) -> ApiResult<Json<Message>> {
    let message = service.nack(message_id(&id)?, body).await?;
    Ok(Json(message))
}

/// `GET /v1/messages/{id}`.
#[utoipa::path(
    get, path = "/v1/messages/{id}", tag = "messages",
    params(("id" = uuid::Uuid, Path, description = "Message id")),
    responses(
        (status = 200, description = "The message", body = Message),
        (status = 400, description = "Invalid input", body = ErrorBody), (status = 401, description = "Missing or unknown API key", body = ErrorBody), (status = 404, description = "Not found", body = ErrorBody), (status = 503, description = "Storage is unavailable", body = ErrorBody)
    )
)]
pub async fn get(
    State(service): State<Service>,
    Path(id): Path<String>,
) -> ApiResult<Json<Message>> {
    Ok(Json(service.get_message(message_id(&id)?).await?))
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RescheduleBody {
    /// Run after this delay. Cannot be combined with `run_at`.
    #[serde(default, with = "humantime_serde")]
    #[schema(value_type = Option<String>, example = "1h")]
    delay: Option<Duration>,
    /// Run at this time. With neither field, the message runs now.
    #[serde(default)]
    run_at: Option<DateTime<Utc>>,
}

/// `PATCH /v1/messages/{id}` with `{"delay": "1h"}` or `{"run_at": "..."}`.
/// An empty object runs it now.
#[utoipa::path(
    patch, path = "/v1/messages/{id}", tag = "messages",
    params(("id" = uuid::Uuid, Path, description = "Message id")),
    request_body = RescheduleBody,
    responses(
        (status = 200, description = "Rescheduled", body = Message),
        (status = 400, description = "Invalid input", body = ErrorBody), (status = 401, description = "Missing or unknown API key", body = ErrorBody), (status = 404, description = "Not found", body = ErrorBody), (status = 409, description = "The lease or status does not allow this", body = ErrorBody), (status = 503, description = "Storage is unavailable", body = ErrorBody)
    )
)]
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
#[utoipa::path(
    delete, path = "/v1/messages/{id}", tag = "messages",
    params(("id" = uuid::Uuid, Path, description = "Message id")),
    responses(
        (status = 200, description = "Cancelled", body = Message),
        (status = 400, description = "Invalid input", body = ErrorBody), (status = 401, description = "Missing or unknown API key", body = ErrorBody), (status = 404, description = "Not found", body = ErrorBody), (status = 409, description = "The lease or status does not allow this", body = ErrorBody), (status = 503, description = "Storage is unavailable", body = ErrorBody)
    )
)]
pub async fn cancel(
    State(service): State<Service>,
    Path(id): Path<String>,
) -> ApiResult<Json<Message>> {
    Ok(Json(service.cancel(message_id(&id)?).await?))
}

#[derive(Debug, Default, Deserialize, IntoParams)]
#[serde(default, deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct ListParams {
    /// Only messages in this queue.
    queue: Option<String>,
    /// Only messages in this status.
    status: Option<Status>,
    /// Only messages older than this one: pass the previous page's `next`.
    #[param(value_type = Option<uuid::Uuid>)]
    before: Option<MessageId>,
    /// At most this many messages, up to 500. Defaults to 50.
    limit: Option<u32>,
}

/// The query parameters of the per-queue listing, which takes the queue
/// from the path. Only used to describe the endpoint.
#[derive(IntoParams)]
#[into_params(parameter_in = Query)]
#[allow(dead_code)]
pub struct QueueListParams {
    /// Only messages in this status.
    status: Option<Status>,
    /// Only messages older than this one: pass the previous page's `next`.
    #[param(value_type = Option<uuid::Uuid>)]
    before: Option<MessageId>,
    /// At most this many messages, up to 500. Defaults to 50.
    limit: Option<u32>,
}

/// `GET /v1/messages?queue=&status=&before=&limit=`, newest first. Pass the
/// response's `next` as `before` to get the following page.
#[utoipa::path(
    get, path = "/v1/messages", tag = "messages",
    params(ListParams),
    responses(
        (status = 200, description = "One page, newest first", body = Page<Message>),
        (status = 400, description = "Invalid input", body = ErrorBody), (status = 401, description = "Missing or unknown API key", body = ErrorBody), (status = 503, description = "Storage is unavailable", body = ErrorBody)
    )
)]
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
#[utoipa::path(
    get, path = "/v1/queues/{queue}/messages", tag = "messages",
    params(("queue" = String, Path, description = "Queue name"), QueueListParams),
    responses(
        (status = 200, description = "One page, newest first", body = Page<Message>),
        (status = 400, description = "Invalid input", body = ErrorBody), (status = 401, description = "Missing or unknown API key", body = ErrorBody), (status = 503, description = "Storage is unavailable", body = ErrorBody)
    )
)]
pub async fn list_in_queue(
    State(service): State<Service>,
    Path(queue): Path<String>,
    Query(mut params): Query<ListParams>,
) -> ApiResult<Json<Page<Message>>> {
    params.queue = Some(queue);
    list(State(service), Query(params)).await
}
