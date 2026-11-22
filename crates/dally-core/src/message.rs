//! Messages and the requests that create and change them.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::queue::QueueName;

/// A message id. Ids are UUIDv7, so sorting them sorts messages by creation
/// time.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "openapi", schema(value_type = String, format = Uuid))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MessageId(Uuid);

impl MessageId {
    /// A new id for a message created now.
    pub fn generate() -> Self {
        Self(Uuid::now_v7())
    }

    /// Wraps an existing UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// The underlying UUID.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for MessageId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Uuid::parse_str(s)
            .map(Self)
            .map_err(|_| Error::invalid(format!("invalid message id {s:?}")))
    }
}

/// The token that proves a worker holds a message. It changes every time the
/// message is leased, so a worker whose lease expired cannot ack a message
/// that was handed to someone else.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "openapi", schema(value_type = String, example = "0f8e2c4b9d1a4e7f8a3b5c6d7e8f9a0b"))]
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LeaseToken(String);

impl LeaseToken {
    /// A new random token.
    pub fn generate() -> Self {
        Self(Uuid::new_v4().simple().to_string())
    }

    /// The token as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for LeaseToken {
    fn from(token: String) -> Self {
        Self(token)
    }
}

/// Where a message is in its lifecycle. See `docs/design.md` for the diagram.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// Waiting to run, now or later.
    Scheduled,
    /// Leased by a worker.
    Running,
    /// Finished successfully.
    Succeeded,
    /// Cancelled before running.
    Cancelled,
    /// Out of attempts.
    Dead,
}

impl Status {
    /// Every status, in lifecycle order.
    pub const ALL: [Status; 5] = [
        Status::Scheduled,
        Status::Running,
        Status::Succeeded,
        Status::Cancelled,
        Status::Dead,
    ];

    /// The status as it is stored and shown in the API.
    pub fn as_str(&self) -> &'static str {
        match self {
            Status::Scheduled => "scheduled",
            Status::Running => "running",
            Status::Succeeded => "succeeded",
            Status::Cancelled => "cancelled",
            Status::Dead => "dead",
        }
    }

    /// Finished messages never change again, and are pruned after the queue's
    /// retention period. The exception is dead messages, which can be retried.
    pub fn is_finished(&self) -> bool {
        matches!(self, Status::Succeeded | Status::Cancelled | Status::Dead)
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Status {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Status::ALL
            .into_iter()
            .find(|status| status.as_str() == s)
            .ok_or_else(|| Error::invalid(format!("unknown status {s:?}")))
    }
}

/// A message as stored.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    /// The message's id.
    pub id: MessageId,
    /// The queue it belongs to.
    pub queue: QueueName,
    /// What the worker gets.
    pub payload: serde_json::Value,
    /// Where it is in its lifecycle.
    pub status: Status,
    /// Higher priorities are delivered first.
    pub priority: i32,
    /// How many times it has been leased.
    pub attempts: u32,
    /// How many attempts it gets before it is marked dead.
    pub max_attempts: u32,
    /// How long each lease lasts.
    #[serde(with = "humantime_serde")]
    #[cfg_attr(feature = "openapi", schema(value_type = String, example = "30s"))]
    pub visibility_timeout: Duration,
    /// The key that prevents duplicates, if one was given.
    pub idempotency_key: Option<String>,
    /// When it is next due.
    pub run_at: DateTime<Utc>,
    /// When the current lease expires, while it is running.
    pub lease_expires_at: Option<DateTime<Utc>>,
    /// The error reported by the last failed attempt.
    pub last_error: Option<String>,
    /// When it was scheduled.
    pub created_at: DateTime<Utc>,
    /// When it last changed.
    pub updated_at: DateTime<Utc>,
    /// When it reached a finished status.
    pub finished_at: Option<DateTime<Utc>>,
}

/// A message handed to a worker, with the token it needs to ack or nack it.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Leased {
    /// The message.
    #[serde(flatten)]
    pub message: Message,
    /// The proof that this worker holds it.
    pub lease: LeaseToken,
}

/// When a message should run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RunAt {
    /// As soon as possible.
    #[default]
    Now,
    /// After a delay, measured by the storage clock.
    After(Duration),
    /// At a point in time.
    At(DateTime<Utc>),
}

/// A request to schedule a message, as clients send it.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleRequest {
    /// What the worker gets.
    pub payload: serde_json::Value,

    /// Run after this delay. Cannot be combined with `run_at`.
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, example = "10m"))]
    pub delay: Option<Duration>,

    /// Run at this time. Cannot be combined with `delay`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_at: Option<DateTime<Utc>>,

    /// Higher priorities are delivered first. Defaults to 0.
    #[serde(default)]
    pub priority: i32,

    /// Scheduling twice with the same key in the same queue creates only one
    /// message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,

    /// Overrides the queue's `max_attempts` for this message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,

    /// Overrides the queue's `visibility_timeout` for this message.
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, example = "2m"))]
    pub visibility_timeout: Option<Duration>,
}

impl ScheduleRequest {
    /// The longest allowed idempotency key, in bytes.
    pub const MAX_IDEMPOTENCY_KEY_LEN: usize = 200;

    /// A request to run `payload` now with default settings.
    pub fn new(payload: serde_json::Value) -> Self {
        Self {
            payload,
            ..Self::default()
        }
    }

    pub(crate) fn run_at(&self) -> Result<RunAt> {
        match (self.delay, self.run_at) {
            (Some(_), Some(_)) => Err(Error::invalid("use either delay or run_at, not both")),
            (Some(delay), None) => Ok(RunAt::After(delay)),
            (None, Some(at)) => Ok(RunAt::At(at)),
            (None, None) => Ok(RunAt::Now),
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if let Some(key) = &self.idempotency_key
            && (key.is_empty() || key.len() > Self::MAX_IDEMPOTENCY_KEY_LEN)
        {
            return Err(Error::invalid(format!(
                "idempotency_key must be 1 to {} bytes",
                Self::MAX_IDEMPOTENCY_KEY_LEN
            )));
        }
        if self.max_attempts == Some(0) {
            return Err(Error::invalid("max_attempts must be at least 1"));
        }
        if self
            .visibility_timeout
            .is_some_and(|t| t < Duration::from_secs(1))
        {
            return Err(Error::invalid("visibility_timeout must be at least 1s"));
        }
        self.run_at().map(|_| ())
    }
}

/// A message ready to be stored: a [`ScheduleRequest`] with the queue's
/// defaults filled in.
#[derive(Debug, Clone, PartialEq)]
pub struct NewMessage {
    /// The id to store it under.
    pub id: MessageId,
    /// What the worker gets.
    pub payload: serde_json::Value,
    /// When it should run.
    pub run_at: RunAt,
    /// Higher priorities are delivered first.
    pub priority: i32,
    /// The key that prevents duplicates.
    pub idempotency_key: Option<String>,
    /// How many attempts it gets.
    pub max_attempts: u32,
    /// How long each lease lasts.
    pub visibility_timeout: Duration,
}

/// The result of scheduling one message.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Scheduled {
    /// The stored message: the new one, or the existing one with the same
    /// idempotency key.
    pub message: Message,
    /// False when a message with the same idempotency key already existed.
    pub created: bool,
}

/// What a worker reports when an attempt fails.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NackRequest {
    /// The lease that proves the worker holds the message.
    pub lease: LeaseToken,
    /// What went wrong. Shown in the UI and kept on the message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Retry after this delay instead of the queue's backoff.
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, example = "30s"))]
    pub retry_in: Option<Duration>,
}

/// How a storage backend should record a failed attempt.
#[derive(Debug, Clone, PartialEq)]
pub struct Retry {
    /// What went wrong.
    pub error: Option<String>,
    /// How long to wait before the next attempt, if there is one.
    pub delay: Duration,
}

/// Which messages to list.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct MessageFilter {
    /// Only messages in this queue.
    pub queue: Option<QueueName>,
    /// Only messages in this status.
    pub status: Option<Status>,
    /// Only messages created before this one. Pass the previous page's `next`
    /// to get the following page.
    pub before: Option<MessageId>,
    /// At most this many messages.
    pub limit: u32,
}

/// One page of a listing, newest first.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Page<T> {
    /// The items on this page.
    pub items: Vec<T>,
    /// The cursor for the next page, if there is one.
    pub next: Option<MessageId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_round_trips_through_strings() {
        for status in Status::ALL {
            assert_eq!(status.as_str().parse::<Status>().ok(), Some(status));
        }
        assert!("unknown".parse::<Status>().is_err());
    }

    #[test]
    fn schedule_request_rejects_delay_and_run_at_together() {
        let request = ScheduleRequest {
            delay: Some(Duration::from_secs(1)),
            run_at: Some(Utc::now()),
            ..ScheduleRequest::default()
        };
        assert!(request.validate().is_err());
    }

    #[test]
    fn schedule_request_parses_human_delay() {
        let request: ScheduleRequest =
            serde_json::from_str(r#"{"payload": {"to": "ana"}, "delay": "10m"}"#)
                .expect("valid request");
        assert_eq!(
            request.run_at().ok(),
            Some(RunAt::After(Duration::from_secs(600)))
        );
    }
}
