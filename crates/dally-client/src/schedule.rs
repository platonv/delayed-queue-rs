//! The builder returned by [`Client::schedule`].

use std::time::Duration;

use chrono::{DateTime, Utc};
use dally_core::{ScheduleRequest, Scheduled};

use crate::client::Client;
use crate::error::{Error, Result};

/// A message about to be scheduled. Created by [`Client::schedule`]; nothing
/// is sent until [`send`](Self::send).
///
/// Without [`delay`](Self::delay) or [`run_at`](Self::run_at) the message is
/// due right away.
#[derive(Debug)]
#[must_use = "nothing is scheduled until you call .send().await"]
pub struct ScheduleBuilder<'a> {
    client: &'a Client,
    queue: &'a str,
    // Serialization errors are kept until `send` so the builder chain stays
    // free of `?`.
    payload: serde_json::Result<serde_json::Value>,
    request: ScheduleRequest,
}

impl<'a> ScheduleBuilder<'a> {
    pub(crate) fn new(
        client: &'a Client,
        queue: &'a str,
        payload: serde_json::Result<serde_json::Value>,
    ) -> Self {
        Self {
            client,
            queue,
            payload,
            request: ScheduleRequest::default(),
        }
    }

    /// Runs the message after this delay, measured by the server's storage
    /// clock. Replaces an earlier [`run_at`](Self::run_at).
    pub fn delay(mut self, delay: Duration) -> Self {
        self.request.delay = Some(delay);
        self.request.run_at = None;
        self
    }

    /// Runs the message at this time. Replaces an earlier
    /// [`delay`](Self::delay).
    pub fn run_at(mut self, at: DateTime<Utc>) -> Self {
        self.request.run_at = Some(at);
        self.request.delay = None;
        self
    }

    /// Higher priorities are delivered first. Defaults to 0.
    pub fn priority(mut self, priority: i32) -> Self {
        self.request.priority = priority;
        self
    }

    /// Scheduling again with the same key in the same queue returns the
    /// existing message instead of creating a new one, with
    /// [`Scheduled::created`] set to false. Keys are remembered until the
    /// message is pruned after the queue's retention period.
    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.request.idempotency_key = Some(key.into());
        self
    }

    /// Overrides the queue's `max_attempts` for this message.
    pub fn max_attempts(mut self, attempts: u32) -> Self {
        self.request.max_attempts = Some(attempts);
        self
    }

    /// Overrides the queue's `visibility_timeout` for this message: how long a
    /// worker has to finish an attempt before the message is handed out again.
    pub fn visibility_timeout(mut self, timeout: Duration) -> Self {
        self.request.visibility_timeout = Some(timeout);
        self
    }

    /// Schedules the message.
    pub async fn send(self) -> Result<Scheduled> {
        let request = ScheduleRequest {
            payload: self.payload.map_err(Error::Json)?,
            ..self.request
        };
        self.client.schedule_request(self.queue, &request).await
    }
}
