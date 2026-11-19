//! Queues and their settings.

use std::fmt;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// The name of a queue: 1 to 100 ASCII letters, digits, `-`, `_` or `.`.
///
/// The character set is deliberately small so names are safe in URLs, Redis
/// keys and metric labels without escaping.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[cfg_attr(
    feature = "openapi",
    schema(value_type = String, pattern = "^[A-Za-z0-9._-]{1,100}$", example = "emails")
)]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct QueueName(String);

impl QueueName {
    /// The longest allowed name, in bytes.
    pub const MAX_LEN: usize = 100;

    /// Validates and wraps a queue name.
    pub fn new(name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        let valid_chars = name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'));

        if name.is_empty() || name.len() > Self::MAX_LEN || !valid_chars {
            return Err(Error::invalid(format!(
                "invalid queue name {name:?}: use 1 to {} letters, digits, '-', '_' or '.'",
                Self::MAX_LEN
            )));
        }
        Ok(Self(name))
    }

    /// The name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for QueueName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for QueueName {
    type Error = Error;

    fn try_from(name: String) -> Result<Self> {
        Self::new(name)
    }
}

impl TryFrom<&str> for QueueName {
    type Error = Error;

    fn try_from(name: &str) -> Result<Self> {
        Self::new(name)
    }
}

impl From<QueueName> for String {
    fn from(name: QueueName) -> Self {
        name.0
    }
}

/// A queue as stored, with its settings.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Queue {
    /// The queue's name.
    pub name: QueueName,
    /// How messages in this queue are delivered.
    #[serde(flatten)]
    pub settings: QueueSettings,
    /// When the queue was created.
    pub created_at: DateTime<Utc>,
    /// When the settings last changed.
    pub updated_at: DateTime<Utc>,
}

/// The settings of a queue. Every field has a default, so a queue created
/// implicitly by scheduling a message works without configuration.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueueSettings {
    /// How long a lease lasts. A message that is not acked or nacked in this
    /// time is handed to another worker.
    #[serde(with = "humantime_serde")]
    #[cfg_attr(feature = "openapi", schema(value_type = String, example = "30s"))]
    pub visibility_timeout: Duration,

    /// How many times a message is attempted before it is marked dead.
    pub max_attempts: u32,

    /// How long to wait before retrying a failed attempt.
    pub backoff: Backoff,

    /// How long finished messages are kept before they are deleted. This is
    /// also how long idempotency keys are remembered.
    #[serde(with = "humantime_serde")]
    #[cfg_attr(feature = "openapi", schema(value_type = String, example = "7days"))]
    pub retention: Duration,

    /// A paused queue accepts new messages but delivers none.
    pub paused: bool,

    /// When set, the server delivers messages by POSTing them to this endpoint
    /// instead of waiting for workers to poll.
    pub push: Option<PushSettings>,
}

impl Default for QueueSettings {
    fn default() -> Self {
        Self {
            visibility_timeout: Duration::from_secs(30),
            max_attempts: 5,
            backoff: Backoff::default(),
            retention: Duration::from_secs(7 * 24 * 60 * 60),
            paused: false,
            push: None,
        }
    }
}

impl QueueSettings {
    /// Checks that the settings make sense together.
    pub fn validate(&self) -> Result<()> {
        if self.visibility_timeout < Duration::from_secs(1) {
            return Err(Error::invalid("visibility_timeout must be at least 1s"));
        }
        if self.max_attempts == 0 {
            return Err(Error::invalid("max_attempts must be at least 1"));
        }
        if self.backoff.base > self.backoff.cap {
            return Err(Error::invalid(
                "backoff.base must not be greater than backoff.cap",
            ));
        }
        if let Some(push) = &self.push {
            push.validate()?;
        }
        Ok(())
    }
}

/// Exponential backoff with jitter: attempt `n` waits about
/// `min(base * 2^(n - 1), cap)`, give or take 20%.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Backoff {
    /// The delay after the first failed attempt.
    #[serde(with = "humantime_serde")]
    #[cfg_attr(feature = "openapi", schema(value_type = String, example = "5s"))]
    pub base: Duration,
    /// The longest delay between attempts.
    #[serde(with = "humantime_serde")]
    #[cfg_attr(feature = "openapi", schema(value_type = String, example = "1h"))]
    pub cap: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(5),
            cap: Duration::from_secs(60 * 60),
        }
    }
}

impl Backoff {
    /// The delay before retrying after `attempt` failed. Attempts count from 1.
    pub fn delay(&self, attempt: u32) -> Duration {
        // 2^30 times any sensible base is far beyond any sensible cap, so
        // clamping the exponent avoids overflow without changing the result.
        let exponent = attempt.saturating_sub(1).min(30);
        let delay = self.base.saturating_mul(1 << exponent).min(self.cap);
        let jitter = 0.8 + fastrand::f64() * 0.4;
        delay.mul_f64(jitter)
    }
}

/// Where and how to push messages from a queue.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushSettings {
    /// The endpoint that receives each message as a POST request.
    pub url: String,

    /// When set, each request is signed with HMAC-SHA256 using this secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,

    /// How long to wait for the endpoint to respond.
    #[serde(default = "PushSettings::default_timeout", with = "humantime_serde")]
    #[cfg_attr(feature = "openapi", schema(value_type = String, example = "30s"))]
    pub timeout: Duration,

    /// How many requests to have in flight at once, per server.
    #[serde(default = "PushSettings::default_concurrency")]
    pub concurrency: u32,
}

impl PushSettings {
    fn default_timeout() -> Duration {
        Duration::from_secs(30)
    }

    fn default_concurrency() -> u32 {
        8
    }

    fn validate(&self) -> Result<()> {
        if !(self.url.starts_with("http://") || self.url.starts_with("https://")) {
            return Err(Error::invalid(
                "push.url must start with http:// or https://",
            ));
        }
        if self.concurrency == 0 {
            return Err(Error::invalid("push.concurrency must be at least 1"));
        }
        Ok(())
    }
}

/// How many messages a queue holds in each status.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueStats {
    /// Waiting to run, now or later.
    pub scheduled: u64,
    /// Leased by a worker.
    pub running: u64,
    /// Finished successfully.
    pub succeeded: u64,
    /// Cancelled before running.
    pub cancelled: u64,
    /// Out of attempts.
    pub dead: u64,
}

/// A queue together with its message counts.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueSummary {
    /// The queue.
    #[serde(flatten)]
    pub queue: Queue,
    /// Its message counts.
    pub stats: QueueStats,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_names() {
        assert!(QueueName::new("emails").is_ok());
        assert!(QueueName::new("billing.invoices-v2_eu").is_ok());
        assert!(QueueName::new("").is_err());
        assert!(QueueName::new("has space").is_err());
        assert!(QueueName::new("slash/name").is_err());
        assert!(QueueName::new("x".repeat(101)).is_err());
    }

    #[test]
    fn backoff_grows_and_caps() {
        let backoff = Backoff {
            base: Duration::from_secs(10),
            cap: Duration::from_secs(60),
        };
        let within = |d: Duration, expected: u64| {
            let expected = Duration::from_secs(expected);
            d >= expected.mul_f64(0.8) && d <= expected.mul_f64(1.2)
        };
        assert!(within(backoff.delay(1), 10));
        assert!(within(backoff.delay(2), 20));
        assert!(within(backoff.delay(3), 40));
        assert!(within(backoff.delay(4), 60));
        assert!(within(backoff.delay(1000), 60));
    }

    #[test]
    fn settings_parse_human_durations() {
        let settings: QueueSettings =
            serde_json::from_str(r#"{"visibility_timeout": "2m", "backoff": {"base": "1s"}}"#)
                .expect("valid settings");
        assert_eq!(settings.visibility_timeout, Duration::from_secs(120));
        assert_eq!(settings.backoff.base, Duration::from_secs(1));
        assert_eq!(settings.backoff.cap, Backoff::default().cap);
        assert_eq!(settings.max_attempts, 5);
    }
}
