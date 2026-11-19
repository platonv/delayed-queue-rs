//! The storage trait and its backends.
//!
//! The trait is shaped around what dally needs, not generic CRUD. Each method
//! is atomic: two servers calling `lease` on the same queue at the same time
//! never get the same message. The rules about *what* should happen (defaults,
//! validation, backoff) live in [`Service`](crate::Service); backends only
//! store and fetch.
//!
//! Every backend runs the same conformance tests in `tests/conformance`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::error::{Error, Result};
use crate::message::{
    LeaseToken, Leased, Message, MessageFilter, MessageId, NewMessage, Page, Retry, RunAt,
    Scheduled,
};
use crate::notify::Subscription;
use crate::queue::{Queue, QueueName, QueueSettings, QueueSummary};

#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "redis")]
pub mod redis;
#[cfg(feature = "sqlite")]
pub mod sqlite;

/// Where dally keeps queues and messages.
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    /// Returns the queue, creating it with `defaults` if it does not exist.
    async fn ensure_queue(&self, name: &QueueName, defaults: &QueueSettings) -> Result<Queue>;

    /// Creates the queue or replaces its settings.
    async fn put_queue(&self, name: &QueueName, settings: &QueueSettings) -> Result<Queue>;

    /// Returns the queue, if it exists.
    async fn get_queue(&self, name: &QueueName) -> Result<Option<QueueSummary>>;

    /// Returns every queue, sorted by name.
    async fn list_queues(&self) -> Result<Vec<QueueSummary>>;

    /// Stores messages in an existing queue. A message whose idempotency key
    /// is already used in the queue is not stored; the existing message is
    /// returned with `created: false` instead. Results are in input order.
    async fn schedule(
        &self,
        queue: &QueueName,
        messages: Vec<NewMessage>,
    ) -> Result<Vec<Scheduled>>;

    /// Leases up to `limit` due messages, highest priority first, then oldest
    /// `run_at` first. Each leased message moves to `running`, its attempt
    /// count goes up by one, and it gets a fresh lease token.
    ///
    /// Running messages whose lease expired count as due, so a crashed
    /// worker's messages are picked up again. If an expired message has no
    /// attempts left, it moves to `dead` instead of being leased.
    async fn lease(&self, queue: &QueueName, limit: u32) -> Result<Vec<Leased>>;

    /// Marks a running message as succeeded. Returns `None` if the message
    /// does not exist or is not running under this lease.
    async fn ack(&self, id: MessageId, lease: &LeaseToken) -> Result<Option<Message>>;

    /// Records a failed attempt. The message goes back to `scheduled` after
    /// `retry.delay`, or to `dead` if it has no attempts left. Returns `None`
    /// if the message does not exist or is not running under this lease.
    async fn nack(
        &self,
        id: MessageId,
        lease: &LeaseToken,
        retry: Retry,
    ) -> Result<Option<Message>>;

    /// Returns the message, if it exists.
    async fn get_message(&self, id: MessageId) -> Result<Option<Message>>;

    /// Lists messages newest first.
    async fn list_messages(&self, filter: &MessageFilter) -> Result<Page<Message>>;

    /// Cancels a scheduled message. Returns `None` if the message does not
    /// exist or is not scheduled.
    async fn cancel(&self, id: MessageId) -> Result<Option<Message>>;

    /// Changes when a scheduled message runs. Returns `None` if the message
    /// does not exist or is not scheduled.
    async fn reschedule(&self, id: MessageId, run_at: RunAt) -> Result<Option<Message>>;

    /// Moves dead messages back to `scheduled` with a fresh set of attempts,
    /// due now. With `ids`, only those messages; otherwise every dead message
    /// in the queue. Returns how many were moved.
    async fn retry_dead(&self, queue: &QueueName, ids: Option<&[MessageId]>) -> Result<u64>;

    /// Deletes finished messages older than their queue's retention. Returns
    /// how many were deleted.
    async fn prune(&self) -> Result<u64>;

    /// For each queue with due messages, how long the oldest of them has been
    /// waiting: the storage clock's now minus the earliest `run_at` among due
    /// scheduled messages. Queues with nothing due are left out. Used for the
    /// `dally_queue_oldest_due_seconds` metric.
    ///
    /// Returns `None` when the backend cannot answer this cheaply, which is
    /// the default.
    async fn oldest_due(&self) -> Result<Option<BTreeMap<QueueName, Duration>>> {
        Ok(None)
    }

    /// Starts listening for newly scheduled messages in `queue`.
    fn subscribe(&self, queue: &QueueName) -> Subscription;

    /// The current time according to the storage clock.
    async fn now(&self) -> Result<DateTime<Utc>>;
}

/// Connection options shared by all backends.
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    /// Run pending schema migrations after connecting.
    pub migrate: bool,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self { migrate: true }
    }
}

/// Connects to the backend named by the URL scheme: `postgres://`, `sqlite://`
/// or `redis://`. A backend is only available if its cargo feature is enabled.
pub async fn connect(url: &str, options: &ConnectOptions) -> Result<Arc<dyn Storage>> {
    // Split at the first colon rather than at "://": SQLite's in-memory URL
    // is `sqlite::memory:`.
    let scheme = url
        .split_once(':')
        .map(|(scheme, _)| scheme)
        .unwrap_or_default();

    match scheme {
        "postgres" | "postgresql" => {
            #[cfg(feature = "postgres")]
            return Ok(Arc::new(
                postgres::PostgresStorage::connect(url, options).await?,
            ));
            #[cfg(not(feature = "postgres"))]
            return Err(missing_feature("postgres"));
        }
        "sqlite" => {
            #[cfg(feature = "sqlite")]
            return Ok(Arc::new(
                sqlite::SqliteStorage::connect(url, options).await?,
            ));
            #[cfg(not(feature = "sqlite"))]
            return Err(missing_feature("sqlite"));
        }
        "redis" | "rediss" => {
            #[cfg(feature = "redis")]
            return Ok(Arc::new(redis::RedisStorage::connect(url, options).await?));
            #[cfg(not(feature = "redis"))]
            return Err(missing_feature("redis"));
        }
        // Only the scheme goes in the message: the URL may contain a password.
        _ => Err(Error::invalid(format!(
            "unsupported storage URL scheme {scheme:?}: expected postgres://, sqlite:// or redis://"
        ))),
    }
}

#[allow(dead_code)] // Unused when every backend is compiled in.
fn missing_feature(backend: &str) -> Error {
    Error::invalid(format!(
        "dally was built without {backend} support; enable the `{backend}` cargo feature"
    ))
}
