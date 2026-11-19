//! The rules of dally, independent of any transport.
//!
//! HTTP handlers, the push dispatcher and the cron ticker all go through
//! [`Service`]. It validates input, fills in queue defaults, computes backoff
//! and turns "nothing matched" from storage into a precise error.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use crate::error::{Error, Result};
use crate::message::{
    LeaseToken, Leased, Message, MessageFilter, MessageId, NackRequest, NewMessage, Page, Retry,
    RunAt, ScheduleRequest, Scheduled, Status,
};
use crate::queue::{Queue, QueueName, QueueSettings, QueueSummary};
use crate::storage::Storage;

/// Limits on what a single request can ask for.
pub mod limits {
    use std::time::Duration;

    /// The most messages one schedule request can create.
    pub const MAX_BATCH: usize = 100;
    /// The most messages one poll can lease.
    pub const MAX_POLL: u32 = 100;
    /// The longest a poll can wait for messages.
    pub const MAX_POLL_WAIT: Duration = Duration::from_secs(30);
    /// The most messages one listing can return.
    pub const MAX_PAGE: u32 = 500;
}

/// How often a waiting poll checks storage even without a notification.
/// Notifications only fire for new messages, so this is what picks up delayed
/// messages becoming due and expired leases.
const POLL_RECHECK_INTERVAL: Duration = Duration::from_secs(1);

/// What a worker asks for when it polls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PollRequest {
    /// Lease at most this many messages.
    pub limit: u32,
    /// If nothing is due, wait up to this long for something to become due.
    pub wait: Duration,
}

impl Default for PollRequest {
    fn default() -> Self {
        Self {
            limit: 1,
            wait: Duration::ZERO,
        }
    }
}

/// Hears about every message that moves through a [`Service`], whichever
/// transport or background task moved it. The server uses this for metrics.
///
/// Every method has an empty default, so an observer only implements what it
/// cares about. Methods are called after the storage operation succeeded and
/// must not block.
pub trait Observer: Send + Sync + 'static {
    /// `created` new messages were stored in `queue`. Messages that matched an
    /// existing idempotency key are not counted.
    fn scheduled(&self, queue: &QueueName, created: usize) {
        let _ = (queue, created);
    }

    /// `count` messages were leased from `queue`.
    fn leased(&self, queue: &QueueName, count: usize) {
        let _ = (queue, count);
    }

    /// A message was acked.
    fn acked(&self, message: &Message) {
        let _ = message;
    }

    /// A message was nacked. Its status says whether it will be retried
    /// (`scheduled`) or is out of attempts (`dead`).
    fn nacked(&self, message: &Message) {
        let _ = message;
    }
}

/// The observer used when none is set.
struct NoObserver;

impl Observer for NoObserver {}

/// The entry point to everything dally does.
#[derive(Clone)]
pub struct Service {
    storage: Arc<dyn Storage>,
    defaults: QueueSettings,
    observer: Arc<dyn Observer>,
}

impl Service {
    /// Creates a service. `defaults` are the settings for queues created
    /// implicitly, by scheduling into or polling from an unknown queue.
    pub fn new(storage: Arc<dyn Storage>, defaults: QueueSettings) -> Self {
        Self {
            storage,
            defaults,
            observer: Arc::new(NoObserver),
        }
    }

    /// Reports every scheduled, leased, acked and nacked message to `observer`.
    pub fn with_observer(mut self, observer: Arc<dyn Observer>) -> Self {
        self.observer = observer;
        self
    }

    /// The storage this service uses.
    pub fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }

    /// Schedules messages, creating the queue if needed. The results are in
    /// the same order as the requests.
    pub async fn schedule(
        &self,
        queue: &QueueName,
        requests: Vec<ScheduleRequest>,
    ) -> Result<Vec<Scheduled>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        if requests.len() > limits::MAX_BATCH {
            return Err(Error::invalid(format!(
                "at most {} messages can be scheduled at once",
                limits::MAX_BATCH
            )));
        }
        for request in &requests {
            request.validate()?;
        }

        let settings = self
            .storage
            .ensure_queue(queue, &self.defaults)
            .await?
            .settings;
        let messages = requests
            .into_iter()
            .map(|request| NewMessage {
                id: MessageId::generate(),
                run_at: request.run_at().unwrap_or_default(),
                max_attempts: request.max_attempts.unwrap_or(settings.max_attempts),
                visibility_timeout: request
                    .visibility_timeout
                    .unwrap_or(settings.visibility_timeout),
                priority: request.priority,
                idempotency_key: request.idempotency_key,
                payload: request.payload,
            })
            .collect();

        let scheduled = self.storage.schedule(queue, messages).await?;
        let created = scheduled.iter().filter(|result| result.created).count();
        self.observer.scheduled(queue, created);
        Ok(scheduled)
    }

    /// Leases due messages. If none are due and `request.wait` is non-zero,
    /// waits until some are or the wait runs out. Polling an unknown queue
    /// creates it, so workers can start before anything is scheduled.
    pub async fn poll(&self, queue: &QueueName, request: PollRequest) -> Result<Vec<Leased>> {
        if request.limit == 0 || request.limit > limits::MAX_POLL {
            return Err(Error::invalid(format!(
                "limit must be between 1 and {}",
                limits::MAX_POLL
            )));
        }
        let deadline = Instant::now() + request.wait.min(limits::MAX_POLL_WAIT);
        self.storage.ensure_queue(queue, &self.defaults).await?;

        loop {
            // Subscribe before looking, so a message scheduled between the
            // lease and the wait still wakes us.
            let mut subscription = self.storage.subscribe(queue);

            if !self.is_paused(queue).await? {
                let leased = self.storage.lease(queue, request.limit).await?;
                if !leased.is_empty() {
                    self.observer.leased(queue, leased.len());
                    return Ok(leased);
                }
            }

            let now = Instant::now();
            if now >= deadline {
                return Ok(Vec::new());
            }
            let recheck_at = deadline.min(now + POLL_RECHECK_INTERVAL);
            let _ = tokio::time::timeout_at(recheck_at, subscription.changed()).await;
        }
    }

    /// Marks a message as succeeded.
    pub async fn ack(&self, id: MessageId, lease: &LeaseToken) -> Result<Message> {
        match self.storage.ack(id, lease).await? {
            Some(message) => {
                self.observer.acked(&message);
                Ok(message)
            }
            None => Err(self.lease_error(id).await),
        }
    }

    /// Records a failed attempt. The message is retried after the queue's
    /// backoff (or `retry_in`, if given), or marked dead if it is out of
    /// attempts.
    pub async fn nack(&self, id: MessageId, request: NackRequest) -> Result<Message> {
        // The backoff depends on the attempt number, so look the message up
        // first. The lease check in `storage.nack` keeps this race-free: if
        // the message changes in between, the nack matches nothing.
        let Some(message) = self.storage.get_message(id).await? else {
            return Err(Error::MessageNotFound(id));
        };
        let delay = match request.retry_in {
            Some(delay) => delay,
            None => self
                .settings_of(&message.queue)
                .await?
                .backoff
                .delay(message.attempts),
        };
        let retry = Retry {
            error: request.error,
            delay,
        };

        match self.storage.nack(id, &request.lease, retry).await? {
            Some(message) => {
                self.observer.nacked(&message);
                Ok(message)
            }
            None => Err(self.lease_error(id).await),
        }
    }

    /// Returns a message.
    pub async fn get_message(&self, id: MessageId) -> Result<Message> {
        self.storage
            .get_message(id)
            .await?
            .ok_or(Error::MessageNotFound(id))
    }

    /// Lists messages newest first. A limit of 0 means the default page size.
    pub async fn list_messages(&self, mut filter: MessageFilter) -> Result<Page<Message>> {
        filter.limit = match filter.limit {
            0 => 50,
            limit => limit.min(limits::MAX_PAGE),
        };
        self.storage.list_messages(&filter).await
    }

    /// Cancels a scheduled message.
    pub async fn cancel(&self, id: MessageId) -> Result<Message> {
        match self.storage.cancel(id).await? {
            Some(message) => Ok(message),
            None => Err(self.status_error(id, Status::Scheduled).await),
        }
    }

    /// Changes when a scheduled message runs.
    pub async fn reschedule(&self, id: MessageId, run_at: RunAt) -> Result<Message> {
        match self.storage.reschedule(id, run_at).await? {
            Some(message) => Ok(message),
            None => Err(self.status_error(id, Status::Scheduled).await),
        }
    }

    /// Gives dead messages a fresh set of attempts. With `ids`, only those
    /// messages; otherwise every dead message in the queue.
    pub async fn retry_dead(&self, queue: &QueueName, ids: Option<&[MessageId]>) -> Result<u64> {
        self.get_queue(queue).await?;
        self.storage.retry_dead(queue, ids).await
    }

    /// Returns a queue with its message counts.
    pub async fn get_queue(&self, name: &QueueName) -> Result<QueueSummary> {
        self.storage
            .get_queue(name)
            .await?
            .ok_or_else(|| Error::QueueNotFound(name.clone()))
    }

    /// Returns every queue with its message counts.
    pub async fn list_queues(&self) -> Result<Vec<QueueSummary>> {
        self.storage.list_queues().await
    }

    /// Creates a queue or replaces its settings. Messages already scheduled
    /// keep the `max_attempts` and `visibility_timeout` they were created with.
    pub async fn put_queue(&self, name: &QueueName, settings: QueueSettings) -> Result<Queue> {
        settings.validate()?;
        self.storage.put_queue(name, &settings).await
    }

    /// Pauses or resumes delivery from a queue.
    pub async fn set_paused(&self, name: &QueueName, paused: bool) -> Result<Queue> {
        let mut settings = self.get_queue(name).await?.queue.settings;
        settings.paused = paused;
        self.storage.put_queue(name, &settings).await
    }

    /// Deletes finished messages past their queue's retention.
    pub async fn prune(&self) -> Result<u64> {
        self.storage.prune().await
    }

    async fn settings_of(&self, queue: &QueueName) -> Result<QueueSettings> {
        Ok(self
            .storage
            .ensure_queue(queue, &self.defaults)
            .await?
            .settings)
    }

    async fn is_paused(&self, queue: &QueueName) -> Result<bool> {
        Ok(self.settings_of(queue).await?.paused)
    }

    /// Explains why an ack or nack matched nothing.
    async fn lease_error(&self, id: MessageId) -> Error {
        match self.storage.get_message(id).await {
            Ok(Some(_)) => Error::LeaseMismatch(id),
            Ok(None) => Error::MessageNotFound(id),
            Err(err) => err,
        }
    }

    /// Explains why an operation that needs a certain status matched nothing.
    async fn status_error(&self, id: MessageId, expected: Status) -> Error {
        match self.storage.get_message(id).await {
            Ok(Some(message)) => Error::InvalidStatus {
                id,
                status: message.status,
                expected,
            },
            Ok(None) => Error::MessageNotFound(id),
            Err(err) => err,
        }
    }
}
