//! Running a handler for every message in a queue.

use std::any::Any;
use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use dally_core::service::limits::MAX_POLL;
use dally_core::{LeaseToken, Leased, Message, MessageId, NackRequest};
use serde::de::DeserializeOwned;
use tokio::task::JoinSet;
use tracing::Instrument;

use crate::client::Client;
use crate::error::Result;

/// One message, handed to a [`Worker`]'s handler with its payload already
/// deserialized.
#[derive(Debug, Clone)]
pub struct Job<T> {
    /// The message's payload.
    pub payload: T,
    message: Message,
}

impl<T> Job<T> {
    /// The message's id. Since delivery is at least once, this is a good key
    /// for remembering that a job is already done.
    pub fn id(&self) -> MessageId {
        self.message.id
    }

    /// Which attempt this is, counting from 1.
    pub fn attempt(&self) -> u32 {
        self.message.attempts
    }

    /// How many attempts the message gets before it is marked dead.
    pub fn max_attempts(&self) -> u32 {
        self.message.max_attempts
    }

    /// True if a failure now marks the message dead instead of retrying it.
    pub fn is_last_attempt(&self) -> bool {
        self.attempt() >= self.max_attempts()
    }

    /// The full message, including its queue, priority and the error from the
    /// previous attempt.
    pub fn message(&self) -> &Message {
        &self.message
    }
}

/// Polls a queue and runs a handler for each message, with bounded
/// concurrency.
///
/// For each message the worker:
///
/// - deserializes the payload into the handler's job type, and nacks the
///   message with the serde error if that fails;
/// - runs the handler in its own task, so a panic only fails that message,
///   which is nacked with `handler panicked: ...`;
/// - acks the message when the handler returns `Ok`, and nacks it with the
///   error's `Display` text when it returns `Err`. The server then retries it
///   with the queue's backoff, or marks it dead after the last attempt.
///
/// ```no_run
/// use dally_client::{Client, Job, Worker};
///
/// #[derive(serde::Deserialize)]
/// struct Welcome {
///     user_id: u64,
/// }
///
/// # async fn send_welcome(_: u64) -> Result<(), std::io::Error> { Ok(()) }
/// # async fn example() -> dally_client::Result<()> {
/// let client = Client::new("http://127.0.0.1:7070")?;
/// Worker::new(client, "emails")
///     .concurrency(8)
///     .run(|job: Job<Welcome>| async move { send_welcome(job.payload.user_id).await })
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// # Long jobs
///
/// The worker does not extend leases. A job that runs longer than its
/// message's visibility timeout (30 seconds by default) is handed to another
/// worker while the first one is still busy, and the first worker's ack then
/// fails. Give queues with long jobs a `visibility_timeout` comfortably above
/// the longest run, either in the queue settings or per message with
/// [`ScheduleBuilder::visibility_timeout`](crate::ScheduleBuilder::visibility_timeout).
///
/// # Errors from the server
///
/// When the server cannot be reached or reports a temporary problem, the
/// worker logs a warning and tries again after a delay that doubles from 1 to
/// 30 seconds. Other errors, such as an invalid queue name or a rejected API
/// key, will not fix themselves: the worker stops polling, waits for running
/// jobs, and returns the error.
#[derive(Debug, Clone)]
pub struct Worker {
    client: Client,
    queue: String,
    concurrency: usize,
    poll_wait: Duration,
}

impl Worker {
    /// A worker for `queue` that runs one job at a time.
    pub fn new(client: Client, queue: impl Into<String>) -> Self {
        Self {
            client,
            queue: queue.into(),
            concurrency: 1,
            poll_wait: Duration::from_secs(20),
        }
    }

    /// How many jobs may run at once. Defaults to 1; 0 is treated as 1.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// How long each poll waits for a message when the queue is empty.
    /// Defaults to 20 seconds; the server caps it at 30. Shorter waits mean
    /// more requests to an idle server, not faster delivery: a waiting poll
    /// returns as soon as a message is due.
    pub fn poll_wait(mut self, wait: Duration) -> Self {
        self.poll_wait = wait;
        self
    }

    /// Runs `handler` for every message until a non-retryable error occurs.
    /// Use [`run_until`](Self::run_until) to stop it cleanly.
    pub async fn run<T, F, Fut, E>(self, handler: F) -> Result<()>
    where
        T: DeserializeOwned + Send + 'static,
        F: Fn(Job<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Display + 'static,
    {
        self.run_until(std::future::pending(), handler).await
    }

    /// Runs `handler` for every message until `shutdown` resolves, then stops
    /// polling and waits for running jobs to finish before returning.
    ///
    /// A poll still waiting when `shutdown` resolves is abandoned. If the
    /// server leased messages to it at that moment, they are delivered again
    /// once their visibility timeout runs out.
    ///
    /// ```no_run
    /// # use dally_client::{Client, Job, Worker};
    /// # async fn example(client: Client) -> dally_client::Result<()> {
    /// Worker::new(client, "emails")
    ///     .run_until(
    ///         async {
    ///             let _ = tokio::signal::ctrl_c().await;
    ///         },
    ///         |job: Job<serde_json::Value>| async move {
    ///             println!("got {}", job.payload);
    ///             Ok::<_, std::io::Error>(())
    ///         },
    ///     )
    ///     .await
    /// # }
    /// ```
    pub async fn run_until<T, F, Fut, E, S>(self, shutdown: S, handler: F) -> Result<()>
    where
        T: DeserializeOwned + Send + 'static,
        F: Fn(Job<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Display + 'static,
        S: Future<Output = ()>,
    {
        let handler = Arc::new(handler);
        let mut in_flight = JoinSet::new();
        let mut backoff = RetryDelay::default();
        let mut shutdown = std::pin::pin!(shutdown);

        let result = loop {
            while let Some(finished) = in_flight.try_join_next() {
                log_supervisor_failure(finished);
            }

            let free = self.concurrency.saturating_sub(in_flight.len());
            if free == 0 {
                tokio::select! {
                    () = &mut shutdown => break Ok(()),
                    Some(finished) = in_flight.join_next() => log_supervisor_failure(finished),
                }
                continue;
            }

            let limit = u32::try_from(free).map_or(MAX_POLL, |free| free.min(MAX_POLL));
            let polled = tokio::select! {
                () = &mut shutdown => break Ok(()),
                polled = self.client.poll(&self.queue, limit, self.poll_wait) => polled,
            };

            match polled {
                Ok(messages) => {
                    backoff.reset();
                    for leased in messages {
                        in_flight.spawn(process(self.client.clone(), handler.clone(), leased));
                    }
                }
                Err(err) if err.is_retryable() => {
                    let delay = backoff.next();
                    tracing::warn!(
                        queue = %self.queue,
                        error = %err,
                        retry_in = ?delay,
                        "polling failed, retrying"
                    );
                    tokio::select! {
                        () = &mut shutdown => break Ok(()),
                        () = tokio::time::sleep(delay) => {}
                    }
                }
                Err(err) => break Err(err),
            }
        };

        if !in_flight.is_empty() {
            tracing::info!(
                queue = %self.queue,
                jobs = in_flight.len(),
                "waiting for running jobs"
            );
        }
        while let Some(finished) = in_flight.join_next().await {
            log_supervisor_failure(finished);
        }
        result
    }
}

/// Runs one message through the handler and reports the outcome.
async fn process<T, F, Fut, E>(client: Client, handler: Arc<F>, leased: Leased)
where
    T: DeserializeOwned + Send + 'static,
    F: Fn(Job<T>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: Display + 'static,
{
    let Leased { message, lease } = leased;
    let id = message.id;
    let span = tracing::info_span!(
        "job",
        queue = %message.queue,
        id = %id,
        attempt = message.attempts
    );

    let outcome = match serde_json::from_value::<T>(message.payload.clone()) {
        Ok(payload) => run_handler(handler, Job { payload, message })
            .instrument(span.clone())
            .await,
        Err(err) => Err(format!("invalid payload: {err}")),
    };
    report(&client, id, &lease, outcome).instrument(span).await;
}

/// Runs the handler in its own task, so that a panic is caught as a
/// [`tokio::task::JoinError`] instead of unwinding through the worker.
async fn run_handler<T, F, Fut, E>(handler: Arc<F>, job: Job<T>) -> Result<(), String>
where
    T: Send + 'static,
    F: Fn(Job<T>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: Display + 'static,
{
    let task = tokio::spawn(
        async move { handler(job).await.map_err(|err| err.to_string()) }.in_current_span(),
    );
    match task.await {
        Ok(result) => result,
        Err(err) if err.is_panic() => {
            let reason = panic_message(err.into_panic());
            tracing::error!(panic = %reason, "handler panicked");
            Err(format!("handler panicked: {reason}"))
        }
        // Tasks are only cancelled when the runtime shuts down under us.
        Err(_) => Err("handler was cancelled".to_owned()),
    }
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_owned()
    }
}

/// Acks or nacks the message. A short retry covers network blips, since a lost
/// ack means the job runs again once its lease expires.
async fn report(client: &Client, id: MessageId, lease: &LeaseToken, outcome: Result<(), String>) {
    const ATTEMPTS: u32 = 3;

    let mut delay = Duration::from_millis(200);
    for attempt in 1..=ATTEMPTS {
        let result = match &outcome {
            Ok(()) => client.ack(id, lease).await,
            Err(error) => {
                let nack = NackRequest {
                    lease: lease.clone(),
                    error: Some(error.clone()),
                    retry_in: None,
                };
                client.nack(id, &nack).await
            }
        };
        match result {
            Ok(message) => {
                tracing::debug!(status = %message.status, "job finished");
                return;
            }
            Err(err) if err.is_lease_mismatch() => {
                tracing::warn!(
                    "the lease expired before the job finished, so the message may run again; \
                     consider a longer visibility_timeout"
                );
                return;
            }
            Err(err) if err.is_retryable() && attempt < ATTEMPTS => {
                tracing::debug!(error = %err, "reporting the job outcome failed, retrying");
                tokio::time::sleep(delay).await;
                delay *= 2;
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "could not report the job outcome; the message runs again when its lease expires"
                );
                return;
            }
        }
    }
}

/// `process` catches handler panics itself, so a failure here is a bug in the
/// worker. It is logged rather than propagated so other jobs keep running.
fn log_supervisor_failure(result: std::result::Result<(), tokio::task::JoinError>) {
    if let Err(err) = result {
        tracing::error!(error = %err, "job supervisor failed");
    }
}

/// The delay between polls after the server could not be reached: 1 second,
/// doubling up to 30.
#[derive(Debug)]
struct RetryDelay {
    next: Duration,
}

impl RetryDelay {
    const INITIAL: Duration = Duration::from_secs(1);
    const MAX: Duration = Duration::from_secs(30);

    fn next(&mut self) -> Duration {
        let delay = self.next;
        self.next = (delay * 2).min(Self::MAX);
        delay
    }

    fn reset(&mut self) {
        self.next = Self::INITIAL;
    }
}

impl Default for RetryDelay {
    fn default() -> Self {
        Self {
            next: Self::INITIAL,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delay_doubles_up_to_the_cap() {
        let mut delay = RetryDelay::default();
        let delays: Vec<u64> = (0..7).map(|_| delay.next().as_secs()).collect();
        assert_eq!(delays, [1, 2, 4, 8, 16, 30, 30]);
        delay.reset();
        assert_eq!(delay.next(), Duration::from_secs(1));
    }

    #[test]
    fn panic_messages() {
        assert_eq!(panic_message(Box::new("boom")), "boom");
        assert_eq!(panic_message(Box::new(String::from("bang"))), "bang");
        assert_eq!(panic_message(Box::new(42)), "unknown panic payload");
    }
}
