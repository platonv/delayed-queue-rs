//! Push delivery: POSTing messages to each push queue's endpoint.
//!
//! A supervisor looks at the queue list every `push.refresh_interval` and
//! keeps one delivery task per queue that has push settings and is not paused.
//! When a queue's push settings change, its task is restarted with the new
//! ones; when they are removed or the queue is paused, its task stops.
//!
//! A delivery task leases messages with a long poll, at most as many as it
//! has free slots (`push.concurrency` per queue, per server), and sends each
//! one in its own task. A 2xx response acks the message. Anything else, a
//! timeout or a connection error nacks it, so the queue's backoff applies.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::http::{HeaderMap, StatusCode, header};
use dally_core::service::limits;
use dally_core::{Leased, NackRequest, PollRequest, PushSettings, QueueName, Service};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::config::PushConfig;
use crate::{Error, signature};

/// How much of an endpoint's error response is kept as the message's error.
const MAX_ERROR_BODY: usize = 1024;

/// How long a delivery task waits after storage fails before polling again.
const RETRY_DELAY: Duration = Duration::from_secs(1);

/// Builds the HTTP client shared by every delivery task.
pub fn client(config: &PushConfig) -> Result<reqwest::Client, Error> {
    reqwest::Client::builder()
        .user_agent(&config.user_agent)
        .build()
        .map_err(|err| Error::Config(format!("push: cannot build HTTP client: {err}")))
}

/// Runs the supervisor until `stop` is cancelled, then waits for every
/// delivery task to finish its in-flight requests.
pub async fn run(
    service: Service,
    config: PushConfig,
    client: reqwest::Client,
    stop: CancellationToken,
) {
    let mut deliveries: HashMap<QueueName, Delivery> = HashMap::new();
    let mut ticker = tokio::time::interval(config.refresh_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = stop.cancelled() => break,
            _ = ticker.tick() => {}
        }
        match push_queues(&service).await {
            Ok(wanted) => reconcile(&mut deliveries, wanted, &service, &config, &client),
            Err(err) => tracing::warn!(error = %err, "cannot list push queues"),
        }
    }

    for delivery in deliveries.into_values() {
        delivery.stop().await;
    }
}

/// The queues that should be delivering right now, with their settings.
async fn push_queues(service: &Service) -> dally_core::Result<HashMap<QueueName, PushSettings>> {
    Ok(service
        .list_queues()
        .await?
        .into_iter()
        .filter(|summary| !summary.queue.settings.paused)
        .filter_map(|summary| {
            let push = summary.queue.settings.push?;
            Some((summary.queue.name, push))
        })
        .collect())
}

/// Starts, restarts and stops delivery tasks so they match `wanted`.
fn reconcile(
    deliveries: &mut HashMap<QueueName, Delivery>,
    wanted: HashMap<QueueName, PushSettings>,
    service: &Service,
    config: &PushConfig,
    client: &reqwest::Client,
) {
    deliveries.retain(|queue, delivery| {
        let keep = wanted.get(queue) == Some(&delivery.settings);
        if !keep {
            tracing::info!(%queue, "stopping push delivery");
            // In-flight requests finish in the background; the old task and
            // a new one may briefly overlap, which at-least-once allows.
            delivery.cancel.cancel();
        }
        keep
    });

    for (queue, settings) in wanted {
        if deliveries.contains_key(&queue) {
            continue;
        }
        tracing::info!(%queue, url = settings.url, "starting push delivery");
        let delivery = Delivery::start(service, queue.clone(), settings, config, client);
        deliveries.insert(queue, delivery);
    }
}

/// A running delivery task for one queue.
struct Delivery {
    settings: PushSettings,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl Delivery {
    fn start(
        service: &Service,
        queue: QueueName,
        settings: PushSettings,
        config: &PushConfig,
        client: &reqwest::Client,
    ) -> Self {
        let cancel = CancellationToken::new();
        let task = tokio::spawn(deliver_queue(
            service.clone(),
            queue,
            settings.clone(),
            client.clone(),
            config.poll_wait,
            cancel.clone(),
        ));
        Self {
            settings,
            cancel,
            task,
        }
    }

    async fn stop(self) {
        self.cancel.cancel();
        let _ = self.task.await;
    }
}

async fn deliver_queue(
    service: Service,
    queue: QueueName,
    settings: PushSettings,
    client: reqwest::Client,
    poll_wait: Duration,
    cancel: CancellationToken,
) {
    let slots = Arc::new(Semaphore::new(settings.concurrency as usize));
    let settings = Arc::new(settings);
    let mut in_flight = JoinSet::new();

    loop {
        // Wait for at least one free slot, then take every other free one, so
        // a single poll fills the queue's whole concurrency.
        let first = tokio::select! {
            _ = cancel.cancelled() => break,
            permit = Arc::clone(&slots).acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => break,
            },
        };
        let mut permits = vec![first];
        while permits.len() < limits::MAX_POLL as usize {
            match Arc::clone(&slots).try_acquire_owned() {
                Ok(permit) => permits.push(permit),
                Err(_) => break,
            }
        }

        // Not cancellable: a poll that is dropped halfway may already hold
        // leases. `poll_wait` bounds how long shutdown waits for it.
        let request = PollRequest {
            limit: permits.len() as u32,
            wait: poll_wait,
        };
        match service.poll(&queue, request).await {
            Ok(messages) => {
                for (message, permit) in messages.into_iter().zip(permits) {
                    let (service, client, settings) =
                        (service.clone(), client.clone(), Arc::clone(&settings));
                    in_flight.spawn(async move {
                        deliver(&service, &client, &settings, message, permit).await;
                    });
                }
            }
            Err(err) => {
                tracing::warn!(%queue, error = %err, "push poll failed");
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(RETRY_DELAY) => {}
                }
            }
        }
        while in_flight.try_join_next().is_some() {}
    }

    while in_flight.join_next().await.is_some() {}
}

/// Sends one message and acks or nacks it depending on the response. The
/// permit is held until the outcome is recorded.
async fn deliver(
    service: &Service,
    client: &reqwest::Client,
    settings: &PushSettings,
    leased: Leased,
    _permit: OwnedSemaphorePermit,
) {
    let Leased { message, lease } = leased;
    let outcome = send(client, settings, &message).await;

    let result = match outcome {
        Outcome::Delivered => service.ack(message.id, &lease).await.map(|_| ()),
        Outcome::Failed { error, retry_in } => {
            tracing::info!(queue = %message.queue, id = %message.id, error, "push delivery failed");
            let nack = NackRequest {
                lease,
                error: Some(error),
                retry_in,
            };
            service.nack(message.id, nack).await.map(|_| ())
        }
    };

    if let Err(err) = result {
        // Most likely the lease expired while the request was running, and
        // the message was handed out again. That attempt decides its fate.
        tracing::warn!(queue = %message.queue, id = %message.id, error = %err, "cannot record push outcome");
    }
}

enum Outcome {
    Delivered,
    Failed {
        error: String,
        retry_in: Option<Duration>,
    },
}

async fn send(
    client: &reqwest::Client,
    settings: &PushSettings,
    message: &dally_core::Message,
) -> Outcome {
    let body = match serde_json::to_vec(&message.payload) {
        Ok(body) => body,
        Err(err) => {
            return Outcome::Failed {
                error: format!("cannot encode payload: {err}"),
                retry_in: None,
            };
        }
    };

    let mut request = client
        .post(&settings.url)
        .timeout(settings.timeout)
        .header(header::CONTENT_TYPE, "application/json")
        .header("Dally-Message-Id", message.id.to_string())
        .header("Dally-Queue", message.queue.as_str())
        .header("Dally-Attempt", message.attempts.to_string());
    if let Some(secret) = &settings.secret {
        let timestamp = chrono::Utc::now().timestamp();
        let signature = signature::sign(secret.as_bytes(), timestamp, &body);
        request = request.header(signature::HEADER, signature);
    }

    let response = match request.body(body).send().await {
        Ok(response) => response,
        Err(err) => {
            return Outcome::Failed {
                error: format!("request failed: {err}"),
                retry_in: None,
            };
        }
    };

    let status = response.status();
    if status.is_success() {
        return Outcome::Delivered;
    }

    let retry_in = match status {
        StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE => {
            retry_after(response.headers())
        }
        _ => None,
    };
    let body = response.bytes().await.unwrap_or_default();
    let excerpt = String::from_utf8_lossy(&body[..body.len().min(MAX_ERROR_BODY)]);
    Outcome::Failed {
        error: format!("HTTP {}: {}", status.as_u16(), excerpt.trim()),
        retry_in,
    }
}

/// Reads a `Retry-After` header given in seconds. The HTTP-date form is rare
/// for APIs and is ignored, so the queue's backoff applies instead.
fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(header::RETRY_AFTER)?.to_str().ok()?;
    value.trim().parse().ok().map(Duration::from_secs)
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    fn headers(retry_after: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        let value = HeaderValue::from_str(retry_after).expect("valid header");
        headers.insert(header::RETRY_AFTER, value);
        headers
    }

    #[test]
    fn retry_after_in_seconds() {
        assert_eq!(retry_after(&headers("120")), Some(Duration::from_secs(120)));
        assert_eq!(retry_after(&headers(" 5 ")), Some(Duration::from_secs(5)));
    }

    #[test]
    fn retry_after_dates_and_junk_are_ignored() {
        assert_eq!(retry_after(&headers("Wed, 21 Oct 2026 07:28:00 GMT")), None);
        assert_eq!(retry_after(&headers("-1")), None);
        assert_eq!(retry_after(&HeaderMap::new()), None);
    }
}
