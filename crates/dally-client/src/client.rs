//! The HTTP client.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use dally_core::{
    LeaseToken, Leased, Message, MessageFilter, MessageId, NackRequest, Page, Queue,
    QueueSettings, QueueSummary, RunAt, ScheduleRequest, Scheduled,
};
use reqwest::{Method, RequestBuilder, Response, StatusCode, Url};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::schedule::ScheduleBuilder;

/// How long a request may take unless [`ClientBuilder::timeout`] says
/// otherwise.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// A client for the dally HTTP API.
///
/// Cloning is cheap and clones share one connection pool, so create one client
/// and pass clones around.
///
/// ```no_run
/// # async fn example() -> dally_client::Result<()> {
/// let client = dally_client::Client::builder("https://dally.internal")
///     .api_key("secret")
///     .build()?;
/// let queues = client.list_queues().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

struct Inner {
    http: reqwest::Client,
    base: Url,
    api_key: Option<String>,
    timeout: Duration,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The API key is left out on purpose: clients end up in logs.
        f.debug_struct("Client")
            .field("base", &self.inner.base.as_str())
            .field("timeout", &self.inner.timeout)
            .finish_non_exhaustive()
    }
}

/// Configures a [`Client`]. Created with [`Client::builder`].
#[derive(Debug)]
pub struct ClientBuilder {
    base_url: String,
    api_key: Option<String>,
    timeout: Duration,
    http: Option<reqwest::Client>,
}

impl ClientBuilder {
    /// Sends `Authorization: Bearer <key>` with every request.
    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }

    /// How long a request may take before it fails with
    /// [`Error::Transport`]. Defaults to 30 seconds.
    ///
    /// Long polls get their wait time on top of this, so a poll that waits 20
    /// seconds for messages does not time out early.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Uses this `reqwest` client, for example to share a connection pool or
    /// to configure proxies and TLS roots.
    pub fn http_client(mut self, http: reqwest::Client) -> Self {
        self.http = Some(http);
        self
    }

    /// Checks the base URL and creates the client.
    pub fn build(self) -> Result<Client> {
        let base = parse_base_url(&self.base_url)?;
        let http = match self.http {
            Some(http) => http,
            None => reqwest::Client::builder()
                .build()
                .map_err(Error::Transport)?,
        };
        Ok(Client {
            inner: Arc::new(Inner {
                http,
                base,
                api_key: self.api_key,
                timeout: self.timeout,
            }),
        })
    }
}

fn parse_base_url(url: &str) -> Result<Url> {
    let invalid = |reason: &str| Error::InvalidUrl {
        url: url.to_owned(),
        reason: reason.to_owned(),
    };
    let parsed = Url::parse(url).map_err(|err| invalid(&err.to_string()))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(invalid("the scheme must be http or https"));
    }
    if parsed.cannot_be_a_base() {
        return Err(invalid("the URL cannot have paths appended to it"));
    }
    Ok(parsed)
}

impl Client {
    /// A client for the server at `base_url`, such as
    /// `http://127.0.0.1:7070`, with default settings.
    ///
    /// The URL may include a path prefix if dally is mounted below the root,
    /// as in `https://example.com/dally`.
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        Self::builder(base_url).build()
    }

    /// Starts configuring a client for the server at `base_url`.
    pub fn builder(base_url: impl Into<String>) -> ClientBuilder {
        ClientBuilder {
            base_url: base_url.into(),
            api_key: None,
            timeout: DEFAULT_TIMEOUT,
            http: None,
        }
    }

    /// The server's base URL.
    pub fn base_url(&self) -> &Url {
        &self.inner.base
    }

    /// Schedules one message with `payload`, which can be any type that
    /// serializes to JSON. Set options on the returned builder, then call
    /// [`send`](ScheduleBuilder::send).
    ///
    /// ```no_run
    /// # use std::time::Duration;
    /// # #[derive(serde::Serialize)]
    /// # struct Welcome { user_id: u64 }
    /// # async fn example(client: dally_client::Client) -> dally_client::Result<()> {
    /// let scheduled = client
    ///     .schedule("emails", &Welcome { user_id: 42 })
    ///     .delay(Duration::from_secs(600))
    ///     .idempotency_key("welcome-42")
    ///     .send()
    ///     .await?;
    /// println!("scheduled {}", scheduled.message.id);
    /// # Ok(())
    /// # }
    /// ```
    pub fn schedule<'a, T>(&'a self, queue: &'a str, payload: &T) -> ScheduleBuilder<'a>
    where
        T: Serialize + ?Sized,
    {
        ScheduleBuilder::new(self, queue, serde_json::to_value(payload))
    }

    /// Schedules one message described by a full [`ScheduleRequest`].
    /// [`schedule`](Self::schedule) is usually more convenient.
    pub async fn schedule_request(
        &self,
        queue: &str,
        request: &ScheduleRequest,
    ) -> Result<Scheduled> {
        let url = self.url(&["v1", "queues", queue, "messages"])?;
        let response = self.execute(self.request(Method::POST, url).json(request)).await?;
        // The server says whether the message is new through the status code:
        // 200 means the idempotency key matched an existing message.
        let created = response.status() == StatusCode::CREATED;
        let message = read_json(response).await?;
        Ok(Scheduled { message, created })
    }

    /// Schedules several messages in one request. The results come back in
    /// the same order as the requests.
    ///
    /// The server accepts at most 100 messages per batch.
    pub async fn schedule_batch(
        &self,
        queue: &str,
        requests: &[ScheduleRequest],
    ) -> Result<Vec<Scheduled>> {
        #[derive(Deserialize)]
        struct Batch {
            messages: Vec<Scheduled>,
        }

        let url = self.url(&["v1", "queues", queue, "messages"])?;
        let batch: Batch = self
            .send(self.request(Method::POST, url).json(requests))
            .await?;
        Ok(batch.messages)
    }

    /// Leases up to `limit` due messages from `queue`. If none are due, waits
    /// up to `wait` for one to arrive before returning an empty list. The
    /// server caps `wait` at 30 seconds and `limit` at 100.
    ///
    /// Each message must be acked or nacked with its lease before the
    /// visibility timeout runs out, or it is handed to another worker.
    /// [`Worker`](crate::Worker) does all of this for you.
    pub async fn poll(&self, queue: &str, limit: u32, wait: Duration) -> Result<Vec<Leased>> {
        #[derive(Serialize)]
        struct Body {
            limit: u32,
            #[serde(with = "humantime_serde")]
            wait: Duration,
        }
        #[derive(Deserialize)]
        struct Polled {
            messages: Vec<Leased>,
        }

        let url = self.url(&["v1", "queues", queue, "poll"])?;
        let request = self
            .request(Method::POST, url)
            .timeout(self.inner.timeout.saturating_add(wait))
            .json(&Body { limit, wait });
        let polled: Polled = self.send(request).await?;
        Ok(polled.messages)
    }

    /// Marks a leased message as done.
    pub async fn ack(&self, id: MessageId, lease: &LeaseToken) -> Result<Message> {
        #[derive(Serialize)]
        struct Body<'a> {
            lease: &'a LeaseToken,
        }

        let url = self.url(&["v1", "messages", &id.to_string(), "ack"])?;
        self.send(self.request(Method::POST, url).json(&Body { lease }))
            .await
    }

    /// Reports that an attempt failed. The message is retried after the
    /// queue's backoff (or `request.retry_in`), or marked dead if it has no
    /// attempts left.
    ///
    /// ```no_run
    /// # use dally_client::NackRequest;
    /// # async fn example(client: dally_client::Client, leased: dally_client::Leased) -> dally_client::Result<()> {
    /// client
    ///     .nack(
    ///         leased.message.id,
    ///         &NackRequest {
    ///             lease: leased.lease.clone(),
    ///             error: Some("SMTP server refused the connection".into()),
    ///             retry_in: None,
    ///         },
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn nack(&self, id: MessageId, request: &NackRequest) -> Result<Message> {
        let url = self.url(&["v1", "messages", &id.to_string(), "nack"])?;
        self.send(self.request(Method::POST, url).json(request))
            .await
    }

    /// Fetches a message by id.
    pub async fn get_message(&self, id: MessageId) -> Result<Message> {
        let url = self.url(&["v1", "messages", &id.to_string()])?;
        self.send(self.request(Method::GET, url)).await
    }

    /// Changes when a scheduled message runs. Only messages that are still
    /// `scheduled` can be rescheduled.
    pub async fn reschedule(&self, id: MessageId, run_at: RunAt) -> Result<Message> {
        #[derive(Serialize)]
        struct Body {
            #[serde(
                with = "humantime_serde",
                skip_serializing_if = "Option::is_none"
            )]
            delay: Option<Duration>,
            #[serde(skip_serializing_if = "Option::is_none")]
            run_at: Option<chrono::DateTime<chrono::Utc>>,
        }

        let body = match run_at {
            RunAt::Now => Body {
                delay: None,
                run_at: None,
            },
            RunAt::After(delay) => Body {
                delay: Some(delay),
                run_at: None,
            },
            RunAt::At(at) => Body {
                delay: None,
                run_at: Some(at),
            },
        };
        let url = self.url(&["v1", "messages", &id.to_string()])?;
        self.send(self.request(Method::PATCH, url).json(&body))
            .await
    }

    /// Cancels a scheduled message so it never runs. Messages that are
    /// already running or finished cannot be cancelled.
    pub async fn cancel(&self, id: MessageId) -> Result<Message> {
        let url = self.url(&["v1", "messages", &id.to_string()])?;
        self.send(self.request(Method::DELETE, url)).await
    }

    /// Lists messages, newest first. Pass the returned page's `next` as
    /// `filter.before` to get the following page. A `limit` of 0 means the
    /// server's default page size.
    pub async fn list_messages(&self, filter: &MessageFilter) -> Result<Page<Message>> {
        #[derive(Serialize)]
        struct Params<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            queue: Option<&'a str>,
            #[serde(skip_serializing_if = "Option::is_none")]
            status: Option<&'a str>,
            #[serde(skip_serializing_if = "Option::is_none")]
            before: Option<MessageId>,
            #[serde(skip_serializing_if = "Option::is_none")]
            limit: Option<u32>,
        }

        let params = Params {
            queue: filter.queue.as_ref().map(|queue| queue.as_str()),
            status: filter.status.as_ref().map(|status| status.as_str()),
            before: filter.before,
            limit: (filter.limit > 0).then_some(filter.limit),
        };
        let url = self.url(&["v1", "messages"])?;
        self.send(self.request(Method::GET, url).query(&params))
            .await
    }

    /// Lists every queue with its message counts.
    pub async fn list_queues(&self) -> Result<Vec<QueueSummary>> {
        #[derive(Deserialize)]
        struct QueueList {
            queues: Vec<QueueSummary>,
        }

        let url = self.url(&["v1", "queues"])?;
        let list: QueueList = self.send(self.request(Method::GET, url)).await?;
        Ok(list.queues)
    }

    /// Fetches a queue's settings and message counts.
    pub async fn get_queue(&self, queue: &str) -> Result<QueueSummary> {
        let url = self.url(&["v1", "queues", queue])?;
        self.send(self.request(Method::GET, url)).await
    }

    /// Creates a queue or replaces all of its settings.
    pub async fn put_queue(&self, queue: &str, settings: &QueueSettings) -> Result<Queue> {
        let url = self.url(&["v1", "queues", queue])?;
        self.send(self.request(Method::PUT, url).json(settings))
            .await
    }

    /// Stops delivering messages from a queue. It still accepts new ones.
    pub async fn pause(&self, queue: &str) -> Result<Queue> {
        let url = self.url(&["v1", "queues", queue, "pause"])?;
        self.send(self.request(Method::POST, url)).await
    }

    /// Resumes delivery from a paused queue.
    pub async fn resume(&self, queue: &str) -> Result<Queue> {
        let url = self.url(&["v1", "queues", queue, "resume"])?;
        self.send(self.request(Method::POST, url)).await
    }

    /// Moves dead messages back to `scheduled` so they run again with a fresh
    /// set of attempts. With `None`, retries every dead message in the queue.
    /// Returns how many messages were retried.
    pub async fn retry_dead(&self, queue: &str, ids: Option<&[MessageId]>) -> Result<u64> {
        #[derive(Serialize)]
        struct Body<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            ids: Option<&'a [MessageId]>,
        }
        #[derive(Deserialize)]
        struct Retried {
            retried: u64,
        }

        let url = self.url(&["v1", "queues", queue, "dead", "retry"])?;
        let retried: Retried = self
            .send(self.request(Method::POST, url).json(&Body { ids }))
            .await?;
        Ok(retried.retried)
    }

    /// Succeeds if the server process is up.
    pub async fn health(&self) -> Result<()> {
        let url = self.url(&["healthz"])?;
        self.execute(self.request(Method::GET, url)).await?;
        Ok(())
    }

    /// Succeeds if the server is up and can reach its storage.
    pub async fn ready(&self) -> Result<()> {
        let url = self.url(&["readyz"])?;
        self.execute(self.request(Method::GET, url)).await?;
        Ok(())
    }

    /// The base URL with `segments` appended. Each segment is percent-encoded,
    /// so a queue name can never escape its place in the path.
    fn url(&self, segments: &[&str]) -> Result<Url> {
        let mut url = self.inner.base.clone();
        url.path_segments_mut()
            .map_err(|()| Error::InvalidUrl {
                url: self.inner.base.to_string(),
                reason: "the URL cannot have paths appended to it".to_owned(),
            })?
            .pop_if_empty()
            .extend(segments);
        Ok(url)
    }

    fn request(&self, method: Method, url: Url) -> RequestBuilder {
        let request = self
            .inner
            .http
            .request(method, url)
            .timeout(self.inner.timeout);
        match &self.inner.api_key {
            Some(key) => request.bearer_auth(key),
            None => request,
        }
    }

    /// Sends a request and parses its JSON response.
    async fn send<T: DeserializeOwned>(&self, request: RequestBuilder) -> Result<T> {
        read_json(self.execute(request).await?).await
    }

    /// Sends a request and turns error statuses into [`Error`]s.
    async fn execute(&self, request: RequestBuilder) -> Result<Response> {
        let response = request.send().await.map_err(Error::Transport)?;
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let body = response.bytes().await.map_err(Error::Transport)?;
        Err(Error::from_body(status, &body))
    }
}

async fn read_json<T: DeserializeOwned>(response: Response) -> Result<T> {
    let body = response.bytes().await.map_err(Error::Transport)?;
    serde_json::from_slice(&body).map_err(Error::Json)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(base: &str, segments: &[&str]) -> String {
        let client = Client::new(base).expect("valid base URL");
        client.url(segments).expect("url").to_string()
    }

    #[test]
    fn appends_paths_to_the_base_url() {
        assert_eq!(
            path("http://localhost:7070", &["v1", "queues"]),
            "http://localhost:7070/v1/queues"
        );
        assert_eq!(
            path("https://example.com/dally/", &["v1", "queues"]),
            "https://example.com/dally/v1/queues"
        );
    }

    #[test]
    fn encodes_path_segments() {
        assert_eq!(
            path("http://localhost", &["v1", "queues", "../x?y"]),
            "http://localhost/v1/queues/..%2Fx%3Fy"
        );
    }

    #[test]
    fn rejects_unusable_base_urls() {
        assert!(Client::new("localhost:7070").is_err());
        assert!(Client::new("ftp://localhost").is_err());
        assert!(Client::new("not a url").is_err());
    }

    #[test]
    fn debug_hides_the_api_key() {
        let client = Client::builder("http://localhost")
            .api_key("hunter2")
            .build()
            .expect("valid client");
        assert!(!format!("{client:?}").contains("hunter2"));
    }
}
