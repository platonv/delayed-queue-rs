//! The Redis backend.
//!
//! # Data model
//!
//! Every key starts with a prefix (`dally:` by default). For each queue:
//!
//! | Key                         | Type       | Holds                                   |
//! |-----------------------------|------------|-----------------------------------------|
//! | `queues`                    | sorted set | every queue name, score 0 (name order)  |
//! | `queue:{q}`                 | hash       | the queue's settings                    |
//! | `queue:{q}:delayed`         | sorted set | scheduled ids, by `run_at`              |
//! | `queue:{q}:ready`           | sorted set | due ids, in delivery order              |
//! | `queue:{q}:running`         | sorted set | leased ids, by lease expiry             |
//! | `queue:{q}:status:{status}` | sorted set | ids per status, score 0 (id order)      |
//! | `queue:{q}:finished`        | sorted set | finished ids, by `finished_at`          |
//! | `queue:{q}:keys`            | hash       | idempotency key to message id           |
//! | `message:{id}`              | hash       | the message                             |
//!
//! Being due depends on `run_at`, but delivery order depends on priority
//! first, so one sorted set cannot do both. Scheduled messages wait in
//! `delayed`; leasing first moves the due ones to `ready`, whose score is the
//! negated priority and whose members start with a zero-padded `run_at`, so
//! that ties sort by time. See `lua/lib.lua`.
//!
//! Ids are UUIDv7, so the per-status sets list messages in creation order by
//! member alone.
//!
//! # Atomicity and time
//!
//! Every operation that touches more than one key is a Lua script, so it runs
//! atomically. Scripts take their time from `TIME`, the Redis server's clock,
//! so several dally servers agree on what is due. Scripts build key names
//! from data they read, which works on a single Redis server or a replicated
//! setup, but not on Redis Cluster.
//!
//! # Durability
//!
//! Redis only keeps what it has persisted. Run it with `appendonly yes` in
//! production; see `docs/backends/redis.md`.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Script};
use tokio::task::JoinHandle;
use uuid::Uuid;

// Selects ring as rustls's crypto provider for `rediss://` connections.
use rustls as _;

use super::{ConnectOptions, Storage};
use crate::error::{Error, Result};
use crate::message::{
    LeaseToken, Leased, Message, MessageFilter, MessageId, NewMessage, Page, Retry, RunAt,
    Scheduled, Status,
};
use crate::notify::{Notifier, Subscription};
use crate::queue::{
    Backoff, PushSettings, Queue, QueueName, QueueSettings, QueueStats, QueueSummary,
};

const DEFAULT_PREFIX: &str = "dally:";

/// The Lua scripts, each prepended with the shared helpers.
struct Scripts {
    ensure_queue: Script,
    put_queue: Script,
    queues: Script,
    schedule: Script,
    lease: Script,
    ack: Script,
    nack: Script,
    cancel: Script,
    reschedule: Script,
    retry_dead: Script,
    prune: Script,
}

static SCRIPTS: LazyLock<Scripts> = LazyLock::new(|| {
    let script = |body: &str| Script::new(&format!("{}\n{body}", include_str!("lua/lib.lua")));
    Scripts {
        ensure_queue: script(include_str!("lua/ensure_queue.lua")),
        put_queue: script(include_str!("lua/put_queue.lua")),
        queues: script(include_str!("lua/queues.lua")),
        schedule: script(include_str!("lua/schedule.lua")),
        lease: script(include_str!("lua/lease.lua")),
        ack: script(include_str!("lua/ack.lua")),
        nack: script(include_str!("lua/nack.lua")),
        cancel: script(include_str!("lua/cancel.lua")),
        reschedule: script(include_str!("lua/reschedule.lua")),
        retry_dead: script(include_str!("lua/retry_dead.lua")),
        prune: script(include_str!("lua/prune.lua")),
    }
});

/// A hash as scripts return it.
type Fields = HashMap<String, String>;

/// Stores queues and messages in Redis.
pub struct RedisStorage {
    connection: ConnectionManager,
    prefix: String,
    notifier: Arc<Notifier>,
    listener: JoinHandle<()>,
}

impl RedisStorage {
    /// Connects using a `redis://` or `rediss://` URL. A `prefix` query
    /// parameter sets the key prefix (`redis://host/0?prefix=myapp:`); it
    /// defaults to `dally:`.
    ///
    /// Redis has no schema, so `options.migrate` has nothing to do.
    pub async fn connect(url: &str, _options: &ConnectOptions) -> Result<Self> {
        let (url, prefix) = split_prefix(url)?;
        let client = redis::Client::open(url)?;
        let connection = client.get_connection_manager().await?;

        let notifier = Arc::new(Notifier::new());
        let channel = format!("{prefix}scheduled");
        let listener = tokio::spawn(listen(client, channel, Arc::clone(&notifier)));

        Ok(Self {
            connection,
            prefix,
            notifier,
            listener,
        })
    }

    /// Runs a script with the key prefix as its first argument.
    fn invoke(&self, script: &'static Script) -> redis::ScriptInvocation<'static> {
        let mut invocation = script.prepare_invoke();
        invocation.arg(&self.prefix);
        invocation
    }

    fn connection(&self) -> ConnectionManager {
        // Cheap: the manager is a handle to one shared, multiplexed connection.
        self.connection.clone()
    }
}

impl Drop for RedisStorage {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

/// Separates dally's `prefix` parameter from the URL the redis crate sees.
fn split_prefix(url: &str) -> Result<(String, String)> {
    let mut parsed =
        url::Url::parse(url).map_err(|err| Error::invalid(format!("invalid Redis URL: {err}")))?;

    let mut prefix = DEFAULT_PREFIX.to_owned();
    let others: Vec<(String, String)> = parsed
        .query_pairs()
        .filter_map(|(name, value)| {
            if name == "prefix" {
                prefix = value.into_owned();
                None
            } else {
                Some((name.into_owned(), value.into_owned()))
            }
        })
        .collect();

    if others.is_empty() {
        parsed.set_query(None);
    } else {
        parsed.query_pairs_mut().clear().extend_pairs(others);
    }
    Ok((parsed.into(), prefix))
}

/// Forwards `PUBLISH {prefix}scheduled <queue>` to local subscribers. If the
/// connection drops, polls fall back to their periodic recheck until it is
/// back, so nothing is lost.
async fn listen(client: redis::Client, channel: String, notifier: Arc<Notifier>) {
    loop {
        let result: redis::RedisResult<()> = async {
            let mut pubsub = client.get_async_pubsub().await?;
            pubsub.subscribe(&channel).await?;
            let mut messages = pubsub.on_message();
            while let Some(message) = messages.next().await {
                if let Ok(queue) = message.get_payload::<String>()
                    && let Ok(queue) = QueueName::new(queue)
                {
                    notifier.notify(&queue);
                }
            }
            Ok(())
        }
        .await;

        match result {
            Ok(()) => tracing::warn!("redis subscription closed; reconnecting in 1s"),
            Err(err) => tracing::warn!(error = %err, "redis listener failed; retrying in 1s"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[async_trait]
impl Storage for RedisStorage {
    async fn ensure_queue(&self, name: &QueueName, defaults: &QueueSettings) -> Result<Queue> {
        let mut invocation = self.invoke(&SCRIPTS.ensure_queue);
        invocation.arg(name.as_str());
        settings_args(&mut invocation, defaults)?;
        let fields: Fields = invocation.invoke_async(&mut self.connection()).await?;
        parse_queue(&fields)
    }

    async fn put_queue(&self, name: &QueueName, settings: &QueueSettings) -> Result<Queue> {
        let mut invocation = self.invoke(&SCRIPTS.put_queue);
        invocation.arg(name.as_str());
        settings_args(&mut invocation, settings)?;
        let fields: Fields = invocation.invoke_async(&mut self.connection()).await?;
        parse_queue(&fields)
    }

    async fn get_queue(&self, name: &QueueName) -> Result<Option<QueueSummary>> {
        let mut invocation = self.invoke(&SCRIPTS.queues);
        invocation.arg(name.as_str());
        let summaries: Vec<(Fields, Vec<u64>)> =
            invocation.invoke_async(&mut self.connection()).await?;
        summaries.into_iter().next().map(parse_summary).transpose()
    }

    async fn list_queues(&self) -> Result<Vec<QueueSummary>> {
        let summaries: Vec<(Fields, Vec<u64>)> = self
            .invoke(&SCRIPTS.queues)
            .invoke_async(&mut self.connection())
            .await?;
        summaries.into_iter().map(parse_summary).collect()
    }

    async fn schedule(
        &self,
        queue: &QueueName,
        messages: Vec<NewMessage>,
    ) -> Result<Vec<Scheduled>> {
        let mut invocation = self.invoke(&SCRIPTS.schedule);
        invocation.arg(queue.as_str());
        for message in &messages {
            let (at, delay) = split_run_at(message.run_at);
            invocation
                .arg(message.id.to_string())
                .arg(serde_json::to_string(&message.payload).map_err(Error::storage)?)
                .arg(message.priority)
                .arg(message.max_attempts)
                .arg(millis(message.visibility_timeout))
                .arg(message.idempotency_key.as_deref().unwrap_or_default())
                .arg(at)
                .arg(delay);
        }

        let results: Vec<(i64, Fields)> = invocation.invoke_async(&mut self.connection()).await?;
        results
            .into_iter()
            .map(|(created, fields)| {
                Ok(Scheduled {
                    message: parse_message(&fields)?,
                    created: created == 1,
                })
            })
            .collect()
    }

    async fn lease(&self, queue: &QueueName, limit: u32) -> Result<Vec<Leased>> {
        let mut invocation = self.invoke(&SCRIPTS.lease);
        invocation.arg(queue.as_str());
        for _ in 0..limit {
            invocation.arg(LeaseToken::generate().as_str());
        }

        let leased: Vec<Fields> = invocation.invoke_async(&mut self.connection()).await?;
        leased.iter().map(parse_leased).collect()
    }

    async fn ack(&self, id: MessageId, lease: &LeaseToken) -> Result<Option<Message>> {
        let mut invocation = self.invoke(&SCRIPTS.ack);
        invocation.arg(id.to_string()).arg(lease.as_str());
        let fields: Option<Fields> = invocation.invoke_async(&mut self.connection()).await?;
        fields.as_ref().map(parse_message).transpose()
    }

    async fn nack(
        &self,
        id: MessageId,
        lease: &LeaseToken,
        retry: Retry,
    ) -> Result<Option<Message>> {
        let mut invocation = self.invoke(&SCRIPTS.nack);
        invocation
            .arg(id.to_string())
            .arg(lease.as_str())
            .arg(millis(retry.delay));
        if let Some(error) = &retry.error {
            invocation.arg(error);
        }
        let fields: Option<Fields> = invocation.invoke_async(&mut self.connection()).await?;
        fields.as_ref().map(parse_message).transpose()
    }

    async fn get_message(&self, id: MessageId) -> Result<Option<Message>> {
        let fields: Fields = self.connection().hgetall(self.message_key(id)).await?;
        if fields.is_empty() {
            return Ok(None);
        }
        parse_message(&fields).map(Some)
    }

    async fn list_messages(&self, filter: &MessageFilter) -> Result<Page<Message>> {
        let ids = self.newest_ids(filter).await?;

        let mut pipe = redis::pipe();
        for id in &ids {
            pipe.hgetall(self.message_key(*id));
        }
        let rows: Vec<Fields> = pipe.query_async(&mut self.connection()).await?;

        // A message can be pruned between the two round trips; skip it.
        let mut items = rows
            .iter()
            .filter(|fields| !fields.is_empty())
            .map(parse_message)
            .collect::<Result<Vec<_>>>()?;

        let next = if ids.len() > filter.limit as usize {
            items.truncate(filter.limit as usize);
            items.last().map(|message| message.id)
        } else {
            None
        };
        Ok(Page { items, next })
    }

    async fn cancel(&self, id: MessageId) -> Result<Option<Message>> {
        let mut invocation = self.invoke(&SCRIPTS.cancel);
        invocation.arg(id.to_string());
        let fields: Option<Fields> = invocation.invoke_async(&mut self.connection()).await?;
        fields.as_ref().map(parse_message).transpose()
    }

    async fn reschedule(&self, id: MessageId, run_at: RunAt) -> Result<Option<Message>> {
        let (at, delay) = split_run_at(run_at);
        let mut invocation = self.invoke(&SCRIPTS.reschedule);
        invocation.arg(id.to_string()).arg(at).arg(delay);
        let fields: Option<Fields> = invocation.invoke_async(&mut self.connection()).await?;
        fields.as_ref().map(parse_message).transpose()
    }

    async fn retry_dead(&self, queue: &QueueName, ids: Option<&[MessageId]>) -> Result<u64> {
        // An empty list means "these zero messages", not "all of them".
        if ids.is_some_and(<[MessageId]>::is_empty) {
            return Ok(0);
        }
        let mut invocation = self.invoke(&SCRIPTS.retry_dead);
        invocation.arg(queue.as_str());
        for id in ids.unwrap_or_default() {
            invocation.arg(id.to_string());
        }
        Ok(invocation.invoke_async(&mut self.connection()).await?)
    }

    async fn prune(&self) -> Result<u64> {
        Ok(self
            .invoke(&SCRIPTS.prune)
            .invoke_async(&mut self.connection())
            .await?)
    }

    fn subscribe(&self, queue: &QueueName) -> Subscription {
        self.notifier.subscribe(queue)
    }

    async fn now(&self) -> Result<DateTime<Utc>> {
        let (seconds, micros): (i64, i64) = redis::cmd("TIME")
            .query_async(&mut self.connection())
            .await?;
        DateTime::from_timestamp_micros(seconds * 1_000_000 + micros)
            .ok_or_else(|| Error::invalid("redis TIME is out of range"))
    }
}

impl RedisStorage {
    fn message_key(&self, id: MessageId) -> String {
        format!("{}message:{id}", self.prefix)
    }

    fn status_key(&self, queue: &str, status: Status) -> String {
        format!("{}queue:{queue}:status:{status}", self.prefix)
    }

    /// The ids of up to `limit + 1` messages matching the filter, newest
    /// first. Each (queue, status) pair has its own index, so this reads the
    /// newest few from each index the filter covers and merges them.
    async fn newest_ids(&self, filter: &MessageFilter) -> Result<Vec<MessageId>> {
        let queues: Vec<String> = match &filter.queue {
            Some(queue) => vec![queue.to_string()],
            None => {
                self.connection()
                    .zrange(format!("{}queues", self.prefix), 0, -1)
                    .await?
            }
        };
        let statuses: Vec<Status> = match filter.status {
            Some(status) => vec![status],
            None => Status::ALL.to_vec(),
        };
        let wanted = filter.limit as usize + 1;
        let upper = match filter.before {
            Some(before) => format!("({before}"),
            None => "+".to_owned(),
        };

        let mut pipe = redis::pipe();
        for queue in &queues {
            for status in &statuses {
                pipe.cmd("ZRANGE")
                    .arg(self.status_key(queue, *status))
                    .arg(&upper)
                    .arg("-")
                    .arg("BYLEX")
                    .arg("REV")
                    .arg("LIMIT")
                    .arg(0)
                    .arg(wanted);
            }
        }
        let batches: Vec<Vec<String>> = pipe.query_async(&mut self.connection()).await?;

        let mut ids = batches
            .into_iter()
            .flatten()
            .map(|id| id.parse::<MessageId>())
            .collect::<Result<Vec<_>>>()?;
        ids.sort_unstable_by(|a, b| b.cmp(a));
        ids.dedup();
        ids.truncate(wanted);
        Ok(ids)
    }
}

/// Appends queue settings in the order `save_settings` in `lib.lua` reads
/// them.
fn settings_args(
    invocation: &mut redis::ScriptInvocation<'_>,
    settings: &QueueSettings,
) -> Result<()> {
    let push = match &settings.push {
        Some(push) => serde_json::to_string(push).map_err(Error::storage)?,
        None => String::new(),
    };
    invocation
        .arg(millis(settings.visibility_timeout))
        .arg(settings.max_attempts)
        .arg(millis(settings.backoff.base))
        .arg(millis(settings.backoff.cap))
        .arg(millis(settings.retention))
        .arg(u8::from(settings.paused))
        .arg(push);
    Ok(())
}

/// Splits a [`RunAt`] into the two arguments scripts expect: an absolute time
/// in ms (empty for none), or a delay from the Redis clock.
fn split_run_at(run_at: RunAt) -> (String, i64) {
    match run_at {
        RunAt::Now => (String::new(), 0),
        RunAt::After(delay) => (String::new(), millis(delay)),
        RunAt::At(at) => (at.timestamp_millis().to_string(), 0),
    }
}

fn parse_summary((fields, counts): (Fields, Vec<u64>)) -> Result<QueueSummary> {
    let count = |i: usize| counts.get(i).copied().unwrap_or_default();
    Ok(QueueSummary {
        queue: parse_queue(&fields)?,
        // In the order of STATUSES in lib.lua.
        stats: QueueStats {
            scheduled: count(0),
            running: count(1),
            succeeded: count(2),
            cancelled: count(3),
            dead: count(4),
        },
    })
}

fn parse_queue(fields: &Fields) -> Result<Queue> {
    let push = match fields.get("push") {
        Some(json) => Some(serde_json::from_str::<PushSettings>(json).map_err(Error::storage)?),
        None => None,
    };
    Ok(Queue {
        name: QueueName::new(field(fields, "name")?)?,
        settings: QueueSettings {
            visibility_timeout: duration(fields, "visibility_timeout_ms")?,
            max_attempts: number(fields, "max_attempts")?,
            backoff: Backoff {
                base: duration(fields, "backoff_base_ms")?,
                cap: duration(fields, "backoff_cap_ms")?,
            },
            retention: duration(fields, "retention_ms")?,
            paused: field(fields, "paused")? == "1",
            push,
        },
        created_at: timestamp(fields, "created_at")?,
        updated_at: timestamp(fields, "updated_at")?,
    })
}

fn parse_message(fields: &Fields) -> Result<Message> {
    let id = Uuid::parse_str(field(fields, "id")?).map_err(Error::storage)?;
    Ok(Message {
        id: MessageId::from_uuid(id),
        queue: QueueName::new(field(fields, "queue")?)?,
        payload: serde_json::from_str(field(fields, "payload")?).map_err(Error::storage)?,
        status: field(fields, "status")?.parse()?,
        priority: number(fields, "priority")?,
        attempts: number(fields, "attempts")?,
        max_attempts: number(fields, "max_attempts")?,
        visibility_timeout: duration(fields, "visibility_timeout_ms")?,
        idempotency_key: fields.get("idempotency_key").cloned(),
        run_at: timestamp(fields, "run_at")?,
        lease_expires_at: optional_timestamp(fields, "lease_expires_at")?,
        last_error: fields.get("last_error").cloned(),
        created_at: timestamp(fields, "created_at")?,
        updated_at: timestamp(fields, "updated_at")?,
        finished_at: optional_timestamp(fields, "finished_at")?,
    })
}

fn parse_leased(fields: &Fields) -> Result<Leased> {
    let lease = LeaseToken::from(field(fields, "lease_token")?.to_owned());
    Ok(Leased {
        message: parse_message(fields)?,
        lease,
    })
}

fn field<'a>(fields: &'a Fields, name: &str) -> Result<&'a str> {
    fields
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| Error::invalid(format!("stored record is missing {name}")))
}

fn number<T: std::str::FromStr>(fields: &Fields, name: &str) -> Result<T> {
    field(fields, name)?
        .parse()
        .map_err(|_| Error::invalid(format!("stored {name} is not a valid number")))
}

fn duration(fields: &Fields, name: &str) -> Result<Duration> {
    number(fields, name).map(Duration::from_millis)
}

fn timestamp(fields: &Fields, name: &str) -> Result<DateTime<Utc>> {
    let millis: i64 = number(fields, name)?;
    DateTime::from_timestamp_millis(millis)
        .ok_or_else(|| Error::invalid(format!("stored {name} is out of range")))
}

fn optional_timestamp(fields: &Fields, name: &str) -> Result<Option<DateTime<Utc>>> {
    if fields.contains_key(name) {
        timestamp(fields, name).map(Some)
    } else {
        Ok(None)
    }
}

fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_is_taken_out_of_the_url() {
        let (url, prefix) = split_prefix("redis://localhost:6379/2?prefix=myapp:").expect("valid");
        assert_eq!(url, "redis://localhost:6379/2");
        assert_eq!(prefix, "myapp:");

        let (url, prefix) =
            split_prefix("rediss://user:pw@host/0?protocol=resp3&prefix=x:").expect("valid");
        assert_eq!(url, "rediss://user:pw@host/0?protocol=resp3");
        assert_eq!(prefix, "x:");

        let (url, prefix) = split_prefix("redis://localhost").expect("valid");
        assert_eq!(url, "redis://localhost");
        assert_eq!(prefix, DEFAULT_PREFIX);
    }
}
