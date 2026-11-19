//! The PostgreSQL backend. Requires PostgreSQL 13 or later.
//!
//! Leasing uses `FOR UPDATE SKIP LOCKED`, so any number of servers can poll
//! the same queue without blocking each other or taking the same row.
//! `LISTEN/NOTIFY` on the `dally_scheduled` channel wakes long polls on every
//! server as soon as a message is scheduled.
//!
//! All timestamps come from the database's `now()`, so servers with skewed
//! clocks still agree on what is due.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::{PgConnectOptions, PgListener, PgPool, PgPoolOptions};
use sqlx::types::Json;
use sqlx::{FromRow, Postgres, Transaction};
use tokio::task::JoinHandle;
use uuid::Uuid;

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

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/postgres");

const CHANNEL: &str = "dally_scheduled";

/// Stores queues and messages in PostgreSQL.
pub struct PostgresStorage {
    pool: PgPool,
    notifier: Arc<Notifier>,
    listener: JoinHandle<()>,
}

impl PostgresStorage {
    /// Connects using a `postgres://` URL.
    pub async fn connect(url: &str, options: &ConnectOptions) -> Result<Self> {
        let connect_options: PgConnectOptions = url.parse()?;
        Self::connect_with(connect_options, options).await
    }

    /// Connects using explicit connection options, for settings a URL cannot
    /// express.
    pub async fn connect_with(
        connect_options: PgConnectOptions,
        options: &ConnectOptions,
    ) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect_with(connect_options)
            .await?;

        if options.migrate {
            MIGRATOR.run(&pool).await.map_err(Error::storage)?;
        }

        let notifier = Arc::new(Notifier::new());
        let listener = tokio::spawn(listen(pool.clone(), Arc::clone(&notifier)));

        Ok(Self {
            pool,
            notifier,
            listener,
        })
    }
}

impl Drop for PostgresStorage {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

/// Forwards `NOTIFY dally_scheduled, '<queue>'` to local subscribers. If the
/// connection drops, polls fall back to their periodic recheck until it is
/// back, so nothing is lost.
async fn listen(pool: PgPool, notifier: Arc<Notifier>) {
    loop {
        let result: Result<(), sqlx::Error> = async {
            let mut listener = PgListener::connect_with(&pool).await?;
            listener.listen(CHANNEL).await?;
            loop {
                let notification = listener.recv().await?;
                if let Ok(queue) = QueueName::new(notification.payload()) {
                    notifier.notify(&queue);
                }
            }
        }
        .await;

        if let Err(err) = result {
            tracing::warn!(error = %err, "postgres listener failed; retrying in 1s");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[async_trait]
impl Storage for PostgresStorage {
    async fn ensure_queue(&self, name: &QueueName, defaults: &QueueSettings) -> Result<Queue> {
        // Most calls find the queue, so look before trying to insert.
        if let Some(queue) = self.find_queue(name).await? {
            return Ok(queue);
        }

        let row: Option<QueueRow> = sqlx::query_as(
            "INSERT INTO dally_queues
                (name, visibility_timeout_ms, max_attempts, backoff_base_ms, backoff_cap_ms,
                 retention_ms, paused, push)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (name) DO NOTHING
             RETURNING *",
        )
        .bind(name.as_str())
        .bind(millis(defaults.visibility_timeout))
        .bind(count(defaults.max_attempts))
        .bind(millis(defaults.backoff.base))
        .bind(millis(defaults.backoff.cap))
        .bind(millis(defaults.retention))
        .bind(defaults.paused)
        .bind(defaults.push.clone().map(Json))
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => row.into_queue(),
            // Another server created it between our select and insert.
            None => self
                .find_queue(name)
                .await?
                .ok_or_else(|| Error::QueueNotFound(name.clone())),
        }
    }

    async fn put_queue(&self, name: &QueueName, settings: &QueueSettings) -> Result<Queue> {
        let row: QueueRow = sqlx::query_as(
            "INSERT INTO dally_queues
                (name, visibility_timeout_ms, max_attempts, backoff_base_ms, backoff_cap_ms,
                 retention_ms, paused, push)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (name) DO UPDATE SET
                visibility_timeout_ms = excluded.visibility_timeout_ms,
                max_attempts = excluded.max_attempts,
                backoff_base_ms = excluded.backoff_base_ms,
                backoff_cap_ms = excluded.backoff_cap_ms,
                retention_ms = excluded.retention_ms,
                paused = excluded.paused,
                push = excluded.push,
                updated_at = now()
             RETURNING *",
        )
        .bind(name.as_str())
        .bind(millis(settings.visibility_timeout))
        .bind(count(settings.max_attempts))
        .bind(millis(settings.backoff.base))
        .bind(millis(settings.backoff.cap))
        .bind(millis(settings.retention))
        .bind(settings.paused)
        .bind(settings.push.clone().map(Json))
        .fetch_one(&self.pool)
        .await?;

        // Resuming a queue makes its messages deliverable again.
        if !settings.paused {
            notify(&self.pool, name).await?;
        }
        row.into_queue()
    }

    async fn get_queue(&self, name: &QueueName) -> Result<Option<QueueSummary>> {
        let Some(queue) = self.find_queue(name).await? else {
            return Ok(None);
        };

        let counts: Vec<(String, i64)> = sqlx::query_as(
            "SELECT status, count(*) FROM dally_messages WHERE queue = $1 GROUP BY status",
        )
        .bind(name.as_str())
        .fetch_all(&self.pool)
        .await?;

        let mut stats = QueueStats::default();
        for (status, count) in counts {
            add_count(&mut stats, &status, count)?;
        }
        Ok(Some(QueueSummary { queue, stats }))
    }

    async fn list_queues(&self) -> Result<Vec<QueueSummary>> {
        let rows: Vec<QueueRow> = sqlx::query_as("SELECT * FROM dally_queues ORDER BY name")
            .fetch_all(&self.pool)
            .await?;
        let counts: Vec<(String, String, i64)> = sqlx::query_as(
            "SELECT queue, status, count(*) FROM dally_messages GROUP BY queue, status",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut summaries = rows
            .into_iter()
            .map(|row| {
                Ok(QueueSummary {
                    queue: row.into_queue()?,
                    stats: QueueStats::default(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        for (queue, status, count) in counts {
            if let Some(summary) = summaries
                .iter_mut()
                .find(|s| s.queue.name.as_str() == queue)
            {
                add_count(&mut summary.stats, &status, count)?;
            }
        }
        Ok(summaries)
    }

    async fn schedule(
        &self,
        queue: &QueueName,
        messages: Vec<NewMessage>,
    ) -> Result<Vec<Scheduled>> {
        let mut tx = self.pool.begin().await?;
        let mut results = Vec::with_capacity(messages.len());

        for message in messages {
            let (at, delay_ms) = split_run_at(message.run_at);
            let inserted: Option<MessageRow> = sqlx::query_as(
                "INSERT INTO dally_messages
                    (id, queue, payload, status, priority, max_attempts, visibility_timeout_ms,
                     idempotency_key, run_at)
                 VALUES ($1, $2, $3, 'scheduled', $4, $5, $6, $7,
                         COALESCE($8, now() + $9::float8 * interval '1 millisecond'))
                 ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL
                 DO NOTHING
                 RETURNING *",
            )
            .bind(message.id.as_uuid())
            .bind(queue.as_str())
            .bind(Json(&message.payload))
            .bind(message.priority)
            .bind(count(message.max_attempts))
            .bind(millis(message.visibility_timeout))
            .bind(message.idempotency_key.as_deref())
            .bind(at)
            .bind(delay_ms)
            .fetch_optional(&mut *tx)
            .await?;

            let result = match inserted {
                Some(row) => Scheduled {
                    message: row.into_message()?,
                    created: true,
                },
                None => {
                    let existing: MessageRow = sqlx::query_as(
                        "SELECT * FROM dally_messages WHERE queue = $1 AND idempotency_key = $2",
                    )
                    .bind(queue.as_str())
                    .bind(message.idempotency_key.as_deref())
                    .fetch_one(&mut *tx)
                    .await?;
                    Scheduled {
                        message: existing.into_message()?,
                        created: false,
                    }
                }
            };
            results.push(result);
        }

        if results.iter().any(|result| result.created) {
            notify(&mut *tx, queue).await?;
        }
        tx.commit().await?;
        Ok(results)
    }

    async fn lease(&self, queue: &QueueName, limit: u32) -> Result<Vec<Leased>> {
        let mut tx = self.pool.begin().await?;
        bury_expired(&mut tx, queue).await?;

        let rows: Vec<MessageRow> = sqlx::query_as(
            "WITH due AS (
                 SELECT id FROM dally_messages
                 WHERE queue = $1
                   AND ((status = 'scheduled' AND run_at <= now())
                     OR (status = 'running' AND lease_expires_at <= now()))
                 ORDER BY priority DESC, run_at, id
                 LIMIT $2
                 FOR UPDATE SKIP LOCKED
             )
             UPDATE dally_messages AS m SET
                 status = 'running',
                 attempts = m.attempts + 1,
                 lease_token = replace(gen_random_uuid()::text, '-', ''),
                 lease_expires_at = now() + m.visibility_timeout_ms * interval '1 millisecond',
                 last_error = CASE WHEN m.status = 'running' THEN 'lease expired'
                                   ELSE m.last_error END,
                 updated_at = now()
             FROM due
             WHERE m.id = due.id
             RETURNING m.*",
        )
        .bind(queue.as_str())
        .bind(i64::from(limit))
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;

        let mut leased = rows
            .into_iter()
            .map(MessageRow::into_leased)
            .collect::<Result<Vec<_>>>()?;
        // RETURNING does not keep the CTE's order.
        leased.sort_by(|a, b| {
            let (a, b) = (&a.message, &b.message);
            b.priority
                .cmp(&a.priority)
                .then(a.run_at.cmp(&b.run_at))
                .then(a.id.cmp(&b.id))
        });
        Ok(leased)
    }

    async fn ack(&self, id: MessageId, lease: &LeaseToken) -> Result<Option<Message>> {
        let row: Option<MessageRow> = sqlx::query_as(
            "UPDATE dally_messages SET
                 status = 'succeeded',
                 lease_token = NULL,
                 lease_expires_at = NULL,
                 finished_at = now(),
                 updated_at = now()
             WHERE id = $1 AND status = 'running' AND lease_token = $2
             RETURNING *",
        )
        .bind(id.as_uuid())
        .bind(lease.as_str())
        .fetch_optional(&self.pool)
        .await?;

        row.map(MessageRow::into_message).transpose()
    }

    async fn nack(
        &self,
        id: MessageId,
        lease: &LeaseToken,
        retry: Retry,
    ) -> Result<Option<Message>> {
        let row: Option<MessageRow> = sqlx::query_as(
            "UPDATE dally_messages SET
                 status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'scheduled' END,
                 run_at = CASE WHEN attempts >= max_attempts THEN run_at
                               ELSE now() + $3::float8 * interval '1 millisecond' END,
                 finished_at = CASE WHEN attempts >= max_attempts THEN now() END,
                 last_error = $4,
                 lease_token = NULL,
                 lease_expires_at = NULL,
                 updated_at = now()
             WHERE id = $1 AND status = 'running' AND lease_token = $2
             RETURNING *",
        )
        .bind(id.as_uuid())
        .bind(lease.as_str())
        .bind(millis(retry.delay) as f64)
        .bind(retry.error.as_deref())
        .fetch_optional(&self.pool)
        .await?;

        row.map(MessageRow::into_message).transpose()
    }

    async fn get_message(&self, id: MessageId) -> Result<Option<Message>> {
        let row: Option<MessageRow> = sqlx::query_as("SELECT * FROM dally_messages WHERE id = $1")
            .bind(id.as_uuid())
            .fetch_optional(&self.pool)
            .await?;

        row.map(MessageRow::into_message).transpose()
    }

    async fn list_messages(&self, filter: &MessageFilter) -> Result<Page<Message>> {
        // Fetch one extra row to learn whether there is another page.
        let rows: Vec<MessageRow> = sqlx::query_as(
            "SELECT * FROM dally_messages
             WHERE ($1::text IS NULL OR queue = $1)
               AND ($2::text IS NULL OR status = $2)
               AND ($3::uuid IS NULL OR id < $3)
             ORDER BY id DESC
             LIMIT $4",
        )
        .bind(filter.queue.as_ref().map(QueueName::as_str))
        .bind(filter.status.map(|status| status.as_str()))
        .bind(filter.before.map(|id| id.as_uuid()))
        .bind(i64::from(filter.limit) + 1)
        .fetch_all(&self.pool)
        .await?;

        let mut items = rows
            .into_iter()
            .map(MessageRow::into_message)
            .collect::<Result<Vec<_>>>()?;

        let next = if items.len() > filter.limit as usize {
            items.truncate(filter.limit as usize);
            items.last().map(|message| message.id)
        } else {
            None
        };
        Ok(Page { items, next })
    }

    async fn cancel(&self, id: MessageId) -> Result<Option<Message>> {
        let row: Option<MessageRow> = sqlx::query_as(
            "UPDATE dally_messages SET
                 status = 'cancelled',
                 finished_at = now(),
                 updated_at = now()
             WHERE id = $1 AND status = 'scheduled'
             RETURNING *",
        )
        .bind(id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;

        row.map(MessageRow::into_message).transpose()
    }

    async fn reschedule(&self, id: MessageId, run_at: RunAt) -> Result<Option<Message>> {
        let (at, delay_ms) = split_run_at(run_at);
        let row: Option<MessageRow> = sqlx::query_as(
            "UPDATE dally_messages SET
                 run_at = COALESCE($2, now() + $3::float8 * interval '1 millisecond'),
                 updated_at = now()
             WHERE id = $1 AND status = 'scheduled'
             RETURNING *",
        )
        .bind(id.as_uuid())
        .bind(at)
        .bind(delay_ms)
        .fetch_optional(&self.pool)
        .await?;

        let message = row.map(MessageRow::into_message).transpose()?;
        if let Some(message) = &message {
            notify(&self.pool, &message.queue).await?;
        }
        Ok(message)
    }

    async fn retry_dead(&self, queue: &QueueName, ids: Option<&[MessageId]>) -> Result<u64> {
        let ids: Option<Vec<Uuid>> = ids.map(|ids| ids.iter().map(MessageId::as_uuid).collect());
        let result = sqlx::query(
            "UPDATE dally_messages SET
                 status = 'scheduled',
                 attempts = 0,
                 run_at = now(),
                 finished_at = NULL,
                 updated_at = now()
             WHERE queue = $1 AND status = 'dead' AND ($2::uuid[] IS NULL OR id = ANY($2))",
        )
        .bind(queue.as_str())
        .bind(ids)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() > 0 {
            notify(&self.pool, queue).await?;
        }
        Ok(result.rows_affected())
    }

    async fn prune(&self) -> Result<u64> {
        let result = sqlx::query(
            "DELETE FROM dally_messages AS m
             USING dally_queues AS q
             WHERE m.queue = q.name
               AND m.finished_at IS NOT NULL
               AND m.finished_at < now() - q.retention_ms * interval '1 millisecond'",
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    fn subscribe(&self, queue: &QueueName) -> Subscription {
        self.notifier.subscribe(queue)
    }

    async fn now(&self) -> Result<DateTime<Utc>> {
        let (now,): (DateTime<Utc>,) = sqlx::query_as("SELECT now()").fetch_one(&self.pool).await?;
        Ok(now)
    }
}

impl PostgresStorage {
    async fn find_queue(&self, name: &QueueName) -> Result<Option<Queue>> {
        let row: Option<QueueRow> = sqlx::query_as("SELECT * FROM dally_queues WHERE name = $1")
            .bind(name.as_str())
            .fetch_optional(&self.pool)
            .await?;

        row.map(QueueRow::into_queue).transpose()
    }
}

/// Moves running messages whose lease expired on their last attempt to
/// `dead`. Runs before leasing, so they are never handed out again.
async fn bury_expired(tx: &mut Transaction<'_, Postgres>, queue: &QueueName) -> Result<()> {
    sqlx::query(
        "UPDATE dally_messages SET
             status = 'dead',
             last_error = 'lease expired',
             lease_token = NULL,
             lease_expires_at = NULL,
             finished_at = now(),
             updated_at = now()
         WHERE queue = $1
           AND status = 'running'
           AND lease_expires_at <= now()
           AND attempts >= max_attempts",
    )
    .bind(queue.as_str())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Tells every server's listener that `queue` has new due messages. Inside a
/// transaction, the notification is sent on commit.
async fn notify<'e, E>(executor: E, queue: &QueueName) -> Result<()>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(CHANNEL)
        .bind(queue.as_str())
        .execute(executor)
        .await?;
    Ok(())
}

/// Splits a [`RunAt`] into the two parameters the SQL expects: an absolute
/// time, or a delay from the database's `now()`.
fn split_run_at(run_at: RunAt) -> (Option<DateTime<Utc>>, f64) {
    match run_at {
        RunAt::Now => (None, 0.0),
        RunAt::After(delay) => (None, millis(delay) as f64),
        RunAt::At(at) => (Some(at), 0.0),
    }
}

fn add_count(stats: &mut QueueStats, status: &str, count: i64) -> Result<()> {
    let count = u64::try_from(count).unwrap_or_default();
    match status.parse::<Status>()? {
        Status::Scheduled => stats.scheduled = count,
        Status::Running => stats.running = count,
        Status::Succeeded => stats.succeeded = count,
        Status::Cancelled => stats.cancelled = count,
        Status::Dead => stats.dead = count,
    }
    Ok(())
}

fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn from_millis(millis: i64) -> Duration {
    Duration::from_millis(u64::try_from(millis).unwrap_or_default())
}

fn count(n: u32) -> i32 {
    i32::try_from(n).unwrap_or(i32::MAX)
}

fn from_count(n: i32) -> u32 {
    u32::try_from(n).unwrap_or_default()
}

#[derive(FromRow)]
struct QueueRow {
    name: String,
    visibility_timeout_ms: i64,
    max_attempts: i32,
    backoff_base_ms: i64,
    backoff_cap_ms: i64,
    retention_ms: i64,
    paused: bool,
    push: Option<Json<PushSettings>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl QueueRow {
    fn into_queue(self) -> Result<Queue> {
        Ok(Queue {
            name: QueueName::new(self.name)?,
            settings: QueueSettings {
                visibility_timeout: from_millis(self.visibility_timeout_ms),
                max_attempts: from_count(self.max_attempts),
                backoff: Backoff {
                    base: from_millis(self.backoff_base_ms),
                    cap: from_millis(self.backoff_cap_ms),
                },
                retention: from_millis(self.retention_ms),
                paused: self.paused,
                push: self.push.map(|Json(push)| push),
            },
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

#[derive(FromRow)]
struct MessageRow {
    id: Uuid,
    queue: String,
    payload: Json<serde_json::Value>,
    status: String,
    priority: i32,
    attempts: i32,
    max_attempts: i32,
    visibility_timeout_ms: i64,
    idempotency_key: Option<String>,
    run_at: DateTime<Utc>,
    lease_token: Option<String>,
    lease_expires_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
}

impl MessageRow {
    fn into_message(self) -> Result<Message> {
        Ok(Message {
            id: MessageId::from_uuid(self.id),
            queue: QueueName::new(self.queue)?,
            payload: self.payload.0,
            status: self.status.parse()?,
            priority: self.priority,
            attempts: from_count(self.attempts),
            max_attempts: from_count(self.max_attempts),
            visibility_timeout: from_millis(self.visibility_timeout_ms),
            idempotency_key: self.idempotency_key,
            run_at: self.run_at,
            lease_expires_at: self.lease_expires_at,
            last_error: self.last_error,
            created_at: self.created_at,
            updated_at: self.updated_at,
            finished_at: self.finished_at,
        })
    }

    fn into_leased(mut self) -> Result<Leased> {
        let lease = self
            .lease_token
            .take()
            .map(LeaseToken::from)
            .ok_or_else(|| Error::invalid("leased message has no lease token"))?;
        Ok(Leased {
            message: self.into_message()?,
            lease,
        })
    }
}
