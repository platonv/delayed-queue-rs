//! The SQLite backend, for local development and small deployments.
//!
//! # One server process per database
//!
//! Only one process may use a database file at a time, for two reasons:
//! long-poll notifications travel through an in-process channel, so a second
//! process would never wake the first one's waiting polls, and timestamps come
//! from the process clock (see below), which two processes need not agree on.
//!
//! [`SqliteStorage`] enforces this with an exclusive lock on a file next to
//! the database, named after it with `.lock` appended (`dally.sqlite.lock` for
//! `dally.sqlite`). Connecting while another process holds the lock fails with
//! [`Error::InvalidInput`]. The operating system releases the lock when the
//! storage is dropped or the process exits, even after a crash, so a stale
//! lock file never blocks a restart. The file itself is left in place.
//! In-memory databases (`sqlite::memory:`) are private to their process and
//! take no lock.
//!
//! # Concurrency
//!
//! The database runs in WAL mode, so reads go through a small pool of
//! read-only connections and never wait for writes. All writes go through one
//! connection, so concurrent writers queue up inside the process rather than
//! retrying against SQLite's file lock. Transactions that read before they
//! write, like leasing, start with `BEGIN IMMEDIATE`: they take the write lock
//! before looking at anything, so two pollers can never pick the same due
//! message.
//!
//! # Time
//!
//! Timestamps come from the process clock. Other backends use the storage
//! clock so that several servers agree on what is due; with a single process
//! there is only one clock to begin with, and SQLite's own date functions read
//! that same system clock anyway. Timestamps are stored as integer
//! milliseconds since the Unix epoch, which sort and compare correctly as
//! plain numbers, unlike SQLite's text dates. Sub-millisecond precision is
//! dropped.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};
use sqlx::types::Json;
use sqlx::{FromRow, Sqlite, Transaction};
use uuid::fmt::Hyphenated;

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

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/sqlite");

/// How long a connection waits for a lock held by someone else, such as the
/// `sqlite3` shell or a backup, before giving up.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Read connections. Reads are short, so a few are plenty.
const READERS: u32 = 4;

/// Starts a transaction that holds the write lock from its first statement.
const BEGIN_IMMEDIATE: &str = "BEGIN IMMEDIATE";

/// Stores queues and messages in a SQLite database.
pub struct SqliteStorage {
    writer: SqlitePool,
    /// For a file database, read-only connections next to the writer. For an
    /// in-memory database, the writer pool itself.
    reader: SqlitePool,
    notifier: Notifier,
    /// Held for as long as the storage lives. Dropping the file releases the
    /// lock.
    _lock: Option<File>,
}

impl SqliteStorage {
    /// Connects using a `sqlite:` URL: `sqlite://relative/path.sqlite`,
    /// `sqlite:///absolute/path.sqlite` or `sqlite::memory:`. A database file
    /// that does not exist yet is created.
    ///
    /// Fails with [`Error::InvalidInput`] if another process is using the
    /// database file.
    pub async fn connect(url: &str, options: &ConnectOptions) -> Result<Self> {
        let connect_options: SqliteConnectOptions = url.parse()?;
        if is_in_memory(url) {
            Self::open_in_memory(connect_options, options).await
        } else {
            Self::open_file(connect_options, options).await
        }
    }

    /// Opens the database file at `path`, creating it if it does not exist.
    ///
    /// Fails with [`Error::InvalidInput`] if another process is using it.
    pub async fn open(path: impl AsRef<Path>, options: &ConnectOptions) -> Result<Self> {
        Self::open_file(SqliteConnectOptions::new().filename(path), options).await
    }

    async fn open_file(
        connect_options: SqliteConnectOptions,
        options: &ConnectOptions,
    ) -> Result<Self> {
        let connect_options = connect_options
            .foreign_keys(true)
            .busy_timeout(BUSY_TIMEOUT);
        // Lock before touching the database, so a second server never gets
        // as far as running migrations.
        let lock = lock_database(connect_options.get_filename())?;

        let writer = single_connection_pool(
            connect_options
                .clone()
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal),
        )
        .await?;
        if options.migrate {
            MIGRATOR.run(&writer).await.map_err(Error::storage)?;
        }

        // Readers connect after the writer, which has created the file and
        // switched it to WAL mode.
        let reader = SqlitePoolOptions::new()
            .max_connections(READERS)
            .connect_with(connect_options.read_only(true))
            .await?;

        Ok(Self {
            writer,
            reader,
            notifier: Notifier::new(),
            _lock: Some(lock),
        })
    }

    async fn open_in_memory(
        connect_options: SqliteConnectOptions,
        options: &ConnectOptions,
    ) -> Result<Self> {
        // One connection serves reads and writes. More connections would
        // need SQLite's shared cache, whose lock conflicts are not retried by
        // the busy timeout.
        let pool = single_connection_pool(connect_options.foreign_keys(true)).await?;
        if options.migrate {
            MIGRATOR.run(&pool).await.map_err(Error::storage)?;
        }

        Ok(Self {
            writer: pool.clone(),
            reader: pool,
            notifier: Notifier::new(),
            _lock: None,
        })
    }
}

/// A pool that opens one connection and keeps it forever. An in-memory
/// database disappears with its last connection, and for a file the one
/// writer is opened once and reused.
async fn single_connection_pool(connect_options: SqliteConnectOptions) -> Result<SqlitePool> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .min_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect_with(connect_options)
        .await?;
    Ok(pool)
}

/// Whether a `sqlite:` URL names an in-memory database, following the same
/// rules as [`SqliteConnectOptions`]'s URL parser.
fn is_in_memory(url: &str) -> bool {
    let rest = url
        .trim_start_matches("sqlite://")
        .trim_start_matches("sqlite:");
    let (database, params) = rest.split_once('?').unwrap_or((rest, ""));
    database == ":memory:" || params.split('&').any(|param| param == "mode=memory")
}

/// Takes the exclusive lock that keeps a second server process away from the
/// database at `database`.
fn lock_database(database: &Path) -> Result<File> {
    let path = lock_path(database);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .map_err(|err| {
            Error::Storage(format!("cannot open lock file {}: {err}", path.display()).into())
        })?;

    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(Error::invalid(format!(
            "SQLite database {} is already in use by another dally server \
             (lock file {} is held); SQLite supports one server process per database, \
             use PostgreSQL to run several",
            database.display(),
            path.display()
        ))),
        Err(TryLockError::Error(err)) => Err(Error::Storage(
            format!("cannot lock {}: {err}", path.display()).into(),
        )),
    }
}

fn lock_path(database: &Path) -> PathBuf {
    let mut path = database.as_os_str().to_owned();
    path.push(".lock");
    PathBuf::from(path)
}

#[async_trait]
impl Storage for SqliteStorage {
    async fn ensure_queue(&self, name: &QueueName, defaults: &QueueSettings) -> Result<Queue> {
        // Most calls find the queue, so look before trying to insert.
        if let Some(queue) = self.find_queue(name).await? {
            return Ok(queue);
        }

        let row: Option<QueueRow> = sqlx::query_as(
            "INSERT INTO dally_queues
                (name, visibility_timeout_ms, max_attempts, backoff_base_ms, backoff_cap_ms,
                 retention_ms, paused, push, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9)
             ON CONFLICT (name) DO NOTHING
             RETURNING *",
        )
        .bind(name.as_str())
        .bind(millis(defaults.visibility_timeout))
        .bind(i64::from(defaults.max_attempts))
        .bind(millis(defaults.backoff.base))
        .bind(millis(defaults.backoff.cap))
        .bind(millis(defaults.retention))
        .bind(defaults.paused)
        .bind(defaults.push.clone().map(Json))
        .bind(now_millis())
        .fetch_optional(&self.writer)
        .await?;

        match row {
            Some(row) => row.into_queue(),
            // Another task created it between our select and insert.
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
                 retention_ms, paused, push, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9)
             ON CONFLICT (name) DO UPDATE SET
                visibility_timeout_ms = excluded.visibility_timeout_ms,
                max_attempts = excluded.max_attempts,
                backoff_base_ms = excluded.backoff_base_ms,
                backoff_cap_ms = excluded.backoff_cap_ms,
                retention_ms = excluded.retention_ms,
                paused = excluded.paused,
                push = excluded.push,
                updated_at = excluded.updated_at
             RETURNING *",
        )
        .bind(name.as_str())
        .bind(millis(settings.visibility_timeout))
        .bind(i64::from(settings.max_attempts))
        .bind(millis(settings.backoff.base))
        .bind(millis(settings.backoff.cap))
        .bind(millis(settings.retention))
        .bind(settings.paused)
        .bind(settings.push.clone().map(Json))
        .bind(now_millis())
        .fetch_one(&self.writer)
        .await?;

        // Resuming a queue makes its messages deliverable again.
        if !settings.paused {
            self.notifier.notify(name);
        }
        row.into_queue()
    }

    async fn get_queue(&self, name: &QueueName) -> Result<Option<QueueSummary>> {
        let Some(queue) = self.find_queue(name).await? else {
            return Ok(None);
        };

        let counts: Vec<(String, i64)> = sqlx::query_as(
            "SELECT status, count(*) FROM dally_messages WHERE queue = ?1 GROUP BY status",
        )
        .bind(name.as_str())
        .fetch_all(&self.reader)
        .await?;

        let mut stats = QueueStats::default();
        for (status, count) in counts {
            add_count(&mut stats, &status, count)?;
        }
        Ok(Some(QueueSummary { queue, stats }))
    }

    async fn list_queues(&self) -> Result<Vec<QueueSummary>> {
        let rows: Vec<QueueRow> = sqlx::query_as("SELECT * FROM dally_queues ORDER BY name")
            .fetch_all(&self.reader)
            .await?;
        let counts: Vec<(String, String, i64)> = sqlx::query_as(
            "SELECT queue, status, count(*) FROM dally_messages GROUP BY queue, status",
        )
        .fetch_all(&self.reader)
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
        let mut tx = self.writer.begin_with(BEGIN_IMMEDIATE).await?;
        let now = now_millis();
        let mut results = Vec::with_capacity(messages.len());

        for message in messages {
            let inserted: Option<MessageRow> = sqlx::query_as(
                "INSERT INTO dally_messages
                    (id, queue, payload, status, priority, max_attempts, visibility_timeout_ms,
                     idempotency_key, run_at, created_at, updated_at)
                 VALUES (?1, ?2, ?3, 'scheduled', ?4, ?5, ?6, ?7, ?8, ?9, ?9)
                 ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL
                 DO NOTHING
                 RETURNING *",
            )
            .bind(message.id.as_uuid().hyphenated())
            .bind(queue.as_str())
            .bind(Json(&message.payload))
            .bind(message.priority)
            .bind(i64::from(message.max_attempts))
            .bind(millis(message.visibility_timeout))
            .bind(message.idempotency_key.as_deref())
            .bind(run_at_millis(message.run_at, now))
            .bind(now)
            .fetch_optional(&mut *tx)
            .await?;

            let result = match inserted {
                Some(row) => Scheduled {
                    message: row.into_message()?,
                    created: true,
                },
                None => {
                    let existing: MessageRow = sqlx::query_as(
                        "SELECT * FROM dally_messages WHERE queue = ?1 AND idempotency_key = ?2",
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
        tx.commit().await?;

        if results.iter().any(|result| result.created) {
            self.notifier.notify(queue);
        }
        Ok(results)
    }

    async fn lease(&self, queue: &QueueName, limit: u32) -> Result<Vec<Leased>> {
        let mut tx = self.writer.begin_with(BEGIN_IMMEDIATE).await?;
        let now = now_millis();
        bury_expired(&mut tx, queue, now).await?;

        let due: Vec<(Hyphenated,)> = sqlx::query_as(
            "SELECT id FROM dally_messages
             WHERE queue = ?1
               AND ((status = 'scheduled' AND run_at <= ?2)
                 OR (status = 'running' AND lease_expires_at <= ?2))
             ORDER BY priority DESC, run_at, id
             LIMIT ?3",
        )
        .bind(queue.as_str())
        .bind(now)
        .bind(i64::from(limit))
        .fetch_all(&mut *tx)
        .await?;

        // One update per message, because each gets its own token. The write
        // lock is held throughout, so nothing can take these rows meanwhile.
        let mut leased = Vec::with_capacity(due.len());
        for (id,) in due {
            let row: MessageRow = sqlx::query_as(
                "UPDATE dally_messages SET
                     status = 'running',
                     attempts = attempts + 1,
                     lease_token = ?2,
                     lease_expires_at = ?3 + visibility_timeout_ms,
                     last_error = CASE WHEN status = 'running' THEN 'lease expired'
                                       ELSE last_error END,
                     updated_at = ?3
                 WHERE id = ?1
                 RETURNING *",
            )
            .bind(id)
            .bind(LeaseToken::generate().as_str())
            .bind(now)
            .fetch_one(&mut *tx)
            .await?;
            leased.push(row.into_leased()?);
        }
        tx.commit().await?;

        Ok(leased)
    }

    async fn ack(&self, id: MessageId, lease: &LeaseToken) -> Result<Option<Message>> {
        let row: Option<MessageRow> = sqlx::query_as(
            "UPDATE dally_messages SET
                 status = 'succeeded',
                 lease_token = NULL,
                 lease_expires_at = NULL,
                 finished_at = ?3,
                 updated_at = ?3
             WHERE id = ?1 AND status = 'running' AND lease_token = ?2
             RETURNING *",
        )
        .bind(id.as_uuid().hyphenated())
        .bind(lease.as_str())
        .bind(now_millis())
        .fetch_optional(&self.writer)
        .await?;

        row.map(MessageRow::into_message).transpose()
    }

    async fn nack(
        &self,
        id: MessageId,
        lease: &LeaseToken,
        retry: Retry,
    ) -> Result<Option<Message>> {
        let now = now_millis();
        let row: Option<MessageRow> = sqlx::query_as(
            "UPDATE dally_messages SET
                 status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'scheduled' END,
                 run_at = CASE WHEN attempts >= max_attempts THEN run_at ELSE ?3 END,
                 finished_at = CASE WHEN attempts >= max_attempts THEN ?5 END,
                 last_error = ?4,
                 lease_token = NULL,
                 lease_expires_at = NULL,
                 updated_at = ?5
             WHERE id = ?1 AND status = 'running' AND lease_token = ?2
             RETURNING *",
        )
        .bind(id.as_uuid().hyphenated())
        .bind(lease.as_str())
        .bind(now.saturating_add(millis(retry.delay)))
        .bind(retry.error.as_deref())
        .bind(now)
        .fetch_optional(&self.writer)
        .await?;

        row.map(MessageRow::into_message).transpose()
    }

    async fn get_message(&self, id: MessageId) -> Result<Option<Message>> {
        let row: Option<MessageRow> = sqlx::query_as("SELECT * FROM dally_messages WHERE id = ?1")
            .bind(id.as_uuid().hyphenated())
            .fetch_optional(&self.reader)
            .await?;

        row.map(MessageRow::into_message).transpose()
    }

    async fn list_messages(&self, filter: &MessageFilter) -> Result<Page<Message>> {
        // Fetch one extra row to learn whether there is another page.
        let rows: Vec<MessageRow> = sqlx::query_as(
            "SELECT * FROM dally_messages
             WHERE (?1 IS NULL OR queue = ?1)
               AND (?2 IS NULL OR status = ?2)
               AND (?3 IS NULL OR id < ?3)
             ORDER BY id DESC
             LIMIT ?4",
        )
        .bind(filter.queue.as_ref().map(QueueName::as_str))
        .bind(filter.status.map(|status| status.as_str()))
        .bind(filter.before.map(|id| id.as_uuid().hyphenated()))
        .bind(i64::from(filter.limit) + 1)
        .fetch_all(&self.reader)
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
                 finished_at = ?2,
                 updated_at = ?2
             WHERE id = ?1 AND status = 'scheduled'
             RETURNING *",
        )
        .bind(id.as_uuid().hyphenated())
        .bind(now_millis())
        .fetch_optional(&self.writer)
        .await?;

        row.map(MessageRow::into_message).transpose()
    }

    async fn reschedule(&self, id: MessageId, run_at: RunAt) -> Result<Option<Message>> {
        let now = now_millis();
        let row: Option<MessageRow> = sqlx::query_as(
            "UPDATE dally_messages SET
                 run_at = ?2,
                 updated_at = ?3
             WHERE id = ?1 AND status = 'scheduled'
             RETURNING *",
        )
        .bind(id.as_uuid().hyphenated())
        .bind(run_at_millis(run_at, now))
        .bind(now)
        .fetch_optional(&self.writer)
        .await?;

        let message = row.map(MessageRow::into_message).transpose()?;
        if let Some(message) = &message {
            self.notifier.notify(&message.queue);
        }
        Ok(message)
    }

    async fn retry_dead(&self, queue: &QueueName, ids: Option<&[MessageId]>) -> Result<u64> {
        // SQLite has no array parameters, so the ids travel as a JSON array.
        let result = sqlx::query(
            "UPDATE dally_messages SET
                 status = 'scheduled',
                 attempts = 0,
                 run_at = ?3,
                 finished_at = NULL,
                 updated_at = ?3
             WHERE queue = ?1
               AND status = 'dead'
               AND (?2 IS NULL OR id IN (SELECT value FROM json_each(?2)))",
        )
        .bind(queue.as_str())
        .bind(ids.map(Json))
        .bind(now_millis())
        .execute(&self.writer)
        .await?;

        if result.rows_affected() > 0 {
            self.notifier.notify(queue);
        }
        Ok(result.rows_affected())
    }

    async fn prune(&self) -> Result<u64> {
        let result = sqlx::query(
            "DELETE FROM dally_messages
             WHERE finished_at IS NOT NULL
               AND finished_at < ?1 - (SELECT retention_ms FROM dally_queues
                                       WHERE name = dally_messages.queue)",
        )
        .bind(now_millis())
        .execute(&self.writer)
        .await?;

        Ok(result.rows_affected())
    }

    fn subscribe(&self, queue: &QueueName) -> Subscription {
        self.notifier.subscribe(queue)
    }

    async fn now(&self) -> Result<DateTime<Utc>> {
        Ok(Utc::now())
    }
}

impl SqliteStorage {
    async fn find_queue(&self, name: &QueueName) -> Result<Option<Queue>> {
        let row: Option<QueueRow> = sqlx::query_as("SELECT * FROM dally_queues WHERE name = ?1")
            .bind(name.as_str())
            .fetch_optional(&self.reader)
            .await?;

        row.map(QueueRow::into_queue).transpose()
    }
}

/// Moves running messages whose lease expired on their last attempt to
/// `dead`. Runs before leasing, so they are never handed out again.
async fn bury_expired(
    tx: &mut Transaction<'_, Sqlite>,
    queue: &QueueName,
    now: i64,
) -> Result<()> {
    sqlx::query(
        "UPDATE dally_messages SET
             status = 'dead',
             last_error = 'lease expired',
             lease_token = NULL,
             lease_expires_at = NULL,
             finished_at = ?2,
             updated_at = ?2
         WHERE queue = ?1
           AND status = 'running'
           AND lease_expires_at <= ?2
           AND attempts >= max_attempts",
    )
    .bind(queue.as_str())
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
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

fn now_millis() -> i64 {
    Utc::now().timestamp_millis()
}

fn run_at_millis(run_at: RunAt, now: i64) -> i64 {
    match run_at {
        RunAt::Now => now,
        RunAt::After(delay) => now.saturating_add(millis(delay)),
        RunAt::At(at) => at.timestamp_millis(),
    }
}

fn from_timestamp(millis: i64) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_millis(millis)
        .ok_or_else(|| Error::invalid(format!("stored timestamp {millis} is out of range")))
}

fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn from_millis(millis: i64) -> Duration {
    Duration::from_millis(u64::try_from(millis).unwrap_or_default())
}

fn from_count(n: i64) -> u32 {
    u32::try_from(n).unwrap_or_default()
}

#[derive(FromRow)]
struct QueueRow {
    name: String,
    visibility_timeout_ms: i64,
    max_attempts: i64,
    backoff_base_ms: i64,
    backoff_cap_ms: i64,
    retention_ms: i64,
    paused: bool,
    push: Option<Json<PushSettings>>,
    created_at: i64,
    updated_at: i64,
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
            created_at: from_timestamp(self.created_at)?,
            updated_at: from_timestamp(self.updated_at)?,
        })
    }
}

#[derive(FromRow)]
struct MessageRow {
    id: Hyphenated,
    queue: String,
    payload: Json<serde_json::Value>,
    status: String,
    priority: i32,
    attempts: i64,
    max_attempts: i64,
    visibility_timeout_ms: i64,
    idempotency_key: Option<String>,
    run_at: i64,
    lease_token: Option<String>,
    lease_expires_at: Option<i64>,
    last_error: Option<String>,
    created_at: i64,
    updated_at: i64,
    finished_at: Option<i64>,
}

impl MessageRow {
    fn into_message(self) -> Result<Message> {
        Ok(Message {
            id: MessageId::from_uuid(self.id.into_uuid()),
            queue: QueueName::new(self.queue)?,
            payload: self.payload.0,
            status: self.status.parse()?,
            priority: self.priority,
            attempts: from_count(self.attempts),
            max_attempts: from_count(self.max_attempts),
            visibility_timeout: from_millis(self.visibility_timeout_ms),
            idempotency_key: self.idempotency_key,
            run_at: from_timestamp(self.run_at)?,
            lease_expires_at: self.lease_expires_at.map(from_timestamp).transpose()?,
            last_error: self.last_error,
            created_at: from_timestamp(self.created_at)?,
            updated_at: from_timestamp(self.updated_at)?,
            finished_at: self.finished_at.map(from_timestamp).transpose()?,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognises_in_memory_urls() {
        assert!(is_in_memory("sqlite::memory:"));
        assert!(is_in_memory("sqlite://:memory:"));
        assert!(is_in_memory("sqlite://shared?mode=memory&cache=shared"));
        assert!(!is_in_memory("sqlite://dally.sqlite"));
        assert!(!is_in_memory("sqlite:///var/lib/dally/dally.sqlite?mode=rwc"));
    }

    #[test]
    fn lock_file_sits_next_to_the_database() {
        assert_eq!(
            lock_path(Path::new("/var/lib/dally/dally.sqlite")),
            Path::new("/var/lib/dally/dally.sqlite.lock")
        );
        assert_eq!(lock_path(Path::new("dally")), Path::new("dally.lock"));
    }
}
