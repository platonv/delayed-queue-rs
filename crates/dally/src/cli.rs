//! The client subcommands. They talk to a running server through
//! `dally-client`, never to storage directly.

use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::{Args, Subcommand};
use dally_client::{Client, Message, MessageFilter, MessageId, QueueSummary, Status};
use serde::Serialize;

/// Where the server is and how to authenticate.
#[derive(Debug, Args)]
pub struct Connection {
    /// The server's base URL.
    #[arg(
        long,
        global = true,
        env = "DALLY_SERVER",
        default_value = "http://127.0.0.1:7070"
    )]
    server: String,

    /// API key, sent as a bearer token.
    #[arg(long, global = true, env = "DALLY_API_KEY", hide_env_values = true)]
    api_key: Option<String>,

    /// Print JSON instead of tables.
    #[arg(long, global = true)]
    json: bool,
}

#[derive(Debug, Subcommand)]
pub enum QueuesCommand {
    /// List queues with their message counts.
    Ls,
    /// Show a queue's settings and message counts.
    Show { name: String },
    /// Stop delivering messages from a queue. New messages are still accepted.
    Pause { name: String },
    /// Start delivering messages from a paused queue again.
    Resume { name: String },
}

#[derive(Debug, Args)]
pub struct ScheduleArgs {
    /// The queue to schedule into. It is created if it does not exist.
    queue: String,

    /// The payload, as JSON.
    #[arg(long)]
    payload: String,

    /// Run after this delay, such as `10m` or `1h30m`.
    #[arg(long, value_parser = humantime::parse_duration, conflicts_with = "run_at")]
    delay: Option<Duration>,

    /// Run at this time (RFC 3339, such as `2026-12-01T09:00:00Z`).
    #[arg(long)]
    run_at: Option<DateTime<Utc>>,

    /// Scheduling twice with the same key creates one message.
    #[arg(long)]
    key: Option<String>,

    /// Higher priorities are delivered first.
    #[arg(long, default_value_t = 0, allow_negative_numbers = true)]
    priority: i32,
}

#[derive(Debug, Subcommand)]
pub enum MessagesCommand {
    /// List messages, newest first.
    Ls {
        /// Only messages in this queue.
        #[arg(long)]
        queue: Option<String>,
        /// Only messages in this status: scheduled, running, succeeded,
        /// cancelled or dead.
        #[arg(long, value_parser = parse_status)]
        status: Option<Status>,
        /// At most this many messages.
        #[arg(long, default_value_t = 50)]
        limit: u32,
    },
}

#[derive(Debug, Subcommand)]
pub enum DeadCommand {
    /// Give dead messages a fresh set of attempts. Without ids, retries every
    /// dead message in the queue.
    Retry { queue: String, ids: Vec<MessageId> },
}

fn parse_status(value: &str) -> Result<Status, String> {
    value.parse::<Status>().map_err(|err| err.to_string())
}

impl Connection {
    /// The server's base URL.
    #[cfg(feature = "ui")]
    pub fn server(&self) -> &str {
        &self.server
    }

    fn client(&self) -> Result<Client> {
        let mut builder = Client::builder(&self.server);
        if let Some(key) = &self.api_key {
            builder = builder.api_key(key);
        }
        builder.build().context("invalid --server URL")
    }

    /// Prints `value` as JSON, or as text produced by `human`.
    fn print<T: Serialize>(&self, value: &T, human: impl FnOnce(&T) -> String) -> Result<()> {
        if self.json {
            println!("{}", serde_json::to_string_pretty(value)?);
        } else {
            print!("{}", human(value));
        }
        Ok(())
    }
}

pub async fn queues(connection: &Connection, command: QueuesCommand) -> Result<()> {
    let client = connection.client()?;
    match command {
        QueuesCommand::Ls => {
            let queues = client.list_queues().await?;
            connection.print(&queues, |queues| queue_table(queues))
        }
        QueuesCommand::Show { name } => {
            let summary = client.get_queue(&name).await?;
            connection.print(&summary, queue_details)
        }
        QueuesCommand::Pause { name } => {
            let queue = client.pause(&name).await?;
            connection.print(&queue, |queue| format!("paused {}\n", queue.name))
        }
        QueuesCommand::Resume { name } => {
            let queue = client.resume(&name).await?;
            connection.print(&queue, |queue| format!("resumed {}\n", queue.name))
        }
    }
}

pub async fn schedule(connection: &Connection, args: ScheduleArgs) -> Result<()> {
    let payload: serde_json::Value =
        serde_json::from_str(&args.payload).context("--payload is not valid JSON")?;

    let client = connection.client()?;
    let mut request = client
        .schedule(&args.queue, &payload)
        .priority(args.priority);
    if let Some(delay) = args.delay {
        request = request.delay(delay);
    }
    if let Some(at) = args.run_at {
        request = request.run_at(at);
    }
    if let Some(key) = args.key {
        request = request.idempotency_key(key);
    }

    let scheduled = request.send().await?;
    connection.print(&scheduled, |scheduled| {
        let verb = if scheduled.created {
            "scheduled"
        } else {
            "already scheduled"
        };
        let message = &scheduled.message;
        format!(
            "{verb} {} in {}, due {}\n",
            message.id, message.queue, message.run_at
        )
    })
}

pub async fn inspect(connection: &Connection, id: MessageId) -> Result<()> {
    let message = connection.client()?.get_message(id).await?;
    connection.print(&message, message_details)
}

pub async fn messages(connection: &Connection, command: MessagesCommand) -> Result<()> {
    let MessagesCommand::Ls {
        queue,
        status,
        limit,
    } = command;
    let filter = MessageFilter {
        queue: queue.map(dally_client::QueueName::new).transpose()?,
        status,
        before: None,
        limit,
    };
    let page = connection.client()?.list_messages(&filter).await?;
    connection.print(&page.items, |messages| message_table(messages))
}

pub async fn dead(connection: &Connection, command: DeadCommand) -> Result<()> {
    let DeadCommand::Retry { queue, ids } = command;
    let ids = (!ids.is_empty()).then_some(ids.as_slice());
    let retried = connection.client()?.retry_dead(&queue, ids).await?;
    connection.print(&serde_json::json!({ "retried": retried }), |_| {
        format!("retried {retried} dead messages in {queue}\n")
    })
}

fn queue_table(queues: &[QueueSummary]) -> String {
    let rows = queues.iter().map(|summary| {
        let stats = summary.stats;
        vec![
            summary.queue.name.to_string(),
            stats.scheduled.to_string(),
            stats.running.to_string(),
            stats.succeeded.to_string(),
            stats.dead.to_string(),
            if summary.queue.settings.paused {
                "paused"
            } else {
                ""
            }
            .to_owned(),
        ]
    });
    table(
        &["QUEUE", "SCHEDULED", "RUNNING", "SUCCEEDED", "DEAD", ""],
        rows,
    )
}

fn queue_details(summary: &QueueSummary) -> String {
    let settings = &summary.queue.settings;
    let stats = summary.stats;
    let duration = |d: Duration| humantime::format_duration(d).to_string();
    let push = settings
        .push
        .as_ref()
        .map_or("none".to_owned(), |push| push.url.clone());

    fields(&[
        ("queue", summary.queue.name.to_string()),
        ("paused", settings.paused.to_string()),
        ("visibility timeout", duration(settings.visibility_timeout)),
        ("max attempts", settings.max_attempts.to_string()),
        (
            "backoff",
            format!(
                "{} up to {}",
                duration(settings.backoff.base),
                duration(settings.backoff.cap)
            ),
        ),
        ("retention", duration(settings.retention)),
        ("push", push),
        ("scheduled", stats.scheduled.to_string()),
        ("running", stats.running.to_string()),
        ("succeeded", stats.succeeded.to_string()),
        ("cancelled", stats.cancelled.to_string()),
        ("dead", stats.dead.to_string()),
    ])
}

fn message_table(messages: &[Message]) -> String {
    let rows = messages.iter().map(|message| {
        vec![
            message.id.to_string(),
            message.queue.to_string(),
            message.status.to_string(),
            format!("{}/{}", message.attempts, message.max_attempts),
            message.run_at.format("%Y-%m-%d %H:%M:%S").to_string(),
        ]
    });
    table(&["ID", "QUEUE", "STATUS", "ATTEMPTS", "RUN AT (UTC)"], rows)
}

fn message_details(message: &Message) -> String {
    let optional = |value: Option<String>| value.unwrap_or_else(|| "-".to_owned());
    let mut text = fields(&[
        ("id", message.id.to_string()),
        ("queue", message.queue.to_string()),
        ("status", message.status.to_string()),
        (
            "attempts",
            format!("{} of {}", message.attempts, message.max_attempts),
        ),
        ("priority", message.priority.to_string()),
        ("run at", message.run_at.to_rfc3339()),
        ("idempotency key", optional(message.idempotency_key.clone())),
        ("last error", optional(message.last_error.clone())),
        ("created at", message.created_at.to_rfc3339()),
        (
            "finished at",
            optional(message.finished_at.map(|at| at.to_rfc3339())),
        ),
    ]);
    let payload = serde_json::to_string_pretty(&message.payload).unwrap_or_default();
    text.push_str(&format!("payload:\n{payload}\n"));
    text
}

/// Lines of `name: value`, with the values lined up.
fn fields(fields: &[(&str, String)]) -> String {
    let width = fields.iter().map(|(name, _)| name.len()).max().unwrap_or(0) + 1;
    fields
        .iter()
        .map(|(name, value)| format!("{:width$} {value}\n", format!("{name}:")))
        .collect()
}

/// A plain text table with left-aligned columns separated by two spaces.
fn table(headers: &[&str], rows: impl Iterator<Item = Vec<String>>) -> String {
    let rows: Vec<Vec<String>> = rows.collect();
    let mut widths: Vec<usize> = headers.iter().map(|header| header.len()).collect();
    for row in &rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.len());
        }
    }

    let line = |cells: Vec<&str>| {
        let padded: Vec<String> = cells
            .iter()
            .zip(&widths)
            .map(|(cell, width)| format!("{cell:width$}"))
            .collect();
        format!("{}\n", padded.join("  ").trim_end())
    };

    let mut text = line(headers.to_vec());
    for row in &rows {
        text.push_str(&line(row.iter().map(String::as_str).collect()));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tables_align_columns() {
        let rows = vec![
            vec!["emails".to_owned(), "3".to_owned()],
            vec!["reports-nightly".to_owned(), "12".to_owned()],
        ];
        assert_eq!(
            table(&["QUEUE", "SCHEDULED"], rows.into_iter()),
            "QUEUE            SCHEDULED\n\
             emails           3\n\
             reports-nightly  12\n"
        );
    }

    #[test]
    fn fields_align_values() {
        let text = fields(&[("id", "1".to_owned()), ("last error", "boom".to_owned())]);
        assert_eq!(text, "id:         1\nlast error: boom\n");
    }
}
