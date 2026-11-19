//! A Rust client for [dally](https://github.com/platonv/dally), a job queue
//! server: schedule jobs from your app, and run them in workers.
//!
//! - [`Client`] covers the whole HTTP API: scheduling, polling, inspecting
//!   and managing messages and queues.
//! - [`Worker`] polls a queue and runs an async handler for each message, with
//!   bounded concurrency, retries through the server, and graceful shutdown.
//!
//! # Quickstart
//!
//! Start a server with `dally server`, then schedule a job from your app:
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use dally_client::Client;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Welcome {
//!     user_id: u64,
//! }
//!
//! # async fn example() -> dally_client::Result<()> {
//! let client = Client::new("http://127.0.0.1:7070")?;
//!
//! // Send the welcome email in ten minutes. Signing up twice schedules it once.
//! client
//!     .schedule("emails", &Welcome { user_id: 42 })
//!     .delay(Duration::from_secs(600))
//!     .idempotency_key("welcome-42")
//!     .send()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! And run it in a worker, in the same process or another one:
//!
//! ```no_run
//! use dally_client::{Client, Job, Worker};
//! # #[derive(serde::Deserialize)]
//! # struct Welcome { user_id: u64 }
//!
//! async fn send_welcome(user_id: u64) -> Result<(), std::io::Error> {
//!     println!("welcome, user {user_id}");
//!     Ok(())
//! }
//!
//! # async fn example() -> dally_client::Result<()> {
//! let client = Client::new("http://127.0.0.1:7070")?;
//! Worker::new(client, "emails")
//!     .concurrency(8)
//!     .run_until(
//!         async {
//!             let _ = tokio::signal::ctrl_c().await;
//!         },
//!         |job: Job<Welcome>| async move { send_welcome(job.payload.user_id).await },
//!     )
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! When the handler returns `Ok`, the message is acked. When it returns an
//! error or panics, the message is nacked and the server retries it with the
//! queue's backoff, until it runs out of attempts and is marked dead.
//!
//! # Delivery guarantees
//!
//! dally delivers every message **at least once**. A worker can crash after
//! doing the work but before acking it, and then the message runs again.
//! Write handlers so that running them twice is harmless, for example by
//! recording [`Job::id`] next to the work they do and skipping ids already
//! recorded.

mod client;
mod error;
mod schedule;
mod worker;

pub use client::{Client, ClientBuilder};
pub use error::{Error, Result};
pub use schedule::ScheduleBuilder;
pub use worker::{Job, Worker};

pub use dally_core::{
    Backoff, LeaseToken, Leased, Message, MessageFilter, MessageId, NackRequest, Page,
    PushSettings, Queue, QueueName, QueueSettings, QueueStats, QueueSummary, RunAt,
    ScheduleRequest, Scheduled, Status,
};

/// The HTTP client library, re-exported so that a custom client passed to
/// [`ClientBuilder::http_client`] comes from a compatible version.
pub use reqwest;
