//! The core of [dally](https://github.com/platonv/dally), a job queue server.
//!
//! This crate holds the domain types, the rules in [`Service`], and the
//! [`Storage`] trait with its backends. Each backend sits behind a cargo
//! feature: `postgres`, `sqlite`, `redis`.
//!
//! Most users want the `dally` binary rather than this crate. It is useful if
//! you are writing a new transport or a new storage backend.

pub mod error;
pub mod message;
pub mod notify;
pub mod queue;
pub mod service;
pub mod storage;

pub use error::{Error, Result};
pub use message::{
    LeaseToken, Leased, Message, MessageFilter, MessageId, NackRequest, NewMessage, Page, Retry,
    RunAt, ScheduleRequest, Scheduled, Status,
};
pub use queue::{Backoff, PushSettings, Queue, QueueName, QueueSettings, QueueStats, QueueSummary};
pub use service::{Observer, PollRequest, Service};
pub use storage::{ConnectOptions, Storage, connect};
