//! The error type shared by everything in dally-core.

use crate::message::{MessageId, Status};
use crate::queue::QueueName;

/// Shorthand for results in dally-core.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Everything that can go wrong in dally-core.
///
/// The variants are split by who has to act: invalid requests and state
/// conflicts are the caller's problem, `Storage` is the operator's.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The request is malformed, for example a queue name with spaces.
    #[error("{0}")]
    InvalidInput(String),

    /// No queue with this name exists.
    #[error("queue {0} not found")]
    QueueNotFound(QueueName),

    /// No message with this id exists. It may have been pruned.
    #[error("message {0} not found")]
    MessageNotFound(MessageId),

    /// The lease token does not match the message's current lease. The lease
    /// expired and the message was handed to another worker, or the message
    /// was already acked or nacked.
    #[error("message {0} is not leased with this token")]
    LeaseMismatch(MessageId),

    /// The operation is not allowed in the message's current status, for
    /// example cancelling a message that is already running.
    #[error("message {id} is {status}, expected {expected}")]
    InvalidStatus {
        /// The message.
        id: MessageId,
        /// Its current status.
        status: Status,
        /// The status the operation needs.
        expected: Status,
    },

    /// The storage backend failed. These are not the caller's fault and are
    /// usually worth retrying.
    #[error("storage error: {0}")]
    Storage(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl Error {
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Error::InvalidInput(message.into())
    }

    /// Wraps a backend error.
    pub fn storage(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Error::Storage(Box::new(err))
    }
}

#[cfg(feature = "postgres")]
impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        Error::storage(err)
    }
}
