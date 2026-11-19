//! The error type returned by [`Client`](crate::Client) and
//! [`Worker`](crate::Worker).

use reqwest::StatusCode;
use serde::Deserialize;

/// Shorthand for results in dally-client.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Everything that can go wrong when talking to a dally server.
///
/// [`Error::Api`] means the server understood the request and refused it, so
/// retrying the same request will not help. The other variants are problems
/// on the way there and back. [`Error::is_retryable`] tells the two apart.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The server rejected the request with one of its documented errors.
    #[error("{message} ({code})")]
    Api {
        /// The HTTP status of the response.
        status: StatusCode,
        /// A stable, machine-readable code such as `message_not_found` or
        /// `lease_mismatch`.
        code: String,
        /// A description meant for people. It may change between versions.
        message: String,
    },

    /// The response had an error status but not dally's error format. This
    /// usually comes from something between the client and dally, such as a
    /// proxy or load balancer.
    #[error("unexpected response with status {status}: {body}")]
    UnexpectedResponse {
        /// The HTTP status of the response.
        status: StatusCode,
        /// The start of the response body.
        body: String,
    },

    /// The request could not be sent, timed out, or its response could not be
    /// read.
    #[error("request failed: {0}")]
    Transport(#[source] reqwest::Error),

    /// A payload could not be serialized, or a response body was not the JSON
    /// the client expected.
    #[error("invalid JSON: {0}")]
    Json(#[source] serde_json::Error),

    /// The base URL given to [`Client::new`](crate::Client::new) cannot be
    /// used.
    #[error("invalid server URL {url:?}: {reason}")]
    InvalidUrl {
        /// The URL as given.
        url: String,
        /// Why it was rejected.
        reason: String,
    },
}

impl Error {
    /// The HTTP status of the response, if the server answered.
    pub fn status(&self) -> Option<StatusCode> {
        match self {
            Error::Api { status, .. } | Error::UnexpectedResponse { status, .. } => Some(*status),
            Error::Transport(err) => err.status(),
            Error::Json(_) | Error::InvalidUrl { .. } => None,
        }
    }

    /// The server's error code, such as `message_not_found`, for
    /// [`Error::Api`] errors.
    pub fn code(&self) -> Option<&str> {
        match self {
            Error::Api { code, .. } => Some(code),
            _ => None,
        }
    }

    /// The message or queue does not exist. A message may also have been
    /// pruned after its queue's retention period.
    pub fn is_not_found(&self) -> bool {
        self.status() == Some(StatusCode::NOT_FOUND)
    }

    /// The lease no longer matches: it expired and the message went to another
    /// worker, or the message was already acked or nacked.
    pub fn is_lease_mismatch(&self) -> bool {
        self.code() == Some("lease_mismatch")
    }

    /// The message is not in a status that allows the operation, for example
    /// cancelling a message that is already running.
    pub fn is_invalid_status(&self) -> bool {
        self.code() == Some("invalid_status")
    }

    /// The request was malformed, for example an invalid queue name.
    pub fn is_invalid_input(&self) -> bool {
        self.code() == Some("invalid_input")
    }

    /// The request might succeed if sent again later: the server could not be
    /// reached, timed out, or reported a temporary problem such as storage
    /// being unavailable.
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Transport(_) => true,
            Error::Api { status, .. } | Error::UnexpectedResponse { status, .. } => {
                status.is_server_error()
                    || *status == StatusCode::TOO_MANY_REQUESTS
                    || *status == StatusCode::REQUEST_TIMEOUT
            }
            Error::Json(_) | Error::InvalidUrl { .. } => false,
        }
    }

    /// Turns an error response into an [`Error`], keeping the server's code
    /// and message when the body has dally's error format.
    pub(crate) fn from_body(status: StatusCode, body: &[u8]) -> Self {
        #[derive(Deserialize)]
        struct Envelope {
            error: Body,
        }
        #[derive(Deserialize)]
        struct Body {
            code: String,
            message: String,
        }

        match serde_json::from_slice::<Envelope>(body) {
            Ok(Envelope { error }) => Error::Api {
                status,
                code: error.code,
                message: error.message,
            },
            Err(_) => Error::UnexpectedResponse {
                status,
                body: truncate(String::from_utf8_lossy(body).trim(), MAX_BODY_IN_ERROR),
            },
        }
    }
}

/// Enough of an unexpected body to recognise it, without pasting a whole
/// HTML error page into a log line.
const MAX_BODY_IN_ERROR: usize = 512;

fn truncate(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_owned();
    }
    let end = (0..=max)
        .rev()
        .find(|&i| text.is_char_boundary(i))
        .unwrap_or(0);
    format!("{}...", &text[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_api_errors() {
        let body = br#"{"error": {"code": "lease_mismatch", "message": "not yours"}}"#;
        let err = Error::from_body(StatusCode::CONFLICT, body);
        assert!(err.is_lease_mismatch());
        assert!(!err.is_not_found());
        assert!(!err.is_retryable());
        assert_eq!(err.to_string(), "not yours (lease_mismatch)");
    }

    #[test]
    fn keeps_other_bodies_as_unexpected() {
        let err = Error::from_body(StatusCode::BAD_GATEWAY, b"<html>bad gateway</html>");
        assert!(matches!(err, Error::UnexpectedResponse { .. }));
        assert!(err.is_retryable());
        assert_eq!(err.code(), None);
    }

    #[test]
    fn truncates_on_char_boundaries() {
        assert_eq!(truncate("short", 10), "short");
        assert_eq!(truncate("ăăă", 3), "ă...");
    }
}
