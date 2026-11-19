use axum::Json;
use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// An error as the API returns it:
/// `{"error": {"code": "lease_mismatch", "message": "..."}}`.
///
/// `code` is stable and meant for programs; `message` is for people and may
/// change.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_input", message)
    }
}

impl From<dally_core::Error> for ApiError {
    fn from(err: dally_core::Error) -> Self {
        use dally_core::Error;

        let (status, code) = match &err {
            Error::InvalidInput(_) => (StatusCode::BAD_REQUEST, "invalid_input"),
            Error::QueueNotFound(_) => (StatusCode::NOT_FOUND, "queue_not_found"),
            Error::MessageNotFound(_) => (StatusCode::NOT_FOUND, "message_not_found"),
            Error::LeaseMismatch(_) => (StatusCode::CONFLICT, "lease_mismatch"),
            Error::InvalidStatus { .. } => (StatusCode::CONFLICT, "invalid_status"),
            Error::Storage(_) => {
                // Storage errors can contain connection details, so the client
                // gets a generic message and the details go to the log.
                tracing::error!(error = %err, "storage error");
                return Self::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "storage_unavailable",
                    "storage is unavailable, try again later",
                );
            }
        };
        Self::new(status, code, err.to_string())
    }
}

impl From<JsonRejection> for ApiError {
    fn from(rejection: JsonRejection) -> Self {
        Self::invalid(rejection.body_text())
    }
}

impl From<QueryRejection> for ApiError {
    fn from(rejection: QueryRejection) -> Self {
        Self::invalid(rejection.body_text())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({ "error": { "code": self.code, "message": self.message } });
        (self.status, Json(body)).into_response()
    }
}
