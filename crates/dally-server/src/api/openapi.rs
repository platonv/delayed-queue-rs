//! The OpenAPI description of the HTTP API, generated from the handlers'
//! annotations and served at `GET /v1/openapi.json`.

use axum::Json;
use dally_core::{Message, ScheduleRequest};
use serde::Serialize;
use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};
use utoipa::{Modify, OpenApi, ToSchema};

use super::{messages, queues};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "dally",
        description = "A job queue server. See docs/design.md and docs/semantics.md for how \
                       delivery works.",
        license(name = "MIT OR Apache-2.0", identifier = "MIT OR Apache-2.0"),
    ),
    paths(
        messages::schedule,
        messages::poll,
        messages::ack,
        messages::nack,
        messages::get,
        messages::reschedule,
        messages::cancel,
        messages::list,
        messages::list_in_queue,
        queues::list,
        queues::get,
        queues::put,
        queues::pause,
        queues::resume,
        queues::retry_dead,
        super::healthz,
        super::readyz,
    ),
    modifiers(&BearerAuth),
    security(("bearer" = [])),
    tags(
        (name = "messages", description = "Scheduling, leasing and inspecting messages"),
        (name = "queues", description = "Queue settings and dead messages"),
        (name = "health", description = "Probes for load balancers and orchestrators"),
    ),
)]
pub struct ApiDoc;

/// Declares the bearer scheme that `security(("bearer" = []))` refers to.
struct BearerAuth;

impl Modify for BearerAuth {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "bearer",
            SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
        );
    }
}

/// `GET /v1/openapi.json`.
pub async fn spec() -> Json<utoipa::openapi::OpenApi> {
    Json(ApiDoc::openapi())
}

/// The error body every endpoint uses.
#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Debug, Serialize, ToSchema)]
struct ErrorDetail {
    /// Stable, for programs: `invalid_input`, `queue_not_found`,
    /// `message_not_found`, `lease_mismatch`, `invalid_status`,
    /// `unauthorized`, `forbidden` or `storage_unavailable`.
    code: String,
    /// For people. May change between versions.
    message: String,
}

/// One message, or an array of up to 100.
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
#[allow(dead_code)] // Only describes the body; the handler parses it by hand.
pub enum ScheduleBody {
    /// A single message.
    One(ScheduleRequest),
    /// A batch.
    Many(Vec<ScheduleRequest>),
}

/// The message for a single schedule, or the results in order for a batch.
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
#[allow(dead_code)] // Only describes the response.
pub enum ScheduleResponse {
    /// The stored message.
    One(Message),
    /// One result per scheduled message.
    Many(messages::Batch),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Run with `UPDATE_OPENAPI=1 cargo test -p dally-server openapi` after
    /// changing the API, and commit `docs/openapi.json`.
    #[test]
    fn committed_spec_is_up_to_date() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/openapi.json");
        let spec = ApiDoc::openapi().to_pretty_json().expect("spec serializes") + "\n";

        if std::env::var_os("UPDATE_OPENAPI").is_some() {
            std::fs::write(path, &spec).expect("write docs/openapi.json");
            return;
        }
        let committed = std::fs::read_to_string(path).unwrap_or_default();
        assert!(
            committed == spec,
            "docs/openapi.json is out of date; run UPDATE_OPENAPI=1 cargo test -p dally-server openapi"
        );
    }
}
