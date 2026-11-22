//! API keys.
//!
//! With no keys configured, auth is off and every request may do anything.
//! With keys, every `/v1` request needs `Authorization: Bearer <key>`. A
//! `client` key may schedule, poll, ack and read; an `admin` key may also
//! change queues. Handlers that need admin take an [`Admin`] argument, so the
//! requirement is visible in their signatures.

use std::fmt;
use std::sync::Arc;

use axum::extract::{FromRequestParts, Request, State};
use axum::http::request::Parts;
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use subtle::ConstantTimeEq;
use tracing::Instrument;

use crate::Error;
use crate::api::ApiError;

/// What a key may do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Schedule, poll, ack, nack, reschedule, cancel, and read anything.
    Client,
    /// Everything, including queue settings, pausing and retrying dead
    /// messages.
    Admin,
}

/// One API key in the configuration.
#[derive(Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyConfig {
    /// Shown in logs instead of the key, such as `billing-worker`.
    pub name: String,
    /// What the key may do.
    pub role: Role,
    /// The key itself. Prefer `key_env`, so secrets stay out of the file.
    #[serde(default)]
    pub key: Option<String>,
    /// The environment variable that holds the key.
    #[serde(default)]
    pub key_env: Option<String>,
}

// Written by hand so that the key never ends up in logs.
impl fmt::Debug for KeyConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyConfig")
            .field("name", &self.name)
            .field("role", &self.role)
            .field("key", &self.key.as_ref().map(|_| "<redacted>"))
            .field("key_env", &self.key_env)
            .finish()
    }
}

/// The `[auth]` section of the configuration.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthConfig {
    /// The accepted keys. Empty means auth is off.
    pub keys: Vec<KeyConfig>,
    /// Require a key, of any role, for `/metrics` too.
    pub protect_metrics: bool,
}

/// Who made a request, as found by [`authenticate`].
#[derive(Debug, Clone)]
pub struct Caller {
    /// The key's name, or `anonymous` when auth is off.
    pub name: String,
    /// What the caller may do.
    pub role: Role,
}

/// The configured keys, ready to check requests against.
pub struct Auth {
    keys: Vec<Key>,
}

struct Key {
    name: String,
    role: Role,
    secret: Vec<u8>,
}

impl Auth {
    /// Reads the keys, including those taken from the environment.
    pub fn from_config(config: &AuthConfig) -> Result<Self, Error> {
        let keys = config
            .keys
            .iter()
            .map(|key| {
                let secret = match (&key.key, &key.key_env) {
                    (Some(secret), None) => secret.clone(),
                    (None, Some(var)) => std::env::var(var).map_err(|_| {
                        Error::Config(format!(
                            "auth key {:?}: environment variable {var} is not set",
                            key.name
                        ))
                    })?,
                    _ => {
                        return Err(Error::Config(format!(
                            "auth key {:?}: set exactly one of `key` and `key_env`",
                            key.name
                        )));
                    }
                };
                if secret.len() < 16 {
                    return Err(Error::Config(format!(
                        "auth key {:?} is too short; use at least 16 characters",
                        key.name
                    )));
                }
                Ok(Key {
                    name: key.name.clone(),
                    role: key.role,
                    secret: secret.into_bytes(),
                })
            })
            .collect::<Result<_, _>>()?;
        Ok(Self { keys })
    }

    /// Whether any keys are configured.
    pub fn is_enabled(&self) -> bool {
        !self.keys.is_empty()
    }

    /// Finds the key matching `token`. Every key is compared, in constant
    /// time, so the time taken does not reveal which keys exist.
    fn identify(&self, token: &[u8]) -> Option<&Key> {
        let mut found = None;
        for key in &self.keys {
            if bool::from(key.secret.ct_eq(token)) {
                found = Some(key);
            }
        }
        found
    }
}

/// Middleware: checks the bearer token and records the [`Caller`] for
/// handlers and [`Admin`]. With auth off, every caller is an admin.
pub async fn authenticate(
    State(auth): State<Arc<Auth>>,
    mut request: Request,
    next: Next,
) -> Response {
    let caller = if auth.is_enabled() {
        let token = bearer_token(request.headers().get(header::AUTHORIZATION));
        match token.and_then(|token| auth.identify(token)) {
            Some(key) => Caller {
                name: key.name.clone(),
                role: key.role,
            },
            None => return unauthorized(),
        }
    } else {
        Caller {
            name: "anonymous".to_owned(),
            role: Role::Admin,
        }
    };

    let span = tracing::info_span!("caller", key = caller.name);
    request.extensions_mut().insert(caller);
    next.run(request).instrument(span).await
}

fn bearer_token(header: Option<&HeaderValue>) -> Option<&[u8]> {
    let value = header?.as_bytes();
    let token = value.strip_prefix(b"Bearer ")?;
    (!token.is_empty()).then_some(token)
}

fn unauthorized() -> Response {
    let error = ApiError::new(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "a valid API key is required: send `Authorization: Bearer <key>`",
    );
    ([(header::WWW_AUTHENTICATE, "Bearer")], error).into_response()
}

/// Proof that the caller has the admin role. Take it as a handler argument to
/// make an endpoint admin-only.
pub struct Admin;

impl<S: Send + Sync> FromRequestParts<S> for Admin {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, ApiError> {
        // No caller means the request skipped `authenticate`; refuse rather
        // than guess.
        match parts.extensions.get::<Caller>() {
            Some(Caller {
                role: Role::Admin, ..
            }) => Ok(Admin),
            _ => Err(ApiError::new(
                StatusCode::FORBIDDEN,
                "forbidden",
                "this endpoint needs an admin key",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &str, role: Role, secret: &str) -> KeyConfig {
        KeyConfig {
            name: name.to_owned(),
            role,
            key: Some(secret.to_owned()),
            key_env: None,
        }
    }

    #[test]
    fn identifies_keys() {
        let config = AuthConfig {
            keys: vec![
                key("worker", Role::Client, "client-key-0123456789"),
                key("ops", Role::Admin, "admin-key-0123456789"),
            ],
            protect_metrics: false,
        };
        let auth = Auth::from_config(&config).expect("valid config");

        let found = auth
            .identify(b"admin-key-0123456789")
            .map(|key| (key.name.as_str(), key.role));
        assert_eq!(found, Some(("ops", Role::Admin)));
        assert!(auth.identify(b"admin-key-012345678").is_none());
        assert!(auth.identify(b"").is_none());
    }

    #[test]
    fn rejects_bad_key_configs() {
        let both = KeyConfig {
            key_env: Some("X".to_owned()),
            ..key("both", Role::Client, "client-key-0123456789")
        };
        let short = key("short", Role::Client, "tiny");
        for bad in [both, short] {
            let config = AuthConfig {
                keys: vec![bad],
                protect_metrics: false,
            };
            assert!(Auth::from_config(&config).is_err());
        }
    }

    #[test]
    fn parses_bearer_tokens() {
        let header = |value: &'static str| HeaderValue::from_static(value);
        assert_eq!(bearer_token(Some(&header("Bearer abc"))), Some(&b"abc"[..]));
        assert_eq!(bearer_token(Some(&header("Basic abc"))), None);
        assert_eq!(bearer_token(Some(&header("Bearer "))), None);
        assert_eq!(bearer_token(None), None);
    }

    #[test]
    fn debug_output_hides_keys() {
        let config = key("ops", Role::Admin, "admin-key-0123456789");
        assert!(!format!("{config:?}").contains("admin-key"));
    }
}
