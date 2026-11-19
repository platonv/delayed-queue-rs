//! Server configuration, from a TOML file and `DALLY_*` environment variables.

// figment::Error is large, but it is returned once at startup, so boxing it
// would only add noise.
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use dally_core::QueueSettings;
use figment::Figment;
use figment::providers::{Env, Format, Toml};
use serde::Deserialize;

/// Everything `dally server` can be configured with. Every field has a
/// default, so an empty file (or no file) is a valid configuration.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// The address the HTTP server listens on.
    pub listen: SocketAddr,

    /// Where queues and messages are stored.
    pub storage: StorageConfig,

    /// Settings for queues that are created implicitly, by scheduling into or
    /// polling from a queue that does not exist yet.
    pub defaults: QueueSettings,

    /// Queues to create or update on startup. Settings here win over changes
    /// made through the API, every time the server starts.
    pub queues: BTreeMap<String, QueueSettings>,

    /// The background task that deletes old finished messages.
    pub janitor: JanitorConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 7070)),
            storage: StorageConfig::default(),
            defaults: QueueSettings::default(),
            queues: BTreeMap::new(),
            janitor: JanitorConfig::default(),
        }
    }
}

/// Where queues and messages are stored.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    /// `postgres://...`, `sqlite://...` or `redis://...`.
    pub url: String,

    /// Run pending schema migrations on startup. Turn this off if you run
    /// migrations as a separate deploy step.
    pub migrate: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            url: "sqlite://dally.sqlite".to_owned(),
            migrate: true,
        }
    }
}

/// How often finished messages past their retention are deleted.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct JanitorConfig {
    /// Time between runs.
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
}

impl Default for JanitorConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
        }
    }
}

impl Config {
    /// The top-level keys, which environment variables are matched against.
    const SECTIONS: &[&str] = &["listen", "storage", "defaults", "queues", "janitor"];

    /// Loads the configuration: defaults, then the file at `path` (if given),
    /// then environment variables.
    ///
    /// An environment variable names a setting by its path, with the first
    /// `_` after the prefix standing for a dot: `DALLY_LISTEN` sets `listen`,
    /// `DALLY_STORAGE_URL` sets `storage.url`, `DALLY_DEFAULTS_MAX_ATTEMPTS`
    /// sets `defaults.max_attempts`.
    pub fn load(path: Option<&Path>) -> Result<Self, figment::Error> {
        let mut figment = Figment::new();
        if let Some(path) = path {
            figment = figment.merge(Toml::file_exact(path));
        }
        // Other DALLY_* variables (DALLY_CONFIG, DALLY_LOG_FORMAT, ...) belong
        // to the command line, not to this file's settings.
        let settings = Env::prefixed("DALLY_")
            .filter(|key| {
                let top_level = key.as_str().split('_').next().unwrap_or_default();
                Self::SECTIONS
                    .iter()
                    .any(|section| section.eq_ignore_ascii_case(top_level))
            })
            .map(|key| key.as_str().replacen('_', ".", 1).into());

        figment.merge(settings).extract()
    }
}

#[cfg(test)]
mod tests {
    use figment::Jail;

    use super::*;

    #[test]
    fn empty_config_uses_defaults() {
        Jail::expect_with(|_| {
            assert_eq!(Config::load(None)?, Config::default());
            Ok(())
        });
    }

    #[test]
    fn file_then_environment() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "dally.toml",
                r#"
                listen = "0.0.0.0:8080"

                [storage]
                url = "postgres://localhost/dally"

                [defaults]
                max_attempts = 3

                [queues.emails]
                visibility_timeout = "2m"

                [queues.emails.push]
                url = "https://example.com/jobs"
                "#,
            )?;
            jail.set_env("DALLY_STORAGE_URL", "postgres://db/dally");
            jail.set_env("DALLY_DEFAULTS_MAX_ATTEMPTS", "7");
            jail.set_env("DALLY_LOG_FORMAT", "json");

            let config = Config::load(Some(Path::new("dally.toml")))?;
            assert_eq!(config.listen, SocketAddr::from(([0, 0, 0, 0], 8080)));
            assert_eq!(config.storage.url, "postgres://db/dally");
            assert_eq!(config.defaults.max_attempts, 7);

            let emails = &config.queues["emails"];
            assert_eq!(emails.visibility_timeout, Duration::from_secs(120));
            assert_eq!(emails.push.as_ref().map(|p| p.concurrency), Some(8));
            Ok(())
        });
    }

    #[test]
    fn unknown_keys_are_rejected() {
        Jail::expect_with(|jail| {
            jail.create_file("dally.toml", "lisen = \"0.0.0.0:8080\"")?;
            assert!(Config::load(Some(Path::new("dally.toml"))).is_err());
            Ok(())
        });
    }
}
