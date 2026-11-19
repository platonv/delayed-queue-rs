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
use serde::de::Error as _;
use serde::{Deserialize, Deserializer};

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
    pub queues: BTreeMap<String, QueueConfig>,

    /// The background task that deletes old finished messages.
    pub janitor: JanitorConfig,

    /// The background task that delivers messages from push queues.
    pub push: PushConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 7070)),
            storage: StorageConfig::default(),
            defaults: QueueSettings::default(),
            queues: BTreeMap::new(),
            janitor: JanitorConfig::default(),
            push: PushConfig::default(),
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

/// How the push dispatcher finds push queues and talks to their endpoints.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PushConfig {
    /// How often to look for queues that gained, changed or lost their push
    /// settings, or were paused or resumed.
    #[serde(with = "humantime_serde")]
    pub refresh_interval: Duration,

    /// How long each long poll for new messages waits. Shutting down waits
    /// for a poll in progress, so this also bounds how long that takes.
    #[serde(with = "humantime_serde")]
    pub poll_wait: Duration,

    /// The `User-Agent` header sent to push endpoints.
    pub user_agent: String,
}

impl Default for PushConfig {
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(5),
            poll_wait: Duration::from_secs(5),
            user_agent: concat!("dally/", env!("CARGO_PKG_VERSION")).to_owned(),
        }
    }
}

/// A queue in the configuration file: its settings, plus the recurring jobs
/// that schedule into it.
///
/// The settings sit directly in the queue's table, next to `cron`:
///
/// ```toml
/// [queues.reports]
/// max_attempts = 3
///
/// [[queues.reports.cron]]
/// name = "nightly"
/// schedule = "0 3 * * *"
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueueConfig {
    /// The queue's settings, stored in the database on startup.
    pub settings: QueueSettings,

    /// Recurring jobs. These live only in the configuration, never in the
    /// database, so every server should be given the same list.
    pub cron: Vec<CronJob>,
}

// `#[serde(flatten)]` cannot be combined with the `deny_unknown_fields` that
// `QueueSettings` needs for the HTTP API, so split the table by hand: take
// out `cron`, and let `QueueSettings` reject whatever else it does not know.
impl<'de> Deserialize<'de> for QueueConfig {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut table = figment::value::Dict::deserialize(deserializer)?;
        let cron = match table.remove("cron") {
            Some(cron) => cron.deserialize().map_err(D::Error::custom)?,
            None => Vec::new(),
        };
        let settings = figment::value::Value::from(table)
            .deserialize()
            .map_err(D::Error::custom)?;
        Ok(Self { settings, cron })
    }
}

/// A recurring job: a payload scheduled into its queue on a cron schedule.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CronJob {
    /// Identifies the job within its queue. It is part of each message's
    /// idempotency key, so renaming a job makes it a new one.
    pub name: String,

    /// A cron expression with five fields (minute to day of week), or six
    /// with a leading seconds field.
    pub schedule: String,

    /// The IANA time zone the schedule is read in, such as
    /// `Europe/Bucharest`. Defaults to UTC.
    #[serde(default)]
    pub timezone: Option<String>,

    /// What each scheduled message carries.
    #[serde(default)]
    pub payload: serde_json::Value,
}

impl Config {
    /// The top-level keys, which environment variables are matched against.
    const SECTIONS: &[&str] = &["listen", "storage", "defaults", "queues", "janitor", "push"];

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

            let emails = &config.queues["emails"].settings;
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

    #[test]
    fn queues_hold_settings_and_cron_jobs() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "dally.toml",
                r#"
                [queues.reports]
                max_attempts = 3

                [[queues.reports.cron]]
                name = "nightly"
                schedule = "0 3 * * *"
                timezone = "Europe/Bucharest"
                payload = { kind = "nightly-report" }
                "#,
            )?;
            let config = Config::load(Some(Path::new("dally.toml")))?;
            let reports = &config.queues["reports"];
            assert_eq!(reports.settings.max_attempts, 3);
            assert_eq!(
                reports.cron,
                [CronJob {
                    name: "nightly".to_owned(),
                    schedule: "0 3 * * *".to_owned(),
                    timezone: Some("Europe/Bucharest".to_owned()),
                    payload: serde_json::json!({"kind": "nightly-report"}),
                }]
            );
            Ok(())
        });
    }

    #[test]
    fn unknown_queue_and_cron_keys_are_rejected() {
        let typo_in_settings = "[queues.reports]\nmax_atempts = 3";
        let typo_in_cron = r#"
            [[queues.reports.cron]]
            name = "nightly"
            shedule = "0 3 * * *"
        "#;
        for file in [typo_in_settings, typo_in_cron] {
            Jail::expect_with(|jail| {
                jail.create_file("dally.toml", file)?;
                assert!(Config::load(Some(Path::new("dally.toml"))).is_err());
                Ok(())
            });
        }
    }
}
