//! Recurring jobs: payloads scheduled into a queue on a cron schedule.
//!
//! Every server runs the ticker, and there is no leader. Each occurrence is
//! scheduled with the idempotency key `cron:{name}:{time}`, so when several
//! servers fire the same occurrence, only the first creates a message.
//!
//! Time comes from the storage clock, so servers agree on when an occurrence
//! is due. The ticker asks storage for the time before each wait and sleeps
//! for the difference, so a skewed local clock only shifts when a server wakes
//! up, never which occurrence it schedules.
//!
//! # Missed runs
//!
//! After downtime, the ticker schedules the most recent missed occurrence and
//! then continues normally; older missed runs are skipped. It only does this
//! if that occurrence is within the queue's retention, because only then can
//! it tell, through the idempotency key, whether the run already happened.

use std::time::Duration;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use croner::Cron;
use croner::parser::{CronParser, Seconds};
use dally_core::{QueueName, ScheduleRequest, Service};
use tokio_util::sync::CancellationToken;

use crate::Error;
use crate::config::{Config, CronJob};

/// The longest allowed job name, so the idempotency key stays well under its
/// 200-byte limit.
const MAX_NAME_LEN: usize = 100;

/// How long to wait before trying again after storage fails.
const RETRY_DELAY: Duration = Duration::from_secs(5);

/// A validated recurring job.
#[derive(Debug, Clone)]
pub struct CronEntry {
    queue: QueueName,
    name: String,
    schedule: Cron,
    timezone: Tz,
    payload: serde_json::Value,
}

impl CronEntry {
    /// Checks a job from the configuration. Errors name the queue and job.
    pub fn new(queue: QueueName, job: &CronJob) -> Result<Self, Error> {
        let context = format!("queues.{queue}.cron {:?}", job.name);
        if job.name.is_empty() || job.name.len() > MAX_NAME_LEN {
            return Err(Error::Config(format!(
                "{context}: name must be 1 to {MAX_NAME_LEN} bytes"
            )));
        }

        let schedule = CronParser::builder()
            .seconds(Seconds::Optional)
            .build()
            .parse(&job.schedule)
            .map_err(|err| Error::Config(format!("{context}: invalid schedule: {err}")))?;

        let timezone = match &job.timezone {
            Some(name) => name
                .parse::<Tz>()
                .map_err(|err| Error::Config(format!("{context}: invalid timezone: {err}")))?,
            None => Tz::UTC,
        };

        Ok(Self {
            queue,
            name: job.name.clone(),
            schedule,
            timezone,
            payload: job.payload.clone(),
        })
    }

    /// The first occurrence strictly after `after`.
    fn next_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let local = after.with_timezone(&self.timezone);
        self.schedule
            .find_next_occurrence(&local, false)
            .ok()
            .map(|next| next.with_timezone(&Utc))
    }

    /// The latest occurrence at or before `at`.
    fn latest_at_or_before(&self, at: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let local = at.with_timezone(&self.timezone);
        self.schedule
            .find_previous_occurrence(&local, true)
            .ok()
            .map(|previous| previous.with_timezone(&Utc))
    }

    /// The idempotency key for one occurrence. The time is in UTC, so the
    /// key does not depend on the server's or the job's time zone.
    fn idempotency_key(&self, occurrence: DateTime<Utc>) -> String {
        let time = occurrence.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        format!("cron:{}:{time}", self.name)
    }
}

/// Validates every cron job in the configuration.
pub fn entries(config: &Config) -> Result<Vec<CronEntry>, Error> {
    let mut entries = Vec::new();
    for (queue, queue_config) in &config.queues {
        let queue = QueueName::new(queue.as_str())
            .map_err(|err| Error::Config(format!("queues.{queue}: {err}")))?;
        for job in &queue_config.cron {
            entries.push(CronEntry::new(queue.clone(), job)?);
        }
    }
    Ok(entries)
}

/// Runs every entry until `stop` is cancelled.
pub async fn run(service: Service, entries: Vec<CronEntry>, stop: CancellationToken) {
    let tickers: Vec<_> = entries
        .into_iter()
        .map(|entry| tokio::spawn(run_entry(service.clone(), entry, stop.clone())))
        .collect();
    for ticker in tickers {
        let _ = ticker.await;
    }
}

async fn run_entry(service: Service, entry: CronEntry, stop: CancellationToken) {
    let mut last = match catch_up(&service, &entry).await {
        Ok(last) => last,
        Err(err) => {
            tracing::warn!(queue = %entry.queue, job = entry.name, error = %err, "cron catch-up failed");
            None
        }
    };

    loop {
        let result = tokio::select! {
            _ = stop.cancelled() => return,
            result = tick(&service, &entry, last) => result,
        };
        match result {
            Ok(Some(fired)) => last = Some(fired),
            Ok(None) => {
                tracing::warn!(queue = %entry.queue, job = entry.name, "cron schedule has no future occurrences");
                return;
            }
            Err(err) => {
                tracing::warn!(queue = %entry.queue, job = entry.name, error = %err, "cron tick failed");
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = tokio::time::sleep(RETRY_DELAY) => {}
                }
            }
        }
    }
}

/// Schedules the most recent occurrence if it may have been missed, and
/// returns it.
async fn catch_up(
    service: &Service,
    entry: &CronEntry,
) -> dally_core::Result<Option<DateTime<Utc>>> {
    let now = service.storage().now().await?;
    let Some(latest) = entry.latest_at_or_before(now) else {
        return Ok(None);
    };

    let retention = service
        .get_queue(&entry.queue)
        .await?
        .queue
        .settings
        .retention;
    let age = (now - latest).to_std().unwrap_or_default();
    if age < retention {
        fire(service, entry, latest).await?;
    }
    Ok(Some(latest))
}

/// Waits for the next occurrence after `last` (or after now) and schedules it.
/// Returns the occurrence, or `None` if the schedule has no more.
async fn tick(
    service: &Service,
    entry: &CronEntry,
    last: Option<DateTime<Utc>>,
) -> dally_core::Result<Option<DateTime<Utc>>> {
    let now = service.storage().now().await?;
    // Counting from the last occurrence rather than from now means a server
    // that wakes up a little early does not skip an occurrence.
    let after = last.map_or(now, |last| last.min(now));
    let Some(next) = entry.next_after(after) else {
        return Ok(None);
    };

    if let Ok(wait) = (next - now).to_std() {
        tokio::time::sleep(wait).await;
    }
    fire(service, entry, next).await?;
    Ok(Some(next))
}

async fn fire(
    service: &Service,
    entry: &CronEntry,
    occurrence: DateTime<Utc>,
) -> dally_core::Result<()> {
    let request = ScheduleRequest {
        run_at: Some(occurrence),
        idempotency_key: Some(entry.idempotency_key(occurrence)),
        ..ScheduleRequest::new(entry.payload.clone())
    };
    let scheduled = service.schedule(&entry.queue, vec![request]).await?;
    if scheduled.first().is_some_and(|result| result.created) {
        tracing::info!(queue = %entry.queue, job = entry.name, %occurrence, "scheduled cron job");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn entry(schedule: &str, timezone: Option<&str>) -> CronEntry {
        let job = CronJob {
            name: "nightly".to_owned(),
            schedule: schedule.to_owned(),
            timezone: timezone.map(str::to_owned),
            payload: serde_json::json!({}),
        };
        CronEntry::new(QueueName::new("reports").expect("valid"), &job).expect("valid job")
    }

    fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, s)
            .single()
            .expect("valid time")
    }

    #[test]
    fn next_occurrence_in_a_time_zone() {
        // 03:00 in Bucharest is 01:00 UTC in winter (UTC+2) and 00:00 UTC in
        // summer (UTC+3).
        let nightly = entry("0 3 * * *", Some("Europe/Bucharest"));
        assert_eq!(
            nightly.next_after(utc(2026, 1, 10, 12, 0, 0)),
            Some(utc(2026, 1, 11, 1, 0, 0))
        );
        assert_eq!(
            nightly.next_after(utc(2026, 7, 10, 12, 0, 0)),
            Some(utc(2026, 7, 11, 0, 0, 0))
        );
    }

    #[test]
    fn next_is_strictly_after() {
        let hourly = entry("0 * * * *", None);
        let on_the_hour = utc(2026, 3, 1, 10, 0, 0);
        assert_eq!(
            hourly.next_after(on_the_hour),
            Some(utc(2026, 3, 1, 11, 0, 0))
        );
        assert_eq!(hourly.latest_at_or_before(on_the_hour), Some(on_the_hour));
    }

    #[test]
    fn seconds_field_is_optional() {
        let every_ten_seconds = entry("*/10 * * * * *", None);
        assert_eq!(
            every_ten_seconds.next_after(utc(2026, 3, 1, 10, 0, 3)),
            Some(utc(2026, 3, 1, 10, 0, 10))
        );
    }

    #[test]
    fn idempotency_key_is_utc() {
        let nightly = entry("0 3 * * *", Some("Europe/Bucharest"));
        assert_eq!(
            nightly.idempotency_key(utc(2026, 1, 11, 1, 0, 0)),
            "cron:nightly:2026-01-11T01:00:00Z"
        );
    }

    #[test]
    fn invalid_jobs_name_the_queue_and_job() {
        let queue = QueueName::new("reports").expect("valid");
        let job = |schedule: &str, timezone: Option<&str>| CronJob {
            name: "nightly".to_owned(),
            schedule: schedule.to_owned(),
            timezone: timezone.map(str::to_owned),
            payload: serde_json::Value::Null,
        };

        let err = CronEntry::new(queue.clone(), &job("not a schedule", None)).expect_err("bad");
        assert!(
            err.to_string().contains(r#"queues.reports.cron "nightly""#),
            "{err}"
        );

        let err = CronEntry::new(queue, &job("0 3 * * *", Some("Mars/Olympus"))).expect_err("bad");
        assert!(err.to_string().contains("timezone"), "{err}");
    }
}
