//! Turning API types into what the templates show.

use chrono::{DateTime, Utc};
use dally_client::{Message, QueueSummary, Status};

/// One row of the queue table.
pub struct QueueRow {
    pub name: String,
    pub scheduled: u64,
    pub running: u64,
    pub succeeded: u64,
    pub dead: u64,
    pub paused: bool,
}

impl From<&QueueSummary> for QueueRow {
    fn from(summary: &QueueSummary) -> Self {
        Self {
            name: summary.queue.name.to_string(),
            scheduled: summary.stats.scheduled,
            running: summary.stats.running,
            succeeded: summary.stats.succeeded,
            dead: summary.stats.dead,
            paused: summary.queue.settings.paused,
        }
    }
}

/// One row of a message table.
pub struct MessageRow {
    pub id: String,
    pub attempts: String,
    pub run_at: String,
    pub when: String,
    pub last_error: String,
}

impl MessageRow {
    pub fn new(message: &Message, now: DateTime<Utc>) -> Self {
        Self {
            id: message.id.to_string(),
            attempts: format!("{}/{}", message.attempts, message.max_attempts),
            run_at: timestamp(message.run_at),
            when: relative(message.run_at, now),
            last_error: message.last_error.clone().unwrap_or_default(),
        }
    }
}

/// A timestamp to the second, in UTC.
pub fn timestamp(at: DateTime<Utc>) -> String {
    at.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

/// "in 5m" or "3h ago", rounded to the largest unit.
pub fn relative(at: DateTime<Utc>, now: DateTime<Utc>) -> String {
    let seconds = (at - now).num_seconds();
    let magnitude = seconds.unsigned_abs();
    let amount = match magnitude {
        0..=59 => format!("{magnitude}s"),
        60..=3_599 => format!("{}m", magnitude / 60),
        3_600..=86_399 => format!("{}h", magnitude / 3_600),
        _ => format!("{}d", magnitude / 86_400),
    };
    if magnitude < 1 {
        "now".to_owned()
    } else if seconds > 0 {
        format!("in {amount}")
    } else {
        format!("{amount} ago")
    }
}

/// The status filter tabs on a queue page, in lifecycle order.
pub const STATUSES: [Status; 5] = Status::ALL;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_times() {
        let now = Utc::now();
        let at = |seconds: i64| now + chrono::Duration::seconds(seconds);
        assert_eq!(relative(at(0), now), "now");
        assert_eq!(relative(at(30), now), "in 30s");
        assert_eq!(relative(at(-90), now), "1m ago");
        assert_eq!(relative(at(7_200), now), "in 2h");
        assert_eq!(relative(at(-3 * 86_400), now), "3d ago");
    }
}
