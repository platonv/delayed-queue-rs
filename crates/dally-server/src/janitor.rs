//! Deletes finished messages once they are past their queue's retention.
//!
//! Every server runs a janitor. Pruning is a single idempotent delete, so
//! several janitors at once only cost a few redundant queries.

use std::time::Duration;

use dally_core::Service;
use tokio_util::sync::CancellationToken;

pub async fn run(service: Service, interval: Duration, stop: CancellationToken) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = ticker.tick() => {}
        }
        match service.prune().await {
            Ok(0) => {}
            Ok(pruned) => tracing::info!(pruned, "pruned finished messages"),
            Err(err) => tracing::warn!(error = %err, "pruning failed"),
        }
    }
}
