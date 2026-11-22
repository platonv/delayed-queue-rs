//! Wake-ups for long polling.
//!
//! When a message is scheduled, the backend calls [`Notifier::notify`] so that
//! requests waiting on that queue look again right away instead of at their
//! next timer tick. Notifications are hints: a waiter may wake up and find
//! nothing, and a missed notification only costs latency, never a message.

use std::collections::HashMap;
use std::sync::Mutex;

use tokio::sync::watch;

use crate::queue::QueueName;

/// Fans out "something was scheduled in queue X" to everyone waiting on X.
#[derive(Debug, Default)]
pub struct Notifier {
    queues: Mutex<HashMap<QueueName, watch::Sender<()>>>,
}

impl Notifier {
    /// Creates a notifier with no subscribers.
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts listening for notifications on `queue`. Notifications sent
    /// after this call are seen by [`Subscription::changed`], even if they
    /// arrive before it is awaited.
    pub fn subscribe(&self, queue: &QueueName) -> Subscription {
        let mut queues = self
            .queues
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let sender = queues
            .entry(queue.clone())
            .or_insert_with(|| watch::channel(()).0);
        Subscription(sender.subscribe())
    }

    /// Wakes everyone waiting on `queue`.
    pub fn notify(&self, queue: &QueueName) {
        let queues = self
            .queues
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(sender) = queues.get(queue) {
            sender.send_replace(());
        }
    }
}

/// A handle that resolves when its queue is notified.
#[derive(Debug)]
pub struct Subscription(watch::Receiver<()>);

impl Subscription {
    /// Waits for the next notification.
    pub async fn changed(&mut self) {
        // The sender lives as long as the notifier, so this only errors while
        // shutting down. Waiting forever is right then: the caller's timeout
        // ends the wait.
        if self.0.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn notifications_sent_before_waiting_are_not_lost() {
        let notifier = Notifier::new();
        let queue = QueueName::new("emails").expect("valid name");

        let mut subscription = notifier.subscribe(&queue);
        notifier.notify(&queue);

        tokio::time::timeout(Duration::from_millis(100), subscription.changed())
            .await
            .expect("notification should already be pending");
    }

    #[tokio::test]
    async fn other_queues_do_not_wake_subscribers() {
        let notifier = Notifier::new();
        let emails = QueueName::new("emails").expect("valid name");
        let reports = QueueName::new("reports").expect("valid name");

        let mut subscription = notifier.subscribe(&emails);
        notifier.notify(&reports);

        let woke = tokio::time::timeout(Duration::from_millis(50), subscription.changed()).await;
        assert!(woke.is_err());
    }
}
