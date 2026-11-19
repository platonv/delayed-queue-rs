//! Prometheus metrics, served at `GET /metrics`.
//!
//! Message counters come from [`Observer`] callbacks on the [`Service`], so
//! they include messages moved by push delivery and cron, not only by HTTP
//! requests. Queue depths and the age of the oldest due message come from
//! storage, refreshed at most every few seconds when `/metrics` is scraped.
//!
//! Counters are per server process. Gauges read shared storage, so every
//! server reports the same values for them; aggregate with `max`, not `sum`.

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{MatchedPath, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use dally_core::{Message, Observer, QueueName, Service, Status};
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;

/// How long gauge values read from storage are reused between scrapes.
const GAUGE_CACHE: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct QueueLabels {
    queue: String,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct QueueStatusLabels {
    queue: String,
    status: String,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RequestLabels {
    method: String,
    route: String,
    status: u16,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RouteLabels {
    method: String,
    route: String,
}

type DurationHistogram = Family<RouteLabels, Histogram, fn() -> Histogram>;

/// Every metric the server exports.
pub struct Metrics {
    registry: Registry,
    scheduled: Family<QueueLabels, Counter>,
    leased: Family<QueueLabels, Counter>,
    acked: Family<QueueLabels, Counter>,
    nacked: Family<QueueLabels, Counter>,
    dead: Family<QueueLabels, Counter>,
    requests: Family<RequestLabels, Counter>,
    request_duration: DurationHistogram,
    queue_messages: Family<QueueStatusLabels, Gauge>,
    oldest_due: Family<QueueLabels, Gauge<f64, AtomicU64>>,
    gauges_refreshed: Mutex<Option<Instant>>,
}

impl Metrics {
    /// Creates and registers every metric.
    pub fn new() -> Arc<Self> {
        fn request_histogram() -> Histogram {
            // 5 ms up to about 40 s: long polls end up in the top buckets.
            Histogram::new(exponential_buckets(0.005, 2.0, 14))
        }

        let scheduled = Family::default();
        let leased = Family::default();
        let acked = Family::default();
        let nacked = Family::default();
        let dead = Family::default();
        let requests = Family::default();
        let request_duration: DurationHistogram = Family::new_with_constructor(request_histogram);
        let queue_messages = Family::default();
        let oldest_due = Family::default();

        let mut registry = Registry::with_prefix("dally");
        registry.register(
            "messages_scheduled",
            "Messages scheduled, not counting repeated idempotency keys",
            scheduled.clone(),
        );
        registry.register(
            "messages_leased",
            "Messages handed to workers",
            leased.clone(),
        );
        registry.register(
            "messages_acked",
            "Messages finished successfully",
            acked.clone(),
        );
        registry.register(
            "messages_nacked",
            "Failed attempts reported by workers",
            nacked.clone(),
        );
        registry.register(
            "messages_dead",
            "Messages that ran out of attempts on a nack; expired leases show in dally_queue_messages",
            dead.clone(),
        );
        registry.register(
            "http_requests",
            "HTTP requests by route and status",
            requests.clone(),
        );
        registry.register(
            "http_request_duration_seconds",
            "HTTP request duration by route",
            request_duration.clone(),
        );
        registry.register(
            "queue_messages",
            "Messages per queue and status, read from storage",
            queue_messages.clone(),
        );
        registry.register(
            "queue_oldest_due_seconds",
            "How long the oldest due message in each queue has been waiting",
            oldest_due.clone(),
        );

        Arc::new(Self {
            registry,
            scheduled,
            leased,
            acked,
            nacked,
            dead,
            requests,
            request_duration,
            queue_messages,
            oldest_due,
            gauges_refreshed: Mutex::new(None),
        })
    }

    /// Renders every metric in the Prometheus text format, refreshing the
    /// storage gauges first if they are stale.
    async fn render(&self, service: &Service) -> Result<String, std::fmt::Error> {
        if self.gauges_are_stale() {
            match self.refresh_gauges(service).await {
                Ok(()) => self.mark_refreshed(),
                Err(err) => tracing::warn!(error = %err, "cannot read queue gauges"),
            }
        }
        let mut text = String::new();
        prometheus_client::encoding::text::encode(&mut text, &self.registry)?;
        Ok(text)
    }

    fn gauges_are_stale(&self) -> bool {
        let refreshed = self
            .gauges_refreshed
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        refreshed.is_none_or(|at| at.elapsed() >= GAUGE_CACHE)
    }

    fn mark_refreshed(&self) {
        *self
            .gauges_refreshed
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(Instant::now());
    }

    async fn refresh_gauges(&self, service: &Service) -> dally_core::Result<()> {
        let queues = service.list_queues().await?;
        let oldest = service.storage().oldest_due().await?;

        for summary in &queues {
            let queue = summary.queue.name.to_string();
            let stats = summary.stats;
            let counts = [
                (Status::Scheduled, stats.scheduled),
                (Status::Running, stats.running),
                (Status::Succeeded, stats.succeeded),
                (Status::Cancelled, stats.cancelled),
                (Status::Dead, stats.dead),
            ];
            for (status, count) in counts {
                let labels = QueueStatusLabels {
                    queue: queue.clone(),
                    status: status.to_string(),
                };
                self.queue_messages
                    .get_or_create(&labels)
                    .set(i64::try_from(count).unwrap_or(i64::MAX));
            }

            // Backends that cannot measure this report nothing, rather than
            // a misleading zero.
            if let Some(oldest) = &oldest {
                let waited = oldest.get(&summary.queue.name).copied().unwrap_or_default();
                self.oldest_due
                    .get_or_create(&QueueLabels { queue })
                    .set(waited.as_secs_f64());
            }
        }
        Ok(())
    }

    fn count(family: &Family<QueueLabels, Counter>, queue: &QueueName, n: usize) {
        let labels = QueueLabels {
            queue: queue.to_string(),
        };
        family.get_or_create(&labels).inc_by(n as u64);
    }
}

impl Observer for Metrics {
    fn scheduled(&self, queue: &QueueName, created: usize) {
        Self::count(&self.scheduled, queue, created);
    }

    fn leased(&self, queue: &QueueName, count: usize) {
        Self::count(&self.leased, queue, count);
    }

    fn acked(&self, message: &Message) {
        Self::count(&self.acked, &message.queue, 1);
    }

    fn nacked(&self, message: &Message) {
        Self::count(&self.nacked, &message.queue, 1);
        if message.status == Status::Dead {
            Self::count(&self.dead, &message.queue, 1);
        }
    }
}

/// The state the `/metrics` handler and the request middleware need.
#[derive(Clone)]
pub struct MetricsState {
    pub metrics: Arc<Metrics>,
    pub service: Service,
}

/// `GET /metrics`.
pub async fn handler(State(state): State<MetricsState>) -> Response {
    match state.metrics.render(&state.service).await {
        Ok(text) => (
            [(
                header::CONTENT_TYPE,
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )],
            text,
        )
            .into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// Counts requests and their duration by route template, never by raw path,
/// so the number of label values stays bounded.
pub async fn track_requests(
    State(metrics): State<Arc<Metrics>>,
    request: Request,
    next: Next,
) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| "unmatched".to_owned(), |path| path.as_str().to_owned());
    let method = request.method().to_string();
    let started = Instant::now();

    let response = next.run(request).await;

    let route_labels = RouteLabels {
        method: method.clone(),
        route: route.clone(),
    };
    metrics
        .request_duration
        .get_or_create(&route_labels)
        .observe(started.elapsed().as_secs_f64());
    metrics
        .requests
        .get_or_create(&RequestLabels {
            method,
            route,
            status: response.status().as_u16(),
        })
        .inc();
    response
}
