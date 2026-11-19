//! The dally server: the HTTP API plus the background tasks that keep queues
//! healthy.
//!
//! Most users run it through the `dally` binary, which calls [`run`]. To
//! embed the server in your own process instead, build a [`Server`], mount
//! [`Server::router`] in your axum app, and call
//! [`Server::start_background_tasks`]:
//!
//! ```no_run
//! # async fn example(storage: std::sync::Arc<dyn dally_core::Storage>) -> Result<(), dally_server::Error> {
//! use dally_server::{CancellationToken, Config, Server};
//!
//! let config = Config::default();
//! let service = dally_core::Service::new(storage, config.defaults.clone());
//! let server = Server::new(service, config)?;
//! server.apply_queues().await?;
//!
//! let stop = CancellationToken::new();
//! let background = server.start_background_tasks(stop.clone())?;
//! let app = axum::Router::new().nest("/dally", server.router());
//! # Ok(())
//! # }
//! ```

mod api;
pub mod auth;
pub mod config;
mod cron;
mod janitor;
mod metrics;
mod push;
pub mod signature;

use std::future::Future;
use std::sync::Arc;

use axum::Router;
use dally_core::{ConnectOptions, QueueName, Service};
use tokio::task::JoinHandle;

pub use config::Config;
pub use tokio_util::sync::CancellationToken;

use crate::auth::Auth;
use crate::metrics::Metrics;

/// Why the server could not start or stopped with an error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The configuration is invalid.
    #[error("invalid configuration: {0}")]
    Config(String),

    /// Storage could not be reached or migrated.
    #[error(transparent)]
    Storage(#[from] dally_core::Error),

    /// The listener could not be bound, or serving failed.
    #[error("http server: {0}")]
    Io(#[from] std::io::Error),
}

/// A configured server: the service, API keys and metrics, wired so that the
/// HTTP API and the background tasks report to the same metrics.
pub struct Server {
    service: Service,
    config: Config,
    auth: Arc<Auth>,
    metrics: Arc<Metrics>,
}

impl Server {
    /// Checks the configuration and wires everything together. Nothing runs
    /// until you serve the router or start the background tasks.
    pub fn new(service: Service, config: Config) -> Result<Self, Error> {
        // Check the cron jobs and the push client now, so mistakes surface
        // before anything is served.
        cron::entries(&config)?;
        push::client(&config.push)?;

        let auth = Arc::new(Auth::from_config(&config.auth)?);
        let metrics = Metrics::new();
        let service = service.with_observer(Arc::clone(&metrics) as _);
        Ok(Self {
            service,
            config,
            auth,
            metrics,
        })
    }

    /// The service, reporting to this server's metrics.
    pub fn service(&self) -> &Service {
        &self.service
    }

    /// The HTTP API: `/v1/...`, `/healthz`, `/readyz` and `/metrics`.
    pub fn router(&self) -> Router {
        api::router(
            self.service.clone(),
            Arc::clone(&self.auth),
            Arc::clone(&self.metrics),
            self.config.auth.protect_metrics,
        )
    }

    /// Creates or updates the queues listed in the configuration.
    pub async fn apply_queues(&self) -> Result<(), Error> {
        for (name, queue) in &self.config.queues {
            let name = QueueName::new(name.as_str())
                .map_err(|err| Error::Config(format!("queues.{name}: {err}")))?;
            self.service
                .put_queue(&name, queue.settings.clone())
                .await
                .map_err(|err| match err {
                    dally_core::Error::InvalidInput(message) => {
                        Error::Config(format!("queues.{name}: {message}"))
                    }
                    other => Error::Storage(other),
                })?;
        }
        Ok(())
    }

    /// Starts the janitor, the push dispatcher and the cron ticker. They run
    /// until `stop` is cancelled; the returned handle finishes once they have
    /// all wound down, including in-flight push requests.
    pub fn start_background_tasks(&self, stop: CancellationToken) -> Result<JoinHandle<()>, Error> {
        let config = &self.config;
        let cron_entries = cron::entries(config)?;
        let client = push::client(&config.push)?;
        let service = self.service.clone();

        let janitor = tokio::spawn(janitor::run(
            service.clone(),
            config.janitor.interval,
            stop.clone(),
        ));
        let push = tokio::spawn(push::run(
            service.clone(),
            config.push.clone(),
            client,
            stop.clone(),
        ));
        let cron = tokio::spawn(cron::run(service, cron_entries, stop));

        Ok(tokio::spawn(async move {
            let _ = tokio::join!(janitor, push, cron);
        }))
    }
}

/// Connects to storage, applies the configured queues, and serves the API
/// until `shutdown` resolves. In-flight requests get to finish; long polls
/// end at their wait deadline.
pub async fn run(
    config: Config,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), Error> {
    run_with(config, Router::new(), shutdown).await
}

/// Like [`run`], with `extra` routes served next to the API, such as the web
/// UI under `/ui`.
pub async fn run_with(
    config: Config,
    extra: Router,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), Error> {
    let options = ConnectOptions {
        migrate: config.storage.migrate,
    };
    let listen = config.listen;
    let storage = dally_core::connect(&config.storage.url, &options).await?;
    let service = Service::new(storage, config.defaults.clone());
    let server = Server::new(service, config)?;
    server.apply_queues().await?;

    if !server.auth.is_enabled() && !listen.ip().is_loopback() {
        tracing::warn!(
            %listen,
            "auth is off and dally is reachable from the network; \
             anyone who can reach it can read and change every queue"
        );
    }

    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(address = %listener.local_addr()?, "dally is listening");

    let stop = CancellationToken::new();
    let background = server.start_background_tasks(stop.clone())?;

    let serve_stop = stop.clone();
    axum::serve(listener, server.router().merge(extra))
        .with_graceful_shutdown(async move {
            shutdown.await;
            serve_stop.cancel();
        })
        .await?;

    stop.cancel();
    let _ = background.await;
    tracing::info!("dally stopped");
    Ok(())
}
