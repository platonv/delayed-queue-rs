//! The dally server: the HTTP API plus the background tasks that keep queues
//! healthy.
//!
//! Most users run it through the `dally` binary. Use this crate to embed the
//! server in your own process, or to mount [`router`] inside an existing axum
//! app.

mod api;
pub mod config;
mod janitor;

use std::future::Future;

use dally_core::{ConnectOptions, QueueName, Service};
use tokio_util::sync::CancellationToken;

pub use api::router;
pub use config::Config;

/// Why the server could not start or stopped with an error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A queue in the configuration is invalid.
    #[error("invalid configuration: {0}")]
    Config(String),

    /// Storage could not be reached or migrated.
    #[error(transparent)]
    Storage(#[from] dally_core::Error),

    /// The listener could not be bound, or serving failed.
    #[error("http server: {0}")]
    Io(#[from] std::io::Error),
}

/// Connects to storage, applies the configured queues, and serves the API
/// until `shutdown` resolves. In-flight requests get to finish; long polls
/// end at their wait deadline.
pub async fn run(
    config: Config,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), Error> {
    let options = ConnectOptions {
        migrate: config.storage.migrate,
    };
    let storage = dally_core::connect(&config.storage.url, &options).await?;
    let service = Service::new(storage, config.defaults.clone());
    apply_queues(&service, &config).await?;

    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    tracing::info!(address = %listener.local_addr()?, "dally is listening");

    let stop = CancellationToken::new();
    let janitor = tokio::spawn(janitor::run(
        service.clone(),
        config.janitor.interval,
        stop.clone(),
    ));

    let app = router(service);
    let serve_stop = stop.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.await;
            serve_stop.cancel();
        })
        .await?;

    stop.cancel();
    let _ = janitor.await;
    tracing::info!("dally stopped");
    Ok(())
}

/// Creates or updates the queues listed in the configuration.
async fn apply_queues(service: &Service, config: &Config) -> Result<(), Error> {
    for (name, settings) in &config.queues {
        let name = QueueName::new(name.as_str())
            .map_err(|err| Error::Config(format!("queues.{name}: {err}")))?;
        service
            .put_queue(&name, settings.clone())
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
