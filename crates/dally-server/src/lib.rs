//! The dally server: the HTTP API plus the background tasks that keep queues
//! healthy.
//!
//! Most users run it through the `dally` binary. Use this crate to embed the
//! server in your own process, or to mount [`router`] inside an existing axum
//! app.

mod api;
pub mod config;
mod cron;
mod janitor;
mod push;
pub mod signature;

use std::future::Future;

use dally_core::{ConnectOptions, QueueName, Service};
use tokio::task::JoinHandle;

pub use api::router;
pub use config::Config;
pub use tokio_util::sync::CancellationToken;

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
    // Check the cron jobs before connecting, so a typo fails fast.
    cron::entries(&config)?;

    let options = ConnectOptions {
        migrate: config.storage.migrate,
    };
    let storage = dally_core::connect(&config.storage.url, &options).await?;
    let service = Service::new(storage, config.defaults.clone());
    apply_queues(&service, &config).await?;

    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    tracing::info!(address = %listener.local_addr()?, "dally is listening");

    let stop = CancellationToken::new();
    let background = start_background_tasks(service.clone(), &config, stop.clone())?;

    let app = router(service);
    let serve_stop = stop.clone();
    axum::serve(listener, app)
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

/// Starts the janitor, the push dispatcher and the cron ticker. They run
/// until `stop` is cancelled; the returned handle finishes once they have all
/// wound down, including in-flight push requests.
///
/// [`run`] calls this. Call it yourself when you serve [`router`] from your
/// own axum app.
pub fn start_background_tasks(
    service: Service,
    config: &Config,
    stop: CancellationToken,
) -> Result<JoinHandle<()>, Error> {
    let cron_entries = cron::entries(config)?;
    let client = push::client(&config.push)?;

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

/// Creates or updates the queues listed in the configuration.
pub async fn apply_queues(service: &Service, config: &Config) -> Result<(), Error> {
    for (name, queue) in &config.queues {
        let name = QueueName::new(name.as_str())
            .map_err(|err| Error::Config(format!("queues.{name}: {err}")))?;
        service
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
