//! The `dally` command line: runs the server, and talks to a running one.

mod cli;
#[cfg(feature = "otel")]
mod telemetry;

use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use dally_server::Config;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    /// Log format.
    #[arg(
        long,
        global = true,
        env = "DALLY_LOG_FORMAT",
        default_value = "pretty"
    )]
    log_format: LogFormat,

    #[command(flatten)]
    connection: cli::Connection,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the server.
    Server {
        /// Configuration file (TOML). `DALLY_*` environment variables override it.
        #[arg(short, long, env = "DALLY_CONFIG")]
        config: Option<PathBuf>,

        /// Also serve the web UI, under `/ui`.
        #[cfg(feature = "ui")]
        #[arg(long, env = "DALLY_UI")]
        ui: bool,
    },

    /// Serve the web UI on its own, for a server running elsewhere. Use
    /// `--server` to say where.
    #[cfg(feature = "ui")]
    Ui {
        /// The address to serve the UI on.
        #[arg(long, default_value = "127.0.0.1:7071")]
        listen: std::net::SocketAddr,
    },

    /// Inspect and manage queues.
    #[command(subcommand)]
    Queues(cli::QueuesCommand),

    /// Schedule a message.
    Schedule(cli::ScheduleArgs),

    /// Show one message, including its payload and last error.
    Inspect {
        /// The message id.
        id: dally_client::MessageId,
    },

    /// List messages.
    #[command(subcommand)]
    Messages(cli::MessagesCommand),

    /// Work with dead messages.
    #[command(subcommand)]
    Dead(cli::DeadCommand),
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum LogFormat {
    Pretty,
    Json,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    // Set up logging before the runtime starts and keep the guard until it
    // has stopped: trace export uses a blocking HTTP client, which must not
    // be created or flushed from inside async code.
    let _logging = init_logging(cli.log_format);

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("starting the async runtime")?
        .block_on(run(cli))
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        #[cfg(feature = "ui")]
        Command::Server { config, ui } => {
            let config = Config::load(config.as_deref()).context("loading configuration")?;
            let extra = if ui {
                ui_in_server(&config)
            } else {
                axum::Router::new()
            };
            dally_server::run_with(config, extra, shutdown_signal()).await?;
        }
        #[cfg(not(feature = "ui"))]
        Command::Server { config } => {
            let config = Config::load(config.as_deref()).context("loading configuration")?;
            dally_server::run(config, shutdown_signal()).await?;
        }
        #[cfg(feature = "ui")]
        Command::Ui { listen } => serve_ui(&cli.connection, listen).await?,
        Command::Queues(command) => cli::queues(&cli.connection, command).await?,
        Command::Schedule(args) => cli::schedule(&cli.connection, args).await?,
        Command::Inspect { id } => cli::inspect(&cli.connection, id).await?,
        Command::Messages(command) => cli::messages(&cli.connection, command).await?,
        Command::Dead(command) => cli::dead(&cli.connection, command).await?,
    }
    Ok(())
}

/// The UI mounted at `/ui` inside the server. It still talks to the server
/// over HTTP, through the server's own listen address.
#[cfg(feature = "ui")]
fn ui_in_server(config: &Config) -> axum::Router {
    let mut address = config.listen;
    if address.ip().is_unspecified() {
        address.set_ip(std::net::Ipv4Addr::LOCALHOST.into());
    }
    tracing::info!("serving the web UI at /ui");
    dally_ui::mount(dally_ui::UiConfig {
        server: format!("http://{address}"),
        base_path: "/ui".to_owned(),
    })
}

/// `dally ui`: the UI on its own listener, for the server at `--server`.
#[cfg(feature = "ui")]
async fn serve_ui(
    connection: &cli::Connection,
    listen: std::net::SocketAddr,
) -> anyhow::Result<()> {
    let ui = dally_ui::router(dally_ui::UiConfig {
        server: connection.server().to_owned(),
        base_path: String::new(),
    });
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("listening on {listen}"))?;
    tracing::info!(address = %listen, server = connection.server(), "serving the web UI");
    axum::serve(listener, ui)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

/// What has to stay alive while the program logs.
struct LoggingGuard {
    #[cfg(feature = "otel")]
    _telemetry: telemetry::Guard,
}

/// Logs at `info` unless `RUST_LOG` says otherwise. Logs go to stderr so that
/// `--json` output on stdout stays parseable.
fn init_logging(format: LogFormat) -> LoggingGuard {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let output = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);
    let output = match format {
        LogFormat::Pretty => output.boxed(),
        LogFormat::Json => output.json().boxed(),
    };
    let subscriber = tracing_subscriber::registry().with(filter).with(output);

    #[cfg(feature = "otel")]
    {
        let (export, guard) = telemetry::layer();
        subscriber.with(export).init();
        LoggingGuard { _telemetry: guard }
    }
    #[cfg(not(feature = "otel"))]
    {
        subscriber.init();
        LoggingGuard {}
    }
}

/// Resolves on Ctrl-C or, on Unix, SIGTERM (what Docker and Kubernetes send).
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %err, "cannot listen for Ctrl-C");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(err) => {
                tracing::error!(error = %err, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    tracing::info!("shutting down");
}
