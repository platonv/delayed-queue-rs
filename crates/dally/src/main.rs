//! The `dally` command line: runs the server and, later, talks to it.

use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use dally_server::Config;
use tracing_subscriber::EnvFilter;

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
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum LogFormat {
    Pretty,
    Json,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_logging(cli.log_format);

    match cli.command {
        Command::Server { config } => {
            let config = Config::load(config.as_deref()).context("loading configuration")?;
            dally_server::run(config, shutdown_signal()).await?;
        }
    }
    Ok(())
}

/// Logs at `info` unless `RUST_LOG` says otherwise.
fn init_logging(format: LogFormat) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let subscriber = tracing_subscriber::fmt().with_env_filter(filter);
    match format {
        LogFormat::Pretty => subscriber.init(),
        LogFormat::Json => subscriber.json().init(),
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
