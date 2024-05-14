use anyhow::Result;
use clap::{command, Command};
use delayed_queue_postgres::DelayedQueuePostgres;
use http_server::DelayedQueueHttpServer;
use std::env;

use crate::message::Message;

mod delayed_queue_postgres;
mod http_server;
mod message;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let database_url = "postgres://postgres:password@localhost:5432/delayed-queue";

    let delayed_queue = DelayedQueuePostgres::new(&database_url).await?;

    let matches = command!()
        .subcommand(Command::new("get").about("Get messages"))
        .subcommand(Command::new("count").about("Count messages"))
        .subcommand(Command::new("schedule").about("Schedule sample message"))
        .subcommand(Command::new("server").about("Start HTTP server"))
        .get_matches();

    match matches.subcommand() {
        Some(("get", _)) => {
            let messages = delayed_queue.get_messages().await?;
            println!("{:?}", messages);
        }
        Some(("schedule", _)) => {
            let message = Message::sample();
            delayed_queue.schedule_message(&message).await?;
            println!("{:?}", message);
            println!("Message scheduled at: {}", message.scheduled_at);
        }
        Some(("server", _)) => {
            env::set_var("RUST_LOG", "actix_web=info");
            env_logger::init();

            let server = DelayedQueueHttpServer::new(delayed_queue);
            server.start().await?;
        }
        _ => {
            println!("Invalid command");
        }
    }

    Ok(())
}
