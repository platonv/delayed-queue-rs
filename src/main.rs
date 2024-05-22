use anyhow::Result;
use clap::{command, Command};
use delayed_queue::DelayedQueuePostgres;
use http_server::DelayedQueueHttpServer;
use std::env;
use webhooks::WebhooksPostgres;

use crate::{message::Message, webhooks::Webhook};

mod delayed_queue;
mod http_server;
mod message;
mod webhooks;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let database_url = "postgres://postgres:password@localhost:5432/delayed-queue";

    let pgPool = sqlx::PgPool::connect(&database_url).await?;

    let delayed_queue = DelayedQueuePostgres::new(pgPool.clone()).await?;
    let webhooks = WebhooksPostgres::new(pgPool.clone()).await?;

    let matches = command!()
        .subcommand(
            Command::new("queue")
                .about("Delayed queue operations")
                .subcommand(Command::new("get").about("Get messages"))
                .subcommand(Command::new("count").about("Count messages"))
                .subcommand(Command::new("schedule").about("Schedule sample message")),
        )
        .subcommand(
            Command::new("webhooks")
                .about("Webhooks operations")
                .subcommand(Command::new("get").about("Get webhooks"))
                .subcommand(Command::new("create").about("Create a new webhook")),
        )
        .subcommand(Command::new("server").about("Start HTTP server"))
        .get_matches();

    match matches.subcommand() {
        Some(("queue", queue_matches)) => match queue_matches.subcommand() {
            Some(("get", _)) => {
                let messages = delayed_queue.get_messages().await?;
                println!("{:?}", messages);
            }
            Some(("schedule", _)) => {
                let message = Message::sample();
                delayed_queue.schedule_message(&message).await?;
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
        },
        Some(("webhooks", queue_matches)) => match queue_matches.subcommand() {
            Some(("get", _)) => {
                let webhooks = webhooks.get_all().await?;
                println!("{:?}", webhooks);
            }
            Some(("create", _)) => {
                let webhook = Webhook::sample();
                webhooks.create(&webhook).await?;
                println!("Webhook created: {:?}", webhook);
            }
            _ => {
                println!("Invalid command");
            }
        },
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
