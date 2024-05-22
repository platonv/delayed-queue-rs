use anyhow::Result;
use clap::{command, Command};
use delayed_queue::DelayedQueuePostgres;
use http_server::DelayedQueueHttpServer;
use std::{env, sync::Arc};
use webhooks::WebhooksPostgres;

use crate::{message::Message, webhooks::Webhook};

mod delayed_queue;
mod http_server;
mod message;
mod webhooks;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let database_url = "postgres://postgres:password@localhost:5432/delayed-queue";

    let pg_pool = sqlx::PgPool::connect(&database_url).await?;

    let delayed_queue: Arc<DelayedQueuePostgres> =
        Arc::new(DelayedQueuePostgres::new(pg_pool.clone()).await?);
    let webhooks: WebhooksPostgres = WebhooksPostgres::new(pg_pool.clone()).await?;

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

            let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));

            let cloned_delayed_queue = Arc::clone(&delayed_queue);

            tokio::spawn(async move {
                loop {
                    interval.tick().await;
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let messages = cloned_delayed_queue
                        .poll(now, None)
                        .await
                        .expect("Poll failed");

                    messages.into_iter().for_each(|message| {
                        let d = Arc::clone(&cloned_delayed_queue);
                        println!("Polled message: {:?}", message);
                        tokio::spawn(async move {
                            let res = d
                                .ack(&message.key, &message.kind, &message.created_at)
                                .await;
                            println!("Ack result for {:?}: {:?}", &message.key, res)
                        });
                    });
                }
            });

            let server = DelayedQueueHttpServer::new(Arc::clone(&delayed_queue));
            server.start().await?;
        }
        _ => {
            println!("Invalid command");
        }
    }

    println!("Closing connection");
    pg_pool.close().await;

    Ok(())
}
