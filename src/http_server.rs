use actix_web::middleware::Logger;
use serde::Deserialize;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use crate::delayed_queue::DelayedQueuePostgres;
use crate::message::Message;

use actix_web::{web, App, HttpServer, Responder};

pub struct DelayedQueueHttpServer {
    delayed_queue: Arc<Mutex<DelayedQueuePostgres>>,
}

#[derive(Deserialize)]
struct AckData {
    key: String,
    kind: String,
    created_at: u64,
    scheduled_at: u64,
}

impl DelayedQueueHttpServer {
    pub fn new<'b>(delayed_queue: DelayedQueuePostgres) -> Self {
        Self {
            delayed_queue: Arc::new(Mutex::new(delayed_queue)),
        }
    }

    pub async fn start(&self) -> std::io::Result<()> {
        let delayed_queue = web::Data::new(self.delayed_queue.clone());

        println!("Starting HTTP server on port 9876");
        HttpServer::new(move || {
            App::new()
                .wrap(Logger::default())
                .app_data(delayed_queue.clone())
                .route("/get_messages", web::get().to(Self::get_messages))
                .route("/schedule_message", web::post().to(Self::schedule_message))
                .route("/poll/{kind}", web::get().to(Self::poll))
                .route("/ack", web::post().to(Self::ack))
        })
        .bind("0.0.0.0:9876")?
        .run()
        .await
    }

    // TODO: Get rid of the unwrap calls
    async fn get_messages(data: web::Data<Arc<Mutex<DelayedQueuePostgres>>>) -> impl Responder {
        let messages = data.lock().unwrap().get_messages().await.unwrap();
        format!("{:?}", messages)
    }

    // TODO: Get rid of the unwrap calls
    async fn schedule_message(data: web::Data<Arc<Mutex<DelayedQueuePostgres>>>) -> impl Responder {
        let message = Message::sample();
        data.lock()
            .unwrap()
            .schedule_message(&message)
            .await
            .unwrap();
        format!("{:?}", message)
    }

    async fn poll(
        kind: web::Path<String>,
        data: web::Data<Arc<Mutex<DelayedQueuePostgres>>>,
    ) -> impl Responder {
        let now: u64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let res = data.lock().unwrap().poll(&kind, now).await;

        format!("{:?}", res)
    }

    async fn ack(
        data: web::Data<Arc<Mutex<DelayedQueuePostgres>>>,
        ack_data: web::Json<AckData>,
    ) -> impl Responder {
        let ack_data = ack_data.into_inner();
        data.lock()
            .unwrap()
            .ack(
                &ack_data.key,
                &ack_data.kind,
                ack_data.created_at,
                ack_data.scheduled_at,
            )
            .await
            .unwrap();
        format!("Acked")
    }
}
