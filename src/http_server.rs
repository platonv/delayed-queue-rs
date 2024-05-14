use actix_web::middleware::Logger;
use std::sync::{Arc, Mutex};

use crate::delayed_queue_postgres::DelayedQueuePostgres;
use crate::message::Message;

use actix_web::{web, App, HttpServer, Responder};

pub struct DelayedQueueHttpServer {
    delayed_queue: Arc<Mutex<DelayedQueuePostgres>>,
}

impl DelayedQueueHttpServer {
    pub fn new(delayed_queue: DelayedQueuePostgres) -> Self {
        Self {
            delayed_queue: Arc::new(Mutex::new(delayed_queue)),
        }
    }

    pub async fn start(&self) -> std::io::Result<()> {
        let delayed_queue = web::Data::new(self.delayed_queue.clone());

        HttpServer::new(move || {
            App::new()
                .wrap(Logger::default())
                .app_data(delayed_queue.clone())
                .route("/get_messages", web::get().to(get_messages))
                .route("/schedule_message", web::post().to(schedule_message))
        })
        .bind("127.0.0.1:9876")?
        .run()
        .await
    }
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
