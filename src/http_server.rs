use actix_web::body::BoxBody;
use actix_web::http::header::ContentType;
use actix_web::middleware::Logger;
use derive_more::{Display, Error};
use reqwest::StatusCode;
use serde::Deserialize;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use crate::delayed_queue::DelayedQueuePostgres;
use crate::message::Message;
use crate::webhooks::{Webhook, WebhooksPostgres};

use actix_web::{error, web, App, HttpRequest, HttpResponse, HttpServer, Responder};

pub struct DelayedQueueHttpServer {}

#[derive(Deserialize)]
struct AckData {
    key: String,
    kind: String,
    created_at: u64,
}

#[derive(Deserialize)]
struct CreateWebhook {
    url: String,
}

impl CreateWebhook {
    fn to_webhook(&self) -> Webhook {
        Webhook::from_url(self.url.clone())
    }
}

impl DelayedQueueHttpServer {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn start(
        &self,
        delayed_queue: &DelayedQueuePostgres,
        webhooks: &WebhooksPostgres,
    ) -> std::io::Result<()> {
        let delayed_queue = web::Data::new(Arc::new(Mutex::new(delayed_queue.clone())));
        let hooks: web::Data<Arc<Mutex<WebhooksPostgres>>> =
            web::Data::new(Arc::new(Mutex::new(webhooks.clone())));

        println!("Starting HTTP server on port 9876");
        HttpServer::new(move || {
            App::new()
                .wrap(Logger::default())
                .app_data(delayed_queue.clone())
                .app_data(hooks.clone())
                .app_data(hooks.clone())
                .route("/get_messages", web::get().to(Self::get_messages))
                .route("/schedule_message", web::post().to(Self::schedule_message))
                .route("/poll/{kind}", web::get().to(Self::poll))
                .route("/webhooks", web::post().to(Self::register_webhook))
                .route("/echo", web::post().to(Self::echo))
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

        let res = data.lock().unwrap().poll(now, Some(&kind)).await;

        format!("{:?}", res)
    }

    async fn ack(
        data: web::Data<Arc<Mutex<DelayedQueuePostgres>>>,
        ack_data: web::Json<AckData>,
    ) -> impl Responder {
        let ack_data = ack_data.into_inner();
        data.lock()
            .unwrap()
            .ack(&ack_data.key, &ack_data.kind, &ack_data.created_at)
            .await
            .unwrap();
        format!("Acked")
    }

    async fn register_webhook(
        data: web::Data<Arc<Mutex<WebhooksPostgres>>>,
        webhook: web::Json<CreateWebhook>,
    ) -> Result<Webhook, AppError> {
        let webhook = webhook.into_inner().to_webhook();
        data.lock()
            .unwrap()
            .create(&webhook)
            .await
            .map_err(|_err_| AppError::InternalError);
        Ok(webhook)
    }

    async fn echo(req: web::Bytes) -> impl Responder {
        let body_string = String::from_utf8(req.to_vec()).unwrap();
        println!("-------------- ECHO: {:?}", body_string);
        format!("Ok")
    }
}

#[derive(Debug, Display, Error)]

enum AppError {
    #[display(fmt = "An internal error occurred. Please try again later.")]
    InternalError,
}

impl error::ResponseError for AppError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).body(self.to_string())
    }

    fn status_code(&self) -> reqwest::StatusCode {
        match *self {
            AppError::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl Responder for Webhook {
    type Body = BoxBody;

    fn respond_to(self, _req: &HttpRequest) -> HttpResponse<Self::Body> {
        let body = serde_json::to_string(&self).unwrap();

        HttpResponse::Ok()
            .content_type(ContentType::json())
            .body(body)
    }
}
