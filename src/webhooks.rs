use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPool;

#[derive(Debug, Serialize, Deserialize)]
pub struct Webhook {
    pub id: String,
    pub url: String,
    pub fails_count: u64,
    pub created_at: u64,
    pub updated_at: u64,
}

impl Webhook {
    pub fn new(id: String, url: String, created_at: u64) -> Self {
        Self {
            id,
            url,
            fails_count: 0,
            created_at,
            updated_at: created_at,
        }
    }

    pub fn from_url(url: String) -> Self {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            url,
            fails_count: 0,
            created_at: ts,
            updated_at: ts,
        }
    }

    pub fn sample() -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            url: "http://localhost:9876".to_string(),
            fails_count: 0,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            updated_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    pub async fn send<T: Serialize>(&self, data: &T) -> Result<()> {
        let client = reqwest::Client::new();
        println!("Sending to {}", &self.url);
        client
            .post(&self.url)
            .body(serde_json::to_string(data)?)
            .send()
            .await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct WebhooksPostgres {
    pool: PgPool,
}

impl WebhooksPostgres {
    pub async fn new(pool: PgPool) -> Result<Self> {
        Ok(Self { pool })
    }

    pub async fn get_all(self: &Self) -> Result<Vec<Webhook>> {
        let webhooks = sqlx::query!(r#"SELECT * FROM webhooks"#)
            .map(|row| {
                Ok(Webhook {
                    id: row.id.clone(),
                    url: row.url.clone(),
                    fails_count: row.fails_count as u64,
                    created_at: row.created_at as u64,
                    updated_at: row.updated_at as u64,
                })
            })
            .fetch_all(&self.pool)
            .await?;

        webhooks.into_iter().collect()
    }

    pub async fn create(self: &Self, webhook: &Webhook) -> Result<()> {
        println!("Creating webhook: {:?}", webhook);
        sqlx::query!(
            r#"
			INSERT INTO webhooks (id, url, fails_count, created_at, updated_at) VALUES ($1, $2, $3, $4, $5)
			"#,
            webhook.id,
            webhook.url,
            webhook.fails_count as i64,
            webhook.created_at as i64,
            webhook.updated_at as i64
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn update(self: &Self, webhook: &Webhook) -> Result<()> {
        sqlx::query!(
            r#"
			UPDATE webhooks
				SET
				url = $2,
				fails_count = $3,
				updated_at = $4
				WHERE
				id = $1
			"#,
            webhook.id,
            webhook.url,
            webhook.fails_count as i64,
            webhook.updated_at as i64
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
