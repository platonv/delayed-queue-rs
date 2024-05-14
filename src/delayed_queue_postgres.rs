use anyhow::Result;
use sqlx::postgres::PgPool;

use crate::message::Message;

pub struct DelayedQueuePostgres {
    pool: PgPool,
}

impl DelayedQueuePostgres {
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPool::connect(&database_url).await?;

        Ok(Self { pool })
    }

    pub async fn get_messages(self: &Self) -> Result<Vec<Message>> {
        let messages = sqlx::query!(
            r#"
            SELECT pkey, pkind, payload, scheduled_at, scheduled_at_initially, created_at FROM DelayedQueue ORDER BY created_at
            "#
        )
        .map(|row| {
            Ok(Message {
                key: row.pkey.clone(),
                kind: row.pkind.clone(),
                payload: row.payload.clone(),
                scheduled_at: row.scheduled_at as u64,
                scheduled_at_initially: row.scheduled_at_initially as u64,
                created_at: row.created_at as u64,
            })
        })
        .fetch_all(&self.pool)
        .await?;

        messages.into_iter().collect()
    }

    pub async fn schedule_message(self: &Self, message: &Message) -> Result<()> {
        sqlx::query!(
            r#"
            INSERT INTO DelayedQueue
            (pkey, pkind, payload, scheduled_at, scheduled_at_initially, created_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
            message.key,
            message.kind,
            message.payload,
            message.scheduled_at as i64,
            message.scheduled_at_initially as i64,
            message.created_at as i64
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
