use std::sync::{Arc, Mutex};

use anyhow::Result;
use sqlx::postgres::PgPool;

use crate::message::Message;

#[derive(Clone)]
pub struct DelayedQueuePostgres {
    pool: PgPool,
}

impl DelayedQueuePostgres {
    pub async fn new(pool: PgPool) -> Result<Self> {
        Ok(Self { pool })
    }

    pub async fn get_messages(self: &Self) -> Result<Vec<Message>> {
        let messages = sqlx::query!(
            r#"
            SELECT pkey, pkind, payload, scheduled_at, scheduled_at_initially, created_at FROM delayed_queue ORDER BY created_at
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

    pub async fn poll(self: &Self, kind: &str, now: u64) -> Result<Option<Message>> {
        Box::pin(async move {
            let message_opt = self.get_scheduled(kind, now).await?;

            match message_opt {
                Some(message) => {
                    let lock_result = self.lock(&message.key, kind, message.scheduled_at).await?;

                    if lock_result {
                        Ok(Some(message))
                    } else {
                        Ok(self.poll(kind, now).await?)
                    }
                }
                None => Ok(None),
            }
        })
        .await
    }

    pub async fn ack(
        self: &Self,
        key: &str,
        kind: &str,
        created_at: u64,
        scheduled_at: u64,
    ) -> Result<()> {
        sqlx::query!(
            r#"
			DELETE FROM delayed_queue
				WHERE
				pkey = $1 AND pkind = $2 AND created_at = $3 AND scheduled_at = $4
			"#,
            key,
            kind,
            created_at as i64,
            scheduled_at as i64
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn schedule_message(self: &Self, message: &Message) -> Result<()> {
        sqlx::query!(
            r#"
            INSERT INTO delayed_queue
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

    async fn get_scheduled(self: &Self, kind: &str, now: u64) -> Result<Option<Message>> {
        let result = sqlx::query!(
            r#"
			SELECT pkey, payload, scheduled_at, created_at
				FROM delayed_queue
				WHERE
				pkind = $1 AND scheduled_at <= $2
				ORDER BY scheduled_at
			"#,
            kind,
            now as i64
        )
        .map(|row| Message {
            key: row.pkey.clone(),
            kind: kind.to_string(),
            payload: row.payload.clone(),
            scheduled_at: row.scheduled_at as u64,
            scheduled_at_initially: row.scheduled_at as u64,
            created_at: row.created_at as u64,
        })
        .fetch_optional(&self.pool)
        .await?;

        Ok(result)
    }

    async fn lock(self: &Self, key: &str, kind: &str, scheduled_at: u64) -> Result<bool> {
        let result = sqlx::query!(
            r#"
			UPDATE delayed_queue
			SET
			scheduled_at = $4
			WHERE
			pkey = $1 AND
			pkind = $2 AND
			scheduled_at = $3
			;
			"#,
            key,
            kind,
            scheduled_at as i64,
            (scheduled_at as i64) + 60 * 5
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }
}
