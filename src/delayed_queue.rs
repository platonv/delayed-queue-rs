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

    pub async fn poll(self: &Self, now: u64, kind: Option<&str>) -> Result<Vec<Message>> {
        Box::pin(async move {
            let message_opt = self.get_scheduled(now, kind).await?;

            match message_opt {
                Some(message) => {
                    let lock_result = self.lock(&message.key, kind, message.scheduled_at).await?;

                    if lock_result {
                        Ok(vec![message]
                            .into_iter()
                            .chain(self.poll(now, kind).await?.into_iter())
                            .collect())
                    } else {
                        Ok(self.poll(now, kind).await?)
                    }
                }
                None => Ok(Vec::new()),
            }
        })
        .await
    }

    pub async fn ack(self: &Self, key: &str, kind: &str, created_at: &u64) -> Result<u64> {
        let res = sqlx::query!(
            r#"
			DELETE FROM delayed_queue
				WHERE
				pkey = $1 AND pkind = $2 AND created_at = $3
			"#,
            key,
            kind,
            *created_at as i64,
        )
        .execute(&self.pool)
        .await?;

        Ok(res.rows_affected())
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

    async fn get_scheduled(self: &Self, now: u64, kind: Option<&str>) -> Result<Option<Message>> {
        let result = sqlx::query!(
            r#"
			SELECT pkey, pkind, payload, scheduled_at, created_at
				FROM delayed_queue
				WHERE
                (CASE 
                    WHEN $2 != '' THEN pkind = $2 AND scheduled_at <= $1
                    ELSE scheduled_at <= $1
                END)
				ORDER BY scheduled_at
			"#,
            now as i64,
            kind
        )
        .map(|row| Message {
            key: row.pkey.clone(),
            kind: row.pkind.clone(),
            payload: row.payload.clone(),
            scheduled_at: row.scheduled_at as u64,
            scheduled_at_initially: row.scheduled_at as u64,
            created_at: row.created_at as u64,
        })
        .fetch_optional(&self.pool)
        .await?;

        Ok(result)
    }

    async fn lock(self: &Self, key: &str, kind: Option<&str>, scheduled_at: u64) -> Result<bool> {
        let result = sqlx::query!(
            r#"
			UPDATE delayed_queue
			SET
			scheduled_at = $4
			WHERE
            (
                CASE
                    WHEN $2 != '' THEN pkey = $1 AND pkind = $2 AND scheduled_at = $3
                    ELSE pkey = $1 AND scheduled_at = $3
                END
            )
			;
			"#,
            key,
            kind,
            scheduled_at as i64,
            (scheduled_at as i64) + 5 // retry interval
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }
}
