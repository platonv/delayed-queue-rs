use std::time::{SystemTime, UNIX_EPOCH};

use uuid::Uuid;

pub struct Message {
    pub key: String,
    pub kind: String,
    pub payload: Vec<u8>,
    pub scheduled_at: u64,
    pub scheduled_at_initially: u64,
    pub created_at: u64,
}

impl Message {
    pub fn sample() -> Self {
        Self {
            key: Uuid::new_v4().to_string(),
            kind: "sample_kind".to_string(),
            payload: b"sample_payload".to_vec(),
            scheduled_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            scheduled_at_initially: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("key", &self.key)
            .field("kind", &self.kind)
            .field("payload", &String::from_utf8_lossy(&self.payload))
            .field("scheduled_at", &self.scheduled_at)
            .field("scheduled_at_initially", &self.scheduled_at_initially)
            .field("created_at", &self.created_at)
            .finish()
    }
}
