use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use sqlx::{Pool, Sqlite};

use crate::models::{SavedLogEvent, SendableError};
use crate::db;
use crate::storage::LogStorage;

pub struct SqliteStorage {
    pub pool: Arc<Mutex<Pool<Sqlite>>>,
}

impl SqliteStorage {
    pub fn new(pool: Arc<Mutex<Pool<Sqlite>>>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl LogStorage for SqliteStorage {
    async fn store_events(&self, log_group: &str, events: &[SavedLogEvent]) -> Result<(), SendableError> {
        db::store_events_in_sqlite(&self.pool, log_group, events).await
    }

    async fn deduplicate(&self) -> Result<(), SendableError> {
        db::dedupe_rows(&self.pool).await
    }
}
