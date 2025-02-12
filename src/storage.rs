use async_trait::async_trait;
use crate::models::{SavedLogEvent, SendableError};

#[async_trait]
pub trait LogStorage: Send + Sync {
    async fn store_events(&self, log_group: &str, events: &[SavedLogEvent]) -> Result<(), SendableError>;
    async fn deduplicate(&self) -> Result<(), SendableError>;
}
