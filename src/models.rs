use clap::ValueEnum;

pub type SendableError = Box<dyn std::error::Error + Send + Sync>;

pub struct SavedLogEvent {
    pub log_stream_name: Option<String>,
    pub timestamp: Option<i64>,
    pub message: Option<String>,
    pub ingestion_time: Option<i64>,
    pub event_id: Option<String>,
}

#[derive(ValueEnum, Debug, Clone)]
pub enum BackendType {
    Sqlite,
    DatFiles
}