use async_trait::async_trait;
use chrono::{Utc, TimeZone};
use std::collections::HashSet;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::models::{SavedLogEvent, SendableError};
use crate::storage::LogStorage;

pub struct DatFilesStorage {
    pub base_dir: PathBuf,
}

impl DatFilesStorage {
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }

    /// Given a timestamp in milliseconds, return a file path like `<base_dir>/YYYY-MM-DD.dat`
    fn get_file_path(&self, timestamp: i64) -> PathBuf {
        let dt = Utc
            .timestamp_millis_opt(timestamp)
            .single()
            .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap());
        let date_str = dt.format("%Y-%m-%d").to_string();
        let mut path = self.base_dir.clone();
        path.push(format!("{}.dat", date_str));
        path
    }
}

#[async_trait]
impl LogStorage for DatFilesStorage {
    async fn store_events(&self, log_group: &str, events: &[SavedLogEvent]) -> Result<(), SendableError> {
        for event in events {
            let timestamp_value = event.timestamp.unwrap_or_else(|| Utc::now().timestamp_millis());
            let file_path = self.get_file_path(timestamp_value);

            if let Some(parent) = file_path.parent() {
                fs::create_dir_all(parent).await?;
            }

            let mut file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&file_path)
                .await?;

            // For log_stream_name, fall back to the provided log_group if missing.
            let log_stream = event.log_stream_name.clone().unwrap_or_else(|| log_group.to_string());
            let ts_str = event.timestamp.map(|ts| ts.to_string()).unwrap_or_else(|| Utc::now().timestamp_millis().to_string());
            let ingestion_time_str = event.ingestion_time.map(|it| it.to_string()).unwrap_or_default();
            let message = event.message.clone().unwrap_or_default();
            let event_id = event.event_id.clone().unwrap_or_default();

            // Sanitize each field by replacing newlines with a space.
            let log_stream = log_stream.replace("\n", " ");
            let ts_str = ts_str.replace("\n", " ");
            let message = message.replace("\n", " ");
            let ingestion_time_str = ingestion_time_str.replace("\n", " ");
            let event_id = event_id.replace("\n", " ");

            // Format each field using fixed widths (helper function assumed to exist)
            let field1 = format_fixed_field(&log_stream, 50);
            let field2 = format_fixed_field(&ts_str, 50);
            let field3 = format_fixed_field(&message, 50000);
            let field4 = format_fixed_field(&ingestion_time_str, 50);
            let field5 = format_fixed_field(&event_id, 50);

            // Concatenate the fixed-width fields into one record.
            let record = format!("{}{}{}{}{}\n", field1, field2, field3, field4, field5);

            file.write_all(record.as_bytes()).await?;
        }
        Ok(())
    }

    async fn deduplicate(&self) -> Result<(), SendableError> {
        // List all .dat files in the base directory.
        let mut dir_entries = fs::read_dir(&self.base_dir).await?;
        while let Some(entry) = dir_entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("dat") {
                // Read the file contents.
                let content = fs::read_to_string(&path).await?;
                let mut seen = HashSet::new();
                let mut unique_lines = Vec::new();

                // For each line in the file, add only if not seen.
                for line in content.lines() {
                    if seen.insert(line.to_string()) {
                        unique_lines.push(line);
                    }
                }

                // Rewrite the file with deduplicated lines.
                let new_content = unique_lines.join("\n") + "\n";
                fs::write(&path, new_content).await?;
            }
        }
        Ok(())
    }
}

/// Helper function: truncates a string to `width` characters and right-pads with spaces.
fn format_fixed_field(value: &str, width: usize) -> String {
    let truncated: String = value.chars().take(width).collect();
    // let pad_len = width.saturating_sub(truncated.chars().count());
    // let padding = " ".repeat(pad_len);
    // format!("{}{}", truncated, padding)
    format!("{}", truncated)
}
