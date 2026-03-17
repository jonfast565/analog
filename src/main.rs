#[macro_use]
extern crate lazy_static;

mod aws;
mod config;
mod db;
mod models;
mod utilities;
mod storage;
mod sqlite_storage;
mod dat_files;

use crate::models::{SendableError, BackendType};
use aws_sdk_cloudwatchlogs::{types::LogGroup, Client};
use chrono::{DateTime, Utc};
use clap::Parser;
use config::AppConfig;
use log::{error, info};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

#[tokio::main]
async fn main() -> Result<(), SendableError> {
    utilities::setup_logger()?;

    let max_concurrent_tasks = 3;
    let semaphore = Arc::new(Semaphore::new(max_concurrent_tasks));

    let app_config = AppConfig::parse();
    info!("Starting application with args: {:?}", app_config);

    let storage_backend: Arc<dyn storage::LogStorage> = 
        match app_config.backend {
            BackendType::Sqlite => {
                let pool = db::init_connection(&app_config).await?;
                let shared_pool = Arc::new(Mutex::new(pool));
                db::init_sqlite_db(&shared_pool).await?;
                Arc::new(sqlite_storage::SqliteStorage::new(shared_pool.clone()))
            },
            BackendType::DatFiles => {
                let base_dir = std::path::PathBuf::from("datfiles");
                Arc::new(dat_files::DatFilesStorage::new(base_dir))
            },
        };

    let config = aws::build_config(&app_config).await?;
    let client = Client::new(&config);
    let all_log_groups = aws::get_log_groups(&client).await?;

    info!("Found {} total log group(s).", all_log_groups.len());

    let log_groups = match &app_config.log_groups {
        Some(lg) => lg.clone(),
        None => Vec::new(),
    };

    let filtered_log_groups = filter_log_groups(all_log_groups, log_groups);
    if filtered_log_groups.is_empty() {
        info!("No matching log groups found. Exiting.");
        return Ok(());
    }

    let (start_time, end_time) = app_config.get_duration();
    info!("Fetching logs from {} to {}", start_time, end_time);

    let mut join_handles = Vec::new();
    for log_group in filtered_log_groups {
        let storage = Arc::clone(&storage_backend);
        let permit = Arc::clone(&semaphore);
        let client = Client::new(&config);
        let handle: tokio::task::JoinHandle<Result<(), SendableError>> = tokio::spawn(async move {
            let _permit = permit.acquire().await?;
            process_one_log_group(storage, client, start_time, end_time, log_group).await?;
            Ok(())
        });
        join_handles.push(handle);
    }

    for handle in join_handles {
        let (res,) = tokio::try_join!(handle)?;
        res?;
    }

    info!("Deduplicate log events");
    storage_backend.deduplicate().await?;

    info!("Done!");
    Ok(())
}

async fn process_one_log_group(
    storage: Arc<dyn storage::LogStorage>,
    client: Client,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    log_group: LogGroup,
) -> Result<(), SendableError> {
    let group_name = log_group.log_group_name.clone().unwrap_or_default();
    info!("Retrieving events for log group: {}", group_name);

    let events = aws::fetch_logs(
        &client,
        &group_name,
        start_time.timestamp_millis(),
        end_time.timestamp_millis(),
    )
    .await;

    let events = match events {
        Ok(t) => t,
        Err(e) => panic!("{}", e),
    };

    let mapped_events = events
        .into_iter()
        .map(|x| models::SavedLogEvent {
            log_stream_name: x.log_stream_name,
            timestamp: x.timestamp,
            message: x.message,
            ingestion_time: x.ingestion_time,
            event_id: x.event_id,
        })
        .collect::<Vec<models::SavedLogEvent>>();

    info!(
        "Retrieved {} event(s) for log group: {}",
        mapped_events.len(),
        group_name
    );

    info!("Storing log events using selected backend");
    if let Err(err) = storage.store_events(&group_name, &mapped_events).await {
        error!(
            "Failed to store events for log group '{}': {}",
            group_name, err
        );
    }
    Ok(())
}

fn filter_log_groups(all_log_groups: Vec<LogGroup>, log_groups: Vec<String>) -> Vec<LogGroup> {
    let filtered_log_groups = if log_groups.contains(&"all".to_string()) {
        all_log_groups
    } else {
        all_log_groups
            .into_iter()
            .filter(|lg| {
                if let Some(lg_name) = &lg.log_group_name {
                    log_groups.is_empty() || log_groups.contains(lg_name)
                } else {
                    false
                }
            })
            .collect()
    };
    filtered_log_groups
}
