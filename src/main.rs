#[macro_use]
extern crate lazy_static;

mod aws;
mod config;
mod db;
mod models;
mod utilities;

use std::time::SystemTime;
use aws_sdk_cloudwatchlogs::Client;
use clap::Parser;
use config::AppConfig;
use db::dedupe_rows;
use log::{error, info};
use models::SavedLogEvent;

pub fn setup_logger() -> Result<(), Box<dyn std::error::Error>> {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{} {} {}] {}",
                humantime::format_rfc3339_seconds(SystemTime::now()),
                record.level(),
                record.target(),
                message
            ))
        })
        .level(log::LevelFilter::Info)
        .chain(std::io::stdout())
        .chain(fern::log_file("output.log")?)
        .apply()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_logger()?;

    let app_config = AppConfig::parse();
    info!("Starting application with args: {:?}", app_config);

    let pool = db::init_connection(&app_config).await?;
    db::init_sqlite_db(&pool).await?;

    let config = aws::build_config(&app_config).await?;
    let client = Client::new(&config);

    let all_log_groups = aws::get_log_groups(&client).await?;
    info!("Found {} total log group(s).", all_log_groups.len());

    let log_groups = match &app_config.log_groups {
        Some(lg) => lg.clone(),
        None => Vec::new(),
    };

    let filtered_log_groups: Vec<_> = all_log_groups
        .into_iter()
        .filter(|lg| {
            if let Some(lg_name) = &lg.log_group_name {
                log_groups.is_empty() || log_groups.contains(lg_name)
            } else {
                false
            }
        })
        .collect();

    if filtered_log_groups.is_empty() {
        info!("No matching log groups found. Exiting.");
        return Ok(());
    }

    let (start_time, end_time) = &app_config.get_duration();
    info!("Fetching logs from {} to {}", start_time, end_time);

    for log_group in filtered_log_groups {
        let group_name = log_group.log_group_name.clone().unwrap_or_default();
        info!("Retrieving events for log group: {}", group_name);

        let events = aws::fetch_logs(
            &client,
            &group_name,
            start_time.timestamp_millis(),
            end_time.timestamp_millis(),
        )
        .await?;

        let mapped_events = events
            .into_iter()
            .map(|x| SavedLogEvent {
                log_stream_name: x.log_stream_name,
                timestamp: x.timestamp,
                message: x.message,
                ingestion_time: x.ingestion_time,
                event_id: x.event_id,
            })
            .collect::<Vec<SavedLogEvent>>();

        info!(
            "Retrieved {} event(s) for log group: {}",
            mapped_events.len(),
            group_name
        );

        info!("Storing log events in db");
        if let Err(err) = db::store_events_in_sqlite(&pool, &group_name, &mapped_events).await {
            error!(
                "Failed to store events for log group '{}': {}",
                group_name, err
            );
        }
    }

    info!("Deduplicate log events");
    dedupe_rows(&pool).await?;

    info!("Done!");
    Ok(())
}
