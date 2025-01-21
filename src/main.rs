mod config;
mod aws;

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_cloudwatchlogs::types::{FilteredLogEvent, LogGroup};
use aws_sdk_cloudwatchlogs::{Client, Error};
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use clap::Parser;
use config::AppConfig;
use env_logger;
use log::{error, info};
use parse_duration::parse;
use sqlx::SqlitePool;


#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::init();

    let app_config = AppConfig::parse();
    info!("Starting application with args: {:?}", app_config);

    // Build AWS config
    let mut config_loader = aws_config::from_env();

    // If a profile is specified, use it
    if let Some(profile) = &app_config.profile {
        config_loader = config_loader.profile_name(profile);
    }

    // If a region is specified, use it; otherwise rely on default
    let region_provider = match &app_config.region {
        Some(region_str) => RegionProviderChain::first_try(region_str.as_str().into()),
        None => RegionProviderChain::default_provider(),
    };
    config_loader = config_loader.region(region_provider);

    let config = config_loader.load().await;
    let client = Client::new(&config);

    // Fetch log groups
    let all_log_groups = get_log_groups(&client).await?;
    info!("Found {} total log group(s).", all_log_groups.len());

    // Filter log groups if --log-group is set
    let filtered_log_groups: Vec<LogGroup> = if let Some(ref group_name) = cli.log_group {
        all_log_groups
            .into_iter()
            .filter(|lg| {
                if let Some(lg_name) = &lg.log_group_name {
                    lg_name == group_name
                } else {
                    false
                }
            })
            .collect()
    } else {
        all_log_groups
    };

    if filtered_log_groups.is_empty() {
        info!("No matching log groups found. Exiting.");
        return Ok(());
    }

    info!("Fetching logs from {} to {}", start_time, end_time);

    // Prepare Excel filename (e.g., logs_20230101T120000_to_20230101T130000.xlsx)
    let filename = format!(
        "logs_{}_to_{}.xlsx",
        start_time.format("%Y%m%dT%H%M%S"),
        end_time.format("%Y%m%dT%H%M%S")
    );

    // Create a new workbook
    let mut workbook = Workbook::new(&filename);

    // --- Initialize SQLite via sqlx ---
    // e.g., connect to "sqlite://logs.db"
    let sqlite_url = format!("sqlite://{}", cli.sqlite_path);
    let pool = SqlitePool::connect(&sqlite_url).await.map_err(|e| {
        let msg = format!("Failed to open SQLite database '{}': {}", cli.sqlite_path, e);
        error!("{}", msg);
        // Return an AWS Error to keep function signature consistent
        Error::Unhandled(Box::new(e))
    })?;

    // Create the table if not exists
    if let Err(e) = init_sqlite_db(&pool).await {
        error!("Failed to create/init table: {}", e);
        return Err(Error::Unhandled(Box::new(e)));
    }

    // For each log group, fetch logs and write to both Excel and SQLite
    for log_group in filtered_log_groups {
        let group_name = log_group.log_group_name.clone().unwrap_or_default();
        info!("Retrieving events for log group: {}", group_name);

        let events = fetch_logs(
            &client,
            &group_name,
            start_time.timestamp_millis(),
            end_time.timestamp_millis(),
        )
        .await?;

        info!(
            "Retrieved {} event(s) for log group: {}",
            events.len(),
            group_name
        );

        // Store in SQLite
        if let Err(err) = store_events_in_sqlite(&pool, &group_name, &events).await {
            error!(
                "Failed to store events for log group '{}': {}",
                group_name, err
            );
        }

        // Create a worksheet named after the log group
        // let worksheet_name = sanitize_worksheet_name(&group_name);
    }

    Ok(())
}

/// Retrieves all log groups from CloudWatch Logs
async fn get_log_groups(client: &Client) -> Result<Vec<LogGroup>, Error> {
    let mut result = Vec::new();
    let mut next_token = None;

    loop {
        let resp = client
            .describe_log_groups()
            .set_next_token(next_token.clone())
            .send()
            .await?;

        if let Some(log_groups) = resp.log_groups {
            result.extend(log_groups);
        }

        next_token = resp.next_token;
        if next_token.is_none() {
            break;
        }
    }
    Ok(result)
}

/// Fetches log events for a single log group within the specified time range (millis since epoch)
async fn fetch_logs(
    client: &Client,
    log_group_name: &str,
    start_time: i64,
    end_time: i64,
) -> Result<Vec<FilteredLogEvent>, Error> {
    let mut result = Vec::new();
    let mut next_token = None;

    loop {
        let resp = client
            .filter_log_events()
            .log_group_name(log_group_name)
            .set_start_time(Some(start_time))
            .set_end_time(Some(end_time))
            .set_next_token(next_token.clone())
            .send()
            .await?;

        if let Some(events) = resp.events {
            result.extend(events);
        }

        next_token = resp.next_token;
        if next_token.is_none() {
            break;
        }
    }

    Ok(result)
}

/// Convert milliseconds since epoch (UTC) to a `DateTime<Utc>`
fn millis_to_datetime(millis: i64) -> DateTime<Utc> {
    // Gracefully handle invalid timestamps
    Utc.timestamp_millis_opt(millis)
        .single()
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap())
}

/// Ensure the Excel worksheet name is valid (<= 31 chars, no invalid chars)
fn sanitize_worksheet_name(name: &str) -> String {
    // Some rules for Excel sheet names:
    //   - max length 31
    //   - cannot contain: [ ] : * ? / \
    let mut clean = name
        .replace('[', "_")
        .replace(']', "_")
        .replace(':', "_")
        .replace('*', "_")
        .replace('?', "_")
        .replace('/', "_")
        .replace('\\', "_");

    // Truncate to 31 chars
    if clean.len() > 31 {
        clean.truncate(31);
    }
    if clean.is_empty() {
        clean = "Sheet".to_string();
    }
    clean
}

/// Create the table in the SQLite database if it doesn't exist.
/// We'll store:
///  - id (autoincrement)
///  - log_group (TEXT)
///  - event_id (TEXT)
///  - timestamp (INTEGER - UTC millis)
///  - message (TEXT)
/// 
/// Also creates indexes on log_group and timestamp for faster queries.
async fn init_sqlite_db(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    // Create table if not exists
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS cloudwatch_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            log_group TEXT NOT NULL,
            event_id TEXT,
            timestamp INTEGER,
            message TEXT
        )
        "#,
    )
    .execute(pool)
    .await?;

    // Create indexes if not exist
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_log_group ON cloudwatch_logs (log_group)")
        .execute(pool)
        .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_timestamp ON cloudwatch_logs (timestamp)")
        .execute(pool)
        .await?;

    Ok(())
}

/// Insert events into the SQLite database.
async fn store_events_in_sqlite(
    pool: &SqlitePool,
    log_group_name: &str,
    events: &[FilteredLogEvent],
) -> Result<(), sqlx::Error> {
    // Start a transaction
    let mut tx = pool.begin().await?;

    let insert_sql = r#"
        INSERT INTO cloudwatch_logs (log_group, event_id, timestamp, message)
        VALUES (?1, ?2, ?3, ?4)
    "#;

    // Insert each event
    for event in events {
        let event_id = event.event_id.clone().unwrap_or_default();
        let timestamp = event.timestamp.unwrap_or(0);
        let message = event.message.clone().unwrap_or_default();

        sqlx::query(insert_sql)
            .bind(log_group_name)
            .bind(event_id)
            .bind(timestamp)
            .bind(message)
            .execute(&mut tx)
            .await?;
    }

    // Commit transaction
    tx.commit().await?;
    Ok(())
}
