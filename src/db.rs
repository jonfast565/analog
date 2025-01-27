use std::ops::Deref;
use std::sync::Arc;
use chrono::Duration;
use sqlx::{sqlite::SqliteConnectOptions, ConnectOptions, Pool, Sqlite, SqlitePool};
use tokio::sync::Mutex;
use crate::{config::AppConfig, models::SavedLogEvent};
use crate::models::SendableError;

pub async fn init_connection(app_config: &AppConfig) -> Result<Pool<Sqlite>, SendableError> {
    let mut options = SqliteConnectOptions::new()
        .filename(app_config.sqlite_path.clone())
        .create_if_missing(true);
    let options_with_logs = options
        .log_statements(log::LevelFilter::Debug)
        .log_slow_statements(
            log::LevelFilter::Warn,
            Duration::seconds(10).to_std()?,
        );
    let unmutable_options = options_with_logs.clone();
    let connection = SqlitePool::connect_with(unmutable_options).await?;
    Ok(connection)
}

pub async fn init_sqlite_db(pool: &Arc<Mutex<Pool<Sqlite>>>) -> Result<(), SendableError> {
    let locked_pool = pool.lock().await;
    let pool_deref = locked_pool.deref();
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
    .execute(pool_deref)
    .await?;

    sqlx::query(
        r#"
        CREATE VIEW IF NOT EXISTS cloudwatch_unique_logs_view AS
        SELECT 
            log_group,
            message,
            COUNT(*) AS message_count
        FROM 
            cloudwatch_logs
        GROUP BY 
            log_group, message;
        "#
    ).execute(pool_deref)
    .await?;

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_log_group ON cloudwatch_logs (log_group)")
        .execute(pool_deref)
        .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_timestamp ON cloudwatch_logs (timestamp)")
        .execute(pool_deref)
        .await?;

    Ok(())
}

pub async fn dedupe_rows(
    pool: &Arc<Mutex<Pool<Sqlite>>>
) -> Result<(), SendableError> {
    let locked_pool = pool.lock().await;
    let mut tx = locked_pool.begin().await?;
    let sql = r#"
        DELETE FROM cloudwatch_logs
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM cloudwatch_logs
            GROUP BY log_group, event_id, timestamp, message
        );
    "#;
    sqlx::query(sql).execute(&mut tx).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn store_events_in_sqlite(
    pool: &Arc<Mutex<Pool<Sqlite>>>,
    log_group_name: &str,
    events: &[SavedLogEvent],
) -> Result<(), SendableError> {
    let locked_pool = pool.lock().await;
    let mut tx = locked_pool.begin().await?;

    let insert_sql = r#"
        INSERT INTO cloudwatch_logs (log_group, event_id, timestamp, message)
        VALUES (?1, ?2, ?3, ?4)
    "#;

    for event in events {
        let event_id = event.event_id.clone().unwrap_or_default();
        let timestamp = event.timestamp.unwrap_or(0);
        let message = event.message.clone().unwrap_or_default();
        let _log_stream_name = event.log_stream_name.clone().unwrap_or_default();
        let _ingestion_time = event.ingestion_time.clone().unwrap_or_default();

        sqlx::query(insert_sql)
            .bind(log_group_name)
            .bind(event_id)
            .bind(timestamp)
            .bind(message)
            .execute(&mut tx)
            .await?;
    }

    tx.commit().await?;
    Ok(())
}
