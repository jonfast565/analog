use chrono::Duration;
use sqlx::{sqlite::SqliteConnectOptions, ConnectOptions, Pool, Sqlite, SqlitePool};

use crate::{config::AppConfig, models::SavedLogEvent};

pub async fn init_connection(app_config: &AppConfig) -> Result<Pool<Sqlite>, sqlx::Error> {
    let mut options = SqliteConnectOptions::new()
        .filename(app_config.sqlite_path.clone())
        .create_if_missing(true);
    let options_with_logs = options
        .log_statements(log::LevelFilter::Debug)
        .log_slow_statements(
            log::LevelFilter::Warn,
            Duration::seconds(10).to_std().unwrap(),
        );
    let unmutable_options = options_with_logs.clone();
    let connection = SqlitePool::connect_with(unmutable_options).await?;
    Ok(connection)
}

pub async fn init_sqlite_db(pool: &SqlitePool) -> Result<(), sqlx::Error> {
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

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_log_group ON cloudwatch_logs (log_group)")
        .execute(pool)
        .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_timestamp ON cloudwatch_logs (timestamp)")
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn dedupe_rows(
    pool: &SqlitePool
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
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
    pool: &SqlitePool,
    log_group_name: &str,
    events: &[SavedLogEvent],
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    let insert_sql = r#"
        INSERT INTO cloudwatch_logs (log_group, event_id, timestamp, message)
        VALUES (?1, ?2, ?3, ?4)
    "#;

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

    tx.commit().await?;
    Ok(())
}
