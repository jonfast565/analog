use chrono::{DateTime, Utc};
use clap::{command, Parser};
use log::error;
use parse_duration::parse;

#[derive(Parser, Debug)]
#[command(
    name = "analog",
    version,
    about = "Fetch AWS CloudWatch logs and store them in SQLite (via sqlx)."
)]
pub struct AppConfig {
    #[arg(long, default_value = "us-east-1")]
    pub region: Option<String>,

    #[arg(long, default_value = "default")]
    pub profile: Option<String>,

    #[arg(long, default_value = "all")]
    pub log_groups: Option<Vec<String>>,

    #[arg(long, default_value = "1h")]
    pub duration: String,

    #[arg(long, default_value = "logs.db")]
    pub sqlite_path: String,
}

impl AppConfig {
    pub fn get_duration(&self) -> (DateTime<Utc>, DateTime<Utc>) {
        // Parse duration string (e.g., "3h", "2days")
        let duration = match parse(&self.duration) {
            Ok(d) => d,
            Err(e) => {
                error!("Failed to parse duration '{}': {}", &self.duration, e);
                std::process::exit(1);
            }
        };
        let end_time = Utc::now();
        let start_time = end_time - chrono::Duration::from_std(duration).unwrap();
        (start_time, end_time)
    }
}

