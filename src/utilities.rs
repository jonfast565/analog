use chrono::{DateTime, Utc, TimeZone};

pub fn millis_to_datetime(millis: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(millis)
        .single()
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap())
}