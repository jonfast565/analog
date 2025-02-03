use aws_config::{Region, SdkConfig};
use aws_sdk_cloudwatchlogs::{types::{FilteredLogEvent, LogGroup}, Client};
use log::info;
use crate::config::AppConfig;
use crate::models::SendableError;

lazy_static! {
    static ref AWS_REGIONS: Vec<&'static str> = vec![
        "us-east-1", "us-east-2", "us-west-1", "us-west-2",
        "af-south-1", "ap-east-1", "ap-south-1", "ap-south-2",
        "ap-southeast-1", "ap-southeast-2", "ap-southeast-3",
        "ap-northeast-1", "ap-northeast-2", "ap-northeast-3",
        "ca-central-1", "eu-central-1", "eu-central-2",
        "eu-west-1", "eu-west-2", "eu-west-3", "eu-south-1",
        "eu-south-2", "eu-north-1", "me-central-1", "me-south-1",
        "sa-east-1",
    ];
}

fn find_region(input: &str) -> Option<&'static str> {
    AWS_REGIONS.iter().find(|&&region| region == input).copied()
}

pub async fn build_config(app_config: &AppConfig) -> Result<SdkConfig, SendableError> {
    let mut loader = aws_config::from_env();

    if let Some(profile_name) = app_config.profile.clone() {
        loader = loader.profile_name(profile_name);
    }

    if let Some(region_str) = &app_config.region.clone() {
        let selected_region = find_region(region_str.as_str());
        let region_provider = Region::new(selected_region.unwrap());
        loader = loader.region(region_provider);
    }

    let shared_config = loader.load().await;
    Ok(shared_config)
}

pub async fn get_log_groups(client: &Client) -> Result<Vec<LogGroup>, SendableError> {
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

pub async fn fetch_logs(
    client: &Client,
    log_group_name: &str,
    start_time: i64,
    end_time: i64,
) -> Result<Vec<FilteredLogEvent>, SendableError> {
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
            info!("Retrieved {} event(s) for log group: {}", events.len(), log_group_name);
            result.extend(events);
        }

        next_token = resp.next_token;
        if next_token.is_none() {
            break;
        }
    }

    Ok(result)
}
