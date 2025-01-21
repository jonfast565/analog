use aws_config::meta::region::RegionProviderChain;

use crate::config::AppConfig;


pub async fn build_config_loader(app_config: &AppConfig) {
    let mut config_loader = aws_config::from_env();

    if let Some(profile) = &app_config.profile {
        config_loader = config_loader.profile_name(profile);
    }

    let region_provider = match &app_config.region {
        Some(region_str) => RegionProviderChain::first_try(region_str.as_str().into()),
        None => RegionProviderChain::default_provider(),
    };
    config_loader = config_loader.region(region_provider);
}