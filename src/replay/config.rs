use chrono::{DateTime, Utc};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct ReplayConfig {
    pub start_timestamp: DateTime<Utc>,
    pub end_timestamp: DateTime<Utc>,
    pub data_directory: PathBuf,
    pub markets_filter: Option<Vec<String>>,
    pub download_from_gcs: bool,
    pub no_auto_download: bool,
    pub gcs_cache_directory: Option<PathBuf>,
    pub gcs_credentials_path: Option<PathBuf>,
}

impl ReplayConfig {
    pub fn new(
        start_timestamp: DateTime<Utc>,
        end_timestamp: DateTime<Utc>,
        data_directory: PathBuf,
    ) -> Self {
        Self {
            start_timestamp,
            end_timestamp,
            data_directory,
            markets_filter: None,
            download_from_gcs: false,
            no_auto_download: false,
            gcs_cache_directory: None,
            gcs_credentials_path: None,
        }
    }

    pub fn with_markets_filter(mut self, markets: Vec<String>) -> Self {
        self.markets_filter = Some(markets);
        self
    }

    pub fn with_gcs_download(mut self, cache_dir: PathBuf) -> Self {
        self.download_from_gcs = true;
        self.gcs_cache_directory = Some(cache_dir);
        self
    }

    pub fn with_gcs_credentials(mut self, credentials_path: PathBuf) -> Self {
        self.gcs_credentials_path = Some(credentials_path);
        self
    }

    pub fn with_no_auto_download(mut self, no_auto_download: bool) -> Self {
        self.no_auto_download = no_auto_download;
        self
    }

    /// Returns true if the given timestamp falls within the replay range
    pub fn is_in_range(&self, timestamp: &DateTime<Utc>) -> bool {
        timestamp >= &self.start_timestamp && timestamp <= &self.end_timestamp
    }

    /// Returns true if the given market should be included based on filter
    pub fn should_include_market(&self, market_id: &str) -> bool {
        match &self.markets_filter {
            Some(filter) => filter.contains(&market_id.to_string()),
            None => true,
        }
    }
}