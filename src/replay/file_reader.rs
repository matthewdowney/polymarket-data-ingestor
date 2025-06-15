use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use std::fs;
use std::path::{Path, PathBuf};

use super::gcs_downloader::GcsDownloader;

pub struct HistoricalDataReader {
    cache_dir: PathBuf,
    start_timestamp: DateTime<Utc>,
    end_timestamp: DateTime<Utc>,
}

impl HistoricalDataReader {
    pub fn new(
        cache_dir: PathBuf,
        start_timestamp: DateTime<Utc>,
        end_timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            cache_dir,
            start_timestamp,
            end_timestamp,
        }
    }

    /// Download required files from GCS
    pub async fn download_from_gcs(&self) -> Result<()> {
        let downloader = GcsDownloader::new(self.cache_dir.clone()).await?;

        println!(
            "Downloading files from GCS for time range {} to {}",
            self.start_timestamp.format("%Y-%m-%d %H:%M:%S UTC"),
            self.end_timestamp.format("%Y-%m-%d %H:%M:%S UTC")
        );

        let downloaded_files = downloader
            .download_for_time_range(self.start_timestamp, self.end_timestamp)
            .await?;

        println!("Downloaded {} files from GCS", downloaded_files.len());
        Ok(())
    }

    /// Discover files including GCS cache directory
    pub fn discover_files_with_gcs_cache(&self) -> Result<Vec<PathBuf>> {
        let mut cache_files = self.discover_files_in_directory(&self.cache_dir)?;
        cache_files.sort();
        Ok(cache_files)
    }

    /// Parse timestamp from filename format YYYY-MM-DD-HH.jsonl.zst
    fn parse_filename_timestamp(&self, filename: &str) -> Result<DateTime<Utc>> {
        let timestamp_part = &filename[..13]; // "YYYY-MM-DD-HH"
        let _datetime_str = format!("{}:00:00Z", timestamp_part);
        let formatted = format!(
            "{}-{}-{}T{}:00:00Z",
            &timestamp_part[0..4],   // year
            &timestamp_part[5..7],   // month
            &timestamp_part[8..10],  // day
            &timestamp_part[11..13]  // hour
        );

        DateTime::parse_from_rfc3339(&formatted)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| {
                anyhow!(
                    "Failed to parse timestamp from filename {}: {}",
                    filename,
                    e
                )
            })
    }

    /// Discover files in a specific directory
    fn discover_files_in_directory(&self, directory: &Path) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();

        if directory.exists() {
            for entry in fs::read_dir(directory)? {
                let entry = entry?;
                let path = entry.path();

                if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                    // Match pattern: YYYY-MM-DD-HH.jsonl.zst
                    if file_name.ends_with(".jsonl.zst")
                        && file_name.len() == "YYYY-MM-DD-HH.jsonl.zst".len()
                        && file_name.chars().nth(4) == Some('-')
                        && file_name.chars().nth(7) == Some('-')
                        && file_name.chars().nth(10) == Some('-')
                    {
                        // Parse timestamp from filename to check if it's in range
                        if let Ok(file_time) = self.parse_filename_timestamp(file_name) {
                            // Include file if it could contain messages in our time range
                            let file_end = file_time + chrono::Duration::hours(1);
                            if file_end > self.start_timestamp && file_time < self.end_timestamp {
                                files.push(path);
                            }
                        }
                    }
                }
            }
        }

        Ok(files)
    }
}
