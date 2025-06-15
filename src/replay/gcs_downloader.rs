use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use tokio::task::JoinSet;

const BUCKET_NAME: &str = "polymarket-data-bucket";
const GCS_PREFIX: &str = "raw/";
const BATCH_SIZE: usize = 4; // Number of files to download in parallel

#[derive(Clone)]
pub struct GcsDownloader {
    local_cache_dir: PathBuf,
}

impl GcsDownloader {
    /// Create a new GCS downloader that uses gcloud storage
    pub async fn new(cache_dir: PathBuf) -> Result<Self> {
        // Create cache directory if it doesn't exist
        fs::create_dir_all(&cache_dir)?;

        // Check if gcloud storage is available
        let gcloud_check = Command::new("gcloud").args(["storage", "--help"]).output();

        if gcloud_check.is_err() || !gcloud_check.unwrap().status.success() {
            return Err(anyhow!(
                "gcloud storage command not found. Please install Google Cloud SDK and run 'gcloud auth login'"
            ));
        }

        Ok(Self {
            local_cache_dir: cache_dir,
        })
    }

    /// Download files for a specific time range
    /// Returns paths to all downloaded files (both newly downloaded and cached)
    pub async fn download_for_time_range(
        &self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<PathBuf>> {
        let required_files = self.get_required_files_for_range(start_time, end_time)?;
        let mut downloaded_files = Vec::new();

        // Filter out files that are already cached
        let (cached_files, files_to_download): (Vec<_>, Vec<_>) = required_files
            .into_iter()
            .map(|file_name| {
                let local_path = self.local_cache_dir.join(&file_name);
                (file_name, local_path)
            })
            .partition(|(_, path)| path.exists());

        // Add cached files to result
        for (file_name, path) in cached_files {
            println!("Using cached file: {}", file_name);
            downloaded_files.push(path);
        }

        // Download remaining files in batches
        for chunk in files_to_download.chunks(BATCH_SIZE) {
            let batch_files = self.download_batch(chunk).await?;
            downloaded_files.extend(batch_files);
        }

        Ok(downloaded_files)
    }

    /// Download a batch of files in parallel
    async fn download_batch(&self, files: &[(String, PathBuf)]) -> Result<Vec<PathBuf>> {
        let mut tasks = JoinSet::new();
        let mut downloaded_files = Vec::new();

        // Spawn tasks for each file in the batch
        for (file_name, local_path) in files {
            let file_name = file_name.clone();
            let local_path = local_path.clone();
            let self_clone = self.clone();

            tasks.spawn(async move {
                println!("Downloading: {}", file_name);
                match self_clone.download_file(&file_name, &local_path).await {
                    Ok(_) => Ok(local_path),
                    Err(e) => {
                        eprintln!("Warning: Failed to download {}: {}", file_name, e);
                        Err(e)
                    }
                }
            });
        }

        // Wait for all downloads in this batch to complete
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(path)) => downloaded_files.push(path),
                Ok(Err(e)) => eprintln!("Warning: Download task failed: {}", e),
                Err(e) => eprintln!("Warning: Download task panicked: {}", e),
            }
        }

        Ok(downloaded_files)
    }

    /// Generate list of required files for a time range
    fn get_required_files_for_range(
        &self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<String>> {
        let mut files = Vec::new();
        let mut current = start_time;

        // Generate hourly file names for the entire range
        while current <= end_time {
            let file_name = format!("{}.jsonl.zst", current.format("%Y-%m-%d-%H"));
            files.push(file_name);

            // Move to next hour
            current += chrono::Duration::hours(1);
        }

        Ok(files)
    }

    /// Download a single file from GCS using gcloud storage
    async fn download_file(&self, file_name: &str, local_path: &Path) -> Result<()> {
        let gcs_path = format!("gs://{}/{}{}", BUCKET_NAME, GCS_PREFIX, file_name);

        // Create parent directory if it doesn't exist
        if let Some(parent) = local_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Use gcloud storage to download the file
        let output = Command::new("gcloud")
            .args(["storage", "cp", &gcs_path, local_path.to_str().unwrap()])
            .output()
            .map_err(|e| anyhow!("Failed to run gcloud storage: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("not found")
                || stderr.contains("does not exist")
                || stderr.contains("No such object")
            {
                return Err(anyhow!("File not found in GCS: {}", file_name));
            } else {
                return Err(anyhow!("GCS download failed: {}", stderr));
            }
        }

        Ok(())
    }
}
