use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const BUCKET_NAME: &str = "polymarket-data-bucket";
const GCS_PREFIX: &str = "raw/";

pub struct GcsDownloader {
    local_cache_dir: PathBuf,
}

impl GcsDownloader {
    /// Create a new GCS downloader that uses gcloud storage
    pub async fn new(cache_dir: PathBuf) -> Result<Self> {
        // Create cache directory if it doesn't exist
        fs::create_dir_all(&cache_dir)?;

        // Check if gcloud storage is available
        let gcloud_check = Command::new("gcloud")
            .args(["storage", "--help"])
            .output();

        if gcloud_check.is_err() || !gcloud_check.unwrap().status.success() {
            return Err(anyhow!(
                "gcloud storage command not found. Please install Google Cloud SDK and run 'gcloud auth login'"
            ));
        }

        Ok(Self {
            local_cache_dir: cache_dir,
        })
    }

    /// Create a new GCS downloader with custom credentials
    pub async fn new_with_credentials(cache_dir: PathBuf, credentials_path: PathBuf) -> Result<Self> {
        // Create cache directory if it doesn't exist
        fs::create_dir_all(&cache_dir)?;

        // Set the credentials environment variable for gcloud
        std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS", credentials_path);

        // Check if gcloud storage is available
        let gcloud_check = Command::new("gcloud")
            .args(["storage", "--help"])
            .output();

        if gcloud_check.is_err() || !gcloud_check.unwrap().status.success() {
            return Err(anyhow!(
                "gcloud storage command not found. Please install Google Cloud SDK"
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

        for file_name in required_files {
            let local_path = self.local_cache_dir.join(&file_name);

            // Check if file already exists locally
            if local_path.exists() {
                println!("Using cached file: {}", file_name);
                downloaded_files.push(local_path);
                continue;
            }

            // Download the file
            println!("Downloading: {}", file_name);
            match self.download_file(&file_name, &local_path).await {
                Ok(_) => {
                    downloaded_files.push(local_path);
                }
                Err(e) => {
                    eprintln!("Warning: Failed to download {}: {}", file_name, e);
                    // Continue with other files - some files might not exist
                }
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
            if stderr.contains("not found") || stderr.contains("does not exist") || stderr.contains("No such object") {
                return Err(anyhow!("File not found in GCS: {}", file_name));
            } else {
                return Err(anyhow!("GCS download failed: {}", stderr));
            }
        }

        Ok(())
    }

    /// List available files in the GCS bucket for debugging/discovery
    pub async fn list_available_files(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        let gcs_pattern = match prefix {
            Some(p) => format!("gs://{}/{}{}", BUCKET_NAME, GCS_PREFIX, p),
            None => format!("gs://{}/{}**", BUCKET_NAME, GCS_PREFIX),
        };

        let output = Command::new("gcloud")
            .args(["storage", "ls", &gcs_pattern])
            .output()
            .map_err(|e| anyhow!("Failed to run gcloud storage ls: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("GCS list failed: {}", stderr));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut file_names = Vec::new();

        for line in stdout.lines() {
            if let Some(file_name) = line.strip_prefix(&format!("gs://{}/{}", BUCKET_NAME, GCS_PREFIX)) {
                if !file_name.is_empty() && !file_name.ends_with('/') {
                    file_names.push(file_name.to_string());
                }
            }
        }

        Ok(file_names)
    }

    /// Clean up old cached files (older than specified days)
    pub async fn cleanup_old_cache(&self, days_old: u64) -> Result<()> {
        let cutoff_time = std::time::SystemTime::now() - std::time::Duration::from_secs(days_old * 24 * 60 * 60);

        let entries = fs::read_dir(&self.local_cache_dir)?;
        let mut files_removed = 0;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() && path.extension().is_some_and(|ext| ext == "zst") {
                let metadata = fs::metadata(&path)?;
                
                if let Ok(modified) = metadata.modified() {
                    if modified < cutoff_time {
                        if let Err(e) = fs::remove_file(&path) {
                            eprintln!("Failed to remove old cache file {:?}: {}", path, e);
                        } else {
                            files_removed += 1;
                        }
                    }
                }
            }
        }

        if files_removed > 0 {
            println!("Cleaned up {} old cache files", files_removed);
        }

        Ok(())
    }

    /// Get the local cache directory path
    pub fn cache_dir(&self) -> &Path {
        &self.local_cache_dir
    }

    /// Check if gcloud storage is properly configured and authenticated
    pub async fn check_authentication(&self) -> Result<()> {
        let output = Command::new("gcloud")
            .args(["storage", "ls", &format!("gs://{}", BUCKET_NAME)])
            .output()
            .map_err(|e| anyhow!("Failed to run gcloud storage: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("Anonymous caller") || stderr.contains("authentication") || stderr.contains("not authenticated") {
                return Err(anyhow!(
                    "Not authenticated with Google Cloud. Please run 'gcloud auth login' or set GOOGLE_APPLICATION_CREDENTIALS"
                ));
            } else {
                return Err(anyhow!("Failed to access GCS bucket: {}", stderr));
            }
        }

        Ok(())
    }
}