use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use std::path::PathBuf;

use polymarket_data_ingestor::replay::HistoricalDataReader;

#[derive(Parser)]
#[command(name = "replay")]
#[command(about = "Download historical Polymarket order book data between two timestamps.")]
#[command(
    long_about = "Download historical Polymarket order book data between two timestamps. Outputs the list of files to stdout for piping to other tools."
)]
struct Args {
    /// Start timestamp (RFC3339, ISO, or YYYY-MM-DD format)
    #[arg(long, help = "Start timestamp (RFC3339, ISO, or YYYY-MM-DD format)")]
    start: String,

    /// End timestamp (RFC3339, ISO, or YYYY-MM-DD format)
    #[arg(long, help = "End timestamp (RFC3339, ISO, or YYYY-MM-DD format)")]
    end: String,

    /// Download missing files from GCS bucket (requires gcloud)
    #[arg(
        long,
        help = "Download missing files from GCS bucket (requires gcloud) (default: true)"
    )]
    download_from_gcs: Option<bool>,

    /// Path to data directory (default: ./data)
    #[arg(long, help = "Path to data directory (default: ./data)")]
    data_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let start = parse_timestamp(&args.start)?;
    let end = parse_timestamp(&args.end)?;
    let data_dir = args
        .data_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("./data"));
    let cache_dir = data_dir.join("gcs_cache");
    let reader = HistoricalDataReader::new(cache_dir, start, end);

    if args.download_from_gcs.unwrap_or(true) {
        reader.download_from_gcs().await?;
    }

    // Discover files in cache directory
    let files = reader.discover_files_with_gcs_cache()?;
    for file in files {
        println!("{}", file.display());
    }

    Ok(())
}

fn parse_timestamp(timestamp_str: &str) -> Result<DateTime<Utc>> {
    // Try parsing RFC3339 format first
    if let Ok(dt) = DateTime::parse_from_rfc3339(timestamp_str) {
        return Ok(dt.with_timezone(&Utc));
    }

    // Try parsing without timezone (assume UTC)
    if let Ok(dt) = DateTime::parse_from_str(&format!("{}Z", timestamp_str), "%Y-%m-%dT%H:%M:%SZ") {
        return Ok(dt.with_timezone(&Utc));
    }

    // Try parsing date only (start of day UTC)
    if let Ok(dt) = chrono::NaiveDate::parse_from_str(timestamp_str, "%Y-%m-%d") {
        return Ok(dt.and_hms_opt(0, 0, 0).unwrap().and_utc());
    }

    Err(anyhow!("Unable to parse timestamp: {}. Supported formats: RFC3339 (2024-01-01T12:00:00Z), ISO without timezone (2024-01-01T12:00:00), or date only (2024-01-01)", timestamp_str))
}
