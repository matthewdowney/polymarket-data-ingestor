use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use std::path::PathBuf;

use polymarket_data_ingestor::replay::{HistoricalDataReader, ReplayConfig};

#[derive(Parser)]
#[command(name = "replay")]
#[command(about = "Replay historical Polymarket order book data between two timestamps.")]
#[command(long_about = "Replay historical Polymarket order book data between two timestamps.\nOutputs JSON messages to stdout for piping to other tools.")]
#[command(after_long_help = "TIMESTAMP FORMATS:\n  RFC3339:        2024-01-01T12:00:00Z\n  ISO (UTC):      2024-01-01T12:00:00\n  Date only:      2024-01-01 (becomes 2024-01-01T00:00:00Z)\n\nEXAMPLES:\n  # Replay one hour of data\n  replay --start 2024-01-01T12:00:00Z --end 2024-01-01T13:00:00Z\n\n  # Replay full day with custom data directory\n  replay --start 2024-01-01 --end 2024-01-02 --data-dir /path/to/data\n\n  # Filter specific markets and pipe to processor\n  replay --start 2024-01-01T12:00:00Z --end 2024-01-01T13:00:00Z \\\n    --markets market1,market2,market3 | my_processor\n\n  # Download from GCS with custom cache directory\n  replay --start 2024-01-01T12:00:00Z --end 2024-01-01T13:00:00Z \\\n    --download-from-gcs --gcs-cache-dir /tmp/replay_cache\n\n  # Use custom GCS credentials\n  replay --start 2024-01-01T12:00:00Z --end 2024-01-01T13:00:00Z \\\n    --download-from-gcs --gcs-credentials /path/to/service-account.json\n\n  # Disable automatic download (only use local files)\n  replay --start 2024-01-01T12:00:00Z --end 2024-01-01T13:00:00Z --no-auto-download")]
struct Args {
    /// Start timestamp (RFC3339, ISO, or YYYY-MM-DD format)
    #[arg(long, help = "Start timestamp (RFC3339, ISO, or YYYY-MM-DD format)")]
    start: String,

    /// End timestamp (RFC3339, ISO, or YYYY-MM-DD format)
    #[arg(long, help = "End timestamp (RFC3339, ISO, or YYYY-MM-DD format)")]
    end: String,

    /// Path to data directory (default: ./data)
    #[arg(long, help = "Path to data directory (default: ./data)")]
    data_dir: Option<PathBuf>,

    /// Comma-separated list of market IDs to filter
    #[arg(long, help = "Comma-separated list of market IDs to filter")]
    markets: Option<String>,

    /// Download missing files from GCS bucket (requires gcloud)
    #[arg(long, help = "Download missing files from GCS bucket (requires gcloud)")]
    download_from_gcs: bool,

    /// Disable automatic download of missing data from GCS
    #[arg(long, help = "Disable automatic download of missing data from GCS")]
    no_auto_download: bool,

    /// Directory for GCS cache (default: <data-dir>/gcs_cache)
    #[arg(long, help = "Directory for GCS cache (default: <data-dir>/gcs_cache)")]
    gcs_cache_dir: Option<PathBuf>,

    /// Path to GCS credentials JSON file
    #[arg(long, help = "Path to GCS credentials JSON file")]
    gcs_credentials: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = build_config(&args)?;
    let reader = HistoricalDataReader::new(config);
    
    // Get streaming iterator in chronological order (with automatic download detection)
    let message_iter = reader.stream_all_messages_with_auto_download().await?;
    
    // Output each message as JSON to stdout for piping
    for message_result in message_iter {
        match message_result {
            Ok(message) => {
                println!("{}", serde_json::to_string(&message)?);
            }
            Err(e) => {
                eprintln!("Error processing message: {}", e);
            }
        }
    }
    
    Ok(())
}

fn build_config(args: &Args) -> Result<ReplayConfig> {
    let start = parse_timestamp(&args.start)?;
    let end = parse_timestamp(&args.end)?;
    let data_dir = args.data_dir.clone().unwrap_or_else(|| PathBuf::from("./data"));
    
    if start >= end {
        return Err(anyhow!("Start timestamp must be before end timestamp"));
    }
    
    let mut config = ReplayConfig::new(start, end, data_dir);
    
    if let Some(markets_str) = &args.markets {
        let markets_filter: Vec<String> = markets_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        config = config.with_markets_filter(markets_filter);
    }
    
    if args.no_auto_download {
        config = config.with_no_auto_download(true);
    }
    
    if args.download_from_gcs {
        let cache_dir = args.gcs_cache_dir.clone().unwrap_or_else(|| 
            config.data_directory.join("gcs_cache")
        );
        config = config.with_gcs_download(cache_dir);
        
        if let Some(creds_path) = &args.gcs_credentials {
            config = config.with_gcs_credentials(creds_path.clone());
        }
    }
    
    Ok(config)
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