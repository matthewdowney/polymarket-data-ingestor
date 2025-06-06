use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use std::env;
use std::path::PathBuf;

use polymarket_data_ingestor::replay::{HistoricalDataReader, ReplayConfig};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    
    // Handle help before argument length check
    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h") {
        print_usage(&args[0]);
        return Ok(());
    }
    
    if args.len() < 3 {
        print_usage(&args[0]);
        return Err(anyhow!("Insufficient arguments"));
    }

    let config = parse_args(&args[1..])?;
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

fn parse_args(args: &[String]) -> Result<ReplayConfig> {
    let mut start_timestamp: Option<DateTime<Utc>> = None;
    let mut end_timestamp: Option<DateTime<Utc>> = None;
    let mut data_directory: Option<PathBuf> = None;
    let mut markets_filter: Option<Vec<String>> = None;
    let mut download_from_gcs = false;
    let mut no_auto_download = false;
    let mut gcs_cache_directory: Option<PathBuf> = None;
    let mut gcs_credentials_path: Option<PathBuf> = None;
    
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--start" => {
                if i + 1 >= args.len() {
                    return Err(anyhow!("--start requires a timestamp argument"));
                }
                start_timestamp = Some(parse_timestamp(&args[i + 1])?);
                i += 2;
            }
            "--end" => {
                if i + 1 >= args.len() {
                    return Err(anyhow!("--end requires a timestamp argument"));
                }
                end_timestamp = Some(parse_timestamp(&args[i + 1])?);
                i += 2;
            }
            "--data-dir" => {
                if i + 1 >= args.len() {
                    return Err(anyhow!("--data-dir requires a path argument"));
                }
                data_directory = Some(PathBuf::from(&args[i + 1]));
                i += 2;
            }
            "--markets" => {
                if i + 1 >= args.len() {
                    return Err(anyhow!("--markets requires a comma-separated list"));
                }
                markets_filter = Some(
                    args[i + 1]
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .collect()
                );
                i += 2;
            }
            "--download-from-gcs" => {
                download_from_gcs = true;
                i += 1;
            }
            "--no-auto-download" => {
                no_auto_download = true;
                i += 1;
            }
            "--gcs-cache-dir" => {
                if i + 1 >= args.len() {
                    return Err(anyhow!("--gcs-cache-dir requires a path argument"));
                }
                gcs_cache_directory = Some(PathBuf::from(&args[i + 1]));
                i += 2;
            }
            "--gcs-credentials" => {
                if i + 1 >= args.len() {
                    return Err(anyhow!("--gcs-credentials requires a path argument"));
                }
                gcs_credentials_path = Some(PathBuf::from(&args[i + 1]));
                i += 2;
            }
            "--help" | "-h" => {
                print_usage("replay");
                std::process::exit(0);
            }
            arg => {
                return Err(anyhow!("Unknown argument: {}", arg));
            }
        }
    }
    
    let start = start_timestamp.ok_or_else(|| anyhow!("--start timestamp is required"))?;
    let end = end_timestamp.ok_or_else(|| anyhow!("--end timestamp is required"))?;
    let data_dir = data_directory.unwrap_or_else(|| PathBuf::from("./data"));
    
    if start >= end {
        return Err(anyhow!("Start timestamp must be before end timestamp"));
    }
    
    let mut config = ReplayConfig::new(start, end, data_dir);
    
    if let Some(markets) = markets_filter {
        config = config.with_markets_filter(markets);
    }
    
    if no_auto_download {
        config = config.with_no_auto_download(true);
    }
    
    if download_from_gcs {
        let cache_dir = gcs_cache_directory.unwrap_or_else(|| 
            config.data_directory.join("gcs_cache")
        );
        config = config.with_gcs_download(cache_dir);
        
        if let Some(creds_path) = gcs_credentials_path {
            config = config.with_gcs_credentials(creds_path);
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

fn print_usage(program_name: &str) {
    println!("Usage: {} --start <timestamp> --end <timestamp> [OPTIONS]", program_name);
    println!();
    println!("Replay historical Polymarket order book data between two timestamps.");
    println!("Outputs JSON messages to stdout for piping to other tools.");
    println!();
    println!("REQUIRED ARGUMENTS:");
    println!("  --start <timestamp>     Start timestamp (RFC3339, ISO, or YYYY-MM-DD format)");
    println!("  --end <timestamp>       End timestamp (RFC3339, ISO, or YYYY-MM-DD format)");
    println!();
    println!("OPTIONS:");
    println!("  --data-dir <path>       Path to data directory (default: ./data)");
    println!("  --markets <list>        Comma-separated list of market IDs to filter");
    println!("  --download-from-gcs     Download missing files from GCS bucket (requires gcloud)");
    println!("  --no-auto-download      Disable automatic download of missing data from GCS");
    println!("  --gcs-cache-dir <path>  Directory for GCS cache (default: <data-dir>/gcs_cache)");
    println!("  --gcs-credentials <path> Path to GCS credentials JSON file");
    println!("  --help, -h              Show this help message");
    println!();
    println!("EXAMPLES:");
    println!("  # Replay one hour of data");
    println!("  {} --start 2024-01-01T12:00:00Z --end 2024-01-01T13:00:00Z", program_name);
    println!();
    println!("  # Replay full day with custom data directory");
    println!("  {} --start 2024-01-01 --end 2024-01-02 --data-dir /path/to/data", program_name);
    println!();
    println!("  # Filter specific markets and pipe to processor");
    println!("  {} --start 2024-01-01T12:00:00Z --end 2024-01-01T13:00:00Z \\", program_name);
    println!("    --markets market1,market2,market3 | my_processor");
    println!();
    println!("  # Download from GCS with custom cache directory");
    println!("  {} --start 2024-01-01T12:00:00Z --end 2024-01-01T13:00:00Z \\", program_name);
    println!("    --download-from-gcs --gcs-cache-dir /tmp/replay_cache");
    println!();
    println!("  # Use custom GCS credentials");
    println!("  {} --start 2024-01-01T12:00:00Z --end 2024-01-01T13:00:00Z \\", program_name);
    println!("    --download-from-gcs --gcs-credentials /path/to/service-account.json");
    println!();
    println!("  # Disable automatic download (only use local files)");
    println!("  {} --start 2024-01-01T12:00:00Z --end 2024-01-01T13:00:00Z --no-auto-download", program_name);
    println!();
    println!("TIMESTAMP FORMATS:");
    println!("  RFC3339:        2024-01-01T12:00:00Z");
    println!("  ISO (UTC):      2024-01-01T12:00:00");
    println!("  Date only:      2024-01-01 (becomes 2024-01-01T00:00:00Z)");
}