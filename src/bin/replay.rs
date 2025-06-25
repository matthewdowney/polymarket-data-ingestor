use anyhow::{anyhow, Result};
use chrono::{DateTime, DurationRound, Utc};
use clap::{CommandFactory, Parser};
use std::path::PathBuf;

use polymarket_data_ingestor::replay::HistoricalDataReader;

#[derive(Parser)]
#[command(name = "replay")]
/// Download historical Polymarket order book data for a given timefram.
struct Args {
    /// A duration string in hours or days (e.g. "12h", "2d")
    #[arg(long, short = 't')]
    since: Option<String>,

    /// Start timestamp (RFC3339, ISO, or YYYY-MM-DD format)
    #[arg(long)]
    start: Option<String>,

    /// End timestamp (RFC3339, ISO, or YYYY-MM-DD format)
    #[arg(long)]
    end: Option<String>,

    /// Path to data directory (default: ./data)
    #[arg(long, help = "Path to data directory (default: ./data)")]
    data_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.since.is_none() && args.start.is_none() && args.end.is_none() {
        let _ = Args::command().print_help();
        std::process::exit(1);
    }

    let (start, end) = parse_time_range(&args)?;

    let data_dir = args
        .data_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("./data"));
    let cache_dir = data_dir.join("gcs_cache");
    let reader = HistoricalDataReader::new(cache_dir, start, end);
    reader.download_from_gcs().await?;

    // Discover files in cache directory
    let files = reader.discover_files_with_gcs_cache()?;
    for file in files {
        println!("{}", file.display());
    }

    Ok(())
}

fn parse_time_range(args: &Args) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
    if let Some(since) = &args.since {
        let duration = if since.ends_with("h") {
            let hours = since.trim_end_matches("h").parse::<i64>()?;
            chrono::Duration::hours(hours)
        } else if since.ends_with("d") {
            let days = since.trim_end_matches("d").parse::<i64>()?;
            chrono::Duration::days(days)
        } else {
            return Err(anyhow!("Invalid duration string: {}", since));
        };

        let end =
            Utc::now().duration_trunc(chrono::Duration::hours(1))? - chrono::Duration::minutes(1);
        let start = Utc::now() - duration;
        return Ok((start, end));
    }

    let start = args
        .start
        .as_ref()
        .map(|s| parse_timestamp(s))
        .expect("start is required")?;
    let end = args
        .end
        .as_ref()
        .map(|s| parse_timestamp(s))
        .expect("end is required")?;
    Ok((start, end))
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
