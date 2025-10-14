use anyhow::{anyhow, Result};
use chrono::{DateTime, DurationRound, Utc};
use clap::{CommandFactory, Parser};
use cli::args::{Args, Commands, DownloadArgs, MarketsArgs, ReplayArgs};
use cli::file_reader::HistoricalDataReader;
use cli::get_handler;
use cli::read_market_info;
use cli::Venue;
use std::path::PathBuf;

/// Directory where raw feed logs are cached
const DATA_DIR: &str = "./data/gcs_cache";

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    match &args.command {
        Commands::Download(download_args) => run_download(download_args, args.venue).await,
        Commands::Replay(replay_args) => run_replay(replay_args, args.venue).await,
        Commands::Markets(markets_args) => run_markets(markets_args, args.venue).await,
    }
}

async fn run_download(args: &DownloadArgs, venue: Venue) -> Result<()> {
    if args.since.is_none() && args.start.is_none() && args.end.is_none() {
        let _ = DownloadArgs::command().print_help();
        std::process::exit(1);
    }
    let (start, end) = parse_time_range(args.since.clone(), args.start.clone(), args.end.clone())?;

    let cache_dir = PathBuf::from(DATA_DIR);
    let reader = HistoricalDataReader::new(cache_dir, start, end, venue.clone());
    reader.download_from_gcs().await?;

    // Discover files in cache directory
    let files = reader.discover_files_with_gcs_cache()?;
    for file in files {
        println!("{}", file.display());
    }

    Ok(())
}

async fn run_replay(args: &ReplayArgs, venue: Venue) -> Result<()> {
    if args.since.is_none() && args.start.is_none() && args.end.is_none() {
        let _ = DownloadArgs::command().print_help();
        std::process::exit(1);
    }

    let (start, end) = parse_time_range(args.since.clone(), args.start.clone(), args.end.clone())?;
    let cache_dir = PathBuf::from(DATA_DIR);
    let reader = HistoricalDataReader::new(cache_dir, start, end, venue.clone());

    // Read the files in order, keep track of market state, and write ticks to the output file
    let output_path = if let Some(output) = args.output.clone() {
        let mut path = PathBuf::from(output);
        if path.extension().is_none() {
            path.set_extension("parquet");
        }
        path
    } else {
        PathBuf::from("output.parquet")
    };

    let files = reader.discover_files_with_gcs_cache()?;
    let handler = get_handler(venue);
    handler.write_ticks(&files, output_path, args.markets.clone())?;

    Ok(())
}

async fn run_markets(args: &MarketsArgs, venue: Venue) -> Result<()> {
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let start = args.start.clone().unwrap_or("1970-01-01".to_string());

    let (start, end) = parse_time_range(args.since.clone(), Some(start), Some(today))?;
    let cache_dir = PathBuf::from(DATA_DIR);
    let reader = HistoricalDataReader::new(cache_dir, start, end, venue.clone());

    // Use first file in range if provided, otherwise use most recent file
    let files = reader.discover_files_with_gcs_cache()?;
    let file = files
        .into_iter()
        .next()
        .ok_or(anyhow!("no data files found"))?;

    let handler = get_handler(venue.clone());
    let msg = read_market_info(&file)?;

    if args.raw {
        println!("{}", serde_json::to_string(&msg)?);
    } else {
        handler.print_markets(msg, args)?;
    }

    Ok(())
}

fn parse_time_range(
    since: Option<String>,
    start: Option<String>,
    end: Option<String>,
) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
    if let Some(since) = since {
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

    let start = start.map(|s| parse_ts(&s)).expect("start required")?;
    let end = end.map(|s| parse_ts(&s)).expect("end required")?;
    Ok((start, end))
}

fn parse_ts(timestamp_str: &str) -> Result<DateTime<Utc>> {
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
