use anyhow::{anyhow, Result};
use chrono::{DateTime, DurationRound, Utc};
use clap::{CommandFactory, Parser, Subcommand};
use cli::file_reader::HistoricalDataReader;
use data_collector::PolymarketMarket;
use std::{
    io::{IsTerminal, Write},
    path::PathBuf,
};

/// Directory where raw feed logs are cached
const DATA_DIR: &str = "./data/gcs_cache";

/// Terminal color codes - empty strings if not outputting to terminal
fn get_colors() -> (&'static str, &'static str, &'static str) {
    if std::io::stdout().is_terminal() {
        ("\x1b[90m", "\x1b[32m", "\x1b[0m") // gray, green, reset
    } else {
        ("", "", "") // no colors when piped
    }
}

#[derive(Parser)]
#[command(name = "cli")]
/// Polymarket historical data download and replay tools
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Download data for a given timeframe
    Download(DownloadArgs),
    /// Replay raw messages and generate tick data
    Replay(ReplayArgs),
    /// Print information about the listed markets
    Markets(MarketsArgs),
}

#[derive(Parser)]
/// Download data for a given timeframe
struct DownloadArgs {
    /// A duration string in hours or days (e.g. "12h", "2d")
    #[arg(long, short = 't')]
    since: Option<String>,

    /// Start timestamp (RFC3339, ISO, or YYYY-MM-DD format)
    #[arg(long)]
    start: Option<String>,

    /// End timestamp (RFC3339, ISO, or YYYY-MM-DD format)
    #[arg(long)]
    end: Option<String>,
}

#[derive(Parser)]
/// Replay raw messages and generate tick data
struct ReplayArgs {
    /// A duration string in hours or days (e.g. "12h", "2d")
    #[arg(long, short = 't')]
    since: Option<String>,

    /// Start timestamp (RFC3339, ISO, or YYYY-MM-DD format)
    #[arg(long)]
    start: Option<String>,

    /// End timestamp (RFC3339, ISO, or YYYY-MM-DD format)
    #[arg(long)]
    end: Option<String>,

    /// Path to the output CSV file (defaults to stdout)
    #[arg(long, short)]
    output: Option<String>,

    /// Specific market ids to replay (can be specified multiple times)
    #[arg(long, short = 'm', num_args = 1..)]
    markets: Option<Vec<String>>,
}

#[derive(Parser)]
/// Print information about the listed markets
struct MarketsArgs {
    /// Optional market name filter (case-insensitive)
    filter: Option<String>,

    /// How long ago to query market info from (e.g. "12h", "2d")
    #[arg(long, short = 't')]
    since: Option<String>,

    /// Date on which to query market info (RFC3339, ISO, or YYYY-MM-DD format)
    #[arg(long)]
    start: Option<String>,

    /// Print raw JSON (default: false)
    #[arg(long, default_value_t = false)]
    raw: bool,

    /// Print as CSV (default: false)
    #[arg(long, default_value_t = false)]
    csv: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    match &args.command {
        Commands::Download(download_args) => run_download(download_args).await,
        Commands::Replay(replay_args) => run_replay(replay_args).await,
        Commands::Markets(markets_args) => run_markets(markets_args).await,
    }
}

async fn run_download(args: &DownloadArgs) -> Result<()> {
    if args.since.is_none() && args.start.is_none() && args.end.is_none() {
        let _ = DownloadArgs::command().print_help();
        std::process::exit(1);
    }
    let (start, end) = parse_time_range(args.since.clone(), args.start.clone(), args.end.clone())?;

    let cache_dir = PathBuf::from(DATA_DIR);
    let reader = HistoricalDataReader::new(cache_dir, start, end);
    reader.download_from_gcs().await?;

    // Discover files in cache directory
    let files = reader.discover_files_with_gcs_cache()?;
    for file in files {
        println!("{}", file.display());
    }

    Ok(())
}

async fn run_replay(args: &ReplayArgs) -> Result<()> {
    if args.since.is_none() && args.start.is_none() && args.end.is_none() {
        let _ = DownloadArgs::command().print_help();
        std::process::exit(1);
    }

    let (start, end) = parse_time_range(args.since.clone(), args.start.clone(), args.end.clone())?;
    let cache_dir = PathBuf::from(DATA_DIR);
    let reader = HistoricalDataReader::new(cache_dir, start, end);

    // Read the files in order, keep track of market state, and write ticks to the output file
    let mut state = cli::tick_generator::MarketState::default();
    if let Some(markets) = args.markets.clone() {
        state.with_market_filter(markets);
    }

    let out: Box<dyn Write> = if let Some(output) = args.output.clone() {
        Box::new(std::fs::File::create(output)?)
    } else {
        Box::new(std::io::stdout())
    };
    let mut writer = csv::Writer::from_writer(out);

    // Discover files in cache directory
    let files = reader.discover_files_with_gcs_cache()?;
    for file in files {
        cli::tick_generator::write_ticks(&file, &mut state, &mut writer)?;
    }

    Ok(())
}

async fn run_markets(args: &MarketsArgs) -> Result<()> {
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let start = args.start.clone().unwrap_or("1970-01-01".to_string());

    let (start, end) = parse_time_range(args.since.clone(), Some(start), Some(today))?;
    let cache_dir = PathBuf::from(DATA_DIR);
    let reader = HistoricalDataReader::new(cache_dir, start, end);

    // Use first file in range if provided, otherwise use most recent file
    let files = reader.discover_files_with_gcs_cache()?;
    let file = if args.since.is_none() && args.start.is_none() {
        files.into_iter().rev().next()
    } else {
        files.into_iter().next()
    };

    if let Some(file) = file {
        let msg = cli::tick_generator::read_market_info(&file)?;
        if args.raw {
            println!("{}", serde_json::to_string(&msg)?);
        } else {
            print_markets(msg, args)?;
        }
    } else {
        eprintln!(
            "No data files found in time range -- use the `download` command to download data"
        );
        std::process::exit(1);
    }

    Ok(())
}

fn print_markets(mut msg: serde_json::Value, args: &MarketsArgs) -> Result<()> {
    let (gray, green, reset) = get_colors();
    let markets = msg
        .get_mut("markets")
        .ok_or(anyhow!("no markets field found"))?
        .take();
    let markets: Vec<PolymarketMarket> = serde_json::from_value(markets)?;
    if args.csv {
        println!("question,question_id,outcome,token_id");
    }
    for market in markets {
        if let Some(filter) = args.filter.as_ref() {
            if !market
                .question
                .to_lowercase()
                .contains(&filter.to_lowercase())
            {
                continue;
            }
        }
        if args.csv {
            for token in market.tokens {
                println!(
                    "{},{},{},{}",
                    market.question, market.question_id, token.outcome, token.token_id
                );
            }
        } else {
            println!("{}", market.question);
            println!("  {}{}{}", gray, market.condition_id, reset);
            for token in market.tokens {
                print!("  {}{:<10}{}", green, token.outcome, reset);
                println!("  {}{}{}", gray, token.token_id, reset);
            }
        }
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
