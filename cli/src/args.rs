use crate::Venue;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "cli")]
/// Polymarket historical data download and replay tools
pub struct Args {
    #[command(subcommand)]
    pub command: Commands,
    #[arg(value_enum)]
    pub venue: Venue,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Download data for a given timeframe
    Download(DownloadArgs),
    /// Replay raw messages and generate tick data
    Replay(ReplayArgs),
    /// Print information about the listed markets
    Markets(MarketsArgs),
}

#[derive(Parser)]
/// Download data for a given timeframe
pub struct DownloadArgs {
    /// A duration string in hours or days (e.g. "12h", "2d")
    #[arg(long, short = 't')]
    pub since: Option<String>,

    /// Start timestamp (RFC3339, ISO, or YYYY-MM-DD format)
    #[arg(long)]
    pub start: Option<String>,

    /// End timestamp (RFC3339, ISO, or YYYY-MM-DD format)
    #[arg(long)]
    pub end: Option<String>,
}

#[derive(Parser)]
/// Replay raw messages and generate tick data
pub struct ReplayArgs {
    /// A duration string in hours or days (e.g. "12h", "2d")
    #[arg(long, short = 't')]
    pub since: Option<String>,

    /// Start timestamp (RFC3339, ISO, or YYYY-MM-DD format)
    #[arg(long)]
    pub start: Option<String>,

    /// End timestamp (RFC3339, ISO, or YYYY-MM-DD format)
    #[arg(long)]
    pub end: Option<String>,

    /// Path to the output CSV file (defaults to stdout)
    #[arg(long, short)]
    pub output: Option<String>,

    /// Specific market ids to replay (can be specified multiple times)
    #[arg(long, short = 'm', num_args = 1..)]
    pub markets: Option<Vec<String>>,
}

#[derive(Parser)]
/// Print information about the listed markets
pub struct MarketsArgs {
    /// Optional market name filter (case-insensitive)
    pub filter: Option<String>,

    /// How long ago to query market info from (e.g. "12h", "2d")
    #[arg(long, short = 't')]
    pub since: Option<String>,

    /// Date on which to query market info (RFC3339, ISO, or YYYY-MM-DD format)
    #[arg(long)]
    pub start: Option<String>,

    /// Print raw JSON (default: false)
    #[arg(long, default_value_t = false)]
    pub raw: bool,

    /// Print as CSV (default: false)
    #[arg(long, default_value_t = false)]
    pub csv: bool,
}
