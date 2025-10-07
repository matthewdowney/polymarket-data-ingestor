pub mod file_reader;
pub mod gcs_downloader;
pub mod tick_generator;
pub mod kalshi_tick_generator;

#[derive(clap::ValueEnum, Debug, Clone)]
pub enum Venue {
    Polymarket,
    Kalshi,
}