pub mod args;
pub mod file_reader;
pub mod gcs_downloader;
pub mod kalshi_tick_generator;
pub mod tick_generator;

use crate::args::MarketsArgs;
use anyhow::{anyhow, Result};
use data_collector::{KalshiMarket, PolymarketMarket};
use std::io::IsTerminal;
use std::path::{Path, PathBuf};

#[derive(clap::ValueEnum, Debug, Clone)]
pub enum Venue {
    Polymarket,
    Kalshi,
}

/// Terminal color codes - empty strings if not outputting to terminal
fn get_colors() -> (&'static str, &'static str, &'static str) {
    if std::io::stdout().is_terminal() {
        ("\x1b[90m", "\x1b[32m", "\x1b[0m") // gray, green, reset
    } else {
        ("", "", "") // no colors when piped
    }
}

pub trait VenueHandler {
    fn read_market_info(&self, path: &Path) -> Result<serde_json::Value>;
    fn write_ticks(
        &self,
        files: &[PathBuf],
        output_path: PathBuf,
        markets: Option<Vec<String>>,
    ) -> Result<()>;
    fn print_markets(&self, markets: serde_json::Value, args: &MarketsArgs) -> Result<()>;
}

pub struct PolymarketHandler;
impl VenueHandler for PolymarketHandler {
    fn read_market_info(&self, path: &Path) -> Result<serde_json::Value> {
        tick_generator::read_market_info(path)
    }

    fn write_ticks(
        &self,
        files: &[PathBuf],
        output_path: PathBuf,
        markets: Option<Vec<String>>,
    ) -> Result<()> {
        let mut state = tick_generator::MarketState::default();
        if let Some(markets) = markets {
            state.with_market_filter(markets);
        }
        let mut parquet_writer = tick_generator::ParquetTickWriter::new(output_path)?;
        for f in files {
            tick_generator::write_ticks(f, &mut state, &mut parquet_writer)?;
        }
        parquet_writer.finish()?;
        Ok(())
    }

    fn print_markets(&self, mut msg: serde_json::Value, args: &crate::MarketsArgs) -> Result<()> {
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
}

pub struct KalshiHandler;
impl VenueHandler for KalshiHandler {
    fn read_market_info(&self, path: &Path) -> Result<serde_json::Value> {
        kalshi_tick_generator::read_market_info(path)
    }

    fn write_ticks(
        &self,
        files: &[PathBuf],
        output_path: PathBuf,
        markets: Option<Vec<String>>,
    ) -> Result<()> {
        let mut state = kalshi_tick_generator::MarketState::default();
        if let Some(markets) = markets {
            state.with_market_filter(markets);
        }
        let mut parquet_writer = kalshi_tick_generator::ParquetTickWriter::new(output_path)?;
        for f in files {
            kalshi_tick_generator::write_ticks(f, &mut state, &mut parquet_writer)?;
        }
        parquet_writer.finish()?;
        Ok(())
    }

    fn print_markets(&self, mut msg: serde_json::Value, args: &MarketsArgs) -> Result<()> {
        let (gray, _, reset) = get_colors();
        let markets = msg
            .get_mut("markets")
            .ok_or(anyhow!("no markets field found"))?
            .take();
        let markets: Vec<KalshiMarket> = serde_json::from_value(markets)?;
        if args.csv {
            println!("title,ticker");
        }
        for market in markets {
            if let Some(filter) = args.filter.as_ref() {
                if !market.title.to_lowercase().contains(&filter.to_lowercase()) {
                    continue;
                }
            }
            if args.csv {
                println!("{},{}", market.title, market.ticker);
            } else {
                println!("{}", market.title);
                println!("  {}{}{}", gray, market.ticker, reset);
            }
        }
        Ok(())
    }
}

pub fn get_handler(venue: Venue) -> Box<dyn VenueHandler> {
    match venue {
        Venue::Polymarket => Box::new(PolymarketHandler),
        Venue::Kalshi => Box::new(KalshiHandler),
    }
}
