pub mod args;
pub mod file_reader;
pub mod gcs_downloader;
pub mod kalshi_tick_generator;
pub mod tick_generator;

use crate::args::MarketsArgs;
use anyhow::{anyhow, Result};
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use chrono::DateTime;
use data_collector::{KalshiMarket, PolymarketMarket};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rust_decimal::{prelude::*, Decimal};
use serde::{Deserialize, Serialize};
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::{
    fs::File,
    io::{BufRead, BufReader},
    sync::Arc,
};

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

/// Shared structs for all venues and tick writers
#[derive(Serialize, Debug, Clone)]
pub struct Row {
    timestamp: String,
    kind: &'static str, // e.g. "BBO" or "TRADE"
    market: String,
    asset: Option<String>,
    side: Side,
    price: Decimal,
    size: Decimal,
}

#[derive(Serialize, Debug, Clone)]
pub enum Side {
    Bid,
    Ask,
}

/// Each log line is a JSON message frame
#[derive(Deserialize)]
#[allow(dead_code)]
struct MessageFrame {
    timestamp: String,
    message_type: String,
    /// When message_type = "feed_message", this is a string-encoded JSON array of FeedMessages
    content: serde_json::Value,
}

/// Unified TickWriter
pub struct TickWriter {
    writer: ArrowWriter<File>,
    schema: Arc<Schema>,
    batch_size: usize,
    timestamps: Vec<String>,
    kinds: Vec<String>,
    markets: Vec<String>,
    assets: Vec<Option<String>>,
    sides: Vec<String>,
    prices: Vec<f64>,
    sizes: Vec<f64>,
}

impl TickWriter {
    pub fn new(path: PathBuf) -> Result<Self> {
        let file = File::create(path)?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Utf8, false),
            Field::new("kind", DataType::Utf8, false),
            Field::new("market", DataType::Utf8, false),
            Field::new("asset", DataType::Utf8, true),
            Field::new("side", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("size", DataType::Float64, false),
        ]));

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();

        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        Ok(Self {
            writer,
            schema,
            batch_size: 10000,
            timestamps: Vec::new(),
            kinds: Vec::new(),
            markets: Vec::new(),
            assets: Vec::new(),
            sides: Vec::new(),
            prices: Vec::new(),
            sizes: Vec::new(),
        })
    }

    fn write_tick(&mut self, row: Row) -> Result<()> {
        self.timestamps.push(row.timestamp);
        self.kinds.push(row.kind.to_string());
        self.markets.push(row.market);
        self.assets.push(row.asset);
        self.sides.push(match row.side {
            Side::Bid => "BID".to_string(),
            Side::Ask => "ASK".to_string(),
        });
        self.prices.push(row.price.to_f64().unwrap());
        self.sizes.push(row.size.to_f64().unwrap());

        if self.timestamps.len() >= self.batch_size {
            self.flush_batch()?;
        }

        Ok(())
    }

    fn flush_batch(&mut self) -> Result<()> {
        if self.timestamps.is_empty() {
            return Ok(());
        }

        let timestamp_arr = Arc::new(StringArray::from(
            self.timestamps.drain(..).collect::<Vec<_>>(),
        ));
        let kind_arr = Arc::new(StringArray::from(self.kinds.drain(..).collect::<Vec<_>>()));
        let market_arr = Arc::new(StringArray::from(
            self.markets.drain(..).collect::<Vec<_>>(),
        ));
        let asset_arr = Arc::new(StringArray::from(self.assets.drain(..).collect::<Vec<_>>()));
        let side_arr = Arc::new(StringArray::from(self.sides.drain(..).collect::<Vec<_>>()));
        let price_arr = Arc::new(Float64Array::from(
            self.prices.drain(..).collect::<Vec<_>>(),
        ));
        let size_arr = Arc::new(Float64Array::from(self.sizes.drain(..).collect::<Vec<_>>()));

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                timestamp_arr,
                kind_arr,
                market_arr,
                asset_arr,
                side_arr,
                price_arr,
                size_arr,
            ],
        )?;

        self.writer.write(&batch)?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        self.flush_batch()?;
        self.writer.close()?;
        Ok(())
    }
}

pub trait HasMarketFilter {
    fn with_market_filter(&mut self, markets: Vec<String>);
}

pub trait VenueHandler {
    fn print_markets(&self, markets: serde_json::Value, args: &MarketsArgs) -> Result<()>;

    fn write_ticks(
        &self,
        files: &[PathBuf],
        output_path: PathBuf,
        markets: Option<Vec<String>>,
    ) -> Result<()>;
}
pub struct PolymarketHandler;
impl VenueHandler for PolymarketHandler {
    fn write_ticks(
        &self,
        files: &[PathBuf],
        output_path: PathBuf,
        markets: Option<Vec<String>>,
    ) -> Result<()>
    {
        let mut state = tick_generator::MarketState::default();
        if let Some(markets) = markets {
            state.with_market_filter(markets);
        }
        let mut writer = TickWriter::new(output_path)?;
        for f in files {
            let mut reader = BufReader::new(zstd::Decoder::new(File::open(f)?)?);
            let mut line = String::new();
            loop {
                // Read next JSONL
                line.clear();
                if reader.read_line(&mut line)? == 0 {
                    break;
                }

                // Decode the frame and check if it contains feed messages or if we should skip
                let frame: MessageFrame = serde_json::from_str(&line)?;
                let msgs: Vec<tick_generator::FeedMessage> = match frame.content {
                    serde_json::Value::String(s) if s != "PONG" => {
                        serde_json::from_str::<Vec<tick_generator::FeedMessage>>(&s)?
                    }
                    _ => continue,
                };

                // Update the market state for each feed message
                for msg in msgs {
                    state.update(msg, &mut writer)?;
                }
            }
        }
        writer.finish()?;
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
    fn write_ticks(
        &self,
        files: &[PathBuf],
        output_path: PathBuf,
        markets: Option<Vec<String>>,
    ) -> Result<()>
    {
        let mut state = kalshi_tick_generator::MarketState::default();
        if let Some(markets) = markets {
            state.with_market_filter(markets);
        }
        let mut writer = TickWriter::new(output_path)?;
        for f in files {
            let mut reader = BufReader::new(zstd::Decoder::new(File::open(f)?)?);
            let mut line = String::new();
            loop {
                // Read next JSONL
                line.clear();
                if reader.read_line(&mut line)? == 0 {
                    break;
                }

                // Decode the frame and check if it contains feed messages or if we should skip
                let frame: MessageFrame = serde_json::from_str(&line)?;
                let frame_ts = parse_timestamp(&frame.timestamp)?;
                let msgs: Vec<kalshi_tick_generator::FeedMessage> = match frame.content {
                    serde_json::Value::String(s) if s != "PONG" => {
                        vec![serde_json::from_str::<kalshi_tick_generator::FeedMessage>(&s)?]
                    }
                    _ => continue,
                };

                // Update the market state for each feed message
                for msg in msgs {
                    state.update(msg, &mut writer, frame_ts.clone())?;
                }
            }
        }
        writer.finish()?;
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

pub fn read_market_info(path: &Path) -> Result<serde_json::Value> {
    let mut reader = BufReader::new(zstd::Decoder::new(File::open(path)?)?);

    let mut line = String::new();
    loop {
        // Read next JSONL
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }

        // Decode the frame and check if it contains feed messages or if we should skip
        let frame: MessageFrame = serde_json::from_str(&line)?;
        if frame.message_type == "active_markets" {
            return Ok(frame.content);
        }
    }

    Err(anyhow::anyhow!("no active_markets message found"))
}

fn parse_timestamp(timestamp: &str) -> Result<String> {
    Ok(DateTime::parse_from_rfc3339(timestamp)?.to_string())
}
