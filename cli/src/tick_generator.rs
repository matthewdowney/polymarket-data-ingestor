use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rust_decimal::{prelude::*, Decimal};
use serde::{Deserialize, Serialize};

pub fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("need file as arg"))?;
    let from_path = PathBuf::from(path);
    let to_path = from_path.with_extension("csv");

    let mut state = MarketState::default();
    let mut parquet_writer = ParquetTickWriter::new(to_path)?;
    write_ticks(&from_path, &mut state, &mut parquet_writer)?;
    parquet_writer.finish()?;

    Ok(())
}

pub struct ParquetTickWriter {
    writer: ArrowWriter<File>,
    schema: Arc<Schema>,
    batch_size: usize,
    timestamps: Vec<String>,
    kinds: Vec<String>,
    markets: Vec<String>,
    assets: Vec<String>,
    sides: Vec<String>,
    prices: Vec<f64>,
    sizes: Vec<f64>,
}

impl ParquetTickWriter {
    pub fn new(path: PathBuf) -> Result<Self> {
        let file = File::create(path)?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Utf8, false),
            Field::new("kind", DataType::Utf8, false),
            Field::new("market", DataType::Utf8, false),
            Field::new("asset", DataType::Utf8, false),
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

pub fn read_market_info(from_path: &Path) -> Result<serde_json::Value> {
    let mut reader = BufReader::new(zstd::Decoder::new(File::open(from_path)?)?);

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

/// Decompress and read the file, keeping track of market state, and write data points as CSV
pub fn write_ticks(
    from_path: &Path,
    state: &mut MarketState,
    writer: &mut ParquetTickWriter,
) -> Result<()> {
    let mut reader = BufReader::new(zstd::Decoder::new(File::open(from_path)?)?);

    let mut line = String::new();
    loop {
        // Read next JSONL
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }

        // Decode the frame and check if it contains feed messages or if we should skip
        let frame: MessageFrame = serde_json::from_str(&line)?;
        let msgs: Vec<FeedMessage> = match frame.content {
            serde_json::Value::String(s) if s != "PONG" => {
                serde_json::from_str::<Vec<FeedMessage>>(&s)?
            }
            _ => continue,
        };

        // Update the market state for each feed message
        for msg in msgs {
            state.update(msg, writer)?;
        }
    }

    Ok(())
}

/// Each tick (trade or book update) is seralized as a row
#[derive(Serialize, Debug, Clone)]
struct Row {
    timestamp: String,
    kind: &'static str, // "BBO" or "TRADE"
    market: String,
    asset: String,
    side: Side,
    price: Decimal,
    size: Decimal,
}

impl Row {
    fn from_trade(t: LastTradePriceMessage) -> Self {
        Self {
            timestamp: t.timestamp,
            kind: "TRADE",
            market: t.market,
            asset: t.asset_id,
            side: t.side,
            price: t.price,
            size: t.size,
        }
    }
}

/// Market state is updated with each message from the feed
#[derive(Default)]
pub struct MarketState {
    /// Asset id to order book
    books: HashMap<String, Book>,

    /// Market ids to replay
    market_ids: Option<HashSet<String>>,
}

/// Limit order book
#[derive(Default)]
struct Book {
    asks: BTreeMap<Decimal, Decimal>,
    bids: BTreeMap<Decimal, Decimal>,
}

impl Book {
    /// Overwrite book state from a Polymarket snapshot message
    fn reset_from_snapshot(&mut self, m: &BookSnapshotMessage) -> &Self {
        self.asks.clear();
        for lvl in m.asks.iter() {
            self.asks.insert(lvl.price, lvl.size);
        }

        self.bids.clear();
        for lvl in m.bids.iter() {
            self.bids.insert(lvl.price, lvl.size);
        }

        self
    }

    /// Update book state from a Polymarket diff message
    fn update_from_diff(&mut self, m: &BookDiffMessage) -> &Self {
        for lvl in m.changes.iter() {
            let book_side = match lvl.side {
                Side::Ask => &mut self.asks,
                Side::Bid => &mut self.bids,
            };

            if lvl.size.is_zero() {
                book_side.remove_entry(&lvl.price);
            } else {
                book_side.insert(lvl.price, lvl.size);
            }
        }

        self
    }

    fn top(&self, side: Side) -> (Decimal, Decimal) {
        match side {
            Side::Ask => self.asks.iter().next(),
            Side::Bid => self.bids.iter().next_back(),
        }
        .map(|(&px, &sz)| (px, sz))
        .unwrap_or_default()
    }

    fn write_bbo(
        &self,
        w: &mut ParquetTickWriter,
        timestamp: String,
        market: String,
        asset: String,
    ) -> Result<()> {
        let (px, sz) = self.top(Side::Ask);
        let row = Row {
            timestamp: timestamp.clone(),
            market: market.clone(),
            asset: asset.clone(),
            price: px,
            size: sz,
            side: Side::Ask,
            kind: "BBO",
        };
        w.write_tick(row)?;

        let (px, sz) = self.top(Side::Bid);
        let row = Row {
            timestamp: timestamp.clone(),
            market: market.clone(),
            asset: asset.clone(),
            price: px,
            size: sz,
            side: Side::Bid,
            kind: "BBO",
        };
        w.write_tick(row)?;

        Ok(())
    }
}

impl MarketState {
    /// Update the market state, write zero or more tick data rows with the writer
    fn update(&mut self, m: FeedMessage, w: &mut ParquetTickWriter) -> Result<()> {
        match m {
            FeedMessage::LastTradePrice(x) => {
                // Skip if market id is not in the filter
                if let Some(market_ids) = &self.market_ids {
                    if !market_ids.contains(&x.market) {
                        return Ok(());
                    }
                }
                w.write_tick(Row::from_trade(x))?;
            }
            FeedMessage::BookSnapshot(x) => {
                // Skip if market id is not in the filter
                if let Some(market_ids) = &self.market_ids {
                    if !market_ids.contains(&x.market) {
                        return Ok(());
                    }
                }

                self.books
                    .entry(x.asset_id.clone())
                    .or_default()
                    .reset_from_snapshot(&x)
                    .write_bbo(w, x.timestamp, x.market, x.asset_id)?;
            }
            FeedMessage::BookDiff(x) => {
                // Skip if market id is not in the filter
                if let Some(market_ids) = &self.market_ids {
                    if !market_ids.contains(&x.market) {
                        return Ok(());
                    }
                }

                self.books
                    .entry(x.asset_id.clone())
                    .or_default()
                    .update_from_diff(&x)
                    .write_bbo(w, x.timestamp, x.market, x.asset_id)?;
            }
            FeedMessage::Other => {}
        }
        Ok(())
    }

    pub fn with_market_filter(&mut self, markets: Vec<String>) {
        self.market_ids = Some(markets.into_iter().collect());
    }
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

#[derive(Deserialize)]
#[serde(tag = "event_type")]
enum FeedMessage {
    #[serde(rename = "last_trade_price")]
    LastTradePrice(LastTradePriceMessage),

    #[serde(rename = "book")]
    BookSnapshot(BookSnapshotMessage),

    #[serde(rename = "price_change")]
    BookDiff(BookDiffMessage),

    #[serde(other)]
    Other,
}

// Structs for serde

#[derive(Deserialize, Debug, Serialize)]
struct LastTradePriceMessage {
    market: String,
    asset_id: String,
    side: Side,
    price: Decimal,
    size: Decimal,
    timestamp: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
enum Side {
    #[serde(rename = "BUY")]
    Bid,
    #[serde(rename = "SELL")]
    Ask,
}

#[derive(Deserialize, Debug)]
struct BookSnapshotMessage {
    asks: Vec<Level>,
    bids: Vec<Level>,
    timestamp: String,
    market: String,
    asset_id: String,
}

#[derive(Deserialize, Debug)]
struct Level {
    price: Decimal,
    size: Decimal,
}

#[derive(Deserialize, Debug)]
struct DiffLevel {
    price: Decimal,
    size: Decimal,
    side: Side,
}

#[derive(Deserialize, Debug)]
struct BookDiffMessage {
    changes: Vec<DiffLevel>,
    timestamp: String,
    market: String,
    asset_id: String,
}
