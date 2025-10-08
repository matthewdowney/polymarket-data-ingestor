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
use chrono::{DateTime, Utc};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rust_decimal::{prelude::*, Decimal};
use serde::{Deserialize, Serialize};

pub fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("need file as arg"))?;
    let from_path = PathBuf::from(path);
    let to_path = from_path.with_extension("parquet");

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
            sides: Vec::new(),
            prices: Vec::new(),
            sizes: Vec::new(),
        })
    }

    fn write_tick(&mut self, row: Row) -> Result<()> {
        self.timestamps.push(row.timestamp.to_string());
        self.kinds.push(row.kind.to_string());
        self.markets.push(row.market);
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
        let frame_ts = parse_timestamp(&frame.timestamp)?;
        let msgs: Vec<FeedMessage> = match frame.content {
            serde_json::Value::String(s) if s != "PONG" => {
                //eprintln!("S: {}", s);
                vec![serde_json::from_str::<FeedMessage>(&s)?]
            }
            _ => continue,
        };

        // Update the market state for each feed message
        for msg in msgs {
            state.update(msg, writer, frame_ts)?;
        }
    }

    Ok(())
}

fn parse_timestamp(timestamp: &str) -> Result<i64> {
    Ok(DateTime::parse_from_rfc3339(timestamp)?.timestamp_millis())
}

/// Each tick (trade or book update) is seralized as a row
#[derive(Serialize, Debug, Clone)]
struct Row {
    timestamp: i64,
    kind: &'static str, // "BBO" or "TRADE"
    market: String,
    side: Side,
    price: Decimal,
    size: Decimal,
}

impl Row {
    fn from_trade(t: &KalshiPublicTrade) -> Self {
        let (side, price, size) = match t.taker_side {
            KalshiSide::Yes => (Side::Bid, t.yes_price / Decimal::from(100), t.count),
            KalshiSide::No => (Side::Ask, t.yes_price / Decimal::from(100), t.count),
        };
        Self {
            timestamp: t.ts.timestamp_millis(),
            kind: "TRADE",
            market: t.market_ticker.clone(),
            side,
            price,
            size,
        }
    }
}

/// Market state is updated with each message from the feed
#[derive(Default)]
pub struct MarketState {
    /// Market ticker to order book
    books: HashMap<String, Book>,

    /// Market tickers to replay
    market_tickers: Option<HashSet<String>>,
}

/// Limit order book
#[derive(Default)]
struct Book {
    asks: BTreeMap<Decimal, Decimal>,
    bids: BTreeMap<Decimal, Decimal>,
    is_initialized: bool,
}

impl Book {
    /// Overwrite book state from a Kalshi snapshot message
    fn reset_from_snapshot(&mut self, m: &KalshiOrderbookSnapshot) -> &Self {
        self.asks.clear();
        for lvl in m.no.iter() {
            self.asks
                .insert((Decimal::from(100) - lvl[0]) / Decimal::from(100), lvl[1]);
        }

        self.bids.clear();
        for lvl in m.yes.iter() {
            self.bids.insert(lvl[0] / Decimal::from(100), lvl[1]);
        }

        self.is_initialized = true;
        self
    }

    /// Update book state from a Kalshi diff message
    fn update_from_delta(&mut self, m: &KalshiOrderbookDelta) -> &Self {
        if !self.is_initialized {
            return self;
        }
        let (book_side, price) = match m.side {
            KalshiSide::No => {
                let neg_price = (Decimal::from(100) - m.price) / Decimal::from(100);
                (&mut self.asks, neg_price)
            }
            KalshiSide::Yes => (&mut self.bids, m.price / Decimal::from(100)),
        };

        let current_size = book_side.get(&price).copied().unwrap_or(Decimal::ZERO);

        let new_size = current_size + m.delta;

        if new_size <= Decimal::ZERO {
            book_side.remove(&price);
        } else {
            book_side.insert(price, new_size);
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

    fn write_bbo(&self, w: &mut ParquetTickWriter, timestamp: i64, market: String) -> Result<()> {
        let (ask_px, ask_sz) = self.top(Side::Ask);
        let (bid_px, bid_sz) = self.top(Side::Bid);

        if ask_px.is_zero() | bid_px.is_zero() {
            return Ok(());
        }

        let row = Row {
            timestamp,
            market: market.clone(),
            price: ask_px,
            size: ask_sz,
            side: Side::Ask,
            kind: "BBO",
        };
        w.write_tick(row)?;

        let row = Row {
            timestamp,
            market: market.clone(),
            price: bid_px,
            size: bid_sz,
            side: Side::Bid,
            kind: "BBO",
        };
        w.write_tick(row)?;

        Ok(())
    }
}

impl MarketState {
    /// Update the market state, write zero or more tick data rows with the writer
    fn update(&mut self, m: FeedMessage, w: &mut ParquetTickWriter, timestamp: i64) -> Result<()> {
        match m {
            FeedMessage::Trade { msg, .. } => {
                if let Some(market_tickers) = &self.market_tickers {
                    if !market_tickers.contains(&msg.market_ticker) {
                        return Ok(());
                    }
                }
                w.write_tick(Row::from_trade(&msg))?;
            }
            FeedMessage::OrderbookSnapshot { msg, .. } => {
                if let Some(market_tickers) = &self.market_tickers {
                    if !market_tickers.contains(&msg.market_ticker) {
                        return Ok(());
                    }
                }

                let book = self.books.entry(msg.market_ticker.clone()).or_default();
                book.reset_from_snapshot(&msg);
                book.is_initialized = true;

                book.write_bbo(w, timestamp, msg.market_ticker)?;
            }
            FeedMessage::OrderbookDelta { msg, .. } => {
                if let Some(market_tickers) = &self.market_tickers {
                    if !market_tickers.contains(&msg.market_ticker) {
                        return Ok(());
                    }
                }

                self.books
                    .entry(msg.market_ticker.clone())
                    .or_default()
                    .update_from_delta(&msg)
                    .write_bbo(w, timestamp, msg.market_ticker)?;
            }
            FeedMessage::Ticker { .. } | FeedMessage::Other => {}
        }
        Ok(())
    }

    pub fn with_market_filter(&mut self, markets: Vec<String>) {
        self.market_tickers = Some(markets.into_iter().collect());
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
#[serde(tag = "type")]
enum FeedMessage {
    #[serde(rename = "orderbook_snapshot")]
    OrderbookSnapshot { msg: KalshiOrderbookSnapshot },

    #[serde(rename = "orderbook_delta")]
    OrderbookDelta { msg: KalshiOrderbookDelta },

    #[serde(rename = "trade")]
    Trade { msg: KalshiPublicTrade },

    #[serde(rename = "ticker")]
    #[allow(dead_code)]
    Ticker { sid: u64, msg: KalshiTicker },

    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum Side {
    Bid,
    Ask,
}

// Structs for serde

#[derive(Debug, Serialize, Deserialize)]
struct KalshiOrderbookSnapshot {
    market_ticker: String,
    market_id: String,
    #[serde(default)]
    yes: Vec<[Decimal; 2]>,
    #[serde(default)]
    no: Vec<[Decimal; 2]>,
}

#[derive(Debug, Serialize, Deserialize)]
struct KalshiOrderbookDelta {
    market_ticker: String,
    market_id: String,
    price: Decimal,
    delta: Decimal,
    side: KalshiSide,
}

#[derive(Debug, Serialize, Deserialize)]
struct KalshiPublicTrade {
    trade_id: String,
    market_ticker: String,
    yes_price: Decimal,
    no_price: Decimal,
    count: Decimal,
    taker_side: KalshiSide,
    #[serde(with = "chrono::serde::ts_seconds")]
    ts: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
struct KalshiTicker {
    market_ticker: String,
    price: Decimal,
    yes_bid: Decimal,
    yes_ask: Decimal,
    volume: Decimal,
    open_interest: Decimal,
    dollar_volume: Decimal,
    dollar_open_interest: Decimal,
    ts: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum KalshiSide {
    Yes,
    No,
}
