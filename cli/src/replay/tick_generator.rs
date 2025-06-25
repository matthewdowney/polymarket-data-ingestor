use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    fs::File,
    io::{BufRead, BufReader, Write},
};

use rust_decimal::{Decimal};
use serde::{Deserialize, Serialize};

pub fn main() -> Result<(), Box<dyn Error>> {
    let mut path = std::env::args().nth(1).ok_or("need file as arg")?;

    // Decompress and read the file, keeping track of market state, and write data points as CSV
    let mut reader = BufReader::new(zstd::Decoder::new(File::open(path.clone())?)?);
    let mut state = MarketState::default();

    path.push_str(".csv");
    let mut writer = csv::Writer::from_writer(File::create(path)?);

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
            state.update(msg, &mut writer)?;
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
struct MarketState {
    /// Asset id to order book
    books: HashMap<String, Book>,
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

    fn write_bbo<W: Write>(
        &self,
        w: &mut csv::Writer<W>,
        timestamp: String,
        market: String,
        asset: String,
    ) -> Result<(), Box<dyn Error>> {
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
        w.serialize(row)?;

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
        w.serialize(row)?;

        Ok(())
    }
}

impl MarketState {
    /// Update the market state, write zero or more tick data rows with the writer
    fn update<W: Write>(
        &mut self,
        m: FeedMessage,
        w: &mut csv::Writer<W>,
    ) -> Result<(), Box<dyn Error>> {
        match m {
            FeedMessage::LastTradePrice(x) => {
                w.serialize(Row::from_trade(x))?;
            }
            FeedMessage::BookSnapshot(x) => {
                self.books
                    .entry(x.asset_id.clone())
                    .or_default()
                    .reset_from_snapshot(&x)
                    .write_bbo(w, x.timestamp, x.market, x.asset_id)?;
            }
            FeedMessage::BookDiff(x) => {
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
