use std::collections::{BTreeMap, HashMap, HashSet};

use crate::{HasMarketFilter, Row, Side, TickWriter};
use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::{dec, Decimal};
use serde::{Deserialize, Serialize};

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
                .insert(penny_price(Decimal::from(100) - lvl[0]), lvl[1]);
        }

        self.bids.clear();
        for lvl in m.yes.iter() {
            self.bids.insert(penny_price(lvl[0]), lvl[1]);
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
                let neg_price = penny_price(Decimal::from(100) - m.price);
                (&mut self.asks, neg_price)
            }
            KalshiSide::Yes => (&mut self.bids, penny_price(m.price)),
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

    fn top(&self, side: Side) -> Option<(Decimal, Decimal)> {
        match side {
            Side::Ask => self.asks.iter().next(),
            Side::Bid => self.bids.iter().next_back(),
        }
        .map(|(&px, &sz)| (px, sz))
    }

    fn write_bbo(&self, w: &mut TickWriter, timestamp: String, market: String) -> Result<()> {
        if let Some((ask_px, ask_sz)) = self.top(Side::Ask) {
            let row = Row {
                timestamp: timestamp.clone(),
                market: market.clone(),
                asset: None,
                price: ask_px,
                size: ask_sz,
                side: Side::Ask,
                kind: "BBO",
            };
            w.write_tick(row)?;
        }

        if let Some((bid_px, bid_sz)) = self.top(Side::Bid) {
            let row = Row {
                timestamp,
                market: market.clone(),
                asset: None,
                price: bid_px,
                size: bid_sz,
                side: Side::Bid,
                kind: "BBO",
            };
            w.write_tick(row)?;
        }

        Ok(())
    }
}

impl MarketState {
    /// Update the market state, write zero or more tick data rows with the writer
    pub fn update(&mut self, m: FeedMessage, w: &mut TickWriter, timestamp: String) -> Result<()> {
        match m {
            FeedMessage::Trade { msg, .. } => {
                if !self.should_include(&msg.market_ticker) {
                    return Ok(());
                }
                w.write_tick(Row::from_kalshi_trade(&msg))?;
            }
            FeedMessage::OrderbookSnapshot { msg, .. } => {
                if !self.should_include(&msg.market_ticker) {
                    return Ok(());
                }

                let book = self.books.entry(msg.market_ticker.clone()).or_default();
                book.reset_from_snapshot(&msg);
                book.is_initialized = true;

                book.write_bbo(w, timestamp, msg.market_ticker)?;
            }
            FeedMessage::OrderbookDelta { msg, .. } => {
                if !self.should_include(&msg.market_ticker) {
                    return Ok(());
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

    fn should_include(&self, ticker: &str) -> bool {
        self.market_tickers
            .as_ref()
            .is_none_or(|m| m.contains(ticker))
    }
}

impl HasMarketFilter for MarketState {
    fn with_market_filter(&mut self, markets: Vec<String>) {
        self.with_market_filter(markets);
    }
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum FeedMessage {
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

// Structs for serde

#[derive(Debug, Serialize, Deserialize)]
pub struct KalshiOrderbookSnapshot {
    market_ticker: String,
    market_id: String,
    #[serde(default)]
    yes: Vec<[Decimal; 2]>,
    #[serde(default)]
    no: Vec<[Decimal; 2]>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KalshiOrderbookDelta {
    market_ticker: String,
    market_id: String,
    price: Decimal,
    delta: Decimal,
    side: KalshiSide,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KalshiPublicTrade {
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
pub struct KalshiTicker {
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

impl From<KalshiSide> for Side {
    fn from(side: KalshiSide) -> Self {
        match side {
            KalshiSide::Yes => Side::Bid,
            KalshiSide::No => Side::Ask,
        }
    }
}

fn penny_price(price: Decimal) -> Decimal {
    price / dec!(100)
}

impl Row {
    fn from_kalshi_trade(t: &KalshiPublicTrade) -> Self {
        let (side, price, size) = match t.taker_side {
            KalshiSide::Yes => (Side::Bid, penny_price(t.yes_price), t.count),
            KalshiSide::No => (Side::Ask, penny_price(t.yes_price), t.count),
        };
        Self {
            timestamp: t.ts.to_string(),
            kind: "TRADE",
            market: t.market_ticker.clone(),
            asset: None,
            side,
            price,
            size,
        }
    }
}
