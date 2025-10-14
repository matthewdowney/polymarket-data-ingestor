use std::collections::{BTreeMap, HashMap, HashSet};

use crate::{HasMarketFilter, Row, Side, TickWriter};
use anyhow::Result;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

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
                PolymarketSide::Ask => &mut self.asks,
                PolymarketSide::Bid => &mut self.bids,
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
        w: &mut TickWriter,
        timestamp: String,
        market: String,
        asset: String,
    ) -> Result<()> {
        let (px, sz) = self.top(Side::Ask);
        let row = Row {
            timestamp: timestamp.clone(),
            market: market.clone(),
            asset: Some(asset.clone()),
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
            asset: Some(asset.clone()),
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
    pub fn update(&mut self, m: FeedMessage, w: &mut TickWriter) -> Result<()> {
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

impl HasMarketFilter for MarketState {
    fn with_market_filter(&mut self, markets: Vec<String>) {
        self.with_market_filter(markets);
    }
}

#[derive(Deserialize)]
#[serde(tag = "event_type")]
pub enum FeedMessage {
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
pub struct LastTradePriceMessage {
    market: String,
    asset_id: String,
    side: PolymarketSide,
    price: Decimal,
    size: Decimal,
    timestamp: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
enum PolymarketSide {
    #[serde(rename = "BUY")]
    Bid,
    #[serde(rename = "SELL")]
    Ask,
}

impl From<PolymarketSide> for Side {
    fn from(side: PolymarketSide) -> Self {
        match side {
            PolymarketSide::Bid => Side::Bid,
            PolymarketSide::Ask => Side::Ask,
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct BookSnapshotMessage {
    asks: Vec<Level>,
    bids: Vec<Level>,
    timestamp: String,
    market: String,
    asset_id: String,
}

#[derive(Deserialize, Debug)]
pub struct Level {
    price: Decimal,
    size: Decimal,
}

#[derive(Deserialize, Debug)]
pub struct DiffLevel {
    price: Decimal,
    size: Decimal,
    side: PolymarketSide,
}

#[derive(Deserialize, Debug)]
pub struct BookDiffMessage {
    changes: Vec<DiffLevel>,
    timestamp: String,
    market: String,
    asset_id: String,
}

impl Row {
    fn from_trade(t: LastTradePriceMessage) -> Self {
        Self {
            timestamp: t.timestamp,
            kind: "TRADE",
            market: t.market,
            asset: Some(t.asset_id),
            side: t.side.into(),
            price: t.price,
            size: t.size,
        }
    }
}
