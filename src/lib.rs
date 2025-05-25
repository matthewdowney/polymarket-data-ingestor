use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// A newline-delimited JSON file of all markets from the Polymarket API.
pub const MARKETS_FILE: &str = "markets.ndjson";

/// A market from the Polymarket API, which may be active or inactive, 
/// past present or future.
#[derive(Debug, Deserialize, Serialize)]
pub struct PolymarketMarket {
    // unclear what the differences between these fields are
    pub closed: bool,
    pub accepting_orders: bool,
    pub active: bool,
    pub archived: bool,
    pub enable_order_book: bool,

    pub condition_id: String,
    pub question_id: String,
    pub question: String,
    pub description: String,

    // there are inconsistencies in the other fields, so treat them dynamically
    #[serde(flatten)]
    pub other: HashMap<String, serde_json::Value>,
}