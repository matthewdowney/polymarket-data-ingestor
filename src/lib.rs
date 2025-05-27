use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub mod client;

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

    pub tokens: Vec<serde_json::Value>,

    // there are inconsistencies in the other fields, so treat them dynamically
    #[serde(flatten)]
    pub other: HashMap<String, serde_json::Value>,
}

impl PolymarketMarket {
    pub fn is_active(&self) -> bool {
        self.enable_order_book && self.accepting_orders && !self.archived && !self.closed
    }
}

#[derive(Debug, Deserialize)]
pub struct ApiResponse {
    pub data: Vec<PolymarketMarket>,
    pub next_cursor: Option<String>,
    pub limit: u32,
    pub count: u32,
}
