//! A library for interacting with Polymarket's book feeds and API.
//!
//! Fetch market data and subscribe to real-time order book feeds with automatic
//! connection management and reconnection.
//!
//! # Example
//!
//! ```rust,no_run
//! use polymarket_data_ingestor::client::PolymarketClient;
//! use tokio_util::sync::CancellationToken;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let cancel = CancellationToken::new();
//!     let client = PolymarketClient::new(cancel.clone());
//!     
//!     // Fetch active markets  
//!     let markets = client.fetch_active_markets().await?;
//!     println!("Found {} active markets", markets.len());
//!     
//!     Ok(())
//! }
//! ```

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub mod client;
pub mod replay;


/// A token represents one outcome in a prediction market.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MarketToken {
    pub outcome: String,
    pub price: f64,
    pub token_id: String,
    pub winner: bool,

    // Additional fields that may be present
    #[serde(flatten)]
    pub other: HashMap<String, serde_json::Value>,
}

/// A market from the Polymarket API, which may be active or inactive,
/// past present or future.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PolymarketMarket {
    // unclear what the differences between these fields are
    pub closed: bool,
    pub accepting_orders: bool,
    pub active: bool,
    pub archived: bool,
    pub enable_order_book: bool,

    pub id: Option<String>,
    pub condition_id: String,
    pub question_id: String,
    pub question: String,
    pub description: String,

    pub tokens: Vec<MarketToken>,

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
pub struct MarketsApiResponse {
    pub data: Vec<PolymarketMarket>,
    pub next_cursor: Option<String>,
    pub limit: u32,
    pub count: u32,
}
