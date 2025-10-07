//! A library for interacting with Polymarket's book feeds and API.
//!
//! Fetch market data and subscribe to real-time order book feeds with automatic
//! connection management and reconnection.
//!
//! # Example
//!
//! ```rust,no_run
//! use data_collector::client::PolymarketClient;
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

use anyhow::Result;
use base64::{engine::general_purpose, Engine as _};
use chrono::{DateTime, Utc};
use futures::Stream;
use rand::rngs::OsRng;
use rsa::pss::SigningKey;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use rsa::{pkcs1::DecodeRsaPrivateKey, pkcs8::DecodePrivateKey, RsaPrivateKey};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

pub mod client;
pub mod kalshi_client;

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

#[derive(Clone, Debug)]
pub struct KalshiCredentials {
    pub(crate) api_key: String,
    pub(crate) private_key: RsaPrivateKey,
}

impl KalshiCredentials {
    pub fn from_file(api_key: &str, path: &str) -> Result<Self> {
        let pem = std::fs::read_to_string(path)?;

        let private_key =
            RsaPrivateKey::from_pkcs8_pem(&pem).or_else(|_| RsaPrivateKey::from_pkcs1_pem(&pem))?;

        Ok(Self {
            api_key: api_key.to_string(),
            private_key,
        })
    }

    /// Sign a message for Kalshi API authentication
    pub fn sign(&self, method: &str, path: &str, timestamp: &str) -> Result<String> {
        let message = format!("{timestamp}{method}{path}");
        let signing_key = SigningKey::<Sha256>::new(self.private_key.clone());
        let mut rng = OsRng;
        let signature = signing_key.sign_with_rng(&mut rng, message.as_bytes());
        let encoded = general_purpose::STANDARD.encode(signature.to_bytes());
        Ok(encoded)
    }
}

/// A market from the Kalshi API, which may be active or inactived,
/// past present or future.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KalshiMarket {
    // State
    pub status: String,
    pub result: String,

    // Specs
    pub response_price_units: String,
    pub tick_size: Decimal,

    /// This is what comes over the feed for "market"
    pub ticker: String,
    pub event_ticker: String,
    pub market_type: String,
    pub title: String,
    pub subtitle: String,
    pub yes_sub_title: String,
    pub no_sub_title: String,

    // Times
    #[serde(deserialize_with = "crate::deserialize_optional_iso_datetime")]
    pub open_time: Option<DateTime<Utc>>,
    #[serde(deserialize_with = "crate::deserialize_optional_iso_datetime")]
    pub close_time: Option<DateTime<Utc>>,
    #[serde(deserialize_with = "crate::deserialize_optional_iso_datetime")]
    pub expiration_time: Option<DateTime<Utc>>,
    pub settlement_timer_seconds: u64,

    // Misc and meaning unclear
    pub notional_value: Decimal,
    pub notional_value_dollars: Decimal,
    pub yes_bid: Decimal,
    pub yes_bid_dollars: Decimal,
    pub yes_ask: Decimal,
    pub yes_ask_dollars: Decimal,
    pub no_bid: Decimal,
    pub no_bid_dollars: Decimal,
    pub no_ask: Decimal,
    pub no_ask_dollars: Decimal,
    pub last_price: Decimal,
    pub last_price_dollars: Decimal,
    pub previous_yes_bid: Decimal,
    pub previous_yes_bid_dollars: Decimal,
    pub previous_yes_ask: Decimal,
    pub previous_yes_ask_dollars: Decimal,
    pub previous_price: Decimal,
    pub previous_price_dollars: Decimal,
    pub volume: Decimal,
    pub volume_24h: Decimal,
    pub liquidity: Decimal,
    pub liquidity_dollars: Decimal,
    pub open_interest: Decimal,
    pub risk_limit_cents: Decimal,
    pub rules_primary: String,
    pub rules_secondary: String,
    pub settlement_value: Option<Decimal>,
    pub settlement_value_dollars: Option<Decimal>,
}

#[derive(Debug, Deserialize)]
pub struct KalshiMarketsApiResponse {
    pub cursor: Option<String>,
    pub markets: Vec<KalshiMarket>,
}

/// Deserialize an optional timestamp from ISO 8601 format.
pub fn deserialize_optional_iso_datetime<'de, D>(
    deserializer: D,
) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        Some(s) => DateTime::parse_from_rfc3339(&s)
            .map(|dt| dt.with_timezone(&Utc))
            .map(Some)
            .map_err(D::Error::custom),
        None => Ok(None),
    }
}

#[derive(Debug)]
pub enum ConnectionEvent {
    FeedMessage(String),
    ConnectionClosed(ConnectionId),
    ConnectionOpened(ConnectionId),
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct ConnectionId(pub u64);

/// Events emitted by the client during operation.
#[derive(Debug)]
pub enum FeedEvent {
    /// A raw JSON message from the WebSocket feed.
    FeedMessage(String),
    ConnectionOpened(ConnectionId, usize, usize),
    ConnectionClosed(ConnectionId, usize, usize),
}

pub struct FeedEventStream {
    rx: mpsc::Receiver<FeedEvent>,
}

impl Stream for FeedEventStream {
    type Item = FeedEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl FeedEventStream {
    fn new(rx: mpsc::Receiver<FeedEvent>) -> Self {
        Self { rx }
    }
}
