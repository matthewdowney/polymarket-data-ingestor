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

use anyhow::{Context, Result};
use base64::{engine::general_purpose, Engine as _};
use chrono::{DateTime, Utc};
use futures::Stream;
use futures_util::{SinkExt, StreamExt};
use rand::rngs::OsRng;
use rsa::pss::SigningKey;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use rsa::{pkcs1::DecodeRsaPrivateKey, pkcs8::DecodePrivateKey, RsaPrivateKey};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use std::pin::Pin;
use std::task::Poll;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::{sync::mpsc, task::JoinHandle, time::timeout};
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;

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

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl FeedEventStream {
    fn new(rx: mpsc::Receiver<FeedEvent>) -> Self {
        Self { rx }
    }
}

pub type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Await the first message from the WebSocket, or timeout and close the connection.
pub async fn await_first_msg(
    ws: &mut Socket,
    timeout_duration: Duration,
    id: ConnectionId,
    tx: mpsc::Sender<ConnectionEvent>,
) -> Result<Option<Instant>> {
    let msg = timeout(timeout_duration, ws.next()).await?;
    if let Some(Ok(Message::Text(text))) = msg {
        tx.send(ConnectionEvent::ConnectionOpened(id.clone()))
            .await
            .context("sending connection opened event")?;

        tx.send(ConnectionEvent::FeedMessage(text.to_string()))
            .await
            .context("sending first feed message")?;

        Ok(Some(Instant::now()))
    } else {
        let _ = ws.close(None).await;

        // For failed initial connections, log that connection failed to establish
        tracing::warn!(connection_id = ?id, "connection failed to establish within timeout");

        tx.send(ConnectionEvent::ConnectionClosed(id.clone()))
            .await
            .context("sending connection closed event")?;
        Err(anyhow::anyhow!(
            "no message received within {} seconds",
            timeout_duration.as_secs()
        ))
    }
}

/// Take ownership of the WebSocket and handle incoming messages until the connection closes.
pub async fn spawn_msg_handler(
    ping: Option<Duration>,
    mut ws: Socket,
    tx: mpsc::Sender<ConnectionEvent>,
    shutdown: CancellationToken,
    id: ConnectionId,
    opened_at: Option<Instant>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ping_interval = ping.map(|p| tokio::time::interval(p));
        if let Some(ref mut interval) = ping_interval {
            interval.tick().await;
        }

        loop {
            tokio::select! {
                // Prioritize shutdown so the connection can be closed even if there are new ws messages
                biased;
                _ = shutdown.cancelled() => {
                    tracing::debug!(connection_id = ?id, "connection closed by client");
                    break;
                }

                msg = ws.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                                if let Some(msg_type) = json.get("type").and_then(|t| t.as_str()) {
                                    match msg_type {
                                        "error" => {
                                            tracing::error!(connection_id = ?id, message = %text, "received error, closing connection");
                                            break;
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            if let Err(e) = tx.send(ConnectionEvent::FeedMessage(text.to_string())).await {
                                tracing::error!(connection_id = ?id, error = %e, "failed to send message");
                                break;
                            }
                        }
                        Some(Ok(Message::Close(_))) => {
                            tracing::warn!(connection_id = ?id, "connection closed by server");
                            break;
                        }
                        Some(Err(e)) => {
                            tracing::warn!(connection_id = ?id, error = %e, "WebSocket error");
                            break;
                        }
                        Some(_) => {
                            // Ignore other message types
                        }
                        None => {
                            tracing::warn!(connection_id = ?id, "WebSocket stream ended");
                            break;
                        }
                    }
                }

                _ = async {
                    match ping_interval.as_mut() {
                        Some(interval) => interval.tick().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Err(e) = ws.send(Message::Text(r#"{"type":"ping"}"#.into())).await {
                        tracing::error!(connection_id = ?id, error = %e, "failed to send ping");
                        break;
                    }
                }
            }
        }

        let _ = ws.close(None).await;

        if let Some(opened_time) = opened_at {
            let connection_duration = opened_time.elapsed();
            tracing::info!(
                connection_id = ?id,
                duration_secs = connection_duration.as_secs(),
                duration_ms = connection_duration.as_millis(),
                "connection closed after duration"
            );
        }

        let _ = tx.send(ConnectionEvent::ConnectionClosed(id)).await;
    })
}

/// Close the connection if open and wait for the message handler to finish.
pub async fn close(mut handle: Option<JoinHandle<()>>, shutdown: CancellationToken) -> Result<()> {
    if let Some(handle) = handle.take() {
        shutdown.cancel();

        handle
            .await
            .context("waiting for message handler to finish")?;
    }
    Ok(())
}
