use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use serde_json::Value;

/// Represents the structured log format used by the feed system
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LogMessage {
    pub timestamp: DateTime<Utc>,
    pub message_type: String,
    pub content: Value, // Can be string or object depending on message type
}

/// Parsed content of different message types
#[derive(Debug, Clone)]
pub enum MessageContent {
    FeedMessage(Vec<OrderBookEvent>),
    ActiveMarkets(Vec<super::super::PolymarketMarket>),
    ShutdownInitiated,
    ConnectionsReady,
    Unknown(String),
}

/// Individual order book event from WebSocket feed
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OrderBookEvent {
    pub market: String,
    pub asset_id: String,
    pub timestamp: String,
    pub hash: String,
    pub bids: Vec<OrderEntry>,
    pub asks: Vec<OrderEntry>,
    pub event_type: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OrderEntry {
    pub price: String,
    pub size: String,
}

impl LogMessage {
    /// Parse a JSON line from the log file
    pub fn from_json_line(line: &str) -> Result<Self> {
        serde_json::from_str(line)
            .map_err(|e| anyhow!("Failed to parse log message: {}", e))
    }

    /// Parse the content field based on message type
    pub fn parse_content(&self) -> Result<MessageContent> {
        match self.message_type.as_str() {
            "feed_message" => {
                // Content should be a string containing JSON array
                let content_str = match &self.content {
                    Value::String(s) => s,
                    _ => return Err(anyhow!("Feed message content should be a string")),
                };
                let events: Vec<OrderBookEvent> = serde_json::from_str(content_str)
                    .map_err(|e| anyhow!("Failed to parse feed message content: {}", e))?;
                Ok(MessageContent::FeedMessage(events))
            }
            "active_markets" => {
                // Content can be either string or object
                let markets = match &self.content {
                    Value::String(s) => {
                        // Legacy format: content is stringified JSON
                        serde_json::from_str::<Vec<super::super::PolymarketMarket>>(s)
                            .map_err(|e| anyhow!("Failed to parse active markets content from string: {}", e))?
                    }
                    Value::Object(_) => {
                        // New format: content is object with markets array
                        #[derive(Deserialize)]
                        struct ActiveMarketsContent {
                            markets: Vec<super::super::PolymarketMarket>,
                        }
                        let content: ActiveMarketsContent = serde_json::from_value(self.content.clone())
                            .map_err(|e| anyhow!("Failed to parse active markets content from object: {}", e))?;
                        content.markets
                    }
                    _ => return Err(anyhow!("Active markets content should be string or object")),
                };
                Ok(MessageContent::ActiveMarkets(markets))
            }
            "shutdown_initiated" => Ok(MessageContent::ShutdownInitiated),
            "all_connections_ready" => Ok(MessageContent::ConnectionsReady),
            _ => {
                let content_str = match &self.content {
                    Value::String(s) => s.clone(),
                    other => serde_json::to_string(other)?,
                };
                Ok(MessageContent::Unknown(content_str))
            }
        }
    }

    /// Get the timestamp of this message
    pub fn get_timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    /// Extract market IDs from feed messages for filtering
    pub fn get_market_ids(&self) -> Result<Vec<String>> {
        match self.parse_content()? {
            MessageContent::FeedMessage(events) => {
                Ok(events.into_iter().map(|e| e.market).collect())
            }
            MessageContent::ActiveMarkets(markets) => {
                Ok(markets.into_iter().map(|m| m.id).collect())
            }
            _ => Ok(Vec::new()),
        }
    }
}