use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use native_tls::TlsConnector;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};

use crate::client::{INITIAL_READ_TIMEOUT, PING_INTERVAL, WS_URL};
use crate::PolymarketMarket;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub enum ConnectionEvent {
    FeedMessage(String),
    ConnectionClosed(ConnectionId),
    ConnectionOpened(ConnectionId),
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct ConnectionId(pub u64);

/// Represents a single WebSocket shard covering a subset of all markets.
#[derive(Debug)]
pub struct Connection {
    /// Used to identify the connection for restarts etc.
    pub id: ConnectionId,
    /// Markets covered by this connection.
    pub markets: Vec<PolymarketMarket>,
    /// Used to send events to the main thread.
    pub tx: mpsc::Sender<ConnectionEvent>,
    /// False before first successful connection.
    pub has_ever_opened: bool,

    /// Timestamp when connection was last successfully opened.
    opened_at: Option<Instant>,

    /// Number of consecutive connection failures.
    consecutive_failures: u32,

    /// Signals an existing connection to close.
    shutdown: CancellationToken,

    /// Handle for the message handler task.
    handle: Option<JoinHandle<()>>,
}

/// Underlying WebSocket stream.
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

impl Connection {
    /// Create a new connection.
    pub fn new(
        id: ConnectionId,
        markets: Vec<PolymarketMarket>,
        tx: mpsc::Sender<ConnectionEvent>,
    ) -> Self {
        Self {
            id,
            markets,
            tx,
            shutdown: CancellationToken::new(),
            handle: None,
            has_ever_opened: false,
            opened_at: None,
            consecutive_failures: 0,
        }
    }

    /// Attempt to open or re-open the WebSocket. Returns an error if the
    /// connection is not fully open within [`INITIAL_READ_TIMEOUT`] and does
    /// **not** send a connection closed event.
    ///
    /// Otherwise spawns a message handler, returns Ok(()), and later broadcasts
    /// a connection closed event when the connection is closed.
    pub async fn connect(&mut self) -> Result<()> {
        // Log the connection attempt with current failure count
        tracing::info!(
            connection_id = ?self.id,
            consecutive_failures = self.consecutive_failures,
            "attempting connection"
        );

        // If a connection is already open, close it and reset the shutdown signal
        if self.handle.is_some() {
            self.close().await?;
            self.shutdown = CancellationToken::new();
        }

        // Attempt to establish connection - track failures
        let connection_result = async {
            // Open the ws and subscribe to books
            let mut ws = timeout(INITIAL_READ_TIMEOUT, self.open_socket())
                .await
                .context("timeout opening WebSocket")?
                .context("failed to open WebSocket")?;
            timeout(INITIAL_READ_TIMEOUT, self.subscribe(&mut ws))
                .await
                .context("timeout subscribing to markets")?
                .context("failed to subscribe to markets")?;

            // Only consider the connection fully open once we see a message,
            // then spawn a task to handle the rest of the messages
            self.await_first_msg(&mut ws).await?;
            let handle = self.spawn_msg_handler(ws).await;
            self.handle = Some(handle);
            self.has_ever_opened = true;
            Ok(())
        }
        .await;

        match connection_result {
            Ok(()) => {
                tracing::info!(
                    connection_id = ?self.id,
                    "connection established successfully"
                );
                Ok(())
            }
            Err(e) => {
                self.consecutive_failures += 1;
                tracing::warn!(
                    connection_id = ?self.id,
                    consecutive_failures = self.consecutive_failures,
                    error = %e,
                    "connection failed"
                );
                Err(e)
            }
        }
    }

    /// Close the connection if open and wait for the message handler to finish.
    pub async fn close(&mut self) -> Result<()> {
        if let Some(handle) = self.handle.take() {
            self.shutdown.cancel();

            handle
                .await
                .context("waiting for message handler to finish")?;
        }
        Ok(())
    }

    /// Reset the consecutive failures counter. Called when a connection
    /// has been successful and long-lived (>30 seconds).
    pub fn reset_failure_count(&mut self) {
        tracing::info!(
            connection_id = ?self.id,
            previous_failures = self.consecutive_failures,
            "resetting consecutive failure count due to successful long-lived connection"
        );
        self.consecutive_failures = 0;
    }

    /// Process connection closure - increment failures for short connections, reset for long ones.
    pub fn process_connection_closed(&mut self) {
        if let Some(opened_time) = self.opened_at {
            let connection_duration = opened_time.elapsed();

            if connection_duration > Duration::from_secs(30) {
                // Long-lived connection - reset failure count
                self.reset_failure_count();
            } else {
                // Short-lived connection - treat as failure
                self.consecutive_failures += 1;
                tracing::warn!(
                    connection_id = ?self.id,
                    duration_ms = connection_duration.as_millis(),
                    consecutive_failures = self.consecutive_failures,
                    "short-lived connection treated as failure"
                );
            }
        }
    }

    /// Get the current number of consecutive failures for this connection.
    #[allow(dead_code)]
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    /// Evaluate whether this connection should be split based on failure patterns.
    ///
    /// A connection should be split if ALL of the following conditions are met:
    /// 1. consecutive_failures >= 20
    /// 2. Market count > 1 (cannot split single-market connections)
    /// 3. has_ever_opened == true (only split connections that have worked before)
    pub fn should_split(&self) -> bool {
        // Check failure count threshold
        if self.consecutive_failures < 20 {
            return false;
        }

        // Only split connections that have successfully opened at least once
        // This prevents splitting during initial connection phase when rate limiting
        // might cause legitimate delays
        if !self.has_ever_opened {
            return false;
        }

        // Check market count - cannot split single-market connections
        self.markets.len() > 1
    }
    /// Attempt to open a WebSocket connection, and return it immediately when
    /// the handshake completes.
    async fn open_socket(&self) -> Result<Socket> {
        // Configure TLS
        let tls = TlsConnector::builder()
            .danger_accept_invalid_certs(false)
            .build()
            .context("failed to build TLS connector")?;

        let connector = Connector::NativeTls(tls);
        let config = WebSocketConfig::default();

        // Connect to the WebSocket server with TLS
        let (ws_stream, _) =
            connect_async_tls_with_config(WS_URL, Some(config), false, Some(connector))
                .await
                .context("failed to connect to WebSocket server")?;

        Ok(ws_stream)
    }

    /// Subscribe to the book for each individual token in the set of markets
    async fn subscribe(&self, ws: &mut Socket) -> Result<()> {
        let assets_ids = self
            .markets
            .iter()
            .flat_map(|m| m.tokens.iter())
            .map(|t| t.token_id.as_str())
            .filter(|id| !id.is_empty())
            .collect::<Vec<_>>();
        let sub_msg = serde_json::json!({
            "type": "MARKET",
            "assets_ids": assets_ids,
        });

        ws.send(Message::text(sub_msg.to_string()))
            .await
            .context("sending sub msg")?;
        Ok(())
    }

    /// Await the first message from the WebSocket, or timeout and close the connection.
    async fn await_first_msg(&mut self, ws: &mut Socket) -> Result<()> {
        let msg = timeout(INITIAL_READ_TIMEOUT, ws.next()).await?;
        if let Some(Ok(Message::Text(text))) = msg {
            // Record the time when connection was successfully opened
            self.opened_at = Some(Instant::now());

            self.tx
                .send(ConnectionEvent::ConnectionOpened(self.id.clone()))
                .await
                .context("sending connection opened event")?;

            self.tx
                .send(ConnectionEvent::FeedMessage(text.to_string()))
                .await
                .context("sending first feed message")?;
            Ok(())
        } else {
            let _ = ws.close(None).await;

            // For failed initial connections, log that connection failed to establish
            tracing::warn!(
                connection_id = ?self.id,
                "connection failed to establish within timeout"
            );

            self.tx
                .send(ConnectionEvent::ConnectionClosed(self.id.clone()))
                .await
                .context("sending connection closed event")?;
            Err(anyhow::anyhow!(
                "no message received within {} seconds",
                INITIAL_READ_TIMEOUT.as_secs()
            ))
        }
    }

    /// Take ownership of the WebSocket and handle incoming messages until the connection closes.
    async fn spawn_msg_handler(&mut self, mut ws: Socket) -> JoinHandle<()> {
        let id = self.id.clone();
        let tx = self.tx.clone();
        let shutdown = self.shutdown.clone();
        let opened_at = self.opened_at;

        tokio::spawn(async move {
            let mut ping_interval = tokio::time::interval(PING_INTERVAL);
            // Skip the initial long delay so the first tick fires after PING_INTERVAL
            ping_interval.tick().await;

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

                    _ = ping_interval.tick() => {
                        if let Err(e) = ws.send(Message::Text(r#"{"type":"ping"}"#.into())).await {
                            tracing::error!(connection_id = ?id, error = %e, "failed to send ping");
                            break;
                        }
                    }
                }
            }

            // Close the WebSocket
            let _ = ws.close(None).await;

            // Log connection duration - duration will be processed when connection is closed
            if let Some(opened_time) = opened_at {
                let connection_duration = opened_time.elapsed();
                tracing::info!(
                    connection_id = ?id,
                    duration_secs = connection_duration.as_secs(),
                    duration_ms = connection_duration.as_millis(),
                    "connection closed after duration"
                );
            }

            // Notify that the connection is closed
            let _ = tx.send(ConnectionEvent::ConnectionClosed(id.clone())).await;
        })
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if self.handle.is_some() {
            self.shutdown.cancel();
            tracing::info!(connection_id = ?self.id, "connection dropped without being closed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn create_test_connection() -> (Connection, mpsc::Receiver<ConnectionEvent>) {
        let (tx, rx) = mpsc::channel(10);
        let connection = Connection::new(
            ConnectionId(123),
            vec![], // empty markets for testing
            tx,
        );
        (connection, rx)
    }

    #[test]
    fn test_new_connection_has_no_opened_at() {
        let (connection, _rx) = create_test_connection();
        assert_eq!(connection.opened_at, None);
        assert_eq!(connection.has_ever_opened, false);
    }

    #[test]
    fn test_connection_tracks_opened_at_time() {
        let (mut connection, _rx) = create_test_connection();

        // Simulate setting opened_at (this would normally happen in await_first_msg)
        let before = Instant::now();
        connection.opened_at = Some(Instant::now());
        let after = Instant::now();

        let opened_at = connection.opened_at.unwrap();
        assert!(opened_at >= before);
        assert!(opened_at <= after);
    }

    #[test]
    fn test_connection_duration_calculation() {
        let (mut connection, _rx) = create_test_connection();

        // Set opened_at to a specific time in the past
        let opened_time = Instant::now() - std::time::Duration::from_millis(500);
        connection.opened_at = Some(opened_time);

        // Calculate duration
        let duration = opened_time.elapsed();

        // Duration should be approximately 500ms (allowing for some variance)
        assert!(duration.as_millis() >= 500);
        assert!(duration.as_millis() < 600); // Should be well under 600ms
    }

    #[test]
    fn test_new_connection_has_zero_consecutive_failures() {
        let (connection, _rx) = create_test_connection();
        assert_eq!(connection.consecutive_failures, 0);
    }

    #[test]
    fn test_reset_failure_count() {
        let (mut connection, _rx) = create_test_connection();

        // Simulate some failures
        connection.consecutive_failures = 5;

        // Reset the count
        connection.reset_failure_count();

        assert_eq!(connection.consecutive_failures, 0);
    }

    #[test]
    fn test_should_split_insufficient_failures() {
        let (mut connection, _rx) = create_test_connection();

        // Add some markets to make splitting possible
        connection.markets = vec![create_test_market(1), create_test_market(2)];
        connection.consecutive_failures = 2; // Less than threshold

        assert!(!connection.should_split());
    }

    #[test]
    fn test_should_split_single_market() {
        let (mut connection, _rx) = create_test_connection();

        // Single market - cannot split
        connection.markets = vec![create_test_market(1)];
        connection.consecutive_failures = 5; // More than threshold

        assert!(!connection.should_split());
    }

    #[test]
    fn test_should_split_multiple_markets_sufficient_failures() {
        let (mut connection, _rx) = create_test_connection();

        // Setup for split
        connection.markets = vec![create_test_market(1), create_test_market(2)];
        connection.consecutive_failures = 3; // At threshold
        connection.has_ever_opened = true; // Required for splitting

        assert!(connection.should_split()); // Should split - all conditions met
    }

    #[test]
    fn test_should_split_never_opened() {
        let (mut connection, _rx) = create_test_connection();

        // Setup conditions that would normally trigger split
        connection.markets = vec![create_test_market(1), create_test_market(2)];
        connection.consecutive_failures = 5; // Well above threshold
        connection.has_ever_opened = false; // But never opened

        assert!(!connection.should_split()); // Should NOT split - never opened
    }

    fn create_test_market(id: u64) -> crate::PolymarketMarket {
        use crate::{MarketToken, PolymarketMarket};
        use std::collections::HashMap;

        PolymarketMarket {
            closed: false,
            accepting_orders: true,
            active: true,
            archived: false,
            enable_order_book: true,
            id: Some(id.to_string()),
            condition_id: format!("condition_{}", id),
            question_id: format!("question_{}", id),
            question: format!("Test question {}", id),
            description: format!("Test description {}", id),
            tokens: vec![
                MarketToken {
                    outcome: "Yes".to_string(),
                    price: 0.5,
                    token_id: format!("token_yes_{}", id),
                    winner: false,
                    other: HashMap::new(),
                },
                MarketToken {
                    outcome: "No".to_string(),
                    price: 0.5,
                    token_id: format!("token_no_{}", id),
                    winner: false,
                    other: HashMap::new(),
                },
            ],
            other: HashMap::new(),
        }
    }
}
