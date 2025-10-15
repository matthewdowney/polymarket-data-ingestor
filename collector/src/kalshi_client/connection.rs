use anyhow::{Context, Result};
use futures_util::{SinkExt};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::kalshi_client::{CONNECTION_ID, INITIAL_READ_TIMEOUT, WS_URL};
use crate::{await_first_msg, spawn_msg_handler, ConnectionEvent, KalshiCredentials, KalshiMarket};
use tokio_util::sync::CancellationToken;

/// Subscription ID for the next subscription message.
pub static LAST_SUB_ID: AtomicU64 = AtomicU64::new(0);

/// Represents a single WebSocket shard covering a subset of all markets
#[derive(Debug)]
pub struct Connection {
    /// Credentials for the connection.
    pub credentials: KalshiCredentials,
    /// Markets covered by this connection.
    pub markets: Vec<KalshiMarket>,
    /// Used to send events to the main thread.
    pub tx: mpsc::Sender<ConnectionEvent>,
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
        credentials: KalshiCredentials,
        markets: Vec<KalshiMarket>,
        tx: mpsc::Sender<ConnectionEvent>,
    ) -> Self {
        Self {
            credentials,
            markets,
            tx,
            shutdown: CancellationToken::new(),
            handle: None,
        }
    }

    /// Attempt to open or re-open the WebSocket.
    pub async fn connect(&mut self) -> Result<()> {
        tracing::info!(markets = %self.markets.len(), "attempting connection");

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
            await_first_msg(
                &mut ws,
                INITIAL_READ_TIMEOUT,
                CONNECTION_ID,
                self.tx.clone(),
            )
            .await?;
            let handle = spawn_msg_handler(
                None,
                ws,
                self.tx.clone(),
                self.shutdown.clone(),
                CONNECTION_ID,
                None,
            )
            .await;
            self.handle = Some(handle);
            Ok(())
        }
        .await;

        match connection_result {
            Ok(()) => {
                tracing::info!("connection established successfully");
                Ok(())
            }
            Err(e) => {
                tracing::warn!(error = %e, "connection failed");
                Err(e)
            }
        }
    }

    pub async fn run_until_closed(&mut self) -> Result<()> {
        if let Some(handle) = self.handle.as_mut() {
            handle.await.context("message handler task failed")?;
            self.handle = None;
            Ok(())
        } else {
            Err(anyhow::anyhow!("connection not open"))
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

    /// Attempt to open a WebSocket connection, and return it immediately when
    /// the handshake completes.
    async fn open_socket(&self) -> Result<Socket> {
        let mut request = WS_URL.into_client_request()?;

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
            .to_string();

        let url = url::Url::parse(WS_URL)?;
        let path = url.path();

        let signature = self.credentials.sign("GET", path, &timestamp)?;

        request
            .headers_mut()
            .insert("KALSHI-ACCESS-KEY", self.credentials.api_key.parse()?);
        request
            .headers_mut()
            .insert("KALSHI-ACCESS-SIGNATURE", signature.parse()?);
        request
            .headers_mut()
            .insert("KALSHI-ACCESS-TIMESTAMP", timestamp.parse()?);
        request
            .headers_mut()
            .insert("User-Agent", "data-ingestor/0.1.0".parse()?);

        // Connect to the Websocket server with TLS
        let (ws_stream, _) = connect_async(request)
            .await
            .context("failed to connect to WebSocket server")?;

        Ok(ws_stream)
    }

    /// Subscribe to the book for each individual token in the set of markets
    async fn subscribe(&self, ws: &mut Socket) -> Result<()> {
        let tickers = self
            .markets
            .iter()
            .map(|m| m.ticker.clone())
            .collect::<Vec<_>>();

        let sub_msg = serde_json::json!({
            "id": LAST_SUB_ID.fetch_add(1, Ordering::Relaxed) + 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["ticker", "trade", "orderbook_delta"],
                "market_tickers": tickers,
            }
        });

        ws.send(Message::text(sub_msg.to_string()))
            .await
            .context("sending sub msg")?;
        Ok(())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if self.handle.is_some() {
            self.shutdown.cancel();
            tracing::info!("connection dropped without being closed");
        }
    }
}
