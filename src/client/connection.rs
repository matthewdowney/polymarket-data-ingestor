use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use native_tls::TlsConnector;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};

use crate::PolymarketMarket;
use crate::client::{INITIAL_READ_TIMEOUT, PING_INTERVAL, WS_URL};

#[derive(Debug)]
pub enum ConnectionEvent {
    FeedMessage(ConnectionId, String),
    ConnectionClosed(ConnectionId),
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
    pub tx: mpsc::UnboundedSender<ConnectionEvent>,
}

/// Underlying WebSocket stream.
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

impl Connection {
    /// Attempt to open or re-open the WebSocket. Returns an error if the
    /// connection is not fully open within [`INITIAL_READ_TIMEOUT`] and does
    /// **not** send a connection closed event.
    ///
    /// Otherwise returns Ok(()), and later broadcasts a connection closed event
    /// when the connection is closed.
    pub async fn connect(&self) -> Result<()> {
        // Open the ws and subscribe to books
        let mut ws = self.open_socket().await?;
        self.subscribe(&mut ws).await?;

        // Only consider the connection fully open once we see a message,
        // then spawn a task to handle the rest of the messages
        self.await_first_msg(&mut ws).await?;
        self.spawn_msg_handler(ws).await;
        Ok(())
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
            .map(|t| t.get("token_id").unwrap().as_str().unwrap())
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
    async fn await_first_msg(&self, ws: &mut Socket) -> Result<()> {
        let msg = timeout(INITIAL_READ_TIMEOUT, ws.next()).await?;
        if let Some(Ok(Message::Text(text))) = msg {
            self.tx
                .send(ConnectionEvent::FeedMessage(
                    self.id.clone(),
                    text.to_string(),
                ))
                .context("sending first feed message")?;
            Ok(())
        } else {
            let _ = ws.close(None).await;
            self.tx
                .send(ConnectionEvent::ConnectionClosed(self.id.clone()))
                .context("sending connection closed event")?;
            Err(anyhow::anyhow!(
                "no message received within {} seconds",
                INITIAL_READ_TIMEOUT.as_secs()
            ))
        }
    }

    /// Take ownership of the WebSocket and handle incoming messages until the connection closes.
    async fn spawn_msg_handler(&self, mut ws: Socket) {
        let id = self.id.clone();
        let tx = self.tx.clone();

        let _handle = tokio::spawn(async move {
            let mut ping_interval = tokio::time::interval(PING_INTERVAL);
            ping_interval.tick().await;

            loop {
                tokio::select! {
                    Some(msg) = ws.next() => {
                        match msg {
                            Ok(Message::Text(text)) => {
                                if let Err(e) = tx.send(ConnectionEvent::FeedMessage(id.clone(), text.to_string())) {
                                    tracing::error!("{:?}: failed to send message: {}", id.clone(), e);
                                    break;
                                }
                            }
                            Ok(Message::Close(_)) => {
                                tracing::info!("{:?}: connection closed by server", id.clone());
                                break;
                            }
                            Err(e) => {
                                tracing::error!(
                                    "{:?}: WebSocket error: {}",
                                    id.clone(),
                                    e
                                );
                                break;
                            }
                            _ => {} // Ignore other message types
                        }
                    }
                    _ = ping_interval.tick() => {
                        if let Err(e) = ws.send(Message::Text(r#"{"type":"ping"}"#.into())).await {
                            tracing::error!("{:?}: failed to send ping: {}", id.clone(), e);
                            break;
                        }
                    }
                }
            }

            // Notify that the connection is closed
            let _ = tx.send(ConnectionEvent::ConnectionClosed(id.clone()));
        });
    }
}
