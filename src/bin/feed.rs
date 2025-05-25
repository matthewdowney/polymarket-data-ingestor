//! Connect to the Polymarket WS feed for all active markets.
//!
//! This is tricky because they only support 25-50 markets per connection,
//! and connections start to drop as new ones open.
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use native_tls::TlsConnector;
use prediction_data_ingestor::{MARKETS_FILE, PolymarketMarket};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
    time::Duration,
};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::Connector;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};

/// Read the markets from disk and filter out inactive markets.
fn load_active_markets() -> Result<Vec<PolymarketMarket>> {
    let file = File::open(MARKETS_FILE).context("failed to open market data file")?;
    let reader = BufReader::new(file);

    let mut markets = Vec::new();
    for line in reader.lines() {
        let market: PolymarketMarket =
            serde_json::from_str(&line?).context("failed to parse market data")?;
        markets.push(market);
    }

    Ok(markets
        .into_iter()
        .filter(|m| m.enable_order_book && m.accepting_orders && !m.archived && !m.closed)
        .collect())
}

struct Connection {
    id: ConnectionId,
    markets: Vec<PolymarketMarket>,
    retry_count: u32,
    open: bool,
}

impl Connection {
    async fn try_open(&mut self, event_tx: mpsc::UnboundedSender<ConnectionEvent>) -> Result<()> {
        let url = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
        let sub_msg = serde_json::json!({
            "type": "MARKET",
            "assets_ids": self.markets.iter().flat_map(|m| m.tokens.iter()).map(|t| t.get("token_id").unwrap().as_str().unwrap()).collect::<Vec<_>>(),
        });
        tracing::info!("sub_msg: {}", sub_msg.to_string());

        // Configure TLS
        let tls = TlsConnector::builder()
            .danger_accept_invalid_certs(false)
            .build()
            .context("failed to build TLS connector")?;

        let connector = Connector::NativeTls(tls);
        let config = WebSocketConfig::default();

        // Connect to the WebSocket server with TLS
        let (mut ws_stream, _) = connect_async_tls_with_config(
            url,
            Some(config),
            false, // disable_nagle
            Some(connector),
        )
        .await
        .context("failed to connect to WebSocket server")?;

        // Send subscription message
        ws_stream
            .send(Message::Text(sub_msg.to_string().into()))
            .await
            .context("failed to send subscription message")?;

        // Await the first response message or timeout
        const READ_TIMEOUT: Duration = Duration::from_secs(10);
        let msg = timeout(READ_TIMEOUT, ws_stream.next()).await?;
        if let Some(Ok(Message::Text(text))) = msg {
            event_tx
                .send(ConnectionEvent::FeedMessage(
                    self.id.clone(),
                    text.to_string(),
                ))
                .unwrap();
        } else {
            tracing::error!(
                "no message received within {} seconds",
                READ_TIMEOUT.as_secs()
            );
            event_tx
                .send(ConnectionEvent::ConnectionClosed(self.id.clone()))
                .unwrap();
            ws_stream.close(None).await?;
            return Err(anyhow::anyhow!("no message received"));
        }

        // Spawn a task to handle incoming messages
        let id = self.id.clone();
        let event_tx = event_tx.clone();

        tokio::spawn(async move {
            let mut ws = ws_stream;
            while let Some(msg) = ws.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        if let Err(e) = event_tx
                            .send(ConnectionEvent::FeedMessage(id.clone(), text.to_string()))
                        {
                            tracing::error!("failed to send message: {}", e);
                            break;
                        }
                    }
                    Ok(Message::Close(_)) => {
                        tracing::info!("connection closed by server");
                        event_tx
                            .send(ConnectionEvent::ConnectionClosed(id.clone()))
                            .unwrap();
                        break;
                    }
                    Err(e) => {
                        tracing::error!("WebSocket error: {}", e);
                        event_tx
                            .send(ConnectionEvent::ConnectionClosed(id.clone()))
                            .unwrap();
                        break;
                    }
                    _ => {} // Ignore other message types
                }
            }
            // Notify that the connection is closed
            let _ = event_tx.send(ConnectionEvent::ConnectionClosed(id));
        });

        Ok(())
    }
}

struct Reconnecter {
    tx: mpsc::UnboundedSender<ConnectionId>,
    rx: mpsc::UnboundedReceiver<ConnectionId>,
    connections: HashMap<ConnectionId, Connection>,
    event_tx: mpsc::UnboundedSender<ConnectionEvent>,
}

impl Reconnecter {
    pub fn new(
        connections: HashMap<ConnectionId, Connection>,
        event_tx: mpsc::UnboundedSender<ConnectionEvent>,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<ConnectionId>();
        Self {
            tx,
            rx,
            connections,
            event_tx,
        }
    }

    pub async fn run(&mut self) {
        let mut error_count = 0;
        loop {
            if let Some(id) = self.rx.recv().await {
                if let Some(connection) = self.connections.get_mut(&id) {
                    if connection.open {
                        tracing::info!("connection {:?} already open", id);
                        continue;
                    }

                    if error_count > 0 {
                        tokio::time::sleep(Duration::from_secs(error_count as u64)).await;
                    }

                    tracing::info!("opening connection {:?}", id);
                    if let Err(e) = connection.try_open(self.event_tx.clone()).await {
                        tracing::error!("error opening connection {:?}: {}", id, e);
                        error_count += 1;
                        self.event_tx
                            .send(ConnectionEvent::ConnectionClosed(id))
                            .unwrap();
                    } else {
                        error_count = 0;
                        connection.open = true;
                    }
                } else {
                    tracing::error!("connection {:?} not found", id);
                }
            } else {
                break;
            }
        }

        tracing::info!("reconnecter shut down");
    }
}

#[derive(Debug)]
enum ConnectionEvent {
    FeedMessage(ConnectionId, String),
    ConnectionClosed(ConnectionId),
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
struct ConnectionId(u64);

pub async fn connect(
    mut markets: Vec<PolymarketMarket>,
    chunk_size: usize,
    mut msg_handler: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    // Chunk the markets into groups of `chunk_size` and create a connection for each group
    let mut connections = HashMap::new();
    let mut id = 0;
    while !markets.is_empty() {
        let chunk = markets
            .drain(..std::cmp::min(chunk_size, markets.len()))
            .collect();
        let connection = Connection {
            id: ConnectionId(id),
            markets: chunk,
            retry_count: 0,
            open: false,
        };
        connections.insert(ConnectionId(id), connection);
        id += 1;
    }

    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<ConnectionEvent>();
    let mut reconnecter = Reconnecter::new(connections, event_tx.clone());
    let reconnecter_tx = reconnecter.tx.clone();

    tokio::spawn(async move { reconnecter.run().await });
    for id in 0..id {
        reconnecter_tx.send(ConnectionId(id)).unwrap();
    }

    // Handle events from the connections
    loop {
        if let Some(event) = event_rx.recv().await {
            match &event {
                ConnectionEvent::FeedMessage(id, msg) => {
                    if let Err(e) = msg_handler(msg) {
                        tracing::error!("error handling message={:?}: {}", id, e);
                    }
                }
                ConnectionEvent::ConnectionClosed(id) => {
                    reconnecter_tx.send(id.clone());
                }
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Setup logging
    tracing_subscriber::fmt::init();

    let markets: Vec<PolymarketMarket> = load_active_markets()?;
    println!("found {} active markets, connecting...", markets.len());

    connect(markets, 12, |msg| {
        tracing::info!("{}", &msg[..120]);
        Ok(())
    })
    .await?;

    Ok(())
}
