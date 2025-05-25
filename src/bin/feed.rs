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
    io::{BufRead, BufReader, BufWriter, Write},
    sync::Arc,
    time::Duration,
};
use tokio::time::timeout;
use tokio::{sync::mpsc, time::Instant};
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

#[derive(Debug)]
struct Connection {
    id: ConnectionId,
    markets: Vec<PolymarketMarket>,
}

impl Connection {
    async fn try_open(&self, event_tx: mpsc::UnboundedSender<ConnectionEvent>) -> Result<()> {
        let url = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

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

        // Configure TLS
        let tls = TlsConnector::builder()
            .danger_accept_invalid_certs(false)
            .build()
            .context("failed to build TLS connector")?;

        let connector = Connector::NativeTls(tls);
        let config = WebSocketConfig::default();

        // Connect to the WebSocket server with TLS
        let (mut ws_stream, _) =
            connect_async_tls_with_config(url, Some(config), false, Some(connector))
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
            let mut ping_interval = tokio::time::interval(Duration::from_secs(15));
            ping_interval.tick().await;
            
            loop {
                tokio::select! {
                    Some(msg) = ws.next() => {
                        match msg {
                            Ok(Message::Text(text)) => {
                                if let Err(e) = event_tx
                                    .send(ConnectionEvent::FeedMessage(id.clone(), text.to_string()))
                                {
                                    tracing::error!("{:?}: failed to send message: {}", id, e);
                                    break;
                                }
                            }
                            Ok(Message::Close(_)) => {
                                tracing::info!("{:?}: connection closed by server", id);
                                break;
                            }
                            Err(e) => {
                                tracing::error!("{:?}: WebSocket error: {}", id, e);
                                break;
                            }
                            _ => {} // Ignore other message types
                        }
                    }
                    _ = ping_interval.tick() => {
                        if let Err(e) = ws.send(Message::Text(r#"{"type":"ping"}"#.into())).await {
                            tracing::error!("{:?}: failed to send ping: {}", id, e);
                            break;
                        }
                    }
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
    connections: HashMap<ConnectionId, Arc<Connection>>,
    event_tx: mpsc::UnboundedSender<ConnectionEvent>,
}

impl Reconnecter {
    pub fn new(
        connections: HashMap<ConnectionId, Connection>,
        event_tx: mpsc::UnboundedSender<ConnectionEvent>,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<ConnectionId>();
        let connections = connections
            .into_iter()
            .map(|(k, v)| (k, Arc::new(v)))
            .collect();
        Self {
            tx,
            rx,
            connections,
            event_tx,
        }
    }

    async fn open_connection(
        connection: &Connection,
        event_tx: mpsc::UnboundedSender<ConnectionEvent>,
    ) -> Result<ConnectionId, (ConnectionId, anyhow::Error)> {
        tracing::info!("opening connection {:?}", connection.id);
        if let Err(e) = connection.try_open(event_tx.clone()).await {
            event_tx
                .send(ConnectionEvent::ConnectionClosed(connection.id.clone()))
                .unwrap();
            Err((connection.id.clone(), e))
        } else {
            Ok(connection.id.clone())
        }
    }

    pub async fn run(&mut self) {
        const MAX_PARALLELISM: usize = 50;
        let mut parallelism = MAX_PARALLELISM;
        let mut error_count: u64 = 0;

        loop {
            // Sleep longer if there are errors
            if error_count > 0 {
                tracing::debug!(
                    "backing off for {} seconds before next connection attempt",
                    error_count
                );
                tokio::time::sleep(Duration::from_secs(error_count)).await;
            } else {
                // Sleep a bit to avoid hammering the server
                tokio::time::sleep(Duration::from_millis(500)).await;
            }

            // Take N connection ids at once to open, but switch to one at a time if there's an error
            let ids = recv_n(&mut self.rx, parallelism).await;
            if let Some(ids) = ids {
                // Fire off all the connection attempts in parallel
                let mut handles = Vec::with_capacity(ids.len());
                for id in ids {
                    if let Some(conn) = self.connections.get(&id) {
                        let conn = Arc::clone(conn);
                        let event_tx = self.event_tx.clone();
                        handles.push(tokio::spawn(async move {
                            Self::open_connection(&conn, event_tx).await
                        }));
                    } else {
                        tracing::error!("connection {:?} not found", id);
                    }
                }

                // Check for errors
                let mut n_errors = 0;
                let n = handles.len();
                for handle in handles {
                    match handle.await {
                        Ok(Ok(_id)) => {
                            // Yay
                        }
                        Ok(Err((id, e))) => {
                            tracing::error!(
                                "error opening connection {:?}: errors={}, error={}",
                                id,
                                error_count,
                                e
                            );
                            n_errors += 1;
                        }
                        Err(join_error) => {
                            tracing::error!("join error opening connection: {}", join_error);
                            n_errors += 1;
                        }
                    }
                }

                // Modify the backoff and parallelism based on success rate.
                if n_errors > 0 {
                    // As long as there are errors, reduce the parallelism and increase the error count
                    parallelism = std::cmp::max(1, parallelism.saturating_sub(1));
                    error_count += 1;
                    tracing::debug!(
                        "{}/{} failed; adjusting parallelism={}, error_count={}",
                        n_errors,
                        n,
                        parallelism,
                        error_count
                    );
                } else {
                    // As long as there are successes, increase the parallelism
                    let nxt_parallelism = std::cmp::min(MAX_PARALLELISM, parallelism + 1);
                    // Cut the error count in half every time all the connections succeed, cap at MAX_PARALLELISM
                    let nxt_error_count = std::cmp::min(MAX_PARALLELISM as u64, error_count / 2);
                    if nxt_parallelism > parallelism || nxt_error_count < error_count {
                        tracing::debug!(
                            "{}/{} succeeded; adjusting parallelism={}, error_count={}",
                            n,
                            n,
                            nxt_parallelism,
                            nxt_error_count
                        );
                    }
                    parallelism = nxt_parallelism;
                    error_count = nxt_error_count;
                }
            } else {
                break;
            }
        }

        tracing::info!("reconnecter shut down");
    }
}

async fn recv_n<T>(rx: &mut mpsc::UnboundedReceiver<T>, n: usize) -> Option<Vec<T>> {
    let mut results = Vec::with_capacity(n);

    let fst = rx.recv().await;
    if let Some(fst) = fst {
        results.push(fst);
    } else {
        return None;
    }

    for _ in 0..(n - 1) {
        if let Ok(nxt) = rx.try_recv() {
            results.push(nxt);
        } else {
            break;
        }
    }

    Some(results)
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
    // Set up logging
    tracing_subscriber::fmt()
        .with_env_filter("feed=debug")
        .init();

    let markets: Vec<PolymarketMarket> = load_active_markets()?;
    println!("found {} active markets, connecting...", markets.len());

    // append to a logfile called feed.log, clearing it if it exists
    let file = File::options().write(true).create(true).truncate(true).open("feed.log")?;
    let mut writer = BufWriter::new(file);

    let mut msg_count = 0;
    let mut bytes_count = 0;
    let mut last_sample_time = Instant::now();
    connect(markets, 12, |msg| {
        msg_count += 1;
        bytes_count += msg.len();
        writer.write_all(msg.as_bytes())?;

        if last_sample_time.elapsed() > Duration::from_secs(15) {
            tracing::info!(
                "{} messages/sec, {} bytes/sec",
                msg_count / 15,
                bytes_count / 15
            );
            msg_count = 0;
            bytes_count = 0;
            last_sample_time = Instant::now();
            writer.flush()?;
        }
        Ok(())
    })
    .await?;
    Ok(())
}

// TODO: Add graceful shutdown
// TODO: Add running count of # of open and closed connections
