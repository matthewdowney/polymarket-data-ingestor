//! Connect to the Polymarket WS feed for all active markets.
//!
//! This is tricky because they only support 25-50 markets per connection,
//! and connections start to drop as new ones open.
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use native_tls::TlsConnector;
use prediction_data_ingestor::{MARKETS_FILE, PolymarketMarket};
use std::{
    collections::{HashMap, VecDeque},
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    sync::Arc,
    time::Duration,
};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio::{sync::mpsc, time::Instant};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};

// TODO: Add graceful shutdown
// TODO: Add running count of # of open and closed connections
// TODO: Should expose a queue instead of taking a callback
// TODO: Wrap together with parallel markets fetching beind a PolymarketFeed struct

/// Each Polymarket WebSocket allows subscribing to this many order books.
pub const MAX_ASSETS_PER_CONNECTION: usize = 25;

/// URL for book data WebSocket feed.
pub const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// How long to wait for the first socket message before considering the feed dead.
pub const INITIAL_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// How often to send application-level pings to the server.
pub const PING_INTERVAL: Duration = Duration::from_secs(15);

/// Maximum number of connections to open at once.
pub const MAX_PARALLELISM: usize = 25;

/// Represents a single WebSocket shard covering a subset of all markets.
#[derive(Debug)]
struct Connection {
    /// Used to identify the connection for restarts etc.
    id: ConnectionId,
    /// Markets covered by this connection.
    markets: Vec<PolymarketMarket>,
    /// Used to send events to the main thread.
    tx: mpsc::UnboundedSender<ConnectionEvent>,
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

        let _ = tokio::spawn(async move {
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

/// Accepts (re)connection requests at any pace and executes them in parallel
/// with error handling, retries, and backoff. Aggregates all events from open
/// connections into a single channel.
struct Reconnecter {
    tx: mpsc::UnboundedSender<ConnectionId>,
    rx: mpsc::UnboundedReceiver<ConnectionId>,
    connections: HashMap<ConnectionId, Arc<Connection>>,
    /// Aggregate events from all connections into a single channel.
    event_tx: mpsc::UnboundedSender<ConnectionEvent>,
}

impl Reconnecter {
    /// Create a new reconnecter which controls the given connections and
    /// forwards their messages to `event_tx`.
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

    /// Monitor connection requests, opening connections in batches and forwarding
    /// messages to [`Self::event_tx`]. Spawn this in a new task before calling
    /// [`Self::request_connection`].
    pub async fn run(&mut self) {
        let mut error_count: u64 = 0;
        loop {
            self.backoff(error_count).await;
            // Take N connection ids at once to open
            if let Some(ids) = self.recv_n(MAX_PARALLELISM).await {
                let n = ids.len();
                let n_errors = self.open_all(ids).await;
                error_count = if n_errors > 0 { error_count + 1 } else { 0 };
                tracing::debug!("{}/{} failed; error_count={}", n_errors, n, error_count);
            } else {
                break; // tx is closed
            }
        }

        tracing::info!("reconnecter shut down");
    }

    /// Open a [`Connection`] and send a connection closed event if it fails.
    async fn connect(
        connection: &Connection,
        event_tx: mpsc::UnboundedSender<ConnectionEvent>,
    ) -> Result<ConnectionId, (ConnectionId, anyhow::Error)> {
        tracing::info!("opening connection {:?}", connection.id);
        if let Err(e) = connection.connect().await {
            event_tx
                .send(ConnectionEvent::ConnectionClosed(connection.id.clone()))
                .map_err(|e| (connection.id.clone(), anyhow::anyhow!("failed to send connection closed event: {}", e)))?;
            Err((connection.id.clone(), e))
        } else {
            Ok(connection.id.clone())
        }
    }

    /// Await the next available connection id, and then try to read up
    /// to `n-1` more without blocking.
    async fn recv_n(&mut self, n: usize) -> Option<Vec<ConnectionId>> {
        let Some(fst) = self.rx.recv().await else {
            return None;
        };

        let mut results = vec![fst];
        for _ in 0..(n - 1) {
            let Ok(nxt) = self.rx.try_recv() else {
                return Some(results);
            };
            results.push(nxt);
        }

        Some(results)
    }

    async fn backoff(&self, error_count: u64) {
        if error_count > 0 {
            let backoff = error_count.max(3);
            tracing::debug!(
                "backing off for {} seconds before next connection attempt",
                backoff
            );
            tokio::time::sleep(Duration::from_secs(backoff)).await;
        } else {
            tokio::time::sleep(Duration::from_millis(500)).await; // avoid hammering the server
        }
    }

    /// Open all the connections in parallel and count the number of errors.
    async fn open_all(&mut self, ids: Vec<ConnectionId>) -> usize {
        // Fire off all the connection attempts in parallel
        let mut handles = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(conn) = self.connections.get(&id) {
                let conn = Arc::clone(conn);
                let event_tx = self.event_tx.clone();
                handles.push(tokio::spawn(
                    async move { Self::connect(&conn, event_tx).await },
                ));
            } else {
                tracing::error!("connection {:?} not found", id);
            }
        }

        // Count the number of errors
        let mut n_errors = 0;
        for handle in handles {
            match handle.await {
                Ok(Ok(_id)) => {}
                Ok(Err((id, e))) => {
                    tracing::error!("error opening connection {:?}: error={}", id, e);
                    n_errors += 1;
                }
                Err(join_error) => {
                    tracing::error!("join error opening connection: {}", join_error);
                    n_errors += 1;
                }
            }
        }

        n_errors
    }
}

#[derive(Debug)]
enum ConnectionEvent {
    FeedMessage(ConnectionId, String),
    ConnectionClosed(ConnectionId),
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
struct ConnectionId(u64);

/// Take a chunk of markets from the front of the queue such that
/// the total number of assets is <= [`MAX_ASSETS_PER_CONNECTION`] 
/// OR the chunk contains just one market.
pub fn take_chunk(markets: &mut VecDeque<PolymarketMarket>) -> Vec<PolymarketMarket> {
    let mut chunk = Vec::new();
    let mut n_assets = 0;
    while n_assets < MAX_ASSETS_PER_CONNECTION {
        if let Some(market) = markets.pop_front() {
            n_assets += market.tokens.len();
            // If we've exceeded the limit, push the market back and return the chunk
            // (unless one market has > MAX_ASSETS_PER_CONNECTION assets)
            if n_assets > MAX_ASSETS_PER_CONNECTION && !chunk.is_empty() {
                markets.push_front(market);
                return chunk;
            }
            chunk.push(market);
        } else {
            break;
        }
    }
    chunk
}

pub async fn connect(
    mut markets: VecDeque<PolymarketMarket>,
    mut msg_handler: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<ConnectionEvent>();

    // Chunk the markets into groups and create a connection for each group
    let mut connections = HashMap::new();
    let mut id = 0;
    while !markets.is_empty() {
        let chunk = take_chunk(&mut markets);
        let connection = Connection {
            id: ConnectionId(id),
            markets: chunk,
            tx: event_tx.clone(),
        };
        connections.insert(ConnectionId(id), connection);
        id += 1;
    }

    // Spawn the reconnecter thread and request all the connections
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
}

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

#[tokio::main]
async fn main() -> Result<()> {
    // Set up logging
    tracing_subscriber::fmt()
        .with_env_filter("feed=debug")
        .init();

    let markets: Vec<PolymarketMarket> = load_active_markets()?;
    println!("found {} active markets, connecting...", markets.len());

    // append to a logfile called feed.log, clearing it if it exists
    let file = File::options()
        .write(true)
        .create(true)
        .truncate(true)
        .open("feed.log")?;
    let mut writer = BufWriter::new(file);

    let mut msg_count = 0;
    let mut bytes_count = 0;
    let mut last_sample_time = Instant::now();
    connect(VecDeque::from(markets), |msg| {
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
