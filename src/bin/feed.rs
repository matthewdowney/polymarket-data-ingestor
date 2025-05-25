//! Connect to the Polymarket WS feed for all active markets.
//!
//! This is tricky because they only support 25-50 markets per connection,
//! and connections start to drop as new ones open.
use anyhow::{Context, Result};
use prediction_data_ingestor::{MARKETS_FILE, PolymarketMarket};
use std::{
    collections::{HashMap},
    fs::File,
    io::{BufRead, BufReader},
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;

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

enum ConnectionState {
    Pending,
    Open,
    Closed,
}

struct Connection {
    id: ConnectionId,
    markets: Vec<PolymarketMarket>,
    state: ConnectionState,
    // ws: WebSocket,
}

impl Connection {
    async fn try_open(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
enum ConnectionEvent {
    FeedMessage(ConnectionId, String),
    ConnectionClosed(ConnectionId),
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
struct ConnectionId(u64);

struct ConnectionManager {
    connections: HashMap<ConnectionId, Arc<Mutex<Connection>>>,

    /// First queue to check for connections to (re)open
    front_tx: mpsc::UnboundedSender<ConnectionId>,
    front_rx: mpsc::UnboundedReceiver<ConnectionId>,

    /// Second queue to check for connections to (re)open
    back_tx: mpsc::UnboundedSender<ConnectionId>,
    back_rx: mpsc::UnboundedReceiver<ConnectionId>,

    event_tx: mpsc::UnboundedSender<ConnectionEvent>,
    event_rx: mpsc::UnboundedReceiver<ConnectionEvent>,
}

impl ConnectionManager {
    pub fn new(mut markets: Vec<PolymarketMarket>, chunk_size: usize) -> Self {
        // Chunk the markets into groups of `chunk_size` and create a connection for each group
        let mut connections = HashMap::new();
        let mut id = 0;
        while !markets.is_empty() {
            let chunk = markets.drain(..chunk_size).collect();
            let connection = Arc::new(Mutex::new(Connection { id, markets: chunk }));
            connections.insert(ConnectionId(id), connection);
            id += 1;
        }

        // Connections can be sent to the front or back of the queue
        let (back_tx, back_rx) = mpsc::unbounded_channel::<ConnectionId>();
        let (front_tx, front_rx) = mpsc::unbounded_channel::<ConnectionId>();

        let (event_tx, event_rx) = mpsc::unbounded_channel::<ConnectionEvent>();

        Self {
            connections,
            front_tx,
            front_rx,
            back_tx,
            back_rx,
            event_tx,
            event_rx,
        }
    }

    pub async fn run_forever(&mut self, mut msg_handler: impl FnMut(&str) -> Result<()>) {
        tokio::spawn(async move {
            // TODO: Task that reads front and back rx and opens/closes connections as needed
        });

        // Handle events from the connections
        loop {
            if let Some(event) = self.event_rx.recv().await {
                if let Err(e) = self.on_event(&event, &mut msg_handler) {
                    tracing::error!("error handling event={:?}: {}", event, e);
                }
            }
        }
    }

    fn on_event(
        &mut self,
        event: &ConnectionEvent,
        mut msg_handler: impl FnMut(&str) -> Result<()>,
    ) -> Result<()> {
        match event {
            ConnectionEvent::FeedMessage(id, msg) => {
                // Connection is open when first message is received
                {
                    let connection = self
                        .connections
                        .get_mut(&id)
                        .expect(&format!("connection {:?} not found", id));
                    connection.lock().unwrap().state = ConnectionState::Open;
                }

                msg_handler(msg)?;
            }
            ConnectionEvent::ConnectionClosed(id) => {
                // Connection is closed when last market is closed
                {
                    let connection = self
                        .connections
                        .get_mut(&id)
                        .expect(&format!("connection {:?} not found", id));
                    connection.lock().unwrap().state = ConnectionState::Closed;
                }

                // When connections close, put them in front of the queue to be opened again
                self.front_tx.send(id.clone())?;
            }
        }

        Ok(())
    }
}

fn main() -> Result<()> {
    // Setup logging
    tracing_subscriber::fmt::init();

    let markets = load_active_markets()?;
    println!("found {} active markets", markets.len());

    let mut state = ConnectionManager::new(markets, 25);
    state.run(|_| Ok(())).await?;

    Ok(())
}
