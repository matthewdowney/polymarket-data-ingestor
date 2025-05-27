mod connection;
mod reconnecter;

use std::time::Duration;

/// Each Polymarket WebSocket allows subscribing to this many order books.
pub const MAX_ASSETS_PER_CONNECTION: usize = 25;
/// URL for book data WebSocket feed.
pub const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
/// How long to wait for the first socket message before considering the feed dead.
pub const INITIAL_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// How often to send application-level pings to the server.
pub const PING_INTERVAL: Duration = Duration::from_secs(15);
/// Maximum number of connections to open at once.
pub const MAX_PARALLELISM: usize = 50;

use crate::PolymarketMarket;
use anyhow::Result;
use connection::{Connection, ConnectionEvent, ConnectionId};
use reconnecter::Reconnecter;
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc;

pub type MsgHandler = Box<dyn FnMut(&str) -> Result<()>>;

/// A client which manages a set of underlying connections to Polymarket book feeds
/// and aggregates them into a single channel.
pub struct Client {
    msg_handler: MsgHandler,
    event_tx: mpsc::UnboundedSender<ConnectionEvent>,
    event_rx: mpsc::UnboundedReceiver<ConnectionEvent>,
}

impl Client {
    /// Create a new client with a message handler.
    pub fn new(msg_handler: MsgHandler) -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel::<ConnectionEvent>();
        Self {
            msg_handler,
            event_tx,
            event_rx,
        }
    }

    /// Open connections to the given markets and loop forever, handling events with the
    /// Client's message handler.
    pub async fn run_forever(&mut self, markets: Vec<PolymarketMarket>) {
        // Distribute the markets across connections
        let connections = self.build_connections(markets);
        let connection_ids = connections.keys().cloned().collect::<Vec<_>>();

        // Spawn a reconnecter task and send connection requests to it
        let mut reconnecter = Reconnecter::new(connections, self.event_tx.clone());
        let reconnecter_tx = reconnecter.tx.clone();
        tokio::spawn(async move { reconnecter.run().await });

        tracing::info!("sending {} connection requests", connection_ids.len());
        for id in connection_ids {
            reconnecter_tx.send(id.clone()).unwrap();
        }

        self.handle_events(reconnecter_tx).await;
    }

    /// Loop forever, passing events to the message handler and requesting reconnects
    /// when a connection closes.
    async fn handle_events(&mut self, rtx: mpsc::UnboundedSender<ConnectionId>) {
        loop {
            if let Some(event) = self.event_rx.recv().await {
                match &event {
                    ConnectionEvent::FeedMessage(id, msg) => {
                        if let Err(e) = (self.msg_handler)(msg) {
                            tracing::error!("error handling message={:?}: {}", id, e);
                        }
                    }
                    ConnectionEvent::ConnectionClosed(id) => {
                        if let Err(e) = rtx.send(id.clone()) {
                            tracing::error!("error sending reconnection request: {}", e);
                        }
                    }
                }
            }
        }
    }

    // Split the markets across different connections
    fn build_connections(&mut self, m: Vec<PolymarketMarket>) -> HashMap<ConnectionId, Connection> {
        let mut connections = HashMap::new();
        let mut id = 0;
        let mut markets = VecDeque::from(m);
        while !markets.is_empty() {
            let chunk = take_chunk(&mut markets);
            let connection = Connection::new(ConnectionId(id), chunk, self.event_tx.clone());
            connections.insert(ConnectionId(id), connection);
            id += 1;
        }

        connections
    }
}

/// Take a chunk of markets from the front of the queue such that the total number of
/// assets is <= [`crate::client::MAX_ASSETS_PER_CONNECTION`] OR the chunk contains just one market,
/// in the case of a market with too many assets.
fn take_chunk(markets: &mut VecDeque<PolymarketMarket>) -> Vec<PolymarketMarket> {
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
