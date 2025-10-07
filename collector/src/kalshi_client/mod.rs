mod connection;
mod reconnector;

use std::time::Duration;

/// Each Kalshi WebSocket holds up to this many tickers.
pub const MAX_TICKERS_PER_CONNECTION: usize = 2000;
/// Base HTTP URL for the Kalshi API
pub const BASE_URL: &str = "https://api.elections.kalshi.com/trade-api/v2";
/// URL for the Kalshi WebSocket feed.
pub const WS_URL: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";
/// How long to wait for the first socket message before considering the feed dead.
pub const INITIAL_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// How often to send application-level pings to the server
pub const PING_INTERVAL: Duration = Duration::from_secs(30);
/// Maximum number of connections to open at once.
pub const MAX_PARALLELISM: usize = 50;

use crate::kalshi_client::connection::Connection;
use crate::kalshi_client::reconnector::Reconnecter;
use crate::{ConnectionEvent, ConnectionId, FeedEvent, FeedEventStream};
use crate::{KalshiCredentials, KalshiMarket, KalshiMarketsApiResponse};
use anyhow::Result;
use reqwest;
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Client for Kalshi's trading data feeds and API.
///
/// Fetches market data and manages WebSocket connections with automatic reconnection.
pub struct KalshiClient {
    event_tx: mpsc::Sender<ConnectionEvent>,
    event_rx: mpsc::Receiver<ConnectionEvent>,
    cancel: CancellationToken,
    http_client: reqwest::Client,
}

impl KalshiClient {
    /// Creates a new client instance.
    ///
    /// The cancellation token is used for graceful shutdown.
    pub fn new(cancel: CancellationToken) -> Self {
        let (event_tx, event_rx) = mpsc::channel::<ConnectionEvent>(1000);
        Self {
            event_tx,
            event_rx,
            cancel,
            http_client: reqwest::Client::new(),
        }
    }

    /// Starts WebSocket connections for real-time data feeds.
    ///
    /// Distributes markets across multiple connections and runs until cancelled.
    /// Sends `FeedEvent`s through the provided channel.
    pub async fn run(
        &mut self,
        credentials: KalshiCredentials,
        markets: Vec<KalshiMarket>,
        tx: mpsc::Sender<FeedEvent>,
    ) {
        // Distribute the markets across connections
        let connections = self.build_connections(credentials, markets);
        let connection_ids = connections.keys().cloned().collect::<Vec<_>>();
        let connection_count = connection_ids.len();

        // Spawn a reconnecter task
        let cancel_reconnecter = CancellationToken::new();
        let mut reconnecter = Reconnecter::new(
            connections,
            self.event_tx.clone(),
            cancel_reconnecter.clone(),
        );
        let reconnecter_tx = reconnecter.tx.clone();
        let reconnecter_handle = tokio::spawn(async move { reconnecter.run().await });

        // Initial connection requests
        tracing::info!(
            connection_count = connection_ids.len(),
            "requesting socket connections"
        );
        for id in connection_ids {
            if let Err(e) = reconnecter_tx.send(id.clone()) {
                tracing::error!(error = %e, "error sending initial connection request");
                break;
            }
        }

        // Wait for self to finish
        self.handle_events(reconnecter_tx, tx, connection_count)
            .await;

        // Wait for the reconnecter to finish
        cancel_reconnecter.cancel();
        let _ = reconnecter_handle.await;
    }

    /// Loop until the cancel token is cancelled, passing events to the message handler
    /// and requesting reconnects when a connection closes.
    async fn handle_events(
        &mut self,
        rtx: mpsc::UnboundedSender<ConnectionId>,
        client_tx: mpsc::Sender<FeedEvent>,
        n_connections: usize,
    ) {
        let mut n_open = 0;
        let mut id_is_open = HashMap::new();

        loop {
            // Get the next event or stop early if the cancel token is cancelled
            let event = tokio::select! {
                event = self.event_rx.recv() => event,
                _ = self.cancel.cancelled() => break,
            };

            if let Some(event) = event {
                let (should_continue, feed_event) = match event {
                    ConnectionEvent::FeedMessage(msg) => (true, FeedEvent::FeedMessage(msg)),
                    ConnectionEvent::ConnectionOpened(id) => {
                        n_open += 1;
                        id_is_open.insert(id.clone(), true);
                        // Use best-effort calculation that accounts for connection splitting
                        // When splits occur, total connections can exceed initial count
                        let pending_connections = n_connections.saturating_sub(n_open);
                        let current_total = id_is_open.len() + pending_connections;
                        (true, FeedEvent::ConnectionOpened(id, n_open, current_total))
                    }
                    ConnectionEvent::ConnectionClosed(id) => {
                        // Only decrement the open count if the connection was actually open,
                        // not if it failed during the initial connection attempt.
                        let was_open = id_is_open.remove(&id).is_some();
                        if was_open {
                            n_open -= 1;
                        }

                        // Use best-effort calculation that accounts for connection splitting
                        // When splits occur, total connections can exceed initial count
                        let pending_connections = n_connections.saturating_sub(n_open);
                        let current_total = id_is_open.len() + pending_connections;

                        // Send reconnection request
                        if let Err(e) = rtx.send(id.clone()) {
                            tracing::error!(connection_id = ?id, error = %e, "failed to send reconnection request - reconnecter channel closed");
                            (
                                false,
                                FeedEvent::ConnectionClosed(id, n_open, current_total),
                            )
                        } else {
                            tracing::debug!(connection_id = ?id, "successfully sent reconnection request");
                            (true, FeedEvent::ConnectionClosed(id, n_open, current_total))
                        }
                    }
                };

                // Forward to client
                if client_tx.send(feed_event).await.is_err() || !should_continue {
                    break;
                }
            } else {
                break;
            }
        }

        tracing::info!("client event handler shut down");
    }

    // Split the markets across different connections
    fn build_connections(
        &mut self,
        credentials: KalshiCredentials,
        m: Vec<KalshiMarket>,
    ) -> HashMap<ConnectionId, Connection> {
        let mut connections = HashMap::new();
        let mut id = 0;
        let mut markets = VecDeque::from(m);
        while !markets.is_empty() {
            let chunk = take_chunk(&mut markets);
            let connection = Connection::new(
                ConnectionId(id),
                credentials.clone(),
                chunk,
                self.event_tx.clone(),
            );
            connections.insert(ConnectionId(id), connection);
            id += 1;
        }

        connections
    }

    /// Fetches all actives markets from the Kalshi API.
    ///
    /// Returns only markets that are currently accepting orders.
    pub async fn fetch_active_markets(&self) -> Result<Vec<KalshiMarket>> {
        let markets = self
            .paginated_get_all::<KalshiMarketsApiResponse, KalshiMarket>(
                "/markets",
                Some(&[("status", "open"), ("limit", "1000")]),
            )
            .await?;

        Ok(markets)
    }

    /// Creates a stream of events for the given markets.
    ///
    /// This consumes the client and returns a stream that yields `FeedEvent`s.
    /// and a join handle for the background task.
    /// The stream will run until the client's cancellation token is cancelled.
    pub async fn into_stream(
        mut self,
        credentials: KalshiCredentials,
        markets: Vec<KalshiMarket>,
    ) -> Result<(FeedEventStream, tokio::task::JoinHandle<()>)> {
        let (tx, rx) = mpsc::channel(1000);
        let stream = FeedEventStream::new(rx);

        // Start the feed in a background task
        let handle = tokio::spawn(async move {
            self.run(credentials, markets, tx).await;
        });

        Ok((stream, handle))
    }

    pub async fn paginated_get_all<R, T>(
        &self,
        path: &str,
        filter: Option<&[(&str, &str)]>,
    ) -> Result<Vec<T>, anyhow::Error>
    where
        R: serde::de::DeserializeOwned + PaginatedResponse<T>,
    {
        let mut cursor = None;
        let mut all_data = Vec::new();
        loop {
            let (data, next_cursor) = self
                .paginated_get::<R, T>(path, cursor.take(), filter)
                .await?;
            all_data.extend(data);
            cursor = next_cursor.filter(|c| !c.is_empty());
            if cursor.is_none() {
                break;
            }
        }
        Ok(all_data)
    }

    pub async fn paginated_get<R, T>(
        &self,
        path: &str,
        cursor: Option<String>,
        filter: Option<&[(&str, &str)]>,
    ) -> Result<(Vec<T>, Option<String>), anyhow::Error>
    where
        R: serde::de::DeserializeOwned + PaginatedResponse<T>,
    {
        // Build HTTP get with optional cursor query param
        let url = format!("{}{}", BASE_URL, path);
        let mut req = self.http_client.get(url);
        if let Some(cursor) = cursor {
            req = req.query(&[("cursor", cursor)]);
        }

        if let Some(filter) = filter {
            req = req.query(filter);
        }

        // Send request and parse response
        let response = match req.send().await {
            // Return a timeout error if the request takes too long
            Ok(response) => response,
            Err(e) => {
                if e.is_timeout() {
                    return Err(anyhow::anyhow!("timeout: {}", e));
                }

                if let Some(status) = e.status() {
                    return Err(anyhow::anyhow!("http error: {}", status.as_u16()));
                }

                return Err(anyhow::anyhow!("transport error: {}", e));
            }
        };

        let body = match response.text().await {
            Ok(body) => body,
            Err(e) => {
                return Err(anyhow::anyhow!("transport error: {}", e));
            }
        };

        let mut deserializer = serde_json::Deserializer::from_str(&body);
        let page: R = serde_path_to_error::deserialize(&mut deserializer)
            .map_err(|e| anyhow::anyhow!("serialization error: {}", e))?;

        Ok({
            let next_cursor = page.next_cursor();
            (page.into_vec(), next_cursor)
        })
    }
}

/// Take a chunk of markets from the front of the queue such that the total number of
/// tickers is <= [`crate::client::MAX_TICKERS_PER_CONNECTION`] OR the chunk contains just one market,
/// in the case of a market with too many tickers.
fn take_chunk(markets: &mut VecDeque<KalshiMarket>) -> Vec<KalshiMarket> {
    let mut chunk = Vec::new();
    let mut n_assets = 0;
    while n_assets < MAX_TICKERS_PER_CONNECTION {
        if let Some(market) = markets.pop_front() {
            n_assets += 1;

            if n_assets > MAX_TICKERS_PER_CONNECTION && !chunk.is_empty() {
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

pub(crate) fn split_markets(markets: Vec<KalshiMarket>) -> (Vec<KalshiMarket>, Vec<KalshiMarket>) {
    let mid = markets.len() / 2;
    let (first_half, second_half) = markets.split_at(mid);
    (first_half.to_vec(), second_half.to_vec())
}

pub trait PaginatedResponse<T> {
    fn into_vec(self) -> Vec<T>;
    fn next_cursor(&self) -> Option<String>;
}

impl PaginatedResponse<KalshiMarket> for KalshiMarketsApiResponse {
    fn into_vec(self) -> Vec<KalshiMarket> {
        self.markets
    }

    fn next_cursor(&self) -> Option<String> {
        self.cursor.clone()
    }
}
