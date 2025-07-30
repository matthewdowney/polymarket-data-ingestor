//! Client for Polymarket WebSocket connections and API interactions.

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

use crate::{MarketsApiResponse, PolymarketMarket};
use anyhow::Result;
use base64::{engine::general_purpose, Engine as _};
use connection::{Connection, ConnectionEvent, ConnectionId};
use futures::Stream;
use futures_util::{future::join_all, TryFutureExt};
use reconnecter::Reconnecter;
use reqwest;
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

/// Client for Polymarket's trading data feeds and API.
///
/// Fetches market data and manages WebSocket connections with automatic reconnection.
pub struct PolymarketClient {
    event_tx: mpsc::Sender<ConnectionEvent>,
    event_rx: mpsc::Receiver<ConnectionEvent>,
    cancel: CancellationToken,
    http_client: reqwest::Client,
}

/// Events emitted by the client during operation.
#[derive(Debug)]
pub enum FeedEvent {
    /// A raw JSON message received from the WebSocket feed.
    FeedMessage(String),
    /// A WebSocket connection was successfully opened.
    ///
    /// Contains: (connection_id, number_of_open_connections, total_connections)
    ConnectionOpened(ConnectionId, usize, usize),
    /// A WebSocket connection was closed or failed to connect.
    ///
    /// Contains: (connection_id, number_of_open_connections, total_connections)
    ///
    /// This event is emitted both when an open connection closes and when
    /// an initial connection attempt fails.
    ConnectionClosed(ConnectionId, usize, usize),
}

/// A stream of feed events from the Polymarket client.
pub struct FeedEventStream {
    rx: mpsc::Receiver<FeedEvent>,
}

impl Stream for FeedEventStream {
    type Item = FeedEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl FeedEventStream {
    fn new(rx: mpsc::Receiver<FeedEvent>) -> Self {
        Self { rx }
    }
}

impl PolymarketClient {
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
    pub async fn run(&mut self, markets: Vec<PolymarketMarket>, tx: mpsc::Sender<FeedEvent>) {
        // Distribute the markets across connections
        let connections = self.build_connections(markets);
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
                        (true, FeedEvent::ConnectionOpened(id, n_open, n_connections))
                    }
                    ConnectionEvent::ConnectionClosed(id) => {
                        // Only decrement the open count if the connection was actually open,
                        // not if it failed during the initial connection attempt.
                        let was_open = id_is_open.remove(&id).is_some();
                        if was_open {
                            n_open -= 1;
                        }

                        // Send reconnection request
                        if let Err(e) = rtx.send(id.clone()) {
                            tracing::error!(connection_id = ?id, error = %e, "failed to send reconnection request - reconnecter channel closed");
                            (
                                false,
                                FeedEvent::ConnectionClosed(id, n_open, n_connections),
                            )
                        } else {
                            tracing::debug!(connection_id = ?id, "successfully sent reconnection request");
                            (true, FeedEvent::ConnectionClosed(id, n_open, n_connections))
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

    /// Fetches all markets from the Polymarket API *with rewards enabled*.
    pub async fn fetch_sampling_markets(&self) -> Result<Vec<PolymarketMarket>> {
        let mut cursor = None;
        let mut markets = Vec::new();

        let mut error_count = 0;
        loop {
            match self
                .get_page(
                    "https://clob.polymarket.com/sampling-markets",
                    cursor.clone(),
                )
                .await
            {
                Ok(page) => {
                    markets.extend(page.data);
                    cursor = page.next_cursor;
                    if page.count < page.limit {
                        // no more pages
                        break;
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "error fetching sampling markets, retrying...");
                    error_count += 1;
                    sleep(Duration::from_secs(error_count.min(30))).await;
                }
            }
        }
        Ok(markets)
    }

    /// Fetches all active markets from the Polymarket API.
    ///
    /// Returns only markets that are currently accepting orders.
    pub async fn fetch_active_markets(&self) -> Result<Vec<PolymarketMarket>> {
        let markets = self.fetch_markets().await?;
        Ok(markets.into_iter().filter(|m| m.is_active()).collect())
    }

    /// Creates a stream of events for the given markets.
    ///
    /// This consumes the client and returns a stream that yields `FeedEvent`s
    /// and a join handle for the background task.
    /// The stream will run until the client's cancellation token is cancelled.
    pub async fn into_stream(
        mut self,
        markets: Vec<PolymarketMarket>,
    ) -> Result<(FeedEventStream, tokio::task::JoinHandle<()>)> {
        let (tx, rx) = mpsc::channel(1000);
        let stream = FeedEventStream::new(rx);

        // Start the feed in a background task
        let handle = tokio::spawn(async move {
            self.run(markets, tx).await;
        });

        Ok((stream, handle))
    }

    /// Fetches all markets from the Polymarket API using concurrent pagination.
    pub async fn fetch_markets(&self) -> Result<Vec<PolymarketMarket>> {
        let mut n_pages = 100;
        let mut ids = (0..n_pages).collect::<Vec<_>>();
        let mut data = Vec::new();
        let mut consecutive_errors = 0;

        while !ids.is_empty() {
            const MAX_CONCURRENCY: usize = 30;
            let handles = ids
                .drain(0..MAX_CONCURRENCY.min(ids.len()))
                .map(|id| {
                    self.get_page(
                        "https://clob.polymarket.com/markets",
                        if id == 0 {
                            None
                        } else {
                            Some(encode_number_to_base64(id * 500))
                        },
                    )
                    .map_err(move |e| (id, e))
                    .map_ok(move |result| (id, result))
                })
                .collect::<Vec<_>>();

            let mut saw_error = false;
            let results = join_all(handles).await;
            for result in results {
                match result {
                    Ok((page, result)) => {
                        if page == n_pages - 1 {
                            if result.data.is_empty() {
                                tracing::debug!("{} was empty; no more pages", page);
                            } else {
                                tracing::debug!("adding {} more pages to query", MAX_CONCURRENCY);
                                ids.extend(n_pages..(n_pages + MAX_CONCURRENCY as u64));
                                n_pages += MAX_CONCURRENCY as u64;
                            }
                        }

                        data.extend(result.data);
                    }
                    Err((page, e)) => {
                        ids.push(page);
                        tracing::warn!("page={}: error={}", page, e);
                        saw_error = true;
                    }
                }
            }

            consecutive_errors = if saw_error { consecutive_errors + 1 } else { 0 };
            if !ids.is_empty() {
                sleep(Duration::from_secs(consecutive_errors)).await;
            }
        }

        Ok(data)
    }

    /// Fetches a single page of markets from the API.
    async fn get_page(&self, url: &str, cursor: Option<String>) -> Result<MarketsApiResponse> {
        let mut url = url.to_string();
        if let Some(c) = &cursor {
            url.push_str(&format!("?next_cursor={}", c));
        }

        let res = self.http_client.get(url).send().await?;

        // Handle rate limit errors without stack trace
        if res.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(anyhow::anyhow!("HTTP 429: rate limit exceeded"));
        }

        let res = res.error_for_status()?;
        let bytes = res.bytes().await?;

        let de = &mut serde_json::Deserializer::from_slice(&bytes);
        let result: MarketsApiResponse = serde_path_to_error::deserialize(de)?;
        Ok(result)
    }
}

/// Encodes a number to base64 for use as a pagination cursor.
fn encode_number_to_base64(n: u64) -> String {
    let num_str = n.to_string();
    general_purpose::STANDARD.encode(num_str.as_bytes())
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

impl Drop for PolymarketClient {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}