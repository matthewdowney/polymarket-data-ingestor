mod connection;

use std::time::Duration;

/// Maximum number of connections allowed by Kalshi
pub const MAX_CONNECTIONS: usize = 20;
/// Base HTTP URL for the Kalshi API
pub const BASE_URL: &str = "https://api.elections.kalshi.com/trade-api/v2";
/// URL for the Kalshi WebSocket feed.
pub const WS_URL: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";
/// How long to wait for the first socket message before considering the feed dead.
pub const INITIAL_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// How often to send application-level pings to the server
pub const PING_INTERVAL: Duration = Duration::from_secs(30);
/// We only open one connection on Kalshi, so we assign ID to 0.
const CONNECTION_ID: ConnectionId = ConnectionId(0);

use crate::kalshi_client::connection::Connection;
use crate::{ConnectionEvent, ConnectionId, FeedEvent, FeedEventStream};
use crate::{KalshiCredentials, KalshiMarket, KalshiMarketsApiResponse};
use anyhow::Result;
use reqwest;
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
        let mut connection = Connection::new(credentials, markets, self.event_tx.clone());

        let cancel_clone = self.cancel.clone();
        let connection_handle = tokio::spawn(async move {
            if let Err(e) = connection.connect().await {
                tracing::error!(error = %e, "initial connection failed");
                return;
            }

            loop {
                tokio::select! {
                    result = connection.run_until_closed() => {
                        match result {
                            Ok(()) => {
                                tracing::info!("connection closed");
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "connection error");
                            }
                        }

                        let mut backoff = Duration::from_secs(1);
                        loop {
                            tokio::select! {
                                _ = tokio::time::sleep(backoff) => {
                                    match connection.connect().await {
                                        Ok(()) => {
                                            tracing::info!("reconnected successfully");
                                            break;
                                        }
                                        Err(e) => {
                                            tracing::warn!(error = %e, backoff_secs = backoff.as_secs(), "reconnection failed, retrying");
                                            backoff = (backoff * 2).min(Duration::from_secs(30));
                                        }
                                    }
                                }
                                _ = cancel_clone.cancelled() => return,
                            }
                        }
                    }
                    _ = cancel_clone.cancelled() => {
                        tracing::info!("shutdown request received, stopping reconnection attempts");
                        let _ = connection.close().await;
                        return;
                    }
                }
            }
        });

        self.handle_events(tx).await;

        let _ = connection_handle.await;
    }

    async fn handle_events(&mut self, client_tx: mpsc::Sender<FeedEvent>) {
        loop {
            let event = tokio::select! {
                event = self.event_rx.recv() => event,
                _ = self.cancel.cancelled() => break,
            };

            if let Some(event) = event {
                let feed_event = match event {
                    ConnectionEvent::FeedMessage(msg) => FeedEvent::FeedMessage(msg),
                    ConnectionEvent::ConnectionOpened(id) => FeedEvent::ConnectionOpened(id, 1, 1),
                    ConnectionEvent::ConnectionClosed(id) => FeedEvent::ConnectionClosed(id, 0, 1),
                };

                if client_tx.send(feed_event).await.is_err() {
                    break;
                }
            } else {
                break;
            }
        }

        tracing::info!("client event handler shut down");
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
