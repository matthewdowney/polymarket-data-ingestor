use crate::client::connection::{Connection, ConnectionEvent, ConnectionId};
use crate::client::{split_markets, MAX_PARALLELISM};
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

/// Accepts (re)connection requests at any pace and executes them in parallel
/// with error handling, retries, and backoff. Aggregates all events from open
/// connections into a single channel.
pub struct Reconnecter {
    /// Channel to send connection requests to.
    pub tx: mpsc::UnboundedSender<ConnectionId>,
    rx: mpsc::UnboundedReceiver<ConnectionId>,
    connections: HashMap<ConnectionId, Arc<Mutex<Connection>>>,
    /// Aggregate events from all connections into a single channel.
    event_tx: mpsc::Sender<ConnectionEvent>,
    /// Signal that the reconnecter has been stopped.
    cancel: CancellationToken,
    /// Counter of connections that have *ever* been opened successfully.
    n_opened: Arc<AtomicUsize>,
    /// Counter for generating new connection IDs when splitting.
    next_connection_id: Arc<AtomicUsize>,
}

impl Reconnecter {
    /// Create a new reconnecter which controls the given connections and
    /// forwards their messages to `event_tx`.
    pub fn new(
        connections: HashMap<ConnectionId, Connection>,
        event_tx: mpsc::Sender<ConnectionEvent>,
        cancel: CancellationToken,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<ConnectionId>();

        // Find the highest connection ID to start the counter from
        let max_id = connections
            .keys()
            .map(|ConnectionId(id)| *id)
            .max()
            .unwrap_or(0);

        let connections = connections
            .into_iter()
            .map(|(k, v)| (k, Arc::new(Mutex::new(v))))
            .collect();
        Self {
            tx,
            rx,
            connections,
            event_tx,
            cancel,
            n_opened: Arc::new(AtomicUsize::new(0)),
            next_connection_id: Arc::new(AtomicUsize::new(max_id as usize + 1)),
        }
    }

    /// Check if all connections have opened at least once.
    fn all_have_opened(&self) -> bool {
        self.n_opened.load(Ordering::Relaxed) == self.connections.len()
    }

    /// Monitor connection requests, opening connections in batches and forwarding
    /// messages to [`Self::event_tx`]. Runs until the cancel token is cancelled.
    pub async fn run(&mut self) {
        tracing::info!(
            initial_connection_count = self.connections.len(),
            "reconnecter_started"
        );
        let mut error_count: u64 = 0;
        loop {
            // Either await backoff or cancellation
            tokio::select! {
                _ = self.backoff(error_count) => {}
                _ = self.cancel.cancelled() => break,
            }

            // Take N connection ids at once to open (returns None if the cancel token is cancelled)
            tracing::trace!("reconnecter waiting for connection requests...");
            if let Some(ids) = self.recv_n(MAX_PARALLELISM).await {
                let n = ids.len();
                tracing::debug!("reconnecter received {} connection requests", n);
                let n_errors = self.open_all(ids).await;
                error_count = match n_errors {
                    0 => 0, // reset if no errors, increment only if majority failed
                    _ if n_errors < n / 2 => error_count,
                    _ => error_count + 1,
                };

                tracing::debug!(
                    successful_connections = n - n_errors,
                    total_attempts = n,
                    error_count = error_count,
                    backoff_duration_ms = self.backoff_duration(error_count).as_millis(),
                    current_connection_count = self.connections.len(),
                    "connection_batch_completed"
                );
            } else {
                break; // tx is closed
            }
        }

        tracing::debug!("main reconnecter loop finished, closing connections");
        self.stop().await;
        tracing::info!("reconnecter shut down");
    }

    /// Close the reconnecter and wait for all connections to close.
    async fn stop(&mut self) {
        tracing::debug!("stopping reconnecter");

        // Call close on all connections in parallel
        let mut tasks = FuturesUnordered::new();
        for conn in self.connections.values() {
            let conn = Arc::clone(conn);
            tasks.push(async move {
                let mut conn = conn.lock().await;
                let id = conn.id.clone();
                conn.close().await.map_err(|e| (id, e))
            });
        }

        // Wait for all connections to close
        tracing::debug!("waiting for connections to close");
        while let Some(result) = tasks.next().await {
            if let Err((id, e)) = result {
                tracing::error!("error closing connection {:?}: {}", id, e);
            }
        }
    }

    /// Open a [`Connection`] and send a connection closed event if it fails.
    async fn connect(
        connection: &mut Connection,
        event_tx: mpsc::Sender<ConnectionEvent>,
        n_open: &AtomicUsize,
    ) -> Result<ConnectionId, (ConnectionId, anyhow::Error)> {
        let is_reconnect = connection.has_ever_opened;
        if let Err(e) = connection.connect().await {
            tracing::error!("error connecting to connection {:?}: {}", connection.id, e);
            event_tx
                .send(ConnectionEvent::ConnectionClosed(connection.id.clone()))
                .await
                .map_err(|e| {
                    (
                        connection.id.clone(),
                        anyhow::anyhow!("failed to send connection closed event: {}", e),
                    )
                })?;
            Err((connection.id.clone(), e))
        } else {
            // Increment the counter on the first connection success
            if !is_reconnect {
                n_open.fetch_add(1, Ordering::Relaxed);
            }
            Ok(connection.id.clone())
        }
    }

    /// Await the next available connection id, and then try to read up
    /// to `n-1` more without blocking.
    async fn recv_n(&mut self, n: usize) -> Option<Vec<ConnectionId>> {
        let fst = tokio::select! {
            id = self.rx.recv() => id?,
            _ = self.cancel.cancelled() => return None,
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

    fn backoff_duration(&self, error_count: u64) -> Duration {
        if error_count > 0 {
            Duration::from_secs(error_count.max(3))
        } else {
            Duration::from_millis(500)
        }
    }

    async fn backoff(&self, error_count: u64) {
        if error_count > 0 {
            tokio::time::sleep(self.backoff_duration(error_count)).await;
        } else {
            tokio::time::sleep(Duration::from_millis(500)).await; // avoid hammering the server
        }
    }

    /// Open all the connections in parallel and count the number of errors.
    /// Checks for split conditions before attempting connection.
    async fn open_all(&mut self, ids: Vec<ConnectionId>) -> usize {
        // Check for split conditions and perform splits
        let mut final_ids = Vec::new();
        for id in ids {
            // Check if this connection should be split
            if let Some(conn) = self.connections.get(&id) {
                let should_split = {
                    let mut connection = conn.lock().await;
                    connection.process_connection_closed(); // Process connection closure first
                    connection.should_split()
                };

                if should_split {
                    tracing::info!(
                        connection_id = ?id,
                        "connection_split_initiated"
                    );

                    if let Some((first_id, second_id)) = self.split_connection(id.clone()) {
                        final_ids.push(first_id);
                        final_ids.push(second_id);
                    } else {
                        tracing::warn!(
                            connection_id = ?id,
                            "connection_split_failed"
                        );
                        final_ids.push(id);
                    }
                } else {
                    final_ids.push(id);
                }
            } else {
                tracing::error!(connection_id = ?id, "connection not found");
                final_ids.push(id); // Keep it to avoid losing the ID
            }
        }

        // Fire off all the connection attempts in parallel
        let mut handles = Vec::with_capacity(final_ids.len());
        for id in final_ids {
            if let Some(conn) = self.connections.get(&id) {
                let conn = Arc::clone(conn);
                let event_tx = self.event_tx.clone();
                let n_open = Arc::clone(&self.n_opened);
                handles.push(tokio::spawn(async move {
                    let mut conn = conn.lock().await;
                    Self::connect(&mut conn, event_tx, &n_open).await
                }));
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
                    // Only log errors after first successful connection
                    if self.all_have_opened() {
                        tracing::error!("error reopening connection {:?}: error={}", id, e);
                    }
                    n_errors += 1;
                }
                Err(join_error) => {
                    // Only log errors after first successful connection
                    tracing::error!("join error opening connection: {}", join_error);
                    n_errors += 1;
                }
            }
        }

        n_errors
    }

    /// Split a connection into two new connections with roughly equal market distribution.
    ///
    /// Removes the original connection and creates two new connections with split markets.
    /// Returns the new connection IDs if successful, or None if the connection cannot be split.
    pub fn split_connection(&mut self, id: ConnectionId) -> Option<(ConnectionId, ConnectionId)> {
        // Remove the original connection
        let original_connection = self.connections.remove(&id)?;

        // Extract the connection to get its markets (this will block briefly)
        let markets = {
            let conn = original_connection.try_lock().ok()?;
            conn.markets.clone()
        };

        // Cannot split if we have 1 or fewer markets
        if markets.len() <= 1 {
            // Put the connection back
            self.connections.insert(id, original_connection);
            return None;
        }

        // Split the markets
        let (first_markets, second_markets) = split_markets(markets);

        // Generate new connection IDs
        let first_id = ConnectionId(self.next_connection_id.fetch_add(1, Ordering::Relaxed) as u64);
        let second_id =
            ConnectionId(self.next_connection_id.fetch_add(1, Ordering::Relaxed) as u64);

        // Create new connections
        let first_connection = Connection::new(
            first_id.clone(),
            first_markets.clone(),
            self.event_tx.clone(),
        );
        let second_connection = Connection::new(
            second_id.clone(),
            second_markets.clone(),
            self.event_tx.clone(),
        );

        // Add the new connections to our tracking
        self.connections
            .insert(first_id.clone(), Arc::new(Mutex::new(first_connection)));
        self.connections
            .insert(second_id.clone(), Arc::new(Mutex::new(second_connection)));

        tracing::info!(
            original_connection_id = ?id,
            new_connection_ids = ?[&first_id, &second_id],
            "connection_split_successful"
        );

        // Log individual market isolation if we get down to single markets
        if first_markets.len() == 1 {
            if let Some(market_id) = first_markets.first().and_then(|m| m.id.as_ref()) {
                tracing::warn!(
                    connection_id = ?first_id,
                    market_id = market_id,
                    "problematic_market_isolated"
                );
            }
        }
        if second_markets.len() == 1 {
            if let Some(market_id) = second_markets.first().and_then(|m| m.id.as_ref()) {
                tracing::warn!(
                    connection_id = ?second_id,
                    market_id = market_id,
                    "problematic_market_isolated"
                );
            }
        }

        Some((first_id, second_id))
    }
}
