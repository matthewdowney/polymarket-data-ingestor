use crate::client::MAX_PARALLELISM;
use crate::client::connection::{Connection, ConnectionEvent, ConnectionId};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

/// Accepts (re)connection requests at any pace and executes them in parallel
/// with error handling, retries, and backoff. Aggregates all events from open
/// connections into a single channel.
///
/// # Example
///
/// ```rust,no_run
///
/// // Channel to aggregate events from all connections
/// let (event_tx, _event_rx) = mpsc::channel::<ConnectionEvent>(1000);
/// let connections = HashMap::new();
/// let reconnecter = Reconnecter::new(connections, event_tx);
///
/// // Spawn reconnecter task and send connection requests to tx
/// let tx = reconnecter.tx.clone();
/// tokio::spawn(async move { reconnecter.run().await });
///
/// for conn in connections.values() {
///     tx.send(conn.id).await.unwrap();
/// }
/// ```
pub struct Reconnecter {
    /// Channel to send connection requests to.
    pub tx: mpsc::Sender<ConnectionId>,
    rx: mpsc::Receiver<ConnectionId>,
    connections: HashMap<ConnectionId, Arc<Mutex<Connection>>>,
    /// Aggregate events from all connections into a single channel.
    event_tx: mpsc::Sender<ConnectionEvent>,
    /// Signal that the reconnecter has been stopped.
    cancel: CancellationToken,
    /// Counter of connections that have *ever* been opened successfully.
    n_opened: Arc<AtomicUsize>,
}

impl Reconnecter {
    /// Create a new reconnecter which controls the given connections and
    /// forwards their messages to `event_tx`.
    pub fn new(
        connections: HashMap<ConnectionId, Connection>,
        event_tx: mpsc::Sender<ConnectionEvent>,
        cancel: CancellationToken,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<ConnectionId>(1000);
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
        }
    }

    /// Check if all connections have opened at least once.
    fn all_have_opened(&self) -> bool {
        self.n_opened.load(Ordering::Relaxed) == self.connections.len()
    }

    /// Monitor connection requests, opening connections in batches and forwarding
    /// messages to [`Self::event_tx`]. Runs until the cancel token is cancelled.
    pub async fn run(&mut self) {
        let mut error_count: u64 = 0;
        loop {
            // Either await backoff or cancellation
            tokio::select! {
                _ = self.backoff(error_count) => {}
                _ = self.cancel.cancelled() => break,
            }

            // Take N connection ids at once to open (returns None if the cancel token is cancelled)
            if let Some(ids) = self.recv_n(MAX_PARALLELISM).await {
                let n = ids.len();
                let n_errors = self.open_all(ids).await;
                error_count = match n_errors {
                    0 => 0, // reset if no errors, increment only if majority failed
                    _ if n_errors < n / 2 => error_count,
                    _ => error_count + 1,
                };

                tracing::debug!(
                    "{}/{} new connections succeeded, backing off {:?}",
                    n - n_errors,
                    n,
                    self.backoff_duration(error_count)
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
    async fn open_all(&mut self, ids: Vec<ConnectionId>) -> usize {
        // Fire off all the connection attempts in parallel
        let mut handles = Vec::with_capacity(ids.len());
        for id in ids {
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
}
