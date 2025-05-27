use crate::client::MAX_PARALLELISM;
use crate::client::connection::{Connection, ConnectionEvent, ConnectionId};
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex, mpsc};

/// Accepts (re)connection requests at any pace and executes them in parallel
/// with error handling, retries, and backoff. Aggregates all events from open
/// connections into a single channel.
///
/// # Example
///
/// ```rust,no_run
///
/// // Channel to aggregate events from all connections
/// let (event_tx, _event_rx) = mpsc::unbounded_channel::<ConnectionEvent>();
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
    pub tx: mpsc::UnboundedSender<ConnectionId>,
    rx: mpsc::UnboundedReceiver<ConnectionId>,
    connections: HashMap<ConnectionId, Arc<Mutex<Connection>>>,
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
            .map(|(k, v)| (k, Arc::new(Mutex::new(v))))
            .collect();
        Self {
            tx,
            rx,
            connections,
            event_tx,
        }
    }

    /// Monitor connection requests, opening connections in batches and forwarding
    /// messages to [`Self::event_tx`]
    pub async fn run(&mut self) {
        let mut error_count: u64 = 0;
        loop {
            self.backoff(error_count).await;
            // Take N connection ids at once to open
            if let Some(ids) = self.recv_n(MAX_PARALLELISM).await {
                let n = ids.len();
                let n_errors = self.open_all(ids).await;
                error_count = match n_errors {
                    0 => 0, // reset if no errors, increment only if majority failed
                    _ if n_errors < n / 2 => error_count,
                    _ => error_count + 1,
                };
                tracing::debug!("{}/{} failed; error_count={}", n_errors, n, error_count);
            } else {
                break; // tx is closed
            }
        }

        tracing::info!("reconnecter shut down");
    }

    /// Open a [`Connection`] and send a connection closed event if it fails.
    async fn connect(
        connection: &mut Connection,
        event_tx: mpsc::UnboundedSender<ConnectionEvent>,
    ) -> Result<ConnectionId, (ConnectionId, anyhow::Error)> {
        tracing::info!("opening connection {:?}", connection.id);
        if let Err(e) = connection.connect().await {
            event_tx
                .send(ConnectionEvent::ConnectionClosed(connection.id.clone()))
                .map_err(|e| {
                    (
                        connection.id.clone(),
                        anyhow::anyhow!("failed to send connection closed event: {}", e),
                    )
                })?;
            Err((connection.id.clone(), e))
        } else {
            Ok(connection.id.clone())
        }
    }

    /// Await the next available connection id, and then try to read up
    /// to `n-1` more without blocking.
    async fn recv_n(&mut self, n: usize) -> Option<Vec<ConnectionId>> {
        let fst = self.rx.recv().await?;

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
                handles.push(tokio::spawn(async move {
                    let mut conn = conn.lock().await;
                    Self::connect(&mut conn, event_tx).await
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
