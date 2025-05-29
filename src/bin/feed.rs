use anyhow::Result;
use futures::StreamExt;
use polymarket_data_ingestor::client;
use std::{
    fs::File,
    io::{BufWriter, Write},
    time::Duration,
};
use tokio::signal;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Handles feed events, writing messages to a file and tracking metrics
struct FeedHandler {
    writer: BufWriter<File>,
    msg_count: u64,
    bytes_count: u64,
    last_sample_time: Instant,
    have_all_connections_opened: bool,
    n_connections_open: usize,
    n_connections_total: usize,
}

impl FeedHandler {
    fn new(file: File) -> Self {
        Self {
            writer: BufWriter::new(file),
            msg_count: 0,
            bytes_count: 0,
            last_sample_time: Instant::now(),
            have_all_connections_opened: false,
            n_connections_open: 0,
            n_connections_total: 0,
        }
    }

    fn handle_message(&mut self, msg: String) -> Result<()> {
        self.msg_count += 1;
        self.bytes_count += msg.len() as u64;

        self.writer.write_all(msg.as_bytes())?;
        if !msg.is_empty() && !msg.ends_with('\n') {
            self.writer.write_all(b"\n")?;
        }

        if self.last_sample_time.elapsed() > Duration::from_secs(15) {
            self.log_metrics()?;
        }

        Ok(())
    }

    fn handle_connection_opened(&mut self, n_open: usize, n_connections: usize) -> Result<()> {
        self.n_connections_open = n_open;
        self.n_connections_total = n_connections;
        if n_open == n_connections && !self.have_all_connections_opened {
            self.have_all_connections_opened = true;
            tracing::info!(connection_count = n_connections, "all connections opened");
        }
        Ok(())
    }

    fn handle_connection_closed(&mut self, n_open: usize, n_connections: usize) -> Result<()> {
        self.n_connections_open = n_open;
        self.n_connections_total = n_connections;
        Ok(())
    }

    fn log_metrics(&mut self) -> Result<()> {
        tracing::info!(
            messages_per_sec = self.msg_count / 15,
            bytes_per_sec = self.bytes_count / 15,
            open_connections = self.n_connections_open,
            total_connections = self.n_connections_total,
            "feed metrics"
        );
        self.msg_count = 0;
        self.bytes_count = 0;
        self.last_sample_time = Instant::now();
        self.writer.flush()?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Set up logging
    tracing_subscriber::fmt()
        .with_env_filter("polymarket_data_ingestor=debug,feed=debug")
        .init();

    // Create client to fetch markets from API
    let cancel = CancellationToken::new();
    let client = client::PolymarketClient::new(cancel.clone());

    // Fetch active markets first
    let markets = client.fetch_active_markets().await?;
    tracing::info!(
        market_count = markets.len(),
        "found active markets, connecting..."
    );

    // Get stream of events for these markets
    let (mut stream, client_handle) = client.into_stream(markets).await?;

    // Set up feed handler
    let file = File::options()
        .write(true)
        .create(true)
        .truncate(true)
        .open("feed.log")?;
    let mut handler = FeedHandler::new(file);

    // Process events from the stream until Ctrl+C is received
    loop {
        tokio::select! {
            event = stream.next() => {
                match event {
                    Some(client::FeedEvent::FeedMessage(msg)) => handler.handle_message(msg)?,
                    Some(client::FeedEvent::ConnectionOpened(_id, n_open, n_connections)) => {
                        handler.handle_connection_opened(n_open, n_connections)?
                    }
                    Some(client::FeedEvent::ConnectionClosed(_id, n_open, n_connections)) => {
                        handler.handle_connection_closed(n_open, n_connections)?
                    }
                    None => {
                        tracing::info!("stream ended");
                        break;
                    }
                }
            }
            _ = signal::ctrl_c() => {
                tracing::info!("received Ctrl+C, shutting down gracefully...");
                cancel.cancel();
            }
        }
    }

    tracing::info!("shutting down, flushing remaining data...");
    handler.flush()?;

    // Wait for the client to finish its shutdown sequence
    tracing::info!("waiting for client to finish...");
    client_handle.await?;
    tracing::info!("shutdown complete");

    Ok(())
}
