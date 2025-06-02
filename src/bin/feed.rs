use anyhow::Result;
use chrono::Utc;
use futures::StreamExt;
use polymarket_data_ingestor::client;
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use zstd::stream::write::Encoder;

/// Handles feed events with automatic hourly file rotation
struct FeedHandler {
    data_dir: PathBuf,
    current_dir: PathBuf,
    current_writer: Option<Encoder<'static, BufWriter<File>>>,
    current_hour_timestamp: u64,

    msg_count: u64,
    bytes_count: u64,
    last_sample_time: Instant,

    have_all_connections_opened: bool,
    n_connections_open: usize,
    n_connections_total: usize,
}

impl FeedHandler {
    fn new<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let current_dir = data_dir.join("current");
        std::fs::create_dir_all(&data_dir)?;
        std::fs::create_dir_all(&current_dir)?;

        let current_hour_timestamp = Self::get_current_hour_timestamp();
        let filepath = current_dir.join("log.jsonl.zst");

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&filepath)?;
        let buf_writer = BufWriter::new(file);
        let encoder = Encoder::new(buf_writer, 3)?;

        tracing::info!(
            filepath = %filepath.display(),
            "initialized current log file"
        );

        Ok(Self {
            data_dir,
            current_dir,
            current_writer: Some(encoder),
            current_hour_timestamp,
            msg_count: 0,
            bytes_count: 0,
            last_sample_time: Instant::now(),
            have_all_connections_opened: false,
            n_connections_open: 0,
            n_connections_total: 0,
        })
    }

    fn get_current_hour_timestamp() -> u64 {
        let now = Utc::now();
        let hour_start = now.timestamp() - (now.timestamp() % 3600);
        hour_start as u64
    }

    fn get_current_hour_filename(timestamp: u64) -> String {
        let dt = chrono::DateTime::from_timestamp(timestamp as i64, 0).unwrap();
        dt.format("%Y-%m-%d-%H").to_string()
    }

    fn ensure_current_file(&mut self) -> Result<()> {
        let current_hour = Self::get_current_hour_timestamp();

        // If we need to rotate to a new file
        if current_hour != self.current_hour_timestamp {
            // Close current writer if exists
            if let Some(writer) = self.current_writer.take() {
                writer.finish()?;

                // Move the current file to timestamped filename
                let old_filename = Self::get_current_hour_filename(self.current_hour_timestamp);
                let old_filepath = self.data_dir.join(format!("{}.jsonl.zst", old_filename));
                let current_filepath = self.current_dir.join("log.jsonl.zst");

                if current_filepath.exists() {
                    std::fs::rename(&current_filepath, &old_filepath)?;
                    tracing::info!(
                        old_filepath = %old_filepath.display(),
                        "rotated log file for upload"
                    );
                }
            }

            // Create new current file
            let filepath = self.current_dir.join("log.jsonl.zst");
            let file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&filepath)?;
            let buf_writer = BufWriter::new(file);
            let encoder = Encoder::new(buf_writer, 3)?;

            self.current_writer = Some(encoder);
            self.current_hour_timestamp = current_hour;

            tracing::info!(
                filepath = %filepath.display(),
                "created new current log file"
            );
        }

        Ok(())
    }

    fn handle_message(&mut self, msg: String) -> Result<()> {
        self.ensure_current_file()?;

        if let Some(ref mut writer) = self.current_writer {
            self.msg_count += 1;
            self.bytes_count += msg.len() as u64;

            writer.write_all(msg.as_bytes())?;
            if !msg.is_empty() && !msg.ends_with('\n') {
                writer.write_all(b"\n")?;
            }

            // Periodic flush for reliability
            if self.msg_count % 100 == 0 {
                writer.flush()?;
            }

            if self.last_sample_time.elapsed() > Duration::from_secs(15) {
                self.log_metrics()?;
            }
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
        if let Some(ref mut writer) = self.current_writer {
            writer.flush()?;
        }
        Ok(())
    }

    fn shutdown(&mut self) -> Result<()> {
        if let Some(writer) = self.current_writer.take() {
            writer.finish()?;
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Set up logging
    tracing_subscriber::fmt()
        .with_env_filter("polymarket_data_ingestor=debug,feed=debug")
        .init();

    // Set up feed handler with the correct data directory
    let base_path = PathBuf::from("./data");
    let mut handler = FeedHandler::new(base_path)?;

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

    // Set up signal handlers for both SIGINT and SIGTERM
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;

    // Process events from the stream until a stop signal is received
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
            // Handle SIGINT (Ctrl+C)
            _ = sigint.recv() => {
                tracing::info!("received SIGINT, shutting down gracefully...");
                cancel.cancel();
                break;
            }
            // Handle SIGTERM (systemd stop)
            _ = sigterm.recv() => {
                tracing::info!("received SIGTERM, shutting down gracefully...");
                cancel.cancel();
                break;
            }
        }
    }

    tracing::info!("shutting down, flushing remaining data...");
    handler.shutdown()?;

    // Wait for the client to finish its shutdown sequence
    tracing::info!("waiting for client to finish...");
    client_handle.await?;
    tracing::info!("shutdown complete");

    Ok(())
}
