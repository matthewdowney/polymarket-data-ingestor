use anyhow::Result;
use chrono::Utc;
use data_collector::{client, PolymarketMarket};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
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

/// A framed message with timestamp and type information
#[derive(Debug, Serialize, Deserialize)]
struct LoggedMessage {
    timestamp: String,
    message_type: String,
    content: serde_json::Value,
}

impl LoggedMessage {
    fn new_feed_message(raw_json: &str) -> Self {
        Self {
            timestamp: Utc::now().to_rfc3339(),
            message_type: "feed_message".to_string(),
            content: serde_json::Value::String(raw_json.to_string()),
        }
    }

    fn new_active_markets(markets: &[PolymarketMarket]) -> Self {
        Self {
            timestamp: Utc::now().to_rfc3339(),
            message_type: "active_markets".to_string(),
            content: serde_json::json!({
                "markets": markets,
                "count": markets.len()
            }),
        }
    }

    fn new_shutdown_initiated(signal: &str) -> Self {
        Self {
            timestamp: Utc::now().to_rfc3339(),
            message_type: "shutdown_initiated".to_string(),
            content: serde_json::json!({
                "signal": signal
            }),
        }
    }

    fn new_all_connections_ready(connection_count: usize, markets_count: usize) -> Self {
        Self {
            timestamp: Utc::now().to_rfc3339(),
            message_type: "all_connections_ready".to_string(),
            content: serde_json::json!({
                "connection_count": connection_count,
                "markets_count": markets_count
            }),
        }
    }

    fn to_json_line(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }
}

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

    active_markets: Vec<PolymarketMarket>,
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
            active_markets: Vec::new(),
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

            // Emit market data as first line after rotation
            self.emit_market_data()?;
        }

        Ok(())
    }

    fn handle_message(&mut self, msg: String) -> Result<()> {
        self.ensure_current_file()?;

        if let Some(ref mut writer) = self.current_writer {
            // Wrap the raw message in our frame format
            let logged_msg = LoggedMessage::new_feed_message(&msg);
            let json_line = logged_msg.to_json_line()?;

            self.msg_count += 1;
            self.bytes_count += json_line.len() as u64;

            writer.write_all(json_line.as_bytes())?;
            writer.write_all(b"\n")?;

            // Periodic flush for reliability
            if self.msg_count.is_multiple_of(100) {
                writer.flush()?;
            }

            if self.last_sample_time.elapsed() > Duration::from_secs(15) {
                self.log_metrics()?;
            }
        }

        Ok(())
    }

    fn set_active_markets(&mut self, markets: Vec<PolymarketMarket>) -> Result<()> {
        self.active_markets = markets;
        self.emit_market_data()
    }

    fn emit_market_data(&mut self) -> Result<()> {
        if !self.active_markets.is_empty() {
            if let Some(ref mut writer) = self.current_writer {
                let logged_msg = LoggedMessage::new_active_markets(&self.active_markets);
                let json_line = logged_msg.to_json_line()?;

                writer.write_all(json_line.as_bytes())?;
                writer.write_all(b"\n")?;
                writer.flush()?;
            }
        }
        Ok(())
    }

    fn log_shutdown_initiated(&mut self, signal: &str) -> Result<()> {
        if let Some(ref mut writer) = self.current_writer {
            let logged_msg = LoggedMessage::new_shutdown_initiated(signal);
            let json_line = logged_msg.to_json_line()?;

            writer.write_all(json_line.as_bytes())?;
            writer.write_all(b"\n")?;
            writer.flush()?;
        }
        Ok(())
    }

    fn log_all_connections_ready(&mut self, connection_count: usize) -> Result<()> {
        if let Some(ref mut writer) = self.current_writer {
            let logged_msg = LoggedMessage::new_all_connections_ready(
                connection_count,
                self.active_markets.len(),
            );
            let json_line = logged_msg.to_json_line()?;

            writer.write_all(json_line.as_bytes())?;
            writer.write_all(b"\n")?;
            writer.flush()?;
        }
        Ok(())
    }

    fn handle_connection_opened(&mut self, n_open: usize, n_connections: usize) -> Result<()> {
        self.n_connections_open = n_open;
        self.n_connections_total = n_connections;
        if n_open == n_connections && !self.have_all_connections_opened {
            self.have_all_connections_opened = true;
            tracing::info!(connection_count = n_connections, "all connections opened");
            self.log_all_connections_ready(n_connections)?;
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
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "collector=debug,data_collector=debug".to_string()),
        )
        .init();

    // Log startup info via tracing
    tracing::info!("Polymarket Data Collector starting up...");
    tracing::info!("Working directory: {:?}", std::env::current_dir()?);
    tracing::info!(
        "RUST_LOG: {:?}",
        std::env::var("RUST_LOG").unwrap_or_else(|_| "not set".to_string())
    );

    // Set up feed handler with the correct data directory
    let base_path = PathBuf::from("./data");
    tracing::info!(
        "Data directory: {:?}",
        base_path
            .canonicalize()
            .unwrap_or_else(|_| base_path.clone())
    );
    let mut handler = FeedHandler::new(base_path)?;

    // Create client to fetch markets from API
    let cancel = CancellationToken::new();
    let client = client::PolymarketClient::new(cancel.clone());

    // Fetch markets

    // let markets = client.fetch_sampling_markets().await?;
    // tracing::info!(
    //     market_count = markets.len(),
    //     "found sampling markets, connecting..."
    // );

    let markets = client.fetch_active_markets().await?;
    tracing::info!(
        market_count = markets.len(),
        "found active markets, connecting..."
    );

    // Store market data in handler and emit as first log line
    handler.set_active_markets(markets.clone())?;

    // Get stream of events for these markets
    let (mut stream, client_handle) = client.into_stream(markets).await?;

    // Set up signal handlers for SIGINT, SIGTERM, and SIGHUP
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sighup = signal(SignalKind::hangup())?;

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
                handler.log_shutdown_initiated("SIGINT")?;
                cancel.cancel();
                break;
            }
            // Handle SIGTERM (systemd stop)
            _ = sigterm.recv() => {
                tracing::info!("received SIGTERM, shutting down gracefully...");
                handler.log_shutdown_initiated("SIGTERM")?;
                cancel.cancel();
                break;
            }
            // Handle SIGHUP (hangup)
            _ = sighup.recv() => {
                tracing::info!("received SIGHUP, shutting down gracefully...");
                handler.log_shutdown_initiated("SIGHUP")?;
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
