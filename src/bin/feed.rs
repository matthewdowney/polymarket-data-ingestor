use anyhow::{Context, Result};
use polymarket_data_ingestor::{MARKETS_FILE, PolymarketMarket, client};
use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    time::Duration,
};
use tokio::{sync::mpsc, task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;

// TODO: Wrap together with parallel markets fetching beind a PolymarketFeed struct
#[tokio::main]
async fn main() -> Result<()> {
    // Set up logging
    tracing_subscriber::fmt()
        .with_env_filter("polymarket_data_ingestor=debug,feed=debug")
        .init();

    let markets: Vec<PolymarketMarket> = load_active_markets()?;
    println!("found {} active markets, connecting...", markets.len());

    // append to a logfile called feed.log, clearing it if it exists
    let file = File::options()
        .write(true)
        .create(true)
        .truncate(true)
        .open("feed.log")?;
    let mut writer = BufWriter::new(file);

    // all feed events go to this channel
    let (event_tx, mut event_rx) = mpsc::channel::<client::FeedEvent>(1000);

    // spawn a task to handle the events
    let event_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
        let mut msg_count = 0;
        let mut bytes_count = 0;
        let mut last_sample_time = Instant::now();
        let mut have_all_connections_opened = false;
        let mut n_connections_open = 0;
        let mut n_connections_total = 0;

        while let Some(event) = event_rx.recv().await {
            match event {
                client::FeedEvent::FeedMessage(msg) => {
                    msg_count += 1;
                    bytes_count += msg.len();
                    writer.write_all(msg.as_bytes())?;

                    if last_sample_time.elapsed() > Duration::from_secs(15) {
                        tracing::info!(
                            "{} messages/sec, {} bytes/sec, {}/{} connections open",
                            msg_count / 15,
                            bytes_count / 15,
                            n_connections_open,
                            n_connections_total
                        );
                        msg_count = 0;
                        bytes_count = 0;
                        last_sample_time = Instant::now();
                        writer.flush()?;
                    }
                }
                client::FeedEvent::ConnectionOpened(_id, n_open, n_connections) => {
                    n_connections_open = n_open;
                    n_connections_total = n_connections;
                    if n_open == n_connections && !have_all_connections_opened {
                        have_all_connections_opened = true;
                        tracing::info!("all {} connections opened", n_connections);
                    }
                }
                client::FeedEvent::ConnectionClosed(_id, n_open, n_connections) => {
                    n_connections_open = n_open;
                    n_connections_total = n_connections;
                }
            }
        }
        tracing::info!("event handler shut down");
        Ok(())
    });

    // start the feed
    let cancel = CancellationToken::new();
    let mut client = client::Client::new(cancel.clone());
    let client_handle = tokio::spawn(async move { client.run(markets, event_tx).await });

    // Wait for ctrl + c, then shut down the client feed
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down...");

    cancel.cancel();
    let _ = client_handle.await;
    let _ = event_handle.await;

    tracing::info!("client feed shut down successfully");

    Ok(())
}

/// Read the markets from disk and filter out inactive markets.
fn load_active_markets() -> Result<Vec<PolymarketMarket>> {
    let file = File::open(MARKETS_FILE).context("failed to open market data file")?;
    let reader = BufReader::new(file);

    let mut markets = Vec::new();
    for line in reader.lines() {
        let m: PolymarketMarket = serde_json::from_str(&line?).context("market parse failed")?;
        if m.is_active() {
            markets.push(m);
        }
    }

    Ok(markets)
}
