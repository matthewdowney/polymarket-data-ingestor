use anyhow::{Context, Result};
use polymarket_data_ingestor::{MARKETS_FILE, PolymarketMarket, client};
use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    time::Duration,
};
use tokio::time::Instant;

// TODO: Add graceful shutdown
// TODO: Add running count of # of open and closed connections
// TODO: Should expose a queue instead of taking a callback
// TODO: Wrap together with parallel markets fetching beind a PolymarketFeed struct
// TODO: Get rid of unbounded channels
// TODO: Only show connection status messages once all connections have opened once, show progress reports until then
// TODO: Enum instead of string for message type
#[tokio::main]
async fn main() -> Result<()> {
    // Set up logging
    tracing_subscriber::fmt()
        .with_env_filter("polymarket_data_ingestor=debug")
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

    let mut msg_count = 0;
    let mut bytes_count = 0;
    let mut last_sample_time = Instant::now();
    let mut client = client::Client::new(Box::new(move |msg| {
        msg_count += 1;
        bytes_count += msg.len();
        writer.write_all(msg.as_bytes())?;

        if last_sample_time.elapsed() > Duration::from_secs(15) {
            tracing::info!(
                "{} messages/sec, {} bytes/sec",
                msg_count / 15,
                bytes_count / 15
            );
            msg_count = 0;
            bytes_count = 0;
            last_sample_time = Instant::now();
            writer.flush()?;
        }
        Ok(())
    }));

    client.run_forever(markets).await;
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
