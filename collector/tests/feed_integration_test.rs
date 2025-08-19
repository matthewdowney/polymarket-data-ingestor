use anyhow::Result;
use data_collector::client;
use futures::StreamExt;
use std::time::Duration;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
#[ignore]
async fn test_feed_connections() -> Result<()> {
    // Set up logging
    tracing_subscriber::fmt()
        .with_env_filter("polymarket_data_ingestor=debug,feed=debug")
        .init();

    // Create client and fetch markets
    let cancel = CancellationToken::new();
    let client = client::PolymarketClient::new(cancel.clone());

    // Fetch active markets and take first 20
    let markets = client.fetch_active_markets().await?;
    let test_markets: Vec<_> = markets.into_iter().take(20).collect();
    let n_test_markets = test_markets.len();
    tracing::info!(market_count = n_test_markets, "testing with markets");

    // Get stream of events
    let (mut stream, client_handle) = client.into_stream(test_markets).await?;

    // Track connection state
    let mut n_connections_open = 0;
    let mut n_connections_total = 0;
    let mut have_all_connections_opened = false;

    // Process events with timeout
    let _result = timeout(CONNECTION_TIMEOUT, async {
        while let Some(event) = stream.next().await {
            match event {
                client::FeedEvent::FeedMessage(_) => {
                    // Ignore feed messages for this test
                }
                client::FeedEvent::ConnectionOpened(_id, n_open, n_connections) => {
                    n_connections_open = n_open;
                    n_connections_total = n_connections;
                    if n_open == n_connections {
                        have_all_connections_opened = true;
                        tracing::info!(connection_count = n_connections, "all connections opened");
                        break;
                    }
                }
                client::FeedEvent::ConnectionClosed(_id, n_open, n_connections) => {
                    n_connections_open = n_open;
                    n_connections_total = n_connections;
                }
            }
        }
    })
    .await;

    // Verify all connections were opened
    assert!(
        have_all_connections_opened,
        "Failed to open all connections within timeout. Open: {}/{}, Total: {}",
        n_connections_open, n_connections_total, n_test_markets
    );

    // Cancel the client and verify clean shutdown
    cancel.cancel();
    let shutdown_result = timeout(SHUTDOWN_TIMEOUT, async {
        while let Some(event) = stream.next().await {
            if let client::FeedEvent::ConnectionClosed(_, n_open, _) = event {
                if n_open == 0 {
                    break;
                }
            }
        }
    })
    .await;

    let result = timeout(SHUTDOWN_TIMEOUT, client_handle).await?;
    assert!(
        result.is_ok(),
        "client handle did not shut down within timeout"
    );

    assert!(
        shutdown_result.is_ok(),
        "Failed to shut down cleanly within timeout"
    );

    Ok(())
}
