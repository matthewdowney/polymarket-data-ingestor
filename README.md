# Polymarket Data Ingestor

A library for streaming and recording Polymarket's order book data for all active markets.

Discovers all live markets and manages as many WebSocket connections as needed to stream book data for all of them to a single channel. Handles reconnects and backoff.

## Usage

See the crate documentation for complete examples and API reference.

## Examples

Run the included examples:

```bash
# Real-time order book feed for every live market
cargo run --bin feed
```

## Testing

To run integration tests which will fetch markets and spin up some connections:
```bash
cargo test -- --ignored
```


# Deploy 

To record data to a GCS bucket, the deploy.rs script:

1. Sets up one bucket and one VM
2. Configures the VM to run the feed binary
3. Periodically uploads feed logs to the bucket

To deploy, customize the bucket const in `src/bin/deploy.rs` and:

    # 1. Setup (once)
    gcloud auth login
    gcloud config set project YOUR_PROJECT_ID
    
    # Create instance and configure
    cargo run --bin deploy -- create

    # 2. Deploy updates
    cargo run --bin deploy -- update
    
    # 3. Check status
    cargo run --bin deploy -- status

## License

MIT