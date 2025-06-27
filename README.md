# Polymarket Data Ingestor

Code for streaming, recording, and replaying Polymarket order book data.

## Overview

Three Rust binaries, each handling a different part of the data pipeline.

- `./collector` Core data collection service. Discovers live markets, manages WebSocket connections, streams order book data, logs raw messages with timestamps to compressed files, rotates logs hourly.

- `./deploy` GCP automation. Creates compute instances, sets up storage buckets, configures systemd services and cron jobs, handles code deployment, manages automatic log uploads.

- `./cli` Historical data tools. Downloads from GCS, replays raw messages to reconstruct order books, generates tick data (trades and BBO updates) as CSV.

## Running locally

```bash
# Stream real-time data
cargo run --bin collector

# Download historical data
cargo run --bin cli -- download --since 24h

# Generate tick data in CSV format
cargo run --bin cli -- replay --since 12h
```

## Deploying to GCP

```bash
# Setup
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
cargo run --bin deploy -- create

# Deploy updates
cargo run --bin deploy -- update

# Print recent logs from the container, show SSH command
cargo run --bin deploy -- status
```

## Testing

```bash
cargo test -- --ignored
```

## License

MIT