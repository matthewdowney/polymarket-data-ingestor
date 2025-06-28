# Polymarket Data Ingestor

Code for streaming, recording, and replaying Polymarket order book data.

## Overview

Three Rust binaries, each handling a different part of the data pipeline.

- `./collector` Core data collection service. Discovers live markets, manages WebSocket connections, streams order book data, logs raw messages with timestamps to compressed files, rotates logs hourly.

- `./deploy` GCP automation. Creates compute instances, sets up storage buckets, configures systemd services and cron jobs, handles code deployment, manages automatic log uploads.

- `./cli` Historical data tools. Downloads from GCS, replays raw messages to reconstruct order books, generates tick data (trades and BBO updates) as CSV.

## Running locally

Requires [gcloud](https://cloud.google.com/sdk/docs/install) cli installed and authenticated.

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

## Research workflow

Download a day of data:

    cargo run --release --bin cli -- download -t 24h

Generate tick data from the feed logs:

    cargo run --release --bin cli -- replay -t 24h -o ticks.csv

Use the CLI to find market and asset ids:

    cargo run --release --bin cli -- markets "Will Iran close"

    Will Iran close the Strait of Hormuz in 2025?
        0x89ff77ee1c11d6c8a480bfaab11eefd6f87b8f2076a065be0706453857dc0958
        Yes         108468416668663017133298741485453125150952822149773262784582671647441799250111
        No          47757079633894387112291987083810225642258238114957712348556688720736895499502
    Will Iran close the Strait of Hormuz before July?
        0x0f4d9792bdf45a5fa5f717cbe55d78107878854816374b2dd741f46062aba2b0
        Yes         8717238053971684305673538480906460309461104157416047071886876622775788196994
        No          70227207630366498561631268042608657578076339758646274683548012561150704284965

Load ticks in a notebook (see [./notebooks/bbo.ipynb](./notebooks/bbo.ipynb)).

## Limitations

1. Download and replay is slow (~2 mins to replay 24h data on laptop). Fine for one-off event studies but not so great for time ranges > 1 week
2. Collector discovers new markets by restarting the process at intervals instead of monitoring and updating open connections
3. Deployment to GCP VM is a bit hacky, no alerting on container issues (only systemd restarts)
4. Would be nice to have a Grafana dashboard with the trailing ~90d of BBO and trade data

Plan to speed up research workflow: log both raw .jsonl.zst messages and processed tick data as Parquet files, store both in GCS, then use Dask, DuckDB, or similar for analysis.

To get a read-time dashboard: make VM bigger, use docker-compose to run ClickHouse and Grafana on the same instance, tick data writer also inserts to ClickHouse.

## License

MIT