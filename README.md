# Polymarket Data Ingestor

A library for streaming Polymarket's book data.

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

## License

MIT