# argus-sdks

Rust client SDKs for [Argus](https://github.com/the-sal/argus-sdks) market-data dispatchers —
lightweight TCP servers that proxy exchange APIs (Hyperliquid, Lighter, ...) over a persistent,
correlation-ID-matched wire protocol.

```
Your code  ←→  argus-* crate  ←→  Argus dispatcher  ←→  Exchange
```

Each dispatcher gets its own crate with a clean, synchronous, fully-typed API. All of them share
the same transport layer, so connecting to, querying, and streaming from any dispatcher looks and
feels the same.

## Crates

| Crate | Description |
| --- | --- |
| [`argus-dispatcher-core`](argus-dispatcher-core) | Shared transport: the P1 (JSON request/response) and P2 (order book streaming) wire protocols, background I/O threads, correlation-ID request/response matching, and the shared order book map/event. Not used directly — every client crate depends on it. |
| [`argus-hyperliquid`](argus-hyperliquid) | Client for the Argus Hyperliquid perpetuals dispatcher. |
| [`argus-lighter`](argus-lighter) | Client for the Argus Lighter (zkLighter) perpetuals dispatcher. |

See each crate's README for its full API reference and usage example.

## Status

Argus is early-stage ("Phase 1.0 of v2"). Dispatchers currently expose only read-only,
unauthenticated market-data actions — no trading or account queries yet. Each client crate's
coverage mirrors its dispatcher's routing table exactly and grows alongside it.

Both dispatchers support live order book streaming: subscribe to symbols and the dispatcher
pushes Protocol 2 snapshots over the same TCP connection.

## Quick Start

Add the crate for the exchange you need:

```toml
[dependencies]
argus-hyperliquid = { git = "https://github.com/the-sal/argus-sdks" }
argus-lighter = { git = "https://github.com/the-sal/argus-sdks" }
```

```rust
use argus_hyperliquid::HyperliquidClient;

fn main() {
    let client = HyperliquidClient::connect("localhost:9972");
    let top_funding = client.get_funding_rates_for_all_perpetuals(0, Some(10)).unwrap();
    for perp in &top_funding {
        println!("{:<12} mark={:<12.4}", perp.asset.name, perp.mark_price());
    }
}
```

## Streaming

The API is identical across the exchange crates (and mirrors the Polymarket SDK): `subscribe`
returns once the dispatcher confirms, order books land in a shared map, and an event wakes your
loop on every update.

```rust
use argus_hyperliquid::{HyperliquidClient, Listener};

fn main() {
    let client = HyperliquidClient::connect("localhost:9972");

    let subscription = client.subscribe(&["BTC", "ETH"]).unwrap();
    assert!(subscription.failed.is_empty());

    let books = client.get_order_book();        // Arc<RwLock<HashMap<String, OrderBook>>>
    let event = client.get_order_book_event();  // Arc<Event>

    loop {
        let listener = event.listen();               // register before reading
        {
            let snapshot = books.read().unwrap();
            if let Some(book) = snapshot.get("BTC") {
                let best_bid = book.bids.iter().find(|o| o.quantity > 0.0);
                let best_ask = book.asks.iter().find(|o| o.quantity > 0.0);
                println!("BTC bid={:?} ask={:?}", best_bid, best_ask);
            }
        }
        for message in client.get_pushed_messages().write().unwrap().drain() {
            eprintln!("dispatcher push: {} {:?}", message.action, message.error);
        }
        listener.wait();
    }
}
```

Register the listener before reading the map, and remember the event fires for every subscribed
symbol — re-check your symbol after each wakeup. Levels the exchange did not send are zero-padded
to the connection's configured depth (10 by default), so filter with `quantity > 0.0`.

## Development

This is a Cargo workspace; the usual commands work from the repo root:

```sh
cargo build
cargo test
```
