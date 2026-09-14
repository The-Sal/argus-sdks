# argus-hyperliquid

A Rust client library for the Argus Hyperliquid perpetuals dispatcher — a market-data server
that proxies Hyperliquid's public `/info` API over a persistent TCP connection.

## Overview

`argus-hyperliquid` sits between your code and the Argus dispatcher process:

```
Your code  ←→  argus-hyperliquid  ←→  Argus Hyperliquid dispatcher  ←→  Hyperliquid
```

This crate handles the P1 (JSON request/response) and P2 (order book streaming) TCP wire
protocols and background I/O threads, and exposes a clean synchronous, fully-typed API. It's
built on [`argus-dispatcher-core`](../argus-dispatcher-core), the transport layer shared with
[`argus-lighter`](../argus-lighter).

The Hyperliquid dispatcher is early-stage (Argus "Phase 1.0 of v2") and currently exposes
read-only market-data actions plus order book streaming — no trading or account queries yet.
This crate's coverage mirrors the dispatcher's routing table exactly and will grow alongside it.

## Quick Start

```toml
[dependencies]
argus-hyperliquid = { git = "https://github.com/the-sal/argus-sdks" }
```

```rust
use argus_hyperliquid::HyperliquidClient;

fn main() {
    // Connect to the Hyperliquid dispatcher (default port 9972).
    let client = HyperliquidClient::connect("localhost:9972");

    let version = client.products_version().unwrap();
    println!("hyperliquid_dispatcher version: {:?}", version.hyperliquid_dispatcher);

    // Top 10 perpetuals on the default dex by funding rate.
    let top_funding = client.get_funding_rates_for_all_perpetuals(0, Some(10)).unwrap();
    for perp in &top_funding {
        println!(
            "{:<12} mark={:<12.4} funding/hr={:.4}%",
            perp.asset.name,
            perp.mark_price(),
            perp.funding_rate() * 100.0,
        );
    }
}
```

## API Reference

### `HyperliquidClient`

```rust
let client = HyperliquidClient::connect("localhost:9972"); // blocks until connected
```

- `products_version() -> Result<ProductsVersion, String>` — dispatcher/component version info.
- `get_dexs() -> Result<Vec<Dex>, String>` — lists HIP-3 (builder-deployed) perp dexes. The
  default dex (`""`) is not included.
- `get_perpetuals_for_dex(dex_name: &str, offset: u32, limit: Option<u32>) -> Result<Vec<Perpetual>, String>` —
  a page of perpetuals for a dex. Pass `""` for the default dex.
- `get_funding_rates_for_all_perpetuals(offset: u32, limit: Option<u32>) -> Result<Vec<Perpetual>, String>` —
  a page of perpetuals across all dexes, sorted by funding rate descending. `limit` defaults to 20.
- `perpetual_info(coin: &str) -> Result<PerpetualInfo, String>` — annotation/category/keywords/
  predicted-funding metadata for one coin (no live market data — use the methods above for that).
- `subscribe(coins: &[&str]) -> Result<SubscriptionResponse, String>` — subscribes to streaming
  order books for one or more coins. Unknown coins land in `failed` without failing the request.
- `unsubscribe(coins: &[&str]) -> Result<UnsubscriptionResponse, String>` — stops streaming for
  the given coins.
- `get_order_book() -> Arc<RwLock<HashMap<String, OrderBook>>>` — live order book map, keyed by
  coin, updated in place by each Protocol 2 packet.
- `get_order_book_event() -> Arc<Event>` — event notified on every Protocol 2 packet.
- `get_pushed_messages() -> Arc<RwLock<PushedMessages>>` / `get_push_event() -> Arc<Event>` —
  unsolicited Protocol 1 pushes from the dispatcher (notifications, fatal errors).

## Streaming

Subscriptions are per-connection and take a batch of Hyperliquid coin names (`"BTC"`, `"ETH"`,
or HIP-3 assets like `"xyz:AAPL"`). The dispatcher then pushes Protocol 2 order book snapshots
over the same TCP connection, which the background thread applies to a shared map and announces
via an [`Event`].

```rust
use argus_hyperliquid::{HyperliquidClient, Listener};

fn main() {
    let client = HyperliquidClient::connect("localhost:9972");

    let subscription = client.subscribe(&["BTC", "ETH"]).unwrap();
    assert!(subscription.failed.is_empty(), "failed: {:?}", subscription.failed);
    println!("subscribed: {:?}", subscription.subscribed);

    let books = client.get_order_book();
    let event = client.get_order_book_event();

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
        listener.wait();                             // block until the next update
    }
}
```

Notes:

- Register the listener **before** reading the map, otherwise an update arriving between the read
  and the wait is missed (the listener only wakes on notifications that happen after it is
  registered).
- The event fires for **every** subscribed coin, not just the one you care about — re-check your
  coin in the map after each wakeup.
- Missing levels are zero-padded to the dispatcher's configured depth (10 by default), so filter
  with `quantity > 0.0` when iterating a side.
- On unsubscribe, entries already in the map are left in place (stale) rather than removed.
- `OrderBook.remote_timestamp` is the exchange timestamp in milliseconds; `argus_timestamp` is
  the Argus server timestamp in fractional Unix seconds.

### `OrderBook`

```rust
pub struct Order {
    pub price: f64,
    pub quantity: f64,
}

pub struct OrderBook {
    pub symbol: String,
    pub bids: Vec<Order>,        // best-first, zero-padded to depth
    pub asks: Vec<Order>,        // best-first, zero-padded to depth
    pub remote_timestamp: f64,   // exchange time, ms
    pub argus_timestamp: f64,    // Argus time, fractional seconds
}

book.print_orderbook();          // formatted side-by-side dump to stdout
```

### `Perpetual`

```rust
pub struct Perpetual {
    pub dex: String,       // "" for the default dex, or a HIP-3 dex name
    pub asset: Asset,       // static metadata (name, szDecimals, maxLeverage, ...)
    pub context: AssetContext, // live market data (markPx, funding, openInterest, ...)
}

perp.mark_price();        // -> f64
perp.funding_rate();      // -> f64, hourly
perp.funding_rate_apr();  // -> f64, naive annualization of the current hourly rate
perp.open_interest_usd(); // -> f64
```

## Extras

**Dexes** — Hyperliquid's own order book is the "default dex" (`dex_name = ""`). HIP-3 lets
third parties deploy additional perp dexes with their own asset universes; [`get_dexs`] lists
those (not the default one).

**Correlation IDs** — every request carries a UUID correlation ID; [`request`] blocks until a
matching response arrives or the timeout (default 10s) elapses, making the API synchronous from
the caller's perspective — identical to how `argus-polymarket` and `argus-lighter` behave.

**Coverage** — this dispatcher does not yet implement trading, account balance, or position
queries; neither does this crate. When the dispatcher adds them, this crate will follow.
