# argus-lighter

A Rust client library for the Argus Lighter perpetuals dispatcher — a market-data server that
proxies Lighter's (zkLighter) public REST API over a persistent TCP connection.

## Overview

`argus-lighter` sits between your code and the Argus dispatcher process:

```
Your code  ←→  argus-lighter  ←→  Argus Lighter dispatcher  ←→  Lighter
```

This crate handles the P1 (JSON request/response) and P2 (order book streaming) TCP wire
protocols and background I/O threads, and exposes a clean synchronous, fully-typed API. It's
built on [`argus-dispatcher-core`](../argus-dispatcher-core), the transport layer shared with
[`argus-hyperliquid`](../argus-hyperliquid).

The Lighter dispatcher is early-stage (Argus "Phase 1.0 of v2") and currently exposes read-only,
unauthenticated market-data actions plus order book streaming — no trading or account queries
yet. This crate's coverage mirrors the dispatcher's routing table exactly and will grow alongside
it.

Unlike Hyperliquid, Lighter has no HIP-3-style builder-deployed dexes — it's a single unified
exchange with one flat market list, so there's no `dex_name` parameter anywhere in this crate.

## Quick Start

```toml
[dependencies]
argus-lighter = { git = "https://github.com/the-sal/argus-sdks" }
```

```rust
use argus_lighter::LighterClient;

fn main() {
    // Connect to the Lighter dispatcher (default port 9974).
    let client = LighterClient::connect("localhost:9974");

    let version = client.products_version().unwrap();
    println!("lighter_dispatcher version: {:?}", version.lighter_dispatcher);

    // Top 10 markets by funding rate.
    let top_funding = client.get_funding_rates_for_all_perpetuals(0, Some(10)).unwrap();
    for perp in &top_funding {
        println!(
            "{:<12} mark={:<12.4} funding/hr={}",
            perp.market.symbol,
            perp.mark_price(),
            perp.funding_rate.map(|r| format!("{:.4}%", r * 100.0)).unwrap_or_else(|| "n/a".to_string()),
        );
    }
}
```

## API Reference

### `LighterClient`

```rust
let client = LighterClient::connect("localhost:9974"); // blocks until connected
```

- `products_version() -> Result<ProductsVersion, String>` — dispatcher/component version info.
- `get_markets(offset: u32, limit: Option<u32>) -> Result<Vec<Perpetual>, String>` — a page of
  all perpetual markets. `limit` defaults to 10.
- `get_funding_rates_for_all_perpetuals(offset: u32, limit: Option<u32>) -> Result<Vec<Perpetual>, String>` —
  a page of markets sorted by funding rate descending. `limit` defaults to 20.
- `market_info(symbol: Option<&str>, market_id: Option<i64>) -> Result<Option<Perpetual>, String>` —
  metadata + live data for one market. Exactly one of `symbol` / `market_id` should be `Some`
  (if both are, `symbol` wins). Returns `Ok(None)` if no market matches.
- `get_funding_history(market_id: i64, start_timestamp: i64, end_timestamp: Option<i64>, resolution: Option<&str>) -> Result<Vec<FundingHistoryEntry>, String>` —
  historical funding for one market. `start_timestamp`/`end_timestamp` are unix seconds;
  `resolution` defaults to `"1h"`.
- `subscribe(symbols: &[&str]) -> Result<SubscriptionResponse, String>` — subscribes to streaming
  order books for one or more markets. Unknown symbols land in `failed` without failing the
  request.
- `unsubscribe(symbols: &[&str]) -> Result<UnsubscriptionResponse, String>` — stops streaming for
  the given markets.
- `get_order_book() -> Arc<RwLock<HashMap<String, OrderBook>>>` — live order book map, keyed by
  market symbol, updated in place by each Protocol 2 packet.
- `get_order_book_event() -> Arc<Event>` — event notified on every Protocol 2 packet.
- `get_pushed_messages() -> Arc<RwLock<PushedMessages>>` / `get_push_event() -> Arc<Event>` —
  unsolicited Protocol 1 pushes from the dispatcher (notifications, fatal errors).

## Streaming

Subscriptions are per-connection and take a batch of Lighter market symbols (`"BTC"`, `"ETH"`).
The dispatcher then pushes Protocol 2 order book snapshots over the same TCP connection, which
the background thread applies to a shared map and announces via an [`Event`].

```rust
use argus_lighter::{LighterClient, Listener};

fn main() {
    let client = LighterClient::connect("localhost:9974");

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
- The event fires for **every** subscribed symbol, not just the one you care about — re-check your
  symbol in the map after each wakeup.
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
    pub market: Market,           // fees, size limits, margin tiers
    pub context: MarketContext,    // live market data (mark_price, open_interest, ...)
    pub funding_rate: Option<f64>, // None if no rate is currently attached
}

perp.mark_price();        // -> f64
perp.open_interest_usd(); // -> f64
perp.funding_rate_apr();  // -> Option<f64>, naive annualization assuming hourly settlement
```

## Extras

**`FundingHistoryEntry.market_id`** — Lighter's `/fundings` response doesn't echo the market ID
back per-entry, so it isn't part of the wire payload; each `FundingHistoryEntry` you get back is
implicitly for the `market_id` you passed to `get_funding_history`.

**Funding rate semantics** — `Perpetual.funding_rate` (from `get_markets` /
`get_funding_rates_for_all_perpetuals`) is a live/current snapshot used for cross-exchange
comparison. `FundingHistoryEntry.rate` (from `get_funding_history`) is Lighter's own
realized/settled rate for that hour. The two are not guaranteed to match in magnitude for the
same market at the same time — treat them as separate quantities.

**Correlation IDs** — every request carries a UUID correlation ID; the client blocks until a
matching response arrives or the timeout (default 10s) elapses, making the API synchronous from
the caller's perspective — identical to how `argus-polymarket` and `argus-hyperliquid` behave.

**Coverage** — this dispatcher does not yet implement trading, account balance, or position
queries; neither does this crate. When the dispatcher adds them, this crate will follow.
