# argus-hyperliquid

A Rust client library for the Argus Hyperliquid perpetuals dispatcher — a market-data server
that proxies Hyperliquid's public `/info` API over a persistent TCP connection.

## Overview

`argus-hyperliquid` sits between your code and the Argus dispatcher process:

```
Your code  ←→  argus-hyperliquid  ←→  Argus Hyperliquid dispatcher  ←→  Hyperliquid
```

This crate handles the P1 TCP wire protocol and background I/O thread, and exposes a clean
synchronous, fully-typed API. It's built on [`argus-dispatcher-core`](../argus-dispatcher-core),
the transport layer shared with [`argus-lighter`](../argus-lighter).

The Hyperliquid dispatcher is early-stage (Argus "Phase 1.0 of v2") and currently exposes only
read-only market-data actions — no trading or account queries yet. This crate's coverage mirrors
the dispatcher's routing table exactly and will grow alongside it.

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
