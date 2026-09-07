//! Integration tests mirroring `Argus/tests/hyper_cli.py`'s gauntlet. Require a live Hyperliquid
//! dispatcher (`python runtime.py hyperliquid --port 9972` from the Argus repo). Run with
//! `cargo test -- --ignored`. Override the address with the `ARGUS_HYPERLIQUID_ADDR` env var.

#![cfg(test)]

use crate::HyperliquidClient;

fn connect() -> HyperliquidClient {
    let address =
        std::env::var("ARGUS_HYPERLIQUID_ADDR").unwrap_or_else(|_| "localhost:9972".to_string());
    HyperliquidClient::connect(&address)
}

#[test]
#[ignore]
fn test_products_version() {
    let client = connect();
    let version = client.products_version().unwrap();
    assert!(!version.argus.is_empty());
    assert_eq!(version.hyperliquid_dispatcher.len(), 4);
}

#[test]
#[ignore]
fn test_get_dexs() {
    let client = connect();
    let dexes = client.get_dexs().unwrap();
    for dex in &dexes {
        assert!(!dex.deployer.is_empty());
    }
}

#[test]
#[ignore]
fn test_get_perpetuals_default_dex() {
    let client = connect();
    let perps = client.get_perpetuals_for_dex("", 0, None).unwrap();
    assert!(!perps.is_empty(), "expected at least one perpetual on the default dex");
    for perp in &perps {
        assert!(!perp.asset.name.is_empty());
    }
}

#[test]
#[ignore]
fn test_get_perpetuals_hip3_dex() {
    let client = connect();
    let dexes = client.get_dexs().unwrap();
    let Some(dex) = dexes.first() else {
        eprintln!("skipped: no HIP-3 dexes registered");
        return;
    };
    let perps = client.get_perpetuals_for_dex(&dex.name, 0, None).unwrap();
    for perp in &perps {
        assert_eq!(perp.dex, dex.name);
    }
}

#[test]
#[ignore]
fn test_get_funding_rates_for_all_perpetuals() {
    let client = connect();
    let perps = client
        .get_funding_rates_for_all_perpetuals(0, None)
        .unwrap();
    assert!(!perps.is_empty(), "expected at least one funding rate entry");
    let fundings: Vec<f64> = perps.iter().map(|p| p.funding_rate()).collect();
    let mut sorted = fundings.clone();
    sorted.sort_by(|a, b| b.partial_cmp(a).unwrap());
    assert_eq!(fundings, sorted, "funding rates are not sorted descending");
}

#[test]
#[ignore]
fn test_perpetual_info() {
    let client = connect();
    let perps = client.get_perpetuals_for_dex("", 0, None).unwrap();
    let coin = perps
        .first()
        .expect("expected at least one perpetual to test perpetual_info with")
        .asset
        .name
        .clone();
    let info = client.perpetual_info(&coin).unwrap();
    assert_eq!(info.coin, coin);
}
