//! Integration tests mirroring `Argus/tests/lighter_cli.py`'s gauntlet. Require a live Lighter
//! dispatcher (`python runtime.py lighter --port 9974` from the Argus repo). Run with
//! `cargo test -- --ignored`. Override the address with the `ARGUS_LIGHTER_ADDR` env var.

#![cfg(test)]

use crate::LighterClient;

fn connect() -> LighterClient {
    let address =
        std::env::var("ARGUS_LIGHTER_ADDR").unwrap_or_else(|_| "localhost:9974".to_string());
    LighterClient::connect(&address)
}

#[test]
#[ignore]
fn test_products_version() {
    let client = connect();
    let version = client.products_version().unwrap();
    assert!(!version.argus.is_empty());
    assert_eq!(version.lighter_dispatcher.len(), 4);
}

#[test]
#[ignore]
fn test_get_markets() {
    let client = connect();
    let perps = client.get_markets(0, Some(10)).unwrap();
    assert!(!perps.is_empty(), "expected at least one perpetual market");
    for perp in &perps {
        assert!(!perp.market.symbol.is_empty());
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
    let rates: Vec<f64> = perps.iter().filter_map(|p| p.funding_rate).collect();
    let mut sorted = rates.clone();
    sorted.sort_by(|a, b| b.partial_cmp(a).unwrap());
    assert_eq!(rates, sorted, "funding rates are not sorted descending");
}

#[test]
#[ignore]
fn test_market_info() {
    let client = connect();
    let perps = client.get_markets(0, Some(10)).unwrap();
    let symbol = perps
        .first()
        .expect("expected at least one market to test market_info with")
        .market
        .symbol
        .clone();
    let info = client
        .market_info(Some(&symbol), None)
        .unwrap()
        .expect("market_info returned None for a symbol just listed by get_markets");
    assert_eq!(info.market.symbol, symbol);
}

#[test]
#[ignore]
fn test_get_funding_history() {
    let client = connect();
    let perps = client.get_markets(0, Some(10)).unwrap();
    let market_id = perps
        .first()
        .expect("expected at least one market to test get_funding_history with")
        .market
        .market_id;
    let start_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        - 24 * 60 * 60;
    let history = client
        .get_funding_history(market_id, start_ts, None, None)
        .unwrap();
    for entry in &history {
        assert!(entry.direction == "long" || entry.direction == "short");
    }
}
