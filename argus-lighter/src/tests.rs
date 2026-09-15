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

#[test]
#[ignore]
fn test_subscribe_and_stream_order_book() {
    use crate::Listener;

    let client = connect();
    let subscription = client.subscribe(&["BTC"]).unwrap();
    assert!(
        subscription.failed.is_empty(),
        "subscribe failed for: {:?}",
        subscription.failed
    );
    assert!(subscription.subscribed.iter().any(|symbol| symbol == "BTC"));

    let books = client.get_order_book();
    let event = client.get_order_book_event();

    // The market event fires for every Protocol 2 packet, so poll until the BTC book has a real
    // (non zero-padded) level rather than assuming the first notification is the one we want.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let best_ask = loop {
        let listener = event.listen();
        {
            let snapshot = books.read().unwrap();
            if let Some(book) = snapshot.get("BTC")
                && let Some(ask) = book.asks.iter().find(|order| order.quantity > 0.0)
            {
                break ask.price;
            }
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(
            remaining.as_millis() > 0,
            "timed out waiting for a BTC order book update"
        );
        listener.wait_timeout(remaining);
    };
    assert!(best_ask > 0.0, "expected a positive best ask, got {}", best_ask);

    let unsubscription = client.unsubscribe(&["BTC"]).unwrap();
    assert!(
        unsubscription.failed.is_empty(),
        "unsubscribe failed for: {:?}",
        unsubscription.failed
    );
    assert!(unsubscription.unsubscribed.iter().any(|symbol| symbol == "BTC"));
}

/// The dispatcher pushes each newly subscribed client its funding rate on its own, shortly after
/// `subscribe` is handled (Argus' `_routine_push_funding_rates_for_client` sleeps a randomized
/// 0.1-1.0s jitter, then sends) -- no manual trigger or waiting for the hourly perpetual refresh
/// needed. 5s is a generous margin above that jitter window.
#[test]
#[ignore]
fn test_funding_rate_update_delivered_within_5s() {
    use crate::{Listener, ReservedKey};

    let client = connect();
    let subscription = client.subscribe(&["BTC"]).unwrap();
    assert!(
        subscription.failed.is_empty(),
        "subscribe failed for: {:?}",
        subscription.failed
    );

    let books = client.get_order_book();
    let event = client.get_order_book_event();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let funding_rate = loop {
        let listener = event.listen();
        {
            let snapshot = books.read().unwrap();
            if let Some(book) = snapshot.get("BTC")
                && book.reserved.contains_key(&ReservedKey::FundingRate)
            {
                break book.funding_rate();
            }
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(
            remaining.as_millis() > 0,
            "timed out after 5s waiting for a BTC funding_rate_update push"
        );
        listener.wait_timeout(remaining);
    };

    assert!(
        funding_rate.is_some_and(f64::is_finite),
        "expected a finite BTC funding rate, got {:?}",
        funding_rate
    );

    let unsubscription = client.unsubscribe(&["BTC"]).unwrap();
    assert!(
        unsubscription.failed.is_empty(),
        "unsubscribe failed for: {:?}",
        unsubscription.failed
    );
}
