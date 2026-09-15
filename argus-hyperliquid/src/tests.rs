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

    // Default-dex coins have no annotation (Hyperliquid returns `null` for `perpAnnotation`,
    // which the dispatcher must treat as "no data" rather than an error), but they do have
    // predicted funding. Regression test for the dispatcher's `_post(..., allow_null=True)` fix.
    let info = client.perpetual_info("BTC").unwrap();
    assert_eq!(info.coin, "BTC");
    assert!(
        info.annotation.is_none(),
        "default-dex coins have no annotation"
    );

    // HIP-3 (builder-deployed) coins are the opposite: they have annotation metadata, and no
    // predicted funding. Use a HIP-3 coin so this exercises the annotation path, mirroring
    // test_get_perpetuals_hip3_dex.
    let dexes = client.get_dexs().unwrap();
    let Some(dex) = dexes.first() else {
        eprintln!("skipped HIP-3 checks: no HIP-3 dexes registered");
        return;
    };
    let perps = client.get_perpetuals_for_dex(&dex.name, 0, Some(1)).unwrap();
    let coin = perps
        .first()
        .expect("expected at least one perpetual in the HIP-3 dex")
        .asset
        .name
        .clone();
    let info = client.perpetual_info(&coin).unwrap();
    assert_eq!(info.coin, coin);
    assert!(
        info.annotation.is_some() || info.category.is_some() || info.concise_annotation.is_some(),
        "expected some annotation metadata for HIP-3 coin {coin}"
    );
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
    assert!(subscription.subscribed.iter().any(|coin| coin == "BTC"));

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
    assert!(unsubscription.unsubscribed.iter().any(|coin| coin == "BTC"));
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
