//! Unit tests for the wire-protocol codecs. These do not need a live dispatcher.

use crate::protocol::{OutBoundMessage, ProtocolFns, ProtocolKind};

/// Builds a syntactically valid depth-`depth` Protocol 2 packet with a real best bid/ask and
/// zero-padded remaining levels, mirroring what the dispatchers emit
/// (`~NNNNMMMM|<symbol><bids>,<asks>,<remote_ts>,<argus_ts>L`).
fn build_p2_packet(symbol: &str, depth: usize, bid_price: f64, ask_price: f64) -> Vec<u8> {
    let mut csv = String::new();
    for i in 0..depth {
        let (price, quantity) = if i == 0 { (bid_price, 1.5) } else { (0.0, 0.0) };
        csv.push_str(&format!("{price},{quantity},"));
    }
    for i in 0..depth {
        let (price, quantity) = if i == 0 { (ask_price, 2.5) } else { (0.0, 0.0) };
        csv.push_str(&format!("{price},{quantity},"));
    }
    csv.push_str("1770251679393,1789386667.010828");

    let body = format!("{:04}|{}{}L", symbol.len(), symbol, csv);
    format!("~{:04}{}", body.len(), body).into_bytes()
}

#[test]
fn analyse_bytes_classifies_p1_and_p2() {
    assert_eq!(
        ProtocolFns::analyse_bytes(&build_p2_packet("BTC", 10, 104500.5, 104501.0)),
        Ok(ProtocolKind::Protocol2)
    );

    let p1 = ProtocolFns::protocol_1_encoder(&OutBoundMessage::new(
        "products_version".to_string(),
        crate::json!({}),
        None,
    ));
    assert_eq!(ProtocolFns::analyse_bytes(&p1), Ok(ProtocolKind::Protocol1));
}

#[test]
fn analyse_bytes_rejects_truncated_p2() {
    let packet = build_p2_packet("BTC", 10, 1.0, 2.0);
    let truncated = &packet[..packet.len() - 1];
    assert!(ProtocolFns::analyse_bytes(truncated).is_err());
}

#[test]
fn bytes_to_orderbook_decodes_levels_and_timestamps() {
    let book = ProtocolFns::bytes_to_orderbook(&build_p2_packet("BTC", 10, 104500.5, 104501.0), None);

    assert_eq!(book.symbol, "BTC");
    assert_eq!(book.bids.len(), 10);
    assert_eq!(book.asks.len(), 10);
    assert_eq!(book.bids[0].price, 104500.5);
    assert_eq!(book.bids[0].quantity, 1.5);
    assert_eq!(book.asks[0].price, 104501.0);
    assert_eq!(book.asks[0].quantity, 2.5);
    assert_eq!(book.bids[9].price, 0.0);
    assert_eq!(book.bids[9].quantity, 0.0);
    assert_eq!(book.remote_timestamp, 1770251679393.0);
    assert_eq!(book.argus_timestamp, 1789386667.010828);
}

#[test]
fn p1_encoder_decoder_roundtrip() {
    let msg = OutBoundMessage::new(
        "subscribe".to_string(),
        crate::json!(["BTC", "ETH"]),
        None,
    );
    let decoded = ProtocolFns::protocol_1_decoder(&ProtocolFns::protocol_1_encoder(&msg));
    assert_eq!(decoded.action, "subscribe");
    assert_eq!(decoded.data, crate::json!(["BTC", "ETH"]));
    assert_eq!(decoded.correlation_id, Some(msg.correlation_id));
}
