use uuid::Uuid;
use std::io::Read;
use serde_json::Value;
use std::collections::HashMap;
use flate2::read::ZlibDecoder;
use serde::{Deserialize, Deserializer, Serialize};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};

/// Deserializes a `u64` from either a JSON number or a quoted numeric string.
///
/// Some Argus dispatchers return integer fields as bare numbers, others as quoted strings.
/// Use this as `#[serde(deserialize_with = "deserialize_u64_from_string")]` on any `u64`
/// field that may arrive as either form.
pub fn deserialize_u64_from_string<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Value = Deserialize::deserialize(deserializer)?;
    match value {
        Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| serde::de::Error::custom("expected u64")),
        Value::String(s) => s.parse::<u64>().map_err(|e| {
            serde::de::Error::custom(format!("failed to parse u64 from string: {}", e))
        }),
        _ => Err(serde::de::Error::custom(
            "expected string or number for u64 field",
        )),
    }
}

/// Deserializes an `f64` from either a JSON number or a quoted numeric string.
///
/// Use this as `#[serde(deserialize_with = "deserialize_f64_from_string")]` on any `f64`
/// field that may arrive as either form.
pub fn deserialize_f64_from_string<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Value = Deserialize::deserialize(deserializer)?;
    match value {
        Value::Number(n) => n
            .as_f64()
            .ok_or_else(|| serde::de::Error::custom("expected f64")),
        Value::String(s) => s.parse::<f64>().map_err(|e| {
            serde::de::Error::custom(format!("failed to parse f64 from string: {}", e))
        }),
        _ => Err(serde::de::Error::custom(
            "expected string or number for f64 field",
        )),
    }
}

/// Deserializes an `Option<f64>` from a JSON number, quoted numeric string, `null`, or empty string.
///
/// Use as `#[serde(deserialize_with = "deserialize_optional_f64_from_string")]`.
pub fn deserialize_optional_f64_from_string<'de, D>(
    deserializer: D,
) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Value = Deserialize::deserialize(deserializer)?;
    match value {
        Value::Null => Ok(None),
        Value::Number(n) => n
            .as_f64()
            .map(Some)
            .ok_or_else(|| serde::de::Error::custom("expected f64")),
        Value::String(s) if s.is_empty() => Ok(None),
        Value::String(s) => s.parse::<f64>().map(Some).map_err(|e| {
            serde::de::Error::custom(format!("failed to parse f64 from string: {}", e))
        }),
        _ => Err(serde::de::Error::custom(
            "expected string, number, or null for optional f64 field",
        )),
    }
}

/// A P1 outbound request: `{"action", "data", "correlation_id"}`.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OutBoundMessage {
    pub action: String,
    pub data: Value,
    pub correlation_id: String,
}

impl OutBoundMessage {
    /// Constructs an outbound message with a randomly generated correlation ID if one is not provided.
    ///
    /// `action` is the dispatcher's command name (e.g. `"products_version"`). `data` is the
    /// command payload as a `serde_json::Value`. `correlation_id` can be supplied to reuse a
    /// known ID; if `None` a UUID v4 is generated. The correlation ID is used to match the
    /// dispatcher's response to this specific request.
    pub fn new(action: String, data: Value, correlation_id: Option<String>) -> Self {
        OutBoundMessage {
            action,
            data,
            correlation_id: correlation_id.unwrap_or(Uuid::new_v4().to_string()),
        }
    }
}

/// A P1 inbound message: a response (`correlation_id` set) or an unsolicited push (`correlation_id` absent).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InBoundMessage {
    pub action: String,
    pub data: Value,
    pub error: Option<String>,
    pub compressed: Option<bool>,
    pub correlation_id: Option<String>,
}

/// Response to a `subscribe` request: which symbols were registered and which were rejected.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscriptionResponse {
    pub subscribed: Vec<String>,
    pub failed: Vec<String>,
}

/// Response to an `unsubscribe` request: which symbols were removed and which were not.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UnsubscriptionResponse {
    pub unsubscribed: Vec<String>,
    pub failed: Vec<String>,
}

/// One price level of an order book.
#[derive(Debug, Clone)]
pub struct Order {
    pub price: f64,
    pub quantity: f64,
}

/// A key identifying one kind of "reserved" live data attached to an [`OrderBook`] that isn't
/// part of the order book itself (e.g. a perpetual's funding rate).
///
/// Used as the key of [`OrderBook::reserved`]; add a variant here (and a matching one on
/// [`ReservedValue`]) for each new kind of out-of-band live data the dispatcher starts pushing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReservedKey {
    FundingRate,
}

/// The typed payload for a [`ReservedKey`] entry in [`OrderBook::reserved`].
#[derive(Debug, Clone, PartialEq)]
pub enum ReservedValue {
    FundingRate(Option<f64>),
}

/// A point-in-time Protocol 2 order book snapshot for one symbol.
#[derive(Debug, Clone)]
pub struct OrderBook {
    pub symbol: String,
    pub bids: Vec<Order>,
    pub asks: Vec<Order>,
    pub remote_timestamp: f64,
    pub argus_timestamp: f64,
    /// Live, non-order data pushed out-of-band by the dispatcher for this symbol (e.g. a
    /// perpetual's funding rate, arriving as an unsolicited `funding_rate_update` Protocol 1
    /// push rather than as part of the Protocol 2 order book stream). Prefer typed accessors
    /// like [`OrderBook::funding_rate`] over reading this map directly.
    ///
    /// For the funding rate specifically, expect it within about a second of a successful
    /// `subscribe`: the dispatcher's `_routine_push_funding_rates_for_client` (in Argus'
    /// `argus/perpetuals/shared/__init__.py`) fires once per subscribing client right after
    /// `subscribe` is handled, sleeping a randomized 0.1-1.0s jitter first before sending —
    /// don't assume it lands in the very first Protocol 2 packet or two. It also arrives again
    /// later, for every already-subscribed client, whenever the dispatcher's hourly perpetual
    /// refresh runs (`_distribute_refreshed_perpetuals`).
    pub reserved: HashMap<ReservedKey, ReservedValue>,
}

impl OrderBook {
    /// The perpetual's live funding rate, if one has been received for this symbol.
    ///
    /// `None` both when no funding rate push has arrived yet and when the dispatcher explicitly
    /// pushed a `null` rate. See [`OrderBook::reserved`] for when to expect the first push.
    pub fn funding_rate(&self) -> Option<f64> {
        match self.reserved.get(&ReservedKey::FundingRate) {
            Some(ReservedValue::FundingRate(v)) => *v,
            _ => None,
        }
    }

    /// Prints a formatted side-by-side view of the order book to stdout.
    ///
    /// Bids are shown on the left and asks on the right, both sorted best-first. Column widths are
    /// computed from the actual data so the output aligns cleanly regardless of price/quantity
    /// magnitude. Prints nothing but the header if both sides are empty.
    pub fn print_orderbook(&self) {
        use std::fmt::Write;
        let mut out = String::with_capacity(4096);

        let _ = writeln!(out, "Order Book for symbol: {}", self.symbol);

        if self.bids.is_empty() && self.asks.is_empty() {
            print!("{out}");
            return;
        }

        let bid_index_width = self.bids.len().to_string().len();
        let ask_index_width = self.asks.len().to_string().len();

        let mut bids = Vec::with_capacity(self.bids.len());
        let mut asks = Vec::with_capacity(self.asks.len());

        let mut max_bid_price = 0;
        let mut max_bid_qty = 0;
        let mut max_ask_price = 0;
        let mut max_ask_qty = 0;

        for o in &self.bids {
            let p = format!("{:.2}", o.price);
            let q = format!("{:.2}", o.quantity);
            max_bid_price = max_bid_price.max(p.len());
            max_bid_qty = max_bid_qty.max(q.len());
            bids.push((p, q));
        }

        for o in &self.asks {
            let p = format!("{:.2}", o.price);
            let q = format!("{:.2}", o.quantity);
            max_ask_price = max_ask_price.max(p.len());
            max_ask_qty = max_ask_qty.max(q.len());
            asks.push((p, q));
        }

        let bid_empty_len = "Bid ".len()
            + bid_index_width
            + ": Price: ".len()
            + max_bid_price
            + ", Quantity: ".len()
            + max_bid_qty;

        let ask_empty_len = "Ask ".len()
            + ask_index_width
            + ": Price: ".len()
            + max_ask_price
            + ", Quantity: ".len()
            + max_ask_qty;

        let bid_empty = " ".repeat(bid_empty_len);
        let ask_empty = " ".repeat(ask_empty_len);

        let rows = bids.len().max(asks.len());
        for i in 0..rows {
            if let Some((p, q)) = bids.get(i) {
                let _ = write!(
                    out,
                    "Bid {:>idx$}: Price: {:>pw$}, Quantity: {:>qw$}",
                    i + 1,
                    p,
                    q,
                    idx = bid_index_width,
                    pw = max_bid_price,
                    qw = max_bid_qty,
                );
            } else {
                out.push_str(&bid_empty);
            }

            out.push_str(" | ");

            if let Some((p, q)) = asks.get(i) {
                let _ = write!(
                    out,
                    "Ask {:>idx$}: Price: {:>pw$}, Quantity: {:>qw$}",
                    i + 1,
                    p,
                    q,
                    idx = ask_index_width,
                    pw = max_ask_price,
                    qw = max_ask_qty,
                );
            } else {
                out.push_str(&ask_empty);
            }

            out.push('\n');
        }

        print!("{out}");
    }
}

/// Which wire protocol a packet belongs to.
///
/// `Protocol1` carries JSON request/response and push messages (`~NNNN|{...}`). `Protocol2`
/// carries compact binary order book snapshots (`~NNNNMMMM|<symbol><values>L`) pushed
/// unsolicited after a `subscribe` request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolKind {
    Protocol1,
    Protocol2,
}

/// Intermediate representation of a Protocol 2 packet: the symbol plus its raw numeric values.
#[derive(Debug)]
pub struct Protocol2IR {
    pub symbol: String,
    pub values: Vec<f64>,
}

pub struct ProtocolFns;

impl ProtocolFns {
    /// Inspects a raw byte slice and determines which wire protocol it belongs to.
    ///
    /// Returns `Ok(ProtocolKind::Protocol1)` for JSON message packets (`~NNNN|{...}`) or
    /// `Ok(ProtocolKind::Protocol2)` for order book packets (`~NNNNMMMM|<symbol><values>L`).
    /// Returns `Err` if the packet is too short, has an invalid header byte, or fails the
    /// per-protocol structural checks. Called by the reading thread on each candidate packet
    /// before it is forwarded for decoding.
    pub fn analyse_bytes(bytes: &[u8]) -> Result<ProtocolKind, String> {
        if bytes.len() < 6 {
            return Err(format!(
                "Invalid packet: expected length of at least 6, got {}",
                bytes.len()
            ));
        }

        let raw_kind;
        let header = &bytes[0..5];

        if header[0] != 0x7E {
            return Err(format!(
                "Invalid header: expected first byte to be 0x7E, got 0x{:02X}",
                header[0]
            ));
        }

        let length_bytes = &header[1..5];
        let length_real = (length_bytes[0] - 0x30) as usize * 1000
            + (length_bytes[1] - 0x30) as usize * 100
            + (length_bytes[2] - 0x30) as usize * 10
            + (length_bytes[3] - 0x30) as usize;

        if bytes.len() < 5 + length_real {
            return Err(format!(
                "Invalid packet: expected length of at least {}, got {}",
                5 + length_real,
                bytes.len()
            ));
        }

        if bytes[5] == 0x7C {
            raw_kind = ProtocolKind::Protocol1;
        } else {
            raw_kind = ProtocolKind::Protocol2;
        }

        match raw_kind {
            ProtocolKind::Protocol1 => {
                if bytes[6] != 0x7B {
                    return Err(format!(
                        "Invalid packet for protocol 1: expected byte after pipe to be 0x7B, got 0x{:02X}",
                        bytes[6]
                    ));
                }

                if bytes[bytes.len() - 1] != 0x7D {
                    return Err(format!(
                        "Invalid packet for protocol 1: expected last byte to be 0x7D, got 0x{:02X}",
                        bytes[bytes.len() - 1]
                    ));
                }

                Ok(ProtocolKind::Protocol1)
            }
            ProtocolKind::Protocol2 => {
                if bytes[9] != 0x7C {
                    return Err(format!(
                        "Invalid packet for protocol 2: expected byte after symbol length to be 0x7C, got 0x{:02X}",
                        bytes[9]
                    ));
                }

                if bytes[bytes.len() - 1] != 0x4C {
                    return Err(format!(
                        "Invalid packet for protocol 2: expected last byte to be 0x4C, got 0x{:02X}",
                        bytes[bytes.len() - 1]
                    ));
                }

                Ok(ProtocolKind::Protocol2)
            }
        }
    }

    /// Serializes an [`OutBoundMessage`] into Protocol 1 wire bytes.
    ///
    /// Produces an owned `Vec<u8>` in the form `~NNNN|{...json...}` where `NNNN` is the
    /// zero-padded length of the JSON body. Pass the result directly to the TCP write stream.
    pub fn protocol_1_encoder(message: &OutBoundMessage) -> Vec<u8> {
        let json_string = serde_json::to_string(message).expect("Failed to serialize message");
        let length_of_message = json_string.len();
        let protocol_message = format!("~{:04}|{}", length_of_message, json_string);
        protocol_message.as_bytes().to_vec()
    }

    /// Deserializes a Protocol 1 packet into an [`InBoundMessage`].
    ///
    /// Strips the `~NNNN` header and the `|` separator, then JSON-parses the body. Panics if
    /// the packet is malformed or the JSON cannot be parsed — caller is expected to only pass
    /// bytes that have already passed [`analyse_bytes`].
    pub fn protocol_1_decoder(message: &[u8]) -> InBoundMessage {
        let message_str = String::from_utf8_lossy(message);
        let parts: Vec<&str> = message_str.splitn(2, '|').collect();
        if parts.len() != 2 {
            panic!("Invalid message format");
        }
        let json_part = parts[1];
        serde_json::from_str(json_part).expect("Failed to deserialize message")
    }

    /// Decompresses a Protocol 1 message in place if it is marked as compressed.
    ///
    /// When a dispatcher sets `compressed: true` on a response (used for large payloads, e.g.
    /// paginated market lists), the `data` field is a base64-encoded zlib-compressed JSON
    /// string. This decodes and decompresses it, replacing `data` with the parsed `Value` and
    /// setting `compressed` to `false`. No-op if `compressed` is not `Some(true)`.
    pub fn maybe_decompress_p1(msg: &mut InBoundMessage) {
        if msg.compressed != Some(true) {
            return;
        }

        let data_str = match msg.data.as_str() {
            Some(s) => s,
            None => return,
        };

        let compressed_bytes = match BASE64.decode(data_str) {
            Ok(b) => b,
            Err(_) => return,
        };

        let mut decoder = ZlibDecoder::new(&compressed_bytes[..]);
        let mut decompressed = String::new();
        if decoder.read_to_string(&mut decompressed).is_err() {
            return;
        }

        if let Ok(json_value) = serde_json::from_str(&decompressed) {
            msg.data = json_value;
            msg.compressed = Some(false);
        }
    }

    /// Decodes a Protocol 2 packet into its intermediate representation.
    ///
    /// Strips framing bytes, extracts the symbol string and the trailing comma-separated float
    /// values (bids, asks, timestamps) using `fast_float` for performance. Returns an owned
    /// [`Protocol2IR`] with the raw `Vec<f64>`. The caller must know the value layout to
    /// interpret the slice; prefer [`bytes_to_orderbook`] which handles the layout automatically.
    /// Uses `unsafe` `from_utf8_unchecked` on the inner content — safe because Argus only
    /// encodes ASCII.
    pub fn protocol_2_decoder(message_bytes: &[u8]) -> Result<Protocol2IR, String> {
        if message_bytes[message_bytes.len() - 1] != 0x4C {
            Err(format!(
                "Invalid packet for protocol 2: expected last byte to be 0x4C, got 0x{:02X}",
                message_bytes[message_bytes.len() - 1]
            ))?;
        }

        let inner_content = &message_bytes[5..message_bytes.len() - 1];
        let inner_content_str: &str;

        // SAFETY: Argus only encodes ASCII
        unsafe {
            inner_content_str = std::str::from_utf8_unchecked(inner_content);
        }

        let symbol_length: usize = inner_content_str[0..4]
            .parse()
            .expect("Failed to parse symbol length");
        let symbol = &inner_content_str[5..5 + symbol_length];
        let message_body = &inner_content_str[5 + symbol_length..];

        let mut working_buff = String::new();
        let mut parsed_values: Vec<f64> = Vec::new();

        for c in message_body.chars() {
            if c == ',' {
                let value: f64 = fast_float::parse(working_buff.as_str())
                    .expect(format!("Failed to parse value: {}", working_buff).as_str());
                parsed_values.push(value);
                working_buff.clear();
            } else {
                working_buff.push(c);
            }
        }

        if !working_buff.is_empty() {
            let value: f64 = fast_float::parse(working_buff.as_str())
                .expect(format!("Failed to parse value: {}", working_buff).as_str());
            parsed_values.push(value);
        }

        Ok(Protocol2IR {
            symbol: symbol.to_string(),
            values: parsed_values,
        })
    }

    /// Decodes a Protocol 2 packet directly into an [`OrderBook`].
    ///
    /// Calls [`protocol_2_decoder`] and interprets the resulting float slice as interleaved
    /// (price, quantity) pairs: the first half is bids, the second half is asks, with the last
    /// two values being the exchange timestamp (`remote_timestamp`) and the Argus timestamp
    /// (`argus_timestamp`). `depth` controls how many levels to read from each side; defaults
    /// to 10 if `None`. Panics if the packet contains fewer values than `depth * 2` per side.
    /// Returns an owned `OrderBook` — this is a point-in-time snapshot, not a live handle.
    pub fn bytes_to_orderbook(message_bytes: &[u8], depth: Option<usize>) -> OrderBook {
        let depth = depth.unwrap_or(10);
        let ir = ProtocolFns::protocol_2_decoder(message_bytes)
            .expect("Failed to decode protocol 2 message");

        let argus_timestamp = ir.values[ir.values.len() - 1];
        let remote_timestamp = ir.values[ir.values.len() - 2];
        let values_without_timestamps = &ir.values[0..ir.values.len() - 2];

        let mut bids: Vec<Order> = Vec::new();
        let mut asks: Vec<Order> = Vec::new();

        let split_index = values_without_timestamps.len() / 2;

        if split_index < depth {
            panic!(
                "Not enough values in the message to fill the orderbook depth, expected at least {}, got {}",
                depth * 2,
                ir.values.len()
            );
        }

        let bid_values = &values_without_timestamps[0..split_index];
        let ask_values = &values_without_timestamps[split_index..];

        for i in 0..depth {
            let bid_price = bid_values[i * 2];
            let bid_quantity = bid_values[i * 2 + 1];
            let ask_price = ask_values[i * 2];
            let ask_quantity = ask_values[i * 2 + 1];

            bids.push(Order {
                price: bid_price,
                quantity: bid_quantity,
            });

            asks.push(Order {
                price: ask_price,
                quantity: ask_quantity,
            });
        }

        OrderBook {
            symbol: ir.symbol,
            bids,
            asks,
            remote_timestamp,
            argus_timestamp,
            reserved: HashMap::new(),
        }
    }

    /// Extracts the symbol and funding rate from a `funding_rate_update` Protocol 1 push's
    /// `data` field: `{"coin": "<symbol>", "funding_rate": "<decimal>" | null}`.
    ///
    /// The dispatcher sends this to one client at a time, shortly (0.1-1.0s jitter) after that
    /// client's `subscribe` is handled (Argus' `_routine_push_funding_rates_for_client`), and
    /// again to every subscribed client on the hourly perpetual refresh
    /// (`_distribute_refreshed_perpetuals`).
    ///
    /// Returns `None` if `data` has no `"coin"` string field. The funding rate is `None` if the
    /// field is absent, `null`, or fails to parse as `f64` — a symbol with no reading is still
    /// meaningful (nothing pushed yet), so this never panics on a malformed rate.
    pub fn parse_funding_rate_update(data: &Value) -> Option<(String, Option<f64>)> {
        let symbol = data.get("coin")?.as_str()?.to_string();
        let funding_rate = match data.get("funding_rate") {
            Some(Value::String(s)) => s.parse::<f64>().ok(),
            Some(Value::Number(n)) => n.as_f64(),
            _ => None,
        };
        Some((symbol, funding_rate))
    }
}
