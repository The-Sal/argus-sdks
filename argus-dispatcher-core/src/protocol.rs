use uuid::Uuid;
use std::io::Read;
use serde_json::Value;
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

/// Which wire protocol a packet belongs to.
///
/// Only `Protocol1` (JSON request/response and push messages) is implemented today, since
/// neither the Hyperliquid nor Lighter dispatcher streams anything else yet. This is a
/// dedicated enum (rather than a bare `()`) so a future binary/streaming protocol — mirroring
/// Polymarket's Protocol 2 order-book feed — can be added as another variant without breaking
/// callers that match on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolKind {
    Protocol1,
}

pub struct ProtocolFns;

impl ProtocolFns {
    /// Inspects a raw byte slice and confirms it is a well-formed Protocol 1 packet.
    ///
    /// Returns `Ok(ProtocolKind::Protocol1)` for a complete `~NNNN|{...}` packet, or `Err` if
    /// the packet is too short, has an invalid header byte, or fails structural checks. Called
    /// by the reading thread on each candidate packet before it is forwarded for decoding.
    pub fn analyse_bytes(bytes: &[u8]) -> Result<ProtocolKind, String> {
        if bytes.len() < 6 {
            return Err(format!(
                "Invalid packet: expected length of at least 6, got {}",
                bytes.len()
            ));
        }

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

        if bytes[5] != 0x7C {
            return Err(format!(
                "Invalid packet: expected byte after length header to be 0x7C, got 0x{:02X}",
                bytes[5]
            ));
        }

        if bytes[6] != 0x7B {
            return Err(format!(
                "Invalid packet: expected byte after pipe to be 0x7B, got 0x{:02X}",
                bytes[6]
            ));
        }

        if bytes[bytes.len() - 1] != 0x7D {
            return Err(format!(
                "Invalid packet: expected last byte to be 0x7D, got 0x{:02X}",
                bytes[bytes.len() - 1]
            ));
        }

        Ok(ProtocolKind::Protocol1)
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
}
