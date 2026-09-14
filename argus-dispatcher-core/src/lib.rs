pub mod connection;
pub mod protocol;

pub use connection::{DispatcherConnection, PushedMessages};
pub use event_listener::{Event, Listener};
pub use protocol::{
    InBoundMessage, Order, OrderBook, OutBoundMessage, Protocol2IR, ProtocolFns, ProtocolKind,
    SubscriptionResponse, UnsubscriptionResponse, deserialize_f64_from_string,
    deserialize_optional_f64_from_string, deserialize_u64_from_string,
};

pub use serde_json::json;

#[cfg(test)]
mod tests;
