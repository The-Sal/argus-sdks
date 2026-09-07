pub mod connection;
pub mod protocol;

pub use connection::{DispatcherConnection, PushedMessages};
pub use protocol::{
    InBoundMessage, OutBoundMessage, ProtocolFns, ProtocolKind,
    deserialize_f64_from_string, deserialize_optional_f64_from_string,
    deserialize_u64_from_string,
};

pub use serde_json::json;
