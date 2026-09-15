/*

Generic wire-protocol client for Argus dispatchers. Handles the TCP connection, background I/O
threading, correlation-id request/response matching, and streaming order books — it has no
exchange-specific types or actions. See the argus-hyperliquid and argus-lighter crates for typed
clients built on top of `DispatcherConnection`.

Both the Hyperliquid and Lighter dispatchers stream Protocol 2 order book snapshots to any
connection that issues a `subscribe` request, on the same TCP socket as the Protocol 1
request/response traffic. The connection keeps the same two-thread architecture Polymarket's
dispatcher client uses: a reading thread that frames both protocols and a processing thread that
routes responses, unsolicited pushes, and order book updates to their respective buffers/events.

*/

use serde_json::Value;
use std::net::TcpStream;
use std::time::Duration;
use std::io::{Read, Write};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{spawn, JoinHandle};
use event_listener::{Event, Listener};
use crossbeam::channel::{unbounded, Receiver, Sender};
use crate::protocol::{
    InBoundMessage, OrderBook, OutBoundMessage, ProtocolFns, ProtocolKind, ReservedKey,
    ReservedValue, SubscriptionResponse, UnsubscriptionResponse,
};


const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Buffer of Protocol 1 messages pushed by the dispatcher that were not solicited by a specific
/// request (i.e. arrived without a `correlation_id`), such as notifications or fatal errors.
#[derive(Debug)]
pub struct PushedMessages {
    buffer: Vec<InBoundMessage>,
}

impl PushedMessages {
    fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    /// Drains all messages that have been pushed but not yet read.
    pub fn drain(&mut self) -> Vec<InBoundMessage> {
        std::mem::take(&mut self.buffer)
    }

    fn add_message(&mut self, msg: InBoundMessage) {
        self.buffer.push(msg);
        if self.buffer.len() > 5 {
            println!(
                "[WARNING] There are {} pushed messages that have not yet been read by the \
                consumer. This may be a sign it isn't keeping up. Buffer: {:?}",
                self.buffer.len(),
                self.buffer
            );
        }
    }
}

/// A connection to an Argus dispatcher over its TCP wire protocol.
///
/// Construct with [`DispatcherConnection::new`], call [`start`](Self::start) once to spawn the
/// background I/O threads, then issue requests with [`request`](Self::request) and stream order
/// books with [`subscribe`](Self::subscribe) + [`get_order_book`](Self::get_order_book).
#[derive(Debug)]
pub struct DispatcherConnection {
    read_stream_handle: Arc<RwLock<TcpStream>>,
    write_stream_handle: Arc<Mutex<TcpStream>>,
    pushed_messages: Arc<RwLock<PushedMessages>>,
    push_event: Arc<Event>,
    response_buf: Arc<RwLock<Vec<InBoundMessage>>>,
    response_event: Arc<Event>,
    order_books: Arc<RwLock<HashMap<String, OrderBook>>>,
    market_event: Arc<Event>,
}

impl DispatcherConnection {
    /// Opens a blocking TCP connection to an Argus dispatcher at `address` (e.g. `"localhost:9972"`).
    ///
    /// Panics if the connection cannot be established. Call [`start`](Self::start) immediately
    /// after construction to start the background I/O threads before issuing any requests.
    pub fn new(address: &str) -> Self {
        println!("Connecting to Argus dispatcher at {}", address);
        let stream = TcpStream::connect(address).expect("Could not connect to dispatcher");
        println!("Successfully connected to Argus dispatcher at {}", address);
        DispatcherConnection {
            read_stream_handle: Arc::new(RwLock::new(
                stream.try_clone().expect("Failed to clone stream"),
            )),
            write_stream_handle: Arc::new(Mutex::new(stream)),
            pushed_messages: Arc::new(RwLock::new(PushedMessages::new())),
            push_event: Arc::new(Event::new()),
            response_buf: Arc::new(Default::default()),
            response_event: Arc::new(Event::new()),
            order_books: Arc::new(RwLock::new(HashMap::new())),
            market_event: Arc::new(Event::new()),
        }
    }

    /// Returns a shared handle to the live order book map.
    ///
    /// The returned `Arc<RwLock<HashMap<String, OrderBook>>>` is backed by the same allocation
    /// that the background processing thread writes to. Every Protocol 2 packet received from
    /// the dispatcher overwrites the bids/asks/timestamps for that symbol in place, so a read
    /// lock taken at any point will see the most recent snapshot available. Keys are the
    /// subscribed symbols: Hyperliquid coins (e.g. `"BTC"`, `"xyz:AAPL"`) or Lighter symbols
    /// (e.g. `"BTC"`).
    ///
    /// Levels with no data behind them are zero-padded, so filter on `quantity > 0.0` when
    /// iterating a side.
    ///
    /// `OrderBook::reserved` (see [`OrderBook::funding_rate`]) is populated separately from
    /// unsolicited `funding_rate_update` Protocol 1 pushes and is preserved across Protocol 2
    /// updates to the same symbol, rather than being overwritten by them. An entry can exist
    /// here with only `reserved` populated if a funding rate push arrives before the first order
    /// book snapshot for that symbol. The dispatcher sends the first funding rate push per
    /// subscribing client shortly (0.1-1.0s randomized jitter) after [`subscribe`](Self::subscribe)
    /// is handled — expect it a beat or two after order book packets start flowing, not
    /// necessarily on the very first one.
    pub fn get_order_book(&self) -> Arc<RwLock<HashMap<String, OrderBook>>> {
        self.order_books.clone()
    }

    /// Returns a shared handle to the market-data notification event.
    ///
    /// The background processing thread calls `notify(usize::MAX)` on this [`Event`] every time
    /// a Protocol 2 order book packet is processed. Register a [`Listener`] *before* reading the
    /// order book map to avoid missing updates between the read and the wait:
    ///
    /// ```no_run
    /// use argus_dispatcher_core::{DispatcherConnection, Listener};
    ///
    /// # fn main() {
    /// let conn = DispatcherConnection::new("localhost:9972");
    /// let books = conn.get_order_book();
    /// let event = conn.get_order_book_event();
    ///
    /// let listener = event.listen(); // register first
    /// let snapshot = books.read().unwrap(); // then read
    /// // ... use snapshot ...
    /// drop(snapshot);
    /// listener.wait(); // block until the next update arrives
    /// # }
    /// ```
    pub fn get_order_book_event(&self) -> Arc<Event> {
        self.market_event.clone()
    }

    /// Returns a shared handle to the buffer of unsolicited dispatcher pushes.
    ///
    /// Protocol 1 messages that arrive without a `correlation_id` (e.g. notifications or errors)
    /// are buffered here; draining works the same as for Polymarket's system messages.
    pub fn get_pushed_messages(&self) -> Arc<RwLock<PushedMessages>> {
        self.pushed_messages.clone()
    }

    /// Returns a shared handle to the event notified whenever a dispatcher push arrives.
    pub fn get_push_event(&self) -> Arc<Event> {
        self.push_event.clone()
    }

    /// Spawns the background reading and processing threads and returns the reading thread's handle.
    ///
    /// Must be called once before any other method. Two threads are started:
    /// - **Reading thread** — reads bytes into a rolling buffer, detects complete packets by their
    ///   terminator byte (`}` for Protocol 1, `L` for Protocol 2), validates them with
    ///   [`ProtocolFns::analyse_bytes`], and forwards `(bytes, protocol)` to the processing thread
    ///   via a crossbeam channel.
    /// - **Processing thread** — routes each packet. Protocol 1 packets with a `correlation_id`
    ///   are placed in the response-matching buffer for [`request`](Self::request) to pick up;
    ///   Protocol 1 packets without one are treated as unsolicited pushes. Protocol 2 packets
    ///   update the order book map and notify the market event.
    ///
    /// The returned `JoinHandle` belongs to the reading thread. The processing thread is detached.
    /// Both threads run indefinitely; the reading thread panics if the dispatcher closes the connection.
    pub fn start(&mut self) -> JoinHandle<()> {
        let read_handle = Arc::clone(&self.read_stream_handle);
        let response_buf_handle = Arc::clone(&self.response_buf);
        let response_event_handle = Arc::clone(&self.response_event);
        let pushed_messages_handle = Arc::clone(&self.pushed_messages);
        let push_event_handle = Arc::clone(&self.push_event);
        let order_books_handle = Arc::clone(&self.order_books);
        let market_event_handle = Arc::clone(&self.market_event);

        let (sender, receiver): (
            Sender<(Vec<u8>, ProtocolKind)>,
            Receiver<(Vec<u8>, ProtocolKind)>,
        ) = unbounded();

        // reading thread
        let handle = spawn(move || {
            let mut buffer = [0; 9999];
            let mut full_packet_buffer: Vec<u8> = Vec::new();

            loop {
                let mut stream = read_handle.write().unwrap();
                match stream.read(&mut buffer) {
                    Ok(bytes_read) => {
                        if bytes_read > 0 {
                            for byte in buffer[..bytes_read].iter() {
                                full_packet_buffer.push(*byte);
                                if (*byte == 0x4C || *byte == 0x7D)
                                    && let Ok(protocol_kind) =
                                        ProtocolFns::analyse_bytes(&full_packet_buffer)
                                {
                                    sender
                                        .send((full_packet_buffer.clone(), protocol_kind))
                                        .expect("Failed to send data to processing thread");
                                    full_packet_buffer.clear();
                                }
                            }
                        } else {
                            panic!("Stream closed by dispatcher");
                        }
                    }
                    Err(e) => {
                        eprintln!("Error reading from stream: {}", e);
                        break;
                    }
                }
            }
        });

        // processing thread
        spawn(move || {
            while let Ok((packet, protocol_kind)) = receiver.recv() {
                match protocol_kind {
                    ProtocolKind::Protocol1 => {
                        let mut decoded = ProtocolFns::protocol_1_decoder(&packet);
                        ProtocolFns::maybe_decompress_p1(&mut decoded);

                        if decoded.correlation_id.is_some() {
                            response_buf_handle
                                .write()
                                .expect("Failed to lock response buffer for writing")
                                .push(decoded);
                            response_event_handle.notify(usize::MAX);
                        } else if decoded.action == "funding_rate_update" {
                            if let Some((symbol, funding_rate)) =
                                ProtocolFns::parse_funding_rate_update(&decoded.data)
                            {
                                let mut order_books = order_books_handle
                                    .write()
                                    .expect("Failed to lock order books for writing");
                                let entry =
                                    order_books.entry(symbol.clone()).or_insert_with(|| OrderBook {
                                        symbol,
                                        bids: Vec::new(),
                                        asks: Vec::new(),
                                        remote_timestamp: 0.0,
                                        argus_timestamp: 0.0,
                                        reserved: HashMap::new(),
                                    });
                                entry.reserved.insert(
                                    ReservedKey::FundingRate,
                                    ReservedValue::FundingRate(funding_rate),
                                );
                            }
                            market_event_handle.notify(usize::MAX);
                        } else {
                            pushed_messages_handle
                                .write()
                                .expect("Failed to lock pushed messages buffer for writing")
                                .add_message(decoded);
                            push_event_handle.notify(usize::MAX);
                        }
                    }
                    ProtocolKind::Protocol2 => {
                        let mut order_book = ProtocolFns::bytes_to_orderbook(&packet, None);
                        {
                            let mut order_books = order_books_handle
                                .write()
                                .expect("Failed to lock order books for writing");
                            if let Some(existing) = order_books.get(&order_book.symbol) {
                                order_book.reserved = existing.reserved.clone();
                            }
                            order_books.insert(order_book.symbol.clone(), order_book);
                        }
                        market_event_handle.notify(usize::MAX);
                    }
                }
            }
        });

        handle
    }

    fn send_message(&self, message: &[u8]) {
        let mut stream = self
            .write_stream_handle
            .lock()
            .expect("Failed to lock the write stream");
        if let Err(e) = stream.write_all(message) {
            eprintln!("Error sending message: {}", e);
        }
    }

    fn wait_for_response(
        &self,
        correlation_id: &str,
        action: &str,
        timeout: Duration,
    ) -> Result<InBoundMessage, String> {
        let start_time = std::time::Instant::now();

        loop {
            let listener = self.response_event.listen();

            let matching_msg: Option<(usize, InBoundMessage)> = {
                let buf = self
                    .response_buf
                    .read()
                    .expect("Failed to lock response buffer for reading");
                buf.iter().enumerate().find_map(|(index, msg)| {
                    if msg.correlation_id.as_deref() == Some(correlation_id) {
                        Some((index, msg.clone()))
                    } else {
                        None
                    }
                })
            };

            if let Some((index, msg)) = matching_msg {
                self.response_buf
                    .write()
                    .expect("Failed to lock response buffer for writing")
                    .remove(index);
                return Ok(msg);
            }

            let remaining = match timeout.checked_sub(start_time.elapsed()) {
                Some(d) if !d.is_zero() => d,
                _ => {
                    return Err(format!(
                        "Timeout waiting for response to action '{}'",
                        action
                    ));
                }
            };

            listener.wait_timeout(remaining);
        }
    }

    /// Sends a request to the dispatcher and blocks until a correlated response arrives,
    /// deserializing its `data` field into `T`.
    ///
    /// `action` is the dispatcher's command name (e.g. `"products_version"`). `data` is the
    /// request payload; pass `serde_json::json!({})` for actions that take no arguments.
    /// `timeout` defaults to 10 seconds if `None`. Returns `Err` if the dispatcher responds
    /// with an error, the request times out, or the response fails to deserialize as `T`.
    pub fn request<T: DeserializeOwned>(
        &self,
        action: &str,
        data: Value,
        timeout: Option<Duration>,
    ) -> Result<T, String> {
        let msg = OutBoundMessage::new(action.to_string(), data, None);
        let packet = ProtocolFns::protocol_1_encoder(&msg);
        self.send_message(&packet);

        let response = self.wait_for_response(
            &msg.correlation_id,
            action,
            timeout.unwrap_or(DEFAULT_TIMEOUT),
        )?;

        if let Some(error) = response.error {
            return Err(format!("Dispatcher error for action '{}': {}", action, error));
        }

        serde_json::from_value(response.data)
            .map_err(|e| format!("Failed to parse response for action '{}': {}", action, e))
    }

    /// Subscribes this connection to a set of instruments and blocks until the dispatcher confirms.
    ///
    /// `instruments` are the dispatcher-side keys: Hyperliquid coins (e.g. `"BTC"`, `"xyz:AAPL"`)
    /// or Lighter symbols (e.g. `"BTC"`). The dispatcher takes a JSON array in one `subscribe`
    /// request, so pass every instrument you want in a single call. Returns a
    /// [`SubscriptionResponse`] listing the instruments that were registered and any that failed
    /// (unknown instruments land in `failed` without failing the whole request).
    ///
    /// After a successful subscription the dispatcher streams Protocol 2 order book packets for
    /// these instruments; look them up in the map from [`get_order_book`](Self::get_order_book)
    /// keyed by the same instrument string, and await updates on the event from
    /// [`get_order_book_event`](Self::get_order_book_event). For each newly subscribed
    /// instrument, the dispatcher also sends this connection its current funding rate shortly
    /// after (0.1-1.0s randomized jitter) — see [`OrderBook::reserved`](crate::OrderBook::reserved).
    pub fn subscribe(&self, instruments: &[&str]) -> Result<SubscriptionResponse, String> {
        self.request("subscribe", serde_json::json!(instruments), None)
            .map_err(|e| format!("Failed to get subscription confirmation: {}", e))
    }

    /// Unsubscribes this connection from a set of instruments and blocks until the dispatcher confirms.
    ///
    /// Returns an [`UnsubscriptionResponse`] listing the instruments that were removed and any
    /// that failed. Any order book entries already in the map from
    /// [`get_order_book`](Self::get_order_book) are left in place (stale) rather than removed.
    pub fn unsubscribe(&self, instruments: &[&str]) -> Result<UnsubscriptionResponse, String> {
        self.request("unsubscribe", serde_json::json!(instruments), None)
            .map_err(|e| format!("Failed to get unsubscription confirmation: {}", e))
    }
}
