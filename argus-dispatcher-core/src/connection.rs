/*

Generic P1 wire-protocol client for Argus dispatchers. Handles the TCP connection, background
I/O threading, and correlation-id request/response matching only — it has no exchange-specific
types or actions. See the argus-hyperliquid and argus-lighter crates for typed clients built on
top of `DispatcherConnection::request`.

Both the Hyperliquid and Lighter dispatchers are early-stage (read-only market data today), but
this connection keeps the same two-thread architecture Polymarket's dispatcher client uses —
including a buffer for unsolicited server pushes — so it doesn't need a rewrite once either
dispatcher grows streaming/subscription actions.

*/

use serde_json::Value;
use std::net::TcpStream;
use std::time::Duration;
use std::io::{Read, Write};
use serde::de::DeserializeOwned;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{spawn, JoinHandle};
use event_listener::{Event, Listener};
use crossbeam::channel::{unbounded, Receiver, Sender};
use crate::protocol::{InBoundMessage, OutBoundMessage, ProtocolFns};


const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Buffer of messages pushed by the dispatcher that were not solicited by a specific request
/// (i.e. arrived without a `correlation_id`). Neither the Hyperliquid nor Lighter dispatcher
/// pushes anything today, but the P1 protocol supports it and this keeps the plumbing ready.
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

/// A connection to an Argus dispatcher over its P1 TCP wire protocol.
///
/// Construct with [`DispatcherConnection::new`], call [`start`](Self::start) once to spawn the
/// background I/O threads, then issue requests with [`request`](Self::request).
#[derive(Debug)]
pub struct DispatcherConnection {
    read_stream_handle: Arc<RwLock<TcpStream>>,
    write_stream_handle: Arc<Mutex<TcpStream>>,
    pushed_messages: Arc<RwLock<PushedMessages>>,
    push_event: Arc<Event>,
    response_buf: Arc<RwLock<Vec<InBoundMessage>>>,
    response_event: Arc<Event>,
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
        }
    }

    /// Returns a shared handle to the buffer of unsolicited dispatcher pushes.
    ///
    /// Empty and unused by both the Hyperliquid and Lighter dispatchers today — reserved for
    /// when either grows push-style actions (account updates, streaming ticks, etc.).
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
    /// - **Reading thread** — reads bytes into a rolling buffer, detects complete P1 packets by
    ///   their terminator byte, and forwards them to the processing thread via a crossbeam channel.
    /// - **Processing thread** — decodes each packet and decompresses it if needed. Packets that
    ///   carry a `correlation_id` are placed in the response-matching buffer for [`request`](Self::request)
    ///   to pick up; packets without one are treated as unsolicited pushes.
    ///
    /// The returned `JoinHandle` belongs to the reading thread. The processing thread is detached.
    /// Both threads run indefinitely; the reading thread panics if the dispatcher closes the connection.
    pub fn start(&mut self) -> JoinHandle<()> {
        let read_handle = Arc::clone(&self.read_stream_handle);
        let response_buf_handle = Arc::clone(&self.response_buf);
        let response_event_handle = Arc::clone(&self.response_event);
        let pushed_messages_handle = Arc::clone(&self.pushed_messages);
        let push_event_handle = Arc::clone(&self.push_event);

        let (sender, receiver): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = unbounded();

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
                                if *byte == 0x7D && ProtocolFns::analyse_bytes(&full_packet_buffer).is_ok() {
                                    sender
                                        .send(full_packet_buffer.clone())
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
            while let Ok(packet) = receiver.recv() {
                let mut decoded = ProtocolFns::protocol_1_decoder(&packet);
                ProtocolFns::maybe_decompress_p1(&mut decoded);

                if decoded.correlation_id.is_some() {
                    response_buf_handle
                        .write()
                        .expect("Failed to lock response buffer for writing")
                        .push(decoded);
                    response_event_handle.notify(usize::MAX);
                } else {
                    pushed_messages_handle
                        .write()
                        .expect("Failed to lock pushed messages buffer for writing")
                        .add_message(decoded);
                    push_event_handle.notify(usize::MAX);
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
}
