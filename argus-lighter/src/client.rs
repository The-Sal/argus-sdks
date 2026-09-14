/*

Verified against Lighter dispatcher v1.0.0.0 (Argus "Phase 1.0 of v2",
https://github.com/The-Sal/Argus/pull/96). Only the read-only market-data actions currently
wired into the dispatcher's routing table are implemented here — trading and account-info
actions are not yet exposed by the dispatcher itself (see `argus/perpetuals/lighter/__init__.py`
in the Argus repo). This coverage is expected to grow as the dispatcher does.

Unlike Hyperliquid, Lighter has no HIP-3-style builder-deployed dexes — it is a single unified
exchange, so there is no `dex_name` parameter anywhere in this client.

Streaming is supported via the dispatcher's `subscribe`/`unsubscribe` actions: after subscribing
to symbols, the dispatcher pushes Protocol 2 order book snapshots on the same TCP connection.
See [`LighterClient::get_order_book`] and [`LighterClient::get_order_book_event`].

*/

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use serde_json::json;
use serde::Deserialize;
use argus_dispatcher_core::{
    DispatcherConnection, Event, OrderBook, PushedMessages, SubscriptionResponse,
    UnsubscriptionResponse,
};
use crate::models::{FundingHistoryEntry, Perpetual, ProductsVersion};



/// A client for the Argus Lighter perpetuals dispatcher.
pub struct LighterClient {
    conn: DispatcherConnection,
}

impl LighterClient {
    /// Opens a blocking TCP connection to the Lighter dispatcher at `address`
    /// (e.g. `"localhost:9974"`, the dispatcher's default port) and starts its background I/O
    /// threads. Panics if the connection cannot be established.
    pub fn connect(address: &str) -> Self {
        let mut conn = DispatcherConnection::new(address);
        conn.start();
        Self { conn }
    }

    /// Returns dispatcher/component version info.
    pub fn products_version(&self) -> Result<ProductsVersion, String> {
        self.conn.request("products_version", json!({}), None)
    }

    /// Returns a page of all perpetual markets (a single unified list — Lighter has no
    /// per-dex venues). `limit` defaults to 10 if `None`.
    pub fn get_markets(&self, offset: u32, limit: Option<u32>) -> Result<Vec<Perpetual>, String> {
        #[derive(Deserialize)]
        struct Response {
            perpetuals: Vec<Perpetual>,
        }
        let mut data = json!({ "offset": offset });
        if let Some(limit) = limit {
            data["limit"] = json!(limit);
        }
        let response: Response = self.conn.request("get_markets", data, None)?;
        Ok(response.perpetuals)
    }

    /// Returns a page of markets sorted by funding rate descending. `limit` defaults to 20 if `None`.
    pub fn get_funding_rates_for_all_perpetuals(
        &self,
        offset: u32,
        limit: Option<u32>,
    ) -> Result<Vec<Perpetual>, String> {
        #[derive(Deserialize)]
        struct Response {
            funding_rates: Vec<Perpetual>,
        }
        let mut data = json!({ "offset": offset });
        if let Some(limit) = limit {
            data["limit"] = json!(limit);
        }
        let response: Response = self
            .conn
            .request("get_funding_rates_for_all_perpetuals", data, None)?;
        Ok(response.funding_rates)
    }

    /// Returns metadata + live data for a single market, looked up by `symbol` (e.g. `"BTC"`) or
    /// `market_id`. If both are `Some`, `symbol` takes precedence. Returns `Ok(None)` if no
    /// market matches — this is a normal "not found" result, not an error.
    pub fn market_info(
        &self,
        symbol: Option<&str>,
        market_id: Option<i64>,
    ) -> Result<Option<Perpetual>, String> {
        #[derive(Deserialize)]
        struct Response {
            perpetual: Option<Perpetual>,
        }
        let data = match (symbol, market_id) {
            (Some(symbol), _) => json!({ "symbol": symbol }),
            (None, Some(market_id)) => json!({ "market_id": market_id }),
            (None, None) => return Err("market_info requires 'symbol' or 'market_id'".to_string()),
        };
        let response: Response = self.conn.request("market_info", data, None)?;
        Ok(response.perpetual)
    }

    /// Returns historical funding for a single market between `start_timestamp` (unix seconds,
    /// required) and `end_timestamp` (defaults to now if `None`). `resolution` defaults to
    /// `"1h"` if `None`.
    pub fn get_funding_history(
        &self,
        market_id: i64,
        start_timestamp: i64,
        end_timestamp: Option<i64>,
        resolution: Option<&str>,
    ) -> Result<Vec<FundingHistoryEntry>, String> {
        #[derive(Deserialize)]
        struct Response {
            funding_history: Vec<FundingHistoryEntry>,
        }
        let mut data = json!({
            "market_id": market_id,
            "start_timestamp": start_timestamp,
            "resolution": resolution.unwrap_or("1h"),
        });
        if let Some(end_timestamp) = end_timestamp {
            data["end_timestamp"] = json!(end_timestamp);
        }
        let response: Response = self.conn.request("get_funding_history", data, None)?;
        Ok(response.funding_history)
    }

    /// Subscribes to order book streaming for one or more markets and blocks until the dispatcher
    /// confirms (up to the default 10 second timeout).
    ///
    /// `symbols` are Lighter market symbols (e.g. `["BTC", "ETH"]`). Returns a
    /// [`SubscriptionResponse`] listing the symbols that were registered and any that failed —
    /// an unknown symbol lands in `failed` without failing the whole request.
    ///
    /// After a successful subscription the dispatcher streams Protocol 2 order book snapshots
    /// for these symbols. Register a listener on [`get_order_book_event`](Self::get_order_book_event)
    /// *before* reading the map from [`get_order_book`](Self::get_order_book), then wait on the
    /// listener to be woken on each update.
    pub fn subscribe(&self, symbols: &[&str]) -> Result<SubscriptionResponse, String> {
        self.conn.subscribe(symbols)
    }

    /// Unsubscribes from order book streaming for one or more markets and blocks until the
    /// dispatcher confirms.
    ///
    /// Returns an [`UnsubscriptionResponse`] listing the symbols that were removed and any that
    /// failed. Any order book entries already in the map from
    /// [`get_order_book`](Self::get_order_book) are left in place (stale) rather than removed.
    pub fn unsubscribe(&self, symbols: &[&str]) -> Result<UnsubscriptionResponse, String> {
        self.conn.unsubscribe(symbols)
    }

    /// Returns a shared handle to the live order book map.
    ///
    /// Populated by Protocol 2 packets for symbols subscribed via [`subscribe`](Self::subscribe)
    /// and keyed by the same symbol string. Every packet overwrites the entry for that symbol in
    /// place, so a read lock always sees the most recent snapshot. Levels the exchange did not
    /// send are zero-padded — filter on `quantity > 0.0` when iterating a side.
    pub fn get_order_book(&self) -> Arc<RwLock<HashMap<String, OrderBook>>> {
        self.conn.get_order_book()
    }

    /// Returns a shared handle to the order book notification event.
    ///
    /// The background processing thread notifies this [`Event`] on every Protocol 2 packet.
    /// Register a listener *before* reading [`get_order_book`](Self::get_order_book):
    ///
    /// ```no_run
    /// use argus_lighter::{LighterClient, Listener};
    ///
    /// # fn main() {
    /// let client = LighterClient::connect("localhost:9974");
    /// let books = client.get_order_book();
    /// let event = client.get_order_book_event();
    ///
    /// let listener = event.listen();          // register first
    /// let snapshot = books.read().unwrap();   // then read
    /// // ... use snapshot ...
    /// drop(snapshot);
    /// listener.wait();                        // block until the next update
    /// # }
    /// ```
    ///
    /// Note the event fires for *every* symbol, not just the one you care about — re-check your
    /// symbol in the map after each wakeup.
    pub fn get_order_book_event(&self) -> Arc<Event> {
        self.conn.get_order_book_event()
    }

    /// Returns a shared handle to the buffer of unsolicited Protocol 1 pushes from the dispatcher
    /// (e.g. notifications or fatal errors), drained via [`PushedMessages::drain`].
    pub fn get_pushed_messages(&self) -> Arc<RwLock<PushedMessages>> {
        self.conn.get_pushed_messages()
    }

    /// Returns a shared handle to the event notified whenever an unsolicited Protocol 1 push arrives.
    pub fn get_push_event(&self) -> Arc<Event> {
        self.conn.get_push_event()
    }
}
