/*

Verified against Hyperliquid dispatcher v1.0.0.0 (Argus "Phase 1.0 of v2",
https://github.com/The-Sal/Argus/pull/96). Only the read-only market-data actions currently
wired into the dispatcher's routing table are implemented here — trading and account-info
actions are not yet exposed by the dispatcher itself (see `argus/perpetuals/hyper/__init__.py`
in the Argus repo). This coverage is expected to grow as the dispatcher does.

Streaming is supported via the dispatcher's `subscribe`/`unsubscribe` actions: after subscribing
to coins, the dispatcher pushes Protocol 2 order book snapshots on the same TCP connection. See
[`HyperliquidClient::get_order_book`] and [`HyperliquidClient::get_order_book_event`].

*/

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use serde_json::json;
use serde::Deserialize;
use argus_dispatcher_core::{
    DispatcherConnection, Event, OrderBook, PushedMessages, SubscriptionResponse,
    UnsubscriptionResponse,
};
use crate::models::{Dex, Perpetual, PerpetualInfo, ProductsVersion};



/// A client for the Argus Hyperliquid perpetuals dispatcher.
pub struct HyperliquidClient {
    conn: DispatcherConnection,
}

impl HyperliquidClient {
    /// Opens a blocking TCP connection to the Hyperliquid dispatcher at `address`
    /// (e.g. `"localhost:9972"`, the dispatcher's default port) and starts its background I/O
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

    /// Lists HIP-3 (builder-deployed) perp dexes.
    ///
    /// Hyperliquid's own default dex (`dex_name = ""`) is not included in this list — pass
    /// `""` directly to [`get_perpetuals_for_dex`](Self::get_perpetuals_for_dex) to query it.
    pub fn get_dexs(&self) -> Result<Vec<Dex>, String> {
        #[derive(Deserialize)]
        struct Response {
            dexes: Vec<Dex>,
        }
        let response: Response = self.conn.request("get_dexs", json!({}), None)?;
        Ok(response.dexes)
    }

    /// Returns a page of perpetuals for a dex.
    ///
    /// Pass `""` for Hyperliquid's default dex, or a HIP-3 dex name from
    /// [`get_dexs`](Self::get_dexs). `limit` defaults to the dispatcher's own default
    /// (currently 100) if `None`.
    pub fn get_perpetuals_for_dex(
        &self,
        dex_name: &str,
        offset: u32,
        limit: Option<u32>,
    ) -> Result<Vec<Perpetual>, String> {
        #[derive(Deserialize)]
        struct Response {
            perpetuals: Vec<Perpetual>,
        }
        let mut data = json!({ "dex_name": dex_name, "offset": offset });
        if let Some(limit) = limit {
            data["limit"] = json!(limit);
        }
        let response: Response = self
            .conn
            .request("get_perpetuals_for_dex", data, None)?;
        Ok(response.perpetuals)
    }

    /// Returns a page of perpetuals across all dexes, sorted by (hourly) funding rate descending.
    ///
    /// `limit` defaults to 20 if `None`.
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

    /// Returns annotation/category/keywords/predicted-funding metadata for one coin.
    ///
    /// `coin` is e.g. `"BTC"` for the default dex, or `"xyz:AAPL"` for a HIP-3 dex asset. Does
    /// not include live market data (mark price, funding rate, open interest) — use
    /// [`get_perpetuals_for_dex`](Self::get_perpetuals_for_dex) or
    /// [`get_funding_rates_for_all_perpetuals`](Self::get_funding_rates_for_all_perpetuals)
    /// for that.
    ///
    /// Per Hyperliquid's API, annotation metadata (`annotation`, `category`,
    /// `concise_annotation`) is only populated for HIP-3 coins, while `predicted_funding` is
    /// only populated for default-dex coins. The other fields are `None` in each case.
    pub fn perpetual_info(&self, coin: &str) -> Result<PerpetualInfo, String> {
        self.conn
            .request("perpetual_info", json!({ "coin": coin }), None)
    }

    /// Subscribes to order book streaming for one or more coins and blocks until the dispatcher
    /// confirms (up to the default 10 second timeout).
    ///
    /// `coins` are Hyperliquid coin names (e.g. `["BTC", "ETH"]` for the default dex, or
    /// `["xyz:AAPL"]` for a HIP-3 dex asset). Returns a [`SubscriptionResponse`] listing the
    /// coins that were registered and any that failed — an unknown coin lands in `failed`
    /// without failing the whole request.
    ///
    /// After a successful subscription the dispatcher streams Protocol 2 order book snapshots
    /// for these coins. Register a listener on [`get_order_book_event`](Self::get_order_book_event)
    /// *before* reading the map from [`get_order_book`](Self::get_order_book), then wait on the
    /// listener to be woken on each update.
    pub fn subscribe(&self, coins: &[&str]) -> Result<SubscriptionResponse, String> {
        self.conn.subscribe(coins)
    }

    /// Unsubscribes from order book streaming for one or more coins and blocks until the
    /// dispatcher confirms.
    ///
    /// Returns an [`UnsubscriptionResponse`] listing the coins that were removed and any that
    /// failed. Any order book entries already in the map from
    /// [`get_order_book`](Self::get_order_book) are left in place (stale) rather than removed.
    pub fn unsubscribe(&self, coins: &[&str]) -> Result<UnsubscriptionResponse, String> {
        self.conn.unsubscribe(coins)
    }

    /// Returns a shared handle to the live order book map.
    ///
    /// Populated by Protocol 2 packets for coins subscribed via [`subscribe`](Self::subscribe)
    /// and keyed by the same coin string. Every packet overwrites the entry for that coin in
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
    /// use argus_hyperliquid::{HyperliquidClient, Listener};
    ///
    /// # fn main() {
    /// let client = HyperliquidClient::connect("localhost:9972");
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
