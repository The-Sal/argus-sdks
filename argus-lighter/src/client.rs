/*

Verified against Lighter dispatcher v1.0.0.0 (Argus "Phase 1.0 of v2",
https://github.com/The-Sal/Argus/pull/96). Only the read-only market-data actions currently
wired into the dispatcher's routing table are implemented here — trading and account-info
actions are not yet exposed by the dispatcher itself (see `argus/perpetuals/lighter/__init__.py`
in the Argus repo). This coverage is expected to grow as the dispatcher does.

Unlike Hyperliquid, Lighter has no HIP-3-style builder-deployed dexes — it is a single unified
exchange, so there is no `dex_name` parameter anywhere in this client.

*/

use serde_json::json;
use serde::Deserialize;
use argus_dispatcher_core::DispatcherConnection;
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
}
