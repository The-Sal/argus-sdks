/*

Verified against Hyperliquid dispatcher v1.0.0.0 (Argus "Phase 1.0 of v2",
https://github.com/The-Sal/Argus/pull/96). Only the read-only market-data actions currently
wired into the dispatcher's routing table are implemented here — trading and account-info
actions are not yet exposed by the dispatcher itself (see `argus/perpetuals/hyper/__init__.py`
in the Argus repo). This coverage is expected to grow as the dispatcher does.

*/

use serde_json::json;
use serde::Deserialize;
use argus_dispatcher_core::DispatcherConnection;
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
    pub fn perpetual_info(&self, coin: &str) -> Result<PerpetualInfo, String> {
        self.conn
            .request("perpetual_info", json!({ "coin": coin }), None)
    }
}
