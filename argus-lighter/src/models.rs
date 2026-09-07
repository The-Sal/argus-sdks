use serde_json::Value;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

use argus_dispatcher_core::{
    deserialize_f64_from_string, deserialize_optional_f64_from_string, deserialize_u64_from_string,
};

/// Response of the `products_version` action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductsVersion {
    pub argus: String,
    pub lighter_dispatcher: [u32; 4],
    #[serde(default)]
    pub sidecars: HashMap<String, Value>,
}

/// The nested `market_config` object of a market entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketConfig {
    pub market_margin_mode: i64,
    pub insurance_fund_account_index: i64,
    pub liquidation_mode: i64,
    pub force_reduce_only: bool,
    pub trading_hours: String,
    pub funding_fee_discounts_enabled: bool,
    pub hidden: bool,
    pub rfq_enabled: bool,
}

/// Static-ish metadata for one Lighter market: fees, size limits, margin tiers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Market {
    pub symbol: String,
    pub market_id: i64,
    /// `"perp"` or `"spot"`.
    pub market_type: String,
    pub base_asset_id: i64,
    pub quote_asset_id: i64,
    /// `"active"` or `"inactive"`.
    pub status: String,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub taker_fee: f64,
    pub is_taker_fee_enabled: bool,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub maker_fee: f64,
    pub is_maker_fee_enabled: bool,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub liquidation_fee: f64,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub min_base_amount: f64,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub min_quote_amount: f64,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub order_quote_limit: f64,
    pub supported_size_decimals: i64,
    pub supported_price_decimals: i64,
    pub supported_quote_decimals: i64,
    #[serde(rename = "created_at", deserialize_with = "deserialize_u64_from_string")]
    pub created_at_ms: u64,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub multiplier: f64,
    pub size_decimals: i64,
    pub price_decimals: i64,
    pub quote_multiplier: i64,
    pub default_initial_margin_fraction: i64,
    pub min_initial_margin_fraction: i64,
    pub maintenance_margin_fraction: i64,
    pub closeout_margin_fraction: i64,
    pub market_config: MarketConfig,
    pub strategy_index: i64,
    pub market_flags: i64,
    pub funding_premium_multiplier: i64,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub funding_clamp_small: f64,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub funding_clamp_big: f64,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub base_interest_rate: f64,
}

impl Market {
    pub fn is_active(&self) -> bool {
        self.status == "active"
    }

    pub fn is_perp(&self) -> bool {
        self.market_type == "perp"
    }
}

/// Live market data for one market, returned alongside [`Market`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketContext {
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub mark_price: f64,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub index_price: f64,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub last_trade_price: f64,
    pub daily_trades_count: i64,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub daily_base_token_volume: f64,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub daily_quote_token_volume: f64,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub daily_price_low: f64,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub daily_price_high: f64,
    /// A percentage, e.g. `-4.97` means -4.97%, not a fraction.
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub daily_price_change: f64,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub open_interest: f64,
    /// Shape undocumented upstream; always an empty object observed live.
    #[serde(default)]
    pub daily_chart: Value,
}

/// A single tradeable Lighter market: its metadata, live data, and (if attached) funding rate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Perpetual {
    pub market: Market,
    pub context: MarketContext,
    #[serde(default, deserialize_with = "deserialize_optional_f64_from_string")]
    pub funding_rate: Option<f64>,
}

impl Perpetual {
    pub fn mark_price(&self) -> f64 {
        self.context.mark_price
    }

    pub fn open_interest_usd(&self) -> f64 {
        self.context.open_interest * self.context.mark_price
    }

    /// Naive annualized funding rate, assuming the current rate holds constant and settles
    /// hourly (per Lighter's funding docs, not an API field). `None` if no funding rate is
    /// attached to this perpetual.
    pub fn funding_rate_apr(&self) -> Option<f64> {
        self.funding_rate.map(|r| r * 24.0 * 365.0)
    }
}

/// One entry of historical funding for a market. `market_id` is not part of the wire payload
/// (Lighter's `/fundings` response doesn't echo it back per-entry) — it's the value you passed
/// to [`crate::LighterClient::get_funding_history`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingHistoryEntry {
    /// Unix seconds.
    pub timestamp: i64,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub value: f64,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub rate: f64,
    /// `"long"` or `"short"`.
    pub direction: String,
}
