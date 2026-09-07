use serde_json::Value;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use argus_dispatcher_core::{deserialize_f64_from_string, deserialize_optional_f64_from_string};


/// Response of the `products_version` action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductsVersion {
    pub argus: String,
    pub hyperliquid_dispatcher: [u32; 4],
    #[serde(default)]
    pub sidecars: HashMap<String, Value>,
}

/// A HIP-3 (builder-deployed) perp dex's deployer configuration, as returned by `get_dexs`.
///
/// Hyperliquid's own default dex (`dex_name = ""`) is not a `Dex` — it has no deployer
/// configuration and is not included in `get_dexs`'s results. Pass `""` directly to
/// `get_perpetuals_for_dex` to query it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dex {
    pub name: String,
    #[serde(rename = "fullName")]
    pub full_name: String,
    pub deployer: String,
    #[serde(rename = "oracleUpdater")]
    pub oracle_updater: Option<String>,
    /// Observed `null` live for at least one dex despite Argus's dataclass declaring it
    /// non-optional — treated as optional here to match reality rather than the type hint.
    #[serde(rename = "feeRecipient", default)]
    pub fee_recipient: Option<String>,
    /// (asset, streaming open-interest cap) pairs.
    #[serde(rename = "assetToStreamingOiCap")]
    pub asset_to_streaming_oi_cap: Vec<(String, String)>,
    /// (privileged action, addresses allowed to perform it) pairs.
    #[serde(rename = "subDeployers")]
    pub sub_deployers: Vec<(String, Vec<String>)>,
    /// (asset, funding multiplier) pairs.
    #[serde(rename = "assetToFundingMultiplier")]
    pub asset_to_funding_multiplier: Vec<(String, String)>,
    /// (asset, funding interest rate) pairs.
    #[serde(rename = "assetToFundingInterestRate")]
    pub asset_to_funding_interest_rate: Vec<(String, String)>,
}

/// Static metadata for one perpetual asset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    pub name: String,
    #[serde(rename = "szDecimals")]
    pub sz_decimals: u32,
    #[serde(rename = "maxLeverage")]
    pub max_leverage: u32,
    /// Present only on delisted / isolated-only assets.
    #[serde(rename = "onlyIsolated", default)]
    pub only_isolated: Option<bool>,
    #[serde(rename = "isDelisted", default)]
    pub is_delisted: Option<bool>,
    /// `"strictIsolated"` or `"noCross"` when set.
    #[serde(rename = "marginMode", default)]
    pub margin_mode: Option<String>,
    /// Present only on HIP-3 (builder-deployed) dex assets.
    #[serde(rename = "marginTableId", default)]
    pub margin_table_id: Option<u32>,
    #[serde(rename = "growthMode", default)]
    pub growth_mode: Option<String>,
    #[serde(rename = "lastGrowthModeChangeTime", default)]
    pub last_growth_mode_change_time: Option<String>,
}

impl Asset {
    /// True for builder-deployed (HIP-3) dex assets, whose names are namespaced as `"dex:COIN"`.
    pub fn is_hip3(&self) -> bool {
        self.name.contains(':')
    }
}

/// Live market data for one perpetual asset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetContext {
    #[serde(rename = "dayNtlVlm", deserialize_with = "deserialize_f64_from_string")]
    pub day_ntl_vlm: f64,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub funding: f64,
    #[serde(rename = "markPx", deserialize_with = "deserialize_f64_from_string")]
    pub mark_px: f64,
    #[serde(rename = "openInterest", deserialize_with = "deserialize_f64_from_string")]
    pub open_interest: f64,
    #[serde(rename = "oraclePx", deserialize_with = "deserialize_f64_from_string")]
    pub oracle_px: f64,
    #[serde(rename = "prevDayPx", deserialize_with = "deserialize_f64_from_string")]
    pub prev_day_px: f64,
    #[serde(rename = "impactPxs", default)]
    pub impact_pxs: Option<(String, String)>,
    #[serde(rename = "midPx", default, deserialize_with = "deserialize_optional_f64_from_string")]
    pub mid_px: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_optional_f64_from_string")]
    pub premium: Option<f64>,
    /// Present only on some HIP-3 dex assets.
    #[serde(rename = "dayBaseVlm", default, deserialize_with = "deserialize_optional_f64_from_string")]
    pub day_base_vlm: Option<f64>,
}

/// A single tradeable perpetual: its static metadata plus its live market data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Perpetual {
    /// `""` for Hyperliquid's own default dex, or a HIP-3 dex name.
    pub dex: String,
    pub asset: Asset,
    pub context: AssetContext,
}

impl Perpetual {
    pub fn mark_price(&self) -> f64 {
        self.context.mark_px
    }

    /// The current (hourly) funding rate, e.g. `0.0000125`.
    pub fn funding_rate(&self) -> f64 {
        self.context.funding
    }

    /// Naive annualized funding rate, assuming the current hourly rate holds constant.
    pub fn funding_rate_apr(&self) -> f64 {
        self.context.funding * 24.0 * 365.0
    }

    pub fn open_interest_usd(&self) -> f64 {
        self.context.open_interest * self.context.mark_px
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Annotation {
    pub category: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConciseAnnotation {
    pub category: String,
    #[serde(default)]
    pub keywords: Vec<String>,
}

/// One external venue's predicted next funding rate for a coin, benchmarked against Hyperliquid.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredictedFundingVenue {
    #[serde(rename = "fundingRate", default, deserialize_with = "deserialize_optional_f64_from_string")]
    pub funding_rate: Option<f64>,
    #[serde(rename = "nextFundingTime", default)]
    pub next_funding_time: Option<i64>,
}

/// Response of the `perpetual_info` action: annotation/category/keywords/predicted-funding
/// metadata for one coin. Does not include live market data (mark price, funding rate, open
/// interest) — see [`Perpetual`] for that.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerpetualInfo {
    pub coin: String,
    pub annotation: Option<Annotation>,
    pub category: Option<String>,
    pub concise_annotation: Option<ConciseAnnotation>,
    /// (venue name, predicted funding data) pairs — one per external venue Hyperliquid
    /// benchmarks against. Only populated for default-dex coins (e.g. `"BTC"`).
    pub predicted_funding: Option<Vec<(String, Option<PredictedFundingVenue>)>>,
}
