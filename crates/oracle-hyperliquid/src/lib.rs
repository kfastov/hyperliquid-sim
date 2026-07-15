//! Capability-safe, read-only Hyperliquid oracle ingestion.
//!
//! The upstream trait deliberately exposes market-data reads only. This crate
//! contains no wallet, signing, exchange, order, cancel, or transfer surface.

use hl_wire::{AssetId, DecimalScale, PriceTicks, WireValueError};
use serde::Deserialize;
use serde_json::Value;
use std::{error::Error, future::Future, pin::Pin};
use thiserror::Error;

/// Boxed future used by the object-safe read-only upstream boundary.
pub type UpstreamFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// The complete upstream capability available to the oracle.
///
/// Implementations can subscribe to public `activeAssetCtx` data and perform
/// the public `metaAndAssetCtxs` info query. State-changing methods cannot be
/// invoked through this boundary because none exist.
pub trait ReadOnlyUpstream: Send {
    type Error: Error + Send + Sync + 'static;

    fn next_active_asset_ctx(&mut self) -> UpstreamFuture<'_, Option<Value>, Self::Error>;

    fn meta_and_asset_ctxs(&mut self) -> UpstreamFuture<'_, Value, Self::Error>;
}

/// Explicit normalization scales for the three supported assets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PriceScales {
    btc: DecimalScale,
    eth: DecimalScale,
    sol: DecimalScale,
}

impl PriceScales {
    #[must_use]
    pub const fn new(btc: DecimalScale, eth: DecimalScale, sol: DecimalScale) -> Self {
        Self { btc, eth, sol }
    }

    #[must_use]
    pub const fn for_asset(self, asset: AssetId) -> DecimalScale {
        match asset {
            AssetId::BTC => self.btc,
            AssetId::ETH => self.eth,
            AssetId::SOL => self.sol,
        }
    }
}

/// A validated, normalized public oracle observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OracleObservation {
    pub asset: AssetId,
    pub price: PriceTicks,
    pub observed_at_ms: u64,
    pub upstream_sequence: u64,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum OracleParseError {
    #[error("malformed activeAssetCtx message")]
    Malformed,
    #[error("unexpected upstream channel")]
    UnexpectedChannel,
    #[error("unsupported oracle asset")]
    UnsupportedAsset,
    #[error("invalid oracle price: {0}")]
    InvalidPrice(WireValueError),
}

#[derive(Deserialize)]
struct ActiveAssetCtxMessage {
    channel: String,
    data: ActiveAssetCtxData,
}

#[derive(Deserialize)]
struct ActiveAssetCtxData {
    coin: String,
    ctx: ActiveAssetCtx,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ActiveAssetCtx {
    oracle_px: String,
}

/// Parses one public WebSocket `activeAssetCtx` update.
///
/// Symbol mapping is name-based rather than positional, preventing one asset's
/// price from being applied to another book when upstream ordering changes.
pub fn parse_active_asset_ctx(
    value: &Value,
    observed_at_ms: u64,
    upstream_sequence: u64,
    scales: PriceScales,
) -> Result<OracleObservation, OracleParseError> {
    let message: ActiveAssetCtxMessage =
        serde_json::from_value(value.clone()).map_err(|_| OracleParseError::Malformed)?;
    if message.channel != "activeAssetCtx" {
        return Err(OracleParseError::UnexpectedChannel);
    }
    let asset =
        AssetId::from_symbol(&message.data.coin).map_err(|_| OracleParseError::UnsupportedAsset)?;
    let price = PriceTicks::parse(&message.data.ctx.oracle_px, scales.for_asset(asset))
        .map_err(OracleParseError::InvalidPrice)?;
    Ok(OracleObservation { asset, price, observed_at_ms, upstream_sequence })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn scales() -> PriceScales {
        let scale = DecimalScale::new(2).expect("valid scale");
        PriceScales::new(scale, scale, scale)
    }

    #[test]
    fn parses_mocked_active_asset_ctx_oracle_prices_for_supported_assets() {
        for (sequence, symbol, expected_asset, price, expected_ticks) in [
            (1, "BTC", AssetId::BTC, "60123.45", 6_012_345),
            (2, "ETH", AssetId::ETH, "3123.40", 312_340),
            (3, "SOL", AssetId::SOL, "142.01", 14_201),
        ] {
            let message = json!({
                "channel": "activeAssetCtx",
                "data": {"coin": symbol, "ctx": {"oraclePx": price}}
            });
            let observation =
                parse_active_asset_ctx(&message, 10_000, sequence, scales()).expect("valid update");
            assert_eq!(observation.asset, expected_asset);
            assert_eq!(observation.price.value(), expected_ticks);
            assert_eq!(observation.observed_at_ms, 10_000);
            assert_eq!(observation.upstream_sequence, sequence);
        }
    }

    #[test]
    fn rejects_unknown_malformed_and_non_positive_updates() {
        let unknown = json!({
            "channel": "activeAssetCtx",
            "data": {"coin": "DOGE", "ctx": {"oraclePx": "1.00"}}
        });
        assert_eq!(
            parse_active_asset_ctx(&unknown, 0, 1, scales()),
            Err(OracleParseError::UnsupportedAsset)
        );

        let malformed = json!({
            "channel": "activeAssetCtx",
            "data": {"coin": "BTC", "ctx": {"oraclePx": 123}}
        });
        assert_eq!(
            parse_active_asset_ctx(&malformed, 0, 1, scales()),
            Err(OracleParseError::Malformed)
        );

        let zero = json!({
            "channel": "activeAssetCtx",
            "data": {"coin": "ETH", "ctx": {"oraclePx": "0"}}
        });
        assert_eq!(
            parse_active_asset_ctx(&zero, 0, 1, scales()),
            Err(OracleParseError::InvalidPrice(WireValueError::NonPositive))
        );
    }
}
