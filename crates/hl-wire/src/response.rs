//! Minimal response and event DTOs for `sim-header-v1`.

use crate::{AssetId, SimUserId};
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use std::{collections::BTreeMap, fmt, str::FromStr};

use crate::api::{Cloid, ErrorDetail, PositiveDecimal, Subscription};

/// A canonical signed decimal string used only at external API boundaries.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct CanonicalDecimal(String);

impl CanonicalDecimal {
    pub fn parse(value: &str) -> Result<Self, CanonicalDecimalError> {
        let unsigned = value.strip_prefix('-').unwrap_or(value);
        if unsigned.is_empty()
            || value.starts_with('+')
            || (value.starts_with('-') && unsigned == "0")
            || !is_canonical_unsigned_decimal(unsigned)
        {
            return Err(CanonicalDecimalError);
        }
        Ok(Self(value.to_owned()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for CanonicalDecimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::parse(&String::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

impl fmt::Display for CanonicalDecimal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for CanonicalDecimal {
    type Err = CanonicalDecimalError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalDecimalError;

impl fmt::Display for CanonicalDecimalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid canonical decimal string")
    }
}

impl std::error::Error for CanonicalDecimalError {}

/// Metadata response. Universe order defines compact exchange asset IDs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetaResponse {
    pub universe: Vec<AssetMeta>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AssetMeta {
    pub name: AssetId,
    pub sz_decimals: u8,
}

/// Per-asset oracle context plus simulator freshness metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AssetContext {
    pub oracle_px: PositiveDecimal,
    pub mid_px: Option<PositiveDecimal>,
    pub observed_at: u64,
    pub is_stale: bool,
}

/// Hyperliquid-shaped tuple returned by `metaAndAssetCtxs`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetaAndAssetCtxsResponse(pub MetaResponse, pub Vec<AssetContext>);

/// Supported-symbol midpoint map.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AllMidsResponse(pub BTreeMap<AssetId, PositiveDecimal>);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct L2BookResponse {
    pub coin: AssetId,
    pub time: u64,
    pub levels: [Vec<BookLevel>; 2],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BookLevel {
    pub px: PositiveDecimal,
    pub sz: PositiveDecimal,
    pub n: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OpenOrder {
    pub coin: AssetId,
    pub limit_px: PositiveDecimal,
    pub oid: u64,
    pub side: OrderSide,
    pub sz: PositiveDecimal,
    pub timestamp: u64,
    pub orig_sz: PositiveDecimal,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloid: Option<Cloid>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OrderSide {
    B,
    A,
}

/// Deliberately simplified synthetic account state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClearinghouseStateResponse {
    pub user: SimUserId,
    pub margin_summary: MarginSummary,
    pub asset_positions: Vec<AssetPosition>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MarginSummary {
    pub account_value: CanonicalDecimal,
    pub total_ntl_pos: CanonicalDecimal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssetPosition {
    pub position: Position,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Position {
    pub coin: AssetId,
    pub szi: CanonicalDecimal,
    pub entry_px: Option<PositiveDecimal>,
}

/// Hyperliquid-shaped successful exchange response.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExchangeResponse {
    pub status: ExchangeResponseStatus,
    pub response: ExchangeResponseData,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExchangeResponseStatus {
    Ok,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "camelCase", deny_unknown_fields)]
pub enum ExchangeResponseData {
    Order(StatusList<OrderStatus>),
    Cancel(StatusList<CancelStatus>),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusList<T> {
    pub statuses: Vec<T>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OrderStatus {
    Resting(RestingOrderStatus),
    Filled(FilledOrderStatus),
    Error(ItemErrorStatus),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestingOrderStatus {
    pub resting: RestingOrderData,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestingOrderData {
    pub oid: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilledOrderStatus {
    pub filled: FilledOrderData,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FilledOrderData {
    pub total_sz: PositiveDecimal,
    pub avg_px: PositiveDecimal,
    pub oid: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ItemErrorStatus {
    pub error: ErrorDetail,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CancelStatus {
    Success(CancelSuccess),
    Error(ItemErrorStatus),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CancelSuccess {
    Success,
}

/// Explicit subscription acknowledgement body.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionAck {
    pub method: SubscriptionMethod,
    pub subscription: Subscription,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SubscriptionMethod {
    Subscribe,
    Unsubscribe,
}

/// Server control messages (`subscriptionResponse` and `pong`).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "channel", rename_all = "camelCase", deny_unknown_fields)]
pub enum WsControlResponse {
    SubscriptionResponse { data: SubscriptionAck },
    Pong {},
}

/// Supported event channel names.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EventChannel {
    AllMids,
    L2Book,
    Trades,
    OrderUpdates,
}

/// Sequenced channel envelope. Adapters select the payload type for the channel.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelEnvelope<T> {
    pub channel: EventChannel,
    pub sequence: u64,
    pub data: T,
}

fn is_canonical_unsigned_decimal(value: &str) -> bool {
    let mut pieces = value.split('.');
    let whole = pieces.next().unwrap_or_default();
    let fraction = pieces.next();
    if pieces.next().is_some()
        || whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || (whole.len() > 1 && whole.starts_with('0'))
    {
        return false;
    }
    match fraction {
        None => true,
        Some(fraction) => {
            !fraction.is_empty()
                && fraction.bytes().all(|byte| byte.is_ascii_digit())
                && !fraction.ends_with('0')
        }
    }
}
