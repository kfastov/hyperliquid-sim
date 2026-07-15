//! Strict, Hyperliquid-shaped request and control DTOs for `sim-header-v1`.
//!
//! These types validate wire syntax only. In particular, signature-shaped fields
//! are never authenticated and payload user fields never establish identity.

use crate::{AssetId, SimUserId};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use std::{fmt, str::FromStr};
use thiserror::Error;

/// Stable boundary error categories from the compatibility profile.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    InvalidRequest,
    Unsupported,
    UnauthorizedSimUser,
    DomainReject,
    OracleStale,
    Overloaded,
    Internal,
}

/// Stable JSON error body.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorDetail {
    pub category: ErrorCategory,
    pub message: String,
}

/// Stable JSON error envelope returned by HTTP and WebSocket adapters.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorEnvelope {
    pub error: ErrorDetail,
}

impl ErrorEnvelope {
    #[must_use]
    pub fn new(category: ErrorCategory, message: impl Into<String>) -> Self {
        Self { error: ErrorDetail { category, message: message.into() } }
    }
}

/// Numeric asset ID used in compact exchange payloads.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiAssetId(AssetId);

impl ApiAssetId {
    #[must_use]
    pub const fn new(asset: AssetId) -> Self {
        Self(asset)
    }

    #[must_use]
    pub const fn asset(self) -> AssetId {
        self.0
    }
}

impl From<AssetId> for ApiAssetId {
    fn from(asset: AssetId) -> Self {
        Self::new(asset)
    }
}

impl Serialize for ApiAssetId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u8(self.0.value())
    }
}

impl<'de> Deserialize<'de> for ApiAssetId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        AssetId::try_from(u8::deserialize(deserializer)?).map(Self).map_err(D::Error::custom)
    }
}

/// A positive canonical decimal string, without scale interpretation.
///
/// Adapters remain responsible for exact symbol-specific tick/lot conversion.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct PositiveDecimal(String);

impl PositiveDecimal {
    pub fn parse(value: &str) -> Result<Self, ApiValueError> {
        if !is_canonical_decimal(value) || value == "0" {
            return Err(ApiValueError::InvalidPositiveDecimal);
        }
        Ok(Self(value.to_owned()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for PositiveDecimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::parse(&String::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

impl fmt::Display for PositiveDecimal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for PositiveDecimal {
    type Err = ApiValueError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

/// A normalized 16-byte client order ID (`0x` plus 32 lowercase hex digits).
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct Cloid(String);

impl Cloid {
    pub fn parse(value: &str) -> Result<Self, ApiValueError> {
        if !is_lower_hex(value, 32) {
            return Err(ApiValueError::InvalidCloid);
        }
        Ok(Self(value.to_owned()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for Cloid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::parse(&String::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

/// A normalized 32-byte signature component (`0x` plus 64 lowercase hex digits).
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SignatureComponent(String);

impl SignatureComponent {
    pub fn parse(value: &str) -> Result<Self, ApiValueError> {
        if !is_lower_hex(value, 64) {
            return Err(ApiValueError::InvalidSignatureComponent);
        }
        Ok(Self(value.to_owned()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for SignatureComponent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::parse(&String::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

/// Signature-shaped compatibility fields. No authentication is performed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Signature {
    pub r: SignatureComponent,
    pub s: SignatureComponent,
    pub v: u8,
}

/// Supported `/info` request variants.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum InfoRequest {
    Meta {},
    MetaAndAssetCtxs {},
    AllMids {},
    L2Book {
        coin: AssetId,
    },
    OpenOrders {
        user: SimUserId,
    },
    #[serde(rename = "clearinghouseState")]
    ClearinghouseState {
        user: SimUserId,
    },
}

/// Supported `/exchange` envelope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExchangeEnvelope {
    pub action: ExchangeAction,
    pub nonce: u64,
    pub signature: Signature,
    pub vault_address: Option<SimUserId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_after: Option<u64>,
}

/// Supported exchange actions. Unsupported action tags fail deserialization.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum ExchangeAction {
    Order { orders: Vec<OrderRequest>, grouping: Grouping },
    Cancel { cancels: Vec<CancelRequest> },
}

/// One compact limit-order request with exact Hyperliquid field names.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderRequest {
    #[serde(rename = "a")]
    pub asset: ApiAssetId,
    #[serde(rename = "b")]
    pub is_buy: bool,
    #[serde(rename = "p")]
    pub limit_px: PositiveDecimal,
    #[serde(rename = "s")]
    pub size: PositiveDecimal,
    #[serde(rename = "r")]
    pub reduce_only: bool,
    #[serde(rename = "t")]
    pub order_type: OrderType,
    #[serde(rename = "c", default, skip_serializing_if = "Option::is_none")]
    pub cloid: Option<Cloid>,
}

/// Only limit orders are in profile; trigger/market variants are rejected.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum OrderType {
    Limit(LimitOrderType),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitOrderType {
    pub tif: TimeInForce,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TimeInForce {
    Gtc,
    Ioc,
    Alo,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Grouping {
    #[serde(rename = "na")]
    Na,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelRequest {
    #[serde(rename = "a")]
    pub asset: ApiAssetId,
    #[serde(rename = "o")]
    pub order_id: u64,
}

/// Supported WebSocket client control messages.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "camelCase", deny_unknown_fields)]
pub enum WsRequest {
    Subscribe { subscription: Subscription },
    Unsubscribe { subscription: Subscription },
    Ping {},
}

/// Supported WebSocket subscriptions.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum Subscription {
    AllMids {},
    L2Book { coin: AssetId },
    Trades { coin: AssetId },
    OrderUpdates { user: SimUserId },
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ApiValueError {
    #[error("invalid positive canonical decimal string")]
    InvalidPositiveDecimal,
    #[error("invalid client order ID")]
    InvalidCloid,
    #[error("invalid signature component")]
    InvalidSignatureComponent,
}

fn is_canonical_decimal(value: &str) -> bool {
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

fn is_lower_hex(value: &str, digits: usize) -> bool {
    value.len() == digits + 2
        && value.starts_with("0x")
        && value.as_bytes()[2..]
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
}
