//! Validated wire-boundary values for the simulator.
//!
//! Decimal strings are converted exactly to integer ticks/lots. This crate has
//! no exchange state and performs no identity inference or network activity.

pub mod api;

use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use std::{fmt, str::FromStr};
use thiserror::Error;

/// The only supported perpetual assets, with protocol-stable numeric IDs.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[repr(u8)]
pub enum AssetId {
    BTC = 0,
    ETH = 1,
    SOL = 2,
}

impl AssetId {
    pub const ALL: [Self; 3] = [Self::BTC, Self::ETH, Self::SOL];

    #[must_use]
    pub const fn value(self) -> u8 {
        self as u8
    }

    #[must_use]
    pub const fn symbol(self) -> &'static str {
        match self {
            Self::BTC => "BTC",
            Self::ETH => "ETH",
            Self::SOL => "SOL",
        }
    }

    pub fn from_symbol(symbol: &str) -> Result<Self, WireValueError> {
        match symbol {
            "BTC" => Ok(Self::BTC),
            "ETH" => Ok(Self::ETH),
            "SOL" => Ok(Self::SOL),
            _ => Err(WireValueError::UnsupportedAsset),
        }
    }
}

impl TryFrom<u8> for AssetId {
    type Error = WireValueError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::BTC),
            1 => Ok(Self::ETH),
            2 => Ok(Self::SOL),
            _ => Err(WireValueError::UnsupportedAsset),
        }
    }
}

impl FromStr for AssetId {
    type Err = WireValueError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::from_symbol(value)
    }
}

/// Number of decimal places represented by one integer unit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct DecimalScale(u8);

impl DecimalScale {
    /// Scales are intentionally bounded so powers of ten fit in `u64`.
    pub fn new(decimal_places: u8) -> Result<Self, WireValueError> {
        if decimal_places <= 18 {
            Ok(Self(decimal_places))
        } else {
            Err(WireValueError::ScaleTooLarge)
        }
    }

    #[must_use]
    pub const fn decimal_places(self) -> u8 {
        self.0
    }
}

impl<'de> Deserialize<'de> for DecimalScale {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(u8::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

/// A strictly positive exact price in configured ticks.
///
/// Serde intentionally uses the internal integer tick representation. Wire
/// adapters that accept decimal strings must parse them with [`PriceTicks::parse`].
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PriceTicks(i64);

impl PriceTicks {
    pub fn new(value: i64) -> Result<Self, WireValueError> {
        if value > 0 { Ok(Self(value)) } else { Err(WireValueError::NonPositive) }
    }

    pub fn parse(value: &str, scale: DecimalScale) -> Result<Self, WireValueError> {
        let parsed = parse_scaled(value, scale, true)?;
        let ticks = i64::try_from(parsed).map_err(|_| WireValueError::Overflow)?;
        Self::new(ticks)
    }

    #[must_use]
    pub const fn value(self) -> i64 {
        self.0
    }

    #[must_use]
    pub fn format(self, scale: DecimalScale) -> String {
        format_scaled(i128::from(self.0), scale)
    }
}

impl<'de> Deserialize<'de> for PriceTicks {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(i64::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

/// A strictly positive exact quantity in configured lots.
///
/// Serde intentionally uses the internal integer lot representation. Wire
/// adapters that accept decimal strings must parse them with [`QtyLots::parse`].
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct QtyLots(u64);

impl QtyLots {
    pub fn new(value: u64) -> Result<Self, WireValueError> {
        if value > 0 { Ok(Self(value)) } else { Err(WireValueError::NonPositive) }
    }

    pub fn parse(value: &str, scale: DecimalScale) -> Result<Self, WireValueError> {
        let parsed = parse_scaled(value, scale, false)?;
        let lots = u64::try_from(parsed).map_err(|_| WireValueError::Overflow)?;
        Self::new(lots)
    }

    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }

    #[must_use]
    pub fn format(self, scale: DecimalScale) -> String {
        format_scaled(i128::from(self.0), scale)
    }
}

impl<'de> Deserialize<'de> for QtyLots {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(u64::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

/// Normalized address-shaped identifier for synthetic local state.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct SimUserId(String);

impl SimUserId {
    pub fn parse(value: &str) -> Result<Self, WireValueError> {
        if value.len() != 42
            || !value.starts_with("0x")
            || !value.as_bytes()[2..]
                .iter()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
        {
            return Err(WireValueError::InvalidSimUser);
        }
        Ok(Self(value.to_owned()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for SimUserId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::parse(&String::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

impl fmt::Display for SimUserId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for SimUserId {
    type Err = WireValueError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireValueError {
    #[error("unsupported asset")]
    UnsupportedAsset,
    #[error("decimal scale is too large")]
    ScaleTooLarge,
    #[error("value must be strictly positive")]
    NonPositive,
    #[error("invalid decimal syntax")]
    InvalidDecimal,
    #[error("value has more precision than the configured scale")]
    ExcessPrecision,
    #[error("numeric conversion overflow")]
    Overflow,
    #[error("invalid normalized simulator user")]
    InvalidSimUser,
}

fn parse_scaled(value: &str, scale: DecimalScale, signed: bool) -> Result<i128, WireValueError> {
    if value.is_empty() || value.starts_with('+') || value.trim() != value {
        return Err(WireValueError::InvalidDecimal);
    }
    let (negative, unsigned) = match value.strip_prefix('-') {
        Some(rest) if signed => (true, rest),
        Some(_) => return Err(WireValueError::InvalidDecimal),
        None => (false, value),
    };
    if unsigned.ends_with('.') {
        return Err(WireValueError::InvalidDecimal);
    }
    let mut pieces = unsigned.split('.');
    let whole = pieces.next().ok_or(WireValueError::InvalidDecimal)?;
    let fraction = pieces.next().unwrap_or("");
    if pieces.next().is_some()
        || whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(WireValueError::InvalidDecimal);
    }
    let places = usize::from(scale.decimal_places());
    if fraction.len() > places {
        return Err(WireValueError::ExcessPrecision);
    }
    let factor =
        10_i128.checked_pow(u32::from(scale.decimal_places())).ok_or(WireValueError::Overflow)?;
    let whole_number = whole.parse::<i128>().map_err(|_| WireValueError::Overflow)?;
    let fraction_number = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<i128>().map_err(|_| WireValueError::Overflow)?
    };
    let padding = u32::try_from(places - fraction.len()).map_err(|_| WireValueError::Overflow)?;
    let scaled_fraction = fraction_number
        .checked_mul(10_i128.checked_pow(padding).ok_or(WireValueError::Overflow)?)
        .ok_or(WireValueError::Overflow)?;
    let magnitude = whole_number
        .checked_mul(factor)
        .and_then(|number| number.checked_add(scaled_fraction))
        .ok_or(WireValueError::Overflow)?;
    if negative { magnitude.checked_neg().ok_or(WireValueError::Overflow) } else { Ok(magnitude) }
}

fn format_scaled(value: i128, scale: DecimalScale) -> String {
    let places = usize::from(scale.decimal_places());
    if places == 0 {
        return value.to_string();
    }
    let factor = 10_i128.pow(u32::from(scale.decimal_places()));
    let whole = value / factor;
    let fraction = value.unsigned_abs() % factor.unsigned_abs();
    if fraction == 0 {
        return whole.to_string();
    }
    let mut fraction_text = format!("{fraction:0places$}");
    while fraction_text.ends_with('0') {
        fraction_text.pop();
    }
    format!("{whole}.{fraction_text}")
}
