//! Capability-safe, read-only Hyperliquid oracle ingestion.
//!
//! The upstream boundary deliberately exposes public market-data reads only.
//! This crate contains no wallet, signing, exchange, order, cancel, or transfer
//! capability.

use futures_util::{SinkExt, StreamExt};
use hl_wire::{AssetId, DecimalScale, PriceTicks, WireValueError};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    future::Future,
    pin::Pin,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::{net::TcpStream, time::Instant};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

pub const STALE_AFTER_MS: u64 = 60_000;
pub const MAINNET_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
pub const MAINNET_INFO_URL: &str = "https://api.hyperliquid.xyz/info";

/// Boxed future used by the object-safe read-only upstream boundary.
pub type UpstreamFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// The complete upstream capability available to the oracle.
///
/// Implementations can subscribe to public `activeAssetCtx` data and perform
/// the public `metaAndAssetCtxs` info query. No state-changing method is
/// expressible through this trait.
pub trait ReadOnlyUpstream: Send {
    type Error: Error + Send + Sync + 'static;

    fn next_active_asset_ctx(&mut self) -> UpstreamFuture<'_, Option<Value>, Self::Error>;
    fn meta_and_asset_ctxs(&mut self) -> UpstreamFuture<'_, Value, Self::Error>;
}

/// A fallback-only client for Hyperliquid's public info endpoint.
///
/// It intentionally has no private key and can issue only the read-only
/// `metaAndAssetCtxs` request. A WebSocket implementation can wrap this client
/// and provide primary `activeAssetCtx` messages through [`ReadOnlyUpstream`].
#[derive(Clone, Debug)]
pub struct HyperliquidInfoClient {
    client: reqwest::Client,
    info_url: String,
}

impl HyperliquidInfoClient {
    #[must_use]
    pub fn new(info_url: impl Into<String>) -> Self {
        Self { client: reqwest::Client::new(), info_url: info_url.into() }
    }
}

impl ReadOnlyUpstream for HyperliquidInfoClient {
    type Error = reqwest::Error;

    fn next_active_asset_ctx(&mut self) -> UpstreamFuture<'_, Option<Value>, Self::Error> {
        Box::pin(async { Ok(None) })
    }

    fn meta_and_asset_ctxs(&mut self) -> UpstreamFuture<'_, Value, Self::Error> {
        Box::pin(async move {
            self.client
                .post(&self.info_url)
                .json(&json!({"type": "metaAndAssetCtxs"}))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await
        })
    }
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationSource {
    ActiveAssetCtx,
    MetaAndAssetCtxs,
}

/// A validated, normalized public oracle observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OracleObservation {
    pub asset: AssetId,
    pub price: PriceTicks,
    pub observed_at_ms: u64,
    /// Sequence assigned by the ingestion owner. Ordering is checked per asset.
    pub upstream_sequence: u64,
    pub source: ObservationSource,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum OracleParseError {
    #[error("malformed oracle message")]
    Malformed,
    #[error("unexpected upstream channel")]
    UnexpectedChannel,
    #[error("unsupported oracle asset")]
    UnsupportedAsset,
    #[error("ambiguous oracle asset mapping")]
    AmbiguousAsset,
    #[error("invalid oracle price: {0}")]
    InvalidPrice(WireValueError),
}

/// Fixed public subscriptions used by the live oracle. The DTO is deliberately
/// not a generic request: callers cannot express any other upstream method.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ActiveAssetCtxSubscription {
    method: &'static str,
    subscription: ActiveAssetCtxSubscriptionData,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ActiveAssetCtxSubscriptionData {
    #[serde(rename = "type")]
    channel_type: &'static str,
    coin: &'static str,
}

impl ActiveAssetCtxSubscription {
    #[must_use]
    pub const fn for_asset(asset: AssetId) -> Self {
        let coin = match asset {
            AssetId::BTC => "BTC",
            AssetId::ETH => "ETH",
            AssetId::SOL => "SOL",
        };
        Self {
            method: "subscribe",
            subscription: ActiveAssetCtxSubscriptionData { channel_type: "activeAssetCtx", coin },
        }
    }
}

/// Valid control/data messages accepted from the public WebSocket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WsInbound {
    SubscriptionAcknowledged(AssetId),
    ActiveAssetCtx { asset: AssetId, oracle_px: String },
    Pong,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChannelEnvelope {
    channel: String,
    #[serde(default)]
    data: Option<Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscriptionResponseData {
    method: String,
    subscription: SubscriptionResponseSubscription,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscriptionResponseSubscription {
    #[serde(rename = "type")]
    channel_type: String,
    coin: String,
}

/// Strictly classifies the only inbound messages needed by the oracle.
pub fn parse_ws_inbound(value: &Value) -> Result<WsInbound, OracleParseError> {
    let envelope: ChannelEnvelope =
        serde_json::from_value(value.clone()).map_err(|_| OracleParseError::Malformed)?;
    match envelope.channel.as_str() {
        "pong" if envelope.data.is_none() => Ok(WsInbound::Pong),
        "subscriptionResponse" => {
            let data: SubscriptionResponseData =
                serde_json::from_value(envelope.data.ok_or(OracleParseError::Malformed)?)
                    .map_err(|_| OracleParseError::Malformed)?;
            if data.method != "subscribe" || data.subscription.channel_type != "activeAssetCtx" {
                return Err(OracleParseError::UnexpectedChannel);
            }
            let asset = AssetId::from_symbol(&data.subscription.coin)
                .map_err(|_| OracleParseError::UnsupportedAsset)?;
            Ok(WsInbound::SubscriptionAcknowledged(asset))
        }
        "activeAssetCtx" => {
            let message: ActiveAssetCtxData =
                serde_json::from_value(envelope.data.ok_or(OracleParseError::Malformed)?)
                    .map_err(|_| OracleParseError::Malformed)?;
            let asset = AssetId::from_symbol(&message.coin)
                .map_err(|_| OracleParseError::UnsupportedAsset)?;
            Ok(WsInbound::ActiveAssetCtx { asset, oracle_px: message.ctx.oracle_px })
        }
        "pong" => Err(OracleParseError::Malformed),
        _ => Err(OracleParseError::UnexpectedChannel),
    }
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

/// Injectable endpoints and operation bounds for the public read-only transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveConfig {
    pub ws_url: String,
    pub info_url: String,
    pub connect_timeout: Duration,
    pub write_timeout: Duration,
    pub read_timeout: Duration,
    pub send_timeout: Duration,
    pub heartbeat_interval: Duration,
}

impl Default for LiveConfig {
    fn default() -> Self {
        Self {
            ws_url: MAINNET_WS_URL.to_owned(),
            info_url: MAINNET_INFO_URL.to_owned(),
            connect_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(40),
            send_timeout: Duration::from_secs(5),
            heartbeat_interval: Duration::from_secs(30),
        }
    }
}

impl LiveConfig {
    fn bounded(mut self) -> Self {
        self.connect_timeout = self.connect_timeout.max(Duration::from_millis(1));
        self.write_timeout = self.write_timeout.max(Duration::from_millis(1));
        self.read_timeout = self.read_timeout.max(Duration::from_millis(1));
        self.send_timeout = self.send_timeout.max(Duration::from_millis(1));
        self.heartbeat_interval =
            self.heartbeat_interval.clamp(Duration::from_millis(1), Duration::from_secs(50));
        self
    }
}

/// Typed failures that require a fresh WebSocket connection and resubscription.
#[derive(Debug, Error)]
pub enum LiveTransportError {
    #[error("websocket connect timed out")]
    ConnectTimeout,
    #[error("websocket subscription write timed out")]
    WriteTimeout,
    #[error("websocket read timed out")]
    ReadTimeout,
    #[error("websocket control send timed out")]
    SendTimeout,
    #[error("websocket is not connected")]
    NotConnected,
    #[error("websocket disconnected")]
    Disconnected,
    #[error("websocket transport failed: {0}")]
    WebSocket(#[source] tokio_tungstenite::tungstenite::Error),
    #[error("HTTP fallback failed: {0}")]
    Http(#[source] reqwest::Error),
    #[error(transparent)]
    Parse(#[from] OracleParseError),
}

impl LiveTransportError {
    /// Whether the primary stream must be recreated before another WS read.
    #[must_use]
    pub const fn is_reconnectable(&self) -> bool {
        matches!(
            self,
            Self::ConnectTimeout
                | Self::WriteTimeout
                | Self::ReadTimeout
                | Self::SendTimeout
                | Self::NotConnected
                | Self::Disconnected
                | Self::WebSocket(_)
        )
    }
}

/// The complete live upstream capability: fixed public connect/read/fallback only.
/// No arbitrary request, wallet, signing, order, cancel, or transfer operation is exposed.
pub trait OracleTransport: Send {
    type Error: Error + Send + Sync + 'static;

    fn connect(&mut self) -> UpstreamFuture<'_, (), Self::Error>;
    fn next_active_asset_ctx(
        &mut self,
        upstream_sequence: u64,
        scales: PriceScales,
    ) -> UpstreamFuture<'_, OracleObservation, Self::Error>;
    fn meta_and_asset_ctxs(
        &mut self,
        upstream_sequence: u64,
        scales: PriceScales,
    ) -> UpstreamFuture<'_, Vec<OracleObservation>, Self::Error>;
}

type LiveSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Tokio implementation of the capability-safe public market-data transport.
pub struct TokioHyperliquidTransport<C> {
    config: LiveConfig,
    clock: C,
    info: HyperliquidInfoClient,
    socket: Option<LiveSocket>,
    last_heartbeat: Instant,
}

impl<C: Clock> TokioHyperliquidTransport<C> {
    #[must_use]
    pub fn new(config: LiveConfig, clock: C) -> Self {
        let config = config.bounded();
        Self {
            info: HyperliquidInfoClient::new(config.info_url.clone()),
            config,
            clock,
            socket: None,
            last_heartbeat: Instant::now(),
        }
    }

    async fn send_control(
        socket: &mut LiveSocket,
        message: Message,
        timeout: Duration,
    ) -> Result<(), LiveTransportError> {
        tokio::time::timeout(timeout, socket.send(message))
            .await
            .map_err(|_| LiveTransportError::SendTimeout)?
            .map_err(LiveTransportError::WebSocket)
    }

    fn disconnect(&mut self) {
        self.socket = None;
    }
}

impl<C: Clock> OracleTransport for TokioHyperliquidTransport<C> {
    type Error = LiveTransportError;

    fn connect(&mut self) -> UpstreamFuture<'_, (), Self::Error> {
        Box::pin(async move {
            self.disconnect();
            let (mut socket, _) = tokio::time::timeout(
                self.config.connect_timeout,
                tokio_tungstenite::connect_async(&self.config.ws_url),
            )
            .await
            .map_err(|_| LiveTransportError::ConnectTimeout)?
            .map_err(LiveTransportError::WebSocket)?;

            for asset in AssetId::ALL {
                let payload = serde_json::to_string(&ActiveAssetCtxSubscription::for_asset(asset))
                    .expect("fixed subscription DTO is serializable");
                tokio::time::timeout(
                    self.config.write_timeout,
                    socket.send(Message::Text(payload.into())),
                )
                .await
                .map_err(|_| LiveTransportError::WriteTimeout)?
                .map_err(LiveTransportError::WebSocket)?;
            }
            self.last_heartbeat = Instant::now();
            self.socket = Some(socket);
            Ok(())
        })
    }

    fn next_active_asset_ctx(
        &mut self,
        upstream_sequence: u64,
        scales: PriceScales,
    ) -> UpstreamFuture<'_, OracleObservation, Self::Error> {
        Box::pin(async move {
            let read_deadline = Instant::now() + self.config.read_timeout;
            loop {
                let heartbeat_deadline = self.last_heartbeat + self.config.heartbeat_interval;
                let deadline = read_deadline.min(heartbeat_deadline);
                let frame = {
                    let socket = self.socket.as_mut().ok_or(LiveTransportError::NotConnected)?;
                    tokio::time::timeout_at(deadline, socket.next()).await
                };
                let frame = match frame {
                    Err(_) if Instant::now() >= read_deadline => {
                        self.disconnect();
                        return Err(LiveTransportError::ReadTimeout);
                    }
                    Err(_) => {
                        let socket =
                            self.socket.as_mut().ok_or(LiveTransportError::NotConnected)?;
                        if let Err(error) = Self::send_control(
                            socket,
                            Message::Text(json!({"method":"ping"}).to_string().into()),
                            self.config.send_timeout,
                        )
                        .await
                        {
                            self.disconnect();
                            return Err(error);
                        }
                        self.last_heartbeat = Instant::now();
                        continue;
                    }
                    Ok(None) => {
                        self.disconnect();
                        return Err(LiveTransportError::Disconnected);
                    }
                    Ok(Some(Err(error))) => {
                        self.disconnect();
                        return Err(LiveTransportError::WebSocket(error));
                    }
                    Ok(Some(Ok(frame))) => frame,
                };

                match frame {
                    Message::Close(_) => {
                        self.disconnect();
                        return Err(LiveTransportError::Disconnected);
                    }
                    Message::Ping(payload) => {
                        let socket =
                            self.socket.as_mut().ok_or(LiveTransportError::NotConnected)?;
                        if let Err(error) = Self::send_control(
                            socket,
                            Message::Pong(payload),
                            self.config.send_timeout,
                        )
                        .await
                        {
                            self.disconnect();
                            return Err(error);
                        }
                    }
                    Message::Text(text) => {
                        let Ok(payload) = serde_json::from_str::<Value>(text.as_str()) else {
                            continue;
                        };
                        match parse_ws_inbound(&payload) {
                            Ok(WsInbound::ActiveAssetCtx { asset, oracle_px }) => {
                                let Ok(price) =
                                    PriceTicks::parse(&oracle_px, scales.for_asset(asset))
                                else {
                                    continue;
                                };
                                // Sample the injected receive clock only after the supported asset
                                // and its positive, exactly-scaled oracle price are fully validated.
                                let observed_at_ms = self.clock.now_ms();
                                return Ok(OracleObservation {
                                    asset,
                                    price,
                                    observed_at_ms,
                                    upstream_sequence,
                                    source: ObservationSource::ActiveAssetCtx,
                                });
                            }
                            Ok(WsInbound::SubscriptionAcknowledged(_) | WsInbound::Pong)
                            | Err(_) => continue,
                        }
                    }
                    Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => continue,
                }
            }
        })
    }

    fn meta_and_asset_ctxs(
        &mut self,
        upstream_sequence: u64,
        scales: PriceScales,
    ) -> UpstreamFuture<'_, Vec<OracleObservation>, Self::Error> {
        Box::pin(async move {
            let payload = ReadOnlyUpstream::meta_and_asset_ctxs(&mut self.info)
                .await
                .map_err(LiveTransportError::Http)?;
            let observed_at_ms = self.clock.now_ms();
            parse_meta_and_asset_ctxs(&payload, observed_at_ms, upstream_sequence, scales)
                .map_err(LiveTransportError::Parse)
        })
    }
}

/// Parses one public WebSocket `activeAssetCtx` update.
pub fn parse_active_asset_ctx(
    value: &Value,
    observed_at_ms: u64,
    upstream_sequence: u64,
    scales: PriceScales,
) -> Result<OracleObservation, OracleParseError> {
    let WsInbound::ActiveAssetCtx { asset, oracle_px } = parse_ws_inbound(value)? else {
        return Err(OracleParseError::UnexpectedChannel);
    };
    let price = PriceTicks::parse(&oracle_px, scales.for_asset(asset))
        .map_err(OracleParseError::InvalidPrice)?;
    Ok(OracleObservation {
        asset,
        price,
        observed_at_ms,
        upstream_sequence,
        source: ObservationSource::ActiveAssetCtx,
    })
}

/// Parses the public `[meta, assetCtxs]` response by joining context entries to
/// metadata names. Positional values are never mapped without validating the
/// corresponding symbol.
pub fn parse_meta_and_asset_ctxs(
    value: &Value,
    observed_at_ms: u64,
    upstream_sequence: u64,
    scales: PriceScales,
) -> Result<Vec<OracleObservation>, OracleParseError> {
    let pair =
        value.as_array().filter(|pair| pair.len() == 2).ok_or(OracleParseError::Malformed)?;
    let universe =
        pair[0].get("universe").and_then(Value::as_array).ok_or(OracleParseError::Malformed)?;
    let contexts = pair[1].as_array().ok_or(OracleParseError::Malformed)?;
    if universe.len() != contexts.len() {
        return Err(OracleParseError::Malformed);
    }

    let mut observations = BTreeMap::new();
    let mut ambiguous = BTreeSet::new();
    let mut first_error = None;
    for (metadata, context) in universe.iter().zip(contexts) {
        let Some(symbol) = metadata.get("name").and_then(Value::as_str) else {
            first_error.get_or_insert(OracleParseError::Malformed);
            continue;
        };
        let Ok(asset) = AssetId::from_symbol(symbol) else {
            continue;
        };
        if ambiguous.contains(&asset) {
            continue;
        }
        let Some(oracle_px) = context.get("oraclePx").and_then(Value::as_str) else {
            first_error.get_or_insert(OracleParseError::Malformed);
            continue;
        };
        let price = match PriceTicks::parse(oracle_px, scales.for_asset(asset)) {
            Ok(price) => price,
            Err(error) => {
                first_error.get_or_insert(OracleParseError::InvalidPrice(error));
                continue;
            }
        };
        let observation = OracleObservation {
            asset,
            price,
            observed_at_ms,
            upstream_sequence,
            source: ObservationSource::MetaAndAssetCtxs,
        };
        if observations.insert(asset, observation).is_some() {
            observations.remove(&asset);
            ambiguous.insert(asset);
            first_error.get_or_insert(OracleParseError::AmbiguousAsset);
        }
    }
    if observations.is_empty()
        && let Some(error) = first_error
    {
        return Err(error);
    }
    Ok(observations.into_values().collect())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationReject {
    OutOfOrder,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OracleFreshness {
    Missing,
    Fresh {
        age_ms: u64,
    },
    Stale {
        age_ms: u64,
    },
    /// The observation is ahead of the runtime clock, including after a clock regression.
    FutureObservation,
}

/// Independent last-known-good state for each supported asset.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OracleState {
    observations: BTreeMap<AssetId, OracleObservation>,
}

impl OracleState {
    pub fn observe(&mut self, observation: OracleObservation) -> Result<(), ObservationReject> {
        if self.observations.get(&observation.asset).is_some_and(|previous| {
            observation.upstream_sequence <= previous.upstream_sequence
                || observation.observed_at_ms < previous.observed_at_ms
        }) {
            return Err(ObservationReject::OutOfOrder);
        }
        self.observations.insert(observation.asset, observation);
        Ok(())
    }

    /// Fails one asset closed without changing independent asset state.
    pub fn mark_unavailable(&mut self, asset: AssetId) {
        self.observations.remove(&asset);
    }

    pub fn apply_ingest_event(
        &mut self,
        event: OracleIngestEvent,
    ) -> Result<(), ObservationReject> {
        match event {
            OracleIngestEvent::Unavailable { asset, .. } => {
                self.mark_unavailable(asset);
                Ok(())
            }
            OracleIngestEvent::Observation(observation) => self.observe(observation),
        }
    }

    #[must_use]
    pub fn observation(&self, asset: AssetId) -> Option<OracleObservation> {
        self.observations.get(&asset).copied()
    }

    #[must_use]
    pub fn freshness(&self, asset: AssetId, now_ms: u64) -> OracleFreshness {
        let Some(observation) = self.observation(asset) else {
            return OracleFreshness::Missing;
        };
        let Some(age_ms) = now_ms.checked_sub(observation.observed_at_ms) else {
            return OracleFreshness::FutureObservation;
        };
        if age_ms > STALE_AFTER_MS {
            OracleFreshness::Stale { age_ms }
        } else {
            OracleFreshness::Fresh { age_ms }
        }
    }
}

/// Clock seam used by ingestion tests and runtime composition.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

/// Production wall clock used by the runtime owner.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
    }
}

#[derive(Debug, Error)]
pub enum OracleReadError<E: Error> {
    #[error("read-only upstream request failed: {0}")]
    Upstream(E),
    #[error(transparent)]
    Parse(#[from] OracleParseError),
}

/// Reads one primary observation, falling back to `metaAndAssetCtxs` whenever
/// the primary stream disconnects or returns an invalid/incomplete message.
/// The caller owns the deterministic sequence and clock.
pub async fn read_observations<U: ReadOnlyUpstream, C: Clock>(
    upstream: &mut U,
    clock: &C,
    upstream_sequence: u64,
    scales: PriceScales,
) -> Result<Vec<OracleObservation>, OracleReadError<U::Error>> {
    if let Ok(Some(value)) = upstream.next_active_asset_ctx().await {
        let observed_at_ms = clock.now_ms();
        if let Ok(observation) =
            parse_active_asset_ctx(&value, observed_at_ms, upstream_sequence, scales)
        {
            return Ok(vec![observation]);
        }
    }

    let fallback = upstream.meta_and_asset_ctxs().await.map_err(OracleReadError::Upstream)?;
    let observed_at_ms = clock.now_ms();
    parse_meta_and_asset_ctxs(&fallback, observed_at_ms, upstream_sequence, scales)
        .map_err(OracleReadError::Parse)
}

/// Bounded deterministic exponential reconnect schedule. Any production jitter
/// is applied by the caller and never enters oracle or engine state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconnectBackoff {
    initial_ms: u64,
    maximum_ms: u64,
    next_ms: u64,
}

impl ReconnectBackoff {
    #[must_use]
    pub fn new(initial: Duration, maximum: Duration) -> Self {
        let initial_ms = u64::try_from(initial.as_millis()).unwrap_or(u64::MAX).max(1);
        let maximum_ms = u64::try_from(maximum.as_millis()).unwrap_or(u64::MAX).max(initial_ms);
        Self { initial_ms, maximum_ms, next_ms: initial_ms }
    }

    pub fn reset(&mut self) {
        self.next_ms = self.initial_ms;
    }

    pub fn next_delay(&mut self) -> Duration {
        let delay = self.next_ms;
        self.next_ms = self.next_ms.saturating_mul(2).min(self.maximum_ms);
        Duration::from_millis(delay)
    }

    #[must_use]
    pub const fn maximum_delay(&self) -> Duration {
        Duration::from_millis(self.maximum_ms)
    }
}

/// Creates a new transport for every connection attempt. Reusing a socket after
/// a disconnect is deliberately not expressible through the orchestrator.
pub trait TransportFactory: Send {
    type Transport: OracleTransport;

    fn create(&mut self) -> Self::Transport;
}

impl<F, T> TransportFactory for F
where
    F: FnMut() -> T + Send,
    T: OracleTransport,
{
    type Transport = T;

    fn create(&mut self) -> Self::Transport {
        self()
    }
}

/// Injectable delay boundary used by reconnect tests and production Tokio time.
pub trait Sleeper: Send {
    fn sleep(&mut self, delay: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TokioSleeper;

impl Sleeper for TokioSleeper {
    fn sleep(&mut self, delay: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(tokio::time::sleep(delay))
    }
}

/// Injectable reconnect jitter. The orchestrator clamps the result to the
/// configured backoff maximum, so an implementation cannot defeat the bound.
pub trait Jitter: Send {
    fn apply(&mut self, base: Duration) -> Duration;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoJitter;

impl Jitter for NoJitter {
    fn apply(&mut self, base: Duration) -> Duration {
        base
    }
}

/// Ordered state transitions emitted by resilient ingestion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OracleIngestEvent {
    /// The shared primary stream is unavailable for this asset. Consumers must
    /// fail closed until a subsequent fallback or WebSocket observation arrives.
    Unavailable {
        asset: AssetId,
        at_ms: u64,
    },
    Observation(OracleObservation),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrchestratorExit {
    Shutdown,
    OutputClosed,
}

/// WS-primary/HTTP-fallback ingestion with fully injectable transition inputs.
pub struct OracleOrchestrator<F, S, J, C> {
    factory: F,
    sleeper: S,
    jitter: J,
    clock: C,
    backoff: ReconnectBackoff,
    scales: PriceScales,
}

impl<F, S, J, C> OracleOrchestrator<F, S, J, C>
where
    F: TransportFactory,
    S: Sleeper,
    J: Jitter,
    C: Clock,
{
    #[must_use]
    pub const fn new(
        factory: F,
        sleeper: S,
        jitter: J,
        clock: C,
        backoff: ReconnectBackoff,
        scales: PriceScales,
    ) -> Self {
        Self { factory, sleeper, jitter, clock, backoff, scales }
    }

    /// Runs until cancellation or until the bounded observation consumer closes.
    /// Every failed connect/read marks all assets unavailable, immediately tries
    /// the fixed HTTP fallback, sleeps once, and then creates a fresh transport.
    pub async fn run(
        mut self,
        output: tokio::sync::mpsc::Sender<OracleIngestEvent>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> OrchestratorExit {
        let mut sequence = 0_u64;

        loop {
            let mut transport = self.factory.create();
            let connected = match until_shutdown(&mut shutdown, transport.connect()).await {
                Some(result) => result.is_ok(),
                None => return OrchestratorExit::Shutdown,
            };

            if connected {
                loop {
                    let candidate = sequence.saturating_add(1);
                    let read = until_shutdown(
                        &mut shutdown,
                        transport.next_active_asset_ctx(candidate, self.scales),
                    )
                    .await;
                    match read {
                        None => return OrchestratorExit::Shutdown,
                        Some(Ok(observation)) => {
                            sequence = candidate;
                            if send_event(
                                &output,
                                &mut shutdown,
                                OracleIngestEvent::Observation(observation),
                            )
                            .await
                            .is_none()
                            {
                                return exit_for(&shutdown);
                            }
                            // A connection is not considered recovered until it yields
                            // its first valid WS observation.
                            self.backoff.reset();
                        }
                        Some(Err(_)) => break,
                    }
                }
            }

            let unavailable_at_ms = self.clock.now_ms();
            for asset in AssetId::ALL {
                if send_event(
                    &output,
                    &mut shutdown,
                    OracleIngestEvent::Unavailable { asset, at_ms: unavailable_at_ms },
                )
                .await
                .is_none()
                {
                    return exit_for(&shutdown);
                }
            }

            let fallback_sequence = sequence.saturating_add(1);
            let fallback = until_shutdown(
                &mut shutdown,
                transport.meta_and_asset_ctxs(fallback_sequence, self.scales),
            )
            .await;
            match fallback {
                None => return OrchestratorExit::Shutdown,
                Some(Ok(observations)) => {
                    sequence = fallback_sequence;
                    for observation in observations {
                        if send_event(
                            &output,
                            &mut shutdown,
                            OracleIngestEvent::Observation(observation),
                        )
                        .await
                        .is_none()
                        {
                            return exit_for(&shutdown);
                        }
                    }
                }
                Some(Err(_)) => {}
            }

            let base = self.backoff.next_delay();
            let delay = self.jitter.apply(base).min(self.backoff.maximum_delay());
            if until_shutdown(&mut shutdown, self.sleeper.sleep(delay)).await.is_none() {
                return OrchestratorExit::Shutdown;
            }
        }
    }
}

fn exit_for(shutdown: &tokio::sync::watch::Receiver<bool>) -> OrchestratorExit {
    if *shutdown.borrow() { OrchestratorExit::Shutdown } else { OrchestratorExit::OutputClosed }
}

async fn send_event(
    output: &tokio::sync::mpsc::Sender<OracleIngestEvent>,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
    event: OracleIngestEvent,
) -> Option<()> {
    until_shutdown(shutdown, output.send(event)).await.and_then(Result::ok)
}

async fn until_shutdown<F: Future>(
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
    future: F,
) -> Option<F::Output> {
    tokio::pin!(future);
    loop {
        if *shutdown.borrow() {
            return None;
        }
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return None;
                }
            }
            result = &mut future => return Some(result),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn scales() -> PriceScales {
        let scale = DecimalScale::new(2).expect("valid scale");
        PriceScales::new(scale, scale, scale)
    }

    fn active(symbol: &str, price: &str, time: u64, sequence: u64) -> OracleObservation {
        parse_active_asset_ctx(
            &json!({"channel":"activeAssetCtx","data":{"coin":symbol,"ctx":{"oraclePx":price}}}),
            time,
            sequence,
            scales(),
        )
        .expect("valid active context")
    }

    #[test]
    fn subscription_dtos_are_fixed_to_the_three_public_active_asset_channels() {
        let requests = AssetId::ALL.map(|asset| {
            serde_json::to_value(ActiveAssetCtxSubscription::for_asset(asset))
                .expect("serialize subscription")
        });
        assert_eq!(
            requests,
            [
                json!({"method":"subscribe","subscription":{"type":"activeAssetCtx","coin":"BTC"}}),
                json!({"method":"subscribe","subscription":{"type":"activeAssetCtx","coin":"ETH"}}),
                json!({"method":"subscribe","subscription":{"type":"activeAssetCtx","coin":"SOL"}}),
            ]
        );
    }

    #[test]
    fn classifies_ack_updates_and_pong_fixtures() {
        for (symbol, asset, price) in [
            ("BTC", AssetId::BTC, "60123.45"),
            ("ETH", AssetId::ETH, "3123.40"),
            ("SOL", AssetId::SOL, "142.01"),
        ] {
            assert_eq!(
                parse_ws_inbound(&json!({
                    "channel":"subscriptionResponse",
                    "data":{"method":"subscribe","subscription":{"type":"activeAssetCtx","coin":symbol}}
                })),
                Ok(WsInbound::SubscriptionAcknowledged(asset))
            );
            assert_eq!(
                parse_ws_inbound(&json!({
                    "channel":"activeAssetCtx",
                    "data":{"coin":symbol,"ctx":{"oraclePx":price,"markPx":"ignored"}}
                })),
                Ok(WsInbound::ActiveAssetCtx { asset, oracle_px: price.to_owned() })
            );
        }
        assert_eq!(parse_ws_inbound(&json!({"channel":"pong"})), Ok(WsInbound::Pong));
    }

    #[test]
    fn malformed_spot_and_unrequested_channels_are_rejected() {
        assert_eq!(
            parse_ws_inbound(&json!({"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{}}})),
            Err(OracleParseError::Malformed)
        );
        assert_eq!(
            parse_ws_inbound(&json!({
                "channel":"activeAssetCtx",
                "data":{"coin":"@107","ctx":{"oraclePx":"1.0"}}
            })),
            Err(OracleParseError::UnsupportedAsset)
        );
        assert_eq!(
            parse_ws_inbound(&json!({"channel":"allMids","data":{"mids":{}}})),
            Err(OracleParseError::UnexpectedChannel)
        );
        assert_eq!(
            parse_ws_inbound(&json!({"channel":"pong","data":{}})),
            Err(OracleParseError::Malformed)
        );
        assert_eq!(
            parse_ws_inbound(&json!({"channel":"pong","extra":true})),
            Err(OracleParseError::Malformed)
        );
    }

    #[test]
    fn parses_mocked_active_asset_ctx_oracle_prices_for_supported_assets() {
        for (sequence, symbol, expected_asset, price, expected_ticks) in [
            (1, "BTC", AssetId::BTC, "60123.45", 6_012_345),
            (2, "ETH", AssetId::ETH, "3123.40", 312_340),
            (3, "SOL", AssetId::SOL, "142.01", 14_201),
        ] {
            let observation = active(symbol, price, 10_000, sequence);
            assert_eq!(observation.asset, expected_asset);
            assert_eq!(observation.price.value(), expected_ticks);
            assert_eq!(observation.source, ObservationSource::ActiveAssetCtx);
        }
    }

    #[test]
    fn fallback_maps_names_not_positions_and_ignores_unsupported_assets() {
        let response = json!([
            {"universe": [{"name":"SOL"},{"name":"DOGE"},{"name":"BTC"},{"name":"ETH"}]},
            [{"oraclePx":"142.01"},{"oraclePx":"0.20"},{"oraclePx":"60123.45"},{"oraclePx":"3123.40"}]
        ]);
        let observations =
            parse_meta_and_asset_ctxs(&response, 9, 7, scales()).expect("valid fallback");
        assert_eq!(observations.iter().map(|item| item.asset).collect::<Vec<_>>(), AssetId::ALL);
        assert_eq!(
            observations.iter().map(|item| item.price.value()).collect::<Vec<_>>(),
            [6_012_345, 312_340, 14_201]
        );
        assert!(observations.iter().all(|item| item.source == ObservationSource::MetaAndAssetCtxs));
    }

    #[test]
    fn malformed_one_asset_fallback_does_not_poison_valid_assets() {
        let mixed = json!([
            {"universe":[{"name":"BTC"},{"name":"ETH"},{"name":"SOL"}]},
            [{"oraclePx":"60000.00"},{"oraclePx":"not-a-price"},{"oraclePx":"140.00"}]
        ]);
        let observations =
            parse_meta_and_asset_ctxs(&mixed, 10, 7, scales()).expect("valid assets survive");
        assert_eq!(
            observations.iter().map(|item| item.asset).collect::<Vec<_>>(),
            [AssetId::BTC, AssetId::SOL]
        );
    }

    #[test]
    fn malformed_non_positive_and_ambiguous_fallback_is_rejected() {
        let zero = json!([{"universe":[{"name":"ETH"}]}, [{"oraclePx":"0"}]]);
        assert_eq!(
            parse_meta_and_asset_ctxs(&zero, 0, 1, scales()),
            Err(OracleParseError::InvalidPrice(WireValueError::NonPositive))
        );
        let duplicate = json!([{"universe":[{"name":"BTC"},{"name":"BTC"}]}, [{"oraclePx":"1"},{"oraclePx":"2"}]]);
        assert_eq!(
            parse_meta_and_asset_ctxs(&duplicate, 0, 1, scales()),
            Err(OracleParseError::AmbiguousAsset)
        );
    }

    struct FakeClock(AtomicU64);
    impl Clock for FakeClock {
        fn now_ms(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    #[test]
    fn state_is_per_asset_ordered_and_stale_only_after_sixty_seconds() {
        let clock = FakeClock(AtomicU64::new(61_000));
        let mut state = OracleState::default();
        state.observe(active("BTC", "100", 1_000, 2)).expect("first BTC");
        state.observe(active("ETH", "20", 500, 1)).expect("independent ETH sequence");
        assert_eq!(
            state.freshness(AssetId::BTC, clock.now_ms()),
            OracleFreshness::Fresh { age_ms: 60_000 }
        );
        clock.0.store(61_001, Ordering::Relaxed);
        assert_eq!(
            state.freshness(AssetId::BTC, clock.now_ms()),
            OracleFreshness::Stale { age_ms: 60_001 }
        );
        assert_eq!(
            state.observe(active("BTC", "101", 2_000, 2)),
            Err(ObservationReject::OutOfOrder)
        );
        assert_eq!(state.observation(AssetId::BTC).expect("BTC retained").price.value(), 10_000);
        assert_eq!(state.observation(AssetId::ETH).expect("ETH retained").price.value(), 2_000);
        assert_eq!(state.freshness(AssetId::SOL, clock.now_ms()), OracleFreshness::Missing);
        assert_eq!(state.freshness(AssetId::BTC, 999), OracleFreshness::FutureObservation);
        state
            .apply_ingest_event(OracleIngestEvent::Unavailable {
                asset: AssetId::ETH,
                at_ms: clock.now_ms(),
            })
            .expect("unavailability transition");
        assert_eq!(state.freshness(AssetId::ETH, clock.now_ms()), OracleFreshness::Missing);
        assert!(state.observation(AssetId::BTC).is_some(), "other assets remain available");
    }

    struct FakeUpstream {
        primary: Option<Value>,
        fallback: Value,
        primary_reads: usize,
        fallback_reads: usize,
    }

    impl ReadOnlyUpstream for FakeUpstream {
        type Error = std::io::Error;

        fn next_active_asset_ctx(&mut self) -> UpstreamFuture<'_, Option<Value>, Self::Error> {
            self.primary_reads += 1;
            let value = self.primary.take();
            Box::pin(async move { Ok(value) })
        }

        fn meta_and_asset_ctxs(&mut self) -> UpstreamFuture<'_, Value, Self::Error> {
            self.fallback_reads += 1;
            let value = self.fallback.clone();
            Box::pin(async move { Ok(value) })
        }
    }

    #[tokio::test]
    async fn disconnected_primary_uses_only_read_only_fallback_with_fake_clock() {
        let clock = FakeClock(AtomicU64::new(42_000));
        let mut upstream = FakeUpstream {
            primary: None,
            fallback: json!([
                {"universe":[{"name":"BTC"},{"name":"ETH"},{"name":"SOL"}]},
                [{"oraclePx":"100"},{"oraclePx":"20"},{"oraclePx":"3"}]
            ]),
            primary_reads: 0,
            fallback_reads: 0,
        };
        let observations =
            read_observations(&mut upstream, &clock, 9, scales()).await.expect("fallback succeeds");
        assert_eq!(observations.len(), 3);
        assert_eq!(upstream.primary_reads, 1);
        assert_eq!(upstream.fallback_reads, 1);
        assert!(observations.iter().all(|item| {
            item.observed_at_ms == 42_000
                && item.upstream_sequence == 9
                && item.source == ObservationSource::MetaAndAssetCtxs
        }));
    }

    #[test]
    fn reconnect_backoff_is_bounded_and_resettable() {
        let mut schedule =
            ReconnectBackoff::new(Duration::from_millis(100), Duration::from_millis(350));
        assert_eq!(
            (0..5).map(|_| schedule.next_delay().as_millis()).collect::<Vec<_>>(),
            [100, 200, 350, 350, 350]
        );
        schedule.reset();
        assert_eq!(schedule.next_delay(), Duration::from_millis(100));
    }
}
