mod config;

use axum::{Json, Router, extract::State, http::StatusCode, routing::get};
use config::{Config, OracleMode};
use hl_wire::api::Subscription;
use hl_wire::response::EventChannel;
use hl_wire::{AssetId, DecimalScale, PriceTicks};
use oracle_hyperliquid::{
    Clock, LiveConfig, NoJitter, ObservationSource, OracleIngestEvent, OracleObservation,
    OracleOrchestrator, PriceScales, ReconnectBackoff, SystemClock, TokioHyperliquidTransport,
    TokioSleeper, TransportFactory,
};
use sim_core::{BookSnapshot, EventRecord};
use sim_server::http::{
    HttpConfig, MarketObservation, MarketSnapshot, MarketView, MarketViewError, router_with_config,
};
use sim_server::runtime::actors::SeededLocalActors;
use sim_server::runtime::{OracleAssetHealth, RuntimeOracleHealth};
use sim_server::runtime::{RuntimeTask, RuntimeTaskError, start_runtime};
use sim_server::ws::{SnapshotView, ViewError, ViewMessage, WsLimits};
use sim_server::{
    RuntimeError, RuntimeHandle, RuntimeLimits, RuntimePort, RuntimeReply, RuntimeRequest,
};
use std::{collections::BTreeMap, fmt, io, process::ExitCode, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Instant, MissedTickBehavior},
};
use tower::limit::ConcurrencyLimitLayer;
use tower_http::timeout::TimeoutLayer;

const LIVE_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const LIVE_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const LIVE_SEND_TIMEOUT: Duration = Duration::from_secs(5);
const LIVE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const LIVE_RECONNECT_INITIAL: Duration = Duration::from_millis(250);
const LIVE_RECONNECT_MAXIMUM: Duration = Duration::from_secs(5);
const MAX_OBSERVATION_AGE_MS: u64 = 60_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OracleCacheSnapshot {
    latest: [Option<OracleObservation>; 3],
    upstream_available: [bool; 3],
    consistent: [bool; 3],
    synthetic: bool,
}

impl OracleCacheSnapshot {
    const fn empty(synthetic: bool) -> Self {
        Self { latest: [None; 3], upstream_available: [false; 3], consistent: [true; 3], synthetic }
    }

    #[cfg(test)]
    fn all_upstream_available(self) -> bool {
        self.upstream_available.into_iter().all(|available| available)
    }
}

/// Immutable shared lifecycle state. The watch value is the bounded, copy-on-write
/// cache later projections and readiness can subscribe to without engine access.
#[derive(Debug)]
struct LifecycleState {
    oracle_cache: watch::Sender<OracleCacheSnapshot>,
}

impl LifecycleState {
    fn new(synthetic: bool) -> Arc<Self> {
        let (oracle_cache, _) = watch::channel(OracleCacheSnapshot::empty(synthetic));
        Arc::new(Self { oracle_cache })
    }

    fn snapshot(&self) -> OracleCacheSnapshot {
        *self.oracle_cache.borrow()
    }

    fn observe(&self, observation: OracleObservation) {
        self.oracle_cache.send_modify(|cache| {
            let index = asset_index(observation.asset);
            if cache.latest[index].is_some_and(|previous| {
                observation.upstream_sequence <= previous.upstream_sequence
                    || observation.observed_at_ms < previous.observed_at_ms
            }) {
                cache.consistent[index] = false;
                cache.upstream_available[index] = true;
                return;
            }
            cache.latest[index] = Some(observation);
            cache.upstream_available[index] = true;
            cache.consistent[index] = true;
        });
    }

    fn unavailable(&self, asset: AssetId) {
        self.oracle_cache.send_modify(|cache| cache.upstream_available[asset_index(asset)] = false);
    }

    #[cfg(test)]
    async fn wait_for_all_upstream(&self, bound: Duration) -> bool {
        let mut cache = self.oracle_cache.subscribe();
        if cache.borrow().all_upstream_available() {
            return true;
        }
        tokio::time::timeout(bound, async move {
            loop {
                if cache.changed().await.is_err() {
                    return false;
                }
                if cache.borrow().all_upstream_available() {
                    return true;
                }
            }
        })
        .await
        .unwrap_or(false)
    }
}

#[derive(Clone, Debug)]
struct LifecycleMarketView {
    state: Arc<LifecycleState>,
}

impl LifecycleMarketView {
    fn new(state: Arc<LifecycleState>) -> Self {
        Self { state }
    }
}

impl MarketView for LifecycleMarketView {
    fn snapshot(&self) -> Result<MarketSnapshot, MarketViewError> {
        // One wall-clock read makes this projection atomic across all assets.
        let captured_at = SystemClock.now_ms();
        project_market_snapshot(self.state.snapshot(), captured_at)
    }
}

fn project_market_snapshot(
    cache: OracleCacheSnapshot,
    captured_at: u64,
) -> Result<MarketSnapshot, MarketViewError> {
    let observations = AssetId::ALL
        .into_iter()
        .map(|asset| {
            let index = asset_index(asset);
            if !cache.consistent[index] {
                return Err(MarketViewError::InvalidSnapshot);
            }
            let observation = cache.latest[index].ok_or(MarketViewError::Unavailable)?;
            if observation.asset != asset {
                return Err(MarketViewError::InvalidSnapshot);
            }
            let age_ms = captured_at
                .checked_sub(observation.observed_at_ms)
                .ok_or(MarketViewError::InvalidSnapshot)?;
            Ok(MarketObservation {
                asset,
                oracle_price: observation.price,
                observed_at: observation.observed_at_ms,
                // Availability is immediate cache state. Timestamp freshness is
                // recomputed locally; no upstream stale classification is trusted.
                is_stale: !cache.upstream_available[index] || age_ms > MAX_OBSERVATION_AGE_MS,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(MarketSnapshot { captured_at, observations })
}

/// Synchronous WS projections only read this immutable cache. It is populated
/// through bounded async runtime requests before the listener starts, so the
/// adapter's synchronous `SnapshotView` seam never blocks the runtime owner.
#[derive(Debug)]
struct LifecycleSnapshotView {
    state: Arc<LifecycleState>,
    sequence: u64,
    books: BTreeMap<AssetId, BookSnapshot>,
}

impl LifecycleSnapshotView {
    fn new(
        state: Arc<LifecycleState>,
        sequence: u64,
        books: impl IntoIterator<Item = BookSnapshot>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state,
            sequence,
            books: books.into_iter().map(|book| (book.asset, book)).collect(),
        })
    }
}

impl SnapshotView for LifecycleSnapshotView {
    fn initial(&self, subscription: &Subscription) -> Result<Option<ViewMessage>, ViewError> {
        let message = match subscription {
            Subscription::AllMids {} => Some(ViewMessage::new(
                EventChannel::AllMids,
                self.sequence,
                fresh_all_mids(self.state.snapshot())?,
            )),
            Subscription::L2Book { coin } => Some(ViewMessage::new(
                EventChannel::L2Book,
                self.sequence,
                project_l2_book(
                    self.books.get(coin).ok_or_else(|| ViewError::new("book unavailable"))?,
                ),
            )),
            Subscription::OrderUpdates { .. } => Some(ViewMessage::new(
                EventChannel::OrderUpdates,
                self.sequence,
                serde_json::json!([]),
            )),
            Subscription::Trades { .. } => None,
        };
        Ok(message)
    }

    fn update(
        &self,
        _subscription: &Subscription,
        _record: &EventRecord,
    ) -> Result<Option<ViewMessage>, ViewError> {
        // Live event projections are deliberately deferred to WS-B.
        Ok(None)
    }
}

async fn refresh_snapshot_view(
    runtime: &dyn RuntimePort,
    state: Arc<LifecycleState>,
    bound: Duration,
) -> Result<Arc<dyn SnapshotView>, LifecycleError> {
    let sequence =
        match request_with_timeout(runtime, RuntimeRequest::EngineSnapshot, bound).await? {
            RuntimeReply::EngineSnapshot(snapshot) => snapshot.sequence,
            _ => return Err(LifecycleError::UnexpectedRuntimeReply),
        };
    let mut books = Vec::with_capacity(AssetId::ALL.len());
    for asset in AssetId::ALL {
        match request_with_timeout(runtime, RuntimeRequest::Book(asset), bound).await? {
            RuntimeReply::Book(book) if book.asset == asset => books.push(book),
            _ => return Err(LifecycleError::UnexpectedRuntimeReply),
        }
    }
    Ok(LifecycleSnapshotView::new(state, sequence, books))
}

fn fresh_all_mids(cache: OracleCacheSnapshot) -> Result<serde_json::Value, ViewError> {
    let snapshot = project_market_snapshot(cache, SystemClock.now_ms())
        .map_err(|_| ViewError::new("oracle snapshot unavailable"))?;
    if snapshot.observations.iter().any(|observation| observation.is_stale) {
        return Err(ViewError::new("oracle snapshot is stale"));
    }
    Ok(serde_json::Value::Object(
        snapshot
            .observations
            .into_iter()
            .map(|observation| {
                (
                    observation.asset.symbol().to_owned(),
                    serde_json::json!(format_price(observation.asset, observation.oracle_price)),
                )
            })
            .collect(),
    ))
}

fn project_l2_book(book: &BookSnapshot) -> serde_json::Value {
    let levels = [&book.bids, &book.asks].map(|side| {
        side.iter()
            .map(|level| {
                serde_json::json!({
                    "px": format_price(book.asset, level.price),
                    "sz": format_size(book.asset, level.quantity_lots),
                    "n": 1
                })
            })
            .collect::<Vec<_>>()
    });
    serde_json::json!({"coin": book.asset, "time": 0, "levels": levels})
}

fn format_price(asset: AssetId, price: PriceTicks) -> String {
    price.format(match asset {
        AssetId::BTC => DecimalScale::new(1).expect("fixed BTC price scale"),
        AssetId::ETH => DecimalScale::new(2).expect("fixed ETH price scale"),
        AssetId::SOL => DecimalScale::new(3).expect("fixed SOL price scale"),
    })
}

fn format_size(asset: AssetId, lots: u128) -> String {
    let places = match asset {
        AssetId::BTC => 5,
        AssetId::ETH => 4,
        AssetId::SOL => 2,
    };
    let digits = lots.to_string();
    if digits.len() <= places {
        return format!("0.{lots:0>places$}")
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_owned();
    }
    let split = digits.len() - places;
    format!("{}.{}", &digits[..split], &digits[split..])
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_owned()
}

async fn healthz() -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::OK, Json(serde_json::json!({"status": "alive"})))
}

#[derive(Clone)]
struct ReadinessState {
    runtime: Arc<dyn RuntimePort>,
    market: Arc<dyn MarketView>,
    reply_timeout: Duration,
}

async fn readyz(State(state): State<ReadinessState>) -> (StatusCode, Json<serde_json::Value>) {
    let runtime_ready = runtime_oracle_ready(&*state.runtime, state.reply_timeout).await;
    let local_ready = state.market.snapshot().is_ok_and(|snapshot| {
        snapshot.observations.len() == AssetId::ALL.len()
            && AssetId::ALL.into_iter().all(|asset| {
                snapshot
                    .observations
                    .iter()
                    .any(|observation| observation.asset == asset && !observation.is_stale)
            })
    });
    if runtime_ready && local_ready {
        (StatusCode::OK, Json(serde_json::json!({"status": "ready"})))
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"status": "not_ready"})))
    }
}

async fn runtime_oracle_ready(runtime: &dyn RuntimePort, bound: Duration) -> bool {
    let Ok(pending) = runtime.try_request(RuntimeRequest::OracleHealth) else {
        return false;
    };
    let Ok(Ok(RuntimeReply::OracleHealth(health))) =
        tokio::time::timeout(bound, pending.receive()).await
    else {
        return false;
    };
    oracle_health_is_ready(health)
}

fn oracle_health_is_ready(health: RuntimeOracleHealth) -> bool {
    health.assets.into_iter().zip(AssetId::ALL).all(|(entry, expected)| {
        matches!(
            entry,
            OracleAssetHealth::Observed {
                asset,
                freshness: oracle_hyperliquid::OracleFreshness::Fresh { age_ms },
                ..
            } if asset == expected && age_ms <= MAX_OBSERVATION_AGE_MS
        )
    })
}

fn http_config(config: &Config) -> HttpConfig {
    HttpConfig {
        max_batch: config.max_batch_size.get(),
        max_body_bytes: config.max_body_bytes.get(),
        max_observation_age_ms: MAX_OBSERVATION_AGE_MS,
        runtime_reply_timeout: config.reply_timeout,
    }
}

fn ws_limits(config: &Config) -> WsLimits {
    WsLimits::new(config.max_ws_subscriptions.get(), config.ws_outbound_capacity.get())
}

fn service_router(
    config: &Config,
    runtime: Arc<dyn RuntimePort>,
    market: Arc<dyn MarketView>,
    snapshots: Arc<dyn SnapshotView>,
) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(ReadinessState {
            runtime: Arc::clone(&runtime),
            market: Arc::clone(&market),
            reply_timeout: (config.reply_timeout / 2).max(Duration::from_millis(1)),
        })
        .merge(router_with_config(Arc::clone(&runtime), market, http_config(config)))
        .merge(sim_server::ws::router(runtime, snapshots, ws_limits(config)))
        .layer(TimeoutLayer::with_status_code(StatusCode::REQUEST_TIMEOUT, config.reply_timeout))
        .layer(ConcurrencyLimitLayer::new(config.max_concurrent_requests.get()))
}

#[derive(Clone, Debug)]
struct LiveOracleFactory {
    config: LiveConfig,
}

impl LiveOracleFactory {
    fn from_config(config: &Config) -> Self {
        Self {
            config: LiveConfig {
                ws_url: config.oracle_wss_url.to_string(),
                info_url: config.oracle_info_url.to_string(),
                connect_timeout: LIVE_CONNECT_TIMEOUT,
                write_timeout: LIVE_WRITE_TIMEOUT,
                read_timeout: config.oracle_completeness_timeout,
                send_timeout: LIVE_SEND_TIMEOUT,
                heartbeat_interval: LIVE_HEARTBEAT_INTERVAL,
            },
        }
    }
}

impl TransportFactory for LiveOracleFactory {
    type Transport = TokioHyperliquidTransport<SystemClock>;

    fn create(&mut self) -> Self::Transport {
        TokioHyperliquidTransport::new(self.config.clone(), SystemClock)
    }
}

#[derive(Debug)]
enum LifecycleError {
    Runtime(RuntimeError),
    RuntimeReplyTimedOut,
    UnexpectedRuntimeReply,
    IngestionTaskFailed,
    IngestionTaskTimedOut,
    RuntimeTask(RuntimeTaskError),
    Listener(io::Error),
    ListenerTaskFailed,
    ListenerTaskTimedOut,
    Signal(std::io::Error),
}

impl fmt::Display for LifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(error) => write!(formatter, "runtime boundary failed: {error}"),
            Self::RuntimeReplyTimedOut => formatter.write_str("runtime reply exceeded its bound"),
            Self::UnexpectedRuntimeReply => {
                formatter.write_str("runtime returned an unexpected reply")
            }
            Self::IngestionTaskFailed => formatter.write_str("oracle ingestion task failed"),
            Self::IngestionTaskTimedOut => {
                formatter.write_str("oracle ingestion did not stop within its bound")
            }
            Self::RuntimeTask(error) => write!(formatter, "runtime owner failed: {error}"),
            Self::Listener(error) => write!(formatter, "listener failed: {error}"),
            Self::ListenerTaskFailed => formatter.write_str("listener task failed"),
            Self::ListenerTaskTimedOut => {
                formatter.write_str("listener did not stop within its bound")
            }
            Self::Signal(error) => write!(formatter, "shutdown signal handler failed: {error}"),
        }
    }
}

struct ServiceLifecycle {
    state: Arc<LifecycleState>,
    runtime: RuntimeHandle,
    runtime_task: RuntimeTask,
    cancel: watch::Sender<bool>,
    ingestion_task: JoinHandle<Result<(), LifecycleError>>,
    listener_task: Option<JoinHandle<Result<(), io::Error>>>,
    reply_timeout: Duration,
    shutdown_timeout: Duration,
}

impl ServiceLifecycle {
    fn attach_listener(&mut self, task: JoinHandle<Result<(), io::Error>>) {
        self.listener_task = Some(task);
    }

    async fn shutdown(self) -> Result<(), LifecycleError> {
        let Self {
            runtime,
            runtime_task,
            cancel,
            mut ingestion_task,
            listener_task,
            reply_timeout,
            shutdown_timeout,
            ..
        } = self;

        let _ = cancel.send(true);
        let ingestion_result =
            match tokio::time::timeout(shutdown_timeout, &mut ingestion_task).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err(LifecycleError::IngestionTaskFailed),
                Err(_) => {
                    ingestion_task.abort();
                    let _ = ingestion_task.await;
                    Err(LifecycleError::IngestionTaskTimedOut)
                }
            };

        // Publish the runtime's explicit shutting-down freshness before waiting
        // for Axum. Active WebSockets then close themselves, allowing graceful
        // listener release instead of forcing the listener task deadline.
        let shutdown_result =
            request_with_timeout(&runtime, RuntimeRequest::Shutdown, reply_timeout).await.and_then(
                |reply| {
                    if reply == RuntimeReply::Shutdown {
                        Ok(())
                    } else {
                        Err(LifecycleError::UnexpectedRuntimeReply)
                    }
                },
            );
        let listener_result = if let Some(mut listener_task) = listener_task {
            match tokio::time::timeout(shutdown_timeout, &mut listener_task).await {
                Ok(Ok(result)) => result.map_err(LifecycleError::Listener),
                Ok(Err(_)) => Err(LifecycleError::ListenerTaskFailed),
                Err(_) => {
                    listener_task.abort();
                    let _ = listener_task.await;
                    Err(LifecycleError::ListenerTaskTimedOut)
                }
            }
        } else {
            Ok(())
        };
        drop(runtime);
        let owner_result =
            runtime_task.wait(shutdown_timeout).await.map_err(LifecycleError::RuntimeTask);

        ingestion_result?;
        shutdown_result?;
        listener_result?;
        owner_result
    }
}

fn start_lifecycle(config: &Config) -> ServiceLifecycle {
    let limits = RuntimeLimits::new(config.command_capacity, config.event_capacity);
    let (runtime, runtime_task) = start_runtime(config.seed, limits);
    let state = LifecycleState::new(config.oracle_mode == OracleMode::Offline);
    let (cancel, cancel_receiver) = watch::channel(false);
    let event_capacity = config.event_capacity.get();
    let (oracle_events, receiver) = mpsc::channel(event_capacity);
    let producer_cancel = cancel_receiver.clone();

    let producer = match config.oracle_mode {
        OracleMode::Offline => {
            tokio::spawn(run_offline_oracle(oracle_events, producer_cancel, config.actor_interval))
        }
        OracleMode::Live => {
            let orchestrator = OracleOrchestrator::new(
                LiveOracleFactory::from_config(config),
                TokioSleeper,
                NoJitter,
                SystemClock,
                ReconnectBackoff::new(LIVE_RECONNECT_INITIAL, LIVE_RECONNECT_MAXIMUM),
                fixed_price_scales(),
            )
            .with_primary_completeness_timeout(config.oracle_completeness_timeout);
            tokio::spawn(async move {
                let _ = orchestrator.run(oracle_events, producer_cancel).await;
            })
        }
    };

    let ingestion_task = tokio::spawn(run_ingestion(
        receiver,
        cancel_receiver,
        producer,
        runtime.clone(),
        state.clone(),
        config.actors_enabled.then(|| SeededLocalActors::new(config.seed)),
        config.actor_interval,
        config.reply_timeout,
        event_capacity,
    ));

    ServiceLifecycle {
        state,
        runtime,
        runtime_task,
        cancel,
        ingestion_task,
        listener_task: None,
        reply_timeout: config.reply_timeout,
        shutdown_timeout: config.shutdown_timeout,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_ingestion(
    mut events: mpsc::Receiver<OracleIngestEvent>,
    mut cancel: watch::Receiver<bool>,
    mut producer: JoinHandle<()>,
    runtime: RuntimeHandle,
    state: Arc<LifecycleState>,
    mut actors: Option<SeededLocalActors>,
    actor_interval: Duration,
    reply_timeout: Duration,
    pending_capacity: usize,
) -> Result<(), LifecycleError> {
    let result = if actors.is_some() {
        let mut interval = tokio::time::interval(actor_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut pending = Vec::with_capacity(pending_capacity.min(64));
        loop {
            tokio::select! {
                biased;
                changed = cancel.changed() => {
                    if changed.is_err() || *cancel.borrow() {
                        break Ok(());
                    }
                }
                _ = interval.tick(), if !pending.is_empty() => {
                    let observations = std::mem::take(&mut pending);
                    forward_observations(
                        &runtime,
                        actors.as_mut(),
                        &observations,
                        SystemClock.now_ms(),
                        reply_timeout,
                    ).await?;
                }
                event = events.recv(), if pending.len() < pending_capacity => match event {
                    Some(OracleIngestEvent::Observation(observation)) => {
                        state.observe(observation);
                        pending.push(observation);
                    }
                    Some(OracleIngestEvent::Unavailable { asset, .. }) => state.unavailable(asset),
                    None => break Ok(()),
                }
            }
        }
    } else {
        loop {
            tokio::select! {
                biased;
                changed = cancel.changed() => {
                    if changed.is_err() || *cancel.borrow() {
                        break Ok(());
                    }
                }
                event = events.recv() => match event {
                    Some(OracleIngestEvent::Observation(observation)) => {
                        state.observe(observation);
                        forward_observations(
                            &runtime,
                            None,
                            &[observation],
                            observation.observed_at_ms,
                            reply_timeout,
                        ).await?;
                    }
                    Some(OracleIngestEvent::Unavailable { asset, .. }) => state.unavailable(asset),
                    None => break Ok(()),
                }
            }
        }
    };

    let producer_aborted = !producer.is_finished() && !*cancel.borrow();
    if producer_aborted {
        producer.abort();
    }
    match tokio::time::timeout(reply_timeout, &mut producer).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) if producer_aborted => {}
        Ok(Err(_)) => return Err(LifecycleError::IngestionTaskFailed),
        Err(_) => {
            producer.abort();
            let _ = producer.await;
        }
    }
    result
}

async fn forward_observations(
    runtime: &dyn RuntimePort,
    actors: Option<&mut SeededLocalActors>,
    observations: &[OracleObservation],
    logical_time: u64,
    reply_timeout: Duration,
) -> Result<(), LifecycleError> {
    if let Some(actors) = actors {
        tokio::time::timeout(reply_timeout, actors.run_step(runtime, logical_time, observations))
            .await
            .map_err(|_| LifecycleError::RuntimeReplyTimedOut)?
            .map_err(LifecycleError::Runtime)?;
        return Ok(());
    }

    for observation in observations {
        let reply = request_with_timeout(
            runtime,
            RuntimeRequest::ObserveOracle(*observation),
            reply_timeout,
        )
        .await?;
        if !matches!(reply, RuntimeReply::OracleObserved(Ok(()))) {
            return Err(LifecycleError::UnexpectedRuntimeReply);
        }
    }
    Ok(())
}

async fn request_with_timeout(
    runtime: &dyn RuntimePort,
    request: RuntimeRequest,
    bound: Duration,
) -> Result<RuntimeReply, LifecycleError> {
    let deadline = Instant::now() + bound;
    let pending = loop {
        match runtime.try_request(request.clone()) {
            Ok(pending) => break pending,
            Err(RuntimeError::Overloaded) if Instant::now() < deadline => {
                tokio::task::yield_now().await;
            }
            Err(error) => return Err(LifecycleError::Runtime(error)),
        }
    };
    let remaining = deadline.saturating_duration_since(Instant::now());
    tokio::time::timeout(remaining, pending.receive())
        .await
        .map_err(|_| LifecycleError::RuntimeReplyTimedOut)?
        .map_err(LifecycleError::Runtime)
}

async fn run_offline_oracle(
    output: mpsc::Sender<OracleIngestEvent>,
    mut cancel: watch::Receiver<bool>,
    period: Duration,
) {
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut sequence = 0_u64;
    loop {
        tokio::select! {
            biased;
            changed = cancel.changed() => {
                if changed.is_err() || *cancel.borrow() {
                    return;
                }
            }
            _ = interval.tick() => {
                sequence = sequence.saturating_add(1);
                let observed_at_ms = SystemClock.now_ms();
                for observation in synthetic_observations(observed_at_ms, sequence) {
                    if output.send(OracleIngestEvent::Observation(observation)).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

/// Fixed deterministic demo values. These are synthetic ticks, never fetched from
/// Hyperliquid or represented as executable upstream prices.
fn synthetic_observations(observed_at_ms: u64, upstream_sequence: u64) -> [OracleObservation; 3] {
    [
        synthetic_observation(AssetId::BTC, 1_000_000, observed_at_ms, upstream_sequence),
        synthetic_observation(AssetId::ETH, 300_000, observed_at_ms, upstream_sequence),
        synthetic_observation(AssetId::SOL, 150_000, observed_at_ms, upstream_sequence),
    ]
}

fn synthetic_observation(
    asset: AssetId,
    ticks: i64,
    observed_at_ms: u64,
    upstream_sequence: u64,
) -> OracleObservation {
    OracleObservation {
        asset,
        price: PriceTicks::new(ticks).expect("fixed synthetic price is positive"),
        observed_at_ms,
        upstream_sequence,
        // The frozen oracle source enum has no synthetic variant. LifecycleState::synthetic
        // is the explicit local provenance marker consumed by later projections/readiness.
        source: ObservationSource::MetaAndAssetCtxs,
    }
}

fn fixed_price_scales() -> PriceScales {
    PriceScales::new(
        DecimalScale::new(1).expect("BTC price scale is valid"),
        DecimalScale::new(2).expect("ETH price scale is valid"),
        DecimalScale::new(3).expect("SOL price scale is valid"),
    )
}

const fn asset_index(asset: AssetId) -> usize {
    match asset {
        AssetId::BTC => 0,
        AssetId::ETH => 1,
        AssetId::SOL => 2,
    }
}

async fn wait_for_shutdown_signal() -> Result<(), LifecycleError> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(LifecycleError::Signal)?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.map_err(LifecycleError::Signal),
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.map_err(LifecycleError::Signal)
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("sim-server configuration invalid: {error}");
            return ExitCode::from(2);
        }
    };
    println!("{}", config.validation_summary());

    let listener = match TcpListener::bind(config.bind_addr).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("sim-server listener bind failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    let local_addr = match listener.local_addr() {
        Ok(address) => address,
        Err(error) => {
            eprintln!("sim-server listener address failed: {error}");
            return ExitCode::FAILURE;
        }
    };

    let mut lifecycle = start_lifecycle(&config);
    let router_runtime: Arc<dyn RuntimePort> = Arc::new(lifecycle.runtime.clone());
    let router_snapshots = match refresh_snapshot_view(
        &*router_runtime,
        lifecycle.state.clone(),
        config.reply_timeout,
    )
    .await
    {
        Ok(snapshots) => snapshots,
        Err(error) => {
            eprintln!("sim-server WebSocket snapshot startup failed: {error}");
            let _ = lifecycle.shutdown().await;
            return ExitCode::FAILURE;
        }
    };
    let mut listener_cancel = lifecycle.cancel.subscribe();
    let (listener_done, listener_stopped) = oneshot::channel();
    let router_market: Arc<dyn MarketView> =
        Arc::new(LifecycleMarketView::new(lifecycle.state.clone()));
    let listener_task = tokio::spawn(async move {
        let result = axum::serve(
            listener,
            service_router(&config, router_runtime, router_market, router_snapshots),
        )
        .with_graceful_shutdown(async move {
            if *listener_cancel.borrow() {
                return;
            }
            while listener_cancel.changed().await.is_ok() {
                if *listener_cancel.borrow() {
                    return;
                }
            }
        })
        .await;
        let _ = listener_done.send(());
        result
    });
    lifecycle.attach_listener(listener_task);
    println!(
        "sim-server readiness: lifecycle=running synthetic={} assets=BTC,ETH,SOL listener={local_addr}",
        lifecycle.state.snapshot().synthetic,
    );

    let trigger_result = tokio::select! {
        result = wait_for_shutdown_signal() => result,
        _ = listener_stopped => Err(LifecycleError::ListenerTaskFailed),
    };
    let shutdown_result = lifecycle.shutdown().await;
    if let Err(error) = trigger_result {
        eprintln!("sim-server lifecycle failed: {error}");
        if let Err(shutdown_error) = shutdown_result {
            eprintln!("sim-server shutdown failed: {shutdown_error}");
        }
        return ExitCode::FAILURE;
    }
    match shutdown_result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("sim-server shutdown failed: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use http_body_util::BodyExt;
    use oracle_hyperliquid::{OracleFreshness, OracleTransport};
    use sim_core::Engine;
    use sim_server::runtime::{OracleAssetHealth, RuntimeOracleHealth};
    use sim_server::{RuntimeReply, bounded_runtime};
    use std::{num::NonZeroUsize, sync::Mutex};
    use tower::ServiceExt;

    fn test_config(actors_enabled: bool) -> Config {
        Config::from_pairs(&[
            ("SIM_ACTORS_ENABLED", if actors_enabled { "true" } else { "false" }),
            ("SIM_ACTOR_INTERVAL_MS", "5"),
            ("SIM_REPLY_TIMEOUT_MS", "500"),
            ("SIM_SHUTDOWN_TIMEOUT_MS", "500"),
        ])
        .expect("test lifecycle config")
    }

    fn test_snapshot_view(state: Arc<LifecycleState>) -> Arc<dyn SnapshotView> {
        let snapshot = Engine::new(7).snapshot();
        LifecycleSnapshotView::new(state, snapshot.sequence, snapshot.books)
    }

    async fn runtime_request(runtime: &dyn RuntimePort, request: RuntimeRequest) -> RuntimeReply {
        request_with_timeout(runtime, request, Duration::from_millis(500))
            .await
            .expect("runtime request")
    }

    #[tokio::test]
    async fn offline_mode_makes_all_three_synthetic_assets_fresh() {
        let lifecycle = start_lifecycle(&test_config(false));
        assert!(lifecycle.state.wait_for_all_upstream(Duration::from_millis(500)).await);
        let cache = lifecycle.state.snapshot();
        assert!(cache.synthetic);
        assert_eq!(
            cache.latest.map(|item| item.map(|observation| observation.asset)),
            [Some(AssetId::BTC), Some(AssetId::ETH), Some(AssetId::SOL)]
        );

        let RuntimeReply::OracleHealth(health) =
            runtime_request(&lifecycle.runtime, RuntimeRequest::OracleHealth).await
        else {
            panic!("oracle health reply")
        };
        for asset in health.assets {
            assert!(matches!(
                asset,
                OracleAssetHealth::Observed { freshness: OracleFreshness::Fresh { .. }, .. }
            ));
        }
        lifecycle.shutdown().await.expect("bounded shutdown");
    }

    #[test]
    fn live_factory_exposes_only_the_read_only_transport_capability() {
        fn assert_factory<F>(_: &F)
        where
            F: TransportFactory,
            F::Transport: OracleTransport,
        {
        }
        let config = Config::from_pairs(&[("SIM_ORACLE_MODE", "live")]).expect("live config");
        let factory = LiveOracleFactory::from_config(&config);
        assert_factory(&factory);
        assert_eq!(factory.config.info_url, "https://api.hyperliquid.xyz/info");
        assert_eq!(factory.config.ws_url, "wss://api.hyperliquid.xyz/ws");
    }

    #[tokio::test]
    async fn cancellation_and_join_exit_within_the_configured_bound() {
        let lifecycle = start_lifecycle(&test_config(false));
        tokio::time::timeout(Duration::from_secs(1), lifecycle.shutdown())
            .await
            .expect("shutdown itself is bounded")
            .expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn graceful_shutdown_terminates_the_runtime_owner() {
        let lifecycle = start_lifecycle(&test_config(false));
        let runtime = lifecycle.runtime.clone();
        lifecycle.shutdown().await.expect("shutdown succeeds");
        assert_eq!(
            runtime.try_request(RuntimeRequest::EngineSnapshot).unwrap_err(),
            RuntimeError::ShuttingDown
        );
    }

    #[tokio::test]
    async fn actor_toggle_never_submits_an_observation_twice() {
        for actors_enabled in [false, true] {
            let limits =
                RuntimeLimits::new(NonZeroUsize::new(32).unwrap(), NonZeroUsize::new(32).unwrap());
            let (runtime, mut owner, _) = bounded_runtime(limits);
            let recorded = Arc::new(Mutex::new(Vec::new()));
            let owner_recorded = recorded.clone();
            let owner_task = tokio::spawn(async move {
                let mut engine = Engine::new(7);
                while let Some(envelope) = owner.recv().await {
                    owner_recorded.lock().unwrap().push(envelope.request.clone());
                    let reply = match &envelope.request {
                        RuntimeRequest::ObserveOracle(_) => RuntimeReply::OracleObserved(Ok(())),
                        RuntimeRequest::Apply(command) => {
                            RuntimeReply::Applied(engine.apply(command.clone()))
                        }
                        RuntimeRequest::Shutdown => {
                            let _ = envelope.respond(Ok(RuntimeReply::Shutdown));
                            break;
                        }
                        _ => panic!("unexpected test request"),
                    };
                    let _ = envelope.respond(Ok(reply));
                }
            });
            let observations = synthetic_observations(SystemClock.now_ms(), 1);
            let mut actors = actors_enabled.then(|| SeededLocalActors::new(7));
            forward_observations(
                &runtime,
                actors.as_mut(),
                &observations,
                SystemClock.now_ms(),
                Duration::from_millis(500),
            )
            .await
            .expect("forward observations");

            let observed = {
                let requests = recorded.lock().unwrap();
                requests
                    .iter()
                    .filter(|request| matches!(request, RuntimeRequest::ObserveOracle(_)))
                    .count()
            };
            assert_eq!(observed, observations.len(), "actors_enabled={actors_enabled}");
            let _ = runtime_request(&runtime, RuntimeRequest::Shutdown).await;
            owner_task.await.expect("test owner exits");
        }
    }

    fn cache_with_observations(observed_at_ms: u64) -> OracleCacheSnapshot {
        let mut cache = OracleCacheSnapshot::empty(false);
        cache.latest = synthetic_observations(observed_at_ms, 1).map(Some);
        cache.upstream_available = [true; 3];
        cache
    }

    #[test]
    fn market_projection_is_fresh_at_60_000_ms_and_stale_at_60_001_ms() {
        for (age_ms, expected_stale) in [(60_000, false), (60_001, true)] {
            let snapshot = project_market_snapshot(cache_with_observations(10), 10 + age_ms)
                .expect("complete cache projects");
            assert_eq!(snapshot.captured_at, 10 + age_ms);
            assert_eq!(
                snapshot.observations.iter().map(|item| item.asset).collect::<Vec<_>>(),
                AssetId::ALL
            );
            assert!(
                snapshot.observations.iter().all(|item| item.is_stale == expected_stale),
                "age_ms={age_ms}"
            );
        }
    }

    #[test]
    fn market_projection_fails_closed_on_future_and_missing_cache_state() {
        assert_eq!(
            project_market_snapshot(cache_with_observations(11), 10),
            Err(MarketViewError::InvalidSnapshot)
        );
        assert_eq!(
            project_market_snapshot(OracleCacheSnapshot::empty(false), 10),
            Err(MarketViewError::Unavailable)
        );
    }

    #[test]
    fn market_projection_fails_closed_after_regressed_cache_input() {
        let state = LifecycleState::new(false);
        for observation in synthetic_observations(10, 2) {
            state.observe(observation);
        }
        state.observe(synthetic_observation(AssetId::BTC, 2_000_000, 9, 1));
        assert_eq!(
            project_market_snapshot(state.snapshot(), 10),
            Err(MarketViewError::InvalidSnapshot)
        );
    }

    #[test]
    fn unavailable_cache_state_immediately_marks_retained_observation_stale() {
        let state = LifecycleState::new(false);
        for observation in synthetic_observations(10, 1) {
            state.observe(observation);
        }
        state.unavailable(AssetId::BTC);
        let snapshot =
            project_market_snapshot(state.snapshot(), 10).expect("retained prices project");
        assert!(snapshot.observations[0].is_stale);
        assert!(!snapshot.observations[1].is_stale);
        assert!(!snapshot.observations[2].is_stale);
    }

    fn fresh_runtime_health() -> RuntimeOracleHealth {
        RuntimeOracleHealth {
            assets: AssetId::ALL.map(|asset| OracleAssetHealth::Observed {
                asset,
                source: ObservationSource::MetaAndAssetCtxs,
                upstream_sequence: 1,
                observed_at_ms: 1,
                freshness: OracleFreshness::Fresh { age_ms: 0 },
            }),
        }
    }

    async fn readiness_response(router: Router) -> (StatusCode, String) {
        let response = router
            .oneshot(Request::builder().uri("/readyz").body(Body::empty()).unwrap())
            .await
            .expect("readiness response");
        let status = response.status();
        let body = response.into_body().collect().await.expect("readiness body").to_bytes();
        (status, String::from_utf8(body.to_vec()).expect("utf8 readiness body"))
    }

    #[test]
    fn validated_http_bounds_map_without_reinterpretation() {
        let config = Config::from_pairs(&[
            ("SIM_MAX_BATCH_SIZE", "7"),
            ("SIM_MAX_BODY_BYTES", "1234"),
            ("SIM_REPLY_TIMEOUT_MS", "4321"),
        ])
        .expect("validated HTTP config");
        assert_eq!(
            http_config(&config),
            HttpConfig {
                max_batch: 7,
                max_body_bytes: 1_234,
                max_observation_age_ms: 60_000,
                runtime_reply_timeout: Duration::from_millis(4_321),
            }
        );
    }

    #[test]
    fn validated_ws_bounds_map_without_reinterpretation() {
        let config = Config::from_pairs(&[
            ("SIM_MAX_WS_SUBSCRIPTIONS", "7"),
            ("SIM_WS_OUTBOUND_CAPACITY", "19"),
        ])
        .expect("validated WebSocket config");
        assert_eq!(ws_limits(&config), WsLimits::new(7, 19));
    }

    #[tokio::test]
    async fn readiness_transitions_from_503_to_200_when_local_cache_completes() {
        let limits = RuntimeLimits::new(NonZeroUsize::new(8).unwrap(), NonZeroUsize::MIN);
        let (runtime, mut owner, _) = bounded_runtime(limits);
        let owner_task = tokio::spawn(async move {
            while let Some(envelope) = owner.recv().await {
                assert_eq!(envelope.request, RuntimeRequest::OracleHealth);
                let _ = envelope.respond(Ok(RuntimeReply::OracleHealth(fresh_runtime_health())));
            }
        });
        let state = LifecycleState::new(false);
        let config = test_config(false);
        let router = service_router(
            &config,
            Arc::new(runtime.clone()),
            Arc::new(LifecycleMarketView::new(state.clone())),
            test_snapshot_view(state.clone()),
        );

        assert_eq!(
            readiness_response(router.clone()).await,
            (StatusCode::SERVICE_UNAVAILABLE, "{\"status\":\"not_ready\"}".to_owned())
        );
        let observed_at_ms = SystemClock.now_ms();
        for observation in synthetic_observations(observed_at_ms, 1) {
            state.observe(observation);
        }
        assert_eq!(
            readiness_response(router).await,
            (StatusCode::OK, "{\"status\":\"ready\"}".to_owned())
        );
        drop(runtime);
        owner_task.await.expect("readiness owner exits");
    }

    #[tokio::test]
    async fn readiness_returns_stable_503_when_runtime_is_shutting_down() {
        let limits = RuntimeLimits::new(NonZeroUsize::MIN, NonZeroUsize::MIN);
        let (runtime, owner, _) = bounded_runtime(limits);
        drop(owner);
        let state = LifecycleState::new(false);
        let observed_at_ms = SystemClock.now_ms();
        for observation in synthetic_observations(observed_at_ms, 1) {
            state.observe(observation);
        }
        let response = readiness_response(service_router(
            &test_config(false),
            Arc::new(runtime),
            Arc::new(LifecycleMarketView::new(state.clone())),
            test_snapshot_view(state),
        ))
        .await;
        assert_eq!(
            response,
            (StatusCode::SERVICE_UNAVAILABLE, "{\"status\":\"not_ready\"}".to_owned())
        );
    }

    #[tokio::test]
    async fn readiness_returns_stable_503_for_runtime_overload_and_timeout() {
        let config = Config::from_pairs(&[("SIM_REPLY_TIMEOUT_MS", "20")]).unwrap();

        let limits = RuntimeLimits::new(NonZeroUsize::MIN, NonZeroUsize::MIN);
        let (overloaded_runtime, overloaded_owner, _) = bounded_runtime(limits);
        let _occupied = overloaded_runtime
            .try_request(RuntimeRequest::EngineSnapshot)
            .expect("fill runtime queue");
        let overloaded_state = LifecycleState::new(false);
        let now = SystemClock.now_ms();
        for observation in synthetic_observations(now, 1) {
            overloaded_state.observe(observation);
        }
        assert_eq!(
            readiness_response(service_router(
                &config,
                Arc::new(overloaded_runtime),
                Arc::new(LifecycleMarketView::new(overloaded_state.clone())),
                test_snapshot_view(overloaded_state),
            ))
            .await,
            (StatusCode::SERVICE_UNAVAILABLE, "{\"status\":\"not_ready\"}".to_owned())
        );
        drop(overloaded_owner);

        let (timed_out_runtime, timed_out_owner, _) = bounded_runtime(limits);
        let timed_out_state = LifecycleState::new(false);
        for observation in synthetic_observations(SystemClock.now_ms(), 1) {
            timed_out_state.observe(observation);
        }
        assert_eq!(
            readiness_response(service_router(
                &config,
                Arc::new(timed_out_runtime),
                Arc::new(LifecycleMarketView::new(timed_out_state.clone())),
                test_snapshot_view(timed_out_state),
            ))
            .await,
            (StatusCode::SERVICE_UNAVAILABLE, "{\"status\":\"not_ready\"}".to_owned())
        );
        drop(timed_out_owner);
    }

    #[test]
    fn unavailable_is_immediate_cache_state_without_discarding_latest_value() {
        let state = LifecycleState::new(false);
        let observation = synthetic_observations(1, 1)[0];
        state.observe(observation);
        state.unavailable(AssetId::BTC);
        let cache = state.snapshot();
        assert_eq!(cache.latest[0], Some(observation));
        assert!(!cache.upstream_available[0]);
    }
}
