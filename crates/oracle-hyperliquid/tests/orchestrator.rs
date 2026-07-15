use hl_wire::{AssetId, DecimalScale, PriceTicks};
use oracle_hyperliquid::{
    Clock, Jitter, ObservationSource, OracleIngestEvent, OracleObservation, OracleOrchestrator,
    OracleTransport, OrchestratorExit, PriceScales, ReconnectBackoff, Sleeper, TransportFactory,
    UpstreamFuture,
};
use std::{
    collections::VecDeque,
    error::Error,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{mpsc, watch};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FakeError {
    Disconnected,
    TimedOut,
}

impl fmt::Display for FakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}
impl Error for FakeError {}

#[derive(Clone)]
struct FakeClock(Arc<Mutex<VecDeque<u64>>>);
impl Clock for FakeClock {
    fn now_ms(&self) -> u64 {
        self.0.lock().expect("clock lock").pop_front().expect("scripted clock")
    }
}

struct TransportScript {
    connect: Result<(), FakeError>,
    ws_delay: Duration,
    ws_delays: VecDeque<Duration>,
    ws: VecDeque<Result<OracleObservation, FakeError>>,
    fallback: Result<Vec<OracleObservation>, FakeError>,
}

struct FakeTransport {
    id: usize,
    log: Arc<Mutex<Vec<String>>>,
    script: TransportScript,
}

impl OracleTransport for FakeTransport {
    type Error = FakeError;

    fn connect(&mut self) -> UpstreamFuture<'_, (), Self::Error> {
        self.log.lock().expect("log lock").extend([
            format!("connect:{}", self.id),
            format!("subscribe:{}:BTC", self.id),
            format!("subscribe:{}:ETH", self.id),
            format!("subscribe:{}:SOL", self.id),
        ]);
        let result = self.script.connect;
        Box::pin(async move { result })
    }

    fn next_active_asset_ctx(
        &mut self,
        upstream_sequence: u64,
        _scales: PriceScales,
    ) -> UpstreamFuture<'_, OracleObservation, Self::Error> {
        self.log.lock().expect("log lock").push(format!("ws:{}:{upstream_sequence}", self.id));
        let result = self.script.ws.pop_front().expect("scripted WS result");
        let delay = self.script.ws_delays.pop_front().unwrap_or(self.script.ws_delay);
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            result
        })
    }

    fn meta_and_asset_ctxs(
        &mut self,
        upstream_sequence: u64,
        _scales: PriceScales,
    ) -> UpstreamFuture<'_, Vec<OracleObservation>, Self::Error> {
        self.log
            .lock()
            .expect("log lock")
            .push(format!("fallback:{}:{upstream_sequence}", self.id));
        let result = self.script.fallback.clone();
        Box::pin(async move { result })
    }
}

struct FakeFactory {
    scripts: VecDeque<TransportScript>,
    log: Arc<Mutex<Vec<String>>>,
    next_id: usize,
}

impl TransportFactory for FakeFactory {
    type Transport = FakeTransport;

    fn create(&mut self) -> Self::Transport {
        self.next_id += 1;
        self.log.lock().expect("log lock").push(format!("factory:{}", self.next_id));
        FakeTransport {
            id: self.next_id,
            log: Arc::clone(&self.log),
            script: self.scripts.pop_front().expect("scripted fresh transport"),
        }
    }
}

#[derive(Clone)]
struct FixedJitter {
    log: Arc<Mutex<Vec<String>>>,
}
impl Jitter for FixedJitter {
    fn apply(&mut self, base: Duration) -> Duration {
        let jittered = base + Duration::from_millis(25);
        self.log.lock().expect("log lock").push(format!(
            "jitter:{}->{}",
            base.as_millis(),
            jittered.as_millis()
        ));
        jittered
    }
}

struct FakeSleeper {
    log: Arc<Mutex<Vec<String>>>,
    calls: usize,
    stop_after: usize,
    shutdown: watch::Sender<bool>,
}
impl Sleeper for FakeSleeper {
    fn sleep(
        &mut self,
        delay: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        self.calls += 1;
        self.log.lock().expect("log lock").push(format!("sleep:{}", delay.as_millis()));
        let stop = (self.calls == self.stop_after).then(|| self.shutdown.clone());
        Box::pin(async move {
            if let Some(stop) = stop {
                stop.send(true).expect("orchestrator still watches shutdown");
                std::future::pending::<()>().await;
            }
        })
    }
}

fn scales() -> PriceScales {
    let scale = DecimalScale::new(2).expect("valid scale");
    PriceScales::new(scale, scale, scale)
}

fn observation(
    asset: AssetId,
    ticks: i64,
    observed_at_ms: u64,
    upstream_sequence: u64,
    source: ObservationSource,
) -> OracleObservation {
    OracleObservation {
        asset,
        price: PriceTicks::new(ticks).expect("positive ticks"),
        observed_at_ms,
        upstream_sequence,
        source,
    }
}

#[tokio::test]
async fn exact_ws_fallback_backoff_reconnect_recovery_and_shutdown_transcript() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let factory = FakeFactory {
        scripts: VecDeque::from([
            TransportScript {
                connect: Ok(()),
                ws_delay: Duration::ZERO,
                ws_delays: VecDeque::new(),
                ws: VecDeque::from([
                    Ok(observation(
                        AssetId::BTC,
                        6_000_000,
                        1_001,
                        1,
                        ObservationSource::ActiveAssetCtx,
                    )),
                    Ok(observation(
                        AssetId::ETH,
                        300_000,
                        1_002,
                        2,
                        ObservationSource::ActiveAssetCtx,
                    )),
                    Ok(observation(
                        AssetId::SOL,
                        14_000,
                        1_003,
                        3,
                        ObservationSource::ActiveAssetCtx,
                    )),
                    Err(FakeError::Disconnected),
                ]),
                fallback: Ok(vec![
                    observation(
                        AssetId::BTC,
                        6_000_100,
                        2_001,
                        4,
                        ObservationSource::MetaAndAssetCtxs,
                    ),
                    observation(
                        AssetId::ETH,
                        300_100,
                        2_001,
                        4,
                        ObservationSource::MetaAndAssetCtxs,
                    ),
                    observation(
                        AssetId::SOL,
                        14_100,
                        2_001,
                        4,
                        ObservationSource::MetaAndAssetCtxs,
                    ),
                ]),
            },
            TransportScript {
                connect: Ok(()),
                ws_delay: Duration::ZERO,
                ws_delays: VecDeque::new(),
                ws: VecDeque::from([
                    Ok(observation(
                        AssetId::BTC,
                        6_000_200,
                        3_001,
                        5,
                        ObservationSource::ActiveAssetCtx,
                    )),
                    Err(FakeError::TimedOut),
                ]),
                // ETH is malformed upstream and rejected in isolation; valid BTC/SOL survive.
                fallback: Ok(vec![
                    observation(
                        AssetId::BTC,
                        6_000_300,
                        4_001,
                        6,
                        ObservationSource::MetaAndAssetCtxs,
                    ),
                    observation(
                        AssetId::SOL,
                        14_300,
                        4_001,
                        6,
                        ObservationSource::MetaAndAssetCtxs,
                    ),
                ]),
            },
        ]),
        log: Arc::clone(&log),
        next_id: 0,
    };
    let sleeper =
        FakeSleeper { log: Arc::clone(&log), calls: 0, stop_after: 2, shutdown: shutdown_tx };
    let jitter = FixedJitter { log: Arc::clone(&log) };
    let clock = FakeClock(Arc::new(Mutex::new(VecDeque::from([2_000, 4_000]))));
    let orchestrator = OracleOrchestrator::new(
        factory,
        sleeper,
        jitter,
        clock,
        ReconnectBackoff::new(Duration::from_millis(100), Duration::from_millis(350)),
        scales(),
    );
    let (output_tx, mut output_rx) = mpsc::channel(32);

    let exit = orchestrator.run(output_tx, shutdown_rx).await;
    assert_eq!(exit, OrchestratorExit::Shutdown);

    let mut events = Vec::new();
    while let Some(event) = output_rx.recv().await {
        events.push(event);
    }
    assert_eq!(
        events,
        vec![
            OracleIngestEvent::Observation(observation(
                AssetId::BTC,
                6_000_000,
                1_001,
                1,
                ObservationSource::ActiveAssetCtx
            )),
            OracleIngestEvent::Observation(observation(
                AssetId::ETH,
                300_000,
                1_002,
                2,
                ObservationSource::ActiveAssetCtx
            )),
            OracleIngestEvent::Observation(observation(
                AssetId::SOL,
                14_000,
                1_003,
                3,
                ObservationSource::ActiveAssetCtx
            )),
            OracleIngestEvent::Unavailable { asset: AssetId::BTC, at_ms: 2_000 },
            OracleIngestEvent::Unavailable { asset: AssetId::ETH, at_ms: 2_000 },
            OracleIngestEvent::Unavailable { asset: AssetId::SOL, at_ms: 2_000 },
            OracleIngestEvent::Observation(observation(
                AssetId::BTC,
                6_000_100,
                2_001,
                4,
                ObservationSource::MetaAndAssetCtxs
            )),
            OracleIngestEvent::Observation(observation(
                AssetId::ETH,
                300_100,
                2_001,
                4,
                ObservationSource::MetaAndAssetCtxs
            )),
            OracleIngestEvent::Observation(observation(
                AssetId::SOL,
                14_100,
                2_001,
                4,
                ObservationSource::MetaAndAssetCtxs
            )),
            OracleIngestEvent::Observation(observation(
                AssetId::BTC,
                6_000_200,
                3_001,
                5,
                ObservationSource::ActiveAssetCtx
            )),
            OracleIngestEvent::Unavailable { asset: AssetId::BTC, at_ms: 4_000 },
            OracleIngestEvent::Unavailable { asset: AssetId::ETH, at_ms: 4_000 },
            OracleIngestEvent::Unavailable { asset: AssetId::SOL, at_ms: 4_000 },
            OracleIngestEvent::Observation(observation(
                AssetId::BTC,
                6_000_300,
                4_001,
                6,
                ObservationSource::MetaAndAssetCtxs
            )),
            OracleIngestEvent::Observation(observation(
                AssetId::SOL,
                14_300,
                4_001,
                6,
                ObservationSource::MetaAndAssetCtxs
            )),
        ]
    );
    assert_eq!(
        *log.lock().expect("log lock"),
        vec![
            "factory:1",
            "connect:1",
            "subscribe:1:BTC",
            "subscribe:1:ETH",
            "subscribe:1:SOL",
            "ws:1:1",
            "ws:1:2",
            "ws:1:3",
            "ws:1:4",
            "fallback:1:4",
            "jitter:100->125",
            "sleep:125",
            "factory:2",
            "connect:2",
            "subscribe:2:BTC",
            "subscribe:2:ETH",
            "subscribe:2:SOL",
            "ws:2:5",
            "ws:2:6",
            "fallback:2:6",
            "jitter:200->225",
            "sleep:225",
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn absolute_completeness_deadline_reconnects_and_resets_only_after_all_assets() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let fallback = |sequence, time| {
        Ok(AssetId::ALL
            .into_iter()
            .map(|asset| {
                observation(
                    asset,
                    10_000 + i64::from(asset.value()),
                    time,
                    sequence,
                    ObservationSource::MetaAndAssetCtxs,
                )
            })
            .collect())
    };
    let factory = FakeFactory {
        scripts: VecDeque::from([
            // Advance backoff first so a partial primary recovery cannot hide a reset.
            TransportScript {
                connect: Err(FakeError::Disconnected),
                ws_delay: Duration::ZERO,
                ws_delays: VecDeque::new(),
                ws: VecDeque::new(),
                fallback: Err(FakeError::Disconnected),
            },
            TransportScript {
                connect: Ok(()),
                ws_delay: Duration::from_millis(3),
                ws_delays: VecDeque::new(),
                ws: VecDeque::from([
                    Ok(observation(AssetId::BTC, 60_000, 11, 1, ObservationSource::ActiveAssetCtx)),
                    Ok(observation(AssetId::BTC, 60_001, 12, 2, ObservationSource::ActiveAssetCtx)),
                    Ok(observation(AssetId::BTC, 60_002, 13, 3, ObservationSource::ActiveAssetCtx)),
                    Ok(observation(AssetId::BTC, 60_003, 14, 4, ObservationSource::ActiveAssetCtx)),
                ]),
                fallback: fallback(4, 20),
            },
            TransportScript {
                connect: Ok(()),
                ws_delay: Duration::from_millis(1),
                ws_delays: VecDeque::new(),
                ws: VecDeque::from([
                    Ok(observation(AssetId::BTC, 60_100, 31, 5, ObservationSource::ActiveAssetCtx)),
                    Ok(observation(AssetId::ETH, 3_100, 32, 6, ObservationSource::ActiveAssetCtx)),
                    Ok(observation(AssetId::SOL, 150, 33, 7, ObservationSource::ActiveAssetCtx)),
                    Err(FakeError::Disconnected),
                ]),
                fallback: fallback(8, 40),
            },
        ]),
        log: Arc::clone(&log),
        next_id: 0,
    };
    let sleeper =
        FakeSleeper { log: Arc::clone(&log), calls: 0, stop_after: 3, shutdown: shutdown_tx };
    let orchestrator = OracleOrchestrator::new(
        factory,
        sleeper,
        FixedJitter { log: Arc::clone(&log) },
        FakeClock(Arc::new(Mutex::new(VecDeque::from([1_000, 2_000, 3_000])))),
        ReconnectBackoff::new(Duration::from_millis(100), Duration::from_millis(400)),
        scales(),
    )
    .with_primary_completeness_timeout(Duration::from_millis(10));
    let (output_tx, mut output_rx) = mpsc::channel(64);

    assert_eq!(orchestrator.run(output_tx, shutdown_rx).await, OrchestratorExit::Shutdown);

    let mut events = Vec::new();
    while let Some(event) = output_rx.recv().await {
        events.push(event);
    }
    let deadline_unavailable: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            OracleIngestEvent::Unavailable { asset, at_ms: 2_000 } => Some(*asset),
            _ => None,
        })
        .collect();
    assert_eq!(deadline_unavailable, vec![AssetId::ETH, AssetId::SOL]);
    assert!(events.contains(&OracleIngestEvent::Observation(observation(
        AssetId::BTC,
        60_002,
        13,
        3,
        ObservationSource::ActiveAssetCtx,
    ))));
    assert!(!events.contains(&OracleIngestEvent::Observation(observation(
        AssetId::BTC,
        60_003,
        14,
        4,
        ObservationSource::ActiveAssetCtx,
    ))));

    let transcript = log.lock().expect("log lock");
    assert!(transcript.windows(2).any(|pair| pair == ["fallback:2:4", "jitter:200->225"]));
    assert!(transcript.windows(2).any(|pair| pair == ["sleep:225", "factory:3"]));
    assert!(transcript.windows(2).any(|pair| pair == ["fallback:3:8", "jitter:100->125"]));
}

#[tokio::test(start_paused = true)]
async fn recovered_primary_still_falls_back_when_only_btc_keeps_progressing() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let fallback = |sequence, time| {
        Ok(AssetId::ALL
            .into_iter()
            .map(|asset| {
                observation(
                    asset,
                    20_000 + i64::from(asset.value()),
                    time,
                    sequence,
                    ObservationSource::MetaAndAssetCtxs,
                )
            })
            .collect())
    };
    let factory = FakeFactory {
        scripts: VecDeque::from([
            TransportScript {
                connect: Ok(()),
                ws_delay: Duration::ZERO,
                ws_delays: VecDeque::from([
                    Duration::ZERO,
                    Duration::ZERO,
                    Duration::ZERO,
                    Duration::from_millis(3),
                    Duration::from_millis(3),
                    Duration::from_millis(3),
                    Duration::from_millis(3),
                ]),
                ws: VecDeque::from([
                    Ok(observation(AssetId::BTC, 60_000, 1, 1, ObservationSource::ActiveAssetCtx)),
                    Ok(observation(AssetId::ETH, 3_000, 2, 2, ObservationSource::ActiveAssetCtx)),
                    Ok(observation(AssetId::SOL, 140, 3, 3, ObservationSource::ActiveAssetCtx)),
                    Ok(observation(AssetId::BTC, 60_001, 4, 4, ObservationSource::ActiveAssetCtx)),
                    Ok(observation(AssetId::BTC, 60_002, 5, 5, ObservationSource::ActiveAssetCtx)),
                    Ok(observation(AssetId::BTC, 60_003, 6, 6, ObservationSource::ActiveAssetCtx)),
                    Ok(observation(AssetId::BTC, 60_004, 7, 7, ObservationSource::ActiveAssetCtx)),
                ]),
                fallback: fallback(7, 10),
            },
            TransportScript {
                connect: Ok(()),
                ws_delay: Duration::ZERO,
                ws_delays: VecDeque::new(),
                ws: VecDeque::from([
                    Ok(observation(AssetId::BTC, 61_000, 20, 8, ObservationSource::ActiveAssetCtx)),
                    Ok(observation(AssetId::ETH, 3_100, 21, 9, ObservationSource::ActiveAssetCtx)),
                    Ok(observation(AssetId::SOL, 150, 22, 10, ObservationSource::ActiveAssetCtx)),
                    Err(FakeError::Disconnected),
                ]),
                fallback: fallback(11, 30),
            },
        ]),
        log: Arc::clone(&log),
        next_id: 0,
    };
    let sleeper =
        FakeSleeper { log: Arc::clone(&log), calls: 0, stop_after: 2, shutdown: shutdown_tx };
    let orchestrator = OracleOrchestrator::new(
        factory,
        sleeper,
        FixedJitter { log: Arc::clone(&log) },
        FakeClock(Arc::new(Mutex::new(VecDeque::from([1_000, 2_000])))),
        ReconnectBackoff::new(Duration::from_millis(100), Duration::from_millis(400)),
        scales(),
    )
    .with_primary_completeness_timeout(Duration::from_millis(10));
    let (output_tx, mut output_rx) = mpsc::channel(64);

    assert_eq!(orchestrator.run(output_tx, shutdown_rx).await, OrchestratorExit::Shutdown);

    let mut events = Vec::new();
    while let Some(event) = output_rx.recv().await {
        events.push(event);
    }
    let stalled: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            OracleIngestEvent::Unavailable { asset, at_ms: 1_000 } => Some(*asset),
            _ => None,
        })
        .collect();
    assert_eq!(stalled, vec![AssetId::ETH, AssetId::SOL]);
    assert!(events.contains(&OracleIngestEvent::Observation(observation(
        AssetId::BTC,
        60_003,
        6,
        6,
        ObservationSource::ActiveAssetCtx,
    ))));
    assert!(
        !events.contains(&OracleIngestEvent::Unavailable { asset: AssetId::BTC, at_ms: 1_000 })
    );

    let transcript = log.lock().expect("log lock");
    assert!(transcript.windows(2).any(|pair| pair == ["fallback:1:7", "jitter:100->125"]));
    assert!(transcript.windows(2).any(|pair| pair == ["sleep:125", "factory:2"]));
    assert!(transcript.windows(2).any(|pair| pair == ["fallback:2:11", "jitter:100->125"]));
}

#[derive(Clone, Copy)]
struct ZeroJitter;

impl Jitter for ZeroJitter {
    fn apply(&mut self, _base: Duration) -> Duration {
        Duration::ZERO
    }
}

#[tokio::test]
async fn zero_jitter_is_clamped_to_a_nonzero_retry_delay() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let factory = FakeFactory {
        scripts: VecDeque::from([TransportScript {
            connect: Err(FakeError::Disconnected),
            ws_delay: Duration::ZERO,
            ws_delays: VecDeque::new(),
            ws: VecDeque::new(),
            fallback: Err(FakeError::Disconnected),
        }]),
        log: Arc::clone(&log),
        next_id: 0,
    };
    let sleeper =
        FakeSleeper { log: Arc::clone(&log), calls: 0, stop_after: 1, shutdown: shutdown_tx };
    let orchestrator = OracleOrchestrator::new(
        factory,
        sleeper,
        ZeroJitter,
        FakeClock(Arc::new(Mutex::new(VecDeque::from([1_000])))),
        ReconnectBackoff::new(Duration::from_millis(100), Duration::from_millis(400)),
        scales(),
    );
    let (output_tx, _output_rx) = mpsc::channel(8);

    assert_eq!(orchestrator.run(output_tx, shutdown_rx).await, OrchestratorExit::Shutdown);
    assert!(log.lock().expect("log lock").contains(&"sleep:1".to_owned()));
}
