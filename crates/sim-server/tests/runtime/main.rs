use hl_wire::{AssetId, PriceTicks, QtyLots, SimUserId};
use oracle_hyperliquid::{Clock, ObservationSource, OracleFreshness, OracleObservation};
use sim_core::{Command, EventRecord, PlaceOrder, Side, TimeInForce};
use sim_server::runtime::actors::{ActorSubmission, SeededLocalActors};
use sim_server::runtime::{
    OracleAssetHealth, RuntimeError, RuntimeEvent, RuntimeFreshness, RuntimeLimits, RuntimePort,
    RuntimeReply, RuntimeRequest, RuntimeTaskError, start_runtime, start_runtime_with_clock,
};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

#[derive(Debug)]
struct FakeClock(AtomicU64);

impl FakeClock {
    fn new(now_ms: u64) -> Self {
        Self(AtomicU64::new(now_ms))
    }

    fn set(&self, now_ms: u64) {
        self.0.store(now_ms, Ordering::Relaxed);
    }
}

impl Clock for FakeClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

fn test_runtime(
    seed: u64,
    now_ms: u64,
) -> (sim_server::runtime::RuntimeHandle, sim_server::runtime::RuntimeTask, Arc<FakeClock>) {
    let clock = Arc::new(FakeClock::new(now_ms));
    let (port, task) = start_runtime_with_clock(seed, limits(), clock.clone());
    (port, task, clock)
}

fn limits() -> RuntimeLimits {
    RuntimeLimits::new(
        NonZeroUsize::new(8).expect("nonzero"),
        NonZeroUsize::new(16).expect("nonzero"),
    )
}

fn user() -> SimUserId {
    SimUserId::parse(&format!("0x{}", "a".repeat(40))).expect("valid user")
}

fn command() -> Command {
    Command::PlaceBatch {
        user: user(),
        timestamp: 123,
        orders: vec![PlaceOrder {
            asset: AssetId::BTC,
            side: Side::Bid,
            price: PriceTicks::new(100).expect("positive price"),
            quantity: QtyLots::new(2).expect("positive quantity"),
            time_in_force: TimeInForce::Gtc,
            client_order_id: Some("deterministic".into()),
        }],
    }
}

fn observation(asset: AssetId, price: i64, time: u64, sequence: u64) -> OracleObservation {
    OracleObservation {
        asset,
        price: PriceTicks::new(price).expect("positive oracle price"),
        observed_at_ms: time,
        upstream_sequence: sequence,
        source: ObservationSource::ActiveAssetCtx,
    }
}

async fn request(
    port: &impl RuntimePort,
    request: RuntimeRequest,
) -> Result<RuntimeReply, RuntimeError> {
    port.try_request(request)?.receive().await
}

async fn observe(port: &impl RuntimePort, item: OracleObservation) {
    assert_eq!(
        request(port, RuntimeRequest::ObserveOracle(item)).await,
        Ok(RuntimeReply::OracleObserved(Ok(())))
    );
}

#[tokio::test]
async fn owner_correlates_all_replies_and_publishes_sequenced_events() {
    let (port, task, _) = test_runtime(77, 100);
    let mut events = port.subscribe_events();
    observe(&port, observation(AssetId::BTC, 100, 100, 1)).await;

    let applied = request(&port, RuntimeRequest::Apply(command())).await.expect("apply reply");
    let RuntimeReply::Applied(applied) = applied else {
        panic!("apply received wrong reply variant");
    };
    assert_eq!(applied.events.len(), 2);

    for expected in &applied.events {
        let (record, freshness) = loop {
            match events.recv().await.expect("event") {
                RuntimeEvent::Event { record, freshness } => break (record, freshness),
                RuntimeEvent::Freshness(RuntimeFreshness::Initializing) => {}
                other => panic!("unexpected runtime event: {other:?}"),
            }
        };
        assert_eq!(&record, expected);
        assert_eq!(freshness, RuntimeFreshness::Current { sequence: record.sequence });
    }

    let RuntimeReply::EngineSnapshot(snapshot) =
        request(&port, RuntimeRequest::EngineSnapshot).await.expect("snapshot")
    else {
        panic!("snapshot received wrong reply variant");
    };
    assert_eq!(snapshot.seed, 77);
    assert_eq!(snapshot.sequence, 2);

    let RuntimeReply::Book(book) =
        request(&port, RuntimeRequest::Book(AssetId::BTC)).await.expect("book")
    else {
        panic!("book received wrong reply variant");
    };
    assert_eq!(book.bids[0].price.value(), 100);

    let RuntimeReply::Account(account) =
        request(&port, RuntimeRequest::Account(user())).await.expect("account")
    else {
        panic!("account received wrong reply variant");
    };
    assert_eq!(account.open_orders, vec![1]);

    let RuntimeReply::Order(Some(order)) =
        request(&port, RuntimeRequest::Order(1)).await.expect("order")
    else {
        panic!("order received wrong reply variant");
    };
    assert_eq!(order.client_order_id.as_deref(), Some("deterministic"));

    let RuntimeReply::Events(records) =
        request(&port, RuntimeRequest::EventsAfter(0)).await.expect("events after")
    else {
        panic!("events-after received wrong reply variant");
    };
    assert_eq!(records, applied.events);

    assert_eq!(request(&port, RuntimeRequest::Shutdown).await, Ok(RuntimeReply::Shutdown));
    assert_eq!(
        events.recv().await.expect("shutdown freshness"),
        RuntimeEvent::Freshness(RuntimeFreshness::ShuttingDown { last_sequence: Some(2) })
    );
    task.wait(Duration::from_secs(1)).await.expect("bounded graceful shutdown");
    assert_eq!(
        port.try_request(RuntimeRequest::EngineSnapshot).unwrap_err(),
        RuntimeError::ShuttingDown
    );
}

async fn deterministic_run() -> Vec<EventRecord> {
    let (port, task, _) = test_runtime(9, 100);
    observe(&port, observation(AssetId::BTC, 100, 100, 1)).await;
    let _ = request(&port, RuntimeRequest::Apply(command())).await.expect("apply");
    let RuntimeReply::Events(events) =
        request(&port, RuntimeRequest::EventsAfter(0)).await.expect("events")
    else {
        panic!("wrong reply");
    };
    assert_eq!(request(&port, RuntimeRequest::Shutdown).await, Ok(RuntimeReply::Shutdown));
    task.wait(Duration::from_secs(1)).await.expect("shutdown");
    events
}

#[tokio::test]
async fn identical_seed_and_commands_produce_identical_event_streams() {
    assert_eq!(deterministic_run().await, deterministic_run().await);
}

#[tokio::test]
async fn completion_wait_is_bounded_and_aborts_an_unresponsive_owner() {
    let (_port, task) = start_runtime(1, limits());
    assert_eq!(task.wait(Duration::ZERO).await, Err(RuntimeTaskError::TimedOut));
}

#[tokio::test]
async fn runtime_clock_owns_freshness_boundaries_and_command_time_cannot_bypass_the_gate() {
    let (port, task, clock) = test_runtime(1, 61_000);
    observe(&port, observation(AssetId::BTC, 100, 1_000, 1)).await;

    let mut boundary = command();
    let Command::PlaceBatch { timestamp, .. } = &mut boundary else { unreachable!() };
    *timestamp = 0;
    assert!(matches!(
        request(&port, RuntimeRequest::Apply(boundary)).await,
        Ok(RuntimeReply::Applied(_))
    ));

    clock.set(61_001);
    let mut stale = command();
    let Command::PlaceBatch { timestamp, .. } = &mut stale else { unreachable!() };
    *timestamp = 1_000;
    assert_eq!(
        request(&port, RuntimeRequest::Apply(stale)).await,
        Err(RuntimeError::OracleStale(AssetId::BTC))
    );
    assert!(matches!(
        request(&port, RuntimeRequest::Book(AssetId::BTC)).await,
        Ok(RuntimeReply::Book(_))
    ));
    let cancel = Command::CancelBatch { user: user(), timestamp: 61_001, cancels: Vec::new() };
    assert!(matches!(
        request(&port, RuntimeRequest::Apply(cancel)).await,
        Ok(RuntimeReply::Applied(_))
    ));

    assert_eq!(request(&port, RuntimeRequest::Shutdown).await, Ok(RuntimeReply::Shutdown));
    task.wait(Duration::from_secs(1)).await.expect("shutdown");
}

#[tokio::test]
async fn future_observation_and_clock_regression_fail_closed_and_health_uses_same_clock() {
    let (port, task, clock) = test_runtime(1, 10_000);
    observe(&port, observation(AssetId::BTC, 100, 10_001, 7)).await;
    observe(&port, observation(AssetId::ETH, 200, 9_000, 8)).await;

    assert_eq!(
        request(&port, RuntimeRequest::Apply(command())).await,
        Err(RuntimeError::OracleStale(AssetId::BTC))
    );
    let RuntimeReply::OracleHealth(health) =
        request(&port, RuntimeRequest::OracleHealth).await.expect("health reply")
    else {
        panic!("health received wrong reply variant")
    };
    assert_eq!(
        health.assets,
        [
            OracleAssetHealth::Observed {
                asset: AssetId::BTC,
                source: ObservationSource::ActiveAssetCtx,
                upstream_sequence: 7,
                observed_at_ms: 10_001,
                freshness: OracleFreshness::FutureObservation,
            },
            OracleAssetHealth::Observed {
                asset: AssetId::ETH,
                source: ObservationSource::ActiveAssetCtx,
                upstream_sequence: 8,
                observed_at_ms: 9_000,
                freshness: OracleFreshness::Fresh { age_ms: 1_000 },
            },
            OracleAssetHealth::Missing { asset: AssetId::SOL },
        ]
    );

    clock.set(8_999);
    let RuntimeReply::OracleHealth(regressed) =
        request(&port, RuntimeRequest::OracleHealth).await.expect("regressed health")
    else {
        panic!("health received wrong reply variant")
    };
    assert!(matches!(
        regressed.assets[1],
        OracleAssetHealth::Observed { freshness: OracleFreshness::FutureObservation, .. }
    ));

    assert_eq!(request(&port, RuntimeRequest::Shutdown).await, Ok(RuntimeReply::Shutdown));
    task.wait(Duration::from_secs(1)).await.expect("shutdown");
}

async fn actor_run(seed: u64) -> Vec<ActorSubmission> {
    let (port, task, _) = test_runtime(44, 10_500);
    let observations = [
        observation(AssetId::BTC, 60_000, 10_000, 1),
        observation(AssetId::ETH, 3_000, 10_000, 1),
        observation(AssetId::SOL, 140, 10_000, 1),
    ];
    let mut actors = SeededLocalActors::new(seed);
    let transcript =
        actors.run_step(&port, 10_500, &observations).await.expect("actor step succeeds");
    assert_eq!(request(&port, RuntimeRequest::Shutdown).await, Ok(RuntimeReply::Shutdown));
    task.wait(Duration::from_secs(1)).await.expect("shutdown");
    transcript
}

#[tokio::test]
async fn seeded_actors_replay_commands_results_and_events_and_different_seeds_diverge() {
    let first = actor_run(7).await;
    let replay = actor_run(7).await;
    let different = actor_run(8).await;
    assert_eq!(first, replay);
    assert_ne!(first, different);
    assert_eq!(first.len(), AssetId::ALL.len() * 2);
    assert!(first.iter().all(|submission| !submission.result.events.is_empty()));
}

#[tokio::test]
async fn actors_surface_runtime_shutdown_without_a_direct_engine_fallback() {
    let (port, task) = start_runtime(2, limits());
    assert_eq!(request(&port, RuntimeRequest::Shutdown).await, Ok(RuntimeReply::Shutdown));
    task.wait(Duration::from_secs(1)).await.expect("shutdown");
    let mut actors = SeededLocalActors::new(3);
    assert_eq!(
        actors.run_step(&port, 1, &[observation(AssetId::BTC, 100, 1, 1)]).await,
        Err(RuntimeError::ShuttingDown)
    );
}

struct OverloadedPort;

impl RuntimePort for OverloadedPort {
    fn try_request(
        &self,
        _request: RuntimeRequest,
    ) -> Result<sim_server::runtime::PendingRuntimeReply, RuntimeError> {
        Err(RuntimeError::Overloaded)
    }

    fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<RuntimeEvent> {
        tokio::sync::broadcast::channel(1).1
    }
}

#[tokio::test]
async fn actors_propagate_bounded_queue_overload() {
    let mut actors = SeededLocalActors::new(3);
    assert_eq!(
        actors.run_step(&OverloadedPort, 1, &[observation(AssetId::BTC, 100, 1, 1)]).await,
        Err(RuntimeError::Overloaded)
    );
}
