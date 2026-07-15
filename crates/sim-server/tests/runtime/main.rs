use hl_wire::{AssetId, PriceTicks, QtyLots, SimUserId};
use sim_core::{Command, EventRecord, PlaceOrder, Side, TimeInForce};
use sim_server::runtime::{
    RuntimeError, RuntimeEvent, RuntimeFreshness, RuntimeLimits, RuntimePort, RuntimeReply,
    RuntimeRequest, RuntimeTaskError, start_runtime,
};
use std::{num::NonZeroUsize, time::Duration};

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

async fn request(
    port: &impl RuntimePort,
    request: RuntimeRequest,
) -> Result<RuntimeReply, RuntimeError> {
    port.try_request(request)?.receive().await
}

#[tokio::test]
async fn owner_correlates_all_replies_and_publishes_sequenced_events() {
    let (port, task) = start_runtime(77, limits());
    let mut events = port.subscribe_events();

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
    let (port, task) = start_runtime(9, limits());
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
