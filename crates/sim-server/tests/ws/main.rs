use axum::Router;
use futures_util::{SinkExt, StreamExt};
use hl_wire::api::Subscription;
use hl_wire::response::EventChannel;
use hl_wire::{AssetId, PriceTicks, QtyLots, SimUserId};
use serde_json::{Value, json};
use sim_core::{Event, EventRecord, Fill, OrderSnapshot, OrderState, Side, TimeInForce};
use sim_server::ws::{SnapshotView, ViewError, ViewMessage, WsLimits, router};
use sim_server::{
    PendingRuntimeReply, RuntimeError, RuntimeEvent, RuntimeFreshness, RuntimePort, RuntimeRequest,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{ClientRequestBuilder, Message},
};

const USER_A: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const USER_B: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

#[derive(Clone)]
struct FakeRuntime {
    events: broadcast::Sender<RuntimeEvent>,
}

impl FakeRuntime {
    fn new(capacity: usize) -> Self {
        let (events, _) = broadcast::channel(capacity);
        Self { events }
    }

    fn publish(&self, record: EventRecord) {
        let sequence = record.sequence;
        let _ = self.events.send(RuntimeEvent::Event {
            record,
            freshness: RuntimeFreshness::Current { sequence },
        });
    }
}

impl RuntimePort for FakeRuntime {
    fn try_request(&self, _: RuntimeRequest) -> Result<PendingRuntimeReply, RuntimeError> {
        Err(RuntimeError::ShuttingDown)
    }

    fn subscribe_events(&self) -> broadcast::Receiver<RuntimeEvent> {
        self.events.subscribe()
    }
}

struct FakeView;

impl SnapshotView for FakeView {
    fn initial(&self, subscription: &Subscription) -> Result<Option<ViewMessage>, ViewError> {
        let message = match subscription {
            Subscription::AllMids {} => Some(ViewMessage::new(
                EventChannel::AllMids,
                10,
                json!({"BTC":"100","ETH":"50","SOL":"10"}),
            )),
            Subscription::L2Book { coin } => Some(ViewMessage::new(
                EventChannel::L2Book,
                10,
                json!({"coin":coin,"time":1000,"levels":[[],[]]}),
            )),
            Subscription::OrderUpdates { user: _ } => {
                Some(ViewMessage::new(EventChannel::OrderUpdates, 10, json!([])))
            }
            Subscription::Trades { .. } => None,
        };
        Ok(message)
    }

    fn update(
        &self,
        subscription: &Subscription,
        record: &EventRecord,
    ) -> Result<Option<ViewMessage>, ViewError> {
        let (event_user, asset) = match &record.event {
            Event::OrderUpdated { user, asset, .. } | Event::OrderCancelled { user, asset, .. } => {
                (Some(user), *asset)
            }
            Event::OrderAccepted { order } => (Some(&order.user), order.asset),
            Event::Fill { fill } => (None, fill.asset),
        };
        let message = match subscription {
            Subscription::AllMids {} => Some(ViewMessage::new(
                EventChannel::AllMids,
                record.sequence,
                json!({"BTC":"101","ETH":"50","SOL":"10"}),
            )),
            Subscription::L2Book { coin } if *coin == asset => Some(ViewMessage::new(
                EventChannel::L2Book,
                record.sequence,
                json!({"coin":coin,"time":record.timestamp,"levels":[[],[]]}),
            )),
            Subscription::Trades { coin }
                if *coin == asset && matches!(record.event, Event::Fill { .. }) =>
            {
                Some(ViewMessage::new(EventChannel::Trades, record.sequence, json!([])))
            }
            // Deliberately project every order event. The adapter, rather than a
            // view implementation, owns the user-isolation boundary.
            Subscription::OrderUpdates { .. }
                if event_user.is_some() || matches!(record.event, Event::Fill { .. }) =>
            {
                Some(ViewMessage::new(
                    EventChannel::OrderUpdates,
                    record.sequence,
                    json!([{"oid":1,"status":"open"}]),
                ))
            }
            _ => None,
        };
        Ok(message)
    }
}

async fn spawn(runtime: FakeRuntime, limits: WsLimits) -> String {
    let app: Router = router(Arc::new(runtime), Arc::new(FakeView), limits);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("ws://{address}/ws")
}

async fn receive_json<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let message = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("websocket response timed out")
        .expect("socket closed")
        .expect("valid websocket frame");
    serde_json::from_str(message.to_text().expect("text frame")).expect("JSON response")
}

#[tokio::test]
async fn public_control_ack_pong_and_initial_snapshots_are_explicit() {
    let url = spawn(FakeRuntime::new(16), WsLimits::default()).await;
    let (mut socket, _) = connect_async(&url).await.unwrap();

    socket.send(Message::text(r#"{"method":"ping"}"#)).await.unwrap();
    assert_eq!(receive_json(&mut socket).await, json!({"channel":"pong"}));

    socket
        .send(Message::text(r#"{"method":"subscribe","subscription":{"type":"allMids"}}"#))
        .await
        .unwrap();
    assert_eq!(
        receive_json(&mut socket).await,
        json!({"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"allMids"}}})
    );
    let snapshot = receive_json(&mut socket).await;
    assert_eq!(snapshot["channel"], "allMids");
    assert_eq!(snapshot["sequence"], 10);

    socket
        .send(Message::text(
            r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}"#,
        ))
        .await
        .unwrap();
    assert_eq!(receive_json(&mut socket).await["data"]["method"], "subscribe");
    assert_eq!(receive_json(&mut socket).await["channel"], "l2Book");

    socket
        .send(Message::text(
            r#"{"method":"unsubscribe","subscription":{"type":"l2Book","coin":"BTC"}}"#,
        ))
        .await
        .unwrap();
    assert_eq!(receive_json(&mut socket).await["data"]["method"], "unsubscribe");
}

#[tokio::test]
async fn unsupported_post_and_subscription_variants_fail_explicitly() {
    let url = spawn(FakeRuntime::new(16), WsLimits::default()).await;
    let (mut socket, _) = connect_async(&url).await.unwrap();

    socket.send(Message::text(r#"{"method":"post","id":1}"#)).await.unwrap();
    assert_eq!(receive_json(&mut socket).await["data"]["category"], "unsupported");
    socket
        .send(Message::text(
            r#"{"method":"subscribe","subscription":{"type":"candles","coin":"BTC"}}"#,
        ))
        .await
        .unwrap();
    assert_eq!(receive_json(&mut socket).await["data"]["category"], "unsupported");

    for request in [
        r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":"DOGE"}}"#,
        r#"{"method":"subscribe","subscription":{"type":"trades","coin":"DOGE"}}"#,
    ] {
        socket.send(Message::text(request)).await.unwrap();
        assert_eq!(receive_json(&mut socket).await["data"]["category"], "unsupported");
    }

    for request in [
        r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":7}}"#,
        r#"{"method":"subscribe","subscription":{"type":"trades"}}"#,
        r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":"DOGE","extra":true}}"#,
    ] {
        socket.send(Message::text(request)).await.unwrap();
        assert_eq!(receive_json(&mut socket).await["data"]["category"], "invalid_request");
    }
}

#[tokio::test]
async fn minimum_outbound_capacity_carries_subscribe_ack_and_snapshot() {
    let url = spawn(FakeRuntime::new(16), WsLimits::new(8, 1)).await;
    let (mut socket, _) = connect_async(&url).await.unwrap();
    socket
        .send(Message::text(r#"{"method":"subscribe","subscription":{"type":"allMids"}}"#))
        .await
        .unwrap();

    assert_eq!(receive_json(&mut socket).await["data"]["method"], "subscribe");
    assert_eq!(receive_json(&mut socket).await["channel"], "allMids");
}

#[tokio::test]
async fn order_updates_require_matching_upgrade_identity_and_do_not_leak() {
    let runtime = FakeRuntime::new(16);
    let url = spawn(runtime.clone(), WsLimits::default()).await;
    let request_a =
        ClientRequestBuilder::new(url.parse().unwrap()).with_header("X-Sim-User", USER_A);
    let request_b =
        ClientRequestBuilder::new(url.parse().unwrap()).with_header("X-Sim-User", USER_B);
    let (mut a, _) = connect_async(request_a).await.unwrap();
    let (mut b, _) = connect_async(request_b).await.unwrap();

    a.send(Message::text(format!(
        r#"{{"method":"subscribe","subscription":{{"type":"orderUpdates","user":"{USER_B}"}}}}"#
    )))
    .await
    .unwrap();
    assert_eq!(receive_json(&mut a).await["data"]["category"], "unauthorized_sim_user");

    for (socket, user) in [(&mut a, USER_A), (&mut b, USER_B)] {
        socket.send(Message::text(format!(r#"{{"method":"subscribe","subscription":{{"type":"orderUpdates","user":"{user}"}}}}"#))).await.unwrap();
        assert_eq!(receive_json(socket).await["data"]["method"], "subscribe");
        assert_eq!(receive_json(socket).await["channel"], "orderUpdates");
    }

    runtime.publish(EventRecord {
        sequence: 11,
        timestamp: 1001,
        event: Event::OrderUpdated {
            order_id: 1,
            user: SimUserId::parse(USER_A).unwrap(),
            asset: AssetId::BTC,
            remaining_lots: 1,
            state: OrderState::Open,
        },
    });
    assert_eq!(receive_json(&mut a).await["sequence"], 11);
    assert!(tokio::time::timeout(Duration::from_millis(150), b.next()).await.is_err());
}

#[tokio::test]
async fn order_update_routing_covers_all_owned_event_roles() {
    let runtime = FakeRuntime::new(32);
    let url = spawn(runtime.clone(), WsLimits::default()).await;
    let request_a =
        ClientRequestBuilder::new(url.parse().unwrap()).with_header("X-Sim-User", USER_A);
    let request_b =
        ClientRequestBuilder::new(url.parse().unwrap()).with_header("X-Sim-User", USER_B);
    let (mut a, _) = connect_async(request_a).await.unwrap();
    let (mut b, _) = connect_async(request_b).await.unwrap();
    for (socket, user) in [(&mut a, USER_A), (&mut b, USER_B)] {
        socket
            .send(Message::text(format!(
                r#"{{"method":"subscribe","subscription":{{"type":"orderUpdates","user":"{user}"}}}}"#
            )))
            .await
            .unwrap();
        let _ = receive_json(socket).await;
        let _ = receive_json(socket).await;
    }

    let user_a = SimUserId::parse(USER_A).unwrap();
    let user_b = SimUserId::parse(USER_B).unwrap();
    let owned_events = [
        Event::OrderAccepted {
            order: OrderSnapshot {
                id: 1,
                user: user_a.clone(),
                asset: AssetId::BTC,
                side: Side::Bid,
                price: PriceTicks::new(100).unwrap(),
                quantity: QtyLots::new(2).unwrap(),
                remaining_lots: 2,
                time_in_force: TimeInForce::Gtc,
                client_order_id: None,
                accepted_sequence: 11,
                state: OrderState::Open,
            },
        },
        Event::OrderUpdated {
            order_id: 1,
            user: user_a.clone(),
            asset: AssetId::BTC,
            remaining_lots: 1,
            state: OrderState::Open,
        },
        Event::OrderCancelled {
            order_id: 1,
            user: user_a.clone(),
            asset: AssetId::BTC,
            cancelled_lots: 1,
        },
    ];
    for (index, event) in owned_events.into_iter().enumerate() {
        let sequence = 11 + u64::try_from(index).unwrap();
        runtime.publish(EventRecord { sequence, timestamp: 1000 + sequence, event });
        assert_eq!(receive_json(&mut a).await["sequence"], sequence);
        assert!(tokio::time::timeout(Duration::from_millis(100), b.next()).await.is_err());
    }

    runtime.publish(EventRecord {
        sequence: 14,
        timestamp: 1014,
        event: Event::Fill {
            fill: Fill {
                trade_id: 1,
                asset: AssetId::BTC,
                price: PriceTicks::new(100).unwrap(),
                quantity_lots: 1,
                maker_order_id: 1,
                taker_order_id: 2,
                maker: user_a,
                taker: user_b,
                taker_side: Side::Ask,
            },
        },
    });
    assert_eq!(receive_json(&mut a).await["sequence"], 14, "maker must receive fill");
    assert_eq!(receive_json(&mut b).await["sequence"], 14, "taker must receive fill");
}

#[tokio::test]
async fn runtime_broadcast_lag_is_observable_and_disconnects_slow_connection() {
    let runtime = FakeRuntime::new(2);
    let url = spawn(runtime.clone(), WsLimits::new(8, 2)).await;
    let (mut socket, _) = connect_async(&url).await.unwrap();
    socket
        .send(Message::text(r#"{"method":"subscribe","subscription":{"type":"allMids"}}"#))
        .await
        .unwrap();
    let _ = receive_json(&mut socket).await;
    let _ = receive_json(&mut socket).await;

    for sequence in 11..100 {
        runtime.publish(EventRecord {
            sequence,
            timestamp: sequence,
            event: Event::OrderUpdated {
                order_id: sequence,
                user: SimUserId::parse(USER_A).unwrap(),
                asset: AssetId::BTC,
                remaining_lots: 1,
                state: OrderState::Open,
            },
        });
    }

    let mut observed_lag = false;
    for _ in 0..8 {
        match tokio::time::timeout(Duration::from_secs(2), socket.next()).await {
            Ok(Some(Ok(message))) if message.is_text() => {
                let value: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
                if value.get("channel") == Some(&Value::String("error".into())) {
                    observed_lag = value["data"]["category"] == "lagged";
                    break;
                }
            }
            Ok(Some(Ok(message))) if message.is_close() => break,
            Ok(None) => break,
            _ => {}
        }
    }
    assert!(observed_lag, "lag must be reported before disconnect");
}
