//! Bounded WebSocket adapter for the `sim-header-v1` compatibility profile.

use axum::Router;
use axum::extract::State;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::http::HeaderMap;
use axum::response::Response;
use axum::routing::get;
use futures_util::{Sink, SinkExt, StreamExt};
use hl_wire::SimUserId;
use hl_wire::api::{Subscription, WsRequest};
use hl_wire::response::{
    ChannelEnvelope, EventChannel, SubscriptionAck, SubscriptionMethod, WsControlResponse,
};
use serde::Serialize;
use serde_json::{Value, json};
use sim_core::{Event, EventRecord};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, watch};

use crate::{RuntimeEvent, RuntimeFreshness, RuntimePort};

const SIM_USER_HEADER: &str = "x-sim-user";
const MIN_OUTBOUND_CAPACITY: usize = 2;
const SOCKET_SEND_TIMEOUT: Duration = Duration::from_secs(1);
const WRITER_JOIN_TIMEOUT: Duration = Duration::from_secs(1);

/// Per-connection resource limits.
///
/// `outbound_capacity` is normalized to at least two so one subscribe control
/// can atomically enqueue its acknowledgement and required initial snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WsLimits {
    pub max_subscriptions: usize,
    pub outbound_capacity: usize,
}

impl WsLimits {
    #[must_use]
    pub const fn new(max_subscriptions: usize, outbound_capacity: usize) -> Self {
        Self {
            max_subscriptions: if max_subscriptions == 0 { 1 } else { max_subscriptions },
            outbound_capacity: if outbound_capacity < MIN_OUTBOUND_CAPACITY {
                MIN_OUTBOUND_CAPACITY
            } else {
                outbound_capacity
            },
        }
    }
}

impl Default for WsLimits {
    fn default() -> Self {
        Self::new(16, 32)
    }
}

/// A wire-ready snapshot or update with an explicit engine sequence boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ViewMessage {
    pub channel: EventChannel,
    pub sequence: u64,
    pub data: Value,
}

impl ViewMessage {
    #[must_use]
    pub const fn new(channel: EventChannel, sequence: u64, data: Value) -> Self {
        Self { channel, sequence, data }
    }
}

/// Failures while projecting runtime state into WebSocket wire payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ViewError {
    message: String,
}

impl ViewError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }
}

impl fmt::Display for ViewError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ViewError {}

/// WS-local projection seam.
///
/// Task #5 may back this with its runtime-owned immutable view. Keeping the
/// projection here avoids teaching the transport about tick/lot scales or
/// mutable engine internals.
pub trait SnapshotView: Send + Sync + 'static {
    /// Returns initial data where the profile requires it. `trades` returns
    /// `None`; `orderUpdates` may return an empty explicit snapshot.
    fn initial(&self, subscription: &Subscription) -> Result<Option<ViewMessage>, ViewError>;

    /// Projects one immutable sequenced runtime event for a subscription.
    fn update(
        &self,
        subscription: &Subscription,
        record: &EventRecord,
    ) -> Result<Option<ViewMessage>, ViewError>;
}

#[derive(Clone)]
struct AdapterState {
    runtime: Arc<dyn RuntimePort>,
    snapshots: Arc<dyn SnapshotView>,
    limits: WsLimits,
}

/// Builds a router exposing exactly `GET /ws`.
pub fn router(
    runtime: Arc<dyn RuntimePort>,
    snapshots: Arc<dyn SnapshotView>,
    limits: WsLimits,
) -> Router {
    Router::new().route("/ws", get(upgrade)).with_state(AdapterState { runtime, snapshots, limits })
}

async fn upgrade(
    State(state): State<AdapterState>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    let identity = headers
        .get(SIM_USER_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| SimUserId::parse(value).ok());
    upgrade.on_upgrade(move |socket| connection(socket, state, identity))
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum SubscriptionKey {
    AllMids,
    L2Book(hl_wire::AssetId),
    Trades(hl_wire::AssetId),
    OrderUpdates(SimUserId),
}

impl From<&Subscription> for SubscriptionKey {
    fn from(subscription: &Subscription) -> Self {
        match subscription {
            Subscription::AllMids {} => Self::AllMids,
            Subscription::L2Book { coin } => Self::L2Book(*coin),
            Subscription::Trades { coin } => Self::Trades(*coin),
            Subscription::OrderUpdates { user } => Self::OrderUpdates(user.clone()),
        }
    }
}

#[derive(Clone)]
struct ActiveSubscription {
    request: Subscription,
    snapshot_boundary: Option<u64>,
}

#[derive(Clone, Debug)]
struct DisconnectNotice {
    category: &'static str,
    message: &'static str,
}

async fn connection(socket: WebSocket, state: AdapterState, identity: Option<SimUserId>) {
    let (sink, mut incoming) = socket.split();
    let (outbound, outbound_rx) = mpsc::channel(state.limits.outbound_capacity);
    let (disconnect, disconnect_rx) = watch::channel(None);
    let mut writer_task = tokio::spawn(writer(sink, outbound_rx, disconnect_rx));
    let mut writer_finished = false;
    let mut runtime_events = state.runtime.subscribe_events();
    let mut subscriptions = HashMap::<SubscriptionKey, ActiveSubscription>::new();

    loop {
        tokio::select! {
            incoming_message = incoming.next() => {
                match incoming_message {
                    Some(Ok(Message::Text(text))) => {
                        if !handle_request(
                            text.as_str(),
                            identity.as_ref(),
                            &state,
                            &mut subscriptions,
                            &outbound,
                        ) {
                            signal_disconnect(&disconnect, "lagged", "outbound queue is full");
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(Message::Ping(payload))) => {
                        if outbound.try_send(Message::Pong(payload)).is_err() {
                            signal_disconnect(&disconnect, "lagged", "outbound queue is full");
                            break;
                        }
                    }
                    Some(Ok(Message::Binary(_))) => {
                        if !send_error(&outbound, "invalid_request", "JSON text message required") {
                            signal_disconnect(&disconnect, "lagged", "outbound queue is full");
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {}
                }
            }
            runtime_event = runtime_events.recv() => {
                match runtime_event {
                    Ok(RuntimeEvent::Event { record, freshness }) => {
                        if freshness_is_lagged(freshness) {
                            signal_disconnect(&disconnect, "lagged", "runtime event stream is not current");
                            break;
                        }
                        if !route_event(&state, &subscriptions, &record, &outbound) {
                            signal_disconnect(&disconnect, "lagged", "outbound queue is full");
                            break;
                        }
                    }
                    Ok(RuntimeEvent::Freshness(freshness)) => {
                        if freshness_is_lagged(freshness) {
                            signal_disconnect(&disconnect, "lagged", "runtime event stream is not current");
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        signal_disconnect(&disconnect, "lagged", "runtime event gap detected");
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        signal_disconnect(&disconnect, "internal", "runtime event stream closed");
                        break;
                    }
                }
            }
            _ = &mut writer_task => {
                writer_finished = true;
                break;
            },
        }
    }

    drop(outbound);
    if !writer_finished {
        finish_writer(writer_task, WRITER_JOIN_TIMEOUT).await;
    }
}

fn freshness_is_lagged(freshness: RuntimeFreshness) -> bool {
    matches!(freshness, RuntimeFreshness::Lagged { .. } | RuntimeFreshness::ShuttingDown { .. })
}

fn handle_request(
    text: &str,
    identity: Option<&SimUserId>,
    state: &AdapterState,
    subscriptions: &mut HashMap<SubscriptionKey, ActiveSubscription>,
    outbound: &mpsc::Sender<Message>,
) -> bool {
    let request = match serde_json::from_str::<WsRequest>(text) {
        Ok(request) => request,
        Err(_) => {
            let category = classify_parse_failure(text);
            return send_error(outbound, category, parse_failure_message(category));
        }
    };

    match request {
        WsRequest::Ping {} => send_json(outbound, &WsControlResponse::Pong {}),
        WsRequest::Subscribe { subscription } => {
            if !authorized(identity, &subscription) {
                return send_error(
                    outbound,
                    "unauthorized_sim_user",
                    "orderUpdates user must match X-Sim-User",
                );
            }
            let key = SubscriptionKey::from(&subscription);
            if subscriptions.contains_key(&key) {
                return send_ack(outbound, SubscriptionMethod::Subscribe, subscription);
            }
            if subscriptions.len() >= state.limits.max_subscriptions {
                return send_error(outbound, "overloaded", "subscription limit reached");
            }
            let initial = match state.snapshots.initial(&subscription) {
                Ok(initial) => initial,
                Err(_) => {
                    return send_error(outbound, "internal", "snapshot unavailable");
                }
            };
            let boundary = initial.as_ref().map(|message| message.sequence);
            subscriptions.insert(
                key,
                ActiveSubscription { request: subscription.clone(), snapshot_boundary: boundary },
            );
            if !send_ack(outbound, SubscriptionMethod::Subscribe, subscription) {
                return false;
            }
            initial.is_none_or(|message| send_view(outbound, message))
        }
        WsRequest::Unsubscribe { subscription } => {
            if !authorized(identity, &subscription) {
                return send_error(
                    outbound,
                    "unauthorized_sim_user",
                    "orderUpdates user must match X-Sim-User",
                );
            }
            subscriptions.remove(&SubscriptionKey::from(&subscription));
            send_ack(outbound, SubscriptionMethod::Unsubscribe, subscription)
        }
    }
}

fn authorized(identity: Option<&SimUserId>, subscription: &Subscription) -> bool {
    match subscription {
        Subscription::OrderUpdates { user } => identity == Some(user),
        _ => true,
    }
}

fn classify_parse_failure(text: &str) -> &'static str {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return "invalid_request";
    };
    let Some(object) = value.as_object() else {
        return "invalid_request";
    };
    match object.get("method").and_then(Value::as_str) {
        Some("post") => "unsupported",
        Some("subscribe" | "unsubscribe") => {
            let Some(subscription) = object.get("subscription").and_then(Value::as_object) else {
                return "invalid_request";
            };
            match subscription.get("type").and_then(Value::as_str) {
                Some("l2Book" | "trades")
                    if object.len() == 2
                        && subscription.len() == 2
                        && subscription.get("coin").and_then(Value::as_str).is_some() =>
                {
                    "unsupported"
                }
                Some("allMids" | "l2Book" | "trades" | "orderUpdates") | None => "invalid_request",
                Some(_) => "unsupported",
            }
        }
        Some(method) if !matches!(method, "ping" | "subscribe" | "unsubscribe") => "unsupported",
        _ => "invalid_request",
    }
}

fn parse_failure_message(category: &str) -> &'static str {
    if category == "unsupported" {
        "WebSocket method or subscription is outside sim-header-v1"
    } else {
        "invalid WebSocket request"
    }
}

fn route_event(
    state: &AdapterState,
    subscriptions: &HashMap<SubscriptionKey, ActiveSubscription>,
    record: &EventRecord,
    outbound: &mpsc::Sender<Message>,
) -> bool {
    for active in subscriptions.values() {
        if active.snapshot_boundary.is_some_and(|boundary| record.sequence <= boundary) {
            continue;
        }
        if !event_is_visible_to_subscription(&active.request, record) {
            continue;
        }
        match state.snapshots.update(&active.request, record) {
            Ok(Some(message)) => {
                if !send_view(outbound, message) {
                    return false;
                }
            }
            Ok(None) => {}
            Err(_) => {
                if !send_error(outbound, "internal", "event projection failed") {
                    return false;
                }
            }
        }
    }
    true
}

fn event_is_visible_to_subscription(subscription: &Subscription, record: &EventRecord) -> bool {
    let Subscription::OrderUpdates { user } = subscription else {
        return true;
    };
    match &record.event {
        Event::OrderAccepted { order } => &order.user == user,
        Event::Fill { fill } => &fill.maker == user || &fill.taker == user,
        Event::OrderUpdated { user: event_user, .. }
        | Event::OrderCancelled { user: event_user, .. } => event_user == user,
    }
}

fn send_ack(
    outbound: &mpsc::Sender<Message>,
    method: SubscriptionMethod,
    subscription: Subscription,
) -> bool {
    send_json(
        outbound,
        &WsControlResponse::SubscriptionResponse { data: SubscriptionAck { method, subscription } },
    )
}

fn send_view(outbound: &mpsc::Sender<Message>, message: ViewMessage) -> bool {
    send_json(
        outbound,
        &ChannelEnvelope {
            channel: message.channel,
            sequence: message.sequence,
            data: message.data,
        },
    )
}

fn send_error(
    outbound: &mpsc::Sender<Message>,
    category: &'static str,
    message: &'static str,
) -> bool {
    send_json(
        outbound,
        &json!({
            "channel": "error",
            "data": {"category": category, "message": message},
        }),
    )
}

fn send_json<T: Serialize>(outbound: &mpsc::Sender<Message>, value: &T) -> bool {
    serde_json::to_string(value)
        .ok()
        .is_some_and(|text| outbound.try_send(Message::Text(text.into())).is_ok())
}

fn signal_disconnect(
    disconnect: &watch::Sender<Option<DisconnectNotice>>,
    category: &'static str,
    message: &'static str,
) {
    disconnect.send_replace(Some(DisconnectNotice { category, message }));
}

async fn writer<S>(
    mut sink: S,
    mut outbound: mpsc::Receiver<Message>,
    mut disconnect: watch::Receiver<Option<DisconnectNotice>>,
) where
    S: Sink<Message> + Unpin,
{
    writer_with_timeout(&mut sink, &mut outbound, &mut disconnect, SOCKET_SEND_TIMEOUT).await;
}

async fn writer_with_timeout<S>(
    sink: &mut S,
    outbound: &mut mpsc::Receiver<Message>,
    disconnect: &mut watch::Receiver<Option<DisconnectNotice>>,
    send_timeout: Duration,
) where
    S: Sink<Message> + Unpin,
{
    loop {
        tokio::select! {
            biased;
            changed = disconnect.changed() => {
                if changed.is_err() {
                    break;
                }
                let notice = disconnect.borrow_and_update().clone();
                if let Some(notice) = notice {
                    if send_to_socket(sink, Message::Text(json!({
                        "channel": "error",
                        "data": {"category": notice.category, "message": notice.message},
                    }).to_string().into()), send_timeout).await.is_err() {
                        break;
                    }
                    let _ = send_to_socket(sink, Message::Close(Some(CloseFrame {
                        code: 1013,
                        reason: "resubscribe required".into(),
                    })), send_timeout).await;
                    break;
                }
            }
            message = outbound.recv() => {
                match message {
                    Some(message) => {
                        if send_to_socket(sink, message, send_timeout).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
    }
}

async fn send_to_socket<S>(sink: &mut S, message: Message, timeout: Duration) -> Result<(), ()>
where
    S: Sink<Message> + Unpin,
{
    tokio::time::timeout(timeout, sink.send(message)).await.map_err(|_| ())?.map_err(|_| ())
}

async fn finish_writer(mut writer: tokio::task::JoinHandle<()>, timeout: Duration) {
    if tokio::time::timeout(timeout, &mut writer).await.is_err() {
        writer.abort();
        let _ = writer.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll};

    struct BlockedSink;

    impl Sink<Message> for BlockedSink {
        type Error = ();

        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn start_send(self: Pin<&mut Self>, _: Message) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }
    }

    #[tokio::test]
    async fn blocked_socket_send_terminates_writer_at_injected_timeout() {
        let (outbound, mut outbound_rx) = mpsc::channel(2);
        let (_disconnect, mut disconnect_rx) = watch::channel(None);
        outbound.try_send(Message::Text("blocked".into())).unwrap();
        let mut sink = BlockedSink;

        tokio::time::timeout(
            Duration::from_millis(100),
            writer_with_timeout(
                &mut sink,
                &mut outbound_rx,
                &mut disconnect_rx,
                Duration::from_millis(10),
            ),
        )
        .await
        .expect("writer must stop after the socket-send deadline");
    }

    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn writer_join_deadline_aborts_stuck_task() {
        let dropped = Arc::new(AtomicBool::new(false));
        let signal = DropSignal(Arc::clone(&dropped));
        let task = tokio::spawn(async move {
            let _signal = signal;
            std::future::pending::<()>().await;
        });

        finish_writer(task, Duration::from_millis(10)).await;
        assert!(dropped.load(Ordering::SeqCst), "timed-out writer must be cancelled");
    }
}
