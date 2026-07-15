//! Typed, bounded boundary around the future single-owner engine runtime.
//!
//! This module deliberately defines channels and messages only. Task #5 owns
//! the loop that receives [`RuntimeEnvelope`] values and mutates the engine.

use hl_wire::{AssetId, SimUserId};
use sim_core::{
    AccountSnapshot, ApplyResult, BookSnapshot, Command, Engine, EngineSnapshot, EventRecord,
    EventSequence, OrderId, OrderSnapshot,
};
use std::{error::Error, fmt, num::NonZeroUsize, time::Duration};
use tokio::sync::{broadcast, mpsc, oneshot};

/// Capacity limits for the runtime command queue and event fan-out.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeLimits {
    pub request_capacity: NonZeroUsize,
    pub event_capacity: NonZeroUsize,
}

impl RuntimeLimits {
    #[must_use]
    pub const fn new(request_capacity: NonZeroUsize, event_capacity: NonZeroUsize) -> Self {
        Self { request_capacity, event_capacity }
    }
}

/// Commands and read-only queries accepted by the engine owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeRequest {
    Apply(Command),
    EngineSnapshot,
    Book(AssetId),
    Account(SimUserId),
    Order(OrderId),
    EventsAfter(EventSequence),
    Shutdown,
}

/// Typed responses corresponding to [`RuntimeRequest`] variants.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeReply {
    Applied(ApplyResult),
    EngineSnapshot(EngineSnapshot),
    Book(BookSnapshot),
    Account(AccountSnapshot),
    Order(Option<OrderSnapshot>),
    Events(Vec<EventRecord>),
    Shutdown,
}

/// Failures at the bounded runtime boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeError {
    /// The bounded command queue has no immediately available capacity.
    Overloaded,
    /// The runtime receiver has closed and cannot accept more work.
    ShuttingDown,
    /// The requester stopped waiting before the owner delivered its reply.
    ReplyDropped,
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Overloaded => "runtime request queue is full",
            Self::ShuttingDown => "runtime is shutting down",
            Self::ReplyDropped => "runtime reply receiver was dropped",
        })
    }
}

impl Error for RuntimeError {}

/// A request plus its one-use typed response channel.
#[derive(Debug)]
pub struct RuntimeEnvelope {
    pub request: RuntimeRequest,
    pub reply: oneshot::Sender<Result<RuntimeReply, RuntimeError>>,
}

impl RuntimeEnvelope {
    /// Completes this request, reporting when the requester has gone away.
    pub fn respond(self, reply: Result<RuntimeReply, RuntimeError>) -> Result<(), RuntimeError> {
        self.reply.send(reply).map_err(|_| RuntimeError::ReplyDropped)
    }
}

/// Awaitable response returned after a successful nonblocking enqueue.
#[derive(Debug)]
pub struct PendingRuntimeReply(oneshot::Receiver<Result<RuntimeReply, RuntimeError>>);

impl PendingRuntimeReply {
    pub async fn receive(self) -> Result<RuntimeReply, RuntimeError> {
        self.0.await.map_err(|_| RuntimeError::ReplyDropped)?
    }
}

/// Adapter-visible state describing whether streamed data is current.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFreshness {
    Initializing,
    Current { sequence: EventSequence },
    Lagged { last_sequence: Option<EventSequence> },
    ShuttingDown { last_sequence: Option<EventSequence> },
}

/// Typed messages sent from the owner to adapter fan-out subscribers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeEvent {
    Event { record: EventRecord, freshness: RuntimeFreshness },
    Freshness(RuntimeFreshness),
}

/// Object-safe boundary used by HTTP, WebSocket, and oracle adapters.
pub trait RuntimePort: Send + Sync {
    /// Attempts to enqueue without waiting for capacity.
    fn try_request(&self, request: RuntimeRequest) -> Result<PendingRuntimeReply, RuntimeError>;

    /// Creates an independent bounded broadcast subscription.
    fn subscribe_events(&self) -> broadcast::Receiver<RuntimeEvent>;
}

/// Cloneable concrete runtime port backed by bounded Tokio channels.
#[derive(Clone, Debug)]
pub struct RuntimeHandle {
    requests: mpsc::Sender<RuntimeEnvelope>,
    events: broadcast::Sender<RuntimeEvent>,
}

impl RuntimePort for RuntimeHandle {
    fn try_request(&self, request: RuntimeRequest) -> Result<PendingRuntimeReply, RuntimeError> {
        let (reply, pending) = oneshot::channel();
        let envelope = RuntimeEnvelope { request, reply };
        self.requests.try_send(envelope).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => RuntimeError::Overloaded,
            mpsc::error::TrySendError::Closed(_) => RuntimeError::ShuttingDown,
        })?;
        Ok(PendingRuntimeReply(pending))
    }

    fn subscribe_events(&self) -> broadcast::Receiver<RuntimeEvent> {
        self.events.subscribe()
    }
}

/// Owner-side event publisher. Sending with no active adapter is a no-op.
#[derive(Clone, Debug)]
pub struct RuntimeEventPublisher {
    events: broadcast::Sender<RuntimeEvent>,
}

impl RuntimeEventPublisher {
    pub fn publish(&self, event: RuntimeEvent) {
        let _ = self.events.send(event);
    }
}

/// Builds the bounded seam without starting or implementing an owner loop.
#[must_use]
pub fn bounded_runtime(
    limits: RuntimeLimits,
) -> (RuntimeHandle, mpsc::Receiver<RuntimeEnvelope>, RuntimeEventPublisher) {
    let (requests, receiver) = mpsc::channel(limits.request_capacity.get());
    let (events, _) = broadcast::channel(limits.event_capacity.get());
    (RuntimeHandle { requests, events: events.clone() }, receiver, RuntimeEventPublisher { events })
}

/// Completion handle for the exclusively owned engine task.
#[derive(Debug)]
pub struct RuntimeTask {
    task: tokio::task::JoinHandle<()>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeTaskError {
    TimedOut,
    OwnerFailed,
}

impl fmt::Display for RuntimeTaskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TimedOut => "runtime did not stop within the graceful shutdown bound",
            Self::OwnerFailed => "runtime owner task failed",
        })
    }
}

impl Error for RuntimeTaskError {}

impl RuntimeTask {
    /// Waits for graceful completion, aborting the owner if the bound expires.
    pub async fn wait(mut self, bound: Duration) -> Result<(), RuntimeTaskError> {
        match tokio::time::timeout(bound, &mut self.task).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(RuntimeTaskError::OwnerFailed),
            Err(_) => {
                self.task.abort();
                let _ = self.task.await;
                Err(RuntimeTaskError::TimedOut)
            }
        }
    }
}

/// Starts the only task that owns and mutates the deterministic engine.
#[must_use]
pub fn start_runtime(seed: u64, limits: RuntimeLimits) -> (RuntimeHandle, RuntimeTask) {
    let (handle, receiver, publisher) = bounded_runtime(limits);
    let task = tokio::spawn(run_owner(Engine::new(seed), receiver, publisher));
    (handle, RuntimeTask { task })
}

async fn run_owner(
    mut engine: Engine,
    mut requests: mpsc::Receiver<RuntimeEnvelope>,
    publisher: RuntimeEventPublisher,
) {
    let mut last_sequence = None;
    publisher.publish(RuntimeEvent::Freshness(RuntimeFreshness::Initializing));

    while let Some(envelope) = requests.recv().await {
        let shutdown = matches!(envelope.request, RuntimeRequest::Shutdown);
        let reply = match &envelope.request {
            RuntimeRequest::Apply(command) => {
                let result = engine.apply(command.clone());
                for record in &result.events {
                    last_sequence = Some(record.sequence);
                    publisher.publish(RuntimeEvent::Event {
                        record: record.clone(),
                        freshness: RuntimeFreshness::Current { sequence: record.sequence },
                    });
                }
                RuntimeReply::Applied(result)
            }
            RuntimeRequest::EngineSnapshot => RuntimeReply::EngineSnapshot(engine.snapshot()),
            RuntimeRequest::Book(asset) => RuntimeReply::Book(engine.book_snapshot(*asset)),
            RuntimeRequest::Account(user) => RuntimeReply::Account(engine.account_snapshot(user)),
            RuntimeRequest::Order(order_id) => RuntimeReply::Order(engine.order(*order_id)),
            RuntimeRequest::EventsAfter(sequence) => {
                RuntimeReply::Events(engine.events_after(*sequence))
            }
            RuntimeRequest::Shutdown => RuntimeReply::Shutdown,
        };
        let _ = envelope.respond(Ok(reply));

        if shutdown {
            requests.close();
            publisher
                .publish(RuntimeEvent::Freshness(RuntimeFreshness::ShuttingDown { last_sequence }));
            while let Some(queued) = requests.recv().await {
                let _ = queued.respond(Err(RuntimeError::ShuttingDown));
            }
            return;
        }
    }

    publisher.publish(RuntimeEvent::Freshness(RuntimeFreshness::ShuttingDown { last_sequence }));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> RuntimeLimits {
        RuntimeLimits::new(NonZeroUsize::MIN, NonZeroUsize::MIN)
    }

    #[tokio::test]
    async fn bounded_port_reports_overload_and_delivers_typed_reply() {
        let (port, mut owner, _) = bounded_runtime(limits());
        let pending = port.try_request(RuntimeRequest::EngineSnapshot).expect("first request fits");
        assert_eq!(
            port.try_request(RuntimeRequest::Shutdown).unwrap_err(),
            RuntimeError::Overloaded
        );

        let envelope = owner.recv().await.expect("owner receives request");
        assert_eq!(envelope.request, RuntimeRequest::EngineSnapshot);
        envelope.respond(Ok(RuntimeReply::Shutdown)).expect("requester is waiting");
        assert_eq!(pending.receive().await, Ok(RuntimeReply::Shutdown));
    }

    #[test]
    fn closed_owner_is_reported_as_shutting_down() {
        let (port, owner, _) = bounded_runtime(limits());
        drop(owner);
        assert_eq!(
            port.try_request(RuntimeRequest::Shutdown).unwrap_err(),
            RuntimeError::ShuttingDown
        );
    }

    #[tokio::test]
    async fn dropped_requester_is_reported_to_owner() {
        let (port, mut owner, _) = bounded_runtime(limits());
        let pending = port.try_request(RuntimeRequest::Shutdown).expect("request fits");
        let envelope = owner.recv().await.expect("owner receives request");
        drop(pending);
        assert_eq!(envelope.respond(Ok(RuntimeReply::Shutdown)), Err(RuntimeError::ReplyDropped));
    }

    #[tokio::test]
    async fn subscribers_receive_typed_freshness_updates() {
        let (port, _owner, publisher) = bounded_runtime(limits());
        let mut subscriber = port.subscribe_events();
        let update = RuntimeEvent::Freshness(RuntimeFreshness::Current { sequence: 7 });
        publisher.publish(update.clone());
        assert_eq!(subscriber.recv().await.expect("published update"), update);
    }

    #[test]
    fn runtime_port_is_object_safe() {
        fn accepts_port(_: &dyn RuntimePort) {}
        let (port, _owner, _publisher) = bounded_runtime(limits());
        accepts_port(&port);
    }
}
