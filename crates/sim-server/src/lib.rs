//! Interface-only seams for downstream adapters and the engine owner runtime.

pub mod http;
pub mod runtime;
pub mod ws;

pub use runtime::{
    PendingRuntimeReply, RuntimeEnvelope, RuntimeError, RuntimeEvent, RuntimeEventPublisher,
    RuntimeFreshness, RuntimeHandle, RuntimeLimits, RuntimePort, RuntimeReply, RuntimeRequest,
    bounded_runtime,
};
