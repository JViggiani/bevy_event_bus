//! Configuration system for backend-agnostic event bus readers and writers
//!
//! This module provides a type-safe configuration-driven approach where backend types
//! are inferred from configuration objects at compile time. This enables clean system
//! signatures without explicit backend type parameters while maintaining full type safety.

pub mod core;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod markers;
#[cfg(feature = "redis")]
pub mod redis;

pub use core::{EventBusConfig, ProcessingLimits, TopologyMode};
#[cfg(feature = "kafka")]
pub use markers::Kafka;
#[cfg(feature = "redis")]
pub use markers::Redis;
pub use markers::{BackendMarker, InMemory};
