//! Backend marker types used for compile-time backend inference.

/// Marker trait for backend types to enable compile-time dispatch
pub trait BackendMarker: Send + Sync + 'static {}

/// Kafka backend marker.
#[cfg(feature = "kafka")]
#[derive(Debug, Clone, Copy)]
pub struct Kafka;
#[cfg(feature = "kafka")]
impl BackendMarker for Kafka {}

/// In-memory backend marker.
#[derive(Debug, Clone, Copy)]
pub struct InMemory;
impl BackendMarker for InMemory {}

/// Redis backend marker.
#[cfg(feature = "redis")]
#[derive(Debug, Clone, Copy)]
pub struct Redis;
#[cfg(feature = "redis")]
impl BackendMarker for Redis {}
