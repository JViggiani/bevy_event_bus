//! Configuration system for backend-agnostic event bus readers and writers
//!
//! This module provides a type-safe configuration-driven approach where backend types
//! are inferred from configuration objects at compile time. This enables clean system
//! signatures without explicit backend type parameters while maintaining full type safety.

use std::fmt::Display;

pub mod kafka;

/// Offset reset behavior for consumers
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OffsetReset {
    /// Read from beginning of topic
    Earliest,
    /// Only new messages (default)
    Latest,
}

impl Display for OffsetReset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OffsetReset::Earliest => write!(f, "earliest"),
            OffsetReset::Latest => write!(f, "latest"),
        }
    }
}

impl Default for OffsetReset {
    fn default() -> Self {
        OffsetReset::Latest
    }
}

/// Compression types for message payloads
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionType::None => write!(f, "none"),
            CompressionType::Gzip => write!(f, "gzip"),
            CompressionType::Snappy => write!(f, "snappy"),
            CompressionType::Lz4 => write!(f, "lz4"),
            CompressionType::Zstd => write!(f, "zstd"),
        }
    }
}

/// Security protocols for connections
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecurityProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl Display for SecurityProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SecurityProtocol::Plaintext => write!(f, "PLAINTEXT"),
            SecurityProtocol::Ssl => write!(f, "SSL"),
            SecurityProtocol::SaslPlaintext => write!(f, "SASL_PLAINTEXT"),
            SecurityProtocol::SaslSsl => write!(f, "SASL_SSL"),
        }
    }
}

impl Default for SecurityProtocol {
    fn default() -> Self {
        SecurityProtocol::Plaintext
    }
}

/// Core trait for event bus configurations that enables type-safe backend inference
pub trait EventBusConfig: Send + Sync + Clone + 'static {
    /// The backend type this configuration is for (e.g., Kafka, InMemory)
    type Backend: BackendMarker;
    
    /// Get topics this config applies to
    fn topics(&self) -> &[String];
}

/// Marker trait for backend types to enable compile-time dispatch
pub trait BackendMarker: Send + Sync + 'static {}

/// Backend marker types for compile-time type inference
#[derive(Debug, Clone, Copy)]
pub struct Kafka;
impl BackendMarker for Kafka {}

#[derive(Debug, Clone, Copy)]
pub struct InMemory;
impl BackendMarker for InMemory {}

/// Configuration for frame-level processing limits
#[derive(Debug, Clone)]
pub struct ProcessingLimits {
    /// Maximum events to process per frame
    pub max_events_per_frame: Option<usize>,
    /// Maximum time to spend draining events per frame
    pub max_drain_millis: Option<u64>,
}

impl Default for ProcessingLimits {
    fn default() -> Self {
        Self {
            max_events_per_frame: Some(100),
            max_drain_millis: Some(10),
        }
    }
}