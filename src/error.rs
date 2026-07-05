//! Unified error surface for the event bus.
//!
//! This is the single source of truth for error categorisation. Both the
//! ECS-facing [`EventBusError`] message and the callback-facing
//! [`BusErrorContext`] classify failures using the same [`EventBusErrorType`]
//! enum so readers and writers speak one error vocabulary.

use std::sync::Arc;

use bevy::prelude::*;
use bevy_event_bus::BusMessage;
use bevy_event_bus::resources::MessageMetadata;
use serde::{Deserialize, Serialize};

/// Categories of errors that can occur in the event bus.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventBusErrorType {
    Serialization,      // JSON serialization failed
    Connection,         // Backend connection issues
    NotConfigured,      // Backend not configured
    Topic,              // Invalid topic name/format
    Timeout,            // Network timeout
    DeliveryFailure,    // Async delivery failure from the backend
    Deserialization,    // Failed to deserialize message from topic
    ConsumerError,      // Background consumer error
    AckFailure,         // Acknowledgement of a consumed message failed
    InvalidReadConfig,  // Reader configuration does not align with the provisioned topology
    InvalidWriteConfig, // Writer configuration does not align with the provisioned topology
    Other,              // Catch-all
}

/// Event fired when any operation on the event bus fails.
///
/// All errors, whether immediate (serialization) or async (delivery), are sent as events.
#[derive(Message, Debug, Clone)]
pub struct EventBusError<T: BusMessage> {
    /// The topic the event was being sent to
    pub topic: String,
    /// The specific type of error that occurred
    pub error_type: EventBusErrorType,
    /// Human-readable error message
    pub error_message: String,
    /// Timestamp when the error occurred
    pub timestamp: std::time::SystemTime,
    /// The original event data (available for immediate errors, None for async delivery errors)
    pub original_event: Option<T>,
    /// The backend that failed (e.g., "kafka")
    pub backend: Option<String>,
    /// Optional metadata for the event (contains backend-specific details like partition/offset)
    pub metadata: Option<MessageMetadata>,
}

impl<T: BusMessage> EventBusError<T> {
    /// Create a new immediate error (with original event available)
    pub fn immediate(
        topic: String,
        error_type: EventBusErrorType,
        error_message: String,
        original_event: T,
    ) -> Self {
        Self {
            topic,
            error_type,
            error_message,
            timestamp: std::time::SystemTime::now(),
            original_event: Some(original_event),
            backend: None,
            metadata: None,
        }
    }

    /// Create a new async error (no original event available)
    pub fn async_delivery(
        topic: String,
        error_message: String,
        backend: Option<String>,
        metadata: Option<MessageMetadata>,
    ) -> Self {
        Self {
            topic,
            error_type: EventBusErrorType::DeliveryFailure,
            error_message,
            timestamp: std::time::SystemTime::now(),
            original_event: None,
            backend,
            metadata,
        }
    }
}

/// Context passed to error callbacks registered on writers/readers.
#[derive(Debug, Clone)]
pub struct BusErrorContext {
    pub backend: &'static str,
    pub topic: String,
    pub kind: EventBusErrorType,
    pub message: String,
    pub metadata: Option<MessageMetadata>,
    pub original_bytes: Option<Vec<u8>>, // when available
}

/// Callback invoked when an error occurs.
pub type BusErrorCallback = Arc<dyn Fn(BusErrorContext) + Send + Sync + 'static>;

impl BusErrorContext {
    pub fn new(
        backend: &'static str,
        topic: impl Into<String>,
        kind: EventBusErrorType,
        message: impl Into<String>,
        metadata: Option<MessageMetadata>,
        original_bytes: Option<Vec<u8>>,
    ) -> Self {
        Self {
            backend,
            topic: topic.into(),
            kind,
            message: message.into(),
            metadata,
            original_bytes,
        }
    }
}
