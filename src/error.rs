use bevy::prelude::*;
use bevy_event_bus::BusEvent;
use bevy_event_bus::resources::MessageMetadata;
use serde::{Deserialize, Serialize};

/// Types of errors that can occur in the event bus
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EventBusErrorType {
    Serialization,      // JSON serialization failed
    Connection,         // Backend connection issues
    NotConfigured,      // Backend not configured
    Topic,              // Invalid topic name/format
    Timeout,            // Network timeout
    DeliveryFailure,    // Async delivery failure from Kafka
    Deserialization,    // Failed to deserialize message from topic
    ConsumerError,      // Background consumer error
    InvalidReadConfig,  // Reader configuration does not align with the provisioned topology
    InvalidWriteConfig, // Writer configuration does not align with the provisioned topology
    Other,              // Catch-all
}

/// Event fired when any operation on the event bus fails
///
/// This replaces both the old Result-based error handling and EventBusDeliveryFailure.
/// All errors, whether immediate (serialization) or async (delivery), are sent as events.
#[derive(Message, Debug, Clone)]
pub struct EventBusError<T: BusEvent> {
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

impl<T: BusEvent> EventBusError<T> {
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

/// Event fired when message deserialization fails
///
/// This is a non-generic error event for cases where we cannot create EventBusError<T>
/// because the deserialization failed and we don't have a T instance.
#[derive(Message, Debug, Clone)]
pub struct EventBusDecodeError {
    /// The topic the message was received from
    pub topic: String,
    /// Human-readable error message
    pub error_message: String,
    /// Timestamp when the error occurred
    pub timestamp: std::time::SystemTime,
    /// The raw message bytes that failed to decode
    pub raw_payload: Vec<u8>,
    /// The decoder name that failed
    pub decoder_name: String,
    /// Optional metadata for the message (contains backend-specific details like partition/offset)
    pub metadata: Option<MessageMetadata>,
}

impl EventBusDecodeError {
    /// Create a new decode error
    pub fn new(
        topic: String,
        error_message: String,
        raw_payload: Vec<u8>,
        decoder_name: String,
        metadata: Option<MessageMetadata>,
    ) -> Self {
        Self {
            topic,
            error_message,
            timestamp: std::time::SystemTime::now(),
            raw_payload,
            decoder_name,
            metadata,
        }
    }
}
