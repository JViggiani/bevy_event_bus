use std::sync::Arc;

use crate::resources::MessageMetadata;

/// Enumerates the error kinds surfaced by writers/readers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BusErrorKind {
    InvalidWriteConfig,
    InvalidReadConfig,
    Serialization,
    DeliveryFailure,
    NotConfigured,
    AckFailure,
    Other,
}

/// Context passed to error callbacks.
#[derive(Debug, Clone)]
pub struct BusErrorContext {
    pub backend: &'static str,
    pub topic: String,
    pub kind: BusErrorKind,
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
        kind: BusErrorKind,
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
