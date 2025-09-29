//! Defines the core BusEvent trait and automatic implementation

use serde::{Serialize, de::DeserializeOwned};

/// Marker trait for events that can be sent/received via the event bus
///
/// Normally you don't need to implement this manually - use the
/// `#[derive(ExternalBusEvent)]` macro instead.
pub trait BusEvent: Serialize + DeserializeOwned + Send + Sync + Clone + 'static {}

// Automatically implement BusEvent for any type that meets the requirements
impl<T> BusEvent for T where T: Serialize + DeserializeOwned + Send + Sync + Clone + 'static {}
