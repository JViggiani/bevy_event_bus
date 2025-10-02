//! Defines the core BusEvent trait and automatic implementation

use serde::{Serialize, de::DeserializeOwned};

/// Marker trait for events that can be sent/received via the event bus.
///
/// Any type that implements `Serialize`, `DeserializeOwned`, `Clone`, `Send` and `Sync`
/// will automatically implement `BusEvent`. Event registration is handled by the
/// topology builder when configuring a backend.
pub trait BusEvent: Serialize + DeserializeOwned + Send + Sync + Clone + 'static {}

// Automatically implement BusEvent for any type that meets the requirements
impl<T> BusEvent for T where T: Serialize + DeserializeOwned + Send + Sync + Clone + 'static {}
