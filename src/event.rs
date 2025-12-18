//! Defines the core BusEvent trait and automatic implementation

use bevy::prelude::Message;
use serde::{Serialize, de::DeserializeOwned};

/// Marker trait for events that can be sent/received via the event bus.
///
/// Any type that implements `Serialize`, `DeserializeOwned`, `Clone`, `Message` and `Sync`
/// will automatically implement `BusEvent`. Event registration is handled by the
/// topology builder when configuring a backend.
pub trait BusEvent: Serialize + DeserializeOwned + Message + Sync + Clone + 'static {}

// Automatically implement BusEvent for any type that meets the requirements
impl<T> BusEvent for T where T: Serialize + DeserializeOwned + Message + Sync + Clone + 'static {}
