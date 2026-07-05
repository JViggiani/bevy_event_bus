//! Serializable payload types exchanged via the event bus.

use bevy::prelude::Message;
use serde::{Serialize, de::DeserializeOwned};

/// Marker trait for types that can be sent and received via the event bus.
///
/// Any type that implements `Serialize`, `DeserializeOwned`, `Clone`, `Message`, and `Sync`
/// automatically implements `BusMessage`. Registration is handled by the topology builder
/// when configuring a backend.
pub trait BusMessage: Serialize + DeserializeOwned + Message + Sync + Clone + 'static {}

impl<T> BusMessage for T where T: Serialize + DeserializeOwned + Message + Sync + Clone + 'static {}
