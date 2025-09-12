//! A global registry for bus events, populated at program startup.
//!
//! The `ExternalBusEvent` derive macro generates a static constructor for each 
//! decorated type. This constructor adds a registration function to the 
//! `EVENT_REGISTRY`. When the `EventBusPlugin` is initialized, it drains this 
//! registry, calling each function to register the event types.

use bevy::app::App;
use once_cell::sync::Lazy;
use std::sync::Mutex;
use crate::plugin::EventBusAppExt;

pub type RegistrationFn = fn(&mut App);

pub static EVENT_REGISTRY: Lazy<Mutex<Vec<RegistrationFn>>> = Lazy::new(|| Mutex::new(Vec::new()));

// Backwards-compatible helper used by the macro-generated code
pub fn register_event<T: crate::BusEvent + bevy::prelude::Event>(app: &mut App) {
    app.register_bus_event::<T>();
}
