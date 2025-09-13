//! Internal event registration registry used by the `ExternalBusEvent` derive macro.
//! Each derived event type contributes a callback that will call `add_event::<T>()`
//! for every Bevy `App` that adds the `EventBusPlugin`.

use bevy::app::App;
use once_cell::sync::Lazy;
use std::sync::Mutex;

pub type RegistrationFn = fn(&mut App);

pub static EVENT_REGISTRY: Lazy<Mutex<Vec<RegistrationFn>>> = Lazy::new(|| Mutex::new(Vec::new()));

pub fn register_event<T: bevy::prelude::Event>(app: &mut App) {
    app.add_event::<T>();
}
