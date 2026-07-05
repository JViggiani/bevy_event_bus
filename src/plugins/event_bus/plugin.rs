use bevy::prelude::*;

use crate::backends::EventBusBackend;
use crate::plugins::event_bus::setup::{
    connect_and_install_backend, ensure_app_runtime, register_backend, register_plugin_world,
};

/// Wires a broker backend into Bevy (runtime, drain systems, lifecycle).
pub struct EventBusPlugin<B: EventBusBackend> {
    backend: B,
}

impl<B: EventBusBackend> EventBusPlugin<B> {
    pub fn new(backend: B) -> Self {
        Self { backend }
    }
}

impl<B: EventBusBackend> From<B> for EventBusPlugin<B> {
    fn from(backend: B) -> Self {
        Self::new(backend)
    }
}

impl<B: EventBusBackend> Plugin for EventBusPlugin<B> {
    fn build(&self, app: &mut App) {
        ensure_app_runtime(app);
        register_plugin_world(app);
        register_backend(app, &self.backend);
        connect_and_install_backend(app);
    }
}
