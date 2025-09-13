use bevy::prelude::*;
use std::collections::HashSet;
use std::any::TypeId;
 
use tracing::info;

use crate::{
    BusEvent,
    backends::{EventBusBackend, EventBusBackendResource},
    registration::EVENT_REGISTRY,
    runtime::{ensure_runtime, block_on},
};

/// Tracks registered event types
#[derive(Resource, Default)]
struct RegisteredBusEvents { types: HashSet<TypeId> }

/// Plugin for integrating with external event brokers
pub struct EventBusPlugin;

impl Plugin for EventBusPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<RegisteredBusEvents>();
        
        // Process any events registered via the derive macro
        let registration_fns = {
            let mut registry = EVENT_REGISTRY.lock().unwrap();
            std::mem::take(&mut *registry)
        };
        
        for register_fn in registration_fns {
            register_fn(app);
        }
    }
}

/// Plugin bundle that configures everything needed for the event bus
pub struct EventBusPlugins<B: EventBusBackend>(pub B);

impl<B: EventBusBackend> Plugin for EventBusPlugins<B> {
    fn build(&self, app: &mut App) {
    // Add the core plugin and ensure runtime exists
    app.add_plugins(EventBusPlugin);
    ensure_runtime(app);
        
        // Create and add the backend as a resource
        let boxed = self.0.clone_box();
        app.insert_resource(EventBusBackendResource::from_box(boxed));
        // Connect asynchronously (blocking for now until background tasks introduced)
        if let Some(backend_res) = app.world().get_resource::<EventBusBackendResource>().cloned() {
            let mut guard = backend_res.write();
            let _ = block_on(guard.connect());
        }
    }
}

/// Register event types and backends with Bevy
pub trait EventBusAppExt { fn register_bus_event<T: BusEvent + Event>(&mut self) -> &mut Self; }

impl EventBusAppExt for App {
    fn register_bus_event<T: BusEvent + Event>(&mut self) -> &mut Self {
        // Make sure Bevy's event system knows about this event type
        self.add_event::<T>();
        
        let type_id = TypeId::of::<T>();
        let mut registered = self.world_mut().resource_mut::<RegisteredBusEvents>();
        
        if registered.types.insert(type_id) {
            info!("Registered bus event type: {}", std::any::type_name::<T>());
        }
        
        self
    }
    
}