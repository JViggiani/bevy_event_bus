use bevy::prelude::*;
use std::collections::HashSet;
use std::any::TypeId;
 
use tracing::info;

use crate::{
    BusEvent, 
    backends::{EventBusBackend, EventBusBackendResource},
    registration::EVENT_REGISTRY
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
        // Add the core plugin
        app.add_plugins(EventBusPlugin);
        
        // Create and add the backend as a resource
    let boxed = self.0.clone_box();
    // boxed is Box<dyn EventBusBackend>; EventBusBackendResource::new expects concrete type implementing trait.
    // Provide a helper constructor that accepts Box<dyn EventBusBackend>.
    app.insert_resource(EventBusBackendResource::from_box(boxed));
    // Connect
    let backend_res = app.world().get_resource::<EventBusBackendResource>().unwrap().clone();
    let _ = { backend_res.write().connect() };
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