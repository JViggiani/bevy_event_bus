use bevy::prelude::*;
use std::sync::{Arc, RwLock};

use super::event_bus_backend::EventBusBackend;

/// Resource wrapper for EventBusBackend
#[derive(Resource, Clone)]
pub struct EventBusBackendResource(pub Arc<RwLock<Box<dyn EventBusBackend>>>);

impl EventBusBackendResource {
    pub fn new<B: EventBusBackend>(backend: B) -> Self {
        Self(Arc::new(RwLock::new(Box::new(backend))))
    }
    pub fn from_box(boxed: Box<dyn EventBusBackend>) -> Self {
        Self(Arc::new(RwLock::new(boxed)))
    }
}


// Provide explicit helpers instead of Deref for clarity
impl EventBusBackendResource {
    pub fn read(&self) -> std::sync::RwLockReadGuard<'_, Box<dyn EventBusBackend>> { self.0.read().unwrap() }
    pub fn write(&self) -> std::sync::RwLockWriteGuard<'_, Box<dyn EventBusBackend>> { self.0.write().unwrap() }
}
