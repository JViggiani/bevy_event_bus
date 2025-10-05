use bevy::prelude::*;
use std::sync::{Arc, RwLock};

use super::event_bus_backend::EventBusBackend;
use crate::runtime;

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
    pub fn read(&self) -> std::sync::RwLockReadGuard<'_, Box<dyn EventBusBackend>> {
        self.0.read().unwrap()
    }
    pub fn write(&self) -> std::sync::RwLockWriteGuard<'_, Box<dyn EventBusBackend>> {
        self.0.write().unwrap()
    }
}

impl Drop for EventBusBackendResource {
    fn drop(&mut self) {
        if Arc::strong_count(&self.0) != 1 {
            return;
        }

        if let Ok(mut backend) = self.0.write() {
            let _ = runtime::block_on(backend.disconnect());
        }
    }
}
