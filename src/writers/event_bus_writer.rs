use bevy::prelude::*;
use std::collections::HashMap;
use std::sync::Mutex;
use crate::{
    BusEvent, EventBusError,
    backends::{EventBusBackendResource, EventBusBackendExt},
};

/// Resource to collect error events that need to be sent
/// This avoids system parameter conflicts between EventBusWriter and EventReader<EventBusError<T>>
#[derive(Resource, Default)]
pub struct EventBusErrorQueue {
    pending_errors: Mutex<Vec<Box<dyn Fn(&mut World) + Send + Sync>>>,
}

impl EventBusErrorQueue {
    pub fn add_error<T: BusEvent + Event>(&self, error: EventBusError<T>) {
        if let Ok(mut pending) = self.pending_errors.lock() {
            pending.push(Box::new(move |world: &mut World| {
                world.send_event(error.clone());
            }));
        }
    }
    
    pub fn flush_errors(&self, world: &mut World) {
        if let Ok(mut pending) = self.pending_errors.lock() {
            for error_fn in pending.drain(..) {
                error_fn(world);
            }
        }
    }
    
    pub fn drain_pending(&self) -> Vec<Box<dyn Fn(&mut World) + Send + Sync>> {
        if let Ok(mut pending) = self.pending_errors.lock() {
            std::mem::take(&mut *pending)
        } else {
            Vec::new()
        }
    }
}

/// Writes events to both internal Bevy events and external message broker topics
/// 
/// All methods use "fire and forget" semantics - errors are sent as EventBusError<T> events
/// rather than returned as Results.
#[derive(bevy::ecs::system::SystemParam)]
pub struct EventBusWriter<'w, T: BusEvent + Event> {
    backend: Option<Res<'w, EventBusBackendResource>>,
    events: EventWriter<'w, T>,
    error_queue: Res<'w, EventBusErrorQueue>,
}

impl<'w, T: BusEvent + Event> EventBusWriter<'w, T> {
    /// Write an event to a specific topic and to internal Bevy events
    /// 
    /// Uses fire-and-forget semantics. Any errors are sent as EventBusError<T> events.
    pub fn write(&mut self, topic: &str, event: T) {
        // Clone event for potential error reporting
        let event_clone = event.clone();
        
        // Send to external backend immediately if available
        if let Some(backend_res) = &self.backend {
            let backend = backend_res.read();
            if !backend.try_send(&event, topic) {
                // Backend send failed - queue error event to avoid conflicts
                let error_event = EventBusError::immediate(
                    topic.to_string(),
                    crate::EventBusErrorType::Other, // Since we don't know the specific reason
                    "Failed to send to external backend".to_string(),
                    event_clone,
                );
                self.error_queue.add_error(error_event);
                return; // Don't send to internal events if external send failed
            }
        }

        // Also send to internal Bevy events
        self.events.write(event);
    }

    /// Write multiple events to a specific topic and to internal Bevy events
    /// 
    /// All events are written as a batch operation. Uses fire-and-forget semantics.
    /// If any event fails, an error event is fired but processing continues.
    pub fn write_batch(&mut self, topic: &str, events: impl IntoIterator<Item = T>) {
        let events: Vec<_> = events.into_iter().collect();

        // Send each event to external backend immediately if available
        if let Some(backend_res) = &self.backend {
            let backend = backend_res.read();
            for event in &events {
                if !backend.try_send(event, topic) {
                    // Backend send failed - queue error event
                    let error_event = EventBusError::immediate(
                        topic.to_string(),
                        crate::EventBusErrorType::Other,
                        "Failed to send to external backend".to_string(),
                        event.clone(),
                    );
                    self.error_queue.add_error(error_event);
                    // Continue with other events even if one fails
                }
            }
        }

        // Also send to internal Bevy events
        for event in events {
            self.events.write(event);
        }
    }

    /// Write an event with headers to a specific topic and to internal Bevy events
    /// 
    /// Headers are only sent to external brokers; internal Bevy events don't support headers.
    /// Uses fire-and-forget semantics. Any errors are sent as EventBusError<T> events.
    pub fn write_with_headers(
        &mut self, 
        topic: &str, 
        event: T, 
        headers: HashMap<String, String>
    ) {
        // Clone event for potential error reporting
        let event_clone = event.clone();
        
        // Send to external backend with headers if available
        if let Some(backend_res) = &self.backend {
            let backend = backend_res.read();
            if !backend.try_send_with_headers(&event, topic, &headers) {
                // Backend send failed - queue error event
                let error_event = EventBusError::immediate(
                    topic.to_string(),
                    crate::EventBusErrorType::Other,
                    "Failed to send to external backend with headers".to_string(),
                    event_clone,
                );
                self.error_queue.add_error(error_event);
                return; // Don't send to internal events if external send failed
            }
        }

        // Also send to internal Bevy events (headers are lost in internal events)
        self.events.write(event);
    }

    /// Write the default value of the event to a specific topic
    /// 
    /// Uses fire-and-forget semantics. Any errors are sent as EventBusError<T> events.
    pub fn write_default(&mut self, topic: &str)
    where
        T: Default,
    {
        self.write(topic, T::default())
    }
}
