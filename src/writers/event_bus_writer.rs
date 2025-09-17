use bevy::prelude::*;
use std::collections::HashMap;
use crate::{
    BusEvent, EventBusError,
    backends::{EventBusBackendResource, EventBusBackendExt},
};

/// Writes events to both internal Bevy events and external message broker topics
#[derive(bevy::ecs::system::SystemParam)]
pub struct EventBusWriter<'w, T: BusEvent + Event> {
    backend: Option<Res<'w, EventBusBackendResource>>,
    events: EventWriter<'w, T>,
}

impl<'w, T: BusEvent + Event> EventBusWriter<'w, T> {
    /// Send an event to a specific topic and to internal Bevy events
    /// 
    /// Returns `Ok(())` on success, `Err(EventBusError)` on failure.
    /// For fire-and-forget behavior, use `.unwrap_or(())` or ignore the result.
    pub fn send(&mut self, topic: &str, event: T) -> Result<(), EventBusError> {
        // Send to external backend immediately if available
        if let Some(backend_res) = &self.backend {
            let backend = backend_res.read();
            backend.try_send(&event, topic)?;
        }

        // Also send to internal Bevy events immediately
        self.events.write(event);
        Ok(())
    }

    /// Send multiple events to a specific topic and to internal Bevy events
    /// 
    /// All events are sent as a batch operation. If any event fails, the entire operation fails.
    pub fn send_batch(
        &mut self,
        topic: &str,
        events: impl IntoIterator<Item = T>,
    ) -> Result<(), EventBusError> {
        let events: Vec<_> = events.into_iter().collect();

        // Send each event to external backend immediately if available
        if let Some(backend_res) = &self.backend {
            let backend = backend_res.read();
            for event in &events {
                backend.try_send(event, topic)?;
            }
        }

        // Also send to internal Bevy events immediately
        for event in events {
            self.events.write(event);
        }

        Ok(())
    }

    /// Send an event with headers to a specific topic and to internal Bevy events
    /// 
    /// Headers are only sent to external brokers; internal Bevy events don't support headers.
    pub fn send_with_headers(
        &mut self, 
        topic: &str, 
        event: T, 
        headers: HashMap<String, String>
    ) -> Result<(), EventBusError> {
        // Send to external backend with headers if available
        if let Some(backend_res) = &self.backend {
            let backend = backend_res.read();
            backend.try_send_with_headers(&event, topic, &headers)?;
        }

        // Also send to internal Bevy events immediately (headers are lost in internal events)
        self.events.write(event);
        Ok(())
    }

    /// Send the default value of the event to a specific topic
    pub fn send_default(&mut self, topic: &str) -> Result<(), EventBusError>
    where
        T: Default,
    {
        self.send(topic, T::default())
    }
}
