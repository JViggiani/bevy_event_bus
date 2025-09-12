use bevy::prelude::*;

use crate::{BusEvent, backends::{EventBusBackendResource, EventBusBackendExt}, EventBusError};

/// Writes events to both internal Bevy events and external message broker topics
#[derive(bevy::ecs::system::SystemParam)]
pub struct EventBusWriter<'w, T: BusEvent + Event> {
    backend: Res<'w, EventBusBackendResource>,
    events: EventWriter<'w, T>,
}

impl<'w, T: BusEvent + Event> EventBusWriter<'w, T> {
    /// Send an event to a specific topic and to internal Bevy events
    pub fn send(&mut self, topic: &str, event: T) -> Result<(), EventBusError> {
        // Send to external event bus
    if let Err(e) = self.backend.read().send(&event, topic) {
            tracing::error!("Failed to send event to topic {}: {:?}", topic, e);
            return Err(e);
        }
        
        // Also send to internal Bevy events
        self.events.write(event.clone());
        Ok(())
    }
    
    /// Send multiple events to a specific topic and to internal Bevy events
    pub fn send_batch(&mut self, topic: &str, events: impl IntoIterator<Item = T>) -> Result<(), EventBusError> {
        let events: Vec<_> = events.into_iter().collect();
        
        // Send each event to the external bus
        for event in &events {
            if let Err(e) = self.backend.read().send(event, topic) {
                tracing::error!("Failed to send event to topic {}: {:?}", topic, e);
                return Err(e);
            }
        }
        
        // Also send to internal Bevy events
        for event in events {
            self.events.write(event);
        }
        
        Ok(())
    }
    
    /// Send the default value of the event to a specific topic
    pub fn send_default(&mut self, topic: &str) -> Result<(), EventBusError>
    where 
        T: Default
    {
    self.send(topic, T::default())
    }
    
    /// Try to send an event - silently continues if error occurs
    pub fn try_send(&mut self, topic: &str, event: T) -> bool {
        self.send(topic, event).is_ok()
    }
    
    /// Try to send a batch of events - silently continues if error occurs
    pub fn try_send_batch(&mut self, topic: &str, events: impl IntoIterator<Item = T>) -> bool {
        self.send_batch(topic, events).is_ok()
    }
}
