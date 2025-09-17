use bevy::prelude::*;
use std::time::Instant;

use crate::{
    BusEvent,
    resources::{DrainedTopicMetadata, EventMetadata, ExternalEvent},
};

/// Iterator over events with metadata received from the event bus
pub struct EventBusMetadataIterator<'a, T: BusEvent> {
    events: &'a [ExternalEvent<T>],
    current: usize,
}

impl<'a, T: BusEvent> Iterator for EventBusMetadataIterator<'a, T> {
    type Item = &'a ExternalEvent<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.events.len() {
            None
        } else {
            let event = &self.events[self.current];
            self.current += 1;
            Some(event)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.events.len() - self.current;
        (remaining, Some(remaining))
    }
}

/// Iterator over events received from the event bus
pub struct EventBusIterator<'a, T: BusEvent> {
    events: &'a [T],
    current: usize,
}

impl<'a, T: BusEvent> Iterator for EventBusIterator<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.events.len() {
            None
        } else {
            let event = &self.events[self.current];
            self.current += 1;
            Some(event)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.events.len() - self.current;
        (remaining, Some(remaining))
    }
}

/// Reads events from both internal Bevy events and external message broker topics
/// 
/// Provides two APIs:
/// - `try_read()` returns `Iterator<Item = &T>` for simple event access
/// - `try_read_with_metadata()` returns `Iterator<Item = &ExternalEvent<T>>` with full metadata
#[derive(bevy::ecs::system::SystemParam)]
pub struct EventBusReader<'w, 's, T: BusEvent + Event> {
    event_buffer: Local<'s, Vec<T>>,
    metadata_buffer: Local<'s, Vec<ExternalEvent<T>>>,
    events: EventReader<'w, 's, T>,
    metadata_drained: Option<ResMut<'w, DrainedTopicMetadata>>,
    metadata_offsets: Local<'s, std::collections::HashMap<String, usize>>, // per-topic number of metadata messages already decoded for this reader
}

impl<'w, 's, T: BusEvent + Event> EventBusReader<'w, 's, T> {
    /// Read events from a specific topic and from internal Bevy events
    /// 
    /// Returns an iterator over `&T` for simple event access without metadata overhead.
    /// If an error occurs, returns an empty iterator and logs the error.
    pub fn read(&mut self, topic: &str) -> Box<dyn Iterator<Item = &T> + '_> {
        // Clear buffer first to avoid accumulating events across frames
        self.event_buffer.clear();
        
        // Read from drained metadata buffers (single source of truth)
        if let Some(metadata_drained) = &mut self.metadata_drained {
            if let Some(messages) = metadata_drained.topics.get(topic) {
                let start = *self.metadata_offsets.get(topic).unwrap_or(&0);
                if start < messages.len() {
                    for processed_msg in messages[start..].iter() {
                        if let Ok(ev) = serde_json::from_slice::<T>(&processed_msg.payload) {
                            self.event_buffer.push(ev);
                        } else {
                            tracing::warn!("Failed to deserialize event from topic {}", topic);
                        }
                    }
                    self.metadata_offsets.insert(topic.to_string(), messages.len());
                }
            }
        }

        // Also read from internal Bevy events
        for event in self.events.read() {
            self.event_buffer.push(event.clone());
        }

        // Return iterator over events
        Box::new(EventBusIterator {
            events: &self.event_buffer,
            current: 0,
        })
    }

    /// Read events with metadata from a specific topic and from internal Bevy events
    /// 
    /// Returns an iterator over `&ExternalEvent<T>` with full metadata access.
    /// `ExternalEvent<T>` implements `Deref<Target = T>` for transparent event field access.
    /// If an error occurs, returns an empty iterator and logs the error.
    pub fn read_with_metadata(&mut self, topic: &str) -> Box<dyn Iterator<Item = &ExternalEvent<T>> + '_> {
        // Clear metadata buffer first to avoid accumulating events across frames
        self.metadata_buffer.clear();
        
        // Read from drained metadata topic buffers (populated by background consumer)
        if let Some(metadata_drained) = &mut self.metadata_drained {
            if let Some(messages) = metadata_drained.topics.get(topic) {
                let start = *self.metadata_offsets.get(topic).unwrap_or(&0);
                if start < messages.len() {
                    for processed_msg in messages[start..].iter() {
                        if let Ok(event) = serde_json::from_slice::<T>(&processed_msg.payload) {
                            // Use pre-computed metadata - no conversion needed!
                            self.metadata_buffer.push(ExternalEvent::new(event, processed_msg.metadata.clone()));
                        } else {
                            tracing::warn!("Failed to deserialize event from topic {}", topic);
                        }
                    }
                    self.metadata_offsets.insert(topic.to_string(), messages.len());
                }
            }
        }

        // Also read from internal Bevy events with synthetic metadata
        for event in self.events.read() {
            let metadata = EventMetadata {
                topic: "bevy_internal".to_string(),
                partition: 0,
                offset: -1,
                timestamp: Instant::now(),
                headers: std::collections::HashMap::new(),
                key: None,
            };
            self.metadata_buffer.push(ExternalEvent::new(event.clone(), metadata));
        }

        // Return iterator over events with metadata
        Box::new(EventBusMetadataIterator {
            events: &self.metadata_buffer,
            current: 0,
        })
    }

    /// Clear the event buffer and mark internal events as read
    pub fn clear(&mut self) {
        self.event_buffer.clear();
        self.metadata_buffer.clear();
        for _ in self.events.read() {}
    }

    /// Get the number of events in the buffer
    pub fn len(&self) -> usize {
        self.event_buffer.len() + self.events.len() + self.metadata_buffer.len()
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.event_buffer.is_empty() && self.events.is_empty() && self.metadata_buffer.is_empty()
    }
}

#[cfg(test)]
mod tests {
    // (Intentionally left empty; integration tests cover behavior.)
}
