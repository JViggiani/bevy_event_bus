use bevy::prelude::*;

use crate::{
    BusEvent,
    resources::{DrainedTopicMetadata, EventWrapper},
};

/// Iterator over events with optional metadata (unified internal + external events)
pub struct EventWrapperIterator<'a, T: BusEvent> {
    events: &'a [EventWrapper<T>],
    current: usize,
}

impl<'a, T: BusEvent> Iterator for EventWrapperIterator<'a, T> {
    type Item = &'a EventWrapper<T>;

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
#[derive(bevy::ecs::system::SystemParam)]
pub struct EventBusReader<'w, 's, T: BusEvent + Event> {
    event_buffer: Local<'s, Vec<T>>,
    wrapped_events: Local<'s, Vec<EventWrapper<T>>>,
    events: EventReader<'w, 's, T>,
    metadata_drained: Option<ResMut<'w, DrainedTopicMetadata>>,
    metadata_offsets: Local<'s, std::collections::HashMap<String, usize>>, // per-topic number of metadata messages already decoded for this reader
}

impl<'w, 's, T: BusEvent + Event> EventBusReader<'w, 's, T> {
    /// Read all events (internal and external) with optional metadata
    /// 
    /// This is the recommended API that provides a unified view of all events.
    /// External events from message brokers include metadata, while internal Bevy
    /// events do not. Use `.metadata()` to check if metadata is available.
    /// 
    /// Returns an iterator over `&EventWrapper<T>` which derefs to `&T`.
    pub fn read(&mut self, topic: &str) -> Box<dyn Iterator<Item = &EventWrapper<T>> + '_> {
        // Clear wrapped events buffer first
        self.wrapped_events.clear();
        
        // Read external events with metadata from drained buffers
        if let Some(metadata_drained) = &mut self.metadata_drained {
            if let Some(messages) = metadata_drained.topics.get(topic) {
                let start = *self.metadata_offsets.get(topic).unwrap_or(&0);
                if start < messages.len() {
                    for processed_msg in messages[start..].iter() {
                        if let Ok(event) = serde_json::from_slice::<T>(&processed_msg.payload) {
                            self.wrapped_events.push(EventWrapper::new_external(
                                event,
                                processed_msg.metadata.clone(),
                            ));
                        } else {
                            tracing::warn!("Failed to deserialize event from topic {}", topic);
                        }
                    }
                    self.metadata_offsets.insert(topic.to_string(), messages.len());
                }
            }
        }

        // Add internal Bevy events (without metadata)
        for event in self.events.read() {
            self.wrapped_events.push(EventWrapper::new_internal(event.clone()));
        }

        Box::new(EventWrapperIterator {
            events: &self.wrapped_events,
            current: 0,
        })
    }


    /// Clear all event buffers and mark internal events as read
    pub fn clear(&mut self) {
        self.event_buffer.clear();
        self.wrapped_events.clear();
        for _ in self.events.read() {}
    }

    /// Get the total number of events across all buffers
    pub fn len(&self) -> usize {
        self.event_buffer.len() + self.events.len() + self.wrapped_events.len()
    }

    /// Check if all buffers are empty
    pub fn is_empty(&self) -> bool {
        self.event_buffer.is_empty() 
            && self.events.is_empty() 
            && self.wrapped_events.is_empty()
    }
}

#[cfg(test)]
mod tests {
    // (Intentionally left empty; integration tests cover behavior.)
}
