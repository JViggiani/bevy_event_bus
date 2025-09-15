use bevy::prelude::*;

use crate::{
    BusEvent, EventBusError,
    resources::DrainedTopicBuffers,
};

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
    events: EventReader<'w, 's, T>,
    drained: Option<ResMut<'w, DrainedTopicBuffers>>,
    decoded_offsets: Local<'s, std::collections::HashMap<String, usize>>, // per-topic number of raw messages already decoded for this reader
}

impl<'w, 's, T: BusEvent + Event> EventBusReader<'w, 's, T> {
    /// Read events from a specific topic and from internal Bevy events
    pub fn read(
        &mut self,
        topic: &str,
    ) -> Result<Box<dyn Iterator<Item = &T> + '_>, EventBusError> {
        // Clear buffer first to avoid accumulating events across frames
        self.event_buffer.clear();
        
        // Read from drained topic buffers (populated by background consumer)
        if let Some(drained) = &mut self.drained {
            if let Some(raws) = drained.topics.get(topic) {
                let start = *self.decoded_offsets.get(topic).unwrap_or(&0);
                if start < raws.len() {
                    for raw in raws[start..].iter() {
                        if let Ok(ev) = serde_json::from_slice::<T>(raw) {
                            self.event_buffer.push(ev);
                        }
                    }
                    self.decoded_offsets.insert(topic.to_string(), raws.len());
                }
            }
        }

        // Also read from internal Bevy events
        for event in self.events.read() {
            self.event_buffer.push(event.clone());
        }

        // Return iterator over events
        Ok(Box::new(EventBusIterator {
            events: &self.event_buffer,
            current: 0,
        }))
    }

    /// Try to read events from a specific topic - returns empty iterator on error
    pub fn try_read(&mut self, topic: &str) -> Box<dyn Iterator<Item = &T> + '_> {
        match self.read(topic) {
            Ok(iter) => iter,
            Err(e) => {
                tracing::error!("Error reading events from topic {}: {:?}", topic, e);
                Box::new(EventBusIterator {
                    events: &[],
                    current: 0,
                })
            }
        }
    }

    /// Clear the event buffer and mark internal events as read
    pub fn clear(&mut self) {
        self.event_buffer.clear();
        for _ in self.events.read() {}
    }

    /// Get the number of events in the buffer
    pub fn len(&self) -> usize {
        self.event_buffer.len() + self.events.len()
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.event_buffer.is_empty() && self.events.is_empty()
    }
}

#[cfg(test)]
mod tests {
    // (Intentionally left empty; integration tests cover behavior.)
}
