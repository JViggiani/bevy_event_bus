use bevy::prelude::*;

use crate::{
    BusEvent,
    config::EventBusConfig,
    resources::{DrainedTopicMetadata, EventWrapper},
};

#[cfg(feature = "kafka")]
use crate::config::kafka::{KafkaConsumerConfig, KafkaEventMetadata, UncommittedEvent};

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
    /// Read all events (internal and external) with mandatory configuration
    ///
    /// This is the only API for reading events. You must provide configuration
    /// that specifies which topics to read from. External events from message
    /// brokers include metadata, while internal Bevy events do not.
    /// Use `.metadata()` to check if metadata is available.
    ///
    /// Returns a vector of `EventWrapper<T>` which derefs to `T`.
    pub fn read<C: EventBusConfig>(&mut self, config: &C) -> Vec<EventWrapper<T>> {
        let mut all_events = Vec::new();

        for topic in config.topics() {
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
                        self.metadata_offsets
                            .insert(topic.to_string(), messages.len());
                    }
                }
            }

            // Add internal Bevy events (without metadata) - these don't have topic context
            // so we add them to every topic query (they are shared across all topics)
            if topic == config.topics().first().unwrap_or(&String::new()) {
                for event in self.events.read() {
                    self.wrapped_events
                        .push(EventWrapper::new_internal(event.clone()));
                }
            }

            // Add events from this topic to the result
            for event_wrapper in &self.wrapped_events {
                all_events.push(event_wrapper.clone());
            }
        }

        all_events
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
        self.event_buffer.is_empty() && self.events.is_empty() && self.wrapped_events.is_empty()
    }
}

#[cfg(feature = "kafka")]
impl<'w, 's, T: BusEvent + Event> EventBusReader<'w, 's, T> {
    /// Read events without auto-committing (manual commit mode) - Kafka specific
    ///
    /// This method is only available when using a Kafka configuration with manual commit enabled.
    /// Returns events that can be manually committed after processing.
    pub fn read_uncommitted(
        &mut self,
        config: &KafkaConsumerConfig,
    ) -> Vec<UncommittedEvent<EventWrapper<T>>> {
        let events = self.read(config);

        events
            .into_iter()
            .filter_map(|wrapper| {
                // Only external events can be manually committed
                if wrapper.is_external() {
                    let metadata = wrapper.metadata().map(|m| {
                        // Extract Kafka-specific metadata if available
                        if let Some(kafka_meta) = m.kafka_metadata() {
                            KafkaEventMetadata {
                                topic: kafka_meta.topic.clone(),
                                partition: kafka_meta.partition,
                                offset: kafka_meta.offset,
                                timestamp: Some(
                                    std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .map(|d| d.as_millis() as i64)
                                        .unwrap_or(0),
                                ),
                                key: m.key.clone(),
                                headers: m.headers.clone(),
                            }
                        } else {
                            // Fallback metadata construction
                            KafkaEventMetadata {
                                topic: m.source.clone(),
                                partition: 0,
                                offset: 0,
                                timestamp: Some(
                                    std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .map(|d| d.as_millis() as i64)
                                        .unwrap_or(0),
                                ),
                                key: m.key.clone(),
                                headers: m.headers.clone(),
                            }
                        }
                    });

                    metadata.map(|kafka_metadata| {
                        UncommittedEvent::new(wrapper, kafka_metadata, || {
                            // TODO: Implement actual commit logic through backend
                            tracing::warn!(
                                "Manual commit not yet implemented - this is a placeholder"
                            );
                            Ok(())
                        })
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get consumer lag information for a Kafka configuration
    ///
    /// Returns the lag for the configured consumer group across all topics.
    /// This is a placeholder implementation that will be enhanced when backend integration is complete.
    pub fn get_consumer_lag(&self, _config: &KafkaConsumerConfig) -> Result<i64, String> {
        // TODO: Implement actual lag calculation through backend
        tracing::warn!("Consumer lag calculation not yet implemented - returning 0");
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    // (Intentionally left empty; integration tests cover behavior.)
}
