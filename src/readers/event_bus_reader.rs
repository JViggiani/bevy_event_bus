use bevy::prelude::*;

use crate::{
    BusEvent,
    config::EventBusConfig,
    resources::{DrainedTopicMetadata, EventWrapper},
};

#[cfg(feature = "kafka")]
use crate::config::kafka::{KafkaConsumerConfig, KafkaEventMetadata, UncommittedEvent};

/// Iterator over events with metadata produced by the external bus pipeline.
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

/// Reads events that originated from external message broker topics
#[derive(bevy::ecs::system::SystemParam)]
pub struct EventBusReader<'w, 's, T: BusEvent + Event> {
    wrapped_events: Local<'s, Vec<EventWrapper<T>>>,
    metadata_drained: Option<ResMut<'w, DrainedTopicMetadata>>,
    metadata_offsets: Local<'s, std::collections::HashMap<String, usize>>, // per-topic number of metadata messages already decoded for this reader
}

impl<'w, 's, T: BusEvent + Event> EventBusReader<'w, 's, T> {
    /// Read all events available for the supplied configuration
    ///
    /// This is the only API for reading events. You must provide configuration
    /// that specifies which topics to read from. External events from message
    /// brokers include metadata – use `.metadata()` to access it.
    ///
    /// Returns a vector of `EventWrapper<T>` which derefs to `T`.
    pub fn read<C: EventBusConfig>(&mut self, config: &C) -> Vec<EventWrapper<T>> {
        let mut all_events = Vec::new();
        let topics = config.topics();

        for topic in topics {
            // Clear wrapped events buffer first
            self.wrapped_events.clear();

            // Read external events with metadata from drained buffers
            if let Some(metadata_drained) = &mut self.metadata_drained {
                if let Some(messages) = metadata_drained.topics.get(topic) {
                    let start = *self.metadata_offsets.get(topic).unwrap_or(&0);
                    if start < messages.len() {
                        for processed_msg in messages.iter().skip(start) {
                            match serde_json::from_slice::<T>(&processed_msg.payload) {
                                Ok(event) => self
                                    .wrapped_events
                                    .push(EventWrapper::new(event, processed_msg.metadata.clone())),
                                Err(_) => tracing::warn!(
                                    "Failed to deserialize event from topic {}",
                                    topic
                                ),
                            }
                        }
                        self.metadata_offsets.insert(topic.clone(), messages.len());
                    }
                }
            }

            // Add events from this topic to the result without cloning
            all_events.append(&mut *self.wrapped_events);
        }

        all_events
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
            .map(|wrapper| {
                let metadata = wrapper.metadata();
                let kafka_metadata = if let Some(kafka_meta) = metadata.kafka_metadata() {
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
                        key: metadata.key.clone(),
                        headers: metadata.headers.clone(),
                    }
                } else {
                    KafkaEventMetadata {
                        topic: metadata.source.clone(),
                        partition: 0,
                        offset: 0,
                        timestamp: Some(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_millis() as i64)
                                .unwrap_or(0),
                        ),
                        key: metadata.key.clone(),
                        headers: metadata.headers.clone(),
                    }
                };

                UncommittedEvent::new(wrapper, kafka_metadata, || {
                    tracing::warn!("Manual commit not yet implemented - this is a placeholder");
                    Ok(())
                })
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
