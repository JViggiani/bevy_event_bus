use std::collections::HashMap;

use bevy::prelude::*;

use bevy_event_bus::backends::KafkaCommitRequest;
use bevy_event_bus::config::{EventBusConfig, kafka::KafkaConsumerConfig};
use bevy_event_bus::resources::{
    DrainedTopicMetadata, EventWrapper, KafkaCommitQueue, KafkaLagCacheResource,
};
use bevy_event_bus::{BusEvent, readers::BusEventReader};
use crossbeam_channel::TrySendError;

/// Errors that can occur when working with the Kafka-specific reader.
#[derive(Debug)]
pub enum KafkaReaderError {
    /// No backend resource was found in the Bevy world.
    BackendUnavailable,
    /// The event did not contain Kafka metadata, so committing is impossible.
    MissingKafkaMetadata,
    /// Manual commit operations were requested while auto-commit is still enabled.
    ManualCommitDisabled,
    /// Manual commit operations could not be queued because the queue is unavailable.
    CommitQueueUnavailable,
    /// Manual commit operations could not be queued because the queue is full.
    CommitQueueFull,
    /// The event is missing consumer group metadata necessary for manual commits.
    MissingConsumerGroup,
    /// Lag information is not yet available for the requested group/topic.
    LagDataUnavailable,
    /// Backend responded with an error message.
    BackendFailure(String),
}

impl std::fmt::Display for KafkaReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KafkaReaderError::BackendUnavailable => {
                write!(f, "Kafka backend resource is not available in the world")
            }
            KafkaReaderError::MissingKafkaMetadata => {
                write!(f, "event is missing Kafka metadata needed for commit")
            }
            KafkaReaderError::ManualCommitDisabled => {
                write!(f, "manual commits requested while auto-commit is enabled")
            }
            KafkaReaderError::CommitQueueUnavailable => {
                write!(f, "commit queue unavailable; backend not ready")
            }
            KafkaReaderError::CommitQueueFull => {
                write!(f, "commit queue is full; retry next frame")
            }
            KafkaReaderError::MissingConsumerGroup => {
                write!(f, "event is missing consumer group metadata")
            }
            KafkaReaderError::LagDataUnavailable => {
                write!(f, "consumer lag data is not currently available")
            }
            KafkaReaderError::BackendFailure(msg) => write!(f, "backend failure: {msg}"),
        }
    }
}

impl std::error::Error for KafkaReaderError {}

/// Kafka-specific `BusEventReader` implementation that can optionally perform manual commits
/// and expose consumer lag metrics.
#[derive(bevy::ecs::system::SystemParam)]
pub struct KafkaEventReader<'w, 's, T: BusEvent + Event> {
    wrapped_events: Local<'s, Vec<EventWrapper<T>>>,
    metadata_drained: Option<ResMut<'w, DrainedTopicMetadata>>,
    metadata_offsets: Local<'s, HashMap<String, usize>>,
    commit_queue: Option<Res<'w, KafkaCommitQueue>>,
    lag_cache: Option<Res<'w, KafkaLagCacheResource>>,
}

impl<'w, 's, T: BusEvent + Event> KafkaEventReader<'w, 's, T> {
    /// Read all messages for the supplied Kafka configuration. When manual commit is requested
    /// (auto commit disabled) the reader ensures the backend has manual commit mode enabled
    /// for the consumer group before returning events.
    pub fn read(&mut self, config: &KafkaConsumerConfig) -> Vec<EventWrapper<T>> {
        self.read_internal(config)
    }

    /// Commit the supplied event using Kafka manual offsets. The configuration used to read the
    /// event must have auto commit disabled.
    pub fn commit(
        &mut self,
        config: &KafkaConsumerConfig,
        event: &EventWrapper<T>,
    ) -> Result<(), KafkaReaderError> {
        if config.is_auto_commit_enabled() {
            return Err(KafkaReaderError::ManualCommitDisabled);
        }

        let metadata = event
            .metadata()
            .kafka_metadata()
            .ok_or(KafkaReaderError::MissingKafkaMetadata)?;

        let consumer_group = metadata
            .consumer_group
            .clone()
            .ok_or(KafkaReaderError::MissingConsumerGroup)?;

        let queue = self
            .commit_queue
            .as_ref()
            .ok_or(KafkaReaderError::CommitQueueUnavailable)?;

        let request = KafkaCommitRequest {
            topic: metadata.topic.clone(),
            partition: metadata.partition,
            offset: metadata.offset,
            consumer_group,
        };

        match queue.0.try_send(request) {
            Ok(_) => Ok(()),
            Err(TrySendError::Full(_)) => Err(KafkaReaderError::CommitQueueFull),
            Err(TrySendError::Disconnected(_)) => Err(KafkaReaderError::CommitQueueUnavailable),
        }
    }

    /// Fetch consumer lag per topic for the supplied configuration.
    pub fn consumer_lag(
        &self,
        config: &KafkaConsumerConfig,
    ) -> Result<HashMap<String, i64>, KafkaReaderError> {
        let lag_cache = self
            .lag_cache
            .as_ref()
            .ok_or(KafkaReaderError::LagDataUnavailable)?;

        let snapshot = lag_cache.0.snapshot_for_group(config.get_consumer_group());

        if snapshot.is_empty() {
            return Err(KafkaReaderError::LagDataUnavailable);
        }

        let mut per_topic = HashMap::new();
        for topic in config.topics() {
            if let Some(measurement) = snapshot.get(topic) {
                per_topic.insert(topic.clone(), measurement.lag);
            } else {
                return Err(KafkaReaderError::LagDataUnavailable);
            }
        }

        Ok(per_topic)
    }

    /// Backend-agnostic read implementation shared between the trait impl and the
    /// Kafka-specific helper. It drains decoded events from `DrainedTopicMetadata` and
    /// converts them into typed wrappers.
    fn read_internal<C: EventBusConfig>(&mut self, config: &C) -> Vec<EventWrapper<T>> {
        let mut all_events = Vec::new();
        let topics = config.topics();

        for topic in topics {
            self.wrapped_events.clear();

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

            all_events.append(&mut *self.wrapped_events);
        }

        all_events
    }
}

impl<'w, 's, T: BusEvent + Event> BusEventReader<T> for KafkaEventReader<'w, 's, T> {
    fn read<C: EventBusConfig>(&mut self, config: &C) -> Vec<EventWrapper<T>> {
        self.read_internal(config)
    }
}
