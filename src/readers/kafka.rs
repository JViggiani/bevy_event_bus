use std::collections::{HashMap, HashSet};

use bevy::prelude::*;

use bevy_event_bus::backends::EventBusBackendResource;
use bevy_event_bus::config::{EventBusConfig, kafka::KafkaConsumerConfig};
use bevy_event_bus::resources::{DrainedTopicMetadata, EventWrapper};
use bevy_event_bus::{BusEvent, runtime};

use super::BusEventReader;

/// Errors that can occur when working with the Kafka-specific reader.
#[derive(Debug)]
pub enum KafkaReaderError {
    /// No backend resource was found in the Bevy world.
    BackendUnavailable,
    /// The event did not contain Kafka metadata, so committing is impossible.
    MissingKafkaMetadata,
    /// Manual commit operations were requested while auto-commit is still enabled.
    ManualCommitDisabled,
    /// Backend responded with an error message.
    BackendFailure(String),
}

impl KafkaReaderError {
    fn backend(err: String) -> Self {
        Self::BackendFailure(err)
    }
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
    backend: Option<Res<'w, EventBusBackendResource>>,
    manual_commit_groups: Local<'s, HashSet<String>>,
}

impl<'w, 's, T: BusEvent + Event> KafkaEventReader<'w, 's, T> {
    /// Read all messages for the supplied Kafka configuration. When manual commit is requested
    /// (auto commit disabled) the reader ensures the backend has manual commit mode enabled
    /// for the consumer group before returning events.
    pub fn read(&mut self, config: &KafkaConsumerConfig) -> Vec<EventWrapper<T>> {
        if !config.is_auto_commit_enabled() {
            if let Err(err) = self.ensure_manual_commit_group(config) {
                tracing::error!("failed to enable manual commit mode: {err}");
            }
        }
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

        let backend_res = self
            .backend
            .as_ref()
            .ok_or(KafkaReaderError::BackendUnavailable)?;

        let backend = backend_res.read();
        runtime::block_on(backend.commit_offset(
            &metadata.topic,
            metadata.partition,
            metadata.offset,
        ))
        .map_err(KafkaReaderError::backend)
    }

    /// Fetch consumer lag per topic for the supplied configuration.
    pub fn consumer_lag(
        &self,
        config: &KafkaConsumerConfig,
    ) -> Result<HashMap<String, i64>, KafkaReaderError> {
        let backend_res = self
            .backend
            .as_ref()
            .ok_or(KafkaReaderError::BackendUnavailable)?;
        let backend = backend_res.read();

        let mut per_topic = HashMap::new();
        for topic in config.topics() {
            let lag =
                runtime::block_on(backend.get_consumer_lag(topic, config.get_consumer_group()))
                    .map_err(KafkaReaderError::backend)?;
            per_topic.insert(topic.clone(), lag);
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

    fn ensure_manual_commit_group(
        &mut self,
        config: &KafkaConsumerConfig,
    ) -> Result<(), KafkaReaderError> {
        let group_id = config.get_consumer_group().to_string();
        if self.manual_commit_groups.contains(&group_id) {
            return Ok(());
        }

        let backend_res = self
            .backend
            .as_ref()
            .ok_or(KafkaReaderError::BackendUnavailable)?;

        let mut backend = backend_res.write();
        runtime::block_on(backend.enable_manual_commits(&group_id))
            .map_err(KafkaReaderError::backend)?;
        self.manual_commit_groups.insert(group_id);
        Ok(())
    }
}

impl<'w, 's, T: BusEvent + Event> BusEventReader<T> for KafkaEventReader<'w, 's, T> {
    fn read<C: EventBusConfig>(&mut self, config: &C) -> Vec<EventWrapper<T>> {
        self.read_internal(config)
    }
}
