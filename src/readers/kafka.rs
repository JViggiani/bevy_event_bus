use std::collections::{HashMap, HashSet};

use bevy::prelude::*;

use bevy_event_bus::backends::KafkaCommitRequest;
use bevy_event_bus::config::{EventBusConfig, kafka::KafkaConsumerConfig};
use bevy_event_bus::resources::{
    DrainedTopicMetadata, KafkaCommitQueue, KafkaLagCacheResource, MessageWrapper,
    ProvisionedTopology,
};
use bevy_event_bus::{BusEvent, EventBusError, EventBusErrorType, readers::BusMessageReader};
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
    /// The reader configuration or associated metadata is invalid.
    InvalidReadConfig(String),
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
            KafkaReaderError::InvalidReadConfig(reason) => {
                write!(f, "invalid Kafka reader configuration: {reason}")
            }
            KafkaReaderError::LagDataUnavailable => {
                write!(f, "consumer lag data is not currently available")
            }
            KafkaReaderError::BackendFailure(msg) => write!(f, "backend failure: {msg}"),
        }
    }
}

impl std::error::Error for KafkaReaderError {}

/// Kafka-specific `BusMessageReader` implementation that can optionally perform manual commits
/// and expose consumer lag metrics.
#[derive(bevy::ecs::system::SystemParam)]
pub struct KafkaMessageReader<'w, 's, T: BusEvent + Message> {
    wrapped_events: Local<'s, Vec<MessageWrapper<T>>>,
    metadata_drained: Option<ResMut<'w, DrainedTopicMetadata>>,
    metadata_offsets: Local<'s, HashMap<String, usize>>,
    commit_queue: Option<Res<'w, KafkaCommitQueue>>,
    lag_cache: Option<Res<'w, KafkaLagCacheResource>>,
    topology: Option<Res<'w, ProvisionedTopology>>,
    invalid_config_reports: Local<'s, HashSet<String>>,
    error_writer: MessageWriter<'w, EventBusError<T>>,
}

impl<'w, 's, T: BusEvent + Message> KafkaMessageReader<'w, 's, T> {
    /// Read all messages for the supplied Kafka configuration. When manual commit is requested
    /// (auto commit disabled) the reader ensures the backend has manual commit mode enabled
    /// for the consumer group before returning events.
    pub fn read<C: EventBusConfig>(&mut self, config: &C) -> Vec<MessageWrapper<T>> {
        if let Some(kafka_config) = config.as_any().downcast_ref::<KafkaConsumerConfig>() {
            if !self.validate_configuration(kafka_config) {
                return Vec::new();
            }
        }

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
                                Ok(event) => self.wrapped_events.push(MessageWrapper::new(
                                    event,
                                    processed_msg.metadata.clone(),
                                )),
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

    /// Commit the supplied event using Kafka manual offsets. The configuration used to read the
    /// event must have auto commit disabled.
    pub fn commit(
        &mut self,
        config: &KafkaConsumerConfig,
        event: &MessageWrapper<T>,
    ) -> Result<(), KafkaReaderError> {
        if config.is_auto_commit_enabled() {
            return Err(KafkaReaderError::ManualCommitDisabled);
        }

        let metadata = event
            .metadata()
            .kafka_metadata()
            .ok_or(KafkaReaderError::MissingKafkaMetadata)?;

        let consumer_group = metadata.consumer_group.clone().ok_or_else(|| {
            KafkaReaderError::InvalidReadConfig(
                "event is missing consumer group metadata".to_string(),
            )
        })?;

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
}

impl<'w, 's, T: BusEvent + Message> KafkaMessageReader<'w, 's, T> {
    fn validate_configuration(&mut self, config: &KafkaConsumerConfig) -> bool {
        let Some(topology) = self.topology.as_ref().and_then(|topo| topo.kafka()) else {
            return true;
        };

        let group_id = config.get_consumer_group();
        let topology_topics = topology.topic_names();

        if let Some(spec) = topology.consumer_groups().get(group_id) {
            let spec_topics: HashSet<_> = spec.topics.iter().collect();

            if let Some(topic) = config
                .topics()
                .iter()
                .find(|topic| !topology_topics.contains(*topic))
            {
                let reason = format!("Topic '{}' is not provisioned in the Kafka topology", topic);
                self.record_invalid_config(config, topic.clone(), group_id, reason);
                return false;
            }

            if let Some(topic) = config
                .topics()
                .iter()
                .find(|topic| !spec_topics.contains(topic))
            {
                let reason = format!(
                    "Topic '{}' is not assigned to consumer group '{}' in the topology",
                    topic, group_id
                );
                self.record_invalid_config(config, topic.clone(), group_id, reason);
                return false;
            }
        } else {
            let topic = topology
                .consumer_groups()
                .get(group_id)
                .and_then(|spec| spec.topics.first().cloned())
                .or_else(|| config.topics().first().cloned())
                .unwrap_or_else(|| "unknown".to_string());
            let reason = format!(
                "Consumer group '{}' is not provisioned in the Kafka topology",
                group_id
            );
            self.record_invalid_config(config, topic, group_id, reason);
            return false;
        }

        true
    }

    fn record_invalid_config(
        &mut self,
        config: &KafkaConsumerConfig,
        topic: String,
        group: &str,
        reason: String,
    ) {
        let key = format!("{}:{reason}", config.config_id());
        if self.invalid_config_reports.insert(key) {
            let error = EventBusError {
                topic: topic.clone(),
                error_type: EventBusErrorType::InvalidReadConfig,
                error_message: reason.clone(),
                timestamp: std::time::SystemTime::now(),
                original_event: None,
                backend: Some(String::from("kafka")),
                metadata: None,
            };
            self.error_writer.write(error);
            tracing::warn!(
                backend = "kafka",
                consumer_group = %group,
                topic = %topic,
                reason = %reason,
                "Kafka reader configuration invalid"
            );
        }
    }
}

impl<'w, 's, T: BusEvent + Message> BusMessageReader<T> for KafkaMessageReader<'w, 's, T> {
    fn read<C: EventBusConfig>(&mut self, config: &C) -> Vec<MessageWrapper<T>> {
        KafkaMessageReader::read(self, config)
    }
}
