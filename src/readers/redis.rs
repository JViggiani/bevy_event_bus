#![cfg(feature = "redis")]

use std::collections::{HashMap, HashSet};

use bevy::prelude::*;
use bevy_event_bus::config::EventBusConfig;
use bevy_event_bus::resources::{
    DrainedTopicMetadata, EventWrapper, ProvisionedTopology, RedisAckQueue, RedisAckRequest,
};
use bevy_event_bus::{BusEvent, EventBusError, EventBusErrorType, readers::BusEventReader};
use crossbeam_channel::TrySendError;

/// Errors that can occur when working with the Redis-specific reader.
#[derive(Debug)]
pub enum RedisReaderError {
    /// The acknowledgement queue is unavailable because manual acknowledgements are disabled.
    AckQueueUnavailable,
    /// The acknowledgement queue is full; retry on a subsequent frame.
    AckQueueFull,
    /// The event is missing Redis metadata, preventing acknowledgement.
    MissingRedisMetadata,
    /// The reader configuration or associated metadata is invalid.
    InvalidReadConfig(String),
}

impl std::fmt::Display for RedisReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisReaderError::AckQueueUnavailable => {
                write!(f, "Redis acknowledgement queue is unavailable")
            }
            RedisReaderError::AckQueueFull => {
                write!(
                    f,
                    "Redis acknowledgement queue is full; try again next frame"
                )
            }
            RedisReaderError::MissingRedisMetadata => {
                write!(
                    f,
                    "event is missing Redis metadata required for acknowledgement"
                )
            }
            RedisReaderError::InvalidReadConfig(reason) => {
                write!(f, "invalid Redis reader configuration: {reason}")
            }
        }
    }
}

impl std::error::Error for RedisReaderError {}

/// Redis-specific `BusEventReader` implementation that exposes manual acknowledgements.
#[derive(bevy::ecs::system::SystemParam)]
pub struct RedisEventReader<'w, 's, T: BusEvent + Event> {
    wrapped_events: Local<'s, Vec<EventWrapper<T>>>,
    metadata_drained: Option<ResMut<'w, DrainedTopicMetadata>>,
    metadata_offsets: Local<'s, HashMap<String, usize>>,
    ack_queue: Option<Res<'w, RedisAckQueue>>,
    topology: Option<Res<'w, ProvisionedTopology>>,
    invalid_config_reports: Local<'s, HashSet<String>>,
    error_writer: EventWriter<'w, EventBusError<T>>,
}

impl<'w, 's, T: BusEvent + Event> RedisEventReader<'w, 's, T> {
    /// Read all messages for the supplied Redis configuration.
    pub fn read<C: EventBusConfig>(&mut self, config: &C) -> Vec<EventWrapper<T>> {
        let redis_config = config
            .as_any()
            .downcast_ref::<bevy_event_bus::config::redis::RedisConsumerConfig>();

        if let Some(redis_config) = redis_config {
            if !self.validate_configuration(redis_config) {
                return Vec::new();
            }
        }

        let required_consumer_group = redis_config.and_then(|cfg| cfg.consumer_group());

        let mut all_events = Vec::new();
        let topics = config.topics();

        for topic in topics {
            self.wrapped_events.clear();

            if let Some(metadata_drained) = &mut self.metadata_drained {
                if let Some(messages) = metadata_drained.topics.get(topic) {
                    let start = *self.metadata_offsets.get(topic).unwrap_or(&0);
                    if start < messages.len() {
                        for processed_msg in messages.iter().skip(start) {
                            // Filter by consumer group if specified
                            let message_matches_group = match required_consumer_group {
                                Some(required_group) => {
                                    // Check if message is from the required consumer group
                                    let message_group = processed_msg
                                        .metadata
                                        .backend_specific
                                        .as_ref()
                                        .and_then(|meta| meta.as_any().downcast_ref::<crate::resources::backend_metadata::RedisMetadata>())
                                        .and_then(|redis_meta| redis_meta.consumer_group.as_deref());

                                    message_group
                                        .map(|msg_group| msg_group == required_group)
                                        .unwrap_or(false)
                                }
                                None => true, // No filter - accept all messages
                            };

                            if message_matches_group {
                                match serde_json::from_slice::<T>(&processed_msg.payload) {
                                    Ok(event) => self.wrapped_events.push(EventWrapper::new(
                                        event,
                                        processed_msg.metadata.clone(),
                                    )),
                                    Err(_) => tracing::warn!(
                                        "Failed to deserialize event from stream {}",
                                        topic
                                    ),
                                }
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

    /// Acknowledge a previously read message. Only valid when manual acknowledgements are enabled.
    pub fn acknowledge(&mut self, event: &EventWrapper<T>) -> Result<(), RedisReaderError> {
        let queue = self
            .ack_queue
            .as_ref()
            .ok_or(RedisReaderError::AckQueueUnavailable)?;

        let metadata = event
            .metadata()
            .redis_metadata()
            .ok_or(RedisReaderError::MissingRedisMetadata)?;

        let consumer_group = metadata.consumer_group.as_ref().ok_or_else(|| {
            RedisReaderError::InvalidReadConfig(
                "event does not reference a consumer group".to_string(),
            )
        })?;

        let request = RedisAckRequest::new(&metadata.stream, &metadata.entry_id, consumer_group);

        match queue.0.try_send(request) {
            Ok(_) => Ok(()),
            Err(TrySendError::Full(_)) => Err(RedisReaderError::AckQueueFull),
            Err(TrySendError::Disconnected(_)) => Err(RedisReaderError::AckQueueUnavailable),
        }
    }
}

impl<'w, 's, T: BusEvent + Event> RedisEventReader<'w, 's, T> {
    fn validate_configuration(
        &mut self,
        config: &bevy_event_bus::config::redis::RedisConsumerConfig,
    ) -> bool {
        let Some(topology) = self.topology.as_ref().and_then(|topo| topo.redis()) else {
            return true;
        };

        let topology_streams = topology.stream_names();

        if let Some(group_id) = config.consumer_group() {
            if let Some(spec) = topology.consumer_groups().get(group_id) {
                if let Some(stream) = config
                    .streams()
                    .iter()
                    .find(|stream| !topology_streams.contains(*stream))
                {
                    let reason = format!(
                        "Stream '{}' is not provisioned in the Redis topology",
                        stream
                    );
                    self.record_invalid_config(config, stream.clone(), Some(group_id), reason);
                    return false;
                }

                if let Some(stream) = config
                    .streams()
                    .iter()
                    .find(|stream| !spec.streams.iter().any(|s| s == *stream))
                {
                    let reason = format!(
                        "Stream '{}' is not assigned to consumer group '{}' in the topology",
                        stream, group_id
                    );
                    self.record_invalid_config(config, stream.clone(), Some(group_id), reason);
                    return false;
                }
            } else {
                let stream = config
                    .primary_stream()
                    .map(|s| s.to_string())
                    .or_else(|| config.streams().first().cloned())
                    .unwrap_or_else(|| "unknown".to_string());
                let reason = format!(
                    "Consumer group '{}' is not provisioned in the Redis topology",
                    group_id
                );
                self.record_invalid_config(config, stream, Some(group_id), reason);
                return false;
            }
        } else if let Some(stream) = config
            .streams()
            .iter()
            .find(|stream| !topology_streams.contains(*stream))
        {
            let reason = format!(
                "Stream '{}' is not provisioned in the Redis topology",
                stream
            );
            self.record_invalid_config(config, stream.clone(), None, reason);
            return false;
        }

        true
    }

    fn record_invalid_config(
        &mut self,
        config: &bevy_event_bus::config::redis::RedisConsumerConfig,
        stream: String,
        group: Option<&str>,
        reason: String,
    ) {
        let key = format!("{}:{reason}", config.config_id());
        if self.invalid_config_reports.insert(key) {
            let error = EventBusError {
                topic: stream.clone(),
                error_type: EventBusErrorType::InvalidReadConfig,
                error_message: reason.clone(),
                timestamp: std::time::SystemTime::now(),
                original_event: None,
                backend: Some(String::from("redis")),
                metadata: None,
            };
            self.error_writer.write(error);
            tracing::warn!(
                backend = "redis",
                consumer_group = group.unwrap_or("ungrouped"),
                stream = %stream,
                reason = %reason,
                "Redis reader configuration invalid"
            );
        }
    }
}

impl<'w, 's, T: BusEvent + Event> BusEventReader<T> for RedisEventReader<'w, 's, T> {
    fn read<C: EventBusConfig>(&mut self, config: &C) -> Vec<EventWrapper<T>> {
        RedisEventReader::read(self, config)
    }
}
