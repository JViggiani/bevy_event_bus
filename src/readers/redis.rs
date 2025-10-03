#![cfg(feature = "redis")]

use std::collections::HashMap;

use bevy::prelude::*;
use bevy_event_bus::config::EventBusConfig;
use bevy_event_bus::resources::{
    DrainedTopicMetadata, EventWrapper, RedisAckQueue, RedisAckRequest,
};
use bevy_event_bus::{BusEvent, readers::BusEventReader};
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
    /// The event was produced without a consumer group, so acknowledgement is undefined.
    MissingConsumerGroup,
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
            RedisReaderError::MissingConsumerGroup => {
                write!(f, "event does not reference a consumer group")
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
}

impl<'w, 's, T: BusEvent + Event> RedisEventReader<'w, 's, T> {
    /// Read all messages for the supplied Redis configuration.
    pub fn read<C: EventBusConfig>(&mut self, config: &C) -> Vec<EventWrapper<T>> {
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
                                    "Failed to deserialize event from stream {}",
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

        let consumer_group = metadata
            .consumer_group
            .as_ref()
            .ok_or(RedisReaderError::MissingConsumerGroup)?;

        let request = RedisAckRequest::new(&metadata.stream, &metadata.entry_id, consumer_group);

        match queue.0.try_send(request) {
            Ok(_) => Ok(()),
            Err(TrySendError::Full(_)) => Err(RedisReaderError::AckQueueFull),
            Err(TrySendError::Disconnected(_)) => Err(RedisReaderError::AckQueueUnavailable),
        }
    }
}

impl<'w, 's, T: BusEvent + Event> BusEventReader<T> for RedisEventReader<'w, 's, T> {
    fn read<C: EventBusConfig>(&mut self, config: &C) -> Vec<EventWrapper<T>> {
        RedisEventReader::read(self, config)
    }
}
