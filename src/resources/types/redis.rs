use bevy::prelude::*;
use crossbeam_channel::Sender;

/// Request dispatched by readers when acknowledging a Redis Stream entry.
#[derive(Debug, Clone)]
pub struct RedisAckRequest {
    pub stream: String,
    pub entry_id: String,
    pub consumer_group: String,
}

impl RedisAckRequest {
    pub fn new(
        stream: impl Into<String>,
        entry_id: impl Into<String>,
        consumer_group: impl Into<String>,
    ) -> Self {
        Self {
            stream: stream.into(),
            entry_id: entry_id.into(),
            consumer_group: consumer_group.into(),
        }
    }
}

/// Channel resource used by Redis readers to enqueue acknowledgement requests.
#[derive(Resource, Clone)]
pub struct RedisAckQueue(pub Sender<RedisAckRequest>);
