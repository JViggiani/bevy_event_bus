use bevy::prelude::Resource;

use crate::config::{kafka::KafkaTopologyConfig, redis::RedisTopologyConfig};

/// Tracks the provisioned backend topologies so readers can validate configuration without
/// touching backend internals. Only one backend of each type is expected to be active.
#[derive(Resource, Clone, Debug, Default)]
pub struct ProvisionedTopology {
    kafka: Option<KafkaTopologyConfig>,
    redis: Option<RedisTopologyConfig>,
}

impl ProvisionedTopology {
    pub fn record_kafka(&mut self, topology: KafkaTopologyConfig) {
        self.kafka = Some(topology);
    }

    pub fn record_redis(&mut self, topology: RedisTopologyConfig) {
        self.redis = Some(topology);
    }

    pub fn kafka(&self) -> Option<&KafkaTopologyConfig> {
        self.kafka.as_ref()
    }

    pub fn redis(&self) -> Option<&RedisTopologyConfig> {
        self.redis.as_ref()
    }
}
