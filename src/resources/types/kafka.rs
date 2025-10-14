use crossbeam_channel::{Receiver, Sender};

use crate::backends::kafka_backend::{KafkaCommitRequest, KafkaCommitResult, KafkaLagCache};
use bevy::prelude::*;

/// Channel used by `KafkaEventReader` to enqueue manual commit requests handled in the background.
#[derive(Resource, Clone)]
pub struct KafkaCommitQueue(pub Sender<KafkaCommitRequest>);

/// Channel delivering results of commit attempts back to the main thread for event dispatching.
#[derive(Resource)]
pub struct KafkaCommitResultChannel {
    pub receiver: Receiver<KafkaCommitResult>,
}

/// Shared cache containing the latest per-topic consumer lag measurements.
#[derive(Resource, Clone)]
pub struct KafkaLagCacheResource(pub KafkaLagCache);
