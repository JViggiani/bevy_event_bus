use std::time::Instant;
use bevy::prelude::*;
use crossbeam_channel::Receiver;

/// Raw incoming message captured by background consumer task
#[derive(Debug, Clone)]
pub struct IncomingMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub payload: Vec<u8>,
    pub timestamp: Instant,
}

/// Configuration controlling how many events are drained each frame
#[derive(Resource, Debug, Clone)]
pub struct EventBusConsumerConfig {
    /// Maximum events to drain per frame (None = unlimited)
    pub max_events_per_frame: Option<usize>,
    /// Optional millisecond budget for drain loop (None = no time limit)
    pub max_drain_millis: Option<u64>,
}
impl Default for EventBusConsumerConfig {
    fn default() -> Self { Self { max_events_per_frame: None, max_drain_millis: None } }
}

/// Channel receiver resource for background consumer -> main thread
#[derive(Resource)]
pub struct MessageQueue { pub receiver: Receiver<IncomingMessage> }

/// Per-topic raw payload buffers filled by drain system each frame
#[derive(Resource, Default, Debug)]
pub struct DrainedTopicBuffers { pub topics: std::collections::HashMap<String, Vec<Vec<u8>>> }

/// Basic consumer metrics (frame-scoped counters + cumulative stats)
#[derive(Resource, Debug, Clone)]
pub struct ConsumerMetrics {
    pub drained_last_frame: usize,
    pub remaining_channel_after_drain: usize,
    pub dropped_messages: usize,
    pub total_drained: usize,
    pub queue_len_start: usize,
    pub queue_len_end: usize,
    pub drain_duration_us: u128,
    pub idle_frames: usize,
}
impl Default for ConsumerMetrics {
    fn default() -> Self {
        Self { drained_last_frame: 0, remaining_channel_after_drain: 0, dropped_messages: 0, total_drained: 0, queue_len_start: 0, queue_len_end: 0, drain_duration_us: 0, idle_frames: 0 }
    }
}

/// Event emitted after each drain with snapshot metrics (optional for user systems)
#[derive(Event, Debug, Clone)]
pub struct DrainMetricsEvent {
    pub drained: usize,
    pub remaining: usize,
    pub total_drained: usize,
    pub dropped: usize,
    pub drain_duration_us: u128,
}
