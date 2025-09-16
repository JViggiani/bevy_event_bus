use bevy::prelude::*;
use crossbeam_channel::Receiver;
use std::time::Instant;

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

/// Outbound message queued for delivery to external broker
#[derive(Debug, Clone)]
pub struct OutboundMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    /// Unique ID for tracking delivery success/failure
    pub delivery_id: u64,
}

/// Events fired when messages are successfully delivered or fail
#[derive(Event, Debug, Clone)]
pub enum DeliveryEvent {
    Success { delivery_id: u64, topic: String },
    Failed { delivery_id: u64, topic: String, error: String },
}

/// Queue of outbound messages waiting to be sent
#[derive(Resource, Debug)]
pub struct OutboundMessageQueue {
    pub messages: std::sync::Mutex<std::collections::VecDeque<OutboundMessage>>,
    pub next_delivery_id: std::sync::atomic::AtomicU64,
}

impl Default for OutboundMessageQueue {
    fn default() -> Self {
        Self {
            messages: std::sync::Mutex::new(std::collections::VecDeque::new()),
            next_delivery_id: std::sync::atomic::AtomicU64::new(1),
        }
    }
}

/// Configuration controlling how many events are drained each frame
#[derive(Resource, Debug, Clone)]
#[derive(Default)]
pub struct EventBusConsumerConfig {
    /// Maximum events to drain per frame (None = unlimited)
    pub max_events_per_frame: Option<usize>,
    /// Optional millisecond budget for drain loop (None = no time limit)
    pub max_drain_millis: Option<u64>,
}

/// Channel receiver resource for background consumer -> main thread
#[derive(Resource)]
pub struct MessageQueue {
    pub receiver: Receiver<IncomingMessage>,
}

/// Per-topic raw payload buffers filled by drain system each frame
#[derive(Resource, Default, Debug)]
pub struct DrainedTopicBuffers {
    pub topics: std::collections::HashMap<String, Vec<Vec<u8>>>,
}

/// Basic consumer metrics (frame-scoped counters + cumulative stats)
#[derive(Resource, Debug, Clone)]
#[derive(Default)]
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

/// Event emitted after each drain with snapshot metrics (optional for user systems)
#[derive(Event, Debug, Clone)]
pub struct DrainMetricsEvent {
    pub drained: usize,
    pub remaining: usize,
    pub total_drained: usize,
    pub dropped: usize,
    pub drain_duration_us: u128,
}
