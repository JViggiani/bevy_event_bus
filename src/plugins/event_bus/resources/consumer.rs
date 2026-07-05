use bevy::prelude::*;
use crossbeam_channel::Receiver;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::resources::ProcessedMessage;

/// Configuration controlling how many events are drained each frame.
#[derive(Resource, Debug, Clone, Default)]
pub struct EventBusConsumerConfig {
    /// Maximum events to drain per frame (None = unlimited)
    pub max_events_per_frame: Option<usize>,
    /// Optional millisecond budget for drain loop (None = no time limit)
    pub max_drain_millis: Option<u64>,
}

/// Channel receiver resource for background consumer -> main thread.
#[derive(Resource)]
pub struct MessageQueue {
    pub receiver: Receiver<crate::resources::IncomingMessage>,
}

/// Monotonic source of stable reader identifiers.
static NEXT_READER_ID: AtomicU64 = AtomicU64::new(1);

/// Allocate a process-unique reader id used to track per-reader consumption.
pub fn allocate_reader_id() -> u64 {
    NEXT_READER_ID.fetch_add(1, Ordering::Relaxed)
}

/// Default upper bound on the number of messages retained per topic.
pub const DEFAULT_MAX_RETAINED_PER_TOPIC: usize = 10_000;

/// Ring-style buffer of drained messages for a single topic.
#[derive(Default, Debug)]
pub struct TopicMessageBuffer {
    messages: Vec<ProcessedMessage>,
    base_offset: usize,
    reader_watermarks: HashMap<u64, usize>,
}

impl TopicMessageBuffer {
    pub fn absolute_len(&self) -> usize {
        self.base_offset + self.messages.len()
    }

    pub fn base_offset(&self) -> usize {
        self.base_offset
    }

    pub fn messages(&self) -> &[ProcessedMessage] {
        &self.messages
    }

    pub fn push(&mut self, message: ProcessedMessage) {
        self.messages.push(message);
    }

    pub fn resolve_start(&self, requested_abs: usize) -> (usize, usize) {
        let effective_abs = requested_abs.max(self.base_offset);
        let relative = effective_abs - self.base_offset;
        (effective_abs, relative)
    }

    pub fn note_reader_progress(&mut self, reader_id: u64, absolute_offset: usize) {
        let entry = self.reader_watermarks.entry(reader_id).or_insert(0);
        *entry = (*entry).max(absolute_offset);
    }

    pub fn compact(&mut self, max_retained: usize) -> usize {
        let mut drop_to_abs = self.reader_watermarks.values().copied().min().unwrap_or(0);

        let retained_after_watermark = self.absolute_len().saturating_sub(drop_to_abs);
        if retained_after_watermark > max_retained {
            let overflow = retained_after_watermark - max_retained;
            drop_to_abs += overflow;
        }

        if drop_to_abs <= self.base_offset {
            return 0;
        }

        let drop_count = (drop_to_abs - self.base_offset).min(self.messages.len());
        if drop_count == 0 {
            return 0;
        }
        self.messages.drain(..drop_count);
        self.base_offset += drop_count;
        drop_count
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.messages.len()
    }
}

/// Per-topic message buffers filled by the drain system and consumed by readers.
#[derive(Resource, Default, Debug)]
pub struct DrainedTopicMetadata {
    topics: HashMap<String, TopicMessageBuffer>,
}

impl DrainedTopicMetadata {
    pub fn push(&mut self, topic: &str, message: ProcessedMessage) {
        self.topics
            .entry(topic.to_string())
            .or_default()
            .push(message);
    }

    pub fn is_empty(&self) -> bool {
        self.topics
            .values()
            .all(|buffer| buffer.messages().is_empty())
    }

    pub fn buffer(&self, topic: &str) -> Option<&TopicMessageBuffer> {
        self.topics.get(topic)
    }

    pub fn buffer_mut(&mut self, topic: &str) -> Option<&mut TopicMessageBuffer> {
        self.topics.get_mut(topic)
    }

    pub fn compact_all(&mut self, max_retained: usize) -> usize {
        self.topics
            .values_mut()
            .map(|buffer| buffer.compact(max_retained))
            .sum()
    }
}

/// Basic consumer metrics (frame-scoped counters + cumulative stats).
#[derive(Resource, Debug, Clone, Default)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::MessageMetadata;
    use std::time::Instant;

    fn processed(payload: &str) -> ProcessedMessage {
        ProcessedMessage {
            payload: payload.as_bytes().to_vec(),
            metadata: MessageMetadata::new("topic".to_string(), Instant::now(), None, None),
        }
    }

    #[test]
    fn buffer_reconciles_reader_offset_against_base() {
        let mut buffer = TopicMessageBuffer::default();
        for i in 0..5 {
            buffer.push(processed(&i.to_string()));
        }
        assert_eq!(buffer.absolute_len(), 5);

        let (start_abs, rel) = buffer.resolve_start(0);
        assert_eq!((start_abs, rel), (0, 0));
        buffer.note_reader_progress(1, 5);

        assert_eq!(buffer.compact(DEFAULT_MAX_RETAINED_PER_TOPIC), 5);
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.base_offset(), 5);

        buffer.push(processed("5"));
        assert_eq!(buffer.absolute_len(), 6);

        let (start_abs, rel) = buffer.resolve_start(5);
        assert_eq!((start_abs, rel), (5, 0));
    }

    #[test]
    fn buffer_retains_prefix_for_slowest_reader() {
        let mut buffer = TopicMessageBuffer::default();
        for i in 0..10 {
            buffer.push(processed(&i.to_string()));
        }

        buffer.note_reader_progress(1, 10);
        buffer.note_reader_progress(2, 3);

        assert_eq!(buffer.compact(DEFAULT_MAX_RETAINED_PER_TOPIC), 3);
        assert_eq!(buffer.base_offset(), 3);
        assert_eq!(buffer.len(), 7);
    }

    #[test]
    fn buffer_enforces_max_retained_without_readers() {
        let mut buffer = TopicMessageBuffer::default();
        for i in 0..50 {
            buffer.push(processed(&i.to_string()));
        }

        let dropped = buffer.compact(10);
        assert_eq!(dropped, 40);
        assert_eq!(buffer.len(), 10);
        assert_eq!(buffer.base_offset(), 40);
    }

    #[test]
    fn drained_metadata_push_and_compact_roundtrip() {
        let mut drained = DrainedTopicMetadata::default();
        for i in 0..4 {
            drained.push("alpha", processed(&i.to_string()));
        }
        assert_eq!(drained.buffer("alpha").unwrap().absolute_len(), 4);

        drained
            .buffer_mut("alpha")
            .unwrap()
            .note_reader_progress(7, 4);
        assert_eq!(drained.compact_all(DEFAULT_MAX_RETAINED_PER_TOPIC), 4);
        assert_eq!(drained.buffer("alpha").unwrap().len(), 0);
    }
}
