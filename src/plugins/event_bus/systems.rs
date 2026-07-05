use bevy::ecs::system::SystemParam;
use bevy::prelude::*;
use std::marker::PhantomData;

use crate::backends::EventBusBackendResource;
use crate::plugins::event_bus::messages::{BackendReadyMessage, DrainMetricsMessage};
use crate::plugins::event_bus::resources::{
    BackendReadyInfo, BackendStatus, ConsumerMetrics, DEFAULT_MAX_RETAINED_PER_TOPIC,
    DrainedTopicMetadata, EventBusConsumerConfig, MessageQueue,
};
use crate::resources::{IncomingMessage, MessageMetadata, ProcessedMessage};

#[derive(SystemParam)]
pub(crate) struct DrainSystemResources<'w, 's> {
    metadata_buffers: ResMut<'w, DrainedTopicMetadata>,
    metrics: ResMut<'w, ConsumerMetrics>,
    config: Res<'w, EventBusConsumerConfig>,
    _marker: PhantomData<&'s ()>,
}

/// Moves messages off the background consumer channel into per-topic buffers.
pub(crate) fn drain_incoming_messages(
    backend: Option<Res<EventBusBackendResource>>,
    resources: DrainSystemResources,
    maybe_queue: Option<Res<MessageQueue>>,
    mut drain_events: MessageWriter<DrainMetricsMessage>,
) {
    let DrainSystemResources {
        metadata_buffers,
        metrics,
        config,
        ..
    } = resources;

    let mut metadata_buffers = metadata_buffers;
    let mut metrics = metrics;

    let frame_start = std::time::Instant::now();
    metrics.drained_last_frame = 0;
    metrics.queue_len_start = 0;
    metrics.queue_len_end = 0;

    if let Some(queue) = maybe_queue {
        metrics.queue_len_start = queue.receiver.len();
        let limit = config.max_events_per_frame;
        let time_budget = config
            .max_drain_millis
            .map(std::time::Duration::from_millis);

        while limit
            .map(|l| metrics.drained_last_frame < l)
            .unwrap_or(true)
        {
            if let Some(budget) = time_budget {
                if frame_start.elapsed() >= budget {
                    break;
                }
                if budget <= std::time::Duration::from_millis(1) && metrics.drained_last_frame > 0
                {
                    break;
                }
            }

            match queue.receiver.try_recv() {
                Ok(msg) => {
                    let IncomingMessage {
                        source,
                        payload,
                        key,
                        timestamp,
                        backend_metadata,
                    } = msg;

                    let topic_str = source.as_str();
                    bevy::log::trace!(topic = %topic_str, "Draining message into topic buffer");

                    let metadata =
                        MessageMetadata::new(source.clone(), timestamp, key, backend_metadata);

                    metadata_buffers.push(topic_str, ProcessedMessage { payload, metadata });
                    metrics.drained_last_frame += 1;
                }
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => break,
            }
        }

        metrics.remaining_channel_after_drain = queue.receiver.len();
        metrics.queue_len_end = metrics.remaining_channel_after_drain;
        metrics.total_drained += metrics.drained_last_frame;
    }

    if let Some(backend_res) = backend.as_ref() {
        backend_res.read().augment_metrics(metrics.as_mut());
    }

    if metrics.drained_last_frame == 0 {
        metrics.idle_frames += 1;
    }

    let dropped = metadata_buffers.compact_all(DEFAULT_MAX_RETAINED_PER_TOPIC);
    if dropped > 0 {
        bevy::log::trace!(dropped, "Compacted consumed messages from topic buffers");
    }

    metrics.drain_duration_us = frame_start.elapsed().as_micros();
    if metrics.drain_duration_us == 0
        && (metrics.drained_last_frame > 0 || metrics.queue_len_start > 0)
    {
        metrics.drain_duration_us = 1;
    }

    drain_events.write(DrainMetricsMessage {
        drained: metrics.drained_last_frame,
        remaining: metrics.remaining_channel_after_drain,
        total_drained: metrics.total_drained,
        dropped: metrics.dropped_messages,
        drain_duration_us: metrics.drain_duration_us,
    });
}

pub(crate) fn emit_backend_ready(
    info: Res<BackendReadyInfo>,
    mut ready_writer: MessageWriter<BackendReadyMessage>,
    mut status: ResMut<BackendStatus>,
) {
    ready_writer.write(BackendReadyMessage {
        backend: info.backend.clone(),
        topics: info.topics.clone(),
    });
    status.ready = true;
}
