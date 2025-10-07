use bevy::prelude::*;

use bevy_event_bus::backends::event_bus_backend::{LagReportingDescriptor, ManualCommitDescriptor};
use bevy_event_bus::backends::{EventBusBackend, EventBusBackendResource};
use bevy_event_bus::decoder::DecoderRegistry;
use bevy_event_bus::resources::{
    ConsumerMetrics, DecodedEventBuffer, DrainMetricsEvent, DrainedTopicMetadata,
    EventBusConsumerConfig, EventMetadata, IncomingMessage, MessageQueue, ProcessedMessage,
    TopicDecodedEvents,
};
use bevy_event_bus::runtime::{block_on, ensure_runtime};
use bevy_event_bus::writers::EventBusErrorQueue;

/// Plugin for integrating with external event brokers
pub struct EventBusPlugin;

impl Plugin for EventBusPlugin {
    fn build(&self, app: &mut App) {
        // Register core error events
        app.add_event::<bevy_event_bus::EventBusDecodeError>();
    }
}

/// Plugin bundle that configures everything needed for the event bus
pub struct EventBusPlugins<B: EventBusBackend>(pub B);

// -----------------------------------------------------------------------------
// Backend lifecycle events
// -----------------------------------------------------------------------------
#[derive(Event, Debug, Clone)]
pub struct BackendReadyEvent {
    pub backend: String,
    pub topics: Vec<String>,
}

#[derive(Event, Debug, Clone)]
pub struct BackendDownEvent {
    pub backend: String,
    pub reason: String,
}

#[derive(Debug)]
enum LifecycleMessage {
    Ready {
        backend: String,
        topics: Vec<String>,
    },
}

#[derive(Resource)]
struct BackendLifecycleChannel(crossbeam_channel::Receiver<LifecycleMessage>);

// Resource tracking backend readiness state
#[derive(Resource, Debug, Clone, Copy)]
pub struct BackendStatus {
    pub ready: bool,
}

#[derive(Resource, Debug, Clone)]
pub struct BackendCapabilities {
    pub backend: String,
    pub message_stream: bool,
    pub manual_commit: Option<ManualCommitDescriptor>,
    pub lag_reporting: Option<LagReportingDescriptor>,
}

impl BackendCapabilities {
    fn new(backend: impl Into<String>) -> Self {
        Self {
            backend: backend.into(),
            message_stream: false,
            manual_commit: None,
            lag_reporting: None,
        }
    }
}

impl<B: EventBusBackend> Plugin for EventBusPlugins<B> {
    fn build(&self, app: &mut App) {
        // Add the core plugin and ensure runtime exists
        app.add_plugins(EventBusPlugin);
        ensure_runtime(app);

        self.0.configure_plugin(app);
        // Create and add the backend as a resource
        let boxed = self.0.clone_box();
        app.insert_resource(EventBusBackendResource::from_box(boxed));
        app.insert_resource(EventBusErrorQueue::default());
        self.0.apply_event_bindings(app);
        bevy_event_bus::writers::outbound_bridge::activate_registered_bridges(app);
        // Connect backend and capture runtime resources (message queue, commit channels, lag cache).
        if let Some(backend_res) = app
            .world()
            .get_resource::<EventBusBackendResource>()
            .cloned()
        {
            let (backend_name, mut backend_setup) = {
                let mut guard = backend_res.write();
                let _ = block_on(guard.connect());
                let backend_name = guard.backend_name().to_string();
                let setup = guard.setup_plugin(app.world_mut());
                (backend_name, setup)
            };

            let mut capabilities = BackendCapabilities::new(backend_name.clone());

            if let Some(rx) = backend_setup.message_stream.take() {
                app.world_mut()
                    .insert_resource(MessageQueue { receiver: rx });
                capabilities.message_stream = true;
            }

            if let Some(handle) = backend_setup.manual_commit.take() {
                let descriptor = handle.descriptor();
                {
                    let world = app.world_mut();
                    handle.register_resources(world);
                }
                capabilities.manual_commit = Some(descriptor);
            }

            if let Some(handle) = backend_setup.lag_reporting.take() {
                let descriptor = handle.descriptor();
                {
                    let world = app.world_mut();
                    handle.register_resources(world);
                }
                capabilities.lag_reporting = Some(descriptor);
            }

            let (tx, rx_life) = crossbeam_channel::unbounded::<LifecycleMessage>();
            let ready_topics = backend_setup.ready_topics.clone();
            let _ = tx.send(LifecycleMessage::Ready {
                backend: backend_name.clone(),
                topics: ready_topics.clone(),
            });
            app.world_mut()
                .insert_resource(BackendLifecycleChannel(rx_life));
            app.world_mut()
                .insert_resource(BackendStatus { ready: false });
            app.world_mut().insert_resource(capabilities);
            // We cannot directly hook errors here; leave placeholder (Down events emitted by future backend error hook TBD)
        }

        // Initialize background consumer related resources if not present
        app.init_resource::<DrainedTopicMetadata>();
        app.init_resource::<DecodedEventBuffer>();
        app.init_resource::<DecoderRegistry>();
        app.init_resource::<ConsumerMetrics>();
        app.init_resource::<EventBusConsumerConfig>();
        app.add_event::<DrainMetricsEvent>();
        app.add_event::<BackendReadyEvent>();
        // MessageQueue will be inserted lazily once backend spawns sender & channel

        // Drain system with multi-decoder pipeline
        fn drain_system(
            backend: Option<Res<EventBusBackendResource>>,
            mut metadata_buffers: ResMut<DrainedTopicMetadata>,
            mut decoded_buffer: ResMut<DecodedEventBuffer>,
            mut decoder_registry: ResMut<DecoderRegistry>,
            mut metrics: ResMut<ConsumerMetrics>,
            config: Res<EventBusConsumerConfig>,
            maybe_queue: Option<Res<MessageQueue>>,
            mut drain_events: EventWriter<DrainMetricsEvent>,
        ) {
            let frame_start = std::time::Instant::now();
            metrics.drained_last_frame = 0;
            // Default queue length metrics when no queue
            metrics.queue_len_start = 0;
            metrics.queue_len_end = 0;

            let backend_ref = backend.as_ref();

            if let Some(queue) = maybe_queue {
                metrics.queue_len_start = queue.receiver.len();
                let limit = config.max_events_per_frame;
                let time_budget = config
                    .max_drain_millis
                    .map(std::time::Duration::from_millis);

                // Drain loop with multi-decoder pipeline
                while limit
                    .map(|l| metrics.drained_last_frame < l)
                    .unwrap_or(true)
                {
                    if let Some(budget) = time_budget {
                        if frame_start.elapsed() >= budget {
                            break;
                        }
                        // For extremely tiny budgets (<1ms) avoid looping excessively once something drained.
                        if budget <= std::time::Duration::from_millis(1)
                            && metrics.drained_last_frame > 0
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

                            let topic = source;
                            let topic_str = topic.as_str();
                            tracing::debug!(topic=%topic_str, "Processing message with multi-decoder pipeline");

                            let metadata =
                                EventMetadata::new(topic.clone(), timestamp, key, backend_metadata);

                            // Attempt multi-decode using registered decoders
                            let decoded_events = decoder_registry.decode_all(topic_str, &payload);

                            // Get or create topic buffer
                            let topic_buffer = decoded_buffer
                                .topics
                                .entry(topic.clone())
                                .or_insert_with(TopicDecodedEvents::new);
                            topic_buffer.total_processed += 1;

                            if decoded_events.is_empty() {
                                // No decoder succeeded - fire decode error event
                                topic_buffer.decode_failures += 1;
                                let decoder_count = decoder_registry.decoder_count(topic_str);
                                tracing::debug!(
                                    topic = %topic_str,
                                    decoders_tried = decoder_count,
                                    "No decoder succeeded for message"
                                );

                                // Generate decode error event
                                let decode_error = bevy_event_bus::EventBusDecodeError::new(
                                    metadata.source.clone(),
                                    format!(
                                        "No decoder succeeded. Tried {} decoders",
                                        decoder_count
                                    ),
                                    payload,
                                    format!("tried_{}_decoders", decoder_count),
                                    Some(metadata.clone()),
                                );

                                // Store decode error for event dispatch
                                metadata_buffers.decode_errors.push(decode_error);
                            } else {
                                // At least one decoder succeeded
                                for decoded_event in decoded_events {
                                    tracing::trace!(
                                        topic = %topic_str,
                                        decoder = %decoded_event.decoder_name,
                                        "Successfully decoded event"
                                    );

                                    // Store the decoded event in the type-erased buffer
                                    // The event Box contains the actual event, we need to store it properly
                                    let type_erased = crate::resources::TypeErasedEvent {
                                        event: decoded_event.event,
                                        metadata: metadata.clone(),
                                        decoder_name: decoded_event.decoder_name,
                                    };

                                    topic_buffer
                                        .events_by_type
                                        .entry(decoded_event.type_id)
                                        .or_insert_with(Vec::new)
                                        .push(type_erased);
                                }

                                // Also add the original message to DrainedTopicMetadata for BusEventReader compatibility
                                // This allows existing BusEventReader<T> to find the events by deserializing the original payload
                                let processed_msg = ProcessedMessage {
                                    payload,
                                    metadata: metadata.clone(),
                                };

                                metadata_buffers
                                    .topics
                                    .entry(topic.clone())
                                    .or_insert_with(Vec::new)
                                    .push(processed_msg);
                            }

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

            if let Some(backend_res) = backend_ref {
                let guard = backend_res.read();
                guard.augment_metrics(&mut metrics);
            }

            // Count idle frame if nothing drained
            if metrics.drained_last_frame == 0 {
                metrics.idle_frames += 1;
            }

            // Periodic cleanup of empty topic buffers
            if metrics.idle_frames % 30 == 0 && metrics.idle_frames > 0 {
                let before_count = metadata_buffers.topics.len();
                metadata_buffers
                    .topics
                    .retain(|_topic, buffer| !buffer.is_empty());
                let after_count = metadata_buffers.topics.len();

                if before_count > after_count {
                    tracing::debug!(
                        cleaned_topics = before_count - after_count,
                        remaining_topics = after_count,
                        "Cleaned up empty topic metadata buffers"
                    );
                }

                // Also clean up decoded event buffers
                let before_decoded = decoded_buffer.topics.len();
                decoded_buffer
                    .topics
                    .retain(|_topic, buffer| buffer.total_events() > 0);
                let after_decoded = decoded_buffer.topics.len();

                if before_decoded > after_decoded {
                    tracing::debug!(
                        cleaned_decoded_topics = before_decoded - after_decoded,
                        remaining_decoded_topics = after_decoded,
                        "Cleaned up empty decoded event buffers"
                    );
                }
            }

            metrics.drain_duration_us = frame_start.elapsed().as_micros();
            if metrics.drain_duration_us == 0
                && (metrics.drained_last_frame > 0 || metrics.queue_len_start > 0)
            {
                metrics.drain_duration_us = 1; // avoid zero-duration flake
            }

            // Emit metrics snapshot every frame for observability
            drain_events.write(DrainMetricsEvent {
                drained: metrics.drained_last_frame,
                remaining: metrics.remaining_channel_after_drain,
                total_drained: metrics.total_drained,
                dropped: metrics.dropped_messages,
                drain_duration_us: metrics.drain_duration_us,
            });
        }
        app.add_systems(PreUpdate, drain_system);
        app.add_systems(PreUpdate, decode_error_dispatch_system.after(drain_system));

        // Error queue flush system - runs in PostUpdate to ensure all BusEventWriter operations complete first
        fn error_queue_flush_system(world: &mut World) {
            // Extract the pending errors first
            let pending_errors = {
                let error_queue = world.resource::<EventBusErrorQueue>();
                error_queue.drain_pending()
            };

            // Then flush them
            for error_fn in pending_errors {
                error_fn(world);
            }
        }
        app.add_systems(PostUpdate, error_queue_flush_system);

        // Producer progress now handled entirely by backend background task; sender_system removed since sends are now direct.

        // Lifecycle drain system converts internal channel messages to Bevy events
        fn lifecycle_system(
            maybe_channel: Option<Res<BackendLifecycleChannel>>,
            mut ready_writer: EventWriter<BackendReadyEvent>,
        ) {
            if let Some(ch) = maybe_channel {
                while let Ok(msg) = ch.0.try_recv() {
                    match msg {
                        LifecycleMessage::Ready { backend, topics } => {
                            ready_writer.write(BackendReadyEvent {
                                backend: backend.clone(),
                                topics: topics.clone(),
                            });
                        }
                    }
                }
            }
        }
        app.add_systems(PreUpdate, lifecycle_system);

        // System to update BackendStatus from events
        fn backend_status_update(
            status: Option<ResMut<BackendStatus>>,
            mut ready_events: EventReader<BackendReadyEvent>,
        ) {
            if let Some(mut s) = status {
                for _ev in ready_events.read() {
                    s.ready = true;
                }
            }
        }
        app.add_systems(PreUpdate, backend_status_update);

        // Decode error dispatch system - sends EventBusDecodeError events
        fn decode_error_dispatch_system(
            mut drained_metadata: ResMut<DrainedTopicMetadata>,
            mut decode_error_writer: EventWriter<bevy_event_bus::EventBusDecodeError>,
        ) {
            // Dispatch all accumulated decode errors as events
            for decode_error in drained_metadata.decode_errors.drain(..) {
                decode_error_writer.write(decode_error);
            }
        }
    }
}
