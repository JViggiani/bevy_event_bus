use bevy::prelude::*;
 

use crate::{
    backends::{EventBusBackend, EventBusBackendResource},
    runtime::{ensure_runtime, block_on},
    resources::{MessageQueue, DrainedTopicBuffers, EventBusConsumerConfig, ConsumerMetrics, DrainMetricsEvent},
    registration::EVENT_REGISTRY,
};

/// Pre-configured topics resource inserted at plugin construction.
#[derive(Resource, Debug, Clone)]
pub struct PreconfiguredTopics(pub Vec<String>);
impl PreconfiguredTopics {
    pub fn new<I: Into<String>, T: IntoIterator<Item=I>>(iter: T) -> Self { Self(iter.into_iter().map(Into::into).collect()) }
}


/// Plugin for integrating with external event brokers
pub struct EventBusPlugin;

impl Plugin for EventBusPlugin {
    fn build(&self, app: &mut App) {
    // Invoke registration callbacks (derive macro populated). We DO NOT drain so multiple Apps each see events.
    let guard = EVENT_REGISTRY.lock().unwrap();
    for cb in guard.iter() { cb(app); }
    }
}

/// Plugin bundle that configures everything needed for the event bus
pub struct EventBusPlugins<B: EventBusBackend>(pub B, pub PreconfiguredTopics);

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
    Ready { backend: String, topics: Vec<String> },
    Down { backend: String, reason: String },
}

#[derive(Resource)]
struct BackendLifecycleChannel(crossbeam_channel::Receiver<LifecycleMessage>);

// Resource tracking backend readiness state
#[derive(Resource, Debug, Clone, Copy)]
pub struct BackendStatus { pub ready: bool }



impl<B: EventBusBackend> Plugin for EventBusPlugins<B> {
    fn build(&self, app: &mut App) {
    // Add the core plugin and ensure runtime exists
    app.add_plugins(EventBusPlugin);
    ensure_runtime(app);
        
        // Create and add the backend as a resource
    let boxed = self.0.clone_box();
    app.insert_resource(EventBusBackendResource::from_box(boxed));
    app.insert_resource(self.1.clone());
        // Pre-create topics and subscribe BEFORE connecting so background consumer starts with full assignment.
        if let Some(pre) = app.world().get_resource::<PreconfiguredTopics>().cloned() {
            if let Some(backend_res) = app.world().get_resource::<EventBusBackendResource>().cloned() {
                let mut guard = backend_res.write();
                if let Some(kafka) = guard.as_any_mut().downcast_mut::<crate::backends::kafka_backend::KafkaEventBusBackend>() {
                    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
                    use rdkafka::client::DefaultClientContext;
                    use rdkafka::config::ClientConfig;
                    let mut cfg = ClientConfig::new();
                    cfg.set("bootstrap.servers", kafka.bootstrap_servers());
                    if let Ok(admin) = cfg.create::<AdminClient<DefaultClientContext>>() {
                        let opts = AdminOptions::new();
                        let topics: Vec<NewTopic> = pre.0.iter().map(|t| NewTopic::new(t, 1, TopicReplication::Fixed(1))).collect();
                        if let Err(e) = block_on(admin.create_topics(topics.iter().collect::<Vec<_>>(), &opts)) {
                            let msg = e.to_string();
                            if !msg.contains("TopicAlreadyExists") { tracing::warn!(error=%msg, "Topic creation batch failed"); }
                        }
                    }
                }
                for topic in pre.0.iter() {
                    if let Err(e) = block_on(guard.subscribe(topic)) { tracing::warn!(topic = %topic, err = ?e, "Failed to pre-subscribe to topic"); }
                }
            }
        }
        // Now connect (spawns background task with existing subscriptions) and capture receiver.
        if let Some(backend_res) = app.world().get_resource::<EventBusBackendResource>().cloned() {
            let mut guard = backend_res.write();
            let _ = block_on(guard.connect());
            if let Some(kafka) = guard.as_any_mut().downcast_mut::<crate::backends::kafka_backend::KafkaEventBusBackend>() {
                if let Some(rx) = kafka.take_receiver() {
                    app.world_mut().insert_resource(MessageQueue { receiver: rx });
                }
                // Inject lifecycle sender into kafka backend by wrapping its background task via a helper channel
                // (Since backend code owns spawning, we detect readiness here: receiver presence == ready)
                let (tx, rx_life) = crossbeam_channel::unbounded::<LifecycleMessage>();
                // Send Ready event now with current subscriptions
                let subs = kafka.current_subscriptions();
                let _ = tx.send(LifecycleMessage::Ready { backend: "kafka".into(), topics: subs });
                app.world_mut().insert_resource(BackendLifecycleChannel(rx_life));
                app.world_mut().insert_resource(BackendStatus { ready: false });
                // We cannot directly hook errors here; leave placeholder (Down events emitted by future backend error hook TBD)
            }
        }

    // Initialize background consumer related resources if not present
    app.init_resource::<DrainedTopicBuffers>();
        app.init_resource::<ConsumerMetrics>();
        app.init_resource::<EventBusConsumerConfig>();
    app.add_event::<DrainMetricsEvent>();
    app.add_event::<BackendReadyEvent>();
    app.add_event::<BackendDownEvent>();
        // MessageQueue will be inserted lazily once backend spawns sender & channel

        // Drain system
    fn drain_system(
            mut commands: Commands,
            backend: Option<Res<EventBusBackendResource>>,
            mut buffers: ResMut<DrainedTopicBuffers>,
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
            if let Some(queue) = maybe_queue {
                metrics.queue_len_start = queue.receiver.len();
                let limit = config.max_events_per_frame;
                let time_budget = config.max_drain_millis.map(std::time::Duration::from_millis);
                // Drain loop
                while limit.map(|l| metrics.drained_last_frame < l).unwrap_or(true) {
                    if let Some(budget) = time_budget {
                        if frame_start.elapsed() >= budget { break; }
                        // For extremely tiny budgets (<1ms) avoid looping excessively once something drained.
                        if budget <= std::time::Duration::from_millis(1) && metrics.drained_last_frame > 0 { break; }
                    }
                    match queue.receiver.try_recv() {
                        Ok(msg) => {
                            let tname = msg.topic.clone();
                            let entry = buffers.topics.entry(msg.topic).or_default();
                            tracing::debug!(topic=%tname, size_before=entry.len(), "Draining message into buffer");
                            entry.push(msg.payload);
                            metrics.drained_last_frame += 1;
                        }
                        Err(crossbeam_channel::TryRecvError::Empty) => break,
                        Err(crossbeam_channel::TryRecvError::Disconnected) => break,
                    }
                }
                metrics.remaining_channel_after_drain = queue.receiver.len();
                metrics.queue_len_end = metrics.remaining_channel_after_drain;
                metrics.total_drained += metrics.drained_last_frame;
            } else if let Some(backend_res) = backend {
                let mut guard = backend_res.write();
                if let Some(kafka) = guard.as_any_mut().downcast_mut::<crate::backends::kafka_backend::KafkaEventBusBackend>() {
                    let dropped = kafka.dropped_count();
                    metrics.dropped_messages = dropped;
                    if let Some(rx) = kafka.take_receiver() {
                        // Insert queue so next frame we can drain
                        commands.insert_resource(MessageQueue { receiver: rx });
                    }
                }
            }
            // Count idle frame if nothing drained (covers both queue-present and queue-absent cases)
            if metrics.drained_last_frame == 0 { metrics.idle_frames += 1; }
            metrics.drain_duration_us = frame_start.elapsed().as_micros();
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

        // Lifecycle drain system converts internal channel messages to Bevy events
        fn lifecycle_system(
            maybe_channel: Option<Res<BackendLifecycleChannel>>,
            mut ready_writer: EventWriter<BackendReadyEvent>,
            mut down_writer: EventWriter<BackendDownEvent>,
        ) {
            if let Some(ch) = maybe_channel {
                while let Ok(msg) = ch.0.try_recv() {
                    match msg {
                        LifecycleMessage::Ready { backend, topics } => {
                            ready_writer.write(BackendReadyEvent { backend: backend.clone(), topics: topics.clone() });
                        }
                        LifecycleMessage::Down { backend, reason } => {
                            down_writer.write(BackendDownEvent { backend, reason });
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
                for _ev in ready_events.read() { s.ready = true; }
            }
        }
        app.add_systems(PreUpdate, backend_status_update);
    }
}
