use bevy::prelude::*;
 

use crate::{
    backends::{EventBusBackend, EventBusBackendResource},
    runtime::{ensure_runtime, block_on},
    resources::{MessageQueue, DrainedTopicBuffers, EventBusConsumerConfig, ConsumerMetrics},
    registration::EVENT_REGISTRY,
};


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
pub struct EventBusPlugins<B: EventBusBackend>(pub B);

impl<B: EventBusBackend> Plugin for EventBusPlugins<B> {
    fn build(&self, app: &mut App) {
    // Add the core plugin and ensure runtime exists
    app.add_plugins(EventBusPlugin);
    ensure_runtime(app);
        
        // Create and add the backend as a resource
        let boxed = self.0.clone_box();
        app.insert_resource(EventBusBackendResource::from_box(boxed));
        // Connect asynchronously (blocking for initial setup)
        if let Some(backend_res) = app.world().get_resource::<EventBusBackendResource>().cloned() {
            let mut guard = backend_res.write();
            let _ = block_on(guard.connect());
        }

        // Initialize background consumer related resources if not present
        app.init_resource::<DrainedTopicBuffers>();
        app.init_resource::<ConsumerMetrics>();
        app.init_resource::<EventBusConsumerConfig>();
        // MessageQueue will be inserted lazily once backend spawns sender & channel

        // Drain system
        fn drain_system(
            mut commands: Commands,
            backend: Option<Res<EventBusBackendResource>>,
            mut buffers: ResMut<DrainedTopicBuffers>,
            mut metrics: ResMut<ConsumerMetrics>,
            config: Res<EventBusConsumerConfig>,
            maybe_queue: Option<Res<MessageQueue>>,
        ) {
            let start = std::time::Instant::now();
            metrics.drained_last_frame = 0;
            if let Some(queue) = maybe_queue {
                let limit = config.max_events_per_frame;
                let time_budget = config.max_drain_millis.map(std::time::Duration::from_millis);
                // Drain loop
                while limit.map(|l| metrics.drained_last_frame < l).unwrap_or(true) {
                    if let Some(budget) = time_budget { if start.elapsed() >= budget { break; } }
                    match queue.receiver.try_recv() {
                        Ok(msg) => {
                            let entry = buffers.topics.entry(msg.topic).or_default();
                            entry.push(msg.payload);
                            metrics.drained_last_frame += 1;
                        }
                        Err(crossbeam_channel::TryRecvError::Empty) => break,
                        Err(crossbeam_channel::TryRecvError::Disconnected) => break,
                    }
                }
                metrics.remaining_channel_after_drain = queue.receiver.len();
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
        }
        app.add_systems(PreUpdate, drain_system);
    }
}
