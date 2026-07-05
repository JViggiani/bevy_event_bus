use bevy::prelude::*;

use crate::backends::{EventBusBackend, EventBusBackendResource};
use crate::decoder::DecoderRegistry;
use crate::plugins::event_bus::messages::{BackendReadyMessage, DrainMetricsMessage};
use crate::plugins::event_bus::resources::{
    BackendCapabilities, BackendReadyInfo, BackendStatus, ConsumerMetrics, DrainedTopicMetadata,
    EventBusConsumerConfig, ProvisionedTopology,
};
use crate::plugins::event_bus::systems::{drain_incoming_messages, emit_backend_ready};
use crate::runtime::{block_on, ensure_runtime};
use crate::writers::outbound_bridge::{
    OutboundBridgeRegistry, OutboundTopicRegistry, activate_registered_bridges,
};

/// Initialize every resource and message the event bus plugin owns.
pub(crate) fn register_plugin_world(app: &mut App) {
    app.init_resource::<ProvisionedTopology>();
    app.init_resource::<DrainedTopicMetadata>();
    app.init_resource::<DecoderRegistry>();
    app.init_resource::<ConsumerMetrics>();
    app.init_resource::<EventBusConsumerConfig>();
    app.init_resource::<BackendStatus>();
    app.init_resource::<OutboundBridgeRegistry>();
    app.init_resource::<OutboundTopicRegistry>();

    app.add_message::<DrainMetricsMessage>();
    app.add_message::<BackendReadyMessage>();

    app.add_systems(PreUpdate, drain_incoming_messages);
    app.add_systems(Startup, emit_backend_ready);
}

pub(crate) fn register_backend<B: EventBusBackend>(app: &mut App, backend: &B) {
    backend.configure_plugin(app);
    app.insert_resource(EventBusBackendResource::from_box(backend.clone_box()));
    backend.apply_event_bindings(app);
    activate_registered_bridges(app);
}

pub(crate) fn connect_and_install_backend(app: &mut App) {
    let backend_res = app
        .world()
        .get_resource::<EventBusBackendResource>()
        .expect("EventBusBackendResource must be registered before connect")
        .clone();

    let (backend_name, ready_topics, mut setup) = {
        let mut backend = backend_res.write();
        let _ = block_on(backend.connect());
        let backend_name = backend.backend_name().to_string();
        let setup = backend.setup_plugin(app.world_mut());
        let ready_topics = setup.ready_topics.clone();
        (backend_name, ready_topics, setup)
    };

    let capabilities =
        BackendCapabilities::from_install(&backend_name, setup.install(app.world_mut()));

    app.insert_resource(capabilities);
    app.insert_resource(BackendReadyInfo {
        backend: backend_name,
        topics: ready_topics,
    });
}

pub(crate) fn ensure_app_runtime(app: &mut App) {
    ensure_runtime(app);
}
