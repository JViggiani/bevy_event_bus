use std::any::TypeId;
use std::collections::{HashMap, HashSet};

use bevy::prelude::*;

use bevy_event_bus::backends::{EventBusBackendResource, event_bus_backend::SendOptions};
use bevy_event_bus::errors::{BusErrorCallback, BusErrorContext, BusErrorKind};
use bevy_event_bus::resources::MessageMetadata;
use bevy_event_bus::BusEvent;

/// Optional error callback used by the outbound bridge.
#[derive(Resource, Clone)]
pub struct OutboundErrorCallback(pub BusErrorCallback);

/// Tracks event types that need an outbound bridge and the systems used to service them.
#[derive(Resource, Default)]
pub struct OutboundBridgeRegistry {
    callbacks: HashMap<TypeId, fn(&mut App)>,
    activated: HashSet<TypeId>,
}

impl OutboundBridgeRegistry {
    pub fn register_type<T: BusEvent + Message>(&mut self) {
        let ty = TypeId::of::<T>();
        self.callbacks.entry(ty).or_insert(add_bridge::<T>);
    }

    fn pending_callbacks(&mut self) -> Vec<fn(&mut App)> {
        let mut pending = Vec::new();
        for (&ty, callback) in self.callbacks.iter() {
            if self.activated.insert(ty) {
                pending.push(*callback);
            }
        }
        pending
    }
}

/// Stores topic assignments for each bus event type so the bridge knows where to publish.
#[derive(Resource, Default)]
pub struct OutboundTopicRegistry {
    topics: HashMap<TypeId, Vec<String>>,
}

impl OutboundTopicRegistry {
    pub fn register_topics<T: 'static>(&mut self, topics: &[String]) {
        let ty = TypeId::of::<T>();
        let entry = self.topics.entry(ty).or_default();
        for topic in topics {
            if !entry.iter().any(|existing| existing == topic) {
                entry.push(topic.clone());
            }
        }
    }

    pub fn topics_for<T: 'static>(&self) -> Option<&[String]> {
        self.topics.get(&TypeId::of::<T>()).map(|v| v.as_slice())
    }
}

fn outbound_bridge_system<T: BusEvent + Message>(
    mut reader: MessageReader<T>,
    topic_registry: Option<Res<OutboundTopicRegistry>>,
    backend: Option<Res<EventBusBackendResource>>,
    error_callback: Option<Res<OutboundErrorCallback>>,
) {
    let Some(registry) = topic_registry else {
        return;
    };

    let Some(topics) = registry.topics_for::<T>() else {
        return;
    };

    if topics.is_empty() {
        return;
    }

    let callback_ref = error_callback.as_ref().map(|cb| &cb.0);

    let emit_error = |topic: &str, kind: BusErrorKind, message: &str, metadata: Option<MessageMetadata>| {
        if let Some(cb) = callback_ref {
            cb(BusErrorContext::new(
                "outbound",
                topic.to_string(),
                kind,
                message.to_string(),
                metadata,
                None,
            ));
        } else {
            tracing::warn!(backend = "outbound", topic = %topic, error = ?kind, "Unhandled outbound bridge error: {message}");
        }
    };

    for event in reader.read() {
        match backend.as_ref() {
            Some(backend_res) => {
                let backend_guard = backend_res.read();
                let backend = &**backend_guard;

                for topic in topics {
                    if !backend.try_send(event, topic, SendOptions::default()) {
                        emit_error(topic, BusErrorKind::DeliveryFailure, "Failed to send to external backend", None);
                    }
                }
            }
            None => {
                for topic in topics {
                    emit_error(topic, BusErrorKind::NotConfigured, "No event bus backend configured", None);
                }
            }
        }
    }
}

fn add_bridge<T: BusEvent + Message>(app: &mut App) {
    app.add_systems(PostUpdate, outbound_bridge_system::<T>);
}

/// Ensure the outbound bridge is configured for the given event type and topics.
pub fn ensure_bridge<T: BusEvent + Message>(app: &mut App, topics: &[String]) {
    {
        let world = app.world_mut();

        let mut topic_registry = world
            .get_resource_or_insert_with::<OutboundTopicRegistry>(OutboundTopicRegistry::default);
        topic_registry.register_topics::<T>(topics);

        let mut bridge_registry = world
            .get_resource_or_insert_with::<OutboundBridgeRegistry>(OutboundBridgeRegistry::default);
        bridge_registry.register_type::<T>();
    }

    activate_registered_bridges(app);
}

/// Activate any bridge systems whose supporting resources are available.
pub fn activate_registered_bridges(app: &mut App) {
    let callbacks = {
        let world = app.world_mut();
        if let Some(mut registry) = world.get_resource_mut::<OutboundBridgeRegistry>() {
            registry.pending_callbacks()
        } else {
            Vec::new()
        }
    };

    for callback in callbacks {
        callback(app);
    }
}
