use std::any::TypeId;
use std::collections::{HashMap, HashSet};

use bevy::prelude::*;

use bevy_event_bus::backends::{EventBusBackendResource, event_bus_backend::SendOptions};
use bevy_event_bus::writers::EventBusErrorQueue;
use bevy_event_bus::{BusEvent, EventBusError, EventBusErrorType};

/// Tracks event types that need an outbound bridge and the systems used to service them.
#[derive(Resource, Default)]
pub struct OutboundBridgeRegistry {
    callbacks: HashMap<TypeId, fn(&mut App)>,
    activated: HashSet<TypeId>,
}

impl OutboundBridgeRegistry {
    pub fn register_type<T: BusEvent + Event>(&mut self) {
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

fn outbound_bridge_system<T: BusEvent + Event>(
    mut reader: EventReader<T>,
    topic_registry: Option<Res<OutboundTopicRegistry>>,
    backend: Option<Res<EventBusBackendResource>>,
    error_queue: Res<EventBusErrorQueue>,
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

    for event in reader.read() {
        match backend.as_ref() {
            Some(backend_res) => {
                let backend_guard = backend_res.read();
                let backend = &**backend_guard;

                for topic in topics {
                    if !backend.try_send(event, topic, SendOptions::default()) {
                        let error_event = EventBusError::immediate(
                            topic.clone(),
                            EventBusErrorType::Other,
                            "Failed to send to external backend".to_string(),
                            event.clone(),
                        );
                        error_queue.add_error(error_event);
                    }
                }
            }
            None => {
                for topic in topics {
                    let error_event = EventBusError::immediate(
                        topic.clone(),
                        EventBusErrorType::NotConfigured,
                        "No event bus backend configured".to_string(),
                        event.clone(),
                    );
                    error_queue.add_error(error_event);
                }
            }
        }
    }
}

fn add_bridge<T: BusEvent + Event>(app: &mut App) {
    app.add_systems(PostUpdate, outbound_bridge_system::<T>);
}

/// Ensure the outbound bridge is configured for the given event type and topics.
pub fn ensure_bridge<T: BusEvent + Event>(app: &mut App, topics: &[String]) {
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
    if !app.world().contains_resource::<EventBusErrorQueue>() {
        return;
    }

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
