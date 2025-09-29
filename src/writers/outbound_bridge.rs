use std::any::TypeId;
use std::collections::{HashMap, HashSet};

use bevy::prelude::*;

use crate::BusEvent;
use crate::config::{EventBusConfig, InMemory};
use crate::writers::event_bus_writer::{EventBusErrorQueue, EventBusWriter};

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

#[derive(Clone)]
struct BridgeConfig {
    topics: Vec<String>,
    id: String,
}

impl BridgeConfig {
    fn new<T: BusEvent + Event>(topics: Vec<String>) -> Self {
        Self {
            topics,
            id: format!("bridge::{}", std::any::type_name::<T>()),
        }
    }
}

impl EventBusConfig for BridgeConfig {
    type Backend = InMemory;

    fn topics(&self) -> &[String] {
        &self.topics
    }

    fn config_id(&self) -> String {
        self.id.clone()
    }
}

fn outbound_bridge_system<T: BusEvent + Event>(
    mut reader: EventReader<T>,
    topic_registry: Option<Res<OutboundTopicRegistry>>,
    mut writer: EventBusWriter,
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

    let config = BridgeConfig::new::<T>(topics.to_vec());

    for event in reader.read() {
        writer.write(&config, event.clone());
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
