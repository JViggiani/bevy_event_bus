use std::{any::Any, time::Duration};

use bevy::prelude::*;

use bevy_event_bus::backends::{
    EventBusBackendResource, KafkaEventBusBackend,
    event_bus_backend::{BackendSpecificSendOptions, SendOptions},
};
use bevy_event_bus::config::{EventBusConfig, kafka::KafkaProducerConfig};
use bevy_event_bus::resources::ProvisionedTopology;
use bevy_event_bus::{BusEvent, EventBusError, EventBusErrorType, runtime};

use super::{BusEventWriter, EventBusErrorQueue};

/// Errors emitted by the Kafka-specific writer when backend operations fail.
#[derive(Debug)]
pub enum KafkaWriterError {
    /// No backend resource was found in the Bevy world.
    BackendUnavailable,
    /// Backend responded with an error message.
    BackendFailure(String),
}

impl KafkaWriterError {
    fn backend(err: String) -> Self {
        Self::BackendFailure(err)
    }
}

impl std::fmt::Display for KafkaWriterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KafkaWriterError::BackendUnavailable => {
                write!(f, "Kafka backend resource is not available in the world")
            }
            KafkaWriterError::BackendFailure(msg) => write!(f, "backend failure: {msg}"),
        }
    }
}

impl std::error::Error for KafkaWriterError {}

/// Kafka-specific writer that extends the generic writer with partition keys, headers and flush support.
#[derive(bevy::ecs::system::SystemParam)]
pub struct KafkaEventWriter<'w> {
    backend: Option<Res<'w, EventBusBackendResource>>,
    error_queue: Res<'w, EventBusErrorQueue>,
    topology: Option<Res<'w, ProvisionedTopology>>,
}

impl<'w> KafkaEventWriter<'w> {
    /// Write an event using the standard generic pipeline.
    pub fn write<T: BusEvent + Event>(&mut self, config: &KafkaProducerConfig, event: T) {
        <Self as BusEventWriter<T>>::write(self, config, event);
    }

    /// Flush all pending messages in the Kafka producer.
    pub fn flush(&mut self, timeout: Duration) -> Result<(), KafkaWriterError> {
        let backend_res = self
            .backend
            .as_ref()
            .ok_or(KafkaWriterError::BackendUnavailable)?;
        let mut backend = backend_res.write();

        if let Some(kafka) = backend.as_any_mut().downcast_mut::<KafkaEventBusBackend>() {
            kafka.flush(timeout).map_err(KafkaWriterError::backend)
        } else {
            runtime::block_on(backend.flush()).map_err(KafkaWriterError::backend)
        }
    }

    fn invalid_topics(&self, config: &KafkaProducerConfig) -> Option<Vec<String>> {
        let topology = self
            .topology
            .as_ref()
            .and_then(|registry| registry.kafka())?;
        let provisioned = topology.topic_names();

        let invalid: Vec<String> = config
            .topics()
            .iter()
            .filter(|topic| !provisioned.contains(*topic))
            .cloned()
            .collect();

        if invalid.is_empty() {
            None
        } else {
            Some(invalid)
        }
    }

    fn report_invalid_topics<T: BusEvent + Event>(&self, topics: &[String], event: &T) {
        for topic in topics {
            let reason = format!("Topic '{}' is not provisioned in the Kafka topology", topic);
            let error_event = EventBusError::immediate(
                topic.clone(),
                EventBusErrorType::InvalidWriteConfig,
                reason.clone(),
                event.clone(),
            );
            self.error_queue.add_error(error_event);
            tracing::warn!(
                backend = "kafka",
                topic = %topic,
                reason = %reason,
                "Kafka writer configuration invalid"
            );
        }
    }

    fn resolve_send_options<'a, C>(config: &'a C) -> SendOptions<'a>
    where
        C: EventBusConfig + Any,
    {
        let mut options = SendOptions::default();
        if let Some(kafka_config) = (config as &dyn Any).downcast_ref::<KafkaProducerConfig>() {
            if let Some(key) = kafka_config.get_partition_key() {
                options = options.partition_key(key);
            }

            let configured_headers = kafka_config.get_headers();
            if !configured_headers.is_empty() {
                let backend_options =
                    BackendSpecificSendOptions::new(configured_headers as &dyn Any);
                options = options.backend_options(backend_options);
            }
        }

        options
    }
}

impl<'w, T> BusEventWriter<T> for KafkaEventWriter<'w>
where
    T: BusEvent + Event,
{
    fn write<C>(&mut self, config: &C, event: T)
    where
        C: EventBusConfig + Any,
    {
        if let Some(kafka_config) = (config as &dyn Any).downcast_ref::<KafkaProducerConfig>() {
            if let Some(invalid_topics) = self.invalid_topics(kafka_config) {
                self.report_invalid_topics(&invalid_topics, &event);
                return;
            }
        }

        let options = Self::resolve_send_options(config);

        if let Some(backend_res) = &self.backend {
            let backend = backend_res.read();
            match serde_json::to_vec(&event) {
                Ok(serialized) => {
                    for topic in config.topics() {
                        if !backend.try_send_serialized(&serialized, topic, options) {
                            let error_event = EventBusError::immediate(
                                topic.clone(),
                                EventBusErrorType::Other,
                                "Failed to send to external backend".to_string(),
                                event.clone(),
                            );
                            self.error_queue.add_error(error_event);
                        }
                    }
                }
                Err(err) => {
                    for topic in config.topics() {
                        let error_event = EventBusError::immediate(
                            topic.clone(),
                            EventBusErrorType::Serialization,
                            err.to_string(),
                            event.clone(),
                        );
                        self.error_queue.add_error(error_event);
                    }
                }
            }
        } else {
            for topic in config.topics() {
                let error_event = EventBusError::immediate(
                    topic.clone(),
                    EventBusErrorType::NotConfigured,
                    "No event bus backend configured".to_string(),
                    event.clone(),
                );
                self.error_queue.add_error(error_event);
            }
        }
    }

    fn write_batch<C, I>(&mut self, config: &C, events: I)
    where
        C: EventBusConfig + Any,
        I: IntoIterator<Item = T>,
    {
        if let Some(kafka_config) = (config as &dyn Any).downcast_ref::<KafkaProducerConfig>() {
            if let Some(invalid_topics) = self.invalid_topics(kafka_config) {
                for event in events.into_iter() {
                    self.report_invalid_topics(&invalid_topics, &event);
                }
                return;
            }
        }

        let options = Self::resolve_send_options(config);

        if let Some(backend_res) = &self.backend {
            let backend = backend_res.read();
            for event in events.into_iter() {
                match serde_json::to_vec(&event) {
                    Ok(serialized) => {
                        for topic in config.topics() {
                            if !backend.try_send_serialized(&serialized, topic, options) {
                                let error_event = EventBusError::immediate(
                                    topic.clone(),
                                    EventBusErrorType::Other,
                                    "Failed to send to external backend".to_string(),
                                    event.clone(),
                                );
                                self.error_queue.add_error(error_event);
                            }
                        }
                    }
                    Err(err) => {
                        for topic in config.topics() {
                            let error_event = EventBusError::immediate(
                                topic.clone(),
                                EventBusErrorType::Serialization,
                                err.to_string(),
                                event.clone(),
                            );
                            self.error_queue.add_error(error_event);
                        }
                    }
                }
            }
        } else {
            for event in events.into_iter() {
                for topic in config.topics() {
                    let error_event = EventBusError::immediate(
                        topic.clone(),
                        EventBusErrorType::NotConfigured,
                        "No event bus backend configured".to_string(),
                        event.clone(),
                    );
                    self.error_queue.add_error(error_event);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bevy::ecs::event::EventReader;
    use bevy::ecs::system::SystemState;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, Event)]
    struct TestEvent {
        value: u32,
    }

    #[test]
    fn invalid_topic_enqueues_error() {
        let mut world = World::default();
        world.insert_resource(EventBusErrorQueue::default());
        world.insert_resource(Events::<EventBusError<TestEvent>>::default());

        let mut topology = ProvisionedTopology::default();
        let mut builder = bevy_event_bus::config::kafka::KafkaTopologyConfig::builder();
        builder.add_topic(bevy_event_bus::config::kafka::KafkaTopicSpec::new("known"));
        topology.record_kafka(builder.build());
        world.insert_resource(topology);

        let mut state = SystemState::<KafkaEventWriter>::new(&mut world);
        {
            let mut writer = state.get_mut(&mut world);
            let config = KafkaProducerConfig::new(["unknown"]);
            let event = TestEvent { value: 42 };
            writer.write(&config, event);
        }
        state.apply(&mut world);

        let pending = {
            let queue = world.resource::<EventBusErrorQueue>();
            queue.drain_pending()
        };
        assert_eq!(pending.len(), 1);
        for job in pending {
            job(&mut world);
        }

        {
            if let Some(mut events) = world.get_resource_mut::<Events<EventBusError<TestEvent>>>() {
                events.update();
            }
        }

        let mut reader_state =
            SystemState::<EventReader<EventBusError<TestEvent>>>::new(&mut world);
        let mut event_reader = reader_state.get_mut(&mut world);
        let collected: Vec<_> = event_reader.read().cloned().collect();
        reader_state.apply(&mut world);

        assert_eq!(collected.len(), 1);
        let error = &collected[0];
        assert_eq!(error.topic, "unknown");
        assert_eq!(error.error_type, EventBusErrorType::InvalidWriteConfig);
        assert!(error.error_message.contains("not provisioned"));
    }
}
