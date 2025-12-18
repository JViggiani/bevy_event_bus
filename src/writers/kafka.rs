use std::{any::Any, sync::Arc, time::Duration};

use bevy::prelude::*;

use bevy_event_bus::backends::{
    EventBusBackendResource, KafkaEventBusBackend,
    event_bus_backend::{BackendSpecificSendOptions, DeliveryFailureCallback, SendOptions},
};
use bevy_event_bus::config::{EventBusConfig, kafka::KafkaProducerConfig};
use bevy_event_bus::errors::{BusErrorCallback, BusErrorContext, BusErrorKind};
use bevy_event_bus::resources::{MessageMetadata, ProvisionedTopology};
use bevy_event_bus::{BusEvent, runtime};

use super::BusMessageWriter;

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

/// Kafka-specific writer that extends the generic message writer with partition keys, headers and flush support.
#[derive(bevy::ecs::system::SystemParam)]
pub struct KafkaMessageWriter<'w> {
    backend: Option<Res<'w, EventBusBackendResource>>,
    topology: Option<Res<'w, ProvisionedTopology>>,
}

impl<'w> KafkaMessageWriter<'w> {
    /// Write a message with an optional error callback.
    pub fn write<T, C>(&mut self, config: &C, event: T, callback: Option<BusErrorCallback>)
    where
        T: BusEvent + Message,
        C: EventBusConfig + Any,
    {
        let callback_ref = callback.as_ref();

        if let Some(kafka_config) = (config as &dyn Any).downcast_ref::<KafkaProducerConfig>() {
            if let Some(invalid_topics) = self.invalid_topics(kafka_config) {
                for topic in invalid_topics {
                    Self::emit_error(
                        callback_ref,
                        "kafka",
                        &topic,
                        BusErrorKind::InvalidWriteConfig,
                        format!("Topic '{topic}' is not provisioned in the Kafka topology"),
                        None,
                        None,
                    );
                }
                return;
            }
        }

        let options = Self::resolve_send_options(config);

        if let Some(backend_res) = &self.backend {
            let backend = backend_res.read();
            match serde_json::to_vec(&event) {
                Ok(serialized) => {
                    for topic in config.topics() {
                        let delivery_cb = callback.clone().map(|cb| {
                            let topic_name = topic.clone();
                            let payload = serialized.clone();
                            Arc::new(DeliveryFailureCallback::new(move |failure| {
                                cb(BusErrorContext::new(
                                    failure.backend,
                                    topic_name.clone(),
                                    failure.kind.clone(),
                                    failure.error.clone(),
                                    failure.metadata.clone(),
                                    Some(payload.clone()),
                                ));
                            })) as Arc<DeliveryFailureCallback>
                        });

                        if !backend.try_send_serialized(&serialized, topic, options, delivery_cb) {
                            Self::emit_error(
                                callback_ref,
                                "kafka",
                                topic,
                                BusErrorKind::DeliveryFailure,
                                "Failed to enqueue Kafka message",
                                None,
                                Some(serialized.clone()),
                            );
                        }
                    }
                }
                Err(err) => {
                    for topic in config.topics() {
                        Self::emit_error(
                            callback_ref,
                            "kafka",
                            topic,
                            BusErrorKind::Serialization,
                            err.to_string(),
                            None,
                            None,
                        );
                    }
                }
            }
        } else {
            for topic in config.topics() {
                Self::emit_error(
                    callback_ref,
                    "kafka",
                    topic,
                    BusErrorKind::NotConfigured,
                    "No event bus backend configured",
                    None,
                    None,
                );
            }
        }
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

    fn emit_error(
        callback: Option<&BusErrorCallback>,
        backend: &'static str,
        topic: &str,
        kind: BusErrorKind,
        message: impl Into<String>,
        metadata: Option<MessageMetadata>,
        payload: Option<Vec<u8>>,
    ) {
        let message = message.into();
        if let Some(cb) = callback {
            cb(BusErrorContext::new(
                backend,
                topic.to_string(),
                kind,
                message.clone(),
                metadata,
                payload,
            ));
        } else {
            bevy::log::warn!(backend = backend, topic = %topic, error = ?kind, "Unhandled writer error: {message}");
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

impl<'w, T> BusMessageWriter<T> for KafkaMessageWriter<'w>
where
    T: BusEvent + Message,
{
    fn write<C>(&mut self, config: &C, event: T, error_callback: Option<BusErrorCallback>)
    where
        C: EventBusConfig + Any,
    {
        KafkaMessageWriter::write(self, config, event, error_callback);
    }

    fn write_batch<C, I>(&mut self, config: &C, events: I, error_callback: Option<BusErrorCallback>)
    where
        C: EventBusConfig + Any,
        I: IntoIterator<Item = T>,
    {
        for event in events.into_iter() {
            KafkaMessageWriter::write(self, config, event, error_callback.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bevy::ecs::system::SystemState;
    use serde::{Deserialize, Serialize};
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Clone, Serialize, Deserialize, Message)]
    struct TestEvent {
        value: u32,
    }

    #[test]
    fn invalid_topic_enqueues_error() {
        let mut world = World::default();
        let mut topology = ProvisionedTopology::default();
        let mut builder = bevy_event_bus::config::kafka::KafkaTopologyConfig::builder();
        builder.add_topic(bevy_event_bus::config::kafka::KafkaTopicSpec::new("known"));
        topology.record_kafka(builder.build());
        world.insert_resource(topology);

        let errors: Arc<Mutex<Vec<BusErrorContext>>> = Arc::new(Mutex::new(Vec::new()));
        let callback: BusErrorCallback = {
            let store: Arc<Mutex<Vec<BusErrorContext>>> = Arc::clone(&errors);
            Arc::new(move |ctx| {
                store.lock().unwrap().push(ctx);
            })
        };

        let mut state = SystemState::<KafkaMessageWriter>::new(&mut world);
        {
            let mut writer = state.get_mut(&mut world);
            let config = KafkaProducerConfig::new(["unknown"]);
            let event = TestEvent { value: 42 };
            writer.write(&config, event, Some(callback.clone()));
        }
        state.apply(&mut world);

        let collected = errors.lock().unwrap().clone();
        assert_eq!(collected.len(), 1);
        let error = &collected[0];
        assert_eq!(error.topic, "unknown");
        assert_eq!(error.kind, BusErrorKind::InvalidWriteConfig);
        assert!(error.message.contains("not provisioned"));
    }
}
