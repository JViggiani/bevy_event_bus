use std::any::Any;
use std::time::Duration;

use bevy::prelude::*;

use bevy_event_bus::backends::{
    EventBusBackendResource, KafkaEventBusBackend, event_bus_backend::SendOptions,
};
use bevy_event_bus::config::{EventBusConfig, kafka::KafkaProducerConfig};
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
}

impl<'w, T> BusEventWriter<T> for KafkaEventWriter<'w>
where
    T: BusEvent + Event,
{
    fn write<C>(&mut self, config: &C, event: T)
    where
        C: EventBusConfig + Any,
    {
        let mut base_options = SendOptions::default();
        if let Some(kafka_config) = (config as &dyn Any).downcast_ref::<KafkaProducerConfig>() {
            if let Some(key) = kafka_config.get_partition_key() {
                base_options = base_options.partition_key(key);
            }

            let headers = kafka_config.get_headers();
            if !headers.is_empty() {
                base_options = base_options.headers(headers);
            }
        }

        for topic in config.topics() {
            match &self.backend {
                Some(backend_res) => {
                    let backend = backend_res.read();
                    if !backend.try_send(&event, topic, base_options) {
                        let error_event = EventBusError::immediate(
                            topic.clone(),
                            EventBusErrorType::Other,
                            "Failed to send to external backend".to_string(),
                            event.clone(),
                        );
                        self.error_queue.add_error(error_event);
                    }
                }
                None => {
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

    fn write_batch<C, I>(&mut self, config: &C, events: I)
    where
        C: EventBusConfig + Any,
        I: IntoIterator<Item = T>,
    {
        let events: Vec<_> = events.into_iter().collect();
        let mut base_options = SendOptions::default();
        if let Some(kafka_config) = (config as &dyn Any).downcast_ref::<KafkaProducerConfig>() {
            if let Some(key) = kafka_config.get_partition_key() {
                base_options = base_options.partition_key(key);
            }

            let headers = kafka_config.get_headers();
            if !headers.is_empty() {
                base_options = base_options.headers(headers);
            }
        }

        for topic in config.topics() {
            match &self.backend {
                Some(backend_res) => {
                    let backend = backend_res.read();
                    for event in &events {
                        if !backend.try_send(event, topic, base_options) {
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
                None => {
                    for event in &events {
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
}
