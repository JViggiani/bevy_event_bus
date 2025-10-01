use std::collections::HashMap;
use std::time::Duration;

use bevy::prelude::*;

use bevy_event_bus::backends::{EventBusBackend, EventBusBackendResource, KafkaEventBusBackend};
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

    /// Write an event with a partition key to preserve ordering semantics.
    pub fn write_with_key<T: BusEvent + Event>(
        &mut self,
        config: &KafkaProducerConfig,
        event: T,
        key: &str,
    ) {
        if let Some(backend_res) = self.backend.as_ref() {
            let backend = backend_res.read();
            for topic in config.topics() {
                if !(**backend).try_send_with_partition_key(&event, topic, key) {
                    let error_event = EventBusError::immediate(
                        topic.clone(),
                        EventBusErrorType::Other,
                        "Failed to send Kafka message with partition key".to_string(),
                        event.clone(),
                    );
                    self.error_queue.add_error(error_event);
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

    /// Write an event with custom headers using Kafka-specific delivery semantics.
    pub fn write_with_headers<T: BusEvent + Event, C: EventBusConfig>(
        &mut self,
        config: &C,
        event: T,
        headers: HashMap<String, String>,
    ) {
        self.write_with_headers_internal(config, event, headers, |backend, topic, evt, hdrs| {
            backend.try_send_with_headers(evt, topic, hdrs)
        });
    }

    /// Convenience helper that accepts borrowed header pairs, converting them into owned strings.
    pub fn write_with_headers_kafka<T: BusEvent + Event>(
        &mut self,
        config: &KafkaProducerConfig,
        event: T,
        headers: &[(&str, &str)],
    ) {
        let headers_map: HashMap<String, String> = headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        self.write_with_headers(config, event, headers_map);
    }

    /// Write an event with both partition key and headers.
    pub fn write_with_key_and_headers<T: BusEvent + Event>(
        &mut self,
        config: &KafkaProducerConfig,
        event: T,
        key: &str,
        headers: HashMap<String, String>,
    ) {
        self.write_with_headers_internal(config, event, headers, |backend, topic, evt, hdrs| {
            backend.try_send_with_key_and_headers(evt, topic, key, hdrs)
        });
    }

    /// Flush all pending messages in the Kafka producer.
    pub fn flush(&mut self, timeout: Duration) -> Result<(), KafkaWriterError> {
        let backend_res = self
            .backend
            .as_ref()
            .ok_or(KafkaWriterError::BackendUnavailable)?;
        let mut backend = backend_res.write();

        if let Some(kafka) = backend.as_any_mut().downcast_mut::<KafkaEventBusBackend>() {
            kafka
                .flush_with_timeout(timeout)
                .map_err(KafkaWriterError::backend)
        } else {
            runtime::block_on(backend.flush()).map_err(KafkaWriterError::backend)
        }
    }

    fn write_with_headers_internal<T: BusEvent + Event, C: EventBusConfig>(
        &mut self,
        config: &C,
        event: T,
        headers: HashMap<String, String>,
        send: impl Fn(&dyn EventBusBackend, &str, &T, &HashMap<String, String>) -> bool,
    ) {
        for topic in config.topics() {
            match &self.backend {
                Some(backend_res) => {
                    let backend_guard = backend_res.read();
                    let backend = &**backend_guard;
                    if !send(backend, topic, &event, &headers) {
                        let error_event = EventBusError::immediate(
                            topic.clone(),
                            EventBusErrorType::Other,
                            "Failed to send to external backend with headers".to_string(),
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
}

impl<'w, T> BusEventWriter<T> for KafkaEventWriter<'w>
where
    T: BusEvent + Event,
{
    fn write<C: EventBusConfig>(&mut self, config: &C, event: T) {
        for topic in config.topics() {
            match &self.backend {
                Some(backend_res) => {
                    let backend = backend_res.read();
                    if !(**backend).try_send(&event, topic) {
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
        C: EventBusConfig,
        I: IntoIterator<Item = T>,
    {
        let events: Vec<_> = events.into_iter().collect();

        for topic in config.topics() {
            match &self.backend {
                Some(backend_res) => {
                    let backend = backend_res.read();
                    for event in &events {
                        if !(**backend).try_send(event, topic) {
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
