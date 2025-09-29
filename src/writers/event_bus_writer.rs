use crate::{BusEvent, EventBusError, backends::EventBusBackendResource, config::EventBusConfig};
use bevy::prelude::*;
use std::{collections::HashMap, sync::Mutex};

#[cfg(feature = "kafka")]
use crate::config::kafka::KafkaProducerConfig;

/// Resource to collect error events that need to be sent
/// This avoids system parameter conflicts between EventBusWriter and EventReader<EventBusError<T>>
#[derive(Resource, Default)]
pub struct EventBusErrorQueue {
    pending_errors: Mutex<Vec<Box<dyn Fn(&mut World) + Send + Sync>>>,
}

impl EventBusErrorQueue {
    pub fn add_error<T: BusEvent + Event>(&self, error: EventBusError<T>) {
        if let Ok(mut pending) = self.pending_errors.lock() {
            pending.push(Box::new(move |world: &mut World| {
                world.send_event(error.clone());
            }));
        }
    }

    pub fn flush_errors(&self, world: &mut World) {
        if let Ok(mut pending) = self.pending_errors.lock() {
            for error_fn in pending.drain(..) {
                error_fn(world);
            }
        }
    }

    pub fn drain_pending(&self) -> Vec<Box<dyn Fn(&mut World) + Send + Sync>> {
        if let Ok(mut pending) = self.pending_errors.lock() {
            std::mem::take(&mut *pending)
        } else {
            Vec::new()
        }
    }
}

/// Writes events to external message broker topics
///
/// All methods use "fire and forget" semantics - errors are sent as EventBusError<T> events
/// rather than returned as Results.
#[derive(bevy::ecs::system::SystemParam)]
pub struct EventBusWriter<'w> {
    backend: Option<Res<'w, EventBusBackendResource>>,
    error_queue: Res<'w, EventBusErrorQueue>,
}

impl<'w> EventBusWriter<'w> {
    /// Write an event using mandatory configuration
    ///
    /// You must provide configuration that specifies which topics to write to.
    /// Uses fire-and-forget semantics. Any errors are sent as EventBusError<T> events.
    pub fn write<T: BusEvent + Event, C: EventBusConfig>(&mut self, config: &C, event: T) {
        for topic in config.topics() {
            match &self.backend {
                Some(backend_res) => {
                    let backend = backend_res.read();
                    if !(**backend).try_send(&event, topic) {
                        let error_event = EventBusError::immediate(
                            topic.clone(),
                            crate::EventBusErrorType::Other,
                            "Failed to send to external backend".to_string(),
                            event.clone(),
                        );
                        self.error_queue.add_error(error_event);
                    }
                }
                None => {
                    let error_event = EventBusError::immediate(
                        topic.clone(),
                        crate::EventBusErrorType::NotConfigured,
                        "No event bus backend configured".to_string(),
                        event.clone(),
                    );
                    self.error_queue.add_error(error_event);
                }
            }
        }
    }

    /// Write multiple events using mandatory configuration
    ///
    /// All events are written as a batch operation to all configured topics.
    /// Uses fire-and-forget semantics. If any event fails, an error event is
    /// fired but processing continues.
    pub fn write_batch<T: BusEvent + Event, C: EventBusConfig>(
        &mut self,
        config: &C,
        events: impl IntoIterator<Item = T>,
    ) {
        let events: Vec<_> = events.into_iter().collect();

        for topic in config.topics() {
            match &self.backend {
                Some(backend_res) => {
                    let backend = backend_res.read();
                    for event in &events {
                        if !(**backend).try_send(event, topic) {
                            let error_event = EventBusError::immediate(
                                topic.clone(),
                                crate::EventBusErrorType::Other,
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
                            crate::EventBusErrorType::NotConfigured,
                            "No event bus backend configured".to_string(),
                            event.clone(),
                        );
                        self.error_queue.add_error(error_event);
                    }
                }
            }
        }
    }

    /// Write an event with headers using mandatory configuration
    ///
    /// Headers are only sent to external brokers.
    /// Uses fire-and-forget semantics. Any errors are sent as EventBusError<T> events.
    pub fn write_with_headers<T: BusEvent + Event, C: EventBusConfig>(
        &mut self,
        config: &C,
        event: T,
        headers: HashMap<String, String>,
    ) {
        for topic in config.topics() {
            match &self.backend {
                Some(backend_res) => {
                    let backend = backend_res.read();
                    if !(**backend).try_send_with_headers(&event, topic, &headers) {
                        let error_event = EventBusError::immediate(
                            topic.clone(),
                            crate::EventBusErrorType::Other,
                            "Failed to send to external backend with headers".to_string(),
                            event.clone(),
                        );
                        self.error_queue.add_error(error_event);
                    }
                }
                None => {
                    let error_event = EventBusError::immediate(
                        topic.clone(),
                        crate::EventBusErrorType::NotConfigured,
                        "No event bus backend configured".to_string(),
                        event.clone(),
                    );
                    self.error_queue.add_error(error_event);
                }
            }
        }
    }

    /// Write the default value of the event using mandatory configuration
    ///
    /// Uses fire-and-forget semantics. Any errors are sent as EventBusError<T> events.
    pub fn write_default<T, C>(&mut self, config: &C)
    where
        T: BusEvent + Event + Default,
        C: EventBusConfig,
    {
        self.write::<T, C>(config, T::default())
    }
}

#[cfg(feature = "kafka")]
impl<'w> EventBusWriter<'w> {
    /// Write with partition key to ensure ordering - Kafka specific
    ///
    /// Uses the partition key to determine which partition the message goes to,
    /// ensuring ordering for messages with the same key.
    /// Requires a KafkaProducerConfig.
    pub fn write_with_key<T: BusEvent + Event>(
        &mut self,
        config: &KafkaProducerConfig,
        event: T,
        _key: &str,
    ) {
        // TODO: Implement actual Kafka partition key functionality through backend
        tracing::warn!(
            "Kafka partition key write not yet implemented - falling back to regular write"
        );
        self.write(config, event);
    }

    /// Write with custom headers - Kafka specific
    ///
    /// Kafka-specific method for writing with headers.
    /// Requires a KafkaProducerConfig.
    pub fn write_with_headers_kafka<T: BusEvent + Event>(
        &mut self,
        config: &KafkaProducerConfig,
        event: T,
        headers: &[(&str, &str)],
    ) {
        // Convert headers to HashMap
        let headers_map: HashMap<String, String> = headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        self.write_with_headers(config, event, headers_map);
    }

    /// Write with both partition key and headers - Kafka specific
    ///
    /// Kafka-specific method for writing with both partition key and headers.
    /// Requires a KafkaProducerConfig.
    pub fn write_with_key_and_headers<T: BusEvent + Event>(
        &mut self,
        config: &KafkaProducerConfig,
        event: T,
        _key: &str,
        headers: &[(&str, &str)],
    ) {
        // TODO: Implement actual Kafka key + headers functionality
        tracing::warn!(
            "Kafka key + headers write not yet implemented - falling back to headers only"
        );
        self.write_with_headers_kafka(config, event, headers);
    }

    /// Flush all pending messages - Kafka specific
    ///
    /// Kafka-specific method for flushing the producer.
    /// Requires a KafkaProducerConfig.
    pub fn flush(&mut self, _config: &KafkaProducerConfig) -> Result<(), String> {
        // TODO: Implement actual Kafka producer flush
        tracing::warn!("Kafka producer flush not yet implemented");
        Ok(())
    }
}
