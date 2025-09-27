use bevy::prelude::*;

use crate::{
    BusEvent,
    resources::EventWrapper,
    config::EventBusConfig,
};

#[cfg(feature = "kafka")]
use crate::config::kafka::KafkaReadConfig;

/// Iterator over events with optional metadata (unified internal + external events)
pub struct EventWrapperIterator<'a, T: BusEvent> {
    events: &'a [EventWrapper<T>],
    current: usize,
}

impl<'a, T: BusEvent> Iterator for EventWrapperIterator<'a, T> {
    type Item = &'a EventWrapper<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.events.len() {
            None
        } else {
            let event = &self.events[self.current];
            self.current += 1;
            Some(event)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.events.len() - self.current;
        (remaining, Some(remaining))
    }
}

/// Iterator over events received from the event bus
pub struct EventBusIterator<'a, T: BusEvent> {
    events: &'a [T],
    current: usize,
}

impl<'a, T: BusEvent> Iterator for EventBusIterator<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.events.len() {
            None
        } else {
            let event = &self.events[self.current];
            self.current += 1;
            Some(event)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.events.len() - self.current;
        (remaining, Some(remaining))
    }
}

/// Reads events from both internal Bevy events and external message broker topics
#[derive(bevy::ecs::system::SystemParam)]
pub struct EventBusReader<'w, 's, T: BusEvent + Event> {
    wrapped_events: Local<'s, Vec<EventWrapper<T>>>,
    events: EventReader<'w, 's, T>,
    backend: Option<Res<'w, crate::backends::EventBusBackendResource>>,
    runtime: Option<Res<'w, crate::runtime::SharedRuntime>>,
    error_queue: Option<Res<'w, crate::writers::event_bus_writer::EventBusErrorQueue>>,
    drained_metadata: Option<Res<'w, crate::resources::DrainedTopicMetadata>>,
}

impl<'w, 's, T: BusEvent + Event + Default> EventBusReader<'w, 's, T> {
    /// Read all events (internal and external) with mandatory configuration
    /// 
    /// This is the only API for reading events. You must provide configuration
    /// that specifies which topics to read from. External events from message 
    /// brokers include metadata, while internal Bevy events do not. 
    /// Use `.metadata()` to check if metadata is available.
    /// 
    /// Returns a vector of `EventWrapper<T>` which derefs to `T`.
    pub fn read<C: EventBusConfig>(&mut self, config: &C) -> Vec<EventWrapper<T>> {
        let mut all_events = Vec::new();
        
        for topic in config.topics() {
            // Clear wrapped events buffer first
            self.wrapped_events.clear();
            
            // For direct polling architecture: poll backend directly if available
            #[cfg(feature = "kafka")]  
            if let (Some(backend_res), Some(runtime_res)) = (&self.backend, &self.runtime) {
                tracing::debug!("EventBusReader: Starting direct polling for topic {}", topic);
                let backend = backend_res.read();
                let rt = &runtime_res.0;
                
                if let Some(kafka_config) = (config as &(dyn std::any::Any + Send + Sync)).downcast_ref::<crate::config::kafka::KafkaReadConfig>() {
                    // Cast backend to KafkaEventBusBackend to access Kafka-specific methods
                    if let Some(kafka_backend) = (**backend).as_any().downcast_ref::<crate::backends::KafkaEventBusBackend>() {
                        let mut wrapped_events = Vec::new();
                        match rt.block_on(kafka_backend.receive::<T>(kafka_config, &mut wrapped_events)) {
                            Ok(()) => {
                                tracing::debug!("EventBusReader: Direct polling returned {} events with metadata", wrapped_events.len());
                                
                                // Events are already wrapped with metadata, just add them
                                self.wrapped_events.extend(wrapped_events);
                            }
                            Err(e) => {
                                tracing::error!("EventBusReader: Direct polling failed: {}", e);
                                
                                // Emit error event using the error queue
                                if let Some(error_queue) = &self.error_queue {
                                    let error_event = crate::EventBusError::immediate(
                                        topic.clone(),
                                        crate::EventBusErrorType::ConsumerError,
                                        format!("Failed to read from topic '{}': {}", topic, e),
                                        T::default(), // Use default event as placeholder since we don't have original
                                    );
                                    error_queue.add_error(error_event);
                                }
                            }
                        }
                    } else {
                        tracing::error!("EventBusReader: Backend is not a Kafka backend");
                    }
                } else {
                    // This should not happen since we only support KafkaReadConfig now
                    tracing::error!("EventBusReader: Unsupported config type - only KafkaReadConfig is supported");
                    
                    if let Some(error_queue) = &self.error_queue {
                        let error_event = crate::EventBusError::immediate(
                            topic.clone(),
                            crate::EventBusErrorType::NotConfigured,
                            "Unsupported config type - only KafkaReadConfig is supported".to_string(),
                            T::default(),
                        );
                        error_queue.add_error(error_event);
                    }
                }
            }
            
            // Check drained metadata buffer for events from the drain system
            if let Some(drained_res) = &self.drained_metadata {
                if let Some(topic_messages) = drained_res.topics.get(topic) {
                    for processed_msg in topic_messages {
                        // Deserialize the event from the payload
                        match serde_json::from_slice::<T>(&processed_msg.payload) {
                            Ok(event) => {
                                self.wrapped_events.push(EventWrapper::new_external(event, processed_msg.metadata.clone()));
                            }
                            Err(e) => {
                                tracing::warn!("Failed to deserialize event from drained metadata for topic '{}': {}", topic, e);
                                
                                // Emit error event using the error queue
                                if let Some(error_queue) = &self.error_queue {
                                    let error_event = crate::EventBusError::immediate(
                                        topic.clone(),
                                        crate::EventBusErrorType::Deserialization,
                                        format!("Failed to deserialize event from drained metadata: {}", e),
                                        T::default(),
                                    );
                                    error_queue.add_error(error_event);
                                }
                            }
                        }
                    }
                }
            }

            // Add internal Bevy events (without metadata) - these don't have topic context
            // so we add them to every topic query (they are shared across all topics)
            if topic == config.topics().first().unwrap_or(&String::new()) {
                for event in self.events.read() {
                    self.wrapped_events.push(EventWrapper::new_internal(event.clone()));
                }
            }

            // Add events from this topic to the result
            for event_wrapper in &self.wrapped_events {
                all_events.push(event_wrapper.clone());
            }
        }
        
        all_events
    }

    /// Clear all event buffers and mark internal events as read
    pub fn clear(&mut self) {
        self.wrapped_events.clear();
        for _ in self.events.read() {}
    }

    /// Get the total number of events across all buffers
    pub fn len(&self) -> usize {
        self.events.len() + self.wrapped_events.len()
    }

    /// Check if all buffers are empty
    pub fn is_empty(&self) -> bool {
        self.events.is_empty() 
            && self.wrapped_events.is_empty()
    }
}

#[cfg(feature = "kafka")]
impl<'w, 's, T: BusEvent + Event + Default> EventBusReader<'w, 's, T> {

    
    /// Get consumer lag information for a Kafka configuration
    /// 
    /// Returns the lag for the configured consumer group across all topics.
    /// Delegates to the backend's lag calculation implementation.
    pub fn get_consumer_lag(&self, config: &KafkaReadConfig) -> Result<i64, String> {
        if let (Some(backend_res), Some(runtime_res)) = (&self.backend, &self.runtime) {
            let backend = backend_res.read();
            let rt = &runtime_res.0;
            
            if let Some(kafka_backend) = backend.as_any().downcast_ref::<crate::backends::kafka_backend::KafkaEventBusBackend>() {
                let group_id = config.consumer_group();
                let mut total_lag = 0i64;
                
                // Calculate lag for each topic in the configuration
                for topic in config.topics() {
                    match rt.block_on(kafka_backend.get_consumer_lag(topic, group_id)) {
                        Ok(lag) => total_lag += lag,
                        Err(e) => {
                            tracing::error!("Failed to get consumer lag for topic '{}': {}", topic, e);
                            return Err(format!("Lag calculation failed for topic '{}': {}", topic, e));
                        }
                    }
                }
                
                Ok(total_lag)
            } else {
                Err("Backend is not a Kafka backend".to_string())
            }
        } else {
            Err("No backend or runtime available for lag calculation".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    // implement any future unit tests here
}
