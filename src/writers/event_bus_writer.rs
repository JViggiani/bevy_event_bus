use bevy::prelude::*;
use std::sync::Mutex;
use crate::{
    BusEvent, EventBusError,
    backends::EventBusBackendResource,
    config::EventBusConfig,
};

#[cfg(feature = "kafka")]
use crate::config::kafka::KafkaWriteConfig;

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

/// Writes events to both internal Bevy events and external message broker topics
/// 
/// All methods use "fire and forget" semantics - errors are sent as EventBusError<T> events
/// rather than returned as Results.
#[derive(bevy::ecs::system::SystemParam)]
pub struct EventBusWriter<'w, T: BusEvent + Event> {
    backend: Option<Res<'w, EventBusBackendResource>>,
    events: EventWriter<'w, T>,
    error_queue: Res<'w, EventBusErrorQueue>,
}

impl<'w, T: BusEvent + Event> EventBusWriter<'w, T> {
    /// Write an event using mandatory configuration
    /// 
    /// You must provide configuration that specifies which topics to write to.
    /// Headers and partition keys are now specified in the config object using builder patterns.
    /// Uses fire-and-forget semantics. Any errors are sent as EventBusError<T> events.
    pub fn write<C: EventBusConfig>(
        &mut self, 
        config: &C,
        event: T
    ) {
        for topic in config.topics() {
            // Clone event for potential error reporting
            let event_clone = event.clone();
            
            // Send to external backend using config
            if let Some(backend_res) = &self.backend {
                let backend = backend_res.read();
                if !(**backend).try_send(&event_clone, config) {
                    // Backend send failed - queue error event
                    let error_event = EventBusError::immediate(
                        topic.clone(),
                        crate::EventBusErrorType::Other,
                        "Failed to send to external backend".to_string(),
                        event_clone,
                    );
                    self.error_queue.add_error(error_event);
                    continue; // Try other topics even if one fails
                }
            }

            // Also send to internal Bevy events (headers and partition key are lost in internal events)
            self.events.write(event_clone);
        }
    }

    /// Write multiple events using mandatory configuration
    /// 
    /// All events are written as a batch operation to all configured topics. 
    /// Uses fire-and-forget semantics. If any event fails, an error event is 
    /// fired but processing continues.
    pub fn write_batch<C: EventBusConfig>(&mut self, config: &C, events: impl IntoIterator<Item = T>) {
        for event in events {
            self.write(config, event);
        }
    }

    /// Write the default value of the event using mandatory configuration
    /// 
    /// Uses fire-and-forget semantics. Any errors are sent as EventBusError<T> events.
    pub fn write_default<C: EventBusConfig>(&mut self, config: &C)
    where
        T: Default,
    {
        self.write(config, T::default())
    }
    

}

#[cfg(feature = "kafka")]
impl<'w, T: BusEvent + Event> EventBusWriter<'w, T> {    
    /// Flush all pending messages - Kafka specific
    /// 
    /// This is a BLOCKING operation that should only be called during shutdown
    /// or other situations where blocking is acceptable. Never call this from
    /// the Bevy update loop or any performance-critical path.
    /// 
    /// # Arguments
    /// * `_config` - Kafka write configuration (currently unused but kept for API consistency)
    /// * `timeout` - Maximum time to wait for all messages to be delivered 
    /// 
    /// # Returns
    /// * `Ok(())` if all messages were successfully flushed
    /// * `Err(String)` if timeout occurred or other error happened
    pub fn flush(&mut self, _config: &KafkaWriteConfig, timeout: std::time::Duration) -> Result<(), String> {
        if let Some(backend_res) = &self.backend {
            let backend = backend_res.read();
            if (**backend).flush(timeout) {
                Ok(())
            } else {
                Err(format!("Failed to flush Kafka producer within {:?}", timeout))
            }
        } else {
            tracing::warn!("No backend available for flush operation");
            Ok(())
        }
    }
}


