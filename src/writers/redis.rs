#![cfg(feature = "redis")]

use std::any::Any;

use bevy::prelude::*;

use bevy_event_bus::backends::{
    EventBusBackendResource, RedisEventBusBackend,
    event_bus_backend::{EventBusBackend, SendOptions, StreamTrimStrategy},
};
use bevy_event_bus::config::{EventBusConfig, redis::RedisProducerConfig, redis::TrimStrategy};
use bevy_event_bus::{BusEvent, EventBusError, EventBusErrorType, runtime};

use super::{BusEventWriter, EventBusErrorQueue};

/// Errors emitted by the Redis-specific writer when backend operations fail.
#[derive(Debug)]
pub enum RedisWriterError {
    /// No backend resource was found in the Bevy world.
    BackendUnavailable,
    /// Backend responded with an error message.
    BackendFailure(String),
}

impl RedisWriterError {
    fn backend(err: String) -> Self {
        Self::BackendFailure(err)
    }
}

impl std::fmt::Display for RedisWriterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisWriterError::BackendUnavailable => {
                write!(f, "Redis backend resource is not available in the world")
            }
            RedisWriterError::BackendFailure(msg) => write!(f, "backend failure: {msg}"),
        }
    }
}

impl std::error::Error for RedisWriterError {}

/// Redis-specific writer that provides stream trimming and configuration-driven options.
#[derive(bevy::ecs::system::SystemParam)]
pub struct RedisEventWriter<'w> {
    backend: Option<Res<'w, EventBusBackendResource>>,
    error_queue: Res<'w, EventBusErrorQueue>,
}

impl<'w> RedisEventWriter<'w> {
    /// Write an event using the standard generic pipeline.
    pub fn write<T: BusEvent + Event>(&mut self, config: &RedisProducerConfig, event: T) {
        <Self as BusEventWriter<T>>::write(self, config, event);
    }

    /// Schedule a Redis stream trim using the backend's asynchronous worker.
    pub fn trim_stream(
        &mut self,
        stream: &str,
        maxlen: usize,
        strategy: TrimStrategy,
    ) -> Result<(), RedisWriterError> {
        let backend_res = self
            .backend
            .as_ref()
            .ok_or(RedisWriterError::BackendUnavailable)?;
        let mut backend = backend_res.write();

        if let Some(redis) = backend.as_any_mut().downcast_mut::<RedisEventBusBackend>() {
            redis
                .trim_stream(stream, maxlen, strategy)
                .map_err(RedisWriterError::backend)
        } else {
            Err(RedisWriterError::BackendUnavailable)
        }
    }

    fn resolve_send_options<'a, C>(config: &'a C) -> SendOptions<'a>
    where
        C: EventBusConfig + Any,
    {
        let mut options = SendOptions::default();
        if let Some(redis_config) = (config as &dyn Any).downcast_ref::<RedisProducerConfig>() {
            if let Some(maxlen) = redis_config.maxlen_value() {
                let strategy = match redis_config.trim_strategy() {
                    TrimStrategy::Exact => StreamTrimStrategy::Exact,
                    TrimStrategy::Approximate => StreamTrimStrategy::Approximate,
                };
                options = options.stream_trim(maxlen, strategy);
            }
        }
        options
    }

    /// Flush all pending messages in the Redis writer queue.
    pub fn flush(&mut self) -> Result<(), RedisWriterError> {
        let backend_res = self
            .backend
            .as_ref()
            .ok_or(RedisWriterError::BackendUnavailable)?;
        let mut backend = backend_res.write();

        if let Some(redis) = backend.as_any_mut().downcast_mut::<RedisEventBusBackend>() {
            runtime::block_on(redis.flush()).map_err(RedisWriterError::backend)
        } else {
            runtime::block_on(backend.flush()).map_err(RedisWriterError::backend)
        }
    }
}

impl<'w, T> BusEventWriter<T> for RedisEventWriter<'w>
where
    T: BusEvent + Event,
{
    fn write<C>(&mut self, config: &C, event: T)
    where
        C: EventBusConfig + Any,
    {
        let base_options = Self::resolve_send_options(config);

        if let Some(backend_res) = &self.backend {
            let backend = backend_res.read();
            for stream in config.topics() {
                if !backend.try_send(&event, stream, base_options) {
                    let error_event = EventBusError::immediate(
                        stream.clone(),
                        EventBusErrorType::Other,
                        "Failed to send to external backend".to_string(),
                        event.clone(),
                    );
                    self.error_queue.add_error(error_event);
                }
            }
        } else {
            for stream in config.topics() {
                let error_event = EventBusError::immediate(
                    stream.clone(),
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
        let base_options = Self::resolve_send_options(config);

        if let Some(backend_res) = &self.backend {
            let backend = backend_res.read();
            for event in events.into_iter() {
                for stream in config.topics() {
                    if !backend.try_send(&event, stream, base_options) {
                        let error_event = EventBusError::immediate(
                            stream.clone(),
                            EventBusErrorType::Other,
                            "Failed to send to external backend".to_string(),
                            event.clone(),
                        );
                        self.error_queue.add_error(error_event);
                    }
                }
            }
        } else {
            for event in events.into_iter() {
                for stream in config.topics() {
                    let error_event = EventBusError::immediate(
                        stream.clone(),
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
    use bevy::ecs::system::SystemState;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, Event)]
    struct TestEvent {
        value: u32,
    }

    #[test]
    fn resolve_send_options_preserves_trim_settings() {
        let config = RedisProducerConfig::new("orders")
            .maxlen(512)
            .set_trim_strategy(TrimStrategy::Exact);

        let options = RedisEventWriter::resolve_send_options(&config);

        assert_eq!(options.stream_trim, Some((512, StreamTrimStrategy::Exact)));
    }

    #[test]
    fn missing_backend_enqueues_error() {
        let mut world = World::default();
        world.insert_resource(EventBusErrorQueue::default());

        let mut state = SystemState::<RedisEventWriter>::new(&mut world);
        {
            let mut writer = state.get_mut(&mut world);
            let config = RedisProducerConfig::new("audit");
            let event = TestEvent { value: 7 };
            writer.write(&config, event);
        }
        state.apply(&mut world);

        let queue = world.resource::<EventBusErrorQueue>();
        let pending = queue.drain_pending();
        assert_eq!(pending.len(), 1);
    }
}
