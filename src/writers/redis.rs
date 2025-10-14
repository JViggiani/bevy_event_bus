#![cfg(feature = "redis")]

use std::any::Any;

use bevy::prelude::*;

use bevy_event_bus::backends::{
    EventBusBackendResource, RedisEventBusBackend,
    event_bus_backend::{EventBusBackend, SendOptions, StreamTrimStrategy},
};
use bevy_event_bus::config::{EventBusConfig, redis::RedisProducerConfig, redis::TrimStrategy};
use bevy_event_bus::resources::ProvisionedTopology;
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
    topology: Option<Res<'w, ProvisionedTopology>>,
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

    fn invalid_streams(&self, config: &RedisProducerConfig) -> Option<Vec<String>> {
        let topology = self
            .topology
            .as_ref()
            .and_then(|registry| registry.redis())?;
        let provisioned = topology.stream_names();

        let invalid: Vec<String> = config
            .topics()
            .iter()
            .filter(|stream| !provisioned.contains(*stream))
            .cloned()
            .collect();

        if invalid.is_empty() {
            None
        } else {
            Some(invalid)
        }
    }

    fn report_invalid_streams<T: BusEvent + Event>(&self, streams: &[String], event: &T) {
        for stream in streams {
            let reason = format!(
                "Stream '{}' is not provisioned in the Redis topology",
                stream
            );
            let error_event = EventBusError::immediate(
                stream.clone(),
                EventBusErrorType::InvalidWriteConfig,
                reason.clone(),
                event.clone(),
            );
            self.error_queue.add_error(error_event);
            tracing::warn!(
                backend = "redis",
                stream = %stream,
                reason = %reason,
                "Redis writer configuration invalid"
            );
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
        if let Some(redis_config) = (config as &dyn Any).downcast_ref::<RedisProducerConfig>() {
            if let Some(invalid_streams) = self.invalid_streams(redis_config) {
                self.report_invalid_streams(&invalid_streams, &event);
                return;
            }
        }

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
        if let Some(redis_config) = (config as &dyn Any).downcast_ref::<RedisProducerConfig>() {
            if let Some(invalid_streams) = self.invalid_streams(redis_config) {
                for event in events.into_iter() {
                    self.report_invalid_streams(&invalid_streams, &event);
                }
                return;
            }
        }

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
    use bevy::ecs::event::EventReader;
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

    #[test]
    fn invalid_stream_enqueues_error() {
        let mut world = World::default();
        world.insert_resource(EventBusErrorQueue::default());
        world.insert_resource(Events::<EventBusError<TestEvent>>::default());

        let mut topology = ProvisionedTopology::default();
        let mut builder = bevy_event_bus::config::redis::RedisTopologyConfig::builder();
        builder.add_stream(bevy_event_bus::config::redis::RedisStreamSpec::new("known"));
        topology.record_redis(builder.build());
        world.insert_resource(topology);

        let mut state = SystemState::<RedisEventWriter>::new(&mut world);
        {
            let mut writer = state.get_mut(&mut world);
            let config = RedisProducerConfig::new("unknown");
            let event = TestEvent { value: 99 };
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
