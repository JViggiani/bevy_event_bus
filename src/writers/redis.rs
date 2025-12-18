use std::{any::Any, sync::Arc};

use bevy::prelude::*;

use bevy_event_bus::backends::{
    EventBusBackendResource, RedisEventBusBackend,
    event_bus_backend::{
        DeliveryFailureCallback, EventBusBackend, SendOptions, StreamTrimStrategy,
    },
};
use bevy_event_bus::config::{EventBusConfig, redis::RedisProducerConfig, redis::TrimStrategy};
use bevy_event_bus::errors::{BusErrorCallback, BusErrorContext, BusErrorKind};
use bevy_event_bus::resources::{MessageMetadata, ProvisionedTopology};
use bevy_event_bus::{BusEvent, runtime};

use super::BusMessageWriter;

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
pub struct RedisMessageWriter<'w> {
    backend: Option<Res<'w, EventBusBackendResource>>,
    topology: Option<Res<'w, ProvisionedTopology>>,
}

impl<'w> RedisMessageWriter<'w> {
    /// Write a message with an optional error callback.
    pub fn write<T, C>(&mut self, config: &C, event: T, callback: Option<BusErrorCallback>)
    where
        T: BusEvent + Message,
        C: EventBusConfig + Any,
    {
        let callback_ref = callback.as_ref();

        if let Some(redis_config) = (config as &dyn Any).downcast_ref::<RedisProducerConfig>() {
            if let Some(invalid_streams) = self.invalid_streams(redis_config) {
                for stream in invalid_streams {
                    Self::emit_error(
                        callback_ref,
                        &stream,
                        BusErrorKind::InvalidWriteConfig,
                        format!("Stream '{stream}' is not provisioned in the Redis topology"),
                        None,
                        None,
                    );
                }
                return;
            }
        }

        let base_options = Self::resolve_send_options(config);

        if let Some(backend_res) = &self.backend {
            let backend = backend_res.read();
            match serde_json::to_vec(&event) {
                Ok(serialized) => {
                    for stream in config.topics() {
                        let delivery_handler = callback.clone().map(|cb| {
                            let stream_name = stream.clone();
                            let payload = serialized.clone();
                            Arc::new(DeliveryFailureCallback::new(move |failure| {
                                cb(BusErrorContext::new(
                                    failure.backend,
                                    stream_name.clone(),
                                    failure.kind.clone(),
                                    failure.error.clone(),
                                    failure.metadata.clone(),
                                    Some(payload.clone()),
                                ));
                            })) as Arc<DeliveryFailureCallback>
                        });

                        if !backend.try_send_serialized(
                            &serialized,
                            stream,
                            base_options,
                            delivery_handler,
                        ) {
                            Self::emit_error(
                                callback_ref,
                                stream,
                                BusErrorKind::DeliveryFailure,
                                "Failed to enqueue Redis message",
                                None,
                                Some(serialized.clone()),
                            );
                        }
                    }
                }
                Err(err) => {
                    for stream in config.topics() {
                        Self::emit_error(
                            callback_ref,
                            stream,
                            BusErrorKind::Serialization,
                            err.to_string(),
                            None,
                            None,
                        );
                    }
                }
            }
        } else {
            for stream in config.topics() {
                Self::emit_error(
                    callback_ref,
                    stream,
                    BusErrorKind::NotConfigured,
                    "No event bus backend configured",
                    None,
                    None,
                );
            }
        }
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

    fn emit_error(
        callback: Option<&BusErrorCallback>,
        stream: &str,
        kind: BusErrorKind,
        message: impl Into<String>,
        metadata: Option<MessageMetadata>,
        payload: Option<Vec<u8>>,
    ) {
        let message = message.into();
        if let Some(cb) = callback {
            cb(BusErrorContext::new(
                "redis",
                stream.to_string(),
                kind,
                message.clone(),
                metadata,
                payload,
            ));
        } else {
            bevy::log::warn!(backend = "redis", stream = %stream, error = ?kind, "Unhandled writer error: {message}");
        }
    }
}

impl<'w, T> BusMessageWriter<T> for RedisMessageWriter<'w>
where
    T: BusEvent + Message,
{
    fn write<C>(&mut self, config: &C, event: T, error_callback: Option<BusErrorCallback>)
    where
        C: EventBusConfig + Any,
    {
        RedisMessageWriter::write(self, config, event, error_callback);
    }

    fn write_batch<C, I>(&mut self, config: &C, events: I, error_callback: Option<BusErrorCallback>)
    where
        C: EventBusConfig + Any,
        I: IntoIterator<Item = T>,
    {
        for event in events.into_iter() {
            RedisMessageWriter::write(self, config, event, error_callback.clone());
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
    fn resolve_send_options_preserves_trim_settings() {
        let config = RedisProducerConfig::new("orders")
            .maxlen(512)
            .set_trim_strategy(TrimStrategy::Exact);

        let options = RedisMessageWriter::resolve_send_options(&config);

        assert_eq!(options.stream_trim, Some((512, StreamTrimStrategy::Exact)));
    }

    #[test]
    fn missing_backend_enqueues_error() {
        let mut world = World::default();

        let errors: Arc<Mutex<Vec<BusErrorContext>>> = Arc::new(Mutex::new(Vec::new()));
        let callback: BusErrorCallback = {
            let store: Arc<Mutex<Vec<BusErrorContext>>> = Arc::clone(&errors);
            Arc::new(move |ctx| {
                store.lock().unwrap().push(ctx);
            })
        };

        let mut state = SystemState::<RedisMessageWriter>::new(&mut world);
        {
            let mut writer = state.get_mut(&mut world);
            let config = RedisProducerConfig::new("audit");
            let event = TestEvent { value: 7 };
            writer.write(&config, event, Some(callback.clone()));
        }
        state.apply(&mut world);

        let collected = errors.lock().unwrap().clone();
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].kind, BusErrorKind::NotConfigured);
        assert_eq!(collected[0].topic, "audit");
    }

    #[test]
    fn invalid_stream_enqueues_error() {
        let mut world = World::default();

        let mut topology = ProvisionedTopology::default();
        let mut builder = bevy_event_bus::config::redis::RedisTopologyConfig::builder();
        builder.add_stream(bevy_event_bus::config::redis::RedisStreamSpec::new("known"));
        topology.record_redis(builder.build());
        world.insert_resource(topology);

        let errors: Arc<Mutex<Vec<BusErrorContext>>> = Arc::new(Mutex::new(Vec::new()));
        let callback: BusErrorCallback = {
            let store: Arc<Mutex<Vec<BusErrorContext>>> = Arc::clone(&errors);
            Arc::new(move |ctx| {
                store.lock().unwrap().push(ctx);
            })
        };

        let mut state = SystemState::<RedisMessageWriter>::new(&mut world);
        {
            let mut writer = state.get_mut(&mut world);
            let config = RedisProducerConfig::new("unknown");
            let event = TestEvent { value: 99 };
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
