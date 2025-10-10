use crate::config::{kafka::KafkaTopologyConfig, redis::RedisTopologyConfig};
use crate::resources::{ConsumerMetrics, IncomingMessage};
use async_trait::async_trait;
use bevy::prelude::{App, World};
use bevy_event_bus::BusEvent;
use crossbeam_channel::Receiver;
use std::any::Any;
use std::error::Error;
use std::fmt::{Debug, Display};

/// Descriptor summarising the manual commit mechanism exposed by a backend.
#[derive(Debug, Clone)]
pub struct ManualCommitDescriptor {
    pub backend: &'static str,
    pub style: ManualCommitStyle,
}

/// Manual commit styles supported by the framework.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManualCommitStyle {
    /// Offsets committed back to the broker via explicit offset queues.
    OffsetQueue,
    /// Stream acknowledgement semantics for append-only streams.
    StreamAck,
}

/// Trait implemented by backend-provided manual commit handles. Allows backends to
/// expose additional resources without the plugin understanding concrete types.
pub trait ManualCommitHandle: Send + Sync {
    fn register_resources(&self, world: &mut World);
    fn descriptor(&self) -> ManualCommitDescriptor;
}

/// Descriptor summarising lag reporting capabilities exposed by a backend.
#[derive(Debug, Clone)]
pub struct LagReportingDescriptor {
    pub backend: &'static str,
    pub detail: &'static str,
}

/// Trait implemented by backend-provided lag reporting handles.
pub trait LagReportingHandle: Send + Sync {
    fn register_resources(&self, world: &mut World);
    fn descriptor(&self) -> LagReportingDescriptor;
}

#[derive(Default)]
pub struct BackendPluginSetup {
    pub ready_topics: Vec<String>,
    pub message_stream: Option<Receiver<IncomingMessage>>,
    pub manual_commit: Option<Box<dyn ManualCommitHandle>>,
    pub lag_reporting: Option<Box<dyn LagReportingHandle>>,
    pub kafka_topology: Option<KafkaTopologyConfig>,
    pub redis_topology: Option<RedisTopologyConfig>,
}

/// Error returned when a backend configuration cannot be applied.
#[derive(Debug, Clone)]
pub struct BackendConfigError {
    backend: &'static str,
    reason: String,
}

impl BackendConfigError {
    pub fn new(backend: &'static str, reason: impl Into<String>) -> Self {
        Self {
            backend,
            reason: reason.into(),
        }
    }

    pub fn backend(&self) -> &'static str {
        self.backend
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl Display for BackendConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Backend '{}' configuration error: {}",
            self.backend, self.reason
        )
    }
}

impl Error for BackendConfigError {}

/// Trait implemented by backend-specific configuration objects to enable dynamic dispatch.
pub trait EventBusBackendConfig: Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;
}

/// Strategy used when trimming append-only streams during writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamTrimStrategy {
    Exact,
    Approximate,
}

/// Backend-specific knobs applied when dispatching serialized events.
#[derive(Clone, Copy, Debug, Default)]
pub struct BackendSpecificSendOptions<'a> {
    data: Option<&'a dyn Any>,
}

impl<'a> BackendSpecificSendOptions<'a> {
    pub fn none() -> Self {
        Self { data: None }
    }

    pub fn new(data: &'a dyn Any) -> Self {
        Self { data: Some(data) }
    }

    pub fn as_any(&self) -> Option<&'a dyn Any> {
        self.data
    }
}

/// Options that customize how serialized events are dispatched to a backend.
#[derive(Clone, Copy, Default)]
pub struct SendOptions<'a> {
    pub partition_key: Option<&'a str>,
    pub stream_trim: Option<(usize, StreamTrimStrategy)>,
    pub backend: BackendSpecificSendOptions<'a>,
}

impl<'a> SendOptions<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn partition_key(mut self, key: &'a str) -> Self {
        self.partition_key = Some(key);
        self
    }

    pub fn stream_trim(mut self, maxlen: usize, strategy: StreamTrimStrategy) -> Self {
        self.stream_trim = Some((maxlen, strategy));
        self
    }

    pub fn backend_options(mut self, backend: BackendSpecificSendOptions<'a>) -> Self {
        self.backend = backend;
        self
    }
}

/// Options that control how messages are received from a backend.
#[derive(Clone, Copy, Default)]
pub struct ReceiveOptions<'a> {
    pub consumer_group: Option<&'a str>,
}

impl<'a> ReceiveOptions<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn consumer_group(mut self, group_id: &'a str) -> Self {
        self.consumer_group = Some(group_id);
        self
    }
}

/// Trait defining the common interface for event bus backends
///
/// Backends should be simple and never panic. Connection/send failures
/// are handled gracefully by returning success/failure indicators.
#[async_trait]
pub trait EventBusBackend: Send + Sync + 'static + Debug {
    fn clone_box(&self) -> Box<dyn EventBusBackend>;
    fn as_any(&self) -> &dyn std::any::Any;
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;

    /// Identifier used for logging and lifecycle events.
    fn backend_name(&self) -> &'static str {
        "unknown"
    }

    /// Allow the backend to register additional events, resources and systems with the plugin's [`App`].
    fn configure_plugin(&self, _app: &mut App) {}

    /// Give the backend a chance to install runtime resources once connection has been established.
    fn setup_plugin(&self, _world: &mut World) -> BackendPluginSetup {
        BackendPluginSetup::default()
    }

    /// Allow the backend to annotate consumer metrics with backend-specific counters.
    fn augment_metrics(&self, _metrics: &mut ConsumerMetrics) {}

    /// Apply backend-specific configuration prior to connecting.
    fn configure(&mut self, _config: &dyn EventBusBackendConfig) -> Result<(), BackendConfigError> {
        let _ = _config;
        Ok(())
    }

    /// Apply topology-defined event registrations to the provided Bevy `App`.
    fn apply_event_bindings(&self, _app: &mut App) {}

    /// Connect to the backend. Returns true if successful.
    async fn connect(&mut self) -> bool;

    /// Disconnect from the backend. Returns true if successful.
    async fn disconnect(&mut self) -> bool;

    /// Non-blocking send that queues the event for delivery.
    /// Returns true if the event was queued successfully, false if it failed.
    fn try_send_serialized(&self, event_json: &[u8], topic: &str, options: SendOptions<'_>)
    -> bool;

    /// Receive serialized messages from a topic.
    /// Returns empty vec if no messages or on error.
    async fn receive_serialized(&self, topic: &str, options: ReceiveOptions<'_>) -> Vec<Vec<u8>>;

    /// Flush all pending messages (producer-side)
    async fn flush(&self) -> Result<(), String> {
        Ok(()) // Default no-op so that basic backends do not need to override it.
    }
}

/// Trait implemented by backends that can provision consumer groups dynamically at runtime.
#[async_trait]
pub trait ConsumerGroupManager: EventBusBackend {
    async fn create_consumer_group(
        &mut self,
        topics: &[String],
        group_id: &str,
    ) -> Result<(), String>;
}

/// Trait implemented by backends that expose manual commit controls.
#[async_trait]
pub trait ManualCommitController: EventBusBackend {
    async fn enable_manual_commits(&mut self, group_id: &str) -> Result<(), String>;
    async fn commit_offset(&self, topic: &str, partition: i32, offset: i64) -> Result<(), String>;
}

/// Trait implemented by backends that can report consumer lag.
#[async_trait]
pub trait LagReportingBackend: EventBusBackend {
    async fn get_consumer_lag(&self, topic: &str, group_id: &str) -> Result<i64, String>;
}

/// Helper methods for working with EventBusBackend trait objects
impl dyn EventBusBackend {
    /// Non-blocking send that queues the event for delivery.
    /// Returns true if the event was serialized and queued successfully.
    pub fn try_send<T: BusEvent>(&self, event: &T, topic: &str, options: SendOptions<'_>) -> bool {
        match serde_json::to_vec(event) {
            Ok(serialized) => self.try_send_serialized(&serialized, topic, options),
            Err(_) => false, // Serialization failed
        }
    }

    /// Receive and deserialize messages from a topic.
    /// Returns empty vec if no messages or on error.
    pub async fn receive<T: BusEvent>(&self, topic: &str, options: ReceiveOptions<'_>) -> Vec<T> {
        let serialized_messages = self.receive_serialized(topic, options).await;
        let mut result = Vec::with_capacity(serialized_messages.len());
        for message in serialized_messages {
            if let Ok(deserialized) = serde_json::from_slice(&message) {
                result.push(deserialized);
            }
            // Silently skip messages that fail to deserialize
        }
        result
    }
}
