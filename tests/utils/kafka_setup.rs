//! Test setup utilities for integration tests.
//!
//! These helpers manage a lightweight Kafka container (when docker is available) and
//! construct ready-to-use backends with sane defaults for the test suite.

use bevy_event_bus::{
    KafkaBackendConfig, KafkaConnectionConfig, KafkaEventBusBackend,
    config::kafka::KafkaTopologyBuilder,
};
use once_cell::sync::Lazy;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseProducer, Producer};
use std::collections::HashMap;
use std::net::TcpStream;
use std::process::Command;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};
use tracing::{debug, info, info_span, warn};

const DEFAULT_IMAGE: &str = "redpandadata/redpanda:v23.3.16";
const CONTAINER_NAME: &str = "bevy_event_bus_test_kafka";
const DEFAULT_OFFSET: &str = "latest";

/// Additional configuration controls for Kafka test backend construction.
#[derive(Clone, Debug)]
pub struct SetupOptions {
    auto_offset_reset: Option<String>,
    additional_connection_config: HashMap<String, String>,
}

impl SetupOptions {
    /// Create a new options struct with sensible defaults.
    /// By default the consumer offset reset policy is set to `latest`.
    pub fn new() -> Self {
        Self {
            auto_offset_reset: Some(DEFAULT_OFFSET.to_string()),
            additional_connection_config: HashMap::new(),
        }
    }

    /// Override the `auto.offset.reset` policy for the backend connection.
    pub fn auto_offset_reset(mut self, offset: impl Into<String>) -> Self {
        self.auto_offset_reset = Some(offset.into());
        self
    }

    /// Remove the `auto.offset.reset` override, falling back to the broker default.
    pub fn disable_auto_offset_reset(mut self) -> Self {
        self.auto_offset_reset = None;
        self
    }

    /// Append an additional key/value pair to the Kafka connection configuration.
    pub fn insert_connection_config(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.additional_connection_config
            .insert(key.into(), value.into());
        self
    }

    /// Convenience helper returning an options struct configured for `earliest` offset behaviour.
    pub fn earliest() -> Self {
        Self::new().auto_offset_reset("earliest")
    }

    /// Convenience helper returning an options struct configured for `latest` offset behaviour.
    pub fn latest() -> Self {
        Self::new().auto_offset_reset("latest")
    }
}

impl Default for SetupOptions {
    fn default() -> Self {
        Self::new()
    }
}

pub struct SetupRequest {
    builder: KafkaTopologyBuilder,
    options: SetupOptions,
}

impl From<KafkaTopologyBuilder> for SetupRequest {
    fn from(builder: KafkaTopologyBuilder) -> Self {
        Self {
            builder,
            options: SetupOptions::default(),
        }
    }
}

impl From<(KafkaTopologyBuilder, SetupOptions)> for SetupRequest {
    fn from((builder, options): (KafkaTopologyBuilder, SetupOptions)) -> Self {
        Self { builder, options }
    }
}

/// Holds lifecycle data for the ephemeral Kafka container used in tests
#[derive(Default, Debug, Clone)]
struct ContainerState {
    id: Option<String>,
    bootstrap: Option<String>,
    launched: bool,
    image: Option<String>,
}

static CONTAINER_STATE: Lazy<Mutex<ContainerState>> =
    Lazy::new(|| Mutex::new(ContainerState::default()));

fn docker_available() -> bool {
    Command::new("docker")
        .arg("version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn ensure_container() -> Option<String> {
    // Respect external override early
    if std::env::var("KAFKA_BOOTSTRAP_SERVERS").is_ok() {
        return None;
    }
    if !docker_available() {
        return None;
    }

    let mut state = CONTAINER_STATE.lock().unwrap();
    if state.launched {
        return state.bootstrap.clone();
    }

    let span = info_span!("kafka_container.ensure");
    let _g = span.enter();

    // Check existing running container
    let detect_start = Instant::now();
    let ps = Command::new("docker")
        .arg("ps")
        .arg("--filter")
        .arg(format!("name={}", CONTAINER_NAME))
        .arg("--format")
        .arg("{{.ID}}")
        .output()
        .ok();
    if let Some(out) = ps {
        if !out.stdout.is_empty() {
            let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
            info!(existing_id = %id, took_ms = detect_start.elapsed().as_millis(), "Found existing Kafka container");
            state.id = Some(id);
            state.bootstrap = Some("localhost:9092".into());
            state.launched = true; // treat as launched for lifetime mgmt (won't remove if not --rm?)
            let _ = Command::new("docker")
                .args([
                    "exec",
                    state.id.as_ref().map(String::as_str).unwrap_or(CONTAINER_NAME),
                    "rpk",
                    "cluster",
                    "config",
                    "set",
                    "auto_create_topics_enabled",
                    "false",
                ])
                .status();
            return state.bootstrap.clone();
        }
    }

    let pull_span = info_span!("kafka_container.pull", image = DEFAULT_IMAGE);
    {
        let _pg = pull_span.enter();
        let _ = Command::new("docker")
            .args(["pull", DEFAULT_IMAGE])
            .status();
    }

    let run_span = info_span!("kafka_container.run", image = DEFAULT_IMAGE);
    let run_start = Instant::now();
    let status = {
        let _rg = run_span.enter();
        Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm",
                "--name",
                CONTAINER_NAME,
                "-p",
                "9092:9092",
                DEFAULT_IMAGE,
                "start",
                "--smp",
                "1",
                "--memory",
                "1G",
                "--reserve-memory",
                "0M",
                "--overprovisioned",
                "--node-id",
                "0",
                "--check=false",
                "--kafka-addr",
                "PLAINTEXT://0.0.0.0:9092",
                "--advertise-kafka-addr",
                "PLAINTEXT://localhost:9092",
                "--set",
                "redpanda.auto_create_topics_enabled=false",
            ])
            .output()
            .ok()
    };

    if let Some(out) = status {
        if out.status.success() {
            let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
            info!(
                container_id = %id,
                image = DEFAULT_IMAGE,
                took_ms = run_start.elapsed().as_millis(),
                "Started Kafka-compatible container"
            );
            state.id = Some(id);
            state.bootstrap = Some("localhost:9092".into());
            state.launched = true;
            state.image = Some(DEFAULT_IMAGE.to_string());
            let _ = Command::new("docker")
                .args([
                    "exec",
                    state.id.as_ref().map(String::as_str).unwrap_or(CONTAINER_NAME),
                    "rpk",
                    "cluster",
                    "config",
                    "set",
                    "auto_create_topics_enabled",
                    "false",
                ])
                .status();
            return state.bootstrap.clone();
        }
    }

    info!("Failed to start Kafka test container; falling back to external localhost:9092");
    None
}

fn wait_ready(bootstrap: &str) -> bool {
    let span = info_span!("kafka_container.wait_ready", bootstrap = bootstrap);
    let _g = span.enter();
    let start = Instant::now();
    // Reduce timeout from 40s -> 8s for faster feedback
    let timeout = Duration::from_secs(8);
    let mut attempts = 0u32;
    while start.elapsed() < timeout {
        attempts += 1;
        if TcpStream::connect(bootstrap).is_ok() {
            info!(
                attempts,
                waited_ms = start.elapsed().as_millis(),
                "Kafka ready"
            );
            return true;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    info!(
        attempts,
        waited_ms = start.elapsed().as_millis(),
        "Kafka NOT ready before timeout"
    );
    false
}

/// Teardown at process end (unless user opts to keep container)
#[ctor::dtor]
fn teardown_container() {
    // Skip if user wants to keep for debugging or external bootstrap used
    if std::env::var("BEVY_EVENT_BUS_KEEP_KAFKA").ok().as_deref() == Some("1") {
        return;
    }
    if std::env::var("KAFKA_BOOTSTRAP_SERVERS").is_ok() {
        return;
    }
    if !docker_available() {
        return;
    }
    let state = CONTAINER_STATE.lock().unwrap().clone();
    if let Some(id) = state.id {
        let span = info_span!("kafka_container.teardown", container_id = %id);
        let _g = span.enter();
        // Best effort stop (container started with --rm, so stop removes it)
        let _ = Command::new("docker").args(["stop", &id]).status();
        info!("Kafka test container stopped");
    }
}

/// Simple metadata readiness check - just verify Kafka broker metadata is accessible
static METADATA_READY: Lazy<std::sync::atomic::AtomicBool> =
    Lazy::new(|| std::sync::atomic::AtomicBool::new(false));

fn wait_metadata(bootstrap: &str, max_wait: Duration) -> (bool, u128) {
    let span = info_span!("kafka_container.wait_metadata", bootstrap = bootstrap);
    let _g = span.enter();
    let start = Instant::now();
    let mut attempt: u32 = 0;

    while start.elapsed() < max_wait {
        attempt += 1;
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", bootstrap);
        if let Ok(producer) = cfg.create::<rdkafka::producer::BaseProducer>() {
            match producer
                .client()
                .fetch_metadata(None, Duration::from_millis(1000))
            {
                Ok(md) => {
                    info!(
                        brokers = md.brokers().len(),
                        attempts = attempt,
                        elapsed_ms = start.elapsed().as_millis(),
                        "Metadata available"
                    );
                    METADATA_READY.store(true, std::sync::atomic::Ordering::SeqCst);
                    return (true, start.elapsed().as_millis());
                }
                Err(_) => {
                    // Simple fixed backoff - metadata is usually ready quickly once TCP is up
                    std::thread::sleep(Duration::from_millis(200));
                }
            }
        }
    }
    warn!(
        attempts = attempt,
        elapsed_ms = start.elapsed().as_millis(),
        "Metadata NOT ready before timeout"
    );
    (false, start.elapsed().as_millis())
}

/// Construct a Kafka backend for integration tests using a prepared topology builder.
/// Tests may provide either a builder directly or pair it with [`SetupOptions`] for
/// additional connection-level tweaks.
pub fn prepare_backend<S>(input: S) -> (KafkaEventBusBackend, String)
where
    S: Into<SetupRequest>,
{
    let SetupRequest { builder, options } = input.into();
    let SetupOptions {
        auto_offset_reset,
        additional_connection_config,
    } = options;

    let container_bootstrap = ensure_container();
    let container_from_docker = container_bootstrap.is_some();
    let bootstrap = container_bootstrap.unwrap_or_else(|| {
        std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".into())
    });

    bevy_event_bus::runtime(); // initialize shared runtime early

    if !wait_ready(&bootstrap) {
        panic!(
            "Kafka not TCP ready at {}. Ensure docker is running or set KAFKA_BOOTSTRAP_SERVERS.",
            bootstrap
        );
    }

    if container_from_docker {
        if let Some(id) = CONTAINER_STATE.lock().unwrap().id.clone() {
            let _ = Command::new("docker")
                .args([
                    "exec",
                    id.as_str(),
                    "rpk",
                    "cluster",
                    "config",
                    "set",
                    "auto_create_topics_enabled",
                    "false",
                ])
                .status();
        }
    }

    // Require metadata readiness once per test process; subsequent setup calls skip wait.
    if !METADATA_READY.load(std::sync::atomic::Ordering::SeqCst) {
        // Allow more generous window for fresh Kafka container internal initialization (KRaft can take >20s cold)
        let (metadata_ok, _elapsed) = wait_metadata(&bootstrap, Duration::from_secs(30));
        if !metadata_ok {
            panic!(
                "Kafka metadata not ready at {} after {}ms",
                bootstrap, _elapsed
            );
        }
    } else {
        debug!("Metadata already confirmed ready earlier; skipping wait");
    }

    // Build config with shorter timeout for quicker negative path.
    // Ensure each backend created via `setup` uses a distinct
    // consumer group id so that writer and reader apps both receive all messages (Kafka replicates to distinct groups).
    static GROUP_COUNTER: AtomicUsize = AtomicUsize::new(0);
    let unique = GROUP_COUNTER.fetch_add(1, AtomicOrdering::SeqCst);

    let topology = builder.build();
    let mut connection = KafkaConnectionConfig::new(&bootstrap)
        .set_client_id(format!("bevy_event_bus_test_client_{}", unique))
        .set_timeout_ms(5000);

    if let Some(offset) = auto_offset_reset {
        connection = connection.insert_additional_config("auto.offset.reset", offset);
    }
    for (key, value) in additional_connection_config {
        connection = connection.insert_additional_config(key, value);
    }

    let config = KafkaBackendConfig::new(connection, topology, Duration::from_secs(1));

    let backend = KafkaEventBusBackend::new(config)
        .unwrap_or_else(|err| panic!("Kafka backend initialization failed: {err}"));
    info!("Kafka test setup complete (ready)");
    (backend, bootstrap)
}

/// Helper to obtain a [`KafkaTopologyBuilder`] by applying a closure, allowing tests to keep
/// concise configuration blocks while still passing a fully-constructed builder into [`setup`].
pub fn build_topology<F>(configure: F) -> KafkaTopologyBuilder
where
    F: FnOnce(&mut KafkaTopologyBuilder),
{
    let mut builder = KafkaTopologyBuilder::default();
    configure(&mut builder);
    builder
}

/// Create a [`SetupRequest`] from a configuration closure and [`SetupOptions`].
pub fn build_request<F>(options: SetupOptions, configure: F) -> SetupRequest
where
    F: FnOnce(&mut KafkaTopologyBuilder),
{
    SetupRequest::from((build_topology(configure), options))
}

/// Convenience helper producing a [`SetupRequest`] configured with `earliest` offset semantics.
pub fn earliest<F>(configure: F) -> SetupRequest
where
    F: FnOnce(&mut KafkaTopologyBuilder),
{
    build_request(SetupOptions::earliest(), configure)
}

/// Convenience helper producing a [`SetupRequest`] configured with `latest` offset semantics.
pub fn latest<F>(configure: F) -> SetupRequest
where
    F: FnOnce(&mut KafkaTopologyBuilder),
{
    build_request(SetupOptions::latest(), configure)
}

/// Same as `setup` but allows providing a unique suffix for the consumer group id so that
/// multiple backends in the same test process do not share a group (ensuring each receives all messages).
/// Ensure a topic exists (idempotent) using Kafka Admin API. Best-effort; ignores 'topic already exists' errors.
pub fn ensure_topic(bootstrap: &str, topic: &str, partitions: i32) {
    let mut cfg = rdkafka::config::ClientConfig::new();
    cfg.set("bootstrap.servers", bootstrap);
    if let Ok(admin) = cfg.create::<AdminClient<DefaultClientContext>>() {
        let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
        let opts = AdminOptions::new();
        let fut = admin.create_topics([&new_topic], &opts);
        if let Err(e) = bevy_event_bus::block_on(fut) {
            let msg = e.to_string();
            if !msg.contains("TopicAlreadyExists") {
                tracing::warn!(topic=%topic, err=%msg, "ensure_topic create failed");
            }
        }
    } else {
        tracing::warn!(topic=%topic, "ensure_topic could not build admin client");
    }
}

/// Create a topic and wait for it to be fully ready for read/write operations.
/// Combines topic creation with readiness verification in a single call.
/// Returns true if the topic is ready, false if timeout exceeded.
pub fn ensure_topic_ready(
    bootstrap: &str,
    topic: &str,
    partitions: i32,
    timeout: Duration,
) -> bool {
    let span = info_span!(
        "kafka_container.ensure_topic_ready",
        bootstrap = bootstrap,
        topic = topic
    );
    let _g = span.enter();
    let start = Instant::now();
    let mut attempts = 0u32;

    // First ensure topic exists using Admin API
    ensure_topic(bootstrap, topic, partitions);

    while start.elapsed() < timeout {
        attempts += 1;

        // Test both producer and consumer can see the topic
        let mut producer_cfg = ClientConfig::new();
        producer_cfg.set("bootstrap.servers", bootstrap);
        producer_cfg.set(
            "client.id",
            format!("topic_readiness_producer_{}", attempts),
        );

        let mut consumer_cfg = ClientConfig::new();
        consumer_cfg.set("bootstrap.servers", bootstrap);
        consumer_cfg.set("group.id", format!("topic_readiness_consumer_{}", attempts));
        consumer_cfg.set(
            "client.id",
            format!("topic_readiness_consumer_{}", attempts),
        );
        consumer_cfg.set("auto.offset.reset", "earliest");

        if let (Ok(producer), Ok(consumer)) = (
            producer_cfg.create::<BaseProducer>(),
            consumer_cfg.create::<BaseConsumer>(),
        ) {
            // Check if producer can fetch metadata for this specific topic
            match producer
                .client()
                .fetch_metadata(Some(topic), Duration::from_millis(1000))
            {
                Ok(metadata) => {
                    // Check if topic exists in metadata with at least one partition
                    if let Some(topic_metadata) =
                        metadata.topics().iter().find(|t| t.name() == topic)
                    {
                        if !topic_metadata.partitions().is_empty()
                            && topic_metadata.error().is_none()
                        {
                            // Topic exists with partitions, now test consumer can subscribe
                            if consumer.subscribe(&[topic]).is_ok() {
                                info!(
                                    attempts,
                                    elapsed_ms = start.elapsed().as_millis(),
                                    "Topic created and ready for read/write operations"
                                );
                                return true;
                            }
                        }
                    }
                }
                Err(e) => {
                    debug!(attempt = attempts, err = %e, "Topic metadata fetch failed");
                }
            }
        }

        // Progressive backoff: 50ms, 100ms, 200ms, 400ms, 400ms...
        let delay_ms = std::cmp::min(50 * 2_u64.pow((attempts - 1).min(3)), 400);
        std::thread::sleep(Duration::from_millis(delay_ms));
    }

    warn!(
        attempts,
        elapsed_ms = start.elapsed().as_millis(),
        "Topic NOT ready before timeout"
    );
    false
}

/// Build a baseline Bevy `App` with the event bus plugins wired to a fresh Kafka backend from [`setup`].
/// An optional customization closure can further configure the `App` (e.g., inserting resources, systems).
/// This centralizes construction so tests share identical initialization semantics.
pub fn build_basic_app<F>(customize: F) -> bevy::prelude::App
where
    F: FnOnce(&mut bevy::prelude::App),
{
    let (backend, _bootstrap) = prepare_backend(latest(|_| {}));
    let mut app = bevy::prelude::App::new();
    app.add_plugins(bevy_event_bus::EventBusPlugins(backend));
    customize(&mut app);
    app
}

/// Convenience overload when no customization is needed.
pub fn build_basic_app_simple() -> bevy::prelude::App {
    build_basic_app(|_| {})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setup_options_can_disable_offset_reset() {
        let options = SetupOptions::new().disable_auto_offset_reset();
        assert!(options.auto_offset_reset.is_none());
    }

    #[test]
    fn setup_options_can_add_connection_config() {
        let options = SetupOptions::new().insert_connection_config("foo", "bar");
        assert_eq!(
            options.additional_connection_config.get("foo"),
            Some(&"bar".to_string())
        );
    }
}
