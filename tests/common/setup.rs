//! Test setup utilities for integration tests.
//!
//! These helpers manage a lightweight Kafka container (when docker is available) and
//! construct ready-to-use backends with sane defaults for the test suite.

use super::helpers::kafka_backend_config_for_tests;
use bevy_event_bus::{KafkaEventBusBackend, config::kafka::KafkaTopologyBuilder};
use once_cell::sync::Lazy;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::Producer as _;
use std::process::Command;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};
use tracing::{debug, info, info_span, warn};

const DEFAULT_IMAGE: &str = "redpandadata/redpanda:v23.3.16";
const CONTAINER_NAME: &str = "bevy_event_bus_test_kafka";

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
        .args([
            "ps",
            "--filter",
            &format!("name={}", CONTAINER_NAME),
            "--format",
            "{{.ID}}",
        ])
        .output()
        .ok();
    if let Some(out) = ps {
        if !out.stdout.is_empty() {
            let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
            info!(existing_id = %id, took_ms = detect_start.elapsed().as_millis(), "Found existing Kafka container");
            state.id = Some(id);
            state.bootstrap = Some("localhost:9092".into());
            state.launched = true; // treat as launched for lifetime mgmt (won't remove if not --rm?)
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
            return state.bootstrap.clone();
        }
    }

    info!("Failed to start Kafka test container; falling back to external localhost:9092");
    None
}

fn wait_ready(bootstrap: &str) -> bool {
    use std::net::TcpStream;
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
    use rdkafka::config::ClientConfig;
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

pub fn setup() -> (KafkaEventBusBackend, String) {
    setup_with_offset("earliest", |_| {})
}

pub fn setup_with_offset<F>(offset: &str, configure_topology: F) -> (KafkaEventBusBackend, String)
where
    F: FnOnce(&mut KafkaTopologyBuilder),
{
    let container_bootstrap = ensure_container();
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

    // Require metadata readiness once per test process; subsequent setup() calls skip wait.
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

    // Build config with shorter timeout for quicker negative path
    // Ensure each backend created via setup() uses a distinct consumer group id so that
    // writer and reader apps both receive all messages (Kafka replicates to distinct groups).
    static GROUP_COUNTER: AtomicUsize = AtomicUsize::new(0);
    let unique = GROUP_COUNTER.fetch_add(1, AtomicOrdering::SeqCst);
    let mut additional_config = std::collections::HashMap::new();

    // Set offset configuration
    additional_config.insert("auto.offset.reset".to_string(), offset.to_string());

    let mut config = kafka_backend_config_for_tests(
        &bootstrap,
        Some(format!("bevy_event_bus_test_client_{}", unique)),
        configure_topology,
    );
    for (key, value) in additional_config {
        config.connection = config.connection.insert_additional_config(key, value);
    }

    let backend = KafkaEventBusBackend::new(config);
    info!("Kafka test setup complete (ready)");
    (backend, bootstrap)
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
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::producer::{BaseProducer, Producer};

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
            &format!("topic_readiness_producer_{}", attempts),
        );

        let mut consumer_cfg = ClientConfig::new();
        consumer_cfg.set("bootstrap.servers", bootstrap);
        consumer_cfg.set(
            "group.id",
            &format!("topic_readiness_consumer_{}", attempts),
        );
        consumer_cfg.set(
            "client.id",
            &format!("topic_readiness_consumer_{}", attempts),
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

/// Build a baseline Bevy `App` with the event bus plugins wired to a fresh Kafka backend from `setup()`.
/// An optional customization closure can further configure the `App` (e.g., inserting resources, systems).
/// This centralizes construction so tests share identical initialization semantics.
pub fn build_basic_app<F>(customize: F) -> bevy::prelude::App
where
    F: FnOnce(&mut bevy::prelude::App),
{
    let (backend, _bootstrap) = setup();
    let mut app = bevy::prelude::App::new();
    app.add_plugins(bevy_event_bus::EventBusPlugins(backend));
    customize(&mut app);
    app
}

/// Convenience overload when no customization is needed.
pub fn build_basic_app_simple() -> bevy::prelude::App {
    build_basic_app(|_| {})
}
