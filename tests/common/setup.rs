//! Test setup utilities. Currently placeholder for docker CLI based Kafka spin-up.
//! The previous testcontainers-based approach was removed for determinism and reduced flakiness.
//! TODO: Implement a small helper that:
//! 1. Invokes `docker run -d -p 9092:9092 bitnami/kafka:latest ...` if a container isn't already running.
//! 2. Waits for readiness by polling metadata using rdkafka.
//! 3. Exposes a simple `setup()` returning a configured `KafkaEventBusBackend`.
//! For now the integration test assumes an external Kafka is available on localhost:9092.

use bevy_event_bus::{KafkaConfig, KafkaEventBusBackend};
use once_cell::sync::Lazy;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::Producer as _;
use std::process::Command;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};
use tracing::{debug, info, info_span, warn};

const DEFAULT_IMAGE: &str = "bitnami/kafka:latest";
const CONTAINER_NAME: &str = "bevy_event_bus_test_kafka";

/// Holds lifecycle data for the ephemeral Kafka container used in tests
#[derive(Default, Debug, Clone)]
struct ContainerState {
    id: Option<String>,
    bootstrap: Option<String>,
    launched: bool,
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

    let span = info_span!("kafka_container.ensure", image = DEFAULT_IMAGE);
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

    // Pull (best effort)
    let pull_span = info_span!("kafka_container.pull");
    {
        let _pg = pull_span.enter();
        let _ = Command::new("docker")
            .args(["pull", DEFAULT_IMAGE])
            .status();
    }

    let run_span = info_span!("kafka_container.run");
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
                "-p",
                "9093:9093",
                "-e",
                "KAFKA_ENABLE_KRAFT=yes",
                "-e",
                "ALLOW_PLAINTEXT_LISTENER=yes",
                "-e",
                "KAFKA_KRAFT_CLUSTER_ID=abcdefghijklmnopqrstuv",
                "-e",
                "KAFKA_CFG_NODE_ID=0",
                "-e",
                "KAFKA_CFG_PROCESS_ROLES=broker,controller",
                "-e",
                "KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@localhost:9093",
                "-e",
                "KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093",
                "-e",
                "KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092",
                "-e",
                "KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
                "-e",
                "KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER",
                "-e",
                "KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
                "-e",
                "KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true",
                "-e",
                "KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
                "-e",
                "KAFKA_CFG_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
                "-e",
                "KAFKA_CFG_TRANSACTION_STATE_LOG_MIN_ISR=1",
                DEFAULT_IMAGE,
            ])
            .output()
            .ok()
    };
    match status {
        Some(out) if out.status.success() => {
            let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
            info!(container_id = %id, took_ms = run_start.elapsed().as_millis(), "Started Kafka container");
            state.id = Some(id);
            state.bootstrap = Some("localhost:9092".into());
            state.launched = true;
            state.bootstrap.clone()
        }
        _ => {
            info!("Failed to start Kafka test container; falling back to external localhost:9092");
            None
        }
    }
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

/// Timing data returned to tests for diagnostics
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct SetupTimings {
    pub total_ms: u128,
    pub ensure_container_ms: u128,
    pub wait_ready_ms: u128,
    pub connect_ms: u128,
    pub metadata_ms: u128,
}

/// Robust metadata readiness loop; returns (ok, elapsed_ms)
static METADATA_READY: Lazy<std::sync::atomic::AtomicBool> =
    Lazy::new(|| std::sync::atomic::AtomicBool::new(false));

fn wait_metadata(bootstrap: &str, max_wait: Duration) -> (bool, u128) {
    use rdkafka::config::ClientConfig;
    let span = info_span!("kafka_container.wait_metadata", bootstrap = bootstrap);
    let _g = span.enter();
    let start = Instant::now();
    let mut attempt: u32 = 0;
    let mut last_err: Option<String> = None;
    let mut backoff = Duration::from_millis(60);
    while start.elapsed() < max_wait {
        attempt += 1;
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", bootstrap);
        if let Ok(producer) = cfg.create::<rdkafka::producer::BaseProducer>() {
            match producer
                .client()
                .fetch_metadata(None, Duration::from_millis(600))
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
                Err(e) => {
                    let msg = e.to_string();
                    // Only escalate to warn after 3 distinct attempts or when >50% of max_wait elapsed
                    let escalate = attempt >= 3 || start.elapsed() > max_wait / 2;
                    if last_err.as_ref() != Some(&msg) {
                        if escalate {
                            warn!(attempt, err = %msg, "Metadata attempt failed");
                        } else {
                            debug!(attempt, err = %msg, "Metadata attempt failed");
                        }
                    }
                    last_err = Some(msg);
                }
            }
        }
        std::thread::sleep(backoff);
        // exponential backoff capped
        backoff = std::cmp::min(backoff * 2, Duration::from_millis(500));
    }
    warn!(
        attempts = attempt,
        elapsed_ms = start.elapsed().as_millis(),
        "Metadata NOT ready before timeout"
    );
    (false, start.elapsed().as_millis())
}

/// Ensure Kafka is fully ready (TCP + metadata) before tests proceed. Returns elapsed ms.
#[allow(dead_code)]
pub fn ensure_kafka_ready(bootstrap: &str) -> u128 {
    // Already did TCP wait in setup path; here we re-verify quickly and then demand metadata.
    let meta_wait_cap = Duration::from_secs(10); // allow up to 10s in CI / cold pull
    let (ok, elapsed) = wait_metadata(bootstrap, meta_wait_cap);
    if !ok {
        panic!(
            "Kafka metadata not ready after {}ms for {}",
            elapsed, bootstrap
        );
    }
    elapsed
}

pub fn setup() -> (KafkaEventBusBackend, String, SetupTimings) {
    let total_start = Instant::now();
    let ensure_start = Instant::now();
    let container_bootstrap = ensure_container();
    let ensure_ms = ensure_start.elapsed().as_millis();
    let bootstrap = container_bootstrap.unwrap_or_else(|| {
        std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".into())
    });

    bevy_event_bus::runtime(); // initialize shared runtime early

    let wait_start = Instant::now();
    if !wait_ready(&bootstrap) {
        panic!(
            "Kafka not TCP ready at {}. Ensure docker is running or set KAFKA_BOOTSTRAP_SERVERS.",
            bootstrap
        );
    }
    let wait_ms = wait_start.elapsed().as_millis();

    // Require metadata readiness once per test process; subsequent setup() calls skip wait.
    let mut metadata_ms = 0u128;
    if !METADATA_READY.load(std::sync::atomic::Ordering::SeqCst) {
        // Allow more generous window for fresh Kafka container internal initialization (KRaft can take >10s cold)
        let (metadata_ok, elapsed) = wait_metadata(&bootstrap, Duration::from_secs(15));
        metadata_ms = elapsed;
        if !metadata_ok {
            panic!(
                "Kafka metadata not ready at {} after {}ms",
                bootstrap, elapsed
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
    let config = KafkaConfig {
        bootstrap_servers: bootstrap.clone(),
        group_id: format!("bevy_event_bus_test_{}_{}", std::process::id(), unique),
        client_id: Some(format!("bevy_event_bus_test_client_{}", unique)),
        timeout_ms: 3000, // modest timeout
        additional_config: Default::default(),
    };

    let connect_start = Instant::now();
    let backend = KafkaEventBusBackend::new(config);
    let connect_ms = connect_start.elapsed().as_millis();

    let timings = SetupTimings {
        total_ms: total_start.elapsed().as_millis(),
        ensure_container_ms: ensure_ms,
        wait_ready_ms: wait_ms,
        connect_ms,
        metadata_ms,
    };
    info!(?timings, "Kafka test setup complete (ready)");
    (backend, bootstrap, timings)
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

/// Build a baseline Bevy `App` with the event bus plugins wired to a fresh Kafka backend from `setup()`.
/// An optional customization closure can further configure the `App` (e.g., inserting resources, systems).
/// This centralizes construction so tests share identical initialization semantics.
pub fn build_basic_app<F>(customize: F) -> bevy::prelude::App
where
    F: FnOnce(&mut bevy::prelude::App),
{
    let (backend, _bootstrap, _timings) = setup();
    let mut app = bevy::prelude::App::new();
    app.add_plugins(bevy_event_bus::EventBusPlugins(
        backend,
        bevy_event_bus::PreconfiguredTopics::new(["default_topic"]),
    ));
    customize(&mut app);
    app
}

/// Convenience overload when no customization is needed.
pub fn build_basic_app_simple() -> bevy::prelude::App {
    build_basic_app(|_| {})
}
