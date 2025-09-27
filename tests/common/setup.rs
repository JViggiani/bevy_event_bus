//! Test setup utilities. Currently placeholder for docker CLI based Kafka spin-up.
//! The previous testcontainers-based approach was removed for determinism and reduced flakiness.
//! TODO: Implement a small helper that:
//! 1. Invokes `docker run -d -p 9092:9092 bitnami/kafka:latest ...` if a container isn't already running.
//! 2. Waits for readiness by polling metadata using rdkafka.
//! 3. Exposes a simple `setup()` returning a configured `KafkaEventBusBackend`.
//! For now the integration test assumes an external Kafka is available on localhost:9092.

use bevy_event_bus::{KafkaConnection, KafkaEventBusBackend};
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

/// Holds lifecycle data for the ephemeral Kafka container used in tests
#[derive(Default, Debug, Clone)]
struct ContainerState {
    id: Option<String>,
    bootstrap: Option<String>,
    launched: bool,
    container_name: String,
    kafka_port: u16,
    controller_port: u16,
}

impl ContainerState {
    fn new() -> Self {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let container_name = format!("bevy_event_bus_test_kafka_{}_{}", pid, nanos);
        
        // Find available ports
        let kafka_port = find_free_port(9092);
        let controller_port = find_free_port(9093);
        
        Self {
            id: None,
            bootstrap: None,
            launched: false,
            container_name,
            kafka_port,
            controller_port,
        }
    }
}

fn find_free_port(start_port: u16) -> u16 {
    for port in start_port..(start_port + 1000) {
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    panic!("Could not find free port starting from {}", start_port);
}

static CONTAINER_STATE: Lazy<Mutex<ContainerState>> =
    Lazy::new(|| Mutex::new(ContainerState::new()));

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
            &format!("name={}", state.container_name),
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
            let bootstrap = format!("localhost:{}", state.kafka_port);
            state.bootstrap = Some(bootstrap.clone());
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
    let kafka_port_mapping = format!("{}:9092", state.kafka_port);
    let controller_port_mapping = format!("{}:9093", state.controller_port);
    let advertised_listeners = format!("KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:{}", state.kafka_port);
    let controller_voters = format!("KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@localhost:{}", state.controller_port);
    let status = {
        let _rg = run_span.enter();
        Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm",
                "--name",
                &state.container_name,
                "-p",
                &kafka_port_mapping,
                "-p",
                &controller_port_mapping,
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
                &controller_voters,
                "-e",
                "KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093",
                "-e",
                &advertised_listeners,
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
            info!(container_id = %id, port = %state.kafka_port, took_ms = run_start.elapsed().as_millis(), "Started Kafka container");
            state.id = Some(id);
            let bootstrap = format!("localhost:{}", state.kafka_port);
            state.bootstrap = Some(bootstrap.clone());
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
    // Allow reasonable time for Kafka container to start up
    let timeout = Duration::from_secs(15);
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
    let _guard = span.enter();
    let start = Instant::now();
    let mut attempt: u32 = 0;
    
    while start.elapsed() < max_wait {
        attempt += 1;
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", bootstrap);
        if let Ok(producer) = cfg.create::<rdkafka::producer::BaseProducer>() {
            match producer.client().fetch_metadata(None, Duration::from_millis(1000)) {
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

/// Setup a Kafka backend with optional configuration.
/// 
/// # Arguments
/// * `offset` - Consumer offset reset behavior ("earliest" or "latest"), defaults to "earliest"
pub fn setup(offset: Option<&str>) -> (KafkaEventBusBackend, String) {
    let offset = offset.unwrap_or("earliest");
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
    
    let config = KafkaConnection {
        bootstrap_servers: bootstrap.clone(),
        client_id: Some(format!("bevy_event_bus_test_client_{}", unique)),
        security_protocol: bevy_event_bus::config::SecurityProtocol::Plaintext,
        timeout_ms: 3000, // modest timeout
        additional_config,
    };

    let backend = KafkaEventBusBackend::new(config);
    info!("Kafka test setup complete (ready)");
    (backend, bootstrap)
}

/// Same as `setup` but allows providing a unique suffix for the consumer group id so that
/// multiple backends in the same test process do not share a group (ensuring each receives all messages).



/// Create a topic and wait for it to be fully ready for read/write operations.
/// Combines topic creation with readiness verification in a single call.
/// Returns true if the topic is ready, false if timeout exceeded.
pub fn ensure_topic_ready(bootstrap: &str, topic: &str, partitions: i32, timeout: Duration) -> bool {
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::producer::{BaseProducer, Producer};
    
    let span = info_span!("kafka_container.ensure_topic_ready", bootstrap = bootstrap, topic = topic);
    let _g = span.enter();
    let start = Instant::now();
    let mut attempts = 0u32;
    
    // First ensure topic exists using Admin API
    {
        let mut cfg = rdkafka::config::ClientConfig::new();
        cfg.set("bootstrap.servers", bootstrap);
        cfg.set("request.timeout.ms", "30000");  // 30 second timeout
        if let Ok(admin) = cfg.create::<AdminClient<DefaultClientContext>>() {
            info!(topic=%topic, "Creating topic with Admin API");
            let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
            let mut opts = AdminOptions::new();
            opts = opts.operation_timeout(Some(std::time::Duration::from_secs(30)));
            let fut = admin.create_topics([&new_topic], &opts);
            match bevy_event_bus::block_on(fut) {
                Ok(results) => {
                    info!(topic=%topic, "Admin API create_topics completed with results: {}", results.len());
                    for result in results {
                        match result {
                            Ok(topic_name) => info!(topic=%topic_name, "Topic created successfully"),
                            Err((topic_name, error_code)) => {
                                let err_msg = format!("{:?}", error_code);
                                if !err_msg.contains("TopicAlreadyExists") {
                                    warn!(topic=%topic_name, err=%err_msg, "Topic creation failed");
                                } else {
                                    info!(topic=%topic_name, "Topic already exists (expected)");
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    let msg = format!("{:?}", e);
                    if !msg.contains("TopicAlreadyExists") {
                        warn!(topic=%topic, err=%msg, "Admin API create_topics failed");
                    } else {
                        info!(topic=%topic, "Topic already exists (expected)");
                    }
                }
            }
        } else {
            warn!(topic=%topic, "ensure_topic could not build admin client");
        }
    }
    
    while start.elapsed() < timeout {
        attempts += 1;
        
        // Test both producer and consumer can see the topic
        let mut producer_cfg = ClientConfig::new();
        producer_cfg.set("bootstrap.servers", bootstrap);
        producer_cfg.set("client.id", &format!("topic_readiness_producer_{}", attempts));
        
        let mut consumer_cfg = ClientConfig::new();
        consumer_cfg.set("bootstrap.servers", bootstrap);
        consumer_cfg.set("group.id", &format!("topic_readiness_consumer_{}", attempts));
        consumer_cfg.set("client.id", &format!("topic_readiness_consumer_{}", attempts));
        consumer_cfg.set("auto.offset.reset", "earliest");
        
        if let (Ok(producer), Ok(consumer)) = (
            producer_cfg.create::<BaseProducer>(),
            consumer_cfg.create::<BaseConsumer>(),
        ) {
            // Check if producer can fetch metadata for this specific topic
            match producer.client().fetch_metadata(Some(topic), Duration::from_millis(1000)) {
                Ok(metadata) => {
                    debug!(attempt = attempts, topics = metadata.topics().len(), "Metadata fetch successful");
                    // Check if topic exists in metadata with at least one partition
                    if let Some(topic_metadata) = metadata.topics().iter().find(|t| t.name() == topic) {
                        debug!(
                            attempt = attempts, 
                            topic = %topic, 
                            partitions = topic_metadata.partitions().len(),
                            error = ?topic_metadata.error(),
                            "Found topic in metadata"
                        );
                        if !topic_metadata.partitions().is_empty() && topic_metadata.error().is_none() {
                            // Topic exists with partitions, now test consumer can subscribe
                            match consumer.subscribe(&[topic]) {
                                Ok(_) => {
                                    info!(
                                        attempts,
                                        elapsed_ms = start.elapsed().as_millis(),
                                        "Topic created and ready for read/write operations"
                                    );
                                    return true;
                                }
                                Err(e) => {
                                    debug!(attempt = attempts, err = %e, "Consumer subscribe failed");
                                }
                            }
                        } else {
                            debug!(
                                attempt = attempts,
                                topic = %topic,
                                partitions = topic_metadata.partitions().len(),
                                error = ?topic_metadata.error(),
                                "Topic not ready (no partitions or has error)"
                            );
                        }
                    } else {
                        debug!(attempt = attempts, topic = %topic, available_topics = ?metadata.topics().iter().map(|t| t.name()).collect::<Vec<_>>(), "Topic not found in metadata");
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

/// Universal Bevy App builder for all test scenarios.
/// 
/// # Arguments
/// * `backend` - The event bus backend to use for this app
/// * `consumer_configs` - Optional consumer group configurations to create during setup
/// * `customize` - Closure to customize the app after basic setup
/// 
/// # Examples
/// ```rust
/// let (backend, _) = setup(None);
/// 
/// // Simple app with no consumer groups
/// let app = build_app(backend, None, |_| {});
/// 
/// // App with single consumer group
/// let (backend, _) = setup(None);
/// let config = KafkaReadConfig::new("group").topics(["topic"]);
/// let app = build_app(backend, Some(&[config]), |app| {
///     app.add_bus_event::<MyEvent>("topic");
/// });
/// ```
pub fn build_app<F>(
    mut backend: KafkaEventBusBackend,
    consumer_configs: Option<&[bevy_event_bus::KafkaReadConfig]>,
    customize: F,
) -> bevy::prelude::App
where
    F: FnOnce(&mut bevy::prelude::App),
{
    // Create consumer groups if specified
    if let Some(configs) = consumer_configs {
        let runtime = bevy_event_bus::runtime();
        for config in configs {
            if let Err(e) = runtime.block_on(backend.create_consumer_group(config)) {
                panic!("Failed to create consumer group '{}' during setup: {}", config.consumer_group(), e);
            }
        }
    }
    
    let mut app = bevy::prelude::App::new();
    app.add_plugins(bevy_event_bus::EventBusPlugins(backend));
    customize(&mut app);
    app
}


