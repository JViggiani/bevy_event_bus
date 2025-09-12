//! Test setup utilities. Currently placeholder for docker CLI based Kafka spin-up.
//! The previous testcontainers-based approach was removed for determinism and reduced flakiness.
//! TODO: Implement a small helper that:
//! 1. Invokes `docker run -d -p 9092:9092 bitnami/kafka:latest ...` if a container isn't already running.
//! 2. Waits for readiness by polling metadata using rdkafka.
//! 3. Exposes a simple `setup()` returning a configured `KafkaEventBusBackend`.
//! For now the integration test assumes an external Kafka is available on localhost:9092.

use bevy_event_bus::{KafkaEventBusBackend, KafkaConfig, EventBusBackend};
use std::process::Command;
use std::time::{Duration, Instant};

const DEFAULT_IMAGE: &str = "bitnami/kafka:latest";
const CONTAINER_NAME: &str = "bevy_event_bus_test_kafka";

fn docker_available() -> bool { Command::new("docker").arg("version").output().map(|o| o.status.success()).unwrap_or(false) }

fn ensure_container() -> Option<String> {
    if !docker_available() { return None; }
    // If user provides external bootstrap, skip starting container
    if std::env::var("KAFKA_BOOTSTRAP_SERVERS").is_ok() { return None; }

    // Check if container already running
    let ps = Command::new("docker").args(["ps","--filter", &format!("name={}", CONTAINER_NAME), "--format","{{.ID}}"]).output().ok();
    if let Some(out) = ps { if !out.stdout.is_empty() { return Some("localhost:9092".into()); } }

    // Run container (KRaft single node)
    // Try pull (ignore failures)
    let _ = Command::new("docker").args(["pull", DEFAULT_IMAGE]).status();
    let status = Command::new("docker").args([
        "run","-d","--rm","--name", CONTAINER_NAME,
        "-p","9092:9092","-p","9093:9093",
        "-e","KAFKA_ENABLE_KRAFT=yes",
        "-e","KAFKA_KRAFT_CLUSTER_ID=abcdefghijklmnopqrstuv",
        "-e","KAFKA_CFG_NODE_ID=0",
        "-e","KAFKA_CFG_PROCESS_ROLES=broker,controller",
        "-e","KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@localhost:9093",
        "-e","KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093",
        "-e","KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092",
        "-e","KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "-e","KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER",
        "-e","KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
        "-e","KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true",
        "-e","KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
        "-e","KAFKA_CFG_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
        "-e","KAFKA_CFG_TRANSACTION_STATE_LOG_MIN_ISR=1",
        DEFAULT_IMAGE
    ]).status().ok();
    if status.map(|s| s.success()).unwrap_or(false) { Some("localhost:9092".into()) } else { None }
}

fn wait_ready(bootstrap: &str) -> bool {
    use std::net::TcpStream;
    let start = Instant::now();
    let timeout = Duration::from_secs(40);
    while start.elapsed() < timeout {
        if TcpStream::connect(bootstrap).is_ok() { return true; }
        std::thread::sleep(Duration::from_millis(500));
    }
    false
}

pub fn setup() -> (KafkaEventBusBackend, String) {
    let bootstrap = ensure_container().unwrap_or_else(|| std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".into()));
    bevy_event_bus::runtime(); // initialize runtime
    if !wait_ready(&bootstrap) {
        panic!("Kafka not ready at {}. Ensure docker is running or set KAFKA_BOOTSTRAP_SERVERS.", bootstrap);
    }
    let config = KafkaConfig {
        bootstrap_servers: bootstrap.clone(),
        group_id: format!("bevy_event_bus_test_{}", std::process::id()),
        client_id: Some("bevy_event_bus_test_client".into()),
        timeout_ms: 5000,
        additional_config: Default::default(),
    };
    let mut backend = KafkaEventBusBackend::new(config);
    // Connect once so first send works quickly
    if let Err(e) = backend.connect() {
        panic!("Kafka connect_error: {:?}", e);
    }
    (backend, bootstrap)
}
