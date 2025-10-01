use bevy::prelude::*;
use bevy_event_bus::backends::EventBusBackend;
use bevy_event_bus::config::kafka::KafkaTopologyBuilder;
use bevy_event_bus::{
    KafkaBackendConfig, KafkaConnectionConfig, KafkaConsumerConfig, KafkaProducerConfig,
};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Default bootstrap servers used across tests when a specific endpoint is not required.
pub const DEFAULT_KAFKA_BOOTSTRAP: &str = "localhost:9092";

/// Construct a `KafkaProducerConfig` using common defaults for tests.
pub fn kafka_producer_config<I, T>(bootstrap_servers: &str, topics: I) -> KafkaProducerConfig
where
    I: IntoIterator<Item = T>,
    T: Into<String>,
{
    KafkaProducerConfig::new(bootstrap_servers, topics)
}

/// Construct a `KafkaConsumerConfig` using common defaults for tests.
pub fn kafka_consumer_config<I, T>(
    bootstrap_servers: &str,
    consumer_group: impl Into<String>,
    topics: I,
) -> KafkaConsumerConfig
where
    I: IntoIterator<Item = T>,
    T: Into<String>,
{
    KafkaConsumerConfig::new(bootstrap_servers, consumer_group, topics)
}

/// Build a `KafkaBackendConfig` for tests with customizable topology.
pub fn kafka_backend_config_for_tests<F>(
    bootstrap_servers: &str,
    client_id: Option<String>,
    configure_topology: F,
) -> KafkaBackendConfig
where
    F: FnOnce(&mut KafkaTopologyBuilder),
{
    let mut connection = KafkaConnectionConfig::new(bootstrap_servers);
    if let Some(id) = client_id {
        connection = connection.set_client_id(id);
    }
    connection = connection.set_timeout_ms(5000);

    let mut builder = KafkaTopologyBuilder::default();
    configure_topology(&mut builder);

    KafkaBackendConfig::new(connection, builder.build(), Duration::from_secs(1))
}

fn next_unique_suffix() -> String {
    static UNIQUE_COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_nanos();
    format!("{}-{}-{}", process::id(), nanos, counter)
}

/// Generate a unique topic name per test invocation.
pub fn unique_topic(base: &str) -> String {
    format!("{}-{}", base, next_unique_suffix())
}

/// Generate a unique consumer group identifier for Kafka-backed tests.
pub fn unique_consumer_group(base: &str) -> String {
    format!("{}-{}", base, next_unique_suffix())
}

/// Spin update frames on a reader app until predicate true or timeout (ms). Returns (success, frames_run).
pub fn update_until(
    reader_app: &mut App,
    timeout_ms: u64,
    mut predicate: impl FnMut(&mut App) -> bool,
) -> (bool, u32) {
    let start = Instant::now();
    let mut frames = 0u32;
    while start.elapsed() < Duration::from_millis(timeout_ms) {
        reader_app.update();
        frames += 1;
        if predicate(reader_app) {
            return (true, frames);
        }
        std::thread::sleep(Duration::from_millis(15));
    }
    (false, frames)
}

/// Convenience: wait for exact count with optional strict ordering check function.
pub fn wait_for_events<T>(
    app: &mut App,
    topic_desc: &str,
    timeout_ms: u64,
    target: usize,
    mut fetch: impl FnMut(&mut App) -> Vec<T>,
) -> Vec<T> {
    let start = Instant::now();
    let mut last_len = 0usize;
    loop {
        app.update();
        let items = fetch(app);
        let len = items.len();
        if len != last_len {
            tracing::info!(topic=%topic_desc, len, target, "wait_for_events progress");
            last_len = len;
        }
        if len >= target {
            return items;
        }
        if start.elapsed() > Duration::from_millis(timeout_ms) {
            return items;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// Wait for a condition to be met by polling with exponential backoff.
/// Returns true if condition was met, false if timeout occurred.
pub async fn wait_for_condition<F, Fut>(
    mut condition: F,
    timeout_ms: u64,
    initial_delay_ms: u64,
    max_delay_ms: u64,
) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    let mut delay = initial_delay_ms;

    while start.elapsed() < Duration::from_millis(timeout_ms) {
        if condition().await {
            return true;
        }

        tokio::time::sleep(Duration::from_millis(delay)).await;
        delay = (delay * 2).min(max_delay_ms);
    }
    false
}

/// Wait for consumer group to be ready by checking if it can receive messages.
pub async fn wait_for_consumer_group_ready<B>(
    backend: &B,
    topic: &str,
    group_id: &str,
    timeout_ms: u64,
) -> bool
where
    B: EventBusBackend,
{
    let topic = topic.to_string();
    let group_id = group_id.to_string();

    wait_for_condition(
        || {
            let topic = topic.clone();
            let group_id = group_id.clone();
            async move { backend.get_consumer_lag(&topic, &group_id).await.is_ok() }
        },
        timeout_ms,
        100,  // Start with 100ms delay
        1000, // Max 1000ms delay
    )
    .await
}

/// Wait for messages to be available in a consumer group.
pub async fn wait_for_messages_in_group<B>(
    backend: &B,
    topic: &str,
    group_id: &str,
    min_count: usize,
    timeout_ms: u64,
) -> Vec<Vec<u8>>
where
    B: EventBusBackend,
{
    let start = Instant::now();
    let mut last_count = 0;

    loop {
        let messages = backend.receive_serialized_with_group(topic, group_id).await;
        let count = messages.len();

        if count != last_count {
            tracing::info!(
                topic,
                group_id,
                count,
                min_count,
                "wait_for_messages_in_group progress"
            );
            last_count = count;
        }

        if count >= min_count {
            return messages;
        }

        if start.elapsed() > Duration::from_millis(timeout_ms) {
            return messages;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Run app updates in a loop without fixed sleeps, using minimal delays.
pub fn run_app_updates(app: &mut App, iterations: u32) {
    for _ in 0..iterations {
        app.update();
        // Yield to prevent busy waiting without a fixed delay
        std::thread::yield_now();
    }
}
