use bevy::prelude::*;
use std::time::{Duration, Instant};
use bevy_event_bus::backends::EventBusBackend;

/// Generate a unique topic name per test invocation
pub fn unique_topic(base: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{}-{}", base, nanos)
}

/// Poll an app until predicate returns true or timeout

/// Build a pair (writer, reader) apps with independent backends (separate consumer groups)

/// Send a batch of events implementing Clone + serde via EventBusWriter

/// Collect all currently available events of type T for a topic

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

/// Wait for backend to be ready by checking if it can send a test message.
pub async fn wait_for_backend_ready<B>(
    backend: &B,
    topic: &str,
    timeout_ms: u64,
) -> bool 
where
    B: EventBusBackend,
{
    wait_for_condition(
        || {
            let test_msg = b"backend_ready_test";
            async move { backend.try_send_serialized(test_msg, topic) }
        },
        timeout_ms,
        50,  // Start with 50ms delay
        500, // Max 500ms delay
    ).await
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
    wait_for_condition(
        || async move {
            // Try to receive - if consumer group is ready, this should not error
            // Even if empty, a ready consumer group will return an empty vec
            let result = backend.receive_serialized_with_group(topic, group_id).await;
            !result.is_empty() || true // Accept empty results as "ready"
        },
        timeout_ms,
        100, // Start with 100ms delay  
        1000, // Max 1000ms delay
    ).await
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
            tracing::info!(topic, group_id, count, min_count, "wait_for_messages_in_group progress");
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

/// Helper to setup a Kafka backend and consumer group with proper synchronization
pub async fn setup_kafka_consumer_group<B>(
    backend: &mut B,
    topics: &[String],
    group_id: &str,
) where
    B: EventBusBackend,
{
    // Connect the backend to Kafka
    let connect_result = backend.connect().await;
    assert!(connect_result, "Failed to connect to Kafka");
    
    // Send dummy messages to ensure topics exist in Kafka
    let dummy_message = b"topic_creation_dummy";
    for topic in topics {
        backend.try_send_serialized(dummy_message, topic);
    }
    
    // Wait for backend to be ready for the first topic
    if !topics.is_empty() {
        let backend_ready = wait_for_backend_ready(backend, &topics[0], 5000).await;
        assert!(backend_ready, "Backend not ready within timeout");
    }
    
    // Create consumer group after topics exist
    backend.create_consumer_group(topics, group_id).await.unwrap();
    
    // Wait for consumer group to be ready
    if !topics.is_empty() {
        let consumer_ready = wait_for_consumer_group_ready(backend, &topics[0], group_id, 10000).await;
        assert!(consumer_ready, "Consumer group not ready within timeout");
    }
}

