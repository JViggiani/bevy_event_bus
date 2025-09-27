use bevy::prelude::*;
use std::time::{Duration, Instant};

/// Generate a unique topic name per test invocation (Kafka-compatible naming)
pub fn unique_topic(base: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    // Extract numeric part from thread ID for Kafka-compatible naming
    let thread_id = std::thread::current().id();
    let thread_id_str = format!("{:?}", thread_id);
    // Extract just the number from "ThreadId(123)" -> "123"
    let thread_num = thread_id_str
        .strip_prefix("ThreadId(")
        .and_then(|s| s.strip_suffix(")"))
        .unwrap_or("0");
    format!("{}-{}-{}", base, thread_num, nanos)
}

/// Generate a unique consumer group name per test invocation
pub fn unique_consumer_group(base: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    // Extract numeric part from thread ID for consumer group naming
    let thread_id = std::thread::current().id();
    let thread_id_str = format!("{:?}", thread_id);
    // Extract just the number from "ThreadId(123)" -> "123"
    let thread_num = thread_id_str
        .strip_prefix("ThreadId(")
        .and_then(|s| s.strip_suffix(")"))
        .unwrap_or("0");
    format!("{}-{}-{}", base, thread_num, nanos)
}

/// Generate a unique string (legacy function - prefer unique_topic or unique_consumer_group)
pub fn unique_string(base: &str) -> String {
    unique_topic(base)
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

/// Wait for consumer group to be properly initialized by checking if messages can be consumed.
/// This is more reliable than arbitrary sleeps after consumer group creation.
pub async fn wait_for_consumer_group_ready<T>(
    backend: &mut bevy_event_bus::backends::KafkaEventBusBackend,
    config: &bevy_event_bus::KafkaReadConfig,
    timeout_ms: u64,
) -> bool 
where
    T: bevy_event_bus::BusEvent,
{
    let start = Instant::now();
    let poll_interval = Duration::from_millis(50);
    
    while start.elapsed() < Duration::from_millis(timeout_ms) {
        // Try to poll - if consumer group is ready, this should succeed without error
        let mut messages = Vec::new();
        match backend.receive::<T>(config, &mut messages).await {
            Ok(_) => {
                tracing::info!("Consumer group '{}' is ready", config.consumer_group());
                return true;
            }
            Err(e) => {
                tracing::debug!("Consumer group not ready yet: {}", e);
                tokio::time::sleep(poll_interval).await;
            }
        }
    }
    
    tracing::warn!("Consumer group '{}' not ready after {}ms", config.consumer_group(), timeout_ms);
    false
}

/// Poll for messages with condition-based waiting instead of hardcoded sleeps.
/// Returns when target number of messages received or timeout reached.
pub async fn wait_for_messages<T>(
    backend: &mut bevy_event_bus::backends::KafkaEventBusBackend,
    config: &bevy_event_bus::KafkaReadConfig,
    target_count: usize,
    timeout_ms: u64,
) -> Vec<T>
where
    T: bevy_event_bus::BusEvent,
{
    let start = Instant::now();
    let mut all_messages = Vec::new();
    let poll_interval = Duration::from_millis(50);
    let mut last_len = 0;
    
    while start.elapsed() < Duration::from_millis(timeout_ms) {
        let mut batch = Vec::new();
        
        match backend.receive::<T>(config, &mut batch).await {
            Ok(_) => {
                // Extract events from EventWrapper instances
                for wrapper in batch {
                    all_messages.push(wrapper.into_event());
                }
                
                // Log progress when we get new messages
                if all_messages.len() != last_len {
                    tracing::info!(
                        "wait_for_messages progress: {}/{} messages for group '{}'",
                        all_messages.len(),
                        target_count,
                        config.consumer_group()
                    );
                    last_len = all_messages.len();
                }
                
                if all_messages.len() >= target_count {
                    tracing::info!(
                        "Target reached: {}/{} messages for group '{}'",
                        all_messages.len(),
                        target_count,
                        config.consumer_group()
                    );
                    return all_messages;
                }
            }
            Err(e) => {
                tracing::debug!("Polling error (will retry): {}", e);
            }
        }
        
        tokio::time::sleep(poll_interval).await;
    }
    
    tracing::warn!(
        "Timeout reached: {}/{} messages for group '{}' after {}ms", 
        all_messages.len(), 
        target_count,
        config.consumer_group(),
        timeout_ms
    );
    all_messages
}

/// Wait for a condition to be true with exponential backoff, avoiding hardcoded sleeps
pub async fn wait_for_condition<F>(
    mut condition: F,
    timeout_ms: u64,
    initial_delay_ms: u64,
) -> bool
where
    F: FnMut() -> bool,
{
    let start = Instant::now();
    let mut delay = Duration::from_millis(initial_delay_ms);
    let max_delay = Duration::from_millis(200);
    
    while start.elapsed() < Duration::from_millis(timeout_ms) {
        if condition() {
            return true;
        }
        
        tokio::time::sleep(delay).await;
        
        // Exponential backoff with cap
        delay = std::cmp::min(delay * 2, max_delay);
    }
    
    false
}


