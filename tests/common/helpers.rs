use bevy::prelude::*;
use std::time::{Duration, Instant};

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
