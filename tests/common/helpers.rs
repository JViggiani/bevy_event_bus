use bevy::prelude::*;
use bevy_event_bus::{EventBusPlugins, EventBusWriter, EventBusReader};
use crate::common::setup::{setup};
use std::time::{Duration, Instant};

/// Generate a unique topic name per test invocation
pub fn unique_topic(base: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("{}-{}", base, nanos)
}

/// Poll an app until predicate returns true or timeout
pub fn poll_until<F>(app: &mut App, timeout: Duration, mut predicate: F) -> bool
where
    F: FnMut(&mut App) -> bool,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        app.update();
        if predicate(app) { return true; }
        std::thread::sleep(Duration::from_millis(60));
    }
    false
}

/// Build a pair (writer, reader) apps with independent backends (separate consumer groups)
pub fn build_writer_reader_pair() -> (App, App) {
    let (backend_writer, _b1, _t1) = setup();
    let (backend_reader, _b2, _t2) = setup();
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend_writer, bevy_event_bus::PreconfiguredTopics::new(["w_default"])));
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(backend_reader, bevy_event_bus::PreconfiguredTopics::new(["r_default"])));
    (writer, reader)
}

/// Send a batch of events implementing Clone + serde via EventBusWriter
pub fn send_batch<T>(writer_app: &mut App, topic: &str, events: &[T])
where
    T: Clone + Send + Sync + 'static + bevy::prelude::Event + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    struct Batch<T>(String, Vec<T>);
    impl<T: Clone + Send + Sync + 'static> Resource for Batch<T> {}
    writer_app.world_mut().insert_resource(Batch(topic.to_string(), events.to_vec()));
    fn sys<T>(mut w: EventBusWriter<T>, batch: Res<Batch<T>>)
    where
        T: Clone + Send + Sync + 'static + bevy::prelude::Event + serde::Serialize + for<'de> serde::Deserialize<'de>,
    {
        for e in batch.1.iter() { let _ = w.send(&batch.0, e.clone()); }
    }
    writer_app.add_systems(Update, sys::<T>);
    writer_app.update();
    writer_app.world_mut().remove_resource::<Batch<T>>();
}

/// Collect all currently available events of type T for a topic
pub fn collect_now<T>(reader_app: &mut App, topic: &str) -> Vec<T>
where
    T: Clone + Send + Sync + 'static + bevy::prelude::Event + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    struct Collector<T>(String, Vec<T>);
    impl<T: Send + Sync + 'static> Resource for Collector<T> {}
    reader_app.world_mut().insert_resource(Collector::<T>(topic.to_string(), Vec::new()));
    fn sys<T>(mut r: EventBusReader<T>, mut c: ResMut<Collector<T>>)
    where
        T: Clone + Send + Sync + 'static + bevy::prelude::Event + serde::Serialize + for<'de> serde::Deserialize<'de>,
    {
        for ev in r.try_read(&c.0) { c.1.push(ev.clone()); }
    }
    reader_app.add_systems(Update, sys::<T>);
    reader_app.update();
    let collected = reader_app.world_mut().remove_resource::<Collector<T>>().unwrap().1;
    collected
}

/// Spin update frames on a reader app until predicate true or timeout (ms). Returns (success, frames_run).
pub fn update_until(reader_app: &mut App, timeout_ms: u64, mut predicate: impl FnMut(&mut App) -> bool) -> (bool, u32) {
    let start = Instant::now();
    let mut frames = 0u32;
    while start.elapsed() < Duration::from_millis(timeout_ms) {
        reader_app.update();
        frames += 1;
        if predicate(reader_app) { return (true, frames); }
        std::thread::sleep(Duration::from_millis(25));
    }
    (false, frames)
}
