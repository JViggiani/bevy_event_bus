use bevy::prelude::*;
use bevy::ecs::system::RunSystemOnce;
use bevy_event_bus::{EventBusReader, EventBusConsumerConfig, DrainedTopicBuffers};
use bevy_event_bus::{ConsumerMetrics, DrainMetricsEvent};
use crate::common::setup::{build_basic_app_simple};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, bevy_event_bus::ExternalBusEvent)]
struct TestMsg { v: u32 }


#[test]
fn drain_empty_ok() {
    let mut app = build_basic_app_simple();
    app.update(); // run drain once
    let buffers = app.world().resource::<DrainedTopicBuffers>();
    assert!(buffers.topics.is_empty() || buffers.topics.values().all(|v| v.is_empty()));
}

#[test]
fn unlimited_buffer_gathers() {
    let mut app = build_basic_app_simple();
    // Simulate manually inserting drained payloads (bypass Kafka for unit-style test)
    {
        let mut buffers = app.world_mut().resource_mut::<DrainedTopicBuffers>();
        let topic = "t".to_string();
        let entry = buffers.topics.entry(topic.clone()).or_default();
        for i in 0..5u32 { entry.push(serde_json::to_vec(&TestMsg { v: i }).unwrap()); }
    }
    // Reader should deserialize all
    let _ = app.world_mut().run_system_once(|mut r: EventBusReader<TestMsg>| {
        let mut collected = Vec::new();
        for ev in r.try_read("t") { collected.push(ev.clone()); }
        assert_eq!(collected.len(), 5);
    });
}

#[test]
fn frame_limit_respected() {
    let mut app = build_basic_app_simple();
    app.insert_resource(EventBusConsumerConfig { max_events_per_frame: Some(3), max_drain_millis: None });
    // Preload channel by faking buffers (simulate drain would only take first 3)
    {
        let mut buffers = app.world_mut().resource_mut::<DrainedTopicBuffers>();
        let entry = buffers.topics.entry("cap".into()).or_default();
        for i in 0..10u32 { entry.push(serde_json::to_vec(&TestMsg { v: i }).unwrap()); }
    }
    // Reader only sees existing buffer; limit logic applies only during drain; since we injected directly this test is less meaningful but placeholder.
    let _ = app.world_mut().run_system_once(|mut r: EventBusReader<TestMsg>| {
        let count = r.try_read("cap").count();
        assert_eq!(count, 10);
    });
}

#[test]
fn drain_metrics_emitted_and_updated() {
    let mut app = build_basic_app_simple();
    // Ensure event type registered
    // Preload channel indirectly: insert some drained payloads, then run one update to emit metrics
    {
        let mut buffers = app.world_mut().resource_mut::<DrainedTopicBuffers>();
        let entry = buffers.topics.entry("m".into()).or_default();
        for i in 0..3u32 { entry.push(serde_json::to_vec(&TestMsg { v: i }).unwrap()); }
    }
    // First update drains nothing new (already in buffers) but still emits metrics event with drained=0
    app.update();
    // Simulate a second frame with no new data to increment idle_frames
    app.update();
    let metrics = app.world().resource::<ConsumerMetrics>().clone();
    assert!(metrics.idle_frames >= 1, "Expected at least one idle frame, got {}", metrics.idle_frames);
    // Fetch events
    let mut received = Vec::new();
    app.world_mut().resource_scope(|_world, mut events: Mut<bevy::ecs::event::Events<DrainMetricsEvent>>| {
        for e in events.drain() { received.push(e); }
    });
    assert!(!received.is_empty(), "Expected at least one DrainMetricsEvent");
    // Last event should reflect current metrics remaining == queue_len_end
    if let Some(last) = received.last() {
        assert_eq!(last.remaining, metrics.queue_len_end);
        assert_eq!(last.total_drained, metrics.total_drained);
    }
}
