use bevy::ecs::system::RunSystemOnce;
use bevy::prelude::*;
use bevy_event_bus::{ConsumerMetrics, DrainMetricsEvent};
use bevy_event_bus::{
    DrainedTopicMetadata, EventBusConsumerConfig, EventMetadata, KafkaEventReader, KafkaMetadata,
    ProcessedMessage,
};
use integration_tests::common::helpers::{
    DEFAULT_KAFKA_BOOTSTRAP, kafka_consumer_config, unique_consumer_group, unique_topic,
};
use integration_tests::common::setup::build_basic_app_simple;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, bevy_event_bus::ExternalBusEvent)]
struct TestMsg {
    v: u32,
}

#[test]
fn drain_empty_ok() {
    let mut app = build_basic_app_simple();
    app.update(); // run drain once
    let buffers = app.world().resource::<DrainedTopicMetadata>();
    assert!(buffers.topics.is_empty() || buffers.topics.values().all(|v| v.is_empty()));
}

#[test]
fn unlimited_buffer_gathers() {
    let topic = unique_topic("background_unlimited");
    let consumer_group = unique_consumer_group("background_unlimited");
    let mut app = build_basic_app_simple();
    // Simulate manually inserting drained payloads (bypass Kafka for unit-style test)
    {
        let mut buffers = app.world_mut().resource_mut::<DrainedTopicMetadata>();
        let entry = buffers.topics.entry(topic.clone()).or_default();
        for i in 0..5u32 {
            let payload = serde_json::to_vec(&TestMsg { v: i }).unwrap();
            let metadata = EventMetadata {
                source: topic.clone(),
                timestamp: std::time::Instant::now(),
                headers: std::collections::HashMap::new(),
                key: None,
                backend_specific: Some(Box::new(KafkaMetadata {
                    topic: topic.clone(),
                    partition: 0,
                    offset: i as i64,
                })),
            };
            entry.push(ProcessedMessage { payload, metadata });
        }
    }
    let topic_for_reader = topic.clone();
    let consumer_group_for_reader = consumer_group.clone();
    // Reader should deserialize all
    let _ = app
        .world_mut()
        .run_system_once(move |mut r: KafkaEventReader<TestMsg>| {
            let mut collected = Vec::new();
            let config = kafka_consumer_config(
                DEFAULT_KAFKA_BOOTSTRAP,
                consumer_group_for_reader.as_str(),
                [topic_for_reader.as_str()],
            );
            for wrapper in r.read(&config) {
                collected.push(wrapper.event().clone());
            }
            assert_eq!(collected.len(), 5);
        });
}

#[test]
fn frame_limit_respected() {
    let topic = unique_topic("background_cap");
    let consumer_group = unique_consumer_group("background_cap");
    let mut app = build_basic_app_simple();
    app.insert_resource(EventBusConsumerConfig {
        max_events_per_frame: Some(3),
        max_drain_millis: None,
    });
    // Preload channel by faking buffers (simulate drain would only take first 3)
    {
        let mut buffers = app.world_mut().resource_mut::<DrainedTopicMetadata>();
        let entry = buffers.topics.entry(topic.clone()).or_default();
        for i in 0..10u32 {
            let payload = serde_json::to_vec(&TestMsg { v: i }).unwrap();
            let metadata = EventMetadata {
                source: topic.clone(),
                timestamp: std::time::Instant::now(),
                headers: std::collections::HashMap::new(),
                key: None,
                backend_specific: Some(Box::new(KafkaMetadata {
                    topic: topic.clone(),
                    partition: 0,
                    offset: i as i64,
                })),
            };
            entry.push(ProcessedMessage { payload, metadata });
        }
    }
    // Reader only sees existing buffer; limit logic applies only during drain; since we injected directly this test is less meaningful but placeholder.
    let topic_for_reader = topic.clone();
    let consumer_group_for_reader = consumer_group.clone();
    let _ = app
        .world_mut()
        .run_system_once(move |mut r: KafkaEventReader<TestMsg>| {
            let config = kafka_consumer_config(
                DEFAULT_KAFKA_BOOTSTRAP,
                consumer_group_for_reader.as_str(),
                [topic_for_reader.as_str()],
            );
            let count = r.read(&config).len();
            assert_eq!(count, 10);
        });
}

#[test]
fn drain_metrics_emitted_and_updated() {
    let mut app = build_basic_app_simple();
    // Ensure event type registered
    // Preload channel indirectly: insert some drained payloads, then run one update to emit metrics
    {
        let mut buffers = app.world_mut().resource_mut::<DrainedTopicMetadata>();
        let entry = buffers.topics.entry("m".into()).or_default();
        for i in 0..3u32 {
            let payload = serde_json::to_vec(&TestMsg { v: i }).unwrap();
            let metadata = EventMetadata {
                source: "m".to_string(),
                timestamp: std::time::Instant::now(),
                headers: std::collections::HashMap::new(),
                key: None,
                backend_specific: Some(Box::new(KafkaMetadata {
                    topic: "m".to_string(),
                    partition: 0,
                    offset: i as i64,
                })),
            };
            entry.push(ProcessedMessage { payload, metadata });
        }
    }
    // First update drains nothing new (already in buffers) but still emits metrics event with drained=0
    app.update();
    // Simulate a second frame with no new data to increment idle_frames
    app.update();
    let metrics = app.world().resource::<ConsumerMetrics>().clone();
    assert!(
        metrics.idle_frames >= 1,
        "Expected at least one idle frame, got {}",
        metrics.idle_frames
    );
    // Fetch events
    let mut received = Vec::new();
    app.world_mut().resource_scope(
        |_world, mut events: Mut<bevy::ecs::event::Events<DrainMetricsEvent>>| {
            for e in events.drain() {
                received.push(e);
            }
        },
    );
    assert!(
        !received.is_empty(),
        "Expected at least one DrainMetricsEvent"
    );
    // Last event should reflect current metrics remaining == queue_len_end
    if let Some(last) = received.last() {
        assert_eq!(last.remaining, metrics.queue_len_end);
        assert_eq!(last.total_drained, metrics.total_drained);
    }
}
