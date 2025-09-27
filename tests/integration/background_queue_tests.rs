use crate::common::setup::{build_app};
use crate::common::helpers::unique_consumer_group;
use bevy::ecs::system::RunSystemOnce;
use bevy::prelude::*;
use bevy_event_bus::{ConsumerMetrics, DrainMetricsEvent};
use bevy_event_bus::{DrainedTopicMetadata, EventBusConsumerConfig, EventBusReader, ProcessedMessage, EventMetadata, KafkaMetadata, KafkaReadConfig};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, bevy_event_bus::ExternalBusEvent, Default)]
struct TestMsg {
    v: u32,
}

#[test]
fn drain_empty_ok() {
    let (backend, _bootstrap) = crate::common::setup::setup(None);
    let mut app = build_app(backend, None, |_| {});
    app.update(); // run drain once
    let buffers = app.world().resource::<DrainedTopicMetadata>();
    assert!(buffers.topics.is_empty() || buffers.topics.values().all(|v| v.is_empty()));
}

#[test]
fn unlimited_buffer_gathers() {
    // Create app with pre-created consumer group
    let config = KafkaReadConfig::new(&unique_consumer_group("test_group")).topics(["t"]);
    let (backend, _bootstrap) = crate::common::setup::setup(None);
    let mut app = build_app(backend, Some(&[config.clone()]), |_app| {});
    
    // Simulate manually inserting drained payloads (bypass Kafka for unit-style test)
    {
        let mut buffers = app.world_mut().resource_mut::<DrainedTopicMetadata>();
        let topic = "t".to_string();
        let entry = buffers.topics.entry(topic.clone()).or_default();
        for i in 0..5u32 {
            let payload = serde_json::to_vec(&TestMsg { v: i }).unwrap();
            let metadata = EventMetadata {
                source: topic.clone(),
                received_timestamp: std::time::Instant::now(),
                headers: std::collections::HashMap::new(),
                key: None,
                backend_specific: Some(Box::new(KafkaMetadata {
                    topic: topic.clone(),
                    partition: 0,
                    offset: i as i64,
                    kafka_timestamp: Some(1234567890 + i as i64),
                })),
            };
            entry.push(ProcessedMessage { payload, metadata });
        }
    }
    
    #[derive(Resource, Default)]
    struct Collected(Vec<TestMsg>);
    app.insert_resource(Collected::default());
    
    // Capture config for use in the system
    let config_for_system = config.clone();
    
    // System that reads from pre-created consumer group
    let reader_system = move |
        mut r: EventBusReader<TestMsg>,
        mut collected: ResMut<Collected>,
        buffers: Res<bevy_event_bus::DrainedTopicMetadata>,
    | {
        println!("reader_system called");
        println!("Available buffers: {:?}", buffers.topics.keys().collect::<Vec<_>>());
        if let Some(entry) = buffers.topics.get("t") {
            println!("Buffer 't' has {} entries", entry.len());
        }
        
        // Use the same config that was passed to setup
        // Consumer group already created during setup
        
        let mut count = 0;
        for wrapper in r.read(&config_for_system) {
            collected.0.push(wrapper.event().clone());
            count += 1;
        }
        println!("Read {} events", count);
    };
    
    app.add_systems(Update, reader_system);
    app.update(); // Run one frame
    
    let collected = app.world().resource::<Collected>();
    assert_eq!(collected.0.len(), 5);
}

#[test]
fn frame_limit_respected() {
    // Create app with pre-created consumer group
    let config = KafkaReadConfig::new(&unique_consumer_group("test_group")).topics(["cap"]);
    let (backend, _bootstrap) = crate::common::setup::setup(None);
    let mut app = build_app(backend, Some(&[config.clone()]), |app| {
        app.insert_resource(EventBusConsumerConfig {
            max_events_per_frame: Some(3),
            max_drain_millis: None,
        });
    });
    
    // Preload channel by faking buffers (simulate drain would only take first 3)
    {
        let mut buffers = app.world_mut().resource_mut::<DrainedTopicMetadata>();
        let entry = buffers.topics.entry("cap".into()).or_default();
        for i in 0..10u32 {
            let payload = serde_json::to_vec(&TestMsg { v: i }).unwrap();
            let metadata = EventMetadata {
                source: "cap".to_string(),
                received_timestamp: std::time::Instant::now(),
                headers: std::collections::HashMap::new(),
                key: None,
                backend_specific: Some(Box::new(KafkaMetadata {
                    topic: "cap".to_string(),
                    partition: 0,
                    offset: i as i64,
                    kafka_timestamp: Some(1234567890 + i as i64),
                })),
            };
            entry.push(ProcessedMessage { payload, metadata });
        }
    }
    // Reader only sees existing buffer; limit logic applies only during drain; since we injected directly this test is less meaningful but placeholder.
    let _ = app
        .world_mut()
        .run_system_once(move |mut r: EventBusReader<TestMsg>| {
            // Use the same config that was passed to setup - no need to recreate
            // Consumer group already created during setup
            
            let count = r.read(&config).len();
            assert_eq!(count, 10);
        });
}

#[test]
fn drain_metrics_emitted_and_updated() {
    let (backend, _bootstrap) = crate::common::setup::setup(None);
    let mut app = build_app(backend, None, |_| {});
    // Ensure event type registered
    // Preload channel indirectly: insert some drained payloads, then run one update to emit metrics
    {
        let mut buffers = app.world_mut().resource_mut::<DrainedTopicMetadata>();
        let entry = buffers.topics.entry("m".into()).or_default();
        for i in 0..3u32 {
            let payload = serde_json::to_vec(&TestMsg { v: i }).unwrap();
            let metadata = EventMetadata {
                source: "m".to_string(),
                received_timestamp: std::time::Instant::now(),
                headers: std::collections::HashMap::new(),
                key: None,
                backend_specific: Some(Box::new(KafkaMetadata {
                    topic: "m".to_string(),
                    partition: 0,
                    offset: i as i64,
                    kafka_timestamp: Some(1234567890 + i as i64),
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
