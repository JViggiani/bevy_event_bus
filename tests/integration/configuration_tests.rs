use crate::common::events::TestEvent;
use crate::common::helpers::{unique_topic, update_until, wait_for_events};
use crate::common::setup::setup;
use bevy::prelude::*;
use bevy_event_bus::{
    EventBusReader, EventBusWriter, EventBusAppExt, KafkaReadConfig, KafkaWriteConfig, EventWrapper
};

use std::collections::HashMap;

#[derive(Resource, Default)]
struct Collected(Vec<TestEvent>);

/// Test that configurations work with readers and writers
#[test]
fn configuration_with_readers_writers_works() {
    let topic = unique_topic("config_test");
    
    // Create separate backends for writer and reader to simulate separate machines
    let (backend_writer, _bootstrap_writer) = setup(None);

    // Producer app
    let mut producer_app = {
        let mut app = crate::common::setup::build_app(backend_writer, None, |app| {
            app.add_bus_event::<TestEvent>(&topic);
        });
        
        // Producer system using configuration
        let topic_clone = topic.clone();
        let producer_system = move |mut writer: EventBusWriter<TestEvent>| {
            // Write using configuration - producers specify topics
            let config = KafkaWriteConfig::new(&topic_clone);
            writer.write(&config, TestEvent {
                message: "config_test".to_string(),
                value: 42,
            });
        };
        
        app.add_systems(Update, producer_system);
        app
    };

    // Consumer app using build_app with pre-created consumer group
    let consumer_group = format!("test_group_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
    let config = KafkaReadConfig::new(&consumer_group).topics([&topic]);
    
    let (backend_reader, _bootstrap_reader) = setup(None);
    let mut consumer_app = crate::common::setup::build_app(backend_reader, Some(&[config.clone()]), |app| {
        app.add_bus_event::<TestEvent>(&topic);
        app.insert_resource(Collected::default());
        
        // Consumer system using the same config that was passed to setup
        app.add_systems(Update, move |mut reader: EventBusReader<TestEvent>, mut collected: ResMut<Collected>| {
            let events = reader.read(&config);
            for wrapper in events {
                collected.0.push(wrapper.event().clone());
            }
        });
    });

    // Run producer until messages are processed (or timeout)
    let (_success, _frames) = update_until(
        &mut producer_app,
        1000, // 1 second should be enough for message sending
        |_app| true, // Always true since we just want to run some frames
    );

    // Then run consumer and verify
    let (success, _frames) = update_until(
        &mut consumer_app,
        10000, // 10 second timeout
        |app| {
            let collected = app.world().get_resource::<Collected>().unwrap();
            collected.0.len() >= 1
        },
    );

    assert!(success, "Should have received at least one event within timeout");
    
    let collected = consumer_app.world().get_resource::<Collected>().unwrap();
    assert!(!collected.0.is_empty(), "Should have received at least one event");
    
    let event = &collected.0[0];
    assert_eq!(event.message, "config_test");
    assert_eq!(event.value, 42);
}

/// Test that Kafka-specific methods work with configurations, including metadata functionality
#[test]
fn test_kafka_write_methods() {
    let topic = unique_topic("kafka_methods");
    
    // Create separate apps for writer and reader like other tests
    let (backend_writer, _) = setup(None);
    let mut writer_app = crate::common::setup::build_app(backend_writer, None, |app| {
        app.add_bus_event::<TestEvent>(&topic);
    });

    // Create consumer configuration with unique consumer group
    let consumer_group = format!("test_kafka_methods_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
    let consumer_config = KafkaReadConfig::new(&consumer_group).topics([&topic]);
    
    let (backend_reader, _) = setup(None);
    let mut reader_app = crate::common::setup::build_app(backend_reader, Some(&[consumer_config.clone()]), |app| {
        app.add_bus_event::<TestEvent>(&topic);
    });

    // Test writing with headers and key (metadata functionality)
    let mut headers = HashMap::new();
    headers.insert("method-test".to_string(), "write_methods".to_string());
    headers.insert("test-type".to_string(), "configuration".to_string());
    
    let producer_config = KafkaWriteConfig::new(&topic)
        .with_headers(headers.clone())
        .with_partition_key("test-key-123");

    #[derive(Resource)]
    struct TestConfigs {
        producer: KafkaWriteConfig,
        consumer: KafkaReadConfig,
    }

    writer_app.insert_resource(TestConfigs {
        producer: producer_config,
        consumer: consumer_config.clone(),
    });

    #[derive(Resource, Default)]
    struct ReceivedEvents(Vec<EventWrapper<TestEvent>>);
    
    reader_app.insert_resource(ReceivedEvents::default());
    reader_app.insert_resource(TestConfigs {
        producer: KafkaWriteConfig::new(&topic), // Not used in reader
        consumer: consumer_config,
    });

    // Writer system that sends event with metadata
    writer_app.add_systems(Update, move |mut writer: EventBusWriter<TestEvent>, configs: Res<TestConfigs>, mut sent: Local<bool>| {
        if !*sent {
            *sent = true;
            
            // Test writing with metadata (headers + key)
            writer.write(&configs.producer, TestEvent { 
                message: "metadata_test".to_string(), 
                value: 42 
            });
            
            // Test flush functionality  
            let _flush_result = writer.flush(&configs.producer, std::time::Duration::from_secs(1));
        }
    });

    // Reader system that captures events with metadata
    reader_app.add_systems(Update, move |mut reader: EventBusReader<TestEvent>, configs: Res<TestConfigs>, mut events: ResMut<ReceivedEvents>| {
        for wrapper in reader.read(&configs.consumer) {
            if wrapper.is_external() {
                events.0.push(wrapper.clone());
            }
        }
    });

    // Send the event
    writer_app.update();
    writer_app.update();

    // Wait for the event to be received with metadata
    let received = wait_for_events(&mut reader_app, &topic, 5000, 1, |app| {
        let events = app.world().resource::<ReceivedEvents>();
        events.0.clone()
    });

    assert!(!received.is_empty(), "Should receive at least one event");
    
    let event_wrapper = &received[0];
    
    // Verify the event data
    assert_eq!(event_wrapper.event().message, "metadata_test");
    assert_eq!(event_wrapper.event().value, 42);
    
    // Verify metadata was properly forwarded
    let metadata = event_wrapper.metadata().expect("Event should have metadata");
    
    // Check headers
    assert_eq!(metadata.headers.get("method-test"), Some(&"write_methods".to_string()));
    assert_eq!(metadata.headers.get("test-type"), Some(&"configuration".to_string()));
    
    // Check key
    assert_eq!(metadata.key, Some("test-key-123".to_string()));
    
    // Verify Kafka-specific metadata
    if let Some(kafka_meta) = metadata.kafka_metadata() {
        assert_eq!(kafka_meta.topic, topic);
        assert!(kafka_meta.partition >= 0);
        assert!(kafka_meta.offset >= 0);
        assert!(kafka_meta.kafka_timestamp.is_some(), "Should have Kafka timestamp");
    } else {
        panic!("Expected Kafka metadata");
    }
}

/// Test that the builder pattern works correctly for configurations
#[test]
fn builder_pattern_works() {
    // Test that we can build consumer config
    let consumer_config = KafkaReadConfig::new("test_group").topics(["topic1", "topic2"]);

    // Test that trait methods work using explicit syntax
    use bevy_event_bus::EventBusConfig;
    assert!(!EventBusConfig::topics(&consumer_config).is_empty());
    assert_eq!(consumer_config.consumer_group(), "test_group");

    // Test that we can build producer config  
    let producer_config = KafkaWriteConfig::new("topic1");

    // Test that topic access works
    assert_eq!(producer_config.topic(), "topic1");
}

/// Test that clean system signatures work without explicit backend types
#[test]
fn clean_system_signatures() {
    // This test demonstrates that systems can have clean signatures
    // without explicitly mentioning backend types

    fn clean_producer_system(
        mut writer: EventBusWriter<TestEvent>,
    ) {
        // Configuration can be injected from resource or built inline
        let config = KafkaWriteConfig::new("test_topic");
        let _ = writer.write(&config, TestEvent {
            message: "clean".to_string(),
            value: 123,
        });
    }

    fn clean_consumer_system(
        mut reader: EventBusReader<TestEvent>,
    ) {
        // Using inline configuration
        let config = KafkaReadConfig::new("clean_group").topics(["test_topic"]);
        for _wrapper in reader.read(&config) {
            // Process events
        }
    }

    // If this compiles, then clean signatures work
    let _ = clean_producer_system;
    let _ = clean_consumer_system;
}