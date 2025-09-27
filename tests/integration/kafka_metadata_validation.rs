use crate::common::events::TestEvent;
use crate::common::helpers::{unique_string, update_until, wait_for_events, unique_consumer_group};
use crate::common::setup::{setup, build_app};
use bevy::prelude::*;
use bevy_event_bus::{EventBusReader, EventBusWriter, EventBusAppExt, EventWrapper, KafkaReadConfig, KafkaWriteConfig};
use tracing::{info, info_span};

/// Test that validates Kafka metadata propagation with real broker interaction
/// 
/// This test:
/// 1. Sets up a real Kafka connection
/// 2. Sends events with specific headers and keys
/// 3. Receives events and validates all metadata fields
/// 4. Ensures backend-specific Kafka metadata is properly accessible
#[test]
fn kafka_metadata_end_to_end_validation() {
    let _span = info_span!("kafka_metadata_validation").entered();
    
    let (backend_w, _b1) = setup(None);
    let topic = unique_string("kafka_metadata_test");
    
    info!("Testing Kafka metadata validation with topic: {}", topic);

    // Writer app
    let mut writer = crate::common::setup::build_app(backend_w, None, |app| {
        app.add_bus_event::<TestEvent>(&topic);
    });

    // Create consumer configuration
    let config = KafkaReadConfig::new(&unique_consumer_group("test_group")).topics([&topic]);
    
    // Reader app with consumer group created during setup
    let (backend_r, _bootstrap_r) = setup(None);
    let mut reader = build_app(backend_r, Some(&[config.clone()]), |app| {
        app.add_bus_event::<TestEvent>(&topic);
    });

    #[derive(Resource, Default)]
    struct ReceivedEventsWithMetadata(Vec<EventWrapper<TestEvent>>);

    #[derive(Resource, Clone)]
    struct ReadConfig(KafkaReadConfig);

    reader.insert_resource(ReceivedEventsWithMetadata::default());
    reader.insert_resource(ReadConfig(config.clone()));

    fn reader_system(
        mut r: EventBusReader<TestEvent>,
        config: Res<ReadConfig>,
        mut events: ResMut<ReceivedEventsWithMetadata>,
    ) {
        for event_wrapper in r.read(&config.0) {
            // Only collect external events with metadata
            if event_wrapper.is_external() {
                events.0.push(event_wrapper.clone());
            }
        }
    }

    reader.add_systems(Update, reader_system);

    // Send test events
    #[derive(Resource, Clone)]
    struct TestData {
        topic: String,
        test_cases: Vec<TestEvent>,
        sent: bool,
    }

    let test_cases = vec![
        TestEvent {
            message: "metadata_test_1".to_string(),
            value: 1,
        },
        TestEvent {
            message: "metadata_test_2".to_string(),
            value: 2,
        },
        TestEvent {
            message: "metadata_test_3".to_string(),
            value: 3,
        },
    ];

    writer.insert_resource(TestData {
        topic: topic.clone(),
        test_cases: test_cases.clone(),
        sent: false,
    });

    fn writer_system(mut w: EventBusWriter<TestEvent>, mut data: ResMut<TestData>) {
        if !data.sent {
            for (i, test_event) in data.test_cases.iter().enumerate() {
                // Add headers to test header forwarding
                let config = KafkaWriteConfig::new(&data.topic)
                    .header("test-id", &format!("metadata_test_{}", i + 1))
                    .header("sequence", &i.to_string())
                    .header("validation-type", "end-to-end");
                let _ = w.write(&config, test_event.clone());
            }
            data.sent = true;
            info!("Sent {} test events", data.test_cases.len());
        }
    }

    writer.add_systems(Update, writer_system);

    // Record test start time for timestamp validation
    let test_start_time = std::time::Instant::now();

    // Send the events
    writer.update();
    writer.update(); // Extra update to ensure all events are sent

    // Wait for events to arrive and validate metadata
    let topic_copy = topic.clone();
    let received_events = wait_for_events(&mut reader, &topic_copy, 10000, 3, |app| {
        let events = app.world().resource::<ReceivedEventsWithMetadata>();
        events.0.clone()
    });

    // Record test end time for timestamp validation
    let test_end_time = std::time::Instant::now();

    assert_eq!(received_events.len(), 3, "Should receive exactly 3 events");

    // Validate each received event and its metadata
    for (i, external_event) in received_events.iter().enumerate() {
        info!("Validating event {}: {:?}", i, external_event.event());
        
        // Validate event data using Deref
        assert_eq!(external_event.message, format!("metadata_test_{}", i + 1));
        assert_eq!(external_event.value, (i + 1) as i32);
        
        // Get metadata for validation
        let metadata = external_event.metadata().expect("External event should have metadata");
        
        // Validate basic metadata
        assert_eq!(metadata.source, topic_copy);
        
        // Validate timestamp is within test execution window
        assert!(metadata.received_timestamp >= test_start_time, 
                "Event timestamp {:?} should be >= test start time {:?}", 
                metadata.received_timestamp, test_start_time);
        assert!(metadata.received_timestamp <= test_end_time, 
                "Event timestamp {:?} should be <= test end time {:?}", 
                metadata.received_timestamp, test_end_time);
        
        // Validate Kafka-specific metadata
        if let Some(kafka_meta) = metadata.kafka_metadata() {
            // Topic should match
            assert_eq!(kafka_meta.topic, topic_copy, "Kafka topic should match");
            
            // Partition should be valid (0 or higher for single partition topic)
            assert!(kafka_meta.partition >= 0, "Partition should be >= 0, got {}", kafka_meta.partition);
            
            // Offset should be valid and increasing
            assert!(kafka_meta.offset >= 0, "Offset should be >= 0, got {}", kafka_meta.offset);
            
            // For ordered events, offset should generally increase
            if i > 0 {
                let prev_metadata = received_events[i-1].metadata().unwrap();
                let prev_kafka_meta = prev_metadata.kafka_metadata().unwrap();
                if kafka_meta.partition == prev_kafka_meta.partition {
                    assert!(kafka_meta.offset > prev_kafka_meta.offset, 
                        "Offset should increase: event {} offset {} should be > event {} offset {}", 
                        i, kafka_meta.offset, i-1, prev_kafka_meta.offset);
                }
            }
            
            info!(
                "Event {} metadata validation passed - Topic: {}, Partition: {}, Offset: {}",
                i, kafka_meta.topic, kafka_meta.partition, kafka_meta.offset
            );
        } else {
            panic!("Expected Kafka metadata for event {}, but got None", i);
        }
        
        // Validate headers were properly forwarded
        assert_eq!(metadata.headers.get("test-id"), Some(&format!("metadata_test_{}", i + 1)));
        assert_eq!(metadata.headers.get("sequence"), Some(&i.to_string()));
        assert_eq!(metadata.headers.get("validation-type"), Some(&"end-to-end".to_string()));
        
        info!("Headers validation passed for event {}: {:?}", i, metadata.headers);
    }

    info!("All Kafka metadata validation tests passed!");
}

#[test]
fn kafka_metadata_message_ordering() {
    let _span = info_span!("kafka_metadata_validation").entered();
    
    let (backend_w, _b1) = setup(None);
    let topic = unique_string("kafka_metadata_ordering_test");
    
    info!("Testing Kafka metadata validation with message ordering: {}", topic);

        // Writer app
    let mut writer = crate::common::setup::build_app(backend_w, None, |app| {
        app.add_bus_event::<TestEvent>(&topic);
    });

    // Create consumer configuration
    let consumer_group = unique_string("test_group_ordering");
    let config = KafkaReadConfig::new(&consumer_group).topics([&topic]);
    
    // Reader app - receives events and validates metadata
    let (backend_r, _bootstrap_r) = crate::common::setup::setup(None);
    let mut reader = build_app(backend_r, Some(&[config.clone()]), |app| {
        app.add_bus_event::<TestEvent>(&topic);
    });

    #[derive(Resource, Default)]
    struct ReceivedEventsWithMetadata(Vec<EventWrapper<TestEvent>>);

    reader.insert_resource(ReceivedEventsWithMetadata::default());

    #[derive(Resource, Clone)]
    struct ReadConfig(KafkaReadConfig);

    // Simple system that just reads - no consumer group creation needed
    fn reader_system(
        mut r: EventBusReader<TestEvent>,
        config: Res<ReadConfig>,
        mut events: ResMut<ReceivedEventsWithMetadata>,
    ) {
        for event_wrapper in r.read(&config.0) {
            // Only collect external events with metadata
            if event_wrapper.is_external() {
                events.0.push(event_wrapper.clone());
            }
        }
    }

    reader.insert_resource(ReadConfig(config.clone()));
    reader.add_systems(Update, reader_system);

    // Send test events
    #[derive(Resource, Clone)]
    struct TestData {
        topic: String,
        test_cases: Vec<TestEvent>,
        sent: bool,
    }

    let test_cases = vec![
        TestEvent {
            message: "metadata_test_1".to_string(),
            value: 1,
        },
        TestEvent {
            message: "metadata_test_2".to_string(),
            value: 2,
        },
        TestEvent {
            message: "metadata_test_3".to_string(),
            value: 3,
        },
    ];

    writer.insert_resource(TestData {
        topic: topic.clone(),
        test_cases: test_cases.clone(),
        sent: false,
    });

    fn writer_system(mut w: EventBusWriter<TestEvent>, mut data: ResMut<TestData>) {
        if !data.sent {
            for (i, test_event) in data.test_cases.iter().enumerate() {
                // Add headers to test header forwarding in message ordering test
                let config = KafkaWriteConfig::new(&data.topic)
                    .header("order-test-id", &format!("ordering_test_{}", i + 1))
                    .header("sequence-number", &i.to_string())
                    .header("test-type", "message-ordering");
                
                let _ = w.write(&config, test_event.clone());
            }
            data.sent = true;
            info!("Sent {} test events with headers for ordering test", data.test_cases.len());
        }
    }

    writer.add_systems(Update, writer_system);

    // Record test start time for timestamp validation
    let test_start_time = std::time::Instant::now();

    // Send the events
    writer.update();
    writer.update(); // Extra update to ensure all events are sent

    // Wait for events to arrive and validate metadata
    let topic_copy = topic.clone();
    let received_events = wait_for_events(&mut reader, &topic_copy, 10000, 3, |app| {
        let events = app.world().resource::<ReceivedEventsWithMetadata>();
        events.0.clone()
    });

    // Record test end time for timestamp validation
    let test_end_time = std::time::Instant::now();

    assert_eq!(received_events.len(), 3, "Should receive exactly 3 events");

    // Validate each received event and its metadata
    for (i, external_event) in received_events.iter().enumerate() {
        info!("Validating event {}: {:?}", i, external_event.event());
        
        // Validate event data using Deref
        assert_eq!(external_event.message, format!("metadata_test_{}", i + 1));
        assert_eq!(external_event.value, (i + 1) as i32);
        
        // Get metadata for validation
        let metadata = external_event.metadata().expect("External event should have metadata");
        
        // Validate basic metadata
        assert_eq!(metadata.source, topic_copy);
        
        // Validate timestamp is within test execution window
        assert!(metadata.received_timestamp >= test_start_time, 
                "Event timestamp {:?} should be >= test start time {:?}", 
                metadata.received_timestamp, test_start_time);
        assert!(metadata.received_timestamp <= test_end_time, 
                "Event timestamp {:?} should be <= test end time {:?}", 
                metadata.received_timestamp, test_end_time);
        
        // Validate Kafka-specific metadata
        if let Some(kafka_meta) = metadata.kafka_metadata() {
            // Topic should match
            assert_eq!(kafka_meta.topic, topic_copy, "Kafka topic should match");
            
            // Partition should be valid (0 or higher for single partition topic)
            assert!(kafka_meta.partition >= 0, "Partition should be >= 0, got {}", kafka_meta.partition);
            
            // Offset should be valid and increasing
            assert!(kafka_meta.offset >= 0, "Offset should be >= 0, got {}", kafka_meta.offset);
            
            // For ordered events, offset should generally increase
            if i > 0 {
                let prev_metadata = received_events[i-1].metadata().unwrap();
                let prev_kafka_meta = prev_metadata.kafka_metadata().unwrap();
                if kafka_meta.partition == prev_kafka_meta.partition {
                    assert!(kafka_meta.offset > prev_kafka_meta.offset, 
                        "Offset should increase: event {} offset {} should be > event {} offset {}", 
                        i, kafka_meta.offset, i-1, prev_kafka_meta.offset);
                }
            }
            
            info!(
                "Event {} metadata validation passed - Topic: {}, Partition: {}, Offset: {}",
                i, kafka_meta.topic, kafka_meta.partition, kafka_meta.offset
            );
        } else {
            panic!("Expected Kafka metadata for event {}, but got None", i);
        }
        
        // Validate headers were properly forwarded in message ordering test
        assert_eq!(metadata.headers.get("order-test-id"), Some(&format!("ordering_test_{}", i + 1)));
        assert_eq!(metadata.headers.get("sequence-number"), Some(&i.to_string()));
        assert_eq!(metadata.headers.get("test-type"), Some(&"message-ordering".to_string()));
        
        info!("Headers validation passed for ordered event {}: {:?}", i, metadata.headers);
    }

    info!("All Kafka metadata validation tests passed!");
}

/// Test that validates metadata isolation between different topics
#[test]
fn kafka_metadata_topic_isolation() {
    let _span = info_span!("kafka_metadata_topic_isolation").entered();
    
    let (backend_w, _b1) = setup(None);
    let topic_a = unique_string("isolation_test_a");
    let topic_b = unique_string("isolation_test_b");
    
    info!("Testing metadata isolation between topics: {} and {}", topic_a, topic_b);

    // Writer app
    let mut writer = crate::common::setup::build_app(backend_w, None, |app| {
        app.add_bus_event::<TestEvent>(&topic_a);
        app.add_bus_event::<TestEvent>(&topic_b);
    });

    // Create consumer configurations for both topics
    let consumer_group_a = unique_string("test_group_a");
    let consumer_group_b = unique_string("test_group_b");
    let config_a = KafkaReadConfig::new(&consumer_group_a).topics([&topic_a]);
    let config_b = KafkaReadConfig::new(&consumer_group_b).topics([&topic_b]);
    
    // Reader app with consumer groups created during setup
    let (backend_r, _bootstrap_r) = crate::common::setup::setup(None);
    let mut reader = build_app(backend_r, Some(&[config_a.clone(), config_b.clone()]), |app| {
        app.add_bus_event::<TestEvent>(&topic_a);
        app.add_bus_event::<TestEvent>(&topic_b);
    });

    #[derive(Resource, Default)]
    struct ReceivedEvents {
        topic_a: Vec<EventWrapper<TestEvent>>,
        topic_b: Vec<EventWrapper<TestEvent>>,
    }

    reader.insert_resource(ReceivedEvents::default());

    #[derive(Resource, Clone)]
    struct ConfigA(KafkaReadConfig);
    #[derive(Resource, Clone)]
    struct ConfigB(KafkaReadConfig);

    // Simple system that just reads - no consumer group creation needed
    fn reader_system_isolation(
        mut r: EventBusReader<TestEvent>,
        config_a: Res<ConfigA>,
        config_b: Res<ConfigB>,
        mut events: ResMut<ReceivedEvents>,
    ) {
        // Read from topic A
        for event_wrapper in r.read(&config_a.0) {
            if event_wrapper.is_external() {
                events.topic_a.push(event_wrapper.clone());
            }
        }
        
        // Read from topic B
        for event_wrapper in r.read(&config_b.0) {
            if event_wrapper.is_external() {
                events.topic_b.push(event_wrapper.clone());
            }
        }
    }

    reader.insert_resource(ConfigA(config_a.clone()));
    reader.insert_resource(ConfigB(config_b.clone()));
    reader.add_systems(Update, reader_system_isolation);

    // Send different events to each topic
    #[derive(Resource, Clone)]
    struct IsolationTestData {
        topic_a: String,
        topic_b: String,
        sent: bool,
    }

    writer.insert_resource(IsolationTestData {
        topic_a: topic_a.clone(),
        topic_b: topic_b.clone(),
        sent: false,
    });

    fn writer_system_isolation(mut w: EventBusWriter<TestEvent>, mut data: ResMut<IsolationTestData>) {
        if !data.sent {
            let event_a = TestEvent {
                message: "topic_a_event".to_string(),
                value: 1000,
            };
            let event_b = TestEvent {
                message: "topic_b_event".to_string(),
                value: 2000,
            };
            
            let _ = w.write(&KafkaWriteConfig::new(&data.topic_a), event_a);
            let _ = w.write(&KafkaWriteConfig::new(&data.topic_b), event_b);
            data.sent = true;
            
            info!("Sent events to both topics");
        }
    }

    writer.add_systems(Update, writer_system_isolation);

    // Send the events
    writer.update();
    writer.update();

    // Wait for events from both topics - using update_until since we need a count  
    let (success, _) = update_until(&mut reader, 10000, |app| {
        let events = app.world().resource::<ReceivedEvents>();
        events.topic_a.len() + events.topic_b.len() >= 2
    });

    assert!(success, "Should receive events from both topics within timeout");

    let final_events = reader.world().resource::<ReceivedEvents>();
    let total_received = final_events.topic_a.len() + final_events.topic_b.len();
    assert_eq!(total_received, 2, "Should receive exactly 2 events total");
    
    // Validate topic A event
    assert_eq!(final_events.topic_a.len(), 1, "Should have 1 event from topic A");
    let event_a = &final_events.topic_a[0];
    assert_eq!(event_a.message, "topic_a_event"); // Use Deref
    
    let metadata_a = event_a.metadata().expect("External event should have metadata");
    assert_eq!(metadata_a.source, topic_a.clone());
    
    if let Some(kafka_meta_a) = metadata_a.kafka_metadata() {
        assert_eq!(kafka_meta_a.topic, topic_a.clone());
    } else {
        panic!("Expected Kafka metadata for topic A event");
    }
    
    // Validate topic B event  
    assert_eq!(final_events.topic_b.len(), 1, "Should have 1 event from topic B");
    let event_b = &final_events.topic_b[0];
    assert_eq!(event_b.message, "topic_b_event"); // Use Deref
    
    let metadata_b = event_b.metadata().expect("External event should have metadata");
    assert_eq!(metadata_b.source, topic_b.clone());
    
    if let Some(kafka_meta_b) = metadata_b.kafka_metadata() {
        assert_eq!(kafka_meta_b.topic, topic_b.clone());
    } else {
        panic!("Expected Kafka metadata for topic B event");
    }    info!("Topic isolation metadata validation passed!");
}

/// Test that validates metadata consistency under load
#[test]
fn kafka_metadata_consistency_under_load() {
    let _span = info_span!("kafka_metadata_consistency").entered();
    
    let (backend_w, _b1) = setup(None);
    let topic = unique_string("consistency_test");
    
    info!("Testing metadata consistency under load with topic: {}", topic);

    // Writer app
    let mut writer = crate::common::setup::build_app(backend_w, None, |app| {
        app.add_bus_event::<TestEvent>(&topic);
    });

    // Create consumer configuration
    let consumer_group = unique_string("test_group_consistency");
    let config = KafkaReadConfig::new(&consumer_group).topics([&topic]);
    
    // Reader app with consumer group created during setup
    let (backend_r, _bootstrap_r) = crate::common::setup::setup(None);
    let mut reader = build_app(backend_r, Some(&[config.clone()]), |app| {
        app.add_bus_event::<TestEvent>(&topic);
    });

    #[derive(Resource, Default)]
    struct ReceivedEventsWithMetadata(Vec<EventWrapper<TestEvent>>);
    
    #[derive(Resource, Clone)]
    struct LoadTestConfig(KafkaReadConfig);

    reader.insert_resource(ReceivedEventsWithMetadata::default());
    reader.insert_resource(LoadTestConfig(config.clone()));

    // Simple system that just reads - no consumer group creation needed
    fn reader_system_consistency(
        mut r: EventBusReader<TestEvent>,
        config: Res<LoadTestConfig>,
        mut events: ResMut<ReceivedEventsWithMetadata>,
    ) {
        for event_wrapper in r.read(&config.0) {
            if event_wrapper.is_external() {
                events.0.push(event_wrapper.clone());
            }
        }
    }

    reader.add_systems(Update, reader_system_consistency);

    // Send multiple batches of events
    const BATCH_SIZE: usize = 10;
    const NUM_BATCHES: usize = 3;
    
    #[derive(Resource, Clone)]
    struct ConsistencyTestData {
        topic: String,
        sent: bool,
    }

    writer.insert_resource(ConsistencyTestData {
        topic: topic.clone(),
        sent: false,
    });

    fn writer_system_consistency(mut w: EventBusWriter<TestEvent>, mut data: ResMut<ConsistencyTestData>) {
        if !data.sent {
            for batch in 0..NUM_BATCHES {
                for i in 0..BATCH_SIZE {
                    let event = TestEvent {
                        message: format!("batch_{}_event_{}", batch, i),
                        value: (batch * BATCH_SIZE + i) as i32,
                    };
                    let _ = w.write(&KafkaWriteConfig::new(&data.topic), event);
                }
            }
            data.sent = true;
            info!("Sent {} events in {} batches", BATCH_SIZE * NUM_BATCHES, NUM_BATCHES);
        }
    }

    writer.add_systems(Update, writer_system_consistency);

    // Record test start time for timestamp validation
    let test_start_time = std::time::Instant::now();

    // Send the events
    writer.update();
    writer.update();

    // Wait for all events to arrive
    let topic_copy = topic.clone();
    let received_events = wait_for_events(&mut reader, &topic_copy, 15000, BATCH_SIZE * NUM_BATCHES, |app| {
        let events = app.world().resource::<ReceivedEventsWithMetadata>();
        events.0.clone()
    });

    // Record test end time for timestamp validation
    let test_end_time = std::time::Instant::now();

    assert_eq!(received_events.len(), BATCH_SIZE * NUM_BATCHES, 
               "Should receive exactly {} events", BATCH_SIZE * NUM_BATCHES);

    // Validate metadata consistency
    let mut partitions_seen = std::collections::HashSet::new();
    let mut offsets_by_partition: std::collections::HashMap<i32, Vec<i64>> = std::collections::HashMap::new();

    for (i, external_event) in received_events.iter().enumerate() {
        // Get metadata for validation
        let metadata = external_event.metadata().expect("External event should have metadata");
        
        // Validate basic metadata fields are present
        assert_eq!(metadata.source, topic);
        
        // Validate timestamp is within test execution window
        assert!(metadata.received_timestamp >= test_start_time, 
                "Event timestamp {:?} should be >= test start time {:?}", 
                metadata.received_timestamp, test_start_time);
        assert!(metadata.received_timestamp <= test_end_time, 
                "Event timestamp {:?} should be <= test end time {:?}", 
                metadata.received_timestamp, test_end_time);
        
        // Validate Kafka metadata
        if let Some(kafka_meta) = metadata.kafka_metadata() {
            assert_eq!(kafka_meta.topic, topic);
            assert!(kafka_meta.partition >= 0);
            assert!(kafka_meta.offset >= 0);
            
            partitions_seen.insert(kafka_meta.partition);
            offsets_by_partition.entry(kafka_meta.partition).or_insert_with(Vec::new).push(kafka_meta.offset);
        } else {
            panic!("Expected Kafka metadata for event {}", i);
        }
    }

    // Validate offset ordering within each partition
    for (partition, mut offsets) in offsets_by_partition {
        offsets.sort();
        for i in 1..offsets.len() {
            assert!(offsets[i] > offsets[i-1], 
                   "Offsets should be strictly increasing within partition {}: {} should be > {}", 
                   partition, offsets[i], offsets[i-1]);
        }
        info!("Partition {} has {} events with consistent offset ordering", partition, offsets.len());
    }

    info!("Metadata consistency validation passed for {} events across {} partitions!", 
          received_events.len(), partitions_seen.len());
}