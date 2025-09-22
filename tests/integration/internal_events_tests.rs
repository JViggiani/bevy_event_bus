use crate::common::TestEvent;
use crate::common::mock_backend::MockEventBusBackend;
use bevy::prelude::*;
use bevy_event_bus::{EventBusPlugins, EventBusReader, EventBusWriter, EventBusAppExt};
use std::collections::HashMap;
use tracing::{info, info_span};
use tracing_subscriber::EnvFilter;

/// Test that internal events work correctly with the simplified EventWrapper API
/// This test uses a MockBackend but focuses on internal-only event processing
#[test]
fn test_internal_only_events() {
    // Initialize tracing subscriber once (idempotent - ignore error)
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init();

    let _span = info_span!("test_internal_only_events").entered();
    
    // Create app with MockBackend (but we'll focus on internal events only)
    let mut app = App::new();
    
    // Add event bus plugins with mock backend
    app.add_plugins(EventBusPlugins(
        MockEventBusBackend::new(), // Mock backend - external events will be mocked
        bevy_event_bus::PreconfiguredTopics::new(Vec::<String>::new()), // No preconfigured topics needed
    ));
    
    // Register our test event
    let topic = "internal_test_topic";
    app.add_bus_event::<TestEvent>(topic);
    
    // Create test events to send
    let test_events = vec![
        TestEvent { message: "Internal Event 1".to_string(), value: 10 },
        TestEvent { message: "Internal Event 2".to_string(), value: 20 },
        TestEvent { message: "Internal Event 3".to_string(), value: 30 },
    ];
    
    #[derive(Resource, Default)]
    struct ReceivedEvents(Vec<bevy_event_bus::EventWrapper<TestEvent>>);
    
    app.insert_resource(ReceivedEvents::default());
    
    // System to send events (these will create both external mock events and internal events)
    let events_to_send = test_events.clone();
    let topic_clone = topic.to_string();
    app.add_systems(Update, move |
        mut writer: EventBusWriter<TestEvent>,
        mut sent: Local<bool>
    | {
        if !*sent {
            *sent = true;
            for event in &events_to_send {
                writer.write(&topic_clone, event.clone());
            }
            info!("Sent {} events (both external mock and internal)", events_to_send.len());
        }
    });
    
    // System to read events and filter for internal ones only
    let topic_clone = topic.to_string();
    app.add_systems(Update, move |
        mut reader: EventBusReader<TestEvent>,
        mut received: ResMut<ReceivedEvents>
    | {
        for event_wrapper in reader.read(&topic_clone) {
            // Only collect internal events for this test
            if event_wrapper.is_internal() {
                received.0.push(event_wrapper.clone());
            }
        }
    });
    
    // Run several update cycles to process events
    for i in 0..10 {
        app.update();
        let received = app.world().resource::<ReceivedEvents>();
        info!("Update cycle {}: {} internal events received", i, received.0.len());
        if received.0.len() >= test_events.len() {
            break;
        }
    }
    
    // Verify results
    let received = app.world().resource::<ReceivedEvents>();
    assert_eq!(received.0.len(), test_events.len(), 
               "Should receive all {} internal events", test_events.len());
    
    // Verify each event is internal and has correct data
    for (i, event_wrapper) in received.0.iter().enumerate() {
        // Verify this is an internal event (no metadata)
        assert!(event_wrapper.is_internal(), 
                "Event {} should be internal", i);
        assert!(!event_wrapper.is_external(), 
                "Event {} should not be external", i);
        assert!(event_wrapper.metadata().is_none(), 
                "Internal event {} should have no metadata", i);
        
        // Verify event data using Deref trait
        assert_eq!(event_wrapper.message, test_events[i].message,
                   "Event {} message should match", i);
        assert_eq!(event_wrapper.value, test_events[i].value,
                   "Event {} value should match", i);
        
        // Verify event data using into_event()
        let cloned_wrapper = event_wrapper.clone();
        let extracted_event = cloned_wrapper.into_event();
        assert_eq!(extracted_event.message, test_events[i].message,
                   "Extracted event {} message should match", i);
        assert_eq!(extracted_event.value, test_events[i].value,
                   "Extracted event {} value should match", i);
    }
    
    info!("All internal events validated successfully!");
}

/// Test that internal events work with headers (headers should be ignored for internal events)
#[test] 
fn test_internal_events_with_headers() {
    // Initialize tracing subscriber once (idempotent - ignore error)
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init();

    let _span = info_span!("test_internal_events_with_headers").entered();
    
    // Create app with MockBackend 
    let mut app = App::new();
    
    // Add event bus plugins with mock backend
    app.add_plugins(EventBusPlugins(
        MockEventBusBackend::new(), // Mock backend - external events will be mocked
        bevy_event_bus::PreconfiguredTopics::new(Vec::<String>::new()), // No preconfigured topics needed
    ));
    
    // Register our test event
    let topic = "internal_headers_test_topic";
    app.add_bus_event::<TestEvent>(topic);
    
    let test_event = TestEvent { 
        message: "Event with headers".to_string(), 
        value: 42 
    };
    
    #[derive(Resource, Default)]
    struct ReceivedEvents(Vec<bevy_event_bus::EventWrapper<TestEvent>>);
    
    app.insert_resource(ReceivedEvents::default());
    
    // System to send event with headers
    let topic_clone = topic.to_string();
    app.add_systems(Update, move |
        mut writer: EventBusWriter<TestEvent>,
        mut sent: Local<bool>
    | {
        if !*sent {
            *sent = true;
            
            // Create headers
            let mut headers = HashMap::new();
            headers.insert("trace-id".to_string(), "internal-123".to_string());
            headers.insert("source".to_string(), "internal-test".to_string());
            
            // Send event with headers (will create both mock external and internal events)
            writer.write_with_headers(&topic_clone, test_event.clone(), headers);
            info!("Sent event with headers (internal portion will ignore headers)");
        }
    });
    
    // System to read events and filter for internal ones only
    let topic_clone = topic.to_string();
    app.add_systems(Update, move |
        mut reader: EventBusReader<TestEvent>,
        mut received: ResMut<ReceivedEvents>
    | {
        for event_wrapper in reader.read(&topic_clone) {
            // Only collect internal events for this test
            if event_wrapper.is_internal() {
                received.0.push(event_wrapper.clone());
            }
        }
    });
    
    // Run several update cycles to process events
    for i in 0..10 {
        app.update();
        let received = app.world().resource::<ReceivedEvents>();
        if !received.0.is_empty() {
            break;
        }
    }
    
    // Verify results
    let received = app.world().resource::<ReceivedEvents>();
    assert_eq!(received.0.len(), 1, "Should receive exactly one internal event");
    
    let event_wrapper = &received.0[0];
    
    // Verify this is an internal event (headers are ignored for internal events)
    assert!(event_wrapper.is_internal(), "Event should be internal");
    assert!(!event_wrapper.is_external(), "Event should not be external");
    assert!(event_wrapper.metadata().is_none(), 
            "Internal event should have no metadata (headers are ignored)");
    
    // Verify event data is correct
    assert_eq!(event_wrapper.message, "Event with headers");
    assert_eq!(event_wrapper.value, 42);
    
    info!("Internal event with headers validated successfully - headers properly ignored!");
}

/// Test the into_parts() method on internal events
#[test]
fn test_internal_events_into_parts() {
    // Initialize tracing subscriber once (idempotent - ignore error)
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init();

    let _span = info_span!("test_internal_events_into_parts").entered();
    
    // Create app with MockBackend
    let mut app = App::new();
    
    // Add event bus plugins with mock backend
    app.add_plugins(EventBusPlugins(
        MockEventBusBackend::new(), // Mock backend - external events will be mocked
        bevy_event_bus::PreconfiguredTopics::new(Vec::<String>::new()), // No preconfigured topics needed
    ));
    
    // Register our test event
    let topic = "internal_parts_test_topic";
    app.add_bus_event::<TestEvent>(topic);
    
    let test_event = TestEvent { 
        message: "Parts test event".to_string(), 
        value: 99 
    };
    
    #[derive(Resource, Default)]
    struct ReceivedEvents(Vec<bevy_event_bus::EventWrapper<TestEvent>>);
    
    app.insert_resource(ReceivedEvents::default());
    
    // System to send event
    let topic_clone = topic.to_string();
    app.add_systems(Update, move |
        mut writer: EventBusWriter<TestEvent>,
        mut sent: Local<bool>
    | {
        if !*sent {
            *sent = true;
            writer.write(&topic_clone, test_event.clone());
            info!("Sent event for into_parts test");
        }
    });
    
    // System to read events and filter for internal ones only  
    let topic_clone = topic.to_string();
    app.add_systems(Update, move |
        mut reader: EventBusReader<TestEvent>,
        mut received: ResMut<ReceivedEvents>
    | {
        for event_wrapper in reader.read(&topic_clone) {
            // Only collect internal events for this test
            if event_wrapper.is_internal() {
                received.0.push(event_wrapper.clone());
            }
        }
    });
    
    // Run several update cycles to process events
    for i in 0..10 {
        app.update();
        let received = app.world().resource::<ReceivedEvents>();
        if !received.0.is_empty() {
            break;
        }
    }
    
    // Verify results
    let received = app.world().resource::<ReceivedEvents>();
    assert_eq!(received.0.len(), 1, "Should receive exactly one internal event");
    
    let event_wrapper = received.0[0].clone();
    
    // Test into_parts() method
    let (event, metadata) = event_wrapper.into_parts();
    
    // Verify event data
    assert_eq!(event.message, "Parts test event");
    assert_eq!(event.value, 99);
    
    // Verify metadata is None for internal events
    assert!(metadata.is_none(), "Internal event metadata should be None");
    
    info!("Internal event into_parts() method validated successfully!");
}