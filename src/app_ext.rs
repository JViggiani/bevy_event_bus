//! Enhanced registration API for multi-decoder pipeline

use bevy::prelude::*;
use bevy_event_bus::BusEvent;
use bevy_event_bus::decoder::{DecoderRegistry, TypedDecoder};

/// Extension trait for the Bevy App to simplify event bus registration
pub trait EventBusAppExt {
    /// Register an event with the event bus and automatically set up JSON decoding for topic(s)
    ///
    /// This is the main convenience method that:
    /// 1. Registers the event in Bevy's event system (like `app.add_event::<T>()`)
    /// 2. Registers the event as a bus event
    /// 3. Sets up a JSON decoder for the specified topic(s)
    ///
    /// # Examples
    /// ```rust
    /// use bevy::prelude::*;
    /// use bevy_event_bus::prelude::*;
    ///
    /// #[derive(Event, Clone, serde::Serialize, serde::Deserialize)]
    /// struct PlayerMove { x: f32, y: f32 }
    ///
    /// let mut app = App::new();
    /// // Single topic
    /// app.add_bus_event::<PlayerMove>("game_events");
    /// ```
    fn add_bus_event<T: BusEvent + Event>(&mut self, topic: &str) -> &mut Self;

    /// Register an event for a slice of topics
    ///
    /// # Example
    /// ```rust
    /// use bevy::prelude::*;
    /// use bevy_event_bus::prelude::*;
    ///
    /// #[derive(Event, Clone, serde::Serialize, serde::Deserialize)]
    /// struct PlayerMove { x: f32, y: f32 }
    ///
    /// let mut app = App::new();
    /// // Multiple topics at once
    /// app.add_bus_event_topics::<PlayerMove>(&["game_events", "move_events"]);
    /// ```
    fn add_bus_event_topics<T: BusEvent + Event>(&mut self, topics: &[&str]) -> &mut Self;

    /// Register an event for multiple topics with automatic JSON decoding
    ///
    /// This supports many-to-many relationships between events and topics.
    /// One event type can be decoded from multiple topics.
    ///
    /// # Example
    /// ```rust
    /// use bevy::prelude::*;
    /// use bevy_event_bus::prelude::*;
    ///
    /// #[derive(Event, Clone, serde::Serialize, serde::Deserialize)]
    /// struct PlayerMove { x: f32, y: f32 }
    ///
    /// let mut app = App::new();
    /// app.add_bus_event_multi::<PlayerMove>(&["game_events", "move_events"]);
    /// ```
    fn add_bus_event_multi<T: BusEvent + Event>(&mut self, topics: &[&str]) -> &mut Self;

    /// Register a custom decoder for a specific topic and event type
    ///
    /// This allows you to register multiple decoders per topic, enabling a single topic
    /// to carry multiple event types or use custom decoding logic.
    ///
    /// # Example
    /// ```rust
    /// use bevy::prelude::*;
    /// use bevy_event_bus::app_ext::EventBusAppExt;
    ///
    /// #[derive(Event, Clone, serde::Serialize, serde::Deserialize)]
    /// struct PlayerMove { x: f32, y: f32 }
    ///
    /// let mut app = App::new();
    /// app.register_topic_decoder("game_events", |data: &[u8]| {
    ///     serde_json::from_slice::<PlayerMove>(data).ok()
    /// });
    /// ```
    fn register_topic_decoder<T: BusEvent + Event>(
        &mut self,
        topic: &str,
        decoder: impl Fn(&[u8]) -> Option<T> + Send + Sync + 'static,
    ) -> &mut Self;
}

impl EventBusAppExt for App {
    fn add_bus_event<T: BusEvent + Event>(&mut self, topic: &str) -> &mut Self {
        // Add the event to Bevy's event system (equivalent to app.add_event::<T>())
        bevy::prelude::App::add_event::<T>(self);

        // Auto-register the corresponding error event type
        bevy::prelude::App::add_event::<bevy_event_bus::EventBusError<T>>(self);

        // Register JSON decoder for the topic
        self.add_bus_event_multi::<T>(&[topic])
    }

    fn add_bus_event_topics<T: BusEvent + Event>(&mut self, topics: &[&str]) -> &mut Self {
        // Add the event to Bevy's event system (equivalent to app.add_event::<T>())
        bevy::prelude::App::add_event::<T>(self);

        // Auto-register the corresponding error event type
        bevy::prelude::App::add_event::<bevy_event_bus::EventBusError<T>>(self);

        // Register JSON decoder for the topics
        self.add_bus_event_multi::<T>(topics)
    }

    fn add_bus_event_multi<T: BusEvent + Event>(&mut self, topics: &[&str]) -> &mut Self {
        let topic_list: Vec<String> = topics.iter().map(|t| (*t).to_string()).collect();

        // Ensure event is registered first
        if !self.world().contains_resource::<Events<T>>() {
            bevy::prelude::App::add_event::<T>(self);
        }

        // Auto-register the corresponding error event type
        if !self
            .world()
            .contains_resource::<Events<bevy_event_bus::EventBusError<T>>>()
        {
            bevy::prelude::App::add_event::<bevy_event_bus::EventBusError<T>>(self);
        }

        // Initialize decoder registry if it doesn't exist
        if !self.world().contains_resource::<DecoderRegistry>() {
            self.insert_resource(DecoderRegistry::new());
        }

        // Register JSON decoder for each topic
        for topic in &topic_list {
            let typed_decoder = TypedDecoder::<T>::json_decoder();

            if let Some(mut registry) = self.world_mut().get_resource_mut::<DecoderRegistry>() {
                registry.register_decoder(topic, typed_decoder);
            } else {
                let mut registry = DecoderRegistry::new();
                registry.register_decoder(topic, typed_decoder);
                self.insert_resource(registry);
            }

            tracing::info!(
                topic = %topic,
                event_type = std::any::type_name::<T>(),
                "Registered JSON decoder for topic"
            );
        }

        bevy_event_bus::writers::outbound_bridge::ensure_bridge::<T>(self, &topic_list);

        self
    }

    fn register_topic_decoder<T: BusEvent + Event>(
        &mut self,
        topic: &str,
        decoder: impl Fn(&[u8]) -> Option<T> + Send + Sync + 'static,
    ) -> &mut Self {
        // Ensure event is registered first
        if !self.world().contains_resource::<Events<T>>() {
            bevy::prelude::App::add_event::<T>(self);
        }

        // Auto-register the corresponding error event type
        if !self
            .world()
            .contains_resource::<Events<bevy_event_bus::EventBusError<T>>>()
        {
            bevy::prelude::App::add_event::<bevy_event_bus::EventBusError<T>>(self);
        }

        // Add custom decoder to registry
        let typed_decoder = TypedDecoder::new(decoder, std::any::type_name::<T>());

        if let Some(mut registry) = self.world_mut().get_resource_mut::<DecoderRegistry>() {
            registry.register_decoder(topic, typed_decoder);
        } else {
            let mut registry = DecoderRegistry::new();
            registry.register_decoder(topic, typed_decoder);
            self.insert_resource(registry);
        }

        tracing::info!(
            topic = %topic,
            event_type = std::any::type_name::<T>(),
            "Registered custom topic decoder"
        );

        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bevy_event_bus::EventBusPlugin;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Event)]
    struct TestMove {
        x: f32,
        y: f32,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Event)]
    struct TestAttack {
        damage: u32,
    }

    #[test]
    fn test_add_event_bevy_like() {
        let mut app = App::new();
        app.add_bus_event::<TestMove>("game_events");

        // Verify event was added to Bevy
        assert!(app.world().contains_resource::<Events<TestMove>>());

        // Verify decoder registry was created
        assert!(app.world().contains_resource::<DecoderRegistry>());

        // Verify JSON decoder was registered
        let registry = app.world().resource::<DecoderRegistry>();
        assert_eq!(registry.decoder_count("game_events"), 1);
    }

    #[test]
    fn test_register_json_decoder_multiple_topics() {
        let mut app = App::new();
        app.add_bus_event_multi::<TestMove>(&["game_events", "move_events", "input_events"]);

        // Verify event was added to Bevy
        assert!(app.world().contains_resource::<Events<TestMove>>());

        // Verify decoders were registered for all topics
        let registry = app.world().resource::<DecoderRegistry>();
        assert_eq!(registry.decoder_count("game_events"), 1);
        assert_eq!(registry.decoder_count("move_events"), 1);
        assert_eq!(registry.decoder_count("input_events"), 1);
    }

    #[test]
    fn test_many_to_many_event_topic_relationship() {
        let mut app = App::new();

        // Register TestMove on multiple topics using new API
        app.add_bus_event_multi::<TestMove>(&["game_events", "move_events"]);

        // Register TestAttack on some overlapping topics using new API
        app.add_bus_event_multi::<TestAttack>(&["game_events", "combat_events"]);

        let registry = app.world().resource::<DecoderRegistry>();

        // game_events should have both TestMove and TestAttack decoders
        assert_eq!(registry.decoder_count("game_events"), 2);

        // move_events should have only TestMove
        assert_eq!(registry.decoder_count("move_events"), 1);

        // combat_events should have only TestAttack
        assert_eq!(registry.decoder_count("combat_events"), 1);
    }

    #[test]
    fn test_mixed_registration_patterns() {
        let mut app = App::new();

        // Use the new single-topic API
        app.add_bus_event::<TestMove>("primary_events");

        // Use multi-topic registration
        app.add_bus_event_multi::<TestAttack>(&["primary_events", "secondary_events"]);

        let registry = app.world().resource::<DecoderRegistry>();

        // primary_events should have both events
        assert_eq!(registry.decoder_count("primary_events"), 2);

        // secondary_events should have only TestAttack
        assert_eq!(registry.decoder_count("secondary_events"), 1);
    }

    #[test]
    fn test_register_custom_decoder() {
        let mut app = App::new();

        app.register_topic_decoder("custom_topic", |data: &[u8]| {
            // Custom decoder that always returns a fixed TestMove
            if data.len() > 0 {
                Some(TestMove { x: 1.0, y: 2.0 })
            } else {
                None
            }
        });

        let registry = app.world().resource::<DecoderRegistry>();
        assert_eq!(registry.decoder_count("custom_topic"), 1);
    }

    // Multi-decoder tests moved from tests/multi_decoder_tests.rs

    #[derive(Event, Deserialize, Serialize, Debug, Clone, PartialEq)]
    struct PlayerMove {
        player_id: u32,
        x: f32,
        y: f32,
    }

    #[derive(Event, Deserialize, Serialize, Debug, Clone, PartialEq)]
    struct PlayerAttack {
        player_id: u32,
        target_id: u32,
        damage: i32,
    }

    /// Test that multiple event types can be decoded from the same topic
    #[test]
    fn test_single_topic_multiple_event_types() {
        let mut app = App::new();
        app.add_plugins(EventBusPlugin);

        // Register events in Bevy and set up JSON decoders for specific topic
        app.add_bus_event::<PlayerMove>("game_events");
        app.add_bus_event::<PlayerAttack>("game_events");

        app.update(); // Initialize systems

        // Verify decoders are registered
        let decoder_registry = app.world().resource::<DecoderRegistry>();
        let decoders = decoder_registry.get_decoders("game_events");
        assert_eq!(
            decoders.len(),
            2,
            "Should have 2 decoders for game_events topic"
        );

        // Create test messages
        let move_event = PlayerMove {
            player_id: 1,
            x: 10.0,
            y: 20.0,
        };
        let attack_event = PlayerAttack {
            player_id: 1,
            target_id: 2,
            damage: 50,
        };

        let move_json = serde_json::to_string(&move_event).unwrap();
        let attack_json = serde_json::to_string(&attack_event).unwrap();

        // Test decoding both message types
        let world = app.world_mut();
        let mut decoder_registry = world.resource_mut::<DecoderRegistry>();

        let decoded_move = decoder_registry.decode_all("game_events", move_json.as_bytes());
        let decoded_attack = decoder_registry.decode_all("game_events", attack_json.as_bytes());

        assert_eq!(
            decoded_move.len(),
            1,
            "Should decode 1 event from move message"
        );
        assert_eq!(
            decoded_attack.len(),
            1,
            "Should decode 1 event from attack message"
        );

        // Verify correct types were decoded
        let move_decoded = decoded_move[0]
            .as_any()
            .downcast_ref::<PlayerMove>()
            .unwrap();
        let attack_decoded = decoded_attack[0]
            .as_any()
            .downcast_ref::<PlayerAttack>()
            .unwrap();

        assert_eq!(*move_decoded, move_event);
        assert_eq!(*attack_decoded, attack_event);
    }

    /// Test that partial decode success is handled gracefully
    #[test]
    fn test_partial_decode_success() {
        let mut app = App::new();
        app.add_plugins(EventBusPlugin);

        // Register events and decoders for a topic
        app.add_bus_event::<PlayerMove>("mixed_events");
        // Note: We don't register PlayerAttack decoder for this topic

        app.update();

        let world = app.world_mut();
        let mut decoder_registry = world.resource_mut::<DecoderRegistry>();

        // Create a message that only matches PlayerMove decoder
        let move_event = PlayerMove {
            player_id: 1,
            x: 15.0,
            y: 25.0,
        };
        let move_json = serde_json::to_string(&move_event).unwrap();

        // Attempt to decode with all decoders
        let decoded_events = decoder_registry.decode_all("mixed_events", move_json.as_bytes());

        // Should succeed with only one decoder (PlayerMove)
        assert_eq!(decoded_events.len(), 1, "Should decode exactly 1 event");

        let decoded = decoded_events[0]
            .as_any()
            .downcast_ref::<PlayerMove>()
            .unwrap();
        assert_eq!(*decoded, move_event);
    }

    /// Test malformed message handling
    #[test]
    fn test_malformed_message_handling() {
        let mut app = App::new();
        app.add_plugins(EventBusPlugin);

        // Register both event types to listen on the same topic
        app.add_bus_event_multi::<PlayerMove>(&["robust_events"]);
        app.add_bus_event_multi::<PlayerAttack>(&["robust_events"]);

        app.update();

        let world = app.world_mut();
        let mut decoder_registry = world.resource_mut::<DecoderRegistry>();

        // Test various malformed messages
        let malformed_messages = vec![
            "",                                  // Empty message
            "not json at all",                   // Not JSON
            "{}",                                // Empty JSON object
            "{\"invalid\": \"structure\"}",      // Wrong structure
            "{\"player_id\": \"not_a_number\"}", // Wrong type
            "null",                              // Null JSON
            "[1, 2]",                            // Array with wrong number of elements
            "[1, 2, 3, 4]",                      // Array with too many elements
        ];

        for malformed_msg in malformed_messages {
            let decoded = decoder_registry.decode_all("robust_events", malformed_msg.as_bytes());
            assert_eq!(
                decoded.len(),
                0,
                "Malformed message '{}' should not decode to any events",
                malformed_msg
            );
        }
    }

    /// Test that decoder names are properly set
    #[test]
    fn test_decoder_names() {
        let mut app = App::new();
        app.add_plugins(EventBusPlugin);

        app.add_bus_event_multi::<PlayerMove>(&["named_topic"]);
        app.add_bus_event_multi::<PlayerAttack>(&["named_topic"]);

        app.update();

        let decoder_registry = app.world().resource::<DecoderRegistry>();
        let decoders = decoder_registry.get_decoders("named_topic");

        assert_eq!(decoders.len(), 2);

        let names: Vec<String> = decoders.iter().map(|d| d.name().to_string()).collect();
        // The actual type names will include the full module path
        println!("Decoder names: {:?}", names);

        // Check that both event types are present (allowing for different module paths)
        assert!(names.iter().any(|name| name.contains("PlayerMove")));
        assert!(names.iter().any(|name| name.contains("PlayerAttack")));
    }

    /// Test many-to-many relationship: one event type on multiple topics
    #[test]
    fn test_one_event_multiple_topics() {
        let mut app = App::new();
        app.add_plugins(EventBusPlugin);

        // Register PlayerMove for multiple topics using the new multi-topic API
        app.add_bus_event_multi::<PlayerMove>(&["game_events", "move_events", "input_events"]);

        app.update();

        let registry = app.world().resource::<DecoderRegistry>();

        // Verify PlayerMove decoder is registered on all topics
        assert_eq!(registry.decoder_count("game_events"), 1);
        assert_eq!(registry.decoder_count("move_events"), 1);
        assert_eq!(registry.decoder_count("input_events"), 1);

        // Test that the same event can be decoded from all topics
        let move_event = PlayerMove {
            player_id: 1,
            x: 10.0,
            y: 20.0,
        };
        let move_json = serde_json::to_string(&move_event).unwrap();

        let mut decoder_registry = app.world_mut().resource_mut::<DecoderRegistry>();

        // Test decoding from each topic
        for topic in ["game_events", "move_events", "input_events"] {
            let decoded = decoder_registry.decode_all(topic, move_json.as_bytes());
            assert_eq!(decoded.len(), 1, "Should decode 1 event from {}", topic);

            let decoded_move = decoded[0].as_any().downcast_ref::<PlayerMove>().unwrap();
            assert_eq!(*decoded_move, move_event);
        }
    }

    /// Test complex many-to-many scenario
    #[test]
    fn test_complex_many_to_many() {
        let mut app = App::new();
        app.add_plugins(EventBusPlugin);

        // Register PlayerMove on multiple topics
        app.add_bus_event_multi::<PlayerMove>(&["game_events", "move_events"]);

        // Register PlayerAttack on overlapping and different topics
        app.add_bus_event_multi::<PlayerAttack>(&["game_events", "combat_events"]);

        // Use the simple API to add another event to a single topic
        app.add_bus_event::<PlayerMove>("special_events");

        app.update();

        let registry = app.world().resource::<DecoderRegistry>();

        // game_events should have both PlayerMove and PlayerAttack decoders
        assert_eq!(registry.decoder_count("game_events"), 2);

        // move_events should have only PlayerMove
        assert_eq!(registry.decoder_count("move_events"), 1);

        // combat_events should have only PlayerAttack
        assert_eq!(registry.decoder_count("combat_events"), 1);

        // special_events should have only PlayerMove (from add_bus_event)
        assert_eq!(registry.decoder_count("special_events"), 1);

        // Test decoding different events from the shared topic
        let move_event = PlayerMove {
            player_id: 1,
            x: 10.0,
            y: 20.0,
        };
        let attack_event = PlayerAttack {
            player_id: 1,
            target_id: 2,
            damage: 50,
        };

        let move_json = serde_json::to_string(&move_event).unwrap();
        let attack_json = serde_json::to_string(&attack_event).unwrap();

        let mut decoder_registry = app.world_mut().resource_mut::<DecoderRegistry>();

        // Decode move event from game_events (should work with PlayerMove decoder)
        let decoded_move = decoder_registry.decode_all("game_events", move_json.as_bytes());
        assert_eq!(decoded_move.len(), 1);
        let decoded = decoded_move[0]
            .as_any()
            .downcast_ref::<PlayerMove>()
            .unwrap();
        assert_eq!(*decoded, move_event);

        // Decode attack event from game_events (should work with PlayerAttack decoder)
        let decoded_attack = decoder_registry.decode_all("game_events", attack_json.as_bytes());
        assert_eq!(decoded_attack.len(), 1);
        let decoded = decoded_attack[0]
            .as_any()
            .downcast_ref::<PlayerAttack>()
            .unwrap();
        assert_eq!(*decoded, attack_event);
    }
}
