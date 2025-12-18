//! Integration test for comprehensive multi-decoder pipeline scenario
//!
//! This test demonstrates a realistic scenario with:
//! - Multiple event types (PlayerMove, PlayerAttack, GameStateUpdate)
//! - Multiple topics (game_events, combat_events, analytics_events)  
//! - Complex many-to-many relationships between events and topics
//! - Verification that decoders work correctly across all configurations
//!
//! This validates the many-to-many relationship capabilities of the event bus
//! in a real Kafka environment.

use bevy::prelude::*;
use bevy_event_bus::prelude::*;
use serde::{Deserialize, Serialize};

use bevy_event_bus::config::kafka::KafkaTopicSpec;
use integration_tests::utils::helpers::unique_topic;
use integration_tests::utils::kafka_setup;

// Event types for the comprehensive test
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Message)]
struct PlayerMove {
    player_id: u32,
    x: f32,
    y: f32,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Message)]
struct PlayerAttack {
    attacker_id: u32,
    target_id: u32,
    damage: u32,
    weapon: String,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Message)]
struct GameStateUpdate {
    level: u32,
    score: u64,
    players_online: u32,
    timestamp: u64,
}

/// Comprehensive integration test with multiple topics, events, and complex relationships
#[test]
fn test_multi_decoder() {
    let topic_game = unique_topic("game_events");
    let topic_combat = unique_topic("combat_events");
    let topic_analytics = unique_topic("analytics_events");

    // Set up a single app that demonstrates comprehensive many-to-many relationships
    let topic_game_cfg = topic_game.clone();
    let topic_combat_cfg = topic_combat.clone();
    let topic_analytics_cfg = topic_analytics.clone();
    let (backend, _) = kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
        builder.add_topic(
            KafkaTopicSpec::new(topic_game_cfg.clone())
                .partitions(1)
                .replication(1),
        );
        builder.add_topic(
            KafkaTopicSpec::new(topic_combat_cfg.clone())
                .partitions(1)
                .replication(1),
        );
        builder.add_topic(
            KafkaTopicSpec::new(topic_analytics_cfg.clone())
                .partitions(1)
                .replication(1),
        );
        builder.add_event::<PlayerMove>([topic_game_cfg.clone(), topic_combat_cfg.clone()]);
        builder.add_event_single::<PlayerAttack>(topic_combat_cfg.clone());
        builder.add_event::<GameStateUpdate>([topic_game_cfg.clone(), topic_analytics_cfg.clone()]);
    }));
    let mut app = App::new();
    app.add_plugins(EventBusPlugins(backend));

    // Initialize the app
    app.update();

    // Verify the decoder registrations are correct
    let registry = app.world().resource::<DecoderRegistry>();

    // Check game_events topic: should have PlayerMove and GameStateUpdate decoders
    assert_eq!(
        registry.decoder_count(&topic_game),
        2,
        "game_events should have 2 decoders (PlayerMove + GameStateUpdate)"
    );

    // Check combat_events topic: should have PlayerMove and PlayerAttack decoders
    assert_eq!(
        registry.decoder_count(&topic_combat),
        2,
        "combat_events should have 2 decoders (PlayerMove + PlayerAttack)"
    );

    // Check analytics_events topic: should have only GameStateUpdate decoder
    assert_eq!(
        registry.decoder_count(&topic_analytics),
        1,
        "analytics_events should have 1 decoder (GameStateUpdate)"
    );

    // Test that the decoders actually work by creating test messages and decoding them

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let move_event = PlayerMove {
        player_id: 1,
        x: 10.0,
        y: 20.0,
        timestamp,
    };

    let attack_event = PlayerAttack {
        attacker_id: 1,
        target_id: 2,
        damage: 50,
        weapon: "sword".to_string(),
        timestamp: timestamp + 1,
    };

    let state_event = GameStateUpdate {
        level: 5,
        score: 1000,
        players_online: 25,
        timestamp: timestamp + 2,
    };

    // Serialize test events to JSON
    let move_json = serde_json::to_string(&move_event).unwrap();
    let attack_json = serde_json::to_string(&attack_event).unwrap();
    let state_json = serde_json::to_string(&state_event).unwrap();

    let mut registry = app.world_mut().resource_mut::<DecoderRegistry>();

    // Test PlayerMove decoding from game_events topic
    let decoded_game_moves = registry.decode_all(&topic_game, move_json.as_bytes());
    assert_eq!(
        decoded_game_moves.len(),
        1,
        "Should decode PlayerMove from game_events"
    );
    let decoded_move = decoded_game_moves[0]
        .as_any()
        .downcast_ref::<PlayerMove>()
        .unwrap();
    assert_eq!(
        *decoded_move, move_event,
        "Decoded PlayerMove should match original"
    );

    // Test PlayerMove decoding from combat_events topic
    let decoded_combat_moves = registry.decode_all(&topic_combat, move_json.as_bytes());
    assert_eq!(
        decoded_combat_moves.len(),
        1,
        "Should decode PlayerMove from combat_events"
    );
    let decoded_move = decoded_combat_moves[0]
        .as_any()
        .downcast_ref::<PlayerMove>()
        .unwrap();
    assert_eq!(
        *decoded_move, move_event,
        "Decoded PlayerMove should match original"
    );

    // Test PlayerAttack decoding from combat_events topic (should work)
    let decoded_attacks = registry.decode_all(&topic_combat, attack_json.as_bytes());
    assert_eq!(
        decoded_attacks.len(),
        1,
        "Should decode PlayerAttack from combat_events"
    );
    let decoded_attack = decoded_attacks[0]
        .as_any()
        .downcast_ref::<PlayerAttack>()
        .unwrap();
    assert_eq!(
        *decoded_attack, attack_event,
        "Decoded PlayerAttack should match original"
    );

    // Test PlayerAttack decoding from game_events topic (should fail - no decoder)
    let decoded_game_attacks = registry.decode_all(&topic_game, attack_json.as_bytes());
    assert_eq!(
        decoded_game_attacks.len(),
        0,
        "Should not decode PlayerAttack from game_events"
    );

    // Test PlayerAttack decoding from analytics_events topic (should fail - no decoder)
    let decoded_analytics_attacks = registry.decode_all(&topic_analytics, attack_json.as_bytes());
    assert_eq!(
        decoded_analytics_attacks.len(),
        0,
        "Should not decode PlayerAttack from analytics_events"
    );

    // Test GameStateUpdate decoding from game_events topic
    let decoded_game_states = registry.decode_all(&topic_game, state_json.as_bytes());
    assert_eq!(
        decoded_game_states.len(),
        1,
        "Should decode GameStateUpdate from game_events"
    );
    let decoded_state = decoded_game_states[0]
        .as_any()
        .downcast_ref::<GameStateUpdate>()
        .unwrap();
    assert_eq!(
        *decoded_state, state_event,
        "Decoded GameStateUpdate should match original"
    );

    // Test GameStateUpdate decoding from analytics_events topic
    let decoded_analytics_states = registry.decode_all(&topic_analytics, state_json.as_bytes());
    assert_eq!(
        decoded_analytics_states.len(),
        1,
        "Should decode GameStateUpdate from analytics_events"
    );
    let decoded_state = decoded_analytics_states[0]
        .as_any()
        .downcast_ref::<GameStateUpdate>()
        .unwrap();
    assert_eq!(
        *decoded_state, state_event,
        "Decoded GameStateUpdate should match original"
    );

    // Test GameStateUpdate decoding from combat_events topic (should fail - no decoder)
    let decoded_combat_states = registry.decode_all(&topic_combat, state_json.as_bytes());
    assert_eq!(
        decoded_combat_states.len(),
        0,
        "Should not decode GameStateUpdate from combat_events"
    );

    println!("✅ Comprehensive multi-decoder pipeline test completed successfully!");
    println!("   - 3 event types across 3 topics");
    println!("   - Many-to-many event-topic relationships verified");
    println!("   - PlayerMove: game_events ✓, combat_events ✓");
    println!("   - PlayerAttack: combat_events ✓ (game_events ✗, analytics_events ✗)");
    println!("   - GameStateUpdate: game_events ✓, analytics_events ✓ (combat_events ✗)");
    println!("   - All decoders working correctly with proper isolation");
}
