//! Multi-decoder pipeline supporting multiple event types per topic

use bevy::prelude::*;
use bevy_event_bus::BusEvent;
use std::any::{Any, TypeId};
use std::collections::HashMap;

/// Function signature for event decoders that can decode bytes to a specific event type
pub trait DecoderFn: Send + Sync + 'static {
    /// Attempt to decode bytes into an event
    /// Returns Some(Box<dyn Any + Send + Sync>) if successful, None if this decoder cannot handle the data
    fn decode(&self, data: &[u8]) -> Option<Box<dyn Any + Send + Sync>>;

    /// Get the [`TypeId`] of the event type this decoder produces
    fn event_type_id(&self) -> TypeId;

    /// Get a human-readable name for this decoder (for debugging/logging)
    fn name(&self) -> &'static str;
}

type DecoderClosure<T> = dyn Fn(&[u8]) -> Option<T> + Send + Sync + 'static;
type BoxedDecoderClosure<T> = Box<DecoderClosure<T>>;

/// Concrete implementation of [`DecoderFn`] for a specific event type
pub struct TypedDecoder<T: BusEvent + Message> {
    decoder_fn: BoxedDecoderClosure<T>,
    name: &'static str,
}

impl<T: BusEvent + Message> TypedDecoder<T> {
    pub fn new<F>(decoder_fn: F, name: &'static str) -> Self
    where
        F: Fn(&[u8]) -> Option<T> + Send + Sync + 'static,
    {
        Self {
            decoder_fn: Box::new(decoder_fn),
            name,
        }
    }

    /// Create a default JSON decoder for a BusEvent type
    pub fn json_decoder() -> Self {
        Self::new(
            |bytes| serde_json::from_slice::<T>(bytes).ok(),
            std::any::type_name::<T>(),
        )
    }
}

impl<T: BusEvent + Message> DecoderFn for TypedDecoder<T> {
    fn decode(&self, data: &[u8]) -> Option<Box<dyn Any + Send + Sync>> {
        (self.decoder_fn)(data).map(|event| Box::new(event) as Box<dyn Any + Send + Sync>)
    }

    fn event_type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }

    fn name(&self) -> &'static str {
        self.name
    }
}

/// Registry mapping topics to multiple decoders, supporting decode priority/ordering
#[derive(Resource)]
pub struct DecoderRegistry {
    /// Maps topic name to ordered list of decoders (tried in order until one succeeds)
    topic_decoders: HashMap<String, Vec<Box<dyn DecoderFn>>>,

    /// Statistics for decoder performance and success rates
    decoder_stats: HashMap<&'static str, DecoderStats>,
}

#[derive(Debug, Clone, Default)]
pub struct DecoderStats {
    pub attempts: usize,
    pub successes: usize,
    pub failures: usize,
}

impl DecoderRegistry {
    pub fn new() -> Self {
        Self {
            topic_decoders: HashMap::new(),
            decoder_stats: HashMap::new(),
        }
    }

    /// Register a decoder for a specific topic
    pub fn register_decoder<T: BusEvent + Message>(
        &mut self,
        topic: &str,
        decoder: TypedDecoder<T>,
    ) {
        let decoder_name = decoder.name();
        let decoders = self.topic_decoders.entry(topic.to_string()).or_default();
        decoders.push(Box::new(decoder));
        tracing::debug!(
            topic = %topic,
            decoder = %decoder_name,
            total_decoders = decoders.len(),
            "Registered new decoder"
        );
    }

    /// Register a default JSON decoder for a BusEvent type on a topic
    pub fn register_json_decoder<T: BusEvent + Message>(&mut self, topic: &str) {
        self.register_decoder(topic, TypedDecoder::<T>::json_decoder());
    }

    /// Get all decoders registered for a topic (for testing/introspection)
    pub fn get_decoders(&self, topic: &str) -> &[Box<dyn DecoderFn>] {
        self.topic_decoders
            .get(topic)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Attempt to decode data using all registered decoders for a topic
    /// Returns a vector of successful decode results with their type information
    pub fn decode_all(&mut self, topic: &str, data: &[u8]) -> Vec<DecodedEvent> {
        let mut results = Vec::new();

        if let Some(decoders) = self.topic_decoders.get(topic) {
            for decoder in decoders {
                let decoder_name = decoder.name();
                let stats = self.decoder_stats.entry(decoder_name).or_default();
                stats.attempts += 1;

                match decoder.decode(data) {
                    Some(event_any) => {
                        stats.successes += 1;
                        results.push(DecodedEvent {
                            event: event_any,
                            type_id: decoder.event_type_id(),
                            decoder_name,
                        });
                        tracing::trace!(
                            topic = %topic,
                            decoder = %decoder_name,
                            "Decoder succeeded"
                        );
                    }
                    None => {
                        stats.failures += 1;
                        tracing::trace!(
                            topic = %topic,
                            decoder = %decoder_name,
                            "Decoder failed"
                        );
                    }
                }
            }
        }

        results
    }

    /// Get decoder statistics for debugging/monitoring
    pub fn get_stats(&self) -> &HashMap<&'static str, DecoderStats> {
        &self.decoder_stats
    }

    /// Get list of topics with registered decoders
    pub fn topics(&self) -> Vec<String> {
        self.topic_decoders.keys().cloned().collect()
    }

    /// Get number of decoders registered for a topic
    pub fn decoder_count(&self, topic: &str) -> usize {
        self.topic_decoders.get(topic).map(|v| v.len()).unwrap_or(0)
    }
}

impl Default for DecoderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of a successful decode operation
pub struct DecodedEvent {
    /// The decoded event as a type-erased box
    pub event: Box<dyn Any + Send + Sync>,
    /// TypeId to identify the event type
    pub type_id: TypeId,
    /// Name of the decoder that produced this result
    pub decoder_name: &'static str,
}

impl DecodedEvent {
    /// Attempt to downcast the event to a specific type
    pub fn downcast<T: 'static>(self) -> Result<T, Box<dyn Any + Send + Sync>> {
        self.event.downcast::<T>().map(|boxed| *boxed)
    }

    /// Check if this event is of a specific type
    pub fn is_type<T: 'static>(&self) -> bool {
        self.type_id == TypeId::of::<T>()
    }

    /// Get a reference to the type-erased event
    pub fn as_any(&self) -> &(dyn Any + Send + Sync) {
        self.event.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Message)]
    struct PlayerMove {
        x: f32,
        y: f32,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Message)]
    struct PlayerAttack {
        target: String,
        damage: u32,
    }

    #[test]
    fn test_decoder_registry_basic() {
        let mut registry = DecoderRegistry::new();

        // Register decoders for the same topic
        registry.register_json_decoder::<PlayerMove>("game_events");
        registry.register_json_decoder::<PlayerAttack>("game_events");

        assert_eq!(registry.decoder_count("game_events"), 2);
        assert_eq!(registry.decoder_count("unknown_topic"), 0);
    }

    #[test]
    fn test_multi_decoder_success() {
        let mut registry = DecoderRegistry::new();
        registry.register_json_decoder::<PlayerMove>("game_events");

        // Test PlayerMove JSON (register only PlayerMove decoder to isolate)
        let move_json = r#"{"x": 10.5, "y": 20.0}"#;
        let results = registry.decode_all("game_events", move_json.as_bytes());

        assert_eq!(
            results.len(),
            1,
            "Expected 1 decoded result, got {}",
            results.len()
        );

        // Debug: verify the actual event content by downcasting without type check
        let decoded_any = &results[0].event;
        println!(
            "Can downcast to PlayerMove: {}",
            decoded_any.downcast_ref::<PlayerMove>().is_some()
        );
        if let Some(player_move) = decoded_any.downcast_ref::<PlayerMove>() {
            println!("Decoded PlayerMove: {:?}", player_move);
        }

        // Debug: print type information
        println!(
            "Expected TypeId: {:?}",
            std::any::TypeId::of::<PlayerMove>()
        );
        println!("Actual TypeId: {:?}", results[0].type_id);
        println!("Decoder name: {}", results[0].decoder_name);

        // Try direct downcast instead of using is_type
        let decoded_move = results
            .into_iter()
            .next()
            .unwrap()
            .downcast::<PlayerMove>()
            .unwrap();
        assert_eq!(decoded_move, PlayerMove { x: 10.5, y: 20.0 });
    }

    #[test]
    fn test_multi_decoder_multiple_success() {
        let mut registry = DecoderRegistry::new();

        // Register custom decoders that can both succeed for the same data
        registry.register_decoder(
            "test_topic",
            TypedDecoder::new(
                |_| Some(PlayerMove { x: 1.0, y: 2.0 }),
                "custom_move_decoder",
            ),
        );
        registry.register_decoder(
            "test_topic",
            TypedDecoder::new(
                |_| {
                    Some(PlayerAttack {
                        target: "test".into(),
                        damage: 100,
                    })
                },
                "custom_attack_decoder",
            ),
        );

        let results = registry.decode_all("test_topic", b"any data");
        assert_eq!(results.len(), 2);

        // Verify both types were decoded by decoder name and successful downcast
        let move_result = results
            .iter()
            .find(|r| r.decoder_name == "custom_move_decoder")
            .unwrap();
        let attack_result = results
            .iter()
            .find(|r| r.decoder_name == "custom_attack_decoder")
            .unwrap();

        // Verify that downcasting works for each
        assert!(move_result.event.downcast_ref::<PlayerMove>().is_some());
        assert!(attack_result.event.downcast_ref::<PlayerAttack>().is_some());

        assert_eq!(move_result.decoder_name, "custom_move_decoder");
        assert_eq!(attack_result.decoder_name, "custom_attack_decoder");
    }

    #[test]
    fn test_decoder_failure_handling() {
        let mut registry = DecoderRegistry::new();
        registry.register_json_decoder::<PlayerMove>("game_events");

        // Invalid JSON should fail gracefully
        let invalid_json = b"not json at all";
        let results = registry.decode_all("game_events", invalid_json);

        assert_eq!(results.len(), 0);

        // Check that stats were recorded
        let stats = registry.get_stats();
        let decoder_stats = stats.get(std::any::type_name::<PlayerMove>()).unwrap();
        assert_eq!(decoder_stats.attempts, 1);
        assert_eq!(decoder_stats.failures, 1);
        assert_eq!(decoder_stats.successes, 0);
    }
}
