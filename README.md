# bevy_event_bus

A Bevy plugin that bridges Bevy's event system with external message brokers such as Kafka and Redis. It keeps authoring ergonomics close to standard Bevy patterns while allowing for ingestion, decoding, and delivery of events to and from a bevy app.

## Features

- Topology-driven registration - declare topics, consumer groups, and event bindings up front via `KafkaTopologyBuilder`; the backend applies them automatically during startup.
- Multi-decoder pipeline – register multiple decoders per topic and fan decoded payloads out to matching Bevy events in a single pass.
- Type-safe configuration – reuse strongly typed consumer and producer configs across systems without exposing backend-specific traits in your signatures.
- Rich observability – drain metrics, lag snapshots, and commit statistics surface as Bevy resources/events ready for diagnostics.
- Extensible architecture – Kafka and Redis ship out of the box; additional brokers can plug in by implementing the backend trait.

## Installation

Add the crate to your `Cargo.toml` and enable the backend features you need:

```toml
[dependencies]
bevy_event_bus = { version = "0.2", features = ["kafka", "redis"] }
```

## Quick start

1. **Define your Bevy events.** Implementing `Serialize`, `Deserialize`, `Clone`, `Send`, and `Sync` is enough — the `BusEvent` marker trait is automatically implemented.
2. **Describe your topology.** Use the `KafkaTopologyBuilder` to declare topics, consumer groups, and which Bevy events bind to which topics.
3. **Spin up the plugin.** Build a `KafkaBackendConfig`, pass it to `KafkaEventBusBackend`, then add `EventBusPlugins` to your app.

```rust
use std::time::Duration;

use bevy::prelude::*;
use bevy_event_bus::prelude::*;
use bevy_event_bus::config::kafka::{
    KafkaBackendConfig, KafkaConnectionConfig, KafkaConsumerConfig, KafkaConsumerGroupSpec,
    KafkaInitialOffset, KafkaProducerConfig, KafkaTopologyBuilder, KafkaTopicSpec,
};

#[derive(Event, Clone, serde::Serialize, serde::Deserialize, Debug)]
struct PlayerLevelUp {
    player_id: u64,
    new_level: u32,
}

#[derive(Component)]
struct LevelComponent(u32);

fn main() {
    let topology = {
        let mut builder = KafkaTopologyBuilder::default();
        builder
            .add_topic(
                KafkaTopicSpec::new("game-events.level-up")
                    .partitions(3)
                    .replication(1),
            )
            .add_consumer_group(
                "game-servers",
                KafkaConsumerGroupSpec::new(["game-events.level-up"])
                    .initial_offset(KafkaInitialOffset::Earliest),
            )
            .add_event_single::<PlayerLevelUp>("game-events.level-up");
        builder.build()
    };

    let backend = KafkaEventBusBackend::new(KafkaBackendConfig::new(
        KafkaConnectionConfig::new("localhost:9092"),
        topology,
        Duration::from_secs(5),
    ));

    App::new()
        .add_plugins(EventBusPlugins(backend))
        .insert_resource(LevelUpProducerConfig::default())
        .insert_resource(LevelUpConsumerConfig::default())
        .add_systems(Update, (emit_level_ups, apply_level_ups))
        .run();
}

#[derive(Resource, Clone)]
struct LevelUpProducerConfig(KafkaProducerConfig);

impl Default for LevelUpProducerConfig {
    fn default() -> Self {
        Self(KafkaProducerConfig::new(["game-events.level-up"]).acks("all"))
    }
}

#[derive(Resource, Clone)]
struct LevelUpConsumerConfig(KafkaConsumerConfig);

impl Default for LevelUpConsumerConfig {
    fn default() -> Self {
        Self(
            KafkaConsumerConfig::new("game-servers", ["game-events.level-up"])
                .auto_offset_reset("earliest"),
        )
    }
}

fn emit_level_ups(
    mut writer: KafkaEventWriter,
    config: Res<LevelUpProducerConfig>,
    query: Query<(Entity, &LevelComponent), Added<LevelComponent>>,
) {
    for (entity, level) in &query {
        let event = PlayerLevelUp {
            player_id: entity.to_bits(),
            new_level: level.0,
        };
        writer.write(&config.0, event);
    }
}

fn apply_level_ups(
    mut reader: KafkaEventReader<PlayerLevelUp>,
    config: Res<LevelUpConsumerConfig>,
) {
    for wrapper in reader.read(&config.0) {
        info!(?wrapper.metadata(), "player leveled up");
    }
}
```

## Error handling

- Delivery failures surface as `EventBusError<T>` events. Add a Bevy system that reads `EventReader<EventBusError<T>>` to react to dropped messages, backend outages, or serialization issues.
- Deserialization issues emit `EventBusDecodeError` events. Metadata includes the raw payload, decoder name, and original topic so you can log or dead-letter messages.
- For Kafka-specific acknowledgement workflows, consume `KafkaCommitResultEvent` and inspect `KafkaCommitResultStats` for aggregate success/failure counts.

## Observability & diagnostics

The plugin inserts several resources once the backend activates:

- `ConsumerMetrics` tracks queue depths, per-frame drain counts, and idle frame streaks.
- `DrainedTopicMetadata` and `DecodedEventBuffer` expose the in-flight multi-decoder output.
- `KafkaLagCacheResource` (Kafka only) provides consumer lag estimates for each topic/partition.

Hook these into your diagnostics UI or telemetry exporters to keep an eye on the pipeline.

## Testing & performance

- The integration suite under `tests/` can launch a temporary Redpanda container when Docker is available. Set `KAFKA_BOOTSTRAP_SERVERS` to target an existing broker if you prefer to manage infrastructure yourself.
- Run `cargo test` for unit coverage and `./run_performance_tests.py` to capture throughput metrics. Compare new runs with `event_bus_perf_results.csv` to spot regressions; each row records the test name plus send/receive delta percentages so you can gauge improvement or drift at a glance.