# bevy_event_bus

A Bevy plugin that connects Bevy's event system to external message brokers like Kafka.

## Features

- **Seamless integration with Bevy's event system**
  - Events are simultaneously sent to both Bevy's internal event system and external message brokers
  - Familiar API design following Bevy's conventions (EventBusReader/EventBusWriter)

- **Automatic event registration**
  - Simply derive `ExternalBusEvent` on your event types
  - No manual registration required

- **Topic-based messaging**
    - Send and receive events on specific topics (auto-subscribe on first read)
    - No manual subscription API required

- **Error handling**
  - Provides detailed error information for connectivity and serialization issues
  - "Try" methods that silently continue on errors for non-critical messaging

- **Backends**
  - Kafka support (with the "kafka" feature)
  - Easily extendable to support other message brokers

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
bevy_event_bus = "0.1"
```

With Kafka support:

```toml
[dependencies]
bevy_event_bus = { version = "0.1", features = ["kafka"] }
```

## Usage

### Define your events

```rust
use bevy::prelude::*;
use bevy_event_bus::prelude::*;

// Define an event - no manual registration needed!
#[derive(ExternalBusEvent, Clone, Debug)]
struct PlayerLevelUpEvent {
    entity_id: u64,
    new_level: u32,
}
```

### Set up the plugin

```rust
use bevy::prelude::*;
use bevy_event_bus::prelude::*;

fn main() {
    // Create a Kafka configuration
    let kafka_config = KafkaConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        group_id: "bevy_game".to_string(),
        ..Default::default()
    };
    
    // Create the Kafka backend
    let kafka_backend = KafkaEventBusBackend::new(kafka_config);

    App::new()
        .add_plugins(EventBusPlugins(kafka_backend))
        .add_systems(Update, (player_level_up_system, handle_level_ups))
        .run();
}
```

### Send events

```rust
// System that sends events
fn player_level_up_system(
    mut ev_writer: EventBusWriter<PlayerLevelUpEvent>,
    query: Query<(Entity, &PlayerXp, &PlayerLevel)>,
) {
    for (entity, xp, level) in query.iter() {
        if xp.0 >= level.next_level_requirement {
            // Send to specific topic - will also trigger Bevy event system
            let _ = ev_writer.send(
                "game-events.level-up",
                PlayerLevelUpEvent { 
                    entity_id: entity.to_bits(), 
                    new_level: level.0 + 1 
                }
            );
        }
    }
}
```

### Receive events

```rust
// System that receives events
fn handle_level_ups(mut ev_reader: EventBusReader<PlayerLevelUpEvent>) {
    for event in ev_reader.try_read("game-events.level-up") {
        println!("Entity {} leveled up to level {}!", event.entity_id, event.new_level);
    }
}
```

## Error Handling

Use `try_` methods for non-critical messaging:

```rust
// This will silently continue if the message cannot be sent
ev_writer.try_send("topic", event);
```

Or handle errors explicitly:

```rust
match ev_writer.send("topic", event) {
    Ok(_) => println!("Message sent successfully"),
    Err(e) => eprintln!("Failed to send message: {:?}", e),
}
```

## Kafka Backend Configuration

```rust
let config = KafkaConfig {
        bootstrap_servers: std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".into()),
        group_id: "bevy_game".to_string(),
        client_id: Some("game-client".to_string()),
        timeout_ms: 5000,
        additional_config: HashMap::new(),
};
let kafka_backend = KafkaEventBusBackend::new(config);

// Add plugin
App::new().add_plugins(EventBusPlugins(kafka_backend));

// Event sending: writer.send(topic, event)
// Event reading: reader.try_read(topic) -> iterator of &T

### Auto-Subscription
Reading from a topic automatically subscribes the consumer to that topic on first use.

### Additional Kafka Config Keys
Use `additional_config` to pass through arbitrary librdkafka properties (e.g. security, retries, acks).

Common keys:
* enable.idempotence=true
* message.timeout.ms=5000 (already set on producer)
* security.protocol=SSL
* ssl.ca.location=/path/to/ca.pem
* ssl.certificate.location=/path/to/cert.pem
* ssl.key.location=/path/to/key.pem

### Local Development (Docker)
You can spin up a single-node Kafka (KRaft) automatically in tests. The test harness will:
1. Try to start `bitnami/kafka:latest` exposing 9092 if `KAFKA_BOOTSTRAP_SERVERS` not set.
2. Poll metadata until the broker is ready.

Manual run:
```bash
docker run -d --rm --name bevy_event_bus_kafka -p 9092:9092 \
    -e KAFKA_ENABLE_KRAFT=yes \
    -e KAFKA_KRAFT_CLUSTER_ID=abcdefghijklmnopqrstuv \
    -e KAFKA_CFG_PROCESS_ROLES=broker,controller \
    -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
    -e KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true \
    bitnami/kafka:latest
```

Set `KAFKA_BOOTSTRAP_SERVERS` to override (e.g. in CI):
```bash
export KAFKA_BOOTSTRAP_SERVERS=my-broker:9092
```

### Testing Notes
Integration test `tests/integration/temp.rs` uses the docker harness or external broker.
It generates a unique topic name per run to avoid offset collisions.
```


With Kafka support:

```toml
[dependencies]
bevy_event_bus = { version = "0.1", features = ["kafka"] }
```

## Usage

### Define your events

```rust
use bevy::prelude::*;
use bevy_event_bus::prelude::*;

// Define an event - no manual registration needed!
#[derive(ExternalBusEvent, Clone, Debug)]
struct PlayerLevelUpEvent {
    entity_id: u64,
    new_level: u32,
}
```

### Set up the plugin

```rust
use bevy::prelude::*;
use bevy_event_bus::prelude::*;

fn main() {
    // Create a Kafka configuration
    let kafka_config = KafkaConfig {
        bootstrap_servers: "localhost:9092".to_string(),
        group_id: "bevy_game".to_string(),
        ..Default::default()
    };
    
    // Create the Kafka backend
    let kafka_backend = KafkaEventBusBackend::new(kafka_config);

    App::new()
        .add_plugins(EventBusPlugins(kafka_backend))
        .add_systems(Update, (player_level_up_system, handle_level_ups))
        .run();
}
```

### Send events

```rust
// System that sends events
fn player_level_up_system(
    mut ev_writer: EventBusWriter<PlayerLevelUpEvent>,
    query: Query<(Entity, &PlayerXp, &PlayerLevel)>,
) {
    for (entity, xp, level) in query.iter() {
        if xp.0 >= level.next_level_requirement {
            // Send to specific topic - will also trigger Bevy event system
            let _ = ev_writer.send(
                PlayerLevelUpEvent { 
                    entity_id: entity.to_bits(), 
                    new_level: level.0 + 1 
                },
                "game-events.level-up"
            );
        }
    }
}
```

### Receive events

```rust
// System that receives events
fn handle_level_ups(
    mut ev_reader: EventBusReader<PlayerLevelUpEvent>,
) {
    // Read from specific topic
    for event in ev_reader.try_read("game-events.level-up") {
        println!("Entity {} leveled up to level {}!", event.entity_id, event.new_level);
    }
}
```

## Error Handling

Use `try_` methods for non-critical messaging:

```rust
// This will silently continue if the message cannot be sent
ev_writer.try_send(event, "topic");
```

Or handle errors explicitly:

```rust
match ev_writer.send(event, "topic") {
    Ok(_) => println!("Message sent successfully"),
    Err(e) => eprintln!("Failed to send message: {:?}", e),
}
```

## Backend Configuration

### Kafka

```rust
let config = KafkaConfig {
    bootstrap_servers: "localhost:9092".to_string(),
    group_id: "bevy_game".to_string(),
    client_id: Some("game-client".to_string()),
    timeout_ms: 5000,
    additional_config: HashMap::new(),
};

let kafka_backend = KafkaEventBusBackend::new(config);
```
