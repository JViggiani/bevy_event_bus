#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisStreamSpec, RedisTopologyBuilder,
};
use bevy_event_bus::{EventBusPlugins, RedisEventReader};
use integration_tests::utils::events::TestEvent;
use integration_tests::utils::helpers::{unique_consumer_group, unique_topic};
use integration_tests::utils::redis_setup;

#[test]
fn idle_empty_stream_poll_does_not_block() {
    let stream = unique_topic("idle_test");
    let consumer_group = unique_consumer_group("idle_group");

    let mut builder = RedisTopologyBuilder::default();
    builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_consumer_group(
            consumer_group.clone(),
            RedisConsumerGroupSpec::new([stream.clone()], consumer_group.clone()),
        )
        .add_event_single::<TestEvent>(stream.clone());

    let (backend, _context) =
        redis_setup::setup_with_builder(builder).expect("Redis backend setup successful");

    let mut app = App::new();
    app.add_plugins(EventBusPlugins(backend));

    #[derive(Resource, Default)]
    struct ReadAttempts(u32);
    #[derive(Resource, Default)]
    struct EventsReceived(Vec<TestEvent>);

    app.insert_resource(ReadAttempts::default());
    app.insert_resource(EventsReceived::default());

    let stream_clone = stream.clone();
    let group_clone = consumer_group.clone();
    app.add_systems(
        Update,
        move |mut reader: RedisEventReader<TestEvent>,
              mut attempts: ResMut<ReadAttempts>,
              mut events: ResMut<EventsReceived>| {
            attempts.0 += 1;
            let config = RedisConsumerConfig::new(stream_clone.clone())
                .set_consumer_group(group_clone.clone())
                .read_block_timeout(std::time::Duration::from_millis(100)); // Short block time for test

            for wrapper in reader.read(&config) {
                events.0.push(wrapper.event().clone());
            }
        },
    );

    // Run several frames - should not block even with empty stream
    let start = std::time::Instant::now();
    for _ in 0..5 {
        app.update();
    }
    let elapsed = start.elapsed();

    // Should complete quickly despite multiple poll attempts on empty stream
    assert!(
        elapsed.as_millis() < 2000,
        "Polling empty stream took too long: {}ms",
        elapsed.as_millis()
    );

    let attempts = app.world().resource::<ReadAttempts>();
    assert_eq!(attempts.0, 5, "Should have made 5 read attempts");

    let events = app.world().resource::<EventsReceived>();
    assert_eq!(
        events.0.len(),
        0,
        "Should not receive any events from empty stream"
    );
}
