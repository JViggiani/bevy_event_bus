#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{RedisConsumerConfig, RedisConsumerGroupSpec, RedisStreamSpec};
use bevy_event_bus::{EventBusPlugins, RedisMessageReader};
use integration_tests::utils::events::TestEvent;
use integration_tests::utils::helpers::{unique_consumer_group_membership, unique_topic};
use integration_tests::utils::redis_setup::{self, SetupOptions};

#[test]
fn idle_empty_stream_poll_does_not_block() {
    let stream = unique_topic("idle");
    let membership = unique_consumer_group_membership("idle_read");
    let consumer_group = membership.group.clone();
    let consumer_name = membership.member.clone();

    let stream_for_backend = stream.clone();
    let group_for_backend = consumer_group.clone();
    let consumer_for_backend = consumer_name.clone();
    let options = SetupOptions::new()
        .read_block_timeout(std::time::Duration::from_millis(25))
        .pool_size(2)
        .insert_connection_config("client-name", "idle-read");

    let request = redis_setup::build_request(options, move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_backend.clone()))
            .add_consumer_group(RedisConsumerGroupSpec::new(
                [stream_for_backend.clone()],
                group_for_backend.clone(),
                consumer_for_backend.clone(),
            ))
            .add_event_single::<TestEvent>(stream_for_backend.clone());
    });

    let (backend, _context) = redis_setup::setup(request).expect("Redis backend setup successful");

    let mut app = App::new();
    app.add_plugins(EventBusPlugins(backend));

    #[derive(Resource, Default)]
    struct Ticks(u32);
    app.insert_resource(Ticks::default());

    let stream_read = stream.clone();
    let group_read = consumer_group.clone();
    app.add_systems(
        Update,
        move |mut reader: RedisMessageReader<TestEvent>, mut ticks: ResMut<Ticks>| {
            let config = RedisConsumerConfig::new(group_read.clone(), [stream_read.clone()])
                .read_block_timeout(std::time::Duration::from_millis(25));
            for _ in reader.read(&config) {
                // No events expected from idle stream
            }
            ticks.0 += 1;
        },
    );

    let start = std::time::Instant::now();
    for _ in 0..200 {
        app.update();
    }
    let elapsed_ms = start.elapsed().as_millis();
    let ticks = app.world().resource::<Ticks>().0;

    assert_eq!(ticks, 200, "All frames should execute (ticks={ticks})");
    assert!(
        elapsed_ms < 1_500,
        "Idle polling took too long: {elapsed_ms}ms"
    );
}
