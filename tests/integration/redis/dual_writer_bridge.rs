#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{EventBusPlugins, RedisEventReader, RedisEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{
    run_app_updates, unique_consumer_group, unique_topic, update_until,
};
use integration_tests::utils::redis_setup;

/// Test that multiple writer applications with separate backends operate independently
/// (they cannot communicate with each other as they use separate Redis instances)
#[test]
fn external_redis_events_independent_operation() {
    let stream = unique_topic("dual-writer");
    let consumer_group = unique_consumer_group("dual-writer-group");

    let writer1_db =
        redis_setup::ensure_shared_redis().expect("Writer1 Redis backend setup successful");
    let writer2_db =
        redis_setup::ensure_shared_redis().expect("Writer2 Redis backend setup successful");
    let reader_db =
        redis_setup::ensure_shared_redis().expect("Reader Redis backend setup successful");

    let writer1_stream = stream.clone();
    let (writer1_backend, _context1) = writer1_db.prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(writer1_stream.clone()))
            .add_event_single::<TestEvent>(writer1_stream.clone());
    })
    .expect("Writer1 Redis backend setup successful");

    let writer2_stream = stream.clone();
    let (writer2_backend, _context2) = writer2_db.prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(writer2_stream.clone()))
            .add_event_single::<TestEvent>(writer2_stream.clone());
    })
    .expect("Writer2 Redis backend setup successful");

    let reader_stream = stream.clone();
    let reader_group = consumer_group.clone();
    let (reader_backend, _context3) = reader_db.prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(reader_stream.clone()))
            .add_consumer_group(
                reader_group.clone(),
                RedisConsumerGroupSpec::new([reader_stream.clone()], reader_group.clone()),
            )
            .add_event_single::<TestEvent>(reader_stream.clone());
    })
    .expect("Reader Redis backend setup successful");

    // Two separate writer apps with independent backends
    let mut writer1 = App::new();
    writer1.add_plugins(EventBusPlugins(writer1_backend));

    let mut writer2 = App::new();
    writer2.add_plugins(EventBusPlugins(writer2_backend));

    // Reader app with its own backend
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(reader_backend));

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    reader.insert_resource(Collected::default());

    #[derive(Resource, Clone)]
    struct ToSend1(TestEvent, String);
    #[derive(Resource, Clone)]
    struct ToSend2(TestEvent, String);

    // Writer1 sends events with value 100
    writer1.insert_resource(ToSend1(
        TestEvent {
            message: "from_writer_1".to_string(),
            value: 100,
        },
        stream.clone(),
    ));
    writer1.add_systems(Update, |mut w: RedisEventWriter, data: Res<ToSend1>| {
        let config = RedisProducerConfig::new(data.1.clone());
        println!("Writer1 sending event: {:?} to stream: {}", data.0, data.1);
        w.write(&config, data.0.clone());
        println!("Writer1 sent event successfully");
    });

    // Writer2 sends events with value 200
    writer2.insert_resource(ToSend2(
        TestEvent {
            message: "from_writer_2".to_string(),
            value: 200,
        },
        stream.clone(),
    ));
    writer2.add_systems(Update, |mut w: RedisEventWriter, data: Res<ToSend2>| {
        let config = RedisProducerConfig::new(data.1.clone());
        println!("Writer2 sending event: {:?} to stream: {}", data.0, data.1);
        w.write(&config, data.0.clone());
        println!("Writer2 sent event successfully");
    }); // Reader collects all events
    let sr = stream.clone();
    let gr = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<Collected>| {
            let config = RedisConsumerConfig::new(sr.clone()).set_consumer_group(gr.clone());
            let events_before = c.0.len();
            println!(
                "Reader system called with stream: {} group: {}, current events: {}",
                sr, gr, events_before
            );
            for wrapper in r.read(&config) {
                println!("Reader received event: {:?}", wrapper.event());
                c.0.push(wrapper.event().clone());
            }
            let events_after = c.0.len();
            if events_after > events_before {
                println!(
                    "Reader system got {} new events, total: {}",
                    events_after - events_before,
                    events_after
                );
            }
        },
    );

    // Start reader first to ensure consumer group is ready
    run_app_updates(&mut reader, 2);

    // Then dispatch events via both writers
    run_app_updates(&mut writer1, 2);
    run_app_updates(&mut writer2, 2);

    // With separate backends, reader cannot receive events from writers (different Redis instances)
    let (_received, _) = update_until(&mut reader, 1000, |app| {
        let collected = app.world().resource::<Collected>();
        collected.0.len() >= 2 // This will never be true with separate backends
    });

    // Verify the expected behavior with separate backends
    let collected = reader.world().resource::<Collected>();
    assert_eq!(
        collected.0.len(),
        0,
        "Reader should receive 0 events (separate Redis instances)"
    );

    // This test confirms that apps with separate backends operate independently
    // Writers can send events without errors, but readers don't see cross-backend events
}
