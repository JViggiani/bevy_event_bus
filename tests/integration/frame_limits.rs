use bevy::prelude::*;
use bevy_event_bus::{
    EventBusAppExt, EventBusConsumerConfig, EventBusPlugins, KafkaEventReader, KafkaEventWriter,
};
use integration_tests::common::events::TestEvent;
use integration_tests::common::helpers::{
    DEFAULT_KAFKA_BOOTSTRAP, kafka_consumer_config, kafka_producer_config, unique_consumer_group,
    unique_topic, update_until,
};
use integration_tests::common::setup::setup;

#[test]
fn frame_limit_spreads_drain() {
    let (backend_w, _b1) = setup();
    let (backend_r, _b2) = setup();
    let topic = unique_topic("limit");
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(
        backend_w,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    writer.add_bus_event::<TestEvent>(&topic);

    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(
        backend_r,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    reader.add_bus_event::<TestEvent>(&topic);
    let tclone = topic.clone();
    writer.add_systems(Update, move |mut w: KafkaEventWriter| {
        for i in 0..15 {
            let _ = w.write(
                &kafka_producer_config(DEFAULT_KAFKA_BOOTSTRAP, [&tclone]),
                TestEvent {
                    message: format!("v{i}"),
                    value: i,
                },
            );
        }
    });
    writer.update();

    reader.insert_resource(EventBusConsumerConfig {
        max_events_per_frame: Some(5),
        max_drain_millis: None,
    });
    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    reader.insert_resource(Collected::default());
    let tr = topic.clone();
    let consumer_group = unique_consumer_group("frame_limit_reader");
    reader.add_systems(
        Update,
        move |mut r: KafkaEventReader<TestEvent>, mut c: ResMut<Collected>| {
            for wrapper in r.read(&kafka_consumer_config(
                DEFAULT_KAFKA_BOOTSTRAP,
                consumer_group.as_str(),
                [&tr],
            )) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // Spin until all 15 collected or timeout
    let (ok, _frames) = update_until(&mut reader, 5000, |app| {
        let c = app.world().resource::<Collected>();
        c.0.len() >= 15
    });
    assert!(ok, "Timed out waiting for all events under frame limit");
    let collected = reader.world().resource::<Collected>();
    assert_eq!(collected.0.len(), 15);
}
