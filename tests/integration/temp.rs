use bevy::prelude::*;
use bevy_event_bus::{EventBusPlugins, EventBusWriter, EventBusReader, EventBusAppExt};
use crate::common::{TestEvent};
use crate::common::setup::setup;

#[test]
fn test_basic_kafka_event_bus() {
    let (backend, _bootstrap) = setup();
    let topic = format!("bevy-event-bus-test-{}", uuid_suffix());

    // Broker is assured ready by setup(); proceed.

    // Writer app
    let mut writer_app = App::new();
    writer_app.add_plugins(EventBusPlugins(backend.clone()));
    writer_app.register_bus_event::<TestEvent>();

    #[derive(Resource, Clone)]
    struct ToSend(TestEvent, String);

    let event_to_send = TestEvent { message: "From Bevy!".into(), value: 100 };
    writer_app.insert_resource(ToSend(event_to_send.clone(), topic.clone()));

    fn writer_system(mut w: EventBusWriter<TestEvent>, data: Res<ToSend>) {
        let _ = w.send(&data.1, data.0.clone());
    }
    writer_app.add_systems(Update, writer_system);
    writer_app.update();

    // Small delay to allow Kafka publish
    std::thread::sleep(std::time::Duration::from_millis(300));

    // Reader app (separate consumer group)
    let mut reader_app = App::new();
    reader_app.add_plugins(EventBusPlugins(backend));
    reader_app.register_bus_event::<TestEvent>();

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    reader_app.insert_resource(Collected::default());
    #[derive(Resource, Clone)] struct Topic(String);
    reader_app.insert_resource(Topic(topic.clone()));

    fn reader_system(mut r: EventBusReader<TestEvent>, topic: Res<Topic>, mut collected: ResMut<Collected>) {
        for e in r.try_read(&topic.0) { collected.0.push(e.clone()); }
    }
    reader_app.add_systems(Update, reader_system);

    // Poll a few frames to accumulate
    for _ in 0..10 { reader_app.update(); std::thread::sleep(std::time::Duration::from_millis(300)); }

    let collected = reader_app.world().resource::<Collected>();
    assert!(collected.0.iter().any(|e| e == &event_to_send), "Expected to find sent event in collected list (collected={:?})", collected.0);
}

fn uuid_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("{}", nanos)
}
