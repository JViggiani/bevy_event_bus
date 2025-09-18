use crate::common::events::TestEvent;
use crate::common::helpers::{unique_topic, update_until};
use crate::common::setup::setup;
use bevy::prelude::*;
use bevy_event_bus::{
    EventBusConsumerConfig, EventBusPlugins, EventBusReader, EventBusWriter, EventBusAppExt,
};

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
    writer.add_systems(Update, move |mut w: EventBusWriter<TestEvent>| {
        for i in 0..15 {
            let _ = w.write(
                &tclone,
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
    reader.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>, mut c: ResMut<Collected>| {
            for ev in r.read(&tr) {
                c.0.push(ev.clone());
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
