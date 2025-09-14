use bevy::prelude::*;
use bevy_event_bus::{EventBusWriter, EventBusReader, EventBusPlugins, EventBusConsumerConfig, ConsumerMetrics};
use crate::common::events::TestEvent;
use crate::common::setup::setup;
use crate::common::helpers::{unique_topic, update_until};

#[test]
fn frame_limit_spreads_drain() {
    let (backend_w, _b1, _t1) = setup();
    let (backend_r, _b2, _t2) = setup();
    let topic = unique_topic("limit");
    let mut writer = App::new(); writer.add_plugins(EventBusPlugins(backend_w, bevy_event_bus::PreconfiguredTopics::new([topic.clone()])));
    let mut reader = App::new(); reader.add_plugins(EventBusPlugins(backend_r, bevy_event_bus::PreconfiguredTopics::new([topic.clone()])));
    let tclone = topic.clone();
    writer.add_systems(Update, move |mut w: EventBusWriter<TestEvent>| {
        for i in 0..15 { let _ = w.send(&tclone, TestEvent { message: format!("v{i}"), value: i }); }
    });
    writer.update();

    reader.insert_resource(EventBusConsumerConfig { max_events_per_frame: Some(5), max_drain_millis: None });
    #[derive(Resource, Default)] struct Collected(Vec<TestEvent>); reader.insert_resource(Collected::default());
    let tr = topic.clone();
    reader.add_systems(Update, move |mut r: EventBusReader<TestEvent>, mut c: ResMut<Collected>| { for ev in r.try_read(&tr) { c.0.push(ev.clone()); } });

    // Spin until all 15 collected or timeout
    let (ok, _frames) = update_until(&mut reader, 5000, |app| {
        let c = app.world().resource::<Collected>();
        c.0.len() >= 15
    });
    assert!(ok, "Timed out waiting for all events under frame limit");
    let collected = reader.world().resource::<Collected>();
    assert_eq!(collected.0.len(), 15);
}

#[test]
fn time_budget_limits_drain() {
    let (backend_w, _b1, _t1) = setup();
    let (backend_r, _b2, _t2) = setup();
    let topic = unique_topic("budget");
    let mut writer = App::new(); writer.add_plugins(EventBusPlugins(backend_w, bevy_event_bus::PreconfiguredTopics::new([topic.clone()])));
    let mut reader = App::new(); reader.add_plugins(EventBusPlugins(backend_r, bevy_event_bus::PreconfiguredTopics::new([topic.clone()])));
    let tclone2 = topic.clone();
    writer.add_systems(Update, move |mut w: EventBusWriter<TestEvent>| {
        for i in 0..200 { let _ = w.send(&tclone2, TestEvent { message: i.to_string(), value: i }); }
    });
    writer.update();

    reader.insert_resource(EventBusConsumerConfig { max_events_per_frame: None, max_drain_millis: Some(1) });
    #[derive(Resource, Default)] struct Collected(Vec<TestEvent>); reader.insert_resource(Collected::default());
    let tr2 = topic.clone();
    reader.add_systems(Update, move |mut r: EventBusReader<TestEvent>, mut c: ResMut<Collected>| { for ev in r.try_read(&tr2) { c.0.push(ev.clone()); } });

    // Run a frame; then inspect metrics to confirm partial drain
    reader.update();
    let metrics = reader.world().resource::<ConsumerMetrics>().clone();
    assert!(metrics.drain_duration_us > 0);
    // We expect not all 200 drained in single frame due to time budget
    let c = reader.world().resource::<Collected>();
    assert!(c.0.len() < 200);
}
