use bevy::prelude::*;
use bevy_event_bus::ExternalBusEvent;
use serde::{Deserialize, Serialize};

#[derive(ExternalBusEvent, Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct TestEvent {
    pub message: String,
    pub value: i32,
}

#[derive(ExternalBusEvent, Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct UserLoginEvent {
    pub user_id: String,
    pub timestamp: i64,
}
