use bevy::prelude::*;
use bevy_event_bus::ExternalBusEvent;
use serde::{Deserialize, Serialize};

#[derive(ExternalBusEvent, Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TestEvent {
    pub message: String,
    pub value: i32,
}

#[derive(ExternalBusEvent, Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UserLoginEvent {
    pub user_id: String,
    pub timestamp: u64,
}
