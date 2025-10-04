use bevy::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Event, Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TestEvent {
    pub message: String,
    pub value: i32,
}

#[derive(Event, Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UserLoginEvent {
    pub user_id: String,
    pub timestamp: u64,
}
