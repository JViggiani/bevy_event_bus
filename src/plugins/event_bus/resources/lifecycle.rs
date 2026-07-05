use bevy::prelude::Resource;

use crate::backends::event_bus_backend::{
    BackendInstallSnapshot, LagReportingDescriptor, ManualCommitDescriptor,
};

#[derive(Resource, Clone)]
pub(crate) struct BackendReadyInfo {
    pub backend: String,
    pub topics: Vec<String>,
}

#[derive(Resource, Debug, Clone, Copy, Default)]
pub struct BackendStatus {
    pub ready: bool,
}

#[derive(Resource, Debug, Clone)]
pub struct BackendCapabilities {
    pub backend: String,
    pub message_stream: bool,
    pub manual_commit: Option<ManualCommitDescriptor>,
    pub lag_reporting: Option<LagReportingDescriptor>,
}

impl BackendCapabilities {
    pub(crate) fn from_install(backend: impl Into<String>, snapshot: BackendInstallSnapshot) -> Self {
        Self {
            backend: backend.into(),
            message_stream: snapshot.message_stream,
            manual_commit: snapshot.manual_commit,
            lag_reporting: snapshot.lag_reporting,
        }
    }
}
