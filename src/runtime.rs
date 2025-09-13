use bevy::prelude::*;
use once_cell::sync::OnceCell;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};

// Store an Arc so we can cheaply clone for the Bevy resource.
static RUNTIME: OnceCell<Arc<Runtime>> = OnceCell::new();

fn init_runtime() -> Arc<Runtime> {
    Arc::new(
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to build shared Tokio runtime"),
    )
}

/// Get a reference to the shared Tokio runtime (Arc inside OnceCell)
pub fn runtime() -> &'static Runtime { &*RUNTIME.get_or_init(init_runtime) }

/// Block on a future using the shared runtime.
pub fn block_on<F: std::future::Future>(fut: F) -> F::Output { runtime().block_on(fut) }

/// Bevy resource wrapper for a shared Tokio runtime (Arc for cheap clone into async tasks)
#[derive(Resource, Clone)]
pub struct SharedRuntime(pub Arc<Runtime>);

/// Ensure a SharedRuntime resource exists in the provided app.
pub fn ensure_runtime(app: &mut App) {
    if app.world().contains_resource::<SharedRuntime>() { return; }
    let arc = RUNTIME.get_or_init(init_runtime).clone();
    app.insert_resource(SharedRuntime(arc));
}
