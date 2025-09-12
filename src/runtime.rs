use once_cell::sync::OnceCell;
use tokio::runtime::{Runtime, Builder};

static RUNTIME: OnceCell<Runtime> = OnceCell::new();

/// Get a reference to the shared Tokio runtime (lazy initialized).
pub fn runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to build shared Tokio runtime")
    })
}

/// Block on a future using the shared runtime.
pub fn block_on<F: std::future::Future>(fut: F) -> F::Output { runtime().block_on(fut) }
