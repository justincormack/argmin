#[cfg(feature = "null-backend")]
pub use ec_null::*;

#[cfg(all(not(feature = "null-backend"), feature = "real-backend"))]
pub use ec_real::*;

#[cfg(not(any(feature = "real-backend", feature = "null-backend")))]
compile_error!("enable either the `real-backend` or `null-backend` feature for crate `ec`");
