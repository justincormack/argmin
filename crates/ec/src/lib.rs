#[cfg(feature = "native-backend")]
pub use ec_null::*;

#[cfg(all(not(feature = "native-backend"), feature = "real-backend"))]
pub use ec_real::*;

#[cfg(not(any(feature = "real-backend", feature = "native-backend")))]
compile_error!("enable either the `real-backend` or `native-backend` feature for crate `ec`");
