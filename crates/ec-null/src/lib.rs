mod codec;
#[cfg(test)]
mod tests;

pub use codec::{self_test, EcConfig, EcError, ErasureCodec, VerifyResult, MAX_TOTAL_SHARDS};
