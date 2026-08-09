// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

mod codec;
mod gf;
mod reconstruct;
#[cfg(test)]
mod tests;

pub use codec::{
    backend_name, self_test, EcConfig, EcError, ErasureCodec, VerifyResult, MAX_TOTAL_SHARDS,
};
