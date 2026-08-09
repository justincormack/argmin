// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::env;

const OPENSSL_3_0_0: u64 = 0x3000_0000;

fn main() {
    if env::var_os("CARGO_FEATURE_OPENSSL").is_none() {
        return;
    }

    let version = env::var("DEP_OPENSSL_VERSION_NUMBER").unwrap_or_else(|_| {
        panic!("the argmin-crypto `openssl` feature requires OpenSSL 3.0 or later")
    });
    let version = u64::from_str_radix(version.trim_start_matches("0x"), 16)
        .expect("openssl-sys returned an invalid OpenSSL version number");
    assert!(
        version >= OPENSSL_3_0_0,
        "the argmin-crypto `openssl` feature requires OpenSSL 3.0 or later"
    );
}
