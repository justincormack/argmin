// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

#![no_main]

use auth::canonical::{parse_iso8601_utc_seconds_with_options, Iso8601UtcOptions};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let input = String::from_utf8_lossy(data);

    let _ = auth::parse_amz_date(&input);
    let _ = auth::parse_auth_header(&input);
    let _ = parse_iso8601_utc_seconds_with_options(
        &input,
        Iso8601UtcOptions {
            trim_whitespace: false,
            require_fixed_width_fields: true,
        },
    );
    let _ = parse_iso8601_utc_seconds_with_options(
        &input,
        Iso8601UtcOptions {
            trim_whitespace: true,
            require_fixed_width_fields: false,
        },
    );
});
