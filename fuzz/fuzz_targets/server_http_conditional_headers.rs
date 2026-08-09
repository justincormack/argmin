// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

#![no_main]

use libfuzzer_sys::fuzz_target;
use server_http::http::serve::{fuzz_conditional_header_entrypoints, ConditionalFuzzInputs};

fuzz_target!(|data: &[u8]| {
    let chunks = argmin_fuzz::split_input(data, 11);
    let if_match = argmin_fuzz::lossy(chunks[0]);
    let if_none_match = argmin_fuzz::lossy(chunks[1]);
    let if_modified_since = argmin_fuzz::lossy(chunks[2]);
    let if_unmodified_since = argmin_fuzz::lossy(chunks[3]);
    let copy_source_if_match = argmin_fuzz::lossy(chunks[4]);
    let copy_source_if_none_match = argmin_fuzz::lossy(chunks[5]);
    let copy_source_if_modified_since = argmin_fuzz::lossy(chunks[6]);
    let copy_source_if_unmodified_since = argmin_fuzz::lossy(chunks[7]);
    let delete_if_match_last_modified_time = argmin_fuzz::lossy(chunks[8]);
    let delete_if_match_size = argmin_fuzz::lossy(chunks[9]);
    let amz_date = argmin_fuzz::lossy(chunks[10]);

    fuzz_conditional_header_entrypoints(ConditionalFuzzInputs {
        if_match: &if_match,
        if_none_match: &if_none_match,
        if_modified_since: &if_modified_since,
        if_unmodified_since: &if_unmodified_since,
        copy_source_if_match: &copy_source_if_match,
        copy_source_if_none_match: &copy_source_if_none_match,
        copy_source_if_modified_since: &copy_source_if_modified_since,
        copy_source_if_unmodified_since: &copy_source_if_unmodified_since,
        delete_if_match_last_modified_time: &delete_if_match_last_modified_time,
        delete_if_match_size: &delete_if_match_size,
        amz_date: &amz_date,
    });
});
