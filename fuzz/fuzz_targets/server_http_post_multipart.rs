#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let chunks = argmin_fuzz::split_input(data, 3);
    let boundary = argmin_fuzz::lossy(chunks[0]);

    let _ = server_http::http::serve::fuzz_post_multipart_parser(
        &boundary,
        chunks[2],
        chunks[1],
    );
});
