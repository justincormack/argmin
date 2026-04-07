#![no_main]

use libfuzzer_sys::fuzz_target;
use server_http::http::router::route;
use server_http::http::xml::parse_url_encoded_tags;

fuzz_target!(|data: &[u8]| {
    let chunks = argmin_fuzz::split_input(data, 4);
    let method = match chunks[0].first().copied().unwrap_or_default() % 5 {
        0 => "GET",
        1 => "PUT",
        2 => "POST",
        3 => "DELETE",
        _ => "HEAD",
    };
    let path = format!("/{}", argmin_fuzz::lossy(chunks[1]));
    let query = argmin_fuzz::lossy(chunks[2]);
    let tags = argmin_fuzz::lossy(chunks[3]);

    let _ = route(method, &path, &query);
    let _ = parse_url_encoded_tags(&tags);
});
