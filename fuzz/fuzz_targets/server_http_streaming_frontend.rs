#![no_main]

use libfuzzer_sys::fuzz_target;
use server_http::http::request::TransportSecurity;

fuzz_target!(|data: &[u8]| {
    let chunks = argmin_fuzz::split_input(data, 11);
    let method = match chunks[0].first().copied().unwrap_or_default() % 6 {
        0 => "GET".to_string(),
        1 => "PUT".to_string(),
        2 => "POST".to_string(),
        3 => "DELETE".to_string(),
        4 => "HEAD".to_string(),
        _ => argmin_fuzz::lossy(chunks[0]),
    };
    let path = format!("/{}", argmin_fuzz::lossy(chunks[1]));
    let query = argmin_fuzz::lossy(chunks[2]);
    let uri = if query.is_empty() {
        path
    } else {
        format!("{path}?{query}")
    };

    let header_names = [
        "content-type",
        "content-encoding",
        "x-amz-content-sha256",
        "x-amz-decoded-content-length",
        "x-amz-trailer",
        "x-amz-copy-source",
        "authorization",
        "host",
    ];
    let mut headers = Vec::with_capacity(header_names.len());
    for (name, value) in header_names.iter().zip(chunks[3..].iter()) {
        headers.push(((*name).to_string(), argmin_fuzz::lossy(value)));
    }

    let transport = if chunks[0].first().copied().unwrap_or_default() & 1 == 0 {
        TransportSecurity::Tls
    } else {
        TransportSecurity::InsecureHttp
    };
    server_http::http::serve::fuzz_streaming_request_entrypoints(
        &method,
        &uri,
        &headers,
        transport,
    );
});
