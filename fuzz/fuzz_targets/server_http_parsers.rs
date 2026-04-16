#![no_main]

use libfuzzer_sys::fuzz_target;
use server_http::http::router::route;
use server_http::http::xml::parse_url_encoded_tags;
use server_http::http::serve::fuzz_request_parser_entrypoints;

fuzz_target!(|data: &[u8]| {
    let chunks = argmin_fuzz::split_input(data, 11);
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
    let copy_source = argmin_fuzz::lossy(chunks[4]);
    let multipart_query = argmin_fuzz::lossy(chunks[5]);
    let list_multipart_query = argmin_fuzz::lossy(chunks[6]);
    let upload_part_query = argmin_fuzz::lossy(chunks[7]);
    let post_key = argmin_fuzz::lossy(chunks[9]);
    let file_name = argmin_fuzz::lossy(chunks[10]);

    let _ = route(method, &path, &query);
    let _ = parse_url_encoded_tags(&tags);
    fuzz_request_parser_entrypoints(
        &copy_source,
        chunks[8],
        &post_key,
        &file_name,
        &multipart_query,
        &list_multipart_query,
        &upload_part_query,
    );
});
