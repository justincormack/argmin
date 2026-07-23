#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let chunks = argmin_fuzz::split_input(data, 8);
    let method = match chunks[0].first().copied().unwrap_or_default() % 6 {
        0 => "GET".to_string(),
        1 => "PUT".to_string(),
        2 => "POST".to_string(),
        3 => "DELETE".to_string(),
        4 => "HEAD".to_string(),
        _ => argmin_fuzz::lossy(chunks[0]),
    };
    let path_owned = format!("/{}", argmin_fuzz::lossy(chunks[1]));
    let query_owned = argmin_fuzz::lossy(chunks[2]);
    let authorization = argmin_fuzz::lossy(chunks[3]);
    let amz_date = argmin_fuzz::lossy(chunks[4]);
    let payload_hash = argmin_fuzz::lossy(chunks[5]);
    let signed_headers = argmin_fuzz::lossy(chunks[6]);
    let signature = argmin_fuzz::lossy(chunks[7]);

    let store = argmin_fuzz::seeded_store();

    let header_auth = vec![
        ("host", "examplebucket.s3.amazonaws.com"),
        ("x-amz-date", amz_date.as_str()),
        ("x-amz-content-sha256", payload_hash.as_str()),
        ("authorization", authorization.as_str()),
    ];
    let _ = auth::authenticate_request(
        &method,
        &path_owned,
        &query_owned,
        &header_auth,
        data,
        &store,
        auth::ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
        auth::SigningService::S3,
        1_700_000_000,
    );

    let presigned_query = format!(
        "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={}&X-Amz-Date={}&X-Amz-Expires={}&X-Amz-SignedHeaders={}&X-Amz-Signature={}",
        auth::canonical::uri_encode("testAccessKey123/20250101/us-east-1/s3/aws4_request"),
        auth::canonical::uri_encode(&amz_date),
        chunks[0].first().copied().unwrap_or(0),
        auth::canonical::uri_encode(&signed_headers),
        auth::canonical::uri_encode(&signature),
    );
    let presigned_headers = vec![("host", "examplebucket.s3.amazonaws.com")];
    let _ = auth::authenticate_request(
        "GET",
        &path_owned,
        &presigned_query,
        &presigned_headers,
        data,
        &store,
        auth::ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
        auth::SigningService::S3,
        1_700_000_000,
    );
});
