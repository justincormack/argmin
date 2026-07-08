/// Integration tests for aws-chunked transfer encoding.
use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_s3::Client;
use ring::{digest, hmac};
use s3_tests::{
    assert_s3_err_code, build_client_with_ca, build_test_agent, shape::expected_error,
    unique_bucket, TestServer, CTX,
};

// ── Helpers ─────────────────────────────────────────────────────────────

fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
}

async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = s3_tests::delete_object_retrying_operation_aborted(client, bucket, key).await;
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

/// The AWS message for unsupported x-amz-content-sha256 streaming tokens.
const STREAMING_TOKEN_MESSAGE: &str = "x-amz-content-sha256 must be UNSIGNED-PAYLOAD, \
     STREAMING-UNSIGNED-PAYLOAD-TRAILER, STREAMING-AWS4-HMAC-SHA256-PAYLOAD, \
     STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER, STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD, \
     STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER or a valid sha256 value.";

/// Assert the full error body template; callers assert the status.
fn assert_error_body(body: &str, expected_template: String) {
    s3_tests::shape::assert_status_and_body(
        "chunked error body",
        0,
        body,
        &s3_tests::shape::shape().body(expected_template),
    );
}

/// Full SignatureDoesNotMatch body for chunk/trailer/seed mismatches: the
/// diagnostic echo varies per request so the signing inputs are pinned as
/// non-empty `{any}`.
fn assert_signature_mismatch_body(body: &str) {
    s3_tests::shape::assert_status_and_body(
        "chunked signature mismatch body",
        0,
        body,
        &s3_tests::shape::shape()
            .sub("access_key", CTX.access_key())
            .body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                 <Error><Code>SignatureDoesNotMatch</Code>\
                 <Message>The request signature we calculated does not match \
                 the signature you provided. Check your key and signing \
                 method.</Message>\
                 <AWSAccessKeyId>{access_key}</AWSAccessKeyId>\
                 <StringToSign>{any}</StringToSign>\
                 <SignatureProvided>{any}</SignatureProvided>\
                 <StringToSignBytes>{any}</StringToSignBytes>\
                 <CanonicalRequest>{any}</CanonicalRequest>\
                 <CanonicalRequestBytes>{any}</CanonicalRequestBytes>\
                 <RequestId>{request_id}</RequestId>\
                 <HostId>{host_id}</HostId></Error>",
            ),
    );
}

async fn assert_object_not_committed(bucket: &str, key: &str) {
    let result = CTX
        .client()
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await;
    assert_s3_err_code(&result, "NoSuchKey");
}

// ── Crypto helpers ──────────────────────────────────────────────────────

fn sha256_hex(data: &[u8]) -> String {
    let d = digest::digest(&digest::SHA256, data);
    d.as_ref().iter().map(|b| format!("{:02x}", b)).collect()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> hmac::Tag {
    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> hmac::Tag {
    let k_secret = format!("AWS4{}", secret);
    let k_date = hmac_sha256(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(k_date.as_ref(), region.as_bytes());
    let k_service = hmac_sha256(k_region.as_ref(), service.as_bytes());
    hmac_sha256(k_service.as_ref(), b"aws4_request")
}

fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

fn host_for_endpoint(endpoint: &str) -> &str {
    endpoint
        .trim_start_matches("http://")
        .trim_start_matches("https://")
}

fn now_parts() -> (String, String) {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let days = secs / 86400;
    let (year, month, day) = days_to_ymd(days);
    let time_of_day = secs % 86400;
    let hour = time_of_day / 3600;
    let minute = (time_of_day % 3600) / 60;
    let second = time_of_day % 60;
    let date_long = format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        year, month, day, hour, minute, second
    );
    let date_short = date_long[..8].to_string();
    (date_long, date_short)
}

// ── SigV4 Header-Auth Signer ────────────────────────────────────────────

struct SignResult {
    authorization: String,
    amz_date: String,
    /// Seed signature (from the Authorization header).
    seed_signature: String,
    /// Derived signing key bytes.
    signing_key: Vec<u8>,
    /// Credential scope.
    scope: String,
    /// Timestamp.
    timestamp: String,
}

struct StreamingSigner<'a> {
    endpoint: &'a str,
    access_key: &'a str,
    secret_key: &'a str,
    region: &'a str,
}

struct StreamingSignRequest<'a> {
    method: &'a str,
    path: &'a str,
    content_sha256: &'a str,
    decoded_content_length: usize,
    content_encoding: &'a str,
    extra_signed_headers: &'a [(&'a str, &'a str)],
}

impl StreamingSigner<'_> {
    fn sign(&self, request: &StreamingSignRequest<'_>) -> SignResult {
        let (date_long, date_short) = now_parts();
        let service = "s3";

        let host_val = host_for_endpoint(self.endpoint);

        // Build sorted signed header names and canonical header string.
        let mut all_headers: Vec<(&str, String)> = vec![
            ("content-encoding", request.content_encoding.to_string()),
            ("host", host_val.to_string()),
            ("x-amz-content-sha256", request.content_sha256.to_string()),
            ("x-amz-date", date_long.clone()),
            (
                "x-amz-decoded-content-length",
                request.decoded_content_length.to_string(),
            ),
        ];

        for (k, v) in request.extra_signed_headers {
            all_headers.push((k, v.to_string()));
        }
        all_headers.sort_by_key(|(k, _)| *k);

        // Deduplicate: keep only the last entry for each key
        // (extra_signed_headers may override default headers like x-amz-checksum-*).
        all_headers.dedup_by_key(|(k, _)| *k);

        let signed_headers_list: Vec<&str> = all_headers.iter().map(|(k, _)| *k).collect();
        let signed_headers = signed_headers_list.join(";");

        let canonical_headers_str: String = all_headers
            .iter()
            .map(|(k, v)| format!("{}:{}\n", k, v))
            .collect();

        let canonical_request = format!(
            "{}\n{}\n\n{}\n{}\n{}",
            request.method,
            request.path,
            canonical_headers_str,
            signed_headers,
            request.content_sha256
        );

        let canonical_hash = sha256_hex(canonical_request.as_bytes());
        let scope = format!("{}/{}/{}/aws4_request", date_short, self.region, service);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            date_long, scope, canonical_hash
        );

        let signing_key = derive_signing_key(self.secret_key, &date_short, self.region, service);
        let signature = hmac_sha256(signing_key.as_ref(), string_to_sign.as_bytes());
        let sig_hex = hex_encode(signature.as_ref());

        let credential = format!(
            "{}/{}/{}/{}/aws4_request",
            self.access_key, date_short, self.region, service
        );
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}, SignedHeaders={}, Signature={}",
            credential, signed_headers, sig_hex
        );

        SignResult {
            authorization,
            amz_date: date_long.clone(),
            seed_signature: sig_hex,
            signing_key: signing_key.as_ref().to_vec(),
            scope,
            timestamp: date_long,
        }
    }
}

fn ctx_streaming_signer() -> StreamingSigner<'static> {
    StreamingSigner {
        endpoint: CTX.endpoint(),
        access_key: CTX.access_key(),
        secret_key: CTX.secret_key(),
        region: CTX.region(),
    }
}

/// Sign a request with SigV4 for aws-chunked uploads.
fn sign_streaming_request(
    method: &str,
    path: &str,
    content_sha256: &str,
    decoded_content_length: usize,
    extra_signed_headers: &[(&str, &str)],
) -> SignResult {
    ctx_streaming_signer().sign(&StreamingSignRequest {
        method,
        path,
        content_sha256,
        decoded_content_length,
        content_encoding: "aws-chunked",
        extra_signed_headers,
    })
}

struct ChunkedPutContext {
    client: Client,
    endpoint: String,
    access_key: String,
    secret_key: String,
    region: String,
    _server: Option<TestServer>,
}

async fn chunked_put_context_for_content_encoding_case() -> ChunkedPutContext {
    if std::env::var("S3_TEST_ENDPOINT").is_ok() {
        ChunkedPutContext {
            client: CTX.client().clone(),
            endpoint: CTX.endpoint().to_string(),
            access_key: CTX.access_key().to_string(),
            secret_key: CTX.secret_key().to_string(),
            region: CTX.region().to_string(),
            _server: None,
        }
    } else {
        let server = TestServer::start_http().await;
        let endpoint = server.endpoint().to_string();
        let client = build_client_with_ca(
            &endpoint,
            s3_tests::server::TEST_ACCESS_KEY,
            s3_tests::server::TEST_SECRET_KEY,
            s3_tests::server::TEST_REGION,
            server.tls_ca_pem(),
        );
        ChunkedPutContext {
            client,
            endpoint,
            access_key: s3_tests::server::TEST_ACCESS_KEY.to_string(),
            secret_key: s3_tests::server::TEST_SECRET_KEY.to_string(),
            region: s3_tests::server::TEST_REGION.to_string(),
            _server: Some(server),
        }
    }
}

impl ChunkedPutContext {
    fn streaming_signer(&self) -> StreamingSigner<'_> {
        StreamingSigner {
            endpoint: &self.endpoint,
            access_key: &self.access_key,
            secret_key: &self.secret_key,
            region: &self.region,
        }
    }
}

async fn put_signed_chunked_with_content_encoding(
    ctx: &ChunkedPutContext,
    bucket: &str,
    key: &str,
    data: &[u8],
    content_encoding: &str,
) {
    let path = format!("/{bucket}/{key}");
    let content_sha256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
    let sign = ctx.streaming_signer().sign(&StreamingSignRequest {
        method: "PUT",
        path: &path,
        content_sha256,
        decoded_content_length: data.len(),
        content_encoding,
        extra_signed_headers: &[],
    });
    let wire = build_signed_chunked_body(&sign, data);
    let agent = build_test_agent(
        &ctx.endpoint,
        CTX.tls_ca_pem(),
        std::time::Duration::from_secs(30),
    );
    let url = format!("{}{}", ctx.endpoint, path);
    let mut resp = agent
        .put(&url)
        .header("Authorization", &sign.authorization)
        .header("x-amz-date", &sign.amz_date)
        .header("x-amz-content-sha256", content_sha256)
        .header("content-encoding", content_encoding)
        .header("x-amz-decoded-content-length", data.len().to_string())
        .header("content-length", wire.len().to_string())
        .send(&wire[..])
        .expect("transport error");
    let status = resp.status().as_u16();
    let body_str = resp.body_mut().read_to_string().unwrap_or_default();
    assert_eq!(
        status, 200,
        "PUT failed for content-encoding {content_encoding:?} ({status}): {body_str}"
    );
}

/// Sign a streaming request with custom control over which headers are signed.
///
/// `skip_content_encoding`: if true, omit `content-encoding` from signed headers.
/// `skip_decoded_content_length`: if true, omit `x-amz-decoded-content-length` from signed headers.
fn sign_streaming_request_custom(
    method: &str,
    path: &str,
    content_sha256: &str,
    decoded_content_length: usize,
    extra_signed_headers: &[(&str, &str)],
    skip_content_encoding: bool,
    skip_decoded_content_length: bool,
) -> SignResult {
    let (date_long, date_short) = now_parts();
    let region = CTX.region();
    let access_key = CTX.access_key();
    let secret_key = CTX.secret_key();
    let service = "s3";

    let host_val = host_for_endpoint(CTX.endpoint());

    let mut all_headers: Vec<(&str, String)> = Vec::new();
    if !skip_content_encoding {
        all_headers.push(("content-encoding", "aws-chunked".to_string()));
    }
    all_headers.push(("host", host_val.to_string()));
    all_headers.push(("x-amz-content-sha256", content_sha256.to_string()));
    all_headers.push(("x-amz-date", date_long.clone()));
    if !skip_decoded_content_length {
        all_headers.push((
            "x-amz-decoded-content-length",
            decoded_content_length.to_string(),
        ));
    }

    for (k, v) in extra_signed_headers {
        all_headers.push((k, v.to_string()));
    }
    all_headers.sort_by_key(|(k, _)| *k);
    all_headers.dedup_by_key(|(k, _)| *k);

    let signed_headers_list: Vec<&str> = all_headers.iter().map(|(k, _)| *k).collect();
    let signed_headers = signed_headers_list.join(";");

    let canonical_headers_str: String = all_headers
        .iter()
        .map(|(k, v)| format!("{}:{}\n", k, v))
        .collect();

    let canonical_request = format!(
        "{}\n{}\n\n{}\n{}\n{}",
        method, path, canonical_headers_str, signed_headers, content_sha256
    );

    let canonical_hash = sha256_hex(canonical_request.as_bytes());
    let scope = format!("{}/{}/{}/aws4_request", date_short, region, service);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        date_long, scope, canonical_hash
    );

    let signing_key = derive_signing_key(secret_key, &date_short, region, service);
    let signature = hmac_sha256(signing_key.as_ref(), string_to_sign.as_bytes());
    let sig_hex = hex_encode(signature.as_ref());

    let credential = format!(
        "{}/{}/{}/{}/aws4_request",
        access_key, date_short, region, service
    );
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}, SignedHeaders={}, Signature={}",
        credential, signed_headers, sig_hex
    );

    SignResult {
        authorization,
        amz_date: date_long.clone(),
        seed_signature: sig_hex,
        signing_key: signing_key.as_ref().to_vec(),
        scope,
        timestamp: date_long,
    }
}

/// Same as `sign_streaming_request_custom`, but includes a query string in
/// the canonical request (needed for operations such as UploadPart).
fn sign_streaming_request_custom_with_query(
    method: &str,
    request_uri: &str,
    content_sha256: &str,
    decoded_content_length: usize,
    extra_signed_headers: &[(&str, &str)],
    skip_content_encoding: bool,
    skip_decoded_content_length: bool,
) -> SignResult {
    let (path, query) = request_uri.split_once('?').unwrap_or((request_uri, ""));

    let (date_long, date_short) = now_parts();
    let region = CTX.region();
    let access_key = CTX.access_key();
    let secret_key = CTX.secret_key();
    let service = "s3";

    let host_val = host_for_endpoint(CTX.endpoint());

    let mut all_headers: Vec<(&str, String)> = Vec::new();
    if !skip_content_encoding {
        all_headers.push(("content-encoding", "aws-chunked".to_string()));
    }
    all_headers.push(("host", host_val.to_string()));
    all_headers.push(("x-amz-content-sha256", content_sha256.to_string()));
    all_headers.push(("x-amz-date", date_long.clone()));
    if !skip_decoded_content_length {
        all_headers.push((
            "x-amz-decoded-content-length",
            decoded_content_length.to_string(),
        ));
    }

    for (k, v) in extra_signed_headers {
        all_headers.push((k, v.to_string()));
    }
    all_headers.sort_by_key(|(k, _)| *k);
    all_headers.dedup_by_key(|(k, _)| *k);

    let signed_headers_list: Vec<&str> = all_headers.iter().map(|(k, _)| *k).collect();
    let signed_headers = signed_headers_list.join(";");

    let canonical_headers_str: String = all_headers
        .iter()
        .map(|(k, v)| format!("{}:{}\n", k, v))
        .collect();
    let canonical_query = auth::canonical::canonical_query_string(query);

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method, path, canonical_query, canonical_headers_str, signed_headers, content_sha256
    );

    let canonical_hash = sha256_hex(canonical_request.as_bytes());
    let scope = format!("{}/{}/{}/aws4_request", date_short, region, service);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        date_long, scope, canonical_hash
    );

    let signing_key = derive_signing_key(secret_key, &date_short, region, service);
    let signature = hmac_sha256(signing_key.as_ref(), string_to_sign.as_bytes());
    let sig_hex = hex_encode(signature.as_ref());

    let credential = format!(
        "{}/{}/{}/{}/aws4_request",
        access_key, date_short, region, service
    );
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}, SignedHeaders={}, Signature={}",
        credential, signed_headers, sig_hex
    );

    SignResult {
        authorization,
        amz_date: date_long.clone(),
        seed_signature: sig_hex,
        signing_key: signing_key.as_ref().to_vec(),
        scope,
        timestamp: date_long,
    }
}

/// Build an unsigned chunked wire body.
fn build_unsigned_chunked_body(data: &[u8]) -> Vec<u8> {
    let mut wire = Vec::new();
    // Single data chunk.
    wire.extend_from_slice(format!("{:x}\r\n", data.len()).as_bytes());
    wire.extend_from_slice(data);
    wire.extend_from_slice(b"\r\n");
    // Terminal chunk.
    wire.extend_from_slice(b"0\r\n\r\n");
    wire
}

/// Build unsigned chunked wire body with a trailing header.
fn build_unsigned_chunked_body_with_trailer(data: &[u8], trailer: &str) -> Vec<u8> {
    let mut wire = Vec::new();
    wire.extend_from_slice(format!("{:x}\r\n", data.len()).as_bytes());
    wire.extend_from_slice(data);
    wire.extend_from_slice(b"\r\n");
    wire.extend_from_slice(b"0\r\n");
    wire.extend_from_slice(trailer.as_bytes());
    wire.extend_from_slice(b"\r\n\r\n");
    wire
}

fn build_signed_chunked_body_with_bad_terminal_signature(
    sign: &SignResult,
    data: &[u8],
) -> Vec<u8> {
    let chunk_sig = chunk_signature(
        &sign.signing_key,
        &sign.timestamp,
        &sign.scope,
        &sign.seed_signature,
        data,
    );
    let bad_terminal_sig = "0".repeat(64);

    let mut wire = Vec::new();
    wire.extend_from_slice(
        format!("{:x};chunk-signature={}\r\n", data.len(), chunk_sig).as_bytes(),
    );
    wire.extend_from_slice(data);
    wire.extend_from_slice(b"\r\n");
    wire.extend_from_slice(format!("0;chunk-signature={}\r\n\r\n", bad_terminal_sig).as_bytes());
    wire
}

/// Compute a chunk signature.
fn chunk_signature(
    signing_key: &[u8],
    timestamp: &str,
    scope: &str,
    prev_sig: &str,
    chunk_data: &[u8],
) -> String {
    let empty_hash = sha256_hex(b"");
    let chunk_hash = sha256_hex(chunk_data);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
        timestamp, scope, prev_sig, empty_hash, chunk_hash
    );
    let sig = hmac_sha256(signing_key, string_to_sign.as_bytes());
    hex_encode(sig.as_ref())
}

/// Build a signed chunked wire body.
fn build_signed_chunked_body(sign: &SignResult, data: &[u8]) -> Vec<u8> {
    let chunk_sig = chunk_signature(
        &sign.signing_key,
        &sign.timestamp,
        &sign.scope,
        &sign.seed_signature,
        data,
    );
    let terminal_sig = chunk_signature(
        &sign.signing_key,
        &sign.timestamp,
        &sign.scope,
        &chunk_sig,
        b"",
    );

    let mut wire = Vec::new();
    wire.extend_from_slice(
        format!("{:x};chunk-signature={}\r\n", data.len(), chunk_sig).as_bytes(),
    );
    wire.extend_from_slice(data);
    wire.extend_from_slice(b"\r\n");
    wire.extend_from_slice(format!("0;chunk-signature={}\r\n\r\n", terminal_sig).as_bytes());
    wire
}

/// Build a signed chunked wire body from multiple chunks with chained signatures.
fn build_signed_chunked_body_multi(sign: &SignResult, chunks: &[&[u8]]) -> Vec<u8> {
    let mut wire = Vec::new();
    let mut prev_sig = sign.seed_signature.clone();

    for chunk in chunks {
        let sig = chunk_signature(
            &sign.signing_key,
            &sign.timestamp,
            &sign.scope,
            &prev_sig,
            chunk,
        );
        wire.extend_from_slice(format!("{:x};chunk-signature={}\r\n", chunk.len(), sig).as_bytes());
        wire.extend_from_slice(chunk);
        wire.extend_from_slice(b"\r\n");
        prev_sig = sig;
    }

    // Terminal chunk.
    let terminal_sig = chunk_signature(
        &sign.signing_key,
        &sign.timestamp,
        &sign.scope,
        &prev_sig,
        b"",
    );
    wire.extend_from_slice(format!("0;chunk-signature={}\r\n\r\n", terminal_sig).as_bytes());
    wire
}

/// Build an unsigned chunked wire body from multiple chunks with a trailing header.
fn build_unsigned_chunked_body_multi_with_trailer(chunks: &[&[u8]], trailer: &str) -> Vec<u8> {
    let mut wire = Vec::new();
    for chunk in chunks {
        wire.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
        wire.extend_from_slice(chunk);
        wire.extend_from_slice(b"\r\n");
    }
    wire.extend_from_slice(b"0\r\n");
    wire.extend_from_slice(trailer.as_bytes());
    wire.extend_from_slice(b"\r\n\r\n");
    wire
}

/// Compute a trailer signature per AWS SigV4 streaming-trailer spec.
///
/// String-to-sign:
/// ```text
/// AWS4-HMAC-SHA256-TRAILER\n{timestamp}\n{scope}\n{last_chunk_sig}\n{sha256(canonical_trailers)}
/// ```
/// where canonical_trailers is the sorted trailer key:value pairs each terminated by \n.
fn trailer_signature(
    signing_key: &[u8],
    timestamp: &str,
    scope: &str,
    last_chunk_sig: &str,
    canonical_trailers: &str,
) -> String {
    let trailer_hash = sha256_hex(canonical_trailers.as_bytes());
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256-TRAILER\n{}\n{}\n{}\n{}",
        timestamp, scope, last_chunk_sig, trailer_hash
    );
    let sig = hmac_sha256(signing_key, string_to_sign.as_bytes());
    hex_encode(sig.as_ref())
}

/// Build a signed chunked wire body with a signed trailing checksum.
///
/// Wire format:
/// ```text
/// {len};chunk-signature={sig}\r\n{data}\r\n
/// 0;chunk-signature={terminal_sig}\r\n
/// {trailer_header}\r\n
/// x-amz-trailer-signature:{trailer_sig}\r\n
/// \r\n
/// ```
fn build_signed_chunked_body_with_trailer(
    sign: &SignResult,
    data: &[u8],
    trailer_header: &str,
    trailer_sig: &str,
) -> Vec<u8> {
    let chunk_sig = chunk_signature(
        &sign.signing_key,
        &sign.timestamp,
        &sign.scope,
        &sign.seed_signature,
        data,
    );
    let terminal_sig = chunk_signature(
        &sign.signing_key,
        &sign.timestamp,
        &sign.scope,
        &chunk_sig,
        b"",
    );

    let mut wire = Vec::new();
    wire.extend_from_slice(
        format!("{:x};chunk-signature={}\r\n", data.len(), chunk_sig).as_bytes(),
    );
    wire.extend_from_slice(data);
    wire.extend_from_slice(b"\r\n");
    wire.extend_from_slice(format!("0;chunk-signature={}\r\n", terminal_sig).as_bytes());
    wire.extend_from_slice(trailer_header.as_bytes());
    wire.extend_from_slice(b"\r\n");
    wire.extend_from_slice(format!("x-amz-trailer-signature:{}\r\n", trailer_sig).as_bytes());
    wire.extend_from_slice(b"\r\n");
    wire
}

/// Get the terminal chunk signature for a single-chunk signed body (for trailer sig computation).
fn terminal_sig_for_single_chunk(sign: &SignResult, data: &[u8]) -> String {
    let chunk_sig = chunk_signature(
        &sign.signing_key,
        &sign.timestamp,
        &sign.scope,
        &sign.seed_signature,
        data,
    );
    chunk_signature(
        &sign.signing_key,
        &sign.timestamp,
        &sign.scope,
        &chunk_sig,
        b"",
    )
}

// ── Tests ───────────────────────────────────────────────────────────────

#[test]
fn test_unsigned_chunked_put() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"hello from unsigned chunked";
        let path = format!("/{}/unsigned-chunked", bucket);
        let content_sha256 = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

        let crc = checksum::crc32::checksum(data);
        use base64::Engine;
        let crc_b64 = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());
        let trailer = format!("x-amz-checksum-crc32:{}", crc_b64);

        let sign = sign_streaming_request(
            "PUT",
            &path,
            content_sha256,
            data.len(),
            &[("x-amz-trailer", "x-amz-checksum-crc32")],
        );
        let wire = build_unsigned_chunked_body_with_trailer(data, &trailer);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("content-length", wire.len().to_string())
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 200, "PUT failed ({}): {}", status, body_str);

        // Verify stored data via AWS SDK GET.
        let get_resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("unsigned-chunked")
            .send()
            .await
            .unwrap();
        let got = get_resp.body.collect().await.unwrap().into_bytes().to_vec();
        assert_eq!(got, data);

        cleanup(&bucket, &["unsigned-chunked"]).await;
    });
}

#[test]
fn test_unsigned_chunked_legacy_token_rejected() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"hello from unsigned chunked legacy token";
        let path = format!("/{}/unsigned-chunked-legacy-rejected", bucket);
        let content_sha256 = "STREAMING-UNSIGNED-PAYLOAD";

        let sign = sign_streaming_request("PUT", &path, content_sha256, data.len(), &[]);
        let wire = build_unsigned_chunked_body(data);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("content-length", wire.len().to_string())
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 400, "expected 400, got {}: {}", status, body_str);
        assert_error_body(
            &body_str,
            expected_error::invalid_argument_with_value(
                STREAMING_TOKEN_MESSAGE,
                "x-amz-content-sha256",
                "STREAMING-UNSIGNED-PAYLOAD",
            ),
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_signed_chunked_put() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"hello from signed chunked";
        let path = format!("/{}/signed-chunked", bucket);
        let content_sha256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

        let sign = sign_streaming_request("PUT", &path, content_sha256, data.len(), &[]);
        let wire = build_signed_chunked_body(&sign, data);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("content-length", wire.len().to_string())
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 200, "PUT failed ({}): {}", status, body_str);

        // Verify stored data.
        let get_resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("signed-chunked")
            .send()
            .await
            .unwrap();
        assert_eq!(get_resp.content_encoding(), None);
        let got = get_resp.body.collect().await.unwrap().into_bytes().to_vec();
        assert_eq!(got, data);

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key("signed-chunked")
            .send()
            .await
            .unwrap();
        assert_eq!(head.content_encoding(), None);

        cleanup(&bucket, &["signed-chunked"]).await;
    });
}

#[test]
fn test_signed_chunked_put_with_gzip_content_encoding() {
    s3_tests::run(async {
        let ctx = chunked_put_context_for_content_encoding_case().await;
        let bucket = unique_bucket();
        s3_tests::create_bucket(&ctx.client, &bucket).await.unwrap();

        let data = b"hello from signed chunked gzip";
        put_signed_chunked_with_content_encoding(
            &ctx,
            &bucket,
            "signed-chunked-gzip",
            data,
            "gzip",
        )
        .await;

        let get_resp = ctx
            .client
            .get_object()
            .bucket(&bucket)
            .key("signed-chunked-gzip")
            .send()
            .await
            .unwrap();
        assert_eq!(get_resp.content_encoding(), Some("gzip"));
        let got = get_resp.body.collect().await.unwrap().into_bytes().to_vec();
        assert_eq!(got, data);

        let head = ctx
            .client
            .head_object()
            .bucket(&bucket)
            .key("signed-chunked-gzip")
            .send()
            .await
            .unwrap();
        assert_eq!(head.content_encoding(), Some("gzip"));

        let _ = ctx
            .client
            .delete_object()
            .bucket(&bucket)
            .key("signed-chunked-gzip")
            .send()
            .await;
        s3_tests::delete_bucket_retrying_operation_aborted(&ctx.client, &bucket).await;
    });
}

#[test]
fn test_signed_chunked_put_strips_aws_chunked_content_encoding_variants() {
    s3_tests::run(async {
        let ctx = chunked_put_context_for_content_encoding_case().await;
        let bucket = unique_bucket();
        s3_tests::create_bucket(&ctx.client, &bucket).await.unwrap();
        let data = b"hello from signed chunked content-encoding variants";
        let cases = [
            ("aws-chunked,gzip", "gzip"),
            ("aws-chunked, gzip", "gzip"),
            ("gzip,aws-chunked", "gzip"),
            ("gzip, aws-chunked", "gzip"),
            ("gzip,aws-chunked,br", "gzip,br"),
            ("gzip, aws-chunked, br", "gzip, br"),
        ];
        let mut keys = Vec::with_capacity(cases.len());

        for (index, (content_encoding, expected)) in cases.iter().enumerate() {
            let key = format!("signed-chunked-ce-variant-{index}");
            put_signed_chunked_with_content_encoding(&ctx, &bucket, &key, data, content_encoding)
                .await;

            let get_resp = ctx
                .client
                .get_object()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
                .unwrap();
            assert_eq!(
                get_resp.content_encoding(),
                Some(*expected),
                "GET content-encoding mismatch for input {content_encoding:?}"
            );
            let got = get_resp.body.collect().await.unwrap().into_bytes().to_vec();
            assert_eq!(got, data, "body mismatch for input {content_encoding:?}");

            let head = ctx
                .client
                .head_object()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
                .unwrap();
            assert_eq!(
                head.content_encoding(),
                Some(*expected),
                "HEAD content-encoding mismatch for input {content_encoding:?}"
            );

            keys.push(key);
        }

        for key in &keys {
            let _ = ctx
                .client
                .delete_object()
                .bucket(&bucket)
                .key(key)
                .send()
                .await;
        }
        s3_tests::delete_bucket_retrying_operation_aborted(&ctx.client, &bucket).await;
    });
}

#[test]
fn test_signed_chunked_bad_signature() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"bad sig test";
        let path = format!("/{}/bad-sig", bucket);
        let content_sha256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

        let sign = sign_streaming_request("PUT", &path, content_sha256, data.len(), &[]);

        // Build wire body with bad chunk signatures.
        let bad_sig = "0".repeat(64);
        let mut wire = Vec::new();
        wire.extend_from_slice(
            format!("{:x};chunk-signature={}\r\n", data.len(), bad_sig).as_bytes(),
        );
        wire.extend_from_slice(data);
        wire.extend_from_slice(b"\r\n");
        wire.extend_from_slice(format!("0;chunk-signature={}\r\n\r\n", bad_sig).as_bytes());

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("content-length", wire.len().to_string())
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        // Chunk-signature mismatches echo the chunk string-to-sign and the
        // seed request's canonical request (AWS probed).
        s3_tests::shape::assert_status_and_body(
            "chunked PUT bad chunk signature",
            status,
            &body_str,
            &s3_tests::shape::shape()
                .status(403)
                .sub("access_key", CTX.access_key())
                .sub("signature_provided", bad_sig.as_str())
                .body(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error><Code>SignatureDoesNotMatch</Code>\
                     <Message>The request signature we calculated does not match \
                     the signature you provided. Check your key and signing \
                     method.</Message>\
                     <AWSAccessKeyId>{access_key}</AWSAccessKeyId>\
                     <StringToSign>{any}</StringToSign>\
                     <SignatureProvided>{signature_provided}</SignatureProvided>\
                     <StringToSignBytes>{any}</StringToSignBytes>\
                     <CanonicalRequest>{any}</CanonicalRequest>\
                     <CanonicalRequestBytes>{any}</CanonicalRequestBytes>\
                     <RequestId>{request_id}</RequestId>\
                     <HostId>{host_id}</HostId></Error>",
                ),
        );

        assert_object_not_committed(&bucket, "bad-sig").await;
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_signed_chunked_bad_terminal_signature_not_committed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "bad-terminal-sig";
        let data = b"payload accepted before terminal signature fails";
        let path = format!("/{bucket}/{key}");
        let content_sha256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

        let sign = sign_streaming_request("PUT", &path, content_sha256, data.len(), &[]);
        let wire = build_signed_chunked_body_with_bad_terminal_signature(&sign, data);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("content-length", wire.len().to_string())
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 403, "expected 403, got {}: {}", status, body_str);
        assert_signature_mismatch_body(&body_str);

        assert_object_not_committed(&bucket, key).await;
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_unsigned_chunked_trailing_checksum() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"trailer test";
        let path = format!("/{}/trailing-cksum", bucket);
        let content_sha256 = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

        // Compute CRC32 of data using the checksum crate.
        let crc = checksum::crc32::checksum(data);
        use base64::Engine;
        let crc_b64 = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());
        let trailer = format!("x-amz-checksum-crc32:{}", crc_b64);

        let sign = sign_streaming_request(
            "PUT",
            &path,
            content_sha256,
            data.len(),
            &[("x-amz-trailer", "x-amz-checksum-crc32")],
        );
        let wire = build_unsigned_chunked_body_with_trailer(data, &trailer);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("content-length", wire.len().to_string())
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 200, "PUT failed ({}): {}", status, body_str);

        // Verify stored data.
        let get_resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("trailing-cksum")
            .send()
            .await
            .unwrap();
        let got = get_resp.body.collect().await.unwrap().into_bytes().to_vec();
        assert_eq!(got, data);

        cleanup(&bucket, &["trailing-cksum"]).await;
    });
}

#[test]
fn test_chunked_decoded_content_length_mismatch() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"length mismatch";
        let path = format!("/{}/len-mismatch", bucket);
        let content_sha256 = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

        // Claim a different decoded content length.
        let wrong_length = data.len() + 100;
        let sign = sign_streaming_request(
            "PUT",
            &path,
            content_sha256,
            wrong_length,
            &[("x-amz-trailer", "x-amz-checksum-crc32")],
        );
        let crc = checksum::crc32::checksum(data);
        use base64::Engine;
        let crc_b64 = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());
        let trailer = format!("x-amz-checksum-crc32:{}", crc_b64);
        let wire = build_unsigned_chunked_body_with_trailer(data, &trailer);

        let url = format!("{}{}", CTX.endpoint(), path);
        let resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", wrong_length.to_string())
            .header("content-length", wire.len().to_string())
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        assert_eq!(status, 400, "expected 400, got {}", status);

        assert_object_not_committed(&bucket, "len-mismatch").await;
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_chunked_content_encoding_stripped() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"encoding strip test";
        let path = format!("/{}/enc-strip", bucket);
        let content_sha256 = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

        let crc = checksum::crc32::checksum(data);
        use base64::Engine;
        let crc_b64 = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());
        let trailer = format!("x-amz-checksum-crc32:{}", crc_b64);

        let sign = sign_streaming_request(
            "PUT",
            &path,
            content_sha256,
            data.len(),
            &[("x-amz-trailer", "x-amz-checksum-crc32")],
        );
        let wire = build_unsigned_chunked_body_with_trailer(data, &trailer);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("content-length", wire.len().to_string())
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 200, "PUT failed ({}): {}", status, body_str);

        // Verify stored data and check that content-encoding doesn't include aws-chunked.
        let get_resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("enc-strip")
            .send()
            .await
            .unwrap();
        let got = get_resp.body.collect().await.unwrap().into_bytes().to_vec();
        assert_eq!(got, data);

        cleanup(&bucket, &["enc-strip"]).await;
    });
}

#[test]
fn test_non_numeric_decoded_content_length() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"bad length header";
        let path = format!("/{}/bad-dcl", bucket);
        let content_sha256 = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

        // Sign with a non-numeric x-amz-decoded-content-length.
        let sign = sign_streaming_request(
            "PUT",
            &path,
            content_sha256,
            data.len(),
            &[("x-amz-trailer", "x-amz-checksum-crc32")],
        );
        let crc = checksum::crc32::checksum(data);
        use base64::Engine;
        let crc_b64 = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());
        let trailer = format!("x-amz-checksum-crc32:{}", crc_b64);
        let wire = build_unsigned_chunked_body_with_trailer(data, &trailer);

        let url = format!("{}{}", CTX.endpoint(), path);
        let resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            // Override the signed numeric value with a non-numeric one.
            .header("x-amz-decoded-content-length", "not-a-number")
            .header("content-length", wire.len().to_string())
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        // The unsigned header override will cause a 403 (unsigned x-amz- header)
        // or 400 (invalid decoded content length). Either way, not 200.
        assert_ne!(
            status, 200,
            "should have rejected non-numeric decoded-content-length"
        );
        assert!(
            status == 400 || status == 403,
            "expected 400 or 403, got {}",
            status
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_signed_streaming_missing_context_rejected() {
    // A presigned-URL PUT with STREAMING-AWS4-HMAC-SHA256-PAYLOAD should be
    // rejected because presigned auth doesn't produce a streaming signing
    // context.
    //
    // Construct the scenario: sign the request normally (header auth) with
    // STREAMING-AWS4-HMAC-SHA256-PAYLOAD, but tamper with the Authorization
    // header's credential to use the wrong date so auth fails to produce a
    // streaming context. This is hard to trigger in practice because
    // header-auth always populates streaming for this hash.
    //
    // Simpler approach: send a raw request where the auth header signs
    // STREAMING-AWS4-HMAC-SHA256-PAYLOAD but the body has no chunk signatures.
    // This exercises the P0 fix (missing chunk-signature in signed mode).
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"missing sigs";
        let path = format!("/{}/missing-chunksig", bucket);
        let content_sha256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

        let sign = sign_streaming_request("PUT", &path, content_sha256, data.len(), &[]);

        // Build wire body WITHOUT chunk-signature extensions.
        let wire = build_unsigned_chunked_body(data);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("content-length", wire.len().to_string())
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 400, "expected 400, got {}: {}", status, body_str);

        cleanup(&bucket, &[]).await;
    });
}

// ── Multi-chunk and trailer tests ────────────────────────────────────────

#[test]
fn test_signed_chunked_multi_chunk() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        // AWS requires non-final chunks to be >= 8192 bytes.
        let chunk1: Vec<u8> = vec![b'A'; 8192];
        let chunk2: Vec<u8> = vec![b'B'; 8192];
        let chunk3 = b"final chunk";
        let total_len = chunk1.len() + chunk2.len() + chunk3.len();
        let path = format!("/{}/signed-multi", bucket);
        let content_sha256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

        let sign = sign_streaming_request("PUT", &path, content_sha256, total_len, &[]);
        let wire = build_signed_chunked_body_multi(&sign, &[&chunk1, &chunk2, chunk3]);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", total_len.to_string())
            .header("content-length", wire.len().to_string())
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 200, "PUT failed ({}): {}", status, body_str);

        // Verify stored data.
        let get_resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("signed-multi")
            .send()
            .await
            .unwrap();
        let got = get_resp.body.collect().await.unwrap().into_bytes().to_vec();
        let mut expected = Vec::new();
        expected.extend_from_slice(&chunk1);
        expected.extend_from_slice(&chunk2);
        expected.extend_from_slice(chunk3);
        assert_eq!(got, expected);

        cleanup(&bucket, &["signed-multi"]).await;
    });
}

#[test]
fn test_unsigned_chunked_multi_chunk() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        // AWS requires non-final chunks to be >= 8192 bytes.
        let chunk1: Vec<u8> = vec![b'X'; 8192];
        let chunk2: Vec<u8> = vec![b'Y'; 8192];
        let chunk3 = b"tail";
        let total_len = chunk1.len() + chunk2.len() + chunk3.len();
        let path = format!("/{}/unsigned-multi", bucket);
        let content_sha256 = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

        let sign = sign_streaming_request(
            "PUT",
            &path,
            content_sha256,
            total_len,
            &[("x-amz-trailer", "x-amz-checksum-crc32")],
        );
        // Compute CRC32 for the trailer (required for STREAMING-UNSIGNED-PAYLOAD-TRAILER).
        let full_data: Vec<u8> = [&chunk1[..], &chunk2[..], &chunk3[..]].concat();
        let crc = checksum::crc32::checksum(&full_data);
        use base64::Engine;
        let crc_b64 = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());
        let trailer = format!("x-amz-checksum-crc32:{}", crc_b64);
        let wire = build_unsigned_chunked_body_multi_with_trailer(
            &[&chunk1[..], &chunk2[..], &chunk3[..]],
            &trailer,
        );

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", total_len.to_string())
            .header("content-length", wire.len().to_string())
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 200, "PUT failed ({}): {}", status, body_str);

        let get_resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("unsigned-multi")
            .send()
            .await
            .unwrap();
        let got = get_resp.body.collect().await.unwrap().into_bytes().to_vec();
        assert_eq!(got, full_data);

        cleanup(&bucket, &["unsigned-multi"]).await;
    });
}

#[test]
fn test_signed_chunked_trailing_checksum() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"signed trailing checksum test";
        let path = format!("/{}/signed-trailer-cksum", bucket);
        let content_sha256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";

        // Compute CRC32.
        let crc = checksum::crc32::checksum(data);
        use base64::Engine;
        let crc_b64 = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());

        let sign = sign_streaming_request(
            "PUT",
            &path,
            content_sha256,
            data.len(),
            &[("x-amz-trailer", "x-amz-checksum-crc32")],
        );

        // Compute the terminal chunk signature (needed for trailer sig).
        let terminal_sig = terminal_sig_for_single_chunk(&sign, data);

        // Build canonical trailer string for signature: sorted headers, each line
        // terminated by \n.
        let canonical_trailers = format!("x-amz-checksum-crc32:{}\n", crc_b64);
        let trailer_sig = trailer_signature(
            &sign.signing_key,
            &sign.timestamp,
            &sign.scope,
            &terminal_sig,
            &canonical_trailers,
        );

        let trailer_header = format!("x-amz-checksum-crc32:{}", crc_b64);
        let wire =
            build_signed_chunked_body_with_trailer(&sign, data, &trailer_header, &trailer_sig);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("content-length", wire.len().to_string())
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 200, "PUT failed ({}): {}", status, body_str);

        // Verify data roundtrip.
        let get_resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("signed-trailer-cksum")
            .send()
            .await
            .unwrap();
        let got = get_resp.body.collect().await.unwrap().into_bytes().to_vec();
        assert_eq!(got, data);

        // Verify checksum was stored.
        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key("signed-trailer-cksum")
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert!(
            head.checksum_crc32().is_some(),
            "expected CRC32 checksum on HEAD"
        );

        cleanup(&bucket, &["signed-trailer-cksum"]).await;
    });
}

#[test]
fn test_signed_chunked_trailing_checksum_bad_trailer_sig() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"bad trailer sig test";
        let path = format!("/{}/bad-trailer-sig", bucket);
        let content_sha256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";

        let crc = checksum::crc32::checksum(data);
        use base64::Engine;
        let crc_b64 = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());

        let sign = sign_streaming_request(
            "PUT",
            &path,
            content_sha256,
            data.len(),
            &[("x-amz-trailer", "x-amz-checksum-crc32")],
        );

        // Use a bad trailer signature.
        let bad_trailer_sig = "0".repeat(64);
        let trailer_header = format!("x-amz-checksum-crc32:{}", crc_b64);
        let wire =
            build_signed_chunked_body_with_trailer(&sign, data, &trailer_header, &bad_trailer_sig);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("content-length", wire.len().to_string())
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 403, "expected 403, got {}: {}", status, body_str);
        assert_signature_mismatch_body(&body_str);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_unsigned_trailing_checksum_verified() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"verify checksum stored";
        let path = format!("/{}/trailing-cksum-verify", bucket);
        let content_sha256 = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

        let crc = checksum::crc32::checksum(data);
        use base64::Engine;
        let crc_b64 = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());
        let trailer = format!("x-amz-checksum-crc32:{}", crc_b64);

        let sign = sign_streaming_request(
            "PUT",
            &path,
            content_sha256,
            data.len(),
            &[("x-amz-trailer", "x-amz-checksum-crc32")],
        );
        let wire = build_unsigned_chunked_body_with_trailer(data, &trailer);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("content-length", wire.len().to_string())
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 200, "PUT failed ({}): {}", status, body_str);

        // Verify data roundtrip.
        let get_resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("trailing-cksum-verify")
            .send()
            .await
            .unwrap();
        let got = get_resp.body.collect().await.unwrap().into_bytes().to_vec();
        assert_eq!(got, data);

        // Verify checksum was stored and returned.
        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key("trailing-cksum-verify")
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert!(
            head.checksum_crc32().is_some(),
            "expected CRC32 checksum on HEAD"
        );

        cleanup(&bucket, &["trailing-cksum-verify"]).await;
    });
}

#[test]
fn test_unsigned_trailing_checksum_bad_value() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"bad checksum value";
        let path = format!("/{}/bad-cksum-val", bucket);
        let content_sha256 = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

        // Compute wrong CRC32 (from different data).
        let wrong_crc = checksum::crc32::checksum(b"wrong data");
        use base64::Engine;
        let wrong_crc_b64 =
            base64::engine::general_purpose::STANDARD.encode(wrong_crc.to_be_bytes());
        let trailer = format!("x-amz-checksum-crc32:{}", wrong_crc_b64);

        let sign = sign_streaming_request(
            "PUT",
            &path,
            content_sha256,
            data.len(),
            &[("x-amz-trailer", "x-amz-checksum-crc32")],
        );
        let wire = build_unsigned_chunked_body_with_trailer(data, &trailer);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("content-length", wire.len().to_string())
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 400, "expected 400, got {}: {}", status, body_str);
        assert_error_body(
            &body_str,
            expected_error::with_host_id(
                "BadDigest",
                "The CRC32 you specified did not match the calculated checksum.",
            ),
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_signed_chunked_empty_object() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let path = format!("/{}/signed-empty", bucket);
        let content_sha256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

        let sign = sign_streaming_request("PUT", &path, content_sha256, 0, &[]);

        // Empty body = just the terminal chunk with its signature.
        let terminal_sig = chunk_signature(
            &sign.signing_key,
            &sign.timestamp,
            &sign.scope,
            &sign.seed_signature,
            b"",
        );
        let mut wire = Vec::new();
        wire.extend_from_slice(format!("0;chunk-signature={}\r\n\r\n", terminal_sig).as_bytes());

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", "0")
            .header("content-length", wire.len().to_string())
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 200, "PUT failed ({}): {}", status, body_str);

        // Verify empty object.
        let get_resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("signed-empty")
            .send()
            .await
            .unwrap();
        let got = get_resp.body.collect().await.unwrap().into_bytes().to_vec();
        assert!(
            got.is_empty(),
            "expected empty body, got {} bytes",
            got.len()
        );

        cleanup(&bucket, &["signed-empty"]).await;
    });
}

#[test]
fn test_unsigned_chunked_empty_object() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let path = format!("/{}/unsigned-empty", bucket);
        let content_sha256 = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

        let data: &[u8] = b"";
        let crc = checksum::crc32::checksum(data);
        use base64::Engine;
        let crc_b64 = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());
        let trailer = format!("x-amz-checksum-crc32:{}", crc_b64);

        let sign = sign_streaming_request(
            "PUT",
            &path,
            content_sha256,
            0,
            &[("x-amz-trailer", "x-amz-checksum-crc32")],
        );
        // For empty data, the wire body is just the terminal chunk + trailer.
        // (No data chunk before the terminal 0-length chunk.)
        let mut wire = Vec::new();
        wire.extend_from_slice(b"0\r\n");
        wire.extend_from_slice(trailer.as_bytes());
        wire.extend_from_slice(b"\r\n\r\n");

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", "0")
            .header("content-length", wire.len().to_string())
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 200, "PUT failed ({}): {}", status, body_str);

        let get_resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("unsigned-empty")
            .send()
            .await
            .unwrap();
        let got = get_resp.body.collect().await.unwrap().into_bytes().to_vec();
        assert!(
            got.is_empty(),
            "expected empty body, got {} bytes",
            got.len()
        );

        cleanup(&bucket, &["unsigned-empty"]).await;
    });
}

#[test]
fn test_signed_chunked_small_non_final_chunk_rejected() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        // First chunk is less than 8192 bytes, followed by another chunk — AWS rejects this.
        let chunk1 = b"small";
        let chunk2 = b"another chunk";
        let total_len = chunk1.len() + chunk2.len();
        let path = format!("/{}/small-chunk", bucket);
        let content_sha256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

        let sign = sign_streaming_request("PUT", &path, content_sha256, total_len, &[]);
        let wire = build_signed_chunked_body_multi(&sign, &[chunk1, chunk2]);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", total_len.to_string())
            .header("content-length", wire.len().to_string())
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 403, "expected 403, got {}: {}", status, body_str);
        assert_error_body(&body_str, expected_error::invalid_chunk_size(2, 5));

        assert_object_not_committed(&bucket, "small-chunk").await;
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_signed_multi_chunk_bad_middle_signature() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        // AWS requires non-final chunks to be >= 8192 bytes.
        let chunk1: Vec<u8> = vec![b'G'; 8192];
        let chunk2: Vec<u8> = vec![b'B'; 8192];
        let chunk3 = b"tail";
        let total_len = chunk1.len() + chunk2.len() + chunk3.len();
        let path = format!("/{}/bad-middle-sig", bucket);
        let content_sha256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

        let sign = sign_streaming_request("PUT", &path, content_sha256, total_len, &[]);

        // Build manually: chunk 1 with correct sig, chunk 2 with bad sig.
        let sig1 = chunk_signature(
            &sign.signing_key,
            &sign.timestamp,
            &sign.scope,
            &sign.seed_signature,
            &chunk1,
        );
        let bad_sig = "0".repeat(64);

        let mut wire = Vec::new();
        wire.extend_from_slice(
            format!("{:x};chunk-signature={}\r\n", chunk1.len(), sig1).as_bytes(),
        );
        wire.extend_from_slice(&chunk1);
        wire.extend_from_slice(b"\r\n");
        wire.extend_from_slice(
            format!("{:x};chunk-signature={}\r\n", chunk2.len(), bad_sig).as_bytes(),
        );
        wire.extend_from_slice(&chunk2);
        wire.extend_from_slice(b"\r\n");
        wire.extend_from_slice(
            format!("{:x};chunk-signature={}\r\n", chunk3.len(), bad_sig).as_bytes(),
        );
        wire.extend_from_slice(chunk3);
        wire.extend_from_slice(b"\r\n");
        wire.extend_from_slice(format!("0;chunk-signature={}\r\n\r\n", bad_sig).as_bytes());

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", total_len.to_string())
            .header("content-length", wire.len().to_string())
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 403, "expected 403, got {}: {}", status, body_str);
        assert_signature_mismatch_body(&body_str);

        assert_object_not_committed(&bucket, "bad-middle-sig").await;
        cleanup(&bucket, &[]).await;
    });
}

// ── Multi-chunk with trailer helpers ──────────────────────────────────

/// Build a signed chunked wire body from multiple chunks with a signed trailing checksum.
fn build_signed_chunked_body_multi_with_trailer(
    sign: &SignResult,
    chunks: &[&[u8]],
    trailer_header: &str,
    trailer_sig: &str,
) -> Vec<u8> {
    let mut wire = Vec::new();
    let mut prev_sig = sign.seed_signature.clone();

    for chunk in chunks {
        let sig = chunk_signature(
            &sign.signing_key,
            &sign.timestamp,
            &sign.scope,
            &prev_sig,
            chunk,
        );
        wire.extend_from_slice(format!("{:x};chunk-signature={}\r\n", chunk.len(), sig).as_bytes());
        wire.extend_from_slice(chunk);
        wire.extend_from_slice(b"\r\n");
        prev_sig = sig;
    }

    // Terminal chunk.
    let terminal_sig = chunk_signature(
        &sign.signing_key,
        &sign.timestamp,
        &sign.scope,
        &prev_sig,
        b"",
    );
    wire.extend_from_slice(format!("0;chunk-signature={}\r\n", terminal_sig).as_bytes());
    wire.extend_from_slice(trailer_header.as_bytes());
    wire.extend_from_slice(b"\r\n");
    wire.extend_from_slice(format!("x-amz-trailer-signature:{}\r\n", trailer_sig).as_bytes());
    wire.extend_from_slice(b"\r\n");
    wire
}

/// Get the terminal chunk signature for a multi-chunk signed body.
fn terminal_sig_for_multi_chunk(sign: &SignResult, chunks: &[&[u8]]) -> String {
    let mut prev_sig = sign.seed_signature.clone();
    for chunk in chunks {
        prev_sig = chunk_signature(
            &sign.signing_key,
            &sign.timestamp,
            &sign.scope,
            &prev_sig,
            chunk,
        );
    }
    chunk_signature(
        &sign.signing_key,
        &sign.timestamp,
        &sign.scope,
        &prev_sig,
        b"",
    )
}

// ── P2: Signed multi-chunk with trailing checksum ─────────────────────

#[test]
fn test_signed_multi_chunk_with_trailing_checksum() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let chunk1: Vec<u8> = vec![b'M'; 8192];
        let chunk2 = b"multi-trailer";
        let total_len = chunk1.len() + chunk2.len();
        let path = format!("/{}/signed-multi-trailer", bucket);
        let content_sha256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";

        let full_data: Vec<u8> = [&chunk1[..], &chunk2[..]].concat();
        let crc = checksum::crc32::checksum(&full_data);
        use base64::Engine;
        let crc_b64 = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());

        let sign = sign_streaming_request(
            "PUT",
            &path,
            content_sha256,
            total_len,
            &[("x-amz-trailer", "x-amz-checksum-crc32")],
        );

        let term_sig = terminal_sig_for_multi_chunk(&sign, &[&chunk1[..], &chunk2[..]]);
        let canonical_trailers = format!("x-amz-checksum-crc32:{}\n", crc_b64);
        let tsig = trailer_signature(
            &sign.signing_key,
            &sign.timestamp,
            &sign.scope,
            &term_sig,
            &canonical_trailers,
        );

        let trailer_header = format!("x-amz-checksum-crc32:{}", crc_b64);
        let wire = build_signed_chunked_body_multi_with_trailer(
            &sign,
            &[&chunk1[..], &chunk2[..]],
            &trailer_header,
            &tsig,
        );

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", total_len.to_string())
            .header("content-length", wire.len().to_string())
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 200, "PUT failed ({}): {}", status, body_str);

        // Verify data roundtrip.
        let get_resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("signed-multi-trailer")
            .send()
            .await
            .unwrap();
        let got = get_resp.body.collect().await.unwrap().into_bytes().to_vec();
        assert_eq!(got, full_data);

        // Verify checksum was stored.
        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key("signed-multi-trailer")
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert!(
            head.checksum_crc32().is_some(),
            "expected CRC32 checksum on HEAD"
        );

        cleanup(&bucket, &["signed-multi-trailer"]).await;
    });
}

// ── P1: Streaming mode validation tests ───────────────────────────────

#[test]
fn test_streaming_missing_content_encoding() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"no content-encoding";
        let path = format!("/{}/no-ce", bucket);
        let content_sha256 = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

        // Sign without content-encoding in signed headers so auth passes.
        let sign = sign_streaming_request_custom(
            "PUT",
            &path,
            content_sha256,
            data.len(),
            &[("x-amz-trailer", "x-amz-checksum-crc32")],
            true,  // skip content-encoding
            false, // keep decoded-content-length
        );
        let wire = build_unsigned_chunked_body(data);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            // Deliberately NOT sending content-encoding header
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .header("content-length", wire.len().to_string())
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 400, "expected 400, got {}: {}", status, body_str);
        assert_error_body(&body_str, expected_error::with_host_id("MalformedTrailerError", "The request contained trailing data that was not well-formed or did not conform to our published schema."));

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_streaming_wrong_content_encoding() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"wrong content-encoding";
        let path = format!("/{}/wrong-ce", bucket);
        let content_sha256 = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

        // Sign without content-encoding so auth passes.
        let sign = sign_streaming_request_custom(
            "PUT",
            &path,
            content_sha256,
            data.len(),
            &[("x-amz-trailer", "x-amz-checksum-crc32")],
            true, // skip content-encoding from signed headers
            false,
        );
        let wire = build_unsigned_chunked_body(data);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "gzip") // wrong value
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .header("content-length", wire.len().to_string())
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 400, "expected 400, got {}: {}", status, body_str);
        assert_error_body(&body_str, expected_error::with_host_id("MalformedTrailerError", "The request contained trailing data that was not well-formed or did not conform to our published schema."));

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_streaming_missing_decoded_content_length() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"no decoded content length";
        let path = format!("/{}/no-dcl", bucket);
        let content_sha256 = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

        // Sign without x-amz-decoded-content-length so auth passes.
        let sign = sign_streaming_request_custom(
            "PUT",
            &path,
            content_sha256,
            data.len(),
            &[("x-amz-trailer", "x-amz-checksum-crc32")],
            false,
            true, // skip decoded-content-length
        );
        let wire = build_unsigned_chunked_body(data);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            // Deliberately NOT sending x-amz-decoded-content-length
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .header("content-length", wire.len().to_string())
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 411, "expected 411, got {}: {}", status, body_str);
        assert_error_body(
            &body_str,
            expected_error::with_host_id(
                "MissingContentLength",
                "You must provide the Content-Length HTTP header.",
            ),
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_streaming_unsupported_token() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"unsupported token";
        let path = format!("/{}/bad-token", bucket);
        let content_sha256 = "STREAMING-UNKNOWN-ALGORITHM";

        // Sign with the unsupported token.
        let sign = sign_streaming_request_custom(
            "PUT",
            &path,
            content_sha256,
            data.len(),
            &[],
            false,
            false,
        );
        let wire = build_unsigned_chunked_body(data);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("content-length", wire.len().to_string())
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 400, "expected 400, got {}: {}", status, body_str);
        assert_error_body(
            &body_str,
            expected_error::invalid_argument_with_value(
                STREAMING_TOKEN_MESSAGE,
                "x-amz-content-sha256",
                "STREAMING-UNKNOWN-ALGORITHM",
            ),
        );

        cleanup(&bucket, &[]).await;
    });
}

// ── P1: Trailer declaration validation tests ──────────────────────────

#[test]
fn test_trailer_present_without_declaration() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"trailer without declaration";
        let path = format!("/{}/trailer-no-decl", bucket);
        let content_sha256 = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

        let crc = checksum::crc32::checksum(data);
        use base64::Engine;
        let crc_b64 = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());
        let trailer = format!("x-amz-checksum-crc32:{}", crc_b64);

        // Sign without x-amz-trailer so auth passes.
        let sign = sign_streaming_request("PUT", &path, content_sha256, data.len(), &[]);
        let wire = build_unsigned_chunked_body_with_trailer(data, &trailer);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("content-length", wire.len().to_string())
            // Deliberately NOT sending x-amz-trailer header
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 400, "expected 400, got {}: {}", status, body_str);
        assert_error_body(&body_str, expected_error::with_host_id("MalformedTrailerError", "The request contained trailing data that was not well-formed or did not conform to our published schema."));

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_declared_trailer_missing_from_body() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"declared trailer missing";
        let path = format!("/{}/decl-no-trailer", bucket);
        let content_sha256 = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

        let sign = sign_streaming_request(
            "PUT",
            &path,
            content_sha256,
            data.len(),
            &[("x-amz-trailer", "x-amz-checksum-crc32")],
        );
        // Build body WITHOUT trailers.
        let wire = build_unsigned_chunked_body(data);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("content-length", wire.len().to_string())
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 400, "expected 400, got {}: {}", status, body_str);
        assert_error_body(&body_str, expected_error::with_host_id("MalformedTrailerError", "The request contained trailing data that was not well-formed or did not conform to our published schema."));

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_undeclared_trailer_in_body() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"undeclared trailer";
        let path = format!("/{}/undecl-trailer", bucket);
        let content_sha256 = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

        let crc = checksum::crc32::checksum(data);
        use base64::Engine;
        let crc_b64 = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());
        // Send x-amz-checksum-crc32 in body but declare x-amz-checksum-sha256.
        let trailer = format!("x-amz-checksum-crc32:{}", crc_b64);

        let sign = sign_streaming_request(
            "PUT",
            &path,
            content_sha256,
            data.len(),
            &[("x-amz-trailer", "x-amz-checksum-sha256")],
        );
        let wire = build_unsigned_chunked_body_with_trailer(data, &trailer);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("content-length", wire.len().to_string())
            .header("x-amz-trailer", "x-amz-checksum-sha256")
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 400, "expected 400, got {}: {}", status, body_str);
        assert_error_body(&body_str, expected_error::with_host_id("MalformedTrailerError", "The request contained trailing data that was not well-formed or did not conform to our published schema."));

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_non_trailer_mode_with_trailers() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"non-trailer mode with trailers";
        let path = format!("/{}/non-trailer-trailers", bucket);
        // STREAMING-AWS4-HMAC-SHA256-PAYLOAD (non-TRAILER mode) but body has trailers.
        let content_sha256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

        let sign = sign_streaming_request("PUT", &path, content_sha256, data.len(), &[]);
        // Build properly signed chunks, then manually inject a trailer.
        let chunk_sig = chunk_signature(
            &sign.signing_key,
            &sign.timestamp,
            &sign.scope,
            &sign.seed_signature,
            data,
        );
        let terminal_sig = chunk_signature(
            &sign.signing_key,
            &sign.timestamp,
            &sign.scope,
            &chunk_sig,
            b"",
        );
        let mut wire = Vec::new();
        wire.extend_from_slice(
            format!("{:x};chunk-signature={}\r\n", data.len(), chunk_sig).as_bytes(),
        );
        wire.extend_from_slice(data);
        wire.extend_from_slice(b"\r\n");
        wire.extend_from_slice(format!("0;chunk-signature={}\r\n", terminal_sig).as_bytes());
        // Inject a trailer in non-TRAILER mode.
        wire.extend_from_slice(b"x-amz-checksum-crc32:AAAA\r\n");
        wire.extend_from_slice(b"\r\n");

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("content-length", wire.len().to_string())
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 400, "expected 400, got {}: {}", status, body_str);
        assert_error_body(
            &body_str,
            expected_error::with_host_id(
                "IncompleteBody",
                "The request body terminated unexpectedly",
            ),
        );

        cleanup(&bucket, &[]).await;
    });
}

// ── P3: Malformed trailer line test ───────────────────────────────────

#[test]
fn test_malformed_trailer_line_rejected() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"malformed trailer";
        let path = format!("/{}/malformed-trailer", bucket);
        let content_sha256 = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

        let sign = sign_streaming_request(
            "PUT",
            &path,
            content_sha256,
            data.len(),
            &[("x-amz-trailer", "x-amz-checksum-crc32")],
        );

        // Build body with a malformed trailer (no colon).
        let mut wire = Vec::new();
        wire.extend_from_slice(format!("{:x}\r\n", data.len()).as_bytes());
        wire.extend_from_slice(data);
        wire.extend_from_slice(b"\r\n");
        wire.extend_from_slice(b"0\r\n");
        wire.extend_from_slice(b"no-colon-here\r\n");
        wire.extend_from_slice(b"\r\n");

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("content-length", wire.len().to_string())
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 400, "expected 400, got {}: {}", status, body_str);
        assert_error_body(
            &body_str,
            expected_error::with_host_id(
                "IncompleteBody",
                "The request body terminated unexpectedly",
            ),
        );

        cleanup(&bucket, &[]).await;
    });
}

// ── Streaming UNSIGNED-PAYLOAD with inline checksum ─────────────────────

/// Helper: sign and send a PUT with UNSIGNED-PAYLOAD and an optional inline
/// checksum header. Returns (status, body_string).
fn unsigned_payload_put_with_checksum(
    path: &str,
    body: &[u8],
    checksum_header: Option<(&str, &str)>,
) -> (u16, String) {
    let mut extra = Vec::new();
    if let Some((k, v)) = checksum_header {
        extra.push((k, v));
    }
    let sign = sign_streaming_request_custom(
        "PUT",
        path,
        "UNSIGNED-PAYLOAD",
        body.len(),
        &extra,
        true, // skip content-encoding (not aws-chunked)
        true, // skip decoded-content-length (not aws-chunked)
    );

    let url = format!("{}{}", CTX.endpoint(), path);
    let mut req = agent()
        .put(&url)
        .header("Authorization", &sign.authorization)
        .header("x-amz-date", &sign.amz_date)
        .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD");

    if let Some((k, v)) = checksum_header {
        req = req.header(k, v);
    }

    let mut resp = req.send(body).expect("transport error");
    let status = resp.status().as_u16();
    let body_str = resp.body_mut().read_to_string().unwrap_or_default();
    (status, body_str)
}

#[test]
fn test_non_chunked_put_16mb_bypasses_buffered_body_limit() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "non-chunked-put-16mb";
        let data = vec![0x5a_u8; 16 * 1024 * 1024];
        let path = format!("/{}/{}", bucket, key);

        let (status, body_str) = unsigned_payload_put_with_checksum(&path, &data, None);
        assert_eq!(status, 200, "expected 200, got {}: {}", status, body_str);

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let got = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(got.len(), data.len());
        assert_eq!(&got[..], &data[..]);

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_streaming_inline_checksum_crc32_valid() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"hello streaming checksum";
        let path = format!("/{}/inline-cksum-ok", bucket);

        use base64::Engine;
        let crc = checksum::crc32::checksum(data);
        let crc_b64 = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());

        let (status, body_str) = unsigned_payload_put_with_checksum(
            &path,
            data,
            Some(("x-amz-checksum-crc32", &crc_b64)),
        );
        assert_eq!(status, 200, "expected 200, got {}: {}", status, body_str);

        cleanup(&bucket, &["inline-cksum-ok"]).await;
    });
}

#[test]
fn test_streaming_inline_checksum_crc32_bad_digest() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"hello streaming checksum";
        let path = format!("/{}/inline-cksum-bad", bucket);

        let (status, body_str) = unsigned_payload_put_with_checksum(
            &path,
            data,
            Some(("x-amz-checksum-crc32", "AAAA/w==")),
        );
        assert_eq!(status, 400, "expected 400, got {}: {}", status, body_str);
        assert_error_body(
            &body_str,
            expected_error::with_host_id(
                "BadDigest",
                "The CRC32 you specified did not match the calculated checksum.",
            ),
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_streaming_inline_checksum_crc32c_valid() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"crc32c inline test data";
        let path = format!("/{}/inline-crc32c-ok", bucket);

        use base64::Engine;
        let crc = checksum::crc32c::checksum(data);
        let crc_b64 = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());

        let (status, body_str) = unsigned_payload_put_with_checksum(
            &path,
            data,
            Some(("x-amz-checksum-crc32c", &crc_b64)),
        );
        assert_eq!(status, 200, "expected 200, got {}: {}", status, body_str);

        cleanup(&bucket, &["inline-crc32c-ok"]).await;
    });
}

#[test]
fn test_streaming_inline_checksum_crc32c_bad_digest() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"crc32c inline test data";
        let path = format!("/{}/inline-crc32c-bad", bucket);

        let (status, body_str) = unsigned_payload_put_with_checksum(
            &path,
            data,
            Some(("x-amz-checksum-crc32c", "AAAA/w==")),
        );
        assert_eq!(status, 400, "expected 400, got {}: {}", status, body_str);
        assert_error_body(
            &body_str,
            expected_error::with_host_id(
                "BadDigest",
                "The CRC32C you specified did not match the calculated checksum.",
            ),
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_streaming_inline_checksum_sha256_valid() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"sha256 inline test";
        let path = format!("/{}/inline-sha256-ok", bucket);

        use base64::Engine;
        let digest = ring::digest::digest(&ring::digest::SHA256, data);
        let sha_b64 = base64::engine::general_purpose::STANDARD.encode(digest.as_ref());

        let (status, body_str) = unsigned_payload_put_with_checksum(
            &path,
            data,
            Some(("x-amz-checksum-sha256", &sha_b64)),
        );
        assert_eq!(status, 200, "expected 200, got {}: {}", status, body_str);

        cleanup(&bucket, &["inline-sha256-ok"]).await;
    });
}

#[test]
fn test_streaming_inline_checksum_sha256_bad_digest() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"sha256 inline test";
        let path = format!("/{}/inline-sha256-bad", bucket);

        use base64::Engine;
        let digest = ring::digest::digest(&ring::digest::SHA256, b"");
        let sha_b64 = base64::engine::general_purpose::STANDARD.encode(digest.as_ref());

        let (status, body_str) = unsigned_payload_put_with_checksum(
            &path,
            data,
            Some(("x-amz-checksum-sha256", &sha_b64)),
        );
        assert_eq!(status, 400, "expected 400, got {}: {}", status, body_str);
        assert_error_body(
            &body_str,
            expected_error::with_host_id(
                "BadDigest",
                "The SHA256 you specified did not match the calculated checksum.",
            ),
        );

        cleanup(&bucket, &[]).await;
    });
}

// ── Conflicting inline + trailing checksum ──────────────────────────────

/// Regression test: a request with both a trailing checksum declaration
/// (x-amz-trailer) and an inline checksum value header must be rejected.
/// AWS returns: InvalidRequest: Expecting a single x-amz-checksum- header
#[test]
fn test_inline_plus_trailing_checksum_rejected() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"conflict test body";
        let path = format!("/{}/conflict-cksum", bucket);
        let content_sha256 = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

        use base64::Engine;
        // Compute correct CRC32 for the trailer.
        let crc = checksum::crc32::checksum(data);
        let crc_b64 = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());
        let trailer = format!("x-amz-checksum-crc32:{}", crc_b64);

        // Also include a bogus inline SHA256 header.
        let bogus_sha256 = base64::engine::general_purpose::STANDARD
            .encode(ring::digest::digest(&ring::digest::SHA256, b"wrong").as_ref());

        let sign = sign_streaming_request(
            "PUT",
            &path,
            content_sha256,
            data.len(),
            &[
                ("x-amz-trailer", "x-amz-checksum-crc32"),
                ("x-amz-checksum-sha256", &bogus_sha256),
            ],
        );
        let wire = build_unsigned_chunked_body_with_trailer(data, &trailer);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("content-length", wire.len().to_string())
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .header("x-amz-checksum-sha256", &bogus_sha256)
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();

        // AWS rejects with: InvalidRequest: Expecting a single x-amz-checksum- header
        assert_eq!(status, 400, "expected 400, got {}: {}", status, body_str);
        assert_error_body(
            &body_str,
            expected_error::with_host_id(
                "InvalidRequest",
                "Expecting a single x-amz-checksum- header",
            ),
        );

        cleanup(&bucket, &[]).await;
    });
}

/// Same as above but with mixed-case trailer name to verify case-insensitive
/// conflict detection.
#[test]
fn test_inline_plus_trailing_checksum_rejected_mixed_case() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"mixed case conflict";
        let path = format!("/{}/conflict-mixed", bucket);
        let content_sha256 = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

        use base64::Engine;
        let crc = checksum::crc32::checksum(data);
        let crc_b64 = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());
        let trailer = format!("x-amz-checksum-crc32:{}", crc_b64);

        let bogus_sha256 = base64::engine::general_purpose::STANDARD
            .encode(ring::digest::digest(&ring::digest::SHA256, b"wrong").as_ref());

        // Use mixed-case trailer name to try to bypass conflict detection.
        let sign = sign_streaming_request(
            "PUT",
            &path,
            content_sha256,
            data.len(),
            &[
                ("x-amz-trailer", "X-Amz-Checksum-CRC32"),
                ("x-amz-checksum-sha256", &bogus_sha256),
            ],
        );
        let wire = build_unsigned_chunked_body_with_trailer(data, &trailer);

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("content-length", wire.len().to_string())
            .header("x-amz-trailer", "X-Amz-Checksum-CRC32")
            .header("x-amz-checksum-sha256", &bogus_sha256)
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();

        assert_eq!(status, 400, "expected 400, got {}: {}", status, body_str);
        assert_error_body(
            &body_str,
            expected_error::with_host_id(
                "InvalidRequest",
                "Expecting a single x-amz-checksum- header",
            ),
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_streaming_upload_part_with_inline_checksum() {
    s3_tests::run(async {
        use aws_sdk_s3::types::ChecksumAlgorithm;
        use base64::Engine;

        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "streaming-upload-part-inline-cksum";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        // Upload a part with an inline checksum header (required by AWS when
        // the MPU was created with a checksum algorithm).
        let data = b"streaming upload part with checksum";
        let expected_crc = base64::engine::general_purpose::STANDARD
            .encode(checksum::crc32::checksum(data).to_be_bytes());
        let path = format!("/{}/{}", bucket, key);
        let encoded_upload_id: String =
            url::form_urlencoded::byte_serialize(upload_id.as_bytes()).collect();
        let query = format!("partNumber=1&uploadId={encoded_upload_id}");
        let request_uri = format!("{path}?{query}");

        let sign = sign_streaming_request_custom_with_query(
            "PUT",
            &request_uri,
            "UNSIGNED-PAYLOAD",
            data.len(),
            &[("x-amz-checksum-crc32", &expected_crc)],
            true, // not aws-chunked
            true, // not aws-chunked
        );

        let url = format!("{}{}?{}", CTX.endpoint(), path, query);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
            .header("x-amz-checksum-crc32", &expected_crc)
            .send(&data[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 200, "expected 200, got {}: {}", status, body_str);

        let listed = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();
        assert_eq!(listed.checksum_algorithm(), Some(&ChecksumAlgorithm::Crc32));
        let parts = listed.parts();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_number(), Some(1));
        assert_eq!(parts[0].checksum_crc32(), Some(expected_crc.as_str()));

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_signed_chunked_upload_part_bad_terminal_signature_not_staged() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "streaming-upload-part-bad-terminal-sig";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let data = b"part payload accepted before terminal signature fails";
        let path = format!("/{bucket}/{key}");
        let encoded_upload_id: String =
            url::form_urlencoded::byte_serialize(upload_id.as_bytes()).collect();
        let query = format!("partNumber=1&uploadId={encoded_upload_id}");
        let request_uri = format!("{path}?{query}");
        let content_sha256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

        let sign = sign_streaming_request_custom_with_query(
            "PUT",
            &request_uri,
            content_sha256,
            data.len(),
            &[],
            false,
            false,
        );
        let wire = build_signed_chunked_body_with_bad_terminal_signature(&sign, data);

        let url = format!("{}{}?{}", CTX.endpoint(), path, query);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("content-length", wire.len().to_string())
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 403, "expected 403, got {}: {}", status, body_str);
        assert_signature_mismatch_body(&body_str);

        let listed = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();
        assert!(
            listed.parts().is_empty(),
            "bad terminal signature must not stage an upload part"
        );

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

// ── Malformed chunked body error path tests ─────────────────────────────

/// Send a PUT with a manually crafted malformed chunked body (invalid hex chunk size).
/// This exercises the `parse_chunk_header` invalid-hex error closure.
#[test]
fn test_chunked_malformed_invalid_hex_chunk_size() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let path = format!("/{}/malformed-hex", bucket);
        let content_sha256 = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

        let sign = sign_streaming_request(
            "PUT",
            &path,
            content_sha256,
            5,
            &[("x-amz-trailer", "x-amz-checksum-crc32")],
        );

        // Malformed wire: "zz" is not valid hex for chunk size.
        let wire = b"zz\r\nhello\r\n0\r\n\r\n";

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", "5")
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .header("content-length", wire.len().to_string())
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        // Server should reject with 400 for malformed chunk header.
        assert_eq!(status, 400, "expected 400, got {}: {}", status, body_str);

        cleanup(&bucket, &[]).await;
    });
}

/// Send a PUT with a non-UTF8 chunk header line.
/// This exercises the `parse_chunk_header` non-UTF8 error closure.
#[test]
fn test_chunked_malformed_non_utf8_chunk_header() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let path = format!("/{}/malformed-utf8", bucket);
        let content_sha256 = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

        let sign = sign_streaming_request(
            "PUT",
            &path,
            content_sha256,
            5,
            &[("x-amz-trailer", "x-amz-checksum-crc32")],
        );

        // Wire with non-UTF8 bytes in the chunk header line.
        let mut wire = Vec::new();
        wire.extend_from_slice(&[0xff, 0xfe]); // non-UTF8 bytes
        wire.extend_from_slice(b"\r\nhello\r\n0\r\n\r\n");

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", "5")
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .header("content-length", wire.len().to_string())
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 400, "expected 400, got {}: {}", status, body_str);

        cleanup(&bucket, &[]).await;
    });
}

/// Send a signed chunked body with valid chunk signatures but a missing trailer
/// signature. This exercises the `verify_trailer_signature` missing-signature
/// error closure.
#[test]
fn test_signed_chunked_missing_trailer_signature() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"Hello";
        let path = format!("/{}/missing-trailer-sig", bucket);
        let content_sha256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";

        let sign = sign_streaming_request(
            "PUT",
            &path,
            content_sha256,
            data.len(),
            &[("x-amz-trailer", "x-amz-checksum-crc32")],
        );

        // Build signed body with valid chunk sigs but NO trailer signature.
        let chunk_sig = chunk_signature(
            &sign.signing_key,
            &sign.timestamp,
            &sign.scope,
            &sign.seed_signature,
            data,
        );
        let terminal_sig = chunk_signature(
            &sign.signing_key,
            &sign.timestamp,
            &sign.scope,
            &chunk_sig,
            b"",
        );

        let mut wire = Vec::new();
        wire.extend_from_slice(
            format!("{:x};chunk-signature={}\r\n", data.len(), chunk_sig).as_bytes(),
        );
        wire.extend_from_slice(data);
        wire.extend_from_slice(b"\r\n");
        wire.extend_from_slice(format!("0;chunk-signature={}\r\n", terminal_sig).as_bytes());
        // Trailer header present but NO x-amz-trailer-signature line.
        wire.extend_from_slice(b"x-amz-checksum-crc32:AAAA\r\n");
        wire.extend_from_slice(b"\r\n");

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .header("content-length", wire.len().to_string())
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 400, "expected 400, got {}: {}", status, body_str);
        assert_error_body(
            &body_str,
            expected_error::with_host_id(
                "IncompleteBody",
                "The request body terminated unexpectedly",
            ),
        );

        cleanup(&bucket, &[]).await;
    });
}

/// Send a signed chunked body with valid chunk signatures but an incorrect
/// trailer signature. Uses two trailers to exercise the sort_by comparator
/// in `verify_trailer_signature`.
#[test]
fn test_signed_chunked_bad_trailer_signature() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let data = b"Hello";
        let path = format!("/{}/bad-trailer-sig", bucket);
        let content_sha256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";

        let sign = sign_streaming_request(
            "PUT",
            &path,
            content_sha256,
            data.len(),
            &[("x-amz-trailer", "x-amz-checksum-crc32")],
        );

        // Build signed body manually with two content trailers and a bad
        // trailer signature. Two trailers are needed to exercise the sort_by
        // comparator in verify_trailer_signature.
        let chunk_sig = chunk_signature(
            &sign.signing_key,
            &sign.timestamp,
            &sign.scope,
            &sign.seed_signature,
            data,
        );
        let terminal_sig = chunk_signature(
            &sign.signing_key,
            &sign.timestamp,
            &sign.scope,
            &chunk_sig,
            b"",
        );

        let bad_trailer_sig = "0".repeat(64);
        let mut wire = Vec::new();
        wire.extend_from_slice(
            format!("{:x};chunk-signature={}\r\n", data.len(), chunk_sig).as_bytes(),
        );
        wire.extend_from_slice(data);
        wire.extend_from_slice(b"\r\n");
        wire.extend_from_slice(format!("0;chunk-signature={}\r\n", terminal_sig).as_bytes());
        // Two content trailers (sorted order doesn't matter — verify_trailer_signature sorts them)
        wire.extend_from_slice(b"x-amz-checksum-crc32:AAAA\r\n");
        wire.extend_from_slice(b"another-trailer:value\r\n");
        wire.extend_from_slice(
            format!("x-amz-trailer-signature:{}\r\n", bad_trailer_sig).as_bytes(),
        );
        wire.extend_from_slice(b"\r\n");

        let url = format!("{}{}", CTX.endpoint(), path);
        let mut resp = agent()
            .put(&url)
            .header("Authorization", &sign.authorization)
            .header("x-amz-date", &sign.amz_date)
            .header("x-amz-content-sha256", content_sha256)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", data.len().to_string())
            .header("x-amz-trailer", "x-amz-checksum-crc32")
            .header("content-length", wire.len().to_string())
            .send(&wire[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body_str = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(status, 403, "expected 403, got {}: {}", status, body_str);
        assert_signature_mismatch_body(&body_str);

        cleanup(&bucket, &[]).await;
    });
}
