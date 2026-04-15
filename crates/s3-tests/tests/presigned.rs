use std::time::Duration;

use aws_sdk_s3::primitives::ByteStream;
use s3_tests::{
    create_public_bucket, object_url, presign_url_with_credentials, sse_c_header_values,
    test_sse_c_key, unique_bucket, PresignedRequest, SignedRequestCredentials, CTX,
};

const NO_HEADERS: [(&str, &str); 0] = [];

/// Create a bucket, returning its name.
async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
}

async fn setup_sse_c_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
        .await
        .unwrap();
    bucket
}

/// Build an agent that returns all HTTP responses (including 4xx/5xx) as Ok.
fn agent() -> ureq::Agent {
    s3_tests::test_agent()
}

fn endpoint_is_https() -> bool {
    CTX.endpoint().starts_with("https://")
}

fn require_https_endpoint() {
    assert!(
        endpoint_is_https(),
        "presigned SSE-C coverage requires an https:// endpoint; got {}",
        CTX.endpoint()
    );
}

macro_rules! with_presigned_headers {
    ($req:expr, $presigned:expr) => {{
        let mut req = $req;
        for (name, value) in $presigned.headers() {
            req = req.header(name, value);
        }
        req
    }};
}

fn sha256_hex(data: &[u8]) -> String {
    auth::canonical::sha256_hex(data)
}

fn primary_credentials() -> SignedRequestCredentials<'static> {
    SignedRequestCredentials {
        access_key: CTX.access_key(),
        secret_key: CTX.secret_key(),
        region: CTX.region(),
        tls_ca_pem: CTX.tls_ca_pem(),
    }
}

fn alt_credentials() -> SignedRequestCredentials<'static> {
    SignedRequestCredentials {
        access_key: CTX.alt_access_key(),
        secret_key: CTX.alt_secret_key(),
        region: CTX.region(),
        tls_ca_pem: CTX.tls_ca_pem(),
    }
}

fn presign_object_with_credentials<K, V, I>(
    credentials: SignedRequestCredentials<'_>,
    method: &str,
    bucket: &str,
    key: &str,
    query: Option<&str>,
    expires: Duration,
    extra_headers: I,
    payload_hash: Option<&str>,
) -> PresignedRequest
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    presign_url_with_credentials(
        method,
        &object_url(CTX.endpoint(), bucket, key, query),
        expires,
        extra_headers,
        payload_hash,
        credentials,
    )
}

fn presign_object<K, V, I>(
    method: &str,
    bucket: &str,
    key: &str,
    query: Option<&str>,
    expires: Duration,
    extra_headers: I,
    payload_hash: Option<&str>,
) -> PresignedRequest
where
    K: AsRef<str>,
    V: AsRef<str>,
    I: IntoIterator<Item = (K, V)>,
{
    presign_object_with_credentials(
        primary_credentials(),
        method,
        bucket,
        key,
        query,
        expires,
        extra_headers,
        payload_hash,
    )
}

/// Cleanup helper.
async fn cleanup_with_client(client: &aws_sdk_s3::Client, bucket: &str, keys: &[&str]) {
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    cleanup_with_client(CTX.client(), bucket, keys).await;
}

// ── Presigned GET ───────────────────────────────────────────────────────

#[test]
fn test_presigned_get_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"presigned get content";

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "GET",
            &bucket,
            "obj",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let mut resp = agent()
            .get(presigned.uri())
            .call()
            .expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);
        let data = resp.body_mut().read_to_vec().unwrap();
        assert_eq!(&data[..], body);

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_presigned_get_object_nonexistent() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        let presigned = presign_object(
            "GET",
            &bucket,
            "no-such-key",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let mut resp = agent()
            .get(presigned.uri())
            .call()
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 404);

        cleanup(&bucket, &[]).await;
    });
}

// ── Presigned PUT ───────────────────────────────────────────────────────

#[test]
fn test_presigned_put_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"presigned put content";

        let presigned = presign_object(
            "PUT",
            &bucket,
            "uploaded",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let mut resp = agent()
            .put(presigned.uri())
            .send(&body[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 200);

        // Verify via normal GET
        let get_resp = client
            .get_object()
            .bucket(&bucket)
            .key("uploaded")
            .send()
            .await
            .unwrap();
        let data = get_resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], body);

        cleanup(&bucket, &["uploaded"]).await;
    });
}

async fn assert_presigned_put_object_with_acl(
    client: &aws_sdk_s3::Client,
    credentials: SignedRequestCredentials<'_>,
) {
    use aws_sdk_s3::types::{ObjectOwnership, OwnershipControls, OwnershipControlsRule};

    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    let ownership = OwnershipControls::builder()
        .rules(
            OwnershipControlsRule::builder()
                .object_ownership(ObjectOwnership::BucketOwnerPreferred)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    client
        .put_bucket_ownership_controls()
        .bucket(&bucket)
        .ownership_controls(ownership)
        .send()
        .await
        .unwrap();
    let body = b"hello world";

    let presigned = presign_object_with_credentials(
        credentials,
        "PUT",
        &bucket,
        "foo",
        None,
        Duration::from_secs(900),
        [("x-amz-acl", "private")],
        None,
    );

    let mut resp = with_presigned_headers!(agent().put(presigned.uri()), presigned)
        .send(&body[..])
        .expect("transport error");
    let status = resp.status().as_u16();
    let response_body = resp.body_mut().read_to_string().unwrap_or_default();
    assert_eq!(
        status, 200,
        "expected 200 for presigned PUT with x-amz-acl, got {} body={}",
        status, response_body
    );

    let get_presigned = presign_object_with_credentials(
        credentials,
        "GET",
        &bucket,
        "foo",
        None,
        Duration::from_secs(900),
        NO_HEADERS,
        None,
    );
    let mut get_resp = agent()
        .get(get_presigned.uri())
        .call()
        .expect("transport error");
    assert_eq!(get_resp.status().as_u16(), 200);
    let data = get_resp.body_mut().read_to_vec().unwrap();
    assert_eq!(&data[..], body);

    cleanup_with_client(client, &bucket, &["foo"]).await;
}

#[test]
fn test_presigned_sse_c_put_object() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_sse_c_bucket().await;
        let body = b"presigned sse-c put content";
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        let presigned = presign_object(
            "PUT",
            &bucket,
            "uploaded-sse-c",
            None,
            Duration::from_secs(900),
            [
                ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                (
                    "x-amz-server-side-encryption-customer-key",
                    key_b64.as_str(),
                ),
                (
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.as_str(),
                ),
            ],
            None,
        );

        let mut resp = with_presigned_headers!(agent().put(presigned.uri()), presigned)
            .send(&body[..])
            .expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);
        let alg = resp
            .headers()
            .get("x-amz-server-side-encryption-customer-algorithm")
            .map(|v| v.to_str().unwrap().to_string());
        let key_md5 = resp
            .headers()
            .get("x-amz-server-side-encryption-customer-key-md5")
            .map(|v| v.to_str().unwrap().to_string());
        let _ = resp.body_mut().read_to_string();
        assert_eq!(alg.as_deref(), Some("AES256"));
        assert_eq!(key_md5.as_deref(), Some(key_md5_b64.as_str()));

        let get_resp = client
            .get_object()
            .bucket(&bucket)
            .key("uploaded-sse-c")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64.clone())
            .sse_customer_key_md5(key_md5_b64.clone())
            .send()
            .await
            .unwrap();
        let data = get_resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], body);

        cleanup(&bucket, &["uploaded-sse-c"]).await;
    });
}

#[test]
fn test_presigned_put_object_signed_payload() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"presigned put with signed payload";
        let body_hash = sha256_hex(body);

        let presigned = presign_object(
            "PUT",
            &bucket,
            "signed-body",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            Some(&body_hash),
        );

        let mut resp = with_presigned_headers!(agent().put(presigned.uri()), presigned)
            .send(&body[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(
            status, 200,
            "expected 200 for signed-payload presigned PUT, got {}",
            status
        );

        // Verify via normal GET
        let get_resp = client
            .get_object()
            .bucket(&bucket)
            .key("signed-body")
            .send()
            .await
            .unwrap();
        let data = get_resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], body);

        cleanup(&bucket, &["signed-body"]).await;
    });
}

#[test]
fn test_presigned_put_object_signed_payload_mismatch() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"the body that was signed";
        let body_hash = sha256_hex(body);

        // Sign the URL for this specific body
        let presigned = presign_object(
            "PUT",
            &bucket,
            "signed-body",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            Some(&body_hash),
        );

        // Send a different body — signature was for the original body
        let wrong_body = b"different body content";
        let mut resp = agent()
            .put(presigned.uri())
            .header("x-amz-content-sha256", &sha256_hex(wrong_body))
            .send(&wrong_body[..])
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(
            status, 403,
            "expected 403 for mismatched body hash, got {}",
            status
        );

        cleanup(&bucket, &[]).await;
    });
}

// ── Presigned DELETE ────────────────────────────────────────────────────

#[test]
fn test_presigned_delete_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("todelete")
            .body(ByteStream::from_static(b"bye"))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "DELETE",
            &bucket,
            "todelete",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let mut resp = agent()
            .delete(presigned.uri())
            .call()
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        // AWS DeleteObject returns 204 No Content
        assert_eq!(status, 204, "expected 204, got {}", status);

        // Verify deleted
        let result = client
            .get_object()
            .bucket(&bucket)
            .key("todelete")
            .send()
            .await;
        assert!(result.is_err());

        cleanup(&bucket, &[]).await;
    });
}

// ── Presigned HEAD ──────────────────────────────────────────────────────

#[test]
fn test_presigned_head_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"head test"))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "HEAD",
            &bucket,
            "obj",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let mut resp = agent()
            .head(presigned.uri())
            .call()
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 200);

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_presigned_sse_c_get_object() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_sse_c_bucket().await;
        let body = b"presigned get sse-c content";
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        client
            .put_object()
            .bucket(&bucket)
            .key("obj-sse-c")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64.clone())
            .sse_customer_key_md5(key_md5_b64.clone())
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "GET",
            &bucket,
            "obj-sse-c",
            None,
            Duration::from_secs(900),
            [
                ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                (
                    "x-amz-server-side-encryption-customer-key",
                    key_b64.as_str(),
                ),
                (
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.as_str(),
                ),
            ],
            None,
        );

        let mut resp = with_presigned_headers!(agent().get(presigned.uri()), presigned)
            .call()
            .expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);
        let alg = resp
            .headers()
            .get("x-amz-server-side-encryption-customer-algorithm")
            .map(|v| v.to_str().unwrap().to_string());
        let key_md5 = resp
            .headers()
            .get("x-amz-server-side-encryption-customer-key-md5")
            .map(|v| v.to_str().unwrap().to_string());
        let data = resp.body_mut().read_to_vec().unwrap();
        assert_eq!(alg.as_deref(), Some("AES256"));
        assert_eq!(key_md5.as_deref(), Some(key_md5_b64.as_str()));
        assert_eq!(&data[..], body);

        cleanup(&bucket, &["obj-sse-c"]).await;
    });
}

#[test]
fn test_presigned_sse_c_get_requires_signed_headers() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_sse_c_bucket().await;
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        client
            .put_object()
            .bucket(&bucket)
            .key("obj-sse-c-missing-headers")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64.clone())
            .sse_customer_key_md5(key_md5_b64.clone())
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "GET",
            &bucket,
            "obj-sse-c-missing-headers",
            None,
            Duration::from_secs(900),
            [
                ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                (
                    "x-amz-server-side-encryption-customer-key",
                    key_b64.as_str(),
                ),
                (
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.as_str(),
                ),
            ],
            None,
        );

        let mut resp = agent()
            .get(presigned.uri())
            .call()
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 403, "expected 403, got {}", status);

        cleanup(&bucket, &["obj-sse-c-missing-headers"]).await;
    });
}

#[test]
fn test_presigned_sse_c_head_object() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_sse_c_bucket().await;
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        client
            .put_object()
            .bucket(&bucket)
            .key("obj-head-sse-c")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64.clone())
            .sse_customer_key_md5(key_md5_b64.clone())
            .body(ByteStream::from_static(b"head test"))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "HEAD",
            &bucket,
            "obj-head-sse-c",
            None,
            Duration::from_secs(900),
            [
                ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                (
                    "x-amz-server-side-encryption-customer-key",
                    key_b64.as_str(),
                ),
                (
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.as_str(),
                ),
            ],
            None,
        );

        let mut resp = with_presigned_headers!(agent().head(presigned.uri()), presigned)
            .call()
            .expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);
        let alg = resp
            .headers()
            .get("x-amz-server-side-encryption-customer-algorithm")
            .map(|v| v.to_str().unwrap().to_string());
        let key_md5 = resp
            .headers()
            .get("x-amz-server-side-encryption-customer-key-md5")
            .map(|v| v.to_str().unwrap().to_string());
        let _ = resp.body_mut().read_to_string();
        assert_eq!(alg.as_deref(), Some("AES256"));
        assert_eq!(key_md5.as_deref(), Some(key_md5_b64.as_str()));

        cleanup(&bucket, &["obj-head-sse-c"]).await;
    });
}

// ── Expired URL ─────────────────────────────────────────────────────────

#[test]
fn test_presigned_get_expired() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // Generate a URL that expires in 1 second
        let presigned = presign_object(
            "GET",
            &bucket,
            "obj",
            None,
            Duration::from_secs(1),
            NO_HEADERS,
            None,
        );

        // Wait for it to expire
        std::thread::sleep(Duration::from_secs(2));

        let mut resp = agent()
            .get(presigned.uri())
            .call()
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 403, "expected 403 for expired URL, got {}", status);

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Tampered signature ──────────────────────────────────────────────────

#[test]
fn test_presigned_get_bad_signature() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "GET",
            &bucket,
            "obj",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        // Tamper with the signature
        let url = presigned.uri().to_string();
        let tampered = url.replace("X-Amz-Signature=", "X-Amz-Signature=0000000000000000");

        let mut resp = agent().get(&tampered).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(
            status, 403,
            "expected 403 for bad signature, got {}",
            status
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Tampered query param (key changed) ──────────────────────────────────

#[test]
fn test_presigned_get_tampered_key() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("original")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "GET",
            &bucket,
            "original",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        // Change the key in the URL path from "original" to "different"
        let url = presigned.uri().to_string();
        let tampered = url.replace("/original?", "/different?");

        let mut resp = agent().get(&tampered).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        // Signature covers the path; tampered key → SignatureDoesNotMatch
        assert_eq!(status, 403, "expected 403, got {}", status);

        cleanup(&bucket, &["original"]).await;
    });
}

// ── Wrong HTTP method ───────────────────────────────────────────────────

#[test]
fn test_presigned_wrong_method() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // Generate a presigned GET URL
        let presigned = presign_object(
            "GET",
            &bucket,
            "obj",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        // Use it with PUT instead of GET — signature was computed for GET
        let mut resp = agent()
            .put(presigned.uri())
            .send(b"overwrite attempt" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(status, 403, "expected 403 for wrong method, got {}", status);

        // Verify original content unchanged
        let get_resp = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        let data = get_resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"data");

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Missing signature param ─────────────────────────────────────────────

#[test]
fn test_presigned_missing_signature() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "GET",
            &bucket,
            "obj",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        // Remove the X-Amz-Signature parameter
        let url = presigned.uri().to_string();
        let stripped: String = url
            .split('&')
            .filter(|p| !p.contains("X-Amz-Signature"))
            .collect::<Vec<_>>()
            .join("&");

        let mut resp = agent().get(&stripped).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert!(
            status >= 400,
            "expected error for missing signature, got {}",
            status
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Presigned PUT then GET verifies round-trip ──────────────────────────

#[test]
fn test_presigned_put_get_round_trip() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let body = b"round-trip content via presigned URLs";

        // Presigned PUT
        let put_presigned = presign_object(
            "PUT",
            &bucket,
            "roundtrip",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let mut put_resp = agent()
            .put(put_presigned.uri())
            .send(&body[..])
            .expect("transport error");
        assert_eq!(put_resp.status().as_u16(), 200);
        let _ = put_resp.body_mut().read_to_string();

        // Presigned GET
        let get_presigned = presign_object(
            "GET",
            &bucket,
            "roundtrip",
            None,
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let mut get_resp = agent()
            .get(get_presigned.uri())
            .call()
            .expect("transport error");
        assert_eq!(get_resp.status().as_u16(), 200);
        let data = get_resp.body_mut().read_to_vec().unwrap();
        assert_eq!(&data[..], body);

        cleanup(&bucket, &["roundtrip"]).await;
    });
}

// ── Presigned GET with response overrides ───────────────────────────────

#[test]
fn test_presigned_get_response_content_type() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let presigned = presign_object(
            "GET",
            &bucket,
            "obj",
            Some("response-content-type=application/pdf"),
            Duration::from_secs(900),
            NO_HEADERS,
            None,
        );

        let mut resp = agent()
            .get(presigned.uri())
            .call()
            .expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);
        let ct = resp
            .headers()
            .get("Content-Type")
            .map(|v| v.to_str().unwrap().to_string());
        let _ = resp.body_mut().read_to_string();
        assert_eq!(ct.as_deref(), Some("application/pdf"));

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── X-Amz-Expires range tests ──────────────────────────────────────────

#[test]
fn test_object_raw_get_x_amz_expires_not_expired() {
    s3_tests::run(async {
        assert_object_raw_get_x_amz_expires_not_expired(CTX.client(), primary_credentials()).await;
    });
}

#[test]
fn test_object_raw_get_x_amz_expires_out_max_range() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // Generate a valid presigned URL, then tamper X-Amz-Expires to exceed the
        // 604800-second (7-day) maximum.
        let presigned = presign_object(
            "GET",
            &bucket,
            "obj",
            None,
            Duration::from_secs(600),
            NO_HEADERS,
            None,
        );

        let tampered_url = presigned
            .uri()
            .replace("X-Amz-Expires=600", "X-Amz-Expires=604801");

        let mut resp = agent().get(&tampered_url).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        // Presigned URL validation: expires > 604800 is an auth parameter error → 400
        assert_eq!(
            status, 400,
            "expected 400 for out-of-range expires, got {}",
            status
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_object_raw_get_x_amz_expires_out_positive_range() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // Manually construct a URL with a negative X-Amz-Expires
        let presigned = presign_object(
            "GET",
            &bucket,
            "obj",
            None,
            Duration::from_secs(600),
            NO_HEADERS,
            None,
        );

        // Replace the X-Amz-Expires value with a negative number
        let tampered_url = presigned
            .uri()
            .replace("X-Amz-Expires=600", "X-Amz-Expires=-1");

        let mut resp = agent().get(&tampered_url).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        // Negative X-Amz-Expires is an auth parameter error → 400
        assert_eq!(
            status, 400,
            "expected 400 for negative expires, got {}",
            status
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_object_raw_get_x_amz_expires_out_range_zero() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // Construct a URL with X-Amz-Expires=0
        let presigned = presign_object(
            "GET",
            &bucket,
            "obj",
            None,
            Duration::from_secs(600),
            NO_HEADERS,
            None,
        );

        let tampered_url = presigned
            .uri()
            .replace("X-Amz-Expires=600", "X-Amz-Expires=0");

        let mut resp = agent().get(&tampered_url).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        // Zero expires means immediately expired → auth expiry
        assert_eq!(status, 403, "expected 403 for zero expires, got {}", status);

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_object_raw_put_authenticated_expired() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        // Generate a presigned PUT URL that's already expired
        let presigned = presign_object(
            "PUT",
            &bucket,
            "obj",
            None,
            Duration::from_secs(1),
            NO_HEADERS,
            None,
        );

        // Wait for expiry
        std::thread::sleep(Duration::from_secs(2));

        let mut resp = agent()
            .put(presigned.uri())
            .send(b"data" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        // Expired presigned URL → auth expiry
        assert_eq!(status, 403, "expected 403 for expired PUT, got {}", status);

        cleanup(&bucket, &[]).await;
    });
}

// ── ACL / Tenant presigned (not implemented) ───────────────────────────

#[test]
fn test_object_presigned_put_object_with_acl() {
    s3_tests::run(async {
        assert_presigned_put_object_with_acl(CTX.client(), primary_credentials()).await;
    });
}

#[test]
fn test_object_presigned_put_object_with_acl_tenant() {
    s3_tests::run(async {
        assert_presigned_put_object_with_acl(CTX.alt_client(), alt_credentials()).await;
    });
}

async fn assert_object_raw_get_x_amz_expires_not_expired(
    client: &aws_sdk_s3::Client,
    credentials: SignedRequestCredentials<'_>,
) {
    let bucket = create_public_bucket(client).await;
    client
        .put_object()
        .bucket(&bucket)
        .key("obj")
        .acl(aws_sdk_s3::types::ObjectCannedAcl::PublicRead)
        .body(ByteStream::from_static(b"data"))
        .send()
        .await
        .unwrap();

    let presigned = presign_object_with_credentials(
        credentials,
        "GET",
        &bucket,
        "obj",
        None,
        Duration::from_secs(600),
        NO_HEADERS,
        None,
    );

    let mut options_resp = agent()
        .options(presigned.uri())
        .call()
        .expect("transport error");
    let options_status = options_resp.status().as_u16();
    let _ = options_resp.body_mut().read_to_string();
    assert_eq!(options_status, 400);

    let mut get_resp = agent()
        .get(presigned.uri())
        .call()
        .expect("transport error");
    assert_eq!(get_resp.status().as_u16(), 200);
    let data = get_resp.body_mut().read_to_vec().unwrap();
    assert_eq!(&data[..], b"data");

    cleanup_with_client(client, &bucket, &["obj"]).await;
}

#[test]
fn test_object_raw_get_x_amz_expires_not_expired_tenant() {
    s3_tests::run(async {
        assert_object_raw_get_x_amz_expires_not_expired(CTX.alt_client(), alt_credentials()).await;
    });
}
