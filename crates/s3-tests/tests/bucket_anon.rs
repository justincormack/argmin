use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ObjectCannedAcl;
use s3_tests::{unique_bucket, CTX};

/// Build an agent that returns all HTTP responses (including 4xx/5xx) as Ok.
fn agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .new_agent()
}

fn is_external() -> bool {
    std::env::var("S3_TEST_ENDPOINT").is_ok()
}

/// Create a public-read bucket, returning its name.
async fn setup_public_bucket() -> String {
    s3_tests::create_public_bucket(CTX.client()).await
}

/// Create a public-read-write bucket, returning its name.
async fn setup_public_write_bucket() -> String {
    s3_tests::create_public_write_bucket(CTX.client()).await
}

/// Create a private bucket (default), returning its name.
async fn setup_private_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    client.create_bucket().bucket(&bucket).send().await.unwrap();
    bucket
}

/// Cleanup helper.
async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

// ── Anonymous GET object ────────────────────────────────────────────────

#[test]
fn test_anon_get_object_public_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_public_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .acl(ObjectCannedAcl::PublicRead)
            .body(ByteStream::from_static(b"public data"))
            .send()
            .await
            .unwrap();

        let url = format!("{}/{}/obj", CTX.endpoint(), bucket);
        let mut resp = agent().get(&url).call().expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);
        let data = resp.body_mut().read_to_vec().unwrap();
        assert_eq!(&data[..], b"public data");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_anon_get_object_private_bucket_fail() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_private_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"private data"))
            .send()
            .await
            .unwrap();

        let url = format!("{}/{}/obj", CTX.endpoint(), bucket);
        let mut resp = agent().get(&url).call().expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap();
        assert_eq!(
            status, 403,
            "expected 403 for anon GET on private bucket, got {}",
            status
        );
        assert!(
            body.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied in body: {}",
            body
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Anonymous HEAD object ───────────────────────────────────────────────

#[test]
fn test_anon_head_object_public_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_public_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .acl(ObjectCannedAcl::PublicRead)
            .body(ByteStream::from_static(b"head me"))
            .send()
            .await
            .unwrap();

        let url = format!("{}/{}/obj", CTX.endpoint(), bucket);
        let resp = agent().head(&url).call().expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Anonymous HEAD bucket ───────────────────────────────────────────────

#[test]
fn test_anon_head_bucket_public() {
    s3_tests::run(async {
        let bucket = setup_public_bucket().await;

        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let resp = agent().head(&url).call().expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_anon_head_bucket_private_fail() {
    s3_tests::run(async {
        let bucket = setup_private_bucket().await;

        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut resp = agent().head(&url).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(
            status, 403,
            "expected 403 for anon HEAD on private bucket, got {}",
            status
        );

        cleanup(&bucket, &[]).await;
    });
}

// ── Anonymous PUT object ────────────────────────────────────────────────

#[test]
fn test_anon_put_object_public_bucket_fail() {
    s3_tests::run(async {
        let bucket = setup_public_bucket().await;

        let url = format!("{}/{}/anon-upload", CTX.endpoint(), bucket);
        let mut resp = agent()
            .put(&url)
            .send(b"should fail" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap();
        assert_eq!(
            status, 403,
            "expected 403 for anon PUT on public-read bucket, got {}",
            status
        );
        assert!(
            body.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied in body: {}",
            body
        );

        cleanup(&bucket, &[]).await;
    });
}

// ── Anonymous DELETE object (should always fail) ────────────────────────

#[test]
fn test_anon_delete_object_public_bucket_fail() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_public_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let url = format!("{}/{}/obj", CTX.endpoint(), bucket);
        let mut resp = agent().delete(&url).call().expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap();
        assert_eq!(
            status, 403,
            "expected 403 for anon DELETE on public-read bucket, got {}",
            status
        );
        assert!(
            body.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied in body: {}",
            body
        );

        // Verify object still exists
        client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Anonymous ListObjects V1 ────────────────────────────────────────────

#[test]
fn test_anon_list_objects_v1_public_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_public_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj1")
            .body(ByteStream::from_static(b"a"))
            .send()
            .await
            .unwrap();

        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut resp = agent().get(&url).call().expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);
        let body = resp.body_mut().read_to_string().unwrap();
        assert!(
            body.contains("<Key>obj1</Key>"),
            "expected obj1 in listing: {}",
            body
        );

        cleanup(&bucket, &["obj1"]).await;
    });
}

#[test]
fn test_anon_list_objects_v1_private_bucket_fail() {
    s3_tests::run(async {
        let bucket = setup_private_bucket().await;

        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut resp = agent().get(&url).call().expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap();
        assert_eq!(
            status, 403,
            "expected 403 for anon list on private bucket, got {}",
            status
        );
        assert!(
            body.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied in body: {}",
            body
        );

        cleanup(&bucket, &[]).await;
    });
}

// ── Anonymous ListObjects V2 ────────────────────────────────────────────

#[test]
fn test_anon_list_objects_v2_public_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_public_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj1")
            .body(ByteStream::from_static(b"a"))
            .send()
            .await
            .unwrap();

        let url = format!("{}/{}?list-type=2", CTX.endpoint(), bucket);
        let mut resp = agent().get(&url).call().expect("transport error");
        assert_eq!(resp.status().as_u16(), 200);
        let body = resp.body_mut().read_to_string().unwrap();
        assert!(
            body.contains("<Key>obj1</Key>"),
            "expected obj1 in v2 listing: {}",
            body
        );

        cleanup(&bucket, &["obj1"]).await;
    });
}

#[test]
fn test_anon_list_objects_v2_private_bucket_fail() {
    s3_tests::run(async {
        let bucket = setup_private_bucket().await;

        let url = format!("{}/{}?list-type=2", CTX.endpoint(), bucket);
        let mut resp = agent().get(&url).call().expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap();
        assert_eq!(
            status, 403,
            "expected 403 for anon listv2 on private bucket, got {}",
            status
        );
        assert!(
            body.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied in body: {}",
            body
        );

        cleanup(&bucket, &[]).await;
    });
}

// ── Anonymous PUT object (private bucket) ────────────────────────────

#[test]
fn test_object_anon_put() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_private_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(ByteStream::from_static(b""))
            .send()
            .await
            .unwrap();

        let url = format!("{}/{}/foo", CTX.endpoint(), bucket);
        let mut resp = agent()
            .put(&url)
            .send(b"foo" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap();
        assert_eq!(
            status, 403,
            "expected 403 for anon PUT on private bucket, got {}",
            status
        );
        assert!(
            body.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied in body: {}",
            body
        );

        cleanup(&bucket, &["foo"]).await;
    });
}

// ── Anonymous PUT object with write access ───────────────────────────

#[test]
fn test_object_anon_put_write_access() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_public_write_bucket().await;

        let url = format!("{}/{}/anon-upload", CTX.endpoint(), bucket);
        let mut resp = agent()
            .put(&url)
            .send(b"public write" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(
            status, 200,
            "expected 200 for anon PUT on public-read-write bucket, got {} body={}",
            status, body
        );

        // AWS semantics are subtle here:
        // - bucket-level public-read-write allows the anonymous upload itself
        // - the uploaded object is not then readable by the bucket owner
        // Local argmin does not yet model per-object owner identity for
        // anonymous writes, so the bucket owner can still read it back there.
        if is_external() {
            let err = client
                .get_object()
                .bucket(&bucket)
                .key("anon-upload")
                .send()
                .await
                .unwrap_err();
            let raw = format!("{:?}", err);
            assert!(
                raw.contains("AccessDenied") || raw.contains("403"),
                "expected owner readback to be denied on external S3, got: {}",
                raw
            );
        } else {
            let out = client
                .get_object()
                .bucket(&bucket)
                .key("anon-upload")
                .send()
                .await
                .unwrap();
            let data = out.body.collect().await.unwrap().into_bytes();
            assert_eq!(&data[..], b"public write");
        }

        client
            .delete_object()
            .bucket(&bucket)
            .key("anon-upload")
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_anon_get_bucket_acl_public_write_bucket_fail() {
    s3_tests::run(async {
        let bucket = setup_public_write_bucket().await;

        let url = format!("{}/{}?acl", CTX.endpoint(), bucket);
        let mut resp = agent().get(&url).call().expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap();
        assert_eq!(
            status, 403,
            "expected 403 for anon GetBucketAcl on public-read-write bucket, got {}",
            status
        );
        assert!(
            body.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied in body: {}",
            body
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_anon_put_bucket_acl_public_write_bucket_fail() {
    s3_tests::run(async {
        let bucket = setup_public_write_bucket().await;

        let url = format!("{}/{}?acl", CTX.endpoint(), bucket);
        let mut resp = agent()
            .put(&url)
            .header("x-amz-acl", "private")
            .send(b"" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap();
        assert_eq!(
            status, 403,
            "expected 403 for anon PutBucketAcl on public-read-write bucket, got {}",
            status
        );
        assert!(
            body.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied in body: {}",
            body
        );

        cleanup(&bucket, &[]).await;
    });
}

// ── Anonymous ListBuckets ────────────────────────────────────────────
//
// Unauthenticated GET / behavior varies across implementations:
//   - Ceph/RGW: returns 200 with an empty bucket list (no owner → no buckets).
//   - AWS S3:   returns 307 redirect to https://aws.amazon.com/s3/ (not an API response).
//   - argmin:   returns 403 AccessDenied (ListBuckets requires authentication).
//
// We return 403 because ListBuckets is an authenticated-only operation.

// ── Anonymous access to non-existent buckets ─────────────────────────
//
// AWS returns 404 NoSuchBucket for anonymous requests to non-existent
// buckets. This matches AWS behavior — bucket name secrecy is not relied
// upon for access control.

#[test]
fn test_anon_get_nonexistent_bucket_returns_404() {
    s3_tests::run(async {
        let url = format!(
            "{}/nonexistent-{}-{}/obj",
            CTX.endpoint(),
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let mut resp = agent().get(&url).call().expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap();
        assert_eq!(
            status, 404,
            "expected 404 for anon GET on nonexistent bucket, got {}",
            status
        );
        assert!(
            body.contains("<Code>NoSuchBucket</Code>"),
            "expected NoSuchBucket in body: {}",
            body
        );
    });
}

#[test]
fn test_anon_head_nonexistent_bucket_returns_404() {
    s3_tests::run(async {
        let url = format!(
            "{}/nonexistent-{}-{}",
            CTX.endpoint(),
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let resp = agent().head(&url).call().expect("transport error");
        assert_eq!(
            resp.status().as_u16(),
            404,
            "expected 404 for anon HEAD on nonexistent bucket, got {}",
            resp.status().as_u16()
        );
    });
}

#[test]
fn test_anon_put_nonexistent_bucket_returns_404() {
    s3_tests::run(async {
        let url = format!(
            "{}/nonexistent-{}-{}/obj",
            CTX.endpoint(),
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let mut resp = agent()
            .put(&url)
            .send(b"data" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap();
        assert_eq!(
            status, 404,
            "expected 404 for anon PUT on nonexistent bucket, got {}",
            status
        );
        assert!(
            body.contains("<Code>NoSuchBucket</Code>"),
            "expected NoSuchBucket in body: {}",
            body
        );
    });
}

// ── Anonymous ListBuckets ────────────────────────────────────────────

#[test]
fn test_list_buckets_anonymous() {
    if std::env::var("S3_TEST_ENDPOINT").is_ok() {
        // AWS returns 307 redirect to marketing page for anonymous GET /
        return;
    }
    s3_tests::run(async {
        let url = format!("{}/", CTX.endpoint());
        let mut resp = agent().get(&url).call().expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap();
        assert_eq!(
            status, 403,
            "expected 403 for anon ListBuckets, got {}",
            status
        );
        assert!(
            body.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied in body: {}",
            body
        );
    });
}
