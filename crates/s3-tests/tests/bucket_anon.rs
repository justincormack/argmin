use std::time::Duration;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ObjectCannedAcl;
use s3_tests::{unique_bucket, CTX};

/// Build an agent that returns all HTTP responses (including 4xx/5xx) as Ok.
fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
}

/// Retry an anonymous HTTP request until it returns the expected status.
///
/// Anonymous data-plane authorization can lag behind control-plane writes such
/// as PutBucketAcl and PutPublicAccessBlock on AWS.  This helper retries the
/// request so that tests do not flake due to eventual consistency.
async fn anon_get_status_eventually(url: &str, expected_status: u16, description: &str) -> String {
    const MAX_ATTEMPTS: usize = 10;

    for attempt in 0..MAX_ATTEMPTS {
        let mut resp = agent().get(url).call().expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap_or_default();
        if status == expected_status {
            return body;
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        panic!(
            "{description} did not converge to HTTP {expected_status} for {url}, last status {status}, body: {body}"
        );
    }
    unreachable!()
}

async fn anon_head_status_eventually(url: &str, expected_status: u16, description: &str) {
    const MAX_ATTEMPTS: usize = 10;

    for attempt in 0..MAX_ATTEMPTS {
        let mut resp = agent().head(url).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        if status == expected_status {
            return;
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        panic!(
            "{description} did not converge to HTTP {expected_status} for {url}, last status {status}"
        );
    }
    unreachable!()
}

async fn anon_put_status_eventually(
    url: &str,
    body: &'static [u8],
    expected_status: u16,
    description: &str,
) {
    const MAX_ATTEMPTS: usize = 10;

    for attempt in 0..MAX_ATTEMPTS {
        let mut resp = agent().put(url).send(body).expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        if status == expected_status {
            return;
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        panic!(
            "{description} did not converge to HTTP {expected_status} for {url}, last status {status}"
        );
    }
    unreachable!()
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
    s3_tests::create_bucket(client, &bucket).await.unwrap();
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
        let body = anon_get_status_eventually(&url, 200, "anon GET on public bucket").await;
        assert_eq!(body.as_bytes(), b"public data");

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
        anon_head_status_eventually(&url, 200, "anon HEAD on public bucket").await;

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Anonymous HEAD bucket ───────────────────────────────────────────────

#[test]
fn test_anon_head_bucket_public() {
    s3_tests::run(async {
        let bucket = setup_public_bucket().await;

        let url = format!("{}/{}", CTX.endpoint(), bucket);
        anon_head_status_eventually(&url, 200, "anon HEAD on public bucket").await;

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

#[test]
fn test_anon_bucket_surfaces_distinguish_private_and_nonexistent_like_aws() {
    s3_tests::run(async {
        // AWS does not hide bucket existence from anonymous bucket-surface reads:
        // an existing private bucket returns 403 AccessDenied, while a missing
        // bucket returns 404 NoSuchBucket. Keep this paired contract explicit so
        // future "bucket enumeration" reports do not push us away from AWS.
        let private_bucket = setup_private_bucket().await;
        let missing_bucket = unique_bucket();

        let private_head = agent()
            .head(&format!("{}/{}", CTX.endpoint(), private_bucket))
            .call()
            .expect("anonymous private HEAD transport error");
        assert_eq!(private_head.status().as_u16(), 403);

        let missing_head = agent()
            .head(&format!("{}/{}", CTX.endpoint(), missing_bucket))
            .call()
            .expect("anonymous missing HEAD transport error");
        assert_eq!(missing_head.status().as_u16(), 404);

        let mut private_list = agent()
            .get(&format!(
                "{}/{}?list-type=2",
                CTX.endpoint(),
                private_bucket
            ))
            .call()
            .expect("anonymous private list transport error");
        assert_eq!(private_list.status().as_u16(), 403);
        let private_body = private_list.body_mut().read_to_string().unwrap();
        assert!(
            private_body.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied in private bucket body: {private_body}"
        );

        let mut missing_list = agent()
            .get(&format!(
                "{}/{}?list-type=2",
                CTX.endpoint(),
                missing_bucket
            ))
            .call()
            .expect("anonymous missing list transport error");
        assert_eq!(missing_list.status().as_u16(), 404);
        let missing_body = missing_list.body_mut().read_to_string().unwrap();
        assert!(
            missing_body.contains("<Code>NoSuchBucket</Code>"),
            "expected NoSuchBucket in missing bucket body: {missing_body}"
        );

        cleanup(&private_bucket, &[]).await;
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
        let body = anon_get_status_eventually(&url, 200, "anon list v1 on public bucket").await;
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

#[test]
fn test_anon_list_objects_v1_nonexistent_bucket_returns_404() {
    s3_tests::run(async {
        let bucket = unique_bucket();

        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut resp = agent().get(&url).call().expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap();
        assert_eq!(
            status, 404,
            "expected 404 for anon list on nonexistent bucket, got {}",
            status
        );
        assert!(
            body.contains("<Code>NoSuchBucket</Code>"),
            "expected NoSuchBucket in body: {}",
            body
        );
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
        let body = anon_get_status_eventually(&url, 200, "anon list v2 on public bucket").await;
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

#[test]
fn test_anon_list_objects_v2_nonexistent_bucket_returns_404() {
    s3_tests::run(async {
        let bucket = unique_bucket();

        let url = format!("{}/{}?list-type=2", CTX.endpoint(), bucket);
        let mut resp = agent().get(&url).call().expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap();
        assert_eq!(
            status, 404,
            "expected 404 for anon listv2 on nonexistent bucket, got {}",
            status
        );
        assert!(
            body.contains("<Code>NoSuchBucket</Code>"),
            "expected NoSuchBucket in body: {}",
            body
        );
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
        let bucket = setup_public_write_bucket().await;

        let url = format!("{}/{}/anon-upload", CTX.endpoint(), bucket);
        anon_put_status_eventually(
            &url,
            b"public write",
            200,
            "anon PUT on public-read-write bucket",
        )
        .await;

        CTX.client()
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
