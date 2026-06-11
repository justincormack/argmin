use aws_sdk_s3::primitives::ByteStream;
use s3_tests::{unique_bucket, CTX};

/// Build an agent that returns all HTTP responses (including 4xx/5xx) as Ok.
fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
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
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

// ── Anonymous GET object ────────────────────────────────────────────────
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
// ── Anonymous HEAD bucket ───────────────────────────────────────────────
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
// ── Anonymous ListObjects V1 ────────────────────────────────────────────
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
