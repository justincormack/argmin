use std::time::Duration;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ObjectCannedAcl;
use s3_tests::CTX;

/// Build an agent that returns all HTTP responses (including 4xx/5xx) as Ok.
fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
}

/// Retry an anonymous HTTP request until it returns the expected status.
///
/// Anonymous data-plane authorization can lag behind control-plane writes such
/// as PutBucketAcl and PutPublicAccessBlock on AWS. This helper retries the
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

/// Cleanup helper.
async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

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
