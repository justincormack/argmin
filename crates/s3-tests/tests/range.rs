use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
use s3_tests::{cleanup_versioned_bucket, err_status, unique_bucket, CTX};

async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

async fn setup_versioned_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    client
        .put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    bucket
}

// ── Valid range variants ──────────────────────────────────────────────

#[test]
fn test_range_get_start_end() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"abcdefghijklmnopqrstuvwxyz";
        client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        // bytes=0-4 → "abcde"
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=0-4")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"abcde");

        cleanup(&bucket, &["r"]).await;
    });
}

#[test]
fn test_range_get_from_start() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"abcdefghijklmnopqrstuvwxyz";
        client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        // bytes=23- → "xyz" (last 3 bytes)
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=23-")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"xyz");

        cleanup(&bucket, &["r"]).await;
    });
}

#[test]
fn test_range_get_suffix() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"abcdefghijklmnopqrstuvwxyz";
        client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        // bytes=-3 → last 3 bytes = "xyz"
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=-3")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"xyz");

        cleanup(&bucket, &["r"]).await;
    });
}

#[test]
fn test_range_get_suffix_exceeds_size() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"abcdefghijklmnopqrstuvwxyz"; // 26 bytes
        client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        // bytes=-999 → suffix exceeds size, returns whole object
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=-999")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], body);

        cleanup(&bucket, &["r"]).await;
    });
}

#[test]
fn test_range_get_unsatisfiable() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"abcdefghijklmnopqrstuvwxyz"; // 26 bytes
        client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        // bytes=100-200 → start past end of object → 416
        let result = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=100-200")
            .send()
            .await;
        assert_eq!(err_status(&result), 416);

        cleanup(&bucket, &["r"]).await;
    });
}

#[test]
fn test_range_get_from_start_unsatisfiable() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"abcdefghijklmnopqrstuvwxyz"; // 26 bytes
        client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();

        // bytes=100- → start past end → 416
        let result = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=100-")
            .send()
            .await;
        assert_eq!(err_status(&result), 416);

        cleanup(&bucket, &["r"]).await;
    });
}

#[test]
fn test_range_get_if_match_returns_partial_content() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let body = b"abcdefghijklmnopqrstuvwxyz";
        let put = client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(body))
            .send()
            .await
            .unwrap();
        let etag = put.e_tag().unwrap().to_string();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=0-4")
            .if_match(&etag)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.accept_ranges(), Some("bytes"));
        assert_eq!(resp.content_range(), Some("bytes 0-4/26"));
        assert_eq!(resp.content_length(), Some(5));
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"abcde");

        cleanup(&bucket, &["r"]).await;
    });
}

#[test]
fn test_range_get_if_none_match_returns_not_modified() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let put = client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(b"abcdefghijklmnopqrstuvwxyz"))
            .send()
            .await
            .unwrap();
        let etag = put.e_tag().unwrap().to_string();

        let result = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=0-4")
            .if_none_match(&etag)
            .send()
            .await;
        assert_eq!(err_status(&result), 304);

        cleanup(&bucket, &["r"]).await;
    });
}

#[test]
fn test_range_get_if_match_mismatch_returns_precondition_failed() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(b"abcdefghijklmnopqrstuvwxyz"))
            .send()
            .await
            .unwrap();

        let result = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .range("bytes=0-4")
            .if_match("\"0000000000000000\"")
            .send()
            .await;
        assert_eq!(err_status(&result), 412);

        cleanup(&bucket, &["r"]).await;
    });
}

#[test]
fn test_range_get_specific_version_returns_expected_slice_and_version_id() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;

        let put_v1 = client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(b"abcdefghijklmnopqrstuvwxyz"))
            .send()
            .await
            .unwrap();
        let v1 = put_v1.version_id().unwrap().to_string();

        client
            .put_object()
            .bucket(&bucket)
            .key("r")
            .body(ByteStream::from_static(
                b"0123456789abcdefghijklmnopqrstuvwxyz",
            ))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("r")
            .version_id(&v1)
            .range("bytes=2-5")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.version_id(), Some(v1.as_str()));
        assert_eq!(resp.content_range(), Some("bytes 2-5/26"));
        assert_eq!(resp.content_length(), Some(4));
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"cdef");

        cleanup_versioned_bucket(client, &bucket).await;
    });
}
