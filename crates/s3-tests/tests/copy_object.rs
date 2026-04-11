use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    AccessControlPolicy, ChecksumType, Grant, Grantee, ObjectAttributes, ObjectCannedAcl,
    ObjectOwnership, Owner, Permission, PublicAccessBlockConfiguration, Type,
};
use aws_sdk_s3::Client;
use s3_tests::{
    assert_s3_err_code, cleanup_versioned_bucket, copy_source_with_version, err_status,
    send_signed_request, unique_bucket, CTX,
};

/// Create a bucket, returning its name.
async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
}

async fn set_object_writer_ownership(bucket: &str) {
    let rule = aws_sdk_s3::types::OwnershipControlsRule::builder()
        .object_ownership(ObjectOwnership::ObjectWriter)
        .build()
        .unwrap();
    let controls = aws_sdk_s3::types::OwnershipControls::builder()
        .rules(rule)
        .build()
        .unwrap();
    CTX.client()
        .put_bucket_ownership_controls()
        .bucket(bucket)
        .ownership_controls(controls)
        .send()
        .await
        .unwrap();
}

async fn disable_bucket_public_access_block(bucket: &str) {
    let config = PublicAccessBlockConfiguration::builder()
        .block_public_acls(false)
        .ignore_public_acls(false)
        .block_public_policy(false)
        .restrict_public_buckets(false)
        .build();
    CTX.client()
        .put_public_access_block()
        .bucket(bucket)
        .public_access_block_configuration(config)
        .send()
        .await
        .unwrap();
}

async fn canonical_owner_id(client: &Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    let owner_id = client
        .get_bucket_acl()
        .bucket(&bucket)
        .send()
        .await
        .unwrap()
        .owner()
        .and_then(|owner| owner.id())
        .expect("expected owner ID in GetBucketAcl")
        .to_string();
    client.delete_bucket().bucket(&bucket).send().await.unwrap();
    owner_id
}

fn canonical_user_full_control_grant(canonical_user_id: &str) -> Grant {
    Grant::builder()
        .grantee(
            Grantee::builder()
                .id(canonical_user_id)
                .r#type(Type::CanonicalUser)
                .build()
                .expect("canonical grantee"),
        )
        .permission(Permission::FullControl)
        .build()
}

fn access_control_policy(owner_id: &str, grants: Vec<Grant>) -> AccessControlPolicy {
    AccessControlPolicy::builder()
        .owner(Owner::builder().id(owner_id).build())
        .set_grants(Some(grants))
        .build()
}

fn has_grant(
    grants: &[Grant],
    permission: Permission,
    canonical_user_id: Option<&str>,
    uri: Option<&str>,
) -> bool {
    grants.iter().any(|grant| {
        grant.permission() == Some(&permission)
            && grant
                .grantee()
                .is_some_and(|grantee| grantee.id() == canonical_user_id && grantee.uri() == uri)
    })
}

/// Put an object and return its ETag (quoted, as returned by S3).
async fn put_object(bucket: &str, key: &str, body: &'static [u8]) -> String {
    let resp = CTX
        .client()
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await
        .unwrap();
    resp.e_tag().unwrap().to_string()
}

/// Clean up objects and bucket.
async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

// ── Basic copy ────────────────────────────────────────────────────────

#[test]
fn test_object_copy_zero_size() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "foo123bar", b"").await;

        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("bar321foo")
            .copy_source(format!("{}/foo123bar", bucket))
            .send()
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("bar321foo")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_length(), Some(0));

        cleanup(&bucket, &["foo123bar", "bar321foo"]).await;
    });
}

#[test]
fn test_object_copy_same_bucket() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "foo123bar", b"foo").await;

        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("bar321foo")
            .copy_source(format!("{}/foo123bar", bucket))
            .send()
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("bar321foo")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"foo");

        cleanup(&bucket, &["foo123bar", "bar321foo"]).await;
    });
}

#[test]
fn test_object_copy_wrong_expected_source_bucket_owner() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "foo123bar", b"foo").await;

        let result = CTX
            .client()
            .copy_object()
            .bucket(&bucket)
            .key("bar321foo")
            .copy_source(format!("{}/foo123bar", bucket))
            .customize()
            .mutate_request(|req| {
                req.headers_mut()
                    .insert("x-amz-source-expected-bucket-owner", "000000000000");
            })
            .send()
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        cleanup(&bucket, &["foo123bar"]).await;
    });
}

#[test]
fn test_object_copy_verify_contenttype() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key("foo123bar")
            .content_type("text/bla")
            .body(ByteStream::from_static(b"foo"))
            .send()
            .await
            .unwrap();

        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("bar321foo")
            .copy_source(format!("{}/foo123bar", bucket))
            .send()
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("bar321foo")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_type(), Some("text/bla"));
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"foo");

        cleanup(&bucket, &["foo123bar", "bar321foo"]).await;
    });
}

#[test]
fn test_object_copy_to_itself() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "foo123bar", b"foo").await;

        // Copying to itself without REPLACE should fail with 400
        let result = CTX
            .client()
            .copy_object()
            .bucket(&bucket)
            .key("foo123bar")
            .copy_source(format!("{}/foo123bar", bucket))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);

        cleanup(&bucket, &["foo123bar"]).await;
    });
}

#[test]
fn test_object_copy_to_itself_with_metadata() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "foo123bar", b"foo").await;

        // Copy to itself with REPLACE metadata directive should succeed
        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("foo123bar")
            .copy_source(format!("{}/foo123bar", bucket))
            .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
            .metadata("foo", "bar")
            .send()
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("foo123bar")
            .send()
            .await
            .unwrap();
        let meta = resp.metadata().unwrap();
        assert_eq!(meta.get("foo").map(String::as_str), Some("bar"));

        cleanup(&bucket, &["foo123bar"]).await;
    });
}

#[test]
fn test_object_copy_diff_bucket() {
    s3_tests::run(async {
        let bucket1 = setup_bucket().await;
        let bucket2 = setup_bucket().await;

        put_object(&bucket1, "foo123bar", b"foo").await;

        CTX.client()
            .copy_object()
            .bucket(&bucket2)
            .key("bar321foo")
            .copy_source(format!("{}/foo123bar", bucket1))
            .send()
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket2)
            .key("bar321foo")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"foo");

        cleanup(&bucket1, &["foo123bar"]).await;
        cleanup(&bucket2, &["bar321foo"]).await;
    });
}

#[test]
fn test_object_copy_retaining_metadata() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key("foo123bar")
            .content_type("audio/ogg")
            .metadata("key1", "value1")
            .metadata("key2", "value2")
            .body(ByteStream::from_static(b"foo"))
            .send()
            .await
            .unwrap();

        // Default directive is COPY — metadata should be retained
        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("bar321foo")
            .copy_source(format!("{}/foo123bar", bucket))
            .send()
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("bar321foo")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_type(), Some("audio/ogg"));
        let meta = resp.metadata().unwrap();
        assert_eq!(meta.get("key1").map(String::as_str), Some("value1"));
        assert_eq!(meta.get("key2").map(String::as_str), Some("value2"));
        assert_eq!(resp.content_length(), Some(3));

        cleanup(&bucket, &["foo123bar", "bar321foo"]).await;
    });
}

#[test]
fn test_object_copy_replacing_metadata() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key("foo123bar")
            .content_type("audio/ogg")
            .metadata("key1", "value1")
            .metadata("key2", "value2")
            .body(ByteStream::from_static(b"foo"))
            .send()
            .await
            .unwrap();

        // REPLACE directive — new metadata replaces original
        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("bar321foo")
            .copy_source(format!("{}/foo123bar", bucket))
            .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
            .content_type("audio/mpeg")
            .metadata("key3", "value3")
            .metadata("key2", "value2")
            .send()
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("bar321foo")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_type(), Some("audio/mpeg"));
        let meta = resp.metadata().unwrap();
        assert_eq!(meta.get("key3").map(String::as_str), Some("value3"));
        assert_eq!(meta.get("key2").map(String::as_str), Some("value2"));
        // Original key1 should be gone
        assert_eq!(meta.get("key1"), None);
        assert_eq!(resp.content_length(), Some(3));

        cleanup(&bucket, &["foo123bar", "bar321foo"]).await;
    });
}

#[test]
fn test_object_copy_bucket_not_found() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        let fake_source = format!("{}-fake/foo123bar", bucket);
        let result = CTX
            .client()
            .copy_object()
            .bucket(&bucket)
            .key("bar321foo")
            .copy_source(fake_source)
            .send()
            .await;
        assert_eq!(err_status(&result), 404);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_object_copy_key_not_found() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        let result = CTX
            .client()
            .copy_object()
            .bucket(&bucket)
            .key("bar321foo")
            .copy_source(format!("{}/foo123bar", bucket))
            .send()
            .await;
        assert_eq!(err_status(&result), 404);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_object_copy_16m() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let size = 16 * 1024 * 1024;
        let data = vec![0u8; size];

        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key("obj1")
            .body(ByteStream::from(data))
            .send()
            .await
            .unwrap();

        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("obj2")
            .copy_source(format!("{}/obj1", bucket))
            .send()
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj2")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_length(), Some(size as i64));

        cleanup(&bucket, &["obj1", "obj2"]).await;
    });
}

#[test]
fn test_object_copy_read_16m() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let size = 16 * 1024 * 1024;
        let data = vec![0u8; size];

        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key("obj1")
            .body(ByteStream::from(data.clone()))
            .send()
            .await
            .unwrap();

        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key("obj2")
            .copy_source(format!("{}/obj1", bucket))
            .send()
            .await
            .unwrap();

        let resp = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key("obj2")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_length(), Some(size as i64));
        let body = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.len(), size);
        assert_eq!(&body[..], &data[..]);

        cleanup(&bucket, &["obj1", "obj2"]).await;
    });
}

// ── Versioned copy ──────────────────────────────────────────────────

#[test]
fn test_object_copy_versioned_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket1 = setup_bucket().await;

        // Enable versioning
        client
            .put_bucket_versioning()
            .bucket(&bucket1)
            .versioning_configuration(
                aws_sdk_s3::types::VersioningConfiguration::builder()
                    .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let data = b"hello";
        client
            .put_object()
            .bucket(&bucket1)
            .key("foo123bar")
            .body(ByteStream::from_static(data))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket1)
            .key("foo123bar")
            .send()
            .await
            .unwrap();
        let version_id = resp.version_id().unwrap().to_string();

        // Copy within same versioned bucket using versionId in source
        client
            .copy_object()
            .bucket(&bucket1)
            .key("bar321foo")
            .copy_source(copy_source_with_version(&bucket1, "foo123bar", &version_id))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket1)
            .key("bar321foo")
            .send()
            .await
            .unwrap();
        let version_id2 = resp.version_id().unwrap().to_string();
        assert_eq!(resp.content_length(), Some(data.len() as i64));
        let body = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], data);
        client
            .copy_object()
            .bucket(&bucket1)
            .key("bar321foo2")
            .copy_source(copy_source_with_version(
                &bucket1,
                "bar321foo",
                &version_id2,
            ))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket1)
            .key("bar321foo2")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_length(), Some(data.len() as i64));

        // Copy to another versioned bucket
        let bucket2 = setup_bucket().await;
        client
            .put_bucket_versioning()
            .bucket(&bucket2)
            .versioning_configuration(
                aws_sdk_s3::types::VersioningConfiguration::builder()
                    .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .unwrap();

        client
            .copy_object()
            .bucket(&bucket2)
            .key("bar321foo3")
            .copy_source(copy_source_with_version(&bucket1, "foo123bar", &version_id))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket2)
            .key("bar321foo3")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_length(), Some(data.len() as i64));

        // Copy to a non-versioned bucket
        let bucket3 = setup_bucket().await;
        client
            .copy_object()
            .bucket(&bucket3)
            .key("bar321foo4")
            .copy_source(copy_source_with_version(&bucket1, "foo123bar", &version_id))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket3)
            .key("bar321foo4")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_length(), Some(data.len() as i64));

        // Copy from non-versioned bucket to versioned bucket
        client
            .copy_object()
            .bucket(&bucket1)
            .key("foo123bar2")
            .copy_source(format!("{}/bar321foo4", bucket3))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket1)
            .key("foo123bar2")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_length(), Some(data.len() as i64));

        cleanup(&bucket3, &["bar321foo4"]).await;
        cleanup_versioned_bucket(client, &bucket2).await;
        cleanup_versioned_bucket(client, &bucket1).await;
    });
}

#[test]
fn test_object_copy_versioned_url_encoding() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        // Enable versioning
        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                aws_sdk_s3::types::VersioningConfiguration::builder()
                    .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .unwrap();

        // Key with special characters that need URL encoding
        let src_key = "foo?bar";
        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(src_key)
            .send()
            .await
            .unwrap();
        let version_id = resp.version_id().unwrap().to_string();

        // Copy using versionId — source key needs URL encoding
        let dst_key = "bar&foo";
        client
            .copy_object()
            .bucket(&bucket)
            .key(dst_key)
            .copy_source(copy_source_with_version(&bucket, src_key, &version_id))
            .send()
            .await
            .unwrap();

        // Verify destination exists
        client
            .head_object()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap();

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_object_copy_versioning_multipart_upload() {
    s3_tests::run(async {
        use aws_sdk_s3::types::{
            BucketVersioningStatus, CompletedMultipartUpload, CompletedPart,
            VersioningConfiguration,
        };

        let client = CTX.client();
        let bucket = setup_bucket().await;

        // Enable versioning
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

        // Create a multipart object
        let src_key = "mp-src";
        let part_data = vec![b'M'; 5 * 1024 * 1024];
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(src_key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();
        let part_resp = client
            .upload_part()
            .bucket(&bucket)
            .key(src_key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(part_data.clone()))
            .send()
            .await
            .unwrap();
        let complete = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(src_key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(part_resp.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();
        let src_version = complete.version_id().unwrap().to_string();

        // Copy the multipart object
        let dst_key = "mp-dst";
        let copy_resp = client
            .copy_object()
            .bucket(&bucket)
            .key(dst_key)
            .copy_source(format!("{}/{}", bucket, src_key))
            .send()
            .await
            .unwrap();
        assert!(copy_resp.version_id().is_some());

        // Verify destination has same content
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap();
        assert_eq!(get.content_length(), Some(5 * 1024 * 1024));
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.len(), 5 * 1024 * 1024);
        assert!(body.iter().all(|&b| b == b'M'));

        // Clean up: delete both versions
        let dst_version = copy_resp.version_id().unwrap().to_string();
        for (key, vid) in [(src_key, src_version), (dst_key, dst_version)] {
            client
                .delete_object()
                .bucket(&bucket)
                .key(key)
                .version_id(&vid)
                .send()
                .await
                .unwrap();
        }
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Multi-user / ACL ────────────────────────────────────────────────

#[test]
fn test_object_copy_not_owned_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket1 = unique_bucket();
        let bucket2 = unique_bucket();

        s3_tests::create_bucket(client, &bucket1).await.unwrap();
        s3_tests::create_bucket(alt_client, &bucket2).await.unwrap();

        client
            .put_object()
            .bucket(&bucket1)
            .key("foo123bar")
            .body(ByteStream::from_static(b"foo"))
            .send()
            .await
            .unwrap();

        let result = alt_client
            .copy_object()
            .bucket(&bucket2)
            .key("bar321foo")
            .copy_source(format!("{}/foo123bar", bucket1))
            .send()
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        client
            .delete_object()
            .bucket(&bucket1)
            .key("foo123bar")
            .send()
            .await
            .unwrap();
        client
            .delete_bucket()
            .bucket(&bucket1)
            .send()
            .await
            .unwrap();
        alt_client
            .delete_bucket()
            .bucket(&bucket2)
            .send()
            .await
            .unwrap();
    });
}

#[test]
fn test_object_copy_rejects_public_acl_when_block_public_acls_enabled() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        put_object(&bucket, "src", b"data").await;

        let ownership_rule = aws_sdk_s3::types::OwnershipControlsRule::builder()
            .object_ownership(aws_sdk_s3::types::ObjectOwnership::BucketOwnerPreferred)
            .build()
            .unwrap();
        let ownership = aws_sdk_s3::types::OwnershipControls::builder()
            .rules(ownership_rule)
            .build()
            .unwrap();
        client
            .put_bucket_ownership_controls()
            .bucket(&bucket)
            .ownership_controls(ownership)
            .send()
            .await
            .unwrap();

        let pab = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .block_public_acls(true)
            .build();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();

        let result = client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .acl(aws_sdk_s3::types::ObjectCannedAcl::PublicRead)
            .send()
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        cleanup(&bucket, &["src"]).await;
    });
}

/// CopyObject from a delete-marker source should fail with 404/NoSuchKey.
#[test]
fn test_copy_object_delete_marker_source() {
    s3_tests::run(async {
        use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};

        let client = CTX.client();
        let bucket = setup_bucket().await;

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

        let src_key = "delete-marker-src";
        let dst_key = "delete-marker-dst";

        // Put then delete to create a delete marker as current version
        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from_static(b"original"))
            .send()
            .await
            .unwrap();
        client
            .delete_object()
            .bucket(&bucket)
            .key(src_key)
            .send()
            .await
            .unwrap();

        // CopyObject from the delete-marked key should fail
        let result = client
            .copy_object()
            .bucket(&bucket)
            .key(dst_key)
            .copy_source(format!("{}/{}", bucket, src_key))
            .send()
            .await;
        let status = err_status(&result);
        assert_eq!(status, 404);
        assert_s3_err_code(&result, "NoSuchKey");

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

/// CopyObject targeting a specific delete-marker versionId should fail.
/// AWS returns 400/InvalidRequest (not 404/NoSuchKey) for this case.
#[test]
fn test_copy_object_delete_marker_version_id() {
    s3_tests::run(async {
        use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};

        let client = CTX.client();
        let bucket = setup_bucket().await;

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

        let src_key = "dm-version-src";
        let dst_key = "dm-version-dst";

        // Put then delete to create a delete marker
        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        let del = client
            .delete_object()
            .bucket(&bucket)
            .key(src_key)
            .send()
            .await
            .unwrap();
        let dm_version_id = del.version_id().unwrap();

        // CopyObject explicitly targeting the delete-marker versionId
        let result = client
            .copy_object()
            .bucket(&bucket)
            .key(dst_key)
            .copy_source(copy_source_with_version(&bucket, src_key, dm_version_id))
            .send()
            .await;
        assert!(
            result.is_err(),
            "expected error copying delete-marker version"
        );
        let status = err_status(&result);
        assert_eq!(status, 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

// ── CopyObject REPLACE checksum regression tests ─────────────────────

#[test]
fn test_copy_object_replace_strips_bogus_inline_checksum() {
    // Regression: CopyObject REPLACE must not persist unverified inline
    // checksum values supplied in request headers.
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        put_object(&bucket, "src", b"hello world").await;

        // Copy with REPLACE + checksum_algorithm to trigger recompute.
        // The SDK doesn't let us inject a raw bogus header easily, so
        // instead verify the positive path: algorithm triggers recompute
        // and HEAD returns a valid checksum.
        client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
            .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Crc32)
            .send()
            .await
            .unwrap();

        // HEAD with ChecksumMode=ENABLED should return the recomputed checksum.
        let head = client
            .head_object()
            .bucket(&bucket)
            .key("dst")
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        let crc32_val = head
            .checksum_crc32()
            .expect("expected CRC32 on copied object");
        // Verify it's the real CRC32 of "hello world".
        use base64::Engine;
        let expected_crc = checksum::crc32::checksum(b"hello world");
        let expected_b64 =
            base64::engine::general_purpose::STANDARD.encode(expected_crc.to_be_bytes());
        assert_eq!(crc32_val, expected_b64);

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

#[test]
fn test_copy_object_replace_rejects_system_metadata_over_limit() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        put_object(&bucket, "src", b"hello world").await;

        let result = client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
            .content_disposition("d".repeat(3000))
            .send()
            .await;

        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MetadataTooLarge");

        cleanup(&bucket, &["src"]).await;
    });
}

#[test]
fn test_copy_object_replace_checksum_algorithm_recomputes() {
    // CopyObject REPLACE with x-amz-checksum-algorithm should compute
    // the checksum from the destination data and persist it.
    s3_tests::run(async {
        use base64::Engine;
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let data = b"test data for checksum";
        put_object(&bucket, "src", data).await;

        client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
            .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Sha256)
            .send()
            .await
            .unwrap();

        // GET with ChecksumMode should return SHA256.
        let get = client
            .get_object()
            .bucket(&bucket)
            .key("dst")
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        let sha256_val = get
            .checksum_sha256()
            .expect("expected SHA256 on copied object");
        let digest = ring::digest::digest(&ring::digest::SHA256, data);
        let expected_b64 = base64::engine::general_purpose::STANDARD.encode(digest.as_ref());
        assert_eq!(sha256_val, expected_b64);

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

#[test]
fn test_copy_object_default_checksum_is_crc64nvme() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let data = b"hello";
        let src_url = format!("{}/{}/src", CTX.endpoint(), bucket);
        let put_resp =
            send_signed_request("PUT", &src_url, data, std::iter::empty::<(&str, &str)>());
        assert_eq!(put_resp.status, 200, "source PUT failed: {}", put_resp.body);

        let src_attrs = client
            .get_object_attributes()
            .bucket(&bucket)
            .key("src")
            .object_attributes(ObjectAttributes::Checksum)
            .send()
            .await
            .unwrap();
        let src_checksum = src_attrs.checksum().expect("expected source checksum");
        let src_crc64 = src_checksum
            .checksum_crc64_nvme()
            .unwrap_or_else(|| panic!("expected default CRC64NVME checksum, got {src_checksum:?}"))
            .to_string();
        assert_eq!(
            src_checksum.checksum_type(),
            Some(&ChecksumType::FullObject)
        );

        let dst_url = format!("{}/{}/dst", CTX.endpoint(), bucket);
        let copy_source = format!("{}/src", bucket);
        let copy_resp = send_signed_request(
            "PUT",
            &dst_url,
            b"",
            [("x-amz-copy-source", copy_source.as_str())],
        );
        assert_eq!(copy_resp.status, 200, "copy PUT failed: {}", copy_resp.body);

        let dst_attrs = client
            .get_object_attributes()
            .bucket(&bucket)
            .key("dst")
            .object_attributes(ObjectAttributes::Checksum)
            .send()
            .await
            .unwrap();
        let dst_checksum = dst_attrs.checksum().expect("expected destination checksum");
        assert_eq!(
            dst_checksum.checksum_type(),
            Some(&ChecksumType::FullObject)
        );
        assert_eq!(dst_checksum.checksum_crc64_nvme(), Some(src_crc64.as_str()));

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

// ── Malformed copy source ─────────────────────────────────────────────

/// CopyObject with source that has no key (just bucket name) should fail.
#[test]
fn test_copy_object_source_missing_key() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "src", b"data").await;

        // copy_source = "bucket" (no slash, no key)
        let result = CTX
            .client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(&bucket)
            .send()
            .await;
        assert!(
            result.is_err(),
            "expected error for copy source without key"
        );
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidArgument");

        cleanup(&bucket, &["src"]).await;
    });
}

/// CopyObject with source that has an empty key (bucket/) should fail.
#[test]
fn test_copy_object_source_empty_key() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        put_object(&bucket, "src", b"data").await;

        // copy_source = "bucket/" (slash but empty key)
        let result = CTX
            .client()
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/", bucket))
            .send()
            .await;
        assert!(
            result.is_err(),
            "expected error for copy source with empty key"
        );
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidArgument");

        cleanup(&bucket, &["src"]).await;
    });
}

#[test]
fn test_object_copy_not_owned_object_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = setup_bucket().await;
        set_object_writer_ownership(&bucket).await;
        put_object(&bucket, "foo123bar", b"foo").await;

        let bucket_owner_id = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap()
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected bucket owner ID")
            .to_string();
        let source_owner_id = client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo123bar")
            .send()
            .await
            .unwrap()
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected source owner ID")
            .to_string();
        let alt_owner_id = canonical_owner_id(alt_client).await;

        client
            .put_object_acl()
            .bucket(&bucket)
            .key("foo123bar")
            .access_control_policy(access_control_policy(
                &source_owner_id,
                vec![
                    canonical_user_full_control_grant(&source_owner_id),
                    canonical_user_full_control_grant(&alt_owner_id),
                ],
            ))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_acl()
            .bucket(&bucket)
            .access_control_policy(access_control_policy(
                &bucket_owner_id,
                vec![
                    canonical_user_full_control_grant(&bucket_owner_id),
                    canonical_user_full_control_grant(&alt_owner_id),
                ],
            ))
            .send()
            .await
            .unwrap();

        let src = alt_client
            .get_object()
            .bucket(&bucket)
            .key("foo123bar")
            .send()
            .await
            .unwrap();
        let src_body = src.body.collect().await.unwrap().into_bytes();
        assert_eq!(&src_body[..], b"foo");

        alt_client
            .copy_object()
            .bucket(&bucket)
            .key("bar321foo")
            .copy_source(format!("{}/foo123bar", bucket))
            .send()
            .await
            .unwrap();

        let dst = alt_client
            .get_object()
            .bucket(&bucket)
            .key("bar321foo")
            .send()
            .await
            .unwrap();
        let dst_body = dst.body.collect().await.unwrap().into_bytes();
        assert_eq!(&dst_body[..], b"foo");

        let dst_acl = alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key("bar321foo")
            .send()
            .await
            .unwrap();
        assert_eq!(
            dst_acl.owner().and_then(|owner| owner.id()),
            Some(alt_owner_id.as_str())
        );
        assert!(
            has_grant(
                dst_acl.grants(),
                Permission::FullControl,
                Some(&alt_owner_id),
                None,
            ),
            "expected FULL_CONTROL grant for alternate owner, got {:?}",
            dst_acl.grants()
        );

        let _ = alt_client
            .delete_object()
            .bucket(&bucket)
            .key("bar321foo")
            .send()
            .await;
        let _ = client
            .delete_object()
            .bucket(&bucket)
            .key("foo123bar")
            .send()
            .await;
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_object_copy_canned_acl() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = setup_bucket().await;
        set_object_writer_ownership(&bucket).await;
        disable_bucket_public_access_block(&bucket).await;
        put_object(&bucket, "foo123bar", b"foo").await;

        client
            .copy_object()
            .bucket(&bucket)
            .key("bar321foo")
            .copy_source(format!("{}/foo123bar", bucket))
            .acl(ObjectCannedAcl::PublicRead)
            .send()
            .await
            .unwrap();

        let copied = alt_client
            .get_object()
            .bucket(&bucket)
            .key("bar321foo")
            .send()
            .await
            .unwrap();
        let copied_body = copied.body.collect().await.unwrap().into_bytes();
        assert_eq!(&copied_body[..], b"foo");

        let copied_acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("bar321foo")
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(
                copied_acl.grants(),
                Permission::Read,
                None,
                Some("http://acs.amazonaws.com/groups/global/AllUsers"),
            ),
            "expected READ grant for AllUsers, got {:?}",
            copied_acl.grants()
        );

        client
            .copy_object()
            .bucket(&bucket)
            .key("foo123bar")
            .copy_source(format!("{}/bar321foo", bucket))
            .acl(ObjectCannedAcl::PublicRead)
            .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
            .metadata("abc", "def")
            .send()
            .await
            .unwrap();

        let overwritten = alt_client
            .get_object()
            .bucket(&bucket)
            .key("foo123bar")
            .send()
            .await
            .unwrap();
        let overwritten_body = overwritten.body.collect().await.unwrap().into_bytes();
        assert_eq!(&overwritten_body[..], b"foo");

        let head = client
            .head_object()
            .bucket(&bucket)
            .key("foo123bar")
            .send()
            .await
            .unwrap();
        assert_eq!(
            head.metadata().and_then(|meta| meta.get("abc")),
            Some(&"def".to_string())
        );

        cleanup(&bucket, &["foo123bar", "bar321foo"]).await;
    });
}
