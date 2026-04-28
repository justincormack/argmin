use std::time::Duration;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    CompletedMultipartUpload, CompletedPart, ObjectCannedAcl, ObjectOwnership, Permission,
};
use s3_tests::{
    assert_s3_err_code, content_md5_header, create_acl_enabled_bucket, err_status, CTX,
};

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

fn anonymous_get(url: &str) -> (u16, String) {
    let mut resp = agent().get(url).call().expect("transport error");
    let status = resp.status().as_u16();
    let body = resp.body_mut().read_to_string().unwrap_or_default();
    (status, body)
}

fn anonymous_put(url: &str, body: &[u8], headers: &[(String, String)]) -> (u16, String) {
    let request = headers
        .iter()
        .fold(agent().put(url), |request, (name, value)| {
            request.header(name, value)
        });
    let mut resp = request.send(body).expect("transport error");
    let status = resp.status().as_u16();
    let body = resp.body_mut().read_to_string().unwrap_or_default();
    (status, body)
}

fn has_grant(grants: &[aws_sdk_s3::types::Grant], permission: Permission, uri: &str) -> bool {
    grants.iter().any(|grant| {
        grant.permission() == Some(&permission)
            && grant.grantee().and_then(|grantee| grantee.uri()) == Some(uri)
    })
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

#[test]
fn test_public_read_object_does_not_make_get_object_tagging_public() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_public_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .acl(ObjectCannedAcl::PublicRead)
            .tagging("env=public")
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        let url = format!("{}/{}/obj?tagging", CTX.endpoint(), bucket);
        let mut resp = agent().get(&url).call().expect("transport error");
        assert_eq!(
            resp.status().as_u16(),
            403,
            "expected anonymous GetObjectTagging to be denied for public-read object, got {}",
            resp.status().as_u16()
        );

        let body = resp.body_mut().read_to_string().unwrap();
        assert!(
            body.contains("AccessDenied"),
            "expected AccessDenied response body, got {body}"
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_anonymous_public_write_object_get_object_tagging_behavior() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_public_write_bucket().await;
        let key = "anonymous-owner-tagging";
        let object_url = format!("{}/{bucket}/{key}", CTX.endpoint());
        let tagging_url = format!("{object_url}?tagging");

        let put = anonymous_put(&object_url, b"hello", &[]);
        assert_eq!(put.0, 200, "unexpected anonymous PUT body: {}", put.1);

        let owner_view = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert!(owner_view.tag_set().is_empty());

        let anonymous_get = anonymous_get(&tagging_url);

        cleanup(&bucket, &[key]).await;

        assert_eq!(
            anonymous_get.0, 403,
            "unexpected anonymous GetObjectTagging body={}",
            anonymous_get.1
        );
        assert!(
            anonymous_get.1.contains("AccessDenied"),
            "unexpected anonymous GetObjectTagging body={}",
            anonymous_get.1
        );
    });
}

#[test]
fn test_anonymous_public_write_object_put_object_tagging_behavior() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_public_write_bucket().await;
        let key = "anonymous-owner-put-tagging";
        let object_url = format!("{}/{bucket}/{key}", CTX.endpoint());
        let tagging_url = format!("{object_url}?tagging");

        let put = anonymous_put(&object_url, b"hello", &[]);
        assert_eq!(put.0, 200, "unexpected anonymous PUT body: {}", put.1);

        let tagging_body =
            br#"<Tagging><TagSet><Tag><Key>env</Key><Value>anon</Value></Tag></TagSet></Tagging>"#;
        let tagging_put = anonymous_put(
            &tagging_url,
            tagging_body,
            &[content_md5_header(tagging_body)],
        );

        let owner_view = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;

        cleanup(&bucket, &[key]).await;

        assert_eq!(
            tagging_put.0, 403,
            "unexpected anonymous PutObjectTagging body={}",
            tagging_put.1
        );
        assert!(
            tagging_put.1.contains("AccessDenied"),
            "unexpected anonymous PutObjectTagging body={}",
            tagging_put.1
        );
        assert!(
            owner_view.unwrap().tag_set().is_empty(),
            "anonymous PutObjectTagging should not write tags"
        );
    });
}

#[test]
fn test_multipart_upload_public_read_acl_allows_anonymous_get() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_acl_enabled_bucket(client, ObjectOwnership::ObjectWriter).await;
        let key = "multipart-public-read";
        let body = vec![b'x'; 1024];

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .acl(ObjectCannedAcl::PublicRead)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let part = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
            .unwrap();

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(part.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(
                acl.grants(),
                Permission::Read,
                "http://acs.amazonaws.com/groups/global/AllUsers",
            ),
            "expected READ grant for AllUsers, got {:?}",
            acl.grants()
        );

        let get_url = format!("{}/{bucket}/{key}", CTX.endpoint());
        let mut resp = agent().get(&get_url).call().expect("transport error");
        assert_eq!(
            resp.status().as_u16(),
            200,
            "expected anonymous GET for multipart public-read object"
        );
        let data = resp.body_mut().read_to_vec().unwrap();
        assert_eq!(&data[..], body.as_slice());

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_copy_object_public_read_acl_allows_cross_account_get() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_acl_enabled_bucket(client, ObjectOwnership::ObjectWriter).await;

        client
            .put_object()
            .bucket(&bucket)
            .key("foo123bar")
            .body(ByteStream::from_static(b"foo"))
            .send()
            .await
            .unwrap();

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
                "http://acs.amazonaws.com/groups/global/AllUsers",
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

#[test]
fn test_anon_create_multipart_upload_public_write_bucket_fail() {
    s3_tests::run(async {
        let bucket = setup_public_write_bucket().await;
        let url = format!("{}/{}/anon-multipart?uploads", CTX.endpoint(), bucket);
        let mut resp = agent()
            .post(&url)
            .send(b"" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(
            status, 403,
            "expected 403 for anonymous CreateMultipartUpload on public-read-write bucket, got {} body={}",
            status, body
        );
        assert!(
            body.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied in body: {body}"
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_signed_create_multipart_upload_public_write_bucket_rejects_existing_owner_key() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = setup_public_write_bucket().await;
        let key = "multipart-existing-owner-key";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"owner-body"))
            .send()
            .await
            .unwrap();

        let create = alt_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;
        assert_eq!(err_status(&create), 403);
        assert_s3_err_code(&create, "AccessDenied");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_complete_multipart_upload_allows_owner_key_created_after_public_write_initiation() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = setup_public_write_bucket().await;
        let key = "multipart-public-write-race";

        let create = alt_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"owner-body"))
            .send()
            .await
            .unwrap();

        let data = vec![b'x'; 1024];
        let upload_part = alt_client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(data.clone()))
            .send()
            .await
            .unwrap();

        let complete = alt_client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(upload_part.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();
        assert!(
            complete.e_tag().is_some(),
            "expected CompleteMultipartUpload to return an ETag"
        );

        cleanup(&bucket, &[key]).await;
    });
}
