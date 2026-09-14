// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use s3_tests::{assert_s3_err_code, err_status, CTX};
use s3_types::ANONYMOUS_UPLOAD_CANONICAL_USER_ID;

fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
}

fn build_multipart(
    fields: &[(&str, &str)],
    file_data: &[u8],
    file_name: &str,
) -> (String, Vec<u8>) {
    let boundary = "----argmin-s3-post-boundary";
    let mut body = Vec::new();

    for (name, value) in fields {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
        );
        body.extend_from_slice(value.as_bytes());
        body.extend_from_slice(b"\r\n");
    }

    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"file\"; filename=\"{file_name}\"\r\n\r\n")
            .as_bytes(),
    );
    body.extend_from_slice(file_data);
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

    (format!("multipart/form-data; boundary={boundary}"), body)
}

fn post_object(
    bucket: &str,
    fields: &[(&str, &str)],
    file_data: &[u8],
    file_name: &str,
) -> (u16, String) {
    let url = format!("{}/{}", CTX.endpoint(), bucket);
    let (content_type, body) = build_multipart(fields, file_data, file_name);
    let mut resp = agent()
        .post(&url)
        .header("Content-Type", &content_type)
        .send(&body[..])
        .expect("HTTP transport error");

    let status = resp.status().as_u16();
    let body_str = resp.body_mut().read_to_string().unwrap_or_default();
    (status, body_str)
}

async fn owner_get_object_access_denied_eventually(bucket: &str, key: &str) {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let result = CTX
            .client()
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;
        if result.is_err() {
            assert_eq!(err_status(&result), 403);
            assert_s3_err_code(&result, "AccessDenied");
            return;
        }

        if attempt + 1 == MAX_ATTEMPTS {
            panic!(
                "bucket-owner GetObject did not converge to AccessDenied for {bucket}/{key}: {:?}",
                result
            );
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    unreachable!()
}

async fn owner_head_object_access_denied_eventually(bucket: &str, key: &str) {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let result = CTX
            .client()
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;
        if result.is_err() {
            // HEAD has no error body, so only the HTTP status is observable.
            assert_eq!(err_status(&result), 403);
            return;
        }

        if attempt + 1 == MAX_ATTEMPTS {
            panic!(
                "bucket-owner HeadObject did not converge to AccessDenied for {bucket}/{key}: {:?}",
                result
            );
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    unreachable!()
}

async fn assert_post_object_anonymous_public_write_bucket() {
    let client = CTX.client();
    let bucket = s3_tests::create_public_write_bucket(client).await;
    let key = "post-anon-owner";

    let fields = [("key", key), ("Content-Type", "text/plain")];
    let (status, body) = post_object(&bucket, &fields, b"data", "test.txt");
    assert_eq!(
        status, 204,
        "expected 204 for anonymous POST on public-read-write bucket, got {} body={}",
        status, body
    );

    let acl_url = format!("{}/{bucket}/{key}?acl", CTX.endpoint());
    let mut acl = s3_tests::test_agent()
        .get(&acl_url)
        .call()
        .expect("transport error");
    assert_eq!(acl.status().as_u16(), 200);
    let acl_body = acl.body_mut().read_to_string().unwrap_or_default();
    assert!(
        acl_body.contains(&format!(
            "<Owner><ID>{ANONYMOUS_UPLOAD_CANONICAL_USER_ID}</ID></Owner>"
        )),
        "unexpected body: {acl_body}"
    );
    assert!(
        acl_body.contains(&format!(
            "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>{ANONYMOUS_UPLOAD_CANONICAL_USER_ID}</ID></Grantee><Permission>FULL_CONTROL</Permission>"
        )),
        "unexpected body: {acl_body}"
    );

    owner_head_object_access_denied_eventually(&bucket, key).await;
    owner_get_object_access_denied_eventually(&bucket, key).await;

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
}

#[test]
fn test_post_object_anonymous_public_write_uses_special_anonymous_owner_id() {
    s3_tests::run(assert_post_object_anonymous_public_write_bucket());
}

async fn assert_post_object_anonymous_public_read_request() {
    let client = CTX.client();
    let bucket = s3_tests::create_public_write_bucket(client).await;
    let key = "post-anon";

    let fields = [
        ("key", key),
        ("acl", "public-read"),
        ("Content-Type", "text/plain"),
    ];
    let (status, body) = post_object(&bucket, &fields, b"data", "test.txt");
    assert_eq!(
        status, 204,
        "expected 204 for anonymous POST on public-read-write bucket, got {} body={}",
        status, body
    );

    let out = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let data = out.body.collect().await.unwrap().into_bytes();
    assert_eq!(&data[..], b"data");

    let acl_url = format!("{}/{bucket}/{key}?acl", CTX.endpoint());
    let mut acl = s3_tests::test_agent()
        .get(&acl_url)
        .call()
        .expect("transport error");
    assert_eq!(acl.status().as_u16(), 200);
    let acl_body = acl.body_mut().read_to_string().unwrap_or_default();
    assert!(
        acl_body.contains(&format!(
            "<Owner><ID>{ANONYMOUS_UPLOAD_CANONICAL_USER_ID}</ID></Owner>"
        )),
        "unexpected body: {acl_body}"
    );
    assert!(
        acl_body.contains(&format!(
            "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>{ANONYMOUS_UPLOAD_CANONICAL_USER_ID}</ID></Grantee><Permission>FULL_CONTROL</Permission>"
        )),
        "unexpected body: {acl_body}"
    );

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
}

#[test]
fn test_post_object_anonymous_public_read_request() {
    s3_tests::run(assert_post_object_anonymous_public_read_request());
}

#[test]
fn test_post_object_anonymous_bucket_owner_full_control_request() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = s3_tests::create_public_write_bucket(client).await;
        let key = "post-anon-bofc";
        let bucket_owner = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap()
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetBucketAcl")
            .to_string();

        let fields = [
            ("key", key),
            ("acl", "bucket-owner-full-control"),
            ("Content-Type", "text/plain"),
        ];
        let (status, body) = post_object(&bucket, &fields, b"data", "test.txt");
        assert_eq!(
            status, 204,
            "expected 204 for anonymous POST with bucket-owner-full-control, got {} body={}",
            status, body
        );

        let out = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = out.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"data");

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(
            acl.owner().and_then(|owner| owner.id()),
            Some(bucket_owner.as_str())
        );
        assert!(
            acl.grants().iter().any(|grant| {
                grant.permission() == Some(&aws_sdk_s3::types::Permission::FullControl)
                    && grant
                        .grantee()
                        .is_some_and(|grantee| grantee.id() == Some(bucket_owner.as_str()))
            }),
            "expected bucket owner FULL_CONTROL grant, got {:?}",
            acl.grants()
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}
