// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    CompletedMultipartUpload, CompletedPart, Grant, ObjectCannedAcl, ObjectOwnership,
    OwnershipControls, OwnershipControlsRule, Permission,
};
use s3_tests::{
    assert_s3_err_code, content_md5_header, create_acl_enabled_bucket, err_status,
    is_retryable_operation_contention, send_signed_request, SendRetryingOperationAborted, CTX,
};
use s3_types::ANONYMOUS_UPLOAD_CANONICAL_USER_ID;
use std::future::Future;

const ALL_USERS_GROUP_URI: &str = "http://acs.amazonaws.com/groups/global/AllUsers";
const AUTHENTICATED_USERS_GROUP_URI: &str =
    "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";
const SETUP_OPERATION_ATTEMPTS: usize = 20;

async fn retrying_operation_aborted<T, E, F, Fut>(description: &str, mut op: F) -> T
where
    E: ProvideErrorMetadata + std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, aws_sdk_s3::error::SdkError<E>>>,
{
    for attempt in 0..SETUP_OPERATION_ATTEMPTS {
        match op().await {
            Ok(output) => return output,
            Err(err)
                if is_retryable_operation_contention(&err)
                    && attempt + 1 < SETUP_OPERATION_ATTEMPTS =>
            {
                tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
            }
            Err(err) => panic!("{description}: {err:?}"),
        }
    }
    unreachable!("{description} retry loop must return on final attempt");
}

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

async fn alt_get_object_eventually(
    bucket: &str,
    key: &str,
) -> aws_sdk_s3::operation::get_object::GetObjectOutput {
    const MAX_ATTEMPTS: usize = 10;

    for attempt in 0..MAX_ATTEMPTS {
        match CTX
            .alt_client()
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => return output,
            Err(_) if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(err) => panic!("alternate GetObject failed unexpectedly: {err:?}"),
        }
    }

    unreachable!()
}

async fn owner_get_object_eventually(
    bucket: &str,
    key: &str,
    description: &str,
) -> aws_sdk_s3::operation::get_object::GetObjectOutput {
    const MAX_ATTEMPTS: usize = 30;

    for attempt in 0..MAX_ATTEMPTS {
        match CTX
            .client()
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => return output,
            Err(_) if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(err) => panic!(
                "{description} did not converge to allowed GetObject for {bucket}/{key}: {err:?}"
            ),
        }
    }

    unreachable!()
}

async fn owner_get_object_access_denied_eventually(bucket: &str, key: &str) {
    const MAX_ATTEMPTS: usize = 30;

    for attempt in 0..MAX_ATTEMPTS {
        let result = CTX
            .client()
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;
        if result.is_err() && err_status(&result) == 403 && {
            assert_s3_err_code(&result, "AccessDenied");
            true
        } {
            return;
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        panic!(
            "bucket-owner GetObject did not converge to AccessDenied for {bucket}/{key}: {:?}",
            result
        );
    }

    unreachable!()
}

async fn owner_head_object_access_denied_eventually(bucket: &str, key: &str) {
    const MAX_ATTEMPTS: usize = 30;

    for attempt in 0..MAX_ATTEMPTS {
        let result = CTX
            .client()
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;
        if result.is_err() && err_status(&result) == 403 && {
            assert_s3_err_code(&result, "AccessDenied");
            true
        } {
            return;
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        panic!(
            "bucket-owner HeadObject did not converge to AccessDenied for {bucket}/{key}: {:?}",
            result
        );
    }

    unreachable!()
}

async fn bucket_owner_id(bucket: &str) -> String {
    CTX.client()
        .get_bucket_acl()
        .bucket(bucket)
        .send_retrying_operation_aborted("S3 operation during public ACL test")
        .await
        .unwrap()
        .owner()
        .and_then(|owner| owner.id())
        .expect("expected owner ID in GetBucketAcl")
        .to_string()
}

async fn object_owner_id(client: &aws_sdk_s3::Client, bucket: &str, key: &str) -> String {
    client
        .get_object_acl()
        .bucket(bucket)
        .key(key)
        .send_retrying_operation_aborted("S3 operation during public ACL test")
        .await
        .unwrap()
        .owner()
        .and_then(|owner| owner.id())
        .expect("expected owner ID in GetObjectAcl")
        .to_string()
}

async fn set_bucket_ownership(bucket: &str, ownership: ObjectOwnership) {
    let rule = OwnershipControlsRule::builder()
        .object_ownership(ownership)
        .build()
        .unwrap();
    let controls = OwnershipControls::builder().rules(rule).build().unwrap();
    retrying_operation_aborted(
        "put bucket ownership controls during public ACL setup",
        || {
            CTX.client()
                .put_bucket_ownership_controls()
                .bucket(bucket)
                .ownership_controls(controls.clone())
                .send()
        },
    )
    .await;
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

fn assert_exact_grants(
    grants: &[Grant],
    expected: &[(Permission, Option<&str>, Option<&str>)],
    context: &str,
) {
    assert_eq!(
        grants.len(),
        expected.len(),
        "unexpected grant count for {context}: {grants:?}"
    );
    for (permission, canonical_user_id, uri) in expected {
        assert!(
            has_grant(grants, permission.clone(), *canonical_user_id, *uri),
            "missing grant {permission:?} id={canonical_user_id:?} uri={uri:?} for {context}: {grants:?}"
        );
    }
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
        retrying_operation_aborted("delete object during public ACL cleanup", || {
            client.delete_object().bucket(bucket).key(*key).send()
        })
        .await;
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

#[test]
fn test_anonymous_public_write_put_uses_special_anonymous_owner_id() {
    s3_tests::run(async {
        let bucket = setup_public_write_bucket().await;
        let key = "anonymous-owner";
        let url = format!("{}/{bucket}/{key}", CTX.endpoint());

        let (status, body) = anonymous_put(&url, b"data", &[]);
        assert_eq!(status, 200, "unexpected body: {body}");

        let acl_url = format!("{}/{bucket}/{key}?acl", CTX.endpoint());
        let mut acl = agent().get(&acl_url).call().expect("transport error");
        let acl_status = acl.status().as_u16();
        let acl_body = acl.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(acl_status, 200, "unexpected body: {acl_body}");
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

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_anonymous_public_write_put_object_acl_denied_for_anonymous_owner() {
    s3_tests::run(async {
        let bucket = setup_public_write_bucket().await;
        let key = "anonymous-owner-acl-write";
        let object_url = format!("{}/{bucket}/{key}", CTX.endpoint());
        let acl_url = format!("{object_url}?acl");

        let (status, body) = anonymous_put(&object_url, b"data", &[]);
        assert_eq!(status, 200, "unexpected body: {body}");

        let acl_put = anonymous_put(
            &acl_url,
            b"",
            &[("x-amz-acl".to_string(), "public-read".to_string())],
        );
        assert_eq!(acl_put.0, 403, "unexpected body: {}", acl_put.1);
        assert!(
            acl_put
                .1
                .contains("Anonymous users cannot invoke this API. Please authenticate."),
            "unexpected body: {}",
            acl_put.1
        );

        let mut acl = agent().get(&acl_url).call().expect("transport error");
        let acl_status = acl.status().as_u16();
        let acl_body = acl.body_mut().read_to_string().unwrap_or_default();

        cleanup(&bucket, &[key]).await;

        assert_eq!(acl_status, 200, "unexpected body: {acl_body}");
        assert!(
            !acl_body.contains(
                "<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>READ</Permission>"
            ),
            "unexpected body: {acl_body}"
        );
    });
}

#[test]
fn test_anonymous_public_write_put_bucket_owner_full_control_makes_bucket_owner_object_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_public_write_bucket().await;
        let key = "anonymous-owner-bofc";
        let object_url = format!("{}/{bucket}/{key}", CTX.endpoint());
        let bucket_owner = bucket_owner_id(&bucket).await;

        let put = anonymous_put(
            &object_url,
            b"data",
            &[(
                "x-amz-acl".to_string(),
                "bucket-owner-full-control".to_string(),
            )],
        );
        assert_eq!(put.0, 200, "unexpected body: {}", put.1);

        assert_eq!(object_owner_id(client, &bucket, key).await, bucket_owner);
        let body = owner_get_object_eventually(
            &bucket,
            key,
            "anonymous public-write bucket-owner-full-control owner read",
        )
        .await
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
        assert_eq!(&body[..], b"data");

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during public ACL test")
            .await
            .unwrap();
        assert_eq!(
            acl.owner().and_then(|owner| owner.id()),
            Some(bucket_owner.as_str())
        );
        assert!(
            has_grant(
                acl.grants(),
                Permission::FullControl,
                Some(&bucket_owner),
                None
            ),
            "expected bucket owner FULL_CONTROL grant, got {:?}",
            acl.grants()
        );

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_owner_enforced_disables_legacy_public_read_object_acl() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_acl_enabled_bucket(client, ObjectOwnership::ObjectWriter).await;

        retrying_operation_aborted("put public-read object during public ACL setup", || {
            client
                .put_object()
                .bucket(&bucket)
                .key("pre-boe-public")
                .acl(ObjectCannedAcl::PublicRead)
                .body(ByteStream::from_static(b"public"))
                .send()
        })
        .await;

        let object_url = format!("{}/{}/pre-boe-public", CTX.endpoint(), bucket);

        let body = anon_get_status_eventually(&object_url, 200, "anonymous GET before BOE").await;
        assert_eq!(body.as_bytes(), b"public");

        set_bucket_ownership(&bucket, ObjectOwnership::BucketOwnerEnforced).await;

        anon_get_status_eventually(&object_url, 403, "anonymous GET after BOE").await;

        cleanup(&bucket, &["pre-boe-public"]).await;
    });
}

#[test]
fn test_public_read_bucket_acl_blocks_bucket_owner_enforced_transition() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_public_bucket().await;
        let body = b"<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>";
        let url = format!("{}/{}?ownershipControls", CTX.endpoint(), bucket);

        let put = send_signed_request("PUT", &url, body, std::iter::empty::<(String, String)>());
        assert_eq!(put.status, 400, "unexpected body: {}", put.body);
        assert!(
            put.body
                .contains("<Code>InvalidBucketAclWithObjectOwnership</Code>"),
            "unexpected body: {}",
            put.body
        );

        retrying_operation_aborted("put private bucket ACL during public ACL setup", || {
            client
                .put_bucket_acl()
                .bucket(&bucket)
                .acl(aws_sdk_s3::types::BucketCannedAcl::Private)
                .send()
        })
        .await;
        set_bucket_ownership(&bucket, ObjectOwnership::BucketOwnerEnforced).await;

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_anon_get_object_public_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_public_bucket().await;

        retrying_operation_aborted("put public object during public ACL setup", || {
            client
                .put_object()
                .bucket(&bucket)
                .key("obj")
                .acl(ObjectCannedAcl::PublicRead)
                .body(ByteStream::from_static(b"public data"))
                .send()
        })
        .await;

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

        retrying_operation_aborted("put public object during public ACL setup", || {
            client
                .put_object()
                .bucket(&bucket)
                .key("obj")
                .acl(ObjectCannedAcl::PublicRead)
                .body(ByteStream::from_static(b"head me"))
                .send()
        })
        .await;

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

        retrying_operation_aborted("put object during public ACL setup", || {
            client
                .put_object()
                .bucket(&bucket)
                .key("obj")
                .body(ByteStream::from_static(b"data"))
                .send()
        })
        .await;

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
            .send_retrying_operation_aborted("S3 operation during public ACL test")
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

        retrying_operation_aborted("put object during public ACL setup", || {
            client
                .put_object()
                .bucket(&bucket)
                .key("obj1")
                .body(ByteStream::from_static(b"a"))
                .send()
        })
        .await;

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

        retrying_operation_aborted("put object during public ACL setup", || {
            client
                .put_object()
                .bucket(&bucket)
                .key("obj1")
                .body(ByteStream::from_static(b"a"))
                .send()
        })
        .await;

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

        retrying_operation_aborted("delete object during public ACL cleanup", || {
            CTX.client()
                .delete_object()
                .bucket(&bucket)
                .key("anon-upload")
                .send()
        })
        .await;

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

        retrying_operation_aborted("put tagged public object during public ACL setup", || {
            client
                .put_object()
                .bucket(&bucket)
                .key("obj")
                .acl(ObjectCannedAcl::PublicRead)
                .tagging("env=public")
                .body(ByteStream::from_static(b"hello"))
                .send()
        })
        .await;

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
            .send_retrying_operation_aborted("S3 operation during public ACL test")
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
            .send_retrying_operation_aborted("get object tagging during public ACL test")
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
fn test_object_acl_canned_during_create() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_acl_enabled_bucket(client, ObjectOwnership::ObjectWriter).await;

        retrying_operation_aborted("put public-read object during public ACL setup", || {
            client
                .put_object()
                .bucket(&bucket)
                .key("foo")
                .acl(ObjectCannedAcl::PublicRead)
                .body(ByteStream::from_static(b"bar"))
                .send()
        })
        .await;

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send_retrying_operation_aborted("S3 operation during public ACL test")
            .await
            .unwrap();
        let owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetObjectAcl")
            .to_string();
        assert_exact_grants(
            acl.grants(),
            &[
                (Permission::Read, None, Some(ALL_USERS_GROUP_URI)),
                (Permission::FullControl, Some(owner_id.as_str()), None),
            ],
            "public-read object ACL during create",
        );

        cleanup(&bucket, &["foo"]).await;
    });
}

#[test]
fn test_put_object_acl_canned_public_read_write_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_acl_enabled_bucket(client, ObjectOwnership::ObjectWriter).await;

        retrying_operation_aborted("put object during public ACL setup", || {
            client
                .put_object()
                .bucket(&bucket)
                .key("foo")
                .body(ByteStream::from_static(b"bar"))
                .send()
        })
        .await;

        retrying_operation_aborted("put public-read-write object ACL", || {
            client
                .put_object_acl()
                .bucket(&bucket)
                .key("foo")
                .acl(ObjectCannedAcl::PublicReadWrite)
                .send()
        })
        .await;

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send_retrying_operation_aborted("S3 operation during public ACL test")
            .await
            .unwrap();
        let owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetObjectAcl")
            .to_string();
        assert_exact_grants(
            acl.grants(),
            &[
                (Permission::Read, None, Some(ALL_USERS_GROUP_URI)),
                (Permission::Write, None, Some(ALL_USERS_GROUP_URI)),
                (Permission::FullControl, Some(owner_id.as_str()), None),
            ],
            "public-read-write object ACL via PutObjectAcl",
        );

        let mut anon_get = agent()
            .get(&format!("{}/{}/foo", CTX.endpoint(), bucket))
            .call()
            .expect("anonymous GET transport error");
        assert_eq!(anon_get.status().as_u16(), 200);
        assert_eq!(anon_get.body_mut().read_to_string().unwrap(), "bar");

        cleanup(&bucket, &["foo"]).await;
    });
}

#[test]
fn test_put_object_acl_canned_authenticated_read_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_acl_enabled_bucket(client, ObjectOwnership::ObjectWriter).await;

        retrying_operation_aborted("put object during public ACL setup", || {
            client
                .put_object()
                .bucket(&bucket)
                .key("foo")
                .body(ByteStream::from_static(b"bar"))
                .send()
        })
        .await;

        retrying_operation_aborted("put authenticated-read object ACL", || {
            client
                .put_object_acl()
                .bucket(&bucket)
                .key("foo")
                .acl(ObjectCannedAcl::AuthenticatedRead)
                .send()
        })
        .await;

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send_retrying_operation_aborted("S3 operation during public ACL test")
            .await
            .unwrap();
        let owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetObjectAcl")
            .to_string();
        assert_exact_grants(
            acl.grants(),
            &[
                (Permission::Read, None, Some(AUTHENTICATED_USERS_GROUP_URI)),
                (Permission::FullControl, Some(owner_id.as_str()), None),
            ],
            "authenticated-read object ACL via PutObjectAcl",
        );

        let get = alt_get_object_eventually(&bucket, "foo").await;
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"bar");

        cleanup(&bucket, &["foo"]).await;
    });
}

#[test]
fn test_put_object_acl_without_content_length_header() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_acl_enabled_bucket(client, ObjectOwnership::ObjectWriter).await;

        retrying_operation_aborted("put object during public ACL setup", || {
            client
                .put_object()
                .bucket(&bucket)
                .key("foo")
                .body(ByteStream::from_static(b"bar"))
                .send()
        })
        .await;

        retrying_operation_aborted("put object ACL without content-length", || {
            client
                .put_object_acl()
                .bucket(&bucket)
                .key("foo")
                .acl(ObjectCannedAcl::PublicRead)
                .customize()
                .mutate_request(|req| {
                    req.headers_mut().remove("content-length");
                })
                .send()
        })
        .await;

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send_retrying_operation_aborted("S3 operation during public ACL test")
            .await
            .unwrap();
        let owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetObjectAcl")
            .to_string();
        assert_exact_grants(
            acl.grants(),
            &[
                (Permission::Read, None, Some(ALL_USERS_GROUP_URI)),
                (Permission::FullControl, Some(owner_id.as_str()), None),
            ],
            "PutObjectAcl without Content-Length",
        );

        cleanup(&bucket, &["foo"]).await;
    });
}

#[test]
fn test_object_header_acl_grants_authenticated_users_read() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_acl_enabled_bucket(client, ObjectOwnership::ObjectWriter).await;
        let key = "auth-users-header-grant";

        retrying_operation_aborted("put object with authenticated-read grant", || {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"authenticated-read"))
                .customize()
                .mutate_request(move |req| {
                    req.headers_mut().insert(
                        "x-amz-grant-read",
                        format!("uri=\"{}\"", AUTHENTICATED_USERS_GROUP_URI),
                    );
                })
                .send()
        })
        .await;

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during public ACL test")
            .await
            .unwrap();
        assert_exact_grants(
            acl.grants(),
            &[(Permission::Read, None, Some(AUTHENTICATED_USERS_GROUP_URI))],
            "authenticated users object ACL via header grant",
        );

        let resp = alt_get_object_eventually(&bucket, key).await;
        let body = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"authenticated-read");

        let mut anon = agent()
            .get(&format!("{}/{}/{}", CTX.endpoint(), bucket, key))
            .call()
            .expect("anonymous GET transport error");
        assert_eq!(anon.status().as_u16(), 403);
        let anon_body = anon.body_mut().read_to_string().unwrap();
        assert!(
            anon_body.contains("AccessDenied"),
            "expected AccessDenied for anonymous GET, got {anon_body}"
        );

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_multipart_upload_public_read_acl_allows_anonymous_get() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_acl_enabled_bucket(client, ObjectOwnership::ObjectWriter).await;
        let key = "multipart-public-read";
        let body = vec![b'x'; 1024];

        let create =
            retrying_operation_aborted("create multipart upload during public ACL setup", || {
                client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .acl(ObjectCannedAcl::PublicRead)
                    .send()
            })
            .await;
        let upload_id = create.upload_id().unwrap().to_string();

        let part = retrying_operation_aborted("upload part during public ACL setup", || {
            client
                .upload_part()
                .bucket(&bucket)
                .key(key)
                .upload_id(&upload_id)
                .part_number(1)
                .body(ByteStream::from(body.clone()))
                .send()
        })
        .await;

        retrying_operation_aborted("complete multipart upload during public ACL setup", || {
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
        })
        .await;

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during public ACL test")
            .await
            .unwrap();
        assert!(
            has_grant(
                acl.grants(),
                Permission::Read,
                None,
                Some(ALL_USERS_GROUP_URI),
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

        retrying_operation_aborted("put object during public ACL setup", || {
            client
                .put_object()
                .bucket(&bucket)
                .key("foo123bar")
                .body(ByteStream::from_static(b"foo"))
                .send()
        })
        .await;

        retrying_operation_aborted("copy object during public ACL setup", || {
            client
                .copy_object()
                .bucket(&bucket)
                .key("bar321foo")
                .copy_source(format!("{}/foo123bar", bucket))
                .acl(ObjectCannedAcl::PublicRead)
                .send()
        })
        .await;

        let copied = alt_client
            .get_object()
            .bucket(&bucket)
            .key("bar321foo")
            .send_retrying_operation_aborted("S3 operation during public ACL test")
            .await
            .unwrap();
        let copied_body = copied.body.collect().await.unwrap().into_bytes();
        assert_eq!(&copied_body[..], b"foo");

        let copied_acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("bar321foo")
            .send_retrying_operation_aborted("S3 operation during public ACL test")
            .await
            .unwrap();
        assert!(
            has_grant(
                copied_acl.grants(),
                Permission::Read,
                None,
                Some(ALL_USERS_GROUP_URI),
            ),
            "expected READ grant for AllUsers, got {:?}",
            copied_acl.grants()
        );

        retrying_operation_aborted("replace copy object during public ACL setup", || {
            client
                .copy_object()
                .bucket(&bucket)
                .key("foo123bar")
                .copy_source(format!("{}/bar321foo", bucket))
                .acl(ObjectCannedAcl::PublicRead)
                .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
                .metadata("abc", "def")
                .send()
        })
        .await;

        let overwritten = alt_client
            .get_object()
            .bucket(&bucket)
            .key("foo123bar")
            .send_retrying_operation_aborted("S3 operation during public ACL test")
            .await
            .unwrap();
        let overwritten_body = overwritten.body.collect().await.unwrap().into_bytes();
        assert_eq!(&overwritten_body[..], b"foo");

        let head = client
            .head_object()
            .bucket(&bucket)
            .key("foo123bar")
            .send_retrying_operation_aborted("S3 operation during public ACL test")
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

        retrying_operation_aborted("put owner object during public ACL setup", || {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"owner-body"))
                .send()
        })
        .await;

        let create = alt_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("create multipart upload during public ACL test")
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

        let create =
            retrying_operation_aborted("create multipart upload during public write setup", || {
                alt_client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key(key)
                    .send()
            })
            .await;
        let upload_id = create.upload_id().unwrap().to_string();

        retrying_operation_aborted("put owner object during public write setup", || {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"owner-body"))
                .send()
        })
        .await;

        let data = vec![b'x'; 1024];
        let upload_part =
            retrying_operation_aborted("upload part during public write setup", || {
                alt_client
                    .upload_part()
                    .bucket(&bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .part_number(1)
                    .body(ByteStream::from(data.clone()))
                    .send()
            })
            .await;

        let complete = retrying_operation_aborted(
            "complete multipart upload during public write setup",
            || {
                alt_client
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
            },
        )
        .await;
        assert!(
            complete.e_tag().is_some(),
            "expected CompleteMultipartUpload to return an ETag"
        );

        cleanup(&bucket, &[key]).await;
    });
}
