use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    CompletedMultipartUpload, CompletedPart, Grant, ObjectOwnership, Permission,
    ServerSideEncryption,
};
use s3_tests::{
    assert_s3_err_code, create_public_bucket, disable_bucket_public_access_block, err_status,
    sse_c_header_values, test_sse_c_key, unique_bucket, CTX,
};
use serde_json::json;

fn agent() -> ureq::Agent {
    s3_tests::test_agent()
}

fn endpoint_is_https() -> bool {
    CTX.endpoint().starts_with("https://")
}

fn require_https_endpoint() {
    assert!(
        endpoint_is_https(),
        "bucket policy SSE-C coverage requires an https:// endpoint; got {}",
        CTX.endpoint()
    );
}

macro_rules! with_sse_c_headers {
    ($op:expr, $algorithm:expr, $key_b64:expr, $key_md5_b64:expr) => {{
        $op.customize().mutate_request({
            let algorithm = $algorithm.to_string();
            let key_b64 = $key_b64.clone();
            let key_md5_b64 = $key_md5_b64.clone();
            move |req| {
                req.headers_mut().insert(
                    "x-amz-server-side-encryption-customer-algorithm",
                    algorithm.clone(),
                );
                req.headers_mut()
                    .insert("x-amz-server-side-encryption-customer-key", key_b64.clone());
                req.headers_mut().insert(
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.clone(),
                );
            }
        })
    }};
}

macro_rules! with_sse_s3_header {
    ($op:expr) => {{
        $op.customize().mutate_request(move |req| {
            req.headers_mut()
                .insert("x-amz-server-side-encryption", "AES256");
        })
    }};
}

async fn cleanup_with_client(client: &aws_sdk_s3::Client, bucket: &str, keys: &[&str]) {
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }

    for _ in 0..10 {
        let uploads = client
            .list_multipart_uploads()
            .bucket(bucket)
            .send()
            .await
            .unwrap();
        for upload in uploads.uploads() {
            let _ = client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(upload.key().unwrap())
                .upload_id(upload.upload_id().unwrap())
                .send()
                .await;
        }

        match client.delete_bucket().bucket(bucket).send().await {
            Ok(_) => return,
            Err(err) => {
                let raw = format!("{err:?}");
                if raw.contains("OperationAborted") || raw.contains("BucketNotEmpty") {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    continue;
                }
                panic!("delete_bucket failed unexpectedly: {raw}");
            }
        }
    }

    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    cleanup_with_client(CTX.client(), bucket, keys).await;
}

async fn create_bucket_allowing_public_policy(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    disable_bucket_public_access_block(client, &bucket).await;
    bucket
}

async fn create_bucket_allowing_sse_c(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket_with_sse_c_enabled(client, &bucket)
        .await
        .unwrap();
    bucket
}

fn bucket_resource(bucket: &str) -> String {
    format!("arn:aws:s3:::{bucket}")
}

fn bucket_wildcard_resource(bucket: &str) -> String {
    format!("arn:aws:s3:::{bucket}/*")
}

fn alt_policy_principal() -> serde_json::Value {
    json!({ "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) })
}

fn fixed_nonpublic_principal() -> serde_json::Value {
    json!({ "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) })
}

async fn bucket_policy_status_is_public(client: &aws_sdk_s3::Client, bucket: &str) -> bool {
    client
        .get_bucket_policy_status()
        .bucket(bucket)
        .send()
        .await
        .unwrap()
        .policy_status()
        .and_then(|status| status.is_public())
        .expect("expected PolicyStatus.IsPublic")
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

async fn canonical_owner_id(client: &aws_sdk_s3::Client, bucket: &str) -> String {
    client
        .get_bucket_acl()
        .bucket(bucket)
        .send()
        .await
        .unwrap()
        .owner()
        .expect("expected owner in GetBucketAcl")
        .id()
        .expect("expected owner ID in GetBucketAcl")
        .to_string()
}

fn has_grant(grants: &[Grant], permission: Permission, canonical_user_id: Option<&str>) -> bool {
    grants.iter().any(|grant| {
        grant.permission() == Some(&permission)
            && grant
                .grantee()
                .is_some_and(|grantee| grantee.id() == canonical_user_id)
    })
}

fn bucket_policy_document(
    principal: serde_json::Value,
    effect: &str,
    action: &str,
    resource: String,
) -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": effect,
            "Principal": principal,
            "Action": action,
            "Resource": resource,
        }],
    })
    .to_string()
}

async fn complete_single_part_upload(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    etag: &str,
) {
    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(CompletedPart::builder().e_tag(etag).part_number(1).build())
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test]
fn test_bucket_policy_put_get_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;

        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Sid": "DenyAllGetObject",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": format!("arn:aws:s3:::{bucket}/*"),
            }],
        })
        .to_string();

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy.clone())
            .send()
            .await
            .unwrap();

        let resp = client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let actual_policy: serde_json::Value =
            serde_json::from_str(resp.policy().unwrap()).unwrap();
        let expected_policy: serde_json::Value = serde_json::from_str(&policy).unwrap();
        assert_eq!(actual_policy, expected_policy);

        client
            .delete_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        let result = client.get_bucket_policy().bucket(&bucket).send().await;
        assert_eq!(err_status(&result), 404);
        let err = result.unwrap_err();
        assert_eq!(
            err.as_service_error().and_then(ProvideErrorMetadata::code),
            Some("NoSuchBucketPolicy")
        );

        client
            .delete_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_get_bucket_policy_status_private_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let result = client
            .get_bucket_policy_status()
            .bucket(&bucket)
            .send()
            .await;
        assert_eq!(err_status(&result), 404);
        assert_eq!(
            result
                .unwrap_err()
                .as_service_error()
                .and_then(ProvideErrorMetadata::code),
            Some("NoSuchBucketPolicy")
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_get_bucket_policy_status_public_bucket_acl() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_public_bucket(client).await;

        let result = client
            .get_bucket_policy_status()
            .bucket(&bucket)
            .send()
            .await;
        assert_eq!(err_status(&result), 404);
        assert_eq!(
            result
                .unwrap_err()
                .as_service_error()
                .and_then(ProvideErrorMetadata::code),
            Some("NoSuchBucketPolicy")
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_get_bucket_policy_status_public_bucket_policy() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;

        let put_result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                json!("*"),
                "Allow",
                "s3:ListBucket",
                bucket_resource(&bucket),
            ))
            .send()
            .await;
        match put_result {
            Ok(_) => {}
            Err(err)
                if std::env::var("S3_TEST_ENDPOINT").is_ok()
                    && err.as_service_error().and_then(ProvideErrorMetadata::code)
                        == Some("AccessDenied")
                    && format!("{err:?}").contains("BlockPublicPolicy") =>
            {
                cleanup(&bucket, &[]).await;
                panic!(
                    "account-level S3 Block Public Access must allow public bucket policies for AWS s3-tests; put_bucket_policy failed with BlockPublicPolicy: {err:?}"
                );
            }
            Err(err) => panic!("put_bucket_policy failed unexpectedly: {err:?}"),
        }

        assert!(bucket_policy_status_is_public(client, &bucket).await);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_get_bucket_policy_status_nonpublic_bucket_policy() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:ListBucket",
                "Resource": bucket_resource(&bucket),
                "Condition": {
                    "IpAddress": {
                        "aws:SourceIp": "10.0.0.0/32"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        assert!(!bucket_policy_status_is_public(client, &bucket).await);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_get_bucket_policy_status_nonpublic_fixed_principal_policy() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_sse_c(client).await;
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": fixed_nonpublic_principal(),
                "Action": "s3:ListBucket",
                "Resource": bucket_resource(&bucket),
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        assert!(!bucket_policy_status_is_public(client, &bucket).await);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_get_bucket_policy_status_cross_account_allow() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        let policy = bucket_policy_document(
            principal,
            "Allow",
            "s3:GetBucketPolicyStatus",
            bucket_resource(&bucket),
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        assert!(!bucket_policy_status_is_public(alt_client, &bucket).await);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_get_bucket_policy_status_cross_account_deny_overrides_allow() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:GetBucketPolicyStatus",
                    "Resource": bucket_resource(&bucket),
                },
                {
                    "Effect": "Deny",
                    "Principal": principal,
                    "Action": "s3:GetBucketPolicyStatus",
                    "Resource": bucket_resource(&bucket),
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        let result = alt_client
            .get_bucket_policy_status()
            .bucket(&bucket)
            .send()
            .await;
        assert_eq!(err_status(&result), 403);
        assert_eq!(
            result
                .unwrap_err()
                .as_service_error()
                .and_then(ProvideErrorMetadata::code),
            Some("AccessDenied")
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_bucket_policy_not_principal_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_sse_c(client).await;
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "NotPrincipal": fixed_nonpublic_principal(),
                "Action": "s3:ListBucket",
                "Resource": bucket_resource(&bucket),
            }],
        })
        .to_string();

        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MalformedPolicy");

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_list_objects_v1() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:ListBucket",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        let response = alt_client
            .list_objects()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(response.contents().len(), 1);
        assert_eq!(response.contents()[0].key(), Some("obj"));

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_bucket_policy_list_objects_v2() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:ListBucket",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();

        let response = alt_client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(response.contents().len(), 1);
        assert_eq!(response.contents()[0].key(), Some("obj"));

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_bucket_policy_list_deny_overrides_bucket_acl() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_public_bucket(client).await;
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let url = format!("{}/{}", CTX.endpoint(), bucket);
        let mut allowed = agent().get(&url).call().expect("transport error");
        assert_eq!(allowed.status().as_u16(), 200);
        let allowed_body = allowed.body_mut().read_to_string().unwrap();
        assert!(
            allowed_body.contains("<Key>obj</Key>"),
            "expected anonymous listing before deny policy: {allowed_body}"
        );

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                json!("*"),
                "Deny",
                "s3:ListBucket",
                bucket_resource(&bucket),
            ))
            .send()
            .await
            .unwrap();
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        let mut denied = agent().get(&url).call().expect("transport error");
        assert_eq!(denied.status().as_u16(), 403);
        let denied_body = denied.body_mut().read_to_string().unwrap();
        assert!(
            denied_body.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied after deny policy: {denied_body}"
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_bucket_policy_list_requires_bucket_resource() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                json!("*"),
                "Allow",
                "s3:ListBucket",
                bucket_wildcard_resource(&bucket),
            ))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MalformedPolicy");

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_grant_full_control() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        let control_bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        s3_tests::create_bucket(client, &control_bucket)
            .await
            .unwrap();
        set_object_writer_ownership(&bucket).await;
        set_object_writer_ownership(&control_bucket).await;
        let owner_id = canonical_owner_id(client, &bucket).await;
        let full_control_header = format!("id=\"{owner_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal.clone(),
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:x-amz-grant-full-control": full_control_header
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&control_bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:PutObject",
                bucket_wildcard_resource(&control_bucket),
            ))
            .send()
            .await
            .unwrap();
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        client
            .get_bucket_policy()
            .bucket(&control_bucket)
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .put_object()
            .bucket(&bucket)
            .key("denied")
            .body(ByteStream::from_static(b"denied"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        alt_client
            .put_object()
            .bucket(&bucket)
            .key("allowed")
            .body(ByteStream::from_static(b"allowed"))
            .customize()
            .mutate_request({
                let full_control_header = full_control_header.clone();
                move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-full-control", full_control_header.clone());
                }
            })
            .send()
            .await
            .unwrap();

        alt_client
            .put_object()
            .bucket(&control_bucket)
            .key("control")
            .body(ByteStream::from_static(b"control"))
            .send()
            .await
            .unwrap();

        let allowed_acl = alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key("allowed")
            .send()
            .await
            .unwrap();
        let control_acl = alt_client
            .get_object_acl()
            .bucket(&control_bucket)
            .key("control")
            .send()
            .await
            .unwrap();
        assert_eq!(
            allowed_acl.owner().and_then(|owner| owner.id()),
            control_acl.owner().and_then(|owner| owner.id())
        );
        assert!(
            has_grant(
                allowed_acl.grants(),
                Permission::FullControl,
                Some(&owner_id)
            ),
            "expected FULL_CONTROL grant for bucket owner, got {:?}",
            allowed_acl.grants()
        );
        assert!(
            !has_grant(
                control_acl.grants(),
                Permission::FullControl,
                Some(&owner_id)
            ),
            "did not expect FULL_CONTROL grant for bucket owner, got {:?}",
            control_acl.grants()
        );

        let control_denied = client
            .get_object_acl()
            .bucket(&control_bucket)
            .key("control")
            .send()
            .await;
        assert_eq!(err_status(&control_denied), 403);
        assert_s3_err_code(&control_denied, "AccessDenied");

        let allowed_owner_view = client
            .get_object_acl()
            .bucket(&bucket)
            .key("allowed")
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(
                allowed_owner_view.grants(),
                Permission::FullControl,
                Some(&owner_id),
            ),
            "expected FULL_CONTROL grant for bucket owner, got {:?}",
            allowed_owner_view.grants()
        );

        cleanup(&bucket, &["allowed", "denied"]).await;
        cleanup(&control_bucket, &["control"]).await;
    });
}

#[test]
fn test_bucket_policy_copy_object_grant_full_control() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_sse_c(client).await;
        set_object_writer_ownership(&bucket).await;
        let owner_id = canonical_owner_id(client, &bucket).await;
        let full_control_header = format!("id=\"{owner_id}\"");
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:GetObject",
                    "Resource": bucket_wildcard_resource(&bucket)
                },
                {
                    "Effect": "Allow",
                    "Principal": principal,
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:x-amz-grant-full-control": full_control_header
                        }
                    }
                }
            ],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"copy-source"))
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .copy_object()
            .bucket(&bucket)
            .key("denied")
            .copy_source(format!("{bucket}/src"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        alt_client
            .copy_object()
            .bucket(&bucket)
            .key("allowed")
            .copy_source(format!("{bucket}/src"))
            .customize()
            .mutate_request({
                let full_control_header = full_control_header.clone();
                move |req| {
                    req.headers_mut()
                        .insert("x-amz-grant-full-control", full_control_header.clone());
                }
            })
            .send()
            .await
            .unwrap();

        let allowed_acl = alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key("allowed")
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(
                allowed_acl.grants(),
                Permission::FullControl,
                Some(&owner_id),
            ),
            "expected FULL_CONTROL grant for bucket owner, got {:?}",
            allowed_acl.grants()
        );

        cleanup(&bucket, &["src", "allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_requires_sse_c_algorithm_header() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_sse_c(client).await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "Null": {
                        "s3:x-amz-server-side-encryption-customer-algorithm": "true"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let denied = client
            .put_object()
            .bucket(&bucket)
            .key("denied")
            .body(ByteStream::from_static(b"plain"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);
        client
            .put_object()
            .bucket(&bucket)
            .key("allowed")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64.clone())
            .sse_customer_key_md5(key_md5_b64.clone())
            .body(ByteStream::from_static(b"secret"))
            .send()
            .await
            .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key("allowed")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64)
            .sse_customer_key_md5(key_md5_b64)
            .send()
            .await
            .unwrap();
        assert_eq!(
            get.body.collect().await.unwrap().into_bytes().as_ref(),
            b"secret"
        );

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_requires_sse_s3_header() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "Null": {
                        "s3:x-amz-server-side-encryption": "true"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let denied = client
            .put_object()
            .bucket(&bucket)
            .key("denied")
            .body(ByteStream::from_static(b"plain"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        client
            .put_object()
            .bucket(&bucket)
            .key("allowed")
            .server_side_encryption(ServerSideEncryption::Aes256)
            .body(ByteStream::from_static(b"secret"))
            .send()
            .await
            .unwrap();

        let head = client
            .head_object()
            .bucket(&bucket)
            .key("allowed")
            .send()
            .await
            .unwrap();
        assert_eq!(
            head.server_side_encryption(),
            Some(&ServerSideEncryption::Aes256)
        );

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_sse_c_algorithm_string_not_equals() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_sse_c(client).await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringNotEquals": {
                        "s3:x-amz-server-side-encryption-customer-algorithm": "AES256"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let denied = client
            .put_object()
            .bucket(&bucket)
            .key("denied")
            .body(ByteStream::from_static(b"plain"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);
        client
            .put_object()
            .bucket(&bucket)
            .key("allowed")
            .sse_customer_algorithm("AES256")
            .sse_customer_key(key_b64)
            .sse_customer_key_md5(key_md5_b64)
            .body(ByteStream::from_static(b"secret"))
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_sse_s3_string_not_equals() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringNotEquals": {
                        "s3:x-amz-server-side-encryption": "AES256"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let denied = client
            .put_object()
            .bucket(&bucket)
            .key("denied")
            .body(ByteStream::from_static(b"plain"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        client
            .put_object()
            .bucket(&bucket)
            .key("allowed")
            .server_side_encryption(ServerSideEncryption::Aes256)
            .body(ByteStream::from_static(b"secret"))
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_copy_object_recognizes_destination_sse_c_header() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_sse_c(client).await;

        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"copy-source"))
            .send()
            .await
            .unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "Null": {
                        "s3:x-amz-server-side-encryption-customer-algorithm": "true"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);
        with_sse_c_headers!(
            client
                .copy_object()
                .bucket(&bucket)
                .key("dst")
                .copy_source(format!("{bucket}/src")),
            "AES256",
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let get = with_sse_c_headers!(
            client.get_object().bucket(&bucket).key("dst"),
            "AES256",
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        assert_eq!(
            get.body.collect().await.unwrap().into_bytes().as_ref(),
            b"copy-source"
        );

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

#[test]
fn test_bucket_policy_copy_object_recognizes_destination_sse_s3_header() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"copy-source"))
            .send()
            .await
            .unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "Null": {
                        "s3:x-amz-server-side-encryption": "true"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        with_sse_s3_header!(client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{bucket}/src")))
        .send()
        .await
        .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key("dst")
            .send()
            .await
            .unwrap();
        assert_eq!(
            get.server_side_encryption(),
            Some(&ServerSideEncryption::Aes256)
        );
        assert_eq!(
            get.body.collect().await.unwrap().into_bytes().as_ref(),
            b"copy-source"
        );

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

#[test]
fn test_bucket_policy_complete_multipart_does_not_reuse_destination_sse_c_header() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_sse_c(client).await;

        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"copy-source"))
            .send()
            .await
            .unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "Null": {
                        "s3:x-amz-server-side-encryption-customer-algorithm": "true"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);
        let create = with_sse_c_headers!(
            client.create_multipart_upload().bucket(&bucket).key("dst"),
            "AES256",
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let copied_part = with_sse_c_headers!(
            client
                .upload_part_copy()
                .bucket(&bucket)
                .key("dst")
                .upload_id(&upload_id)
                .part_number(1)
                .copy_source(format!("{bucket}/src")),
            "AES256",
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let complete = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("dst")
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(copied_part.copy_part_result().unwrap().e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;
        assert_eq!(err_status(&complete), 403);
        assert_s3_err_code(&complete, "AccessDenied");
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("dst")
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        let get = with_sse_c_headers!(
            client.get_object().bucket(&bucket).key("dst"),
            "AES256",
            key_b64,
            key_md5_b64
        )
        .send()
        .await;
        assert_eq!(err_status(&get), 404);
        assert_s3_err_code(&get, "NoSuchKey");

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

#[test]
fn test_bucket_policy_multipart_copy_inherits_destination_sse_s3() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"copy-source"))
            .send()
            .await
            .unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "Null": {
                        "s3:x-amz-server-side-encryption": "true"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let create =
            with_sse_s3_header!(client.create_multipart_upload().bucket(&bucket).key("dst"))
                .send()
                .await
                .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let copied_part = client
            .upload_part_copy()
            .bucket(&bucket)
            .key("dst")
            .upload_id(&upload_id)
            .part_number(1)
            .copy_source(format!("{bucket}/src"))
            .send()
            .await
            .unwrap();
        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("dst")
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(copied_part.copy_part_result().unwrap().e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key("dst")
            .send()
            .await
            .unwrap();
        assert_eq!(
            get.server_side_encryption(),
            Some(&ServerSideEncryption::Aes256)
        );
        assert_eq!(
            get.body.collect().await.unwrap().into_bytes().as_ref(),
            b"copy-source"
        );

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

#[test]
fn test_bucket_policy_streaming_put_rejects_lowercase_sse_c_algorithm() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_sse_c(client).await;

        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);
        let put = with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key("obj")
                .body(ByteStream::from_static(b"body")),
            "aes256",
            key_b64,
            key_md5_b64
        )
        .send()
        .await;
        assert_eq!(err_status(&put), 400);
        assert_s3_err_code(&put, "InvalidEncryptionAlgorithmError");

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_request_object_tag() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:RequestObjectTag/security": "public"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .put_object()
            .bucket(&bucket)
            .key("denied")
            .body(ByteStream::from_static(b"denied"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        alt_client
            .put_object()
            .bucket(&bucket)
            .key("allowed")
            .tagging("security=public")
            .body(ByteStream::from_static(b"allowed"))
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_multipart_upload_requires_object_resource() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_bucket_allowing_public_policy(client).await;
        let key = "mpobj";

        let denied = alt_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let bucket_only = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal.clone(),
                "Allow",
                "s3:PutObject",
                bucket_resource(&bucket),
            ))
            .send()
            .await;
        assert_eq!(err_status(&bucket_only), 400);
        assert_s3_err_code(&bucket_only, "MalformedPolicy");

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:PutObject",
                format!("arn:aws:s3:::{bucket}/{key}"),
            ))
            .send()
            .await
            .unwrap();

        let upload = alt_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload.upload_id().unwrap())
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_multipart_upload_on_bucket_with_policy() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_bucket_allowing_public_policy(client).await;
        let key = "foo";

        let owner_principal = json!({
            "AWS": format!("arn:aws:iam::{}:root", CTX.account_id())
        });
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": owner_principal,
                "Action": "*",
                "Resource": [
                    bucket_resource(&bucket),
                    bucket_wildcard_resource(&bucket),
                ],
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let upload = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = upload.upload_id().unwrap().to_string();
        let part = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"policy-body"))
            .send()
            .await
            .unwrap();
        complete_single_part_upload(client, &bucket, key, &upload_id, part.e_tag().unwrap()).await;

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"policy-body");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_multipart_upload_request_object_tag() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringEquals": {
                        "s3:RequestObjectTag/security": "public"
                    }
                }
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();
        client
            .get_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("denied")
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let upload = alt_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("allowed")
            .tagging("security=public")
            .send()
            .await
            .unwrap();
        let upload_id = upload.upload_id().unwrap().to_string();
        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("allowed")
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &["allowed", "denied"]).await;
    });
}

#[test]
fn test_bucket_policy_upload_part_copy_copy_source() {
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let src_bucket = unique_bucket();
        let dst_bucket = unique_bucket();
        s3_tests::create_bucket(client, &src_bucket).await.unwrap();
        s3_tests::create_bucket(alt_client, &dst_bucket)
            .await
            .unwrap();

        for (key, body) in [
            ("public/foo", ByteStream::from_static(b"public/foo")),
            ("public/bar", ByteStream::from_static(b"public/bar")),
            ("private/foo", ByteStream::from_static(b"private/foo")),
        ] {
            client
                .put_object()
                .bucket(&src_bucket)
                .key(key)
                .body(body)
                .send()
                .await
                .unwrap();
        }

        let src_policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:GetObject",
                "Resource": format!("arn:aws:s3:::{src_bucket}/public/*"),
            }],
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&src_bucket)
            .policy(src_policy)
            .send()
            .await
            .unwrap();
        client
            .get_bucket_policy()
            .bucket(&src_bucket)
            .send()
            .await
            .unwrap();

        let upload = alt_client
            .create_multipart_upload()
            .bucket(&dst_bucket)
            .key("copied")
            .send()
            .await
            .unwrap();
        let upload_id = upload.upload_id().unwrap().to_string();

        let copied_part = alt_client
            .upload_part_copy()
            .bucket(&dst_bucket)
            .key("copied")
            .upload_id(&upload_id)
            .part_number(1)
            .copy_source(format!("{src_bucket}/public/foo"))
            .send()
            .await
            .unwrap();
        complete_single_part_upload(
            alt_client,
            &dst_bucket,
            "copied",
            &upload_id,
            copied_part.copy_part_result().unwrap().e_tag().unwrap(),
        )
        .await;

        let second_upload = alt_client
            .create_multipart_upload()
            .bucket(&dst_bucket)
            .key("copied2")
            .send()
            .await
            .unwrap();
        let second_upload_id = second_upload.upload_id().unwrap().to_string();
        let second_part = alt_client
            .upload_part_copy()
            .bucket(&dst_bucket)
            .key("copied2")
            .upload_id(&second_upload_id)
            .part_number(1)
            .copy_source(format!("{src_bucket}/public/bar"))
            .send()
            .await
            .unwrap();
        complete_single_part_upload(
            alt_client,
            &dst_bucket,
            "copied2",
            &second_upload_id,
            second_part.copy_part_result().unwrap().e_tag().unwrap(),
        )
        .await;

        let denied_upload = alt_client
            .create_multipart_upload()
            .bucket(&dst_bucket)
            .key("copied-denied")
            .send()
            .await
            .unwrap();
        let denied_upload_id = denied_upload.upload_id().unwrap().to_string();
        let denied = alt_client
            .upload_part_copy()
            .bucket(&dst_bucket)
            .key("copied-denied")
            .upload_id(&denied_upload_id)
            .part_number(1)
            .copy_source(format!("{src_bucket}/private/foo"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let response = alt_client
            .get_object()
            .bucket(&dst_bucket)
            .key("copied")
            .send()
            .await
            .unwrap();
        let body = response.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"public/foo");

        let response = alt_client
            .get_object()
            .bucket(&dst_bucket)
            .key("copied2")
            .send()
            .await
            .unwrap();
        let body = response.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"public/bar");

        cleanup_with_client(
            alt_client,
            &dst_bucket,
            &["copied", "copied2", "copied-denied"],
        )
        .await;
        cleanup(&src_bucket, &["public/foo", "public/bar", "private/foo"]).await;
    });
}

/// Apply the same policy to two different buckets and verify both work.
///
/// Matches Ceph: test_bucket_policy_another_bucket
#[test]
fn test_bucket_policy_another_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let principal = alt_policy_principal();

        let bucket1 = unique_bucket();
        let bucket2 = unique_bucket();
        s3_tests::create_bucket(client, &bucket1).await.unwrap();
        s3_tests::create_bucket(client, &bucket2).await.unwrap();

        client
            .put_object()
            .bucket(&bucket1)
            .key("obj1")
            .body(ByteStream::from_static(b"data1"))
            .send()
            .await
            .unwrap();
        client
            .put_object()
            .bucket(&bucket2)
            .key("obj2")
            .body(ByteStream::from_static(b"data2"))
            .send()
            .await
            .unwrap();

        client
            .put_bucket_policy()
            .bucket(&bucket1)
            .policy(bucket_policy_document(
                principal.clone(),
                "Allow",
                "s3:ListBucket",
                bucket_resource(&bucket1),
            ))
            .send()
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket2)
            .policy(bucket_policy_document(
                principal,
                "Allow",
                "s3:ListBucket",
                bucket_resource(&bucket2),
            ))
            .send()
            .await
            .unwrap();

        let resp1 = alt_client
            .list_objects()
            .bucket(&bucket1)
            .send()
            .await
            .unwrap();
        assert_eq!(resp1.contents().len(), 1);
        assert_eq!(resp1.contents()[0].key(), Some("obj1"));

        let resp2 = alt_client
            .list_objects()
            .bucket(&bucket2)
            .send()
            .await
            .unwrap();
        assert_eq!(resp2.contents().len(), 1);
        assert_eq!(resp2.contents()[0].key(), Some("obj2"));

        cleanup(&bucket1, &["obj1"]).await;
        cleanup(&bucket2, &["obj2"]).await;
    });
}

// StringLikeIfExists condition operator: condition is satisfied when the key
// is absent from the request (IfExists semantics).
//
// Matches the Ceph IfExists compatibility gap on the currently supported
// condition-key subset.
#[test]
fn test_bucket_policy_condition_operator_if_exists() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_bucket_allowing_public_policy(client).await;

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": alt_policy_principal(),
                "Action": "s3:PutObject",
                "Resource": bucket_wildcard_resource(&bucket),
                "Condition": {
                    "StringLikeIfExists": {
                        "s3:x-amz-copy-source": "src/public/*"
                    }
                }
            }]
        })
        .to_string();

        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        alt_client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        let body = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"bar");

        cleanup(&bucket, &["foo"]).await;
    });
}
