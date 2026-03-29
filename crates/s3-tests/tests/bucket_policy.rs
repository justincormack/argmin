use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    CompletedMultipartUpload, CompletedPart, Grant, ObjectOwnership, Permission,
};
use s3_tests::{
    assert_s3_err_code, create_public_bucket, ensure_distinct_s3_owners_or_skip, err_status,
    unique_bucket, CTX,
};
use serde_json::json;

fn agent() -> ureq::Agent {
    s3_tests::test_agent()
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

fn bucket_resource(bucket: &str) -> String {
    format!("arn:aws:s3:::{bucket}")
}

fn bucket_wildcard_resource(bucket: &str) -> String {
    format!("arn:aws:s3:::{bucket}/*")
}

fn alt_policy_principal() -> Option<serde_json::Value> {
    CTX.alt_account_id()
        .map(|account_id| json!({ "AWS": format!("arn:aws:iam::{account_id}:root") }))
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
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

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
fn test_bucket_policy_list_objects_v1() {
    s3_tests::run(async {
        if !CTX.has_alt_client() {
            return;
        }
        let Some(principal) = alt_policy_principal() else {
            return;
        };

        let client = CTX.client();
        let alt_client = CTX.alt_client();
        if !ensure_distinct_s3_owners_or_skip(
            client,
            alt_client,
            "test_bucket_policy_list_objects_v1",
        )
        .await
        {
            return;
        }

        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();
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
        if !CTX.has_alt_client() {
            return;
        }
        let Some(principal) = alt_policy_principal() else {
            return;
        };

        let client = CTX.client();
        let alt_client = CTX.alt_client();
        if !ensure_distinct_s3_owners_or_skip(
            client,
            alt_client,
            "test_bucket_policy_list_objects_v2",
        )
        .await
        {
            return;
        }

        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();
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
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();
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
        if !CTX.has_alt_client() {
            return;
        }
        let Some(principal) = alt_policy_principal() else {
            return;
        };

        let client = CTX.client();
        let alt_client = CTX.alt_client();
        if !ensure_distinct_s3_owners_or_skip(
            client,
            alt_client,
            "test_bucket_policy_put_obj_grant_full_control",
        )
        .await
        {
            return;
        }

        let bucket = unique_bucket();
        let control_bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client
            .create_bucket()
            .bucket(&control_bucket)
            .send()
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
fn test_bucket_policy_put_obj_request_object_tag() {
    s3_tests::run(async {
        if !CTX.has_alt_client() {
            return;
        }
        let Some(principal) = alt_policy_principal() else {
            return;
        };

        let client = CTX.client();
        let alt_client = CTX.alt_client();
        if !ensure_distinct_s3_owners_or_skip(
            client,
            alt_client,
            "test_bucket_policy_put_obj_request_object_tag",
        )
        .await
        {
            return;
        }

        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();
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
fn test_bucket_policy_multipart_upload_request_object_tag() {
    s3_tests::run(async {
        if !CTX.has_alt_client() {
            return;
        }
        let Some(principal) = alt_policy_principal() else {
            return;
        };

        let client = CTX.client();
        let alt_client = CTX.alt_client();
        if !ensure_distinct_s3_owners_or_skip(
            client,
            alt_client,
            "test_bucket_policy_multipart_upload_request_object_tag",
        )
        .await
        {
            return;
        }

        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();
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
        if !CTX.has_alt_client() {
            return;
        }
        let Some(principal) = alt_policy_principal() else {
            return;
        };

        let client = CTX.client();
        let alt_client = CTX.alt_client();
        if !ensure_distinct_s3_owners_or_skip(
            client,
            alt_client,
            "test_bucket_policy_upload_part_copy_copy_source",
        )
        .await
        {
            return;
        }

        let src_bucket = unique_bucket();
        let dst_bucket = unique_bucket();
        client
            .create_bucket()
            .bucket(&src_bucket)
            .send()
            .await
            .unwrap();
        alt_client
            .create_bucket()
            .bucket(&dst_bucket)
            .send()
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
