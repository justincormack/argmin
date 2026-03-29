use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use s3_tests::{
    assert_s3_err_code, create_public_bucket, ensure_distinct_s3_owners_or_skip, err_status,
    unique_bucket, CTX,
};
use serde_json::json;

fn agent() -> ureq::Agent {
    s3_tests::test_agent()
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
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
