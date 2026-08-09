// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::delete_bucket_policy::{
    DeleteBucketPolicyError, DeleteBucketPolicyOutput,
};
use aws_sdk_s3::operation::put_bucket_policy::{PutBucketPolicyError, PutBucketPolicyOutput};
use aws_sdk_s3::operation::put_public_access_block::{
    PutPublicAccessBlockError, PutPublicAccessBlockOutput,
};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::PublicAccessBlockConfiguration;
use aws_sdk_s3::Client;
use s3_tests::{
    assert_s3_err_code, err_status, retrying_operation_aborted, retrying_operation_aborted_result,
    send_signed_request,
    shape::{assert_shape, error_response_headers, shape},
    unique_bucket, SendRetryingOperationAborted, CTX,
};

/// Build an agent that returns all HTTP responses (including 4xx/5xx) as Ok.
fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
}

/// Cleanup helper.
async fn cleanup(bucket: &str) {
    let client = CTX.client();
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

async fn put_public_access_block_retrying_operation_aborted(
    client: &Client,
    bucket: &str,
    config: PublicAccessBlockConfiguration,
) -> PutPublicAccessBlockOutput {
    retrying_operation_aborted::<PutPublicAccessBlockOutput, PutPublicAccessBlockError, _, _>(
        "put public access block",
        || {
            client
                .put_public_access_block()
                .bucket(bucket)
                .public_access_block_configuration(config.clone())
                .send()
        },
    )
    .await
}

async fn put_bucket_policy_retrying_operation_aborted(
    client: &Client,
    bucket: &str,
    policy: &str,
) -> PutBucketPolicyOutput {
    retrying_operation_aborted::<PutBucketPolicyOutput, PutBucketPolicyError, _, _>(
        "put bucket policy",
        || {
            client
                .put_bucket_policy()
                .bucket(bucket)
                .policy(policy)
                .send()
        },
    )
    .await
}

async fn put_bucket_policy_result_retrying_operation_aborted(
    client: &Client,
    bucket: &str,
    policy: &str,
) -> Result<PutBucketPolicyOutput, SdkError<PutBucketPolicyError>> {
    retrying_operation_aborted_result(|| {
        client
            .put_bucket_policy()
            .bucket(bucket)
            .policy(policy)
            .send()
    })
    .await
}

async fn delete_bucket_policy_retrying_operation_aborted(
    client: &Client,
    bucket: &str,
) -> DeleteBucketPolicyOutput {
    retrying_operation_aborted::<DeleteBucketPolicyOutput, DeleteBucketPolicyError, _, _>(
        "delete bucket policy",
        || client.delete_bucket_policy().bucket(bucket).send(),
    )
    .await
}

async fn put_object_retrying_operation_aborted(
    client: &Client,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
) {
    retrying_operation_aborted("put object during public access policy setup", || {
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body.clone()))
            .send()
    })
    .await;
}

#[test]
fn test_block_public_policy() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let pab = PublicAccessBlockConfiguration::builder()
            .block_public_acls(false)
            .ignore_public_acls(false)
            .block_public_policy(true)
            .restrict_public_buckets(false)
            .build();
        put_public_access_block_retrying_operation_aborted(client, &bucket, pab).await;

        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:GetObject",
                "Resource": format!("arn:aws:s3:::{bucket}/*"),
            }],
        })
        .to_string();

        let result = client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send_retrying_operation_aborted("put bucket policy during public access policy test")
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        cleanup(&bucket).await;
    });
}

#[test]
fn test_block_public_policy_with_principal() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let pab = PublicAccessBlockConfiguration::builder()
            .block_public_acls(false)
            .ignore_public_acls(false)
            .block_public_policy(true)
            .restrict_public_buckets(false)
            .build();
        put_public_access_block_retrying_operation_aborted(client, &bucket, pab).await;

        let principal =
            serde_json::json!({"AWS": format!("arn:aws:iam::{}:root", CTX.account_id())});
        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": principal,
                "Action": "s3:GetObject",
                "Resource": format!("arn:aws:s3:::{bucket}/*"),
            }],
        })
        .to_string();

        put_bucket_policy_retrying_operation_aborted(client, &bucket, &policy).await;

        let resp = client
            .get_bucket_policy()
            .bucket(&bucket)
            .send_retrying_operation_aborted("get bucket policy during public access policy test")
            .await
            .unwrap();
        let actual_policy: serde_json::Value =
            serde_json::from_str(resp.policy().unwrap()).unwrap();
        let expected_policy: serde_json::Value = serde_json::from_str(&policy).unwrap();
        assert_eq!(actual_policy, expected_policy);

        delete_bucket_policy_retrying_operation_aborted(client, &bucket).await;

        cleanup(&bucket).await;
    });
}

#[test]
fn test_block_public_restrict_public_buckets() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        client
            .delete_public_access_block()
            .bucket(&bucket)
            .send_retrying_operation_aborted(
                "delete public access block during public access policy test",
            )
            .await
            .unwrap();

        put_object_retrying_operation_aborted(client, &bucket, "foo", b"bar".to_vec()).await;

        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:GetObject",
                "Resource": format!("arn:aws:s3:::{bucket}/*"),
            }],
        })
        .to_string();
        match put_bucket_policy_result_retrying_operation_aborted(client, &bucket, &policy).await {
            Ok(_) => {}
            Err(err) => {
                if std::env::var("S3_TEST_ENDPOINT").is_ok()
                    && err.raw_response().map(|resp| resp.status().as_u16()) == Some(403)
                {
                    client
                        .delete_object()
                        .bucket(&bucket)
                        .key("foo")
                        .send_retrying_operation_aborted(
                            "delete object during public access policy cleanup",
                        )
                        .await
                        .unwrap();
                    cleanup(&bucket).await;
                    panic!(
                        "account-level S3 Block Public Access must allow public bucket policies for AWS s3-tests; put_bucket_policy failed while setting up RestrictPublicBuckets coverage: {err:?}"
                    );
                }
                panic!("put_bucket_policy failed: {err:?}");
            }
        }

        let get_url = format!("{}/{bucket}/foo", CTX.endpoint());
        let mut public_resp = agent().get(&get_url).call().expect("transport error");
        assert_eq!(public_resp.status().as_u16(), 200);
        assert_eq!(public_resp.body_mut().read_to_string().unwrap(), "bar");

        let pab = PublicAccessBlockConfiguration::builder()
            .block_public_acls(false)
            .ignore_public_acls(false)
            .block_public_policy(false)
            .restrict_public_buckets(true)
            .build();
        put_public_access_block_retrying_operation_aborted(client, &bucket, pab).await;

        let mut denied_resp = agent().get(&get_url).call().expect("transport error");
        let _ = denied_resp.body_mut().read_to_string();
        assert_eq!(denied_resp.status().as_u16(), 403);

        let owner_resp = client
            .get_object()
            .bucket(&bucket)
            .key("foo")
            .send_retrying_operation_aborted("get object during public access policy test")
            .await
            .unwrap();
        let body = owner_resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], b"bar");

        client
            .delete_object()
            .bucket(&bucket)
            .key("foo")
            .send_retrying_operation_aborted("delete object during public access policy cleanup")
            .await
            .unwrap();
        cleanup(&bucket).await;
    });
}

#[test]
fn test_get_public_block_deny_bucket_policy() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let pab = PublicAccessBlockConfiguration::builder()
            .block_public_acls(true)
            .ignore_public_acls(true)
            .block_public_policy(true)
            .restrict_public_buckets(false)
            .build();
        put_public_access_block_retrying_operation_aborted(client, &bucket, pab).await;

        let resp = client
            .get_public_access_block()
            .bucket(&bucket)
            .send_retrying_operation_aborted(
                "get public access block during public access policy test",
            )
            .await
            .unwrap();
        let config = resp.public_access_block_configuration().unwrap();
        assert_eq!(config.block_public_acls(), Some(true));
        assert_eq!(config.ignore_public_acls(), Some(true));
        assert_eq!(config.block_public_policy(), Some(true));
        assert_eq!(config.restrict_public_buckets(), Some(false));

        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": {"AWS": "*"},
                "Action": "s3:GetBucketPublicAccessBlock",
                "Resource": format!("arn:aws:s3:::{bucket}"),
            }],
        })
        .to_string();
        put_bucket_policy_retrying_operation_aborted(client, &bucket, &policy).await;

        let denied = client
            .get_public_access_block()
            .bucket(&bucket)
            .send_retrying_operation_aborted(
                "get public access block during public access policy test",
            )
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        delete_bucket_policy_retrying_operation_aborted(client, &bucket).await;
        cleanup(&bucket).await;
    });
}

/// Full error shape for a public policy rejected by BlockPublicPolicy. The
/// message names the requester, so the principal is a shape-only `{any}`:
/// AWS renders an IAM user ARN, other deployments their own principal form.
#[test]
fn test_block_public_policy_denial_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let pab = PublicAccessBlockConfiguration::builder()
            .block_public_acls(false)
            .ignore_public_acls(false)
            .block_public_policy(true)
            .restrict_public_buckets(false)
            .build();
        put_public_access_block_retrying_operation_aborted(client, &bucket, pab).await;

        let policy = format!(
            "{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Effect\":\"Allow\",\
             \"Principal\":\"*\",\"Action\":\"s3:GetObject\",\
             \"Resource\":\"arn:aws:s3:::{bucket}/*\"}}]}}"
        );
        let response = send_signed_request(
            "PUT",
            &format!("{}/{}?policy=", CTX.endpoint(), bucket),
            policy.as_bytes(),
            std::iter::empty::<(&str, &str)>(),
        );
        assert_shape(
            "PutBucketPolicy public with BlockPublicPolicy",
            &response,
            &shape()
                .status(403)
                .headers(error_response_headers())
                .sub("bucket", &bucket)
                .body(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error><Code>AccessDenied</Code>\
                     <Message>User: {any} is not authorized to perform: \
                     s3:PutBucketPolicy on resource: \"arn:aws:s3:::{bucket}\" \
                     because public policies are prevented by the \
                     BlockPublicPolicy setting in S3 Block Public Access.</Message>\
                     <RequestId>{request_id}</RequestId>\
                     <HostId>{host_id}</HostId></Error>",
                ),
        );

        cleanup(&bucket).await;
    });
}
