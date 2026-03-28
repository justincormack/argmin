use aws_sdk_s3::error::ProvideErrorMetadata;
use s3_tests::{err_status, unique_bucket, CTX};

async fn cleanup(bucket: &str) {
    let client = CTX.client();
    client.delete_bucket().bucket(bucket).send().await.unwrap();
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

        cleanup(&bucket).await;
    });
}
