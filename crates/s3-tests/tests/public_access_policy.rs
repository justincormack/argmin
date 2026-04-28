use s3_tests::{assert_s3_err_code, err_status, unique_bucket, CTX};

/// Build an agent that returns all HTTP responses (including 4xx/5xx) as Ok.
fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
}

/// Cleanup helper.
async fn cleanup(bucket: &str) {
    let client = CTX.client();
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

#[test]
fn test_block_public_policy() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let pab = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .block_public_acls(false)
            .ignore_public_acls(false)
            .block_public_policy(true)
            .restrict_public_buckets(false)
            .build();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();

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
            .send()
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

        let pab = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .block_public_acls(false)
            .ignore_public_acls(false)
            .block_public_policy(true)
            .restrict_public_buckets(false)
            .build();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();

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
            .send()
            .await
            .unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

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
        match client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
        {
            Ok(_) => {}
            Err(err) => {
                if std::env::var("S3_TEST_ENDPOINT").is_ok()
                    && err.raw_response().map(|resp| resp.status().as_u16()) == Some(403)
                {
                    client
                        .delete_object()
                        .bucket(&bucket)
                        .key("foo")
                        .send()
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

        let pab = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .block_public_acls(false)
            .ignore_public_acls(false)
            .block_public_policy(false)
            .restrict_public_buckets(true)
            .build();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();

        let mut denied_resp = agent().get(&get_url).call().expect("transport error");
        let _ = denied_resp.body_mut().read_to_string();
        assert_eq!(denied_resp.status().as_u16(), 403);

        let owner_resp = client
            .get_object()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        let body = owner_resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], b"bar");

        client
            .delete_object()
            .bucket(&bucket)
            .key("foo")
            .send()
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

        let pab = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .block_public_acls(true)
            .ignore_public_acls(true)
            .block_public_policy(true)
            .restrict_public_buckets(false)
            .build();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();

        let resp = client
            .get_public_access_block()
            .bucket(&bucket)
            .send()
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
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let denied = client
            .get_public_access_block()
            .bucket(&bucket)
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        client
            .delete_bucket_policy()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        cleanup(&bucket).await;
    });
}
