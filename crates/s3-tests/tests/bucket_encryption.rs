use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BlockedEncryptionTypes, EncryptionType, ServerSideEncryption, ServerSideEncryptionByDefault,
    ServerSideEncryptionConfiguration, ServerSideEncryptionRule,
};
use s3_tests::{
    assert_s3_err_code, create_bucket_with_sse_c_enabled, err_status, sse_c_header_values,
    test_sse_c_key, unique_bucket, CTX,
};

fn endpoint_is_https() -> bool {
    CTX.endpoint().starts_with("https://")
}

fn require_https_endpoint() {
    assert!(
        endpoint_is_https(),
        "bucket encryption SSE-C coverage requires an https:// endpoint; got {}",
        CTX.endpoint()
    );
}

macro_rules! with_sse_c_headers {
    ($op:expr, $key_b64:expr, $key_md5_b64:expr) => {{
        $op.customize().mutate_request({
            let key_b64 = $key_b64.clone();
            let key_md5_b64 = $key_md5_b64.clone();
            move |req| {
                req.headers_mut()
                    .insert("x-amz-server-side-encryption-customer-algorithm", "AES256");
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

fn sse_s3_bucket_encryption(blocked: EncryptionType) -> ServerSideEncryptionConfiguration {
    let default = ServerSideEncryptionByDefault::builder()
        .sse_algorithm(ServerSideEncryption::Aes256)
        .build()
        .unwrap();
    ServerSideEncryptionConfiguration::builder()
        .rules(
            ServerSideEncryptionRule::builder()
                .apply_server_side_encryption_by_default(default)
                .blocked_encryption_types(
                    BlockedEncryptionTypes::builder()
                        .encryption_type(blocked)
                        .build(),
                )
                .build(),
        )
        .build()
        .unwrap()
}

fn blocked_encryption_types(rule: &ServerSideEncryptionRule) -> Vec<String> {
    rule.blocked_encryption_types()
        .map(|blocked| {
            blocked
                .encryption_type()
                .iter()
                .map(|value| value.as_str().to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn default_sse_c_blocked(rule: &ServerSideEncryptionRule) -> bool {
    let blocked = blocked_encryption_types(rule);
    blocked == vec!["SSE-C".to_string()]
}

async fn cleanup(bucket: &str, key: Option<&str>) {
    let client = CTX.client();
    if let Some(key) = key {
        let _ = client.delete_object().bucket(bucket).key(key).send().await;
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

#[test]
fn test_get_bucket_encryption_default_reports_current_sse_c_state() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let resp = client
            .get_bucket_encryption()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let config = resp.server_side_encryption_configuration().unwrap();
        assert_eq!(config.rules().len(), 1);
        let rule = &config.rules()[0];
        let default = rule.apply_server_side_encryption_by_default().unwrap();
        assert_eq!(default.sse_algorithm(), &ServerSideEncryption::Aes256);

        let blocked = blocked_encryption_types(rule);
        assert!(
            blocked.is_empty()
                || blocked == vec!["NONE".to_string()]
                || blocked == vec!["SSE-C".to_string()],
            "expected default blocked types to be NONE/empty or SSE-C, got {:?}",
            blocked
        );

        cleanup(&bucket, None).await;
    });
}

#[test]
fn test_bucket_encryption_default_sse_c_put_behavior_matches_reported_state() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "default-blocked-put";
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let resp = client
            .get_bucket_encryption()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let rule = &resp.server_side_encryption_configuration().unwrap().rules()[0];
        let sse_c_blocked = default_sse_c_blocked(rule);

        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);
        let result = with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"blocked")),
            key_b64,
            key_md5_b64
        )
        .send()
        .await;
        if sse_c_blocked {
            assert_eq!(err_status(&result), 403);
            assert_s3_err_code(&result, "AccessDenied");
            cleanup(&bucket, None).await;
        } else {
            result.unwrap();
            cleanup(&bucket, Some(key)).await;
        }
    });
}

#[test]
fn test_put_get_bucket_encryption_blocks_and_unblocks_sse_c() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        client
            .put_bucket_encryption()
            .bucket(&bucket)
            .server_side_encryption_configuration(sse_s3_bucket_encryption(EncryptionType::SseC))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_bucket_encryption()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let blocked = blocked_encryption_types(
            &resp.server_side_encryption_configuration().unwrap().rules()[0],
        );
        assert_eq!(blocked, vec!["SSE-C".to_string()]);

        client
            .put_bucket_encryption()
            .bucket(&bucket)
            .server_side_encryption_configuration(sse_s3_bucket_encryption(EncryptionType::None))
            .send()
            .await
            .unwrap();

        let resp = client
            .get_bucket_encryption()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let blocked = blocked_encryption_types(
            &resp.server_side_encryption_configuration().unwrap().rules()[0],
        );
        assert!(
            blocked.is_empty() || blocked == vec!["NONE".to_string()],
            "expected no blocked types or NONE, got {:?}",
            blocked
        );

        cleanup(&bucket, None).await;
    });
}

#[test]
fn test_bucket_encryption_blocks_sse_c_put_object() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "blocked-put";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_bucket_encryption()
            .bucket(&bucket)
            .server_side_encryption_configuration(sse_s3_bucket_encryption(EncryptionType::SseC))
            .send()
            .await
            .unwrap();

        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);
        let result = with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"blocked")),
            key_b64,
            key_md5_b64
        )
        .send()
        .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        cleanup(&bucket, None).await;
    });
}

#[test]
fn test_bucket_encryption_blocks_sse_c_create_multipart_upload() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "blocked-mpu";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_bucket_encryption()
            .bucket(&bucket)
            .server_side_encryption_configuration(sse_s3_bucket_encryption(EncryptionType::SseC))
            .send()
            .await
            .unwrap();

        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);
        let result = with_sse_c_headers!(
            client.create_multipart_upload().bucket(&bucket).key(key),
            key_b64,
            key_md5_b64
        )
        .send()
        .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        cleanup(&bucket, None).await;
    });
}

#[test]
fn test_bucket_encryption_does_not_block_existing_sse_c_reads() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "existing-sse-c-object";
        create_bucket_with_sse_c_enabled(client, &bucket)
            .await
            .unwrap();

        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);
        with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"secret-body")),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        client
            .put_bucket_encryption()
            .bucket(&bucket)
            .server_side_encryption_configuration(sse_s3_bucket_encryption(EncryptionType::SseC))
            .send()
            .await
            .unwrap();

        let resp = with_sse_c_headers!(
            client.get_object().bucket(&bucket).key(key),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        let body = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"secret-body");

        cleanup(&bucket, Some(key)).await;
    });
}
