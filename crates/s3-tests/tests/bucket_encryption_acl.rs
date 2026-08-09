// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BlockedEncryptionTypes, EncryptionType, ObjectOwnership, ServerSideEncryption,
    ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration, ServerSideEncryptionRule,
};
use s3_tests::{
    assert_s3_err_code, create_acl_enabled_bucket, enable_bucket_sse_c, err_status,
    sse_c_header_values, test_sse_c_key, CTX,
};

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

fn require_https_endpoint() {
    assert!(
        CTX.endpoint().starts_with("https://"),
        "bucket encryption SSE-C ACL coverage requires an https:// endpoint; got {}",
        CTX.endpoint()
    );
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

async fn create_acl_bucket() -> String {
    create_acl_enabled_bucket(CTX.client(), ObjectOwnership::ObjectWriter).await
}

async fn cleanup(bucket: &str, key: Option<&str>) {
    let client = CTX.client();
    if let Some(key) = key {
        let _ = s3_tests::delete_object_retrying_operation_aborted(client, bucket, key).await;
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

async fn wait_for_sse_c_blocked(bucket: &str) {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let result = CTX
            .client()
            .get_bucket_encryption()
            .bucket(bucket)
            .send()
            .await;
        if let Ok(resp) = result {
            if let Some(config) = resp.server_side_encryption_configuration() {
                let blocked = blocked_encryption_types(&config.rules()[0]);
                if blocked == vec!["SSE-C".to_string()] {
                    return;
                }
            }
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            continue;
        }
        panic!("bucket SSE-C blocking did not converge for {bucket}");
    }
}

#[test]
fn test_bucket_encryption_acl_default_blocks_sse_c_put_object() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_acl_bucket().await;
        let key = "acl-default-blocked-put";
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
        let status = err_status(&result);
        cleanup(&bucket, Some(key)).await;

        assert_eq!(status, 403);
        assert_s3_err_code(&result, "AccessDenied");
    });
}

#[test]
fn test_bucket_encryption_acl_blocks_sse_c_put_object() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_acl_bucket().await;
        let key = "acl-blocked-put";
        client
            .put_bucket_encryption()
            .bucket(&bucket)
            .server_side_encryption_configuration(sse_s3_bucket_encryption(EncryptionType::SseC))
            .send()
            .await
            .unwrap();
        wait_for_sse_c_blocked(&bucket).await;

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
        let status = err_status(&result);
        cleanup(&bucket, Some(key)).await;

        assert_eq!(status, 403);
        assert_s3_err_code(&result, "AccessDenied");
    });
}

#[test]
fn test_bucket_encryption_acl_blocks_sse_c_create_multipart_upload() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_acl_bucket().await;
        let key = "acl-blocked-mpu";
        client
            .put_bucket_encryption()
            .bucket(&bucket)
            .server_side_encryption_configuration(sse_s3_bucket_encryption(EncryptionType::SseC))
            .send()
            .await
            .unwrap();
        wait_for_sse_c_blocked(&bucket).await;

        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);
        let result = with_sse_c_headers!(
            client.create_multipart_upload().bucket(&bucket).key(key),
            key_b64,
            key_md5_b64
        )
        .send()
        .await;
        let status = err_status(&result);
        cleanup(&bucket, None).await;

        assert_eq!(status, 403);
        assert_s3_err_code(&result, "AccessDenied");
    });
}

#[test]
fn test_bucket_encryption_acl_allows_sse_c_when_unblocked() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_acl_bucket().await;
        let key = "acl-unblocked-put";
        enable_bucket_sse_c(client, &bucket).await;

        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);
        with_sse_c_headers!(
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"allowed")),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();

        let head = with_sse_c_headers!(
            client.head_object().bucket(&bucket).key(key),
            key_b64,
            key_md5_b64
        )
        .send()
        .await
        .unwrap();
        assert_eq!(head.content_length(), Some(7));
        assert_eq!(head.sse_customer_algorithm(), Some("AES256"));

        cleanup(&bucket, Some(key)).await;
    });
}
