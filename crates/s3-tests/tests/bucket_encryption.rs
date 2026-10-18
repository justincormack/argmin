// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BlockedEncryptionTypes, CompletedMultipartUpload, CompletedPart, EncryptionType,
    ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
    ServerSideEncryptionRule,
};
use s3_tests::{
    assert_s3_err_code, content_md5_header, create_bucket_with_sse_c_enabled, err_status,
    post_object_to_test_endpoint, raw_bucket, send_signed_request,
    shape::{assert_shape, id_headers, shape},
    sigv4_post_fields_for_credentials, sse_c_header_values, test_sse_c_key, unique_bucket,
    SendRetryingOperationAborted, CTX,
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

async fn cleanup(bucket: &str, key: Option<&str>) {
    let client = CTX.client();
    if let Some(key) = key {
        let _ = s3_tests::delete_object_retrying_operation_aborted(client, bucket, key).await;
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

async fn assert_sse_s3_put_selection(bucket: &str, key: &str, explicit: bool) {
    let client = CTX.client();
    let body = format!("write-selection-{key}").into_bytes();
    let put = s3_tests::retrying_operation_aborted("write-selection PutObject", || {
        let body = body.clone();
        async move {
            let mut request = client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from(body));
            if explicit {
                request = request.server_side_encryption(ServerSideEncryption::Aes256);
            }
            request.send().await
        }
    })
    .await;
    assert_eq!(
        put.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "PutObject did not report SSE-S3 for {key}"
    );

    let head = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send_retrying_operation_aborted("write-selection HeadObject")
        .await
        .expect("write-selection HeadObject");
    assert_eq!(
        head.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "HeadObject did not report SSE-S3 for {key}"
    );
    assert_eq!(head.content_length(), Some(body.len() as i64));
}

async fn assert_sse_s3_head_selection(bucket: &str, key: &str) {
    let head = CTX
        .client()
        .head_object()
        .bucket(bucket)
        .key(key)
        .send_retrying_operation_aborted("write-selection HeadObject")
        .await
        .expect("write-selection HeadObject");
    assert_eq!(
        head.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "HeadObject did not report SSE-S3 for {key}"
    );
}

async fn assert_sse_s3_post_selection(bucket: &str, key: &str, explicit: bool) {
    let conditions = explicit
        .then(|| serde_json::json!({"x-amz-server-side-encryption": "AES256"}))
        .into_iter()
        .collect::<Vec<_>>();
    let mut fields = sigv4_post_fields_for_credentials(
        CTX.access_key(),
        CTX.secret_key(),
        CTX.region(),
        bucket,
        key,
        &conditions,
    );
    if explicit {
        fields.push((
            "x-amz-server-side-encryption".to_string(),
            "AES256".to_string(),
        ));
    }
    let field_refs = fields
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect::<Vec<_>>();
    let (status, body) = post_object_to_test_endpoint(
        CTX.endpoint(),
        CTX.tls_ca_pem(),
        bucket,
        &field_refs,
        b"write-selection-post",
        "write-selection.txt",
    );
    assert_eq!(status, 204, "POST Object failed: {body}");
    assert_sse_s3_head_selection(bucket, key).await;
}

async fn assert_sse_s3_copy_selection(
    bucket: &str,
    source_key: &str,
    destination_key: &str,
    explicit: bool,
) {
    assert_sse_s3_put_selection(bucket, source_key, explicit).await;
    let client = CTX.client();
    let mut request = client
        .copy_object()
        .bucket(bucket)
        .key(destination_key)
        .copy_source(format!("{bucket}/{source_key}"));
    if explicit {
        request = request.server_side_encryption(ServerSideEncryption::Aes256);
    }
    let copy = request
        .send_retrying_operation_aborted("write-selection CopyObject")
        .await
        .expect("write-selection CopyObject");
    assert_eq!(
        copy.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "CopyObject did not report SSE-S3 for {destination_key}"
    );
    assert_sse_s3_head_selection(bucket, destination_key).await;
}

async fn create_sse_s3_multipart_upload(bucket: &str, key: &str, explicit: bool) -> String {
    let client = CTX.client();
    let mut request = client.create_multipart_upload().bucket(bucket).key(key);
    if explicit {
        request = request.server_side_encryption(ServerSideEncryption::Aes256);
    }
    let create = request
        .send_retrying_operation_aborted("write-selection CreateMultipartUpload")
        .await
        .expect("write-selection CreateMultipartUpload");
    assert_eq!(
        create.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "CreateMultipartUpload did not report SSE-S3 for {key}"
    );
    create.upload_id().expect("multipart upload ID").to_string()
}

async fn complete_sse_s3_multipart_upload(bucket: &str, key: &str, upload_id: &str, etag: &str) {
    let complete = CTX
        .client()
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(CompletedPart::builder().part_number(1).e_tag(etag).build())
                .build(),
        )
        .send_retrying_operation_aborted("write-selection CompleteMultipartUpload")
        .await
        .expect("write-selection CompleteMultipartUpload");
    assert_eq!(
        complete.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "CompleteMultipartUpload did not report SSE-S3 for {key}"
    );
    assert_sse_s3_head_selection(bucket, key).await;
}

async fn assert_sse_s3_multipart_selection(bucket: &str, key: &str, explicit: bool) {
    let client = CTX.client();
    let upload_id = create_sse_s3_multipart_upload(bucket, key, explicit).await;
    let part = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"write-selection-part"))
        .send()
        .await
        .expect("write-selection UploadPart");
    assert_eq!(
        part.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "UploadPart did not report inherited SSE-S3 for {key}"
    );
    complete_sse_s3_multipart_upload(
        bucket,
        key,
        &upload_id,
        part.e_tag().expect("UploadPart ETag"),
    )
    .await;
}

async fn assert_sse_s3_upload_part_copy_selection(
    bucket: &str,
    source_key: &str,
    destination_key: &str,
    explicit: bool,
) {
    assert_sse_s3_put_selection(bucket, source_key, explicit).await;
    let client = CTX.client();
    let upload_id = create_sse_s3_multipart_upload(bucket, destination_key, explicit).await;
    let part = client
        .upload_part_copy()
        .bucket(bucket)
        .key(destination_key)
        .upload_id(&upload_id)
        .part_number(1)
        .copy_source(format!("{bucket}/{source_key}"))
        .send_retrying_operation_aborted("write-selection UploadPartCopy")
        .await
        .expect("write-selection UploadPartCopy");
    assert_eq!(
        part.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "UploadPartCopy did not report inherited SSE-S3 for {destination_key}"
    );
    complete_sse_s3_multipart_upload(
        bucket,
        destination_key,
        &upload_id,
        part.copy_part_result()
            .and_then(|result| result.e_tag())
            .expect("UploadPartCopy ETag"),
    )
    .await;
}

async fn assert_sse_s3_operation_selection_matrix(bucket: &str, prefix: &str, explicit: bool) {
    let put_key = format!("{prefix}-put");
    let post_key = format!("{prefix}-post");
    let copy_source = format!("{prefix}-copy-source");
    let copy_destination = format!("{prefix}-copy-destination");
    let multipart_key = format!("{prefix}-multipart");
    let part_copy_source = format!("{prefix}-part-copy-source");
    let part_copy_destination = format!("{prefix}-part-copy-destination");

    assert_sse_s3_put_selection(bucket, &put_key, explicit).await;
    assert_sse_s3_post_selection(bucket, &post_key, explicit).await;
    assert_sse_s3_copy_selection(bucket, &copy_source, &copy_destination, explicit).await;
    assert_sse_s3_multipart_selection(bucket, &multipart_key, explicit).await;
    assert_sse_s3_upload_part_copy_selection(
        bucket,
        &part_copy_source,
        &part_copy_destination,
        explicit,
    )
    .await;

    for key in [
        put_key,
        post_key,
        copy_source,
        copy_destination,
        multipart_key,
        part_copy_source,
        part_copy_destination,
    ] {
        s3_tests::delete_object_retrying_operation_aborted(CTX.client(), bucket, &key)
            .await
            .expect("delete write-selection object");
    }
}

#[test]
fn test_bucket_encryption_write_selection_matrix() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        assert_sse_s3_operation_selection_matrix(&bucket, "implicit-fresh", false).await;

        client
            .put_bucket_encryption()
            .bucket(&bucket)
            .server_side_encryption_configuration(sse_s3_bucket_encryption(EncryptionType::SseC))
            .send_retrying_operation_aborted("set explicit AES256 bucket encryption")
            .await
            .expect("set explicit AES256 bucket encryption");
        assert_sse_s3_operation_selection_matrix(&bucket, "explicit-bucket-default", false).await;

        client
            .delete_bucket_encryption()
            .bucket(&bucket)
            .send_retrying_operation_aborted("delete explicit bucket encryption")
            .await
            .expect("delete explicit bucket encryption");
        assert_sse_s3_operation_selection_matrix(&bucket, "implicit-after-delete", false).await;
        assert_sse_s3_operation_selection_matrix(&bucket, "explicit-request", true).await;

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_encryption_raw_get_returns_canonical_xml() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        s3_tests::create_bucket(CTX.client(), &bucket)
            .await
            .unwrap();

        let body = br#"
            <ServerSideEncryptionConfiguration>
                <Rule>
                    <BucketKeyEnabled>false</BucketKeyEnabled>
                    <BlockedEncryptionTypes>
                        <EncryptionType>SSE-C</EncryptionType>
                    </BlockedEncryptionTypes>
                    <ApplyServerSideEncryptionByDefault>
                        <SSEAlgorithm>AES256</SSEAlgorithm>
                    </ApplyServerSideEncryptionByDefault>
                </Rule>
            </ServerSideEncryptionConfiguration>
        "#;

        let parsed = server_http::http::xml::parse_bucket_encryption_xml(body).unwrap();
        let expected = server_http::http::xml::get_bucket_encryption_xml(parsed.effective());

        let url = format!("{}/{}?encryption", CTX.endpoint(), bucket);
        let put = send_signed_request("PUT", &url, body, [content_md5_header(body)]);
        assert_eq!(put.status, 200, "unexpected body: {}", put.body);

        let get = send_signed_request("GET", &url, b"", std::iter::empty::<(String, String)>());

        cleanup(&bucket, None).await;

        assert_eq!(get.status, 200, "unexpected body: {}", get.body);
        assert_eq!(get.body, expected);
    });
}

#[test]
fn test_get_bucket_encryption_default_blocks_sse_c() {
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
        assert_eq!(blocked, vec!["SSE-C".to_string()]);

        cleanup(&bucket, None).await;
    });
}

#[test]
fn test_bucket_encryption_default_sse_c_put_is_denied() {
    require_https_endpoint();
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let key = "default-blocked-put";
        s3_tests::create_bucket(client, &bucket).await.unwrap();

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

// ── GetBucketEncryption response shape ──────────────────────────────

#[test]
fn test_get_bucket_encryption_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let encryption = aws_sdk_s3::types::ServerSideEncryptionConfiguration::builder()
            .rules(
                aws_sdk_s3::types::ServerSideEncryptionRule::builder()
                    .apply_server_side_encryption_by_default(
                        aws_sdk_s3::types::ServerSideEncryptionByDefault::builder()
                            .sse_algorithm(aws_sdk_s3::types::ServerSideEncryption::Aes256)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .build()
            .unwrap();
        client
            .put_bucket_encryption()
            .bucket(&bucket)
            .server_side_encryption_configuration(encryption)
            .send()
            .await
            .expect("put bucket encryption");

        let response = raw_bucket("GET", &bucket, Some("encryption="));
        assert_shape(
            "GetBucketEncryption",
            &response,
            &shape().status(200).headers(id_headers()).body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <ServerSideEncryptionConfiguration \
                     xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Rule>\
                     <BucketKeyEnabled>false</BucketKeyEnabled>\
                     <ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm>\
                     </ApplyServerSideEncryptionByDefault><BlockedEncryptionTypes>\
                     <EncryptionType>SSE-C</EncryptionType></BlockedEncryptionTypes></Rule>\
                     </ServerSideEncryptionConfiguration>",
            ),
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_get_bucket_encryption_default_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // A fresh bucket's default encryption configuration: SSE-S3 with
        // SSE-C uploads blocked, identical to the explicitly-configured
        // AES256 shape.
        let response = raw_bucket("GET", &bucket, Some("encryption="));
        assert_shape(
            "GetBucketEncryption default",
            &response,
            &shape().status(200).headers(id_headers()).body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <ServerSideEncryptionConfiguration \
                     xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Rule>\
                     <BucketKeyEnabled>false</BucketKeyEnabled>\
                     <ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm>\
                     </ApplyServerSideEncryptionByDefault><BlockedEncryptionTypes>\
                     <EncryptionType>SSE-C</EncryptionType></BlockedEncryptionTypes></Rule>\
                     </ServerSideEncryptionConfiguration>",
            ),
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

/// Full ack shapes for PutBucketEncryption (200) and
/// DeleteBucketEncryption (204).
#[test]
fn test_bucket_encryption_write_ack_shapes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let config = "<ServerSideEncryptionConfiguration \
             xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Rule>\
             <ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm>\
             </ApplyServerSideEncryptionByDefault></Rule>\
             </ServerSideEncryptionConfiguration>";
        let md5 = content_md5_header(config.as_bytes());
        let put = send_signed_request(
            "PUT",
            &format!("{}/{}?encryption=", CTX.endpoint(), bucket),
            config.as_bytes(),
            [(md5.0.as_str(), md5.1.as_str())],
        );
        assert_shape(
            "PutBucketEncryption ack",
            &put,
            &shape().status(200).headers(id_headers()).body_empty(),
        );
        assert_shape(
            "DeleteBucketEncryption ack",
            &raw_bucket("DELETE", &bucket, Some("encryption=")),
            &shape().status(204).headers(id_headers()).body_empty(),
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}
