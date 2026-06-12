use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, MetadataDirective,
    VersioningConfiguration,
};
use s3_tests::{
    assert_s3_err_code, object_url, post_object_raw_to_test_endpoint_with_headers,
    sigv4_post_fields_for_credentials, unique_bucket, SendRetryingOperationAborted, CTX,
};
use std::time::Duration;

const SYSTEM_METADATA_SIZE_LIMIT: usize = 2 * 1024;
const WEBSITE_REDIRECT_HEADER_NAME: &str = "x-amz-website-redirect-location";
const WEBSITE_REDIRECT_OPERATION_ATTEMPTS: usize = 20;

fn is_operation_aborted<E: ProvideErrorMetadata>(err: &aws_sdk_s3::error::SdkError<E>) -> bool {
    err.as_service_error().and_then(ProvideErrorMetadata::code) == Some("OperationAborted")
}

async fn put_object_retrying_operation_aborted(
    bucket: &str,
    key: &str,
    redirect: Option<&str>,
    body: Vec<u8>,
) -> aws_sdk_s3::operation::put_object::PutObjectOutput {
    for attempt in 0..WEBSITE_REDIRECT_OPERATION_ATTEMPTS {
        let mut put = CTX
            .client()
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body.clone()));
        if let Some(redirect) = redirect {
            put = put.website_redirect_location(redirect);
        }
        match put.send().await {
            Ok(output) => return output,
            Err(err)
                if is_operation_aborted(&err)
                    && attempt + 1 < WEBSITE_REDIRECT_OPERATION_ATTEMPTS =>
            {
                tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
            }
            Err(err) => panic!("put object during website redirect test: {err:?}"),
        }
    }
    unreachable!("put object retry loop must return on final attempt");
}

async fn put_object_result_retrying_operation_aborted(
    bucket: &str,
    key: &str,
    redirect: Option<&str>,
    body: Vec<u8>,
) -> Result<
    aws_sdk_s3::operation::put_object::PutObjectOutput,
    aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>,
> {
    for attempt in 0..WEBSITE_REDIRECT_OPERATION_ATTEMPTS {
        let mut put = CTX
            .client()
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body.clone()));
        if let Some(redirect) = redirect {
            put = put.website_redirect_location(redirect);
        }
        match put.send().await {
            Err(err)
                if is_operation_aborted(&err)
                    && attempt + 1 < WEBSITE_REDIRECT_OPERATION_ATTEMPTS =>
            {
                tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
            }
            result => return result,
        }
    }
    unreachable!("put object result retry loop must return on final attempt");
}

async fn upload_part_retrying_operation_aborted(
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: Vec<u8>,
) -> aws_sdk_s3::operation::upload_part::UploadPartOutput {
    for attempt in 0..WEBSITE_REDIRECT_OPERATION_ATTEMPTS {
        match CTX
            .client()
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
        {
            Ok(output) => return output,
            Err(err)
                if is_operation_aborted(&err)
                    && attempt + 1 < WEBSITE_REDIRECT_OPERATION_ATTEMPTS =>
            {
                tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
            }
            Err(err) => panic!("upload part during website redirect test: {err:?}"),
        }
    }
    unreachable!("upload part retry loop must return on final attempt");
}

async fn setup_bucket() -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket(CTX.client(), &bucket)
        .await
        .unwrap();
    bucket
}

async fn setup_versioned_bucket() -> String {
    let bucket = setup_bucket().await;
    CTX.client()
        .put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send_retrying_operation_aborted("put bucket versioning during website redirect setup")
        .await
        .unwrap();
    bucket
}

async fn cleanup_bucket(bucket: &str) {
    let _ = CTX
        .client()
        .delete_bucket()
        .bucket(bucket)
        .send_retrying_operation_aborted("delete bucket during website redirect cleanup")
        .await;
}

async fn cleanup_object_and_bucket(bucket: &str, key: &str) {
    let _ = CTX
        .client()
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send_retrying_operation_aborted("delete object during website redirect cleanup")
        .await;
    cleanup_bucket(bucket).await;
}

async fn complete_single_part_multipart_upload_with_redirect(
    bucket: &str,
    key: &str,
    redirect: Option<&str>,
    body: &[u8],
) {
    let mut create = CTX
        .client()
        .create_multipart_upload()
        .bucket(bucket)
        .key(key);
    if let Some(redirect) = redirect {
        create = create.website_redirect_location(redirect);
    }
    let upload = create
        .send_retrying_operation_aborted("create multipart upload during website redirect setup")
        .await
        .unwrap();
    let upload_id = upload.upload_id().unwrap().to_string();

    let part =
        upload_part_retrying_operation_aborted(bucket, key, &upload_id, 1, body.to_vec()).await;

    CTX.client()
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part.e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send_retrying_operation_aborted("complete multipart upload during website redirect setup")
        .await
        .unwrap();
}

fn assert_raw_s3_error_code(response: &s3_tests::RawResponse, status: u16, code: &str) {
    assert_eq!(
        response.status, status,
        "unexpected response body: {}",
        response.body
    );
    assert!(
        response.body.contains(&format!("<Code>{code}</Code>")),
        "expected {code} in response body, got: {}",
        response.body
    );
}

fn assert_raw_metadata_too_large(response: &s3_tests::RawResponse, expected_size: usize) {
    assert_raw_s3_error_code(response, 400, "MetadataTooLarge");
    assert!(
        response.body.contains(
            "<Message>Your metadata headers exceed the maximum allowed metadata size</Message>"
        ),
        "expected MetadataTooLarge message, got: {}",
        response.body
    );
    assert!(
        response
            .body
            .contains(&format!("<Size>{expected_size}</Size>")),
        "expected Size={expected_size}, got: {}",
        response.body
    );
    assert!(
        response
            .body
            .contains("<MaxSizeAllowed>2048</MaxSizeAllowed>"),
        "expected MaxSizeAllowed=2048, got: {}",
        response.body
    );
}

fn redirect_value_with_len(len: usize) -> String {
    assert!(len >= 1, "redirect length must allow leading slash");
    format!("/{}", "r".repeat(len - 1))
}

fn put_object_with_raw_headers(
    bucket: &str,
    key: &str,
    headers: Vec<(String, String)>,
) -> s3_tests::RawResponse {
    let url = object_url(CTX.endpoint(), bucket, key, None);
    s3_tests::send_signed_request("PUT", &url, b"redirect-boundary-body", headers)
}

#[test]
fn test_put_object_website_redirect_round_trips_on_head_and_get() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "put-redirect";
        let redirect = "/docs/landing.html";

        put_object_retrying_operation_aborted(
            &bucket,
            key,
            Some(redirect),
            b"redirect-body".to_vec(),
        )
        .await;

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await
            .unwrap();
        assert_eq!(head.website_redirect_location(), Some(redirect));

        let get = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await
            .unwrap();
        assert_eq!(get.website_redirect_location(), Some(redirect));
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], b"redirect-body");

        cleanup_object_and_bucket(&bucket, key).await;
    });
}

#[test]
fn test_put_object_website_redirect_without_leading_slash_rejected() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "invalid-relative";
        let url = object_url(CTX.endpoint(), &bucket, key, None);

        let response = s3_tests::send_signed_request(
            "PUT",
            &url,
            b"invalid-relative-body",
            [("x-amz-website-redirect-location", "docs/landing.html")],
        );

        assert_raw_s3_error_code(&response, 400, "InvalidRedirectLocation");

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await;
        assert!(head.is_err(), "invalid redirect should not create object");

        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_put_object_website_redirect_unsupported_scheme_rejected() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "invalid-scheme";

        let result = put_object_result_retrying_operation_aborted(
            &bucket,
            key,
            Some("ftp://example.com/out"),
            b"invalid-scheme-body".to_vec(),
        )
        .await;

        assert_s3_err_code(&result, "InvalidRedirectLocation");

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await;
        assert!(head.is_err(), "invalid redirect should not create object");

        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_put_object_website_redirect_exact_2k_with_header_name_is_accepted() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "redirect-2k-exact";
        let redirect_len = SYSTEM_METADATA_SIZE_LIMIT - WEBSITE_REDIRECT_HEADER_NAME.len();
        let redirect = redirect_value_with_len(redirect_len);

        let response = put_object_with_raw_headers(
            &bucket,
            key,
            vec![(WEBSITE_REDIRECT_HEADER_NAME.to_string(), redirect.clone())],
        );
        assert_eq!(
            response.status, 200,
            "unexpected response body: {}",
            response.body
        );

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await
            .unwrap();
        assert_eq!(head.website_redirect_location(), Some(redirect.as_str()));

        cleanup_object_and_bucket(&bucket, key).await;
    });
}

#[test]
fn test_put_object_website_redirect_lengths_over_aggregate_limit_are_rejected() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        for redirect_len in [
            SYSTEM_METADATA_SIZE_LIMIT - WEBSITE_REDIRECT_HEADER_NAME.len() + 1,
            SYSTEM_METADATA_SIZE_LIMIT,
            SYSTEM_METADATA_SIZE_LIMIT + 1,
        ] {
            let expected_size = WEBSITE_REDIRECT_HEADER_NAME.len() + redirect_len;
            let key = format!("redirect-over-limit-{redirect_len}");
            let redirect = redirect_value_with_len(redirect_len);
            let response = put_object_with_raw_headers(
                &bucket,
                &key,
                vec![(WEBSITE_REDIRECT_HEADER_NAME.to_string(), redirect)],
            );
            assert_raw_metadata_too_large(&response, expected_size);

            let head = CTX
                .client()
                .head_object()
                .bucket(&bucket)
                .key(&key)
                .send_retrying_operation_aborted("S3 operation during website redirect test")
                .await;
            assert!(
                head.is_err(),
                "over-limit redirect should not create object"
            );
        }

        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_put_object_website_redirect_exact_2k_plus_small_system_header_is_rejected() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "redirect-plus-cache-control";
        let redirect_len = SYSTEM_METADATA_SIZE_LIMIT - WEBSITE_REDIRECT_HEADER_NAME.len();
        let redirect = redirect_value_with_len(redirect_len);

        let response = put_object_with_raw_headers(
            &bucket,
            key,
            vec![
                (WEBSITE_REDIRECT_HEADER_NAME.to_string(), redirect),
                ("cache-control".to_string(), "x".to_string()),
            ],
        );
        let expected_size =
            WEBSITE_REDIRECT_HEADER_NAME.len() + redirect_len + "cache-control".len() + "x".len();
        assert_raw_metadata_too_large(&response, expected_size);

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await;
        assert!(
            head.is_err(),
            "aggregate over-limit request should not create object"
        );

        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_copy_object_does_not_copy_redirect_without_explicit_header() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let src_key = "copy-src";
        let dst_key = "copy-dst";
        let redirect = "/docs/source.html";

        put_object_retrying_operation_aborted(
            &bucket,
            src_key,
            Some(redirect),
            b"copy-body".to_vec(),
        )
        .await;

        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key(dst_key)
            .copy_source(format!("{bucket}/{src_key}"))
            .metadata_directive(MetadataDirective::Copy)
            .send_retrying_operation_aborted("copy object during website redirect test")
            .await
            .unwrap();

        let src_head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(src_key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await
            .unwrap();
        assert_eq!(src_head.website_redirect_location(), Some(redirect));

        let dst_head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await
            .unwrap();
        assert_eq!(dst_head.website_redirect_location(), None);

        let _ = CTX
            .client()
            .delete_object()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("delete copied object during website redirect cleanup")
            .await;
        cleanup_object_and_bucket(&bucket, src_key).await;
    });
}

#[test]
fn test_copy_object_explicit_redirect_persists_on_destination() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let src_key = "copy-src-explicit";
        let dst_key = "copy-dst-explicit";

        put_object_retrying_operation_aborted(
            &bucket,
            src_key,
            None,
            b"copy-explicit-body".to_vec(),
        )
        .await;

        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key(dst_key)
            .copy_source(format!("{bucket}/{src_key}"))
            .website_redirect_location("/docs/destination.html")
            .send_retrying_operation_aborted("copy object during website redirect test")
            .await
            .unwrap();

        let dst_head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await
            .unwrap();
        assert_eq!(
            dst_head.website_redirect_location(),
            Some("/docs/destination.html")
        );

        let _ = CTX
            .client()
            .delete_object()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("delete copied object during website redirect cleanup")
            .await;
        cleanup_object_and_bucket(&bucket, src_key).await;
    });
}

#[test]
fn test_copy_object_same_key_redirect_only_change_is_allowed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "copy-same-key-redirect";

        put_object_retrying_operation_aborted(&bucket, key, None, b"same-key-body".to_vec()).await;

        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key(key)
            .copy_source(format!("{bucket}/{key}"))
            .website_redirect_location("/docs/changed-by-copy.html")
            .send_retrying_operation_aborted("copy object during website redirect test")
            .await
            .unwrap();

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await
            .unwrap();
        assert_eq!(
            head.website_redirect_location(),
            Some("/docs/changed-by-copy.html")
        );

        cleanup_object_and_bucket(&bucket, key).await;
    });
}

#[test]
fn test_copy_object_same_key_with_explicit_same_redirect_is_allowed() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "copy-same-key-no-change";
        let redirect = "/docs/original.html";

        put_object_retrying_operation_aborted(
            &bucket,
            key,
            Some(redirect),
            b"same-key-no-change-body".to_vec(),
        )
        .await;

        // AWS accepts the request when the redirect header is explicitly
        // present, even if the value matches the stored redirect exactly.
        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key(key)
            .copy_source(format!("{bucket}/{key}"))
            .website_redirect_location(redirect)
            .send_retrying_operation_aborted("copy object during website redirect test")
            .await
            .unwrap();

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await
            .unwrap();
        assert_eq!(head.website_redirect_location(), Some(redirect));

        cleanup_object_and_bucket(&bucket, key).await;
    });
}

#[test]
fn test_multipart_upload_redirect_persists_from_initiation() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "multipart-redirect";
        let redirect = "/docs/multipart.html";

        complete_single_part_multipart_upload_with_redirect(
            &bucket,
            key,
            Some(redirect),
            b"multipart-body",
        )
        .await;

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await
            .unwrap();
        assert_eq!(head.website_redirect_location(), Some(redirect));

        cleanup_object_and_bucket(&bucket, key).await;
    });
}

#[test]
fn test_multipart_upload_without_redirect_remains_absent() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "multipart-no-redirect";

        complete_single_part_multipart_upload_with_redirect(&bucket, key, None, b"multipart-body")
            .await;

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await
            .unwrap();
        assert_eq!(head.website_redirect_location(), None);

        cleanup_object_and_bucket(&bucket, key).await;
    });
}

#[test]
fn test_post_object_website_redirect_persists() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-redirect";
        let redirect = "/docs/post-landing.html";

        let mut fields = sigv4_post_fields_for_credentials(
            CTX.access_key(),
            CTX.secret_key(),
            CTX.region(),
            &bucket,
            key,
            &[serde_json::json!({"x-amz-website-redirect-location": redirect})],
        );
        fields.push((
            "x-amz-website-redirect-location".to_string(),
            redirect.to_string(),
        ));

        let response = post_object_raw_to_test_endpoint_with_headers(
            CTX.endpoint(),
            CTX.tls_ca_pem(),
            &bucket,
            &fields,
            b"post-redirect-body",
            "post.txt",
            &[],
        );
        assert_eq!(response.status, 204, "unexpected body: {}", response.body);

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await
            .unwrap();
        assert_eq!(head.website_redirect_location(), Some(redirect));

        cleanup_object_and_bucket(&bucket, key).await;
    });
}

#[test]
fn test_post_object_website_redirect_policy_missing_field_is_rejected() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-redirect-policy-missing";

        let mut fields = sigv4_post_fields_for_credentials(
            CTX.access_key(),
            CTX.secret_key(),
            CTX.region(),
            &bucket,
            key,
            &[],
        );
        fields.push((
            "x-amz-website-redirect-location".to_string(),
            "/docs/policy-missing.html".to_string(),
        ));

        let response = post_object_raw_to_test_endpoint_with_headers(
            CTX.endpoint(),
            CTX.tls_ca_pem(),
            &bucket,
            &fields,
            b"post-policy-missing-body",
            "post.txt",
            &[],
        );
        assert_raw_s3_error_code(&response, 403, "AccessDenied");
        assert!(
            response
                .body
                .to_ascii_lowercase()
                .contains("x-amz-website-redirect-location"),
            "expected field name in response body, got: {}",
            response.body
        );

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await;
        assert!(head.is_err(), "policy rejection should not create object");

        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_post_object_website_redirect_policy_mismatch_is_rejected() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "post-redirect-policy-mismatch";

        let mut fields = sigv4_post_fields_for_credentials(
            CTX.access_key(),
            CTX.secret_key(),
            CTX.region(),
            &bucket,
            key,
            &[serde_json::json!({
                "x-amz-website-redirect-location": "/docs/expected.html"
            })],
        );
        fields.push((
            "x-amz-website-redirect-location".to_string(),
            "/docs/actual.html".to_string(),
        ));

        let response = post_object_raw_to_test_endpoint_with_headers(
            CTX.endpoint(),
            CTX.tls_ca_pem(),
            &bucket,
            &fields,
            b"post-policy-mismatch-body",
            "post.txt",
            &[],
        );
        assert_raw_s3_error_code(&response, 403, "AccessDenied");
        assert!(
            response
                .body
                .to_ascii_lowercase()
                .contains("x-amz-website-redirect-location"),
            "expected field name in response body, got: {}",
            response.body
        );

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await;
        assert!(head.is_err(), "policy mismatch should not create object");

        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_versioned_objects_surface_redirect_metadata_per_version() {
    s3_tests::run(async {
        let bucket = setup_versioned_bucket().await;
        let key = "versioned-redirect";

        let put_v1 = put_object_retrying_operation_aborted(
            &bucket,
            key,
            Some("/docs/v1.html"),
            b"version-one".to_vec(),
        )
        .await;
        let v1 = put_v1.version_id().unwrap().to_string();

        let put_v2 = put_object_retrying_operation_aborted(
            &bucket,
            key,
            Some("/docs/v2.html"),
            b"version-two".to_vec(),
        )
        .await;
        let v2 = put_v2.version_id().unwrap().to_string();

        let current_head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await
            .unwrap();
        assert_eq!(
            current_head.website_redirect_location(),
            Some("/docs/v2.html")
        );

        let v1_head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&v1)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await
            .unwrap();
        assert_eq!(v1_head.website_redirect_location(), Some("/docs/v1.html"));

        let v2_get = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&v2)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await
            .unwrap();
        assert_eq!(v2_get.website_redirect_location(), Some("/docs/v2.html"));
        let v2_body = v2_get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&v2_body[..], b"version-two");

        let v1_get = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&v1)
            .send_retrying_operation_aborted("S3 operation during website redirect test")
            .await
            .unwrap();
        assert_eq!(v1_get.website_redirect_location(), Some("/docs/v1.html"));
        let v1_body = v1_get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&v1_body[..], b"version-one");

        s3_tests::cleanup_versioned_bucket(CTX.client(), &bucket).await;
    });
}
