use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, MetadataDirective,
    VersioningConfiguration,
};
use s3_tests::{
    assert_s3_err_code, object_url, post_object_raw_to_test_endpoint_with_headers,
    sigv4_post_fields_for_credentials, unique_bucket, CTX,
};

fn aws_conformance_mode() -> bool {
    std::env::var_os("S3_TEST_ENDPOINT").is_some()
}

fn skip_without_external_endpoint() -> bool {
    if aws_conformance_mode() {
        false
    } else {
        eprintln!(
            "skipping website_redirect Phase 1 conformance test in embedded-server mode; \
             these tests are intended to map AWS REST behavior before Argmin implements it"
        );
        true
    }
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
        .send()
        .await
        .unwrap();
    bucket
}

async fn cleanup_bucket(bucket: &str) {
    let _ = CTX.client().delete_bucket().bucket(bucket).send().await;
}

async fn cleanup_object_and_bucket(bucket: &str, key: &str) {
    let _ = CTX
        .client()
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
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
    let upload = create.send().await.unwrap();
    let upload_id = upload.upload_id().unwrap().to_string();

    let part = CTX
        .client()
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(body.to_vec()))
        .send()
        .await
        .unwrap();

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
        .send()
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

#[test]
fn test_put_object_website_redirect_round_trips_on_head_and_get() {
    s3_tests::run(async {
        if skip_without_external_endpoint() {
            return;
        }

        let bucket = setup_bucket().await;
        let key = "put-redirect";
        let redirect = "/docs/landing.html";

        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key(key)
            .website_redirect_location(redirect)
            .body(ByteStream::from_static(b"redirect-body"))
            .send()
            .await
            .unwrap();

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(head.website_redirect_location(), Some(redirect));

        let get = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
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
        if skip_without_external_endpoint() {
            return;
        }

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
            .send()
            .await;
        assert!(head.is_err(), "invalid redirect should not create object");

        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_put_object_website_redirect_unsupported_scheme_rejected() {
    s3_tests::run(async {
        if skip_without_external_endpoint() {
            return;
        }

        let bucket = setup_bucket().await;
        let key = "invalid-scheme";

        let result = CTX
            .client()
            .put_object()
            .bucket(&bucket)
            .key(key)
            .website_redirect_location("ftp://example.com/out")
            .body(ByteStream::from_static(b"invalid-scheme-body"))
            .send()
            .await;

        assert_s3_err_code(&result, "InvalidRedirectLocation");

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;
        assert!(head.is_err(), "invalid redirect should not create object");

        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_copy_object_does_not_copy_redirect_without_explicit_header() {
    s3_tests::run(async {
        if skip_without_external_endpoint() {
            return;
        }

        let bucket = setup_bucket().await;
        let src_key = "copy-src";
        let dst_key = "copy-dst";
        let redirect = "/docs/source.html";

        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .website_redirect_location(redirect)
            .body(ByteStream::from_static(b"copy-body"))
            .send()
            .await
            .unwrap();

        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key(dst_key)
            .copy_source(format!("{bucket}/{src_key}"))
            .metadata_directive(MetadataDirective::Copy)
            .send()
            .await
            .unwrap();

        let src_head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(src_key)
            .send()
            .await
            .unwrap();
        assert_eq!(src_head.website_redirect_location(), Some(redirect));

        let dst_head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap();
        assert_eq!(dst_head.website_redirect_location(), None);

        let _ = CTX
            .client()
            .delete_object()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await;
        cleanup_object_and_bucket(&bucket, src_key).await;
    });
}

#[test]
fn test_copy_object_explicit_redirect_persists_on_destination() {
    s3_tests::run(async {
        if skip_without_external_endpoint() {
            return;
        }

        let bucket = setup_bucket().await;
        let src_key = "copy-src-explicit";
        let dst_key = "copy-dst-explicit";

        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from_static(b"copy-explicit-body"))
            .send()
            .await
            .unwrap();

        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key(dst_key)
            .copy_source(format!("{bucket}/{src_key}"))
            .website_redirect_location("/docs/destination.html")
            .send()
            .await
            .unwrap();

        let dst_head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(dst_key)
            .send()
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
            .send()
            .await;
        cleanup_object_and_bucket(&bucket, src_key).await;
    });
}

#[test]
fn test_copy_object_same_key_redirect_only_change_is_allowed() {
    s3_tests::run(async {
        if skip_without_external_endpoint() {
            return;
        }

        let bucket = setup_bucket().await;
        let key = "copy-same-key-redirect";

        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"same-key-body"))
            .send()
            .await
            .unwrap();

        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key(key)
            .copy_source(format!("{bucket}/{key}"))
            .website_redirect_location("/docs/changed-by-copy.html")
            .send()
            .await
            .unwrap();

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
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
        if skip_without_external_endpoint() {
            return;
        }

        let bucket = setup_bucket().await;
        let key = "copy-same-key-no-change";
        let redirect = "/docs/original.html";

        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key(key)
            .website_redirect_location(redirect)
            .body(ByteStream::from_static(b"same-key-no-change-body"))
            .send()
            .await
            .unwrap();

        // AWS accepts the request when the redirect header is explicitly
        // present, even if the value matches the stored redirect exactly.
        CTX.client()
            .copy_object()
            .bucket(&bucket)
            .key(key)
            .copy_source(format!("{bucket}/{key}"))
            .website_redirect_location(redirect)
            .send()
            .await
            .unwrap();

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(head.website_redirect_location(), Some(redirect));

        cleanup_object_and_bucket(&bucket, key).await;
    });
}

#[test]
fn test_multipart_upload_redirect_persists_from_initiation() {
    s3_tests::run(async {
        if skip_without_external_endpoint() {
            return;
        }

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
            .send()
            .await
            .unwrap();
        assert_eq!(head.website_redirect_location(), Some(redirect));

        cleanup_object_and_bucket(&bucket, key).await;
    });
}

#[test]
fn test_multipart_upload_without_redirect_remains_absent() {
    s3_tests::run(async {
        if skip_without_external_endpoint() {
            return;
        }

        let bucket = setup_bucket().await;
        let key = "multipart-no-redirect";

        complete_single_part_multipart_upload_with_redirect(&bucket, key, None, b"multipart-body")
            .await;

        let head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(head.website_redirect_location(), None);

        cleanup_object_and_bucket(&bucket, key).await;
    });
}

#[test]
fn test_post_object_website_redirect_persists() {
    s3_tests::run(async {
        if skip_without_external_endpoint() {
            return;
        }

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
            .send()
            .await
            .unwrap();
        assert_eq!(head.website_redirect_location(), Some(redirect));

        cleanup_object_and_bucket(&bucket, key).await;
    });
}

#[test]
fn test_post_object_website_redirect_policy_missing_field_is_rejected() {
    s3_tests::run(async {
        if skip_without_external_endpoint() {
            return;
        }

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
            .send()
            .await;
        assert!(head.is_err(), "policy rejection should not create object");

        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_post_object_website_redirect_policy_mismatch_is_rejected() {
    s3_tests::run(async {
        if skip_without_external_endpoint() {
            return;
        }

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
            .send()
            .await;
        assert!(head.is_err(), "policy mismatch should not create object");

        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_versioned_objects_surface_redirect_metadata_per_version() {
    s3_tests::run(async {
        if skip_without_external_endpoint() {
            return;
        }

        let bucket = setup_versioned_bucket().await;
        let key = "versioned-redirect";

        let put_v1 = CTX
            .client()
            .put_object()
            .bucket(&bucket)
            .key(key)
            .website_redirect_location("/docs/v1.html")
            .body(ByteStream::from_static(b"version-one"))
            .send()
            .await
            .unwrap();
        let v1 = put_v1.version_id().unwrap().to_string();

        let put_v2 = CTX
            .client()
            .put_object()
            .bucket(&bucket)
            .key(key)
            .website_redirect_location("/docs/v2.html")
            .body(ByteStream::from_static(b"version-two"))
            .send()
            .await
            .unwrap();
        let v2 = put_v2.version_id().unwrap().to_string();

        let current_head = CTX
            .client()
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
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
            .send()
            .await
            .unwrap();
        assert_eq!(v1_head.website_redirect_location(), Some("/docs/v1.html"));

        let v2_get = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&v2)
            .send()
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
            .send()
            .await
            .unwrap();
        assert_eq!(v1_get.website_redirect_location(), Some("/docs/v1.html"));
        let v1_body = v1_get.body.collect().await.unwrap().into_bytes();
        assert_eq!(&v1_body[..], b"version-one");

        s3_tests::cleanup_versioned_bucket(CTX.client(), &bucket).await;
    });
}
