use aws_sdk_s3::primitives::{ByteStream, DateTime};
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
use s3_tests::{
    err_status, retrying_operation_aborted, send_signed_request, unique_bucket, RawResponse,
    SendRetryingOperationAborted, CTX,
};

fn assert_error_code(body: &str, code: &str) {
    let expected = format!("<Code>{code}</Code>");
    assert!(
        body.contains(&expected),
        "expected {expected} in body, got {body}"
    );
}

fn assert_header_not_implemented(response: &RawResponse, header: &str) {
    assert_eq!(
        response.status, 501,
        "unexpected status: {}",
        response.status
    );
    assert_error_code(&response.body, "NotImplemented");
    let expected = format!("<Header>{header}</Header>");
    assert!(
        response.body.contains(&expected),
        "expected {expected} in body, got {}",
        response.body
    );
}

fn assert_query_parameter_not_implemented(response: &RawResponse, query_parameter: &str) {
    assert_eq!(
        response.status, 501,
        "unexpected status: {}",
        response.status
    );
    assert_error_code(&response.body, "NotImplemented");
    let expected = format!("<QueryParameter>{query_parameter}</QueryParameter>");
    assert!(
        response.body.contains(&expected),
        "expected {expected} in body, got {}",
        response.body
    );
}

async fn create_bucket_in_test_region(bucket: &str) {
    let mut request = CTX.client().create_bucket().bucket(bucket);
    if CTX.region() != "us-east-1" {
        let config = CreateBucketConfiguration::builder()
            .location_constraint(BucketLocationConstraint::from(CTX.region()))
            .build();
        request = request.create_bucket_configuration(config);
    }
    request
        .send_retrying_operation_aborted("create directory feature test bucket")
        .await
        .unwrap();
}

async fn cleanup_bucket(bucket: &str, keys: &[&str]) {
    for key in keys {
        let _ =
            s3_tests::delete_object_retrying_operation_aborted(CTX.client(), bucket, *key).await;
    }
    let _ = CTX
        .client()
        .delete_bucket()
        .bucket(bucket)
        .send_retrying_operation_aborted("delete directory feature test bucket")
        .await;
}

async fn put_object(bucket: &str, key: &str, body: &'static [u8]) {
    retrying_operation_aborted("put directory feature object", || async move {
        CTX.client()
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(body))
            .send()
            .await
    })
    .await;
}

#[test]
fn test_put_object_write_offset_bytes_not_implemented_on_standard_bucket() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        create_bucket_in_test_region(&bucket).await;
        let key = "offset";
        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);

        let response =
            send_signed_request("PUT", &url, b"hello", [("x-amz-write-offset-bytes", "0")]);
        assert_header_not_implemented(&response, "x-amz-write-offset-bytes");

        let get_result = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;
        assert!(
            get_result.is_err(),
            "expected no object to be created when x-amz-write-offset-bytes is rejected"
        );

        cleanup_bucket(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_rename_source_not_implemented_on_standard_bucket() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        create_bucket_in_test_region(&bucket).await;
        let key = "dst";
        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);

        let response =
            send_signed_request("PUT", &url, b"hello", [("x-amz-rename-source", "/src")]);
        assert_header_not_implemented(&response, "x-amz-rename-source");

        let get_result = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;
        assert!(
            get_result.is_err(),
            "expected no object to be created when x-amz-rename-source is rejected"
        );

        cleanup_bucket(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_object_rename_object_query_not_implemented_on_standard_bucket() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        create_bucket_in_test_region(&bucket).await;
        let key = "dst";
        let url = format!("{}/{}/{}?renameObject", CTX.endpoint(), bucket, key);

        let response = send_signed_request("PUT", &url, b"hello", [] as [(&str, &str); 0]);
        assert_query_parameter_not_implemented(&response, "renameObject");

        let get_result = CTX
            .client()
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;
        assert!(
            get_result.is_err(),
            "expected no object to be created when ?renameObject is rejected"
        );

        cleanup_bucket(&bucket, &[key]).await;
    });
}

#[test]
fn test_get_bucket_session_query_is_ignored_on_standard_bucket() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        create_bucket_in_test_region(&bucket).await;

        let response = send_signed_request(
            "GET",
            &format!("{}/{}?session", CTX.endpoint(), bucket),
            b"",
            [("x-amz-create-session-mode", "ReadWrite")],
        );
        assert_eq!(
            response.status, 200,
            "unexpected status: {}",
            response.status
        );
        assert!(
            response.body.contains("<ListBucketResult"),
            "expected GET ?session on a standard bucket to behave like ListObjectsV1, got {}",
            response.body
        );

        cleanup_bucket(&bucket, &[]).await;
    });
}

#[test]
fn test_delete_object_if_match_last_modified_time_not_implemented_on_standard_bucket() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        create_bucket_in_test_region(&bucket).await;
        put_object(&bucket, "obj", b"hello").await;

        let result = CTX
            .client()
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .if_match_last_modified_time(DateTime::from_secs(0))
            .send()
            .await;
        assert_eq!(err_status(&result), 501);

        cleanup_bucket(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_delete_object_if_match_size_not_implemented_on_standard_bucket() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        create_bucket_in_test_region(&bucket).await;
        put_object(&bucket, "obj", b"hello").await;

        let result = CTX
            .client()
            .delete_object()
            .bucket(&bucket)
            .key("obj")
            .if_match_size(5)
            .send()
            .await;
        assert_eq!(err_status(&result), 501);

        cleanup_bucket(&bucket, &["obj"]).await;
    });
}
