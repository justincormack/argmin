//! Multipart upload integration tests.
//!
//! Tests the full multipart upload lifecycle through the S3 HTTP API:
//! CreateMultipartUpload, UploadPart, CompleteMultipartUpload,
//! AbortMultipartUpload, ListMultipartUploads, ListParts.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use aws_sdk_s3::error::{BoxError, ProvideErrorMetadata};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, EncodingType,
    VersioningConfiguration,
};
use aws_smithy_types::body::SdkBody;
use bytes::Bytes;
use http_body_1x::{Body, Frame, SizeHint};
use s3_tests::{
    assert_s3_err_code, copy_source_with_version, err_status, is_sdk_stream_disconnect_or_status,
    object_url, presign_url, raw_bucket, raw_object_query, send_signed_request,
    send_signed_request_with_credentials,
    shape::{
        assert_shape, error_response_headers, expected_error, shape, xml_response_headers,
        xml_tag_text,
    },
    unique_bucket, write_partial_request_and_disconnect, RawResponse, SendRetryingOperationAborted,
    SignedRequestCredentials, CTX,
};

const PART_SIZE: usize = 5 * 1024 * 1024; // 5 MB minimum part size
const SLOW_PART_SIZE: usize = 8 * 1024 * 1024;
const SLOW_PART_CHUNK_SIZE: usize = 64 * 1024;
const INTERRUPTED_PART_SIZE: usize = 128 * 1024;
const INTERRUPTED_PART_CHUNK_SIZE: usize = 64 * 1024;
const CONCURRENT_MULTIPART_OPERATION_ATTEMPTS: usize = 20;

fn assert_invalid_part_number_body_shape(
    body: &str,
    part_number_requested: u32,
    actual_part_count: u32,
) {
    assert!(
        body.contains("<Code>InvalidPartNumber</Code>"),
        "body: {body}"
    );
    assert!(
        body.contains("<Message>The requested partnumber is not satisfiable</Message>"),
        "body: {body}"
    );
    assert!(
        body.contains(&format!(
            "<PartNumberRequested>{part_number_requested}</PartNumberRequested>"
        )),
        "body: {body}"
    );
    assert!(
        body.contains(&format!(
            "<ActualPartCount>{actual_part_count}</ActualPartCount>"
        )),
        "body: {body}"
    );
    assert!(
        body.contains("<RequestId>"),
        "expected RequestId in body: {body}"
    );
    assert!(body.contains("<HostId>"), "expected HostId in body: {body}");
    assert!(
        !body.contains("<Resource>"),
        "expected no Resource element in body: {body}"
    );
}

struct SlowUploadPartBody {
    remaining: usize,
    first_frame_sent: Arc<AtomicBool>,
    delay: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl SlowUploadPartBody {
    fn new(first_frame_sent: Arc<AtomicBool>) -> Self {
        Self {
            remaining: SLOW_PART_SIZE,
            first_frame_sent,
            delay: None,
        }
    }
}

impl Body for SlowUploadPartBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if let Some(delay) = &mut self.delay {
            if delay.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            self.delay = None;
        }

        if self.remaining == 0 {
            return Poll::Ready(None);
        }

        let len = self.remaining.min(SLOW_PART_CHUNK_SIZE);
        self.remaining -= len;
        self.first_frame_sent.store(true, Ordering::SeqCst);
        if self.remaining != 0 {
            self.delay = Some(Box::pin(tokio::time::sleep(Duration::from_millis(20))));
        }
        Poll::Ready(Some(Ok(Frame::data(Bytes::from(vec![b'x'; len])))))
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.remaining as u64)
    }
}

async fn wait_for_slow_body_to_start(first_frame_sent: &AtomicBool) {
    for _ in 0..100 {
        if first_frame_sent.load(Ordering::SeqCst) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("slow UploadPart body did not start sending");
}

fn is_operation_aborted<E: ProvideErrorMetadata>(err: &aws_sdk_s3::error::SdkError<E>) -> bool {
    err.as_service_error().and_then(ProvideErrorMetadata::code) == Some("OperationAborted")
}

async fn put_bucket_versioning_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    status: BucketVersioningStatus,
) {
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(status.clone())
                .build(),
        )
        .send_retrying_operation_aborted("put bucket versioning during multipart setup")
        .await
        .unwrap();
}

async fn put_object_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
) -> aws_sdk_s3::operation::put_object::PutObjectOutput {
    for attempt in 0..CONCURRENT_MULTIPART_OPERATION_ATTEMPTS {
        match client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
        {
            Ok(output) => return output,
            Err(err)
                if is_operation_aborted(&err)
                    && attempt + 1 < CONCURRENT_MULTIPART_OPERATION_ATTEMPTS =>
            {
                tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
            }
            Err(err) => panic!("put object during multipart setup: {err:?}"),
        }
    }
    panic!("put object during multipart setup did not complete");
}

async fn delete_object_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> aws_sdk_s3::operation::delete_object::DeleteObjectOutput {
    client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send_retrying_operation_aborted("delete object during multipart setup")
        .await
        .unwrap()
}

async fn create_multipart_upload_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput {
    client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send_retrying_operation_aborted("create multipart upload during multipart setup")
        .await
        .unwrap()
}

async fn upload_part_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: Vec<u8>,
) -> aws_sdk_s3::operation::upload_part::UploadPartOutput {
    for attempt in 0..CONCURRENT_MULTIPART_OPERATION_ATTEMPTS {
        match client
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
                    && attempt + 1 < CONCURRENT_MULTIPART_OPERATION_ATTEMPTS =>
            {
                tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
            }
            Err(err) => panic!("upload part during multipart setup: {err:?}"),
        }
    }
    panic!("upload part during multipart setup did not complete");
}

async fn upload_part_result_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: Vec<u8>,
) -> Result<
    aws_sdk_s3::operation::upload_part::UploadPartOutput,
    aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::upload_part::UploadPartError>,
> {
    for attempt in 0..CONCURRENT_MULTIPART_OPERATION_ATTEMPTS {
        let result = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(body.clone()))
            .send()
            .await;
        match result {
            Ok(output) => return Ok(output),
            Err(err)
                if is_operation_aborted(&err)
                    && attempt + 1 < CONCURRENT_MULTIPART_OPERATION_ATTEMPTS =>
            {
                tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
            }
            Err(err) => return Err(err),
        }
    }
    panic!("upload part during multipart setup did not complete");
}

async fn upload_part_with_crc32_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: Vec<u8>,
) -> aws_sdk_s3::operation::upload_part::UploadPartOutput {
    use aws_sdk_s3::types::ChecksumAlgorithm;

    for attempt in 0..CONCURRENT_MULTIPART_OPERATION_ATTEMPTS {
        match client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(body.clone()))
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send()
            .await
        {
            Ok(output) => return output,
            Err(err)
                if is_operation_aborted(&err)
                    && attempt + 1 < CONCURRENT_MULTIPART_OPERATION_ATTEMPTS =>
            {
                tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
            }
            Err(err) => panic!("upload part with checksum during multipart setup: {err:?}"),
        }
    }
    panic!("upload part with checksum during multipart setup did not complete");
}

async fn upload_part_copy_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    copy_source: String,
) -> Result<
    aws_sdk_s3::operation::upload_part_copy::UploadPartCopyOutput,
    aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::upload_part_copy::UploadPartCopyError>,
> {
    client
        .upload_part_copy()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .copy_source(copy_source)
        .send_retrying_operation_aborted("upload part copy during multipart setup")
        .await
}

type CompleteMultipartResult = Result<
    aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput,
    aws_sdk_s3::error::SdkError<
        aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError,
    >,
>;

fn single_part_completion(etag: &str, part_number: i32) -> CompletedMultipartUpload {
    CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .e_tag(etag)
                .part_number(part_number)
                .build(),
        )
        .build()
}

async fn send_single_part_completion(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    etag: &str,
) -> CompleteMultipartResult {
    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(single_part_completion(etag, part_number))
        .send()
        .await
}

fn spawn_barrier_single_part_completion(
    client: aws_sdk_s3::Client,
    bucket: String,
    key: &'static str,
    upload_id: String,
    part_number: i32,
    etag: String,
    barrier: Arc<tokio::sync::Barrier>,
) -> tokio::task::JoinHandle<CompleteMultipartResult> {
    tokio::spawn(async move {
        barrier.wait().await;
        send_single_part_completion(&client, &bucket, key, &upload_id, part_number, &etag).await
    })
}

async fn create_single_part_upload(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    part_number: i32,
    body: &[u8],
) -> (String, String) {
    let upload_id = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send_retrying_operation_aborted("S3 operation during multipart test")
        .await
        .unwrap()
        .upload_id()
        .unwrap()
        .to_string();
    let part = upload_part_retrying_operation_aborted(
        client,
        bucket,
        key,
        &upload_id,
        part_number,
        body.to_vec(),
    )
    .await;
    (upload_id, part.e_tag().unwrap().to_string())
}

async fn complete_multipart_upload_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    etag: &str,
) {
    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(single_part_completion(etag, 1))
        .send_retrying_operation_aborted("complete multipart upload during multipart setup")
        .await
        .unwrap();
}

async fn abort_multipart_upload_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
) {
    client
        .abort_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .send_retrying_operation_aborted("abort multipart upload during multipart setup")
        .await
        .unwrap();
}

async fn assert_list_parts_no_such_upload(bucket: &str, key: &str, upload_id: &str) {
    let result = CTX
        .client()
        .list_parts()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .send_retrying_operation_aborted("list parts during multipart assertion")
        .await;
    assert_eq!(
        err_status(&result),
        404,
        "unexpected ListParts result: {result:?}"
    );
    assert_s3_err_code(&result, "NoSuchUpload");
}

async fn assert_multipart_parts_preserved(
    bucket: &str,
    key: &str,
    upload_id: &str,
    expected: &[(i32, i64, &str)],
) {
    let output = CTX
        .client()
        .list_parts()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .send_retrying_operation_aborted("list parts after rejected multipart completion")
        .await
        .unwrap();
    let actual: Vec<_> = output
        .parts()
        .iter()
        .map(|part| {
            (
                part.part_number().unwrap(),
                part.size().unwrap(),
                part.e_tag().unwrap(),
            )
        })
        .collect();
    assert_eq!(actual, expected);
}

async fn assert_object_contents_and_etag(
    bucket: &str,
    key: &str,
    expected_etag: &str,
    expected_body: &[u8],
) {
    let output = CTX
        .client()
        .get_object()
        .bucket(bucket)
        .key(key)
        .send_retrying_operation_aborted("get object after multipart completion attempt")
        .await
        .unwrap();
    assert_eq!(output.e_tag(), Some(expected_etag));
    assert_eq!(
        output.body.collect().await.unwrap().into_bytes().as_ref(),
        expected_body
    );
}

fn assert_complete_multipart_processing_error_shape(
    operation: &str,
    response: &RawResponse,
    error_status: u16,
    expected_body: String,
) {
    // AWS documents that CompleteMultipartUpload may commit a 200 response
    // before processing finishes and then embed an error in the body. The SDK
    // handles both forms automatically; raw shape tests must do so explicitly.
    // The local server completes processing before sending headers and uses the
    // ordinary error status.
    assert!(
        response.status == error_status || response.status == 200,
        "{operation}: expected status {error_status} or embedded-error status 200, got {}",
        response.status
    );
    assert_shape(
        operation,
        response,
        &shape()
            .status(response.status)
            .headers(error_response_headers())
            .body(expected_body),
    );
}

fn primary_credentials() -> SignedRequestCredentials<'static> {
    SignedRequestCredentials {
        access_key: CTX.access_key(),
        secret_key: CTX.secret_key(),
        region: CTX.region(),
        tls_ca_pem: CTX.tls_ca_pem(),
    }
}

fn alt_credentials() -> SignedRequestCredentials<'static> {
    SignedRequestCredentials {
        access_key: CTX.alt_access_key(),
        secret_key: CTX.alt_secret_key(),
        region: CTX.region(),
        tls_ca_pem: CTX.tls_ca_pem(),
    }
}

fn complete_multipart_upload_xml(etag: &str, part_number: u32) -> Vec<u8> {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<CompleteMultipartUpload>\
<Part><PartNumber>{part_number}</PartNumber><ETag>{etag}</ETag></Part>\
</CompleteMultipartUpload>"
    )
    .into_bytes()
}

fn assert_invalid_upload_id_no_such_upload(response: &RawResponse, invalid_upload_id: &str) {
    assert_eq!(
        response.status, 404,
        "unexpected response body: {}",
        response.body
    );
    assert!(
        response.body.contains("<Code>NoSuchUpload</Code>"),
        "unexpected response body: {}",
        response.body
    );
    assert!(
        response.body.contains(
            "<Message>The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.</Message>"
        ),
        "unexpected response body: {}",
        response.body
    );
    assert!(
        response
            .body
            .contains(&format!("<UploadId>{invalid_upload_id}</UploadId>")),
        "unexpected response body: {}",
        response.body
    );
}

fn assert_access_denied(response: &RawResponse) {
    assert_eq!(
        response.status, 403,
        "unexpected response body: {}",
        response.body
    );
    assert!(
        response.body.contains("<Code>AccessDenied</Code>"),
        "unexpected response body: {}",
        response.body
    );
}

fn assert_listing_invalid_argument(
    case: &str,
    response: &RawResponse,
    message: &str,
    argument_name: &str,
    argument_value: &str,
) {
    assert_shape(
        case,
        response,
        &shape().status(400).headers(error_response_headers()).body(
            expected_error::invalid_argument_with_value(message, argument_name, argument_value),
        ),
    );
}

fn assert_malformed_xml(response: &RawResponse) {
    assert_eq!(
        response.status, 400,
        "unexpected response body: {}",
        response.body
    );
    assert!(
        response.body.contains("<Code>MalformedXML</Code>"),
        "unexpected response body: {}",
        response.body
    );
    assert!(
        response.body.contains(
            "<Message>The XML you provided was not well-formed or did not validate against our published schema</Message>"
        ),
        "unexpected response body: {}",
        response.body
    );
}

async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();

    // AWS can keep failed or recently aborted multipart uploads visible briefly,
    // causing DeleteBucket to return OperationAborted or BucketNotEmpty.
    let mut last_cleanup_error = None;
    for _ in 0..30 {
        for key in keys {
            if let Err(err) = client
                .delete_object()
                .bucket(bucket)
                .key(*key)
                .send_retrying_operation_aborted("delete object during multipart cleanup")
                .await
            {
                let raw = format!("{err:?}");
                if !raw.contains("NoSuchBucket") && !raw.contains("NoSuchKey") {
                    last_cleanup_error = Some(format!("delete_object {key:?}: {raw}"));
                }
            }
        }

        let uploads = client
            .list_multipart_uploads()
            .bucket(bucket)
            .send_retrying_operation_aborted("list multipart uploads during multipart cleanup")
            .await
            .unwrap();
        for upload in uploads.uploads() {
            let _ = client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(upload.key().unwrap())
                .upload_id(upload.upload_id().unwrap())
                .send_retrying_operation_aborted("abort multipart upload during multipart cleanup")
                .await;
        }

        match client
            .delete_bucket()
            .bucket(bucket)
            .send_retrying_operation_aborted("delete bucket during multipart cleanup")
            .await
        {
            Ok(_) => return,
            Err(err) => {
                let raw = format!("{err:?}");
                if raw.contains("NoSuchBucket") {
                    return;
                }
                if raw.contains("OperationAborted") || raw.contains("BucketNotEmpty") {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    continue;
                }
                panic!("delete_bucket failed unexpectedly: {raw}");
            }
        }
    }

    let result = client
        .delete_bucket()
        .bucket(bucket)
        .send_retrying_operation_aborted("final delete bucket during multipart cleanup")
        .await;
    if let Err(err) = result {
        let raw = format!("{err:?}");
        if raw.contains("NoSuchBucket") {
            return;
        }
        panic!(
            "delete_bucket did not converge: {raw}; last cleanup error: {}",
            last_cleanup_error.as_deref().unwrap_or("none")
        );
    }
}

fn expected_raw_list_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\u{0001}'..='\u{0008}' | '\u{000B}' | '\u{000C}' | '\u{000E}'..='\u{001F}' => {
                escaped.push_str(&format!("&#x{:x};", ch as u32));
            }
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn expected_url_list_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(byte as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn query_encode_value(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

/// Helper: create multipart upload, upload parts, complete, return (etag, version_id).
async fn do_multipart_upload(bucket: &str, key: &str, parts_data: &[Vec<u8>]) -> String {
    let client = CTX.client();

    let create = create_multipart_upload_retrying_operation_aborted(client, bucket, key).await;
    let upload_id = create.upload_id().unwrap();

    let mut completed_parts = Vec::new();
    for (i, data) in parts_data.iter().enumerate() {
        let part_number = (i + 1) as i32;
        let resp = upload_part_retrying_operation_aborted(
            client,
            bucket,
            key,
            upload_id,
            part_number,
            data.clone(),
        )
        .await;
        completed_parts.push(
            CompletedPart::builder()
                .e_tag(resp.e_tag().unwrap())
                .part_number(part_number)
                .build(),
        );
    }

    let complete = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build(),
        )
        .send_retrying_operation_aborted("complete multipart upload")
        .await
        .unwrap();
    complete.e_tag().unwrap().to_string()
}

async fn complete_single_part_multipart_upload(bucket: &str, key: &str, body: &[u8]) -> String {
    let client = CTX.client();
    let create = create_multipart_upload_retrying_operation_aborted(client, bucket, key).await;
    let upload_id = create.upload_id().unwrap().to_string();

    let part =
        upload_part_retrying_operation_aborted(client, bucket, key, &upload_id, 1, body.to_vec())
            .await;

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .e_tag(part.e_tag().unwrap())
                        .part_number(1)
                        .build(),
                )
                .build(),
        )
        .send_retrying_operation_aborted("complete single-part multipart upload")
        .await
        .unwrap();

    upload_id
}

#[test]
fn test_abort_multipart_upload_with_completed_part_hides_upload_for_list_parts() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "abort-completed-part";
        let upload_id = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap()
            .upload_id()
            .unwrap()
            .to_string();

        upload_part_retrying_operation_aborted(
            client,
            &bucket,
            key,
            &upload_id,
            1,
            vec![b'a'; PART_SIZE],
        )
        .await;

        let before_abort = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(before_abort.parts().len(), 1);
        assert_eq!(before_abort.parts()[0].part_number(), Some(1));

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        assert_list_parts_no_such_upload(&bucket, key, &upload_id).await;
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_abort_multipart_upload_racing_started_upload_part_returns_success_or_no_such_upload() {
    s3_tests::run(async {
        let client = CTX.client().clone();
        let bucket = setup_bucket().await;
        let key = "abort-races-started-part";
        let upload_id = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap()
            .upload_id()
            .unwrap()
            .to_string();

        let first_frame_sent = Arc::new(AtomicBool::new(false));
        let body = ByteStream::new(SdkBody::from_body_1_x(SlowUploadPartBody::new(Arc::clone(
            &first_frame_sent,
        ))));
        let upload_bucket = bucket.clone();
        let upload_id_for_task = upload_id.clone();
        let upload_task = tokio::spawn(async move {
            client
                .upload_part()
                .bucket(upload_bucket)
                .key(key)
                .upload_id(upload_id_for_task)
                .part_number(1)
                .body(body)
                .send()
                .await
        });

        wait_for_slow_body_to_start(&first_frame_sent).await;

        CTX.client()
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        assert_list_parts_no_such_upload(&bucket, key, &upload_id).await;

        let upload_part = upload_task
            .await
            .expect("slow UploadPart task should not panic");
        let uploaded_part = match &upload_part {
            Ok(output) => Some(output),
            Err(err) => {
                if is_sdk_stream_disconnect_or_status(err, 404) {
                    None
                } else {
                    assert_eq!(
                        err_status(&upload_part),
                        404,
                        "unexpected raced UploadPart error: {err:?}"
                    );
                    assert_s3_err_code(&upload_part, "NoSuchUpload");
                    None
                }
            }
        };
        if let Some(uploaded_part) = uploaded_part {
            assert!(
                uploaded_part.e_tag().is_some(),
                "successful UploadPart should return an ETag"
            );
        }

        let list_after_race = CTX
            .client()
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        if let Ok(parts) = list_after_race {
            assert_eq!(parts.parts().len(), 1);
            assert_eq!(parts.parts()[0].part_number(), Some(1));
            CTX.client()
                .abort_multipart_upload()
                .bucket(&bucket)
                .key(key)
                .upload_id(&upload_id)
                .send_retrying_operation_aborted("S3 operation during multipart test")
                .await
                .unwrap();
        }

        assert_list_parts_no_such_upload(&bucket, key, &upload_id).await;
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_abort_multipart_upload_racing_started_second_part_returns_success_or_no_such_upload() {
    s3_tests::run(async {
        let client = CTX.client().clone();
        let bucket = setup_bucket().await;
        let key = "abort-races-started-second-part";
        let upload_id = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap()
            .upload_id()
            .unwrap()
            .to_string();

        upload_part_retrying_operation_aborted(
            &client,
            &bucket,
            key,
            &upload_id,
            1,
            vec![b'a'; PART_SIZE],
        )
        .await;

        let before_race = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(before_race.parts().len(), 1);
        assert_eq!(before_race.parts()[0].part_number(), Some(1));

        let first_frame_sent = Arc::new(AtomicBool::new(false));
        let body = ByteStream::new(SdkBody::from_body_1_x(SlowUploadPartBody::new(Arc::clone(
            &first_frame_sent,
        ))));
        let upload_bucket = bucket.clone();
        let upload_id_for_task = upload_id.clone();
        let upload_task = tokio::spawn(async move {
            client
                .upload_part()
                .bucket(upload_bucket)
                .key(key)
                .upload_id(upload_id_for_task)
                .part_number(2)
                .body(body)
                .send()
                .await
        });

        wait_for_slow_body_to_start(&first_frame_sent).await;

        CTX.client()
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        assert_list_parts_no_such_upload(&bucket, key, &upload_id).await;

        let upload_part = upload_task
            .await
            .expect("slow UploadPart task should not panic");
        let uploaded_part = match &upload_part {
            Ok(output) => Some(output),
            Err(err) => {
                if is_sdk_stream_disconnect_or_status(err, 404) {
                    None
                } else {
                    assert_eq!(
                        err_status(&upload_part),
                        404,
                        "unexpected raced UploadPart error after established part: {err:?}"
                    );
                    assert_s3_err_code(&upload_part, "NoSuchUpload");
                    None
                }
            }
        };
        if let Some(uploaded_part) = uploaded_part {
            assert!(
                uploaded_part.e_tag().is_some(),
                "successful UploadPart should return an ETag"
            );
        }

        let list_after_race = CTX
            .client()
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        if let Ok(parts) = list_after_race {
            assert!(
                !parts.parts().is_empty(),
                "remaining parts should be visible before the repeated abort"
            );
            CTX.client()
                .abort_multipart_upload()
                .bucket(&bucket)
                .key(key)
                .upload_id(&upload_id)
                .send_retrying_operation_aborted("S3 operation during multipart test")
                .await
                .unwrap();
        }

        assert_list_parts_no_such_upload(&bucket, key, &upload_id).await;
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_create_multipart_upload_rejects_system_metadata_over_limit() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let result = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("multipart-system-metadata-too-large")
            .content_disposition("d".repeat(3000))
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;

        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MetadataTooLarge");

        cleanup(&bucket, &[]).await;
    });
}

// ── Basic lifecycle ─────────────────────────────────────────────────

#[test]
fn test_multipart_upload_basic() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-basic";

        let part1 = vec![b'a'; PART_SIZE];
        let part2 = vec![b'b'; 1024]; // last part can be < 5MB

        let etag = do_multipart_upload(&bucket, key, &[part1.clone(), part2.clone()]).await;
        assert!(!etag.is_empty());

        // Verify the object is readable and has correct content
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(data.len(), PART_SIZE + 1024);
        assert!(data[..PART_SIZE].iter().all(|&b| b == b'a'));
        assert!(data[PART_SIZE..].iter().all(|&b| b == b'b'));

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_multipart_upload_single_part() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-single";

        // Single part (last part exempt from min size)
        let part = vec![b'x'; 256];
        do_multipart_upload(&bucket, key, std::slice::from_ref(&part)).await;

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], &part[..]);

        cleanup(&bucket, &[key]).await;
    });
}

// ── Abort ───────────────────────────────────────────────────────────

#[test]
fn test_multipart_upload_abort() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-abort";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload a part
        upload_part_retrying_operation_aborted(client, &bucket, key, upload_id, 1, vec![0u8; 1024])
            .await;

        // Abort the upload
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        // The object should not exist
        let result = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        assert!(result.is_err());

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_abort_multipart_upload_after_complete_succeeds() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-abort-after-complete";
        let upload_id = complete_single_part_multipart_upload(&bucket, key, b"hello, world!").await;

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"hello, world!");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_abort_multipart_upload_after_complete_wrong_upload_id_fails() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-abort-after-complete-wrong-upload-id";

        let _upload_id =
            complete_single_part_multipart_upload(&bucket, key, b"hello, world!").await;

        let result = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id("definitely-wrong-upload-id")
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        assert_s3_err_code(&result, "NoSuchUpload");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_abort_multipart_upload_invalid_present_upload_id_overlong_message() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-invalid-upload-id-message";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let valid_upload_id = create.upload_id().unwrap().to_string();
        let invalid_upload_id = "a".repeat(1025);
        let url = object_url(
            CTX.endpoint(),
            &bucket,
            key,
            Some(&format!("uploadId={invalid_upload_id}")),
        );

        let response = send_signed_request_with_credentials(
            "DELETE",
            &url,
            b"",
            std::iter::empty::<(&str, &str)>(),
            primary_credentials(),
        );
        assert_invalid_upload_id_no_such_upload(&response, &invalid_upload_id);

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&valid_upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_upload_part_invalid_present_upload_id_overlong_message() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "multipart-upload-part-invalid-upload-id-message";
        let invalid_upload_id = "a".repeat(1025);
        let url = object_url(
            CTX.endpoint(),
            &bucket,
            key,
            Some(&format!("partNumber=1&uploadId={invalid_upload_id}")),
        );

        let response = send_signed_request_with_credentials(
            "PUT",
            &url,
            b"x",
            std::iter::empty::<(&str, &str)>(),
            primary_credentials(),
        );
        assert_invalid_upload_id_no_such_upload(&response, &invalid_upload_id);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_upload_part_number_wire_matrix() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "upload-part-number-wire-matrix";
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("create multipart upload for part-number matrix")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let missing_url = object_url(
            CTX.endpoint(),
            &bucket,
            key,
            Some(&format!("uploadId={upload_id}")),
        );
        let missing_response = send_signed_request_with_credentials(
            "PUT",
            &missing_url,
            b"must not become an object",
            std::iter::empty::<(&str, &str)>(),
            primary_credentials(),
        );
        assert_shape(
            "UploadPart missing partNumber",
            &missing_response,
            &shape()
                .status(405)
                .headers(error_response_headers())
                .header("allow", "DELETE, POST, GET")
                .body(expected_error::put_multipart_upload_method_not_allowed()),
        );

        const INVALID_PART_NUMBER_MESSAGE: &str =
            "Part number must be an integer between 1 and 10000, inclusive";
        for (case, value, query) in [
            ("empty", "", format!("partNumber=&uploadId={upload_id}")),
            (
                "nonnumeric",
                "abc",
                format!("partNumber=abc&uploadId={upload_id}"),
            ),
            (
                "negative",
                "-1",
                format!("partNumber=-1&uploadId={upload_id}"),
            ),
            ("zero", "0", format!("partNumber=0&uploadId={upload_id}")),
            (
                "above maximum",
                "10001",
                format!("partNumber=10001&uploadId={upload_id}"),
            ),
            (
                "u32 overflow",
                "4294967296",
                format!("partNumber=4294967296&uploadId={upload_id}"),
            ),
            (
                "decimal overflow",
                "999999999999999999999999",
                format!("partNumber=999999999999999999999999&uploadId={upload_id}"),
            ),
            (
                "invalid first duplicate",
                "0",
                format!("partNumber=0&partNumber=4&uploadId={upload_id}"),
            ),
        ] {
            let url = object_url(CTX.endpoint(), &bucket, key, Some(&query));
            let response = send_signed_request_with_credentials(
                "PUT",
                &url,
                b"must not become a part",
                std::iter::empty::<(&str, &str)>(),
                primary_credentials(),
            );
            assert_shape(
                case,
                &response,
                &shape().status(400).headers(error_response_headers()).body(
                    expected_error::invalid_argument_with_value(
                        INVALID_PART_NUMBER_MESSAGE,
                        "partNumber",
                        value,
                    ),
                ),
            );
        }

        let accepted_cases = [
            (
                1,
                format!("partNumber=%2B1&uploadId={upload_id}"),
                b"explicit plus".as_slice(),
            ),
            (
                2,
                format!("partNumber=2&partNumber=4&uploadId={upload_id}"),
                b"first duplicate two".as_slice(),
            ),
            (
                3,
                format!("partNumber=3&partNumber=0&uploadId={upload_id}"),
                b"first duplicate three".as_slice(),
            ),
            (
                10_000,
                format!("partNumber=10000&uploadId={upload_id}"),
                b"maximum".as_slice(),
            ),
        ];
        for (_, query, body) in &accepted_cases {
            let url = object_url(CTX.endpoint(), &bucket, key, Some(query));
            let response = send_signed_request_with_credentials(
                "PUT",
                &url,
                body,
                std::iter::empty::<(&str, &str)>(),
                primary_credentials(),
            );
            assert_eq!(response.status, 200, "unexpected response: {response:?}");
            assert!(
                response.body.is_empty(),
                "unexpected response: {response:?}"
            );
            assert!(
                response.headers.iter().any(|(name, _)| name == "etag"),
                "UploadPart response lacks ETag: {response:?}"
            );
        }

        let listed = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("list parts after part-number matrix")
            .await
            .unwrap();
        let stored_parts: Vec<_> = listed
            .parts()
            .iter()
            .map(|part| (part.part_number().unwrap(), part.size().unwrap()))
            .collect();
        let expected_parts: Vec<_> = accepted_cases
            .iter()
            .map(|(part_number, _, body)| (*part_number, body.len() as i64))
            .collect();
        assert_eq!(stored_parts, expected_parts);

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("head object after UploadPart part-number matrix")
            .await;
        assert_eq!(
            err_status(&head),
            404,
            "UploadPart matrix unexpectedly published an object: {head:?}"
        );

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send_retrying_operation_aborted("abort multipart upload after part-number matrix")
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_complete_multipart_upload_invalid_present_upload_id_overlong_message() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "multipart-complete-invalid-upload-id-message";
        let invalid_upload_id = "a".repeat(1025);
        let url = object_url(
            CTX.endpoint(),
            &bucket,
            key,
            Some(&format!("uploadId={invalid_upload_id}")),
        );
        let body = complete_multipart_upload_xml("\"abc\"", 1);

        let response = send_signed_request_with_credentials(
            "POST",
            &url,
            &body,
            [("content-type", "application/xml")],
            primary_credentials(),
        );
        assert_invalid_upload_id_no_such_upload(&response, &invalid_upload_id);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_parts_invalid_present_upload_id_overlong_message() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "multipart-list-parts-invalid-upload-id-message";
        let invalid_upload_id = "a".repeat(1025);
        let url = object_url(
            CTX.endpoint(),
            &bucket,
            key,
            Some(&format!("uploadId={invalid_upload_id}")),
        );

        let response = send_signed_request_with_credentials(
            "GET",
            &url,
            b"",
            std::iter::empty::<(&str, &str)>(),
            primary_credentials(),
        );
        assert_invalid_upload_id_no_such_upload(&response, &invalid_upload_id);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_multipart_upload_id_wire_matrix() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let missing_key = "upload-part-missing-upload-id";
        let missing_url = object_url(CTX.endpoint(), &bucket, missing_key, Some("partNumber=1"));
        let missing_response = send_signed_request_with_credentials(
            "PUT",
            &missing_url,
            b"must not become an object",
            std::iter::empty::<(&str, &str)>(),
            primary_credentials(),
        );
        assert_shape(
            "UploadPart missing uploadId",
            &missing_response,
            &shape().status(400).headers(error_response_headers()).body(
                expected_error::invalid_argument_with_value(
                    "This operation does not accept partNumber without uploadId",
                    "partNumber",
                    "partNumber",
                ),
            ),
        );

        for (operation, method, query_prefix, body, headers) in [
            ("UploadPart", "PUT", "partNumber=1&", b"x".as_slice(), None),
            (
                "CompleteMultipartUpload",
                "POST",
                "",
                b"<".as_slice(),
                Some(("content-type", "application/xml")),
            ),
            ("ListParts", "GET", "", b"".as_slice(), None),
            ("AbortMultipartUpload", "DELETE", "", b"".as_slice(), None),
        ] {
            for case in [
                "empty",
                "encoded-valid",
                "wrong-key",
                "valid-first-duplicate",
                "invalid-first-duplicate",
            ] {
                let key = format!("upload-id-wire-{operation}-{case}");
                let create = client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key(&key)
                    .send_retrying_operation_aborted("create upload for upload ID wire matrix")
                    .await
                    .unwrap();
                let upload_id = create.upload_id().unwrap();
                let encoded_upload_id =
                    format!("%{:02X}{}", upload_id.as_bytes()[0], &upload_id[1..]);
                let mut invalid_upload_id = upload_id.to_string();
                let replacement = if invalid_upload_id.ends_with('A') {
                    'B'
                } else {
                    'A'
                };
                invalid_upload_id.pop();
                invalid_upload_id.push(replacement);

                let query = match case {
                    "empty" => format!("{query_prefix}uploadId="),
                    "encoded-valid" => format!("{query_prefix}uploadId={encoded_upload_id}"),
                    "wrong-key" => format!("{query_prefix}uploadId={upload_id}"),
                    "valid-first-duplicate" => {
                        format!("{query_prefix}uploadId={upload_id}&uploadId={invalid_upload_id}")
                    }
                    "invalid-first-duplicate" => {
                        format!("{query_prefix}uploadId={invalid_upload_id}&uploadId={upload_id}")
                    }
                    _ => unreachable!(),
                };
                let request_key = if case == "wrong-key" {
                    format!("{key}-wrong")
                } else {
                    key.clone()
                };
                let url = object_url(CTX.endpoint(), &bucket, &request_key, Some(&query));
                let response = send_signed_request_with_credentials(
                    method,
                    &url,
                    body,
                    headers,
                    primary_credentials(),
                );

                let selects_active_upload =
                    matches!(case, "encoded-valid" | "valid-first-duplicate");
                if selects_active_upload {
                    match operation {
                        "UploadPart" => {
                            assert_eq!(response.status, 200, "{operation} {case}: {response:?}");
                            assert!(response.body.is_empty(), "{operation} {case}: {response:?}");
                            assert!(
                                response.headers.iter().any(|(name, _)| name == "etag"),
                                "{operation} {case}: {response:?}"
                            );
                        }
                        "CompleteMultipartUpload" => {
                            assert_shape(
                                &format!("{operation} {case}"),
                                &response,
                                &shape()
                                    .status(400)
                                    .headers(error_response_headers())
                                    .body(expected_error::malformed_xml_no_decl()),
                            );
                        }
                        "ListParts" => {
                            assert_eq!(response.status, 200, "{operation} {case}: {response:?}");
                            assert!(
                                response
                                    .body
                                    .contains(&format!("<UploadId>{upload_id}</UploadId>")),
                                "{operation} {case}: {response:?}"
                            );
                            assert!(
                                response.body.contains(&format!("<Key>{key}</Key>")),
                                "{operation} {case}: {response:?}"
                            );
                        }
                        "AbortMultipartUpload" => {
                            assert_eq!(response.status, 204, "{operation} {case}: {response:?}");
                            assert!(response.body.is_empty(), "{operation} {case}: {response:?}");
                        }
                        _ => unreachable!(),
                    }
                } else if operation == "UploadPart" && case == "empty" {
                    assert_shape(
                        "UploadPart empty uploadId",
                        &response,
                        &shape().status(400).headers(error_response_headers()).body(
                            expected_error::invalid_argument_with_value(
                                "This operation does not accept partNumber without uploadId",
                                "partNumber",
                                "partNumber",
                            ),
                        ),
                    );
                } else {
                    let echoed_upload_id = match case {
                        "empty" => "",
                        "wrong-key" => upload_id,
                        "invalid-first-duplicate" => invalid_upload_id.as_str(),
                        _ => unreachable!(),
                    };
                    let body = if operation == "CompleteMultipartUpload" {
                        expected_error::complete_multipart_no_such_upload(echoed_upload_id)
                    } else {
                        expected_error::no_such_upload(echoed_upload_id)
                    };
                    assert_shape(
                        &format!("{operation} {case}"),
                        &response,
                        &shape()
                            .status(404)
                            .headers(error_response_headers())
                            .body(body),
                    );
                }

                let listed = client
                    .list_parts()
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id(upload_id)
                    .send_retrying_operation_aborted("verify upload ID wire matrix state")
                    .await;
                if operation == "AbortMultipartUpload" && selects_active_upload {
                    assert_eq!(
                        err_status(&listed),
                        404,
                        "{operation} {case} left the selected upload active: {listed:?}"
                    );
                } else {
                    let listed = listed.unwrap_or_else(|err| {
                        panic!("{operation} {case} damaged the active upload: {err:?}")
                    });
                    let expected_part_count =
                        usize::from(operation == "UploadPart" && selects_active_upload);
                    assert_eq!(
                        listed.parts().len(),
                        expected_part_count,
                        "{operation} {case} stored unexpected parts: {:?}",
                        listed.parts()
                    );
                    if expected_part_count == 1 {
                        assert_eq!(listed.parts()[0].part_number(), Some(1));
                        assert_eq!(listed.parts()[0].size(), Some(1));
                    }
                }

                let head = client
                    .head_object()
                    .bucket(&bucket)
                    .key(&key)
                    .send_retrying_operation_aborted("verify upload ID matrix non-publication")
                    .await;
                assert_eq!(
                    err_status(&head),
                    404,
                    "{operation} {case} unexpectedly published an object: {head:?}"
                );
            }
        }

        let missing_head = client
            .head_object()
            .bucket(&bucket)
            .key(missing_key)
            .send_retrying_operation_aborted("verify missing upload ID non-publication")
            .await;
        assert_eq!(err_status(&missing_head), 404, "{missing_head:?}");

        cleanup(&bucket, &[missing_key]).await;
    });
}

#[test]
fn test_multipart_upload_id_authorization_precedence() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-upload-id-auth-precedence";
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("create multipart upload for precedence test")
            .await
            .unwrap();
        let valid_upload_id = create.upload_id().unwrap().to_string();
        let invalid_upload_id = "a".repeat(1025);
        let complete_body = complete_multipart_upload_xml("\"abc\"", 1);

        for (operation, method, valid_query, invalid_query, body, headers) in [
            (
                "UploadPart",
                "PUT",
                format!("partNumber=1&uploadId={valid_upload_id}"),
                format!("partNumber=1&uploadId={invalid_upload_id}"),
                b"x".as_slice(),
                None,
            ),
            (
                "CompleteMultipartUpload",
                "POST",
                format!("uploadId={valid_upload_id}"),
                format!("uploadId={invalid_upload_id}"),
                complete_body.as_slice(),
                Some(("content-type", "application/xml")),
            ),
            (
                "ListParts",
                "GET",
                format!("uploadId={valid_upload_id}"),
                format!("uploadId={invalid_upload_id}"),
                b"".as_slice(),
                None,
            ),
            (
                "AbortMultipartUpload",
                "DELETE",
                format!("uploadId={valid_upload_id}"),
                format!("uploadId={invalid_upload_id}"),
                b"".as_slice(),
                None,
            ),
        ] {
            let valid_url = object_url(CTX.endpoint(), &bucket, key, Some(&valid_query));
            let valid_response = send_signed_request_with_credentials(
                method,
                &valid_url,
                body,
                headers,
                alt_credentials(),
            );
            assert_access_denied(&valid_response);

            let invalid_url = object_url(CTX.endpoint(), &bucket, key, Some(&invalid_query));
            let invalid_response = send_signed_request_with_credentials(
                method,
                &invalid_url,
                body,
                headers,
                alt_credentials(),
            );
            assert_invalid_upload_id_no_such_upload(&invalid_response, &invalid_upload_id);

            assert_ne!(
                valid_response.status, invalid_response.status,
                "{operation} did not distinguish authorization from upload-ID validation"
            );
        }

        let parts = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&valid_upload_id)
            .send_retrying_operation_aborted(
                "list parts after denied multipart authorization requests",
            )
            .await
            .unwrap();
        assert!(
            parts.parts().is_empty(),
            "denied UploadPart request unexpectedly stored parts: {:?}",
            parts.parts()
        );

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(valid_upload_id)
            .send_retrying_operation_aborted("abort multipart upload after precedence test")
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_upload_part_validation_authorization_precedence() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let source_key = "part-validation-precedence-source";
        let canary_key = "part-validation-precedence-canary";
        let target_key = "part-validation-precedence-target";
        put_object_retrying_operation_aborted(client, &bucket, source_key, b"copy source".to_vec())
            .await;

        let canary =
            create_multipart_upload_retrying_operation_aborted(client, &bucket, canary_key).await;
        let canary_upload_id = canary.upload_id().unwrap();
        let canary_copy = raw_multipart_query(
            "PUT",
            &bucket,
            canary_key,
            &format!("partNumber=1&uploadId={canary_upload_id}"),
            b"",
            &[("x-amz-copy-source", &format!("{bucket}/{source_key}"))],
        );
        assert_eq!(canary_copy.status, 200, "copy canary: {canary_copy:?}");
        assert_multipart_parts_preserved(
            &bucket,
            canary_key,
            canary_upload_id,
            &[(
                1,
                b"copy source".len() as i64,
                xml_tag_text(&canary_copy.body, "ETag").unwrap(),
            )],
        )
        .await;
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(canary_key)
            .upload_id(canary_upload_id)
            .send_retrying_operation_aborted("abort UploadPartCopy precedence canary")
            .await
            .unwrap();

        let target =
            create_multipart_upload_retrying_operation_aborted(client, &bucket, target_key).await;
        let active_upload_id = target.upload_id().unwrap();
        let invalid_upload_id = "a".repeat(1025);
        let body = b"must not become a part";
        let bad_md5 = "AAAAAAAAAAAAAAAAAAAAAA==";
        let copy_source = format!("{bucket}/{source_key}");

        for (name, part_number, upload_id, headers, credentials) in [
            (
                "UploadPart invalid number active authorized",
                "0",
                active_upload_id,
                Vec::new(),
                primary_credentials(),
            ),
            (
                "UploadPart invalid number invalid ID authorized",
                "0",
                invalid_upload_id.as_str(),
                Vec::new(),
                primary_credentials(),
            ),
            (
                "UploadPart nonnumeric number invalid ID authorized",
                "abc",
                invalid_upload_id.as_str(),
                Vec::new(),
                primary_credentials(),
            ),
            (
                "UploadPart invalid number active unauthorized",
                "0",
                active_upload_id,
                Vec::new(),
                alt_credentials(),
            ),
            (
                "UploadPart invalid number invalid ID unauthorized",
                "0",
                invalid_upload_id.as_str(),
                Vec::new(),
                alt_credentials(),
            ),
            (
                "UploadPart bad checksum active authorized",
                "1",
                active_upload_id,
                vec![("content-md5", bad_md5)],
                primary_credentials(),
            ),
            (
                "UploadPart bad checksum invalid ID authorized",
                "1",
                invalid_upload_id.as_str(),
                vec![("content-md5", bad_md5)],
                primary_credentials(),
            ),
            (
                "UploadPart malformed Content-MD5 invalid ID authorized",
                "1",
                invalid_upload_id.as_str(),
                vec![("content-md5", "bad")],
                primary_credentials(),
            ),
            (
                "UploadPart malformed SHA256 checksum invalid ID authorized",
                "1",
                invalid_upload_id.as_str(),
                vec![("x-amz-checksum-sha256", "bad")],
                primary_credentials(),
            ),
            (
                "UploadPart incomplete SSE-C headers invalid ID authorized",
                "1",
                invalid_upload_id.as_str(),
                vec![("x-amz-server-side-encryption-customer-algorithm", "AES256")],
                primary_credentials(),
            ),
            (
                "UploadPart bad checksum active unauthorized",
                "1",
                active_upload_id,
                vec![("content-md5", bad_md5)],
                alt_credentials(),
            ),
            (
                "UploadPart bad checksum invalid ID unauthorized",
                "1",
                invalid_upload_id.as_str(),
                vec![("content-md5", bad_md5)],
                alt_credentials(),
            ),
        ] {
            let query = format!(
                "partNumber={part_number}&uploadId={}",
                query_encode_value(upload_id)
            );
            let response = send_signed_request_with_credentials(
                "PUT",
                &object_url(CTX.endpoint(), &bucket, target_key, Some(&query)),
                body,
                headers,
                credentials,
            );
            let (expected_status, expected_code) = match name {
                name if name.contains("invalid ID") => (404, "NoSuchUpload"),
                "UploadPart bad checksum active authorized" => (400, "BadDigest"),
                "UploadPart bad checksum active unauthorized" => (403, "AccessDenied"),
                _ => (400, "InvalidArgument"),
            };
            assert_eq!(response.status, expected_status, "{name}: {response:?}");
            assert_eq!(
                xml_tag_text(&response.body, "Code"),
                Some(expected_code),
                "{name}: {response:?}"
            );
            if expected_code == "BadDigest" {
                assert_eq!(
                    xml_tag_text(&response.body, "Message"),
                    Some("The Content-MD5 you specified did not match what we received."),
                    "{name}: {response:?}"
                );
            }
        }

        for (name, part_number, upload_id, range, credentials) in [
            (
                "UploadPartCopy invalid number active authorized",
                "0",
                active_upload_id,
                None,
                primary_credentials(),
            ),
            (
                "UploadPartCopy invalid number invalid ID authorized",
                "0",
                invalid_upload_id.as_str(),
                None,
                primary_credentials(),
            ),
            (
                "UploadPartCopy nonnumeric number invalid ID authorized",
                "abc",
                invalid_upload_id.as_str(),
                None,
                primary_credentials(),
            ),
            (
                "UploadPartCopy invalid number active unauthorized",
                "0",
                active_upload_id,
                None,
                alt_credentials(),
            ),
            (
                "UploadPartCopy invalid number invalid ID unauthorized",
                "0",
                invalid_upload_id.as_str(),
                None,
                alt_credentials(),
            ),
            (
                "UploadPartCopy malformed range active authorized",
                "1",
                active_upload_id,
                Some("bytes=500-100"),
                primary_credentials(),
            ),
            (
                "UploadPartCopy malformed range invalid ID authorized",
                "1",
                invalid_upload_id.as_str(),
                Some("bytes=500-100"),
                primary_credentials(),
            ),
            (
                "UploadPartCopy malformed range active unauthorized",
                "1",
                active_upload_id,
                Some("bytes=500-100"),
                alt_credentials(),
            ),
            (
                "UploadPartCopy malformed range invalid ID unauthorized",
                "1",
                invalid_upload_id.as_str(),
                Some("bytes=500-100"),
                alt_credentials(),
            ),
            (
                "UploadPartCopy out-of-bounds range active authorized",
                "1",
                active_upload_id,
                Some("bytes=0-9999"),
                primary_credentials(),
            ),
            (
                "UploadPartCopy out-of-bounds range invalid ID authorized",
                "1",
                invalid_upload_id.as_str(),
                Some("bytes=0-9999"),
                primary_credentials(),
            ),
            (
                "UploadPartCopy out-of-bounds range active unauthorized",
                "1",
                active_upload_id,
                Some("bytes=0-9999"),
                alt_credentials(),
            ),
            (
                "UploadPartCopy out-of-bounds range invalid ID unauthorized",
                "1",
                invalid_upload_id.as_str(),
                Some("bytes=0-9999"),
                alt_credentials(),
            ),
        ] {
            let query = format!(
                "partNumber={part_number}&uploadId={}",
                query_encode_value(upload_id)
            );
            let mut headers = vec![("x-amz-copy-source", copy_source.as_str())];
            if let Some(range) = range {
                headers.push(("x-amz-copy-source-range", range));
            }
            let response = send_signed_request_with_credentials(
                "PUT",
                &object_url(CTX.endpoint(), &bucket, target_key, Some(&query)),
                b"",
                headers,
                credentials,
            );
            let (expected_status, expected_code) = match name {
                name if name.contains("invalid ID") => (404, "NoSuchUpload"),
                "UploadPartCopy out-of-bounds range active unauthorized" => (403, "AccessDenied"),
                _ => (400, "InvalidArgument"),
            };
            assert_eq!(response.status, expected_status, "{name}: {response:?}");
            assert_eq!(
                xml_tag_text(&response.body, "Code"),
                Some(expected_code),
                "{name}: {response:?}"
            );
            if name.contains("malformed range active") {
                assert_eq!(
                    xml_tag_text(&response.body, "Message"),
                    Some("The x-amz-copy-source-range value must be of the form bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy"),
                    "{name}: {response:?}"
                );
            }
        }

        assert_multipart_parts_preserved(&bucket, target_key, active_upload_id, &[]).await;
        let head = client
            .head_object()
            .bucket(&bucket)
            .key(target_key)
            .send_retrying_operation_aborted("check validation precedence non-publication")
            .await;
        assert_eq!(err_status(&head), 404, "unexpected target object: {head:?}");

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(target_key)
            .upload_id(active_upload_id)
            .send_retrying_operation_aborted("abort validation precedence target")
            .await
            .unwrap();
        cleanup(&bucket, &[source_key]).await;
    });
}

#[test]
fn test_failed_and_interrupted_part_replacement_preserves_prior_part() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let source_key = "failed-part-replacement-source";
        let target_key = "failed-part-replacement-target";
        let original_body = b"original multipart part";

        put_object_retrying_operation_aborted(
            client,
            &bucket,
            source_key,
            b"replacement copy source".to_vec(),
        )
        .await;
        let create =
            create_multipart_upload_retrying_operation_aborted(client, &bucket, target_key).await;
        let upload_id = create.upload_id().unwrap();
        let original_part = upload_part_retrying_operation_aborted(
            client,
            &bucket,
            target_key,
            upload_id,
            1,
            original_body.to_vec(),
        )
        .await;
        let original_etag = original_part.e_tag().unwrap();

        let checksum_failure = raw_multipart_query(
            "PUT",
            &bucket,
            target_key,
            &format!("partNumber=1&uploadId={upload_id}"),
            b"rejected checksum replacement",
            &[("content-md5", "AAAAAAAAAAAAAAAAAAAAAA==")],
        );
        assert_eq!(
            checksum_failure.status, 400,
            "failed UploadPart replacement: {checksum_failure:?}"
        );
        assert_eq!(
            xml_tag_text(&checksum_failure.body, "Code"),
            Some("BadDigest")
        );

        let interrupted_url = object_url(
            CTX.endpoint(),
            &bucket,
            target_key,
            Some(&format!(
                "partNumber=1&uploadId={}",
                query_encode_value(upload_id)
            )),
        );
        let presigned = presign_url(
            "PUT",
            &interrupted_url,
            Duration::from_secs(900),
            std::iter::empty::<(&str, &str)>(),
            None,
        );
        let presigned_headers: Vec<_> = presigned.headers().collect();
        assert!(
            !presigned_headers
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case("host")),
            "presigned transport headers must omit the Host header already represented by the URI"
        );
        let partial_body = vec![b'i'; INTERRUPTED_PART_CHUNK_SIZE];
        write_partial_request_and_disconnect(
            "PUT",
            presigned.uri(),
            INTERRUPTED_PART_SIZE,
            &partial_body,
            &presigned_headers,
            CTX.tls_ca_pem(),
        )
        .await
        .expect("write and flush partial UploadPart body before disconnecting");

        let copy_failure = raw_multipart_query(
            "PUT",
            &bucket,
            target_key,
            &format!("partNumber=1&uploadId={upload_id}"),
            b"",
            &[
                ("x-amz-copy-source", &format!("{bucket}/{source_key}")),
                ("x-amz-copy-source-if-match", "\"0000000000000000\""),
            ],
        );
        assert_eq!(
            copy_failure.status, 412,
            "failed UploadPartCopy replacement: {copy_failure:?}"
        );
        assert_eq!(
            xml_tag_text(&copy_failure.body, "Code"),
            Some("PreconditionFailed")
        );

        assert_multipart_parts_preserved(
            &bucket,
            target_key,
            upload_id,
            &[(1, original_body.len() as i64, original_etag)],
        )
        .await;

        complete_multipart_upload_retrying_operation_aborted(
            client,
            &bucket,
            target_key,
            upload_id,
            original_etag,
        )
        .await;
        let completed = client
            .get_object()
            .bucket(&bucket)
            .key(target_key)
            .send_retrying_operation_aborted("read object after failed part replacements")
            .await
            .unwrap();
        assert_eq!(
            completed
                .body
                .collect()
                .await
                .unwrap()
                .into_bytes()
                .as_ref(),
            original_body
        );

        cleanup(&bucket, &[source_key, target_key]).await;
    });
}

#[test]
fn test_complete_multipart_upload_xml_precedence() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "complete-multipart-xml-precedence";
        let original_body = b"object before malformed completion";
        let original =
            put_object_retrying_operation_aborted(client, &bucket, key, original_body.to_vec())
                .await;
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("create multipart upload for XML precedence test")
            .await
            .unwrap();
        let valid_upload_id = create.upload_id().unwrap().to_string();
        let part_body = b"object after corrected completion";
        let part = upload_part_retrying_operation_aborted(
            client,
            &bucket,
            key,
            &valid_upload_id,
            1,
            part_body.to_vec(),
        )
        .await;
        let invalid_upload_id = "a".repeat(1025);

        let valid_url = object_url(
            CTX.endpoint(),
            &bucket,
            key,
            Some(&format!("uploadId={valid_upload_id}")),
        );
        let authorization_canary = send_signed_request_with_credentials(
            "POST",
            &valid_url,
            &complete_multipart_upload_xml("\"abc\"", 1),
            [("content-type", "application/xml")],
            alt_credentials(),
        );
        assert_access_denied(&authorization_canary);

        for credentials in [primary_credentials(), alt_credentials()] {
            let valid_url = object_url(
                CTX.endpoint(),
                &bucket,
                key,
                Some(&format!("uploadId={valid_upload_id}")),
            );
            let valid_response = send_signed_request_with_credentials(
                "POST",
                &valid_url,
                b"<",
                [("content-type", "application/xml")],
                credentials,
            );
            assert_malformed_xml(&valid_response);

            let invalid_url = object_url(
                CTX.endpoint(),
                &bucket,
                key,
                Some(&format!("uploadId={invalid_upload_id}")),
            );
            let invalid_response = send_signed_request_with_credentials(
                "POST",
                &invalid_url,
                b"<",
                [("content-type", "application/xml")],
                credentials,
            );
            assert_invalid_upload_id_no_such_upload(&invalid_response, &invalid_upload_id);
        }

        assert_multipart_parts_preserved(
            &bucket,
            key,
            &valid_upload_id,
            &[(1, part_body.len() as i64, part.e_tag().unwrap())],
        )
        .await;
        assert_object_contents_and_etag(&bucket, key, original.e_tag().unwrap(), original_body)
            .await;

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&valid_upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(part.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("retry multipart completion with well-formed XML")
            .await
            .unwrap();
        assert_list_parts_no_such_upload(&bucket, key, &valid_upload_id).await;
        let completed = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("get object after corrected malformed completion")
            .await
            .unwrap();
        assert_eq!(
            completed
                .body
                .collect()
                .await
                .unwrap()
                .into_bytes()
                .as_ref(),
            part_body
        );

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_abort_multipart_upload_after_complete_and_delete_succeeds() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-abort-after-complete-delete";

        let upload_id = complete_single_part_multipart_upload(&bucket, key, b"first").await;

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        assert_s3_err_code(&get, "NoSuchKey");

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_abort_multipart_upload_after_complete_and_overwrite_succeeds() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-abort-after-complete-overwrite";

        let first_upload_id = complete_single_part_multipart_upload(&bucket, key, b"first").await;
        let second_upload_id = complete_single_part_multipart_upload(&bucket, key, b"second").await;

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&first_upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&second_upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"second");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_abort_multipart_upload_after_bucket_delete_and_recreate_fails() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-abort-after-bucket-recreate";

        let upload_id = complete_single_part_multipart_upload(&bucket, key, b"first").await;

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
        s3_tests::create_bucket_retrying_reuse(client, &bucket)
            .await
            .unwrap();

        let result = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        assert_s3_err_code(&result, "NoSuchUpload");

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_multipart_terminal_retries() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let completed_key = "terminal-retry-completed";
        let aborted_key = "terminal-retry-aborted";

        let (_, completed_upload_id) = raw_create_upload(&bucket, completed_key, &[]);
        let (_, completed_etag) = raw_upload_part(
            &bucket,
            completed_key,
            &completed_upload_id,
            1,
            b"completed body",
            &[],
        );
        let completed_body = single_part_complete_body(&completed_etag);
        let completed = raw_complete_upload(
            &bucket,
            completed_key,
            &completed_upload_id,
            &completed_body,
            &[],
        );
        assert_eq!(completed.status, 200, "initial completion: {completed:?}");
        let completed_result_etag = xml_tag_text(&completed.body, "ETag")
            .expect("initial completion result must contain an ETag");

        let (_, aborted_upload_id) = raw_create_upload(&bucket, aborted_key, &[]);
        let (_, aborted_etag) = raw_upload_part(
            &bucket,
            aborted_key,
            &aborted_upload_id,
            1,
            b"aborted body",
            &[],
        );
        let aborted_body = single_part_complete_body(&aborted_etag);
        let aborted = raw_multipart_query(
            "DELETE",
            &bucket,
            aborted_key,
            &format!("uploadId={aborted_upload_id}"),
            b"",
            &[],
        );
        assert_eq!(aborted.status, 204, "initial abort: {aborted:?}");

        let changed_completion_body =
            single_part_complete_body("\"00000000000000000000000000000000\"");
        let changed_completion = raw_complete_upload(
            &bucket,
            completed_key,
            &completed_upload_id,
            &changed_completion_body,
            &[],
        );
        assert_invalid_upload_id_no_such_upload(&changed_completion, &completed_upload_id);

        let probe = |completed_replays: bool, aborts_are_idempotent: bool| {
            let complete_completed = raw_complete_upload(
                &bucket,
                completed_key,
                &completed_upload_id,
                &completed_body,
                &[],
            );
            if completed_replays {
                assert_eq!(
                    complete_completed.status, 200,
                    "completion retry: {complete_completed:?}"
                );
                assert_eq!(
                    xml_tag_text(&complete_completed.body, "ETag"),
                    Some(completed_result_etag)
                );
            } else {
                assert_invalid_upload_id_no_such_upload(&complete_completed, &completed_upload_id);
            }

            let abort_completed = raw_multipart_query(
                "DELETE",
                &bucket,
                completed_key,
                &format!("uploadId={completed_upload_id}"),
                b"",
                &[],
            );
            if aborts_are_idempotent {
                assert_eq!(abort_completed.status, 204, "{abort_completed:?}");
            } else {
                assert_invalid_upload_id_no_such_upload(&abort_completed, &completed_upload_id);
            }

            let complete_completed_after_abort = raw_complete_upload(
                &bucket,
                completed_key,
                &completed_upload_id,
                &completed_body,
                &[],
            );
            if completed_replays {
                assert_eq!(
                    complete_completed_after_abort.status, 200,
                    "completion retry after abort retry: {complete_completed_after_abort:?}"
                );
                assert_eq!(
                    xml_tag_text(&complete_completed_after_abort.body, "ETag"),
                    Some(completed_result_etag)
                );
            } else {
                assert_invalid_upload_id_no_such_upload(
                    &complete_completed_after_abort,
                    &completed_upload_id,
                );
            }

            let complete_aborted =
                raw_complete_upload(&bucket, aborted_key, &aborted_upload_id, &aborted_body, &[]);
            assert_invalid_upload_id_no_such_upload(&complete_aborted, &aborted_upload_id);

            let abort_aborted = raw_multipart_query(
                "DELETE",
                &bucket,
                aborted_key,
                &format!("uploadId={aborted_upload_id}"),
                b"",
                &[],
            );
            if aborts_are_idempotent {
                assert_eq!(abort_aborted.status, 204, "{abort_aborted:?}");
            } else {
                assert_invalid_upload_id_no_such_upload(&abort_aborted, &aborted_upload_id);
            }

            let complete_aborted_after_abort =
                raw_complete_upload(&bucket, aborted_key, &aborted_upload_id, &aborted_body, &[]);
            assert_invalid_upload_id_no_such_upload(
                &complete_aborted_after_abort,
                &aborted_upload_id,
            );
        };

        probe(true, true);
        let completed_object = client
            .get_object()
            .bucket(&bucket)
            .key(completed_key)
            .send_retrying_operation_aborted("get completed object after terminal retries")
            .await
            .unwrap();
        assert_eq!(
            completed_object.body.collect().await.unwrap().into_bytes(),
            &b"completed body"[..]
        );
        assert_s3_err_code(
            &client
                .get_object()
                .bucket(&bucket)
                .key(aborted_key)
                .send_retrying_operation_aborted("get aborted upload key after terminal retries")
                .await,
            "NoSuchKey",
        );

        put_object_retrying_operation_aborted(
            client,
            &bucket,
            completed_key,
            b"completed overwrite".to_vec(),
        )
        .await;
        put_object_retrying_operation_aborted(
            client,
            &bucket,
            aborted_key,
            b"aborted overwrite".to_vec(),
        )
        .await;
        probe(false, true);
        let malformed_xml =
            raw_complete_upload(&bucket, completed_key, &completed_upload_id, "<", &[]);
        assert_malformed_xml(&malformed_xml);
        let malformed_expected_size = raw_complete_upload(
            &bucket,
            completed_key,
            &completed_upload_id,
            &completed_body,
            &[("x-amz-mp-object-size", "bad")],
        );
        assert_eq!(
            malformed_expected_size.status, 400,
            "malformed expected-size header after overwrite: {malformed_expected_size:?}"
        );
        assert_eq!(
            xml_tag_text(&malformed_expected_size.body, "Code"),
            Some("InvalidRequest")
        );
        for (key, expected) in [
            (completed_key, &b"completed overwrite"[..]),
            (aborted_key, &b"aborted overwrite"[..]),
        ] {
            let object = client
                .get_object()
                .bucket(&bucket)
                .key(key)
                .send_retrying_operation_aborted("get overwritten terminal retry object")
                .await
                .unwrap();
            assert_eq!(object.body.collect().await.unwrap().into_bytes(), expected);
        }

        delete_object_retrying_operation_aborted(client, &bucket, completed_key).await;
        delete_object_retrying_operation_aborted(client, &bucket, aborted_key).await;
        probe(false, true);
        for key in [completed_key, aborted_key] {
            assert_s3_err_code(
                &client
                    .get_object()
                    .bucket(&bucket)
                    .key(key)
                    .send_retrying_operation_aborted("get deleted terminal retry object")
                    .await,
                "NoSuchKey",
            );
        }

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
        s3_tests::create_bucket_retrying_reuse(client, &bucket)
            .await
            .unwrap();
        probe(false, false);
        let uploads = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list uploads after terminal bucket recreation")
            .await
            .unwrap();
        assert!(uploads.uploads().is_empty());
        let objects = client
            .list_objects_v2()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list objects after terminal bucket recreation")
            .await
            .unwrap();
        assert!(objects.contents().is_empty());

        cleanup(&bucket, &[]).await;
    });
}

// ── ListMultipartUploads ────────────────────────────────────────────

#[test]
fn test_multipart_terminal_completion_replay_versioned_history() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "terminal-retry-versioned";
        put_bucket_versioning_retrying_operation_aborted(
            client,
            &bucket,
            BucketVersioningStatus::Enabled,
        )
        .await;

        let (_, upload_id) = raw_create_upload(&bucket, key, &[]);
        let (_, part_etag) = raw_upload_part(
            &bucket,
            key,
            &upload_id,
            1,
            b"versioned completed body",
            &[],
        );
        let completion_body = single_part_complete_body(&part_etag);
        let completed = raw_complete_upload(&bucket, key, &upload_id, &completion_body, &[]);
        assert_eq!(completed.status, 200, "initial completion: {completed:?}");
        let completed_etag = xml_tag_text(&completed.body, "ETag")
            .expect("versioned completion result must contain an ETag")
            .to_string();
        let completed_version_id =
            s3_tests::shape::response_header_value(&completed, "x-amz-version-id")
                .expect("versioned completion result must contain x-amz-version-id")
                .to_string();

        let assert_replay = |expected: bool, history: &str| {
            let replay = raw_complete_upload(&bucket, key, &upload_id, &completion_body, &[]);
            if expected {
                assert_eq!(replay.status, 200, "{history}: {replay:?}");
                assert_eq!(
                    xml_tag_text(&replay.body, "ETag"),
                    Some(completed_etag.as_str()),
                    "{history}: {replay:?}"
                );
                assert_eq!(
                    s3_tests::shape::response_header_value(&replay, "x-amz-version-id"),
                    Some(completed_version_id.as_str()),
                    "{history}: {replay:?}"
                );
            } else {
                assert_invalid_upload_id_no_such_upload(&replay, &upload_id);
            }
        };

        assert_replay(true, "immediate versioned replay");

        let later =
            put_object_retrying_operation_aborted(client, &bucket, key, b"later version".to_vec())
                .await;
        let later_version_id = later
            .version_id()
            .expect("versioned overwrite must return a VersionId")
            .to_string();
        assert_replay(true, "replay after later version");

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&later_version_id)
            .send_retrying_operation_aborted("delete later version during replay oracle")
            .await
            .unwrap();
        assert_replay(true, "replay after completed version becomes current again");

        let delete_marker = delete_object_retrying_operation_aborted(client, &bucket, key).await;
        assert!(delete_marker.delete_marker().unwrap_or(false));
        let delete_marker_version_id = delete_marker
            .version_id()
            .expect("versioned delete must return a delete-marker VersionId")
            .to_string();
        assert_replay(true, "replay while delete marker is current");

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&delete_marker_version_id)
            .send_retrying_operation_aborted("delete marker during replay oracle")
            .await
            .unwrap();
        assert_replay(true, "replay after delete marker removal");

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .version_id(&completed_version_id)
            .send_retrying_operation_aborted("delete completed version during replay oracle")
            .await
            .unwrap();
        assert_replay(false, "replay after completed version deletion");

        s3_tests::cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_multipart_terminal_completion_replay_suspended_history() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        put_bucket_versioning_retrying_operation_aborted(
            client,
            &bucket,
            BucketVersioningStatus::Enabled,
        )
        .await;

        let numbered_key = "terminal-retry-suspended-numbered";
        let (numbered_upload_id, numbered_body, numbered_completion) =
            raw_complete_single_part_upload(&bucket, numbered_key, b"numbered completion");
        let numbered_etag = xml_tag_text(&numbered_completion.body, "ETag")
            .expect("numbered completion must return an ETag")
            .to_string();
        let numbered_version_id =
            s3_tests::shape::response_header_value(&numbered_completion, "x-amz-version-id")
                .expect("enabled completion must return x-amz-version-id")
                .to_string();

        put_bucket_versioning_retrying_operation_aborted(
            client,
            &bucket,
            BucketVersioningStatus::Suspended,
        )
        .await;

        assert_terminal_completion_replay(
            &bucket,
            numbered_key,
            &numbered_upload_id,
            &numbered_body,
            Some((&numbered_etag, Some(&numbered_version_id))),
            "numbered completion after suspension",
        );

        let later_put = put_object_retrying_operation_aborted(
            client,
            &bucket,
            numbered_key,
            b"later suspended put".to_vec(),
        )
        .await;
        assert_eq!(later_put.version_id(), None);
        assert_terminal_completion_replay(
            &bucket,
            numbered_key,
            &numbered_upload_id,
            &numbered_body,
            Some((&numbered_etag, Some(&numbered_version_id))),
            "numbered completion after suspended null write",
        );

        let delete_marker =
            delete_object_retrying_operation_aborted(client, &bucket, numbered_key).await;
        assert!(delete_marker.delete_marker().unwrap_or(false));
        let delete_marker_version_id = delete_marker
            .version_id()
            .expect("suspended delete marker must return a version ID")
            .to_string();
        assert_eq!(delete_marker_version_id, "null");
        assert_terminal_completion_replay(
            &bucket,
            numbered_key,
            &numbered_upload_id,
            &numbered_body,
            Some((&numbered_etag, Some(&numbered_version_id))),
            "numbered completion under suspended delete marker",
        );
        let repeated_delete_marker =
            delete_object_retrying_operation_aborted(client, &bucket, numbered_key).await;
        assert!(repeated_delete_marker.delete_marker().unwrap_or(false));
        assert_eq!(repeated_delete_marker.version_id(), Some("null"));
        assert_terminal_completion_replay(
            &bucket,
            numbered_key,
            &numbered_upload_id,
            &numbered_body,
            Some((&numbered_etag, Some(&numbered_version_id))),
            "numbered completion under repeated suspended delete marker",
        );
        let removed_numbered_marker = client
            .delete_object()
            .bucket(&bucket)
            .key(numbered_key)
            .version_id(&delete_marker_version_id)
            .send_retrying_operation_aborted("remove suspended delete marker during replay oracle")
            .await
            .unwrap();
        assert_eq!(removed_numbered_marker.version_id(), Some("null"));
        assert!(removed_numbered_marker.delete_marker().unwrap_or(false));
        assert_terminal_completion_replay(
            &bucket,
            numbered_key,
            &numbered_upload_id,
            &numbered_body,
            Some((&numbered_etag, Some(&numbered_version_id))),
            "numbered completion after suspended delete-marker removal",
        );

        let (later_upload_id, later_body, later_completion) =
            raw_complete_single_part_upload(&bucket, numbered_key, b"later null completion");
        let later_etag = xml_tag_text(&later_completion.body, "ETag")
            .expect("suspended completion must return an ETag")
            .to_string();
        assert_eq!(
            s3_tests::shape::response_header_value(&later_completion, "x-amz-version-id"),
            None
        );
        assert_terminal_completion_replay(
            &bucket,
            numbered_key,
            &numbered_upload_id,
            &numbered_body,
            Some((&numbered_etag, Some(&numbered_version_id))),
            "numbered completion after later null multipart completion",
        );
        assert_terminal_completion_replay(
            &bucket,
            numbered_key,
            &later_upload_id,
            &later_body,
            Some((&later_etag, None)),
            "later null multipart completion",
        );

        client
            .delete_object()
            .bucket(&bucket)
            .key(numbered_key)
            .version_id(&numbered_version_id)
            .send_retrying_operation_aborted(
                "delete numbered completion during suspended replay oracle",
            )
            .await
            .unwrap();
        assert_terminal_completion_replay(
            &bucket,
            numbered_key,
            &numbered_upload_id,
            &numbered_body,
            None,
            "deleted numbered completion",
        );
        assert_terminal_completion_replay(
            &bucket,
            numbered_key,
            &later_upload_id,
            &later_body,
            Some((&later_etag, None)),
            "retained null completion after numbered-version deletion",
        );
        let deleted_null = client
            .delete_object()
            .bucket(&bucket)
            .key(numbered_key)
            .version_id("null")
            .send_retrying_operation_aborted(
                "delete null completion during suspended replay oracle",
            )
            .await
            .unwrap();
        assert_eq!(deleted_null.version_id(), Some("null"));
        assert!(!deleted_null.delete_marker().unwrap_or(false));
        assert_terminal_completion_replay(
            &bucket,
            numbered_key,
            &later_upload_id,
            &later_body,
            None,
            "deleted null completion",
        );

        let replaced_key = "terminal-retry-suspended-null-replaced";
        let (replaced_upload_id, replaced_body, replaced_completion) =
            raw_complete_single_part_upload(&bucket, replaced_key, b"replace this null version");
        let replaced_etag = xml_tag_text(&replaced_completion.body, "ETag").unwrap();
        assert_terminal_completion_replay(
            &bucket,
            replaced_key,
            &replaced_upload_id,
            &replaced_body,
            Some((replaced_etag, None)),
            "initial null completion",
        );
        put_object_retrying_operation_aborted(
            client,
            &bucket,
            replaced_key,
            b"replacement null write".to_vec(),
        )
        .await;
        assert_terminal_completion_replay(
            &bucket,
            replaced_key,
            &replaced_upload_id,
            &replaced_body,
            None,
            "null completion replaced by suspended write",
        );

        let marker_key = "terminal-retry-suspended-null-marker";
        let (marker_upload_id, marker_body, _) =
            raw_complete_single_part_upload(&bucket, marker_key, b"delete this null version");
        let marker = delete_object_retrying_operation_aborted(client, &bucket, marker_key).await;
        let marker_version_id = marker.version_id().unwrap_or("null").to_string();
        assert_eq!(marker_version_id, "null");
        assert_terminal_completion_replay(
            &bucket,
            marker_key,
            &marker_upload_id,
            &marker_body,
            None,
            "null completion replaced by suspended delete marker",
        );
        let removed_marker = client
            .delete_object()
            .bucket(&bucket)
            .key(marker_key)
            .version_id(&marker_version_id)
            .send_retrying_operation_aborted("remove null delete marker during replay oracle")
            .await
            .unwrap();
        assert_eq!(removed_marker.version_id(), Some("null"));
        assert!(removed_marker.delete_marker().unwrap_or(false));
        assert_terminal_completion_replay(
            &bucket,
            marker_key,
            &marker_upload_id,
            &marker_body,
            None,
            "null completion after delete-marker removal",
        );

        let later_multipart_key = "terminal-retry-suspended-later-multipart";
        let (first_upload_id, first_body, _) =
            raw_complete_single_part_upload(&bucket, later_multipart_key, b"first null completion");
        let (second_upload_id, second_body, second_completion) = raw_complete_single_part_upload(
            &bucket,
            later_multipart_key,
            b"second null completion",
        );
        let second_etag = xml_tag_text(&second_completion.body, "ETag").unwrap();
        assert_terminal_completion_replay(
            &bucket,
            later_multipart_key,
            &first_upload_id,
            &first_body,
            None,
            "null completion replaced by later multipart completion",
        );
        assert_terminal_completion_replay(
            &bucket,
            later_multipart_key,
            &second_upload_id,
            &second_body,
            Some((second_etag, None)),
            "current later multipart completion",
        );

        s3_tests::cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_terminal_completion_replay_header_matrix() {
    s3_tests::run(async {
        use base64::Engine;

        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "terminal-retry-header-matrix";
        put_bucket_versioning_retrying_operation_aborted(
            client,
            &bucket,
            BucketVersioningStatus::Enabled,
        )
        .await;

        let (_, upload_id) =
            raw_create_upload(&bucket, key, &[("x-amz-checksum-algorithm", "SHA256")]);
        let part_body = b"terminal replay header body";
        let part_checksum = base64::engine::general_purpose::STANDARD
            .encode(ring::digest::digest(&ring::digest::SHA256, part_body).as_ref());
        let (_, part_etag) = raw_upload_part(
            &bucket,
            key,
            &upload_id,
            1,
            part_body,
            &[("x-amz-checksum-sha256", part_checksum.as_str())],
        );
        let completion_body = format!(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{part_etag}</ETag>\
             <ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part></CompleteMultipartUpload>"
        );
        let completed = raw_complete_upload(&bucket, key, &upload_id, &completion_body, &[]);
        assert_eq!(completed.status, 200, "initial completion: {completed:?}");
        let completed_etag = xml_tag_text(&completed.body, "ETag").unwrap().to_string();
        let completed_version_id =
            s3_tests::shape::response_header_value(&completed, "x-amz-version-id")
                .expect("versioned completion should return x-amz-version-id")
                .to_string();
        let completed_checksum = xml_tag_text(&completed.body, "ChecksumSHA256")
            .expect("completion should return ChecksumSHA256")
            .to_string();
        let wrong_checksum = format!(
            "{}-1",
            base64::engine::general_purpose::STANDARD.encode([0u8; 32])
        );
        let matching_size = part_body.len().to_string();
        let mismatched_size = (part_body.len() + 1).to_string();

        let replay_cases: Vec<(&str, Vec<(&str, &str)>)> = vec![
            ("absent", vec![]),
            (
                "if-match current",
                vec![("if-match", completed_etag.as_str())],
            ),
            ("if-match mismatch", vec![("if-match", "\"wrong\"")]),
            ("if-none-match wildcard", vec![("if-none-match", "*")]),
            (
                "checksum matching",
                vec![("x-amz-checksum-sha256", completed_checksum.as_str())],
            ),
            (
                "checksum mismatch",
                vec![("x-amz-checksum-sha256", wrong_checksum.as_str())],
            ),
            (
                "size matching",
                vec![("x-amz-mp-object-size", matching_size.as_str())],
            ),
            (
                "size mismatch",
                vec![("x-amz-mp-object-size", mismatched_size.as_str())],
            ),
        ];

        for (label, headers) in replay_cases {
            let response =
                raw_complete_upload(&bucket, key, &upload_id, &completion_body, &headers);
            assert_eq!(response.status, 200, "{label}: {response:?}");
            assert_eq!(
                xml_tag_text(&response.body, "ETag"),
                Some(completed_etag.as_str()),
                "{label}: {response:?}"
            );
            assert_eq!(
                s3_tests::shape::response_header_value(&response, "x-amz-version-id"),
                Some(completed_version_id.as_str()),
                "{label}: {response:?}"
            );
            assert_eq!(xml_tag_text(&response.body, "ChecksumSHA256"), None);
            assert_eq!(xml_tag_text(&response.body, "ChecksumType"), None);
        }

        let malformed_cases = [
            (
                "empty If-Match",
                vec![("if-match", "")],
                shape()
                    .status(400)
                    .headers(error_response_headers())
                    .body(
                        "<Error><Code>InvalidArgument</Code>\
                         <Message>The value provided for the If-Match query parameter cannot be empty for this API.</Message>\
                         <ArgumentName>If-Match</ArgumentName>\
                         <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>",
                    ),
            ),
            (
                "specific matching If-None-Match",
                vec![("if-none-match", completed_etag.as_str())],
                shape()
                    .status(501)
                    .headers(error_response_headers())
                    .header("cache-control", "no-store")
                    .body(
                        "<Error><Code>NotImplemented</Code>\
                         <Message>A header you provided implies functionality that is not implemented</Message>\
                         <Header>If-None-Match</Header>\
                         <additionalMessage>We don't accept the provided value of If-None-Match header for this API</additionalMessage>\
                         <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>",
                    ),
            ),
            (
                "specific mismatched If-None-Match",
                vec![("if-none-match", "\"wrong\"")],
                shape()
                    .status(501)
                    .headers(error_response_headers())
                    .header("cache-control", "no-store")
                    .body(
                        "<Error><Code>NotImplemented</Code>\
                         <Message>A header you provided implies functionality that is not implemented</Message>\
                         <Header>If-None-Match</Header>\
                         <additionalMessage>We don't accept the provided value of If-None-Match header for this API</additionalMessage>\
                         <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>",
                    ),
            ),
            (
                "empty If-None-Match",
                vec![("if-none-match", "")],
                shape()
                    .status(501)
                    .headers(error_response_headers())
                    .header("cache-control", "no-store")
                    .body(
                        "<Error><Code>NotImplemented</Code>\
                         <Message>A header you provided implies functionality that is not implemented</Message>\
                         <Header>If-None-Match</Header>\
                         <additionalMessage>We don't accept the provided value of If-None-Match header for this API</additionalMessage>\
                         <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>",
                    ),
            ),
            (
                "malformed aggregate checksum",
                vec![("x-amz-checksum-sha256", "bad")],
                shape()
                    .status(400)
                    .headers(error_response_headers())
                    .body(expected_error::complete_multipart_checksum_header_invalid(
                        "x-amz-checksum-sha256",
                    )),
            ),
            (
                "malformed expected size",
                vec![("x-amz-mp-object-size", "bad")],
                shape()
                    .status(400)
                    .headers(error_response_headers())
                    .body(
                        "<Error><Code>InvalidRequest</Code>\
                         <Message>Value for x-amz-mp-object-size header is invalid: 'bad'</Message>\
                         <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>",
                    ),
            ),
        ];
        for (label, headers, expected) in malformed_cases {
            let response =
                raw_complete_upload(&bucket, key, &upload_id, &completion_body, &headers);
            assert_shape(label, &response, &expected);
        }

        let claimed_key = "terminal-retry-header-matrix-claimed";
        let (_, claimed_upload_id) = raw_create_upload(
            &bucket,
            claimed_key,
            &[("x-amz-checksum-algorithm", "SHA256")],
        );
        let (_, claimed_part_etag) = raw_upload_part(
            &bucket,
            claimed_key,
            &claimed_upload_id,
            1,
            part_body,
            &[("x-amz-checksum-sha256", part_checksum.as_str())],
        );
        let claimed_completion_body = format!(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{claimed_part_etag}</ETag>\
             <ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part></CompleteMultipartUpload>"
        );
        let claimed = raw_complete_upload(
            &bucket,
            claimed_key,
            &claimed_upload_id,
            &claimed_completion_body,
            &[
                ("x-amz-checksum-sha256", completed_checksum.as_str()),
                ("x-amz-mp-object-size", matching_size.as_str()),
            ],
        );
        assert_eq!(claimed.status, 200, "claimed completion: {claimed:?}");
        let claimed_etag = xml_tag_text(&claimed.body, "ETag").unwrap().to_string();
        let claimed_version_id =
            s3_tests::shape::response_header_value(&claimed, "x-amz-version-id")
                .expect("claimed completion should return x-amz-version-id")
                .to_string();
        assert_eq!(
            xml_tag_text(&claimed.body, "ChecksumSHA256"),
            Some(completed_checksum.as_str())
        );
        let claimed_replay = raw_complete_upload(
            &bucket,
            claimed_key,
            &claimed_upload_id,
            &claimed_completion_body,
            &[],
        );
        assert_eq!(claimed_replay.status, 200, "{claimed_replay:?}");
        assert_eq!(
            xml_tag_text(&claimed_replay.body, "ETag"),
            Some(claimed_etag.as_str())
        );
        assert_eq!(
            s3_tests::shape::response_header_value(&claimed_replay, "x-amz-version-id"),
            Some(claimed_version_id.as_str())
        );
        assert_eq!(xml_tag_text(&claimed_replay.body, "ChecksumSHA256"), None);
        assert_eq!(xml_tag_text(&claimed_replay.body, "ChecksumType"), None);

        assert_list_parts_no_such_upload(&bucket, key, &upload_id).await;
        assert_list_parts_no_such_upload(&bucket, claimed_key, &claimed_upload_id).await;
        assert_object_contents_and_etag(&bucket, key, &completed_etag, part_body).await;
        assert_object_contents_and_etag(&bucket, claimed_key, &claimed_etag, part_body).await;

        let versions = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list versions after terminal replay header matrix")
            .await
            .unwrap();
        assert_eq!(versions.versions().len(), 2);
        assert!(versions.delete_markers().is_empty());
        assert!(versions.versions().iter().any(|version| {
            version.key() == Some(key)
                && version.version_id() == Some(completed_version_id.as_str())
        }));
        assert!(versions.versions().iter().any(|version| {
            version.key() == Some(claimed_key)
                && version.version_id() == Some(claimed_version_id.as_str())
        }));

        s3_tests::cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_multipart_terminal_completion_replay_obeys_current_explicit_deny() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "terminal-retry-current-policy";

        let (_, upload_id) = raw_create_upload(&bucket, key, &[]);
        let (_, part_etag) =
            raw_upload_part(&bucket, key, &upload_id, 1, b"policy replay body", &[]);
        let completion_body = single_part_complete_body(&part_etag);
        let completed = raw_complete_upload(&bucket, key, &upload_id, &completion_body, &[]);
        assert_eq!(completed.status, 200, "initial completion: {completed:?}");
        let replay = raw_complete_upload(&bucket, key, &upload_id, &completion_body, &[]);
        assert_eq!(replay.status, 200, "pre-policy replay canary: {replay:?}");

        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": format!("arn:aws:s3:::{bucket}/{key}")
            }]
        });
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy.to_string())
            .send_retrying_operation_aborted("install terminal replay explicit deny")
            .await
            .unwrap();

        let mut denied = false;
        for attempt in 0..60 {
            let response = raw_complete_upload(&bucket, key, &upload_id, &completion_body, &[]);
            match response.status {
                403 => {
                    assert_access_denied(&response);
                    denied = true;
                    break;
                }
                200 if attempt + 1 < 60 => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                _ => panic!("terminal replay did not converge to explicit deny: {response:?}"),
            }
        }
        assert!(
            denied,
            "terminal replay must observe the current explicit deny"
        );

        client
            .delete_bucket_policy()
            .bucket(&bucket)
            .send_retrying_operation_aborted("remove terminal replay explicit deny")
            .await
            .unwrap();
        let mut allowed = false;
        for attempt in 0..60 {
            let response = raw_complete_upload(&bucket, key, &upload_id, &completion_body, &[]);
            match response.status {
                200 => {
                    allowed = true;
                    break;
                }
                403 if attempt + 1 < 60 => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                _ => panic!("terminal replay did not recover after policy removal: {response:?}"),
            }
        }
        assert!(allowed, "terminal replay must recover after policy removal");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_list_multipart_uploads_empty() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert!(resp.uploads().is_empty());

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_active() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        // Create two uploads
        let create1 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("key1")
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let uid1 = create1.upload_id().unwrap().to_string();

        let create2 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("key2")
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let uid2 = create2.upload_id().unwrap().to_string();

        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let uploads = resp.uploads();
        assert_eq!(uploads.len(), 2);

        let keys: Vec<&str> = uploads.iter().map(|u| u.key().unwrap()).collect();
        assert!(keys.contains(&"key1"));
        assert!(keys.contains(&"key2"));

        // Abort both
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("key1")
            .upload_id(&uid1)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("key2")
            .upload_id(&uid2)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        // Should be empty now
        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert!(resp.uploads().is_empty());

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_prefix() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let c1 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("photos/a.jpg")
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let uid1 = c1.upload_id().unwrap().to_string();

        let c2 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("docs/b.txt")
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let uid2 = c2.upload_id().unwrap().to_string();

        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .prefix("photos/")
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let uploads = resp.uploads();
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].key().unwrap(), "photos/a.jpg");

        // Cleanup
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("photos/a.jpg")
            .upload_id(&uid1)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("docs/b.txt")
            .upload_id(&uid2)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_pagination_and_markers() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let uploads = ["a&upload", "b<upload", "c\"upload"];
        let mut created = Vec::new();
        for key in uploads {
            let create = client
                .create_multipart_upload()
                .bucket(&bucket)
                .key(key)
                .send_retrying_operation_aborted("S3 operation during multipart test")
                .await
                .unwrap();
            created.push((key.to_string(), create.upload_id().unwrap().to_string()));
        }

        let resp1 = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .max_uploads(1)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(resp1.uploads().len(), 1);
        assert_eq!(resp1.uploads()[0].key().unwrap(), "a&upload");
        assert_eq!(resp1.is_truncated(), Some(true));
        let next_key_1 = resp1.next_key_marker().unwrap().to_string();
        let next_upload_1 = resp1.next_upload_id_marker().unwrap().to_string();

        let resp2 = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .key_marker(next_key_1)
            .upload_id_marker(next_upload_1)
            .max_uploads(1)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(resp2.uploads().len(), 1);
        assert_eq!(resp2.uploads()[0].key().unwrap(), "b<upload");
        assert_eq!(resp2.is_truncated(), Some(true));
        let next_key_2 = resp2.next_key_marker().unwrap().to_string();
        let next_upload_2 = resp2.next_upload_id_marker().unwrap().to_string();

        let resp3 = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .key_marker(next_key_2)
            .upload_id_marker(next_upload_2)
            .max_uploads(1)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(resp3.uploads().len(), 1);
        assert_eq!(resp3.uploads()[0].key().unwrap(), "c\"upload");
        assert_eq!(resp3.is_truncated(), Some(false));

        for (key, upload_id) in created {
            client
                .abort_multipart_upload()
                .bucket(&bucket)
                .key(key)
                .upload_id(upload_id)
                .send_retrying_operation_aborted("S3 operation during multipart test")
                .await
                .unwrap();
        }
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_same_key_ordering_markers_and_terminal_states() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let same_key = "same-key-order";

        let mut created = Vec::new();
        for key in ["a-before", same_key, same_key, same_key, "z-after"] {
            let upload = client
                .create_multipart_upload()
                .bucket(&bucket)
                .key(key)
                .send_retrying_operation_aborted("S3 operation during multipart test")
                .await
                .unwrap();
            created.push((key, upload.upload_id().unwrap().to_string()));
        }

        let all = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(
            all.uploads()
                .iter()
                .map(|upload| upload.key().unwrap())
                .collect::<Vec<_>>(),
            ["a-before", same_key, same_key, same_key, "z-after"]
        );
        assert_eq!(
            all.uploads()[1..4]
                .iter()
                .map(|upload| upload.upload_id().unwrap())
                .collect::<Vec<_>>(),
            created[1..4]
                .iter()
                .map(|(_, upload_id)| upload_id.as_str())
                .collect::<Vec<_>>()
        );
        assert!(all.uploads()[1..4]
            .windows(2)
            .all(|pair| pair[0].initiated().unwrap() <= pair[1].initiated().unwrap()));
        assert_eq!(all.is_truncated(), Some(false));
        assert_eq!(all.next_key_marker(), Some("z-after"));
        assert_eq!(all.next_upload_id_marker(), Some(created[4].1.as_str()));

        let first_page = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .prefix(same_key)
            .max_uploads(2)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(first_page.uploads().len(), 2);
        assert_eq!(first_page.is_truncated(), Some(true));
        assert_eq!(first_page.next_key_marker(), Some(same_key));
        assert_eq!(
            first_page.next_upload_id_marker(),
            Some(created[2].1.as_str())
        );

        let second_page = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .prefix(same_key)
            .key_marker(first_page.next_key_marker().unwrap())
            .upload_id_marker(first_page.next_upload_id_marker().unwrap())
            .max_uploads(2)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(second_page.uploads().len(), 1);
        assert_eq!(
            second_page.uploads()[0].upload_id(),
            Some(created[3].1.as_str())
        );
        assert_eq!(second_page.is_truncated(), Some(false));
        assert_eq!(second_page.next_key_marker(), Some(same_key));
        assert_eq!(
            second_page.next_upload_id_marker(),
            Some(created[3].1.as_str())
        );

        let after_key = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .key_marker(same_key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(after_key.uploads().len(), 1);
        assert_eq!(after_key.uploads()[0].key(), Some("z-after"));
        assert_eq!(after_key.next_key_marker(), Some("z-after"));
        assert_eq!(
            after_key.next_upload_id_marker(),
            Some(created[4].1.as_str())
        );

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(same_key)
            .upload_id(&created[1].1)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let completed_part = upload_part_retrying_operation_aborted(
            client,
            &bucket,
            same_key,
            &created[2].1,
            1,
            b"completed-body".to_vec(),
        )
        .await;
        complete_multipart_upload_retrying_operation_aborted(
            client,
            &bucket,
            same_key,
            &created[2].1,
            completed_part.e_tag().unwrap(),
        )
        .await;

        let one_active = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .prefix(same_key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(one_active.uploads().len(), 1);
        assert_eq!(
            one_active.uploads()[0].upload_id(),
            Some(created[3].1.as_str())
        );
        assert_eq!(one_active.next_key_marker(), Some(same_key));
        assert_eq!(
            one_active.next_upload_id_marker(),
            Some(created[3].1.as_str())
        );

        for index in [0, 3, 4] {
            client
                .abort_multipart_upload()
                .bucket(&bucket)
                .key(created[index].0)
                .upload_id(&created[index].1)
                .send_retrying_operation_aborted("S3 operation during multipart test")
                .await
                .unwrap();
        }
        cleanup(&bucket, &[same_key]).await;
    });
}

#[test]
fn test_list_multipart_uploads_pagination_across_lifecycle_changes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let marker_key = "m-marker";

        let mut initial = Vec::new();
        for key in [
            "a-before",
            marker_key,
            marker_key,
            marker_key,
            "n-complete",
            "o-abort",
            "z-stable",
        ] {
            let upload =
                create_multipart_upload_retrying_operation_aborted(client, &bucket, key).await;
            initial.push((key, upload.upload_id().unwrap().to_string()));
        }

        let first = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .max_uploads(3)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let first_entries = first
            .uploads()
            .iter()
            .map(|upload| {
                (
                    upload.key().unwrap().to_string(),
                    upload.upload_id().unwrap().to_string(),
                )
            })
            .collect::<Vec<_>>();
        let first_next_key_marker = first.next_key_marker().unwrap().to_string();
        let first_next_upload_id_marker = first.next_upload_id_marker().unwrap().to_string();

        // Remove the exact marker row, add uploads on both sides of the marker,
        // and remove later rows through both terminal paths before resuming.
        abort_multipart_upload_retrying_operation_aborted(
            client,
            &bucket,
            marker_key,
            &initial[2].1,
        )
        .await;
        let before_marker =
            create_multipart_upload_retrying_operation_aborted(client, &bucket, "b-new").await;
        let same_key_after_marker =
            create_multipart_upload_retrying_operation_aborted(client, &bucket, marker_key).await;
        let after_marker =
            create_multipart_upload_retrying_operation_aborted(client, &bucket, "p-new").await;

        let completed_part = upload_part_retrying_operation_aborted(
            client,
            &bucket,
            initial[4].0,
            &initial[4].1,
            1,
            b"completed-between-pages".to_vec(),
        )
        .await;
        complete_multipart_upload_retrying_operation_aborted(
            client,
            &bucket,
            initial[4].0,
            &initial[4].1,
            completed_part.e_tag().unwrap(),
        )
        .await;
        abort_multipart_upload_retrying_operation_aborted(
            client,
            &bucket,
            initial[5].0,
            &initial[5].1,
        )
        .await;

        let second = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .key_marker(&first_next_key_marker)
            .upload_id_marker(&first_next_upload_id_marker)
            .max_uploads(100)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let second_entries = second
            .uploads()
            .iter()
            .map(|upload| {
                (
                    upload.key().unwrap().to_string(),
                    upload.upload_id().unwrap().to_string(),
                )
            })
            .collect::<Vec<_>>();
        let second_is_truncated = second.is_truncated();
        let second_next_key_marker = second.next_key_marker().map(str::to_string);
        let second_next_upload_id_marker = second.next_upload_id_marker().map(str::to_string);

        let full = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let full_entries = full
            .uploads()
            .iter()
            .map(|upload| {
                (
                    upload.key().unwrap().to_string(),
                    upload.upload_id().unwrap().to_string(),
                )
            })
            .collect::<Vec<_>>();

        for (key, upload_id) in initial
            .iter()
            .map(|(key, upload_id)| (*key, upload_id.as_str()))
            .chain([
                ("b-new", before_marker.upload_id().unwrap()),
                (marker_key, same_key_after_marker.upload_id().unwrap()),
                ("p-new", after_marker.upload_id().unwrap()),
            ])
        {
            abort_multipart_upload_retrying_operation_aborted(client, &bucket, key, upload_id)
                .await;
        }
        cleanup(&bucket, &[initial[4].0]).await;

        assert_eq!(
            first_entries,
            [
                (initial[0].0.to_string(), initial[0].1.clone()),
                (initial[1].0.to_string(), initial[1].1.clone()),
                (initial[2].0.to_string(), initial[2].1.clone()),
            ]
        );
        assert_eq!(first.is_truncated(), Some(true));
        assert_eq!(first_next_key_marker, marker_key);
        assert_eq!(first_next_upload_id_marker, initial[2].1);

        assert_eq!(
            second_entries,
            [
                (marker_key.to_string(), initial[3].1.clone()),
                (
                    marker_key.to_string(),
                    same_key_after_marker.upload_id().unwrap().to_string(),
                ),
                (
                    "p-new".to_string(),
                    after_marker.upload_id().unwrap().to_string(),
                ),
                (initial[6].0.to_string(), initial[6].1.clone()),
            ]
        );
        assert_eq!(second_is_truncated, Some(false));
        assert_eq!(second_next_key_marker.as_deref(), Some(initial[6].0));
        assert_eq!(
            second_next_upload_id_marker.as_deref(),
            Some(initial[6].1.as_str())
        );

        assert_eq!(
            full_entries,
            [
                (initial[0].0.to_string(), initial[0].1.clone()),
                (
                    "b-new".to_string(),
                    before_marker.upload_id().unwrap().to_string(),
                ),
                (marker_key.to_string(), initial[1].1.clone()),
                (marker_key.to_string(), initial[3].1.clone()),
                (
                    marker_key.to_string(),
                    same_key_after_marker.upload_id().unwrap().to_string(),
                ),
                (
                    "p-new".to_string(),
                    after_marker.upload_id().unwrap().to_string(),
                ),
                (initial[6].0.to_string(), initial[6].1.clone()),
            ]
        );
    });
}

#[test]
fn test_list_multipart_uploads_stale_marker_precedes_recreated_same_key_upload() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "stale-marker-recreated-same-key";

        let marker = create_multipart_upload_retrying_operation_aborted(client, &bucket, key)
            .await
            .upload_id()
            .unwrap()
            .to_string();
        let first = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .prefix(key)
            .max_uploads(1)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let first_entries = first
            .uploads()
            .iter()
            .map(|upload| {
                (
                    upload.key().unwrap().to_string(),
                    upload.upload_id().unwrap().to_string(),
                )
            })
            .collect::<Vec<_>>();
        let key_marker = first.next_key_marker().unwrap().to_string();
        let upload_id_marker = first.next_upload_id_marker().unwrap().to_string();

        abort_multipart_upload_retrying_operation_aborted(client, &bucket, key, &marker).await;
        let replacement =
            create_multipart_upload_retrying_operation_aborted(client, &bucket, key).await;
        let replacement_id = replacement.upload_id().unwrap().to_string();

        let resumed = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .prefix(key)
            .key_marker(&key_marker)
            .upload_id_marker(&upload_id_marker)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let resumed_entries = resumed
            .uploads()
            .iter()
            .map(|upload| {
                (
                    upload.key().unwrap().to_string(),
                    upload.upload_id().unwrap().to_string(),
                )
            })
            .collect::<Vec<_>>();

        abort_multipart_upload_retrying_operation_aborted(client, &bucket, key, &replacement_id)
            .await;
        cleanup(&bucket, &[]).await;

        assert_eq!(first_entries, [(key.to_string(), marker.clone())]);
        assert_eq!(key_marker, key);
        assert_eq!(upload_id_marker, marker);
        assert_eq!(resumed_entries, [(key.to_string(), replacement_id)]);
        assert_eq!(resumed.is_truncated(), Some(false));
    });
}

#[test]
fn test_list_multipart_uploads_delimiter_pagination() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let mut created = Vec::new();
        for key in ["a-root", "dir/one", "dir/two", "z-root"] {
            let upload = client
                .create_multipart_upload()
                .bucket(&bucket)
                .key(key)
                .send_retrying_operation_aborted("S3 operation during multipart test")
                .await
                .unwrap();
            created.push((key, upload.upload_id().unwrap().to_string()));
        }

        let first = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .delimiter("/")
            .max_uploads(1)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(first.delimiter(), Some("/"));
        assert_eq!(first.uploads().len(), 1);
        assert_eq!(first.uploads()[0].key(), Some("a-root"));
        assert!(first.common_prefixes().is_empty());
        assert_eq!(first.is_truncated(), Some(true));
        assert_eq!(first.next_key_marker(), Some("a-root"));
        assert_eq!(first.next_upload_id_marker(), Some(created[0].1.as_str()));

        let second = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .delimiter("/")
            .key_marker(first.next_key_marker().unwrap())
            .upload_id_marker(first.next_upload_id_marker().unwrap())
            .max_uploads(1)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert!(second.uploads().is_empty());
        assert_eq!(second.common_prefixes().len(), 1);
        assert_eq!(second.common_prefixes()[0].prefix(), Some("dir/"));
        assert_eq!(second.is_truncated(), Some(true));
        assert_eq!(second.next_key_marker(), Some(""));
        assert_eq!(second.next_upload_id_marker(), Some(""));

        let returned_markers = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .delimiter("/")
            .key_marker(second.next_key_marker().unwrap())
            .upload_id_marker(second.next_upload_id_marker().unwrap())
            .max_uploads(1)
            .send()
            .await
            .unwrap();
        assert_eq!(returned_markers.key_marker(), Some(""));
        assert_eq!(returned_markers.upload_id_marker(), Some(""));
        assert_eq!(returned_markers.uploads().len(), 1);
        assert_eq!(returned_markers.uploads()[0].key(), Some("a-root"));
        assert!(returned_markers.common_prefixes().is_empty());
        assert_eq!(returned_markers.is_truncated(), Some(true));
        assert_eq!(returned_markers.next_key_marker(), Some("a-root"));
        assert_eq!(
            returned_markers.next_upload_id_marker(),
            Some(created[0].1.as_str())
        );

        let third = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .delimiter("/")
            .key_marker(second.common_prefixes()[0].prefix().unwrap())
            .max_uploads(1)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(third.uploads().len(), 1);
        assert_eq!(third.uploads()[0].key(), Some("z-root"));
        assert!(third.common_prefixes().is_empty());
        assert_eq!(third.is_truncated(), Some(false));
        assert_eq!(third.next_key_marker(), Some("z-root"));
        assert_eq!(third.next_upload_id_marker(), Some(created[3].1.as_str()));

        let full = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .delimiter("/")
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(
            full.uploads()
                .iter()
                .map(|upload| upload.key().unwrap())
                .collect::<Vec<_>>(),
            ["a-root", "z-root"]
        );
        assert_eq!(full.common_prefixes().len(), 1);
        assert_eq!(full.common_prefixes()[0].prefix(), Some("dir/"));
        assert_eq!(full.is_truncated(), Some(false));
        assert_eq!(full.next_key_marker(), Some("z-root"));
        assert_eq!(full.next_upload_id_marker(), Some(created[3].1.as_str()));

        let common_prefix_only = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .prefix("d")
            .delimiter("/")
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert!(common_prefix_only.uploads().is_empty());
        assert_eq!(common_prefix_only.common_prefixes().len(), 1);
        assert_eq!(
            common_prefix_only.common_prefixes()[0].prefix(),
            Some("dir/")
        );
        assert_eq!(common_prefix_only.is_truncated(), Some(false));
        assert_eq!(common_prefix_only.next_key_marker(), Some(""));
        assert_eq!(common_prefix_only.next_upload_id_marker(), Some(""));

        for (key, upload_id) in created {
            client
                .abort_multipart_upload()
                .bucket(&bucket)
                .key(key)
                .upload_id(upload_id)
                .send_retrying_operation_aborted("S3 operation during multipart test")
                .await
                .unwrap();
        }
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_max_uploads_above_aws_limit_is_clamped() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;

        let url = format!("{}/{bucket}?uploads&max-uploads=5000", CTX.endpoint());
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<MaxUploads>1000</MaxUploads>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            !response.body.contains("<MaxUploads>5000</MaxUploads>"),
            "unexpected body: {}",
            response.body
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_invalid_present_upload_id_marker_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let mut created = Vec::new();
        for key in ["same-key", "same-key", "same-key", "zzz"] {
            let create = client
                .create_multipart_upload()
                .bucket(&bucket)
                .key(key)
                .send_retrying_operation_aborted("S3 operation during multipart test")
                .await
                .unwrap();
            created.push((key.to_string(), create.upload_id().unwrap().to_string()));
        }

        let invalid_marker = "x".repeat(1025);
        let url = format!(
            "{}/{bucket}?uploads&key-marker={}&upload-id-marker={}",
            CTX.endpoint(),
            query_encode_value("same-key"),
            query_encode_value(&invalid_marker),
        );
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 400, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<Code>InvalidArgument</Code>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response
                .body
                .contains("<Message>Invalid uploadId marker</Message>"),
            "unexpected body: {}",
            response.body
        );

        for (key, upload_id) in created {
            client
                .abort_multipart_upload()
                .bucket(&bucket)
                .key(key)
                .upload_id(upload_id)
                .send_retrying_operation_aborted("S3 operation during multipart test")
                .await
                .unwrap();
        }
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_parts_parameter_wire_matrix_and_authorization_precedence() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "list-parts-parameter-matrix";
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("create upload for ListParts parameter matrix")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();
        upload_part_retrying_operation_aborted(
            client,
            &bucket,
            key,
            &upload_id,
            1,
            b"list parts canary".to_vec(),
        )
        .await;
        let encoded_upload_id = query_encode_value(&upload_id);

        let canary_url = object_url(
            CTX.endpoint(),
            &bucket,
            key,
            Some(&format!("uploadId={encoded_upload_id}")),
        );
        let canary = send_signed_request_with_credentials(
            "GET",
            &canary_url,
            b"",
            std::iter::empty::<(&str, &str)>(),
            primary_credentials(),
        );
        assert_eq!(canary.status, 200, "ListParts canary: {canary:?}");
        assert_eq!(
            xml_tag_text(&canary.body, "UploadId"),
            Some(upload_id.as_str())
        );
        assert_eq!(xml_tag_text(&canary.body, "PartNumber"), Some("1"));

        let denied_canary = send_signed_request_with_credentials(
            "GET",
            &canary_url,
            b"",
            std::iter::empty::<(&str, &str)>(),
            alt_credentials(),
        );
        assert_access_denied(&denied_canary);

        for (case, query, expected_marker, expected_max) in [
            (
                "empty numeric parameters use defaults",
                format!(
                    "uploadId={encoded_upload_id}&part-number-marker=&max-parts="
                ),
                "0",
                "1000",
            ),
            (
                "explicit plus is accepted",
                format!(
                    "uploadId={encoded_upload_id}&part-number-marker=%2B1&max-parts=%2B1"
                ),
                "1",
                "1",
            ),
            (
                "signed integer maximum is accepted and clamped",
                format!(
                    "uploadId={encoded_upload_id}&part-number-marker=2147483647&max-parts=2147483647"
                ),
                "2147483647",
                "1000",
            ),
            (
                "first duplicate numeric values win",
                format!(
                    "uploadId={encoded_upload_id}&part-number-marker=0&part-number-marker=abc&max-parts=1&max-parts=abc"
                ),
                "0",
                "1",
            ),
        ] {
            let url = object_url(CTX.endpoint(), &bucket, key, Some(&query));
            let response = send_signed_request_with_credentials(
                "GET",
                &url,
                b"",
                std::iter::empty::<(&str, &str)>(),
                primary_credentials(),
            );
            assert_eq!(response.status, 200, "{case}: {response:?}");
            assert_eq!(
                xml_tag_text(&response.body, "PartNumberMarker"),
                Some(expected_marker),
                "{case}: {response:?}"
            );
            assert_eq!(
                xml_tag_text(&response.body, "MaxParts"),
                Some(expected_max),
                "{case}: {response:?}"
            );
        }

        let malformed_cases = [
            (
                "max-parts wins over part-number-marker",
                format!("uploadId={encoded_upload_id}&part-number-marker=abc&max-parts=abc"),
                "Provided max-parts not an integer or within integer range",
                "max-parts",
                "abc",
            ),
            (
                "negative max-parts",
                format!("uploadId={encoded_upload_id}&max-parts=-1"),
                "Argument max-parts must be an integer between 0 and 2147483647",
                "max-parts",
                "-1",
            ),
            (
                "max-parts above signed integer range",
                format!("uploadId={encoded_upload_id}&max-parts=2147483648"),
                "Provided max-parts not an integer or within integer range",
                "max-parts",
                "2147483648",
            ),
            (
                "invalid first duplicate max-parts",
                format!("uploadId={encoded_upload_id}&max-parts=abc&max-parts=1"),
                "Provided max-parts not an integer or within integer range",
                "max-parts",
                "abc",
            ),
            (
                "part marker precedes upload lookup",
                "uploadId=invalid&part-number-marker=abc".to_string(),
                "Provided part-number-marker not an integer or within integer range",
                "part-number-marker",
                "abc",
            ),
            (
                "negative part-number-marker",
                format!("uploadId={encoded_upload_id}&part-number-marker=-1"),
                "Argument part-number-marker must be an integer between 0 and 2147483647",
                "part-number-marker",
                "-1",
            ),
            (
                "part-number-marker above signed integer range",
                format!("uploadId={encoded_upload_id}&part-number-marker=2147483648"),
                "Provided part-number-marker not an integer or within integer range",
                "part-number-marker",
                "2147483648",
            ),
            (
                "invalid first duplicate part-number-marker",
                format!("uploadId={encoded_upload_id}&part-number-marker=abc&part-number-marker=0"),
                "Provided part-number-marker not an integer or within integer range",
                "part-number-marker",
                "abc",
            ),
        ];
        for (case, query, message, argument_name, argument_value) in malformed_cases {
            let url = object_url(CTX.endpoint(), &bucket, key, Some(&query));
            for (principal, credentials) in [
                ("primary", primary_credentials()),
                ("alternate", alt_credentials()),
            ] {
                let response = send_signed_request_with_credentials(
                    "GET",
                    &url,
                    b"",
                    std::iter::empty::<(&str, &str)>(),
                    credentials,
                );
                assert_listing_invalid_argument(
                    &format!("ListParts {case} for {principal}"),
                    &response,
                    message,
                    argument_name,
                    argument_value,
                );
            }
        }

        let final_canary = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("final ListParts parameter-matrix canary")
            .await
            .unwrap();
        assert_eq!(final_canary.parts().len(), 1);
        assert_eq!(final_canary.parts()[0].part_number(), Some(1));

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send_retrying_operation_aborted("abort upload after ListParts parameter matrix")
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_parameter_wire_matrix_and_authorization_precedence() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "list-uploads-parameter-matrix";
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted(
                "create upload for ListMultipartUploads parameter matrix",
            )
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();
        let encoded_upload_id = query_encode_value(&upload_id);

        let canary_url = format!("{}/{bucket}?uploads=", CTX.endpoint());
        let canary = send_signed_request_with_credentials(
            "GET",
            &canary_url,
            b"",
            std::iter::empty::<(&str, &str)>(),
            primary_credentials(),
        );
        assert_eq!(
            canary.status, 200,
            "ListMultipartUploads canary: {canary:?}"
        );
        assert!(
            canary
                .body
                .contains(&format!("<UploadId>{upload_id}</UploadId>")),
            "ListMultipartUploads canary: {canary:?}"
        );

        let denied_canary = send_signed_request_with_credentials(
            "GET",
            &canary_url,
            b"",
            std::iter::empty::<(&str, &str)>(),
            alt_credentials(),
        );
        assert_access_denied(&denied_canary);

        for (
            case,
            query,
            expected_key_marker,
            expected_upload_marker,
            expected_max,
            expected_encoding,
            expected_prefix,
            expected_delimiter,
            lists_upload,
        ) in [
            (
                "empty max-uploads uses the default",
                "uploads=&max-uploads=".to_string(),
                "",
                "",
                "1000",
                None,
                None,
                None,
                true,
            ),
            (
                "explicit plus and first duplicate max-uploads are accepted",
                "uploads=&max-uploads=%2B1&max-uploads=abc".to_string(),
                "",
                "",
                "1",
                None,
                None,
                None,
                true,
            ),
            (
                "signed integer maximum is clamped",
                "uploads=&max-uploads=2147483647".to_string(),
                "",
                "",
                "1000",
                None,
                None,
                None,
                true,
            ),
            (
                "first duplicate encoding-type wins",
                "uploads=&encoding-type=url&encoding-type=invalid".to_string(),
                "",
                "",
                "1000",
                Some("url"),
                None,
                None,
                true,
            ),
            (
                "upload-id-marker without key-marker is ignored",
                "uploads=&upload-id-marker=invalid".to_string(),
                "",
                "",
                "1000",
                None,
                None,
                None,
                true,
            ),
            (
                "first duplicate upload-id-marker wins",
                format!(
                    "uploads=&key-marker={key}&upload-id-marker={encoded_upload_id}&upload-id-marker=invalid"
                ),
                key,
                upload_id.as_str(),
                "1000",
                None,
                None,
                None,
                false,
            ),
            (
                "first duplicate key-marker wins",
                "uploads=&key-marker=a&key-marker=z".to_string(),
                "a",
                "",
                "1000",
                None,
                None,
                None,
                true,
            ),
            (
                "first duplicate prefix wins",
                "uploads=&prefix=list-uploads&prefix=absent".to_string(),
                "",
                "",
                "1000",
                None,
                Some("list-uploads"),
                None,
                true,
            ),
            (
                "first duplicate delimiter wins",
                "uploads=&delimiter=/&delimiter=-".to_string(),
                "",
                "",
                "1000",
                None,
                None,
                Some("/"),
                true,
            ),
        ] {
            let url = format!("{}/{bucket}?{query}", CTX.endpoint());
            let response = send_signed_request_with_credentials(
                "GET",
                &url,
                b"",
                std::iter::empty::<(&str, &str)>(),
                primary_credentials(),
            );
            assert_eq!(response.status, 200, "{case}: {response:?}");
            assert_eq!(
                xml_tag_text(&response.body, "KeyMarker"),
                Some(expected_key_marker),
                "{case}: {response:?}"
            );
            assert_eq!(
                xml_tag_text(&response.body, "UploadIdMarker"),
                Some(expected_upload_marker),
                "{case}: {response:?}"
            );
            assert_eq!(
                xml_tag_text(&response.body, "MaxUploads"),
                Some(expected_max),
                "{case}: {response:?}"
            );
            assert_eq!(
                xml_tag_text(&response.body, "EncodingType"),
                expected_encoding,
                "{case}: {response:?}"
            );
            assert_eq!(
                xml_tag_text(&response.body, "Prefix"),
                expected_prefix,
                "{case}: {response:?}"
            );
            assert_eq!(
                xml_tag_text(&response.body, "Delimiter"),
                expected_delimiter,
                "{case}: {response:?}"
            );
            assert_eq!(
                response
                    .body
                    .contains(&format!("<UploadId>{upload_id}</UploadId>")),
                lists_upload,
                "{case}: {response:?}"
            );
        }

        let malformed_cases = [
            (
                "max-uploads wins over encoding-type",
                "uploads=&encoding-type=invalid&max-uploads=abc".to_string(),
                "Provided max-uploads not an integer or within integer range",
                "max-uploads",
                "abc",
            ),
            (
                "negative max-uploads",
                "uploads=&max-uploads=-1".to_string(),
                "Argument max-uploads must be an integer between 0 and 2147483647",
                "max-uploads",
                "-1",
            ),
            (
                "max-uploads above signed integer range",
                "uploads=&max-uploads=2147483648".to_string(),
                "Provided max-uploads not an integer or within integer range",
                "max-uploads",
                "2147483648",
            ),
            (
                "invalid first duplicate max-uploads",
                "uploads=&max-uploads=abc&max-uploads=1".to_string(),
                "Provided max-uploads not an integer or within integer range",
                "max-uploads",
                "abc",
            ),
            (
                "empty encoding-type",
                "uploads=&encoding-type=".to_string(),
                "Invalid Encoding Method specified in Request",
                "encoding-type",
                "",
            ),
            (
                "encoding-type wins over upload-id-marker",
                format!(
                    "uploads=&key-marker={key}&upload-id-marker=invalid&encoding-type=invalid"
                ),
                "Invalid Encoding Method specified in Request",
                "encoding-type",
                "invalid",
            ),
            (
                "invalid first duplicate encoding-type",
                "uploads=&encoding-type=invalid&encoding-type=url".to_string(),
                "Invalid Encoding Method specified in Request",
                "encoding-type",
                "invalid",
            ),
            (
                "invalid upload-id-marker with key-marker",
                format!("uploads=&key-marker={key}&upload-id-marker=invalid"),
                "Invalid uploadId marker",
                "upload-id-marker",
                "invalid",
            ),
            (
                "invalid first duplicate upload-id-marker",
                format!(
                    "uploads=&key-marker={key}&upload-id-marker=invalid&upload-id-marker={encoded_upload_id}"
                ),
                "Invalid uploadId marker",
                "upload-id-marker",
                "invalid",
            ),
        ];
        for (case, query, message, argument_name, argument_value) in malformed_cases {
            let url = format!("{}/{bucket}?{query}", CTX.endpoint());
            for (principal, credentials) in [
                ("primary", primary_credentials()),
                ("alternate", alt_credentials()),
            ] {
                let response = send_signed_request_with_credentials(
                    "GET",
                    &url,
                    b"",
                    std::iter::empty::<(&str, &str)>(),
                    credentials,
                );
                assert_listing_invalid_argument(
                    &format!("ListMultipartUploads {case} for {principal}"),
                    &response,
                    message,
                    argument_name,
                    argument_value,
                );
            }
        }

        let final_canary = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send_retrying_operation_aborted("final ListMultipartUploads parameter-matrix canary")
            .await
            .unwrap();
        assert_eq!(final_canary.uploads().len(), 1);
        assert_eq!(final_canary.uploads()[0].key(), Some(key));
        assert_eq!(
            final_canary.uploads()[0].upload_id(),
            Some(upload_id.as_str())
        );

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send_retrying_operation_aborted(
                "abort upload after ListMultipartUploads parameter matrix",
            )
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_encoding_type_url() {
    const KEY: &str = "multi part <>&\"+";

    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(KEY)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .encoding_type(EncodingType::Url)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(resp.encoding_type(), Some(&EncodingType::Url));
        assert_eq!(resp.uploads().len(), 1);
        let encoded_key = expected_url_list_value(KEY);
        assert_eq!(resp.uploads()[0].key(), Some(encoded_key.as_str()));

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(KEY)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_raw_response_encodes_key_fields() {
    const KEY: &str = "multi<>&\"";
    const PREFIX: &str = "multi<>&\"";

    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(KEY)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();
        let url = format!(
            "{}/{bucket}?uploads&encoding-type=url&prefix={}",
            CTX.endpoint(),
            query_encode_value(PREFIX)
        );
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<EncodingType>url</EncodingType>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains(&format!(
                "<Prefix>{}</Prefix>",
                expected_url_list_value(PREFIX)
            )),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response
                .body
                .contains(&format!("<Key>{}</Key>", expected_url_list_value(KEY))),
            "unexpected body: {}",
            response.body
        );

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(KEY)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_raw_response_without_encoding_type_keeps_raw_key_fields() {
    const KEY: &str = "multi<>&\"";
    const PREFIX: &str = "multi<>&\"";

    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(KEY)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let url = format!(
            "{}/{bucket}?uploads&prefix={}",
            CTX.endpoint(),
            query_encode_value(PREFIX)
        );
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            !response.body.contains("<EncodingType>url</EncodingType>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains(&format!(
                "<Prefix>{}</Prefix>",
                expected_raw_list_value(PREFIX)
            )),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response
                .body
                .contains(&format!("<Key>{}</Key>", expected_raw_list_value(KEY))),
            "unexpected body: {}",
            response.body
        );

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(KEY)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_raw_response_encodes_key_marker() {
    const FIRST_KEY: &str = "a<>&\"";
    const SECOND_KEY: &str = "b";

    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let create_first = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(FIRST_KEY)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let first_upload_id = create_first.upload_id().unwrap().to_string();

        let create_second = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(SECOND_KEY)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let second_upload_id = create_second.upload_id().unwrap().to_string();

        let url = format!(
            "{}/{bucket}?uploads&encoding-type=url&key-marker={}&max-uploads=1",
            CTX.endpoint(),
            query_encode_value(FIRST_KEY)
        );
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains(&format!(
                "<KeyMarker>{}</KeyMarker>",
                expected_url_list_value(FIRST_KEY)
            )),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains("<Key>b</Key>"),
            "unexpected body: {}",
            response.body
        );

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(FIRST_KEY)
            .upload_id(&first_upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(SECOND_KEY)
            .upload_id(&second_upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_raw_response_encodes_next_markers() {
    const FIRST_KEY: &str = "a<>&\"";
    const SECOND_KEY: &str = "b";

    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let create_first = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(FIRST_KEY)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let first_upload_id = create_first.upload_id().unwrap().to_string();

        let create_second = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(SECOND_KEY)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let second_upload_id = create_second.upload_id().unwrap().to_string();

        let url = format!(
            "{}/{bucket}?uploads&encoding-type=url&max-uploads=1",
            CTX.endpoint(),
        );
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains(&format!(
                "<NextKeyMarker>{}</NextKeyMarker>",
                expected_url_list_value(FIRST_KEY)
            )),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains(&format!(
                "<NextUploadIdMarker>{first_upload_id}</NextUploadIdMarker>"
            )),
            "unexpected body: {}",
            response.body
        );

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(FIRST_KEY)
            .upload_id(&first_upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(SECOND_KEY)
            .upload_id(&second_upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

// ── ListParts ───────────────────────────────────────────────────────

#[test]
fn test_list_parts() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "list-parts-key";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload two parts
        upload_part_retrying_operation_aborted(
            client,
            &bucket,
            key,
            upload_id,
            1,
            vec![b'a'; PART_SIZE],
        )
        .await;

        upload_part_retrying_operation_aborted(
            client,
            &bucket,
            key,
            upload_id,
            2,
            vec![b'b'; 1024],
        )
        .await;

        let resp = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let parts = resp.parts();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].part_number().unwrap(), 1);
        assert_eq!(parts[0].size().unwrap(), PART_SIZE as i64);
        assert_eq!(parts[1].part_number().unwrap(), 2);
        assert_eq!(parts[1].size().unwrap(), 1024);

        // Abort to clean up
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_parts_max_parts_above_aws_limit_is_clamped() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "list-parts-max-clamp";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();
        let encoded_upload_id = query_encode_value(&upload_id);

        let url = format!(
            "{}/{bucket}/{key}?uploadId={encoded_upload_id}&max-parts=5000",
            CTX.endpoint()
        );
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<MaxParts>1000</MaxParts>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            !response.body.contains("<MaxParts>5000</MaxParts>"),
            "unexpected body: {}",
            response.body
        );

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_parts_zero_max_parts() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "list-parts-zero-max";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        upload_part_retrying_operation_aborted(
            client,
            &bucket,
            key,
            upload_id,
            1,
            vec![b'a'; PART_SIZE],
        )
        .await;

        let resp = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .max_parts(0)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(resp.parts().len(), 0);
        assert_eq!(resp.is_truncated(), Some(false));
        assert_eq!(resp.next_part_number_marker(), Some("0"));

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_parts_pagination_with_checksums() {
    use aws_sdk_s3::types::ChecksumAlgorithm;

    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "list-parts-page";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let mut expected = Vec::new();
        for (part_number, data) in [(1, vec![b'a'; PART_SIZE]), (2, vec![b'b'; 1024])] {
            let resp = upload_part_with_crc32_retrying_operation_aborted(
                client,
                &bucket,
                key,
                &upload_id,
                part_number,
                data,
            )
            .await;
            expected.push((part_number, resp.checksum_crc32().unwrap().to_string()));
        }

        let resp1 = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .max_parts(1)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(resp1.parts().len(), 1);
        assert_eq!(resp1.parts()[0].part_number(), Some(1));
        assert_eq!(
            resp1.parts()[0].checksum_crc32(),
            Some(expected[0].1.as_str())
        );
        assert_eq!(resp1.checksum_algorithm(), Some(&ChecksumAlgorithm::Crc32));
        assert_eq!(resp1.is_truncated(), Some(true));
        let next_marker = resp1.next_part_number_marker().unwrap();

        let resp2 = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number_marker(next_marker)
            .max_parts(1)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(resp2.parts().len(), 1);
        assert_eq!(resp2.parts()[0].part_number(), Some(2));
        assert_eq!(
            resp2.parts()[0].checksum_crc32(),
            Some(expected[1].1.as_str())
        );
        assert_eq!(resp2.checksum_algorithm(), Some(&ChecksumAlgorithm::Crc32));
        assert_eq!(resp2.is_truncated(), Some(false));

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

// ── Error cases ─────────────────────────────────────────────────────

#[test]
fn test_list_parts_sparse_markers_ordering_and_overwrite() {
    use aws_sdk_s3::types::ChecksumAlgorithm;

    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "list-parts-sparse-overwrite";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let original = upload_part_with_crc32_retrying_operation_aborted(
            client,
            &bucket,
            key,
            &upload_id,
            2,
            b"original-part-two".to_vec(),
        )
        .await;
        let original_etag = original.e_tag().unwrap().to_string();
        let before_overwrite = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let original_last_modified = *before_overwrite.parts()[0]
            .last_modified()
            .expect("listed part should have LastModified");

        // ListParts timestamps have one-second precision. Ensure the replacement
        // is observably newer so the oracle can distinguish stale metadata.
        tokio::time::sleep(Duration::from_millis(1_100)).await;

        upload_part_with_crc32_retrying_operation_aborted(
            client,
            &bucket,
            key,
            &upload_id,
            10,
            b"part-ten".to_vec(),
        )
        .await;
        upload_part_with_crc32_retrying_operation_aborted(
            client,
            &bucket,
            key,
            &upload_id,
            7,
            b"part-seven".to_vec(),
        )
        .await;
        let replacement_body = b"replacement-part-two-is-longer".to_vec();
        let replacement = upload_part_with_crc32_retrying_operation_aborted(
            client,
            &bucket,
            key,
            &upload_id,
            2,
            replacement_body.clone(),
        )
        .await;
        let replacement_etag = replacement.e_tag().unwrap().to_string();
        let replacement_checksum = replacement.checksum_crc32().unwrap().to_string();
        assert_ne!(replacement_etag, original_etag);

        let all = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(all.part_number_marker(), Some("0"));
        assert_eq!(all.max_parts(), Some(1000));
        assert_eq!(all.is_truncated(), Some(false));
        assert_eq!(all.next_part_number_marker(), Some("10"));
        assert_eq!(
            all.parts()
                .iter()
                .map(|part| part.part_number().unwrap())
                .collect::<Vec<_>>(),
            [2, 7, 10]
        );

        let listed_replacement = &all.parts()[0];
        assert_eq!(listed_replacement.e_tag(), Some(replacement_etag.as_str()));
        assert_eq!(
            listed_replacement.size(),
            Some(replacement_body.len() as i64)
        );
        assert_eq!(
            listed_replacement.checksum_crc32(),
            Some(replacement_checksum.as_str())
        );
        assert!(
            listed_replacement
                .last_modified()
                .expect("listed replacement should have LastModified")
                .secs()
                > original_last_modified.secs()
        );
        assert!(all.parts()[1].checksum_crc32().is_some());
        assert!(all.parts()[2].checksum_crc32().is_some());
        assert_eq!(all.checksum_algorithm(), Some(&ChecksumAlgorithm::Crc32));

        let first_page = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .max_parts(1)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(first_page.parts()[0].part_number(), Some(2));
        assert_eq!(first_page.is_truncated(), Some(true));
        assert_eq!(first_page.next_part_number_marker(), Some("2"));

        let between_parts = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number_marker("3")
            .max_parts(1)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(between_parts.parts()[0].part_number(), Some(7));
        assert_eq!(between_parts.is_truncated(), Some(true));
        assert_eq!(between_parts.next_part_number_marker(), Some("7"));

        let exact_marker = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number_marker("7")
            .max_parts(1)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(exact_marker.parts()[0].part_number(), Some(10));
        assert_eq!(exact_marker.is_truncated(), Some(false));
        assert_eq!(exact_marker.next_part_number_marker(), Some("10"));

        let after_last = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number_marker("10")
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert!(after_last.parts().is_empty());
        assert_eq!(after_last.is_truncated(), Some(false));
        assert_eq!(after_last.next_part_number_marker(), Some("0"));

        let zero_after_marker = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number_marker("7")
            .max_parts(0)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert!(zero_after_marker.parts().is_empty());
        assert_eq!(zero_after_marker.is_truncated(), Some(false));
        assert_eq!(zero_after_marker.next_part_number_marker(), Some("0"));

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_complete_multipart_no_such_upload() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("nokey")
            .upload_id("nonexistent-upload-id")
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag("\"abc\"")
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        assert!(result.is_err());

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_abort_multipart_no_such_upload() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let result = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("nokey")
            .upload_id("nonexistent-upload-id")
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        // Our server returns NoSuchUpload for nonexistent upload IDs.
        s3_tests::assert_s3_err_code(&result, "NoSuchUpload");

        cleanup(&bucket, &[]).await;
    });
}

// ── Part size validation ────────────────────────────────────────────

#[test]
fn test_multipart_part_too_small() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "part-too-small";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload two parts, first one too small (< 5MB)
        let resp1 = upload_part_retrying_operation_aborted(
            client,
            &bucket,
            key,
            upload_id,
            1,
            vec![0u8; 100],
        )
        .await;

        let resp2 = upload_part_retrying_operation_aborted(
            client,
            &bucket,
            key,
            upload_id,
            2,
            vec![0u8; 100],
        )
        .await;

        // CompleteMultipartUpload should fail with EntityTooSmall
        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp1.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp2.e_tag().unwrap())
                            .part_number(2)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        assert!(result.is_err());

        // Abort to clean up
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_multipart_part_size_boundary_and_final_exception() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "part-size-boundary";
        let create = create_multipart_upload_retrying_operation_aborted(client, &bucket, key).await;
        let upload_id = create.upload_id().unwrap();

        let below_minimum = vec![b'a'; PART_SIZE - 1];
        let part_one = upload_part_retrying_operation_aborted(
            client,
            &bucket,
            key,
            upload_id,
            1,
            below_minimum.clone(),
        )
        .await;
        let part_two =
            upload_part_retrying_operation_aborted(client, &bucket, key, upload_id, 2, Vec::new())
                .await;

        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(part_one.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .parts(
                        CompletedPart::builder()
                            .e_tag(part_two.e_tag().unwrap())
                            .part_number(2)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("complete with a part one byte below the minimum")
            .await;
        assert_s3_err_code(&result, "EntityTooSmall");
        assert_multipart_parts_preserved(
            &bucket,
            key,
            upload_id,
            &[
                (1, (PART_SIZE - 1) as i64, part_one.e_tag().unwrap()),
                (2, 0, part_two.e_tag().unwrap()),
            ],
        )
        .await;

        // The same part is accepted when it is the last part selected for
        // completion. Parts uploaded after it need not be included.
        let complete = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(part_one.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("complete with a sub-minimum final part")
            .await
            .unwrap();
        assert_object_contents_and_etag(&bucket, key, complete.e_tag().unwrap(), &below_minimum)
            .await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_multipart_overwritten_part_size_controls_completion() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "overwritten-part-size";
        let create = create_multipart_upload_retrying_operation_aborted(client, &bucket, key).await;
        let upload_id = create.upload_id().unwrap();

        upload_part_retrying_operation_aborted(
            client,
            &bucket,
            key,
            upload_id,
            1,
            vec![b'a'; PART_SIZE],
        )
        .await;
        let zero_part_one =
            upload_part_retrying_operation_aborted(client, &bucket, key, upload_id, 1, Vec::new())
                .await;
        let zero_part_two =
            upload_part_retrying_operation_aborted(client, &bucket, key, upload_id, 2, Vec::new())
                .await;

        let completion = |part_one_etag: &str| {
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .e_tag(part_one_etag)
                        .part_number(1)
                        .build(),
                )
                .parts(
                    CompletedPart::builder()
                        .e_tag(zero_part_two.e_tag().unwrap())
                        .part_number(2)
                        .build(),
                )
                .build()
        };
        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(completion(zero_part_one.e_tag().unwrap()))
            .send_retrying_operation_aborted(
                "complete after overwriting a valid part with zero bytes",
            )
            .await;
        assert_s3_err_code(&result, "EntityTooSmall");
        assert_multipart_parts_preserved(
            &bucket,
            key,
            upload_id,
            &[
                (1, 0, zero_part_one.e_tag().unwrap()),
                (2, 0, zero_part_two.e_tag().unwrap()),
            ],
        )
        .await;

        let replacement = vec![b'b'; PART_SIZE];
        let replacement_part_one = upload_part_retrying_operation_aborted(
            client,
            &bucket,
            key,
            upload_id,
            1,
            replacement.clone(),
        )
        .await;
        let complete = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(completion(replacement_part_one.e_tag().unwrap()))
            .send_retrying_operation_aborted("complete after replacing a zero-byte non-final part")
            .await
            .unwrap();
        assert_object_contents_and_etag(&bucket, key, complete.e_tag().unwrap(), &replacement)
            .await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_complete_multipart_maximum_part_count() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let maximum_number_key = "maximum-part-number";
        let create =
            create_multipart_upload_retrying_operation_aborted(client, &bucket, maximum_number_key)
                .await;
        let maximum_number_upload_id = create.upload_id().unwrap();
        let maximum_number_part = upload_part_retrying_operation_aborted(
            client,
            &bucket,
            maximum_number_key,
            maximum_number_upload_id,
            10_000,
            b"maximum part number".to_vec(),
        )
        .await;
        let complete = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(maximum_number_key)
            .upload_id(maximum_number_upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(maximum_number_part.e_tag().unwrap())
                            .part_number(10_000)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("complete with only part number 10000")
            .await
            .unwrap();
        assert_object_contents_and_etag(
            &bucket,
            maximum_number_key,
            complete.e_tag().unwrap(),
            b"maximum part number",
        )
        .await;

        let key = "maximum-part-count";
        let create = create_multipart_upload_retrying_operation_aborted(client, &bucket, key).await;
        let upload_id = create.upload_id().unwrap();

        let completion_body = |part_count: u32| {
            let mut body = String::from("<CompleteMultipartUpload>");
            for part_number in 1..=part_count {
                body.push_str(&format!(
                    "<Part><PartNumber>{part_number}</PartNumber><ETag>\"00000000000000000000000000000000\"</ETag></Part>"
                ));
            }
            body.push_str("</CompleteMultipartUpload>");
            body
        };

        let at_limit = raw_complete_upload(&bucket, key, upload_id, &completion_body(10_000), &[]);
        assert!(
            at_limit.status == 400 || at_limit.status == 200,
            "at-limit response: {at_limit:?}"
        );
        assert_eq!(xml_tag_text(&at_limit.body, "Code"), Some("InvalidPart"));
        assert_eq!(xml_tag_text(&at_limit.body, "UploadId"), Some(upload_id));
        assert_eq!(
            xml_tag_text(&at_limit.body, "ETag"),
            Some("00000000000000000000000000000000")
        );
        let rejected_part = xml_tag_text(&at_limit.body, "PartNumber")
            .unwrap()
            .parse::<u32>()
            .unwrap();
        assert!((1..=10_000).contains(&rejected_part));

        let above_limit =
            raw_complete_upload(&bucket, key, upload_id, &completion_body(10_001), &[]);
        assert_shape(
            "CompleteMultipartUpload above maximum part count",
            &above_limit,
            &shape().status(400).headers(error_response_headers()).body(
                expected_error::invalid_argument_with_value_no_decl(
                    "The CompleteMultipartUpload reqeust contains for than 10000 parts.",
                    "CompleteMultipartUpload",
                    "CompleteMultipartUpload",
                ),
            ),
        );
        assert_multipart_parts_preserved(&bucket, key, upload_id, &[]).await;

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send_retrying_operation_aborted("abort maximum-part-count oracle upload")
            .await
            .unwrap();
        cleanup(&bucket, &[maximum_number_key]).await;
    });
}

#[test]
fn test_upload_part_invalid_part_number_exceeds_max() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "invalid-upload-part-number";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let result = upload_part_result_retrying_operation_aborted(
            client,
            &bucket,
            key,
            upload_id,
            10_001,
            b"hello".to_vec(),
        )
        .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidArgument");

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

// ── Overwrite existing object ───────────────────────────────────────

#[test]
fn test_multipart_overwrites_existing_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "overwrite-me";

        // Put a regular object first
        put_object_retrying_operation_aborted(client, &bucket, key, b"original".to_vec()).await;

        // Overwrite with multipart
        let new_data = vec![b'z'; 512];
        do_multipart_upload(&bucket, key, std::slice::from_ref(&new_data)).await;

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], &new_data[..]);

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_failed_multipart_completion_preserves_existing_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "failed-completion-preserves-existing";
        let original_body = b"original object".to_vec();
        let replacement_body = b"replacement object".to_vec();

        let original =
            put_object_retrying_operation_aborted(client, &bucket, key, original_body.clone())
                .await;

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();
        let uploaded_part = upload_part_retrying_operation_aborted(
            client,
            &bucket,
            key,
            upload_id,
            1,
            replacement_body.clone(),
        )
        .await;

        let rejected = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag("\"ffffffffffffffff\"")
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        assert_s3_err_code(&rejected, "InvalidPart");

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(get.e_tag(), original.e_tag());
        assert_eq!(
            get.body.collect().await.unwrap().into_bytes(),
            original_body
        );

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(uploaded_part.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(
            get.body.collect().await.unwrap().into_bytes(),
            replacement_body
        );

        cleanup(&bucket, &[key]).await;
    });
}

// ── HeadObject on multipart object ──────────────────────────────────

#[test]
fn test_multipart_head_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-head";

        let part1 = vec![b'h'; PART_SIZE];
        let part2 = vec![b'i'; 2048];
        do_multipart_upload(&bucket, key, &[part1, part2]).await;

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(resp.content_length().unwrap(), (PART_SIZE + 2048) as i64);

        cleanup(&bucket, &[key]).await;
    });
}

// ── Range read on multipart object ──────────────────────────────────

#[test]
fn test_multipart_range_read() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-range";

        let part1 = vec![b'A'; PART_SIZE];
        let part2 = vec![b'B'; 2048];
        do_multipart_upload(&bucket, key, &[part1, part2]).await;

        // Read across the part boundary
        let start = PART_SIZE - 10;
        let end = PART_SIZE + 9;
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .range(format!("bytes={}-{}", start, end))
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(data.len(), 20);
        assert!(data[..10].iter().all(|&b| b == b'A'));
        assert!(data[10..].iter().all(|&b| b == b'B'));

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_multipart_get_part_rejects_range_header() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-part-precedence";

        let part1 = vec![b'A'; PART_SIZE];
        let part2 = vec![b'B'; 2048];
        do_multipart_upload(&bucket, key, &[part1, part2.clone()]).await;

        let result = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .part_number(2)
            .range("bytes=0-1")
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup(&bucket, &[key]).await;
    });
}

// ── Multiple concurrent uploads for same key ────────────────────────

#[test]
fn test_complete_multipart_upload_racing_abort_is_serializable() {
    s3_tests::run(async {
        for attempt in 0..6 {
            let client = CTX.client();
            let bucket = setup_bucket().await;
            let key = "complete-abort-race";
            let (upload_id, etag) =
                create_single_part_upload(client, &bucket, key, 1, b"race body").await;

            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            let complete_task = spawn_barrier_single_part_completion(
                client.clone(),
                bucket.clone(),
                key,
                upload_id.clone(),
                1,
                etag.clone(),
                Arc::clone(&barrier),
            );

            let abort_client = client.clone();
            let abort_bucket = bucket.clone();
            let abort_upload_id = upload_id.clone();
            let abort_barrier = Arc::clone(&barrier);
            let abort_task = tokio::spawn(async move {
                abort_barrier.wait().await;
                if attempt % 2 != 0 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                abort_client
                    .abort_multipart_upload()
                    .bucket(abort_bucket)
                    .key(key)
                    .upload_id(abort_upload_id)
                    .send()
                    .await
            });

            let (complete, abort) = tokio::join!(complete_task, abort_task);
            let complete = complete.unwrap();
            let abort = abort.unwrap();
            abort.expect("raced AbortMultipartUpload should remain idempotently successful");
            let get = client.get_object().bucket(&bucket).key(key).send().await;
            let list = client
                .list_parts()
                .bucket(&bucket)
                .key(key)
                .upload_id(&upload_id)
                .send()
                .await;
            let retry =
                send_single_part_completion(client, &bucket, key, &upload_id, 1, &etag).await;

            assert_eq!(err_status(&list), 404);
            assert_s3_err_code(&list, "NoSuchUpload");

            match complete {
                Ok(completed) => {
                    let object = get.expect(
                        "a successful raced completion must leave its object visible after abort",
                    );
                    assert_eq!(
                        object.body.collect().await.unwrap().into_bytes().as_ref(),
                        b"race body"
                    );
                    let replay = retry.expect(
                        "an exact retry of the winning completion must remain successful after abort",
                    );
                    assert_eq!(replay.e_tag(), completed.e_tag());
                    cleanup(&bucket, &[key]).await;
                }
                Err(err) => {
                    assert_eq!(err.code(), Some("NoSuchUpload"));
                    assert_eq!(err_status(&get), 404);
                    assert_s3_err_code(&get, "NoSuchKey");
                    assert_eq!(err_status(&retry), 404);
                    assert_s3_err_code(&retry, "NoSuchUpload");
                    cleanup(&bucket, &[]).await;
                }
            }
        }
    });
}

#[test]
fn test_simultaneous_identical_complete_multipart_uploads_are_idempotent() {
    s3_tests::run(async {
        for attempt in 0..5 {
            let client = CTX.client();
            let bucket = setup_bucket().await;
            let key = "simultaneous-identical-complete";
            let (upload_id, etag) =
                create_single_part_upload(client, &bucket, key, 1, b"identical completion body")
                    .await;

            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            let spawn_completion = |barrier: Arc<tokio::sync::Barrier>| {
                spawn_barrier_single_part_completion(
                    client.clone(),
                    bucket.clone(),
                    key,
                    upload_id.clone(),
                    1,
                    etag.clone(),
                    barrier,
                )
            };
            let first = spawn_completion(Arc::clone(&barrier));
            let second = spawn_completion(barrier);
            let (first, second) = tokio::join!(first, second);
            let first = first.unwrap();
            let second = second.unwrap();
            let retry =
                send_single_part_completion(client, &bucket, key, &upload_id, 1, &etag).await;
            let first = first.unwrap_or_else(|err| {
                panic!("first identical completion attempt {attempt} failed: {err:?}")
            });
            let second = second.unwrap_or_else(|err| {
                panic!("second identical completion attempt {attempt} failed: {err:?}")
            });
            let retry = retry.unwrap_or_else(|err| {
                panic!("identical completion retry for attempt {attempt} failed: {err:?}")
            });
            assert_eq!(second.e_tag(), first.e_tag());
            assert_eq!(retry.e_tag(), first.e_tag());

            let object = client
                .get_object()
                .bucket(&bucket)
                .key(key)
                .send_retrying_operation_aborted("read simultaneously completed object")
                .await
                .unwrap();
            assert_eq!(object.e_tag(), first.e_tag());
            assert_eq!(
                object.body.collect().await.unwrap().into_bytes().as_ref(),
                b"identical completion body"
            );
            assert_list_parts_no_such_upload(&bucket, key, &upload_id).await;

            cleanup(&bucket, &[key]).await;
        }
    });
}

#[test]
fn test_simultaneous_different_complete_multipart_uploads_publish_one_manifest() {
    s3_tests::run(async {
        for attempt in 0..5 {
            let client = CTX.client();
            let bucket = setup_bucket().await;
            let key = "simultaneous-different-complete";
            let (upload_id, first_etag) =
                create_single_part_upload(client, &bucket, key, 1, b"first manifest").await;
            let second_part = upload_part_retrying_operation_aborted(
                client,
                &bucket,
                key,
                &upload_id,
                2,
                b"second manifest".to_vec(),
            )
            .await;
            let second_etag = second_part.e_tag().unwrap().to_string();

            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            let spawn_completion =
                |barrier: Arc<tokio::sync::Barrier>, part_number: i32, etag: String| {
                    spawn_barrier_single_part_completion(
                        client.clone(),
                        bucket.clone(),
                        key,
                        upload_id.clone(),
                        part_number,
                        etag,
                        barrier,
                    )
                };
            let first = spawn_completion(Arc::clone(&barrier), 1, first_etag.clone());
            let second = spawn_completion(barrier, 2, second_etag.clone());
            let (first, second) = tokio::join!(first, second);
            let first = first.unwrap();
            let second = second.unwrap();

            let object = client
                .get_object()
                .bucket(&bucket)
                .key(key)
                .send()
                .await
                .unwrap();
            let object_etag = object.e_tag().unwrap().to_string();
            let object_body = object.body.collect().await.unwrap().into_bytes();
            let first_retry =
                send_single_part_completion(client, &bucket, key, &upload_id, 1, &first_etag).await;
            let second_retry =
                send_single_part_completion(client, &bucket, key, &upload_id, 2, &second_etag)
                    .await;
            for completion in [&first, &second] {
                if completion.is_err() {
                    assert_eq!(err_status(completion), 404);
                    assert_s3_err_code(completion, "NoSuchUpload");
                }
            }

            match object_body.as_ref() {
                b"first manifest" => {
                    let first = first.unwrap_or_else(|err| {
                        panic!(
                            "the published first manifest in attempt {attempt} must have completed successfully: {err:?}"
                        )
                    });
                    let replay = first_retry.unwrap_or_else(|err| {
                        panic!(
                            "the published first manifest in attempt {attempt} must replay successfully: {err:?}"
                        )
                    });
                    assert_eq!(first.e_tag(), replay.e_tag());
                    assert_eq!(replay.e_tag(), Some(object_etag.as_str()));
                    assert_eq!(err_status(&second_retry), 404);
                    assert_s3_err_code(&second_retry, "NoSuchUpload");
                    if let Ok(second) = second {
                        assert!(second.e_tag().is_some());
                    }
                }
                b"second manifest" => {
                    let second = second.unwrap_or_else(|err| {
                        panic!(
                            "the published second manifest in attempt {attempt} must have completed successfully: {err:?}"
                        )
                    });
                    let replay = second_retry.unwrap_or_else(|err| {
                        panic!(
                            "the published second manifest in attempt {attempt} must replay successfully: {err:?}"
                        )
                    });
                    assert_eq!(second.e_tag(), replay.e_tag());
                    assert_eq!(replay.e_tag(), Some(object_etag.as_str()));
                    assert_eq!(err_status(&first_retry), 404);
                    assert_s3_err_code(&first_retry, "NoSuchUpload");
                    if let Ok(first) = first {
                        assert!(first.e_tag().is_some());
                    }
                }
                other => panic!(
                    "attempt {attempt} published bytes from neither competing manifest: {other:?}"
                ),
            }

            assert_list_parts_no_such_upload(&bucket, key, &upload_id).await;
            cleanup(&bucket, &[key]).await;
        }
    });
}

#[test]
fn test_upload_part_replacement_racing_completion_is_serializable() {
    s3_tests::run(async {
        for attempt in 0..10 {
            let client = CTX.client();
            let bucket = setup_bucket().await;
            let key = "upload-part-completion-race";
            let (upload_id, original_etag) =
                create_single_part_upload(client, &bucket, key, 1, b"original part").await;

            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            let spawn_replacement = |barrier: Arc<tokio::sync::Barrier>| {
                let client = client.clone();
                let bucket = bucket.clone();
                let upload_id = upload_id.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    client
                        .upload_part()
                        .bucket(bucket)
                        .key(key)
                        .upload_id(upload_id)
                        .part_number(1)
                        .body(ByteStream::from_static(b"replacement part"))
                        .send()
                        .await
                })
            };
            let spawn_completion = |barrier: Arc<tokio::sync::Barrier>| {
                spawn_barrier_single_part_completion(
                    client.clone(),
                    bucket.clone(),
                    key,
                    upload_id.clone(),
                    1,
                    original_etag.clone(),
                    barrier,
                )
            };
            let (replacement, completion) = if attempt % 2 == 0 {
                (
                    spawn_replacement(Arc::clone(&barrier)),
                    spawn_completion(barrier),
                )
            } else {
                let completion = spawn_completion(Arc::clone(&barrier));
                let replacement = spawn_replacement(barrier);
                (replacement, completion)
            };
            let (replacement, completion) = tokio::join!(replacement, completion);
            let replacement = replacement.unwrap();
            let completion = completion.unwrap();

            let get = client.get_object().bucket(&bucket).key(key).send().await;
            let get_state = match get {
                Ok(object) => Ok(object.body.collect().await.unwrap().into_bytes()),
                Err(err) => Err(err.code().map(str::to_string)),
            };
            let list = client
                .list_parts()
                .bucket(&bucket)
                .key(key)
                .upload_id(&upload_id)
                .send()
                .await;
            let retry =
                send_single_part_completion(client, &bucket, key, &upload_id, 1, &original_etag)
                    .await;

            match completion {
                Ok(completed) => {
                    assert_eq!(err_status(&replacement), 404);
                    assert_s3_err_code(&replacement, "NoSuchUpload");
                    assert_eq!(
                        get_state.unwrap_or_else(|err| panic!(
                            "completion-winning attempt {attempt} did not publish its object: {err:?}"
                        )),
                        Bytes::from_static(b"original part")
                    );
                    assert_eq!(err_status(&list), 404);
                    assert_s3_err_code(&list, "NoSuchUpload");
                    let replay = retry.unwrap_or_else(|err| {
                        panic!(
                            "completion-winning attempt {attempt} did not replay successfully: {err:?}"
                        )
                    });
                    assert_eq!(replay.e_tag(), completed.e_tag());
                    cleanup(&bucket, &[key]).await;
                }
                Err(err) => {
                    assert_eq!(err.code(), Some("InvalidPart"));
                    let replacement = replacement.unwrap_or_else(|err| {
                        panic!(
                            "replacement-winning attempt {attempt} did not return success: {err:?}"
                        )
                    });
                    assert_eq!(get_state, Err(Some("NoSuchKey".to_string())));
                    let listed = list.unwrap_or_else(|err| {
                        panic!(
                            "replacement-winning attempt {attempt} did not preserve the upload: {err:?}"
                        )
                    });
                    assert_eq!(listed.parts().len(), 1);
                    assert_eq!(listed.parts()[0].part_number(), Some(1));
                    assert_eq!(listed.parts()[0].e_tag(), replacement.e_tag());
                    assert_eq!(err_status(&retry), 400);
                    assert_s3_err_code(&retry, "InvalidPart");

                    let corrected = send_single_part_completion(
                        client,
                        &bucket,
                        key,
                        &upload_id,
                        1,
                        replacement.e_tag().unwrap(),
                    )
                    .await
                    .unwrap();
                    assert_object_contents_and_etag(
                        &bucket,
                        key,
                        corrected.e_tag().unwrap(),
                        b"replacement part",
                    )
                    .await;
                    cleanup(&bucket, &[key]).await;
                }
            }
        }
    });
}

#[test]
fn test_multipart_concurrent_uploads_same_key() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "concurrent";

        // Start two uploads for the same key
        let c1 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let uid1 = c1.upload_id().unwrap().to_string();

        let c2 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let uid2 = c2.upload_id().unwrap().to_string();
        assert_ne!(uid1, uid2);

        // Both should appear in listing
        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(resp.uploads().len(), 2);

        // Complete the first, abort the second
        let r1 =
            upload_part_retrying_operation_aborted(client, &bucket, key, &uid1, 1, vec![b'1'; 100])
                .await;

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&uid1)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(r1.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&uid2)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        // Only the completed upload's object should exist
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(data.len(), 100);
        assert!(data.iter().all(|&b| b == b'1'));

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_simultaneous_completions_of_distinct_uploads_to_same_key() {
    s3_tests::run(async {
        for attempt in 0..5 {
            let client = CTX.client();
            let bucket = setup_bucket().await;
            let key = "distinct-upload-completion-race";

            let (first_upload_id, first_etag) =
                create_single_part_upload(client, &bucket, key, 1, b"first upload").await;
            let (second_upload_id, second_etag) =
                create_single_part_upload(client, &bucket, key, 1, b"second upload").await;

            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            let spawn_completion =
                |barrier: Arc<tokio::sync::Barrier>, upload_id: String, etag: String| {
                    spawn_barrier_single_part_completion(
                        client.clone(),
                        bucket.clone(),
                        key,
                        upload_id,
                        1,
                        etag,
                        barrier,
                    )
                };
            let (first, second) = if attempt % 2 == 0 {
                let first = spawn_completion(
                    Arc::clone(&barrier),
                    first_upload_id.clone(),
                    first_etag.clone(),
                );
                let second =
                    spawn_completion(barrier, second_upload_id.clone(), second_etag.clone());
                (first, second)
            } else {
                let second = spawn_completion(
                    Arc::clone(&barrier),
                    second_upload_id.clone(),
                    second_etag.clone(),
                );
                let first = spawn_completion(barrier, first_upload_id.clone(), first_etag.clone());
                (first, second)
            };
            let (first, second) = tokio::join!(first, second);
            let first = first.unwrap();
            let second = second.unwrap();

            let object = client
                .get_object()
                .bucket(&bucket)
                .key(key)
                .send()
                .await
                .unwrap();
            let object_etag = object.e_tag().unwrap().to_string();
            let object_body = object.body.collect().await.unwrap().into_bytes();
            let first_retry =
                send_single_part_completion(client, &bucket, key, &first_upload_id, 1, &first_etag)
                    .await;
            let second_retry = send_single_part_completion(
                client,
                &bucket,
                key,
                &second_upload_id,
                1,
                &second_etag,
            )
            .await;
            let first = first.unwrap_or_else(|err| {
                panic!("first distinct upload completion attempt {attempt} failed: {err:?}")
            });
            let second = second.unwrap_or_else(|err| {
                panic!("second distinct upload completion attempt {attempt} failed: {err:?}")
            });

            match object_body.as_ref() {
                b"first upload" => {
                    let replay = first_retry.unwrap_or_else(|err| {
                        panic!(
                            "the current first upload in attempt {attempt} must replay successfully: {err:?}"
                        )
                    });
                    assert_eq!(first.e_tag(), replay.e_tag());
                    assert_eq!(replay.e_tag(), Some(object_etag.as_str()));
                    assert_eq!(err_status(&second_retry), 404);
                    assert_s3_err_code(&second_retry, "NoSuchUpload");
                    assert!(second.e_tag().is_some());
                }
                b"second upload" => {
                    let replay = second_retry.unwrap_or_else(|err| {
                        panic!(
                            "the current second upload in attempt {attempt} must replay successfully: {err:?}"
                        )
                    });
                    assert_eq!(second.e_tag(), replay.e_tag());
                    assert_eq!(replay.e_tag(), Some(object_etag.as_str()));
                    assert_eq!(err_status(&first_retry), 404);
                    assert_s3_err_code(&first_retry, "NoSuchUpload");
                    assert!(first.e_tag().is_some());
                }
                other => panic!(
                    "attempt {attempt} published bytes from neither distinct upload: {other:?}"
                ),
            }

            assert_list_parts_no_such_upload(&bucket, key, &first_upload_id).await;
            assert_list_parts_no_such_upload(&bucket, key, &second_upload_id).await;
            cleanup(&bucket, &[key]).await;
        }
    });
}

#[test]
fn test_complete_multipart_upload_accepts_matching_expected_object_size() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "mp-object-size-match";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let body = b"hello world";
        let part = upload_part_retrying_operation_aborted(
            client,
            &bucket,
            key,
            &upload_id,
            1,
            body.to_vec(),
        )
        .await;

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .mpu_object_size(body.len() as i64)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(part.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        let object = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(object.content_length(), Some(body.len() as i64));

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_complete_multipart_upload_rejects_mismatched_expected_object_size() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "mp-object-size-mismatch";
        let original_body = b"object before expected-size mismatch";
        let original =
            put_object_retrying_operation_aborted(client, &bucket, key, original_body.to_vec())
                .await;

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let body = b"hello world";
        let part = upload_part_retrying_operation_aborted(
            client,
            &bucket,
            key,
            &upload_id,
            1,
            body.to_vec(),
        )
        .await;

        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .mpu_object_size((body.len() as i64) + 1)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(part.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;

        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        assert_multipart_parts_preserved(
            &bucket,
            key,
            &upload_id,
            &[(1, body.len() as i64, part.e_tag().unwrap())],
        )
        .await;
        assert_object_contents_and_etag(&bucket, key, original.e_tag().unwrap(), original_body)
            .await;

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .mpu_object_size(body.len() as i64)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(part.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted(
                "retry multipart completion with corrected object size",
            )
            .await
            .unwrap();
        assert_list_parts_no_such_upload(&bucket, key, &upload_id).await;
        let completed = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("get object after corrected expected-size completion")
            .await
            .unwrap();
        assert_eq!(
            completed
                .body
                .collect()
                .await
                .unwrap()
                .into_bytes()
                .as_ref(),
            body
        );

        cleanup(&bucket, &[key]).await;
    });
}

// ── Part overwrite (re-upload same part number) ─────────────────────

#[test]
fn test_multipart_part_overwrite() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "part-overwrite";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload part 1 with data 'a'
        upload_part_retrying_operation_aborted(client, &bucket, key, upload_id, 1, vec![b'a'; 256])
            .await;

        // Re-upload part 1 with data 'b' — should replace
        let resp = upload_part_retrying_operation_aborted(
            client,
            &bucket,
            key,
            upload_id,
            1,
            vec![b'b'; 512],
        )
        .await;

        // Complete with the second upload's ETag
        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let data = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(data.len(), 512);
        assert!(data.iter().all(|&b| b == b'b'));

        cleanup(&bucket, &[key]).await;
    });
}

// ── Completion validation (Ceph parity) ─────────────────────────────

/// Ceph: test_multipart_upload_empty — completing with no parts should fail.
#[test]
fn test_multipart_complete_empty_parts() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "empty-complete";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(CompletedMultipartUpload::builder().build())
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        assert!(result.is_err());

        // Abort to clean up
        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        cleanup(&bucket, &[]).await;
    });
}

/// Ceph: test_multipart_upload_incorrect_etag — wrong ETag should fail with InvalidPart.
#[test]
fn test_multipart_complete_incorrect_etag() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "wrong-etag";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let part_body = vec![0u8; 256];
        let uploaded_part = upload_part_retrying_operation_aborted(
            client,
            &bucket,
            key,
            upload_id,
            1,
            part_body.clone(),
        )
        .await;

        // Complete with a fabricated ETag
        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag("\"ffffffffffffffff\"")
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        assert_s3_err_code(&result, "InvalidPart");

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        assert_eq!(
            err_status(&head),
            404,
            "unexpected HeadObject result: {head:?}"
        );

        let parts = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(parts.parts().len(), 1);
        assert_eq!(parts.parts()[0].part_number(), Some(1));
        assert_eq!(parts.parts()[0].e_tag(), uploaded_part.e_tag());

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(uploaded_part.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(get.body.collect().await.unwrap().into_bytes(), part_body);

        cleanup(&bucket, &[key]).await;
    });
}

/// Ceph: test_multipart_upload_missing_part — referencing an unuploaded part number.
#[test]
fn test_multipart_complete_missing_part() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "missing-part";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload part 1
        let resp = upload_part_retrying_operation_aborted(
            client,
            &bucket,
            key,
            upload_id,
            1,
            vec![0u8; 256],
        )
        .await;

        // Complete referencing part 9999 (never uploaded)
        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp.e_tag().unwrap())
                            .part_number(9999)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        assert_s3_err_code(&result, "InvalidPart");

        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        cleanup(&bucket, &[]).await;
    });
}

/// Ceph: test_multipart_upload — metadata and content-type survive multipart.
#[test]
fn test_multipart_metadata_preserved() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "meta-preserved";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .content_type("application/octet-stream")
            .metadata("testkey", "testvalue")
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let resp = upload_part_retrying_operation_aborted(
            client,
            &bucket,
            key,
            upload_id,
            1,
            vec![b'm'; 128],
        )
        .await;

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(head.content_type().unwrap(), "application/octet-stream");
        assert_eq!(
            head.metadata().unwrap().get("testkey").unwrap(),
            "testvalue"
        );

        cleanup(&bucket, &[key]).await;
    });
}

/// Parts must be in strictly ascending order.
#[test]
fn test_multipart_complete_invalid_order() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "invalid-order";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let r1 = upload_part_retrying_operation_aborted(
            client,
            &bucket,
            key,
            upload_id,
            1,
            vec![b'a'; PART_SIZE],
        )
        .await;

        let r2 = upload_part_retrying_operation_aborted(
            client,
            &bucket,
            key,
            upload_id,
            2,
            vec![b'b'; 256],
        )
        .await;

        // Complete with parts in reverse order (2, 1)
        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(r2.e_tag().unwrap())
                            .part_number(2)
                            .build(),
                    )
                    .parts(
                        CompletedPart::builder()
                            .e_tag(r1.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        assert_s3_err_code(&result, "InvalidPartOrder");

        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        cleanup(&bucket, &[]).await;
    });
}

/// Multipart ETag is a composite format: "hex-N".
#[test]
fn test_multipart_composite_etag() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "composite-etag";

        let part_data = vec![b'e'; 256];
        let etag = do_multipart_upload(&bucket, key, &[part_data]).await;
        // Multipart ETags have the format "hex-N" where N is part count
        assert!(
            etag.contains("-1"),
            "expected composite ETag with -1 suffix, got: {etag}"
        );

        cleanup(&bucket, &[key]).await;
    });
}

// ── Ceph parity: resend part ────────────────────────────────────────

/// Re-uploading a part before completion replaces the previous upload.
///
/// Matches Ceph: test_multipart_upload_resend_part
#[test]
fn test_multipart_upload_resend_part() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "resend-part";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload part 1 with data 'A'
        let data_a = vec![b'A'; PART_SIZE];
        let _resp_a =
            upload_part_retrying_operation_aborted(client, &bucket, key, upload_id, 1, data_a)
                .await;

        // Re-upload part 1 with data 'B' (replaces the first upload)
        let data_b = vec![b'B'; PART_SIZE];
        let resp_b = upload_part_retrying_operation_aborted(
            client,
            &bucket,
            key,
            upload_id,
            1,
            data_b.clone(),
        )
        .await;

        // Complete with the second ETag
        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp_b.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        // Verify the content is from the second upload
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.len(), PART_SIZE);
        assert!(body.iter().all(|&b| b == b'B'));

        cleanup(&bucket, &[key]).await;
    });
}

// ── Ceph parity: multiple sizes ─────────────────────────────────────

/// Multipart upload with various total sizes, covering all boundary
/// variants from the Ceph test.
///
/// Matches Ceph: test_multipart_upload_multiple_sizes
#[test]
fn test_multipart_upload_multiple_sizes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multi-sizes";
        let mb = 1024 * 1024;
        let kb = 1024;

        // Helper: upload with given total size split into 5MB parts + remainder
        async fn upload_and_check(
            client: &aws_sdk_s3::Client,
            bucket: &str,
            key: &str,
            total: usize,
        ) {
            let part_size = 5 * 1024 * 1024;
            let mut parts = Vec::new();
            let mut remaining = total;
            while remaining > 0 {
                let sz = remaining.min(part_size);
                parts.push(vec![b'x'; sz]);
                remaining -= sz;
            }
            do_multipart_upload(bucket, key, &parts).await;
            let head = client
                .head_object()
                .bucket(bucket)
                .key(key)
                .send_retrying_operation_aborted("S3 operation during multipart test")
                .await
                .unwrap();
            assert_eq!(
                head.content_length(),
                Some(total as i64),
                "size mismatch for {total} byte upload"
            );
        }

        // Ceph sizes: 5MB, 5MB+100KB, 5MB+600KB, 10MB+100KB, 10MB+600KB, 10MB
        for size in [
            5 * mb,
            5 * mb + 100 * kb,
            5 * mb + 600 * kb,
            10 * mb + 100 * kb,
            10 * mb + 600 * kb,
            10 * mb,
        ] {
            upload_and_check(client, &bucket, key, size).await;
        }

        cleanup(&bucket, &[key]).await;
    });
}

// ── PartNumber GET semantics ────────────────────────────────────────

#[test]
fn test_multipart_get_part() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "mymultipart";

        let part_sizes = [PART_SIZE, PART_SIZE, PART_SIZE, 1024 * 1024];
        let parts_data: Vec<Vec<u8>> = part_sizes
            .iter()
            .enumerate()
            .map(|(i, &sz)| vec![(i as u8) + b'A'; sz])
            .collect();

        let etag = do_multipart_upload(&bucket, key, &parts_data).await;
        let part_count = part_sizes.len() as i32;

        // HeadObject + GetObject for each valid part
        let mut data_offset = 0usize;
        for (i, data) in parts_data.iter().enumerate() {
            let pn = (i + 1) as i32;

            // HeadObject with partNumber
            let head = client
                .head_object()
                .bucket(&bucket)
                .key(key)
                .part_number(pn)
                .send_retrying_operation_aborted("S3 operation during multipart test")
                .await
                .unwrap();
            assert_eq!(
                head.parts_count(),
                Some(part_count),
                "PartsCount for part {pn}"
            );
            assert_eq!(
                head.e_tag().unwrap(),
                etag,
                "ETag mismatch on HEAD part {pn}"
            );

            // GetObject with partNumber
            let resp = client
                .get_object()
                .bucket(&bucket)
                .key(key)
                .part_number(pn)
                .send_retrying_operation_aborted("S3 operation during multipart test")
                .await
                .unwrap();
            assert_eq!(
                resp.parts_count(),
                Some(part_count),
                "PartsCount for GET part {pn}"
            );
            assert_eq!(
                resp.e_tag().unwrap(),
                etag,
                "ETag mismatch on GET part {pn}"
            );
            assert_eq!(
                resp.content_length(),
                Some(data.len() as i64),
                "ContentLength for part {pn}"
            );

            let body = resp.body.collect().await.unwrap().into_bytes();
            assert_eq!(&body[..], &data[..], "data mismatch for part {pn}");
            data_offset += data.len();
        }
        let _ = data_offset; // consumed all data

        // Out-of-range partNumber on GET → 416 Range Not Satisfiable (AWS behavior)
        let result = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .part_number(part_count + 1)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        assert_eq!(err_status(&result), 416);

        let raw = send_signed_request(
            "GET",
            &object_url(
                CTX.endpoint(),
                &bucket,
                key,
                Some(&format!("partNumber={}", part_count + 1)),
            ),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_eq!(raw.status, 416, "unexpected raw GET body: {}", raw.body);
        assert_invalid_part_number_body_shape(
            &raw.body,
            (part_count + 1) as u32,
            part_count as u32,
        );

        // Out-of-range partNumber on HEAD → same error
        let result = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .part_number(part_count + 1)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        assert_eq!(err_status(&result), 416);

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_non_multipart_get_part() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "singlepart";

        let resp =
            put_object_retrying_operation_aborted(client, &bucket, key, b"body".to_vec()).await;
        let etag = resp.e_tag().unwrap().to_string();

        // GET PartNumber > 1 → 416 InvalidPartNumber (AWS behavior)
        let result = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .part_number(2)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        assert_eq!(err_status(&result), 416);

        let raw = send_signed_request(
            "GET",
            &object_url(CTX.endpoint(), &bucket, key, Some("partNumber=2")),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_eq!(raw.status, 416, "unexpected raw GET body: {}", raw.body);
        assert_invalid_part_number_body_shape(&raw.body, 2, 1);

        // HEAD PartNumber > 1 → same error
        let result = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .part_number(2)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        assert_eq!(err_status(&result), 416);

        // PartNumber = 1 → returns entire object
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .part_number(1)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(resp.e_tag().unwrap(), etag);
        let body = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], b"body");

        cleanup(&bucket, &[key]).await;
    });
}

// ── Zero-byte final part with partNumber ────────────────────────────

#[test]
fn test_multipart_get_zero_byte_final_part() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "zerobyte-final";

        let part1 = vec![b'X'; PART_SIZE];
        let part2 = vec![]; // zero-byte final part
        let etag = do_multipart_upload(&bucket, key, &[part1.clone(), part2]).await;

        // GET partNumber=1 → normal data
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .part_number(1)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(resp.parts_count(), Some(2));
        assert_eq!(resp.e_tag().unwrap(), etag);
        let body = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.len(), PART_SIZE);

        // GET partNumber=2 → zero-byte part, should not panic
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .part_number(2)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(resp.parts_count(), Some(2));
        assert_eq!(resp.content_length(), Some(0));
        let body = resp.body.collect().await.unwrap().into_bytes();
        assert!(body.is_empty());

        // HEAD partNumber=2 → zero-byte
        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .part_number(2)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        assert_eq!(head.parts_count(), Some(2));
        assert_eq!(head.content_length(), Some(0));

        cleanup(&bucket, &[key]).await;
    });
}

// ── UploadPartCopy ──────────────────────────────────────────────────

#[test]
fn test_multipart_copy_small() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let src_key = "copy-src-small";
        let dst_key = "copy-dst-small";

        // Create source object
        let src_data = vec![b'x'; PART_SIZE];
        put_object_retrying_operation_aborted(client, &bucket, src_key, src_data.clone()).await;

        // Create multipart upload, upload_part_copy entire source as one part
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let copy_resp = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(1)
            .copy_source(format!("{}/{}", bucket, src_key))
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        let etag = copy_resp.copy_part_result().unwrap().e_tag().unwrap();

        // Complete multipart upload
        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(CompletedPart::builder().e_tag(etag).part_number(1).build())
                    .build(),
            )
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        // Verify GET returns correct data
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), &src_data[..]);

        cleanup(&bucket, &[src_key, dst_key]).await;
    });
}

#[test]
fn test_multipart_copy_without_range() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let src_key = "copy-src-no-range";
        let dst_key = "copy-dst-no-range";

        // Create source with known data
        let src_data = vec![b'A'; PART_SIZE + 1000];
        put_object_retrying_operation_aborted(client, &bucket, src_key, src_data.clone()).await;

        // UploadPartCopy without range copies full object
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let copy_resp = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(1)
            .copy_source(format!("{}/{}", bucket, src_key))
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        let etag = copy_resp.copy_part_result().unwrap().e_tag().unwrap();

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(CompletedPart::builder().e_tag(etag).part_number(1).build())
                    .build(),
            )
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.len(), src_data.len());
        assert_eq!(body.as_ref(), &src_data[..]);

        cleanup(&bucket, &[src_key, dst_key]).await;
    });
}

#[test]
fn test_multipart_copy_invalid_range() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let src_key = "copy-src-invalid-range";
        let dst_key = "copy-dst-invalid-range";

        // Create small source
        let src_data = vec![b'Z'; 1000];
        put_object_retrying_operation_aborted(client, &bucket, src_key, src_data).await;

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Range beyond source size → InvalidArgument (400)
        let result = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(1)
            .copy_source(format!("{}/{}", bucket, src_key))
            .copy_source_range("bytes=0-9999")
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        let status = err_status(&result);
        assert!(status == 400, "expected 400, got {status}");
        assert_s3_err_code(&result, "InvalidArgument");

        // Cleanup
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        cleanup(&bucket, &[src_key]).await;
    });
}

#[test]
fn test_multipart_copy_improper_range() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let src_key = "copy-src-improper";
        let dst_key = "copy-dst-improper";

        let src_data = vec![b'M'; 1000];
        put_object_retrying_operation_aborted(client, &bucket, src_key, src_data).await;

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // start > end → InvalidArgument (400)
        let result = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(1)
            .copy_source(format!("{}/{}", bucket, src_key))
            .copy_source_range("bytes=500-100")
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        let status = err_status(&result);
        assert!(status == 400, "expected 400, got {status}");
        assert_s3_err_code(&result, "InvalidArgument");

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        cleanup(&bucket, &[src_key]).await;
    });
}

#[test]
fn test_multipart_copy_invalid_part_number_exceeds_max() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let src_key = "copy-src-invalid-part-number";
        let dst_key = "copy-dst-invalid-part-number";

        put_object_retrying_operation_aborted(client, &bucket, src_key, b"hello".to_vec()).await;

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let result = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(10_001)
            .copy_source(format!("{}/{}", bucket, src_key))
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidArgument");

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        cleanup(&bucket, &[src_key]).await;
    });
}

#[test]
fn test_upload_part_copy_number_wire_matrix() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let src_key = "upload-part-copy-number-source";
        let dst_key = "upload-part-copy-number-matrix";
        put_object_retrying_operation_aborted(client, &bucket, src_key, b"copy body".to_vec())
            .await;
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("create multipart upload for copy part-number matrix")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();
        let copy_source = format!("{bucket}/{src_key}");

        for (case, query) in [
            ("UploadPartCopy missing uploadId", "partNumber=1"),
            ("UploadPartCopy empty uploadId", "partNumber=1&uploadId="),
        ] {
            let url = object_url(CTX.endpoint(), &bucket, dst_key, Some(query));
            let response = send_signed_request_with_credentials(
                "PUT",
                &url,
                b"",
                [("x-amz-copy-source", copy_source.as_str())],
                primary_credentials(),
            );
            assert_shape(
                case,
                &response,
                &shape().status(400).headers(error_response_headers()).body(
                    expected_error::invalid_argument_with_value_no_decl(
                        "This operation does not accept partNumber without uploadId",
                        "partNumber",
                        "partNumber",
                    ),
                ),
            );
        }

        let missing_url = object_url(
            CTX.endpoint(),
            &bucket,
            dst_key,
            Some(&format!("uploadId={upload_id}")),
        );
        let missing_response = send_signed_request_with_credentials(
            "PUT",
            &missing_url,
            b"",
            [("x-amz-copy-source", copy_source.as_str())],
            primary_credentials(),
        );
        assert_shape(
            "UploadPartCopy missing partNumber",
            &missing_response,
            &shape()
                .status(405)
                .headers(error_response_headers())
                .header("allow", "DELETE, POST, GET")
                .body(expected_error::put_multipart_upload_method_not_allowed()),
        );

        const INVALID_PART_NUMBER_MESSAGE: &str =
            "Part number must be an integer between 1 and 10000, inclusive";
        for (case, value, query) in [
            ("empty", "", format!("partNumber=&uploadId={upload_id}")),
            (
                "above maximum",
                "10001",
                format!("partNumber=10001&uploadId={upload_id}"),
            ),
            (
                "invalid first duplicate",
                "0",
                format!("partNumber=0&partNumber=3&uploadId={upload_id}"),
            ),
        ] {
            let url = object_url(CTX.endpoint(), &bucket, dst_key, Some(&query));
            let response = send_signed_request_with_credentials(
                "PUT",
                &url,
                b"",
                [("x-amz-copy-source", copy_source.as_str())],
                primary_credentials(),
            );
            assert_shape(
                case,
                &response,
                &shape().status(400).headers(error_response_headers()).body(
                    expected_error::invalid_argument_with_value_no_decl(
                        INVALID_PART_NUMBER_MESSAGE,
                        "partNumber",
                        value,
                    ),
                ),
            );
        }

        for query in [
            format!("partNumber=%2B1&uploadId={upload_id}"),
            format!("partNumber=2&partNumber=4&uploadId={upload_id}"),
            format!("partNumber=10000&uploadId={upload_id}"),
        ] {
            let url = object_url(CTX.endpoint(), &bucket, dst_key, Some(&query));
            let response = send_signed_request_with_credentials(
                "PUT",
                &url,
                b"",
                [("x-amz-copy-source", copy_source.as_str())],
                primary_credentials(),
            );
            assert_eq!(response.status, 200, "unexpected response: {response:?}");
            assert!(
                response.body.contains("<CopyPartResult"),
                "unexpected response: {response:?}"
            );
        }

        let listed = client
            .list_parts()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(&upload_id)
            .send_retrying_operation_aborted("list parts after copy part-number matrix")
            .await
            .unwrap();
        let stored_parts: Vec<_> = listed
            .parts()
            .iter()
            .map(|part| (part.part_number().unwrap(), part.size().unwrap()))
            .collect();
        assert_eq!(stored_parts, [(1, 9), (2, 9), (10_000, 9)]);

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("head destination after copy part-number matrix")
            .await;
        assert_eq!(
            err_status(&head),
            404,
            "UploadPartCopy matrix unexpectedly published an object: {head:?}"
        );

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .send_retrying_operation_aborted("abort multipart upload after copy part-number matrix")
            .await
            .unwrap();
        cleanup(&bucket, &[src_key]).await;
    });
}

#[test]
fn test_multipart_copy_special_names() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let src_key = "special key with spaces/and/slashes";
        let dst_key = "copy-dst-special";

        let src_data = vec![b'S'; PART_SIZE];
        put_object_retrying_operation_aborted(client, &bucket, src_key, src_data.clone()).await;

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let copy_resp = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(1)
            .copy_source(format!("{}/{}", bucket, src_key))
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        let etag = copy_resp.copy_part_result().unwrap().e_tag().unwrap();

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(CompletedPart::builder().e_tag(etag).part_number(1).build())
                    .build(),
            )
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), &src_data[..]);

        cleanup(&bucket, &[src_key, dst_key]).await;
    });
}

#[test]
fn test_multipart_copy_versioned() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        put_bucket_versioning_retrying_operation_aborted(
            client,
            &bucket,
            BucketVersioningStatus::Enabled,
        )
        .await;

        let src_key = "versioned-src";
        let dst_key = "versioned-dst";

        let data_v1 = vec![b'1'; PART_SIZE];
        let put1 =
            put_object_retrying_operation_aborted(client, &bucket, src_key, data_v1.clone()).await;
        let v1_id = put1.version_id().unwrap().to_string();

        let data_v2 = vec![b'2'; PART_SIZE];
        put_object_retrying_operation_aborted(client, &bucket, src_key, data_v2).await;

        let create =
            create_multipart_upload_retrying_operation_aborted(client, &bucket, dst_key).await;
        let upload_id = create.upload_id().unwrap();

        let copy_resp = upload_part_copy_retrying_operation_aborted(
            client,
            &bucket,
            dst_key,
            upload_id,
            copy_source_with_version(&bucket, src_key, &v1_id),
        )
        .await
        .unwrap();

        let etag = copy_resp.copy_part_result().unwrap().e_tag().unwrap();

        complete_multipart_upload_retrying_operation_aborted(
            client, &bucket, dst_key, upload_id, etag,
        )
        .await;

        // Verify we got version 1 data
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), &data_v1[..]);

        s3_tests::cleanup_versioned_bucket(client, &bucket).await;
    });
}

/// Copying from a delete-marker source should fail with 404/NoSuchKey.
#[test]
fn test_multipart_copy_delete_marker_source() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        put_bucket_versioning_retrying_operation_aborted(
            client,
            &bucket,
            BucketVersioningStatus::Enabled,
        )
        .await;

        let src_key = "delete-marker-src";
        let dst_key = "delete-marker-dst";

        put_object_retrying_operation_aborted(client, &bucket, src_key, vec![b'd'; PART_SIZE])
            .await;
        let delete = delete_object_retrying_operation_aborted(client, &bucket, src_key).await;
        assert!(delete.delete_marker().unwrap_or(false));

        let create =
            create_multipart_upload_retrying_operation_aborted(client, &bucket, dst_key).await;
        let upload_id = create.upload_id().unwrap();

        let result = upload_part_copy_retrying_operation_aborted(
            client,
            &bucket,
            dst_key,
            upload_id,
            format!("{}/{}", bucket, src_key),
        )
        .await;
        let status = err_status(&result);
        assert_eq!(status, 404);
        assert_s3_err_code(&result, "NoSuchKey");

        abort_multipart_upload_retrying_operation_aborted(client, &bucket, dst_key, upload_id)
            .await;
        s3_tests::cleanup_versioned_bucket(client, &bucket).await;
    });
}

/// UploadPartCopy targeting a specific delete-marker versionId should fail
/// with 400/InvalidRequest (not 404/NoSuchKey).
#[test]
fn test_multipart_copy_delete_marker_version_id() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        put_bucket_versioning_retrying_operation_aborted(
            client,
            &bucket,
            BucketVersioningStatus::Enabled,
        )
        .await;

        let src_key = "dm-vid-src";
        let dst_key = "dm-vid-dst";

        put_object_retrying_operation_aborted(client, &bucket, src_key, vec![b'd'; PART_SIZE])
            .await;
        let del = delete_object_retrying_operation_aborted(client, &bucket, src_key).await;
        assert!(del.delete_marker().unwrap_or(false));
        let dm_version_id = del.version_id().unwrap();

        let create =
            create_multipart_upload_retrying_operation_aborted(client, &bucket, dst_key).await;
        let upload_id = create.upload_id().unwrap();

        let result = upload_part_copy_retrying_operation_aborted(
            client,
            &bucket,
            dst_key,
            upload_id,
            copy_source_with_version(&bucket, src_key, dm_version_id),
        )
        .await;
        assert!(result.is_err());
        let status = err_status(&result);
        assert_eq!(status, 400);
        assert_s3_err_code(&result, "InvalidRequest");

        abort_multipart_upload_retrying_operation_aborted(client, &bucket, dst_key, upload_id)
            .await;
        s3_tests::cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_multipart_copy_multiple_sizes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let src_key = "copy-src-multi";
        let dst_key = "copy-dst-multi";

        // Create a source large enough for multiple range-copied parts
        let total_size = PART_SIZE * 2 + 500;
        let src_data: Vec<u8> = (0..total_size).map(|i| (i % 256) as u8).collect();
        put_object_retrying_operation_aborted(client, &bucket, src_key, src_data.clone()).await;

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Part 1: first PART_SIZE bytes
        let p1 = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(1)
            .copy_source(format!("{}/{}", bucket, src_key))
            .copy_source_range(format!("bytes=0-{}", PART_SIZE - 1))
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let etag1 = p1.copy_part_result().unwrap().e_tag().unwrap().to_string();

        // Part 2: next PART_SIZE bytes
        let p2 = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(2)
            .copy_source(format!("{}/{}", bucket, src_key))
            .copy_source_range(format!("bytes={}-{}", PART_SIZE, PART_SIZE * 2 - 1))
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let etag2 = p2.copy_part_result().unwrap().e_tag().unwrap().to_string();

        // Part 3: remaining 500 bytes
        let p3 = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(3)
            .copy_source(format!("{}/{}", bucket, src_key))
            .copy_source_range(format!("bytes={}-{}", PART_SIZE * 2, total_size - 1))
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let etag3 = p3.copy_part_result().unwrap().e_tag().unwrap().to_string();

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(&etag1)
                            .part_number(1)
                            .build(),
                    )
                    .parts(
                        CompletedPart::builder()
                            .e_tag(&etag2)
                            .part_number(2)
                            .build(),
                    )
                    .parts(
                        CompletedPart::builder()
                            .e_tag(&etag3)
                            .part_number(3)
                            .build(),
                    )
                    .build(),
            )
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();

        // Verify assembled object matches source
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.len(), total_size);
        assert_eq!(body.as_ref(), &src_data[..]);

        cleanup(&bucket, &[src_key, dst_key]).await;
    });
}

/// Ceph parity: put object with percent-encoded key (`anyfilename%25.txt` stores as
/// `anyfilename%.txt`), then attempt upload_part_copy using the raw `%` key. The
/// raw key resolves differently than the percent-encoded one, so the copy source
/// should not be found.
#[test]
fn test_upload_part_copy_percent_encoded_key() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let dst_key = "anyfile.txt";
        // This key contains a literal percent: "anyfilename%.txt"
        let encoded_key = "anyfilename%25.txt";
        let raw_key = "anyfilename%.txt";

        // Put the copy source under the percent-encoded key
        put_object_retrying_operation_aborted(client, &bucket, encoded_key, b"foo".to_vec()).await;

        // Put the destination object (initial state)
        put_object_retrying_operation_aborted(client, &bucket, dst_key, b"foo".to_vec()).await;

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Copy using raw_key ("anyfilename%.txt") which is NOT the same as
        // the percent-encoded key — this should fail with NoSuchKey / 404.
        let result = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(1)
            .copy_source(format!("{}/{}", bucket, raw_key))
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await;
        assert!(result.is_err(), "expected error copying with raw % key");

        // Verify the original destination object is untouched
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(dst_key)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"foo");

        // Cleanup
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .send_retrying_operation_aborted("S3 operation during multipart test")
            .await
            .unwrap();
        cleanup(&bucket, &[encoded_key, dst_key]).await;
    });
}

// ── Response shapes ─────────────────────────────────────────────────

fn raw_multipart_query(
    method: &str,
    bucket: &str,
    key: &str,
    query: &str,
    body: &[u8],
    headers: &[(&str, &str)],
) -> RawResponse {
    send_signed_request(
        method,
        &format!("{}/{}/{}?{}", CTX.endpoint(), bucket, key, query),
        body,
        headers.iter().copied(),
    )
}

fn raw_create_upload(bucket: &str, key: &str, headers: &[(&str, &str)]) -> (RawResponse, String) {
    let response = raw_multipart_query("POST", bucket, key, "uploads=", b"", headers);
    let upload_id = xml_tag_text(&response.body, "UploadId")
        .unwrap_or_else(|| panic!("create upload failed: {response:?}"))
        .to_string();
    (response, upload_id)
}

fn raw_upload_part(
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: u32,
    body: &[u8],
    headers: &[(&str, &str)],
) -> (RawResponse, String) {
    let response = raw_multipart_query(
        "PUT",
        bucket,
        key,
        &format!("partNumber={part_number}&uploadId={upload_id}"),
        body,
        headers,
    );
    let etag = s3_tests::shape::response_header_value(&response, "etag")
        .unwrap_or_else(|| panic!("upload part failed: {response:?}"))
        .to_string();
    (response, etag)
}

fn raw_complete_upload(
    bucket: &str,
    key: &str,
    upload_id: &str,
    body: &str,
    extra_headers: &[(&str, &str)],
) -> RawResponse {
    let mut headers = vec![("Content-Type", "application/xml")];
    headers.extend_from_slice(extra_headers);
    raw_multipart_query(
        "POST",
        bucket,
        key,
        &format!("uploadId={upload_id}"),
        body.as_bytes(),
        &headers,
    )
}

fn raw_complete_single_part_upload(
    bucket: &str,
    key: &str,
    part_body: &[u8],
) -> (String, String, RawResponse) {
    let (_, upload_id) = raw_create_upload(bucket, key, &[]);
    let (_, etag) = raw_upload_part(bucket, key, &upload_id, 1, part_body, &[]);
    let completion_body = single_part_complete_body(&etag);
    let completion = raw_complete_upload(bucket, key, &upload_id, &completion_body, &[]);
    assert_eq!(completion.status, 200, "initial completion: {completion:?}");
    (upload_id, completion_body, completion)
}

fn assert_terminal_completion_replay(
    bucket: &str,
    key: &str,
    upload_id: &str,
    completion_body: &str,
    expected: Option<(&str, Option<&str>)>,
    history: &str,
) {
    let replay = raw_complete_upload(bucket, key, upload_id, completion_body, &[]);
    let Some((expected_etag, expected_version_id)) = expected else {
        assert_eq!(replay.status, 404, "{history}: {replay:?}");
        assert_invalid_upload_id_no_such_upload(&replay, upload_id);
        return;
    };
    assert_eq!(replay.status, 200, "{history}: {replay:?}");
    assert_eq!(
        xml_tag_text(&replay.body, "ETag"),
        Some(expected_etag),
        "{history}: {replay:?}"
    );
    assert_eq!(
        s3_tests::shape::response_header_value(&replay, "x-amz-version-id"),
        expected_version_id,
        "{history}: {replay:?}"
    );
}

fn raw_abort_upload(bucket: &str, key: &str, upload_id: &str) {
    let response = raw_multipart_query(
        "DELETE",
        bucket,
        key,
        &format!("uploadId={upload_id}"),
        b"",
        &[],
    );
    assert_eq!(response.status, 204, "abort upload failed: {response:?}");
}

fn single_part_complete_body(etag: &str) -> String {
    format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part>\
         </CompleteMultipartUpload>"
    )
}

#[test]
fn test_complete_multipart_validation_precedence() {
    s3_tests::run(async {
        use base64::Engine;

        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let key = "complete-validation-precedence.txt";
        let original =
            put_object_retrying_operation_aborted(client, &bucket, key, b"existing".to_vec()).await;

        let (_, upload_id) =
            raw_create_upload(&bucket, key, &[("x-amz-checksum-algorithm", "SHA256")]);
        let part_body = [b'a'; 100];
        let part_checksum = base64::engine::general_purpose::STANDARD
            .encode(ring::digest::digest(&ring::digest::SHA256, &part_body).as_ref());
        let (_, etag_one) = raw_upload_part(
            &bucket,
            key,
            &upload_id,
            1,
            &part_body,
            &[("x-amz-checksum-sha256", &part_checksum)],
        );
        let (_, etag_two) = raw_upload_part(
            &bucket,
            key,
            &upload_id,
            2,
            &part_body,
            &[("x-amz-checksum-sha256", &part_checksum)],
        );
        let wrong_object_checksum = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);

        let cases = [
            (
                "invalid expected-size header before xml",
                "<".to_string(),
                vec![("x-amz-mp-object-size", "bad")],
            ),
            (
                "invalid checksum header versus xml",
                "<".to_string(),
                vec![("x-amz-checksum-sha256", "bad")],
            ),
            (
                "conflicting conditions versus xml",
                "<".to_string(),
                vec![("if-match", "*"), ("if-none-match", "*")],
            ),
            (
                "part order versus missing part",
                format!(
                    "<CompleteMultipartUpload>\
                     <Part><PartNumber>999</PartNumber><ETag>{etag_one}</ETag><ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part>\
                     <Part><PartNumber>2</PartNumber><ETag>{etag_two}</ETag><ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part>\
                     </CompleteMultipartUpload>"
                ),
                vec![],
            ),
            (
                "part order versus object checksum",
                format!(
                    "<CompleteMultipartUpload>\
                     <Part><PartNumber>2</PartNumber><ETag>{etag_two}</ETag><ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part>\
                     <Part><PartNumber>1</PartNumber><ETag>{etag_one}</ETag><ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part>\
                     </CompleteMultipartUpload>"
                ),
                vec![("x-amz-checksum-sha256", wrong_object_checksum.as_str())],
            ),
            (
                "missing part versus condition",
                format!(
                    "<CompleteMultipartUpload>\
                     <Part><PartNumber>999</PartNumber><ETag>{etag_one}</ETag><ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part>\
                     </CompleteMultipartUpload>"
                ),
                vec![("if-match", "\"wrong\"")],
            ),
            (
                "missing part versus object checksum",
                format!(
                    "<CompleteMultipartUpload>\
                     <Part><PartNumber>999</PartNumber><ETag>{etag_one}</ETag><ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part>\
                     </CompleteMultipartUpload>"
                ),
                vec![("x-amz-checksum-sha256", wrong_object_checksum.as_str())],
            ),
            (
                "object checksum versus etag",
                format!(
                    "<CompleteMultipartUpload>\
                     <Part><PartNumber>1</PartNumber><ETag>\"wrong\"</ETag><ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part>\
                     </CompleteMultipartUpload>"
                ),
                vec![("x-amz-checksum-sha256", wrong_object_checksum.as_str())],
            ),
            (
                "etag versus condition",
                format!(
                    "<CompleteMultipartUpload>\
                     <Part><PartNumber>1</PartNumber><ETag>\"wrong\"</ETag><ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part>\
                     </CompleteMultipartUpload>"
                ),
                vec![("if-match", "\"wrong\"")],
            ),
            (
                "etag versus missing checksum",
                "<CompleteMultipartUpload>\
                 <Part><PartNumber>1</PartNumber><ETag>\"wrong\"</ETag></Part>\
                 </CompleteMultipartUpload>"
                    .to_string(),
                vec![],
            ),
            (
                "missing checksum versus condition",
                format!(
                    "<CompleteMultipartUpload>\
                     <Part><PartNumber>1</PartNumber><ETag>{etag_one}</ETag></Part>\
                     </CompleteMultipartUpload>"
                ),
                vec![("if-match", "\"wrong\"")],
            ),
            (
                "missing checksum versus size",
                format!(
                    "<CompleteMultipartUpload>\
                     <Part><PartNumber>1</PartNumber><ETag>{etag_one}</ETag></Part>\
                     <Part><PartNumber>2</PartNumber><ETag>{etag_two}</ETag></Part>\
                     </CompleteMultipartUpload>"
                ),
                vec![],
            ),
            (
                "condition versus size",
                format!(
                    "<CompleteMultipartUpload>\
                     <Part><PartNumber>1</PartNumber><ETag>{etag_one}</ETag><ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part>\
                     <Part><PartNumber>2</PartNumber><ETag>{etag_two}</ETag><ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part>\
                     </CompleteMultipartUpload>"
                ),
                vec![("if-match", "\"wrong\"")],
            ),
            (
                "condition versus expected size",
                format!(
                    "<CompleteMultipartUpload>\
                     <Part><PartNumber>1</PartNumber><ETag>{etag_one}</ETag><ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part>\
                     </CompleteMultipartUpload>"
                ),
                vec![("if-match", "\"wrong\""), ("x-amz-mp-object-size", "999")],
            ),
            (
                "size versus expected size",
                format!(
                    "<CompleteMultipartUpload>\
                     <Part><PartNumber>1</PartNumber><ETag>{etag_one}</ETag><ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part>\
                     <Part><PartNumber>2</PartNumber><ETag>{etag_two}</ETag><ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part>\
                     </CompleteMultipartUpload>"
                ),
                vec![("x-amz-mp-object-size", "999")],
            ),
            (
                "expected size versus object checksum",
                format!(
                    "<CompleteMultipartUpload>\
                     <Part><PartNumber>1</PartNumber><ETag>{etag_one}</ETag><ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part>\
                     </CompleteMultipartUpload>"
                ),
                vec![
                    ("x-amz-mp-object-size", "999"),
                    ("x-amz-checksum-sha256", wrong_object_checksum.as_str()),
                ],
            ),
        ];

        for (name, body, headers) in cases {
            let response = raw_complete_upload(&bucket, key, &upload_id, &body, &headers);
            let (expected_status, expected_code, expected_message, may_be_embedded) = match name {
                "invalid expected-size header before xml" => (
                    400,
                    "InvalidRequest",
                    "Value for x-amz-mp-object-size header is invalid: 'bad'",
                    false,
                ),
                "invalid checksum header versus xml" => (
                    400,
                    "InvalidRequest",
                    "Value for x-amz-checksum-sha256 header is invalid.",
                    false,
                ),
                "conflicting conditions versus xml" => (
                    501,
                    "NotImplemented",
                    "A header you provided implies functionality that is not implemented",
                    false,
                ),
                "part order versus missing part" | "part order versus object checksum" => (
                    400,
                    "InvalidPartOrder",
                    "The list of parts was not in ascending order. Parts must be ordered by part number.",
                    true,
                ),
                "missing part versus condition" => (
                    400,
                    "InvalidPart",
                    "One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.",
                    true,
                ),
                "missing part versus object checksum"
                | "expected size versus object checksum" => (
                    400,
                    "BadDigest",
                    "The sha256 you specified did not match the calculated checksum.",
                    true,
                ),
                "object checksum versus etag" | "etag versus condition"
                | "etag versus missing checksum" => (
                    400,
                    "InvalidPart",
                    "One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.",
                    true,
                ),
                "missing checksum versus condition" | "missing checksum versus size" => (
                    400,
                    "InvalidRequest",
                    "The upload was created using a sha256 checksum. The complete request must include the checksum for each part. It was missing for part 1 in the request.",
                    true,
                ),
                "condition versus size" | "size versus expected size" => (
                    400,
                    "EntityTooSmall",
                    "Your proposed upload is smaller than the minimum allowed size",
                    true,
                ),
                "condition versus expected size" => (
                    400,
                    "InvalidRequest",
                    "The provided 'x-amz-mp-object-size' header value 999 does not match what was computed: 100",
                    true,
                ),
                _ => unreachable!("unhandled precedence case {name}"),
            };
            assert!(
                response.status == expected_status || (may_be_embedded && response.status == 200),
                "{name}: unexpected response: {response:?}"
            );
            assert_eq!(
                xml_tag_text(&response.body, "Code"),
                Some(expected_code),
                "{name}: {response:?}"
            );
            assert_eq!(
                xml_tag_text(&response.body, "Message"),
                Some(expected_message),
                "{name}: {response:?}"
            );
        }

        let invalid_upload_id = "a".repeat(1025);
        for (name, headers) in [
            (
                "invalid upload versus expected-size header",
                vec![("x-amz-mp-object-size", "bad")],
            ),
            (
                "invalid upload versus checksum header",
                vec![("x-amz-checksum-sha256", "bad")],
            ),
            (
                "invalid upload versus conflicting conditions",
                vec![("if-match", "*"), ("if-none-match", "*")],
            ),
        ] {
            let response = raw_complete_upload(&bucket, key, &invalid_upload_id, "<", &headers);
            let (expected_status, expected_code, expected_message) = if name
                == "invalid upload versus conflicting conditions"
            {
                (
                    501,
                    "NotImplemented",
                    "A header you provided implies functionality that is not implemented",
                )
            } else {
                (
                        404,
                        "NoSuchUpload",
                        "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.",
                    )
            };
            assert_eq!(response.status, expected_status, "{name}: {response:?}");
            assert_eq!(
                xml_tag_text(&response.body, "Code"),
                Some(expected_code),
                "{name}: {response:?}"
            );
            assert_eq!(
                xml_tag_text(&response.body, "Message"),
                Some(expected_message),
                "{name}: {response:?}"
            );
        }

        let checksum_algorithm_mismatch = raw_complete_upload(
            &bucket,
            key,
            &upload_id,
            &format!(
                "<CompleteMultipartUpload>\
                 <Part><PartNumber>1</PartNumber><ETag>{etag_one}</ETag></Part>\
                 </CompleteMultipartUpload>"
            ),
            &[("x-amz-checksum-crc32", "AAAAAA==")],
        );
        assert!(
            checksum_algorithm_mismatch.status == 400
                || checksum_algorithm_mismatch.status == 200,
            "checksum algorithm mismatch versus missing part checksum: {checksum_algorithm_mismatch:?}"
        );
        assert_eq!(
            xml_tag_text(&checksum_algorithm_mismatch.body, "Code"),
            Some("InvalidRequest")
        );
        assert_eq!(
            xml_tag_text(&checksum_algorithm_mismatch.body, "Message"),
            Some(
                "The upload was created using a sha256 checksum. The complete request must include the checksum for each part. It was missing for part 1 in the request."
            )
        );

        let checksum_part_number_gap = raw_complete_upload(
            &bucket,
            key,
            &upload_id,
            &format!(
                "<CompleteMultipartUpload>\
                 <Part><PartNumber>2</PartNumber><ETag>{etag_two}</ETag><ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part>\
                 </CompleteMultipartUpload>"
            ),
            &[],
        );
        match (
            checksum_part_number_gap.status,
            xml_tag_text(&checksum_part_number_gap.body, "Code"),
        ) {
            (500 | 200, Some("InternalError")) => assert_eq!(
                xml_tag_text(&checksum_part_number_gap.body, "Message"),
                Some("We encountered an internal error. Please try again.")
            ),
            (400, Some("InvalidRequest")) => assert_eq!(
                xml_tag_text(&checksum_part_number_gap.body, "Message"),
                Some("Part numbers must be consecutive and begin with 1 when a checksum is used.")
            ),
            result => {
                panic!("checksum part-number gap returned {result:?}: {checksum_part_number_gap:?}")
            }
        }

        assert_multipart_parts_preserved(
            &bucket,
            key,
            &upload_id,
            &[
                (1, part_body.len() as i64, &etag_one),
                (2, part_body.len() as i64, &etag_two),
            ],
        )
        .await;
        assert_object_contents_and_etag(&bucket, key, original.e_tag().unwrap(), b"existing").await;

        let corrected = raw_complete_upload(
            &bucket,
            key,
            &upload_id,
            &format!(
                "<CompleteMultipartUpload>\
                 <Part><PartNumber>1</PartNumber><ETag>{etag_one}</ETag><ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part>\
                 </CompleteMultipartUpload>"
            ),
            &[],
        );
        assert_eq!(corrected.status, 200, "corrected completion: {corrected:?}");
        assert_list_parts_no_such_upload(&bucket, key, &upload_id).await;
        let completed_etag = xml_tag_text(&corrected.body, "ETag").unwrap();
        assert_object_contents_and_etag(&bucket, key, completed_etag, &part_body).await;
        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_multipart_flow_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let key = "shape-multipart.txt";

        let (create, upload_id) =
            raw_create_upload(&bucket, key, &[("x-amz-checksum-algorithm", "CRC64NVME")]);
        assert_shape(
            "CreateMultipartUpload shape",
            &create,
            &shape()
                .status(200)
                .headers([
                    ("x-amz-checksum-algorithm", "CRC64NVME"),
                    ("x-amz-checksum-type", "FULL_OBJECT"),
                    ("x-amz-server-side-encryption", "AES256"),
                    ("x-amz-request-id", "{request_id}"),
                    ("x-amz-id-2", "{host_id}"),
                ])
                .body(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <InitiateMultipartUploadResult \
                     xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>{bucket}</Bucket>\
                     <Key>{key}</Key><UploadId>{upload_id}</UploadId>\
                     </InitiateMultipartUploadResult>",
                )
                .sub("bucket", bucket.as_str())
                .sub("key", key)
                .sub("upload_id", upload_id.as_str()),
        );

        let (part, part_etag) =
            raw_upload_part(&bucket, key, &upload_id, 1, b"multipart-part-body", &[]);
        assert_shape(
            "UploadPart shape",
            &part,
            &shape()
                .status(200)
                .headers([
                    ("etag", "{etag}"),
                    ("x-amz-checksum-crc64nvme", "fE1y/S5sY5Y="),
                    ("x-amz-server-side-encryption", "AES256"),
                    ("x-amz-request-id", "{request_id}"),
                    ("x-amz-id-2", "{host_id}"),
                ])
                .body_empty(),
        );

        let list_parts = raw_multipart_query(
            "GET",
            &bucket,
            key,
            &format!("uploadId={upload_id}"),
            b"",
            &[],
        );
        assert_shape(
            "ListParts shape",
            &list_parts,
            &shape()
                .status(200)
                .headers([
                    ("content-type", "application/xml"),
                    ("x-amz-request-id", "{request_id}"),
                    ("x-amz-id-2", "{host_id}"),
                ])
                .body(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListPartsResult \
                     xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>{bucket}</Bucket>\
                     <Key>{key}</Key><UploadId>{upload_id}</UploadId>\
                     <Initiator><ID>{any}</ID><DisplayName>{any}</DisplayName></Initiator>\
                     <Owner><ID>{owner_id}</ID></Owner>\
                     <StorageClass>STANDARD</StorageClass>\
                     <ChecksumAlgorithm>CRC64NVME</ChecksumAlgorithm>\
                     <ChecksumType>FULL_OBJECT</ChecksumType>\
                     <PartNumberMarker>0</PartNumberMarker>\
                     <NextPartNumberMarker>1</NextPartNumberMarker>\
                     <MaxParts>1000</MaxParts><IsTruncated>false</IsTruncated>\
                     <Part><PartNumber>1</PartNumber><LastModified>{iso8601}</LastModified>\
                     <ETag>{part_etag_escaped}</ETag><Size>19</Size>\
                     <ChecksumCRC64NVME>fE1y/S5sY5Y=</ChecksumCRC64NVME></Part></ListPartsResult>",
                )
                .sub("bucket", bucket.as_str())
                .sub("key", key)
                .sub("upload_id", upload_id.as_str())
                .sub("part_etag_escaped", part_etag.replace('"', "&quot;")),
        );

        let complete = raw_complete_upload(
            &bucket,
            key,
            &upload_id,
            &single_part_complete_body(&part_etag),
            &[],
        );
        // AWS may pad the completion body with whitespace after the XML
        // declaration while the completion is in progress; {ws} accepts it.
        assert_shape(
            "CompleteMultipartUpload shape",
            &complete,
            &shape()
                .status(200)
                .headers([
                    ("content-type", "application/xml"),
                    ("x-amz-server-side-encryption", "AES256"),
                    ("x-amz-request-id", "{request_id}"),
                    ("x-amz-id-2", "{host_id}"),
                ])
                .body(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{ws}\
                     <CompleteMultipartUploadResult \
                     xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                     <Location>https://s3.{region}.amazonaws.com/{bucket}/{key}</Location>\
                     <Bucket>{bucket}</Bucket><Key>{key}</Key><ETag>{etag}</ETag>\
                     <ChecksumCRC64NVME>fE1y/S5sY5Y=</ChecksumCRC64NVME>\
                     <ChecksumType>FULL_OBJECT</ChecksumType>\
                     </CompleteMultipartUploadResult>",
                )
                .sub("region", CTX.region())
                .sub("bucket", bucket.as_str())
                .sub("key", key),
        );

        s3_tests::delete_object_retrying_operation_aborted(client, &bucket, key)
            .await
            .expect("delete multipart shape fixture");
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_object_part_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let key = "shape-object-part.txt";
        let part_one = vec![b'A'; 5 * 1024 * 1024];

        let (_, upload_id) = raw_create_upload(
            &bucket,
            key,
            &[("Content-Type", "application/octet-stream")],
        );
        let (_, etag_one) = raw_upload_part(&bucket, key, &upload_id, 1, &part_one, &[]);
        let (_, etag_two) =
            raw_upload_part(&bucket, key, &upload_id, 2, b"second-multipart-part", &[]);
        let complete = raw_complete_upload(
            &bucket,
            key,
            &upload_id,
            &format!(
                "<CompleteMultipartUpload>\
                 <Part><PartNumber>1</PartNumber><ETag>{etag_one}</ETag></Part>\
                 <Part><PartNumber>2</PartNumber><ETag>{etag_two}</ETag></Part>\
                 </CompleteMultipartUpload>"
            ),
            &[],
        );
        assert_eq!(complete.status, 200, "part fixture complete: {complete:?}");

        let part_headers = [
            ("etag", "{etag}"),
            ("content-length", "5242880"),
            ("last-modified", "{http_date}"),
            ("accept-ranges", "bytes"),
            ("x-amz-mp-parts-count", "2"),
            ("content-range", "bytes 0-5242879/5242901"),
            ("content-type", "application/octet-stream"),
            ("x-amz-server-side-encryption", "AES256"),
            ("x-amz-request-id", "{request_id}"),
            ("x-amz-id-2", "{host_id}"),
        ];
        let head = raw_object_query("HEAD", &bucket, key, "partNumber=1");
        let head_captures = assert_shape(
            "HeadObject partNumber shape",
            &head,
            &shape().status(206).headers(part_headers).body_empty(),
        );
        let get = raw_object_query("GET", &bucket, key, "partNumber=1");
        let get_captures = assert_shape(
            "GetObject partNumber shape",
            &get,
            &shape()
                .status(206)
                .headers(part_headers)
                .body("A".repeat(5 * 1024 * 1024)),
        );
        assert_eq!(head_captures["etag"], get_captures["etag"]);

        s3_tests::delete_object_retrying_operation_aborted(client, &bucket, key)
            .await
            .expect("delete part shape fixture");
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_upload_part_copy_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let src_key = "shape-upload-part-copy-src.txt";
        let dst_key = "shape-upload-part-copy-dst.txt";

        s3_tests::raw_object_with(
            "PUT",
            &bucket,
            src_key,
            b"upload-part-copy-source-body",
            &[],
        );
        let (_, upload_id) = raw_create_upload(&bucket, dst_key, &[]);

        let copy = raw_multipart_query(
            "PUT",
            &bucket,
            dst_key,
            &format!("partNumber=1&uploadId={upload_id}"),
            b"",
            &[("x-amz-copy-source", &format!("{bucket}/{src_key}"))],
        );
        assert_shape(
            "UploadPartCopy shape",
            &copy,
            &shape()
                .status(200)
                .headers([
                    ("content-type", "application/xml"),
                    ("content-length", "{any}"),
                    ("x-amz-server-side-encryption", "AES256"),
                    ("x-amz-request-id", "{request_id}"),
                    ("x-amz-id-2", "{host_id}"),
                ])
                .body(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CopyPartResult \
                     xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                     <LastModified>{iso8601}</LastModified><ETag>{etag}</ETag></CopyPartResult>",
                ),
        );

        raw_abort_upload(&bucket, dst_key, &upload_id);
        s3_tests::delete_object_retrying_operation_aborted(client, &bucket, src_key)
            .await
            .expect("delete copy source fixture");
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_upload_part_copy_rejects_managed_encryption_request_headers() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let src_key = "upload-part-copy-read-header-src.txt";
        let dst_key = "upload-part-copy-read-header-dst.txt";

        s3_tests::raw_object_with("PUT", &bucket, src_key, b"upload part copy source", &[]);
        let (_, upload_id) = raw_create_upload(&bucket, dst_key, &[]);

        let invalid_sse_body = expected_error::invalid_argument_with_value_no_decl(
            "x-amz-server-side-encryption header is not supported for this operation.",
            "x-amz-server-side-encryption",
            "AES256",
        );
        let sse_response = raw_multipart_query(
            "PUT",
            &bucket,
            dst_key,
            &format!("partNumber=1&uploadId={upload_id}"),
            b"",
            &[
                ("x-amz-copy-source", &format!("{bucket}/{src_key}")),
                ("x-amz-server-side-encryption", "AES256"),
            ],
        );
        assert_shape(
            "UploadPartCopy rejects x-amz-server-side-encryption request header",
            &sse_response,
            &shape()
                .status(400)
                .headers(error_response_headers())
                .body(invalid_sse_body.as_str()),
        );

        let invalid_kms_key_body = expected_error::invalid_argument_no_decl(
            "Server Side Encryption with AWS KMS managed key requires HTTP header x-amz-server-side-encryption : aws:kms",
            "x-amz-server-side-encryption",
        );
        let kms_key_response = raw_multipart_query(
            "PUT",
            &bucket,
            dst_key,
            &format!("partNumber=1&uploadId={upload_id}"),
            b"",
            &[
                ("x-amz-copy-source", &format!("{bucket}/{src_key}")),
                (
                    "x-amz-server-side-encryption-aws-kms-key-id",
                    "arn:aws:kms:us-east-1:111122223333:key/example",
                ),
            ],
        );
        assert_shape(
            "UploadPartCopy rejects x-amz-server-side-encryption-aws-kms-key-id request header",
            &kms_key_response,
            &shape()
                .status(400)
                .headers(error_response_headers())
                .body(invalid_kms_key_body.as_str()),
        );

        raw_abort_upload(&bucket, dst_key, &upload_id);
        s3_tests::delete_object_retrying_operation_aborted(client, &bucket, src_key)
            .await
            .expect("delete upload-part-copy source fixture");
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_complete_multipart_no_such_upload_error_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let key = "shape-complete-no-such-upload.txt";
        let (_, upload_id) = raw_create_upload(&bucket, key, &[]);
        raw_abort_upload(&bucket, key, &upload_id);

        let response = raw_complete_upload(
            &bucket,
            key,
            &upload_id,
            &single_part_complete_body("\"ffffffffffffffff\""),
            &[],
        );
        assert_shape(
            "CompleteMultipartUpload NoSuchUpload",
            &response,
            &shape().status(404).headers(error_response_headers()).body(
                expected_error::complete_multipart_no_such_upload(&upload_id),
            ),
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_complete_multipart_upload_rejects_managed_encryption_request_headers() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let key = "complete-read-header.txt";

        let (_, upload_id) = raw_create_upload(&bucket, key, &[]);
        let (_, etag) = raw_upload_part(&bucket, key, &upload_id, 1, b"complete part body", &[]);
        let complete_body = single_part_complete_body(&etag);

        let invalid_sse_body = expected_error::invalid_argument_with_value_no_decl(
            "x-amz-server-side-encryption header is not supported for this operation.",
            "x-amz-server-side-encryption",
            "AES256",
        );
        let sse_response = raw_complete_upload(
            &bucket,
            key,
            &upload_id,
            &complete_body,
            &[("x-amz-server-side-encryption", "AES256")],
        );
        assert_shape(
            "CompleteMultipartUpload rejects x-amz-server-side-encryption request header",
            &sse_response,
            &shape()
                .status(400)
                .headers(error_response_headers())
                .body(invalid_sse_body.as_str()),
        );

        let invalid_kms_key_body = expected_error::invalid_argument_no_decl(
            "Server Side Encryption with AWS KMS managed key requires HTTP header x-amz-server-side-encryption : aws:kms",
            "x-amz-server-side-encryption",
        );
        let kms_key_response = raw_complete_upload(
            &bucket,
            key,
            &upload_id,
            &complete_body,
            &[(
                "x-amz-server-side-encryption-aws-kms-key-id",
                "arn:aws:kms:us-east-1:111122223333:key/example",
            )],
        );
        assert_shape(
            "CompleteMultipartUpload rejects x-amz-server-side-encryption-aws-kms-key-id request header",
            &kms_key_response,
            &shape()
                .status(400)
                .headers(error_response_headers())
                .body(invalid_kms_key_body.as_str()),
        );

        raw_abort_upload(&bucket, key, &upload_id);
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_complete_multipart_invalid_part_error_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let key = "shape-complete-invalid-part.txt";
        let (_, upload_id) = raw_create_upload(&bucket, key, &[]);
        let _ = raw_upload_part(&bucket, key, &upload_id, 1, &[0u8; 256], &[]);

        let response = raw_complete_upload(
            &bucket,
            key,
            &upload_id,
            &single_part_complete_body("\"ffffffffffffffff\""),
            &[],
        );
        assert_complete_multipart_processing_error_shape(
            "CompleteMultipartUpload InvalidPart",
            &response,
            400,
            expected_error::complete_multipart_invalid_part(&upload_id, 1, "ffffffffffffffff"),
        );

        raw_abort_upload(&bucket, key, &upload_id);
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_complete_multipart_invalid_part_order_error_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let key = "shape-complete-invalid-order.txt";
        let original_body = b"object before invalid part order";
        let original =
            put_object_retrying_operation_aborted(client, &bucket, key, original_body.to_vec())
                .await;
        let (_, upload_id) = raw_create_upload(&bucket, key, &[]);
        let large_part = vec![b'a'; 5 * 1024 * 1024];
        let (_, etag_one) = raw_upload_part(&bucket, key, &upload_id, 1, &large_part, &[]);
        let final_part = [b'b'; 256];
        let (_, etag_two) = raw_upload_part(&bucket, key, &upload_id, 2, &final_part, &[]);

        let response = raw_complete_upload(
            &bucket,
            key,
            &upload_id,
            &format!(
                "<CompleteMultipartUpload>\
                 <Part><PartNumber>2</PartNumber><ETag>{etag_two}</ETag></Part>\
                 <Part><PartNumber>1</PartNumber><ETag>{etag_one}</ETag></Part>\
                 </CompleteMultipartUpload>"
            ),
            &[],
        );
        assert_complete_multipart_processing_error_shape(
            "CompleteMultipartUpload InvalidPartOrder",
            &response,
            400,
            expected_error::complete_multipart_invalid_part_order(&upload_id),
        );

        assert_multipart_parts_preserved(
            &bucket,
            key,
            &upload_id,
            &[
                (1, large_part.len() as i64, &etag_one),
                (2, final_part.len() as i64, &etag_two),
            ],
        )
        .await;
        assert_object_contents_and_etag(&bucket, key, original.e_tag().unwrap(), original_body)
            .await;

        let corrected = raw_complete_upload(
            &bucket,
            key,
            &upload_id,
            &format!(
                "<CompleteMultipartUpload>\
                 <Part><PartNumber>1</PartNumber><ETag>{etag_one}</ETag></Part>\
                 <Part><PartNumber>2</PartNumber><ETag>{etag_two}</ETag></Part>\
                 </CompleteMultipartUpload>"
            ),
            &[],
        );
        assert_eq!(corrected.status, 200, "corrected completion: {corrected:?}");
        assert_list_parts_no_such_upload(&bucket, key, &upload_id).await;
        let mut completed_body = large_part;
        completed_body.extend_from_slice(&final_part);
        let completed_etag = xml_tag_text(&corrected.body, "ETag").unwrap();
        assert_object_contents_and_etag(&bucket, key, completed_etag, &completed_body).await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_complete_multipart_entity_too_small_error_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let key = "shape-complete-entity-too-small.txt";
        let original_body = b"object before entity too small";
        let original =
            put_object_retrying_operation_aborted(client, &bucket, key, original_body.to_vec())
                .await;
        let (_, upload_id) = raw_create_upload(&bucket, key, &[]);
        let small_part = [0u8; 100];
        let (_, etag_one) = raw_upload_part(&bucket, key, &upload_id, 1, &small_part, &[]);
        let (_, etag_two) = raw_upload_part(&bucket, key, &upload_id, 2, &small_part, &[]);

        let response = raw_complete_upload(
            &bucket,
            key,
            &upload_id,
            &format!(
                "<CompleteMultipartUpload>\
                 <Part><PartNumber>1</PartNumber><ETag>{etag_one}</ETag></Part>\
                 <Part><PartNumber>2</PartNumber><ETag>{etag_two}</ETag></Part>\
                 </CompleteMultipartUpload>"
            ),
            &[],
        );
        // The error echoes the first offending part's ETag without quotes.
        assert_complete_multipart_processing_error_shape(
            "CompleteMultipartUpload EntityTooSmall",
            &response,
            400,
            expected_error::complete_multipart_entity_too_small(
                100,
                5242880,
                1,
                etag_one.trim_matches('"'),
            ),
        );

        assert_multipart_parts_preserved(
            &bucket,
            key,
            &upload_id,
            &[
                (1, small_part.len() as i64, &etag_one),
                (2, small_part.len() as i64, &etag_two),
            ],
        )
        .await;
        assert_object_contents_and_etag(&bucket, key, original.e_tag().unwrap(), original_body)
            .await;

        let corrected = raw_complete_upload(
            &bucket,
            key,
            &upload_id,
            &single_part_complete_body(&etag_two)
                .replace("<PartNumber>1</PartNumber>", "<PartNumber>2</PartNumber>"),
            &[],
        );
        assert_eq!(corrected.status, 200, "corrected completion: {corrected:?}");
        assert_list_parts_no_such_upload(&bucket, key, &upload_id).await;
        let completed_etag = xml_tag_text(&corrected.body, "ETag").unwrap();
        assert_object_contents_and_etag(&bucket, key, completed_etag, &small_part).await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_complete_multipart_checksum_mismatch_error_shape() {
    s3_tests::run(async {
        use base64::Engine;
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let key = "shape-complete-checksum-mismatch.txt";
        let original_body = b"object before checksum mismatch";
        let original =
            put_object_retrying_operation_aborted(client, &bucket, key, original_body.to_vec())
                .await;
        let (_, upload_id) =
            raw_create_upload(&bucket, key, &[("x-amz-checksum-algorithm", "SHA256")]);

        let part_body = b"checksum mismatch multipart part";
        let part_checksum = base64::engine::general_purpose::STANDARD
            .encode(ring::digest::digest(&ring::digest::SHA256, part_body).as_ref());
        let (_, etag) = raw_upload_part(
            &bucket,
            key,
            &upload_id,
            1,
            part_body,
            &[("x-amz-checksum-sha256", part_checksum.as_str())],
        );

        let valid_body = format!(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag>\
             <ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part>\
             </CompleteMultipartUpload>"
        );
        let response = raw_complete_upload(
            &bucket,
            key,
            &upload_id,
            &valid_body,
            &[("x-amz-checksum-sha256", "bad")],
        );
        assert_complete_multipart_processing_error_shape(
            "CompleteMultipartUpload checksum header invalid",
            &response,
            400,
            expected_error::complete_multipart_checksum_header_invalid("x-amz-checksum-sha256"),
        );

        assert_multipart_parts_preserved(
            &bucket,
            key,
            &upload_id,
            &[(1, part_body.len() as i64, &etag)],
        )
        .await;
        assert_object_contents_and_etag(&bucket, key, original.e_tag().unwrap(), original_body)
            .await;

        let corrected = raw_complete_upload(&bucket, key, &upload_id, &valid_body, &[]);
        assert_eq!(corrected.status, 200, "corrected completion: {corrected:?}");
        assert_list_parts_no_such_upload(&bucket, key, &upload_id).await;
        let completed_etag = xml_tag_text(&corrected.body, "ETag").unwrap();
        assert_object_contents_and_etag(&bucket, key, completed_etag, part_body).await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_complete_multipart_missing_part_checksum_error_shape() {
    s3_tests::run(async {
        use base64::Engine;
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let key = "shape-complete-missing-part-checksum.txt";
        let original_body = b"object before missing part checksum";
        let original =
            put_object_retrying_operation_aborted(client, &bucket, key, original_body.to_vec())
                .await;
        let (_, upload_id) =
            raw_create_upload(&bucket, key, &[("x-amz-checksum-algorithm", "SHA256")]);

        let part_body = b"missing part checksum multipart part";
        let part_checksum = base64::engine::general_purpose::STANDARD
            .encode(ring::digest::digest(&ring::digest::SHA256, part_body).as_ref());
        let (_, etag) = raw_upload_part(
            &bucket,
            key,
            &upload_id,
            1,
            part_body,
            &[("x-amz-checksum-sha256", part_checksum.as_str())],
        );

        let response = raw_complete_upload(
            &bucket,
            key,
            &upload_id,
            &single_part_complete_body(&etag),
            &[],
        );
        assert_complete_multipart_processing_error_shape(
            "CompleteMultipartUpload missing part checksum",
            &response,
            400,
            expected_error::complete_multipart_missing_part_checksum("sha256", 1),
        );

        assert_multipart_parts_preserved(
            &bucket,
            key,
            &upload_id,
            &[(1, part_body.len() as i64, &etag)],
        )
        .await;
        assert_object_contents_and_etag(&bucket, key, original.e_tag().unwrap(), original_body)
            .await;

        let corrected = raw_complete_upload(
            &bucket,
            key,
            &upload_id,
            &format!(
                "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag>\
                 <ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part>\
                 </CompleteMultipartUpload>"
            ),
            &[],
        );
        assert_eq!(corrected.status, 200, "corrected completion: {corrected:?}");
        assert_list_parts_no_such_upload(&bucket, key, &upload_id).await;
        let completed_etag = xml_tag_text(&corrected.body, "ETag").unwrap();
        assert_object_contents_and_etag(&bucket, key, completed_etag, part_body).await;

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_upload_part_copy_invalid_range_error_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let src_key = "shape-copy-invalid-range-src.txt";
        let dst_key = "shape-copy-invalid-range-dst.txt";

        s3_tests::raw_object_with("PUT", &bucket, src_key, &[b'Z'; 1000], &[]);
        let (_, upload_id) = raw_create_upload(&bucket, dst_key, &[]);

        let response = raw_multipart_query(
            "PUT",
            &bucket,
            dst_key,
            &format!("partNumber=1&uploadId={upload_id}"),
            b"",
            &[
                ("x-amz-copy-source", &format!("{bucket}/{src_key}")),
                ("x-amz-copy-source-range", "bytes=0-9999"),
            ],
        );
        assert_shape(
            "UploadPartCopy invalid range",
            &response,
            &shape().status(400).headers(error_response_headers()).body(
                expected_error::upload_part_copy_invalid_range("bytes=0-9999", 1000),
            ),
        );

        raw_abort_upload(&bucket, dst_key, &upload_id);
        s3_tests::delete_object_retrying_operation_aborted(client, &bucket, src_key)
            .await
            .expect("delete invalid-range source");
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_upload_part_copy_source_if_match_failed_error_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        let src_key = "shape-copy-if-match-src.txt";
        let dst_key = "shape-copy-if-match-dst.txt";

        s3_tests::raw_object_with("PUT", &bucket, src_key, b"copy-source-body", &[]);
        let (_, upload_id) = raw_create_upload(&bucket, dst_key, &[]);

        let response = raw_multipart_query(
            "PUT",
            &bucket,
            dst_key,
            &format!("partNumber=1&uploadId={upload_id}"),
            b"",
            &[
                ("x-amz-copy-source", &format!("{bucket}/{src_key}")),
                ("x-amz-copy-source-if-match", "\"0000000000000000\""),
            ],
        );
        assert_shape(
            "UploadPartCopy source if-match failed",
            &response,
            &shape().status(412).headers(error_response_headers()).body(
                expected_error::upload_part_copy_precondition_failed("x-amz-copy-source-If-Match"),
            ),
        );

        raw_abort_upload(&bucket, dst_key, &upload_id);
        s3_tests::delete_object_retrying_operation_aborted(client, &bucket, src_key)
            .await
            .expect("delete if-match source");
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_list_multipart_uploads_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let (_, upload_a) = raw_create_upload(&bucket, "mpu-a.txt", &[]);
        let (_, upload_b) = raw_create_upload(&bucket, "mpu-b.txt", &[]);

        // Truncated at 1: only upload a is listed and the markers point at
        // it. Initiator ID is endpoint-specific (an ARN on AWS, a canonical
        // ID locally); Owner ID is the canonical owner everywhere.
        let response = raw_bucket("GET", &bucket, Some("uploads=&max-uploads=1"));
        assert_shape(
            "ListMultipartUploads shape",
            &response,
            &shape()
                .status(200)
                .headers(xml_response_headers())
                .body(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <ListMultipartUploadsResult \
                     xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>{bucket}</Bucket>\
                     <KeyMarker></KeyMarker><UploadIdMarker></UploadIdMarker>\
                     <NextKeyMarker>mpu-a.txt</NextKeyMarker>\
                     <NextUploadIdMarker>{upload_a}</NextUploadIdMarker>\
                     <MaxUploads>1</MaxUploads><IsTruncated>true</IsTruncated>\
                     <Upload><Key>mpu-a.txt</Key><UploadId>{upload_a}</UploadId>\
                     <Initiator><ID>{any}</ID><DisplayName>{any}</DisplayName></Initiator>\
                     <Owner><ID>{owner_id}</ID></Owner><StorageClass>STANDARD</StorageClass>\
                     <Initiated>{iso8601}</Initiated></Upload></ListMultipartUploadsResult>",
                )
                .sub("bucket", bucket.as_str())
                .sub("upload_a", upload_a.as_str()),
        );

        raw_abort_upload(&bucket, "mpu-a.txt", &upload_a);
        raw_abort_upload(&bucket, "mpu-b.txt", &upload_b);
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}
