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
    object_url, send_signed_request, send_signed_request_with_credentials, unique_bucket,
    RawResponse, SignedRequestCredentials, CTX,
};

const PART_SIZE: usize = 5 * 1024 * 1024; // 5 MB minimum part size
const SLOW_PART_SIZE: usize = 8 * 1024 * 1024;
const SLOW_PART_CHUNK_SIZE: usize = 64 * 1024;
const CONCURRENT_MULTIPART_OPERATION_ATTEMPTS: usize = 20;

fn external_test_mode() -> bool {
    std::env::var_os("S3_TEST_ENDPOINT").is_some()
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
    for attempt in 0..CONCURRENT_MULTIPART_OPERATION_ATTEMPTS {
        match client
            .put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(status.clone())
                    .build(),
            )
            .send()
            .await
        {
            Ok(_) => return,
            Err(err)
                if is_operation_aborted(&err)
                    && attempt + 1 < CONCURRENT_MULTIPART_OPERATION_ATTEMPTS =>
            {
                tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
            }
            Err(err) => panic!("put bucket versioning during multipart setup: {err:?}"),
        }
    }
    panic!("put bucket versioning during multipart setup did not complete");
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
    for attempt in 0..CONCURRENT_MULTIPART_OPERATION_ATTEMPTS {
        match client.delete_object().bucket(bucket).key(key).send().await {
            Ok(output) => return output,
            Err(err)
                if is_operation_aborted(&err)
                    && attempt + 1 < CONCURRENT_MULTIPART_OPERATION_ATTEMPTS =>
            {
                tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
            }
            Err(err) => panic!("delete object during multipart setup: {err:?}"),
        }
    }
    panic!("delete object during multipart setup did not complete");
}

async fn create_multipart_upload_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput {
    for attempt in 0..CONCURRENT_MULTIPART_OPERATION_ATTEMPTS {
        match client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
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
            Err(err) => panic!("create multipart upload during multipart setup: {err:?}"),
        }
    }
    panic!("create multipart upload during multipart setup did not complete");
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
    for attempt in 0..CONCURRENT_MULTIPART_OPERATION_ATTEMPTS {
        let result = client
            .upload_part_copy()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .copy_source(copy_source.clone())
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
    panic!("upload part copy during multipart setup did not complete");
}

async fn complete_multipart_upload_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    etag: &str,
) {
    for attempt in 0..CONCURRENT_MULTIPART_OPERATION_ATTEMPTS {
        match client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(CompletedPart::builder().e_tag(etag).part_number(1).build())
                    .build(),
            )
            .send()
            .await
        {
            Ok(_) => return,
            Err(err)
                if is_operation_aborted(&err)
                    && attempt + 1 < CONCURRENT_MULTIPART_OPERATION_ATTEMPTS =>
            {
                tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
            }
            Err(err) => panic!("complete multipart upload during multipart setup: {err:?}"),
        }
    }
    panic!("complete multipart upload during multipart setup did not complete");
}

async fn abort_multipart_upload_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
) {
    for attempt in 0..CONCURRENT_MULTIPART_OPERATION_ATTEMPTS {
        match client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
        {
            Ok(_) => return,
            Err(err)
                if is_operation_aborted(&err)
                    && attempt + 1 < CONCURRENT_MULTIPART_OPERATION_ATTEMPTS =>
            {
                tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
            }
            Err(err) => panic!("abort multipart upload during multipart setup: {err:?}"),
        }
    }
    panic!("abort multipart upload during multipart setup did not complete");
}

async fn assert_list_parts_no_such_upload(bucket: &str, key: &str, upload_id: &str) {
    let result = CTX
        .client()
        .list_parts()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .send()
        .await;
    assert_eq!(
        err_status(&result),
        404,
        "unexpected ListParts result: {result:?}"
    );
    assert_s3_err_code(&result, "NoSuchUpload");
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
            if let Err(err) = client.delete_object().bucket(bucket).key(*key).send().await {
                let raw = format!("{err:?}");
                if !raw.contains("NoSuchBucket") && !raw.contains("NoSuchKey") {
                    last_cleanup_error = Some(format!("delete_object {key:?}: {raw}"));
                }
            }
        }

        let uploads = client
            .list_multipart_uploads()
            .bucket(bucket)
            .send()
            .await
            .unwrap();
        for upload in uploads.uploads() {
            let _ = client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(upload.key().unwrap())
                .upload_id(upload.upload_id().unwrap())
                .send()
                .await;
        }

        match client.delete_bucket().bucket(bucket).send().await {
            Ok(_) => return,
            Err(err) => {
                let raw = format!("{err:?}");
                if raw.contains("OperationAborted") || raw.contains("BucketNotEmpty") {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    continue;
                }
                panic!("delete_bucket failed unexpectedly: {raw}");
            }
        }
    }

    let result = client.delete_bucket().bucket(bucket).send().await;
    if let Err(err) = result {
        panic!(
            "delete_bucket did not converge: {err:?}; last cleanup error: {}",
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

    let create = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap();

    let mut completed_parts = Vec::new();
    for (i, data) in parts_data.iter().enumerate() {
        let part_number = (i + 1) as i32;
        let resp = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(data.clone()))
            .send()
            .await
            .unwrap();
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
        .send()
        .await
        .unwrap();
    complete.e_tag().unwrap().to_string()
}

async fn complete_single_part_multipart_upload(bucket: &str, key: &str, body: &[u8]) -> String {
    let client = CTX.client();
    let create = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();

    let part = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(body.to_vec()))
        .send()
        .await
        .unwrap();

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
        .send()
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
            .send()
            .await
            .unwrap()
            .upload_id()
            .unwrap()
            .to_string();

        client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![b'a'; PART_SIZE]))
            .send()
            .await
            .unwrap();

        let before_abort = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();
        assert_eq!(before_abort.parts().len(), 1);
        assert_eq!(before_abort.parts()[0].part_number(), Some(1));

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
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
            .send()
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
            .send()
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
            .send()
            .await;
        if let Ok(parts) = list_after_race {
            assert_eq!(parts.parts().len(), 1);
            assert_eq!(parts.parts()[0].part_number(), Some(1));
            CTX.client()
                .abort_multipart_upload()
                .bucket(&bucket)
                .key(key)
                .upload_id(&upload_id)
                .send()
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
            .send()
            .await
            .unwrap()
            .upload_id()
            .unwrap()
            .to_string();

        client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![b'a'; PART_SIZE]))
            .send()
            .await
            .unwrap();

        let before_race = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
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
            .send()
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
            .send()
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
                .send()
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
            .send()
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
            .send()
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
            .send()
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
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload a part
        client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![0u8; 1024]))
            .send()
            .await
            .unwrap();

        // Abort the upload
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();

        // The object should not exist
        let result = client.get_object().bucket(&bucket).key(key).send().await;
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
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
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
            .send()
            .await;
        assert_s3_err_code(&result, "NoSuchUpload");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_abort_multipart_upload_invalid_present_upload_id_overlong_message_external() {
    s3_tests::run(async {
        if !external_test_mode() {
            return;
        }

        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-invalid-upload-id-message";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
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
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_upload_part_invalid_present_upload_id_overlong_auth_precedence_external() {
    s3_tests::run(async {
        if !external_test_mode() {
            return;
        }

        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-invalid-upload-id-auth-precedence";

        client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();

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
            alt_credentials(),
        );
        assert_invalid_upload_id_no_such_upload(&response, &invalid_upload_id);

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_complete_multipart_upload_invalid_present_upload_id_overlong_message_external() {
    s3_tests::run(async {
        if !external_test_mode() {
            return;
        }

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
fn test_complete_multipart_upload_invalid_present_upload_id_overlong_auth_precedence_external() {
    s3_tests::run(async {
        if !external_test_mode() {
            return;
        }

        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-complete-invalid-upload-id-auth-precedence";

        client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();

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
            alt_credentials(),
        );
        assert_invalid_upload_id_no_such_upload(&response, &invalid_upload_id);

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_list_parts_invalid_present_upload_id_overlong_message_external() {
    s3_tests::run(async {
        if !external_test_mode() {
            return;
        }

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
fn test_list_parts_invalid_present_upload_id_overlong_auth_precedence_external() {
    s3_tests::run(async {
        if !external_test_mode() {
            return;
        }

        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-list-parts-invalid-upload-id-auth-precedence";

        client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();

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
            alt_credentials(),
        );
        assert_invalid_upload_id_no_such_upload(&response, &invalid_upload_id);

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
            .send()
            .await
            .unwrap();

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        let get = client.get_object().bucket(&bucket).key(key).send().await;
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
            .send()
            .await
            .unwrap();

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&second_upload_id)
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
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
            .send()
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
            .send()
            .await;
        assert_s3_err_code(&result, "NoSuchUpload");

        cleanup(&bucket, &[]).await;
    });
}

// ── ListMultipartUploads ────────────────────────────────────────────

#[test]
fn test_list_multipart_uploads_empty() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send()
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
            .send()
            .await
            .unwrap();
        let uid1 = create1.upload_id().unwrap().to_string();

        let create2 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("key2")
            .send()
            .await
            .unwrap();
        let uid2 = create2.upload_id().unwrap().to_string();

        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send()
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
            .send()
            .await
            .unwrap();
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("key2")
            .upload_id(&uid2)
            .send()
            .await
            .unwrap();

        // Should be empty now
        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send()
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
            .send()
            .await
            .unwrap();
        let uid1 = c1.upload_id().unwrap().to_string();

        let c2 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("docs/b.txt")
            .send()
            .await
            .unwrap();
        let uid2 = c2.upload_id().unwrap().to_string();

        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .prefix("photos/")
            .send()
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
            .send()
            .await
            .unwrap();
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("docs/b.txt")
            .upload_id(&uid2)
            .send()
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
                .send()
                .await
                .unwrap();
            created.push((key.to_string(), create.upload_id().unwrap().to_string()));
        }

        let resp1 = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .max_uploads(1)
            .send()
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
            .send()
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
            .send()
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
                .send()
                .await
                .unwrap();
        }
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
                .send()
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
                .send()
                .await
                .unwrap();
        }
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
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .encoding_type(EncodingType::Url)
            .send()
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
            .send()
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
            .send()
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
            .send()
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
            .send()
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
            .send()
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
            .send()
            .await
            .unwrap();
        let first_upload_id = create_first.upload_id().unwrap().to_string();

        let create_second = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(SECOND_KEY)
            .send()
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
            .send()
            .await
            .unwrap();
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(SECOND_KEY)
            .upload_id(&second_upload_id)
            .send()
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
            .send()
            .await
            .unwrap();
        let first_upload_id = create_first.upload_id().unwrap().to_string();

        let create_second = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(SECOND_KEY)
            .send()
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
            .send()
            .await
            .unwrap();
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(SECOND_KEY)
            .upload_id(&second_upload_id)
            .send()
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
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload two parts
        client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![b'a'; PART_SIZE]))
            .send()
            .await
            .unwrap();

        client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(2)
            .body(ByteStream::from(vec![b'b'; 1024]))
            .send()
            .await
            .unwrap();

        let resp = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
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
            .send()
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
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![b'a'; PART_SIZE]))
            .send()
            .await
            .unwrap();

        let resp = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .max_parts(0)
            .send()
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
            .send()
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
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let mut expected = Vec::new();
        for (part_number, data) in [(1, vec![b'a'; PART_SIZE]), (2, vec![b'b'; 1024])] {
            let resp = client
                .upload_part()
                .bucket(&bucket)
                .key(key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(ByteStream::from(data))
                .checksum_algorithm(ChecksumAlgorithm::Crc32)
                .send()
                .await
                .unwrap();
            expected.push((part_number, resp.checksum_crc32().unwrap().to_string()));
        }

        let resp1 = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .max_parts(1)
            .send()
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
            .send()
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
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

// ── Error cases ─────────────────────────────────────────────────────

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
            .send()
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
            .send()
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
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload two parts, first one too small (< 5MB)
        let resp1 = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![0u8; 100]))
            .send()
            .await
            .unwrap();

        let resp2 = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(2)
            .body(ByteStream::from(vec![0u8; 100]))
            .send()
            .await
            .unwrap();

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
            .send()
            .await;
        assert!(result.is_err());

        // Abort to clean up
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
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
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let result = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(10_001)
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidArgument");

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"original"))
            .send()
            .await
            .unwrap();

        // Overwrite with multipart
        let new_data = vec![b'z'; 512];
        do_multipart_upload(&bucket, key, std::slice::from_ref(&new_data)).await;

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], &new_data[..]);

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
            .send()
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
            .send()
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
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup(&bucket, &[key]).await;
    });
}

// ── Multiple concurrent uploads for same key ────────────────────────

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
            .send()
            .await
            .unwrap();
        let uid1 = c1.upload_id().unwrap().to_string();

        let c2 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let uid2 = c2.upload_id().unwrap().to_string();
        assert_ne!(uid1, uid2);

        // Both should appear in listing
        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.uploads().len(), 2);

        // Complete the first, abort the second
        let r1 = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&uid1)
            .part_number(1)
            .body(ByteStream::from(vec![b'1'; 100]))
            .send()
            .await
            .unwrap();

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
            .send()
            .await
            .unwrap();

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&uid2)
            .send()
            .await
            .unwrap();

        // Only the completed upload's object should exist
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(data.len(), 100);
        assert!(data.iter().all(|&b| b == b'1'));

        cleanup(&bucket, &[key]).await;
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
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let body = b"hello world";
        let part = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(body.to_vec()))
            .send()
            .await
            .unwrap();

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
            .send()
            .await
            .unwrap();

        let object = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
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

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let body = b"hello world";
        let part = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(body.to_vec()))
            .send()
            .await
            .unwrap();

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
            .send()
            .await;

        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup(&bucket, &[]).await;
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
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload part 1 with data 'a'
        client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![b'a'; 256]))
            .send()
            .await
            .unwrap();

        // Re-upload part 1 with data 'b' — should replace
        let resp = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![b'b'; 512]))
            .send()
            .await
            .unwrap();

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
            .send()
            .await
            .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
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
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(CompletedMultipartUpload::builder().build())
            .send()
            .await;
        assert!(result.is_err());

        // Abort to clean up
        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
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
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![0u8; 256]))
            .send()
            .await
            .unwrap();

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
            .send()
            .await;
        assert_s3_err_code(&result, "InvalidPart");

        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
        cleanup(&bucket, &[]).await;
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
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload part 1
        let resp = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![0u8; 256]))
            .send()
            .await
            .unwrap();

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
            .send()
            .await;
        assert_s3_err_code(&result, "InvalidPart");

        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
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
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let resp = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![b'm'; 128]))
            .send()
            .await
            .unwrap();

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
            .send()
            .await
            .unwrap();

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
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
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let r1 = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![b'a'; PART_SIZE]))
            .send()
            .await
            .unwrap();

        let r2 = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(2)
            .body(ByteStream::from(vec![b'b'; 256]))
            .send()
            .await
            .unwrap();

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
            .send()
            .await;
        assert_s3_err_code(&result, "InvalidPartOrder");

        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
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
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload part 1 with data 'A'
        let data_a = vec![b'A'; PART_SIZE];
        let _resp_a = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(data_a))
            .send()
            .await
            .unwrap();

        // Re-upload part 1 with data 'B' (replaces the first upload)
        let data_b = vec![b'B'; PART_SIZE];
        let resp_b = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(data_b.clone()))
            .send()
            .await
            .unwrap();

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
            .send()
            .await
            .unwrap();

        // Verify the content is from the second upload
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
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
                .send()
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
                .send()
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
                .send()
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
            .send()
            .await;
        assert_eq!(err_status(&result), 416);

        // Out-of-range partNumber on HEAD → same error
        let result = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .part_number(part_count + 1)
            .send()
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

        let resp = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(b"body".to_vec()))
            .send()
            .await
            .unwrap();
        let etag = resp.e_tag().unwrap().to_string();

        // GET PartNumber > 1 → 416 Range Not Satisfiable (AWS behavior)
        let result = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .part_number(2)
            .send()
            .await;
        assert_eq!(err_status(&result), 416);

        // HEAD PartNumber > 1 → same error
        let result = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .part_number(2)
            .send()
            .await;
        assert_eq!(err_status(&result), 416);

        // PartNumber = 1 → returns entire object
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .part_number(1)
            .send()
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
            .send()
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
            .send()
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
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from(src_data.clone()))
            .send()
            .await
            .unwrap();

        // Create multipart upload, upload_part_copy entire source as one part
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send()
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
            .send()
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
            .send()
            .await
            .unwrap();

        // Verify GET returns correct data
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(dst_key)
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from(src_data.clone()))
            .send()
            .await
            .unwrap();

        // UploadPartCopy without range copies full object
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send()
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
            .send()
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
            .send()
            .await
            .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(dst_key)
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from(src_data))
            .send()
            .await
            .unwrap();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send()
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
            .send()
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
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from(src_data))
            .send()
            .await
            .unwrap();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send()
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
            .send()
            .await;
        let status = err_status(&result);
        assert!(status == 400, "expected 400, got {status}");
        assert_s3_err_code(&result, "InvalidArgument");

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .send()
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

        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send()
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
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidArgument");

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from(src_data.clone()))
            .send()
            .await
            .unwrap();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send()
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
            .send()
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
            .send()
            .await
            .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(dst_key)
            .send()
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
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from(src_data.clone()))
            .send()
            .await
            .unwrap();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send()
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
            .send()
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
            .send()
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
            .send()
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
            .send()
            .await
            .unwrap();

        // Verify assembled object matches source
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(dst_key)
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key(encoded_key)
            .body(ByteStream::from(b"foo".to_vec()))
            .send()
            .await
            .unwrap();

        // Put the destination object (initial state)
        client
            .put_object()
            .bucket(&bucket)
            .key(dst_key)
            .body(ByteStream::from(b"foo".to_vec()))
            .send()
            .await
            .unwrap();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send()
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
            .send()
            .await;
        assert!(result.is_err(), "expected error copying with raw % key");

        // Verify the original destination object is untouched
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(dst_key)
            .send()
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
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[encoded_key, dst_key]).await;
    });
}
