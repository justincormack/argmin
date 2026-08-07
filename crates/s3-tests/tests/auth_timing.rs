use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    AccessControlPolicy, Grant, Grantee, ObjectCannedAcl, ObjectOwnership, Owner, Permission, Tag,
    Tagging, Type,
};
use s3_tests::{
    build_post_object_multipart_body, cleanup_versioned_bucket, create_acl_enabled_bucket,
    create_boe_bucket, delete_bucket_retrying_operation_aborted,
    delete_object_retrying_operation_aborted, enable_bucket_versioning, err_status, object_url,
    open_flushed_partial_request, raw_alt_credentials, sign_aws_chunked_request_with_credentials,
    sign_request_headers_with_credentials, sigv4_post_fields_for_credentials, unique_bucket,
    FlushedPartialRequest, FlushedResponse, SendRetryingOperationAborted, CTX,
};
use serde_json::json;
use std::future::Future;
use std::time::{Duration, Instant};

const STAGED_PUT_BYTES: usize = 4 * 1024 * 1024;
const PROMOTED_STREAMING_PUT_BYTES: usize = server_core::coordinator::INTERNAL_SEGMENT_SIZE + 1;
const DIRECT_PUT_BYTES: usize = 64 * 1024;
const DIRECT_PUT_PREFIX_BYTES: usize = 16 * 1024;
const STAGED_PUT_PREFIX_BYTES: usize = 64 * 1024;
const STREAMED_READ_BYTES: usize = 4 * 1024 * 1024;
const RESPONSE_STAGE_WINDOW: Duration = Duration::from_secs(2);
const ORIGINAL_TIMING_DESTINATION: &[u8] = b"original ObjectWriter timing destination";

const _: () = assert!(DIRECT_PUT_BYTES <= server_core::coordinator::INTERNAL_SEGMENT_SIZE);
const _: () =
    assert!(PROMOTED_STREAMING_PUT_BYTES > server_core::coordinator::INTERNAL_SEGMENT_SIZE);

#[derive(Clone, Copy, Debug)]
enum TimingDestinationState {
    Absent,
    Live,
    DeleteMarker,
}

#[derive(Clone, Copy, Debug)]
enum TimingPutEncoding {
    Plain,
    AwsChunked,
    PostObject,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TimingPutOutcome {
    AuthorizationResolved,
    RetryableContention,
}

fn is_object_writer_timing_slow_down(status: u16, body: &[u8]) -> bool {
    status == 503
        && std::str::from_utf8(body)
            .ok()
            .and_then(|body| s3_tests::shape::xml_tag_text(body, "Code"))
            == Some("SlowDown")
}

#[test]
fn object_writer_timing_slow_down_classification_requires_status_and_code() {
    let slow_down = b"<Error><Code>SlowDown</Code></Error>";
    let operation_aborted = b"<Error><Code>OperationAborted</Code></Error>";

    assert!(is_object_writer_timing_slow_down(503, slow_down));
    assert!(!is_object_writer_timing_slow_down(409, slow_down));
    assert!(!is_object_writer_timing_slow_down(503, operation_aborted));
}

impl TimingPutEncoding {
    fn label(self) -> &'static str {
        match self {
            Self::Plain => "plain",
            Self::AwsChunked => "aws-chunked",
            Self::PostObject => "post-object",
        }
    }
}

impl TimingDestinationState {
    fn label(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Live => "live",
            Self::DeleteMarker => "delete-marker",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TimingObjectVersion {
    version_id: String,
    etag: Option<String>,
    is_latest: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TimingDeleteMarker {
    version_id: String,
    is_latest: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TimingObjectHistory {
    versions: Vec<TimingObjectVersion>,
    delete_markers: Vec<TimingDeleteMarker>,
}

struct StagedObjectWriterPut {
    label: String,
    key: String,
    state: TimingDestinationState,
    body: Vec<u8>,
    wire_body: Vec<u8>,
    success_status: u16,
    flushed_prefix_bytes: usize,
    baseline: TimingObjectHistory,
    request: Option<FlushedPartialRequest>,
    body_open: bool,
}

fn ordinary_alt_client() -> aws_sdk_s3::Client {
    CTX.alt_client().clone()
}

async fn open_alt_flushed_put(
    bucket: &str,
    key: &str,
    body: &[u8],
    flushed_prefix_bytes: usize,
    extra_headers: &[(&str, &str)],
) -> FlushedPartialRequest {
    let url = object_url(CTX.endpoint(), bucket, key, None);
    let signed = sign_request_headers_with_credentials(
        "PUT",
        &url,
        body,
        extra_headers.iter().copied(),
        raw_alt_credentials(),
    );
    let headers = signed.headers().collect::<Vec<_>>();
    open_flushed_partial_request(
        "PUT",
        &url,
        body.len(),
        &body[..flushed_prefix_bytes],
        &headers,
        CTX.tls_ca_pem(),
    )
    .await
    .expect("open and flush signed raw PutObject prefix")
}

fn alt_principal() -> serde_json::Value {
    json!({ "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) })
}

fn alt_put_policy(bucket: &str, effect: &str) -> String {
    json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": effect,
            "Principal": alt_principal(),
            "Action": "s3:PutObject",
            "Resource": format!("arn:aws:s3:::{bucket}/*"),
        }],
    })
    .to_string()
}

async fn set_alt_put_policy(bucket: &str, effect: &str) {
    CTX.client()
        .put_bucket_policy()
        .bucket(bucket)
        .policy(alt_put_policy(bucket, effect))
        .send_retrying_operation_aborted("set auth timing bucket policy")
        .await
        .unwrap();
}

fn test_deadline() -> Instant {
    let timeout_secs = std::env::var("S3_TEST_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(30);
    Instant::now() + Duration::from_secs(timeout_secs)
}

fn revocation_soak_duration() -> Duration {
    let timeout_secs = std::env::var("S3_TEST_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(30);
    Duration::from_secs((timeout_secs / 4).clamp(5, 30))
}

async fn wait_for_put_allowed(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    stable_for: Duration,
) {
    let deadline = test_deadline();
    let mut stable_since = None;
    loop {
        let result = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"allow canary"))
            .send()
            .await;
        if result.is_ok() {
            let stable_since = stable_since.get_or_insert_with(Instant::now);
            if stable_since.elapsed() >= stable_for {
                return;
            }
        } else {
            stable_since = None;
        }
        if Instant::now() >= deadline {
            panic!("alternate PutObject allow did not converge: {result:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_put_denied(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    stable_for: Duration,
) {
    let deadline = test_deadline();
    let mut stable_since = None;
    loop {
        let result = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"deny canary"))
            .send()
            .await;
        let denied = result.as_ref().err().is_some_and(|error| {
            error
                .raw_response()
                .map(|response| response.status().as_u16())
                == Some(403)
                && error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                    == Some("AccessDenied")
        });
        if denied {
            let stable_since = stable_since.get_or_insert_with(Instant::now);
            if stable_since.elapsed() >= stable_for {
                return;
            }
        } else {
            stable_since = None;
        }
        if Instant::now() >= deadline {
            panic!("alternate PutObject denial did not converge: {result:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_while_keeping_body_active<F>(
    wait: F,
    request: &mut FlushedPartialRequest,
) -> (usize, bool)
where
    F: Future<Output = ()>,
{
    let mut wait = Box::pin(wait);
    let mut emitted = 0;
    let mut body_open = true;
    loop {
        tokio::select! {
            () = &mut wait => return (emitted, body_open),
            () = tokio::time::sleep(Duration::from_secs(1)), if body_open => {
                body_open = request.write_and_flush(b"k").await.is_ok();
                if body_open {
                    emitted += 1;
                }
            }
        }
    }
}

async fn wait_for_put_denied_while_keeping_body_active(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    stable_for: Duration,
    request: &mut FlushedPartialRequest,
) -> (usize, bool) {
    wait_while_keeping_body_active(
        wait_for_put_denied(client, bucket, key, stable_for),
        request,
    )
    .await
}

async fn canonical_owner_id(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    let owner_id = client
        .get_bucket_acl()
        .bucket(&bucket)
        .send_retrying_operation_aborted("get auth timing canonical owner ID")
        .await
        .unwrap()
        .owner()
        .and_then(|owner| owner.id())
        .expect("expected canonical owner ID")
        .to_string();
    delete_bucket_retrying_operation_aborted(client, &bucket).await;
    owner_id
}

fn canonical_user_grant(canonical_user_id: &str, permission: Permission) -> Grant {
    Grant::builder()
        .grantee(
            Grantee::builder()
                .r#type(Type::CanonicalUser)
                .id(canonical_user_id)
                .build()
                .expect("canonical user grantee"),
        )
        .permission(permission)
        .build()
}

fn object_acl(owner_id: &str, grants: Vec<Grant>) -> AccessControlPolicy {
    AccessControlPolicy::builder()
        .owner(Owner::builder().id(owner_id).build())
        .set_grants(Some(grants))
        .build()
}

fn object_tagging(key: &str, value: &str) -> Tagging {
    Tagging::builder()
        .tag_set(Tag::builder().key(key).value(value).build().unwrap())
        .build()
        .unwrap()
}

fn streamed_read_payload() -> Vec<u8> {
    (0..STREAMED_READ_BYTES)
        .map(|offset| (offset % 251) as u8)
        .collect()
}

async fn wait_for_head_allowed(client: &aws_sdk_s3::Client, bucket: &str, key: &str) {
    let deadline = test_deadline();
    loop {
        let result = client.head_object().bucket(bucket).key(key).send().await;
        if result.is_ok() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("alternate HeadObject allow did not converge: {result:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn timing_object_history(bucket: &str, key: &str) -> TimingObjectHistory {
    let listed = CTX
        .client()
        .list_object_versions()
        .bucket(bucket)
        .prefix(key)
        .send_retrying_operation_aborted("list ObjectWriter timing object history")
        .await
        .unwrap();
    let mut versions = listed
        .versions()
        .iter()
        .filter(|version| version.key() == Some(key))
        .map(|version| TimingObjectVersion {
            version_id: version
                .version_id()
                .expect("versioned timing object must have a version ID")
                .to_string(),
            etag: version.e_tag().map(str::to_string),
            is_latest: version.is_latest().unwrap_or(false),
        })
        .collect::<Vec<_>>();
    versions.sort_by(|left, right| left.version_id.cmp(&right.version_id));

    let mut delete_markers = listed
        .delete_markers()
        .iter()
        .filter(|marker| marker.key() == Some(key))
        .map(|marker| TimingDeleteMarker {
            version_id: marker
                .version_id()
                .expect("versioned timing delete marker must have a version ID")
                .to_string(),
            is_latest: marker.is_latest().unwrap_or(false),
        })
        .collect::<Vec<_>>();
    delete_markers.sort_by(|left, right| left.version_id.cmp(&right.version_id));

    TimingObjectHistory {
        versions,
        delete_markers,
    }
}

async fn prepare_timing_destination(
    bucket: &str,
    key: &str,
    state: TimingDestinationState,
) -> TimingObjectHistory {
    if matches!(
        state,
        TimingDestinationState::Live | TimingDestinationState::DeleteMarker
    ) {
        s3_tests::retrying_operation_aborted("put ObjectWriter timing destination", || {
            CTX.client()
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from_static(ORIGINAL_TIMING_DESTINATION))
                .send()
        })
        .await;
    }
    if matches!(state, TimingDestinationState::DeleteMarker) {
        let deleted = CTX
            .client()
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send_retrying_operation_aborted("create ObjectWriter timing delete marker")
            .await
            .unwrap();
        assert_eq!(deleted.delete_marker(), Some(true));
        assert!(deleted.version_id().is_some());
    }

    let history = timing_object_history(bucket, key).await;
    match state {
        TimingDestinationState::Absent => {
            assert!(history.versions.is_empty());
            assert!(history.delete_markers.is_empty());
        }
        TimingDestinationState::Live => {
            assert_eq!(history.versions.len(), 1);
            assert!(history.versions[0].is_latest);
            assert!(history.delete_markers.is_empty());
        }
        TimingDestinationState::DeleteMarker => {
            assert_eq!(history.versions.len(), 1);
            assert!(!history.versions[0].is_latest);
            assert_eq!(history.delete_markers.len(), 1);
            assert!(history.delete_markers[0].is_latest);
        }
    }
    history
}

fn assert_baseline_history_retained_after_put(
    label: &str,
    baseline: &TimingObjectHistory,
    current: &TimingObjectHistory,
) {
    assert_eq!(
        current.versions.len(),
        baseline.versions.len() + 1,
        "{label}: successful PutObject must add exactly one object version"
    );
    assert_eq!(
        current.delete_markers.len(),
        baseline.delete_markers.len(),
        "{label}: successful PutObject must retain every delete marker"
    );
    for baseline_version in &baseline.versions {
        let retained = current
            .versions
            .iter()
            .find(|version| version.version_id == baseline_version.version_id)
            .unwrap_or_else(|| {
                panic!(
                    "{label}: baseline version {} disappeared",
                    baseline_version.version_id
                )
            });
        assert_eq!(
            retained.etag, baseline_version.etag,
            "{label}: baseline version ETag changed"
        );
        assert!(
            !retained.is_latest,
            "{label}: baseline version remained latest after successful PutObject"
        );
    }
    for baseline_marker in &baseline.delete_markers {
        let retained = current
            .delete_markers
            .iter()
            .find(|marker| marker.version_id == baseline_marker.version_id)
            .unwrap_or_else(|| {
                panic!(
                    "{label}: baseline delete marker {} disappeared",
                    baseline_marker.version_id
                )
            });
        assert!(
            !retained.is_latest,
            "{label}: baseline delete marker remained latest after successful PutObject"
        );
    }
    assert_eq!(
        current
            .versions
            .iter()
            .filter(|version| version.is_latest)
            .count(),
        1,
        "{label}: successful PutObject must leave one latest live version"
    );
    assert!(
        current
            .delete_markers
            .iter()
            .all(|marker| !marker.is_latest),
        "{label}: successful PutObject left a current delete marker"
    );
}

async fn assert_object_writer_timing_put_result(
    bucket: &str,
    case: &StagedObjectWriterPut,
    response: &FlushedResponse,
) -> TimingPutOutcome {
    let current = timing_object_history(bucket, &case.key).await;
    match response.status() {
        status if status == case.success_status => {
            let object = CTX
                .alt_client()
                .get_object()
                .bucket(bucket)
                .key(&case.key)
                .send()
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "{}: successful timing-dependent object write must be readable: {error:?}",
                        case.label
                    )
                });
            let body = object.body.collect().await.unwrap().into_bytes();
            assert_eq!(
                &body[..],
                case.body.as_slice(),
                "{}: successful object write published incomplete or wrong bytes",
                case.label
            );
            assert_baseline_history_retained_after_put(&case.label, &case.baseline, &current);
            TimingPutOutcome::AuthorizationResolved
        }
        403 => {
            let response_body = std::str::from_utf8(response.body())
                .expect("ObjectWriter timing PutObject error response must be UTF-8");
            assert_eq!(
                s3_tests::shape::xml_tag_text(response_body, "Code"),
                Some("AccessDenied"),
                "{}: unexpected timing-dependent PutObject error: {response_body}",
                case.label
            );
            assert_eq!(
                current, case.baseline,
                "{}: denied object write mutated object version history",
                case.label
            );
            let current_object = CTX
                .client()
                .get_object()
                .bucket(bucket)
                .key(&case.key)
                .send()
                .await;
            match case.state {
                TimingDestinationState::Live => {
                    let body = current_object
                        .unwrap_or_else(|error| {
                            panic!(
                                "{}: baseline live object disappeared: {error:?}",
                                case.label
                            )
                        })
                        .body
                        .collect()
                        .await
                        .unwrap()
                        .into_bytes();
                    assert_eq!(&body[..], ORIGINAL_TIMING_DESTINATION);
                }
                TimingDestinationState::Absent | TimingDestinationState::DeleteMarker => {
                    assert_eq!(
                        err_status(&current_object),
                        404,
                        "{}: {current_object:?}",
                        case.label
                    );
                }
            }
            TimingPutOutcome::AuthorizationResolved
        }
        status if is_object_writer_timing_slow_down(status, response.body()) => {
            assert_eq!(
                current, case.baseline,
                "{}: slowed object write mutated object version history",
                case.label
            );
            TimingPutOutcome::RetryableContention
        }
        status => {
            let response_body = String::from_utf8_lossy(response.body());
            panic!(
                "{}: unexpected timing-dependent object-write status {status}: {response_body}",
                case.label
            );
        }
    }
}

async fn open_object_writer_plain_put_cases(
    bucket: &str,
    transition: &str,
) -> Vec<StagedObjectWriterPut> {
    let modes = [
        ("direct", DIRECT_PUT_BYTES, DIRECT_PUT_PREFIX_BYTES),
        (
            "streamed",
            PROMOTED_STREAMING_PUT_BYTES,
            STAGED_PUT_PREFIX_BYTES,
        ),
    ];
    let states = [
        TimingDestinationState::Absent,
        TimingDestinationState::Live,
        TimingDestinationState::DeleteMarker,
    ];
    let mut cases = Vec::with_capacity(modes.len() * states.len());
    for (mode_index, (mode, body_bytes, prefix_bytes)) in modes.into_iter().enumerate() {
        for (state_index, state) in states.into_iter().enumerate() {
            let label = format!("{transition}-{mode}-{}", state.label());
            let key = format!("object-writer-{label}");
            let baseline = prepare_timing_destination(bucket, &key, state).await;
            let body = vec![b'a' + (mode_index * states.len() + state_index) as u8; body_bytes];
            cases.push(StagedObjectWriterPut {
                label,
                key,
                state,
                wire_body: body.clone(),
                body,
                success_status: 200,
                flushed_prefix_bytes: prefix_bytes,
                baseline,
                request: None,
                body_open: true,
            });
        }
    }
    for case in &mut cases {
        case.request = Some(
            open_alt_flushed_put(
                bucket,
                &case.key,
                &case.body,
                case.flushed_prefix_bytes,
                &[],
            )
            .await,
        );
    }
    cases
}

async fn open_object_writer_chunked_put_cases(
    bucket: &str,
    transition: &str,
) -> Vec<StagedObjectWriterPut> {
    let states = [
        TimingDestinationState::Absent,
        TimingDestinationState::Live,
        TimingDestinationState::DeleteMarker,
    ];
    let mut cases = Vec::with_capacity(states.len());
    for (state_index, state) in states.into_iter().enumerate() {
        let label = format!("{transition}-aws-chunked-{}", state.label());
        let key = format!("object-writer-{label}");
        let baseline = prepare_timing_destination(bucket, &key, state).await;
        let body = vec![b'k' + state_index as u8; STAGED_PUT_BYTES];
        let url = object_url(CTX.endpoint(), bucket, &key, None);
        let signed = sign_aws_chunked_request_with_credentials(
            "PUT",
            &url,
            &[
                &body[..STAGED_PUT_PREFIX_BYTES],
                &body[STAGED_PUT_PREFIX_BYTES..],
            ],
            std::iter::empty::<(&str, &str)>(),
            raw_alt_credentials(),
        );
        let headers = signed.headers().collect::<Vec<_>>();
        let request = open_flushed_partial_request(
            "PUT",
            &url,
            signed.wire_body().len(),
            &signed.wire_body()[..signed.first_chunk_wire_len()],
            &headers,
            CTX.tls_ca_pem(),
        )
        .await
        .expect("open and flush signed raw aws-chunked PutObject prefix");
        cases.push(StagedObjectWriterPut {
            label,
            key,
            state,
            body,
            wire_body: signed.wire_body().to_vec(),
            success_status: 200,
            flushed_prefix_bytes: signed.first_chunk_wire_len(),
            baseline,
            request: Some(request),
            body_open: true,
        });
    }
    cases
}

async fn open_object_writer_post_object_cases(
    bucket: &str,
    transition: &str,
) -> Vec<StagedObjectWriterPut> {
    let states = [
        TimingDestinationState::Absent,
        TimingDestinationState::Live,
        TimingDestinationState::DeleteMarker,
    ];
    let mut cases = Vec::with_capacity(states.len());
    for (state_index, state) in states.into_iter().enumerate() {
        let label = format!("{transition}-post-object-{}", state.label());
        let key = format!("object-writer-{label}");
        let baseline = prepare_timing_destination(bucket, &key, state).await;
        let body = vec![b'p' + state_index as u8; STAGED_PUT_BYTES];
        let fields = sigv4_post_fields_for_credentials(
            CTX.alt_access_key(),
            CTX.alt_secret_key(),
            CTX.region(),
            bucket,
            &key,
            &[],
        );
        let field_refs = fields
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        let (content_type, wire_body, file_offset) =
            build_post_object_multipart_body(&field_refs, &body, "timing.bin");
        let flushed_prefix_bytes = file_offset + STAGED_PUT_PREFIX_BYTES;
        let url = format!("{}/{}", CTX.endpoint().trim_end_matches('/'), bucket);
        let headers = [("content-type", content_type.as_str())];
        let request = open_flushed_partial_request(
            "POST",
            &url,
            wire_body.len(),
            &wire_body[..flushed_prefix_bytes],
            &headers,
            CTX.tls_ca_pem(),
        )
        .await
        .expect("open and flush raw POST Object prefix");
        cases.push(StagedObjectWriterPut {
            label,
            key,
            state,
            body,
            wire_body,
            success_status: 204,
            flushed_prefix_bytes,
            baseline,
            request: Some(request),
            body_open: true,
        });
    }
    cases
}

async fn finish_object_writer_put_case(
    mut case: StagedObjectWriterPut,
) -> (StagedObjectWriterPut, FlushedResponse) {
    let mut request = case
        .request
        .take()
        .expect("staged ObjectWriter PutObject request");
    let response_visible = request
        .response_status_within(Duration::from_millis(100))
        .await
        .unwrap()
        .is_some();
    let response = if response_visible || !case.body_open {
        request
            .read_response()
            .await
            .expect("read early ObjectWriter PutObject response")
    } else {
        let written = request.written_body_bytes();
        if request
            .write_and_flush(&case.wire_body[written..])
            .await
            .is_ok()
        {
            request
                .finish_and_read_response()
                .await
                .expect("read ObjectWriter PutObject response after body completion")
        } else {
            request
                .read_response()
                .await
                .expect("read early ObjectWriter PutObject response")
        }
    };
    (case, response)
}

async fn wait_while_keeping_put_cases_active<F>(wait: F, cases: &mut [StagedObjectWriterPut])
where
    F: Future<Output = ()>,
{
    let mut wait = Box::pin(wait);
    loop {
        tokio::select! {
            () = &mut wait => return,
            () = tokio::time::sleep(Duration::from_secs(1)) => {
                for case in cases.iter_mut().filter(|case| case.body_open) {
                    let request = case
                        .request
                        .as_mut()
                        .expect("staged ObjectWriter PutObject request");
                    let written = request.written_body_bytes();
                    if written == case.wire_body.len() {
                        continue;
                    }
                    case.body_open = request
                        .write_and_flush(&case.wire_body[written..written + 1])
                        .await
                        .is_ok();
                }
            }
        }
    }
}

async fn cleanup_auth_timing_bucket(bucket: &str, keys: &[&str]) {
    let _ = CTX
        .client()
        .delete_bucket_policy()
        .bucket(bucket)
        .send_retrying_operation_aborted("delete auth timing bucket policy")
        .await;
    for key in keys {
        let _ = delete_object_retrying_operation_aborted(CTX.client(), bucket, key).await;
    }
    delete_bucket_retrying_operation_aborted(CTX.client(), bucket).await;
}

async fn cleanup_versioned_auth_timing_bucket(bucket: &str) {
    let _ = CTX
        .client()
        .delete_bucket_policy()
        .bucket(bucket)
        .send_retrying_operation_aborted("delete versioned auth timing bucket policy")
        .await;
    cleanup_versioned_bucket(CTX.client(), bucket).await;
}

async fn assert_timing_dependent_put_result(
    bucket: &str,
    key: &str,
    expected_body: &[u8],
    response: &FlushedResponse,
) {
    match response.status() {
        200 => {
            let object = CTX
                .client()
                .get_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .expect("successful timing-dependent PutObject must be readable");
            let body = object.body.collect().await.unwrap().into_bytes();
            assert_eq!(&body[..], expected_body);
        }
        403 => {
            let response_body = std::str::from_utf8(response.body())
                .expect("timing-dependent PutObject error response must be UTF-8");
            assert_eq!(
                s3_tests::shape::xml_tag_text(response_body, "Code"),
                Some("AccessDenied"),
                "unexpected timing-dependent PutObject error: {response_body}"
            );
            let head = CTX
                .client()
                .head_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await;
            assert_eq!(err_status(&head), 404, "{head:?}");
        }
        status => panic!("unexpected timing-dependent PutObject status {status}"),
    }
}

#[test]
fn test_get_object_stream_uses_acl_snapshot_after_strong_revocation() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt = CTX.alt_client();
        let bucket = create_acl_enabled_bucket(client, ObjectOwnership::ObjectWriter).await;
        let key = "streamed-get-acl-revocation";
        let payload = streamed_read_payload();

        s3_tests::retrying_operation_aborted("put ACL timing object", || {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(payload.clone()))
                .send()
        })
        .await;
        let initial_acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("get initial ACL timing object ACL")
            .await
            .unwrap();
        let owner_id = initial_acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected object owner canonical ID")
            .to_string();
        let alt_owner_id = canonical_owner_id(alt).await;
        client
            .put_object_acl()
            .bucket(&bucket)
            .key(key)
            .access_control_policy(object_acl(
                &owner_id,
                vec![
                    canonical_user_grant(&owner_id, Permission::FullControl),
                    canonical_user_grant(&alt_owner_id, Permission::Read),
                ],
            ))
            .send_retrying_operation_aborted("grant alternate read on ACL timing object")
            .await
            .unwrap();
        wait_for_head_allowed(alt, &bucket, key).await;

        let in_flight = alt
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("start ACL-authorized streamed GetObject")
            .await
            .unwrap();

        client
            .put_object_acl()
            .bucket(&bucket)
            .key(key)
            .acl(ObjectCannedAcl::Private)
            .send_retrying_operation_aborted("revoke alternate read on ACL timing object")
            .await
            .unwrap();
        let current_acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("read back revoked ACL timing object ACL")
            .await
            .unwrap();
        assert!(
            !current_acl.grants().iter().any(|grant| {
                grant.permission() == Some(&Permission::Read)
                    && grant
                        .grantee()
                        .is_some_and(|grantee| grantee.id() == Some(alt_owner_id.as_str()))
            }),
            "alternate READ grant remained after private ACL replacement: {:?}",
            current_acl.grants()
        );
        let fresh = alt.head_object().bucket(&bucket).key(key).send().await;
        assert_eq!(err_status(&fresh), 403, "{fresh:?}");

        let body = in_flight.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], payload.as_slice());

        delete_object_retrying_operation_aborted(client, &bucket, key)
            .await
            .unwrap();
        delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_get_object_stream_uses_existing_tag_snapshot_after_strong_mutation() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt = CTX.alt_client();
        let bucket = create_boe_bucket(client).await;
        let key = "streamed-get-existing-tag-mutation";
        let payload = streamed_read_payload();

        s3_tests::retrying_operation_aborted("put existing-tag timing object", || {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(payload.clone()))
                .send()
        })
        .await;
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(object_tagging("security", "allow"))
            .send_retrying_operation_aborted("set allowing existing tag")
            .await
            .unwrap();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(
                json!({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": alt_principal(),
                        "Action": "s3:GetObject",
                        "Resource": format!("arn:aws:s3:::{bucket}/{key}"),
                        "Condition": {
                            "StringEquals": {
                                "s3:ExistingObjectTag/security": "allow"
                            }
                        }
                    }],
                })
                .to_string(),
            )
            .send_retrying_operation_aborted("set existing-tag timing bucket policy")
            .await
            .unwrap();
        wait_for_head_allowed(alt, &bucket, key).await;

        let in_flight = alt
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("start tag-authorized streamed GetObject")
            .await
            .unwrap();

        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(object_tagging("security", "deny"))
            .send_retrying_operation_aborted("replace allowing existing tag")
            .await
            .unwrap();
        let current_tags = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("read back replaced existing tag")
            .await
            .unwrap();
        assert_eq!(current_tags.tag_set().len(), 1);
        assert_eq!(current_tags.tag_set()[0].key(), "security");
        assert_eq!(current_tags.tag_set()[0].value(), "deny");
        let fresh = alt.head_object().bucket(&bucket).key(key).send().await;
        assert_eq!(err_status(&fresh), 403, "{fresh:?}");

        let body = in_flight.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], payload.as_slice());

        cleanup_auth_timing_bucket(&bucket, &[key]).await;
    });
}

#[test]
fn test_boe_inflight_put_policy_revocation_preserves_latched_outcome_state() {
    s3_tests::run(async {
        let bucket = create_boe_bucket(CTX.client()).await;
        let client = ordinary_alt_client();
        let allow_canary = "revocation-allow-canary";
        let deny_canary = "revocation-deny-canary";
        let key = "inflight-policy-revocation";
        let total_bytes = STAGED_PUT_BYTES;

        set_alt_put_policy(&bucket, "Allow").await;
        wait_for_put_allowed(&client, &bucket, allow_canary, Duration::from_secs(2)).await;

        let body = vec![b'k'; total_bytes];
        let mut request =
            open_alt_flushed_put(&bucket, key, &body, STAGED_PUT_PREFIX_BYTES, &[]).await;
        assert!(
            request
                .response_status_within(Duration::from_millis(250))
                .await
                .unwrap()
                .is_none(),
            "in-flight PutObject completed before policy revocation"
        );

        set_alt_put_policy(&bucket, "Deny").await;
        let soak = revocation_soak_duration();
        let (keepalive_bytes, body_open) = wait_for_put_denied_while_keeping_body_active(
            &client,
            &bucket,
            deny_canary,
            soak,
            &mut request,
        )
        .await;

        let response_visible = request
            .response_status_within(RESPONSE_STAGE_WINDOW)
            .await
            .unwrap()
            .is_some();
        let response = if response_visible || !body_open {
            request
                .read_response()
                .await
                .expect("read early in-flight PutObject response")
        } else if request
            .write_and_flush(&body[STAGED_PUT_PREFIX_BYTES + keepalive_bytes..total_bytes])
            .await
            .is_ok()
        {
            request
                .finish_and_read_response()
                .await
                .expect("read in-flight PutObject response after body completion")
        } else {
            request
                .read_response()
                .await
                .expect("read early in-flight PutObject response")
        };
        let status = response.status();
        println!(
            "BOE in-flight PutObject after converged policy revocation status: {status}, \
                 deny soak: {soak:?}"
        );
        assert_timing_dependent_put_result(&bucket, key, &body, &response).await;

        cleanup_auth_timing_bucket(&bucket, &[allow_canary, deny_canary, key]).await;
    });
}

#[test]
fn test_boe_inflight_put_policy_grant_preserves_latched_outcome_state() {
    s3_tests::run(async {
        let bucket = create_boe_bucket(CTX.client()).await;
        let client = ordinary_alt_client();
        let deny_canary = "grant-deny-canary";
        let allow_canary = "grant-allow-canary";
        let key = "inflight-policy-grant";
        let total_bytes = STAGED_PUT_BYTES;

        set_alt_put_policy(&bucket, "Deny").await;
        wait_for_put_denied(&client, &bucket, deny_canary, Duration::from_secs(2)).await;

        let body = vec![b'g'; total_bytes];
        let mut request =
            open_alt_flushed_put(&bucket, key, &body, STAGED_PUT_PREFIX_BYTES, &[]).await;
        let early_status = request
            .response_status_within(RESPONSE_STAGE_WINDOW)
            .await
            .unwrap();

        set_alt_put_policy(&bucket, "Allow").await;
        wait_for_put_allowed(&client, &bucket, allow_canary, Duration::from_secs(5)).await;

        let response = if early_status.is_some() {
            request
                .read_response()
                .await
                .expect("read early policy-denied PutObject response")
        } else if request
            .write_and_flush(&body[STAGED_PUT_PREFIX_BYTES..])
            .await
            .is_ok()
        {
            request
                .finish_and_read_response()
                .await
                .expect("read policy-granted PutObject response")
        } else {
            request
                .read_response()
                .await
                .expect("read early policy-denied PutObject response")
        };
        let status = response.status();
        println!("BOE in-flight PutObject after converged policy grant status: {status}");
        assert_timing_dependent_put_result(&bucket, key, &body, &response).await;

        cleanup_auth_timing_bucket(&bucket, &[deny_canary, allow_canary, key]).await;
    });
}

async fn run_object_writer_put_policy_transition_once(
    encoding: TimingPutEncoding,
    transition: &str,
    initial_effect: &str,
    final_effect: &str,
) -> bool {
    let client = ordinary_alt_client();
    let bucket = create_acl_enabled_bucket(CTX.client(), ObjectOwnership::ObjectWriter).await;
    enable_bucket_versioning(CTX.client(), &bucket).await;
    let initial_canary = format!("object-writer-{transition}-initial-canary");
    let final_canary = format!("object-writer-{transition}-final-canary");

    set_alt_put_policy(&bucket, initial_effect).await;
    match initial_effect {
        "Allow" => {
            wait_for_put_allowed(&client, &bucket, &initial_canary, Duration::from_secs(2)).await
        }
        "Deny" => {
            wait_for_put_denied(&client, &bucket, &initial_canary, Duration::from_secs(2)).await
        }
        effect => panic!("unsupported initial timing policy effect {effect}"),
    }

    let mut cases = match encoding {
        TimingPutEncoding::Plain => open_object_writer_plain_put_cases(&bucket, transition).await,
        TimingPutEncoding::AwsChunked => {
            open_object_writer_chunked_put_cases(&bucket, transition).await
        }
        TimingPutEncoding::PostObject => {
            open_object_writer_post_object_cases(&bucket, transition).await
        }
    };
    set_alt_put_policy(&bucket, final_effect).await;
    match final_effect {
        "Allow" => {
            wait_while_keeping_put_cases_active(
                wait_for_put_allowed(&client, &bucket, &final_canary, Duration::from_secs(5)),
                &mut cases,
            )
            .await
        }
        "Deny" => {
            wait_while_keeping_put_cases_active(
                wait_for_put_denied(&client, &bucket, &final_canary, revocation_soak_duration()),
                &mut cases,
            )
            .await
        }
        effect => panic!("unsupported final timing policy effect {effect}"),
    }

    let mut finishing = tokio::task::JoinSet::new();
    for case in cases {
        finishing.spawn(finish_object_writer_put_case(case));
    }
    let mut results = Vec::new();
    while let Some(result) = finishing.join_next().await {
        results.push(result.expect("finish staged ObjectWriter request task"));
    }

    let mut saw_retryable_contention = false;
    for (case, response) in results {
        println!(
            "ObjectWriter {} {transition} {} status: {}",
            encoding.label(),
            case.label,
            response.status()
        );
        saw_retryable_contention |= matches!(
            assert_object_writer_timing_put_result(&bucket, &case, &response).await,
            TimingPutOutcome::RetryableContention
        );
    }

    cleanup_versioned_auth_timing_bucket(&bucket).await;
    !saw_retryable_contention
}

async fn run_object_writer_put_policy_transition(
    encoding: TimingPutEncoding,
    transition: &str,
    initial_effect: &str,
    final_effect: &str,
) {
    const MAX_ATTEMPTS: usize = 3;

    for attempt in 1..=MAX_ATTEMPTS {
        if run_object_writer_put_policy_transition_once(
            encoding,
            transition,
            initial_effect,
            final_effect,
        )
        .await
        {
            return;
        }
        if attempt < MAX_ATTEMPTS {
            println!(
                "ObjectWriter {} {transition} attempt {attempt}/{MAX_ATTEMPTS} encountered SlowDown; retrying the complete transition",
                encoding.label()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    panic!(
        "ObjectWriter {} {transition} did not produce complete authorization outcomes after {MAX_ATTEMPTS} attempts",
        encoding.label()
    );
}

#[test]
fn test_object_writer_plain_put_policy_revocation_preserves_permitted_state() {
    s3_tests::run(run_object_writer_put_policy_transition(
        TimingPutEncoding::Plain,
        "allow-to-deny",
        "Allow",
        "Deny",
    ));
}

#[test]
fn test_object_writer_plain_put_policy_grant_preserves_permitted_state() {
    s3_tests::run(run_object_writer_put_policy_transition(
        TimingPutEncoding::Plain,
        "deny-to-allow",
        "Deny",
        "Allow",
    ));
}

#[test]
fn test_object_writer_aws_chunked_put_policy_revocation_preserves_permitted_state() {
    s3_tests::run(run_object_writer_put_policy_transition(
        TimingPutEncoding::AwsChunked,
        "allow-to-deny",
        "Allow",
        "Deny",
    ));
}

#[test]
fn test_object_writer_aws_chunked_put_policy_grant_preserves_permitted_state() {
    s3_tests::run(run_object_writer_put_policy_transition(
        TimingPutEncoding::AwsChunked,
        "deny-to-allow",
        "Deny",
        "Allow",
    ));
}

#[test]
fn test_object_writer_post_object_policy_revocation_preserves_permitted_state() {
    s3_tests::run(run_object_writer_put_policy_transition(
        TimingPutEncoding::PostObject,
        "allow-to-deny",
        "Allow",
        "Deny",
    ));
}

#[test]
fn test_object_writer_post_object_policy_grant_preserves_permitted_state() {
    s3_tests::run(run_object_writer_put_policy_transition(
        TimingPutEncoding::PostObject,
        "deny-to-allow",
        "Deny",
        "Allow",
    ));
}
