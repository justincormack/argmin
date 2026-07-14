/// HTTP frontend: parses requests, authenticates, dispatches to coordinator.
pub mod chunked;
pub mod conditional;
pub mod multipart;
pub mod request;
pub mod response;
pub mod router;
pub mod serve;
pub mod xml;

use std::time::{Instant, SystemTime, UNIX_EPOCH};
use std::{
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll},
};

use auth::{authenticate_request, AuthContext, AuthMode, CredentialStore};
use bytes::Bytes;
use hyper::body::{Body, Frame, SizeHint};

use crate::coordinator::BeginStreamPartRequest;
use crate::coordinator::BucketRequest;
use crate::coordinator::ChecksumClaim;
use crate::coordinator::Coordinator;
use crate::coordinator::CopyObjectRequest;
use crate::coordinator::CopySource;
use crate::coordinator::EncodedChecksumClaim;
use crate::coordinator::FinalizeStreamPartRequest;
use crate::coordinator::MetadataDirective;
use crate::coordinator::MultipartObjectRequest;
use crate::coordinator::ObjectRequest;
use crate::coordinator::ObjectVersionRequest;
use crate::coordinator::PutObjectPolicyContext;
use crate::coordinator::TaggingDirective;
use crate::coordinator::UploadPartCopyRequest;
use crate::coordinator::{AuthorizePutObjectRequest, AuthorizedPutObjectWrite};
use crate::error::{ManagedEncryptionReadHeader, ManagedEncryptionReadHeaderContext, ServerError};
use crate::metadata_blob::{MetadataBlob, USER_METADATA_SIZE_LIMIT};
use checksum::{ChecksumAlgorithm, ChecksumType, MultipartChecksumConfig, RawChecksum};
use conditional::{
    copy_source_condition_from_headers, delete_condition_from_headers, read_condition_from_headers,
    write_condition_from_headers,
};
use md5_legacy::Digest;
use request::{S3Request, TransportSecurity};
use response::{ErrorDiagnostic, S3Response, WireResponseIds};
use router::{route, S3Operation};
use s3_types::{
    requires_sigv4, BucketLifecycleConfiguration, BucketNamespace, LegalHoldStatus, ObjectLockMode,
    ObjectLockState, ObjectRetention, StoredLegalHoldStatus, VersionId, WebsiteRedirectLocation,
    WebsiteRedirectLocationError, WEBSITE_REDIRECT_LOCATION_HEADER_NAME,
};
use server_core::sse::{
    SseCustomerRequest, SseCustomerWriteContext, SSE_CUSTOMER_ALGORITHM, SSE_C_CUSTOMER_KEY_LEN,
};
use server_core::system_metadata::{
    is_checksum_algorithm_header_name, is_checksum_value_header_name,
    is_system_metadata_header_name, SystemMetadata,
};
use storage::{
    BucketName, ManagedEncryptionAlgorithm, ObjectKey, SessionId, StorageCluster, UploadId,
};
use tokio::sync::{mpsc, OwnedSemaphorePermit};

const TRACE_TARGET: &str = "server_http";
const S3_MAX_LIST_KEYS: u32 = 1_000;
const SLOW_REQUEST_EVENT_THRESHOLD_US: u128 = 5_000_000;
const SYSTEM_METADATA_SIZE_LIMIT: usize = 2 * 1024;
const INVALID_REDIRECT_LOCATION_MESSAGE: &str =
    "The website redirect location must have a prefix of 'http://' or 'https://' or '/'.";
const REQUEST_ID_ALPHABET: &[u8; 36] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const AWS_SERVER_HEADER_VALUE: &str = "AmazonS3";

#[cfg(test)]
thread_local! {
    static SUPPRESS_EXPECTED_PANIC_ON_500_DIAGNOSTICS: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

fn should_dump_panic_on_500_flight_recorder() -> bool {
    #[cfg(test)]
    {
        if SUPPRESS_EXPECTED_PANIC_ON_500_DIAGNOSTICS.with(std::cell::Cell::get) {
            return false;
        }
    }
    true
}

pub(crate) fn new_request_trace_context() -> observability::TraceContext {
    use ring::rand::SecureRandom;

    let rng = ring::rand::SystemRandom::new();
    let mut trace_bytes = [0u8; 16];
    rng.fill(&mut trace_bytes)
        .expect("system randomness is available");

    let mut request_id = String::with_capacity(16);
    let mut random_bytes = [0u8; 32];
    while request_id.len() < 16 {
        rng.fill(&mut random_bytes)
            .expect("system randomness is available");
        for byte in random_bytes {
            if byte < 252 {
                request_id.push(REQUEST_ID_ALPHABET[(byte % 36) as usize] as char);
                if request_id.len() == 16 {
                    break;
                }
            }
        }
    }

    let mut trace_id = String::with_capacity(32);
    for byte in trace_bytes {
        use std::fmt::Write;
        let _ = write!(trace_id, "{byte:02x}");
    }

    observability::TraceContext::from_ids(observability::TraceContextIds {
        trace_id,
        request_id,
    })
}

#[must_use]
pub fn new_host_id() -> String {
    use base64::Engine as _;
    use ring::rand::SecureRandom;

    let rng = ring::rand::SystemRandom::new();
    let mut bytes = [0u8; 32];
    rng.fill(&mut bytes)
        .expect("system randomness is available");
    base64::engine::general_purpose::STANDARD_NO_PAD.encode(bytes)
}

fn current_trace_context() -> observability::TraceContext {
    observability::current_context().unwrap_or_else(new_request_trace_context)
}

/// Parse versionId query parameter from an S3 request.
/// Returns `Ok(None)` if the parameter is absent, `Ok(Some(id))` if valid,
/// or `Err` if the value is present but not a valid version ID.
/// Parse a version-id string into a typed `VersionId`.
fn parse_version_id_str(v: &str) -> Result<VersionId, ServerError> {
    parse_version_id_str_with_label(v, "versionId")
}

fn parse_version_id_str_with_label(v: &str, label: &str) -> Result<VersionId, ServerError> {
    v.parse::<VersionId>()
        .map_err(|_| ServerError::InvalidVersionId {
            argument_name: label.to_string(),
            argument_value: v.to_string(),
        })
}

fn parse_optional_version_id<S: AsRef<str>>(
    raw: Option<S>,
    label: &str,
) -> Result<Option<VersionId>, ServerError> {
    raw.map(|value| parse_version_id_str_with_label(value.as_ref(), label))
        .transpose()
}

fn parse_optional_u32<S: AsRef<str>>(
    raw: Option<S>,
    invalid_reason: &str,
) -> Result<Option<u32>, ServerError> {
    raw.map(|value| {
        value
            .as_ref()
            .parse::<u32>()
            .map_err(|_| ServerError::InvalidArgument {
                reason: invalid_reason.to_string(),
            })
    })
    .transpose()
}

fn parse_u32_or_default<S: AsRef<str>>(
    raw: Option<S>,
    default: u32,
    invalid_reason: &str,
) -> Result<u32, ServerError> {
    Ok(parse_optional_u32(raw, invalid_reason)?.unwrap_or(default))
}

fn parse_optional_upload_id_marker(raw: Option<&str>) -> Result<Option<UploadId>, ServerError> {
    raw.map(|value| {
        UploadId::try_from(value).map_err(|_| ServerError::InvalidArgument {
            reason: "Invalid uploadId marker".to_string(),
        })
    })
    .transpose()
}

fn canned_acl_and_header_grants_conflict() -> ServerError {
    ServerError::InvalidRequest {
        reason: "Specifying both Canned ACLs and Header Grants is not allowed".to_string(),
    }
}

fn acl_xml_and_header_grants_conflict() -> ServerError {
    ServerError::UnexpectedContent
}

fn parse_bucket_name(name: &str) -> Result<BucketName, ServerError> {
    BucketName::try_from(name.to_string()).map_err(|error| ServerError::InvalidBucketName {
        reason: error.to_string(),
    })
}

fn parse_bucket_resource_arn(resource_arn: &str) -> Result<BucketName, ServerError> {
    let bucket =
        resource_arn
            .strip_prefix("arn:aws:s3:::")
            .ok_or_else(|| ServerError::InvalidRequest {
                reason: format!("unsupported TagResource resource ARN: {resource_arn}"),
            })?;
    if bucket.is_empty() || bucket.contains('/') {
        return Err(ServerError::InvalidRequest {
            reason: format!("unsupported TagResource resource ARN: {resource_arn}"),
        });
    }
    parse_bucket_name(bucket)
}

fn validate_untag_resource_tag_keys(tag_keys: &[String]) -> Result<(), ServerError> {
    if tag_keys.is_empty() || tag_keys.iter().any(String::is_empty) {
        return Err(ServerError::InvalidTag {
            reason: "At least one tag is required.".to_string(),
            tag_key: None,
            tag_value: None,
        });
    }
    if tag_keys.len() > 50 {
        return Err(ServerError::InvalidTag {
            reason: "too many tagKeys in UntagResource request".to_string(),
            tag_key: None,
            tag_value: None,
        });
    }
    Ok(())
}

fn parse_object_key(key: &str) -> Result<ObjectKey, ServerError> {
    ObjectKey::try_from(key.to_string()).map_err(|error| ServerError::InvalidRequest {
        reason: error.to_string(),
    })
}

fn parse_copy_source_header(
    copy_source: &str,
) -> Result<(BucketName, ObjectKey, Option<VersionId>), ServerError> {
    let (src_bucket_raw, src_key, src_version_id_str) = request::parse_copy_source(copy_source)?;
    let src_bucket =
        BucketName::try_from(src_bucket_raw.clone()).map_err(|_| ServerError::BucketNotFound {
            name: src_bucket_raw,
        })?;
    let src_version_id = parse_optional_version_id(src_version_id_str, "versionId in copy source")?;
    Ok((src_bucket, src_key, src_version_id))
}

fn bucket_request<'a>(
    bucket: &BucketName,
    requester: crate::coordinator::Requester,
    expected_bucket_owner: Option<&'a str>,
) -> Result<BucketRequest<'a>, ServerError> {
    Ok(BucketRequest::new(
        bucket.clone(),
        requester,
        expected_bucket_owner,
    ))
}

fn object_request<'a>(
    bucket: &BucketName,
    key: &str,
    requester: crate::coordinator::Requester,
    expected_bucket_owner: Option<&'a str>,
) -> Result<ObjectRequest<'a>, ServerError> {
    Ok(ObjectRequest::new(
        bucket.clone(),
        parse_object_key(key)?,
        requester,
        expected_bucket_owner,
    ))
}

fn object_version_request<'a>(
    bucket: &BucketName,
    key: &str,
    version_id: Option<VersionId>,
    requester: crate::coordinator::Requester,
    expected_bucket_owner: Option<&'a str>,
) -> Result<ObjectVersionRequest<'a>, ServerError> {
    Ok(ObjectVersionRequest::new(
        bucket.clone(),
        parse_object_key(key)?,
        version_id,
        requester,
        expected_bucket_owner,
    ))
}

fn multipart_object_request<'a>(
    bucket: &BucketName,
    key: &str,
    upload_id: UploadId,
    requester: crate::coordinator::Requester,
    expected_bucket_owner: Option<&'a str>,
) -> Result<MultipartObjectRequest<'a>, ServerError> {
    Ok(MultipartObjectRequest::new(
        bucket.clone(),
        parse_object_key(key)?,
        upload_id,
        requester,
        expected_bucket_owner,
    ))
}

fn invalid_upload_id_error(upload_id: &str) -> ServerError {
    ServerError::NoSuchUpload {
        upload_id: upload_id.to_string(),
    }
}

fn parse_present_upload_id(upload_id: &str) -> Result<UploadId, ServerError> {
    UploadId::try_from(upload_id).map_err(|_| invalid_upload_id_error(upload_id))
}

fn parse_required_upload_id(raw: Option<&str>) -> Result<UploadId, ServerError> {
    let upload_id = raw.ok_or_else(|| ServerError::InvalidRequest {
        reason: "missing uploadId query parameter".to_string(),
    })?;
    parse_present_upload_id(upload_id)
}

fn current_auth_epoch_secs() -> Result<u64, ServerError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ServerError::Auth(auth::AuthError::RequestExpired))
        .map(|duration| duration.as_secs())
}

#[doc(hidden)]
pub fn fuzz_upload_id_query_entrypoints(
    multipart_query: &str,
    list_multipart_query: &str,
    upload_part_query: &str,
) {
    let multipart_upload_id = request::query_param_lossy(multipart_query, "uploadId");
    let _ = parse_required_upload_id(multipart_upload_id.as_deref());
    let upload_id_marker = request::query_param_lossy(list_multipart_query, "upload-id-marker");
    let _ = parse_optional_upload_id_marker(upload_id_marker.as_deref());
    if let Ok((upload_id_raw, _)) = request::parse_upload_part_query(upload_part_query) {
        let _ = parse_present_upload_id(upload_id_raw.as_str());
    }
}

const MAX_WRITE_REQUEST_HEADER_SECTION_SIZE: usize = 8 * 1024;

fn validate_write_request_header_section_size(headers: &[(&str, &str)]) -> Result<(), ServerError> {
    let mut total_header_section_size = 0usize;
    for (name, value) in headers {
        total_header_section_size = total_header_section_size
            .checked_add(name.len())
            .and_then(|size| size.checked_add(value.len()))
            .ok_or(ServerError::RequestHeaderSectionTooLarge)?;
        if total_header_section_size > MAX_WRITE_REQUEST_HEADER_SECTION_SIZE {
            return Err(ServerError::RequestHeaderSectionTooLarge);
        }
    }
    Ok(())
}

fn parse_website_redirect_location(value: &str) -> Result<WebsiteRedirectLocation, ServerError> {
    WebsiteRedirectLocation::new(value).map_err(|err| match err {
        WebsiteRedirectLocationError::Empty
        | WebsiteRedirectLocationError::InvalidHeaderBytes
        | WebsiteRedirectLocationError::InvalidPrefix => ServerError::InvalidRedirectLocation {
            reason: INVALID_REDIRECT_LOCATION_MESSAGE.to_string(),
        },
    })
}

fn parse_request_metadata<'a, I>(headers: I) -> Result<(MetadataBlob, SystemMetadata), ServerError>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let headers: Vec<(&str, &str)> = headers.into_iter().collect();
    let mut total_user_metadata_size = 0usize;
    let mut total_system_metadata_size = 0usize;
    for (name, value) in &headers {
        let lower = name.to_ascii_lowercase();
        if lower == WEBSITE_REDIRECT_LOCATION_HEADER_NAME {
            let _ = parse_website_redirect_location(value)?;
        }
        if lower.starts_with("x-amz-meta-") {
            let user_key_len = lower.trim_start_matches("x-amz-meta-").len();
            total_user_metadata_size = total_user_metadata_size
                .checked_add(user_key_len)
                .and_then(|size| size.checked_add(value.len()))
                .ok_or(ServerError::MetadataTooLargeDetailed {
                    size: usize::MAX,
                    max_size_allowed: USER_METADATA_SIZE_LIMIT,
                })?;
            if total_user_metadata_size > USER_METADATA_SIZE_LIMIT {
                return Err(ServerError::MetadataTooLargeDetailed {
                    size: total_user_metadata_size,
                    max_size_allowed: USER_METADATA_SIZE_LIMIT,
                });
            }
        } else if is_system_metadata_header_name(&lower) {
            total_system_metadata_size = total_system_metadata_size
                .checked_add(lower.len())
                .and_then(|size| size.checked_add(value.len()))
                .ok_or(ServerError::MetadataTooLargeDetailed {
                    size: usize::MAX,
                    max_size_allowed: SYSTEM_METADATA_SIZE_LIMIT,
                })?;
            if total_system_metadata_size > SYSTEM_METADATA_SIZE_LIMIT {
                return Err(ServerError::MetadataTooLargeDetailed {
                    size: total_system_metadata_size,
                    max_size_allowed: SYSTEM_METADATA_SIZE_LIMIT,
                });
            }
        }
    }
    Ok((
        MetadataBlob::from_header_iter(headers.iter().copied())?,
        SystemMetadata::from_header_iter(headers)?,
    ))
}

fn parse_put_object_request_metadata<'a, I>(
    headers: I,
) -> Result<(MetadataBlob, SystemMetadata), ServerError>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    parse_request_metadata(headers.into_iter().filter(|(name, _)| {
        !is_checksum_algorithm_header_name(name)
            && !name.eq_ignore_ascii_case("x-amz-checksum-type")
    }))
}

fn parse_request_metadata_without_checksum_headers<'a, I>(
    headers: I,
) -> Result<(MetadataBlob, SystemMetadata), ServerError>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    parse_request_metadata(headers.into_iter().filter(|(name, _)| {
        !is_checksum_algorithm_header_name(name)
            && !name.eq_ignore_ascii_case("x-amz-checksum-type")
            && !is_checksum_value_header_name(name)
    }))
}

fn parse_version_id(req: &S3Request) -> Result<Option<VersionId>, ServerError> {
    parse_optional_version_id(req.query_param_lossy("versionId"), "versionId")
}

fn ensure_lifecycle_rule_ids(
    mut config: BucketLifecycleConfiguration,
) -> Result<BucketLifecycleConfiguration, ServerError> {
    for rule in &mut config.rules {
        if rule.id.is_none() {
            rule.id = Some(generate_lifecycle_rule_id()?);
        }
    }
    Ok(config)
}

fn generate_lifecycle_rule_id() -> Result<String, ServerError> {
    use base64::Engine;
    use ring::rand::SecureRandom;

    let mut bytes = [0u8; 16];
    let rng = ring::rand::SystemRandom::new();
    rng.fill(&mut bytes)
        .map_err(|_| ServerError::InternalError {
            reason: "failed to generate lifecycle rule ID".to_string(),
        })?;

    // Format as UUIDv4, then base64-encode the textual UUID without padding.
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let uuid = format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    );
    Ok(base64::engine::general_purpose::STANDARD_NO_PAD.encode(uuid))
}

fn expected_bucket_owner(req: &S3Request) -> Option<&str> {
    req.header("x-amz-expected-bucket-owner")
}

fn expected_source_bucket_owner(req: &S3Request) -> Option<&str> {
    req.header("x-amz-source-expected-bucket-owner")
}

fn required_account_id(req: &S3Request) -> Result<&str, ServerError> {
    if req.header_count("x-amz-account-id") > 1 {
        return Err(ServerError::InvalidArgument {
            reason: "x-amz-account-id must not be repeated".to_string(),
        });
    }
    req.header("x-amz-account-id")
        .ok_or_else(|| ServerError::InvalidRequest {
            reason: "Missing required header for this request: x-amz-account-id".to_string(),
        })
}

fn parse_bucket_namespace(req: &S3Request) -> Result<BucketNamespace, ServerError> {
    if req.header_count("x-amz-bucket-namespace") > 1 {
        return Err(ServerError::InvalidArgument {
            reason: "x-amz-bucket-namespace must not be repeated".to_string(),
        });
    }
    let Some(value) = req.header("x-amz-bucket-namespace") else {
        return Ok(BucketNamespace::Global);
    };
    value
        .parse::<BucketNamespace>()
        .map_err(|_| ServerError::InvalidArgument {
            reason: format!("invalid x-amz-bucket-namespace: {value}"),
        })
}

fn reject_directory_bucket_only_object_features(req: &S3Request) -> Result<(), ServerError> {
    const DIRECTORY_BUCKET_ONLY_OBJECT_HEADERS: [&str; 6] = [
        "x-amz-write-offset-bytes",
        "x-amz-rename-source",
        "x-amz-rename-source-if-match",
        "x-amz-rename-source-if-none-match",
        "x-amz-rename-source-if-modified-since",
        "x-amz-rename-source-if-unmodified-since",
    ];

    if let Some(header) = DIRECTORY_BUCKET_ONLY_OBJECT_HEADERS
        .into_iter()
        .find(|header| req.header(header).is_some())
    {
        return Err(ServerError::HeaderNotImplemented {
            header: header.to_string(),
        });
    }

    if req.query_param_lossy("renameObject").is_some() {
        return Err(ServerError::QueryParameterNotImplemented {
            query_parameter: "renameObject".to_string(),
        });
    }

    Ok(())
}

/// The HTTP frontend that handles incoming requests.
pub struct HttpFrontend {
    pub coordinator: Arc<Coordinator>,
    pub credentials: CredentialStore,
    pub host_id: Arc<str>,
}

enum S3HyperBodyState {
    Buffered(Option<Bytes>),
    Streaming(mpsc::Receiver<Result<Bytes, ServerError>>),
}

#[derive(Clone)]
pub struct ResponseTraceMeta {
    context: observability::TraceContext,
    host_id: Arc<str>,
    method: String,
    path: String,
    query: observability::QuerySummary,
    started_at: Instant,
}

impl ResponseTraceMeta {
    #[must_use]
    pub fn new(
        context: observability::TraceContext,
        host_id: Arc<str>,
        method: impl Into<String>,
        path: impl Into<String>,
        query: impl Into<String>,
    ) -> Self {
        let query = query.into();
        Self {
            context,
            host_id,
            method: method.into(),
            path: path.into(),
            query: observability::query_summary(&query),
            started_at: Instant::now(),
        }
    }
}

struct ResponseBodyTrace {
    meta: ResponseTraceMeta,
    status_code: u16,
    body_len: u64,
    bytes_sent: u64,
    streaming: bool,
    terminal_event_emitted: bool,
}

impl ResponseBodyTrace {
    fn new(meta: ResponseTraceMeta, status_code: u16, body_len: u64, streaming: bool) -> Self {
        Self {
            meta,
            status_code,
            body_len,
            bytes_sent: 0,
            streaming,
            terminal_event_emitted: false,
        }
    }

    fn worker_context(&self) -> observability::TraceContext {
        self.meta.context.clone()
    }

    fn elapsed_lifetime_us(&self) -> u128 {
        self.meta.started_at.elapsed().as_micros()
    }

    fn request_summary(&self) -> observability::RequestSummary<'_> {
        observability::RequestSummary {
            method: &self.meta.method,
            path: &self.meta.path,
            query: self.meta.query,
            status_code: self.status_code,
            streaming: self.streaming,
            body_len: self.body_len,
            bytes_sent: self.bytes_sent,
            lifetime_us: self.elapsed_lifetime_us(),
        }
    }

    fn emit_slow_request_if_needed(&self, outcome: &'static str, error_code: Option<&str>) {
        let summary = self.request_summary();
        if summary.lifetime_us < SLOW_REQUEST_EVENT_THRESHOLD_US {
            return;
        }
        let _ = observability::emit_slow_request(
            &self.meta.context,
            TRACE_TARGET,
            summary,
            outcome,
            error_code,
        );
    }

    fn record_bytes(&mut self, len: usize) {
        self.bytes_sent += len as u64;
    }

    fn emit_finish(&mut self, outcome: &'static str) {
        if self.terminal_event_emitted {
            return;
        }
        self.terminal_event_emitted = true;
        self.emit_slow_request_if_needed(outcome, None);
        let _ = observability::emit_request_finish(
            &self.meta.context,
            TRACE_TARGET,
            self.request_summary(),
            outcome,
        );
    }

    fn emit_error(&mut self, err: &ServerError) {
        let diagnostic = ErrorDiagnostic {
            status_code: err.http_status(),
            error_code: err.s3_error_code(),
            cause_label: err.diagnostic_cause_label(),
            cause_chain: err.diagnostic_cause_chain(),
            server_detail: err.server_storage_rpc_detail(),
        };
        self.emit_error_diagnostic(&diagnostic);
    }

    fn emit_error_diagnostic(&mut self, diagnostic: &ErrorDiagnostic) {
        if self.terminal_event_emitted {
            return;
        }
        self.terminal_event_emitted = true;
        self.emit_slow_request_if_needed("error", Some(diagnostic.error_code));
        let summary = self.request_summary();
        let _ = observability::emit_request_error(
            &self.meta.context,
            TRACE_TARGET,
            summary,
            "response_body",
            diagnostic.error_code,
            diagnostic.cause_label,
        );
        if summary.status_code == 500 {
            let _ = observability::emit_http_500_cause_chain(
                &self.meta.context,
                TRACE_TARGET,
                summary,
                diagnostic.cause_label,
                &diagnostic.cause_chain,
            );
            if let Some(server_detail) = diagnostic.server_detail.as_deref() {
                let _ = observability::emit_http_500_server_detail(
                    &self.meta.context,
                    TRACE_TARGET,
                    summary,
                    server_detail,
                );
            }
        }
    }
}

pub struct S3HyperBody {
    state: S3HyperBodyState,
    trace: Option<ResponseBodyTrace>,
    _inflight_requests_guard: Option<observability::InflightRequestsGuard>,
    _permit: Option<OwnedSemaphorePermit>,
}

impl S3HyperBody {
    fn buffered(
        body: Vec<u8>,
        permit: Option<OwnedSemaphorePermit>,
        inflight_requests_guard: Option<observability::InflightRequestsGuard>,
        trace: ResponseBodyTrace,
    ) -> Self {
        Self {
            state: S3HyperBodyState::Buffered(Some(Bytes::from(body))),
            trace: Some(trace),
            _inflight_requests_guard: inflight_requests_guard,
            _permit: permit,
        }
    }

    fn streaming(
        body: crate::coordinator::ReadHandle,
        permit: Option<OwnedSemaphorePermit>,
        inflight_requests_guard: Option<observability::InflightRequestsGuard>,
        read_chunk_size: usize,
        trace: ResponseBodyTrace,
    ) -> Self {
        let worker_trace = trace.worker_context();
        let (tx, rx) = mpsc::channel(2);
        tokio::task::spawn_blocking(move || {
            let _trace = observability::AttachedTrace::new(worker_trace);
            observability::trace_scope!(
                TRACE_TARGET,
                "S3HyperBody::streaming",
                "read_chunk_size={}",
                read_chunk_size
            );
            let mut body = body;
            loop {
                match body.next_chunk(read_chunk_size) {
                    Ok(Some(chunk)) => {
                        if tx.blocking_send(Ok(Bytes::from_owner(chunk))).is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        let _ = tx.blocking_send(Err(err));
                        break;
                    }
                }
            }
        });

        Self {
            state: S3HyperBodyState::Streaming(rx),
            trace: Some(trace),
            _inflight_requests_guard: inflight_requests_guard,
            _permit: permit,
        }
    }
}

impl Body for S3HyperBody {
    type Data = Bytes;
    type Error = ServerError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        match &mut this.state {
            S3HyperBodyState::Buffered(bytes) => match bytes.take() {
                Some(bytes) if !bytes.is_empty() => {
                    if let Some(trace) = this.trace.as_mut() {
                        trace.record_bytes(bytes.len());
                    }
                    Poll::Ready(Some(Ok(Frame::data(bytes))))
                }
                _ => {
                    if let Some(trace) = this.trace.as_mut() {
                        trace.emit_finish("complete");
                    }
                    Poll::Ready(None)
                }
            },
            S3HyperBodyState::Streaming(rx) => match Pin::new(rx).poll_recv(cx) {
                Poll::Ready(Some(Ok(bytes))) => {
                    if let Some(trace) = this.trace.as_mut() {
                        trace.record_bytes(bytes.len());
                    }
                    Poll::Ready(Some(Ok(Frame::data(bytes))))
                }
                Poll::Ready(Some(Err(err))) => {
                    if let Some(trace) = this.trace.as_mut() {
                        trace.emit_error(&err);
                    }
                    Poll::Ready(Some(Err(err)))
                }
                Poll::Ready(None) => {
                    if let Some(trace) = this.trace.as_mut() {
                        trace.emit_finish("complete");
                    }
                    Poll::Ready(None)
                }
                Poll::Pending => Poll::Pending,
            },
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.state {
            S3HyperBodyState::Buffered(bytes) => bytes.is_none(),
            S3HyperBodyState::Streaming(_) => false,
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &self.state {
            S3HyperBodyState::Buffered(Some(bytes)) => SizeHint::with_exact(bytes.len() as u64),
            S3HyperBodyState::Buffered(None) => SizeHint::with_exact(0),
            S3HyperBodyState::Streaming(_) => SizeHint::default(),
        }
    }
}

impl Drop for S3HyperBody {
    fn drop(&mut self) {
        let Some(trace) = self.trace.as_mut() else {
            return;
        };
        if trace.terminal_event_emitted {
            return;
        }
        if trace.bytes_sent == trace.body_len {
            trace.emit_finish("complete");
        } else {
            trace.emit_finish("dropped");
        }
    }
}

impl HttpFrontend {
    /// Handle a parsed S3 request: authenticate, dispatch, and return the response.
    ///
    /// The caller (serve layer) is responsible for parsing the HTTP request into
    /// an `S3Request` and converting the `S3Response` back to an HTTP response.
    #[must_use]
    pub fn handle_s3_request(&self, s3req: &S3Request, wire_ids: &WireResponseIds) -> S3Response {
        let query = observability::query_summary(s3req.query_string());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::handle_s3_request",
            "method={} path={:?} has_query={} query_params={} sigv4_query={}",
            s3req.method,
            s3req.path(),
            query.has_query(),
            query.param_count(),
            query.has_sigv4_params()
        );
        // Route first to detect OPTIONS requests (which bypass auth).
        let operation = {
            observability::trace_scope!(
                TRACE_TARGET,
                "HttpFrontend::route_request",
                "method={} path={:?} has_query={} query_params={} sigv4_query={}",
                s3req.method.as_str(),
                s3req.path(),
                query.has_query(),
                query.param_count(),
                query.has_sigv4_params()
            );
            match route(s3req.method.as_str(), s3req.path(), s3req.query_string()) {
                Ok(op) => op,
                Err(err) => return S3Response::error_with_ids(&err, s3req.path(), wire_ids),
            }
        };

        // OPTIONS (preflight CORS) bypasses authentication.
        if let S3Operation::OptionsRequest { ref bucket, .. } = operation {
            return self.handle_options_request(s3req, bucket, wire_ids);
        }

        let actual_cors_bucket = operation.bucket_name().cloned();
        // AWS reveals the bucket region on security-token auth errors only
        // for bucket-scoped requests to existing buckets; object-scoped
        // requests and unknown buckets omit the header.
        let token_error_bucket_region_bucket = if operation.object_key().is_none() {
            actual_cors_bucket.clone()
        } else {
            None
        };
        // AWS also reveals the bucket region on AccessDenied for the region
        // discovery surfaces — HeadBucket and ListObjects — in any auth mode
        // (probed anonymous and cross-account); bucket subresources and
        // other bucket-scoped writes omit it.
        let denied_bucket_region_bucket = match &operation {
            S3Operation::HeadBucket { bucket }
            | S3Operation::ListObjectsV1 { bucket }
            | S3Operation::ListObjectsV2 { bucket } => Some(bucket.clone()),
            _ => None,
        };
        let defer_region_check = self.should_defer_region_check(&operation);
        let auth = {
            observability::trace_scope!(
                TRACE_TARGET,
                "HttpFrontend::authenticate",
                "method={} path={:?}",
                s3req.method.as_str(),
                s3req.path()
            );
            self.authenticate(s3req, defer_region_check)
        };
        let result = match auth {
            Ok(auth) => {
                if defer_region_check {
                    if let Err(err) = self.enforce_bucket_region_for_operation(&operation, &auth) {
                        Err(err)
                    } else if let Err(err) = self.reject_streaming_fallthrough(s3req) {
                        Err(err)
                    } else {
                        self.dispatch_routed(s3req, &auth, operation)
                    }
                } else if let Err(err) = self.reject_streaming_fallthrough(s3req) {
                    Err(err)
                } else {
                    self.dispatch_routed(s3req, &auth, operation)
                }
            }
            Err(err) => Err(err),
        };
        let add_bucket_region_for_token_error = token_error_bucket_region_bucket
            .as_ref()
            .filter(|_| {
                matches!(
                    &result,
                    Err(ServerError::Auth(
                        auth::AuthError::UnexpectedSecurityToken { .. }
                    ))
                )
            })
            // A metadata lookup failure only suppresses the optional header;
            // the auth error response itself must still be returned.
            .is_some_and(|bucket| self.coordinator.bucket_exists(bucket).unwrap_or(false));
        let add_bucket_region_for_denied_discovery = denied_bucket_region_bucket
            .as_ref()
            .filter(|_| {
                matches!(
                    &result,
                    Err(err) if err.http_status() == 403 && err.s3_error_code() == "AccessDenied"
                )
            })
            .is_some_and(|bucket| self.coordinator.bucket_exists(bucket).unwrap_or(false));
        let mut resp = {
            observability::trace_scope!(
                TRACE_TARGET,
                "HttpFrontend::map_dispatch_result",
                "method={} path={:?}",
                s3req.method.as_str(),
                s3req.path()
            );
            match result {
                Ok(resp) => resp,
                Err(ServerError::NotModified {
                    ref etag,
                    last_modified,
                }) => S3Response::not_modified(etag, last_modified),
                Err(ServerError::PreconditionFailed { condition }) => {
                    S3Response::precondition_failed_with_ids(condition, wire_ids)
                }
                Err(ref err @ ServerError::DeleteMarkerHit { .. }) => {
                    let mut resp = S3Response::error_with_ids(err, s3req.path(), wire_ids);
                    resp.headers
                        .push(("x-amz-delete-marker".to_string(), "true".to_string()));
                    resp
                }
                Err(err) => {
                    if err.http_status() >= 500 {
                        let _ = observability::event(
                            TRACE_TARGET,
                            "dispatch_internal_error",
                            Some(format_args!(
                                "method={} path={:?} status={} code={} cause_label={}",
                                s3req.method.as_str(),
                                s3req.path(),
                                err.http_status(),
                                err.s3_error_code(),
                                err.diagnostic_cause_label()
                            )),
                        );
                    }
                    S3Response::error_with_ids(&err, s3req.path(), wire_ids)
                }
            }
        };
        if (add_bucket_region_for_token_error || add_bucket_region_for_denied_discovery)
            && !resp
                .headers
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case("x-amz-bucket-region"))
        {
            resp.headers.push((
                "x-amz-bucket-region".to_string(),
                self.coordinator.region().to_string(),
            ));
        }

        // CORS response headers on actual (non-preflight) requests.
        if let Some(origin) = s3req.header("origin") {
            if let Some(bucket) = actual_cors_bucket {
                observability::trace_scope!(
                    TRACE_TARGET,
                    "HttpFrontend::apply_actual_cors",
                    "method={} path={:?} bucket={:?}",
                    s3req.method.as_str(),
                    s3req.path(),
                    bucket
                );
                self.apply_cors_headers(&mut resp, &bucket, origin, s3req.method.as_str());
            }
        }

        resp
    }

    /// Handle an OPTIONS (CORS preflight) request. No auth required.
    fn handle_options_request(
        &self,
        req: &S3Request,
        bucket: &BucketName,
        wire_ids: &WireResponseIds,
    ) -> S3Response {
        let origin = match req.header("origin") {
            Some(o) => o,
            None => {
                return S3Response::error_with_ids(
                    &ServerError::InvalidRequest {
                        reason: "Insufficient information. Origin request header needed."
                            .to_string(),
                    },
                    req.path(),
                    wire_ids,
                );
            }
        };

        let request_method = match req.header("access-control-request-method") {
            Some(m) => m,
            None => return S3Response::forbidden_with_ids(wire_ids),
        };

        let request_headers_str = req.header("access-control-request-headers");
        let request_headers: Vec<&str> = request_headers_str
            .map(|h| h.split(',').map(str::trim).collect())
            .unwrap_or_default();

        // Load CORS config
        let cors_config_xml = match self.coordinator.load_bucket_cors_config(bucket) {
            Ok(Some(xml)) => xml,
            _ => return S3Response::forbidden_with_ids(wire_ids),
        };
        let config = match crate::http::xml::parse_cors_config_xml(cors_config_xml.as_bytes()) {
            Ok(c) => c,
            Err(_) => return S3Response::forbidden_with_ids(wire_ids),
        };

        match crate::cors::find_matching_rule(&config, origin, request_method, &request_headers) {
            Some(m) => {
                let headers = crate::cors::preflight_response_headers(
                    m.rule,
                    origin,
                    m.matched_origin,
                    request_headers_str,
                );
                let mut resp = S3Response::cors_preflight();
                for (k, v) in headers {
                    resp.headers.push((k, v));
                }
                resp
            }
            None => S3Response::forbidden_with_ids(wire_ids),
        }
    }

    /// Apply CORS headers to an actual (non-preflight) response if the request
    /// has an Origin header and a matching CORS rule exists.
    pub(crate) fn actual_cors_headers(
        &self,
        bucket: &BucketName,
        origin: &str,
        method: &str,
    ) -> Vec<(String, String)> {
        let cors_config_xml = match self.coordinator.load_bucket_cors_config(bucket) {
            Ok(Some(xml)) => xml,
            _ => return Vec::new(),
        };
        let config = match crate::http::xml::parse_cors_config_xml(cors_config_xml.as_bytes()) {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };

        if let Some(m) = crate::cors::find_matching_rule(&config, origin, method, &[]) {
            crate::cors::actual_response_headers(m.rule, origin, m.matched_origin)
        } else {
            Vec::new()
        }
    }

    fn apply_cors_headers(
        &self,
        resp: &mut S3Response,
        bucket: &BucketName,
        origin: &str,
        method: &str,
    ) {
        for (k, v) in self.actual_cors_headers(bucket, origin, method) {
            resp.headers.push((k, v));
        }
    }

    fn authenticated_account(
        auth: &AuthContext,
    ) -> Result<&s3_types::AccountIdentity, ServerError> {
        auth.account.as_ref().ok_or(ServerError::AccessDenied)
    }
}

fn auth_type_condition_value(mode: AuthMode) -> Option<&'static str> {
    match mode {
        AuthMode::HeaderSigV4 => Some("REST-HEADER"),
        AuthMode::PresignedSigV4 => Some("REST-QUERY-STRING"),
        AuthMode::PostSigV4 => Some("POST"),
        AuthMode::Anonymous => None,
    }
}

fn signature_version_condition_value(mode: AuthMode) -> Option<&'static str> {
    match mode {
        AuthMode::HeaderSigV4 | AuthMode::PresignedSigV4 | AuthMode::PostSigV4 => {
            Some("AWS4-HMAC-SHA256")
        }
        AuthMode::Anonymous => None,
    }
}

fn signature_age_millis(auth: &AuthContext, req: &S3Request) -> Option<u64> {
    if !matches!(auth.mode, AuthMode::PresignedSigV4 | AuthMode::PostSigV4) {
        return None;
    }
    let signed_epoch_seconds = auth.request_epoch_secs?;
    Some(
        req.request_epoch_seconds()
            .saturating_sub(signed_epoch_seconds)
            .saturating_mul(1000),
    )
}

impl HttpFrontend {
    fn requester_from_auth(
        &self,
        auth: &AuthContext,
        req: &S3Request,
    ) -> crate::coordinator::Requester {
        crate::coordinator::Requester::from_auth(auth)
            .with_source_ip(req.source_ip())
            .with_request_epoch_seconds(Some(req.request_epoch_seconds()))
            .with_secure_transport(Some(req.transport_security.is_secure()))
            .with_requested_region(Some(self.coordinator.region().to_string()))
            .with_referer(req.header("referer").map(str::to_string))
            .with_auth_type(auth_type_condition_value(auth.mode))
            .with_signature_version(signature_version_condition_value(auth.mode))
            .with_signature_age_millis(signature_age_millis(auth, req))
            .with_tls_version(
                req.tls_version
                    .map(|version| version.policy_value().to_string()),
            )
            .with_content_sha256(req.header("x-amz-content-sha256").map(str::to_string))
    }

    fn acl_owner_display_name(
        &self,
        owner_principal: &str,
        owner_canonical_id: &s3_types::CanonicalUserId,
    ) -> String {
        self.credentials
            .find_account_by_canonical_user_id(owner_canonical_id)
            .map(|account| account.display_name().to_string())
            .unwrap_or_else(|| owner_principal.to_string())
    }

    fn render_acl_grants(
        &self,
        owner_principal: &str,
        owner_canonical_id: &s3_types::CanonicalUserId,
        acl_grants: &s3_types::AclGrants,
    ) -> (String, Vec<xml::RenderedAclGrant>) {
        let owner_display_name = self.acl_owner_display_name(owner_principal, owner_canonical_id);
        let grants = acl_grants
            .iter()
            .map(|grant| {
                let display_name = match grant.grantee() {
                    s3_types::AclGrantee::CanonicalUser(id) if id == owner_canonical_id => {
                        Some(owner_display_name.clone())
                    }
                    s3_types::AclGrantee::CanonicalUser(id) => self
                        .credentials
                        .find_account_by_canonical_user_id(id)
                        .map(|account| account.display_name().to_string()),
                    s3_types::AclGrantee::AllUsers | s3_types::AclGrantee::AuthenticatedUsers => {
                        None
                    }
                };
                xml::RenderedAclGrant {
                    grantee: grant.grantee().clone(),
                    permission: grant.permission(),
                    display_name,
                }
            })
            .collect();
        (owner_display_name, grants)
    }

    fn render_multipart_uploads(
        &self,
        result: crate::coordinator::ListMultipartUploadsResult,
    ) -> xml::RenderedListMultipartUploadsResult {
        let uploads = result
            .uploads
            .into_iter()
            .map(|upload| {
                let owner = xml::RenderedCanonicalUser {
                    canonical_id: upload.owner.canonical_id.clone(),
                    display_name: None,
                };
                let initiator = xml::RenderedCanonicalUser {
                    canonical_id: upload.initiator.canonical_id.clone(),
                    display_name: self
                        .credentials
                        .find_account_by_canonical_user_id(&upload.initiator.canonical_id)
                        .map(|account| account.display_name().to_string()),
                };
                xml::RenderedMultipartUploadEntry {
                    key: upload.key,
                    upload_id: upload.upload_id.to_string(),
                    initiated: upload.initiated,
                    owner,
                    initiator,
                    checksum_algorithm: upload.checksum_algorithm,
                    checksum_type: upload.checksum_type,
                }
            })
            .collect();
        xml::RenderedListMultipartUploadsResult {
            uploads,
            is_truncated: result.is_truncated,
            next_key_marker: result.next_key_marker,
            next_upload_id_marker: result
                .next_upload_id_marker
                .map(|upload_id| upload_id.to_string()),
        }
    }

    fn dispatch_routed(
        &self,
        req: &S3Request,
        auth: &AuthContext,
        operation: S3Operation,
    ) -> Result<S3Response, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::dispatch_routed",
            "method={} path={:?} op={:?} principal={:?}",
            req.method.as_str(),
            req.path(),
            operation,
            auth.principal()
        );
        let expected_bucket_owner = expected_bucket_owner(req);
        // Dispatch to coordinator
        match operation {
            S3Operation::TagResource { resource_arn } => {
                let account_id = required_account_id(req)?;
                let bucket = parse_bucket_resource_arn(&resource_arn)?;
                let tags = xml::TagSet::parse_tag_resource_xml(&req.body)?;
                let requester = self.requester_from_auth(auth, req);
                let control = crate::coordinator::BucketTagControlRequest {
                    bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                    account_id,
                };
                let existing_tags = self
                    .coordinator
                    .get_bucket_tags_for_tag_resource(
                        &control,
                        tags.as_slice(),
                        auth::PolicyAction::TagResource,
                    )?
                    .map(|tagging_xml| xml::TagSet::parse_tagging_xml(tagging_xml.as_bytes(), 50))
                    .transpose()?
                    .unwrap_or_else(|| xml::TagSet::empty(50));
                let merged_tags = existing_tags.merge(&tags)?;
                let merged_xml = merged_tags.to_xml();
                self.coordinator.put_bucket_tags_for_tag_resource(
                    &crate::coordinator::PutBucketTagControlRequest {
                        control,
                        config: &merged_xml,
                        request_tags: tags.as_slice(),
                    },
                )?;
                Ok(S3Response::tag_resource())
            }
            S3Operation::UntagResource { resource_arn } => {
                let account_id = required_account_id(req)?;
                let bucket = parse_bucket_resource_arn(&resource_arn)?;
                let tag_keys = req
                    .query_params_lossy("tagKeys")
                    .into_iter()
                    .map(std::borrow::Cow::into_owned)
                    .collect::<Vec<_>>();
                validate_untag_resource_tag_keys(&tag_keys)?;
                let request_tags = tag_keys
                    .iter()
                    .map(|key| (key.clone(), String::new()))
                    .collect::<Vec<_>>();
                let requester = self.requester_from_auth(auth, req);
                let control = crate::coordinator::BucketTagControlRequest {
                    bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                    account_id,
                };
                let existing_tags = self
                    .coordinator
                    .get_bucket_tags_for_tag_resource(
                        &control,
                        request_tags.as_slice(),
                        auth::PolicyAction::UntagResource,
                    )?
                    .map(|tagging_xml| xml::TagSet::parse_tagging_xml(tagging_xml.as_bytes(), 50))
                    .transpose()?
                    .unwrap_or_else(|| xml::TagSet::empty(50));
                let remaining_tags = existing_tags.remove_keys(&tag_keys);
                if remaining_tags.is_empty() {
                    self.coordinator.delete_bucket_tags_for_untag_resource(
                        &crate::coordinator::UntagBucketTagControlRequest {
                            control,
                            request_tags: request_tags.as_slice(),
                        },
                    )?;
                } else {
                    let remaining_xml = remaining_tags.to_xml();
                    self.coordinator.put_bucket_tags_for_untag_resource(
                        &crate::coordinator::PutBucketTagsForUntagResourceRequest {
                            control,
                            config: &remaining_xml,
                            request_tags: request_tags.as_slice(),
                        },
                    )?;
                }
                Ok(S3Response::untag_resource())
            }
            S3Operation::ListBuckets => {
                let requester = self.requester_from_auth(auth, req);
                let owner_account = Self::authenticated_account(auth)?;
                let prefix = req.query_param_lossy("prefix");
                let mut buckets = self
                    .coordinator
                    .list_buckets(&crate::coordinator::ListBucketsRequest { requester })?;
                if let Some(ref prefix) = prefix {
                    buckets.retain(|bucket| bucket.name.as_str().starts_with(prefix.as_ref()));
                }
                Ok(S3Response::list_buckets(
                    &buckets,
                    owner_account.canonical_user_id(),
                    self.coordinator.region(),
                    prefix.as_deref(),
                ))
            }
            S3Operation::CreateBucket { bucket } => {
                let acl = parse_create_bucket_acl(req)?;
                let object_lock_enabled = parse_bucket_object_lock_enabled(
                    req.header("x-amz-bucket-object-lock-enabled"),
                )?;
                let namespace = parse_bucket_namespace(req)?;
                let ownership = parse_bucket_ownership(req.header("x-amz-object-ownership"))?;
                let requester = self.requester_from_auth(auth, req);
                self.coordinator
                    .create_bucket(&crate::coordinator::CreateBucketRequest {
                        name: bucket.clone(),
                        requester,
                        namespace,
                        acl,
                        ownership,
                        object_lock_enabled,
                    })?;
                Ok(S3Response::create_bucket(bucket.as_str()))
            }
            S3Operation::DeleteBucket { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                self.coordinator.delete_bucket(&bucket_request(
                    &bucket,
                    requester,
                    expected_bucket_owner,
                )?)?;
                Ok(S3Response::delete_bucket())
            }
            S3Operation::HeadBucket { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                let info = self.coordinator.head_bucket(&bucket_request(
                    &bucket,
                    requester,
                    expected_bucket_owner,
                )?)?;
                Ok(S3Response::head_bucket(&info, self.coordinator.region()))
            }
            S3Operation::GetBucketLocation { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                self.coordinator.get_bucket_location(&bucket_request(
                    &bucket,
                    requester,
                    expected_bucket_owner,
                )?)?;
                Ok(S3Response::get_bucket_location(self.coordinator.region()))
            }
            S3Operation::ListObjectsV1 { bucket } => {
                let prefix = req.query_param_lossy("prefix");
                let delimiter = req.query_param_lossy("delimiter").filter(|d| !d.is_empty());
                let marker = req.query_param_lossy("marker");
                let encoding_type = req.query_param_lossy("encoding-type");
                let allow_unordered = req.query_param_lossy("allow-unordered");
                if allow_unordered.is_some() && delimiter.is_some() {
                    return Err(ServerError::InvalidArgument {
                        reason: "allow-unordered is not supported with delimiter".to_string(),
                    });
                }
                let max_keys_param = req.query_param_lossy("max-keys");
                let max_keys: u32 = parse_max_keys(max_keys_param.as_deref())?;
                let requested_max_keys = max_keys_param
                    .as_deref()
                    .map(|value| parse_requested_max_keys(Some(value)))
                    .transpose()?;
                let requester = self.requester_from_auth(auth, req);

                let result = self.coordinator.list_objects_v2(
                    &crate::coordinator::ListObjectsV2Request {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        prefix: prefix.as_deref(),
                        delimiter: delimiter.as_deref(),
                        continuation_token: marker.as_deref(),
                        max_keys,
                        requested_max_keys,
                    },
                )?;
                Ok(S3Response::list_objects_v1(
                    bucket.as_str(),
                    self.coordinator.region(),
                    prefix.as_deref(),
                    delimiter.as_deref(),
                    marker.as_deref(),
                    encoding_type.as_deref(),
                    max_keys,
                    &result,
                ))
            }
            S3Operation::ListObjectsV2 { bucket } => {
                let prefix = req.query_param_lossy("prefix");
                let delimiter = req.query_param_lossy("delimiter").filter(|d| !d.is_empty());
                let encoding_type = req.query_param_lossy("encoding-type");
                let fetch_owner = req
                    .query_param_lossy("fetch-owner")
                    .is_some_and(|v| v == "true" || v == "1" || v == "True");
                let allow_unordered = req.query_param_lossy("allow-unordered");
                if allow_unordered.is_some() && delimiter.is_some() {
                    return Err(ServerError::InvalidArgument {
                        reason: "allow-unordered is not supported with delimiter".to_string(),
                    });
                }

                let continuation_token_raw = req.query_param_lossy("continuation-token");
                // AWS rejects empty continuation-token with InvalidArgument
                if let Some(ref ct) = continuation_token_raw {
                    if ct.is_empty() {
                        return Err(ServerError::InvalidArgument {
                            reason: "The continuation token provided is incorrect".to_string(),
                        });
                    }
                }
                let start_after_raw = req.query_param_lossy("start-after");
                let continuation_token = continuation_token_raw
                    .as_deref()
                    .or(start_after_raw.as_deref())
                    .filter(|v| !v.is_empty());
                let max_keys_param = req.query_param_lossy("max-keys");
                let max_keys: u32 = parse_max_keys(max_keys_param.as_deref())?;
                let requested_max_keys = max_keys_param
                    .as_deref()
                    .map(|value| parse_requested_max_keys(Some(value)))
                    .transpose()?;
                let requester = self.requester_from_auth(auth, req);

                let result = self.coordinator.list_objects_v2(
                    &crate::coordinator::ListObjectsV2Request {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        prefix: prefix.as_deref(),
                        delimiter: delimiter.as_deref(),
                        continuation_token,
                        max_keys,
                        requested_max_keys,
                    },
                )?;
                Ok(S3Response::list_objects_v2(
                    bucket.as_str(),
                    self.coordinator.region(),
                    prefix.as_deref(),
                    delimiter.as_deref(),
                    encoding_type.as_deref(),
                    continuation_token_raw.as_deref(),
                    start_after_raw.as_deref(),
                    fetch_owner,
                    max_keys,
                    &result,
                ))
            }
            S3Operation::PutObject { bucket, key } => {
                reject_directory_bucket_only_object_features(req)?;
                if let Some(copy_source) = req.header("x-amz-copy-source") {
                    // CopyObject path
                    let (src_bucket, src_key, src_version_id) =
                        parse_copy_source_header(copy_source)?;
                    let requester = self.requester_from_auth(auth, req);
                    let source_sse_customer = parse_sse_customer_copy_source_request(req)?;
                    let dst_sse_customer = parse_sse_customer_request(req)?;
                    let destination_managed_encryption =
                        parse_managed_encryption_request(req, dst_sse_customer.is_some())?;
                    let object_lock = parse_object_lock_headers(req)?;
                    let acl = parse_put_object_write_acl(req)?;
                    let src_cond = copy_source_condition_from_headers(req);
                    let dst_cond = write_condition_from_headers(req)?;
                    let request_headers: Vec<(&str, &str)> = req.header_iter().collect();
                    validate_write_request_header_section_size(&request_headers)?;
                    let website_redirect_location = req
                        .header(WEBSITE_REDIRECT_LOCATION_HEADER_NAME)
                        .map(parse_website_redirect_location)
                        .transpose()?;
                    // Parse metadata and checksum algorithm at the HTTP boundary
                    // so the coordinator never sees raw headers.
                    let replace_metadata;
                    let replace_system_metadata;
                    let replace_checksum_algo;
                    let directive = match req.header("x-amz-metadata-directive") {
                        Some(d) if d.eq_ignore_ascii_case("REPLACE") => {
                            let (blob, system_metadata) =
                                parse_request_metadata_without_checksum_headers(
                                    request_headers.iter().copied(),
                                )?;
                            replace_metadata = blob;
                            replace_system_metadata = system_metadata;

                            // Parse checksum algorithm if present.
                            replace_checksum_algo = match req.header("x-amz-checksum-algorithm") {
                                None => None,
                                Some(v) => Some(parse_checksum_algorithm_header_value(v)?),
                            };

                            MetadataDirective::Replace {
                                metadata: &replace_metadata,
                                system_metadata: &replace_system_metadata,
                                checksum_algorithm: replace_checksum_algo,
                            }
                        }
                        Some(d) if d.eq_ignore_ascii_case("COPY") => {
                            MetadataDirective::CopyExplicit
                        }
                        Some(other) => {
                            return Err(ServerError::InvalidArgument {
                                reason: format!("invalid x-amz-metadata-directive value: {other}"),
                            });
                        }
                        None => MetadataDirective::Copy,
                    };
                    // Parse inline tags before writing so invalid tags don't leave orphan objects
                    let replace_tags_xml = if req
                        .header("x-amz-tagging-directive")
                        .is_some_and(|d| d.eq_ignore_ascii_case("REPLACE"))
                    {
                        if let Some(tagging_header) = req.header("x-amz-tagging") {
                            let tags = xml::parse_url_encoded_tags(tagging_header)?;
                            if tags.is_empty() {
                                None
                            } else {
                                Some(xml::get_tagging_xml(&tags))
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    let tagging = if req
                        .header("x-amz-tagging-directive")
                        .is_some_and(|d| d.eq_ignore_ascii_case("REPLACE"))
                    {
                        TaggingDirective::Replace(replace_tags_xml.as_deref())
                    } else {
                        TaggingDirective::Copy
                    };
                    let policy_context = put_object_policy_context_from_request(
                        req,
                        replace_tags_xml.as_deref(),
                        Some(copy_source),
                        directive.policy_condition_value(),
                        acl.policy_condition_value(),
                        destination_managed_encryption,
                    );
                    let result = self.coordinator.copy_object(&CopyObjectRequest {
                        source: CopySource::new(
                            src_bucket,
                            src_key,
                            src_version_id,
                            &src_cond,
                            expected_source_bucket_owner(req),
                        ),
                        destination: object_request(
                            &bucket,
                            &key,
                            requester,
                            expected_bucket_owner,
                        )?,
                        dst_condition: &dst_cond,
                        directive,
                        website_redirect_location,
                        tagging,
                        acl,
                        policy_context,
                        source_sse_customer: source_sse_customer.as_ref(),
                        destination_encryption:
                            crate::coordinator::WriteEncryptionRequest::from_request_parts(
                                dst_sse_customer.as_ref(),
                                destination_managed_encryption,
                            )?,
                        object_lock,
                    })?;
                    Ok(S3Response::copy_object(&result))
                } else {
                    // Normal PutObject — use streaming upload path directly.
                    let object_lock = parse_object_lock_headers(req)?;
                    let checksum_requirement = if object_lock.retention.is_some() {
                        RequestChecksumRequirement::PutObjectWithObjectLock
                    } else {
                        RequestChecksumRequirement::Optional
                    };
                    require_request_checksum(req, checksum_requirement)?;
                    let sse_customer = parse_sse_customer_request(req)?;
                    let sse_s3 =
                        parse_managed_encryption_request(req, sse_customer.is_some())?.is_some();
                    let inline_tags_xml = if let Some(tagging_header) = req.header("x-amz-tagging")
                    {
                        let tags = xml::parse_url_encoded_tags(tagging_header)?;
                        if tags.is_empty() {
                            None
                        } else {
                            Some(xml::get_tagging_xml(&tags))
                        }
                    } else {
                        None
                    };
                    let request_headers: Vec<(&str, &str)> = req.header_iter().collect();
                    validate_write_request_header_section_size(&request_headers)?;
                    let (metadata_blob, system_metadata) =
                        parse_put_object_request_metadata(request_headers.iter().copied())?;
                    let cond = write_condition_from_headers(req)?;
                    let requester = self.requester_from_auth(auth, req);
                    let acl = parse_put_object_write_acl(req)?;
                    let policy_context = put_object_policy_context_from_request(
                        req,
                        inline_tags_xml.as_deref(),
                        None,
                        None,
                        acl.policy_condition_value(),
                        sse_s3.then_some(ManagedEncryptionAlgorithm::Aes256),
                    );
                    let result =
                        self.coordinator
                            .put_object(&crate::coordinator::PutObjectRequest {
                                object: object_request(
                                    &bucket,
                                    &key,
                                    requester,
                                    expected_bucket_owner,
                                )?,
                                data: &req.body,
                                metadata: &metadata_blob,
                                system_metadata: &system_metadata,
                                tags: inline_tags_xml.as_deref(),
                                cond: &cond,
                                acl,
                                policy_context,
                                object_lock,
                                encryption:
                                    crate::coordinator::WriteEncryptionRequest::from_request_parts(
                                        sse_customer.as_ref(),
                                        sse_s3
                                            .then_some(storage::ManagedEncryptionAlgorithm::Aes256),
                                    )?,
                            })?;
                    let mut resp = S3Response::put_object(&result);
                    apply_sse_customer_write_response_headers(&mut resp, sse_customer.as_ref());
                    for (_, header) in checksum_headers() {
                        if let Some(value) = req.header(header) {
                            resp.headers.push((header.to_string(), value.to_string()));
                        }
                    }
                    Ok(resp)
                }
            }
            S3Operation::GetObject { bucket, key } => {
                reject_managed_encryption_read_headers(
                    req,
                    ManagedEncryptionReadHeaderContext::StandardObjectRead,
                )?;
                reject_anonymous_response_overrides(req, auth)?;
                let sse_customer = parse_sse_customer_request(req)?;
                let cond = read_condition_from_headers(req);
                let vid = parse_version_id(req)?;
                let requester = self.requester_from_auth(auth, req);
                #[cfg(feature = "deep-tracing")]
                let trace = current_trace_context();
                if let Some(pn_str) = req.query_param_lossy("partNumber") {
                    if req.header("range").is_some() {
                        return Err(ServerError::InvalidRequest {
                            reason:
                                "Cannot specify both Range header and partNumber query parameter"
                                    .to_string(),
                        });
                    }
                    let part_number = request::parse_part_number(pn_str.as_ref())?;
                    #[cfg(feature = "deep-tracing")]
                    let _ = observability::event_in_context(
                        &trace,
                        TRACE_TARGET,
                        "get_object_part_request",
                        Some(format_args!(
                            "bucket={:?} key={:?} version_id={:?} part_number={}",
                            bucket, key, vid, part_number
                        )),
                    );
                    let result = self.coordinator.get_object_part(
                        &crate::coordinator::GetObjectPartRequest {
                            object: object_version_request(
                                &bucket,
                                &key,
                                vid,
                                requester,
                                expected_bucket_owner,
                            )?,
                            part_number,
                            cond: &cond,
                            sse_customer: sse_customer.as_ref(),
                        },
                    )?;
                    let mut resp = S3Response::get_object_part(result);
                    apply_response_overrides(&mut resp, req);
                    Ok(resp)
                } else if let Some(range_header) = req.header("range") {
                    match crate::range::ByteRange::parse(range_header) {
                        Ok(byte_range) => {
                            #[cfg(feature = "deep-tracing")]
                            let _ = observability::event_in_context(
                                &trace,
                                TRACE_TARGET,
                                "get_object_range_request",
                                Some(format_args!(
                                    "bucket={:?} key={:?} version_id={:?} raw_range={:?} parsed_range={}",
                                    bucket, key, vid, range_header, byte_range
                                )),
                            );
                            let result = self.coordinator.get_object_range(
                                &crate::coordinator::GetObjectRangeRequest {
                                    object: object_version_request(
                                        &bucket,
                                        &key,
                                        vid,
                                        requester,
                                        expected_bucket_owner,
                                    )?,
                                    range: byte_range,
                                    cond: &cond,
                                    sse_customer: sse_customer.as_ref(),
                                },
                            )?;
                            Ok(S3Response::get_object_range(result))
                        }
                        Err(_) => {
                            #[cfg(feature = "deep-tracing")]
                            let _ = observability::event_in_context(
                                &trace,
                                TRACE_TARGET,
                                "get_object_range_ignored",
                                Some(format_args!(
                                    "bucket={:?} key={:?} version_id={:?} raw_range={:?} reason=invalid_header",
                                    bucket,
                                    key,
                                    vid,
                                    range_header
                                )),
                            );
                            let result = self.coordinator.get_object(
                                &crate::coordinator::GetObjectRequest {
                                    object: object_version_request(
                                        &bucket,
                                        &key,
                                        vid,
                                        requester,
                                        expected_bucket_owner,
                                    )?,
                                    cond: &cond,
                                    sse_customer: sse_customer.as_ref(),
                                },
                            )?;
                            let checksum_mode = req.header("x-amz-checksum-mode");
                            let mut resp = S3Response::get_object(result, checksum_mode);
                            apply_response_overrides(&mut resp, req);
                            Ok(resp)
                        }
                    }
                } else {
                    let result =
                        self.coordinator
                            .get_object(&crate::coordinator::GetObjectRequest {
                                object: object_version_request(
                                    &bucket,
                                    &key,
                                    vid,
                                    requester,
                                    expected_bucket_owner,
                                )?,
                                cond: &cond,
                                sse_customer: sse_customer.as_ref(),
                            })?;
                    let checksum_mode = req.header("x-amz-checksum-mode");
                    let mut resp = S3Response::get_object(result, checksum_mode);
                    apply_response_overrides(&mut resp, req);
                    Ok(resp)
                }
            }
            S3Operation::DeleteObject { bucket, key } => {
                let cond = delete_condition_from_headers(req)?;
                let vid = parse_version_id(req)?;
                let bypass_governance = parse_bypass_governance_retention(req);
                if vid.is_some() && !cond.is_empty() {
                    return Err(ServerError::NotImplemented {
                        feature:
                            "A header you provided implies functionality that is not implemented"
                                .to_string(),
                    });
                }
                let requester = self.requester_from_auth(auth, req);
                let result =
                    self.coordinator
                        .delete_object(&crate::coordinator::DeleteObjectRequest {
                            object: object_version_request(
                                &bucket,
                                &key,
                                vid,
                                requester,
                                expected_bucket_owner,
                            )?,
                            bypass_governance,
                            cond: &cond,
                        })?;
                Ok(S3Response::delete_object(&result))
            }
            S3Operation::HeadObject { bucket, key } => {
                reject_managed_encryption_read_headers(
                    req,
                    ManagedEncryptionReadHeaderContext::StandardObjectRead,
                )?;
                let sse_customer = parse_sse_customer_request(req)?;
                let cond = read_condition_from_headers(req);
                let vid = parse_version_id(req)?;
                let requester = self.requester_from_auth(auth, req);
                #[cfg(feature = "deep-tracing")]
                let trace = current_trace_context();
                if let Some(pn_str) = req.query_param_lossy("partNumber") {
                    let part_number = request::parse_part_number(pn_str.as_ref())?;
                    #[cfg(feature = "deep-tracing")]
                    let _ = observability::event_in_context(
                        &trace,
                        TRACE_TARGET,
                        "head_object_part_request",
                        Some(format_args!(
                            "bucket={:?} key={:?} version_id={:?} part_number={}",
                            bucket, key, vid, part_number
                        )),
                    );
                    let result = self.coordinator.head_object_part(
                        &crate::coordinator::GetObjectPartRequest {
                            object: object_version_request(
                                &bucket,
                                &key,
                                vid,
                                requester,
                                expected_bucket_owner,
                            )?,
                            part_number,
                            cond: &cond,
                            sse_customer: sse_customer.as_ref(),
                        },
                    )?;
                    Ok(S3Response::head_object_part(&result))
                } else {
                    let result =
                        self.coordinator
                            .head_object(&crate::coordinator::GetObjectRequest {
                                object: object_version_request(
                                    &bucket,
                                    &key,
                                    vid,
                                    requester,
                                    expected_bucket_owner,
                                )?,
                                cond: &cond,
                                sse_customer: sse_customer.as_ref(),
                            })?;
                    let checksum_mode = req.header("x-amz-checksum-mode");
                    Ok(S3Response::head_object(&result, checksum_mode))
                }
            }
            S3Operation::GetObjectAttributes { bucket, key } => {
                reject_managed_encryption_read_headers(
                    req,
                    ManagedEncryptionReadHeaderContext::ObjectAttributes,
                )?;
                let sse_customer = parse_sse_customer_request(req)?;
                // Parse x-amz-object-attributes header (required, comma-separated).
                // The AWS Rust SDK may send one header per list element; accept both
                // repeated headers and comma-delimited header values.
                let attr_values: Vec<&str> = req
                    .headers
                    .get_all("x-amz-object-attributes")
                    .iter()
                    .map(|value| {
                        std::str::from_utf8(value.as_bytes())
                            .expect("S3Request stores only validated UTF-8")
                    })
                    .collect();
                if attr_values.is_empty() {
                    return Err(ServerError::InvalidArgument {
                        reason: "missing required header: x-amz-object-attributes".to_string(),
                    });
                }
                let requested: Vec<&str> = attr_values
                    .into_iter()
                    .flat_map(|value| value.split(','))
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .collect();
                if requested.is_empty() {
                    return Err(ServerError::InvalidArgument {
                        reason: "missing required header: x-amz-object-attributes".to_string(),
                    });
                }
                for &attr in &requested {
                    if !xml::is_valid_object_attribute(attr) {
                        return Err(ServerError::InvalidArgument {
                            reason: format!("invalid object attribute: {attr}"),
                        });
                    }
                }
                let want_parts = requested.contains(&"ObjectParts");
                let max_parts = parse_u32_or_default(
                    req.header("x-amz-max-parts"),
                    1000,
                    "invalid x-amz-max-parts",
                )?;
                let part_number_marker = parse_optional_u32(
                    req.header("x-amz-part-number-marker"),
                    "x-amz-part-number-marker must be an integer",
                )?;

                let cond = read_condition_from_headers(req);
                let vid = parse_version_id(req)?;
                let requester_ctx = self.requester_from_auth(auth, req);
                let result = self.coordinator.get_object_attributes(
                    &crate::coordinator::GetObjectAttributesRequest {
                        object: object_version_request(
                            &bucket,
                            &key,
                            vid,
                            requester_ctx,
                            expected_bucket_owner,
                        )?,
                        cond: &cond,
                        want_parts,
                        part_number_marker,
                        max_parts,
                        sse_customer: sse_customer.as_ref(),
                    },
                )?;
                let checksum_entries = result.system_metadata.checksum_header_pairs();
                // Extract checksum algorithm from metadata for per-part checksum XML elements.
                let obj_checksum_algo = result.system_metadata.checksum_algorithm();
                let body_xml = xml::get_object_attributes_xml(
                    &requested,
                    &result.etag,
                    result.size,
                    &checksum_entries,
                    result.object_parts.as_ref(),
                    obj_checksum_algo,
                );
                Ok(S3Response::get_object_attributes(
                    &body_xml,
                    result.last_modified,
                    result.version_id,
                ))
            }
            S3Operation::DeleteObjects { bucket } => {
                require_request_checksum(
                    req,
                    RequestChecksumRequirement::ContentMd5OrChecksumHeader,
                )?;
                let bypass_governance = parse_bypass_governance_retention(req);
                let (xml_entries, quiet) = xml::parse_delete_objects_xml(&req.body)?;
                let requester = self.requester_from_auth(auth, req);
                let mut entries: Vec<crate::coordinator::DeleteEntry> = Vec::new();
                let mut validation_errors: Vec<crate::coordinator::DeleteError> = Vec::new();
                for e in &xml_entries {
                    let version_id = match e.version_id.as_deref() {
                        Some(raw_version_id) => match parse_version_id_str(raw_version_id) {
                            Ok(version_id) => Some(version_id),
                            Err(ServerError::InvalidVersionId { .. }) => {
                                validation_errors.push(crate::coordinator::DeleteError {
                                    key: e.key.to_string(),
                                    version_id: Some(
                                        crate::coordinator::DeleteErrorVersionId::Raw(
                                            raw_version_id.to_string(),
                                        ),
                                    ),
                                    code: "NoSuchVersion".to_string(),
                                    message: "The specified version does not exist.".to_string(),
                                });
                                continue;
                            }
                            Err(error) => return Err(error),
                        },
                        None => None,
                    };
                    let has_unsupported_form_fields =
                        e.last_modified_time.is_some() || e.size.is_some();
                    let has_unsupported_versioned_etag = version_id.is_some() && e.etag.is_some();
                    if has_unsupported_form_fields || has_unsupported_versioned_etag {
                        validation_errors.push(crate::coordinator::DeleteError {
                            key: e.key.to_string(),
                            version_id: version_id.map(Into::into),
                            code: "NotImplemented".to_string(),
                            message:
                                "A form field you provided implies functionality that is not implemented"
                                    .to_string(),
                        });
                        continue;
                    }

                    let cond = match e.etag.as_deref() {
                        Some(etag) => {
                            crate::conditional::DeleteCondition::from_delete_objects_etag(etag)
                        }
                        None => crate::conditional::DeleteCondition::None,
                    };
                    entries.push(crate::coordinator::DeleteEntry {
                        key: e.key.clone(),
                        version_id,
                        cond,
                    });
                }
                let mut result = if entries.is_empty() {
                    crate::coordinator::DeleteObjectsResult {
                        deleted: Vec::new(),
                        errors: Vec::new(),
                    }
                } else {
                    self.coordinator
                        .delete_objects(&crate::coordinator::DeleteObjectsRequest {
                            bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                            entries: &entries,
                            bypass_governance,
                        })?
                };
                result.errors.extend(validation_errors);
                Ok(S3Response::delete_objects(&result, quiet))
            }
            S3Operation::PutBucketVersioning { bucket } => {
                validate_request_checksum_headers(req, true, false, true)?;
                let versioning_state = xml::parse_versioning_config_xml(&req.body)?;
                let requester = self.requester_from_auth(auth, req);
                self.coordinator.put_bucket_versioning(
                    &crate::coordinator::PutBucketVersioningRequest {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        state: versioning_state,
                    },
                )?;
                Ok(S3Response::put_bucket_versioning())
            }
            S3Operation::GetBucketVersioning { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                let state = self.coordinator.get_bucket_versioning(&bucket_request(
                    &bucket,
                    requester,
                    expected_bucket_owner,
                )?)?;
                Ok(S3Response::get_bucket_versioning(state))
            }
            S3Operation::PutBucketObjectLockConfiguration { bucket } => {
                require_request_checksum(
                    req,
                    RequestChecksumRequirement::ContentMd5OrChecksumHeader,
                )?;
                let config = xml::parse_bucket_object_lock_configuration_xml(&req.body)?;
                let requester = self.requester_from_auth(auth, req);
                self.coordinator.put_bucket_object_lock_configuration(
                    &crate::coordinator::PutBucketObjectLockConfigurationRequest {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        config,
                    },
                )?;
                Ok(S3Response::put_bucket_object_lock_configuration())
            }
            S3Operation::GetBucketObjectLockConfiguration { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                let config =
                    self.coordinator
                        .get_bucket_object_lock_configuration(&bucket_request(
                            &bucket,
                            requester,
                            expected_bucket_owner,
                        )?)?;
                Ok(S3Response::get_bucket_object_lock_configuration(config))
            }
            S3Operation::PutBucketEncryption { bucket } => {
                validate_request_checksum_headers(req, true, false, true)?;
                let config = xml::parse_bucket_encryption_xml(&req.body)?;
                let requester = self.requester_from_auth(auth, req);
                self.coordinator.put_bucket_encryption(
                    &crate::coordinator::PutBucketEncryptionRequest {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        config,
                    },
                )?;
                Ok(S3Response::put_bucket_encryption())
            }
            S3Operation::GetBucketEncryption { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                let config = self.coordinator.get_bucket_encryption(&bucket_request(
                    &bucket,
                    requester,
                    expected_bucket_owner,
                )?)?;
                Ok(S3Response::get_bucket_encryption(config))
            }
            S3Operation::DeleteBucketEncryption { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                self.coordinator.delete_bucket_encryption(&bucket_request(
                    &bucket,
                    requester,
                    expected_bucket_owner,
                )?)?;
                Ok(S3Response::delete_bucket_encryption())
            }
            S3Operation::PostObject { .. } => {
                // POST Object is handled by the streaming path in serve.rs.
                // If it reaches dispatch_routed, something is wrong.
                Err(ServerError::InvalidRequest {
                    reason: "POST Object must use the streaming path".to_string(),
                })
            }
            S3Operation::PutBucketCors { bucket } => {
                require_request_checksum(
                    req,
                    RequestChecksumRequirement::ContentMd5OrChecksumHeader,
                )?;
                let config = xml::parse_cors_config_xml(&req.body)?;
                let config_xml = xml::get_cors_config_xml(&config);
                let requester = self.requester_from_auth(auth, req);
                self.coordinator
                    .put_bucket_cors(&crate::coordinator::PutBucketConfigRequest {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        config: &config_xml,
                    })?;
                Ok(S3Response::put_bucket_cors())
            }
            S3Operation::GetBucketCors { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                match self.coordinator.get_bucket_cors(&bucket_request(
                    &bucket,
                    requester,
                    expected_bucket_owner,
                )?)? {
                    Some(config_xml) => Ok(S3Response::get_bucket_cors(&config_xml)),
                    None => Err(ServerError::NoSuchCorsConfiguration {
                        bucket: bucket.to_string(),
                    }),
                }
            }
            S3Operation::DeleteBucketCors { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                self.coordinator.delete_bucket_cors(&bucket_request(
                    &bucket,
                    requester,
                    expected_bucket_owner,
                )?)?;
                Ok(S3Response::delete_bucket_cors())
            }
            S3Operation::PutBucketTagging { bucket } => {
                require_request_checksum(
                    req,
                    RequestChecksumRequirement::ContentMd5OrChecksumHeader,
                )?;
                let tags = xml::TagSet::parse_tagging_xml(&req.body, 50)?;
                let tags_xml = tags.to_xml();
                let requester = self.requester_from_auth(auth, req);
                self.coordinator
                    .put_bucket_tags(&crate::coordinator::PutBucketConfigRequest {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        config: &tags_xml,
                    })?;
                Ok(S3Response::put_bucket_tagging())
            }
            S3Operation::GetBucketTagging { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                match self.coordinator.get_bucket_tags(&bucket_request(
                    &bucket,
                    requester,
                    expected_bucket_owner,
                )?)? {
                    Some(tags_xml) => Ok(S3Response::get_bucket_tagging(&tags_xml)),
                    None => Err(ServerError::NoSuchTagSet {
                        resource: bucket.to_string(),
                    }),
                }
            }
            S3Operation::DeleteBucketTagging { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                self.coordinator.delete_bucket_tags(&bucket_request(
                    &bucket,
                    requester,
                    expected_bucket_owner,
                )?)?;
                Ok(S3Response::delete_bucket_tagging())
            }
            S3Operation::PutBucketAbac { bucket } => {
                require_request_checksum(
                    req,
                    RequestChecksumRequirement::ContentMd5OrChecksumHeader,
                )?;
                let enabled = xml::parse_bucket_abac_xml(&req.body)?;
                let requester = self.requester_from_auth(auth, req);
                self.coordinator
                    .put_bucket_abac(&crate::coordinator::PutBucketAbacRequest {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        enabled,
                    })?;
                Ok(S3Response::put_bucket_abac())
            }
            S3Operation::GetBucketAbac { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                let enabled = self.coordinator.get_bucket_abac(&bucket_request(
                    &bucket,
                    requester,
                    expected_bucket_owner,
                )?)?;
                Ok(S3Response::get_bucket_abac(&xml::get_bucket_abac_xml(
                    enabled,
                )))
            }
            S3Operation::PutBucketLifecycle { bucket } => {
                require_request_checksum(req, RequestChecksumRequirement::PutBucketLifecycle)?;
                let config = ensure_lifecycle_rule_ids(
                    xml::parse_bucket_lifecycle_configuration_xml(&req.body)?,
                )?;
                let config_xml = xml::get_bucket_lifecycle_configuration_xml(&config);
                let requester = self.requester_from_auth(auth, req);
                self.coordinator.put_bucket_lifecycle(
                    &crate::coordinator::PutBucketConfigRequest {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        config: &config_xml,
                    },
                )?;
                Ok(S3Response::put_bucket_lifecycle())
            }
            S3Operation::GetBucketLifecycle { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                match self.coordinator.get_bucket_lifecycle(&bucket_request(
                    &bucket,
                    requester,
                    expected_bucket_owner,
                )?)? {
                    Some(config_xml) => Ok(S3Response::get_bucket_lifecycle(&config_xml)),
                    None => Err(ServerError::NoSuchLifecycleConfiguration {
                        bucket: bucket.to_string(),
                    }),
                }
            }
            S3Operation::DeleteBucketLifecycle { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                self.coordinator.delete_bucket_lifecycle(&bucket_request(
                    &bucket,
                    requester,
                    expected_bucket_owner,
                )?)?;
                Ok(S3Response::delete_bucket_lifecycle())
            }
            S3Operation::PutObjectRetention { bucket, key } => {
                require_request_checksum(
                    req,
                    RequestChecksumRequirement::ContentMd5OrChecksumHeader,
                )?;
                let vid = parse_version_id(req)?;
                let retention = xml::parse_object_retention_xml(&req.body)?;
                let bypass_governance = req
                    .header("x-amz-bypass-governance-retention")
                    .is_some_and(|value| value.eq_ignore_ascii_case("true"));
                let requester = self.requester_from_auth(auth, req);
                let version_id = self.coordinator.put_object_retention(
                    &crate::coordinator::PutObjectRetentionRequest {
                        object: object_version_request(
                            &bucket,
                            &key,
                            vid,
                            requester,
                            expected_bucket_owner,
                        )?,
                        retention,
                        bypass_governance,
                    },
                )?;
                Ok(S3Response::put_object_retention(version_id))
            }
            S3Operation::GetObjectRetention { bucket, key } => {
                let vid = parse_version_id(req)?;
                let requester = self.requester_from_auth(auth, req);
                let retention = self
                    .coordinator
                    .get_object_retention(&object_version_request(
                        &bucket,
                        &key,
                        vid,
                        requester,
                        expected_bucket_owner,
                    )?)?;
                Ok(S3Response::get_object_retention(retention))
            }
            S3Operation::PutObjectLegalHold { bucket, key } => {
                require_request_checksum(
                    req,
                    RequestChecksumRequirement::ContentMd5OrChecksumHeader,
                )?;
                let vid = parse_version_id(req)?;
                let legal_hold = xml::parse_object_legal_hold_xml(&req.body)?;
                let requester = self.requester_from_auth(auth, req);
                let version_id = self.coordinator.put_object_legal_hold(
                    &crate::coordinator::PutObjectLegalHoldRequest {
                        object: object_version_request(
                            &bucket,
                            &key,
                            vid,
                            requester,
                            expected_bucket_owner,
                        )?,
                        legal_hold,
                    },
                )?;
                Ok(S3Response::put_object_legal_hold(version_id))
            }
            S3Operation::GetObjectLegalHold { bucket, key } => {
                let vid = parse_version_id(req)?;
                let requester = self.requester_from_auth(auth, req);
                let legal_hold =
                    self.coordinator
                        .get_object_legal_hold(&object_version_request(
                            &bucket,
                            &key,
                            vid,
                            requester,
                            expected_bucket_owner,
                        )?)?;
                Ok(S3Response::get_object_legal_hold(legal_hold))
            }
            S3Operation::PutObjectTagging { bucket, key } => {
                validate_request_checksum_headers(req, true, false, true)?;
                let vid = parse_version_id(req)?;
                let tags = xml::TagSet::parse_tagging_xml(&req.body, 10)?;
                let tags_xml = tags.to_xml();
                let requester = self.requester_from_auth(auth, req);
                self.coordinator
                    .put_object_tags(&crate::coordinator::PutObjectTagsRequest {
                        object: object_version_request(
                            &bucket,
                            &key,
                            vid,
                            requester,
                            expected_bucket_owner,
                        )?,
                        tags: &tags_xml,
                    })?;
                Ok(S3Response::put_object_tagging())
            }
            S3Operation::GetObjectTagging { bucket, key } => {
                let vid = parse_version_id(req)?;
                let requester = self.requester_from_auth(auth, req);
                if let Some(tags_xml) = self.coordinator.get_object_tags(
                    &object_version_request(&bucket, &key, vid, requester, expected_bucket_owner)?,
                )? {
                    let mut tags = xml::TagSet::parse_tagging_xml(tags_xml.as_bytes(), 10)?;
                    tags.reverse();
                    Ok(S3Response::get_object_tagging(&tags.to_xml()))
                } else {
                    // S3 returns empty TagSet (not 404) for objects with no tags
                    let empty = xml::TagSet::empty(10).to_xml();
                    Ok(S3Response::get_object_tagging(&empty))
                }
            }
            S3Operation::DeleteObjectTagging { bucket, key } => {
                let vid = parse_version_id(req)?;
                let requester = self.requester_from_auth(auth, req);
                self.coordinator
                    .delete_object_tags(&object_version_request(
                        &bucket,
                        &key,
                        vid,
                        requester,
                        expected_bucket_owner,
                    )?)?;
                Ok(S3Response::delete_object_tagging())
            }
            S3Operation::GetObjectAcl { bucket, key } => {
                let version_id = parse_version_id(req)?;
                let requester = self.requester_from_auth(auth, req);
                let result = self.coordinator.get_object_acl(&object_version_request(
                    &bucket,
                    &key,
                    version_id,
                    requester,
                    expected_bucket_owner,
                )?)?;
                let (owner_display_name, grants) = self.render_acl_grants(
                    &result.owner_principal,
                    &result.owner_canonical_id,
                    &result.acl_grants,
                );
                Ok(S3Response::get_object_acl(
                    &result,
                    &owner_display_name,
                    &grants,
                ))
            }
            S3Operation::PutObjectAcl { bucket, key } => {
                validate_request_checksum_headers(req, true, false, true)?;
                let version_id = parse_version_id(req)?;
                let requester = self.requester_from_auth(auth, req);
                let result_version_id = if req.header("x-amz-acl").is_some() {
                    if !req.body.is_empty() {
                        return Err(ServerError::InvalidArgument {
                            reason: "x-amz-acl cannot be combined with ACL XML body".to_string(),
                        });
                    }
                    if has_acl_grant_headers(req) {
                        return Err(canned_acl_and_header_grants_conflict());
                    }
                    let acl = parse_put_object_acl(req.header("x-amz-acl"));
                    self.coordinator
                        .put_object_acl(&crate::coordinator::PutObjectAclRequest {
                            object: object_version_request(
                                &bucket,
                                &key,
                                version_id,
                                requester,
                                expected_bucket_owner,
                            )?,
                            acl: crate::coordinator::PutObjectAclInput::Canned(acl),
                            policy_context: crate::coordinator::PutObjectPolicyContext::default()
                                .with_default_canned_acl(acl.policy_condition_value()),
                        })?
                } else {
                    let acl_grants = parse_acl_grants(req)?;
                    self.coordinator
                        .put_object_acl(&crate::coordinator::PutObjectAclRequest {
                            object: object_version_request(
                                &bucket,
                                &key,
                                version_id,
                                requester,
                                expected_bucket_owner,
                            )?,
                            acl: crate::coordinator::PutObjectAclInput::Grants(acl_grants),
                            policy_context: crate::coordinator::PutObjectPolicyContext::default()
                                .with_acl_grant_headers(
                                    req.header("x-amz-grant-read"),
                                    req.header("x-amz-grant-write"),
                                    req.header("x-amz-grant-read-acp"),
                                    req.header("x-amz-grant-write-acp"),
                                    req.header("x-amz-grant-full-control"),
                                ),
                        })?
                };
                Ok(S3Response::put_object_acl(result_version_id))
            }
            S3Operation::PutBucketPublicAccessBlock { bucket } => {
                validate_request_checksum_headers(req, true, false, true)?;
                let config = xml::parse_public_access_block_xml(&req.body)?;
                let requester = self.requester_from_auth(auth, req);
                self.coordinator.put_bucket_public_access_block(
                    &crate::coordinator::PutBucketPublicAccessBlockRequest {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        config,
                    },
                )?;
                Ok(S3Response::put_bucket_public_access_block())
            }
            S3Operation::GetBucketPublicAccessBlock { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                match self
                    .coordinator
                    .get_bucket_public_access_block(&bucket_request(
                        &bucket,
                        requester,
                        expected_bucket_owner,
                    )?)? {
                    Some(config) => Ok(S3Response::get_bucket_public_access_block(
                        &xml::get_public_access_block_xml(&config),
                    )),
                    None => Err(ServerError::NoSuchPublicAccessBlockConfiguration {
                        bucket: bucket.to_string(),
                    }),
                }
            }
            S3Operation::DeleteBucketPublicAccessBlock { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                self.coordinator
                    .delete_bucket_public_access_block(&bucket_request(
                        &bucket,
                        requester,
                        expected_bucket_owner,
                    )?)?;
                Ok(S3Response::delete_bucket_public_access_block())
            }
            S3Operation::PutBucketOwnershipControls { bucket } => {
                validate_request_checksum_headers(req, true, false, true)?;
                let value = xml::parse_ownership_controls_xml(&req.body)?;
                let requester = self.requester_from_auth(auth, req);
                self.coordinator.put_bucket_ownership_controls(
                    &crate::coordinator::PutBucketOwnershipControlsRequest {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        config: value,
                    },
                )?;
                Ok(S3Response::put_bucket_ownership_controls())
            }
            S3Operation::GetBucketOwnershipControls { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                match self
                    .coordinator
                    .get_bucket_ownership_controls(&bucket_request(
                        &bucket,
                        requester,
                        expected_bucket_owner,
                    )?)? {
                    Some(config) => Ok(S3Response::get_bucket_ownership_controls(
                        &xml::get_ownership_controls_xml(&config),
                    )),
                    None => Err(ServerError::OwnershipControlsNotFound {
                        bucket: bucket.to_string(),
                    }),
                }
            }
            S3Operation::DeleteBucketOwnershipControls { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                self.coordinator
                    .delete_bucket_ownership_controls(&bucket_request(
                        &bucket,
                        requester,
                        expected_bucket_owner,
                    )?)?;
                Ok(S3Response::delete_bucket_ownership_controls())
            }
            S3Operation::PutBucketPolicy { bucket } => {
                validate_request_checksum_headers(req, true, false, true)?;
                let policy =
                    std::str::from_utf8(&req.body).map_err(|_| ServerError::InvalidArgument {
                        reason: "invalid UTF-8 in bucket policy JSON body".to_string(),
                    })?;
                let confirm_remove_self_bucket_access = parse_confirm_remove_self_bucket_access(
                    req.headers
                        .get("x-amz-confirm-remove-self-bucket-access")
                        .map(|value| {
                            std::str::from_utf8(value.as_bytes())
                                .expect("S3Request stores only validated UTF-8")
                        }),
                )?;
                let requester = self.requester_from_auth(auth, req);
                self.coordinator.put_bucket_policy(
                    &crate::coordinator::PutBucketPolicyRequest {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        config: policy,
                        confirm_remove_self_bucket_access,
                    },
                )?;
                Ok(S3Response::put_bucket_policy())
            }
            S3Operation::GetBucketPolicy { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                match self.coordinator.get_bucket_policy(&bucket_request(
                    &bucket,
                    requester,
                    expected_bucket_owner,
                )?)? {
                    Some(policy) => Ok(S3Response::get_bucket_policy(&policy)),
                    None => Err(ServerError::NoSuchBucketPolicy {
                        bucket: bucket.to_string(),
                    }),
                }
            }
            S3Operation::GetBucketPolicyStatus { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                let is_public = self.coordinator.get_bucket_policy_status(&bucket_request(
                    &bucket,
                    requester,
                    expected_bucket_owner,
                )?)?;
                Ok(S3Response::get_bucket_policy_status(is_public))
            }
            S3Operation::DeleteBucketPolicy { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                self.coordinator.delete_bucket_policy(&bucket_request(
                    &bucket,
                    requester,
                    expected_bucket_owner,
                )?)?;
                Ok(S3Response::delete_bucket_policy())
            }
            S3Operation::GetBucketAcl { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                let result = self.coordinator.get_bucket_acl(&bucket_request(
                    &bucket,
                    requester,
                    expected_bucket_owner,
                )?)?;
                let (owner_display_name, grants) = self.render_acl_grants(
                    &result.owner_principal,
                    &result.owner_canonical_id,
                    &result.acl_grants,
                );
                Ok(S3Response::get_bucket_acl(
                    &result,
                    &owner_display_name,
                    &grants,
                ))
            }
            S3Operation::PutBucketAcl { bucket } => {
                let requester = self.requester_from_auth(auth, req);
                let acl = if req.header("x-amz-acl").is_some() {
                    if !req.body.is_empty() {
                        return Err(ServerError::InvalidArgument {
                            reason: "x-amz-acl cannot be combined with ACL XML body".to_string(),
                        });
                    }
                    if has_acl_grant_headers(req) {
                        return Err(canned_acl_and_header_grants_conflict());
                    }
                    let acl = match parse_create_bucket_acl(req)? {
                        crate::coordinator::CreateBucketAcl::Canned(acl) => acl,
                        crate::coordinator::CreateBucketAcl::DefaultPrivate
                        | crate::coordinator::CreateBucketAcl::Grants(_) => {
                            unreachable!("x-amz-acl header must parse to a canned ACL")
                        }
                    };
                    crate::coordinator::PutBucketAclInput::Canned(acl)
                } else {
                    if !req.body.is_empty() && has_acl_grant_headers(req) {
                        return Err(acl_xml_and_header_grants_conflict());
                    }
                    crate::coordinator::PutBucketAclInput::Grants(parse_acl_grants(req)?)
                };
                let acl_req = crate::coordinator::PutBucketAclRequest {
                    bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                    acl,
                    policy_context: if let Some(canned_acl) = req.header("x-amz-acl") {
                        crate::coordinator::PutObjectPolicyContext::default()
                            .with_default_canned_acl(Some(canned_acl))
                    } else {
                        crate::coordinator::PutObjectPolicyContext::default()
                            .with_acl_grant_headers(
                                req.header("x-amz-grant-read"),
                                req.header("x-amz-grant-write"),
                                req.header("x-amz-grant-read-acp"),
                                req.header("x-amz-grant-write-acp"),
                                req.header("x-amz-grant-full-control"),
                            )
                    },
                };
                self.coordinator.validate_put_bucket_acl_request(&acl_req)?;
                validate_request_checksum_headers(req, true, false, true)?;
                self.coordinator.put_bucket_acl(&acl_req)?;
                Ok(S3Response::put_bucket_acl())
            }
            S3Operation::CreateMultipartUpload { bucket, key } => {
                let sse_customer = parse_sse_customer_request(req)?;
                let sse_s3 =
                    parse_managed_encryption_request(req, sse_customer.is_some())?.is_some();
                let sse_customer_headers = sse_customer
                    .as_ref()
                    .map(SseCustomerRequest::response_headers);
                let request_headers: Vec<(&str, &str)> = req.header_iter().collect();
                validate_write_request_header_section_size(&request_headers)?;
                let (metadata, system_metadata) = parse_request_metadata_without_checksum_headers(
                    request_headers.iter().copied(),
                )?;

                // Parse optional checksum algorithm/type headers.
                let checksum_algorithm = match req.header("x-amz-checksum-algorithm") {
                    None => None,
                    Some(v) => Some(parse_checksum_algorithm_header_value(v)?),
                };
                let checksum_type = match req.header("x-amz-checksum-type") {
                    None => None,
                    Some(v) => Some(ChecksumType::parse(v).ok_or_else(|| {
                        ServerError::InvalidRequestHostId {
                            reason: "Value for x-amz-checksum-type header is invalid.".to_string(),
                        }
                    })?),
                };

                // Validate: checksum-type without checksum-algorithm is invalid.
                if checksum_type.is_some() && checksum_algorithm.is_none() {
                    return Err(ServerError::InvalidRequestHostId {
                        reason: "The x-amz-checksum-type header can only be used with the x-amz-checksum-algorithm header.".to_string(),
                    });
                }

                // Build validated config (rejects invalid algo+type combinations).
                let checksum = match checksum_algorithm {
                    Some(algo) => Some(MultipartChecksumConfig::new(algo, checksum_type).map_err(
                        |_| {
                            let Some(checksum_type) = checksum_type else {
                                return ServerError::InvalidRequestHostId {
                                    reason: "Invalid checksum configuration".to_string(),
                                };
                            };
                            ServerError::InvalidRequestHostId {
                                reason: format!(
                                    "The {} checksum type cannot be used with the {} checksum algorithm.",
                                    checksum_type.as_str(),
                                    algo.as_str().to_ascii_lowercase()
                                ),
                            }
                        },
                    )?),
                    None => None,
                };
                let inline_tags_xml = if let Some(tagging_header) = req.header("x-amz-tagging") {
                    let tags = xml::parse_url_encoded_tags(tagging_header)?;
                    if tags.is_empty() {
                        None
                    } else {
                        Some(xml::get_tagging_xml(&tags))
                    }
                } else {
                    None
                };
                let requester = self.requester_from_auth(auth, req);
                let acl = parse_put_object_write_acl(req)?;
                let object_lock = parse_object_lock_headers(req)?;
                let policy_context = put_object_policy_context_from_request(
                    req,
                    inline_tags_xml.as_deref(),
                    None,
                    None,
                    acl.policy_condition_value(),
                    sse_s3.then_some(ManagedEncryptionAlgorithm::Aes256),
                )
                .with_object_creation_operation(false);

                let result = self.coordinator.create_multipart_upload(
                    &crate::coordinator::CreateMultipartUploadRequest {
                        object: object_request(&bucket, &key, requester, expected_bucket_owner)?,
                        metadata: &metadata,
                        system_metadata: &system_metadata,
                        tags: inline_tags_xml.as_deref(),
                        checksum,
                        acl,
                        policy_context,
                        object_lock,
                        encryption: crate::coordinator::WriteEncryptionRequest::from_request_parts(
                            sse_customer.as_ref(),
                            sse_s3.then_some(storage::ManagedEncryptionAlgorithm::Aes256),
                        )?,
                    },
                )?;
                Ok(S3Response::create_multipart_upload(
                    bucket.as_str(),
                    &key,
                    &result.upload_id,
                    crate::http::response::CreateMultipartUploadResponseContext {
                        managed_encryption: result.managed_encryption,
                        checksum_algorithm: checksum.map(MultipartChecksumConfig::algorithm),
                        checksum_type: checksum.map(MultipartChecksumConfig::checksum_type),
                        lifecycle_abort: result.lifecycle_abort.as_ref(),
                        sse_customer: sse_customer_headers.as_ref(),
                    },
                ))
            }
            S3Operation::UploadPart { bucket, key } => {
                let (upload_id_raw, part_number) =
                    request::parse_upload_part_query(req.query_string())?;
                let upload_id = parse_present_upload_id(upload_id_raw.as_str())?;
                // Normal UploadPart requests are intercepted in serve.rs and
                // streamed before they reach dispatch_routed(). Only copy-source
                // variants should remain on this buffered path.
                let Some(copy_source) = req.header("x-amz-copy-source") else {
                    return Err(ServerError::InternalError {
                        reason: "buffered dispatcher reached non-copy UploadPart".to_string(),
                    });
                };

                let (src_bucket, src_key, src_version_id) = parse_copy_source_header(copy_source)?;
                let requester = self.requester_from_auth(auth, req);
                let source_sse_customer = parse_sse_customer_copy_source_request(req)?;
                let sse_customer = parse_sse_customer_request(req)?;
                reject_managed_encryption_read_headers(
                    req,
                    ManagedEncryptionReadHeaderContext::Multipart,
                )?;
                let src_cond = copy_source_condition_from_headers(req);
                let copy_source_range =
                    if let Some(range_header) = req.header("x-amz-copy-source-range") {
                        Some(crate::range::parse_copy_source_range(range_header)?)
                    } else {
                        None
                    };
                let result = self
                    .coordinator
                    .upload_part_copy(&UploadPartCopyRequest {
                        source: CopySource::new(
                            src_bucket,
                            src_key,
                            src_version_id,
                            &src_cond,
                            expected_source_bucket_owner(req),
                        ),
                        upload: multipart_object_request(
                            &bucket,
                            &key,
                            upload_id,
                            requester,
                            expected_bucket_owner,
                        )?,
                        part_number,
                        copy_source_range,
                        policy_context: PutObjectPolicyContext::new(Some(copy_source), None, None),
                        source_sse_customer: source_sse_customer.as_ref(),
                        sse_customer: sse_customer.as_ref(),
                    })
                    .map_err(|err| match err {
                        ServerError::PreconditionFailed { condition } => {
                            ServerError::UploadPartCopyPreconditionFailed {
                                condition: condition.to_string(),
                            }
                        }
                        other => other,
                    })?;
                Ok(S3Response::upload_part_copy(
                    &result.etag,
                    result.last_modified,
                    result.checksum.as_ref(),
                    result.managed_encryption,
                    result.sse_customer.as_ref(),
                ))
            }
            S3Operation::CompleteMultipartUpload { bucket, key } => {
                reject_managed_encryption_read_headers(
                    req,
                    ManagedEncryptionReadHeaderContext::Multipart,
                )?;
                let upload_id =
                    parse_required_upload_id(req.query_param_lossy("uploadId").as_deref())?;
                let upload_id_text = upload_id.as_str().to_string();
                let wire_ids = WireResponseIds::new(
                    current_trace_context().request_id(),
                    self.host_id.clone(),
                );
                let parts = match xml::parse_complete_multipart_upload_xml(&req.body) {
                    Ok(parts) => parts,
                    Err(ServerError::MalformedXML { .. }) => {
                        return Ok(S3Response::complete_multipart_malformed_xml(&wire_ids));
                    }
                    Err(err) => return Err(err),
                };
                let cond = write_condition_from_headers(req)?;
                // Extract object-level checksum claim from request headers as a raw
                // string. CompleteMultipartUpload checksums may be composite ("base64-N"),
                // so we cannot decode them as plain base64.
                let claimed_checksum = extract_encoded_checksum_header(req)?;
                let expected_object_size = req
                    .header("x-amz-mp-object-size")
                    .map(|value| {
                        value
                            .parse::<u64>()
                            .map_err(|_| ServerError::InvalidRequest {
                                reason: format!("invalid x-amz-mp-object-size: {value}"),
                            })
                    })
                    .transpose()?;
                let sse_customer = parse_sse_customer_request(req)?;
                let requester = self.requester_from_auth(auth, req);
                let result = match self.coordinator.complete_multipart_upload(
                    &crate::coordinator::CompleteMultipartUploadRequest {
                        upload: multipart_object_request(
                            &bucket,
                            &key,
                            upload_id,
                            requester,
                            expected_bucket_owner,
                        )?,
                        parts: &parts,
                        claimed_checksum: claimed_checksum.as_ref(),
                        expected_object_size,
                        cond: &cond,
                        sse_customer: sse_customer.as_ref(),
                    },
                ) {
                    Ok(result) => result,
                    Err(ServerError::NoSuchUpload { .. }) => {
                        return Ok(S3Response::complete_multipart_no_such_upload(
                            &upload_id_text,
                            &wire_ids,
                        ));
                    }
                    Err(ServerError::InvalidPart { part_number }) => {
                        let etag = parts
                            .iter()
                            .find(|part| part.part_number == part_number)
                            .map(|part| part.etag.as_str())
                            .unwrap_or("");
                        return Ok(S3Response::complete_multipart_invalid_part(
                            &upload_id_text,
                            part_number,
                            etag,
                            &wire_ids,
                        ));
                    }
                    Err(ServerError::InvalidPartOrder) => {
                        return Ok(S3Response::complete_multipart_invalid_part_order(
                            &upload_id_text,
                            &wire_ids,
                        ));
                    }
                    Err(ServerError::EntityTooSmall {
                        part_number,
                        size,
                        min,
                    }) => {
                        let etag = parts
                            .iter()
                            .find(|part| part.part_number == part_number)
                            .map(|part| part.etag.as_str())
                            .unwrap_or("");
                        return Ok(S3Response::complete_multipart_entity_too_small(
                            size,
                            min,
                            part_number,
                            etag,
                            &wire_ids,
                        ));
                    }
                    Err(err) => return Err(err),
                };
                Ok(S3Response::complete_multipart_upload(
                    bucket.as_str(),
                    &key,
                    self.coordinator.region(),
                    &result,
                ))
            }
            S3Operation::AbortMultipartUpload { bucket, key } => {
                let upload_id =
                    parse_required_upload_id(req.query_param_lossy("uploadId").as_deref())?;
                let requester = self.requester_from_auth(auth, req);
                self.coordinator
                    .abort_multipart_upload(&multipart_object_request(
                        &bucket,
                        &key,
                        upload_id,
                        requester,
                        expected_bucket_owner,
                    )?)?;
                Ok(S3Response::abort_multipart_upload())
            }
            S3Operation::ListMultipartUploads { bucket } => {
                let prefix = req.query_param_lossy("prefix");
                let key_marker = req.query_param_lossy("key-marker");
                let upload_id_marker = req.query_param_lossy("upload-id-marker");
                let parsed_upload_id_marker =
                    parse_optional_upload_id_marker(upload_id_marker.as_deref())?;
                let encoding_type = req.query_param_lossy("encoding-type");
                let max_uploads = parse_s3_list_limit(
                    req.query_param_lossy("max-uploads"),
                    "invalid max-uploads",
                )?;
                let requester = self.requester_from_auth(auth, req);
                let result = self.coordinator.list_multipart_uploads(
                    &crate::coordinator::ListMultipartUploadsRequest {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        prefix: prefix.as_deref(),
                        key_marker: key_marker.as_deref(),
                        upload_id_marker: parsed_upload_id_marker,
                        max_uploads,
                    },
                )?;
                let rendered = self.render_multipart_uploads(result);
                Ok(S3Response::list_multipart_uploads(
                    bucket.as_str(),
                    prefix.as_deref(),
                    key_marker.as_deref(),
                    upload_id_marker.as_deref(),
                    encoding_type.as_deref(),
                    max_uploads,
                    &rendered,
                ))
            }
            S3Operation::ListParts { bucket, key } => {
                let upload_id =
                    parse_required_upload_id(req.query_param_lossy("uploadId").as_deref())?;
                let part_number_marker = parse_optional_u32(
                    req.query_param_lossy("part-number-marker"),
                    "part-number-marker must be an integer",
                )?;
                let max_parts =
                    parse_s3_list_limit(req.query_param_lossy("max-parts"), "invalid max-parts")?;
                let requester = self.requester_from_auth(auth, req);
                let result =
                    self.coordinator
                        .list_parts(&crate::coordinator::ListPartsRequest {
                            upload: multipart_object_request(
                                &bucket,
                                &key,
                                upload_id.clone(),
                                requester,
                                expected_bucket_owner,
                            )?,
                            part_number_marker,
                            max_parts,
                        })?;
                let owner = xml::RenderedCanonicalUser {
                    canonical_id: result.owner.canonical_id.clone(),
                    display_name: None,
                };
                let initiator = xml::RenderedCanonicalUser {
                    canonical_id: result.initiator.canonical_id.clone(),
                    display_name: self
                        .credentials
                        .find_account_by_canonical_user_id(&result.initiator.canonical_id)
                        .map(|account| account.display_name().to_string()),
                };
                Ok(S3Response::list_parts(
                    bucket.as_str(),
                    &key,
                    upload_id.as_str(),
                    part_number_marker,
                    max_parts,
                    &initiator,
                    &owner,
                    &result,
                ))
            }
            // OptionsRequest is handled before auth in handle_s3_request
            S3Operation::OptionsRequest { .. } => {
                unreachable!("OPTIONS handled before dispatch")
            }
            S3Operation::ListObjectVersions { bucket } => {
                let prefix = req.query_param_lossy("prefix");
                let delimiter = req.query_param_lossy("delimiter").filter(|d| !d.is_empty());
                let key_marker = req.query_param_lossy("key-marker");
                let encoding_type = req.query_param_lossy("encoding-type");
                let version_id_marker = parse_optional_version_id(
                    req.query_param_lossy("version-id-marker"),
                    "version-id-marker",
                )?;
                if version_id_marker.is_some() && key_marker.is_none() {
                    return Err(ServerError::InvalidArgument {
                        reason: "A version-id marker cannot be specified without a key marker."
                            .to_string(),
                    });
                }
                let max_keys_param = req.query_param_lossy("max-keys");
                let requested_max_keys = parse_requested_max_keys(max_keys_param.as_deref())?;
                let policy_requested_max_keys = max_keys_param
                    .as_deref()
                    .map(|value| parse_requested_max_keys(Some(value)))
                    .transpose()?;
                let requester = self.requester_from_auth(auth, req);

                let result = self.coordinator.list_object_versions(
                    &crate::coordinator::ListObjectVersionsRequest {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        prefix: prefix.as_deref(),
                        delimiter: delimiter.as_deref(),
                        key_marker: key_marker.as_deref(),
                        version_id_marker,
                        max_keys: requested_max_keys,
                        requested_max_keys: policy_requested_max_keys,
                    },
                )?;
                Ok(S3Response::list_object_versions(
                    bucket.as_str(),
                    prefix.as_deref(),
                    delimiter.as_deref(),
                    key_marker.as_deref(),
                    encoding_type.as_deref(),
                    requested_max_keys,
                    &result,
                ))
            }
        }
    }

    fn unsupported_sigv2_error(authorization: Option<&str>, region: &str) -> Option<ServerError> {
        if authorization.is_some_and(|value| value.starts_with("AWS ")) && requires_sigv4(region) {
            return Some(ServerError::InvalidRequest {
                reason: "The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256.".to_string(),
            });
        }
        None
    }

    fn authenticate(
        &self,
        req: &S3Request,
        defer_region_check: bool,
    ) -> Result<AuthContext, ServerError> {
        self.authenticate_with_payload_check(req, true, defer_region_check)
    }

    /// Authenticate a request, optionally skipping x-amz-content-sha256 body
    /// verification.
    ///
    /// Streaming write setup passes `verify_payload_hash = false` because the
    /// body is consumed incrementally in `serve.rs`. Buffered request handling
    /// must pass `true`.
    fn authenticate_with_payload_check(
        &self,
        req: &S3Request,
        verify_payload_hash: bool,
        defer_region_check: bool,
    ) -> Result<AuthContext, ServerError> {
        let now = current_auth_epoch_secs()?;

        let auth_result = if defer_region_check {
            authenticate_request(
                req.method.as_str(),
                req.path(),
                req.query_string(),
                &req.header_source(),
                &req.body,
                &self.credentials,
                auth::ExpectedSigningRegion::DeferredToBucketRouting,
                "s3",
                now,
            )
        } else {
            authenticate_request(
                req.method.as_str(),
                req.path(),
                req.query_string(),
                &req.header_source(),
                &req.body,
                &self.credentials,
                auth::ExpectedSigningRegion::ExactEndpointRegion(self.coordinator.region()),
                "s3",
                now,
            )
        };

        let auth = match auth_result {
            Ok(auth) => auth,
            Err(auth::AuthError::MissingAuth) => AuthContext::anonymous(),
            // Missing x-amz-date when declared as signed → AccessDenied
            // AWS: "AWS authentication requires a valid Date or x-amz-date header"
            Err(auth::AuthError::MissingSignedHeader { header }) if header == "x-amz-date" => {
                return Err(ServerError::Auth(auth::AuthError::AccessDenied));
            }
            Err(auth::AuthError::UnsupportedAuthType) => {
                if let Some(err) = Self::unsupported_sigv2_error(
                    req.header("authorization"),
                    self.coordinator.region(),
                ) {
                    return Err(err);
                }
                return Err(ServerError::Auth(auth::AuthError::UnsupportedAuthType));
            }
            Err(err) => return Err(ServerError::Auth(err)),
        };

        // Verify payload integrity: if the client provided an actual content hash
        // (not UNSIGNED-PAYLOAD and not STREAMING-*), recompute and compare to
        // detect transit corruption. STREAMING-* bodies are verified by the
        // chunked decoder.
        if verify_payload_hash {
            if let Some(claimed) = req.header("x-amz-content-sha256") {
                if claimed != "UNSIGNED-PAYLOAD" && !claimed.starts_with("STREAMING-") {
                    let actual = auth::canonical::sha256_hex(&req.body);
                    if actual != claimed {
                        return Err(ServerError::XAmzContentSHA256Mismatch {
                            client_hash: claimed.to_string(),
                            server_hash: actual,
                        });
                    }
                }
            }
        }

        Ok(auth)
    }

    fn require_content_sha256_for_sigv4_header_auth(req: &S3Request) -> Result<(), ServerError> {
        if req.header("x-amz-content-sha256").is_some() {
            return Ok(());
        }
        if req.header_count("authorization") == 1
            && req
                .header("authorization")
                .is_some_and(|value| value.starts_with("AWS4-HMAC-SHA256"))
        {
            return Err(ServerError::InvalidRequest {
                reason: "Missing required header for this request: x-amz-content-sha256"
                    .to_string(),
            });
        }
        Ok(())
    }

    fn should_defer_region_check(&self, operation: &S3Operation) -> bool {
        operation.bucket_name().is_some()
    }

    fn enforce_bucket_region_for_operation(
        &self,
        operation: &S3Operation,
        auth: &AuthContext,
    ) -> Result<(), ServerError> {
        let Some(bucket) = operation.bucket_name() else {
            return Ok(());
        };
        self.enforce_bucket_region(bucket, auth)
    }

    fn enforce_bucket_region(
        &self,
        bucket: &BucketName,
        auth: &AuthContext,
    ) -> Result<(), ServerError> {
        let Some(signing_region) = auth.signing_region.as_deref() else {
            return Ok(());
        };
        if signing_region == self.coordinator.region() {
            return Ok(());
        }
        match auth.mode {
            AuthMode::HeaderSigV4 => Err(ServerError::WrongRegion {
                provided_region: signing_region.to_string(),
                expected_region: self.coordinator.region().to_string(),
                bucket_region_header: self.coordinator.bucket_exists(bucket)?,
            }),
            AuthMode::PresignedSigV4 => Err(ServerError::Auth(
                auth::AuthError::InvalidQueryCredentialRegion {
                    param: "X-Amz-Credential",
                    provided_region: signing_region.to_string(),
                    expected_region: self.coordinator.region().to_string(),
                },
            )),
            AuthMode::PostSigV4 => {
                // POST form credentials are authenticated with
                // ExactEndpointRegion before AuthContext is built. Reaching
                // this deferred bucket-region check would mean a future
                // refactor bypassed the AWS-shaped POST scope validation.
                debug_assert_eq!(
                    signing_region,
                    self.coordinator.region(),
                    "POST SigV4 region mismatch must be rejected during form credential validation"
                );
                Err(ServerError::InternalError {
                    reason:
                        "POST SigV4 reached deferred bucket-region check after credential validation"
                            .to_string(),
                })
            }
            AuthMode::Anonymous => Ok(()),
        }
    }

    fn enforce_bucket_region_raw(
        &self,
        bucket: &str,
        auth: &AuthContext,
    ) -> Result<(), ServerError> {
        let bucket = parse_bucket_name(bucket)?;
        self.enforce_bucket_region(&bucket, auth)
    }

    /// Reject streaming requests that fell through `is_streaming_write` in serve.rs.
    ///
    /// Valid streaming requests are handled by the streaming path in serve.rs.
    /// If a request with `x-amz-content-sha256: STREAMING-*` reaches the
    /// non-streaming path, it means preconditions were missing (content-encoding,
    /// decoded-content-length, etc.). Reject rather than processing raw wire data.
    fn reject_streaming_fallthrough(&self, req: &S3Request) -> Result<(), ServerError> {
        let content_sha = match req.header("x-amz-content-sha256") {
            Some(v) if v.starts_with("STREAMING-") => v,
            _ => return Ok(()),
        };

        // Whitelist allowed streaming tokens — reject unknown ones.
        match content_sha {
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
            | "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
            | "STREAMING-UNSIGNED-PAYLOAD-TRAILER" => {}
            _ => {
                return Err(ServerError::UnsupportedStreamingToken {
                    token: content_sha.to_string(),
                });
            }
        }

        // Require x-amz-decoded-content-length.
        let expected_str = req
            .header("x-amz-decoded-content-length")
            .ok_or(ServerError::MissingContentLength)?;
        expected_str
            .parse::<usize>()
            .map_err(|_| ServerError::InvalidRequest {
                reason: format!("invalid x-amz-decoded-content-length: {expected_str}"),
            })?;

        // If all preconditions are met, the request should have gone through the
        // streaming path. If it didn't (e.g. non-PUT operation), reject it.
        Err(ServerError::InvalidRequest {
            reason: "streaming upload not supported for this operation".to_string(),
        })
    }

    /// If the request uses aws-chunked encoding, decode the body and return
    /// a new `S3Request` with the decoded payload. Returns None for non-chunked requests.
    ///
    /// Used only in unit tests — production uses `IncrementalChunkedDecoder`
    /// via the streaming path in serve.rs.
    #[cfg(test)]
    fn maybe_decode_chunked(
        &self,
        req: &S3Request,
        auth: &AuthContext,
    ) -> Result<Option<S3Request>, ServerError> {
        let content_sha = match req.header("x-amz-content-sha256") {
            Some(v) if v.starts_with("STREAMING-") => v,
            _ => return Ok(None),
        };

        // Whitelist allowed streaming tokens.
        match content_sha {
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
            | "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
            | "STREAMING-UNSIGNED-PAYLOAD-TRAILER" => {}
            _ => {
                return Err(ServerError::UnsupportedStreamingToken {
                    token: content_sha.to_string(),
                });
            }
        }

        // Require x-amz-decoded-content-length.
        let expected_str = req
            .header("x-amz-decoded-content-length")
            .ok_or(ServerError::MissingContentLength)?;
        let expected_len =
            expected_str
                .parse::<usize>()
                .map_err(|_| ServerError::InvalidRequest {
                    reason: format!("invalid x-amz-decoded-content-length: {expected_str}"),
                })?;

        let is_trailer_mode = content_sha.ends_with("-TRAILER");

        let is_signed = content_sha.starts_with("STREAMING-AWS4-HMAC-SHA256");

        let streaming_ctx = if is_signed {
            Some(auth.streaming.as_ref().ok_or_else(|| {
                ServerError::Auth(auth::AuthError::SignatureMismatch { diagnostics: None })
            })?)
        } else {
            None
        };

        let decoded = chunked::decode_chunked_body(&req.body, streaming_ctx, is_trailer_mode)?;

        // Validate decoded length.
        if decoded.data.len() != expected_len {
            return Err(ServerError::MalformedChunkedBody {
                reason: format!(
                    "decoded content length mismatch: expected {}, got {}",
                    expected_len,
                    decoded.data.len()
                ),
            });
        }

        // Trailer declaration validation.
        // Content trailers = trailers excluding x-amz-trailer-signature.
        let content_trailers: Vec<&(String, String)> = decoded
            .trailers
            .iter()
            .filter(|(k, _)| k != "x-amz-trailer-signature")
            .collect();

        let declared_trailer = req.header("x-amz-trailer");

        if !is_trailer_mode && !content_trailers.is_empty() {
            return Err(ServerError::IncompleteBody);
        }

        if !content_trailers.is_empty() && declared_trailer.is_none() {
            return Err(ServerError::MalformedTrailerError {
                reason: "trailers present in body but x-amz-trailer header missing".to_string(),
            });
        }

        if let Some(declared) = declared_trailer {
            // Parse declared trailer names as comma-separated list.
            let declared_names: Vec<String> = declared
                .split(',')
                .map(|s| s.trim().to_ascii_lowercase())
                .filter(|s| !s.is_empty())
                .collect();

            if content_trailers.is_empty() {
                return Err(ServerError::MalformedTrailerError {
                    reason: format!(
                        "x-amz-trailer header declares {declared} but no trailers in body"
                    ),
                });
            }
            // Check that all content trailers were declared.
            for (name, _) in &content_trailers {
                if !declared_names.iter().any(|d| d == name.as_str()) {
                    return Err(ServerError::MalformedTrailerError {
                        reason: format!(
                            "undeclared trailer in body: {name} (declared: {declared})"
                        ),
                    });
                }
            }
            // Check that all declared names appear in body (exact-set).
            let body_names: Vec<&str> = content_trailers.iter().map(|(k, _)| k.as_str()).collect();
            for name in &declared_names {
                if !body_names.contains(&name.as_str()) {
                    return Err(ServerError::MalformedTrailerError {
                        reason: format!(
                            "declared trailer missing from body: {name} (declared: {declared})"
                        ),
                    });
                }
            }
        }

        Ok(Some(req.with_decoded_body(decoded.data, decoded.trailers)))
    }

    // ── Streaming write helpers ─────────────────────────────────────

    /// Prepare a streaming `PostObject` session after multipart field parsing.
    ///
    /// `form_fields` are non-file multipart fields parsed in order.
    fn prepare_streaming_post_object(
        &self,
        req: &S3Request,
        bucket: &str,
        form_fields: &[(String, String)],
        file_name: Option<&str>,
    ) -> Result<StreamingPostContext, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::prepare_streaming_post_object",
            "bucket={:?} file_name_present={}",
            bucket,
            file_name.is_some()
        );
        if req.header_count("authorization") == 1
            && req
                .header("authorization")
                .is_some_and(|value| value.starts_with("AWS4-HMAC-SHA256"))
        {
            Self::require_content_sha256_for_sigv4_header_auth(req)?;
            return Err(ServerError::PostObjectHeaderAuthUnsupported);
        }
        let header_auth = self.authenticate_with_payload_check(req, false, true)?;

        let field = |name: &str| -> Option<&str> {
            form_fields
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.as_str())
        };
        let now = current_auth_epoch_secs()?;

        let post_auth = if [
            "x-amz-algorithm",
            "x-amz-credential",
            "x-amz-date",
            "policy",
            "x-amz-signature",
        ]
        .iter()
        .any(|name| field(name).is_some())
        {
            let algo = field("x-amz-algorithm").ok_or_else(|| ServerError::InvalidRequest {
                reason: "missing x-amz-algorithm".to_string(),
            })?;
            auth::authenticate_post_sigv4(
                auth::PostSigV4Request {
                    algorithm: algo,
                    credential: field("x-amz-credential").ok_or_else(|| {
                        ServerError::InvalidRequest {
                            reason: "missing x-amz-credential".to_string(),
                        }
                    })?,
                    date: field("x-amz-date").ok_or_else(|| ServerError::InvalidRequest {
                        reason: "missing x-amz-date".to_string(),
                    })?,
                    policy_b64: field("policy").ok_or_else(|| ServerError::InvalidRequest {
                        reason: "missing policy".to_string(),
                    })?,
                    signature_hex: field("x-amz-signature").ok_or_else(|| {
                        ServerError::InvalidRequest {
                            reason: "missing x-amz-signature".to_string(),
                        }
                    })?,
                    security_token: field("x-amz-security-token"),
                },
                &self.credentials,
                auth::ExpectedCredentialScope::new(
                    auth::ExpectedSigningRegion::ExactEndpointRegion(self.coordinator.region()),
                    "s3",
                ),
                now,
            )
            .map_err(ServerError::Auth)?
        } else {
            AuthContext::anonymous()
        };

        // Resolve object key (with ${filename} substitution).
        let pseudo_form = multipart::PostFormData {
            fields: form_fields.to_vec(),
            file_data: Vec::new(),
            file_name: file_name.map(std::string::ToString::to_string),
        };
        let key = pseudo_form.resolve_key()?;

        // Prefer explicit POST auth when present; otherwise fall back to header
        // auth, which may be anonymous for public-write buckets.
        let effective_auth = if post_auth.mode == AuthMode::Anonymous {
            &header_auth
        } else {
            &post_auth
        };
        self.enforce_bucket_region_raw(bucket, effective_auth)?;

        let post_policy = if let Some(policy_b64) = field("policy") {
            let mut field_pairs: Vec<(&str, &str)> = form_fields
                .iter()
                .filter(|(k, _)| !k.eq_ignore_ascii_case("key"))
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            field_pairs.push(("key", key.as_str()));
            Some(
                auth::prepare_post_policy(policy_b64, &field_pairs, bucket, now)
                    .map_err(Self::map_post_policy_error)?,
            )
        } else {
            None
        };

        // Build metadata headers from form fields.
        let mut header_pairs: Vec<(String, String)> = Vec::new();
        if let Some(ct) = field("Content-Type") {
            header_pairs.push(("content-type".to_string(), ct.to_string()));
        }
        // Pass through x-amz-meta-* fields.
        for (k, v) in form_fields {
            if k.to_ascii_lowercase().starts_with("x-amz-meta-") {
                header_pairs.push((k.to_ascii_lowercase(), v.clone()));
            }
        }
        // Also pass cache-control, content-disposition, etc.
        for name in &[
            "cache-control",
            "content-disposition",
            "content-encoding",
            "content-language",
            "expires",
            WEBSITE_REDIRECT_LOCATION_HEADER_NAME,
        ] {
            if let Some(val) = field(name) {
                header_pairs.push((name.to_string(), val.to_string()));
            }
        }
        let hp_refs: Vec<(&str, &str)> = header_pairs
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let (metadata_blob, system_metadata) = parse_request_metadata(hp_refs.iter().copied())?;
        let tags_xml = if let Some(tagging_field) = field("tagging") {
            let tags = xml::parse_tagging_xml(tagging_field.as_bytes(), 10)?;
            if tags.is_empty() {
                None
            } else {
                Some(xml::get_tagging_xml(&tags))
            }
        } else {
            None
        };
        let sse_customer_request =
            parse_sse_customer_form_fields(req.transport_security, form_fields)?;
        let managed_encryption =
            parse_managed_encryption_form_fields(form_fields, sse_customer_request.is_some())?;
        let cond = write_condition_from_headers(req)?;

        let requester = self.requester_from_auth(effective_auth, req);
        let acl = parse_put_object_acl(field("acl"));
        let request_encryption = crate::coordinator::WriteEncryptionRequest::from_request_parts(
            sse_customer_request.as_ref(),
            managed_encryption,
        )?;
        let bucket_name = parse_bucket_name(bucket)?;
        let prepared_put = self
            .coordinator
            .begin_stream_put(&AuthorizePutObjectRequest {
                object: ObjectRequest::new(
                    bucket_name.clone(),
                    key.clone(),
                    requester.clone(),
                    None,
                ),
                acl: acl.into(),
                policy_context: crate::coordinator::PutObjectPolicyContext::new(
                    None,
                    None,
                    acl.policy_condition_value(),
                )
                .with_if_match(req.header("if-match"))
                .with_if_none_match(cond.if_none_match_policy_value())
                .with_managed_encryption(managed_encryption)
                .with_sse_customer_algorithm(
                    sse_customer_request.as_ref().map(|req| req.algorithm()),
                )
                .with_website_redirect_location(field(WEBSITE_REDIRECT_LOCATION_HEADER_NAME))
                .with_request_object_tags_xml(tags_xml.as_deref()),
                object_lock: ObjectLockState::default(),
                tags: tags_xml.as_deref(),
                encryption: request_encryption,
            })?;

        let success_status = field("success_action_status")
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(204);
        let success_redirect = field("success_action_redirect")
            .or_else(|| field("redirect"))
            .filter(|s| !s.is_empty())
            .map(std::string::ToString::to_string);
        let response_location = req.header("host").map(|host| {
            let scheme = if req.transport_security.is_secure() {
                "https"
            } else {
                "http"
            };
            format!(
                "{scheme}://{host}/{}/{}",
                percent_encode_location_path_segment(bucket_name.as_str()),
                percent_encode_location_key(key.as_str())
            )
        });

        Ok(StreamingPostContext {
            trace: current_trace_context(),
            session_id: prepared_put.session_id,
            storage_node: prepared_put.storage_node,
            metadata_blob,
            system_metadata,
            success_status,
            success_redirect,
            response_location,
            post_policy,
            checksum: post_checksum_claim_from_fields(form_fields)?,
            sse_customer: sse_customer_request,
            authorized_write: prepared_put.authorized_write,
        })
    }

    /// Finalize a streaming `PostObject` session and return a POST response.
    fn finalize_streaming_post_object(
        &self,
        ctx: &StreamingPostContext,
        crc64: u64,
        total_size: u64,
        actual_checksum: Option<&RawChecksum>,
    ) -> Result<S3Response, ServerError> {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::finalize_streaming_post_object",
            "bucket={:?} key={:?} bytes={}",
            ctx.bucket(),
            ctx.key(),
            total_size
        );
        // Validate late size-dependent policy constraints.
        if let Some(post_policy) = ctx.post_policy.as_ref() {
            let file_size =
                usize::try_from(total_size).map_err(|_| ServerError::ObjectTooLarge {
                    size: total_size,
                    max: crate::coordinator::MAX_OBJECT_SIZE,
                })?;

            auth::validate_prepared_post_policy_size(post_policy, file_size)
                .map_err(Self::map_post_policy_error)?;
        }

        if let Some(claimed) = ctx.checksum.as_ref() {
            let Some(actual) = actual_checksum else {
                return Err(ServerError::InvalidRequest {
                    reason: "checksum algorithm was not computed".to_string(),
                });
            };
            if actual.algorithm() != claimed.algorithm()
                || actual.bytes() != claimed.expected_bytes()
            {
                return Err(ServerError::ChecksumDigestMismatch {
                    algorithm: claimed.algorithm().as_str().to_string(),
                });
            }
        }

        let result = self
            .coordinator
            .finalize_authorized_stream_put_with_storage_node(
                &ctx.storage_node,
                &crate::coordinator::AuthorizedFinalizeStreamPutRequest {
                    session_id: ctx.session_id(),
                    crc64,
                    total_size,
                    metadata_blob: &ctx.metadata_blob,
                    system_metadata: &ctx.system_metadata,
                    write_encryption: crate::coordinator::ActiveWriteEncryptionRef::None,
                    cond: &crate::conditional::WriteCondition::default(),
                },
                &ctx.authorized_write,
                ctx.sse_customer.as_ref(),
            )?;

        let mut resp = S3Response::post_object(
            &result,
            ctx.bucket().as_str(),
            ctx.key().as_str(),
            ctx.success_status,
            ctx.success_redirect.as_deref(),
            ctx.response_location.as_deref(),
        );
        apply_sse_customer_write_response_headers(&mut resp, ctx.sse_customer.as_ref());
        Ok(resp)
    }

    fn map_post_policy_error(error: auth::PostPolicyError) -> ServerError {
        match error {
            auth::PostPolicyError::InvalidDocument(reason) => {
                ServerError::InvalidPolicyDocument { reason }
            }
            auth::PostPolicyError::Malformed(_) => ServerError::InvalidRequest {
                reason: error.to_string(),
            },
            auth::PostPolicyError::ConditionFailed {
                condition: "content-length-range",
                ..
            } => ServerError::InvalidRequest {
                reason: error.to_string(),
            },
            auth::PostPolicyError::ConditionFailed {
                field: Some(field), ..
            } => ServerError::PostPolicyAccessDenied {
                reason: format!("Access denied by POST policy condition on field '{field}'"),
            },
            auth::PostPolicyError::Expired | auth::PostPolicyError::ConditionFailed { .. } => {
                ServerError::Auth(auth::AuthError::AccessDenied)
            }
        }
    }

    /// Append a segment to a streaming POST session.
    fn streaming_append_post_segment(
        &self,
        ctx: &StreamingPostContext,
        segment_index: u32,
        data: &[u8],
    ) -> Result<(), ServerError> {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::streaming_append_post_segment",
            "bucket={:?} key={:?} segment_index={} bytes={}",
            ctx.bucket(),
            ctx.key(),
            segment_index,
            data.len()
        );
        self.coordinator.append_stream_put_data_with_storage_node(
            &ctx.storage_node,
            &crate::coordinator::AppendStreamPutRequest {
                bucket: ctx.authorized_write.bucket_typed(),
                key: ctx.authorized_write.key_typed(),
                session_id: ctx.session_id(),
                segment_index,
                data,
                sse_customer: ctx.sse_customer.as_ref(),
            },
        )
    }

    /// Abort a streaming POST session (best-effort cleanup).
    fn abort_streaming_post_object(&self, ctx: &StreamingPostContext) {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::abort_streaming_post_object",
            "bucket={:?} key={:?}",
            ctx.bucket(),
            ctx.key()
        );
        let _ = self.coordinator.abort_stream_put_session_with_storage_node(
            &ctx.storage_node,
            ctx.bucket(),
            ctx.key(),
            ctx.session_id(),
        );
    }

    fn merged_streaming_put_metadata_blob(
        ctx: &StreamingPutContext,
        trailer_checksums: &[(String, String)],
    ) -> crate::metadata_blob::MetadataBlob {
        let _ = trailer_checksums;
        ctx.metadata_blob.clone()
    }

    fn merged_streaming_put_system_metadata(
        ctx: &StreamingPutContext,
        trailer_checksums: &[(String, String)],
    ) -> SystemMetadata {
        if trailer_checksums.is_empty() {
            return ctx.system_metadata.clone();
        }

        let mut metadata = ctx.system_metadata.clone();
        for (name, value) in trailer_checksums {
            if let Some(algo) = checksum_algo_from_header(name) {
                metadata.set_checksum(algo, None, value.clone());
            }
        }
        metadata
    }

    fn apply_streaming_put_checksum_headers(
        ctx: &StreamingPutContext,
        resp: &mut S3Response,
        trailer_checksums: &[(String, String)],
    ) {
        for (name, value) in &ctx.checksum.response_headers.0 {
            if let Some((_, tv)) = trailer_checksums.iter().find(|(k, _)| k == name) {
                resp.headers.push((name.clone(), tv.clone()));
            } else {
                resp.headers.push((name.clone(), value.clone()));
            }
        }
        for (name, value) in trailer_checksums {
            if !ctx
                .checksum
                .response_headers
                .0
                .iter()
                .any(|(k, _)| k == name)
            {
                resp.headers.push((name.clone(), value.clone()));
            }
        }
    }

    /// Prepare a streaming `PutObject`: authenticate and validate the request.
    ///
    /// Returns a context struct that the async streaming loop uses to drive
    /// either the direct single-segment fast path or a promoted streaming
    /// session once the body exceeds one internal segment.
    fn prepare_streaming_put(
        &self,
        req: &S3Request,
        bucket: &str,
        key: &str,
        uses_aws_chunked_transport: bool,
    ) -> Result<StreamingPutContext, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::prepare_streaming_put",
            "bucket={:?} key={:?}",
            bucket,
            key
        );
        Self::require_content_sha256_for_sigv4_header_auth(req)?;
        let auth = self.authenticate_with_payload_check(req, false, true)?;
        self.enforce_bucket_region_raw(bucket, &auth)?;
        reject_directory_bucket_only_object_features(req)?;

        let object_lock = parse_object_lock_headers(req)?;
        let checksum_state = validate_request_checksum_headers(req, false, true, true)?;
        if object_lock.retention.is_some()
            && !checksum_state.has_content_md5
            && !checksum_state.has_checksum_header
            && !checksum_state.has_trailing_checksum
        {
            return Err(ServerError::InvalidRequest {
                reason: "Content-MD5 OR x-amz-checksum- HTTP header is required for Put Object requests with Object Lock parameters".to_string(),
            });
        }
        if !uses_aws_chunked_transport && req.header("content-length").is_none() {
            return Err(ServerError::MissingContentLength);
        }
        let content_md5 = ContentMd5Claim::from_request(req)?;
        let sse_customer_request = parse_sse_customer_request(req)?;
        let managed_encryption =
            parse_managed_encryption_request(req, sse_customer_request.is_some())?;

        // Parse inline tags before starting the session.
        let inline_tags_xml = if let Some(tagging_header) = req.header("x-amz-tagging") {
            let tags = xml::parse_url_encoded_tags(tagging_header)?;
            if tags.is_empty() {
                None
            } else {
                Some(xml::get_tagging_xml(&tags))
            }
        } else {
            None
        };

        // Reject if both a trailing checksum (via x-amz-trailer) and an inline
        // checksum value header are present. AWS returns:
        //   InvalidRequest: Expecting a single x-amz-checksum- header
        let has_trailing_checksum = checksum_state.has_trailing_checksum;

        if has_trailing_checksum {
            let has_inline_checksum = ChecksumAlgorithm::ALL
                .into_iter()
                .any(|algorithm| req.header(algorithm.header_name()).is_some());
            if has_inline_checksum {
                return Err(ServerError::InvalidRequestHostId {
                    reason: "Expecting a single x-amz-checksum- header".to_string(),
                });
            }
        }

        let request_headers: Vec<(&str, &str)> = req.header_iter().collect();
        validate_write_request_header_section_size(&request_headers)?;
        let (metadata_blob, mut system_metadata) =
            parse_put_object_request_metadata(request_headers.iter().copied())?;
        if uses_aws_chunked_transport {
            system_metadata.strip_aws_chunked_content_encoding()?;
        }
        let cond = write_condition_from_headers(req)?;
        let acl_grants = parse_acl_grants_headers(req)?;
        if req.header("x-amz-acl").is_some() && acl_grants.is_some() {
            return Err(ServerError::InvalidArgument {
                reason: "x-amz-acl cannot be combined with x-amz-grant-* headers".to_string(),
            });
        }

        // Collect checksum response headers to echo back in the response.
        let mut checksum_response: Vec<(String, String)> = Vec::new();
        for (_, header) in checksum_headers() {
            if let Some(val) = req.header(header) {
                checksum_response.push((header.to_string(), val.to_string()));
            }
        }

        let bucket_name = parse_bucket_name(bucket)?;
        let object_key = parse_object_key(key)?;
        let requester = self.requester_from_auth(&auth, req);
        let storage_node = self.coordinator.storage_node_for_request();
        let authorized_write = self
            .coordinator
            .prepare_put_object_write_with_storage_node(
                &storage_node,
                &AuthorizePutObjectRequest {
                    object: ObjectRequest::new(
                        bucket_name.clone(),
                        object_key.clone(),
                        requester,
                        expected_bucket_owner(req),
                    ),
                    acl: put_object_write_acl_from_components(
                        req.header("x-amz-acl"),
                        acl_grants.as_ref(),
                    ),
                    policy_context: put_object_policy_context_from_request_fields(
                        PutObjectPolicyContextFields {
                            tags_xml: inline_tags_xml.as_deref(),
                            copy_source: None,
                            metadata_directive: None,
                            canned_acl: parse_put_object_acl(req.header("x-amz-acl"))
                                .policy_condition_value(),
                            website_redirect_location: req
                                .header(WEBSITE_REDIRECT_LOCATION_HEADER_NAME),
                            managed_encryption,
                            sse_customer_algorithm: sse_customer_request
                                .as_ref()
                                .map(SseCustomerRequest::algorithm),
                            grants: PutObjectGrantHeaders {
                                grant_read: req.header("x-amz-grant-read"),
                                grant_write: req.header("x-amz-grant-write"),
                                grant_read_acp: req.header("x-amz-grant-read-acp"),
                                grant_write_acp: req.header("x-amz-grant-write-acp"),
                                grant_full_control: req.header("x-amz-grant-full-control"),
                            },
                            conditions: PutObjectConditionalHeaders {
                                if_match: req
                                    .header("if-match")
                                    .map(if_match_header_entity_tag_value),
                                if_none_match: req.header("if-none-match"),
                            },
                        },
                    ),
                    object_lock,
                    tags: inline_tags_xml.as_deref(),
                    encryption: crate::coordinator::WriteEncryptionRequest::from_request_parts(
                        sse_customer_request.as_ref(),
                        managed_encryption,
                    )?,
                },
            )?;

        Ok(StreamingPutContext {
            trace: current_trace_context(),
            storage_node,
            bucket: bucket_name,
            key: object_key,
            metadata_blob,
            system_metadata,
            cond,
            checksum: StreamingPutChecksumContract {
                content_md5,
                response_headers: ChecksumResponseHeaders(checksum_response),
            },
            sse_customer: sse_customer_request,
            authorized_write: RwLock::new(authorized_write),
            streaming_signing: auth.streaming,
        })
    }

    /// Start a stream-backed `PutObject` session after the body has exceeded
    /// one internal segment.
    fn start_streaming_put_session(
        &self,
        ctx: &StreamingPutContext,
    ) -> Result<SessionId, ServerError> {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::start_streaming_put_session",
            "bucket={:?} key={:?}",
            ctx.bucket(),
            ctx.key()
        );
        ctx.with_authorized_write(|authorized_write| {
            self.coordinator
                .begin_stream_put_session_with_storage_node(&ctx.storage_node, authorized_write)
        })
    }

    /// Append a segment to a streaming session.
    fn streaming_append_segment(
        &self,
        ctx: &StreamingPutContext,
        session_id: &SessionId,
        segment_index: u32,
        data: &[u8],
    ) -> Result<(), ServerError> {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::streaming_append_segment",
            "bucket={:?} key={:?} segment_index={} bytes={}",
            ctx.bucket(),
            ctx.key(),
            segment_index,
            data.len()
        );
        self.coordinator.append_stream_put_data_with_storage_node(
            &ctx.storage_node,
            &crate::coordinator::AppendStreamPutRequest {
                bucket: ctx.bucket(),
                key: ctx.key(),
                session_id,
                segment_index,
                data,
                sse_customer: ctx.sse_customer.as_ref(),
            },
        )
    }

    fn heartbeat_streaming_put_object(
        &self,
        ctx: &StreamingPutContext,
        session_id: &SessionId,
    ) -> Result<(), ServerError> {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        self.coordinator
            .heartbeat_stream_put_session_with_storage_node(
                &ctx.storage_node,
                ctx.bucket(),
                ctx.key(),
                session_id,
            )
    }

    fn heartbeat_streaming_post_object(
        &self,
        ctx: &StreamingPostContext,
    ) -> Result<(), ServerError> {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        self.coordinator
            .heartbeat_stream_put_session_with_storage_node(
                &ctx.storage_node,
                ctx.bucket(),
                ctx.key(),
                ctx.session_id(),
            )
    }

    /// Commit a single-segment `PutObject` without creating a stream session.
    fn put_single_segment_object(
        &self,
        ctx: &StreamingPutContext,
        data: &[u8],
        trailer_checksums: &[(String, String)],
    ) -> Result<S3Response, ServerError> {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::put_single_segment_object",
            "bucket={:?} key={:?} bytes={} trailer_checksums={}",
            ctx.bucket(),
            ctx.key(),
            data.len(),
            trailer_checksums.len()
        );
        let metadata_blob = Self::merged_streaming_put_metadata_blob(ctx, trailer_checksums);
        let system_metadata = Self::merged_streaming_put_system_metadata(ctx, trailer_checksums);
        let result = ctx.with_authorized_write(|authorized_write| {
            self.coordinator.commit_put_object_write_with_storage_node(
                &ctx.storage_node,
                &crate::coordinator::AuthorizedPutObjectCommitRequest {
                    data,
                    metadata: &metadata_blob,
                    system_metadata: &system_metadata,
                    cond: &ctx.cond,
                },
                authorized_write,
            )
        })?;

        let mut resp = S3Response::put_object(&result);
        apply_sse_customer_write_response_headers(&mut resp, ctx.sse_customer.as_ref());
        Self::apply_streaming_put_checksum_headers(ctx, &mut resp, trailer_checksums);
        Ok(resp)
    }

    /// Finalize a streaming `PutObject` session and return an `S3Response`.
    ///
    /// `trailer_checksums` contains checksum headers extracted from aws-chunked
    /// trailers (e.g. `x-amz-checksum-crc32`). These are merged into the
    /// metadata blob for storage and echoed back in the response.
    fn finalize_streaming_put(
        &self,
        ctx: &StreamingPutContext,
        session_id: &SessionId,
        crc64: u64,
        total_size: u64,
        trailer_checksums: &[(String, String)],
    ) -> Result<S3Response, ServerError> {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::finalize_streaming_put",
            "bucket={:?} key={:?} bytes={} trailer_checksums={}",
            ctx.bucket(),
            ctx.key(),
            total_size,
            trailer_checksums.len()
        );
        let metadata_blob = Self::merged_streaming_put_metadata_blob(ctx, trailer_checksums);
        let system_metadata = Self::merged_streaming_put_system_metadata(ctx, trailer_checksums);
        let result = ctx.with_authorized_write(|authorized_write| {
            self.coordinator
                .finalize_authorized_stream_put_with_storage_node(
                    &ctx.storage_node,
                    &crate::coordinator::AuthorizedFinalizeStreamPutRequest {
                        session_id,
                        crc64,
                        total_size,
                        metadata_blob: &metadata_blob,
                        system_metadata: &system_metadata,
                        write_encryption: crate::coordinator::ActiveWriteEncryptionRef::None,
                        cond: &ctx.cond,
                    },
                    authorized_write,
                    ctx.sse_customer.as_ref(),
                )
        })?;

        let mut resp = S3Response::put_object(&result);
        apply_sse_customer_write_response_headers(&mut resp, ctx.sse_customer.as_ref());
        Self::apply_streaming_put_checksum_headers(ctx, &mut resp, trailer_checksums);
        Ok(resp)
    }

    /// Abort a streaming session (best-effort cleanup).
    fn abort_streaming_put(&self, ctx: &StreamingPutContext, session_id: &SessionId) {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::abort_streaming_put",
            "bucket={:?} key={:?}",
            ctx.bucket(),
            ctx.key()
        );
        let _ = self.coordinator.abort_stream_put_session_with_storage_node(
            &ctx.storage_node,
            ctx.bucket(),
            ctx.key(),
            session_id,
        );
    }

    /// Prepare a streaming `UploadPart` session.
    fn prepare_streaming_part(
        &self,
        req: &S3Request,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
    ) -> Result<StreamingPartContext, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::prepare_streaming_part",
            "bucket={:?} key={:?} upload_id={:?} part_number={}",
            bucket,
            key,
            upload_id,
            part_number
        );
        Self::require_content_sha256_for_sigv4_header_auth(req)?;
        let auth = self.authenticate_with_payload_check(req, false, true)?;
        self.enforce_bucket_region_raw(bucket, &auth)?;

        validate_request_checksum_headers(req, false, false, false)?;
        let content_md5 = ContentMd5Claim::from_request(req)?;
        let claimed_checksum = extract_checksum_header(req)?;
        let sse_customer_request = parse_sse_customer_request(req)?;
        let requester = self.requester_from_auth(&auth, req);
        let expected_bucket_owner = expected_bucket_owner(req).map(str::to_string);

        let mut checksum_response: Vec<(String, String)> = Vec::new();
        for (_, header) in checksum_headers() {
            if let Some(val) = req.header(header) {
                checksum_response.push((header.to_string(), val.to_string()));
            }
        }

        let bucket_name = parse_bucket_name(bucket)?;
        let upload = multipart_object_request(
            &bucket_name,
            key,
            parse_present_upload_id(upload_id)?,
            requester.clone(),
            expected_bucket_owner.as_deref(),
        )?;
        let binding_upload_id = upload.upload_id().clone();
        let binding_bucket = upload.object.bucket.name.clone();
        let binding_key = upload.object.key.clone();
        let storage_node = self.coordinator.storage_node_for_request();
        let begin = self.coordinator.begin_stream_part_with_storage_node(
            &storage_node,
            &BeginStreamPartRequest {
                upload,
                part_number,
                policy_context: crate::coordinator::PutObjectPolicyContext::default()
                    .with_sse_customer_algorithm(
                        sse_customer_request
                            .as_ref()
                            .map(SseCustomerRequest::algorithm),
                    )
                    .with_object_creation_operation(false),
                sse_customer: sse_customer_request.as_ref(),
            },
        )?;

        Ok(StreamingPartContext {
            trace: current_trace_context(),
            storage_node,
            binding: StreamPartBinding::new(
                StreamObjectBinding::new(begin.session_id, binding_bucket, binding_key),
                binding_upload_id,
                part_number,
            ),
            requester,
            expected_bucket_owner,
            checksum: StreamingPartChecksumContract {
                content_md5,
                upload_checksum_algorithm: begin.checksum_algorithm,
                claim: claimed_checksum,
                response_headers: ChecksumResponseHeaders(checksum_response),
            },
            sse_customer: begin.sse_customer,
            streaming_signing: auth.streaming,
        })
    }

    /// Append a segment to a streaming `UploadPart` session.
    fn streaming_append_part_segment(
        &self,
        ctx: &StreamingPartContext,
        segment_index: u32,
        data: &[u8],
    ) -> Result<(), ServerError> {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::streaming_append_part_segment",
            "bucket={:?} key={:?} upload_id={:?} part_number={} segment_index={} bytes={}",
            ctx.bucket(),
            ctx.key(),
            ctx.upload_id(),
            ctx.part_number(),
            segment_index,
            data.len()
        );
        self.coordinator.append_stream_part_data_with_storage_node(
            &ctx.storage_node,
            &crate::coordinator::AppendStreamPartRequest {
                bucket: ctx.bucket().clone(),
                key: ctx.key().clone(),
                upload_id: ctx.upload_id(),
                session_id: ctx.session_id(),
                part_number: ctx.part_number(),
                segment_index,
                data,
                sse_customer: ctx
                    .sse_customer
                    .as_ref()
                    .map(SseCustomerWriteContext::request),
            },
        )
    }

    /// Finalize a streaming `UploadPart` session and return an `S3Response`.
    ///
    /// `trailer_checksums` contains checksum headers from aws-chunked trailers.
    /// `computed_checksum` is the incrementally computed checksum (algo, bytes).
    #[allow(clippy::too_many_arguments)]
    fn finalize_streaming_part(
        &self,
        ctx: &StreamingPartContext,
        crc64: u64,
        total_size: u64,
        trailer_checksums: &[(String, String)],
        computed_checksum: Option<RawChecksum>,
    ) -> Result<S3Response, ServerError> {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::finalize_streaming_part",
            "bucket={:?} key={:?} upload_id={:?} part_number={} bytes={} trailer_checksums={}",
            ctx.bucket(),
            ctx.key(),
            ctx.upload_id(),
            ctx.part_number(),
            total_size,
            trailer_checksums.len()
        );
        // If trailer checksums are present, use the first one as the claimed
        // checksum (overriding any from request headers). Trailing checksums
        // take precedence since they are computed after the body is sent.
        let trailer_claim = if let Some((k, v)) = trailer_checksums.first() {
            match checksum_algo_from_header(k) {
                Some(algo) => Some(ChecksumClaim::from_base64(algo, v)?),
                None => None,
            }
        } else {
            None
        };
        let effective_claim = trailer_claim.as_ref().or(ctx.checksum.claim.as_ref());

        let result = self.coordinator.finalize_stream_part_with_storage_node(
            &ctx.storage_node,
            FinalizeStreamPartRequest {
                upload: MultipartObjectRequest::new(
                    ctx.bucket().clone(),
                    ctx.key().clone(),
                    ctx.upload_id().clone(),
                    ctx.requester.clone(),
                    ctx.expected_bucket_owner.as_deref(),
                ),
                session_id: ctx.session_id(),
                part_number: ctx.part_number(),
                crc64,
                total_size,
                claimed_checksum: effective_claim,
                computed_checksum,
            },
        )?;

        let sse_customer_headers = ctx
            .sse_customer
            .as_ref()
            .map(|sse_customer| sse_customer.request().response_headers());
        let mut resp = S3Response::upload_part(
            &result.etag,
            result.checksum.as_ref(),
            result.managed_encryption,
            sse_customer_headers.as_ref(),
        );
        // The coordinator's result already includes the checksum via
        // S3Response::upload_part. Only echo headers NOT already present
        // (e.g. x-amz-checksum-type). Trailer values take precedence.
        let already_set: Option<&str> = result
            .checksum
            .as_ref()
            .map(|c| c.algorithm().header_name());
        for (name, value) in &ctx.checksum.response_headers.0 {
            if already_set == Some(name.as_str()) {
                continue; // Already set by S3Response::upload_part
            }
            if let Some((_, tv)) = trailer_checksums.iter().find(|(k, _)| k == name) {
                resp.headers.push((name.clone(), tv.clone()));
            } else {
                resp.headers.push((name.clone(), value.clone()));
            }
        }
        for (name, value) in trailer_checksums {
            if already_set == Some(name.as_str()) {
                continue; // Already set by S3Response::upload_part
            }
            if !ctx
                .checksum
                .response_headers
                .0
                .iter()
                .any(|(k, _)| k == name)
            {
                resp.headers.push((name.clone(), value.clone()));
            }
        }
        Ok(resp)
    }

    /// Abort a streaming `UploadPart` session (best-effort cleanup).
    fn abort_streaming_part(&self, ctx: &StreamingPartContext) {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::abort_streaming_part",
            "bucket={:?} key={:?} upload_id={:?} part_number={}",
            ctx.bucket(),
            ctx.key(),
            ctx.upload_id(),
            ctx.part_number()
        );
        let _ = self
            .coordinator
            .abort_stream_part_session_with_storage_node(
                &ctx.storage_node,
                ctx.bucket(),
                ctx.key(),
                ctx.session_id(),
            );
        let _ = observability::emit_stream_upload_phase(
            &ctx.trace,
            TRACE_TARGET,
            observability::StreamUploadPhaseSummary {
                operation: "UploadPart",
                phase: observability::StreamUploadPhase::SessionAborted,
                bucket: ctx.bucket().as_str(),
                key: ctx.key().as_str(),
                upload_id: Some(ctx.upload_id().as_str()),
                part_number: Some(ctx.part_number()),
                session_id: Some(ctx.session_id().as_str()),
                segment_index: None,
                body_bytes_received: None,
                segment_bytes: None,
                segment_count: None,
            },
        );
    }
}

/// Session binding for a streaming object-scoped upload.
struct StreamObjectBinding {
    session_id: SessionId,
    bucket: BucketName,
    key: ObjectKey,
}

/// Session binding for a streaming multipart-part upload.
struct StreamPartBinding {
    object: StreamObjectBinding,
    upload_id: UploadId,
    part_number: u32,
}

/// Checksum response headers to echo back on a streaming response.
struct ChecksumResponseHeaders(Vec<(String, String)>);

/// Checksum contract for streaming `PutObject`.
struct StreamingPutChecksumContract {
    content_md5: Option<ContentMd5Claim>,
    response_headers: ChecksumResponseHeaders,
}

/// Checksum contract for streaming `UploadPart`.
struct StreamingPartChecksumContract {
    content_md5: Option<ContentMd5Claim>,
    upload_checksum_algorithm: Option<ChecksumAlgorithm>,
    claim: Option<ChecksumClaim>,
    response_headers: ChecksumResponseHeaders,
}

/// Context for an in-progress streaming `PutObject`.
///
/// Created by `prepare_streaming_put`, used across async/blocking boundaries.
struct StreamingPutContext {
    trace: observability::TraceContext,
    storage_node: Arc<StorageCluster>,
    bucket: BucketName,
    key: ObjectKey,
    metadata_blob: crate::metadata_blob::MetadataBlob,
    system_metadata: SystemMetadata,
    cond: crate::conditional::WriteCondition,
    checksum: StreamingPutChecksumContract,
    sse_customer: Option<SseCustomerRequest>,
    authorized_write: RwLock<AuthorizedPutObjectWrite>,
    /// Signing context for aws-chunked modes, None for unsigned/plain.
    streaming_signing: Option<auth::StreamingSigningContext>,
}

/// Context for an in-progress streaming `PostObject`.
struct StreamingPostContext {
    trace: observability::TraceContext,
    session_id: SessionId,
    storage_node: Arc<StorageCluster>,
    metadata_blob: crate::metadata_blob::MetadataBlob,
    system_metadata: SystemMetadata,
    success_status: u16,
    success_redirect: Option<String>,
    response_location: Option<String>,
    post_policy: Option<auth::PreparedPostPolicy>,
    checksum: Option<ChecksumClaim>,
    sse_customer: Option<SseCustomerRequest>,
    authorized_write: AuthorizedPutObjectWrite,
}

/// Context for an in-progress streaming `UploadPart`.
///
/// Created by `prepare_streaming_part`, used across async/blocking boundaries.
struct StreamingPartContext {
    trace: observability::TraceContext,
    storage_node: Arc<StorageCluster>,
    binding: StreamPartBinding,
    requester: crate::coordinator::Requester,
    expected_bucket_owner: Option<String>,
    checksum: StreamingPartChecksumContract,
    sse_customer: Option<SseCustomerWriteContext>,
    /// Signing context for aws-chunked modes, None for unsigned/plain.
    streaming_signing: Option<auth::StreamingSigningContext>,
}

impl StreamObjectBinding {
    fn new(session_id: SessionId, bucket: BucketName, key: ObjectKey) -> Self {
        Self {
            session_id,
            bucket,
            key,
        }
    }

    fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    fn bucket(&self) -> &BucketName {
        &self.bucket
    }

    fn key(&self) -> &ObjectKey {
        &self.key
    }
}

impl StreamPartBinding {
    fn new(object: StreamObjectBinding, upload_id: UploadId, part_number: u32) -> Self {
        Self {
            object,
            upload_id,
            part_number,
        }
    }

    fn session_id(&self) -> &SessionId {
        self.object.session_id()
    }

    fn bucket(&self) -> &BucketName {
        self.object.bucket()
    }

    fn key(&self) -> &ObjectKey {
        self.object.key()
    }

    fn upload_id(&self) -> &UploadId {
        &self.upload_id
    }

    fn part_number(&self) -> u32 {
        self.part_number
    }
}

impl StreamingPutContext {
    fn bucket(&self) -> &BucketName {
        &self.bucket
    }

    fn key(&self) -> &ObjectKey {
        &self.key
    }

    fn with_authorized_write<T>(&self, f: impl FnOnce(&AuthorizedPutObjectWrite) -> T) -> T {
        let guard = self.authorized_write.read().unwrap();
        f(&guard)
    }
}

impl StreamingPostContext {
    fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    fn bucket(&self) -> &BucketName {
        self.authorized_write.bucket_typed()
    }

    fn key(&self) -> &ObjectKey {
        self.authorized_write.key_typed()
    }
}

impl StreamingPartContext {
    fn session_id(&self) -> &SessionId {
        self.binding.session_id()
    }

    fn bucket(&self) -> &BucketName {
        self.binding.bucket()
    }

    fn key(&self) -> &ObjectKey {
        self.binding.key()
    }

    fn upload_id(&self) -> &UploadId {
        self.binding.upload_id()
    }

    fn part_number(&self) -> u32 {
        self.binding.part_number()
    }
}

fn percent_encode_location_path_segment(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(value.len());
    for &b in value.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                out.push('%');
                out.push(HEX[(b >> 4) as usize] as char);
                out.push(HEX[(b & 0x0F) as usize] as char);
            }
        }
    }
    out
}

fn percent_encode_location_key(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(value.len());
    for &b in value.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char);
            }
            _ => {
                out.push('%');
                out.push(HEX[(b >> 4) as usize] as char);
                out.push(HEX[(b & 0x0F) as usize] as char);
            }
        }
    }
    out
}

/// Convert an `S3Response` into a hyper-compatible HTTP response.
#[must_use]
pub fn s3_response_to_hyper(
    resp: S3Response,
    permit: Option<OwnedSemaphorePermit>,
    stream_read_chunk_size: usize,
    panic_on_500: bool,
    abort_on_500: bool,
    trace_meta: ResponseTraceMeta,
) -> http::Response<S3HyperBody> {
    fn fail_on_500_diagnostic(message: String, panic_on_500: bool, abort_on_500: bool) {
        if abort_on_500 {
            use std::io::Write as _;

            static ABORT_DIAGNOSTIC_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
            let _guard = ABORT_DIAGNOSTIC_LOCK
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut stderr = std::io::stderr().lock();
            let _ = writeln!(stderr, "{message}");
            let _ = stderr.flush();
            drop(stderr);
            if should_dump_panic_on_500_flight_recorder() {
                observability::dump_flight_recorder_to_stderr("abort-on-500");
            }
            std::process::abort();
        }
        if panic_on_500 {
            if should_dump_panic_on_500_flight_recorder() {
                observability::dump_flight_recorder_to_stderr("panic-on-500");
            }
            panic!("{message}");
        }
    }

    fn internal_error_response(
        reason: String,
        permit: Option<OwnedSemaphorePermit>,
        panic_on_500: bool,
        abort_on_500: bool,
        trace_meta: ResponseTraceMeta,
    ) -> http::Response<S3HyperBody> {
        let diagnostic_message = format!(
            "HTTP response conversion produced InternalError for {} {} (has_query={}): {reason}",
            trace_meta.method,
            trace_meta.path,
            trace_meta.query.has_query()
        );
        let wire_ids =
            WireResponseIds::new(trace_meta.context.request_id(), trace_meta.host_id.clone());
        let resp =
            S3Response::error_with_ids(&ServerError::InternalError { reason }, "", &wire_ids);
        let status = http::StatusCode::from_u16(resp.status_code)
            .unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR);
        let error_diagnostic = resp.error_diagnostic.clone();
        let mut trace =
            ResponseBodyTrace::new(trace_meta, resp.status_code, resp.body.len() as u64, false);
        if let Some(diagnostic) = &error_diagnostic {
            trace.emit_error_diagnostic(diagnostic);
        }
        if panic_on_500 || abort_on_500 {
            fail_on_500_diagnostic(diagnostic_message, panic_on_500, abort_on_500);
        }
        let inflight_requests_guard = permit
            .as_ref()
            .map(|_| observability::inflight_requests_guard());
        let mut response = http::Response::new(S3HyperBody::buffered(
            resp.body,
            permit,
            inflight_requests_guard,
            trace,
        ));
        *response.status_mut() = status;
        for (name, value) in resp.headers {
            if let (Ok(name), Ok(value)) = (
                http::header::HeaderName::from_bytes(name.as_bytes()),
                http::header::HeaderValue::from_str(&value),
            ) {
                response.headers_mut().insert(name, value);
            }
        }
        response.headers_mut().insert(
            http::header::HeaderName::from_static("x-amz-request-id"),
            http::header::HeaderValue::from_str(wire_ids.request_id())
                .expect("request id is a valid header value"),
        );
        response.headers_mut().insert(
            http::header::HeaderName::from_static("x-amz-id-2"),
            http::header::HeaderValue::from_str(wire_ids.host_id())
                .expect("host id is a valid header value"),
        );
        response
    }

    let body_len = if resp.stream.is_some() {
        resp.headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("Content-Length"))
            .and_then(|(_, value)| value.parse::<u64>().ok())
            .unwrap_or(0)
    } else {
        resp.body.len() as u64
    };
    if (panic_on_500 || abort_on_500) && resp.status_code == 500 {
        let body = String::from_utf8_lossy(&resp.body);
        let diagnostic_suffix = resp
            .error_diagnostic
            .as_ref()
            .map(|diagnostic| {
                let mut suffix = format!(
                    " cause_label={} cause_chain={}",
                    diagnostic.cause_label, diagnostic.cause_chain
                );
                if let Some(server_detail) = diagnostic.server_detail.as_deref() {
                    suffix.push_str(&format!(" server_detail={server_detail:?}"));
                }
                suffix
            })
            .unwrap_or_default();
        if let Some(diagnostic) = resp.error_diagnostic.as_ref() {
            let summary = observability::RequestSummary {
                method: &trace_meta.method,
                path: &trace_meta.path,
                query: trace_meta.query,
                status_code: resp.status_code,
                streaming: resp.stream.is_some(),
                body_len,
                bytes_sent: 0,
                lifetime_us: trace_meta.started_at.elapsed().as_micros(),
            };
            let _ = observability::emit_request_error(
                &trace_meta.context,
                TRACE_TARGET,
                summary,
                "response_body",
                diagnostic.error_code,
                diagnostic.cause_label,
            );
            let _ = observability::emit_http_500_cause_chain(
                &trace_meta.context,
                TRACE_TARGET,
                summary,
                diagnostic.cause_label,
                &diagnostic.cause_chain,
            );
            if let Some(server_detail) = diagnostic.server_detail.as_deref() {
                let _ = observability::emit_http_500_server_detail(
                    &trace_meta.context,
                    TRACE_TARGET,
                    summary,
                    server_detail,
                );
            }
        }
        fail_on_500_diagnostic(
            format!(
                "server produced HTTP 500 response for {} {} (has_query={}){}: {body}",
                trace_meta.method,
                trace_meta.path,
                trace_meta.query.has_query(),
                diagnostic_suffix
            ),
            panic_on_500,
            abort_on_500,
        );
    }
    let status = match http::StatusCode::from_u16(resp.status_code) {
        Ok(status) => status,
        Err(err) => {
            return internal_error_response(
                format!("invalid response status code {}: {err}", resp.status_code),
                permit,
                panic_on_500,
                abort_on_500,
                trace_meta,
            )
        }
    };
    let mut has_request_id_header = false;
    let mut has_host_id_header = false;
    let mut has_server_header = false;
    let mut validated_headers = Vec::with_capacity(resp.headers.len() + 2);
    for (name, value) in &resp.headers {
        let parsed_name = match http::header::HeaderName::from_bytes(name.as_bytes()) {
            Ok(name) => name,
            Err(err) => {
                return internal_error_response(
                    format!("invalid response header name {name:?}: {err}"),
                    permit,
                    panic_on_500,
                    abort_on_500,
                    trace_meta,
                )
            }
        };
        let parsed_value = match http::header::HeaderValue::from_str(value) {
            Ok(value) => value,
            Err(err) => {
                return internal_error_response(
                    format!("invalid response header value for {name}: {err}"),
                    permit,
                    panic_on_500,
                    abort_on_500,
                    trace_meta,
                )
            }
        };
        if parsed_name == http::header::HeaderName::from_static("x-amz-request-id") {
            has_request_id_header = true;
        }
        if parsed_name == http::header::HeaderName::from_static("x-amz-id-2") {
            has_host_id_header = true;
        }
        if parsed_name == http::header::SERVER {
            has_server_header = true;
        }
        validated_headers.push((parsed_name, parsed_value));
    }
    if !has_request_id_header {
        validated_headers.push((
            http::header::HeaderName::from_static("x-amz-request-id"),
            http::header::HeaderValue::from_str(trace_meta.context.request_id())
                .expect("request id is a valid header value"),
        ));
    }
    if !has_host_id_header {
        validated_headers.push((
            http::header::HeaderName::from_static("x-amz-id-2"),
            http::header::HeaderValue::from_str(&trace_meta.host_id)
                .expect("host id is a valid header value"),
        ));
    }
    if !has_server_header {
        validated_headers.push((
            http::header::SERVER,
            http::header::HeaderValue::from_static(AWS_SERVER_HEADER_VALUE),
        ));
    }
    let trace = ResponseBodyTrace::new(
        trace_meta,
        resp.status_code,
        body_len,
        resp.stream.is_some(),
    );
    let error_diagnostic = resp.error_diagnostic.clone();
    let inflight_requests_guard = permit
        .as_ref()
        .map(|_| observability::inflight_requests_guard());
    let mut body = match resp.stream {
        Some(stream) => S3HyperBody::streaming(
            stream,
            permit,
            inflight_requests_guard,
            stream_read_chunk_size,
            trace,
        ),
        None => S3HyperBody::buffered(resp.body, permit, inflight_requests_guard, trace),
    };
    if let Some(diagnostic) = &error_diagnostic {
        if let Some(trace) = body.trace.as_mut() {
            trace.emit_error_diagnostic(diagnostic);
        }
    }
    let mut response = http::Response::new(body);
    *response.status_mut() = status;
    for (name, value) in validated_headers {
        response.headers_mut().insert(name, value);
    }
    response
}

fn parse_max_keys<S: AsRef<str>>(raw: Option<S>) -> Result<u32, ServerError> {
    Ok(parse_u32_or_default(raw, 1000, "invalid max-keys")?.min(S3_MAX_LIST_KEYS))
}

fn parse_s3_list_limit<S: AsRef<str>>(
    raw: Option<S>,
    invalid_reason: &str,
) -> Result<u32, ServerError> {
    Ok(parse_u32_or_default(raw, 1000, invalid_reason)?.min(S3_MAX_LIST_KEYS))
}

fn parse_requested_max_keys<S: AsRef<str>>(raw: Option<S>) -> Result<u32, ServerError> {
    parse_u32_or_default(raw, 1000, "invalid max-keys")
}

/// Checksum algorithms and their corresponding `x-amz-checksum-*` value headers.
fn checksum_headers() -> impl Iterator<Item = (ChecksumAlgorithm, &'static str)> {
    ChecksumAlgorithm::ALL
        .into_iter()
        .map(|algorithm| (algorithm, algorithm.header_name()))
}

const SSE_C_ALGORITHM_HEADER: &str = "x-amz-server-side-encryption-customer-algorithm";
const SSE_C_KEY_HEADER: &str = "x-amz-server-side-encryption-customer-key";
const SSE_C_KEY_MD5_HEADER: &str = "x-amz-server-side-encryption-customer-key-md5";
const SSE_HEADER: &str = "x-amz-server-side-encryption";
const SSE_KMS_KEY_ID_HEADER: &str = "x-amz-server-side-encryption-aws-kms-key-id";
const SSE_C_COPY_SOURCE_ALGORITHM_HEADER: &str =
    "x-amz-copy-source-server-side-encryption-customer-algorithm";
const SSE_C_COPY_SOURCE_KEY_HEADER: &str = "x-amz-copy-source-server-side-encryption-customer-key";
const SSE_C_COPY_SOURCE_KEY_MD5_HEADER: &str =
    "x-amz-copy-source-server-side-encryption-customer-key-md5";

fn sse_customer_key_md5_mismatch_error() -> ServerError {
    ServerError::InvalidSseCustomerKeyMd5
}

fn invalid_sse_customer_algorithm_error(value: &str) -> ServerError {
    ServerError::InvalidEncryptionAlgorithmError {
        value: value.to_string(),
    }
}

fn parse_sse_customer_request_with_names(
    req: &S3Request,
    algorithm_header: &str,
    customer_key_header: &str,
    customer_key_md5_header: &str,
) -> Result<Option<SseCustomerRequest>, ServerError> {
    use base64::Engine;

    for header in [
        algorithm_header,
        customer_key_header,
        customer_key_md5_header,
    ] {
        if header_count(req, header) > 1 {
            return Err(ServerError::InvalidRequest {
                reason: format!("duplicate header: {header}"),
            });
        }
    }

    let algorithm = req.header(algorithm_header);
    let customer_key = req.header(customer_key_header);
    let customer_key_md5 = req.header(customer_key_md5_header);

    let Some((algorithm, customer_key, customer_key_md5)) =
        require_complete_sse_customer_fields(algorithm, customer_key, customer_key_md5)?
    else {
        return Ok(None);
    };
    require_secure_transport_for_sse_c(req.transport_security)?;
    if algorithm != SSE_CUSTOMER_ALGORITHM {
        return Err(invalid_sse_customer_algorithm_error(algorithm));
    }

    let decoded_key = base64::engine::general_purpose::STANDARD
        .decode(customer_key)
        .map_err(|_| ServerError::InvalidArgument {
            reason: "invalid base64 in SSE-C key".to_string(),
        })?;
    let customer_key_bytes: [u8; SSE_C_CUSTOMER_KEY_LEN] =
        decoded_key
            .try_into()
            .map_err(|_| ServerError::InvalidArgument {
                reason: format!("SSE-C key must decode to exactly {SSE_C_CUSTOMER_KEY_LEN} bytes"),
            })?;

    let decoded_md5 = base64::engine::general_purpose::STANDARD
        .decode(customer_key_md5)
        .map_err(|_| ServerError::InvalidDigest)?;
    let claimed_md5: [u8; 16] = decoded_md5
        .try_into()
        .map_err(|_| ServerError::InvalidDigest)?;

    let actual_md5 = md5_legacy::Md5::digest(customer_key_bytes);
    let mut actual_md5_bytes = [0u8; 16];
    actual_md5_bytes.copy_from_slice(actual_md5.as_ref());
    if claimed_md5 != actual_md5_bytes {
        return Err(sse_customer_key_md5_mismatch_error());
    }

    Ok(Some(SseCustomerRequest::new(
        customer_key_bytes,
        base64::engine::general_purpose::STANDARD.encode(actual_md5_bytes),
    )))
}

fn parse_sse_customer_request(req: &S3Request) -> Result<Option<SseCustomerRequest>, ServerError> {
    parse_sse_customer_request_with_names(
        req,
        SSE_C_ALGORITHM_HEADER,
        SSE_C_KEY_HEADER,
        SSE_C_KEY_MD5_HEADER,
    )
}

fn parse_form_field_once<'a>(
    form_fields: &'a [(String, String)],
    name: &str,
) -> Result<Option<&'a str>, ServerError> {
    let mut matches = form_fields
        .iter()
        .filter(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str());
    let first = matches.next();
    if matches.next().is_some() {
        return Err(ServerError::InvalidRequest {
            reason: format!("duplicate form field: {name}"),
        });
    }
    Ok(first)
}

fn post_checksum_claim_from_fields(
    form_fields: &[(String, String)],
) -> Result<Option<ChecksumClaim>, ServerError> {
    let declared_algorithm = parse_form_field_once(form_fields, "x-amz-checksum-algorithm")?;
    let mut claim: Option<ChecksumClaim> = None;

    for (algorithm, header) in checksum_headers() {
        let Some(value) = parse_form_field_once(form_fields, header)? else {
            continue;
        };
        if claim.is_some() {
            return Err(ServerError::InvalidRequest {
                reason: "only one checksum field may be specified".to_string(),
            });
        }
        if let Some(declared) = declared_algorithm {
            let algorithm_name = algorithm.as_str();
            if !declared.eq_ignore_ascii_case(algorithm_name) {
                return Err(ServerError::InvalidRequest {
                    reason: format!(
                        "checksum algorithm mismatch: field says {declared} but got {algorithm_name}"
                    ),
                });
            }
        }
        claim = Some(ChecksumClaim::from_base64(algorithm, value)?);
    }

    Ok(claim)
}

fn parse_sse_customer_form_fields(
    transport_security: TransportSecurity,
    form_fields: &[(String, String)],
) -> Result<Option<SseCustomerRequest>, ServerError> {
    use base64::Engine;

    let algorithm = parse_form_field_once(form_fields, SSE_C_ALGORITHM_HEADER)?;
    let customer_key = parse_form_field_once(form_fields, SSE_C_KEY_HEADER)?;
    let customer_key_md5 = parse_form_field_once(form_fields, SSE_C_KEY_MD5_HEADER)?;

    let Some((algorithm, customer_key, customer_key_md5)) =
        require_complete_sse_customer_fields(algorithm, customer_key, customer_key_md5)?
    else {
        return Ok(None);
    };
    require_secure_transport_for_sse_c(transport_security)?;
    if algorithm != SSE_CUSTOMER_ALGORITHM {
        return Err(invalid_sse_customer_algorithm_error(algorithm));
    }

    let decoded_key = base64::engine::general_purpose::STANDARD
        .decode(customer_key)
        .map_err(|_| ServerError::InvalidArgument {
            reason: "invalid base64 in SSE-C key".to_string(),
        })?;
    let customer_key_bytes: [u8; SSE_C_CUSTOMER_KEY_LEN] =
        decoded_key
            .try_into()
            .map_err(|_| ServerError::InvalidArgument {
                reason: format!("SSE-C key must decode to exactly {SSE_C_CUSTOMER_KEY_LEN} bytes"),
            })?;

    let decoded_md5 = base64::engine::general_purpose::STANDARD
        .decode(customer_key_md5)
        .map_err(|_| ServerError::InvalidDigest)?;
    let claimed_md5: [u8; 16] = decoded_md5
        .try_into()
        .map_err(|_| ServerError::InvalidDigest)?;

    let actual_md5 = md5_legacy::Md5::digest(customer_key_bytes);
    let mut actual_md5_bytes = [0u8; 16];
    actual_md5_bytes.copy_from_slice(actual_md5.as_ref());
    if claimed_md5 != actual_md5_bytes {
        return Err(sse_customer_key_md5_mismatch_error());
    }

    Ok(Some(SseCustomerRequest::new(
        customer_key_bytes,
        base64::engine::general_purpose::STANDARD.encode(actual_md5_bytes),
    )))
}

fn parse_sse_customer_copy_source_request(
    req: &S3Request,
) -> Result<Option<SseCustomerRequest>, ServerError> {
    parse_sse_customer_request_with_names(
        req,
        SSE_C_COPY_SOURCE_ALGORITHM_HEADER,
        SSE_C_COPY_SOURCE_KEY_HEADER,
        SSE_C_COPY_SOURCE_KEY_MD5_HEADER,
    )
}

fn require_complete_sse_customer_fields<'a>(
    algorithm: Option<&'a str>,
    customer_key: Option<&'a str>,
    customer_key_md5: Option<&'a str>,
) -> Result<Option<(&'a str, &'a str, &'a str)>, ServerError> {
    if algorithm.is_none() && customer_key.is_none() && customer_key_md5.is_none() {
        return Ok(None);
    }
    if algorithm.is_none() {
        return Err(ServerError::MissingSseCustomerAlgorithm);
    }
    if customer_key.is_none() {
        return Err(ServerError::MissingSseCustomerKey);
    }
    if customer_key_md5.is_none() {
        return Err(ServerError::MissingSseCustomerKeyMd5);
    }
    Ok(Some((
        algorithm.expect("checked above"),
        customer_key.expect("checked above"),
        customer_key_md5.expect("checked above"),
    )))
}

fn require_secure_transport_for_sse_c(
    transport_security: TransportSecurity,
) -> Result<(), ServerError> {
    if transport_security.is_secure() {
        Ok(())
    } else {
        Err(ServerError::InvalidArgument {
            reason: "Requests specifying Server Side Encryption with Customer provided keys must be made over a secure connection.".to_string(),
        })
    }
}

fn apply_sse_customer_write_response_headers(
    resp: &mut S3Response,
    sse_customer: Option<&SseCustomerRequest>,
) {
    let Some(sse_customer) = sse_customer else {
        return;
    };
    resp.headers.push((
        SSE_C_ALGORITHM_HEADER.to_string(),
        SSE_CUSTOMER_ALGORITHM.to_string(),
    ));
    resp.headers.push((
        SSE_C_KEY_MD5_HEADER.to_string(),
        sse_customer.response_headers().key_md5_b64,
    ));
}

fn parse_managed_encryption_request(
    req: &S3Request,
    sse_customer_present: bool,
) -> Result<Option<ManagedEncryptionAlgorithm>, ServerError> {
    for header in [SSE_HEADER, SSE_KMS_KEY_ID_HEADER] {
        if header_count(req, header) > 1 {
            return Err(ServerError::InvalidRequest {
                reason: format!("duplicate header: {header}"),
            });
        }
    }

    let server_side_encryption = req.header(SSE_HEADER);
    let kms_key_id = req.header(SSE_KMS_KEY_ID_HEADER);

    if sse_customer_present && (server_side_encryption.is_some() || kms_key_id.is_some()) {
        return Err(ServerError::InvalidArgument {
            reason: "x-amz-server-side-encryption may not be used with SSE-C headers".to_string(),
        });
    }

    match (server_side_encryption, kms_key_id) {
        (None, None) => Ok(None),
        (None, Some(_)) => Err(ServerError::InvalidArgument {
            reason:
                "x-amz-server-side-encryption-aws-kms-key-id requires x-amz-server-side-encryption: aws:kms"
                    .to_string(),
        }),
        (Some("AES256"), None) => Ok(Some(ManagedEncryptionAlgorithm::Aes256)),
        (Some("AES256"), Some(_)) => Err(ServerError::InvalidArgument {
            reason:
                "x-amz-server-side-encryption-aws-kms-key-id may not be used with x-amz-server-side-encryption: AES256"
                    .to_string(),
        }),
        (Some("aws:kms"), _) => Err(ServerError::NotImplemented {
            feature: "SSE-KMS object encryption".to_string(),
        }),
        (Some(other), _) => Err(ServerError::InvalidArgument {
            reason: format!("invalid x-amz-server-side-encryption value: {other}"),
        }),
    }
}

fn parse_managed_encryption_form_fields(
    form_fields: &[(String, String)],
    sse_customer_present: bool,
) -> Result<Option<ManagedEncryptionAlgorithm>, ServerError> {
    let server_side_encryption = parse_form_field_once(form_fields, SSE_HEADER)?;
    let kms_key_id = parse_form_field_once(form_fields, SSE_KMS_KEY_ID_HEADER)?;

    if sse_customer_present && (server_side_encryption.is_some() || kms_key_id.is_some()) {
        return Err(ServerError::InvalidArgument {
            reason: "x-amz-server-side-encryption may not be used with SSE-C headers".to_string(),
        });
    }

    match (server_side_encryption, kms_key_id) {
        (None, None) => Ok(None),
        (None, Some(_)) => Err(ServerError::InvalidArgument {
            reason:
                "x-amz-server-side-encryption-aws-kms-key-id requires x-amz-server-side-encryption: aws:kms"
                    .to_string(),
        }),
        (Some("AES256"), None) => Ok(Some(ManagedEncryptionAlgorithm::Aes256)),
        (Some("AES256"), Some(_)) => Err(ServerError::InvalidArgument {
            reason:
                "x-amz-server-side-encryption-aws-kms-key-id may not be used with x-amz-server-side-encryption: AES256"
                    .to_string(),
        }),
        (Some("aws:kms"), _) => Err(ServerError::NotImplemented {
            feature: "SSE-KMS POST object encryption".to_string(),
        }),
        (Some(other), _) => Err(ServerError::InvalidArgument {
            reason: format!("invalid x-amz-server-side-encryption value: {other}"),
        }),
    }
}

fn reject_managed_encryption_read_headers(
    req: &S3Request,
    context: ManagedEncryptionReadHeaderContext,
) -> Result<(), ServerError> {
    for header in [SSE_HEADER, SSE_KMS_KEY_ID_HEADER] {
        if header_count(req, header) > 1 {
            return Err(ServerError::InvalidRequest {
                reason: format!("duplicate header: {header}"),
            });
        }
    }
    if let Some(value) = req.header(SSE_HEADER) {
        return Err(ServerError::InvalidManagedEncryptionReadHeader {
            context,
            header: ManagedEncryptionReadHeader::ServerSideEncryption {
                value: value.to_string(),
            },
        });
    }
    if req.header(SSE_KMS_KEY_ID_HEADER).is_some() {
        return Err(ServerError::InvalidManagedEncryptionReadHeader {
            context,
            header: ManagedEncryptionReadHeader::KmsKeyId,
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContentMd5Claim([u8; 16]);

impl ContentMd5Claim {
    fn from_request(req: &S3Request) -> Result<Option<Self>, ServerError> {
        use base64::Engine;

        if header_count(req, "content-md5") > 1 {
            return Err(ServerError::InvalidDigest);
        }
        let Some(header) = req.header("content-md5") else {
            return Ok(None);
        };

        let decoded = base64::engine::general_purpose::STANDARD
            .decode(header)
            .map_err(|_| ServerError::InvalidDigest)?;
        let bytes: [u8; 16] = decoded.try_into().map_err(|_| ServerError::InvalidDigest)?;
        Ok(Some(Self(bytes)))
    }

    fn verify(self, actual: &[u8; 16]) -> Result<(), ServerError> {
        if self.0 == *actual {
            Ok(())
        } else {
            Err(ServerError::BadDigest)
        }
    }
}

fn validate_content_md5(req: &S3Request) -> Result<(), ServerError> {
    let Some(claim) = ContentMd5Claim::from_request(req)? else {
        return Ok(());
    };
    let actual = md5_legacy::Md5::digest(&req.body);
    let mut actual_bytes = [0u8; 16];
    actual_bytes.copy_from_slice(actual.as_ref());
    claim.verify(&actual_bytes)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequestChecksumRequirement {
    Optional,
    ContentMd5OrChecksumHeader,
    PutBucketLifecycle,
    PutObjectWithObjectLock,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct RequestChecksumState {
    has_content_md5: bool,
    has_checksum_header: bool,
    has_trailing_checksum: bool,
}

fn trailing_checksum_algorithm(req: &S3Request) -> Option<ChecksumAlgorithm> {
    req.header("x-amz-trailer").and_then(|value| {
        value
            .split(',')
            .find_map(|name| checksum_algo_from_header(name.trim()))
    })
}

fn parse_checksum_algorithm_value(value: &str) -> Option<ChecksumAlgorithm> {
    ChecksumAlgorithm::ALL
        .into_iter()
        .find(|algorithm| value.eq_ignore_ascii_case(algorithm.as_str()))
}

fn parse_checksum_algorithm_header_value(value: &str) -> Result<ChecksumAlgorithm, ServerError> {
    parse_checksum_algorithm_value(value).ok_or_else(ServerError::unsupported_checksum_algorithm)
}

fn validate_sdk_checksum_algorithm(
    req: &S3Request,
    checksum_header_algorithm: Option<ChecksumAlgorithm>,
    trailing_checksum_algorithm: Option<ChecksumAlgorithm>,
) -> Result<(), ServerError> {
    let sdk_header = "x-amz-sdk-checksum-algorithm";
    if header_count(req, sdk_header) > 1 {
        return Err(ServerError::InvalidRequest {
            reason: format!("duplicate header: {sdk_header}"),
        });
    }
    let Some(declared) = req.header(sdk_header) else {
        return Ok(());
    };
    let Some(actual) = checksum_header_algorithm.or(trailing_checksum_algorithm) else {
        return Err(ServerError::InvalidRequestHostId {
            reason:
                "x-amz-sdk-checksum-algorithm specified, but no corresponding x-amz-checksum-* or x-amz-trailer headers were found."
                    .to_string(),
        });
    };
    let Some(declared_algorithm) = parse_checksum_algorithm_value(declared) else {
        return Err(ServerError::InvalidRequestHostId {
            reason: "Value for x-amz-sdk-checksum-algorithm header is invalid.".to_string(),
        });
    };
    if declared_algorithm != actual {
        return Err(ServerError::InvalidRequestHostId {
            reason: "Value for x-amz-sdk-checksum-algorithm header is invalid.".to_string(),
        });
    }
    Ok(())
}

fn validate_request_checksum_headers(
    req: &S3Request,
    verify_body: bool,
    allow_trailing_checksum: bool,
    validate_sdk_algorithm: bool,
) -> Result<RequestChecksumState, ServerError> {
    let has_content_md5 = req.header("content-md5").is_some();
    let checksum_header = extract_encoded_checksum_header(req)?;
    let has_checksum_header = checksum_header.is_some();
    let trailing_checksum_algorithm = allow_trailing_checksum
        .then(|| trailing_checksum_algorithm(req))
        .flatten();
    let has_trailing_checksum = trailing_checksum_algorithm.is_some();
    if validate_sdk_algorithm {
        validate_sdk_checksum_algorithm(
            req,
            checksum_header
                .as_ref()
                .map(EncodedChecksumClaim::algorithm),
            trailing_checksum_algorithm,
        )?;
    }

    if verify_body {
        validate_content_md5(req)?;
    } else {
        ContentMd5Claim::from_request(req)?;
    }
    validate_checksum_headers(req, verify_body)?;

    Ok(RequestChecksumState {
        has_content_md5,
        has_checksum_header,
        has_trailing_checksum,
    })
}

fn require_request_checksum(
    req: &S3Request,
    requirement: RequestChecksumRequirement,
) -> Result<RequestChecksumState, ServerError> {
    let state = validate_request_checksum_headers(req, true, false, true)?;
    match requirement {
        RequestChecksumRequirement::Optional => Ok(state),
        RequestChecksumRequirement::ContentMd5OrChecksumHeader => {
            if !state.has_content_md5 && !state.has_checksum_header {
                return Err(ServerError::InvalidRequest {
                    reason:
                        "Missing required header for this request: Content-MD5 OR x-amz-checksum-*"
                            .to_string(),
                });
            }
            Ok(state)
        }
        RequestChecksumRequirement::PutBucketLifecycle => {
            if !state.has_content_md5 && !state.has_checksum_header {
                return Err(ServerError::InvalidRequest {
                    reason: "Missing required header for this request: Content-MD5".to_string(),
                });
            }
            Ok(state)
        }
        RequestChecksumRequirement::PutObjectWithObjectLock => {
            if !state.has_content_md5 && !state.has_checksum_header {
                return Err(ServerError::InvalidRequest {
                    reason: "Content-MD5 OR x-amz-checksum- HTTP header is required for Put Object requests with Object Lock parameters".to_string(),
                });
            }
            Ok(state)
        }
    }
}

/// Map a checksum header name (e.g. `x-amz-checksum-crc32`) to its
/// `ChecksumAlgorithm`. Returns `None` for unrecognized headers.
fn checksum_algo_from_header(header: &str) -> Option<ChecksumAlgorithm> {
    ChecksumAlgorithm::from_header_name(header)
}

/// Validate checksum headers on `PutObject`.
///
/// Enforces that at most one checksum header is present, validates base64
/// format/length, and verifies the provided checksum against the body when
/// requested.
///
/// When `verify_body` is true, also computes the actual checksum from
/// `req.body` and returns `BadDigest` on mismatch. Pass `false` for
/// streaming paths where the body is not yet available.
fn validate_checksum_headers(req: &S3Request, verify_body: bool) -> Result<(), ServerError> {
    use base64::Engine;

    let Some(claim) = extract_encoded_checksum_header(req)? else {
        return Ok(());
    };

    let algorithm = claim.algorithm();
    let header = algorithm.header_name();
    ChecksumClaim::from_base64(algorithm, claim.encoded_value()).map_err(|_| {
        ServerError::InvalidRequest {
            reason: format!("Value for {header} header is invalid."),
        }
    })?;

    if verify_body {
        let actual = checksum::compute_checksum(algorithm, &req.body);
        let actual_b64 = base64::engine::general_purpose::STANDARD.encode(actual.bytes());
        if claim.encoded_value() != actual_b64 {
            return Err(ServerError::ChecksumDigestMismatch {
                algorithm: algorithm.as_str().to_string(),
            });
        }
    }
    Ok(())
}

/// Count how many times a header name appears in the request.
fn header_count(req: &S3Request, name: &str) -> usize {
    req.header_count(name)
}

fn first_duplicate_header_value<'a>(req: &'a S3Request, name: &str) -> Option<&'a str> {
    req.headers.get_all(name).iter().nth(1).map(|value| {
        std::str::from_utf8(value.as_bytes()).expect("S3Request stores only validated UTF-8")
    })
}

/// Extract a checksum header as an encoded typed claim.
///
/// Shared validation for all checksum-header consumers. Rejects if:
/// - multiple distinct checksum value headers are present (e.g. crc32 + sha256)
/// - the same checksum header appears more than once
fn extract_encoded_checksum_header(
    req: &S3Request,
) -> Result<Option<EncodedChecksumClaim>, ServerError> {
    let mut found: Option<EncodedChecksumClaim> = None;
    for (algo, header) in checksum_headers() {
        if let Some(claimed) = req.header(header) {
            if found.is_some() {
                return Err(ServerError::InvalidRequest {
                    reason: "only one checksum header may be specified".into(),
                });
            }
            // Reject duplicate same-name headers (req.header returns only
            // the first, so a second with a different value would be silent).
            if header_count(req, header) > 1 {
                return Err(ServerError::DuplicateChecksumHeader {
                    header: header.to_string(),
                    value: first_duplicate_header_value(req, header)
                        .unwrap_or(claimed)
                        .to_string(),
                });
            }
            found = Some(EncodedChecksumClaim::new(algo, claimed.to_string()));
        }
    }
    Ok(found)
}

/// Extract a claimed checksum from request headers, decoded and validated.
///
/// Used by `UploadPart` and streaming paths where the value is always plain base64.
fn extract_checksum_header(req: &S3Request) -> Result<Option<ChecksumClaim>, ServerError> {
    match extract_encoded_checksum_header(req)? {
        Some(claim) => Ok(Some(ChecksumClaim::from_base64(
            claim.algorithm(),
            claim.encoded_value(),
        )?)),
        None => Ok(None),
    }
}

fn apply_response_overrides(resp: &mut S3Response, req: &S3Request) {
    fn sanitize_override_value(value: &str) -> std::borrow::Cow<'_, str> {
        if !value.contains(['\r', '\n']) {
            return std::borrow::Cow::Borrowed(value);
        }
        std::borrow::Cow::Owned(
            value
                .chars()
                .map(|ch| match ch {
                    '\r' | '\n' => ' ',
                    _ => ch,
                })
                .collect(),
        )
    }

    fn validated_override_value(header_name: &str, value: &str) -> Option<String> {
        let sanitized = sanitize_override_value(value);
        if http::header::HeaderValue::from_str(sanitized.as_ref()).is_err() {
            return None;
        }
        match header_name {
            "Content-Type"
            | "Content-Disposition"
            | "Content-Encoding"
            | "Content-Language"
            | "Cache-Control"
            | "Expires" => Some(sanitized.into_owned()),
            _ => None,
        }
    }

    for &(param, header_name) in RESPONSE_OVERRIDE_HEADERS {
        if let Some(value) = req.query_param_lossy(param) {
            let Some(value) = validated_override_value(header_name, value.as_ref()) else {
                continue;
            };
            resp.headers
                .retain(|(k, _)| !k.eq_ignore_ascii_case(header_name));
            resp.headers.push((header_name.to_string(), value));
        }
    }
}

const RESPONSE_OVERRIDE_HEADERS: &[(&str, &str)] = &[
    ("response-content-type", "Content-Type"),
    ("response-content-disposition", "Content-Disposition"),
    ("response-content-encoding", "Content-Encoding"),
    ("response-content-language", "Content-Language"),
    ("response-cache-control", "Cache-Control"),
    ("response-expires", "Expires"),
];

fn has_response_override_params(req: &S3Request) -> bool {
    RESPONSE_OVERRIDE_HEADERS
        .iter()
        .any(|(param, _)| req.query_param_lossy(param).is_some())
}

fn reject_anonymous_response_overrides(
    req: &S3Request,
    auth: &AuthContext,
) -> Result<(), ServerError> {
    if auth.mode == AuthMode::Anonymous && has_response_override_params(req) {
        return Err(ServerError::InvalidRequest {
            reason: "Request specific response headers cannot be used for anonymous GET requests."
                .to_string(),
        });
    }
    Ok(())
}

fn parse_create_bucket_acl(
    req: &S3Request,
) -> Result<crate::coordinator::CreateBucketAcl, ServerError> {
    let acl_grants = parse_acl_grants_headers_with_options(req, true)?;
    match req.header("x-amz-acl") {
        Some(_) if acl_grants.is_some() => Err(ServerError::InvalidArgument {
            reason: "x-amz-acl cannot be combined with x-amz-grant-* headers".to_string(),
        }),
        Some("private") => Ok(crate::coordinator::CreateBucketAcl::Canned(
            crate::coordinator::BucketAcl::Private,
        )),
        Some("public-read") => Ok(crate::coordinator::CreateBucketAcl::Canned(
            crate::coordinator::BucketAcl::PublicRead,
        )),
        Some("public-read-write") => Ok(crate::coordinator::CreateBucketAcl::Canned(
            crate::coordinator::BucketAcl::PublicReadWrite,
        )),
        Some("authenticated-read") => Ok(crate::coordinator::CreateBucketAcl::Canned(
            crate::coordinator::BucketAcl::AuthenticatedRead,
        )),
        Some(other) => Err(ServerError::InvalidArgument {
            reason: format!("unsupported x-amz-acl value: {other}"),
        }),
        None => Ok(acl_grants
            .map(crate::coordinator::CreateBucketAcl::Grants)
            .unwrap_or_default()),
    }
}

const ACL_GRANT_HEADERS: [(&str, s3_types::AclPermission); 5] = [
    ("x-amz-grant-read", s3_types::AclPermission::Read),
    ("x-amz-grant-write", s3_types::AclPermission::Write),
    ("x-amz-grant-read-acp", s3_types::AclPermission::ReadAcp),
    ("x-amz-grant-write-acp", s3_types::AclPermission::WriteAcp),
    (
        "x-amz-grant-full-control",
        s3_types::AclPermission::FullControl,
    ),
];

fn has_acl_grant_headers(req: &S3Request) -> bool {
    ACL_GRANT_HEADERS
        .iter()
        .any(|(name, _)| req.header_count(name) > 0)
}

fn parse_acl_grant_header_value(
    value: &str,
    permission: s3_types::AclPermission,
    allow_unquoted_values: bool,
) -> Result<Vec<s3_types::AclGrant>, ServerError> {
    let mut grants = Vec::new();
    let mut remaining = value.trim();
    if remaining.is_empty() {
        return Err(ServerError::InvalidArgument {
            reason: "empty ACL grant header value".to_string(),
        });
    }

    while !remaining.is_empty() {
        let (grantee_kind, rest) =
            remaining
                .split_once('=')
                .ok_or_else(|| ServerError::InvalidArgument {
                    reason: format!("invalid ACL grant header entry: {remaining}"),
                })?;
        let grantee_kind = grantee_kind.trim();
        let rest = rest.trim_start();
        let (grantee_value, next) = if let Some(quoted) = rest.strip_prefix('"') {
            let quote_end = quoted
                .find('"')
                .ok_or_else(|| ServerError::InvalidArgument {
                    reason: format!("invalid ACL grant header entry: {remaining}"),
                })?;
            (&quoted[..quote_end], &quoted[quote_end + 1..])
        } else if allow_unquoted_values {
            let value_end = rest.find(',').unwrap_or(rest.len());
            (rest[..value_end].trim_end(), &rest[value_end..])
        } else {
            return Err(ServerError::InvalidArgument {
                reason: format!("invalid ACL grant header entry: {remaining}"),
            });
        };
        if grantee_value.is_empty() {
            return Err(ServerError::InvalidArgument {
                reason: format!("invalid ACL grant header entry: {remaining}"),
            });
        }
        let grantee = match grantee_kind {
            "id" => s3_types::AclGrantee::CanonicalUser(
                s3_types::CanonicalUserId::new(grantee_value).ok_or_else(|| {
                    ServerError::InvalidArgument {
                        reason: "invalid canonical user ID in ACL grant header".to_string(),
                    }
                })?,
            ),
            "uri" => s3_types::AclGrantee::parse_group_uri(grantee_value).ok_or_else(|| {
                ServerError::InvalidArgument {
                    reason: format!("unsupported ACL group URI: {grantee_value}"),
                }
            })?,
            other => {
                return Err(ServerError::InvalidArgument {
                    reason: format!("unsupported ACL grant header grantee: {other}"),
                });
            }
        };
        grants.push(s3_types::AclGrant::new(grantee, permission));

        remaining = next.trim_start();
        if remaining.is_empty() {
            break;
        }
        remaining = remaining
            .strip_prefix(',')
            .ok_or_else(|| ServerError::InvalidArgument {
                reason: format!("invalid ACL grant header entry: {remaining}"),
            })?;
        remaining = remaining.trim_start();
        if remaining.is_empty() {
            return Err(ServerError::InvalidArgument {
                reason: format!("invalid ACL grant header entry: {value}"),
            });
        }
    }

    Ok(grants)
}

fn parse_acl_grants_headers_with_options(
    req: &S3Request,
    allow_unquoted_values: bool,
) -> Result<Option<s3_types::AclGrants>, ServerError> {
    if !has_acl_grant_headers(req) {
        return Ok(None);
    }

    let mut grants = Vec::new();
    for (header_name, permission) in ACL_GRANT_HEADERS {
        for raw_value in req.headers.get_all(header_name).iter() {
            let value = std::str::from_utf8(raw_value.as_bytes()).map_err(|_| {
                ServerError::InvalidArgument {
                    reason: format!("invalid UTF-8 in ACL grant header {header_name}"),
                }
            })?;
            grants.extend(parse_acl_grant_header_value(
                value,
                permission,
                allow_unquoted_values,
            )?);
        }
    }
    Ok(Some(s3_types::AclGrants::new(grants)))
}

fn parse_acl_grants_headers(req: &S3Request) -> Result<Option<s3_types::AclGrants>, ServerError> {
    parse_acl_grants_headers_with_options(req, false)
}

fn parse_acl_grants(req: &S3Request) -> Result<s3_types::AclGrants, ServerError> {
    let header_grants = parse_acl_grants_headers(req)?;
    if !req.body.is_empty() {
        if header_grants.is_some() {
            return Err(ServerError::InvalidArgument {
                reason: "ACL XML body cannot be combined with x-amz-grant-* headers".to_string(),
            });
        }
        return xml::parse_acl_xml(&req.body);
    }
    if let Some(grants) = header_grants {
        return Ok(grants);
    }
    Err(ServerError::InvalidArgument {
        reason: "missing ACL XML body".to_string(),
    })
}

fn put_object_write_acl_from_components<'a>(
    acl_header: Option<&'a str>,
    acl_grants: Option<&s3_types::AclGrants>,
) -> crate::coordinator::PutObjectWriteAcl<'a> {
    match (acl_header, acl_grants) {
        (Some(header), None) => parse_put_object_acl(Some(header)).into(),
        (None, Some(grants)) => crate::coordinator::PutObjectWriteAcl::Grants(grants.clone()),
        (None, None) => crate::coordinator::PutObjectWriteAcl::None,
        (Some(_), Some(_)) => unreachable!("validated before constructing object write ACL"),
    }
}

fn parse_put_object_write_acl(
    req: &S3Request,
) -> Result<crate::coordinator::PutObjectWriteAcl<'_>, ServerError> {
    let acl_grants = parse_acl_grants_headers(req)?;
    if req.header("x-amz-acl").is_some() && acl_grants.is_some() {
        return Err(ServerError::InvalidArgument {
            reason: "x-amz-acl cannot be combined with x-amz-grant-* headers".to_string(),
        });
    }
    Ok(put_object_write_acl_from_components(
        req.header("x-amz-acl"),
        acl_grants.as_ref(),
    ))
}

fn put_object_policy_context_from_request<'a>(
    req: &'a S3Request,
    tags_xml: Option<&'a str>,
    copy_source: Option<&'a str>,
    metadata_directive: Option<&'a str>,
    canned_acl: Option<&'a str>,
    managed_encryption: Option<ManagedEncryptionAlgorithm>,
) -> crate::coordinator::PutObjectPolicyContext<'a> {
    put_object_policy_context_from_request_fields(PutObjectPolicyContextFields {
        tags_xml,
        copy_source,
        metadata_directive,
        canned_acl,
        website_redirect_location: req.header(WEBSITE_REDIRECT_LOCATION_HEADER_NAME),
        managed_encryption,
        sse_customer_algorithm: req.header(SSE_C_ALGORITHM_HEADER),
        grants: PutObjectGrantHeaders {
            grant_read: req.header("x-amz-grant-read"),
            grant_write: req.header("x-amz-grant-write"),
            grant_read_acp: req.header("x-amz-grant-read-acp"),
            grant_write_acp: req.header("x-amz-grant-write-acp"),
            grant_full_control: req.header("x-amz-grant-full-control"),
        },
        conditions: PutObjectConditionalHeaders {
            if_match: req.header("if-match").map(if_match_header_entity_tag_value),
            if_none_match: req.header("if-none-match"),
        },
    })
}

fn if_match_header_entity_tag_value(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(value)
}

#[derive(Clone, Copy, Default)]
struct PutObjectGrantHeaders<'a> {
    grant_read: Option<&'a str>,
    grant_write: Option<&'a str>,
    grant_read_acp: Option<&'a str>,
    grant_write_acp: Option<&'a str>,
    grant_full_control: Option<&'a str>,
}

#[derive(Clone, Copy, Default)]
struct PutObjectConditionalHeaders<'a> {
    if_match: Option<&'a str>,
    if_none_match: Option<&'a str>,
}

#[derive(Clone, Copy, Default)]
struct PutObjectPolicyContextFields<'a> {
    tags_xml: Option<&'a str>,
    copy_source: Option<&'a str>,
    metadata_directive: Option<&'a str>,
    canned_acl: Option<&'a str>,
    website_redirect_location: Option<&'a str>,
    managed_encryption: Option<ManagedEncryptionAlgorithm>,
    sse_customer_algorithm: Option<&'a str>,
    grants: PutObjectGrantHeaders<'a>,
    conditions: PutObjectConditionalHeaders<'a>,
}

fn put_object_policy_context_from_request_fields<'a>(
    fields: PutObjectPolicyContextFields<'a>,
) -> crate::coordinator::PutObjectPolicyContext<'a> {
    crate::coordinator::PutObjectPolicyContext::new(
        fields.copy_source,
        fields.metadata_directive,
        fields.canned_acl,
    )
    .with_website_redirect_location(fields.website_redirect_location)
    .with_managed_encryption(fields.managed_encryption)
    .with_sse_customer_algorithm(fields.sse_customer_algorithm)
    .with_request_object_tags_xml(fields.tags_xml)
    .with_acl_grant_headers(
        fields.grants.grant_read,
        fields.grants.grant_write,
        fields.grants.grant_read_acp,
        fields.grants.grant_write_acp,
        fields.grants.grant_full_control,
    )
    .with_if_match(fields.conditions.if_match)
    .with_if_none_match(fields.conditions.if_none_match)
    .with_object_creation_operation(true)
}

fn parse_bucket_ownership(
    value: Option<&str>,
) -> Result<crate::coordinator::BucketObjectOwnership, ServerError> {
    match value {
        None => Ok(crate::coordinator::BucketObjectOwnership::BucketOwnerEnforced),
        Some("BucketOwnerEnforced") => {
            Ok(crate::coordinator::BucketObjectOwnership::BucketOwnerEnforced)
        }
        Some("BucketOwnerPreferred") => {
            Ok(crate::coordinator::BucketObjectOwnership::BucketOwnerPreferred)
        }
        Some("ObjectWriter") => Ok(crate::coordinator::BucketObjectOwnership::ObjectWriter),
        Some(other) => Err(ServerError::InvalidArgument {
            reason: format!("invalid x-amz-object-ownership value: {other}"),
        }),
    }
}

fn parse_bucket_object_lock_enabled(value: Option<&str>) -> Result<bool, ServerError> {
    match value {
        None => Ok(false),
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        Some(other) => Err(ServerError::InvalidArgument {
            reason: format!("invalid x-amz-bucket-object-lock-enabled value: {other}"),
        }),
    }
}

fn parse_confirm_remove_self_bucket_access(value: Option<&str>) -> Result<bool, ServerError> {
    match value {
        None => Ok(false),
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        Some(other) => Err(ServerError::InvalidArgument {
            reason: format!("invalid x-amz-confirm-remove-self-bucket-access value: {other}"),
        }),
    }
}

fn parse_object_lock_headers(req: &S3Request) -> Result<ObjectLockState, ServerError> {
    const MODE_HEADER: &str = "x-amz-object-lock-mode";
    const RETAIN_UNTIL_HEADER: &str = "x-amz-object-lock-retain-until-date";
    const LEGAL_HOLD_HEADER: &str = "x-amz-object-lock-legal-hold";

    for header in [MODE_HEADER, RETAIN_UNTIL_HEADER, LEGAL_HOLD_HEADER] {
        if header_count(req, header) > 1 {
            return Err(ServerError::InvalidRequest {
                reason: format!("duplicate header: {header}"),
            });
        }
    }

    let mode = match req.header(MODE_HEADER) {
        None => None,
        Some("GOVERNANCE") => Some(ObjectLockMode::Governance),
        Some("COMPLIANCE") => Some(ObjectLockMode::Compliance),
        Some(other) => {
            return Err(ServerError::InvalidArgument {
                reason: format!("invalid {MODE_HEADER} value: {other}"),
            });
        }
    };
    let retain_until = req
        .header(RETAIN_UNTIL_HEADER)
        .map(xml::parse_object_lock_header_timestamp_secs)
        .transpose()?;
    let legal_hold = match req.header(LEGAL_HOLD_HEADER) {
        None => StoredLegalHoldStatus::NotSet,
        Some("ON") => StoredLegalHoldStatus::from_legal_hold_status(Some(LegalHoldStatus::On)),
        Some("OFF") => StoredLegalHoldStatus::from_legal_hold_status(Some(LegalHoldStatus::Off)),
        Some(other) => {
            return Err(ServerError::InvalidArgument {
                reason: format!("invalid {LEGAL_HOLD_HEADER} value: {other}"),
            });
        }
    };

    let retention = match (mode, retain_until) {
        (None, None) => None,
        (Some(mode), Some(retain_until_unix_seconds)) => Some(ObjectRetention {
            mode,
            retain_until_unix_seconds,
        }),
        _ => {
            return Err(ServerError::InvalidRequest {
                reason: "Object Lock parameters must be paired. If you specify x-amz-object-lock-mode, you must also specify x-amz-object-lock-retain-until-date, and vice versa.".to_string(),
            });
        }
    };

    Ok(ObjectLockState {
        retention,
        legal_hold,
    })
}

fn parse_bypass_governance_retention(req: &S3Request) -> bool {
    req.header("x-amz-bypass-governance-retention")
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

fn parse_put_object_acl(value: Option<&str>) -> crate::coordinator::PutObjectAcl<'_> {
    match value {
        None => crate::coordinator::PutObjectAcl::None,
        Some("private") => crate::coordinator::PutObjectAcl::Private,
        Some("public-read") => crate::coordinator::PutObjectAcl::PublicRead,
        Some("public-read-write") => crate::coordinator::PutObjectAcl::PublicReadWrite,
        Some("authenticated-read") => crate::coordinator::PutObjectAcl::AuthenticatedRead,
        Some("aws-exec-read") => crate::coordinator::PutObjectAcl::AwsExecRead,
        Some("bucket-owner-read") => crate::coordinator::PutObjectAcl::BucketOwnerRead,
        Some("bucket-owner-full-control") => {
            crate::coordinator::PutObjectAcl::BucketOwnerFullControl
        }
        Some(other) => crate::coordinator::PutObjectAcl::Invalid(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::{Coordinator, INTERNAL_SEGMENT_SIZE};
    use auth::canonical::{
        canonical_headers, canonical_query_string, canonical_request, sha256_hex, string_to_sign,
    };
    use auth::SecretKey;
    use ring::hmac;
    use server_core::sse::{ManagedWrappingKeyConfig, StaticManagedKeyProvider};
    use std::sync::Arc;
    use storage::{NodeId, StorageCluster};

    const TEST_SIGV4_ACCESS_KEY: &str = "AKID";
    const TEST_SIGV4_SECRET: &str = "secret";
    const TEST_SSE_S3_WRAPPING_KEY_B64: &str = "YWJjZGVmMDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODk=";

    static EXPECTED_PANIC_ON_500_HOOK: std::sync::Once = std::sync::Once::new();

    struct SuppressExpectedPanicOn500Diagnostics {
        previous_suppressed: bool,
    }

    impl SuppressExpectedPanicOn500Diagnostics {
        fn new() -> Self {
            EXPECTED_PANIC_ON_500_HOOK.call_once(|| {
                let previous_hook = std::panic::take_hook();
                std::panic::set_hook(Box::new(move |panic_info| {
                    if SUPPRESS_EXPECTED_PANIC_ON_500_DIAGNOSTICS.with(std::cell::Cell::get) {
                        return;
                    }
                    previous_hook(panic_info);
                }));
            });
            let previous_suppressed =
                SUPPRESS_EXPECTED_PANIC_ON_500_DIAGNOSTICS.with(|suppressed| {
                    let previous = suppressed.get();
                    suppressed.set(true);
                    previous
                });
            Self {
                previous_suppressed,
            }
        }
    }

    impl Drop for SuppressExpectedPanicOn500Diagnostics {
        fn drop(&mut self) {
            SUPPRESS_EXPECTED_PANIC_ON_500_DIAGNOSTICS
                .with(|suppressed| suppressed.set(self.previous_suppressed));
        }
    }

    fn setup_frontend(dir: &std::path::Path) -> HttpFrontend {
        setup_frontend_with_sse_s3(dir)
    }

    fn open_test_storage_cluster(dir: &std::path::Path, pg_ids: &[u32]) -> Arc<StorageCluster> {
        let ec_config = ec::EcConfig::default();
        let ec_shape = storage::EcShape {
            k: ec_config.data_shards(),
            m: ec_config.parity_shards(),
        };
        let node_count = u32::from(ec_shape.k) + u32::from(ec_shape.m);
        let node_ids: Vec<NodeId> = (0..node_count).map(NodeId::new).collect();
        StorageCluster::open_local_nodes(dir, &node_ids, pg_ids, ec_shape)
            .expect("open local storage cluster")
    }

    fn setup_frontend_with_sse_s3(dir: &std::path::Path) -> HttpFrontend {
        let pg_ids: Vec<u32> = (0..1).collect();
        let storage_cluster = open_test_storage_cluster(dir, &pg_ids);
        let sse_s3_provider = StaticManagedKeyProvider::single(
            ManagedWrappingKeyConfig::from_base64(1, TEST_SSE_S3_WRAPPING_KEY_B64).unwrap(),
        );
        let coordinator = Coordinator::new_with_managed_key_provider_for_storage_cluster(
            storage_cluster,
            "us-east-1".to_string(),
            None,
            sse_s3_provider,
        )
        .unwrap();
        let credentials = auth::CredentialStore::new();
        HttpFrontend {
            coordinator: Arc::new(coordinator),
            credentials,
            host_id: Arc::<str>::from("host-id"),
        }
    }

    fn test_auth() -> auth::AuthContext {
        auth::AuthContext {
            mode: auth::AuthMode::HeaderSigV4,
            access_key_id: Some("AKID".to_string()),
            account: Some(auth::AccountIdentity::from_principal("testuser")),
            authorization_profile: auth::AuthorizationProfile::Standard,
            request_epoch_secs: Some(0),
            signing_region: Some("us-east-1".to_string()),
            streaming: None,
        }
    }

    #[test]
    fn put_object_policy_context_stores_if_match_entity_tag() {
        let policy_context =
            put_object_policy_context_from_request_fields(PutObjectPolicyContextFields {
                conditions: PutObjectConditionalHeaders {
                    if_match: Some(if_match_header_entity_tag_value("\"abcdef1234567890\"")),
                    if_none_match: None,
                },
                ..Default::default()
            });

        assert_eq!(policy_context.if_match, Some("abcdef1234567890"));
    }

    fn create_test_bucket(coord: &Coordinator, name: &str) {
        coord
            .create_bucket(&crate::coordinator::CreateBucketRequest {
                name: parse_bucket_name(name).unwrap(),
                requester: crate::coordinator::test_helpers::requester("testuser"),
                namespace: BucketNamespace::Global,
                acl: crate::coordinator::CreateBucketAcl::DefaultPrivate,
                ownership: crate::coordinator::BucketObjectOwnership::ObjectWriter,
                object_lock_enabled: false,
            })
            .unwrap();
    }

    fn create_sigv4_test_bucket(coord: &Coordinator, name: &str, object_lock_enabled: bool) {
        coord
            .create_bucket(&crate::coordinator::CreateBucketRequest {
                name: parse_bucket_name(name).unwrap(),
                requester: crate::coordinator::test_helpers::requester(TEST_SIGV4_ACCESS_KEY),
                namespace: BucketNamespace::Global,
                acl: crate::coordinator::CreateBucketAcl::DefaultPrivate,
                ownership: crate::coordinator::BucketObjectOwnership::ObjectWriter,
                object_lock_enabled,
            })
            .unwrap();
    }

    fn test_bucket_name(name: &str) -> BucketName {
        parse_bucket_name(name).unwrap()
    }

    #[allow(clippy::format_collect)]
    fn hex_lower(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn current_sigv4_timestamp() -> (String, String) {
        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("current time should be after epoch")
            .as_millis() as u64;
        let timestamp = xml::format_timestamp(now_millis);
        let date = format!(
            "{}{}{}",
            &timestamp[0..4],
            &timestamp[5..7],
            &timestamp[8..10]
        );
        let amz_date = format!(
            "{}{}{}T{}{}{}Z",
            &timestamp[0..4],
            &timestamp[5..7],
            &timestamp[8..10],
            &timestamp[11..13],
            &timestamp[14..16],
            &timestamp[17..19]
        );
        (date, amz_date)
    }

    fn signed_v4_put_req(body: &[u8], extra_headers: Vec<(String, String)>) -> S3Request {
        let (date, amz_date) = current_sigv4_timestamp();
        let body_hash = sha256_hex(body);
        let mut headers = vec![
            (
                "host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            ),
            ("content-length".to_string(), body.len().to_string()),
            ("x-amz-content-sha256".to_string(), body_hash.clone()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        headers.extend(extra_headers);

        let mut signed_headers: Vec<&str> = headers.iter().map(|(name, _)| name.as_str()).collect();
        signed_headers.sort_unstable();
        let signed_headers_str = signed_headers.join(";");
        let canonical_headers_input: Vec<(&str, &str)> = headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        let canonical_headers = canonical_headers(&canonical_headers_input);
        let canonical_query = canonical_query_string("");
        let canonical_req = canonical_request(
            "PUT",
            "/",
            &canonical_query,
            &canonical_headers,
            &signed_headers_str,
            &body_hash,
        );
        let scope = format!("{date}/us-east-1/s3/aws4_request");
        let sts = string_to_sign(&amz_date, &scope, &sha256_hex(canonical_req.as_bytes()));
        let signing_key = auth::sigv4::derive_signing_key(
            &SecretKey::new(TEST_SIGV4_SECRET.to_string()),
            &date,
            "us-east-1",
            "s3",
        );
        let signature = hex_lower(
            hmac::sign(
                &hmac::Key::new(hmac::HMAC_SHA256, signing_key.as_ref()),
                sts.as_bytes(),
            )
            .as_ref(),
        );
        headers.push((
            "authorization".to_string(),
            format!(
                "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
                TEST_SIGV4_ACCESS_KEY, scope, signed_headers_str, signature
            ),
        ));
        new_req(http::Method::PUT, "/", "", headers, body.to_vec())
    }

    fn signed_post_policy_fields(
        bucket: &str,
        key: &str,
        extra_conditions: &[&str],
        extra_fields: &[(&str, &str)],
    ) -> Vec<(String, String)> {
        signed_post_policy_fields_for_region(
            bucket,
            key,
            "us-east-1",
            extra_conditions,
            extra_fields,
        )
    }

    fn signed_post_policy_fields_for_region(
        bucket: &str,
        key: &str,
        region: &str,
        extra_conditions: &[&str],
        extra_fields: &[(&str, &str)],
    ) -> Vec<(String, String)> {
        use base64::Engine;

        let (date, amz_date) = current_sigv4_timestamp();
        let credential = format!("{TEST_SIGV4_ACCESS_KEY}/{date}/{region}/s3/aws4_request");
        let mut conditions = vec![
            format!(r#"{{"bucket":"{bucket}"}}"#),
            format!(r#"{{"key":"{key}"}}"#),
            r#"{"x-amz-algorithm":"AWS4-HMAC-SHA256"}"#.to_string(),
            format!(r#"{{"x-amz-credential":"{credential}"}}"#),
            format!(r#"{{"x-amz-date":"{amz_date}"}}"#),
        ];
        conditions.extend(extra_conditions.iter().map(|value| (*value).to_string()));
        let policy = format!(
            r#"{{"expiration":"2099-12-31T23:59:59Z","conditions":[{}]}}"#,
            conditions.join(",")
        );
        let policy_b64 = base64::engine::general_purpose::STANDARD.encode(policy.as_bytes());
        let signing_key = auth::sigv4::derive_signing_key(
            &SecretKey::new(TEST_SIGV4_SECRET.to_string()),
            &date,
            region,
            "s3",
        );
        let signature = hex_lower(
            hmac::sign(
                &hmac::Key::new(hmac::HMAC_SHA256, signing_key.as_ref()),
                policy_b64.as_bytes(),
            )
            .as_ref(),
        );

        let mut fields = vec![
            ("key".to_string(), key.to_string()),
            (
                "x-amz-algorithm".to_string(),
                "AWS4-HMAC-SHA256".to_string(),
            ),
            ("x-amz-credential".to_string(), credential),
            ("x-amz-date".to_string(), amz_date),
            ("policy".to_string(), policy_b64),
            ("x-amz-signature".to_string(), signature),
        ];
        fields.extend(
            extra_fields
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string())),
        );
        fields
    }

    #[test]
    fn validate_write_request_header_section_size_accepts_headers_at_limit() {
        let headers = [(
            "x-test-padding",
            "p".repeat(MAX_WRITE_REQUEST_HEADER_SECTION_SIZE - "x-test-padding".len()),
        )];
        let refs: Vec<(&str, &str)> = headers
            .iter()
            .map(|(name, value)| (*name, value.as_str()))
            .collect();
        validate_write_request_header_section_size(&refs).unwrap();
    }

    #[test]
    fn validate_write_request_header_section_size_rejects_headers_over_limit() {
        let headers = [(
            "x-test-padding",
            "p".repeat(MAX_WRITE_REQUEST_HEADER_SECTION_SIZE - "x-test-padding".len() + 1),
        )];
        let refs: Vec<(&str, &str)> = headers
            .iter()
            .map(|(name, value)| (*name, value.as_str()))
            .collect();
        let err = validate_write_request_header_section_size(&refs).unwrap_err();
        assert!(matches!(err, ServerError::RequestHeaderSectionTooLarge));
    }

    #[test]
    fn parse_request_metadata_accepts_user_metadata_at_limit() {
        let key = "x-amz-meta-limit";
        let value = "m".repeat(USER_METADATA_SIZE_LIMIT - "limit".len());

        let (metadata, system_metadata) = parse_request_metadata([(key, value.as_str())]).unwrap();

        assert_eq!(metadata.get("x-amz-meta-limit"), Some(value.as_str()));
        assert_eq!(system_metadata, SystemMetadata::EMPTY);
    }

    #[test]
    fn parse_put_object_request_metadata_ignores_invalid_checksum_type() {
        let (metadata, system_metadata) =
            parse_put_object_request_metadata([("x-amz-checksum-type", "BOGUS")]).unwrap();

        assert_eq!(metadata, MetadataBlob::new());
        assert_eq!(system_metadata, SystemMetadata::EMPTY);
    }

    #[test]
    fn parse_put_object_request_metadata_ignores_checksum_type_with_value() {
        let (metadata, system_metadata) = parse_put_object_request_metadata([
            ("x-amz-checksum-type", "COMPOSITE"),
            ("x-amz-checksum-crc32", "AAAAAA=="),
        ])
        .unwrap();

        assert_eq!(metadata, MetadataBlob::new());
        let checksum = system_metadata.checksum().expect("checksum metadata");
        assert_eq!(checksum.algorithm(), ChecksumAlgorithm::Crc32);
        assert_eq!(checksum.checksum_type(), None);
        assert_eq!(checksum.value(), "AAAAAA==");
    }

    #[test]
    fn parse_request_metadata_rejects_user_metadata_over_limit() {
        let key = "x-amz-meta-limit";
        let value = "m".repeat(USER_METADATA_SIZE_LIMIT - "limit".len() + 1);

        let err = parse_request_metadata([(key, value.as_str())]).unwrap_err();
        assert!(matches!(
            err,
            ServerError::MetadataTooLargeDetailed {
                max_size_allowed: USER_METADATA_SIZE_LIMIT,
                ..
            }
        ));
    }

    #[test]
    fn parse_request_metadata_accepts_system_metadata_at_limit() {
        let value = "v".repeat(SYSTEM_METADATA_SIZE_LIMIT - "content-disposition".len());

        let (metadata, system_metadata) =
            parse_request_metadata([("content-disposition", value.as_str())]).unwrap();

        assert_eq!(metadata, MetadataBlob::new());
        assert_eq!(
            system_metadata
                .content_disposition()
                .map(|value| value.as_str()),
            Some(value.as_str())
        );
    }

    #[test]
    fn parse_request_metadata_rejects_system_metadata_over_limit() {
        let value = "v".repeat(SYSTEM_METADATA_SIZE_LIMIT - "content-disposition".len() + 1);

        let err = parse_request_metadata([("content-disposition", value.as_str())]).unwrap_err();
        assert!(matches!(
            err,
            ServerError::MetadataTooLargeDetailed {
                max_size_allowed: SYSTEM_METADATA_SIZE_LIMIT,
                ..
            }
        ));
    }

    #[test]
    fn parse_request_metadata_rejects_redirect_plus_other_system_metadata_over_limit() {
        let redirect_len = SYSTEM_METADATA_SIZE_LIMIT - "x-amz-website-redirect-location".len();
        let redirect = format!("/{}", "r".repeat(redirect_len - 1));

        let err = parse_request_metadata([
            ("x-amz-website-redirect-location", redirect.as_str()),
            ("cache-control", "x"),
        ])
        .unwrap_err();
        assert!(matches!(
            err,
            ServerError::MetadataTooLargeDetailed {
                max_size_allowed: SYSTEM_METADATA_SIZE_LIMIT,
                ..
            }
        ));
    }

    #[test]
    fn parse_request_metadata_rejects_redirect_without_supported_prefix() {
        let err =
            parse_request_metadata([(WEBSITE_REDIRECT_LOCATION_HEADER_NAME, "docs/landing.html")])
                .unwrap_err();
        assert!(matches!(err, ServerError::InvalidRedirectLocation { .. }));
    }

    #[test]
    fn parse_request_metadata_rejects_redirect_with_unsupported_scheme() {
        let err = parse_request_metadata([(
            WEBSITE_REDIRECT_LOCATION_HEADER_NAME,
            "ftp://example.com/out",
        )])
        .unwrap_err();
        assert!(matches!(err, ServerError::InvalidRedirectLocation { .. }));
    }

    #[test]
    fn parse_copy_source_header_parses_typed_source() {
        let (bucket, key, version_id) =
            parse_copy_source_header("/source-bucket/path/to/key?versionId=42").unwrap();
        assert_eq!(bucket.as_str(), "source-bucket");
        assert_eq!(key.as_str(), "path/to/key");
        assert_eq!(version_id, Some(VersionId::from_u64(42)));
    }

    #[test]
    fn parse_copy_source_header_maps_invalid_source_bucket_to_bucket_not_found() {
        match parse_copy_source_header("/BadBucket/key") {
            Err(ServerError::BucketNotFound { name }) => assert_eq!(name, "BadBucket"),
            other => panic!("expected BucketNotFound, got {other:?}"),
        }
    }

    #[test]
    fn parse_copy_source_header_maps_oversized_source_bucket_to_bucket_not_found() {
        let oversized_bucket = "a".repeat(64);
        match parse_copy_source_header(&format!("/{oversized_bucket}/key")) {
            Err(ServerError::BucketNotFound { name }) => assert_eq!(name, oversized_bucket),
            other => panic!("expected BucketNotFound, got {other:?}"),
        }
    }

    #[test]
    fn unsupported_sigv2_error_returns_sigv4_required_message_in_eu_central_1() {
        let err = HttpFrontend::unsupported_sigv2_error(Some("AWS AKIA:signature"), "eu-central-1")
            .expect("expected eu-central-1 SigV2 to be rejected");
        match err {
            ServerError::InvalidRequest { reason } => assert_eq!(
                reason,
                "The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256."
            ),
            other => panic!("expected InvalidRequest, got {other:?}"),
        }
    }

    #[test]
    fn unsupported_sigv2_error_does_not_override_other_regions() {
        assert!(
            HttpFrontend::unsupported_sigv2_error(Some("AWS AKIA:signature"), "us-west-2")
                .is_none()
        );
    }

    #[test]
    fn bucket_region_mismatch_returns_wrong_region_for_existing_bucket() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let auth = auth::AuthContext {
            mode: auth::AuthMode::HeaderSigV4,
            access_key_id: Some("AKID".to_string()),
            account: Some(auth::AccountIdentity::from_principal("testuser")),
            authorization_profile: auth::AuthorizationProfile::Standard,
            request_epoch_secs: Some(0),
            signing_region: Some("us-west-2".to_string()),
            streaming: None,
        };

        match fe.enforce_bucket_region_for_operation(
            &S3Operation::PutObject {
                bucket: test_bucket_name("mybucket"),
                key: "key".to_string(),
            },
            &auth,
        ) {
            Err(ServerError::WrongRegion {
                provided_region,
                expected_region,
                bucket_region_header,
            }) => {
                assert_eq!(provided_region, "us-west-2");
                assert_eq!(expected_region, "us-east-1");
                assert!(bucket_region_header);
            }
            other => panic!("expected WrongRegion, got {other:?}"),
        }
    }

    #[test]
    fn bucket_region_mismatch_returns_wrong_region_without_header_for_missing_bucket() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        let auth = auth::AuthContext {
            mode: auth::AuthMode::HeaderSigV4,
            access_key_id: Some("AKID".to_string()),
            account: Some(auth::AccountIdentity::from_principal("testuser")),
            authorization_profile: auth::AuthorizationProfile::Standard,
            request_epoch_secs: Some(0),
            signing_region: Some("us-west-2".to_string()),
            streaming: None,
        };

        match fe.enforce_bucket_region_for_operation(
            &S3Operation::PutObject {
                bucket: test_bucket_name("missing"),
                key: "key".to_string(),
            },
            &auth,
        ) {
            Err(ServerError::WrongRegion {
                provided_region,
                expected_region,
                bucket_region_header,
            }) => {
                assert_eq!(provided_region, "us-west-2");
                assert_eq!(expected_region, "us-east-1");
                assert!(!bucket_region_header);
            }
            other => panic!("expected WrongRegion, got {other:?}"),
        }
    }

    #[test]
    fn create_bucket_defers_region_check() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        assert!(fe.should_defer_region_check(&S3Operation::CreateBucket {
            bucket: test_bucket_name("mybucket"),
        }));
    }

    fn test_bucket_request(name: &str) -> crate::coordinator::BucketRequest<'_> {
        crate::coordinator::BucketRequest::new(
            test_bucket_name(name),
            crate::coordinator::test_helpers::requester("testuser"),
            None,
        )
    }

    fn test_object_request<'a>(
        bucket: &'a str,
        key: &'a str,
    ) -> crate::coordinator::ObjectRequest<'a> {
        crate::coordinator::ObjectRequest::new(
            parse_bucket_name(bucket).unwrap(),
            parse_object_key(key).unwrap(),
            crate::coordinator::test_helpers::requester("testuser"),
            None,
        )
    }

    #[test]
    fn list_buckets_uses_owner_id_without_display_name() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        let owner_canonical_id = s3_types::CanonicalUserId::from_principal("custom-account-id");
        let account = auth::AccountIdentity::new("testuser", owner_canonical_id.clone(), "User A");

        fe.coordinator
            .create_bucket(&crate::coordinator::CreateBucketRequest {
                name: parse_bucket_name("mybucket").unwrap(),
                requester: crate::coordinator::Requester::authenticated(account.clone()),
                namespace: BucketNamespace::Global,
                acl: crate::coordinator::CreateBucketAcl::DefaultPrivate,
                ownership: crate::coordinator::BucketObjectOwnership::ObjectWriter,
                object_lock_enabled: false,
            })
            .unwrap();

        let bucket = fe
            .coordinator
            .head_bucket(&crate::coordinator::BucketRequest::new(
                parse_bucket_name("mybucket").unwrap(),
                crate::coordinator::Requester::authenticated(account.clone()),
                None,
            ))
            .unwrap();
        assert_eq!(bucket.owner_canonical_id, owner_canonical_id);

        let auth = auth::AuthContext {
            mode: auth::AuthMode::HeaderSigV4,
            access_key_id: Some("AKID".to_string()),
            account: Some(account),
            authorization_profile: auth::AuthorizationProfile::Standard,
            request_epoch_secs: Some(0),
            signing_region: Some("us-east-1".to_string()),
            streaming: None,
        };
        let resp = fe
            .dispatch_routed(&make_req(""), &auth, S3Operation::ListBuckets)
            .unwrap();
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains(owner_canonical_id.as_str()));
        assert!(!body.contains("<DisplayName>"));
        assert!(body.contains("<BucketArn>arn:aws:s3:::mybucket</BucketArn>"));
    }

    #[test]
    fn get_bucket_location_dispatches_through_dedicated_bucket_location_checks() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(http::Method::GET, "/mybucket", "location", vec![], vec![]);
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::GetBucketLocation {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<LocationConstraint"));
        assert!(!body.contains(">us-east-1<"));
    }

    fn make_req(query: &str) -> S3Request {
        S3Request::new_for_test(
            http::Method::GET,
            "/",
            query,
            test_headers(vec![]),
            vec![],
            0,
        )
    }

    fn new_req(
        method: http::Method,
        path: &str,
        query: &str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> S3Request {
        S3Request::new_for_test(method, path, query, test_headers(headers), body, 0)
    }

    fn test_headers(headers: Vec<(String, String)>) -> http::HeaderMap {
        request::header_map_from_owned(headers)
    }

    fn find_header<'a>(resp: &'a S3Response, name: &str) -> Option<&'a str> {
        resp.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn response_body(resp: S3Response) -> Vec<u8> {
        match resp.stream {
            Some(mut stream) => {
                let mut body = Vec::new();
                while let Some(chunk) = stream
                    .next_chunk(INTERNAL_SEGMENT_SIZE)
                    .expect("read streamed response body")
                {
                    body.extend_from_slice(&chunk);
                }
                body
            }
            None => resp.body,
        }
    }

    #[test]
    fn response_overrides_apply_valid_values() {
        let cases = [
            (
                "response-content-type=text%2Fplain",
                "Content-Type",
                "text/plain",
            ),
            (
                "response-content-disposition=attachment%3B%20filename%3D%22test.txt%22",
                "Content-Disposition",
                "attachment; filename=\"test.txt\"",
            ),
            ("response-content-encoding=gzip", "Content-Encoding", "gzip"),
            (
                "response-content-language=en-US",
                "Content-Language",
                "en-US",
            ),
            (
                "response-cache-control=max-age%3D60",
                "Cache-Control",
                "max-age=60",
            ),
            (
                "response-expires=Mon%2C%2015%20Jan%202024%2012%3A30%3A45%20GMT",
                "Expires",
                "Mon, 15 Jan 2024 12:30:45 GMT",
            ),
        ];

        for (query, header_name, expected) in cases {
            let req = make_req(query);
            let mut resp = S3Response {
                status_code: 200,
                headers: Vec::new(),
                body: Vec::new(),
                stream: None,
                error_diagnostic: None,
            };
            apply_response_overrides(&mut resp, &req);
            assert_eq!(find_header(&resp, header_name), Some(expected));
        }
    }

    #[test]
    fn response_overrides_sanitize_or_ignore_invalid_values() {
        let cases = [
            (
                "response-content-type=text%2Fplain%0D%0AInjected%3A%20x",
                "Content-Type",
                Some("text/plain  Injected: x"),
            ),
            (
                "response-content-disposition=attachment%0D%0AInjected%3A%20x",
                "Content-Disposition",
                Some("attachment  Injected: x"),
            ),
            (
                "response-content-encoding=gzip%0D%0AInjected%3A%20x",
                "Content-Encoding",
                Some("gzip  Injected: x"),
            ),
            (
                "response-content-language=en-US%0D%0AInjected%3A%20x",
                "Content-Language",
                Some("en-US  Injected: x"),
            ),
            (
                "response-cache-control=max-age%3D60%0D%0AInjected%3A%20x",
                "Cache-Control",
                Some("max-age=60  Injected: x"),
            ),
            ("response-expires=not-a-date", "Expires", Some("not-a-date")),
        ];

        for (query, header_name, expected) in cases {
            let req = make_req(query);
            let mut resp = S3Response {
                status_code: 200,
                headers: Vec::new(),
                body: Vec::new(),
                stream: None,
                error_diagnostic: None,
            };
            if header_name == "Content-Type" {
                resp.headers.push((
                    "Content-Type".to_string(),
                    "application/octet-stream".to_string(),
                ));
            }
            apply_response_overrides(&mut resp, &req);
            assert_eq!(find_header(&resp, header_name), expected);
        }
    }

    #[test]
    fn s3_response_to_hyper_invalid_header_returns_internal_error() {
        let mut resp = S3Response {
            status_code: 200,
            headers: Vec::new(),
            body: Vec::new(),
            stream: None,
            error_diagnostic: None,
        };
        resp.headers.push((
            "Content-Type".to_string(),
            "text/plain\r\nInjected: x".to_string(),
        ));

        let hyper_resp = s3_response_to_hyper(
            resp,
            None,
            8192,
            false,
            false,
            ResponseTraceMeta::new(
                crate::http::new_request_trace_context(),
                Arc::<str>::from("host-id"),
                "GET",
                "/",
                "",
            ),
        );
        assert_eq!(hyper_resp.status(), http::StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn s3_response_to_hyper_invalid_header_records_diagnostic_before_panic() {
        let mut resp = S3Response {
            status_code: 200,
            headers: Vec::new(),
            body: Vec::new(),
            stream: None,
            error_diagnostic: None,
        };
        resp.headers.push((
            "Content-Type".to_string(),
            "text/plain\r\nInjected: x".to_string(),
        ));

        let result = {
            let _diagnostic_guard = SuppressExpectedPanicOn500Diagnostics::new();
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = s3_response_to_hyper(
                    resp,
                    None,
                    8192,
                    true,
                    false,
                    ResponseTraceMeta::new(
                        observability::TraceContext::from_ids(observability::TraceContextIds {
                            trace_id: "trace-conversion-error".to_string(),
                            request_id: "request-conversion-error".to_string(),
                        }),
                        Arc::<str>::from("host-id"),
                        "GET",
                        "/secret-bucket/secret-key",
                        "X-Amz-Signature=secret",
                    ),
                );
            }))
        };

        let panic_payload =
            result.expect_err("conversion error should panic when panic-on-500 is enabled");
        let panic_message = panic_payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic_payload.downcast_ref::<&str>().copied())
            .expect("panic should carry diagnostic string");
        assert!(panic_message.contains("HTTP response conversion produced InternalError"));

        let records = observability::flight_recorder_snapshot();
        let request_error_record = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "request-conversion-error" && record.event == "request_error"
            })
            .expect("conversion error should be recorded before panic");
        assert!(request_error_record.detail.contains("status=500"));
        assert!(request_error_record.detail.contains("path_hash="));
        assert!(request_error_record.detail.contains("sigv4_query=true"));
        assert!(request_error_record
            .detail
            .contains("error_code=InternalError"));
        assert!(request_error_record
            .detail
            .contains("cause_label=internal_error"));
        assert!(!request_error_record.detail.contains("secret-bucket"));
        assert!(!request_error_record.detail.contains("secret-key"));
        assert!(!request_error_record.detail.contains("secret"));

        let cause_chain_record = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "request-conversion-error"
                    && record.event == "request_500_cause_chain"
            })
            .expect("conversion error cause chain should be recorded before panic");
        assert!(cause_chain_record.detail.contains("status=500"));
        assert!(cause_chain_record.detail.contains("path_hash="));
        assert!(cause_chain_record.detail.contains("sigv4_query=true"));
        assert!(cause_chain_record
            .detail
            .contains("cause_label=internal_error"));
        assert!(cause_chain_record
            .detail
            .contains("cause_chain=\"server_error>internal_error\""));
        assert!(!cause_chain_record.detail.contains("secret-bucket"));
        assert!(!cause_chain_record.detail.contains("secret-key"));
        assert!(!cause_chain_record.detail.contains("secret"));
    }

    #[test]
    fn s3_response_to_hyper_panics_on_500_when_enabled() {
        let resp = S3Response {
            status_code: 500,
            headers: Vec::new(),
            body: b"<Error><Code>InternalError</Code></Error>".to_vec(),
            stream: None,
            error_diagnostic: None,
        };

        let result = {
            let _diagnostic_guard = SuppressExpectedPanicOn500Diagnostics::new();
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = s3_response_to_hyper(
                    resp,
                    None,
                    8192,
                    true,
                    false,
                    ResponseTraceMeta::new(
                        crate::http::new_request_trace_context(),
                        Arc::<str>::from("host-id"),
                        "GET",
                        "/",
                        "",
                    ),
                );
            }))
        };

        let panic_payload =
            result.expect_err("HTTP 500 response should panic when panic-on-500 is enabled");
        let panic_message = panic_payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic_payload.downcast_ref::<&str>().copied())
            .expect("panic should carry diagnostic string");
        assert!(panic_message.contains("server produced HTTP 500 response"));
    }

    #[test]
    fn s3_response_to_hyper_deduplicates_content_length() {
        let resp = S3Response {
            status_code: 200,
            headers: vec![
                ("Content-Length".to_string(), "5".to_string()),
                ("Content-Length".to_string(), "5".to_string()),
            ],
            body: b"hello".to_vec(),
            stream: None,
            error_diagnostic: None,
        };

        let hyper_resp = s3_response_to_hyper(
            resp,
            None,
            8192,
            false,
            false,
            ResponseTraceMeta::new(
                crate::http::new_request_trace_context(),
                Arc::<str>::from("host-id"),
                "GET",
                "/",
                "",
            ),
        );
        assert_eq!(
            hyper_resp
                .headers()
                .get_all(http::header::CONTENT_LENGTH)
                .iter()
                .count(),
            1
        );
        assert_eq!(
            hyper_resp
                .headers()
                .get(http::header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok()),
            Some("5")
        );
    }

    #[test]
    fn new_request_trace_context_matches_expected_id_shapes() {
        let ctx = crate::http::new_request_trace_context();
        assert_eq!(ctx.trace_id().len(), 32);
        assert_eq!(ctx.request_id().len(), 16);
        assert!(ctx.trace_id().bytes().all(|b| b.is_ascii_hexdigit()));
        assert!(ctx.trace_id().bytes().all(|b| !b.is_ascii_uppercase()));
        assert!(ctx
            .request_id()
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit()));
    }

    #[test]
    fn new_host_id_returns_visible_ascii_header_value() {
        let host_id = crate::http::new_host_id();
        assert!(!host_id.is_empty());
        assert!(host_id.bytes().all(|b| b.is_ascii_graphic()));
        assert!(http::header::HeaderValue::from_str(&host_id).is_ok());
    }

    #[test]
    fn s3_response_to_hyper_injects_request_and_host_headers() {
        let resp = S3Response {
            status_code: 200,
            headers: Vec::new(),
            body: Vec::new(),
            stream: None,
            error_diagnostic: None,
        };

        let hyper_resp = s3_response_to_hyper(
            resp,
            None,
            8192,
            false,
            false,
            ResponseTraceMeta::new(
                observability::TraceContext::from_ids(observability::TraceContextIds {
                    trace_id: "0123456789abcdef0123456789abcdef".to_string(),
                    request_id: "2VG1X5NNMZ52HKC0".to_string(),
                }),
                Arc::<str>::from("stable-host-id"),
                "GET",
                "/",
                "",
            ),
        );

        assert_eq!(
            hyper_resp
                .headers()
                .get("x-amz-request-id")
                .and_then(|value| value.to_str().ok()),
            Some("2VG1X5NNMZ52HKC0")
        );
        assert_eq!(
            hyper_resp
                .headers()
                .get("x-amz-id-2")
                .and_then(|value| value.to_str().ok()),
            Some("stable-host-id")
        );
    }

    #[test]
    fn streaming_body_error_diagnostic_preserves_response_status() {
        let mut trace = ResponseBodyTrace::new(
            ResponseTraceMeta::new(
                observability::TraceContext::from_ids(observability::TraceContextIds {
                    trace_id: "0123456789abcdef0123456789abcdef".to_string(),
                    request_id: "2VG1X5NNMZ52HKC0".to_string(),
                }),
                Arc::<str>::from("stable-host-id"),
                "GET",
                "/bucket/key",
                "partNumber=1",
            ),
            206,
            1024,
            true,
        );

        trace.emit_error(&ServerError::Store(
            storage::StoreError::StorageRpcResourceExhausted {
                node_id: 1,
                operation: "ReadHandlesAcquire",
                message: "limit exceeded".to_string(),
            },
        ));

        assert_eq!(trace.status_code, 206);
        assert!(trace.terminal_event_emitted);

        let records = observability::flight_recorder_snapshot();
        let request_error_record = records
            .iter()
            .rev()
            .find(|record| {
                record.request_id == "2VG1X5NNMZ52HKC0" && record.event == "request_error"
            })
            .expect("streaming body error should record request error");
        assert!(request_error_record.detail.contains("status=206"));
        assert!(request_error_record
            .detail
            .contains("error_code=InternalError"));
        assert!(!records.iter().any(|record| {
            record.request_id == "2VG1X5NNMZ52HKC0" && record.event == "request_500_cause_chain"
        }));
    }

    #[test]
    fn s3_response_error_with_ids_embeds_explicit_request_and_host_ids() {
        let wire_ids =
            WireResponseIds::new("2VG1X5NNMZ52HKC0".to_string(), "stable-host-id".to_string());
        let resp = S3Response::error_with_ids(
            &ServerError::MetadataTooLargeDetailed {
                size: 2049,
                max_size_allowed: 2048,
            },
            "",
            &wire_ids,
        );
        let body = std::str::from_utf8(&resp.body).unwrap_or("");
        assert!(resp.stream.is_some());
        assert!(body.is_empty());
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<RequestId>2VG1X5NNMZ52HKC0</RequestId>"));
        assert!(body.contains("<HostId>stable-host-id</HostId>"));
    }

    #[test]
    fn parse_bucket_namespace_defaults_to_global() {
        let req = new_req(http::Method::PUT, "/bucket", "", vec![], vec![]);
        assert_eq!(
            parse_bucket_namespace(&req).unwrap(),
            BucketNamespace::Global
        );
    }

    #[test]
    fn parse_bucket_namespace_accepts_account_regional() {
        let req = new_req(
            http::Method::PUT,
            "/bucket",
            "",
            vec![(
                "x-amz-bucket-namespace".to_string(),
                "account-regional".to_string(),
            )],
            vec![],
        );
        assert_eq!(
            parse_bucket_namespace(&req).unwrap(),
            BucketNamespace::AccountRegional
        );
    }

    #[test]
    fn parse_bucket_namespace_rejects_invalid_values() {
        let req = new_req(
            http::Method::PUT,
            "/bucket",
            "",
            vec![("x-amz-bucket-namespace".to_string(), "bogus".to_string())],
            vec![],
        );
        match parse_bucket_namespace(&req).unwrap_err() {
            ServerError::InvalidArgument { reason } => {
                assert_eq!(reason, "invalid x-amz-bucket-namespace: bogus");
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn parse_sse_customer_request_mismatched_key_md5_is_invalid_argument() {
        use base64::Engine;

        let key_b64 = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
        let req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![
                (
                    SSE_C_ALGORITHM_HEADER.to_string(),
                    SSE_CUSTOMER_ALGORITHM.to_string(),
                ),
                (SSE_C_KEY_HEADER.to_string(), key_b64),
                (
                    SSE_C_KEY_MD5_HEADER.to_string(),
                    "AAAAAAAAAAAAAAAAAAAAAA==".to_string(),
                ),
            ],
            vec![],
        );

        match parse_sse_customer_request(&req) {
            Err(ServerError::InvalidSseCustomerKeyMd5) => {}
            other => panic!("expected InvalidSseCustomerKeyMd5, got {other:?}"),
        }
    }

    #[test]
    fn parse_sse_customer_form_fields_mismatched_key_md5_is_invalid_argument() {
        use base64::Engine;

        let key_b64 = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
        let form_fields = vec![
            (
                SSE_C_ALGORITHM_HEADER.to_string(),
                SSE_CUSTOMER_ALGORITHM.to_string(),
            ),
            (SSE_C_KEY_HEADER.to_string(), key_b64),
            (
                SSE_C_KEY_MD5_HEADER.to_string(),
                "AAAAAAAAAAAAAAAAAAAAAA==".to_string(),
            ),
        ];

        match parse_sse_customer_form_fields(TransportSecurity::Tls, &form_fields) {
            Err(ServerError::InvalidSseCustomerKeyMd5) => {}
            other => panic!("expected InvalidSseCustomerKeyMd5, got {other:?}"),
        }
    }

    #[test]
    fn parse_sse_customer_request_rejects_lowercase_algorithm() {
        use base64::Engine;

        let key_b64 = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
        let req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![
                (SSE_C_ALGORITHM_HEADER.to_string(), "aes256".to_string()),
                (SSE_C_KEY_HEADER.to_string(), key_b64),
                (
                    SSE_C_KEY_MD5_HEADER.to_string(),
                    "cLyPS3KoaSFGi/joRB3OUQ==".to_string(),
                ),
            ],
            vec![],
        );

        match parse_sse_customer_request(&req) {
            Err(ServerError::InvalidEncryptionAlgorithmError { value }) => {
                assert_eq!(value, "aes256");
            }
            other => panic!(
                "expected InvalidEncryptionAlgorithmError for lowercase SSE-C algorithm, got {other:?}"
            ),
        }
    }

    #[test]
    fn parse_sse_customer_form_fields_rejects_lowercase_algorithm() {
        use base64::Engine;

        let key_b64 = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
        let form_fields = vec![
            (SSE_C_ALGORITHM_HEADER.to_string(), "aes256".to_string()),
            (SSE_C_KEY_HEADER.to_string(), key_b64),
            (
                SSE_C_KEY_MD5_HEADER.to_string(),
                "cLyPS3KoaSFGi/joRB3OUQ==".to_string(),
            ),
        ];

        match parse_sse_customer_form_fields(TransportSecurity::Tls, &form_fields) {
            Err(ServerError::InvalidEncryptionAlgorithmError { value }) => {
                assert_eq!(value, "aes256");
            }
            other => panic!(
                "expected InvalidEncryptionAlgorithmError for lowercase POST SSE-C algorithm, got {other:?}"
            ),
        }
    }

    #[test]
    fn put_object_explicit_sse_s3_returns_encryption_headers() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend_with_sse_s3(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let put_req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![(SSE_HEADER.to_string(), "AES256".to_string())],
            b"hello world".to_vec(),
        );
        let put_resp = fe
            .dispatch_routed(
                &put_req,
                &test_auth(),
                S3Operation::PutObject {
                    bucket: test_bucket_name("mybucket"),
                    key: "mykey".to_string(),
                },
            )
            .unwrap();
        assert_eq!(put_resp.status_code, 200);
        assert_eq!(
            find_header(&put_resp, "x-amz-server-side-encryption"),
            Some("AES256")
        );

        let head_req = new_req(http::Method::HEAD, "", "", vec![], vec![]);
        let head_resp = fe
            .dispatch_routed(
                &head_req,
                &test_auth(),
                S3Operation::HeadObject {
                    bucket: test_bucket_name("mybucket"),
                    key: "mykey".to_string(),
                },
            )
            .unwrap();
        assert_eq!(head_resp.status_code, 200);
        assert_eq!(
            find_header(&head_resp, "x-amz-server-side-encryption"),
            Some("AES256")
        );
    }

    #[test]
    fn put_object_rejects_sse_s3_with_sse_c_headers() {
        use base64::Engine;

        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let key_b64 = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![
                (SSE_HEADER.to_string(), "AES256".to_string()),
                (
                    SSE_C_ALGORITHM_HEADER.to_string(),
                    SSE_CUSTOMER_ALGORITHM.to_string(),
                ),
                (SSE_C_KEY_HEADER.to_string(), key_b64),
                (
                    SSE_C_KEY_MD5_HEADER.to_string(),
                    "cLyPS3KoaSFGi/joRB3OUQ==".to_string(),
                ),
            ],
            b"hello world".to_vec(),
        );

        match fe.dispatch_routed(
            &req,
            &test_auth(),
            S3Operation::PutObject {
                bucket: test_bucket_name("mybucket"),
                key: "mykey".to_string(),
            },
        ) {
            Err(ServerError::InvalidArgument { reason })
                if reason == "x-amz-server-side-encryption may not be used with SSE-C headers" => {}
            Err(err) => panic!("expected InvalidArgument, got {err:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_object_rejects_aes256_with_kms_key_id() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![
                (SSE_HEADER.to_string(), "AES256".to_string()),
                (
                    SSE_KMS_KEY_ID_HEADER.to_string(),
                    "arn:aws:kms:us-east-1:111122223333:key/example".to_string(),
                ),
            ],
            b"hello world".to_vec(),
        );

        match fe.dispatch_routed(
            &req,
            &test_auth(),
            S3Operation::PutObject {
                bucket: test_bucket_name("mybucket"),
                key: "mykey".to_string(),
            },
        ) {
            Err(ServerError::InvalidArgument { reason })
                if reason
                    == "x-amz-server-side-encryption-aws-kms-key-id may not be used with x-amz-server-side-encryption: AES256" => {}
            Err(err) => panic!("expected InvalidArgument, got {err:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn head_object_rejects_managed_encryption_request_headers() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let put_req = new_req(http::Method::PUT, "", "", vec![], b"hello world".to_vec());
        fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutObject {
                bucket: test_bucket_name("mybucket"),
                key: "mykey".to_string(),
            },
        )
        .unwrap();

        let head_req = new_req(
            http::Method::HEAD,
            "",
            "",
            vec![(SSE_HEADER.to_string(), "AES256".to_string())],
            vec![],
        );
        match fe.dispatch_routed(
            &head_req,
            &test_auth(),
            S3Operation::HeadObject {
                bucket: test_bucket_name("mybucket"),
                key: "mykey".to_string(),
            },
        ) {
            Err(ServerError::InvalidManagedEncryptionReadHeader { context, header })
                if context == ManagedEncryptionReadHeaderContext::StandardObjectRead
                    && header
                        == (ManagedEncryptionReadHeader::ServerSideEncryption {
                            value: "AES256".to_string(),
                        }) => {}
            Err(err) => panic!("expected InvalidRequest, got {err:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn parse_acl_grants_accepts_supported_header_grantees() {
        let canonical_id = s3_types::CanonicalUserId::from_principal("grantee-a");
        let req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![
                (
                    "x-amz-grant-read".to_string(),
                    format!(
                        "id=\"{}\", uri=\"{}\"",
                        canonical_id.as_str(),
                        s3_types::AclGrantee::all_users_uri()
                    ),
                ),
                (
                    "x-amz-grant-full-control".to_string(),
                    format!("id=\"{}\"", canonical_id.as_str()),
                ),
            ],
            vec![],
        );

        let grants = parse_acl_grants(&req).unwrap();
        assert_eq!(
            grants,
            s3_types::AclGrants::new(vec![
                s3_types::AclGrant::new(
                    s3_types::AclGrantee::CanonicalUser(canonical_id.clone()),
                    s3_types::AclPermission::Read,
                ),
                s3_types::AclGrant::new(
                    s3_types::AclGrantee::AllUsers,
                    s3_types::AclPermission::Read,
                ),
                s3_types::AclGrant::new(
                    s3_types::AclGrantee::CanonicalUser(canonical_id),
                    s3_types::AclPermission::FullControl,
                ),
            ])
        );
    }

    #[test]
    fn parse_confirm_remove_self_bucket_access_accepts_supported_values() {
        assert!(!parse_confirm_remove_self_bucket_access(None).unwrap());
        assert!(parse_confirm_remove_self_bucket_access(Some("true")).unwrap());
        assert!(!parse_confirm_remove_self_bucket_access(Some("false")).unwrap());
    }

    #[test]
    fn parse_confirm_remove_self_bucket_access_rejects_invalid_value() {
        match parse_confirm_remove_self_bucket_access(Some("True")) {
            Err(ServerError::InvalidArgument { reason }) => {
                assert!(reason.contains("x-amz-confirm-remove-self-bucket-access"));
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn parse_acl_grants_rejects_body_and_headers_together() {
        let req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![(
                "x-amz-grant-read".to_string(),
                format!("uri=\"{}\"", s3_types::AclGrantee::all_users_uri()),
            )],
            b"<AccessControlPolicy/>".to_vec(),
        );

        match parse_acl_grants(&req) {
            Err(ServerError::InvalidArgument { reason }) => {
                assert!(reason.contains("cannot be combined"));
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn parse_create_bucket_acl_accepts_unquoted_grant_headers() {
        let canonical_id = s3_types::CanonicalUserId::from_principal("grantee-a");
        let req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![(
                "x-amz-grant-read".to_string(),
                format!("id={}", canonical_id.as_str()),
            )],
            vec![],
        );

        let acl = parse_create_bucket_acl(&req).unwrap();
        assert_eq!(
            acl,
            crate::coordinator::CreateBucketAcl::Grants(s3_types::AclGrants::new(vec![
                s3_types::AclGrant::new(
                    s3_types::AclGrantee::CanonicalUser(canonical_id),
                    s3_types::AclPermission::Read,
                ),
            ]))
        );
    }

    #[test]
    fn parse_acl_grants_rejects_unquoted_header_value() {
        let canonical_id = s3_types::CanonicalUserId::from_principal("grantee-a");
        let req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![(
                "x-amz-grant-read".to_string(),
                format!("id={}", canonical_id.as_str()),
            )],
            vec![],
        );

        match parse_acl_grants(&req) {
            Err(ServerError::InvalidArgument { .. }) => {}
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn parse_create_bucket_acl_rejects_acl_and_grants_together() {
        let canonical_id = s3_types::CanonicalUserId::from_principal("grantee-a");
        let req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![
                ("x-amz-acl".to_string(), "private".to_string()),
                (
                    "x-amz-grant-read".to_string(),
                    format!("id={}", canonical_id.as_str()),
                ),
            ],
            vec![],
        );

        match parse_create_bucket_acl(&req) {
            Err(ServerError::InvalidArgument { reason }) => {
                assert!(reason.contains("cannot be combined"));
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn parse_put_object_write_acl_rejects_acl_and_grants_together() {
        let canonical_id = s3_types::CanonicalUserId::from_principal("grantee-a");
        let req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![
                ("x-amz-acl".to_string(), "private".to_string()),
                (
                    "x-amz-grant-read".to_string(),
                    format!("id=\"{}\"", canonical_id.as_str()),
                ),
            ],
            b"data".to_vec(),
        );

        match parse_put_object_write_acl(&req) {
            Err(ServerError::InvalidArgument { reason }) => {
                assert!(reason.contains("cannot be combined"));
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn parse_acl_grants_rejects_malformed_header_value() {
        let req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![("x-amz-grant-read".to_string(), "id=".to_string())],
            vec![],
        );

        match parse_acl_grants(&req) {
            Err(ServerError::InvalidArgument { .. }) => {}
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn put_bucket_acl_accepts_header_grants_and_renders_them() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let canonical_id = s3_types::CanonicalUserId::from_principal("grantee-a");

        let put_req = new_req(
            http::Method::PUT,
            "/",
            "acl",
            {
                let mut headers = vec![(
                    "x-amz-grant-read-acp".to_string(),
                    format!("id=\"{}\"", canonical_id.as_str()),
                )];
                headers.extend(checksum_header_pairs(&[]));
                headers
            },
            vec![],
        );
        fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutBucketAcl {
                bucket: test_bucket_name("mybucket"),
            },
        )
        .unwrap();

        let get_req = new_req(http::Method::GET, "/", "acl", vec![], vec![]);
        let resp = fe
            .dispatch_routed(
                &get_req,
                &test_auth(),
                S3Operation::GetBucketAcl {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains(canonical_id.as_str()));
    }

    #[test]
    fn bucket_policy_put_get_delete_round_trip() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let policy = "{\n  \"Statement\": {\n    \"Resource\": [\"arn:aws:s3:::mybucket\"],\n    \"Action\": [\"s3:ListBucket\"],\n    \"Principal\": {\"AWS\": \"arn:aws:iam::123456789012:root\"},\n    \"Effect\": \"Allow\",\n    \"Sid\": \"One\"\n  },\n  \"Version\": \"2012-10-17\"\n}";
        let expected_policy = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"One\",\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::123456789012:root\"},\"Action\":\"s3:ListBucket\",\"Resource\":\"arn:aws:s3:::mybucket\"}]}";
        let put_req = new_req(
            http::Method::PUT,
            "/",
            "policy",
            checksum_header_pairs(policy.as_bytes()),
            policy.as_bytes().to_vec(),
        );
        let put_resp = fe
            .dispatch_routed(
                &put_req,
                &test_auth(),
                S3Operation::PutBucketPolicy {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(put_resp.status_code, 204);

        let get_req = new_req(http::Method::GET, "/", "policy", vec![], vec![]);
        let get_resp = fe
            .dispatch_routed(
                &get_req,
                &test_auth(),
                S3Operation::GetBucketPolicy {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(get_resp.status_code, 200);
        assert_eq!(String::from_utf8(get_resp.body).unwrap(), expected_policy);
        assert!(get_resp
            .headers
            .iter()
            .any(|(name, value)| { name == "Content-Type" && value == "application/json" }));

        let delete_req = new_req(http::Method::DELETE, "/", "policy", vec![], vec![]);
        let delete_resp = fe
            .dispatch_routed(
                &delete_req,
                &test_auth(),
                S3Operation::DeleteBucketPolicy {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(delete_resp.status_code, 204);

        match fe.dispatch_routed(
            &get_req,
            &test_auth(),
            S3Operation::GetBucketPolicy {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::NoSuchBucketPolicy { bucket }) => assert_eq!(bucket, "mybucket"),
            Err(e) => panic!("expected NoSuchBucketPolicy, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_bucket_policy_rejects_invalid_utf8() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let put_req = new_req(
            http::Method::PUT,
            "/",
            "policy",
            checksum_header_pairs(&[0xff, 0xfe, 0xfd]),
            vec![0xff, 0xfe, 0xfd],
        );
        match fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutBucketPolicy {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::InvalidArgument { reason }) => {
                assert!(reason.contains("bucket policy JSON body"));
            }
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_bucket_policy_rejects_invalid_json() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let put_req = new_req(
            http::Method::PUT,
            "/",
            "policy",
            checksum_header_pairs(b"{"),
            b"{".to_vec(),
        );
        match fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutBucketPolicy {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::MalformedPolicy { reason, .. }) => {
                assert!(reason.contains("Policies must be valid JSON"));
            }
            Err(e) => panic!("expected MalformedPolicy, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    #[allow(clippy::format_push_string)]
    fn put_bucket_policy_rejects_normalized_policy_over_20kb() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let mut policy = String::from("{\"Version\":\"2012-10-17\",\"Statement\":[");
        let mut statement_count = 0usize;
        while policy.len() <= auth::bucket_policy::MAX_BUCKET_POLICY_BYTES {
            if statement_count > 0 {
                policy.push(',');
            }
            policy.push_str(&format!(
                "{{\"Sid\":\"Stmt{statement_count:04}\",\"Effect\":\"Allow\",\"Principal\":{{\"AWS\":\"arn:aws:iam::123456789012:root\"}},\"Action\":\"s3:GetObject\",\"Resource\":\"arn:aws:s3:::mybucket/path-{statement_count:04}*\"}}"
            ));
            statement_count += 1;
        }
        policy.push_str("]}");
        let put_req = new_req(
            http::Method::PUT,
            "/",
            "policy",
            checksum_header_pairs(policy.as_bytes()),
            policy.into_bytes(),
        );
        match fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutBucketPolicy {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::MalformedPolicy { reason, .. }) => {
                assert_eq!(
                    reason,
                    format!(
                        "Normalized policy document exceeds the maximum allowed size of {} bytes",
                        auth::bucket_policy::MAX_BUCKET_POLICY_BYTES
                    )
                );
            }
            Err(e) => panic!("expected MalformedPolicy, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn get_bucket_policy_status_renders_xml() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        fe.coordinator
            .put_bucket_policy(&crate::coordinator::PutBucketPolicyRequest {
                bucket: test_bucket_request("mybucket"),
                config: r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::mybucket"}]}"#,
                confirm_remove_self_bucket_access: false,
            })
            .unwrap();

        let get_req = new_req(http::Method::GET, "/", "policyStatus", vec![], vec![]);
        let get_resp = fe
            .dispatch_routed(
                &get_req,
                &test_auth(),
                S3Operation::GetBucketPolicyStatus {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(get_resp.status_code, 200);
        let body = String::from_utf8(get_resp.body).unwrap();
        assert!(body.contains("<PolicyStatus"));
        assert!(body.contains("<IsPublic>true</IsPublic>"));
    }

    #[test]
    fn bucket_lifecycle_put_get_delete_round_trip() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let lifecycle = br#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <ID>expire-current</ID>
    <Filter><Prefix>logs/</Prefix></Filter>
    <Status>Enabled</Status>
    <Expiration><Days>3</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;
        let expected = xml::get_bucket_lifecycle_configuration_xml(
            &xml::parse_bucket_lifecycle_configuration_xml(lifecycle).unwrap(),
        );

        let put_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![("Content-MD5".to_string(), content_md5_value(lifecycle))],
            lifecycle.to_vec(),
        );
        let put_resp = fe
            .dispatch_routed(
                &put_req,
                &test_auth(),
                S3Operation::PutBucketLifecycle {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(put_resp.status_code, 200);

        let get_req = new_req(http::Method::GET, "/", "lifecycle", vec![], vec![]);
        let get_resp = fe
            .dispatch_routed(
                &get_req,
                &test_auth(),
                S3Operation::GetBucketLifecycle {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(get_resp.status_code, 200);
        assert_eq!(find_header(&get_resp, "Content-Type"), None);
        assert_eq!(
            String::from_utf8(get_resp.into_test_body_bytes().unwrap()).unwrap(),
            expected
        );

        let delete_req = new_req(http::Method::DELETE, "/", "lifecycle", vec![], vec![]);
        let delete_resp = fe
            .dispatch_routed(
                &delete_req,
                &test_auth(),
                S3Operation::DeleteBucketLifecycle {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(delete_resp.status_code, 204);

        match fe.dispatch_routed(
            &get_req,
            &test_auth(),
            S3Operation::GetBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::NoSuchLifecycleConfiguration { bucket }) => {
                assert_eq!(bucket, "mybucket");
            }
            Err(e) => panic!("expected NoSuchLifecycleConfiguration, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn bucket_lifecycle_put_without_ids_gets_generated_rule_ids() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let lifecycle = br#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <Filter><Prefix>test1/</Prefix></Filter>
    <Status>Enabled</Status>
    <Expiration><Days>31</Days></Expiration>
  </Rule>
  <Rule>
    <Filter><Prefix>test2/</Prefix></Filter>
    <Status>Enabled</Status>
    <Expiration><Days>120</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;

        let put_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![("Content-MD5".to_string(), content_md5_value(lifecycle))],
            lifecycle.to_vec(),
        );
        let put_resp = fe
            .dispatch_routed(
                &put_req,
                &test_auth(),
                S3Operation::PutBucketLifecycle {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(put_resp.status_code, 200);

        let get_req = new_req(http::Method::GET, "/", "lifecycle", vec![], vec![]);
        let get_resp = fe
            .dispatch_routed(
                &get_req,
                &test_auth(),
                S3Operation::GetBucketLifecycle {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(get_resp.status_code, 200);

        let body = String::from_utf8(get_resp.body).unwrap();
        let parsed = xml::parse_bucket_lifecycle_configuration_xml(body.as_bytes()).unwrap();
        assert_eq!(parsed.rules.len(), 2);
        let mut ids = std::collections::HashSet::new();
        for rule in &parsed.rules {
            let id = rule.id.as_deref().expect("generated lifecycle rule ID");
            assert!(!id.is_empty());
            assert!(ids.insert(id.to_string()), "duplicate generated ID {id}");
        }
    }

    #[test]
    fn get_bucket_lifecycle_absent_returns_no_such_lifecycle_configuration() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let get_req = new_req(http::Method::GET, "/", "lifecycle", vec![], vec![]);
        match fe.dispatch_routed(
            &get_req,
            &test_auth(),
            S3Operation::GetBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::NoSuchLifecycleConfiguration { bucket }) => {
                assert_eq!(bucket, "mybucket");
            }
            Err(e) => panic!("expected NoSuchLifecycleConfiguration, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_bucket_lifecycle_rejects_invalid_status() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let lifecycle = br#"<LifecycleConfiguration>
  <Rule>
    <Status>enabled</Status>
    <Expiration><Days>1</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;
        let put_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![("Content-MD5".to_string(), content_md5_value(lifecycle))],
            lifecycle.to_vec(),
        );
        match fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::MalformedXML { .. }) => {}
            Err(e) => panic!("expected MalformedXML, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_bucket_lifecycle_rejects_invalid_date_as_malformed_xml() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let lifecycle = br#"<LifecycleConfiguration>
  <Rule>
    <Status>Enabled</Status>
    <Expiration><Date>20200101</Date></Expiration>
  </Rule>
</LifecycleConfiguration>"#;
        let put_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![("Content-MD5".to_string(), content_md5_value(lifecycle))],
            lifecycle.to_vec(),
        );
        match fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::MalformedXML { .. }) => {}
            Err(e) => panic!("expected MalformedXML, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_bucket_lifecycle_missing_content_md5_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let lifecycle = br#"<LifecycleConfiguration>
  <Rule>
    <Status>Enabled</Status>
    <Expiration><Days>1</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;
        let put_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![],
            lifecycle.to_vec(),
        );
        match fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::InvalidRequest { reason }) => {
                assert_eq!(
                    reason,
                    "Missing required header for this request: Content-MD5"
                );
            }
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_bucket_lifecycle_invalid_content_md5_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let lifecycle = br#"<LifecycleConfiguration>
  <Rule>
    <Status>Enabled</Status>
    <Expiration><Days>1</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;
        let put_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![("Content-MD5".to_string(), "not-base64".to_string())],
            lifecycle.to_vec(),
        );
        match fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::InvalidDigest) => {}
            Err(e) => panic!("expected InvalidDigest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_bucket_lifecycle_bad_content_md5_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let lifecycle = br#"<LifecycleConfiguration>
  <Rule>
    <Status>Enabled</Status>
    <Expiration><Days>1</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;
        let put_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![(
                "Content-MD5".to_string(),
                "AAAAAAAAAAAAAAAAAAAAAA==".to_string(),
            )],
            lifecycle.to_vec(),
        );
        match fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::BadDigest) => {}
            Err(e) => panic!("expected BadDigest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_object_emits_lifecycle_expiration_header() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let lifecycle = br#"<LifecycleConfiguration>
  <Rule>
    <ID>expire-current</ID>
    <Filter><Prefix>logs/</Prefix></Filter>
    <Status>Enabled</Status>
    <Expiration><Days>1</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;
        let put_lifecycle_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![("Content-MD5".to_string(), content_md5_value(lifecycle))],
            lifecycle.to_vec(),
        );
        fe.dispatch_routed(
            &put_lifecycle_req,
            &test_auth(),
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        )
        .unwrap();

        let put_req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![],
            b"hello lifecycle".to_vec(),
        );
        let put_resp = fe
            .dispatch_routed(
                &put_req,
                &test_auth(),
                S3Operation::PutObject {
                    bucket: test_bucket_name("mybucket"),
                    key: "logs/app.txt".to_string(),
                },
            )
            .unwrap();
        let expiration = find_header(&put_resp, "x-amz-expiration").unwrap();
        assert!(expiration.contains("expiry-date=\""));
        assert!(expiration.contains("rule-id=\"expire-current\""));
    }

    #[test]
    fn get_object_explicit_current_version_suppresses_lifecycle_expiration_header() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        fe.coordinator
            .put_bucket_versioning(&crate::coordinator::PutBucketVersioningRequest {
                bucket: test_bucket_request("mybucket"),
                state: s3_types::BucketVersioningState::Enabled,
            })
            .unwrap();

        let lifecycle = br#"<LifecycleConfiguration>
  <Rule>
    <ID>expire-current</ID>
    <Filter><Prefix>logs/</Prefix></Filter>
    <Status>Enabled</Status>
    <Expiration><Days>1</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;
        let put_lifecycle_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![("Content-MD5".to_string(), content_md5_value(lifecycle))],
            lifecycle.to_vec(),
        );
        fe.dispatch_routed(
            &put_lifecycle_req,
            &test_auth(),
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        )
        .unwrap();

        let put_req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![],
            b"hello lifecycle".to_vec(),
        );
        let put_resp = fe
            .dispatch_routed(
                &put_req,
                &test_auth(),
                S3Operation::PutObject {
                    bucket: test_bucket_name("mybucket"),
                    key: "logs/app.txt".to_string(),
                },
            )
            .unwrap();
        let version_id = find_header(&put_resp, "x-amz-version-id")
            .expect("versioned put should return version id")
            .to_string();

        let get_req = new_req(
            http::Method::GET,
            "/",
            &format!("versionId={version_id}"),
            vec![],
            vec![],
        );
        let get_resp = fe
            .dispatch_routed(
                &get_req,
                &test_auth(),
                S3Operation::GetObject {
                    bucket: test_bucket_name("mybucket"),
                    key: "logs/app.txt".to_string(),
                },
            )
            .unwrap();
        assert!(find_header(&get_resp, "x-amz-expiration").is_none());
    }

    #[test]
    fn head_object_explicit_null_version_suppresses_lifecycle_expiration_header() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let lifecycle = br#"<LifecycleConfiguration>
  <Rule>
    <ID>expire-current</ID>
    <Filter><Prefix>logs/</Prefix></Filter>
    <Status>Enabled</Status>
    <Expiration><Days>1</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;
        let put_lifecycle_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![("Content-MD5".to_string(), content_md5_value(lifecycle))],
            lifecycle.to_vec(),
        );
        fe.dispatch_routed(
            &put_lifecycle_req,
            &test_auth(),
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        )
        .unwrap();

        let put_req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![],
            b"hello lifecycle".to_vec(),
        );
        fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutObject {
                bucket: test_bucket_name("mybucket"),
                key: "logs/app.txt".to_string(),
            },
        )
        .unwrap();

        let head_req = new_req(http::Method::HEAD, "/", "versionId=null", vec![], vec![]);
        let head_resp = fe
            .dispatch_routed(
                &head_req,
                &test_auth(),
                S3Operation::HeadObject {
                    bucket: test_bucket_name("mybucket"),
                    key: "logs/app.txt".to_string(),
                },
            )
            .unwrap();
        assert!(find_header(&head_resp, "x-amz-expiration").is_none());
    }

    #[test]
    fn multipart_lifecycle_abort_headers_are_emitted_on_create_and_list_parts() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let lifecycle = br#"<LifecycleConfiguration>
  <Rule>
    <ID>abort-stale</ID>
    <Filter><Prefix>uploads/</Prefix></Filter>
    <Status>Enabled</Status>
    <AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation></AbortIncompleteMultipartUpload>
  </Rule>
</LifecycleConfiguration>"#;
        let put_lifecycle_req = new_req(
            http::Method::PUT,
            "/",
            "lifecycle",
            vec![("Content-MD5".to_string(), content_md5_value(lifecycle))],
            lifecycle.to_vec(),
        );
        fe.dispatch_routed(
            &put_lifecycle_req,
            &test_auth(),
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        )
        .unwrap();

        let create_req = new_req(http::Method::POST, "/", "uploads", vec![], vec![]);
        let create_resp = fe
            .dispatch_routed(
                &create_req,
                &test_auth(),
                S3Operation::CreateMultipartUpload {
                    bucket: test_bucket_name("mybucket"),
                    key: "uploads/archive.bin".to_string(),
                },
            )
            .unwrap();
        assert!(find_header(&create_resp, "x-amz-abort-date").is_some());
        assert_eq!(
            find_header(&create_resp, "x-amz-abort-rule-id"),
            Some("abort-stale")
        );

        let create_body = response_body(create_resp);
        let body = std::str::from_utf8(&create_body).unwrap();
        let start = body.find("<UploadId>").unwrap() + "<UploadId>".len();
        let end = start + body[start..].find("</UploadId>").unwrap();
        let upload_id = &body[start..end];

        let list_req = new_req(
            http::Method::GET,
            "/",
            &format!("uploadId={upload_id}"),
            vec![],
            vec![],
        );
        let list_resp = fe
            .dispatch_routed(
                &list_req,
                &test_auth(),
                S3Operation::ListParts {
                    bucket: test_bucket_name("mybucket"),
                    key: "uploads/archive.bin".to_string(),
                },
            )
            .unwrap();
        assert!(find_header(&list_resp, "x-amz-abort-date").is_some());
        assert_eq!(
            find_header(&list_resp, "x-amz-abort-rule-id"),
            Some("abort-stale")
        );
    }

    #[test]
    fn put_object_accepts_header_grants_and_renders_them() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        fe.coordinator
            .create_bucket(&crate::coordinator::CreateBucketRequest {
                name: parse_bucket_name("mybucket").unwrap(),
                requester: crate::coordinator::test_helpers::requester("testuser"),
                namespace: BucketNamespace::Global,
                acl: crate::coordinator::CreateBucketAcl::DefaultPrivate,
                ownership: crate::coordinator::BucketObjectOwnership::ObjectWriter,
                object_lock_enabled: false,
            })
            .unwrap();
        let canonical_id = s3_types::CanonicalUserId::from_principal("grantee-a");

        let put_req = new_req(
            http::Method::PUT,
            "/",
            "",
            vec![(
                "x-amz-grant-read-acp".to_string(),
                format!("id=\"{}\"", canonical_id.as_str()),
            )],
            b"data".to_vec(),
        );
        fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutObject {
                bucket: test_bucket_name("mybucket"),
                key: "mykey".to_string(),
            },
        )
        .unwrap();

        let get_req = new_req(http::Method::GET, "/", "acl", vec![], vec![]);
        let resp = fe
            .dispatch_routed(
                &get_req,
                &test_auth(),
                S3Operation::GetObjectAcl {
                    bucket: test_bucket_name("mybucket"),
                    key: "mykey".to_string(),
                },
            )
            .unwrap();
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains(canonical_id.as_str()));
    }

    #[test]
    fn put_bucket_acl_accepts_authenticated_users_header_grant() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let put_req = new_req(
            http::Method::PUT,
            "/",
            "acl",
            {
                let mut headers = vec![(
                    "x-amz-grant-read".to_string(),
                    format!(
                        "uri=\"{}\"",
                        s3_types::AclGrantee::authenticated_users_uri()
                    ),
                )];
                headers.extend(checksum_header_pairs(&[]));
                headers
            },
            vec![],
        );
        fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutBucketAcl {
                bucket: test_bucket_name("mybucket"),
            },
        )
        .unwrap();

        let get_req = new_req(http::Method::GET, "/", "acl", vec![], vec![]);
        let resp = fe
            .dispatch_routed(
                &get_req,
                &test_auth(),
                S3Operation::GetBucketAcl {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        let body = String::from_utf8(resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains(s3_types::AclGrantee::authenticated_users_uri()));
    }

    #[test]
    fn create_bucket_with_object_lock_header_enables_bucket_object_lock() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        let req = new_req(
            http::Method::PUT,
            "/mybucket",
            "",
            vec![(
                "x-amz-bucket-object-lock-enabled".to_string(),
                "true".to_string(),
            )],
            vec![],
        );
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::CreateBucket {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
        assert_eq!(
            fe.coordinator
                .get_bucket_versioning(&test_bucket_request("mybucket"))
                .unwrap(),
            s3_types::BucketVersioningState::Enabled
        );
        assert_eq!(
            fe.coordinator
                .get_bucket_object_lock_configuration(&test_bucket_request("mybucket",))
                .unwrap(),
            s3_types::BucketObjectLockConfig {
                enabled: true,
                default_retention: None,
            }
        );
    }

    #[test]
    fn bucket_object_lock_operations_are_implemented() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        let create_req = new_req(http::Method::PUT, "/mybucket", "", vec![], vec![]);
        fe.dispatch_routed(
            &create_req,
            &test_auth(),
            S3Operation::CreateBucket {
                bucket: test_bucket_name("mybucket"),
            },
        )
        .unwrap();
        fe.coordinator
            .put_bucket_versioning(&crate::coordinator::PutBucketVersioningRequest {
                bucket: test_bucket_request("mybucket"),
                state: s3_types::BucketVersioningState::Enabled,
            })
            .unwrap();

        let put_req = new_req(
            http::Method::PUT,
            "/mybucket",
            "object-lock",
            checksum_header_pairs(
                br#"
                <ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                  <ObjectLockEnabled>Enabled</ObjectLockEnabled>
                  <Rule>
                    <DefaultRetention>
                      <Mode>GOVERNANCE</Mode>
                      <Days>1</Days>
                    </DefaultRetention>
                  </Rule>
                </ObjectLockConfiguration>
            "#,
            ),
            br#"
                <ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                  <ObjectLockEnabled>Enabled</ObjectLockEnabled>
                  <Rule>
                    <DefaultRetention>
                      <Mode>GOVERNANCE</Mode>
                      <Days>1</Days>
                    </DefaultRetention>
                  </Rule>
                </ObjectLockConfiguration>
            "#
            .to_vec(),
        );
        let put_resp = fe
            .dispatch_routed(
                &put_req,
                &test_auth(),
                S3Operation::PutBucketObjectLockConfiguration {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(put_resp.status_code, 200);

        let get_req = new_req(
            http::Method::GET,
            "/mybucket",
            "object-lock",
            vec![],
            vec![],
        );
        let get_resp = fe
            .dispatch_routed(
                &get_req,
                &test_auth(),
                S3Operation::GetBucketObjectLockConfiguration {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(get_resp.status_code, 200);
        let body = String::from_utf8(get_resp.into_test_body_bytes().unwrap()).unwrap();
        assert!(body.contains("<ObjectLockEnabled>Enabled</ObjectLockEnabled>"));
        assert!(body.contains("<Days>1</Days>"));
    }

    #[test]
    fn put_object_acl_accepts_write_header_grant() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let metadata = crate::metadata_blob::MetadataBlob::default();
        let system_metadata = server_core::system_metadata::SystemMetadata::default();
        fe.coordinator
            .put_object(&crate::coordinator::PutObjectRequest {
                object: test_object_request("mybucket", "mykey"),
                data: b"data",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: &crate::conditional::WriteCondition::default(),
                acl: crate::coordinator::PutObjectAcl::None.into(),
                policy_context: crate::coordinator::PutObjectPolicyContext::default(),
                object_lock: Default::default(),
                encryption: crate::coordinator::WriteEncryptionRequest::none(),
            })
            .unwrap();

        let canonical_id = s3_types::CanonicalUserId::from_principal("grantee-a");
        let put_req = new_req(
            http::Method::PUT,
            "/",
            "acl",
            {
                let mut headers = vec![(
                    "x-amz-grant-write".to_string(),
                    format!("id=\"{}\"", canonical_id.as_str()),
                )];
                headers.extend(checksum_header_pairs(&[]));
                headers
            },
            vec![],
        );
        let put_resp = fe
            .dispatch_routed(
                &put_req,
                &test_auth(),
                S3Operation::PutObjectAcl {
                    bucket: test_bucket_name("mybucket"),
                    key: "mykey".to_string(),
                },
            )
            .unwrap();
        assert_eq!(put_resp.status_code, 200);

        let acl = fe
            .coordinator
            .get_object_acl(&ObjectVersionRequest::new(
                parse_bucket_name("mybucket").unwrap(),
                parse_object_key("mykey").unwrap(),
                None,
                crate::coordinator::test_helpers::requester("testuser"),
                None,
            ))
            .unwrap();
        assert!(acl
            .acl_grants
            .allows_canonical_user(&canonical_id, s3_types::AclPermission::Write));
    }

    fn content_md5_value(body: &[u8]) -> String {
        use base64::Engine;

        let digest = md5_legacy::Md5::digest(body);
        base64::engine::general_purpose::STANDARD.encode(&digest[..])
    }

    fn checksum_crc32_value(body: &[u8]) -> String {
        use base64::Engine;

        let crc = checksum::crc32::checksum(body);
        base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes())
    }

    fn checksum_header_pairs(body: &[u8]) -> Vec<(String, String)> {
        vec![
            (
                "x-amz-sdk-checksum-algorithm".to_string(),
                "CRC32".to_string(),
            ),
            (
                "x-amz-checksum-crc32".to_string(),
                checksum_crc32_value(body),
            ),
        ]
    }

    fn assert_missing_request_checksum_rejected(
        method: http::Method,
        query: &str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
        op: S3Operation,
        expected_reason: &str,
    ) {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(method, "", query, headers, body);
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { reason }) => {
                assert_eq!(reason, expected_reason);
            }
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    fn assert_sdk_checksum_request_accepted(
        method: http::Method,
        query: &str,
        mut headers: Vec<(String, String)>,
        body: Vec<u8>,
        op: S3Operation,
    ) {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        headers.extend(checksum_header_pairs(&body));
        let req = new_req(method, "", query, headers, body);
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert!(
            matches!(resp.status_code, 200 | 204),
            "expected success, got status {}",
            resp.status_code
        );
    }

    // ── UploadPart validation ────────────────────────────────────────

    #[test]
    fn upload_part_missing_upload_id() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("partNumber=1");
        let op = S3Operation::UploadPart {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn upload_part_invalid_upload_id_returns_no_such_upload() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let invalid_upload_id = "a".repeat(storage::UPLOAD_ID_LEN + 1);
        let req = make_req(&format!("partNumber=1&uploadId={invalid_upload_id}"));
        let op = S3Operation::UploadPart {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::NoSuchUpload { upload_id }) => {
                assert_eq!(upload_id, invalid_upload_id);
            }
            Err(e) => panic!("expected NoSuchUpload, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn upload_part_invalid_part_number() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("partNumber=abc&uploadId=xyz");
        let op = S3Operation::UploadPart {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn prepare_streaming_part_sigv4_header_auth_requires_content_sha256() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![
                (
                    "authorization".to_string(),
                    "AWS4-HMAC-SHA256 Credential=test/20260318/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=deadbeef".to_string(),
                ),
                ("x-amz-date".to_string(), "20260318T000000Z".to_string()),
            ],
            b"hello world".to_vec(),
        );
        match fe.prepare_streaming_part(&req, "mybucket", "mykey", "upload-id", 1) {
            Err(ServerError::InvalidRequest { reason }) => {
                assert_eq!(
                    reason,
                    "Missing required header for this request: x-amz-content-sha256"
                );
            }
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_object_invalid_content_md5_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![("Content-MD5".to_string(), "not-base64".to_string())],
            b"hello world".to_vec(),
        );
        let op = S3Operation::PutObject {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidDigest) => {}
            Err(e) => panic!("expected InvalidDigest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_object_bad_content_md5_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![(
                "Content-MD5".to_string(),
                "AAAAAAAAAAAAAAAAAAAAAA==".to_string(),
            )],
            b"hello world".to_vec(),
        );
        let op = S3Operation::PutObject {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::BadDigest) => {}
            Err(e) => panic!("expected BadDigest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn put_object_without_checksum_allowed_when_object_lock_not_requested() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(http::Method::PUT, "", "", vec![], b"hello world".to_vec());
        let op = S3Operation::PutObject {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_bucket_versioning_missing_request_checksum_allowed() {
        let body =
            b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>".to_vec();
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let req = new_req(http::Method::PUT, "", "", vec![], body);
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutBucketVersioning {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_bucket_object_lock_configuration_missing_request_checksum_rejected() {
        let body = br#"<ObjectLockConfiguration>
  <ObjectLockEnabled>Enabled</ObjectLockEnabled>
  <Rule>
    <DefaultRetention>
      <Mode>GOVERNANCE</Mode>
      <Days>1</Days>
    </DefaultRetention>
  </Rule>
</ObjectLockConfiguration>"#
            .to_vec();
        assert_missing_request_checksum_rejected(
            http::Method::PUT,
            "",
            vec![],
            body,
            S3Operation::PutBucketObjectLockConfiguration {
                bucket: test_bucket_name("mybucket"),
            },
            "Missing required header for this request: Content-MD5 OR x-amz-checksum-*",
        );
    }

    #[test]
    fn put_bucket_encryption_missing_request_checksum_allowed() {
        let body = br#"<ServerSideEncryptionConfiguration>
  <Rule>
    <ApplyServerSideEncryptionByDefault>
      <SSEAlgorithm>AES256</SSEAlgorithm>
    </ApplyServerSideEncryptionByDefault>
  </Rule>
</ServerSideEncryptionConfiguration>"#
            .to_vec();
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let req = new_req(http::Method::PUT, "", "", vec![], body);
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutBucketEncryption {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_bucket_cors_missing_request_checksum_rejected() {
        let body = br#"<CORSConfiguration>
  <CORSRule>
    <AllowedMethod>GET</AllowedMethod>
    <AllowedOrigin>https://example.com</AllowedOrigin>
  </CORSRule>
</CORSConfiguration>"#
            .to_vec();
        assert_missing_request_checksum_rejected(
            http::Method::PUT,
            "",
            vec![],
            body,
            S3Operation::PutBucketCors {
                bucket: test_bucket_name("mybucket"),
            },
            "Missing required header for this request: Content-MD5 OR x-amz-checksum-*",
        );
    }

    #[test]
    fn put_bucket_tagging_missing_request_checksum_rejected() {
        let body =
            br#"<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>"#
                .to_vec();
        assert_missing_request_checksum_rejected(
            http::Method::PUT,
            "",
            vec![],
            body,
            S3Operation::PutBucketTagging {
                bucket: test_bucket_name("mybucket"),
            },
            "Missing required header for this request: Content-MD5 OR x-amz-checksum-*",
        );
    }

    #[test]
    fn put_bucket_abac_missing_request_checksum_rejected() {
        let body = br#"<AbacStatus><Status>Enabled</Status></AbacStatus>"#.to_vec();
        assert_missing_request_checksum_rejected(
            http::Method::PUT,
            "",
            vec![],
            body,
            S3Operation::PutBucketAbac {
                bucket: test_bucket_name("mybucket"),
            },
            "Missing required header for this request: Content-MD5 OR x-amz-checksum-*",
        );
    }

    #[test]
    fn put_bucket_abac_sdk_checksum_header_accepted() {
        let body = br#"<AbacStatus><Status>Enabled</Status></AbacStatus>"#.to_vec();
        assert_sdk_checksum_request_accepted(
            http::Method::PUT,
            "",
            vec![],
            body,
            S3Operation::PutBucketAbac {
                bucket: test_bucket_name("mybucket"),
            },
        );
    }

    #[test]
    fn put_bucket_abac_content_md5_accepted() {
        let body = br#"<AbacStatus><Status>Enabled</Status></AbacStatus>"#.to_vec();
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![("Content-MD5".to_string(), content_md5_value(&body))],
            body,
        );
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutBucketAbac {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_object_retention_missing_request_checksum_rejected() {
        let body = br#"<Retention>
  <Mode>GOVERNANCE</Mode>
  <RetainUntilDate>2099-01-01T00:00:00Z</RetainUntilDate>
</Retention>"#
            .to_vec();
        assert_missing_request_checksum_rejected(
            http::Method::PUT,
            "",
            vec![],
            body,
            S3Operation::PutObjectRetention {
                bucket: test_bucket_name("mybucket"),
                key: "mykey".to_string(),
            },
            "Missing required header for this request: Content-MD5 OR x-amz-checksum-*",
        );
    }

    #[test]
    fn put_object_legal_hold_missing_request_checksum_rejected() {
        let body = br#"<LegalHold><Status>ON</Status></LegalHold>"#.to_vec();
        assert_missing_request_checksum_rejected(
            http::Method::PUT,
            "",
            vec![],
            body,
            S3Operation::PutObjectLegalHold {
                bucket: test_bucket_name("mybucket"),
                key: "mykey".to_string(),
            },
            "Missing required header for this request: Content-MD5 OR x-amz-checksum-*",
        );
    }

    #[test]
    fn put_object_tagging_missing_request_checksum_allowed() {
        let body =
            br#"<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>"#
                .to_vec();
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let metadata = crate::metadata_blob::MetadataBlob::default();
        let system_metadata = server_core::system_metadata::SystemMetadata::default();
        fe.coordinator
            .put_object(&crate::coordinator::PutObjectRequest {
                object: test_object_request("mybucket", "mykey"),
                data: b"hello",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: &crate::conditional::WriteCondition::default(),
                acl: crate::coordinator::PutObjectAcl::None.into(),
                policy_context: crate::coordinator::PutObjectPolicyContext::default(),
                object_lock: Default::default(),
                encryption: crate::coordinator::WriteEncryptionRequest::none(),
            })
            .unwrap();
        let req = new_req(http::Method::PUT, "", "", vec![], body);
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutObjectTagging {
                    bucket: test_bucket_name("mybucket"),
                    key: "mykey".to_string(),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_object_acl_missing_request_checksum_allowed() {
        let canonical_id = s3_types::CanonicalUserId::from_principal("testuser");
        let body = format!(
            "<AccessControlPolicy><AccessControlList>\
             <Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">\
             <ID>{}</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant>\
             </AccessControlList></AccessControlPolicy>",
            canonical_id.as_str()
        )
        .into_bytes();
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let metadata = crate::metadata_blob::MetadataBlob::default();
        let system_metadata = server_core::system_metadata::SystemMetadata::default();
        fe.coordinator
            .put_object(&crate::coordinator::PutObjectRequest {
                object: test_object_request("mybucket", "mykey"),
                data: b"hello",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: &crate::conditional::WriteCondition::default(),
                acl: crate::coordinator::PutObjectAcl::None.into(),
                policy_context: crate::coordinator::PutObjectPolicyContext::default(),
                object_lock: Default::default(),
                encryption: crate::coordinator::WriteEncryptionRequest::none(),
            })
            .unwrap();
        let req = new_req(http::Method::PUT, "", "", vec![], body);
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutObjectAcl {
                    bucket: test_bucket_name("mybucket"),
                    key: "mykey".to_string(),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_object_acl_header_only_without_checksum_allowed() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let metadata = crate::metadata_blob::MetadataBlob::default();
        let system_metadata = server_core::system_metadata::SystemMetadata::default();
        fe.coordinator
            .put_object(&crate::coordinator::PutObjectRequest {
                object: test_object_request("mybucket", "mykey"),
                data: b"hello",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: &crate::conditional::WriteCondition::default(),
                acl: crate::coordinator::PutObjectAcl::None.into(),
                policy_context: crate::coordinator::PutObjectPolicyContext::default(),
                object_lock: Default::default(),
                encryption: crate::coordinator::WriteEncryptionRequest::none(),
            })
            .unwrap();

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![("x-amz-acl".to_string(), "private".to_string())],
            vec![],
        );
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutObjectAcl {
                    bucket: test_bucket_name("mybucket"),
                    key: "mykey".to_string(),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_object_acl_without_acl_payload_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let metadata = crate::metadata_blob::MetadataBlob::default();
        let system_metadata = server_core::system_metadata::SystemMetadata::default();
        fe.coordinator
            .put_object(&crate::coordinator::PutObjectRequest {
                object: test_object_request("mybucket", "mykey"),
                data: b"hello",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: &crate::conditional::WriteCondition::default(),
                acl: crate::coordinator::PutObjectAcl::None.into(),
                policy_context: crate::coordinator::PutObjectPolicyContext::default(),
                object_lock: Default::default(),
                encryption: crate::coordinator::WriteEncryptionRequest::none(),
            })
            .unwrap();

        let req = new_req(http::Method::PUT, "", "", vec![], vec![]);
        match fe.dispatch_routed(
            &req,
            &test_auth(),
            S3Operation::PutObjectAcl {
                bucket: test_bucket_name("mybucket"),
                key: "mykey".to_string(),
            },
        ) {
            Err(ServerError::InvalidArgument { reason }) => {
                assert_eq!(reason, "missing ACL XML body");
            }
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected InvalidArgument, got Ok"),
        }
    }

    #[test]
    fn put_bucket_public_access_block_missing_request_checksum_allowed() {
        let body = br#"<PublicAccessBlockConfiguration>
  <BlockPublicAcls>true</BlockPublicAcls>
  <IgnorePublicAcls>true</IgnorePublicAcls>
  <BlockPublicPolicy>true</BlockPublicPolicy>
  <RestrictPublicBuckets>true</RestrictPublicBuckets>
</PublicAccessBlockConfiguration>"#
            .to_vec();
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let req = new_req(http::Method::PUT, "", "", vec![], body);
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutBucketPublicAccessBlock {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_bucket_ownership_controls_missing_request_checksum_allowed() {
        let body = br#"<OwnershipControls><Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule></OwnershipControls>"#
            .to_vec();
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let req = new_req(http::Method::PUT, "", "", vec![], body);
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutBucketOwnershipControls {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_bucket_policy_missing_request_checksum_allowed() {
        let body = br#"{"Version":"2012-10-17","Statement":[{"Sid":"AllowOwnerList","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::test-account-id:root"},"Action":"s3:ListBucket","Resource":"arn:aws:s3:::mybucket"}]}"#.to_vec();
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let req = new_req(http::Method::PUT, "", "", vec![], body);
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutBucketPolicy {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 204);
    }

    #[test]
    fn put_bucket_acl_missing_request_checksum_allowed() {
        let canonical_id = s3_types::CanonicalUserId::from_principal("testuser");
        let body = format!(
            "<AccessControlPolicy><AccessControlList>\
             <Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">\
             <ID>{}</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant>\
             </AccessControlList></AccessControlPolicy>",
            canonical_id.as_str()
        )
        .into_bytes();
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let req = new_req(http::Method::PUT, "", "", vec![], body);
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutBucketAcl {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_bucket_acl_header_only_without_checksum_allowed() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![("x-amz-acl".to_string(), "private".to_string())],
            vec![],
        );
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::PutBucketAcl {
                    bucket: test_bucket_name("mybucket"),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn put_bucket_acl_anonymous_request_denied_before_checksum_validation() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![("x-amz-acl".to_string(), "private".to_string())],
            vec![],
        );
        match fe.dispatch_routed(
            &req,
            &auth::AuthContext::anonymous(),
            S3Operation::PutBucketAcl {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::AccessDenied) => {}
            Err(err) => panic!("expected AccessDenied, got Err({err:?})"),
            Ok(_) => panic!("expected AccessDenied, got Ok"),
        }
    }

    #[test]
    fn put_bucket_lifecycle_sdk_checksum_header_accepted() {
        let body = br#"<LifecycleConfiguration>
  <Rule>
    <ID>rule1</ID>
    <Filter><Prefix>logs/</Prefix></Filter>
    <Status>Enabled</Status>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#
            .to_vec();
        assert_sdk_checksum_request_accepted(
            http::Method::PUT,
            "",
            vec![],
            body,
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        );
    }

    #[test]
    fn put_bucket_policy_sdk_checksum_header_accepted() {
        let body = br#"{"Version":"2012-10-17","Statement":[{"Sid":"AllowOwnerList","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::test-account-id:root"},"Action":"s3:ListBucket","Resource":"arn:aws:s3:::mybucket"}]}"#.to_vec();
        assert_sdk_checksum_request_accepted(
            http::Method::PUT,
            "",
            vec![],
            body,
            S3Operation::PutBucketPolicy {
                bucket: test_bucket_name("mybucket"),
            },
        );
    }

    #[test]
    fn put_bucket_acl_sdk_checksum_header_accepted() {
        assert_sdk_checksum_request_accepted(
            http::Method::PUT,
            "",
            vec![("x-amz-acl".to_string(), "private".to_string())],
            vec![],
            S3Operation::PutBucketAcl {
                bucket: test_bucket_name("mybucket"),
            },
        );
    }

    #[test]
    fn request_checksum_algorithm_without_value_header_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "lifecycle",
            vec![(
                "x-amz-sdk-checksum-algorithm".to_string(),
                "CRC32".to_string(),
            )],
            br#"<LifecycleConfiguration><Rule><ID>rule1</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>"#.to_vec(),
        );
        match fe.dispatch_routed(
            &req,
            &test_auth(),
            S3Operation::PutBucketLifecycle {
                bucket: test_bucket_name("mybucket"),
            },
        ) {
            Err(ServerError::InvalidRequestHostId { reason }) => {
                assert_eq!(
                    reason,
                    "x-amz-sdk-checksum-algorithm specified, but no corresponding x-amz-checksum-* or x-amz-trailer headers were found."
                );
            }
            Err(e) => panic!("expected InvalidRequestHostId, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn prepare_streaming_put_with_object_lock_requires_checksum() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![
                (
                    "x-amz-object-lock-mode".to_string(),
                    "GOVERNANCE".to_string(),
                ),
                (
                    "x-amz-object-lock-retain-until-date".to_string(),
                    "2099-01-01T00:00:00Z".to_string(),
                ),
            ],
            b"hello world".to_vec(),
        );
        match fe.prepare_streaming_put(&req, "mybucket", "mykey", false) {
            Err(ServerError::InvalidRequest { reason }) => {
                assert_eq!(
                    reason,
                    "Content-MD5 OR x-amz-checksum- HTTP header is required for Put Object requests with Object Lock parameters"
                );
            }
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn prepare_streaming_put_sigv4_header_auth_requires_content_sha256() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![
                (
                    "authorization".to_string(),
                    "AWS4-HMAC-SHA256 Credential=test/20260318/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=deadbeef".to_string(),
                ),
                ("x-amz-date".to_string(), "20260318T000000Z".to_string()),
            ],
            b"hello world".to_vec(),
        );
        match fe.prepare_streaming_put(&req, "mybucket", "mykey", false) {
            Err(ServerError::InvalidRequest { reason }) => {
                assert_eq!(
                    reason,
                    "Missing required header for this request: x-amz-content-sha256"
                );
            }
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn prepare_streaming_put_without_content_length_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![(
                "x-amz-content-sha256".to_string(),
                sha256_hex(b"").to_string(),
            )],
            Vec::new(),
        );
        match fe.prepare_streaming_put(&req, "mybucket", "mykey", false) {
            Err(ServerError::MissingContentLength) => {}
            Err(e) => panic!("expected MissingContentLength, got {e:?}"),
            Ok(_) => panic!("expected MissingContentLength, got Ok"),
        }
    }

    #[test]
    fn prepare_streaming_put_denies_anonymous_write_to_private_bucket() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![("content-length".to_string(), "11".to_string())],
            b"hello world".to_vec(),
        );
        match fe.prepare_streaming_put(&req, "mybucket", "mykey", false) {
            Err(ServerError::AccessDenied) => {}
            Err(err) => panic!("expected AccessDenied, got {err:?}"),
            Ok(_) => panic!("expected AccessDenied, got Ok"),
        }
    }

    #[test]
    fn prepare_streaming_post_object_denied_policy_does_not_create_session() {
        let tmp = test_util::tempdir();
        let mut fe = setup_frontend(tmp.path());
        fe.credentials.add(
            TEST_SIGV4_ACCESS_KEY.to_string(),
            SecretKey::new(TEST_SIGV4_SECRET.to_string()),
        );
        create_sigv4_test_bucket(&fe.coordinator, "mybucket", false);

        let req = new_req(
            http::Method::POST,
            "/mybucket",
            "",
            vec![(
                "host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            )],
            vec![],
        );
        let fields = signed_post_policy_fields(
            "mybucket",
            "mykey",
            &[r#"{"acl":"private"}"#],
            &[("acl", "public-read")],
        );

        match fe.prepare_streaming_post_object(&req, "mybucket", &fields, Some("upload.txt")) {
            Err(ServerError::PostPolicyAccessDenied { reason }) => {
                assert!(
                    reason.contains("'acl'"),
                    "unexpected denial reason: {reason}"
                );
            }
            Err(err) => panic!("expected PostPolicyAccessDenied, got {err:?}"),
            Ok(_) => panic!("expected PostPolicyAccessDenied, got Ok"),
        }

        assert_eq!(
            fe.coordinator.scavenge_stale_sessions(0),
            0,
            "policy-denied POST should not create a stream session"
        );
    }

    #[test]
    fn prepare_streaming_post_object_does_not_set_object_creation_operation_policy_condition() {
        let tmp = test_util::tempdir();
        let mut fe = setup_frontend(tmp.path());
        fe.credentials.add(
            TEST_SIGV4_ACCESS_KEY.to_string(),
            SecretKey::new(TEST_SIGV4_SECRET.to_string()),
        );
        create_sigv4_test_bucket(&fe.coordinator, "mybucket", false);
        fe.coordinator
            .put_bucket_policy(&crate::coordinator::PutBucketPolicyRequest {
                bucket: crate::coordinator::BucketRequest::new(
                    test_bucket_name("mybucket"),
                    crate::coordinator::test_helpers::requester(TEST_SIGV4_ACCESS_KEY),
                    None,
                ),
                config: r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::mybucket/*","Condition":{"Bool":{"s3:ObjectCreationOperation":"true"},"Null":{"s3:if-none-match":"true"}}}]}"#,
                confirm_remove_self_bucket_access: false,
            })
            .unwrap();

        let req = new_req(
            http::Method::POST,
            "/mybucket",
            "",
            vec![(
                "host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            )],
            vec![],
        );
        let fields = signed_post_policy_fields("mybucket", "mykey", &[], &[]);

        let ctx = fe
            .prepare_streaming_post_object(&req, "mybucket", &fields, Some("upload.txt"))
            .unwrap();
        fe.abort_streaming_post_object(&ctx);

        assert_eq!(
            fe.coordinator.scavenge_stale_sessions(0),
            0,
            "aborted POST should not leave a stream session"
        );
    }

    #[test]
    fn prepare_streaming_post_object_passes_if_none_match_to_bucket_policy() {
        let tmp = test_util::tempdir();
        let mut fe = setup_frontend(tmp.path());
        fe.credentials.add(
            TEST_SIGV4_ACCESS_KEY.to_string(),
            SecretKey::new(TEST_SIGV4_SECRET.to_string()),
        );
        create_sigv4_test_bucket(&fe.coordinator, "mybucket", false);
        fe.coordinator
            .put_bucket_policy(&crate::coordinator::PutBucketPolicyRequest {
                bucket: crate::coordinator::BucketRequest::new(
                    test_bucket_name("mybucket"),
                    crate::coordinator::test_helpers::requester(TEST_SIGV4_ACCESS_KEY),
                    None,
                ),
                config: r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::mybucket/*","Condition":{"Null":{"s3:if-none-match":"true"}}}]}"#,
                confirm_remove_self_bucket_access: false,
            })
            .unwrap();

        let missing_header_req = new_req(
            http::Method::POST,
            "/mybucket",
            "",
            vec![(
                "host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            )],
            vec![],
        );
        let fields = signed_post_policy_fields("mybucket", "mykey", &[], &[]);
        match fe.prepare_streaming_post_object(
            &missing_header_req,
            "mybucket",
            &fields,
            Some("upload.txt"),
        ) {
            Err(ServerError::AccessDenied) => {}
            Err(err) => panic!("expected AccessDenied, got {err:?}"),
            Ok(_) => panic!("expected AccessDenied, got Ok"),
        }

        let header_req = new_req(
            http::Method::POST,
            "/mybucket",
            "",
            vec![
                (
                    "host".to_string(),
                    "examplebucket.s3.amazonaws.com".to_string(),
                ),
                ("if-none-match".to_string(), "*".to_string()),
            ],
            vec![],
        );
        let ctx = fe
            .prepare_streaming_post_object(&header_req, "mybucket", &fields, Some("upload.txt"))
            .unwrap();
        fe.abort_streaming_post_object(&ctx);
    }

    #[test]
    fn prepare_streaming_post_object_wrong_region_returns_post_scope_error_before_policy_denial() {
        let tmp = test_util::tempdir();
        let mut fe = setup_frontend(tmp.path());
        fe.credentials.add(
            TEST_SIGV4_ACCESS_KEY.to_string(),
            SecretKey::new(TEST_SIGV4_SECRET.to_string()),
        );
        create_sigv4_test_bucket(&fe.coordinator, "mybucket", false);

        let req = new_req(
            http::Method::POST,
            "/mybucket",
            "",
            vec![(
                "host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            )],
            vec![],
        );
        let fields = signed_post_policy_fields_for_region(
            "mybucket",
            "mykey",
            "us-west-2",
            &[r#"{"acl":"private"}"#],
            &[("acl", "public-read")],
        );

        match fe.prepare_streaming_post_object(&req, "mybucket", &fields, Some("upload.txt")) {
            Err(ServerError::Auth(auth::AuthError::InvalidCredentialScopeRegion {
                provided_region,
                expected_region,
                ..
            })) => {
                assert_eq!(provided_region, "us-west-2");
                assert_eq!(expected_region, "us-east-1");
            }
            Err(err) => panic!("expected InvalidCredentialScopeRegion, got {err:?}"),
            Ok(_) => panic!("expected InvalidCredentialScopeRegion, got Ok"),
        }

        assert_eq!(
            fe.coordinator.scavenge_stale_sessions(0),
            0,
            "wrong-region POST should not create a stream session"
        );
    }

    #[test]
    fn prepare_streaming_put_with_object_lock_accepts_sdk_checksum_header() {
        let tmp = test_util::tempdir();
        let mut fe = setup_frontend(tmp.path());
        fe.credentials.add(
            TEST_SIGV4_ACCESS_KEY.to_string(),
            SecretKey::new(TEST_SIGV4_SECRET.to_string()),
        );
        create_sigv4_test_bucket(&fe.coordinator, "mybucket", true);

        let body = b"hello world".to_vec();
        let mut headers = vec![
            (
                "x-amz-object-lock-mode".to_string(),
                "GOVERNANCE".to_string(),
            ),
            (
                "x-amz-object-lock-retain-until-date".to_string(),
                "2099-01-01T00:00:00Z".to_string(),
            ),
        ];
        headers.extend(checksum_header_pairs(&body));
        let req = signed_v4_put_req(&body, headers);
        let ctx = fe
            .prepare_streaming_put(&req, "mybucket", "mykey", false)
            .unwrap();
        assert_eq!(ctx.key().as_str(), "mykey");
    }

    #[test]
    fn start_streaming_put_session_uses_prepare_authorization_result() {
        let tmp = test_util::tempdir();
        let mut fe = setup_frontend(tmp.path());
        fe.credentials.add(
            TEST_SIGV4_ACCESS_KEY.to_string(),
            SecretKey::new(TEST_SIGV4_SECRET.to_string()),
        );
        create_sigv4_test_bucket(&fe.coordinator, "mybucket", false);

        let body = b"hello world".to_vec();
        let req = signed_v4_put_req(&body, vec![]);
        let ctx = fe
            .prepare_streaming_put(&req, "mybucket", "mykey", false)
            .unwrap();

        fe.coordinator
            .put_bucket_policy(&crate::coordinator::PutBucketPolicyRequest {
                bucket: crate::coordinator::BucketRequest::new(
                    test_bucket_name("mybucket"),
                    crate::coordinator::test_helpers::requester(TEST_SIGV4_ACCESS_KEY),
                    None,
                ),
                config: r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":"AKID"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::mybucket/*"}]}"#,
                confirm_remove_self_bucket_access: false,
            })
            .unwrap();

        let session_id = fe.start_streaming_put_session(&ctx).unwrap();
        fe.coordinator
            .abort_stream_put_session_with_storage_node(
                &ctx.storage_node,
                ctx.bucket(),
                ctx.key(),
                &session_id,
            )
            .unwrap();
    }

    #[test]
    fn put_object_invalid_acl_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![(
                "x-amz-acl".to_string(),
                "definitely-not-a-real-acl".to_string(),
            )],
            b"hello world".to_vec(),
        );
        let op = S3Operation::PutObject {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    // ── CompleteMultipartUpload validation ────────────────────────────

    #[test]
    fn complete_multipart_missing_upload_id() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("");
        let op = S3Operation::CompleteMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn complete_multipart_invalid_upload_id_returns_no_such_upload() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let invalid_upload_id = "a".repeat(storage::UPLOAD_ID_LEN + 1);
        let req = make_req(&format!("uploadId={invalid_upload_id}"));
        let op = S3Operation::CompleteMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::NoSuchUpload { upload_id }) => {
                assert_eq!(upload_id, invalid_upload_id);
            }
            Err(e) => panic!("expected NoSuchUpload, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    #[allow(clippy::format_push_string)]
    fn complete_multipart_multiple_checksum_headers_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        let xml = "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>\"x\"</ETag></Part>\
             </CompleteMultipartUpload>"
            .to_string();
        let req = new_req(
            http::Method::GET,
            "",
            &format!("uploadId={upload_id}"),
            vec![
                ("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string()),
                ("x-amz-checksum-sha256".to_string(), "BBBBBB==".to_string()),
            ],
            xml.into_bytes(),
        );
        let op = S3Operation::CompleteMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn complete_multipart_ignores_checksum_algorithm_mismatch() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        let xml = "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>\"x\"</ETag></Part>\
             </CompleteMultipartUpload>"
            .to_string();
        let req = new_req(
            http::Method::GET,
            "",
            &format!("uploadId={upload_id}"),
            vec![
                // CompleteMultipartUpload uses the concrete checksum header
                // name, not x-amz-checksum-algorithm, to identify the value.
                ("x-amz-checksum-algorithm".to_string(), "SHA256".to_string()),
                ("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string()),
            ],
            xml.into_bytes(),
        );
        let op = S3Operation::CompleteMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        fe.dispatch_routed(&req, &test_auth(), op).unwrap();
    }

    #[test]
    fn complete_multipart_duplicate_same_checksum_header_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        let xml = "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>\"x\"</ETag></Part>\
             </CompleteMultipartUpload>"
            .to_string();
        let req = new_req(
            http::Method::GET,
            "",
            &format!("uploadId={upload_id}"),
            vec![
                ("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string()),
                ("x-amz-checksum-crc32".to_string(), "BBBBBB==".to_string()),
            ],
            xml.into_bytes(),
        );
        let op = S3Operation::CompleteMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::DuplicateChecksumHeader { header, value }) => {
                assert_eq!(header, "x-amz-checksum-crc32");
                assert_eq!(value, "BBBBBB==");
            }
            Err(e) => panic!("expected DuplicateChecksumHeader, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn complete_multipart_ignores_checksum_algorithm_header() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        let xml = "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>\"x\"</ETag></Part>\
             </CompleteMultipartUpload>"
            .to_string();
        let req = new_req(
            http::Method::GET,
            "",
            &format!("uploadId={upload_id}"),
            vec![
                ("x-amz-checksum-algorithm".to_string(), "CRC32".to_string()),
                ("x-amz-checksum-algorithm".to_string(), "SHA256".to_string()),
                ("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string()),
            ],
            xml.into_bytes(),
        );
        let op = S3Operation::CompleteMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        fe.dispatch_routed(&req, &test_auth(), op).unwrap();
    }

    #[test]
    fn complete_multipart_checksum_algo_mismatch_upload_rejected() {
        // Upload created with CRC32 but complete sends SHA256 checksum header.
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", Some("CRC32"));
        // Upload a part so complete has something to work with.
        let part_data = vec![0u8; 1024];
        let etag = stream_upload_part(
            &fe,
            "mybucket",
            "k",
            &upload_id,
            1,
            &part_data,
            Some(ChecksumAlgorithm::Crc32),
        )
        .etag;

        let xml = format!(
            "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part>\
             </CompleteMultipartUpload>"
        );
        let req = new_req(
            http::Method::GET,
            "",
            &format!("uploadId={upload_id}"),
            vec![("x-amz-checksum-sha256".to_string(), "AAAAAA==".to_string())],
            xml.into_bytes(),
        );
        let op = S3Operation::CompleteMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::CompleteMultipartMissingPartChecksum {
                algorithm,
                part_number,
            }) => {
                assert_eq!(algorithm, "crc32");
                assert_eq!(part_number, 1);
            }
            Err(e) => panic!("expected CompleteMultipartMissingPartChecksum, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn complete_multipart_invalid_mp_object_size_header_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let upload_id = create_upload_with_checksum(&fe, "mybucket", "k", None);
        let xml = "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>\"x\"</ETag></Part>\
             </CompleteMultipartUpload>"
            .to_string();
        let req = new_req(
            http::Method::GET,
            "",
            &format!("uploadId={upload_id}"),
            vec![(
                "x-amz-mp-object-size".to_string(),
                "not-a-number".to_string(),
            )],
            xml.into_bytes(),
        );
        let op = S3Operation::CompleteMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    // ── AbortMultipartUpload validation ──────────────────────────────

    #[test]
    fn abort_multipart_missing_upload_id() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("");
        let op = S3Operation::AbortMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn abort_multipart_invalid_upload_id_returns_no_such_upload() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let invalid_upload_id = "a".repeat(storage::UPLOAD_ID_LEN + 1);
        let req = make_req(&format!("uploadId={invalid_upload_id}"));
        let op = S3Operation::AbortMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::NoSuchUpload { upload_id }) => {
                assert_eq!(upload_id, invalid_upload_id);
            }
            Err(e) => panic!("expected NoSuchUpload, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    // ── ListParts validation ─────────────────────────────────────────

    #[test]
    fn list_parts_missing_upload_id() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("");
        let op = S3Operation::ListParts {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn list_parts_invalid_part_number_marker() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let upload_id = create_upload_with_checksum(&fe, "mybucket", "mykey", None);

        let req = make_req(&format!("uploadId={upload_id}&part-number-marker=xyz"));
        let op = S3Operation::ListParts {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn list_parts_invalid_max_parts() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let upload_id = create_upload_with_checksum(&fe, "mybucket", "mykey", None);

        let req = make_req(&format!("uploadId={upload_id}&max-parts=notanumber"));
        let op = S3Operation::ListParts {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn list_parts_clamps_max_parts_to_s3_limit() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let upload_id = create_upload_with_checksum(&fe, "mybucket", "mykey", None);

        let req = make_req(&format!("uploadId={upload_id}&max-parts=4294967295"));
        let op = S3Operation::ListParts {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body = String::from_utf8(response_body(resp)).unwrap();
        assert!(
            body.contains("<MaxParts>1000</MaxParts>"),
            "unexpected ListParts body: {body}"
        );
        assert!(
            !body.contains("<MaxParts>4294967295</MaxParts>"),
            "unexpected ListParts body: {body}"
        );
    }

    #[test]
    fn list_parts_invalid_upload_id_returns_no_such_upload() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("uploadId=abc");
        let op = S3Operation::ListParts {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::NoSuchUpload { upload_id }) => assert_eq!(upload_id, "abc"),
            Err(e) => panic!("expected NoSuchUpload, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    // ── GetObjectAttributes header validation ──────────────────────

    #[test]
    fn get_object_attributes_invalid_max_parts() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::GET,
            "",
            "",
            vec![
                (
                    "x-amz-object-attributes".to_string(),
                    "ObjectParts".to_string(),
                ),
                ("x-amz-max-parts".to_string(), "notanumber".to_string()),
            ],
            vec![],
        );
        let op = S3Operation::GetObjectAttributes {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn get_object_attributes_invalid_part_number_marker() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::GET,
            "",
            "",
            vec![
                (
                    "x-amz-object-attributes".to_string(),
                    "ObjectParts".to_string(),
                ),
                ("x-amz-part-number-marker".to_string(), "xyz".to_string()),
            ],
            vec![],
        );
        let op = S3Operation::GetObjectAttributes {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn get_object_attributes_preserves_max_parts_above_s3_list_limit() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        let parts = [(1, vec![b'a'; 5 * 1024 * 1024]), (2, b"tail".to_vec())];
        do_multipart_upload(&fe, "mybucket", "mykey", &parts, Some("CRC32"));

        let req = new_req(
            http::Method::GET,
            "",
            "",
            vec![
                (
                    "x-amz-object-attributes".to_string(),
                    "ObjectParts".to_string(),
                ),
                ("x-amz-max-parts".to_string(), "4294967295".to_string()),
            ],
            vec![],
        );
        let op = S3Operation::GetObjectAttributes {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body = String::from_utf8(response_body(resp)).unwrap();
        assert!(
            body.contains("<MaxParts>4294967295</MaxParts>"),
            "unexpected GetObjectAttributes body: {body}"
        );
    }

    // ── End-to-end multipart upload flow ────────────────────────────

    #[test]
    fn multipart_upload_e2e_quoted_etags() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        // 1. CreateMultipartUpload
        let req = make_req("uploads");
        let op = S3Operation::CreateMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body_bytes = response_body(resp);
        let body = std::str::from_utf8(&body_bytes).unwrap();
        // Extract upload_id from <UploadId>...</UploadId>
        let uid_start = body.find("<UploadId>").unwrap() + "<UploadId>".len();
        let uid_end = uid_start + body[uid_start..].find("</UploadId>").unwrap();
        let upload_id = &body[uid_start..uid_end];
        assert!(!upload_id.is_empty());

        // 2. UploadPart — single part (last part is exempt from min-size)
        let part_body = vec![0u8; 1024];
        let etag =
            stream_upload_part(&fe, "mybucket", "mykey", upload_id, 1, &part_body, None).etag;
        // ETag must be quoted
        assert!(
            etag.starts_with('"') && etag.ends_with('"'),
            "ETag not quoted: {etag}"
        );

        // 3. CompleteMultipartUpload with quoted ETag from UploadPart response
        let complete_xml = format!(
            "<CompleteMultipartUpload>\
               <Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part>\
             </CompleteMultipartUpload>"
        );
        let req = new_req(
            http::Method::GET,
            "",
            &format!("uploadId={upload_id}"),
            vec![],
            complete_xml.into_bytes(),
        );
        let op = S3Operation::CompleteMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body_bytes = response_body(resp);
        let body = std::str::from_utf8(&body_bytes).unwrap();
        assert!(
            body.contains("<CompleteMultipartUploadResult"),
            "missing result element: {body}"
        );
        assert!(body.contains("<Key>mykey</Key>"), "missing key: {body}");
        assert!(body.contains("<ETag>"), "missing etag: {body}");
    }

    // ── ListMultipartUploads validation ──────────────────────────────

    #[test]
    fn list_multipart_uploads_invalid_max_uploads() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("uploads&max-uploads=abc");
        let op = S3Operation::ListMultipartUploads {
            bucket: test_bucket_name("mybucket"),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn list_multipart_uploads_clamps_max_uploads_to_s3_limit() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("uploads&max-uploads=4294967295");
        let op = S3Operation::ListMultipartUploads {
            bucket: test_bucket_name("mybucket"),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body = String::from_utf8(response_body(resp)).unwrap();
        assert!(
            body.contains("<MaxUploads>1000</MaxUploads>"),
            "unexpected ListMultipartUploads body: {body}"
        );
        assert!(
            !body.contains("<MaxUploads>4294967295</MaxUploads>"),
            "unexpected ListMultipartUploads body: {body}"
        );
    }

    #[test]
    fn list_multipart_uploads_invalid_upload_id_marker() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("uploads&upload-id-marker=bad");
        let op = S3Operation::ListMultipartUploads {
            bucket: test_bucket_name("mybucket"),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { reason }) => {
                assert_eq!(reason, "Invalid uploadId marker");
            }
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn list_object_versions_invalid_max_keys() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("versions&max-keys=abc");
        let op = S3Operation::ListObjectVersions {
            bucket: test_bucket_name("mybucket"),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn list_object_versions_echoes_oversized_max_keys() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("versions&max-keys=5000");
        let op = S3Operation::ListObjectVersions {
            bucket: test_bucket_name("mybucket"),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);

        let body = String::from_utf8(response_body(resp)).unwrap();
        assert!(
            body.contains("<MaxKeys>5000</MaxKeys>"),
            "unexpected body: {body}"
        );
    }

    #[test]
    fn list_object_versions_invalid_version_id_marker() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("versions&version-id-marker=abc");
        let op = S3Operation::ListObjectVersions {
            bucket: test_bucket_name("mybucket"),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidVersionId {
                argument_name,
                argument_value,
            }) => {
                assert_eq!(argument_name, "version-id-marker");
                assert_eq!(argument_value, "abc");
            }
            Err(e) => panic!("expected InvalidVersionId, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn list_object_versions_rejects_version_id_marker_without_key_marker() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = make_req("versions&version-id-marker=1");
        let op = S3Operation::ListObjectVersions {
            bucket: test_bucket_name("mybucket"),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { reason }) => {
                assert_eq!(
                    reason,
                    "A version-id marker cannot be specified without a key marker."
                );
            }
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn anonymous_get_rejects_response_override_params() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        let req = new_req(
            http::Method::GET,
            "/mybucket/key",
            "response-content-type=text%2Fplain",
            vec![],
            vec![],
        );
        let op = S3Operation::GetObject {
            bucket: test_bucket_name("mybucket"),
            key: "key".to_string(),
        };
        match fe.dispatch_routed(&req, &auth::AuthContext::anonymous(), op) {
            Err(ServerError::InvalidRequest { reason }) => {
                assert_eq!(
                    reason,
                    "Request specific response headers cannot be used for anonymous GET requests."
                );
            }
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    // ── CreateMultipartUpload checksum validation ───────────────────

    #[test]
    fn create_multipart_rejects_acl_on_bucket_owner_enforced_bucket() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");
        fe.coordinator
            .put_bucket_ownership_controls(&crate::coordinator::PutBucketOwnershipControlsRequest {
                bucket: test_bucket_request("mybucket"),
                config: storage::BucketOwnershipControls {
                    object_ownership: storage::BucketObjectOwnership::BucketOwnerEnforced,
                },
            })
            .unwrap();

        let req = new_req(
            http::Method::GET,
            "",
            "uploads",
            vec![("x-amz-acl".to_string(), "public-read".to_string())],
            vec![],
        );
        let op = S3Operation::CreateMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::AccessControlListNotSupported) => {}
            Err(e) => panic!("expected AccessControlListNotSupported, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn create_multipart_invalid_checksum_algorithm() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::GET,
            "",
            "uploads",
            vec![("x-amz-checksum-algorithm".to_string(), "BOGUS".to_string())],
            vec![],
        );
        let op = S3Operation::CreateMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequestHostId { reason }) => {
                assert_eq!(reason, ServerError::UNSUPPORTED_CHECKSUM_ALGORITHM_MESSAGE);
            }
            Err(e) => panic!("expected InvalidRequestHostId, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn create_multipart_invalid_checksum_type() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::GET,
            "",
            "uploads",
            vec![
                ("x-amz-checksum-algorithm".to_string(), "CRC32".to_string()),
                ("x-amz-checksum-type".to_string(), "INVALID".to_string()),
            ],
            vec![],
        );
        let op = S3Operation::CreateMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequestHostId { reason }) => {
                assert_eq!(reason, "Value for x-amz-checksum-type header is invalid.");
            }
            Err(e) => panic!("expected InvalidRequestHostId, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn create_multipart_checksum_type_without_algorithm() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::GET,
            "",
            "uploads",
            vec![("x-amz-checksum-type".to_string(), "COMPOSITE".to_string())],
            vec![],
        );
        let op = S3Operation::CreateMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequestHostId { reason }) => {
                assert_eq!(
                    reason,
                    "The x-amz-checksum-type header can only be used with the x-amz-checksum-algorithm header."
                );
            }
            Err(e) => panic!("expected InvalidRequestHostId, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn create_multipart_sha_full_object_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::GET,
            "",
            "uploads",
            vec![
                ("x-amz-checksum-algorithm".to_string(), "SHA256".to_string()),
                ("x-amz-checksum-type".to_string(), "FULL_OBJECT".to_string()),
            ],
            vec![],
        );
        let op = S3Operation::CreateMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequestHostId { reason }) => {
                assert_eq!(
                    reason,
                    "The FULL_OBJECT checksum type cannot be used with the sha256 checksum algorithm."
                );
            }
            Err(e) => panic!("expected InvalidRequestHostId, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn create_multipart_with_checksum_reports_fields_in_headers_only() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::GET,
            "",
            "uploads",
            vec![
                ("x-amz-checksum-algorithm".to_string(), "CRC32".to_string()),
                ("x-amz-checksum-type".to_string(), "FULL_OBJECT".to_string()),
            ],
            vec![],
        );
        let op = S3Operation::CreateMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        assert_eq!(
            find_header(&resp, "x-amz-checksum-algorithm"),
            Some("CRC32")
        );
        assert_eq!(
            find_header(&resp, "x-amz-checksum-type"),
            Some("FULL_OBJECT")
        );
        let body_bytes = response_body(resp);
        let body = std::str::from_utf8(&body_bytes).unwrap();
        // AWS reports checksum configuration only in headers; the
        // InitiateMultipartUploadResult body carries just Bucket/Key/UploadId.
        assert!(
            !body.contains("ChecksumAlgorithm"),
            "unexpected ChecksumAlgorithm in body: {body}"
        );
        assert!(
            !body.contains("ChecksumType"),
            "unexpected ChecksumType in body: {body}"
        );
    }

    #[test]
    fn create_multipart_crc32_composite_accepted() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::GET,
            "",
            "uploads",
            vec![
                ("x-amz-checksum-algorithm".to_string(), "CRC32".to_string()),
                ("x-amz-checksum-type".to_string(), "COMPOSITE".to_string()),
            ],
            vec![],
        );
        let op = S3Operation::CreateMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        assert_eq!(
            find_header(&resp, "x-amz-checksum-algorithm"),
            Some("CRC32")
        );
        assert_eq!(find_header(&resp, "x-amz-checksum-type"), Some("COMPOSITE"));
    }

    #[test]
    fn create_multipart_algorithm_only_defaults_type() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(
            http::Method::GET,
            "",
            "uploads",
            vec![("x-amz-checksum-algorithm".to_string(), "SHA256".to_string())],
            vec![],
        );
        let op = S3Operation::CreateMultipartUpload {
            bucket: test_bucket_name("mybucket"),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        assert_eq!(
            find_header(&resp, "x-amz-checksum-algorithm"),
            Some("SHA256")
        );
        assert_eq!(find_header(&resp, "x-amz-checksum-type"), Some("COMPOSITE"));
    }

    // ── UploadPart checksum validation ──────────────────────────────

    /// Helper: create a multipart upload with optional checksum algorithm, return `upload_id`.
    fn create_upload_with_checksum(
        fe: &HttpFrontend,
        bucket: &str,
        key: &str,
        algo: Option<&str>,
    ) -> String {
        let mut headers = Vec::new();
        if let Some(a) = algo {
            headers.push(("x-amz-checksum-algorithm".to_string(), a.to_string()));
        }
        let req = new_req(http::Method::GET, "", "uploads", headers, vec![]);
        let op = S3Operation::CreateMultipartUpload {
            bucket: test_bucket_name(bucket),
            key: key.to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        let body_bytes = response_body(resp);
        let body = std::str::from_utf8(&body_bytes).unwrap();
        let start = body.find("<UploadId>").unwrap() + "<UploadId>".len();
        let end = start + body[start..].find("</UploadId>").unwrap();
        body[start..end].to_string()
    }

    fn compute_checksum_for_test(algo: ChecksumAlgorithm, data: &[u8]) -> RawChecksum {
        checksum::compute_checksum(algo, data)
    }

    #[test]
    fn validate_checksum_headers_accepts_all_algorithms() {
        use base64::Engine;

        let body = b"new checksum algorithms";
        for algorithm in ChecksumAlgorithm::ALL {
            let checksum = checksum::compute_checksum(algorithm, body);
            let encoded = base64::engine::general_purpose::STANDARD.encode(checksum.bytes());
            let req = new_req(
                http::Method::PUT,
                "",
                "",
                vec![(algorithm.header_name().to_string(), encoded)],
                body.to_vec(),
            );
            validate_checksum_headers(&req, true).unwrap();
        }
    }

    #[test]
    fn validate_checksum_headers_rejects_duplicate_same_header() {
        use base64::Engine;

        let body = b"duplicate checksum header";
        let algorithm = ChecksumAlgorithm::Crc32;
        let checksum = checksum::compute_checksum(algorithm, body);
        let encoded = base64::engine::general_purpose::STANDARD.encode(checksum.bytes());
        let req = new_req(
            http::Method::PUT,
            "",
            "",
            vec![
                (algorithm.header_name().to_string(), encoded.clone()),
                (algorithm.header_name().to_string(), "AAAAAA==".to_string()),
            ],
            body.to_vec(),
        );

        match validate_checksum_headers(&req, true) {
            Err(ServerError::DuplicateChecksumHeader { header, value }) => {
                assert_eq!(header, "x-amz-checksum-crc32");
                assert_eq!(value, "AAAAAA==");
            }
            other => panic!("expected duplicate header rejection, got {other:?}"),
        }
    }

    #[test]
    fn post_checksum_claim_from_fields_accepts_sha512() {
        use base64::Engine;

        let checksum = checksum::compute_checksum(ChecksumAlgorithm::Sha512, b"post-body");
        let encoded = base64::engine::general_purpose::STANDARD.encode(checksum.bytes());
        let fields = vec![
            ("key".to_string(), "mykey".to_string()),
            ("x-amz-checksum-algorithm".to_string(), "SHA512".to_string()),
            ("x-amz-checksum-sha512".to_string(), encoded),
        ];
        let claim = post_checksum_claim_from_fields(&fields)
            .unwrap()
            .expect("checksum field should be parsed");
        assert_eq!(claim.algorithm(), ChecksumAlgorithm::Sha512);
        assert_eq!(claim.expected_bytes(), checksum.bytes());
    }

    #[test]
    fn post_checksum_claim_from_fields_rejects_algorithm_mismatch() {
        let fields = vec![
            ("key".to_string(), "mykey".to_string()),
            ("x-amz-checksum-algorithm".to_string(), "SHA512".to_string()),
            (
                "x-amz-checksum-md5".to_string(),
                "AAAAAAAAAAAAAAAAAAAAAA==".to_string(),
            ),
        ];
        assert!(post_checksum_claim_from_fields(&fields).is_err());
    }

    fn stream_upload_part(
        fe: &HttpFrontend,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        data: &[u8],
        checksum_algorithm: Option<ChecksumAlgorithm>,
    ) -> crate::coordinator::UploadPartResult {
        let requester = crate::coordinator::Requester::from_auth(&test_auth())
            .with_request_epoch_seconds(Some(storage::clock::current_time_millis() / 1_000));
        let bucket_name = test_bucket_name(bucket);
        let upload_id = parse_present_upload_id(upload_id).unwrap();
        let storage_node = fe.coordinator.storage_node_for_request();
        let session = fe
            .coordinator
            .begin_stream_part_with_storage_node(
                &storage_node,
                &BeginStreamPartRequest {
                    upload: multipart_object_request(
                        &bucket_name,
                        key,
                        upload_id.clone(),
                        requester.clone(),
                        None,
                    )
                    .unwrap(),
                    part_number,
                    policy_context: crate::coordinator::PutObjectPolicyContext::default(),
                    sse_customer: None,
                },
            )
            .unwrap();
        let result = (|| {
            use base64::Engine;

            for (segment_index, chunk) in data
                .chunks(crate::coordinator::INTERNAL_SEGMENT_SIZE)
                .enumerate()
            {
                fe.coordinator.append_stream_part_data_with_storage_node(
                    &storage_node,
                    &crate::coordinator::AppendStreamPartRequest {
                        bucket: parse_bucket_name(bucket).unwrap(),
                        key: parse_object_key(key).unwrap(),
                        upload_id: &upload_id,
                        session_id: &session.session_id,
                        part_number,
                        segment_index: segment_index as u32,
                        data: chunk,
                        sse_customer: None,
                    },
                )?;
            }
            let computed_checksum =
                checksum_algorithm.map(|algo| compute_checksum_for_test(algo, data));
            let claimed_checksum = computed_checksum.as_ref().map(|expected| {
                let encoded = base64::engine::general_purpose::STANDARD.encode(expected.bytes());
                ChecksumClaim::from_base64(expected.algorithm(), &encoded)
                    .expect("checksum helper must round-trip through base64")
            });
            fe.coordinator.finalize_stream_part_with_storage_node(
                &storage_node,
                FinalizeStreamPartRequest {
                    upload: multipart_object_request(
                        &bucket_name,
                        key,
                        upload_id.clone(),
                        requester,
                        None,
                    )
                    .unwrap(),
                    session_id: &session.session_id,
                    part_number,
                    crc64: checksum::crc64::checksum(data),
                    total_size: data.len() as u64,
                    claimed_checksum: claimed_checksum.as_ref(),
                    computed_checksum,
                },
            )
        })();
        if result.is_err() {
            let _ = fe.coordinator.abort_stream_part_session_with_storage_node(
                &storage_node,
                &parse_bucket_name(bucket).unwrap(),
                &parse_object_key(key).unwrap(),
                &session.session_id,
            );
        }
        result.unwrap()
    }

    // ── GET ?partNumber=N tests ─────────────────────────────────────

    /// Helper: do a full multipart upload through the live streaming coordinator path.
    #[allow(clippy::format_push_string)]
    fn do_multipart_upload(
        fe: &HttpFrontend,
        bucket: &str,
        key: &str,
        parts: &[(u32, Vec<u8>)],
        algo: Option<&str>,
    ) {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD;
        let upload_id = create_upload_with_checksum(fe, bucket, key, algo);
        let checksum_algorithm = algo.map(|name| ChecksumAlgorithm::parse(name).unwrap());
        let mut part_info: Vec<(u32, String, Option<String>)> = Vec::new();
        for (part_number, data) in parts {
            let result = stream_upload_part(
                fe,
                bucket,
                key,
                &upload_id,
                *part_number,
                data,
                checksum_algorithm,
            );
            let checksum_b64 = result
                .checksum
                .as_ref()
                .map(|checksum| b64.encode(checksum.bytes()));
            part_info.push((*part_number, result.etag, checksum_b64));
        }
        let mut xml_parts = String::new();
        for (pn, etag, cksum) in &part_info {
            xml_parts.push_str(&format!(
                "<Part><PartNumber>{pn}</PartNumber><ETag>{etag}</ETag>"
            ));
            if let (Some(a), Some(val)) = (algo, cksum) {
                let algo_enum = ChecksumAlgorithm::parse(a).unwrap();
                let elem = algo_enum.xml_element_name();
                xml_parts.push_str(&format!("<{elem}>{val}</{elem}>"));
            }
            xml_parts.push_str("</Part>");
        }
        let xml = format!("<CompleteMultipartUpload>{xml_parts}</CompleteMultipartUpload>");
        let req = new_req(
            http::Method::GET,
            "",
            &format!("uploadId={upload_id}"),
            vec![],
            xml.into_bytes(),
        );
        let op = S3Operation::CompleteMultipartUpload {
            bucket: test_bucket_name(bucket),
            key: key.to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn get_object_part_multipart() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        // 5 MiB minimum for non-final parts
        let part1 = vec![0xAA; 5 * 1024 * 1024];
        let part2 = vec![0xBB; 5 * 1024 * 1024];
        let part3 = vec![0xCC; 100];
        do_multipart_upload(
            &fe,
            "mybucket",
            "k",
            &[(1, part1.clone()), (2, part2.clone()), (3, part3.clone())],
            None,
        );

        // GET partNumber=2
        let req = make_req("partNumber=2");
        let op = S3Operation::GetObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 206);

        // Verify Content-Range
        let content_range = resp
            .headers
            .iter()
            .find(|(k, _)| k == "Content-Range")
            .map(|(_, v)| v.as_str())
            .unwrap();
        let total = 5 * 1024 * 1024 + 5 * 1024 * 1024 + 100;
        let start = 5 * 1024 * 1024;
        let end = 2 * 5 * 1024 * 1024 - 1;
        assert_eq!(content_range, format!("bytes {start}-{end}/{total}"));

        // Verify x-amz-mp-parts-count
        let parts_count = resp
            .headers
            .iter()
            .find(|(k, _)| k == "x-amz-mp-parts-count")
            .map(|(_, v)| v.as_str())
            .unwrap();
        assert_eq!(parts_count, "3");

        let content_length_count = resp
            .headers
            .iter()
            .filter(|(k, _)| k.eq_ignore_ascii_case("Content-Length"))
            .count();
        assert_eq!(content_length_count, 1);

        // Verify data
        assert_eq!(resp.into_test_body_bytes().unwrap(), part2);
    }

    #[test]
    fn get_object_part_invalid_zero() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        // PUT a simple object
        let req = new_req(http::Method::GET, "", "", vec![], b"hello".to_vec());
        let op = S3Operation::PutObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        fe.dispatch_routed(&req, &test_auth(), op).unwrap();

        // partNumber=0 → InvalidArgument
        let req = make_req("partNumber=0");
        let op = S3Operation::GetObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn get_object_part_out_of_range() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let part1 = vec![0xAA; 5 * 1024 * 1024];
        let part2 = vec![0xBB; 5 * 1024 * 1024];
        let part3 = vec![0xCC; 100];
        do_multipart_upload(
            &fe,
            "mybucket",
            "k",
            &[(1, part1), (2, part2), (3, part3)],
            None,
        );

        // partNumber=99 on a 3-part object → 416 InvalidPartNumber
        let req = make_req("partNumber=99");
        let op = S3Operation::GetObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidPartNumber {
                part_number: 99,
                parts_count: 3,
            }) => {}
            Err(e) => panic!("expected InvalidPartNumber, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn get_object_part_and_range_rejected_together() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let part1 = vec![0xAA; 5 * 1024 * 1024];
        let part2 = vec![0xBB; 100];
        do_multipart_upload(&fe, "mybucket", "k", &[(1, part1), (2, part2)], None);

        let req = new_req(
            http::Method::GET,
            "",
            "partNumber=2",
            vec![("Range".to_string(), "bytes=0-1".to_string())],
            vec![],
        );
        let op = S3Operation::GetObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { reason }) => {
                assert_eq!(
                    reason,
                    "Cannot specify both Range header and partNumber query parameter"
                );
            }
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn get_object_part_non_multipart() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let data = b"hello world";
        let req = new_req(http::Method::GET, "", "", vec![], data.to_vec());
        let op = S3Operation::PutObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        fe.dispatch_routed(&req, &test_auth(), op).unwrap();

        // partNumber=1 on inline object → 206 with full data
        let req = make_req("partNumber=1");
        let op = S3Operation::GetObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 206);

        let parts_count = resp
            .headers
            .iter()
            .find(|(k, _)| k == "x-amz-mp-parts-count")
            .map(|(_, v)| v.as_str())
            .unwrap();
        assert_eq!(parts_count, "1");

        let content_range = resp
            .headers
            .iter()
            .find(|(k, _)| k == "Content-Range")
            .map(|(_, v)| v.as_str())
            .unwrap();
        assert_eq!(
            content_range,
            format!("bytes 0-{}/{}", data.len() - 1, data.len())
        );

        assert_eq!(resp.into_test_body_bytes().unwrap(), data);
    }

    #[test]
    fn get_object_part_non_multipart_out_of_range() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(http::Method::GET, "", "", vec![], b"hello".to_vec());
        let op = S3Operation::PutObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        fe.dispatch_routed(&req, &test_auth(), op).unwrap();

        // partNumber=2 on non-multipart → 416 InvalidPartNumber
        let req = make_req("partNumber=2");
        let op = S3Operation::GetObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidPartNumber {
                part_number: 2,
                parts_count: 1,
            }) => {}
            Err(e) => panic!("expected InvalidPartNumber, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn get_object_part_with_checksum() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let part1 = vec![0xAA; 5 * 1024 * 1024];
        let part2 = vec![0xBB; 100];
        do_multipart_upload(
            &fe,
            "mybucket",
            "k",
            &[(1, part1), (2, part2)],
            Some("CRC32"),
        );

        // GET partNumber=1 — checksum always emitted for part GETs
        let req = make_req("partNumber=1");
        let op = S3Operation::GetObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 206);

        // Should have per-part checksum header (always, not just with ENABLED)
        let has_checksum = resp
            .headers
            .iter()
            .any(|(k, _)| k == "x-amz-checksum-crc32");
        assert!(has_checksum, "expected x-amz-checksum-crc32 header");

        // Should also have checksum-type header
        let has_type = resp.headers.iter().any(|(k, _)| k == "x-amz-checksum-type");
        assert!(has_type, "expected x-amz-checksum-type header");
    }

    // ── aws-chunked decode edge cases ──────────────────────────────────

    #[test]
    fn signed_streaming_without_context_returns_signature_mismatch() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        // Auth context claims signed streaming but has no streaming context.
        let auth = auth::AuthContext {
            mode: auth::AuthMode::HeaderSigV4,
            access_key_id: Some("AKID".to_string()),
            account: Some(auth::AccountIdentity::from_principal("testuser")),
            authorization_profile: auth::AuthorizationProfile::Standard,
            request_epoch_secs: Some(0),
            signing_region: Some("us-east-1".to_string()),
            streaming: None, // missing!
        };

        let req = new_req(
            http::Method::PUT,
            "/mybucket/key",
            "",
            vec![
                (
                    "x-amz-content-sha256".to_string(),
                    "STREAMING-AWS4-HMAC-SHA256-PAYLOAD".to_string(),
                ),
                ("content-encoding".to_string(), "aws-chunked".to_string()),
                ("x-amz-decoded-content-length".to_string(), "5".to_string()),
            ],
            b"5\r\nhello\r\n0\r\n\r\n".to_vec(),
        );

        match fe.maybe_decode_chunked(&req, &auth) {
            Err(ServerError::Auth(auth::AuthError::SignatureMismatch { .. })) => {} // expected
            other => panic!("expected SignatureMismatch, got {:?}", other.err()),
        }
    }

    #[test]
    fn non_numeric_decoded_content_length_returns_400() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        let req = new_req(
            http::Method::PUT,
            "/mybucket/key",
            "",
            vec![
                (
                    "x-amz-content-sha256".to_string(),
                    "STREAMING-UNSIGNED-PAYLOAD-TRAILER".to_string(),
                ),
                ("content-encoding".to_string(), "aws-chunked".to_string()),
                (
                    "x-amz-decoded-content-length".to_string(),
                    "not-a-number".to_string(),
                ),
                (
                    "x-amz-trailer".to_string(),
                    "x-amz-checksum-crc32".to_string(),
                ),
            ],
            b"5\r\nhello\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n".to_vec(),
        );

        match fe.maybe_decode_chunked(&req, &test_auth()) {
            Err(ServerError::InvalidRequest { .. }) => {} // expected
            other => panic!("expected InvalidRequest, got {:?}", other.err()),
        }
    }

    #[test]
    fn unsigned_streaming_with_non_aws_content_encoding_decodes_and_preserves_header() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        let req = new_req(
            http::Method::PUT,
            "/mybucket/key",
            "",
            vec![
                (
                    "x-amz-content-sha256".to_string(),
                    "STREAMING-UNSIGNED-PAYLOAD-TRAILER".to_string(),
                ),
                ("content-encoding".to_string(), "gzip".to_string()),
                ("x-amz-decoded-content-length".to_string(), "5".to_string()),
                (
                    "x-amz-trailer".to_string(),
                    "x-amz-checksum-crc32".to_string(),
                ),
            ],
            b"5\r\nhello\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n".to_vec(),
        );

        match fe.reject_streaming_fallthrough(&req) {
            Err(ServerError::InvalidRequest { .. }) => {}
            other => panic!("expected streaming-path rejection, got {:?}", other),
        }

        let decoded = fe
            .maybe_decode_chunked(&req, &test_auth())
            .expect("decode should succeed")
            .expect("streaming body should decode");
        assert_eq!(decoded.header("content-encoding"), Some("gzip"));
        assert_eq!(decoded.body, b"hello");
    }

    #[test]
    fn ecdsa_streaming_token_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        let req = new_req(
            http::Method::PUT,
            "/mybucket/key",
            "",
            vec![
                (
                    "x-amz-content-sha256".to_string(),
                    "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER".to_string(),
                ),
                ("content-encoding".to_string(), "aws-chunked".to_string()),
                ("x-amz-decoded-content-length".to_string(), "5".to_string()),
                (
                    "x-amz-trailer".to_string(),
                    "x-amz-checksum-crc32".to_string(),
                ),
            ],
            b"5\r\nhello\r\n0\r\nx-amz-checksum-crc32:AAAA\r\n\r\n".to_vec(),
        );

        match fe.maybe_decode_chunked(&req, &test_auth()) {
            Err(ServerError::UnsupportedStreamingToken { .. }) => {} // expected
            other => panic!("expected UnsupportedStreamingToken, got {:?}", other.err()),
        }
    }

    // ── DeleteObjects version-id validation ────────────────────────────

    #[test]
    fn delete_objects_invalid_version_id_returns_per_object_no_such_version() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let xml = br#"<?xml version="1.0"?>
<Delete>
  <Object><Key>key1</Key><VersionId>not-a-number</VersionId></Object>
</Delete>"#;
        let req = new_req(
            http::Method::POST,
            "/mybucket",
            "delete",
            vec![("Content-MD5".to_string(), content_md5_value(xml))],
            xml.to_vec(),
        );
        let op = S3Operation::DeleteObjects {
            bucket: test_bucket_name("mybucket"),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body = String::from_utf8(response_body(resp)).unwrap();
        assert!(body.contains("<Error><Key>key1</Key><VersionId>not-a-number</VersionId><Code>NoSuchVersion</Code><Message>The specified version does not exist.</Message></Error>"), "unexpected body: {body}");
    }

    #[test]
    fn delete_objects_null_version_id_accepted() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let xml = br#"<?xml version="1.0"?>
<Delete>
  <Object><Key>key1</Key><VersionId>null</VersionId></Object>
</Delete>"#;
        let req = new_req(
            http::Method::POST,
            "/mybucket",
            "delete",
            vec![("Content-MD5".to_string(), content_md5_value(xml))],
            xml.to_vec(),
        );
        let op = S3Operation::DeleteObjects {
            bucket: test_bucket_name("mybucket"),
        };
        // "null" is a valid version ID — should not error on parsing
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Ok(_) => {}
            Err(e) => panic!("expected Ok, got {e:?}"),
        }
    }

    #[test]
    fn delete_objects_missing_content_md5_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let xml = br#"<?xml version="1.0"?>
<Delete>
  <Object><Key>key1</Key></Object>
</Delete>"#;
        let req = new_req(
            http::Method::POST,
            "/mybucket",
            "delete",
            vec![],
            xml.to_vec(),
        );
        let op = S3Operation::DeleteObjects {
            bucket: test_bucket_name("mybucket"),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn delete_objects_invalid_content_md5_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let xml = br#"<?xml version="1.0"?>
<Delete>
  <Object><Key>key1</Key></Object>
</Delete>"#;
        let req = new_req(
            http::Method::POST,
            "/mybucket",
            "delete",
            vec![("Content-MD5".to_string(), "not-base64".to_string())],
            xml.to_vec(),
        );
        let op = S3Operation::DeleteObjects {
            bucket: test_bucket_name("mybucket"),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidDigest) => {}
            Err(e) => panic!("expected InvalidDigest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn delete_objects_bad_content_md5_rejected() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let xml = br#"<?xml version="1.0"?>
<Delete>
  <Object><Key>key1</Key></Object>
</Delete>"#;
        let req = new_req(
            http::Method::POST,
            "/mybucket",
            "delete",
            vec![(
                "Content-MD5".to_string(),
                "AAAAAAAAAAAAAAAAAAAAAA==".to_string(),
            )],
            xml.to_vec(),
        );
        let op = S3Operation::DeleteObjects {
            bucket: test_bucket_name("mybucket"),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::BadDigest) => {}
            Err(e) => panic!("expected BadDigest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    // ── Abort-on-error path tests ──────────────────────────────────────

    #[test]
    fn put_object_abort_cleans_up_session_on_precondition_failure() {
        // Write an object, then PutObject with If-None-Match:* so finalize
        // fails with PreconditionFailed.  The handler's abort path must
        // clean up the streaming session so no session is left behind.
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        // First put succeeds.
        let req = new_req(http::Method::GET, "", "", vec![], b"hello".to_vec());
        let op = S3Operation::PutObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        fe.dispatch_routed(&req, &test_auth(), op).unwrap();

        // Second put with If-None-Match:* must fail.
        let req2 = new_req(
            http::Method::GET,
            "",
            "",
            vec![("if-none-match".to_string(), "*".to_string())],
            b"world".to_vec(),
        );
        let op2 = S3Operation::PutObject {
            bucket: test_bucket_name("mybucket"),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req2, &test_auth(), op2) {
            Err(ServerError::PreconditionFailed { .. }) => {}
            Err(e) => panic!("expected PreconditionFailed, got {e:?}"),
            Ok(_) => panic!("expected PreconditionFailed, got Ok"),
        }

        // No leaked streaming sessions.
        assert_eq!(
            fe.coordinator.scavenge_stale_sessions(0),
            0,
            "streaming session leaked after PutObject precondition failure"
        );
    }
}
