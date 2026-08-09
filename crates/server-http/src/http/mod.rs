/// HTTP frontend: parses requests, authenticates, dispatches to coordinator.
pub mod chunked;
pub mod conditional;
pub mod multipart;
pub mod request;
pub mod response;
pub mod router;
pub mod serve;
mod sts;
pub mod xml;

use std::time::{Instant, SystemTime, UNIX_EPOCH};
use std::{
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll},
};

use auth::{authenticate_request, AuthContext, AuthMode, IdentityProvider};
use bytes::Bytes;
use hyper::body::{Body, Frame, Incoming, SizeHint};

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
use request::{S3Request, TransportSecurity};
use response::{ErrorDiagnostic, S3Response, WireResponseIds};
use router::{
    route_service, EndpointKind, S3ControlOperation, S3ControlRouteError, S3Operation, ServiceKind,
    ServiceOperation, ServiceRouteError,
};
use s3_types::{
    parse_account_regional_bucket_name, requires_sigv4, BucketLifecycleConfiguration,
    BucketNamespace, LegalHoldStatus, ObjectLockMode, ObjectLockState, ObjectRetention,
    StoredLegalHoldStatus, VersionId, WebsiteRedirectLocation, WebsiteRedirectLocationError,
    WEBSITE_REDIRECT_LOCATION_HEADER_NAME,
};
use server_core::sse::{
    SseCustomerRequest, SseCustomerWriteContext, SSE_CUSTOMER_ALGORITHM, SSE_C_CUSTOMER_KEY_LEN,
};
use server_core::system_metadata::{
    is_checksum_algorithm_header_name, is_checksum_value_header_name,
    is_system_metadata_header_name, SystemMetadata,
};
use storage::{BucketName, ManagedEncryptionAlgorithm, ObjectKey, SessionId, UploadId};
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
    let mut trace_bytes = [0u8; 16];
    argmin_crypto::random::fill(&mut trace_bytes).expect("system randomness is available");

    let mut request_id = String::with_capacity(16);
    let mut random_bytes = [0u8; 32];
    while request_id.len() < 16 {
        argmin_crypto::random::fill(&mut random_bytes).expect("system randomness is available");
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

    let mut bytes = [0u8; 32];
    argmin_crypto::random::fill(&mut bytes).expect("system randomness is available");
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
    raw.filter(|value| !value.is_empty())
        .map(|value| {
            UploadId::try_from(value).map_err(|_| ServerError::InvalidArgumentValue {
                reason: "Invalid uploadId marker".to_string(),
                argument_name: "upload-id-marker".to_string(),
                argument_value: value.to_string(),
            })
        })
        .transpose()
}

fn parse_optional_s3_list_integer(
    raw: Option<impl AsRef<str>>,
    argument_name: &str,
) -> Result<Option<u32>, ServerError> {
    let Some(value) = raw.map(|value| value.as_ref().to_string()) else {
        return Ok(None);
    };
    if value.is_empty() {
        return Ok(None);
    }

    match value.parse::<i32>() {
        Ok(parsed) if parsed >= 0 => Ok(Some(parsed as u32)),
        Ok(_) => Err(ServerError::InvalidArgumentValue {
            reason: format!("Argument {argument_name} must be an integer between 0 and 2147483647"),
            argument_name: argument_name.to_string(),
            argument_value: value,
        }),
        Err(_) => Err(ServerError::InvalidArgumentValue {
            reason: format!("Provided {argument_name} not an integer or within integer range"),
            argument_name: argument_name.to_string(),
            argument_value: value,
        }),
    }
}

fn validate_list_encoding_type(raw: Option<&str>) -> Result<(), ServerError> {
    match raw {
        None | Some("url") => Ok(()),
        Some(value) => Err(ServerError::InvalidArgumentValue {
            reason: "Invalid Encoding Method specified in Request".to_string(),
            argument_name: "encoding-type".to_string(),
            argument_value: value.to_string(),
        }),
    }
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

fn validate_untag_resource_tag_key_members(tag_keys: &[String]) -> Result<(), ServerError> {
    if tag_keys.is_empty() {
        return Err(xml::empty_s3_control_tag_set());
    }
    if tag_keys.len() > s3_types::MAX_BUCKET_TAGS || tag_keys.iter().any(String::is_empty) {
        return Err(xml::invalid_s3_control_tag());
    }
    let mut unique = std::collections::HashSet::new();
    if tag_keys.iter().any(|key| !unique.insert(key.as_str())) {
        return Err(ServerError::InvalidTag {
            reason: "Duplicate tag keys are not supported.".to_string(),
            tag_key: None,
            tag_value: None,
        });
    }
    Ok(())
}

fn validate_untag_resource_tag_key_values(
    tag_keys: Vec<String>,
) -> Result<Vec<s3_types::TagKey>, ServerError> {
    tag_keys
        .into_iter()
        .map(|key| {
            if key.starts_with("aws:") {
                return Err(xml::reserved_s3_control_tag());
            }
            s3_types::TagKey::new(key).map_err(|_| xml::invalid_s3_control_tag())
        })
        .collect::<Result<Vec<_>, _>>()
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

    let mut bytes = [0u8; 16];
    argmin_crypto::random::fill(&mut bytes).map_err(|_| ServerError::InternalError {
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

fn parse_bucket_namespace(
    req: &S3Request,
    bucket: &BucketName,
) -> Result<BucketNamespace, ServerError> {
    if req.header_count("x-amz-bucket-namespace") > 1 {
        return Err(ServerError::InvalidArgument {
            reason: "x-amz-bucket-namespace must not be repeated".to_string(),
        });
    }
    let account_regional_name = s3_types::parse_account_regional_bucket_name(bucket.as_str());
    let Some(value) = req.header("x-amz-bucket-namespace") else {
        if account_regional_name.is_some() {
            return Err(ServerError::MissingNamespaceHeader);
        }
        return Ok(BucketNamespace::Global);
    };
    let namespace = value
        .parse::<BucketNamespace>()
        .map_err(|_| ServerError::InvalidArgument {
            reason: format!("invalid x-amz-bucket-namespace: {value}"),
        })?;
    if namespace == BucketNamespace::Global && account_regional_name.is_some() {
        return Err(
            ServerError::GlobalNamespaceHeaderRejectedForAccountRegionalBucket {
                bucket: bucket.to_string(),
            },
        );
    }
    Ok(namespace)
}

fn complete_multipart_write_condition_from_headers(
    req: &S3Request,
) -> Result<crate::conditional::WriteCondition, ServerError> {
    if req
        .header("if-match")
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err(ServerError::CompleteMultipartEmptyIfMatch);
    }
    if req
        .header("if-none-match")
        .is_some_and(|value| value.trim() != "*")
    {
        return Err(ServerError::CompleteMultipartIfNoneMatchNotImplemented);
    }
    write_condition_from_headers(req)
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
    pub identity_provider: IdentityProvider,
    pub host_id: Arc<str>,
    #[cfg(test)]
    test_storage_cluster: Arc<storage::StorageCluster>,
    #[cfg(test)]
    actual_cors_metadata_lookup_count: std::sync::atomic::AtomicUsize,
}

impl Clone for HttpFrontend {
    fn clone(&self) -> Self {
        Self {
            coordinator: Arc::clone(&self.coordinator),
            identity_provider: self.identity_provider.clone(),
            host_id: Arc::clone(&self.host_id),
            #[cfg(test)]
            test_storage_cluster: Arc::clone(&self.test_storage_cluster),
            #[cfg(test)]
            actual_cors_metadata_lookup_count: std::sync::atomic::AtomicUsize::new(
                self.actual_cors_metadata_lookup_count
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
        }
    }
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
        }
    }
}

pub struct S3HyperBody {
    state: S3HyperBodyState,
    trace: Option<ResponseBodyTrace>,
    _inflight_requests_guard: Option<observability::InflightRequestsGuard>,
    _permit: Option<OwnedSemaphorePermit>,
    // An early response can be produced before Hyper has consumed the request
    // body. Keep that body alive through response delivery: dropping it first
    // can make Hyper finish the connection after sending only the headers.
    _unread_request_body: Option<Incoming>,
}

pub(crate) struct HttpRequestAdmission {
    permit: OwnedSemaphorePermit,
    inflight_requests_guard: observability::InflightRequestsGuard,
}

impl HttpRequestAdmission {
    pub(crate) fn new(permit: OwnedSemaphorePermit) -> Self {
        Self {
            permit,
            inflight_requests_guard: observability::inflight_requests_guard(),
        }
    }

    fn into_parts(
        self,
    ) -> (
        Option<OwnedSemaphorePermit>,
        Option<observability::InflightRequestsGuard>,
    ) {
        (Some(self.permit), Some(self.inflight_requests_guard))
    }
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
            _unread_request_body: None,
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
            _unread_request_body: None,
        }
    }

    pub(crate) fn retain_unread_request_body(&mut self, body: Incoming) {
        debug_assert!(self._unread_request_body.is_none());
        self._unread_request_body = Some(body);
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
        self.handle_service_request(EndpointKind::S3Only, s3req, wire_ids)
    }

    /// Handle a request on a listener-selected endpoint kind. The endpoint is
    /// trusted server configuration and is never derived from request
    /// authority text.
    #[must_use]
    pub(crate) fn handle_service_request(
        &self,
        endpoint: EndpointKind,
        s3req: &S3Request,
        wire_ids: &WireResponseIds,
    ) -> S3Response {
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
            match route_service(
                endpoint,
                s3req.method.as_str(),
                s3req.path(),
                s3req.query_string(),
            ) {
                Ok(op) => op,
                Err(ServiceRouteError::S3(err)) => {
                    return S3Response::error_with_ids(&err, s3req.path(), wire_ids);
                }
                Err(ServiceRouteError::S3Control(S3ControlRouteError::InvalidUri { uri })) => {
                    return S3Response::s3_control_invalid_uri(&uri, wire_ids);
                }
                Err(ServiceRouteError::S3Control(S3ControlRouteError::EmptyBadRequest)) => {
                    return S3Response::outer_empty_bad_request();
                }
                Err(ServiceRouteError::S3Control(S3ControlRouteError::FrontendBadRequest)) => {
                    return S3Response::s3_control_frontend_bad_request(wire_ids);
                }
            }
        };

        // OPTIONS (preflight CORS) bypasses authentication.
        if let ServiceOperation::S3(S3Operation::OptionsRequest { ref bucket, .. }) = operation {
            let storage_route_admission = match self.coordinator.admit_storage_route_for_request() {
                Ok(admission) => admission,
                Err(err) => {
                    return S3Response::error_with_ids(&err, s3req.path(), wire_ids);
                }
            };
            return self.handle_options_request(s3req, &storage_route_admission, bucket, wire_ids);
        }
        if let ServiceOperation::S3Control(S3ControlOperation::Options) = &operation {
            return Self::handle_s3_control_options(s3req, wire_ids);
        }
        if let ServiceOperation::S3Control(S3ControlOperation::HeadBucketTags) = &operation {
            return S3Response::s3_control_head_method_not_allowed();
        }
        if let ServiceOperation::S3Control(S3ControlOperation::MethodNotAllowed { method }) =
            &operation
        {
            return S3Response::s3_control_method_not_allowed(method, wire_ids);
        }
        // AWS rejects complete absence of this required query member before
        // service-scope and HMAC checks. Present values, including invalid
        // ones, remain post-authentication validation in dispatch.
        if matches!(
            &operation,
            ServiceOperation::S3Control(S3ControlOperation::UntagResource { .. })
        ) && s3req.query_params_lossy("tagKeys").is_empty()
        {
            return S3Response::s3_control_error_with_ids(
                &xml::empty_s3_control_tag_set(),
                wire_ids,
            );
        }

        let is_s3_control = matches!(&operation, ServiceOperation::S3Control(_));
        let s3_operation = operation.s3();
        let actual_cors_bucket = s3_operation.and_then(S3Operation::bucket_name).cloned();
        // AWS reveals the bucket region on the pinned header/presigned
        // credential errors for bucket-scoped requests to existing buckets.
        // A valid account-regional suffix also supplies the region hint for a
        // header-auth region error even when the bucket is missing. Object-
        // scoped requests and POST/streaming writes omit the header.
        let auth_error_bucket_region_bucket = if s3_operation
            .is_some_and(|operation| operation.object_key().is_none())
            && !matches!(s3_operation, Some(S3Operation::PostObject { .. }))
        {
            actual_cors_bucket.clone()
        } else {
            None
        };
        // AWS also reveals the bucket region on AccessDenied for the region
        // discovery surfaces — HeadBucket and ListObjects — in any auth mode
        // (probed anonymous and cross-account); bucket subresources and
        // other bucket-scoped writes omit it.
        let denied_bucket_region_bucket = match s3_operation {
            Some(S3Operation::HeadBucket { bucket })
            | Some(S3Operation::ListObjectsV1 { bucket })
            | Some(S3Operation::ListObjectsV2 { bucket }) => Some(bucket.clone()),
            _ => None,
        };
        let auth_bucket = s3_operation
            .and_then(S3Operation::bucket_name)
            .map(BucketName::as_str);
        let service_kind = operation.service_kind();
        let auth = {
            observability::trace_scope!(
                TRACE_TARGET,
                "HttpFrontend::authenticate",
                "method={} path={:?}",
                s3req.method.as_str(),
                s3req.path()
            );
            self.authenticate_for_service(s3req, auth_bucket, service_kind)
        };
        let (result, mut storage_route_admission) = match auth {
            Ok(auth) => match self.coordinator.admit_storage_route_for_request() {
                Ok(admission) => {
                    let result = if let Some(s3_operation) = s3_operation {
                        if auth_bucket.is_some() {
                            if let Err(err) = self.enforce_bucket_region_for_operation(
                                &admission,
                                s3_operation,
                                &auth,
                            ) {
                                Err(err)
                            } else if let Err(err) = self.reject_streaming_fallthrough(s3req) {
                                Err(err)
                            } else {
                                self.dispatch_service(s3req, &auth, &admission, operation)
                            }
                        } else if let Err(err) = self.reject_streaming_fallthrough(s3req) {
                            Err(err)
                        } else {
                            self.dispatch_service(s3req, &auth, &admission, operation)
                        }
                    } else if let Err(err) = self.reject_streaming_fallthrough(s3req) {
                        Err(err)
                    } else {
                        self.dispatch_service(s3req, &auth, &admission, operation)
                    };
                    (result, Some(admission))
                }
                Err(err) => (Err(err), None),
            },
            Err(err) => (Err(err), None),
        };
        let auth_error_needs_bucket_region_lookup = matches!(
            &result,
            Err(ServerError::Auth(
                auth::AuthError::UnexpectedSecurityToken { .. }
                    | auth::AuthError::UnknownAccessKey { .. }
                    | auth::AuthError::InvalidHeaderCredentialService { .. }
                    | auth::AuthError::InvalidQueryCredentialRegion { .. }
                    | auth::AuthError::InvalidQueryCredentialService { .. }
            ))
        );
        if auth_error_needs_bucket_region_lookup && storage_route_admission.is_none() {
            // The region header is optional enrichment of an already selected
            // authentication error. Route expiry must suppress that lookup,
            // never replace the AWS-facing authentication response.
            storage_route_admission = self.coordinator.admit_storage_route_for_request().ok();
        }
        let add_bucket_region_for_auth_error = auth_error_bucket_region_bucket
            .as_ref()
            .filter(|_| auth_error_needs_bucket_region_lookup)
            .is_some_and(|bucket| {
                storage_route_admission.as_ref().is_some_and(|admission| {
                    self.coordinator
                        .bucket_exists_on_admitted_route(admission, bucket)
                        .unwrap_or(false)
                })
            });
        let add_bucket_region_for_denied_discovery = denied_bucket_region_bucket
            .as_ref()
            .filter(|_| {
                matches!(
                    &result,
                    Err(err) if err.http_status() == 403 && err.s3_error_code() == "AccessDenied"
                )
            })
            .is_some_and(|bucket| {
                storage_route_admission.as_ref().is_some_and(|admission| {
                    self.coordinator
                        .bucket_exists_on_admitted_route(admission, bucket)
                        .unwrap_or(false)
                        || self.account_regional_bucket_region_is_known(bucket)
                })
            });
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
                Err(err) if is_s3_control => S3Response::s3_control_error_with_ids(&err, wire_ids),
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
        if (add_bucket_region_for_auth_error || add_bucket_region_for_denied_discovery)
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
                if storage_route_admission.is_none() {
                    // CORS is optional enrichment of the response selected
                    // above. In particular, route expiry must not replace an
                    // authentication error or permit an unadmitted metadata
                    // lookup while rendering it.
                    storage_route_admission =
                        self.coordinator.admit_storage_route_for_request().ok();
                }
                if let Some(admission) = storage_route_admission.as_ref() {
                    observability::trace_scope!(
                        TRACE_TARGET,
                        "HttpFrontend::apply_actual_cors",
                        "method={} path={:?} bucket={:?}",
                        s3req.method.as_str(),
                        s3req.path(),
                        bucket
                    );
                    self.apply_cors_headers(
                        admission,
                        &mut resp,
                        &bucket,
                        origin,
                        s3req.method.as_str(),
                    );
                }
            }
        }

        resp
    }

    fn handle_s3_control_options(req: &S3Request, wire_ids: &WireResponseIds) -> S3Response {
        if req.header("origin").is_none() {
            return S3Response::s3_control_options_missing_origin(wire_ids);
        }
        let requested_method = match req.header("access-control-request-method") {
            Some(method) => method,
            None => return S3Response::s3_control_options_missing_origin(wire_ids),
        };
        S3Response::s3_control_cors_bucket_not_found(requested_method, wire_ids)
    }

    /// Handle an OPTIONS (CORS preflight) request. No auth required.
    fn handle_options_request(
        &self,
        req: &S3Request,
        storage_route_admission: &storage::StorageClusterRouteAdmission,
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
        let cors_config_xml = match self
            .coordinator
            .load_bucket_cors_config(storage_route_admission, bucket)
        {
            Ok(Some(xml)) => xml,
            Ok(None) => return S3Response::cors_not_enabled(request_method, wire_ids),
            Err(ServerError::BucketNotFound { .. }) => {
                return S3Response::cors_bucket_not_found(request_method, wire_ids);
            }
            Err(err) => return S3Response::error_with_ids(&err, req.path(), wire_ids),
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
            None => S3Response::cors_request_not_allowed(request_method, wire_ids),
        }
    }

    /// Apply CORS headers to an actual (non-preflight) response if the request
    /// has an Origin header and a matching CORS rule exists.
    pub(crate) fn actual_cors_headers(
        &self,
        storage_route_admission: &storage::StorageClusterRouteAdmission,
        bucket: &BucketName,
        origin: &str,
        method: &str,
    ) -> Vec<(String, String)> {
        #[cfg(test)]
        self.actual_cors_metadata_lookup_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let cors_config_xml = match self
            .coordinator
            .load_bucket_cors_config(storage_route_admission, bucket)
        {
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

    #[cfg(test)]
    pub(crate) fn test_actual_cors_metadata_lookup_count(&self) -> usize {
        self.actual_cors_metadata_lookup_count
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn apply_cors_headers(
        &self,
        storage_route_admission: &storage::StorageClusterRouteAdmission,
        resp: &mut S3Response,
        bucket: &BucketName,
        origin: &str,
        method: &str,
    ) {
        for (k, v) in self.actual_cors_headers(storage_route_admission, bucket, origin, method) {
            resp.headers.push((k, v));
        }
    }

    fn authenticated_account(
        auth: &AuthContext,
    ) -> Result<&s3_types::AccountIdentity, ServerError> {
        auth.identity
            .as_ref()
            .map(auth::AuthenticatedIdentity::account)
            .ok_or(ServerError::AccessDenied)
    }

    fn account_regional_bucket_region_is_known(&self, bucket: &BucketName) -> bool {
        parse_account_regional_bucket_name(bucket.as_str())
            .is_some_and(|name| name.region() == self.coordinator.region())
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
    ) -> Result<crate::coordinator::Requester, ServerError> {
        let supported = auth::ConfiguredOrAnonymousAuth::try_from(auth)
            .map_err(|_| ServerError::AccessDenied)?;
        Ok(crate::coordinator::Requester::from_auth(supported)
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
            .with_content_sha256(req.header("x-amz-content-sha256").map(str::to_string)))
    }

    fn map_auth_error(error: auth::AuthError) -> ServerError {
        match error {
            auth::AuthError::IdentityProviderFailure(error) => ServerError::IdentityProvider(error),
            error => ServerError::Auth(error),
        }
    }

    fn find_account_by_canonical_user_id(
        &self,
        canonical_user_id: &s3_types::CanonicalUserId,
    ) -> Result<Option<s3_types::AccountIdentity>, ServerError> {
        self.identity_provider
            .find_account_by_canonical_user_id(canonical_user_id)
            .map_err(ServerError::IdentityProvider)
    }

    fn acl_owner_display_name(
        &self,
        owner_principal: &str,
        owner_canonical_id: &s3_types::CanonicalUserId,
    ) -> Result<String, ServerError> {
        Ok(self
            .find_account_by_canonical_user_id(owner_canonical_id)?
            .map(|account| account.display_name().to_string())
            .unwrap_or_else(|| owner_principal.to_string()))
    }

    fn render_acl_grants(
        &self,
        owner_principal: &str,
        owner_canonical_id: &s3_types::CanonicalUserId,
        acl_grants: &s3_types::AclGrants,
    ) -> Result<(String, Vec<xml::RenderedAclGrant>), ServerError> {
        let owner_display_name =
            self.acl_owner_display_name(owner_principal, owner_canonical_id)?;
        let grants = acl_grants
            .iter()
            .map(|grant| {
                let display_name = match grant.grantee() {
                    s3_types::AclGrantee::CanonicalUser(id) if id == owner_canonical_id => {
                        Some(owner_display_name.clone())
                    }
                    s3_types::AclGrantee::CanonicalUser(id) => self
                        .find_account_by_canonical_user_id(id)?
                        .map(|account| account.display_name().to_string()),
                    s3_types::AclGrantee::AllUsers | s3_types::AclGrantee::AuthenticatedUsers => {
                        None
                    }
                };
                Ok(xml::RenderedAclGrant {
                    grantee: grant.grantee().clone(),
                    permission: grant.permission(),
                    display_name,
                })
            })
            .collect::<Result<Vec<_>, ServerError>>()?;
        Ok((owner_display_name, grants))
    }

    fn render_multipart_uploads(
        &self,
        result: crate::coordinator::ListMultipartUploadsResult,
    ) -> Result<xml::RenderedListMultipartUploadsResult, ServerError> {
        let (next_key_marker, next_upload_id_marker) = match result.next_marker {
            Some(crate::coordinator::ListMultipartUploadsNextMarker::Upload { key, upload_id }) => {
                (Some(key), Some(upload_id.to_string()))
            }
            Some(crate::coordinator::ListMultipartUploadsNextMarker::CommonPrefix) => {
                (Some(String::new()), Some(String::new()))
            }
            None => (None, None),
        };
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
                        .find_account_by_canonical_user_id(&upload.initiator.canonical_id)?
                        .map(|account| account.display_name().to_string()),
                };
                Ok(xml::RenderedMultipartUploadEntry {
                    key: upload.key,
                    upload_id: upload.upload_id.to_string(),
                    initiated: upload.initiated,
                    owner,
                    initiator,
                    checksum_algorithm: upload.checksum_algorithm,
                    checksum_type: upload.checksum_type,
                })
            })
            .collect::<Result<Vec<_>, ServerError>>()?;
        Ok(xml::RenderedListMultipartUploadsResult {
            uploads,
            common_prefixes: result.common_prefixes,
            is_truncated: result.is_truncated,
            next_key_marker,
            next_upload_id_marker,
        })
    }

    fn dispatch_service(
        &self,
        req: &S3Request,
        auth: &AuthContext,
        storage_route_admission: &storage::StorageClusterRouteAdmission,
        operation: ServiceOperation,
    ) -> Result<S3Response, ServerError> {
        match operation {
            ServiceOperation::S3(operation) => self.dispatch_routed_on_admitted_route(
                req,
                auth,
                storage_route_admission,
                operation,
            ),
            ServiceOperation::S3Control(operation) => {
                self.dispatch_s3_control(req, auth, storage_route_admission, operation)
            }
        }
    }

    fn dispatch_s3_control(
        &self,
        req: &S3Request,
        auth: &AuthContext,
        storage_route_admission: &storage::StorageClusterRouteAdmission,
        operation: S3ControlOperation,
    ) -> Result<S3Response, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::dispatch_s3_control",
            "method={} path={:?} op={:?} principal={:?}",
            req.method.as_str(),
            req.path(),
            operation,
            auth.configured_principal()
        );
        let expected_bucket_owner = expected_bucket_owner(req);
        match operation {
            S3ControlOperation::ListTagsForResource { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                let control = crate::coordinator::BucketTagControlRequest {
                    bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                };
                let tags = self
                    .coordinator
                    .get_bucket_tags_for_control_action_on_admitted_route(
                        storage_route_admission,
                        &control,
                        &[],
                        crate::coordinator::BucketTagControlAction::ListTagsForResource,
                    )?
                    .map(xml::TagSet::from_aws_tag_set)
                    .unwrap_or_else(|| xml::TagSet::empty(s3_types::MAX_BUCKET_TAGS));
                Ok(S3Response::list_tags_for_resource(
                    tags.to_list_tags_for_resource_xml(),
                ))
            }
            S3ControlOperation::TagResource { bucket } => {
                let tags = xml::TagSet::parse_tag_resource_xml(&req.body)?;
                let request_tags = tags.clone().into_vec();
                let requester = self.requester_from_auth(auth, req)?;
                let control = crate::coordinator::BucketTagControlRequest {
                    bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                };
                let existing_tags = self
                    .coordinator
                    .get_bucket_tags_for_control_action_on_admitted_route(
                        storage_route_admission,
                        &control,
                        request_tags.as_slice(),
                        crate::coordinator::BucketTagControlAction::TagResource,
                    )?
                    .map(xml::TagSet::from_aws_tag_set)
                    .unwrap_or_else(|| xml::TagSet::empty(s3_types::MAX_BUCKET_TAGS));
                let merged_tags = existing_tags.merge(&tags)?;
                self.coordinator
                    .put_bucket_tags_for_tag_resource_on_admitted_route(
                        storage_route_admission,
                        &crate::coordinator::PutBucketTagControlRequest {
                            control,
                            tags: merged_tags.as_aws_tag_set().clone(),
                            request_tags: request_tags.as_slice(),
                        },
                    )?;
                Ok(S3Response::tag_resource())
            }
            S3ControlOperation::UntagResource { bucket } => {
                let tag_keys = req
                    .query_params_lossy("tagKeys")
                    .into_iter()
                    .map(std::borrow::Cow::into_owned)
                    .collect::<Vec<_>>();
                validate_untag_resource_tag_key_members(&tag_keys)?;
                let request_tags = tag_keys
                    .iter()
                    .map(|key| (key.clone(), String::new()))
                    .collect::<Vec<_>>();
                let requester = self.requester_from_auth(auth, req)?;
                let control = crate::coordinator::BucketTagControlRequest {
                    bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                };
                let existing_tags = self
                    .coordinator
                    .get_bucket_tags_for_control_action_on_admitted_route(
                        storage_route_admission,
                        &control,
                        request_tags.as_slice(),
                        crate::coordinator::BucketTagControlAction::UntagResource,
                    )?
                    .map(xml::TagSet::from_aws_tag_set)
                    .unwrap_or_else(|| xml::TagSet::empty(s3_types::MAX_BUCKET_TAGS));
                // AWS treats invalid-character and overlong keys as a successful
                // no-op when no resource tags exist. Once any tag exists, it
                // validates those values before applying the removal.
                if existing_tags.is_empty() {
                    return Ok(S3Response::untag_resource());
                }
                let tag_keys = validate_untag_resource_tag_key_values(tag_keys)?;
                let remaining_tags = existing_tags.remove_keys(&tag_keys);
                if remaining_tags.is_empty() {
                    self.coordinator
                        .delete_bucket_tags_for_untag_resource_on_admitted_route(
                            storage_route_admission,
                            &crate::coordinator::UntagBucketTagControlRequest {
                                control,
                                request_tags: request_tags.as_slice(),
                            },
                        )?;
                } else {
                    self.coordinator
                        .put_bucket_tags_for_untag_resource_on_admitted_route(
                            storage_route_admission,
                            &crate::coordinator::PutBucketTagsForUntagResourceRequest {
                                control,
                                tags: remaining_tags.as_aws_tag_set().clone(),
                                request_tags: request_tags.as_slice(),
                            },
                        )?;
                }
                Ok(S3Response::untag_resource())
            }
            S3ControlOperation::HeadBucketTags
            | S3ControlOperation::MethodNotAllowed { .. }
            | S3ControlOperation::Options => {
                unreachable!("S3 Control method-only operations are handled before authentication")
            }
        }
    }

    #[cfg(test)]
    fn dispatch_routed(
        &self,
        req: &S3Request,
        auth: &AuthContext,
        operation: S3Operation,
    ) -> Result<S3Response, ServerError> {
        let storage_route_admission = self.coordinator.admit_storage_route_for_request()?;
        self.dispatch_routed_on_admitted_route(req, auth, &storage_route_admission, operation)
    }

    fn dispatch_routed_on_admitted_route(
        &self,
        req: &S3Request,
        auth: &AuthContext,
        storage_route_admission: &storage::StorageClusterRouteAdmission,
        operation: S3Operation,
    ) -> Result<S3Response, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::dispatch_routed",
            "method={} path={:?} op={:?} principal={:?}",
            req.method.as_str(),
            req.path(),
            operation,
            auth.configured_principal()
        );
        let expected_bucket_owner = expected_bucket_owner(req);
        // Dispatch to coordinator
        match operation {
            S3Operation::ListBuckets => {
                let requester = self.requester_from_auth(auth, req)?;
                let owner_account = Self::authenticated_account(auth)?;
                let prefix = req.query_param_lossy("prefix");
                let mut buckets = self.coordinator.list_buckets_on_admitted_route(
                    storage_route_admission,
                    &crate::coordinator::ListBucketsRequest { requester },
                )?;
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
                let namespace = parse_bucket_namespace(req, &bucket)?;
                let ownership = parse_bucket_ownership(req.header("x-amz-object-ownership"))?;
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator.create_bucket_on_admitted_route(
                    storage_route_admission,
                    &crate::coordinator::CreateBucketRequest {
                        name: bucket.clone(),
                        requester,
                        namespace,
                        acl,
                        ownership,
                        object_lock_enabled,
                    },
                )?;
                Ok(S3Response::create_bucket(bucket.as_str()))
            }
            S3Operation::DeleteBucket { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator.delete_bucket_on_admitted_route(
                    storage_route_admission,
                    &bucket_request(&bucket, requester, expected_bucket_owner)?,
                )?;
                Ok(S3Response::delete_bucket())
            }
            S3Operation::HeadBucket { bucket } => {
                let conceal_account_regional_missing =
                    if let Some(name) = parse_account_regional_bucket_name(bucket.as_str()) {
                        if name.region() != self.coordinator.region() {
                            return Ok(S3Response::head_bucket_region_redirect(name.region()));
                        }
                        !auth.identity.as_ref().is_some_and(|identity| {
                            identity.account().account_id() == Some(name.account_id())
                        })
                    } else {
                        false
                    };
                let requester = self.requester_from_auth(auth, req)?;
                let info = match self.coordinator.head_bucket_on_admitted_route(
                    storage_route_admission,
                    &bucket_request(&bucket, requester, expected_bucket_owner)?,
                ) {
                    Err(ServerError::BucketNotFound { .. }) if conceal_account_regional_missing => {
                        return Err(ServerError::AccessDenied);
                    }
                    result => result?,
                };
                Ok(S3Response::head_bucket(&info, self.coordinator.region()))
            }
            S3Operation::GetBucketLocation { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator.get_bucket_location_on_admitted_route(
                    storage_route_admission,
                    &bucket_request(&bucket, requester, expected_bucket_owner)?,
                )?;
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
                let requester = self.requester_from_auth(auth, req)?;

                let result = self.coordinator.list_objects_v2_on_admitted_route(
                    storage_route_admission,
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
                let requester = self.requester_from_auth(auth, req)?;

                let result = self.coordinator.list_objects_v2_on_admitted_route(
                    storage_route_admission,
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
                    let requester = self.requester_from_auth(auth, req)?;
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
                    let replace_tags = if req
                        .header("x-amz-tagging-directive")
                        .is_some_and(|d| d.eq_ignore_ascii_case("REPLACE"))
                    {
                        if let Some(tagging_header) = req.header("x-amz-tagging") {
                            let tags = xml::TagSet::new(
                                xml::parse_url_encoded_tags(tagging_header)?,
                                s3_types::MAX_OBJECT_TAGS,
                            )?;
                            if tags.is_empty() {
                                None
                            } else {
                                Some(tags)
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
                        TaggingDirective::Replace(
                            replace_tags.as_ref().map(xml::TagSet::as_aws_tag_set),
                        )
                    } else {
                        TaggingDirective::Copy
                    };
                    let policy_context = put_object_policy_context_from_request(
                        req,
                        replace_tags.as_ref().map(xml::TagSet::as_aws_tag_set),
                        Some(copy_source),
                        directive.policy_condition_value(),
                        acl.policy_condition_value(),
                        destination_managed_encryption,
                    );
                    let result = self.coordinator.copy_object_on_admitted_route(
                        storage_route_admission,
                        &CopyObjectRequest {
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
                        },
                    )?;
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
                    let inline_tags = if let Some(tagging_header) = req.header("x-amz-tagging") {
                        let tags = xml::TagSet::new(
                            xml::parse_url_encoded_tags(tagging_header)?,
                            s3_types::MAX_OBJECT_TAGS,
                        )?;
                        if tags.is_empty() {
                            None
                        } else {
                            Some(tags)
                        }
                    } else {
                        None
                    };
                    let request_headers: Vec<(&str, &str)> = req.header_iter().collect();
                    validate_write_request_header_section_size(&request_headers)?;
                    let (metadata_blob, system_metadata) =
                        parse_put_object_request_metadata(request_headers.iter().copied())?;
                    let cond = write_condition_from_headers(req)?;
                    let requester = self.requester_from_auth(auth, req)?;
                    let acl = parse_put_object_write_acl(req)?;
                    let policy_context = put_object_policy_context_from_request(
                        req,
                        inline_tags.as_ref().map(xml::TagSet::as_aws_tag_set),
                        None,
                        None,
                        acl.policy_condition_value(),
                        sse_s3.then_some(ManagedEncryptionAlgorithm::Aes256),
                    );
                    let result = self.coordinator.put_object_on_admitted_route(
                        storage_route_admission,
                        &crate::coordinator::PutObjectRequest {
                            object: object_request(
                                &bucket,
                                &key,
                                requester,
                                expected_bucket_owner,
                            )?,
                            data: &req.body,
                            metadata: &metadata_blob,
                            system_metadata: &system_metadata,
                            tags: inline_tags.as_ref().map(xml::TagSet::as_aws_tag_set),
                            cond: &cond,
                            acl,
                            policy_context,
                            object_lock,
                            encryption:
                                crate::coordinator::WriteEncryptionRequest::from_request_parts(
                                    sse_customer.as_ref(),
                                    sse_s3.then_some(storage::ManagedEncryptionAlgorithm::Aes256),
                                )?,
                        },
                    )?;
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
                let requester = self.requester_from_auth(auth, req)?;
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
                    let result = self.coordinator.get_object_part_on_admitted_route(
                        storage_route_admission,
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
                            let result = self.coordinator.get_object_range_on_admitted_route(
                                storage_route_admission,
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
                            let result = self.coordinator.get_object_on_admitted_route(
                                storage_route_admission,
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
                    let result = self.coordinator.get_object_on_admitted_route(
                        storage_route_admission,
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
                let requester = self.requester_from_auth(auth, req)?;
                let result = self.coordinator.delete_object_on_admitted_route(
                    storage_route_admission,
                    &crate::coordinator::DeleteObjectRequest {
                        object: object_version_request(
                            &bucket,
                            &key,
                            vid,
                            requester,
                            expected_bucket_owner,
                        )?,
                        bypass_governance,
                        cond: &cond,
                    },
                )?;
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
                let requester = self.requester_from_auth(auth, req)?;
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
                    let result = self.coordinator.head_object_part_on_admitted_route(
                        storage_route_admission,
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
                    let result = self.coordinator.head_object_on_admitted_route(
                        storage_route_admission,
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
                let requester_ctx = self.requester_from_auth(auth, req)?;
                let result = self.coordinator.get_object_attributes_on_admitted_route(
                    storage_route_admission,
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
                let requester = self.requester_from_auth(auth, req)?;
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
                    self.coordinator.delete_objects_on_admitted_route(
                        storage_route_admission,
                        &crate::coordinator::DeleteObjectsRequest {
                            bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                            entries: &entries,
                            bypass_governance,
                        },
                    )?
                };
                result.errors.extend(validation_errors);
                Ok(S3Response::delete_objects(&result, quiet))
            }
            S3Operation::PutBucketVersioning { bucket } => {
                validate_request_checksum_headers(req, true, false, true)?;
                let versioning_state = xml::parse_versioning_config_xml(&req.body)?;
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator.put_bucket_versioning_on_admitted_route(
                    storage_route_admission,
                    &crate::coordinator::PutBucketVersioningRequest {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        state: versioning_state,
                    },
                )?;
                Ok(S3Response::put_bucket_versioning())
            }
            S3Operation::GetBucketVersioning { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                let state = self.coordinator.get_bucket_versioning_on_admitted_route(
                    storage_route_admission,
                    &bucket_request(&bucket, requester, expected_bucket_owner)?,
                )?;
                Ok(S3Response::get_bucket_versioning(state))
            }
            S3Operation::PutBucketObjectLockConfiguration { bucket } => {
                require_request_checksum(
                    req,
                    RequestChecksumRequirement::ContentMd5OrChecksumHeader,
                )?;
                let config = xml::parse_bucket_object_lock_configuration_xml(&req.body)?;
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator
                    .put_bucket_object_lock_configuration_on_admitted_route(
                        storage_route_admission,
                        &crate::coordinator::PutBucketObjectLockConfigurationRequest {
                            bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                            config,
                        },
                    )?;
                Ok(S3Response::put_bucket_object_lock_configuration())
            }
            S3Operation::GetBucketObjectLockConfiguration { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                let config = self
                    .coordinator
                    .get_bucket_object_lock_configuration_on_admitted_route(
                        storage_route_admission,
                        &bucket_request(&bucket, requester, expected_bucket_owner)?,
                    )?;
                Ok(S3Response::get_bucket_object_lock_configuration(config))
            }
            S3Operation::PutBucketEncryption { bucket } => {
                validate_request_checksum_headers(req, true, false, true)?;
                let config = xml::parse_bucket_encryption_xml(&req.body)?;
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator.put_bucket_encryption_on_admitted_route(
                    storage_route_admission,
                    &crate::coordinator::PutBucketEncryptionRequest {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        config,
                    },
                )?;
                Ok(S3Response::put_bucket_encryption())
            }
            S3Operation::GetBucketEncryption { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                let config = self.coordinator.get_bucket_encryption_on_admitted_route(
                    storage_route_admission,
                    &bucket_request(&bucket, requester, expected_bucket_owner)?,
                )?;
                Ok(S3Response::get_bucket_encryption(config))
            }
            S3Operation::DeleteBucketEncryption { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator
                    .delete_bucket_encryption_on_admitted_route(
                        storage_route_admission,
                        &bucket_request(&bucket, requester, expected_bucket_owner)?,
                    )?;
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
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator.put_bucket_cors_on_admitted_route(
                    storage_route_admission,
                    &crate::coordinator::PutBucketConfigRequest {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        config: &config_xml,
                    },
                )?;
                Ok(S3Response::put_bucket_cors())
            }
            S3Operation::GetBucketCors { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                match self.coordinator.get_bucket_cors_on_admitted_route(
                    storage_route_admission,
                    &bucket_request(&bucket, requester, expected_bucket_owner)?,
                )? {
                    Some(config_xml) => Ok(S3Response::get_bucket_cors(&config_xml)),
                    None => Err(ServerError::NoSuchCorsConfiguration {
                        bucket: bucket.to_string(),
                    }),
                }
            }
            S3Operation::DeleteBucketCors { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator.delete_bucket_cors_on_admitted_route(
                    storage_route_admission,
                    &bucket_request(&bucket, requester, expected_bucket_owner)?,
                )?;
                Ok(S3Response::delete_bucket_cors())
            }
            S3Operation::PutBucketTagging { bucket } => {
                require_request_checksum(
                    req,
                    RequestChecksumRequirement::ContentMd5OrChecksumHeader,
                )?;
                let tags = xml::TagSet::parse_tagging_xml(&req.body, s3_types::MAX_BUCKET_TAGS)?;
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator.put_bucket_tags_on_admitted_route(
                    storage_route_admission,
                    &crate::coordinator::PutBucketTagsRequest {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        tags: tags.as_aws_tag_set().clone(),
                    },
                )?;
                Ok(S3Response::put_bucket_tagging())
            }
            S3Operation::GetBucketTagging { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                match self.coordinator.get_bucket_tags_on_admitted_route(
                    storage_route_admission,
                    &bucket_request(&bucket, requester, expected_bucket_owner)?,
                )? {
                    Some(tags) => Ok(S3Response::get_bucket_tagging(&tags.to_xml())),
                    None => Err(ServerError::NoSuchTagSet {
                        resource: bucket.to_string(),
                    }),
                }
            }
            S3Operation::DeleteBucketTagging { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator.delete_bucket_tags_on_admitted_route(
                    storage_route_admission,
                    &bucket_request(&bucket, requester, expected_bucket_owner)?,
                )?;
                Ok(S3Response::delete_bucket_tagging())
            }
            S3Operation::PutBucketAbac { bucket } => {
                require_request_checksum(
                    req,
                    RequestChecksumRequirement::ContentMd5OrChecksumHeader,
                )?;
                let enabled = xml::parse_bucket_abac_xml(&req.body)?;
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator.put_bucket_abac_on_admitted_route(
                    storage_route_admission,
                    &crate::coordinator::PutBucketAbacRequest {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        enabled,
                    },
                )?;
                Ok(S3Response::put_bucket_abac())
            }
            S3Operation::GetBucketAbac { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                let enabled = self.coordinator.get_bucket_abac_on_admitted_route(
                    storage_route_admission,
                    &bucket_request(&bucket, requester, expected_bucket_owner)?,
                )?;
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
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator.put_bucket_lifecycle_on_admitted_route(
                    storage_route_admission,
                    &crate::coordinator::PutBucketConfigRequest {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        config: &config_xml,
                    },
                )?;
                Ok(S3Response::put_bucket_lifecycle())
            }
            S3Operation::GetBucketLifecycle { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                match self.coordinator.get_bucket_lifecycle_on_admitted_route(
                    storage_route_admission,
                    &bucket_request(&bucket, requester, expected_bucket_owner)?,
                )? {
                    Some(config_xml) => Ok(S3Response::get_bucket_lifecycle(&config_xml)),
                    None => Err(ServerError::NoSuchLifecycleConfiguration {
                        bucket: bucket.to_string(),
                    }),
                }
            }
            S3Operation::DeleteBucketLifecycle { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator.delete_bucket_lifecycle_on_admitted_route(
                    storage_route_admission,
                    &bucket_request(&bucket, requester, expected_bucket_owner)?,
                )?;
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
                let requester = self.requester_from_auth(auth, req)?;
                let version_id = self.coordinator.put_object_retention_on_admitted_route(
                    storage_route_admission,
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
                let requester = self.requester_from_auth(auth, req)?;
                let retention = self.coordinator.get_object_retention_on_admitted_route(
                    storage_route_admission,
                    &object_version_request(&bucket, &key, vid, requester, expected_bucket_owner)?,
                )?;
                Ok(S3Response::get_object_retention(retention))
            }
            S3Operation::PutObjectLegalHold { bucket, key } => {
                require_request_checksum(
                    req,
                    RequestChecksumRequirement::ContentMd5OrChecksumHeader,
                )?;
                let vid = parse_version_id(req)?;
                let legal_hold = xml::parse_object_legal_hold_xml(&req.body)?;
                let requester = self.requester_from_auth(auth, req)?;
                let version_id = self.coordinator.put_object_legal_hold_on_admitted_route(
                    storage_route_admission,
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
                let requester = self.requester_from_auth(auth, req)?;
                let legal_hold = self.coordinator.get_object_legal_hold_on_admitted_route(
                    storage_route_admission,
                    &object_version_request(&bucket, &key, vid, requester, expected_bucket_owner)?,
                )?;
                Ok(S3Response::get_object_legal_hold(legal_hold))
            }
            S3Operation::PutObjectTagging { bucket, key } => {
                validate_request_checksum_headers(req, true, false, true)?;
                let vid = parse_version_id(req)?;
                let tags = xml::TagSet::parse_tagging_xml(&req.body, s3_types::MAX_OBJECT_TAGS)?;
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator.put_object_tags_on_admitted_route(
                    storage_route_admission,
                    &crate::coordinator::PutObjectTagsRequest {
                        object: object_version_request(
                            &bucket,
                            &key,
                            vid,
                            requester,
                            expected_bucket_owner,
                        )?,
                        tags: tags.as_aws_tag_set(),
                    },
                )?;
                Ok(S3Response::put_object_tagging())
            }
            S3Operation::GetObjectTagging { bucket, key } => {
                let vid = parse_version_id(req)?;
                let requester = self.requester_from_auth(auth, req)?;
                if let Some(mut tags) = self.coordinator.get_object_tags_on_admitted_route(
                    storage_route_admission,
                    &object_version_request(&bucket, &key, vid, requester, expected_bucket_owner)?,
                )? {
                    tags.reverse();
                    Ok(S3Response::get_object_tagging(&tags.to_xml()))
                } else {
                    // S3 returns empty TagSet (not 404) for objects with no tags
                    let empty = xml::TagSet::empty(s3_types::MAX_OBJECT_TAGS).to_xml();
                    Ok(S3Response::get_object_tagging(&empty))
                }
            }
            S3Operation::DeleteObjectTagging { bucket, key } => {
                let vid = parse_version_id(req)?;
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator.delete_object_tags_on_admitted_route(
                    storage_route_admission,
                    &object_version_request(&bucket, &key, vid, requester, expected_bucket_owner)?,
                )?;
                Ok(S3Response::delete_object_tagging())
            }
            S3Operation::GetObjectAcl { bucket, key } => {
                let version_id = parse_version_id(req)?;
                let requester = self.requester_from_auth(auth, req)?;
                let result = self.coordinator.get_object_acl_on_admitted_route(
                    storage_route_admission,
                    &object_version_request(
                        &bucket,
                        &key,
                        version_id,
                        requester,
                        expected_bucket_owner,
                    )?,
                )?;
                let (owner_display_name, grants) = self.render_acl_grants(
                    &result.owner_principal,
                    &result.owner_canonical_id,
                    &result.acl_grants,
                )?;
                Ok(S3Response::get_object_acl(
                    &result,
                    &owner_display_name,
                    &grants,
                ))
            }
            S3Operation::PutObjectAcl { bucket, key } => {
                validate_request_checksum_headers(req, true, false, true)?;
                let version_id = parse_version_id(req)?;
                let requester = self.requester_from_auth(auth, req)?;
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
                    self.coordinator.put_object_acl_on_admitted_route(
                        storage_route_admission,
                        &crate::coordinator::PutObjectAclRequest {
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
                        },
                    )?
                } else {
                    let acl_grants = parse_acl_grants(req)?;
                    self.coordinator.put_object_acl_on_admitted_route(
                        storage_route_admission,
                        &crate::coordinator::PutObjectAclRequest {
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
                        },
                    )?
                };
                Ok(S3Response::put_object_acl(result_version_id))
            }
            S3Operation::PutBucketPublicAccessBlock { bucket } => {
                validate_request_checksum_headers(req, true, false, true)?;
                let config = xml::parse_public_access_block_xml(&req.body)?;
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator
                    .put_bucket_public_access_block_on_admitted_route(
                        storage_route_admission,
                        &crate::coordinator::PutBucketPublicAccessBlockRequest {
                            bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                            config,
                        },
                    )?;
                Ok(S3Response::put_bucket_public_access_block())
            }
            S3Operation::GetBucketPublicAccessBlock { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                match self
                    .coordinator
                    .get_bucket_public_access_block_on_admitted_route(
                        storage_route_admission,
                        &bucket_request(&bucket, requester, expected_bucket_owner)?,
                    )? {
                    Some(config) => Ok(S3Response::get_bucket_public_access_block(
                        &xml::get_public_access_block_xml(&config),
                    )),
                    None => Err(ServerError::NoSuchPublicAccessBlockConfiguration {
                        bucket: bucket.to_string(),
                    }),
                }
            }
            S3Operation::DeleteBucketPublicAccessBlock { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator
                    .delete_bucket_public_access_block_on_admitted_route(
                        storage_route_admission,
                        &bucket_request(&bucket, requester, expected_bucket_owner)?,
                    )?;
                Ok(S3Response::delete_bucket_public_access_block())
            }
            S3Operation::PutBucketOwnershipControls { bucket } => {
                validate_request_checksum_headers(req, true, false, true)?;
                let value = xml::parse_ownership_controls_xml(&req.body)?;
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator
                    .put_bucket_ownership_controls_on_admitted_route(
                        storage_route_admission,
                        &crate::coordinator::PutBucketOwnershipControlsRequest {
                            bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                            config: value,
                        },
                    )?;
                Ok(S3Response::put_bucket_ownership_controls())
            }
            S3Operation::GetBucketOwnershipControls { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                match self
                    .coordinator
                    .get_bucket_ownership_controls_on_admitted_route(
                        storage_route_admission,
                        &bucket_request(&bucket, requester, expected_bucket_owner)?,
                    )? {
                    Some(config) => Ok(S3Response::get_bucket_ownership_controls(
                        &xml::get_ownership_controls_xml(&config),
                    )),
                    None => Err(ServerError::OwnershipControlsNotFound {
                        bucket: bucket.to_string(),
                    }),
                }
            }
            S3Operation::DeleteBucketOwnershipControls { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator
                    .delete_bucket_ownership_controls_on_admitted_route(
                        storage_route_admission,
                        &bucket_request(&bucket, requester, expected_bucket_owner)?,
                    )?;
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
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator.put_bucket_policy_on_admitted_route(
                    storage_route_admission,
                    &crate::coordinator::PutBucketPolicyRequest {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        config: policy,
                        confirm_remove_self_bucket_access,
                    },
                )?;
                Ok(S3Response::put_bucket_policy())
            }
            S3Operation::GetBucketPolicy { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                match self.coordinator.get_bucket_policy_on_admitted_route(
                    storage_route_admission,
                    &bucket_request(&bucket, requester, expected_bucket_owner)?,
                )? {
                    Some(policy) => Ok(S3Response::get_bucket_policy(&policy)),
                    None => Err(ServerError::NoSuchBucketPolicy {
                        bucket: bucket.to_string(),
                    }),
                }
            }
            S3Operation::GetBucketPolicyStatus { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                let is_public = self
                    .coordinator
                    .get_bucket_policy_status_on_admitted_route(
                        storage_route_admission,
                        &bucket_request(&bucket, requester, expected_bucket_owner)?,
                    )?;
                Ok(S3Response::get_bucket_policy_status(is_public))
            }
            S3Operation::DeleteBucketPolicy { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator.delete_bucket_policy_on_admitted_route(
                    storage_route_admission,
                    &bucket_request(&bucket, requester, expected_bucket_owner)?,
                )?;
                Ok(S3Response::delete_bucket_policy())
            }
            S3Operation::GetBucketAcl { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
                let result = self.coordinator.get_bucket_acl_on_admitted_route(
                    storage_route_admission,
                    &bucket_request(&bucket, requester, expected_bucket_owner)?,
                )?;
                let (owner_display_name, grants) = self.render_acl_grants(
                    &result.owner_principal,
                    &result.owner_canonical_id,
                    &result.acl_grants,
                )?;
                Ok(S3Response::get_bucket_acl(
                    &result,
                    &owner_display_name,
                    &grants,
                ))
            }
            S3Operation::PutBucketAcl { bucket } => {
                let requester = self.requester_from_auth(auth, req)?;
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
                self.coordinator
                    .validate_put_bucket_acl_request_on_admitted_route(
                        storage_route_admission,
                        &acl_req,
                    )?;
                validate_request_checksum_headers(req, true, false, true)?;
                self.coordinator
                    .put_bucket_acl_on_admitted_route(storage_route_admission, &acl_req)?;
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
                let inline_tags = if let Some(tagging_header) = req.header("x-amz-tagging") {
                    let tags = xml::TagSet::new(
                        xml::parse_url_encoded_tags(tagging_header)?,
                        s3_types::MAX_OBJECT_TAGS,
                    )?;
                    if tags.is_empty() {
                        None
                    } else {
                        Some(tags)
                    }
                } else {
                    None
                };
                let requester = self.requester_from_auth(auth, req)?;
                let acl = parse_put_object_write_acl(req)?;
                let object_lock = parse_object_lock_headers(req)?;
                let policy_context = put_object_policy_context_from_request(
                    req,
                    inline_tags.as_ref().map(xml::TagSet::as_aws_tag_set),
                    None,
                    None,
                    acl.policy_condition_value(),
                    sse_s3.then_some(ManagedEncryptionAlgorithm::Aes256),
                )
                .with_object_creation_operation(false);

                let result = self.coordinator.create_multipart_upload_on_admitted_route(
                    storage_route_admission,
                    &crate::coordinator::CreateMultipartUploadRequest {
                        object: object_request(&bucket, &key, requester, expected_bucket_owner)?,
                        metadata: &metadata,
                        system_metadata: &system_metadata,
                        tags: inline_tags.as_ref().map(xml::TagSet::as_aws_tag_set),
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
                let (upload_id_raw, part_number_raw) = request::parse_upload_part_query_raw(
                    req.query_string(),
                    ServerError::UploadPartCopyMissingUploadId,
                )?;
                let upload_id = parse_present_upload_id(upload_id_raw.as_str())?;
                // Normal UploadPart requests are intercepted in serve.rs and
                // streamed before they reach dispatch_routed(). Only copy-source
                // variants should remain on this buffered path.
                let Some(copy_source) = req.header("x-amz-copy-source") else {
                    return Err(ServerError::InternalError {
                        reason: "buffered dispatcher reached non-copy UploadPart".to_string(),
                    });
                };

                let requester = self.requester_from_auth(auth, req)?;
                let upload_request = multipart_object_request(
                    &bucket,
                    &key,
                    upload_id,
                    requester,
                    expected_bucket_owner,
                )?;
                self.coordinator
                    .validate_in_progress_multipart_upload_target_on_admitted_route(
                        storage_route_admission,
                        &upload_request,
                    )?;
                let part_number = request::parse_upload_part_copy_number_value(&part_number_raw)?;

                let (src_bucket, src_key, src_version_id) = parse_copy_source_header(copy_source)?;
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
                    .upload_part_copy_on_admitted_route(
                        storage_route_admission,
                        &UploadPartCopyRequest {
                            source: CopySource::new(
                                src_bucket,
                                src_key,
                                src_version_id,
                                &src_cond,
                                expected_source_bucket_owner(req),
                            ),
                            upload: upload_request,
                            part_number,
                            copy_source_range,
                            policy_context: PutObjectPolicyContext::new(
                                Some(copy_source),
                                None,
                                None,
                            ),
                            source_sse_customer: source_sse_customer.as_ref(),
                            sse_customer: sse_customer.as_ref(),
                        },
                    )
                    .map_err(|err| match err {
                        ServerError::PreconditionFailed { condition } => {
                            ServerError::UploadPartCopyPreconditionFailed {
                                condition: condition.to_string(),
                            }
                        }
                        other => other,
                    })?;
                Ok(S3Response::upload_part_copy(&result))
            }
            S3Operation::CompleteMultipartUpload { bucket, key } => {
                reject_managed_encryption_read_headers(
                    req,
                    ManagedEncryptionReadHeaderContext::Multipart,
                )?;
                let wire_ids = WireResponseIds::new(
                    current_trace_context().request_id(),
                    self.host_id.clone(),
                );
                // AWS rejects unsupported conditional-header combinations before
                // resolving the upload, but evaluates a supported condition only
                // after all multipart completion validation has succeeded.
                let cond = complete_multipart_write_condition_from_headers(req)?;
                let upload_id =
                    match parse_required_upload_id(req.query_param_lossy("uploadId").as_deref()) {
                        Ok(upload_id) => upload_id,
                        Err(ServerError::NoSuchUpload { upload_id }) => {
                            return Ok(S3Response::complete_multipart_no_such_upload(
                                &upload_id, &wire_ids,
                            ));
                        }
                        Err(err) => return Err(err),
                    };
                let upload_id_text = upload_id.as_str().to_string();
                let requester = self.requester_from_auth(auth, req)?;
                let upload_request = multipart_object_request(
                    &bucket,
                    &key,
                    upload_id,
                    requester,
                    expected_bucket_owner,
                )?;
                match self
                    .coordinator
                    .validate_complete_multipart_upload_target_on_admitted_route(
                        storage_route_admission,
                        &upload_request,
                    ) {
                    Ok(()) => {}
                    Err(ServerError::NoSuchUpload { upload_id }) => {
                        return Ok(S3Response::complete_multipart_no_such_upload(
                            &upload_id, &wire_ids,
                        ));
                    }
                    Err(err) => return Err(err),
                }
                // AWS validates the value shape of these headers after resolving
                // the upload but before parsing the completion XML.
                let claimed_checksum = extract_encoded_checksum_header(req)?;
                if let Some(claimed) = claimed_checksum.as_ref() {
                    claimed.validate_complete_multipart_header_value()?;
                }
                let expected_object_size = req
                    .header("x-amz-mp-object-size")
                    .map(|value| {
                        value.parse::<u64>().map_err(|_| {
                            ServerError::CompleteMultipartExpectedSizeHeaderInvalid {
                                value: value.to_string(),
                            }
                        })
                    })
                    .transpose()?;
                let parts = match xml::parse_complete_multipart_upload_xml(&req.body) {
                    Ok(parts) => parts,
                    Err(ServerError::MalformedXML { .. }) => {
                        return Ok(S3Response::complete_multipart_malformed_xml(&wire_ids));
                    }
                    Err(err) => return Err(err),
                };
                let sse_customer = parse_sse_customer_request(req)?;
                let result = match self
                    .coordinator
                    .complete_multipart_upload_on_admitted_route(
                        storage_route_admission,
                        &crate::coordinator::CompleteMultipartUploadRequest {
                            upload: upload_request,
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
                let requester = self.requester_from_auth(auth, req)?;
                self.coordinator.abort_multipart_upload_on_admitted_route(
                    storage_route_admission,
                    &multipart_object_request(
                        &bucket,
                        &key,
                        upload_id,
                        requester,
                        expected_bucket_owner,
                    )?,
                )?;
                Ok(S3Response::abort_multipart_upload())
            }
            S3Operation::ListMultipartUploads { bucket } => {
                let prefix = req.query_param_lossy("prefix");
                let delimiter = req.query_param_lossy("delimiter");
                let key_marker = req.query_param_lossy("key-marker");
                let upload_id_marker = req.query_param_lossy("upload-id-marker");
                let encoding_type = req.query_param_lossy("encoding-type");
                let max_uploads =
                    parse_s3_list_limit(req.query_param_lossy("max-uploads"), "max-uploads")?;
                validate_list_encoding_type(encoding_type.as_deref())?;
                let effective_upload_id_marker = key_marker
                    .as_deref()
                    .filter(|marker| !marker.is_empty())
                    .and(upload_id_marker.as_deref())
                    .filter(|marker| !marker.is_empty());
                let parsed_upload_id_marker =
                    parse_optional_upload_id_marker(effective_upload_id_marker)?;
                let requester = self.requester_from_auth(auth, req)?;
                let result = self.coordinator.list_multipart_uploads_on_admitted_route(
                    storage_route_admission,
                    &crate::coordinator::ListMultipartUploadsRequest {
                        bucket: bucket_request(&bucket, requester, expected_bucket_owner)?,
                        prefix: prefix.as_deref(),
                        delimiter: delimiter.as_deref(),
                        key_marker: key_marker.as_deref(),
                        upload_id_marker: parsed_upload_id_marker,
                        max_uploads,
                    },
                )?;
                let rendered = self.render_multipart_uploads(result)?;
                Ok(S3Response::list_multipart_uploads(
                    xml::RenderedListMultipartUploadsRequest {
                        bucket: bucket.as_str(),
                        prefix: prefix.as_deref(),
                        delimiter: delimiter.as_deref(),
                        key_marker: key_marker.as_deref(),
                        upload_id_marker: effective_upload_id_marker,
                        encoding_type: encoding_type.as_deref(),
                        max_uploads,
                    },
                    &rendered,
                ))
            }
            S3Operation::ListParts { bucket, key } => {
                let max_parts =
                    parse_s3_list_limit(req.query_param_lossy("max-parts"), "max-parts")?;
                let part_number_marker = parse_optional_s3_list_integer(
                    req.query_param_lossy("part-number-marker"),
                    "part-number-marker",
                )?;
                let upload_id =
                    parse_required_upload_id(req.query_param_lossy("uploadId").as_deref())?;
                let requester = self.requester_from_auth(auth, req)?;
                let result = self.coordinator.list_parts_on_admitted_route(
                    storage_route_admission,
                    &crate::coordinator::ListPartsRequest {
                        upload: multipart_object_request(
                            &bucket,
                            &key,
                            upload_id.clone(),
                            requester,
                            expected_bucket_owner,
                        )?,
                        part_number_marker,
                        max_parts,
                    },
                )?;
                let owner = xml::RenderedCanonicalUser {
                    canonical_id: result.owner.canonical_id.clone(),
                    display_name: None,
                };
                let initiator = xml::RenderedCanonicalUser {
                    canonical_id: result.initiator.canonical_id.clone(),
                    display_name: self
                        .find_account_by_canonical_user_id(&result.initiator.canonical_id)?
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
                let requester = self.requester_from_auth(auth, req)?;

                let result = self.coordinator.list_object_versions_on_admitted_route(
                    storage_route_admission,
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

    #[cfg(test)]
    fn authenticate(
        &self,
        req: &S3Request,
        bucket: Option<&str>,
    ) -> Result<AuthContext, ServerError> {
        self.authenticate_with_payload_check(req, true, bucket)
    }

    fn authenticate_for_service(
        &self,
        req: &S3Request,
        bucket: Option<&str>,
        service: ServiceKind,
    ) -> Result<AuthContext, ServerError> {
        if service == ServiceKind::S3Control {
            Self::require_content_sha256_for_sigv4_header_auth(req)?;
        }
        let canonical_path = service.canonical_signing_path(req.path());
        self.authenticate_with_payload_check_for_service(
            req,
            true,
            bucket,
            service.signing_service(),
            &canonical_path,
        )
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
        bucket: Option<&str>,
    ) -> Result<AuthContext, ServerError> {
        self.authenticate_with_payload_check_for_service(
            req,
            verify_payload_hash,
            bucket,
            auth::SigningService::S3,
            req.path(),
        )
    }

    fn authenticate_with_payload_check_for_service(
        &self,
        req: &S3Request,
        verify_payload_hash: bool,
        bucket: Option<&str>,
        expected_service: auth::SigningService,
        canonical_path: &str,
    ) -> Result<AuthContext, ServerError> {
        let now = current_auth_epoch_secs()?;

        let auth_result = authenticate_request(
            req.method.as_str(),
            canonical_path,
            req.query_string(),
            &req.header_source(),
            &req.body,
            &self.identity_provider,
            auth::ExpectedSigningRegion::ExactEndpointRegion(self.coordinator.region()),
            expected_service,
            now,
        );

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
            Err(auth::AuthError::InvalidHeaderCredentialRegion {
                provided_region,
                expected_region,
            }) => {
                let bucket_region_header = if let Some(bucket) = bucket {
                    let bucket = parse_bucket_name(bucket)?;
                    self.coordinator
                        .admit_storage_route_for_request()
                        .ok()
                        .is_some_and(|admission| {
                            self.coordinator
                                .bucket_exists_on_admitted_route(&admission, &bucket)
                                .unwrap_or(false)
                        })
                        || self.account_regional_bucket_region_is_known(&bucket)
                } else {
                    false
                };
                return Err(ServerError::WrongRegion {
                    provided_region,
                    expected_region,
                    bucket_region_header,
                });
            }
            Err(err) => return Err(Self::map_auth_error(err)),
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

    fn enforce_bucket_region_for_operation(
        &self,
        storage_route_admission: &storage::StorageClusterRouteAdmission,
        operation: &S3Operation,
        auth: &AuthContext,
    ) -> Result<(), ServerError> {
        let Some(bucket) = operation.bucket_name() else {
            return Ok(());
        };
        self.enforce_bucket_region(storage_route_admission, bucket, auth)
    }

    fn enforce_bucket_region(
        &self,
        storage_route_admission: &storage::StorageClusterRouteAdmission,
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
                bucket_region_header: self
                    .coordinator
                    .bucket_exists_on_admitted_route(storage_route_admission, bucket)?,
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
        storage_route_admission: &storage::StorageClusterRouteAdmission,
        bucket: &str,
        auth: &AuthContext,
    ) -> Result<(), ServerError> {
        let bucket = parse_bucket_name(bucket)?;
        self.enforce_bucket_region(storage_route_admission, &bucket, auth)
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

    /// Exercise the production aws-chunked parser and incremental decoder with
    /// a request whose complete wire body is already in memory.
    #[cfg(test)]
    fn maybe_decode_chunked(
        &self,
        req: &S3Request,
        auth: &AuthContext,
    ) -> Result<Option<S3Request>, ServerError> {
        let chunked = serve::parse_chunked_mode(req)?;
        let mut decoder = match serve::make_chunked_decoder(&chunked, auth.streaming.as_ref())? {
            Some(decoder) => decoder,
            None => return Ok(None),
        };
        let data = decoder.feed(&req.body)?;
        if !decoder.is_done() {
            return Err(ServerError::IncompleteBody);
        }
        let trailers = decoder.into_trailers();
        serve::validate_chunked_post_decode(
            &chunked,
            u64::try_from(data.len()).expect("decoded test body length fits in u64"),
            &trailers,
            req.header("x-amz-trailer"),
        )?;
        Ok(Some(req.with_decoded_body(data, trailers)))
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
        // AWS routes POST Object away from form authentication when this HTTP
        // header is present, regardless of its value. The header is therefore
        // neither decoded nor considered as a fallback form token.
        if req.header_count("x-amz-security-token") != 0 {
            return Err(ServerError::PostObjectNoAccessKeyPresented);
        }
        let header_auth = self.authenticate_with_payload_check(req, false, Some(bucket))?;

        let field = |name: &str| -> Option<&str> {
            form_fields
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.as_str())
        };
        let now = current_auth_epoch_secs()?;
        let security_tokens = form_fields
            .iter()
            .filter(|(name, _)| name.eq_ignore_ascii_case("x-amz-security-token"))
            .map(|(_, value)| value.as_str())
            .collect::<Vec<_>>();

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
                    security_tokens: &security_tokens,
                },
                &self.identity_provider,
                auth::ExpectedCredentialScope::new(
                    auth::ExpectedSigningRegion::ExactEndpointRegion(self.coordinator.region()),
                    "s3",
                ),
                now,
            )
            .map_err(Self::map_auth_error)?
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
        let storage_route_admission = self.coordinator.admit_storage_route_for_request()?;
        self.enforce_bucket_region_raw(&storage_route_admission, bucket, effective_auth)?;

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
        let tags = if let Some(tagging_field) = field("tagging") {
            let tags = xml::TagSet::parse_tagging_xml(
                tagging_field.as_bytes(),
                s3_types::MAX_OBJECT_TAGS,
            )?;
            if tags.is_empty() {
                None
            } else {
                Some(tags)
            }
        } else {
            None
        };
        let sse_customer_request =
            parse_sse_customer_form_fields(req.transport_security, form_fields)?;
        let managed_encryption =
            parse_managed_encryption_form_fields(form_fields, sse_customer_request.is_some())?;
        let cond = write_condition_from_headers(req)?;

        let requester = self.requester_from_auth(effective_auth, req)?;
        let acl = parse_put_object_acl(field("acl"));
        let request_encryption = crate::coordinator::WriteEncryptionRequest::from_request_parts(
            sse_customer_request.as_ref(),
            managed_encryption,
        )?;
        let bucket_name = parse_bucket_name(bucket)?;
        let stream_cleanup = self.coordinator.retained_stream_upload_cleanup(
            &storage_route_admission,
            &bucket_name,
            &key,
        )?;
        let prepared_put = self
            .coordinator
            .begin_stream_put_with_storage_admission_and_cleanup_deadline(
                &storage_route_admission,
                &AuthorizePutObjectRequest {
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
                    .with_request_object_tags(tags.as_ref().map(xml::TagSet::as_aws_tag_set)),
                    object_lock: ObjectLockState::default(),
                    tags: tags.as_ref().map(xml::TagSet::as_aws_tag_set),
                    encryption: request_encryption,
                },
                storage_route_admission.authority_valid_until_ms(),
            )?;

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
            storage_route_admission,
            stream_cleanup,
            session_id: prepared_put.session_id,
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
            .finalize_authorized_stream_put_with_storage_admission(
                &ctx.storage_route_admission,
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
            auth::PostPolicyError::ConditionExpressionFailed { expression } => {
                ServerError::PostPolicyConditionAccessDenied { expression }
            }
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
        self.coordinator
            .append_stream_put_data_with_storage_admission(
                &ctx.storage_route_admission,
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
        let _ = self
            .coordinator
            .abort_stream_upload_with_retained_cleanup(&ctx.stream_cleanup, ctx.session_id());
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
        let auth = self.authenticate_with_payload_check(req, false, Some(bucket))?;
        let storage_route_admission = self.coordinator.admit_storage_route_for_request()?;
        self.enforce_bucket_region_raw(&storage_route_admission, bucket, &auth)?;
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
        let inline_tags = if let Some(tagging_header) = req.header("x-amz-tagging") {
            let tags = xml::TagSet::new(
                xml::parse_url_encoded_tags(tagging_header)?,
                s3_types::MAX_OBJECT_TAGS,
            )?;
            if tags.is_empty() {
                None
            } else {
                Some(tags)
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
        let requester = self.requester_from_auth(&auth, req)?;
        let stream_cleanup = self.coordinator.retained_stream_upload_cleanup(
            &storage_route_admission,
            &bucket_name,
            &object_key,
        )?;
        let authorized_write = self
            .coordinator
            .prepare_put_object_write_with_storage_admission(
                &storage_route_admission,
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
                            tags: inline_tags.as_ref().map(xml::TagSet::as_aws_tag_set),
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
                    tags: inline_tags.as_ref().map(xml::TagSet::as_aws_tag_set),
                    encryption: crate::coordinator::WriteEncryptionRequest::from_request_parts(
                        sse_customer_request.as_ref(),
                        managed_encryption,
                    )?,
                },
            )?;

        Ok(StreamingPutContext {
            trace: current_trace_context(),
            storage_route_admission,
            stream_cleanup,
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
            auth_mode: auth.mode,
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
                .begin_stream_put_session_with_storage_admission_and_cleanup_deadline(
                    &ctx.storage_route_admission,
                    authorized_write,
                    ctx.storage_route_admission.authority_valid_until_ms(),
                )
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
        self.coordinator
            .append_stream_put_data_with_storage_admission(
                &ctx.storage_route_admission,
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
            .heartbeat_stream_put_session_with_storage_admission(
                &ctx.storage_route_admission,
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
            .heartbeat_stream_put_session_with_storage_admission(
                &ctx.storage_route_admission,
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
            self.coordinator
                .commit_put_object_write_with_storage_admission(
                    &ctx.storage_route_admission,
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
                .finalize_authorized_stream_put_with_storage_admission(
                    &ctx.storage_route_admission,
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
        let _ = self
            .coordinator
            .abort_stream_upload_with_retained_cleanup(&ctx.stream_cleanup, session_id);
    }

    /// Prepare a streaming `UploadPart` session.
    fn prepare_streaming_part(
        &self,
        req: &S3Request,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: &str,
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
        let auth = self.authenticate_with_payload_check(req, false, Some(bucket))?;
        let storage_route_admission = self.coordinator.admit_storage_route_for_request()?;
        self.enforce_bucket_region_raw(&storage_route_admission, bucket, &auth)?;

        let requester = self.requester_from_auth(&auth, req)?;
        let expected_bucket_owner = expected_bucket_owner(req).map(str::to_string);
        let bucket_name = parse_bucket_name(bucket)?;
        let upload = multipart_object_request(
            &bucket_name,
            key,
            parse_present_upload_id(upload_id)?,
            requester.clone(),
            expected_bucket_owner.as_deref(),
        )?;
        self.coordinator
            .validate_in_progress_multipart_upload_target_on_admitted_route(
                &storage_route_admission,
                &upload,
            )?;

        validate_request_checksum_headers(req, false, false, false)?;
        let content_md5 = ContentMd5Claim::from_request(req)?;
        let claimed_checksum = extract_checksum_header(req)?;
        let sse_customer_request = parse_sse_customer_request(req)?;
        let mut checksum_response: Vec<(String, String)> = Vec::new();
        for (_, header) in checksum_headers() {
            if let Some(val) = req.header(header) {
                checksum_response.push((header.to_string(), val.to_string()));
            }
        }

        let part_number = request::parse_upload_part_number_value(part_number)?;
        let binding_upload_id = upload.upload_id().clone();
        let binding_bucket = upload.object.bucket.name.clone();
        let binding_key = upload.object.key.clone();
        let stream_cleanup = self.coordinator.retained_stream_upload_cleanup(
            &storage_route_admission,
            &binding_bucket,
            &binding_key,
        )?;
        let begin = self.coordinator.begin_stream_part_on_admitted_route(
            &storage_route_admission,
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
            storage_route_admission,
            stream_cleanup,
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
            auth_mode: auth.mode,
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
        self.coordinator.append_stream_part_data_on_admitted_route(
            &ctx.storage_route_admission,
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

        let result = self
            .coordinator
            .finalize_stream_part_with_storage_admission(
                &ctx.storage_route_admission,
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
            .abort_stream_upload_with_retained_cleanup(&ctx.stream_cleanup, ctx.session_id());
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
    storage_route_admission: storage::StorageClusterRouteAdmission,
    stream_cleanup: storage::RetainedStreamUploadCleanup,
    bucket: BucketName,
    key: ObjectKey,
    metadata_blob: crate::metadata_blob::MetadataBlob,
    system_metadata: SystemMetadata,
    cond: crate::conditional::WriteCondition,
    checksum: StreamingPutChecksumContract,
    sse_customer: Option<SseCustomerRequest>,
    authorized_write: RwLock<AuthorizedPutObjectWrite>,
    /// Authentication mode controls whether streaming payload markers activate
    /// aws-chunked decoding. AWS only does so for header SigV4.
    auth_mode: auth::AuthMode,
    /// Signing context for aws-chunked modes, None for unsigned/plain.
    streaming_signing: Option<auth::StreamingSigningContext>,
}

/// Context for an in-progress streaming `PostObject`.
struct StreamingPostContext {
    trace: observability::TraceContext,
    storage_route_admission: storage::StorageClusterRouteAdmission,
    stream_cleanup: storage::RetainedStreamUploadCleanup,
    session_id: SessionId,
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
    storage_route_admission: storage::StorageClusterRouteAdmission,
    stream_cleanup: storage::RetainedStreamUploadCleanup,
    binding: StreamPartBinding,
    requester: crate::coordinator::Requester,
    expected_bucket_owner: Option<String>,
    checksum: StreamingPartChecksumContract,
    sse_customer: Option<SseCustomerWriteContext>,
    /// Authentication mode controls whether streaming payload markers activate
    /// aws-chunked decoding. AWS only does so for header SigV4.
    auth_mode: auth::AuthMode,
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
    fn storage_route_admission(&self) -> &storage::StorageClusterRouteAdmission {
        &self.storage_route_admission
    }

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
    fn storage_route_admission(&self) -> &storage::StorageClusterRouteAdmission {
        &self.storage_route_admission
    }

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
    fn storage_route_admission(&self) -> &storage::StorageClusterRouteAdmission {
        &self.storage_route_admission
    }

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
pub(crate) fn s3_response_to_hyper(
    resp: S3Response,
    admission: Option<HttpRequestAdmission>,
    stream_read_chunk_size: usize,
    panic_on_500: bool,
    abort_on_500: bool,
    trace_meta: ResponseTraceMeta,
) -> http::Response<S3HyperBody> {
    let (permit, inflight_requests_guard) = admission
        .map(HttpRequestAdmission::into_parts)
        .unwrap_or((None, None));

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
        inflight_requests_guard: Option<observability::InflightRequestsGuard>,
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
                format!(
                    " cause_label={} cause_chain={}",
                    diagnostic.cause_label, diagnostic.cause_chain
                )
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
                inflight_requests_guard,
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
                    inflight_requests_guard,
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
                    inflight_requests_guard,
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
    if resp.include_wire_ids && !has_request_id_header {
        validated_headers.push((
            http::header::HeaderName::from_static("x-amz-request-id"),
            http::header::HeaderValue::from_str(trace_meta.context.request_id())
                .expect("request id is a valid header value"),
        ));
    }
    if resp.include_wire_ids && !has_host_id_header {
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

fn parse_s3_list_limit(
    raw: Option<impl AsRef<str>>,
    argument_name: &str,
) -> Result<u32, ServerError> {
    Ok(parse_optional_s3_list_integer(raw, argument_name)?
        .unwrap_or(1000)
        .min(S3_MAX_LIST_KEYS))
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

    let actual_md5 = argmin_crypto::digest::md5(&customer_key_bytes);
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

    let actual_md5 = argmin_crypto::digest::md5(&customer_key_bytes);
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
            Err(ServerError::ContentMd5Mismatch)
        }
    }
}

fn validate_content_md5(req: &S3Request) -> Result<(), ServerError> {
    let Some(claim) = ContentMd5Claim::from_request(req)? else {
        return Ok(());
    };
    let actual = argmin_crypto::digest::md5(&req.body);
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
    tags: Option<&'a s3_types::TagSet>,
    copy_source: Option<&'a str>,
    metadata_directive: Option<&'a str>,
    canned_acl: Option<&'a str>,
    managed_encryption: Option<ManagedEncryptionAlgorithm>,
) -> crate::coordinator::PutObjectPolicyContext<'a> {
    put_object_policy_context_from_request_fields(PutObjectPolicyContextFields {
        tags,
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
    tags: Option<&'a s3_types::TagSet>,
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
    .with_request_object_tags(fields.tags)
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

include!("tests.rs");
