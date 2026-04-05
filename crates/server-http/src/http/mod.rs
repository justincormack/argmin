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
    task::{Context, Poll},
};

use auth::{
    authenticate_request, authenticate_request_allow_wrong_region, AuthContext, AuthMode,
    CredentialStore,
};
use bytes::Bytes;
use hyper::body::{Body, Frame, SizeHint};

use crate::coordinator::BeginStreamPartRequest;
use crate::coordinator::BeginStreamPutRequest;
use crate::coordinator::BucketRequest;
use crate::coordinator::ChecksumClaim;
use crate::coordinator::Coordinator;
use crate::coordinator::CopyObjectRequest;
use crate::coordinator::CopySource;
use crate::coordinator::EncodedChecksumClaim;
use crate::coordinator::FinalizeStreamPartRequest;
use crate::coordinator::FinalizeStreamPutRequest;
use crate::coordinator::MetadataDirective;
use crate::coordinator::MultipartObjectRequest;
use crate::coordinator::ObjectRequest;
use crate::coordinator::ObjectVersionRequest;
use crate::coordinator::TaggingDirective;
use crate::coordinator::UploadPartCopyRequest;
use crate::error::ServerError;
use crate::metadata_blob::MetadataBlob;
use checksum::{ChecksumAlgorithm, ChecksumType, MultipartChecksumConfig, RawChecksum};
use conditional::{
    copy_source_condition_from_headers, delete_condition_from_headers, read_condition_from_headers,
    write_condition_from_headers,
};
use md5_legacy::Digest;
use request::{S3Request, TransportSecurity};
use response::S3Response;
use router::{route, S3Operation};
use s3_types::{
    requires_sigv4, LegalHoldStatus, ObjectLockMode, ObjectLockState, ObjectRetention,
    StoredLegalHoldStatus, VersionId,
};
use server_core::sse::{
    SseCustomerRequest, SseCustomerWriteContext, SSE_CUSTOMER_ALGORITHM, SSE_C_CUSTOMER_KEY_LEN,
};
use server_core::system_metadata::SystemMetadata;
use storage::ManagedEncryptionAlgorithm;
use tokio::sync::{mpsc, OwnedSemaphorePermit};

const TRACE_TARGET: &str = "server_http";

fn current_trace_context() -> observability::TraceContext {
    observability::current_context().unwrap_or_else(observability::TraceContext::new_request)
}

/// Parse versionId query parameter from an S3 request.
/// Returns `Ok(None)` if the parameter is absent, `Ok(Some(id))` if valid,
/// or `Err` if the value is present but not a valid version ID.
/// Parse a version-id string into a typed `VersionId`.
fn parse_version_id_str(v: &str) -> Result<VersionId, ServerError> {
    if v == "null" {
        Ok(VersionId::Null)
    } else {
        v.parse::<u64>()
            .map(VersionId::from_u64)
            .map_err(|_| ServerError::InvalidArgument {
                reason: format!("invalid versionId: {v}"),
            })
    }
}

fn parse_request_metadata<'a, I>(headers: I) -> Result<(MetadataBlob, SystemMetadata), ServerError>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let headers: Vec<(&str, &str)> = headers.into_iter().collect();
    Ok((
        MetadataBlob::from_header_iter(headers.iter().copied())?,
        SystemMetadata::from_header_iter(headers)?,
    ))
}

fn parse_version_id(req: &S3Request) -> Result<Option<VersionId>, ServerError> {
    match req.query_param_lossy("versionId") {
        None => Ok(None),
        Some(v) => parse_version_id_str(&v).map(Some),
    }
}

fn ensure_lifecycle_rule_ids(
    mut config: storage::BucketLifecycleConfiguration,
) -> Result<storage::BucketLifecycleConfiguration, ServerError> {
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
    pub coordinator: Coordinator,
    pub credentials: CredentialStore,
}

enum S3HyperBodyState {
    Buffered(Option<Bytes>),
    Streaming(mpsc::Receiver<Result<Bytes, ServerError>>),
}

#[derive(Clone)]
pub struct ResponseTraceMeta {
    context: observability::TraceContext,
    method: String,
    path: String,
    query: String,
    started_at: Instant,
}

impl ResponseTraceMeta {
    #[must_use]
    pub fn new(
        context: observability::TraceContext,
        method: impl Into<String>,
        path: impl Into<String>,
        query: impl Into<String>,
    ) -> Self {
        Self {
            context,
            method: method.into(),
            path: path.into(),
            query: query.into(),
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
    first_chunk_emitted: bool,
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
            first_chunk_emitted: false,
            terminal_event_emitted: false,
        }
    }

    fn worker_context(&self) -> observability::TraceContext {
        self.meta.context.clone()
    }

    fn emit_first_chunk(&mut self, len: usize) {
        if self.first_chunk_emitted {
            return;
        }
        self.first_chunk_emitted = true;
        let _ = observability::event_in_context(
            &self.meta.context,
            TRACE_TARGET,
            "response_first_chunk",
            Some(format_args!(
                "status={} method={} path={} query={} streaming={} body_len={} first_chunk_len={} lifetime_us={}",
                self.status_code,
                self.meta.method,
                self.meta.path,
                self.meta.query,
                self.streaming,
                self.body_len,
                len,
                self.meta.started_at.elapsed().as_micros()
            )),
        );
    }

    fn record_bytes(&mut self, len: usize) {
        if len > 0 {
            self.emit_first_chunk(len);
        }
        self.bytes_sent += len as u64;
    }

    fn emit_complete(&mut self) {
        if self.terminal_event_emitted {
            return;
        }
        self.terminal_event_emitted = true;
        let _ = observability::event_in_context(
            &self.meta.context,
            TRACE_TARGET,
            "response_body_complete",
            Some(format_args!(
                "status={} method={} path={} query={} streaming={} body_len={} bytes_sent={} lifetime_us={}",
                self.status_code,
                self.meta.method,
                self.meta.path,
                self.meta.query,
                self.streaming,
                self.body_len,
                self.bytes_sent,
                self.meta.started_at.elapsed().as_micros()
            )),
        );
    }

    fn emit_error(&mut self, err: &ServerError) {
        if self.terminal_event_emitted {
            return;
        }
        self.terminal_event_emitted = true;
        let _ = observability::event_in_context(
            &self.meta.context,
            TRACE_TARGET,
            "response_body_error",
            Some(format_args!(
                "status={} method={} path={} query={} streaming={} body_len={} bytes_sent={} lifetime_us={} error={}",
                self.status_code,
                self.meta.method,
                self.meta.path,
                self.meta.query,
                self.streaming,
                self.body_len,
                self.bytes_sent,
                self.meta.started_at.elapsed().as_micros(),
                err
            )),
        );
    }

    fn emit_dropped(&mut self) {
        if self.terminal_event_emitted {
            return;
        }
        self.terminal_event_emitted = true;
        let _ = observability::event_in_context(
            &self.meta.context,
            TRACE_TARGET,
            "response_body_dropped",
            Some(format_args!(
                "status={} method={} path={} query={} streaming={} body_len={} bytes_sent={} lifetime_us={}",
                self.status_code,
                self.meta.method,
                self.meta.path,
                self.meta.query,
                self.streaming,
                self.body_len,
                self.bytes_sent,
                self.meta.started_at.elapsed().as_micros()
            )),
        );
    }
}

pub struct S3HyperBody {
    state: S3HyperBodyState,
    trace: Option<ResponseBodyTrace>,
    _permit: Option<OwnedSemaphorePermit>,
}

impl S3HyperBody {
    fn buffered(
        body: Vec<u8>,
        permit: Option<OwnedSemaphorePermit>,
        trace: ResponseBodyTrace,
    ) -> Self {
        Self {
            state: S3HyperBodyState::Buffered(Some(Bytes::from(body))),
            trace: Some(trace),
            _permit: permit,
        }
    }

    fn streaming(
        body: crate::coordinator::ReadHandle,
        permit: OwnedSemaphorePermit,
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
            _permit: Some(permit),
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
                        trace.emit_complete();
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
                        trace.emit_complete();
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
            trace.emit_complete();
        } else {
            trace.emit_dropped();
        }
    }
}

impl HttpFrontend {
    /// Handle a parsed S3 request: authenticate, dispatch, and return the response.
    ///
    /// The caller (serve layer) is responsible for parsing the HTTP request into
    /// an `S3Request` and converting the `S3Response` back to an HTTP response.
    #[must_use]
    pub fn handle_s3_request(&self, s3req: &S3Request) -> S3Response {
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::handle_s3_request",
            "method={} path={} query={}",
            s3req.method,
            s3req.path(),
            s3req.query_string()
        );
        // Route first to detect OPTIONS requests (which bypass auth).
        let operation = {
            observability::trace_scope!(
                TRACE_TARGET,
                "HttpFrontend::route_request",
                "method={} path={} query={}",
                s3req.method.as_str(),
                s3req.path(),
                s3req.query_string()
            );
            match route(s3req.method.as_str(), s3req.path(), s3req.query_string()) {
                Ok(op) => op,
                Err(err) => return S3Response::error(&err, s3req.path()),
            }
        };

        // OPTIONS (preflight CORS) bypasses authentication.
        if let S3Operation::OptionsRequest { ref bucket, .. } = operation {
            return self.handle_options_request(s3req, bucket);
        }

        let defer_region_check = self.should_defer_region_check(&operation);
        let auth = {
            observability::trace_scope!(
                TRACE_TARGET,
                "HttpFrontend::authenticate",
                "method={} path={}",
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

        let mut resp = {
            observability::trace_scope!(
                TRACE_TARGET,
                "HttpFrontend::map_dispatch_result",
                "method={} path={}",
                s3req.method.as_str(),
                s3req.path()
            );
            match result {
                Ok(resp) => resp,
                Err(ServerError::NotModified {
                    ref etag,
                    last_modified,
                }) => S3Response::not_modified(etag, last_modified),
                Err(ServerError::PreconditionFailed) => S3Response::precondition_failed(),
                Err(ref err @ ServerError::DeleteMarkerHit { .. }) => {
                    let mut resp = S3Response::error(err, s3req.path());
                    resp.headers
                        .push(("x-amz-delete-marker".to_string(), "true".to_string()));
                    resp
                }
                Err(err) => S3Response::error(&err, s3req.path()),
            }
        };

        // CORS response headers on actual (non-preflight) requests.
        if let Some(origin) = s3req.header("origin") {
            let bucket = self.extract_bucket_from_path(s3req.path());
            if let Some(bucket) = bucket {
                observability::trace_scope!(
                    TRACE_TARGET,
                    "HttpFrontend::apply_actual_cors",
                    "method={} path={} bucket={}",
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
    fn handle_options_request(&self, req: &S3Request, bucket: &str) -> S3Response {
        let origin = match req.header("origin") {
            Some(o) => o,
            None => {
                return S3Response::error(
                    &ServerError::InvalidRequest {
                        reason: "Insufficient information. Origin request header needed."
                            .to_string(),
                    },
                    req.path(),
                );
            }
        };

        let request_method = match req.header("access-control-request-method") {
            Some(m) => m,
            None => return S3Response::forbidden(),
        };

        let request_headers_str = req.header("access-control-request-headers");
        let request_headers: Vec<&str> = request_headers_str
            .map(|h| h.split(',').map(str::trim).collect())
            .unwrap_or_default();

        // Load CORS config
        let cors_config_xml = match self.coordinator.load_bucket_cors_config(bucket) {
            Ok(Some(xml)) => xml,
            _ => return S3Response::forbidden(),
        };
        let config = match crate::http::xml::parse_cors_config_xml(cors_config_xml.as_bytes()) {
            Ok(c) => c,
            Err(_) => return S3Response::forbidden(),
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
            None => S3Response::forbidden(),
        }
    }

    /// Apply CORS headers to an actual (non-preflight) response if the request
    /// has an Origin header and a matching CORS rule exists.
    pub(crate) fn actual_cors_headers(
        &self,
        bucket: &str,
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

    fn apply_cors_headers(&self, resp: &mut S3Response, bucket: &str, origin: &str, method: &str) {
        for (k, v) in self.actual_cors_headers(bucket, origin, method) {
            resp.headers.push((k, v));
        }
    }

    /// Extract bucket name from the request path (first path segment).
    fn extract_bucket_from_path(&self, path: &str) -> Option<String> {
        let trimmed = path.strip_prefix('/').unwrap_or(path);
        if trimmed.is_empty() {
            return None;
        }
        let bucket = match trimmed.find('/') {
            Some(pos) => &trimmed[..pos],
            None => trimmed,
        };
        if bucket.is_empty() {
            None
        } else {
            Some(bucket.to_string())
        }
    }

    fn requester_from_auth(auth: &AuthContext) -> crate::coordinator::Requester {
        crate::coordinator::Requester::from_account(auth.account.as_ref())
    }

    fn authenticated_account(
        auth: &AuthContext,
    ) -> Result<&s3_types::AccountIdentity, ServerError> {
        auth.account.as_ref().ok_or(ServerError::AccessDenied)
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
                };
                let initiator_identity = upload.initiator.unwrap_or_else(|| upload.owner.clone());
                let initiator = xml::RenderedCanonicalUser {
                    canonical_id: initiator_identity.canonical_id.clone(),
                };
                xml::RenderedMultipartUploadEntry {
                    key: upload.key,
                    upload_id: upload.upload_id,
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
            next_upload_id_marker: result.next_upload_id_marker,
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
            "method={} path={} op={:?} principal={:?}",
            req.method.as_str(),
            req.path(),
            operation,
            auth.principal()
        );
        let expected_bucket_owner = expected_bucket_owner(req);
        // Dispatch to coordinator
        match operation {
            S3Operation::ListBuckets => {
                let requester = Self::requester_from_auth(auth);
                let owner_account = Self::authenticated_account(auth)?;
                let buckets = self
                    .coordinator
                    .list_buckets(&crate::coordinator::ListBucketsRequest { requester })?;
                Ok(S3Response::list_buckets(
                    &buckets,
                    owner_account.display_name(),
                    owner_account.canonical_user_id(),
                ))
            }
            S3Operation::CreateBucket { bucket } => {
                let acl = parse_create_bucket_acl(req)?;
                let object_lock_enabled = parse_bucket_object_lock_enabled(
                    req.header("x-amz-bucket-object-lock-enabled"),
                )?;
                let ownership = parse_bucket_ownership(req.header("x-amz-object-ownership"))?;
                let requester = Self::requester_from_auth(auth);
                self.coordinator
                    .create_bucket(&crate::coordinator::CreateBucketRequest {
                        name: &bucket,
                        requester,
                        acl,
                        ownership,
                        object_lock_enabled,
                    })?;
                Ok(S3Response::create_bucket(&bucket))
            }
            S3Operation::DeleteBucket { bucket } => {
                let requester = Self::requester_from_auth(auth);
                self.coordinator
                    .delete_bucket(&crate::coordinator::BucketRequest {
                        name: &bucket,
                        requester,
                        expected_bucket_owner,
                    })?;
                Ok(S3Response::delete_bucket())
            }
            S3Operation::HeadBucket { bucket } => {
                let requester = Self::requester_from_auth(auth);
                let info = self
                    .coordinator
                    .head_bucket(&crate::coordinator::BucketRequest {
                        name: &bucket,
                        requester,
                        expected_bucket_owner,
                    })?;
                Ok(S3Response::head_bucket(&info, self.coordinator.region()))
            }
            S3Operation::GetBucketLocation { bucket } => {
                let requester = Self::requester_from_auth(auth);
                let _info = self
                    .coordinator
                    .head_bucket(&crate::coordinator::BucketRequest {
                        name: &bucket,
                        requester,
                        expected_bucket_owner,
                    })?;
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
                let max_keys: u32 = parse_max_keys(req.query_param_lossy("max-keys"))?;
                let requester = Self::requester_from_auth(auth);

                let result = self.coordinator.list_objects_v2(
                    &crate::coordinator::ListObjectsV2Request {
                        bucket: BucketRequest::new(&bucket, requester, expected_bucket_owner),
                        prefix: prefix.as_deref(),
                        delimiter: delimiter.as_deref(),
                        continuation_token: marker.as_deref(),
                        max_keys,
                    },
                )?;
                Ok(S3Response::list_objects_v1(
                    &bucket,
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
                let max_keys: u32 = parse_max_keys(req.query_param_lossy("max-keys"))?;
                let requester = Self::requester_from_auth(auth);

                let result = self.coordinator.list_objects_v2(
                    &crate::coordinator::ListObjectsV2Request {
                        bucket: BucketRequest::new(&bucket, requester, expected_bucket_owner),
                        prefix: prefix.as_deref(),
                        delimiter: delimiter.as_deref(),
                        continuation_token,
                        max_keys,
                    },
                )?;
                Ok(S3Response::list_objects_v2(
                    &bucket,
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
                    let (src_bucket, src_key, src_version_id_str) =
                        request::parse_copy_source(copy_source)?;
                    let src_version_id = match src_version_id_str {
                        None => None,
                        Some(v) if v == "null" => Some(VersionId::Null),
                        Some(v) => Some(VersionId::from_u64(v.parse::<u64>().map_err(|_| {
                            ServerError::InvalidArgument {
                                reason: format!("invalid versionId in copy source: {v}"),
                            }
                        })?)),
                    };
                    let requester = Self::requester_from_auth(auth);
                    let source_sse_customer = parse_sse_customer_copy_source_request(req)?;
                    let dst_sse_customer = parse_sse_customer_request(req)?;
                    let destination_managed_encryption =
                        parse_managed_encryption_request(req, dst_sse_customer.is_some())?;
                    let object_lock = parse_object_lock_headers(req)?;
                    let acl = parse_put_object_write_acl(req)?;
                    let src_cond = copy_source_condition_from_headers(req);
                    let dst_cond = write_condition_from_headers(req)?;
                    // Parse metadata and checksum algorithm at the HTTP boundary
                    // so the coordinator never sees raw headers.
                    let replace_metadata;
                    let replace_system_metadata;
                    let replace_checksum_algo;
                    let directive = match req.header("x-amz-metadata-directive") {
                        Some(d) if d.eq_ignore_ascii_case("REPLACE") => {
                            let (blob, mut system_metadata) =
                                parse_request_metadata(req.header_iter())?;
                            system_metadata.strip_checksum_values();
                            replace_metadata = blob;
                            replace_system_metadata = system_metadata;

                            // Parse checksum algorithm if present.
                            replace_checksum_algo = match req.header("x-amz-checksum-algorithm") {
                                None => None,
                                Some(v) => Some(ChecksumAlgorithm::parse(v).ok_or_else(|| {
                                    ServerError::InvalidArgument {
                                        reason: format!("unsupported checksum algorithm: {v}"),
                                    }
                                })?),
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
                        source: CopySource {
                            bucket: &src_bucket,
                            key: &src_key,
                            version_id: src_version_id,
                            condition: &src_cond,
                            expected_bucket_owner: expected_source_bucket_owner(req),
                        },
                        destination: ObjectRequest::new(
                            &bucket,
                            &key,
                            requester,
                            expected_bucket_owner,
                        ),
                        dst_condition: &dst_cond,
                        directive,
                        tagging,
                        acl,
                        policy_context,
                        source_sse_customer: source_sse_customer.as_ref(),
                        destination_encryption:
                            crate::coordinator::WriteEncryptionRequest::from_request_parts(
                                dst_sse_customer.as_ref(),
                                destination_managed_encryption,
                            ),
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
                    let (metadata_blob, system_metadata) =
                        parse_request_metadata(req.header_iter())?;
                    let cond = write_condition_from_headers(req)?;
                    let requester = Self::requester_from_auth(auth);
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
                                object: ObjectRequest::new(
                                    &bucket,
                                    &key,
                                    requester,
                                    expected_bucket_owner,
                                ),
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
                                    ),
                            })?;
                    let mut resp = S3Response::put_object(&result);
                    apply_sse_customer_write_response_headers(&mut resp, sse_customer.as_ref());
                    for &(_, header) in CHECKSUM_HEADERS {
                        if let Some(value) = req.header(header) {
                            resp.headers.push((header.to_string(), value.to_string()));
                        }
                    }
                    Ok(resp)
                }
            }
            S3Operation::GetObject { bucket, key } => {
                reject_managed_encryption_read_headers(req)?;
                let sse_customer = parse_sse_customer_request(req)?;
                let cond = read_condition_from_headers(req);
                let vid = parse_version_id(req)?;
                let requester = Self::requester_from_auth(auth);
                let trace = current_trace_context();
                if let Some(pn_str) = req.query_param_lossy("partNumber") {
                    if req.header("range").is_some() {
                        return Err(ServerError::InvalidRequest {
                            reason:
                                "Cannot specify both Range header and partNumber query parameter"
                                    .to_string(),
                        });
                    }
                    let part_number: u32 =
                        pn_str.parse().map_err(|_| ServerError::InvalidArgument {
                            reason: "partNumber must be a positive integer".into(),
                        })?;
                    if part_number == 0 {
                        return Err(ServerError::InvalidArgument {
                            reason: "partNumber must be >= 1".into(),
                        });
                    }
                    let _ = observability::event_in_context(
                        &trace,
                        TRACE_TARGET,
                        "get_object_part_request",
                        Some(format_args!(
                            "bucket={} key={} version_id={:?} part_number={}",
                            bucket, key, vid, part_number
                        )),
                    );
                    let result = self
                        .coordinator
                        .get_object_part(&crate::coordinator::GetObjectPartRequest {
                            object: ObjectVersionRequest::new(
                                &bucket,
                                &key,
                                vid,
                                requester,
                                expected_bucket_owner,
                            ),
                            part_number,
                            cond: &cond,
                            sse_customer: sse_customer.as_ref(),
                        })
                        .map_err(|e| match e {
                            ServerError::InvalidPart { .. } => {
                                ServerError::InvalidRange { total_size: 0 }
                            }
                            other => other,
                        })?;
                    let tags = result.tags.clone();
                    let mut resp = S3Response::get_object_part(result);
                    apply_response_overrides(&mut resp, req);
                    if let Some(tags_xml) = tags {
                        add_tagging_count_header(&mut resp, &tags_xml)?;
                    }
                    Ok(resp)
                } else if let Some(range_header) = req.header("range") {
                    match crate::range::ByteRange::parse(range_header) {
                        Ok(byte_range) => {
                            let _ = observability::event_in_context(
                                &trace,
                                TRACE_TARGET,
                                "get_object_range_request",
                                Some(format_args!(
                                    "bucket={} key={} version_id={:?} raw_range={} parsed_range={}",
                                    bucket, key, vid, range_header, byte_range
                                )),
                            );
                            match self.coordinator.get_object_range(
                                &crate::coordinator::GetObjectRangeRequest {
                                    object: ObjectVersionRequest::new(
                                        &bucket,
                                        &key,
                                        vid,
                                        requester,
                                        expected_bucket_owner,
                                    ),
                                    range: byte_range,
                                    cond: &cond,
                                    sse_customer: sse_customer.as_ref(),
                                },
                            ) {
                                Ok(result) => {
                                    let tags = result.tags.clone();
                                    let mut resp = S3Response::get_object_range(result);
                                    if let Some(tags_xml) = tags {
                                        add_tagging_count_header(&mut resp, &tags_xml)?;
                                    }
                                    Ok(resp)
                                }
                                Err(ServerError::InvalidRange { total_size }) => {
                                    Ok(S3Response::range_not_satisfiable(total_size))
                                }
                                Err(e) => Err(e),
                            }
                        }
                        Err(_) => {
                            let _ = observability::event_in_context(
                                &trace,
                                TRACE_TARGET,
                                "get_object_range_ignored",
                                Some(format_args!(
                                    "bucket={} key={} version_id={:?} raw_range={} reason=invalid_header",
                                    bucket,
                                    key,
                                    vid,
                                    range_header
                                )),
                            );
                            let result = self.coordinator.get_object(
                                &crate::coordinator::GetObjectRequest {
                                    object: ObjectVersionRequest::new(
                                        &bucket,
                                        &key,
                                        vid,
                                        requester,
                                        expected_bucket_owner,
                                    ),
                                    cond: &cond,
                                    sse_customer: sse_customer.as_ref(),
                                },
                            )?;
                            let checksum_mode = req.header("x-amz-checksum-mode");
                            let tags = result.tags.clone();
                            let mut resp = S3Response::get_object(result, checksum_mode);
                            apply_response_overrides(&mut resp, req);
                            if let Some(tags_xml) = tags {
                                add_tagging_count_header(&mut resp, &tags_xml)?;
                            }
                            Ok(resp)
                        }
                    }
                } else {
                    let result =
                        self.coordinator
                            .get_object(&crate::coordinator::GetObjectRequest {
                                object: ObjectVersionRequest::new(
                                    &bucket,
                                    &key,
                                    vid,
                                    requester,
                                    expected_bucket_owner,
                                ),
                                cond: &cond,
                                sse_customer: sse_customer.as_ref(),
                            })?;
                    let checksum_mode = req.header("x-amz-checksum-mode");
                    let tags = result.tags.clone();
                    let mut resp = S3Response::get_object(result, checksum_mode);
                    apply_response_overrides(&mut resp, req);
                    // Add x-amz-tagging-count if the object has tags
                    if let Some(tags_xml) = tags {
                        add_tagging_count_header(&mut resp, &tags_xml)?;
                    }
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
                let requester = Self::requester_from_auth(auth);
                let result =
                    self.coordinator
                        .delete_object(&crate::coordinator::DeleteObjectRequest {
                            object: ObjectVersionRequest::new(
                                &bucket,
                                &key,
                                vid,
                                requester,
                                expected_bucket_owner,
                            ),
                            bypass_governance,
                            cond: &cond,
                        })?;
                Ok(S3Response::delete_object(&result))
            }
            S3Operation::HeadObject { bucket, key } => {
                reject_managed_encryption_read_headers(req)?;
                let sse_customer = parse_sse_customer_request(req)?;
                let cond = read_condition_from_headers(req);
                let vid = parse_version_id(req)?;
                let requester = Self::requester_from_auth(auth);
                let trace = current_trace_context();
                if let Some(pn_str) = req.query_param_lossy("partNumber") {
                    let part_number: u32 =
                        pn_str.parse().map_err(|_| ServerError::InvalidArgument {
                            reason: "partNumber must be a positive integer".into(),
                        })?;
                    if part_number == 0 {
                        return Err(ServerError::InvalidArgument {
                            reason: "partNumber must be >= 1".into(),
                        });
                    }
                    let _ = observability::event_in_context(
                        &trace,
                        TRACE_TARGET,
                        "head_object_part_request",
                        Some(format_args!(
                            "bucket={} key={} version_id={:?} part_number={}",
                            bucket, key, vid, part_number
                        )),
                    );
                    let result = self
                        .coordinator
                        .head_object_part(&crate::coordinator::GetObjectPartRequest {
                            object: ObjectVersionRequest::new(
                                &bucket,
                                &key,
                                vid,
                                requester,
                                expected_bucket_owner,
                            ),
                            part_number,
                            cond: &cond,
                            sse_customer: sse_customer.as_ref(),
                        })
                        .map_err(|e| match e {
                            ServerError::InvalidPart { .. } => {
                                ServerError::InvalidRange { total_size: 0 }
                            }
                            other => other,
                        })?;
                    let mut resp = S3Response::head_object_part(&result);
                    if let Some(tags_xml) = &result.tags {
                        add_tagging_count_header(&mut resp, tags_xml)?;
                    }
                    Ok(resp)
                } else {
                    let result =
                        self.coordinator
                            .head_object(&crate::coordinator::GetObjectRequest {
                                object: ObjectVersionRequest::new(
                                    &bucket,
                                    &key,
                                    vid,
                                    requester,
                                    expected_bucket_owner,
                                ),
                                cond: &cond,
                                sse_customer: sse_customer.as_ref(),
                            })?;
                    let checksum_mode = req.header("x-amz-checksum-mode");
                    let mut resp = S3Response::head_object(&result, checksum_mode);
                    if let Some(tags_xml) = &result.tags {
                        add_tagging_count_header(&mut resp, tags_xml)?;
                    }
                    Ok(resp)
                }
            }
            S3Operation::GetObjectAttributes { bucket, key } => {
                reject_managed_encryption_read_headers(req)?;
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
                let max_parts: u32 = match req.header("x-amz-max-parts") {
                    None => 1000,
                    Some(v) => v.parse().map_err(|_| ServerError::InvalidArgument {
                        reason: "invalid x-amz-max-parts".to_string(),
                    })?,
                };
                let part_number_marker: Option<u32> = req
                    .header("x-amz-part-number-marker")
                    .map(|v| {
                        v.parse().map_err(|_| ServerError::InvalidArgument {
                            reason: "x-amz-part-number-marker must be an integer".to_string(),
                        })
                    })
                    .transpose()?;

                let cond = read_condition_from_headers(req);
                let vid = parse_version_id(req)?;
                let requester_ctx = Self::requester_from_auth(auth);
                let result = self.coordinator.get_object_attributes(
                    &crate::coordinator::GetObjectAttributesRequest {
                        object: ObjectVersionRequest::new(
                            &bucket,
                            &key,
                            vid,
                            requester_ctx,
                            expected_bucket_owner,
                        ),
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
                    result.managed_encryption,
                    result.sse_customer.as_ref(),
                ))
            }
            S3Operation::DeleteObjects { bucket } => {
                require_request_checksum(
                    req,
                    RequestChecksumRequirement::ContentMd5OrChecksumHeader,
                )?;
                let bypass_governance = parse_bypass_governance_retention(req);
                let (xml_entries, quiet) = xml::parse_delete_objects_xml(&req.body)?;
                let requester = Self::requester_from_auth(auth);
                let mut entries: Vec<crate::coordinator::DeleteEntry> = Vec::new();
                let mut validation_errors: Vec<crate::coordinator::DeleteError> = Vec::new();
                for e in &xml_entries {
                    let version_id = e
                        .version_id
                        .as_deref()
                        .map(parse_version_id_str)
                        .transpose()?;
                    let has_unsupported_form_fields =
                        e.last_modified_time.is_some() || e.size.is_some();
                    let has_unsupported_versioned_etag = version_id.is_some() && e.etag.is_some();
                    if has_unsupported_form_fields || has_unsupported_versioned_etag {
                        validation_errors.push(crate::coordinator::DeleteError {
                            key: e.key.clone(),
                            version_id,
                            code: "NotImplemented".to_string(),
                            message:
                                "A form field you provided implies functionality that is not implemented"
                                    .to_string(),
                        });
                        continue;
                    }

                    let cond = match e.etag.as_deref() {
                        Some(etag) => crate::conditional::DeleteCondition::IfMatch(
                            crate::conditional::EtagMatchList::from_header_value(etag),
                        ),
                        None => crate::conditional::DeleteCondition::None,
                    };
                    entries.push(crate::coordinator::DeleteEntry {
                        key: &e.key,
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
                            bucket: BucketRequest::new(&bucket, requester, expected_bucket_owner),
                            entries: &entries,
                            bypass_governance,
                        })?
                };
                result.errors.extend(validation_errors);
                Ok(S3Response::delete_objects(&result, quiet))
            }
            S3Operation::PutBucketVersioning { bucket } => {
                validate_request_checksum_headers(req, true, false)?;
                let versioning_state = xml::parse_versioning_config_xml(&req.body)?;
                let requester = Self::requester_from_auth(auth);
                self.coordinator.put_bucket_versioning(
                    &crate::coordinator::PutBucketVersioningRequest {
                        bucket: crate::coordinator::BucketRequest {
                            name: &bucket,
                            requester,
                            expected_bucket_owner,
                        },
                        state: versioning_state,
                    },
                )?;
                Ok(S3Response::put_bucket_versioning())
            }
            S3Operation::GetBucketVersioning { bucket } => {
                let requester = Self::requester_from_auth(auth);
                let state =
                    self.coordinator
                        .get_bucket_versioning(&crate::coordinator::BucketRequest {
                            name: &bucket,
                            requester,
                            expected_bucket_owner,
                        })?;
                Ok(S3Response::get_bucket_versioning(state))
            }
            S3Operation::PutBucketObjectLockConfiguration { bucket } => {
                require_request_checksum(
                    req,
                    RequestChecksumRequirement::ContentMd5OrChecksumHeader,
                )?;
                let config = xml::parse_bucket_object_lock_configuration_xml(&req.body)?;
                let requester = Self::requester_from_auth(auth);
                self.coordinator.put_bucket_object_lock_configuration(
                    &crate::coordinator::PutBucketObjectLockConfigurationRequest {
                        bucket: crate::coordinator::BucketRequest {
                            name: &bucket,
                            requester,
                            expected_bucket_owner,
                        },
                        config,
                    },
                )?;
                Ok(S3Response::put_bucket_object_lock_configuration())
            }
            S3Operation::GetBucketObjectLockConfiguration { bucket } => {
                let requester = Self::requester_from_auth(auth);
                let config = self.coordinator.get_bucket_object_lock_configuration(
                    &crate::coordinator::BucketRequest {
                        name: &bucket,
                        requester,
                        expected_bucket_owner,
                    },
                )?;
                Ok(S3Response::get_bucket_object_lock_configuration(config))
            }
            S3Operation::PutBucketEncryption { bucket } => {
                validate_request_checksum_headers(req, true, false)?;
                let config = xml::parse_bucket_encryption_xml(&req.body)?;
                let requester = Self::requester_from_auth(auth);
                self.coordinator.put_bucket_encryption(
                    &crate::coordinator::PutBucketEncryptionRequest {
                        bucket: crate::coordinator::BucketRequest {
                            name: &bucket,
                            requester,
                            expected_bucket_owner,
                        },
                        config,
                    },
                )?;
                Ok(S3Response::put_bucket_encryption())
            }
            S3Operation::GetBucketEncryption { bucket } => {
                let requester = Self::requester_from_auth(auth);
                let config =
                    self.coordinator
                        .get_bucket_encryption(&crate::coordinator::BucketRequest {
                            name: &bucket,
                            requester,
                            expected_bucket_owner,
                        })?;
                Ok(S3Response::get_bucket_encryption(config))
            }
            S3Operation::DeleteBucketEncryption { bucket } => {
                let requester = Self::requester_from_auth(auth);
                self.coordinator
                    .delete_bucket_encryption(&crate::coordinator::BucketRequest {
                        name: &bucket,
                        requester,
                        expected_bucket_owner,
                    })?;
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
                let requester = Self::requester_from_auth(auth);
                self.coordinator
                    .put_bucket_cors(&crate::coordinator::PutBucketConfigRequest {
                        bucket: crate::coordinator::BucketRequest {
                            name: &bucket,
                            requester,
                            expected_bucket_owner,
                        },
                        config: &config_xml,
                    })?;
                Ok(S3Response::put_bucket_cors())
            }
            S3Operation::GetBucketCors { bucket } => {
                let requester = Self::requester_from_auth(auth);
                match self
                    .coordinator
                    .get_bucket_cors(&crate::coordinator::BucketRequest {
                        name: &bucket,
                        requester,
                        expected_bucket_owner,
                    })? {
                    Some(config_xml) => Ok(S3Response::get_bucket_cors(&config_xml)),
                    None => Err(ServerError::NoSuchCorsConfiguration {
                        bucket: bucket.clone(),
                    }),
                }
            }
            S3Operation::DeleteBucketCors { bucket } => {
                let requester = Self::requester_from_auth(auth);
                self.coordinator
                    .delete_bucket_cors(&crate::coordinator::BucketRequest {
                        name: &bucket,
                        requester,
                        expected_bucket_owner,
                    })?;
                Ok(S3Response::delete_bucket_cors())
            }
            S3Operation::PutBucketTagging { bucket } => {
                require_request_checksum(
                    req,
                    RequestChecksumRequirement::ContentMd5OrChecksumHeader,
                )?;
                let tags = xml::parse_tagging_xml(&req.body, 50)?;
                let tags_xml = xml::get_tagging_xml(&tags);
                let requester = Self::requester_from_auth(auth);
                self.coordinator
                    .put_bucket_tags(&crate::coordinator::PutBucketConfigRequest {
                        bucket: crate::coordinator::BucketRequest {
                            name: &bucket,
                            requester,
                            expected_bucket_owner,
                        },
                        config: &tags_xml,
                    })?;
                Ok(S3Response::put_bucket_tagging())
            }
            S3Operation::GetBucketTagging { bucket } => {
                let requester = Self::requester_from_auth(auth);
                match self
                    .coordinator
                    .get_bucket_tags(&crate::coordinator::BucketRequest {
                        name: &bucket,
                        requester,
                        expected_bucket_owner,
                    })? {
                    Some(tags_xml) => Ok(S3Response::get_bucket_tagging(&tags_xml)),
                    None => Err(ServerError::NoSuchTagSet {
                        resource: bucket.clone(),
                    }),
                }
            }
            S3Operation::DeleteBucketTagging { bucket } => {
                let requester = Self::requester_from_auth(auth);
                self.coordinator
                    .delete_bucket_tags(&crate::coordinator::BucketRequest {
                        name: &bucket,
                        requester,
                        expected_bucket_owner,
                    })?;
                Ok(S3Response::delete_bucket_tagging())
            }
            S3Operation::PutBucketLifecycle { bucket } => {
                require_request_checksum(req, RequestChecksumRequirement::PutBucketLifecycle)?;
                let config = ensure_lifecycle_rule_ids(
                    xml::parse_bucket_lifecycle_configuration_xml(&req.body)?,
                )?;
                let config_xml = xml::get_bucket_lifecycle_configuration_xml(&config);
                let requester = Self::requester_from_auth(auth);
                self.coordinator.put_bucket_lifecycle(
                    &crate::coordinator::PutBucketConfigRequest {
                        bucket: crate::coordinator::BucketRequest {
                            name: &bucket,
                            requester,
                            expected_bucket_owner,
                        },
                        config: &config_xml,
                    },
                )?;
                Ok(S3Response::put_bucket_lifecycle())
            }
            S3Operation::GetBucketLifecycle { bucket } => {
                let requester = Self::requester_from_auth(auth);
                match self
                    .coordinator
                    .get_bucket_lifecycle(&crate::coordinator::BucketRequest {
                        name: &bucket,
                        requester,
                        expected_bucket_owner,
                    })? {
                    Some(config_xml) => Ok(S3Response::get_bucket_lifecycle(&config_xml)),
                    None => Err(ServerError::NoSuchLifecycleConfiguration {
                        bucket: bucket.clone(),
                    }),
                }
            }
            S3Operation::DeleteBucketLifecycle { bucket } => {
                let requester = Self::requester_from_auth(auth);
                self.coordinator
                    .delete_bucket_lifecycle(&crate::coordinator::BucketRequest {
                        name: &bucket,
                        requester,
                        expected_bucket_owner,
                    })?;
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
                let requester = Self::requester_from_auth(auth);
                self.coordinator.put_object_retention(
                    &crate::coordinator::PutObjectRetentionRequest {
                        object: ObjectVersionRequest::new(
                            &bucket,
                            &key,
                            vid,
                            requester,
                            expected_bucket_owner,
                        ),
                        retention,
                        bypass_governance,
                    },
                )?;
                Ok(S3Response::put_object_retention())
            }
            S3Operation::GetObjectRetention { bucket, key } => {
                let vid = parse_version_id(req)?;
                let requester = Self::requester_from_auth(auth);
                let retention =
                    self.coordinator
                        .get_object_retention(&ObjectVersionRequest::new(
                            &bucket,
                            &key,
                            vid,
                            requester,
                            expected_bucket_owner,
                        ))?;
                Ok(S3Response::get_object_retention(retention))
            }
            S3Operation::PutObjectLegalHold { bucket, key } => {
                require_request_checksum(
                    req,
                    RequestChecksumRequirement::ContentMd5OrChecksumHeader,
                )?;
                let vid = parse_version_id(req)?;
                let legal_hold = xml::parse_object_legal_hold_xml(&req.body)?;
                let requester = Self::requester_from_auth(auth);
                self.coordinator.put_object_legal_hold(
                    &crate::coordinator::PutObjectLegalHoldRequest {
                        object: ObjectVersionRequest::new(
                            &bucket,
                            &key,
                            vid,
                            requester,
                            expected_bucket_owner,
                        ),
                        legal_hold,
                    },
                )?;
                Ok(S3Response::put_object_legal_hold())
            }
            S3Operation::GetObjectLegalHold { bucket, key } => {
                let vid = parse_version_id(req)?;
                let requester = Self::requester_from_auth(auth);
                let legal_hold =
                    self.coordinator
                        .get_object_legal_hold(&ObjectVersionRequest::new(
                            &bucket,
                            &key,
                            vid,
                            requester,
                            expected_bucket_owner,
                        ))?;
                Ok(S3Response::get_object_legal_hold(legal_hold))
            }
            S3Operation::PutObjectTagging { bucket, key } => {
                validate_request_checksum_headers(req, true, false)?;
                let vid = parse_version_id(req)?;
                let tags = xml::parse_tagging_xml(&req.body, 10)?;
                let tags_xml = xml::get_tagging_xml(&tags);
                let requester = Self::requester_from_auth(auth);
                self.coordinator
                    .put_object_tags(&crate::coordinator::PutObjectTagsRequest {
                        object: ObjectVersionRequest::new(
                            &bucket,
                            &key,
                            vid,
                            requester,
                            expected_bucket_owner,
                        ),
                        tags: &tags_xml,
                    })?;
                Ok(S3Response::put_object_tagging())
            }
            S3Operation::GetObjectTagging { bucket, key } => {
                let vid = parse_version_id(req)?;
                let requester = Self::requester_from_auth(auth);
                if let Some(tags_xml) =
                    self.coordinator
                        .get_object_tags(&ObjectVersionRequest::new(
                            &bucket,
                            &key,
                            vid,
                            requester,
                            expected_bucket_owner,
                        ))?
                {
                    Ok(S3Response::get_object_tagging(&tags_xml))
                } else {
                    // S3 returns empty TagSet (not 404) for objects with no tags
                    let empty = xml::get_tagging_xml(&[]);
                    Ok(S3Response::get_object_tagging(&empty))
                }
            }
            S3Operation::DeleteObjectTagging { bucket, key } => {
                let vid = parse_version_id(req)?;
                let requester = Self::requester_from_auth(auth);
                self.coordinator
                    .delete_object_tags(&ObjectVersionRequest::new(
                        &bucket,
                        &key,
                        vid,
                        requester,
                        expected_bucket_owner,
                    ))?;
                Ok(S3Response::delete_object_tagging())
            }
            S3Operation::GetObjectAcl { bucket, key } => {
                let version_id = parse_version_id(req)?;
                let requester = Self::requester_from_auth(auth);
                let result = self.coordinator.get_object_acl(&ObjectVersionRequest::new(
                    &bucket,
                    &key,
                    version_id,
                    requester,
                    expected_bucket_owner,
                ))?;
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
                validate_request_checksum_headers(req, true, false)?;
                let version_id = parse_version_id(req)?;
                let requester = Self::requester_from_auth(auth);
                let result_version_id = if req.header("x-amz-acl").is_some() {
                    if !req.body.is_empty() {
                        return Err(ServerError::InvalidArgument {
                            reason: "x-amz-acl cannot be combined with ACL XML body".to_string(),
                        });
                    }
                    if has_acl_grant_headers(req) {
                        return Err(ServerError::InvalidArgument {
                            reason: "x-amz-acl cannot be combined with x-amz-grant-* headers"
                                .to_string(),
                        });
                    }
                    let acl = parse_put_object_acl(req.header("x-amz-acl"));
                    self.coordinator
                        .put_object_acl(&crate::coordinator::PutObjectAclRequest {
                            object: ObjectVersionRequest::new(
                                &bucket,
                                &key,
                                version_id,
                                requester,
                                expected_bucket_owner,
                            ),
                            acl: crate::coordinator::PutObjectAclInput::Canned(acl),
                        })?
                } else {
                    let acl_grants = parse_acl_grants(req)?;
                    self.coordinator
                        .put_object_acl(&crate::coordinator::PutObjectAclRequest {
                            object: ObjectVersionRequest::new(
                                &bucket,
                                &key,
                                version_id,
                                requester,
                                expected_bucket_owner,
                            ),
                            acl: crate::coordinator::PutObjectAclInput::Grants(acl_grants),
                        })?
                };
                Ok(S3Response::put_object_acl(result_version_id))
            }
            S3Operation::PutBucketPublicAccessBlock { bucket } => {
                validate_request_checksum_headers(req, true, false)?;
                let config = xml::parse_public_access_block_xml(&req.body)?;
                let config_xml = xml::get_public_access_block_xml(&config);
                let requester = Self::requester_from_auth(auth);
                self.coordinator.put_bucket_public_access_block(
                    &crate::coordinator::PutBucketConfigRequest {
                        bucket: crate::coordinator::BucketRequest {
                            name: &bucket,
                            requester,
                            expected_bucket_owner,
                        },
                        config: &config_xml,
                    },
                )?;
                Ok(S3Response::put_bucket_public_access_block())
            }
            S3Operation::GetBucketPublicAccessBlock { bucket } => {
                let requester = Self::requester_from_auth(auth);
                match self.coordinator.get_bucket_public_access_block(
                    &crate::coordinator::BucketRequest {
                        name: &bucket,
                        requester,
                        expected_bucket_owner,
                    },
                )? {
                    Some(config_xml) => Ok(S3Response::get_bucket_public_access_block(&config_xml)),
                    None => Err(ServerError::NoSuchPublicAccessBlockConfiguration {
                        bucket: bucket.clone(),
                    }),
                }
            }
            S3Operation::DeleteBucketPublicAccessBlock { bucket } => {
                let requester = Self::requester_from_auth(auth);
                self.coordinator.delete_bucket_public_access_block(
                    &crate::coordinator::BucketRequest {
                        name: &bucket,
                        requester,
                        expected_bucket_owner,
                    },
                )?;
                Ok(S3Response::delete_bucket_public_access_block())
            }
            S3Operation::PutBucketOwnershipControls { bucket } => {
                validate_request_checksum_headers(req, true, false)?;
                let value = xml::parse_ownership_controls_xml(&req.body)?;
                let config_xml = xml::get_ownership_controls_xml(&value);
                let requester = Self::requester_from_auth(auth);
                self.coordinator.put_bucket_ownership_controls(
                    &crate::coordinator::PutBucketConfigRequest {
                        bucket: crate::coordinator::BucketRequest {
                            name: &bucket,
                            requester,
                            expected_bucket_owner,
                        },
                        config: &config_xml,
                    },
                )?;
                Ok(S3Response::put_bucket_ownership_controls())
            }
            S3Operation::GetBucketOwnershipControls { bucket } => {
                let requester = Self::requester_from_auth(auth);
                match self.coordinator.get_bucket_ownership_controls(
                    &crate::coordinator::BucketRequest {
                        name: &bucket,
                        requester,
                        expected_bucket_owner,
                    },
                )? {
                    Some(config_xml) => Ok(S3Response::get_bucket_ownership_controls(&config_xml)),
                    None => Err(ServerError::OwnershipControlsNotFound {
                        bucket: bucket.clone(),
                    }),
                }
            }
            S3Operation::DeleteBucketOwnershipControls { bucket } => {
                let requester = Self::requester_from_auth(auth);
                self.coordinator.delete_bucket_ownership_controls(
                    &crate::coordinator::BucketRequest {
                        name: &bucket,
                        requester,
                        expected_bucket_owner,
                    },
                )?;
                Ok(S3Response::delete_bucket_ownership_controls())
            }
            S3Operation::PutBucketPolicy { bucket } => {
                validate_request_checksum_headers(req, true, false)?;
                let policy =
                    std::str::from_utf8(&req.body).map_err(|_| ServerError::InvalidArgument {
                        reason: "invalid UTF-8 in bucket policy JSON body".to_string(),
                    })?;
                let requester = Self::requester_from_auth(auth);
                self.coordinator.put_bucket_policy(
                    &crate::coordinator::PutBucketConfigRequest {
                        bucket: crate::coordinator::BucketRequest {
                            name: &bucket,
                            requester,
                            expected_bucket_owner,
                        },
                        config: policy,
                    },
                )?;
                Ok(S3Response::put_bucket_policy())
            }
            S3Operation::GetBucketPolicy { bucket } => {
                let requester = Self::requester_from_auth(auth);
                match self
                    .coordinator
                    .get_bucket_policy(&crate::coordinator::BucketRequest {
                        name: &bucket,
                        requester,
                        expected_bucket_owner,
                    })? {
                    Some(policy) => Ok(S3Response::get_bucket_policy(&policy)),
                    None => Err(ServerError::NoSuchBucketPolicy {
                        bucket: bucket.clone(),
                    }),
                }
            }
            S3Operation::GetBucketPolicyStatus { bucket } => {
                let requester = Self::requester_from_auth(auth);
                let is_public = self.coordinator.get_bucket_policy_status(
                    &crate::coordinator::BucketRequest {
                        name: &bucket,
                        requester,
                        expected_bucket_owner,
                    },
                )?;
                Ok(S3Response::get_bucket_policy_status(is_public))
            }
            S3Operation::DeleteBucketPolicy { bucket } => {
                let requester = Self::requester_from_auth(auth);
                self.coordinator
                    .delete_bucket_policy(&crate::coordinator::BucketRequest {
                        name: &bucket,
                        requester,
                        expected_bucket_owner,
                    })?;
                Ok(S3Response::delete_bucket_policy())
            }
            S3Operation::GetBucketAcl { bucket } => {
                let requester = Self::requester_from_auth(auth);
                let result =
                    self.coordinator
                        .get_bucket_acl(&crate::coordinator::BucketRequest {
                            name: &bucket,
                            requester,
                            expected_bucket_owner,
                        })?;
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
                let requester = Self::requester_from_auth(auth);
                let acl = if req.header("x-amz-acl").is_some() {
                    if !req.body.is_empty() {
                        return Err(ServerError::InvalidArgument {
                            reason: "x-amz-acl cannot be combined with ACL XML body".to_string(),
                        });
                    }
                    if has_acl_grant_headers(req) {
                        return Err(ServerError::InvalidArgument {
                            reason: "x-amz-acl cannot be combined with x-amz-grant-* headers"
                                .to_string(),
                        });
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
                    crate::coordinator::PutBucketAclInput::Grants(parse_acl_grants(req)?)
                };
                let acl_req = crate::coordinator::PutBucketAclRequest {
                    bucket: crate::coordinator::BucketRequest {
                        name: &bucket,
                        requester,
                        expected_bucket_owner,
                    },
                    acl,
                };
                self.coordinator.validate_put_bucket_acl_request(&acl_req)?;
                validate_request_checksum_headers(req, true, false)?;
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
                let (metadata, system_metadata) = parse_request_metadata(req.header_iter())?;

                // Parse optional checksum algorithm/type headers.
                let checksum_algorithm = match req.header("x-amz-checksum-algorithm") {
                    None => None,
                    Some(v) => Some(ChecksumAlgorithm::parse(v).ok_or_else(|| {
                        ServerError::InvalidArgument {
                            reason: format!("unsupported checksum algorithm: {v}"),
                        }
                    })?),
                };
                let checksum_type = match req.header("x-amz-checksum-type") {
                    None => None,
                    Some(v) => Some(ChecksumType::parse(v).ok_or_else(|| {
                        ServerError::InvalidArgument {
                            reason: format!("unsupported checksum type: {v}"),
                        }
                    })?),
                };

                // Validate: checksum-type without checksum-algorithm is invalid.
                if checksum_type.is_some() && checksum_algorithm.is_none() {
                    return Err(ServerError::InvalidArgument {
                        reason: "x-amz-checksum-type requires x-amz-checksum-algorithm".to_string(),
                    });
                }

                // Build validated config (rejects invalid algo+type combinations).
                let checksum = checksum_algorithm
                    .map(|algo| MultipartChecksumConfig::new(algo, checksum_type))
                    .transpose()?;
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
                let requester = Self::requester_from_auth(auth);
                let acl = parse_put_object_write_acl(req)?;
                let object_lock = parse_object_lock_headers(req)?;
                let policy_context = put_object_policy_context_from_request(
                    req,
                    inline_tags_xml.as_deref(),
                    None,
                    None,
                    acl.policy_condition_value(),
                    sse_s3.then_some(ManagedEncryptionAlgorithm::Aes256),
                );

                let result = self.coordinator.create_multipart_upload(
                    &crate::coordinator::CreateMultipartUploadRequest {
                        object: ObjectRequest::new(&bucket, &key, requester, expected_bucket_owner),
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
                        ),
                    },
                )?;
                Ok(S3Response::create_multipart_upload(
                    &bucket,
                    &key,
                    &result.upload_id,
                    crate::http::response::CreateMultipartUploadResponseContext {
                        managed_encryption: result.managed_encryption,
                        checksum_algorithm,
                        checksum_type,
                        lifecycle_abort: result.lifecycle_abort.as_ref(),
                        sse_customer: sse_customer_headers.as_ref(),
                    },
                ))
            }
            S3Operation::UploadPart { bucket, key } => {
                let upload_id = req.query_param_lossy("uploadId").ok_or_else(|| {
                    ServerError::InvalidRequest {
                        reason: "missing uploadId query parameter".to_string(),
                    }
                })?;
                let part_number: u32 = req
                    .query_param_lossy("partNumber")
                    .ok_or_else(|| ServerError::InvalidRequest {
                        reason: "missing partNumber query parameter".to_string(),
                    })?
                    .parse()
                    .map_err(|_| ServerError::InvalidArgument {
                        reason: "partNumber must be a positive integer".to_string(),
                    })?;
                // Normal UploadPart requests are intercepted in serve.rs and
                // streamed before they reach dispatch_routed(). Only copy-source
                // variants should remain on this buffered path.
                let Some(copy_source) = req.header("x-amz-copy-source") else {
                    return Err(ServerError::InternalError {
                        reason: "buffered dispatcher reached non-copy UploadPart".to_string(),
                    });
                };

                let (src_bucket, src_key, src_version_id_str) =
                    request::parse_copy_source(copy_source)?;
                let src_version_id = match src_version_id_str {
                    None => None,
                    Some(v) if v == "null" => Some(VersionId::Null),
                    Some(v) => Some(VersionId::from_u64(v.parse::<u64>().map_err(|_| {
                        ServerError::InvalidArgument {
                            reason: format!("invalid versionId in copy source: {v}"),
                        }
                    })?)),
                };
                let requester = Self::requester_from_auth(auth);
                let source_sse_customer = parse_sse_customer_copy_source_request(req)?;
                let sse_customer = parse_sse_customer_request(req)?;
                reject_managed_encryption_read_headers(req)?;
                let src_cond = copy_source_condition_from_headers(req);
                let copy_source_range =
                    if let Some(range_header) = req.header("x-amz-copy-source-range") {
                        Some(crate::range::parse_copy_source_range(range_header)?)
                    } else {
                        None
                    };
                let result = self.coordinator.upload_part_copy(&UploadPartCopyRequest {
                    source: CopySource {
                        bucket: &src_bucket,
                        key: &src_key,
                        version_id: src_version_id,
                        condition: &src_cond,
                        expected_bucket_owner: expected_source_bucket_owner(req),
                    },
                    upload: MultipartObjectRequest::new(
                        &bucket,
                        &key,
                        &upload_id,
                        requester,
                        expected_bucket_owner,
                    ),
                    part_number,
                    copy_source_range,
                    source_sse_customer: source_sse_customer.as_ref(),
                    sse_customer: sse_customer.as_ref(),
                })?;
                Ok(S3Response::upload_part_copy(
                    &result.etag,
                    result.last_modified,
                    result.managed_encryption,
                    result.sse_customer.as_ref(),
                ))
            }
            S3Operation::CompleteMultipartUpload { bucket, key } => {
                reject_managed_encryption_read_headers(req)?;
                let upload_id = req.query_param_lossy("uploadId").ok_or_else(|| {
                    ServerError::InvalidRequest {
                        reason: "missing uploadId query parameter".to_string(),
                    }
                })?;
                let parts = xml::parse_complete_multipart_upload_xml(&req.body)?;
                let cond = write_condition_from_headers(req)?;
                // Extract object-level checksum claim from request headers as a raw
                // string. CompleteMultipartUpload checksums may be composite ("base64-N"),
                // so we cannot decode them as plain base64.
                let claimed_checksum = extract_encoded_checksum_header(req)?;
                let sse_customer = parse_sse_customer_request(req)?;
                let requester = Self::requester_from_auth(auth);
                let result = self.coordinator.complete_multipart_upload(
                    &crate::coordinator::CompleteMultipartUploadRequest {
                        upload: MultipartObjectRequest::new(
                            &bucket,
                            &key,
                            &upload_id,
                            requester,
                            expected_bucket_owner,
                        ),
                        parts: &parts,
                        claimed_checksum: claimed_checksum.as_ref(),
                        cond: &cond,
                        sse_customer: sse_customer.as_ref(),
                    },
                )?;
                Ok(S3Response::complete_multipart_upload(
                    &bucket, &key, &result,
                ))
            }
            S3Operation::AbortMultipartUpload { bucket, key } => {
                let upload_id = req.query_param_lossy("uploadId").ok_or_else(|| {
                    ServerError::InvalidRequest {
                        reason: "missing uploadId query parameter".to_string(),
                    }
                })?;
                let requester = Self::requester_from_auth(auth);
                self.coordinator
                    .abort_multipart_upload(&MultipartObjectRequest::new(
                        &bucket,
                        &key,
                        &upload_id,
                        requester,
                        expected_bucket_owner,
                    ))?;
                Ok(S3Response::abort_multipart_upload())
            }
            S3Operation::ListMultipartUploads { bucket } => {
                let prefix = req.query_param_lossy("prefix");
                let key_marker = req.query_param_lossy("key-marker");
                let upload_id_marker = req.query_param_lossy("upload-id-marker");
                let max_uploads: u32 = match req.query_param_lossy("max-uploads") {
                    None => 1000,
                    Some(s) => s.parse().map_err(|_| ServerError::InvalidArgument {
                        reason: "invalid max-uploads".to_string(),
                    })?,
                };
                let requester = Self::requester_from_auth(auth);
                let result = self.coordinator.list_multipart_uploads(
                    &crate::coordinator::ListMultipartUploadsRequest {
                        bucket: BucketRequest::new(&bucket, requester, expected_bucket_owner),
                        prefix: prefix.as_deref(),
                        key_marker: key_marker.as_deref(),
                        upload_id_marker: upload_id_marker.as_deref(),
                        max_uploads,
                    },
                )?;
                let rendered = self.render_multipart_uploads(result);
                Ok(S3Response::list_multipart_uploads(
                    &bucket,
                    prefix.as_deref(),
                    key_marker.as_deref(),
                    upload_id_marker.as_deref(),
                    max_uploads,
                    &rendered,
                ))
            }
            S3Operation::ListParts { bucket, key } => {
                let upload_id = req.query_param_lossy("uploadId").ok_or_else(|| {
                    ServerError::InvalidRequest {
                        reason: "missing uploadId query parameter".to_string(),
                    }
                })?;
                let part_number_marker: Option<u32> = req
                    .query_param_lossy("part-number-marker")
                    .map(|s| {
                        s.parse().map_err(|_| ServerError::InvalidArgument {
                            reason: "part-number-marker must be an integer".to_string(),
                        })
                    })
                    .transpose()?;
                let max_parts: u32 = match req.query_param_lossy("max-parts") {
                    None => 1000,
                    Some(s) => s.parse().map_err(|_| ServerError::InvalidArgument {
                        reason: "invalid max-parts".to_string(),
                    })?,
                };
                let requester = Self::requester_from_auth(auth);
                let result =
                    self.coordinator
                        .list_parts(&crate::coordinator::ListPartsRequest {
                            upload: MultipartObjectRequest::new(
                                &bucket,
                                &key,
                                &upload_id,
                                requester,
                                expected_bucket_owner,
                            ),
                            part_number_marker,
                            max_parts,
                        })?;
                Ok(S3Response::list_parts(
                    &bucket,
                    &key,
                    &upload_id,
                    part_number_marker,
                    max_parts,
                    &result,
                ))
            }
            // OptionsRequest is handled before auth in handle_s3_request
            S3Operation::OptionsRequest { .. } => {
                unreachable!("OPTIONS handled before dispatch")
            }
            S3Operation::ListObjectVersions { bucket } => {
                let prefix = req.query_param_lossy("prefix");
                let key_marker = req.query_param_lossy("key-marker");
                let version_id_marker = match req.query_param_lossy("version-id-marker") {
                    None => None,
                    Some(v) if v == "null" => Some(VersionId::Null),
                    Some(v) => Some(VersionId::from_u64(v.parse::<u64>().map_err(|_| {
                        ServerError::InvalidArgument {
                            reason: format!("invalid version-id-marker: {v}"),
                        }
                    })?)),
                };
                let max_keys: u32 = req
                    .query_param_lossy("max-keys")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1000);
                let requester = Self::requester_from_auth(auth);

                let result = self.coordinator.list_object_versions(
                    &crate::coordinator::ListObjectVersionsRequest {
                        bucket: BucketRequest::new(&bucket, requester, expected_bucket_owner),
                        prefix: prefix.as_deref(),
                        key_marker: key_marker.as_deref(),
                        version_id_marker,
                        max_keys,
                    },
                )?;
                Ok(S3Response::list_object_versions(
                    &bucket,
                    prefix.as_deref(),
                    key_marker.as_deref(),
                    max_keys,
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
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Pre-auth time skew check for SigV4 requests: AWS rejects expired requests
        // before signature verification. Only applies to well-formed SigV4 auth headers.
        let is_sigv4 = req
            .header("authorization")
            .is_some_and(|h| h.starts_with("AWS4-HMAC-SHA256"));
        if is_sigv4 {
            if let Some(date_str) = req.header("x-amz-date") {
                if let Some(epoch) = auth::parse_amz_date(date_str) {
                    let skew = now.abs_diff(epoch);
                    if skew > 15 * 60 {
                        return Err(ServerError::Auth(auth::AuthError::RequestExpired));
                    }
                }
                // malformed date → fall through to auth which will handle it
            }
        }

        let auth_result = if defer_region_check {
            authenticate_request_allow_wrong_region(
                req.method.as_str(),
                req.path(),
                req.query_string(),
                &req.header_source(),
                &req.body,
                &self.credentials,
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
                self.coordinator.region(),
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

    fn enforce_bucket_region(&self, bucket: &str, auth: &AuthContext) -> Result<(), ServerError> {
        let Some(signing_region) = auth.signing_region.as_deref() else {
            return Ok(());
        };
        if signing_region == self.coordinator.region() {
            return Ok(());
        }
        if !self.coordinator.bucket_exists(bucket)? {
            return Ok(());
        }
        Err(ServerError::WrongRegion {
            provided_region: signing_region.to_string(),
            expected_region: self.coordinator.region().to_string(),
        })
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
                return Err(ServerError::InvalidArgument {
                    reason: format!("unsupported streaming token: {content_sha}"),
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
                return Err(ServerError::InvalidArgument {
                    reason: format!("unsupported streaming token: {content_sha}"),
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
            Some(
                auth.streaming
                    .as_ref()
                    .ok_or_else(|| ServerError::Auth(auth::AuthError::SignatureMismatch))?,
            )
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
    pub fn prepare_streaming_post_object(
        &self,
        req: &S3Request,
        bucket: &str,
        form_fields: &[(String, String)],
        file_name: Option<&str>,
    ) -> Result<StreamingPostContext, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::prepare_streaming_post_object",
            "bucket={} file_name_present={}",
            bucket,
            file_name.is_some()
        );
        let header_auth = self.authenticate_with_payload_check(req, false, true)?;

        let field = |name: &str| -> Option<&str> {
            form_fields
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.as_str())
        };

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
                algo,
                field("x-amz-credential").ok_or_else(|| ServerError::InvalidRequest {
                    reason: "missing x-amz-credential".to_string(),
                })?,
                field("x-amz-date").ok_or_else(|| ServerError::InvalidRequest {
                    reason: "missing x-amz-date".to_string(),
                })?,
                field("policy").ok_or_else(|| ServerError::InvalidRequest {
                    reason: "missing policy".to_string(),
                })?,
                field("x-amz-signature").ok_or_else(|| ServerError::InvalidRequest {
                    reason: "missing x-amz-signature".to_string(),
                })?,
                &self.credentials,
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
        self.enforce_bucket_region(bucket, effective_auth)?;

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
        let sse_customer = self
            .coordinator
            .prepare_sse_customer_write_context(sse_customer_request.as_ref())?;
        let managed_encryption =
            parse_managed_encryption_form_fields(form_fields, sse_customer.is_some())?;

        let requester = Self::requester_from_auth(effective_auth);
        let acl = parse_put_object_acl(field("acl"));
        let session_id = self.coordinator.begin_stream_put(&BeginStreamPutRequest {
            object: ObjectRequest::new(bucket, &key, requester.clone(), None),
            acl: acl.into(),
            policy: crate::coordinator::PutObjectPolicyContext::new(
                None,
                None,
                acl.policy_condition_value(),
            )
            .with_managed_encryption(managed_encryption)
            .with_sse_customer_algorithm(sse_customer.as_ref().map(|ctx| ctx.request().algorithm()))
            .with_request_object_tags_xml(tags_xml.as_deref()),
            encryption: crate::coordinator::WriteEncryptionRequest::from_request_parts(
                sse_customer.as_ref().map(SseCustomerWriteContext::request),
                managed_encryption,
            ),
            object_lock: ObjectLockState::default(),
        })?;

        let success_status = field("success_action_status")
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(204);
        let success_redirect = field("success_action_redirect")
            .or_else(|| field("redirect"))
            .filter(|s| !s.is_empty())
            .map(std::string::ToString::to_string);

        Ok(StreamingPostContext {
            trace: current_trace_context(),
            binding: StreamObjectBinding {
                session_id,
                bucket: bucket.to_string(),
                key,
            },
            requester,
            acl_header: field("acl").map(std::string::ToString::to_string),
            metadata_blob,
            system_metadata,
            success_status,
            success_redirect,
            form_fields: form_fields.to_vec(),
            policy_b64: field("policy").map(std::string::ToString::to_string),
            checksum_sha256_b64: field("x-amz-checksum-sha256")
                .map(std::string::ToString::to_string),
            tags_xml,
            managed_encryption,
            sse_customer,
        })
    }

    /// Finalize a streaming `PostObject` session and return a POST response.
    pub fn finalize_streaming_post_object(
        &self,
        ctx: &StreamingPostContext,
        crc64: u64,
        total_size: u64,
        actual_sha256_b64: &str,
    ) -> Result<S3Response, ServerError> {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::finalize_streaming_post_object",
            "bucket={} key={} bytes={}",
            ctx.binding.bucket,
            ctx.binding.key,
            total_size
        );
        // Validate policy (if present) with the actual uploaded file size.
        if let Some(policy_b64) = ctx.policy_b64.as_deref() {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let file_size =
                usize::try_from(total_size).map_err(|_| ServerError::ObjectTooLarge {
                    size: total_size,
                    max: crate::coordinator::MAX_OBJECT_SIZE,
                })?;

            let mut field_pairs: Vec<(&str, &str)> = ctx
                .form_fields
                .iter()
                .filter(|(k, _)| !k.eq_ignore_ascii_case("key"))
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            field_pairs.push(("key", &ctx.binding.key));

            auth::validate_post_policy(
                policy_b64,
                &field_pairs,
                file_size,
                &ctx.binding.bucket,
                now,
            )
            .map_err(|e| match &e {
                // Structural/format errors → 400
                auth::PostPolicyError::Malformed(_) => ServerError::InvalidRequest {
                    reason: e.to_string(),
                },
                // content-length-range violations → 400
                auth::PostPolicyError::ConditionFailed("content-length-range") => {
                    ServerError::InvalidRequest {
                        reason: e.to_string(),
                    }
                }
                // Other condition failures and expiration → 403
                auth::PostPolicyError::Expired | auth::PostPolicyError::ConditionFailed(_) => {
                    ServerError::Auth(auth::AuthError::AccessDenied)
                }
            })?;
        }

        // Validate optional x-amz-checksum-sha256 form field.
        if let Some(claimed) = ctx.checksum_sha256_b64.as_deref() {
            if claimed != actual_sha256_b64 {
                return Err(ServerError::InvalidRequest {
                    reason: "checksum mismatch".to_string(),
                });
            }
        }

        let acl = parse_put_object_acl(ctx.acl_header.as_deref());
        let policy_context = crate::coordinator::PutObjectPolicyContext::new(
            None,
            None,
            acl.policy_condition_value(),
        )
        .with_managed_encryption(ctx.managed_encryption)
        .with_sse_customer_algorithm(
            ctx.sse_customer
                .as_ref()
                .map(|ctx| ctx.request().algorithm()),
        )
        .with_request_object_tags_xml(ctx.tags_xml.as_deref());
        let write_encryption = self.coordinator.load_stream_put_write_encryption(
            &ctx.binding.bucket,
            &ctx.binding.key,
            &ctx.binding.session_id,
            ctx.sse_customer
                .as_ref()
                .map(SseCustomerWriteContext::request),
        )?;
        let result = self
            .coordinator
            .finalize_stream_put(&FinalizeStreamPutRequest {
                object: ObjectRequest::new(
                    &ctx.binding.bucket,
                    &ctx.binding.key,
                    ctx.requester.clone(),
                    None,
                ),
                session_id: &ctx.binding.session_id,
                crc64,
                total_size,
                metadata_blob: &ctx.metadata_blob,
                system_metadata: &ctx.system_metadata,
                write_encryption: write_encryption.as_ref(),
                tags: ctx.tags_xml.as_deref(),
                cond: &crate::conditional::WriteCondition::default(),
                acl: acl.into(),
                policy_context,
                requested_object_lock: ObjectLockState::default(),
            })?;

        let mut resp = S3Response::post_object(
            &result,
            &ctx.binding.bucket,
            &ctx.binding.key,
            ctx.success_status,
            ctx.success_redirect.as_deref(),
        );
        apply_sse_customer_write_response_headers(
            &mut resp,
            ctx.sse_customer
                .as_ref()
                .map(SseCustomerWriteContext::request),
        );
        Ok(resp)
    }

    /// Append a segment to a streaming POST session.
    pub fn streaming_append_post_segment(
        &self,
        ctx: &StreamingPostContext,
        segment_index: u32,
        data: &[u8],
    ) -> Result<(), ServerError> {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::streaming_append_post_segment",
            "bucket={} key={} segment_index={} bytes={}",
            ctx.binding.bucket,
            ctx.binding.key,
            segment_index,
            data.len()
        );
        let write_encryption = self.coordinator.load_stream_put_write_encryption(
            &ctx.binding.bucket,
            &ctx.binding.key,
            &ctx.binding.session_id,
            ctx.sse_customer
                .as_ref()
                .map(SseCustomerWriteContext::request),
        )?;
        let data = write_encryption.encrypt_segment(segment_index, data)?;
        self.coordinator.append_stream_segment(
            &ctx.binding.bucket,
            &ctx.binding.key,
            &ctx.binding.session_id,
            segment_index,
            &data,
        )
    }

    /// Abort a streaming POST session (best-effort cleanup).
    pub fn abort_streaming_post_object(&self, ctx: &StreamingPostContext) {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::abort_streaming_post_object",
            "bucket={} key={}",
            ctx.binding.bucket,
            ctx.binding.key
        );
        let _ = self.coordinator.abort_stream_put(
            &ctx.binding.bucket,
            &ctx.binding.key,
            &ctx.binding.session_id,
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
    pub fn prepare_streaming_put(
        &self,
        req: &S3Request,
        bucket: &str,
        key: &str,
        uses_aws_chunked_transport: bool,
    ) -> Result<StreamingPutContext, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::prepare_streaming_put",
            "bucket={} key={}",
            bucket,
            key
        );
        let auth = self.authenticate_with_payload_check(req, false, true)?;
        self.enforce_bucket_region(bucket, &auth)?;
        reject_directory_bucket_only_object_features(req)?;

        let object_lock = parse_object_lock_headers(req)?;
        let checksum_state = validate_request_checksum_headers(req, false, true)?;
        if object_lock.retention.is_some()
            && !checksum_state.has_content_md5
            && !checksum_state.has_checksum_header
            && !checksum_state.has_trailing_checksum
        {
            return Err(ServerError::InvalidRequest {
                reason: "Content-MD5 OR x-amz-checksum- HTTP header is required for Put Object requests with Object Lock parameters".to_string(),
            });
        }
        let content_md5 = ContentMd5Claim::from_request(req)?;
        let sse_customer_request = parse_sse_customer_request(req)?;
        let sse_customer = self
            .coordinator
            .prepare_sse_customer_write_context(sse_customer_request.as_ref())?;
        let managed_encryption = parse_managed_encryption_request(req, sse_customer.is_some())?;

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
            let checksum_value_headers: &[&str] = &[
                "x-amz-checksum-sha256",
                "x-amz-checksum-crc64nvme",
                "x-amz-checksum-crc32",
                "x-amz-checksum-crc32c",
                "x-amz-checksum-sha1",
            ];
            let has_inline_checksum = checksum_value_headers
                .iter()
                .any(|h| req.header(h).is_some());
            if has_inline_checksum {
                return Err(ServerError::InvalidRequest {
                    reason: "Expecting a single x-amz-checksum- header".to_string(),
                });
            }
        }

        let (metadata_blob, mut system_metadata) = parse_request_metadata(req.header_iter())?;
        if uses_aws_chunked_transport {
            system_metadata.strip_aws_chunked_content_encoding();
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
        for &(_, header) in CHECKSUM_HEADERS {
            if let Some(val) = req.header(header) {
                checksum_response.push((header.to_string(), val.to_string()));
            }
        }

        Ok(StreamingPutContext {
            trace: current_trace_context(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            requester: Self::requester_from_auth(&auth),
            expected_bucket_owner: expected_bucket_owner(req).map(str::to_string),
            acl_header: req.header("x-amz-acl").map(str::to_string),
            grant_read_header: req.header("x-amz-grant-read").map(str::to_string),
            grant_write_header: req.header("x-amz-grant-write").map(str::to_string),
            grant_read_acp_header: req.header("x-amz-grant-read-acp").map(str::to_string),
            grant_write_acp_header: req.header("x-amz-grant-write-acp").map(str::to_string),
            grant_full_control_header: req.header("x-amz-grant-full-control").map(str::to_string),
            acl_grants,
            metadata_blob,
            system_metadata,
            cond,
            inline_tags_xml,
            object_lock,
            checksum: StreamingPutChecksumContract {
                content_md5,
                response_headers: ChecksumResponseHeaders(checksum_response),
            },
            managed_encryption,
            sse_customer,
            streaming_signing: auth.streaming,
        })
    }

    /// Start a stream-backed `PutObject` session after the body has exceeded
    /// one internal segment.
    pub fn start_streaming_put_session(
        &self,
        ctx: &StreamingPutContext,
    ) -> Result<String, ServerError> {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::start_streaming_put_session",
            "bucket={} key={}",
            ctx.bucket,
            ctx.key
        );
        self.coordinator.begin_stream_put(&BeginStreamPutRequest {
            object: ObjectRequest::new(
                &ctx.bucket,
                &ctx.key,
                ctx.requester.clone(),
                ctx.expected_bucket_owner.as_deref(),
            ),
            acl: put_object_write_acl_from_components(
                ctx.acl_header.as_deref(),
                ctx.acl_grants.as_ref(),
            ),
            policy: ctx.policy_context(),
            encryption: crate::coordinator::WriteEncryptionRequest::from_request_parts(
                ctx.sse_customer
                    .as_ref()
                    .map(SseCustomerWriteContext::request),
                ctx.managed_encryption,
            ),
            object_lock: ctx.object_lock,
        })
    }

    /// Append a segment to a streaming session.
    pub fn streaming_append_segment(
        &self,
        ctx: &StreamingPutContext,
        session_id: &str,
        segment_index: u32,
        data: &[u8],
    ) -> Result<(), ServerError> {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::streaming_append_segment",
            "bucket={} key={} segment_index={} bytes={}",
            ctx.bucket,
            ctx.key,
            segment_index,
            data.len()
        );
        let write_encryption = self.coordinator.load_stream_put_write_encryption(
            &ctx.bucket,
            &ctx.key,
            session_id,
            ctx.sse_customer
                .as_ref()
                .map(SseCustomerWriteContext::request),
        )?;
        let segment_data = write_encryption.encrypt_segment(segment_index, data)?;
        self.coordinator.append_stream_segment(
            &ctx.bucket,
            &ctx.key,
            session_id,
            segment_index,
            &segment_data,
        )
    }

    /// Commit a single-segment `PutObject` without creating a stream session.
    pub fn put_single_segment_object(
        &self,
        ctx: &StreamingPutContext,
        data: &[u8],
        trailer_checksums: &[(String, String)],
    ) -> Result<S3Response, ServerError> {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::put_single_segment_object",
            "bucket={} key={} bytes={} trailer_checksums={}",
            ctx.bucket,
            ctx.key,
            data.len(),
            trailer_checksums.len()
        );
        let metadata_blob = Self::merged_streaming_put_metadata_blob(ctx, trailer_checksums);
        let system_metadata = Self::merged_streaming_put_system_metadata(ctx, trailer_checksums);
        let result = self
            .coordinator
            .put_object(&crate::coordinator::PutObjectRequest {
                object: ObjectRequest::new(
                    &ctx.bucket,
                    &ctx.key,
                    ctx.requester.clone(),
                    ctx.expected_bucket_owner.as_deref(),
                ),
                data,
                metadata: &metadata_blob,
                system_metadata: &system_metadata,
                tags: ctx.inline_tags_xml.as_deref(),
                cond: &ctx.cond,
                acl: put_object_write_acl_from_components(
                    ctx.acl_header.as_deref(),
                    ctx.acl_grants.as_ref(),
                ),
                policy_context: ctx.policy_context(),
                object_lock: ctx.object_lock,
                encryption: crate::coordinator::WriteEncryptionRequest::from_request_parts(
                    ctx.sse_customer
                        .as_ref()
                        .map(SseCustomerWriteContext::request),
                    ctx.managed_encryption,
                ),
            })?;

        let mut resp = S3Response::put_object(&result);
        apply_sse_customer_write_response_headers(
            &mut resp,
            ctx.sse_customer
                .as_ref()
                .map(SseCustomerWriteContext::request),
        );
        Self::apply_streaming_put_checksum_headers(ctx, &mut resp, trailer_checksums);
        Ok(resp)
    }

    /// Finalize a streaming `PutObject` session and return an `S3Response`.
    ///
    /// `trailer_checksums` contains checksum headers extracted from aws-chunked
    /// trailers (e.g. `x-amz-checksum-crc32`). These are merged into the
    /// metadata blob for storage and echoed back in the response.
    pub fn finalize_streaming_put(
        &self,
        ctx: &StreamingPutContext,
        session_id: &str,
        crc64: u64,
        total_size: u64,
        trailer_checksums: &[(String, String)],
    ) -> Result<S3Response, ServerError> {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::finalize_streaming_put",
            "bucket={} key={} bytes={} trailer_checksums={}",
            ctx.bucket,
            ctx.key,
            total_size,
            trailer_checksums.len()
        );
        let metadata_blob = Self::merged_streaming_put_metadata_blob(ctx, trailer_checksums);
        let system_metadata = Self::merged_streaming_put_system_metadata(ctx, trailer_checksums);
        let write_encryption = self.coordinator.load_stream_put_write_encryption(
            &ctx.bucket,
            &ctx.key,
            session_id,
            ctx.sse_customer
                .as_ref()
                .map(SseCustomerWriteContext::request),
        )?;

        let result = self
            .coordinator
            .finalize_stream_put(&FinalizeStreamPutRequest {
                object: ObjectRequest::new(
                    &ctx.bucket,
                    &ctx.key,
                    ctx.requester.clone(),
                    ctx.expected_bucket_owner.as_deref(),
                ),
                session_id,
                crc64,
                total_size,
                metadata_blob: &metadata_blob,
                system_metadata: &system_metadata,
                write_encryption: write_encryption.as_ref(),
                tags: ctx.inline_tags_xml.as_deref(),
                cond: &ctx.cond,
                acl: put_object_write_acl_from_components(
                    ctx.acl_header.as_deref(),
                    ctx.acl_grants.as_ref(),
                ),
                policy_context: ctx.policy_context(),
                requested_object_lock: ctx.object_lock,
            })?;

        let mut resp = S3Response::put_object(&result);
        apply_sse_customer_write_response_headers(
            &mut resp,
            ctx.sse_customer
                .as_ref()
                .map(SseCustomerWriteContext::request),
        );
        Self::apply_streaming_put_checksum_headers(ctx, &mut resp, trailer_checksums);
        Ok(resp)
    }

    /// Abort a streaming session (best-effort cleanup).
    pub fn abort_streaming_put(&self, ctx: &StreamingPutContext, session_id: &str) {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::abort_streaming_put",
            "bucket={} key={}",
            ctx.bucket,
            ctx.key
        );
        let _ = self
            .coordinator
            .abort_stream_put(&ctx.bucket, &ctx.key, session_id);
    }

    /// Prepare a streaming `UploadPart` session.
    pub fn prepare_streaming_part(
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
            "bucket={} key={} upload_id={} part_number={}",
            bucket,
            key,
            upload_id,
            part_number
        );
        let auth = self.authenticate_with_payload_check(req, false, true)?;
        self.enforce_bucket_region(bucket, &auth)?;

        validate_request_checksum_headers(req, false, false)?;
        let content_md5 = ContentMd5Claim::from_request(req)?;
        let claimed_checksum = extract_checksum_header(req)?;
        let sse_customer_request = parse_sse_customer_request(req)?;
        let requester = Self::requester_from_auth(&auth);
        let expected_bucket_owner = expected_bucket_owner(req).map(str::to_string);

        let mut checksum_response: Vec<(String, String)> = Vec::new();
        for &(_, header) in CHECKSUM_HEADERS {
            if let Some(val) = req.header(header) {
                checksum_response.push((header.to_string(), val.to_string()));
            }
        }

        let begin = self
            .coordinator
            .begin_stream_part(&BeginStreamPartRequest {
                upload: MultipartObjectRequest::new(
                    bucket,
                    key,
                    upload_id,
                    requester.clone(),
                    expected_bucket_owner.as_deref(),
                ),
                part_number,
                policy_context: crate::coordinator::PutObjectPolicyContext::default()
                    .with_sse_customer_algorithm(
                        sse_customer_request
                            .as_ref()
                            .map(SseCustomerRequest::algorithm),
                    ),
                sse_customer: sse_customer_request.as_ref(),
            })?;

        Ok(StreamingPartContext {
            trace: current_trace_context(),
            binding: StreamPartBinding {
                object: StreamObjectBinding {
                    session_id: begin.session_id,
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                },
                upload_id: upload_id.to_string(),
                part_number,
            },
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
    pub fn streaming_append_part_segment(
        &self,
        ctx: &StreamingPartContext,
        segment_index: u32,
        data: &[u8],
    ) -> Result<(), ServerError> {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::streaming_append_part_segment",
            "bucket={} key={} upload_id={} part_number={} segment_index={} bytes={}",
            ctx.binding.object.bucket,
            ctx.binding.object.key,
            ctx.binding.upload_id,
            ctx.binding.part_number,
            segment_index,
            data.len()
        );
        let write_encryption = self.coordinator.load_stream_part_write_encryption(
            &ctx.binding.object.bucket,
            &ctx.binding.object.key,
            &ctx.binding.object.session_id,
            ctx.binding.part_number,
            ctx.sse_customer
                .as_ref()
                .map(SseCustomerWriteContext::request),
        )?;
        let data = write_encryption.encrypt_segment(segment_index, data)?;
        self.coordinator.append_stream_segment(
            &ctx.binding.object.bucket,
            &ctx.binding.object.key,
            &ctx.binding.object.session_id,
            segment_index,
            &data,
        )
    }

    /// Finalize a streaming `UploadPart` session and return an `S3Response`.
    ///
    /// `trailer_checksums` contains checksum headers from aws-chunked trailers.
    /// `computed_checksum` is the incrementally computed checksum (algo, bytes).
    #[allow(clippy::too_many_arguments)]
    pub fn finalize_streaming_part(
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
            "bucket={} key={} upload_id={} part_number={} bytes={} trailer_checksums={}",
            ctx.binding.object.bucket,
            ctx.binding.object.key,
            ctx.binding.upload_id,
            ctx.binding.part_number,
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
            .finalize_stream_part(FinalizeStreamPartRequest {
                upload: MultipartObjectRequest::new(
                    &ctx.binding.object.bucket,
                    &ctx.binding.object.key,
                    &ctx.binding.upload_id,
                    ctx.requester.clone(),
                    ctx.expected_bucket_owner.as_deref(),
                ),
                session_id: &ctx.binding.object.session_id,
                part_number: ctx.binding.part_number,
                crc64,
                total_size,
                claimed_checksum: effective_claim,
                computed_checksum,
            })?;

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
    pub fn abort_streaming_part(&self, ctx: &StreamingPartContext) {
        let _trace = observability::AttachedTrace::new(ctx.trace.clone());
        observability::trace_scope!(
            TRACE_TARGET,
            "HttpFrontend::abort_streaming_part",
            "bucket={} key={} upload_id={} part_number={}",
            ctx.binding.object.bucket,
            ctx.binding.object.key,
            ctx.binding.upload_id,
            ctx.binding.part_number
        );
        let _ = self.coordinator.abort_stream_put(
            &ctx.binding.object.bucket,
            &ctx.binding.object.key,
            &ctx.binding.object.session_id,
        );
    }
}

/// Session binding for a streaming object-scoped upload.
pub struct StreamObjectBinding {
    pub session_id: String,
    pub bucket: String,
    pub key: String,
}

/// Session binding for a streaming multipart-part upload.
pub struct StreamPartBinding {
    pub object: StreamObjectBinding,
    pub upload_id: String,
    pub part_number: u32,
}

/// Checksum response headers to echo back on a streaming response.
pub struct ChecksumResponseHeaders(pub Vec<(String, String)>);

/// Checksum contract for streaming `PutObject`.
pub struct StreamingPutChecksumContract {
    pub content_md5: Option<ContentMd5Claim>,
    pub response_headers: ChecksumResponseHeaders,
}

/// Checksum contract for streaming `UploadPart`.
pub struct StreamingPartChecksumContract {
    pub content_md5: Option<ContentMd5Claim>,
    pub upload_checksum_algorithm: Option<ChecksumAlgorithm>,
    pub claim: Option<ChecksumClaim>,
    pub response_headers: ChecksumResponseHeaders,
}

/// Context for an in-progress streaming `PutObject`.
///
/// Created by `prepare_streaming_put`, used across async/blocking boundaries.
pub struct StreamingPutContext {
    pub trace: observability::TraceContext,
    pub bucket: String,
    pub key: String,
    pub requester: crate::coordinator::Requester,
    pub expected_bucket_owner: Option<String>,
    pub acl_header: Option<String>,
    pub grant_read_header: Option<String>,
    pub grant_write_header: Option<String>,
    pub grant_read_acp_header: Option<String>,
    pub grant_write_acp_header: Option<String>,
    pub grant_full_control_header: Option<String>,
    pub acl_grants: Option<s3_types::AclGrants>,
    pub metadata_blob: crate::metadata_blob::MetadataBlob,
    pub system_metadata: SystemMetadata,
    pub cond: crate::conditional::WriteCondition,
    pub inline_tags_xml: Option<String>,
    pub object_lock: ObjectLockState,
    pub checksum: StreamingPutChecksumContract,
    pub managed_encryption: Option<ManagedEncryptionAlgorithm>,
    pub sse_customer: Option<SseCustomerWriteContext>,
    /// Signing context for aws-chunked modes, None for unsigned/plain.
    pub streaming_signing: Option<auth::StreamingSigningContext>,
}

impl StreamingPutContext {
    fn policy_context(&self) -> crate::coordinator::PutObjectPolicyContext<'_> {
        put_object_policy_context_from_request_fields(
            self.inline_tags_xml.as_deref(),
            None,
            None,
            parse_put_object_acl(self.acl_header.as_deref()).policy_condition_value(),
            self.managed_encryption,
            self.sse_customer
                .as_ref()
                .map(|ctx| ctx.request().algorithm()),
            PutObjectGrantHeaders {
                grant_read: self.grant_read_header.as_deref(),
                grant_write: self.grant_write_header.as_deref(),
                grant_read_acp: self.grant_read_acp_header.as_deref(),
                grant_write_acp: self.grant_write_acp_header.as_deref(),
                grant_full_control: self.grant_full_control_header.as_deref(),
            },
        )
    }
}

/// Context for an in-progress streaming `PostObject`.
pub struct StreamingPostContext {
    pub trace: observability::TraceContext,
    pub binding: StreamObjectBinding,
    pub requester: crate::coordinator::Requester,
    pub acl_header: Option<String>,
    pub metadata_blob: crate::metadata_blob::MetadataBlob,
    pub system_metadata: SystemMetadata,
    pub success_status: u16,
    pub success_redirect: Option<String>,
    pub form_fields: Vec<(String, String)>,
    pub policy_b64: Option<String>,
    pub checksum_sha256_b64: Option<String>,
    pub tags_xml: Option<String>,
    pub managed_encryption: Option<ManagedEncryptionAlgorithm>,
    pub sse_customer: Option<SseCustomerWriteContext>,
}

/// Context for an in-progress streaming `UploadPart`.
///
/// Created by `prepare_streaming_part`, used across async/blocking boundaries.
pub struct StreamingPartContext {
    pub trace: observability::TraceContext,
    pub binding: StreamPartBinding,
    pub requester: crate::coordinator::Requester,
    pub expected_bucket_owner: Option<String>,
    pub checksum: StreamingPartChecksumContract,
    pub sse_customer: Option<SseCustomerWriteContext>,
    /// Signing context for aws-chunked modes, None for unsigned/plain.
    pub streaming_signing: Option<auth::StreamingSigningContext>,
}

/// Convert an `S3Response` into a hyper-compatible HTTP response.
#[must_use]
pub fn s3_response_to_hyper(
    resp: S3Response,
    permit: Option<OwnedSemaphorePermit>,
    stream_read_chunk_size: usize,
    trace_meta: ResponseTraceMeta,
) -> http::Response<S3HyperBody> {
    let body_len = if resp.stream.is_some() {
        resp.headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("Content-Length"))
            .and_then(|(_, value)| value.parse::<u64>().ok())
            .unwrap_or(0)
    } else {
        resp.body.len() as u64
    };
    let trace = ResponseBodyTrace::new(
        trace_meta,
        resp.status_code,
        body_len,
        resp.stream.is_some(),
    );
    let mut builder = http::Response::builder().status(resp.status_code);
    for (name, value) in &resp.headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    let body = match resp.stream {
        Some(stream) => S3HyperBody::streaming(
            stream,
            permit.expect("streaming response requires request permit"),
            stream_read_chunk_size,
            trace,
        ),
        None => S3HyperBody::buffered(resp.body, permit, trace),
    };
    builder
        .body(body)
        .expect("response builder should not fail")
}

fn parse_max_keys<S: AsRef<str>>(raw: Option<S>) -> Result<u32, ServerError> {
    match raw {
        None => Ok(1000),
        Some(s) => s
            .as_ref()
            .parse::<u32>()
            .map_err(|_| ServerError::InvalidArgument {
                reason: "invalid max-keys".to_string(),
            }),
    }
}

/// Apply response-* query parameter overrides to a GET response.
/// Checksum algorithm names and the corresponding header names.
const CHECKSUM_HEADERS: &[(&str, &str)] = &[
    ("SHA256", "x-amz-checksum-sha256"),
    ("CRC64NVME", "x-amz-checksum-crc64nvme"),
    ("CRC32", "x-amz-checksum-crc32"),
    ("CRC32C", "x-amz-checksum-crc32c"),
    ("SHA1", "x-amz-checksum-sha1"),
];

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

    Ok(Some(
        SseCustomerRequest::new(
            customer_key_bytes,
            base64::engine::general_purpose::STANDARD.encode(actual_md5_bytes),
        )
        .with_algorithm(algorithm.to_string()),
    ))
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

    Ok(Some(
        SseCustomerRequest::new(
            customer_key_bytes,
            base64::engine::general_purpose::STANDARD.encode(actual_md5_bytes),
        )
        .with_algorithm(algorithm.to_string()),
    ))
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
        return Err(ServerError::InvalidArgument {
            reason:
                "Requests specifying Server Side Encryption with Customer provided keys must provide a valid encryption algorithm."
                    .to_string(),
        });
    }
    if customer_key.is_none() {
        return Err(ServerError::InvalidArgument {
            reason:
                "Requests specifying Server Side Encryption with Customer provided keys must provide an appropriate secret key."
                    .to_string(),
        });
    }
    if customer_key_md5.is_none() {
        return Err(ServerError::InvalidArgument {
            reason:
                "Requests specifying Server Side Encryption with Customer provided keys must provide the client calculated MD5 of the secret key."
                    .to_string(),
        });
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

fn reject_managed_encryption_read_headers(req: &S3Request) -> Result<(), ServerError> {
    for header in [SSE_HEADER, SSE_KMS_KEY_ID_HEADER] {
        if header_count(req, header) > 1 {
            return Err(ServerError::InvalidRequest {
                reason: format!("duplicate header: {header}"),
            });
        }
    }
    if req.header(SSE_HEADER).is_some() || req.header(SSE_KMS_KEY_ID_HEADER).is_some() {
        return Err(ServerError::InvalidRequest {
            reason: "x-amz-server-side-encryption headers are not valid for this operation"
                .to_string(),
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

fn has_trailing_checksum(req: &S3Request) -> bool {
    req.header("x-amz-trailer").is_some_and(|value| {
        value
            .split(',')
            .any(|name| checksum_algo_from_header(name.trim()).is_some())
    })
}

fn validate_request_checksum_headers(
    req: &S3Request,
    verify_body: bool,
    allow_trailing_checksum: bool,
) -> Result<RequestChecksumState, ServerError> {
    let has_content_md5 = req.header("content-md5").is_some();
    let checksum_header = extract_encoded_checksum_header(req)?;
    let has_checksum_header = checksum_header.is_some();
    let has_trailing_checksum = allow_trailing_checksum && has_trailing_checksum(req);

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
    let state = validate_request_checksum_headers(req, true, false)?;
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
    let lower = header.to_ascii_lowercase();
    for &(algo_name, h) in CHECKSUM_HEADERS {
        if h == lower {
            return ChecksumAlgorithm::parse(algo_name);
        }
    }
    None
}

/// Validate checksum headers on `PutObject`.
///
/// Enforces that at most one checksum header is present, validates base64
/// format/length, and if `x-amz-checksum-algorithm` is set it must match
/// the provided checksum header.
///
/// When `verify_body` is true, also computes the actual checksum from
/// `req.body` and returns `BadDigest` on mismatch. Pass `false` for
/// streaming paths where the body is not yet available.
fn validate_checksum_headers(req: &S3Request, verify_body: bool) -> Result<(), ServerError> {
    use base64::Engine;

    let algo_header = req.header("x-amz-checksum-algorithm");
    let mut found_algo: Option<&str> = None;

    for &(algo, header) in CHECKSUM_HEADERS {
        if let Some(claimed) = req.header(header) {
            // Reject multiple checksum headers
            if found_algo.is_some() {
                return Err(ServerError::InvalidRequest {
                    reason: "only one checksum header may be specified".into(),
                });
            }
            found_algo = Some(algo);

            // If x-amz-checksum-algorithm is set, it must match this header
            if let Some(declared) = algo_header {
                if !declared.eq_ignore_ascii_case(algo) {
                    return Err(ServerError::InvalidRequest {
                        reason: format!(
                            "checksum algorithm mismatch: header says {declared} but got {algo}"
                        ),
                    });
                }
            }

            // Expected byte length for each algorithm.
            let expected_len = match algo {
                "SHA256" => 32,
                "SHA1" => 20,
                "CRC32" | "CRC32C" => 4,
                "CRC64NVME" => 8,
                _ => continue,
            };

            // Validate checksum value format (base64 decodes to correct length).
            let decoded_bytes = base64::engine::general_purpose::STANDARD
                .decode(claimed)
                .ok();
            match decoded_bytes {
                Some(ref bytes) if bytes.len() == expected_len => {}
                _ => {
                    return Err(ServerError::InvalidRequest {
                        reason: format!("Value for {header} header is invalid."),
                    });
                }
            }

            if verify_body {
                let actual_b64 = match algo {
                    "SHA256" => {
                        let digest = ring::digest::digest(&ring::digest::SHA256, &req.body);
                        base64::engine::general_purpose::STANDARD.encode(digest.as_ref())
                    }
                    "SHA1" => {
                        let digest = ring::digest::digest(
                            &ring::digest::SHA1_FOR_LEGACY_USE_ONLY,
                            &req.body,
                        );
                        base64::engine::general_purpose::STANDARD.encode(digest.as_ref())
                    }
                    "CRC32" => {
                        let crc = checksum::crc32::checksum(&req.body);
                        base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes())
                    }
                    "CRC32C" => {
                        let crc = checksum::crc32c::checksum(&req.body);
                        base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes())
                    }
                    "CRC64NVME" => {
                        let crc = checksum::crc64::checksum(&req.body);
                        base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes())
                    }
                    _ => continue,
                };
                if claimed != actual_b64 {
                    return Err(ServerError::BadDigest);
                }
            }
        }
    }
    Ok(())
}

/// Count how many times a header name appears in the request.
fn header_count(req: &S3Request, name: &str) -> usize {
    req.header_count(name)
}

/// Extract a checksum header as an encoded typed claim.
///
/// Shared validation for all checksum-header consumers. Rejects if:
/// - multiple distinct checksum value headers are present (e.g. crc32 + sha256)
/// - the same checksum header appears more than once
/// - `x-amz-checksum-algorithm` contradicts the value header's algorithm
fn extract_encoded_checksum_header(
    req: &S3Request,
) -> Result<Option<EncodedChecksumClaim>, ServerError> {
    if header_count(req, "x-amz-checksum-algorithm") > 1 {
        return Err(ServerError::InvalidRequest {
            reason: "duplicate header: x-amz-checksum-algorithm".into(),
        });
    }
    let algo_header = req.header("x-amz-checksum-algorithm");
    let mut found: Option<EncodedChecksumClaim> = None;
    for &(algo_name, header) in CHECKSUM_HEADERS {
        if let Some(claimed) = req.header(header) {
            if found.is_some() {
                return Err(ServerError::InvalidRequest {
                    reason: "only one checksum header may be specified".into(),
                });
            }
            // Reject duplicate same-name headers (req.header returns only
            // the first, so a second with a different value would be silent).
            if header_count(req, header) > 1 {
                return Err(ServerError::InvalidRequest {
                    reason: format!("duplicate header: {header}"),
                });
            }
            // Cross-check x-amz-checksum-algorithm if present.
            if let Some(declared) = algo_header {
                if !declared.eq_ignore_ascii_case(algo_name) {
                    return Err(ServerError::InvalidRequest {
                        reason: format!(
                            "checksum algorithm mismatch: header says {declared} but got {algo_name}"
                        ),
                    });
                }
            }
            // CHECKSUM_HEADERS uses known-good algo names.
            let algo = ChecksumAlgorithm::parse(algo_name).unwrap();
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
    let overrides: &[(&str, &str)] = &[
        ("response-content-type", "Content-Type"),
        ("response-content-disposition", "Content-Disposition"),
        ("response-content-encoding", "Content-Encoding"),
        ("response-content-language", "Content-Language"),
        ("response-cache-control", "Cache-Control"),
        ("response-expires", "Expires"),
    ];
    for &(param, header_name) in overrides {
        if let Some(value) = req.query_param_lossy(param) {
            resp.headers
                .retain(|(k, _)| !k.eq_ignore_ascii_case(header_name));
            resp.headers
                .push((header_name.to_string(), value.into_owned()));
        }
    }
}

fn add_tagging_count_header(resp: &mut S3Response, tags_xml: &str) -> Result<(), ServerError> {
    let count = xml::count_tags_in_xml(tags_xml).map_err(|err| ServerError::InternalError {
        reason: format!("invalid stored object tags: {err}"),
    })?;
    if count > 0 {
        resp.headers
            .push(("x-amz-tagging-count".to_string(), count.to_string()));
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
    put_object_policy_context_from_request_fields(
        tags_xml,
        copy_source,
        metadata_directive,
        canned_acl,
        managed_encryption,
        req.header(SSE_C_ALGORITHM_HEADER),
        PutObjectGrantHeaders {
            grant_read: req.header("x-amz-grant-read"),
            grant_write: req.header("x-amz-grant-write"),
            grant_read_acp: req.header("x-amz-grant-read-acp"),
            grant_write_acp: req.header("x-amz-grant-write-acp"),
            grant_full_control: req.header("x-amz-grant-full-control"),
        },
    )
}

#[derive(Clone, Copy, Default)]
struct PutObjectGrantHeaders<'a> {
    grant_read: Option<&'a str>,
    grant_write: Option<&'a str>,
    grant_read_acp: Option<&'a str>,
    grant_write_acp: Option<&'a str>,
    grant_full_control: Option<&'a str>,
}

fn put_object_policy_context_from_request_fields<'a>(
    tags_xml: Option<&'a str>,
    copy_source: Option<&'a str>,
    metadata_directive: Option<&'a str>,
    canned_acl: Option<&'a str>,
    managed_encryption: Option<ManagedEncryptionAlgorithm>,
    sse_customer_algorithm: Option<&'a str>,
    grants: PutObjectGrantHeaders<'a>,
) -> crate::coordinator::PutObjectPolicyContext<'a> {
    crate::coordinator::PutObjectPolicyContext::new(copy_source, metadata_directive, canned_acl)
        .with_managed_encryption(managed_encryption)
        .with_sse_customer_algorithm(sse_customer_algorithm)
        .with_request_object_tags_xml(tags_xml)
        .with_acl_grant_headers(
            grants.grant_read,
            grants.grant_write,
            grants.grant_read_acp,
            grants.grant_write_acp,
            grants.grant_full_control,
        )
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
    use crate::coordinator::Coordinator;
    use ec::EcConfig;
    use server_core::sse::{ManagedWrappingKeyConfig, StaticManagedKeyProvider};
    use std::sync::Arc;
    use storage::SharedStorageNode;

    const TEST_SSE_S3_WRAPPING_KEY_B64: &str = "YWJjZGVmMDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODk=";

    fn setup_frontend(dir: &std::path::Path) -> HttpFrontend {
        setup_frontend_with_sse_s3(dir)
    }

    fn setup_frontend_with_sse_s3(dir: &std::path::Path) -> HttpFrontend {
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(dir, &pg_ids).unwrap());
        let ec_config = EcConfig::new(4, 2).unwrap();
        let sse_s3_provider = StaticManagedKeyProvider::single(
            ManagedWrappingKeyConfig::from_base64(1, TEST_SSE_S3_WRAPPING_KEY_B64).unwrap(),
        );
        let coordinator = Coordinator::new_with_managed_key_provider(
            storage_node,
            ec_config,
            "us-east-1".to_string(),
            None,
            sse_s3_provider,
        )
        .unwrap();
        let credentials = auth::CredentialStore::new();
        HttpFrontend {
            coordinator,
            credentials,
        }
    }

    fn test_auth() -> auth::AuthContext {
        auth::AuthContext {
            mode: auth::AuthMode::HeaderSigV4,
            access_key_id: Some("AKID".to_string()),
            account: Some(auth::AccountIdentity::from_principal("testuser")),
            request_epoch_secs: Some(0),
            signing_region: Some("us-east-1".to_string()),
            streaming: None,
        }
    }

    fn create_test_bucket(coord: &Coordinator, name: &str) {
        coord
            .create_bucket(&crate::coordinator::CreateBucketRequest {
                name,
                requester: crate::coordinator::test_helpers::requester("testuser"),
                acl: crate::coordinator::CreateBucketAcl::DefaultPrivate,
                ownership: crate::coordinator::BucketObjectOwnership::ObjectWriter,
                object_lock_enabled: false,
            })
            .unwrap();
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
            request_epoch_secs: Some(0),
            signing_region: Some("us-west-2".to_string()),
            streaming: None,
        };

        match fe.enforce_bucket_region_for_operation(
            &S3Operation::PutObject {
                bucket: "mybucket".to_string(),
                key: "key".to_string(),
            },
            &auth,
        ) {
            Err(ServerError::WrongRegion {
                provided_region,
                expected_region,
            }) => {
                assert_eq!(provided_region, "us-west-2");
                assert_eq!(expected_region, "us-east-1");
            }
            other => panic!("expected WrongRegion, got {other:?}"),
        }
    }

    #[test]
    fn bucket_region_mismatch_is_ignored_for_missing_bucket() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        let auth = auth::AuthContext {
            mode: auth::AuthMode::HeaderSigV4,
            access_key_id: Some("AKID".to_string()),
            account: Some(auth::AccountIdentity::from_principal("testuser")),
            request_epoch_secs: Some(0),
            signing_region: Some("us-west-2".to_string()),
            streaming: None,
        };

        fe.enforce_bucket_region_for_operation(
            &S3Operation::PutObject {
                bucket: "missing".to_string(),
                key: "key".to_string(),
            },
            &auth,
        )
        .unwrap();
    }

    #[test]
    fn create_bucket_defers_region_check() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());

        assert!(fe.should_defer_region_check(&S3Operation::CreateBucket {
            bucket: "mybucket".to_string(),
        }));
    }

    fn test_bucket_request(name: &str) -> crate::coordinator::BucketRequest<'_> {
        crate::coordinator::BucketRequest::new(
            name,
            crate::coordinator::test_helpers::requester("testuser"),
            None,
        )
    }

    fn test_object_request<'a>(
        bucket: &'a str,
        key: &'a str,
    ) -> crate::coordinator::ObjectRequest<'a> {
        crate::coordinator::ObjectRequest::new(
            bucket,
            key,
            crate::coordinator::test_helpers::requester("testuser"),
            None,
        )
    }

    #[test]
    fn list_buckets_uses_account_display_name_and_explicit_canonical_id() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        let owner_canonical_id = s3_types::CanonicalUserId::from_principal("custom-account-id");
        let account = auth::AccountIdentity::new("testuser", owner_canonical_id.clone(), "User A");

        fe.coordinator
            .create_bucket(&crate::coordinator::CreateBucketRequest {
                name: "mybucket",
                requester: crate::coordinator::Requester::authenticated(account.clone()),
                acl: crate::coordinator::CreateBucketAcl::DefaultPrivate,
                ownership: crate::coordinator::BucketObjectOwnership::ObjectWriter,
                object_lock_enabled: false,
            })
            .unwrap();

        let bucket = fe
            .coordinator
            .head_bucket(&crate::coordinator::BucketRequest {
                name: "mybucket",
                requester: crate::coordinator::Requester::authenticated(account.clone()),
                expected_bucket_owner: None,
            })
            .unwrap();
        assert_eq!(bucket.owner_canonical_id, owner_canonical_id);

        let auth = auth::AuthContext {
            mode: auth::AuthMode::HeaderSigV4,
            access_key_id: Some("AKID".to_string()),
            account: Some(account),
            request_epoch_secs: Some(0),
            signing_region: Some("us-east-1".to_string()),
            streaming: None,
        };
        let resp = fe
            .dispatch_routed(&make_req(""), &auth, S3Operation::ListBuckets)
            .unwrap();
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains(owner_canonical_id.as_str()));
        assert!(body.contains("<DisplayName>User A</DisplayName>"));
        assert!(!body.contains("<DisplayName>testuser</DisplayName>"));
    }

    #[test]
    fn get_bucket_location_dispatches_through_head_bucket_checks() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let req = new_req(http::Method::GET, "/mybucket", "location", vec![], vec![]);
        let resp = fe
            .dispatch_routed(
                &req,
                &test_auth(),
                S3Operation::GetBucketLocation {
                    bucket: "mybucket".to_string(),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains("<LocationConstraint"));
        assert!(!body.contains(">us-east-1<"));
    }

    fn make_req(query: &str) -> S3Request {
        S3Request::new_for_test(http::Method::GET, "/", query, test_headers(vec![]), vec![])
    }

    fn new_req(
        method: http::Method,
        path: &str,
        query: &str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> S3Request {
        S3Request::new_for_test(method, path, query, test_headers(headers), body)
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
                    bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
                key: "mykey".to_string(),
            },
        ) {
            Err(ServerError::InvalidRequest { reason })
                if reason
                    == "x-amz-server-side-encryption headers are not valid for this operation" => {}
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
                bucket: "mybucket".to_string(),
            },
        )
        .unwrap();

        let get_req = new_req(http::Method::GET, "/", "acl", vec![], vec![]);
        let resp = fe
            .dispatch_routed(
                &get_req,
                &test_auth(),
                S3Operation::GetBucketAcl {
                    bucket: "mybucket".to_string(),
                },
            )
            .unwrap();
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains(canonical_id.as_str()));
    }

    #[test]
    fn bucket_policy_put_get_delete_round_trip() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

        let policy = "{\"Version\":\"2012-10-17\",\"Statement\":[]}";
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
                    bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
                },
            )
            .unwrap();
        assert_eq!(get_resp.status_code, 200);
        assert_eq!(String::from_utf8(get_resp.body).unwrap(), policy);
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
                    bucket: "mybucket".to_string(),
                },
            )
            .unwrap();
        assert_eq!(delete_resp.status_code, 204);

        match fe.dispatch_routed(
            &get_req,
            &test_auth(),
            S3Operation::GetBucketPolicy {
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
            },
        ) {
            Err(ServerError::MalformedPolicy { reason }) => {
                assert!(reason.contains("invalid JSON"));
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
            .put_bucket_policy(&crate::coordinator::PutBucketConfigRequest {
                bucket: test_bucket_request("mybucket"),
                config: r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::mybucket"}]}"#,
            })
            .unwrap();

        let get_req = new_req(http::Method::GET, "/", "policyStatus", vec![], vec![]);
        let get_resp = fe
            .dispatch_routed(
                &get_req,
                &test_auth(),
                S3Operation::GetBucketPolicyStatus {
                    bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
                },
            )
            .unwrap();
        assert_eq!(get_resp.status_code, 200);
        assert_eq!(
            find_header(&get_resp, "Content-Type"),
            Some("application/xml")
        );
        assert_eq!(String::from_utf8(get_resp.body).unwrap(), expected);

        let delete_req = new_req(http::Method::DELETE, "/", "lifecycle", vec![], vec![]);
        let delete_resp = fe
            .dispatch_routed(
                &delete_req,
                &test_auth(),
                S3Operation::DeleteBucketLifecycle {
                    bucket: "mybucket".to_string(),
                },
            )
            .unwrap();
        assert_eq!(delete_resp.status_code, 204);

        match fe.dispatch_routed(
            &get_req,
            &test_auth(),
            S3Operation::GetBucketLifecycle {
                bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
            },
        )
        .unwrap();

        let create_req = new_req(http::Method::POST, "/", "uploads", vec![], vec![]);
        let create_resp = fe
            .dispatch_routed(
                &create_req,
                &test_auth(),
                S3Operation::CreateMultipartUpload {
                    bucket: "mybucket".to_string(),
                    key: "uploads/archive.bin".to_string(),
                },
            )
            .unwrap();
        assert!(find_header(&create_resp, "x-amz-abort-date").is_some());
        assert_eq!(
            find_header(&create_resp, "x-amz-abort-rule-id"),
            Some("abort-stale")
        );

        let body = std::str::from_utf8(&create_resp.body).unwrap();
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
                    bucket: "mybucket".to_string(),
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
                name: "mybucket",
                requester: crate::coordinator::test_helpers::requester("testuser"),
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
                bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
                    key: "mykey".to_string(),
                },
            )
            .unwrap();
        let body = String::from_utf8(resp.body).unwrap();
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
                bucket: "mybucket".to_string(),
            },
        )
        .unwrap();

        let get_req = new_req(http::Method::GET, "/", "acl", vec![], vec![]);
        let resp = fe
            .dispatch_routed(
                &get_req,
                &test_auth(),
                S3Operation::GetBucketAcl {
                    bucket: "mybucket".to_string(),
                },
            )
            .unwrap();
        let body = String::from_utf8(resp.body).unwrap();
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
                    bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
                },
            )
            .unwrap();
        assert_eq!(get_resp.status_code, 200);
        let body = String::from_utf8(get_resp.body).unwrap();
        assert!(body.contains("<ObjectLockEnabled>Enabled</ObjectLockEnabled>"));
        assert!(body.contains("<Days>1</Days>"));
    }

    #[test]
    fn put_object_acl_rejects_write_header_grant() {
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
        match fe.dispatch_routed(
            &put_req,
            &test_auth(),
            S3Operation::PutObjectAcl {
                bucket: "mybucket".to_string(),
                key: "mykey".to_string(),
            },
        ) {
            Err(ServerError::InvalidArgument { reason }) => {
                assert!(reason.contains("WRITE grants"));
            }
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected InvalidArgument, got Ok"),
        }
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
            ("x-amz-checksum-algorithm".to_string(), "CRC32".to_string()),
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
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
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
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
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
            bucket: "mybucket".to_string(),
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
            bucket: "mybucket".to_string(),
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
            bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
            },
            "Missing required header for this request: Content-MD5 OR x-amz-checksum-*",
        );
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
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
                    key: "mykey".to_string(),
                },
            )
            .unwrap();
        assert_eq!(resp.status_code, 200);
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
                    bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
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
                    bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
                bucket: "mybucket".to_string(),
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
            vec![("x-amz-checksum-algorithm".to_string(), "CRC32".to_string())],
            br#"<LifecycleConfiguration><Rule><ID>rule1</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>"#.to_vec(),
        );
        match fe.dispatch_routed(
            &req,
            &test_auth(),
            S3Operation::PutBucketLifecycle {
                bucket: "mybucket".to_string(),
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
    fn prepare_streaming_put_with_object_lock_accepts_sdk_checksum_header() {
        let tmp = test_util::tempdir();
        let fe = setup_frontend(tmp.path());
        create_test_bucket(&fe.coordinator, "mybucket");

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
        let req = new_req(http::Method::PUT, "", "", headers, body);
        let ctx = fe
            .prepare_streaming_put(&req, "mybucket", "mykey", false)
            .unwrap();
        assert_eq!(ctx.key, "mykey");
        assert!(ctx.object_lock.retention.is_some());
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
            bucket: "mybucket".to_string(),
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
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
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
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn complete_multipart_algorithm_header_contradicts_value_header() {
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
                // Algorithm header says SHA256 but value header is CRC32.
                ("x-amz-checksum-algorithm".to_string(), "SHA256".to_string()),
                ("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string()),
            ],
            xml.into_bytes(),
        );
        let op = S3Operation::CompleteMultipartUpload {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
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
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn complete_multipart_duplicate_checksum_algorithm_header_rejected() {
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
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
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
            bucket: "mybucket".to_string(),
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
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRequest { .. }) => {}
            Err(e) => panic!("expected InvalidRequest, got {e:?}"),
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
            bucket: "mybucket".to_string(),
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

        let req = make_req("uploadId=abc&part-number-marker=xyz");
        let op = S3Operation::ListParts {
            bucket: "mybucket".to_string(),
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

        let req = make_req("uploadId=abc&max-parts=notanumber");
        let op = S3Operation::ListParts {
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
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
            bucket: "mybucket".to_string(),
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
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
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
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body = std::str::from_utf8(&resp.body).unwrap();
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
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body = std::str::from_utf8(&resp.body).unwrap();
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
            bucket: "mybucket".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
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
            .put_bucket_ownership_controls(
                &crate::coordinator::PutBucketConfigRequest {
                    bucket: test_bucket_request("mybucket"),
                    config: "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
                },
            )
            .unwrap();

        let req = new_req(
            http::Method::GET,
            "",
            "uploads",
            vec![("x-amz-acl".to_string(), "public-read".to_string())],
            vec![],
        );
        let op = S3Operation::CreateMultipartUpload {
            bucket: "mybucket".to_string(),
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
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
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
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
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
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
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
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn create_multipart_with_checksum_returns_fields_in_xml() {
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
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body = std::str::from_utf8(&resp.body).unwrap();
        assert!(
            body.contains("<ChecksumAlgorithm>CRC32</ChecksumAlgorithm>"),
            "missing ChecksumAlgorithm: {body}"
        );
        assert!(
            body.contains("<ChecksumType>FULL_OBJECT</ChecksumType>"),
            "missing ChecksumType: {body}"
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
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body = std::str::from_utf8(&resp.body).unwrap();
        assert!(
            body.contains("<ChecksumAlgorithm>CRC32</ChecksumAlgorithm>"),
            "missing ChecksumAlgorithm: {body}"
        );
        assert!(
            body.contains("<ChecksumType>COMPOSITE</ChecksumType>"),
            "missing ChecksumType: {body}"
        );
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
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        assert_eq!(resp.status_code, 200);
        let body = std::str::from_utf8(&resp.body).unwrap();
        assert!(
            body.contains("<ChecksumAlgorithm>SHA256</ChecksumAlgorithm>"),
            "missing ChecksumAlgorithm: {body}"
        );
        // When no type specified, no ChecksumType element emitted.
        assert!(
            !body.contains("ChecksumType"),
            "unexpected ChecksumType: {body}"
        );
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
            bucket: bucket.to_string(),
            key: key.to_string(),
        };
        let resp = fe.dispatch_routed(&req, &test_auth(), op).unwrap();
        let body = std::str::from_utf8(&resp.body).unwrap();
        let start = body.find("<UploadId>").unwrap() + "<UploadId>".len();
        let end = start + body[start..].find("</UploadId>").unwrap();
        body[start..end].to_string()
    }

    fn compute_checksum_for_test(algo: ChecksumAlgorithm, data: &[u8]) -> RawChecksum {
        match algo {
            ChecksumAlgorithm::Crc32 => {
                RawChecksum::new(algo, checksum::crc32::checksum(data).to_be_bytes())
            }
            ChecksumAlgorithm::Crc32c => {
                RawChecksum::new(algo, checksum::crc32c::checksum(data).to_be_bytes())
            }
            ChecksumAlgorithm::Crc64nvme => {
                RawChecksum::new(algo, checksum::crc64::checksum(data).to_be_bytes())
            }
            ChecksumAlgorithm::Sha256 => RawChecksum::new(
                algo,
                ring::digest::digest(&ring::digest::SHA256, data).as_ref(),
            ),
            ChecksumAlgorithm::Sha1 => RawChecksum::new(
                algo,
                ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, data).as_ref(),
            ),
        }
        .expect("checksum helper produces bytes matching the requested algorithm")
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
        let requester = HttpFrontend::requester_from_auth(&test_auth());
        let session = fe
            .coordinator
            .begin_stream_part(&BeginStreamPartRequest {
                upload: MultipartObjectRequest::new(
                    bucket,
                    key,
                    upload_id,
                    requester.clone(),
                    None,
                ),
                part_number,
                policy_context: crate::coordinator::PutObjectPolicyContext::default(),
                sse_customer: None,
            })
            .unwrap();
        let result = (|| {
            use base64::Engine;

            let write_encryption = fe.coordinator.load_stream_part_write_encryption(
                bucket,
                key,
                &session.session_id,
                part_number,
                None,
            )?;
            let mut staged = Vec::new();

            for (segment_index, chunk) in data
                .chunks(crate::coordinator::INTERNAL_SEGMENT_SIZE)
                .enumerate()
            {
                let chunk = write_encryption.encrypt_segment(segment_index as u32, chunk)?;
                staged.extend_from_slice(&chunk);
                fe.coordinator.append_stream_segment(
                    bucket,
                    key,
                    &session.session_id,
                    segment_index as u32,
                    &chunk,
                )?;
            }
            let computed_checksum =
                checksum_algorithm.map(|algo| compute_checksum_for_test(algo, data));
            let claimed_checksum = computed_checksum.as_ref().map(|expected| {
                let encoded = base64::engine::general_purpose::STANDARD.encode(expected.bytes());
                ChecksumClaim::from_base64(expected.algorithm(), &encoded)
                    .expect("checksum helper must round-trip through base64")
            });
            fe.coordinator
                .finalize_stream_part(FinalizeStreamPartRequest {
                    upload: MultipartObjectRequest::new(bucket, key, upload_id, requester, None),
                    session_id: &session.session_id,
                    part_number,
                    crc64: checksum::crc64::checksum(&staged),
                    total_size: data.len() as u64,
                    claimed_checksum: claimed_checksum.as_ref(),
                    computed_checksum,
                })
        })();
        if result.is_err() {
            let _ = fe
                .coordinator
                .abort_stream_put(bucket, key, &session.session_id);
        }
        result.unwrap()
    }

    // ── GET ?partNumber=N tests ─────────────────────────────────────

    /// Helper: do a full multipart upload through the live streaming coordinator path.
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
            bucket: bucket.to_string(),
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
            bucket: "mybucket".to_string(),
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
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        fe.dispatch_routed(&req, &test_auth(), op).unwrap();

        // partNumber=0 → InvalidArgument
        let req = make_req("partNumber=0");
        let op = S3Operation::GetObject {
            bucket: "mybucket".to_string(),
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

        // partNumber=99 on a 3-part object → 416 InvalidRange
        let req = make_req("partNumber=99");
        let op = S3Operation::GetObject {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRange { .. }) => {}
            Err(e) => panic!("expected InvalidRange, got {e:?}"),
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
            bucket: "mybucket".to_string(),
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
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        fe.dispatch_routed(&req, &test_auth(), op).unwrap();

        // partNumber=1 on inline object → 206 with full data
        let req = make_req("partNumber=1");
        let op = S3Operation::GetObject {
            bucket: "mybucket".to_string(),
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
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        fe.dispatch_routed(&req, &test_auth(), op).unwrap();

        // partNumber=2 on non-multipart → 416 InvalidRange
        let req = make_req("partNumber=2");
        let op = S3Operation::GetObject {
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidRange { .. }) => {}
            Err(e) => panic!("expected InvalidRange, got {e:?}"),
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
            bucket: "mybucket".to_string(),
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
            Err(ServerError::Auth(auth::AuthError::SignatureMismatch)) => {} // expected
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
            Err(ServerError::InvalidArgument { .. }) => {} // expected
            other => panic!("expected InvalidArgument, got {:?}", other.err()),
        }
    }

    // ── DeleteObjects version-id validation ────────────────────────────

    #[test]
    fn delete_objects_invalid_version_id_rejected() {
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
            bucket: "mybucket".to_string(),
        };
        match fe.dispatch_routed(&req, &test_auth(), op) {
            Err(ServerError::InvalidArgument { .. }) => {}
            Err(e) => panic!("expected InvalidArgument, got {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
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
            bucket: "mybucket".to_string(),
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
            bucket: "mybucket".to_string(),
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
            bucket: "mybucket".to_string(),
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
            bucket: "mybucket".to_string(),
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
            bucket: "mybucket".to_string(),
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
            bucket: "mybucket".to_string(),
            key: "k".to_string(),
        };
        match fe.dispatch_routed(&req2, &test_auth(), op2) {
            Err(ServerError::PreconditionFailed) => {}
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
