// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

/// Async hyper HTTP server loop with frontend pool and backpressure.
use std::convert::Infallible;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use checksum::{ChecksumAlgorithm, RawChecksum};
use http_body_util::{BodyExt, LengthLimitError, Limited};
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::time::Instant as TokioInstant;
use tokio_rustls::TlsAcceptor;

#[cfg(any(test, feature = "local-debug-endpoints"))]
use super::request::percent_decode_strict;
use super::request::{
    parse_upload_part_query_raw, S3Request, TlsProtocolVersion, TransportSecurity,
    MAX_BUFFERED_CONTROL_BODY_SIZE,
};
use super::response::{S3Response, WireResponseIds};
use super::router::{
    route, route_service, EndpointKind, S3ControlOperation, S3Operation, ServiceOperation,
    ServiceRouteError,
};
use super::s3_response_to_hyper;
use super::{HttpFrontend, HttpRequestAdmission, S3HyperBody};
use crate::coordinator::MAX_OBJECT_SIZE;
use crate::error::ServerError;
use server_core::metadata_blob::USER_METADATA_SIZE_LIMIT;
#[cfg(test)]
use storage::test_support::{
    StorageClusterFailureTestSupport as _, StorageClusterLifecycleTestSupport as _,
    StorageClusterPayloadTestSupport as _, StorageClusterRouteHandleTestSupport as _,
    StorageClusterRouteMapTestSupport as _, StorageClusterSchedulingTestSupport as _,
};
use storage::{BucketName, SessionId};
#[cfg(any(test, feature = "local-debug-endpoints"))]
use storage::{
    MetadataCheckpointDiagnosticOutcome, ObjectKey, ObjectPayloadPlacementDiagnosticOutcome,
};

const TRACE_TARGET: &str = "server_http";
const MAX_STREAMING_POST_PART_HEADER_BYTES: usize = 8 * 1024;
const MAX_STREAMING_POST_NON_FILE_FORM_BYTES: usize = MAX_BUFFERED_CONTROL_BODY_SIZE;
const REQUEST_ADMISSION_WAIT_EVENT_THRESHOLD_US: u128 = 10_000;
const CHUNKED_DECODER_FEED_BYTES: usize = 64 * 1024;
#[cfg(not(test))]
const STREAMING_PUT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
#[cfg(test)]
const STREAMING_PUT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(10);
const MAX_STREAMING_POST_DEFAULT_FIELD_BYTES: usize = 8 * 1024;
const MAX_STREAMING_POST_KEY_FIELD_BYTES: usize = 2 * 1024;
const MAX_STREAMING_POST_POLICY_FIELD_BYTES: usize = 256 * 1024;
const MAX_STREAMING_POST_TAGGING_FIELD_BYTES: usize = 16 * 1024;
const MAX_STREAMING_POST_SIGNATURE_FIELD_BYTES: usize = 256;
const MAX_STREAMING_POST_DATE_FIELD_BYTES: usize = 64;
const MAX_STREAMING_POST_CREDENTIAL_FIELD_BYTES: usize = 2 * 1024;
const MAX_STREAMING_POST_ALGORITHM_FIELD_BYTES: usize = 64;
const MAX_STREAMING_POST_ACL_FIELD_BYTES: usize = 128;
const MAX_STREAMING_POST_STATUS_FIELD_BYTES: usize = 16;
const MAX_STREAMING_POST_CHECKSUM_FIELD_BYTES: usize = 128;
const MAX_STREAMING_POST_SSE_FIELD_BYTES: usize = 4 * 1024;
const MAX_STREAMING_REJECT_DRAIN_BYTES: usize =
    crate::coordinator::INTERNAL_SEGMENT_SIZE + (64 * 1024);
const MAX_STREAMING_REJECT_DRAIN_DURATION: Duration = Duration::from_secs(2);
/// Lingering close: how long to wait, per read, for a client to go quiet
/// after its connection is done, before closing the socket.
const LINGERING_CLOSE_QUIET: Duration = Duration::from_millis(500);
/// Lingering close: total bound on post-connection draining.
const LINGERING_CLOSE_MAX: Duration = Duration::from_secs(5);
const MAX_ACL_XML_BYTES: usize = 200 * 1024;
const MAX_DELETE_OBJECTS_XML_BYTES: usize = 2_048_000;
const MAX_VERSIONING_CONFIGURATION_BYTES: usize = 1024;
const MAX_OBJECT_LOCK_CONFIGURATION_BYTES: usize = 2 * 1024 * 1024;
const MAX_BUCKET_ENCRYPTION_CONFIGURATION_BYTES: usize = 2 * 1024 * 1024;
const MAX_LIFECYCLE_CONFIGURATION_BYTES: usize = 2 * 1024 * 1024;
const MAX_CORS_CONFIGURATION_BYTES: usize = 64 * 1024;
const MAX_TAGGING_XML_BYTES: usize = 160 * 1024;
const MAX_PUBLIC_ACCESS_BLOCK_CONFIGURATION_BYTES: usize = 2 * 1024 * 1024;
const MAX_OWNERSHIP_CONTROLS_XML_BYTES: usize = 2048;
const MAX_BUCKET_ABAC_XML_BYTES: usize = 1024;
const MAX_COMPLETE_MULTIPART_UPLOAD_XML_BYTES: usize = 2_621_440;

/// Incremental hasher for validating trailing checksums in streaming uploads.
///
/// Created when `x-amz-trailer` declares a checksum header. Fed with decoded
/// payload during streaming, then finalized to a base64 string for comparison
/// with the trailer value.
struct TrailingChecksumHasher(checksum::ChecksumHasher);

impl TrailingChecksumHasher {
    fn from_algorithm(algorithm: ChecksumAlgorithm) -> Self {
        Self(checksum::ChecksumHasher::new(algorithm))
    }

    /// Create a hasher from a trailer header name (e.g. `x-amz-checksum-crc32`).
    ///
    /// Matches case-insensitively since HTTP header names are case-insensitive.
    fn from_trailer_header(header: &str) -> Option<Self> {
        ChecksumAlgorithm::from_header_name(header).map(Self::from_algorithm)
    }

    fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    /// Finalize and return a validated `RawChecksum`.
    fn finalize_raw(self) -> RawChecksum {
        self.0.finalize()
    }

    /// Finalize and return the base64-encoded checksum string.
    fn finalize_b64(self) -> String {
        use base64::Engine;
        let cksum = self.finalize_raw();
        base64::engine::general_purpose::STANDARD.encode(cksum.bytes())
    }

    fn aws_algorithm_name(&self) -> &'static str {
        self.0.algorithm().as_str()
    }
}

/// Identifies which streaming write operation a request maps to.
/// Whether the request body uses aws-chunked encoding.
#[derive(Debug, PartialEq, Clone)]
pub(super) enum ChunkedMode {
    /// Plain HTTP body (Content-Length).
    None,
    /// aws-chunked with per-chunk signatures, no trailers.
    Signed { expected_len: u64 },
    /// aws-chunked with per-chunk signatures + signed trailers.
    SignedTrailer { expected_len: u64 },
    /// aws-chunked unsigned with trailers.
    UnsignedTrailer { expected_len: u64 },
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RouteBoundedBodyOperation {
    PutObject,
    PostObject,
    UploadPart,
}

#[cfg(test)]
type RouteBoundedBodyWaitHook = Arc<dyn Fn(RouteBoundedBodyOperation) + Send + Sync + 'static>;

impl ChunkedMode {
    fn is_trailer_mode(&self) -> bool {
        matches!(
            self,
            ChunkedMode::SignedTrailer { .. } | ChunkedMode::UnsignedTrailer { .. }
        )
    }

    fn expected_len(&self) -> Option<u64> {
        match self {
            ChunkedMode::None => Option::None,
            ChunkedMode::Signed { expected_len }
            | ChunkedMode::SignedTrailer { expected_len }
            | ChunkedMode::UnsignedTrailer { expected_len } => Some(*expected_len),
        }
    }
}

/// Identifies which streaming write operation a request maps to.
#[derive(Debug, PartialEq)]
enum StreamingWriteOp {
    PutObject {
        bucket: BucketName,
        key: String,
    },
    UploadPart {
        bucket: BucketName,
        key: String,
        upload_id: String,
        part_number: String,
    },
}

/// Tunable timeouts for the HTTP serve layer.
pub struct ServeConfig {
    /// Time allowed for TLS handshake completion and for a client to send
    /// request headers. Also serves as the idle timeout between keep-alive
    /// requests.
    pub header_read_timeout: Duration,
    /// Time a request will wait for a processing slot before being shed with
    /// 503 `SlowDown`.
    pub request_wait_timeout: Duration,
    /// Per-frame idle timeout for body reads. Resets on every chunk so
    /// slow-but-steady uploads complete; only truly stalled connections are
    /// killed.
    pub body_idle_timeout: Duration,
    /// Absolute deadline for request-body bytes received before authentication
    /// and authorization complete. This bounds slow-but-steady unauthenticated
    /// clients without limiting authorized streaming object payloads.
    pub pre_auth_body_timeout: Duration,
    /// Chunk size used when pulling data from core `ReadHandle`s into the HTTP
    /// response body stream.
    pub stream_read_chunk_size: usize,
    /// Panic instead of returning HTTP 500 responses.
    ///
    /// This is a diagnostic mode for local conformance/stress runs where SDK
    /// retries can otherwise hide transient internal errors.
    #[cfg(any(test, debug_assertions))]
    pub panic_on_500: bool,
    /// Abort the process instead of returning HTTP 500 responses.
    ///
    /// This is stricter than `panic_on_500`: it makes hidden 500s fail the
    /// whole local test process instead of only dropping one request task.
    #[cfg(any(test, debug_assertions))]
    pub abort_on_500: bool,
    /// Enable local operator-only diagnostics endpoints.
    ///
    /// The binary config only permits this on loopback listeners. The endpoints
    /// expose bounded counters and an explicit flight-recorder stderr dump
    /// trigger; they do not return request headers, payload bytes, keys, or
    /// recorder details over HTTP.
    #[cfg(any(test, feature = "local-debug-endpoints"))]
    pub local_debug_endpoint: bool,
    /// Status source for frontend control-plane runtime-map refresh diagnostics.
    pub frontend_runtime_map_refresh_status:
        Option<storage::StorageClusterRuntimeMapRefreshLoopStatusHandle>,
    #[cfg(test)]
    route_bounded_body_wait_hook: Option<RouteBoundedBodyWaitHook>,
}

impl Default for ServeConfig {
    fn default() -> Self {
        Self {
            header_read_timeout: Duration::from_secs(30),
            request_wait_timeout: Duration::from_secs(5),
            body_idle_timeout: Duration::from_secs(30),
            pre_auth_body_timeout: Duration::from_secs(60),
            stream_read_chunk_size: server_core::coordinator::INTERNAL_SEGMENT_SIZE,
            #[cfg(any(test, debug_assertions))]
            panic_on_500: false,
            #[cfg(any(test, debug_assertions))]
            abort_on_500: false,
            #[cfg(any(test, feature = "local-debug-endpoints"))]
            local_debug_endpoint: false,
            frontend_runtime_map_refresh_status: None,
            #[cfg(test)]
            route_bounded_body_wait_hook: None,
        }
    }
}

/// Shared server state: frontend pool, round-robin counter, request semaphore,
/// and timeout configuration.
struct ServerState {
    pool: Vec<Arc<HttpFrontend>>,
    host_id: Arc<str>,
    counter: AtomicUsize,
    request_semaphore: Arc<Semaphore>,
    _request_admission_capacity_guard: observability::RequestAdmissionCapacityGuard,
    segment_buffer_pool: SegmentBufferPool,
    config: ServeConfig,
    endpoint_kind: EndpointKind,
}

struct SegmentBufferPool {
    max_cached: usize,
    cached: Mutex<Vec<Vec<u8>>>,
}

fn lock_mutex_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
}

struct PooledSegmentBuffer {
    state: Arc<ServerState>,
    buf: Option<Vec<u8>>,
}

impl SegmentBufferPool {
    fn new(max_inflight_requests: usize) -> Self {
        let default_cached = std::thread::available_parallelism()
            .map(|n| n.get().saturating_mul(2))
            .unwrap_or(8)
            .max(1);
        Self {
            max_cached: default_cached.min(max_inflight_requests).max(1),
            cached: Mutex::new(Vec::new()),
        }
    }

    fn checkout(&self) -> Vec<u8> {
        let mut buf = lock_mutex_unpoisoned(&self.cached)
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(crate::coordinator::INTERNAL_SEGMENT_SIZE));
        buf.clear();
        buf
    }

    fn recycle(&self, mut buf: Vec<u8>) {
        if buf.capacity() < crate::coordinator::INTERNAL_SEGMENT_SIZE {
            return;
        }
        buf.clear();
        let mut cached = lock_mutex_unpoisoned(&self.cached);
        if cached.len() < self.max_cached {
            cached.push(buf);
        }
    }
}

impl PooledSegmentBuffer {
    fn new(state: &Arc<ServerState>) -> Self {
        Self {
            state: Arc::clone(state),
            buf: Some(state.segment_buffer_pool.checkout()),
        }
    }
}

impl std::ops::Deref for PooledSegmentBuffer {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        self.buf.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PooledSegmentBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buf.as_mut().unwrap()
    }
}

impl Drop for PooledSegmentBuffer {
    fn drop(&mut self) {
        if let Some(buf) = self.buf.take() {
            self.state.segment_buffer_pool.recycle(buf);
        }
    }
}

enum StreamingAbortCleanup {
    Put {
        ctx: Arc<super::StreamingPutContext>,
        session_id: SessionId,
    },
    Post {
        ctx: Arc<super::StreamingPostContext>,
    },
    Part {
        ctx: Arc<super::StreamingPartContext>,
        _active_session: observability::StreamUploadActiveSessionGuard,
    },
}

struct StreamingAbortGuard {
    state: Arc<ServerState>,
    cleanup: Mutex<Option<StreamingAbortCleanup>>,
    disarmed: AtomicBool,
    put_heartbeat_started: AtomicBool,
}

impl StreamingAbortGuard {
    fn new(state: &Arc<ServerState>) -> Arc<Self> {
        Arc::new(Self {
            state: Arc::clone(state),
            cleanup: Mutex::new(None),
            disarmed: AtomicBool::new(false),
            put_heartbeat_started: AtomicBool::new(false),
        })
    }

    fn arm_put(&self, ctx: &Arc<super::StreamingPutContext>, session_id: &SessionId) {
        *lock_mutex_unpoisoned(&self.cleanup) = Some(StreamingAbortCleanup::Put {
            ctx: Arc::clone(ctx),
            session_id: session_id.clone(),
        });
    }

    fn start_put_object_heartbeat(
        self: &Arc<Self>,
        ctx: Weak<super::StreamingPutContext>,
        session_id: SessionId,
    ) {
        if self.put_heartbeat_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let guard: Weak<Self> = Arc::downgrade(self);
        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(STREAMING_PUT_HEARTBEAT_INTERVAL).await;
                let Some(guard) = guard.upgrade() else {
                    break;
                };
                if guard.disarmed.load(Ordering::Acquire) {
                    break;
                }
                drop(guard);
                let Some(ctx) = ctx.upgrade() else {
                    break;
                };
                let state = Arc::clone(&state);
                let session_id = session_id.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    let frontend = acquire_frontend(&state);
                    frontend.heartbeat_streaming_put_object(&ctx, &session_id)
                })
                .await;
            }
        });
    }

    fn start_put_heartbeat(
        self: &Arc<Self>,
        ctx: &Arc<super::StreamingPutContext>,
        session_id: &SessionId,
    ) {
        self.start_put_object_heartbeat(Arc::downgrade(ctx), session_id.clone());
    }

    fn start_post_object_heartbeat(self: &Arc<Self>, ctx: Weak<super::StreamingPostContext>) {
        if self.put_heartbeat_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let guard: Weak<Self> = Arc::downgrade(self);
        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(STREAMING_PUT_HEARTBEAT_INTERVAL).await;
                let Some(guard) = guard.upgrade() else {
                    break;
                };
                if guard.disarmed.load(Ordering::Acquire) {
                    break;
                }
                drop(guard);
                let Some(ctx) = ctx.upgrade() else {
                    break;
                };
                let state = Arc::clone(&state);
                let _ = tokio::task::spawn_blocking(move || {
                    let frontend = acquire_frontend(&state);
                    frontend.heartbeat_streaming_post_object(&ctx)
                })
                .await;
            }
        });
    }

    fn arm_post(self: &Arc<Self>, ctx: &Arc<super::StreamingPostContext>) {
        *lock_mutex_unpoisoned(&self.cleanup) = Some(StreamingAbortCleanup::Post {
            ctx: Arc::clone(ctx),
        });
        self.start_post_object_heartbeat(Arc::downgrade(ctx));
    }

    fn arm_part(&self, ctx: &Arc<super::StreamingPartContext>) {
        let active_session = observability::stream_upload_active_session_guard();
        emit_streaming_part_phase(
            ctx,
            observability::StreamUploadPhase::SessionCreated,
            None,
            None,
            None,
            None,
        );
        *lock_mutex_unpoisoned(&self.cleanup) = Some(StreamingAbortCleanup::Part {
            ctx: Arc::clone(ctx),
            _active_session: active_session,
        });
    }

    fn disarm(&self) {
        self.disarmed.store(true, Ordering::Release);
        *lock_mutex_unpoisoned(&self.cleanup) = None;
    }
}

impl Drop for StreamingAbortGuard {
    fn drop(&mut self) {
        if self.disarmed.load(Ordering::Acquire) {
            return;
        }
        let Some(cleanup) = lock_mutex_unpoisoned(&self.cleanup).take() else {
            return;
        };
        let state = Arc::clone(&self.state);
        tokio::task::spawn_blocking(move || {
            let frontend = acquire_frontend(&state);
            match cleanup {
                StreamingAbortCleanup::Put { ctx, session_id } => {
                    frontend.abort_streaming_put(&ctx, &session_id);
                }
                StreamingAbortCleanup::Post { ctx } => {
                    frontend.abort_streaming_post_object(&ctx);
                }
                StreamingAbortCleanup::Part { ctx, .. } => {
                    frontend.abort_streaming_part(&ctx);
                }
            }
        });
    }
}

fn spawn_blocking_with_trace<F, R>(
    trace: observability::TraceContext,
    f: F,
) -> tokio::task::JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let _trace = observability::AttachedTrace::new(trace);
        f()
    })
}

fn elapsed_micros(start: Instant) -> u64 {
    start.elapsed().as_micros() as u64
}

#[derive(Default)]
struct StreamingBodyTiming {
    data_frames: u64,
    frame_wait_us: u64,
    decode_us: u64,
    ingest_local_us: u64,
    append_wait_us: u64,
}

/// Run the HTTP server, accepting connections and dispatching to the frontend pool.
///
/// Two layers of admission control:
/// - A connection semaphore (`max_connections`) limits concurrent TCP connections.
/// - A request semaphore (`max_inflight_requests`) limits concurrent
///   in-flight requests,
///   acquired before body collection to bound memory.
///
/// Connection-level timeouts prevent idle/slow clients from pinning slots.
/// Requests that cannot acquire a processing slot within `REQUEST_WAIT_TIMEOUT`
/// are shed with 503 `SlowDown`.
pub async fn serve(
    listener: TcpListener,
    frontends: Vec<HttpFrontend>,
    max_connections: u32,
    max_inflight_requests: u32,
    config: ServeConfig,
) {
    serve_plain_or_tls(
        listener,
        frontends,
        max_connections,
        max_inflight_requests,
        config,
        EndpointKind::S3Only,
        None,
    )
    .await;
}

pub async fn serve_tls(
    listener: TcpListener,
    tls_acceptor: TlsAcceptor,
    frontends: Vec<HttpFrontend>,
    max_connections: u32,
    max_inflight_requests: u32,
    config: ServeConfig,
) {
    serve_plain_or_tls(
        listener,
        frontends,
        max_connections,
        max_inflight_requests,
        config,
        EndpointKind::SharedRegional,
        Some(tls_acceptor),
    )
    .await;
}

/// Run a TLS-only STS Query API listener.
pub async fn serve_sts_tls(
    listener: TcpListener,
    tls_acceptor: TlsAcceptor,
    frontends: Vec<HttpFrontend>,
    max_connections: u32,
    max_inflight_requests: u32,
    config: ServeConfig,
) {
    serve_plain_or_tls(
        listener,
        frontends,
        max_connections,
        max_inflight_requests,
        config,
        EndpointKind::StsOnly,
        Some(tls_acceptor),
    )
    .await;
}

async fn serve_plain_or_tls(
    listener: TcpListener,
    frontends: Vec<HttpFrontend>,
    max_connections: u32,
    max_inflight_requests: u32,
    config: ServeConfig,
    endpoint_kind: EndpointKind,
    tls_acceptor: Option<TlsAcceptor>,
) {
    assert!(
        matches!(endpoint_kind, EndpointKind::S3Only) || tls_acceptor.is_some(),
        "SharedRegional and StsOnly endpoints require TLS"
    );
    assert!(!frontends.is_empty(), "at least one frontend required");
    let host_id = frontends
        .first()
        .expect("frontends is non-empty")
        .host_id
        .clone();
    assert!(
        frontends.iter().all(|frontend| frontend.host_id == host_id),
        "all frontends must share the same host id"
    );
    let first_coordinator = &frontends
        .first()
        .expect("frontends is non-empty")
        .coordinator;
    assert!(
        frontends.iter().all(|frontend| frontend
            .coordinator
            .shares_storage_route_admission_with(first_coordinator)),
        "all frontends must share one storage route-admission domain"
    );

    let header_read_timeout = config.header_read_timeout;
    let state = Arc::new(ServerState {
        pool: frontends.into_iter().map(Arc::new).collect(),
        host_id,
        counter: AtomicUsize::new(0),
        request_semaphore: Arc::new(Semaphore::new(max_inflight_requests as usize)),
        _request_admission_capacity_guard: observability::request_admission_capacity_guard(
            u64::from(max_inflight_requests),
        ),
        segment_buffer_pool: SegmentBufferPool::new(max_inflight_requests as usize),
        config,
        endpoint_kind,
    });

    let conn_semaphore = Arc::new(Semaphore::new(max_connections as usize));

    loop {
        // Acquire connection permit before accepting — excess connections queue
        // in the kernel listen backlog, providing TCP-level backpressure.
        let conn_permit = conn_semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("connection semaphore closed");

        let (stream, addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                eprintln!("accept error: {e}");
                drop(conn_permit);
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        if let Err(e) = stream.set_nodelay(true) {
            eprintln!("set_nodelay error: {e}");
        }

        let state = Arc::clone(&state);
        let tls_acceptor = tls_acceptor.clone();
        let source_ip = Some(canonical_source_ip(addr.ip()));
        tokio::spawn(async move {
            let _conn_permit = conn_permit;
            match tls_acceptor {
                Some(acceptor) => {
                    match tokio::time::timeout(header_read_timeout, acceptor.accept(stream)).await {
                        Ok(Ok(tls_stream)) => {
                            let tls_version = tls_stream
                                .get_ref()
                                .1
                                .protocol_version()
                                .and_then(tls_protocol_version);
                            serve_connection(
                                Arc::clone(&state),
                                TokioIo::new(tls_stream),
                                header_read_timeout,
                                TransportSecurity::Tls,
                                tls_version,
                                source_ip,
                            )
                            .await;
                        }
                        Ok(Err(e)) => {
                            eprintln!("tls handshake error: {e}");
                        }
                        Err(_) => {
                            eprintln!(
                                "tls handshake timeout after {} ms",
                                header_read_timeout.as_millis()
                            );
                        }
                    }
                }
                None => {
                    serve_connection(
                        Arc::clone(&state),
                        TokioIo::new(stream),
                        header_read_timeout,
                        TransportSecurity::InsecureHttp,
                        None,
                        source_ip,
                    )
                    .await;
                }
            }
        });
    }
}

fn canonical_source_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(ip)),
        IpAddr::V4(_) => ip,
    }
}

fn tls_protocol_version(
    version: tokio_rustls::rustls::ProtocolVersion,
) -> Option<TlsProtocolVersion> {
    match version {
        tokio_rustls::rustls::ProtocolVersion::TLSv1_2 => Some(TlsProtocolVersion::Tls12),
        tokio_rustls::rustls::ProtocolVersion::TLSv1_3 => Some(TlsProtocolVersion::Tls13),
        _ => None,
    }
}

async fn serve_connection<IO>(
    state: Arc<ServerState>,
    io: TokioIo<IO>,
    header_read_timeout: Duration,
    transport_security: TransportSecurity,
    tls_version: Option<TlsProtocolVersion>,
    source_ip: Option<IpAddr>,
) where
    IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let conn = http1::Builder::new()
        .timer(TokioTimer::new())
        .header_read_timeout(header_read_timeout)
        .serve_connection(
            io,
            service_fn(move |req: Request<Incoming>| {
                let state = Arc::clone(&state);
                async move { handle(state, req, transport_security, tls_version, source_ip).await }
            }),
        );
    // Lingering close: take the IO back from hyper instead of letting it
    // close the socket the moment the connection is done. Closing a socket
    // that still holds unread request bytes makes the kernel send RST, which
    // can discard response data the client has not yet read. First shut down
    // the response side, after Hyper has finished writing it, then drain and
    // discard whatever the client keeps sending until it goes quiet, reaches
    // EOF, or exceeds the bound. Keeping the read side alive during that
    // interval prevents the eventual socket close from replacing the
    // completed response with a reset.
    let Ok(mut parts) = conn.without_shutdown().await else {
        return;
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let io = parts.io.inner_mut();
    let _ = io.shutdown().await;
    let deadline = tokio::time::Instant::now() + LINGERING_CLOSE_MAX;
    let mut discard = [0u8; 8192];
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        let read_timeout = LINGERING_CLOSE_QUIET.min(deadline - now);
        match tokio::time::timeout(read_timeout, io.read(&mut discard)).await {
            // Client finished sending; the receive queue is drained.
            Ok(Ok(0)) => break,
            Ok(Ok(_)) => {}
            Ok(Err(_)) => break,
            // Client went quiet: nothing unread remains to trigger an RST.
            Err(_) => break,
        }
    }
}

struct TrackedIncoming {
    inner: Incoming,
    eof_observed: bool,
}

impl TrackedIncoming {
    fn new(inner: Incoming) -> Self {
        // `true` is a definitive empty body. A `false` value is only a hint:
        // chunked bodies can remain marked non-terminal after yielding EOF.
        let eof_observed = inner.is_end_stream();
        Self {
            inner,
            eof_observed,
        }
    }

    fn into_parts(self) -> (Incoming, bool) {
        (self.inner, self.eof_observed)
    }
}

impl Body for TrackedIncoming {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let frame = Pin::new(&mut self.inner).poll_frame(cx);
        if matches!(frame, Poll::Ready(None)) {
            self.eof_observed = true;
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.eof_observed
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// Handle a single HTTP request: parse, route streaming writes, or buffer body
/// for control-plane dispatch.
///
/// For `PutObject`/`UploadPart` (except copy-source variants), body frames are
/// consumed incrementally and fed to coordinator segment appends. All other
/// requests collect the full body first.
///
/// Errors are always converted to S3 XML error responses.
async fn handle(
    state: Arc<ServerState>,
    req: Request<Incoming>,
    transport_security: TransportSecurity,
    tls_version: Option<TlsProtocolVersion>,
    source_ip: Option<IpAddr>,
) -> Result<http::Response<S3HyperBody>, Infallible> {
    let trace = crate::http::new_request_trace_context();
    let _trace = observability::AttachedTrace::new(trace.clone());
    let wire_ids = WireResponseIds::new(trace.request_id(), state.host_id.clone());
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();
    let request_epoch_seconds = storage::clock::current_time_millis() / 1_000;
    let response_trace = crate::http::ResponseTraceMeta::new(
        trace.clone(),
        state.host_id.clone(),
        &method,
        &path,
        &query,
    );
    #[cfg(feature = "deep-tracing")]
    let query_summary = observability::query_summary(&query);
    #[cfg(feature = "deep-tracing")]
    let range_suffix = req
        .headers()
        .get(http::header::RANGE)
        .and_then(|value| value.to_str().ok())
        .map(|value| format!(" raw_range={}", observability::escaped(value)))
        .unwrap_or_default();
    #[cfg(feature = "deep-tracing")]
    let _ = observability::event_in_context(
        &trace,
        TRACE_TARGET,
        "request_start",
        Some(format_args!(
            "method={} path={:?} has_query={} query_params={} sigv4_query={}{}",
            method,
            path,
            query_summary.has_query(),
            query_summary.param_count(),
            query_summary.has_sigv4_params(),
            range_suffix
        )),
    );

    let (parts, body) = req.into_parts();
    let mut body = TrackedIncoming::new(body);

    #[cfg(any(test, feature = "local-debug-endpoints"))]
    if state.config.local_debug_endpoint {
        if let Some(resp) = local_debug_response(&state, &parts.method, parts.uri.path()) {
            return Ok(response_to_hyper_with_request_body(
                &state,
                resp,
                body,
                None,
                response_trace,
            ));
        }
    }

    let _ = observability::emit_request_start(
        &trace,
        TRACE_TARGET,
        observability::RequestStartSummary {
            method: &method,
            path: &path,
            query: response_trace.query,
        },
    );

    // Acquire request permit before body collection to bound memory.
    let admission_started_at = Instant::now();
    let req_permit = match tokio::time::timeout(
        state.config.request_wait_timeout,
        Arc::clone(&state.request_semaphore).acquire_owned(),
    )
    .await
    {
        Ok(Ok(permit)) => {
            let wait_us = admission_started_at.elapsed().as_micros();
            if wait_us >= REQUEST_ADMISSION_WAIT_EVENT_THRESHOLD_US {
                let _ = observability::emit_request_admission_wait(
                    &response_trace.context,
                    TRACE_TARGET,
                    observability::RequestAdmissionWaitSummary {
                        method: response_trace.method.as_str(),
                        path: response_trace.path.as_str(),
                        query: response_trace.query,
                        wait_us,
                    },
                );
            }
            permit
        }
        _ => {
            let wait_us = admission_started_at.elapsed().as_micros();
            let _ = observability::emit_request_admission_timeout(
                &response_trace.context,
                TRACE_TARGET,
                observability::RequestAdmissionTimeoutSummary {
                    method: response_trace.method.as_str(),
                    path: response_trace.path.as_str(),
                    query: response_trace.query,
                    wait_us,
                    timeout_us: state.config.request_wait_timeout.as_micros(),
                },
            );
            let resp = S3Response::error_with_ids(&ServerError::SlowDown, "", &wire_ids);
            return Ok(response_to_hyper_with_request_body(
                &state,
                resp,
                body,
                None,
                response_trace,
            ));
        }
    };
    // Keep the foreground-pressure signal active for the complete admitted
    // request, including body collection and storage RPCs. The response body
    // takes over this signal when the handler returns.
    let request_admission = HttpRequestAdmission::new(req_permit);

    // Check if this request should use the streaming write path.
    let streaming_op = match is_streaming_write_for_endpoint(state.endpoint_kind, &parts) {
        Ok(op) => op,
        Err(err) => {
            // Routing has rejected a request whose body has not been
            // consumed. The connection cannot be reused safely: a client may
            // still be sending the declared body. Keep the unread Incoming
            // alive until Hyper has delivered the response body; dropping it
            // first can close the connection after only the response headers.
            return Ok(response_to_hyper_with_request_body(
                &state,
                S3Response::error_with_ids(&err, "", &wire_ids),
                body,
                Some(request_admission),
                response_trace,
            ));
        }
    };
    if let Some(op) = streaming_op {
        let s3req = match S3Request::from_hyper_headers_with_source_ip(
            parts,
            transport_security,
            source_ip,
            request_epoch_seconds,
        )
        .map(|req| req.with_tls_version(tls_version))
        {
            Ok(req) => req,
            Err(err) => {
                return Ok(response_to_hyper_with_request_body(
                    &state,
                    S3Response::error_with_ids(&err, "", &wire_ids),
                    body,
                    Some(request_admission),
                    response_trace,
                ))
            }
        };
        let origin = s3req.header("origin").map(str::to_string);
        let method = s3req.method.as_str().to_string();
        let chunked = match parse_chunked_mode(&s3req) {
            Ok(mode) => mode,
            Err(err) => {
                return Ok(response_to_hyper_with_request_body(
                    &state,
                    S3Response::error_with_ids(&err, "", &wire_ids),
                    body,
                    Some(request_admission),
                    response_trace,
                ))
            }
        };
        let resp = match op {
            StreamingWriteOp::PutObject { bucket, key } => {
                let mut resp = handle_streaming_put(
                    Arc::clone(&state),
                    s3req,
                    &mut body,
                    bucket.clone(),
                    key,
                    chunked,
                    trace.clone(),
                    wire_ids.clone(),
                )
                .await;
                append_actual_cors_headers(
                    &state,
                    &mut resp,
                    &bucket,
                    origin.as_deref(),
                    &method,
                    &trace,
                )
                .await;
                resp
            }
            StreamingWriteOp::UploadPart {
                bucket,
                key,
                upload_id,
                part_number,
            } => {
                let mut resp = handle_streaming_part(
                    Arc::clone(&state),
                    s3req,
                    &mut body,
                    bucket.clone(),
                    key,
                    upload_id,
                    part_number,
                    chunked,
                    trace.clone(),
                    wire_ids.clone(),
                )
                .await;
                append_actual_cors_headers(
                    &state,
                    &mut resp,
                    &bucket,
                    origin.as_deref(),
                    &method,
                    &trace,
                )
                .await;
                resp
            }
        };
        return Ok(response_to_hyper_with_request_body(
            &state,
            resp,
            body,
            Some(request_admission),
            response_trace,
        ));
    }

    if state.endpoint_kind != EndpointKind::StsOnly {
        if let Some(bucket) = post_object_bucket(&parts) {
            let s3req = match S3Request::from_hyper_headers_with_source_ip(
                parts,
                transport_security,
                source_ip,
                request_epoch_seconds,
            )
            .map(|req| req.with_tls_version(tls_version))
            {
                Ok(req) => req,
                Err(err) => {
                    return Ok(response_to_hyper_with_request_body(
                        &state,
                        S3Response::error_with_ids(&err, "", &wire_ids),
                        body,
                        Some(request_admission),
                        response_trace,
                    ));
                }
            };
            let origin = s3req.header("origin").map(str::to_string);
            let method = s3req.method.as_str().to_string();
            let mut resp = handle_streaming_post_object(
                Arc::clone(&state),
                s3req,
                &mut body,
                bucket.clone(),
                trace.clone(),
                wire_ids.clone(),
            )
            .await;
            append_actual_cors_headers(
                &state,
                &mut resp,
                &bucket,
                origin.as_deref(),
                &method,
                &trace,
            )
            .await;
            return Ok(response_to_hyper_with_request_body(
                &state,
                resp,
                body,
                Some(request_admission),
                response_trace,
            ));
        }
    }

    if parts.method == http::Method::OPTIONS {
        let s3req = match S3Request::from_hyper_headers_with_source_ip(
            parts,
            transport_security,
            source_ip,
            request_epoch_seconds,
        )
        .map(|req| req.with_tls_version(tls_version))
        {
            Ok(req) => req,
            Err(err) => {
                return Ok(response_to_hyper_with_request_body(
                    &state,
                    S3Response::error_with_ids(&err, "", &wire_ids),
                    body,
                    Some(request_admission),
                    response_trace,
                ));
            }
        };
        let state_ref = Arc::clone(&state);
        let wire_ids_for_blocking = wire_ids.clone();
        let resp = spawn_blocking_with_trace(trace, move || {
            let frontend = acquire_frontend(&state_ref);
            match state_ref.endpoint_kind {
                EndpointKind::StsOnly => {
                    frontend.handle_sts_request(&s3req, &wire_ids_for_blocking)
                }
                EndpointKind::S3Only | EndpointKind::SharedRegional => frontend
                    .handle_service_request(
                        state_ref.endpoint_kind,
                        &s3req,
                        &wire_ids_for_blocking,
                    ),
            }
        })
        .await
        .unwrap_or_else(|_| internal_error_response(&wire_ids));
        return Ok(response_to_hyper_with_request_body(
            &state,
            resp,
            body,
            Some(request_admission),
            response_trace,
        ));
    }

    // Non-streaming path: collect the full body for buffered control-plane
    // style requests (mostly XML payloads).
    let body_limit = buffered_body_limit_for_request_parts(state.endpoint_kind, &parts);
    let body_bytes = match collect_body_with_limit(
        &mut body,
        state.config.body_idle_timeout,
        state.config.pre_auth_body_timeout,
        body_limit,
    )
    .await
    {
        Ok(bytes) => bytes,
        Err(err) => {
            return Ok(response_to_hyper_with_request_body(
                &state,
                S3Response::error_with_ids(&err, "", &wire_ids),
                body,
                Some(request_admission),
                response_trace,
            ));
        }
    };

    let s3req = match S3Request::from_hyper_with_source_ip(
        parts,
        body_bytes,
        transport_security,
        source_ip,
        request_epoch_seconds,
    )
    .map(|req| req.with_tls_version(tls_version))
    {
        Ok(req) => req,
        Err(err) => {
            return Ok(response_to_hyper_with_request_body(
                &state,
                S3Response::error_with_ids(&err, "", &wire_ids),
                body,
                Some(request_admission),
                response_trace,
            ));
        }
    };

    let state_ref = Arc::clone(&state);
    let wire_ids_for_blocking = wire_ids.clone();
    let resp = spawn_blocking_with_trace(trace, move || {
        let frontend = acquire_frontend(&state_ref);
        match state_ref.endpoint_kind {
            EndpointKind::StsOnly => frontend.handle_sts_request(&s3req, &wire_ids_for_blocking),
            EndpointKind::S3Only | EndpointKind::SharedRegional => frontend.handle_service_request(
                state_ref.endpoint_kind,
                &s3req,
                &wire_ids_for_blocking,
            ),
        }
    })
    .await
    .unwrap_or_else(|_| internal_error_response(&wire_ids));

    Ok(response_to_hyper_with_request_body(
        &state,
        resp,
        body,
        Some(request_admission),
        response_trace,
    ))
}

#[cfg(any(test, feature = "local-debug-endpoints"))]
#[cfg(test)]
static SUPPRESS_LOCAL_DEBUG_FLIGHT_RECORDER_DUMP: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(any(test, feature = "local-debug-endpoints"))]
#[cfg(test)]
static LOCAL_DEBUG_FLIGHT_RECORDER_DUMP_CALLS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[cfg(any(test, feature = "local-debug-endpoints"))]
fn dump_local_debug_flight_recorder() {
    #[cfg(test)]
    {
        LOCAL_DEBUG_FLIGHT_RECORDER_DUMP_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if SUPPRESS_LOCAL_DEBUG_FLIGHT_RECORDER_DUMP.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
    }

    observability::dump_flight_recorder_to_stderr("local-debug-endpoint");
}

#[cfg(any(test, feature = "local-debug-endpoints"))]
fn local_debug_response(
    state: &Arc<ServerState>,
    method: &http::Method,
    path: &str,
) -> Option<S3Response> {
    match (method, path) {
        (&http::Method::GET, "/__argmin/debug/metrics") => {
            let body = local_debug_metrics_body(state);
            Some(S3Response {
                status_code: 200,
                headers: vec![
                    (
                        "Content-Type".to_string(),
                        "text/plain; charset=utf-8".to_string(),
                    ),
                    ("Content-Length".to_string(), body.len().to_string()),
                ],
                body: body.into_bytes(),
                stream: None,
                error_diagnostic: None,
                include_wire_ids: true,
            })
        }
        (&http::Method::POST, "/__argmin/debug/flight-recorder/dump") => {
            dump_local_debug_flight_recorder();
            Some(S3Response {
                status_code: 204,
                headers: Vec::new(),
                body: Vec::new(),
                stream: None,
                error_diagnostic: None,
                include_wire_ids: true,
            })
        }
        (&http::Method::GET, path)
            if path.starts_with("/__argmin/debug/bucket-delete-attempt/") =>
        {
            let raw_bucket = path.trim_start_matches("/__argmin/debug/bucket-delete-attempt/");
            let bucket = match percent_decode_strict(raw_bucket)
                .ok()
                .and_then(|bucket| BucketName::try_from(bucket).ok())
            {
                Some(bucket) => bucket,
                None => {
                    return Some(local_debug_text_response(
                        400,
                        "invalid bucket\n".to_string(),
                    ));
                }
            };
            let storage = state
                .pool
                .first()
                .expect("server has at least one frontend")
                .coordinator
                .storage_node_for_request();
            match storage.bucket_delete_diagnostic(&bucket) {
                Ok(diagnostic) => Some(local_debug_text_response(200, diagnostic.into_text())),
                Err(error) => Some(local_debug_text_response(409, format!("{error:?}\n"))),
            }
        }
        (&http::Method::GET, path)
            if path.starts_with("/__argmin/debug/object-payload-placement/") =>
        {
            let raw_object = path.trim_start_matches("/__argmin/debug/object-payload-placement/");
            let Some((raw_bucket, raw_key)) = raw_object.split_once('/') else {
                return Some(local_debug_text_response(
                    400,
                    "invalid object path\n".to_string(),
                ));
            };
            let bucket = percent_decode_strict(raw_bucket)
                .ok()
                .and_then(|bucket| BucketName::try_from(bucket).ok());
            let key = percent_decode_strict(raw_key)
                .ok()
                .and_then(|key| ObjectKey::try_from(key).ok());
            let (Some(bucket), Some(key)) = (bucket, key) else {
                return Some(local_debug_text_response(
                    400,
                    "invalid object path\n".to_string(),
                ));
            };
            let diagnostic = state
                .pool
                .first()
                .expect("server has at least one frontend")
                .coordinator
                .object_payload_placement_diagnostic(&bucket, &key);
            let status_code = match diagnostic.outcome() {
                ObjectPayloadPlacementDiagnosticOutcome::Success => 200,
                ObjectPayloadPlacementDiagnosticOutcome::Mismatch
                | ObjectPayloadPlacementDiagnosticOutcome::Conflict => 409,
            };
            Some(local_debug_text_response(
                status_code,
                diagnostic.into_text(),
            ))
        }
        (&http::Method::POST, path)
            if path.starts_with("/__argmin/debug/metadata-checkpoint/record/") =>
        {
            let selector = path.trim_start_matches("/__argmin/debug/metadata-checkpoint/record/");
            let result = state
                .pool
                .first()
                .expect("server has at least one frontend")
                .coordinator
                .metadata_checkpoint_diagnostic(selector);
            let status_code = match result.outcome() {
                MetadataCheckpointDiagnosticOutcome::Success => 200,
                MetadataCheckpointDiagnosticOutcome::InvalidInput => 400,
                MetadataCheckpointDiagnosticOutcome::Conflict => 409,
            };
            Some(local_debug_text_response(status_code, result.into_text()))
        }
        (_, path) if path.starts_with("/__argmin/debug/") || path == "/__argmin/debug" => {
            let body = b"not found\n".to_vec();
            Some(S3Response {
                status_code: 404,
                headers: vec![
                    (
                        "Content-Type".to_string(),
                        "text/plain; charset=utf-8".to_string(),
                    ),
                    ("Content-Length".to_string(), body.len().to_string()),
                ],
                body,
                stream: None,
                error_diagnostic: None,
                include_wire_ids: true,
            })
        }
        _ => None,
    }
}

#[cfg(any(test, feature = "local-debug-endpoints"))]
fn local_debug_text_response(status_code: u16, body: String) -> S3Response {
    S3Response {
        status_code,
        headers: vec![
            (
                "Content-Type".to_string(),
                "text/plain; charset=utf-8".to_string(),
            ),
            ("Content-Length".to_string(), body.len().to_string()),
        ],
        body: body.into_bytes(),
        stream: None,
        error_diagnostic: None,
        include_wire_ids: true,
    }
}

#[cfg(any(test, feature = "local-debug-endpoints"))]
fn local_debug_metrics_body(state: &Arc<ServerState>) -> String {
    use std::fmt::Write as _;

    let snapshot = observability::metrics_snapshot();
    let frontend_storage_cluster_epoch = state
        .pool
        .iter()
        .map(|frontend| {
            frontend
                .coordinator
                .storage_node_for_request()
                .cluster_epoch()
                .get()
        })
        .max()
        .unwrap_or(0);
    let mut body = format!("frontend_storage_cluster_epoch {frontend_storage_cluster_epoch}\n");
    if let Some(status_handle) = &state.config.frontend_runtime_map_refresh_status {
        write_frontend_runtime_map_refresh_status(&mut body, &status_handle.status());
    }
    for (name, value) in snapshot.iter_named() {
        let _ = writeln!(body, "{name} {value}");
    }
    for sample in observability::metadata_command_conflict_dimension_snapshot() {
        let _ = writeln!(
            body,
            "metadata_command_conflict_by_pg_command_total{{pg_id=\"{}\",kind=\"{}\",command_kind=\"{}\"}} {}",
            sample.pg_id, sample.classifier, sample.command_kind, sample.count
        );
    }
    for sample in observability::metadata_command_pending_slot_action_dimension_snapshot() {
        let _ = writeln!(
            body,
            "metadata_command_pending_slot_action_by_pg_command_total{{pg_id=\"{}\",action=\"{}\",command_kind=\"{}\"}} {}",
            sample.pg_id, sample.classifier, sample.command_kind, sample.count
        );
    }
    for sample in observability::metadata_command_recovery_admission_dimension_snapshot() {
        let _ = writeln!(
            body,
            "metadata_command_recovery_admission_by_pg_command_total{{pg_id=\"{}\",admission=\"{}\",command_kind=\"{}\"}} {}",
            sample.pg_id, sample.classifier, sample.command_kind, sample.count
        );
    }
    for sample in observability::metadata_command_recovery_outcome_dimension_snapshot() {
        let _ = writeln!(
            body,
            "metadata_command_recovery_outcome_by_pg_command_total{{pg_id=\"{}\",outcome=\"{}\",command_kind=\"{}\"}} {}",
            sample.pg_id, sample.classifier, sample.command_kind, sample.count
        );
    }
    for sample in observability::metadata_command_budget_dimension_snapshot() {
        let pg_id = debug_metric_optional_pg_id_label(sample.pg_id);
        let operation = debug_metric_label_value(sample.operation);
        let context = debug_metric_label_value(sample.context);
        let _ = writeln!(
            body,
            "metadata_command_budget_exhausted_by_pg_context_total{{pg_id=\"{}\",operation=\"{}\",context=\"{}\"}} {}",
            pg_id, operation, context, sample.count
        );
        let _ = writeln!(
            body,
            "metadata_command_budget_exhausted_by_pg_context_elapsed_us_total{{pg_id=\"{}\",operation=\"{}\",context=\"{}\"}} {}",
            pg_id, operation, context, sample.elapsed_us_total
        );
        let _ = writeln!(
            body,
            "metadata_command_budget_exhausted_by_pg_context_elapsed_us_max{{pg_id=\"{}\",operation=\"{}\",context=\"{}\"}} {}",
            pg_id, operation, context, sample.elapsed_us_max
        );
        let _ = writeln!(
            body,
            "metadata_command_budget_exhausted_by_pg_context_budget_us_max{{pg_id=\"{}\",operation=\"{}\",context=\"{}\"}} {}",
            pg_id, operation, context, sample.budget_us_max
        );
    }
    for sample in observability::metadata_command_backoff_dimension_snapshot() {
        let pg_id = debug_metric_optional_pg_id_label(sample.pg_id);
        let operation = debug_metric_label_value(sample.operation);
        let context = debug_metric_label_value(sample.context);
        let _ = writeln!(
            body,
            "metadata_command_backoff_by_pg_context_total{{pg_id=\"{}\",operation=\"{}\",context=\"{}\"}} {}",
            pg_id, operation, context, sample.count
        );
        let _ = writeln!(
            body,
            "metadata_command_backoff_by_pg_context_sleep_us_total{{pg_id=\"{}\",operation=\"{}\",context=\"{}\"}} {}",
            pg_id, operation, context, sample.sleep_us_total
        );
        let _ = writeln!(
            body,
            "metadata_command_backoff_by_pg_context_sleep_us_max{{pg_id=\"{}\",operation=\"{}\",context=\"{}\"}} {}",
            pg_id, operation, context, sample.sleep_us_max
        );
    }
    for sample in observability::reclaim_work_queue_action_dimension_snapshot() {
        let work_kind = debug_metric_label_value(sample.work_kind);
        let action = debug_metric_label_value(sample.action);
        let _ = writeln!(
            body,
            "reclaim_work_queue_action_by_kind_total{{kind=\"{}\",action=\"{}\"}} {}",
            work_kind, action, sample.count
        );
    }
    for sample in observability::object_payload_reclaim_event_dimension_snapshot() {
        let event = debug_metric_label_value(sample.event);
        let _ = writeln!(
            body,
            "object_payload_reclaim_event_by_pg_total{{pg_id=\"{}\",event=\"{}\"}} {}",
            sample.pg_id, event, sample.count
        );
    }
    for sample in observability::object_payload_reclaim_durable_scan_dimension_snapshot() {
        let outcome = debug_metric_label_value(sample.event);
        let _ = writeln!(
            body,
            "object_payload_reclaim_durable_scan_by_pg_total{{pg_id=\"{}\",outcome=\"{}\"}} {}",
            sample.pg_id, outcome, sample.count
        );
    }
    for sample in observability::shard_repair_event_dimension_snapshot() {
        let pg_id = debug_metric_optional_pg_id_label(sample.pg_id);
        let event = debug_metric_label_value(sample.event);
        let _ = writeln!(
            body,
            "shard_repair_event_by_pg_total{{pg_id=\"{}\",event=\"{}\"}} {}",
            pg_id, event, sample.count
        );
    }
    for sample in observability::shard_backfill_event_dimension_snapshot() {
        let pg_id = debug_metric_optional_pg_id_label(sample.pg_id);
        let event = debug_metric_label_value(sample.event);
        let _ = writeln!(
            body,
            "shard_backfill_event_by_pg_total{{pg_id=\"{}\",event=\"{}\"}} {}",
            pg_id, event, sample.count
        );
    }
    for sample in observability::shard_backfill_outcome_snapshot() {
        let _ = writeln!(
            body,
            "shard_backfill_backfilled_by_pg_total{{pg_id=\"{}\"}} {}",
            sample.pg_id, sample.backfilled
        );
        let _ = writeln!(
            body,
            "shard_backfill_complete_succeeded_by_pg_total{{pg_id=\"{}\"}} {}",
            sample.pg_id, sample.complete_succeeded
        );
    }
    for sample in observability::shard_repair_error_dimension_snapshot() {
        let pg_id = debug_metric_optional_pg_id_label(sample.pg_id);
        let event = debug_metric_label_value(sample.event);
        let error_kind = debug_metric_label_value(sample.error_kind);
        let _ = writeln!(
            body,
            "shard_repair_error_by_pg_total{{pg_id=\"{}\",event=\"{}\",error_kind=\"{}\"}} {}",
            pg_id, event, error_kind, sample.count
        );
    }
    for sample in observability::shard_backfill_error_dimension_snapshot() {
        let pg_id = debug_metric_optional_pg_id_label(sample.pg_id);
        let event = debug_metric_label_value(sample.event);
        let error_kind = debug_metric_label_value(sample.error_kind);
        let _ = writeln!(
            body,
            "shard_backfill_error_by_pg_total{{pg_id=\"{}\",event=\"{}\",error_kind=\"{}\"}} {}",
            pg_id, event, error_kind, sample.count
        );
    }
    for sample in observability::metadata_command_checkpoint_record_error_dimension_snapshot() {
        let outcome = debug_metric_label_value(sample.outcome);
        let error_kind = debug_metric_label_value(sample.error_kind);
        let _ = writeln!(
            body,
            "metadata_command_checkpoint_record_error_by_pg_total{{pg_id=\"{}\",outcome=\"{}\",error_kind=\"{}\"}} {}",
            sample.pg_id, outcome, error_kind, sample.count
        );
    }
    for sample in observability::background_work_admission_dimension_snapshot() {
        let class = debug_metric_label_value(sample.class);
        let event = debug_metric_label_value(sample.event);
        let _ = writeln!(
            body,
            "background_work_admission_by_class_total{{class=\"{}\",event=\"{}\"}} {}",
            class, event, sample.count
        );
        let _ = writeln!(
            body,
            "background_work_admission_by_class_elapsed_us_total{{class=\"{}\",event=\"{}\"}} {}",
            class, event, sample.elapsed_us_total
        );
    }
    body
}

#[cfg(any(test, feature = "local-debug-endpoints"))]
fn write_frontend_runtime_map_refresh_status(
    body: &mut String,
    status: &storage::StorageClusterRuntimeMapRefreshLoopStatus,
) {
    use std::fmt::Write as _;

    let _ = writeln!(
        body,
        "frontend_runtime_map_refresh_attempt_total {}",
        status.attempts
    );
    let _ = writeln!(
        body,
        "frontend_runtime_map_refresh_success_total {}",
        status.successes
    );
    let _ = writeln!(
        body,
        "frontend_runtime_map_refresh_failure_total {}",
        status.failures
    );
    let _ = writeln!(
        body,
        "frontend_pending_command_fallback_recovery_attempt_total {}",
        status.fallback_recovery_attempts
    );
    let _ = writeln!(
        body,
        "frontend_pending_command_fallback_recovery_failure_total {}",
        status.fallback_recovery_failures
    );
    let _ = writeln!(
        body,
        "frontend_runtime_map_refresh_last_success_epoch {}",
        status
            .last_success
            .map_or(0, |success| success.cluster_epoch.get())
    );
    let _ = writeln!(
        body,
        "frontend_runtime_map_refresh_last_success_valid_until_ms {}",
        status.last_success.map_or(0, |success| {
            success.route_map_validity.valid_until_ms().unwrap_or(0)
        })
    );
    let _ = writeln!(
        body,
        "frontend_runtime_map_refresh_last_failure_present {}",
        u8::from(status.last_failure.is_some())
    );
    if let Some(failure) = status.last_failure {
        let _ = writeln!(
            body,
            "frontend_runtime_map_refresh_last_failure_attempt {}",
            failure.attempt
        );
        let _ = writeln!(
            body,
            "frontend_runtime_map_refresh_last_failure_info{{kind=\"{}\"}} 1",
            failure.kind
        );
    }
}

#[cfg(any(test, feature = "local-debug-endpoints"))]
fn debug_metric_optional_pg_id_label(pg_id: Option<u32>) -> String {
    pg_id.map_or_else(|| "unknown".to_string(), |pg_id| pg_id.to_string())
}

#[cfg(any(test, feature = "local-debug-endpoints"))]
fn debug_metric_label_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            ch if ch.is_ascii_whitespace() => escaped.push('_'),
            _ => escaped.push(ch),
        }
    }
    escaped
}

async fn append_actual_cors_headers(
    state: &Arc<ServerState>,
    resp: &mut S3Response,
    bucket: &BucketName,
    origin: Option<&str>,
    method: &str,
    trace: &observability::TraceContext,
) {
    let Some(origin) = origin else {
        return;
    };

    let st = Arc::clone(state);
    let bucket = bucket.clone();
    let origin = origin.to_string();
    let method = method.to_string();
    let headers = spawn_blocking_with_trace(trace.clone(), move || {
        let frontend = acquire_frontend(&st);
        let Ok(storage_route_admission) = frontend.coordinator.admit_storage_route_for_request()
        else {
            return Vec::new();
        };
        frontend.actual_cors_headers(&storage_route_admission, &bucket, &origin, &method)
    })
    .await
    .unwrap_or_default();

    resp.headers.extend(headers);
}

/// Check if a PUT request should use the streaming write path.
///
/// Returns a `StreamingWriteOp` target for `PutObject` and `UploadPart`
/// requests that are not `CopyObject` (no `x-amz-copy-source` header).
///
fn is_streaming_write(
    parts: &http::request::Parts,
) -> Result<Option<StreamingWriteOp>, ServerError> {
    is_streaming_write_for_endpoint(EndpointKind::S3Only, parts)
}

fn is_streaming_write_for_endpoint(
    endpoint_kind: EndpointKind,
    parts: &http::request::Parts,
) -> Result<Option<StreamingWriteOp>, ServerError> {
    if endpoint_kind == EndpointKind::StsOnly {
        return Ok(None);
    }
    if parts.method != http::Method::PUT {
        return Ok(None);
    }

    let path = parts.uri.path();
    let query = parts.uri.query().unwrap_or("");
    let method = parts.method.as_str();

    let op = match route_service(endpoint_kind, method, path, query) {
        Ok(ServiceOperation::S3(op)) => op,
        Ok(ServiceOperation::S3Control(_)) | Err(ServiceRouteError::S3Control(_)) => {
            return Ok(None);
        }
        Err(ServiceRouteError::S3(err @ ServerError::PutMultipartUploadMethodNotAllowed)) => {
            return Err(err);
        }
        Err(ServiceRouteError::S3(_)) => return Ok(None),
    };

    // Check headers via hyper types (not yet parsed into S3Request). Do this
    // after routing so request-shape errors known from the URI can be returned
    // without collecting a CopyObject/UploadPartCopy body.
    let has_copy_source = parts.headers.contains_key("x-amz-copy-source");
    if has_copy_source {
        return Ok(None);
    }
    match op {
        S3Operation::PutObject { bucket, key } => {
            Ok(Some(StreamingWriteOp::PutObject { bucket, key }))
        }
        S3Operation::UploadPart { bucket, key } => {
            let (upload_id, part_number) =
                parse_upload_part_query_raw(query, ServerError::UploadPartMissingUploadId)?;
            Ok(Some(StreamingWriteOp::UploadPart {
                bucket,
                key,
                upload_id,
                part_number,
            }))
        }
        _ => Ok(None),
    }
}

fn buffered_body_limit_for_request_parts(
    endpoint_kind: EndpointKind,
    parts: &http::request::Parts,
) -> usize {
    if endpoint_kind == EndpointKind::StsOnly {
        return super::sts::MAX_STS_QUERY_BODY_SIZE;
    }
    let path = parts.uri.path();
    let query = parts.uri.query().unwrap_or("");
    let method = parts.method.as_str();
    let Ok(op) = super::router::route_service(endpoint_kind, method, path, query) else {
        return MAX_BUFFERED_CONTROL_BODY_SIZE;
    };
    match op {
        ServiceOperation::S3(operation) => buffered_body_limit_for_operation(&operation),
        ServiceOperation::S3Control(S3ControlOperation::TagResource { .. }) => {
            MAX_TAGGING_XML_BYTES
        }
        ServiceOperation::S3Control(
            S3ControlOperation::ListTagsForResource { .. }
            | S3ControlOperation::UntagResource { .. }
            | S3ControlOperation::HeadBucketTags
            | S3ControlOperation::MethodNotAllowed { .. }
            | S3ControlOperation::Options,
        ) => MAX_BUFFERED_CONTROL_BODY_SIZE,
    }
}

fn buffered_body_limit_for_operation(op: &S3Operation) -> usize {
    match op {
        S3Operation::DeleteObjects { .. } => MAX_DELETE_OBJECTS_XML_BYTES,
        S3Operation::PutBucketVersioning { .. } => MAX_VERSIONING_CONFIGURATION_BYTES,
        S3Operation::PutBucketObjectLockConfiguration { .. } => MAX_OBJECT_LOCK_CONFIGURATION_BYTES,
        S3Operation::PutBucketEncryption { .. } => MAX_BUCKET_ENCRYPTION_CONFIGURATION_BYTES,
        S3Operation::PutBucketCors { .. } => MAX_CORS_CONFIGURATION_BYTES,
        S3Operation::PutBucketTagging { .. } | S3Operation::PutObjectTagging { .. } => {
            MAX_TAGGING_XML_BYTES
        }
        S3Operation::PutBucketAbac { .. } => MAX_BUCKET_ABAC_XML_BYTES,
        S3Operation::PutBucketLifecycle { .. } => MAX_LIFECYCLE_CONFIGURATION_BYTES,
        S3Operation::PutObjectAcl { .. } | S3Operation::PutBucketAcl { .. } => MAX_ACL_XML_BYTES,
        S3Operation::PutBucketPublicAccessBlock { .. } => {
            MAX_PUBLIC_ACCESS_BLOCK_CONFIGURATION_BYTES
        }
        S3Operation::PutBucketOwnershipControls { .. } => MAX_OWNERSHIP_CONTROLS_XML_BYTES,
        S3Operation::CompleteMultipartUpload { .. } => MAX_COMPLETE_MULTIPART_UPLOAD_XML_BYTES,
        _ => MAX_BUFFERED_CONTROL_BODY_SIZE,
    }
}

/// Parse aws-chunked mode from request headers.
///
/// Returns `ChunkedMode::None` for plain PUT bodies (including missing
/// `x-amz-content-sha256`, `UNSIGNED-PAYLOAD`, and fixed SHA256 hashes).
pub(super) fn parse_chunked_mode(req: &S3Request) -> Result<ChunkedMode, ServerError> {
    let Some(content_sha256) = req.header("x-amz-content-sha256") else {
        return Ok(ChunkedMode::None);
    };

    match content_sha256 {
        "UNSIGNED-PAYLOAD" => Ok(ChunkedMode::None),
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
        | "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
        | "STREAMING-UNSIGNED-PAYLOAD-TRAILER" => {
            // x-amz-decoded-content-length must be present and valid.
            let expected_len_str = req
                .header("x-amz-decoded-content-length")
                .ok_or(ServerError::MissingContentLength)?;
            let expected_len =
                expected_len_str
                    .parse::<u64>()
                    .map_err(|_| ServerError::InvalidRequest {
                        reason: format!("invalid x-amz-decoded-content-length: {expected_len_str}"),
                    })?;
            if expected_len > MAX_OBJECT_SIZE {
                return Err(ServerError::ObjectTooLarge {
                    size: expected_len,
                    max: MAX_OBJECT_SIZE,
                });
            }

            match content_sha256 {
                "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" => Ok(ChunkedMode::Signed { expected_len }),
                "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER" => {
                    Ok(ChunkedMode::SignedTrailer { expected_len })
                }
                "STREAMING-UNSIGNED-PAYLOAD-TRAILER" => {
                    Ok(ChunkedMode::UnsignedTrailer { expected_len })
                }
                _ => unreachable!(),
            }
        }
        // STREAMING-UNSIGNED-PAYLOAD (without -TRAILER) and unknown streaming
        // tokens are rejected by AWS.
        v if v.starts_with("STREAMING-") => Err(ServerError::UnsupportedStreamingToken {
            token: v.to_string(),
        }),
        // Fixed payload hash (hex SHA256): verify incrementally in streaming loop.
        _ => Ok(ChunkedMode::None),
    }
}

fn post_object_bucket(parts: &http::request::Parts) -> Option<BucketName> {
    route(
        parts.method.as_str(),
        parts.uri.path(),
        parts.uri.query().unwrap_or(""),
    )
    .ok()
    .and_then(|op| match op {
        S3Operation::PostObject { bucket } => Some(bucket),
        _ => None,
    })
}

#[derive(Debug)]
enum PostMultipartEvent {
    Field { name: String, value: String },
    FileStart { file_name: Option<String> },
    FileChunk(Bytes),
    FileEnd,
}

#[derive(Debug)]
enum PostMultipartState {
    Start,
    Headers,
    Data { name: String, is_file: bool },
    AfterBoundary,
    Done,
}

fn streaming_post_field_value_limit(name: &str) -> usize {
    let lower = name.to_ascii_lowercase();
    if lower.starts_with("x-amz-meta-") {
        return USER_METADATA_SIZE_LIMIT;
    }
    if ChecksumAlgorithm::from_header_name(&lower).is_some() {
        return MAX_STREAMING_POST_CHECKSUM_FIELD_BYTES;
    }

    match lower.as_str() {
        "key" => MAX_STREAMING_POST_KEY_FIELD_BYTES,
        "policy" => MAX_STREAMING_POST_POLICY_FIELD_BYTES,
        "tagging" => MAX_STREAMING_POST_TAGGING_FIELD_BYTES,
        "x-amz-signature" => MAX_STREAMING_POST_SIGNATURE_FIELD_BYTES,
        "x-amz-date" => MAX_STREAMING_POST_DATE_FIELD_BYTES,
        "x-amz-credential" => MAX_STREAMING_POST_CREDENTIAL_FIELD_BYTES,
        "x-amz-algorithm" => MAX_STREAMING_POST_ALGORITHM_FIELD_BYTES,
        "acl" => MAX_STREAMING_POST_ACL_FIELD_BYTES,
        "success_action_status" => MAX_STREAMING_POST_STATUS_FIELD_BYTES,
        "x-amz-server-side-encryption"
        | "x-amz-server-side-encryption-aws-kms-key-id"
        | "x-amz-server-side-encryption-customer-algorithm"
        | "x-amz-server-side-encryption-customer-key"
        | "x-amz-server-side-encryption-customer-key-md5" => MAX_STREAMING_POST_SSE_FIELD_BYTES,
        _ => MAX_STREAMING_POST_DEFAULT_FIELD_BYTES,
    }
}

fn streaming_post_field_too_large(name: &str, limit: usize) -> ServerError {
    ServerError::InvalidRequest {
        reason: format!("multipart form field '{name}' exceeds maximum size of {limit} bytes"),
    }
}

fn streaming_post_form_too_large(limit: usize) -> ServerError {
    ServerError::InvalidRequest {
        reason: format!("multipart form fields exceed maximum total size of {limit} bytes"),
    }
}

fn is_streaming_post_metadata_field(name: &str) -> bool {
    name.as_bytes()
        .get(..11)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"x-amz-meta-"))
}

#[derive(Default)]
struct StreamingPostFieldBudget {
    total_bytes: usize,
    metadata_bytes: usize,
}

impl StreamingPostFieldBudget {
    fn record(&mut self, name: &str, value: &str) -> Result<(), ServerError> {
        let field_bytes = name
            .len()
            .checked_add(value.len())
            .ok_or_else(|| streaming_post_form_too_large(MAX_STREAMING_POST_NON_FILE_FORM_BYTES))?;

        self.total_bytes = self
            .total_bytes
            .checked_add(field_bytes)
            .ok_or_else(|| streaming_post_form_too_large(MAX_STREAMING_POST_NON_FILE_FORM_BYTES))?;
        if self.total_bytes > MAX_STREAMING_POST_NON_FILE_FORM_BYTES {
            return Err(streaming_post_form_too_large(
                MAX_STREAMING_POST_NON_FILE_FORM_BYTES,
            ));
        }

        if is_streaming_post_metadata_field(name) {
            self.metadata_bytes = self.metadata_bytes.checked_add(field_bytes).ok_or(
                ServerError::MetadataTooLargeDetailed {
                    size: usize::MAX,
                    max_size_allowed: USER_METADATA_SIZE_LIMIT,
                },
            )?;
            if self.metadata_bytes > USER_METADATA_SIZE_LIMIT {
                return Err(ServerError::MetadataTooLargeDetailed {
                    size: self.metadata_bytes,
                    max_size_allowed: USER_METADATA_SIZE_LIMIT,
                });
            }
        }

        Ok(())
    }
}

struct PostMultipartParser {
    boundary: Vec<u8>,
    delimiter: Vec<u8>,
    buf: BytesMut,
    state: PostMultipartState,
}

impl PostMultipartParser {
    fn new(boundary: &str) -> Self {
        let boundary_bytes = format!("--{boundary}").into_bytes();
        let delimiter = format!("\r\n--{boundary}").into_bytes();
        Self {
            boundary: boundary_bytes,
            delimiter,
            buf: BytesMut::new(),
            state: PostMultipartState::Start,
        }
    }

    fn is_done(&self) -> bool {
        matches!(self.state, PostMultipartState::Done)
    }

    fn feed(&mut self, data: &[u8]) -> Result<Vec<PostMultipartEvent>, ServerError> {
        self.buf.extend_from_slice(data);
        let mut events = Vec::new();

        loop {
            match &mut self.state {
                PostMultipartState::Start => {
                    let Some(pos) = find_subslice(&self.buf, &self.boundary) else {
                        // Keep a small suffix to detect boundary across chunk splits.
                        if self.buf.len() > self.boundary.len() {
                            let drop_len = self.buf.len() - self.boundary.len();
                            let _ = self.buf.split_to(drop_len);
                        }
                        break;
                    };
                    if pos > 0 {
                        let _ = self.buf.split_to(pos);
                    }
                    if self.buf.len() < self.boundary.len() + 2 {
                        break;
                    }
                    let _ = self.buf.split_to(self.boundary.len());
                    if self.buf.starts_with(b"--") {
                        return Err(ServerError::InvalidRequest {
                            reason: "empty multipart form".to_string(),
                        });
                    }
                    if self.buf.starts_with(b"\r\n") {
                        let _ = self.buf.split_to(2);
                        self.state = PostMultipartState::Headers;
                        continue;
                    }
                    return Err(ServerError::InvalidRequest {
                        reason: "malformed multipart: boundary not followed by CRLF".to_string(),
                    });
                }
                PostMultipartState::Headers => {
                    let Some(end) = find_subslice(&self.buf, b"\r\n\r\n") else {
                        if self.buf.len().saturating_sub(3) > MAX_STREAMING_POST_PART_HEADER_BYTES {
                            return Err(ServerError::InvalidRequest {
                                reason: format!(
                                    "multipart part headers exceed maximum size of {} bytes",
                                    MAX_STREAMING_POST_PART_HEADER_BYTES
                                ),
                            });
                        }
                        break;
                    };
                    if end > MAX_STREAMING_POST_PART_HEADER_BYTES {
                        return Err(ServerError::InvalidRequest {
                            reason: format!(
                                "multipart part headers exceed maximum size of {} bytes",
                                MAX_STREAMING_POST_PART_HEADER_BYTES
                            ),
                        });
                    }
                    let header_block = self.buf.split_to(end + 4);
                    let header_bytes = &header_block[..end];
                    let header_str = std::str::from_utf8(header_bytes).map_err(|_| {
                        ServerError::InvalidRequest {
                            reason: "invalid UTF-8 in multipart headers".to_string(),
                        }
                    })?;
                    let (name, file_name) =
                        super::multipart::parse_content_disposition(header_str)?;
                    let is_file = name.eq_ignore_ascii_case("file");
                    if is_file {
                        events.push(PostMultipartEvent::FileStart { file_name });
                    }
                    self.state = PostMultipartState::Data { name, is_file };
                }
                PostMultipartState::Data { name, is_file } => {
                    if let Some(idx) = find_subslice(&self.buf, &self.delimiter) {
                        let content = self.buf.split_to(idx).freeze();
                        if *is_file {
                            if !content.is_empty() {
                                events.push(PostMultipartEvent::FileChunk(content));
                            }
                            events.push(PostMultipartEvent::FileEnd);
                        } else {
                            let limit = streaming_post_field_value_limit(name);
                            if content.len() > limit {
                                return Err(streaming_post_field_too_large(name, limit));
                            }
                            let value = std::str::from_utf8(&content).map_err(|_| {
                                ServerError::InvalidRequest {
                                    reason: format!("invalid UTF-8 in form field '{name}'"),
                                }
                            })?;
                            events.push(PostMultipartEvent::Field {
                                name: name.clone(),
                                value: value.to_string(),
                            });
                        }
                        let _ = self.buf.split_to(self.delimiter.len());
                        self.state = PostMultipartState::AfterBoundary;
                        continue;
                    }

                    if *is_file {
                        let keep = self.delimiter.len().saturating_sub(1);
                        if self.buf.len() > keep {
                            let flush_len = self.buf.len() - keep;
                            let chunk = self.buf.split_to(flush_len).freeze();
                            if !chunk.is_empty() {
                                events.push(PostMultipartEvent::FileChunk(chunk));
                            }
                            continue;
                        }
                    }
                    let limit = streaming_post_field_value_limit(name);
                    let buffered_value_len = self
                        .buf
                        .len()
                        .saturating_sub(self.delimiter.len().saturating_sub(1));
                    if buffered_value_len > limit {
                        return Err(streaming_post_field_too_large(name, limit));
                    }
                    break;
                }
                PostMultipartState::AfterBoundary => {
                    if self.buf.len() < 2 {
                        break;
                    }
                    if self.buf.starts_with(b"--") {
                        let _ = self.buf.split_to(2);
                        self.state = PostMultipartState::Done;
                        continue;
                    }
                    if self.buf.starts_with(b"\r\n") {
                        let _ = self.buf.split_to(2);
                        self.state = PostMultipartState::Headers;
                        continue;
                    }
                    return Err(ServerError::InvalidRequest {
                        reason: "malformed multipart: bad boundary terminator".to_string(),
                    });
                }
                PostMultipartState::Done => break,
            }
        }

        Ok(events)
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

#[doc(hidden)]
pub fn fuzz_post_multipart_parser(
    boundary: &str,
    body: &[u8],
    feed_controls: &[u8],
) -> Result<(), ServerError> {
    let mut parser = PostMultipartParser::new(boundary);
    let mut field_budget = StreamingPostFieldBudget::default();
    let mut seen_file = false;
    let mut file_ended = false;
    let mut offset = 0;

    let mut handle_events = |events: Vec<PostMultipartEvent>| -> Result<(), ServerError> {
        for event in events {
            match event {
                PostMultipartEvent::Field { name, value } => {
                    if seen_file {
                        return Err(ServerError::InvalidRequest {
                            reason: "file field must be the final multipart part".to_string(),
                        });
                    }
                    field_budget.record(&name, &value)?;
                }
                PostMultipartEvent::FileStart { .. } => {
                    if seen_file {
                        return Err(ServerError::InvalidRequest {
                            reason: "multiple file fields are not supported".to_string(),
                        });
                    }
                    seen_file = true;
                }
                PostMultipartEvent::FileChunk(_) => {
                    if !seen_file || file_ended {
                        return Err(ServerError::InvalidRequest {
                            reason: "missing file field in multipart form".to_string(),
                        });
                    }
                }
                PostMultipartEvent::FileEnd => {
                    if !seen_file || file_ended {
                        return Err(ServerError::InvalidRequest {
                            reason: "missing file field in multipart form".to_string(),
                        });
                    }
                    file_ended = true;
                }
            }
        }
        Ok(())
    };

    for &control in feed_controls {
        if offset >= body.len() {
            break;
        }
        let remaining = body.len() - offset;
        let len = 1 + usize::from(control) % remaining;
        let end = offset + len;
        let events = parser.feed(&body[offset..end])?;
        handle_events(events)?;
        offset = end;
    }

    if offset < body.len() || body.is_empty() {
        let events = parser.feed(&body[offset..])?;
        handle_events(events)?;
    }

    if !parser.is_done() {
        return Err(ServerError::IncompleteBody);
    }
    if !seen_file {
        return Err(ServerError::InvalidRequest {
            reason: "missing file field in multipart form".to_string(),
        });
    }
    if !file_ended {
        return Err(ServerError::IncompleteBody);
    }

    Ok(())
}

#[doc(hidden)]
pub fn fuzz_request_parser_entrypoints(
    copy_source: &str,
    post_key: &str,
    file_name: &str,
    multipart_query: &str,
    list_multipart_query: &str,
    upload_part_query: &str,
) {
    let _ = crate::http::parse_copy_source_header(copy_source);
    crate::http::fuzz_upload_id_query_entrypoints(
        multipart_query,
        list_multipart_query,
        upload_part_query,
    );

    let form = crate::http::multipart::PostFormData {
        fields: vec![("key".to_string(), post_key.to_string())],
        file_data: Vec::new(),
        file_name: (!file_name.is_empty()).then(|| file_name.to_string()),
    };
    let _ = form.resolve_key();
}

#[doc(hidden)]
pub struct XmlFuzzInputs<'a> {
    pub complete_multipart_upload_xml: &'a [u8],
    pub delete_objects_xml: &'a [u8],
    pub bucket_lifecycle_xml: &'a [u8],
    pub bucket_cors_xml: &'a [u8],
    pub bucket_acl_xml: &'a [u8],
    pub bucket_object_lock_configuration_xml: &'a [u8],
    pub object_retention_xml: &'a [u8],
    pub object_legal_hold_xml: &'a [u8],
    pub bucket_versioning_xml: &'a [u8],
    pub bucket_encryption_xml: &'a [u8],
    pub bucket_tagging_xml: &'a [u8],
    pub object_tagging_xml: &'a [u8],
    pub public_access_block_xml: &'a [u8],
    pub ownership_controls_xml: &'a [u8],
}

#[doc(hidden)]
pub fn fuzz_xml_parser_entrypoints(inputs: XmlFuzzInputs<'_>) {
    fn clamp(data: &[u8], max_len: usize) -> &[u8] {
        &data[..data.len().min(max_len)]
    }

    const MAX_COMPLETE_MULTIPART_UPLOAD_XML_BYTES: usize = 2_621_440;
    const MAX_DELETE_OBJECTS_XML_BYTES: usize = 2_048_000;
    const MAX_BUCKET_LIFECYCLE_XML_BYTES: usize = 2 * 1024 * 1024;
    const MAX_BUCKET_CORS_XML_BYTES: usize = 64 * 1024;
    const MAX_BUCKET_OBJECT_LOCK_CONFIGURATION_XML_BYTES: usize = 2 * 1024 * 1024;
    const MAX_BUCKET_VERSIONING_XML_BYTES: usize = 1024;
    const MAX_TAGGING_XML_BYTES: usize = 160 * 1024;
    const MAX_PUBLIC_ACCESS_BLOCK_XML_BYTES: usize = 2 * 1024 * 1024;
    const MAX_OWNERSHIP_CONTROLS_XML_BYTES: usize = 2048;

    // These parsers do not currently enforce a request-size bound themselves.
    const MAX_ACL_XML_FUZZ_BYTES: usize = 64 * 1024;
    const MAX_OBJECT_RETENTION_XML_FUZZ_BYTES: usize = 64 * 1024;
    const MAX_OBJECT_LEGAL_HOLD_XML_FUZZ_BYTES: usize = 64 * 1024;
    const MAX_BUCKET_ENCRYPTION_XML_FUZZ_BYTES: usize = 64 * 1024;

    let _ = crate::http::xml::parse_complete_multipart_upload_xml(clamp(
        inputs.complete_multipart_upload_xml,
        MAX_COMPLETE_MULTIPART_UPLOAD_XML_BYTES,
    ));
    let _ = crate::http::xml::parse_delete_objects_xml(clamp(
        inputs.delete_objects_xml,
        MAX_DELETE_OBJECTS_XML_BYTES,
    ));
    let _ = crate::http::xml::parse_bucket_lifecycle_configuration_xml(clamp(
        inputs.bucket_lifecycle_xml,
        MAX_BUCKET_LIFECYCLE_XML_BYTES,
    ));
    let _ = crate::http::xml::parse_cors_config_xml(clamp(
        inputs.bucket_cors_xml,
        MAX_BUCKET_CORS_XML_BYTES,
    ));
    let _ = crate::http::xml::parse_acl_xml(clamp(inputs.bucket_acl_xml, MAX_ACL_XML_FUZZ_BYTES));
    let _ = crate::http::xml::parse_bucket_object_lock_configuration_xml(clamp(
        inputs.bucket_object_lock_configuration_xml,
        MAX_BUCKET_OBJECT_LOCK_CONFIGURATION_XML_BYTES,
    ));
    let _ = crate::http::xml::parse_object_retention_xml(clamp(
        inputs.object_retention_xml,
        MAX_OBJECT_RETENTION_XML_FUZZ_BYTES,
    ));
    let _ = crate::http::xml::parse_object_legal_hold_xml(clamp(
        inputs.object_legal_hold_xml,
        MAX_OBJECT_LEGAL_HOLD_XML_FUZZ_BYTES,
    ));
    let _ = crate::http::xml::parse_versioning_config_xml(clamp(
        inputs.bucket_versioning_xml,
        MAX_BUCKET_VERSIONING_XML_BYTES,
    ));
    let _ = crate::http::xml::parse_bucket_encryption_xml(clamp(
        inputs.bucket_encryption_xml,
        MAX_BUCKET_ENCRYPTION_XML_FUZZ_BYTES,
    ));
    let _ = crate::http::xml::parse_tagging_xml(
        clamp(inputs.bucket_tagging_xml, MAX_TAGGING_XML_BYTES),
        50,
    );
    let _ = crate::http::xml::parse_tagging_xml(
        clamp(inputs.object_tagging_xml, MAX_TAGGING_XML_BYTES),
        10,
    );
    let _ = crate::http::xml::parse_public_access_block_xml(clamp(
        inputs.public_access_block_xml,
        MAX_PUBLIC_ACCESS_BLOCK_XML_BYTES,
    ));
    let _ = crate::http::xml::parse_ownership_controls_xml(clamp(
        inputs.ownership_controls_xml,
        MAX_OWNERSHIP_CONTROLS_XML_BYTES,
    ));
}

#[doc(hidden)]
pub struct ConditionalFuzzInputs<'a> {
    pub if_match: &'a str,
    pub if_none_match: &'a str,
    pub if_modified_since: &'a str,
    pub if_unmodified_since: &'a str,
    pub copy_source_if_match: &'a str,
    pub copy_source_if_none_match: &'a str,
    pub copy_source_if_modified_since: &'a str,
    pub copy_source_if_unmodified_since: &'a str,
    pub delete_if_match_last_modified_time: &'a str,
    pub delete_if_match_size: &'a str,
    pub amz_date: &'a str,
}

#[doc(hidden)]
pub fn fuzz_conditional_header_entrypoints(inputs: ConditionalFuzzInputs<'_>) {
    let _ = crate::http::response::parse_http_date(inputs.if_modified_since);
    let _ = crate::http::response::parse_http_date(inputs.if_unmodified_since);
    let _ = crate::http::response::parse_http_date(inputs.copy_source_if_modified_since);
    let _ = crate::http::response::parse_http_date(inputs.copy_source_if_unmodified_since);

    let mut request = http::Request::new(());
    *request.method_mut() = http::Method::GET;
    *request.uri_mut() = "/".parse().expect("static URI is valid");

    let header_values = [
        ("if-match", inputs.if_match),
        ("if-none-match", inputs.if_none_match),
        ("if-modified-since", inputs.if_modified_since),
        ("if-unmodified-since", inputs.if_unmodified_since),
        ("x-amz-copy-source-if-match", inputs.copy_source_if_match),
        (
            "x-amz-copy-source-if-none-match",
            inputs.copy_source_if_none_match,
        ),
        (
            "x-amz-copy-source-if-modified-since",
            inputs.copy_source_if_modified_since,
        ),
        (
            "x-amz-copy-source-if-unmodified-since",
            inputs.copy_source_if_unmodified_since,
        ),
        (
            "x-amz-if-match-last-modified-time",
            inputs.delete_if_match_last_modified_time,
        ),
        ("x-amz-if-match-size", inputs.delete_if_match_size),
        ("x-amz-date", inputs.amz_date),
        ("authorization", "AWS4-HMAC-SHA256 fuzz"),
    ];

    for (name, value) in header_values {
        if value.is_empty() {
            continue;
        }
        let Ok(value) = http::HeaderValue::from_str(value) else {
            continue;
        };
        request.headers_mut().append(name, value);
    }

    let Ok(req) = crate::http::request::S3Request::from_hyper_headers(
        request.into_parts().0,
        crate::http::request::TransportSecurity::Tls,
        storage::clock::current_time_millis() / 1_000,
    ) else {
        return;
    };

    let _ = crate::http::conditional::read_condition_from_headers(&req);
    let _ = crate::http::conditional::write_condition_from_headers(&req);
    let _ = crate::http::conditional::delete_condition_from_headers(&req);
    let _ = crate::http::conditional::copy_source_condition_from_headers(&req);
}

#[doc(hidden)]
pub fn fuzz_streaming_request_entrypoints(
    method: &str,
    uri: &str,
    headers: &[(String, String)],
    transport_security: TransportSecurity,
) {
    fn build_parts(
        method: &str,
        uri: &str,
        headers: &[(String, String)],
    ) -> Option<http::request::Parts> {
        let method = http::Method::from_bytes(method.as_bytes()).ok()?;
        let uri = uri.parse::<http::Uri>().ok()?;

        let mut request = http::Request::new(());
        *request.method_mut() = method;
        *request.uri_mut() = uri;

        for (name, value) in headers {
            let Ok(name) = http::header::HeaderName::from_bytes(name.as_bytes()) else {
                continue;
            };
            let Ok(value) = http::HeaderValue::from_str(value) else {
                continue;
            };
            request.headers_mut().append(name, value);
        }

        Some(request.into_parts().0)
    }

    if let Some(parts) = build_parts(method, uri, headers) {
        let _ = is_streaming_write(&parts);
        let _ = post_object_bucket(&parts);
    }

    if let Some(parts) = build_parts(method, uri, headers) {
        let request_epoch_seconds = storage::clock::current_time_millis() / 1_000;
        let Ok(req) =
            S3Request::from_hyper_headers(parts, transport_security, request_epoch_seconds)
        else {
            return;
        };

        let _ = parse_chunked_mode(&req);
        let _ = claimed_payload_sha256_from_request(&req);
        let _ = trailing_hasher_from_request(&req);
        let _ = inline_checksum_hasher_from_request(&req);
        let _ = req
            .header("content-type")
            .and_then(super::multipart::extract_boundary);
    }
}

fn route_bounded_body_frame_timeout(
    admission: Option<&storage::StorageClusterRouteAdmission>,
    idle_timeout: Duration,
) -> Result<Duration, ServerError> {
    let Some(admission) = admission else {
        return Ok(idle_timeout);
    };
    let remaining = admission
        .remaining_validity()
        .map_err(|_| ServerError::SlowDown)?;
    Ok(remaining.map_or(idle_timeout, |remaining| idle_timeout.min(remaining)))
}

enum RouteBoundedBodyWait<T> {
    Ready(T),
    TimedOut,
    RouteInvalid(ServerError),
}

async fn wait_for_route_bounded_body_frame<F>(
    admission: Option<&storage::StorageClusterRouteAdmission>,
    idle_timeout: Duration,
    on_wait_started: impl FnOnce(),
    frame: F,
) -> RouteBoundedBodyWait<F::Output>
where
    F: std::future::Future,
{
    let initial_timeout = match route_bounded_body_frame_timeout(admission, idle_timeout) {
        Ok(timeout) => timeout,
        Err(error) => return RouteBoundedBodyWait::RouteInvalid(error),
    };
    tokio::pin!(frame);
    let initial_sleep = tokio::time::sleep(initial_timeout);
    tokio::pin!(initial_sleep);

    let Some(admission) = admission else {
        on_wait_started();
        return tokio::select! {
            output = &mut frame => RouteBoundedBodyWait::Ready(output),
            () = &mut initial_sleep => RouteBoundedBodyWait::TimedOut,
        };
    };
    let publication_pending = admission.wait_for_route_publication_pending();
    tokio::pin!(publication_pending);
    on_wait_started();
    tokio::select! {
        output = &mut frame => RouteBoundedBodyWait::Ready(output),
        () = &mut initial_sleep => RouteBoundedBodyWait::TimedOut,
        () = &mut publication_pending => {
            let shortened_timeout = match route_bounded_body_frame_timeout(
                Some(admission),
                idle_timeout,
            ) {
                Ok(timeout) => timeout,
                Err(error) => return RouteBoundedBodyWait::RouteInvalid(error),
            };
            tokio::select! {
                output = &mut frame => RouteBoundedBodyWait::Ready(output),
                () = tokio::time::sleep(shortened_timeout) => RouteBoundedBodyWait::TimedOut,
            }
        }
    }
}

fn pre_auth_body_deadline(timeout: Duration) -> TokioInstant {
    let now = TokioInstant::now();
    now.checked_add(timeout).unwrap_or(now)
}

fn pre_auth_body_frame_deadline(
    idle_timeout: Duration,
    absolute_deadline: TokioInstant,
) -> Result<TokioInstant, ServerError> {
    let now = TokioInstant::now();
    if now >= absolute_deadline {
        return Err(ServerError::InvalidRequest {
            reason: "request body authentication deadline expired".to_string(),
        });
    }
    Ok(now
        .checked_add(idle_timeout)
        .unwrap_or(absolute_deadline)
        .min(absolute_deadline))
}

fn pre_auth_body_timeout_error(deadline: TokioInstant) -> ServerError {
    ServerError::InvalidRequest {
        reason: if TokioInstant::now() >= deadline {
            "request body authentication deadline expired"
        } else {
            "request body read timed out"
        }
        .to_string(),
    }
}

async fn await_pre_auth_body_frame<F, T>(
    frame: F,
    idle_timeout: Duration,
    absolute_deadline: TokioInstant,
) -> Result<T, ServerError>
where
    F: std::future::Future<Output = T>,
{
    let frame_deadline = pre_auth_body_frame_deadline(idle_timeout, absolute_deadline)?;
    let timer = tokio::time::sleep_until(frame_deadline);
    tokio::pin!(timer);
    let result = tokio::select! {
        biased;
        _ = &mut timer => return Err(pre_auth_body_timeout_error(absolute_deadline)),
        result = frame => result,
    };

    // Also cover scheduler preemption after the timer poll but before the
    // ready frame is returned to the caller.
    if TokioInstant::now() >= absolute_deadline {
        return Err(pre_auth_body_timeout_error(absolute_deadline));
    }
    Ok(result)
}

fn route_bounded_body_timeout_error(
    admission: Option<&storage::StorageClusterRouteAdmission>,
) -> ServerError {
    if admission.is_some_and(|admission| admission.require_valid_now().is_err()) {
        ServerError::SlowDown
    } else {
        ServerError::InvalidRequest {
            reason: "request body read timed out".to_string(),
        }
    }
}

async fn handle_streaming_post_object(
    state: Arc<ServerState>,
    s3req: S3Request,
    body: &mut TrackedIncoming,
    bucket: BucketName,
    trace: observability::TraceContext,
    wire_ids: WireResponseIds,
) -> S3Response {
    let idle_timeout = state.config.body_idle_timeout;
    let pre_auth_deadline = pre_auth_body_deadline(state.config.pre_auth_body_timeout);

    let req_arc = Arc::new(s3req);

    let content_type = match req_arc.header("content-type") {
        Some(v) => v,
        None => {
            // AWS returns 412 for missing/wrong Content-Type on POST Object.
            return error_response(
                &ServerError::PreconditionFailed {
                    condition: "Bucket POST must be of the enclosure-type multipart/form-data",
                },
                &wire_ids,
            );
        }
    };
    // Check if this is actually multipart/form-data before looking for boundary.
    // AWS returns 412 for wrong content-type, 400 for missing boundary.
    let is_multipart = content_type
        .split(';')
        .next()
        .is_some_and(|t| t.trim().eq_ignore_ascii_case("multipart/form-data"));
    if !is_multipart {
        return error_response(
            &ServerError::PreconditionFailed {
                condition: "Bucket POST must be of the enclosure-type multipart/form-data",
            },
            &wire_ids,
        );
    }
    let boundary = match super::multipart::extract_boundary(content_type) {
        Some(b) => b,
        None => {
            return error_response(
                &ServerError::MalformedPOSTRequest {
                    reason: "The body of your POST request is not well-formed multipart/form-data."
                        .to_string(),
                },
                &wire_ids,
            );
        }
    };

    let mut parser = PostMultipartParser::new(boundary);
    let mut fields: Vec<(String, String)> = Vec::new();
    let mut field_budget = StreamingPostFieldBudget::default();
    let mut ctx: Option<Arc<super::StreamingPostContext>> = None;
    let abort_guard = StreamingAbortGuard::new(&state);
    let mut seen_file = false;
    let mut file_ended = false;

    let mut crc64 = checksum::crc64::Hasher::new();
    let mut post_checksum_hasher: Option<checksum::ChecksumHasher> = None;
    let mut total_size: u64 = 0;
    let mut segment_index: u32 = 0;
    let mut upload_buf = PooledSegmentBuffer::new(&state);

    loop {
        let next_frame: Result<_, ServerError> = match ctx.as_ref() {
            Some(ctx) => match wait_for_route_bounded_body_frame(
                Some(ctx.storage_route_admission()),
                idle_timeout,
                || {
                    #[cfg(test)]
                    if let Some(hook) = state.config.route_bounded_body_wait_hook.as_ref() {
                        hook(RouteBoundedBodyOperation::PostObject);
                    }
                },
                body.frame(),
            )
            .await
            {
                RouteBoundedBodyWait::Ready(frame) => Ok(frame),
                RouteBoundedBodyWait::TimedOut => Err(route_bounded_body_timeout_error(Some(
                    ctx.storage_route_admission(),
                ))),
                RouteBoundedBodyWait::RouteInvalid(error) => Err(error),
            },
            None => await_pre_auth_body_frame(body.frame(), idle_timeout, pre_auth_deadline).await,
        };
        match next_frame {
            Ok(Some(Ok(frame))) => {
                if let Some(chunk) = frame.data_ref() {
                    let events = match parser.feed(chunk) {
                        Ok(v) => v,
                        Err(err) => {
                            if let Some(ref c) = ctx {
                                abort_streaming_post_object(&state, c).await;
                            }
                            return error_response(&err, &wire_ids);
                        }
                    };
                    for event in events {
                        match event {
                            PostMultipartEvent::Field { name, value } => {
                                if seen_file {
                                    if let Some(ref c) = ctx {
                                        abort_streaming_post_object(&state, c).await;
                                    }
                                    return error_response(
                                        &ServerError::InvalidRequest {
                                            reason: "file field must be the final multipart part"
                                                .to_string(),
                                        },
                                        &wire_ids,
                                    );
                                }
                                if let Err(err) = field_budget.record(&name, &value) {
                                    if let Some(ref c) = ctx {
                                        abort_streaming_post_object(&state, c).await;
                                    }
                                    return error_response(&err, &wire_ids);
                                }
                                fields.push((name, value));
                            }
                            PostMultipartEvent::FileStart { file_name } => {
                                if seen_file {
                                    if let Some(ref c) = ctx {
                                        abort_streaming_post_object(&state, c).await;
                                    }
                                    return error_response(
                                        &ServerError::InvalidRequest {
                                            reason: "multiple file fields are not supported"
                                                .to_string(),
                                        },
                                        &wire_ids,
                                    );
                                }
                                seen_file = true;
                                let st = Arc::clone(&state);
                                let req = Arc::clone(&req_arc);
                                let bucket_clone = bucket.clone();
                                let fields_clone = fields.clone();
                                let abort_guard_for_worker = Arc::clone(&abort_guard);
                                let ctx_res = spawn_blocking_with_trace(trace.clone(), move || {
                                    let frontend = acquire_frontend(&st);
                                    let result = frontend.prepare_streaming_post_object(
                                        &req,
                                        bucket_clone.as_str(),
                                        &fields_clone,
                                        file_name.as_deref(),
                                    );
                                    result.map(|ctx| {
                                        let ctx = Arc::new(ctx);
                                        abort_guard_for_worker.arm_post(&ctx);
                                        ctx
                                    })
                                })
                                .await;
                                match ctx_res {
                                    Ok(Ok(c)) => {
                                        post_checksum_hasher = c.checksum.as_ref().map(|claim| {
                                            checksum::ChecksumHasher::new(claim.algorithm())
                                        });
                                        ctx = Some(c);
                                    }
                                    Ok(Err(err)) => {
                                        return finish_streaming_post_rejection(
                                            error_response(&err, &wire_ids),
                                            body,
                                            idle_timeout,
                                        )
                                        .await;
                                    }
                                    Err(_) => return internal_error_response(&wire_ids),
                                }
                            }
                            PostMultipartEvent::FileChunk(data) => {
                                let Some(ref c) = ctx else {
                                    return error_response(
                                        &ServerError::InvalidRequest {
                                            reason: "missing file field in multipart form"
                                                .to_string(),
                                        },
                                        &wire_ids,
                                    );
                                };
                                crc64.update(&data);
                                if let Some(hasher) = post_checksum_hasher.as_mut() {
                                    hasher.update(&data);
                                }
                                total_size += data.len() as u64;
                                if total_size > MAX_OBJECT_SIZE {
                                    abort_streaming_post_object(&state, c).await;
                                    return finish_streaming_post_rejection(
                                        error_response(
                                            &ServerError::ObjectTooLarge {
                                                size: total_size,
                                                max: MAX_OBJECT_SIZE,
                                            },
                                            &wire_ids,
                                        ),
                                        body,
                                        idle_timeout,
                                    )
                                    .await;
                                }
                                if c.post_policy
                                    .as_ref()
                                    .and_then(auth::PreparedPostPolicy::max_content_length)
                                    .is_some_and(|max| total_size > max)
                                {
                                    abort_streaming_post_object(&state, c).await;
                                    return finish_streaming_post_rejection(
                                        error_response(
                                            &ServerError::InvalidRequest {
                                                reason: auth::PostPolicyError::ConditionFailed {
                                                    condition: "content-length-range",
                                                    field: None,
                                                }
                                                .to_string(),
                                            },
                                            &wire_ids,
                                        ),
                                        body,
                                        idle_timeout,
                                    )
                                    .await;
                                }

                                let mut remaining: &[u8] = data.as_ref();
                                while !remaining.is_empty() {
                                    let needed = crate::coordinator::INTERNAL_SEGMENT_SIZE
                                        - upload_buf.len();
                                    let take = needed.min(remaining.len());
                                    upload_buf.extend_from_slice(&remaining[..take]);
                                    remaining = &remaining[take..];
                                    if upload_buf.len() < crate::coordinator::INTERNAL_SEGMENT_SIZE
                                    {
                                        continue;
                                    }

                                    let mut flush_data = PooledSegmentBuffer::new(&state);
                                    std::mem::swap(&mut upload_buf, &mut flush_data);
                                    let idx = segment_index;
                                    segment_index += 1;
                                    let ctx_ref = Arc::clone(c);
                                    let st = Arc::clone(&state);
                                    let abort_guard_for_append = Arc::clone(&abort_guard);
                                    match tokio::task::spawn_blocking(move || {
                                        let _abort_guard = abort_guard_for_append;
                                        let frontend = acquire_frontend(&st);
                                        let result = frontend.streaming_append_post_segment(
                                            &ctx_ref,
                                            idx,
                                            &flush_data,
                                        );
                                        (result, flush_data)
                                    })
                                    .await
                                    {
                                        Ok((Ok(()), _flush_data)) => {}
                                        Ok((Err(err), _flush_data)) => {
                                            abort_streaming_post_object(&state, c).await;
                                            return error_response(&err, &wire_ids);
                                        }
                                        Err(_) => {
                                            abort_streaming_post_object(&state, c).await;
                                            return internal_error_response(&wire_ids);
                                        }
                                    }
                                }
                            }
                            PostMultipartEvent::FileEnd => {
                                file_ended = true;
                            }
                        }
                    }
                }
            }
            Ok(Some(Err(_))) => {
                if let Some(ref c) = ctx {
                    abort_streaming_post_object(&state, c).await;
                }
                return error_response(
                    &ServerError::InvalidRequest {
                        reason: "failed to read request body".to_string(),
                    },
                    &wire_ids,
                );
            }
            Ok(None) => break,
            Err(error) => {
                if let Some(ref c) = ctx {
                    abort_streaming_post_object(&state, c).await;
                }
                return error_response(&error, &wire_ids);
            }
        }
    }

    if !parser.is_done() {
        if let Some(ref c) = ctx {
            abort_streaming_post_object(&state, c).await;
        }
        return error_response(&ServerError::IncompleteBody, &wire_ids);
    }
    if !seen_file {
        return error_response(
            &ServerError::InvalidRequest {
                reason: "missing file field in multipart form".to_string(),
            },
            &wire_ids,
        );
    }
    if !file_ended {
        if let Some(ref c) = ctx {
            abort_streaming_post_object(&state, c).await;
        }
        return error_response(&ServerError::IncompleteBody, &wire_ids);
    }

    let Some(ctx) = ctx else {
        return error_response(
            &ServerError::InvalidRequest {
                reason: "missing file field in multipart form".to_string(),
            },
            &wire_ids,
        );
    };

    if !upload_buf.is_empty() {
        let idx = segment_index;
        let st = Arc::clone(&state);
        let ctx_ref = Arc::clone(&ctx);
        let abort_guard_for_append = Arc::clone(&abort_guard);
        match tokio::task::spawn_blocking(move || {
            let _abort_guard = abort_guard_for_append;
            let frontend = acquire_frontend(&st);
            let result = frontend.streaming_append_post_segment(&ctx_ref, idx, &upload_buf);
            (result, upload_buf)
        })
        .await
        {
            Ok((Ok(()), _upload_buf)) => {}
            Ok((Err(err), _upload_buf)) => {
                abort_streaming_post_object(&state, &ctx).await;
                return error_response(&err, &wire_ids);
            }
            Err(_) => {
                abort_streaming_post_object(&state, &ctx).await;
                return internal_error_response(&wire_ids);
            }
        }
    }

    let actual_checksum = post_checksum_hasher.map(checksum::ChecksumHasher::finalize);
    let crc64 = crc64.finalize();
    let st = Arc::clone(&state);
    let ctx_ref = Arc::clone(&ctx);
    let abort_guard_for_finalize = Arc::clone(&abort_guard);
    match tokio::task::spawn_blocking(move || {
        let frontend = acquire_frontend(&st);
        let result = frontend.finalize_streaming_post_object(
            &ctx_ref,
            crc64,
            total_size,
            actual_checksum.as_ref(),
        );
        if result.is_ok() {
            abort_guard_for_finalize.disarm();
        }
        result
    })
    .await
    {
        Ok(Ok(resp)) => resp,
        Ok(Err(err)) => {
            abort_streaming_post_object(&state, &ctx).await;
            error_response(&err, &wire_ids)
        }
        Err(_) => {
            abort_streaming_post_object(&state, &ctx).await;
            internal_error_response(&wire_ids)
        }
    }
}

#[derive(Clone, Copy)]
enum PresignedStreamingOperation {
    PutObject,
    UploadPart,
}

async fn presigned_streaming_body_error(
    body: &mut TrackedIncoming,
    chunked: &ChunkedMode,
    operation: PresignedStreamingOperation,
    idle_timeout: Duration,
) -> ServerError {
    let Some(expected) = chunked.expected_len() else {
        return ServerError::InternalError {
            reason: "presigned streaming validation called for a plain body".to_string(),
        };
    };
    let client_hash = match chunked {
        ChunkedMode::Signed { .. } => "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
        ChunkedMode::SignedTrailer { .. } => "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
        ChunkedMode::UnsignedTrailer { .. } => "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        ChunkedMode::None => unreachable!("plain bodies returned above"),
    };

    let mut provided = 0u64;
    let mut hasher = checksum::sha256::Sha256::new();
    loop {
        match tokio::time::timeout(idle_timeout, body.frame()).await {
            Ok(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    let Some(new_total) = provided.checked_add(data.len() as u64) else {
                        return ServerError::ObjectTooLarge {
                            size: u64::MAX,
                            max: MAX_OBJECT_SIZE,
                        };
                    };
                    provided = new_total;
                    if provided > MAX_OBJECT_SIZE {
                        return ServerError::ObjectTooLarge {
                            size: provided,
                            max: MAX_OBJECT_SIZE,
                        };
                    }
                    hasher.update(data);
                }
            }
            Ok(Some(Err(_))) => {
                return ServerError::InvalidRequest {
                    reason: "failed to read request body".to_string(),
                };
            }
            Ok(None) => break,
            Err(_) => {
                return ServerError::InvalidRequest {
                    reason: "request body read timed out".to_string(),
                };
            }
        }
    }

    if provided < expected {
        return ServerError::PresignedStreamingIncompleteBody { expected, provided };
    }

    if provided == expected
        && !(matches!(operation, PresignedStreamingOperation::UploadPart)
            && chunked.is_trailer_mode())
    {
        return ServerError::PresignedStreamingContentSHA256Mismatch {
            client_hash: client_hash.to_string(),
            server_hash: sha256_hex_from_digest(&hasher.finalize()),
        };
    }

    match chunked {
        ChunkedMode::Signed { .. } => {
            ServerError::PresignedStreamingIncompleteBody { expected, provided }
        }
        ChunkedMode::SignedTrailer { .. } | ChunkedMode::UnsignedTrailer { .. } => {
            ServerError::MalformedTrailerError {
                reason: match operation {
                    PresignedStreamingOperation::PutObject => {
                        "presigned PutObject trailer body exceeds the declared decoded length"
                    }
                    PresignedStreamingOperation::UploadPart => {
                        "presigned UploadPart does not accept this trailer body shape"
                    }
                }
                .to_string(),
            }
        }
        ChunkedMode::None => unreachable!("plain bodies returned above"),
    }
}

/// Handle a streaming `PutObject`: read body frame-by-frame, feed chunks to
/// coordinator append API, finalize atomically.
///
/// Each chunk append is dispatched via `spawn_blocking` with a brief frontend
/// lock. Between appends, no frontend is held — body reading is async.
#[allow(clippy::too_many_arguments)]
async fn handle_streaming_put(
    state: Arc<ServerState>,
    s3req: S3Request,
    body: &mut TrackedIncoming,
    bucket: BucketName,
    key: String,
    chunked: ChunkedMode,
    trace: observability::TraceContext,
    wire_ids: WireResponseIds,
) -> S3Response {
    let idle_timeout = state.config.body_idle_timeout;
    let state2 = Arc::clone(&state);
    let bucket_clone = bucket.clone();
    let key_clone = key.clone();
    let uses_aws_chunked_transport = !matches!(chunked, ChunkedMode::None);
    let declared_trailer = s3req.header("x-amz-trailer").map(str::to_string);
    let claimed_payload_sha256 = claimed_payload_sha256_from_request(&s3req);
    let mut trailing_hasher = trailing_hasher_from_request(&s3req);
    let mut inline_checksum_claim: Option<String> = None;
    if trailing_hasher.is_none() {
        if let Some((h, claimed)) = inline_checksum_hasher_from_request(&s3req) {
            trailing_hasher = Some(h);
            inline_checksum_claim = Some(claimed);
        }
    }
    let has_auth_attempt = request_has_auth_attempt(&s3req);
    let ctx = match spawn_blocking_with_trace(trace, move || {
        let frontend = acquire_frontend(&state2);
        frontend.prepare_streaming_put(
            &s3req,
            bucket_clone.as_str(),
            &key_clone,
            uses_aws_chunked_transport,
        )
    })
    .await
    {
        Ok(Ok(ctx)) => ctx,
        Ok(Err(err)) => {
            return finish_streaming_prepare_failure(
                error_response(&err, &wire_ids),
                &err,
                has_auth_attempt,
                body,
                idle_timeout,
            )
            .await
        }
        Err(_) => {
            let err = internal_error();
            return finish_streaming_prepare_failure(
                internal_error_response(&wire_ids),
                &err,
                has_auth_attempt,
                body,
                idle_timeout,
            )
            .await;
        }
    };
    if ctx.auth_mode == auth::AuthMode::PresignedSigV4 && chunked != ChunkedMode::None {
        let err = presigned_streaming_body_error(
            body,
            &chunked,
            PresignedStreamingOperation::PutObject,
            idle_timeout,
        )
        .await;
        return error_response(&err, &wire_ids);
    }
    // Build chunked decoder if needed.
    let mut decoder = match make_chunked_decoder(&chunked, ctx.streaming_signing.as_ref()) {
        Ok(decoder) => decoder,
        Err(err) => return error_response(&err, &wire_ids),
    };
    let mut payload_sha256_hasher = claimed_payload_sha256
        .as_ref()
        .map(|_| checksum::sha256::Sha256::new());
    let mut content_md5_hasher = ctx
        .checksum
        .content_md5
        .map(|_| argmin_crypto::digest::Md5::new());

    // 2. Stream body frames, accumulating into internal segment-sized buffers.
    let ctx = Arc::new(ctx);
    let abort_guard = StreamingAbortGuard::new(&state);
    let mut hasher = checksum::crc64::Hasher::new();
    let mut session_id: Option<SessionId> = None;
    let mut segment_index: u32 = 0;
    let mut buf = PooledSegmentBuffer::new(&state);
    let mut total_size: u64 = 0;
    let mut body_started_emitted = false;
    let mut body_timing = StreamingBodyTiming::default();
    loop {
        let frame_wait_start = Instant::now();
        let next_frame = wait_for_route_bounded_body_frame(
            Some(ctx.storage_route_admission()),
            idle_timeout,
            || {
                #[cfg(test)]
                if let Some(hook) = state.config.route_bounded_body_wait_hook.as_ref() {
                    hook(RouteBoundedBodyOperation::PutObject);
                }
            },
            body.frame(),
        )
        .await;
        body_timing.frame_wait_us += elapsed_micros(frame_wait_start);
        match next_frame {
            RouteBoundedBodyWait::Ready(Some(Ok(frame))) => {
                if let Some(wire_data) = frame.data_ref() {
                    body_timing.data_frames += 1;
                    if let Some(ref mut dec) = decoder {
                        for wire_chunk in wire_data.chunks(CHUNKED_DECODER_FEED_BYTES) {
                            let decode_start = Instant::now();
                            let payload = dec.feed(wire_chunk);
                            body_timing.decode_us += elapsed_micros(decode_start);
                            let payload = match payload {
                                Ok(p) => p,
                                Err(err) => {
                                    abort_streaming(&state, &ctx, session_id.clone()).await;
                                    return error_response(&err, &wire_ids);
                                }
                            };
                            let mut ingest = StreamingPutIngestState {
                                hasher: &mut hasher,
                                payload_sha256_hasher: &mut payload_sha256_hasher,
                                content_md5_hasher: &mut content_md5_hasher,
                                trailing_hasher: &mut trailing_hasher,
                                total_size: &mut total_size,
                                buf: &mut buf,
                                session_id: &mut session_id,
                                segment_index: &mut segment_index,
                                body_started_emitted: &mut body_started_emitted,
                                timing: &mut body_timing,
                                abort_guard: &abort_guard,
                            };
                            if let Err(resp) =
                                ingest_streaming_put_payload(&state, &ctx, &payload, &mut ingest)
                                    .await
                            {
                                return resp;
                            }
                        }
                    } else {
                        let mut ingest = StreamingPutIngestState {
                            hasher: &mut hasher,
                            payload_sha256_hasher: &mut payload_sha256_hasher,
                            content_md5_hasher: &mut content_md5_hasher,
                            trailing_hasher: &mut trailing_hasher,
                            total_size: &mut total_size,
                            buf: &mut buf,
                            session_id: &mut session_id,
                            segment_index: &mut segment_index,
                            body_started_emitted: &mut body_started_emitted,
                            timing: &mut body_timing,
                            abort_guard: &abort_guard,
                        };
                        if let Err(resp) = ingest_streaming_put_payload(
                            &state,
                            &ctx,
                            wire_data.as_ref(),
                            &mut ingest,
                        )
                        .await
                        {
                            return resp;
                        }
                    }
                }
            }
            RouteBoundedBodyWait::Ready(Some(Err(_))) => {
                abort_streaming(&state, &ctx, session_id.clone()).await;
                return error_response(
                    &ServerError::InvalidRequest {
                        reason: "failed to read request body".to_string(),
                    },
                    &wire_ids,
                );
            }
            RouteBoundedBodyWait::Ready(None) => break, // Body complete
            RouteBoundedBodyWait::TimedOut => {
                abort_streaming(&state, &ctx, session_id.clone()).await;
                return error_response(
                    &route_bounded_body_timeout_error(Some(ctx.storage_route_admission())),
                    &wire_ids,
                );
            }
            RouteBoundedBodyWait::RouteInvalid(error) => {
                abort_streaming(&state, &ctx, session_id.clone()).await;
                return error_response(&error, &wire_ids);
            }
        }
    }
    emit_streaming_put_event(
        &ctx,
        "streaming_put_body_read_complete",
        format_args!(
            "bucket={:?} key={:?} session_id={:?} body_bytes_received={} full_segments_flushed={} buffered_tail_bytes={} data_frames={} frame_wait_us={} decode_us={} ingest_local_us={} append_wait_us={}",
            ctx.bucket(),
            ctx.key(),
            streaming_put_session_label(session_id.as_ref()),
            total_size,
            segment_index,
            buf.len(),
            body_timing.data_frames,
            body_timing.frame_wait_us,
            body_timing.decode_us,
            body_timing.ingest_local_us,
            body_timing.append_wait_us
        ),
    );

    // Verify chunked decoding completed and validate post-decode conditions.
    // Extract checksum trailers from aws-chunked body for metadata storage.
    let mut trailer_checksums: Vec<(String, String)> = Vec::new();
    if let Some(dec) = decoder {
        if !dec.is_done() {
            abort_streaming(&state, &ctx, session_id.clone()).await;
            return error_response(&ServerError::IncompleteBody, &wire_ids);
        }
        let trailers = dec.into_trailers();
        if let Err(err) = validate_chunked_post_decode(
            &chunked,
            total_size,
            &trailers,
            declared_trailer.as_deref(),
        ) {
            abort_streaming(&state, &ctx, session_id.clone()).await;
            return error_response(&err, &wire_ids);
        }
        match extract_checksum_trailers(&trailers) {
            Ok(tc) => trailer_checksums = tc,
            Err(err) => {
                abort_streaming(&state, &ctx, session_id.clone()).await;
                return error_response(&err, &wire_ids);
            }
        }
    }

    if let (Some(claimed), Some(h)) = (claimed_payload_sha256.as_ref(), payload_sha256_hasher) {
        let actual = sha256_hex_from_digest(&h.finalize());
        if &actual != claimed {
            abort_streaming(&state, &ctx, session_id.clone()).await;
            return error_response(
                &ServerError::XAmzContentSHA256Mismatch {
                    client_hash: claimed.clone(),
                    server_hash: actual,
                },
                &wire_ids,
            );
        }
    }

    if let (Some(claim), Some(hasher)) = (ctx.checksum.content_md5, content_md5_hasher.take()) {
        let actual = hasher.finalize();
        let mut actual_bytes = [0u8; 16];
        actual_bytes.copy_from_slice(actual.as_ref());
        if let Err(err) = claim.verify(&actual_bytes) {
            abort_streaming(&state, &ctx, session_id.clone()).await;
            return error_response(&err, &wire_ids);
        }
    }

    // Validate checksum against incrementally computed value.
    if let Some(th) = trailing_hasher {
        let algorithm = th.aws_algorithm_name().to_string();
        let actual_b64 = th.finalize_b64();
        if let Some(ref claimed) = inline_checksum_claim {
            // Inline checksum header: verify against streamed body.
            if *claimed != actual_b64 {
                abort_streaming(&state, &ctx, session_id.clone()).await;
                return error_response(
                    &ServerError::ChecksumDigestMismatch { algorithm },
                    &wire_ids,
                );
            }
        } else {
            // Trailing checksum: exactly one trailer expected.
            match trailer_checksums.len() {
                1 => {
                    if trailer_checksums[0].1 != actual_b64 {
                        abort_streaming(&state, &ctx, session_id.clone()).await;
                        return error_response(
                            &ServerError::ChecksumDigestMismatch { algorithm },
                            &wire_ids,
                        );
                    }
                }
                0 => {} // No checksum trailer in body — nothing to validate.
                _ => {
                    // Multiple distinct checksum trailers — reject.
                    abort_streaming(&state, &ctx, session_id.clone()).await;
                    return error_response(
                        &ServerError::InvalidRequest {
                            reason: "multiple checksum trailers not supported".to_string(),
                        },
                        &wire_ids,
                    );
                }
            }
        }
    }

    let crc64 = hasher.finalize();
    let had_tail = !buf.is_empty();
    if session_id.is_none() {
        emit_streaming_put_event(
            &ctx,
            "streaming_put_direct_ready",
            format_args!(
                "bucket={:?} key={:?} body_bytes_received={} segment_bytes={} trailer_checksums={}",
                ctx.bucket(),
                ctx.key(),
                total_size,
                buf.len(),
                trailer_checksums.len()
            ),
        );
        let ctx_ref = Arc::clone(&ctx);
        let st = Arc::clone(&state);
        let trace = ctx.trace.clone();
        return match spawn_blocking_with_trace(trace, move || {
            let frontend = acquire_frontend(&st);
            frontend.put_single_segment_object(&ctx_ref, &buf, &trailer_checksums)
        })
        .await
        {
            Ok(Ok(resp)) => resp,
            Ok(Err(err)) => error_response(&err, &wire_ids),
            Err(_) => internal_error_response(&wire_ids),
        };
    }

    if had_tail {
        let idx = segment_index;
        emit_streaming_put_event(
            &ctx,
            "streaming_put_tail_segment_ready",
            format_args!(
                "bucket={:?} key={:?} session_id={:?} segment_index={} segment_bytes={} body_bytes_received={}",
                ctx.bucket(),
                ctx.key(),
                streaming_put_session_label(session_id.as_ref()),
                idx,
                buf.len(),
                total_size
            ),
        );
        let mut append_state = StreamingPutAppendState {
            session_id: &mut session_id,
            segment_index: &mut segment_index,
            timing: &mut body_timing,
            abort_guard: &abort_guard,
        };
        if let Err(resp) =
            append_streaming_put_buffer(&state, &ctx, &mut append_state, buf, total_size).await
        {
            return resp;
        }
    }

    // 4. Finalize the promoted streaming upload.
    let active_session_id = session_id.expect("streaming put session must exist before finalize");
    emit_streaming_put_event(
        &ctx,
        "streaming_put_finalize_ready",
        format_args!(
            "bucket={:?} key={:?} session_id={:?} body_bytes_received={} segment_count={} trailer_checksums={}",
            ctx.bucket(),
            ctx.key(),
            active_session_id,
            total_size,
            segment_index,
            trailer_checksums.len()
        ),
    );
    let ctx_ref = Arc::clone(&ctx);
    let st = Arc::clone(&state);
    emit_streaming_put_event(
        &ctx,
        "streaming_put_finalize_dispatch",
        format_args!(
            "bucket={:?} key={:?} session_id={:?} body_bytes_received={} segment_count={} trailer_checksums={}",
            ctx.bucket(),
            ctx.key(),
            active_session_id,
            total_size,
            segment_index,
            trailer_checksums.len()
        ),
    );
    let trace = ctx.trace.clone();
    let session_id_for_finalize = active_session_id.clone();
    let abort_guard_for_finalize = Arc::clone(&abort_guard);
    match spawn_blocking_with_trace(trace, move || {
        emit_streaming_put_event(
            &ctx_ref,
            "streaming_put_finalize_worker_start",
            format_args!(
                "bucket={:?} key={:?} session_id={:?} body_bytes_received={}",
                ctx_ref.bucket(),
                ctx_ref.key(),
                session_id_for_finalize,
                total_size
            ),
        );
        let frontend = acquire_frontend(&st);
        emit_streaming_put_event(
            &ctx_ref,
            "streaming_put_finalize_frontend_acquired",
            format_args!(
                "bucket={:?} key={:?} session_id={:?} body_bytes_received={}",
                ctx_ref.bucket(),
                ctx_ref.key(),
                session_id_for_finalize,
                total_size
            ),
        );
        let result = frontend.finalize_streaming_put(
            &ctx_ref,
            &session_id_for_finalize,
            crc64,
            total_size,
            &trailer_checksums,
        );
        if result.is_ok() {
            abort_guard_for_finalize.disarm();
        }
        result
    })
    .await
    {
        Ok(Ok(resp)) => {
            abort_guard.disarm();
            resp
        }
        Ok(Err(err)) => {
            abort_streaming(&state, &ctx, Some(active_session_id.clone())).await;
            error_response(&err, &wire_ids)
        }
        Err(_) => {
            abort_streaming(&state, &ctx, Some(active_session_id)).await;
            internal_error_response(&wire_ids)
        }
    }
}

/// Best-effort abort of a streaming upload session.
async fn abort_streaming(
    state: &Arc<ServerState>,
    ctx: &Arc<super::StreamingPutContext>,
    session_id: Option<SessionId>,
) {
    let Some(session_id) = session_id else {
        return;
    };
    let st = Arc::clone(state);
    let ctx = Arc::clone(ctx);
    let _ = tokio::task::spawn_blocking(move || {
        let frontend = acquire_frontend(&st);
        frontend.abort_streaming_put(&ctx, &session_id);
    })
    .await;
}

struct StreamingPutIngestState<'a> {
    hasher: &'a mut checksum::crc64::Hasher,
    payload_sha256_hasher: &'a mut Option<checksum::sha256::Sha256>,
    content_md5_hasher: &'a mut Option<argmin_crypto::digest::Md5>,
    trailing_hasher: &'a mut Option<TrailingChecksumHasher>,
    total_size: &'a mut u64,
    buf: &'a mut PooledSegmentBuffer,
    session_id: &'a mut Option<SessionId>,
    segment_index: &'a mut u32,
    body_started_emitted: &'a mut bool,
    timing: &'a mut StreamingBodyTiming,
    abort_guard: &'a Arc<StreamingAbortGuard>,
}

struct StreamingPutAppendState<'a> {
    session_id: &'a mut Option<SessionId>,
    segment_index: &'a mut u32,
    timing: &'a mut StreamingBodyTiming,
    abort_guard: &'a Arc<StreamingAbortGuard>,
}

fn streaming_put_session_label(session_id: Option<&SessionId>) -> &str {
    session_id.map(SessionId::as_str).unwrap_or("-")
}

fn emit_streaming_put_event(
    ctx: &Arc<super::StreamingPutContext>,
    name: &'static str,
    fields: std::fmt::Arguments<'_>,
) {
    #[cfg(not(feature = "deep-tracing"))]
    {
        let _ = (ctx, name, fields);
    }

    #[cfg(feature = "deep-tracing")]
    let _ = observability::event_in_context(&ctx.trace, TRACE_TARGET, name, Some(fields));
}

async fn ensure_streaming_put_session(
    state: &Arc<ServerState>,
    ctx: &Arc<super::StreamingPutContext>,
    session_id: &mut Option<SessionId>,
    body_bytes_received: u64,
    abort_guard: &Arc<StreamingAbortGuard>,
) -> Result<(), S3Response> {
    let wire_ids = WireResponseIds::new(ctx.trace.request_id(), state.host_id.clone());
    if let Some(existing) = session_id.as_ref() {
        abort_guard.arm_put(ctx, existing);
        abort_guard.start_put_heartbeat(ctx, existing);
        return Ok(());
    }

    emit_streaming_put_event(
        ctx,
        "streaming_put_session_start_dispatch",
        format_args!(
            "bucket={:?} key={:?} body_bytes_received={}",
            ctx.bucket(),
            ctx.key(),
            body_bytes_received
        ),
    );
    let ctx_ref = Arc::clone(ctx);
    let st = Arc::clone(state);
    let trace = ctx.trace.clone();
    let abort_guard_for_worker = Arc::clone(abort_guard);
    match spawn_blocking_with_trace(trace, move || {
        emit_streaming_put_event(
            &ctx_ref,
            "streaming_put_session_start_worker",
            format_args!("bucket={:?} key={:?}", ctx_ref.bucket(), ctx_ref.key()),
        );
        let frontend = acquire_frontend(&st);
        emit_streaming_put_event(
            &ctx_ref,
            "streaming_put_session_start_frontend_acquired",
            format_args!("bucket={:?} key={:?}", ctx_ref.bucket(), ctx_ref.key()),
        );
        let result = frontend.start_streaming_put_session(&ctx_ref);
        if let Ok(session_id) = result.as_ref() {
            abort_guard_for_worker.arm_put(&ctx_ref, session_id);
        }
        result
    })
    .await
    {
        Ok(Ok(new_session_id)) => {
            emit_streaming_put_event(
                ctx,
                "streaming_put_session_started",
                format_args!(
                    "bucket={:?} key={:?} session_id={:?} body_bytes_received={}",
                    ctx.bucket(),
                    ctx.key(),
                    new_session_id,
                    body_bytes_received
                ),
            );
            abort_guard.start_put_heartbeat(ctx, &new_session_id);
            *session_id = Some(new_session_id);
            Ok(())
        }
        Ok(Err(err)) => Err(error_response(&err, &wire_ids)),
        Err(_) => Err(internal_error_response(&wire_ids)),
    }
}

async fn append_streaming_put_buffer(
    state: &Arc<ServerState>,
    ctx: &Arc<super::StreamingPutContext>,
    append: &mut StreamingPutAppendState<'_>,
    flush_data: PooledSegmentBuffer,
    body_bytes_received: u64,
) -> Result<(), S3Response> {
    let wire_ids = WireResponseIds::new(ctx.trace.request_id(), state.host_id.clone());
    ensure_streaming_put_session(
        state,
        ctx,
        append.session_id,
        body_bytes_received,
        append.abort_guard,
    )
    .await?;
    let session_id_value = append
        .session_id
        .as_ref()
        .expect("session must exist before appending a promoted segment");
    let idx = *append.segment_index;
    emit_streaming_put_event(
        ctx,
        "streaming_put_segment_ready",
        format_args!(
            "bucket={:?} key={:?} session_id={:?} segment_index={} segment_bytes={} body_bytes_received={}",
            ctx.bucket(),
            ctx.key(),
            session_id_value,
            idx,
            flush_data.len(),
            body_bytes_received
        ),
    );
    let ctx_ref = Arc::clone(ctx);
    let st = Arc::clone(state);
    let dispatch_start = Instant::now();
    emit_streaming_put_event(
        ctx,
        "streaming_put_append_dispatch",
        format_args!(
            "bucket={:?} key={:?} session_id={:?} segment_index={} segment_bytes={} body_bytes_received={}",
            ctx.bucket(),
            ctx.key(),
            session_id_value,
            idx,
            flush_data.len(),
            body_bytes_received
        ),
    );
    let trace = ctx.trace.clone();
    let session_id_owned = session_id_value.clone();
    let abort_guard_for_append = Arc::clone(append.abort_guard);
    match spawn_blocking_with_trace(trace, move || {
        let _abort_guard = abort_guard_for_append;
        emit_streaming_put_event(
            &ctx_ref,
            "streaming_put_append_worker_start",
            format_args!(
                "bucket={:?} key={:?} session_id={:?} segment_index={} segment_bytes={}",
                ctx_ref.bucket(),
                ctx_ref.key(),
                session_id_owned,
                idx,
                flush_data.len()
            ),
        );
        let frontend = acquire_frontend(&st);
        emit_streaming_put_event(
            &ctx_ref,
            "streaming_put_append_frontend_acquired",
            format_args!(
                "bucket={:?} key={:?} session_id={:?} segment_index={} segment_bytes={}",
                ctx_ref.bucket(),
                ctx_ref.key(),
                session_id_owned,
                idx,
                flush_data.len()
            ),
        );
        let result =
            frontend.streaming_append_segment(&ctx_ref, &session_id_owned, idx, &flush_data);
        (result, flush_data)
    })
    .await
    {
        Ok((Ok(()), _flush_data)) => {
            *append.segment_index += 1;
            append.timing.append_wait_us += elapsed_micros(dispatch_start);
            Ok(())
        }
        Ok((Err(err), _flush_data)) => {
            abort_streaming(state, ctx, append.session_id.clone()).await;
            Err(error_response(&err, &wire_ids))
        }
        Err(_) => {
            abort_streaming(state, ctx, append.session_id.clone()).await;
            Err(internal_error_response(&wire_ids))
        }
    }
}

async fn ingest_streaming_put_payload(
    state: &Arc<ServerState>,
    ctx: &Arc<super::StreamingPutContext>,
    payload: &[u8],
    ingest: &mut StreamingPutIngestState<'_>,
) -> Result<(), S3Response> {
    let wire_ids = WireResponseIds::new(ctx.trace.request_id(), state.host_id.clone());
    if payload.is_empty() {
        return Ok(());
    }

    let accounting_start = Instant::now();
    ingest.hasher.update(payload);
    if let Some(h) = ingest.payload_sha256_hasher.as_mut() {
        h.update(payload);
    }
    if let Some(h) = ingest.content_md5_hasher.as_mut() {
        h.update(payload);
    }
    if let Some(th) = ingest.trailing_hasher.as_mut() {
        th.update(payload);
    }
    *ingest.total_size += payload.len() as u64;
    if *ingest.total_size > MAX_OBJECT_SIZE {
        abort_streaming(state, ctx, ingest.session_id.clone()).await;
        return Err(error_response(
            &ServerError::ObjectTooLarge {
                size: *ingest.total_size,
                max: MAX_OBJECT_SIZE,
            },
            &wire_ids,
        ));
    }
    if !*ingest.body_started_emitted {
        *ingest.body_started_emitted = true;
        emit_streaming_put_event(
            ctx,
            "streaming_put_body_started",
            format_args!(
                "bucket={:?} key={:?} session_id={:?} frame_bytes={} body_bytes_received={}",
                ctx.bucket(),
                ctx.key(),
                streaming_put_session_label(ingest.session_id.as_ref()),
                payload.len(),
                *ingest.total_size
            ),
        );
    }
    ingest.timing.ingest_local_us += elapsed_micros(accounting_start);

    let mut remaining = payload;
    while !remaining.is_empty() {
        if ingest.buf.len() == crate::coordinator::INTERNAL_SEGMENT_SIZE {
            let mut flush_data = PooledSegmentBuffer::new(state);
            std::mem::swap(ingest.buf, &mut flush_data);
            let mut append_state = StreamingPutAppendState {
                session_id: ingest.session_id,
                segment_index: ingest.segment_index,
                timing: ingest.timing,
                abort_guard: ingest.abort_guard,
            };
            append_streaming_put_buffer(
                state,
                ctx,
                &mut append_state,
                flush_data,
                *ingest.total_size,
            )
            .await?;
        }

        let fill_start = Instant::now();
        let needed = crate::coordinator::INTERNAL_SEGMENT_SIZE - ingest.buf.len();
        let take = needed.min(remaining.len());
        ingest.buf.extend_from_slice(&remaining[..take]);
        remaining = &remaining[take..];
        ingest.timing.ingest_local_us += elapsed_micros(fill_start);
    }

    Ok(())
}

async fn abort_streaming_post_object(
    state: &Arc<ServerState>,
    ctx: &Arc<super::StreamingPostContext>,
) {
    let st = Arc::clone(state);
    let ctx = Arc::clone(ctx);
    let _ = tokio::task::spawn_blocking(move || {
        let frontend = acquire_frontend(&st);
        frontend.abort_streaming_post_object(&ctx);
    })
    .await;
}

fn close_response_connection(mut resp: S3Response) -> S3Response {
    resp.headers
        .push(("Connection".to_string(), "close".to_string()));
    resp
}

fn response_to_hyper_with_request_body(
    state: &ServerState,
    mut resp: S3Response,
    request_body: TrackedIncoming,
    admission: Option<HttpRequestAdmission>,
    response_trace: super::ResponseTraceMeta,
) -> http::Response<S3HyperBody> {
    let (request_body, eof_observed) = request_body.into_parts();
    let retain_unread_request_body = !eof_observed;
    if retain_unread_request_body {
        resp = close_response_connection(resp);
    }
    #[cfg(any(test, debug_assertions))]
    let failure_diagnostics = super::ResponseFailureDiagnostics::new(
        state.config.panic_on_500,
        state.config.abort_on_500,
    );
    #[cfg(not(any(test, debug_assertions)))]
    let failure_diagnostics = super::ResponseFailureDiagnostics::disabled();
    let mut response = s3_response_to_hyper(
        resp,
        admission,
        state.config.stream_read_chunk_size,
        failure_diagnostics,
        response_trace,
    );
    if retain_unread_request_body {
        response.body_mut().retain_unread_request_body(request_body);
    }
    response
}

async fn finish_streaming_prepare_failure(
    resp: S3Response,
    err: &ServerError,
    has_auth_attempt: bool,
    body: &mut TrackedIncoming,
    idle_timeout: Duration,
) -> S3Response {
    if should_close_streaming_prepare_failure(err, has_auth_attempt) {
        close_response_connection(resp)
    } else {
        finish_streaming_rejection_bounded(resp, body, idle_timeout).await
    }
}

/// Decide whether a streaming prepare failure should close the connection
/// immediately or drain the unread body and return a normal error response.
///
/// The important distinction is between:
/// - auth-layer failures / anonymous-deny cases, where continuing to read the
///   body would let an unauthenticated client hold request capacity, and
/// - authenticated permission denials (for example bucket policy
///   `AccessDenied`), where SDK clients expect a normal S3 error rather than a
///   transport-level broken pipe while they are still writing the body.
///
/// So:
/// - auth failures always close promptly
/// - anonymous `AccessDenied` closes promptly
/// - authenticated `AccessDenied` drains within a small budget, then responds
///   or closes if the client keeps sending
/// - other prepare-time validation failures use the same bounded drain path
///   rather than an unbounded EOF drain
fn should_close_streaming_prepare_failure(err: &ServerError, has_auth_attempt: bool) -> bool {
    match err {
        ServerError::Auth(_) => true,
        ServerError::AccessDenied => !has_auth_attempt,
        _ => false,
    }
}

fn request_has_auth_attempt(req: &S3Request) -> bool {
    req.header("authorization").is_some()
        || req.query_param_lossy("X-Amz-Algorithm").is_some()
        || req.query_param_lossy("X-Amz-Credential").is_some()
        || req.query_param_lossy("X-Amz-Signature").is_some()
}

async fn finish_streaming_post_rejection(
    resp: S3Response,
    body: &mut TrackedIncoming,
    idle_timeout: Duration,
) -> S3Response {
    finish_streaming_rejection_bounded(resp, body, idle_timeout).await
}

async fn finish_streaming_rejection_bounded(
    resp: S3Response,
    body: &mut TrackedIncoming,
    idle_timeout: Duration,
) -> S3Response {
    if drain_request_body_bounded(
        body,
        idle_timeout,
        MAX_STREAMING_REJECT_DRAIN_BYTES,
        MAX_STREAMING_REJECT_DRAIN_DURATION,
    )
    .await
    {
        resp
    } else {
        close_response_connection(resp)
    }
}

async fn drain_request_body_bounded(
    body: &mut TrackedIncoming,
    idle_timeout: Duration,
    max_bytes: usize,
    max_duration: Duration,
) -> bool {
    let deadline = Instant::now() + max_duration;
    let mut drained_bytes = 0usize;

    loop {
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        let read_timeout = idle_timeout.min(deadline.saturating_duration_since(now));
        match tokio::time::timeout(read_timeout, body.frame()).await {
            Ok(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    drained_bytes = match drained_bytes.checked_add(data.len()) {
                        Some(total) => total,
                        None => return false,
                    };
                    if drained_bytes > max_bytes {
                        return false;
                    }
                }
            }
            Ok(Some(Err(_))) => return false,
            Ok(None) => return true,
            Err(_) => return false,
        }
    }
}

/// Handle a streaming `UploadPart`: read body frame-by-frame, feed chunks to
/// coordinator append API, finalize atomically.
#[allow(clippy::too_many_arguments)]
async fn handle_streaming_part(
    state: Arc<ServerState>,
    s3req: S3Request,
    body: &mut TrackedIncoming,
    bucket: BucketName,
    key: String,
    upload_id: String,
    part_number: String,
    chunked: ChunkedMode,
    trace: observability::TraceContext,
    wire_ids: WireResponseIds,
) -> S3Response {
    let idle_timeout = state.config.body_idle_timeout;
    let abort_guard = StreamingAbortGuard::new(&state);

    let state2 = Arc::clone(&state);
    let bucket_clone = bucket.clone();
    let key_clone = key.clone();
    let upload_id_clone = upload_id.clone();
    let declared_trailer = s3req.header("x-amz-trailer").map(str::to_string);
    let claimed_payload_sha256 = claimed_payload_sha256_from_request(&s3req);
    let mut trailing_hasher = trailing_hasher_from_request(&s3req);
    let mut inline_checksum_claim: Option<String> = None;
    if trailing_hasher.is_none() {
        if let Some((h, claimed)) = inline_checksum_hasher_from_request(&s3req) {
            trailing_hasher = Some(h);
            inline_checksum_claim = Some(claimed);
        }
    }
    let has_auth_attempt = request_has_auth_attempt(&s3req);
    let abort_guard_for_prepare = Arc::clone(&abort_guard);
    let ctx = match spawn_blocking_with_trace(trace, move || {
        let frontend = acquire_frontend(&state2);
        let result = frontend.prepare_streaming_part(
            &s3req,
            bucket_clone.as_str(),
            &key_clone,
            &upload_id_clone,
            &part_number,
        );
        result.map(|ctx| {
            let ctx = Arc::new(ctx);
            abort_guard_for_prepare.arm_part(&ctx);
            ctx
        })
    })
    .await
    {
        Ok(Ok(ctx)) => ctx,
        Ok(Err(err)) => {
            return finish_streaming_prepare_failure(
                error_response(&err, &wire_ids),
                &err,
                has_auth_attempt,
                body,
                idle_timeout,
            )
            .await
        }
        Err(_) => {
            let err = internal_error();
            return finish_streaming_prepare_failure(
                internal_error_response(&wire_ids),
                &err,
                has_auth_attempt,
                body,
                idle_timeout,
            )
            .await;
        }
    };
    if ctx.auth_mode == auth::AuthMode::PresignedSigV4 && chunked != ChunkedMode::None {
        let err = presigned_streaming_body_error(
            body,
            &chunked,
            PresignedStreamingOperation::UploadPart,
            idle_timeout,
        )
        .await;
        abort_streaming_part_ctx(&state, &ctx).await;
        return error_response(&err, &wire_ids);
    }
    if trailing_hasher.is_none() {
        if let Some(upload_algorithm) = ctx.checksum.upload_checksum_algorithm {
            trailing_hasher = Some(TrailingChecksumHasher::from_algorithm(upload_algorithm));
        }
    }

    // Build chunked decoder if needed.
    let mut decoder = match make_chunked_decoder(&chunked, ctx.streaming_signing.as_ref()) {
        Ok(decoder) => decoder,
        Err(err) => return error_response(&err, &wire_ids),
    };
    let mut payload_sha256_hasher = claimed_payload_sha256
        .as_ref()
        .map(|_| checksum::sha256::Sha256::new());
    let mut content_md5_hasher = ctx
        .checksum
        .content_md5
        .map(|_| argmin_crypto::digest::Md5::new());
    // 2. Stream body frames, accumulating into internal segment-sized buffers.
    let mut hasher = checksum::crc64::Hasher::new();
    let mut segment_index: u32 = 0;
    let mut buf = PooledSegmentBuffer::new(&state);
    let mut total_size: u64 = 0;
    let mut body_started_emitted = false;
    let mut body_timing = StreamingBodyTiming::default();
    loop {
        let frame_wait_start = Instant::now();
        let next_frame = wait_for_route_bounded_body_frame(
            Some(ctx.storage_route_admission()),
            idle_timeout,
            || {
                #[cfg(test)]
                if let Some(hook) = state.config.route_bounded_body_wait_hook.as_ref() {
                    hook(RouteBoundedBodyOperation::UploadPart);
                }
            },
            body.frame(),
        )
        .await;
        body_timing.frame_wait_us += elapsed_micros(frame_wait_start);
        match next_frame {
            RouteBoundedBodyWait::Ready(Some(Ok(frame))) => {
                if let Some(wire_data) = frame.data_ref() {
                    body_timing.data_frames += 1;
                    if let Some(ref mut dec) = decoder {
                        for wire_chunk in wire_data.chunks(CHUNKED_DECODER_FEED_BYTES) {
                            let decode_start = Instant::now();
                            let payload = dec.feed(wire_chunk);
                            body_timing.decode_us += elapsed_micros(decode_start);
                            let payload = match payload {
                                Ok(p) => p,
                                Err(err) => {
                                    abort_streaming_part_ctx(&state, &ctx).await;
                                    return error_response(&err, &wire_ids);
                                }
                            };
                            let mut ingest = StreamingPartIngestState {
                                hasher: &mut hasher,
                                payload_sha256_hasher: &mut payload_sha256_hasher,
                                content_md5_hasher: &mut content_md5_hasher,
                                trailing_hasher: &mut trailing_hasher,
                                total_size: &mut total_size,
                                buf: &mut buf,
                                segment_index: &mut segment_index,
                                body_started_emitted: &mut body_started_emitted,
                                timing: &mut body_timing,
                                abort_guard: &abort_guard,
                            };
                            if let Err(resp) =
                                ingest_streaming_part_payload(&state, &ctx, &payload, &mut ingest)
                                    .await
                            {
                                return resp;
                            }
                        }
                    } else {
                        let mut ingest = StreamingPartIngestState {
                            hasher: &mut hasher,
                            payload_sha256_hasher: &mut payload_sha256_hasher,
                            content_md5_hasher: &mut content_md5_hasher,
                            trailing_hasher: &mut trailing_hasher,
                            total_size: &mut total_size,
                            buf: &mut buf,
                            segment_index: &mut segment_index,
                            body_started_emitted: &mut body_started_emitted,
                            timing: &mut body_timing,
                            abort_guard: &abort_guard,
                        };
                        if let Err(resp) = ingest_streaming_part_payload(
                            &state,
                            &ctx,
                            wire_data.as_ref(),
                            &mut ingest,
                        )
                        .await
                        {
                            return resp;
                        }
                    }
                }
            }
            RouteBoundedBodyWait::Ready(Some(Err(_))) => {
                abort_streaming_part_ctx(&state, &ctx).await;
                return error_response(
                    &ServerError::InvalidRequest {
                        reason: "failed to read request body".to_string(),
                    },
                    &wire_ids,
                );
            }
            RouteBoundedBodyWait::Ready(None) => break,
            RouteBoundedBodyWait::TimedOut => {
                abort_streaming_part_ctx(&state, &ctx).await;
                return error_response(
                    &route_bounded_body_timeout_error(Some(ctx.storage_route_admission())),
                    &wire_ids,
                );
            }
            RouteBoundedBodyWait::RouteInvalid(error) => {
                abort_streaming_part_ctx(&state, &ctx).await;
                return error_response(&error, &wire_ids);
            }
        }
    }
    emit_streaming_part_event(
        &ctx,
        "streaming_part_body_read_complete",
        format_args!(
            "bucket={:?} key={:?} upload_id={:?} part_number={} session_id={:?} body_bytes_received={} full_segments_flushed={} buffered_tail_bytes={} data_frames={} frame_wait_us={} decode_us={} ingest_local_us={} append_wait_us={}",
            ctx.bucket(),
            ctx.key(),
            ctx.upload_id(),
            ctx.part_number(),
            ctx.session_id(),
            total_size,
            segment_index,
            buf.len(),
            body_timing.data_frames,
            body_timing.frame_wait_us,
            body_timing.decode_us,
            body_timing.ingest_local_us,
            body_timing.append_wait_us
        ),
    );
    emit_streaming_part_phase(
        &ctx,
        observability::StreamUploadPhase::BodyReadComplete,
        None,
        Some(total_size),
        None,
        Some(segment_index),
    );

    // Verify chunked decoding completed and validate post-decode conditions.
    // Extract checksum trailers from aws-chunked body.
    let mut trailer_checksums: Vec<(String, String)> = Vec::new();
    if let Some(dec) = decoder {
        if !dec.is_done() {
            abort_streaming_part_ctx(&state, &ctx).await;
            return error_response(&ServerError::IncompleteBody, &wire_ids);
        }
        let trailers = dec.into_trailers();
        if let Err(err) = validate_chunked_post_decode(
            &chunked,
            total_size,
            &trailers,
            declared_trailer.as_deref(),
        ) {
            abort_streaming_part_ctx(&state, &ctx).await;
            return error_response(&err, &wire_ids);
        }
        match extract_checksum_trailers(&trailers) {
            Ok(tc) => trailer_checksums = tc,
            Err(err) => {
                abort_streaming_part_ctx(&state, &ctx).await;
                return error_response(&err, &wire_ids);
            }
        }
    }

    if let (Some(claimed), Some(h)) = (claimed_payload_sha256.as_ref(), payload_sha256_hasher) {
        let actual = sha256_hex_from_digest(&h.finalize());
        if &actual != claimed {
            abort_streaming_part_ctx(&state, &ctx).await;
            return error_response(
                &ServerError::XAmzContentSHA256Mismatch {
                    client_hash: claimed.clone(),
                    server_hash: actual,
                },
                &wire_ids,
            );
        }
    }

    if let (Some(claim), Some(hasher)) = (ctx.checksum.content_md5, content_md5_hasher.take()) {
        let actual = hasher.finalize();
        let mut actual_bytes = [0u8; 16];
        actual_bytes.copy_from_slice(actual.as_ref());
        if let Err(err) = claim.verify(&actual_bytes) {
            abort_streaming_part_ctx(&state, &ctx).await;
            return error_response(&err, &wire_ids);
        }
    }

    // Validate checksum against incrementally computed value.
    // Keep the computed RawChecksum for passing to finalization.
    let computed_checksum = if let Some(th) = trailing_hasher {
        use base64::Engine;
        let algorithm = th.aws_algorithm_name().to_string();
        let cksum = th.finalize_raw();
        let actual_b64 = base64::engine::general_purpose::STANDARD.encode(cksum.bytes());
        if let Some(ref claimed) = inline_checksum_claim {
            // Inline checksum header: verify against streamed body.
            if *claimed != actual_b64 {
                abort_streaming_part_ctx(&state, &ctx).await;
                return error_response(
                    &ServerError::ChecksumDigestMismatch { algorithm },
                    &wire_ids,
                );
            }
        } else {
            // Trailing checksum: validate if present.
            match trailer_checksums.len() {
                1 => {
                    if trailer_checksums[0].1 != actual_b64 {
                        abort_streaming_part_ctx(&state, &ctx).await;
                        return error_response(
                            &ServerError::ChecksumDigestMismatch { algorithm },
                            &wire_ids,
                        );
                    }
                }
                0 => {}
                _ => {
                    abort_streaming_part_ctx(&state, &ctx).await;
                    return error_response(
                        &ServerError::InvalidRequest {
                            reason: "multiple checksum trailers not supported".to_string(),
                        },
                        &wire_ids,
                    );
                }
            }
        }
        Some(cksum)
    } else {
        None
    };

    // 3. Flush remaining buffer.
    let had_tail = !buf.is_empty();
    if had_tail {
        let idx = segment_index;
        emit_streaming_part_event(
            &ctx,
            "streaming_part_tail_segment_ready",
            format_args!(
                "bucket={:?} key={:?} upload_id={:?} part_number={} session_id={:?} segment_index={} segment_bytes={} body_bytes_received={}",
                ctx.bucket(),
                ctx.key(),
                ctx.upload_id(),
                ctx.part_number(),
                ctx.session_id(),
                idx,
                buf.len(),
                total_size
            ),
        );
        let ctx_ref = Arc::clone(&ctx);
        let st = Arc::clone(&state);
        emit_streaming_part_event(
            &ctx,
            "streaming_part_append_dispatch",
            format_args!(
                "bucket={:?} key={:?} upload_id={:?} part_number={} session_id={:?} segment_index={} segment_bytes={} body_bytes_received={}",
                ctx.bucket(),
                ctx.key(),
                ctx.upload_id(),
                ctx.part_number(),
                ctx.session_id(),
                idx,
                buf.len(),
                total_size
            ),
        );
        emit_streaming_part_phase(
            &ctx,
            observability::StreamUploadPhase::SegmentAppendStarted,
            Some(idx),
            Some(total_size),
            Some(buf.len() as u64),
            None,
        );
        let trace = ctx.trace.clone();
        let abort_guard_for_append = Arc::clone(&abort_guard);
        match spawn_blocking_with_trace(trace, move || {
            let _abort_guard = abort_guard_for_append;
            emit_streaming_part_event(
                &ctx_ref,
                "streaming_part_append_worker_start",
                format_args!(
                    "bucket={:?} key={:?} upload_id={:?} part_number={} session_id={:?} segment_index={} segment_bytes={}",
                    ctx_ref.bucket(),
                    ctx_ref.key(),
                    ctx_ref.upload_id(),
                    ctx_ref.part_number(),
                    ctx_ref.session_id(),
                    idx,
                    buf.len()
                ),
            );
            let frontend = acquire_frontend(&st);
            emit_streaming_part_event(
                &ctx_ref,
                "streaming_part_append_frontend_acquired",
                format_args!(
                    "bucket={:?} key={:?} upload_id={:?} part_number={} session_id={:?} segment_index={} segment_bytes={}",
                    ctx_ref.bucket(),
                    ctx_ref.key(),
                    ctx_ref.upload_id(),
                    ctx_ref.part_number(),
                    ctx_ref.session_id(),
                    idx,
                    buf.len()
                ),
            );
            let result = frontend.streaming_append_part_segment(&ctx_ref, idx, &buf);
            (result, buf)
        })
        .await
        {
            Ok((Ok(()), _buf)) => {
                emit_streaming_part_phase(
                    &ctx,
                    observability::StreamUploadPhase::SegmentAppendFinished,
                    Some(idx),
                    Some(total_size),
                    None,
                    None,
                );
            }
            Ok((Err(err), _buf)) => {
                emit_streaming_part_phase(
                    &ctx,
                    observability::StreamUploadPhase::SegmentAppendError,
                    Some(idx),
                    Some(total_size),
                    None,
                    None,
                );
                abort_streaming_part_ctx(&state, &ctx).await;
                return error_response(&err, &wire_ids);
            }
            Err(_) => {
                emit_streaming_part_phase(
                    &ctx,
                    observability::StreamUploadPhase::SegmentAppendError,
                    Some(idx),
                    Some(total_size),
                    None,
                    None,
                );
                abort_streaming_part_ctx(&state, &ctx).await;
                return internal_error_response(&wire_ids);
            }
        }
    }

    // 4. Finalize the streaming upload part.
    let crc64 = hasher.finalize();
    emit_streaming_part_event(
        &ctx,
        "streaming_part_finalize_ready",
        format_args!(
            "bucket={:?} key={:?} upload_id={:?} part_number={} session_id={:?} body_bytes_received={} segment_count={} trailer_checksums={}",
            ctx.bucket(),
            ctx.key(),
            ctx.upload_id(),
            ctx.part_number(),
            ctx.session_id(),
            total_size,
            segment_index + u32::from(had_tail),
            trailer_checksums.len()
        ),
    );
    let ctx_ref = Arc::clone(&ctx);
    let st = Arc::clone(&state);
    emit_streaming_part_event(
        &ctx,
        "streaming_part_finalize_dispatch",
        format_args!(
            "bucket={:?} key={:?} upload_id={:?} part_number={} session_id={:?} body_bytes_received={} segment_count={} trailer_checksums={}",
            ctx.bucket(),
            ctx.key(),
            ctx.upload_id(),
            ctx.part_number(),
            ctx.session_id(),
            total_size,
            segment_index + u32::from(had_tail),
            trailer_checksums.len()
        ),
    );
    emit_streaming_part_phase(
        &ctx,
        observability::StreamUploadPhase::FinalizeStarted,
        None,
        Some(total_size),
        None,
        Some(segment_index + u32::from(had_tail)),
    );
    let trace = ctx.trace.clone();
    let abort_guard_for_finalize = Arc::clone(&abort_guard);
    match spawn_blocking_with_trace(trace, move || {
        emit_streaming_part_event(
            &ctx_ref,
            "streaming_part_finalize_worker_start",
            format_args!(
                "bucket={:?} key={:?} upload_id={:?} part_number={} session_id={:?} body_bytes_received={}",
                ctx_ref.bucket(),
                ctx_ref.key(),
                ctx_ref.upload_id(),
                ctx_ref.part_number(),
                ctx_ref.session_id(),
                total_size
            ),
        );
        let frontend = acquire_frontend(&st);
        emit_streaming_part_event(
            &ctx_ref,
            "streaming_part_finalize_frontend_acquired",
            format_args!(
                "bucket={:?} key={:?} upload_id={:?} part_number={} session_id={:?} body_bytes_received={}",
                ctx_ref.bucket(),
                ctx_ref.key(),
                ctx_ref.upload_id(),
                ctx_ref.part_number(),
                ctx_ref.session_id(),
                total_size
            ),
        );
        let result = frontend.finalize_streaming_part(
            &ctx_ref,
            crc64,
            total_size,
            &trailer_checksums,
            computed_checksum,
        );
        if result.is_ok() {
            abort_guard_for_finalize.disarm();
        }
        result
    })
    .await
    {
        Ok(Ok(resp)) => {
            emit_streaming_part_phase(
                &ctx,
                observability::StreamUploadPhase::SessionFinalized,
                None,
                Some(total_size),
                None,
                Some(segment_index + u32::from(had_tail)),
            );
            resp
        }
        Ok(Err(err)) => {
            emit_streaming_part_phase(
                &ctx,
                observability::StreamUploadPhase::FinalizeError,
                None,
                Some(total_size),
                None,
                Some(segment_index + u32::from(had_tail)),
            );
            abort_streaming_part_ctx(&state, &ctx).await;
            error_response(&err, &wire_ids)
        }
        Err(_) => {
            emit_streaming_part_phase(
                &ctx,
                observability::StreamUploadPhase::FinalizeError,
                None,
                Some(total_size),
                None,
                Some(segment_index + u32::from(had_tail)),
            );
            abort_streaming_part_ctx(&state, &ctx).await;
            internal_error_response(&wire_ids)
        }
    }
}

/// Best-effort abort of a streaming `UploadPart` session.
async fn abort_streaming_part_ctx(
    state: &Arc<ServerState>,
    ctx: &Arc<super::StreamingPartContext>,
) {
    let st = Arc::clone(state);
    let ctx = Arc::clone(ctx);
    let _ = tokio::task::spawn_blocking(move || {
        let frontend = acquire_frontend(&st);
        frontend.abort_streaming_part(&ctx);
    })
    .await;
}

struct StreamingPartIngestState<'a> {
    hasher: &'a mut checksum::crc64::Hasher,
    payload_sha256_hasher: &'a mut Option<checksum::sha256::Sha256>,
    content_md5_hasher: &'a mut Option<argmin_crypto::digest::Md5>,
    trailing_hasher: &'a mut Option<TrailingChecksumHasher>,
    total_size: &'a mut u64,
    buf: &'a mut PooledSegmentBuffer,
    segment_index: &'a mut u32,
    body_started_emitted: &'a mut bool,
    timing: &'a mut StreamingBodyTiming,
    abort_guard: &'a Arc<StreamingAbortGuard>,
}

fn emit_streaming_part_event(
    ctx: &Arc<super::StreamingPartContext>,
    name: &'static str,
    fields: std::fmt::Arguments<'_>,
) {
    #[cfg(not(feature = "deep-tracing"))]
    {
        let _ = (ctx, name, fields);
    }

    #[cfg(feature = "deep-tracing")]
    let _ = observability::event_in_context(&ctx.trace, TRACE_TARGET, name, Some(fields));
}

fn emit_streaming_part_phase(
    ctx: &Arc<super::StreamingPartContext>,
    phase: observability::StreamUploadPhase,
    segment_index: Option<u32>,
    body_bytes_received: Option<u64>,
    segment_bytes: Option<u64>,
    segment_count: Option<u32>,
) {
    let _ = observability::emit_stream_upload_phase(
        &ctx.trace,
        TRACE_TARGET,
        observability::StreamUploadPhaseSummary {
            operation: "UploadPart",
            phase,
            bucket: ctx.bucket().as_str(),
            key: ctx.key().as_str(),
            upload_id: Some(ctx.upload_id().as_str()),
            part_number: Some(ctx.part_number()),
            session_id: Some(ctx.session_id().as_str()),
            segment_index,
            body_bytes_received,
            segment_bytes,
            segment_count,
        },
    );
}

async fn ingest_streaming_part_payload(
    state: &Arc<ServerState>,
    ctx: &Arc<super::StreamingPartContext>,
    payload: &[u8],
    ingest: &mut StreamingPartIngestState<'_>,
) -> Result<(), S3Response> {
    let wire_ids = WireResponseIds::new(ctx.trace.request_id(), state.host_id.clone());
    if payload.is_empty() {
        return Ok(());
    }

    let accounting_start = Instant::now();
    ingest.hasher.update(payload);
    if let Some(h) = ingest.payload_sha256_hasher.as_mut() {
        h.update(payload);
    }
    if let Some(h) = ingest.content_md5_hasher.as_mut() {
        h.update(payload);
    }
    if let Some(th) = ingest.trailing_hasher.as_mut() {
        th.update(payload);
    }
    *ingest.total_size += payload.len() as u64;
    if *ingest.total_size > MAX_OBJECT_SIZE {
        abort_streaming_part_ctx(state, ctx).await;
        return Err(error_response(
            &ServerError::ObjectTooLarge {
                size: *ingest.total_size,
                max: MAX_OBJECT_SIZE,
            },
            &wire_ids,
        ));
    }
    if !*ingest.body_started_emitted {
        *ingest.body_started_emitted = true;
        emit_streaming_part_event(
            ctx,
            "streaming_part_body_started",
            format_args!(
                "bucket={:?} key={:?} upload_id={:?} part_number={} session_id={:?} frame_bytes={} body_bytes_received={}",
                ctx.bucket(),
                ctx.key(),
                ctx.upload_id(),
                ctx.part_number(),
                ctx.session_id(),
                payload.len(),
                *ingest.total_size
            ),
        );
        emit_streaming_part_phase(
            ctx,
            observability::StreamUploadPhase::BodyStarted,
            None,
            Some(*ingest.total_size),
            Some(payload.len() as u64),
            None,
        );
    }
    ingest.timing.ingest_local_us += elapsed_micros(accounting_start);

    let mut remaining = payload;
    while !remaining.is_empty() {
        let fill_start = Instant::now();
        let needed = crate::coordinator::INTERNAL_SEGMENT_SIZE - ingest.buf.len();
        let take = needed.min(remaining.len());
        ingest.buf.extend_from_slice(&remaining[..take]);
        remaining = &remaining[take..];
        if ingest.buf.len() < crate::coordinator::INTERNAL_SEGMENT_SIZE {
            ingest.timing.ingest_local_us += elapsed_micros(fill_start);
            continue;
        }

        let mut flush_data = PooledSegmentBuffer::new(state);
        std::mem::swap(ingest.buf, &mut flush_data);
        let idx = *ingest.segment_index;
        *ingest.segment_index += 1;
        emit_streaming_part_event(
            ctx,
            "streaming_part_segment_ready",
            format_args!(
                "bucket={:?} key={:?} upload_id={:?} part_number={} session_id={:?} segment_index={} segment_bytes={} body_bytes_received={}",
                ctx.bucket(),
                ctx.key(),
                ctx.upload_id(),
                ctx.part_number(),
                ctx.session_id(),
                idx,
                flush_data.len(),
                *ingest.total_size
            ),
        );
        ingest.timing.ingest_local_us += elapsed_micros(fill_start);
        let ctx_ref = Arc::clone(ctx);
        let st = Arc::clone(state);
        let dispatch_start = Instant::now();
        let segment_bytes = flush_data.len() as u64;
        emit_streaming_part_event(
            ctx,
            "streaming_part_append_dispatch",
            format_args!(
                "bucket={:?} key={:?} upload_id={:?} part_number={} session_id={:?} segment_index={} segment_bytes={} body_bytes_received={}",
                ctx.bucket(),
                ctx.key(),
                ctx.upload_id(),
                ctx.part_number(),
                ctx.session_id(),
                idx,
                flush_data.len(),
                *ingest.total_size
            ),
        );
        emit_streaming_part_phase(
            ctx,
            observability::StreamUploadPhase::SegmentAppendStarted,
            Some(idx),
            Some(*ingest.total_size),
            Some(segment_bytes),
            None,
        );
        let trace = ctx.trace.clone();
        let abort_guard_for_append = Arc::clone(ingest.abort_guard);
        match spawn_blocking_with_trace(trace, move || {
            let _abort_guard = abort_guard_for_append;
            emit_streaming_part_event(
                &ctx_ref,
                "streaming_part_append_worker_start",
                format_args!(
                    "bucket={:?} key={:?} upload_id={:?} part_number={} session_id={:?} segment_index={} segment_bytes={}",
                    ctx_ref.bucket(),
                    ctx_ref.key(),
                    ctx_ref.upload_id(),
                    ctx_ref.part_number(),
                    ctx_ref.session_id(),
                    idx,
                    flush_data.len()
                ),
            );
            let frontend = acquire_frontend(&st);
            emit_streaming_part_event(
                &ctx_ref,
                "streaming_part_append_frontend_acquired",
                format_args!(
                    "bucket={:?} key={:?} upload_id={:?} part_number={} session_id={:?} segment_index={} segment_bytes={}",
                    ctx_ref.bucket(),
                    ctx_ref.key(),
                    ctx_ref.upload_id(),
                    ctx_ref.part_number(),
                    ctx_ref.session_id(),
                    idx,
                    flush_data.len()
                ),
            );
            let result = frontend.streaming_append_part_segment(&ctx_ref, idx, &flush_data);
            (result, flush_data)
        })
        .await
        {
            Ok((Ok(()), _flush_data)) => {
                emit_streaming_part_phase(
                    ctx,
                    observability::StreamUploadPhase::SegmentAppendFinished,
                    Some(idx),
                    Some(*ingest.total_size),
                    Some(segment_bytes),
                    None,
                );
            }
            Ok((Err(err), _flush_data)) => {
                emit_streaming_part_phase(
                    ctx,
                    observability::StreamUploadPhase::SegmentAppendError,
                    Some(idx),
                    Some(*ingest.total_size),
                    Some(segment_bytes),
                    None,
                );
                abort_streaming_part_ctx(state, ctx).await;
                return Err(error_response(&err, &wire_ids));
            }
            Err(_) => {
                emit_streaming_part_phase(
                    ctx,
                    observability::StreamUploadPhase::SegmentAppendError,
                    Some(idx),
                    Some(*ingest.total_size),
                    Some(segment_bytes),
                    None,
                );
                abort_streaming_part_ctx(state, ctx).await;
                return Err(internal_error_response(&wire_ids));
            }
        }
        ingest.timing.append_wait_us += elapsed_micros(dispatch_start);
    }

    Ok(())
}

/// Validate post-decode conditions for aws-chunked requests.
///
/// Checks decoded content length matches declared, and trailer declarations
/// are consistent with actual trailers in the body.
pub(super) fn validate_chunked_post_decode(
    chunked: &ChunkedMode,
    total_decoded: u64,
    trailers: &[(String, String)],
    declared_trailer: Option<&str>,
) -> Result<(), ServerError> {
    // Validate decoded length matches x-amz-decoded-content-length.
    if let Some(expected) = chunked.expected_len() {
        if total_decoded != expected {
            return Err(ServerError::MalformedChunkedBody {
                reason: format!(
                    "decoded content length mismatch: expected {expected}, got {total_decoded}"
                ),
            });
        }
    }

    let is_trailer = chunked.is_trailer_mode();

    // Content trailers = trailers excluding x-amz-trailer-signature.
    let content_trailers: Vec<&(String, String)> = trailers
        .iter()
        .filter(|(k, _)| k != "x-amz-trailer-signature")
        .collect();

    // Non-trailer mode must not have trailers in body.
    if !is_trailer && !content_trailers.is_empty() {
        return Err(ServerError::IncompleteBody);
    }

    // Trailers in body but no declaration header.
    if !content_trailers.is_empty() && declared_trailer.is_none() {
        return Err(ServerError::MalformedTrailerError {
            reason: "trailers present in body but x-amz-trailer header missing".to_string(),
        });
    }

    if let Some(declared) = declared_trailer {
        let declared_names: Vec<String> = declared
            .split(',')
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| !s.is_empty())
            .collect();

        if content_trailers.is_empty() {
            return Err(ServerError::MalformedTrailerError {
                reason: format!("x-amz-trailer header declares {declared} but no trailers in body"),
            });
        }

        // All body trailers must be declared.
        for (name, _) in &content_trailers {
            if !declared_names.iter().any(|d| d == name.as_str()) {
                return Err(ServerError::MalformedTrailerError {
                    reason: format!("undeclared trailer in body: {name} (declared: {declared})"),
                });
            }
        }

        // All declared names must appear in body.
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

    Ok(())
}

/// Extract checksum-related trailers from aws-chunked body trailers.
///
/// Returns at most one `(header_name, value)` pair. Rejects requests with
/// duplicate checksum trailer names (same key appearing more than once).
fn extract_checksum_trailers(
    trailers: &[(String, String)],
) -> Result<Vec<(String, String)>, ServerError> {
    let mut result = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for (k, v) in trailers {
        let lower = k.to_ascii_lowercase();
        if lower.starts_with("x-amz-checksum-")
            && lower != "x-amz-checksum-algorithm"
            && lower != "x-amz-checksum-type"
        {
            if !seen.insert(lower.clone()) {
                return Err(ServerError::InvalidRequest {
                    reason: format!("duplicate checksum trailer: {k}"),
                });
            }
            result.push((lower, v.clone()));
        }
    }
    Ok(result)
}

/// Build a `TrailingChecksumHasher` from the `x-amz-trailer` request header.
///
/// Handles comma-separated trailer declarations and case-insensitive matching.
/// Returns the hasher for the first recognized checksum trailer name.
fn trailing_hasher_from_request(req: &S3Request) -> Option<TrailingChecksumHasher> {
    let header_val = req.header("x-amz-trailer")?;
    for name in header_val.split(',') {
        let trimmed = name.trim();
        if let Some(h) = TrailingChecksumHasher::from_trailer_header(trimmed) {
            return Some(h);
        }
    }
    None
}

/// Build a `TrailingChecksumHasher` from an inline `x-amz-checksum-*` header value.
///
/// Used when a streaming PUT includes a checksum value header but no trailing
/// checksum declaration. Returns the hasher and the claimed base64 value so
/// the caller can verify after body streaming completes.
fn inline_checksum_hasher_from_request(
    req: &S3Request,
) -> Option<(TrailingChecksumHasher, String)> {
    // The header names in CHECKSUM_HEADERS (in mod.rs) match the trailer
    // header names used by TrailingChecksumHasher::from_trailer_header.
    for algorithm in ChecksumAlgorithm::ALL {
        let name = algorithm.header_name();
        if let Some(val) = req.header(name) {
            if let Some(h) = TrailingChecksumHasher::from_trailer_header(name) {
                return Some((h, val.to_string()));
            }
        }
    }
    None
}

/// Return the claimed fixed payload SHA256 (hex) from `x-amz-content-sha256`.
///
/// Excludes UNSIGNED-PAYLOAD and STREAMING-* sentinel values.
fn claimed_payload_sha256_from_request(req: &S3Request) -> Option<String> {
    let value = req.header("x-amz-content-sha256")?;
    if value == "UNSIGNED-PAYLOAD" || value.starts_with("STREAMING-") {
        return None;
    }
    Some(value.to_string())
}

fn sha256_hex_from_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// Create an incremental chunked decoder for the given mode, or None for plain bodies.
pub(super) fn make_chunked_decoder(
    mode: &ChunkedMode,
    streaming_ctx: Option<&auth::StreamingSigningContext>,
) -> Result<Option<super::chunked::IncrementalChunkedDecoder>, ServerError> {
    let decoder = match mode {
        ChunkedMode::None => None,
        ChunkedMode::Signed { expected_len } => Some(
            super::chunked::IncrementalChunkedDecoder::new_with_expected_len(
                Some(streaming_ctx.cloned().ok_or_else(|| ServerError::InternalError {
                    reason: "signed aws-chunked mode is missing its authenticated signing context"
                        .to_string(),
                })?),
                false,
                Some(*expected_len),
            ),
        ),
        ChunkedMode::SignedTrailer { expected_len } => Some(
            super::chunked::IncrementalChunkedDecoder::new_with_expected_len(
                Some(streaming_ctx.cloned().ok_or_else(|| ServerError::InternalError {
                    reason: "signed aws-chunked trailer mode is missing its authenticated signing context"
                        .to_string(),
                })?),
                true,
                Some(*expected_len),
            ),
        ),
        ChunkedMode::UnsignedTrailer { expected_len } => Some(
            super::chunked::IncrementalChunkedDecoder::new_with_expected_len(
                None,
                true,
                Some(*expected_len),
            ),
        ),
    };
    Ok(decoder)
}

/// Acquire a frontend from the pool using round-robin with `try_lock`.
fn acquire_frontend(state: &ServerState) -> Arc<HttpFrontend> {
    let pool_size = state.pool.len();
    let idx = state.counter.fetch_add(1, Ordering::Relaxed) % pool_size;
    Arc::clone(&state.pool[idx])
}

/// Collect an unauthenticated request body with size, idle, and absolute limits.
///
/// The idle timeout resets on each frame, while the absolute timeout bounds the
/// complete pre-authentication exchange even when a client keeps sending.
async fn collect_body_with_limit(
    body: &mut TrackedIncoming,
    idle_timeout: Duration,
    absolute_timeout: Duration,
    max_size: usize,
) -> Result<Bytes, ServerError> {
    let mut limited = Limited::new(body, max_size);
    let mut data = Vec::new();
    let deadline = pre_auth_body_deadline(absolute_timeout);

    loop {
        match await_pre_auth_body_frame(limited.frame(), idle_timeout, deadline).await? {
            // Got a data/trailers frame
            Some(Ok(frame)) => {
                if let Some(chunk) = frame.data_ref() {
                    data.extend_from_slice(chunk);
                }
            }
            // Body stream error (includes size limit exceeded)
            Some(Err(e)) => {
                if e.downcast_ref::<LengthLimitError>().is_some() {
                    return Err(ServerError::MaxMessageLengthExceeded {
                        max_message_length_bytes: max_size,
                    });
                }
                return Err(ServerError::InvalidRequest {
                    reason: "failed to read request body".to_string(),
                });
            }
            // Body complete
            None => break,
        }
    }

    Ok(Bytes::from(data))
}

fn error_response(err: &ServerError, wire_ids: &WireResponseIds) -> S3Response {
    S3Response::error_with_ids(err, "", wire_ids)
}

fn internal_error() -> ServerError {
    ServerError::InternalError {
        reason: "internal error".to_string(),
    }
}

fn internal_error_response(wire_ids: &WireResponseIds) -> S3Response {
    S3Response::error_with_ids(&internal_error(), "", wire_ids)
}

include!("serve/tests.rs");
