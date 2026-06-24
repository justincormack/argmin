/// Async hyper HTTP server loop with frontend pool and backpressure.
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use checksum::{ChecksumAlgorithm, RawChecksum};
use http_body_util::{BodyExt, LengthLimitError, Limited};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::{TokioIo, TokioTimer};
use md5_legacy::Digest;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_rustls::TlsAcceptor;

use super::request::{
    parse_upload_part_query, S3Request, TransportSecurity, MAX_BUFFERED_CONTROL_BODY_SIZE,
};
use super::response::{S3Response, WireResponseIds};
use super::router::{route, S3Operation};
use super::s3_response_to_hyper;
use super::{HttpFrontend, S3HyperBody};
use crate::coordinator::MAX_OBJECT_SIZE;
use crate::error::ServerError;
use server_core::metadata_blob::USER_METADATA_SIZE_LIMIT;
use storage::{BucketName, SessionId};

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
enum ChunkedMode {
    /// Plain HTTP body (Content-Length).
    None,
    /// aws-chunked with per-chunk signatures, no trailers.
    Signed { expected_len: u64 },
    /// aws-chunked with per-chunk signatures + signed trailers.
    SignedTrailer { expected_len: u64 },
    /// aws-chunked unsigned with trailers.
    UnsignedTrailer { expected_len: u64 },
}

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
        part_number: u32,
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
    /// Chunk size used when pulling data from core `ReadHandle`s into the HTTP
    /// response body stream.
    pub stream_read_chunk_size: usize,
    /// Panic instead of returning HTTP 500 responses.
    ///
    /// This is a diagnostic mode for local conformance/stress runs where SDK
    /// retries can otherwise hide transient internal errors.
    pub panic_on_500: bool,
    /// Abort the process instead of returning HTTP 500 responses.
    ///
    /// This is stricter than `panic_on_500`: it makes hidden 500s fail the
    /// whole local test process instead of only dropping one request task.
    pub abort_on_500: bool,
    /// Enable local operator-only diagnostics endpoints.
    ///
    /// The binary config only permits this on loopback listeners. The endpoints
    /// expose bounded counters and an explicit flight-recorder stderr dump
    /// trigger; they do not return request headers, payload bytes, keys, or
    /// recorder details over HTTP.
    pub local_debug_endpoint: bool,
}

impl Default for ServeConfig {
    fn default() -> Self {
        Self {
            header_read_timeout: Duration::from_secs(30),
            request_wait_timeout: Duration::from_secs(5),
            body_idle_timeout: Duration::from_secs(30),
            stream_read_chunk_size: server_core::coordinator::INTERNAL_SEGMENT_SIZE,
            panic_on_500: false,
            abort_on_500: false,
            local_debug_endpoint: false,
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
    segment_buffer_pool: SegmentBufferPool,
    config: ServeConfig,
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
        ctx: Arc<super::StreamingPutContext>,
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
                let state = Arc::clone(&state);
                let ctx = Arc::clone(&ctx);
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
        self.start_put_object_heartbeat(Arc::clone(ctx), session_id.clone());
    }

    fn start_post_object_heartbeat(self: &Arc<Self>, ctx: Arc<super::StreamingPostContext>) {
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
                let state = Arc::clone(&state);
                let ctx = Arc::clone(&ctx);
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
        self.start_post_object_heartbeat(Arc::clone(ctx));
    }

    fn arm_part(&self, ctx: &Arc<super::StreamingPartContext>) {
        let active_session = observability::stream_upload_active_session_guard();
        emit_streaming_part_phase(ctx, "session_created", None, None, None, None);
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
    tls_acceptor: Option<TlsAcceptor>,
) {
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

    let header_read_timeout = config.header_read_timeout;
    let state = Arc::new(ServerState {
        pool: frontends.into_iter().map(Arc::new).collect(),
        host_id,
        counter: AtomicUsize::new(0),
        request_semaphore: Arc::new(Semaphore::new(max_inflight_requests as usize)),
        segment_buffer_pool: SegmentBufferPool::new(max_inflight_requests as usize),
        config,
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

        let (stream, _addr) = match listener.accept().await {
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
        tokio::spawn(async move {
            let _conn_permit = conn_permit;
            match tls_acceptor {
                Some(acceptor) => {
                    match tokio::time::timeout(header_read_timeout, acceptor.accept(stream)).await {
                        Ok(Ok(tls_stream)) => {
                            serve_connection(
                                Arc::clone(&state),
                                TokioIo::new(tls_stream),
                                header_read_timeout,
                                TransportSecurity::Tls,
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
                    )
                    .await;
                }
            }
        });
    }
}

async fn serve_connection<IO>(
    state: Arc<ServerState>,
    io: TokioIo<IO>,
    header_read_timeout: Duration,
    transport_security: TransportSecurity,
) where
    IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let _ = http1::Builder::new()
        .timer(TokioTimer::new())
        .header_read_timeout(header_read_timeout)
        .serve_connection(
            io,
            service_fn(move |req: Request<Incoming>| {
                let state = Arc::clone(&state);
                async move { handle(state, req, transport_security).await }
            }),
        )
        .await;
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
) -> Result<http::Response<S3HyperBody>, Infallible> {
    let trace = crate::http::new_request_trace_context();
    let _trace = observability::AttachedTrace::new(trace.clone());
    let wire_ids = WireResponseIds::new(trace.request_id(), state.host_id.clone());
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();
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

    if state.config.local_debug_endpoint {
        if let Some(resp) = local_debug_response(&state, req.method(), req.uri().path()) {
            return Ok(s3_response_to_hyper(
                resp,
                None,
                state.config.stream_read_chunk_size,
                state.config.panic_on_500,
                state.config.abort_on_500,
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
                    observability::RequestAdmissionSummary {
                        method: response_trace.method.as_str(),
                        path: response_trace.path.as_str(),
                        query: response_trace.query,
                        wait_us,
                        timeout_us: Some(state.config.request_wait_timeout.as_micros()),
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
                observability::RequestAdmissionSummary {
                    method: response_trace.method.as_str(),
                    path: response_trace.path.as_str(),
                    query: response_trace.query,
                    wait_us,
                    timeout_us: Some(state.config.request_wait_timeout.as_micros()),
                },
            );
            let resp = S3Response::error_with_ids(&ServerError::SlowDown, "", &wire_ids);
            return Ok(s3_response_to_hyper(
                resp,
                None,
                state.config.stream_read_chunk_size,
                state.config.panic_on_500,
                state.config.abort_on_500,
                response_trace,
            ));
        }
    };

    let (parts, body) = req.into_parts();

    // Check if this request should use the streaming write path.
    let streaming_op = match is_streaming_write(&parts) {
        Ok(op) => op,
        Err(err) => {
            return Ok(s3_response_to_hyper(
                S3Response::error_with_ids(&err, "", &wire_ids),
                Some(req_permit),
                state.config.stream_read_chunk_size,
                state.config.panic_on_500,
                state.config.abort_on_500,
                response_trace,
            ))
        }
    };
    if let Some(op) = streaming_op {
        let s3req = match S3Request::from_hyper_headers(parts, transport_security) {
            Ok(req) => req,
            Err(err) => {
                return Ok(s3_response_to_hyper(
                    S3Response::error_with_ids(&err, "", &wire_ids),
                    Some(req_permit),
                    state.config.stream_read_chunk_size,
                    state.config.panic_on_500,
                    state.config.abort_on_500,
                    response_trace.clone(),
                ))
            }
        };
        let origin = s3req.header("origin").map(str::to_string);
        let method = s3req.method.as_str().to_string();
        let chunked = match parse_chunked_mode(&s3req) {
            Ok(mode) => mode,
            Err(err) => {
                return Ok(s3_response_to_hyper(
                    S3Response::error_with_ids(&err, "", &wire_ids),
                    Some(req_permit),
                    state.config.stream_read_chunk_size,
                    state.config.panic_on_500,
                    state.config.abort_on_500,
                    response_trace.clone(),
                ))
            }
        };
        let resp = match op {
            StreamingWriteOp::PutObject { bucket, key } => {
                let mut resp = handle_streaming_put(
                    Arc::clone(&state),
                    s3req,
                    body,
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
                    body,
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
        return Ok(s3_response_to_hyper(
            resp,
            Some(req_permit),
            state.config.stream_read_chunk_size,
            state.config.panic_on_500,
            state.config.abort_on_500,
            response_trace,
        ));
    }

    if let Some(bucket) = post_object_bucket(&parts) {
        let s3req = match S3Request::from_hyper_headers(parts, transport_security) {
            Ok(req) => req,
            Err(err) => {
                return Ok(s3_response_to_hyper(
                    S3Response::error_with_ids(&err, "", &wire_ids),
                    Some(req_permit),
                    state.config.stream_read_chunk_size,
                    state.config.panic_on_500,
                    state.config.abort_on_500,
                    response_trace,
                ));
            }
        };
        let origin = s3req.header("origin").map(str::to_string);
        let method = s3req.method.as_str().to_string();
        let mut resp = handle_streaming_post_object(
            Arc::clone(&state),
            s3req,
            body,
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
        return Ok(s3_response_to_hyper(
            resp,
            Some(req_permit),
            state.config.stream_read_chunk_size,
            state.config.panic_on_500,
            state.config.abort_on_500,
            response_trace,
        ));
    }

    // Non-streaming path: collect the full body for buffered control-plane
    // style requests (mostly XML payloads).
    let body_limit = buffered_body_limit_for_request_parts(&parts);
    let body_bytes =
        match collect_body_with_limit(body, state.config.body_idle_timeout, body_limit).await {
            Ok(bytes) => bytes,
            Err(err) => {
                return Ok(s3_response_to_hyper(
                    S3Response::error_with_ids(&err, "", &wire_ids),
                    Some(req_permit),
                    state.config.stream_read_chunk_size,
                    state.config.panic_on_500,
                    state.config.abort_on_500,
                    response_trace,
                ));
            }
        };

    let s3req = match S3Request::from_hyper(parts, body_bytes, transport_security) {
        Ok(req) => req,
        Err(err) => {
            return Ok(s3_response_to_hyper(
                S3Response::error_with_ids(&err, "", &wire_ids),
                Some(req_permit),
                state.config.stream_read_chunk_size,
                state.config.panic_on_500,
                state.config.abort_on_500,
                response_trace,
            ));
        }
    };

    let state_ref = Arc::clone(&state);
    let wire_ids_for_blocking = wire_ids.clone();
    let resp = spawn_blocking_with_trace(trace, move || {
        let frontend = acquire_frontend(&state_ref);
        frontend.handle_s3_request(&s3req, &wire_ids_for_blocking)
    })
    .await
    .unwrap_or_else(|_| {
        S3Response::error_with_ids(
            &ServerError::InvalidRequest {
                reason: "internal error".to_string(),
            },
            "",
            &wire_ids,
        )
    });

    Ok(s3_response_to_hyper(
        resp,
        Some(req_permit),
        state.config.stream_read_chunk_size,
        state.config.panic_on_500,
        state.config.abort_on_500,
        response_trace,
    ))
}

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
            })
        }
        (&http::Method::POST, "/__argmin/debug/flight-recorder/dump") => {
            observability::dump_flight_recorder_to_stderr("local-debug-endpoint");
            Some(S3Response {
                status_code: 204,
                headers: Vec::new(),
                body: Vec::new(),
                stream: None,
                error_diagnostic: None,
            })
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
            })
        }
        _ => None,
    }
}

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
    let mut body = format!(
        concat!(
            "frontend_storage_cluster_epoch {}\n",
            "request_start_total {}\n",
            "inflight_requests {}\n",
            "request_finish_total {}\n",
            "request_error_total {}\n",
            "http_500_response_total {}\n",
            "operation_aborted_response_total {}\n",
            "slow_down_response_total {}\n",
            "slow_request_total {}\n",
            "request_admission_wait_total {}\n",
            "request_admission_wait_us_total {}\n",
            "request_admission_timeout_total {}\n",
            "storage_rpc_error_total {}\n",
            "storage_rpc_admission_total {}\n",
            "storage_rpc_admission_wait_total {}\n",
            "storage_rpc_admission_wait_us_total {}\n",
            "storage_rpc_admission_timeout_total {}\n",
            "storage_rpc_active_total {}\n",
            "storage_rpc_active_control {}\n",
            "storage_rpc_active_completion {}\n",
            "storage_rpc_active_progress {}\n",
            "storage_rpc_active_start_write {}\n",
            "storage_rpc_active_read {}\n",
            "storage_rpc_active_list {}\n",
            "storage_rpc_pending_envelope_active {}\n",
            "storage_rpc_pending_envelope_started_total {}\n",
            "storage_rpc_pending_envelope_completed_total {}\n",
            "storage_rpc_pending_envelope_long_running_total {}\n",
            "storage_rpc_pending_envelope_long_running_us_max {}\n",
            "storage_rpc_pending_envelope_oldest_active_us {}\n",
            "storage_rpc_pending_envelope_oldest_active_node_id {}\n",
            "storage_rpc_pending_envelope_oldest_active_request_id {}\n",
            "shard_scavenger_observation_total {}\n",
            "shard_scavenger_scan_incomplete_total {}\n",
            "metadata_command_conflict_total {}\n",
            "metadata_command_pending_slot_action_total {}\n",
            "metadata_command_session_wait_total {}\n",
            "metadata_command_recovery_leader_total {}\n",
            "metadata_command_recovery_wait_total {}\n",
            "metadata_command_recovery_wait_us_total {}\n",
            "metadata_command_recovery_wait_us_max {}\n",
            "metadata_command_recovery_timeout_total {}\n",
            "metadata_command_recovery_outcome_total {}\n",
            "metadata_command_budget_exhausted_total {}\n",
            "metadata_command_backoff_total {}\n",
            "metadata_command_backoff_us_total {}\n",
            "metadata_command_backoff_us_max {}\n",
            "reclaim_work_queue_depth {}\n",
            "object_payload_reclaim_queue_depth {}\n",
            "object_payload_reclaim_outstanding_depth {}\n",
            "bucket_delete_finalize_queue_depth {}\n",
            "reclaim_work_queue_action_total {}\n",
            "object_payload_reclaim_event_total {}\n",
            "object_payload_reclaim_durable_scan_total {}\n",
            "shard_repair_queue_depth {}\n",
            "shard_repair_event_total {}\n",
            "shard_repair_shards_rewritten_total {}\n",
            "shard_backfill_queue_depth {}\n",
            "shard_backfill_event_total {}\n",
            "shard_backfill_shards_written_total {}\n",
            "shard_backfill_candidate_scan_total {}\n",
            "shard_backfill_candidate_scanned_total {}\n",
            "shard_backfill_candidate_current_epoch_total {}\n",
            "shard_backfill_candidate_already_queued_total {}\n",
            "shard_backfill_candidate_already_complete_total {}\n",
            "shard_backfill_candidate_enqueued_total {}\n",
            "shard_backfill_candidate_unrecoverable_total {}\n",
            "shard_backfill_candidate_deferred_total {}\n",
            "shard_backfill_candidate_failed_total {}\n",
            "shard_backfill_candidate_limit_reached_total {}\n",
            "shard_backfill_candidate_scan_error_total {}\n",
            "metadata_command_checkpoint_record_scan_total {}\n",
            "metadata_command_checkpoint_record_scanned_total {}\n",
            "metadata_command_checkpoint_record_recorded_total {}\n",
            "metadata_command_checkpoint_record_already_current_total {}\n",
            "metadata_command_checkpoint_record_skipped_cadence_total {}\n",
            "metadata_command_checkpoint_record_skipped_inactive_total {}\n",
            "metadata_command_checkpoint_record_skipped_empty_total {}\n",
            "metadata_command_checkpoint_record_skipped_stale_epoch_total {}\n",
            "metadata_command_checkpoint_record_compacted_total {}\n",
            "metadata_command_checkpoint_record_compaction_noop_total {}\n",
            "metadata_command_checkpoint_record_compaction_no_checkpoint_total {}\n",
            "metadata_command_checkpoint_record_compaction_pending_total {}\n",
            "metadata_command_checkpoint_record_compaction_failed_total {}\n",
            "metadata_command_checkpoint_record_failed_total {}\n",
            "metadata_command_checkpoint_record_limit_reached_total {}\n",
            "metadata_command_checkpoint_record_scan_error_total {}\n",
            "background_work_admission_event_total {}\n",
            "background_work_active_total {}\n",
            "background_work_finished_total {}\n",
            "background_work_elapsed_us_total {}\n",
            "stream_upload_active_sessions {}\n",
            "stream_upload_session_created_total {}\n",
            "stream_upload_session_aborted_total {}\n",
            "stream_upload_session_finalized_total {}\n",
            "stream_upload_body_started_total {}\n",
            "stream_upload_body_read_complete_total {}\n",
            "stream_upload_segment_append_started_total {}\n",
            "stream_upload_segment_append_finished_total {}\n",
            "stream_upload_segment_append_error_total {}\n",
            "stream_upload_finalize_error_total {}\n"
        ),
        frontend_storage_cluster_epoch,
        snapshot.request_start_total,
        snapshot.inflight_requests,
        snapshot.request_finish_total,
        snapshot.request_error_total,
        snapshot.http_500_response_total,
        snapshot.operation_aborted_response_total,
        snapshot.slow_down_response_total,
        snapshot.slow_request_total,
        snapshot.request_admission_wait_total,
        snapshot.request_admission_wait_us_total,
        snapshot.request_admission_timeout_total,
        snapshot.storage_rpc_error_total,
        snapshot.storage_rpc_admission_total,
        snapshot.storage_rpc_admission_wait_total,
        snapshot.storage_rpc_admission_wait_us_total,
        snapshot.storage_rpc_admission_timeout_total,
        snapshot.storage_rpc_active_total,
        snapshot.storage_rpc_active_control,
        snapshot.storage_rpc_active_completion,
        snapshot.storage_rpc_active_progress,
        snapshot.storage_rpc_active_start_write,
        snapshot.storage_rpc_active_read,
        snapshot.storage_rpc_active_list,
        snapshot.storage_rpc_pending_envelope_active,
        snapshot.storage_rpc_pending_envelope_started_total,
        snapshot.storage_rpc_pending_envelope_completed_total,
        snapshot.storage_rpc_pending_envelope_long_running_total,
        snapshot.storage_rpc_pending_envelope_long_running_us_max,
        snapshot.storage_rpc_pending_envelope_oldest_active_us,
        snapshot.storage_rpc_pending_envelope_oldest_active_node_id,
        snapshot.storage_rpc_pending_envelope_oldest_active_request_id,
        snapshot.shard_scavenger_observation_total,
        snapshot.shard_scavenger_scan_incomplete_total,
        snapshot.metadata_command_conflict_total,
        snapshot.metadata_command_pending_slot_action_total,
        snapshot.metadata_command_session_wait_total,
        snapshot.metadata_command_recovery_leader_total,
        snapshot.metadata_command_recovery_wait_total,
        snapshot.metadata_command_recovery_wait_us_total,
        snapshot.metadata_command_recovery_wait_us_max,
        snapshot.metadata_command_recovery_timeout_total,
        snapshot.metadata_command_recovery_outcome_total,
        snapshot.metadata_command_budget_exhausted_total,
        snapshot.metadata_command_backoff_total,
        snapshot.metadata_command_backoff_us_total,
        snapshot.metadata_command_backoff_us_max,
        snapshot.reclaim_work_queue_depth,
        snapshot.object_payload_reclaim_queue_depth,
        snapshot.object_payload_reclaim_outstanding_depth,
        snapshot.bucket_delete_finalize_queue_depth,
        snapshot.reclaim_work_queue_action_total,
        snapshot.object_payload_reclaim_event_total,
        snapshot.object_payload_reclaim_durable_scan_total,
        snapshot.shard_repair_queue_depth,
        snapshot.shard_repair_event_total,
        snapshot.shard_repair_shards_rewritten_total,
        snapshot.shard_backfill_queue_depth,
        snapshot.shard_backfill_event_total,
        snapshot.shard_backfill_shards_written_total,
        snapshot.shard_backfill_candidate_scan_total,
        snapshot.shard_backfill_candidate_scanned_total,
        snapshot.shard_backfill_candidate_current_epoch_total,
        snapshot.shard_backfill_candidate_already_queued_total,
        snapshot.shard_backfill_candidate_already_complete_total,
        snapshot.shard_backfill_candidate_enqueued_total,
        snapshot.shard_backfill_candidate_unrecoverable_total,
        snapshot.shard_backfill_candidate_deferred_total,
        snapshot.shard_backfill_candidate_failed_total,
        snapshot.shard_backfill_candidate_limit_reached_total,
        snapshot.shard_backfill_candidate_scan_error_total,
        snapshot.metadata_command_checkpoint_record_scan_total,
        snapshot.metadata_command_checkpoint_record_scanned_total,
        snapshot.metadata_command_checkpoint_record_recorded_total,
        snapshot.metadata_command_checkpoint_record_already_current_total,
        snapshot.metadata_command_checkpoint_record_skipped_cadence_total,
        snapshot.metadata_command_checkpoint_record_skipped_inactive_total,
        snapshot.metadata_command_checkpoint_record_skipped_empty_total,
        snapshot.metadata_command_checkpoint_record_skipped_stale_epoch_total,
        snapshot.metadata_command_checkpoint_record_compacted_total,
        snapshot.metadata_command_checkpoint_record_compaction_noop_total,
        snapshot.metadata_command_checkpoint_record_compaction_no_checkpoint_total,
        snapshot.metadata_command_checkpoint_record_compaction_pending_total,
        snapshot.metadata_command_checkpoint_record_compaction_failed_total,
        snapshot.metadata_command_checkpoint_record_failed_total,
        snapshot.metadata_command_checkpoint_record_limit_reached_total,
        snapshot.metadata_command_checkpoint_record_scan_error_total,
        snapshot.background_work_admission_event_total,
        snapshot.background_work_active_total,
        snapshot.background_work_finished_total,
        snapshot.background_work_elapsed_us_total,
        snapshot.stream_upload_active_sessions,
        snapshot.stream_upload_session_created_total,
        snapshot.stream_upload_session_aborted_total,
        snapshot.stream_upload_session_finalized_total,
        snapshot.stream_upload_body_started_total,
        snapshot.stream_upload_body_read_complete_total,
        snapshot.stream_upload_segment_append_started_total,
        snapshot.stream_upload_segment_append_finished_total,
        snapshot.stream_upload_segment_append_error_total,
        snapshot.stream_upload_finalize_error_total,
    );
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

fn debug_metric_optional_pg_id_label(pg_id: Option<u32>) -> String {
    pg_id.map_or_else(|| "unknown".to_string(), |pg_id| pg_id.to_string())
}

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
        frontend.actual_cors_headers(&bucket, &origin, &method)
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
    if parts.method != http::Method::PUT {
        return Ok(None);
    }

    // Check headers via hyper types (not yet parsed into S3Request).
    let has_copy_source = parts.headers.contains_key("x-amz-copy-source");
    if has_copy_source {
        return Ok(None);
    }

    let path = parts.uri.path();
    let query = parts.uri.query().unwrap_or("");
    let method = parts.method.as_str();

    let Some(op) = route(method, path, query).ok() else {
        return Ok(None);
    };
    match op {
        S3Operation::PutObject { bucket, key } => {
            Ok(Some(StreamingWriteOp::PutObject { bucket, key }))
        }
        S3Operation::UploadPart { bucket, key } => {
            let (upload_id, part_number) = parse_upload_part_query(query)?;
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

fn buffered_body_limit_for_request_parts(parts: &http::request::Parts) -> usize {
    let path = parts.uri.path();
    let query = parts.uri.query().unwrap_or("");
    let method = parts.method.as_str();
    let Ok(op) = route(method, path, query) else {
        return MAX_BUFFERED_CONTROL_BODY_SIZE;
    };
    buffered_body_limit_for_operation(&op)
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
fn parse_chunked_mode(req: &S3Request) -> Result<ChunkedMode, ServerError> {
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
        v if v.starts_with("STREAMING-") => Err(ServerError::InvalidArgument {
            reason: format!("unsupported streaming token: {v}"),
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
    ) else {
        return;
    };

    let _ = crate::http::enforce_sigv4_time_skew(&req, 1_700_000_000);
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
        let Ok(req) = S3Request::from_hyper_headers(parts, transport_security) else {
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

async fn handle_streaming_post_object(
    state: Arc<ServerState>,
    s3req: S3Request,
    body: Incoming,
    bucket: BucketName,
    trace: observability::TraceContext,
    wire_ids: WireResponseIds,
) -> S3Response {
    let idle_timeout = state.config.body_idle_timeout;
    let error_response = |err: &ServerError| S3Response::error_with_ids(err, "", &wire_ids);
    let internal_error_response = || {
        S3Response::error_with_ids(
            &ServerError::InvalidRequest {
                reason: "internal error".to_string(),
            },
            "",
            &wire_ids,
        )
    };

    let req_arc = Arc::new(s3req);

    let content_type = match req_arc.header("content-type") {
        Some(v) => v,
        None => {
            // AWS returns 412 for missing/wrong Content-Type on POST Object.
            return error_response(&ServerError::PreconditionFailed);
        }
    };
    // Check if this is actually multipart/form-data before looking for boundary.
    // AWS returns 412 for wrong content-type, 400 for missing boundary.
    let is_multipart = content_type
        .split(';')
        .next()
        .is_some_and(|t| t.trim().eq_ignore_ascii_case("multipart/form-data"));
    if !is_multipart {
        return error_response(&ServerError::PreconditionFailed);
    }
    let boundary = match super::multipart::extract_boundary(content_type) {
        Some(b) => b,
        None => {
            return error_response(&ServerError::MalformedPOSTRequest {
                reason: "The body of your POST request is not well-formed multipart/form-data."
                    .to_string(),
            });
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

    let mut body = body;
    loop {
        match tokio::time::timeout(idle_timeout, body.frame()).await {
            Ok(Some(Ok(frame))) => {
                if let Some(chunk) = frame.data_ref() {
                    let events = match parser.feed(chunk) {
                        Ok(v) => v,
                        Err(err) => {
                            if let Some(ref c) = ctx {
                                abort_streaming_post_object(&state, c).await;
                            }
                            return error_response(&err);
                        }
                    };
                    for event in events {
                        match event {
                            PostMultipartEvent::Field { name, value } => {
                                if seen_file {
                                    if let Some(ref c) = ctx {
                                        abort_streaming_post_object(&state, c).await;
                                    }
                                    return error_response(&ServerError::InvalidRequest {
                                        reason: "file field must be the final multipart part"
                                            .to_string(),
                                    });
                                }
                                if let Err(err) = field_budget.record(&name, &value) {
                                    if let Some(ref c) = ctx {
                                        abort_streaming_post_object(&state, c).await;
                                    }
                                    return error_response(&err);
                                }
                                fields.push((name, value));
                            }
                            PostMultipartEvent::FileStart { file_name } => {
                                if seen_file {
                                    if let Some(ref c) = ctx {
                                        abort_streaming_post_object(&state, c).await;
                                    }
                                    return error_response(&ServerError::InvalidRequest {
                                        reason: "multiple file fields are not supported"
                                            .to_string(),
                                    });
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
                                            error_response(&err),
                                            &mut body,
                                            idle_timeout,
                                        )
                                        .await;
                                    }
                                    Err(_) => return internal_error_response(),
                                }
                            }
                            PostMultipartEvent::FileChunk(data) => {
                                let Some(ref c) = ctx else {
                                    return error_response(&ServerError::InvalidRequest {
                                        reason: "missing file field in multipart form".to_string(),
                                    });
                                };
                                crc64.update(&data);
                                if let Some(hasher) = post_checksum_hasher.as_mut() {
                                    hasher.update(&data);
                                }
                                total_size += data.len() as u64;
                                if total_size > MAX_OBJECT_SIZE {
                                    abort_streaming_post_object(&state, c).await;
                                    return finish_streaming_post_rejection(
                                        error_response(&ServerError::ObjectTooLarge {
                                            size: total_size,
                                            max: MAX_OBJECT_SIZE,
                                        }),
                                        &mut body,
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
                                        error_response(&ServerError::InvalidRequest {
                                            reason: auth::PostPolicyError::ConditionFailed {
                                                condition: "content-length-range",
                                                field: None,
                                            }
                                            .to_string(),
                                        }),
                                        &mut body,
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
                                            return error_response(&err);
                                        }
                                        Err(_) => {
                                            abort_streaming_post_object(&state, c).await;
                                            return internal_error_response();
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
                return error_response(&ServerError::InvalidRequest {
                    reason: "failed to read request body".to_string(),
                });
            }
            Ok(None) => break,
            Err(_) => {
                if let Some(ref c) = ctx {
                    abort_streaming_post_object(&state, c).await;
                }
                return error_response(&ServerError::InvalidRequest {
                    reason: "request body read timed out".to_string(),
                });
            }
        }
    }

    if !parser.is_done() {
        if let Some(ref c) = ctx {
            abort_streaming_post_object(&state, c).await;
        }
        return error_response(&ServerError::IncompleteBody);
    }
    if !seen_file {
        return error_response(&ServerError::InvalidRequest {
            reason: "missing file field in multipart form".to_string(),
        });
    }
    if !file_ended {
        if let Some(ref c) = ctx {
            abort_streaming_post_object(&state, c).await;
        }
        return error_response(&ServerError::IncompleteBody);
    }

    let Some(ctx) = ctx else {
        return error_response(&ServerError::InvalidRequest {
            reason: "missing file field in multipart form".to_string(),
        });
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
                return error_response(&err);
            }
            Err(_) => {
                abort_streaming_post_object(&state, &ctx).await;
                return internal_error_response();
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
            error_response(&err)
        }
        Err(_) => {
            abort_streaming_post_object(&state, &ctx).await;
            internal_error_response()
        }
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
    body: Incoming,
    bucket: BucketName,
    key: String,
    chunked: ChunkedMode,
    trace: observability::TraceContext,
    wire_ids: WireResponseIds,
) -> S3Response {
    let idle_timeout = state.config.body_idle_timeout;
    let error_response = |err: &ServerError| S3Response::error_with_ids(err, "", &wire_ids);
    let internal_error_response = || {
        S3Response::error_with_ids(
            &ServerError::InvalidRequest {
                reason: "internal error".to_string(),
            },
            "",
            &wire_ids,
        )
    };
    let mut body = body;

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
                error_response(&err),
                &err,
                has_auth_attempt,
                &mut body,
                idle_timeout,
            )
            .await
        }
        Err(_) => {
            return finish_streaming_prepare_failure(
                internal_error_response(),
                &ServerError::InvalidRequest {
                    reason: "internal error".to_string(),
                },
                has_auth_attempt,
                &mut body,
                idle_timeout,
            )
            .await
        }
    };
    // Build chunked decoder if needed.
    let mut decoder = make_chunked_decoder(&chunked, ctx.streaming_signing.as_ref());
    let mut payload_sha256_hasher = claimed_payload_sha256
        .as_ref()
        .map(|_| ring::digest::Context::new(&ring::digest::SHA256));
    let mut content_md5_hasher = ctx.checksum.content_md5.map(|_| md5_legacy::Md5::new());

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
        let next_frame = tokio::time::timeout(idle_timeout, body.frame()).await;
        body_timing.frame_wait_us += elapsed_micros(frame_wait_start);
        match next_frame {
            Ok(Some(Ok(frame))) => {
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
                                    return error_response(&err);
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
            Ok(Some(Err(_))) => {
                abort_streaming(&state, &ctx, session_id.clone()).await;
                return error_response(&ServerError::InvalidRequest {
                    reason: "failed to read request body".to_string(),
                });
            }
            Ok(None) => break, // Body complete
            Err(_) => {
                abort_streaming(&state, &ctx, session_id.clone()).await;
                return error_response(&ServerError::InvalidRequest {
                    reason: "request body read timed out".to_string(),
                });
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
            return error_response(&ServerError::IncompleteBody);
        }
        let trailers = dec.into_trailers();
        if let Err(err) = validate_chunked_post_decode(
            &chunked,
            total_size,
            &trailers,
            declared_trailer.as_deref(),
        ) {
            abort_streaming(&state, &ctx, session_id.clone()).await;
            return error_response(&err);
        }
        match extract_checksum_trailers(&trailers) {
            Ok(tc) => trailer_checksums = tc,
            Err(err) => {
                abort_streaming(&state, &ctx, session_id.clone()).await;
                return error_response(&err);
            }
        }
    }

    if let (Some(claimed), Some(h)) = (claimed_payload_sha256.as_ref(), payload_sha256_hasher) {
        let actual = sha256_hex_from_digest(h.finish().as_ref());
        if &actual != claimed {
            abort_streaming(&state, &ctx, session_id.clone()).await;
            return error_response(&ServerError::XAmzContentSHA256Mismatch {
                client_hash: claimed.clone(),
                server_hash: actual,
            });
        }
    }

    if let (Some(claim), Some(hasher)) = (ctx.checksum.content_md5, content_md5_hasher.take()) {
        let actual = hasher.finalize();
        let mut actual_bytes = [0u8; 16];
        actual_bytes.copy_from_slice(actual.as_ref());
        if let Err(err) = claim.verify(&actual_bytes) {
            abort_streaming(&state, &ctx, session_id.clone()).await;
            return error_response(&err);
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
                return error_response(&ServerError::ChecksumDigestMismatch { algorithm });
            }
        } else {
            // Trailing checksum: exactly one trailer expected.
            match trailer_checksums.len() {
                1 => {
                    if trailer_checksums[0].1 != actual_b64 {
                        abort_streaming(&state, &ctx, session_id.clone()).await;
                        return error_response(&ServerError::ChecksumDigestMismatch { algorithm });
                    }
                }
                0 => {} // No checksum trailer in body — nothing to validate.
                _ => {
                    // Multiple distinct checksum trailers — reject.
                    abort_streaming(&state, &ctx, session_id.clone()).await;
                    return error_response(&ServerError::InvalidRequest {
                        reason: "multiple checksum trailers not supported".to_string(),
                    });
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
            Ok(Err(err)) => error_response(&err),
            Err(_) => internal_error_response(),
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
            error_response(&err)
        }
        Err(_) => {
            abort_streaming(&state, &ctx, Some(active_session_id)).await;
            internal_error_response()
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
    payload_sha256_hasher: &'a mut Option<ring::digest::Context>,
    content_md5_hasher: &'a mut Option<md5_legacy::Md5>,
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

async fn finish_streaming_prepare_failure(
    resp: S3Response,
    err: &ServerError,
    has_auth_attempt: bool,
    body: &mut Incoming,
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
    body: &mut Incoming,
    idle_timeout: Duration,
) -> S3Response {
    finish_streaming_rejection_bounded(resp, body, idle_timeout).await
}

async fn finish_streaming_rejection_bounded(
    resp: S3Response,
    body: &mut Incoming,
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
    body: &mut Incoming,
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
    body: Incoming,
    bucket: BucketName,
    key: String,
    upload_id: String,
    part_number: u32,
    chunked: ChunkedMode,
    trace: observability::TraceContext,
    wire_ids: WireResponseIds,
) -> S3Response {
    let idle_timeout = state.config.body_idle_timeout;
    let error_response = |err: &ServerError| S3Response::error_with_ids(err, "", &wire_ids);
    let internal_error_response = || {
        S3Response::error_with_ids(
            &ServerError::InvalidRequest {
                reason: "internal error".to_string(),
            },
            "",
            &wire_ids,
        )
    };
    let mut body = body;
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
            part_number,
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
                error_response(&err),
                &err,
                has_auth_attempt,
                &mut body,
                idle_timeout,
            )
            .await
        }
        Err(_) => {
            return finish_streaming_prepare_failure(
                internal_error_response(),
                &ServerError::InvalidRequest {
                    reason: "internal error".to_string(),
                },
                has_auth_attempt,
                &mut body,
                idle_timeout,
            )
            .await
        }
    };
    if trailing_hasher.is_none() {
        if let Some(upload_algorithm) = ctx.checksum.upload_checksum_algorithm {
            trailing_hasher = Some(TrailingChecksumHasher::from_algorithm(upload_algorithm));
        }
    }

    // Build chunked decoder if needed.
    let mut decoder = make_chunked_decoder(&chunked, ctx.streaming_signing.as_ref());
    let mut payload_sha256_hasher = claimed_payload_sha256
        .as_ref()
        .map(|_| ring::digest::Context::new(&ring::digest::SHA256));
    let mut content_md5_hasher = ctx.checksum.content_md5.map(|_| md5_legacy::Md5::new());
    // 2. Stream body frames, accumulating into internal segment-sized buffers.
    let mut hasher = checksum::crc64::Hasher::new();
    let mut segment_index: u32 = 0;
    let mut buf = PooledSegmentBuffer::new(&state);
    let mut total_size: u64 = 0;
    let mut body_started_emitted = false;
    let mut body_timing = StreamingBodyTiming::default();
    loop {
        let frame_wait_start = Instant::now();
        let next_frame = tokio::time::timeout(idle_timeout, body.frame()).await;
        body_timing.frame_wait_us += elapsed_micros(frame_wait_start);
        match next_frame {
            Ok(Some(Ok(frame))) => {
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
                                    return error_response(&err);
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
            Ok(Some(Err(_))) => {
                abort_streaming_part_ctx(&state, &ctx).await;
                return error_response(&ServerError::InvalidRequest {
                    reason: "failed to read request body".to_string(),
                });
            }
            Ok(None) => break,
            Err(_) => {
                abort_streaming_part_ctx(&state, &ctx).await;
                return error_response(&ServerError::InvalidRequest {
                    reason: "request body read timed out".to_string(),
                });
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
        "body_read_complete",
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
            return error_response(&ServerError::IncompleteBody);
        }
        let trailers = dec.into_trailers();
        if let Err(err) = validate_chunked_post_decode(
            &chunked,
            total_size,
            &trailers,
            declared_trailer.as_deref(),
        ) {
            abort_streaming_part_ctx(&state, &ctx).await;
            return error_response(&err);
        }
        match extract_checksum_trailers(&trailers) {
            Ok(tc) => trailer_checksums = tc,
            Err(err) => {
                abort_streaming_part_ctx(&state, &ctx).await;
                return error_response(&err);
            }
        }
    }

    if let (Some(claimed), Some(h)) = (claimed_payload_sha256.as_ref(), payload_sha256_hasher) {
        let actual = sha256_hex_from_digest(h.finish().as_ref());
        if &actual != claimed {
            abort_streaming_part_ctx(&state, &ctx).await;
            return error_response(&ServerError::XAmzContentSHA256Mismatch {
                client_hash: claimed.clone(),
                server_hash: actual,
            });
        }
    }

    if let (Some(claim), Some(hasher)) = (ctx.checksum.content_md5, content_md5_hasher.take()) {
        let actual = hasher.finalize();
        let mut actual_bytes = [0u8; 16];
        actual_bytes.copy_from_slice(actual.as_ref());
        if let Err(err) = claim.verify(&actual_bytes) {
            abort_streaming_part_ctx(&state, &ctx).await;
            return error_response(&err);
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
                return error_response(&ServerError::ChecksumDigestMismatch { algorithm });
            }
        } else {
            // Trailing checksum: validate if present.
            match trailer_checksums.len() {
                1 => {
                    if trailer_checksums[0].1 != actual_b64 {
                        abort_streaming_part_ctx(&state, &ctx).await;
                        return error_response(&ServerError::ChecksumDigestMismatch { algorithm });
                    }
                }
                0 => {}
                _ => {
                    abort_streaming_part_ctx(&state, &ctx).await;
                    return error_response(&ServerError::InvalidRequest {
                        reason: "multiple checksum trailers not supported".to_string(),
                    });
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
            "segment_append_started",
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
                    "segment_append_finished",
                    Some(idx),
                    Some(total_size),
                    None,
                    None,
                );
            }
            Ok((Err(err), _buf)) => {
                emit_streaming_part_phase(
                    &ctx,
                    "segment_append_error",
                    Some(idx),
                    Some(total_size),
                    None,
                    None,
                );
                abort_streaming_part_ctx(&state, &ctx).await;
                return error_response(&err);
            }
            Err(_) => {
                emit_streaming_part_phase(
                    &ctx,
                    "segment_append_error",
                    Some(idx),
                    Some(total_size),
                    None,
                    None,
                );
                abort_streaming_part_ctx(&state, &ctx).await;
                return internal_error_response();
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
        "finalize_started",
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
                "session_finalized",
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
                "finalize_error",
                None,
                Some(total_size),
                None,
                Some(segment_index + u32::from(had_tail)),
            );
            abort_streaming_part_ctx(&state, &ctx).await;
            error_response(&err)
        }
        Err(_) => {
            emit_streaming_part_phase(
                &ctx,
                "finalize_error",
                None,
                Some(total_size),
                None,
                Some(segment_index + u32::from(had_tail)),
            );
            abort_streaming_part_ctx(&state, &ctx).await;
            internal_error_response()
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
    payload_sha256_hasher: &'a mut Option<ring::digest::Context>,
    content_md5_hasher: &'a mut Option<md5_legacy::Md5>,
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
    phase: &'static str,
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
            "body_started",
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
            "segment_append_started",
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
                    "segment_append_finished",
                    Some(idx),
                    Some(*ingest.total_size),
                    Some(segment_bytes),
                    None,
                );
            }
            Ok((Err(err), _flush_data)) => {
                emit_streaming_part_phase(
                    ctx,
                    "segment_append_error",
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
                    "segment_append_error",
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
fn validate_chunked_post_decode(
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
fn make_chunked_decoder(
    mode: &ChunkedMode,
    streaming_ctx: Option<&auth::StreamingSigningContext>,
) -> Option<super::chunked::IncrementalChunkedDecoder> {
    match mode {
        ChunkedMode::None => None,
        ChunkedMode::Signed { expected_len } => Some(
            super::chunked::IncrementalChunkedDecoder::new_with_expected_len(
                streaming_ctx.cloned(),
                false,
                Some(*expected_len),
            ),
        ),
        ChunkedMode::SignedTrailer { expected_len } => Some(
            super::chunked::IncrementalChunkedDecoder::new_with_expected_len(
                streaming_ctx.cloned(),
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
    }
}

/// Acquire a frontend from the pool using round-robin with `try_lock`.
fn acquire_frontend(state: &ServerState) -> Arc<HttpFrontend> {
    let pool_size = state.pool.len();
    let idx = state.counter.fetch_add(1, Ordering::Relaxed) % pool_size;
    Arc::clone(&state.pool[idx])
}

/// Collect a request body with size limiting and per-frame idle timeout.
///
/// Each call to `frame()` is individually wrapped in a timeout that resets on
/// every chunk. A client sending data steadily (even slowly) will never be
/// timed out; only truly stalled connections are killed.
async fn collect_body_with_limit(
    body: Incoming,
    idle_timeout: Duration,
    max_size: usize,
) -> Result<Bytes, ServerError> {
    let mut limited = Limited::new(body, max_size);
    let mut data = Vec::new();

    loop {
        match tokio::time::timeout(idle_timeout, limited.frame()).await {
            // Got a data/trailers frame
            Ok(Some(Ok(frame))) => {
                if let Some(chunk) = frame.data_ref() {
                    data.extend_from_slice(chunk);
                }
            }
            // Body stream error (includes size limit exceeded)
            Ok(Some(Err(e))) => {
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
            Ok(None) => break,
            // No data frame within BODY_IDLE_TIMEOUT
            Err(_) => {
                return Err(ServerError::InvalidRequest {
                    reason: "request body read timed out".to_string(),
                });
            }
        }
    }

    Ok(Bytes::from(data))
}

fn error_response(err: &ServerError, wire_ids: &WireResponseIds) -> S3Response {
    S3Response::error_with_ids(err, "", wire_ids)
}

fn internal_error_response(wire_ids: &WireResponseIds) -> S3Response {
    S3Response::error_with_ids(
        &ServerError::InvalidRequest {
            reason: "internal error".to_string(),
        },
        "",
        wire_ids,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream as StdTcpStream};
    use std::panic::AssertUnwindSafe;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use auth::canonical::{
        canonical_headers, canonical_query_string, canonical_request, sha256_hex, string_to_sign,
    };
    use auth::sigv4::derive_signing_key;
    use hyper_util::rt::TokioIo;
    use ring::hmac;
    use server_core::sse::{ManagedWrappingKeyConfig, StaticManagedKeyProvider};
    use storage::{NodeId, StorageCluster};

    use crate::metadata_blob::MetadataBlob;

    const TEST_ACCESS_KEY: &str = "AKID";
    const TEST_SECRET_KEY: &str = "test-secret";
    const TEST_SSE_S3_WRAPPING_KEY_B64: &str = "YWJjZGVmMDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODk=";

    fn open_test_storage_cluster(dir: &std::path::Path, pg_ids: &[u32]) -> Arc<StorageCluster> {
        let ec_config = ec::EcConfig::default();
        let ec_shape = storage::EcShape {
            k: ec_config.data_shards,
            m: ec_config.parity_shards,
        };
        let node_count = u32::from(ec_shape.k) + u32::from(ec_shape.m);
        let node_ids: Vec<NodeId> = (0..node_count).map(NodeId::new).collect();
        StorageCluster::open_local_nodes(dir, &node_ids, pg_ids, ec_shape)
            .expect("open local storage cluster")
    }

    struct ServerGuard(tokio::task::JoinHandle<()>);

    impl Drop for ServerGuard {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    #[test]
    fn segment_buffer_pool_recovers_from_poisoned_lock() {
        let pool = SegmentBufferPool::new(4);
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = pool.cached.lock().unwrap();
            panic!("poison segment buffer pool");
        }));

        let mut buf = pool.checkout();
        buf.extend_from_slice(b"data");
        pool.recycle(buf);

        let recycled = pool.checkout();
        assert!(recycled.capacity() >= crate::coordinator::INTERNAL_SEGMENT_SIZE);
        assert!(recycled.is_empty());
    }

    #[test]
    fn buffered_body_limits_use_operation_specific_caps() {
        let parts = make_parts("PUT", "/bucket?encryption", &[]);
        assert_eq!(
            buffered_body_limit_for_request_parts(&parts),
            MAX_BUCKET_ENCRYPTION_CONFIGURATION_BYTES
        );

        let parts = make_parts("POST", "/bucket/key?uploadId=upload-id", &[]);
        assert_eq!(
            buffered_body_limit_for_request_parts(&parts),
            MAX_COMPLETE_MULTIPART_UPLOAD_XML_BYTES
        );

        let parts = make_parts("GET", "/bucket/key", &[]);
        assert_eq!(
            buffered_body_limit_for_request_parts(&parts),
            MAX_BUFFERED_CONTROL_BODY_SIZE
        );
    }

    /// Build a minimal `http::request::Parts` for testing `is_streaming_write`.
    fn make_parts(method: &str, uri: &str, headers: &[(&str, &str)]) -> http::request::Parts {
        let mut builder = http::Request::builder().method(method).uri(uri);
        for (k, v) in headers {
            builder = builder.header(*k, *v);
        }
        let (parts, _body) = builder.body(()).unwrap().into_parts();
        parts
    }

    fn make_s3req(method: &str, uri: &str, headers: &[(&str, &str)]) -> S3Request {
        S3Request::from_hyper_headers(make_parts(method, uri, headers), TransportSecurity::Tls)
            .unwrap()
    }

    fn setup_frontend(dir: &std::path::Path) -> Arc<HttpFrontend> {
        let pg_ids: Vec<u32> = (0..1).collect();
        let storage_cluster = open_test_storage_cluster(dir, &pg_ids);
        let sse_s3_provider = StaticManagedKeyProvider::single(
            ManagedWrappingKeyConfig::from_base64(1, TEST_SSE_S3_WRAPPING_KEY_B64).unwrap(),
        );
        let coordinator =
            server_core::coordinator::Coordinator::new_with_managed_key_provider_for_storage_cluster(
            storage_cluster,
            "us-east-1".to_string(),
            None,
            sse_s3_provider,
        )
        .unwrap();
        let mut credentials = auth::CredentialStore::new();
        credentials.add(
            TEST_ACCESS_KEY.to_string(),
            auth::SecretKey::new(TEST_SECRET_KEY.to_string()),
        );
        Arc::new(HttpFrontend {
            coordinator: Arc::new(coordinator),
            credentials,
            host_id: Arc::<str>::from("host-id"),
        })
    }

    fn create_test_bucket(frontend: &HttpFrontend, bucket: &str) {
        let requester = server_core::coordinator::test_helpers::requester(TEST_ACCESS_KEY);
        frontend
            .coordinator
            .create_bucket(&crate::coordinator::CreateBucketRequest {
                name: storage::BucketName::try_from(bucket.to_string()).unwrap(),
                requester,
                namespace: s3_types::BucketNamespace::Global,
                acl: crate::coordinator::CreateBucketAcl::DefaultPrivate,
                ownership: crate::coordinator::BucketObjectOwnership::ObjectWriter,
                object_lock_enabled: false,
            })
            .unwrap();
    }

    fn create_test_bucket_and_upload(frontend: &HttpFrontend, bucket: &str, key: &str) -> String {
        create_test_bucket(frontend, bucket);
        let requester = server_core::coordinator::test_helpers::requester(TEST_ACCESS_KEY);
        frontend
            .coordinator
            .create_multipart_upload(&crate::coordinator::CreateMultipartUploadRequest {
                object: crate::coordinator::ObjectRequest::new(
                    storage::BucketName::try_from(bucket.to_string()).unwrap(),
                    storage::ObjectKey::try_from(key.to_string()).unwrap(),
                    requester,
                    None,
                ),
                metadata: &MetadataBlob::new(),
                system_metadata: &server_core::system_metadata::SystemMetadata::EMPTY,
                tags: None,
                checksum: None,
                acl: crate::coordinator::PutObjectAcl::None.into(),
                policy_context: crate::coordinator::PutObjectPolicyContext::default(),
                object_lock: s3_types::ObjectLockState::default(),
                encryption: crate::coordinator::WriteEncryptionRequest::none(),
            })
            .unwrap()
            .upload_id
            .to_string()
    }

    async fn start_test_server(frontend: Arc<HttpFrontend>) -> (String, ServerGuard) {
        start_test_server_with_config(frontend, ServeConfig::default(), 8).await
    }

    async fn start_test_server_with_config(
        frontend: Arc<HttpFrontend>,
        config: ServeConfig,
        request_slots: usize,
    ) -> (String, ServerGuard) {
        let std_listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = std_listener.local_addr().unwrap().to_string();
        std_listener.set_nonblocking(true).unwrap();
        let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();

        let header_read_timeout = config.header_read_timeout;
        let host_id = frontend.host_id.clone();
        let segment_buffer_pool_slots = request_slots.max(1);
        let state = Arc::new(ServerState {
            pool: vec![frontend],
            host_id,
            counter: AtomicUsize::new(0),
            request_semaphore: Arc::new(tokio::sync::Semaphore::new(request_slots)),
            segment_buffer_pool: SegmentBufferPool::new(segment_buffer_pool_slots),
            config,
        });

        let handle = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.expect("accept");
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    serve_connection(
                        state,
                        TokioIo::new(stream),
                        header_read_timeout,
                        TransportSecurity::InsecureHttp,
                    )
                    .await;
                });
            }
        });

        (addr, ServerGuard(handle))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_debug_endpoint_is_disabled_by_default() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let (addr, _guard) = start_test_server(frontend).await;

        let mut stream = StdTcpStream::connect(&addr).unwrap();
        stream
            .write_all(
                concat!(
                    "GET /__argmin/debug/metrics HTTP/1.1\r\n",
                    "Host: localhost\r\n",
                    "Connection: close\r\n",
                    "\r\n",
                )
                .as_bytes(),
            )
            .unwrap();
        let response = read_http_response(&mut stream, Duration::from_secs(3));

        assert!(
            !response.starts_with("HTTP/1.1 200"),
            "debug endpoint unexpectedly enabled by default: {response}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_debug_metrics_endpoint_bypasses_request_admission() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let config = ServeConfig {
            local_debug_endpoint: true,
            ..ServeConfig::default()
        };
        let (addr, _guard) = start_test_server_with_config(frontend, config, 0).await;

        let mut stream = StdTcpStream::connect(&addr).unwrap();
        stream
            .write_all(
                concat!(
                    "GET /__argmin/debug/metrics HTTP/1.1\r\n",
                    "Host: localhost\r\n",
                    "Connection: close\r\n",
                    "\r\n",
                )
                .as_bytes(),
            )
            .unwrap();
        let response = read_http_response(&mut stream, Duration::from_secs(3));

        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response
            .to_ascii_lowercase()
            .contains("content-type: text/plain; charset=utf-8"));
        assert!(response.contains("metadata_command_conflict_total "));
        assert!(response.contains("storage_rpc_error_total "));
        assert!(response.contains("storage_rpc_admission_total "));
        assert!(response.contains("storage_rpc_admission_wait_total "));
        assert!(response.contains("storage_rpc_admission_wait_us_total "));
        assert!(response.contains("storage_rpc_admission_timeout_total "));
        assert!(response.contains("metadata_command_budget_exhausted_total "));
        assert!(response.contains("metadata_command_backoff_total "));
        assert!(response.contains("metadata_command_backoff_us_total "));
        assert!(response.contains("metadata_command_backoff_us_max "));
        assert!(response.contains("shard_repair_queue_depth "));
        assert!(response.contains("shard_repair_event_total "));
        assert!(response.contains("shard_repair_shards_rewritten_total "));
        assert!(response.contains("shard_backfill_queue_depth "));
        assert!(response.contains("shard_backfill_event_total "));
        assert!(response.contains("shard_backfill_shards_written_total "));
        assert!(response.contains("shard_backfill_candidate_scan_total "));
        assert!(response.contains("shard_backfill_candidate_scanned_total "));
        assert!(response.contains("shard_backfill_candidate_current_epoch_total "));
        assert!(response.contains("shard_backfill_candidate_already_queued_total "));
        assert!(response.contains("shard_backfill_candidate_already_complete_total "));
        assert!(response.contains("shard_backfill_candidate_enqueued_total "));
        assert!(response.contains("shard_backfill_candidate_unrecoverable_total "));
        assert!(response.contains("shard_backfill_candidate_deferred_total "));
        assert!(response.contains("shard_backfill_candidate_failed_total "));
        assert!(response.contains("shard_backfill_candidate_limit_reached_total "));
        assert!(response.contains("shard_backfill_candidate_scan_error_total "));
        assert!(response.contains("metadata_command_checkpoint_record_scan_total "));
        assert!(response.contains("metadata_command_checkpoint_record_scanned_total "));
        assert!(response.contains("metadata_command_checkpoint_record_recorded_total "));
        assert!(response.contains("metadata_command_checkpoint_record_already_current_total "));
        assert!(response.contains("metadata_command_checkpoint_record_skipped_cadence_total "));
        assert!(response.contains("metadata_command_checkpoint_record_skipped_inactive_total "));
        assert!(response.contains("metadata_command_checkpoint_record_skipped_empty_total "));
        assert!(response.contains("metadata_command_checkpoint_record_skipped_stale_epoch_total "));
        assert!(response.contains("metadata_command_checkpoint_record_compacted_total "));
        assert!(response.contains("metadata_command_checkpoint_record_compaction_noop_total "));
        assert!(
            response.contains("metadata_command_checkpoint_record_compaction_no_checkpoint_total ")
        );
        assert!(response.contains("metadata_command_checkpoint_record_compaction_pending_total "));
        assert!(response.contains("metadata_command_checkpoint_record_compaction_failed_total "));
        assert!(response.contains("metadata_command_checkpoint_record_failed_total "));
        assert!(response.contains("metadata_command_checkpoint_record_limit_reached_total "));
        assert!(response.contains("metadata_command_checkpoint_record_scan_error_total "));
        assert!(response.contains("request_admission_wait_total "));
        assert!(response.contains("request_admission_timeout_total "));
        assert!(!response.contains("bucket_lock_wait_exceeded_total "));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_debug_flight_recorder_dump_endpoint_is_explicit_post_only() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let config = ServeConfig {
            local_debug_endpoint: true,
            ..ServeConfig::default()
        };
        let (addr, _guard) = start_test_server_with_config(frontend, config, 0).await;

        let mut stream = StdTcpStream::connect(&addr).unwrap();
        stream
            .write_all(
                concat!(
                    "GET /__argmin/debug/flight-recorder/dump HTTP/1.1\r\n",
                    "Host: localhost\r\n",
                    "Connection: close\r\n",
                    "\r\n",
                )
                .as_bytes(),
            )
            .unwrap();
        let response = read_http_response(&mut stream, Duration::from_secs(3));
        assert!(response.starts_with("HTTP/1.1 404"), "{response}");

        let mut stream = StdTcpStream::connect(&addr).unwrap();
        stream
            .write_all(
                concat!(
                    "POST /__argmin/debug/flight-recorder/dump HTTP/1.1\r\n",
                    "Host: localhost\r\n",
                    "Content-Length: 0\r\n",
                    "Connection: close\r\n",
                    "\r\n",
                )
                .as_bytes(),
            )
            .unwrap();
        let response = read_http_response(&mut stream, Duration::from_secs(3));
        assert!(response.starts_with("HTTP/1.1 204"), "{response}");
    }

    fn response_body_complete(buf: &[u8], header_end: usize, headers: &str) -> bool {
        let body_start = header_end + 4;
        if let Some(content_length) = headers.lines().find_map(|line| {
            let lower = line.to_lowercase();
            lower
                .strip_prefix("content-length: ")
                .map(|v| v.trim().parse::<usize>().unwrap_or(0))
        }) {
            return buf.len() >= body_start + content_length;
        }

        let is_chunked = headers.lines().any(|line| {
            let lower = line.to_lowercase();
            lower
                .strip_prefix("transfer-encoding: ")
                .is_some_and(|value| value.split(',').any(|part| part.trim() == "chunked"))
        });
        if !is_chunked {
            return body_start == buf.len();
        }

        let mut offset = body_start;
        while offset < buf.len() {
            let Some(line_end_rel) = buf[offset..].windows(2).position(|w| w == b"\r\n") else {
                return false;
            };
            let line_end = offset + line_end_rel;
            let size_line = match std::str::from_utf8(&buf[offset..line_end]) {
                Ok(line) => line,
                Err(_) => return false,
            };
            let size_hex = size_line.split(';').next().unwrap_or("").trim();
            let Ok(chunk_size) = usize::from_str_radix(size_hex, 16) else {
                return false;
            };
            offset = line_end + 2;
            let Some(chunk_end) = offset.checked_add(chunk_size) else {
                return false;
            };
            let Some(chunk_crlf_end) = chunk_end.checked_add(2) else {
                return false;
            };
            if buf.len() < chunk_crlf_end {
                return false;
            }
            if &buf[chunk_end..chunk_crlf_end] != b"\r\n" {
                return false;
            }
            offset = chunk_crlf_end;
            if chunk_size == 0 {
                if buf.len() < offset + 2 {
                    return false;
                }
                return &buf[offset..offset + 2] == b"\r\n";
            }
        }

        false
    }

    fn read_http_response(stream: &mut StdTcpStream, timeout: Duration) -> String {
        let mut buf = Vec::with_capacity(8192);
        let mut tmp = [0u8; 4096];
        stream
            .set_read_timeout(Some(timeout))
            .expect("set read timeout");

        loop {
            match stream.read(&mut tmp) {
                Ok(0) => break,
                Ok(n) => {
                    buf.extend_from_slice(&tmp[..n]);
                }
                Err(_) => break,
            }

            let text = String::from_utf8_lossy(&buf);
            if let Some(header_end) = text.find("\r\n\r\n") {
                let headers = &text[..header_end];
                if response_body_complete(&buf, header_end, headers) {
                    break;
                }
            }
        }

        String::from_utf8_lossy(&buf).into_owned()
    }

    fn denied_streaming_request_response(
        addr: &str,
        request_head: String,
        total_body_bytes: usize,
    ) -> (String, usize) {
        const WRITE_CHUNK_BYTES: usize = 1024;
        const WRITE_CHUNK_DELAY: Duration = Duration::from_millis(20);
        const RESPONSE_TIMEOUT: Duration = Duration::from_secs(3);

        let mut stream = StdTcpStream::connect(addr).unwrap();
        stream.set_nodelay(true).unwrap();

        let mut writer = stream.try_clone().unwrap();
        writer.set_nodelay(true).unwrap();

        let bytes_sent = Arc::new(AtomicUsize::new(0));
        let bytes_sent_writer = Arc::clone(&bytes_sent);
        let body_chunk = vec![b'x'; WRITE_CHUNK_BYTES];
        let writer_handle = std::thread::spawn(move || {
            writer.write_all(request_head.as_bytes()).unwrap();
            let chunk_count = total_body_bytes / WRITE_CHUNK_BYTES;
            for _ in 0..chunk_count {
                match writer.write_all(&body_chunk) {
                    Ok(()) => {
                        bytes_sent_writer.fetch_add(WRITE_CHUNK_BYTES, Ordering::Relaxed);
                        std::thread::sleep(WRITE_CHUNK_DELAY);
                    }
                    Err(_) => break,
                }
            }
        });

        let response = read_http_response(&mut stream, RESPONSE_TIMEOUT);
        writer_handle.join().unwrap();
        (response, bytes_sent.load(Ordering::Relaxed))
    }

    fn denied_streaming_multipart_request_response(
        addr: &str,
        request_head: String,
        file_prefix: Vec<u8>,
        file_suffix: Vec<u8>,
        total_file_bytes: usize,
    ) -> (String, usize) {
        const WRITE_CHUNK_BYTES: usize = 1024;
        const WRITE_CHUNK_DELAY: Duration = Duration::from_millis(20);
        const RESPONSE_TIMEOUT: Duration = Duration::from_secs(3);

        let mut stream = StdTcpStream::connect(addr).unwrap();
        stream.set_nodelay(true).unwrap();

        let mut writer = stream.try_clone().unwrap();
        writer.set_nodelay(true).unwrap();

        let bytes_sent = Arc::new(AtomicUsize::new(0));
        let bytes_sent_writer = Arc::clone(&bytes_sent);
        let body_chunk = vec![b'x'; WRITE_CHUNK_BYTES];
        let writer_handle = std::thread::spawn(move || {
            writer.write_all(request_head.as_bytes()).unwrap();
            writer.write_all(&file_prefix).unwrap();
            let mut remaining = total_file_bytes;
            while remaining > 0 {
                let next = remaining.min(WRITE_CHUNK_BYTES);
                match writer.write_all(&body_chunk[..next]) {
                    Ok(()) => {
                        bytes_sent_writer.fetch_add(next, Ordering::Relaxed);
                        remaining -= next;
                        std::thread::sleep(WRITE_CHUNK_DELAY);
                    }
                    Err(_) => return,
                }
            }
            let _ = writer.write_all(&file_suffix);
        });

        let response = read_http_response(&mut stream, RESPONSE_TIMEOUT);
        writer_handle.join().unwrap();
        (response, bytes_sent.load(Ordering::Relaxed))
    }

    fn hmac_sha256(key: &[u8], data: &[u8]) -> hmac::Tag {
        hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data)
    }

    #[allow(clippy::format_collect)]
    fn hex_encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }

    fn days_to_ymd(days: u64) -> (u64, u64, u64) {
        let z = days + 719468;
        let era = z / 146097;
        let doe = z - era * 146097;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        let y = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = if m <= 2 { y + 1 } else { y };
        (y, m, d)
    }

    struct SignedHeaders {
        authorization: String,
        amz_date: String,
        amz_content_sha256: String,
    }

    fn sign_headers(
        method: &str,
        uri: &str,
        host: &str,
        body: &[u8],
        extra_headers: &[(&str, &str)],
    ) -> SignedHeaders {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let days = secs / 86400;
        let (year, month, day) = days_to_ymd(days);
        let time_of_day = secs % 86400;
        let hour = time_of_day / 3600;
        let minute = (time_of_day % 3600) / 60;
        let second = time_of_day % 60;
        let date_long = format!(
            "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
            year, month, day, hour, minute, second
        );
        let date_short = &date_long[..8];
        let content_sha256 = sha256_hex(body);
        let (path, query) = uri.split_once('?').unwrap_or((uri, ""));
        let mut signed_header_pairs = vec![
            ("host", host),
            ("x-amz-content-sha256", content_sha256.as_str()),
            ("x-amz-date", date_long.as_str()),
        ];
        signed_header_pairs.extend_from_slice(extra_headers);
        signed_header_pairs.sort_by_key(|(name, _)| *name);

        let signed_headers = signed_header_pairs
            .iter()
            .map(|(name, _)| *name)
            .collect::<Vec<_>>()
            .join(";");
        let canonical_headers = canonical_headers(&signed_header_pairs);
        let canonical_request = canonical_request(
            method,
            path,
            &canonical_query_string(query),
            &canonical_headers,
            &signed_headers,
            &content_sha256,
        );
        let canonical_hash = sha256_hex(canonical_request.as_bytes());
        let scope = format!("{}/us-east-1/s3/aws4_request", date_short);
        let string_to_sign = string_to_sign(&date_long, &scope, &canonical_hash);
        let signing_key = derive_signing_key(
            &auth::SecretKey::new(TEST_SECRET_KEY.to_string()),
            date_short,
            "us-east-1",
            "s3",
        );
        let signature = hmac_sha256(signing_key.as_ref(), string_to_sign.as_bytes());
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            TEST_ACCESS_KEY,
            scope,
            signed_headers,
            hex_encode(signature.as_ref())
        );

        SignedHeaders {
            authorization,
            amz_date: date_long,
            amz_content_sha256: content_sha256,
        }
    }

    fn sign_post_policy_fields(
        bucket: &str,
        key: &str,
        extra_conditions: &[&str],
        extra_fields: &[(&str, &str)],
    ) -> Vec<(String, String)> {
        use base64::Engine;

        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let days = secs / 86400;
        let (year, month, day) = days_to_ymd(days);
        let time_of_day = secs % 86400;
        let hour = time_of_day / 3600;
        let minute = (time_of_day % 3600) / 60;
        let second = time_of_day % 60;
        let amz_date = format!(
            "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
            year, month, day, hour, minute, second
        );
        let date = amz_date[..8].to_string();
        let credential = format!("{TEST_ACCESS_KEY}/{date}/us-east-1/s3/aws4_request");
        let mut conditions = vec![
            format!(r#"{{"bucket":"{bucket}"}}"#),
            format!(r#"{{"key":"{key}"}}"#),
            r#"{"x-amz-algorithm":"AWS4-HMAC-SHA256"}"#.to_string(),
            format!(r#"{{"x-amz-credential":"{credential}"}}"#),
            format!(r#"{{"x-amz-date":"{amz_date}"}}"#),
        ];
        conditions.extend(
            extra_conditions
                .iter()
                .map(|condition| (*condition).to_string()),
        );
        let policy = format!(
            r#"{{"expiration":"2099-12-31T23:59:59Z","conditions":[{}]}}"#,
            conditions.join(",")
        );
        let policy_b64 = base64::engine::general_purpose::STANDARD.encode(policy.as_bytes());
        let signing_key = derive_signing_key(
            &auth::SecretKey::new(TEST_SECRET_KEY.to_string()),
            &date,
            "us-east-1",
            "s3",
        );
        let signature =
            hex_encode(hmac_sha256(signing_key.as_ref(), policy_b64.as_bytes()).as_ref());

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

    fn build_streaming_multipart_parts(
        fields: &[(String, String)],
        file_name: &str,
    ) -> (String, Vec<u8>, Vec<u8>) {
        let boundary = "----TestBoundary7MA4YWxkTrZu0gW";
        let mut prefix = Vec::new();

        for (name, value) in fields {
            prefix.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
            prefix.extend_from_slice(
                format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
            );
            prefix.extend_from_slice(value.as_bytes());
            prefix.extend_from_slice(b"\r\n");
        }

        prefix.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        prefix.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"file\"; filename=\"{file_name}\"\r\n")
                .as_bytes(),
        );
        prefix.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");

        let suffix = format!("\r\n--{boundary}--\r\n").into_bytes();
        (
            format!("multipart/form-data; boundary={boundary}"),
            prefix,
            suffix,
        )
    }

    #[test]
    fn streaming_put_eligible() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        let result = is_streaming_write(&parts);
        assert!(matches!(
            result,
            Ok(Some(StreamingWriteOp::PutObject { ref bucket, ref key, .. }))
            if bucket == "mybucket" && key == "mykey"
        ));
    }

    #[test]
    fn streaming_put_not_put_method() {
        let parts = make_parts(
            "POST",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert!(matches!(is_streaming_write(&parts), Ok(None)));
    }

    #[test]
    fn streaming_put_get_method() {
        let parts = make_parts(
            "GET",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert!(matches!(is_streaming_write(&parts), Ok(None)));
    }

    #[test]
    fn streaming_put_copy_source_excluded() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[
                ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
                ("x-amz-copy-source", "/src-bucket/src-key"),
            ],
        );
        assert!(matches!(is_streaming_write(&parts), Ok(None)));
    }

    #[test]
    fn streaming_put_real_sha256_routed() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[(
                "x-amz-content-sha256",
                "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            )],
        );
        let result = is_streaming_write(&parts);
        assert!(matches!(
            result,
            Ok(Some(StreamingWriteOp::PutObject { ref bucket, ref key, .. }))
            if bucket == "mybucket" && key == "mykey"
        ));
    }

    #[test]
    fn streaming_put_no_sha256_header_routed() {
        let parts = make_parts("PUT", "/mybucket/mykey", &[]);
        let result = is_streaming_write(&parts);
        assert!(matches!(
            result,
            Ok(Some(StreamingWriteOp::PutObject { ref bucket, ref key, .. }))
            if bucket == "mybucket" && key == "mykey"
        ));
    }

    #[test]
    fn parse_chunked_mode_signed_chunked() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[
                ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
                ("content-encoding", "aws-chunked"),
                ("x-amz-decoded-content-length", "100"),
            ],
        );
        let mode = parse_chunked_mode(&req).unwrap();
        assert!(matches!(mode, ChunkedMode::Signed { expected_len: 100 }));
    }

    #[test]
    fn parse_chunked_mode_signed_trailer_chunked() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[
                (
                    "x-amz-content-sha256",
                    "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
                ),
                ("content-encoding", "aws-chunked"),
                ("x-amz-decoded-content-length", "100"),
            ],
        );
        let mode = parse_chunked_mode(&req).unwrap();
        assert!(matches!(
            mode,
            ChunkedMode::SignedTrailer { expected_len: 100 }
        ));
    }

    #[test]
    fn parse_chunked_mode_unsigned_trailer_chunked() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[
                ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
                ("content-encoding", "aws-chunked"),
                ("x-amz-decoded-content-length", "100"),
            ],
        );
        let mode = parse_chunked_mode(&req).unwrap();
        assert!(matches!(
            mode,
            ChunkedMode::UnsignedTrailer { expected_len: 100 }
        ));
    }

    #[test]
    fn parse_chunked_mode_unsigned_payload_alone_rejected() {
        // STREAMING-UNSIGNED-PAYLOAD (without -TRAILER) is rejected by AWS.
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD")],
        );
        let err = parse_chunked_mode(&req).unwrap_err();
        assert!(matches!(err, ServerError::InvalidArgument { .. }));
    }

    #[test]
    fn parse_chunked_mode_missing_content_encoding_allowed() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[
                ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
                ("x-amz-decoded-content-length", "100"),
            ],
        );
        let mode = parse_chunked_mode(&req).unwrap();
        assert!(matches!(mode, ChunkedMode::Signed { expected_len: 100 }));
    }

    #[test]
    fn parse_chunked_mode_non_aws_content_encoding_allowed() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[
                ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
                ("content-encoding", "gzip"),
                ("x-amz-decoded-content-length", "100"),
            ],
        );
        let mode = parse_chunked_mode(&req).unwrap();
        assert!(matches!(mode, ChunkedMode::Signed { expected_len: 100 }));
    }

    #[test]
    fn parse_chunked_mode_missing_decoded_length_rejected() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[
                ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
                ("content-encoding", "aws-chunked"),
            ],
        );
        let err = parse_chunked_mode(&req).unwrap_err();
        assert!(matches!(err, ServerError::MissingContentLength));
    }

    #[test]
    fn parse_chunked_mode_decoded_length_over_object_limit_rejected() {
        let too_large = (MAX_OBJECT_SIZE + 1).to_string();
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[
                ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
                ("content-encoding", "aws-chunked"),
                ("x-amz-decoded-content-length", too_large.as_str()),
            ],
        );
        let err = parse_chunked_mode(&req).unwrap_err();
        assert!(matches!(err, ServerError::ObjectTooLarge { .. }));
    }

    #[test]
    fn streaming_put_bucket_config_excluded() {
        // PUT /<bucket>?versioning is a bucket config op, not PutObject.
        let parts = make_parts(
            "PUT",
            "/mybucket?versioning",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert!(matches!(is_streaming_write(&parts), Ok(None)));
    }

    #[test]
    fn streaming_put_object_retention_excluded() {
        // PUT /<bucket>/<key>?retention is an object-lock API, not PutObject.
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey?retention",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert!(matches!(is_streaming_write(&parts), Ok(None)));
    }

    #[test]
    fn streaming_put_deep_key() {
        let parts = make_parts(
            "PUT",
            "/mybucket/path/to/deep/key.txt",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        let result = is_streaming_write(&parts);
        assert!(matches!(
            result,
            Ok(Some(StreamingWriteOp::PutObject { ref bucket, ref key, .. }))
            if bucket == "mybucket" && key == "path/to/deep/key.txt"
        ));
    }

    #[test]
    fn streaming_upload_part_routed() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey?partNumber=3&uploadId=abc123",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        let result = is_streaming_write(&parts);
        assert!(matches!(
            result,
            Ok(Some(StreamingWriteOp::UploadPart {
                ref bucket,
                ref key,
                ref upload_id,
                part_number: 3,
                ..
            })) if bucket == "mybucket" && key == "mykey" && upload_id == "abc123"
        ));
    }

    #[test]
    fn streaming_upload_part_real_sha256_routed() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey?partNumber=3&uploadId=abc123",
            &[(
                "x-amz-content-sha256",
                "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            )],
        );
        let result = is_streaming_write(&parts);
        assert!(matches!(
            result,
            Ok(Some(StreamingWriteOp::UploadPart {
                ref bucket,
                ref key,
                ref upload_id,
                part_number: 3,
            })) if bucket == "mybucket" && key == "mykey" && upload_id == "abc123"
        ));
    }

    #[test]
    fn streaming_upload_part_missing_upload_id_rejected() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey?partNumber=3",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert!(matches!(
            is_streaming_write(&parts),
            Err(ServerError::InvalidRequest { reason })
                if reason == "missing uploadId query parameter"
        ));
    }

    #[test]
    fn streaming_upload_part_invalid_upload_id_is_preserved_for_later_validation() {
        let invalid_upload_id = "a".repeat(storage::UPLOAD_ID_LEN + 1);
        let parts = make_parts(
            "PUT",
            &format!("/mybucket/mykey?partNumber=3&uploadId={invalid_upload_id}"),
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        let result = is_streaming_write(&parts);
        assert!(matches!(
            result,
            Ok(Some(StreamingWriteOp::UploadPart {
                ref bucket,
                ref key,
                ref upload_id,
                part_number: 3,
                ..
            })) if bucket == "mybucket" && key == "mykey" && upload_id == &invalid_upload_id
        ));
    }

    #[test]
    fn streaming_upload_part_invalid_part_number_rejected() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey?partNumber=abc&uploadId=abc123",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert!(matches!(
            is_streaming_write(&parts),
            Err(ServerError::InvalidArgument { reason })
                if reason == "partNumber must be a positive integer"
        ));
    }

    #[test]
    fn streaming_upload_part_zero_part_number_rejected() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey?partNumber=0&uploadId=abc123",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert!(matches!(
            is_streaming_write(&parts),
            Err(ServerError::InvalidArgument { reason })
                if reason == "partNumber must be >= 1"
        ));
    }

    #[test]
    fn streaming_upload_part_copy_excluded() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey?partNumber=1&uploadId=abc",
            &[
                ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
                ("x-amz-copy-source", "/src/key"),
            ],
        );
        assert!(matches!(is_streaming_write(&parts), Ok(None)));
    }

    #[test]
    fn post_object_detected() {
        let parts = make_parts("POST", "/mybucket", &[]);
        assert_eq!(
            post_object_bucket(&parts).as_ref().map(BucketName::as_str),
            Some("mybucket")
        );
    }

    #[test]
    fn post_delete_objects_not_detected_as_post_object() {
        let parts = make_parts("POST", "/mybucket?delete", &[]);
        assert!(post_object_bucket(&parts).is_none());
    }

    #[test]
    fn post_multipart_parser_boundary_and_header_split_across_feeds() {
        let boundary = "BoundaryX";
        let mut parser = PostMultipartParser::new(boundary);

        let c1 = b"--Bound";
        let c2 = b"aryX\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\nvalue\r\n--BoundaryX\r\nContent-Disposition: form-data; name=\"file\"; filename=\"x\"\r\nContent-Type: application/octet-stream\r\n\r";
        let c3 = b"\nhello-world\r\n--BoundaryX--\r\n";

        let ev1 = parser.feed(c1).unwrap();
        assert!(ev1.is_empty());

        let ev2 = parser.feed(c2).unwrap();
        assert_eq!(ev2.len(), 1);
        match &ev2[0] {
            PostMultipartEvent::Field { name, value } => {
                assert_eq!(name, "key");
                assert_eq!(value, "value");
            }
            _ => panic!("expected a single field event"),
        }

        let ev3 = parser.feed(c3).unwrap();
        let mut saw_start = false;
        let mut saw_end = false;
        let mut file = Vec::new();
        for ev in ev3 {
            match ev {
                PostMultipartEvent::FileStart { file_name } => {
                    saw_start = true;
                    assert_eq!(file_name.as_deref(), Some("x"));
                }
                PostMultipartEvent::FileChunk(bytes) => file.extend_from_slice(&bytes),
                PostMultipartEvent::FileEnd => saw_end = true,
                PostMultipartEvent::Field { .. } => panic!("unexpected field event"),
            }
        }
        assert!(saw_start);
        assert!(saw_end);
        assert_eq!(file, b"hello-world");
        assert!(parser.is_done());
    }

    #[test]
    fn post_multipart_parser_file_data_integrity_with_split_delimiter() {
        let boundary = "BoundaryY";
        let delimiter = format!("\r\n--{boundary}--\r\n");

        let file_data: Vec<u8> = (0_u8..=127).cycle().take(1024).collect();
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            b"Content-Disposition: form-data; name=\"file\"; filename=\"blob.bin\"\r\n",
        );
        body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
        body.extend_from_slice(&file_data);
        body.extend_from_slice(delimiter.as_bytes());

        let marker_pos = body
            .windows(delimiter.len())
            .position(|w| w == delimiter.as_bytes())
            .unwrap();
        // Split in the middle of the final delimiter to force cross-feed detection.
        let split_at = marker_pos + 3;

        let mut parser = PostMultipartParser::new(boundary);
        let ev1 = parser.feed(&body[..split_at]).unwrap();
        let ev2 = parser.feed(&body[split_at..]).unwrap();

        let mut saw_start = false;
        let mut saw_end = false;
        let mut reconstructed = Vec::new();
        for ev in ev1.into_iter().chain(ev2) {
            match ev {
                PostMultipartEvent::FileStart { file_name } => {
                    saw_start = true;
                    assert_eq!(file_name.as_deref(), Some("blob.bin"));
                }
                PostMultipartEvent::FileChunk(bytes) => reconstructed.extend_from_slice(&bytes),
                PostMultipartEvent::FileEnd => saw_end = true,
                PostMultipartEvent::Field { .. } => panic!("unexpected field event"),
            }
        }

        assert!(saw_start);
        assert!(saw_end);
        assert_eq!(reconstructed, file_data);
        assert!(parser.is_done());
    }

    #[test]
    fn post_multipart_parser_rejects_oversized_part_headers() {
        let boundary = "BoundaryZ";
        let mut parser = PostMultipartParser::new(boundary);

        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"");
        body.extend(std::iter::repeat_n(
            b'h',
            MAX_STREAMING_POST_PART_HEADER_BYTES,
        ));

        let err = parser.feed(&body).unwrap_err();
        match err {
            ServerError::InvalidRequest { reason } => {
                assert!(reason.contains("multipart part headers exceed maximum size"));
            }
            other => panic!("expected InvalidRequest, got {other:?}"),
        }
    }

    #[test]
    fn post_multipart_parser_rejects_oversized_non_file_field_without_boundary() {
        let boundary = "BoundaryField";
        let mut parser = PostMultipartParser::new(boundary);

        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"x-amz-date\"\r\n\r\n");
        body.extend(std::iter::repeat_n(
            b'0',
            MAX_STREAMING_POST_DATE_FIELD_BYTES + parser.delimiter.len(),
        ));

        let err = parser.feed(&body).unwrap_err();
        match err {
            ServerError::InvalidRequest { reason } => {
                assert!(reason.contains("multipart form field 'x-amz-date' exceeds maximum size"));
            }
            other => panic!("expected InvalidRequest, got {other:?}"),
        }
    }

    #[test]
    fn fuzz_post_multipart_parser_rejects_missing_final_boundary() {
        let boundary = "BoundaryEOF";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"blob.bin\"\r\nContent-Type: application/octet-stream\r\n\r\nhello world"
        );

        let err = fuzz_post_multipart_parser(boundary, body.as_bytes(), &[3, 1, 4, 1]).unwrap_err();
        assert!(matches!(err, ServerError::IncompleteBody));
    }

    #[test]
    fn fuzz_post_multipart_parser_rejects_missing_file_field() {
        let boundary = "BoundaryNoFile";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\nobject-key\r\n--{boundary}--"
        );

        let err = fuzz_post_multipart_parser(boundary, body.as_bytes(), &[2, 7, 1]).unwrap_err();
        assert!(matches!(
            err,
            ServerError::InvalidRequest { reason } if reason == "missing file field in multipart form"
        ));
    }

    #[test]
    fn streaming_post_field_budget_rejects_metadata_over_limit() {
        let mut budget = StreamingPostFieldBudget::default();
        let name = "x-amz-meta-limit";
        let value = "m".repeat(USER_METADATA_SIZE_LIMIT - name.len());

        budget.record(name, &value).unwrap();

        let err = budget.record(name, "x").unwrap_err();
        assert!(matches!(
            err,
            ServerError::MetadataTooLargeDetailed {
                max_size_allowed: USER_METADATA_SIZE_LIMIT,
                ..
            }
        ));
    }

    #[test]
    fn streaming_post_field_budget_rejects_total_non_file_bytes_over_limit() {
        let mut budget = StreamingPostFieldBudget::default();
        let value = "r".repeat(MAX_STREAMING_POST_DEFAULT_FIELD_BYTES);
        let field_bytes = "redirect".len() + value.len();
        while budget.total_bytes + field_bytes <= MAX_STREAMING_POST_NON_FILE_FORM_BYTES {
            budget.record("redirect", &value).unwrap();
        }

        let overflow = "o".repeat(MAX_STREAMING_POST_NON_FILE_FORM_BYTES - budget.total_bytes + 1);
        let err = budget.record("redirect", &overflow).unwrap_err();
        match err {
            ServerError::InvalidRequest { reason } => {
                assert!(reason.contains("multipart form fields exceed maximum total size"));
            }
            other => panic!("expected InvalidRequest, got {other:?}"),
        }
    }

    // ── Regression tests for P0–P2 security fixes ────────────────────

    #[test]
    fn extract_checksum_trailers_rejects_duplicates() {
        // P0: Duplicate trailer names must be rejected to prevent bypass.
        let trailers = vec![
            ("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string()),
            ("x-amz-checksum-crc32".to_string(), "BBBBBB==".to_string()),
        ];
        let err = extract_checksum_trailers(&trailers).unwrap_err();
        assert!(matches!(err, ServerError::InvalidRequest { .. }));
    }

    #[test]
    fn extract_checksum_trailers_rejects_mixed_case_duplicates() {
        // P0: Case-insensitive dedup — mixed-case duplicates are rejected.
        let trailers = vec![
            ("x-amz-checksum-crc32".to_string(), "AAAAAA==".to_string()),
            ("X-Amz-Checksum-CRC32".to_string(), "BBBBBB==".to_string()),
        ];
        let err = extract_checksum_trailers(&trailers).unwrap_err();
        assert!(matches!(err, ServerError::InvalidRequest { .. }));
    }

    #[test]
    fn extract_checksum_trailers_ignores_non_checksum() {
        // Non-checksum trailers and excluded names are not extracted.
        let trailers = vec![
            ("x-amz-checksum-algorithm".to_string(), "CRC32".to_string()),
            ("x-amz-checksum-type".to_string(), "FULL_OBJECT".to_string()),
            ("x-amz-request-id".to_string(), "abc".to_string()),
            ("x-amz-checksum-sha256".to_string(), "dGVzdA==".to_string()),
        ];
        let result = extract_checksum_trailers(&trailers).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "x-amz-checksum-sha256");
    }

    #[test]
    fn trailing_hasher_case_insensitive() {
        // P1: Header name matching must be case-insensitive.
        assert!(TrailingChecksumHasher::from_trailer_header("X-Amz-Checksum-CRC32").is_some());
        assert!(TrailingChecksumHasher::from_trailer_header("x-amz-checksum-crc32").is_some());
        assert!(TrailingChecksumHasher::from_trailer_header("X-AMZ-CHECKSUM-CRC32C").is_some());
        assert!(TrailingChecksumHasher::from_trailer_header("X-Amz-Checksum-Sha256").is_some());
        assert!(TrailingChecksumHasher::from_trailer_header("x-amz-checksum-sha1").is_some());
        assert!(TrailingChecksumHasher::from_trailer_header("X-AMZ-CHECKSUM-CRC64NVME").is_some());
        assert!(TrailingChecksumHasher::from_trailer_header("x-amz-checksum-md5").is_some());
        assert!(TrailingChecksumHasher::from_trailer_header("X-Amz-Checksum-XXHash64").is_some());
        assert!(TrailingChecksumHasher::from_trailer_header("x-amz-checksum-xxhash3").is_some());
        assert!(TrailingChecksumHasher::from_trailer_header("X-Amz-Checksum-XXHash128").is_some());
        assert!(TrailingChecksumHasher::from_trailer_header("x-amz-checksum-sha512").is_some());
    }

    #[test]
    fn trailing_hasher_from_request_csv() {
        // P1: x-amz-trailer can be comma-separated; first recognized name wins.
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-trailer", "x-amz-checksum-type, x-amz-checksum-crc32")],
        );
        let hasher = trailing_hasher_from_request(&req);
        assert!(hasher.is_some());
        // Verify it's a CRC32 hasher by finalizing empty data.
        let cksum = hasher.unwrap().finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Crc32);
    }

    #[test]
    fn trailing_hasher_from_request_single() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-trailer", "x-amz-checksum-sha256")],
        );
        let hasher = trailing_hasher_from_request(&req);
        assert!(hasher.is_some());
        let cksum = hasher.unwrap().finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Sha256);
    }

    #[test]
    fn trailing_hasher_from_request_none_when_no_header() {
        let req = make_s3req("PUT", "/mybucket/mykey", &[]);
        assert!(trailing_hasher_from_request(&req).is_none());
    }

    #[test]
    fn claimed_payload_sha256_from_request_recognizes_fixed_hash() {
        let fixed = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let req = make_s3req("PUT", "/mybucket/mykey", &[("x-amz-content-sha256", fixed)]);
        assert_eq!(
            claimed_payload_sha256_from_request(&req).as_deref(),
            Some(fixed)
        );
    }

    #[test]
    fn claimed_payload_sha256_from_request_ignores_sentinel_values() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert!(claimed_payload_sha256_from_request(&req).is_none());

        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")],
        );
        assert!(claimed_payload_sha256_from_request(&req).is_none());
    }

    #[test]
    fn crc32c_streaming_matches_canonical() {
        // P1: Incremental CRC32C must match checksum::crc32c::checksum().
        let data = b"123456789";
        let expected = checksum::crc32c::checksum(data);

        let mut hasher =
            TrailingChecksumHasher::from_trailer_header("x-amz-checksum-crc32c").unwrap();
        hasher.update(data);
        let cksum = hasher.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Crc32c);
        assert_eq!(
            u32::from_be_bytes(cksum.bytes().try_into().unwrap()),
            expected,
            "streaming CRC32C mismatch for b\"123456789\""
        );
    }

    #[test]
    fn crc32c_streaming_incremental_matches_canonical() {
        // P1: Multi-chunk incremental CRC32C must also match.
        let data = b"hello world!";
        let expected = checksum::crc32c::checksum(data);

        let mut hasher =
            TrailingChecksumHasher::from_trailer_header("x-amz-checksum-crc32c").unwrap();
        hasher.update(b"hello ");
        hasher.update(b"world!");
        let cksum = hasher.finalize_raw();
        assert_eq!(
            u32::from_be_bytes(cksum.bytes().try_into().unwrap()),
            expected,
            "incremental streaming CRC32C mismatch"
        );
    }

    #[test]
    fn crc32c_streaming_empty_matches_canonical() {
        let expected = checksum::crc32c::checksum(b"");
        let hasher = TrailingChecksumHasher::from_trailer_header("x-amz-checksum-crc32c").unwrap();
        let cksum = hasher.finalize_raw();
        assert_eq!(
            u32::from_be_bytes(cksum.bytes().try_into().unwrap()),
            expected
        );
    }

    #[test]
    fn crc32_streaming_matches_canonical() {
        // Sanity check: CRC32 streaming also matches.
        let data = b"123456789";
        let expected = checksum::crc32::checksum(data);

        let mut hasher =
            TrailingChecksumHasher::from_trailer_header("x-amz-checksum-crc32").unwrap();
        hasher.update(data);
        let cksum = hasher.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Crc32);
        assert_eq!(
            u32::from_be_bytes(cksum.bytes().try_into().unwrap()),
            expected
        );
    }

    #[test]
    fn crc64_streaming_matches_canonical() {
        let data = b"123456789";
        let expected = checksum::crc64::checksum(data);

        let mut hasher =
            TrailingChecksumHasher::from_trailer_header("x-amz-checksum-crc64nvme").unwrap();
        hasher.update(data);
        let cksum = hasher.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Crc64nvme);
        assert_eq!(
            u64::from_be_bytes(cksum.bytes().try_into().unwrap()),
            expected
        );
    }

    #[test]
    fn sha256_streaming_matches_canonical() {
        let data = b"123456789";
        let expected = ring::digest::digest(&ring::digest::SHA256, data);

        let mut hasher =
            TrailingChecksumHasher::from_trailer_header("x-amz-checksum-sha256").unwrap();
        hasher.update(data);
        let cksum = hasher.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Sha256);
        assert_eq!(cksum.bytes(), expected.as_ref());
    }

    #[test]
    fn sha1_streaming_matches_canonical() {
        let data = b"123456789";
        let expected = ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, data);

        let mut hasher =
            TrailingChecksumHasher::from_trailer_header("x-amz-checksum-sha1").unwrap();
        hasher.update(data);
        let cksum = hasher.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Sha1);
        assert_eq!(cksum.bytes(), expected.as_ref());
    }

    #[test]
    fn sha512_streaming_matches_shared_checksum() {
        let data = b"123456789";
        let expected = checksum::compute_checksum(ChecksumAlgorithm::Sha512, data);

        let mut hasher =
            TrailingChecksumHasher::from_trailer_header("x-amz-checksum-sha512").unwrap();
        hasher.update(data);
        let cksum = hasher.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Sha512);
        assert_eq!(cksum.bytes(), expected.bytes());
    }

    #[test]
    fn xxhash3_streaming_matches_shared_checksum() {
        let data = b"123456789";
        let expected = checksum::compute_checksum(ChecksumAlgorithm::XxHash3, data);

        let mut hasher =
            TrailingChecksumHasher::from_trailer_header("x-amz-checksum-xxhash3").unwrap();
        hasher.update(data);
        let cksum = hasher.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::XxHash3);
        assert_eq!(cksum.bytes(), expected.bytes());
    }

    #[test]
    fn sha256_streaming_incremental_matches_canonical() {
        let data = b"hello world!";
        let expected = ring::digest::digest(&ring::digest::SHA256, data);

        let mut hasher =
            TrailingChecksumHasher::from_trailer_header("x-amz-checksum-sha256").unwrap();
        hasher.update(b"hello ");
        hasher.update(b"world!");
        let cksum = hasher.finalize_raw();
        assert_eq!(cksum.bytes(), expected.as_ref());
    }

    // ── Inline checksum hasher tests ─────────────────────────────────

    #[test]
    fn inline_checksum_hasher_picks_up_crc32_header() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-checksum-crc32", "AAAAAA==")],
        );
        let result = inline_checksum_hasher_from_request(&req);
        assert!(result.is_some());
        let (h, claimed) = result.unwrap();
        assert_eq!(claimed, "AAAAAA==");
        let cksum = h.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Crc32);
    }

    #[test]
    fn inline_checksum_hasher_none_when_no_checksum() {
        let req = make_s3req("PUT", "/mybucket/mykey", &[]);
        assert!(inline_checksum_hasher_from_request(&req).is_none());
    }

    #[test]
    fn inline_checksum_hasher_picks_sha256() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-checksum-sha256", "dGVzdA==")],
        );
        let result = inline_checksum_hasher_from_request(&req);
        assert!(result.is_some());
        let (h, claimed) = result.unwrap();
        assert_eq!(claimed, "dGVzdA==");
        let cksum = h.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Sha256);
    }

    #[test]
    fn inline_checksum_hasher_picks_sha512() {
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-checksum-sha512", "dGVzdA==")],
        );
        let result = inline_checksum_hasher_from_request(&req);
        assert!(result.is_some());
        let (h, claimed) = result.unwrap();
        assert_eq!(claimed, "dGVzdA==");
        let cksum = h.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Sha512);
    }

    #[test]
    fn inline_checksum_hasher_not_triggered_by_non_checksum_headers() {
        // x-amz-checksum-algorithm is not a checksum value header.
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-checksum-algorithm", "CRC32")],
        );
        assert!(inline_checksum_hasher_from_request(&req).is_none());
    }

    #[test]
    fn streaming_post_field_value_limit_covers_new_checksums() {
        assert_eq!(
            streaming_post_field_value_limit("x-amz-checksum-sha512"),
            MAX_STREAMING_POST_CHECKSUM_FIELD_BYTES
        );
        assert_eq!(
            streaming_post_field_value_limit("X-Amz-Checksum-XXHash128"),
            MAX_STREAMING_POST_CHECKSUM_FIELD_BYTES
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_upload_part_bad_checksum_aborts_session() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let upload_id = create_test_bucket_and_upload(&frontend, "mybucket", "mykey");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;

        let body = b"part-data";
        let uri = format!("/mybucket/mykey?partNumber=1&uploadId={upload_id}");
        let checksum = "AAAAAA==";
        let signed = sign_headers(
            "PUT",
            &uri,
            &addr,
            body,
            &[("x-amz-checksum-crc32", checksum)],
        );

        let mut stream = StdTcpStream::connect(&addr).unwrap();
        let request = format!(
            "PUT {uri} HTTP/1.1\r\n\
Host: {addr}\r\n\
Authorization: {}\r\n\
x-amz-date: {}\r\n\
x-amz-content-sha256: {}\r\n\
x-amz-checksum-crc32: {}\r\n\
Content-Length: {}\r\n\
Connection: close\r\n\r\n",
            signed.authorization,
            signed.amz_date,
            signed.amz_content_sha256,
            checksum,
            body.len()
        );
        stream.write_all(request.as_bytes()).unwrap();
        stream.write_all(body).unwrap();

        let response = read_http_response(&mut stream, Duration::from_secs(5));
        assert!(
            response.starts_with("HTTP/1.1 400"),
            "expected 400 status, got: {}",
            response.lines().next().unwrap_or("")
        );
        assert!(
            response.contains("<Code>BadDigest</Code>"),
            "expected BadDigest body, got: {response}"
        );
        assert_eq!(
            frontend.coordinator.scavenge_stale_sessions(0),
            0,
            "streaming session leaked after UploadPart bad checksum"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_put_abort_guard_cleans_promoted_session_on_drop() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "mybucket");
        let body = vec![b'x'; crate::coordinator::INTERNAL_SEGMENT_SIZE + 1];
        let signed = sign_headers("PUT", "/mybucket/mykey", "localhost", &body, &[]);
        let content_length = body.len().to_string();
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[
                ("host", "localhost"),
                ("authorization", &signed.authorization),
                ("x-amz-date", &signed.amz_date),
                ("x-amz-content-sha256", &signed.amz_content_sha256),
                ("content-length", &content_length),
            ],
        );
        let ctx = Arc::new(
            frontend
                .prepare_streaming_put(&req, "mybucket", "mykey", false)
                .unwrap(),
        );
        let session_id = frontend.start_streaming_put_session(&ctx).unwrap();
        let state = Arc::new(ServerState {
            pool: vec![Arc::clone(&frontend)],
            host_id: frontend.host_id.clone(),
            counter: AtomicUsize::new(0),
            request_semaphore: Arc::new(tokio::sync::Semaphore::new(8)),
            segment_buffer_pool: SegmentBufferPool::new(8),
            config: ServeConfig::default(),
        });

        let guard = StreamingAbortGuard::new(&state);
        guard.arm_put(&ctx, &session_id);
        drop(guard);

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            frontend.coordinator.scavenge_stale_sessions(0),
            0,
            "streaming PUT abort guard left a durable stream session behind"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_put_abort_guard_cleans_session_created_after_request_drop() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "mybucket");
        let body = vec![b'x'; crate::coordinator::INTERNAL_SEGMENT_SIZE + 1];
        let signed = sign_headers("PUT", "/mybucket/mykey", "localhost", &body, &[]);
        let content_length = body.len().to_string();
        let req = make_s3req(
            "PUT",
            "/mybucket/mykey",
            &[
                ("host", "localhost"),
                ("authorization", &signed.authorization),
                ("x-amz-date", &signed.amz_date),
                ("x-amz-content-sha256", &signed.amz_content_sha256),
                ("content-length", &content_length),
            ],
        );
        let ctx = Arc::new(
            frontend
                .prepare_streaming_put(&req, "mybucket", "mykey", false)
                .unwrap(),
        );
        let state = Arc::new(ServerState {
            pool: vec![Arc::clone(&frontend)],
            host_id: frontend.host_id.clone(),
            counter: AtomicUsize::new(0),
            request_semaphore: Arc::new(tokio::sync::Semaphore::new(8)),
            segment_buffer_pool: SegmentBufferPool::new(8),
            config: ServeConfig::default(),
        });

        let guard = StreamingAbortGuard::new(&state);
        let worker_guard = Arc::clone(&guard);
        let worker_frontend = Arc::clone(&frontend);
        let worker_ctx = Arc::clone(&ctx);
        let join = tokio::task::spawn_blocking(move || {
            let session_id = worker_frontend
                .start_streaming_put_session(&worker_ctx)
                .unwrap();
            worker_guard.arm_put(&worker_ctx, &session_id);
            session_id
        });
        drop(guard);

        let session_id = join.await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            frontend.coordinator.scavenge_stale_sessions(0),
            0,
            "streaming PUT abort guard left session {session_id:?} after request-side drop"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_post_abort_guard_cleans_session_created_after_request_drop() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "mybucket");
        let signed = sign_headers("POST", "/mybucket", "localhost", &[], &[]);
        let req = make_s3req(
            "POST",
            "/mybucket",
            &[
                ("host", "localhost"),
                ("authorization", &signed.authorization),
                ("x-amz-date", &signed.amz_date),
                ("x-amz-content-sha256", &signed.amz_content_sha256),
            ],
        );
        let state = Arc::new(ServerState {
            pool: vec![Arc::clone(&frontend)],
            host_id: frontend.host_id.clone(),
            counter: AtomicUsize::new(0),
            request_semaphore: Arc::new(tokio::sync::Semaphore::new(8)),
            segment_buffer_pool: SegmentBufferPool::new(8),
            config: ServeConfig::default(),
        });

        let guard = StreamingAbortGuard::new(&state);
        let worker_guard = Arc::clone(&guard);
        let worker_frontend = Arc::clone(&frontend);
        let join = tokio::task::spawn_blocking(move || {
            let ctx = worker_frontend
                .prepare_streaming_post_object(
                    &req,
                    "mybucket",
                    &[("key".to_string(), "mykey".to_string())],
                    Some("upload.txt"),
                )
                .unwrap();
            let ctx = Arc::new(ctx);
            worker_guard.arm_post(&ctx);
            (
                ctx.session_id().clone(),
                worker_guard.put_heartbeat_started.load(Ordering::Acquire),
            )
        });
        drop(guard);

        let (session_id, heartbeat_started) = join.await.unwrap();
        assert!(
            heartbeat_started,
            "streaming POST must renew the PutObject stream proof while the request is active"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            frontend.coordinator.scavenge_stale_sessions(0),
            0,
            "streaming POST abort guard left session {session_id:?} after request-side drop"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_part_abort_guard_cleans_session_created_after_request_drop() {
        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let upload_id = create_test_bucket_and_upload(&frontend, "mybucket", "mykey");
        let uri = format!("/mybucket/mykey?partNumber=1&uploadId={upload_id}");
        let signed = sign_headers("PUT", &uri, "localhost", b"", &[]);
        let req = make_s3req(
            "PUT",
            &uri,
            &[
                ("host", "localhost"),
                ("authorization", &signed.authorization),
                ("x-amz-date", &signed.amz_date),
                ("x-amz-content-sha256", &signed.amz_content_sha256),
            ],
        );
        let state = Arc::new(ServerState {
            pool: vec![Arc::clone(&frontend)],
            host_id: frontend.host_id.clone(),
            counter: AtomicUsize::new(0),
            request_semaphore: Arc::new(tokio::sync::Semaphore::new(8)),
            segment_buffer_pool: SegmentBufferPool::new(8),
            config: ServeConfig::default(),
        });

        let guard = StreamingAbortGuard::new(&state);
        let worker_guard = Arc::clone(&guard);
        let worker_frontend = Arc::clone(&frontend);
        let upload_id_for_worker = upload_id.clone();
        let join = tokio::task::spawn_blocking(move || {
            let ctx = worker_frontend
                .prepare_streaming_part(&req, "mybucket", "mykey", &upload_id_for_worker, 1)
                .unwrap();
            let ctx = Arc::new(ctx);
            worker_guard.arm_part(&ctx);
            ctx.session_id().clone()
        });
        drop(guard);

        let session_id = join.await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            frontend.coordinator.scavenge_stale_sessions(0),
            0,
            "streaming UploadPart abort guard left session {session_id:?} after request-side drop"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn denied_streaming_put_responds_before_full_body_is_sent() {
        const TOTAL_BODY_BYTES: usize = 256 * 1024;

        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "mybucket");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;

        let request = format!(
            "PUT /mybucket/mykey HTTP/1.1\r\n\
Host: {addr}\r\n\
Content-Length: {TOTAL_BODY_BYTES}\r\n\
Connection: keep-alive\r\n\r\n"
        );
        let (response, bytes_sent) =
            denied_streaming_request_response(&addr, request, TOTAL_BODY_BYTES);

        assert!(
            response.starts_with("HTTP/1.1 403"),
            "expected 403 status, got: {}",
            response.lines().next().unwrap_or("")
        );
        assert!(
            response.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied body, got: {response}"
        );
        assert!(
            response.to_ascii_lowercase().contains("connection: close"),
            "expected Connection: close header, got: {response}"
        );
        assert!(
            bytes_sent < TOTAL_BODY_BYTES,
            "server read the full denied PUT body before responding: sent {bytes_sent} bytes"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn denied_streaming_upload_part_responds_before_full_body_is_sent() {
        const TOTAL_BODY_BYTES: usize = 256 * 1024;

        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        let upload_id = create_test_bucket_and_upload(&frontend, "mybucket", "mykey");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;

        let request = format!(
            "PUT /mybucket/mykey?partNumber=1&uploadId={upload_id} HTTP/1.1\r\n\
Host: {addr}\r\n\
Content-Length: {TOTAL_BODY_BYTES}\r\n\
Connection: keep-alive\r\n\r\n"
        );
        let (response, bytes_sent) =
            denied_streaming_request_response(&addr, request, TOTAL_BODY_BYTES);

        assert!(
            response.starts_with("HTTP/1.1 403"),
            "expected 403 status, got: {}",
            response.lines().next().unwrap_or("")
        );
        assert!(
            response.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied body, got: {response}"
        );
        assert!(
            response.to_ascii_lowercase().contains("connection: close"),
            "expected Connection: close header, got: {response}"
        );
        assert!(
            bytes_sent < TOTAL_BODY_BYTES,
            "server read the full denied UploadPart body before responding: sent {bytes_sent} bytes"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_put_missing_content_sha256_closes_before_full_body_is_sent() {
        const TOTAL_BODY_BYTES: usize = 256 * 1024;

        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "mybucket");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;

        let body = vec![b'x'; TOTAL_BODY_BYTES];
        let signed = sign_headers("PUT", "/mybucket/mykey", &addr, &body, &[]);
        let request = format!(
            "PUT /mybucket/mykey HTTP/1.1\r\n\
Host: {addr}\r\n\
Authorization: {}\r\n\
x-amz-date: {}\r\n\
Content-Length: {TOTAL_BODY_BYTES}\r\n\
Connection: keep-alive\r\n\r\n",
            signed.authorization, signed.amz_date
        );
        let (response, bytes_sent) =
            denied_streaming_request_response(&addr, request, TOTAL_BODY_BYTES);

        assert!(
            response.starts_with("HTTP/1.1 400"),
            "expected 400 status, got: {}",
            response.lines().next().unwrap_or("")
        );
        assert!(
            response.contains("Missing required header for this request: x-amz-content-sha256"),
            "expected missing x-amz-content-sha256 body, got: {response}"
        );
        assert!(
            response.to_ascii_lowercase().contains("connection: close"),
            "expected Connection: close header, got: {response}"
        );
        assert!(
            bytes_sent < TOTAL_BODY_BYTES,
            "server read the full missing-sha256 PUT body before responding: sent {bytes_sent} bytes"
        );
        assert_eq!(
            frontend.coordinator.scavenge_stale_sessions(0),
            0,
            "missing-sha256 PUT should not create a stream session"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_put_object_lock_without_checksum_closes_before_full_body_is_sent() {
        const TOTAL_BODY_BYTES: usize = 256 * 1024;

        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "mybucket");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;

        let body = vec![b'x'; TOTAL_BODY_BYTES];
        let signed = sign_headers(
            "PUT",
            "/mybucket/mykey",
            &addr,
            &body,
            &[
                ("x-amz-object-lock-mode", "COMPLIANCE"),
                (
                    "x-amz-object-lock-retain-until-date",
                    "2099-01-01T00:00:00Z",
                ),
            ],
        );
        let request = format!(
            "PUT /mybucket/mykey HTTP/1.1\r\n\
Host: {addr}\r\n\
Authorization: {}\r\n\
x-amz-date: {}\r\n\
x-amz-content-sha256: {}\r\n\
x-amz-object-lock-mode: COMPLIANCE\r\n\
x-amz-object-lock-retain-until-date: 2099-01-01T00:00:00Z\r\n\
Content-Length: {TOTAL_BODY_BYTES}\r\n\
Connection: keep-alive\r\n\r\n",
            signed.authorization, signed.amz_date, signed.amz_content_sha256
        );
        let (response, bytes_sent) =
            denied_streaming_request_response(&addr, request, TOTAL_BODY_BYTES);

        assert!(
            response.starts_with("HTTP/1.1 400"),
            "expected 400 status, got: {}",
            response.lines().next().unwrap_or("")
        );
        assert!(
            response.contains(
                "Content-MD5 OR x-amz-checksum- HTTP header is required for Put Object requests with Object Lock parameters"
            ),
            "expected object-lock checksum requirement body, got: {response}"
        );
        assert!(
            response.to_ascii_lowercase().contains("connection: close"),
            "expected Connection: close header, got: {response}"
        );
        assert!(
            bytes_sent < TOTAL_BODY_BYTES,
            "server read the full object-lock checksum failure PUT body before responding: sent {bytes_sent} bytes"
        );
        assert_eq!(
            frontend.coordinator.scavenge_stale_sessions(0),
            0,
            "object-lock checksum failure PUT should not create a stream session"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn denied_streaming_post_policy_closes_before_full_body_is_sent() {
        const TOTAL_FILE_BYTES: usize = 256 * 1024;

        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "mybucket");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;

        let fields = sign_post_policy_fields(
            "mybucket",
            "mykey",
            &[r#"{"Content-Type":"text/plain"}"#],
            &[("Content-Type", "image/png")],
        );
        let (content_type, prefix, suffix) = build_streaming_multipart_parts(&fields, "test.txt");
        let content_length = prefix.len() + TOTAL_FILE_BYTES + suffix.len();
        let request = format!(
            "POST /mybucket HTTP/1.1\r\n\
Host: {addr}\r\n\
Content-Type: {content_type}\r\n\
Content-Length: {content_length}\r\n\
Connection: keep-alive\r\n\r\n"
        );
        let (response, bytes_sent) = denied_streaming_multipart_request_response(
            &addr,
            request,
            prefix,
            suffix,
            TOTAL_FILE_BYTES,
        );

        assert!(
            response.starts_with("HTTP/1.1 403"),
            "expected 403 status, got: {}",
            response.lines().next().unwrap_or("")
        );
        assert!(
            response.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied body, got: {response}"
        );
        assert!(
            response.to_ascii_lowercase().contains("connection: close"),
            "expected Connection: close header, got: {response}"
        );
        assert!(
            bytes_sent < TOTAL_FILE_BYTES,
            "server read the full denied POST body before responding: sent {bytes_sent} bytes"
        );
        assert_eq!(
            frontend.coordinator.scavenge_stale_sessions(0),
            0,
            "policy-denied POST should not create a stream session"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_post_content_length_range_max_abort_closes_before_full_body_is_sent() {
        const TOTAL_FILE_BYTES: usize = 256 * 1024;

        let tmp = test_util::tempdir();
        let frontend = setup_frontend(tmp.path());
        create_test_bucket(&frontend, "mybucket");
        let (addr, _guard) = start_test_server(Arc::clone(&frontend)).await;

        let fields = sign_post_policy_fields(
            "mybucket",
            "mykey",
            &[r#"["content-length-range",0,1024]"#],
            &[],
        );
        let (content_type, prefix, suffix) = build_streaming_multipart_parts(&fields, "test.txt");
        let content_length = prefix.len() + TOTAL_FILE_BYTES + suffix.len();
        let request = format!(
            "POST /mybucket HTTP/1.1\r\n\
Host: {addr}\r\n\
Content-Type: {content_type}\r\n\
Content-Length: {content_length}\r\n\
Connection: keep-alive\r\n\r\n"
        );
        let (response, bytes_sent) = denied_streaming_multipart_request_response(
            &addr,
            request,
            prefix,
            suffix,
            TOTAL_FILE_BYTES,
        );

        assert!(
            response.starts_with("HTTP/1.1 400"),
            "expected 400 status, got: {}",
            response.lines().next().unwrap_or("")
        );
        assert!(
            response.contains("content-length-range"),
            "expected content-length-range failure body, got: {response}"
        );
        assert!(
            response.to_ascii_lowercase().contains("connection: close"),
            "expected Connection: close header, got: {response}"
        );
        assert!(
            bytes_sent < TOTAL_FILE_BYTES,
            "server read the full over-max POST body before responding: sent {bytes_sent} bytes"
        );
        assert_eq!(
            frontend.coordinator.scavenge_stale_sessions(0),
            0,
            "over-max POST should not leak a stream session"
        );
    }
}
