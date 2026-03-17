/// Async hyper HTTP server loop with frontend pool and backpressure.
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use checksum::{ChecksumAlgorithm, RawChecksum};
use http_body_util::{BodyExt, LengthLimitError, Limited};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

use super::request::{S3Request, MAX_BUFFERED_CONTROL_BODY_SIZE};
use super::response::S3Response;
use super::router::{route, S3Operation};
use super::s3_response_to_hyper;
use super::{HttpFrontend, S3HyperBody};
use crate::coordinator::MAX_OBJECT_SIZE;
use crate::error::ServerError;

const TRACE_TARGET: &str = "server_http";

/// Incremental hasher for validating trailing checksums in streaming uploads.
///
/// Created when `x-amz-trailer` declares a checksum header. Fed with decoded
/// payload during streaming, then finalized to a base64 string for comparison
/// with the trailer value.
enum TrailingChecksumHasher {
    Crc32(u32),
    Crc32c(checksum::crc32c::Hasher),
    Crc64(checksum::crc64::Hasher),
    Sha256(ring::digest::Context),
    Sha1(ring::digest::Context),
}

impl TrailingChecksumHasher {
    /// Create a hasher from a trailer header name (e.g. `x-amz-checksum-crc32`).
    ///
    /// Matches case-insensitively since HTTP header names are case-insensitive.
    fn from_trailer_header(header: &str) -> Option<Self> {
        match header.to_ascii_lowercase().as_str() {
            "x-amz-checksum-crc32" => Some(Self::Crc32(0)),
            "x-amz-checksum-crc32c" => Some(Self::Crc32c(checksum::crc32c::Hasher::new())),
            "x-amz-checksum-crc64nvme" => Some(Self::Crc64(checksum::crc64::Hasher::new())),
            "x-amz-checksum-sha256" => Some(Self::Sha256(ring::digest::Context::new(
                &ring::digest::SHA256,
            ))),
            "x-amz-checksum-sha1" => Some(Self::Sha1(ring::digest::Context::new(
                &ring::digest::SHA1_FOR_LEGACY_USE_ONLY,
            ))),
            _ => None,
        }
    }

    fn update(&mut self, data: &[u8]) {
        match self {
            Self::Crc32(crc) => {
                *crc = unsafe { ec_sys::crc32_gzip_refl(*crc, data.as_ptr(), data.len() as u64) };
            }
            Self::Crc32c(h) => {
                h.update(data);
            }
            Self::Crc64(h) => {
                h.update(data);
            }
            Self::Sha256(ctx) | Self::Sha1(ctx) => ctx.update(data),
        }
    }

    /// Finalize and return a validated `RawChecksum`.
    fn finalize_raw(self) -> RawChecksum {
        match self {
            Self::Crc32(crc) => RawChecksum::new(ChecksumAlgorithm::Crc32, crc.to_be_bytes()),
            Self::Crc32c(h) => {
                RawChecksum::new(ChecksumAlgorithm::Crc32c, h.finalize().to_be_bytes())
            }
            Self::Crc64(h) => {
                RawChecksum::new(ChecksumAlgorithm::Crc64nvme, h.finalize().to_be_bytes())
            }
            Self::Sha256(ctx) => RawChecksum::new(ChecksumAlgorithm::Sha256, ctx.finish().as_ref()),
            Self::Sha1(ctx) => RawChecksum::new(ChecksumAlgorithm::Sha1, ctx.finish().as_ref()),
        }
        .expect("hasher produces correct length")
    }

    /// Finalize and return the base64-encoded checksum string.
    fn finalize_b64(self) -> String {
        use base64::Engine;
        let cksum = self.finalize_raw();
        base64::engine::general_purpose::STANDARD.encode(cksum.bytes())
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
        bucket: String,
        key: String,
    },
    UploadPart {
        bucket: String,
        key: String,
        upload_id: String,
        part_number: u32,
    },
}

/// Tunable timeouts for the HTTP serve layer.
pub struct ServeConfig {
    /// Time allowed for a client to send request headers. Also serves as the
    /// idle timeout between keep-alive requests.
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
}

impl Default for ServeConfig {
    fn default() -> Self {
        Self {
            header_read_timeout: Duration::from_secs(30),
            request_wait_timeout: Duration::from_secs(5),
            body_idle_timeout: Duration::from_secs(30),
            stream_read_chunk_size: server_core::coordinator::INTERNAL_SEGMENT_SIZE,
        }
    }
}

/// Shared server state: frontend pool, round-robin counter, request semaphore,
/// and timeout configuration.
struct ServerState {
    pool: Vec<Arc<HttpFrontend>>,
    counter: AtomicUsize,
    request_semaphore: Arc<Semaphore>,
    segment_buffer_pool: SegmentBufferPool,
    config: ServeConfig,
}

struct SegmentBufferPool {
    max_cached: usize,
    cached: Mutex<Vec<Vec<u8>>>,
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
        let mut buf = self
            .cached
            .lock()
            .unwrap()
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
        let mut cached = self.cached.lock().unwrap();
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
    assert!(!frontends.is_empty(), "at least one frontend required");

    let header_read_timeout = config.header_read_timeout;
    let state = Arc::new(ServerState {
        pool: frontends.into_iter().map(Arc::new).collect(),
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

        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let _conn_permit = conn_permit;
            let io = TokioIo::new(stream);

            // header_read_timeout doubles as the idle timeout between
            // keep-alive requests: after sending a response, hyper waits
            // for the next request's headers and closes the connection if
            // none arrive within the timeout. This releases the connection
            // permit without killing active transfers.
            let _ = http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(header_read_timeout)
                .serve_connection(
                    io,
                    service_fn(move |req: Request<Incoming>| {
                        let state = Arc::clone(&state);
                        async move { handle(state, req).await }
                    }),
                )
                .await;
        });
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
) -> Result<http::Response<S3HyperBody>, Infallible> {
    let trace = observability::TraceContext::new_request();
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();
    let response_trace = crate::http::ResponseTraceMeta::new(trace.clone(), &method, &path, &query);
    let range_suffix = req
        .headers()
        .get(http::header::RANGE)
        .and_then(|value| value.to_str().ok())
        .map(|value| format!(" range={value}"))
        .unwrap_or_default();
    let _ = observability::event_in_context(
        &trace,
        TRACE_TARGET,
        "request_start",
        Some(format_args!(
            "method={} path={} query={}{}",
            method, path, query, range_suffix
        )),
    );

    // Acquire request permit before body collection to bound memory.
    let req_permit = if let Ok(Ok(permit)) = tokio::time::timeout(
        state.config.request_wait_timeout,
        Arc::clone(&state.request_semaphore).acquire_owned(),
    )
    .await
    {
        permit
    } else {
        let resp = S3Response::error(&ServerError::SlowDown, "");
        return Ok(s3_response_to_hyper(
            resp,
            None,
            state.config.stream_read_chunk_size,
            response_trace,
        ));
    };

    let (parts, body) = req.into_parts();

    // Check if this request should use the streaming write path.
    if let Some(op) = is_streaming_write(&parts) {
        let chunked = match parse_chunked_mode(&parts) {
            Ok(mode) => mode,
            Err(err) => {
                return Ok(s3_response_to_hyper(
                    S3Response::error(&err, ""),
                    Some(req_permit),
                    state.config.stream_read_chunk_size,
                    response_trace.clone(),
                ))
            }
        };
        let resp = match op {
            StreamingWriteOp::PutObject { bucket, key } => {
                handle_streaming_put(
                    Arc::clone(&state),
                    parts,
                    body,
                    bucket,
                    key,
                    chunked,
                    trace.clone(),
                )
                .await
            }
            StreamingWriteOp::UploadPart {
                bucket,
                key,
                upload_id,
                part_number,
            } => {
                handle_streaming_part(
                    Arc::clone(&state),
                    parts,
                    body,
                    bucket,
                    key,
                    upload_id,
                    part_number,
                    chunked,
                    trace.clone(),
                )
                .await
            }
        };
        return Ok(s3_response_to_hyper(
            resp,
            Some(req_permit),
            state.config.stream_read_chunk_size,
            response_trace,
        ));
    }

    if let Some(bucket) = post_object_bucket(&parts) {
        let resp =
            handle_streaming_post_object(Arc::clone(&state), parts, body, bucket, trace.clone())
                .await;
        return Ok(s3_response_to_hyper(
            resp,
            Some(req_permit),
            state.config.stream_read_chunk_size,
            response_trace,
        ));
    }

    // Non-streaming path: collect the full body for buffered control-plane
    // style requests (mostly XML payloads).
    let body_bytes = match collect_body(body, state.config.body_idle_timeout).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return Ok(s3_response_to_hyper(
                S3Response::error(&err, ""),
                Some(req_permit),
                state.config.stream_read_chunk_size,
                response_trace,
            ));
        }
    };

    let s3req = match S3Request::from_hyper(&parts, body_bytes) {
        Ok(req) => req,
        Err(err) => {
            return Ok(s3_response_to_hyper(
                S3Response::error(&err, ""),
                Some(req_permit),
                state.config.stream_read_chunk_size,
                response_trace,
            ));
        }
    };

    let state_ref = Arc::clone(&state);
    let resp = spawn_blocking_with_trace(trace, move || {
        let frontend = acquire_frontend(&state_ref);
        frontend.handle_s3_request(&s3req)
    })
    .await
    .unwrap_or_else(|_| {
        S3Response::error(
            &ServerError::InvalidRequest {
                reason: "internal error".to_string(),
            },
            "",
        )
    });

    Ok(s3_response_to_hyper(
        resp,
        Some(req_permit),
        state.config.stream_read_chunk_size,
        response_trace,
    ))
}

/// Check if a PUT request should use the streaming write path.
///
/// Returns a `StreamingWriteOp` target for `PutObject` and `UploadPart`
/// requests that are not `CopyObject` (no `x-amz-copy-source` header).
///
fn is_streaming_write(parts: &http::request::Parts) -> Option<StreamingWriteOp> {
    if parts.method != http::Method::PUT {
        return None;
    }

    // Check headers via hyper types (not yet parsed into S3Request).
    let has_copy_source = parts.headers.contains_key("x-amz-copy-source");
    if has_copy_source {
        return None;
    }

    let path = parts.uri.path();
    let query = parts.uri.query().unwrap_or("");
    let method = parts.method.as_str();

    let op = route(method, path, query).ok()?;
    match op {
        S3Operation::PutObject { bucket, key } => Some(StreamingWriteOp::PutObject { bucket, key }),
        S3Operation::UploadPart { bucket, key } => {
            let upload_id = extract_query_param(query, "uploadId")?;
            let part_number: u32 =
                extract_query_param(query, "partNumber").and_then(|s| s.parse().ok())?;
            Some(StreamingWriteOp::UploadPart {
                bucket,
                key,
                upload_id,
                part_number,
            })
        }
        _ => None,
    }
}

/// Parse aws-chunked mode from request headers.
///
/// Returns `ChunkedMode::None` for plain PUT bodies (including missing
/// `x-amz-content-sha256`, `UNSIGNED-PAYLOAD`, and fixed SHA256 hashes).
fn parse_chunked_mode(parts: &http::request::Parts) -> Result<ChunkedMode, ServerError> {
    let content_sha256 = parts
        .headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok());

    let Some(content_sha256) = content_sha256 else {
        return Ok(ChunkedMode::None);
    };

    match content_sha256 {
        "UNSIGNED-PAYLOAD" => Ok(ChunkedMode::None),
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
        | "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
        | "STREAMING-UNSIGNED-PAYLOAD-TRAILER" => {
            // content-encoding must contain aws-chunked.
            let has_aws_chunked = parts
                .headers
                .get("content-encoding")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|ce| {
                    ce.split(',')
                        .any(|part| part.trim().eq_ignore_ascii_case("aws-chunked"))
                });
            if !has_aws_chunked {
                return Err(ServerError::MalformedTrailerError {
                    reason: "content-encoding must contain aws-chunked for streaming uploads"
                        .to_string(),
                });
            }

            // x-amz-decoded-content-length must be present and valid.
            let expected_len_str = parts
                .headers
                .get("x-amz-decoded-content-length")
                .and_then(|v| v.to_str().ok())
                .ok_or(ServerError::MissingContentLength)?;
            let expected_len =
                expected_len_str
                    .parse::<u64>()
                    .map_err(|_| ServerError::InvalidRequest {
                        reason: format!("invalid x-amz-decoded-content-length: {expected_len_str}"),
                    })?;

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

fn post_object_bucket(parts: &http::request::Parts) -> Option<String> {
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
                        break;
                    };
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

async fn handle_streaming_post_object(
    state: Arc<ServerState>,
    parts: http::request::Parts,
    body: Incoming,
    bucket: String,
    trace: observability::TraceContext,
) -> S3Response {
    use base64::Engine;

    let idle_timeout = state.config.body_idle_timeout;

    let s3req = match S3Request::from_hyper_headers(&parts) {
        Ok(req) => req,
        Err(err) => return S3Response::error(&err, ""),
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
    let mut ctx: Option<Arc<super::StreamingPostContext>> = None;
    let mut seen_file = false;
    let mut file_ended = false;

    let mut crc64 = checksum::crc64::Hasher::new();
    let mut sha256 = ring::digest::Context::new(&ring::digest::SHA256);
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
                                let ctx_res = spawn_blocking_with_trace(trace.clone(), move || {
                                    let frontend = acquire_frontend(&st);
                                    frontend.prepare_streaming_post_object(
                                        &req,
                                        &bucket_clone,
                                        &fields_clone,
                                        file_name.as_deref(),
                                    )
                                })
                                .await;
                                match ctx_res {
                                    Ok(Ok(c)) => ctx = Some(Arc::new(c)),
                                    Ok(Err(err)) => return error_response(&err),
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
                                sha256.update(&data);
                                total_size += data.len() as u64;
                                if total_size > MAX_OBJECT_SIZE {
                                    abort_streaming_post_object(&state, c).await;
                                    return error_response(&ServerError::ObjectTooLarge {
                                        size: total_size,
                                        max: MAX_OBJECT_SIZE,
                                    });
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
                                    match tokio::task::spawn_blocking(move || {
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
        match tokio::task::spawn_blocking(move || {
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

    let actual_sha256_b64 =
        base64::engine::general_purpose::STANDARD.encode(sha256.finish().as_ref());
    let crc64 = crc64.finalize();
    let st = Arc::clone(&state);
    let ctx_ref = Arc::clone(&ctx);
    match tokio::task::spawn_blocking(move || {
        let frontend = acquire_frontend(&st);
        frontend.finalize_streaming_post_object(&ctx_ref, crc64, total_size, &actual_sha256_b64)
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

/// Extract a query parameter value from a query string.
fn extract_query_param(query: &str, name: &str) -> Option<String> {
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == name {
                return Some(v.to_string());
            }
        }
    }
    None
}

/// Handle a streaming `PutObject`: read body frame-by-frame, feed chunks to
/// coordinator append API, finalize atomically.
///
/// Each chunk append is dispatched via `spawn_blocking` with a brief frontend
/// lock. Between appends, no frontend is held — body reading is async.
async fn handle_streaming_put(
    state: Arc<ServerState>,
    parts: http::request::Parts,
    body: Incoming,
    bucket: String,
    key: String,
    chunked: ChunkedMode,
    trace: observability::TraceContext,
) -> S3Response {
    let idle_timeout = state.config.body_idle_timeout;

    // 1. Parse headers (no body) and prepare streaming session.
    let s3req = match S3Request::from_hyper_headers(&parts) {
        Ok(req) => req,
        Err(err) => return S3Response::error(&err, ""),
    };

    let state2 = Arc::clone(&state);
    let bucket_clone = bucket.clone();
    let key_clone = key.clone();
    let ctx = match spawn_blocking_with_trace(trace, move || {
        let frontend = acquire_frontend(&state2);
        frontend.prepare_streaming_put(&s3req, &bucket_clone, &key_clone)
    })
    .await
    {
        Ok(Ok(ctx)) => ctx,
        Ok(Err(err)) => return error_response(&err),
        Err(_) => return internal_error_response(),
    };

    // Build chunked decoder if needed.
    let mut decoder = make_chunked_decoder(&chunked, ctx.streaming_signing.as_ref());
    let claimed_payload_sha256 = claimed_payload_sha256_from_parts(&parts);
    let mut payload_sha256_hasher = claimed_payload_sha256
        .as_ref()
        .map(|_| ring::digest::Context::new(&ring::digest::SHA256));

    // If a trailing checksum is declared, prepare an incremental hasher for validation.
    // Otherwise, if an inline checksum header is present, prepare a hasher for that
    // so we can verify the claimed value against the actual streamed body.
    let mut trailing_hasher = trailing_hasher_from_parts(&parts);
    let mut inline_checksum_claim: Option<String> = None;
    if trailing_hasher.is_none() {
        if let Some((h, claimed)) = inline_checksum_hasher_from_parts(&parts) {
            trailing_hasher = Some(h);
            inline_checksum_claim = Some(claimed);
        }
    }

    // 2. Stream body frames, accumulating into internal segment-sized buffers.
    let ctx = Arc::new(ctx);
    let mut hasher = checksum::crc64::Hasher::new();
    let mut segment_index: u32 = 0;
    let mut buf = PooledSegmentBuffer::new(&state);
    let mut total_size: u64 = 0;
    let mut body_started_emitted = false;
    let mut body_timing = StreamingBodyTiming::default();
    let mut body = body;

    loop {
        let frame_wait_start = Instant::now();
        let next_frame = tokio::time::timeout(idle_timeout, body.frame()).await;
        body_timing.frame_wait_us += elapsed_micros(frame_wait_start);
        match next_frame {
            Ok(Some(Ok(frame))) => {
                if let Some(wire_data) = frame.data_ref() {
                    body_timing.data_frames += 1;
                    if let Some(ref mut dec) = decoder {
                        let decode_start = Instant::now();
                        let payload = dec.feed(wire_data);
                        body_timing.decode_us += elapsed_micros(decode_start);
                        let payload = match payload {
                            Ok(p) => p,
                            Err(err) => {
                                abort_streaming(&state, &ctx).await;
                                return error_response(&err);
                            }
                        };
                        let mut ingest = StreamingPutIngestState {
                            hasher: &mut hasher,
                            payload_sha256_hasher: &mut payload_sha256_hasher,
                            trailing_hasher: &mut trailing_hasher,
                            total_size: &mut total_size,
                            buf: &mut buf,
                            segment_index: &mut segment_index,
                            body_started_emitted: &mut body_started_emitted,
                            timing: &mut body_timing,
                        };
                        if let Err(resp) =
                            ingest_streaming_put_payload(&state, &ctx, &payload, &mut ingest).await
                        {
                            return resp;
                        }
                    } else {
                        let mut ingest = StreamingPutIngestState {
                            hasher: &mut hasher,
                            payload_sha256_hasher: &mut payload_sha256_hasher,
                            trailing_hasher: &mut trailing_hasher,
                            total_size: &mut total_size,
                            buf: &mut buf,
                            segment_index: &mut segment_index,
                            body_started_emitted: &mut body_started_emitted,
                            timing: &mut body_timing,
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
                abort_streaming(&state, &ctx).await;
                return error_response(&ServerError::InvalidRequest {
                    reason: "failed to read request body".to_string(),
                });
            }
            Ok(None) => break, // Body complete
            Err(_) => {
                abort_streaming(&state, &ctx).await;
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
            "bucket={} key={} session_id={} body_bytes_received={} full_segments_flushed={} buffered_tail_bytes={} data_frames={} frame_wait_us={} decode_us={} ingest_local_us={} append_wait_us={}",
            ctx.binding.bucket,
            ctx.binding.key,
            ctx.binding.session_id,
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
            abort_streaming(&state, &ctx).await;
            return error_response(&ServerError::IncompleteBody);
        }
        let trailers = dec.into_trailers();
        if let Err(err) = validate_chunked_post_decode(&chunked, total_size, &trailers, &parts) {
            abort_streaming(&state, &ctx).await;
            return error_response(&err);
        }
        match extract_checksum_trailers(&trailers) {
            Ok(tc) => trailer_checksums = tc,
            Err(err) => {
                abort_streaming(&state, &ctx).await;
                return error_response(&err);
            }
        }
    }

    if let (Some(claimed), Some(h)) = (claimed_payload_sha256.as_ref(), payload_sha256_hasher) {
        let actual = sha256_hex_from_digest(h.finish().as_ref());
        if &actual != claimed {
            abort_streaming(&state, &ctx).await;
            return error_response(&ServerError::XAmzContentSHA256Mismatch {
                client_hash: claimed.clone(),
                server_hash: actual,
            });
        }
    }

    // Validate checksum against incrementally computed value.
    if let Some(th) = trailing_hasher {
        let actual_b64 = th.finalize_b64();
        if let Some(ref claimed) = inline_checksum_claim {
            // Inline checksum header: verify against streamed body.
            if *claimed != actual_b64 {
                abort_streaming(&state, &ctx).await;
                return error_response(&ServerError::BadDigest);
            }
        } else {
            // Trailing checksum: exactly one trailer expected.
            match trailer_checksums.len() {
                1 => {
                    if trailer_checksums[0].1 != actual_b64 {
                        abort_streaming(&state, &ctx).await;
                        return error_response(&ServerError::BadDigest);
                    }
                }
                0 => {} // No checksum trailer in body — nothing to validate.
                _ => {
                    // Multiple distinct checksum trailers — reject.
                    abort_streaming(&state, &ctx).await;
                    return error_response(&ServerError::InvalidRequest {
                        reason: "multiple checksum trailers not supported".to_string(),
                    });
                }
            }
        }
    }

    // 3. Flush remaining buffer.
    let had_tail = !buf.is_empty();
    if had_tail {
        let idx = segment_index;
        emit_streaming_put_event(
            &ctx,
            "streaming_put_tail_segment_ready",
            format_args!(
                "bucket={} key={} session_id={} segment_index={} segment_bytes={} body_bytes_received={}",
                ctx.binding.bucket,
                ctx.binding.key,
                ctx.binding.session_id,
                idx,
                buf.len(),
                total_size
            ),
        );
        let ctx_ref = Arc::clone(&ctx);
        let st = Arc::clone(&state);
        emit_streaming_put_event(
            &ctx,
            "streaming_put_append_dispatch",
            format_args!(
                "bucket={} key={} session_id={} segment_index={} segment_bytes={} body_bytes_received={}",
                ctx.binding.bucket,
                ctx.binding.key,
                ctx.binding.session_id,
                idx,
                buf.len(),
                total_size
            ),
        );
        let trace = ctx.trace.clone();
        match spawn_blocking_with_trace(trace, move || {
            emit_streaming_put_event(
                &ctx_ref,
                "streaming_put_append_worker_start",
                format_args!(
                    "bucket={} key={} session_id={} segment_index={} segment_bytes={}",
                    ctx_ref.binding.bucket,
                    ctx_ref.binding.key,
                    ctx_ref.binding.session_id,
                    idx,
                    buf.len()
                ),
            );
            let frontend = acquire_frontend(&st);
            emit_streaming_put_event(
                &ctx_ref,
                "streaming_put_append_frontend_acquired",
                format_args!(
                    "bucket={} key={} session_id={} segment_index={} segment_bytes={}",
                    ctx_ref.binding.bucket,
                    ctx_ref.binding.key,
                    ctx_ref.binding.session_id,
                    idx,
                    buf.len()
                ),
            );
            let result = frontend.streaming_append_segment(&ctx_ref, idx, &buf);
            (result, buf)
        })
        .await
        {
            Ok((Ok(()), _buf)) => {}
            Ok((Err(err), _buf)) => {
                abort_streaming(&state, &ctx).await;
                return error_response(&err);
            }
            Err(_) => {
                abort_streaming(&state, &ctx).await;
                return internal_error_response();
            }
        }
    }

    // 4. Finalize the streaming upload.
    let crc64 = hasher.finalize();
    emit_streaming_put_event(
        &ctx,
        "streaming_put_finalize_ready",
        format_args!(
            "bucket={} key={} session_id={} body_bytes_received={} segment_count={} trailer_checksums={}",
            ctx.binding.bucket,
            ctx.binding.key,
            ctx.binding.session_id,
            total_size,
            segment_index + u32::from(had_tail),
            trailer_checksums.len()
        ),
    );
    let ctx_ref = Arc::clone(&ctx);
    let st = Arc::clone(&state);
    emit_streaming_put_event(
        &ctx,
        "streaming_put_finalize_dispatch",
        format_args!(
            "bucket={} key={} session_id={} body_bytes_received={} segment_count={} trailer_checksums={}",
            ctx.binding.bucket,
            ctx.binding.key,
            ctx.binding.session_id,
            total_size,
            segment_index + u32::from(had_tail),
            trailer_checksums.len()
        ),
    );
    let trace = ctx.trace.clone();
    match spawn_blocking_with_trace(trace, move || {
        emit_streaming_put_event(
            &ctx_ref,
            "streaming_put_finalize_worker_start",
            format_args!(
                "bucket={} key={} session_id={} body_bytes_received={}",
                ctx_ref.binding.bucket, ctx_ref.binding.key, ctx_ref.binding.session_id, total_size
            ),
        );
        let frontend = acquire_frontend(&st);
        emit_streaming_put_event(
            &ctx_ref,
            "streaming_put_finalize_frontend_acquired",
            format_args!(
                "bucket={} key={} session_id={} body_bytes_received={}",
                ctx_ref.binding.bucket, ctx_ref.binding.key, ctx_ref.binding.session_id, total_size
            ),
        );
        frontend.finalize_streaming_put(&ctx_ref, crc64, total_size, &trailer_checksums)
    })
    .await
    {
        Ok(Ok(resp)) => resp,
        Ok(Err(err)) => {
            abort_streaming(&state, &ctx).await;
            error_response(&err)
        }
        Err(_) => {
            abort_streaming(&state, &ctx).await;
            internal_error_response()
        }
    }
}

/// Best-effort abort of a streaming upload session.
async fn abort_streaming(state: &Arc<ServerState>, ctx: &Arc<super::StreamingPutContext>) {
    let st = Arc::clone(state);
    let ctx = Arc::clone(ctx);
    let _ = tokio::task::spawn_blocking(move || {
        let frontend = acquire_frontend(&st);
        frontend.abort_streaming_put(&ctx);
    })
    .await;
}

struct StreamingPutIngestState<'a> {
    hasher: &'a mut checksum::crc64::Hasher,
    payload_sha256_hasher: &'a mut Option<ring::digest::Context>,
    trailing_hasher: &'a mut Option<TrailingChecksumHasher>,
    total_size: &'a mut u64,
    buf: &'a mut PooledSegmentBuffer,
    segment_index: &'a mut u32,
    body_started_emitted: &'a mut bool,
    timing: &'a mut StreamingBodyTiming,
}

fn emit_streaming_put_event(
    ctx: &Arc<super::StreamingPutContext>,
    name: &'static str,
    fields: std::fmt::Arguments<'_>,
) {
    let _ = observability::event_in_context(&ctx.trace, TRACE_TARGET, name, Some(fields));
}

async fn ingest_streaming_put_payload(
    state: &Arc<ServerState>,
    ctx: &Arc<super::StreamingPutContext>,
    payload: &[u8],
    ingest: &mut StreamingPutIngestState<'_>,
) -> Result<(), S3Response> {
    if payload.is_empty() {
        return Ok(());
    }

    let accounting_start = Instant::now();
    ingest.hasher.update(payload);
    if let Some(h) = ingest.payload_sha256_hasher.as_mut() {
        h.update(payload);
    }
    if let Some(th) = ingest.trailing_hasher.as_mut() {
        th.update(payload);
    }
    *ingest.total_size += payload.len() as u64;
    if *ingest.total_size > MAX_OBJECT_SIZE {
        abort_streaming(state, ctx).await;
        return Err(error_response(&ServerError::ObjectTooLarge {
            size: *ingest.total_size,
            max: MAX_OBJECT_SIZE,
        }));
    }
    if !*ingest.body_started_emitted {
        *ingest.body_started_emitted = true;
        emit_streaming_put_event(
            ctx,
            "streaming_put_body_started",
            format_args!(
                "bucket={} key={} session_id={} frame_bytes={} body_bytes_received={}",
                ctx.binding.bucket,
                ctx.binding.key,
                ctx.binding.session_id,
                payload.len(),
                *ingest.total_size
            ),
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
        emit_streaming_put_event(
            ctx,
            "streaming_put_segment_ready",
            format_args!(
                "bucket={} key={} session_id={} segment_index={} segment_bytes={} body_bytes_received={}",
                ctx.binding.bucket,
                ctx.binding.key,
                ctx.binding.session_id,
                idx,
                flush_data.len(),
                *ingest.total_size
            ),
        );
        ingest.timing.ingest_local_us += elapsed_micros(fill_start);
        let ctx_ref = Arc::clone(ctx);
        let st = Arc::clone(state);
        let dispatch_start = Instant::now();
        emit_streaming_put_event(
            ctx,
            "streaming_put_append_dispatch",
            format_args!(
                "bucket={} key={} session_id={} segment_index={} segment_bytes={} body_bytes_received={}",
                ctx.binding.bucket,
                ctx.binding.key,
                ctx.binding.session_id,
                idx,
                flush_data.len(),
                *ingest.total_size
            ),
        );
        let trace = ctx.trace.clone();
        match spawn_blocking_with_trace(trace, move || {
            emit_streaming_put_event(
                &ctx_ref,
                "streaming_put_append_worker_start",
                format_args!(
                    "bucket={} key={} session_id={} segment_index={} segment_bytes={}",
                    ctx_ref.binding.bucket,
                    ctx_ref.binding.key,
                    ctx_ref.binding.session_id,
                    idx,
                    flush_data.len()
                ),
            );
            let frontend = acquire_frontend(&st);
            emit_streaming_put_event(
                &ctx_ref,
                "streaming_put_append_frontend_acquired",
                format_args!(
                    "bucket={} key={} session_id={} segment_index={} segment_bytes={}",
                    ctx_ref.binding.bucket,
                    ctx_ref.binding.key,
                    ctx_ref.binding.session_id,
                    idx,
                    flush_data.len()
                ),
            );
            let result = frontend.streaming_append_segment(&ctx_ref, idx, &flush_data);
            (result, flush_data)
        })
        .await
        {
            Ok((Ok(()), _flush_data)) => {}
            Ok((Err(err), _flush_data)) => {
                abort_streaming(state, ctx).await;
                return Err(error_response(&err));
            }
            Err(_) => {
                abort_streaming(state, ctx).await;
                return Err(internal_error_response());
            }
        }
        ingest.timing.append_wait_us += elapsed_micros(dispatch_start);
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

/// Handle a streaming `UploadPart`: read body frame-by-frame, feed chunks to
/// coordinator append API, finalize atomically.
#[allow(clippy::too_many_arguments)]
async fn handle_streaming_part(
    state: Arc<ServerState>,
    parts: http::request::Parts,
    body: Incoming,
    bucket: String,
    key: String,
    upload_id: String,
    part_number: u32,
    chunked: ChunkedMode,
    trace: observability::TraceContext,
) -> S3Response {
    let idle_timeout = state.config.body_idle_timeout;

    // 1. Parse headers and prepare streaming session.
    let s3req = match S3Request::from_hyper_headers(&parts) {
        Ok(req) => req,
        Err(err) => return S3Response::error(&err, ""),
    };

    let state2 = Arc::clone(&state);
    let bucket_clone = bucket.clone();
    let key_clone = key.clone();
    let upload_id_clone = upload_id.clone();
    let ctx = match spawn_blocking_with_trace(trace, move || {
        let frontend = acquire_frontend(&state2);
        frontend.prepare_streaming_part(
            &s3req,
            &bucket_clone,
            &key_clone,
            &upload_id_clone,
            part_number,
        )
    })
    .await
    {
        Ok(Ok(ctx)) => ctx,
        Ok(Err(err)) => return error_response(&err),
        Err(_) => return internal_error_response(),
    };

    // Build chunked decoder if needed.
    let mut decoder = make_chunked_decoder(&chunked, ctx.streaming_signing.as_ref());
    let claimed_payload_sha256 = claimed_payload_sha256_from_parts(&parts);
    let mut payload_sha256_hasher = claimed_payload_sha256
        .as_ref()
        .map(|_| ring::digest::Context::new(&ring::digest::SHA256));

    // If a trailing checksum is declared, prepare an incremental hasher for validation.
    // Otherwise, if an inline checksum header is present, prepare a hasher for that
    // so we can verify the claimed value against the actual streamed body.
    let mut trailing_hasher = trailing_hasher_from_parts(&parts);
    let mut inline_checksum_claim: Option<String> = None;
    if trailing_hasher.is_none() {
        if let Some((h, claimed)) = inline_checksum_hasher_from_parts(&parts) {
            trailing_hasher = Some(h);
            inline_checksum_claim = Some(claimed);
        }
    }
    // 2. Stream body frames, accumulating into internal segment-sized buffers.
    let ctx = Arc::new(ctx);
    let mut hasher = checksum::crc64::Hasher::new();
    let mut segment_index: u32 = 0;
    let mut buf = PooledSegmentBuffer::new(&state);
    let mut total_size: u64 = 0;
    let mut body_started_emitted = false;
    let mut body_timing = StreamingBodyTiming::default();
    let mut body = body;

    loop {
        let frame_wait_start = Instant::now();
        let next_frame = tokio::time::timeout(idle_timeout, body.frame()).await;
        body_timing.frame_wait_us += elapsed_micros(frame_wait_start);
        match next_frame {
            Ok(Some(Ok(frame))) => {
                if let Some(wire_data) = frame.data_ref() {
                    body_timing.data_frames += 1;
                    if let Some(ref mut dec) = decoder {
                        let decode_start = Instant::now();
                        let payload = dec.feed(wire_data);
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
                            trailing_hasher: &mut trailing_hasher,
                            total_size: &mut total_size,
                            buf: &mut buf,
                            segment_index: &mut segment_index,
                            body_started_emitted: &mut body_started_emitted,
                            timing: &mut body_timing,
                        };
                        if let Err(resp) =
                            ingest_streaming_part_payload(&state, &ctx, &payload, &mut ingest).await
                        {
                            return resp;
                        }
                    } else {
                        let mut ingest = StreamingPartIngestState {
                            hasher: &mut hasher,
                            payload_sha256_hasher: &mut payload_sha256_hasher,
                            trailing_hasher: &mut trailing_hasher,
                            total_size: &mut total_size,
                            buf: &mut buf,
                            segment_index: &mut segment_index,
                            body_started_emitted: &mut body_started_emitted,
                            timing: &mut body_timing,
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
            "bucket={} key={} upload_id={} part_number={} session_id={} body_bytes_received={} full_segments_flushed={} buffered_tail_bytes={} data_frames={} frame_wait_us={} decode_us={} ingest_local_us={} append_wait_us={}",
            ctx.binding.object.bucket,
            ctx.binding.object.key,
            ctx.binding.upload_id,
            ctx.binding.part_number,
            ctx.binding.object.session_id,
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
    // Extract checksum trailers from aws-chunked body.
    let mut trailer_checksums: Vec<(String, String)> = Vec::new();
    if let Some(dec) = decoder {
        if !dec.is_done() {
            abort_streaming_part_ctx(&state, &ctx).await;
            return error_response(&ServerError::IncompleteBody);
        }
        let trailers = dec.into_trailers();
        if let Err(err) = validate_chunked_post_decode(&chunked, total_size, &trailers, &parts) {
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

    // Validate checksum against incrementally computed value.
    // Keep the computed RawChecksum for passing to finalization.
    let computed_checksum = if let Some(th) = trailing_hasher {
        use base64::Engine;
        let cksum = th.finalize_raw();
        let actual_b64 = base64::engine::general_purpose::STANDARD.encode(cksum.bytes());
        if let Some(ref claimed) = inline_checksum_claim {
            // Inline checksum header: verify against streamed body.
            if *claimed != actual_b64 {
                abort_streaming_part_ctx(&state, &ctx).await;
                return error_response(&ServerError::BadDigest);
            }
        } else {
            // Trailing checksum: validate if present.
            match trailer_checksums.len() {
                1 => {
                    if trailer_checksums[0].1 != actual_b64 {
                        abort_streaming_part_ctx(&state, &ctx).await;
                        return error_response(&ServerError::BadDigest);
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
                "bucket={} key={} upload_id={} part_number={} session_id={} segment_index={} segment_bytes={} body_bytes_received={}",
                ctx.binding.object.bucket,
                ctx.binding.object.key,
                ctx.binding.upload_id,
                ctx.binding.part_number,
                ctx.binding.object.session_id,
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
                "bucket={} key={} upload_id={} part_number={} session_id={} segment_index={} segment_bytes={} body_bytes_received={}",
                ctx.binding.object.bucket,
                ctx.binding.object.key,
                ctx.binding.upload_id,
                ctx.binding.part_number,
                ctx.binding.object.session_id,
                idx,
                buf.len(),
                total_size
            ),
        );
        let trace = ctx.trace.clone();
        match spawn_blocking_with_trace(trace, move || {
            emit_streaming_part_event(
                &ctx_ref,
                "streaming_part_append_worker_start",
                format_args!(
                    "bucket={} key={} upload_id={} part_number={} session_id={} segment_index={} segment_bytes={}",
                    ctx_ref.binding.object.bucket,
                    ctx_ref.binding.object.key,
                    ctx_ref.binding.upload_id,
                    ctx_ref.binding.part_number,
                    ctx_ref.binding.object.session_id,
                    idx,
                    buf.len()
                ),
            );
            let frontend = acquire_frontend(&st);
            emit_streaming_part_event(
                &ctx_ref,
                "streaming_part_append_frontend_acquired",
                format_args!(
                    "bucket={} key={} upload_id={} part_number={} session_id={} segment_index={} segment_bytes={}",
                    ctx_ref.binding.object.bucket,
                    ctx_ref.binding.object.key,
                    ctx_ref.binding.upload_id,
                    ctx_ref.binding.part_number,
                    ctx_ref.binding.object.session_id,
                    idx,
                    buf.len()
                ),
            );
            let result = frontend.streaming_append_part_segment(&ctx_ref, idx, &buf);
            (result, buf)
        })
        .await
        {
            Ok((Ok(()), _buf)) => {}
            Ok((Err(err), _buf)) => {
                abort_streaming_part_ctx(&state, &ctx).await;
                return error_response(&err);
            }
            Err(_) => {
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
            "bucket={} key={} upload_id={} part_number={} session_id={} body_bytes_received={} segment_count={} trailer_checksums={}",
            ctx.binding.object.bucket,
            ctx.binding.object.key,
            ctx.binding.upload_id,
            ctx.binding.part_number,
            ctx.binding.object.session_id,
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
            "bucket={} key={} upload_id={} part_number={} session_id={} body_bytes_received={} segment_count={} trailer_checksums={}",
            ctx.binding.object.bucket,
            ctx.binding.object.key,
            ctx.binding.upload_id,
            ctx.binding.part_number,
            ctx.binding.object.session_id,
            total_size,
            segment_index + u32::from(had_tail),
            trailer_checksums.len()
        ),
    );
    let trace = ctx.trace.clone();
    match spawn_blocking_with_trace(trace, move || {
        emit_streaming_part_event(
            &ctx_ref,
            "streaming_part_finalize_worker_start",
            format_args!(
                "bucket={} key={} upload_id={} part_number={} session_id={} body_bytes_received={}",
                ctx_ref.binding.object.bucket,
                ctx_ref.binding.object.key,
                ctx_ref.binding.upload_id,
                ctx_ref.binding.part_number,
                ctx_ref.binding.object.session_id,
                total_size
            ),
        );
        let frontend = acquire_frontend(&st);
        emit_streaming_part_event(
            &ctx_ref,
            "streaming_part_finalize_frontend_acquired",
            format_args!(
                "bucket={} key={} upload_id={} part_number={} session_id={} body_bytes_received={}",
                ctx_ref.binding.object.bucket,
                ctx_ref.binding.object.key,
                ctx_ref.binding.upload_id,
                ctx_ref.binding.part_number,
                ctx_ref.binding.object.session_id,
                total_size
            ),
        );
        frontend.finalize_streaming_part(
            &ctx_ref,
            crc64,
            total_size,
            &trailer_checksums,
            computed_checksum,
        )
    })
    .await
    {
        Ok(Ok(resp)) => resp,
        Ok(Err(err)) => {
            abort_streaming_part_ctx(&state, &ctx).await;
            error_response(&err)
        }
        Err(_) => {
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
    trailing_hasher: &'a mut Option<TrailingChecksumHasher>,
    total_size: &'a mut u64,
    buf: &'a mut PooledSegmentBuffer,
    segment_index: &'a mut u32,
    body_started_emitted: &'a mut bool,
    timing: &'a mut StreamingBodyTiming,
}

fn emit_streaming_part_event(
    ctx: &Arc<super::StreamingPartContext>,
    name: &'static str,
    fields: std::fmt::Arguments<'_>,
) {
    let _ = observability::event_in_context(&ctx.trace, TRACE_TARGET, name, Some(fields));
}

async fn ingest_streaming_part_payload(
    state: &Arc<ServerState>,
    ctx: &Arc<super::StreamingPartContext>,
    payload: &[u8],
    ingest: &mut StreamingPartIngestState<'_>,
) -> Result<(), S3Response> {
    if payload.is_empty() {
        return Ok(());
    }

    let accounting_start = Instant::now();
    ingest.hasher.update(payload);
    if let Some(h) = ingest.payload_sha256_hasher.as_mut() {
        h.update(payload);
    }
    if let Some(th) = ingest.trailing_hasher.as_mut() {
        th.update(payload);
    }
    *ingest.total_size += payload.len() as u64;
    if *ingest.total_size > MAX_OBJECT_SIZE {
        abort_streaming_part_ctx(state, ctx).await;
        return Err(error_response(&ServerError::ObjectTooLarge {
            size: *ingest.total_size,
            max: MAX_OBJECT_SIZE,
        }));
    }
    if !*ingest.body_started_emitted {
        *ingest.body_started_emitted = true;
        emit_streaming_part_event(
            ctx,
            "streaming_part_body_started",
            format_args!(
                "bucket={} key={} upload_id={} part_number={} session_id={} frame_bytes={} body_bytes_received={}",
                ctx.binding.object.bucket,
                ctx.binding.object.key,
                ctx.binding.upload_id,
                ctx.binding.part_number,
                ctx.binding.object.session_id,
                payload.len(),
                *ingest.total_size
            ),
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
                "bucket={} key={} upload_id={} part_number={} session_id={} segment_index={} segment_bytes={} body_bytes_received={}",
                ctx.binding.object.bucket,
                ctx.binding.object.key,
                ctx.binding.upload_id,
                ctx.binding.part_number,
                ctx.binding.object.session_id,
                idx,
                flush_data.len(),
                *ingest.total_size
            ),
        );
        ingest.timing.ingest_local_us += elapsed_micros(fill_start);
        let ctx_ref = Arc::clone(ctx);
        let st = Arc::clone(state);
        let dispatch_start = Instant::now();
        emit_streaming_part_event(
            ctx,
            "streaming_part_append_dispatch",
            format_args!(
                "bucket={} key={} upload_id={} part_number={} session_id={} segment_index={} segment_bytes={} body_bytes_received={}",
                ctx.binding.object.bucket,
                ctx.binding.object.key,
                ctx.binding.upload_id,
                ctx.binding.part_number,
                ctx.binding.object.session_id,
                idx,
                flush_data.len(),
                *ingest.total_size
            ),
        );
        let trace = ctx.trace.clone();
        match spawn_blocking_with_trace(trace, move || {
            emit_streaming_part_event(
                &ctx_ref,
                "streaming_part_append_worker_start",
                format_args!(
                    "bucket={} key={} upload_id={} part_number={} session_id={} segment_index={} segment_bytes={}",
                    ctx_ref.binding.object.bucket,
                    ctx_ref.binding.object.key,
                    ctx_ref.binding.upload_id,
                    ctx_ref.binding.part_number,
                    ctx_ref.binding.object.session_id,
                    idx,
                    flush_data.len()
                ),
            );
            let frontend = acquire_frontend(&st);
            emit_streaming_part_event(
                &ctx_ref,
                "streaming_part_append_frontend_acquired",
                format_args!(
                    "bucket={} key={} upload_id={} part_number={} session_id={} segment_index={} segment_bytes={}",
                    ctx_ref.binding.object.bucket,
                    ctx_ref.binding.object.key,
                    ctx_ref.binding.upload_id,
                    ctx_ref.binding.part_number,
                    ctx_ref.binding.object.session_id,
                    idx,
                    flush_data.len()
                ),
            );
            let result = frontend.streaming_append_part_segment(&ctx_ref, idx, &flush_data);
            (result, flush_data)
        })
        .await
        {
            Ok((Ok(()), _flush_data)) => {}
            Ok((Err(err), _flush_data)) => {
                abort_streaming_part_ctx(state, ctx).await;
                return Err(error_response(&err));
            }
            Err(_) => {
                abort_streaming_part_ctx(state, ctx).await;
                return Err(internal_error_response());
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
    parts: &http::request::Parts,
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

    let declared_trailer = parts
        .headers
        .get("x-amz-trailer")
        .and_then(|v| v.to_str().ok());

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
fn trailing_hasher_from_parts(parts: &http::request::Parts) -> Option<TrailingChecksumHasher> {
    let header_val = parts
        .headers
        .get("x-amz-trailer")
        .and_then(|v| v.to_str().ok())?;
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
fn inline_checksum_hasher_from_parts(
    parts: &http::request::Parts,
) -> Option<(TrailingChecksumHasher, String)> {
    // The header names in CHECKSUM_HEADERS (in mod.rs) match the trailer
    // header names used by TrailingChecksumHasher::from_trailer_header.
    for name in &[
        "x-amz-checksum-crc32",
        "x-amz-checksum-crc32c",
        "x-amz-checksum-crc64nvme",
        "x-amz-checksum-sha256",
        "x-amz-checksum-sha1",
    ] {
        if let Some(val) = parts.headers.get(*name).and_then(|v| v.to_str().ok()) {
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
fn claimed_payload_sha256_from_parts(parts: &http::request::Parts) -> Option<String> {
    let value = parts
        .headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())?;
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
        ChunkedMode::Signed { .. } => Some(super::chunked::IncrementalChunkedDecoder::new(
            streaming_ctx.cloned(),
            false,
        )),
        ChunkedMode::SignedTrailer { .. } => Some(super::chunked::IncrementalChunkedDecoder::new(
            streaming_ctx.cloned(),
            true,
        )),
        ChunkedMode::UnsignedTrailer { .. } => {
            Some(super::chunked::IncrementalChunkedDecoder::new(None, true))
        }
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
async fn collect_body(body: Incoming, idle_timeout: Duration) -> Result<Bytes, ServerError> {
    collect_body_with_limit(body, idle_timeout, MAX_BUFFERED_CONTROL_BODY_SIZE).await
}

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
                    return Err(ServerError::ObjectTooLarge {
                        size: 0,
                        max: max_size as u64,
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

fn error_response(err: &ServerError) -> S3Response {
    S3Response::error(err, "")
}

fn internal_error_response() -> S3Response {
    S3Response::error(
        &ServerError::InvalidRequest {
            reason: "internal error".to_string(),
        },
        "",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal `http::request::Parts` for testing `is_streaming_write`.
    fn make_parts(method: &str, uri: &str, headers: &[(&str, &str)]) -> http::request::Parts {
        let mut builder = http::Request::builder().method(method).uri(uri);
        for (k, v) in headers {
            builder = builder.header(*k, *v);
        }
        let (parts, _body) = builder.body(()).unwrap().into_parts();
        parts
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
            Some(StreamingWriteOp::PutObject { ref bucket, ref key, .. })
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
        assert_eq!(is_streaming_write(&parts), None);
    }

    #[test]
    fn streaming_put_get_method() {
        let parts = make_parts(
            "GET",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert_eq!(is_streaming_write(&parts), None);
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
        assert_eq!(is_streaming_write(&parts), None);
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
            Some(StreamingWriteOp::PutObject { ref bucket, ref key, .. })
            if bucket == "mybucket" && key == "mykey"
        ));
    }

    #[test]
    fn streaming_put_no_sha256_header_routed() {
        let parts = make_parts("PUT", "/mybucket/mykey", &[]);
        let result = is_streaming_write(&parts);
        assert!(matches!(
            result,
            Some(StreamingWriteOp::PutObject { ref bucket, ref key, .. })
            if bucket == "mybucket" && key == "mykey"
        ));
    }

    #[test]
    fn parse_chunked_mode_signed_chunked() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[
                ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
                ("content-encoding", "aws-chunked"),
                ("x-amz-decoded-content-length", "100"),
            ],
        );
        let mode = parse_chunked_mode(&parts).unwrap();
        assert!(matches!(mode, ChunkedMode::Signed { expected_len: 100 }));
    }

    #[test]
    fn parse_chunked_mode_signed_trailer_chunked() {
        let parts = make_parts(
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
        let mode = parse_chunked_mode(&parts).unwrap();
        assert!(matches!(
            mode,
            ChunkedMode::SignedTrailer { expected_len: 100 }
        ));
    }

    #[test]
    fn parse_chunked_mode_unsigned_trailer_chunked() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[
                ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
                ("content-encoding", "aws-chunked"),
                ("x-amz-decoded-content-length", "100"),
            ],
        );
        let mode = parse_chunked_mode(&parts).unwrap();
        assert!(matches!(
            mode,
            ChunkedMode::UnsignedTrailer { expected_len: 100 }
        ));
    }

    #[test]
    fn parse_chunked_mode_unsigned_payload_alone_rejected() {
        // STREAMING-UNSIGNED-PAYLOAD (without -TRAILER) is rejected by AWS.
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD")],
        );
        let err = parse_chunked_mode(&parts).unwrap_err();
        assert!(matches!(err, ServerError::InvalidArgument { .. }));
    }

    #[test]
    fn parse_chunked_mode_missing_content_encoding_rejected() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[
                ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
                ("x-amz-decoded-content-length", "100"),
            ],
        );
        let err = parse_chunked_mode(&parts).unwrap_err();
        assert!(matches!(err, ServerError::MalformedTrailerError { .. }));
    }

    #[test]
    fn parse_chunked_mode_missing_decoded_length_rejected() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[
                ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
                ("content-encoding", "aws-chunked"),
            ],
        );
        let err = parse_chunked_mode(&parts).unwrap_err();
        assert!(matches!(err, ServerError::MissingContentLength));
    }

    #[test]
    fn streaming_put_bucket_config_excluded() {
        // PUT /<bucket>?versioning is a bucket config op, not PutObject.
        let parts = make_parts(
            "PUT",
            "/mybucket?versioning",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert_eq!(is_streaming_write(&parts), None);
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
            Some(StreamingWriteOp::PutObject { ref bucket, ref key, .. })
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
            Some(StreamingWriteOp::UploadPart {
                ref bucket,
                ref key,
                ref upload_id,
                part_number: 3,
                ..
            }) if bucket == "mybucket" && key == "mykey" && upload_id == "abc123"
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
            Some(StreamingWriteOp::UploadPart {
                ref bucket,
                ref key,
                ref upload_id,
                part_number: 3,
            }) if bucket == "mybucket" && key == "mykey" && upload_id == "abc123"
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
        assert!(is_streaming_write(&parts).is_none());
    }

    #[test]
    fn post_object_detected() {
        let parts = make_parts("POST", "/mybucket", &[]);
        assert_eq!(post_object_bucket(&parts).as_deref(), Some("mybucket"));
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
    }

    #[test]
    fn trailing_hasher_from_parts_csv() {
        // P1: x-amz-trailer can be comma-separated; first recognized name wins.
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-trailer", "x-amz-checksum-type, x-amz-checksum-crc32")],
        );
        let hasher = trailing_hasher_from_parts(&parts);
        assert!(hasher.is_some());
        // Verify it's a CRC32 hasher by finalizing empty data.
        let cksum = hasher.unwrap().finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Crc32);
    }

    #[test]
    fn trailing_hasher_from_parts_single() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-trailer", "x-amz-checksum-sha256")],
        );
        let hasher = trailing_hasher_from_parts(&parts);
        assert!(hasher.is_some());
        let cksum = hasher.unwrap().finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Sha256);
    }

    #[test]
    fn trailing_hasher_from_parts_none_when_no_header() {
        let parts = make_parts("PUT", "/mybucket/mykey", &[]);
        assert!(trailing_hasher_from_parts(&parts).is_none());
    }

    #[test]
    fn claimed_payload_sha256_from_parts_recognizes_fixed_hash() {
        let fixed = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let parts = make_parts("PUT", "/mybucket/mykey", &[("x-amz-content-sha256", fixed)]);
        assert_eq!(
            claimed_payload_sha256_from_parts(&parts).as_deref(),
            Some(fixed)
        );
    }

    #[test]
    fn claimed_payload_sha256_from_parts_ignores_sentinel_values() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert!(claimed_payload_sha256_from_parts(&parts).is_none());

        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")],
        );
        assert!(claimed_payload_sha256_from_parts(&parts).is_none());
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
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-checksum-crc32", "AAAAAA==")],
        );
        let result = inline_checksum_hasher_from_parts(&parts);
        assert!(result.is_some());
        let (h, claimed) = result.unwrap();
        assert_eq!(claimed, "AAAAAA==");
        let cksum = h.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Crc32);
    }

    #[test]
    fn inline_checksum_hasher_none_when_no_checksum() {
        let parts = make_parts("PUT", "/mybucket/mykey", &[]);
        assert!(inline_checksum_hasher_from_parts(&parts).is_none());
    }

    #[test]
    fn inline_checksum_hasher_picks_sha256() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-checksum-sha256", "dGVzdA==")],
        );
        let result = inline_checksum_hasher_from_parts(&parts);
        assert!(result.is_some());
        let (h, claimed) = result.unwrap();
        assert_eq!(claimed, "dGVzdA==");
        let cksum = h.finalize_raw();
        assert_eq!(cksum.algorithm(), ChecksumAlgorithm::Sha256);
    }

    #[test]
    fn inline_checksum_hasher_not_triggered_by_non_checksum_headers() {
        // x-amz-checksum-algorithm is not a checksum value header.
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-checksum-algorithm", "CRC32")],
        );
        assert!(inline_checksum_hasher_from_parts(&parts).is_none());
    }
}
