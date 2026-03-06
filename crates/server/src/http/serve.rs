/// Async hyper HTTP server loop with frontend pool and backpressure.
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

use super::request::{S3Request, MAX_BODY_SIZE};
use super::response::S3Response;
use super::router::{route, S3Operation};
use super::s3_response_to_hyper;
use super::HttpFrontend;
use crate::coordinator::MAX_OBJECT_SIZE;
use crate::error::ServerError;

/// Default internal chunk payload size for streaming writes (4 MiB).
const STREAM_CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// Tunable timeouts for the HTTP serve layer.
pub struct ServeConfig {
    /// Time allowed for a client to send request headers. Also serves as the
    /// idle timeout between keep-alive requests.
    pub header_read_timeout: Duration,
    /// Time a request will wait for a processing slot before being shed with
    /// 503 SlowDown.
    pub request_wait_timeout: Duration,
    /// Per-frame idle timeout for body reads. Resets on every chunk so
    /// slow-but-steady uploads complete; only truly stalled connections are
    /// killed.
    pub body_idle_timeout: Duration,
}

impl Default for ServeConfig {
    fn default() -> Self {
        Self {
            header_read_timeout: Duration::from_secs(30),
            request_wait_timeout: Duration::from_secs(5),
            body_idle_timeout: Duration::from_secs(30),
        }
    }
}

/// Shared server state: frontend pool, round-robin counter, request semaphore,
/// and timeout configuration.
struct ServerState {
    pool: Vec<Mutex<HttpFrontend>>,
    counter: AtomicUsize,
    request_semaphore: Semaphore,
    config: ServeConfig,
}

/// Run the HTTP server, accepting connections and dispatching to the frontend pool.
///
/// Two layers of admission control:
/// - A connection semaphore (`max_connections`) limits concurrent TCP connections.
/// - A request semaphore (pool size) limits concurrent in-flight requests,
///   acquired before body collection to bound memory.
///
/// Connection-level timeouts prevent idle/slow clients from pinning slots.
/// Requests that cannot acquire a processing slot within REQUEST_WAIT_TIMEOUT
/// are shed with 503 SlowDown.
pub async fn serve(
    listener: TcpListener,
    frontends: Vec<HttpFrontend>,
    max_connections: u32,
    config: ServeConfig,
) {
    assert!(!frontends.is_empty(), "at least one frontend required");
    let pool_size = frontends.len();

    let header_read_timeout = config.header_read_timeout;
    let state = Arc::new(ServerState {
        pool: frontends.into_iter().map(Mutex::new).collect(),
        counter: AtomicUsize::new(0),
        request_semaphore: Semaphore::new(pool_size),
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
                eprintln!("accept error: {}", e);
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

/// Handle a single HTTP request: collect body, parse, dispatch, return response.
///
/// For streaming-eligible writes (PutObject/UploadPart with UNSIGNED-PAYLOAD
/// and no copy source), body frames are consumed incrementally and fed to
/// coordinator chunk appends. All other requests collect the full body first.
///
/// Errors are always converted to S3 XML error responses.
async fn handle(
    state: Arc<ServerState>,
    req: Request<Incoming>,
) -> Result<http::Response<Full<Bytes>>, Infallible> {
    // Acquire request permit before body collection to bound memory.
    let _req_permit = match tokio::time::timeout(
        state.config.request_wait_timeout,
        state.request_semaphore.acquire(),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        _ => {
            let resp = S3Response::error(&ServerError::SlowDown, "");
            return Ok(s3_response_to_hyper(resp));
        }
    };

    let (parts, body) = req.into_parts();

    // Check if this request should use the streaming write path.
    if let Some((bucket, key)) = is_streaming_put(&parts) {
        let resp = handle_streaming_put(Arc::clone(&state), parts, body, bucket, key).await;
        return Ok(s3_response_to_hyper(resp));
    }

    // Non-streaming path: collect full body, parse, dispatch.
    let body_bytes = match collect_body(body, state.config.body_idle_timeout).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return Ok(s3_response_to_hyper(S3Response::error(&err, "")));
        }
    };

    let s3req = match S3Request::from_hyper(&parts, body_bytes) {
        Ok(req) => req,
        Err(err) => {
            return Ok(s3_response_to_hyper(S3Response::error(&err, "")));
        }
    };

    let state_ref = Arc::clone(&state);
    let resp = tokio::task::spawn_blocking(move || {
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

    Ok(s3_response_to_hyper(resp))
}

/// Check if a PUT request should use the streaming write path.
///
/// Returns `Some((bucket, key))` for PutObject requests that:
/// - Are not CopyObject (no `x-amz-copy-source` header)
/// - Use UNSIGNED-PAYLOAD (body not needed for auth verification)
/// - Are not aws-chunked (no STREAMING-* content hash)
///
/// Currently gated: returns `None` unconditionally because GET for stream-put
/// objects is not yet implemented (Phase 4). Remove the gate once the read
/// path supports chunk manifests.
fn is_streaming_put(parts: &http::request::Parts) -> Option<(String, String)> {
    // Gate: stream-put objects are not readable via GET until Phase 4.
    let _ = parts;
    if true {
        return None;
    }

    #[allow(unreachable_code)]
    is_streaming_put_inner(parts)
}

/// Inner logic for streaming PUT eligibility, separated for testability.
fn is_streaming_put_inner(parts: &http::request::Parts) -> Option<(String, String)> {
    if parts.method != http::Method::PUT {
        return None;
    }

    // Check headers via hyper types (not yet parsed into S3Request).
    let has_copy_source = parts.headers.contains_key("x-amz-copy-source");
    if has_copy_source {
        return None;
    }

    let content_sha256 = parts
        .headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok());

    match content_sha256 {
        Some("UNSIGNED-PAYLOAD") => {} // Eligible for streaming
        Some(v) if v.starts_with("STREAMING-") => return None, // Phase 3b
        Some(_) => return None, // Real SHA256 hash — need full body for verification
        None => return None,    // No header — need full body for auth
    }

    let path = parts.uri.path();
    let query = parts.uri.query().unwrap_or("");
    let method = parts.method.as_str();

    // Route to check if this is PutObject (not bucket config or other PUT ops).
    let op = route(method, path, query).ok()?;
    match op {
        S3Operation::PutObject { bucket, key } => Some((bucket, key)),
        // TODO: Phase 3a UploadPart streaming (needs begin_stream_part coordinator method)
        _ => None,
    }
}

/// Handle a streaming PutObject: read body frame-by-frame, feed chunks to
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
    let ctx = match tokio::task::spawn_blocking(move || {
        let frontend = acquire_frontend(&state2);
        frontend.prepare_streaming_put(&s3req, &bucket_clone, &key_clone)
    })
    .await
    {
        Ok(Ok(ctx)) => ctx,
        Ok(Err(err)) => return error_response(&err),
        Err(_) => return internal_error_response(),
    };

    // 2. Stream body frames, accumulating into STREAM_CHUNK_SIZE buffers.
    let ctx = Arc::new(ctx);
    let mut hasher = crc64::Hasher::new();
    let mut chunk_index: u32 = 0;
    let mut buf = Vec::with_capacity(STREAM_CHUNK_SIZE);
    let mut total_size: u64 = 0;
    let mut body = body;

    loop {
        match tokio::time::timeout(idle_timeout, body.frame()).await {
            Ok(Some(Ok(frame))) => {
                if let Some(chunk) = frame.data_ref() {
                    hasher.update(chunk);
                    total_size += chunk.len() as u64;
                    if total_size > MAX_OBJECT_SIZE {
                        abort_streaming(&state, &ctx).await;
                        return error_response(&ServerError::ObjectTooLarge {
                            size: total_size,
                            max: MAX_OBJECT_SIZE,
                        });
                    }
                    buf.extend_from_slice(chunk);

                    // Flush when buffer reaches chunk size.
                    while buf.len() >= STREAM_CHUNK_SIZE {
                        let flush_data: Vec<u8> = buf.drain(..STREAM_CHUNK_SIZE).collect();
                        let idx = chunk_index;
                        chunk_index += 1;
                        let ctx_ref = Arc::clone(&ctx);
                        let st = Arc::clone(&state);
                        match tokio::task::spawn_blocking(move || {
                            let frontend = acquire_frontend(&st);
                            frontend.streaming_append_chunk(&ctx_ref, idx, &flush_data)
                        })
                        .await
                        {
                            Ok(Ok(())) => {}
                            Ok(Err(err)) => {
                                abort_streaming(&state, &ctx).await;
                                return error_response(&err);
                            }
                            Err(_) => {
                                abort_streaming(&state, &ctx).await;
                                return internal_error_response();
                            }
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

    // 3. Flush remaining buffer.
    if !buf.is_empty() {
        let idx = chunk_index;
        let ctx_ref = Arc::clone(&ctx);
        let st = Arc::clone(&state);
        match tokio::task::spawn_blocking(move || {
            let frontend = acquire_frontend(&st);
            frontend.streaming_append_chunk(&ctx_ref, idx, &buf)
        })
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
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
    let ctx_ref = Arc::clone(&ctx);
    let st = Arc::clone(&state);
    match tokio::task::spawn_blocking(move || {
        let frontend = acquire_frontend(&st);
        frontend.finalize_streaming_put(&ctx_ref, crc64, total_size)
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
async fn abort_streaming(
    state: &Arc<ServerState>,
    ctx: &Arc<super::StreamingPutContext>,
) {
    let st = Arc::clone(state);
    let ctx = Arc::clone(ctx);
    let _ = tokio::task::spawn_blocking(move || {
        let frontend = acquire_frontend(&st);
        frontend.abort_streaming_put(&ctx);
    })
    .await;
}

/// Acquire a frontend from the pool using round-robin with try_lock.
fn acquire_frontend(state: &ServerState) -> MutexGuard<'_, HttpFrontend> {
    let pool_size = state.pool.len();
    let start = state.counter.fetch_add(1, Ordering::Relaxed) % pool_size;

    for i in 0..pool_size {
        let idx = (start + i) % pool_size;
        if let Ok(frontend) = state.pool[idx].try_lock() {
            return frontend;
        }
    }

    state.pool[start].lock().expect("frontend mutex poisoned")
}

/// Collect a request body with size limiting and per-frame idle timeout.
///
/// Each call to `frame()` is individually wrapped in a timeout that resets on
/// every chunk. A client sending data steadily (even slowly) will never be
/// timed out; only truly stalled connections are killed.
async fn collect_body(body: Incoming, idle_timeout: Duration) -> Result<Bytes, ServerError> {
    let mut limited = Limited::new(body, MAX_BODY_SIZE);
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
                        max: MAX_BODY_SIZE as u64,
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

    /// Build a minimal `http::request::Parts` for testing `is_streaming_put_inner`.
    fn make_parts(
        method: &str,
        uri: &str,
        headers: &[(&str, &str)],
    ) -> http::request::Parts {
        let mut builder = http::Request::builder()
            .method(method)
            .uri(uri);
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
        let result = is_streaming_put_inner(&parts);
        assert_eq!(result, Some(("mybucket".to_string(), "mykey".to_string())));
    }

    #[test]
    fn streaming_put_not_put_method() {
        let parts = make_parts(
            "POST",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert_eq!(is_streaming_put_inner(&parts), None);
    }

    #[test]
    fn streaming_put_get_method() {
        let parts = make_parts(
            "GET",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert_eq!(is_streaming_put_inner(&parts), None);
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
        assert_eq!(is_streaming_put_inner(&parts), None);
    }

    #[test]
    fn streaming_put_real_sha256_excluded() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")],
        );
        assert_eq!(is_streaming_put_inner(&parts), None);
    }

    #[test]
    fn streaming_put_no_sha256_header_excluded() {
        let parts = make_parts("PUT", "/mybucket/mykey", &[]);
        assert_eq!(is_streaming_put_inner(&parts), None);
    }

    #[test]
    fn streaming_put_chunked_encoding_excluded() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")],
        );
        assert_eq!(is_streaming_put_inner(&parts), None);
    }

    #[test]
    fn streaming_put_unsigned_chunked_excluded() {
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")],
        );
        assert_eq!(is_streaming_put_inner(&parts), None);
    }

    #[test]
    fn streaming_put_bucket_config_excluded() {
        // PUT /<bucket>?versioning is a bucket config op, not PutObject.
        let parts = make_parts(
            "PUT",
            "/mybucket?versioning",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert_eq!(is_streaming_put_inner(&parts), None);
    }

    #[test]
    fn streaming_put_deep_key() {
        let parts = make_parts(
            "PUT",
            "/mybucket/path/to/deep/key.txt",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        let result = is_streaming_put_inner(&parts);
        assert_eq!(
            result,
            Some(("mybucket".to_string(), "path/to/deep/key.txt".to_string()))
        );
    }

    #[test]
    fn streaming_gate_returns_none() {
        // The public is_streaming_put is gated to always return None until Phase 4.
        let parts = make_parts(
            "PUT",
            "/mybucket/mykey",
            &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        );
        assert_eq!(is_streaming_put(&parts), None);
    }
}
