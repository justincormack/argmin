/// Async hyper HTTP server loop with frontend pool and backpressure.
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
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
use super::s3_response_to_hyper;
use super::HttpFrontend;
use crate::error::ServerError;

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
/// The request semaphore is acquired before body collection so that at most
/// pool_size request bodies are buffered concurrently, bounding memory to
/// `pool_size * MAX_BODY_SIZE`. If the semaphore cannot be acquired within
/// REQUEST_WAIT_TIMEOUT, a 503 SlowDown response is returned.
///
/// Body collection uses a per-frame idle timeout (BODY_IDLE_TIMEOUT) rather
/// than a hard total timeout. The timer resets on every data frame, so
/// legitimate slow-but-steady uploads complete regardless of total transfer
/// time. Only truly stalled connections are killed.
///
/// Tradeoff: slow uploads hold a request permit for the entire body read,
/// which can delay cheap operations (HEAD, small GET) under heavy upload load.
/// A future improvement could split body-memory permits from execution permits
/// to avoid this starvation. For now the idle timeout + 5-second shed timeout
/// limits the blast radius.
///
/// Errors are always converted to S3 XML error responses.
async fn handle(
    state: Arc<ServerState>,
    req: Request<Incoming>,
) -> Result<http::Response<Full<Bytes>>, Infallible> {
    // Acquire request permit before body collection to bound memory.
    // If all workers are busy, shed load with 503 after a brief wait.
    // The permit is held in this async function (not moved into spawn_blocking)
    // and released when the function returns.
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

    // Collect body with size limit and per-frame idle timeout.
    // The idle timeout resets on every data frame, so slow-but-steady uploads
    // complete successfully; only truly stalled connections are killed.
    let body_bytes = match collect_body(body, state.config.body_idle_timeout).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return Ok(s3_response_to_hyper(S3Response::error(&err, "")));
        }
    };

    // Parse into S3Request
    let s3req = match S3Request::from_hyper(&parts, body_bytes) {
        Ok(req) => req,
        Err(err) => {
            return Ok(s3_response_to_hyper(S3Response::error(&err, "")));
        }
    };

    // Dispatch on blocking thread pool with try_lock scheduling.
    // Try each frontend starting from the round-robin position; if all are
    // locked (busy), fall back to blocking on the first choice.
    let state_ref = Arc::clone(&state);
    let resp = tokio::task::spawn_blocking(move || {
        let pool_size = state_ref.pool.len();
        let start = state_ref.counter.fetch_add(1, Ordering::Relaxed) % pool_size;

        for i in 0..pool_size {
            let idx = (start + i) % pool_size;
            if let Ok(frontend) = state_ref.pool[idx].try_lock() {
                return frontend.handle_s3_request(&s3req);
            }
        }

        // All busy — block on the original choice
        let frontend = state_ref.pool[start]
            .lock()
            .expect("frontend mutex poisoned");
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
