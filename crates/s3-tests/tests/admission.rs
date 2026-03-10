//! Tests for connection and request admission control.
//!
//! These tests start dedicated servers with constrained pool sizes to exercise
//! the overload paths (SlowDown shedding, body read timeout) that are hard to
//! trigger with the default shared test server.

use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Guard that aborts a spawned server task on drop, preventing leaked
/// background servers when multiple tests run in the same file.
struct ServerGuard(tokio::task::JoinHandle<()>);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Start a minimal test server with the given pool size and max connections.
/// Returns the address (host:port), a guard that aborts the server on drop,
/// and the temp dir (must be kept alive for the duration of the test).
async fn start_server(
    pool_size: usize,
    max_connections: u32,
) -> (String, ServerGuard, test_util::TempDir) {
    let std_listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = std_listener.local_addr().unwrap().to_string();
    std_listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();

    let temp_dir = test_util::tempdir();
    let data_path = temp_dir.path().join("data");
    ec::self_test().unwrap();

    let pg_count: u32 = 4;
    let pg_ids: Vec<u32> = (0..pg_count).collect();

    let storage_node =
        Arc::new(storage::SharedStorageNode::open(&data_path, &pg_ids).expect("open storage"));

    let frontends: Vec<server_http::http::HttpFrontend> = (0..pool_size)
        .map(|_| {
            let ec_config = ec::EcConfig::new(4, 2).unwrap();
            let coordinator = server_http::coordinator::Coordinator::new(
                Arc::clone(&storage_node),
                ec_config,
                "us-east-1".to_string(),
            )
            .expect("create coordinator");

            let mut credentials = auth::CredentialStore::new();
            credentials.add(
                s3_tests::server::TEST_ACCESS_KEY.to_string(),
                auth::SecretKey::new(s3_tests::server::TEST_SECRET_KEY.to_string()),
            );

            server_http::http::HttpFrontend {
                coordinator,
                credentials,
            }
        })
        .collect();

    let config = server_http::http::serve::ServeConfig {
        // Use a short request wait timeout so the SlowDown test completes
        // in ~100ms instead of the production default (5s).
        request_wait_timeout: Duration::from_millis(100),
        ..server_http::http::serve::ServeConfig::default()
    };

    let handle = tokio::spawn(server_http::http::serve::serve(
        listener,
        frontends,
        max_connections,
        config,
    ));

    (addr, ServerGuard(handle), temp_dir)
}

/// Read a complete HTTP response from a TCP stream by accumulating reads
/// until we find the end of the body (using Content-Length) or the stream
/// returns 0 bytes. Returns the full response as a string.
async fn read_http_response(stream: &mut TcpStream, timeout: Duration) -> String {
    let mut buf = Vec::with_capacity(8192);
    let mut tmp = [0u8; 4096];
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        match tokio::time::timeout_at(deadline, stream.read(&mut tmp)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => {
                buf.extend_from_slice(&tmp[..n]);

                // Check if we have a complete response: headers + body.
                let text = String::from_utf8_lossy(&buf);
                if let Some(header_end) = text.find("\r\n\r\n") {
                    let headers = &text[..header_end];
                    // Look for Content-Length to know when body is complete.
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let lower = line.to_lowercase();
                            lower
                                .strip_prefix("content-length: ")
                                .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                        })
                        .unwrap_or(0);
                    let body_start = header_end + 4;
                    if buf.len() >= body_start + content_length {
                        break;
                    }
                }
            }
            Ok(Err(_)) => break,
            Err(_) => break, // overall timeout
        }
    }

    String::from_utf8_lossy(&buf).into_owned()
}

/// When the single request slot is occupied by a slow-drip upload, a second
/// request should be shed with 503 SlowDown after REQUEST_WAIT_TIMEOUT (5s).
#[tokio::test]
async fn slow_down_when_request_slot_held_by_slow_body() {
    let (addr, _guard, _dir) = start_server(1, 8).await;

    // Connection 1: send valid HTTP headers with a large Content-Length but
    // never send the body. This holds the request permit while the server
    // waits for body data (up to BODY_IDLE_TIMEOUT per frame).
    let mut slow_conn = TcpStream::connect(&addr).await.unwrap();
    slow_conn
        .write_all(
            b"PUT /test-bucket/slow-key HTTP/1.1\r\n\
              Host: localhost\r\n\
              Content-Length: 999999\r\n\r\n",
        )
        .await
        .unwrap();

    // Give the server time to accept and start processing (acquire permit,
    // begin body read).
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Connection 2: send a complete request. The server's single request
    // permit is held by the slow upload, so this request waits for
    // request_wait_timeout (100ms in test config) then gets 503 SlowDown.
    let mut fast_conn = TcpStream::connect(&addr).await.unwrap();
    fast_conn
        .write_all(
            b"GET / HTTP/1.1\r\n\
              Host: localhost\r\n\r\n",
        )
        .await
        .unwrap();

    let start = Instant::now();

    // Read the complete response.
    let response = read_http_response(&mut fast_conn, Duration::from_secs(5)).await;
    let elapsed = start.elapsed();

    // Verify we got 503 SlowDown
    assert!(
        response.starts_with("HTTP/1.1 503"),
        "expected 503 status, got: {}",
        response.lines().next().unwrap_or("")
    );
    assert!(
        response.contains("SlowDown"),
        "expected SlowDown in response body, got: {}",
        response
    );

    // With request_wait_timeout=100ms, should complete well under 2s.
    assert!(
        elapsed < Duration::from_secs(2),
        "shed too slow: {:?}",
        elapsed
    );
}
