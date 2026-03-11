use std::net::TcpListener;
use std::sync::Arc;

/// Well-known test credentials.
pub const TEST_ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
pub const TEST_SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
pub const TEST_REGION: &str = "us-east-1";

/// Alternate test credentials (non-owner user).
pub const ALT_ACCESS_KEY: &str = "AKIAI44QH8DHBEXAMPLE";
pub const ALT_SECRET_KEY: &str = "je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY";

/// Number of frontend instances in the pool.
///
/// All frontends share one `SharedStorageNode` (PG access serialized by mutex).
/// This controls the parallelism level for request processing.
const POOL_SIZE: usize = 4;

/// A local S3 server running as a tokio task for integration testing.
///
/// The server binds to a random available port on localhost. Storage uses a
/// temporary directory that is cleaned up on drop. The background server task
/// is aborted when the TestServer is dropped.
pub struct TestServer {
    endpoint: String,
    _temp_dir: test_util::TempDir,
    _server_task: tokio::task::JoinHandle<()>,
}

impl TestServer {
    /// Start a new test server on a random port.
    ///
    /// Returns once the server is listening and ready to accept requests.
    /// Must be called from within a tokio runtime.
    pub async fn start() -> Self {
        // Bind to port 0 to get a random free port (sync bind avoids TOCTOU).
        let std_listener = TcpListener::bind("127.0.0.1:0").expect("bind to random port");
        let port = std_listener.local_addr().unwrap().port();
        let endpoint = format!("http://127.0.0.1:{}", port);
        std_listener.set_nonblocking(true).expect("set nonblocking");
        let listener = tokio::net::TcpListener::from_std(std_listener).expect("tokio listener");

        // Create temp directory for storage
        let temp_dir = test_util::tempdir();
        let data_path = temp_dir.path().join("data");

        // Run EC self-test once
        ec::self_test().expect("EC self-test");

        let pg_count: u32 = 4;
        let pg_ids: Vec<u32> = (0..pg_count).collect();

        // Create one shared storage node for all frontends.
        let storage_node = Arc::new(
            storage::SharedStorageNode::open(&data_path, &pg_ids).expect("open storage node"),
        );

        let frontends: Vec<server_http::http::HttpFrontend> = (0..POOL_SIZE)
            .map(|_| {
                let ec_config = ec::EcConfig::new(4, 2).expect("EC config");
                let coordinator = server_core::coordinator::Coordinator::new(
                    Arc::clone(&storage_node),
                    ec_config,
                    TEST_REGION.to_string(),
                )
                .expect("create coordinator");

                let mut credentials = auth::CredentialStore::default();
                credentials.add(
                    TEST_ACCESS_KEY.to_string(),
                    auth::SecretKey::new(TEST_SECRET_KEY.to_string()),
                );
                credentials.add(
                    ALT_ACCESS_KEY.to_string(),
                    auth::SecretKey::new(ALT_SECRET_KEY.to_string()),
                );

                server_http::http::HttpFrontend {
                    coordinator,
                    credentials,
                }
            })
            .collect();

        // Spawn the server as a background task
        let server_task = tokio::spawn(server_http::http::serve::serve(
            listener,
            frontends,
            64,
            server_http::http::serve::ServeConfig::default(),
        ));

        TestServer {
            endpoint,
            _temp_dir: temp_dir,
            _server_task: server_task,
        }
    }

    /// The HTTP endpoint URL (e.g. "http://127.0.0.1:12345").
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self._server_task.abort();
    }
}
