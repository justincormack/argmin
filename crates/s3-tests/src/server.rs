use std::net::TcpListener;
use std::sync::Arc;
use std::thread;

/// Well-known test credentials.
pub const TEST_ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
pub const TEST_SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
pub const TEST_REGION: &str = "us-east-1";

/// A local S3 server running in background threads for integration testing.
///
/// The server binds to a random available port on localhost. Multiple handler
/// threads process requests concurrently. Storage uses a temporary directory
/// that is cleaned up on drop.
pub struct TestServer {
    endpoint: String,
    _temp_dir: tempfile::TempDir,
    server: Arc<tiny_http::Server>,
}

/// Number of handler threads for the test server.
///
/// Each handler thread opens its own SQLite connections (SQLite WAL mode +
/// busy_timeout handles concurrent access).
const NUM_HANDLER_THREADS: usize = 8;

impl TestServer {
    /// Start a new test server on a random port.
    ///
    /// Returns once the server is listening and ready to accept requests.
    pub fn start() -> Self {
        // Bind to port 0 to get a random free port, then pass the listener
        // to tiny_http to avoid a TOCTOU port race.
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind to random port");
        let port = listener.local_addr().unwrap().port();
        let endpoint = format!("http://127.0.0.1:{}", port);

        // Create temp directory for storage
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let data_path = temp_dir.path().join("data");

        // Run EC self-test once before spawning threads.
        ec::self_test().expect("EC self-test");

        // Pre-create the storage node and PG directories on the main thread.
        // Each handler thread will open its own connections (SQLite is not Sync).
        let pg_count: u32 = 4;
        let pg_ids: Vec<u32> = (0..pg_count).collect();
        {
            use storage::LocalStorageNode;
            let _node = LocalStorageNode::open(&data_path, &pg_ids).expect("open storage node");
            // Drop — just created directories. Each thread opens its own.
        }

        let server = Arc::new(
            tiny_http::Server::from_listener(listener, None)
                .expect("start HTTP server from listener"),
        );

        // Spawn multiple handler threads. Each opens its own storage and
        // SQLite connections, avoiding Sync issues with HttpFrontend.
        for _ in 0..NUM_HANDLER_THREADS {
            let server_clone = Arc::clone(&server);
            let data_path = data_path.clone();
            let pg_ids = pg_ids.clone();
            thread::spawn(move || {
                use auth::{CredentialStore, SecretKey};
                use ec::EcConfig;
                use server::coordinator::Coordinator;
                use server::http::HttpFrontend;
                use storage::{LocalStorageNode, SqliteBucketDb};

                let ec_config = EcConfig::new(4, 2).expect("EC config");

                let storage_node =
                    LocalStorageNode::open(&data_path, &pg_ids).expect("open storage node");

                let bucket_db_path = data_path.join("buckets.db");
                let bucket_db = SqliteBucketDb::open(&bucket_db_path).expect("open bucket db");

                let coordinator = Coordinator::new(
                    storage_node,
                    bucket_db,
                    ec_config,
                    pg_count,
                    TEST_REGION.to_string(),
                )
                .expect("create coordinator");

                let mut credentials = CredentialStore::new();
                credentials.add(
                    TEST_ACCESS_KEY.to_string(),
                    SecretKey(TEST_SECRET_KEY.to_string()),
                );

                let frontend = HttpFrontend {
                    coordinator,
                    credentials,
                };

                for request in server_clone.incoming_requests() {
                    frontend.handle_request(request);
                }
            });
        }

        TestServer {
            endpoint,
            _temp_dir: temp_dir,
            server,
        }
    }

    /// The HTTP endpoint URL (e.g. "http://127.0.0.1:12345").
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Unblock causes incoming_requests() to return on all handler threads,
        // terminating them.
        self.server.unblock();
    }
}
