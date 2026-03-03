pub mod helpers;
pub mod server;

pub use helpers::{
    assert_s3_err_code, create_objects, create_objects_with_keys, delete_all_and_bucket,
    err_status, unique_bucket,
};
pub use server::TestServer;

use std::sync::LazyLock;

use aws_sdk_s3::Client;

/// Shared tokio runtime for all tests in a binary.
///
/// All tests must use `RT.block_on(async { ... })` rather than `#[tokio::test]`
/// to ensure the AWS SDK client's connection pool stays on a single runtime.
pub static RT: LazyLock<tokio::runtime::Runtime> =
    LazyLock::new(|| tokio::runtime::Runtime::new().expect("create tokio runtime"));

/// Shared test context for all tests in a binary.
///
/// Initialized lazily on first access. The context (and its local server,
/// if any) lives for the entire process.
pub static CTX: LazyLock<TestContext> = LazyLock::new(|| RT.block_on(TestContext::setup()));

/// Run an async test body on the shared runtime.
///
/// Forces `CTX` initialization before entering `block_on` to avoid nested
/// `block_on` calls (CTX's LazyLock init uses RT.block_on internally).
pub fn run<F: std::future::Future>(f: F) -> F::Output {
    // Touch CTX to force initialization outside of block_on
    let _ = &*CTX;
    RT.block_on(f)
}

/// Test context providing an S3 client and (optionally) a local server.
///
/// When `S3_TEST_ENDPOINT` is set, connects to an external S3-compatible
/// endpoint with the provided credentials. Otherwise, starts a local
/// `TestServer` on a random port with well-known test credentials.
pub struct TestContext {
    client: Client,
    alt_client: Client,
    endpoint: String,
    access_key: String,
    secret_key: String,
    region: String,
    _server: Option<TestServer>,
}

impl TestContext {
    /// Set up a test context.
    ///
    /// Reads environment variables to decide whether to start a local
    /// server or connect to an external endpoint:
    ///
    /// - `S3_TEST_ENDPOINT`: external endpoint URL
    /// - `S3_TEST_ACCESS_KEY`: access key (defaults to test key)
    /// - `S3_TEST_SECRET_KEY`: secret key (defaults to test key)
    /// - `S3_TEST_REGION`: region (defaults to "us-east-1")
    pub async fn setup() -> Self {
        let external_endpoint = std::env::var("S3_TEST_ENDPOINT").ok();

        if let Some(endpoint) = external_endpoint {
            // External endpoint mode
            let access_key = std::env::var("S3_TEST_ACCESS_KEY")
                .expect("S3_TEST_ACCESS_KEY required with S3_TEST_ENDPOINT");
            let secret_key = std::env::var("S3_TEST_SECRET_KEY")
                .expect("S3_TEST_SECRET_KEY required with S3_TEST_ENDPOINT");
            let alt_access_key = std::env::var("S3_TEST_ALT_ACCESS_KEY")
                .unwrap_or_else(|_| server::ALT_ACCESS_KEY.to_string());
            let alt_secret_key = std::env::var("S3_TEST_ALT_SECRET_KEY")
                .unwrap_or_else(|_| server::ALT_SECRET_KEY.to_string());
            let region =
                std::env::var("S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".to_string());

            let client = build_client(&endpoint, &access_key, &secret_key, &region).await;
            let alt_client =
                build_client(&endpoint, &alt_access_key, &alt_secret_key, &region).await;
            TestContext {
                client,
                alt_client,
                endpoint,
                access_key,
                secret_key,
                region,
                _server: None,
            }
        } else {
            // Local server mode
            let server = TestServer::start().await;
            let endpoint = server.endpoint().to_string();
            let client = build_client(
                &endpoint,
                server::TEST_ACCESS_KEY,
                server::TEST_SECRET_KEY,
                server::TEST_REGION,
            )
            .await;
            let alt_client = build_client(
                &endpoint,
                server::ALT_ACCESS_KEY,
                server::ALT_SECRET_KEY,
                server::TEST_REGION,
            )
            .await;
            TestContext {
                client,
                alt_client,
                endpoint,
                access_key: server::TEST_ACCESS_KEY.to_string(),
                secret_key: server::TEST_SECRET_KEY.to_string(),
                region: server::TEST_REGION.to_string(),
                _server: Some(server),
            }
        }
    }

    /// The S3 client (bucket owner).
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// An alternate S3 client (different user, not the bucket owner).
    pub fn alt_client(&self) -> &Client {
        &self.alt_client
    }

    /// The HTTP endpoint URL (e.g. "http://127.0.0.1:12345").
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The access key ID.
    pub fn access_key(&self) -> &str {
        &self.access_key
    }

    /// The secret access key.
    pub fn secret_key(&self) -> &str {
        &self.secret_key
    }

    /// The region.
    pub fn region(&self) -> &str {
        &self.region
    }
}

async fn build_client(endpoint: &str, access_key: &str, secret_key: &str, region: &str) -> Client {
    use std::time::Duration;

    let creds = aws_credential_types::Credentials::new(
        access_key, secret_key, None, // session token
        None, // expiry
        "s3-tests",
    );

    let timeout_config = aws_sdk_s3::config::timeout::TimeoutConfig::builder()
        .connect_timeout(Duration::from_secs(5))
        .operation_attempt_timeout(Duration::from_secs(5))
        .build();

    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .credentials_provider(creds)
        .region(aws_config::Region::new(region.to_string()))
        .endpoint_url(endpoint)
        .timeout_config(timeout_config)
        .load()
        .await;

    let s3_config = aws_sdk_s3::config::Builder::from(&config)
        .force_path_style(true)
        .build();

    Client::from_conf(s3_config)
}
