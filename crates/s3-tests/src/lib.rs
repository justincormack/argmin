pub mod helpers;
pub mod server;

pub use helpers::{
    assert_s3_err_code, bucket_prefix, cleanup_versioned_bucket, copy_source_with_version,
    create_objects, create_objects_with_keys, create_public_bucket, create_public_write_bucket,
    delete_all_and_bucket, delete_objects_with_md5, ensure_distinct_s3_owners_or_skip, err_status,
    sse_c_header_values, test_sse_c_key, unique_bucket,
};
pub use server::TestServer;

use std::sync::LazyLock;

use aws_sdk_s3::Client;
use aws_smithy_http_client::tls::{rustls_provider::CryptoMode, Provider, TlsContext, TrustStore};
use ureq::tls::{Certificate, RootCerts, TlsConfig, TlsProvider};

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
    alt_client: Option<Client>,
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
    ///
    /// Local embedded-server tracing helpers:
    ///
    /// - `S3_TEST_TRACE`: enables tracing for the local embedded server
    /// - `S3_TEST_TRACE_FILTER`: comma-separated trace target filter
    /// - `S3_TEST_TRACE_FILE`: trace file path
    /// - `S3_TEST_TRACE_DIR`: trace directory; writes `<binary>.trace`
    pub async fn setup() -> Self {
        let external_endpoint = std::env::var("S3_TEST_ENDPOINT").ok();

        if let Some(endpoint) = external_endpoint {
            // External endpoint mode
            let access_key = std::env::var("S3_TEST_ACCESS_KEY")
                .expect("S3_TEST_ACCESS_KEY required with S3_TEST_ENDPOINT");
            let secret_key = std::env::var("S3_TEST_SECRET_KEY")
                .expect("S3_TEST_SECRET_KEY required with S3_TEST_ENDPOINT");
            let alt_access_key = std::env::var("S3_TEST_ALT_ACCESS_KEY").ok();
            let alt_secret_key = std::env::var("S3_TEST_ALT_SECRET_KEY").ok();
            let region =
                std::env::var("S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".to_string());

            let client = build_client(&endpoint, &access_key, &secret_key, &region).await;
            let alt_client = match (alt_access_key.as_deref(), alt_secret_key.as_deref()) {
                (Some(access_key), Some(secret_key)) => {
                    Some(build_client(&endpoint, access_key, secret_key, &region).await)
                }
                (None, None) => None,
                _ => {
                    panic!(
                        "S3_TEST_ALT_ACCESS_KEY and S3_TEST_ALT_SECRET_KEY must either both be set or both be unset"
                    );
                }
            };
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
            let client = build_client_with_ca(
                &endpoint,
                server::TEST_ACCESS_KEY,
                server::TEST_SECRET_KEY,
                server::TEST_REGION,
                server.tls_ca_pem(),
            )
            .await;
            let alt_client = build_client_with_ca(
                &endpoint,
                server::ALT_ACCESS_KEY,
                server::ALT_SECRET_KEY,
                server::TEST_REGION,
                server.tls_ca_pem(),
            )
            .await;
            TestContext {
                client,
                alt_client: Some(alt_client),
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
        self.alt_client.as_ref().expect(
            "alternate client is unavailable; set S3_TEST_ALT_ACCESS_KEY and \
S3_TEST_ALT_SECRET_KEY when running against an external endpoint",
        )
    }

    /// Whether an alternate authenticated client is configured.
    pub fn has_alt_client(&self) -> bool {
        self.alt_client.is_some()
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
    build_client_with_ca(endpoint, access_key, secret_key, region, None).await
}

pub async fn build_client_with_ca(
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    tls_ca_pem: Option<&[u8]>,
) -> Client {
    use std::time::Duration;

    let creds = aws_credential_types::Credentials::new(
        access_key, secret_key, None, // session token
        None, // expiry
        "s3-tests",
    );

    let timeout_secs: u64 = std::env::var("S3_TEST_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    let timeout_config = aws_sdk_s3::config::timeout::TimeoutConfig::builder()
        .connect_timeout(Duration::from_secs(timeout_secs))
        .operation_attempt_timeout(Duration::from_secs(timeout_secs))
        .build();

    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .credentials_provider(creds)
        .region(aws_config::Region::new(region.to_string()))
        .endpoint_url(endpoint)
        .timeout_config(timeout_config);
    if let Some(tls_ca_pem) = tls_ca_pem.filter(|_| endpoint.starts_with("https://")) {
        let tls_context = TlsContext::builder()
            .with_trust_store(TrustStore::empty().with_pem_certificate(tls_ca_pem))
            .build()
            .expect("valid custom trust store");
        let http_client = aws_smithy_http_client::Builder::new()
            .tls_provider(Provider::Rustls(CryptoMode::Ring))
            .tls_context(tls_context)
            .build_https();
        loader = loader.http_client(http_client);
    }
    let config = loader.load().await;

    let s3_config = aws_sdk_s3::config::Builder::from(&config)
        .force_path_style(true)
        .build();

    Client::from_conf(s3_config)
}

pub fn test_agent() -> ureq::Agent {
    build_test_agent(
        CTX.endpoint(),
        CTX._server.as_ref().and_then(TestServer::tls_ca_pem),
    )
}

pub fn build_test_agent(endpoint: &str, tls_ca_pem: Option<&[u8]>) -> ureq::Agent {
    let mut builder = ureq::config::Config::builder().http_status_as_error(false);
    if let Some(tls_ca_pem) = tls_ca_pem.filter(|_| endpoint.starts_with("https://")) {
        let cert = Certificate::from_pem(tls_ca_pem).expect("valid test TLS PEM");
        builder = builder.tls_config(
            TlsConfig::builder()
                .provider(TlsProvider::Rustls)
                .root_certs(RootCerts::from([cert]))
                .build(),
        );
    }
    builder.build().new_agent()
}
