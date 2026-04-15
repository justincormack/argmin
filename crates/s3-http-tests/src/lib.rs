use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;

use s3_tests::{
    aws_sdk_s3::{
        self,
        types::{BucketLocationConstraint, CreateBucketConfiguration},
        Client,
    },
    build_client_with_ca, build_test_agent, server, TestServer, RT,
};

static BUCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Shared HTTP-only test context for all tests in a binary.
pub static CTX: LazyLock<HttpTestContext> = LazyLock::new(|| RT.block_on(HttpTestContext::setup()));

/// Run an async test body on the shared runtime.
pub fn run<F: std::future::Future>(f: F) -> F::Output {
    let _ = &*CTX;
    RT.block_on(f)
}

/// Test context for validating HTTP-only transport behavior.
pub struct HttpTestContext {
    client: Client,
    endpoint: String,
    access_key: String,
    secret_key: String,
    region: String,
    _server: Option<TestServer>,
}

impl HttpTestContext {
    async fn setup() -> Self {
        if let Some(endpoint) = external_http_endpoint() {
            let access_key = std::env::var("S3_TEST_ACCESS_KEY")
                .expect("S3_TEST_ACCESS_KEY required for external s3-http-tests runs");
            let secret_key = std::env::var("S3_TEST_SECRET_KEY")
                .expect("S3_TEST_SECRET_KEY required for external s3-http-tests runs");
            let region =
                std::env::var("S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".to_string());
            let client = build_client_with_ca(&endpoint, &access_key, &secret_key, &region, None);
            Self {
                client,
                endpoint,
                access_key,
                secret_key,
                region,
                _server: None,
            }
        } else {
            let server = TestServer::start_http().await;
            let endpoint = server.endpoint().to_string();
            let client = build_client_with_ca(
                &endpoint,
                server::TEST_ACCESS_KEY,
                server::TEST_SECRET_KEY,
                server::TEST_REGION,
                None,
            );
            Self {
                client,
                endpoint,
                access_key: server::TEST_ACCESS_KEY.to_string(),
                secret_key: server::TEST_SECRET_KEY.to_string(),
                region: server::TEST_REGION.to_string(),
                _server: Some(server),
            }
        }
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn access_key(&self) -> &str {
        &self.access_key
    }

    pub fn secret_key(&self) -> &str {
        &self.secret_key
    }

    pub fn region(&self) -> &str {
        &self.region
    }
}

pub fn test_agent() -> ureq::Agent {
    build_test_agent(CTX.endpoint(), None, std::time::Duration::from_secs(30))
}

pub async fn create_bucket(
    client: &Client,
    bucket: &str,
) -> Result<(), aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::create_bucket::CreateBucketError>>
{
    let mut request = client.create_bucket().bucket(bucket);
    if CTX.region() != "us-east-1" {
        request = request.create_bucket_configuration(
            CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::from(CTX.region()))
                .build(),
        );
    }
    request.send().await.map(|_| ())
}

pub fn unique_bucket() -> String {
    let n = BUCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    format!("{}-{}-{}-{}", bucket_prefix(), pid, n, timestamp_millis())
}

fn bucket_prefix() -> &'static str {
    static BUCKET_PREFIX: LazyLock<String> = LazyLock::new(|| {
        if uses_external_endpoint() {
            std::env::var("S3_TEST_BUCKET_PREFIX").expect(
                "S3_TEST_BUCKET_PREFIX required for external s3-http-tests runs; use a dedicated prefix such as claude-s3-",
            )
        } else {
            std::env::var("S3_TEST_BUCKET_PREFIX").unwrap_or_else(|_| "test".to_string())
        }
    });

    &BUCKET_PREFIX
}

fn uses_external_endpoint() -> bool {
    std::env::var("S3_TEST_HTTP_ENDPOINT").is_ok() || std::env::var("S3_TEST_ENDPOINT").is_ok()
}

fn external_http_endpoint() -> Option<String> {
    if let Ok(endpoint) = std::env::var("S3_TEST_HTTP_ENDPOINT") {
        assert!(
            endpoint.starts_with("http://"),
            "S3_TEST_HTTP_ENDPOINT must use http:// for s3-http-tests; got {endpoint}"
        );
        return Some(endpoint);
    }

    let endpoint = std::env::var("S3_TEST_ENDPOINT").ok()?;
    if endpoint.starts_with("http://") {
        Some(endpoint)
    } else if endpoint.starts_with("https://") {
        Some(format!(
            "http://{}",
            endpoint.trim_start_matches("https://")
        ))
    } else {
        panic!("S3_TEST_ENDPOINT must start with http:// or https://; got {endpoint}");
    }
}

fn timestamp_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
