// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

pub mod helpers;
mod hyper_client;
mod post_form;
mod raw_http_client;
pub mod server;
pub mod shape;
#[doc(hidden)]
pub mod uat_pg_backfill_support;

pub use aws_sdk_s3;
pub use helpers::{
    assert_complete_multipart_raw_error, assert_complete_multipart_sdk_error,
    assert_raw_bucket_location, assert_s3_err_code, bucket_location_url, bucket_prefix,
    cleanup_versioned_bucket, content_md5_header, copy_source_with_version,
    create_account_regional_bucket_with_credentials, create_acl_enabled_bucket, create_boe_bucket,
    create_bucket_with_ownership, create_bucket_with_sse_c_enabled, create_objects,
    create_objects_with_keys, create_public_bucket, create_public_write_bucket,
    delete_all_and_bucket, delete_bucket_retrying_operation_aborted,
    delete_object_retrying_operation_aborted,
    delete_objects_retrying_exact_operation_aborted_result,
    delete_objects_retrying_operation_aborted, delete_objects_with_md5,
    disable_bucket_public_access_block, enable_bucket_sse_c, enable_bucket_versioning, err_status,
    eventually_raw_alt_object_status, expected_raw_bucket_location_constraint,
    get_object_body_retrying_operation_aborted, is_retryable_operation_contention,
    is_retryable_operation_contention_response, is_sdk_stream_disconnect,
    is_sdk_stream_disconnect_or_status, object_url, presign_url,
    presign_url_for_service_with_aws_signer_credentials, presign_url_for_service_with_credentials,
    presign_url_with_credentials, presign_url_without_host_signed_header,
    put_bucket_lifecycle_with_md5, put_object_retrying_operation_aborted, raw_alt_credentials,
    raw_alt_object_request, raw_anonymous, raw_anonymous_put, raw_bucket, raw_fetch_url,
    raw_object, raw_object_query, raw_object_with, raw_response_header,
    retrying_exact_operation_aborted_result, retrying_operation_aborted,
    retrying_operation_aborted_result, sdk_checksum_headers,
    send_checked_signed_payload_request_for_service_with_credentials,
    send_checked_signed_request_for_service_with_credentials,
    send_checked_signed_request_to_endpoint_for_service_with_credentials, send_signed_request,
    send_signed_request_allow_response_body_error,
    send_signed_request_for_service_with_credentials,
    send_signed_request_to_endpoint_for_service_with_credentials,
    send_signed_request_with_credentials, send_signed_request_with_unsigned_headers,
    send_signed_request_without_host_signed_header, sign_aws_chunked_request_with_credentials,
    sign_request_headers_for_service_with_checked_credentials,
    sign_request_headers_with_credentials, sse_c_header_values, test_sse_c_key,
    unique_account_regional_bucket, unique_alt_account_regional_bucket, unique_bucket,
    wait_for_versioned_writes_visible, PresignedRequest, RawAltObjectRequest, RawResponse,
    SendRetryingOperationAborted, SignedAwsChunkedRequest, SignedRequestCredentials,
    SignedRequestHeaders, SigningPayload, SigningService,
};
pub use post_form::{
    build_post_object_multipart_body, post_object_raw_to_test_endpoint_with_headers,
    post_object_to_test_endpoint, post_object_to_test_endpoint_with_headers,
    sigv4_post_fields_for_credentials, sigv4_post_fields_for_credentials_at_epoch,
    sigv4_post_fields_for_service_with_credentials, sigv4_post_sse_c_fields_for_credentials,
};
pub use raw_http_client::{
    open_flushed_partial_request, write_partial_request_and_disconnect, Agent,
    FlushedPartialRequest, FlushedResponse, Response,
};
pub use server::TestServer;

use std::sync::LazyLock;
use std::time::{Duration, Instant};

use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
use aws_sdk_s3::Client;
use s3_types::is_legacy_create_bucket_region;
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
/// endpoint with the provided credentials. HTTPS is the recommended mode for
/// full coverage, but HTTP is allowed for partial runs where HTTPS-dependent
/// cases are expected to fail. Otherwise, starts a local `TestServer` on a
/// random port with well-known test credentials.
pub struct TestContext {
    client: Client,
    second_client: Option<Client>,
    owner_root_client: Option<Client>,
    alt_client: Client,
    endpoint: String,
    s3_control_endpoint: String,
    alt_s3_control_endpoint: String,
    access_key: String,
    secret_key: String,
    alt_access_key: String,
    alt_secret_key: String,
    account_id: String,
    alt_account_id: String,
    region: String,
    tls_ca_pem: Option<Vec<u8>>,
    _server: Option<TestServer>,
}

impl TestContext {
    /// Set up a test context.
    ///
    /// Reads environment variables to decide whether to start a local
    /// server or connect to an external endpoint:
    ///
    /// - `S3_TEST_ENDPOINT`: external endpoint URL
    /// - `S3_CONTROL_TEST_ENDPOINT`: external S3 Control endpoint URL
    /// - `S3_CONTROL_ALT_TEST_ENDPOINT`: alternate-account external S3 Control endpoint URL
    /// - `AWS_TEST_ACCESS_KEY`: primary access key
    /// - `AWS_TEST_SECRET_KEY`: primary secret key
    /// - `AWS_TEST_ACCOUNT_ID`: primary AWS account ID
    /// - `AWS_TEST_ALT_ACCESS_KEY`: alternate access key from a different AWS account
    /// - `AWS_TEST_ALT_SECRET_KEY`: alternate secret key from a different AWS account
    /// - `AWS_TEST_ALT_ACCOUNT_ID`: alternate AWS account ID
    /// - `AWS_TEST_SECOND_ACCESS_KEY`: optional same-account constrained access key
    /// - `AWS_TEST_SECOND_SECRET_KEY`: optional same-account constrained secret key
    /// - `AWS_TEST_SECOND_PRINCIPAL`: optional exact principal ARN for non-AWS external endpoints
    /// - `AWS_TEST_OWNER_ROOT_ACCESS_KEY`: optional owner-account root access key
    /// - `AWS_TEST_OWNER_ROOT_SECRET_KEY`: optional owner-account root secret key
    /// - `S3_TEST_TLS_CA_CERT_PATH`: optional PEM CA bundle for local HTTPS endpoints
    /// - `S3_TEST_BUCKET_PREFIX`: required prefix for external test buckets
    /// - `AWS_TEST_REGION`: region (defaults to "us-east-1")
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
            assert!(
                endpoint.starts_with("https://") || endpoint.starts_with("http://"),
                "S3_TEST_ENDPOINT must use http:// or https://; https:// is recommended for full external s3-tests coverage; got {endpoint}"
            );
            let s3_control_endpoint = std::env::var("S3_CONTROL_TEST_ENDPOINT").expect(
                "S3_CONTROL_TEST_ENDPOINT required with S3_TEST_ENDPOINT; set it to the S3 Control endpoint used by this test target",
            );
            assert!(
                s3_control_endpoint.starts_with("https://")
                    || s3_control_endpoint.starts_with("http://"),
                "S3_CONTROL_TEST_ENDPOINT must use http:// or https://; got {s3_control_endpoint}"
            );
            let alt_s3_control_endpoint = std::env::var("S3_CONTROL_ALT_TEST_ENDPOINT").expect(
                "S3_CONTROL_ALT_TEST_ENDPOINT required with S3_TEST_ENDPOINT; set it to the alternate account's S3 Control endpoint",
            );
            assert!(
                alt_s3_control_endpoint.starts_with("https://")
                    || alt_s3_control_endpoint.starts_with("http://"),
                "S3_CONTROL_ALT_TEST_ENDPOINT must use http:// or https://; got {alt_s3_control_endpoint}"
            );
            let access_key = std::env::var("AWS_TEST_ACCESS_KEY")
                .expect("AWS_TEST_ACCESS_KEY required with S3_TEST_ENDPOINT");
            let secret_key = std::env::var("AWS_TEST_SECRET_KEY")
                .expect("AWS_TEST_SECRET_KEY required with S3_TEST_ENDPOINT");
            let account_id = std::env::var("AWS_TEST_ACCOUNT_ID").expect(
                "AWS_TEST_ACCOUNT_ID required with S3_TEST_ENDPOINT; full external s3-tests runs need the primary AWS account ID",
            );
            let alt_access_key = std::env::var("AWS_TEST_ALT_ACCESS_KEY").expect(
                "AWS_TEST_ALT_ACCESS_KEY required with S3_TEST_ENDPOINT; full external s3-tests runs need alternate credentials from a different AWS account",
            );
            let alt_secret_key = std::env::var("AWS_TEST_ALT_SECRET_KEY").expect(
                "AWS_TEST_ALT_SECRET_KEY required with S3_TEST_ENDPOINT; full external s3-tests runs need alternate credentials from a different AWS account",
            );
            let alt_account_id = std::env::var("AWS_TEST_ALT_ACCOUNT_ID").expect(
                "AWS_TEST_ALT_ACCOUNT_ID required with S3_TEST_ENDPOINT; full external s3-tests runs need the alternate AWS account ID",
            );
            let region =
                std::env::var("AWS_TEST_REGION").unwrap_or_else(|_| "us-east-1".to_string());
            let tls_ca_pem = match std::env::var("S3_TEST_TLS_CA_CERT_PATH") {
                Ok(path) => Some(std::fs::read(&path).unwrap_or_else(|err| {
                    panic!("read S3_TEST_TLS_CA_CERT_PATH {path}: {err}");
                })),
                Err(std::env::VarError::NotPresent) => None,
                Err(err) => panic!("read S3_TEST_TLS_CA_CERT_PATH: {err}"),
            };
            let second_client = match (
                std::env::var("AWS_TEST_SECOND_ACCESS_KEY"),
                std::env::var("AWS_TEST_SECOND_SECRET_KEY"),
            ) {
                (Ok(second_access_key), Ok(second_secret_key)) => Some(build_client(
                    &endpoint,
                    &second_access_key,
                    &second_secret_key,
                    &region,
                    tls_ca_pem.as_deref(),
                )),
                (Err(std::env::VarError::NotPresent), Err(std::env::VarError::NotPresent)) => None,
                (Err(std::env::VarError::NotPresent), Ok(_)) => {
                    panic!("AWS_TEST_SECOND_ACCESS_KEY required with AWS_TEST_SECOND_SECRET_KEY");
                }
                (Ok(_), Err(std::env::VarError::NotPresent)) => {
                    panic!("AWS_TEST_SECOND_SECRET_KEY required with AWS_TEST_SECOND_ACCESS_KEY");
                }
                (Err(err), _) | (_, Err(err)) => {
                    panic!("read second same-account AWS test credentials: {err}");
                }
            };
            let owner_root_client = match (
                std::env::var("AWS_TEST_OWNER_ROOT_ACCESS_KEY"),
                std::env::var("AWS_TEST_OWNER_ROOT_SECRET_KEY"),
            ) {
                (Ok(owner_root_access_key), Ok(owner_root_secret_key)) => Some(build_client(
                    &endpoint,
                    &owner_root_access_key,
                    &owner_root_secret_key,
                    &region,
                    tls_ca_pem.as_deref(),
                )),
                (Err(std::env::VarError::NotPresent), Err(std::env::VarError::NotPresent)) => None,
                (Err(std::env::VarError::NotPresent), Ok(_)) => {
                    panic!(
                        "AWS_TEST_OWNER_ROOT_ACCESS_KEY required with AWS_TEST_OWNER_ROOT_SECRET_KEY"
                    );
                }
                (Ok(_), Err(std::env::VarError::NotPresent)) => {
                    panic!(
                        "AWS_TEST_OWNER_ROOT_SECRET_KEY required with AWS_TEST_OWNER_ROOT_ACCESS_KEY"
                    );
                }
                (Err(err), _) | (_, Err(err)) => {
                    panic!("read owner-root AWS test credentials: {err}");
                }
            };
            let _bucket_prefix = std::env::var("S3_TEST_BUCKET_PREFIX").expect(
                "S3_TEST_BUCKET_PREFIX required with S3_TEST_ENDPOINT; use a dedicated prefix such as claude-s3- that matches the test IAM policy",
            );

            let client = build_client(
                &endpoint,
                &access_key,
                &secret_key,
                &region,
                tls_ca_pem.as_deref(),
            );
            let alt_client = build_client(
                &endpoint,
                &alt_access_key,
                &alt_secret_key,
                &region,
                tls_ca_pem.as_deref(),
            );
            assert_distinct_external_s3_owners(
                &client,
                owner_root_client.as_ref(),
                &alt_client,
                &account_id,
                &alt_account_id,
                &region,
            )
            .await;
            TestContext {
                client,
                second_client,
                owner_root_client,
                alt_client,
                endpoint,
                s3_control_endpoint,
                alt_s3_control_endpoint,
                access_key,
                secret_key,
                alt_access_key,
                alt_secret_key,
                account_id,
                alt_account_id,
                region,
                tls_ca_pem,
                _server: None,
            }
        } else {
            // Local server mode
            let server = TestServer::start().await;
            let endpoint = server.endpoint().to_string();
            let s3_control_endpoint = endpoint.clone();
            let alt_s3_control_endpoint = endpoint.clone();
            let client = build_client_with_ca(
                &endpoint,
                server::TEST_ACCESS_KEY,
                server::TEST_SECRET_KEY,
                server::TEST_REGION,
                server.tls_ca_pem(),
            );
            let alt_client = build_client_with_ca(
                &endpoint,
                server::ALT_ACCESS_KEY,
                server::ALT_SECRET_KEY,
                server::TEST_REGION,
                server.tls_ca_pem(),
            );
            let second_client = Some(build_client_with_ca(
                &endpoint,
                server::TEST_SECOND_ACCESS_KEY,
                server::TEST_SECOND_SECRET_KEY,
                server::TEST_REGION,
                server.tls_ca_pem(),
            ));
            let owner_root_client = Some(build_client_with_ca(
                &endpoint,
                server::TEST_OWNER_ROOT_ACCESS_KEY,
                server::TEST_OWNER_ROOT_SECRET_KEY,
                server::TEST_REGION,
                server.tls_ca_pem(),
            ));
            TestContext {
                client,
                second_client,
                owner_root_client,
                alt_client,
                endpoint,
                s3_control_endpoint,
                alt_s3_control_endpoint,
                access_key: server::TEST_ACCESS_KEY.to_string(),
                secret_key: server::TEST_SECRET_KEY.to_string(),
                alt_access_key: server::ALT_ACCESS_KEY.to_string(),
                alt_secret_key: server::ALT_SECRET_KEY.to_string(),
                account_id: server::TEST_ACCOUNT_ID.to_string(),
                alt_account_id: server::ALT_ACCOUNT_ID.to_string(),
                region: server::TEST_REGION.to_string(),
                tls_ca_pem: server.tls_ca_pem().map(<[u8]>::to_vec),
                _server: Some(server),
            }
        }
    }

    /// The S3 client (bucket owner).
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// An optional same-account constrained S3 client.
    pub fn second_client(&self) -> Option<&Client> {
        self.second_client.as_ref()
    }

    /// The same-account constrained S3 client, or panic with a focused setup message.
    pub fn require_second_client(&self) -> &Client {
        self.second_client.as_ref().unwrap_or_else(|| {
            panic!(
                "AWS_TEST_SECOND_ACCESS_KEY/AWS_TEST_SECOND_SECRET_KEY required for constrained same-account AWS tests"
            )
        })
    }

    /// An owner-account root S3 client, when configured for external AWS tests.
    pub fn owner_root_client(&self) -> Option<&Client> {
        self.owner_root_client.as_ref()
    }

    /// The owner-account root S3 client, or panic with a focused setup message.
    pub fn require_owner_root_client(&self) -> &Client {
        self.owner_root_client.as_ref().unwrap_or_else(|| {
            panic!(
                "AWS_TEST_OWNER_ROOT_ACCESS_KEY/AWS_TEST_OWNER_ROOT_SECRET_KEY required for privileged owner-root AWS tests"
            )
        })
    }

    /// An alternate S3 client (different user, not the bucket owner).
    pub fn alt_client(&self) -> &Client {
        &self.alt_client
    }

    /// The HTTP endpoint URL (e.g. "http://127.0.0.1:12345").
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The endpoint URL for S3 Control requests.
    pub fn s3_control_endpoint(&self) -> &str {
        &self.s3_control_endpoint
    }

    /// The endpoint URL for alternate-account S3 Control requests.
    pub fn alt_s3_control_endpoint(&self) -> &str {
        &self.alt_s3_control_endpoint
    }

    /// The local test server CA, if running against the embedded HTTPS server.
    pub fn tls_ca_pem(&self) -> Option<&[u8]> {
        self.tls_ca_pem.as_deref()
    }

    /// The access key ID.
    pub fn access_key(&self) -> &str {
        &self.access_key
    }

    /// The secret access key.
    pub fn secret_key(&self) -> &str {
        &self.secret_key
    }

    /// The alternate access key ID.
    pub fn alt_access_key(&self) -> &str {
        &self.alt_access_key
    }

    /// The alternate secret access key.
    pub fn alt_secret_key(&self) -> &str {
        &self.alt_secret_key
    }

    /// The primary test account ID.
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    /// The alternate test account ID.
    pub fn alt_account_id(&self) -> &str {
        &self.alt_account_id
    }

    /// The region.
    pub fn region(&self) -> &str {
        &self.region
    }
}

fn build_client(
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    tls_ca_pem: Option<&[u8]>,
) -> Client {
    build_client_with_ca(endpoint, access_key, secret_key, region, tls_ca_pem)
}

fn configured_test_timeout() -> std::time::Duration {
    let timeout_secs: u64 = std::env::var("S3_TEST_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    std::time::Duration::from_secs(timeout_secs)
}

pub fn build_client_with_ca(
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    tls_ca_pem: Option<&[u8]>,
) -> Client {
    let creds = aws_credential_types::Credentials::new(
        access_key, secret_key, None, // session token
        None, // expiry
        "s3-tests",
    );

    let timeout = configured_test_timeout();
    let timeout_config = aws_sdk_s3::config::timeout::TimeoutConfig::builder()
        .connect_timeout(timeout)
        .operation_attempt_timeout(timeout)
        .build();

    let http_client = hyper_client::TestHttpClient::new(tls_ca_pem);

    let s3_config = aws_sdk_s3::config::Builder::new()
        .behavior_version_latest()
        .credentials_provider(creds)
        .region(aws_sdk_s3::config::Region::new(region.to_string()))
        .endpoint_url(endpoint)
        .timeout_config(timeout_config)
        .http_client(http_client)
        .force_path_style(true)
        .build();

    Client::from_conf(s3_config)
}

async fn assert_distinct_external_s3_owners(
    client: &Client,
    owner_root_client: Option<&Client>,
    alt_client: &Client,
    account_id: &str,
    alt_account_id: &str,
    region: &str,
) {
    assert_ne!(
        account_id, alt_account_id,
        "AWS_TEST_ALT_ACCESS_KEY/AWS_TEST_ALT_SECRET_KEY must belong to a different AWS account than AWS_TEST_ACCESS_KEY/AWS_TEST_SECRET_KEY"
    );

    let primary_bucket = unique_bucket();
    create_external_setup_probe_bucket(client, &primary_bucket, region, "primary")
        .await
        .expect("create primary probe bucket for external s3-tests setup");

    let alt_bucket = unique_bucket();
    if let Err(err) =
        create_external_setup_probe_bucket(alt_client, &alt_bucket, region, "alternate").await
    {
        delete_external_setup_probe_bucket(client, &primary_bucket, "primary").await;
        panic!("create alternate probe bucket for external s3-tests setup: {err:?}");
    }

    let primary_owner_id = client
        .get_bucket_acl()
        .bucket(&primary_bucket)
        .send()
        .await
        .expect("get primary probe bucket ACL during external s3-tests setup")
        .owner()
        .expect("expected owner in primary probe GetBucketAcl during external s3-tests setup")
        .id()
        .expect("expected owner ID in primary probe GetBucketAcl during external s3-tests setup")
        .to_string();
    if let Some(owner_root_client) = owner_root_client {
        let owner_root_bucket = unique_bucket();
        if let Err(err) = create_external_setup_probe_bucket(
            owner_root_client,
            &owner_root_bucket,
            region,
            "owner-root",
        )
        .await
        {
            delete_external_setup_probe_bucket(client, &primary_bucket, "primary").await;
            delete_external_setup_probe_bucket(alt_client, &alt_bucket, "alternate").await;
            panic!("create owner-root probe bucket for external s3-tests setup: {err:?}");
        }

        let owner_root_id = owner_root_client
            .get_bucket_acl()
            .bucket(&owner_root_bucket)
            .send()
            .await
            .expect("get owner-root probe bucket ACL during external s3-tests setup")
            .owner()
            .expect(
                "expected owner in owner-root probe GetBucketAcl during external s3-tests setup",
            )
            .id()
            .expect(
                "expected owner ID in owner-root probe GetBucketAcl during external s3-tests setup",
            )
            .to_string();

        delete_external_setup_probe_bucket(owner_root_client, &owner_root_bucket, "owner-root")
            .await;

        assert_eq!(
            owner_root_id, primary_owner_id,
            "AWS_TEST_OWNER_ROOT_ACCESS_KEY/AWS_TEST_OWNER_ROOT_SECRET_KEY must resolve to the same S3 canonical owner ID as the primary credentials"
        );
    }
    let alt_owner_id = alt_client
        .get_bucket_acl()
        .bucket(&alt_bucket)
        .send()
        .await
        .expect("get alternate probe bucket ACL during external s3-tests setup")
        .owner()
        .expect("expected owner in alternate probe GetBucketAcl during external s3-tests setup")
        .id()
        .expect("expected owner ID in alternate probe GetBucketAcl during external s3-tests setup")
        .to_string();

    delete_external_setup_probe_bucket(client, &primary_bucket, "primary").await;
    delete_external_setup_probe_bucket(alt_client, &alt_bucket, "alternate").await;

    assert_ne!(
        primary_owner_id, alt_owner_id,
        "AWS_TEST_ALT_ACCESS_KEY/AWS_TEST_ALT_SECRET_KEY resolve to the same S3 canonical owner ID as the primary credentials; use alternate credentials from a different AWS account"
    );
}

async fn create_external_setup_probe_bucket(
    client: &Client,
    bucket: &str,
    region: &str,
    label: &str,
) -> Result<(), aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::create_bucket::CreateBucketError>>
{
    create_bucket_in_region_accepting_verified_lost_success(client, bucket, region, label).await
}

async fn delete_external_setup_probe_bucket(client: &Client, bucket: &str, label: &str) {
    const RETRY_DELAY: Duration = Duration::from_millis(100);
    let deadline = Instant::now() + configured_test_timeout();

    loop {
        match client.delete_bucket().bucket(bucket).send().await {
            Ok(_) => return,
            Err(err) if err.code() == Some("NoSuchBucket") => return,
            Err(err)
                if is_delete_bucket_retryable_backpressure(&err) && Instant::now() < deadline =>
            {
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(err) => {
                panic!("delete {label} probe bucket during external s3-tests setup: {err:?}");
            }
        }
    }
}

pub async fn create_bucket(
    client: &Client,
    bucket: &str,
) -> Result<(), aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::create_bucket::CreateBucketError>>
{
    create_bucket_in_region_accepting_verified_lost_success(client, bucket, CTX.region(), "bucket")
        .await
}

/// Create a bucket, retrying transient same-name reuse conflicts after `DeleteBucket`.
///
/// AWS does not guarantee that deleting a bucket immediately makes the name
/// reusable. Local tests use this helper for delete-then-recreate flows so they
/// model the same bounded retry behavior instead of assuming synchronous
/// namespace release.
pub async fn create_bucket_retrying_reuse(
    client: &Client,
    bucket: &str,
) -> Result<(), aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::create_bucket::CreateBucketError>>
{
    const RETRY_DELAY: Duration = Duration::from_millis(200);
    let deadline = Instant::now() + configured_test_timeout();

    loop {
        match create_bucket_in_region(client, bucket, CTX.region()).await {
            Ok(()) => return Ok(()),
            Err(err) if is_retryable_bucket_reuse_error(&err) && Instant::now() < deadline => {
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(err) => return Err(err),
        }
    }
}

pub fn create_bucket_request(
    client: &Client,
    bucket: &str,
) -> aws_sdk_s3::operation::create_bucket::builders::CreateBucketFluentBuilder {
    create_bucket_request_in_region(client, bucket, CTX.region())
}

async fn create_bucket_in_region(
    client: &Client,
    bucket: &str,
    region: &str,
) -> Result<(), aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::create_bucket::CreateBucketError>>
{
    create_bucket_request_in_region(client, bucket, region)
        .send()
        .await
        .map(|_| ())
}

async fn create_bucket_in_region_accepting_verified_lost_success(
    client: &Client,
    bucket: &str,
    region: &str,
    label: &str,
) -> Result<(), aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::create_bucket::CreateBucketError>>
{
    const RETRY_DELAY: Duration = Duration::from_millis(200);
    let deadline = Instant::now() + configured_test_timeout();

    loop {
        match create_bucket_in_region(client, bucket, region).await {
            Ok(()) => return Ok(()),
            Err(err)
                if is_create_bucket_retryable_contention(&err) && Instant::now() < deadline =>
            {
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(err) if is_create_bucket_lost_success_retry(&err) => {
                match client
                    .head_bucket()
                    .bucket(bucket)
                    .send_retrying_operation_aborted("head bucket after create lost success")
                    .await
                {
                    Ok(_) => return Ok(()),
                    Err(head_err) => {
                        eprintln!(
                            "create {label} returned BucketAlreadyOwnedByYou but HeadBucket failed for {bucket}: {head_err:?}"
                        );
                        return Err(err);
                    }
                }
            }
            Err(err) => return Err(err),
        };
    }
}

fn create_bucket_request_in_region(
    client: &Client,
    bucket: &str,
    region: &str,
) -> aws_sdk_s3::operation::create_bucket::builders::CreateBucketFluentBuilder {
    let mut request = client.create_bucket().bucket(bucket);
    if !is_legacy_create_bucket_region(region) {
        let config = CreateBucketConfiguration::builder()
            .location_constraint(BucketLocationConstraint::from(region))
            .build();
        request = request.create_bucket_configuration(config);
    }
    request
}

fn is_retryable_bucket_reuse_error(
    err: &aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::create_bucket::CreateBucketError>,
) -> bool {
    matches!(
        err.as_service_error().and_then(ProvideErrorMetadata::code),
        Some("BucketAlreadyExists" | "BucketAlreadyOwnedByYou" | "OperationAborted" | "SlowDown")
    )
}

fn is_create_bucket_lost_success_retry(
    err: &aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::create_bucket::CreateBucketError>,
) -> bool {
    err.as_service_error().and_then(ProvideErrorMetadata::code) == Some("BucketAlreadyOwnedByYou")
}

fn is_create_bucket_retryable_contention(
    err: &aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::create_bucket::CreateBucketError>,
) -> bool {
    matches!(
        err.as_service_error().and_then(ProvideErrorMetadata::code),
        Some("OperationAborted" | "SlowDown")
    )
}

fn is_delete_bucket_retryable_backpressure(
    err: &aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::delete_bucket::DeleteBucketError>,
) -> bool {
    matches!(
        err.as_service_error().and_then(ProvideErrorMetadata::code),
        Some("OperationAborted" | "SlowDown" | "BucketNotEmpty")
    )
}

pub fn test_agent() -> Agent {
    test_agent_with_timeout(configured_test_timeout())
}

pub fn test_agent_with_timeout(timeout: std::time::Duration) -> Agent {
    build_test_agent(CTX.endpoint(), CTX.tls_ca_pem(), timeout)
}

pub fn build_test_agent(
    endpoint: &str,
    tls_ca_pem: Option<&[u8]>,
    timeout: std::time::Duration,
) -> Agent {
    Agent::new(endpoint, tls_ca_pem, timeout)
}

/// Build a raw test client using the suite's configured request timeout.
pub fn build_configured_test_agent(endpoint: &str, tls_ca_pem: Option<&[u8]>) -> Agent {
    build_test_agent(endpoint, tls_ca_pem, configured_test_timeout())
}
