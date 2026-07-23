use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use auth::AccountIdentity;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use storage::CanonicalUserId;

/// Well-known test credentials.
pub const TEST_ACCOUNT_ID: &str = "111122223333";
pub const TEST_ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
pub const TEST_SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
pub const TEST_SECOND_ACCESS_KEY: &str = "AKIAISECONDUSEREXAMPLE";
pub const TEST_SECOND_SECRET_KEY: &str = "secondUserSecretKeyExampleDontUse";
pub const TEST_OWNER_ROOT_ACCESS_KEY: &str = "AKIAIROOTOWNEREXAMPLE";
pub const TEST_OWNER_ROOT_SECRET_KEY: &str = "rootOwnerSecretKeyExampleDontUse";
pub const TEST_STS_ACCESS_KEY: &str = "AKIAISTSISSUEREXAMPLE";
pub const TEST_STS_SECRET_KEY: &str = "stsIssuerSecretKeyExampleDontUseForAnything";
pub const TEST_STS_CALLER_ARN: &str = "arn:aws:iam::111122223333:user/argmin-sts-tests/issuer";
pub const TEST_STS_ROLE_NAME: &str = "argmin-sts-test-role";
pub const TEST_STS_ROLE_ARN: &str =
    "arn:aws:iam::111122223333:role/argmin-sts-tests/argmin-sts-test-role";
pub const TEST_REGION: &str = "us-east-1";

/// Alternate test credentials (non-owner user).
pub const ALT_ACCOUNT_ID: &str = "444455556666";
pub const ALT_ACCESS_KEY: &str = "AKIAI44QH8DHBEXAMPLE";
pub const ALT_SECRET_KEY: &str = "je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY";
pub const TEST_SSE_C_VALIDATOR_KEY_B64: &str = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=";
pub const TEST_SSE_S3_WRAPPING_KEY_B64: &str = "YWJjZGVmMDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODk=";
pub const TEST_TLS_CA_CERT_PEM: &[u8] = include_bytes!("../testdata/ca-cert.pem");
pub const TEST_TLS_CERT_PEM: &[u8] = include_bytes!("../testdata/localhost-cert.pem");
const TEST_TLS_KEY_PEM: &[u8] = include_bytes!("../testdata/localhost-key.pem");

/// Number of frontend instances in the pool.
///
/// All frontends share one local storage cluster.
/// This controls the parallelism level for request processing.
const POOL_SIZE: usize = 4;
const TEST_PG_COUNT: u32 = 1;
const TEST_MAX_CONNECTIONS: u32 = 512;
const TEST_MAX_INFLIGHT_REQUESTS: u32 = 32;
const SHARD_SCAVENGER_CLEAN_POLL_INTERVAL: Duration = Duration::from_millis(10);

fn configured_credential(
    access_key_id: &str,
    secret_key: &str,
    account_id: &str,
    principal: impl Into<String>,
    display_name: &str,
    authorization_profile: auth::AuthorizationProfile,
) -> auth::StoredCredential {
    auth::StoredCredential::configured(
        access_key_id.to_string(),
        auth::SecretKey::new(secret_key.to_string()),
        AccountIdentity::new(
            account_id,
            CanonicalUserId::from_principal(account_id),
            display_name,
        ),
        auth::ConfiguredPrincipalIdentity::new(principal),
        authorization_profile,
        None,
        true,
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TestServerTransport {
    Http,
    Https,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct LocalTraceConfig {
    enabled: bool,
    filter: Option<String>,
    file: Option<String>,
    sync_file: bool,
}

struct LocalTraceEnvInputs {
    argmin_trace: Option<std::ffi::OsString>,
    argmin_trace_filter: Option<std::ffi::OsString>,
    argmin_trace_file: Option<std::ffi::OsString>,
    s3_test_trace: Option<std::ffi::OsString>,
    s3_test_trace_filter: Option<std::ffi::OsString>,
    s3_test_trace_file: Option<std::ffi::OsString>,
    s3_test_trace_dir: Option<std::ffi::OsString>,
    argmin_trace_sync: Option<std::ffi::OsString>,
    s3_test_trace_sync: Option<std::ffi::OsString>,
    binary_name: String,
}

/// A local S3 server running as a tokio task for integration testing.
///
/// The server binds to a random available port on localhost. Storage uses a
/// temporary directory that is cleaned up on drop. The background server task
/// is aborted when the TestServer is dropped.
pub struct TestServer {
    endpoint: String,
    sts_endpoint: Option<String>,
    tls_ca_pem: Option<&'static [u8]>,
    storage_cluster: Arc<storage::StorageCluster>,
    control_coordinator: server_core::coordinator::Coordinator,
    _temp_dir: test_util::TempDir,
    _server_tasks: Vec<tokio::task::JoinHandle<()>>,
}

pub fn open_test_storage_cluster(data_path: &Path, pg_ids: &[u32]) -> Arc<storage::StorageCluster> {
    let ec_config = ec::EcConfig::default();
    let ec_shape = storage::EcShape {
        k: ec_config.data_shards(),
        m: ec_config.parity_shards(),
    };
    let node_count = u32::from(ec_shape.k) + u32::from(ec_shape.m);
    let node_ids: Vec<storage::NodeId> = (0..node_count).map(storage::NodeId::new).collect();
    storage::StorageCluster::open_local_nodes(data_path, &node_ids, pg_ids, ec_shape)
        .expect("open local storage cluster")
}

impl TestServer {
    /// Start a new test server on a random port.
    ///
    /// Returns once the server is listening and ready to accept requests.
    /// Must be called from within a tokio runtime.
    pub async fn start() -> Self {
        Self::start_https().await
    }

    pub async fn start_http() -> Self {
        Self::start_http_in_region(TEST_REGION).await
    }

    pub async fn start_https() -> Self {
        Self::start_https_in_region(TEST_REGION).await
    }

    pub async fn start_http_in_region(region: &str) -> Self {
        Self::start_with_transport(TestServerTransport::Http, region).await
    }

    pub async fn start_https_in_region(region: &str) -> Self {
        Self::start_with_transport(TestServerTransport::Https, region).await
    }

    async fn start_with_transport(transport: TestServerTransport, region: &str) -> Self {
        configure_local_tracing();
        let _ = rustls::crypto::ring::default_provider().install_default();

        // Bind to port 0 to get a random free port (sync bind avoids TOCTOU).
        let std_listener = TcpListener::bind("127.0.0.1:0").expect("bind to random port");
        let port = std_listener.local_addr().unwrap().port();
        let endpoint = match transport {
            TestServerTransport::Http => format!("http://127.0.0.1:{port}"),
            TestServerTransport::Https => format!("https://localhost:{port}"),
        };
        std_listener.set_nonblocking(true).expect("set nonblocking");
        let listener = tokio::net::TcpListener::from_std(std_listener).expect("tokio listener");
        let (sts_endpoint, sts_listener) = if transport == TestServerTransport::Https {
            let std_listener = TcpListener::bind("127.0.0.1:0").expect("bind STS to random port");
            let port = std_listener.local_addr().unwrap().port();
            std_listener
                .set_nonblocking(true)
                .expect("set STS listener nonblocking");
            (
                Some(format!("https://localhost:{port}")),
                Some(tokio::net::TcpListener::from_std(std_listener).expect("tokio STS listener")),
            )
        } else {
            (None, None)
        };

        // Create temp directory for storage
        let temp_dir = test_util::tempdir();
        let data_path = temp_dir.path().join("data");

        // Run EC self-test once
        ec::self_test().expect("EC self-test");

        let pg_ids: Vec<u32> = (0..TEST_PG_COUNT).collect();

        // Create one shared local storage cluster for all frontends.
        let storage_cluster = open_test_storage_cluster(&data_path, &pg_ids);
        let control_coordinator = {
            let sse_c_validator = server_core::sse::SseCustomerValidatorConfig::from_base64(
                1,
                TEST_SSE_C_VALIDATOR_KEY_B64,
            )
            .expect("valid test SSE-C validator key");
            let sse_s3_provider = server_core::sse::ManagedWrappingKeyConfig::from_base64(
                1,
                TEST_SSE_S3_WRAPPING_KEY_B64,
            )
            .map(server_core::sse::StaticManagedKeyProvider::single)
            .expect("valid test SSE-S3 wrapping key");
            server_core::coordinator::Coordinator::new_with_managed_key_provider_for_storage_cluster(
                Arc::clone(&storage_cluster),
                region.to_string(),
                Some(sse_c_validator),
                sse_s3_provider,
            )
            .expect("create control coordinator")
        };

        let host_id = Arc::<str>::from(server_http::http::new_host_id());
        let mut credentials = auth::CredentialStore::default();
        credentials
            .add_record(configured_credential(
                TEST_ACCESS_KEY,
                TEST_SECRET_KEY,
                TEST_ACCOUNT_ID,
                TEST_ACCOUNT_ID,
                "test-account",
                auth::AuthorizationProfile::OwnerAccountAdmin,
            ))
            .unwrap();
        credentials
            .add_record(configured_credential(
                TEST_SECOND_ACCESS_KEY,
                TEST_SECOND_SECRET_KEY,
                TEST_ACCOUNT_ID,
                format!("arn:aws:iam::{TEST_ACCOUNT_ID}:user/limited"),
                "test-account-limited",
                auth::AuthorizationProfile::Standard,
            ))
            .unwrap();
        credentials
            .add_record(configured_credential(
                TEST_OWNER_ROOT_ACCESS_KEY,
                TEST_OWNER_ROOT_SECRET_KEY,
                TEST_ACCOUNT_ID,
                format!("arn:aws:iam::{TEST_ACCOUNT_ID}:root"),
                "test-account-root",
                auth::AuthorizationProfile::OwnerAccountAdmin,
            ))
            .unwrap();
        credentials
            .add_record(configured_credential(
                ALT_ACCESS_KEY,
                ALT_SECRET_KEY,
                ALT_ACCOUNT_ID,
                ALT_ACCOUNT_ID,
                "alt-account",
                auth::AuthorizationProfile::OwnerAccountAdmin,
            ))
            .unwrap();
        credentials
            .add_record(configured_credential(
                TEST_STS_ACCESS_KEY,
                TEST_STS_SECRET_KEY,
                TEST_ACCOUNT_ID,
                TEST_STS_CALLER_ARN,
                "test-account",
                auth::AuthorizationProfile::Standard,
            ))
            .unwrap();
        let sts_account = AccountIdentity::new(
            TEST_ACCOUNT_ID,
            CanonicalUserId::from_principal(TEST_ACCOUNT_ID),
            "test-account",
        );
        let sts_role = auth::IamRoleIdentity::new(
            auth::AwsAccountId::new(TEST_ACCOUNT_ID).unwrap(),
            auth::StableRoleId::new("ARGR0123456789ABCDEFGHIJ").unwrap(),
            auth::RoleName::new(TEST_STS_ROLE_NAME).unwrap(),
            auth::IamPath::new("/argmin-sts-tests/").unwrap(),
        );
        let live_sts_role = auth::LiveRoleIdentity::new(sts_account.clone(), sts_role).unwrap();
        let mut roles = auth::RoleIdentityStore::new();
        roles.add(live_sts_role.clone()).unwrap();
        let mut authorization = auth::AuthorizationRecordStore::new();
        authorization
            .add_role(
                auth::RoleAuthorizationRecord::new(
                    Arc::new(live_sts_role),
                    auth::RoleRecordTimestamps::new(1, 1).unwrap(),
                    auth::RoleMaximumSessionDuration::new(3_600).unwrap(),
                    Arc::new(
                        auth::RoleTrustPolicy::new(
                            Some(auth::PolicyVersion::V2012_10_17),
                            vec![auth::RoleTrustPolicyStatement::new(
                                auth::PolicyEffect::Allow,
                                vec![auth::RoleTrustPrincipal::new(TEST_STS_CALLER_ARN).unwrap()],
                            )
                            .unwrap()],
                        )
                        .unwrap(),
                    ),
                    Vec::new(),
                )
                .unwrap(),
            )
            .unwrap();
        let sts_caller_key = auth::ConfiguredPrincipalAuthorizationKey::new(
            auth::AwsAccountId::new(TEST_ACCOUNT_ID).unwrap(),
            auth::ConfiguredPrincipalIdentity::new(TEST_STS_CALLER_ARN),
        );
        authorization
            .add_configured_principal(
                auth::ConfiguredPrincipalAuthorizationRecord::new(
                    sts_caller_key,
                    sts_account,
                    Vec::new(),
                )
                .unwrap(),
            )
            .unwrap();
        let identity_provider =
            auth::IdentityProvider::in_memory_with_authorization(credentials, roles, authorization)
                .expect("initialize session-token key ring");
        let frontend_storage_handle =
            storage::StorageClusterRuntimeMapHandle::new(Arc::clone(&storage_cluster));
        let frontends: Vec<server_http::http::HttpFrontend> = (0..POOL_SIZE)
            .map(|_| {
                let sse_c_validator = server_core::sse::SseCustomerValidatorConfig::from_base64(
                    1,
                    TEST_SSE_C_VALIDATOR_KEY_B64,
                )
                .expect("valid test SSE-C validator key");
                let sse_s3_provider = server_core::sse::ManagedWrappingKeyConfig::from_base64(
                    1,
                    TEST_SSE_S3_WRAPPING_KEY_B64,
                )
                .map(server_core::sse::StaticManagedKeyProvider::single)
                .expect("valid test SSE-S3 wrapping key");
                let coordinator = server_core::coordinator::Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
                        frontend_storage_handle.clone(),
                        region.to_string(),
                        Some(sse_c_validator),
                        sse_s3_provider,
                        server_core::coordinator::BackgroundWorkerMode::all(),
                    )
                    .expect("create coordinator");

                server_http::http::HttpFrontend {
                    coordinator: Arc::new(coordinator),
                    identity_provider: identity_provider.clone(),
                    host_id: Arc::clone(&host_id),
                }
            })
            .collect();

        // Spawn the server as a background task
        let serve_config = server_http::http::serve::ServeConfig {
            abort_on_500: true,
            ..server_http::http::serve::ServeConfig::default()
        };
        let sts_frontends = sts_listener.as_ref().map(|_| frontends.clone());
        let server_task = match transport {
            TestServerTransport::Http => tokio::spawn(server_http::http::serve::serve(
                listener,
                frontends,
                TEST_MAX_CONNECTIONS,
                TEST_MAX_INFLIGHT_REQUESTS,
                serve_config,
            )),
            TestServerTransport::Https => {
                let tls_acceptor = make_test_tls_acceptor();
                tokio::spawn(server_http::http::serve::serve_tls(
                    listener,
                    tls_acceptor,
                    frontends,
                    TEST_MAX_CONNECTIONS,
                    TEST_MAX_INFLIGHT_REQUESTS,
                    serve_config,
                ))
            }
        };
        let mut server_tasks = vec![server_task];
        if let (Some(listener), Some(frontends)) = (sts_listener, sts_frontends) {
            let tls_acceptor = make_test_tls_acceptor();
            server_tasks.push(tokio::spawn(server_http::http::serve::serve_sts_tls(
                listener,
                tls_acceptor,
                frontends,
                TEST_MAX_CONNECTIONS,
                TEST_MAX_INFLIGHT_REQUESTS,
                server_http::http::serve::ServeConfig {
                    abort_on_500: true,
                    ..server_http::http::serve::ServeConfig::default()
                },
            )));
        }

        TestServer {
            endpoint,
            sts_endpoint,
            tls_ca_pem: (transport == TestServerTransport::Https).then_some(TEST_TLS_CA_CERT_PEM),
            storage_cluster,
            control_coordinator,
            _temp_dir: temp_dir,
            _server_tasks: server_tasks,
        }
    }

    /// The HTTP endpoint URL (e.g. "http://127.0.0.1:12345").
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn tls_ca_pem(&self) -> Option<&'static [u8]> {
        self.tls_ca_pem
    }

    /// The dedicated local STS endpoint, available for HTTPS test servers.
    pub fn sts_endpoint(&self) -> Option<&str> {
        self.sts_endpoint.as_deref()
    }

    /// Run one deterministic lifecycle sweep at a caller-provided timestamp.
    pub fn run_lifecycle_sweep_at(
        &self,
        now_millis: u64,
    ) -> Result<(), server_core::error::ServerError> {
        self.control_coordinator
            .run_lifecycle_sweep_for_test(now_millis)
    }

    pub fn assert_shard_scavenger_clean(&self) {
        assert_shard_scavenger_clean(&self.storage_cluster, "local test server")
    }

    pub async fn wait_for_shard_scavenger_clean(&self, timeout: Duration) -> Result<(), String> {
        wait_for_shard_scavenger_clean(&self.storage_cluster, "local test server", timeout).await
    }

    pub async fn assert_shard_scavenger_clean_after_async_cleanup(&self, timeout: Duration) {
        if let Err(message) = self.wait_for_shard_scavenger_clean(timeout).await {
            panic!("{message}");
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        for task in &self._server_tasks {
            task.abort();
        }
    }
}

fn assert_shard_scavenger_clean(storage_cluster: &storage::StorageCluster, context: &str) {
    if let Err(message) = shard_scavenger_clean_check_message(storage_cluster, context) {
        panic!("{message}");
    }
}

async fn wait_for_shard_scavenger_clean(
    storage_cluster: &storage::StorageCluster,
    context: &str,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    storage_cluster.wake_reclaim_workers();
    let mut last_error = match shard_scavenger_clean_check_message(storage_cluster, context) {
        Ok(()) => return Ok(()),
        Err(message) => message,
    };
    loop {
        if Instant::now() >= deadline {
            return Err(last_error);
        }
        tokio::time::sleep(SHARD_SCAVENGER_CLEAN_POLL_INTERVAL).await;
        storage_cluster.wake_reclaim_workers();
        match shard_scavenger_clean_check_message(storage_cluster, context) {
            Ok(()) => return Ok(()),
            Err(message) => last_error = message,
        }
    }
}

fn shard_scavenger_clean_check_message(
    storage_cluster: &storage::StorageCluster,
    context: &str,
) -> Result<(), String> {
    let observations = storage_cluster
        .audit_shard_storage_for_scavenger()
        .map_err(|error| format!("shard scavenger final audit failed for {context}: {error}"))?;
    let unresolved: Vec<_> = observations
        .iter()
        .filter(|observation| observation.resolved_at.is_none())
        .collect();
    if unresolved.is_empty() {
        return Ok(());
    }

    let mut message = format!(
        "shard scavenger final audit found {} unresolved observation(s) for {context}",
        unresolved.len()
    );
    for observation in unresolved.iter().take(16) {
        message.push_str(&format!(
            "\n  node={} data_pg={} shard_index={} shard_key={} reason={:?} file_exists={} shard_row_exists={} count={} last_error={}",
            observation.key.node_id,
            observation.key.data_pg_id,
            observation.key.shard_index.get(),
            observation.key.shard_key.hex(),
            observation.reason,
            observation.file_exists,
            observation.shard_row_exists,
            observation.observation_count,
            observation.last_error.as_deref().unwrap_or("<none>"),
        ));
    }
    if unresolved.len() > 16 {
        message.push_str(&format!(
            "\n  ... {} more unresolved observation(s) omitted",
            unresolved.len() - 16
        ));
    }
    Err(message)
}

fn configure_local_tracing() {
    let config = resolve_local_trace_config(LocalTraceEnvInputs {
        argmin_trace: std::env::var_os("ARGMIN_TRACE"),
        argmin_trace_filter: std::env::var_os("ARGMIN_TRACE_FILTER"),
        argmin_trace_file: std::env::var_os("ARGMIN_TRACE_FILE"),
        s3_test_trace: std::env::var_os("S3_TEST_TRACE"),
        s3_test_trace_filter: std::env::var_os("S3_TEST_TRACE_FILTER"),
        s3_test_trace_file: std::env::var_os("S3_TEST_TRACE_FILE"),
        s3_test_trace_dir: std::env::var_os("S3_TEST_TRACE_DIR"),
        argmin_trace_sync: std::env::var_os("ARGMIN_TRACE_SYNC"),
        s3_test_trace_sync: std::env::var_os("S3_TEST_TRACE_SYNC"),
        binary_name: current_test_binary_name(),
    });
    let _ = observability::configure_with_options(
        config.enabled,
        config.filter.as_deref(),
        config.file.as_deref(),
        config.sync_file,
    );
}

fn resolve_local_trace_config(inputs: LocalTraceEnvInputs) -> LocalTraceConfig {
    let argmin_trace = normalize_env_value(inputs.argmin_trace);
    let s3_test_trace = normalize_env_value(inputs.s3_test_trace);
    let enabled = argmin_trace
        .as_deref()
        .or(s3_test_trace.as_deref())
        .is_some_and(trace_enabled);
    let filter = normalize_env_value(inputs.argmin_trace_filter)
        .or_else(|| normalize_env_value(inputs.s3_test_trace_filter));
    let file = normalize_env_value(inputs.argmin_trace_file)
        .or_else(|| normalize_env_value(inputs.s3_test_trace_file))
        .or_else(|| {
            normalize_env_value(inputs.s3_test_trace_dir).map(|dir| {
                trace_file_in_dir(&dir, &inputs.binary_name)
                    .display()
                    .to_string()
            })
        });
    let sync_file = normalize_env_value(inputs.argmin_trace_sync)
        .or_else(|| normalize_env_value(inputs.s3_test_trace_sync))
        .map(|value| trace_enabled(&value))
        .unwrap_or_else(|| {
            enabled && argmin_trace.is_none() && s3_test_trace.is_some() && file.is_some()
        });

    LocalTraceConfig {
        enabled,
        filter,
        file,
        sync_file,
    }
}

fn normalize_env_value(value: Option<std::ffi::OsString>) -> Option<String> {
    value
        .and_then(|value| value.into_string().ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn trace_enabled(value: &str) -> bool {
    !matches!(value, "0" | "false" | "False" | "FALSE" | "off" | "OFF")
}

fn current_test_binary_name() -> String {
    std::env::current_exe()
        .ok()
        .and_then(|path| {
            path.file_name()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "s3-tests".to_string())
}

fn trace_file_in_dir(dir: &str, binary_name: &str) -> PathBuf {
    Path::new(dir).join(format!("{binary_name}.trace"))
}

fn make_test_tls_acceptor() -> tokio_rustls::TlsAcceptor {
    let certs = load_certs_from_pem(TEST_TLS_CERT_PEM).expect("valid test TLS cert");
    let key = load_private_key_from_pem(TEST_TLS_KEY_PEM).expect("valid test TLS key");
    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("build test TLS config");
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    tokio_rustls::TlsAcceptor::from(Arc::new(config))
}

fn load_certs_from_pem(data: &[u8]) -> Result<Vec<CertificateDer<'static>>, String> {
    CertificateDer::pem_slice_iter(data)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("failed to read test TLS cert: {e}"))
}

fn load_private_key_from_pem(data: &[u8]) -> Result<PrivateKeyDer<'static>, String> {
    PrivateKeyDer::from_pem_slice(data).map_err(|e| match e {
        rustls::pki_types::pem::Error::NoItemsFound => {
            "no private key found in test TLS key PEM".to_string()
        }
        _ => format!("failed to read test TLS key: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use crate::helpers::{
        send_checked_signed_request_for_service_with_credentials,
        send_checked_signed_request_to_endpoint_for_service_with_credentials,
        send_signed_request_to_endpoint_for_service_with_credentials, RawResponse,
        SignedRequestCredentials, SigningService,
    };
    use crate::shape::{assert_shape, shape};
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::client::WebPkiServerVerifier;
    use rustls::pki_types::{ServerName, UnixTime};
    use rustls::{
        CertificateError, ClientConfig, DigitallySignedStruct, Error as TlsError, RootCertStore,
        SignatureScheme,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    const ROUTING_BOUNDARY_TARGET: &str =
        "/v20180820/tags/arn%3Aaws%3As3%3A%3A%3Aauthority-probe?tagKeys=probe";

    #[derive(Debug)]
    struct FixedCertificateNameVerifier {
        inner: Arc<WebPkiServerVerifier>,
        certificate_name: ServerName<'static>,
    }

    impl ServerCertVerifier for FixedCertificateNameVerifier {
        fn verify_server_cert(
            &self,
            end_entity: &CertificateDer<'_>,
            intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            ocsp_response: &[u8],
            now: UnixTime,
        ) -> Result<ServerCertVerified, TlsError> {
            self.inner.verify_server_cert(
                end_entity,
                intermediates,
                &self.certificate_name,
                ocsp_response,
                now,
            )
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, TlsError> {
            self.inner.verify_tls12_signature(message, cert, dss)
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, TlsError> {
            self.inner.verify_tls13_signature(message, cert, dss)
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.inner.supported_verify_schemes()
        }
    }

    fn routing_boundary_request(
        host_headers: &[&str],
        absolute_form: bool,
        amz_date: &str,
    ) -> Vec<u8> {
        routing_boundary_request_for_target(
            ROUTING_BOUNDARY_TARGET,
            host_headers,
            absolute_form,
            amz_date,
        )
    }

    fn routing_boundary_request_for_target(
        request_target: &str,
        host_headers: &[&str],
        absolute_form: bool,
        amz_date: &str,
    ) -> Vec<u8> {
        let target = if absolute_form {
            format!("http://absolute-target.invalid{request_target}")
        } else {
            request_target.to_string()
        };
        let date = &amz_date[..8];
        let mut request = format!(
            "POST {target} HTTP/1.1\r\n\
             Authorization: AWS4-HMAC-SHA256 Credential={TEST_ACCESS_KEY}/{date}/{TEST_REGION}/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={}\r\n\
             x-amz-content-sha256: {}\r\n\
             x-amz-date: {amz_date}\r\n\
             Content-Length: 0\r\n\
             Connection: close\r\n",
            "0".repeat(64),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        );
        for host in host_headers {
            request.push_str("Host: ");
            request.push_str(host);
            request.push_str("\r\n");
        }
        request.push_str("\r\n");
        request.into_bytes()
    }

    fn current_amz_date() -> String {
        let epoch_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time is after the Unix epoch")
            .as_secs();
        crate::helpers::format_amz_date(epoch_secs)
    }

    fn endpoint_port(server: &TestServer) -> u16 {
        url::Url::parse(server.endpoint())
            .expect("valid test endpoint")
            .port()
            .expect("test endpoint has a port")
    }

    async fn read_raw_response<S>(mut stream: S, request: &[u8]) -> Vec<u8>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        stream.write_all(request).await.expect("write raw request");
        let mut response = Vec::new();
        tokio::time::timeout(
            crate::configured_test_timeout(),
            stream.read_to_end(&mut response),
        )
        .await
        .expect("raw response should arrive")
        .expect("read raw response");
        response
    }

    async fn send_raw_http(server: &TestServer, request: &[u8]) -> Vec<u8> {
        let stream = tokio::net::TcpStream::connect(("127.0.0.1", endpoint_port(server)))
            .await
            .expect("connect to HTTP test server");
        read_raw_response(stream, request).await
    }

    fn test_tls_client_config(fixed_certificate_name: bool) -> ClientConfig {
        let mut roots = RootCertStore::empty();
        for cert in load_certs_from_pem(TEST_TLS_CA_CERT_PEM).expect("valid test CA") {
            roots.add(cert).expect("add test CA root");
        }
        let roots = Arc::new(roots);
        let mut config = if fixed_certificate_name {
            let verifier = WebPkiServerVerifier::builder(roots)
                .build()
                .expect("build test TLS verifier");
            let verifier = FixedCertificateNameVerifier {
                inner: verifier,
                certificate_name: ServerName::try_from("localhost")
                    .expect("valid certificate DNS name")
                    .to_owned(),
            };
            ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier))
                .with_no_client_auth()
        } else {
            ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth()
        };
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        config
    }

    async fn connect_raw_tls(
        server: &TestServer,
        server_name: ServerName<'static>,
        fixed_certificate_name: bool,
    ) -> std::io::Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
        let stream = tokio::net::TcpStream::connect(("127.0.0.1", endpoint_port(server)))
            .await
            .expect("connect to HTTPS test server");
        let connector = tokio_rustls::TlsConnector::from(Arc::new(test_tls_client_config(
            fixed_certificate_name,
        )));
        connector.connect(server_name, stream).await
    }

    #[derive(Debug, PartialEq, Eq)]
    struct ClassifierFingerprint<'a> {
        status: &'a str,
        code: Option<&'a str>,
        message: Option<&'a str>,
        s3_error_root: bool,
        s3_control_error_root: bool,
        semantic_body_empty: bool,
        has_s3_request_id_header: bool,
    }

    fn xml_element<'a>(body: &'a str, name: &str) -> Option<&'a str> {
        let start = format!("<{name}>");
        let end = format!("</{name}>");
        let (_, value) = body.split_once(&start)?;
        let (value, _) = value.split_once(&end)?;
        Some(value)
    }

    fn classifier_fingerprint(response: &[u8]) -> ClassifierFingerprint<'_> {
        let response = std::str::from_utf8(response).expect("HTTP response is UTF-8");
        let (head, body) = response
            .split_once("\r\n\r\n")
            .expect("HTTP response has header terminator");
        let status = head.lines().next().expect("HTTP status line");
        let has_s3_request_id_header = head.lines().skip(1).any(|line| {
            line.split_once(':').is_some_and(|(name, _)| {
                name.eq_ignore_ascii_case("x-amz-request-id")
                    || name.eq_ignore_ascii_case("x-amz-id-2")
            })
        });
        ClassifierFingerprint {
            status,
            code: xml_element(body, "Code"),
            message: xml_element(body, "Message"),
            s3_error_root: body.contains("<Error><Code>"),
            s3_control_error_root: body.contains("<ErrorResponse><Error><Code>"),
            semantic_body_empty: body.is_empty() || body == "0\r\n\r\n",
            has_s3_request_id_header,
        }
    }

    fn assert_s3_only_classifier_or_http_rejection(fingerprint: &ClassifierFingerprint<'_>) {
        if fingerprint.status == "HTTP/1.1 400 Bad Request" && fingerprint.code.is_none() {
            assert!(
                fingerprint.semantic_body_empty,
                "HTTP-parser rejection must have an empty semantic body: {fingerprint:?}"
            );
            assert!(
                !fingerprint.has_s3_request_id_header,
                "HTTP-parser rejection must precede S3 request-ID allocation: {fingerprint:?}"
            );
            return;
        }
        assert_eq!(fingerprint.status, "HTTP/1.1 405 Method Not Allowed");
        assert!(fingerprint.s3_error_root);
        assert_eq!(fingerprint.code, Some("MethodNotAllowed"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn s3_only_listener_host_authority_cannot_select_an_endpoint_kind() {
        let server = TestServer::start_http().await;
        let port = endpoint_port(&server);
        let configured = format!("127.0.0.1:{port}");
        let amz_date = current_amz_date();
        let authorities = [
            configured.as_str(),
            "sts.us-east-1.amazonaws.com",
            "111122223333.s3-control.us-east-1.amazonaws.com",
            "unrelated.example",
            "127.0.0.1:1",
        ];

        let baseline = send_raw_http(
            &server,
            &routing_boundary_request(&[&configured], false, &amz_date),
        )
        .await;
        let baseline_fingerprint = classifier_fingerprint(&baseline);
        assert_eq!(
            baseline_fingerprint,
            ClassifierFingerprint {
                status: "HTTP/1.1 405 Method Not Allowed",
                code: Some("MethodNotAllowed"),
                message: Some("method not allowed"),
                s3_error_root: true,
                s3_control_error_root: false,
                semantic_body_empty: false,
                has_s3_request_id_header: true,
            }
        );

        for authority in authorities {
            let response = send_raw_http(
                &server,
                &routing_boundary_request(&[authority], false, &amz_date),
            )
            .await;
            assert_eq!(classifier_fingerprint(&response), baseline_fingerprint);
        }

        for hosts in [
            Vec::<&str>::new(),
            vec![configured.as_str(), configured.as_str()],
            vec![configured.as_str(), "sts.us-east-1.amazonaws.com"],
        ] {
            let response =
                send_raw_http(&server, &routing_boundary_request(&hosts, false, &amz_date)).await;
            let fingerprint = classifier_fingerprint(&response);
            assert_s3_only_classifier_or_http_rejection(&fingerprint);
        }

        let absolute = send_raw_http(
            &server,
            &routing_boundary_request(&[configured.as_str()], true, &amz_date),
        )
        .await;
        assert_eq!(classifier_fingerprint(&absolute), baseline_fingerprint);
    }

    fn local_signed_request_credentials(server: &TestServer) -> SignedRequestCredentials<'_> {
        SignedRequestCredentials {
            access_key: TEST_ACCESS_KEY,
            secret_key: TEST_SECRET_KEY,
            region: TEST_REGION,
            tls_ca_pem: server.tls_ca_pem(),
        }
    }

    fn assert_local_s3_control_error(
        label: &str,
        response: &RawResponse,
        status: u16,
        code: &str,
        message: &str,
        detail: &str,
        allow: Option<&str>,
    ) {
        let mut expected = shape().status(status).headers([
            ("content-type", "application/xml"),
            ("x-amz-id-2", "{host_id}"),
            ("x-amz-request-id", "{request_id}"),
        ]);
        if let Some(allow) = allow {
            expected = expected.header("allow", allow);
        }
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <ErrorResponse><Error><Code>{code}</Code><Message>{message}</Message>{detail}</Error>\
             <RequestId>{{request_id}}</RequestId><HostId>{{host_id}}</HostId></ErrorResponse>"
        );
        assert_shape(label, response, &expected.body(body));
    }

    fn assert_local_s3_control_frontend_bad_request(label: &str, response: &RawResponse) {
        assert_shape(
            label,
            response,
            &shape()
                .status(400)
                .headers([
                    ("content-type", "application/xml"),
                    ("x-amz-id-2", "{host_id}"),
                    ("x-amz-request-id", "{request_id}"),
                ])
                .body(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <Error><Code>BadRequest</Code><Message>An error occurred when parsing the HTTP request.</Message>\
                     <RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>",
                ),
        );
    }

    fn assert_local_s3_control_signature_mismatch(label: &str, response: &RawResponse) {
        assert_shape(
            label,
            response,
            &shape().status(403).headers([
                ("content-type", "application/xml"),
                ("x-amz-id-2", "{host_id}"),
                ("x-amz-request-id", "{request_id}"),
            ]),
        );
        assert_eq!(
            xml_element(&response.body, "Code"),
            Some("SignatureDoesNotMatch"),
            "{label}: wrong error code: {response:?}"
        );
        assert!(
            response
                .body
                .contains("<ErrorResponse><Error><Code>SignatureDoesNotMatch</Code>"),
            "{label}: signature mismatch did not use the nested S3 Control envelope: {response:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn plain_http_listener_does_not_enable_s3_control() {
        let server = TestServer::start_http().await;
        let url = format!(
            "{}/v20180820/tags/arn%3Aaws%3As3%3A%3A%3Aplain-http-probe",
            server.endpoint()
        );
        let response = send_checked_signed_request_for_service_with_credentials(
            "GET",
            &url,
            b"",
            Vec::<(&str, &str)>::new(),
            SigningService::S3Control,
            "s3",
            local_signed_request_credentials(&server),
        );

        assert_eq!(response.status, 404);
        assert!(response.body.contains("<Code>NoSuchBucket</Code>"));
        assert!(response.body.contains("<BucketName>v20180820</BucketName>"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shared_tls_s3_control_method_path_and_auth_precedence_match_aws() {
        let server = TestServer::start_https().await;
        let endpoint = server.endpoint();
        let credentials = local_signed_request_credentials(&server);
        let raw_resource = "arn:aws:s3:::bounded-routing-probe";
        let resource = "arn%3Aaws%3As3%3A%3A%3Abounded-routing-probe";
        let tags_path = format!("/v20180820/tags/{resource}");
        let tag_body = concat!(
            "<TagResourceRequest xmlns=\"http://awss3control.amazonaws.com/doc/2018-08-20/\">",
            "<Tags><Tag><Key>routing</Key><Value>probe</Value></Tag></Tags>",
            "</TagResourceRequest>"
        );

        for method in ["GET", "POST", "DELETE"] {
            let target = if method == "DELETE" {
                format!("{tags_path}?tagKeys=routing")
            } else {
                tags_path.clone()
            };
            let body = if method == "POST" {
                tag_body.as_bytes()
            } else {
                b""
            };
            let headers = if method == "POST" {
                vec![("content-type", "application/xml")]
            } else {
                vec![]
            };
            let response = send_checked_signed_request_for_service_with_credentials(
                method,
                &format!("{endpoint}{target}"),
                body,
                headers,
                SigningService::S3Control,
                "s3",
                credentials,
            );
            assert_local_s3_control_error(
                &format!("S3 Control {method} missing resource"),
                &response,
                404,
                "NoSuchResource",
                "The specified resource doesn't exist.",
                "",
                None,
            );
        }

        let head = send_checked_signed_request_for_service_with_credentials(
            "HEAD",
            &format!("{endpoint}{tags_path}"),
            b"",
            Vec::<(&str, &str)>::new(),
            SigningService::S3Control,
            "s3",
            credentials,
        );
        assert_shape(
            "S3 Control HEAD method rejection",
            &head,
            &shape()
                .status(405)
                .headers([
                    ("allow", "DELETE, POST, GET"),
                    ("x-amz-id-2", "{host_id}"),
                    ("x-amz-request-id", "{request_id}"),
                ])
                .body_empty(),
        );

        for method in ["PUT", "PATCH"] {
            let response = send_checked_signed_request_for_service_with_credentials(
                method,
                &format!("{endpoint}{tags_path}"),
                b"",
                Vec::<(&str, &str)>::new(),
                SigningService::S3Control,
                "s3",
                credentials,
            );
            assert_local_s3_control_error(
                &format!("S3 Control {method} method rejection"),
                &response,
                405,
                "MethodNotAllowed",
                "The specified method is not allowed against this resource.",
                &format!("<Method>{method}</Method><ResourceType>BUCKET_TAGS</ResourceType>"),
                Some("DELETE, POST, GET"),
            );
        }

        let options = send_checked_signed_request_for_service_with_credentials(
            "OPTIONS",
            &format!("{endpoint}{tags_path}"),
            b"",
            Vec::<(&str, &str)>::new(),
            SigningService::S3Control,
            "s3",
            credentials,
        );
        assert_local_s3_control_error(
            "S3 Control OPTIONS missing Origin",
            &options,
            400,
            "BadRequest",
            "Insufficient information. Origin request header needed.",
            "",
            None,
        );

        let options_cors = send_checked_signed_request_for_service_with_credentials(
            "OPTIONS",
            &format!("{endpoint}{tags_path}"),
            b"",
            [
                ("origin", "https://example.com"),
                ("access-control-request-method", "POST"),
            ],
            SigningService::S3Control,
            "s3",
            credentials,
        );
        assert_local_s3_control_error(
            "S3 Control OPTIONS missing bucket CORS",
            &options_cors,
            403,
            "AccessForbidden",
            "CORSResponse: Bucket not found",
            "<Method>POST</Method><ResourceType>BUCKET</ResourceType>",
            None,
        );

        for method in ["PROPFIND", "X-ARGMIN-PROBE"] {
            let response = send_checked_signed_request_for_service_with_credentials(
                method,
                &format!("{endpoint}{tags_path}"),
                b"",
                Vec::<(&str, &str)>::new(),
                SigningService::S3Control,
                "s3",
                credentials,
            );
            assert_local_s3_control_frontend_bad_request(
                &format!("S3 Control {method} frontend rejection"),
                &response,
            );
        }

        enum PathResult {
            NoSuchResource,
            InvalidUri(String),
            EmptyBadRequest,
        }

        let path_cases = vec![
            (
                "tags-no-resource",
                "/v20180820/tags".to_string(),
                None,
                PathResult::InvalidUri("tags".to_string()),
            ),
            (
                "tags-empty-resource",
                "/v20180820/tags/".to_string(),
                None,
                PathResult::InvalidUri("tags/".to_string()),
            ),
            (
                "tags-extra-segment",
                format!("{tags_path}/unexpected"),
                None,
                PathResult::InvalidUri(format!("tags/{raw_resource}/unexpected")),
            ),
            (
                "tag-singular",
                format!("/v20180820/tag/{resource}"),
                None,
                PathResult::InvalidUri(format!("tag/{raw_resource}")),
            ),
            (
                "tags-prefix-suffix",
                format!("/v20180820/tagsx/{resource}"),
                None,
                PathResult::InvalidUri(format!("tagsx/{raw_resource}")),
            ),
            (
                "wrong-version",
                format!("/v20180819/tags/{resource}"),
                None,
                PathResult::InvalidUri(format!("/v20180819/tags/{resource}")),
            ),
            (
                "uppercase-version",
                format!("/V20180820/tags/{resource}"),
                None,
                PathResult::InvalidUri(format!("/V20180820/tags/{resource}")),
            ),
            (
                "double-leading-slash",
                format!("//v20180820/tags/{resource}"),
                None,
                PathResult::InvalidUri(format!("//v20180820/tags/{resource}")),
            ),
            (
                "encoded-path-separator",
                format!("/v20180820/tags%2F{resource}"),
                Some(tags_path.clone()),
                PathResult::NoSuchResource,
            ),
            (
                "unencoded-valid-arn",
                format!("/v20180820/tags/{raw_resource}"),
                Some(tags_path.clone()),
                PathResult::NoSuchResource,
            ),
            (
                "malformed-percent-bare",
                "/v20180820/tags/%".to_string(),
                None,
                PathResult::EmptyBadRequest,
            ),
            (
                "malformed-percent-short",
                "/v20180820/tags/%2".to_string(),
                None,
                PathResult::EmptyBadRequest,
            ),
            (
                "malformed-percent-hex",
                "/v20180820/tags/%GG".to_string(),
                None,
                PathResult::EmptyBadRequest,
            ),
            (
                "invalid-utf8-percent",
                "/v20180820/tags/%FF".to_string(),
                None,
                PathResult::InvalidUri("/v20180820/tags/%FF".to_string()),
            ),
            (
                "malformed-arn",
                "/v20180820/tags/not-an-arn".to_string(),
                None,
                PathResult::InvalidUri("tags/not-an-arn".to_string()),
            ),
            (
                "empty-bucket-arn",
                "/v20180820/tags/arn%3Aaws%3As3%3A%3A%3A".to_string(),
                None,
                PathResult::InvalidUri("tags/arn:aws:s3:::".to_string()),
            ),
            (
                "wrong-service-arn",
                "/v20180820/tags/arn%3Aaws%3Aiam%3A%3A111122223333%3Arole%2Fprobe".to_string(),
                None,
                PathResult::InvalidUri("tags/arn:aws:iam::111122223333:role/probe".to_string()),
            ),
            (
                "object-arn",
                format!("{tags_path}%2Fobject"),
                None,
                PathResult::InvalidUri(format!("tags/{raw_resource}/object")),
            ),
            (
                "double-encoded-arn",
                format!("/v20180820/tags/{}", resource.replace('%', "%25")),
                None,
                PathResult::InvalidUri(format!("tags/{resource}")),
            ),
        ];

        for (label, wire_path, signed_path, expected) in path_cases {
            let signed_path = signed_path.as_deref().unwrap_or(&wire_path);
            let connect_url = format!("{endpoint}{wire_path}");
            let signed_url = format!("{endpoint}{signed_path}");
            let response = if matches!(&expected, PathResult::EmptyBadRequest) {
                send_signed_request_to_endpoint_for_service_with_credentials(
                    "GET",
                    &connect_url,
                    &signed_url,
                    b"",
                    Vec::<(&str, &str)>::new(),
                    SigningService::S3Control,
                    credentials,
                )
            } else {
                send_checked_signed_request_to_endpoint_for_service_with_credentials(
                    "GET",
                    &connect_url,
                    &signed_url,
                    b"",
                    Vec::<(&str, &str)>::new(),
                    SigningService::S3Control,
                    "s3",
                    credentials,
                )
            };
            match expected {
                PathResult::NoSuchResource => assert_local_s3_control_error(
                    label,
                    &response,
                    404,
                    "NoSuchResource",
                    "The specified resource doesn't exist.",
                    "",
                    None,
                ),
                PathResult::InvalidUri(uri) => assert_local_s3_control_error(
                    label,
                    &response,
                    400,
                    "InvalidURI",
                    "Couldn't parse the specified URI.",
                    &format!("<URI>{uri}</URI>"),
                    None,
                ),
                PathResult::EmptyBadRequest => {
                    assert_shape(
                        label,
                        &response,
                        &shape()
                            .status(400)
                            .headers(std::iter::empty::<(&str, &str)>())
                            .body_empty(),
                    );
                }
            }
        }

        let port = endpoint_port(&server);
        let configured = format!("localhost:{port}");
        let malformed_arn_bad_hmac = routing_boundary_request_for_target(
            "/v20180820/tags/not-an-arn",
            &[&configured],
            false,
            &current_amz_date(),
        );
        let stream = connect_raw_tls(
            &server,
            ServerName::try_from("localhost")
                .expect("valid DNS name")
                .to_owned(),
            false,
        )
        .await
        .expect("TLS connection");
        let response = read_raw_response(stream, &malformed_arn_bad_hmac).await;
        let fingerprint = classifier_fingerprint(&response);
        assert_eq!(fingerprint.status, "HTTP/1.1 400 Bad Request");
        assert_eq!(fingerprint.code, Some("InvalidURI"));
        assert!(fingerprint.s3_control_error_root);
        assert_eq!(
            xml_element(std::str::from_utf8(&response).unwrap(), "URI"),
            Some("tags/not-an-arn")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shared_tls_s3_control_body_query_validation_and_auth_precedence_match_aws() {
        let server = TestServer::start_https().await;
        let credentials = local_signed_request_credentials(&server);
        let bucket = "body-query-validation";
        let create_url = format!("{}/{bucket}", server.endpoint());
        let create = send_checked_signed_request_for_service_with_credentials(
            "PUT",
            &create_url,
            b"",
            Vec::<(&str, &str)>::new(),
            SigningService::S3,
            "s3",
            credentials,
        );
        assert_eq!(create.status, 200, "create bucket response: {create:?}");
        let resource_url = format!(
            "{}/v20180820/tags/arn%3Aaws%3As3%3A%3A%3A{bucket}",
            server.endpoint(),
        );
        let wrong_secret = "0".repeat(40);
        let bad_signature_credentials = SignedRequestCredentials {
            secret_key: &wrong_secret,
            ..credentials
        };
        let malformed_xml_message =
            "The XML you provided was not well-formed or did not validate against our published schema";
        let invalid_tag_message = "This request contains a tag key or value that isn't valid. Valid characters include the following: [a-zA-Z+-=._:/]. Tag keys can contain up to 128 characters. Tag values can contain up to 256 characters.";
        let success_headers = [
            ("x-amz-id-2", "{host_id}"),
            ("x-amz-request-id", "{request_id}"),
        ];

        for (label, body, code, message) in [
            (
                "empty body",
                b"".as_slice(),
                "MissingRequestBodyError",
                "Request Body is empty",
            ),
            (
                "truncated XML",
                b"<TagResourceRequest".as_slice(),
                "MalformedXML",
                malformed_xml_message,
            ),
            (
                "wrong root",
                b"<WrongRoot/>".as_slice(),
                "InvalidTag",
                "At least one tag is required.",
            ),
            (
                "missing Tags",
                b"<TagResourceRequest xmlns=\"http://awss3control.amazonaws.com/doc/2018-08-20/\"/>".as_slice(),
                "InvalidTag",
                "At least one tag is required.",
            ),
            (
                "empty Tags",
                b"<TagResourceRequest xmlns=\"http://awss3control.amazonaws.com/doc/2018-08-20/\"><Tags/></TagResourceRequest>".as_slice(),
                "InvalidTag",
                "At least one tag is required.",
            ),
        ] {
            let response = send_checked_signed_request_for_service_with_credentials(
                "POST",
                &resource_url,
                body,
                [("content-type", "application/xml")],
                SigningService::S3Control,
                "s3",
                credentials,
            );
            assert_local_s3_control_error(label, &response, 400, code, message, "", None);

            let bad_signature = send_checked_signed_request_for_service_with_credentials(
                "POST",
                &resource_url,
                body,
                [("content-type", "application/xml")],
                SigningService::S3Control,
                "s3",
                bad_signature_credentials,
            );
            assert_local_s3_control_signature_mismatch(
                &format!("{label} with bad signature"),
                &bad_signature,
            );
        }

        let tag_resource_body = |members: &str| {
            format!(
                "<TagResourceRequest xmlns=\"http://awss3control.amazonaws.com/doc/2018-08-20/\"><Tags>{members}</Tags></TagResourceRequest>"
            )
        };
        let member_cases = [
            (
                "empty tag key",
                tag_resource_body("<Tag><Key/><Value>value</Value></Tag>"),
                invalid_tag_message,
            ),
            (
                "overlong tag key",
                tag_resource_body(&format!(
                    "<Tag><Key>{}</Key><Value>value</Value></Tag>",
                    "x".repeat(129)
                )),
                invalid_tag_message,
            ),
            (
                "overlong tag value",
                tag_resource_body(&format!(
                    "<Tag><Key>key</Key><Value>{}</Value></Tag>",
                    "x".repeat(257)
                )),
                invalid_tag_message,
            ),
            (
                "duplicate tag member key",
                tag_resource_body(
                    "<Tag><Key>duplicate</Key><Value>one</Value></Tag><Tag><Key>duplicate</Key><Value>two</Value></Tag>",
                ),
                "There are duplicate tag keys in your request. Remove the duplicate tag keys and try again.",
            ),
            (
                "invalid tag member character",
                tag_resource_body("<Tag><Key>invalid!</Key><Value>value</Value></Tag>"),
                invalid_tag_message,
            ),
            (
                "invalid tag value character",
                tag_resource_body("<Tag><Key>valid-key</Key><Value>invalid!</Value></Tag>"),
                invalid_tag_message,
            ),
            (
                "alphabetic combining mark in tag key",
                tag_resource_body("<Tag><Key>key-\u{0345}</Key><Value>value</Value></Tag>"),
                invalid_tag_message,
            ),
            (
                "alphabetic combining mark in tag value",
                tag_resource_body("<Tag><Key>valid-key</Key><Value>value-\u{0345}</Value></Tag>"),
                invalid_tag_message,
            ),
        ];
        for (label, body, message) in member_cases {
            let response = send_checked_signed_request_for_service_with_credentials(
                "POST",
                &resource_url,
                body.as_bytes(),
                [("content-type", "application/xml")],
                SigningService::S3Control,
                "s3",
                credentials,
            );
            assert_local_s3_control_error(label, &response, 400, "InvalidTag", message, "", None);

            let bad_signature = send_checked_signed_request_for_service_with_credentials(
                "POST",
                &resource_url,
                body.as_bytes(),
                [("content-type", "application/xml")],
                SigningService::S3Control,
                "s3",
                bad_signature_credentials,
            );
            assert_local_s3_control_signature_mismatch(
                &format!("{label} with bad signature"),
                &bad_signature,
            );
        }

        let wrong_service = send_checked_signed_request_for_service_with_credentials(
            "POST",
            &resource_url,
            b"<TagResourceRequest",
            [("content-type", "application/xml")],
            SigningService::S3Control,
            "sts",
            credentials,
        );
        assert_local_s3_control_error(
            "malformed body with wrong credential service",
            &wrong_service,
            400,
            "AuthorizationHeaderMalformed",
            "The authorization header is malformed; incorrect service \"sts\". This endpoint belongs to \"s3\".",
            "",
            None,
        );

        for (label, credential_service, signing_credentials) in [
            ("valid signature", "s3", credentials),
            ("bad signature", "s3", bad_signature_credentials),
            ("wrong credential service", "sts", credentials),
        ] {
            let response = send_checked_signed_request_for_service_with_credentials(
                "DELETE",
                &resource_url,
                b"",
                Vec::<(&str, &str)>::new(),
                SigningService::S3Control,
                credential_service,
                signing_credentials,
            );
            assert_local_s3_control_error(
                &format!("missing tagKeys with {label}"),
                &response,
                400,
                "InvalidTag",
                "At least one tag is required.",
                "",
                None,
            );
        }

        let empty_key_url = format!("{resource_url}?tagKeys=");
        let empty_key = send_checked_signed_request_for_service_with_credentials(
            "DELETE",
            &empty_key_url,
            b"",
            Vec::<(&str, &str)>::new(),
            SigningService::S3Control,
            "s3",
            credentials,
        );
        assert_local_s3_control_error(
            "empty tag key",
            &empty_key,
            400,
            "InvalidTag",
            invalid_tag_message,
            "",
            None,
        );
        let empty_key_bad_signature = send_checked_signed_request_for_service_with_credentials(
            "DELETE",
            &empty_key_url,
            b"",
            Vec::<(&str, &str)>::new(),
            SigningService::S3Control,
            "s3",
            bad_signature_credentials,
        );
        assert_local_s3_control_signature_mismatch(
            "empty tag key with bad signature",
            &empty_key_bad_signature,
        );

        let overlong_key = "x".repeat(129);
        let state_dependent_queries = [
            ("invalid tag key pattern", "tagKeys=%21".to_string()),
            ("overlong tag key", format!("tagKeys={overlong_key}")),
        ];
        for (label, query) in &state_dependent_queries {
            let url = format!("{resource_url}?{query}");
            let response = send_checked_signed_request_for_service_with_credentials(
                "DELETE",
                &url,
                b"",
                Vec::<(&str, &str)>::new(),
                SigningService::S3Control,
                "s3",
                credentials,
            );
            assert_shape(
                &format!("{label} with empty tag set"),
                &response,
                &shape().status(204).headers(success_headers).body_empty(),
            );

            let bad_signature = send_checked_signed_request_for_service_with_credentials(
                "DELETE",
                &url,
                b"",
                Vec::<(&str, &str)>::new(),
                SigningService::S3Control,
                "s3",
                bad_signature_credentials,
            );
            assert_local_s3_control_signature_mismatch(
                &format!("{label} with empty tag set and bad signature"),
                &bad_signature,
            );
        }

        for (label, key, value) in [
            ("space tag key", "space key", "value"),
            ("at-sign tag key", "at@key", "value"),
            ("Unicode tag key", "環境", "value"),
            ("Unicode digit tag key", "digit-١", "value"),
            ("no-break space tag key", "key\u{00a0}", "value"),
            ("line separator tag key", "key\u{2028}", "value"),
            ("paragraph separator tag key", "key\u{2029}", "value"),
            ("letter number tag key", "key-\u{2167}", "value"),
            ("other number tag key", "key-\u{00b2}", "value"),
            ("punctuation tag key", "punctuation+-=._:/@", "value"),
            ("space tag value", "space-value", "with space"),
            ("at-sign tag value", "at-value", "value@example"),
            ("Unicode tag value", "unicode-value", "本番"),
            ("Unicode digit tag value", "unicode-digit-value", "value-١"),
            (
                "no-break space tag value",
                "no-break-space-value",
                "value\u{00a0}",
            ),
            (
                "line separator tag value",
                "line-separator-value",
                "value\u{2028}",
            ),
            (
                "paragraph separator tag value",
                "paragraph-separator-value",
                "value\u{2029}",
            ),
            (
                "letter number tag value",
                "letter-number-value",
                "value-\u{2167}",
            ),
            (
                "other number tag value",
                "other-number-value",
                "value-\u{00b2}",
            ),
            (
                "punctuation tag value",
                "punctuation-value",
                "value+-=._:/@",
            ),
        ] {
            let body = tag_resource_body(&format!(
                "<Tag><Key>{key}</Key><Value>{value}</Value></Tag>"
            ));
            let response = send_checked_signed_request_for_service_with_credentials(
                "POST",
                &resource_url,
                body.as_bytes(),
                [("content-type", "application/xml")],
                SigningService::S3Control,
                "s3",
                credentials,
            );
            assert_shape(
                label,
                &response,
                &shape().status(204).headers(success_headers).body_empty(),
            );

            let bad_signature = send_checked_signed_request_for_service_with_credentials(
                "POST",
                &resource_url,
                body.as_bytes(),
                [("content-type", "application/xml")],
                SigningService::S3Control,
                "s3",
                bad_signature_credentials,
            );
            assert_local_s3_control_signature_mismatch(
                &format!("{label} with bad signature"),
                &bad_signature,
            );
        }

        let seed_body = b"<TagResourceRequest xmlns=\"http://awss3control.amazonaws.com/doc/2018-08-20/\"><Tags><Tag><Key>existing</Key><Value>value</Value></Tag></Tags></TagResourceRequest>";
        let seed = send_checked_signed_request_for_service_with_credentials(
            "POST",
            &resource_url,
            seed_body,
            [("content-type", "application/xml")],
            SigningService::S3Control,
            "s3",
            credentials,
        );
        assert_shape(
            "seed resource tag",
            &seed,
            &shape().status(204).headers(success_headers).body_empty(),
        );

        for (label, query, message) in [
            (
                "invalid tag key pattern",
                "tagKeys=%21".to_string(),
                invalid_tag_message,
            ),
            (
                "overlong tag key",
                format!("tagKeys={overlong_key}"),
                invalid_tag_message,
            ),
            (
                "identical duplicate tag keys",
                "tagKeys=body-probe&tagKeys=body-probe".to_string(),
                "Duplicate tag keys are not supported.",
            ),
        ] {
            let url = format!("{resource_url}?{query}");
            let response = send_checked_signed_request_for_service_with_credentials(
                "DELETE",
                &url,
                b"",
                Vec::<(&str, &str)>::new(),
                SigningService::S3Control,
                "s3",
                credentials,
            );
            assert_local_s3_control_error(label, &response, 400, "InvalidTag", message, "", None);

            let bad_signature = send_checked_signed_request_for_service_with_credentials(
                "DELETE",
                &url,
                b"",
                Vec::<(&str, &str)>::new(),
                SigningService::S3Control,
                "s3",
                bad_signature_credentials,
            );
            assert_local_s3_control_signature_mismatch(
                &format!("{label} with bad signature"),
                &bad_signature,
            );
        }

        let too_many_query = (0..51)
            .map(|index| format!("tagKeys=key-{index}"))
            .collect::<Vec<_>>()
            .join("&");
        let too_many_url = format!("{resource_url}?{too_many_query}");
        let too_many = send_checked_signed_request_for_service_with_credentials(
            "DELETE",
            &too_many_url,
            b"",
            Vec::<(&str, &str)>::new(),
            SigningService::S3Control,
            "s3",
            credentials,
        );
        assert_local_s3_control_error(
            "51 distinct tag keys",
            &too_many,
            400,
            "InvalidTag",
            invalid_tag_message,
            "",
            None,
        );
        let too_many_bad_signature = send_checked_signed_request_for_service_with_credentials(
            "DELETE",
            &too_many_url,
            b"",
            Vec::<(&str, &str)>::new(),
            SigningService::S3Control,
            "s3",
            bad_signature_credentials,
        );
        assert_local_s3_control_signature_mismatch(
            "51 distinct tag keys with bad signature",
            &too_many_bad_signature,
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shared_tls_s3_control_tag_operations_ignore_all_account_id_header_forms() {
        let server = TestServer::start_https().await;
        let credentials = local_signed_request_credentials(&server);
        let bucket = "shared-regional-tags";
        let create_url = format!("{}/{bucket}", server.endpoint());
        let create = send_checked_signed_request_for_service_with_credentials(
            "PUT",
            &create_url,
            b"",
            Vec::<(&str, &str)>::new(),
            SigningService::S3,
            "s3",
            credentials,
        );
        assert_eq!(create.status, 200, "create bucket response: {create:?}");

        let resource_url = format!(
            "{}/v20180820/tags/arn%3Aaws%3As3%3A%3A%3A{bucket}",
            server.endpoint()
        );
        let delete_url = format!("{resource_url}?tagKeys=team");
        let tag_body = concat!(
            "<TagResourceRequest xmlns=\"http://awss3control.amazonaws.com/doc/2018-08-20/\">",
            "<Tags><Tag><Key>team</Key><Value>storage</Value></Tag></Tags>",
            "</TagResourceRequest>"
        );
        let list_body = concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
            "<ListTagsForResourceResult xmlns=\"http://awss3control.amazonaws.com/doc/2018-08-20/\">",
            "<Tags><Tag><Key>team</Key><Value>storage</Value></Tag></Tags>",
            "</ListTagsForResourceResult>"
        );
        let success_headers = [
            ("x-amz-id-2", "{host_id}"),
            ("x-amz-request-id", "{request_id}"),
        ];
        let account_header_cases = [
            ("missing", vec![]),
            ("empty", vec![("x-amz-account-id", "")]),
            ("correct", vec![("x-amz-account-id", TEST_ACCOUNT_ID)]),
            ("wrong", vec![("x-amz-account-id", "999900001111")]),
            ("malformed-short", vec![("x-amz-account-id", "1")]),
            (
                "malformed-alpha",
                vec![("x-amz-account-id", "not-an-account")],
            ),
            (
                "duplicate-correct",
                vec![
                    ("x-amz-account-id", TEST_ACCOUNT_ID),
                    ("x-amz-account-id", TEST_ACCOUNT_ID),
                ],
            ),
            (
                "duplicate-correct-wrong",
                vec![
                    ("x-amz-account-id", TEST_ACCOUNT_ID),
                    ("x-amz-account-id", "999900001111"),
                ],
            ),
            (
                "duplicate-wrong-correct",
                vec![
                    ("x-amz-account-id", "999900001111"),
                    ("x-amz-account-id", TEST_ACCOUNT_ID),
                ],
            ),
            (
                "duplicate-wrong",
                vec![
                    ("x-amz-account-id", "999900001111"),
                    ("x-amz-account-id", "222233334444"),
                ],
            ),
        ];

        for (case, account_headers) in account_header_cases {
            let tag = send_checked_signed_request_for_service_with_credentials(
                "POST",
                &resource_url,
                tag_body.as_bytes(),
                account_headers.iter().copied(),
                SigningService::S3Control,
                "s3",
                credentials,
            );
            assert_shape(
                &format!("TagResource ({case})"),
                &tag,
                &shape().status(204).headers(success_headers).body_empty(),
            );

            let list = send_checked_signed_request_for_service_with_credentials(
                "GET",
                &resource_url,
                b"",
                account_headers.iter().copied(),
                SigningService::S3Control,
                "s3",
                credentials,
            );
            assert_shape(
                &format!("ListTagsForResource ({case})"),
                &list,
                &shape().status(200).headers(success_headers).body(list_body),
            );

            let untag = send_checked_signed_request_for_service_with_credentials(
                "DELETE",
                &delete_url,
                b"",
                account_headers,
                SigningService::S3Control,
                "s3",
                credentials,
            );
            assert_shape(
                &format!("UntagResource ({case})"),
                &untag,
                &shape().status(204).headers(success_headers).body_empty(),
            );
        }

        let empty = send_checked_signed_request_for_service_with_credentials(
            "GET",
            &resource_url,
            b"",
            Vec::<(&str, &str)>::new(),
            SigningService::S3Control,
            "s3",
            credentials,
        );
        assert_shape(
            "ListTagsForResource empty tag set",
            &empty,
            &shape().status(200).headers(success_headers).body(concat!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
                "<ListTagsForResourceResult xmlns=\"http://awss3control.amazonaws.com/doc/2018-08-20/\">",
                "<Tags/></ListTagsForResourceResult>"
            )),
        );

        let distinct_absent_keys = send_checked_signed_request_for_service_with_credentials(
            "DELETE",
            &format!("{resource_url}?tagKeys=absent-a&tagKeys=absent-b"),
            b"",
            Vec::<(&str, &str)>::new(),
            SigningService::S3Control,
            "s3",
            credentials,
        );
        assert_shape(
            "UntagResource distinct absent tag keys",
            &distinct_absent_keys,
            &shape().status(204).headers(success_headers).body_empty(),
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shared_tls_listener_sni_and_authority_cannot_select_an_endpoint_kind() {
        let server = TestServer::start_https().await;
        let port = endpoint_port(&server);
        let configured = format!("localhost:{port}");
        let amz_date = current_amz_date();
        let request = routing_boundary_request(&[&configured], false, &amz_date);

        let matching = connect_raw_tls(
            &server,
            ServerName::try_from("localhost")
                .expect("valid DNS name")
                .to_owned(),
            false,
        )
        .await
        .expect("matching SNI handshake");
        let baseline = read_raw_response(matching, &request).await;
        let baseline_fingerprint = classifier_fingerprint(&baseline);
        assert_eq!(
            baseline_fingerprint,
            ClassifierFingerprint {
                status: "HTTP/1.1 403 Forbidden",
                code: Some("SignatureDoesNotMatch"),
                message: Some("The request signature we calculated does not match the signature you provided. Check your key and signing method."),
                s3_error_root: true,
                s3_control_error_root: true,
                semantic_body_empty: false,
                has_s3_request_id_header: true,
            }
        );

        let mismatched_sni = connect_raw_tls(
            &server,
            ServerName::try_from("sts.us-east-1.amazonaws.com")
                .expect("valid DNS name")
                .to_owned(),
            true,
        )
        .await
        .expect("server accepts mismatched SNI independently of routing");
        let response = read_raw_response(mismatched_sni, &request).await;
        assert_eq!(classifier_fingerprint(&response), baseline_fingerprint);

        let no_sni = connect_raw_tls(
            &server,
            ServerName::try_from("127.0.0.1")
                .expect("valid IP server name")
                .to_owned(),
            true,
        )
        .await
        .expect("server permits a TLS connection without SNI");
        let response = read_raw_response(no_sni, &request).await;
        assert_eq!(classifier_fingerprint(&response), baseline_fingerprint);

        let mismatched_authority = routing_boundary_request(
            &["111122223333.s3-control.us-east-1.amazonaws.com"],
            false,
            &amz_date,
        );
        let matching_sni = connect_raw_tls(
            &server,
            ServerName::try_from("localhost")
                .expect("valid DNS name")
                .to_owned(),
            false,
        )
        .await
        .expect("matching SNI handshake");
        let response = read_raw_response(matching_sni, &mismatched_authority).await;
        assert_eq!(classifier_fingerprint(&response), baseline_fingerprint);

        let rejected = connect_raw_tls(
            &server,
            ServerName::try_from("sts.us-east-1.amazonaws.com")
                .expect("valid DNS name")
                .to_owned(),
            false,
        )
        .await;
        let error = rejected
            .expect_err("normal certificate verification must reject a mismatched TLS authority");
        let tls_error = error
            .get_ref()
            .and_then(|source| source.downcast_ref::<TlsError>());
        assert!(
            matches!(
                tls_error,
                Some(TlsError::InvalidCertificate(
                    CertificateError::NotValidForName
                        | CertificateError::NotValidForNameContext { .. }
                ))
            ),
            "expected certificate-name rejection, got {error:?}"
        );
    }

    #[test]
    fn resolve_local_trace_config_prefers_explicit_argmin_vars() {
        let config = resolve_local_trace_config(LocalTraceEnvInputs {
            argmin_trace: Some("1".into()),
            argmin_trace_filter: Some("server_core".into()),
            argmin_trace_file: Some("/tmp/already-set.trace".into()),
            s3_test_trace: Some("1".into()),
            s3_test_trace_filter: Some("server_http".into()),
            s3_test_trace_file: Some("/tmp/ignored.trace".into()),
            s3_test_trace_dir: Some("/tmp/ignored-dir".into()),
            argmin_trace_sync: None,
            s3_test_trace_sync: None,
            binary_name: "copy_object-test".to_string(),
        });
        assert_eq!(
            config,
            LocalTraceConfig {
                enabled: true,
                filter: Some("server_core".to_string()),
                file: Some("/tmp/already-set.trace".to_string()),
                sync_file: false,
            }
        );
    }

    #[test]
    fn resolve_local_trace_config_uses_s3_test_file() {
        let config = resolve_local_trace_config(LocalTraceEnvInputs {
            argmin_trace: None,
            argmin_trace_filter: None,
            argmin_trace_file: None,
            s3_test_trace: Some("1".into()),
            s3_test_trace_filter: Some("server_http,server_core".into()),
            s3_test_trace_file: Some("/tmp/test.trace".into()),
            s3_test_trace_dir: Some("/tmp/trace-dir".into()),
            argmin_trace_sync: None,
            s3_test_trace_sync: None,
            binary_name: "copy_object-test".to_string(),
        });
        assert_eq!(
            config,
            LocalTraceConfig {
                enabled: true,
                filter: Some("server_http,server_core".to_string()),
                file: Some("/tmp/test.trace".to_string()),
                sync_file: true,
            }
        );
    }

    #[test]
    fn resolve_local_trace_config_builds_trace_path_from_dir() {
        let config = resolve_local_trace_config(LocalTraceEnvInputs {
            argmin_trace: None,
            argmin_trace_filter: None,
            argmin_trace_file: None,
            s3_test_trace: Some("true".into()),
            s3_test_trace_filter: None,
            s3_test_trace_file: None,
            s3_test_trace_dir: Some("/tmp/trace-dir".into()),
            argmin_trace_sync: None,
            s3_test_trace_sync: None,
            binary_name: "copy_object-test".to_string(),
        });
        assert_eq!(
            config,
            LocalTraceConfig {
                enabled: true,
                filter: None,
                file: Some("/tmp/trace-dir/copy_object-test.trace".to_string()),
                sync_file: true,
            }
        );
    }

    #[test]
    fn resolve_local_trace_config_keeps_tracing_disabled_for_falsey_values() {
        let config = resolve_local_trace_config(LocalTraceEnvInputs {
            argmin_trace: None,
            argmin_trace_filter: None,
            argmin_trace_file: None,
            s3_test_trace: Some("false".into()),
            s3_test_trace_filter: Some("server_core".into()),
            s3_test_trace_file: Some("/tmp/test.trace".into()),
            s3_test_trace_dir: None,
            argmin_trace_sync: None,
            s3_test_trace_sync: None,
            binary_name: "copy_object-test".to_string(),
        });
        assert_eq!(
            config,
            LocalTraceConfig {
                enabled: false,
                filter: Some("server_core".to_string()),
                file: Some("/tmp/test.trace".to_string()),
                sync_file: false,
            }
        );
    }

    #[test]
    fn resolve_local_trace_config_respects_explicit_sync_override() {
        let config = resolve_local_trace_config(LocalTraceEnvInputs {
            argmin_trace: None,
            argmin_trace_filter: None,
            argmin_trace_file: None,
            s3_test_trace: Some("true".into()),
            s3_test_trace_filter: None,
            s3_test_trace_file: Some("/tmp/test.trace".into()),
            s3_test_trace_dir: None,
            argmin_trace_sync: None,
            s3_test_trace_sync: Some("false".into()),
            binary_name: "copy_object-test".to_string(),
        });
        assert_eq!(
            config,
            LocalTraceConfig {
                enabled: true,
                filter: None,
                file: Some("/tmp/test.trace".to_string()),
                sync_file: false,
            }
        );
    }
}
