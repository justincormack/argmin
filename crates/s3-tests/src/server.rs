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
    tls_ca_pem: Option<&'static [u8]>,
    storage_cluster: Arc<storage::StorageCluster>,
    control_coordinator: server_core::coordinator::Coordinator,
    _temp_dir: test_util::TempDir,
    _server_task: tokio::task::JoinHandle<()>,
}

pub fn open_test_storage_cluster(data_path: &Path, pg_ids: &[u32]) -> Arc<storage::StorageCluster> {
    let ec_config = ec::EcConfig::default();
    let ec_shape = storage::EcShape {
        k: ec_config.data_shards,
        m: ec_config.parity_shards,
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
                let coordinator =
                    server_core::coordinator::Coordinator::new_with_managed_key_provider_for_storage_cluster(
                        Arc::clone(&storage_cluster),
                        region.to_string(),
                        Some(sse_c_validator),
                        sse_s3_provider,
                    )
                    .expect("create coordinator");

                let mut credentials = auth::CredentialStore::default();
                credentials.add_record(auth::CredentialRecord {
                    access_key_id: TEST_ACCESS_KEY.to_string(),
                    secret_key: auth::SecretKey::new(TEST_SECRET_KEY.to_string()),
                    account: AccountIdentity::new(
                        TEST_ACCOUNT_ID,
                        CanonicalUserId::from_principal(TEST_ACCOUNT_ID),
                        "test-account",
                    ),
                    authorization_profile: auth::AuthorizationProfile::OwnerAccountAdmin,
                    session_token: None,
                    expires_at_epoch_secs: None,
                    enabled: true,
                });
                credentials.add_record(auth::CredentialRecord {
                    access_key_id: TEST_SECOND_ACCESS_KEY.to_string(),
                    secret_key: auth::SecretKey::new(TEST_SECOND_SECRET_KEY.to_string()),
                    account: AccountIdentity::new(
                        format!("arn:aws:iam::{TEST_ACCOUNT_ID}:user/limited"),
                        CanonicalUserId::from_principal(TEST_ACCOUNT_ID),
                        "test-account-limited",
                    ),
                    authorization_profile: auth::AuthorizationProfile::Standard,
                    session_token: None,
                    expires_at_epoch_secs: None,
                    enabled: true,
                });
                credentials.add_record(auth::CredentialRecord {
                    access_key_id: TEST_OWNER_ROOT_ACCESS_KEY.to_string(),
                    secret_key: auth::SecretKey::new(TEST_OWNER_ROOT_SECRET_KEY.to_string()),
                    account: AccountIdentity::new(
                        format!("arn:aws:iam::{TEST_ACCOUNT_ID}:root"),
                        CanonicalUserId::from_principal(TEST_ACCOUNT_ID),
                        "test-account-root",
                    ),
                    authorization_profile: auth::AuthorizationProfile::OwnerAccountAdmin,
                    session_token: None,
                    expires_at_epoch_secs: None,
                    enabled: true,
                });
                credentials.add_record(auth::CredentialRecord {
                    access_key_id: ALT_ACCESS_KEY.to_string(),
                    secret_key: auth::SecretKey::new(ALT_SECRET_KEY.to_string()),
                    account: AccountIdentity::new(
                        ALT_ACCOUNT_ID,
                        CanonicalUserId::from_principal(ALT_ACCOUNT_ID),
                        "alt-account",
                    ),
                    authorization_profile: auth::AuthorizationProfile::OwnerAccountAdmin,
                    session_token: None,
                    expires_at_epoch_secs: None,
                    enabled: true,
                });

                server_http::http::HttpFrontend {
                    coordinator,
                    credentials,
                    host_id: Arc::clone(&host_id),
                }
            })
            .collect();

        // Spawn the server as a background task
        let serve_config = server_http::http::serve::ServeConfig {
            abort_on_500: true,
            ..server_http::http::serve::ServeConfig::default()
        };
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

        TestServer {
            endpoint,
            tls_ca_pem: (transport == TestServerTransport::Https).then_some(TEST_TLS_CA_CERT_PEM),
            storage_cluster,
            control_coordinator,
            _temp_dir: temp_dir,
            _server_task: server_task,
        }
    }

    /// The HTTP endpoint URL (e.g. "http://127.0.0.1:12345").
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn tls_ca_pem(&self) -> Option<&'static [u8]> {
        self.tls_ca_pem
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
        self._server_task.abort();
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
    use super::*;

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
