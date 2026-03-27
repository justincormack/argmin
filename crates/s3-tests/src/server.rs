use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Well-known test credentials.
pub const TEST_ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
pub const TEST_SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
pub const TEST_REGION: &str = "us-east-1";

/// Alternate test credentials (non-owner user).
pub const ALT_ACCESS_KEY: &str = "AKIAI44QH8DHBEXAMPLE";
pub const ALT_SECRET_KEY: &str = "je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY";
pub const TEST_SSE_C_VALIDATOR_KEY_B64: &str = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=";

/// Number of frontend instances in the pool.
///
/// All frontends share one `SharedStorageNode` (PG access serialized by mutex).
/// This controls the parallelism level for request processing.
const POOL_SIZE: usize = 4;

#[derive(Debug, Default, PartialEq, Eq)]
struct LocalTraceConfig {
    enabled: bool,
    filter: Option<String>,
    file: Option<String>,
}

struct LocalTraceEnvInputs {
    argmin_trace: Option<std::ffi::OsString>,
    argmin_trace_filter: Option<std::ffi::OsString>,
    argmin_trace_file: Option<std::ffi::OsString>,
    s3_test_trace: Option<std::ffi::OsString>,
    s3_test_trace_filter: Option<std::ffi::OsString>,
    s3_test_trace_file: Option<std::ffi::OsString>,
    s3_test_trace_dir: Option<std::ffi::OsString>,
    binary_name: String,
}

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
        configure_local_tracing();

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
                let sse_c_validator = server_core::sse::SseCustomerValidatorConfig::from_base64(
                    1,
                    TEST_SSE_C_VALIDATOR_KEY_B64,
                )
                .expect("valid test SSE-C validator key");
                let coordinator = server_core::coordinator::Coordinator::new_with_sse_c_validator(
                    Arc::clone(&storage_node),
                    ec_config,
                    TEST_REGION.to_string(),
                    Some(sse_c_validator),
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
            32,
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

fn configure_local_tracing() {
    let config = resolve_local_trace_config(LocalTraceEnvInputs {
        argmin_trace: std::env::var_os("ARGMIN_TRACE"),
        argmin_trace_filter: std::env::var_os("ARGMIN_TRACE_FILTER"),
        argmin_trace_file: std::env::var_os("ARGMIN_TRACE_FILE"),
        s3_test_trace: std::env::var_os("S3_TEST_TRACE"),
        s3_test_trace_filter: std::env::var_os("S3_TEST_TRACE_FILTER"),
        s3_test_trace_file: std::env::var_os("S3_TEST_TRACE_FILE"),
        s3_test_trace_dir: std::env::var_os("S3_TEST_TRACE_DIR"),
        binary_name: current_test_binary_name(),
    });
    let _ = observability::configure(
        config.enabled,
        config.filter.as_deref(),
        config.file.as_deref(),
    );
}

fn resolve_local_trace_config(inputs: LocalTraceEnvInputs) -> LocalTraceConfig {
    let enabled = normalize_env_value(inputs.argmin_trace)
        .or_else(|| normalize_env_value(inputs.s3_test_trace))
        .is_some_and(|value| trace_enabled(&value));
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

    LocalTraceConfig {
        enabled,
        filter,
        file,
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
            binary_name: "copy_object-test".to_string(),
        });
        assert_eq!(
            config,
            LocalTraceConfig {
                enabled: true,
                filter: Some("server_core".to_string()),
                file: Some("/tmp/already-set.trace".to_string()),
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
            binary_name: "copy_object-test".to_string(),
        });
        assert_eq!(
            config,
            LocalTraceConfig {
                enabled: true,
                filter: Some("server_http,server_core".to_string()),
                file: Some("/tmp/test.trace".to_string()),
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
            binary_name: "copy_object-test".to_string(),
        });
        assert_eq!(
            config,
            LocalTraceConfig {
                enabled: true,
                filter: None,
                file: Some("/tmp/trace-dir/copy_object-test.trace".to_string()),
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
            binary_name: "copy_object-test".to_string(),
        });
        assert_eq!(
            config,
            LocalTraceConfig {
                enabled: false,
                filter: Some("server_core".to_string()),
                file: Some("/tmp/test.trace".to_string()),
            }
        );
    }
}
