use ec::EcConfig;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProcessRole {
    LegacyLocal,
    Frontend,
    StorageNode,
    Combined,
}

impl ProcessRole {
    pub(crate) fn has_storage_node(self) -> bool {
        matches!(self, Self::StorageNode | Self::Combined)
    }

    fn has_frontend(self) -> bool {
        matches!(self, Self::LegacyLocal | Self::Frontend | Self::Combined)
    }

    pub(crate) fn requires_remote_frontend_routing(self) -> bool {
        matches!(self, Self::Frontend | Self::Combined)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConfiguredCredentialProfile {
    Standard,
    OwnerAccountAdmin,
}

#[derive(Debug, Clone)]
pub(crate) struct ConfiguredCredential {
    pub(crate) access_key_id: String,
    pub(crate) secret_access_key: String,
    pub(crate) account_id: String,
    pub(crate) principal: String,
    pub(crate) display_name: String,
    pub(crate) authorization_profile: ConfiguredCredentialProfile,
}

/// Server configuration, loaded from environment variables.
/// Configuration for the S3 server.
#[derive(Debug, Clone)]
pub(crate) struct ServerConfig {
    pub(crate) process_role: ProcessRole,
    pub(crate) listen_addr: String,
    pub(crate) tls_cert_path: Option<String>,
    pub(crate) tls_key_path: Option<String>,
    pub(crate) data_dir: String,
    pub(crate) pg_count: u32,
    pub(crate) local_node_count: u32,
    pub(crate) storage_node_id: Option<u32>,
    pub(crate) storage_node_data_dir: Option<String>,
    pub(crate) storage_node_socket_path: Option<String>,
    pub(crate) ec_k: u8,
    pub(crate) ec_m: u8,
    pub(crate) account_id: String,
    pub(crate) access_key_id: String,
    pub(crate) secret_access_key: String,
    pub(crate) uat_credentials: Vec<ConfiguredCredential>,
    pub(crate) host_id: Option<String>,
    pub(crate) sse_c_validator_key_b64: Option<String>,
    pub(crate) sse_s3_wrapping_key_b64: String,
    pub(crate) region: String,
    pub(crate) workers: u32,
    pub(crate) max_connections: u32,
    pub(crate) max_inflight_requests: u32,
    pub(crate) stream_read_chunk_size: usize,
    pub(crate) panic_on_500: bool,
    pub(crate) abort_on_500: bool,
}

impl ServerConfig {
    /// Load configuration from environment variables.
    ///
    /// Required: `ARGMIN_ACCOUNT_ID`, `ARGMIN_ACCESS_KEY_ID`,
    /// `ARGMIN_SECRET_ACCESS_KEY`
    /// Optional (with defaults):
    ///   `ARGMIN_HOST_ID` (random stable-for-process host ID)
    ///   `ARGMIN_LISTEN_ADDR` (127.0.0.1:9000)
    ///   `ARGMIN_TLS_CERT_PATH` / `ARGMIN_TLS_KEY_PATH` (unset)
    ///   `ARGMIN_DATA_DIR` (./data)
    ///   `ARGMIN_PG_COUNT` (16)
    ///   `ARGMIN_EC_K` (4)
    ///   `ARGMIN_EC_M` (2)
    ///   `ARGMIN_LOCAL_NODE_COUNT` (`ARGMIN_EC_K + ARGMIN_EC_M`)
    ///   `ARGMIN_REGION` (us-east-1)
    ///   `ARGMIN_WORKERS` (4)
    ///   `ARGMIN_MAX_CONNECTIONS` (512)
    ///   `ARGMIN_MAX_INFLIGHT_REQUESTS` (32)
    ///   `ARGMIN_STREAM_READ_CHUNK_SIZE` (8388608)
    ///   `ARGMIN_PANIC_ON_500` (false)
    ///   `ARGMIN_ABORT_ON_500` (false)
    ///
    /// UAT-only optional credentials for running `s3-tests` against the
    /// standalone binary:
    ///   `ARGMIN_UAT_ALT_ACCOUNT_ID`
    ///   `ARGMIN_UAT_ALT_ACCESS_KEY_ID`
    ///   `ARGMIN_UAT_ALT_SECRET_ACCESS_KEY`
    ///   `ARGMIN_UAT_SECOND_ACCESS_KEY_ID`
    ///   `ARGMIN_UAT_SECOND_SECRET_ACCESS_KEY`
    ///   `ARGMIN_UAT_OWNER_ROOT_ACCESS_KEY_ID`
    ///   `ARGMIN_UAT_OWNER_ROOT_SECRET_ACCESS_KEY`
    pub(crate) fn from_env() -> Result<Self, String> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    /// Build configuration from an arbitrary key-lookup function.
    /// Used by `from_env` (with `std::env::var`) and directly by tests.
    fn from_lookup<F: Fn(&str) -> Option<String>>(get: F) -> Result<Self, String> {
        let process_role = match get("ARGMIN_PROCESS_ROLE") {
            Some(value) => parse_process_role(&value)?,
            None => ProcessRole::LegacyLocal,
        };
        if process_role.requires_remote_frontend_routing() {
            return Err(unsupported_remote_frontend_role_message(process_role));
        }
        let (
            account_id,
            access_key_id,
            secret_access_key,
            uat_credentials,
            sse_s3_wrapping_key_b64,
        ) = if process_role.has_frontend() {
            let account_id = get("ARGMIN_ACCOUNT_ID")
                .ok_or_else(|| "ARGMIN_ACCOUNT_ID is required".to_string())?;
            if account_id.len() != 12 || !account_id.bytes().all(|b| b.is_ascii_digit()) {
                return Err("ARGMIN_ACCOUNT_ID must be a 12-digit AWS account ID".to_string());
            }
            let access_key_id = get("ARGMIN_ACCESS_KEY_ID")
                .ok_or_else(|| "ARGMIN_ACCESS_KEY_ID is required".to_string())?;
            let secret_access_key = get("ARGMIN_SECRET_ACCESS_KEY")
                .ok_or_else(|| "ARGMIN_SECRET_ACCESS_KEY is required".to_string())?;
            let uat_credentials = read_uat_credentials(&get, &account_id)?;
            reject_duplicate_access_keys(&access_key_id, &uat_credentials)?;
            let sse_s3_wrapping_key_b64 = get("ARGMIN_SSE_S3_WRAPPING_KEY")
                .ok_or_else(|| "ARGMIN_SSE_S3_WRAPPING_KEY is required".to_string())?;
            (
                account_id,
                access_key_id,
                secret_access_key,
                uat_credentials,
                sse_s3_wrapping_key_b64,
            )
        } else {
            (
                String::new(),
                String::new(),
                String::new(),
                Vec::new(),
                String::new(),
            )
        };
        let host_id = get("ARGMIN_HOST_ID");
        let sse_c_validator_key_b64 = get("ARGMIN_SSE_C_VALIDATOR_KEY");
        let listen_addr = get("ARGMIN_LISTEN_ADDR").unwrap_or_else(|| "127.0.0.1:9000".to_string());
        let tls_cert_path = get("ARGMIN_TLS_CERT_PATH");
        let tls_key_path = get("ARGMIN_TLS_KEY_PATH");
        let data_dir = get("ARGMIN_DATA_DIR").unwrap_or_else(|| "./data".to_string());
        let storage_node_data_dir = get("ARGMIN_STORAGE_NODE_DATA_DIR");
        let storage_node_socket_path = get("ARGMIN_STORAGE_NODE_SOCKET_PATH");
        let storage_node_id = get("ARGMIN_STORAGE_NODE_ID")
            .map(|value| {
                value
                    .parse()
                    .map_err(|e| format!("invalid ARGMIN_STORAGE_NODE_ID: {e}"))
            })
            .transpose()?;
        let pg_count: u32 = get("ARGMIN_PG_COUNT")
            .unwrap_or_else(|| "16".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_PG_COUNT: {e}"))?;
        let ec_k: u8 = get("ARGMIN_EC_K")
            .unwrap_or_else(|| "4".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_EC_K: {e}"))?;
        let ec_m: u8 = get("ARGMIN_EC_M")
            .unwrap_or_else(|| "2".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_EC_M: {e}"))?;
        let ec_config = EcConfig::new(ec_k, ec_m).map_err(|e| format!("invalid EC config: {e}"))?;
        let local_node_count: u32 = match get("ARGMIN_LOCAL_NODE_COUNT") {
            Some(value) => value
                .parse()
                .map_err(|e| format!("invalid ARGMIN_LOCAL_NODE_COUNT: {e}"))?,
            None => u32::try_from(ec_config.total_shards())
                .map_err(|_| "ARGMIN_LOCAL_NODE_COUNT default is too large".to_string())?,
        };
        let region = get("ARGMIN_REGION").unwrap_or_else(|| "us-east-1".to_string());
        let workers: u32 = get("ARGMIN_WORKERS")
            .unwrap_or_else(|| "4".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_WORKERS: {e}"))?;
        let max_connections: u32 = get("ARGMIN_MAX_CONNECTIONS")
            .unwrap_or_else(|| "512".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_MAX_CONNECTIONS: {e}"))?;
        let max_inflight_requests: u32 = get("ARGMIN_MAX_INFLIGHT_REQUESTS")
            .unwrap_or_else(|| "32".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_MAX_INFLIGHT_REQUESTS: {e}"))?;
        let stream_read_chunk_size: usize = get("ARGMIN_STREAM_READ_CHUNK_SIZE")
            .unwrap_or_else(|| server_core::coordinator::INTERNAL_SEGMENT_SIZE.to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_STREAM_READ_CHUNK_SIZE: {e}"))?;
        let panic_on_500 = match get("ARGMIN_PANIC_ON_500") {
            Some(value) => parse_bool_env("ARGMIN_PANIC_ON_500", &value)?,
            None => false,
        };
        let abort_on_500 = match get("ARGMIN_ABORT_ON_500") {
            Some(value) => parse_bool_env("ARGMIN_ABORT_ON_500", &value)?,
            None => false,
        };

        if pg_count == 0 {
            return Err("ARGMIN_PG_COUNT must be > 0".to_string());
        }
        if local_node_count == 0 {
            return Err("ARGMIN_LOCAL_NODE_COUNT must be > 0".to_string());
        }
        if process_role.has_storage_node() {
            let storage_node_id = storage_node_id.ok_or_else(|| {
                "ARGMIN_STORAGE_NODE_ID is required for storage roles".to_string()
            })?;
            if storage_node_id >= local_node_count {
                return Err(
                    "ARGMIN_STORAGE_NODE_ID must be less than ARGMIN_LOCAL_NODE_COUNT".to_string(),
                );
            }
            if storage_node_socket_path.is_none() {
                return Err(
                    "ARGMIN_STORAGE_NODE_SOCKET_PATH is required for storage roles".to_string(),
                );
            }
        }
        let local_node_count_usize = usize::try_from(local_node_count)
            .map_err(|_| "ARGMIN_LOCAL_NODE_COUNT is too large for this platform".to_string())?;
        if local_node_count_usize < ec_config.total_shards() {
            return Err(format!(
                "ARGMIN_LOCAL_NODE_COUNT must be at least ARGMIN_EC_K + ARGMIN_EC_M ({}) for the configured EC shape",
                ec_config.total_shards()
            ));
        }
        if workers == 0 {
            return Err("ARGMIN_WORKERS must be > 0".to_string());
        }
        if max_connections == 0 {
            return Err("ARGMIN_MAX_CONNECTIONS must be > 0".to_string());
        }
        if max_inflight_requests == 0 {
            return Err("ARGMIN_MAX_INFLIGHT_REQUESTS must be > 0".to_string());
        }
        if stream_read_chunk_size == 0 {
            return Err("ARGMIN_STREAM_READ_CHUNK_SIZE must be > 0".to_string());
        }
        if let Some(host_id) = &host_id {
            if host_id.is_empty() || !host_id.bytes().all(|b| b.is_ascii_graphic()) {
                return Err(
                    "ARGMIN_HOST_ID must be non-empty and contain only printable non-space ASCII"
                        .to_string(),
                );
            }
        }
        match (&tls_cert_path, &tls_key_path) {
            (Some(_), None) => {
                return Err(
                    "ARGMIN_TLS_KEY_PATH is required when ARGMIN_TLS_CERT_PATH is set".to_string(),
                )
            }
            (None, Some(_)) => {
                return Err(
                    "ARGMIN_TLS_CERT_PATH is required when ARGMIN_TLS_KEY_PATH is set".to_string(),
                )
            }
            _ => {}
        }

        Ok(Self {
            process_role,
            listen_addr,
            tls_cert_path,
            tls_key_path,
            data_dir,
            pg_count,
            local_node_count,
            storage_node_id,
            storage_node_data_dir,
            storage_node_socket_path,
            ec_k,
            ec_m,
            account_id,
            access_key_id,
            secret_access_key,
            uat_credentials,
            host_id,
            sse_c_validator_key_b64,
            sse_s3_wrapping_key_b64,
            region,
            workers,
            max_connections,
            max_inflight_requests,
            stream_read_chunk_size,
            panic_on_500,
            abort_on_500,
        })
    }
}

fn parse_bool_env(name: &str, value: &str) -> Result<bool, String> {
    match value.trim() {
        "1" | "true" | "TRUE" | "True" | "yes" | "YES" | "Yes" | "on" | "ON" | "On" => Ok(true),
        "0" | "false" | "FALSE" | "False" | "no" | "NO" | "No" | "off" | "OFF" | "Off" => Ok(false),
        _ => Err(format!("{name} must be a boolean value")),
    }
}

fn parse_process_role(value: &str) -> Result<ProcessRole, String> {
    match value.trim() {
        "frontend" => Ok(ProcessRole::Frontend),
        "storage-node" => Ok(ProcessRole::StorageNode),
        "combined" => Ok(ProcessRole::Combined),
        "legacy-local" => Ok(ProcessRole::LegacyLocal),
        _ => Err(
            "ARGMIN_PROCESS_ROLE must be one of frontend, storage-node, combined, legacy-local"
                .to_string(),
        ),
    }
}

fn process_role_name(role: ProcessRole) -> &'static str {
    match role {
        ProcessRole::LegacyLocal => "legacy-local",
        ProcessRole::Frontend => "frontend",
        ProcessRole::StorageNode => "storage-node",
        ProcessRole::Combined => "combined",
    }
}

pub(crate) fn unsupported_remote_frontend_role_message(role: ProcessRole) -> String {
    format!(
        "ARGMIN_PROCESS_ROLE={} is parsed but remote frontend routing is not wired until Phase 10.4/10.5",
        process_role_name(role)
    )
}

fn validate_account_id(name: &str, value: &str) -> Result<(), String> {
    if value.len() != 12 || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("{name} must be a 12-digit AWS account ID"));
    }
    Ok(())
}

fn optional_pair<F: Fn(&str) -> Option<String>>(
    get: &F,
    access_key_name: &str,
    secret_key_name: &str,
) -> Result<Option<(String, String)>, String> {
    match (get(access_key_name), get(secret_key_name)) {
        (Some(access_key_id), Some(secret_access_key)) => {
            Ok(Some((access_key_id, secret_access_key)))
        }
        (None, None) => Ok(None),
        (None, Some(_)) => Err(format!(
            "{access_key_name} is required with {secret_key_name}"
        )),
        (Some(_), None) => Err(format!(
            "{secret_key_name} is required with {access_key_name}"
        )),
    }
}

fn read_uat_credentials<F: Fn(&str) -> Option<String>>(
    get: &F,
    primary_account_id: &str,
) -> Result<Vec<ConfiguredCredential>, String> {
    let mut credentials = Vec::new();

    match (
        get("ARGMIN_UAT_ALT_ACCOUNT_ID"),
        get("ARGMIN_UAT_ALT_ACCESS_KEY_ID"),
        get("ARGMIN_UAT_ALT_SECRET_ACCESS_KEY"),
    ) {
        (Some(account_id), Some(access_key_id), Some(secret_access_key)) => {
            validate_account_id("ARGMIN_UAT_ALT_ACCOUNT_ID", &account_id)?;
            if account_id == primary_account_id {
                return Err(
                    "ARGMIN_UAT_ALT_ACCOUNT_ID must differ from ARGMIN_ACCOUNT_ID".to_string(),
                );
            }
            credentials.push(ConfiguredCredential {
                access_key_id,
                secret_access_key,
                account_id: account_id.clone(),
                principal: account_id.clone(),
                display_name: "argmin-uat-alt-account".to_string(),
                authorization_profile: ConfiguredCredentialProfile::OwnerAccountAdmin,
            });
        }
        (None, None, None) => {}
        _ => {
            return Err(
                "ARGMIN_UAT_ALT_ACCOUNT_ID, ARGMIN_UAT_ALT_ACCESS_KEY_ID, and ARGMIN_UAT_ALT_SECRET_ACCESS_KEY must be set together".to_string(),
            );
        }
    }

    if let Some((access_key_id, secret_access_key)) = optional_pair(
        get,
        "ARGMIN_UAT_SECOND_ACCESS_KEY_ID",
        "ARGMIN_UAT_SECOND_SECRET_ACCESS_KEY",
    )? {
        credentials.push(ConfiguredCredential {
            access_key_id,
            secret_access_key,
            account_id: primary_account_id.to_string(),
            principal: format!("arn:aws:iam::{primary_account_id}:user/limited"),
            display_name: "argmin-uat-second-user".to_string(),
            authorization_profile: ConfiguredCredentialProfile::Standard,
        });
    }

    if let Some((access_key_id, secret_access_key)) = optional_pair(
        get,
        "ARGMIN_UAT_OWNER_ROOT_ACCESS_KEY_ID",
        "ARGMIN_UAT_OWNER_ROOT_SECRET_ACCESS_KEY",
    )? {
        credentials.push(ConfiguredCredential {
            access_key_id,
            secret_access_key,
            account_id: primary_account_id.to_string(),
            principal: format!("arn:aws:iam::{primary_account_id}:root"),
            display_name: "argmin-uat-owner-root".to_string(),
            authorization_profile: ConfiguredCredentialProfile::OwnerAccountAdmin,
        });
    }

    Ok(credentials)
}

fn reject_duplicate_access_keys(
    primary_access_key_id: &str,
    uat_credentials: &[ConfiguredCredential],
) -> Result<(), String> {
    let mut seen = HashSet::new();
    seen.insert(primary_access_key_id);
    for credential in uat_credentials {
        if !seen.insert(credential.access_key_id.as_str()) {
            return Err(format!(
                "duplicate access key ID in configured credentials: {}",
                credential.access_key_id
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_env<'a>(overrides: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |key| {
            overrides
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
        }
    }

    fn make_required_env<'a>(
        overrides: &'a [(&'a str, &'a str)],
    ) -> impl Fn(&str) -> Option<String> + 'a {
        let mut values = required_only();
        for (key, value) in overrides {
            values.insert(key, value);
        }
        move |key| values.get(key).map(std::string::ToString::to_string)
    }

    fn required_only() -> HashMap<&'static str, &'static str> {
        let mut m = HashMap::new();
        m.insert("ARGMIN_ACCOUNT_ID", "111122223333");
        m.insert("ARGMIN_ACCESS_KEY_ID", "AKID");
        m.insert("ARGMIN_SECRET_ACCESS_KEY", "SECRET");
        m.insert(
            "ARGMIN_SSE_S3_WRAPPING_KEY",
            "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
        );
        m
    }

    fn lookup<'a>(m: &'a HashMap<&'a str, &'a str>) -> impl Fn(&str) -> Option<String> + 'a {
        move |key| m.get(key).map(std::string::ToString::to_string)
    }

    #[test]
    fn missing_access_key_id() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_ACCOUNT_ID", "111122223333"),
            ("ARGMIN_SECRET_ACCESS_KEY", "s"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_ACCESS_KEY_ID"));
    }

    #[test]
    fn missing_secret_access_key() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_ACCOUNT_ID", "111122223333"),
            ("ARGMIN_ACCESS_KEY_ID", "a"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_SECRET_ACCESS_KEY"));
    }

    #[test]
    fn missing_account_id() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_ACCESS_KEY_ID", "a"),
            ("ARGMIN_SECRET_ACCESS_KEY", "s"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_ACCOUNT_ID"));
    }

    #[test]
    fn invalid_account_id() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_ACCOUNT_ID", "not-an-account"),
            ("ARGMIN_ACCESS_KEY_ID", "a"),
            ("ARGMIN_SECRET_ACCESS_KEY", "s"),
        ]))
        .unwrap_err();
        assert!(err.contains("12-digit AWS account ID"));
    }

    #[test]
    fn defaults_applied() {
        let m = required_only();
        let cfg = ServerConfig::from_lookup(lookup(&m)).unwrap();
        assert_eq!(cfg.process_role, ProcessRole::LegacyLocal);
        assert_eq!(cfg.listen_addr, "127.0.0.1:9000");
        assert_eq!(cfg.tls_cert_path, None);
        assert_eq!(cfg.tls_key_path, None);
        assert_eq!(cfg.data_dir, "./data");
        assert_eq!(cfg.pg_count, 16);
        assert_eq!(cfg.local_node_count, 6);
        assert_eq!(cfg.storage_node_id, None);
        assert_eq!(cfg.storage_node_data_dir, None);
        assert_eq!(cfg.storage_node_socket_path, None);
        assert_eq!(cfg.ec_k, 4);
        assert_eq!(cfg.ec_m, 2);
        assert_eq!(cfg.account_id, "111122223333");
        assert_eq!(cfg.region, "us-east-1");
        assert!(cfg.uat_credentials.is_empty());
        assert_eq!(cfg.workers, 4);
        assert_eq!(cfg.max_connections, 512);
        assert_eq!(cfg.max_inflight_requests, 32);
        assert_eq!(
            cfg.stream_read_chunk_size,
            server_core::coordinator::INTERNAL_SEGMENT_SIZE
        );
        assert!(!cfg.panic_on_500);
        assert!(!cfg.abort_on_500);
        assert_eq!(cfg.access_key_id, "AKID");
        assert_eq!(cfg.secret_access_key, "SECRET");
        assert_eq!(cfg.host_id, None);
        assert_eq!(cfg.sse_c_validator_key_b64, None);
        assert_eq!(
            cfg.sse_s3_wrapping_key_b64,
            "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="
        );
    }

    #[test]
    fn custom_values() {
        let cfg = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_ACCOUNT_ID", "444455556666"),
            ("ARGMIN_ACCESS_KEY_ID", "mykey"),
            ("ARGMIN_SECRET_ACCESS_KEY", "mysecret"),
            ("ARGMIN_HOST_ID", "custom-host-id"),
            ("ARGMIN_SSE_C_VALIDATOR_KEY", "Zm9v"),
            ("ARGMIN_SSE_S3_WRAPPING_KEY", "YmFy"),
            ("ARGMIN_LISTEN_ADDR", "0.0.0.0:8080"),
            ("ARGMIN_TLS_CERT_PATH", "/tmp/cert.pem"),
            ("ARGMIN_TLS_KEY_PATH", "/tmp/key.pem"),
            ("ARGMIN_DATA_DIR", "/tmp/storage"),
            ("ARGMIN_PG_COUNT", "32"),
            ("ARGMIN_LOCAL_NODE_COUNT", "12"),
            ("ARGMIN_EC_K", "8"),
            ("ARGMIN_EC_M", "4"),
            ("ARGMIN_REGION", "eu-west-1"),
        ]))
        .unwrap();
        assert_eq!(cfg.listen_addr, "0.0.0.0:8080");
        assert_eq!(cfg.tls_cert_path.as_deref(), Some("/tmp/cert.pem"));
        assert_eq!(cfg.tls_key_path.as_deref(), Some("/tmp/key.pem"));
        assert_eq!(cfg.data_dir, "/tmp/storage");
        assert_eq!(cfg.pg_count, 32);
        assert_eq!(cfg.local_node_count, 12);
        assert_eq!(cfg.ec_k, 8);
        assert_eq!(cfg.ec_m, 4);
        assert_eq!(cfg.account_id, "444455556666");
        assert_eq!(cfg.region, "eu-west-1");
        assert_eq!(cfg.workers, 4); // not overridden, uses default
        assert_eq!(cfg.max_inflight_requests, 32); // not overridden, uses default
        assert_eq!(
            cfg.stream_read_chunk_size,
            server_core::coordinator::INTERNAL_SEGMENT_SIZE
        );
        assert_eq!(cfg.access_key_id, "mykey");
        assert_eq!(cfg.secret_access_key, "mysecret");
        assert_eq!(cfg.host_id.as_deref(), Some("custom-host-id"));
        assert_eq!(cfg.sse_c_validator_key_b64, Some("Zm9v".to_string()));
        assert_eq!(cfg.sse_s3_wrapping_key_b64, "YmFy");
    }

    #[test]
    fn process_role_storage_node_requires_storage_identity_and_socket() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_PROCESS_ROLE",
            "storage-node",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_STORAGE_NODE_ID"));

        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "storage-node"),
            ("ARGMIN_STORAGE_NODE_ID", "0"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_STORAGE_NODE_SOCKET_PATH"));
    }

    #[test]
    fn process_role_storage_node_parses_storage_config() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "storage-node"),
            ("ARGMIN_STORAGE_NODE_ID", "2"),
            ("ARGMIN_STORAGE_NODE_DATA_DIR", "/tmp/argmin-node-2"),
            ("ARGMIN_STORAGE_NODE_SOCKET_PATH", "/tmp/argmin/node-2.sock"),
        ]))
        .unwrap();

        assert_eq!(cfg.process_role, ProcessRole::StorageNode);
        assert_eq!(cfg.storage_node_id, Some(2));
        assert_eq!(
            cfg.storage_node_data_dir.as_deref(),
            Some("/tmp/argmin-node-2")
        );
        assert_eq!(
            cfg.storage_node_socket_path.as_deref(),
            Some("/tmp/argmin/node-2.sock")
        );
    }

    #[test]
    fn process_role_storage_node_does_not_require_frontend_secrets() {
        let cfg = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "storage-node"),
            ("ARGMIN_STORAGE_NODE_ID", "0"),
            ("ARGMIN_STORAGE_NODE_SOCKET_PATH", "/tmp/argmin/node-0.sock"),
        ]))
        .unwrap();

        assert_eq!(cfg.process_role, ProcessRole::StorageNode);
        assert_eq!(cfg.account_id, "");
        assert_eq!(cfg.access_key_id, "");
        assert_eq!(cfg.secret_access_key, "");
        assert!(cfg.uat_credentials.is_empty());
        assert_eq!(cfg.sse_s3_wrapping_key_b64, "");
    }

    #[test]
    fn process_role_frontend_fails_before_frontend_config_validation() {
        let err = ServerConfig::from_lookup(make_env(&[("ARGMIN_PROCESS_ROLE", "frontend")]))
            .unwrap_err();

        assert!(err.contains("remote frontend routing is not wired"));
    }

    #[test]
    fn process_role_combined_fails_before_storage_config_validation() {
        let err = ServerConfig::from_lookup(make_env(&[("ARGMIN_PROCESS_ROLE", "combined")]))
            .unwrap_err();

        assert!(err.contains("remote frontend routing is not wired"));
    }

    #[test]
    fn process_role_combined_fails_even_with_complete_local_config() {
        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "combined"),
            ("ARGMIN_STORAGE_NODE_ID", "0"),
            ("ARGMIN_STORAGE_NODE_SOCKET_PATH", "/tmp/argmin/node-0.sock"),
        ]))
        .unwrap_err();

        assert!(err.contains("remote frontend routing is not wired"));
    }

    #[test]
    fn process_role_rejects_unknown_value() {
        let err = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_PROCESS_ROLE", "other")]))
            .unwrap_err();

        assert!(err.contains("ARGMIN_PROCESS_ROLE"));
    }

    #[test]
    fn storage_node_id_must_be_in_configured_local_node_set() {
        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "storage-node"),
            ("ARGMIN_LOCAL_NODE_COUNT", "6"),
            ("ARGMIN_STORAGE_NODE_ID", "6"),
            ("ARGMIN_STORAGE_NODE_SOCKET_PATH", "/tmp/argmin/node-6.sock"),
        ]))
        .unwrap_err();

        assert!(err.contains("ARGMIN_STORAGE_NODE_ID"));
    }

    #[test]
    fn uat_acceptance_credentials() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_UAT_ALT_ACCOUNT_ID", "444455556666"),
            ("ARGMIN_UAT_ALT_ACCESS_KEY_ID", "alt"),
            ("ARGMIN_UAT_ALT_SECRET_ACCESS_KEY", "alt-secret"),
            ("ARGMIN_UAT_SECOND_ACCESS_KEY_ID", "second"),
            ("ARGMIN_UAT_SECOND_SECRET_ACCESS_KEY", "second-secret"),
            ("ARGMIN_UAT_OWNER_ROOT_ACCESS_KEY_ID", "root"),
            ("ARGMIN_UAT_OWNER_ROOT_SECRET_ACCESS_KEY", "root-secret"),
        ]))
        .unwrap();

        assert_eq!(cfg.uat_credentials.len(), 3);
        assert_eq!(cfg.uat_credentials[0].access_key_id, "alt");
        assert_eq!(cfg.uat_credentials[0].secret_access_key, "alt-secret");
        assert_eq!(cfg.uat_credentials[0].account_id, "444455556666");
        assert_eq!(cfg.uat_credentials[0].principal, "444455556666");
        assert_eq!(
            cfg.uat_credentials[0].display_name,
            "argmin-uat-alt-account"
        );
        assert_eq!(
            cfg.uat_credentials[0].authorization_profile,
            ConfiguredCredentialProfile::OwnerAccountAdmin
        );

        assert_eq!(cfg.uat_credentials[1].access_key_id, "second");
        assert_eq!(cfg.uat_credentials[1].account_id, "111122223333");
        assert_eq!(
            cfg.uat_credentials[1].principal,
            "arn:aws:iam::111122223333:user/limited"
        );
        assert_eq!(
            cfg.uat_credentials[1].display_name,
            "argmin-uat-second-user"
        );
        assert_eq!(
            cfg.uat_credentials[1].authorization_profile,
            ConfiguredCredentialProfile::Standard
        );

        assert_eq!(cfg.uat_credentials[2].access_key_id, "root");
        assert_eq!(cfg.uat_credentials[2].account_id, "111122223333");
        assert_eq!(
            cfg.uat_credentials[2].principal,
            "arn:aws:iam::111122223333:root"
        );
        assert_eq!(cfg.uat_credentials[2].display_name, "argmin-uat-owner-root");
        assert_eq!(
            cfg.uat_credentials[2].authorization_profile,
            ConfiguredCredentialProfile::OwnerAccountAdmin
        );
    }

    #[test]
    fn uat_alt_credentials_must_be_complete() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_UAT_ALT_ACCESS_KEY_ID",
            "alt",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_UAT_ALT_ACCOUNT_ID"));
    }

    #[test]
    fn uat_alt_account_must_differ_from_primary() {
        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_UAT_ALT_ACCOUNT_ID", "111122223333"),
            ("ARGMIN_UAT_ALT_ACCESS_KEY_ID", "alt"),
            ("ARGMIN_UAT_ALT_SECRET_ACCESS_KEY", "alt-secret"),
        ]))
        .unwrap_err();
        assert!(err.contains("must differ"));
    }

    #[test]
    fn uat_second_credentials_must_be_complete() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_UAT_SECOND_SECRET_ACCESS_KEY",
            "second-secret",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_UAT_SECOND_ACCESS_KEY_ID"));
    }

    #[test]
    fn uat_access_keys_must_be_unique() {
        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_UAT_ALT_ACCOUNT_ID", "444455556666"),
            ("ARGMIN_UAT_ALT_ACCESS_KEY_ID", "AKID"),
            ("ARGMIN_UAT_ALT_SECRET_ACCESS_KEY", "alt-secret"),
        ]))
        .unwrap_err();
        assert!(err.contains("duplicate access key ID"));
    }

    #[test]
    fn invalid_host_id() {
        let err = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_HOST_ID", "bad host")]))
            .unwrap_err();
        assert!(err.contains("ARGMIN_HOST_ID"));
    }

    #[test]
    fn missing_sse_s3_wrapping_key() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_ACCOUNT_ID", "111122223333"),
            ("ARGMIN_ACCESS_KEY_ID", "a"),
            ("ARGMIN_SECRET_ACCESS_KEY", "s"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_SSE_S3_WRAPPING_KEY"));
    }

    #[test]
    fn custom_workers() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_WORKERS", "8")])).unwrap();
        assert_eq!(cfg.workers, 8);
    }

    #[test]
    fn workers_zero() {
        let err =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_WORKERS", "0")])).unwrap_err();
        assert!(err.contains("ARGMIN_WORKERS must be > 0"));
    }

    #[test]
    fn invalid_workers() {
        let err =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_WORKERS", "abc")])).unwrap_err();
        assert!(err.contains("ARGMIN_WORKERS"));
    }

    #[test]
    fn invalid_pg_count_non_integer() {
        let err = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_PG_COUNT", "abc")]))
            .unwrap_err();
        assert!(err.contains("ARGMIN_PG_COUNT"));
    }

    #[test]
    fn pg_count_zero() {
        let err =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_PG_COUNT", "0")])).unwrap_err();
        assert!(err.contains("ARGMIN_PG_COUNT must be > 0"));
    }

    #[test]
    fn invalid_local_node_count_non_integer() {
        let err =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_LOCAL_NODE_COUNT", "abc")]))
                .unwrap_err();
        assert!(err.contains("ARGMIN_LOCAL_NODE_COUNT"));
    }

    #[test]
    fn local_node_count_zero() {
        let err = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_LOCAL_NODE_COUNT", "0")]))
            .unwrap_err();
        assert!(err.contains("ARGMIN_LOCAL_NODE_COUNT must be > 0"));
    }

    #[test]
    fn local_node_count_defaults_to_ec_shape_total() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_EC_K", "8"),
            ("ARGMIN_EC_M", "4"),
        ]))
        .unwrap();
        assert_eq!(cfg.local_node_count, 12);
    }

    #[test]
    fn local_node_count_one_rejected_for_default_ec_shape() {
        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_LOCAL_NODE_COUNT", "1"),
            ("ARGMIN_EC_K", "4"),
            ("ARGMIN_EC_M", "2"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_LOCAL_NODE_COUNT"));
        assert!(err.contains("at least ARGMIN_EC_K + ARGMIN_EC_M (6)"));
    }

    #[test]
    fn local_node_count_too_small_for_multihost_ec_shape() {
        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_LOCAL_NODE_COUNT", "5"),
            ("ARGMIN_EC_K", "4"),
            ("ARGMIN_EC_M", "2"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_LOCAL_NODE_COUNT"));
        assert!(err.contains("at least ARGMIN_EC_K + ARGMIN_EC_M (6)"));
    }

    #[test]
    fn local_node_count_accepts_first_valid_multihost_ec_shape() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_LOCAL_NODE_COUNT", "6"),
            ("ARGMIN_EC_K", "4"),
            ("ARGMIN_EC_M", "2"),
        ]))
        .unwrap();
        assert_eq!(cfg.local_node_count, 6);
    }

    #[test]
    fn invalid_ec_k() {
        let err = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_EC_K", "not_a_number")]))
            .unwrap_err();
        assert!(err.contains("ARGMIN_EC_K"));
    }

    #[test]
    fn invalid_ec_m() {
        let err =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_EC_M", "xyz")])).unwrap_err();
        assert!(err.contains("ARGMIN_EC_M"));
    }

    #[test]
    fn custom_max_connections() {
        let cfg =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_MAX_CONNECTIONS", "1024")]))
                .unwrap();
        assert_eq!(cfg.max_connections, 1024);
    }

    #[test]
    fn max_connections_zero() {
        let err = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_MAX_CONNECTIONS", "0")]))
            .unwrap_err();
        assert!(err.contains("ARGMIN_MAX_CONNECTIONS must be > 0"));
    }

    #[test]
    fn invalid_max_connections() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_MAX_CONNECTIONS",
            "not_a_number",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_MAX_CONNECTIONS"));
    }

    #[test]
    fn custom_max_inflight_requests() {
        let cfg =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_MAX_INFLIGHT_REQUESTS", "64")]))
                .unwrap();
        assert_eq!(cfg.max_inflight_requests, 64);
    }

    #[test]
    fn max_inflight_requests_zero() {
        let err =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_MAX_INFLIGHT_REQUESTS", "0")]))
                .unwrap_err();
        assert!(err.contains("ARGMIN_MAX_INFLIGHT_REQUESTS must be > 0"));
    }

    #[test]
    fn invalid_max_inflight_requests() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_MAX_INFLIGHT_REQUESTS",
            "not_a_number",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_MAX_INFLIGHT_REQUESTS"));
    }

    #[test]
    fn custom_stream_read_chunk_size() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_STREAM_READ_CHUNK_SIZE",
            "8388608",
        )]))
        .unwrap();
        assert_eq!(cfg.stream_read_chunk_size, 8 * 1024 * 1024);
    }

    #[test]
    fn stream_read_chunk_size_zero() {
        let err =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_STREAM_READ_CHUNK_SIZE", "0")]))
                .unwrap_err();
        assert!(err.contains("ARGMIN_STREAM_READ_CHUNK_SIZE must be > 0"));
    }

    #[test]
    fn panic_on_500_accepts_boolean_values() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_PANIC_ON_500", "true")]))
            .unwrap();
        assert!(cfg.panic_on_500);

        let cfg =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_PANIC_ON_500", "0")])).unwrap();
        assert!(!cfg.panic_on_500);
    }

    #[test]
    fn panic_on_500_rejects_invalid_boolean() {
        let err = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_PANIC_ON_500", "maybe")]))
            .unwrap_err();
        assert!(err.contains("ARGMIN_PANIC_ON_500"));
    }

    #[test]
    fn abort_on_500_accepts_boolean_values() {
        let cfg =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_ABORT_ON_500", "on")])).unwrap();
        assert!(cfg.abort_on_500);

        let cfg = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_ABORT_ON_500", "false")]))
            .unwrap();
        assert!(!cfg.abort_on_500);
    }

    #[test]
    fn abort_on_500_rejects_invalid_boolean() {
        let err = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_ABORT_ON_500", "maybe")]))
            .unwrap_err();
        assert!(err.contains("ARGMIN_ABORT_ON_500"));
    }
}
