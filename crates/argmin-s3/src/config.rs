/// Server configuration, loaded from environment variables.
/// Configuration for the S3 server.
#[derive(Debug, Clone)]
pub(crate) struct ServerConfig {
    pub(crate) listen_addr: String,
    pub(crate) tls_cert_path: Option<String>,
    pub(crate) tls_key_path: Option<String>,
    pub(crate) data_dir: String,
    pub(crate) pg_count: u32,
    pub(crate) local_node_count: u32,
    pub(crate) ec_k: u8,
    pub(crate) ec_m: u8,
    pub(crate) account_id: String,
    pub(crate) access_key_id: String,
    pub(crate) secret_access_key: String,
    pub(crate) host_id: Option<String>,
    pub(crate) sse_c_validator_key_b64: Option<String>,
    pub(crate) sse_s3_wrapping_key_b64: String,
    pub(crate) region: String,
    pub(crate) workers: u32,
    pub(crate) max_connections: u32,
    pub(crate) max_inflight_requests: u32,
    pub(crate) stream_read_chunk_size: usize,
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
    ///   `ARGMIN_LOCAL_NODE_COUNT` (1)
    ///   `ARGMIN_EC_K` (4)
    ///   `ARGMIN_EC_M` (2)
    ///   `ARGMIN_REGION` (us-east-1)
    ///   `ARGMIN_WORKERS` (4)
    ///   `ARGMIN_MAX_CONNECTIONS` (512)
    ///   `ARGMIN_MAX_INFLIGHT_REQUESTS` (32)
    ///   `ARGMIN_STREAM_READ_CHUNK_SIZE` (8388608)
    pub(crate) fn from_env() -> Result<Self, String> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    /// Build configuration from an arbitrary key-lookup function.
    /// Used by `from_env` (with `std::env::var`) and directly by tests.
    fn from_lookup<F: Fn(&str) -> Option<String>>(get: F) -> Result<Self, String> {
        let account_id =
            get("ARGMIN_ACCOUNT_ID").ok_or_else(|| "ARGMIN_ACCOUNT_ID is required".to_string())?;
        if account_id.len() != 12 || !account_id.bytes().all(|b| b.is_ascii_digit()) {
            return Err("ARGMIN_ACCOUNT_ID must be a 12-digit AWS account ID".to_string());
        }
        let access_key_id = get("ARGMIN_ACCESS_KEY_ID")
            .ok_or_else(|| "ARGMIN_ACCESS_KEY_ID is required".to_string())?;
        let secret_access_key = get("ARGMIN_SECRET_ACCESS_KEY")
            .ok_or_else(|| "ARGMIN_SECRET_ACCESS_KEY is required".to_string())?;
        let host_id = get("ARGMIN_HOST_ID");
        let sse_c_validator_key_b64 = get("ARGMIN_SSE_C_VALIDATOR_KEY");
        let sse_s3_wrapping_key_b64 = get("ARGMIN_SSE_S3_WRAPPING_KEY")
            .ok_or_else(|| "ARGMIN_SSE_S3_WRAPPING_KEY is required".to_string())?;

        let listen_addr = get("ARGMIN_LISTEN_ADDR").unwrap_or_else(|| "127.0.0.1:9000".to_string());
        let tls_cert_path = get("ARGMIN_TLS_CERT_PATH");
        let tls_key_path = get("ARGMIN_TLS_KEY_PATH");
        let data_dir = get("ARGMIN_DATA_DIR").unwrap_or_else(|| "./data".to_string());
        let pg_count: u32 = get("ARGMIN_PG_COUNT")
            .unwrap_or_else(|| "16".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_PG_COUNT: {e}"))?;
        let local_node_count: u32 = get("ARGMIN_LOCAL_NODE_COUNT")
            .unwrap_or_else(|| "1".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_LOCAL_NODE_COUNT: {e}"))?;
        let ec_k: u8 = get("ARGMIN_EC_K")
            .unwrap_or_else(|| "4".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_EC_K: {e}"))?;
        let ec_m: u8 = get("ARGMIN_EC_M")
            .unwrap_or_else(|| "2".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_EC_M: {e}"))?;
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

        if pg_count == 0 {
            return Err("ARGMIN_PG_COUNT must be > 0".to_string());
        }
        if local_node_count == 0 {
            return Err("ARGMIN_LOCAL_NODE_COUNT must be > 0".to_string());
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
            listen_addr,
            tls_cert_path,
            tls_key_path,
            data_dir,
            pg_count,
            local_node_count,
            ec_k,
            ec_m,
            account_id,
            access_key_id,
            secret_access_key,
            host_id,
            sse_c_validator_key_b64,
            sse_s3_wrapping_key_b64,
            region,
            workers,
            max_connections,
            max_inflight_requests,
            stream_read_chunk_size,
        })
    }
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
        assert_eq!(cfg.listen_addr, "127.0.0.1:9000");
        assert_eq!(cfg.tls_cert_path, None);
        assert_eq!(cfg.tls_key_path, None);
        assert_eq!(cfg.data_dir, "./data");
        assert_eq!(cfg.pg_count, 16);
        assert_eq!(cfg.local_node_count, 1);
        assert_eq!(cfg.ec_k, 4);
        assert_eq!(cfg.ec_m, 2);
        assert_eq!(cfg.account_id, "111122223333");
        assert_eq!(cfg.region, "us-east-1");
        assert_eq!(cfg.workers, 4);
        assert_eq!(cfg.max_connections, 512);
        assert_eq!(cfg.max_inflight_requests, 32);
        assert_eq!(
            cfg.stream_read_chunk_size,
            server_core::coordinator::INTERNAL_SEGMENT_SIZE
        );
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
            ("ARGMIN_LOCAL_NODE_COUNT", "3"),
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
        assert_eq!(cfg.local_node_count, 3);
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
}
