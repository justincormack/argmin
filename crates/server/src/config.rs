/// Server configuration, loaded from environment variables.

/// Configuration for the S3 server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub listen_addr: String,
    pub data_dir: String,
    pub pg_count: u32,
    pub ec_k: u8,
    pub ec_m: u8,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
}

impl ServerConfig {
    /// Load configuration from environment variables.
    ///
    /// Required: ARGMIN_ACCESS_KEY_ID, ARGMIN_SECRET_ACCESS_KEY
    /// Optional (with defaults):
    ///   ARGMIN_LISTEN_ADDR (127.0.0.1:9000)
    ///   ARGMIN_DATA_DIR (./data)
    ///   ARGMIN_PG_COUNT (16)
    ///   ARGMIN_EC_K (4)
    ///   ARGMIN_EC_M (2)
    ///   ARGMIN_REGION (us-east-1)
    pub fn from_env() -> Result<Self, String> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    /// Build configuration from an arbitrary key-lookup function.
    /// Used by `from_env` (with `std::env::var`) and directly by tests.
    fn from_lookup<F: Fn(&str) -> Option<String>>(get: F) -> Result<Self, String> {
        let access_key_id = get("ARGMIN_ACCESS_KEY_ID")
            .ok_or_else(|| "ARGMIN_ACCESS_KEY_ID is required".to_string())?;
        let secret_access_key = get("ARGMIN_SECRET_ACCESS_KEY")
            .ok_or_else(|| "ARGMIN_SECRET_ACCESS_KEY is required".to_string())?;

        let listen_addr = get("ARGMIN_LISTEN_ADDR")
            .unwrap_or_else(|| "127.0.0.1:9000".to_string());
        let data_dir = get("ARGMIN_DATA_DIR")
            .unwrap_or_else(|| "./data".to_string());
        let pg_count: u32 = get("ARGMIN_PG_COUNT")
            .unwrap_or_else(|| "16".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_PG_COUNT: {}", e))?;
        let ec_k: u8 = get("ARGMIN_EC_K")
            .unwrap_or_else(|| "4".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_EC_K: {}", e))?;
        let ec_m: u8 = get("ARGMIN_EC_M")
            .unwrap_or_else(|| "2".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_EC_M: {}", e))?;
        let region = get("ARGMIN_REGION")
            .unwrap_or_else(|| "us-east-1".to_string());

        if pg_count == 0 {
            return Err("ARGMIN_PG_COUNT must be > 0".to_string());
        }

        Ok(Self {
            listen_addr,
            data_dir,
            pg_count,
            ec_k,
            ec_m,
            access_key_id,
            secret_access_key,
            region,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_env<'a>(overrides: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |key| overrides.iter().find(|(k, _)| *k == key).map(|(_, v)| v.to_string())
    }

    fn required_only() -> HashMap<&'static str, &'static str> {
        let mut m = HashMap::new();
        m.insert("ARGMIN_ACCESS_KEY_ID", "AKID");
        m.insert("ARGMIN_SECRET_ACCESS_KEY", "SECRET");
        m
    }

    fn lookup<'a>(m: &'a HashMap<&'a str, &'a str>) -> impl Fn(&str) -> Option<String> + 'a {
        move |key| m.get(key).map(|v| v.to_string())
    }

    #[test]
    fn missing_access_key_id() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_SECRET_ACCESS_KEY", "s"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_ACCESS_KEY_ID"));
    }

    #[test]
    fn missing_secret_access_key() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_ACCESS_KEY_ID", "a"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_SECRET_ACCESS_KEY"));
    }

    #[test]
    fn defaults_applied() {
        let m = required_only();
        let cfg = ServerConfig::from_lookup(lookup(&m)).unwrap();
        assert_eq!(cfg.listen_addr, "127.0.0.1:9000");
        assert_eq!(cfg.data_dir, "./data");
        assert_eq!(cfg.pg_count, 16);
        assert_eq!(cfg.ec_k, 4);
        assert_eq!(cfg.ec_m, 2);
        assert_eq!(cfg.region, "us-east-1");
        assert_eq!(cfg.access_key_id, "AKID");
        assert_eq!(cfg.secret_access_key, "SECRET");
    }

    #[test]
    fn custom_values() {
        let cfg = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_ACCESS_KEY_ID", "mykey"),
            ("ARGMIN_SECRET_ACCESS_KEY", "mysecret"),
            ("ARGMIN_LISTEN_ADDR", "0.0.0.0:8080"),
            ("ARGMIN_DATA_DIR", "/tmp/storage"),
            ("ARGMIN_PG_COUNT", "32"),
            ("ARGMIN_EC_K", "8"),
            ("ARGMIN_EC_M", "4"),
            ("ARGMIN_REGION", "eu-west-1"),
        ]))
        .unwrap();
        assert_eq!(cfg.listen_addr, "0.0.0.0:8080");
        assert_eq!(cfg.data_dir, "/tmp/storage");
        assert_eq!(cfg.pg_count, 32);
        assert_eq!(cfg.ec_k, 8);
        assert_eq!(cfg.ec_m, 4);
        assert_eq!(cfg.region, "eu-west-1");
        assert_eq!(cfg.access_key_id, "mykey");
        assert_eq!(cfg.secret_access_key, "mysecret");
    }

    #[test]
    fn invalid_pg_count_non_integer() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_ACCESS_KEY_ID", "a"),
            ("ARGMIN_SECRET_ACCESS_KEY", "s"),
            ("ARGMIN_PG_COUNT", "abc"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_PG_COUNT"));
    }

    #[test]
    fn pg_count_zero() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_ACCESS_KEY_ID", "a"),
            ("ARGMIN_SECRET_ACCESS_KEY", "s"),
            ("ARGMIN_PG_COUNT", "0"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_PG_COUNT must be > 0"));
    }

    #[test]
    fn invalid_ec_k() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_ACCESS_KEY_ID", "a"),
            ("ARGMIN_SECRET_ACCESS_KEY", "s"),
            ("ARGMIN_EC_K", "not_a_number"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_EC_K"));
    }

    #[test]
    fn invalid_ec_m() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_ACCESS_KEY_ID", "a"),
            ("ARGMIN_SECRET_ACCESS_KEY", "s"),
            ("ARGMIN_EC_M", "xyz"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_EC_M"));
    }
}
