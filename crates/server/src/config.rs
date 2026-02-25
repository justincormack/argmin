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
        let access_key_id = std::env::var("ARGMIN_ACCESS_KEY_ID")
            .map_err(|_| "ARGMIN_ACCESS_KEY_ID is required".to_string())?;
        let secret_access_key = std::env::var("ARGMIN_SECRET_ACCESS_KEY")
            .map_err(|_| "ARGMIN_SECRET_ACCESS_KEY is required".to_string())?;

        let listen_addr = std::env::var("ARGMIN_LISTEN_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:9000".to_string());
        let data_dir =
            std::env::var("ARGMIN_DATA_DIR").unwrap_or_else(|_| "./data".to_string());
        let pg_count: u32 = std::env::var("ARGMIN_PG_COUNT")
            .unwrap_or_else(|_| "16".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_PG_COUNT: {}", e))?;
        let ec_k: u8 = std::env::var("ARGMIN_EC_K")
            .unwrap_or_else(|_| "4".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_EC_K: {}", e))?;
        let ec_m: u8 = std::env::var("ARGMIN_EC_M")
            .unwrap_or_else(|_| "2".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_EC_M: {}", e))?;
        let region =
            std::env::var("ARGMIN_REGION").unwrap_or_else(|_| "us-east-1".to_string());

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
