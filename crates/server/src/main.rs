use std::path::Path;

use auth::{CredentialStore, SecretKey};
use ec::EcConfig;
use storage::{LocalStorageNode, SqliteBucketDb};

use server::config::ServerConfig;
use server::coordinator::Coordinator;
use server::http::HttpFrontend;

fn main() {
    let config = match ServerConfig::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("configuration error: {}", e);
            std::process::exit(1);
        }
    };

    // EC self-test
    if let Err(e) = ec::self_test() {
        eprintln!("EC self-test failed: {}", e);
        std::process::exit(1);
    }

    // Build EC config
    let ec_config = match EcConfig::new(config.ec_k, config.ec_m) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("invalid EC config: {}", e);
            std::process::exit(1);
        }
    };

    // Open storage node
    let pg_ids: Vec<u32> = (0..config.pg_count).collect();
    let data_dir = Path::new(&config.data_dir);
    let storage_node = match LocalStorageNode::open(data_dir, &pg_ids) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("failed to open storage: {}", e);
            std::process::exit(1);
        }
    };

    // Open bucket database
    let bucket_db_path = data_dir.join("buckets.db");
    let bucket_db = match SqliteBucketDb::open(&bucket_db_path) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("failed to open bucket database: {}", e);
            std::process::exit(1);
        }
    };

    // Build coordinator
    let coordinator = match Coordinator::new(
        storage_node,
        bucket_db,
        ec_config,
        config.pg_count,
        config.region.clone(),
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to create coordinator: {}", e);
            std::process::exit(1);
        }
    };

    // Build credential store
    let mut credentials = CredentialStore::new();
    credentials.add(
        config.access_key_id.clone(),
        SecretKey(config.secret_access_key.clone()),
    );

    let frontend = HttpFrontend {
        coordinator,
        credentials,
    };

    // Start HTTP server
    let server = match tiny_http::Server::http(&config.listen_addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("failed to start HTTP server: {}", e);
            std::process::exit(1);
        }
    };

    eprintln!(
        "argmin-s3 listening on {} (EC {},{}, {} PGs, region {})",
        config.listen_addr, config.ec_k, config.ec_m, config.pg_count, config.region
    );

    // Serve requests serially
    for request in server.incoming_requests() {
        frontend.handle_request(request);
    }
}
