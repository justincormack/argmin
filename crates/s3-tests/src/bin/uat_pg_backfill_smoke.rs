use std::path::Path;

use s3_tests::{
    build_client_with_ca, delete_bucket_retrying_operation_aborted,
    delete_object_retrying_operation_aborted, get_object_body_retrying_operation_aborted,
    put_object_retrying_operation_aborted, retrying_operation_aborted_result, unique_bucket, RT,
};
use storage::{BucketName, GenerationId, ObjectKey, PgTopology};

fn usage() -> ! {
    eprintln!(
        "usage: uat_pg_backfill_smoke create-put <bucket-file> <key> <body-file> | create-put-distinct-data-pg <bucket-file> <key-file> <data-pg-file> <key-prefix> <body-file> | put-for-data-pg <bucket-file> <key-file> <key-prefix> <body-file> <data-pg> | put <bucket-file> <key> <body-file> | get <bucket-file> <key> <body-file> | cleanup <bucket-file> <key>..."
    );
    std::process::exit(2);
}

fn read_bucket(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("read bucket file {}: {error}", path.display()))
        .trim()
        .to_string()
}

fn read_body(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|error| panic!("read body file {}: {error}", path.display()))
}

fn run<F: std::future::Future>(future: F) -> F::Output {
    RT.block_on(future)
}

fn client_from_env() -> s3_tests::aws_sdk_s3::Client {
    let endpoint = std::env::var("S3_TEST_ENDPOINT").expect("S3_TEST_ENDPOINT is required");
    let access_key = std::env::var("S3_TEST_ACCESS_KEY").expect("S3_TEST_ACCESS_KEY is required");
    let secret_key = std::env::var("S3_TEST_SECRET_KEY").expect("S3_TEST_SECRET_KEY is required");
    let region = std::env::var("S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let tls_ca_pem = std::env::var("S3_TEST_TLS_CA_CERT_PATH").ok().map(|path| {
        std::fs::read(&path).unwrap_or_else(|error| {
            panic!("read S3_TEST_TLS_CA_CERT_PATH {path}: {error}");
        })
    });
    build_client_with_ca(
        &endpoint,
        &access_key,
        &secret_key,
        &region,
        tls_ca_pem.as_deref(),
    )
}

async fn create_bucket(client: &s3_tests::aws_sdk_s3::Client, bucket: &str) {
    retrying_operation_aborted_result(|| {
        let request = client.create_bucket().bucket(bucket);
        Box::pin(async move { request.send().await.map(|_| ()) })
    })
    .await
    .unwrap_or_else(|error| panic!("create UAT bucket {bucket}: {error:?}"));
}

async fn put_object(client: &s3_tests::aws_sdk_s3::Client, bucket: &str, key: &str, body: Vec<u8>) {
    put_object_retrying_operation_aborted(client, bucket, key, body).await;
}

async fn get_object_body(
    client: &s3_tests::aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> Vec<u8> {
    get_object_body_retrying_operation_aborted(
        client,
        bucket,
        key,
        None,
        "uat PG backfill get object",
    )
    .await
}

async fn cleanup_bucket(client: &s3_tests::aws_sdk_s3::Client, bucket: &str, keys: &[String]) {
    for key in keys {
        delete_object_retrying_operation_aborted(client, bucket, key)
            .await
            .unwrap_or_else(|error| panic!("delete UAT object {bucket}/{key}: {error:?}"));
    }
    delete_bucket_retrying_operation_aborted(client, bucket).await;
}

fn pg_topology_from_env() -> PgTopology {
    let pg_count = std::env::var("ARGMIN_PG_COUNT")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1);
    let pg_ids: Vec<u32> = (0..pg_count).collect();
    PgTopology::new(&pg_ids).expect("UAT PG topology must be valid")
}

fn choose_key_with_distinct_data_pg(
    bucket: &str,
    key_prefix: &str,
    target_data_pg: Option<u32>,
) -> (String, u32) {
    let topology = pg_topology_from_env();
    let bucket_name = BucketName::try_from(bucket.to_string()).expect("UAT bucket must be valid");
    let bucket_pg = topology.bucket_metadata_pg_for(&bucket_name).get();
    let generation_id = GenerationId::new(1).expect("first object generation id is valid");

    for suffix in 0..10_000u32 {
        let key = format!("{key_prefix}-{suffix:04}");
        let object_key = ObjectKey::try_from(key.clone()).expect("UAT key must be valid");
        let object_pg = topology
            .object_metadata_pg_for(&bucket_name, &object_key)
            .get();
        let data_pg = topology
            .object_generation_segment_data_pg(&bucket_name, &object_key, generation_id, 0)
            .get();
        if data_pg != bucket_pg
            && data_pg != object_pg
            && target_data_pg.is_none_or(|target| data_pg == target)
        {
            return (key, data_pg);
        }
    }
    panic!("could not find UAT key with distinct bucket/object metadata PG and data PG");
}

fn main() {
    let mut args = std::env::args_os().skip(1);
    let Some(command) = args.next().and_then(|arg| arg.into_string().ok()) else {
        usage();
    };

    match command.as_str() {
        "create-put" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            let Some(key) = args.next().and_then(|arg| arg.into_string().ok()) else {
                usage();
            };
            let Some(body_file) = args.next() else {
                usage();
            };
            if args.next().is_some() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let bucket = unique_bucket();
                create_bucket(&client, &bucket).await;
                let body = read_body(Path::new(&body_file));
                put_object(&client, &bucket, &key, body).await;
                std::fs::write(&bucket_file, format!("{bucket}\n")).unwrap_or_else(|error| {
                    panic!("write bucket file {:?}: {error}", bucket_file);
                });
            });
        }
        "create-put-distinct-data-pg" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            let Some(key_file) = args.next() else {
                usage();
            };
            let Some(data_pg_file) = args.next() else {
                usage();
            };
            let Some(key_prefix) = args.next().and_then(|arg| arg.into_string().ok()) else {
                usage();
            };
            let Some(body_file) = args.next() else {
                usage();
            };
            if args.next().is_some() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let bucket = unique_bucket();
                create_bucket(&client, &bucket).await;
                let (key, data_pg) = choose_key_with_distinct_data_pg(&bucket, &key_prefix, None);
                let body = read_body(Path::new(&body_file));
                put_object(&client, &bucket, &key, body).await;
                std::fs::write(&bucket_file, format!("{bucket}\n")).unwrap_or_else(|error| {
                    panic!("write bucket file {:?}: {error}", bucket_file);
                });
                std::fs::write(&key_file, format!("{key}\n")).unwrap_or_else(|error| {
                    panic!("write key file {:?}: {error}", key_file);
                });
                std::fs::write(&data_pg_file, format!("{data_pg}\n")).unwrap_or_else(|error| {
                    panic!("write data PG file {:?}: {error}", data_pg_file);
                });
            });
        }
        "put-for-data-pg" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            let Some(key_file) = args.next() else {
                usage();
            };
            let Some(key_prefix) = args.next().and_then(|arg| arg.into_string().ok()) else {
                usage();
            };
            let Some(body_file) = args.next() else {
                usage();
            };
            let Some(data_pg) = args
                .next()
                .and_then(|arg| arg.into_string().ok())
                .and_then(|value| value.parse::<u32>().ok())
            else {
                usage();
            };
            if args.next().is_some() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let bucket = read_bucket(Path::new(&bucket_file));
                let (key, actual_data_pg) =
                    choose_key_with_distinct_data_pg(&bucket, &key_prefix, Some(data_pg));
                assert_eq!(actual_data_pg, data_pg);
                let body = read_body(Path::new(&body_file));
                put_object(&client, &bucket, &key, body).await;
                std::fs::write(&key_file, format!("{key}\n")).unwrap_or_else(|error| {
                    panic!("write key file {:?}: {error}", key_file);
                });
            });
        }
        "put" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            let Some(key) = args.next().and_then(|arg| arg.into_string().ok()) else {
                usage();
            };
            let Some(body_file) = args.next() else {
                usage();
            };
            if args.next().is_some() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let bucket = read_bucket(Path::new(&bucket_file));
                let body = read_body(Path::new(&body_file));
                put_object(&client, &bucket, &key, body).await;
            });
        }
        "get" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            let Some(key) = args.next().and_then(|arg| arg.into_string().ok()) else {
                usage();
            };
            let Some(body_file) = args.next() else {
                usage();
            };
            if args.next().is_some() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let bucket = read_bucket(Path::new(&bucket_file));
                let expected = read_body(Path::new(&body_file));
                let actual = get_object_body(&client, &bucket, &key).await;
                assert_eq!(actual, expected, "object body mismatch for {key}");
            });
        }
        "cleanup" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            let keys: Vec<String> = args
                .map(|arg| arg.into_string().unwrap_or_else(|_| usage()))
                .collect();
            if keys.is_empty() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let bucket = read_bucket(Path::new(&bucket_file));
                cleanup_bucket(&client, &bucket, &keys).await;
            });
        }
        _ => usage(),
    }
}
