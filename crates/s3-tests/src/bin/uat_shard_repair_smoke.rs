use std::path::Path;

use s3_tests::{
    create_boe_bucket, delete_all_and_bucket, get_object_body_retrying_operation_aborted,
    put_object_retrying_operation_aborted, run, CTX,
};

fn usage() -> ! {
    eprintln!(
        "usage: uat_shard_repair_smoke put <bucket-file> <key> <body-file> | get <bucket-file> <key> <body-file> | cleanup <bucket-file> <key>"
    );
    std::process::exit(2);
}

fn read_bucket(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("read bucket file {}: {error}", path.display()))
        .trim()
        .to_string()
}

fn main() {
    let mut args = std::env::args_os().skip(1);
    let Some(command) = args.next().and_then(|arg| arg.into_string().ok()) else {
        usage();
    };

    match command.as_str() {
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
                let bucket = create_boe_bucket(CTX.client()).await;
                let body = std::fs::read(&body_file)
                    .unwrap_or_else(|error| panic!("read body file {:?}: {error}", body_file));
                put_object_retrying_operation_aborted(CTX.client(), &bucket, &key, body).await;
                std::fs::write(&bucket_file, format!("{bucket}\n")).unwrap_or_else(|error| {
                    panic!("write bucket file {:?}: {error}", bucket_file);
                });
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
                let bucket = read_bucket(Path::new(&bucket_file));
                let expected = std::fs::read(&body_file)
                    .unwrap_or_else(|error| panic!("read body file {:?}: {error}", body_file));
                let actual = get_object_body_retrying_operation_aborted(
                    CTX.client(),
                    &bucket,
                    &key,
                    None,
                    "uat shard repair get object",
                )
                .await;
                assert_eq!(actual, expected, "recovered object body mismatch");
            });
        }
        "cleanup" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            let Some(key) = args.next().and_then(|arg| arg.into_string().ok()) else {
                usage();
            };
            if args.next().is_some() {
                usage();
            }
            run(async {
                let bucket = read_bucket(Path::new(&bucket_file));
                delete_all_and_bucket(CTX.client(), &bucket, &[key]).await;
            });
        }
        _ => usage(),
    }
}
