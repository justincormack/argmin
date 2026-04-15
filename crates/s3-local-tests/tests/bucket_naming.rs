use std::sync::atomic::{AtomicU64, Ordering};

use aws_sdk_s3::primitives::ByteStream;
use s3_tests::{build_client_with_ca, err_status, TestServer, RT};

static BUCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

fn run_local<F: std::future::Future>(f: F) -> F::Output {
    RT.block_on(f)
}

async fn local_client() -> (TestServer, aws_sdk_s3::Client) {
    let server = TestServer::start().await;
    let client = build_client_with_ca(
        server.endpoint(),
        s3_tests::server::TEST_ACCESS_KEY,
        s3_tests::server::TEST_SECRET_KEY,
        s3_tests::server::TEST_REGION,
        server.tls_ca_pem(),
    );
    (server, client)
}

fn unique_bucket() -> String {
    let n = BUCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("local-bucket-{pid}-{n}-{timestamp}")
}

// ── Good names ──────────────────────────────────────────────────────────

#[test]
fn test_bucket_create_naming_good_starts_alpha() {
    run_local(async {
        let (_server, client) = local_client().await;
        let bucket = format!("abc-{}", unique_bucket());
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_starts_digit() {
    run_local(async {
        let (_server, client) = local_client().await;
        let bucket = format!("3bucket-{}", unique_bucket());
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_contains_period() {
    run_local(async {
        let (_server, client) = local_client().await;
        let bucket = format!("foo.bar.{}", unique_bucket());
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_contains_hyphen() {
    run_local(async {
        let (_server, client) = local_client().await;
        let bucket = format!("foo-bar-{}", unique_bucket());
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_long_63() {
    run_local(async {
        let (_server, client) = local_client().await;
        let base = unique_bucket();
        let bucket = if base.len() >= 63 {
            base[..63].to_string()
        } else {
            format!("{}{}", base, "a".repeat(63 - base.len()))
        };
        assert_eq!(bucket.len(), 63);
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_three_chars() {
    run_local(async {
        let (_server, client) = local_client().await;
        let bucket = "abc";
        client.create_bucket().bucket(bucket).send().await.unwrap();
        client.delete_bucket().bucket(bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_all_digits() {
    run_local(async {
        let (_server, client) = local_client().await;
        let bucket = "123456";
        client.create_bucket().bucket(bucket).send().await.unwrap();
        client.delete_bucket().bucket(bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_has_period_put_get() {
    run_local(async {
        let (_server, client) = local_client().await;
        let bucket = format!("a.b.{}", unique_bucket());
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("key")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("key")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"data");

        client
            .delete_object()
            .bucket(&bucket)
            .key("key")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_has_hyphen_put_get() {
    run_local(async {
        let (_server, client) = local_client().await;
        let bucket = format!("a-b-{}", unique_bucket());
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("key")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key("key")
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"data");

        client
            .delete_object()
            .bucket(&bucket)
            .key("key")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Bad: length ─────────────────────────────────────────────────────────

#[test]
fn test_bucket_create_naming_bad_short_empty() {
    run_local(async {
        let (_server, client) = local_client().await;
        let result = client.create_bucket().bucket("").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_bad_short_one() {
    run_local(async {
        let (_server, client) = local_client().await;
        let result = client.create_bucket().bucket("a").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_bad_short_two() {
    run_local(async {
        let (_server, client) = local_client().await;
        let result = client.create_bucket().bucket("ab").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_dns_long() {
    run_local(async {
        let (_server, client) = local_client().await;
        let bucket = "a".repeat(64);
        let result = client.create_bucket().bucket(&bucket).send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_bad_long_256() {
    run_local(async {
        let (_server, client) = local_client().await;
        let bucket = "a".repeat(256);
        let result = client.create_bucket().bucket(&bucket).send().await;
        assert!(result.is_err());
    });
}

// ── Bad: start/end ──────────────────────────────────────────────────────

#[test]
fn test_bucket_create_naming_bad_starts_dash() {
    run_local(async {
        let (_server, client) = local_client().await;
        let result = client.create_bucket().bucket("-bucket").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_dns_dash_at_end() {
    run_local(async {
        let (_server, client) = local_client().await;
        let result = client.create_bucket().bucket("foo-").send().await;
        assert_eq!(err_status(&result), 400);
    });
}

#[test]
fn test_bucket_create_naming_bad_starts_dot() {
    run_local(async {
        let (_server, client) = local_client().await;
        let result = client.create_bucket().bucket(".bucket").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_bad_ends_dot() {
    run_local(async {
        let (_server, client) = local_client().await;
        let result = client.create_bucket().bucket("bucket.").send().await;
        assert!(result.is_err());
    });
}

// ── Bad: invalid characters ─────────────────────────────────────────────

#[test]
fn test_bucket_create_naming_bad_uppercase() {
    run_local(async {
        let (_server, client) = local_client().await;
        let result = client.create_bucket().bucket("MyBucket").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_dns_underscore() {
    run_local(async {
        let (_server, client) = local_client().await;
        let result = client.create_bucket().bucket("foo_bar").send().await;
        assert_eq!(err_status(&result), 400);
    });
}

#[test]
fn test_bucket_create_naming_bad_special_at() {
    run_local(async {
        let (_server, client) = local_client().await;
        let result = client.create_bucket().bucket("foo@bar").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_bad_special_hash() {
    run_local(async {
        let (_server, client) = local_client().await;
        let result = client.create_bucket().bucket("foo#bar").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_bad_space() {
    run_local(async {
        let (_server, client) = local_client().await;
        let result = client.create_bucket().bucket("foo bar").send().await;
        assert!(result.is_err());
    });
}

// ── Bad: DNS label rules ────────────────────────────────────────────────

#[test]
fn test_bucket_create_naming_dns_dot_dot() {
    run_local(async {
        let (_server, client) = local_client().await;
        let result = client.create_bucket().bucket("foo..bar").send().await;
        assert_eq!(err_status(&result), 400);
    });
}

#[test]
fn test_bucket_create_naming_dns_dot_dash() {
    run_local(async {
        let (_server, client) = local_client().await;
        let result = client.create_bucket().bucket("foo.-bar").send().await;
        assert_eq!(err_status(&result), 400);
    });
}

#[test]
fn test_bucket_create_naming_dns_dash_dot() {
    run_local(async {
        let (_server, client) = local_client().await;
        let result = client.create_bucket().bucket("foo-.bar").send().await;
        assert_eq!(err_status(&result), 400);
    });
}

// ── Bad: reserved patterns ──────────────────────────────────────────────

#[test]
fn test_bucket_create_naming_bad_ip() {
    run_local(async {
        let (_server, client) = local_client().await;
        let result = client.create_bucket().bucket("192.168.5.123").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_bad_xn_prefix() {
    run_local(async {
        let (_server, client) = local_client().await;
        let result = client.create_bucket().bucket("xn--bucket").send().await;
        assert!(result.is_err());
    });
}

// ── Good: specific lengths ─────────────────────────────────────────────

#[test]
fn test_bucket_create_naming_good_long_60() {
    run_local(async {
        let (_server, client) = local_client().await;
        let base = unique_bucket();
        let bucket = if base.len() >= 60 {
            base[..60].to_string()
        } else {
            format!("{}{}", base, "a".repeat(60 - base.len()))
        };
        assert_eq!(bucket.len(), 60);
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_long_61() {
    run_local(async {
        let (_server, client) = local_client().await;
        let base = unique_bucket();
        let bucket = if base.len() >= 61 {
            base[..61].to_string()
        } else {
            format!("{}{}", base, "a".repeat(61 - base.len()))
        };
        assert_eq!(bucket.len(), 61);
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_long_62() {
    run_local(async {
        let (_server, client) = local_client().await;
        let base = unique_bucket();
        let bucket = if base.len() >= 62 {
            base[..62].to_string()
        } else {
            format!("{}{}", base, "a".repeat(62 - base.len()))
        };
        assert_eq!(bucket.len(), 62);
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Bad: non-alphanumeric start ────────────────────────────────────────

#[test]
fn test_bucket_create_naming_bad_starts_nonalpha() {
    run_local(async {
        let (_server, client) = local_client().await;
        let result = client.create_bucket().bucket("!bucket").send().await;
        assert!(result.is_err());
    });
}
