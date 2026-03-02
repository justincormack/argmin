use aws_sdk_s3::primitives::ByteStream;
use s3_tests::{err_status, unique_bucket, CTX};

// ── Good names ──────────────────────────────────────────────────────────

#[test]
fn test_bucket_create_naming_good_starts_alpha() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = format!("abc-{}", &unique_bucket()[..8]);
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_starts_digit() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = format!("3bucket-{}", &unique_bucket()[..8]);
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_contains_period() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = format!("foo.bar.{}", &unique_bucket()[..8]);
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_contains_hyphen() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = format!("foo-bar-{}", &unique_bucket()[..8]);
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_long_63() {
    s3_tests::run(async {
        let client = CTX.client();
        // 63 chars is the max allowed
        let bucket = "a".repeat(63);
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_three_chars() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = "abc";
        client.create_bucket().bucket(bucket).send().await.unwrap();
        client.delete_bucket().bucket(bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_all_digits() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = "123456";
        client.create_bucket().bucket(bucket).send().await.unwrap();
        client.delete_bucket().bucket(bucket).send().await.unwrap();
    });
}

/// Verify that a bucket with periods in the name supports normal operations.
#[test]
fn test_bucket_create_naming_good_has_period_put_get() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = format!("a.b.{}", &unique_bucket()[..8]);
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

/// Verify that a bucket with hyphens supports normal operations.
#[test]
fn test_bucket_create_naming_good_has_hyphen_put_get() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = format!("a-b-{}", &unique_bucket()[..8]);
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
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_bad_short_one() {
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("a").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_bad_short_two() {
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("ab").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_dns_long() {
    s3_tests::run(async {
        let client = CTX.client();
        // 64 chars exceeds the limit
        let bucket = "a".repeat(64);
        let result = client.create_bucket().bucket(&bucket).send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_bad_long_256() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = "a".repeat(256);
        let result = client.create_bucket().bucket(&bucket).send().await;
        assert!(result.is_err());
    });
}

// ── Bad: start/end ──────────────────────────────────────────────────────

#[test]
fn test_bucket_create_naming_bad_starts_dash() {
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("-bucket").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_dns_dash_at_end() {
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("foo-").send().await;
        assert_eq!(err_status(&result), 400);
    });
}

#[test]
fn test_bucket_create_naming_bad_starts_dot() {
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket(".bucket").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_bad_ends_dot() {
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("bucket.").send().await;
        assert!(result.is_err());
    });
}

// ── Bad: invalid characters ─────────────────────────────────────────────

#[test]
fn test_bucket_create_naming_bad_uppercase() {
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("MyBucket").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_dns_underscore() {
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("foo_bar").send().await;
        assert_eq!(err_status(&result), 400);
    });
}

#[test]
fn test_bucket_create_naming_bad_special_at() {
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("foo@bar").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_bad_special_hash() {
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("foo#bar").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_bad_space() {
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("foo bar").send().await;
        assert!(result.is_err());
    });
}

// ── Bad: DNS label rules ────────────────────────────────────────────────

#[test]
fn test_bucket_create_naming_dns_dot_dot() {
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("foo..bar").send().await;
        assert_eq!(err_status(&result), 400);
    });
}

#[test]
fn test_bucket_create_naming_dns_dot_dash() {
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("foo.-bar").send().await;
        assert_eq!(err_status(&result), 400);
    });
}

#[test]
fn test_bucket_create_naming_dns_dash_dot() {
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("foo-.bar").send().await;
        assert_eq!(err_status(&result), 400);
    });
}

// ── Bad: reserved patterns ──────────────────────────────────────────────

#[test]
fn test_bucket_create_naming_bad_ip() {
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("192.168.5.123").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_bad_xn_prefix() {
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("xn--bucket").send().await;
        assert!(result.is_err());
    });
}

// ── Good: specific lengths ─────────────────────────────────────────────

#[test]
fn test_bucket_create_naming_good_long_60() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = "a".repeat(60);
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_long_61() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = "a".repeat(61);
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_long_62() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = "a".repeat(62);
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── Bad: non-alphanumeric start ────────────────────────────────────────

#[test]
fn test_bucket_create_naming_bad_starts_nonalpha() {
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("!bucket").send().await;
        assert!(result.is_err());
    });
}
