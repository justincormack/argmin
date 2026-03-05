use aws_sdk_s3::primitives::ByteStream;
use s3_tests::{err_status, unique_bucket, CTX};

/// Returns `true` when running against an external endpoint (e.g. AWS).
///
/// Bucket naming tests use hardcoded or short names that can collide in the
/// global AWS bucket namespace.  They only validate our server's naming logic,
/// so skip them when the target is a real S3 endpoint.
fn is_external() -> bool {
    std::env::var("S3_TEST_ENDPOINT").is_ok()
}

// ── Good names ──────────────────────────────────────────────────────────

#[test]
fn test_bucket_create_naming_good_starts_alpha() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = format!("abc-{}", unique_bucket());
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_starts_digit() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = format!("3bucket-{}", unique_bucket());
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_contains_period() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = format!("foo.bar.{}", unique_bucket());
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_contains_hyphen() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = format!("foo-bar-{}", unique_bucket());
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_long_63() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        // 63 chars is the max allowed — pad unique_bucket() to exactly 63
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
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = "abc";
        client.create_bucket().bucket(bucket).send().await.unwrap();
        client.delete_bucket().bucket(bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_naming_good_all_digits() {
    if is_external() {
        return;
    }
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
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
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

/// Verify that a bucket with hyphens supports normal operations.
#[test]
fn test_bucket_create_naming_good_has_hyphen_put_get() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
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
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_bad_short_one() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("a").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_bad_short_two() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("ab").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_dns_long() {
    if is_external() {
        return;
    }
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
    if is_external() {
        return;
    }
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
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("-bucket").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_dns_dash_at_end() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("foo-").send().await;
        assert_eq!(err_status(&result), 400);
    });
}

#[test]
fn test_bucket_create_naming_bad_starts_dot() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket(".bucket").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_bad_ends_dot() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("bucket.").send().await;
        assert!(result.is_err());
    });
}

// ── Bad: invalid characters ─────────────────────────────────────────────

#[test]
fn test_bucket_create_naming_bad_uppercase() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("MyBucket").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_dns_underscore() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("foo_bar").send().await;
        assert_eq!(err_status(&result), 400);
    });
}

#[test]
fn test_bucket_create_naming_bad_special_at() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("foo@bar").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_bad_special_hash() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("foo#bar").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_bad_space() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("foo bar").send().await;
        assert!(result.is_err());
    });
}

// ── Bad: DNS label rules ────────────────────────────────────────────────

#[test]
fn test_bucket_create_naming_dns_dot_dot() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("foo..bar").send().await;
        assert_eq!(err_status(&result), 400);
    });
}

#[test]
fn test_bucket_create_naming_dns_dot_dash() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("foo.-bar").send().await;
        assert_eq!(err_status(&result), 400);
    });
}

#[test]
fn test_bucket_create_naming_dns_dash_dot() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("foo-.bar").send().await;
        assert_eq!(err_status(&result), 400);
    });
}

// ── Bad: reserved patterns ──────────────────────────────────────────────

#[test]
fn test_bucket_create_naming_bad_ip() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("192.168.5.123").send().await;
        assert!(result.is_err());
    });
}

#[test]
fn test_bucket_create_naming_bad_xn_prefix() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("xn--bucket").send().await;
        assert!(result.is_err());
    });
}

// ── Good: specific lengths ─────────────────────────────────────────────

#[test]
fn test_bucket_create_naming_good_long_60() {
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
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
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
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
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
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
    if is_external() {
        return;
    }
    s3_tests::run(async {
        let client = CTX.client();
        let result = client.create_bucket().bucket("!bucket").send().await;
        assert!(result.is_err());
    });
}
