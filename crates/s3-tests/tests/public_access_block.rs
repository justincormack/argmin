use aws_sdk_s3::types::BucketCannedAcl;
use s3_tests::{unique_bucket, CTX};

/// Build an agent that returns all HTTP responses (including 4xx/5xx) as Ok.
fn agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .new_agent()
}

/// Cleanup helper.
async fn cleanup(bucket: &str) {
    let client = CTX.client();
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

// ── test_put_public_block ─────────────────────────────────────────────

#[test]
fn test_put_public_block() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        // PUT public access block — matches Ceph: RestrictPublicBuckets=false
        let pab = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .block_public_acls(true)
            .ignore_public_acls(true)
            .block_public_policy(true)
            .restrict_public_buckets(false)
            .build();

        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();

        // GET it back and verify all flags
        let resp = client
            .get_public_access_block()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        let config = resp.public_access_block_configuration().unwrap();
        assert_eq!(config.block_public_acls(), Some(true));
        assert_eq!(config.ignore_public_acls(), Some(true));
        assert_eq!(config.block_public_policy(), Some(true));
        assert_eq!(config.restrict_public_buckets(), Some(false));

        cleanup(&bucket).await;
    });
}

// ── test_put_get_delete_public_block ──────────────────────────────────

#[test]
fn test_put_get_delete_public_block() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        // PUT config
        let pab = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .block_public_acls(true)
            .build();

        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();

        // GET should succeed — verify all 4 fields
        let resp = client
            .get_public_access_block()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let config = resp.public_access_block_configuration().unwrap();
        assert_eq!(config.block_public_acls(), Some(true));
        assert_eq!(config.ignore_public_acls(), Some(false));
        assert_eq!(config.block_public_policy(), Some(false));
        assert_eq!(config.restrict_public_buckets(), Some(false));

        // DELETE
        client
            .delete_public_access_block()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        // GET after delete should fail
        let err = client
            .get_public_access_block()
            .bucket(&bucket)
            .send()
            .await
            .unwrap_err();
        let raw = format!("{:?}", err);
        assert!(
            raw.contains("NoSuchPublicAccessBlockConfiguration") || raw.contains("404"),
            "expected NoSuchPublicAccessBlockConfiguration, got: {}",
            raw
        );

        cleanup(&bucket).await;
    });
}

// ── test_get_undefined_public_block ───────────────────────────────────

#[test]
fn test_get_undefined_public_block() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        // Delete first (matching Ceph: ensures clean state)
        client
            .delete_public_access_block()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        // GET after delete should fail
        let err = client
            .get_public_access_block()
            .bucket(&bucket)
            .send()
            .await
            .unwrap_err();
        let raw = format!("{:?}", err);
        assert!(
            raw.contains("NoSuchPublicAccessBlockConfiguration") || raw.contains("404"),
            "expected NoSuchPublicAccessBlockConfiguration, got: {}",
            raw
        );

        cleanup(&bucket).await;
    });
}

// ── test_block_public_put_bucket_acls ─────────────────────────────────

#[test]
fn test_block_public_put_bucket_acls() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        // Set BlockPublicAcls = true
        let pab = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .block_public_acls(true)
            .build();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();

        // Attempt PutBucketAcl with public-read → should be denied
        let url = format!("{}/{}?acl", CTX.endpoint(), bucket);
        let resp = send_signed_put(&url, b"", &[("x-amz-acl", "public-read")]);
        assert_eq!(
            resp, 403,
            "expected 403 for public-read when BlockPublicAcls is set, got {}",
            resp
        );

        // public-read-write → should also be denied
        let resp = send_signed_put(&url, b"", &[("x-amz-acl", "public-read-write")]);
        assert_eq!(
            resp, 403,
            "expected 403 for public-read-write when BlockPublicAcls is set, got {}",
            resp
        );

        // authenticated-read → should also be denied
        let resp = send_signed_put(&url, b"", &[("x-amz-acl", "authenticated-read")]);
        assert_eq!(
            resp, 403,
            "expected 403 for authenticated-read when BlockPublicAcls is set, got {}",
            resp
        );

        // PutBucketAcl with private should succeed
        let resp = send_signed_put(&url, b"", &[("x-amz-acl", "private")]);
        assert_eq!(resp, 200, "expected 200 for private ACL, got {}", resp);

        cleanup(&bucket).await;
    });
}

// ── test_ignore_public_acls ───────────────────────────────────────────

#[test]
fn test_ignore_public_acls() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        // Create a public-read bucket
        client
            .create_bucket()
            .bucket(&bucket)
            .acl(BucketCannedAcl::PublicRead)
            .send()
            .await
            .unwrap();

        let alt_client = CTX.alt_client();

        // Upload an object
        client
            .put_object()
            .bucket(&bucket)
            .key("key1")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abcde"))
            .send()
            .await
            .unwrap();

        // Verify alt_client (non-owner) can list objects on public-read bucket
        let list_resp = alt_client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await;
        assert!(
            list_resp.is_ok(),
            "public-read bucket should allow alt_client list_objects"
        );
        let list_output = list_resp.unwrap();
        let contents = list_output.contents();
        assert!(
            contents.iter().any(|o| o.key() == Some("key1")),
            "list should contain key1"
        );

        // Verify alt_client can GET object on public-read bucket
        let get_resp = alt_client
            .get_object()
            .bucket(&bucket)
            .key("key1")
            .send()
            .await
            .unwrap();
        let data = get_resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"abcde");

        // Set IgnorePublicAcls = true
        let pab = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .ignore_public_acls(true)
            .build();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();

        // Re-apply public-read ACL (matching Ceph test: ACL still set, but ignored)
        let acl_url = format!("{}/{}?acl", CTX.endpoint(), bucket);
        let resp = send_signed_put(&acl_url, b"", &[("x-amz-acl", "public-read")]);
        assert_eq!(resp, 200, "PutBucketAcl should succeed (IgnorePublicAcls doesn't block setting)");

        // alt_client list_objects should now fail (public ACL is ignored)
        let list_err = alt_client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await;
        assert!(
            list_err.is_err(),
            "expected alt_client list_objects to fail when IgnorePublicAcls is set"
        );

        // alt_client GET object should also fail
        let get_err = alt_client
            .get_object()
            .bucket(&bucket)
            .key("key1")
            .send()
            .await;
        assert!(
            get_err.is_err(),
            "expected alt_client get_object to fail when IgnorePublicAcls is set"
        );

        // Authenticated owner access should still work
        let info = client.head_bucket().bucket(&bucket).send().await;
        assert!(info.is_ok(), "authenticated head_bucket should still work");

        let get_resp = client
            .get_object()
            .bucket(&bucket)
            .key("key1")
            .send()
            .await
            .unwrap();
        let body = get_resp
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes();
        assert_eq!(&body[..], b"abcde", "authenticated owner should still read object");

        // Cleanup
        client
            .delete_object()
            .bucket(&bucket)
            .key("key1")
            .send()
            .await
            .unwrap();
        cleanup(&bucket).await;
    });
}

// ── test_get_public_access_block_requires_owner ──────────────────────

#[test]
fn test_get_public_access_block_requires_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        // Create a public-read bucket
        client
            .create_bucket()
            .bucket(&bucket)
            .acl(BucketCannedAcl::PublicRead)
            .send()
            .await
            .unwrap();

        // Put a PAB config so there's something to GET
        let pab = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .block_public_acls(true)
            .build();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();

        // Anonymous GET of public access block should fail with 403
        let url = format!("{}/{}?publicAccessBlock", CTX.endpoint(), bucket);
        let mut resp = agent().get(&url).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(
            status, 403,
            "anonymous GetBucketPublicAccessBlock on public bucket should be 403, got {}",
            status
        );

        // Authenticated owner GET should succeed
        let resp = client
            .get_public_access_block()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.public_access_block_configuration()
                .unwrap()
                .block_public_acls(),
            Some(true)
        );

        cleanup(&bucket).await;
    });
}

// ── Ignored tests (not yet implemented) ──────────────────────────────

#[test]
#[ignore = "not implemented: per-object ACLs"]
fn test_block_public_object_canned_acls() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: bucket policies"]
fn test_block_public_policy() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: bucket policies"]
fn test_block_public_policy_with_principal() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: bucket policies"]
fn test_block_public_restrict_public_buckets() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: bucket policies"]
fn test_get_public_block_deny_bucket_policy() {
    s3_tests::run(async {});
}

// ── Helper: send a signed PUT request via raw HTTP ───────────────────

fn send_signed_put(url_str: &str, body: &[u8], extra_headers: &[(&str, &str)]) -> u16 {
    use std::time::SystemTime;

    let a = agent();

    let parsed = url::Url::parse(url_str).expect("parse URL");
    let path = parsed.path();
    let raw_query = parsed.query().unwrap_or("");
    // Normalize query parameters: bare keys like "acl" become "acl="
    let query = normalize_query(raw_query);

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let secs = now.as_secs();
    let dt = format_amz_date(secs);
    let date_stamp = &dt[..8];

    let access_key = CTX.access_key();
    let secret_key = CTX.secret_key();
    let region = CTX.region();
    let service = "s3";

    let host = parsed
        .host_str()
        .map(|h| {
            if let Some(port) = parsed.port() {
                format!("{h}:{port}")
            } else {
                h.to_string()
            }
        })
        .unwrap();

    let payload_hash = sha256_hex(body);

    let mut header_map: Vec<(String, String)> = vec![
        ("host".to_string(), host.clone()),
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-date".to_string(), dt.clone()),
    ];
    for &(k, v) in extra_headers {
        header_map.push((k.to_lowercase(), v.to_string()));
    }
    header_map.sort_by(|a, b| a.0.cmp(&b.0));

    let signed_headers: String = header_map
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");

    let canonical_headers: String = header_map
        .iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
        .collect();

    let canonical_request =
        format!("PUT\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}");

    let cr_hash = sha256_hex(canonical_request.as_bytes());
    let scope = format!("{date_stamp}/{region}/{service}/aws4_request");
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{dt}\n{scope}\n{cr_hash}");

    let k_date = hmac_sha256(
        format!("AWS4{secret_key}").as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");

    let signature = hex_encode(&hmac_sha256(&k_signing, string_to_sign.as_bytes()));

    let auth_header = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );

    let mut request = a
        .put(url_str)
        .header("Authorization", &auth_header)
        .header("x-amz-date", &dt)
        .header("x-amz-content-sha256", &payload_hash);

    for (k, v) in extra_headers {
        request = request.header(*k, *v);
    }

    let resp = request.send(body).expect("transport error");
    resp.status().as_u16()
}

fn sha256_hex(data: &[u8]) -> String {
    use ring::digest;
    let d = digest::digest(&digest::SHA256, data);
    hex_encode(d.as_ref())
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    use ring::hmac;
    let k = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&k, data).as_ref().to_vec()
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn format_amz_date(epoch_secs: u64) -> String {
    let secs = epoch_secs;
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;
    let (year, month, day) = days_to_date(days_since_epoch as i64);
    format!("{year:04}{month:02}{day:02}T{hours:02}{minutes:02}{seconds:02}Z")
}

/// Normalize a raw query string for SigV4 canonical request.
/// Bare keys like "acl" become "acl=", and parameters are sorted.
fn normalize_query(raw: &str) -> String {
    if raw.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = raw
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next().unwrap_or("").to_string();
            let val = parts.next().unwrap_or("").to_string();
            (key, val)
        })
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn days_to_date(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}
