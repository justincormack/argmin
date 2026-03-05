use aws_sdk_s3::types::ObjectOwnership;
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

// ── test_create_bucket_no_ownership_controls ────────────────────────

/// A fresh bucket (no ownership header) should default to BucketOwnerEnforced.
#[test]
fn test_create_bucket_no_ownership_controls() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        // GET ownership controls should return BucketOwnerEnforced (AWS default)
        let resp = client
            .get_bucket_ownership_controls()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let rules = resp.ownership_controls().unwrap().rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(
            rules[0].object_ownership,
            ObjectOwnership::BucketOwnerEnforced
        );

        cleanup(&bucket).await;
    });
}

// ── test_bucket_create_delete_bucket_ownership ──────────────────────

/// Full PUT/GET/DELETE lifecycle for ownership controls.
#[test]
fn test_bucket_create_delete_bucket_ownership() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client.create_bucket().bucket(&bucket).send().await.unwrap();

        // PUT ownership controls
        let rule = aws_sdk_s3::types::OwnershipControlsRule::builder()
            .object_ownership(ObjectOwnership::BucketOwnerEnforced)
            .build()
            .unwrap();
        let controls = aws_sdk_s3::types::OwnershipControls::builder()
            .rules(rule)
            .build()
            .unwrap();
        client
            .put_bucket_ownership_controls()
            .bucket(&bucket)
            .ownership_controls(controls)
            .send()
            .await
            .unwrap();

        // GET should return BucketOwnerEnforced
        let resp = client
            .get_bucket_ownership_controls()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let rules = resp.ownership_controls().unwrap().rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(
            rules[0].object_ownership,
            ObjectOwnership::BucketOwnerEnforced
        );

        // DELETE
        client
            .delete_bucket_ownership_controls()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        // GET after delete should fail
        let err = client
            .get_bucket_ownership_controls()
            .bucket(&bucket)
            .send()
            .await
            .unwrap_err();
        let raw = format!("{:?}", err);
        assert!(
            raw.contains("OwnershipControlsNotFoundError") || raw.contains("404"),
            "expected OwnershipControlsNotFoundError after delete, got: {}",
            raw
        );

        // Second delete should be idempotent (matches Ceph behavior)
        client
            .delete_bucket_ownership_controls()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        cleanup(&bucket).await;
    });
}

// ── test_create_bucket_bucket_owner_enforced ────────────────────────

/// Create a bucket with BucketOwnerEnforced, verify GET returns it, then
/// exercise the BOE behavior matrix: put/copy with ACL blocked, without ACL
/// allowed, bucket-owner-full-control allowed, PutBucketAcl blocked.
/// Mirrors Ceph _test_object_ownership_bucket_owner_enforced.
#[test]
fn test_create_bucket_bucket_owner_enforced() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client
            .create_bucket()
            .bucket(&bucket)
            .object_ownership(ObjectOwnership::BucketOwnerEnforced)
            .send()
            .await
            .unwrap();

        // GET should return BucketOwnerEnforced
        let resp = client
            .get_bucket_ownership_controls()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let rules = resp.ownership_controls().unwrap().rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(
            rules[0].object_ownership,
            ObjectOwnership::BucketOwnerEnforced
        );

        // PutObject without ACL should succeed
        client
            .put_object()
            .bucket(&bucket)
            .key("put-object-no-acl")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // PutObject with bucket-owner-full-control should succeed
        let obj_url = format!("{}/{}/put-object-bofc", CTX.endpoint(), bucket);
        let status = send_signed_put(
            &obj_url,
            b"data",
            &[("x-amz-acl", "bucket-owner-full-control")],
        );
        assert_eq!(
            status, 200,
            "PutObject with bucket-owner-full-control should succeed, got {status}"
        );

        // PutObject with ACL=private should succeed (private is compatible with BOE)
        let obj_url2 = format!("{}/{}/put-object-private", CTX.endpoint(), bucket);
        let status = send_signed_put(&obj_url2, b"data", &[("x-amz-acl", "private")]);
        assert_eq!(
            status, 200,
            "PutObject with ACL=private should succeed under BOE, got {status}"
        );

        // PutObject with ACL=public-read should fail
        let obj_url3 = format!("{}/{}/put-object-public", CTX.endpoint(), bucket);
        let status = send_signed_put(&obj_url3, b"data", &[("x-amz-acl", "public-read")]);
        assert_eq!(
            status, 400,
            "PutObject with ACL=public-read should fail under BOE, got {status}"
        );

        // CopyObject without ACL should succeed
        client
            .copy_object()
            .bucket(&bucket)
            .key("copy-object-no-acl")
            .copy_source(format!("{}/put-object-no-acl", bucket))
            .send()
            .await
            .unwrap();

        // CopyObject with ACL=private should succeed (private is compatible with BOE)
        let copy_url = format!("{}/{}/copy-object-private", CTX.endpoint(), bucket);
        let status = send_signed_put(
            &copy_url,
            b"",
            &[
                ("x-amz-acl", "private"),
                (
                    "x-amz-copy-source",
                    &format!("{}/put-object-no-acl", bucket),
                ),
            ],
        );
        assert_eq!(
            status, 200,
            "CopyObject with ACL=private should succeed under BOE, got {status}"
        );

        // CopyObject with ACL=public-read should fail
        let copy_url2 = format!("{}/{}/copy-object-public", CTX.endpoint(), bucket);
        let status = send_signed_put(
            &copy_url2,
            b"",
            &[
                ("x-amz-acl", "public-read"),
                (
                    "x-amz-copy-source",
                    &format!("{}/put-object-no-acl", bucket),
                ),
            ],
        );
        assert_eq!(
            status, 400,
            "CopyObject with ACL=public-read should fail under BOE, got {status}"
        );

        // PutBucketAcl private should fail (all PutBucketAcl rejected under BOE)
        let acl_url = format!("{}/{}?acl", CTX.endpoint(), bucket);
        let status = send_signed_put(&acl_url, b"", &[("x-amz-acl", "private")]);
        assert_eq!(
            status, 400,
            "PutBucketAcl private should fail under BOE, got {status}"
        );

        // Cleanup objects
        for key in [
            "put-object-no-acl",
            "put-object-bofc",
            "put-object-private",
            "copy-object-no-acl",
            "copy-object-private",
        ] {
            client
                .delete_object()
                .bucket(&bucket)
                .key(key)
                .send()
                .await
                .unwrap();
        }
        cleanup(&bucket).await;
    });
}

// ── test_put_bucket_ownership_enforced_rejects_public_acl ───────────

/// PUT ownership controls on a public-read bucket should fail with
/// InvalidBucketAclWithObjectOwnership. Setting ACL to private first, then
/// setting ownership to BucketOwnerEnforced should succeed.
#[test]
fn test_put_bucket_ownership_enforced_rejects_public_acl() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = s3_tests::create_public_bucket(client).await;

        // PUT BucketOwnerEnforced should fail — bucket is public-read
        let oc_url = format!("{}/{}?ownershipControls", CTX.endpoint(), bucket);
        let body = b"<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>";
        let status = send_signed_put(&oc_url, body, &[]);
        assert_eq!(
            status, 400,
            "expected 400 for BucketOwnerEnforced on public bucket, got {}",
            status
        );

        // Set ACL to private
        let acl_url = format!("{}/{}?acl", CTX.endpoint(), bucket);
        let status = send_signed_put(&acl_url, b"", &[("x-amz-acl", "private")]);
        assert_eq!(status, 200, "set private ACL failed: {}", status);

        // PUT BucketOwnerEnforced should now succeed
        let status = send_signed_put(&oc_url, body, &[]);
        assert_eq!(
            status, 200,
            "expected 200 for BucketOwnerEnforced on private bucket, got {}",
            status
        );

        cleanup(&bucket).await;
    });
}

// ── test_bucket_owner_enforced_rejects_object_acl ───────────────────

/// PutObject with x-amz-acl on a BucketOwnerEnforced bucket should fail
/// with AccessControlListNotSupported. PutObject without ACL should succeed.
#[test]
fn test_bucket_owner_enforced_rejects_object_acl() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client
            .create_bucket()
            .bucket(&bucket)
            .object_ownership(ObjectOwnership::BucketOwnerEnforced)
            .send()
            .await
            .unwrap();

        // PutObject with x-amz-acl: public-read should fail
        let obj_url = format!("{}/{}/testkey", CTX.endpoint(), bucket);
        let status = send_signed_put(&obj_url, b"hello", &[("x-amz-acl", "public-read")]);
        assert_eq!(
            status, 400,
            "expected 400 for PutObject with ACL on BucketOwnerEnforced, got {}",
            status
        );

        // PutObject without ACL should succeed
        client
            .put_object()
            .bucket(&bucket)
            .key("testkey")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        // Cleanup
        client
            .delete_object()
            .bucket(&bucket)
            .key("testkey")
            .send()
            .await
            .unwrap();
        cleanup(&bucket).await;
    });
}

// ── test_bucket_owner_enforced_allows_bucket_owner_full_control ─────

/// PutObject with x-amz-acl: bucket-owner-full-control on a
/// BucketOwnerEnforced bucket should succeed.
#[test]
fn test_bucket_owner_enforced_allows_bucket_owner_full_control() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        client
            .create_bucket()
            .bucket(&bucket)
            .object_ownership(ObjectOwnership::BucketOwnerEnforced)
            .send()
            .await
            .unwrap();

        // PutObject with bucket-owner-full-control should succeed
        let obj_url = format!("{}/{}/testkey2", CTX.endpoint(), bucket);
        let status = send_signed_put(
            &obj_url,
            b"data",
            &[("x-amz-acl", "bucket-owner-full-control")],
        );
        assert_eq!(
            status, 200,
            "expected 200 for PutObject with bucket-owner-full-control, got {}",
            status
        );

        // Cleanup
        client
            .delete_object()
            .bucket(&bucket)
            .key("testkey2")
            .send()
            .await
            .unwrap();
        cleanup(&bucket).await;
    });
}

// ── test_put_bucket_ownership_bucket_owner_enforced ──────────────────

/// Mirrors Ceph test_put_bucket_ownership_bucket_owner_enforced:
/// 1. Create bucket with public-read ACL
/// 2. PutBucketOwnershipControls BOE fails (InvalidBucketAclWithObjectOwnership)
/// 3. Set ACL to private
/// 4. PutBucketOwnershipControls BOE succeeds
/// 5. Verify BOE behavior: PutObject/CopyObject ACL blocked, PutBucketAcl blocked
#[test]
fn test_put_bucket_ownership_bucket_owner_enforced() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = s3_tests::create_public_bucket(client).await;

        // PutBucketOwnershipControls BOE should fail — bucket is public-read
        let oc_url = format!("{}/{}?ownershipControls", CTX.endpoint(), bucket);
        let oc_body = b"<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>";
        let status = send_signed_put(&oc_url, oc_body, &[]);
        assert_eq!(
            status, 400,
            "BOE on public-read bucket should fail, got {status}"
        );

        // Set ACL to private
        let acl_url = format!("{}/{}?acl", CTX.endpoint(), bucket);
        let status = send_signed_put(&acl_url, b"", &[("x-amz-acl", "private")]);
        assert_eq!(status, 200, "set private ACL failed: {status}");

        // PutBucketOwnershipControls BOE should now succeed
        let status = send_signed_put(&oc_url, oc_body, &[]);
        assert_eq!(
            status, 200,
            "BOE on private bucket should succeed, got {status}"
        );

        // --- Verify BOE behavior matrix ---

        // PutObject without ACL should succeed
        client
            .put_object()
            .bucket(&bucket)
            .key("put-object-no-acl")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // PutObject with ACL=private should succeed (private is compatible with BOE)
        let obj_url = format!("{}/{}/put-object-private", CTX.endpoint(), bucket);
        let status = send_signed_put(&obj_url, b"data", &[("x-amz-acl", "private")]);
        assert_eq!(
            status, 200,
            "PutObject with ACL=private should succeed under BOE, got {status}"
        );

        // PutObject with ACL=public-read should fail
        let obj_url2 = format!("{}/{}/put-object-public", CTX.endpoint(), bucket);
        let status = send_signed_put(&obj_url2, b"data", &[("x-amz-acl", "public-read")]);
        assert_eq!(
            status, 400,
            "PutObject with ACL=public-read should fail under BOE, got {status}"
        );

        // CopyObject with ACL=private should succeed (private is compatible with BOE)
        let copy_url = format!("{}/{}/copy-object-private", CTX.endpoint(), bucket);
        let status = send_signed_put(
            &copy_url,
            b"",
            &[
                ("x-amz-acl", "private"),
                (
                    "x-amz-copy-source",
                    &format!("{}/put-object-no-acl", bucket),
                ),
            ],
        );
        assert_eq!(
            status, 200,
            "CopyObject with ACL=private should succeed under BOE, got {status}"
        );

        // PutBucketAcl private should fail (all PutBucketAcl rejected under BOE)
        let status = send_signed_put(&acl_url, b"", &[("x-amz-acl", "private")]);
        assert_eq!(
            status, 400,
            "PutBucketAcl private should fail under BOE, got {status}"
        );

        // Cleanup
        for key in [
            "put-object-no-acl",
            "put-object-private",
            "copy-object-private",
        ] {
            client
                .delete_object()
                .bucket(&bucket)
                .key(key)
                .send()
                .await
                .unwrap();
        }
        cleanup(&bucket).await;
    });
}

// ── Ignored tests (need bucket policies or get_object_acl) ──────────

#[test]
#[ignore = "not implemented: bucket policies"]
fn test_create_bucket_bucket_owner_preferred() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: bucket policies"]
fn test_create_bucket_object_writer() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: bucket policies"]
fn test_put_bucket_ownership_bucket_owner_preferred() {
    s3_tests::run(async {});
}

#[test]
#[ignore = "not implemented: bucket policies"]
fn test_put_bucket_ownership_object_writer() {
    s3_tests::run(async {});
}

// ── Helper: send a signed PUT request via raw HTTP ───────────────────

fn send_signed_put(url_str: &str, body: &[u8], extra_headers: &[(&str, &str)]) -> u16 {
    use std::time::SystemTime;

    let a = agent();

    let parsed = url::Url::parse(url_str).expect("parse URL");
    let path = parsed.path();
    let raw_query = parsed.query().unwrap_or("");
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
