use s3_tests::{content_md5_header, send_signed_request, unique_bucket, CTX};

/// Cleanup helper.
async fn cleanup(bucket: &str) {
    let client = CTX.client();
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

#[test]
fn test_public_access_block_raw_get_returns_canonical_xml() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        s3_tests::create_bucket(CTX.client(), &bucket)
            .await
            .unwrap();

        let body = br#"
            <PublicAccessBlockConfiguration>
                <RestrictPublicBuckets>true</RestrictPublicBuckets>
                <BlockPublicPolicy>true</BlockPublicPolicy>
                <IgnorePublicAcls>true</IgnorePublicAcls>
                <BlockPublicAcls>true</BlockPublicAcls>
            </PublicAccessBlockConfiguration>
        "#;

        let parsed = server_http::http::xml::parse_public_access_block_xml(body).unwrap();
        let expected = server_http::http::xml::get_public_access_block_xml(&parsed);

        let url = format!("{}/{}?publicAccessBlock", CTX.endpoint(), bucket);
        let put = send_signed_request("PUT", &url, body, [content_md5_header(body)]);
        assert_eq!(put.status, 200, "unexpected body: {}", put.body);

        let get = send_signed_request("GET", &url, b"", std::iter::empty::<(String, String)>());

        cleanup(&bucket).await;

        assert_eq!(get.status, 200, "unexpected body: {}", get.body);
        assert_eq!(get.body, expected);
    });
}

// ── test_put_public_block ─────────────────────────────────────────────

#[test]
fn test_put_public_block() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

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
        s3_tests::create_bucket(client, &bucket).await.unwrap();

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
        s3_tests::create_bucket(client, &bucket).await.unwrap();

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
