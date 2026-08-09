// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use s3_tests::{
    content_md5_header, raw_bucket, send_signed_request,
    shape::{assert_shape, error_response_headers, expected_error, id_headers, shape},
    unique_bucket, CTX,
};

/// Cleanup helper.
async fn cleanup(bucket: &str) {
    let client = CTX.client();
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
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

// ── GetPublicAccessBlock response shape ─────────────────────────────

#[test]
fn test_get_public_access_block_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let config = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .block_public_acls(true)
            .ignore_public_acls(true)
            .block_public_policy(true)
            .restrict_public_buckets(false)
            .build();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(config)
            .send()
            .await
            .expect("put public access block");

        let response = raw_bucket("GET", &bucket, Some("publicAccessBlock="));
        assert_shape(
            "GetPublicAccessBlock",
            &response,
            &shape().status(200).headers(id_headers()).body(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <PublicAccessBlockConfiguration \
                     xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                     <BlockPublicAcls>true</BlockPublicAcls>\
                     <IgnorePublicAcls>true</IgnorePublicAcls>\
                     <BlockPublicPolicy>true</BlockPublicPolicy>\
                     <RestrictPublicBuckets>false</RestrictPublicBuckets>\
                     </PublicAccessBlockConfiguration>",
            ),
        );

        cleanup(&bucket).await;
    });
}

/// Full response shapes for the PublicAccessBlock CRUD cycle: 200 empty ack
/// for Put, 204 for Delete, and the 404
/// `NoSuchPublicAccessBlockConfiguration` body when no configuration exists.
#[test]
fn test_public_access_block_crud_response_shapes() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        s3_tests::create_bucket(CTX.client(), &bucket)
            .await
            .unwrap();

        let pab_xml = "<PublicAccessBlockConfiguration \
             xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <BlockPublicAcls>true</BlockPublicAcls>\
             <IgnorePublicAcls>true</IgnorePublicAcls>\
             <BlockPublicPolicy>true</BlockPublicPolicy>\
             <RestrictPublicBuckets>true</RestrictPublicBuckets>\
             </PublicAccessBlockConfiguration>";
        let md5 = content_md5_header(pab_xml.as_bytes());
        let put = send_signed_request(
            "PUT",
            &format!("{}/{}?publicAccessBlock=", CTX.endpoint(), bucket),
            pab_xml.as_bytes(),
            [(md5.0.as_str(), md5.1.as_str())],
        );
        assert_shape(
            "PutPublicAccessBlock",
            &put,
            &shape().status(200).headers(id_headers()).body_empty(),
        );

        assert_shape(
            "DeletePublicAccessBlock",
            &raw_bucket("DELETE", &bucket, Some("publicAccessBlock=")),
            &shape().status(204).headers(id_headers()).body_empty(),
        );

        assert_shape(
            "GetPublicAccessBlock missing configuration",
            &raw_bucket("GET", &bucket, Some("publicAccessBlock=")),
            &shape()
                .status(404)
                .headers(error_response_headers())
                .body(expected_error::no_such_public_access_block(&bucket)),
        );

        cleanup(&bucket).await;
    });
}
