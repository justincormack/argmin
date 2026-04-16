//! Multipart upload integration tests.
//!
//! Tests the full multipart upload lifecycle through the S3 HTTP API:
//! CreateMultipartUpload, UploadPart, CompleteMultipartUpload,
//! AbortMultipartUpload, ListMultipartUploads, ListParts.

use std::collections::BTreeMap;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    CompletedMultipartUpload, CompletedPart, EncodingType, ObjectCannedAcl, ObjectOwnership,
    Permission, PublicAccessBlockConfiguration,
};
use s3_tests::{
    assert_s3_err_code, copy_source_with_version, create_public_write_bucket, err_status,
    object_url, send_signed_request, send_signed_request_with_credentials, unique_bucket,
    RawResponse, SignedRequestCredentials, CTX,
};

const PART_SIZE: usize = 5 * 1024 * 1024; // 5 MB minimum part size

fn owner_root_client() -> &'static aws_sdk_s3::Client {
    CTX.require_owner_root_client()
}

fn external_test_mode() -> bool {
    std::env::var_os("S3_TEST_ENDPOINT").is_some()
}

fn primary_credentials() -> SignedRequestCredentials<'static> {
    SignedRequestCredentials {
        access_key: CTX.access_key(),
        secret_key: CTX.secret_key(),
        region: CTX.region(),
        tls_ca_pem: CTX.tls_ca_pem(),
    }
}

fn alt_credentials() -> SignedRequestCredentials<'static> {
    SignedRequestCredentials {
        access_key: CTX.alt_access_key(),
        secret_key: CTX.alt_secret_key(),
        region: CTX.region(),
        tls_ca_pem: CTX.tls_ca_pem(),
    }
}

fn complete_multipart_upload_xml(etag: &str, part_number: u32) -> Vec<u8> {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<CompleteMultipartUpload>\
<Part><PartNumber>{part_number}</PartNumber><ETag>{etag}</ETag></Part>\
</CompleteMultipartUpload>"
    )
    .into_bytes()
}

fn assert_invalid_upload_id_no_such_upload(response: &RawResponse, invalid_upload_id: &str) {
    assert_eq!(
        response.status, 404,
        "unexpected response body: {}",
        response.body
    );
    assert!(
        response.body.contains("<Code>NoSuchUpload</Code>"),
        "unexpected response body: {}",
        response.body
    );
    assert!(
        response.body.contains(
            "<Message>The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.</Message>"
        ),
        "unexpected response body: {}",
        response.body
    );
    assert!(
        response
            .body
            .contains(&format!("<UploadId>{invalid_upload_id}</UploadId>")),
        "unexpected response body: {}",
        response.body
    );
}

async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
}

async fn set_object_writer_ownership(bucket: &str) {
    let rule = aws_sdk_s3::types::OwnershipControlsRule::builder()
        .object_ownership(ObjectOwnership::ObjectWriter)
        .build()
        .unwrap();
    let controls = aws_sdk_s3::types::OwnershipControls::builder()
        .rules(rule)
        .build()
        .unwrap();
    CTX.client()
        .put_bucket_ownership_controls()
        .bucket(bucket)
        .ownership_controls(controls)
        .send()
        .await
        .unwrap();
}

async fn disable_bucket_public_access_block(bucket: &str) {
    let config = PublicAccessBlockConfiguration::builder()
        .block_public_acls(false)
        .ignore_public_acls(false)
        .block_public_policy(false)
        .restrict_public_buckets(false)
        .build();
    CTX.client()
        .put_public_access_block()
        .bucket(bucket)
        .public_access_block_configuration(config)
        .send()
        .await
        .unwrap();
}

async fn canonical_owner_id(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    let owner_id = client
        .get_bucket_acl()
        .bucket(&bucket)
        .send()
        .await
        .unwrap()
        .owner()
        .expect("expected owner in GetBucketAcl")
        .id()
        .expect("expected owner ID in GetBucketAcl")
        .to_string();
    client.delete_bucket().bucket(&bucket).send().await.unwrap();
    owner_id
}

async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }

    // AWS can keep failed or recently aborted multipart uploads visible briefly,
    // causing DeleteBucket to return OperationAborted or BucketNotEmpty.
    for _ in 0..10 {
        let uploads = client
            .list_multipart_uploads()
            .bucket(bucket)
            .send()
            .await
            .unwrap();
        for upload in uploads.uploads() {
            let _ = client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(upload.key().unwrap())
                .upload_id(upload.upload_id().unwrap())
                .send()
                .await;
        }

        match client.delete_bucket().bucket(bucket).send().await {
            Ok(_) => return,
            Err(err) => {
                let raw = format!("{err:?}");
                if raw.contains("OperationAborted") || raw.contains("BucketNotEmpty") {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    continue;
                }
                panic!("delete_bucket failed unexpectedly: {raw}");
            }
        }
    }

    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

fn expected_raw_list_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\u{0001}'..='\u{0008}' | '\u{000B}' | '\u{000C}' | '\u{000E}'..='\u{001F}' => {
                escaped.push_str(&format!("&#x{:x};", ch as u32));
            }
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn expected_url_list_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(byte as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn query_encode_value(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

fn anon_agent() -> s3_tests::Agent {
    s3_tests::test_agent()
}

/// Helper: create multipart upload, upload parts, complete, return (etag, version_id).
async fn do_multipart_upload(bucket: &str, key: &str, parts_data: &[Vec<u8>]) -> String {
    do_multipart_upload_with_acl(bucket, key, parts_data, None).await
}

async fn do_multipart_upload_with_acl(
    bucket: &str,
    key: &str,
    parts_data: &[Vec<u8>],
    acl: Option<ObjectCannedAcl>,
) -> String {
    let client = CTX.client();

    let mut create_req = client.create_multipart_upload().bucket(bucket).key(key);
    if let Some(acl) = acl {
        create_req = create_req.acl(acl);
    }
    let create = create_req.send().await.unwrap();
    let upload_id = create.upload_id().unwrap();

    let mut completed_parts = Vec::new();
    for (i, data) in parts_data.iter().enumerate() {
        let part_number = (i + 1) as i32;
        let resp = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(data.clone()))
            .send()
            .await
            .unwrap();
        completed_parts.push(
            CompletedPart::builder()
                .e_tag(resp.e_tag().unwrap())
                .part_number(part_number)
                .build(),
        );
    }

    let complete = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build(),
        )
        .send()
        .await
        .unwrap();
    complete.e_tag().unwrap().to_string()
}

async fn complete_single_part_multipart_upload(bucket: &str, key: &str, body: &[u8]) -> String {
    complete_single_part_multipart_upload_with_client_and_acl(CTX.client(), bucket, key, body, None)
        .await
}

async fn complete_single_part_multipart_upload_with_client_and_acl(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: &[u8],
    acl: Option<ObjectCannedAcl>,
) -> String {
    let mut create_req = client.create_multipart_upload().bucket(bucket).key(key);
    if let Some(acl) = acl {
        create_req = create_req.acl(acl);
    }
    let create = create_req.send().await.unwrap();
    let upload_id = create.upload_id().unwrap().to_string();

    let part = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(body.to_vec()))
        .send()
        .await
        .unwrap();

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .e_tag(part.e_tag().unwrap())
                        .part_number(1)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    upload_id
}

fn has_grant(grants: &[aws_sdk_s3::types::Grant], permission: Permission, uri: &str) -> bool {
    grants.iter().any(|grant| {
        grant.permission() == Some(&permission)
            && grant.grantee().and_then(|grantee| grantee.uri()) == Some(uri)
    })
}

fn has_canonical_grant(
    grants: &[aws_sdk_s3::types::Grant],
    permission: Permission,
    canonical_user_id: &str,
) -> bool {
    grants.iter().any(|grant| {
        grant.permission() == Some(&permission)
            && grant.grantee().and_then(|grantee| grantee.id()) == Some(canonical_user_id)
    })
}

#[test]
fn test_create_multipart_upload_rejects_system_metadata_over_limit() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let result = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("multipart-system-metadata-too-large")
            .content_disposition("d".repeat(3000))
            .send()
            .await;

        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "MetadataTooLarge");

        cleanup(&bucket, &[]).await;
    });
}

// ── Basic lifecycle ─────────────────────────────────────────────────

#[test]
fn test_multipart_upload_basic() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-basic";

        let part1 = vec![b'a'; PART_SIZE];
        let part2 = vec![b'b'; 1024]; // last part can be < 5MB

        let etag = do_multipart_upload(&bucket, key, &[part1.clone(), part2.clone()]).await;
        assert!(!etag.is_empty());

        // Verify the object is readable and has correct content
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(data.len(), PART_SIZE + 1024);
        assert!(data[..PART_SIZE].iter().all(|&b| b == b'a'));
        assert!(data[PART_SIZE..].iter().all(|&b| b == b'b'));

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_multipart_upload_single_part() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-single";

        // Single part (last part exempt from min size)
        let part = vec![b'x'; 256];
        do_multipart_upload(&bucket, key, std::slice::from_ref(&part)).await;

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], &part[..]);

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_multipart_upload_canned_acl_persists_to_completed_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-public-read";
        let body = vec![b'x'; 1024];

        set_object_writer_ownership(&bucket).await;
        disable_bucket_public_access_block(&bucket).await;

        do_multipart_upload_with_acl(
            &bucket,
            key,
            std::slice::from_ref(&body),
            Some(ObjectCannedAcl::PublicRead),
        )
        .await;

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(
                acl.grants(),
                Permission::Read,
                "http://acs.amazonaws.com/groups/global/AllUsers",
            ),
            "expected READ grant for AllUsers, got {:?}",
            acl.grants()
        );

        let get_url = format!("{}/{bucket}/{key}", CTX.endpoint());
        let mut resp = anon_agent().get(&get_url).call().expect("transport error");
        assert_eq!(
            resp.status().as_u16(),
            200,
            "expected anonymous GET for multipart public-read object"
        );
        let data = resp.body_mut().read_to_vec().unwrap();
        assert_eq!(&data[..], body.as_slice());

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_create_multipart_upload_grant_write_header_persists_write_grant() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-grant-write";
        let body = vec![b'x'; 1024];

        set_object_writer_ownership(&bucket).await;
        disable_bucket_public_access_block(&bucket).await;

        let owner_id = canonical_owner_id(client).await;
        let grant_write_owner_id = owner_id.clone();
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .customize()
            .mutate_request(move |req| {
                req.headers_mut().insert(
                    "x-amz-grant-write",
                    format!("id=\"{grant_write_owner_id}\""),
                );
            })
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let part = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(body))
            .send()
            .await
            .unwrap();

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(part.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert!(
            has_canonical_grant(acl.grants(), Permission::Write, &owner_id),
            "expected WRITE grant for object owner, got {:?}",
            acl.grants()
        );

        cleanup(&bucket, &[key]).await;
    });
}

// ── Abort ───────────────────────────────────────────────────────────

#[test]
fn test_multipart_upload_abort() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-abort";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload a part
        client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![0u8; 1024]))
            .send()
            .await
            .unwrap();

        // Abort the upload
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();

        // The object should not exist
        let result = client.get_object().bucket(&bucket).key(key).send().await;
        assert!(result.is_err());

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_abort_multipart_upload_after_complete_succeeds() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-abort-after-complete";
        let upload_id = complete_single_part_multipart_upload(&bucket, key, b"hello, world!").await;

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"hello, world!");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_abort_multipart_upload_after_complete_wrong_upload_id_fails() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-abort-after-complete-wrong-upload-id";

        let _upload_id =
            complete_single_part_multipart_upload(&bucket, key, b"hello, world!").await;

        let result = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id("definitely-wrong-upload-id")
            .send()
            .await;
        assert_s3_err_code(&result, "NoSuchUpload");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_abort_multipart_upload_invalid_present_upload_id_overlong_message_external() {
    s3_tests::run(async {
        if !external_test_mode() {
            return;
        }

        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-invalid-upload-id-message";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let valid_upload_id = create.upload_id().unwrap().to_string();
        let invalid_upload_id = "a".repeat(1025);
        let url = object_url(
            CTX.endpoint(),
            &bucket,
            key,
            Some(&format!("uploadId={invalid_upload_id}")),
        );

        let response = send_signed_request_with_credentials(
            "DELETE",
            &url,
            b"",
            std::iter::empty::<(&str, &str)>(),
            primary_credentials(),
        );
        assert_invalid_upload_id_no_such_upload(&response, &invalid_upload_id);

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&valid_upload_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_upload_part_invalid_present_upload_id_overlong_auth_precedence_external() {
    s3_tests::run(async {
        if !external_test_mode() {
            return;
        }

        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-invalid-upload-id-auth-precedence";

        client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();

        let invalid_upload_id = "a".repeat(1025);
        let url = object_url(
            CTX.endpoint(),
            &bucket,
            key,
            Some(&format!("partNumber=1&uploadId={invalid_upload_id}")),
        );

        let response = send_signed_request_with_credentials(
            "PUT",
            &url,
            b"x",
            std::iter::empty::<(&str, &str)>(),
            alt_credentials(),
        );
        assert_invalid_upload_id_no_such_upload(&response, &invalid_upload_id);

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_complete_multipart_upload_invalid_present_upload_id_overlong_message_external() {
    s3_tests::run(async {
        if !external_test_mode() {
            return;
        }

        let bucket = setup_bucket().await;
        let key = "multipart-complete-invalid-upload-id-message";
        let invalid_upload_id = "a".repeat(1025);
        let url = object_url(
            CTX.endpoint(),
            &bucket,
            key,
            Some(&format!("uploadId={invalid_upload_id}")),
        );
        let body = complete_multipart_upload_xml("\"abc\"", 1);

        let response = send_signed_request_with_credentials(
            "POST",
            &url,
            &body,
            [("content-type", "application/xml")],
            primary_credentials(),
        );
        assert_invalid_upload_id_no_such_upload(&response, &invalid_upload_id);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_complete_multipart_upload_invalid_present_upload_id_overlong_auth_precedence_external() {
    s3_tests::run(async {
        if !external_test_mode() {
            return;
        }

        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-complete-invalid-upload-id-auth-precedence";

        client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();

        let invalid_upload_id = "a".repeat(1025);
        let url = object_url(
            CTX.endpoint(),
            &bucket,
            key,
            Some(&format!("uploadId={invalid_upload_id}")),
        );
        let body = complete_multipart_upload_xml("\"abc\"", 1);

        let response = send_signed_request_with_credentials(
            "POST",
            &url,
            &body,
            [("content-type", "application/xml")],
            alt_credentials(),
        );
        assert_invalid_upload_id_no_such_upload(&response, &invalid_upload_id);

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_list_parts_invalid_present_upload_id_overlong_message_external() {
    s3_tests::run(async {
        if !external_test_mode() {
            return;
        }

        let bucket = setup_bucket().await;
        let key = "multipart-list-parts-invalid-upload-id-message";
        let invalid_upload_id = "a".repeat(1025);
        let url = object_url(
            CTX.endpoint(),
            &bucket,
            key,
            Some(&format!("uploadId={invalid_upload_id}")),
        );

        let response = send_signed_request_with_credentials(
            "GET",
            &url,
            b"",
            std::iter::empty::<(&str, &str)>(),
            primary_credentials(),
        );
        assert_invalid_upload_id_no_such_upload(&response, &invalid_upload_id);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_parts_invalid_present_upload_id_overlong_auth_precedence_external() {
    s3_tests::run(async {
        if !external_test_mode() {
            return;
        }

        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-list-parts-invalid-upload-id-auth-precedence";

        client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();

        let invalid_upload_id = "a".repeat(1025);
        let url = object_url(
            CTX.endpoint(),
            &bucket,
            key,
            Some(&format!("uploadId={invalid_upload_id}")),
        );

        let response = send_signed_request_with_credentials(
            "GET",
            &url,
            b"",
            std::iter::empty::<(&str, &str)>(),
            alt_credentials(),
        );
        assert_invalid_upload_id_no_such_upload(&response, &invalid_upload_id);

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_abort_multipart_upload_after_complete_and_delete_succeeds() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-abort-after-complete-delete";

        let upload_id = complete_single_part_multipart_upload(&bucket, key, b"first").await;

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        let get = client.get_object().bucket(&bucket).key(key).send().await;
        assert_s3_err_code(&get, "NoSuchKey");

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_abort_multipart_upload_after_complete_and_overwrite_succeeds() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-abort-after-complete-overwrite";

        let first_upload_id = complete_single_part_multipart_upload(&bucket, key, b"first").await;
        let second_upload_id = complete_single_part_multipart_upload(&bucket, key, b"second").await;

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&first_upload_id)
            .send()
            .await
            .unwrap();

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&second_upload_id)
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"second");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_abort_completed_multipart_upload_initiator_succeeds_when_owner_is_bucket_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_public_write_bucket(client).await;
        let key = "multipart-abort-after-complete-initiator-not-owner";

        let bucket_owner_id = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap()
            .owner()
            .expect("expected bucket owner in GetBucketAcl")
            .id()
            .expect("expected bucket owner ID in GetBucketAcl")
            .to_string();
        let alt_owner_id = canonical_owner_id(alt_client).await;

        let upload_id = complete_single_part_multipart_upload_with_client_and_acl(
            alt_client,
            &bucket,
            key,
            b"hello, world!",
            Some(ObjectCannedAcl::BucketOwnerFullControl),
        )
        .await;

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let object_owner_id = acl
            .owner()
            .expect("expected object owner in GetObjectAcl")
            .id()
            .expect("expected object owner ID in GetObjectAcl")
            .to_string();
        assert_eq!(object_owner_id, bucket_owner_id);
        assert_ne!(object_owner_id, alt_owner_id);

        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"hello, world!");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_abort_completed_multipart_upload_owner_root_admin_succeeds() {
    s3_tests::run(async {
        let client = CTX.client();
        let root_client = owner_root_client();
        let alt_client = CTX.alt_client();
        let bucket = create_public_write_bucket(client).await;
        let key = "multipart-abort-after-complete-owner-root";

        let upload_id = complete_single_part_multipart_upload_with_client_and_acl(
            alt_client,
            &bucket,
            key,
            b"hello, world!",
            None,
        )
        .await;

        root_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        let resp = alt_client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"hello, world!");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_abort_completed_multipart_upload_rejects_unrelated_cross_account_requester() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_public_write_bucket(client).await;
        let key = "multipart-abort-after-complete-unrelated-cross-account";

        let upload_id = complete_single_part_multipart_upload_with_client_and_acl(
            client,
            &bucket,
            key,
            b"hello, world!",
            None,
        )
        .await;

        let result = alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], b"hello, world!");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_abort_multipart_upload_after_bucket_delete_and_recreate_fails() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-abort-after-bucket-recreate";

        let upload_id = complete_single_part_multipart_upload(&bucket, key, b"first").await;

        client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
        s3_tests::create_bucket_retrying_reuse(client, &bucket)
            .await
            .unwrap();

        let result = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await;
        assert_s3_err_code(&result, "NoSuchUpload");

        cleanup(&bucket, &[]).await;
    });
}

// ── ListMultipartUploads ────────────────────────────────────────────

#[test]
fn test_list_multipart_uploads_empty() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(resp.uploads().is_empty());

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_active() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        // Create two uploads
        let create1 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("key1")
            .send()
            .await
            .unwrap();
        let uid1 = create1.upload_id().unwrap().to_string();

        let create2 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("key2")
            .send()
            .await
            .unwrap();
        let uid2 = create2.upload_id().unwrap().to_string();

        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let uploads = resp.uploads();
        assert_eq!(uploads.len(), 2);

        let keys: Vec<&str> = uploads.iter().map(|u| u.key().unwrap()).collect();
        assert!(keys.contains(&"key1"));
        assert!(keys.contains(&"key2"));

        // Abort both
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("key1")
            .upload_id(&uid1)
            .send()
            .await
            .unwrap();
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("key2")
            .upload_id(&uid2)
            .send()
            .await
            .unwrap();

        // Should be empty now
        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(resp.uploads().is_empty());

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_prefix() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let c1 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("photos/a.jpg")
            .send()
            .await
            .unwrap();
        let uid1 = c1.upload_id().unwrap().to_string();

        let c2 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("docs/b.txt")
            .send()
            .await
            .unwrap();
        let uid2 = c2.upload_id().unwrap().to_string();

        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .prefix("photos/")
            .send()
            .await
            .unwrap();
        let uploads = resp.uploads();
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].key().unwrap(), "photos/a.jpg");

        // Cleanup
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("photos/a.jpg")
            .upload_id(&uid1)
            .send()
            .await
            .unwrap();
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("docs/b.txt")
            .upload_id(&uid2)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_pagination_and_markers() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let uploads = ["a&upload", "b<upload", "c\"upload"];
        let mut created = Vec::new();
        for key in uploads {
            let create = client
                .create_multipart_upload()
                .bucket(&bucket)
                .key(key)
                .send()
                .await
                .unwrap();
            created.push((key.to_string(), create.upload_id().unwrap().to_string()));
        }

        let resp1 = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .max_uploads(1)
            .send()
            .await
            .unwrap();
        assert_eq!(resp1.uploads().len(), 1);
        assert_eq!(resp1.uploads()[0].key().unwrap(), "a&upload");
        assert_eq!(resp1.is_truncated(), Some(true));
        let next_key_1 = resp1.next_key_marker().unwrap().to_string();
        let next_upload_1 = resp1.next_upload_id_marker().unwrap().to_string();

        let resp2 = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .key_marker(next_key_1)
            .upload_id_marker(next_upload_1)
            .max_uploads(1)
            .send()
            .await
            .unwrap();
        assert_eq!(resp2.uploads().len(), 1);
        assert_eq!(resp2.uploads()[0].key().unwrap(), "b<upload");
        assert_eq!(resp2.is_truncated(), Some(true));
        let next_key_2 = resp2.next_key_marker().unwrap().to_string();
        let next_upload_2 = resp2.next_upload_id_marker().unwrap().to_string();

        let resp3 = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .key_marker(next_key_2)
            .upload_id_marker(next_upload_2)
            .max_uploads(1)
            .send()
            .await
            .unwrap();
        assert_eq!(resp3.uploads().len(), 1);
        assert_eq!(resp3.uploads()[0].key().unwrap(), "c\"upload");
        assert_eq!(resp3.is_truncated(), Some(false));

        for (key, upload_id) in created {
            client
                .abort_multipart_upload()
                .bucket(&bucket)
                .key(key)
                .upload_id(upload_id)
                .send()
                .await
                .unwrap();
        }
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_invalid_present_upload_id_marker_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let mut created = Vec::new();
        for key in ["same-key", "same-key", "same-key", "zzz"] {
            let create = client
                .create_multipart_upload()
                .bucket(&bucket)
                .key(key)
                .send()
                .await
                .unwrap();
            created.push((key.to_string(), create.upload_id().unwrap().to_string()));
        }

        let invalid_marker = "x".repeat(1025);
        let url = format!(
            "{}/{bucket}?uploads&key-marker={}&upload-id-marker={}",
            CTX.endpoint(),
            query_encode_value("same-key"),
            query_encode_value(&invalid_marker),
        );
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 400, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<Code>InvalidArgument</Code>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response
                .body
                .contains("<Message>Invalid uploadId marker</Message>"),
            "unexpected body: {}",
            response.body
        );

        for (key, upload_id) in created {
            client
                .abort_multipart_upload()
                .bucket(&bucket)
                .key(key)
                .upload_id(upload_id)
                .send()
                .await
                .unwrap();
        }
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_encoding_type_url() {
    const KEY: &str = "multi part <>&\"+";

    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(KEY)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .encoding_type(EncodingType::Url)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.encoding_type(), Some(&EncodingType::Url));
        assert_eq!(resp.uploads().len(), 1);
        let encoded_key = expected_url_list_value(KEY);
        assert_eq!(resp.uploads()[0].key(), Some(encoded_key.as_str()));

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(KEY)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_raw_response_encodes_key_fields() {
    const KEY: &str = "multi<>&\"";
    const PREFIX: &str = "multi<>&\"";

    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(KEY)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();
        let url = format!(
            "{}/{bucket}?uploads&encoding-type=url&prefix={}",
            CTX.endpoint(),
            query_encode_value(PREFIX)
        );
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<EncodingType>url</EncodingType>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains(&format!(
                "<Prefix>{}</Prefix>",
                expected_url_list_value(PREFIX)
            )),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response
                .body
                .contains(&format!("<Key>{}</Key>", expected_url_list_value(KEY))),
            "unexpected body: {}",
            response.body
        );

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(KEY)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_raw_response_without_encoding_type_keeps_raw_key_fields() {
    const KEY: &str = "multi<>&\"";
    const PREFIX: &str = "multi<>&\"";

    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(KEY)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let url = format!(
            "{}/{bucket}?uploads&prefix={}",
            CTX.endpoint(),
            query_encode_value(PREFIX)
        );
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            !response.body.contains("<EncodingType>url</EncodingType>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains(&format!(
                "<Prefix>{}</Prefix>",
                expected_raw_list_value(PREFIX)
            )),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response
                .body
                .contains(&format!("<Key>{}</Key>", expected_raw_list_value(KEY))),
            "unexpected body: {}",
            response.body
        );

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(KEY)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_raw_response_encodes_key_marker() {
    const FIRST_KEY: &str = "a<>&\"";
    const SECOND_KEY: &str = "b";

    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let create_first = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(FIRST_KEY)
            .send()
            .await
            .unwrap();
        let first_upload_id = create_first.upload_id().unwrap().to_string();

        let create_second = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(SECOND_KEY)
            .send()
            .await
            .unwrap();
        let second_upload_id = create_second.upload_id().unwrap().to_string();

        let url = format!(
            "{}/{bucket}?uploads&encoding-type=url&key-marker={}&max-uploads=1",
            CTX.endpoint(),
            query_encode_value(FIRST_KEY)
        );
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains(&format!(
                "<KeyMarker>{}</KeyMarker>",
                expected_url_list_value(FIRST_KEY)
            )),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains("<Key>b</Key>"),
            "unexpected body: {}",
            response.body
        );

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(FIRST_KEY)
            .upload_id(&first_upload_id)
            .send()
            .await
            .unwrap();
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(SECOND_KEY)
            .upload_id(&second_upload_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_multipart_uploads_raw_response_encodes_next_markers() {
    const FIRST_KEY: &str = "a<>&\"";
    const SECOND_KEY: &str = "b";

    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let create_first = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(FIRST_KEY)
            .send()
            .await
            .unwrap();
        let first_upload_id = create_first.upload_id().unwrap().to_string();

        let create_second = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(SECOND_KEY)
            .send()
            .await
            .unwrap();
        let second_upload_id = create_second.upload_id().unwrap().to_string();

        let url = format!(
            "{}/{bucket}?uploads&encoding-type=url&max-uploads=1",
            CTX.endpoint(),
        );
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains(&format!(
                "<NextKeyMarker>{}</NextKeyMarker>",
                expected_url_list_value(FIRST_KEY)
            )),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains(&format!(
                "<NextUploadIdMarker>{first_upload_id}</NextUploadIdMarker>"
            )),
            "unexpected body: {}",
            response.body
        );

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(FIRST_KEY)
            .upload_id(&first_upload_id)
            .send()
            .await
            .unwrap();
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(SECOND_KEY)
            .upload_id(&second_upload_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

// ── ListParts ───────────────────────────────────────────────────────

#[test]
fn test_list_parts() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "list-parts-key";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload two parts
        client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![b'a'; PART_SIZE]))
            .send()
            .await
            .unwrap();

        client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(2)
            .body(ByteStream::from(vec![b'b'; 1024]))
            .send()
            .await
            .unwrap();

        let resp = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();
        let parts = resp.parts();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].part_number().unwrap(), 1);
        assert_eq!(parts[0].size().unwrap(), PART_SIZE as i64);
        assert_eq!(parts[1].part_number().unwrap(), 2);
        assert_eq!(parts[1].size().unwrap(), 1024);

        // Abort to clean up
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_list_parts_pagination_with_checksums() {
    use aws_sdk_s3::types::ChecksumAlgorithm;

    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "list-parts-page";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let mut expected = Vec::new();
        for (part_number, data) in [(1, vec![b'a'; PART_SIZE]), (2, vec![b'b'; 1024])] {
            let resp = client
                .upload_part()
                .bucket(&bucket)
                .key(key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(ByteStream::from(data))
                .checksum_algorithm(ChecksumAlgorithm::Crc32)
                .send()
                .await
                .unwrap();
            expected.push((part_number, resp.checksum_crc32().unwrap().to_string()));
        }

        let resp1 = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .max_parts(1)
            .send()
            .await
            .unwrap();
        assert_eq!(resp1.parts().len(), 1);
        assert_eq!(resp1.parts()[0].part_number(), Some(1));
        assert_eq!(
            resp1.parts()[0].checksum_crc32(),
            Some(expected[0].1.as_str())
        );
        assert_eq!(resp1.checksum_algorithm(), Some(&ChecksumAlgorithm::Crc32));
        assert_eq!(resp1.is_truncated(), Some(true));
        let next_marker = resp1.next_part_number_marker().unwrap();

        let resp2 = client
            .list_parts()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number_marker(next_marker)
            .max_parts(1)
            .send()
            .await
            .unwrap();
        assert_eq!(resp2.parts().len(), 1);
        assert_eq!(resp2.parts()[0].part_number(), Some(2));
        assert_eq!(
            resp2.parts()[0].checksum_crc32(),
            Some(expected[1].1.as_str())
        );
        assert_eq!(resp2.checksum_algorithm(), Some(&ChecksumAlgorithm::Crc32));
        assert_eq!(resp2.is_truncated(), Some(false));

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

// ── Error cases ─────────────────────────────────────────────────────

#[test]
fn test_complete_multipart_no_such_upload() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("nokey")
            .upload_id("nonexistent-upload-id")
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag("\"abc\"")
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;
        assert!(result.is_err());

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_abort_multipart_no_such_upload() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        let result = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("nokey")
            .upload_id("nonexistent-upload-id")
            .send()
            .await;
        // Our server returns NoSuchUpload for nonexistent upload IDs.
        s3_tests::assert_s3_err_code(&result, "NoSuchUpload");

        cleanup(&bucket, &[]).await;
    });
}

// ── Part size validation ────────────────────────────────────────────

#[test]
fn test_multipart_part_too_small() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "part-too-small";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload two parts, first one too small (< 5MB)
        let resp1 = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![0u8; 100]))
            .send()
            .await
            .unwrap();

        let resp2 = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(2)
            .body(ByteStream::from(vec![0u8; 100]))
            .send()
            .await
            .unwrap();

        // CompleteMultipartUpload should fail with EntityTooSmall
        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp1.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp2.e_tag().unwrap())
                            .part_number(2)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;
        assert!(result.is_err());

        // Abort to clean up
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_upload_part_invalid_part_number_exceeds_max() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "invalid-upload-part-number";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let result = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(10_001)
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidArgument");

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[]).await;
    });
}

// ── Overwrite existing object ───────────────────────────────────────

#[test]
fn test_multipart_overwrites_existing_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "overwrite-me";

        // Put a regular object first
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"original"))
            .send()
            .await
            .unwrap();

        // Overwrite with multipart
        let new_data = vec![b'z'; 512];
        do_multipart_upload(&bucket, key, std::slice::from_ref(&new_data)).await;

        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&data[..], &new_data[..]);

        cleanup(&bucket, &[key]).await;
    });
}

// ── HeadObject on multipart object ──────────────────────────────────

#[test]
fn test_multipart_head_object() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-head";

        let part1 = vec![b'h'; PART_SIZE];
        let part2 = vec![b'i'; 2048];
        do_multipart_upload(&bucket, key, &[part1, part2]).await;

        let resp = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.content_length().unwrap(), (PART_SIZE + 2048) as i64);

        cleanup(&bucket, &[key]).await;
    });
}

// ── Range read on multipart object ──────────────────────────────────

#[test]
fn test_multipart_range_read() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-range";

        let part1 = vec![b'A'; PART_SIZE];
        let part2 = vec![b'B'; 2048];
        do_multipart_upload(&bucket, key, &[part1, part2]).await;

        // Read across the part boundary
        let start = PART_SIZE - 10;
        let end = PART_SIZE + 9;
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .range(format!("bytes={}-{}", start, end))
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(data.len(), 20);
        assert!(data[..10].iter().all(|&b| b == b'A'));
        assert!(data[10..].iter().all(|&b| b == b'B'));

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_multipart_get_part_rejects_range_header() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multipart-part-precedence";

        let part1 = vec![b'A'; PART_SIZE];
        let part2 = vec![b'B'; 2048];
        do_multipart_upload(&bucket, key, &[part1, part2.clone()]).await;

        let result = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .part_number(2)
            .range("bytes=0-1")
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup(&bucket, &[key]).await;
    });
}

// ── Multiple concurrent uploads for same key ────────────────────────

#[test]
fn test_multipart_concurrent_uploads_same_key() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "concurrent";

        // Start two uploads for the same key
        let c1 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let uid1 = c1.upload_id().unwrap().to_string();

        let c2 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let uid2 = c2.upload_id().unwrap().to_string();
        assert_ne!(uid1, uid2);

        // Both should appear in listing
        let resp = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.uploads().len(), 2);

        // Complete the first, abort the second
        let r1 = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&uid1)
            .part_number(1)
            .body(ByteStream::from(vec![b'1'; 100]))
            .send()
            .await
            .unwrap();

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&uid1)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(r1.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&uid2)
            .send()
            .await
            .unwrap();

        // Only the completed upload's object should exist
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(data.len(), 100);
        assert!(data.iter().all(|&b| b == b'1'));

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_complete_multipart_upload_accepts_matching_expected_object_size() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "mp-object-size-match";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let body = b"hello world";
        let part = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(body.to_vec()))
            .send()
            .await
            .unwrap();

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .mpu_object_size(body.len() as i64)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(part.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let object = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(object.content_length(), Some(body.len() as i64));

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_complete_multipart_upload_rejects_mismatched_expected_object_size() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "mp-object-size-mismatch";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        let body = b"hello world";
        let part = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(body.to_vec()))
            .send()
            .await
            .unwrap();

        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .mpu_object_size((body.len() as i64) + 1)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(part.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;

        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidRequest");

        cleanup(&bucket, &[]).await;
    });
}

// ── Part overwrite (re-upload same part number) ─────────────────────

#[test]
fn test_multipart_part_overwrite() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "part-overwrite";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload part 1 with data 'a'
        client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![b'a'; 256]))
            .send()
            .await
            .unwrap();

        // Re-upload part 1 with data 'b' — should replace
        let resp = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![b'b'; 512]))
            .send()
            .await
            .unwrap();

        // Complete with the second upload's ETag
        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let data = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(data.len(), 512);
        assert!(data.iter().all(|&b| b == b'b'));

        cleanup(&bucket, &[key]).await;
    });
}

// ── Completion validation (Ceph parity) ─────────────────────────────

/// Ceph: test_multipart_upload_empty — completing with no parts should fail.
#[test]
fn test_multipart_complete_empty_parts() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "empty-complete";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(CompletedMultipartUpload::builder().build())
            .send()
            .await;
        assert!(result.is_err());

        // Abort to clean up
        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
        cleanup(&bucket, &[]).await;
    });
}

/// Ceph: test_multipart_upload_incorrect_etag — wrong ETag should fail with InvalidPart.
#[test]
fn test_multipart_complete_incorrect_etag() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "wrong-etag";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![0u8; 256]))
            .send()
            .await
            .unwrap();

        // Complete with a fabricated ETag
        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag("\"ffffffffffffffff\"")
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;
        assert_s3_err_code(&result, "InvalidPart");

        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
        cleanup(&bucket, &[]).await;
    });
}

/// Ceph: test_multipart_upload_missing_part — referencing an unuploaded part number.
#[test]
fn test_multipart_complete_missing_part() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "missing-part";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload part 1
        let resp = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![0u8; 256]))
            .send()
            .await
            .unwrap();

        // Complete referencing part 9999 (never uploaded)
        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp.e_tag().unwrap())
                            .part_number(9999)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;
        assert_s3_err_code(&result, "InvalidPart");

        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
        cleanup(&bucket, &[]).await;
    });
}

/// Ceph: test_multipart_upload — metadata and content-type survive multipart.
#[test]
fn test_multipart_metadata_preserved() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "meta-preserved";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .content_type("application/octet-stream")
            .metadata("testkey", "testvalue")
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let resp = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![b'm'; 128]))
            .send()
            .await
            .unwrap();

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(head.content_type().unwrap(), "application/octet-stream");
        assert_eq!(
            head.metadata().unwrap().get("testkey").unwrap(),
            "testvalue"
        );

        cleanup(&bucket, &[key]).await;
    });
}

/// Parts must be in strictly ascending order.
#[test]
fn test_multipart_complete_invalid_order() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "invalid-order";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let r1 = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![b'a'; PART_SIZE]))
            .send()
            .await
            .unwrap();

        let r2 = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(2)
            .body(ByteStream::from(vec![b'b'; 256]))
            .send()
            .await
            .unwrap();

        // Complete with parts in reverse order (2, 1)
        let result = client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(r2.e_tag().unwrap())
                            .part_number(2)
                            .build(),
                    )
                    .parts(
                        CompletedPart::builder()
                            .e_tag(r1.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await;
        assert_s3_err_code(&result, "InvalidPartOrder");

        let _ = client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
        cleanup(&bucket, &[]).await;
    });
}

/// Multipart ETag is a composite format: "hex-N".
#[test]
fn test_multipart_composite_etag() {
    s3_tests::run(async {
        let bucket = setup_bucket().await;
        let key = "composite-etag";

        let part_data = vec![b'e'; 256];
        let etag = do_multipart_upload(&bucket, key, &[part_data]).await;
        // Multipart ETags have the format "hex-N" where N is part count
        assert!(
            etag.contains("-1"),
            "expected composite ETag with -1 suffix, got: {etag}"
        );

        cleanup(&bucket, &[key]).await;
    });
}

// ── Ceph parity: resend part ────────────────────────────────────────

/// Re-uploading a part before completion replaces the previous upload.
///
/// Matches Ceph: test_multipart_upload_resend_part
#[test]
fn test_multipart_upload_resend_part() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "resend-part";

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Upload part 1 with data 'A'
        let data_a = vec![b'A'; PART_SIZE];
        let _resp_a = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(data_a))
            .send()
            .await
            .unwrap();

        // Re-upload part 1 with data 'B' (replaces the first upload)
        let data_b = vec![b'B'; PART_SIZE];
        let resp_b = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(data_b.clone()))
            .send()
            .await
            .unwrap();

        // Complete with the second ETag
        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(resp_b.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();

        // Verify the content is from the second upload
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.len(), PART_SIZE);
        assert!(body.iter().all(|&b| b == b'B'));

        cleanup(&bucket, &[key]).await;
    });
}

// ── Ceph parity: multiple sizes ─────────────────────────────────────

/// Multipart upload with various total sizes, covering all boundary
/// variants from the Ceph test.
///
/// Matches Ceph: test_multipart_upload_multiple_sizes
#[test]
fn test_multipart_upload_multiple_sizes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "multi-sizes";
        let mb = 1024 * 1024;
        let kb = 1024;

        // Helper: upload with given total size split into 5MB parts + remainder
        async fn upload_and_check(
            client: &aws_sdk_s3::Client,
            bucket: &str,
            key: &str,
            total: usize,
        ) {
            let part_size = 5 * 1024 * 1024;
            let mut parts = Vec::new();
            let mut remaining = total;
            while remaining > 0 {
                let sz = remaining.min(part_size);
                parts.push(vec![b'x'; sz]);
                remaining -= sz;
            }
            do_multipart_upload(bucket, key, &parts).await;
            let head = client
                .head_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .unwrap();
            assert_eq!(
                head.content_length(),
                Some(total as i64),
                "size mismatch for {total} byte upload"
            );
        }

        // Ceph sizes: 5MB, 5MB+100KB, 5MB+600KB, 10MB+100KB, 10MB+600KB, 10MB
        for size in [
            5 * mb,
            5 * mb + 100 * kb,
            5 * mb + 600 * kb,
            10 * mb + 100 * kb,
            10 * mb + 600 * kb,
            10 * mb,
        ] {
            upload_and_check(client, &bucket, key, size).await;
        }

        cleanup(&bucket, &[key]).await;
    });
}

// ── PartNumber GET semantics ────────────────────────────────────────

#[test]
fn test_multipart_get_part() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "mymultipart";

        let part_sizes = [PART_SIZE, PART_SIZE, PART_SIZE, 1024 * 1024];
        let parts_data: Vec<Vec<u8>> = part_sizes
            .iter()
            .enumerate()
            .map(|(i, &sz)| vec![(i as u8) + b'A'; sz])
            .collect();

        let etag = do_multipart_upload(&bucket, key, &parts_data).await;
        let part_count = part_sizes.len() as i32;

        // HeadObject + GetObject for each valid part
        let mut data_offset = 0usize;
        for (i, data) in parts_data.iter().enumerate() {
            let pn = (i + 1) as i32;

            // HeadObject with partNumber
            let head = client
                .head_object()
                .bucket(&bucket)
                .key(key)
                .part_number(pn)
                .send()
                .await
                .unwrap();
            assert_eq!(
                head.parts_count(),
                Some(part_count),
                "PartsCount for part {pn}"
            );
            assert_eq!(
                head.e_tag().unwrap(),
                etag,
                "ETag mismatch on HEAD part {pn}"
            );

            // GetObject with partNumber
            let resp = client
                .get_object()
                .bucket(&bucket)
                .key(key)
                .part_number(pn)
                .send()
                .await
                .unwrap();
            assert_eq!(
                resp.parts_count(),
                Some(part_count),
                "PartsCount for GET part {pn}"
            );
            assert_eq!(
                resp.e_tag().unwrap(),
                etag,
                "ETag mismatch on GET part {pn}"
            );
            assert_eq!(
                resp.content_length(),
                Some(data.len() as i64),
                "ContentLength for part {pn}"
            );

            let body = resp.body.collect().await.unwrap().into_bytes();
            assert_eq!(&body[..], &data[..], "data mismatch for part {pn}");
            data_offset += data.len();
        }
        let _ = data_offset; // consumed all data

        // Out-of-range partNumber on GET → 416 Range Not Satisfiable (AWS behavior)
        let result = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .part_number(part_count + 1)
            .send()
            .await;
        assert_eq!(err_status(&result), 416);

        // Out-of-range partNumber on HEAD → same error
        let result = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .part_number(part_count + 1)
            .send()
            .await;
        assert_eq!(err_status(&result), 416);

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_non_multipart_get_part() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "singlepart";

        let resp = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(b"body".to_vec()))
            .send()
            .await
            .unwrap();
        let etag = resp.e_tag().unwrap().to_string();

        // GET PartNumber > 1 → 416 Range Not Satisfiable (AWS behavior)
        let result = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .part_number(2)
            .send()
            .await;
        assert_eq!(err_status(&result), 416);

        // HEAD PartNumber > 1 → same error
        let result = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .part_number(2)
            .send()
            .await;
        assert_eq!(err_status(&result), 416);

        // PartNumber = 1 → returns entire object
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .part_number(1)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.e_tag().unwrap(), etag);
        let body = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], b"body");

        cleanup(&bucket, &[key]).await;
    });
}

// ── Zero-byte final part with partNumber ────────────────────────────

#[test]
fn test_multipart_get_zero_byte_final_part() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let key = "zerobyte-final";

        let part1 = vec![b'X'; PART_SIZE];
        let part2 = vec![]; // zero-byte final part
        let etag = do_multipart_upload(&bucket, key, &[part1.clone(), part2]).await;

        // GET partNumber=1 → normal data
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .part_number(1)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.parts_count(), Some(2));
        assert_eq!(resp.e_tag().unwrap(), etag);
        let body = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.len(), PART_SIZE);

        // GET partNumber=2 → zero-byte part, should not panic
        let resp = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .part_number(2)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.parts_count(), Some(2));
        assert_eq!(resp.content_length(), Some(0));
        let body = resp.body.collect().await.unwrap().into_bytes();
        assert!(body.is_empty());

        // HEAD partNumber=2 → zero-byte
        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .part_number(2)
            .send()
            .await
            .unwrap();
        assert_eq!(head.parts_count(), Some(2));
        assert_eq!(head.content_length(), Some(0));

        cleanup(&bucket, &[key]).await;
    });
}

// ── UploadPartCopy ──────────────────────────────────────────────────

#[test]
fn test_multipart_copy_small() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let src_key = "copy-src-small";
        let dst_key = "copy-dst-small";

        // Create source object
        let src_data = vec![b'x'; PART_SIZE];
        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from(src_data.clone()))
            .send()
            .await
            .unwrap();

        // Create multipart upload, upload_part_copy entire source as one part
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let copy_resp = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(1)
            .copy_source(format!("{}/{}", bucket, src_key))
            .send()
            .await
            .unwrap();

        let etag = copy_resp.copy_part_result().unwrap().e_tag().unwrap();

        // Complete multipart upload
        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(CompletedPart::builder().e_tag(etag).part_number(1).build())
                    .build(),
            )
            .send()
            .await
            .unwrap();

        // Verify GET returns correct data
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap();
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), &src_data[..]);

        cleanup(&bucket, &[src_key, dst_key]).await;
    });
}

#[test]
fn test_multipart_copy_without_range() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let src_key = "copy-src-no-range";
        let dst_key = "copy-dst-no-range";

        // Create source with known data
        let src_data = vec![b'A'; PART_SIZE + 1000];
        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from(src_data.clone()))
            .send()
            .await
            .unwrap();

        // UploadPartCopy without range copies full object
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let copy_resp = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(1)
            .copy_source(format!("{}/{}", bucket, src_key))
            .send()
            .await
            .unwrap();

        let etag = copy_resp.copy_part_result().unwrap().e_tag().unwrap();

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(CompletedPart::builder().e_tag(etag).part_number(1).build())
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap();
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.len(), src_data.len());
        assert_eq!(body.as_ref(), &src_data[..]);

        cleanup(&bucket, &[src_key, dst_key]).await;
    });
}

#[test]
fn test_multipart_copy_invalid_range() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let src_key = "copy-src-invalid-range";
        let dst_key = "copy-dst-invalid-range";

        // Create small source
        let src_data = vec![b'Z'; 1000];
        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from(src_data))
            .send()
            .await
            .unwrap();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Range beyond source size → InvalidArgument (400)
        let result = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(1)
            .copy_source(format!("{}/{}", bucket, src_key))
            .copy_source_range("bytes=0-9999")
            .send()
            .await;
        let status = err_status(&result);
        assert!(status == 400, "expected 400, got {status}");
        assert_s3_err_code(&result, "InvalidArgument");

        // Cleanup
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[src_key]).await;
    });
}

#[test]
fn test_multipart_copy_improper_range() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let src_key = "copy-src-improper";
        let dst_key = "copy-dst-improper";

        let src_data = vec![b'M'; 1000];
        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from(src_data))
            .send()
            .await
            .unwrap();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // start > end → InvalidArgument (400)
        let result = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(1)
            .copy_source(format!("{}/{}", bucket, src_key))
            .copy_source_range("bytes=500-100")
            .send()
            .await;
        let status = err_status(&result);
        assert!(status == 400, "expected 400, got {status}");
        assert_s3_err_code(&result, "InvalidArgument");

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[src_key]).await;
    });
}

#[test]
fn test_multipart_copy_invalid_part_number_exceeds_max() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let src_key = "copy-src-invalid-part-number";
        let dst_key = "copy-dst-invalid-part-number";

        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let result = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(10_001)
            .copy_source(format!("{}/{}", bucket, src_key))
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidArgument");

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[src_key]).await;
    });
}

#[test]
fn test_multipart_copy_special_names() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let src_key = "special key with spaces/and/slashes";
        let dst_key = "copy-dst-special";

        let src_data = vec![b'S'; PART_SIZE];
        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from(src_data.clone()))
            .send()
            .await
            .unwrap();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let copy_resp = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(1)
            .copy_source(format!("{}/{}", bucket, src_key))
            .send()
            .await
            .unwrap();

        let etag = copy_resp.copy_part_result().unwrap().e_tag().unwrap();

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(CompletedPart::builder().e_tag(etag).part_number(1).build())
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let get = client
            .get_object()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap();
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), &src_data[..]);

        cleanup(&bucket, &[src_key, dst_key]).await;
    });
}

#[test]
fn test_multipart_copy_versioned() {
    s3_tests::run(async {
        use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};

        let client = CTX.client();
        let bucket = setup_bucket().await;

        // Enable versioning
        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let src_key = "versioned-src";
        let dst_key = "versioned-dst";

        // Put version 1
        let data_v1 = vec![b'1'; PART_SIZE];
        let put1 = client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from(data_v1.clone()))
            .send()
            .await
            .unwrap();
        let v1_id = put1.version_id().unwrap().to_string();

        // Put version 2
        let data_v2 = vec![b'2'; PART_SIZE];
        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from(data_v2))
            .send()
            .await
            .unwrap();

        // Copy version 1 specifically via ?versionId=
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let copy_resp = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(1)
            .copy_source(copy_source_with_version(&bucket, src_key, &v1_id))
            .send()
            .await
            .unwrap();

        let etag = copy_resp.copy_part_result().unwrap().e_tag().unwrap();

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(CompletedPart::builder().e_tag(etag).part_number(1).build())
                    .build(),
            )
            .send()
            .await
            .unwrap();

        // Verify we got version 1 data
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap();
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), &data_v1[..]);

        s3_tests::cleanup_versioned_bucket(client, &bucket).await;
    });
}

/// Copying from a delete-marker source should fail with 404/NoSuchKey.
#[test]
fn test_multipart_copy_delete_marker_source() {
    s3_tests::run(async {
        use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};

        let client = CTX.client();
        let bucket = setup_bucket().await;

        // Enable versioning
        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let src_key = "delete-marker-src";
        let dst_key = "delete-marker-dst";

        // Put then delete to create a delete marker as current version
        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from(vec![b'd'; PART_SIZE]))
            .send()
            .await
            .unwrap();
        client
            .delete_object()
            .bucket(&bucket)
            .key(src_key)
            .send()
            .await
            .unwrap();

        // Attempt upload_part_copy from the delete-marked key
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let result = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(1)
            .copy_source(format!("{}/{}", bucket, src_key))
            .send()
            .await;
        let status = err_status(&result);
        assert_eq!(status, 404);
        assert_s3_err_code(&result, "NoSuchKey");

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();
        s3_tests::cleanup_versioned_bucket(client, &bucket).await;
    });
}

/// UploadPartCopy targeting a specific delete-marker versionId should fail
/// with 400/InvalidRequest (not 404/NoSuchKey).
#[test]
fn test_multipart_copy_delete_marker_version_id() {
    s3_tests::run(async {
        use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};

        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let src_key = "dm-vid-src";
        let dst_key = "dm-vid-dst";

        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from(vec![b'd'; PART_SIZE]))
            .send()
            .await
            .unwrap();
        let del = client
            .delete_object()
            .bucket(&bucket)
            .key(src_key)
            .send()
            .await
            .unwrap();
        let dm_version_id = del.version_id().unwrap();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let result = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(1)
            .copy_source(copy_source_with_version(&bucket, src_key, dm_version_id))
            .send()
            .await;
        assert!(result.is_err());
        let status = err_status(&result);
        assert_eq!(status, 400);
        assert_s3_err_code(&result, "InvalidRequest");

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();
        s3_tests::cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_multipart_copy_multiple_sizes() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let src_key = "copy-src-multi";
        let dst_key = "copy-dst-multi";

        // Create a source large enough for multiple range-copied parts
        let total_size = PART_SIZE * 2 + 500;
        let src_data: Vec<u8> = (0..total_size).map(|i| (i % 256) as u8).collect();
        client
            .put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from(src_data.clone()))
            .send()
            .await
            .unwrap();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Part 1: first PART_SIZE bytes
        let p1 = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(1)
            .copy_source(format!("{}/{}", bucket, src_key))
            .copy_source_range(format!("bytes=0-{}", PART_SIZE - 1))
            .send()
            .await
            .unwrap();
        let etag1 = p1.copy_part_result().unwrap().e_tag().unwrap().to_string();

        // Part 2: next PART_SIZE bytes
        let p2 = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(2)
            .copy_source(format!("{}/{}", bucket, src_key))
            .copy_source_range(format!("bytes={}-{}", PART_SIZE, PART_SIZE * 2 - 1))
            .send()
            .await
            .unwrap();
        let etag2 = p2.copy_part_result().unwrap().e_tag().unwrap().to_string();

        // Part 3: remaining 500 bytes
        let p3 = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(3)
            .copy_source(format!("{}/{}", bucket, src_key))
            .copy_source_range(format!("bytes={}-{}", PART_SIZE * 2, total_size - 1))
            .send()
            .await
            .unwrap();
        let etag3 = p3.copy_part_result().unwrap().e_tag().unwrap().to_string();

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(&etag1)
                            .part_number(1)
                            .build(),
                    )
                    .parts(
                        CompletedPart::builder()
                            .e_tag(&etag2)
                            .part_number(2)
                            .build(),
                    )
                    .parts(
                        CompletedPart::builder()
                            .e_tag(&etag3)
                            .part_number(3)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();

        // Verify assembled object matches source
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap();
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.len(), total_size);
        assert_eq!(body.as_ref(), &src_data[..]);

        cleanup(&bucket, &[src_key, dst_key]).await;
    });
}

/// Ceph parity: put object with percent-encoded key (`anyfilename%25.txt` stores as
/// `anyfilename%.txt`), then attempt upload_part_copy using the raw `%` key. The
/// raw key resolves differently than the percent-encoded one, so the copy source
/// should not be found.
#[test]
fn test_upload_part_copy_percent_encoded_key() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        let dst_key = "anyfile.txt";
        // This key contains a literal percent: "anyfilename%.txt"
        let encoded_key = "anyfilename%25.txt";
        let raw_key = "anyfilename%.txt";

        // Put the copy source under the percent-encoded key
        client
            .put_object()
            .bucket(&bucket)
            .key(encoded_key)
            .body(ByteStream::from(b"foo".to_vec()))
            .send()
            .await
            .unwrap();

        // Put the destination object (initial state)
        client
            .put_object()
            .bucket(&bucket)
            .key(dst_key)
            .body(ByteStream::from(b"foo".to_vec()))
            .send()
            .await
            .unwrap();

        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        // Copy using raw_key ("anyfilename%.txt") which is NOT the same as
        // the percent-encoded key — this should fail with NoSuchKey / 404.
        let result = client
            .upload_part_copy()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .part_number(1)
            .copy_source(format!("{}/{}", bucket, raw_key))
            .send()
            .await;
        assert!(result.is_err(), "expected error copying with raw % key");

        // Verify the original destination object is untouched
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(dst_key)
            .send()
            .await
            .unwrap();
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"foo");

        // Cleanup
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(dst_key)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();
        cleanup(&bucket, &[encoded_key, dst_key]).await;
    });
}

// ── Multi-user (not implemented) ────────────────────────────────────

#[test]
fn test_list_multipart_upload_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_public_write_bucket(client).await;
        let owner_id = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap()
            .owner()
            .expect("expected bucket owner in GetBucketAcl")
            .id()
            .expect("expected bucket owner ID in GetBucketAcl")
            .to_string();
        let alt_owner_id = canonical_owner_id(alt_client).await;

        let upload1 = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("multipart1")
            .send()
            .await
            .unwrap();
        let upload1_id = upload1.upload_id().unwrap().to_string();
        let upload2 = alt_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("multipart2")
            .send()
            .await
            .unwrap();
        let upload2_id = upload2.upload_id().unwrap().to_string();

        let mut views = Vec::new();
        for lister in [client, alt_client] {
            let resp = lister
                .list_multipart_uploads()
                .bucket(&bucket)
                .send()
                .await
                .unwrap();

            let uploads: BTreeMap<_, _> = resp
                .uploads()
                .iter()
                .map(|upload| {
                    let owner = upload.owner().expect("upload should have owner");
                    let initiator = upload.initiator().expect("upload should have initiator");
                    (
                        upload.key().expect("upload should have key").to_string(),
                        (
                            upload
                                .upload_id()
                                .expect("upload should have upload ID")
                                .to_string(),
                            owner.id().expect("owner should have ID").to_string(),
                            initiator
                                .id()
                                .expect("initiator should have ID")
                                .to_string(),
                        ),
                    )
                })
                .collect();

            assert_eq!(uploads.len(), 2);
            views.push(uploads);
        }

        assert_eq!(views[0], views[1]);

        let multipart1 = views[0].get("multipart1").expect("expected multipart1");
        assert_eq!(multipart1.0, upload1_id);
        assert_eq!(multipart1.1, owner_id);
        assert!(!multipart1.2.is_empty());

        let multipart2 = views[0].get("multipart2").expect("expected multipart2");
        assert_eq!(multipart2.0, upload2_id);
        assert_eq!(multipart2.1, alt_owner_id);
        assert!(!multipart2.2.is_empty());

        assert_ne!(
            multipart1.2, multipart2.2,
            "initiator IDs should distinguish the two upload creators"
        );

        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("multipart1")
            .upload_id(&upload1_id)
            .send()
            .await
            .unwrap();
        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("multipart2")
            .upload_id(&upload2_id)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_anon_create_multipart_upload_public_write_bucket_fail() {
    s3_tests::run(async {
        let bucket = create_public_write_bucket(CTX.client()).await;
        let url = format!("{}/{}/anon-multipart?uploads", CTX.endpoint(), bucket);
        let mut resp = anon_agent()
            .post(&url)
            .send(b"" as &[u8])
            .expect("transport error");
        let status = resp.status().as_u16();
        let body = resp.body_mut().read_to_string().unwrap_or_default();
        assert_eq!(
            status, 403,
            "expected 403 for anonymous CreateMultipartUpload on public-read-write bucket, got {} body={}",
            status, body
        );
        assert!(
            body.contains("<Code>AccessDenied</Code>"),
            "expected AccessDenied in body: {body}"
        );

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_signed_create_multipart_upload_public_write_bucket_rejects_existing_owner_key() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_public_write_bucket(client).await;
        let key = "multipart-existing-owner-key";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"owner-body"))
            .send()
            .await
            .unwrap();

        let create = alt_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;
        assert_eq!(err_status(&create), 403);
        assert_s3_err_code(&create, "AccessDenied");

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_complete_multipart_upload_allows_owner_key_created_after_public_write_initiation() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_public_write_bucket(client).await;
        let key = "multipart-public-write-race";

        let create = alt_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"owner-body"))
            .send()
            .await
            .unwrap();

        let data = vec![b'x'; 1024];
        let upload_part = alt_client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(data.clone()))
            .send()
            .await
            .unwrap();

        let complete = alt_client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .e_tag(upload_part.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();
        assert!(
            complete.e_tag().is_some(),
            "expected CompleteMultipartUpload to return an ETag"
        );

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_multipart_initiator_cannot_continue_after_bucket_acl_change() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = create_public_write_bucket(client).await;
        let key = "multipart-acl-change";

        let create = alt_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap().to_string();

        client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(aws_sdk_s3::types::BucketCannedAcl::Private)
            .send()
            .await
            .unwrap();

        let upload_part = alt_client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(vec![b'x'; 1024]))
            .send()
            .await;
        assert_eq!(err_status(&upload_part), 403);
        assert_s3_err_code(&upload_part, "AccessDenied");

        alt_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();

        cleanup(&bucket, &[]).await;
    });
}
