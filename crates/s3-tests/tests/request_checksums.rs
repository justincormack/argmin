use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketLocationConstraint, CreateBucketConfiguration, ObjectLockLegalHold,
    ObjectLockLegalHoldStatus, ObjectOwnership,
};
use s3_tests::{content_md5_header, sdk_checksum_headers, send_signed_request, unique_bucket, CTX};

const REQUIRED_CHECKSUM_MESSAGE: &str =
    "Missing required header for this request: Content-MD5 OR x-amz-checksum-*";
const LIFECYCLE_REQUIRED_CHECKSUM_MESSAGE: &str =
    "Missing required header for this request: Content-MD5";
const OBJECT_LOCK_PUT_REQUIRED_CHECKSUM_MESSAGE: &str =
    "Content-MD5 OR x-amz-checksum- HTTP header is required for Put Object requests with Object Lock parameters";

fn assert_error_code(body: &str, code: &str) {
    let expected = format!("<Code>{code}</Code>");
    assert!(
        body.contains(&expected),
        "expected {expected} in body, got {body}"
    );
}

fn assert_error_message(body: &str, message: &str) {
    let expected = format!("<Message>{message}</Message>");
    assert!(
        body.contains(&expected),
        "expected {expected} in body, got {body}"
    );
}

fn assert_max_message_length(body: &str, expected_bytes: usize) {
    assert_error_code(body, "MaxMessageLengthExceeded");
    assert_error_message(body, "Your request was too big.");
    let expected = format!("<MaxMessageLengthBytes>{expected_bytes}</MaxMessageLengthBytes>");
    assert!(
        body.contains(&expected),
        "expected {expected} in body, got {body}"
    );
}

fn comment_pad(size: usize) -> String {
    format!("<!--{}-->", "x".repeat(size))
}

async fn create_bucket_in_test_region(
    object_ownership: Option<ObjectOwnership>,
    object_lock_enabled: bool,
) -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    let mut request = client.create_bucket().bucket(&bucket);
    if CTX.region() != "us-east-1" {
        let config = CreateBucketConfiguration::builder()
            .location_constraint(BucketLocationConstraint::from(CTX.region()))
            .build();
        request = request.create_bucket_configuration(config);
    }
    if let Some(object_ownership) = object_ownership {
        request = request.object_ownership(object_ownership);
    }
    if object_lock_enabled {
        request = request.object_lock_enabled_for_bucket(true);
    }
    request.send().await.unwrap();
    bucket
}

async fn cleanup_bucket(bucket: &str) {
    CTX.client()
        .delete_bucket()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
}

async fn cleanup_bucket_with_keys(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    cleanup_bucket(bucket).await;
}

async fn cleanup_object_lock_bucket_with_version(bucket: &str, key: &str, version_id: &str) {
    let client = CTX.client();
    let _ = client
        .put_object_legal_hold()
        .bucket(bucket)
        .key(key)
        .version_id(version_id)
        .legal_hold(
            ObjectLockLegalHold::builder()
                .status(ObjectLockLegalHoldStatus::Off)
                .build(),
        )
        .send()
        .await;
    let _ = client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .version_id(version_id)
        .bypass_governance_retention(true)
        .send()
        .await;
    cleanup_bucket(bucket).await;
}

async fn current_version_id(bucket: &str, key: &str) -> String {
    CTX.client()
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap()
        .version_id()
        .unwrap()
        .to_string()
}

async fn abort_multipart_upload(bucket: &str, key: &str, upload_id: &str) {
    CTX.client()
        .abort_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .send()
        .await
        .unwrap();
}

async fn bucket_owner_id(bucket: &str) -> String {
    CTX.client()
        .get_bucket_acl()
        .bucket(bucket)
        .send()
        .await
        .unwrap()
        .owner()
        .and_then(|owner| owner.id())
        .unwrap()
        .to_string()
}

fn valid_bucket_policy_json(bucket: &str) -> String {
    format!(
        "{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Sid\":\"AllowOwnerList\",\"Effect\":\"Allow\",\"Principal\":{{\"AWS\":\"arn:aws:iam::{}:root\"}},\"Action\":\"s3:ListBucket\",\"Resource\":\"arn:aws:s3:::{}\"}}]}}",
        CTX.account_id(),
        bucket
    )
}

fn assert_missing_checksum_rejected(
    name: &str,
    method: &str,
    url: &str,
    body: &[u8],
    headers: &[(String, String)],
    expected_message: &str,
) {
    let response = send_signed_request(
        method,
        url,
        body,
        headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str())),
    );
    assert_eq!(
        response.status, 400,
        "{name}: expected missing checksum to fail, got {} body {}",
        response.status, response.body
    );
    assert_error_code(&response.body, "InvalidRequest");
    assert_error_message(&response.body, expected_message);
}

fn assert_request_succeeds(
    name: &str,
    method: &str,
    url: &str,
    body: &[u8],
    headers: &[(String, String)],
) {
    let response = send_signed_request(
        method,
        url,
        body,
        headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str())),
    );
    assert!(
        response.status < 300,
        "{name}: expected success, got {} body {}",
        response.status,
        response.body
    );
}

#[derive(Clone, Copy)]
enum BucketSetup {
    Standard,
    ObjectLock,
}

struct BucketChecksumCase {
    name: &'static str,
    query: &'static str,
    body: &'static [u8],
    headers: &'static [(&'static str, &'static str)],
    setup: BucketSetup,
    expected_missing_message: &'static str,
}

#[test]
fn test_bucket_subresource_request_checksum_matrix() {
    const CASES: &[BucketChecksumCase] = &[
        BucketChecksumCase {
            name: "PutBucketCors",
            query: "cors",
            body: br#"<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>https://example.com</AllowedOrigin></CORSRule></CORSConfiguration>"#,
            headers: &[],
            setup: BucketSetup::Standard,
            expected_missing_message: REQUIRED_CHECKSUM_MESSAGE,
        },
        BucketChecksumCase {
            name: "PutBucketEncryption",
            query: "encryption",
            body: br#"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"#,
            headers: &[],
            setup: BucketSetup::Standard,
            expected_missing_message: "",
        },
        BucketChecksumCase {
            name: "PutBucketLifecycleConfiguration",
            query: "lifecycle",
            body: br#"<LifecycleConfiguration><Rule><ID>rule1</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>"#,
            headers: &[],
            setup: BucketSetup::Standard,
            expected_missing_message: LIFECYCLE_REQUIRED_CHECKSUM_MESSAGE,
        },
        BucketChecksumCase {
            name: "PutBucketOwnershipControls",
            query: "ownershipControls",
            body: br#"<OwnershipControls><Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule></OwnershipControls>"#,
            headers: &[],
            setup: BucketSetup::Standard,
            expected_missing_message: "",
        },
        BucketChecksumCase {
            name: "PutBucketTagging",
            query: "tagging",
            body: br#"<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>"#,
            headers: &[],
            setup: BucketSetup::Standard,
            expected_missing_message: REQUIRED_CHECKSUM_MESSAGE,
        },
        BucketChecksumCase {
            name: "PutBucketVersioning",
            query: "versioning",
            body: br#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#,
            headers: &[],
            setup: BucketSetup::Standard,
            expected_missing_message: "",
        },
        BucketChecksumCase {
            name: "PutBucketPublicAccessBlock",
            query: "publicAccessBlock",
            body: br#"<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration>"#,
            headers: &[],
            setup: BucketSetup::Standard,
            expected_missing_message: "",
        },
        BucketChecksumCase {
            name: "PutBucketObjectLockConfiguration",
            query: "object-lock",
            body: br#"<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days></DefaultRetention></Rule></ObjectLockConfiguration>"#,
            headers: &[],
            setup: BucketSetup::ObjectLock,
            expected_missing_message: REQUIRED_CHECKSUM_MESSAGE,
        },
    ];

    s3_tests::run(async {
        for case in CASES {
            let bucket = match case.setup {
                BucketSetup::Standard => create_bucket_in_test_region(None, false).await,
                BucketSetup::ObjectLock => create_bucket_in_test_region(None, true).await,
            };
            let url = format!("{}/{}?{}", CTX.endpoint(), bucket, case.query);
            let base_headers: Vec<(String, String)> = case
                .headers
                .iter()
                .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
                .collect();

            if case.expected_missing_message.is_empty() {
                assert_request_succeeds(case.name, "PUT", &url, case.body, &base_headers);
            } else {
                assert_missing_checksum_rejected(
                    case.name,
                    "PUT",
                    &url,
                    case.body,
                    &base_headers,
                    case.expected_missing_message,
                );
            }

            let mut md5_headers = base_headers.clone();
            md5_headers.push(content_md5_header(case.body));
            assert_request_succeeds(case.name, "PUT", &url, case.body, &md5_headers);

            let mut sdk_headers = base_headers;
            sdk_headers.extend(sdk_checksum_headers(case.body));
            assert_request_succeeds(case.name, "PUT", &url, case.body, &sdk_headers);

            cleanup_bucket(&bucket).await;
        }

        let bucket = create_bucket_in_test_region(None, false).await;
        let body = valid_bucket_policy_json(&bucket);
        let url = format!("{}/{}?policy", CTX.endpoint(), bucket);
        assert_request_succeeds("PutBucketPolicy", "PUT", &url, body.as_bytes(), &[]);
        let md5_headers = [content_md5_header(body.as_bytes())];
        assert_request_succeeds(
            "PutBucketPolicy",
            "PUT",
            &url,
            body.as_bytes(),
            &md5_headers,
        );
        let sdk_headers = sdk_checksum_headers(body.as_bytes());
        assert_request_succeeds(
            "PutBucketPolicy",
            "PUT",
            &url,
            body.as_bytes(),
            &sdk_headers,
        );
        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_object_subresource_request_checksum_matrix() {
    s3_tests::run(async {
        let key = "obj";

        let bucket = create_bucket_in_test_region(None, false).await;
        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();
        cleanup_bucket_with_keys(&bucket, &[key]).await;

        let bucket = create_bucket_in_test_region(None, true).await;
        let put = CTX
            .client()
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();
        let version_id = put.version_id().unwrap().to_string();
        let legal_hold_body =
            br#"<LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>OFF</Status></LegalHold>"#;
        let legal_hold_url = format!(
            "{}/{}/{}?legal-hold&versionId={}",
            CTX.endpoint(),
            bucket,
            key,
            url::form_urlencoded::byte_serialize(version_id.as_bytes()).collect::<String>()
        );
        assert_missing_checksum_rejected(
            "PutObjectLegalHold",
            "PUT",
            &legal_hold_url,
            legal_hold_body,
            &[],
            REQUIRED_CHECKSUM_MESSAGE,
        );
        let md5_headers = vec![content_md5_header(legal_hold_body)];
        assert_request_succeeds(
            "PutObjectLegalHold",
            "PUT",
            &legal_hold_url,
            legal_hold_body,
            &md5_headers,
        );
        let sdk_headers = sdk_checksum_headers(legal_hold_body);
        assert_request_succeeds(
            "PutObjectLegalHold",
            "PUT",
            &legal_hold_url,
            legal_hold_body,
            &sdk_headers,
        );
        cleanup_object_lock_bucket_with_version(&bucket, key, &version_id).await;

        let bucket = create_bucket_in_test_region(None, true).await;
        let put = CTX
            .client()
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();
        let version_id = put.version_id().unwrap().to_string();
        let retain_until = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 24 * 60 * 60;
        let retention_body = format!(
            "<ObjectLockRetention xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Mode>GOVERNANCE</Mode><RetainUntilDate>{}</RetainUntilDate></ObjectLockRetention>",
            aws_sdk_s3::primitives::DateTime::from_secs(retain_until as i64)
                .fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime)
                .unwrap()
        );
        let retention_url = format!(
            "{}/{}/{}?retention&versionId={}",
            CTX.endpoint(),
            bucket,
            key,
            url::form_urlencoded::byte_serialize(version_id.as_bytes()).collect::<String>()
        );
        assert_missing_checksum_rejected(
            "PutObjectRetention",
            "PUT",
            &retention_url,
            retention_body.as_bytes(),
            &[],
            REQUIRED_CHECKSUM_MESSAGE,
        );
        let md5_headers = vec![content_md5_header(retention_body.as_bytes())];
        assert_request_succeeds(
            "PutObjectRetention",
            "PUT",
            &retention_url,
            retention_body.as_bytes(),
            &md5_headers,
        );
        let sdk_headers = sdk_checksum_headers(retention_body.as_bytes());
        assert_request_succeeds(
            "PutObjectRetention",
            "PUT",
            &retention_url,
            retention_body.as_bytes(),
            &sdk_headers,
        );
        cleanup_object_lock_bucket_with_version(&bucket, key, &version_id).await;
    });
}

#[test]
fn test_delete_objects_request_checksum_matrix() {
    s3_tests::run(async {
        let bucket = create_bucket_in_test_region(None, false).await;
        let key = "delete-me";

        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        let body = format!("<Delete><Object><Key>{key}</Key></Object></Delete>");
        let url = format!("{}/{}?delete", CTX.endpoint(), bucket);

        let missing = send_signed_request(
            "POST",
            &url,
            body.as_bytes(),
            std::iter::empty::<(&str, &str)>(),
        );
        assert_eq!(missing.status, 400, "body: {}", missing.body);
        assert_error_code(&missing.body, "InvalidRequest");
        assert_error_message(&missing.body, REQUIRED_CHECKSUM_MESSAGE);

        let md5_headers = [content_md5_header(body.as_bytes())];
        let md5_ok = send_signed_request(
            "POST",
            &url,
            body.as_bytes(),
            md5_headers
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str())),
        );
        assert_eq!(md5_ok.status, 200, "body: {}", md5_ok.body);

        let sdk_headers = sdk_checksum_headers(body.as_bytes());
        let sdk_ok = send_signed_request(
            "POST",
            &url,
            body.as_bytes(),
            sdk_headers
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str())),
        );
        assert_eq!(sdk_ok.status, 200, "body: {}", sdk_ok.body);

        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_allow_missing_checksum_exceptions() {
    s3_tests::run(async {
        let bucket = create_bucket_in_test_region(None, false).await;
        let key = "plain-put";
        let put_url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);
        let put = send_signed_request(
            "PUT",
            &put_url,
            b"hello",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_eq!(put.status, 200, "body: {}", put.body);
        cleanup_bucket_with_keys(&bucket, &[key]).await;

        let bucket = create_bucket_in_test_region(None, false).await;
        let key = "tagging";
        CTX.client()
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();
        let tagging_body =
            br#"<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>"#;
        let tagging_url = format!("{}/{}/{}?tagging", CTX.endpoint(), bucket, key);
        let tagging_put = send_signed_request(
            "PUT",
            &tagging_url,
            tagging_body,
            std::iter::empty::<(&str, &str)>(),
        );
        assert_eq!(tagging_put.status, 200, "body: {}", tagging_put.body);
        cleanup_bucket_with_keys(&bucket, &[key]).await;

        let bucket = create_bucket_in_test_region(None, true).await;
        let lock_key = "locked-put";
        let lock_url = format!("{}/{}/{}", CTX.endpoint(), bucket, lock_key);
        let retain_until = aws_sdk_s3::primitives::DateTime::from_secs(
            (SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 24 * 60 * 60) as i64,
        )
        .fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime)
        .unwrap();
        let locked_put = send_signed_request(
            "PUT",
            &lock_url,
            b"hello",
            [
                ("x-amz-object-lock-mode", "GOVERNANCE"),
                ("x-amz-object-lock-retain-until-date", retain_until.as_str()),
            ],
        );
        assert_eq!(locked_put.status, 400, "body: {}", locked_put.body);
        assert_error_code(&locked_put.body, "InvalidRequest");
        assert_error_message(&locked_put.body, OBJECT_LOCK_PUT_REQUIRED_CHECKSUM_MESSAGE);
        cleanup_bucket(&bucket).await;

        let bucket = create_bucket_in_test_region(None, true).await;
        let key = "locked-put-md5";
        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);
        let retain_until = aws_sdk_s3::primitives::DateTime::from_secs(
            (SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 24 * 60 * 60) as i64,
        )
        .fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime)
        .unwrap();
        let body = b"hello";
        let mut headers = vec![
            (
                "x-amz-object-lock-mode".to_string(),
                "GOVERNANCE".to_string(),
            ),
            (
                "x-amz-object-lock-retain-until-date".to_string(),
                retain_until.clone(),
            ),
        ];
        headers.push(content_md5_header(body));
        assert_request_succeeds(
            "PutObject with Object Lock Content-MD5",
            "PUT",
            &url,
            body,
            &headers,
        );
        let version_id = current_version_id(&bucket, key).await;
        cleanup_object_lock_bucket_with_version(&bucket, key, &version_id).await;

        let bucket = create_bucket_in_test_region(None, true).await;
        let key = "locked-put-sdk";
        let url = format!("{}/{}/{}", CTX.endpoint(), bucket, key);
        let retain_until = aws_sdk_s3::primitives::DateTime::from_secs(
            (SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 24 * 60 * 60) as i64,
        )
        .fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime)
        .unwrap();
        let body = b"hello";
        let mut headers = vec![
            (
                "x-amz-object-lock-mode".to_string(),
                "GOVERNANCE".to_string(),
            ),
            (
                "x-amz-object-lock-retain-until-date".to_string(),
                retain_until,
            ),
        ];
        headers.extend(sdk_checksum_headers(body));
        assert_request_succeeds(
            "PutObject with Object Lock checksum family",
            "PUT",
            &url,
            body,
            &headers,
        );
        let version_id = current_version_id(&bucket, key).await;
        cleanup_object_lock_bucket_with_version(&bucket, key, &version_id).await;

        let bucket = create_bucket_in_test_region(None, false).await;
        let key = "multipart";
        let upload = CTX
            .client()
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = upload.upload_id().unwrap();
        let part_url = format!(
            "{}/{}/{}?partNumber=1&uploadId={}",
            CTX.endpoint(),
            bucket,
            key,
            url::form_urlencoded::byte_serialize(upload_id.as_bytes()).collect::<String>()
        );
        let upload_part = send_signed_request(
            "PUT",
            &part_url,
            b"hello multipart",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_eq!(upload_part.status, 200, "body: {}", upload_part.body);
        abort_multipart_upload(&bucket, key, upload_id).await;
        cleanup_bucket(&bucket).await;

        let bucket = create_bucket_in_test_region(None, false).await;
        let key = "multipart-md5";
        let upload = CTX
            .client()
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = upload.upload_id().unwrap();
        let part_url = format!(
            "{}/{}/{}?partNumber=1&uploadId={}",
            CTX.endpoint(),
            bucket,
            key,
            url::form_urlencoded::byte_serialize(upload_id.as_bytes()).collect::<String>()
        );
        let body = b"hello multipart";
        let md5_headers = [content_md5_header(body)];
        assert_request_succeeds(
            "UploadPart with Content-MD5",
            "PUT",
            &part_url,
            body,
            &md5_headers,
        );
        abort_multipart_upload(&bucket, key, upload_id).await;
        cleanup_bucket(&bucket).await;

        let bucket = create_bucket_in_test_region(None, false).await;
        let key = "multipart-sdk";
        let upload = CTX
            .client()
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Crc32)
            .send()
            .await
            .unwrap();
        let upload_id = upload.upload_id().unwrap();
        let part_url = format!(
            "{}/{}/{}?partNumber=1&uploadId={}",
            CTX.endpoint(),
            bucket,
            key,
            url::form_urlencoded::byte_serialize(upload_id.as_bytes()).collect::<String>()
        );
        let body = b"hello multipart";
        let sdk_headers = sdk_checksum_headers(body);
        assert_request_succeeds(
            "UploadPart with checksum family",
            "PUT",
            &part_url,
            body,
            &sdk_headers,
        );
        abort_multipart_upload(&bucket, key, upload_id).await;
        cleanup_bucket(&bucket).await;

        let bucket = create_bucket_in_test_region(None, false).await;
        let key = "complete";
        let upload = CTX
            .client()
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        let upload_id = upload.upload_id().unwrap().to_string();
        let part = CTX
            .client()
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"hello multipart"))
            .send()
            .await
            .unwrap();
        let complete_body = format!(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
            part.e_tag().unwrap()
        );
        let complete_url = format!(
            "{}/{}/{}?uploadId={}",
            CTX.endpoint(),
            bucket,
            key,
            url::form_urlencoded::byte_serialize(upload_id.as_bytes()).collect::<String>()
        );
        let complete = send_signed_request(
            "POST",
            &complete_url,
            complete_body.as_bytes(),
            std::iter::empty::<(&str, &str)>(),
        );
        assert_eq!(complete.status, 200, "body: {}", complete.body);
        cleanup_bucket_with_keys(&bucket, &[key]).await;
    });
}

#[test]
fn test_checksum_algorithm_without_value_header_is_rejected() {
    s3_tests::run(async {
        let bucket = create_bucket_in_test_region(None, false).await;
        let url = format!("{}/{}?lifecycle", CTX.endpoint(), bucket);
        let body = br#"<LifecycleConfiguration><Rule><ID>rule1</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>"#;
        let response =
            send_signed_request("PUT", &url, body, [("x-amz-checksum-algorithm", "CRC32")]);
        assert_eq!(response.status, 400, "body: {}", response.body);
        assert_error_code(&response.body, "InvalidRequest");
        assert_error_message(&response.body, LIFECYCLE_REQUIRED_CHECKSUM_MESSAGE);
        cleanup_bucket(&bucket).await;
    });
}

#[test]
fn test_oversized_xml_body_limits_match_aws() {
    s3_tests::run(async {
        let versioning_pad = comment_pad(600);
        let acl_pad = comment_pad(70_000);
        let encryption_pad = comment_pad(700_000);
        let tagging_pad = comment_pad(41_000);
        let ownership_pad = comment_pad(1_900);
        let public_access_block_pad = comment_pad(420_000);
        let object_lock_pad = comment_pad(700_000);
        let lifecycle_pad = comment_pad(1024 * 1024);
        let delete_pad = comment_pad(1024 * 1024);

        let bucket = create_bucket_in_test_region(None, false).await;
        let versioning_url = format!("{}/{}?versioning", CTX.endpoint(), bucket);
        let versioning_body = format!(
            "<VersioningConfiguration>{versioning_pad}<Status>Enabled</Status>{versioning_pad}</VersioningConfiguration>"
        )
        .into_bytes();
        let versioning = send_signed_request(
            "PUT",
            &versioning_url,
            &versioning_body,
            [content_md5_header(&versioning_body)],
        );
        cleanup_bucket(&bucket).await;
        assert_eq!(versioning.status, 400, "body: {}", versioning.body);
        assert_max_message_length(&versioning.body, 1024);

        let bucket =
            create_bucket_in_test_region(Some(ObjectOwnership::BucketOwnerPreferred), false).await;
        let owner_id = bucket_owner_id(&bucket).await;
        let acl_url = format!("{}/{}?acl", CTX.endpoint(), bucket);
        let acl_body = format!(
            "<AccessControlPolicy>{acl_pad}<Owner><ID>{owner_id}</ID></Owner>{acl_pad}<AccessControlList>{acl_pad}\
             <Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">\
             <ID>{owner_id}</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant>{acl_pad}\
             </AccessControlList>{acl_pad}</AccessControlPolicy>"
        )
        .into_bytes();
        let acl = send_signed_request(
            "PUT",
            &acl_url,
            &acl_body,
            std::iter::empty::<(&str, &str)>(),
        );
        cleanup_bucket(&bucket).await;
        assert_eq!(acl.status, 400, "body: {}", acl.body);
        assert_max_message_length(&acl.body, 204_800);

        let bucket = create_bucket_in_test_region(None, false).await;
        let encryption_url = format!("{}/{}?encryption", CTX.endpoint(), bucket);
        let encryption_body = format!(
            "<ServerSideEncryptionConfiguration>{encryption_pad}<Rule>{encryption_pad}\
             <ApplyServerSideEncryptionByDefault>{encryption_pad}<SSEAlgorithm>AES256</SSEAlgorithm>{encryption_pad}\
             </ApplyServerSideEncryptionByDefault>{encryption_pad}</Rule>{encryption_pad}\
             </ServerSideEncryptionConfiguration>"
        )
        .into_bytes();
        let encryption = send_signed_request(
            "PUT",
            &encryption_url,
            &encryption_body,
            std::iter::empty::<(&str, &str)>(),
        );
        cleanup_bucket(&bucket).await;
        assert_eq!(encryption.status, 400, "body: {}", encryption.body);
        assert_max_message_length(&encryption.body, 2_097_152);

        let bucket = create_bucket_in_test_region(None, false).await;
        let tagging_url = format!("{}/{}?tagging", CTX.endpoint(), bucket);
        let tagging_body = format!(
            "<Tagging>{tagging_pad}<TagSet>{tagging_pad}<Tag><Key>a</Key><Value>b</Value></Tag>{tagging_pad}</TagSet>{tagging_pad}</Tagging>"
        )
        .into_bytes();
        let tagging = send_signed_request(
            "PUT",
            &tagging_url,
            &tagging_body,
            [content_md5_header(&tagging_body)],
        );
        cleanup_bucket(&bucket).await;
        assert_eq!(tagging.status, 400, "body: {}", tagging.body);
        assert_max_message_length(&tagging.body, 163_840);

        let bucket = create_bucket_in_test_region(None, false).await;
        let ownership_url = format!("{}/{}?ownershipControls", CTX.endpoint(), bucket);
        let ownership_body = format!(
            "<OwnershipControls>{ownership_pad}<Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule>{ownership_pad}</OwnershipControls>"
        )
        .into_bytes();
        let ownership = send_signed_request(
            "PUT",
            &ownership_url,
            &ownership_body,
            [content_md5_header(&ownership_body)],
        );
        cleanup_bucket(&bucket).await;
        assert_eq!(ownership.status, 400, "body: {}", ownership.body);
        assert_max_message_length(&ownership.body, 2048);

        let bucket = create_bucket_in_test_region(None, false).await;
        let pab_url = format!("{}/{}?publicAccessBlock", CTX.endpoint(), bucket);
        let pab_body = format!(
            "<PublicAccessBlockConfiguration>{public_access_block_pad}<BlockPublicAcls>true</BlockPublicAcls>{public_access_block_pad}<IgnorePublicAcls>true</IgnorePublicAcls>{public_access_block_pad}<BlockPublicPolicy>true</BlockPublicPolicy>{public_access_block_pad}<RestrictPublicBuckets>true</RestrictPublicBuckets>{public_access_block_pad}</PublicAccessBlockConfiguration>"
        )
        .into_bytes();
        let pab = send_signed_request("PUT", &pab_url, &pab_body, [content_md5_header(&pab_body)]);
        cleanup_bucket(&bucket).await;
        assert_eq!(pab.status, 400, "body: {}", pab.body);
        assert_max_message_length(&pab.body, 2_097_152);

        let bucket = create_bucket_in_test_region(None, true).await;
        let object_lock_url = format!("{}/{}?object-lock", CTX.endpoint(), bucket);
        let object_lock_body = format!(
            "<ObjectLockConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{object_lock_pad}<ObjectLockEnabled>Enabled</ObjectLockEnabled>{object_lock_pad}<Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days></DefaultRetention></Rule>{object_lock_pad}</ObjectLockConfiguration>"
        )
        .into_bytes();
        let object_lock = send_signed_request(
            "PUT",
            &object_lock_url,
            &object_lock_body,
            [content_md5_header(&object_lock_body)],
        );
        cleanup_bucket(&bucket).await;
        assert_eq!(object_lock.status, 400, "body: {}", object_lock.body);
        assert_max_message_length(&object_lock.body, 2_097_152);

        let bucket = create_bucket_in_test_region(None, false).await;
        let lifecycle_url = format!("{}/{}?lifecycle", CTX.endpoint(), bucket);
        let lifecycle_body = format!(
            "<LifecycleConfiguration>{lifecycle_pad}<Rule><ID>rule1</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule>{lifecycle_pad}</LifecycleConfiguration>"
        )
        .into_bytes();
        let lifecycle = send_signed_request(
            "PUT",
            &lifecycle_url,
            &lifecycle_body,
            [content_md5_header(&lifecycle_body)],
        );
        cleanup_bucket(&bucket).await;
        assert_eq!(lifecycle.status, 400, "body: {}", lifecycle.body);
        assert_max_message_length(&lifecycle.body, 2_097_152);

        let bucket = create_bucket_in_test_region(None, false).await;
        let delete_url = format!("{}/{}?delete", CTX.endpoint(), bucket);
        let delete_body = format!(
            "<Delete>{delete_pad}<Object><Key>delete-me</Key></Object>{delete_pad}</Delete>"
        )
        .into_bytes();
        let delete = send_signed_request(
            "POST",
            &delete_url,
            &delete_body,
            [content_md5_header(&delete_body)],
        );
        cleanup_bucket(&bucket).await;
        assert_eq!(delete.status, 400, "body: {}", delete.body);
        assert_max_message_length(&delete.body, 2_048_000);
    });
}

#[test]
fn test_complete_multipart_upload_body_limit_matches_aws() {
    s3_tests::run(async {
        let cases = [
            ("pad-1310k", 1_310_000usize, true),
            ("pad-1400k", 1_400_000usize, false),
        ];

        for (label, pad_size, should_succeed) in cases {
            let bucket = create_bucket_in_test_region(None, false).await;
            let key = format!("complete-{label}");
            let upload = CTX
                .client()
                .create_multipart_upload()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
                .unwrap();
            let upload_id = upload.upload_id().unwrap().to_string();
            let part = CTX
                .client()
                .upload_part()
                .bucket(&bucket)
                .key(&key)
                .upload_id(&upload_id)
                .part_number(1)
                .body(ByteStream::from_static(b"hello multipart"))
                .send()
                .await
                .unwrap();
            let etag = part.e_tag().unwrap();
            let pad = comment_pad(pad_size);
            let body = format!(
                "<CompleteMultipartUpload>{pad}<Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part>{pad}</CompleteMultipartUpload>"
            );
            let complete_url = format!(
                "{}/{}/{}?uploadId={}",
                CTX.endpoint(),
                bucket,
                key,
                url::form_urlencoded::byte_serialize(upload_id.as_bytes()).collect::<String>()
            );
            let response = send_signed_request(
                "POST",
                &complete_url,
                body.as_bytes(),
                [content_md5_header(body.as_bytes())],
            );

            if should_succeed {
                assert_eq!(response.status, 200, "body: {}", response.body);
                cleanup_bucket_with_keys(&bucket, &[key.as_str()]).await;
            } else {
                assert_eq!(response.status, 400, "body: {}", response.body);
                assert_max_message_length(&response.body, 2_621_440);
                abort_multipart_upload(&bucket, &key, &upload_id).await;
                cleanup_bucket(&bucket).await;
            }
        }
    });
}
