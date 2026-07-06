use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use s3_diff_tests::require_external_diff_test_env;
use s3_tests::{
    aws_sdk_s3::{
        self,
        primitives::{ByteStream, DateTime},
        types::{
            BucketLocationConstraint, BucketVersioningStatus, CreateBucketConfiguration,
            DefaultRetention, ObjectLockConfiguration, ObjectLockEnabled, ObjectLockLegalHold,
            ObjectLockLegalHoldStatus, ObjectLockMode, ObjectLockRetention,
            ObjectLockRetentionMode, ObjectLockRule, Tag, Tagging, VersioningConfiguration,
        },
        Client,
    },
    build_client_with_ca, cleanup_versioned_bucket, content_md5_header,
    create_bucket_with_sse_c_enabled, delete_all_and_bucket,
    post_object_raw_to_test_endpoint_with_headers, send_signed_request_with_credentials,
    sigv4_post_fields_for_credentials, sse_c_header_values, test_sse_c_key, unique_bucket,
    RawResponse, SignedRequestCredentials, TestServer, CTX,
};
use s3_types::is_legacy_create_bucket_region;

const COMMON_TRANSPORT_IGNORED_HEADERS: &[&str] = &["connection", "date", "server"];
const COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS: &[&str] =
    &["connection", "date", "server", "etag"];
const COMMON_TRANSPORT_AND_TRANSFER_ENCODING_IGNORED_HEADERS: &[&str] =
    &["connection", "date", "server", "transfer-encoding"];
const COMMON_PRESENCE_ONLY_HEADERS: &[&str] = &["last-modified"];
const AWS_MIN_MULTIPART_PART_SIZE: usize = 5 * 1024 * 1024;
const REQUEST_ID_HEADER_NAME: &str = "x-amz-request-id";
const HOST_ID_HEADER_NAME: &str = "x-amz-id-2";

fn is_aws_request_id_shape(value: &str) -> bool {
    value.len() == 16
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || b.is_ascii_uppercase())
}

fn is_aws_host_id_shape(value: &str) -> bool {
    value.len() >= 40
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/' | b'='))
}

fn normalize_id_value(name: &str, value: &str) -> String {
    match name {
        REQUEST_ID_HEADER_NAME | "RequestId" if is_aws_request_id_shape(value) => {
            "<aws-request-id>".to_string()
        }
        HOST_ID_HEADER_NAME | "HostId" if is_aws_host_id_shape(value) => {
            "<aws-host-id>".to_string()
        }
        _ => value.to_string(),
    }
}

struct ComparisonEnv {
    external_client: Client,
    external_region: String,
    local_client: Client,
    local_server: TestServer,
}

impl ComparisonEnv {
    async fn setup() -> Option<Self> {
        require_external_diff_test_env();

        let _ = &*CTX;
        let external_region = CTX.region().to_string();
        let local_server = TestServer::start_https_in_region(&external_region).await;
        let local_client = build_client_with_ca(
            local_server.endpoint(),
            s3_tests::server::TEST_ACCESS_KEY,
            s3_tests::server::TEST_SECRET_KEY,
            &external_region,
            local_server.tls_ca_pem(),
        );

        Some(Self {
            external_client: CTX.client().clone(),
            external_region,
            local_client,
            local_server,
        })
    }

    async fn create_bucket_pair(&self) -> (String, String) {
        let external_bucket = unique_bucket();
        let local_bucket = external_bucket.clone();
        create_bucket_in_region(
            &self.external_client,
            &external_bucket,
            &self.external_region,
        )
        .await;
        create_bucket_in_region(&self.local_client, &local_bucket, &self.external_region).await;
        (external_bucket, local_bucket)
    }

    fn send_external<K, V, I>(
        &self,
        method: &str,
        bucket: &str,
        key: &str,
        query: Option<&str>,
        body: &[u8],
        extra_headers: I,
    ) -> RawResponse
    where
        K: AsRef<str>,
        V: AsRef<str>,
        I: IntoIterator<Item = (K, V)>,
    {
        send_signed_request_with_credentials(
            method,
            &object_url(CTX.endpoint(), bucket, key, query),
            body,
            extra_headers,
            SignedRequestCredentials {
                access_key: CTX.access_key(),
                secret_key: CTX.secret_key(),
                region: &self.external_region,
                tls_ca_pem: None,
            },
        )
    }

    fn send_local<K, V, I>(
        &self,
        method: &str,
        bucket: &str,
        key: &str,
        query: Option<&str>,
        body: &[u8],
        extra_headers: I,
    ) -> RawResponse
    where
        K: AsRef<str>,
        V: AsRef<str>,
        I: IntoIterator<Item = (K, V)>,
    {
        send_signed_request_with_credentials(
            method,
            &object_url(self.local_server.endpoint(), bucket, key, query),
            body,
            extra_headers,
            SignedRequestCredentials {
                access_key: s3_tests::server::TEST_ACCESS_KEY,
                secret_key: s3_tests::server::TEST_SECRET_KEY,
                region: &self.external_region,
                tls_ca_pem: self.local_server.tls_ca_pem(),
            },
        )
    }
}

fn object_url(endpoint: &str, bucket: &str, key: &str, query: Option<&str>) -> String {
    let path = if key.is_empty() {
        format!("{endpoint}/{bucket}")
    } else {
        format!("{endpoint}/{bucket}/{key}")
    };
    match query {
        Some(query) => format!("{path}?{query}"),
        None => path,
    }
}

fn alternate_region(region: &str) -> &str {
    if region == "us-east-1" {
        "us-west-2"
    } else {
        "us-east-1"
    }
}

async fn create_bucket_in_region(client: &Client, bucket: &str, region: &str) {
    let mut request = client.create_bucket().bucket(bucket);
    if !is_legacy_create_bucket_region(region) {
        request = request.create_bucket_configuration(
            CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::from(region))
                .build(),
        );
    }
    request.send().await.expect("create bucket");
}

async fn create_sse_c_enabled_bucket_pair(env: &ComparisonEnv) -> (String, String) {
    let external_bucket = unique_bucket();
    let local_bucket = external_bucket.clone();
    create_bucket_with_sse_c_enabled(&env.external_client, &external_bucket)
        .await
        .expect("create external SSE-C-enabled bucket");
    create_bucket_with_sse_c_enabled(&env.local_client, &local_bucket)
        .await
        .expect("create local SSE-C-enabled bucket");
    (external_bucket, local_bucket)
}

fn sse_c_headers<'a>(key_b64: &'a str, key_md5_b64: &'a str) -> [(&'a str, &'a str); 3] {
    [
        ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
        ("x-amz-server-side-encryption-customer-key", key_b64),
        ("x-amz-server-side-encryption-customer-key-md5", key_md5_b64),
    ]
}

async fn create_object_lock_bucket_in_region(client: &Client, bucket: &str, region: &str) {
    let mut request = client
        .create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true);
    if !is_legacy_create_bucket_region(region) {
        request = request.create_bucket_configuration(
            CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::from(region))
                .build(),
        );
    }
    request.send().await.expect("create object lock bucket");
}

async fn enable_bucket_versioning(client: &Client, bucket: &str) {
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("enable bucket versioning");
}

async fn create_object_lock_bucket_pair(env: &ComparisonEnv) -> (String, String) {
    let external_bucket = unique_bucket();
    let local_bucket = unique_bucket();
    create_object_lock_bucket_in_region(
        &env.external_client,
        &external_bucket,
        &env.external_region,
    )
    .await;
    create_object_lock_bucket_in_region(
        &env.local_client,
        &local_bucket,
        s3_tests::server::TEST_REGION,
    )
    .await;
    (external_bucket, local_bucket)
}

async fn create_matching_object_lock_bucket_pair(env: &ComparisonEnv) -> (String, String) {
    let external_bucket = unique_bucket();
    let local_bucket = external_bucket.clone();
    create_object_lock_bucket_in_region(
        &env.external_client,
        &external_bucket,
        &env.external_region,
    )
    .await;
    create_object_lock_bucket_in_region(&env.local_client, &local_bucket, &env.external_region)
        .await;
    (external_bucket, local_bucket)
}

async fn put_object_pair(
    env: &ComparisonEnv,
    external_bucket: &str,
    local_bucket: &str,
    key: &str,
    body: &'static [u8],
) {
    env.external_client
        .put_object()
        .bucket(external_bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await
        .expect("put external fixture object");
    env.local_client
        .put_object()
        .bucket(local_bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await
        .expect("put local fixture object");
}

fn future_datetime(seconds_from_now: u64) -> DateTime {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs() as i64;
    DateTime::from_secs(now + seconds_from_now as i64)
}

fn object_lock_legal_hold(status: ObjectLockLegalHoldStatus) -> ObjectLockLegalHold {
    ObjectLockLegalHold::builder().status(status).build()
}

async fn cleanup_object_lock_bucket(client: &Client, bucket: &str) {
    loop {
        let versions = client
            .list_object_versions()
            .bucket(bucket)
            .send()
            .await
            .expect("list object lock bucket versions");

        if versions.versions().is_empty() && versions.delete_markers().is_empty() {
            client
                .delete_bucket()
                .bucket(bucket)
                .send()
                .await
                .expect("delete object lock bucket");
            return;
        }

        for marker in versions.delete_markers() {
            client
                .delete_object()
                .bucket(bucket)
                .key(marker.key().expect("delete marker key"))
                .version_id(marker.version_id().expect("delete marker version id"))
                .send()
                .await
                .expect("delete object lock delete marker");
        }

        for version in versions.versions() {
            let key = version.key().expect("version key");
            let version_id = version.version_id().expect("version id");
            let head = client
                .head_object()
                .bucket(bucket)
                .key(key)
                .version_id(version_id)
                .send()
                .await
                .expect("head object lock version");
            if head.object_lock_legal_hold_status() == Some(&ObjectLockLegalHoldStatus::On) {
                client
                    .put_object_legal_hold()
                    .bucket(bucket)
                    .key(key)
                    .version_id(version_id)
                    .legal_hold(object_lock_legal_hold(ObjectLockLegalHoldStatus::Off))
                    .send()
                    .await
                    .expect("disable legal hold for cleanup");
            }
            client
                .delete_object()
                .bucket(bucket)
                .key(key)
                .version_id(version_id)
                .bypass_governance_retention(true)
                .send()
                .await
                .expect("delete object lock version");
        }
    }
}

fn normalized_headers(
    headers: &[(String, String)],
    ignored_headers: &[&str],
    presence_only_headers: &[&str],
) -> BTreeMap<String, Vec<String>> {
    let mut normalized = BTreeMap::new();
    for (name, value) in headers {
        let name = name.to_ascii_lowercase();
        if ignored_headers.contains(&name.as_str()) {
            continue;
        }
        let value = if presence_only_headers.contains(&name.as_str()) {
            "<present>".to_string()
        } else {
            normalize_id_value(&name, value)
        };
        normalized.entry(name).or_insert_with(Vec::new).push(value);
    }
    for values in normalized.values_mut() {
        values.sort();
    }
    normalized
}

fn assert_response_shape_matches(
    operation: &str,
    aws: &RawResponse,
    local: &RawResponse,
    ignored_headers: &[&str],
    presence_only_headers: &[&str],
) {
    assert_eq!(
        local.status, aws.status,
        "{operation}: status mismatch\naws: {aws:?}\nlocal: {local:?}"
    );
    assert_eq!(
        normalized_headers(&local.headers, ignored_headers, presence_only_headers),
        normalized_headers(&aws.headers, ignored_headers, presence_only_headers),
        "{operation}: normalized header mismatch\naws headers: {:?}\nlocal headers: {:?}",
        normalized_headers(&aws.headers, ignored_headers, presence_only_headers),
        normalized_headers(&local.headers, ignored_headers, presence_only_headers),
    );
    assert_eq!(
        local.body, aws.body,
        "{operation}: body mismatch\naws body: {}\nlocal body: {}",
        aws.body, local.body
    );
}

fn assert_response_headers_match(
    operation: &str,
    aws: &RawResponse,
    local: &RawResponse,
    ignored_headers: &[&str],
    presence_only_headers: &[&str],
) {
    assert_eq!(
        local.status, aws.status,
        "{operation}: status mismatch\naws: {aws:?}\nlocal: {local:?}"
    );
    assert_eq!(
        normalized_headers(&local.headers, ignored_headers, presence_only_headers),
        normalized_headers(&aws.headers, ignored_headers, presence_only_headers),
        "{operation}: normalized header mismatch\naws headers: {:?}\nlocal headers: {:?}",
        normalized_headers(&aws.headers, ignored_headers, presence_only_headers),
        normalized_headers(&local.headers, ignored_headers, presence_only_headers),
    );
}

fn extract_xml_text(body: &str, tag: &str) -> Option<String> {
    let start_tag = format!("<{tag}>");
    let end_tag = format!("</{tag}>");
    let start = body.find(&start_tag)? + start_tag.len();
    let end = body[start..].find(&end_tag)? + start;
    Some(body[start..end].to_string())
}

fn replace_xml_text(body: &str, tag: &str, replacement: &str) -> String {
    let start_tag = format!("<{tag}>");
    let end_tag = format!("</{tag}>");
    let Some(start) = body.find(&start_tag) else {
        return body.to_string();
    };
    let content_start = start + start_tag.len();
    let Some(relative_end) = body[content_start..].find(&end_tag) else {
        return body.to_string();
    };
    let end = content_start + relative_end;
    let mut out = body.to_string();
    out.replace_range(content_start..end, replacement);
    out
}

fn assert_response_id_shapes(operation: &str, response: &RawResponse) {
    if let Some(request_id) = response_header_value(response, REQUEST_ID_HEADER_NAME) {
        assert!(
            is_aws_request_id_shape(request_id),
            "{operation}: invalid x-amz-request-id shape: {request_id}"
        );
    }
    if let Some(host_id) = response_header_value(response, HOST_ID_HEADER_NAME) {
        assert!(
            is_aws_host_id_shape(host_id),
            "{operation}: invalid x-amz-id-2 shape: {host_id}"
        );
    }
}

fn assert_xml_error_ids_match_headers(operation: &str, response: &RawResponse) {
    let header_request_id = response_header_value(response, REQUEST_ID_HEADER_NAME);
    let header_host_id = response_header_value(response, HOST_ID_HEADER_NAME);
    let xml_request_id = extract_xml_text(&response.body, "RequestId");
    let xml_host_id = extract_xml_text(&response.body, "HostId");

    if let (Some(header_request_id), Some(xml_request_id)) =
        (header_request_id, xml_request_id.as_deref())
    {
        assert_eq!(
            xml_request_id, header_request_id,
            "{operation}: RequestId XML/header mismatch\nresponse: {response:?}"
        );
    }
    if let (Some(header_host_id), Some(xml_host_id)) = (header_host_id, xml_host_id.as_deref()) {
        assert_eq!(
            xml_host_id, header_host_id,
            "{operation}: HostId XML/header mismatch\nresponse: {response:?}"
        );
    }
}

fn normalize_xml_text_tags(body: &str, tags: &[&str]) -> String {
    let mut normalized = body.to_string();
    for tag in tags {
        let start_tag = format!("<{tag}>");
        let end_tag = format!("</{tag}>");
        let mut search_from = 0;
        while let Some(relative_start) = normalized[search_from..].find(&start_tag) {
            let start = search_from + relative_start;
            let content_start = start + start_tag.len();
            let Some(relative_end) = normalized[content_start..].find(&end_tag) else {
                break;
            };
            let replacement = format!("{start_tag}<present>{end_tag}");
            let end = content_start + relative_end + end_tag.len();
            normalized.replace_range(start..end, &replacement);
            search_from = start + replacement.len();
        }
    }
    normalized
}

fn normalize_sse_c_blocked_access_denied_message(body: &str) -> String {
    let Some(message) = extract_xml_text(body, "Message") else {
        return body.to_string();
    };
    const MARKER: &str = " is not authorized to perform: s3:PutObject on resource: \"";
    let Some(marker_index) = message.find(MARKER) else {
        return body.to_string();
    };
    let normalized_message = format!("User: <principal>{}", &message[marker_index..]);
    replace_xml_text(body, "Message", &normalized_message)
}

fn assert_xml_response_shape_matches(
    operation: &str,
    aws: &RawResponse,
    local: &RawResponse,
    ignored_headers: &[&str],
    presence_only_headers: &[&str],
    presence_only_xml_tags: &[&str],
) {
    assert_response_id_shapes(operation, aws);
    assert_response_id_shapes(operation, local);
    assert_response_headers_match(
        operation,
        aws,
        local,
        ignored_headers,
        presence_only_headers,
    );
    assert_eq!(
        normalize_xml_text_tags(&local.body, presence_only_xml_tags),
        normalize_xml_text_tags(&aws.body, presence_only_xml_tags),
        "{operation}: normalized XML body mismatch\naws body: {}\nlocal body: {}",
        aws.body,
        local.body,
    );
    assert_xml_error_ids_match_headers(operation, aws);
    assert_xml_error_ids_match_headers(operation, local);
}

fn extract_xml_blocks(body: &str, tag: &str) -> Vec<String> {
    let start_tag = format!("<{tag}>");
    let end_tag = format!("</{tag}>");
    let mut blocks = Vec::new();
    let mut search_from = 0;

    while let Some(relative_start) = body[search_from..].find(&start_tag) {
        let start = search_from + relative_start;
        let content_start = start + start_tag.len();
        let Some(relative_end) = body[content_start..].find(&end_tag) else {
            break;
        };
        let end = content_start + relative_end + end_tag.len();
        blocks.push(body[start..end].to_string());
        search_from = end;
    }

    blocks
}

fn assert_delete_objects_response_shape_matches(aws: &RawResponse, local: &RawResponse) {
    assert_response_headers_match(
        "DeleteObjects",
        aws,
        local,
        COMMON_TRANSPORT_IGNORED_HEADERS,
        COMMON_PRESENCE_ONLY_HEADERS,
    );

    let aws_root_end = aws
        .body
        .find('>')
        .expect("DeleteObjects AWS body should have a root tag")
        + 1;
    let local_root_end = local
        .body
        .find('>')
        .expect("DeleteObjects local body should have a root tag")
        + 1;
    let aws_deleted = extract_xml_blocks(&aws.body, "Deleted");
    let local_deleted = extract_xml_blocks(&local.body, "Deleted");
    let aws_errors = extract_xml_blocks(&aws.body, "Error");
    let local_errors = extract_xml_blocks(&local.body, "Error");

    assert_eq!(
        &local.body[..local_root_end],
        &aws.body[..aws_root_end],
        "DeleteObjects: root tag mismatch\naws body: {}\nlocal body: {}",
        aws.body,
        local.body,
    );
    assert!(
        aws.body.ends_with("</DeleteResult>") && local.body.ends_with("</DeleteResult>"),
        "DeleteObjects: expected DeleteResult root\naws body: {}\nlocal body: {}",
        aws.body,
        local.body,
    );

    let mut aws_deleted_sorted = aws_deleted;
    let mut local_deleted_sorted = local_deleted;
    let mut aws_errors_sorted = aws_errors;
    let mut local_errors_sorted = local_errors;
    aws_deleted_sorted.sort();
    local_deleted_sorted.sort();
    aws_errors_sorted.sort();
    local_errors_sorted.sort();

    assert_eq!(
        local_deleted_sorted, aws_deleted_sorted,
        "DeleteObjects: normalized Deleted entries mismatch\naws body: {}\nlocal body: {}",
        aws.body, local.body,
    );
    assert_eq!(
        local_errors_sorted, aws_errors_sorted,
        "DeleteObjects: normalized Error entries mismatch\naws body: {}\nlocal body: {}",
        aws.body, local.body,
    );
}

fn xml_tag_text<'a>(body: &'a str, tag: &str) -> Option<&'a str> {
    let start_tag = format!("<{tag}>");
    let end_tag = format!("</{tag}>");
    let start = body.find(&start_tag)? + start_tag.len();
    let end = body[start..].find(&end_tag)? + start;
    Some(&body[start..end])
}

fn response_header_value<'a>(response: &'a RawResponse, name: &str) -> Option<&'a str> {
    response
        .headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn normalize_location_header(value: &str) -> String {
    let (scheme, rest) = value
        .split_once("://")
        .expect("location header should contain a scheme");
    let path_start = rest.find('/').unwrap_or(rest.len());
    format!("{scheme}://<authority>{}", &rest[path_start..])
}

fn xml_text_unescape(text: &str) -> String {
    text.replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

fn multipart_part_checksum_headers(
    create_headers: &[(&str, &str)],
    body: &[u8],
) -> Vec<(String, String)> {
    let Some((_, algorithm)) = create_headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-amz-checksum-algorithm"))
    else {
        return Vec::new();
    };

    use base64::Engine;
    let value = match *algorithm {
        "CRC32" => base64::engine::general_purpose::STANDARD
            .encode(checksum::crc32::checksum(body).to_be_bytes()),
        "CRC32C" => base64::engine::general_purpose::STANDARD
            .encode(checksum::crc32c::checksum(body).to_be_bytes()),
        "CRC64NVME" => base64::engine::general_purpose::STANDARD
            .encode(checksum::crc64::checksum(body).to_be_bytes()),
        "SHA1" => {
            let digest = ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, body);
            base64::engine::general_purpose::STANDARD.encode(digest.as_ref())
        }
        "SHA256" => {
            let digest = ring::digest::digest(&ring::digest::SHA256, body);
            base64::engine::general_purpose::STANDARD.encode(digest.as_ref())
        }
        other => panic!("unsupported multipart checksum algorithm in fixture: {other}"),
    };
    let header_name = match *algorithm {
        "CRC32" => "x-amz-checksum-crc32",
        "CRC32C" => "x-amz-checksum-crc32c",
        "CRC64NVME" => "x-amz-checksum-crc64nvme",
        "SHA1" => "x-amz-checksum-sha1",
        "SHA256" => "x-amz-checksum-sha256",
        other => panic!("unsupported multipart checksum algorithm in fixture: {other}"),
    };
    vec![(header_name.to_string(), value)]
}

fn checksum_header_to_xml_tag(header_name: &str) -> Option<&'static str> {
    match header_name {
        "x-amz-checksum-crc32" => Some("ChecksumCRC32"),
        "x-amz-checksum-crc32c" => Some("ChecksumCRC32C"),
        "x-amz-checksum-crc64nvme" => Some("ChecksumCRC64NVME"),
        "x-amz-checksum-sha1" => Some("ChecksumSHA1"),
        "x-amz-checksum-sha256" => Some("ChecksumSHA256"),
        _ => None,
    }
}

async fn create_multipart_upload_pair(
    env: &ComparisonEnv,
    external_bucket: &str,
    local_bucket: &str,
    key: &str,
    create_headers: Vec<(&str, &str)>,
) -> (String, String) {
    let aws_create = env.send_external(
        "POST",
        external_bucket,
        key,
        Some("uploads="),
        b"",
        create_headers.clone(),
    );
    let local_create = env.send_local(
        "POST",
        local_bucket,
        key,
        Some("uploads="),
        b"",
        create_headers,
    );
    assert_eq!(aws_create.status, 200, "aws create multipart upload failed");
    assert_eq!(
        local_create.status, 200,
        "local create multipart upload failed"
    );
    let external_upload_id = xml_tag_text(&aws_create.body, "UploadId")
        .expect("external upload id")
        .to_string();
    let local_upload_id = xml_tag_text(&local_create.body, "UploadId")
        .expect("local upload id")
        .to_string();
    (external_upload_id, local_upload_id)
}

async fn upload_part_pair(
    env: &ComparisonEnv,
    buckets: (&str, &str),
    key: &str,
    upload_ids: (&str, &str),
    part_number: i32,
    body: &[u8],
) -> (String, String) {
    let (external_bucket, local_bucket) = buckets;
    let (external_upload_id, local_upload_id) = upload_ids;
    let aws_upload = env
        .external_client
        .upload_part()
        .bucket(external_bucket)
        .key(key)
        .upload_id(external_upload_id)
        .part_number(part_number)
        .body(ByteStream::from(body.to_vec()))
        .send()
        .await
        .expect("upload external part");
    let local_upload = env
        .local_client
        .upload_part()
        .bucket(local_bucket)
        .key(key)
        .upload_id(local_upload_id)
        .part_number(part_number)
        .body(ByteStream::from(body.to_vec()))
        .send()
        .await
        .expect("upload local part");
    (
        aws_upload.e_tag().expect("external part etag").to_string(),
        local_upload.e_tag().expect("local part etag").to_string(),
    )
}

async fn create_completed_multipart_pair(
    env: &ComparisonEnv,
    external_bucket: &str,
    local_bucket: &str,
    key: &str,
    create_headers: Vec<(&str, &str)>,
    part_bodies: &[&[u8]],
) {
    let (external_upload_id, local_upload_id) = create_multipart_upload_pair(
        env,
        external_bucket,
        local_bucket,
        key,
        create_headers.clone(),
    )
    .await;

    let mut external_part_etags = Vec::with_capacity(part_bodies.len());
    let mut local_part_etags = Vec::with_capacity(part_bodies.len());
    let mut external_part_checksum_xml = Vec::with_capacity(part_bodies.len());
    let mut local_part_checksum_xml = Vec::with_capacity(part_bodies.len());
    for (index, part_body) in part_bodies.iter().enumerate() {
        let part_number = index + 1;
        let part_headers = multipart_part_checksum_headers(&create_headers, part_body);
        let checksum_xml = part_headers
            .first()
            .and_then(|(header_name, value)| {
                checksum_header_to_xml_tag(header_name)
                    .map(|xml_tag| format!("<{xml_tag}>{value}</{xml_tag}>"))
            })
            .unwrap_or_default();
        let aws_upload = env.send_external(
            "PUT",
            external_bucket,
            key,
            Some(&format!(
                "partNumber={part_number}&uploadId={external_upload_id}"
            )),
            part_body,
            part_headers.clone(),
        );
        let local_upload = env.send_local(
            "PUT",
            local_bucket,
            key,
            Some(&format!(
                "partNumber={part_number}&uploadId={local_upload_id}"
            )),
            part_body,
            part_headers,
        );
        assert_eq!(
            aws_upload.status, 200,
            "aws upload part fixture {part_number} failed: {aws_upload:?}"
        );
        assert_eq!(
            local_upload.status, 200,
            "local upload part fixture {part_number} failed: {local_upload:?}"
        );
        external_part_etags.push(
            response_header_value(&aws_upload, "etag")
                .expect("external part etag")
                .to_string(),
        );
        local_part_etags.push(
            response_header_value(&local_upload, "etag")
                .expect("local part etag")
                .to_string(),
        );
        external_part_checksum_xml.push(checksum_xml.clone());
        local_part_checksum_xml.push(checksum_xml);
    }

    let external_complete_body = format!(
        "<CompleteMultipartUpload>{}</CompleteMultipartUpload>",
        external_part_etags
            .iter()
            .enumerate()
            .map(|(index, etag)| {
                format!(
                    "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag>{}</Part>",
                    index + 1,
                    etag,
                    external_part_checksum_xml[index]
                )
            })
            .collect::<String>()
    );
    let local_complete_body = format!(
        "<CompleteMultipartUpload>{}</CompleteMultipartUpload>",
        local_part_etags
            .iter()
            .enumerate()
            .map(|(index, etag)| {
                format!(
                    "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag>{}</Part>",
                    index + 1,
                    etag,
                    local_part_checksum_xml[index]
                )
            })
            .collect::<String>()
    );
    let aws_complete = env.send_external(
        "POST",
        external_bucket,
        key,
        Some(&format!("uploadId={external_upload_id}")),
        external_complete_body.as_bytes(),
        [("Content-Type", "application/xml")],
    );
    let local_complete = env.send_local(
        "POST",
        local_bucket,
        key,
        Some(&format!("uploadId={local_upload_id}")),
        local_complete_body.as_bytes(),
        [("Content-Type", "application/xml")],
    );
    assert_eq!(
        aws_complete.status, 200,
        "aws complete multipart fixture failed: {aws_complete:?}"
    );
    assert_eq!(
        local_complete.status, 200,
        "local complete multipart fixture failed: {local_complete:?}"
    );
}

#[test]
fn test_put_get_head_object_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let key = "shape-object.txt";
        let body = b"response-shape-body";
        let object_headers = vec![
            ("Content-Type", "text/plain"),
            ("Content-Encoding", "gzip"),
            (
                "Content-Disposition",
                "attachment; filename=\"shape-object.txt\"",
            ),
            ("Content-Language", "en-US"),
            ("Cache-Control", "max-age=60"),
            ("x-amz-meta-author", "alice"),
        ];

        let aws_put = env.send_external(
            "PUT",
            &external_bucket,
            key,
            None,
            body,
            object_headers.clone(),
        );
        let local_put = env.send_local(
            "PUT",
            &local_bucket,
            key,
            None,
            body,
            object_headers.clone(),
        );
        assert_response_shape_matches(
            "PutObject",
            &aws_put,
            &local_put,
            COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
        );

        let aws_get = env.send_external(
            "GET",
            &external_bucket,
            key,
            None,
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_get = env.send_local(
            "GET",
            &local_bucket,
            key,
            None,
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_response_shape_matches(
            "GetObject",
            &aws_get,
            &local_get,
            COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
        );

        let aws_head = env.send_external(
            "HEAD",
            &external_bucket,
            key,
            None,
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_head = env.send_local(
            "HEAD",
            &local_bucket,
            key,
            None,
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_response_shape_matches(
            "HeadObject",
            &aws_head,
            &local_head,
            COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[key.to_string()]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[key.to_string()]).await;
    });
}

#[test]
fn test_head_bucket_response_shape_matches_aws_when_regions_match() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };

        let (external_bucket, local_bucket) = env.create_bucket_pair().await;

        let aws_head = env.send_external(
            "HEAD",
            &external_bucket,
            "",
            None,
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_head = env.send_local(
            "HEAD",
            &local_bucket,
            "",
            None,
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_response_shape_matches(
            "HeadBucket",
            &aws_head,
            &local_head,
            COMMON_TRANSPORT_AND_TRANSFER_ENCODING_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[]).await;
    });
}

#[test]
fn test_list_objects_v2_no_such_bucket_error_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };

        let missing_bucket = unique_bucket();
        let aws_error = env.send_external(
            "GET",
            &missing_bucket,
            "",
            Some("list-type=2"),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_error = env.send_local(
            "GET",
            &missing_bucket,
            "",
            Some("list-type=2"),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_xml_response_shape_matches(
            "ListObjectsV2NoSuchBucket",
            &aws_error,
            &local_error,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &["RequestId", "HostId"],
        );
    });
}

#[test]
fn test_sigv4_wrong_region_error_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };

        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let wrong_region = alternate_region(&env.external_region);
        let aws_error = send_signed_request_with_credentials(
            "GET",
            &object_url(CTX.endpoint(), &external_bucket, "", None),
            b"",
            std::iter::empty::<(&str, &str)>(),
            SignedRequestCredentials {
                access_key: CTX.access_key(),
                secret_key: CTX.secret_key(),
                region: wrong_region,
                tls_ca_pem: None,
            },
        );
        let local_error = send_signed_request_with_credentials(
            "GET",
            &object_url(env.local_server.endpoint(), &local_bucket, "", None),
            b"",
            std::iter::empty::<(&str, &str)>(),
            SignedRequestCredentials {
                access_key: s3_tests::server::TEST_ACCESS_KEY,
                secret_key: s3_tests::server::TEST_SECRET_KEY,
                region: wrong_region,
                tls_ca_pem: env.local_server.tls_ca_pem(),
            },
        );
        assert_xml_response_shape_matches(
            "SigV4WrongRegion",
            &aws_error,
            &local_error,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &["RequestId", "HostId"],
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[]).await;
    });
}

#[test]
fn test_sigv4_invalid_token_error_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };

        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let aws_error = send_signed_request_with_credentials(
            "GET",
            &object_url(CTX.endpoint(), &external_bucket, "", None),
            b"",
            [("x-amz-security-token", "bad-token-causes-400")],
            SignedRequestCredentials {
                access_key: CTX.access_key(),
                secret_key: CTX.secret_key(),
                region: &env.external_region,
                tls_ca_pem: None,
            },
        );
        let local_error = send_signed_request_with_credentials(
            "GET",
            &object_url(env.local_server.endpoint(), &local_bucket, "", None),
            b"",
            [("x-amz-security-token", "bad-token-causes-400")],
            SignedRequestCredentials {
                access_key: s3_tests::server::TEST_ACCESS_KEY,
                secret_key: s3_tests::server::TEST_SECRET_KEY,
                region: &env.external_region,
                tls_ca_pem: env.local_server.tls_ca_pem(),
            },
        );
        assert_xml_response_shape_matches(
            "SigV4InvalidToken",
            &aws_error,
            &local_error,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &["RequestId", "HostId"],
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[]).await;
    });
}

#[test]
fn test_get_object_no_such_key_error_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };

        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let aws_error = env.send_external(
            "GET",
            &external_bucket,
            "missing-key.txt",
            None,
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_error = env.send_local(
            "GET",
            &local_bucket,
            "missing-key.txt",
            None,
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_xml_response_shape_matches(
            "GetObjectNoSuchKey",
            &aws_error,
            &local_error,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &["RequestId", "HostId"],
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[]).await;
    });
}

#[test]
fn test_get_bucket_encryption_default_blocks_sse_c_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;

        let aws_get = env.send_external(
            "GET",
            &external_bucket,
            "",
            Some("encryption="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_get = env.send_local(
            "GET",
            &local_bucket,
            "",
            Some("encryption="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );

        assert_xml_response_shape_matches(
            "GetBucketEncryptionDefaultBlocksSseC",
            &aws_get,
            &local_get,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &[],
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[]).await;
    });
}

#[test]
fn test_put_object_sse_c_blocked_by_default_error_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let key = "shape-sse-c-blocked-by-default.txt";
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        let aws_put = env.send_external(
            "PUT",
            &external_bucket,
            key,
            None,
            b"secret",
            [
                ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                (
                    "x-amz-server-side-encryption-customer-key",
                    key_b64.as_str(),
                ),
                (
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.as_str(),
                ),
            ],
        );
        let local_put = env.send_local(
            "PUT",
            &local_bucket,
            key,
            None,
            b"secret",
            [
                ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                (
                    "x-amz-server-side-encryption-customer-key",
                    key_b64.as_str(),
                ),
                (
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.as_str(),
                ),
            ],
        );

        assert_response_id_shapes("PutObjectSseCBlockedByDefault", &aws_put);
        assert_response_id_shapes("PutObjectSseCBlockedByDefault", &local_put);
        assert_response_headers_match(
            "PutObjectSseCBlockedByDefault",
            &aws_put,
            &local_put,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
        );
        // The AWS-backed run uses the real caller ARN, while the embedded server uses the
        // local test principal for the same request shape.
        assert_eq!(
            normalize_sse_c_blocked_access_denied_message(&normalize_xml_text_tags(
                &local_put.body,
                &["RequestId", "HostId"],
            )),
            normalize_sse_c_blocked_access_denied_message(&normalize_xml_text_tags(
                &aws_put.body,
                &["RequestId", "HostId"],
            )),
            "PutObjectSseCBlockedByDefault: normalized XML body mismatch\naws body: {}\nlocal body: {}",
            aws_put.body,
            local_put.body,
        );
        assert_xml_error_ids_match_headers("PutObjectSseCBlockedByDefault", &aws_put);
        assert_xml_error_ids_match_headers("PutObjectSseCBlockedByDefault", &local_put);

        delete_all_and_bucket(&env.external_client, &external_bucket, &[]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[]).await;
    });
}

#[test]
fn test_put_object_sse_c_missing_key_md5_error_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = create_sse_c_enabled_bucket_pair(&env).await;
        let key = "shape-sse-c-missing-key-md5.txt";
        let customer_key = test_sse_c_key();
        let (key_b64, _) = sse_c_header_values(&customer_key);

        let aws_put = env.send_external(
            "PUT",
            &external_bucket,
            key,
            None,
            b"secret",
            [
                ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                (
                    "x-amz-server-side-encryption-customer-key",
                    key_b64.as_str(),
                ),
            ],
        );
        let local_put = env.send_local(
            "PUT",
            &local_bucket,
            key,
            None,
            b"secret",
            [
                ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                (
                    "x-amz-server-side-encryption-customer-key",
                    key_b64.as_str(),
                ),
            ],
        );

        assert_xml_response_shape_matches(
            "PutObjectSseCMissingKeyMd5",
            &aws_put,
            &local_put,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &["RequestId", "HostId"],
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[]).await;
    });
}

#[test]
fn test_put_object_sse_c_enabled_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = create_sse_c_enabled_bucket_pair(&env).await;
        let key = "shape-sse-c-enabled.txt";
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        let aws_put = env.send_external(
            "PUT",
            &external_bucket,
            key,
            None,
            b"secret",
            sse_c_headers(key_b64.as_str(), key_md5_b64.as_str()),
        );
        let local_put = env.send_local(
            "PUT",
            &local_bucket,
            key,
            None,
            b"secret",
            sse_c_headers(key_b64.as_str(), key_md5_b64.as_str()),
        );

        assert_response_shape_matches(
            "PutObjectSseCEnabled",
            &aws_put,
            &local_put,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            &["etag"],
        );

        let aws_head = env.send_external(
            "HEAD",
            &external_bucket,
            key,
            None,
            b"",
            sse_c_headers(key_b64.as_str(), key_md5_b64.as_str()),
        );
        let local_head = env.send_local(
            "HEAD",
            &local_bucket,
            key,
            None,
            b"",
            sse_c_headers(key_b64.as_str(), key_md5_b64.as_str()),
        );

        assert_response_headers_match(
            "HeadObjectSseCEnabled",
            &aws_head,
            &local_head,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            &["etag", "last-modified", "x-amz-version-id"],
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[key.to_string()]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[key.to_string()]).await;
    });
}

#[test]
fn test_put_object_sse_c_missing_key_error_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = create_sse_c_enabled_bucket_pair(&env).await;
        let key = "shape-sse-c-missing-key.txt";
        let customer_key = test_sse_c_key();
        let (_, key_md5_b64) = sse_c_header_values(&customer_key);

        let aws_put = env.send_external(
            "PUT",
            &external_bucket,
            key,
            None,
            b"secret",
            [
                ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                (
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.as_str(),
                ),
            ],
        );
        let local_put = env.send_local(
            "PUT",
            &local_bucket,
            key,
            None,
            b"secret",
            [
                ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
                (
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.as_str(),
                ),
            ],
        );

        assert_xml_response_shape_matches(
            "PutObjectSseCMissingKey",
            &aws_put,
            &local_put,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &["RequestId", "HostId"],
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[]).await;
    });
}

#[test]
fn test_put_object_sse_c_wrong_algorithm_error_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = create_sse_c_enabled_bucket_pair(&env).await;
        let key = "shape-sse-c-wrong-algorithm.txt";
        let customer_key = test_sse_c_key();
        let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);

        let aws_put = env.send_external(
            "PUT",
            &external_bucket,
            key,
            None,
            b"secret",
            [
                ("x-amz-server-side-encryption-customer-algorithm", "aws:kms"),
                (
                    "x-amz-server-side-encryption-customer-key",
                    key_b64.as_str(),
                ),
                (
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.as_str(),
                ),
            ],
        );
        let local_put = env.send_local(
            "PUT",
            &local_bucket,
            key,
            None,
            b"secret",
            [
                ("x-amz-server-side-encryption-customer-algorithm", "aws:kms"),
                (
                    "x-amz-server-side-encryption-customer-key",
                    key_b64.as_str(),
                ),
                (
                    "x-amz-server-side-encryption-customer-key-md5",
                    key_md5_b64.as_str(),
                ),
            ],
        );

        assert_xml_response_shape_matches(
            "PutObjectSseCWrongAlgorithm",
            &aws_put,
            &local_put,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &["RequestId", "HostId"],
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[]).await;
    });
}

#[test]
fn test_get_bucket_object_lock_configuration_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = create_matching_object_lock_bucket_pair(&env).await;
        let config = ObjectLockConfiguration::builder()
            .object_lock_enabled(ObjectLockEnabled::Enabled)
            .rule(
                ObjectLockRule::builder()
                    .default_retention(
                        DefaultRetention::builder()
                            .mode(ObjectLockRetentionMode::Governance)
                            .days(7)
                            .build(),
                    )
                    .build(),
            )
            .build();

        env.external_client
            .put_object_lock_configuration()
            .bucket(&external_bucket)
            .object_lock_configuration(config.clone())
            .send()
            .await
            .expect("put external object lock configuration");
        env.local_client
            .put_object_lock_configuration()
            .bucket(&local_bucket)
            .object_lock_configuration(config)
            .send()
            .await
            .expect("put local object lock configuration");

        let aws_get = env.send_external(
            "GET",
            &external_bucket,
            "",
            Some("object-lock="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_get = env.send_local(
            "GET",
            &local_bucket,
            "",
            Some("object-lock="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );

        assert_xml_response_shape_matches(
            "GetBucketObjectLockConfiguration",
            &aws_get,
            &local_get,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &[],
        );

        cleanup_object_lock_bucket(&env.external_client, &external_bucket).await;
        cleanup_object_lock_bucket(&env.local_client, &local_bucket).await;
    });
}

#[test]
fn test_get_object_retention_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = create_matching_object_lock_bucket_pair(&env).await;
        let key = "shape-retention.txt";
        let retain_until = future_datetime(24 * 60 * 60);

        let external_put = env
            .external_client
            .put_object()
            .bucket(&external_bucket)
            .key(key)
            .body(ByteStream::from_static(b"retention"))
            .send()
            .await
            .expect("put external retention fixture");
        let local_put = env
            .local_client
            .put_object()
            .bucket(&local_bucket)
            .key(key)
            .body(ByteStream::from_static(b"retention"))
            .send()
            .await
            .expect("put local retention fixture");
        let external_version = external_put.version_id().expect("external version id");
        let local_version = local_put.version_id().expect("local version id");
        let retention = ObjectLockRetention::builder()
            .mode(ObjectLockRetentionMode::Governance)
            .retain_until_date(retain_until)
            .build();

        env.external_client
            .put_object_retention()
            .bucket(&external_bucket)
            .key(key)
            .version_id(external_version)
            .retention(retention.clone())
            .send()
            .await
            .expect("put external object retention");
        env.local_client
            .put_object_retention()
            .bucket(&local_bucket)
            .key(key)
            .version_id(local_version)
            .retention(retention)
            .send()
            .await
            .expect("put local object retention");

        let aws_get = env.send_external(
            "GET",
            &external_bucket,
            key,
            Some(&format!("retention=&versionId={external_version}")),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_get = env.send_local(
            "GET",
            &local_bucket,
            key,
            Some(&format!("retention=&versionId={local_version}")),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );

        assert_xml_response_shape_matches(
            "GetObjectRetention",
            &aws_get,
            &local_get,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            &["x-amz-version-id"],
            &[],
        );

        cleanup_object_lock_bucket(&env.external_client, &external_bucket).await;
        cleanup_object_lock_bucket(&env.local_client, &local_bucket).await;
    });
}

#[test]
fn test_get_object_legal_hold_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = create_matching_object_lock_bucket_pair(&env).await;
        let key = "shape-legal-hold.txt";

        let external_put = env
            .external_client
            .put_object()
            .bucket(&external_bucket)
            .key(key)
            .body(ByteStream::from_static(b"legal-hold"))
            .send()
            .await
            .expect("put external legal hold fixture");
        let local_put = env
            .local_client
            .put_object()
            .bucket(&local_bucket)
            .key(key)
            .body(ByteStream::from_static(b"legal-hold"))
            .send()
            .await
            .expect("put local legal hold fixture");
        let external_version = external_put.version_id().expect("external version id");
        let local_version = local_put.version_id().expect("local version id");

        env.external_client
            .put_object_legal_hold()
            .bucket(&external_bucket)
            .key(key)
            .version_id(external_version)
            .legal_hold(object_lock_legal_hold(ObjectLockLegalHoldStatus::On))
            .send()
            .await
            .expect("put external legal hold");
        env.local_client
            .put_object_legal_hold()
            .bucket(&local_bucket)
            .key(key)
            .version_id(local_version)
            .legal_hold(object_lock_legal_hold(ObjectLockLegalHoldStatus::On))
            .send()
            .await
            .expect("put local legal hold");

        let aws_get = env.send_external(
            "GET",
            &external_bucket,
            key,
            Some(&format!("legal-hold=&versionId={external_version}")),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_get = env.send_local(
            "GET",
            &local_bucket,
            key,
            Some(&format!("legal-hold=&versionId={local_version}")),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );

        assert_xml_response_shape_matches(
            "GetObjectLegalHold",
            &aws_get,
            &local_get,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            &["x-amz-version-id"],
            &[],
        );

        cleanup_object_lock_bucket(&env.external_client, &external_bucket).await;
        cleanup_object_lock_bucket(&env.local_client, &local_bucket).await;
    });
}

#[test]
fn test_get_object_tagging_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let key = "shape-object-tagging.txt";
        let tagging = Tagging::builder()
            .tag_set(Tag::builder().key("env").value("prod").build().unwrap())
            .tag_set(Tag::builder().key("tier").value("hot").build().unwrap())
            .build()
            .unwrap();

        put_object_pair(
            &env,
            &external_bucket,
            &local_bucket,
            key,
            b"object-tagging",
        )
        .await;
        env.external_client
            .put_object_tagging()
            .bucket(&external_bucket)
            .key(key)
            .tagging(tagging.clone())
            .send()
            .await
            .expect("put external object tagging");
        env.local_client
            .put_object_tagging()
            .bucket(&local_bucket)
            .key(key)
            .tagging(tagging)
            .send()
            .await
            .expect("put local object tagging");

        let aws_get = env.send_external(
            "GET",
            &external_bucket,
            key,
            Some("tagging="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_get = env.send_local(
            "GET",
            &local_bucket,
            key,
            Some("tagging="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );

        assert_xml_response_shape_matches(
            "GetObjectTagging",
            &aws_get,
            &local_get,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &[],
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[key.to_string()]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[key.to_string()]).await;
    });
}

#[test]
fn test_get_object_acl_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let key = "shape-object-acl.txt";

        put_object_pair(&env, &external_bucket, &local_bucket, key, b"object-acl").await;

        let aws_get = env.send_external(
            "GET",
            &external_bucket,
            key,
            Some("acl="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_get = env.send_local(
            "GET",
            &local_bucket,
            key,
            Some("acl="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );

        assert_xml_response_shape_matches(
            "GetObjectAcl",
            &aws_get,
            &local_get,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &["ID", "DisplayName"],
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[key.to_string()]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[key.to_string()]).await;
    });
}

#[test]
fn test_post_object_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let key = "shape-post-object.txt";
        let file_data = b"post-body";
        let aws_fields = sigv4_post_fields_for_credentials(
            CTX.access_key(),
            CTX.secret_key(),
            &env.external_region,
            &external_bucket,
            key,
            &[],
        );
        let local_fields = sigv4_post_fields_for_credentials(
            s3_tests::server::TEST_ACCESS_KEY,
            s3_tests::server::TEST_SECRET_KEY,
            &env.external_region,
            &local_bucket,
            key,
            &[],
        );

        let aws_post = post_object_raw_to_test_endpoint_with_headers(
            CTX.endpoint(),
            None,
            &external_bucket,
            &aws_fields,
            file_data,
            "test.txt",
            &[],
        );
        let local_post = post_object_raw_to_test_endpoint_with_headers(
            env.local_server.endpoint(),
            env.local_server.tls_ca_pem(),
            &local_bucket,
            &local_fields,
            file_data,
            "test.txt",
            &[],
        );

        assert_xml_response_shape_matches(
            "PostObject",
            &aws_post,
            &local_post,
            &["date", "etag", "location", "server"],
            COMMON_PRESENCE_ONLY_HEADERS,
            &["ETag"],
        );
        assert_eq!(
            normalize_location_header(
                response_header_value(&local_post, "location").expect("local post location"),
            ),
            normalize_location_header(
                response_header_value(&aws_post, "location").expect("aws post location"),
            ),
            "PostObject: normalized Location mismatch\naws headers: {:?}\nlocal headers: {:?}",
            aws_post.headers,
            local_post.headers,
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[key.to_string()]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[key.to_string()]).await;
    });
}

#[test]
fn test_copy_object_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let src_key = "copy-source.txt";
        let dst_key = "copy-dest.txt";
        let body = b"copy source body";

        let aws_put = env.send_external(
            "PUT",
            &external_bucket,
            src_key,
            None,
            body,
            std::iter::empty::<(&str, &str)>(),
        );
        let local_put = env.send_local(
            "PUT",
            &local_bucket,
            src_key,
            None,
            body,
            std::iter::empty::<(&str, &str)>(),
        );
        assert_response_shape_matches(
            "CopySourceFixture",
            &aws_put,
            &local_put,
            COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
        );

        let aws_copy = env.send_external(
            "PUT",
            &external_bucket,
            dst_key,
            None,
            b"",
            [("x-amz-copy-source", format!("{external_bucket}/{src_key}"))],
        );
        let local_copy = env.send_local(
            "PUT",
            &local_bucket,
            dst_key,
            None,
            b"",
            [("x-amz-copy-source", format!("{local_bucket}/{src_key}"))],
        );
        assert_xml_response_shape_matches(
            "CopyObject",
            &aws_copy,
            &local_copy,
            &["content-length", "date"],
            COMMON_PRESENCE_ONLY_HEADERS,
            &["ETag", "LastModified"],
        );

        delete_all_and_bucket(
            &env.external_client,
            &external_bucket,
            &[src_key.to_string(), dst_key.to_string()],
        )
        .await;
        delete_all_and_bucket(
            &env.local_client,
            &local_bucket,
            &[src_key.to_string(), dst_key.to_string()],
        )
        .await;
    });
}

#[test]
fn test_multipart_response_headers_match_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let key = "shape-multipart.txt";
        let part_body = b"multipart-part-body";

        let aws_create = env.send_external(
            "POST",
            &external_bucket,
            key,
            Some("uploads="),
            b"",
            [("x-amz-checksum-algorithm", "CRC64NVME")],
        );
        let local_create = env.send_local(
            "POST",
            &local_bucket,
            key,
            Some("uploads="),
            b"",
            [("x-amz-checksum-algorithm", "CRC64NVME")],
        );
        assert_response_headers_match(
            "CreateMultipartUpload",
            &aws_create,
            &local_create,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
        );
        let external_upload_id = xml_tag_text(&aws_create.body, "UploadId")
            .expect("external raw create upload id")
            .to_string();
        let local_upload_id = xml_tag_text(&local_create.body, "UploadId")
            .expect("local raw create upload id")
            .to_string();

        let aws_upload_part = env.send_external(
            "PUT",
            &external_bucket,
            key,
            Some(&format!("partNumber=1&uploadId={external_upload_id}")),
            part_body,
            std::iter::empty::<(&str, &str)>(),
        );
        let local_upload_part = env.send_local(
            "PUT",
            &local_bucket,
            key,
            Some(&format!("partNumber=1&uploadId={local_upload_id}")),
            part_body,
            std::iter::empty::<(&str, &str)>(),
        );
        assert_response_headers_match(
            "UploadPart",
            &aws_upload_part,
            &local_upload_part,
            COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
        );
        let external_part_etag =
            response_header_value(&aws_upload_part, "etag").expect("external upload part etag");
        let local_part_etag =
            response_header_value(&local_upload_part, "etag").expect("local upload part etag");

        let aws_list_parts = env.send_external(
            "GET",
            &external_bucket,
            key,
            Some(&format!("uploadId={external_upload_id}")),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_list_parts = env.send_local(
            "GET",
            &local_bucket,
            key,
            Some(&format!("uploadId={local_upload_id}")),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_response_headers_match(
            "ListParts",
            &aws_list_parts,
            &local_list_parts,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
        );

        let complete_body_external = format!(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
            external_part_etag
        );
        let complete_body_local = format!(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
            local_part_etag
        );
        let aws_complete = env.send_external(
            "POST",
            &external_bucket,
            key,
            Some(&format!("uploadId={external_upload_id}")),
            complete_body_external.as_bytes(),
            [("Content-Type", "application/xml")],
        );
        let local_complete = env.send_local(
            "POST",
            &local_bucket,
            key,
            Some(&format!("uploadId={local_upload_id}")),
            complete_body_local.as_bytes(),
            [("Content-Type", "application/xml")],
        );
        assert_response_headers_match(
            "CompleteMultipartUpload",
            &aws_complete,
            &local_complete,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[key.to_string()]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[key.to_string()]).await;
    });
}

#[test]
fn test_object_part_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let key = "shape-object-part.txt";
        let part_one = vec![b'A'; AWS_MIN_MULTIPART_PART_SIZE];
        let part_two = b"second-multipart-part".to_vec();

        create_completed_multipart_pair(
            &env,
            &external_bucket,
            &local_bucket,
            key,
            vec![("Content-Type", "application/octet-stream")],
            &[part_one.as_slice(), part_two.as_slice()],
        )
        .await;

        let aws_head_part = env.send_external(
            "HEAD",
            &external_bucket,
            key,
            Some("partNumber=1"),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_head_part = env.send_local(
            "HEAD",
            &local_bucket,
            key,
            Some("partNumber=1"),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_response_shape_matches(
            "HeadObjectPart",
            &aws_head_part,
            &local_head_part,
            COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
        );

        let aws_get_part = env.send_external(
            "GET",
            &external_bucket,
            key,
            Some("partNumber=1"),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_get_part = env.send_local(
            "GET",
            &local_bucket,
            key,
            Some("partNumber=1"),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_response_shape_matches(
            "GetObjectPart",
            &aws_get_part,
            &local_get_part,
            COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[key.to_string()]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[key.to_string()]).await;
    });
}

#[test]
fn test_upload_part_copy_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let src_key = "shape-upload-part-copy-src.txt";
        let dst_key = "shape-upload-part-copy-dst.txt";
        let source_body = b"upload-part-copy-source-body";

        let aws_put = env.send_external(
            "PUT",
            &external_bucket,
            src_key,
            None,
            source_body,
            std::iter::empty::<(&str, &str)>(),
        );
        let local_put = env.send_local(
            "PUT",
            &local_bucket,
            src_key,
            None,
            source_body,
            std::iter::empty::<(&str, &str)>(),
        );
        assert_response_shape_matches(
            "UploadPartCopySourceFixture",
            &aws_put,
            &local_put,
            COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
        );

        let (external_upload_id, local_upload_id) =
            create_multipart_upload_pair(&env, &external_bucket, &local_bucket, dst_key, vec![])
                .await;

        let aws_upload_part_copy = env.send_external(
            "PUT",
            &external_bucket,
            dst_key,
            Some(&format!("partNumber=1&uploadId={external_upload_id}")),
            b"",
            [("x-amz-copy-source", format!("{external_bucket}/{src_key}"))],
        );
        let local_upload_part_copy = env.send_local(
            "PUT",
            &local_bucket,
            dst_key,
            Some(&format!("partNumber=1&uploadId={local_upload_id}")),
            b"",
            [("x-amz-copy-source", format!("{local_bucket}/{src_key}"))],
        );
        assert_xml_response_shape_matches(
            "UploadPartCopy",
            &aws_upload_part_copy,
            &local_upload_part_copy,
            &["content-length", "date"],
            COMMON_PRESENCE_ONLY_HEADERS,
            &["ETag", "LastModified"],
        );

        let external_copy_part_etag = xml_text_unescape(
            xml_tag_text(&aws_upload_part_copy.body, "ETag")
                .expect("external upload-part-copy etag"),
        );
        let local_copy_part_etag = xml_text_unescape(
            xml_tag_text(&local_upload_part_copy.body, "ETag")
                .expect("local upload-part-copy etag"),
        );

        env.external_client
            .complete_multipart_upload()
            .bucket(&external_bucket)
            .key(dst_key)
            .upload_id(&external_upload_id)
            .multipart_upload(
                aws_sdk_s3::types::CompletedMultipartUpload::builder()
                    .parts(
                        aws_sdk_s3::types::CompletedPart::builder()
                            .part_number(1)
                            .e_tag(external_copy_part_etag)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .expect("complete external upload-part-copy fixture");
        env.local_client
            .complete_multipart_upload()
            .bucket(&local_bucket)
            .key(dst_key)
            .upload_id(&local_upload_id)
            .multipart_upload(
                aws_sdk_s3::types::CompletedMultipartUpload::builder()
                    .parts(
                        aws_sdk_s3::types::CompletedPart::builder()
                            .part_number(1)
                            .e_tag(local_copy_part_etag)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .expect("complete local upload-part-copy fixture");

        delete_all_and_bucket(
            &env.external_client,
            &external_bucket,
            &[src_key.to_string(), dst_key.to_string()],
        )
        .await;
        delete_all_and_bucket(
            &env.local_client,
            &local_bucket,
            &[src_key.to_string(), dst_key.to_string()],
        )
        .await;
    });
}

#[test]
fn test_complete_multipart_no_such_upload_error_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let key = "shape-complete-no-such-upload.txt";
        let (external_upload_id, local_upload_id) =
            create_multipart_upload_pair(&env, &external_bucket, &local_bucket, key, vec![]).await;

        env.external_client
            .abort_multipart_upload()
            .bucket(&external_bucket)
            .key(key)
            .upload_id(&external_upload_id)
            .send()
            .await
            .expect("abort external upload");
        env.local_client
            .abort_multipart_upload()
            .bucket(&local_bucket)
            .key(key)
            .upload_id(&local_upload_id)
            .send()
            .await
            .expect("abort local upload");

        let complete_body = concat!(
            "<CompleteMultipartUpload>",
            "<Part><PartNumber>1</PartNumber><ETag>\"ffffffffffffffff\"</ETag></Part>",
            "</CompleteMultipartUpload>",
        );
        let aws_complete = env.send_external(
            "POST",
            &external_bucket,
            key,
            Some(&format!("uploadId={external_upload_id}")),
            complete_body.as_bytes(),
            [("Content-Type", "application/xml")],
        );
        let local_complete = env.send_local(
            "POST",
            &local_bucket,
            key,
            Some(&format!("uploadId={local_upload_id}")),
            complete_body.as_bytes(),
            [("Content-Type", "application/xml")],
        );
        assert_xml_response_shape_matches(
            "CompleteMultipartNoSuchUpload",
            &aws_complete,
            &local_complete,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &["RequestId", "HostId", "UploadId"],
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[]).await;
    });
}

#[test]
fn test_complete_multipart_invalid_part_error_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let key = "shape-complete-invalid-part.txt";
        let (external_upload_id, local_upload_id) =
            create_multipart_upload_pair(&env, &external_bucket, &local_bucket, key, vec![]).await;

        let _ = upload_part_pair(
            &env,
            (&external_bucket, &local_bucket),
            key,
            (&external_upload_id, &local_upload_id),
            1,
            &[0u8; 256],
        )
        .await;

        let complete_body = concat!(
            "<CompleteMultipartUpload>",
            "<Part><PartNumber>1</PartNumber><ETag>\"ffffffffffffffff\"</ETag></Part>",
            "</CompleteMultipartUpload>",
        );
        let aws_complete = env.send_external(
            "POST",
            &external_bucket,
            key,
            Some(&format!("uploadId={external_upload_id}")),
            complete_body.as_bytes(),
            [("Content-Type", "application/xml")],
        );
        let local_complete = env.send_local(
            "POST",
            &local_bucket,
            key,
            Some(&format!("uploadId={local_upload_id}")),
            complete_body.as_bytes(),
            [("Content-Type", "application/xml")],
        );
        assert_xml_response_shape_matches(
            "CompleteMultipartInvalidPart",
            &aws_complete,
            &local_complete,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &["RequestId", "HostId", "UploadId", "PartNumber"],
        );

        env.external_client
            .abort_multipart_upload()
            .bucket(&external_bucket)
            .key(key)
            .upload_id(&external_upload_id)
            .send()
            .await
            .expect("abort external upload after invalid part");
        env.local_client
            .abort_multipart_upload()
            .bucket(&local_bucket)
            .key(key)
            .upload_id(&local_upload_id)
            .send()
            .await
            .expect("abort local upload after invalid part");
        delete_all_and_bucket(&env.external_client, &external_bucket, &[]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[]).await;
    });
}

#[test]
fn test_complete_multipart_invalid_part_order_error_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let key = "shape-complete-invalid-order.txt";
        let (external_upload_id, local_upload_id) =
            create_multipart_upload_pair(&env, &external_bucket, &local_bucket, key, vec![]).await;

        let large_part = vec![b'a'; AWS_MIN_MULTIPART_PART_SIZE];
        let (external_etag_1, local_etag_1) = upload_part_pair(
            &env,
            (&external_bucket, &local_bucket),
            key,
            (&external_upload_id, &local_upload_id),
            1,
            &large_part,
        )
        .await;
        let (external_etag_2, local_etag_2) = upload_part_pair(
            &env,
            (&external_bucket, &local_bucket),
            key,
            (&external_upload_id, &local_upload_id),
            2,
            &[b'b'; 256],
        )
        .await;

        let aws_complete_body = format!(
            "<CompleteMultipartUpload>\
             <Part><PartNumber>2</PartNumber><ETag>{external_etag_2}</ETag></Part>\
             <Part><PartNumber>1</PartNumber><ETag>{external_etag_1}</ETag></Part>\
             </CompleteMultipartUpload>"
        );
        let local_complete_body = format!(
            "<CompleteMultipartUpload>\
             <Part><PartNumber>2</PartNumber><ETag>{local_etag_2}</ETag></Part>\
             <Part><PartNumber>1</PartNumber><ETag>{local_etag_1}</ETag></Part>\
             </CompleteMultipartUpload>"
        );
        let aws_complete = env.send_external(
            "POST",
            &external_bucket,
            key,
            Some(&format!("uploadId={external_upload_id}")),
            aws_complete_body.as_bytes(),
            [("Content-Type", "application/xml")],
        );
        let local_complete = env.send_local(
            "POST",
            &local_bucket,
            key,
            Some(&format!("uploadId={local_upload_id}")),
            local_complete_body.as_bytes(),
            [("Content-Type", "application/xml")],
        );
        assert_xml_response_shape_matches(
            "CompleteMultipartInvalidPartOrder",
            &aws_complete,
            &local_complete,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &["RequestId", "HostId", "UploadId"],
        );

        env.external_client
            .abort_multipart_upload()
            .bucket(&external_bucket)
            .key(key)
            .upload_id(&external_upload_id)
            .send()
            .await
            .expect("abort external upload after invalid order");
        env.local_client
            .abort_multipart_upload()
            .bucket(&local_bucket)
            .key(key)
            .upload_id(&local_upload_id)
            .send()
            .await
            .expect("abort local upload after invalid order");
        delete_all_and_bucket(&env.external_client, &external_bucket, &[]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[]).await;
    });
}

#[test]
fn test_complete_multipart_entity_too_small_error_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let key = "shape-complete-entity-too-small.txt";
        let (external_upload_id, local_upload_id) =
            create_multipart_upload_pair(&env, &external_bucket, &local_bucket, key, vec![]).await;

        let (external_etag_1, local_etag_1) = upload_part_pair(
            &env,
            (&external_bucket, &local_bucket),
            key,
            (&external_upload_id, &local_upload_id),
            1,
            &[0u8; 100],
        )
        .await;
        let (external_etag_2, local_etag_2) = upload_part_pair(
            &env,
            (&external_bucket, &local_bucket),
            key,
            (&external_upload_id, &local_upload_id),
            2,
            &[0u8; 100],
        )
        .await;

        let aws_complete_body = format!(
            "<CompleteMultipartUpload>\
             <Part><PartNumber>1</PartNumber><ETag>{external_etag_1}</ETag></Part>\
             <Part><PartNumber>2</PartNumber><ETag>{external_etag_2}</ETag></Part>\
             </CompleteMultipartUpload>"
        );
        let local_complete_body = format!(
            "<CompleteMultipartUpload>\
             <Part><PartNumber>1</PartNumber><ETag>{local_etag_1}</ETag></Part>\
             <Part><PartNumber>2</PartNumber><ETag>{local_etag_2}</ETag></Part>\
             </CompleteMultipartUpload>"
        );
        let aws_complete = env.send_external(
            "POST",
            &external_bucket,
            key,
            Some(&format!("uploadId={external_upload_id}")),
            aws_complete_body.as_bytes(),
            [("Content-Type", "application/xml")],
        );
        let local_complete = env.send_local(
            "POST",
            &local_bucket,
            key,
            Some(&format!("uploadId={local_upload_id}")),
            local_complete_body.as_bytes(),
            [("Content-Type", "application/xml")],
        );
        assert_xml_response_shape_matches(
            "CompleteMultipartEntityTooSmall",
            &aws_complete,
            &local_complete,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &["RequestId", "HostId", "ETag"],
        );

        env.external_client
            .abort_multipart_upload()
            .bucket(&external_bucket)
            .key(key)
            .upload_id(&external_upload_id)
            .send()
            .await
            .expect("abort external upload after entity too small");
        env.local_client
            .abort_multipart_upload()
            .bucket(&local_bucket)
            .key(key)
            .upload_id(&local_upload_id)
            .send()
            .await
            .expect("abort local upload after entity too small");
        delete_all_and_bucket(&env.external_client, &external_bucket, &[]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[]).await;
    });
}

#[test]
fn test_upload_part_copy_invalid_range_error_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let src_key = "shape-upload-part-copy-invalid-range-src.txt";
        let dst_key = "shape-upload-part-copy-invalid-range-dst.txt";

        put_object_pair(
            &env,
            &external_bucket,
            &local_bucket,
            src_key,
            &[b'Z'; 1000],
        )
        .await;
        let (external_upload_id, local_upload_id) =
            create_multipart_upload_pair(&env, &external_bucket, &local_bucket, dst_key, vec![])
                .await;

        let aws_copy = env.send_external(
            "PUT",
            &external_bucket,
            dst_key,
            Some(&format!("partNumber=1&uploadId={external_upload_id}")),
            b"",
            [
                ("x-amz-copy-source", format!("{external_bucket}/{src_key}")),
                ("x-amz-copy-source-range", "bytes=0-9999".to_string()),
            ],
        );
        let local_copy = env.send_local(
            "PUT",
            &local_bucket,
            dst_key,
            Some(&format!("partNumber=1&uploadId={local_upload_id}")),
            b"",
            [
                ("x-amz-copy-source", format!("{local_bucket}/{src_key}")),
                ("x-amz-copy-source-range", "bytes=0-9999".to_string()),
            ],
        );
        assert_xml_response_shape_matches(
            "UploadPartCopyInvalidRange",
            &aws_copy,
            &local_copy,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &["RequestId", "HostId"],
        );

        env.external_client
            .abort_multipart_upload()
            .bucket(&external_bucket)
            .key(dst_key)
            .upload_id(&external_upload_id)
            .send()
            .await
            .expect("abort external invalid-range upload");
        env.local_client
            .abort_multipart_upload()
            .bucket(&local_bucket)
            .key(dst_key)
            .upload_id(&local_upload_id)
            .send()
            .await
            .expect("abort local invalid-range upload");
        delete_all_and_bucket(
            &env.external_client,
            &external_bucket,
            &[src_key.to_string()],
        )
        .await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[src_key.to_string()]).await;
    });
}

#[test]
fn test_complete_multipart_checksum_mismatch_error_shape_matches_aws() {
    s3_tests::run(async {
        use base64::Engine;

        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let key = "shape-complete-checksum-mismatch.txt";
        let create_headers = vec![("x-amz-checksum-algorithm", "SHA256")];
        let (external_upload_id, local_upload_id) = create_multipart_upload_pair(
            &env,
            &external_bucket,
            &local_bucket,
            key,
            create_headers,
        )
        .await;

        let part_body = b"checksum mismatch multipart part";
        let part_checksum = base64::engine::general_purpose::STANDARD
            .encode(ring::digest::digest(&ring::digest::SHA256, part_body).as_ref());
        let aws_upload = env.send_external(
            "PUT",
            &external_bucket,
            key,
            Some(&format!("partNumber=1&uploadId={external_upload_id}")),
            part_body,
            [("x-amz-checksum-sha256", part_checksum.as_str())],
        );
        let local_upload = env.send_local(
            "PUT",
            &local_bucket,
            key,
            Some(&format!("partNumber=1&uploadId={local_upload_id}")),
            part_body,
            [("x-amz-checksum-sha256", part_checksum.as_str())],
        );
        assert_eq!(
            aws_upload.status, 200,
            "aws upload part failed: {aws_upload:?}"
        );
        assert_eq!(
            local_upload.status, 200,
            "local upload part failed: {local_upload:?}"
        );

        let external_etag =
            response_header_value(&aws_upload, "etag").expect("external checksum part etag");
        let local_etag =
            response_header_value(&local_upload, "etag").expect("local checksum part etag");
        let aws_complete_body = format!(
            "<CompleteMultipartUpload>\
             <Part><PartNumber>1</PartNumber><ETag>{external_etag}</ETag>\
             <ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part>\
             </CompleteMultipartUpload>"
        );
        let local_complete_body = format!(
            "<CompleteMultipartUpload>\
             <Part><PartNumber>1</PartNumber><ETag>{local_etag}</ETag>\
             <ChecksumSHA256>{part_checksum}</ChecksumSHA256></Part>\
             </CompleteMultipartUpload>"
        );
        let aws_complete = env.send_external(
            "POST",
            &external_bucket,
            key,
            Some(&format!("uploadId={external_upload_id}")),
            aws_complete_body.as_bytes(),
            [
                ("Content-Type", "application/xml"),
                ("x-amz-checksum-sha256", "bad"),
            ],
        );
        let local_complete = env.send_local(
            "POST",
            &local_bucket,
            key,
            Some(&format!("uploadId={local_upload_id}")),
            local_complete_body.as_bytes(),
            [
                ("Content-Type", "application/xml"),
                ("x-amz-checksum-sha256", "bad"),
            ],
        );
        assert_xml_response_shape_matches(
            "CompleteMultipartChecksumMismatch",
            &aws_complete,
            &local_complete,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &["RequestId", "HostId"],
        );

        env.external_client
            .abort_multipart_upload()
            .bucket(&external_bucket)
            .key(key)
            .upload_id(&external_upload_id)
            .send()
            .await
            .expect("abort external upload after checksum mismatch");
        env.local_client
            .abort_multipart_upload()
            .bucket(&local_bucket)
            .key(key)
            .upload_id(&local_upload_id)
            .send()
            .await
            .expect("abort local upload after checksum mismatch");
        delete_all_and_bucket(&env.external_client, &external_bucket, &[]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[]).await;
    });
}

#[test]
fn test_complete_multipart_missing_part_checksum_error_shape_matches_aws() {
    s3_tests::run(async {
        use base64::Engine;

        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let key = "shape-complete-missing-part-checksum.txt";
        let create_headers = vec![("x-amz-checksum-algorithm", "SHA256")];
        let (external_upload_id, local_upload_id) = create_multipart_upload_pair(
            &env,
            &external_bucket,
            &local_bucket,
            key,
            create_headers,
        )
        .await;

        let part_body = b"missing part checksum multipart part";
        let part_checksum = base64::engine::general_purpose::STANDARD
            .encode(ring::digest::digest(&ring::digest::SHA256, part_body).as_ref());
        let aws_upload = env.send_external(
            "PUT",
            &external_bucket,
            key,
            Some(&format!("partNumber=1&uploadId={external_upload_id}")),
            part_body,
            [("x-amz-checksum-sha256", part_checksum.as_str())],
        );
        let local_upload = env.send_local(
            "PUT",
            &local_bucket,
            key,
            Some(&format!("partNumber=1&uploadId={local_upload_id}")),
            part_body,
            [("x-amz-checksum-sha256", part_checksum.as_str())],
        );
        assert_eq!(
            aws_upload.status, 200,
            "aws upload part failed: {aws_upload:?}"
        );
        assert_eq!(
            local_upload.status, 200,
            "local upload part failed: {local_upload:?}"
        );

        let external_etag =
            response_header_value(&aws_upload, "etag").expect("external checksum part etag");
        let local_etag =
            response_header_value(&local_upload, "etag").expect("local checksum part etag");
        let aws_complete_body = format!(
            "<CompleteMultipartUpload>\
             <Part><PartNumber>1</PartNumber><ETag>{external_etag}</ETag></Part>\
             </CompleteMultipartUpload>"
        );
        let local_complete_body = format!(
            "<CompleteMultipartUpload>\
             <Part><PartNumber>1</PartNumber><ETag>{local_etag}</ETag></Part>\
             </CompleteMultipartUpload>"
        );
        let aws_complete = env.send_external(
            "POST",
            &external_bucket,
            key,
            Some(&format!("uploadId={external_upload_id}")),
            aws_complete_body.as_bytes(),
            [("Content-Type", "application/xml")],
        );
        let local_complete = env.send_local(
            "POST",
            &local_bucket,
            key,
            Some(&format!("uploadId={local_upload_id}")),
            local_complete_body.as_bytes(),
            [("Content-Type", "application/xml")],
        );
        assert_xml_response_shape_matches(
            "CompleteMultipartMissingPartChecksum",
            &aws_complete,
            &local_complete,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &["RequestId", "HostId"],
        );

        env.external_client
            .abort_multipart_upload()
            .bucket(&external_bucket)
            .key(key)
            .upload_id(&external_upload_id)
            .send()
            .await
            .expect("abort external upload after missing part checksum");
        env.local_client
            .abort_multipart_upload()
            .bucket(&local_bucket)
            .key(key)
            .upload_id(&local_upload_id)
            .send()
            .await
            .expect("abort local upload after missing part checksum");
        delete_all_and_bucket(&env.external_client, &external_bucket, &[]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[]).await;
    });
}

#[test]
fn test_upload_part_copy_source_if_match_failed_error_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let src_key = "shape-upload-part-copy-if-match-src.txt";
        let dst_key = "shape-upload-part-copy-if-match-dst.txt";

        put_object_pair(
            &env,
            &external_bucket,
            &local_bucket,
            src_key,
            b"copy-source-body",
        )
        .await;
        let (external_upload_id, local_upload_id) =
            create_multipart_upload_pair(&env, &external_bucket, &local_bucket, dst_key, vec![])
                .await;

        let aws_copy = env.send_external(
            "PUT",
            &external_bucket,
            dst_key,
            Some(&format!("partNumber=1&uploadId={external_upload_id}")),
            b"",
            [
                ("x-amz-copy-source", format!("{external_bucket}/{src_key}")),
                (
                    "x-amz-copy-source-if-match",
                    "\"0000000000000000\"".to_string(),
                ),
            ],
        );
        let local_copy = env.send_local(
            "PUT",
            &local_bucket,
            dst_key,
            Some(&format!("partNumber=1&uploadId={local_upload_id}")),
            b"",
            [
                ("x-amz-copy-source", format!("{local_bucket}/{src_key}")),
                (
                    "x-amz-copy-source-if-match",
                    "\"0000000000000000\"".to_string(),
                ),
            ],
        );
        assert_xml_response_shape_matches(
            "UploadPartCopySourceIfMatchFailed",
            &aws_copy,
            &local_copy,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &["RequestId", "HostId"],
        );

        env.external_client
            .abort_multipart_upload()
            .bucket(&external_bucket)
            .key(dst_key)
            .upload_id(&external_upload_id)
            .send()
            .await
            .expect("abort external if-match upload");
        env.local_client
            .abort_multipart_upload()
            .bucket(&local_bucket)
            .key(dst_key)
            .upload_id(&local_upload_id)
            .send()
            .await
            .expect("abort local if-match upload");
        delete_all_and_bucket(
            &env.external_client,
            &external_bucket,
            &[src_key.to_string()],
        )
        .await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[src_key.to_string()]).await;
    });
}

#[test]
fn test_get_object_attributes_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let key = "shape-object-attributes.txt";
        let part_one = vec![b'B'; AWS_MIN_MULTIPART_PART_SIZE];
        let part_two = b"attributes-second-part".to_vec();

        create_completed_multipart_pair(
            &env,
            &external_bucket,
            &local_bucket,
            key,
            vec![
                ("Content-Type", "application/octet-stream"),
                ("x-amz-checksum-algorithm", "CRC32"),
            ],
            &[part_one.as_slice(), part_two.as_slice()],
        )
        .await;

        let attributes_header = "Checksum,ObjectParts,ObjectSize,StorageClass";
        let aws_get_object_attributes = env.send_external(
            "GET",
            &external_bucket,
            key,
            Some("attributes="),
            b"",
            [("x-amz-object-attributes", attributes_header)],
        );
        let local_get_object_attributes = env.send_local(
            "GET",
            &local_bucket,
            key,
            Some("attributes="),
            b"",
            [("x-amz-object-attributes", attributes_header)],
        );
        assert_response_shape_matches(
            "GetObjectAttributes",
            &aws_get_object_attributes,
            &local_get_object_attributes,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[key.to_string()]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[key.to_string()]).await;
    });
}

#[test]
fn test_object_lock_read_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = create_object_lock_bucket_pair(&env).await;
        let locked_key = "shape-object-lock.txt";
        let plain_key = "shape-object-lock-plain.txt";
        let retain_until = future_datetime(24 * 60 * 60);

        let external_locked_put = env
            .external_client
            .put_object()
            .bucket(&external_bucket)
            .key(locked_key)
            .body(ByteStream::from_static(b"object-lock-body"))
            .object_lock_mode(ObjectLockMode::Governance)
            .object_lock_retain_until_date(retain_until)
            .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
            .send()
            .await
            .expect("put external object lock object");
        let local_locked_put = env
            .local_client
            .put_object()
            .bucket(&local_bucket)
            .key(locked_key)
            .body(ByteStream::from_static(b"object-lock-body"))
            .object_lock_mode(ObjectLockMode::Governance)
            .object_lock_retain_until_date(retain_until)
            .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
            .send()
            .await
            .expect("put local object lock object");
        let external_locked_version = external_locked_put
            .version_id()
            .expect("external locked version id")
            .to_string();
        let local_locked_version = local_locked_put
            .version_id()
            .expect("local locked version id")
            .to_string();

        env.external_client
            .put_object()
            .bucket(&external_bucket)
            .key(plain_key)
            .body(ByteStream::from_static(b"plain-object-lock-body"))
            .send()
            .await
            .expect("put external plain object lock fixture");
        env.local_client
            .put_object()
            .bucket(&local_bucket)
            .key(plain_key)
            .body(ByteStream::from_static(b"plain-object-lock-body"))
            .send()
            .await
            .expect("put local plain object lock fixture");

        let aws_locked_get = env.send_external(
            "GET",
            &external_bucket,
            locked_key,
            None,
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_locked_get = env.send_local(
            "GET",
            &local_bucket,
            locked_key,
            None,
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_response_shape_matches(
            "GetObjectObjectLock",
            &aws_locked_get,
            &local_locked_get,
            COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
            &["last-modified", "x-amz-version-id"],
        );

        let aws_locked_head = env.send_external(
            "HEAD",
            &external_bucket,
            locked_key,
            None,
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_locked_head = env.send_local(
            "HEAD",
            &local_bucket,
            locked_key,
            None,
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_response_shape_matches(
            "HeadObjectObjectLock",
            &aws_locked_head,
            &local_locked_head,
            COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
            &["last-modified", "x-amz-version-id"],
        );

        let aws_locked_attributes = env.send_external(
            "GET",
            &external_bucket,
            locked_key,
            Some("attributes="),
            b"",
            [("x-amz-object-attributes", "ObjectSize")],
        );
        let local_locked_attributes = env.send_local(
            "GET",
            &local_bucket,
            locked_key,
            Some("attributes="),
            b"",
            [("x-amz-object-attributes", "ObjectSize")],
        );
        assert_response_shape_matches(
            "GetObjectAttributesObjectLock",
            &aws_locked_attributes,
            &local_locked_attributes,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            &["last-modified", "x-amz-version-id"],
        );

        let aws_locked_version_get = env.send_external(
            "GET",
            &external_bucket,
            locked_key,
            Some(&format!("versionId={external_locked_version}")),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_locked_version_get = env.send_local(
            "GET",
            &local_bucket,
            locked_key,
            Some(&format!("versionId={local_locked_version}")),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_response_shape_matches(
            "GetObjectObjectLockExplicitVersion",
            &aws_locked_version_get,
            &local_locked_version_get,
            COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
            &["last-modified", "x-amz-version-id"],
        );

        let aws_locked_version_head = env.send_external(
            "HEAD",
            &external_bucket,
            locked_key,
            Some(&format!("versionId={external_locked_version}")),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_locked_version_head = env.send_local(
            "HEAD",
            &local_bucket,
            locked_key,
            Some(&format!("versionId={local_locked_version}")),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_response_shape_matches(
            "HeadObjectObjectLockExplicitVersion",
            &aws_locked_version_head,
            &local_locked_version_head,
            COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
            &["last-modified", "x-amz-version-id"],
        );

        let aws_plain_head = env.send_external(
            "HEAD",
            &external_bucket,
            plain_key,
            None,
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_plain_head = env.send_local(
            "HEAD",
            &local_bucket,
            plain_key,
            None,
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_response_shape_matches(
            "HeadObjectObjectLockBucketPlainObject",
            &aws_plain_head,
            &local_plain_head,
            COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
            &["last-modified", "x-amz-version-id"],
        );

        cleanup_object_lock_bucket(&env.external_client, &external_bucket).await;
        cleanup_object_lock_bucket(&env.local_client, &local_bucket).await;
    });
}

#[test]
fn test_delete_objects_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let keys = ["shape-delete-objects-a.txt", "shape-delete-objects-b.txt"];
        let delete_body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <Delete>\
             <Object><Key>{}</Key></Object>\
             <Object><Key>{}</Key></Object>\
             </Delete>",
            keys[0], keys[1]
        );
        let md5_header = content_md5_header(delete_body.as_bytes());

        for key in keys {
            put_object_pair(
                &env,
                &external_bucket,
                &local_bucket,
                key,
                b"delete-objects",
            )
            .await;
        }

        let aws_delete = env.send_external(
            "POST",
            &external_bucket,
            "",
            Some("delete="),
            delete_body.as_bytes(),
            [md5_header.clone()],
        );
        let local_delete = env.send_local(
            "POST",
            &local_bucket,
            "",
            Some("delete="),
            delete_body.as_bytes(),
            [md5_header],
        );

        assert_delete_objects_response_shape_matches(&aws_delete, &local_delete);

        delete_all_and_bucket(&env.external_client, &external_bucket, &[]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[]).await;
    });
}

#[test]
fn test_list_object_versions_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        enable_bucket_versioning(&env.external_client, &external_bucket).await;
        enable_bucket_versioning(&env.local_client, &local_bucket).await;

        env.external_client
            .put_object()
            .bucket(&external_bucket)
            .key("versions-a.txt")
            .body(ByteStream::from_static(b"v1"))
            .send()
            .await
            .expect("put external version fixture a1");
        env.local_client
            .put_object()
            .bucket(&local_bucket)
            .key("versions-a.txt")
            .body(ByteStream::from_static(b"v1"))
            .send()
            .await
            .expect("put local version fixture a1");
        env.external_client
            .put_object()
            .bucket(&external_bucket)
            .key("versions-a.txt")
            .body(ByteStream::from_static(b"v2"))
            .send()
            .await
            .expect("put external version fixture a2");
        env.local_client
            .put_object()
            .bucket(&local_bucket)
            .key("versions-a.txt")
            .body(ByteStream::from_static(b"v2"))
            .send()
            .await
            .expect("put local version fixture a2");
        env.external_client
            .put_object()
            .bucket(&external_bucket)
            .key("versions-b.txt")
            .body(ByteStream::from_static(b"vb"))
            .send()
            .await
            .expect("put external version fixture b1");
        env.local_client
            .put_object()
            .bucket(&local_bucket)
            .key("versions-b.txt")
            .body(ByteStream::from_static(b"vb"))
            .send()
            .await
            .expect("put local version fixture b1");
        env.external_client
            .delete_object()
            .bucket(&external_bucket)
            .key("versions-a.txt")
            .send()
            .await
            .expect("delete external version fixture current");
        env.local_client
            .delete_object()
            .bucket(&local_bucket)
            .key("versions-a.txt")
            .send()
            .await
            .expect("delete local version fixture current");

        let aws_list_versions = env.send_external(
            "GET",
            &external_bucket,
            "",
            Some("versions=&max-keys=2"),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_list_versions = env.send_local(
            "GET",
            &local_bucket,
            "",
            Some("versions=&max-keys=2"),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_xml_response_shape_matches(
            "ListObjectVersions",
            &aws_list_versions,
            &local_list_versions,
            &["content-length", "date"],
            COMMON_PRESENCE_ONLY_HEADERS,
            &[
                "Name",
                "VersionId",
                "NextVersionIdMarker",
                "LastModified",
                "ETag",
                "ID",
                "DisplayName",
            ],
        );

        cleanup_versioned_bucket(&env.external_client, &external_bucket).await;
        cleanup_versioned_bucket(&env.local_client, &local_bucket).await;
    });
}

#[test]
fn test_list_objects_v1_without_delimiter_omits_next_marker_like_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;

        for (bucket, client) in [
            (&external_bucket, &env.external_client),
            (&local_bucket, &env.local_client),
        ] {
            client
                .put_object()
                .bucket(bucket)
                .key("aaa")
                .body(ByteStream::from_static(b"a"))
                .send()
                .await
                .expect("put first list-objects fixture");
            client
                .put_object()
                .bucket(bucket)
                .key("zzz")
                .body(ByteStream::from_static(b"z"))
                .send()
                .await
                .expect("put second list-objects fixture");
        }

        let aws_list = env.send_external(
            "GET",
            &external_bucket,
            "",
            Some("max-keys=1"),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_list = env.send_local(
            "GET",
            &local_bucket,
            "",
            Some("max-keys=1"),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );

        assert_xml_response_shape_matches(
            "ListObjectsV1WithoutDelimiter",
            &aws_list,
            &local_list,
            &["content-length", "date", "server", "transfer-encoding"],
            &[],
            &["Name", "LastModified", "ETag", "ID", "DisplayName"],
        );
        assert!(
            xml_tag_text(&aws_list.body, "NextMarker").is_none(),
            "aws unexpectedly included NextMarker: {}",
            aws_list.body
        );
        assert!(
            xml_tag_text(&local_list.body, "NextMarker").is_none(),
            "local unexpectedly included NextMarker: {}",
            local_list.body
        );

        delete_all_and_bucket(
            &env.external_client,
            &external_bucket,
            &["aaa".to_string(), "zzz".to_string()],
        )
        .await;
        delete_all_and_bucket(
            &env.local_client,
            &local_bucket,
            &["aaa".to_string(), "zzz".to_string()],
        )
        .await;
    });
}

#[test]
fn test_list_objects_v2_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;

        for (bucket, client) in [
            (&external_bucket, &env.external_client),
            (&local_bucket, &env.local_client),
        ] {
            client
                .put_object()
                .bucket(bucket)
                .key("aaa")
                .body(ByteStream::from_static(b"a"))
                .send()
                .await
                .expect("put first list-objects-v2 fixture");
            client
                .put_object()
                .bucket(bucket)
                .key("zzz")
                .body(ByteStream::from_static(b"z"))
                .send()
                .await
                .expect("put second list-objects-v2 fixture");
        }

        let aws_list = env.send_external(
            "GET",
            &external_bucket,
            "",
            Some("list-type=2&max-keys=1"),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_list = env.send_local(
            "GET",
            &local_bucket,
            "",
            Some("list-type=2&max-keys=1"),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );

        assert_xml_response_shape_matches(
            "ListObjectsV2",
            &aws_list,
            &local_list,
            &["content-length", "date", "server", "transfer-encoding"],
            &[],
            &["Name", "NextContinuationToken", "LastModified", "ETag"],
        );

        delete_all_and_bucket(
            &env.external_client,
            &external_bucket,
            &["aaa".to_string(), "zzz".to_string()],
        )
        .await;
        delete_all_and_bucket(
            &env.local_client,
            &local_bucket,
            &["aaa".to_string(), "zzz".to_string()],
        )
        .await;
    });
}

#[test]
fn test_list_multipart_uploads_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;

        let external_upload_a = env
            .external_client
            .create_multipart_upload()
            .bucket(&external_bucket)
            .key("mpu-a.txt")
            .send()
            .await
            .expect("create external multipart upload a");
        let local_upload_a = env
            .local_client
            .create_multipart_upload()
            .bucket(&local_bucket)
            .key("mpu-a.txt")
            .send()
            .await
            .expect("create local multipart upload a");
        let external_upload_b = env
            .external_client
            .create_multipart_upload()
            .bucket(&external_bucket)
            .key("mpu-b.txt")
            .send()
            .await
            .expect("create external multipart upload b");
        let local_upload_b = env
            .local_client
            .create_multipart_upload()
            .bucket(&local_bucket)
            .key("mpu-b.txt")
            .send()
            .await
            .expect("create local multipart upload b");

        let aws_list_uploads = env.send_external(
            "GET",
            &external_bucket,
            "",
            Some("uploads=&max-uploads=1"),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_list_uploads = env.send_local(
            "GET",
            &local_bucket,
            "",
            Some("uploads=&max-uploads=1"),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_xml_response_shape_matches(
            "ListMultipartUploads",
            &aws_list_uploads,
            &local_list_uploads,
            &["content-length", "date"],
            COMMON_PRESENCE_ONLY_HEADERS,
            &[
                "Bucket",
                "UploadId",
                "NextUploadIdMarker",
                "Initiated",
                "ID",
                "DisplayName",
            ],
        );

        env.external_client
            .abort_multipart_upload()
            .bucket(&external_bucket)
            .key("mpu-a.txt")
            .upload_id(
                external_upload_a
                    .upload_id()
                    .expect("external multipart upload id a"),
            )
            .send()
            .await
            .expect("abort external multipart upload a");
        env.external_client
            .abort_multipart_upload()
            .bucket(&external_bucket)
            .key("mpu-b.txt")
            .upload_id(
                external_upload_b
                    .upload_id()
                    .expect("external multipart upload id b"),
            )
            .send()
            .await
            .expect("abort external multipart upload b");
        env.local_client
            .abort_multipart_upload()
            .bucket(&local_bucket)
            .key("mpu-a.txt")
            .upload_id(
                local_upload_a
                    .upload_id()
                    .expect("local multipart upload id a"),
            )
            .send()
            .await
            .expect("abort local multipart upload a");
        env.local_client
            .abort_multipart_upload()
            .bucket(&local_bucket)
            .key("mpu-b.txt")
            .upload_id(
                local_upload_b
                    .upload_id()
                    .expect("local multipart upload id b"),
            )
            .send()
            .await
            .expect("abort local multipart upload b");

        delete_all_and_bucket(&env.external_client, &external_bucket, &[]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[]).await;
    });
}
