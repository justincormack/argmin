use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use s3_tests::{
    aws_sdk_s3::{
        self,
        primitives::{ByteStream, DateTime},
        types::{
            BucketLifecycleConfiguration, BucketLocationConstraint, BucketVersioningStatus,
            CorsConfiguration, CorsRule, CreateBucketConfiguration, DefaultRetention,
            ExpirationStatus, LifecycleExpiration, LifecycleRule, LifecycleRuleFilter,
            ObjectLockConfiguration, ObjectLockEnabled, ObjectLockLegalHold,
            ObjectLockLegalHoldStatus, ObjectLockMode, ObjectLockRetention,
            ObjectLockRetentionMode, ObjectLockRule, ObjectOwnership, OwnershipControls,
            OwnershipControlsRule, PublicAccessBlockConfiguration, ServerSideEncryption,
            ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
            ServerSideEncryptionRule, Tag, Tagging, VersioningConfiguration,
        },
        Client,
    },
    build_client_with_ca, build_test_agent, cleanup_versioned_bucket, content_md5_header,
    delete_all_and_bucket, post_object_raw_to_test_endpoint_with_headers,
    put_bucket_lifecycle_with_md5, send_signed_request_with_credentials,
    sigv4_post_fields_for_credentials, unique_bucket, RawResponse, SignedRequestCredentials,
    TestServer, CTX,
};
use s3_types::is_legacy_create_bucket_region;

const COMMON_TRANSPORT_IGNORED_HEADERS: &[&str] = &["connection", "date", "server"];
const COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS: &[&str] =
    &["connection", "date", "server", "etag"];
const COMMON_TRANSPORT_AND_TRANSFER_ENCODING_IGNORED_HEADERS: &[&str] =
    &["connection", "date", "server", "transfer-encoding"];
const COMMON_PRESENCE_ONLY_HEADERS: &[&str] = &["last-modified"];
const AWS_MIN_MULTIPART_PART_SIZE: usize = 5 * 1024 * 1024;
const SYSTEM_METADATA_SIZE_LIMIT: usize = 2 * 1024;
const WEBSITE_REDIRECT_HEADER_NAME: &str = "x-amz-website-redirect-location";
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
        std::env::var_os("S3_TEST_ENDPOINT")?;

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

fn send_anonymous_get(
    endpoint: &str,
    tls_ca_pem: Option<&[u8]>,
    bucket: &str,
    key: &str,
    query: Option<&str>,
) -> RawResponse {
    let url = object_url(endpoint, bucket, key, query);
    let agent = build_test_agent(endpoint, tls_ca_pem, std::time::Duration::from_secs(30));
    let mut response = agent
        .get(&url)
        .call()
        .expect("anonymous GET transport error");
    let headers = response
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                value
                    .to_str()
                    .expect("response header is valid utf-8")
                    .to_string(),
            )
        })
        .collect();
    RawResponse {
        status: response.status().as_u16(),
        headers,
        body: response.body_mut().read_to_string().unwrap_or_default(),
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
        loop {
            let Some(relative_start) = normalized[search_from..].find(&start_tag) else {
                break;
            };
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

    loop {
        let Some(relative_start) = body[search_from..].find(&start_tag) else {
            break;
        };
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

fn redirect_value_with_len(len: usize) -> String {
    assert!(len >= 1, "redirect length must allow a leading slash");
    format!("/{}", "r".repeat(len - 1))
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
fn test_object_website_redirect_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let key = "shape-redirect-object.txt";
        let redirect = "/docs/landing.html";

        let aws_put = env.send_external(
            "PUT",
            &external_bucket,
            key,
            None,
            b"redirect-body",
            [(WEBSITE_REDIRECT_HEADER_NAME, redirect)],
        );
        let local_put = env.send_local(
            "PUT",
            &local_bucket,
            key,
            None,
            b"redirect-body",
            [(WEBSITE_REDIRECT_HEADER_NAME, redirect)],
        );
        assert_response_shape_matches(
            "PutObjectWebsiteRedirect",
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
            "GetObjectWebsiteRedirect",
            &aws_get,
            &local_get,
            COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
        );
        assert_eq!(
            response_header_value(&aws_get, WEBSITE_REDIRECT_HEADER_NAME),
            Some(redirect)
        );
        assert_eq!(
            response_header_value(&local_get, WEBSITE_REDIRECT_HEADER_NAME),
            Some(redirect)
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
            "HeadObjectWebsiteRedirect",
            &aws_head,
            &local_head,
            COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
        );
        assert_eq!(
            response_header_value(&aws_head, WEBSITE_REDIRECT_HEADER_NAME),
            Some(redirect)
        );
        assert_eq!(
            response_header_value(&local_head, WEBSITE_REDIRECT_HEADER_NAME),
            Some(redirect)
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[key.to_string()]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[key.to_string()]).await;
    });
}

#[test]
fn test_object_website_redirect_invalid_value_error_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let key = "shape-invalid-redirect.txt";

        let aws_put = env.send_external(
            "PUT",
            &external_bucket,
            key,
            None,
            b"invalid-redirect-body",
            [(WEBSITE_REDIRECT_HEADER_NAME, "docs/landing.html")],
        );
        let local_put = env.send_local(
            "PUT",
            &local_bucket,
            key,
            None,
            b"invalid-redirect-body",
            [(WEBSITE_REDIRECT_HEADER_NAME, "docs/landing.html")],
        );
        assert_xml_response_shape_matches(
            "PutObjectInvalidWebsiteRedirect",
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
fn test_object_website_redirect_metadata_too_large_error_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let key = "shape-redirect-metadata-too-large.txt";
        let redirect_len = SYSTEM_METADATA_SIZE_LIMIT - WEBSITE_REDIRECT_HEADER_NAME.len() + 1;
        let redirect = redirect_value_with_len(redirect_len);

        let aws_put = env.send_external(
            "PUT",
            &external_bucket,
            key,
            None,
            b"redirect-too-large-body",
            [(WEBSITE_REDIRECT_HEADER_NAME, redirect.as_str())],
        );
        let local_put = env.send_local(
            "PUT",
            &local_bucket,
            key,
            None,
            b"redirect-too-large-body",
            [(WEBSITE_REDIRECT_HEADER_NAME, redirect.as_str())],
        );
        assert_xml_response_shape_matches(
            "PutObjectWebsiteRedirectMetadataTooLarge",
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
fn test_object_system_metadata_headers_round_trip_raw_values_match_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let cases = [
            ("Cache-Control", "max-age=60,private"),
            ("Content-Disposition", "attachment;filename=\"report.pdf\""),
            ("Content-Encoding", "gzip,br"),
            ("Content-Language", "en-US,fr-CA"),
            ("Content-Type", "text/plain;charset=utf-8"),
            ("Expires", "Mon, 15 Jan 2024 12:30:45 GMT"),
        ];

        for (index, (header_name, header_value)) in cases.iter().enumerate() {
            let (external_bucket, local_bucket) = env.create_bucket_pair().await;
            let key = format!("raw-header-roundtrip-{index}");

            let aws_put = env.send_external(
                "PUT",
                &external_bucket,
                &key,
                None,
                b"header-roundtrip-body",
                [(*header_name, *header_value)],
            );
            let local_put = env.send_local(
                "PUT",
                &local_bucket,
                &key,
                None,
                b"header-roundtrip-body",
                [(*header_name, *header_value)],
            );
            assert_response_shape_matches(
                &format!("PutObjectRawSystemMetadata[{header_name}]"),
                &aws_put,
                &local_put,
                COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
                COMMON_PRESENCE_ONLY_HEADERS,
            );

            let aws_head = env.send_external(
                "HEAD",
                &external_bucket,
                &key,
                None,
                b"",
                std::iter::empty::<(&str, &str)>(),
            );
            let local_head = env.send_local(
                "HEAD",
                &local_bucket,
                &key,
                None,
                b"",
                std::iter::empty::<(&str, &str)>(),
            );
            assert_response_shape_matches(
                &format!("HeadObjectRawSystemMetadata[{header_name}]"),
                &aws_head,
                &local_head,
                COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
                COMMON_PRESENCE_ONLY_HEADERS,
            );
            assert_eq!(
                response_header_value(&aws_head, header_name),
                Some(*header_value),
                "AWS {header_name} should round-trip exactly"
            );
            assert_eq!(
                response_header_value(&local_head, header_name),
                Some(*header_value),
                "local {header_name} should round-trip exactly"
            );

            delete_all_and_bucket(
                &env.external_client,
                &external_bucket,
                std::slice::from_ref(&key),
            )
            .await;
            delete_all_and_bucket(&env.local_client, &local_bucket, std::slice::from_ref(&key))
                .await;
        }
    });
}

#[test]
fn test_range_and_override_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let key = "shape-range.txt";
        let body = b"abcdefghij";

        let aws_put = env.send_external(
            "PUT",
            &external_bucket,
            key,
            None,
            body,
            [("Content-Type", "application/octet-stream")],
        );
        let local_put = env.send_local(
            "PUT",
            &local_bucket,
            key,
            None,
            body,
            [("Content-Type", "application/octet-stream")],
        );
        assert_response_shape_matches(
            "PutObjectRangeFixture",
            &aws_put,
            &local_put,
            COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
        );

        let aws_range = env.send_external(
            "GET",
            &external_bucket,
            key,
            None,
            b"",
            [("Range", "bytes=2-5")],
        );
        let local_range = env.send_local(
            "GET",
            &local_bucket,
            key,
            None,
            b"",
            [("Range", "bytes=2-5")],
        );
        assert_response_shape_matches(
            "GetObjectRange",
            &aws_range,
            &local_range,
            COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
        );

        let override_query = concat!(
            "response-content-type=text%2Fhtml",
            "&response-content-disposition=attachment%3B%20filename%3D%22override.txt%22"
        );
        let aws_override = env.send_external(
            "GET",
            &external_bucket,
            key,
            Some(override_query),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_override = env.send_local(
            "GET",
            &local_bucket,
            key,
            Some(override_query),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_response_shape_matches(
            "GetObjectOverrides",
            &aws_override,
            &local_override,
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
fn test_get_object_access_denied_error_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };

        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let key = "private.txt";
        let body = b"secret";

        env.external_client
            .put_object()
            .bucket(&external_bucket)
            .key(key)
            .body(ByteStream::from_static(body))
            .send()
            .await
            .expect("put external private object");
        env.local_client
            .put_object()
            .bucket(&local_bucket)
            .key(key)
            .body(ByteStream::from_static(body))
            .send()
            .await
            .expect("put local private object");

        let aws_error = send_anonymous_get(CTX.endpoint(), None, &external_bucket, key, None);
        let local_error = send_anonymous_get(
            env.local_server.endpoint(),
            env.local_server.tls_ca_pem(),
            &local_bucket,
            key,
            None,
        );
        assert_xml_response_shape_matches(
            "GetObjectAccessDenied",
            &aws_error,
            &local_error,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &["RequestId", "HostId"],
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[key.to_string()]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[key.to_string()]).await;
    });
}

#[test]
fn test_checksum_mode_object_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let key = "shape-checksum-mode.txt";
        let body = b"checksum-mode-body";

        let aws_put = env.send_external(
            "PUT",
            &external_bucket,
            key,
            None,
            body,
            [("Content-Type", "application/octet-stream")],
        );
        let local_put = env.send_local(
            "PUT",
            &local_bucket,
            key,
            None,
            body,
            [("Content-Type", "application/octet-stream")],
        );
        assert_response_shape_matches(
            "PutObjectChecksumFixture",
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
            [("x-amz-checksum-mode", "ENABLED")],
        );
        let local_get = env.send_local(
            "GET",
            &local_bucket,
            key,
            None,
            b"",
            [("x-amz-checksum-mode", "ENABLED")],
        );
        assert_response_shape_matches(
            "GetObjectChecksumMode",
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
            [("x-amz-checksum-mode", "ENABLED")],
        );
        let local_head = env.send_local(
            "HEAD",
            &local_bucket,
            key,
            None,
            b"",
            [("x-amz-checksum-mode", "ENABLED")],
        );
        assert_response_shape_matches(
            "HeadObjectChecksumMode",
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
fn test_versioned_object_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        enable_bucket_versioning(&env.external_client, &external_bucket).await;
        enable_bucket_versioning(&env.local_client, &local_bucket).await;

        let key = "shape-versioned.txt";
        let body = b"versioned-body";
        let external_put = env
            .external_client
            .put_object()
            .bucket(&external_bucket)
            .key(key)
            .body(ByteStream::from_static(body))
            .send()
            .await
            .expect("put external versioned object");
        let local_put = env
            .local_client
            .put_object()
            .bucket(&local_bucket)
            .key(key)
            .body(ByteStream::from_static(body))
            .send()
            .await
            .expect("put local versioned object");
        let external_version = external_put
            .version_id()
            .expect("external version id")
            .to_string();
        let local_version = local_put
            .version_id()
            .expect("local version id")
            .to_string();

        let aws_current_get = env.send_external(
            "GET",
            &external_bucket,
            key,
            None,
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_current_get = env.send_local(
            "GET",
            &local_bucket,
            key,
            None,
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_response_shape_matches(
            "GetObjectVersionedCurrent",
            &aws_current_get,
            &local_current_get,
            COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
            &["last-modified", "x-amz-version-id"],
        );

        let aws_version_get = env.send_external(
            "GET",
            &external_bucket,
            key,
            Some(&format!("versionId={external_version}")),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_version_get = env.send_local(
            "GET",
            &local_bucket,
            key,
            Some(&format!("versionId={local_version}")),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_response_shape_matches(
            "GetObjectExplicitVersion",
            &aws_version_get,
            &local_version_get,
            COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
            &["last-modified", "x-amz-version-id"],
        );

        let aws_version_head = env.send_external(
            "HEAD",
            &external_bucket,
            key,
            Some(&format!("versionId={external_version}")),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_version_head = env.send_local(
            "HEAD",
            &local_bucket,
            key,
            Some(&format!("versionId={local_version}")),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_response_shape_matches(
            "HeadObjectExplicitVersion",
            &aws_version_head,
            &local_version_head,
            COMMON_TRANSPORT_AND_ETAG_IGNORED_HEADERS,
            &["last-modified", "x-amz-version-id"],
        );

        cleanup_versioned_bucket(&env.external_client, &external_bucket).await;
        cleanup_versioned_bucket(&env.local_client, &local_bucket).await;
    });
}

#[test]
fn test_get_bucket_location_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;

        let aws_get = env.send_external(
            "GET",
            &external_bucket,
            "",
            Some("location="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_get = env.send_local(
            "GET",
            &local_bucket,
            "",
            Some("location="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );

        assert_xml_response_shape_matches(
            "GetBucketLocation",
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
fn test_get_bucket_versioning_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        enable_bucket_versioning(&env.external_client, &external_bucket).await;
        enable_bucket_versioning(&env.local_client, &local_bucket).await;

        let aws_get = env.send_external(
            "GET",
            &external_bucket,
            "",
            Some("versioning="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_get = env.send_local(
            "GET",
            &local_bucket,
            "",
            Some("versioning="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );

        assert_xml_response_shape_matches(
            "GetBucketVersioning",
            &aws_get,
            &local_get,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &[],
        );

        cleanup_versioned_bucket(&env.external_client, &external_bucket).await;
        cleanup_versioned_bucket(&env.local_client, &local_bucket).await;
    });
}

#[test]
fn test_get_bucket_encryption_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let encryption = ServerSideEncryptionConfiguration::builder()
            .rules(
                ServerSideEncryptionRule::builder()
                    .apply_server_side_encryption_by_default(
                        ServerSideEncryptionByDefault::builder()
                            .sse_algorithm(ServerSideEncryption::Aes256)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .build()
            .unwrap();

        env.external_client
            .put_bucket_encryption()
            .bucket(&external_bucket)
            .server_side_encryption_configuration(encryption.clone())
            .send()
            .await
            .expect("put external bucket encryption");
        env.local_client
            .put_bucket_encryption()
            .bucket(&local_bucket)
            .server_side_encryption_configuration(encryption)
            .send()
            .await
            .expect("put local bucket encryption");

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
            "GetBucketEncryption",
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
fn test_get_bucket_cors_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let cors = CorsConfiguration::builder()
            .cors_rules(
                CorsRule::builder()
                    .allowed_origins("https://example.com")
                    .allowed_methods("GET")
                    .allowed_methods("PUT")
                    .allowed_headers("*")
                    .expose_headers("x-amz-request-id")
                    .max_age_seconds(3600)
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        env.external_client
            .put_bucket_cors()
            .bucket(&external_bucket)
            .cors_configuration(cors.clone())
            .send()
            .await
            .expect("put external bucket cors");
        env.local_client
            .put_bucket_cors()
            .bucket(&local_bucket)
            .cors_configuration(cors)
            .send()
            .await
            .expect("put local bucket cors");

        let aws_get = env.send_external(
            "GET",
            &external_bucket,
            "",
            Some("cors="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_get = env.send_local(
            "GET",
            &local_bucket,
            "",
            Some("cors="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );

        assert_xml_response_shape_matches(
            "GetBucketCors",
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
fn test_get_bucket_tagging_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let tagging = Tagging::builder()
            .tag_set(Tag::builder().key("env").value("prod").build().unwrap())
            .tag_set(Tag::builder().key("team").value("storage").build().unwrap())
            .build()
            .unwrap();

        env.external_client
            .put_bucket_tagging()
            .bucket(&external_bucket)
            .tagging(tagging.clone())
            .send()
            .await
            .expect("put external bucket tagging");
        env.local_client
            .put_bucket_tagging()
            .bucket(&local_bucket)
            .tagging(tagging)
            .send()
            .await
            .expect("put local bucket tagging");

        let aws_get = env.send_external(
            "GET",
            &external_bucket,
            "",
            Some("tagging="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_get = env.send_local(
            "GET",
            &local_bucket,
            "",
            Some("tagging="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );

        assert_xml_response_shape_matches(
            "GetBucketTagging",
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
fn test_get_bucket_lifecycle_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let lifecycle = BucketLifecycleConfiguration::builder()
            .rules(
                LifecycleRule::builder()
                    .id("expire-current")
                    .filter(LifecycleRuleFilter::builder().prefix("logs/").build())
                    .status(ExpirationStatus::Enabled)
                    .expiration(LifecycleExpiration::builder().days(30).build())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        put_bucket_lifecycle_with_md5(&env.external_client, &external_bucket, lifecycle.clone())
            .send()
            .await
            .expect("put external bucket lifecycle");
        put_bucket_lifecycle_with_md5(&env.local_client, &local_bucket, lifecycle)
            .send()
            .await
            .expect("put local bucket lifecycle");

        let aws_get = env.send_external(
            "GET",
            &external_bucket,
            "",
            Some("lifecycle="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_get = env.send_local(
            "GET",
            &local_bucket,
            "",
            Some("lifecycle="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );

        assert_xml_response_shape_matches(
            "GetBucketLifecycleConfiguration",
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
fn test_get_bucket_public_access_block_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let config = PublicAccessBlockConfiguration::builder()
            .block_public_acls(true)
            .ignore_public_acls(true)
            .block_public_policy(true)
            .restrict_public_buckets(false)
            .build();

        env.external_client
            .put_public_access_block()
            .bucket(&external_bucket)
            .public_access_block_configuration(config.clone())
            .send()
            .await
            .expect("put external public access block");
        env.local_client
            .put_public_access_block()
            .bucket(&local_bucket)
            .public_access_block_configuration(config)
            .send()
            .await
            .expect("put local public access block");

        let aws_get = env.send_external(
            "GET",
            &external_bucket,
            "",
            Some("publicAccessBlock="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_get = env.send_local(
            "GET",
            &local_bucket,
            "",
            Some("publicAccessBlock="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );

        assert_xml_response_shape_matches(
            "GetPublicAccessBlock",
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
fn test_get_bucket_ownership_controls_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        let ownership_controls = OwnershipControls::builder()
            .rules(
                OwnershipControlsRule::builder()
                    .object_ownership(ObjectOwnership::BucketOwnerPreferred)
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        env.external_client
            .put_bucket_ownership_controls()
            .bucket(&external_bucket)
            .ownership_controls(ownership_controls.clone())
            .send()
            .await
            .expect("put external ownership controls");
        env.local_client
            .put_bucket_ownership_controls()
            .bucket(&local_bucket)
            .ownership_controls(ownership_controls)
            .send()
            .await
            .expect("put local ownership controls");

        let aws_get = env.send_external(
            "GET",
            &external_bucket,
            "",
            Some("ownershipControls="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_get = env.send_local(
            "GET",
            &local_bucket,
            "",
            Some("ownershipControls="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );

        assert_xml_response_shape_matches(
            "GetBucketOwnershipControls",
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
fn test_get_bucket_policy_status_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;

        let aws_get = env.send_external(
            "GET",
            &external_bucket,
            "",
            Some("policyStatus="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_get = env.send_local(
            "GET",
            &local_bucket,
            "",
            Some("policyStatus="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );

        assert_xml_response_shape_matches(
            "GetBucketPolicyStatus",
            &aws_get,
            &local_get,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &["RequestId", "HostId"],
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[]).await;
    });
}

#[test]
fn test_get_bucket_acl_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;

        let aws_get = env.send_external(
            "GET",
            &external_bucket,
            "",
            Some("acl="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_get = env.send_local(
            "GET",
            &local_bucket,
            "",
            Some("acl="),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );

        assert_xml_response_shape_matches(
            "GetBucketAcl",
            &aws_get,
            &local_get,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            COMMON_PRESENCE_ONLY_HEADERS,
            &["ID", "DisplayName"],
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
fn test_delete_object_response_shape_matches_aws() {
    s3_tests::run(async {
        let Some(env) = ComparisonEnv::setup().await else {
            return;
        };
        let (external_bucket, local_bucket) = env.create_bucket_pair().await;
        enable_bucket_versioning(&env.external_client, &external_bucket).await;
        enable_bucket_versioning(&env.local_client, &local_bucket).await;

        let key = "shape-delete.txt";
        env.external_client
            .put_object()
            .bucket(&external_bucket)
            .key(key)
            .body(ByteStream::from_static(b"delete-shape"))
            .send()
            .await
            .expect("put external delete fixture");
        env.local_client
            .put_object()
            .bucket(&local_bucket)
            .key(key)
            .body(ByteStream::from_static(b"delete-shape"))
            .send()
            .await
            .expect("put local delete fixture");

        let aws_delete_current = env.send_external(
            "DELETE",
            &external_bucket,
            key,
            None,
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_delete_current = env.send_local(
            "DELETE",
            &local_bucket,
            key,
            None,
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_response_shape_matches(
            "DeleteObjectCurrentVersion",
            &aws_delete_current,
            &local_delete_current,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            &["x-amz-version-id"],
        );

        let external_delete_marker_version =
            response_header_value(&aws_delete_current, "x-amz-version-id")
                .expect("external delete marker version id")
                .to_string();
        let local_delete_marker_version =
            response_header_value(&local_delete_current, "x-amz-version-id")
                .expect("local delete marker version id")
                .to_string();

        let aws_delete_marker = env.send_external(
            "DELETE",
            &external_bucket,
            key,
            Some(&format!("versionId={external_delete_marker_version}")),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        let local_delete_marker = env.send_local(
            "DELETE",
            &local_bucket,
            key,
            Some(&format!("versionId={local_delete_marker_version}")),
            b"",
            std::iter::empty::<(&str, &str)>(),
        );
        assert_response_shape_matches(
            "DeleteObjectDeleteMarkerVersion",
            &aws_delete_marker,
            &local_delete_marker,
            COMMON_TRANSPORT_IGNORED_HEADERS,
            &["x-amz-version-id"],
        );

        cleanup_versioned_bucket(&env.external_client, &external_bucket).await;
        cleanup_versioned_bucket(&env.local_client, &local_bucket).await;
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
