use std::collections::BTreeMap;

use s3_diff_tests::require_external_diff_test_env;
use s3_tests::{
    aws_sdk_s3::{
        primitives::ByteStream,
        types::{
            BucketLocationConstraint, BucketVersioningStatus, CreateBucketConfiguration, Tag,
            Tagging, VersioningConfiguration,
        },
        Client,
    },
    build_client_with_ca, cleanup_versioned_bucket, content_md5_header, delete_all_and_bucket,
    post_object_raw_to_test_endpoint_with_headers, send_signed_request_with_credentials,
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
