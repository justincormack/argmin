use std::collections::BTreeMap;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketLocationConstraint, BucketVersioningStatus, CreateBucketConfiguration,
    VersioningConfiguration,
};
use aws_sdk_s3::Client;
use s3_tests::{
    build_client_with_ca, cleanup_versioned_bucket, delete_all_and_bucket,
    send_signed_request_with_credentials, unique_bucket, RawResponse, SignedRequestCredentials,
    TestServer, CTX,
};
use s3_types::is_legacy_create_bucket_region;

const DROPPED_RESPONSE_HEADERS: &[&str] = &[
    "connection",
    "date",
    "server",
    "x-amz-id-2",
    "x-amz-request-id",
];
const IGNORE_VALUE_RESPONSE_HEADERS: &[&str] = &["last-modified"];
// The repo intentionally does not match AWS exact ETag semantics yet.
// See plans/completed/sse-c-encryption-plan.md and plans/completed/territory-map.md.
const KNOWN_ETAG_DIVERGENCE_HEADERS: &[&str] = &["etag"];
const AWS_MIN_MULTIPART_PART_SIZE: usize = 5 * 1024 * 1024;

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
        let local_server = TestServer::start().await;
        let local_client = build_client_with_ca(
            local_server.endpoint(),
            s3_tests::server::TEST_ACCESS_KEY,
            s3_tests::server::TEST_SECRET_KEY,
            s3_tests::server::TEST_REGION,
            local_server.tls_ca_pem(),
        )
        .await;

        Some(Self {
            external_client: CTX.client().clone(),
            external_region: CTX.region().to_string(),
            local_client,
            local_server,
        })
    }

    async fn create_bucket_pair(&self) -> (String, String) {
        let external_bucket = unique_bucket();
        let local_bucket = unique_bucket();
        create_bucket_in_region(
            &self.external_client,
            &external_bucket,
            &self.external_region,
        )
        .await;
        create_bucket_in_region(
            &self.local_client,
            &local_bucket,
            s3_tests::server::TEST_REGION,
        )
        .await;
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
                region: s3_tests::server::TEST_REGION,
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

fn normalized_headers(
    headers: &[(String, String)],
    ignored_headers: &[&str],
    presence_only_headers: &[&str],
) -> BTreeMap<String, Vec<String>> {
    let mut normalized = BTreeMap::new();
    for (name, value) in headers {
        let name = name.to_ascii_lowercase();
        if DROPPED_RESPONSE_HEADERS.contains(&name.as_str()) {
            continue;
        }
        if ignored_headers.contains(&name.as_str()) {
            continue;
        }
        let value = if IGNORE_VALUE_RESPONSE_HEADERS.contains(&name.as_str())
            || presence_only_headers.contains(&name.as_str())
        {
            "<present>".to_string()
        } else {
            value.clone()
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

fn normalize_xml_text_tags(body: &str, tags: &[&str]) -> String {
    let mut normalized = body.to_string();
    for tag in tags {
        let start_tag = format!("<{tag}>");
        let end_tag = format!("</{tag}>");
        let replacement = format!("{start_tag}<present>{end_tag}");
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
            KNOWN_ETAG_DIVERGENCE_HEADERS,
            &[],
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
            KNOWN_ETAG_DIVERGENCE_HEADERS,
            &[],
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
            KNOWN_ETAG_DIVERGENCE_HEADERS,
            &[],
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[key.to_string()]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[key.to_string()]).await;
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
            KNOWN_ETAG_DIVERGENCE_HEADERS,
            &[],
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
            KNOWN_ETAG_DIVERGENCE_HEADERS,
            &[],
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
            KNOWN_ETAG_DIVERGENCE_HEADERS,
            &[],
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
        if env.external_region != s3_tests::server::TEST_REGION {
            return;
        }

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
        assert_response_shape_matches("HeadBucket", &aws_head, &local_head, &[], &[]);

        delete_all_and_bucket(&env.external_client, &external_bucket, &[]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[]).await;
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
            KNOWN_ETAG_DIVERGENCE_HEADERS,
            &[],
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
            KNOWN_ETAG_DIVERGENCE_HEADERS,
            &[],
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
            KNOWN_ETAG_DIVERGENCE_HEADERS,
            &[],
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
            KNOWN_ETAG_DIVERGENCE_HEADERS,
            &["x-amz-version-id"],
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
            KNOWN_ETAG_DIVERGENCE_HEADERS,
            &["x-amz-version-id"],
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
            KNOWN_ETAG_DIVERGENCE_HEADERS,
            &["x-amz-version-id"],
        );

        cleanup_versioned_bucket(&env.external_client, &external_bucket).await;
        cleanup_versioned_bucket(&env.local_client, &local_bucket).await;
    });
}

#[test]
fn test_copy_object_response_headers_match_aws() {
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
            KNOWN_ETAG_DIVERGENCE_HEADERS,
            &[],
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
        assert_response_headers_match(
            "CopyObject",
            &aws_copy,
            &local_copy,
            &["content-length"],
            &[],
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
            &[],
            &[],
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
            KNOWN_ETAG_DIVERGENCE_HEADERS,
            &[],
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
        assert_response_headers_match("ListParts", &aws_list_parts, &local_list_parts, &[], &[]);

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
            &[],
            &[],
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
            KNOWN_ETAG_DIVERGENCE_HEADERS,
            &[],
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
            KNOWN_ETAG_DIVERGENCE_HEADERS,
            &[],
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
            KNOWN_ETAG_DIVERGENCE_HEADERS,
            &[],
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
            &["content-length"],
            &[],
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
            &[],
            &[],
        );

        delete_all_and_bucket(&env.external_client, &external_bucket, &[key.to_string()]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[key.to_string()]).await;
    });
}
