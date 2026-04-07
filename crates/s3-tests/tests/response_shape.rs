use std::collections::BTreeMap;

use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
use aws_sdk_s3::Client;
use s3_tests::{
    build_client_with_ca, delete_all_and_bucket, send_signed_request_with_credentials,
    unique_bucket, RawResponse, SignedRequestCredentials, TestServer, CTX,
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

fn normalized_headers(
    headers: &[(String, String)],
    ignored_headers: &[&str],
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
        let value = if IGNORE_VALUE_RESPONSE_HEADERS.contains(&name.as_str()) {
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
) {
    assert_eq!(
        local.status, aws.status,
        "{operation}: status mismatch\naws: {aws:?}\nlocal: {local:?}"
    );
    assert_eq!(
        normalized_headers(&local.headers, ignored_headers),
        normalized_headers(&aws.headers, ignored_headers),
        "{operation}: normalized header mismatch\naws headers: {:?}\nlocal headers: {:?}",
        normalized_headers(&aws.headers, ignored_headers),
        normalized_headers(&local.headers, ignored_headers),
    );
    assert_eq!(
        local.body, aws.body,
        "{operation}: body mismatch\naws body: {}\nlocal body: {}",
        aws.body, local.body
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
        assert_response_shape_matches("HeadBucket", &aws_head, &local_head, &[]);

        delete_all_and_bucket(&env.external_client, &external_bucket, &[]).await;
        delete_all_and_bucket(&env.local_client, &local_bucket, &[]).await;
    });
}
