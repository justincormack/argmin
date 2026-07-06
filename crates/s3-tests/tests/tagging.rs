use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart,
    MetadataDirective as CopyMetadataDirective, Tag, Tagging, VersioningConfiguration,
};
use s3_tests::{
    assert_s3_err_code, cleanup_versioned_bucket, content_md5_header,
    delete_bucket_retrying_operation_aborted, err_status, raw_bucket, send_signed_request,
    shape::{assert_shape, chunked_response_headers, shape},
    unique_bucket, SendRetryingOperationAborted, CTX,
};
use serde_json::json;
use std::collections::BTreeSet;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

static BUCKET_POLICY_TEST_GUARD: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

const CONCURRENT_TAGGING_OPERATION_ATTEMPTS: usize = 20;

fn is_operation_aborted<E: ProvideErrorMetadata>(err: &aws_sdk_s3::error::SdkError<E>) -> bool {
    err.as_service_error().and_then(ProvideErrorMetadata::code) == Some("OperationAborted")
}

async fn put_object_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
    tagging: Option<&str>,
) -> aws_sdk_s3::operation::put_object::PutObjectOutput {
    for attempt in 0..CONCURRENT_TAGGING_OPERATION_ATTEMPTS {
        let mut request = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body.clone()));
        if let Some(tagging) = tagging {
            request = request.tagging(tagging);
        }
        match request.send().await {
            Ok(output) => return output,
            Err(err)
                if is_operation_aborted(&err)
                    && attempt + 1 < CONCURRENT_TAGGING_OPERATION_ATTEMPTS =>
            {
                tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
            }
            Err(err) => panic!("put object during tagging setup: {err:?}"),
        }
    }
    panic!("put object during tagging setup did not complete");
}

async fn put_object_result_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
    tagging: Option<&str>,
) -> Result<
    aws_sdk_s3::operation::put_object::PutObjectOutput,
    aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>,
> {
    for attempt in 0..CONCURRENT_TAGGING_OPERATION_ATTEMPTS {
        let mut request = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body.clone()));
        if let Some(tagging) = tagging {
            request = request.tagging(tagging);
        }
        match request.send().await {
            Err(err)
                if is_operation_aborted(&err)
                    && attempt + 1 < CONCURRENT_TAGGING_OPERATION_ATTEMPTS =>
            {
                tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
            }
            result => return result,
        }
    }
    unreachable!("put object result retry loop must return on final attempt");
}

async fn upload_part_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: Vec<u8>,
) -> aws_sdk_s3::operation::upload_part::UploadPartOutput {
    for attempt in 0..CONCURRENT_TAGGING_OPERATION_ATTEMPTS {
        match client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
        {
            Ok(output) => return output,
            Err(err)
                if is_operation_aborted(&err)
                    && attempt + 1 < CONCURRENT_TAGGING_OPERATION_ATTEMPTS =>
            {
                tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
            }
            Err(err) => panic!("upload part during tagging setup: {err:?}"),
        }
    }
    panic!("upload part during tagging setup did not complete");
}

/// Cleanup helper.
async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client
            .delete_object()
            .bucket(bucket)
            .key(*key)
            .send_retrying_operation_aborted("delete object during tagging cleanup")
            .await;
    }
    delete_bucket_retrying_operation_aborted(client, bucket).await;
}

fn tag(key: &str, value: &str) -> Tag {
    Tag::builder().key(key).value(value).build().unwrap()
}

fn tagging(tags: Vec<Tag>) -> Tagging {
    Tagging::builder().set_tag_set(Some(tags)).build().unwrap()
}

async fn wait_for_tag_count(bucket: &str, key: &str, expected_count: usize, description: &str) {
    const MAX_ATTEMPTS: usize = 40;

    let mut last_seen = None;
    for attempt in 0..MAX_ATTEMPTS {
        let result = CTX
            .client()
            .get_object_tagging()
            .bucket(bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await;
        match result {
            Ok(resp) if resp.tag_set().len() == expected_count => return,
            Ok(resp) => last_seen = Some(format!("{} tags", resp.tag_set().len())),
            Err(err) => last_seen = Some(format!("{err:?}")),
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    let last_seen = last_seen.unwrap_or_else(|| "no response".to_string());
    panic!(
        "{description} did not converge for {bucket}/{key}: expected {expected_count} tags, last saw {last_seen}"
    );
}

async fn complete_multipart_upload_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    etag: &str,
) {
    for attempt in 0..CONCURRENT_TAGGING_OPERATION_ATTEMPTS {
        match client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(CompletedPart::builder().e_tag(etag).part_number(1).build())
                    .build(),
            )
            .send()
            .await
        {
            Ok(_) => return,
            Err(err)
                if is_operation_aborted(&err)
                    && attempt + 1 < CONCURRENT_TAGGING_OPERATION_ATTEMPTS =>
            {
                tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
            }
            Err(err) => panic!("complete multipart upload during tagging setup: {err:?}"),
        }
    }
}

async fn copy_object_without_metadata_directive_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    copy_source: &str,
) -> Result<
    aws_sdk_s3::operation::copy_object::CopyObjectOutput,
    aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::copy_object::CopyObjectError>,
> {
    for attempt in 0..CONCURRENT_TAGGING_OPERATION_ATTEMPTS {
        let result = client
            .copy_object()
            .bucket(bucket)
            .key(key)
            .copy_source(copy_source)
            .metadata("foo", "bar")
            .customize()
            .mutate_request(|req| {
                req.headers_mut().remove("x-amz-metadata-directive");
            })
            .send()
            .await;
        match result {
            Err(err)
                if is_operation_aborted(&err)
                    && attempt + 1 < CONCURRENT_TAGGING_OPERATION_ATTEMPTS =>
            {
                tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
            }
            result => return result,
        }
    }
    unreachable!("customized copy retry loop must return on final attempt");
}

async fn wait_for_current_delete_marker_tagging_method_not_allowed(
    bucket: &str,
    key: &str,
    description: &str,
) {
    const MAX_ATTEMPTS: usize = 40;

    let mut last_seen = None;
    for attempt in 0..MAX_ATTEMPTS {
        let result = CTX
            .client()
            .get_object_tagging()
            .bucket(bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await;
        if result.is_err() && err_status(&result) == 405 {
            assert_s3_err_code(&result, "MethodNotAllowed");
            return;
        }
        last_seen = Some(match result {
            Ok(resp) => format!("Ok({} tags)", resp.tag_set().len()),
            Err(err) => format!("{err:?}"),
        });
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    let last_seen = last_seen.unwrap_or_else(|| "no response".to_string());
    panic!(
        "{description} did not converge for {bucket}/{key}: expected 405 MethodNotAllowed, last saw {last_seen}"
    );
}

fn assert_tag_sets_match_unordered(actual: &[Tag], expected: &[Tag]) {
    let actual: BTreeSet<_> = actual
        .iter()
        .map(|tag| (tag.key().to_string(), tag.value().to_string()))
        .collect();
    let expected: BTreeSet<_> = expected
        .iter()
        .map(|tag| (tag.key().to_string(), tag.value().to_string()))
        .collect();
    assert_eq!(actual, expected);
}

fn object_resource(bucket: &str, key: &str) -> String {
    format!("arn:aws:s3:::{bucket}/{key}")
}

fn bucket_wildcard_resource(bucket: &str) -> String {
    format!("arn:aws:s3:::{bucket}/*")
}

fn raw_response_is_operation_aborted(response: &s3_tests::RawResponse) -> bool {
    response.status == 409 && response.body.contains("<Code>OperationAborted</Code>")
}

fn send_raw_retrying_operation_aborted<F>(description: &str, mut send: F) -> s3_tests::RawResponse
where
    F: FnMut() -> s3_tests::RawResponse,
{
    for attempt in 0..CONCURRENT_TAGGING_OPERATION_ATTEMPTS {
        let response = send();
        if !raw_response_is_operation_aborted(&response) {
            return response;
        }
        if attempt + 1 < CONCURRENT_TAGGING_OPERATION_ATTEMPTS {
            std::thread::sleep(Duration::from_millis(10 * (attempt as u64 + 1)));
        }
    }
    panic!("{description} did not complete without OperationAborted");
}

fn put_bucket_tagging_raw(bucket: &str, body: &[u8]) -> s3_tests::RawResponse {
    let url = format!("{}/{bucket}?tagging", CTX.endpoint());
    send_raw_retrying_operation_aborted("put raw bucket tagging", || {
        send_signed_request("PUT", &url, body, [content_md5_header(body)])
    })
}

fn alt_policy_principal() -> serde_json::Value {
    json!({ "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) })
}

#[test]
fn test_bucket_tagging_raw_get_returns_canonical_xml() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        s3_tests::create_bucket(CTX.client(), &bucket)
            .await
            .unwrap();

        let body = br#"
            <Tagging>
                <TagSet>
                    <Tag>
                        <Key>env</Key>
                        <Value>prod</Value>
                    </Tag>
                    <Tag>
                        <Key>team</Key>
                        <Value>storage</Value>
                    </Tag>
                </TagSet>
            </Tagging>
        "#;

        let parsed = server_http::http::xml::parse_tagging_xml(body, 50).unwrap();
        let expected = server_http::http::xml::get_tagging_xml(&parsed);

        let put = put_bucket_tagging_raw(&bucket, body);
        assert_eq!(put.status, 204, "unexpected body: {}", put.body);

        let url = format!("{}/{}?tagging", CTX.endpoint(), bucket);
        let get = send_raw_retrying_operation_aborted("get raw bucket tagging", || {
            send_signed_request("GET", &url, b"", std::iter::empty::<(String, String)>())
        });

        cleanup(&bucket, &[]).await;

        assert_eq!(get.status, 200, "unexpected body: {}", get.body);
        assert_eq!(get.body, expected);
    });
}

fn bucket_policy_document(
    principal: serde_json::Value,
    action: &str,
    resource: String,
    conditions: Option<serde_json::Value>,
) -> String {
    let mut statement = json!({
        "Effect": "Allow",
        "Principal": principal,
        "Action": action,
        "Resource": resource,
    });
    if let Some(conditions) = conditions {
        statement["Condition"] = conditions;
    }
    json!({
        "Version": "2012-10-17",
        "Statement": [statement],
    })
    .to_string()
}

// ── Bucket tagging ──────────────────────────────────────────────────────

#[test]
fn test_put_get_delete_bucket_tagging() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // PUT bucket tagging
        let tags = tagging(vec![tag("env", "prod"), tag("team", "platform")]);
        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(tags)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        // GET bucket tagging
        let result = client
            .get_bucket_tagging()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();

        let tag_set = result.tag_set();
        assert_eq!(tag_set.len(), 2);
        assert!(tag_set
            .iter()
            .any(|t| t.key() == "env" && t.value() == "prod"));
        assert!(tag_set
            .iter()
            .any(|t| t.key() == "team" && t.value() == "platform"));

        // DELETE bucket tagging
        client
            .delete_bucket_tagging()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        // GET after delete should fail with NoSuchTagSet
        let result = client
            .get_bucket_tagging()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await;
        assert!(result.is_err());

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_get_bucket_tagging_not_set() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // GET bucket tagging when not set → error (NoSuchTagSet 404)
        let result = client
            .get_bucket_tagging()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await;
        assert!(result.is_err());

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_delete_bucket_tagging_not_set() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // DELETE bucket tagging when not set → idempotent, should succeed
        client
            .delete_bucket_tagging()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_bucket_tagging_max_tags() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // 50 tags should succeed (bucket limit)
        let tags: Vec<Tag> = (0..50)
            .map(|i| tag(&format!("key{i}"), &format!("val{i}")))
            .collect();
        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(tagging(tags))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let result = client
            .get_bucket_tagging()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert_eq!(result.tag_set().len(), 50);

        cleanup(&bucket, &[]).await;
    });
}

#[test]
fn test_put_bucket_tagging_too_many() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // 51 tags should fail (bucket limit is 50)
        let tags: Vec<Tag> = (0..51)
            .map(|i| tag(&format!("key{i}"), &format!("val{i}")))
            .collect();
        let result = client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(tagging(tags))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await;
        assert!(result.is_err());

        cleanup(&bucket, &[]).await;
    });
}

// ── Object tagging ──────────────────────────────────────────────────────

#[test]
fn test_put_get_delete_object_tagging() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        put_object_retrying_operation_aborted(client, &bucket, "obj", b"hello".to_vec(), None)
            .await;

        // PUT object tagging
        let tags = tagging(vec![tag("env", "staging"), tag("cost-center", "123")]);
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tags)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        // GET object tagging
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();

        let tag_set = result.tag_set();
        assert_eq!(tag_set.len(), 2);
        assert!(tag_set
            .iter()
            .any(|t| t.key() == "env" && t.value() == "staging"));
        assert!(tag_set
            .iter()
            .any(|t| t.key() == "cost-center" && t.value() == "123"));

        // DELETE object tagging
        client
            .delete_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        // GET after delete should return empty tag set
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert!(result.tag_set().is_empty());

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_get_object_tagging_not_set() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        put_object_retrying_operation_aborted(client, &bucket, "obj", b"hello".to_vec(), None)
            .await;

        // GET object tagging when not set → returns empty TagSet (not 404)
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert!(result.tag_set().is_empty());

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_delete_object_tagging_not_set() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        put_object_retrying_operation_aborted(client, &bucket, "obj", b"hello".to_vec(), None)
            .await;

        // DELETE object tagging when not set → idempotent, succeeds
        client
            .delete_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_put_object_tagging_max_tags() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        put_object_retrying_operation_aborted(client, &bucket, "obj", b"hello".to_vec(), None)
            .await;

        // 10 tags should succeed
        let tags: Vec<Tag> = (0..10)
            .map(|i| tag(&format!("key{i}"), &format!("val{i}")))
            .collect();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(tags))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert_eq!(result.tag_set().len(), 10);

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_put_object_tagging_too_many() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        put_object_retrying_operation_aborted(client, &bucket, "obj", b"hello".to_vec(), None)
            .await;

        // 11 tags should fail
        let tags: Vec<Tag> = (0..11)
            .map(|i| tag(&format!("key{i}"), &format!("val{i}")))
            .collect();
        let result = client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(tags))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await;
        assert!(result.is_err());

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_put_object_tagging_overwrite() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        put_object_retrying_operation_aborted(client, &bucket, "obj", b"hello".to_vec(), None)
            .await;

        // Set initial tags
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(vec![tag("old", "value")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        // Overwrite with new tags
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(vec![tag("new", "value2")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();

        let tag_set = result.tag_set();
        assert_eq!(tag_set.len(), 1);
        assert_eq!(tag_set[0].key(), "new");
        assert_eq!(tag_set[0].value(), "value2");

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Inline tagging (x-amz-tagging header) ───────────────────────────────

#[test]
fn test_put_object_with_tagging_header() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // PutObject with x-amz-tagging header including a bare key (empty value)
        put_object_retrying_operation_aborted(
            client,
            &bucket,
            "obj",
            b"hello".to_vec(),
            Some("foo=bar&bar"),
        )
        .await;

        // Verify via GetObjectTagging
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();

        let tag_set = result.tag_set();
        assert_eq!(tag_set.len(), 2);
        assert!(tag_set
            .iter()
            .any(|t| t.key() == "foo" && t.value() == "bar"));
        assert!(tag_set.iter().any(|t| t.key() == "bar" && t.value() == ""));

        cleanup(&bucket, &["obj"]).await;
    });
}
#[test]
fn test_get_object_tagging_count_header() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // Put object with tags
        put_object_retrying_operation_aborted(
            client,
            &bucket,
            "obj",
            b"hello".to_vec(),
            Some("a=1&b=2&c=3"),
        )
        .await;

        // Use AWS SDK's GetObject which exposes tag_count
        let result = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();

        assert_eq!(result.tag_count(), Some(3));

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_head_object_tagging_count_header() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // Put object with tags
        put_object_retrying_operation_aborted(
            client,
            &bucket,
            "obj",
            b"hello".to_vec(),
            Some("a=1&b=2"),
        )
        .await;

        // HeadObject should return x-amz-tagging-count
        let result = client
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert_eq!(result.tag_count(), Some(2));

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── CopyObject with tagging ────────────────────────────────────────────

#[test]
fn test_copy_object_with_tagging() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // Put source object
        put_object_retrying_operation_aborted(client, &bucket, "src", b"data".to_vec(), None).await;

        // CopyObject with x-amz-tagging + REPLACE directive
        client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .tagging("copied=true&env=test")
            .tagging_directive(aws_sdk_s3::types::TaggingDirective::Replace)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        // Verify tags on destination
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("dst")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();

        let tag_set = result.tag_set();
        assert_eq!(tag_set.len(), 2);
        assert!(tag_set
            .iter()
            .any(|t| t.key() == "copied" && t.value() == "true"));
        assert!(tag_set
            .iter()
            .any(|t| t.key() == "env" && t.value() == "test"));

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

// ── Ceph-matching tests ──────────────────────────────────────────────

/// Matches ceph test_set_bucket_tagging: get (404), put, get, delete, get (404).
#[test]
fn test_set_bucket_tagging() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // GET before set → NoSuchTagSet
        let result = client
            .get_bucket_tagging()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await;
        assert!(result.is_err());

        // PUT single tag
        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(tagging(vec![tag("Hello", "World")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        // GET → verify
        let result = client
            .get_bucket_tagging()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        let tag_set = result.tag_set();
        assert_eq!(tag_set.len(), 1);
        assert_eq!(tag_set[0].key(), "Hello");
        assert_eq!(tag_set[0].value(), "World");

        // DELETE
        client
            .delete_bucket_tagging()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        // GET after delete → NoSuchTagSet
        let result = client
            .get_bucket_tagging()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await;
        assert!(result.is_err());

        cleanup(&bucket, &[]).await;
    });
}

/// Matches ceph test_get_obj_tagging: put 2 tags, get, verify match.
#[test]
fn test_get_obj_tagging() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        put_object_retrying_operation_aborted(client, &bucket, "obj", b"hello".to_vec(), None)
            .await;

        let input_tags = vec![tag("0", "0"), tag("1", "1")];
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(input_tags))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        let tag_set = result.tag_set();
        assert_eq!(tag_set.len(), 2);
        assert!(tag_set.iter().any(|t| t.key() == "0" && t.value() == "0"));
        assert!(tag_set.iter().any(|t| t.key() == "1" && t.value() == "1"));

        cleanup(&bucket, &["obj"]).await;
    });
}

/// Matches ceph test_get_obj_head_tagging: put 2 tags, HEAD, check x-amz-tagging-count.
#[test]
fn test_get_obj_head_tagging() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        put_object_retrying_operation_aborted(client, &bucket, "obj", b"hello".to_vec(), None)
            .await;

        let input_tags = vec![tag("0", "0"), tag("1", "1")];
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(input_tags))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let result = client
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert_eq!(result.tag_count(), Some(2));

        cleanup(&bucket, &["obj"]).await;
    });
}

/// Matches ceph test_put_max_tags: put 10 tags (max), get, verify all match.
#[test]
fn test_put_max_tags() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        put_object_retrying_operation_aborted(client, &bucket, "obj", b"hello".to_vec(), None)
            .await;

        let input_tags: Vec<Tag> = (0..10)
            .map(|i| tag(&i.to_string(), &i.to_string()))
            .collect();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(input_tags.clone()))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        let tag_set = result.tag_set();
        assert_eq!(tag_set.len(), 10);
        for i in 0..10 {
            let s = i.to_string();
            assert!(tag_set.iter().any(|t| t.key() == s && t.value() == s));
        }

        cleanup(&bucket, &["obj"]).await;
    });
}

/// Matches ceph test_put_excess_tags: 11 tags → 400 InvalidTag, no tags stored.
#[test]
fn test_put_excess_tags() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        put_object_retrying_operation_aborted(client, &bucket, "obj", b"hello".to_vec(), None)
            .await;

        let input_tags: Vec<Tag> = (0..11)
            .map(|i| tag(&i.to_string(), &i.to_string()))
            .collect();
        let result = client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(input_tags))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await;
        assert!(result.is_err());

        // No tags should be stored
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert!(result.tag_set().is_empty());

        cleanup(&bucket, &["obj"]).await;
    });
}

/// Matches ceph test_put_modify_tags: set 2 tags, verify, replace with 1 tag, verify.
#[test]
fn test_put_modify_tags() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        put_object_retrying_operation_aborted(client, &bucket, "obj", b"hello".to_vec(), None)
            .await;

        // Set initial tags
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(vec![tag("key", "val"), tag("key2", "val2")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        let tag_set = result.tag_set();
        assert_eq!(tag_set.len(), 2);
        assert!(tag_set
            .iter()
            .any(|t| t.key() == "key" && t.value() == "val"));
        assert!(tag_set
            .iter()
            .any(|t| t.key() == "key2" && t.value() == "val2"));

        // Replace with different tags
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(vec![tag("key3", "val3")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        let tag_set = result.tag_set();
        assert_eq!(tag_set.len(), 1);
        assert_eq!(tag_set[0].key(), "key3");
        assert_eq!(tag_set[0].value(), "val3");

        cleanup(&bucket, &["obj"]).await;
    });
}

/// Matches ceph test_put_delete_tags: put 2 tags, verify, delete (204), verify empty.
#[test]
fn test_put_delete_tags() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        put_object_retrying_operation_aborted(client, &bucket, "obj", b"hello".to_vec(), None)
            .await;

        let input_tags = vec![tag("0", "0"), tag("1", "1")];
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(input_tags))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert_eq!(result.tag_set().len(), 2);

        client
            .delete_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert!(result.tag_set().is_empty());

        cleanup(&bucket, &["obj"]).await;
    });
}

/// Matches ceph test_put_obj_with_tags: PutObject with x-amz-tagging "foo=bar&bar",
/// verify body, verify tags including bare key with empty value.
#[test]
fn test_put_obj_with_tags() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let data = "A".repeat(100);
        put_object_retrying_operation_aborted(
            client,
            &bucket,
            "obj",
            data.clone().into_bytes(),
            Some("foo=bar&bar"),
        )
        .await;

        // Verify body
        let result = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        let body = result.body.collect().await.unwrap().into_bytes().to_vec();
        assert_eq!(String::from_utf8(body).unwrap(), data);

        // Verify tags
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        let tag_set = result.tag_set();
        assert_eq!(tag_set.len(), 2);
        assert!(tag_set
            .iter()
            .any(|t| t.key() == "foo" && t.value() == "bar"));
        assert!(tag_set.iter().any(|t| t.key() == "bar" && t.value() == ""));

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Tag key/value size validation ─────────────────────────────────────

fn random_string(len: usize) -> String {
    (0..len).map(|i| (b'a' + (i % 26) as u8) as char).collect()
}

#[test]
fn test_put_max_kvsize_tags() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        put_object_retrying_operation_aborted(client, &bucket, "obj", b"hello".to_vec(), None)
            .await;

        // 10 tags with max-size keys (128 chars) and max-size values (256 chars)
        let tags: Vec<Tag> = (0..10)
            .map(|i| {
                let key = format!("{}{}", i, random_string(128 - i.to_string().len()));
                let val = format!("{}{}", i, random_string(256 - i.to_string().len()));
                tag(&key, &val)
            })
            .collect();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(tags.clone()))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert_eq!(result.tag_set().len(), 10);

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_put_excess_key_tags() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        put_object_retrying_operation_aborted(client, &bucket, "obj", b"hello".to_vec(), None)
            .await;

        // Tag key of 129 chars should be rejected
        let tags = vec![tag(&random_string(129), "val")];
        let result = client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(tags))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await;
        assert!(result.is_err());

        // Verify no tags were stored
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert!(result.tag_set().is_empty());

        cleanup(&bucket, &["obj"]).await;
    });
}

#[test]
fn test_put_excess_val_tags() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        put_object_retrying_operation_aborted(client, &bucket, "obj", b"hello".to_vec(), None)
            .await;

        // Tag value of 257 chars should be rejected
        let tags = vec![tag("key", &random_string(257))];
        let result = client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(tags))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await;
        assert!(result.is_err());

        // Verify no tags were stored
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert!(result.tag_set().is_empty());

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Atomicity / malformed XML / copy directive tests ──────────────────

/// PutObject with invalid inline tags (>10 URL-encoded) should fail and
/// the object should not exist.
#[test]
fn test_put_object_invalid_tagging_not_stored() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // 11 tags via URL-encoded header should be rejected
        let tag_str: String = (0..11)
            .map(|i| format!("k{}=v{}", i, i))
            .collect::<Vec<_>>()
            .join("&");
        let result = put_object_result_retrying_operation_aborted(
            client,
            &bucket,
            "obj",
            b"hello".to_vec(),
            Some(&tag_str),
        )
        .await;
        assert!(result.is_err());

        // Object should not exist
        let head = client
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await;
        assert!(head.is_err());

        cleanup(&bucket, &[]).await;
    });
}

/// CopyObject with default (COPY) directive should copy source tags.
#[test]
fn test_copy_object_default_copies_tags() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // Put source with tags
        put_object_retrying_operation_aborted(
            client,
            &bucket,
            "src",
            b"data".to_vec(),
            Some("color=blue&size=large"),
        )
        .await;

        // Copy without tagging-directive (default = COPY)
        client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        // Destination should have source's tags
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("dst")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();

        let tag_set = result.tag_set();
        assert_eq!(tag_set.len(), 2);
        assert!(tag_set
            .iter()
            .any(|t| t.key() == "color" && t.value() == "blue"));
        assert!(tag_set
            .iter()
            .any(|t| t.key() == "size" && t.value() == "large"));

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

/// CopyObject with REPLACE directive but no x-amz-tagging should have no tags.
#[test]
fn test_copy_object_replace_clears_tags() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // Put source with tags
        put_object_retrying_operation_aborted(
            client,
            &bucket,
            "src",
            b"data".to_vec(),
            Some("color=blue"),
        )
        .await;

        // Copy with REPLACE but no tagging header
        client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .tagging_directive(aws_sdk_s3::types::TaggingDirective::Replace)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        // Destination should have no tags
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("dst")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert!(result.tag_set().is_empty());

        cleanup(&bucket, &["src", "dst"]).await;
    });
}

// ── Delete marker tagging ─────────────────────────────────────────────

/// Helper: create a versioned bucket, put an object, delete it to create a
/// delete marker, and return (bucket, key, delete_marker_version_id).
async fn create_delete_marker() -> (String, String, String) {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();

    // Enable versioning
    client
        .put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send_retrying_operation_aborted("S3 mutation during tagging test")
        .await
        .unwrap();

    let key = "dm-test-obj";

    // Put an object
    put_object_retrying_operation_aborted(client, &bucket, key, b"hello".to_vec(), None).await;

    // Delete the object (creates a delete marker)
    let delete_resp = client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send_retrying_operation_aborted("S3 mutation during tagging test")
        .await
        .unwrap();

    assert!(delete_resp.delete_marker().unwrap_or(false));
    let dm_version_id = delete_resp.version_id().unwrap().to_string();

    (bucket, key.to_string(), dm_version_id)
}

/// PutObjectTagging on a delete marker (by versionId) should return 405.
#[test]
fn test_put_tagging_on_delete_marker() {
    s3_tests::run(async {
        let (bucket, key, dm_version_id) = create_delete_marker().await;
        let client = CTX.client();

        let result = client
            .put_object_tagging()
            .bucket(&bucket)
            .key(&key)
            .version_id(&dm_version_id)
            .tagging(tagging(vec![tag("foo", "bar")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await;

        assert!(result.is_err());
        assert_eq!(err_status(&result), 405);
        assert_s3_err_code(&result, "MethodNotAllowed");

        cleanup_versioned_bucket(CTX.client(), &bucket).await;
    });
}

/// GetObjectTagging on a delete marker (by versionId) should return 405.
#[test]
fn test_get_tagging_on_delete_marker() {
    s3_tests::run(async {
        let (bucket, key, dm_version_id) = create_delete_marker().await;
        let client = CTX.client();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(&key)
            .version_id(&dm_version_id)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await;

        assert!(result.is_err());
        assert_eq!(err_status(&result), 405);
        assert_s3_err_code(&result, "MethodNotAllowed");

        cleanup_versioned_bucket(CTX.client(), &bucket).await;
    });
}

/// DeleteObjectTagging on a delete marker (by versionId) should return 405.
#[test]
fn test_delete_tagging_on_delete_marker() {
    s3_tests::run(async {
        let (bucket, key, dm_version_id) = create_delete_marker().await;
        let client = CTX.client();

        let result = client
            .delete_object_tagging()
            .bucket(&bucket)
            .key(&key)
            .version_id(&dm_version_id)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await;

        assert!(result.is_err());
        assert_eq!(err_status(&result), 405);
        assert_s3_err_code(&result, "MethodNotAllowed");

        cleanup_versioned_bucket(CTX.client(), &bucket).await;
    });
}

/// Tagging on a deleted object (no versionId, current version is delete marker)
/// should return 405 MethodNotAllowed, not succeed silently.
/// AWS returns 405 even without an explicit versionId when the current version
/// is a delete marker.
#[test]
fn test_tagging_on_deleted_object_without_version_id() {
    s3_tests::run(async {
        let (bucket, key, _dm_version_id) = create_delete_marker().await;
        let client = CTX.client();

        // PutObjectTagging without versionId on deleted object → 405
        let result = client
            .put_object_tagging()
            .bucket(&bucket)
            .key(&key)
            .tagging(tagging(vec![tag("foo", "bar")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await;
        assert!(result.is_err());
        assert_eq!(err_status(&result), 405);
        assert_s3_err_code(&result, "MethodNotAllowed");

        // GetObjectTagging without versionId on deleted object → 405
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(&key)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await;
        assert!(result.is_err());
        assert_eq!(err_status(&result), 405);
        assert_s3_err_code(&result, "MethodNotAllowed");

        // DeleteObjectTagging without versionId on deleted object → 405
        let result = client
            .delete_object_tagging()
            .bucket(&bucket)
            .key(&key)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await;
        assert!(result.is_err());
        assert_eq!(err_status(&result), 405);
        assert_s3_err_code(&result, "MethodNotAllowed");

        cleanup_versioned_bucket(CTX.client(), &bucket).await;
    });
}

/// Deleting a tagged object should not leave tags on the delete marker.
/// The delete marker is a separate version with only a last-modified time — no data, metadata, or tags.
#[test]
fn test_delete_tagged_object_no_tags_on_delete_marker() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // Enable versioning
        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let key = "tagged-then-deleted";

        // Put an object with tags
        put_object_retrying_operation_aborted(
            client,
            &bucket,
            key,
            b"hello".to_vec(),
            Some("env=prod&team=platform"),
        )
        .await;

        // Verify tags are set
        wait_for_tag_count(&bucket, key, 2, "tag visibility after put").await;
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert_eq!(result.tag_set().len(), 2);

        // Delete the object (creates delete marker)
        let delete_resp = client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();
        assert!(delete_resp.delete_marker().unwrap_or(false));
        let dm_version_id = delete_resp.version_id().unwrap().to_string();

        // GetObjectTagging on the delete marker by versionId → 405
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&dm_version_id)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await;
        assert!(result.is_err());
        assert_eq!(err_status(&result), 405);

        // GetObjectTagging without versionId (current = delete marker) → 405
        wait_for_current_delete_marker_tagging_method_not_allowed(
            &bucket,
            key,
            "current delete-marker GetObjectTagging",
        )
        .await;

        cleanup_versioned_bucket(CTX.client(), &bucket).await;
    });
}

// ── Multipart upload with tagging ─────────────────────────────────────

#[test]
fn test_set_multipart_tagging() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let key = "multipart-tagged";
        let create = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .tagging("foo=bar&bar")
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let data = vec![b'a'; 5 * 1024 * 1024];
        let upload =
            upload_part_retrying_operation_aborted(client, &bucket, key, upload_id, 1, data).await;

        complete_multipart_upload_retrying_operation_aborted(
            client,
            &bucket,
            key,
            upload_id,
            upload.e_tag().unwrap(),
        )
        .await;

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        let tag_set = result.tag_set();
        assert_eq!(tag_set.len(), 2);
        assert!(tag_set
            .iter()
            .any(|t| t.key() == "foo" && t.value() == "bar"));
        assert!(tag_set.iter().any(|t| t.key() == "bar" && t.value() == ""));

        client
            .delete_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert!(result.tag_set().is_empty());

        cleanup(&bucket, &[key]).await;
    });
}

// ── Bucket policy tagging access control ──────────────────────────────

#[test]
fn test_bucket_policy_get_object_tagging_alt_account() {
    let _guard = BUCKET_POLICY_TEST_GUARD
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "testputtagsacl";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        put_object_retrying_operation_aborted(client, &bucket, key, b"data".to_vec(), None).await;

        let policy = bucket_policy_document(
            principal,
            "s3:GetObjectTagging",
            object_resource(&bucket, key),
            None,
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let input_tags = tagging(
            (0..10)
                .map(|i| tag(&format!("{i}"), &format!("{i}")))
                .collect(),
        );
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(input_tags.clone())
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let response = alt_client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert_tag_sets_match_unordered(response.tag_set(), input_tags.tag_set());

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_put_object_tagging_alt_account() {
    let _guard = BUCKET_POLICY_TEST_GUARD
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "testputtagsacl";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        put_object_retrying_operation_aborted(client, &bucket, key, b"data".to_vec(), None).await;

        let policy = bucket_policy_document(
            principal,
            "s3:PutObjectTagging",
            object_resource(&bucket, key),
            None,
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let input_tags = tagging(
            (0..10)
                .map(|i| tag(&format!("{i}"), &format!("{i}")))
                .collect(),
        );
        alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(input_tags.clone())
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let response = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert_tag_sets_match_unordered(response.tag_set(), input_tags.tag_set());

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_delete_object_tagging_alt_account() {
    let _guard = BUCKET_POLICY_TEST_GUARD
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "testputtagsacl";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        put_object_retrying_operation_aborted(client, &bucket, key, b"data".to_vec(), None).await;

        let policy = bucket_policy_document(
            principal,
            "s3:DeleteObjectTagging",
            object_resource(&bucket, key),
            None,
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(tagging(
                (0..10)
                    .map(|i| tag(&format!("{i}"), &format!("{i}")))
                    .collect(),
            ))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        alt_client
            .delete_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let response = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert!(response.tag_set().is_empty());

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_delete_obj_tagging_existing_tag() {
    let _guard = BUCKET_POLICY_TEST_GUARD
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let allow_key = "allowtag-delete";
        let deny_key = "denytag-delete";
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        for key in [allow_key, deny_key] {
            put_object_retrying_operation_aborted(client, &bucket, key, b"data".to_vec(), None)
                .await;
        }

        let policy = bucket_policy_document(
            principal,
            "s3:DeleteObjectTagging",
            bucket_wildcard_resource(&bucket),
            Some(json!({
                "StringEquals": {
                    "s3:ExistingObjectTag/security": "allow"
                }
            })),
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(allow_key)
            .tagging(tagging(vec![tag("security", "allow"), tag("foo", "bar")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(deny_key)
            .tagging(tagging(vec![tag("security", "deny")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        alt_client
            .delete_object_tagging()
            .bucket(&bucket)
            .key(allow_key)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let denied = alt_client
            .delete_object_tagging()
            .bucket(&bucket)
            .key(deny_key)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let allow_tags = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(allow_key)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert!(allow_tags.tag_set().is_empty());

        let deny_tags = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(deny_key)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert!(deny_tags
            .tag_set()
            .iter()
            .any(|tag| tag.key() == "security" && tag.value() == "deny"));

        cleanup(&bucket, &[allow_key, deny_key]).await;
    });
}

#[test]
fn test_bucket_policy_get_obj_existing_tag() {
    let _guard = BUCKET_POLICY_TEST_GUARD
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        for key in ["allowtag", "denytag", "invalidtag"] {
            put_object_retrying_operation_aborted(
                client,
                &bucket,
                key,
                key.as_bytes().to_vec(),
                None,
            )
            .await;
        }

        let policy = bucket_policy_document(
            principal,
            "s3:GetObject",
            bucket_wildcard_resource(&bucket),
            Some(json!({
                "StringEquals": {
                    "s3:ExistingObjectTag/security": "allow"
                }
            })),
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("allowtag")
            .tagging(tagging(vec![tag("security", "allow"), tag("foo", "bar")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("denytag")
            .tagging(tagging(vec![tag("security", "deny")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("invalidtag")
            .tagging(tagging(vec![tag("security1", "allow")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let response = alt_client
            .get_object()
            .bucket(&bucket)
            .key("allowtag")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        let body = response.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"allowtag");

        for key in ["denytag", "invalidtag"] {
            let result = alt_client
                .get_object()
                .bucket(&bucket)
                .key(key)
                .send_retrying_operation_aborted("S3 operation during tagging test")
                .await;
            assert_eq!(err_status(&result), 403);
            assert_s3_err_code(&result, "AccessDenied");
        }

        cleanup(&bucket, &["allowtag", "denytag", "invalidtag"]).await;
    });
}

#[test]
fn test_bucket_policy_get_obj_tagging_existing_tag() {
    let _guard = BUCKET_POLICY_TEST_GUARD
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        for key in ["allowtag", "denytag", "invalidtag"] {
            put_object_retrying_operation_aborted(
                client,
                &bucket,
                key,
                key.as_bytes().to_vec(),
                None,
            )
            .await;
        }

        let policy = bucket_policy_document(
            principal,
            "s3:GetObjectTagging",
            bucket_wildcard_resource(&bucket),
            Some(json!({
                "StringEquals": {
                    "s3:ExistingObjectTag/security": "allow"
                }
            })),
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("allowtag")
            .tagging(tagging(vec![tag("security", "allow"), tag("foo", "bar")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("denytag")
            .tagging(tagging(vec![tag("security", "deny")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("invalidtag")
            .tagging(tagging(vec![tag("security1", "allow")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let response = alt_client
            .get_object_tagging()
            .bucket(&bucket)
            .key("allowtag")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert!(response
            .tag_set()
            .iter()
            .any(|tag| tag.key() == "security" && tag.value() == "allow"));

        let get_object = alt_client
            .get_object()
            .bucket(&bucket)
            .key("allowtag")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await;
        assert_eq!(err_status(&get_object), 403);
        assert_s3_err_code(&get_object, "AccessDenied");

        for key in ["denytag", "invalidtag"] {
            let result = alt_client
                .get_object_tagging()
                .bucket(&bucket)
                .key(key)
                .send_retrying_operation_aborted("S3 operation during tagging test")
                .await;
            assert_eq!(err_status(&result), 403);
            assert_s3_err_code(&result, "AccessDenied");
        }

        cleanup(&bucket, &["allowtag", "denytag", "invalidtag"]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_tagging_existing_tag() {
    let _guard = BUCKET_POLICY_TEST_GUARD
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        for key in ["allowtag", "denytag"] {
            put_object_retrying_operation_aborted(
                client,
                &bucket,
                key,
                key.as_bytes().to_vec(),
                None,
            )
            .await;
        }

        let policy = bucket_policy_document(
            principal,
            "s3:PutObjectTagging",
            bucket_wildcard_resource(&bucket),
            Some(json!({
                "StringEquals": {
                    "s3:ExistingObjectTag/security": "allow"
                }
            })),
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("allowtag")
            .tagging(tagging(vec![tag("security", "allow"), tag("foo", "bar")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("denytag")
            .tagging(tagging(vec![tag("security", "deny")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let allow_tags = tagging(vec![tag("security", "allow"), tag("foo", "bar")]);
        alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key("allowtag")
            .tagging(allow_tags.clone())
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let deny_result = alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key("denytag")
            .tagging(allow_tags.clone())
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await;
        assert_eq!(err_status(&deny_result), 403);
        assert_s3_err_code(&deny_result, "AccessDenied");

        let deny_tags = tagging(vec![tag("security", "deny")]);
        alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key("allowtag")
            .tagging(deny_tags)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let second_result = alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key("allowtag")
            .tagging(allow_tags)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await;
        assert_eq!(err_status(&second_result), 403);
        assert_s3_err_code(&second_result, "AccessDenied");

        cleanup(&bucket, &["allowtag", "denytag"]).await;
    });
}

#[test]
fn test_bucket_policy_get_obj_version_tagging_existing_tag() {
    let _guard = BUCKET_POLICY_TEST_GUARD
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let allow_key = "allowtag-version-get";
        let deny_key = "denytag-version-get";
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let allow_version = put_object_retrying_operation_aborted(
            client,
            &bucket,
            allow_key,
            b"data".to_vec(),
            None,
        )
        .await
        .version_id()
        .expect("expected version id")
        .to_string();
        let deny_version = put_object_retrying_operation_aborted(
            client,
            &bucket,
            deny_key,
            b"data".to_vec(),
            None,
        )
        .await
        .version_id()
        .expect("expected version id")
        .to_string();

        let policy = bucket_policy_document(
            principal,
            "s3:GetObjectVersionTagging",
            bucket_wildcard_resource(&bucket),
            Some(json!({
                "StringEquals": {
                    "s3:ExistingObjectTag/security": "allow"
                }
            })),
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(allow_key)
            .version_id(&allow_version)
            .tagging(tagging(vec![tag("security", "allow"), tag("foo", "bar")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(deny_key)
            .version_id(&deny_version)
            .tagging(tagging(vec![tag("security", "deny")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let response = alt_client
            .get_object_tagging()
            .bucket(&bucket)
            .key(allow_key)
            .version_id(&allow_version)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert!(response
            .tag_set()
            .iter()
            .any(|tag| tag.key() == "security" && tag.value() == "allow"));

        let denied = alt_client
            .get_object_tagging()
            .bucket(&bucket)
            .key(deny_key)
            .version_id(&deny_version)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        cleanup_versioned_bucket(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_version_tagging_existing_tag() {
    let _guard = BUCKET_POLICY_TEST_GUARD
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let allow_key = "allowtag-version-put";
        let deny_key = "denytag-version-put";
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let allow_version = put_object_retrying_operation_aborted(
            client,
            &bucket,
            allow_key,
            b"data".to_vec(),
            None,
        )
        .await
        .version_id()
        .expect("expected version id")
        .to_string();
        let deny_version = put_object_retrying_operation_aborted(
            client,
            &bucket,
            deny_key,
            b"data".to_vec(),
            None,
        )
        .await
        .version_id()
        .expect("expected version id")
        .to_string();

        let policy = bucket_policy_document(
            principal,
            "s3:PutObjectVersionTagging",
            bucket_wildcard_resource(&bucket),
            Some(json!({
                "StringEquals": {
                    "s3:ExistingObjectTag/security": "allow"
                }
            })),
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(allow_key)
            .version_id(&allow_version)
            .tagging(tagging(vec![tag("security", "allow"), tag("foo", "bar")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(deny_key)
            .version_id(&deny_version)
            .tagging(tagging(vec![tag("security", "deny")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let allow_tags = tagging(vec![tag("security", "allow"), tag("foo", "bar")]);
        alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key(allow_key)
            .version_id(&allow_version)
            .tagging(allow_tags.clone())
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let denied = alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key(deny_key)
            .version_id(&deny_version)
            .tagging(allow_tags.clone())
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        cleanup_versioned_bucket(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_tagging_request_object_tag() {
    let _guard = BUCKET_POLICY_TEST_GUARD
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "requesttag";
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        put_object_retrying_operation_aborted(client, &bucket, key, b"data".to_vec(), None).await;

        let policy = bucket_policy_document(
            principal,
            "s3:PutObjectTagging",
            bucket_wildcard_resource(&bucket),
            Some(json!({
                "StringEquals": {
                    "s3:RequestObjectTag/security": "allow"
                }
            })),
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let allow_tags = tagging(vec![tag("security", "allow"), tag("foo", "bar")]);
        alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(allow_tags.clone())
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let denied = alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(tagging(vec![tag("security", "deny")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let stored = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert_tag_sets_match_unordered(stored.tag_set(), allow_tags.tag_set());

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_version_tagging_request_object_tag() {
    let _guard = BUCKET_POLICY_TEST_GUARD
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "requesttag-versioned";
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let version_id =
            put_object_retrying_operation_aborted(client, &bucket, key, b"data".to_vec(), None)
                .await
                .version_id()
                .expect("expected version id")
                .to_string();

        let policy = bucket_policy_document(
            principal,
            "s3:PutObjectVersionTagging",
            bucket_wildcard_resource(&bucket),
            Some(json!({
                "StringEquals": {
                    "s3:RequestObjectTag/security": "allow"
                }
            })),
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let allow_tags = tagging(vec![tag("security", "allow"), tag("foo", "bar")]);
        alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .tagging(allow_tags.clone())
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let denied = alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .tagging(tagging(vec![tag("security", "deny")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let stored = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert_tag_sets_match_unordered(stored.tag_set(), allow_tags.tag_set());

        cleanup_versioned_bucket(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_bucket_policy_delete_obj_version_tagging_existing_tag() {
    let _guard = BUCKET_POLICY_TEST_GUARD
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let allow_key = "allowtag-version-delete";
        let deny_key = "denytag-version-delete";
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        client
            .put_bucket_versioning()
            .bucket(&bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let allow_version = put_object_retrying_operation_aborted(
            client,
            &bucket,
            allow_key,
            b"data".to_vec(),
            None,
        )
        .await
        .version_id()
        .expect("expected version id")
        .to_string();
        let deny_version = put_object_retrying_operation_aborted(
            client,
            &bucket,
            deny_key,
            b"data".to_vec(),
            None,
        )
        .await
        .version_id()
        .expect("expected version id")
        .to_string();

        let policy = bucket_policy_document(
            principal,
            "s3:DeleteObjectVersionTagging",
            bucket_wildcard_resource(&bucket),
            Some(json!({
                "StringEquals": {
                    "s3:ExistingObjectTag/security": "allow"
                }
            })),
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(allow_key)
            .version_id(&allow_version)
            .tagging(tagging(vec![tag("security", "allow"), tag("foo", "bar")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(deny_key)
            .version_id(&deny_version)
            .tagging(tagging(vec![tag("security", "deny")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        alt_client
            .delete_object_tagging()
            .bucket(&bucket)
            .key(allow_key)
            .version_id(&allow_version)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let denied = alt_client
            .delete_object_tagging()
            .bucket(&bucket)
            .key(deny_key)
            .version_id(&deny_version)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let allow_tags = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(allow_key)
            .version_id(&allow_version)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert!(allow_tags.tag_set().is_empty());

        let deny_tags = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(deny_key)
            .version_id(&deny_version)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert!(deny_tags
            .tag_set()
            .iter()
            .any(|tag| tag.key() == "security" && tag.value() == "deny"));

        cleanup_versioned_bucket(CTX.client(), &bucket).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_tagging_request_object_tag_on_pretagged_object() {
    let _guard = BUCKET_POLICY_TEST_GUARD
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "requesttag-pretagged";
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        put_object_retrying_operation_aborted(client, &bucket, key, b"data".to_vec(), None).await;

        let bootstrap_tags = tagging(vec![tag("security", "bootstrap")]);
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(bootstrap_tags.clone())
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let policy = bucket_policy_document(
            principal,
            "s3:PutObjectTagging",
            bucket_wildcard_resource(&bucket),
            Some(json!({
                "StringEquals": {
                    "s3:RequestObjectTag/security": "allow"
                }
            })),
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(tagging(vec![tag("security", "allow"), tag("foo", "bar")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let stored = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert_tag_sets_match_unordered(
            stored.tag_set(),
            tagging(vec![tag("security", "allow"), tag("foo", "bar")]).tag_set(),
        );

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_tagging_request_object_tag_single_tag() {
    let _guard = BUCKET_POLICY_TEST_GUARD
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "requesttag-single";
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        put_object_retrying_operation_aborted(client, &bucket, key, b"data".to_vec(), None).await;

        let policy = bucket_policy_document(
            principal,
            "s3:PutObjectTagging",
            bucket_wildcard_resource(&bucket),
            Some(json!({
                "StringEquals": {
                    "s3:RequestObjectTag/security": "allow"
                }
            })),
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let allow_tags = tagging(vec![tag("security", "allow")]);
        alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(allow_tags.clone())
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let denied = alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(tagging(vec![tag("security", "deny")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let stored = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        assert_tag_sets_match_unordered(stored.tag_set(), allow_tags.tag_set());

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_copy_source() {
    let _guard = BUCKET_POLICY_TEST_GUARD
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let src_bucket = unique_bucket();
        let dst_bucket = unique_bucket();
        s3_tests::create_bucket(client, &src_bucket).await.unwrap();
        s3_tests::create_bucket(client, &dst_bucket).await.unwrap();

        for key in ["public/foo", "public/bar", "private/foo"] {
            put_object_retrying_operation_aborted(
                client,
                &src_bucket,
                key,
                key.as_bytes().to_vec(),
                None,
            )
            .await;
        }

        let src_policy = bucket_policy_document(
            principal.clone(),
            "s3:GetObject",
            bucket_wildcard_resource(&src_bucket),
            None,
        );
        client
            .put_bucket_policy()
            .bucket(&src_bucket)
            .policy(src_policy)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let dst_policy = bucket_policy_document(
            principal,
            "s3:PutObject",
            bucket_wildcard_resource(&dst_bucket),
            Some(json!({
                "StringLike": {
                    "s3:x-amz-copy-source": format!("{src_bucket}/public/*")
                }
            })),
        );
        client
            .put_bucket_policy()
            .bucket(&dst_bucket)
            .policy(dst_policy)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        alt_client
            .copy_object()
            .bucket(&dst_bucket)
            .key("new_foo")
            .copy_source(format!("{src_bucket}/public/foo"))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let response = client
            .get_object()
            .bucket(&dst_bucket)
            .key("new_foo")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        let body = response.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"public/foo");

        alt_client
            .copy_object()
            .bucket(&dst_bucket)
            .key("new_foo2")
            .copy_source(format!("{src_bucket}/public/bar"))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let response = client
            .get_object()
            .bucket(&dst_bucket)
            .key("new_foo2")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        let body = response.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"public/bar");

        let result = alt_client
            .copy_object()
            .bucket(&dst_bucket)
            .key("new_foo3")
            .copy_source(format!("{src_bucket}/private/foo"))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        cleanup(&dst_bucket, &["new_foo", "new_foo2", "new_foo3"]).await;
        cleanup(&src_bucket, &["public/foo", "public/bar", "private/foo"]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_copy_source_meta() {
    let _guard = BUCKET_POLICY_TEST_GUARD
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let src_bucket = unique_bucket();
        let dst_bucket = unique_bucket();
        s3_tests::create_bucket(client, &src_bucket).await.unwrap();
        s3_tests::create_bucket(client, &dst_bucket).await.unwrap();

        for key in ["public/foo", "public/bar"] {
            put_object_retrying_operation_aborted(
                client,
                &src_bucket,
                key,
                key.as_bytes().to_vec(),
                None,
            )
            .await;
        }

        let src_policy = bucket_policy_document(
            principal.clone(),
            "s3:GetObject",
            bucket_wildcard_resource(&src_bucket),
            None,
        );
        client
            .put_bucket_policy()
            .bucket(&src_bucket)
            .policy(src_policy)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let dst_policy = bucket_policy_document(
            principal,
            "s3:PutObject",
            bucket_wildcard_resource(&dst_bucket),
            Some(json!({
                "StringEquals": {
                    "s3:x-amz-metadata-directive": "COPY"
                }
            })),
        );
        client
            .put_bucket_policy()
            .bucket(&dst_bucket)
            .policy(dst_policy)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        alt_client
            .copy_object()
            .bucket(&dst_bucket)
            .key("new_foo")
            .copy_source(format!("{src_bucket}/public/foo"))
            .metadata_directive(CopyMetadataDirective::Copy)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        let response = client
            .get_object()
            .bucket(&dst_bucket)
            .key("new_foo")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();
        let body = response.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"public/foo");

        let result = copy_object_without_metadata_directive_retrying_operation_aborted(
            alt_client,
            &dst_bucket,
            "new_foo2",
            &format!("{src_bucket}/public/bar"),
        )
        .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        cleanup(&dst_bucket, &["new_foo", "new_foo2"]).await;
        cleanup(&src_bucket, &["public/foo", "public/bar"]).await;
    });
}

#[test]
fn test_bucket_policy_get_obj_acl_existing_tag() {
    let _guard = BUCKET_POLICY_TEST_GUARD
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        for key in ["allowtag", "denytag", "invalidtag"] {
            put_object_retrying_operation_aborted(
                client,
                &bucket,
                key,
                key.as_bytes().to_vec(),
                None,
            )
            .await;
        }

        let policy = bucket_policy_document(
            principal,
            "s3:GetObjectAcl",
            bucket_wildcard_resource(&bucket),
            Some(json!({
                "StringEquals": {
                    "s3:ExistingObjectTag/security": "allow"
                }
            })),
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("allowtag")
            .tagging(tagging(vec![tag("security", "allow"), tag("foo", "bar")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("denytag")
            .tagging(tagging(vec![tag("security", "deny")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("invalidtag")
            .tagging(tagging(vec![tag("security1", "allow")]))
            .send_retrying_operation_aborted("S3 mutation during tagging test")
            .await
            .unwrap();

        alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key("allowtag")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await
            .unwrap();

        let get_object = alt_client
            .get_object()
            .bucket(&bucket)
            .key("allowtag")
            .send_retrying_operation_aborted("S3 operation during tagging test")
            .await;
        assert_eq!(err_status(&get_object), 403);
        assert_s3_err_code(&get_object, "AccessDenied");

        for key in ["denytag", "invalidtag"] {
            let result = alt_client
                .get_object_acl()
                .bucket(&bucket)
                .key(key)
                .send_retrying_operation_aborted("S3 operation during tagging test")
                .await;
            assert_eq!(err_status(&result), 403);
            assert_s3_err_code(&result, "AccessDenied");
        }

        cleanup(&bucket, &["allowtag", "denytag", "invalidtag"]).await;
    });
}

// ── GetBucketTagging response shape ─────────────────────────────────

#[test]
fn test_get_bucket_tagging_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let tagging = aws_sdk_s3::types::Tagging::builder()
            .tag_set(
                aws_sdk_s3::types::Tag::builder()
                    .key("env")
                    .value("prod")
                    .build()
                    .unwrap(),
            )
            .tag_set(
                aws_sdk_s3::types::Tag::builder()
                    .key("team")
                    .value("storage")
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(tagging)
            .send()
            .await
            .expect("put bucket tagging");

        let response = raw_bucket("GET", &bucket, Some("tagging="));
        assert_shape(
            "GetBucketTagging",
            &response,
            &shape()
                .status(200)
                .headers(chunked_response_headers())
                .body(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Tagging \
                     xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><TagSet><Tag>\
                     <Key>env</Key><Value>prod</Value></Tag><Tag><Key>team</Key>\
                     <Value>storage</Value></Tag></TagSet></Tagging>",
                ),
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}
