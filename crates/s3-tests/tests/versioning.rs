use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, Delete, EncodingType,
    ObjectIdentifier, VersioningConfiguration,
};
use s3_tests::{
    assert_s3_err_code, cleanup_versioned_bucket, content_md5_header, copy_source_with_version,
    delete_bucket_retrying_operation_aborted, delete_objects_retrying_operation_aborted,
    err_status, get_object_body_retrying_operation_aborted, raw_bucket, raw_object,
    raw_object_query, send_signed_request,
    shape::{assert_shape, chunked_response_headers, id_headers, shape},
    unique_bucket, RawResponse, SendRetryingOperationAborted, CTX,
};
use tokio::time::{sleep, Duration};

// ── Helpers ─────────────────────────────────────────────────────────

const CONTROL_KEY_CASES: &[(&str, &str)] = &[
    ("bad\u{0001}key", "bad%01key"),
    ("bad\u{001F}key", "bad%1Fkey"),
    ("bad\u{007F}key", "bad%7Fkey"),
    ("bad\u{0080}key", "bad%C2%80key"),
];

const XML_SPECIAL_KEY: &str = "xml<>&\"key";
const XML_SPECIAL_KEY_ENCODED: &str = "xml%3C%3E%26%22key";
const CONCURRENT_VERSION_OPERATION_ATTEMPTS: usize = 20;

async fn put_object_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
) -> aws_sdk_s3::operation::put_object::PutObjectOutput {
    s3_tests::retrying_operation_aborted("put object during versioning setup", || {
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body.clone()))
            .send()
    })
    .await
}

async fn upload_part_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: Vec<u8>,
) -> aws_sdk_s3::operation::upload_part::UploadPartOutput {
    s3_tests::retrying_operation_aborted("upload part during versioning setup", || {
        client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(body.clone()))
            .send()
    })
    .await
}

async fn create_multipart_upload_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput {
    s3_tests::retrying_operation_aborted("create multipart upload during versioning setup", || {
        client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
    })
    .await
}

async fn delete_object_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> String {
    let output =
        s3_tests::retrying_operation_aborted("delete object during versioning setup", || {
            client.delete_object().bucket(bucket).key(key).send()
        })
        .await;
    assert!(output.delete_marker().unwrap_or(false));
    output
        .version_id()
        .expect("delete marker version id")
        .to_string()
}

async fn delete_object_version_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    version_id: &str,
) {
    s3_tests::retrying_operation_aborted(
        "delete object version during concurrent version race",
        || {
            client
                .delete_object()
                .bucket(bucket)
                .key(key)
                .version_id(version_id)
                .send()
        },
    )
    .await;
}

async fn complete_multipart_upload_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    etag: &str,
) -> aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput {
    s3_tests::retrying_operation_aborted(
        "complete multipart upload during versioning setup",
        || {
            client
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
        },
    )
    .await
}

async fn copy_object_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    copy_source: String,
) -> aws_sdk_s3::operation::copy_object::CopyObjectOutput {
    s3_tests::retrying_operation_aborted("copy object during versioning setup", || {
        client
            .copy_object()
            .bucket(bucket)
            .key(key)
            .copy_source(copy_source.clone())
            .send()
    })
    .await
}

async fn put_bucket_versioning_retrying_operation_aborted(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    status: BucketVersioningStatus,
) {
    s3_tests::retrying_operation_aborted("put bucket versioning during versioning setup", || {
        client
            .put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(status.clone())
                    .build(),
            )
            .send()
    })
    .await;
    if status == BucketVersioningStatus::Enabled {
        s3_tests::wait_for_versioned_writes_visible(client, bucket).await;
    }
}

fn expected_raw_list_key(decoded_key: &str) -> String {
    let mut escaped = String::with_capacity(decoded_key.len());
    for ch in decoded_key.chars() {
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
    format!("<Key>{escaped}</Key>")
}

fn raw_response_is_retryable_operation_contention(response: &RawResponse) -> bool {
    (response.status == 409 && response.body.contains("<Code>OperationAborted</Code>"))
        || (response.status == 503 && response.body.contains("<Code>SlowDown</Code>"))
}

fn send_raw_retrying_operation_aborted<F>(description: &str, mut send: F) -> RawResponse
where
    F: FnMut() -> RawResponse,
{
    for attempt in 0..CONCURRENT_VERSION_OPERATION_ATTEMPTS {
        let response = send();
        if !raw_response_is_retryable_operation_contention(&response) {
            return response;
        }
        if attempt + 1 < CONCURRENT_VERSION_OPERATION_ATTEMPTS {
            std::thread::sleep(Duration::from_millis(10 * (attempt as u64 + 1)));
        }
    }
    panic!("{description} did not complete without retryable operation contention");
}

fn assert_canonical_owner_id(id: &str) {
    assert_eq!(
        id.len(),
        64,
        "expected 64-char canonical owner ID, got {id}"
    );
    assert!(
        id.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
        "expected lowercase hex canonical owner ID, got {id}"
    );
}

async fn setup_versioned_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    put_bucket_versioning_retrying_operation_aborted(
        client,
        &bucket,
        BucketVersioningStatus::Enabled,
    )
    .await;
    bucket
}

#[test]
fn test_bucket_versioning_raw_get_returns_canonical_xml() {
    s3_tests::run(async {
        let bucket = unique_bucket();
        s3_tests::create_bucket(CTX.client(), &bucket)
            .await
            .unwrap();

        let body = br#"
            <VersioningConfiguration>
                <Status>Enabled</Status>
            </VersioningConfiguration>
        "#;

        let parsed = server_http::http::xml::parse_versioning_config_xml(body).unwrap();
        let expected = server_http::http::xml::get_bucket_versioning_xml(parsed);

        let url = format!("{}/{}?versioning", CTX.endpoint(), bucket);
        let put = send_raw_retrying_operation_aborted("put raw bucket versioning", || {
            send_signed_request("PUT", &url, body, [content_md5_header(body)])
        });
        assert_eq!(put.status, 200, "unexpected body: {}", put.body);

        let get = send_raw_retrying_operation_aborted("get raw bucket versioning", || {
            send_signed_request("GET", &url, b"", std::iter::empty::<(String, String)>())
        });
        delete_bucket_retrying_operation_aborted(CTX.client(), &bucket).await;

        assert_eq!(get.status, 200, "unexpected body: {}", get.body);
        assert_eq!(get.body, expected);
    });
}

fn put_raw_object(bucket: &str, encoded_key: &str) {
    let url = format!("{}/{bucket}/{encoded_key}", CTX.endpoint());
    let response = send_raw_retrying_operation_aborted("put raw object", || {
        send_signed_request("PUT", &url, b"v1", std::iter::empty::<(&str, &str)>())
    });
    assert_eq!(response.status, 200, "unexpected body: {}", response.body);
}

async fn cleanup_versioned_bucket_with_encoding(client: &aws_sdk_s3::Client, bucket: &str) {
    loop {
        let resp = client
            .list_object_versions()
            .bucket(bucket)
            .encoding_type(EncodingType::Url)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .expect("list object versions");

        let mut deleted_any = false;
        for v in resp.versions() {
            let encoded_key = v.key().unwrap_or_default();
            let version_id = v.version_id().unwrap_or_default();
            let encoded_version_id: String =
                url::form_urlencoded::byte_serialize(version_id.as_bytes()).collect();
            let url = format!(
                "{}/{bucket}/{encoded_key}?versionId={encoded_version_id}",
                CTX.endpoint()
            );
            let response = send_raw_retrying_operation_aborted("delete raw object version", || {
                send_signed_request("DELETE", &url, b"", std::iter::empty::<(&str, &str)>())
            });
            assert_eq!(response.status, 204, "unexpected body: {}", response.body);
            deleted_any = true;
        }
        for m in resp.delete_markers() {
            let encoded_key = m.key().unwrap_or_default();
            let version_id = m.version_id().unwrap_or_default();
            let encoded_version_id: String =
                url::form_urlencoded::byte_serialize(version_id.as_bytes()).collect();
            let url = format!(
                "{}/{bucket}/{encoded_key}?versionId={encoded_version_id}",
                CTX.endpoint()
            );
            let response =
                send_raw_retrying_operation_aborted("delete raw object delete marker", || {
                    send_signed_request("DELETE", &url, b"", std::iter::empty::<(&str, &str)>())
                });
            assert_eq!(response.status, 204, "unexpected body: {}", response.body);
            deleted_any = true;
        }

        if !deleted_any {
            break;
        }
    }

    delete_bucket_retrying_operation_aborted(client, bucket).await;
}

/// Put multiple versions of the same key, returning (version_ids, contents).
async fn create_multiple_versions(
    bucket: &str,
    key: &str,
    num: usize,
) -> (Vec<String>, Vec<String>) {
    let client = CTX.client();
    let mut version_ids = Vec::new();
    let mut contents = Vec::new();
    for i in 0..num {
        let body = format!("content-{}", i);
        let resp =
            put_object_retrying_operation_aborted(client, bucket, key, body.clone().into_bytes())
                .await;
        version_ids.push(resp.version_id().unwrap().to_string());
        contents.push(body);
    }
    (version_ids, contents)
}

/// Verify that GET with a specific versionId returns expected content.
async fn check_obj_content(bucket: &str, key: &str, version_id: &str, expected: &str) {
    let body = get_object_body_retrying_operation_aborted(
        CTX.client(),
        bucket,
        key,
        Some(version_id),
        "get object body during versioning test",
    )
    .await;
    assert_eq!(
        std::str::from_utf8(&body).unwrap(),
        expected,
        "version {} content mismatch",
        version_id
    );
}

/// Delete all version IDs then delete the bucket.
async fn cleanup_versioned(bucket: &str, key: &str, version_ids: &[String]) {
    let client = CTX.client();
    for vid in version_ids {
        delete_object_version_retrying_operation_aborted(client, bucket, key, vid).await;
    }
    delete_bucket_retrying_operation_aborted(client, bucket).await;
}

async fn create_versioned_object_concurrent(
    client: aws_sdk_s3::Client,
    bucket: String,
    key: String,
    num: usize,
) {
    let mut tasks = Vec::with_capacity(num);
    for i in 0..num {
        let client = client.clone();
        let bucket = bucket.clone();
        let key = key.clone();
        tasks.push(tokio::spawn(async move {
            let body = format!("data {i}");
            put_object_retrying_operation_aborted(&client, &bucket, &key, body.into_bytes()).await;
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }
}

async fn clear_versioned_bucket_concurrent(client: aws_sdk_s3::Client, bucket: String) {
    loop {
        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();

        if resp.versions().is_empty() && resp.delete_markers().is_empty() {
            sleep(Duration::from_millis(200)).await;
            let confirm = client
                .list_object_versions()
                .bucket(&bucket)
                .send_retrying_operation_aborted("S3 operation during versioning test")
                .await
                .unwrap();
            if confirm.versions().is_empty() && confirm.delete_markers().is_empty() {
                return;
            }
            continue;
        }

        let mut tasks = Vec::with_capacity(resp.versions().len() + resp.delete_markers().len());
        for version in resp.versions() {
            let client = client.clone();
            let bucket = bucket.clone();
            let key = version.key().unwrap().to_string();
            let version_id = version.version_id().unwrap().to_string();
            tasks.push(tokio::spawn(async move {
                delete_object_version_retrying_operation_aborted(
                    &client,
                    &bucket,
                    &key,
                    &version_id,
                )
                .await;
            }));
        }
        for marker in resp.delete_markers() {
            let client = client.clone();
            let bucket = bucket.clone();
            let key = marker.key().unwrap().to_string();
            let version_id = marker.version_id().unwrap().to_string();
            tasks.push(tokio::spawn(async move {
                delete_object_version_retrying_operation_aborted(
                    &client,
                    &bucket,
                    &key,
                    &version_id,
                )
                .await;
            }));
        }

        for task in tasks {
            task.await.unwrap();
        }
    }
}

async fn wait_for_minimum_version_listing_counts(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    minimum_versions: usize,
    expected_delete_markers: usize,
    description: &str,
) {
    let mut last_seen = None;

    for _ in 0..40 {
        let resp = client
            .list_object_versions()
            .bucket(bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        let counts = (resp.versions().len(), resp.delete_markers().len());

        if counts.0 >= minimum_versions && counts.1 == expected_delete_markers {
            sleep(Duration::from_millis(200)).await;

            let confirm = client
                .list_object_versions()
                .bucket(bucket)
                .send_retrying_operation_aborted("S3 operation during versioning test")
                .await
                .unwrap();
            let confirmed = (confirm.versions().len(), confirm.delete_markers().len());
            if confirmed.0 >= minimum_versions && confirmed.1 == expected_delete_markers {
                return;
            }
            last_seen = Some(confirmed);
        } else {
            last_seen = Some(counts);
        }

        sleep(Duration::from_millis(250)).await;
    }

    let (versions, delete_markers) = last_seen.unwrap_or_default();
    panic!(
        "{description} did not converge for {bucket}: expected at least {minimum_versions} versions and {expected_delete_markers} delete markers, last saw {versions} versions and {delete_markers} delete markers"
    );
}

async fn wait_for_version_listing_counts(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    expected_versions: usize,
    expected_delete_markers: usize,
    description: &str,
) {
    let mut last_seen = None;

    for _ in 0..40 {
        let resp = client
            .list_object_versions()
            .bucket(bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        let counts = (resp.versions().len(), resp.delete_markers().len());

        if counts == (expected_versions, expected_delete_markers) {
            sleep(Duration::from_millis(200)).await;

            let confirm = client
                .list_object_versions()
                .bucket(bucket)
                .send_retrying_operation_aborted("S3 operation during versioning test")
                .await
                .unwrap();
            let confirmed = (confirm.versions().len(), confirm.delete_markers().len());
            if confirmed == counts {
                return;
            }
            last_seen = Some(confirmed);
        } else {
            last_seen = Some(counts);
        }

        sleep(Duration::from_millis(250)).await;
    }

    let (versions, delete_markers) = last_seen.unwrap_or_default();
    panic!(
        "{description} did not converge for {bucket}: expected {expected_versions} versions and {expected_delete_markers} delete markers, last saw {versions} versions and {delete_markers} delete markers"
    );
}

// ── Basic versioning CRUD ───────────────────────────────────────────

#[test]
fn test_versioning_obj_create_read_remove() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "testobj";
        let num = 5;

        // Create 5 versions, verify each is readable, remove all
        let (version_ids, contents) = create_multiple_versions(&bucket, key, num).await;

        // Verify each version is independently readable
        for (vid, content) in version_ids.iter().zip(contents.iter()) {
            check_obj_content(&bucket, key, vid, content).await;
        }

        // Remove each version by versionId
        for vid in &version_ids {
            delete_object_version_retrying_operation_aborted(client, &bucket, key, vid).await;
        }

        // Bucket should have no versions left
        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        assert!(
            resp.versions().is_empty(),
            "expected no versions after removal"
        );

        delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_versioning_obj_create_read_remove_head() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "testobj";
        let num = 5;

        let (mut version_ids, mut contents) = create_multiple_versions(&bucket, key, num).await;

        // Remove the latest (head) version
        let removed_vid = version_ids.pop().unwrap();
        contents.pop();
        delete_object_version_retrying_operation_aborted(client, &bucket, key, &removed_vid).await;

        // GET should now return the previous version
        let body = get_object_body_retrying_operation_aborted(
            client,
            &bucket,
            key,
            None,
            "get object body during versioning test",
        )
        .await;
        assert_eq!(
            std::str::from_utf8(&body).unwrap(),
            contents.last().unwrap()
        );

        // Add a delete marker
        let dm_vid = delete_object_retrying_operation_aborted(client, &bucket, key).await;
        version_ids.push(dm_vid.clone());

        // list_object_versions should show versions + 1 delete marker
        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        assert_eq!(resp.versions().len(), num - 1);
        assert_eq!(resp.delete_markers().len(), 1);
        assert_eq!(
            resp.delete_markers()[0].version_id().unwrap(),
            dm_vid.as_str()
        );

        cleanup_versioned(&bucket, key, &version_ids).await;
    });
}

#[test]
fn test_versioning_obj_create_versions_remove_all() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "testobj";
        let num = 10;

        let (version_ids, contents) = create_multiple_versions(&bucket, key, num).await;

        // Remove each version, verifying content before removal
        for i in 0..num {
            check_obj_content(&bucket, key, &version_ids[i], &contents[i]).await;
            delete_object_version_retrying_operation_aborted(client, &bucket, key, &version_ids[i])
                .await;
        }

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        assert!(resp.versions().is_empty());

        delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_versioning_obj_create_versions_remove_special_names() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let num = 10;
        let mut deleted_versions = Vec::new();

        for key in &["_testobj", "_", ":", "foo bar"] {
            let (version_ids, contents) = create_multiple_versions(&bucket, key, num).await;

            for i in 0..num {
                check_obj_content(&bucket, key, &version_ids[i], &contents[i]).await;
                delete_object_version_retrying_operation_aborted(
                    client,
                    &bucket,
                    key,
                    &version_ids[i],
                )
                .await;
                deleted_versions.push(((*key).to_string(), version_ids[i].clone()));
            }
        }

        let mut key_marker: Option<String> = None;
        let mut version_id_marker: Option<String> = None;
        loop {
            let mut req = client.list_object_versions().bucket(&bucket);
            if let Some(ref key_marker) = key_marker {
                req = req.key_marker(key_marker);
            }
            if let Some(ref version_id_marker) = version_id_marker {
                req = req.version_id_marker(version_id_marker);
            }

            let resp = req
                .send_retrying_operation_aborted("list object versions during versioning test")
                .await
                .unwrap();
            assert!(
                resp.delete_markers().is_empty(),
                "expected no delete markers after deleting explicit special-name versions"
            );
            for version in resp.versions() {
                let listed_key = version.key().unwrap_or_default();
                let listed_version_id = version.version_id().unwrap_or_default();
                assert!(
                    !deleted_versions
                        .iter()
                        .any(|(key, version_id)| key == listed_key && version_id == listed_version_id),
                    "deleted version still listed: key={listed_key:?} version_id={listed_version_id:?}"
                );
            }

            if resp.is_truncated() != Some(true) {
                break;
            }
            key_marker = resp.next_key_marker().map(str::to_string);
            version_id_marker = resp.next_version_id_marker().map(str::to_string);
            assert!(key_marker.is_some());
            assert!(version_id_marker.is_some());
        }

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

// ── Delete markers ──────────────────────────────────────────────────

#[test]
fn test_versioning_stack_delete_merkers() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "test1/a";

        let (mut all_vids, _) = create_multiple_versions(&bucket, key, 1).await;

        // Create 3 delete markers by deleting without versionId
        for _ in 0..3 {
            all_vids.push(delete_object_retrying_operation_aborted(client, &bucket, key).await);
        }

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        assert_eq!(resp.versions().len(), 1);
        assert_eq!(resp.delete_markers().len(), 3);

        cleanup_versioned(&bucket, key, &all_vids).await;
    });
}

// ── Null version handling ───────────────────────────────────────────

#[test]
fn test_versioning_obj_plain_null_version_removal() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // Put object before versioning is enabled (null version)
        let key = "testobjfoo";
        put_object_retrying_operation_aborted(client, &bucket, key, b"fooz".to_vec()).await;

        // Enable versioning
        put_bucket_versioning_retrying_operation_aborted(
            client,
            &bucket,
            BucketVersioningStatus::Enabled,
        )
        .await;

        // Delete the null version
        delete_object_version_retrying_operation_aborted(client, &bucket, key, "null").await;

        // GET should now 404
        let result = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("get object during versioning test")
            .await;
        assert_eq!(err_status(&result), 404);

        // No versions should remain
        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        assert!(resp.versions().is_empty());

        delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_versioning_obj_plain_null_version_overwrite() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let key = "testobjfoo";
        // Put before versioning
        put_object_retrying_operation_aborted(client, &bucket, key, b"fooz".to_vec()).await;

        // Enable versioning
        put_bucket_versioning_retrying_operation_aborted(
            client,
            &bucket,
            BucketVersioningStatus::Enabled,
        )
        .await;

        // Put new version (gets a real version ID)
        let resp =
            put_object_retrying_operation_aborted(client, &bucket, key, b"zzz".to_vec()).await;
        let version_id = resp.version_id().unwrap().to_string();

        // GET returns new version
        let body = get_object_body_retrying_operation_aborted(
            client,
            &bucket,
            key,
            None,
            "get object body during versioning test",
        )
        .await;
        assert_eq!(&body[..], b"zzz");

        // Delete the new version → old null version becomes current
        delete_object_version_retrying_operation_aborted(client, &bucket, key, &version_id).await;

        let body = get_object_body_retrying_operation_aborted(
            client,
            &bucket,
            key,
            None,
            "get object body during versioning test",
        )
        .await;
        assert_eq!(&body[..], b"fooz");

        // Delete the null version
        delete_object_version_retrying_operation_aborted(client, &bucket, key, "null").await;

        let result = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("get object during versioning test")
            .await;
        assert_eq!(err_status(&result), 404);

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        assert!(resp.versions().is_empty());

        delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Suspend / resume ────────────────────────────────────────────────

#[test]
fn test_versioning_obj_suspend_versions() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "testobj";
        let num = 5;

        let (version_ids, _) = create_multiple_versions(&bucket, key, num).await;

        // Suspend versioning
        put_bucket_versioning_retrying_operation_aborted(
            client,
            &bucket,
            BucketVersioningStatus::Suspended,
        )
        .await;

        // Puts while suspended overwrite the null version
        put_object_retrying_operation_aborted(client, &bucket, key, b"suspended content".to_vec())
            .await;

        // Re-enable versioning
        put_bucket_versioning_retrying_operation_aborted(
            client,
            &bucket,
            BucketVersioningStatus::Enabled,
        )
        .await;

        let (extra_vids, _) = create_multiple_versions(&bucket, key, 3).await;

        // Clean up: delete all versioned + null
        for vid in version_ids.iter().chain(extra_vids.iter()) {
            delete_object_version_retrying_operation_aborted(client, &bucket, key, vid).await;
        }
        // Delete null version from suspended period
        delete_object_version_retrying_operation_aborted(client, &bucket, key, "null").await;

        delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_versioning_list_object_versions_suspended_null_is_latest() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "testobj";

        let older =
            put_object_retrying_operation_aborted(client, &bucket, key, b"older".to_vec()).await;
        let older_version_id = older.version_id().unwrap().to_string();

        put_bucket_versioning_retrying_operation_aborted(
            client,
            &bucket,
            BucketVersioningStatus::Suspended,
        )
        .await;

        put_object_retrying_operation_aborted(client, &bucket, key, b"current".to_vec()).await;

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .prefix(key)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();

        let versions = resp.versions();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].key().unwrap(), key);
        assert_eq!(versions[0].version_id(), Some("null"));
        assert_eq!(versions[0].is_latest(), Some(true));
        assert_eq!(versions[1].key().unwrap(), key);
        assert_eq!(versions[1].version_id(), Some(older_version_id.as_str()));
        assert_eq!(versions[1].is_latest(), Some(false));
        assert!(resp.delete_markers().is_empty());

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_versioning_obj_plain_null_version_overwrite_suspended() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let key = "testobjbar";
        // Put before versioning
        put_object_retrying_operation_aborted(client, &bucket, key, b"foooz".to_vec()).await;

        // Enable then suspend
        put_bucket_versioning_retrying_operation_aborted(
            client,
            &bucket,
            BucketVersioningStatus::Enabled,
        )
        .await;
        put_bucket_versioning_retrying_operation_aborted(
            client,
            &bucket,
            BucketVersioningStatus::Suspended,
        )
        .await;

        // Put while suspended overwrites null
        put_object_retrying_operation_aborted(client, &bucket, key, b"zzz".to_vec()).await;

        let body = get_object_body_retrying_operation_aborted(
            client,
            &bucket,
            key,
            None,
            "get object body during versioning test",
        )
        .await;
        assert_eq!(&body[..], b"zzz");

        // Should only have 1 version (the null)
        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        assert_eq!(resp.versions().len(), 1);

        // Delete null version
        delete_object_version_retrying_operation_aborted(client, &bucket, key, "null").await;

        let result = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("get object during versioning test")
            .await;
        assert_eq!(err_status(&result), 404);

        delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Suspended copy ──────────────────────────────────────────────────

#[test]
fn test_versioning_obj_suspended_copy() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key1 = "testobj1";

        let (_version_ids, _) = create_multiple_versions(&bucket, key1, 1).await;

        // Suspend versioning
        put_bucket_versioning_retrying_operation_aborted(
            client,
            &bucket,
            BucketVersioningStatus::Suspended,
        )
        .await;

        // Overwrite with null version
        put_object_retrying_operation_aborted(client, &bucket, key1, b"null content".to_vec())
            .await;

        // Copy to another key in same bucket
        let key2 = "testobj2";
        copy_object_retrying_operation_aborted(
            client,
            &bucket,
            key2,
            format!("{}/{}", bucket, key1),
        )
        .await;

        // Copy to another non-versioned bucket
        let bucket2 = unique_bucket();
        s3_tests::create_bucket(client, &bucket2).await.unwrap();
        copy_object_retrying_operation_aborted(
            client,
            &bucket2,
            key1,
            format!("{}/{}", bucket, key1),
        )
        .await;

        // Delete source (creates delete marker or overwrites null)
        client
            .delete_object()
            .bucket(&bucket)
            .key(key1)
            .send_retrying_operation_aborted("delete object during versioning suspended copy")
            .await
            .unwrap();

        // Verify copies
        let body = get_object_body_retrying_operation_aborted(
            client,
            &bucket,
            key2,
            None,
            "get object body during versioning test",
        )
        .await;
        assert_eq!(&body[..], b"null content");

        let body = get_object_body_retrying_operation_aborted(
            client,
            &bucket2,
            key1,
            None,
            "get object body during versioning test",
        )
        .await;
        assert_eq!(&body[..], b"null content");

        // Cleanup
        cleanup_versioned_bucket(client, &bucket2).await;
        cleanup_versioned_bucket(client, &bucket).await;
    });
}

// ── Version list ordering ───────────────────────────────────────────

#[test]
fn test_versioning_obj_list_marker() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "testobj";
        let key2 = "testobj-1";
        let num = 5;

        let (version_ids, contents) = create_multiple_versions(&bucket, key, num).await;
        let (version_ids2, contents2) = create_multiple_versions(&bucket, key2, num).await;

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        let versions = resp.versions();

        // Versions come out sorted by key, then newest-first within each key
        // key < key2 lexicographically ("testobj" < "testobj-1")
        assert_eq!(versions.len(), num * 2);

        // First `num` entries should be for `key`, newest first
        for i in 0..num {
            let v = &versions[i];
            assert_eq!(v.key().unwrap(), key);
            assert_eq!(v.version_id().unwrap(), version_ids[num - 1 - i]);
            check_obj_content(
                &bucket,
                key,
                v.version_id().unwrap(),
                &contents[num - 1 - i],
            )
            .await;
        }

        // Next `num` entries for `key2`, newest first
        for i in 0..num {
            let v = &versions[num + i];
            assert_eq!(v.key().unwrap(), key2);
            assert_eq!(v.version_id().unwrap(), version_ids2[num - 1 - i]);
            check_obj_content(
                &bucket,
                key2,
                v.version_id().unwrap(),
                &contents2[num - 1 - i],
            )
            .await;
        }

        // Clean up both keys' versions, then delete bucket
        let client = CTX.client();
        for vid in &version_ids {
            delete_object_version_retrying_operation_aborted(client, &bucket, key, vid).await;
        }
        for vid in &version_ids2 {
            delete_object_version_retrying_operation_aborted(client, &bucket, key2, vid).await;
        }
        delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_versioning_list_object_versions_pagination_and_markers() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "alpha&versions";
        let other_key = "beta<versions";

        let (mut key_versions, _) = create_multiple_versions(&bucket, key, 2).await;
        let other_resp =
            put_object_retrying_operation_aborted(client, &bucket, other_key, b"other".to_vec())
                .await;
        let other_vid = other_resp.version_id().unwrap().to_string();

        let delete_marker_vid =
            delete_object_retrying_operation_aborted(client, &bucket, key).await;
        key_versions.push(delete_marker_vid.clone());

        let mut key_marker: Option<String> = None;
        let mut version_id_marker: Option<String> = None;
        let mut pages = 0;
        let mut seen_delete_marker = false;
        let mut seen_version_ids = Vec::new();

        loop {
            let mut req = client
                .list_object_versions()
                .bucket(&bucket)
                .prefix("alpha")
                .max_keys(1);
            if let Some(ref km) = key_marker {
                req = req.key_marker(km);
            }
            if let Some(ref vm) = version_id_marker {
                req = req.version_id_marker(vm);
            }

            let resp = req
                .send_retrying_operation_aborted("list object versions during versioning test")
                .await
                .unwrap();
            pages += 1;

            if !resp.delete_markers().is_empty() {
                seen_delete_marker = true;
            }
            for version in resp.versions() {
                seen_version_ids.push(version.version_id().unwrap().to_string());
            }

            if resp.is_truncated() != Some(true) {
                break;
            }

            key_marker = resp.next_key_marker().map(str::to_string);
            version_id_marker = resp.next_version_id_marker().map(str::to_string);
            assert!(key_marker.is_some());
            assert!(version_id_marker.is_some());
        }

        assert!(pages >= 3, "expected paginated result, got {pages} page(s)");
        assert!(seen_delete_marker);
        assert_eq!(seen_version_ids.len(), 2);
        assert!(seen_version_ids
            .iter()
            .all(|vid| key_versions.contains(vid)));

        delete_object_version_retrying_operation_aborted(client, &bucket, other_key, &other_vid)
            .await;
        cleanup_versioned(&bucket, key, &key_versions).await;
    });
}

#[test]
fn test_versioning_list_object_versions_rejects_version_id_marker_without_key_marker() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;

        let alpha_v1 =
            put_object_retrying_operation_aborted(client, &bucket, "alpha", b"v1".to_vec())
                .await
                .version_id()
                .expect("alpha v1 version id")
                .to_string();
        put_object_retrying_operation_aborted(client, &bucket, "alpha", b"v2".to_vec()).await;
        put_object_retrying_operation_aborted(client, &bucket, "beta", b"v1".to_vec()).await;

        let result = client
            .list_object_versions()
            .bucket(&bucket)
            .max_keys(10)
            .version_id_marker(alpha_v1)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await;

        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidArgument");

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_versioning_list_object_versions_oversized_max_keys_echoed_by_aws() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;

        put_object_retrying_operation_aborted(
            client,
            &bucket,
            "oversized-max-keys.txt",
            b"v1".to_vec(),
        )
        .await;

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .max_keys(5000)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();

        assert_eq!(resp.max_keys(), Some(5000));
        assert_eq!(resp.is_truncated(), Some(false));
        assert_eq!(resp.versions().len(), 1);
        assert!(resp.delete_markers().is_empty());
        assert_eq!(resp.next_key_marker(), None);
        assert_eq!(resp.next_version_id_marker(), None);

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_versioning_list_object_versions_raw_xml_echoes_oversized_max_keys_on_aws() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;

        put_object_retrying_operation_aborted(
            client,
            &bucket,
            "oversized-max-keys-raw.txt",
            b"v1".to_vec(),
        )
        .await;

        let url = format!("{}/{bucket}?versions=&max-keys=5000", CTX.endpoint());
        let response = send_raw_retrying_operation_aborted("get raw list object versions", || {
            send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>())
        });

        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<MaxKeys>5000</MaxKeys>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response.body.contains("<IsTruncated>false</IsTruncated>"),
            "unexpected body: {}",
            response.body
        );

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_versioning_list_object_versions_oversized_max_keys_returns_at_most_1000_entries() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "oversized-max-keys-many-versions.txt";

        for _ in 0..1001 {
            put_object_retrying_operation_aborted(client, &bucket, key, Vec::new()).await;
        }

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .max_keys(5000)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();

        let entry_count = resp.versions().len() + resp.delete_markers().len();
        assert_eq!(entry_count, 1000);
        assert_eq!(resp.is_truncated(), Some(true));
        assert!(resp.next_key_marker().is_some());
        assert!(resp.next_version_id_marker().is_some());

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_versioning_head_delete_marker_version_returns_method_not_allowed() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "delete-marker-head";

        put_object_retrying_operation_aborted(client, &bucket, key, b"payload".to_vec()).await;

        let delete_marker_version =
            delete_object_retrying_operation_aborted(client, &bucket, key).await;

        let url = format!(
            "{}/{bucket}/{key}?versionId={delete_marker_version}",
            CTX.endpoint()
        );
        let response = send_signed_request("HEAD", &url, b"", std::iter::empty::<(&str, &str)>());

        assert_eq!(response.status, 405, "unexpected body: {}", response.body);
        assert_eq!(response.body, "");
        assert!(
            response.headers.iter().any(|(name, value)| {
                name.eq_ignore_ascii_case("x-amz-delete-marker") && value == "true"
            }),
            "missing x-amz-delete-marker header: {:?}",
            response.headers
        );
        assert!(
            response
                .headers
                .iter()
                .any(|(name, value)| name.eq_ignore_ascii_case("allow") && value == "DELETE"),
            "missing Allow: DELETE header: {:?}",
            response.headers
        );

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_versioning_list_object_versions_encoding_type_url() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "dir/hello world&plus+";

        put_object_retrying_operation_aborted(client, &bucket, key, b"v1".to_vec()).await;

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .encoding_type(EncodingType::Url)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        assert_eq!(resp.encoding_type(), Some(&EncodingType::Url));

        let url = format!("{}/{bucket}?versions=&encoding-type=url", CTX.endpoint());
        let response = send_raw_retrying_operation_aborted("get raw list object versions", || {
            send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>())
        });
        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<EncodingType>url</EncodingType>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response
                .body
                .contains("<Key>dir/hello+world%26plus%2B</Key>"),
            "unexpected body: {}",
            response.body
        );

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_versioning_list_object_versions_without_encoding_type_keeps_control_characters_literal() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;

        for &(_, encoded_key) in CONTROL_KEY_CASES {
            put_raw_object(&bucket, encoded_key);
        }

        let url = format!("{}/{bucket}?versions=", CTX.endpoint());
        let response = send_raw_retrying_operation_aborted("get raw list object versions", || {
            send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>())
        });
        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            !response.body.contains("<EncodingType>url</EncodingType>"),
            "unexpected body: {}",
            response.body
        );
        for &(decoded_key, _) in CONTROL_KEY_CASES {
            let needle = expected_raw_list_key(decoded_key);
            assert!(
                response.body.contains(&needle),
                "unexpected body: {}",
                response.body
            );
        }

        cleanup_versioned_bucket_with_encoding(client, &bucket).await;
    });
}

#[test]
fn test_versioning_list_object_versions_encoding_type_url_encodes_control_characters() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;

        for &(_, encoded_key) in CONTROL_KEY_CASES {
            put_raw_object(&bucket, encoded_key);
        }

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .encoding_type(EncodingType::Url)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        assert_eq!(resp.encoding_type(), Some(&EncodingType::Url));
        let mut keys: Vec<String> = resp
            .versions()
            .iter()
            .filter_map(|version| version.key().map(str::to_string))
            .collect();
        let mut expected: Vec<String> = CONTROL_KEY_CASES
            .iter()
            .map(|(_, encoded_key)| (*encoded_key).to_string())
            .collect();
        keys.sort();
        expected.sort();
        assert_eq!(keys, expected);

        let url = format!("{}/{bucket}?versions=&encoding-type=url", CTX.endpoint());
        let response = send_raw_retrying_operation_aborted("get raw list object versions", || {
            send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>())
        });
        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<EncodingType>url</EncodingType>"),
            "unexpected body: {}",
            response.body
        );
        for &(_, encoded_key) in CONTROL_KEY_CASES {
            assert!(
                response.body.contains(&format!("<Key>{encoded_key}</Key>")),
                "unexpected body: {}",
                response.body
            );
        }

        cleanup_versioned_bucket_with_encoding(client, &bucket).await;
    });
}

#[test]
fn test_versioning_list_object_versions_without_encoding_type_escapes_xml_special_characters() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;

        put_raw_object(&bucket, XML_SPECIAL_KEY_ENCODED);

        let url = format!("{}/{bucket}?versions=", CTX.endpoint());
        let response = send_raw_retrying_operation_aborted("get raw list object versions", || {
            send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>())
        });
        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response
                .body
                .contains(&expected_raw_list_key(XML_SPECIAL_KEY)),
            "unexpected body: {}",
            response.body
        );

        cleanup_versioned_bucket_with_encoding(client, &bucket).await;
    });
}

#[test]
fn test_versioning_list_object_versions_encoding_type_url_encodes_xml_special_characters() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;

        put_raw_object(&bucket, XML_SPECIAL_KEY_ENCODED);

        let url = format!("{}/{bucket}?versions=&encoding-type=url", CTX.endpoint());
        let response = send_raw_retrying_operation_aborted("get raw list object versions", || {
            send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>())
        });
        assert_eq!(response.status, 200, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<EncodingType>url</EncodingType>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response
                .body
                .contains(&format!("<Key>{XML_SPECIAL_KEY_ENCODED}</Key>")),
            "unexpected body: {}",
            response.body
        );

        cleanup_versioned_bucket_with_encoding(client, &bucket).await;
    });
}

// ── Copy specific versions ──────────────────────────────────────────

#[test]
fn test_versioning_copy_obj_version() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "testobj";
        let num = 3;

        let (version_ids, contents) = create_multiple_versions(&bucket, key, num).await;

        // Copy each version to a new key in same bucket
        let mut copy_keys = Vec::new();
        for i in 0..num {
            let new_key = format!("key_{}", i);
            copy_object_retrying_operation_aborted(
                client,
                &bucket,
                &new_key,
                copy_source_with_version(&bucket, key, &version_ids[i]),
            )
            .await;

            let body = get_object_body_retrying_operation_aborted(
                client,
                &bucket,
                &new_key,
                None,
                "get object body during versioning test",
            )
            .await;
            assert_eq!(std::str::from_utf8(&body).unwrap(), contents[i]);
            copy_keys.push(new_key);
        }

        // Copy each version to another bucket
        let bucket2 = unique_bucket();
        s3_tests::create_bucket(client, &bucket2).await.unwrap();

        for i in 0..num {
            let new_key = format!("key_{}", i);
            copy_object_retrying_operation_aborted(
                client,
                &bucket2,
                &new_key,
                copy_source_with_version(&bucket, key, &version_ids[i]),
            )
            .await;

            let body = get_object_body_retrying_operation_aborted(
                client,
                &bucket2,
                &new_key,
                None,
                "get object body during versioning test",
            )
            .await;
            assert_eq!(std::str::from_utf8(&body).unwrap(), contents[i]);
        }

        // Copy latest (no versionId) to another bucket
        copy_object_retrying_operation_aborted(
            client,
            &bucket2,
            "new_key",
            format!("{}/{}", bucket, key),
        )
        .await;

        let body = get_object_body_retrying_operation_aborted(
            client,
            &bucket2,
            "new_key",
            None,
            "get object body during versioning test",
        )
        .await;
        assert_eq!(std::str::from_utf8(&body).unwrap(), contents[num - 1]);

        // Cleanup
        cleanup_versioned_bucket(client, &bucket2).await;
        cleanup_versioned_bucket(client, &bucket).await;
    });
}

// ── Multi-object delete with versions ───────────────────────────────

#[test]
fn test_versioning_multi_object_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "key";

        let (version_ids, _) = create_multiple_versions(&bucket, key, 2).await;
        assert_eq!(version_ids.len(), 2);

        // Delete both versions via DeleteObjects
        let objects: Vec<ObjectIdentifier> = version_ids
            .iter()
            .map(|v| {
                ObjectIdentifier::builder()
                    .key(key)
                    .version_id(v)
                    .build()
                    .unwrap()
            })
            .collect();
        let resp = delete_objects_retrying_operation_aborted(
            client,
            &bucket,
            Delete::builder()
                .set_objects(Some(objects.clone()))
                .build()
                .unwrap(),
        )
        .await;
        assert!(
            resp.errors().is_empty(),
            "unexpected errors: {:?}",
            resp.errors()
        );

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        assert!(resp.versions().is_empty());

        // Deleting again should succeed (idempotent)
        let resp = delete_objects_retrying_operation_aborted(
            client,
            &bucket,
            Delete::builder()
                .set_objects(Some(objects))
                .build()
                .unwrap(),
        )
        .await;
        assert!(
            resp.errors().is_empty(),
            "unexpected errors: {:?}",
            resp.errors()
        );

        delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_versioning_multi_object_delete_with_marker() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "key";

        let (version_ids, _) = create_multiple_versions(&bucket, key, 2).await;

        // Create a delete marker
        let dm_vid = delete_object_retrying_operation_aborted(client, &bucket, key).await;

        // Delete all versions + delete marker
        let mut all_ids = version_ids.clone();
        all_ids.push(dm_vid);

        let objects: Vec<ObjectIdentifier> = all_ids
            .iter()
            .map(|v| {
                ObjectIdentifier::builder()
                    .key(key)
                    .version_id(v)
                    .build()
                    .unwrap()
            })
            .collect();
        let resp = delete_objects_retrying_operation_aborted(
            client,
            &bucket,
            Delete::builder()
                .set_objects(Some(objects.clone()))
                .build()
                .unwrap(),
        )
        .await;
        assert!(
            resp.errors().is_empty(),
            "unexpected errors: {:?}",
            resp.errors()
        );

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        assert!(resp.versions().is_empty());
        assert!(resp.delete_markers().is_empty());

        // Idempotent re-delete
        let resp = delete_objects_retrying_operation_aborted(
            client,
            &bucket,
            Delete::builder()
                .set_objects(Some(objects))
                .build()
                .unwrap(),
        )
        .await;
        assert!(
            resp.errors().is_empty(),
            "unexpected errors: {:?}",
            resp.errors()
        );

        delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_versioning_multi_object_delete_with_marker_create() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "key";

        // Use delete_objects to create a delete marker on a nonexistent key
        let objects = vec![ObjectIdentifier::builder().key(key).build().unwrap()];
        let resp = delete_objects_retrying_operation_aborted(
            client,
            &bucket,
            Delete::builder()
                .set_objects(Some(objects))
                .build()
                .unwrap(),
        )
        .await;

        assert!(
            resp.errors().is_empty(),
            "unexpected errors: {:?}",
            resp.errors()
        );
        assert_eq!(resp.deleted().len(), 1);
        assert!(resp.deleted()[0].delete_marker().unwrap_or(false));
        let dm_vid = resp.deleted()[0]
            .delete_marker_version_id()
            .unwrap()
            .to_string();

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        assert_eq!(resp.delete_markers().len(), 1);
        assert_eq!(
            resp.delete_markers()[0].version_id().unwrap(),
            dm_vid.as_str()
        );
        assert_eq!(resp.delete_markers()[0].key().unwrap(), key);

        // Cleanup
        delete_object_version_retrying_operation_aborted(client, &bucket, key, &dm_vid).await;
        delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Version ID return behavior ──────────────────────────────────────

#[test]
fn test_versioning_bucket_atomic_upload_return_version_id() {
    s3_tests::run(async {
        let client = CTX.client();

        // Versioning-enabled: should return a version ID
        let bucket = setup_versioned_bucket().await;
        let resp = put_object_retrying_operation_aborted(client, &bucket, "bar", Vec::new()).await;
        let version_id = resp.version_id().unwrap().to_string();

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        assert_eq!(resp.versions().len(), 1);
        assert_eq!(
            resp.versions()[0].version_id().unwrap(),
            version_id.as_str()
        );

        cleanup_versioned_bucket(client, &bucket).await;

        // Default (no versioning): should not return a version ID
        let bucket2 = unique_bucket();
        s3_tests::create_bucket(client, &bucket2).await.unwrap();
        let resp = put_object_retrying_operation_aborted(client, &bucket2, "baz", Vec::new()).await;
        assert!(
            resp.version_id().is_none(),
            "expected no version ID for non-versioned bucket"
        );
        client
            .delete_object()
            .bucket(&bucket2)
            .key("baz")
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        delete_bucket_retrying_operation_aborted(client, &bucket2).await;

        // Suspended: should not return a version ID
        let bucket3 = unique_bucket();
        s3_tests::create_bucket(client, &bucket3).await.unwrap();
        put_bucket_versioning_retrying_operation_aborted(
            client,
            &bucket3,
            BucketVersioningStatus::Suspended,
        )
        .await;
        let resp = put_object_retrying_operation_aborted(client, &bucket3, "baz", Vec::new()).await;
        assert!(
            resp.version_id().is_none(),
            "expected no version ID for suspended bucket"
        );
        cleanup_versioned_bucket(client, &bucket3).await;
    });
}

// ── Concurrent delete ───────────────────────────────────────────────

#[test]
fn test_versioned_concurrent_object_create_concurrent_remove() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "myobj";
        let num_versions = 5;

        for _ in 0..5 {
            create_versioned_object_concurrent(
                client.clone(),
                bucket.clone(),
                key.to_string(),
                num_versions,
            )
            .await;

            wait_for_minimum_version_listing_counts(
                client,
                &bucket,
                num_versions,
                0,
                "version listing after concurrent creates",
            )
            .await;

            clear_versioned_bucket_concurrent(client.clone(), bucket.clone()).await;

            wait_for_version_listing_counts(
                client,
                &bucket,
                0,
                0,
                "version listing after concurrent removal",
            )
            .await;
        }

        delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_versioned_concurrent_object_create_and_remove() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "myobj";
        let num_versions = 3;

        let mut tasks = Vec::new();
        for _ in 0..3 {
            tasks.push(tokio::spawn(create_versioned_object_concurrent(
                client.clone(),
                bucket.clone(),
                key.to_string(),
                num_versions,
            )));
            tasks.push(tokio::spawn(clear_versioned_bucket_concurrent(
                client.clone(),
                bucket.clone(),
            )));
        }

        for task in tasks {
            task.await.unwrap();
        }

        clear_versioned_bucket_concurrent(client.clone(), bucket.clone()).await;

        wait_for_version_listing_counts(
            client,
            &bucket,
            0,
            0,
            "version listing after final concurrent cleanup",
        )
        .await;

        delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_versioning_concurrent_multi_object_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let num_objects = 5;
        let num_versions = 3;

        let key_names: Vec<String> = (0..num_objects).map(|i| format!("key_{}", i)).collect();

        // Create num_versions versions of each key
        for _ in 0..num_versions {
            for key in &key_names {
                put_object_retrying_operation_aborted(client, &bucket, key, b"data".to_vec()).await;
            }
        }

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        let versions = resp.versions();
        assert_eq!(versions.len(), num_objects * num_versions);

        // Delete all versions
        let objects: Vec<ObjectIdentifier> = versions
            .iter()
            .map(|v| {
                ObjectIdentifier::builder()
                    .key(v.key().unwrap())
                    .version_id(v.version_id().unwrap())
                    .build()
                    .unwrap()
            })
            .collect();

        let resp = delete_objects_retrying_operation_aborted(
            client,
            &bucket,
            Delete::builder()
                .set_objects(Some(objects))
                .build()
                .unwrap(),
        )
        .await;
        assert!(
            resp.errors().is_empty(),
            "unexpected errors: {:?}",
            resp.errors()
        );
        assert_eq!(resp.deleted().len(), num_objects * num_versions);

        wait_for_version_listing_counts(
            client,
            &bucket,
            0,
            0,
            "version listing after multi-object delete",
        )
        .await;

        delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Delete marker tests ─────────────────────────────────────────────

#[test]
fn test_delete_marker_nonversioned() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let key = "frodo.txt";
        put_object_retrying_operation_aborted(client, &bucket, key, b"body".to_vec()).await;

        let resp = client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        // Non-versioned delete should not produce a delete marker
        assert!(!resp.delete_marker().unwrap_or(false));

        delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_delete_marker_versioned() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;

        let key = "bilbo.txt";
        let put_resp =
            put_object_retrying_operation_aborted(client, &bucket, key, b"body".to_vec()).await;
        let vid = put_resp.version_id().unwrap().to_string();

        let dm_vid = delete_object_retrying_operation_aborted(client, &bucket, key).await;

        // Cleanup
        delete_object_version_retrying_operation_aborted(client, &bucket, key, &dm_vid).await;
        delete_object_version_retrying_operation_aborted(client, &bucket, key, &vid).await;
        delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── Versioning configuration ─────────────────────────────────────────

#[test]
fn test_versioning_bucket_create_suspend() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // Fresh bucket: versioning status should be absent (unversioned)
        let resp = client
            .get_bucket_versioning()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        assert!(
            resp.status().is_none(),
            "expected no versioning status on new bucket"
        );

        // Suspend → Suspended
        put_bucket_versioning_retrying_operation_aborted(
            client,
            &bucket,
            BucketVersioningStatus::Suspended,
        )
        .await;
        let resp = client
            .get_bucket_versioning()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        assert_eq!(resp.status(), Some(&BucketVersioningStatus::Suspended));

        // Enable → Enabled
        put_bucket_versioning_retrying_operation_aborted(
            client,
            &bucket,
            BucketVersioningStatus::Enabled,
        )
        .await;
        let resp = client
            .get_bucket_versioning()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        assert_eq!(resp.status(), Some(&BucketVersioningStatus::Enabled));

        // Enable again (idempotent) → still Enabled
        put_bucket_versioning_retrying_operation_aborted(
            client,
            &bucket,
            BucketVersioningStatus::Enabled,
        )
        .await;
        let resp = client
            .get_bucket_versioning()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        assert_eq!(resp.status(), Some(&BucketVersioningStatus::Enabled));

        // Suspend → Suspended
        put_bucket_versioning_retrying_operation_aborted(
            client,
            &bucket,
            BucketVersioningStatus::Suspended,
        )
        .await;
        let resp = client
            .get_bucket_versioning()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        assert_eq!(resp.status(), Some(&BucketVersioningStatus::Suspended));

        delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_versioning_obj_create_overwrite_multipart() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "mp-overwrite";

        // Put a plain object first
        let put =
            put_object_retrying_operation_aborted(client, &bucket, key, b"original".to_vec()).await;
        let v1 = put.version_id().unwrap().to_string();

        // Overwrite with a multipart upload
        let part_data = vec![b'Z'; 5 * 1024 * 1024];
        let create = create_multipart_upload_retrying_operation_aborted(client, &bucket, key).await;
        let upload_id = create.upload_id().unwrap();
        let part_resp =
            upload_part_retrying_operation_aborted(client, &bucket, key, upload_id, 1, part_data)
                .await;
        let complete = complete_multipart_upload_retrying_operation_aborted(
            client,
            &bucket,
            key,
            upload_id,
            part_resp.e_tag().unwrap(),
        )
        .await;
        let v2 = complete.version_id().unwrap().to_string();
        assert_ne!(v1, v2, "multipart should create a new version");

        // Current version is the multipart object
        let get = client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();
        assert_eq!(get.content_length(), Some(5 * 1024 * 1024));

        // Old version still has original content
        let body = get_object_body_retrying_operation_aborted(
            client,
            &bucket,
            key,
            Some(&v1),
            "get object body during versioning test",
        )
        .await;
        assert_eq!(&body[..], b"original");

        cleanup_versioned(&bucket, key, &[v1, v2]).await;
    });
}

#[test]
fn test_list_object_versions_includes_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;

        put_object_retrying_operation_aborted(client, &bucket, "owned", b"data".to_vec()).await;

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();

        let version = resp
            .versions()
            .iter()
            .find(|v| v.key() == Some("owned"))
            .expect("expected version entry for uploaded object");
        let owner = version.owner().expect("expected owner in version entry");
        let owner_id = owner.id().expect("expected owner ID in version entry");
        assert_canonical_owner_id(owner_id);

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_list_object_versions_delete_marker_includes_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;

        put_object_retrying_operation_aborted(client, &bucket, "owned", b"data".to_vec()).await;

        delete_object_retrying_operation_aborted(client, &bucket, "owned").await;

        let resp = client
            .list_object_versions()
            .bucket(&bucket)
            .send_retrying_operation_aborted("S3 operation during versioning test")
            .await
            .unwrap();

        let delete_marker = resp
            .delete_markers()
            .iter()
            .find(|v| v.key() == Some("owned"))
            .expect("expected delete marker entry for deleted object");
        let owner = delete_marker
            .owner()
            .expect("expected owner in delete marker entry");
        let owner_id = owner
            .id()
            .expect("expected owner ID in delete marker entry");
        assert_canonical_owner_id(owner_id);

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_versioning_bucket_multipart_upload_return_version_id() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_versioned_bucket().await;
        let key = "mp-vid";

        // Multipart upload on versioned bucket should return version_id
        let part_data = vec![b'V'; 5 * 1024 * 1024];
        let create = create_multipart_upload_retrying_operation_aborted(client, &bucket, key).await;
        let upload_id = create.upload_id().unwrap();
        let part_resp =
            upload_part_retrying_operation_aborted(client, &bucket, key, upload_id, 1, part_data)
                .await;
        let complete = complete_multipart_upload_retrying_operation_aborted(
            client,
            &bucket,
            key,
            upload_id,
            part_resp.e_tag().unwrap(),
        )
        .await;

        let version_id = complete
            .version_id()
            .expect("CompleteMultipartUpload should return version_id on versioned bucket");
        assert!(!version_id.is_empty());

        cleanup_versioned(&bucket, key, &[version_id.to_string()]).await;
    });
}

// ── GetBucketVersioning response shape ──────────────────────────────

#[test]
fn test_get_bucket_versioning_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        s3_tests::enable_bucket_versioning(client, &bucket).await;

        let response = raw_bucket("GET", &bucket, Some("versioning="));
        assert_shape(
            "GetBucketVersioning",
            &response,
            &shape()
                .status(200)
                .headers(chunked_response_headers())
                .body(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<VersioningConfiguration \
                     xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>Enabled</Status>\
                     </VersioningConfiguration>",
                ),
        );

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

// ── Versioned object response shapes ────────────────────────────────

#[test]
fn test_versioned_object_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        s3_tests::enable_bucket_versioning(client, &bucket).await;
        let key = "shape-versioned.txt";

        let put = s3_tests::put_object_retrying_operation_aborted(
            client,
            &bucket,
            key,
            b"versioned-body".to_vec(),
        )
        .await;
        let version = put.version_id().expect("version id").to_string();

        let expected_headers = [
            ("etag", "{etag}"),
            ("last-modified", "{http_date}"),
            ("accept-ranges", "bytes"),
            ("x-amz-version-id", "{version_id}"),
            ("content-type", "application/octet-stream"),
            ("x-amz-server-side-encryption", "AES256"),
            ("content-length", "14"),
            ("x-amz-request-id", "{request_id}"),
            ("x-amz-id-2", "{host_id}"),
        ];

        for (operation, method, query, expect_body) in [
            ("GetObject current version", "GET", None, true),
            ("HeadObject current version", "HEAD", None, false),
            ("GetObject explicit version", "GET", Some(&version), true),
            ("HeadObject explicit version", "HEAD", Some(&version), false),
        ] {
            let response = match query {
                Some(version) => {
                    raw_object_query(method, &bucket, key, &format!("versionId={version}"))
                }
                None => raw_object(method, &bucket, key),
            };
            let mut spec = shape()
                .status(200)
                .headers(expected_headers)
                .sub("version_id", version.as_str());
            spec = if expect_body {
                spec.body("versioned-body")
            } else {
                spec.body_empty()
            };
            assert_shape(operation, &response, &spec);
        }

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_delete_object_versioned_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        s3_tests::enable_bucket_versioning(client, &bucket).await;
        let key = "shape-delete.txt";

        s3_tests::put_object_retrying_operation_aborted(
            client,
            &bucket,
            key,
            b"delete-shape".to_vec(),
        )
        .await;

        let delete_current = raw_object("DELETE", &bucket, key);
        let marker_captures = assert_shape(
            "DeleteObject current version",
            &delete_current,
            &shape()
                .status(204)
                .headers(id_headers())
                .header("x-amz-version-id", "{version_id}")
                .header("x-amz-delete-marker", "true")
                .body_empty(),
        );
        let marker_version = marker_captures["version_id"].clone();

        let delete_marker = raw_object_query(
            "DELETE",
            &bucket,
            key,
            &format!("versionId={marker_version}"),
        );
        assert_shape(
            "DeleteObject delete marker version",
            &delete_marker,
            &shape()
                .status(204)
                .headers(id_headers())
                .header("x-amz-version-id", "{version_id}")
                .header("x-amz-delete-marker", "true")
                .sub("version_id", marker_version.as_str())
                .body_empty(),
        );

        cleanup_versioned_bucket(client, &bucket).await;
    });
}
