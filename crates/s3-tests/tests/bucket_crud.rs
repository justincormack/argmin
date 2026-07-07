use std::sync::atomic::{AtomicU64, Ordering};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketLocationConstraint, CreateBucketConfiguration, VersioningConfiguration,
};
use s3_tests::{
    assert_s3_err_code, bucket_prefix, cleanup_versioned_bucket, delete_all_and_bucket, err_status,
    expected_raw_bucket_location_constraint, raw_bucket, retrying_operation_aborted,
    retrying_operation_aborted_result, send_signed_request,
    shape::{assert_shape, assert_shape_one_of, shape, xml_response_headers},
    unique_bucket, RawResponse, SendRetryingOperationAborted, CTX,
};
use s3_types::{is_legacy_create_bucket_region, BucketNamespace};

static ACCOUNT_REGIONAL_BUCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

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

async fn create_bucket_in_test_region(client: &aws_sdk_s3::Client, bucket: &str) {
    let mut request = client.create_bucket().bucket(bucket);
    if CTX.region() != "us-east-1" {
        let config = CreateBucketConfiguration::builder()
            .location_constraint(BucketLocationConstraint::from(CTX.region()))
            .build();
        request = request.create_bucket_configuration(config);
    }
    request
        .send_retrying_operation_aborted("create bucket in test region")
        .await
        .unwrap();
}

async fn put_object(bucket: &str, key: &str, body: &'static [u8]) {
    retrying_operation_aborted("put bucket CRUD object", || async move {
        CTX.client()
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(body))
            .send()
            .await
    })
    .await;
}

fn account_regional_bucket_name(account_id: &str, region: &str) -> String {
    let suffix = format!("-{account_id}-{region}-an");
    let n = ACCOUNT_REGIONAL_BUCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let prefix_max = 63usize
        .checked_sub(suffix.len())
        .expect("account-regional suffix should leave room for a prefix");
    let mut prefix = format!("{}{}{}", bucket_prefix(), std::process::id(), n);
    if prefix.len() > prefix_max {
        prefix.truncate(prefix_max);
    }
    format!("{prefix}{suffix}")
}

fn create_bucket_configuration_body(region: &str) -> Vec<u8> {
    if is_legacy_create_bucket_region(region) {
        Vec::new()
    } else {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>{region}</LocationConstraint></CreateBucketConfiguration>"#
        )
        .into_bytes()
    }
}

fn create_bucket_in_namespace(bucket: &str, namespace: BucketNamespace) -> RawResponse {
    let url = format!("{}/{}", CTX.endpoint(), bucket);
    let body = create_bucket_configuration_body(CTX.region());
    send_signed_request(
        "PUT",
        &url,
        &body,
        [("x-amz-bucket-namespace", namespace.as_header_value())],
    )
}

fn assert_raw_s3_error(response: &RawResponse, status: u16, code: &str) {
    assert_eq!(
        response.status, status,
        "unexpected response body: {}",
        response.body
    );
    assert!(
        response.body.contains(&format!("<Code>{code}</Code>")),
        "expected {code} in response body, got: {}",
        response.body
    );
}

// ── CreateBucket ─────────────────────────────────────────────────────

#[test]
fn test_bucket_create_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        // Clean up
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_create_exists() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // Verify bucket exists via HEAD
        client
            .head_bucket()
            .bucket(&bucket)
            .send_retrying_operation_aborted("bucket CRUD request")
            .await
            .unwrap();

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_create_already_exists() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        create_bucket_in_test_region(client, &bucket).await;

        let mut request = client.create_bucket().bucket(&bucket);
        if CTX.region() != "us-east-1" {
            let config = CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::from(CTX.region()))
                .build();
            request = request.create_bucket_configuration(config);
        }
        let result = request
            .send_retrying_operation_aborted("bucket CRUD request")
            .await;
        if CTX.region() == "us-east-1" {
            result.unwrap();
        } else {
            assert_eq!(err_status(&result), 409);
            assert_s3_err_code(&result, "BucketAlreadyOwnedByYou");
        }

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_recreate_not_overriding() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let keys = vec!["mykey1".to_string(), "mykey2".to_string()];

        create_bucket_in_test_region(client, &bucket).await;
        for key in &keys {
            put_object(&bucket, key, b"data").await;
        }

        let mut request = client.create_bucket().bucket(&bucket);
        if CTX.region() != "us-east-1" {
            let config = CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::from(CTX.region()))
                .build();
            request = request.create_bucket_configuration(config);
        }
        let result = request
            .send_retrying_operation_aborted("bucket CRUD request")
            .await;
        if CTX.region() == "us-east-1" {
            result.unwrap();
        } else {
            assert_eq!(err_status(&result), 409);
            assert_s3_err_code(&result, "BucketAlreadyOwnedByYou");
        }

        let listed = client
            .list_objects_v2()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list bucket after recreate")
            .await
            .unwrap();
        let mut got: Vec<_> = listed
            .contents()
            .iter()
            .filter_map(|obj| obj.key().map(ToString::to_string))
            .collect();
        got.sort();
        assert_eq!(got, keys);

        delete_all_and_bucket(client, &bucket, &keys).await;
    });
}

#[test]
fn test_account_regional_bucket_create_succeeds_when_suffix_matches() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = account_regional_bucket_name(CTX.account_id(), CTX.region());
        let response = create_bucket_in_namespace(&bucket, BucketNamespace::AccountRegional);
        assert_eq!(
            response.status, 200,
            "unexpected response body: {}",
            response.body
        );
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_account_regional_bucket_rejects_mismatched_account_suffix() {
    s3_tests::run(async {
        let bucket = account_regional_bucket_name(CTX.alt_account_id(), CTX.region());
        let response = create_bucket_in_namespace(&bucket, BucketNamespace::AccountRegional);
        assert_raw_s3_error(&response, 400, "InvalidBucketNamespace");
    });
}

#[test]
fn test_account_regional_bucket_rejects_mismatched_region_suffix() {
    s3_tests::run(async {
        let wrong_region = if CTX.region() == "us-east-1" {
            "us-west-2"
        } else {
            "us-east-1"
        };
        let bucket = account_regional_bucket_name(CTX.account_id(), wrong_region);
        let response = create_bucket_in_namespace(&bucket, BucketNamespace::AccountRegional);
        assert_raw_s3_error(&response, 400, "InvalidBucketNamespace");
    });
}

#[test]
fn test_account_regional_bucket_recreate_returns_bucket_already_owned_by_you() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = account_regional_bucket_name(CTX.account_id(), CTX.region());

        let first = create_bucket_in_namespace(&bucket, BucketNamespace::AccountRegional);
        assert_eq!(
            first.status, 200,
            "unexpected response body: {}",
            first.body
        );

        let second = create_bucket_in_namespace(&bucket, BucketNamespace::AccountRegional);
        assert_raw_s3_error(&second, 409, "BucketAlreadyOwnedByYou");

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── DeleteBucket ─────────────────────────────────────────────────────

#[test]
fn test_bucket_delete_notexist() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let result = client
            .delete_bucket()
            .bucket(&bucket)
            .send_retrying_operation_aborted("bucket CRUD request")
            .await;
        assert_eq!(err_status(&result), 404);
    });
}

#[test]
fn test_bucket_delete_nonempty() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        put_object(&bucket, "key", b"data").await;

        // Delete bucket should fail (not empty)
        let result = client
            .delete_bucket()
            .bucket(&bucket)
            .send_retrying_operation_aborted("bucket CRUD request")
            .await;
        assert_eq!(err_status(&result), 409);

        // Clean up
        s3_tests::delete_object_retrying_operation_aborted(client, &bucket, "key")
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

/// Deleting a versioned bucket that contains only delete markers must fail
/// with 409 BucketNotEmpty. On AWS, delete_object on a versioned bucket
/// creates a delete marker rather than removing the object.
#[test]
fn test_bucket_delete_nonempty_delete_markers() {
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
                    .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send_retrying_operation_aborted("enable versioning for delete marker bucket")
            .await
            .unwrap();

        // Put an object, then delete it (creates a delete marker)
        put_object(&bucket, "key", b"data").await;
        client
            .delete_object()
            .bucket(&bucket)
            .key("key")
            .send_retrying_operation_aborted("create versioned delete marker")
            .await
            .unwrap();

        // Bucket still has versions + delete marker; delete must fail
        let result = client
            .delete_bucket()
            .bucket(&bucket)
            .send_retrying_operation_aborted("bucket CRUD request")
            .await;
        assert_eq!(err_status(&result), 409);

        // Clean up properly
        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_bucket_delete_then_recreate() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();

        s3_tests::create_bucket(client, &bucket).await.unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;

        // AWS documents that bucket removal can take time to finish, and
        // immediate same-name recreate may transiently return BucketAlreadyExists.
        s3_tests::create_bucket_retrying_reuse(client, &bucket)
            .await
            .unwrap();
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── HeadBucket ───────────────────────────────────────────────────────

#[test]
fn test_bucket_head() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        client
            .head_bucket()
            .bucket(&bucket)
            .send_retrying_operation_aborted("bucket CRUD request")
            .await
            .unwrap();

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_get_location() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        create_bucket_in_test_region(client, &bucket).await;

        let url = s3_tests::bucket_location_url(CTX.endpoint(), &bucket);
        let response = send_signed_request("GET", &url, b"", std::iter::empty::<(&str, &str)>());
        s3_tests::assert_raw_bucket_location(&response, CTX.region());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_head_expected_owner() {
    s3_tests::run(async {
        let account_id = CTX.account_id().to_string();
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        retrying_operation_aborted("head bucket with expected owner", || {
            let request = client
                .head_bucket()
                .bucket(&bucket)
                .customize()
                .mutate_request({
                    let account_id = account_id.clone();
                    move |req| {
                        req.headers_mut()
                            .insert("x-amz-expected-bucket-owner", account_id.clone());
                    }
                });
            async move { request.send().await }
        })
        .await;

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_head_wrong_expected_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let result = retrying_operation_aborted_result(|| {
            let request = client
                .head_bucket()
                .bucket(&bucket)
                .customize()
                .mutate_request(|req| {
                    req.headers_mut()
                        .insert("x-amz-expected-bucket-owner", "000000000000");
                });
            async move { request.send().await }
        })
        .await;
        assert_eq!(err_status(&result), 403);

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_head_notexist() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let result = client
            .head_bucket()
            .bucket(&bucket)
            .send_retrying_operation_aborted("bucket CRUD request")
            .await;
        assert!(result.is_err());
    });
}

// ── ListBuckets ──────────────────────────────────────────────────────

#[test]
fn test_buckets_list_empty() {
    s3_tests::run(async {
        // Note: this test may see buckets from other concurrent tests.
        // We just verify that list_buckets returns without error.
        let client = CTX.client();
        let _resp = client
            .list_buckets()
            .send_retrying_operation_aborted("bucket CRUD request")
            .await
            .unwrap();
    });
}

#[test]
fn test_buckets_list_contains_created() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let resp = client
            .list_buckets()
            .send_retrying_operation_aborted("bucket CRUD request")
            .await
            .unwrap();
        let names: Vec<&str> = resp.buckets().iter().filter_map(|b| b.name()).collect();
        assert!(
            names.contains(&bucket.as_str()),
            "expected bucket '{}' in list: {:?}",
            bucket,
            names
        );
        let owner = resp.owner().expect("expected owner in ListBuckets");
        let owner_id = owner.id().expect("expected owner ID in ListBuckets");
        assert_canonical_owner_id(owner_id);

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_list_objects_empty() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list empty bucket")
            .await
            .unwrap();
        assert_eq!(resp.key_count(), Some(0));
        assert!(resp.contents().is_empty());

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_list_objects_with_objects() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        for i in 0..3 {
            let key = format!("key{}", i);
            put_object(&bucket, &key, b"content").await;
        }

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list bucket with objects")
            .await
            .unwrap();
        assert_eq!(resp.key_count(), Some(3));

        let keys: Vec<&str> = resp.contents().iter().filter_map(|o| o.key()).collect();
        assert_eq!(keys, vec!["key0", "key1", "key2"]);

        // Clean up
        for i in 0..3 {
            s3_tests::delete_object_retrying_operation_aborted(
                client,
                &bucket,
                &format!("key{}", i),
            )
            .await
            .unwrap();
        }
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_list_objects_nonexistent_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let result = client
            .list_objects_v2()
            .bucket(&bucket)
            .send_retrying_operation_aborted("bucket CRUD request")
            .await;
        assert_eq!(err_status(&result), 404);
        assert_s3_err_code(&result, "NoSuchBucket");
    });
}

#[test]
fn test_bucket_list_objects_nonexistent_bucket_alt_client() {
    s3_tests::run(async {
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let result = alt_client
            .list_objects_v2()
            .bucket(&bucket)
            .send_retrying_operation_aborted("bucket CRUD request")
            .await;
        assert_eq!(err_status(&result), 404);
        assert_s3_err_code(&result, "NoSuchBucket");
    });
}

// ── Extended HEAD / ACL / ownership ─────────────────────────────────

#[test]
fn test_bucket_head_extended() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // HEAD should return without error and include standard headers
        client
            .head_bucket()
            .bucket(&bucket)
            .send_retrying_operation_aborted("bucket CRUD request")
            .await
            .unwrap();

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_create_special_key_names() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // Create objects with special key names
        let special_keys = &["foo/bar", "foo&bar", "foo bar", "foo+bar"];
        for key in special_keys {
            put_object(&bucket, key, b"data").await;
        }

        // Verify they all exist
        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .send_retrying_operation_aborted("list bucket with special keys")
            .await
            .unwrap();
        assert_eq!(resp.key_count(), Some(special_keys.len() as i32));

        // Clean up
        for key in special_keys {
            s3_tests::delete_object_retrying_operation_aborted(client, &bucket, key)
                .await
                .unwrap();
        }
        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_buckets_list_ctime() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let resp = client
            .list_buckets()
            .send_retrying_operation_aborted("bucket CRUD request")
            .await
            .unwrap();
        let found = resp
            .buckets()
            .iter()
            .find(|b| b.name() == Some(bucket.as_str()));
        assert!(found.is_some(), "bucket should be in listing");
        assert!(
            found.unwrap().creation_date().is_some(),
            "bucket should have creation date"
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_bucket_create_exists_nonowner() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let result = s3_tests::create_bucket_request(alt_client, &bucket)
            .send_retrying_operation_aborted("create bucket as nonowner")
            .await;
        assert_eq!(err_status(&result), 409);
        assert_s3_err_code(&result, "BucketAlreadyExists");

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

// ── GetBucketLocation response shape ────────────────────────────────

#[test]
fn test_get_bucket_location_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let response = raw_bucket("GET", &bucket, Some("location="));
        let expected_body = match expected_raw_bucket_location_constraint(CTX.region()) {
            Some(constraint) => format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LocationConstraint \
                 xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{constraint}</LocationConstraint>"
            ),
            None => "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LocationConstraint \
                     xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>"
                .to_string(),
        };
        assert_shape(
            "GetBucketLocation",
            &response,
            &shape()
                .status(200)
                .headers(xml_response_headers())
                .body(expected_body),
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}

#[test]
fn test_head_bucket_response_shape() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let response = raw_bucket("HEAD", &bucket, None);
        let base_headers = [
            ("content-type", "application/xml"),
            ("x-amz-access-point-alias", "false"),
            ("x-amz-bucket-arn", "{bucket_arn}"),
            ("x-amz-bucket-region", "{region}"),
            ("x-amz-request-id", "{request_id}"),
            ("x-amz-id-2", "{host_id}"),
        ];
        // AWS includes Transfer-Encoding: chunked on HeadBucket; Argmin's
        // HEAD handling (via Hyper) omits it. See guides/aws-compatibility.md
        // ("HeadBucket omits Transfer-Encoding").
        assert_shape_one_of(
            "HeadBucket shape",
            &response,
            &[
                shape()
                    .status(200)
                    .headers(base_headers)
                    .header("transfer-encoding", "chunked")
                    .sub("bucket_arn", format!("arn:aws:s3:::{bucket}"))
                    .sub("region", CTX.region())
                    .body_empty(),
                shape()
                    .status(200)
                    .headers(base_headers)
                    .sub("bucket_arn", format!("arn:aws:s3:::{bucket}"))
                    .sub("region", CTX.region())
                    .body_empty(),
            ],
        );

        s3_tests::delete_bucket_retrying_operation_aborted(client, &bucket).await;
    });
}
