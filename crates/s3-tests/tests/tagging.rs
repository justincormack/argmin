use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart,
    MetadataDirective as CopyMetadataDirective, ObjectCannedAcl, Tag, Tagging,
    VersioningConfiguration,
};
use s3_tests::{
    assert_s3_err_code, cleanup_versioned_bucket, create_public_bucket, err_status, unique_bucket,
    CTX,
};
use serde_json::json;
use std::collections::BTreeSet;
use std::sync::{LazyLock, Mutex};

static BUCKET_POLICY_TEST_GUARD: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// Cleanup helper.
async fn cleanup(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

fn tag(key: &str, value: &str) -> Tag {
    Tag::builder().key(key).value(value).build().unwrap()
}

fn tagging(tags: Vec<Tag>) -> Tagging {
    Tagging::builder().set_tag_set(Some(tags)).build().unwrap()
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

fn alt_policy_principal() -> serde_json::Value {
    json!({ "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) })
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
            .send()
            .await
            .unwrap();

        // GET bucket tagging
        let result = client
            .get_bucket_tagging()
            .bucket(&bucket)
            .send()
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
            .send()
            .await
            .unwrap();

        // GET after delete should fail with NoSuchTagSet
        let result = client.get_bucket_tagging().bucket(&bucket).send().await;
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
        let result = client.get_bucket_tagging().bucket(&bucket).send().await;
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
            .send()
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
            .send()
            .await
            .unwrap();

        let result = client
            .get_bucket_tagging()
            .bucket(&bucket)
            .send()
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
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        // PUT object tagging
        let tags = tagging(vec![tag("env", "staging"), tag("cost-center", "123")]);
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tags)
            .send()
            .await
            .unwrap();

        // GET object tagging
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send()
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
            .send()
            .await
            .unwrap();

        // GET after delete should return empty tag set
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        // GET object tagging when not set → returns empty TagSet (not 404)
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        // DELETE object tagging when not set → idempotent, succeeds
        client
            .delete_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        // 10 tags should succeed
        let tags: Vec<Tag> = (0..10)
            .map(|i| tag(&format!("key{i}"), &format!("val{i}")))
            .collect();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(tags))
            .send()
            .await
            .unwrap();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        // 11 tags should fail
        let tags: Vec<Tag> = (0..11)
            .map(|i| tag(&format!("key{i}"), &format!("val{i}")))
            .collect();
        let result = client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(tags))
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        // Set initial tags
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(vec![tag("old", "value")]))
            .send()
            .await
            .unwrap();

        // Overwrite with new tags
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(vec![tag("new", "value2")]))
            .send()
            .await
            .unwrap();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"hello"))
            .tagging("foo=bar&bar")
            .send()
            .await
            .unwrap();

        // Verify via GetObjectTagging
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send()
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
fn test_public_read_object_does_not_make_get_object_tagging_public() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = create_public_bucket(client).await;

        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .acl(ObjectCannedAcl::PublicRead)
            .tagging("env=public")
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        let url = format!("{}/{}/obj?tagging", CTX.endpoint(), bucket);
        let mut resp = s3_tests::test_agent()
            .get(&url)
            .call()
            .expect("transport error");
        assert_eq!(
            resp.status().as_u16(),
            403,
            "expected anonymous GetObjectTagging to be denied for public-read object, got {}",
            resp.status().as_u16()
        );

        let body = resp.body_mut().read_to_string().unwrap();
        assert!(
            body.contains("AccessDenied"),
            "expected AccessDenied response body, got {body}"
        );

        cleanup(&bucket, &["obj"]).await;
    });
}

// ── Tagging count header ────────────────────────────────────────────────

#[test]
fn test_get_object_tagging_count_header() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // Put object with tags
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"hello"))
            .tagging("a=1&b=2&c=3")
            .send()
            .await
            .unwrap();

        // Use AWS SDK's GetObject which exposes tag_count
        let result = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"hello"))
            .tagging("a=1&b=2")
            .send()
            .await
            .unwrap();

        // HeadObject should return x-amz-tagging-count
        let result = client
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // CopyObject with x-amz-tagging + REPLACE directive
        client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .tagging("copied=true&env=test")
            .tagging_directive(aws_sdk_s3::types::TaggingDirective::Replace)
            .send()
            .await
            .unwrap();

        // Verify tags on destination
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("dst")
            .send()
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
        let result = client.get_bucket_tagging().bucket(&bucket).send().await;
        assert!(result.is_err());

        // PUT single tag
        client
            .put_bucket_tagging()
            .bucket(&bucket)
            .tagging(tagging(vec![tag("Hello", "World")]))
            .send()
            .await
            .unwrap();

        // GET → verify
        let result = client
            .get_bucket_tagging()
            .bucket(&bucket)
            .send()
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
            .send()
            .await
            .unwrap();

        // GET after delete → NoSuchTagSet
        let result = client.get_bucket_tagging().bucket(&bucket).send().await;
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
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        let input_tags = vec![tag("0", "0"), tag("1", "1")];
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(input_tags))
            .send()
            .await
            .unwrap();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        let input_tags = vec![tag("0", "0"), tag("1", "1")];
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(input_tags))
            .send()
            .await
            .unwrap();

        let result = client
            .head_object()
            .bucket(&bucket)
            .key("obj")
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        let input_tags: Vec<Tag> = (0..10)
            .map(|i| tag(&i.to_string(), &i.to_string()))
            .collect();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(input_tags.clone()))
            .send()
            .await
            .unwrap();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        let input_tags: Vec<Tag> = (0..11)
            .map(|i| tag(&i.to_string(), &i.to_string()))
            .collect();
        let result = client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(input_tags))
            .send()
            .await;
        assert!(result.is_err());

        // No tags should be stored
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        // Set initial tags
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(vec![tag("key", "val"), tag("key2", "val2")]))
            .send()
            .await
            .unwrap();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send()
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
            .send()
            .await
            .unwrap();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        let input_tags = vec![tag("0", "0"), tag("1", "1")];
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(input_tags))
            .send()
            .await
            .unwrap();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        assert_eq!(result.tag_set().len(), 2);

        client
            .delete_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from(data.clone().into_bytes()))
            .tagging("foo=bar&bar")
            .send()
            .await
            .unwrap();

        // Verify body
        let result = client
            .get_object()
            .bucket(&bucket)
            .key("obj")
            .send()
            .await
            .unwrap();
        let body = result.body.collect().await.unwrap().into_bytes().to_vec();
        assert_eq!(String::from_utf8(body).unwrap(), data);

        // Verify tags
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

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
            .send()
            .await
            .unwrap();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        // Tag key of 129 chars should be rejected
        let tags = vec![tag(&random_string(129), "val")];
        let result = client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(tags))
            .send()
            .await;
        assert!(result.is_err());

        // Verify no tags were stored
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await
            .unwrap();

        // Tag value of 257 chars should be rejected
        let tags = vec![tag("key", &random_string(257))];
        let result = client
            .put_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .tagging(tagging(tags))
            .send()
            .await;
        assert!(result.is_err());

        // Verify no tags were stored
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("obj")
            .send()
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
        let result = client
            .put_object()
            .bucket(&bucket)
            .key("obj")
            .body(ByteStream::from_static(b"hello"))
            .tagging(&tag_str)
            .send()
            .await;
        assert!(result.is_err());

        // Object should not exist
        let head = client.head_object().bucket(&bucket).key("obj").send().await;
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
        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"data"))
            .tagging("color=blue&size=large")
            .send()
            .await
            .unwrap();

        // Copy without tagging-directive (default = COPY)
        client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .send()
            .await
            .unwrap();

        // Destination should have source's tags
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("dst")
            .send()
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
        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"data"))
            .tagging("color=blue")
            .send()
            .await
            .unwrap();

        // Copy with REPLACE but no tagging header
        client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .tagging_directive(aws_sdk_s3::types::TaggingDirective::Replace)
            .send()
            .await
            .unwrap();

        // Destination should have no tags
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key("dst")
            .send()
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
        .send()
        .await
        .unwrap();

    let key = "dm-test-obj";

    // Put an object
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(b"hello"))
        .send()
        .await
        .unwrap();

    // Delete the object (creates a delete marker)
    let delete_resp = client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
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
            .send()
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
            .send()
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
            .send()
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
            .send()
            .await;
        assert!(result.is_err());
        assert_eq!(err_status(&result), 405);
        assert_s3_err_code(&result, "MethodNotAllowed");

        // GetObjectTagging without versionId on deleted object → 405
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await;
        assert!(result.is_err());
        assert_eq!(err_status(&result), 405);
        assert_s3_err_code(&result, "MethodNotAllowed");

        // DeleteObjectTagging without versionId on deleted object → 405
        let result = client
            .delete_object_tagging()
            .bucket(&bucket)
            .key(&key)
            .send()
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
            .send()
            .await
            .unwrap();

        let key = "tagged-then-deleted";

        // Put an object with tags
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"hello"))
            .tagging("env=prod&team=platform")
            .send()
            .await
            .unwrap();

        // Verify tags are set
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(result.tag_set().len(), 2);

        // Delete the object (creates delete marker)
        let delete_resp = client
            .delete_object()
            .bucket(&bucket)
            .key(key)
            .send()
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
            .send()
            .await;
        assert!(result.is_err());
        assert_eq!(err_status(&result), 405);

        // GetObjectTagging without versionId (current = delete marker) → 405
        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send()
            .await;
        assert!(result.is_err());
        assert_eq!(err_status(&result), 405);

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
            .send()
            .await
            .unwrap();
        let upload_id = create.upload_id().unwrap();

        let data = vec![b'a'; 5 * 1024 * 1024];
        let upload = client
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(data))
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
                            .e_tag(upload.e_tag().unwrap())
                            .part_number(1)
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send()
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
            .send()
            .await
            .unwrap();

        let result = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert!(result.tag_set().is_empty());

        cleanup(&bucket, &[key]).await;
    });
}

// ── Bucket policy tagging access control ──────────────────────────────

#[test]
fn test_get_tags_acl_public() {
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
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

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
            .send()
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
            .send()
            .await
            .unwrap();

        let response = alt_client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_tag_sets_match_unordered(response.tag_set(), input_tags.tag_set());

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_put_tags_acl_public() {
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
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

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
            .send()
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
            .send()
            .await
            .unwrap();

        let response = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_tag_sets_match_unordered(response.tag_set(), input_tags.tag_set());

        cleanup(&bucket, &[key]).await;
    });
}

#[test]
fn test_delete_tags_obj_public() {
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
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

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
            .send()
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
            .send()
            .await
            .unwrap();

        alt_client
            .delete_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();

        let response = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert!(response.tag_set().is_empty());

        cleanup(&bucket, &[key]).await;
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

        for key in ["publictag", "privatetag", "invalidtag"] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(key.as_bytes().to_vec()))
                .send()
                .await
                .unwrap();
        }

        let policy = bucket_policy_document(
            principal,
            "s3:GetObject",
            bucket_wildcard_resource(&bucket),
            Some(json!({
                "StringEquals": {
                    "s3:ExistingObjectTag/security": "public"
                }
            })),
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("publictag")
            .tagging(tagging(vec![tag("security", "public"), tag("foo", "bar")]))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("privatetag")
            .tagging(tagging(vec![tag("security", "private")]))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("invalidtag")
            .tagging(tagging(vec![tag("security1", "public")]))
            .send()
            .await
            .unwrap();

        let response = alt_client
            .get_object()
            .bucket(&bucket)
            .key("publictag")
            .send()
            .await
            .unwrap();
        let body = response.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"publictag");

        for key in ["privatetag", "invalidtag"] {
            let result = alt_client
                .get_object()
                .bucket(&bucket)
                .key(key)
                .send()
                .await;
            assert_eq!(err_status(&result), 403);
            assert_s3_err_code(&result, "AccessDenied");
        }

        cleanup(&bucket, &["publictag", "privatetag", "invalidtag"]).await;
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

        for key in ["publictag", "privatetag", "invalidtag"] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(key.as_bytes().to_vec()))
                .send()
                .await
                .unwrap();
        }

        let policy = bucket_policy_document(
            principal,
            "s3:GetObjectTagging",
            bucket_wildcard_resource(&bucket),
            Some(json!({
                "StringEquals": {
                    "s3:ExistingObjectTag/security": "public"
                }
            })),
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("publictag")
            .tagging(tagging(vec![tag("security", "public"), tag("foo", "bar")]))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("privatetag")
            .tagging(tagging(vec![tag("security", "private")]))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("invalidtag")
            .tagging(tagging(vec![tag("security1", "public")]))
            .send()
            .await
            .unwrap();

        let response = alt_client
            .get_object_tagging()
            .bucket(&bucket)
            .key("publictag")
            .send()
            .await
            .unwrap();
        assert!(response
            .tag_set()
            .iter()
            .any(|tag| tag.key() == "security" && tag.value() == "public"));

        let get_object = alt_client
            .get_object()
            .bucket(&bucket)
            .key("publictag")
            .send()
            .await;
        assert_eq!(err_status(&get_object), 403);
        assert_s3_err_code(&get_object, "AccessDenied");

        for key in ["privatetag", "invalidtag"] {
            let result = alt_client
                .get_object_tagging()
                .bucket(&bucket)
                .key(key)
                .send()
                .await;
            assert_eq!(err_status(&result), 403);
            assert_s3_err_code(&result, "AccessDenied");
        }

        cleanup(&bucket, &["publictag", "privatetag", "invalidtag"]).await;
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

        for key in ["publictag", "privatetag"] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(key.as_bytes().to_vec()))
                .send()
                .await
                .unwrap();
        }

        let policy = bucket_policy_document(
            principal,
            "s3:PutObjectTagging",
            bucket_wildcard_resource(&bucket),
            Some(json!({
                "StringEquals": {
                    "s3:ExistingObjectTag/security": "public"
                }
            })),
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("publictag")
            .tagging(tagging(vec![tag("security", "public"), tag("foo", "bar")]))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("privatetag")
            .tagging(tagging(vec![tag("security", "private")]))
            .send()
            .await
            .unwrap();

        let public_tags = tagging(vec![tag("security", "public"), tag("foo", "bar")]);
        alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key("publictag")
            .tagging(public_tags.clone())
            .send()
            .await
            .unwrap();

        let private_result = alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key("privatetag")
            .tagging(public_tags.clone())
            .send()
            .await;
        assert_eq!(err_status(&private_result), 403);
        assert_s3_err_code(&private_result, "AccessDenied");

        let private_tags = tagging(vec![tag("security", "private")]);
        alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key("publictag")
            .tagging(private_tags)
            .send()
            .await
            .unwrap();

        let second_result = alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key("publictag")
            .tagging(public_tags)
            .send()
            .await;
        assert_eq!(err_status(&second_result), 403);
        assert_s3_err_code(&second_result, "AccessDenied");

        cleanup(&bucket, &["publictag", "privatetag"]).await;
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

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let policy = bucket_policy_document(
            principal,
            "s3:PutObjectTagging",
            bucket_wildcard_resource(&bucket),
            Some(json!({
                "StringEquals": {
                    "s3:RequestObjectTag/security": "public"
                }
            })),
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let public_tags = tagging(vec![tag("security", "public"), tag("foo", "bar")]);
        alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(public_tags.clone())
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(tagging(vec![tag("security", "private")]))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let stored = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_tag_sets_match_unordered(stored.tag_set(), public_tags.tag_set());

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
            .send()
            .await
            .unwrap();

        let version_id = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected version id")
            .to_string();

        let policy = bucket_policy_document(
            principal,
            "s3:PutObjectVersionTagging",
            bucket_wildcard_resource(&bucket),
            Some(json!({
                "StringEquals": {
                    "s3:RequestObjectTag/security": "public"
                }
            })),
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        let public_tags = tagging(vec![tag("security", "public"), tag("foo", "bar")]);
        alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .tagging(public_tags.clone())
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .tagging(tagging(vec![tag("security", "private")]))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let stored = client
            .get_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .send()
            .await
            .unwrap();
        assert_tag_sets_match_unordered(stored.tag_set(), public_tags.tag_set());

        cleanup_versioned_bucket(CTX.client(), &bucket).await;
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
            client
                .put_object()
                .bucket(&src_bucket)
                .key(key)
                .body(ByteStream::from(key.as_bytes().to_vec()))
                .send()
                .await
                .unwrap();
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
            .send()
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
            .send()
            .await
            .unwrap();

        alt_client
            .copy_object()
            .bucket(&dst_bucket)
            .key("new_foo")
            .copy_source(format!("{src_bucket}/public/foo"))
            .send()
            .await
            .unwrap();

        let response = client
            .get_object()
            .bucket(&dst_bucket)
            .key("new_foo")
            .send()
            .await
            .unwrap();
        let body = response.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"public/foo");

        alt_client
            .copy_object()
            .bucket(&dst_bucket)
            .key("new_foo2")
            .copy_source(format!("{src_bucket}/public/bar"))
            .send()
            .await
            .unwrap();

        let response = client
            .get_object()
            .bucket(&dst_bucket)
            .key("new_foo2")
            .send()
            .await
            .unwrap();
        let body = response.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"public/bar");

        let result = alt_client
            .copy_object()
            .bucket(&dst_bucket)
            .key("new_foo3")
            .copy_source(format!("{src_bucket}/private/foo"))
            .send()
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
            client
                .put_object()
                .bucket(&src_bucket)
                .key(key)
                .body(ByteStream::from(key.as_bytes().to_vec()))
                .send()
                .await
                .unwrap();
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
            .send()
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
            .send()
            .await
            .unwrap();

        alt_client
            .copy_object()
            .bucket(&dst_bucket)
            .key("new_foo")
            .copy_source(format!("{src_bucket}/public/foo"))
            .metadata_directive(CopyMetadataDirective::Copy)
            .send()
            .await
            .unwrap();

        let response = client
            .get_object()
            .bucket(&dst_bucket)
            .key("new_foo")
            .send()
            .await
            .unwrap();
        let body = response.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"public/foo");

        let result = alt_client
            .copy_object()
            .bucket(&dst_bucket)
            .key("new_foo2")
            .copy_source(format!("{src_bucket}/public/bar"))
            .metadata("foo", "bar")
            .customize()
            .mutate_request(|req| {
                req.headers_mut().remove("x-amz-metadata-directive");
            })
            .send()
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        cleanup(&dst_bucket, &["new_foo", "new_foo2"]).await;
        cleanup(&src_bucket, &["public/foo", "public/bar"]).await;
    });
}

#[test]
fn test_bucket_policy_put_obj_acl() {
    let _guard = BUCKET_POLICY_TEST_GUARD
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    s3_tests::run(async {
        let principal = alt_policy_principal();
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let policy = json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": principal.clone(),
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                },
                {
                    "Effect": "Deny",
                    "Principal": principal,
                    "Action": "s3:PutObject",
                    "Resource": bucket_wildcard_resource(&bucket),
                    "Condition": {
                        "StringLike": {
                            "s3:x-amz-acl": "public*"
                        }
                    }
                }
            ]
        })
        .to_string();
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        alt_client
            .put_object()
            .bucket(&bucket)
            .key("private-key")
            .body(ByteStream::from_static(b"private-key"))
            .send()
            .await
            .unwrap();

        let denied = alt_client
            .put_object()
            .bucket(&bucket)
            .key("public-key")
            .acl(ObjectCannedAcl::PublicRead)
            .body(ByteStream::from_static(b"public-key"))
            .send()
            .await;
        assert_eq!(err_status(&denied), 403);
        assert_s3_err_code(&denied, "AccessDenied");

        let streaming_denied = alt_client
            .put_object()
            .bucket(&bucket)
            .key("public-streaming-key")
            .acl(ObjectCannedAcl::PublicRead)
            .body(ByteStream::from(vec![
                0x5Au8;
                server_core::coordinator::INTERNAL_SEGMENT_SIZE
                    + 1
            ]))
            .send()
            .await;
        assert_eq!(err_status(&streaming_denied), 403);
        assert_s3_err_code(&streaming_denied, "AccessDenied");

        cleanup(
            &bucket,
            &["private-key", "public-key", "public-streaming-key"],
        )
        .await;
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

        for key in ["publictag", "privatetag", "invalidtag"] {
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from(key.as_bytes().to_vec()))
                .send()
                .await
                .unwrap();
        }

        let policy = bucket_policy_document(
            principal,
            "s3:GetObjectAcl",
            bucket_wildcard_resource(&bucket),
            Some(json!({
                "StringEquals": {
                    "s3:ExistingObjectTag/security": "public"
                }
            })),
        );
        client
            .put_bucket_policy()
            .bucket(&bucket)
            .policy(policy)
            .send()
            .await
            .unwrap();

        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("publictag")
            .tagging(tagging(vec![tag("security", "public"), tag("foo", "bar")]))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("privatetag")
            .tagging(tagging(vec![tag("security", "private")]))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key("invalidtag")
            .tagging(tagging(vec![tag("security1", "public")]))
            .send()
            .await
            .unwrap();

        alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key("publictag")
            .send()
            .await
            .unwrap();

        let get_object = alt_client
            .get_object()
            .bucket(&bucket)
            .key("publictag")
            .send()
            .await;
        assert_eq!(err_status(&get_object), 403);
        assert_s3_err_code(&get_object, "AccessDenied");

        for key in ["privatetag", "invalidtag"] {
            let result = alt_client
                .get_object_acl()
                .bucket(&bucket)
                .key(key)
                .send()
                .await;
            assert_eq!(err_status(&result), 403);
            assert_s3_err_code(&result, "AccessDenied");
        }

        cleanup(&bucket, &["publictag", "privatetag", "invalidtag"]).await;
    });
}
