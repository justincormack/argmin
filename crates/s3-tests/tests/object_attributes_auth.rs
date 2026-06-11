use std::future::Future;
use std::time::Duration;

use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, ObjectAttributes,
    ObjectOwnership, Tag, Tagging, VersioningConfiguration,
};
use s3_tests::{cleanup_versioned_bucket, unique_bucket, CTX};
use serde_json::json;

fn owner_root_client() -> &'static aws_sdk_s3::Client {
    CTX.require_owner_root_client()
}

async fn eventually_ok<T, E, F, Fut>(description: &str, mut op: F) -> T
where
    E: std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, aws_sdk_s3::error::SdkError<E>>>,
{
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        match op().await {
            Ok(output) => return output,
            Err(_) if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(err) => panic!("{description} failed unexpectedly: {err:?}"),
        }
    }

    unreachable!()
}

async fn eventually_err_status<T, E, F, Fut>(
    description: &str,
    expected_status: u16,
    expected_code: Option<&str>,
    mut op: F,
) where
    E: std::fmt::Debug + ProvideErrorMetadata,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, aws_sdk_s3::error::SdkError<E>>>,
{
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let result = op().await;
        match &result {
            Err(err)
                if err
                    .raw_response()
                    .map(|response| response.status().as_u16())
                    == Some(expected_status)
                    && expected_code.is_none_or(|code| {
                        err.as_service_error().and_then(ProvideErrorMetadata::code) == Some(code)
                    }) =>
            {
                return;
            }
            _ if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            _ => panic!(
                "{description} did not converge to HTTP {expected_status} ({expected_code:?})"
            ),
        }
    }

    unreachable!()
}

async fn create_boe_bucket(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket_request(client, &bucket)
        .object_ownership(ObjectOwnership::BucketOwnerEnforced)
        .send()
        .await
        .unwrap();
    bucket
}

async fn create_versioned_bucket(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
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
    bucket
}

fn bucket_resource(bucket: &str) -> String {
    format!("arn:aws:s3:::{bucket}")
}

fn object_resource(bucket: &str, key: &str) -> String {
    format!("arn:aws:s3:::{bucket}/{key}")
}

fn object_wildcard_resource(bucket: &str) -> String {
    format!("arn:aws:s3:::{bucket}/*")
}

fn alt_policy_principal() -> serde_json::Value {
    json!({ "AWS": format!("arn:aws:iam::{}:root", CTX.alt_account_id()) })
}

fn object_tagging(key: &str, value: &str) -> Tagging {
    Tagging::builder()
        .tag_set(Tag::builder().key(key).value(value).build().unwrap())
        .build()
        .unwrap()
}

async fn put_bucket_policy_json(bucket: &str, policy: serde_json::Value) {
    CTX.client()
        .put_bucket_policy()
        .bucket(bucket)
        .policy(policy.to_string())
        .send()
        .await
        .unwrap();
}

async fn multipart_upload_parts(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    parts: &[Vec<u8>],
) -> (String, CompletedMultipartUpload) {
    let create = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().expect("expected upload id").to_string();

    let mut completed_parts = Vec::new();
    for (index, body) in parts.iter().enumerate() {
        let part_number = (index + 1) as i32;
        let uploaded = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(part_number)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
            .unwrap();
        completed_parts.push(
            CompletedPart::builder()
                .part_number(part_number)
                .e_tag(uploaded.e_tag().expect("expected part etag"))
                .build(),
        );
    }

    (
        upload_id,
        CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build(),
    )
}

async fn cleanup_bucket(bucket: &str, keys: &[&str]) {
    let client = CTX.client();
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(*key).send().await;
    }
    s3_tests::delete_bucket_retrying_operation_aborted(client, bucket).await;
}

#[test]
fn test_get_object_attributes_is_not_head_object_and_list_parts_intersection_for_boe_root() {
    s3_tests::run(async {
        let root_client = owner_root_client();
        let client = CTX.client();
        let bucket = create_boe_bucket(client).await;
        let key = "multipart";
        let part1 = vec![b'a'; 5 * 1024 * 1024];
        let part2 = vec![b'b'; 1024];
        let (upload_id, completed) =
            multipart_upload_parts(client, &bucket, key, &[part1.clone(), part2.clone()]).await;

        let root_parts = eventually_ok("root ListParts on BOE multipart upload", || {
            root_client
                .list_parts()
                .bucket(&bucket)
                .key(key)
                .upload_id(&upload_id)
                .send()
        })
        .await;
        assert_eq!(root_parts.parts().len(), 2);
        let owner_parts = eventually_ok("owner ListParts on BOE multipart upload", || {
            client
                .list_parts()
                .bucket(&bucket)
                .key(key)
                .upload_id(&upload_id)
                .send()
        })
        .await;
        assert_eq!(owner_parts.parts().len(), 2);

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .unwrap();

        eventually_ok("root HeadObject on completed BOE multipart object", || {
            root_client.head_object().bucket(&bucket).key(key).send()
        })
        .await;
        eventually_err_status(
            "root GetObjectAttributes on completed BOE multipart object",
            403,
            Some("AccessDenied"),
            || {
                root_client
                    .get_object_attributes()
                    .bucket(&bucket)
                    .key(key)
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .object_attributes(ObjectAttributes::ObjectParts)
                    .send()
            },
        )
        .await;

        eventually_ok("owner HeadObject on completed BOE multipart object", || {
            client.head_object().bucket(&bucket).key(key).send()
        })
        .await;
        let attrs = eventually_ok(
            "owner GetObjectAttributes on completed BOE multipart object",
            || {
                client
                    .get_object_attributes()
                    .bucket(&bucket)
                    .key(key)
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .object_attributes(ObjectAttributes::ObjectParts)
                    .send()
            },
        )
        .await;
        assert_eq!(
            attrs.object_size(),
            Some((part1.len() + part2.len()) as i64)
        );
        assert_eq!(
            attrs
                .object_parts()
                .and_then(|parts| parts.total_parts_count()),
            Some(2)
        );

        cleanup_bucket(&bucket, &[key]).await;
    });
}

#[test]
fn test_get_object_attributes_bucket_policy_requires_get_object_too() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "policy-object";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": "s3:GetObjectAttributes",
                    "Resource": object_resource(&bucket, key),
                }]
            }),
        )
        .await;
        eventually_err_status(
            "alt GetObjectAttributes with only GetObjectAttributes bucket policy",
            403,
            Some("AccessDenied"),
            || {
                alt.get_object_attributes()
                    .bucket(&bucket)
                    .key(key)
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .send()
            },
        )
        .await;

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": ["s3:GetObject", "s3:GetObjectAttributes"],
                    "Resource": object_resource(&bucket, key),
                }]
            }),
        )
        .await;
        let attrs = eventually_ok(
            "alt GetObjectAttributes with GetObject and GetObjectAttributes bucket policy",
            || {
                alt.get_object_attributes()
                    .bucket(&bucket)
                    .key(key)
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .send()
            },
        )
        .await;
        assert_eq!(attrs.object_size(), Some(4));

        cleanup_bucket(&bucket, &[key]).await;
    });
}

#[test]
fn test_get_object_attributes_bucket_policy_requires_get_object_version_too() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt = CTX.alt_client();
        let bucket = create_versioned_bucket(client).await;
        let key = "policy-versioned-object";
        let version_id = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"versioned"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected version id")
            .to_string();

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": "s3:GetObjectVersionAttributes",
                    "Resource": object_resource(&bucket, key),
                }]
            }),
        )
        .await;
        eventually_err_status(
            "alt GetObjectAttributes version with only GetObjectVersionAttributes bucket policy",
            403,
            Some("AccessDenied"),
            || {
                alt.get_object_attributes()
                    .bucket(&bucket)
                    .key(key)
                    .version_id(&version_id)
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .send()
            },
        )
        .await;

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": ["s3:GetObjectVersion", "s3:GetObjectVersionAttributes"],
                    "Resource": object_resource(&bucket, key),
                }]
            }),
        )
        .await;
        let attrs = eventually_ok(
            "alt GetObjectAttributes version with GetObjectVersion and GetObjectVersionAttributes bucket policy",
            || {
                alt.get_object_attributes()
                    .bucket(&bucket)
                    .key(key)
                    .version_id(&version_id)
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .send()
            },
        )
        .await;
        assert_eq!(attrs.object_size(), Some(9));

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_get_object_attributes_bucket_policy_existing_tag_condition_does_not_authorize() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "policy-tagged-object";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(object_tagging("security", "public"))
            .send()
            .await
            .unwrap();

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": ["s3:GetObject", "s3:GetObjectAttributes"],
                    "Resource": object_resource(&bucket, key),
                    "Condition": {
                        "StringEquals": {
                            "s3:ExistingObjectTag/security": "public"
                        }
                    }
                }]
            }),
        )
        .await;

        eventually_ok(
            "alt GetObject with ExistingObjectTag bucket policy on tagged object",
            || alt.get_object().bucket(&bucket).key(key).send(),
        )
        .await;

        eventually_err_status(
            "alt GetObjectAttributes with ExistingObjectTag bucket policy on tagged object",
            403,
            Some("AccessDenied"),
            || {
                alt.get_object_attributes()
                    .bucket(&bucket)
                    .key(key)
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .send()
            },
        )
        .await;

        cleanup_bucket(&bucket, &[key]).await;
    });
}

#[test]
fn test_get_object_version_attributes_bucket_policy_existing_tag_condition_does_not_authorize() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt = CTX.alt_client();
        let bucket = create_versioned_bucket(client).await;
        let key = "policy-versioned-tagged-object";
        let version_id = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"versioned-tagged"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected version id")
            .to_string();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .tagging(object_tagging("security", "public"))
            .send()
            .await
            .unwrap();

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": ["s3:GetObjectVersion", "s3:GetObjectVersionAttributes"],
                    "Resource": object_resource(&bucket, key),
                    "Condition": {
                        "StringEquals": {
                            "s3:ExistingObjectTag/security": "public"
                        }
                    }
                }]
            }),
        )
        .await;

        eventually_ok(
            "alt GetObject version with ExistingObjectTag bucket policy on tagged version",
            || {
                alt.get_object()
                    .bucket(&bucket)
                    .key(key)
                    .version_id(&version_id)
                    .send()
            },
        )
        .await;

        eventually_err_status(
            "alt GetObjectAttributes version with ExistingObjectTag bucket policy on tagged version",
            403,
            Some("AccessDenied"),
            || {
                alt.get_object_attributes()
                    .bucket(&bucket)
                    .key(key)
                    .version_id(&version_id)
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_get_object_bucket_policy_existing_tag_condition_authorizes() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "policy-tagged-get-object";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(object_tagging("security", "public"))
            .send()
            .await
            .unwrap();

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, key),
                    "Condition": {
                        "StringEquals": {
                            "s3:ExistingObjectTag/security": "public"
                        }
                    }
                }]
            }),
        )
        .await;

        eventually_ok(
            "alt GetObject with ExistingObjectTag bucket policy on tagged object",
            || alt.get_object().bucket(&bucket).key(key).send(),
        )
        .await;

        cleanup_bucket(&bucket, &[key]).await;
    });
}

#[test]
fn test_get_object_version_bucket_policy_existing_tag_condition_authorizes() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt = CTX.alt_client();
        let bucket = create_versioned_bucket(client).await;
        let key = "policy-versioned-tagged-get-object";
        let version_id = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"versioned-tagged"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected version id")
            .to_string();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .tagging(object_tagging("security", "public"))
            .send()
            .await
            .unwrap();

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": "s3:GetObjectVersion",
                    "Resource": object_resource(&bucket, key),
                    "Condition": {
                        "StringEquals": {
                            "s3:ExistingObjectTag/security": "public"
                        }
                    }
                }]
            }),
        )
        .await;

        eventually_ok(
            "alt GetObject version with ExistingObjectTag bucket policy on tagged version",
            || {
                alt.get_object()
                    .bucket(&bucket)
                    .key(key)
                    .version_id(&version_id)
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_head_object_bucket_policy_existing_tag_condition_authorizes() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "policy-tagged-head-object";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(object_tagging("security", "public"))
            .send()
            .await
            .unwrap();

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": "s3:GetObject",
                    "Resource": object_resource(&bucket, key),
                    "Condition": {
                        "StringEquals": {
                            "s3:ExistingObjectTag/security": "public"
                        }
                    }
                }]
            }),
        )
        .await;

        eventually_ok(
            "alt HeadObject with ExistingObjectTag bucket policy on tagged object",
            || alt.head_object().bucket(&bucket).key(key).send(),
        )
        .await;

        cleanup_bucket(&bucket, &[key]).await;
    });
}

#[test]
fn test_head_object_version_bucket_policy_existing_tag_condition_authorizes() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt = CTX.alt_client();
        let bucket = create_versioned_bucket(client).await;
        let key = "policy-versioned-tagged-head-object";
        let version_id = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"versioned-tagged"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected version id")
            .to_string();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .tagging(object_tagging("security", "public"))
            .send()
            .await
            .unwrap();

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": "s3:GetObjectVersion",
                    "Resource": object_resource(&bucket, key),
                    "Condition": {
                        "StringEquals": {
                            "s3:ExistingObjectTag/security": "public"
                        }
                    }
                }]
            }),
        )
        .await;

        eventually_ok(
            "alt HeadObject version with ExistingObjectTag bucket policy on tagged version",
            || {
                alt.head_object()
                    .bucket(&bucket)
                    .key(key)
                    .version_id(&version_id)
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_get_object_attributes_existing_tag_condition_still_denies_with_tag_read_access() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "policy-tagged-object-with-tag-read";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(object_tagging("security", "public"))
            .send()
            .await
            .unwrap();

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:GetObjectTagging",
                        "Resource": object_resource(&bucket, key),
                    },
                    {
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": ["s3:GetObject", "s3:GetObjectAttributes"],
                        "Resource": object_resource(&bucket, key),
                        "Condition": {
                            "StringEquals": {
                                "s3:ExistingObjectTag/security": "public"
                            }
                        }
                    }
                ]
            }),
        )
        .await;

        let tagging = eventually_ok(
            "alt GetObjectTagging with explicit tag-read policy on tagged object",
            || alt.get_object_tagging().bucket(&bucket).key(key).send(),
        )
        .await;
        assert_eq!(
            tagging
                .tag_set()
                .iter()
                .map(|tag| (tag.key(), tag.value()))
                .collect::<Vec<_>>(),
            vec![("security", "public")]
        );

        eventually_ok(
            "alt GetObject with ExistingObjectTag bucket policy and tag-read access",
            || alt.get_object().bucket(&bucket).key(key).send(),
        )
        .await;

        eventually_err_status(
            "alt GetObjectAttributes with ExistingObjectTag bucket policy and tag-read access",
            403,
            Some("AccessDenied"),
            || {
                alt.get_object_attributes()
                    .bucket(&bucket)
                    .key(key)
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .send()
            },
        )
        .await;

        cleanup_bucket(&bucket, &[key]).await;
    });
}

#[test]
fn test_get_object_existing_tag_condition_still_authorizes_with_tag_read_access() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "policy-tagged-get-object-with-tag-read";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(object_tagging("security", "public"))
            .send()
            .await
            .unwrap();

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:GetObjectTagging",
                        "Resource": object_resource(&bucket, key),
                    },
                    {
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:GetObject",
                        "Resource": object_resource(&bucket, key),
                        "Condition": {
                            "StringEquals": {
                                "s3:ExistingObjectTag/security": "public"
                            }
                        }
                    }
                ]
            }),
        )
        .await;

        let tagging = eventually_ok(
            "alt GetObjectTagging with explicit tag-read policy on tagged object for GetObject",
            || alt.get_object_tagging().bucket(&bucket).key(key).send(),
        )
        .await;
        assert_eq!(
            tagging
                .tag_set()
                .iter()
                .map(|tag| (tag.key(), tag.value()))
                .collect::<Vec<_>>(),
            vec![("security", "public")]
        );

        eventually_ok(
            "alt GetObject with ExistingObjectTag bucket policy and tag-read access",
            || alt.get_object().bucket(&bucket).key(key).send(),
        )
        .await;

        cleanup_bucket(&bucket, &[key]).await;
    });
}

#[test]
fn test_head_object_existing_tag_condition_still_authorizes_with_tag_read_access() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt = CTX.alt_client();
        let bucket = unique_bucket();
        let key = "policy-tagged-head-object-with-tag-read";
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .tagging(object_tagging("security", "public"))
            .send()
            .await
            .unwrap();

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:GetObjectTagging",
                        "Resource": object_resource(&bucket, key),
                    },
                    {
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:GetObject",
                        "Resource": object_resource(&bucket, key),
                        "Condition": {
                            "StringEquals": {
                                "s3:ExistingObjectTag/security": "public"
                            }
                        }
                    }
                ]
            }),
        )
        .await;

        let tagging = eventually_ok(
            "alt GetObjectTagging with explicit tag-read policy on tagged object for HeadObject",
            || alt.get_object_tagging().bucket(&bucket).key(key).send(),
        )
        .await;
        assert_eq!(
            tagging
                .tag_set()
                .iter()
                .map(|tag| (tag.key(), tag.value()))
                .collect::<Vec<_>>(),
            vec![("security", "public")]
        );

        eventually_ok(
            "alt HeadObject with ExistingObjectTag bucket policy and tag-read access",
            || alt.head_object().bucket(&bucket).key(key).send(),
        )
        .await;

        cleanup_bucket(&bucket, &[key]).await;
    });
}

#[test]
fn test_get_object_version_existing_tag_condition_still_authorizes_with_tag_read_access() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt = CTX.alt_client();
        let bucket = create_versioned_bucket(client).await;
        let key = "policy-versioned-tagged-get-object-with-tag-read";
        let version_id = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"versioned-tagged"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected version id")
            .to_string();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .tagging(object_tagging("security", "public"))
            .send()
            .await
            .unwrap();

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:GetObjectVersionTagging",
                        "Resource": object_resource(&bucket, key),
                    },
                    {
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:GetObjectVersion",
                        "Resource": object_resource(&bucket, key),
                        "Condition": {
                            "StringEquals": {
                                "s3:ExistingObjectTag/security": "public"
                            }
                        }
                    }
                ]
            }),
        )
        .await;

        let tagging = eventually_ok(
            "alt GetObjectTagging version with explicit tag-read policy on tagged version for GetObject",
            || {
                alt.get_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .version_id(&version_id)
                    .send()
            },
        )
        .await;
        assert_eq!(
            tagging
                .tag_set()
                .iter()
                .map(|tag| (tag.key(), tag.value()))
                .collect::<Vec<_>>(),
            vec![("security", "public")]
        );

        eventually_ok(
            "alt GetObject version with ExistingObjectTag bucket policy and tag-read access",
            || {
                alt.get_object()
                    .bucket(&bucket)
                    .key(key)
                    .version_id(&version_id)
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_head_object_version_existing_tag_condition_still_authorizes_with_tag_read_access() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt = CTX.alt_client();
        let bucket = create_versioned_bucket(client).await;
        let key = "policy-versioned-tagged-head-object-with-tag-read";
        let version_id = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"versioned-tagged"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected version id")
            .to_string();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .tagging(object_tagging("security", "public"))
            .send()
            .await
            .unwrap();

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:GetObjectVersionTagging",
                        "Resource": object_resource(&bucket, key),
                    },
                    {
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:GetObjectVersion",
                        "Resource": object_resource(&bucket, key),
                        "Condition": {
                            "StringEquals": {
                                "s3:ExistingObjectTag/security": "public"
                            }
                        }
                    }
                ]
            }),
        )
        .await;

        let tagging = eventually_ok(
            "alt GetObjectTagging version with explicit tag-read policy on tagged version for HeadObject",
            || {
                alt.get_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .version_id(&version_id)
                    .send()
            },
        )
        .await;
        assert_eq!(
            tagging
                .tag_set()
                .iter()
                .map(|tag| (tag.key(), tag.value()))
                .collect::<Vec<_>>(),
            vec![("security", "public")]
        );

        eventually_ok(
            "alt HeadObject version with ExistingObjectTag bucket policy and tag-read access",
            || {
                alt.head_object()
                    .bucket(&bucket)
                    .key(key)
                    .version_id(&version_id)
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_get_object_version_attributes_existing_tag_condition_still_denies_with_tag_read_access() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt = CTX.alt_client();
        let bucket = create_versioned_bucket(client).await;
        let key = "policy-versioned-tagged-object-with-tag-read";
        let version_id = client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"versioned-tagged"))
            .send()
            .await
            .unwrap()
            .version_id()
            .expect("expected version id")
            .to_string();
        client
            .put_object_tagging()
            .bucket(&bucket)
            .key(key)
            .version_id(&version_id)
            .tagging(object_tagging("security", "public"))
            .send()
            .await
            .unwrap();

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:GetObjectVersionTagging",
                        "Resource": object_resource(&bucket, key),
                    },
                    {
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": ["s3:GetObjectVersion", "s3:GetObjectVersionAttributes"],
                        "Resource": object_resource(&bucket, key),
                        "Condition": {
                            "StringEquals": {
                                "s3:ExistingObjectTag/security": "public"
                            }
                        }
                    }
                ]
            }),
        )
        .await;

        let tagging = eventually_ok(
            "alt GetObjectTagging version with explicit tag-read policy on tagged version",
            || {
                alt.get_object_tagging()
                    .bucket(&bucket)
                    .key(key)
                    .version_id(&version_id)
                    .send()
            },
        )
        .await;
        assert_eq!(
            tagging
                .tag_set()
                .iter()
                .map(|tag| (tag.key(), tag.value()))
                .collect::<Vec<_>>(),
            vec![("security", "public")]
        );

        eventually_ok(
            "alt GetObject version with ExistingObjectTag bucket policy and tag-read access",
            || {
                alt.get_object()
                    .bucket(&bucket)
                    .key(key)
                    .version_id(&version_id)
                    .send()
            },
        )
        .await;

        eventually_err_status(
            "alt GetObjectAttributes version with ExistingObjectTag bucket policy and tag-read access",
            403,
            Some("AccessDenied"),
            || {
                alt.get_object_attributes()
                    .bucket(&bucket)
                    .key(key)
                    .version_id(&version_id)
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .send()
            },
        )
        .await;

        cleanup_versioned_bucket(client, &bucket).await;
    });
}

#[test]
fn test_get_object_attributes_missing_key_uses_list_bucket_policy_for_404_vs_403() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt = CTX.alt_client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": alt_policy_principal(),
                    "Action": ["s3:GetObject", "s3:GetObjectAttributes"],
                    "Resource": object_wildcard_resource(&bucket),
                }]
            }),
        )
        .await;
        eventually_err_status(
            "alt GetObjectAttributes missing key without ListBucket policy",
            403,
            Some("AccessDenied"),
            || {
                alt.get_object_attributes()
                    .bucket(&bucket)
                    .key("missing")
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .send()
            },
        )
        .await;

        put_bucket_policy_json(
            &bucket,
            json!({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": ["s3:GetObject", "s3:GetObjectAttributes"],
                        "Resource": object_wildcard_resource(&bucket),
                    },
                    {
                        "Effect": "Allow",
                        "Principal": alt_policy_principal(),
                        "Action": "s3:ListBucket",
                        "Resource": bucket_resource(&bucket),
                    }
                ]
            }),
        )
        .await;
        eventually_err_status(
            "alt GetObjectAttributes missing key with ListBucket policy",
            404,
            Some("NoSuchKey"),
            || {
                alt.get_object_attributes()
                    .bucket(&bucket)
                    .key("missing")
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .send()
            },
        )
        .await;

        cleanup_bucket(&bucket, &[]).await;
    });
}
