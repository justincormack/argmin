use std::time::{Duration, Instant};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketCannedAcl, BucketLocationConstraint, CreateBucketConfiguration, ObjectOwnership,
    Permission, VersioningConfiguration,
};
use s3_tests::{
    assert_s3_err_code, cleanup_versioned_bucket, delete_all_and_bucket,
    disable_bucket_public_access_block, err_status, unique_bucket, CTX,
};

const ALL_USERS_GROUP_URI: &str = "http://acs.amazonaws.com/groups/global/AllUsers";

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

async fn recreate_bucket_after_delete(client: &aws_sdk_s3::Client, bucket: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let result = s3_tests::create_bucket_request(client, bucket).send().await;
        match result {
            Ok(_) => return,
            Err(err) => {
                let debug = format!("{err:?}");
                if !debug.contains("BucketAlreadyExists") {
                    panic!("expected BucketAlreadyExists while waiting to recreate bucket, got {debug}");
                }
                if Instant::now() >= deadline {
                    panic!("bucket {bucket} was not reusable within 5s after delete: {debug}");
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn create_bucket_in_test_region(client: &aws_sdk_s3::Client, bucket: &str) {
    let mut request = client.create_bucket().bucket(bucket);
    if CTX.region() != "us-east-1" {
        let config = CreateBucketConfiguration::builder()
            .location_constraint(BucketLocationConstraint::from(CTX.region()))
            .build();
        request = request.create_bucket_configuration(config);
    }
    request.send().await.unwrap();
}

fn expected_bucket_location_constraint_for_sdk(region: &str) -> Option<&str> {
    match region {
        // The SDK models the legacy us-east-1 null as an empty string.
        "us-east-1" => Some(""),
        "eu-west-1" => Some("EU"),
        other => Some(other),
    }
}

async fn canonical_owner_id(client: &aws_sdk_s3::Client) -> String {
    let bucket = unique_bucket();
    create_bucket_in_test_region(client, &bucket).await;
    let owner_id = client
        .get_bucket_acl()
        .bucket(&bucket)
        .send()
        .await
        .unwrap()
        .owner()
        .and_then(|owner| owner.id())
        .expect("expected owner ID in GetBucketAcl")
        .to_string();
    client.delete_bucket().bucket(&bucket).send().await.unwrap();
    owner_id
}

fn has_canonical_user_grant(
    grants: &[aws_sdk_s3::types::Grant],
    canonical_user_id: &str,
    permission: Permission,
) -> bool {
    grants.iter().any(|grant| {
        grant.permission() == Some(&permission)
            && grant
                .grantee()
                .is_some_and(|grantee| grantee.id() == Some(canonical_user_id))
    })
}

fn has_group_grant(grants: &[aws_sdk_s3::types::Grant], uri: &str, permission: Permission) -> bool {
    grants.iter().any(|grant| {
        grant.permission() == Some(&permission)
            && grant
                .grantee()
                .is_some_and(|grantee| grantee.uri() == Some(uri))
    })
}

async fn create_acl_enabled_bucket(client: &aws_sdk_s3::Client, bucket: &str) {
    s3_tests::create_bucket_request(client, bucket)
        .object_ownership(ObjectOwnership::ObjectWriter)
        .send()
        .await
        .unwrap();
    disable_bucket_public_access_block(client, bucket).await;
}

// ── CreateBucket ─────────────────────────────────────────────────────

#[test]
fn test_bucket_create_delete() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();
        // Clean up
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_exists() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        // Verify bucket exists via HEAD
        client.head_bucket().bucket(&bucket).send().await.unwrap();

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
        let result = request.send().await;
        if CTX.region() == "us-east-1" {
            result.unwrap();
        } else {
            assert_eq!(err_status(&result), 409);
            assert_s3_err_code(&result, "BucketAlreadyOwnedByYou");
        }

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
            client
                .put_object()
                .bucket(&bucket)
                .key(key)
                .body(ByteStream::from_static(b"data"))
                .send()
                .await
                .unwrap();
        }

        let mut request = client.create_bucket().bucket(&bucket);
        if CTX.region() != "us-east-1" {
            let config = CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::from(CTX.region()))
                .build();
            request = request.create_bucket_configuration(config);
        }
        let result = request.send().await;
        if CTX.region() == "us-east-1" {
            result.unwrap();
        } else {
            assert_eq!(err_status(&result), 409);
            assert_s3_err_code(&result, "BucketAlreadyOwnedByYou");
        }

        let listed = client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
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
fn test_bucket_recreate_overwrite_acl() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();

        create_acl_enabled_bucket(client, &bucket).await;
        client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(BucketCannedAcl::PublicRead)
            .send()
            .await
            .unwrap();

        let mut request = client.create_bucket().bucket(&bucket);
        if CTX.region() != "us-east-1" {
            let config = CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::from(CTX.region()))
                .build();
            request = request.create_bucket_configuration(config);
        }
        let result = request.send().await;

        let acl = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetBucketAcl");
        if CTX.region() == "us-east-1" {
            result.unwrap();
            assert_eq!(acl.grants().len(), 1);
            assert!(has_canonical_user_grant(
                acl.grants(),
                owner_id,
                Permission::FullControl
            ));
            assert!(!has_group_grant(
                acl.grants(),
                ALL_USERS_GROUP_URI,
                Permission::Read
            ));
        } else {
            assert_eq!(err_status(&result), 409);
            assert_s3_err_code(&result, "BucketAlreadyOwnedByYou");
            assert_eq!(acl.grants().len(), 2);
            assert!(has_group_grant(
                acl.grants(),
                ALL_USERS_GROUP_URI,
                Permission::Read
            ));
        }

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_recreate_new_acl() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();

        create_acl_enabled_bucket(client, &bucket).await;

        let mut request = client
            .create_bucket()
            .bucket(&bucket)
            .acl(BucketCannedAcl::PublicRead)
            .object_ownership(ObjectOwnership::ObjectWriter);
        if CTX.region() != "us-east-1" {
            let config = CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::from(CTX.region()))
                .build();
            request = request.create_bucket_configuration(config);
        }
        let result = request.send().await;

        let acl = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetBucketAcl");
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidBucketAclWithBlockPublicAccessError");
        assert_eq!(acl.grants().len(), 1);
        assert!(has_canonical_user_grant(
            acl.grants(),
            owner_id,
            Permission::FullControl
        ));
        assert!(!has_group_grant(
            acl.grants(),
            ALL_USERS_GROUP_URI,
            Permission::Read
        ));

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_create_public_read_acl_rejected() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();

        let mut request = client
            .create_bucket()
            .bucket(&bucket)
            .acl(BucketCannedAcl::PublicRead)
            .object_ownership(ObjectOwnership::ObjectWriter);
        if CTX.region() != "us-east-1" {
            let config = CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::from(CTX.region()))
                .build();
            request = request.create_bucket_configuration(config);
        }

        let result = request.send().await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidBucketAclWithBlockPublicAccessError");
    });
}

#[test]
fn test_bucket_recreate_new_header_grants() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();

        create_acl_enabled_bucket(client, &bucket).await;
        let owner_id = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap()
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetBucketAcl")
            .to_string();
        let alt_owner_id = canonical_owner_id(alt_client).await;

        let mut request = client
            .create_bucket()
            .bucket(&bucket)
            .object_ownership(ObjectOwnership::ObjectWriter);
        if CTX.region() != "us-east-1" {
            let config = CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::from(CTX.region()))
                .build();
            request = request.create_bucket_configuration(config);
        }
        let result = request
            .customize()
            .mutate_request({
                let owner_id = owner_id.clone();
                let alt_owner_id = alt_owner_id.clone();
                move |req| {
                    let headers = req.headers_mut();
                    headers.insert("x-amz-grant-full-control", format!("id={owner_id}"));
                    headers.insert("x-amz-grant-read", format!("id={alt_owner_id}"));
                }
            })
            .send()
            .await;

        let acl = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        if CTX.region() == "us-east-1" {
            result.unwrap();
            assert_eq!(acl.grants().len(), 2);
            assert!(has_canonical_user_grant(
                acl.grants(),
                &owner_id,
                Permission::FullControl
            ));
            assert!(has_canonical_user_grant(
                acl.grants(),
                &alt_owner_id,
                Permission::Read
            ));
        } else {
            assert_eq!(err_status(&result), 409);
            assert_s3_err_code(&result, "BucketAlreadyOwnedByYou");
            assert_eq!(acl.grants().len(), 1);
            assert!(has_canonical_user_grant(
                acl.grants(),
                &owner_id,
                Permission::FullControl
            ));
            assert!(!has_canonical_user_grant(
                acl.grants(),
                &alt_owner_id,
                Permission::Read
            ));
        }

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── DeleteBucket ─────────────────────────────────────────────────────

#[test]
fn test_bucket_delete_notexist() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let result = client.delete_bucket().bucket(&bucket).send().await;
        assert_eq!(err_status(&result), 404);
    });
}

#[test]
fn test_bucket_delete_nonempty() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("key")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        // Delete bucket should fail (not empty)
        let result = client.delete_bucket().bucket(&bucket).send().await;
        assert_eq!(err_status(&result), 409);

        // Clean up
        client
            .delete_object()
            .bucket(&bucket)
            .key("key")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
            .send()
            .await
            .unwrap();

        // Put an object, then delete it (creates a delete marker)
        client
            .put_object()
            .bucket(&bucket)
            .key("key")
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
        client
            .delete_object()
            .bucket(&bucket)
            .key("key")
            .send()
            .await
            .unwrap();

        // Bucket still has versions + delete marker; delete must fail
        let result = client.delete_bucket().bucket(&bucket).send().await;
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
        client.delete_bucket().bucket(&bucket).send().await.unwrap();

        // AWS documents that bucket removal can take time to finish, and
        // immediate same-name recreate may transiently return BucketAlreadyExists.
        recreate_bucket_after_delete(client, &bucket).await;
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

// ── HeadBucket ───────────────────────────────────────────────────────

#[test]
fn test_bucket_head() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        client.head_bucket().bucket(&bucket).send().await.unwrap();

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_get_location() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        create_bucket_in_test_region(client, &bucket).await;

        let output = client
            .get_bucket_location()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(
            output.location_constraint().map(|value| value.as_str()),
            expected_bucket_location_constraint_for_sdk(CTX.region())
        );

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_head_expected_owner() {
    s3_tests::run(async {
        let account_id = CTX.account_id().to_string();
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        client
            .head_bucket()
            .bucket(&bucket)
            .customize()
            .mutate_request({
                let account_id = account_id.clone();
                move |req| {
                    req.headers_mut()
                        .insert("x-amz-expected-bucket-owner", account_id.clone());
                }
            })
            .send()
            .await
            .unwrap();

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_head_wrong_expected_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let result = client
            .head_bucket()
            .bucket(&bucket)
            .customize()
            .mutate_request(|req| {
                req.headers_mut()
                    .insert("x-amz-expected-bucket-owner", "000000000000");
            })
            .send()
            .await;
        assert_eq!(err_status(&result), 403);

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_head_notexist() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let result = client.head_bucket().bucket(&bucket).send().await;
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
        let _resp = client.list_buckets().send().await.unwrap();
    });
}

#[test]
fn test_buckets_list_contains_created() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let resp = client.list_buckets().send().await.unwrap();
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

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
            .send()
            .await
            .unwrap();
        assert_eq!(resp.key_count(), Some(0));
        assert!(resp.contents().is_empty());

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
            client
                .put_object()
                .bucket(&bucket)
                .key(&key)
                .body(ByteStream::from_static(b"content"))
                .send()
                .await
                .unwrap();
        }

        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.key_count(), Some(3));

        let keys: Vec<&str> = resp.contents().iter().filter_map(|o| o.key()).collect();
        assert_eq!(keys, vec!["key0", "key1", "key2"]);

        // Clean up
        for i in 0..3 {
            client
                .delete_object()
                .bucket(&bucket)
                .key(format!("key{}", i))
                .send()
                .await
                .unwrap();
        }
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_list_objects_nonexistent_bucket() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        let result = client.list_objects_v2().bucket(&bucket).send().await;
        assert_eq!(err_status(&result), 404);
        assert_s3_err_code(&result, "NoSuchBucket");
    });
}

#[test]
fn test_bucket_list_objects_nonexistent_bucket_alt_client() {
    s3_tests::run(async {
        let alt_client = CTX.alt_client();
        let bucket = unique_bucket();
        let result = alt_client.list_objects_v2().bucket(&bucket).send().await;
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
        client.head_bucket().bucket(&bucket).send().await.unwrap();

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
            client
                .put_object()
                .bucket(&bucket)
                .key(*key)
                .body(ByteStream::from_static(b"data"))
                .send()
                .await
                .unwrap();
        }

        // Verify they all exist
        let resp = client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.key_count(), Some(special_keys.len() as i32));

        // Clean up
        for key in special_keys {
            client
                .delete_object()
                .bucket(&bucket)
                .key(*key)
                .send()
                .await
                .unwrap();
        }
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_buckets_list_ctime() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket(client, &bucket).await.unwrap();

        let resp = client.list_buckets().send().await.unwrap();
        let found = resp
            .buckets()
            .iter()
            .find(|b| b.name() == Some(bucket.as_str()));
        assert!(found.is_some(), "bucket should be in listing");
        assert!(
            found.unwrap().creation_date().is_some(),
            "bucket should have creation date"
        );

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
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
            .send()
            .await;
        assert_eq!(err_status(&result), 409);
        assert_s3_err_code(&result, "BucketAlreadyExists");

        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}

#[test]
fn test_bucket_header_acl_grants() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let alt_owner_id = canonical_owner_id(alt_client).await;
        assert_canonical_owner_id(&alt_owner_id);

        let bucket = unique_bucket();
        s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::ObjectWriter)
            .customize()
            .mutate_request({
                let alt_owner_id = alt_owner_id.clone();
                move |req| {
                    let headers = req.headers_mut();
                    headers.insert("x-amz-grant-read", format!("id={alt_owner_id}"));
                    headers.insert("x-amz-grant-write", format!("id={alt_owner_id}"));
                    headers.insert("x-amz-grant-read-acp", format!("id={alt_owner_id}"));
                    headers.insert("x-amz-grant-write-acp", format!("id={alt_owner_id}"));
                    headers.insert("x-amz-grant-full-control", format!("id={alt_owner_id}"));
                }
            })
            .send()
            .await
            .unwrap();

        let acl = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetBucketAcl");
        assert_canonical_owner_id(owner_id);
        let grants = acl.grants();
        assert_eq!(
            grants.len(),
            5,
            "expected exact alternate-user grants without implicit owner FULL_CONTROL, got {grants:?}"
        );
        assert!(
            has_canonical_user_grant(grants, &alt_owner_id, Permission::Read),
            "expected READ grant for alternate owner in {grants:?}"
        );
        assert!(
            has_canonical_user_grant(grants, &alt_owner_id, Permission::Write),
            "expected WRITE grant for alternate owner in {grants:?}"
        );
        assert!(
            has_canonical_user_grant(grants, &alt_owner_id, Permission::ReadAcp),
            "expected READ_ACP grant for alternate owner in {grants:?}"
        );
        assert!(
            has_canonical_user_grant(grants, &alt_owner_id, Permission::WriteAcp),
            "expected WRITE_ACP grant for alternate owner in {grants:?}"
        );
        assert!(
            has_canonical_user_grant(grants, &alt_owner_id, Permission::FullControl),
            "expected FULL_CONTROL grant for alternate owner in {grants:?}"
        );
        assert!(
            !has_canonical_user_grant(grants, owner_id, Permission::FullControl),
            "did not expect implicit owner FULL_CONTROL grant in {grants:?}"
        );

        alt_client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        alt_client
            .put_object()
            .bucket(&bucket)
            .key("granted-key")
            .body(ByteStream::from_static(b"granted-write"))
            .send()
            .await
            .unwrap();

        alt_client
            .delete_object()
            .bucket(&bucket)
            .key("granted-key")
            .send()
            .await
            .unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    });
}
