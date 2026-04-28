use aws_sdk_s3::types::{
    AccessControlPolicy, BucketCannedAcl, Grant, Grantee, ObjectCannedAcl, ObjectOwnership, Owner,
    Permission, Type,
};
use s3_tests::{assert_s3_err_code, create_acl_enabled_bucket, err_status, unique_bucket, CTX};
use std::time::Duration;

const AUTHENTICATED_USERS_GROUP_URI: &str =
    "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";

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

fn assert_error_code(body: &str, code: &str) {
    let expected = format!("<Code>{code}</Code>");
    assert!(
        body.contains(&expected),
        "expected {expected} in body, got {body}"
    );
}

/// Build an agent that returns all HTTP responses (including 4xx/5xx) as Ok.
fn agent() -> s3_tests::Agent {
    s3_tests::test_agent()
}

fn anonymous_get_status(url: &str) -> u16 {
    let mut resp = agent().get(url).call().expect("transport error");
    let status = resp.status().as_u16();
    let _ = resp.body_mut().read_to_string();
    status
}

async fn anonymous_get_status_eventually(url: &str, expected_status: u16, description: &str) {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let status = anonymous_get_status(url);
        if status == expected_status {
            return;
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            continue;
        }
        panic!(
            "{description} did not converge to HTTP {expected_status} for {url}, last status {status}"
        );
    }

    unreachable!()
}

/// Cleanup helper.
async fn cleanup(bucket: &str) {
    let client = CTX.client();
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

async fn setup_acl_enabled_bucket() -> String {
    let bucket = create_acl_enabled_bucket(CTX.client(), ObjectOwnership::ObjectWriter).await;
    wait_for_bucket_ownership_controls(&bucket, ObjectOwnership::ObjectWriter).await;
    bucket
}

async fn bucket_owner_id(bucket: &str) -> String {
    CTX.client()
        .get_bucket_acl()
        .bucket(bucket)
        .send()
        .await
        .unwrap()
        .owner()
        .expect("expected owner in GetBucketAcl")
        .id()
        .expect("expected owner ID in GetBucketAcl")
        .to_string()
}

async fn object_owner_id(bucket: &str, key: &str) -> String {
    CTX.client()
        .get_object_acl()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap()
        .owner()
        .expect("expected owner in GetObjectAcl")
        .id()
        .expect("expected owner ID in GetObjectAcl")
        .to_string()
}

async fn alt_get_object_access_denied_eventually(bucket: &str, key: &str) {
    const MAX_ATTEMPTS: usize = 120;

    for attempt in 0..MAX_ATTEMPTS {
        let result = CTX
            .alt_client()
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;
        if result.is_err() && err_status(&result) == 403 && {
            assert_s3_err_code(&result, "AccessDenied");
            true
        } {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let confirm = CTX
                .alt_client()
                .get_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await;
            if confirm.is_err() && err_status(&confirm) == 403 && {
                assert_s3_err_code(&confirm, "AccessDenied");
                true
            } {
                return;
            }
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }
        panic!(
            "alternate GetObject did not converge to AccessDenied for {bucket}/{key}: {:?}",
            result
        );
    }

    unreachable!()
}

async fn alt_list_bucket_access_denied_eventually(bucket: &str) {
    const MAX_ATTEMPTS: usize = 120;

    for attempt in 0..MAX_ATTEMPTS {
        let result = CTX
            .alt_client()
            .list_objects_v2()
            .bucket(bucket)
            .send()
            .await;
        if result.is_err() && err_status(&result) == 403 && {
            assert_s3_err_code(&result, "AccessDenied");
            true
        } {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let confirm = CTX
                .alt_client()
                .list_objects_v2()
                .bucket(bucket)
                .send()
                .await;
            if confirm.is_err() && err_status(&confirm) == 403 && {
                assert_s3_err_code(&confirm, "AccessDenied");
                true
            } {
                return;
            }
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }
        panic!(
            "alternate ListObjectsV2 did not converge to AccessDenied for {bucket}: {:?}",
            result
        );
    }

    unreachable!()
}

async fn wait_for_ignore_public_acls(bucket: &str, expected: bool) {
    const MAX_ATTEMPTS: usize = 60;

    for attempt in 0..MAX_ATTEMPTS {
        let result = CTX
            .client()
            .get_public_access_block()
            .bucket(bucket)
            .send()
            .await;
        if let Ok(resp) = result {
            if resp
                .public_access_block_configuration()
                .and_then(|config| config.ignore_public_acls())
                == Some(expected)
            {
                return;
            }
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            continue;
        }
        panic!("IgnorePublicAcls={expected} did not converge for {bucket}");
    }
}

async fn wait_for_bucket_ownership_controls(bucket: &str, expected: ObjectOwnership) {
    const MAX_ATTEMPTS: usize = 20;

    for attempt in 0..MAX_ATTEMPTS {
        let result = CTX
            .client()
            .get_bucket_ownership_controls()
            .bucket(bucket)
            .send()
            .await;
        if let Ok(resp) = result {
            if resp.ownership_controls().map(|controls| {
                controls
                    .rules()
                    .iter()
                    .any(|rule| rule.object_ownership() == &expected)
            }) == Some(true)
            {
                return;
            }
        }
        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            continue;
        }
        panic!("ObjectOwnership={expected:?} did not converge for {bucket}");
    }
}

fn authenticated_users_group_grant(permission: Permission) -> Grant {
    Grant::builder()
        .grantee(
            Grantee::builder()
                .r#type(Type::Group)
                .uri(AUTHENTICATED_USERS_GROUP_URI)
                .build()
                .expect("authenticated users grantee"),
        )
        .permission(permission)
        .build()
}

fn access_control_policy(owner_id: &str, grants: Vec<Grant>) -> AccessControlPolicy {
    AccessControlPolicy::builder()
        .owner(Owner::builder().id(owner_id).build())
        .set_grants(Some(grants))
        .build()
}

#[test]
fn test_block_public_put_bucket_acls() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::BucketOwnerPreferred)
            .send()
            .await
            .unwrap();

        // Set BlockPublicAcls = true
        let pab = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .block_public_acls(true)
            .build();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();

        // Attempt PutBucketAcl with public-read -> should be denied
        let url = format!("{}/{}?acl", CTX.endpoint(), bucket);
        let resp = send_signed_put(&url, b"", &[("x-amz-acl", "public-read")]);
        assert_eq!(
            resp, 403,
            "expected 403 for public-read when BlockPublicAcls is set, got {}",
            resp
        );

        // public-read-write -> should also be denied
        let resp = send_signed_put(&url, b"", &[("x-amz-acl", "public-read-write")]);
        assert_eq!(
            resp, 403,
            "expected 403 for public-read-write when BlockPublicAcls is set, got {}",
            resp
        );

        // authenticated-read -> should also be denied
        let resp = send_signed_put(&url, b"", &[("x-amz-acl", "authenticated-read")]);
        assert_eq!(
            resp, 403,
            "expected 403 for authenticated-read when BlockPublicAcls is set, got {}",
            resp
        );

        // PutBucketAcl with private should succeed
        client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(BucketCannedAcl::Private)
            .send()
            .await
            .unwrap();

        cleanup(&bucket).await;
    });
}

#[test]
fn test_block_public_put_bucket_acl_authenticated_users_xml_grant() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_acl_enabled_bucket().await;
        let owner_id = bucket_owner_id(&bucket).await;

        let pab = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .block_public_acls(true)
            .build();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();

        let result = client
            .put_bucket_acl()
            .bucket(&bucket)
            .access_control_policy(access_control_policy(
                &owner_id,
                vec![
                    authenticated_users_group_grant(Permission::Read),
                    Grant::builder()
                        .grantee(
                            Grantee::builder()
                                .id(&owner_id)
                                .r#type(Type::CanonicalUser)
                                .build()
                                .expect("canonical grantee"),
                        )
                        .permission(Permission::FullControl)
                        .build(),
                ],
            ))
            .send()
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        cleanup(&bucket).await;
    });
}

#[test]
fn test_get_bucket_acl_public_read_write() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = s3_tests::create_public_write_bucket(client).await;

        let resp = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let owner = resp.owner().expect("expected owner in GetBucketAcl");
        let owner_id = owner.id().expect("expected owner ID in GetBucketAcl");
        assert_canonical_owner_id(owner_id);

        let grants = resp.grants();
        assert!(
            grants.iter().any(|grant| {
                grant.permission() == Some(&Permission::Read)
                    && grant.grantee().and_then(|g| g.uri())
                        == Some("http://acs.amazonaws.com/groups/global/AllUsers")
            }),
            "expected READ grant for AllUsers, got {:?}",
            grants
        );
        assert!(
            grants.iter().any(|grant| {
                grant.permission() == Some(&Permission::Write)
                    && grant.grantee().and_then(|g| g.uri())
                        == Some("http://acs.amazonaws.com/groups/global/AllUsers")
            }),
            "expected WRITE grant for AllUsers, got {:?}",
            grants
        );

        cleanup(&bucket).await;
    });
}

#[test]
fn test_ignore_public_acls() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = s3_tests::create_public_bucket(client).await;

        // Upload an object with public-read ACL (needed for object-level access on AWS)
        client
            .put_object()
            .bucket(&bucket)
            .key("key1")
            .acl(ObjectCannedAcl::PublicRead)
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abcde"))
            .send()
            .await
            .unwrap();

        // Verify anonymous user can list objects on public-read bucket
        let list_url = format!("{}/{bucket}/?list-type=2", CTX.endpoint());
        let mut list_resp = agent().get(&list_url).call().expect("transport error");
        assert_eq!(
            list_resp.status().as_u16(),
            200,
            "public-read bucket should allow anonymous list_objects"
        );
        let list_body = list_resp.body_mut().read_to_string().unwrap();
        assert!(
            list_body.contains("<Key>key1</Key>"),
            "list should contain key1"
        );

        // Verify anonymous user can GET object on public-read bucket
        let get_url = format!("{}/{bucket}/key1", CTX.endpoint());
        let mut get_resp = agent().get(&get_url).call().expect("transport error");
        assert_eq!(get_resp.status().as_u16(), 200);
        let data = get_resp.body_mut().read_to_vec().unwrap();
        assert_eq!(&data[..], b"abcde");

        // Set IgnorePublicAcls = true
        let pab = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .ignore_public_acls(true)
            .build();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();
        wait_for_ignore_public_acls(&bucket, true).await;

        // Re-apply public-read ACL (matching Ceph test: ACL still set, but ignored)
        client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(BucketCannedAcl::PublicRead)
            .send()
            .await
            .unwrap();

        // Anonymous list_objects should now fail (public ACL is ignored)
        anonymous_get_status_eventually(
            &list_url,
            403,
            "anonymous list_objects after IgnorePublicAcls",
        )
        .await;

        // Anonymous GET object should also fail
        anonymous_get_status_eventually(
            &get_url,
            403,
            "anonymous get_object after IgnorePublicAcls",
        )
        .await;

        // Authenticated owner access should still work
        let info = client.head_bucket().bucket(&bucket).send().await;
        assert!(info.is_ok(), "authenticated head_bucket should still work");

        let get_resp = client
            .get_object()
            .bucket(&bucket)
            .key("key1")
            .send()
            .await
            .unwrap();
        let body = get_resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(
            &body[..],
            b"abcde",
            "authenticated owner should still read object"
        );

        // Cleanup
        client
            .delete_object()
            .bucket(&bucket)
            .key("key1")
            .send()
            .await
            .unwrap();
        cleanup(&bucket).await;
    });
}

#[test]
fn test_ignore_public_acls_disables_authenticated_read_object_acl() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = setup_acl_enabled_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("key1")
            .acl(ObjectCannedAcl::AuthenticatedRead)
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abcde"))
            .send()
            .await
            .unwrap();

        let before = alt_client
            .get_object()
            .bucket(&bucket)
            .key("key1")
            .send()
            .await
            .unwrap();
        let before_body = before.body.collect().await.unwrap().into_bytes();
        assert_eq!(&before_body[..], b"abcde");

        let pab = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .ignore_public_acls(true)
            .build();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();
        wait_for_ignore_public_acls(&bucket, true).await;

        alt_get_object_access_denied_eventually(&bucket, "key1").await;

        client
            .delete_object()
            .bucket(&bucket)
            .key("key1")
            .send()
            .await
            .unwrap();
        cleanup(&bucket).await;
    });
}

#[test]
fn test_ignore_public_acls_disables_authenticated_read_bucket_acl() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = setup_acl_enabled_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("key1")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abcde"))
            .send()
            .await
            .unwrap();

        client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(BucketCannedAcl::AuthenticatedRead)
            .send()
            .await
            .unwrap();

        let before = alt_client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let before_keys: Vec<&str> = before
            .contents()
            .iter()
            .filter_map(|obj| obj.key())
            .collect();
        assert_eq!(before_keys, vec!["key1"]);

        let pab = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .ignore_public_acls(true)
            .build();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();
        wait_for_ignore_public_acls(&bucket, true).await;

        alt_list_bucket_access_denied_eventually(&bucket).await;

        client
            .delete_object()
            .bucket(&bucket)
            .key("key1")
            .send()
            .await
            .unwrap();
        cleanup(&bucket).await;
    });
}

#[test]
fn test_ignore_public_acls_disables_authenticated_read_put_object_acl() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = setup_acl_enabled_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("key1")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abcde"))
            .send()
            .await
            .unwrap();

        client
            .put_object_acl()
            .bucket(&bucket)
            .key("key1")
            .acl(ObjectCannedAcl::AuthenticatedRead)
            .send()
            .await
            .unwrap();

        let before = alt_client
            .get_object()
            .bucket(&bucket)
            .key("key1")
            .send()
            .await
            .unwrap();
        let before_body = before.body.collect().await.unwrap().into_bytes();
        assert_eq!(&before_body[..], b"abcde");

        let pab = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .ignore_public_acls(true)
            .build();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();
        wait_for_ignore_public_acls(&bucket, true).await;

        alt_get_object_access_denied_eventually(&bucket, "key1").await;

        client
            .delete_object()
            .bucket(&bucket)
            .key("key1")
            .send()
            .await
            .unwrap();
        cleanup(&bucket).await;
    });
}

#[test]
fn test_get_public_access_block_requires_owner() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = s3_tests::create_public_bucket(client).await;

        // Put a PAB config so there's something to GET
        // (create_public_bucket already disabled PAB, re-enable block_public_acls)
        let pab = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .block_public_acls(true)
            .build();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();

        // Anonymous GET of public access block should fail with 403
        let url = format!("{}/{}?publicAccessBlock", CTX.endpoint(), bucket);
        let mut resp = agent().get(&url).call().expect("transport error");
        let status = resp.status().as_u16();
        let _ = resp.body_mut().read_to_string();
        assert_eq!(
            status, 403,
            "anonymous GetBucketPublicAccessBlock on public bucket should be 403, got {}",
            status
        );

        // Authenticated owner GET should succeed
        let resp = client
            .get_public_access_block()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.public_access_block_configuration()
                .unwrap()
                .block_public_acls(),
            Some(true)
        );

        cleanup(&bucket).await;
    });
}

#[test]
fn test_block_public_object_canned_acls() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::BucketOwnerPreferred)
            .send()
            .await
            .unwrap();

        let pab = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .block_public_acls(true)
            .ignore_public_acls(false)
            .block_public_policy(false)
            .restrict_public_buckets(false)
            .build();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();

        for (key, acl) in [
            ("foo1", "public-read"),
            ("foo2", "public-read-write"),
            ("foo3", "authenticated-read"),
        ] {
            let url = format!("{}/{bucket}/{key}", CTX.endpoint());
            let response = send_signed_put_response(&url, b"", &[("x-amz-acl", acl)]);
            assert_eq!(
                response.status, 403,
                "expected 403 for PutObject with x-amz-acl={acl} when BlockPublicAcls is set, got {}",
                response.status,
            );
            assert_error_code(&response.body, "AccessDenied");
        }

        let private_url = format!("{}/{bucket}/foo4", CTX.endpoint());
        let private_status = send_signed_put(&private_url, b"", &[("x-amz-acl", "private")]);
        assert_eq!(
            private_status, 200,
            "expected 200 for PutObject with x-amz-acl=private when BlockPublicAcls is set, got {private_status}",
        );
        let head = client
            .head_object()
            .bucket(&bucket)
            .key("foo4")
            .send()
            .await
            .unwrap();
        assert_eq!(head.content_length(), Some(0));

        client
            .delete_object()
            .bucket(&bucket)
            .key("foo4")
            .send()
            .await
            .unwrap();
        cleanup(&bucket).await;
    });
}

#[test]
fn test_block_public_copy_object_canned_acl() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = unique_bucket();
        s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::BucketOwnerPreferred)
            .send()
            .await
            .unwrap();

        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let pab = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .block_public_acls(true)
            .build();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();

        let result = client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{}/src", bucket))
            .acl(ObjectCannedAcl::PublicRead)
            .send()
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        client
            .delete_object()
            .bucket(&bucket)
            .key("src")
            .send()
            .await
            .unwrap();
        cleanup(&bucket).await;
    });
}

#[test]
fn test_block_public_object_authenticated_users_grant_header() {
    s3_tests::run(async {
        let bucket = setup_acl_enabled_bucket().await;
        let client = CTX.client();

        let pab = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .block_public_acls(true)
            .ignore_public_acls(false)
            .block_public_policy(false)
            .restrict_public_buckets(false)
            .build();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();

        let url = format!("{}/{bucket}/grant-header", CTX.endpoint());
        let response = send_signed_put_response(
            &url,
            b"",
            &[(
                "x-amz-grant-read",
                &format!("uri=\"{AUTHENTICATED_USERS_GROUP_URI}\""),
            )],
        );
        assert_eq!(response.status, 403);
        assert_error_code(&response.body, "AccessDenied");

        cleanup(&bucket).await;
    });
}

#[test]
fn test_block_public_put_object_acl_authenticated_users_xml_grant() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_acl_enabled_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        let owner_id = object_owner_id(&bucket, "foo").await;
        let pab = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
            .block_public_acls(true)
            .build();
        client
            .put_public_access_block()
            .bucket(&bucket)
            .public_access_block_configuration(pab)
            .send()
            .await
            .unwrap();

        let result = client
            .put_object_acl()
            .bucket(&bucket)
            .key("foo")
            .access_control_policy(access_control_policy(
                &owner_id,
                vec![
                    authenticated_users_group_grant(Permission::Read),
                    Grant::builder()
                        .grantee(
                            Grantee::builder()
                                .id(&owner_id)
                                .r#type(Type::CanonicalUser)
                                .build()
                                .expect("canonical grantee"),
                        )
                        .permission(Permission::FullControl)
                        .build(),
                ],
            ))
            .send()
            .await;
        assert_eq!(err_status(&result), 403);
        assert_s3_err_code(&result, "AccessDenied");

        client
            .delete_object()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        cleanup(&bucket).await;
    });
}

struct RawPutResponse {
    status: u16,
    body: String,
}

fn send_signed_put(url_str: &str, body: &[u8], extra_headers: &[(&str, &str)]) -> u16 {
    send_signed_put_response(url_str, body, extra_headers).status
}

fn send_signed_put_response(
    url_str: &str,
    body: &[u8],
    extra_headers: &[(&str, &str)],
) -> RawPutResponse {
    use std::time::SystemTime;

    let a = agent();

    let parsed = url::Url::parse(url_str).expect("parse URL");
    let path = parsed.path();
    let raw_query = parsed.query().unwrap_or("");
    // Normalize query parameters: bare keys like "acl" become "acl="
    let query = normalize_query(raw_query);

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let secs = now.as_secs();
    let dt = format_amz_date(secs);
    let date_stamp = &dt[..8];

    let access_key = CTX.access_key();
    let secret_key = CTX.secret_key();
    let region = CTX.region();
    let service = "s3";

    let host = parsed
        .host_str()
        .map(|h| {
            if let Some(port) = parsed.port() {
                format!("{h}:{port}")
            } else {
                h.to_string()
            }
        })
        .unwrap();

    let payload_hash = sha256_hex(body);

    let mut header_map: Vec<(String, String)> = vec![
        ("host".to_string(), host.clone()),
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-date".to_string(), dt.clone()),
    ];
    for &(k, v) in extra_headers {
        header_map.push((k.to_lowercase(), v.to_string()));
    }
    header_map.sort_by(|a, b| a.0.cmp(&b.0));

    let signed_headers: String = header_map
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");

    let canonical_headers: String = header_map
        .iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
        .collect();

    let canonical_request =
        format!("PUT\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}");

    let cr_hash = sha256_hex(canonical_request.as_bytes());
    let scope = format!("{date_stamp}/{region}/{service}/aws4_request");
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{dt}\n{scope}\n{cr_hash}");

    let k_date = hmac_sha256(
        format!("AWS4{secret_key}").as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");

    let signature = hex_encode(&hmac_sha256(&k_signing, string_to_sign.as_bytes()));

    let auth_header = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );

    let mut request = a
        .put(url_str)
        .header("Authorization", &auth_header)
        .header("x-amz-date", &dt)
        .header("x-amz-content-sha256", &payload_hash);

    for (k, v) in extra_headers {
        request = request.header(*k, *v);
    }

    let mut resp = request.send(body).expect("transport error");
    RawPutResponse {
        status: resp.status().as_u16(),
        body: resp.body_mut().read_to_string().unwrap_or_default(),
    }
}

fn sha256_hex(data: &[u8]) -> String {
    use ring::digest;
    let d = digest::digest(&digest::SHA256, data);
    hex_encode(d.as_ref())
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    use ring::hmac;
    let k = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&k, data).as_ref().to_vec()
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn format_amz_date(epoch_secs: u64) -> String {
    let secs = epoch_secs;
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;
    let (year, month, day) = days_to_date(days_since_epoch as i64);
    format!("{year:04}{month:02}{day:02}T{hours:02}{minutes:02}{seconds:02}Z")
}

/// Normalize a raw query string for SigV4 canonical request.
/// Bare keys like "acl" become "acl=", and parameters are sorted.
fn normalize_query(raw: &str) -> String {
    if raw.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = raw
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next().unwrap_or("").to_string();
            let val = parts.next().unwrap_or("").to_string();
            (key, val)
        })
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn days_to_date(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}
