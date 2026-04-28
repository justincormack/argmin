use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    AccessControlPolicy, Grant, Grantee, ObjectCannedAcl, ObjectOwnership, Owner,
    OwnershipControls, OwnershipControlsRule, Permission, Type,
};
use aws_sdk_s3::Client;
use s3_tests::{
    create_public_write_bucket, delete_all_and_bucket, disable_bucket_public_access_block,
    err_status, send_signed_request, unique_bucket, CTX,
};
use std::time::Duration;

const AUTHENTICATED_USERS_GROUP_URI: &str =
    "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";
const AWS_EXEC_READ_CANONICAL_ID: &str =
    "6aa5a366c34c1cbe25dc49211496e913e0351eb0e8c37aa3477e40942ec6b97c";

/// Create a bucket, returning its name. Tests are responsible for cleanup.
async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
}

async fn set_object_writer_ownership(bucket: &str) {
    let rule = OwnershipControlsRule::builder()
        .object_ownership(ObjectOwnership::ObjectWriter)
        .build()
        .unwrap();
    let controls = OwnershipControls::builder().rules(rule).build().unwrap();
    CTX.client()
        .put_bucket_ownership_controls()
        .bucket(bucket)
        .ownership_controls(controls)
        .send()
        .await
        .unwrap();
}

async fn setup_acl_enabled_bucket() -> String {
    let client = CTX.client();
    let bucket = setup_bucket().await;
    disable_bucket_public_access_block(client, &bucket).await;
    set_object_writer_ownership(&bucket).await;
    client
        .get_public_access_block()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    client
        .get_bucket_ownership_controls()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    bucket
}

async fn canonical_owner_id(client: &Client) -> String {
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
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

fn canonical_user_grant(canonical_user_id: &str, permission: Permission) -> Grant {
    Grant::builder()
        .grantee(
            Grantee::builder()
                .id(canonical_user_id)
                .r#type(Type::CanonicalUser)
                .build()
                .expect("canonical grantee"),
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

fn has_grant(
    grants: &[Grant],
    permission: Permission,
    canonical_user_id: Option<&str>,
    uri: Option<&str>,
) -> bool {
    grants.iter().any(|grant| {
        grant.permission() == Some(&permission)
            && grant
                .grantee()
                .is_some_and(|grantee| grantee.id() == canonical_user_id && grantee.uri() == uri)
    })
}

fn assert_exact_grants(
    grants: &[Grant],
    expected: &[(Permission, Option<&str>, Option<&str>)],
    context: &str,
) {
    assert_eq!(
        grants.len(),
        expected.len(),
        "unexpected grant count for {context}: {grants:?}"
    );
    for (permission, canonical_user_id, uri) in expected {
        assert!(
            has_grant(grants, permission.clone(), *canonical_user_id, *uri),
            "missing grant {permission:?} id={canonical_user_id:?} uri={uri:?} for {context}: {grants:?}"
        );
    }
}

async fn object_owner_id(client: &Client, bucket: &str, key: &str) -> String {
    client
        .get_object_acl()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap()
        .owner()
        .and_then(|owner| owner.id())
        .expect("expected owner ID in GetObjectAcl")
        .to_string()
}

async fn bucket_owner_id(client: &Client, bucket: &str) -> String {
    client
        .get_bucket_acl()
        .bucket(bucket)
        .send()
        .await
        .unwrap()
        .owner()
        .and_then(|owner| owner.id())
        .expect("expected owner ID in GetBucketAcl")
        .to_string()
}

async fn run_object_header_acl_grants_case(key: &str, body: Vec<u8>) {
    let client = CTX.client();
    let alt_client = CTX.alt_client();

    let bucket = setup_bucket().await;
    set_object_writer_ownership(&bucket).await;
    let alt_owner_id = canonical_owner_id(alt_client).await;

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .customize()
        .mutate_request({
            let alt_owner_id = alt_owner_id.clone();
            move |req| {
                req.headers_mut().insert(
                    "x-amz-grant-read",
                    format!("id=\"{}\"", alt_owner_id.clone()),
                );
                req.headers_mut().insert(
                    "x-amz-grant-read-acp",
                    format!("id=\"{}\"", alt_owner_id.clone()),
                );
                req.headers_mut().insert(
                    "x-amz-grant-write-acp",
                    format!("id=\"{}\"", alt_owner_id.clone()),
                );
                req.headers_mut().insert(
                    "x-amz-grant-full-control",
                    format!("id=\"{}\"", alt_owner_id.clone()),
                );
            }
        })
        .send()
        .await
        .unwrap();

    let acl = client
        .get_object_acl()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let grants = acl.grants();
    assert!(has_grant(
        grants,
        Permission::Read,
        Some(&alt_owner_id),
        None
    ));
    assert!(has_grant(
        grants,
        Permission::ReadAcp,
        Some(&alt_owner_id),
        None
    ));
    assert!(has_grant(
        grants,
        Permission::WriteAcp,
        Some(&alt_owner_id),
        None
    ));
    assert!(has_grant(
        grants,
        Permission::FullControl,
        Some(&alt_owner_id),
        None
    ));
    let owner_id = acl
        .owner()
        .and_then(|owner| owner.id())
        .expect("expected owner ID in GetObjectAcl")
        .to_string();
    assert_eq!(
        grants.len(),
        4,
        "expected exact explicit grants without implicit owner FULL_CONTROL, got {grants:?}"
    );
    assert!(
        !has_grant(grants, Permission::FullControl, Some(&owner_id), None),
        "did not expect implicit owner FULL_CONTROL grant in {grants:?}"
    );

    let read = alt_get_object_eventually(&bucket, key).await;
    let read_body = read.body.collect().await.unwrap().into_bytes();
    assert_eq!(&read_body[..], body.as_slice());

    alt_client
        .get_object_acl()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap();

    alt_client
        .put_object_acl()
        .bucket(&bucket)
        .key(key)
        .access_control_policy(access_control_policy(
            &owner_id,
            vec![
                canonical_user_grant(&owner_id, Permission::FullControl),
                canonical_user_grant(&alt_owner_id, Permission::Read),
                canonical_user_grant(&alt_owner_id, Permission::ReadAcp),
                canonical_user_grant(&alt_owner_id, Permission::WriteAcp),
                canonical_user_grant(&alt_owner_id, Permission::FullControl),
            ],
        ))
        .send()
        .await
        .unwrap();

    client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    client.delete_bucket().bucket(&bucket).send().await.unwrap();
}

async fn alt_get_object_eventually(
    bucket: &str,
    key: &str,
) -> aws_sdk_s3::operation::get_object::GetObjectOutput {
    const MAX_ATTEMPTS: usize = 10;

    for attempt in 0..MAX_ATTEMPTS {
        match CTX
            .alt_client()
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => return output,
            Err(_) if attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(err) => panic!("alternate GetObject failed unexpectedly: {err:?}"),
        }
    }

    unreachable!()
}

async fn assert_alt_get_object_allowed(bucket: &str, key: &str, expected_body: &[u8]) {
    let get = CTX
        .alt_client()
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let body = get.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), expected_body);
}

async fn assert_alt_get_object_denied(bucket: &str, key: &str) {
    let result = CTX
        .alt_client()
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await;
    assert_eq!(err_status(&result), 403);
}

async fn assert_alt_get_object_acl_allowed(bucket: &str, key: &str) {
    CTX.alt_client()
        .get_object_acl()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
}

async fn assert_alt_get_object_acl_denied(bucket: &str, key: &str) {
    let result = CTX
        .alt_client()
        .get_object_acl()
        .bucket(bucket)
        .key(key)
        .send()
        .await;
    assert_eq!(err_status(&result), 403);
}

async fn assert_alt_put_object_acl_allowed(
    bucket: &str,
    key: &str,
    owner_id: &str,
    alt_owner_id: &str,
    alt_permission: Permission,
) {
    CTX.alt_client()
        .put_object_acl()
        .bucket(bucket)
        .key(key)
        .access_control_policy(access_control_policy(
            owner_id,
            vec![
                canonical_user_grant(owner_id, Permission::FullControl),
                canonical_user_grant(alt_owner_id, alt_permission),
            ],
        ))
        .send()
        .await
        .unwrap();
}

async fn assert_alt_put_object_acl_denied(bucket: &str, key: &str) {
    let result = CTX
        .alt_client()
        .put_object_acl()
        .bucket(bucket)
        .key(key)
        .acl(ObjectCannedAcl::Private)
        .send()
        .await;
    assert_eq!(err_status(&result), 403);
}

async fn setup_object_with_alt_acl_grant(permission: Permission) -> (String, String, String) {
    let client = CTX.client();
    let alt_client = CTX.alt_client();
    let bucket = setup_acl_enabled_bucket().await;

    client
        .put_object()
        .bucket(&bucket)
        .key("foo")
        .body(ByteStream::from_static(b"bar"))
        .send()
        .await
        .unwrap();

    let owner_id = object_owner_id(client, &bucket, "foo").await;
    let alt_owner_id = canonical_owner_id(alt_client).await;
    client
        .put_object_acl()
        .bucket(&bucket)
        .key("foo")
        .access_control_policy(access_control_policy(
            &owner_id,
            vec![
                canonical_user_grant(&owner_id, Permission::FullControl),
                canonical_user_grant(&alt_owner_id, permission),
            ],
        ))
        .send()
        .await
        .unwrap();

    (bucket, owner_id, alt_owner_id)
}

async fn run_object_acl_canonical_user_permission_case(
    permission: Permission,
    expect_get_object: bool,
    expect_get_object_acl: bool,
    expect_put_object_acl: bool,
) {
    let client = CTX.client();
    let (bucket, owner_id, alt_owner_id) =
        setup_object_with_alt_acl_grant(permission.clone()).await;

    let acl = client
        .get_object_acl()
        .bucket(&bucket)
        .key("foo")
        .send()
        .await
        .unwrap();
    assert_exact_grants(
        acl.grants(),
        &[
            (Permission::FullControl, Some(owner_id.as_str()), None),
            (permission.clone(), Some(alt_owner_id.as_str()), None),
        ],
        "object ACL canonical-user grant matrix setup",
    );

    if expect_get_object {
        assert_alt_get_object_allowed(&bucket, "foo", b"bar").await;
    } else {
        assert_alt_get_object_denied(&bucket, "foo").await;
    }

    if expect_get_object_acl {
        assert_alt_get_object_acl_allowed(&bucket, "foo").await;
    } else {
        assert_alt_get_object_acl_denied(&bucket, "foo").await;
    }

    if expect_put_object_acl {
        assert_alt_put_object_acl_allowed(&bucket, "foo", &owner_id, &alt_owner_id, permission)
            .await;
    } else {
        assert_alt_put_object_acl_denied(&bucket, "foo").await;
    }

    delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
}

#[test]
fn test_object_acl_default() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        let owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetObjectAcl")
            .to_string();
        assert_exact_grants(
            acl.grants(),
            &[(Permission::FullControl, Some(owner_id.as_str()), None)],
            "default object ACL",
        );

        delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
    });
}

#[test]
fn test_put_object_acl_canned_private_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_bucket().await;
        set_object_writer_ownership(&bucket).await;

        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        client
            .put_object_acl()
            .bucket(&bucket)
            .key("foo")
            .acl(ObjectCannedAcl::Private)
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        let owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetObjectAcl")
            .to_string();
        assert_exact_grants(
            acl.grants(),
            &[(Permission::FullControl, Some(owner_id.as_str()), None)],
            "private object ACL via PutObjectAcl",
        );

        delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
    });
}

#[test]
fn test_object_acl_canned_aws_exec_read_during_create() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_acl_enabled_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .acl(ObjectCannedAcl::AwsExecRead)
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        let owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetObjectAcl")
            .to_string();
        assert_exact_grants(
            acl.grants(),
            &[
                (Permission::Read, Some(AWS_EXEC_READ_CANONICAL_ID), None),
                (Permission::FullControl, Some(owner_id.as_str()), None),
            ],
            "aws-exec-read object ACL during create",
        );

        delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
    });
}

#[test]
fn test_put_object_acl_canned_bucket_owner_read_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_public_write_bucket(client).await;

        alt_client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        alt_client
            .put_object_acl()
            .bucket(&bucket)
            .key("foo")
            .acl(ObjectCannedAcl::BucketOwnerRead)
            .send()
            .await
            .unwrap();

        let acl = alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        let alt_owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected object owner ID in GetObjectAcl")
            .to_string();
        let bucket_owner_id = bucket_owner_id(client, &bucket).await;
        assert_exact_grants(
            acl.grants(),
            &[
                (Permission::FullControl, Some(alt_owner_id.as_str()), None),
                (Permission::Read, Some(bucket_owner_id.as_str()), None),
            ],
            "bucket-owner-read object ACL via PutObjectAcl",
        );

        delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
    });
}

#[test]
fn test_put_object_acl_canned_aws_exec_read_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_acl_enabled_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        client
            .put_object_acl()
            .bucket(&bucket)
            .key("foo")
            .acl(ObjectCannedAcl::AwsExecRead)
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        let owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetObjectAcl")
            .to_string();
        assert_exact_grants(
            acl.grants(),
            &[
                (Permission::Read, Some(AWS_EXEC_READ_CANONICAL_ID), None),
                (Permission::FullControl, Some(owner_id.as_str()), None),
            ],
            "aws-exec-read object ACL via PutObjectAcl",
        );

        delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
    });
}

#[test]
fn test_put_object_acl_canned_bucket_owner_full_control_round_trip() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = create_public_write_bucket(client).await;

        alt_client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        alt_client
            .put_object_acl()
            .bucket(&bucket)
            .key("foo")
            .acl(ObjectCannedAcl::BucketOwnerFullControl)
            .send()
            .await
            .unwrap();

        let acl = alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        let alt_owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected object owner ID in GetObjectAcl")
            .to_string();
        let bucket_owner_id = bucket_owner_id(client, &bucket).await;
        assert_exact_grants(
            acl.grants(),
            &[
                (Permission::FullControl, Some(alt_owner_id.as_str()), None),
                (
                    Permission::FullControl,
                    Some(bucket_owner_id.as_str()),
                    None,
                ),
            ],
            "bucket-owner-full-control object ACL via PutObjectAcl",
        );

        client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();

        delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
    });
}

#[test]
fn test_put_object_acl_full_control_verify_attributes() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = setup_bucket().await;
        set_object_writer_ownership(&bucket).await;

        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        let owner_id = object_owner_id(client, &bucket, "foo").await;
        let alt_owner_id = canonical_owner_id(alt_client).await;
        client
            .put_object_acl()
            .bucket(&bucket)
            .key("foo")
            .access_control_policy(access_control_policy(
                &owner_id,
                vec![
                    canonical_user_grant(&owner_id, Permission::FullControl),
                    canonical_user_grant(&alt_owner_id, Permission::FullControl),
                ],
            ))
            .send()
            .await
            .unwrap();

        let acl = alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        assert_exact_grants(
            acl.grants(),
            &[
                (Permission::FullControl, Some(owner_id.as_str()), None),
                (Permission::FullControl, Some(alt_owner_id.as_str()), None),
            ],
            "cross-account FULL_CONTROL object ACL",
        );

        let get = alt_client
            .get_object()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        let body = get.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"bar");

        delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
    });
}

#[test]
fn test_object_header_acl_grants() {
    s3_tests::run(async {
        run_object_header_acl_grants_case("testobj", b"header-acl".to_vec()).await;
    });
}

#[test]
fn test_object_header_acl_grants_streaming_put() {
    s3_tests::run(async {
        let body = vec![0x5Au8; server_core::coordinator::INTERNAL_SEGMENT_SIZE + 1];
        run_object_header_acl_grants_case("streaming-testobj", body).await;
    });
}

#[test]
fn test_put_object_grant_write_header_persists_write_grant() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_acl_enabled_bucket().await;
        let owner_id = canonical_owner_id(client).await;
        let key = "grant-write-header";
        let grant_write_owner_id = owner_id.clone();

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"grant-write-header"))
            .customize()
            .mutate_request(move |req| {
                req.headers_mut().insert(
                    "x-amz-grant-write",
                    format!("id=\"{grant_write_owner_id}\""),
                );
            })
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(acl.grants(), Permission::Write, Some(&owner_id), None),
            "expected WRITE grant for object owner, got {:?}",
            acl.grants()
        );

        delete_all_and_bucket(client, &bucket, &[key.to_string()]).await;
    });
}

#[test]
fn test_put_object_acl_grant_write_header_persists_write_grant() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_acl_enabled_bucket().await;
        let key = "put-object-acl-grant-write";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"put-object-acl-grant-write"))
            .send()
            .await
            .unwrap();

        let owner_id = object_owner_id(client, &bucket, key).await;
        let grant_write_owner_id = owner_id.clone();
        client
            .put_object_acl()
            .bucket(&bucket)
            .key(key)
            .customize()
            .mutate_request(move |req| {
                req.headers_mut().insert(
                    "x-amz-grant-write",
                    format!("id=\"{grant_write_owner_id}\""),
                );
            })
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(acl.grants(), Permission::Write, Some(&owner_id), None),
            "expected WRITE grant for object owner, got {:?}",
            acl.grants()
        );

        delete_all_and_bucket(client, &bucket, &[key.to_string()]).await;
    });
}

#[test]
fn test_put_object_acl_grant_write_xml_persists_write_grant() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = setup_acl_enabled_bucket().await;
        let key = "put-object-acl-grant-write-xml";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"put-object-acl-grant-write-xml"))
            .send()
            .await
            .unwrap();

        let owner_id = object_owner_id(client, &bucket, key).await;
        let alt_owner_id = canonical_owner_id(alt_client).await;
        client
            .put_object_acl()
            .bucket(&bucket)
            .key(key)
            .access_control_policy(access_control_policy(
                &owner_id,
                vec![canonical_user_grant(&alt_owner_id, Permission::Write)],
            ))
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(acl.grants(), Permission::Write, Some(&alt_owner_id), None),
            "expected WRITE grant for alternate canonical user, got {:?}",
            acl.grants()
        );

        delete_all_and_bucket(client, &bucket, &[key.to_string()]).await;
    });
}

#[test]
fn test_copy_object_grant_write_header_persists_write_grant() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_acl_enabled_bucket().await;
        let owner_id = canonical_owner_id(client).await;
        let grant_write_owner_id = owner_id.clone();

        client
            .put_object()
            .bucket(&bucket)
            .key("src")
            .body(ByteStream::from_static(b"copy-source"))
            .send()
            .await
            .unwrap();

        client
            .copy_object()
            .bucket(&bucket)
            .key("dst")
            .copy_source(format!("{bucket}/src"))
            .customize()
            .mutate_request(move |req| {
                req.headers_mut().insert(
                    "x-amz-grant-write",
                    format!("id=\"{grant_write_owner_id}\""),
                );
            })
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("dst")
            .send()
            .await
            .unwrap();
        assert!(
            has_grant(acl.grants(), Permission::Write, Some(&owner_id), None),
            "expected WRITE grant for object owner, got {:?}",
            acl.grants()
        );

        delete_all_and_bucket(client, &bucket, &["src".to_string(), "dst".to_string()]).await;
    });
}

#[test]
fn test_put_object_acl_rejects_canned_acl_with_explicit_grant_headers() {
    s3_tests::run(async {
        let client = CTX.client();
        let bucket = setup_acl_enabled_bucket().await;
        let key = "put-object-acl-conflict";

        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();

        let url = format!("{}/{bucket}/{key}?acl", CTX.endpoint());
        let response = send_signed_request(
            "PUT",
            &url,
            b"",
            vec![
                ("x-amz-acl".to_string(), "private".to_string()),
                (
                    "x-amz-grant-read".to_string(),
                    format!("uri=\"{AUTHENTICATED_USERS_GROUP_URI}\""),
                ),
            ],
        );

        assert_eq!(response.status, 400, "unexpected body: {}", response.body);
        assert!(
            response.body.contains("<Code>InvalidRequest</Code>"),
            "unexpected body: {}",
            response.body
        );
        assert!(
            response
                .body
                .contains("Specifying both Canned ACLs and Header Grants is not allowed"),
            "unexpected body: {}",
            response.body
        );

        delete_all_and_bucket(client, &bucket, &[key.to_string()]).await;
    });
}

#[test]
fn test_put_object_acl_explicit_grants_do_not_add_owner_full_control() {
    s3_tests::run(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = setup_acl_enabled_bucket().await;

        client
            .put_object()
            .bucket(&bucket)
            .key("foo")
            .body(ByteStream::from_static(b"bar"))
            .send()
            .await
            .unwrap();

        let owner_id = object_owner_id(client, &bucket, "foo").await;
        let alt_owner_id = canonical_owner_id(alt_client).await;
        client
            .put_object_acl()
            .bucket(&bucket)
            .key("foo")
            .access_control_policy(access_control_policy(
                &owner_id,
                vec![canonical_user_grant(&alt_owner_id, Permission::FullControl)],
            ))
            .send()
            .await
            .unwrap();

        let acl = client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();
        assert_exact_grants(
            acl.grants(),
            &[(Permission::FullControl, Some(alt_owner_id.as_str()), None)],
            "explicit PutObjectAcl grant without implicit owner FULL_CONTROL",
        );

        alt_client
            .get_object_acl()
            .bucket(&bucket)
            .key("foo")
            .send()
            .await
            .unwrap();

        delete_all_and_bucket(client, &bucket, &["foo".to_string()]).await;
    });
}

#[test]
fn test_object_acl_grant_canonical_user_read() {
    s3_tests::run(async {
        run_object_acl_canonical_user_permission_case(Permission::Read, true, false, false).await;
    });
}

#[test]
fn test_object_acl_grant_canonical_user_read_acp() {
    s3_tests::run(async {
        run_object_acl_canonical_user_permission_case(Permission::ReadAcp, false, true, false)
            .await;
    });
}

#[test]
fn test_object_acl_grant_canonical_user_write_acp() {
    s3_tests::run(async {
        run_object_acl_canonical_user_permission_case(Permission::WriteAcp, false, false, true)
            .await;
    });
}

#[test]
fn test_object_acl_grant_canonical_user_full_control() {
    s3_tests::run(async {
        run_object_acl_canonical_user_permission_case(Permission::FullControl, true, true, true)
            .await;
    });
}
