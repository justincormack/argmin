//! Bucket ACL integration tests.
//!
//! Covers canned bucket ACLs, default ACL verification, grant revocation,
//! and error handling for invalid grant targets.

use aws_sdk_s3::types::{
    AccessControlPolicy, BucketCannedAcl, Grant, Grantee, ObjectOwnership, Owner,
    OwnershipControls, OwnershipControlsRule, Permission, Type,
};
use s3_tests::{
    assert_s3_err_code, disable_bucket_public_access_block, err_status, send_signed_request,
    unique_bucket, CTX,
};

const ALL_USERS_GROUP_URI: &str = "http://acs.amazonaws.com/groups/global/AllUsers";
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

fn sdk_err_status<E: std::fmt::Debug>(err: &aws_sdk_s3::error::SdkError<E>) -> u16 {
    err.raw_response()
        .map(|response| response.status().as_u16())
        .unwrap_or_else(|| panic!("error has no raw HTTP response: {err:?}"))
}

fn assert_sdk_err_code<E: std::fmt::Debug>(
    err: &aws_sdk_s3::error::SdkError<E>,
    expected_code: &str,
) {
    let msg = format!("{err:?}");
    assert!(
        msg.contains(expected_code),
        "expected error code '{expected_code}' in error: {msg}"
    );
}

async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    s3_tests::create_bucket(client, &bucket).await.unwrap();
    bucket
}

/// Create a bucket with ObjectWriter ownership and public access block disabled,
/// so that canned ACLs can be applied via PutBucketAcl.
async fn setup_acl_enabled_bucket() -> String {
    let client = CTX.client();
    let bucket = setup_bucket().await;
    disable_bucket_public_access_block(client, &bucket).await;
    let rule = OwnershipControlsRule::builder()
        .object_ownership(ObjectOwnership::ObjectWriter)
        .build()
        .unwrap();
    let controls = OwnershipControls::builder().rules(rule).build().unwrap();
    client
        .put_bucket_ownership_controls()
        .bucket(&bucket)
        .ownership_controls(controls)
        .send()
        .await
        .unwrap();
    bucket
}

async fn setup_named_acl_enabled_bucket(client: &aws_sdk_s3::Client, bucket: &str) {
    s3_tests::create_bucket_request(client, bucket)
        .object_ownership(ObjectOwnership::ObjectWriter)
        .send()
        .await
        .unwrap();
    disable_bucket_public_access_block(client, bucket).await;
}

async fn cleanup(bucket: &str) {
    let client = CTX.client();
    for attempt in 0..20 {
        let result = client.delete_bucket().bucket(bucket).send().await;
        match result {
            Ok(_) => return,
            Err(err) => {
                if sdk_err_status(&err) == 409 {
                    assert_sdk_err_code(&err, "OperationAborted");
                    if attempt < 19 {
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                        continue;
                    }
                }
                panic!("delete bucket cleanup failed: {err:?}");
            }
        }
    }
}

async fn cleanup_object_if_present(bucket: &str, key: &str) {
    let client = CTX.client();
    let _ = client.delete_object().bucket(bucket).key(key).send().await;
}

async fn anonymous_get(url: &str) -> s3_tests::Response {
    let url = url.to_string();
    tokio::task::spawn_blocking(move || s3_tests::test_agent().get(&url).call())
        .await
        .expect("anonymous GET task join")
        .unwrap_or_else(|err| panic!("anonymous bucket transport error: {err}"))
}

fn run_bucket_acl_test<F: std::future::Future>(future: F) -> F::Output {
    s3_tests::run(future)
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

fn has_canonical_user_grant(
    grants: &[Grant],
    canonical_user_id: &str,
    permission: Permission,
) -> bool {
    has_grant(grants, permission, Some(canonical_user_id), None)
}

fn has_group_grant(grants: &[Grant], uri: &str, permission: Permission) -> bool {
    has_grant(grants, permission, None, Some(uri))
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

fn all_users_group_grant(permission: Permission) -> Grant {
    Grant::builder()
        .grantee(
            Grantee::builder()
                .r#type(Type::Group)
                .uri(ALL_USERS_GROUP_URI)
                .build()
                .expect("all users grantee"),
        )
        .permission(permission)
        .build()
}

fn canonical_user_grant(canonical_user_id: &str, permission: Permission) -> Grant {
    Grant::builder()
        .grantee(
            Grantee::builder()
                .r#type(Type::CanonicalUser)
                .id(canonical_user_id)
                .build()
                .expect("canonical grantee"),
        )
        .permission(permission)
        .build()
}

fn bucket_acl_policy(owner_id: &str, grants: Vec<Grant>) -> AccessControlPolicy {
    AccessControlPolicy::builder()
        .owner(Owner::builder().id(owner_id).build())
        .set_grants(Some(grants))
        .build()
}

async fn alt_canonical_owner_id() -> String {
    let bucket = unique_bucket();
    let alt_client = CTX.alt_client();
    s3_tests::create_bucket(alt_client, &bucket).await.unwrap();
    let id = alt_client
        .get_bucket_acl()
        .bucket(&bucket)
        .send()
        .await
        .unwrap()
        .owner()
        .and_then(|owner| owner.id())
        .expect("expected owner ID in GetBucketAcl")
        .to_string();
    alt_client
        .delete_bucket()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    id
}

async fn apply_bucket_canonical_user_grant(
    bucket: &str,
    owner_id: &str,
    grantee_id: &str,
    permission: Permission,
) {
    CTX.client()
        .put_bucket_acl()
        .bucket(bucket)
        .access_control_policy(bucket_acl_policy(
            owner_id,
            vec![
                canonical_user_grant(owner_id, Permission::FullControl),
                canonical_user_grant(grantee_id, permission),
            ],
        ))
        .send()
        .await
        .unwrap();
}

async fn assert_alt_head_bucket_allowed(bucket: &str) {
    CTX.alt_client()
        .head_bucket()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
}

async fn assert_alt_head_bucket_denied(bucket: &str) {
    let result = CTX.alt_client().head_bucket().bucket(bucket).send().await;
    assert_eq!(err_status(&result), 403);
}

async fn assert_alt_get_bucket_acl_allowed(bucket: &str) {
    CTX.alt_client()
        .get_bucket_acl()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
}

async fn assert_alt_get_bucket_acl_denied(bucket: &str) {
    let result = CTX
        .alt_client()
        .get_bucket_acl()
        .bucket(bucket)
        .send()
        .await;
    assert_eq!(err_status(&result), 403);
}

async fn assert_alt_put_object_allowed(bucket: &str, key: &str) {
    CTX.alt_client()
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(
            b"alt-write",
        ))
        .send()
        .await
        .unwrap();
}

async fn assert_alt_put_object_denied(bucket: &str, key: &str) {
    let result = CTX
        .alt_client()
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(
            b"alt-write",
        ))
        .send()
        .await;
    assert_eq!(err_status(&result), 403);
}

async fn assert_alt_put_bucket_acl_allowed(bucket: &str) {
    CTX.alt_client()
        .put_bucket_acl()
        .bucket(bucket)
        .acl(BucketCannedAcl::Private)
        .send()
        .await
        .unwrap();
}

async fn assert_alt_put_bucket_acl_denied(bucket: &str) {
    let result = CTX
        .alt_client()
        .put_bucket_acl()
        .bucket(bucket)
        .acl(BucketCannedAcl::Private)
        .send()
        .await;
    assert_eq!(err_status(&result), 403);
}

#[test]
fn test_bucket_recreate_overwrite_acl() {
    run_bucket_acl_test(async {
        let client = CTX.client();
        let bucket = unique_bucket();

        setup_named_acl_enabled_bucket(client, &bucket).await;
        client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(BucketCannedAcl::PublicRead)
            .send()
            .await
            .unwrap();

        let result = s3_tests::create_bucket_request(client, &bucket)
            .send()
            .await;

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

        cleanup(&bucket).await;
    });
}

#[test]
fn test_bucket_recreate_new_acl() {
    run_bucket_acl_test(async {
        let client = CTX.client();
        let bucket = unique_bucket();

        setup_named_acl_enabled_bucket(client, &bucket).await;

        let result = s3_tests::create_bucket_request(client, &bucket)
            .acl(BucketCannedAcl::PublicRead)
            .object_ownership(ObjectOwnership::ObjectWriter)
            .send()
            .await;

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

        cleanup(&bucket).await;
    });
}

#[test]
fn test_bucket_create_public_read_acl_rejected() {
    run_bucket_acl_test(async {
        let client = CTX.client();
        let bucket = unique_bucket();

        let result = s3_tests::create_bucket_request(client, &bucket)
            .acl(BucketCannedAcl::PublicRead)
            .object_ownership(ObjectOwnership::ObjectWriter)
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidBucketAclWithBlockPublicAccessError");
    });
}

#[test]
fn test_bucket_recreate_new_header_grants() {
    run_bucket_acl_test(async {
        let client = CTX.client();
        let bucket = unique_bucket();

        setup_named_acl_enabled_bucket(client, &bucket).await;
        let owner_id = bucket_owner_id(&bucket).await;
        let alt_owner_id = alt_canonical_owner_id().await;

        let result = s3_tests::create_bucket_request(client, &bucket)
            .object_ownership(ObjectOwnership::ObjectWriter)
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

        cleanup(&bucket).await;
    });
}

#[test]
fn test_bucket_header_acl_grants() {
    run_bucket_acl_test(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();

        let alt_owner_id = alt_canonical_owner_id().await;
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
            .body(aws_sdk_s3::primitives::ByteStream::from_static(
                b"granted-write",
            ))
            .send()
            .await
            .unwrap();

        cleanup_object_if_present(&bucket, "granted-key").await;
        cleanup(&bucket).await;
    });
}

/// Verify default ACL on a new bucket: owner gets FULL_CONTROL only.
///
/// Matches Ceph: test_bucket_acl_default
#[test]
fn test_bucket_acl_default() {
    run_bucket_acl_test(async {
        let bucket = setup_bucket().await;

        let resp = CTX
            .client()
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let owner_id = resp
            .owner()
            .expect("expected owner")
            .id()
            .expect("expected owner ID")
            .to_string();

        assert_exact_grants(
            resp.grants(),
            &[(Permission::FullControl, Some(owner_id.as_str()), None)],
            "default bucket ACL",
        );

        cleanup(&bucket).await;
    });
}

/// Set public-read canned ACL on a bucket, verify grant structure.
///
/// Matches Ceph: test_bucket_acl_canned
#[test]
fn test_bucket_acl_canned_public_read() {
    run_bucket_acl_test(async {
        let client = CTX.client();
        let bucket = setup_acl_enabled_bucket().await;

        client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(BucketCannedAcl::PublicRead)
            .send()
            .await
            .unwrap();

        let resp = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let owner_id = resp
            .owner()
            .expect("expected owner")
            .id()
            .expect("expected owner ID")
            .to_string();

        assert_exact_grants(
            resp.grants(),
            &[
                (Permission::FullControl, Some(owner_id.as_str()), None),
                (Permission::Read, None, Some(ALL_USERS_GROUP_URI)),
            ],
            "public-read bucket ACL",
        );

        cleanup(&bucket).await;
    });
}

/// Set authenticated-read canned ACL, verify AuthenticatedUsers group grant.
///
/// Matches Ceph: test_bucket_acl_canned_authenticatedread
///
#[test]
fn test_bucket_acl_canned_authenticated_read() {
    run_bucket_acl_test(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = setup_acl_enabled_bucket().await;

        client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(BucketCannedAcl::AuthenticatedRead)
            .send()
            .await
            .unwrap();

        let resp = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let owner_id = resp
            .owner()
            .expect("expected owner")
            .id()
            .expect("expected owner ID")
            .to_string();

        assert_exact_grants(
            resp.grants(),
            &[
                (Permission::FullControl, Some(owner_id.as_str()), None),
                (Permission::Read, None, Some(AUTHENTICATED_USERS_GROUP_URI)),
            ],
            "authenticated-read bucket ACL",
        );

        alt_client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        cleanup(&bucket).await;
    });
}

#[test]
fn test_create_bucket_acl_canned_authenticated_read_rejected_with_default_ownership() {
    run_bucket_acl_test(async {
        let client = CTX.client();
        let bucket = unique_bucket();

        let result = s3_tests::create_bucket_request(client, &bucket)
            .acl(BucketCannedAcl::AuthenticatedRead)
            .send()
            .await;
        assert_eq!(err_status(&result), 400);
        assert_s3_err_code(&result, "InvalidBucketAclWithObjectOwnership");
    });
}

#[test]
fn test_bucket_acl_grant_authenticated_users_read_via_xml() {
    run_bucket_acl_test(async {
        let client = CTX.client();
        let alt_client = CTX.alt_client();
        let bucket = setup_acl_enabled_bucket().await;
        let owner_id = bucket_owner_id(&bucket).await;

        let acl = AccessControlPolicy::builder()
            .owner(Owner::builder().id(&owner_id).build())
            .set_grants(Some(vec![
                canonical_user_grant(&owner_id, Permission::FullControl),
                authenticated_users_group_grant(Permission::Read),
            ]))
            .build();

        client
            .put_bucket_acl()
            .bucket(&bucket)
            .access_control_policy(acl)
            .send()
            .await
            .unwrap();

        let resp = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_exact_grants(
            resp.grants(),
            &[
                (Permission::FullControl, Some(owner_id.as_str()), None),
                (Permission::Read, None, Some(AUTHENTICATED_USERS_GROUP_URI)),
            ],
            "bucket ACL XML authenticated users grant",
        );

        alt_client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();

        let mut anon = anonymous_get(&format!("{}/{}", CTX.endpoint(), bucket)).await;
        assert_eq!(anon.status().as_u16(), 403);
        let body = anon.body_mut().read_to_string().unwrap();
        assert!(
            body.contains("AccessDenied"),
            "expected AccessDenied for anonymous bucket access, got {body}"
        );

        cleanup(&bucket).await;
    });
}

#[test]
fn test_put_bucket_acl_grant_all_users_read_via_xml() {
    run_bucket_acl_test(async {
        let client = CTX.client();
        let bucket = setup_acl_enabled_bucket().await;
        let owner_id = bucket_owner_id(&bucket).await;

        let acl = AccessControlPolicy::builder()
            .owner(Owner::builder().id(&owner_id).build())
            .set_grants(Some(vec![
                canonical_user_grant(&owner_id, Permission::FullControl),
                all_users_group_grant(Permission::Read),
            ]))
            .build();

        client
            .put_bucket_acl()
            .bucket(&bucket)
            .access_control_policy(acl)
            .send()
            .await
            .unwrap();

        let resp = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_exact_grants(
            resp.grants(),
            &[
                (Permission::FullControl, Some(owner_id.as_str()), None),
                (Permission::Read, None, Some(ALL_USERS_GROUP_URI)),
            ],
            "bucket ACL XML all users read grant",
        );

        let mut anon = anonymous_get(&format!("{}/{}", CTX.endpoint(), bucket)).await;
        assert_eq!(anon.status().as_u16(), 200);
        let body = anon.body_mut().read_to_string().unwrap();
        assert!(
            body.contains("ListBucketResult"),
            "expected anonymous bucket list response, got {body}"
        );

        cleanup(&bucket).await;
    });
}

#[test]
fn test_put_bucket_acl_rejects_canned_acl_with_explicit_grant_headers() {
    run_bucket_acl_test(async {
        let bucket = setup_acl_enabled_bucket().await;
        let url = format!("{}/{bucket}?acl", CTX.endpoint());

        let response = send_signed_request(
            "PUT",
            &url,
            b"",
            vec![
                ("x-amz-acl".to_string(), "private".to_string()),
                (
                    "x-amz-grant-read".to_string(),
                    format!("uri=\"{ALL_USERS_GROUP_URI}\""),
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

        cleanup(&bucket).await;
    });
}

#[test]
fn test_bucket_acl_grant_canonical_user_full_control() {
    run_bucket_acl_test(async {
        let client = CTX.client();
        let bucket = setup_acl_enabled_bucket().await;
        let owner_id = bucket_owner_id(&bucket).await;
        let alt_owner_id = alt_canonical_owner_id().await;

        apply_bucket_canonical_user_grant(
            &bucket,
            &owner_id,
            &alt_owner_id,
            Permission::FullControl,
        )
        .await;

        let acl = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_exact_grants(
            acl.grants(),
            &[
                (Permission::FullControl, Some(owner_id.as_str()), None),
                (Permission::FullControl, Some(alt_owner_id.as_str()), None),
            ],
            "bucket canonical-user FULL_CONTROL grant",
        );

        assert_alt_head_bucket_allowed(&bucket).await;
        assert_alt_get_bucket_acl_allowed(&bucket).await;
        assert_alt_put_object_allowed(&bucket, "alt-full-control").await;
        assert_alt_put_bucket_acl_allowed(&bucket).await;

        let owner_after = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap()
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetBucketAcl")
            .to_string();
        assert_eq!(owner_after, owner_id);

        cleanup_object_if_present(&bucket, "alt-full-control").await;
        cleanup(&bucket).await;
    });
}

#[test]
fn test_bucket_acl_grant_canonical_user_read() {
    run_bucket_acl_test(async {
        let bucket = setup_acl_enabled_bucket().await;
        let owner_id = bucket_owner_id(&bucket).await;
        let alt_owner_id = alt_canonical_owner_id().await;

        apply_bucket_canonical_user_grant(&bucket, &owner_id, &alt_owner_id, Permission::Read)
            .await;

        assert_alt_head_bucket_allowed(&bucket).await;
        assert_alt_get_bucket_acl_denied(&bucket).await;
        assert_alt_put_object_denied(&bucket, "alt-read").await;
        assert_alt_put_bucket_acl_denied(&bucket).await;

        cleanup_object_if_present(&bucket, "alt-read").await;
        cleanup(&bucket).await;
    });
}

#[test]
fn test_bucket_acl_grant_canonical_user_read_acp() {
    run_bucket_acl_test(async {
        let bucket = setup_acl_enabled_bucket().await;
        let owner_id = bucket_owner_id(&bucket).await;
        let alt_owner_id = alt_canonical_owner_id().await;

        apply_bucket_canonical_user_grant(&bucket, &owner_id, &alt_owner_id, Permission::ReadAcp)
            .await;

        assert_alt_head_bucket_denied(&bucket).await;
        assert_alt_get_bucket_acl_allowed(&bucket).await;
        assert_alt_put_object_denied(&bucket, "alt-read-acp").await;
        assert_alt_put_bucket_acl_denied(&bucket).await;

        cleanup_object_if_present(&bucket, "alt-read-acp").await;
        cleanup(&bucket).await;
    });
}

#[test]
fn test_bucket_acl_grant_canonical_user_write() {
    run_bucket_acl_test(async {
        let bucket = setup_acl_enabled_bucket().await;
        let owner_id = bucket_owner_id(&bucket).await;
        let alt_owner_id = alt_canonical_owner_id().await;

        apply_bucket_canonical_user_grant(&bucket, &owner_id, &alt_owner_id, Permission::Write)
            .await;

        assert_alt_head_bucket_denied(&bucket).await;
        assert_alt_get_bucket_acl_denied(&bucket).await;
        assert_alt_put_object_allowed(&bucket, "alt-write").await;
        assert_alt_put_bucket_acl_denied(&bucket).await;

        cleanup_object_if_present(&bucket, "alt-write").await;
        cleanup(&bucket).await;
    });
}

#[test]
fn test_bucket_acl_grant_canonical_user_write_acp() {
    run_bucket_acl_test(async {
        let bucket = setup_acl_enabled_bucket().await;
        let owner_id = bucket_owner_id(&bucket).await;
        let alt_owner_id = alt_canonical_owner_id().await;

        apply_bucket_canonical_user_grant(&bucket, &owner_id, &alt_owner_id, Permission::WriteAcp)
            .await;

        assert_alt_head_bucket_denied(&bucket).await;
        assert_alt_get_bucket_acl_denied(&bucket).await;
        assert_alt_put_object_denied(&bucket, "alt-write-acp").await;
        assert_alt_put_bucket_acl_allowed(&bucket).await;

        cleanup_object_if_present(&bucket, "alt-write-acp").await;
        cleanup(&bucket).await;
    });
}

/// Set private canned ACL on a bucket that is already private.
///
/// Matches Ceph: test_bucket_acl_canned_private_to_private
#[test]
fn test_bucket_acl_canned_private_to_private() {
    run_bucket_acl_test(async {
        let client = CTX.client();
        let bucket = setup_acl_enabled_bucket().await;

        // Bucket starts private; apply private again
        client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(BucketCannedAcl::Private)
            .send()
            .await
            .unwrap();

        let resp = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let owner_id = resp
            .owner()
            .expect("expected owner")
            .id()
            .expect("expected owner ID")
            .to_string();

        assert_exact_grants(
            resp.grants(),
            &[(Permission::FullControl, Some(owner_id.as_str()), None)],
            "private-to-private bucket ACL",
        );

        cleanup(&bucket).await;
    });
}

#[test]
fn test_bucket_concurrent_set_canned_acl() {
    run_bucket_acl_test(async {
        let bucket = setup_acl_enabled_bucket().await;
        let mut tasks = Vec::new();

        for _ in 0..50 {
            let client = CTX.client().clone();
            let bucket = bucket.clone();
            tasks.push(tokio::spawn(async move {
                client
                    .put_bucket_acl()
                    .bucket(&bucket)
                    .acl(BucketCannedAcl::PublicRead)
                    .send()
                    .await
            }));
        }

        let mut success_count = 0usize;
        for task in tasks {
            match task.await.unwrap() {
                Ok(_) => success_count += 1,
                Err(err) => {
                    assert_eq!(sdk_err_status(&err), 409);
                    assert_sdk_err_code(&err, "OperationAborted");
                }
            }
        }
        assert!(
            success_count > 0,
            "expected at least one PutBucketAcl to succeed"
        );

        let acl = CTX
            .client()
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        let owner_id = acl
            .owner()
            .and_then(|owner| owner.id())
            .expect("expected owner ID in GetBucketAcl")
            .to_string();
        assert_exact_grants(
            acl.grants(),
            &[
                (Permission::FullControl, Some(owner_id.as_str()), None),
                (Permission::Read, None, Some(ALL_USERS_GROUP_URI)),
            ],
            "concurrent public-read bucket ACL",
        );

        cleanup(&bucket).await;
    });
}

/// Grant by nonexistent canonical user ID returns 400 InvalidArgument.
///
/// Matches Ceph: test_bucket_acl_grant_nonexist_user
#[test]
fn test_bucket_acl_grant_nonexist_user() {
    run_bucket_acl_test(async {
        let client = CTX.client();
        let bucket = setup_acl_enabled_bucket().await;
        let owner_id = bucket_owner_id(&bucket).await;

        let bad_grantee = Grantee::builder()
            .r#type(Type::CanonicalUser)
            .id("nonexistent-canonical-id-that-does-not-exist")
            .build()
            .unwrap();
        let grant = Grant::builder()
            .grantee(bad_grantee)
            .permission(Permission::Read)
            .build();
        let acl = AccessControlPolicy::builder()
            .owner(Owner::builder().id(&owner_id).build())
            .grants(grant)
            .build();

        let result = client
            .put_bucket_acl()
            .bucket(&bucket)
            .access_control_policy(acl)
            .send()
            .await;
        assert_eq!(err_status(&result), 400);

        cleanup(&bucket).await;
    });
}

/// Revoke public grants by switching back to private canned ACL.
///
/// Matches Ceph: test_bucket_acl_revoke_all
#[test]
fn test_bucket_acl_revoke_all() {
    run_bucket_acl_test(async {
        let client = CTX.client();
        let bucket = setup_acl_enabled_bucket().await;
        let owner_id = bucket_owner_id(&bucket).await;

        // Set public-read first so there are grants to revoke
        client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(BucketCannedAcl::PublicRead)
            .send()
            .await
            .unwrap();
        let resp = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert!(
            resp.grants().len() > 1,
            "expected multiple grants before revoke"
        );

        // Revoke all public grants by applying the private canned ACL.
        // AWS rejects an empty grants list as MalformedACLError, so we
        // use the private canned ACL which leaves only owner FULL_CONTROL.
        client
            .put_bucket_acl()
            .bucket(&bucket)
            .acl(BucketCannedAcl::Private)
            .send()
            .await
            .unwrap();

        let resp = client
            .get_bucket_acl()
            .bucket(&bucket)
            .send()
            .await
            .unwrap();
        assert_exact_grants(
            resp.grants(),
            &[(Permission::FullControl, Some(owner_id.as_str()), None)],
            "revoked bucket ACL (only owner FULL_CONTROL)",
        );

        cleanup(&bucket).await;
    });
}
