//! Bucket ACL integration tests.
//!
//! Covers canned bucket ACLs, default ACL verification, grant revocation,
//! and error handling for invalid grant targets.

use aws_sdk_s3::types::{
    AccessControlPolicy, BucketCannedAcl, Grant, Grantee, ObjectOwnership, Owner,
    OwnershipControls, OwnershipControlsRule, Permission, Type,
};
use s3_tests::{disable_bucket_public_access_block, err_status, unique_bucket, CTX};

const ALL_USERS_GROUP_URI: &str = "http://acs.amazonaws.com/groups/global/AllUsers";
// Uncomment when authenticated-read bucket ACL is implemented.
// const AUTHENTICATED_USERS_GROUP_URI: &str =
//     "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";

async fn setup_bucket() -> String {
    let client = CTX.client();
    let bucket = unique_bucket();
    client.create_bucket().bucket(&bucket).send().await.unwrap();
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

async fn cleanup(bucket: &str) {
    let client = CTX.client();
    client.delete_bucket().bucket(bucket).send().await.unwrap();
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

/// Verify default ACL on a new bucket: owner gets FULL_CONTROL only.
///
/// Matches Ceph: test_bucket_acl_default
#[test]
fn test_bucket_acl_default() {
    s3_tests::run(async {
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
    s3_tests::run(async {
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
/// NOTE: authenticated-read for buckets is not yet implemented (501).
/// Uncomment when support is added.
// #[test]
// fn test_bucket_acl_canned_authenticated_read() {
//     s3_tests::run(async {
//         let client = CTX.client();
//         let bucket = setup_acl_enabled_bucket().await;
//
//         client
//             .put_bucket_acl()
//             .bucket(&bucket)
//             .acl(BucketCannedAcl::AuthenticatedRead)
//             .send()
//             .await
//             .unwrap();
//
//         let resp = client
//             .get_bucket_acl()
//             .bucket(&bucket)
//             .send()
//             .await
//             .unwrap();
//         let owner_id = resp
//             .owner()
//             .expect("expected owner")
//             .id()
//             .expect("expected owner ID")
//             .to_string();
//
//         assert_exact_grants(
//             resp.grants(),
//             &[
//                 (Permission::FullControl, Some(owner_id.as_str()), None),
//                 (
//                     Permission::Read,
//                     None,
//                     Some(AUTHENTICATED_USERS_GROUP_URI),
//                 ),
//             ],
//             "authenticated-read bucket ACL",
//         );
//
//         cleanup(&bucket).await;
//     });
// }

/// Set private canned ACL on a bucket that is already private.
///
/// Matches Ceph: test_bucket_acl_canned_private_to_private
#[test]
fn test_bucket_acl_canned_private_to_private() {
    s3_tests::run(async {
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

/// Grant by nonexistent canonical user ID returns 400 InvalidArgument.
///
/// Matches Ceph: test_bucket_acl_grant_nonexist_user
#[test]
fn test_bucket_acl_grant_nonexist_user() {
    s3_tests::run(async {
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
    s3_tests::run(async {
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
