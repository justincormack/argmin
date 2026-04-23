use super::test_helpers::{self, UploadPartRequest};
use super::test_support::*;
use super::*;
use crate::coordinator::authz::BucketPolicyRequestContext;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

#[test]
fn put_object_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("other-user"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn put_object_rejects_acl_on_bucket_owner_enforced_bucket() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_ownership_controls_test(&coord,
            "bucket",
            "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
            test_requester(), None)
        .unwrap();

    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: PutObjectAcl::PublicRead.into(),
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessControlListNotSupported));
}

#[test]
fn put_bucket_ownership_controls_rejects_public_read_bucket() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", true)
        .unwrap();

    let err = put_bucket_ownership_controls_test(&coord,
            "bucket",
            "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
            test_helpers::requester("owner-a"), None)
        .unwrap_err();
    assert!(matches!(
        err,
        ServerError::InvalidBucketAclWithObjectOwnership
    ));
}

#[test]
fn authorize_put_bucket_ownership_controls_rejects_public_read_bucket() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", true)
        .unwrap();

    let err = coord
        .authorize_put_bucket_ownership_controls(&PutBucketOwnershipControlsRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("owner-a"),
                None,
            ),
            config: BucketOwnershipControls {
                object_ownership: BucketObjectOwnership::BucketOwnerEnforced,
            },
        })
        .unwrap_err();
    assert!(matches!(
        err,
        ServerError::InvalidBucketAclWithObjectOwnership
    ));
}

#[test]
fn put_bucket_acl_rejects_block_public_acls() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_public_access_block_test(&coord,
            "bucket",
            "<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>false</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let err = put_bucket_canned_acl_test(
        &coord,
        "bucket",
        BucketAcl::PublicRead,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let err = put_bucket_canned_acl_test(
        &coord,
        "bucket",
        BucketAcl::AuthenticatedRead,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn authorize_put_bucket_acl_rejects_block_public_acls() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_public_access_block_test(&coord,
            "bucket",
            "<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>false</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let err = coord
        .authorize_put_bucket_acl(&PutBucketAclRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("owner-a"),
                None,
            ),
            acl: PutBucketAclInput::Canned(BucketAcl::PublicRead),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let err = coord
        .authorize_put_bucket_acl(&PutBucketAclRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("owner-a"),
                None,
            ),
            acl: PutBucketAclInput::Canned(BucketAcl::AuthenticatedRead),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn get_bucket_acl_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetBucketAcl","Resource":"arn:aws:s3:::bucket"}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    let acl = get_bucket_acl_test(
        &coord,
        "bucket",
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert_eq!(acl.owner_principal, "111122223333");
}

#[test]
fn put_bucket_acl_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:PutBucketAcl","Resource":"arn:aws:s3:::bucket"}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    coord
        .put_bucket_acl(&PutBucketAclRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("444455556666"),
                None,
            ),
            acl: PutBucketAclInput::Canned(BucketAcl::Private),
            policy_context: PutObjectPolicyContext::default()
                .with_default_canned_acl(Some("private")),
        })
        .unwrap();
}

#[test]
fn put_bucket_acl_bucket_policy_canned_acl_condition_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:PutBucketAcl","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"s3:x-amz-acl":"private"}}}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    coord
        .put_bucket_acl(&PutBucketAclRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("444455556666"),
                None,
            ),
            acl: PutBucketAclInput::Canned(BucketAcl::Private),
            policy_context: PutObjectPolicyContext::default()
                .with_default_canned_acl(Some("private")),
        })
        .unwrap();

    let err = coord
        .put_bucket_acl(&PutBucketAclRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("444455556666"),
                None,
            ),
            acl: PutBucketAclInput::Canned(BucketAcl::PublicRead),
            policy_context: PutObjectPolicyContext::default()
                .with_default_canned_acl(Some("public-read")),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn put_bucket_acl_bucket_policy_grant_read_condition_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    let alt_canonical_id = CanonicalUserId::from_principal("444455556666");
    let grant_read_header = format!("id=\"{alt_canonical_id}\"");
    let escaped_grant_read_header = grant_read_header.replace('"', "\\\"");
    put_bucket_policy_test(
        &coord,
        "bucket",
        &format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"arn:aws:iam::444455556666:root"}},"Action":"s3:PutBucketAcl","Resource":"arn:aws:s3:::bucket","Condition":{{"StringEquals":{{"s3:x-amz-grant-read":"{escaped_grant_read_header}"}}}}}}]}}"#
        ),
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    let err = coord
        .put_bucket_acl(&PutBucketAclRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("444455556666"),
                None,
            ),
            acl: PutBucketAclInput::Canned(BucketAcl::Private),
            policy_context: PutObjectPolicyContext::default()
                .with_default_canned_acl(Some("private")),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let acl_grants = AclGrants::new(vec![AclGrant::new(
        AclGrantee::CanonicalUser(alt_canonical_id.clone()),
        AclPermission::Read,
    )]);
    coord
        .put_bucket_acl(&PutBucketAclRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("444455556666"),
                None,
            ),
            acl: PutBucketAclInput::Grants(acl_grants),
            policy_context: PutObjectPolicyContext::default().with_acl_grant_headers(
                Some(grant_read_header.as_str()),
                None,
                None,
                None,
                None,
            ),
        })
        .unwrap();

    let acl = get_bucket_acl_test(
        &coord,
        "bucket",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    assert!(acl
        .acl_grants
        .allows_canonical_user(&alt_canonical_id, AclPermission::Read));
}

#[test]
fn put_bucket_acl_rejects_authenticated_users_grant_when_block_public_acls_enabled() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_public_access_block_test(&coord,
            "bucket",
            "<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>false</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let err = put_bucket_acl_test(
        &coord,
        "bucket",
        AclGrants::new(vec![AclGrant::new(
            AclGrantee::AuthenticatedUsers,
            AclPermission::Read,
        )]),
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn put_object_rejects_public_acl_when_block_public_acls_enabled() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_public_access_block_test(&coord,
            "bucket",
            "<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>false</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: PutObjectAcl::PublicRead.into(),
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn put_object_rejects_authenticated_read_acl_when_block_public_acls_enabled() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_public_access_block_test(&coord,
            "bucket",
            "<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>false</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: PutObjectAcl::AuthenticatedRead.into(),
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn put_object_rejects_authenticated_users_grant_when_block_public_acls_enabled() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_public_access_block_test(&coord,
            "bucket",
            "<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>false</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: PutObjectWriteAcl::Grants(AclGrants::new(vec![AclGrant::new(
                AclGrantee::AuthenticatedUsers,
                AclPermission::Read,
            )])),
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn put_object_accepts_bucket_owner_read_acl_for_same_owner() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: PutObjectAcl::BucketOwnerRead.into(),
        },
    )
    .unwrap();
}

#[test]
fn put_object_rejects_invalid_acl_value() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: PutObjectAcl::Invalid("definitely-not-a-real-acl").into(),
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::InvalidArgument { .. }));
}

#[test]
fn put_object_persists_explicit_acl_grants_on_write() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::new(
        "owner-a",
        CanonicalUserId::from_principal("owner-write-grants-canonical"),
        "Owner A",
    );
    let grantee = AccountIdentity::new(
        "grantee-a",
        CanonicalUserId::from_principal("grantee-write-grants-canonical"),
        "Grantee A",
    );
    let owner_requester = Requester::authenticated(owner.clone());
    let grantee_requester = Requester::authenticated(grantee.clone());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: owner_requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                owner_requester.clone(),
                None,
            ),
            data: b"granted-read",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: PutObjectWriteAcl::Grants(AclGrants::new(vec![
                AclGrant::new(
                    AclGrantee::CanonicalUser(grantee.canonical_user_id().clone()),
                    AclPermission::Read,
                ),
                AclGrant::new(
                    AclGrantee::CanonicalUser(grantee.canonical_user_id().clone()),
                    AclPermission::ReadAcp,
                ),
            ])),
        },
    )
    .unwrap();

    let object = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                grantee_requester.clone(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(object.body.read_all().unwrap(), b"granted-read");

    let acl = get_object_acl_test(
        &coord,
        "bucket",
        "key",
        None,
        grantee_requester.clone(),
        None,
    )
    .unwrap();
    assert!(grants_contain(
        &acl.acl_grants,
        &AclGrantee::CanonicalUser(grantee.canonical_user_id().clone()),
        AclPermission::Read,
    ));
    assert!(grants_contain(
        &acl.acl_grants,
        &AclGrantee::CanonicalUser(grantee.canonical_user_id().clone()),
        AclPermission::ReadAcp,
    ));
}

#[test]
fn bucket_owner_cannot_manage_cross_owned_object_acl_without_explicit_grant() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::new(
        "owner-a",
        CanonicalUserId::from_principal("owner-cross-acl-canonical"),
        "Owner A",
    );
    let writer = AccountIdentity::new(
        "writer-a",
        CanonicalUserId::from_principal("writer-cross-acl-canonical"),
        "Writer A",
    );
    let owner_requester = Requester::authenticated(owner.clone());
    let writer_requester = Requester::authenticated(writer.clone());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: owner_requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::Grants(AclGrants::new(vec![AclGrant::new(
                AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
                AclPermission::Write,
            )])),
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                writer_requester.clone(),
                None,
            ),
            data: b"owned-by-writer",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let err = get_object_acl_test(&coord, "bucket", "key", None, owner_requester.clone(), None)
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let err = put_object_acl_test(
        &coord,
        "bucket",
        "key",
        None,
        AclGrants::new(vec![]),
        owner_requester.clone(),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    get_object_acl_test(
        &coord,
        "bucket",
        "key",
        None,
        writer_requester.clone(),
        None,
    )
    .unwrap();
}

#[test]
fn put_object_persists_write_acl_grants_on_write() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::new(
        "owner-a",
        CanonicalUserId::from_principal("owner-write-grant-invalid-canonical"),
        "Owner A",
    );
    let grantee = AccountIdentity::new(
        "grantee-a",
        CanonicalUserId::from_principal("grantee-write-grant-invalid-canonical"),
        "Grantee A",
    );
    let owner_requester = Requester::authenticated(owner);

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: owner_requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                owner_requester.clone(),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: PutObjectWriteAcl::Grants(AclGrants::new(vec![AclGrant::new(
                AclGrantee::CanonicalUser(grantee.canonical_user_id().clone()),
                AclPermission::Write,
            )])),
        },
    )
    .unwrap();

    let acl = get_object_acl_test(&coord, "bucket", "key", None, owner_requester, None).unwrap();
    assert!(grants_contain(
        &acl.acl_grants,
        &AclGrantee::CanonicalUser(grantee.canonical_user_id().clone()),
        AclPermission::Write,
    ));
}

#[test]
fn put_object_acl_persists_write_acl_grants() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::new(
        "owner-a",
        CanonicalUserId::from_principal("owner-put-object-acl-write-grant-canonical"),
        "Owner A",
    );
    let grantee = AccountIdentity::new(
        "grantee-a",
        CanonicalUserId::from_principal("grantee-put-object-acl-write-grant-canonical"),
        "Grantee A",
    );
    let owner_requester = Requester::authenticated(owner);

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: owner_requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                owner_requester.clone(),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    put_object_acl_test(
        &coord,
        "bucket",
        "key",
        None,
        AclGrants::new(vec![AclGrant::new(
            AclGrantee::CanonicalUser(grantee.canonical_user_id().clone()),
            AclPermission::Write,
        )]),
        owner_requester.clone(),
        None,
    )
    .unwrap();

    let acl = get_object_acl_test(&coord, "bucket", "key", None, owner_requester, None).unwrap();
    assert!(grants_contain(
        &acl.acl_grants,
        &AclGrantee::CanonicalUser(grantee.canonical_user_id().clone()),
        AclPermission::Write,
    ));
}

#[test]
fn put_object_rejects_acl_grants_on_bucket_owner_enforced_bucket() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::new(
        "owner-a",
        CanonicalUserId::from_principal("owner-boe-grants-canonical"),
        "Owner A",
    );
    let grantee = AccountIdentity::new(
        "grantee-a",
        CanonicalUserId::from_principal("grantee-boe-grants-canonical"),
        "Grantee A",
    );
    let owner_requester = Requester::authenticated(owner);

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: owner_requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap();

    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                owner_requester.clone(),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: PutObjectWriteAcl::Grants(AclGrants::new(vec![AclGrant::new(
                AclGrantee::CanonicalUser(grantee.canonical_user_id().clone()),
                AclPermission::Read,
            )])),
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessControlListNotSupported));
}

#[test]
fn put_object_allows_bucket_owner_full_control_grant_on_bucket_owner_enforced_bucket() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::new(
        "owner-a",
        CanonicalUserId::from_principal("owner-boe-grants-canonical"),
        "Owner A",
    );
    let owner_requester = Requester::authenticated(owner.clone());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: owner_requester,
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                Requester::authenticated(owner.clone()),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: PutObjectWriteAcl::Grants(AclGrants::new(vec![AclGrant::new(
                AclGrantee::CanonicalUser(owner.canonical_user_id().clone()),
                AclPermission::FullControl,
            )])),
        },
    )
    .unwrap();
}

#[test]
fn get_object_tags_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let err = get_object_tags_test(
        &coord,
        "bucket",
        "key",
        None,
        test_helpers::requester("other-user"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn get_object_bucket_policy_existing_tag_controls_access() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let public_tags = "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag><Tag><Key>foo</Key><Value>bar</Value></Tag></TagSet></Tagging>";
    let private_tags =
        "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>";
    let invalid_tags =
        "<Tagging><TagSet><Tag><Key>security1</Key><Value>public</Value></Tag></TagSet></Tagging>";

    for (key, body, tags) in [
        ("publictag", b"public".as_slice(), Some(public_tags)),
        ("privatetag", b"private".as_slice(), Some(private_tags)),
        ("invalidtag", b"invalid".as_slice(), Some(invalid_tags)),
    ] {
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    key,
                    test_helpers::requester("owner-a"),
                    None,
                ),
                data: body,
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let object = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "publictag",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(object.body.read_all().unwrap(), b"public");

    for key in ["privatetag", "invalidtag"] {
        let err = coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    key,
                    None,
                    test_helpers::requester("other-user"),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }
}

#[test]
fn authorize_get_object_bucket_policy_existing_tag_controls_access() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "publictag",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"public",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
            ),
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let authorized = coord
        .authorize_get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "publictag",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert!(matches!(authorized.locked.record, StoredObject::Live(_)));
}

#[test]
fn get_object_part_and_head_part_bucket_policy_allow_cross_account_read() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/key"}]}"#,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"public-via-policy",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let part = coord
        .get_object_part(&GetObjectPartRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            part_number: 1,
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(part.body.read_all().unwrap(), b"public-via-policy");

    let head = coord
        .head_object_part(&GetObjectPartRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            part_number: 1,
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(head.part_size, b"public-via-policy".len() as u64);
    assert_eq!(head.total_size, b"public-via-policy".len() as u64);
}

#[test]
fn get_object_range_bucket_policy_allow_cross_account_read() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/key"}]}"#,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"public-via-policy",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let range = coord
        .get_object_range(&GetObjectRangeRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            range: ByteRange::Range { start: 0, end: 5 },
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(range.body.read_all().unwrap(), b"public");
    assert_eq!(range.range_start, 0);
    assert_eq!(range.range_end, 5);
}

#[test]
fn authorize_get_object_masks_missing_private_object_as_access_denied() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let err = coord
        .authorize_get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "missing",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn put_object_tagging_bucket_policy_uses_current_existing_tags() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:PutObjectTagging","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let public_tags =
        "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>";
    let private_tags =
        "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>";

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(public_tags),
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    put_object_tags_test(
        &coord,
        "bucket",
        "key",
        None,
        private_tags,
        test_helpers::requester("other-user"),
        None,
    )
    .unwrap();

    let err = put_object_tags_test(
        &coord,
        "bucket",
        "key",
        None,
        public_tags,
        test_helpers::requester("other-user"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let tags = get_object_tags_test(
        &coord,
        "bucket",
        "key",
        None,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap()
    .expect("expected tags after update");
    assert!(tags.contains("<Key>security</Key>"));
    assert!(tags.contains("<Value>private</Value>"));
}

#[test]
fn copy_object_bucket_policy_copy_source_controls_access() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "src", false)
        .unwrap();
    coord
        .create_bucket_for_owner("owner-a", "dst", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "src",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::src/*"}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();
    put_bucket_policy_test(&coord,
            "dst",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::dst/*","Condition":{"StringLike":{"s3:x-amz-copy-source":"src/public/*"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    for (key, body) in [
        ("public/foo", b"public-foo".as_slice()),
        ("private/foo", b"private-foo".as_slice()),
    ] {
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "src",
                    key,
                    test_helpers::requester("owner-a"),
                    None,
                ),
                data: body,
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let source = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "src",
                "public/foo",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(source.body.read_all().unwrap(), b"public-foo");
    coord
        .authorize_put_object_write(&AuthorizePutObjectRequest {
            object: object_request_with_expected_owner(
                "dst",
                "copied",
                test_helpers::requester("other-user"),
                None,
            ),
            acl: PutObjectAcl::None.into(),
            policy_context: PutObjectPolicyContext::new(Some("src/public/foo"), None, None),
            object_lock: ObjectLockState::default(),
            tags: None,
            encryption: WriteEncryptionRequest::none(),
        })
        .unwrap();
    let authorized = coord
        .authorize_copy_object(&CopyObjectRequest {
            source: copy_source("src", "public/foo", None),
            destination: object_request_with_expected_owner(
                "dst",
                "copied",
                test_helpers::requester("other-user"),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: PutObjectAcl::None.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();
    assert!(matches!(authorized.source.stored, StoredObject::Live(_)));
    drop(authorized);

    let copied = coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("src", "public/foo", None),
            destination: object_request_with_expected_owner(
                "dst",
                "copied",
                test_helpers::requester("other-user"),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: PutObjectAcl::None.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();
    assert_eq!(copied.version_id, VersionId::Null);

    let copied_body = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "dst",
                "copied",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(copied_body.body.read_all().unwrap(), b"public-foo");

    let err = coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("src", "private/foo", None),
            destination: object_request_with_expected_owner(
                "dst",
                "denied",
                test_helpers::requester("other-user"),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: PutObjectAcl::None.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn copy_object_bucket_policy_requires_explicit_copy_metadata_directive() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "src", false)
        .unwrap();
    coord
        .create_bucket_for_owner("owner-a", "dst", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "src",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::src/*"}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();
    put_bucket_policy_test(&coord,
            "dst",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::dst/*","Condition":{"StringEquals":{"s3:x-amz-metadata-directive":"COPY"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "src",
                "public/foo",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"public-foo",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let source = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "src",
                "public/foo",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(source.body.read_all().unwrap(), b"public-foo");
    coord
        .authorize_put_object_write(&AuthorizePutObjectRequest {
            object: object_request_with_expected_owner(
                "dst",
                "copied",
                test_helpers::requester("other-user"),
                None,
            ),
            acl: PutObjectAcl::None.into(),
            policy_context: PutObjectPolicyContext::new(Some("src/public/foo"), Some("COPY"), None),
            object_lock: ObjectLockState::default(),
            tags: None,
            encryption: WriteEncryptionRequest::none(),
        })
        .unwrap();

    coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("src", "public/foo", None),
            destination: object_request_with_expected_owner(
                "dst",
                "copied",
                test_helpers::requester("other-user"),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::CopyExplicit,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: PutObjectAcl::None.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();

    let err = coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("src", "public/foo", None),
            destination: object_request_with_expected_owner(
                "dst",
                "denied",
                test_helpers::requester("other-user"),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: PutObjectAcl::None.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn put_object_bucket_policy_deny_on_public_acl() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*"},{"Effect":"Deny","Principal":{"AWS":"other-user"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringLike":{"s3:x-amz-acl":"public*"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "private-key",
                test_helpers::requester("other-user"),
                None,
            ),
            data: b"private",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: PutObjectAcl::None.into(),
        },
    )
    .unwrap();

    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default()
                .with_default_canned_acl(PutObjectAcl::PublicRead.policy_condition_value()),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "public-key",
                test_helpers::requester("other-user"),
                None,
            ),
            data: b"public",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: PutObjectAcl::PublicRead.into(),
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn put_object_bucket_policy_request_object_tag_controls_access() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":["s3:PutObject","s3:PutObjectTagging"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:RequestObjectTag/security":"public"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let denied = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "private-key",
                test_helpers::requester("other-user"),
                None,
            ),
            data: b"private",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap_err();
    assert!(matches!(denied, ServerError::AccessDenied));

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "public-key", test_helpers::requester("other-user"), None),
            data: b"public",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
            ),
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
            },
    )
    .unwrap();
}

#[test]
fn bucket_policy_decision_for_put_object_tagging_requires_matching_action() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:RequestObjectTag/security":"public"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let bucket = coord.unchecked_active_bucket_summary("bucket").unwrap();
    let policy = coord.cached_bucket_policy(&bucket).unwrap();
    let decision = coord.bucket_policy_decision_for_put_object_action(
        BucketPolicyRequestContext {
            requester: &test_helpers::requester("other-user"),
            bucket: &bucket,
            bucket_tags: None,
            action: auth::PolicyAction::PutObjectTagging,
            policy_context: PutObjectPolicyContext::default().with_request_object_tags_xml(Some(
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
            )),
            policy: policy.as_deref(),
        },
        "public-key",
    )
    .unwrap();

    assert_eq!(decision, auth::PolicyEvaluation::NoMatch);
}

#[test]
fn bucket_policy_decision_for_put_object_tagging_honors_inline_request_object_tags() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":["s3:PutObject","s3:PutObjectTagging"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:RequestObjectTag/security":"public"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let bucket = coord.unchecked_active_bucket_summary("bucket").unwrap();
    let policy = coord.cached_bucket_policy(&bucket).unwrap();
    let decision = coord.bucket_policy_decision_for_put_object_action(
        BucketPolicyRequestContext {
            requester: &test_helpers::requester("other-user"),
            bucket: &bucket,
            bucket_tags: None,
            action: auth::PolicyAction::PutObjectTagging,
            policy_context: PutObjectPolicyContext::default().with_request_object_tags_xml(Some(
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
            )),
            policy: policy.as_deref(),
        },
        "public-key",
    )
    .unwrap();

    assert_eq!(decision, auth::PolicyEvaluation::ExplicitAllow);
}

#[test]
fn put_object_bucket_policy_request_object_tag_requires_put_object_tagging_action() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:RequestObjectTag/security":"public"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let denied = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "public-key", test_helpers::requester("other-user"), None),
            data: b"public",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
            ),
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
            },
    )
    .unwrap_err();
    assert!(matches!(denied, ServerError::AccessDenied));
}

#[test]
fn put_object_tagging_bucket_policy_request_object_tag_controls_access() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:PutObjectTagging","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:RequestObjectTag/security":"public"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let public_tags =
        "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>";
    put_object_tags_test(
        &coord,
        "bucket",
        "key",
        None,
        public_tags,
        test_helpers::requester("other-user"),
        None,
    )
    .unwrap();

    let denied = put_object_tags_test(
        &coord,
        "bucket",
        "key",
        None,
        "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>",
        test_helpers::requester("other-user"),
        None,
    )
    .unwrap_err();
    assert!(matches!(denied, ServerError::AccessDenied));

    let tags = get_object_tags_test(
        &coord,
        "bucket",
        "key",
        None,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap()
    .expect("expected tags after update");
    assert!(tags.contains("<Key>security</Key>"));
    assert!(tags.contains("<Value>public</Value>"));
}

#[test]
fn put_object_version_tagging_bucket_policy_request_object_tag_controls_access() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(&coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:PutObjectVersionTagging","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:RequestObjectTag/security":"public"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let public_tags =
        "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>";
    put_object_tags_test(
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        public_tags,
        test_helpers::requester("other-user"),
        None,
    )
    .unwrap();

    let denied = put_object_tags_test(
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>",
        test_helpers::requester("other-user"),
        None,
    )
    .unwrap_err();
    assert!(matches!(denied, ServerError::AccessDenied));

    let tags = get_object_tags_test(
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap()
    .expect("expected version tags after update");
    assert!(tags.contains("<Key>security</Key>"));
    assert!(tags.contains("<Value>public</Value>"));
}

#[test]
fn create_multipart_upload_bucket_policy_request_object_tag_controls_access() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":["s3:PutObject","s3:PutObjectTagging"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:RequestObjectTag/security":"public"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let denied = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "private-key",
                test_helpers::requester("other-user"),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap_err();
    assert!(matches!(denied, ServerError::AccessDenied));

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "public-key", test_helpers::requester("other-user"), None),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
            ),
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
            })
        .unwrap();
    assert!(!upload.upload_id.as_str().is_empty());
}

#[test]
fn authorize_create_multipart_upload_bucket_policy_request_object_tag_controls_access() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":["s3:PutObject","s3:PutObjectTagging"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:RequestObjectTag/security":"public"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let authorized = coord
        .authorize_create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "public-key",
                test_helpers::requester("other-user"),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
            ),
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    assert_eq!(authorized.bucket, "bucket");
    assert_eq!(authorized.key, "public-key");
    assert!(authorized.tags.is_some());
}

#[test]
fn create_multipart_upload_bucket_policy_request_object_tag_requires_put_object_tagging_action() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:RequestObjectTag/security":"public"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let denied = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "public-key", test_helpers::requester("other-user"), None),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
            ),
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
            })
        .unwrap_err();
    assert!(matches!(denied, ServerError::AccessDenied));
}

#[test]
fn copy_object_bucket_policy_existing_tag_source_is_not_evaluable() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "src", false)
        .unwrap();
    coord
        .create_bucket_for_owner("other-user", "dst", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "src",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::src/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    for (key, body, tags) in [
        (
            "public/foo",
            b"public-foo".as_slice(),
            Some(
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
            ),
        ),
        (
            "private/foo",
            b"private-foo".as_slice(),
            Some(
                "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>",
            ),
        ),
    ] {
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "src",
                    key,
                    test_helpers::requester("owner-a"),
                    None,
                ),
                data: body,
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let source = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "src",
                "public/foo",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(source.body.read_all().unwrap(), b"public-foo");

    let denied = coord
        .authorize_copy_object(&CopyObjectRequest {
            source: copy_source("src", "public/foo", None),
            destination: object_request_with_expected_owner(
                "dst",
                "copied",
                test_helpers::requester("other-user"),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            tagging: TaggingDirective::Copy,
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap_err();
    assert!(matches!(denied, ServerError::AccessDenied));
}

#[test]
fn upload_part_copy_bucket_policy_copy_source_controls_access() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "src", false)
        .unwrap();
    coord
        .create_bucket_for_owner("other-user", "dst", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "src",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::src/public/*"}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    for (key, body) in [
        ("public/foo", b"public-foo".as_slice()),
        ("private/foo", b"private-foo".as_slice()),
    ] {
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "src",
                    key,
                    test_helpers::requester("owner-a"),
                    None,
                ),
                data: body,
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "dst",
                "copied",
                test_helpers::requester("other-user"),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let copied_part = coord
        .upload_part_copy(&UploadPartCopyRequest {
            source: copy_source("src", "public/foo", None),
            upload: multipart_object_request_with_expected_owner(
                "dst",
                "copied",
                &upload.upload_id,
                test_helpers::requester("other-user"),
                None,
            ),
            part_number: 1,
            copy_source_range: None,

            source_sse_customer: None,
            sse_customer: None,
        })
        .unwrap();
    assert!(!copied_part.etag.is_empty());

    let denied = coord
        .upload_part_copy(&UploadPartCopyRequest {
            source: copy_source("src", "private/foo", None),
            upload: multipart_object_request_with_expected_owner(
                "dst",
                "copied",
                &upload.upload_id,
                test_helpers::requester("other-user"),
                None,
            ),
            part_number: 2,
            copy_source_range: None,

            source_sse_customer: None,
            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(denied, ServerError::AccessDenied));
}

#[test]
fn upload_part_copy_bucket_policy_existing_tag_source_controls_access() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "src", false)
        .unwrap();
    coord
        .create_bucket_for_owner("other-user", "dst", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "src",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::src/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    for (key, body, tags) in [
        (
            "public/foo",
            b"public-foo".as_slice(),
            Some(
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
            ),
        ),
        (
            "private/foo",
            b"private-foo".as_slice(),
            Some(
                "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>",
            ),
        ),
    ] {
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "src",
                    key,
                    test_helpers::requester("owner-a"),
                    None,
                ),
                data: body,
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let source = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "src",
                "public/foo",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(source.body.read_all().unwrap(), b"public-foo");

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "dst",
                "copied",
                test_helpers::requester("other-user"),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let copied_part = coord
        .upload_part_copy(&UploadPartCopyRequest {
            source: copy_source("src", "public/foo", None),
            upload: multipart_object_request_with_expected_owner(
                "dst",
                "copied",
                &upload.upload_id,
                test_helpers::requester("other-user"),
                None,
            ),
            part_number: 1,
            copy_source_range: None,
            source_sse_customer: None,
            sse_customer: None,
        })
        .unwrap();
    assert!(!copied_part.etag.is_empty());

    let denied_upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "dst",
                "copied-denied",
                test_helpers::requester("other-user"),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let denied = coord
        .upload_part_copy(&UploadPartCopyRequest {
            source: copy_source("src", "private/foo", None),
            upload: multipart_object_request_with_expected_owner(
                "dst",
                "copied-denied",
                &denied_upload.upload_id,
                test_helpers::requester("other-user"),
                None,
            ),
            part_number: 1,
            copy_source_range: None,
            source_sse_customer: None,
            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(denied, ServerError::AccessDenied));
}

#[test]
fn multipart_upload_managed_encryption_policy_context_enables_upload_part_copy_write() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "dst", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "dst",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::dst/*"},{"Effect":"Deny","Principal":{"AWS":"other-user"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::dst/*","Condition":{"Null":{"s3:x-amz-server-side-encryption":"true"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "dst",
                "copied",
                test_helpers::requester("other-user"),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::managed(ManagedEncryptionAlgorithm::Aes256),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let bucket = coord.unchecked_active_bucket_summary("dst").unwrap();
    let policy = coord.cached_bucket_policy(&bucket).unwrap();
    let meta_pg = coord
        .storage_node
        .get_pg(coord.object_pg_id("dst", "copied"))
        .unwrap();
    let upload = meta_pg.get_multipart_upload(&upload.upload_id).unwrap();
    let requester = test_helpers::requester("other-user");
    let base_context = PutObjectPolicyContext::new(Some("src/public/foo"), None, None);

    assert!(!coord
        .requester_can_write_multipart_upload_with_bucket_policy(
            &requester,
            &bucket,
            None,
            &upload,
            base_context,
            policy.as_deref(),
        )
        .unwrap());
    assert!(coord
        .requester_can_write_multipart_upload_with_bucket_policy(
            &requester,
            &bucket,
            None,
            &upload,
            Coordinator::with_multipart_upload_managed_encryption_policy_context(
                PutObjectPolicyContext::new(Some("src/public/foo"), None, None),
                &upload,
            ),
            policy.as_deref(),
        )
        .unwrap());
}

#[test]
fn begin_stream_put_bucket_policy_deny_on_public_acl() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*"},{"Effect":"Deny","Principal":{"AWS":"other-user"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringLike":{"s3:x-amz-acl":"public*"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let private_session = begin_stream_put_with_authorized_request_test(
        &coord,
        object_request_with_expected_owner(
            "bucket",
            "private-key",
            test_helpers::requester("other-user"),
            None,
        ),
        PutObjectAcl::None.into(),
        PutObjectPolicyContext::default(),
        WriteEncryptionRequest::none(),
        ObjectLockState::default(),
    )
    .unwrap();
    coord
        .abort_stream_put("bucket", "private-key", &private_session)
        .unwrap();

    let err = begin_stream_put_with_authorized_request_test(
        &coord,
        object_request_with_expected_owner(
            "bucket",
            "public-key",
            test_helpers::requester("other-user"),
            None,
        ),
        PutObjectAcl::PublicRead.into(),
        PutObjectPolicyContext::default(),
        WriteEncryptionRequest::none(),
        ObjectLockState::default(),
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn get_object_acl_bucket_policy_existing_tag_controls_access() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:GetObjectAcl","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    for (key, body, tags) in [
        (
            "publictag",
            b"public".as_slice(),
            Some("<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag><Tag><Key>foo</Key><Value>bar</Value></Tag></TagSet></Tagging>"),
        ),
        (
            "privatetag",
            b"private".as_slice(),
            Some("<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>"),
        ),
        (
            "invalidtag",
            b"invalid".as_slice(),
            Some("<Tagging><TagSet><Tag><Key>security1</Key><Value>public</Value></Tag></TagSet></Tagging>"),
        ),
    ] {
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    key,
                    test_helpers::requester("owner-a"),
                    None,
                ),
                data: body,
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let acl = get_object_acl_test(
        &coord,
        "bucket",
        "publictag",
        None,
        test_helpers::requester("other-user"),
        None,
    )
    .unwrap();
    assert_eq!(acl.version_id, VersionId::Null);

    let denied_object = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "publictag",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(denied_object, ServerError::AccessDenied));

    for key in ["privatetag", "invalidtag"] {
        let err = get_object_acl_test(
            &coord,
            "bucket",
            key,
            None,
            test_helpers::requester("other-user"),
            None,
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }
}

#[test]
fn authorize_get_object_acl_bucket_policy_existing_tag_controls_access() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:GetObjectAcl","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "publictag",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"public",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
            ),
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let authorized = coord
        .authorize_get_object_acl(&object_version_request_with_expected_owner(
            "bucket",
            "publictag",
            None,
            test_helpers::requester("other-user"),
            None,
        ))
        .unwrap();
    assert_eq!(authorized.result.version_id, VersionId::Null);
}

#[test]
fn put_object_acl_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:PutObjectAcl","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    put_object_canned_acl_test(
        &coord,
        "bucket",
        "key",
        None,
        PutObjectAcl::PublicRead,
        test_helpers::requester("other-user"),
        None,
    )
    .unwrap();

    let acl = get_object_acl_test(
        &coord,
        "bucket",
        "key",
        None,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    assert!(grants_contain(
        &acl.acl_grants,
        &AclGrantee::AllUsers,
        AclPermission::Read,
    ));
}

#[test]
fn put_object_acl_bucket_policy_deny_on_public_acl() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:PutObjectAcl","Resource":"arn:aws:s3:::bucket/*"},{"Effect":"Deny","Principal":{"AWS":"other-user"},"Action":"s3:PutObjectAcl","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringLike":{"s3:x-amz-acl":"public*"}}}]}"#,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    put_object_canned_acl_test(
        &coord,
        "bucket",
        "key",
        None,
        PutObjectAcl::Private,
        test_helpers::requester("other-user"),
        None,
    )
    .unwrap();

    let err = put_object_canned_acl_test(
        &coord,
        "bucket",
        "key",
        None,
        PutObjectAcl::PublicRead,
        test_helpers::requester("other-user"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let acl = get_object_acl_test(
        &coord,
        "bucket",
        "key",
        None,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    assert!(!grants_contain(
        &acl.acl_grants,
        &AclGrantee::AllUsers,
        AclPermission::Read,
    ));
}

#[test]
fn put_object_acl_rejects_canned_acl_policy_context_mismatch() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:PutObjectAcl","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    let err = coord
        .put_object_acl(&PutObjectAclRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            acl: PutObjectAclInput::Canned(PutObjectAcl::Private),
            policy_context: PutObjectPolicyContext::default()
                .with_default_canned_acl(PutObjectAcl::PublicRead.policy_condition_value()),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::InvalidArgument { .. }));
}

#[test]
fn put_object_acl_rejects_grant_policy_context_mismatch() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:PutObjectAcl","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    let err = coord
        .put_object_acl(&PutObjectAclRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            acl: PutObjectAclInput::Grants(AclGrants::new(vec![AclGrant::new(
                AclGrantee::CanonicalUser(CanonicalUserId::from_principal("grantee-a")),
                AclPermission::Read,
            )])),
            policy_context: PutObjectPolicyContext::default().with_acl_grant_headers(
                Some(r#"id="different-grantee""#),
                None,
                None,
                None,
                None,
            ),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::InvalidArgument { .. }));
}

#[test]
fn put_object_version_acl_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:PutObjectVersionAcl","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    put_object_canned_acl_test(
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        PutObjectAcl::PublicRead,
        test_helpers::requester("other-user"),
        None,
    )
    .unwrap();

    let acl = get_object_acl_test(
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    assert!(grants_contain(
        &acl.acl_grants,
        &AclGrantee::AllUsers,
        AclPermission::Read,
    ));
}

#[test]
fn put_object_version_acl_bucket_policy_grant_read_condition_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let grantee_canonical_id = CanonicalUserId::from_principal("grantee-canonical");
    let grant_read_header = format!("id=\"{grantee_canonical_id}\"");
    let grant_read_header_json = format!(
        "\"{}\"",
        grant_read_header.replace('\\', "\\\\").replace('"', "\\\"")
    );
    put_bucket_policy_test(
        &coord,
        "bucket",
        &format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"other-user"}},"Action":"s3:PutObjectVersionAcl","Resource":"arn:aws:s3:::bucket/*","Condition":{{"StringEquals":{{"s3:x-amz-grant-read":{}}}}}}}]}}"#,
            grant_read_header_json
        ),
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    let err = put_object_acl_test(
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        AclGrants::new(vec![AclGrant::new(
            AclGrantee::CanonicalUser(grantee_canonical_id.clone()),
            AclPermission::Read,
        )]),
        test_helpers::requester("other-user"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    coord
        .put_object_acl(&PutObjectAclRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                Some(put.version_id),
                test_helpers::requester("other-user"),
                None,
            ),
            acl: PutObjectAclInput::Grants(AclGrants::new(vec![AclGrant::new(
                AclGrantee::CanonicalUser(grantee_canonical_id.clone()),
                AclPermission::Read,
            )])),
            policy_context: PutObjectPolicyContext::default().with_acl_grant_headers(
                Some(&grant_read_header),
                None,
                None,
                None,
                None,
            ),
        })
        .unwrap();

    let acl = get_object_acl_test(
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    assert!(grants_contain(
        &acl.acl_grants,
        &AclGrantee::CanonicalUser(grantee_canonical_id),
        AclPermission::Read,
    ));
}

#[test]
fn bucket_policy_cache_invalidates_across_coordinators_on_replace() {
    let tmp = test_util::tempdir();
    let (admin, reader) = setup_coordinators_with_pg_count(tmp.path(), 4);
    admin
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    let tags_xml =
        "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>";
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(tags_xml),
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    put_bucket_policy_test(&admin,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let first = reader
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(first.body.read_all().unwrap(), b"data");

    put_bucket_policy_test(&admin,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":"other-user"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let err = reader
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn lifecycle_sweeper_is_shared_per_storage_node() {
    let tmp = test_util::tempdir();
    let (first, second) = setup_coordinators_with_pg_count(tmp.path(), 4);

    assert!(Arc::ptr_eq(
        &first._lifecycle_sweeper,
        &second._lifecycle_sweeper,
    ));
}

#[test]
fn lifecycle_sweeper_drops_with_last_coordinator() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let lifecycle_sweeper = Arc::downgrade(&coord._lifecycle_sweeper);
    let storage_node = Arc::downgrade(&coord.storage_node);

    drop(coord);

    assert!(lifecycle_sweeper.upgrade().is_none());
    assert!(storage_node.upgrade().is_none());
}

#[test]
fn bucket_lifecycle_cache_invalidates_across_coordinators_on_replace() {
    let tmp = test_util::tempdir();
    let (admin, reader) = setup_coordinators_with_pg_count(tmp.path(), 4);
    admin
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "logs/key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    put_bucket_lifecycle_test(
        &admin,
        "bucket",
        "<LifecycleConfiguration>\
            <Rule>\
                <ID>expire-soon</ID>\
                <Filter><Prefix>logs/</Prefix></Filter>\
                <Status>Enabled</Status>\
                <Expiration><Days>1</Days></Expiration>\
            </Rule>\
        </LifecycleConfiguration>",
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    let first = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "logs/key",
                None,
                test_helpers::requester("owner-a"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap()
        .lifecycle_expiration
        .unwrap();
    assert_eq!(first.rule_id.as_deref(), Some("expire-soon"));

    put_bucket_lifecycle_test(
        &admin,
        "bucket",
        "<LifecycleConfiguration>\
            <Rule>\
                <ID>expire-later</ID>\
                <Filter><Prefix>logs/</Prefix></Filter>\
                <Status>Enabled</Status>\
                <Expiration><Days>30</Days></Expiration>\
            </Rule>\
        </LifecycleConfiguration>",
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    let second = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "logs/key",
                None,
                test_helpers::requester("owner-a"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap()
        .lifecycle_expiration
        .unwrap();
    assert_eq!(second.rule_id.as_deref(), Some("expire-later"));
    assert!(second.expiry_time_millis > first.expiry_time_millis);
}

#[test]
fn put_object_bucket_lifecycle_same_pg_completes_without_deadlock() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_pg_count(tmp.path(), 1);
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Suspended,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    put_bucket_lifecycle_test(
        &coord,
        "bucket",
        "<LifecycleConfiguration>\
            <Rule>\
                <ID>expire-soon</ID>\
                <Filter><Prefix>logs/</Prefix></Filter>\
                <Status>Enabled</Status>\
                <Expiration><Days>1</Days></Expiration>\
            </Rule>\
        </LifecycleConfiguration>",
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "logs/key",
                    test_helpers::requester("owner-a"),
                    None,
                ),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        );
        tx.send(res.map(|result| result.version_id)).unwrap();
    });

    let version_id = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("put_object with bucket lifecycle should not self-deadlock")
        .unwrap();
    assert_eq!(version_id, VersionId::Null);
    handle.join().unwrap();
}

#[test]
fn put_object_bucket_policy_same_pg_completes_without_deadlock() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_pg_count(tmp.path(), 1);
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"owner-a"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    coord.clear_bucket_policy_cache(&trusted_bucket_name("bucket"));

    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "key",
                    test_helpers::requester("owner-a"),
                    None,
                ),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        );
        tx.send(res.map(|result| result.version_id)).unwrap();
    });

    let version_id = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("put_object with bucket policy should not self-deadlock")
        .unwrap();
    assert_eq!(version_id, VersionId::Null);
    handle.join().unwrap();
}

#[test]
fn create_multipart_upload_bucket_lifecycle_same_pg_completes_without_deadlock() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_pg_count(tmp.path(), 1);
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_lifecycle_test(
        &coord,
        "bucket",
        "<LifecycleConfiguration>\
            <Rule>\
                <ID>abort-mpu</ID>\
                <Filter><Prefix>logs/</Prefix></Filter>\
                <Status>Enabled</Status>\
                <AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload>\
            </Rule>\
        </LifecycleConfiguration>",
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = coord.create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "logs/key",
                test_helpers::requester("owner-a"),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        });
        tx.send(res.map(|result| result.lifecycle_abort.is_some()))
            .unwrap();
    });

    let has_abort_header = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("create_multipart_upload with bucket lifecycle should not self-deadlock")
        .unwrap();
    assert!(has_abort_header);
    handle.join().unwrap();
}

#[test]
fn begin_stream_part_bucket_policy_and_abac_same_pg_completes_without_deadlock() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_pg_count(tmp.path(), 1);
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    coord
        .put_bucket_tags(&PutBucketConfigRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("111122223333"),
                None,
            ),
            config:
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
        })
        .unwrap();
    coord
        .put_bucket_abac(&PutBucketAbacRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("111122223333"),
                None,
            ),
            enabled: true,
        })
        .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:BucketTag/security":"public"}}}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("444455556666"),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = coord.begin_stream_part(&BeginStreamPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload.upload_id,
                test_helpers::requester("444455556666"),
                None,
            ),
            part_number: 1,
            policy_context: PutObjectPolicyContext::default(),
            sse_customer: None,
        });
        tx.send(res.map(|result| result.checksum_algorithm))
            .unwrap();
    });

    let checksum_algorithm = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("begin_stream_part should not self-deadlock on same-pg bucket policy/abac state")
        .unwrap();
    assert_eq!(checksum_algorithm, None);
    handle.join().unwrap();
}

#[test]
fn finalize_stream_part_reupload_same_pg_completes_without_deadlock() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_pg_count(tmp.path(), 1);
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = (|| -> Result<(String, String), ServerError> {
            let session_a = coord
                .begin_stream_part(&BeginStreamPartRequest {
                    upload: multipart_object_request_with_expected_owner(
                        "bucket",
                        "key",
                        &upload.upload_id,
                        test_helpers::requester("owner-a"),
                        None,
                    ),
                    part_number: 1,
                    policy_context: PutObjectPolicyContext::default(),
                    sse_customer: None,
                })?
                .session_id;
            let data_a = b"streamed-reupload-a";
            coord
                .append_plaintext_stream_segment_for_test("bucket", "key", &session_a, 0, data_a)?;
            let result_a = coord.finalize_stream_part(FinalizeStreamPartRequest {
                upload: multipart_object_request_with_expected_owner(
                    "bucket",
                    "key",
                    &upload.upload_id,
                    test_helpers::requester("owner-a"),
                    None,
                ),
                session_id: &session_a,
                part_number: 1,
                crc64: checksum::crc64::checksum(data_a),
                total_size: data_a.len() as u64,
                claimed_checksum: None,
                computed_checksum: None,
            })?;

            let session_b = coord
                .begin_stream_part(&BeginStreamPartRequest {
                    upload: multipart_object_request_with_expected_owner(
                        "bucket",
                        "key",
                        &upload.upload_id,
                        test_helpers::requester("owner-a"),
                        None,
                    ),
                    part_number: 1,
                    policy_context: PutObjectPolicyContext::default(),
                    sse_customer: None,
                })?
                .session_id;
            let data_b = b"streamed-reupload-b";
            coord
                .append_plaintext_stream_segment_for_test("bucket", "key", &session_b, 0, data_b)?;
            let result_b = coord.finalize_stream_part(FinalizeStreamPartRequest {
                upload: multipart_object_request_with_expected_owner(
                    "bucket",
                    "key",
                    &upload.upload_id,
                    test_helpers::requester("owner-a"),
                    None,
                ),
                session_id: &session_b,
                part_number: 1,
                crc64: checksum::crc64::checksum(data_b),
                total_size: data_b.len() as u64,
                claimed_checksum: None,
                computed_checksum: None,
            })?;
            Ok((result_a.etag.to_string(), result_b.etag.to_string()))
        })();
        tx.send(res).unwrap();
    });

    let (etag_a, etag_b) = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("finalize_stream_part reupload should not self-deadlock on same-pg topology")
        .unwrap();
    assert_ne!(etag_a, etag_b);
    handle.join().unwrap();
}

#[test]
fn begin_stream_put_bucket_policy_and_abac_same_pg_completes_without_deadlock() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_pg_count(tmp.path(), 1);
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    coord
        .put_bucket_tags(&PutBucketConfigRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("111122223333"),
                None,
            ),
            config:
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
        })
        .unwrap();
    coord
        .put_bucket_abac(&PutBucketAbacRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("111122223333"),
                None,
            ),
            enabled: true,
        })
        .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:BucketTag/security":"public"}}}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = coord.begin_stream_put(&AuthorizePutObjectRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("444455556666"),
                None,
            ),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            tags: None,
            encryption: WriteEncryptionRequest::none(),
        });
        tx.send(res.map(|prepared| prepared.session_id)).unwrap();
    });

    let session_id = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("begin_stream_put should not self-deadlock on same-pg bucket policy/abac state")
        .unwrap();
    assert_eq!(session_id.as_str().len(), storage::SESSION_ID_LEN);
    handle.join().unwrap();
}

#[test]
fn finalize_stream_put_bucket_lifecycle_same_pg_completes_without_deadlock() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_pg_count(tmp.path(), 1);
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_lifecycle_test(
        &coord,
        "bucket",
        "<LifecycleConfiguration>\
            <Rule>\
                <ID>expire-soon</ID>\
                <Filter><Prefix>logs/</Prefix></Filter>\
                <Status>Enabled</Status>\
                <Expiration><Days>1</Days></Expiration>\
            </Rule>\
        </LifecycleConfiguration>",
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    let session_id = begin_stream_put_with_authorized_request_test(
        &coord,
        object_request("bucket", "logs/key", test_helpers::requester("owner-a")),
        NO_PUT_OBJECT_ACL.into(),
        PutObjectPolicyContext::default(),
        WriteEncryptionRequest::none(),
        ObjectLockState::default(),
    )
    .unwrap();
    coord
        .append_stream_put_data(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("logs/key"),
            &session_id,
            0,
            b"data",
            None,
        )
        .unwrap();

    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = coord.finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "logs/key", test_helpers::requester("owner-a")),
            session_id: &session_id,
            crc64: checksum::crc64::checksum(b"data"),
            total_size: 4,
            metadata_blob: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: ActiveWriteEncryptionRef::None,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        });
        tx.send(res.map(|result| result.version_id)).unwrap();
    });

    let version_id = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("finalize_stream_put with bucket lifecycle should not self-deadlock")
        .unwrap();
    assert_eq!(version_id, VersionId::Null);
    handle.join().unwrap();
}

#[test]
fn put_bucket_lifecycle_bucket_policy_same_pg_completes_without_deadlock() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_pg_count(tmp.path(), 1);
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"owner-a"},"Action":"s3:PutLifecycleConfiguration","Resource":"arn:aws:s3:::bucket"}]}"#,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    coord.clear_bucket_policy_cache(&trusted_bucket_name("bucket"));

    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = put_bucket_lifecycle_test(
            &coord,
            "bucket",
            "<LifecycleConfiguration>\
                <Rule>\
                    <ID>expire</ID>\
                    <Filter><Prefix>logs/</Prefix></Filter>\
                    <Status>Enabled</Status>\
                    <Expiration><Days>3</Days></Expiration>\
                </Rule>\
            </LifecycleConfiguration>",
            test_helpers::requester("owner-a"),
            None,
        );
        tx.send(res).unwrap();
    });

    rx.recv_timeout(Duration::from_secs(1))
        .expect("put_bucket_lifecycle with bucket policy should not self-deadlock")
        .unwrap();
    handle.join().unwrap();
}

#[test]
fn put_bucket_acl_bucket_policy_same_pg_completes_without_deadlock() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_pg_count(tmp.path(), 1);
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"owner-a"},"Action":"s3:PutBucketAcl","Resource":"arn:aws:s3:::bucket"}]}"#,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    coord.clear_bucket_policy_cache(&trusted_bucket_name("bucket"));

    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = coord.put_bucket_acl(&PutBucketAclRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("owner-a"),
                None,
            ),
            acl: PutBucketAclInput::Canned(BucketAcl::Private),
            policy_context: PutObjectPolicyContext::default(),
        });
        tx.send(res).unwrap();
    });

    rx.recv_timeout(Duration::from_secs(1))
        .expect("put_bucket_acl with bucket policy should not self-deadlock")
        .unwrap();
    handle.join().unwrap();
}

#[test]
fn get_object_bucket_policy_same_pg_completes_without_deadlock() {
    let tmp = test_util::tempdir();
    let (admin, reader) = setup_coordinators_with_single_pg(tmp.path());
    admin
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&admin,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();
    admin.clear_bucket_policy_cache(&trusted_bucket_name("bucket"));
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_helpers::requester("owner-a"), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
            ),
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
            },
    )
    .unwrap();

    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = reader.get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
        });
        tx.send(res.map(|result| result.body.read_all())).unwrap();
    });

    let body = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("get_object with bucket policy should not self-deadlock")
        .unwrap()
        .unwrap();
    assert_eq!(body, b"data");
    handle.join().unwrap();
}

#[test]
fn get_object_tagging_bucket_policy_same_pg_completes_without_deadlock() {
    let tmp = test_util::tempdir();
    let (admin, reader) = setup_coordinators_with_single_pg(tmp.path());
    admin
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&admin,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:GetObjectTagging","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();
    admin.clear_bucket_policy_cache(&trusted_bucket_name("bucket"));
    let tags_xml =
        "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>";
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(tags_xml),
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = get_object_tags_test(
            &reader,
            "bucket",
            "key",
            None,
            test_helpers::requester("other-user"),
            None,
        );
        tx.send(res).unwrap();
    });

    let tags = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("get_object_tagging with bucket policy should not self-deadlock")
        .unwrap()
        .expect("expected tags");
    assert!(tags.contains("<Key>security</Key>"));
    assert!(tags.contains("<Value>public</Value>"));
    handle.join().unwrap();
}

#[test]
fn put_object_tagging_bucket_policy_same_pg_completes_without_deadlock() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_pg_count(tmp.path(), 1);
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"owner-a"},"Action":"s3:PutObjectTagging","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    coord.clear_bucket_policy_cache(&trusted_bucket_name("bucket"));

    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = put_object_tags_test(
            &coord,
            "bucket",
            "key",
            None,
            "<Tagging><TagSet><Tag><Key>team</Key><Value>storage</Value></Tag></TagSet></Tagging>",
            test_helpers::requester("owner-a"),
            None,
        );
        tx.send(res).unwrap();
    });

    rx.recv_timeout(Duration::from_secs(1))
        .expect("put_object_tagging with bucket policy should not self-deadlock")
        .unwrap();
    handle.join().unwrap();
}

#[test]
fn get_object_retention_bucket_policy_same_pg_completes_without_deadlock() {
    let tmp = test_util::tempdir();
    let (admin, reader) = setup_coordinators_with_single_pg(tmp.path());
    let owner_requester = test_helpers::requester("owner-a");

    admin
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: owner_requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
        .unwrap();
    let put = test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                owner_requester.clone(),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let retention = ObjectRetention {
        mode: ObjectLockMode::Governance,
        retain_until_unix_seconds: Coordinator::current_unix_seconds().unwrap() + 3600,
    };
    put_object_retention_test(
        &admin,
        "bucket",
        "key",
        Some(put.version_id),
        retention,
        false,
        owner_requester.clone(),
    )
    .unwrap();
    put_bucket_policy_test(
        &admin,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:GetObjectRetention","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        owner_requester,
        None,
    )
    .unwrap();
    admin.clear_bucket_policy_cache(&trusted_bucket_name("bucket"));

    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = get_object_retention_test(
            &reader,
            "bucket",
            "key",
            Some(put.version_id),
            test_helpers::requester("other-user"),
        );
        tx.send(res).unwrap();
    });

    let fetched = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("get_object_retention with bucket policy should not self-deadlock")
        .unwrap()
        .expect("expected retention");
    assert_eq!(fetched, retention);
    handle.join().unwrap();
}

#[test]
fn delete_object_object_lock_bucket_policy_same_pg_completes_without_deadlock() {
    let tmp = test_util::tempdir();
    let (admin, deleter) = setup_coordinators_with_single_pg(tmp.path());
    let owner_requester = test_helpers::requester("owner-a");

    admin
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: owner_requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
        .unwrap();
    let put = test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                owner_requester.clone(),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    put_object_retention_test(
        &admin,
        "bucket",
        "key",
        Some(put.version_id),
        ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until_unix_seconds: Coordinator::current_unix_seconds().unwrap() + 3600,
        },
        false,
        owner_requester.clone(),
    )
    .unwrap();
    put_bucket_policy_test(&admin,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":"owner-a"},"Action":"s3:BypassGovernanceRetention","Resource":"arn:aws:s3:::bucket/*"}]}"#,
            owner_requester.clone(), None)
        .unwrap();
    admin.clear_bucket_policy_cache(&trusted_bucket_name("bucket"));

    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = deleter.delete_object(&DeleteObjectRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                Some(put.version_id),
                owner_requester.clone(),
                None,
            ),
            bypass_governance: false,
            cond: NO_DELETE,
        });
        tx.send(res).unwrap();
    });

    let err = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("delete_object with object lock bucket policy should not self-deadlock")
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
    handle.join().unwrap();
}

#[test]
fn get_object_acl_masks_missing_object_for_unauthorized_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let err = get_object_acl_test(
        &coord,
        "bucket",
        "missing",
        None,
        test_helpers::requester("other-user"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn get_object_attributes_bucket_policy_requires_get_object_and_attributes() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:GetObjectAttributes","Resource":"arn:aws:s3:::bucket/key"}]}"#,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    let err = coord
        .get_object_attributes(&GetObjectAttributesRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
            want_parts: false,
            part_number_marker: None,
            max_parts: 0,
            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":["s3:GetObject","s3:GetObjectAttributes"],"Resource":"arn:aws:s3:::bucket/key"}]}"#,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    coord
        .get_object_attributes(&GetObjectAttributesRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
            want_parts: false,
            part_number_marker: None,
            max_parts: 0,
            sse_customer: None,
        })
        .unwrap();
}

#[test]
fn get_object_attributes_version_bucket_policy_requires_get_object_version_and_attributes() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:GetObjectVersionAttributes","Resource":"arn:aws:s3:::bucket/key"}]}"#,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    let err = coord
        .get_object_attributes(&GetObjectAttributesRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                Some(put.version_id),
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
            want_parts: false,
            part_number_marker: None,
            max_parts: 0,
            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":["s3:GetObjectVersion","s3:GetObjectVersionAttributes"],"Resource":"arn:aws:s3:::bucket/key"}]}"#,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    coord
        .get_object_attributes(&GetObjectAttributesRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                Some(put.version_id),
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
            want_parts: false,
            part_number_marker: None,
            max_parts: 0,
            sse_customer: None,
        })
        .unwrap();
}

#[test]
fn get_object_attributes_missing_key_uses_bucket_policy_list_bucket_for_masking() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":["s3:GetObject","s3:GetObjectAttributes"],"Resource":"arn:aws:s3:::bucket/*"}]}"#,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    let err = coord
        .get_object_attributes(&GetObjectAttributesRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "missing",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
            want_parts: false,
            part_number_marker: None,
            max_parts: 0,
            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":["s3:GetObject","s3:GetObjectAttributes"],"Resource":"arn:aws:s3:::bucket/*"},{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket"}]}"#,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    let err = coord
        .get_object_attributes(&GetObjectAttributesRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "missing",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
            want_parts: false,
            part_number_marker: None,
            max_parts: 0,
            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::ObjectNotFound { .. }));
}

#[test]
fn put_object_acl_masks_missing_object_for_unauthorized_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let err = put_object_acl_test(
        &coord,
        "bucket",
        "missing",
        None,
        AclGrants::new(vec![]),
        test_helpers::requester("other-user"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn get_object_acl_masks_missing_version_for_unauthorized_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    let current = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let err = get_object_acl_test(
        &coord,
        "bucket",
        "key",
        Some(VersionId::from_u64(current.version_id.to_u64() + 1000)),
        test_helpers::requester("other-user"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn put_object_acl_masks_missing_version_for_unauthorized_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    let current = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let err = put_object_acl_test(
        &coord,
        "bucket",
        "key",
        Some(VersionId::from_u64(current.version_id.to_u64() + 1000)),
        AclGrants::new(vec![]),
        test_helpers::requester("other-user"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn bucket_owner_enforced_same_account_non_owner_can_discover_missing_object_and_acl() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let bucket_owner = AccountIdentity::new(
        "arn:aws:iam::111122223333:root",
        CanonicalUserId::from_principal("bucket-owner-canonical"),
        "Bucket Owner",
    );
    let same_account_user = AccountIdentity::new(
        "arn:aws:iam::111122223333:user/reader",
        CanonicalUserId::from_principal("same-account-reader-canonical"),
        "Same Account Reader",
    );
    let owner_requester = Requester::authenticated(bucket_owner.clone());
    let same_account_requester = Requester::authenticated_owner_account_admin(same_account_user);

    create_bucket_for_owner_with_flags(
        &coord,
        bucket_owner.principal(),
        bucket_owner.canonical_user_id(),
        "bucket",
        false,
        false,
        false,
    )
    .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        owner_requester.clone(),
        None,
    )
    .unwrap();
    put_bucket_ownership_controls_test(
        &coord,
        "bucket",
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        owner_requester.clone(),
        None,
    )
    .unwrap();

    let current = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                owner_requester.clone(),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let object = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                same_account_requester.clone(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(object.body.read_all().unwrap(), b"data");

    get_object_acl_test(
        &coord,
        "bucket",
        "key",
        None,
        same_account_requester.clone(),
        None,
    )
    .unwrap();

    let missing_object = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "missing",
                None,
                same_account_requester.clone(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(missing_object, ServerError::ObjectNotFound { .. }));

    let missing_acl = get_object_acl_test(
        &coord,
        "bucket",
        "missing",
        None,
        same_account_requester.clone(),
        None,
    )
    .unwrap_err();
    assert!(matches!(missing_acl, ServerError::ObjectNotFound { .. }));

    let missing_attrs = coord
        .get_object_attributes(&GetObjectAttributesRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "missing",
                None,
                same_account_requester.clone(),
                None,
            ),
            cond: NO_READ,
            want_parts: false,
            part_number_marker: None,
            max_parts: 0,
            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(missing_attrs, ServerError::AccessDenied));

    let missing_version = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                Some(VersionId::from_u64(current.version_id.to_u64() + 1000)),
                same_account_requester.clone(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(
        missing_version,
        ServerError::VersionNotFound { .. }
    ));

    let missing_acl_version = get_object_acl_test(
        &coord,
        "bucket",
        "key",
        Some(VersionId::from_u64(current.version_id.to_u64() + 1000)),
        same_account_requester,
        None,
    )
    .unwrap_err();
    assert!(matches!(
        missing_acl_version,
        ServerError::VersionNotFound { .. }
    ));
}

#[test]
fn bucket_owner_enforced_same_account_standard_requester_cannot_read_acl_attributes_or_discover() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let bucket_owner = AccountIdentity::new(
        "arn:aws:iam::111122223333:root",
        CanonicalUserId::from_principal("bucket-owner-standard-canonical"),
        "Bucket Owner",
    );
    let same_account_user = AccountIdentity::new(
        "arn:aws:iam::111122223333:user/reader",
        CanonicalUserId::from_principal("same-account-standard-canonical"),
        "Same Account Reader",
    );
    let owner_requester = Requester::authenticated(bucket_owner.clone());
    let same_account_requester = Requester::authenticated(same_account_user);

    create_bucket_for_owner_with_flags(
        &coord,
        bucket_owner.principal(),
        bucket_owner.canonical_user_id(),
        "bucket",
        false,
        false,
        false,
    )
    .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        owner_requester.clone(),
        None,
    )
    .unwrap();
    put_bucket_ownership_controls_test(
        &coord,
        "bucket",
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        owner_requester.clone(),
        None,
    )
    .unwrap();

    let current = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                owner_requester.clone(),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(
                "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>",
            ),
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                same_account_requester.clone(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                Some(current.version_id),
                same_account_requester.clone(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let err = coord
        .get_object_attributes(&GetObjectAttributesRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                same_account_requester.clone(),
                None,
            ),
            cond: NO_READ,
            want_parts: false,
            part_number_marker: None,
            max_parts: 0,
            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let err = coord
        .get_object_attributes(&GetObjectAttributesRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                Some(current.version_id),
                same_account_requester.clone(),
                None,
            ),
            cond: NO_READ,
            want_parts: false,
            part_number_marker: None,
            max_parts: 0,
            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let err =
        get_bucket_acl_test(&coord, "bucket", same_account_requester.clone(), None).unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let err = get_object_acl_test(
        &coord,
        "bucket",
        "key",
        None,
        same_account_requester.clone(),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let err = get_object_acl_test(
        &coord,
        "bucket",
        "key",
        Some(current.version_id),
        same_account_requester.clone(),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "missing",
                None,
                same_account_requester.clone(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let err = get_object_acl_test(
        &coord,
        "bucket",
        "missing",
        None,
        same_account_requester.clone(),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let err = coord
        .get_object_attributes(&GetObjectAttributesRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "missing",
                None,
                same_account_requester,
                None,
            ),
            cond: NO_READ,
            want_parts: false,
            part_number_marker: None,
            max_parts: 0,
            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn bucket_owner_enforced_same_account_standard_requester_cannot_manage_object_tags() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let bucket_owner = AccountIdentity::new(
        "arn:aws:iam::111122223333:root",
        CanonicalUserId::from_principal("bucket-owner-tag-canonical"),
        "Bucket Owner",
    );
    let same_account_user = AccountIdentity::new(
        "arn:aws:iam::111122223333:user/tagger",
        CanonicalUserId::from_principal("same-account-tagger-canonical"),
        "Same Account Tagger",
    );
    let owner_requester = Requester::authenticated(bucket_owner.clone());
    let same_account_requester = Requester::authenticated(same_account_user);

    create_bucket_for_owner_with_flags(
        &coord,
        bucket_owner.principal(),
        bucket_owner.canonical_user_id(),
        "bucket",
        false,
        false,
        false,
    )
    .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        owner_requester.clone(),
        None,
    )
    .unwrap();
    put_bucket_ownership_controls_test(
        &coord,
        "bucket",
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        owner_requester.clone(),
        None,
    )
    .unwrap();

    let current = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                owner_requester.clone(),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(
                "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>",
            ),
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let err = get_object_tags_test(
        &coord,
        "bucket",
        "key",
        None,
        same_account_requester.clone(),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let err = get_object_tags_test(
        &coord,
        "bucket",
        "key",
        Some(current.version_id),
        same_account_requester.clone(),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let err = put_object_tags_test(
        &coord,
        "bucket",
        "key",
        None,
        "<Tagging><TagSet><Tag><Key>env</Key><Value>test</Value></Tag></TagSet></Tagging>",
        same_account_requester.clone(),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let err = put_object_tags_test(
        &coord,
        "bucket",
        "key",
        Some(current.version_id),
        "<Tagging><TagSet><Tag><Key>env</Key><Value>test</Value></Tag></TagSet></Tagging>",
        same_account_requester.clone(),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let err = coord
        .delete_object_tags(&object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            same_account_requester.clone(),
            None,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let err = coord
        .delete_object_tags(&object_version_request_with_expected_owner(
            "bucket",
            "key",
            Some(current.version_id),
            same_account_requester,
            None,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn bucket_owner_enforced_shared_canonical_standard_requester_is_not_object_owner() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let shared_canonical_id = CanonicalUserId::from_principal("111122223333");
    let bucket_owner = AccountIdentity::new(
        "arn:aws:iam::111122223333:root",
        shared_canonical_id.clone(),
        "Bucket Owner",
    );
    let shared_principal = AccountIdentity::new(
        "arn:aws:iam::111122223333:user/shared",
        shared_canonical_id,
        "Shared Principal",
    );
    let owner_requester = Requester::authenticated(bucket_owner.clone());
    let shared_standard_requester = Requester::authenticated(shared_principal.clone());
    let shared_admin_requester = Requester::authenticated_owner_account_admin(shared_principal);

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: owner_requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        owner_requester.clone(),
        None,
    )
    .unwrap();

    let current = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                owner_requester.clone(),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(
                "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>",
            ),
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let owner_object = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                owner_requester.clone(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(owner_object.body.read_all().unwrap(), b"data");
    get_object_acl_test(
        &coord,
        "bucket",
        "key",
        Some(current.version_id),
        owner_requester.clone(),
        None,
    )
    .unwrap();
    assert!(get_object_tags_test(
        &coord,
        "bucket",
        "key",
        Some(current.version_id),
        owner_requester,
        None,
    )
    .unwrap()
    .is_some());
    coord
        .get_object_attributes(&GetObjectAttributesRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                Some(current.version_id),
                Requester::authenticated(bucket_owner.clone()),
                None,
            ),
            cond: NO_READ,
            want_parts: false,
            part_number_marker: None,
            max_parts: 0,
            sse_customer: None,
        })
        .unwrap();

    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                shared_standard_requester.clone(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
    let err = get_object_acl_test(
        &coord,
        "bucket",
        "key",
        Some(current.version_id),
        shared_standard_requester.clone(),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
    let err = get_object_tags_test(
        &coord,
        "bucket",
        "key",
        Some(current.version_id),
        shared_standard_requester.clone(),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
    let err = coord
        .get_object_attributes(&GetObjectAttributesRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                Some(current.version_id),
                shared_standard_requester.clone(),
                None,
            ),
            cond: NO_READ,
            want_parts: false,
            part_number_marker: None,
            max_parts: 0,
            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let shared_admin_object = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                shared_admin_requester.clone(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(shared_admin_object.body.read_all().unwrap(), b"data");
    get_object_acl_test(
        &coord,
        "bucket",
        "key",
        Some(current.version_id),
        shared_admin_requester.clone(),
        None,
    )
    .unwrap();
    assert!(get_object_tags_test(
        &coord,
        "bucket",
        "key",
        Some(current.version_id),
        shared_admin_requester.clone(),
        None,
    )
    .unwrap()
    .is_some());
    let err = coord
        .get_object_attributes(&GetObjectAttributesRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                Some(current.version_id),
                shared_admin_requester,
                None,
            ),
            cond: NO_READ,
            want_parts: false,
            part_number_marker: None,
            max_parts: 0,
            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn bucket_owner_enforced_admin_tagging_preserves_delete_marker_and_missing_version_errors() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let bucket_owner = AccountIdentity::new(
        "arn:aws:iam::111122223333:root",
        CanonicalUserId::from_principal("bucket-owner-delete-marker-canonical"),
        "Bucket Owner",
    );
    let same_account_user = AccountIdentity::new(
        "arn:aws:iam::111122223333:user/tag-admin",
        CanonicalUserId::from_principal("same-account-tag-admin-canonical"),
        "Same Account Tag Admin",
    );
    let owner_requester = Requester::authenticated(bucket_owner.clone());
    let admin_requester = Requester::authenticated_owner_account_admin(same_account_user);

    create_bucket_for_owner_with_flags(
        &coord,
        bucket_owner.principal(),
        bucket_owner.canonical_user_id(),
        "bucket",
        false,
        false,
        false,
    )
    .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        owner_requester.clone(),
        None,
    )
    .unwrap();
    put_bucket_ownership_controls_test(
        &coord,
        "bucket",
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        owner_requester.clone(),
        None,
    )
    .unwrap();

    let current = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", owner_requester, None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(
                "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>",
            ),
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let delete_marker = coord
        .delete_object(&DeleteObjectRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                admin_requester.clone(),
                None,
            ),
            cond: NO_DELETE,
            bypass_governance: false,
        })
        .unwrap();
    let delete_marker_version = delete_marker.version_id;

    let err = get_object_tags_test(
        &coord,
        "bucket",
        "key",
        Some(delete_marker_version),
        admin_requester.clone(),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::MethodNotAllowed));

    let err = coord
        .delete_object_tags(&object_version_request_with_expected_owner(
            "bucket",
            "key",
            Some(delete_marker_version),
            admin_requester.clone(),
            None,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::MethodNotAllowed));

    let missing_version = VersionId::from_u64(current.version_id.to_u64() + 1000);

    let err = get_object_tags_test(
        &coord,
        "bucket",
        "key",
        Some(missing_version),
        admin_requester.clone(),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::VersionNotFound { .. }));

    let err = coord
        .delete_object_tags(&object_version_request_with_expected_owner(
            "bucket",
            "key",
            Some(missing_version),
            admin_requester,
            None,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::VersionNotFound { .. }));
}

#[test]
fn get_object_tags_rejects_public_read_for_anonymous() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    let tags_xml =
        "<Tagging><TagSet><Tag><Key>env</Key><Value>public</Value></Tag></TagSet></Tagging>";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"public",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(tags_xml),
            cond: NO_WRITE,

            acl: PutObjectAcl::PublicRead.into(),
        },
    )
    .unwrap();

    let err = get_object_tags_test(&coord, "bucket", "key", None, Requester::anonymous(), None)
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn authorize_get_object_tags_rejects_public_read_for_anonymous() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    let tags_xml =
        "<Tagging><TagSet><Tag><Key>env</Key><Value>public</Value></Tag></TagSet></Tagging>";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"public",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(tags_xml),
            cond: NO_WRITE,
            acl: PutObjectAcl::PublicRead.into(),
        },
    )
    .unwrap();

    let err = coord
        .authorize_get_object_tags(&object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            Requester::anonymous(),
            None,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn bucket_owner_can_manage_object_tags_for_cross_owned_object() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner_canonical_id = CanonicalUserId::from_principal("owner-a");
    create_bucket_for_owner_with_flags(
        &coord,
        "owner-a",
        &owner_canonical_id,
        "bucket",
        false,
        true,
        false,
    )
    .unwrap();
    let tags_xml =
        "<Tagging><TagSet><Tag><Key>env</Key><Value>writer</Value></Tag></TagSet></Tagging>";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("writer-a"),
                None,
            ),
            data: b"writer-owned",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let denied = put_object_tags_test(
        &coord,
        "bucket",
        "key",
        None,
        tags_xml,
        test_helpers::requester("writer-a"),
        None,
    )
    .unwrap_err();
    assert!(matches!(denied, ServerError::AccessDenied));

    put_object_tags_test(
        &coord,
        "bucket",
        "key",
        None,
        tags_xml,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    let tags = get_object_tags_test(
        &coord,
        "bucket",
        "key",
        None,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap()
    .expect("expected tag set");
    assert!(tags.contains("<Key>env</Key>"));
    assert!(tags.contains("<Value>writer</Value>"));
}

#[test]
fn delete_object_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let err = coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            test_helpers::requester("other-user"),
            false,
            NO_DELETE,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn delete_objects_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let entries = vec![DeleteEntry {
        key: trusted_object_key("key"),
        version_id: None,
        cond: DeleteCondition::None,
    }];
    let result = coord
        .delete_objects(&DeleteObjectsRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("other-user"),
                None,
            ),
            entries: &entries,
            bypass_governance: false,
        })
        .unwrap();
    assert!(result.deleted.is_empty());
    assert_eq!(result.errors.len(), 1);
    assert_eq!(result.errors[0].key, "key");
    assert_eq!(result.errors[0].code, "AccessDenied");
}

#[test]
fn authorize_delete_objects_entry_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let entries = vec![DeleteEntry {
        key: trusted_object_key("key"),
        version_id: None,
        cond: DeleteCondition::None,
    }];
    let err = coord
        .authorize_delete_objects_entry(
            &DeleteObjectsRequest {
                bucket: bucket_request_with_expected_owner(
                    "bucket",
                    test_helpers::requester("other-user"),
                    None,
                ),
                entries: &entries,
                bypass_governance: false,
            },
            &entries[0],
        )
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn get_object_rejects_private_read_for_non_owner() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"secret",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn get_object_range_rejects_private_read_for_non_owner() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"secret",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let err = coord
        .get_object_range(&GetObjectRangeRequest {
            range: ByteRange::Range { start: 0, end: 0 },
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn get_object_allows_object_owner_without_bucket_read_access() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner_canonical_id = CanonicalUserId::from_principal("owner-a");
    create_bucket_for_owner_with_flags(
        &coord,
        "owner-a",
        &owner_canonical_id,
        "bucket",
        false,
        true,
        false,
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("writer-a"),
                None,
            ),
            data: b"writer-owned",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_helpers::requester("writer-a"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"writer-owned");
}

#[test]
fn get_object_rejects_bucket_owner_when_private_object_owned_by_other_principal() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner_canonical_id = CanonicalUserId::from_principal("owner-a");
    create_bucket_for_owner_with_flags(
        &coord,
        "owner-a",
        &owner_canonical_id,
        "bucket",
        false,
        true,
        false,
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("writer-a"),
                None,
            ),
            data: b"writer-owned",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_helpers::requester("owner-a"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn get_object_allows_public_read_for_anonymous() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"public",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: PutObjectAcl::PublicRead.into(),
        },
    )
    .unwrap();

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                Requester::anonymous(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"public");
}

#[test]
fn get_object_ignores_public_read_acl_when_ignore_public_acls_enabled() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"public",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: PutObjectAcl::PublicRead.into(),
        },
    )
    .unwrap();
    put_bucket_public_access_block_test(&coord,
            "bucket",
            "<PublicAccessBlockConfiguration><BlockPublicAcls>false</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls><BlockPublicPolicy>false</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                Requester::anonymous(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn get_object_bucket_owner_enforced_disables_legacy_public_read_acl() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"public",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: PutObjectAcl::PublicRead.into(),
        },
    )
    .unwrap();

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                Requester::anonymous(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"public");

    put_bucket_ownership_controls_test(
        &coord,
        "bucket",
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                Requester::anonymous(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn get_object_ignores_authenticated_read_acl_when_ignore_public_acls_enabled() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"authenticated-read",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: PutObjectAcl::AuthenticatedRead.into(),
        },
    )
    .unwrap();

    let object = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(object.body.read_all().unwrap(), b"authenticated-read");

    put_bucket_public_access_block_test(&coord,
            "bucket",
            "<PublicAccessBlockConfiguration><BlockPublicAcls>false</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls><BlockPublicPolicy>false</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_helpers::requester("other-user"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let owner_object = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_helpers::requester("owner-a"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(owner_object.body.read_all().unwrap(), b"authenticated-read");
}

#[test]
fn list_objects_ignores_authenticated_read_bucket_acl_when_ignore_public_acls_enabled() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    put_bucket_acl_test(
        &coord,
        "bucket",
        AclGrants::new(vec![AclGrant::new(
            AclGrantee::AuthenticatedUsers,
            AclPermission::Read,
        )]),
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    let before = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("other-user"),
                None,
            ),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
        })
        .unwrap();
    assert_eq!(before.objects.len(), 1);
    assert_eq!(before.objects[0].key, "key");

    put_bucket_public_access_block_test(&coord,
            "bucket",
            "<PublicAccessBlockConfiguration><BlockPublicAcls>false</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls><BlockPublicPolicy>false</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let err = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("other-user"),
                None,
            ),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn head_bucket_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let err = coord
        .head_bucket(&BucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("other-user"),
            expected_bucket_owner: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn authorize_head_bucket_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let err = coord
        .authorize_head_bucket(&bucket_request_with_expected_owner(
            "bucket",
            test_helpers::requester("owner-b"),
            None,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn head_bucket_allows_matching_expected_bucket_owner() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();

    let info = coord
        .head_bucket(&BucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("111122223333"),
            expected_bucket_owner: None,
        })
        .unwrap();
    Coordinator::ensure_expected_bucket_owner(&info, Some("111122223333")).unwrap();
    assert_eq!(info.name, "bucket");
}

#[test]
fn head_bucket_rejects_mismatched_expected_bucket_owner() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();

    let info = coord
        .head_bucket(&BucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("111122223333"),
            expected_bucket_owner: None,
        })
        .unwrap();
    let err = Coordinator::ensure_expected_bucket_owner(&info, Some("999988887777")).unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn head_bucket_allows_public_read_for_anonymous() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", true)
        .unwrap();

    let info = coord
        .head_bucket(&BucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::anonymous(),
            expected_bucket_owner: None,
        })
        .unwrap();
    assert_eq!(info.name, "bucket");
}

#[test]
fn list_objects_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let err = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("other-user"),
                None,
            ),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn authorize_list_objects_v2_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let err = coord
        .authorize_list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("other-user"),
                None,
            ),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn authorize_list_objects_v2_allows_explicit_bucket_policy_allow() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    coord
        .authorize_list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("other-user"),
                None,
            ),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
        })
        .unwrap();
}

#[test]
fn list_objects_allows_explicit_bucket_policy_allow() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    put_bucket_policy_test(&coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let result = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("other-user"),
                None,
            ),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
        })
        .unwrap();
    assert_eq!(result.objects.len(), 1);
    assert_eq!(result.objects[0].key, "key");
}

#[test]
fn authorize_list_object_versions_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let err = coord
        .authorize_list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("other-user"),
                None,
            ),
            prefix: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 1000,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn list_objects_bucket_policy_deny_overrides_public_read_acl() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", true)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    put_bucket_policy_test(&coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let err = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", Requester::anonymous(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn delete_bucket_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let err = coord
        .delete_bucket(&BucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("other-user"),
            expected_bucket_owner: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn authorize_delete_bucket_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let err = coord
        .authorize_delete_bucket(&bucket_request_with_expected_owner(
            "bucket",
            test_helpers::requester("owner-b"),
            None,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn create_multipart_upload_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let err = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("other-user"),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn upload_part_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let err = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload.upload_id,
                test_helpers::requester("other-user"),
                None,
            ),
            part_number: 1,
            data: b"data",
            claimed_checksum: None,
            sse_customer: None,
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn list_object_versions_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let metadata = MetadataBlob::default();
    let system_metadata = SystemMetadata::default();
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            object: object_request("bucket", "key", test_helpers::requester("111122223333")),
            data: b"one",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: &WriteCondition::None,
            acl: PutObjectWriteAcl::None,
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            encryption: WriteEncryptionRequest::none(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            object: object_request("bucket", "key", test_helpers::requester("111122223333")),
            data: b"two",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: &WriteCondition::None,
            acl: PutObjectWriteAcl::None,
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            encryption: WriteEncryptionRequest::none(),
        },
    )
    .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:ListBucketVersions","Resource":"arn:aws:s3:::bucket"}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    let result = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("444455556666"),
                None,
            ),
            prefix: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 1000,
        })
        .unwrap();
    assert!(result.versions.len() >= 2);
}

#[test]
fn create_multipart_upload_rejects_acl_on_bucket_owner_enforced_bucket() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_ownership_controls_test(&coord,
            "bucket",
            "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let err = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: PutObjectAcl::PublicRead.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessControlListNotSupported));
}

#[test]
fn complete_multipart_upload_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let err = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload.upload_id,
                test_helpers::requester("other-user"),
                None,
            ),
            parts: &[],
            claimed_checksum: None,

            expected_object_size: None,

            cond: &WriteCondition::default(),

            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn authorize_complete_multipart_upload_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let err = coord
        .authorize_complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload.upload_id,
                test_helpers::requester("other-user"),
                None,
            ),
            parts: &[],
            claimed_checksum: None,

            expected_object_size: None,

            cond: &WriteCondition::default(),

            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn multipart_upload_managed_encryption_policy_context_enables_complete_multipart_write() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_policy_test(&coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*"},{"Effect":"Deny","Principal":{"AWS":"other-user"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"Null":{"s3:x-amz-server-side-encryption":"true"}}}]}"#,
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("other-user"),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::managed(ManagedEncryptionAlgorithm::Aes256),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let bucket = coord.unchecked_active_bucket_summary("bucket").unwrap();
    let policy = coord.cached_bucket_policy(&bucket).unwrap();
    let meta_pg = coord
        .storage_node
        .get_pg(coord.object_pg_id("bucket", "key"))
        .unwrap();
    let upload = meta_pg.get_multipart_upload(&upload.upload_id).unwrap();
    let requester = test_helpers::requester("other-user");

    assert!(!coord
        .requester_can_write_multipart_upload_with_bucket_policy(
            &requester,
            &bucket,
            None,
            &upload,
            PutObjectPolicyContext::default(),
            policy.as_deref(),
        )
        .unwrap());
    assert!(coord
        .requester_can_write_multipart_upload_with_bucket_policy(
            &requester,
            &bucket,
            None,
            &upload,
            Coordinator::with_multipart_upload_managed_encryption_policy_context(
                PutObjectPolicyContext::default(),
                &upload,
            ),
            policy.as_deref(),
        )
        .unwrap());
}

#[test]
fn abort_multipart_upload_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let err = coord
        .abort_multipart_upload(&multipart_object_request_with_expected_owner(
            "bucket",
            "key",
            &upload.upload_id,
            test_helpers::requester("other-user"),
            None,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn authorize_abort_multipart_upload_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let err = coord
        .authorize_abort_multipart_upload(&multipart_object_request_with_expected_owner(
            "bucket",
            "key",
            &upload.upload_id,
            test_helpers::requester("other-user"),
            None,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn begin_stream_put_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let err = begin_stream_put_with_authorized_request_test(
        &coord,
        object_request_with_expected_owner(
            "bucket",
            "key",
            test_helpers::requester("other-user"),
            None,
        ),
        NO_PUT_OBJECT_ACL.into(),
        PutObjectPolicyContext::default(),
        WriteEncryptionRequest::none(),
        ObjectLockState::default(),
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn begin_stream_put_rejects_sse_s3_without_provider() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_managed_key_provider(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let err = begin_stream_put_with_authorized_request_test(
        &coord,
        object_request_with_expected_owner("bucket", "key", test_requester(), None),
        NO_PUT_OBJECT_ACL.into(),
        PutObjectPolicyContext::default(),
        WriteEncryptionRequest::managed(ManagedEncryptionAlgorithm::Aes256),
        ObjectLockState::default(),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ServerError::NotImplemented { ref feature }
        if feature == "SSE-S3 requires managed key provider configuration"
    ));
}

#[test]
fn begin_stream_part_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let err = coord
        .begin_stream_part(&BeginStreamPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload.upload_id,
                test_helpers::requester("other-user"),
                None,
            ),
            part_number: 1,
            policy_context: PutObjectPolicyContext::default(),
            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn authorize_begin_stream_part_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let err = coord
        .authorize_begin_stream_part(&BeginStreamPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload.upload_id,
                test_helpers::requester("other-user"),
                None,
            ),
            part_number: 1,
            policy_context: PutObjectPolicyContext::default(),
            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn begin_stream_put_rejects_acl_on_bucket_owner_enforced_bucket() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_ownership_controls_test(&coord,
            "bucket",
            "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
            test_helpers::requester("owner-a"), None)
        .unwrap();

    let err = begin_stream_put_with_authorized_request_test(
        &coord,
        object_request_with_expected_owner(
            "bucket",
            "key",
            test_helpers::requester("owner-a"),
            None,
        ),
        PutObjectAcl::PublicRead.into(),
        PutObjectPolicyContext::default(),
        WriteEncryptionRequest::none(),
        ObjectLockState::default(),
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessControlListNotSupported));
}

#[test]
fn begin_stream_put_rejects_object_lock_headers_on_plain_bucket() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let err = begin_stream_put_with_authorized_request_test(
        &coord,
        object_request_with_expected_owner("bucket", "key", test_requester(), None),
        NO_PUT_OBJECT_ACL.into(),
        PutObjectPolicyContext::default(),
        WriteEncryptionRequest::none(),
        ObjectLockState {
            retention: None,
            legal_hold: StoredLegalHoldStatus::On,
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::InvalidRequest { .. }));
}

#[test]
fn get_if_match_returns_object() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let cond = ReadCondition {
        if_match: Some(put.etag.into()),
        ..Default::default()
    };
    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: &cond,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"data");
}

#[test]
fn get_if_match_wrong_etag_returns_412() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let cond = ReadCondition {
        if_match: Some("\"0000000000000000\"".into()),
        ..Default::default()
    };
    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: &cond,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::PreconditionFailed));
}

#[test]
fn get_if_none_match_returns_304() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let cond = ReadCondition {
        if_none_match: Some(put.etag.into()),
        ..Default::default()
    };
    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: &cond,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::NotModified { .. }));
}

#[test]
fn head_if_none_match_returns_304() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let cond = ReadCondition {
        if_none_match: Some(put.etag.into()),
        ..Default::default()
    };
    let err = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: &cond,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::NotModified { .. }));
}

#[test]
fn delete_if_match_succeeds() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let cond = DeleteCondition::IfMatch(put.etag.into());
    coord
        .delete_object(&DeleteObjectRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            bypass_governance: false,
            cond: &cond,
        })
        .unwrap();
    assert!(coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None
            ),
            cond: NO_READ,
        })
        .is_err());
}

#[test]
fn delete_if_match_wrong_etag_returns_412() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let cond = DeleteCondition::IfMatch("\"0000000000000000\"".into());
    let err = coord
        .delete_object(&DeleteObjectRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            bypass_governance: false,
            cond: &cond,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::PreconditionFailed));
}

#[test]
fn delete_version_if_match_is_not_implemented() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();

    let v1 = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"v1",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"v2",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let cond = DeleteCondition::IfMatch("\"0000000000000000\"".into());
    let err = coord
        .delete_object(&DeleteObjectRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                Some(v1.version_id),
                test_requester(),
                None,
            ),
            bypass_governance: false,
            cond: &cond,
        })
        .unwrap_err();
    assert!(matches!(
        err,
        ServerError::NotImplemented { ref feature }
        if feature == "conditional delete with versionId"
    ));

    let v1_obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                Some(v1.version_id),
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(v1_obj.body.read_all().unwrap(), b"v1");
}

#[test]
fn delete_objects_if_match_per_entry() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let p1 = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key1", test_requester(), None),
            data: b"data1",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key2", test_requester(), None),
            data: b"data2",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // Use key1's etag for both entries; key2 will fail the condition.
    let entries = vec![
        DeleteEntry {
            key: trusted_object_key("key1"),
            version_id: None,
            cond: DeleteCondition::IfMatch(p1.etag.clone().into()),
        },
        DeleteEntry {
            key: trusted_object_key("key2"),
            version_id: None,
            cond: DeleteCondition::IfMatch(p1.etag.into()),
        },
    ];
    let result = coord
        .delete_objects(&DeleteObjectsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            entries: &entries,
            bypass_governance: false,
        })
        .unwrap();
    assert_eq!(result.deleted.len(), 1);
    assert_eq!(result.deleted[0].key, "key1");
    assert_eq!(result.errors.len(), 1);
    assert_eq!(result.errors[0].key, "key2");
}

#[test]
fn range_get_if_match_returns_data() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"Hello, World!",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let cond = ReadCondition {
        if_match: Some(put.etag.into()),
        ..Default::default()
    };
    let result = coord
        .get_object_range(&GetObjectRangeRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            range: ByteRange::Range { start: 0, end: 4 },
            cond: &cond,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), b"Hello");
}

// ── CopyObject tests ──────────────────────────────────────────────
