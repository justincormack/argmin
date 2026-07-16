use super::test_helpers;
use super::test_support::*;
use super::test_topology::*;
use super::*;
use crate::conditional::{ReadCondition, SpecificEtag, WriteCondition};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

struct MultipartMetadataRaceSync {
    snapshot_reached: Arc<Barrier>,
    snapshot_resume: Arc<Barrier>,
    delete_reached: Arc<Barrier>,
    delete_resume: Arc<Barrier>,
    _serial_guard: MutexGuard<'static, ()>,
    _guard: ReclamationTestHookGuard,
}

struct ObjectReadSnapshotRaceSync {
    snapshot_reached: Arc<Barrier>,
    snapshot_resume: Arc<Barrier>,
    _serial_guard: MutexGuard<'static, ()>,
    _guard: ReclamationTestHookGuard,
}

fn install_object_read_snapshot_race_hook(bucket: &str, key: &str) -> ObjectReadSnapshotRaceSync {
    let serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let snapshot_reached = Arc::new(Barrier::new(2));
    let snapshot_resume = Arc::new(Barrier::new(2));
    let snapshot_reached_hook = Arc::clone(&snapshot_reached);
    let snapshot_resume_hook = Arc::clone(&snapshot_resume);
    let guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), key.to_string())),
        after_object_read_snapshot: Some(Arc::new(move || {
            snapshot_reached_hook.wait();
            snapshot_resume_hook.wait();
        })),
        ..ReclamationTestHooks::default()
    });
    ObjectReadSnapshotRaceSync {
        snapshot_reached,
        snapshot_resume,
        _serial_guard: serial,
        _guard: guard,
    }
}

fn install_multipart_metadata_race_hooks(bucket: &str, key: &str) -> MultipartMetadataRaceSync {
    let serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let snapshot_reached = Arc::new(Barrier::new(2));
    let snapshot_resume = Arc::new(Barrier::new(2));
    let delete_reached = Arc::new(Barrier::new(2));
    let delete_resume = Arc::new(Barrier::new(2));
    let snapshot_reached_hook = Arc::clone(&snapshot_reached);
    let snapshot_resume_hook = Arc::clone(&snapshot_resume);
    let delete_reached_hook = Arc::clone(&delete_reached);
    let delete_resume_hook = Arc::clone(&delete_resume);
    let guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), key.to_string())),
        after_multipart_snapshot: Some(Arc::new(move || {
            snapshot_reached_hook.wait();
            snapshot_resume_hook.wait();
        })),
        after_multipart_delete_metadata: Some(Arc::new(move || {
            delete_reached_hook.wait();
            delete_resume_hook.wait();
        })),
        ..ReclamationTestHooks::default()
    });
    MultipartMetadataRaceSync {
        snapshot_reached,
        snapshot_resume,
        delete_reached,
        delete_resume,
        _serial_guard: serial,
        _guard: guard,
    }
}

struct ObjectSegmentsDeleteRaceSync {
    first_segment_reached: Arc<Barrier>,
    first_segment_resume: Arc<Barrier>,
    delete_reached: Arc<Barrier>,
    delete_resume: Arc<Barrier>,
    _serial_guard: MutexGuard<'static, ()>,
    _guard: ReclamationTestHookGuard,
}

fn install_object_segments_delete_race_hooks(
    bucket: &str,
    key: &str,
) -> ObjectSegmentsDeleteRaceSync {
    let serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let first_segment_reached = Arc::new(Barrier::new(2));
    let first_segment_resume = Arc::new(Barrier::new(2));
    let delete_reached = Arc::new(Barrier::new(2));
    let delete_resume = Arc::new(Barrier::new(2));
    let first_segment_reached_hook = Arc::clone(&first_segment_reached);
    let first_segment_resume_hook = Arc::clone(&first_segment_resume);
    let delete_reached_hook = Arc::clone(&delete_reached);
    let delete_resume_hook = Arc::clone(&delete_resume);
    let guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), key.to_string())),
        after_object_segments_first_segment: Some(Arc::new(move || {
            first_segment_reached_hook.wait();
            first_segment_resume_hook.wait();
        })),
        after_object_segments_delete_metadata: Some(Arc::new(move || {
            delete_reached_hook.wait();
            delete_resume_hook.wait();
        })),
        ..ReclamationTestHooks::default()
    });
    ObjectSegmentsDeleteRaceSync {
        first_segment_reached,
        first_segment_resume,
        delete_reached,
        delete_resume,
        _serial_guard: serial,
        _guard: guard,
    }
}

#[test]
fn copy_object_basic() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let headers = [("Content-Type", "text/plain")];
    let metadata = MetadataBlob::from_headers(&headers).unwrap();
    let system_metadata = SystemMetadata::from_headers(&headers).unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"hello copy",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let result = coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();
    assert!(!result.etag.is_empty());

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "dst",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"hello copy");
}

#[test]
fn copy_object_does_not_copy_website_redirect_without_explicit_override() {
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
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"hello copy",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::from_headers(&[
                ("Content-Type", "text/plain"),
                ("x-amz-website-redirect-location", "/docs/source.html"),
            ])
            .unwrap(),
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "dst",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"hello copy");
    assert_eq!(obj.system_metadata.website_redirect_location(), None);
}

#[test]
fn copy_object_explicit_website_redirect_override_persists() {
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
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"hello copy",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::from_headers(&[("Content-Type", "text/plain")])
                .unwrap(),
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: Some(
                s3_types::WebsiteRedirectLocation::new("/docs/destination.html").unwrap(),
            ),
            tagging: TaggingDirective::Copy,
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();

    let obj = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "dst",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        obj.system_metadata
            .website_redirect_location()
            .map(|value| value.as_str()),
        Some("/docs/destination.html")
    );
}

#[test]
fn copy_object_same_key_explicit_website_redirect_override_is_allowed() {
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
            data: b"same-key-body",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "key", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "key",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: Some(
                s3_types::WebsiteRedirectLocation::new("/docs/changed.html").unwrap(),
            ),
            tagging: TaggingDirective::Copy,
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();

    let obj = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        obj.system_metadata
            .website_redirect_location()
            .map(|value| value.as_str()),
        Some("/docs/changed.html")
    );
}

#[test]
fn copy_object_explicit_sse_s3_destination_preserves_managed_encryption() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_sse_c(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    enable_bucket_sse_c_test(&coord, "bucket", test_requester(), None).unwrap();

    let sse_customer = test_sse_customer_request();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::sse_customer(&sse_customer),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"copy managed destination",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let result = coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst-sse-s3",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: Some(&sse_customer),
            destination_encryption: WriteEncryptionRequest::managed(
                ManagedEncryptionAlgorithm::Aes256,
            ),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();
    assert_eq!(
        result.managed_encryption,
        Some(ManagedEncryptionAlgorithm::Aes256)
    );
    assert!(result.sse_customer.is_none());

    let destination = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "dst-sse-s3",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        destination.body.read_all().unwrap(),
        b"copy managed destination"
    );
    assert_eq!(
        destination.managed_encryption,
        Some(ManagedEncryptionAlgorithm::Aes256)
    );
}

#[test]
fn copy_object_explicit_sse_c_destination_requires_customer_headers() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_sse_c(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    enable_bucket_sse_c_test(&coord, "bucket", test_requester(), None).unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"copy sse-c destination",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let sse_customer = test_sse_customer_request();
    let result = coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst-sse-c",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::sse_customer(&sse_customer),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();
    assert!(result.managed_encryption.is_none());
    assert!(result.sse_customer.is_some());

    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "dst-sse-c",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::InvalidRequest { .. }));

    let destination = coord
        .get_object(&GetObjectRequest {
            sse_customer: Some(&sse_customer),
            object: object_version_request_with_expected_owner(
                "bucket",
                "dst-sse-c",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        destination.body.read_all().unwrap(),
        b"copy sse-c destination"
    );
    assert!(destination.managed_encryption.is_none());
    assert!(destination.sse_customer.is_some());
}

#[test]
fn copy_object_rejects_private_source_read_for_non_owner() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "src-bucket", false)
        .unwrap();
    coord
        .create_bucket_for_owner("owner-b", "dst-bucket", false)
        .unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "src-bucket",
                "src",
                test_helpers::requester("owner-a"),
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
    .unwrap();

    let err = coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("src-bucket", "src", None),
            destination: object_request_with_expected_owner(
                "dst-bucket",
                "dst",
                test_helpers::requester("owner-b"),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn copy_object_rejects_bucket_owner_copying_private_object_owned_by_other_principal() {
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
                "src",
                test_helpers::requester("writer-a"),
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
    .unwrap();

    let err = coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_helpers::requester("owner-a"),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn copy_object_allows_grantee_with_full_control_on_bucket_and_source_object() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::new(
        "owner-a",
        CanonicalUserId::from_principal("owner-canonical"),
        "Owner A",
    );
    let writer = AccountIdentity::new(
        "writer-a",
        CanonicalUserId::from_principal("writer-canonical"),
        "Writer A",
    );
    let owner_requester = Requester::authenticated(owner.clone());
    let writer_requester = Requester::authenticated(writer.clone());

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
                "src",
                owner_requester.clone(),
                None,
            ),
            data: b"granted-copy",
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
            AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
            AclPermission::FullControl,
        )]),
        owner_requester.clone(),
        None,
    )
    .unwrap();
    put_object_acl_test(
        &coord,
        "bucket",
        "src",
        None,
        AclGrants::new(vec![AclGrant::new(
            AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
            AclPermission::FullControl,
        )]),
        owner_requester.clone(),
        None,
    )
    .unwrap();

    coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                writer_requester.clone(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();

    let copied = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "dst",
                None,
                writer_requester.clone(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(copied.body.read_all().unwrap(), b"granted-copy");

    let dst_acl = get_object_acl_test(
        &coord,
        "bucket",
        "dst",
        None,
        writer_requester.clone(),
        None,
    )
    .unwrap();
    assert_eq!(dst_acl.owner_principal, writer.principal());
    assert_eq!(
        dst_acl.owner_canonical_id,
        writer.canonical_user_id().clone()
    );
    assert!(grants_contain(
        &dst_acl.acl_grants,
        &AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
        AclPermission::FullControl,
    ));
}

#[test]
fn copy_object_rejects_acl_on_bucket_owner_enforced_bucket() {
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
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
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
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: PutObjectAcl::PublicRead.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessControlListNotSupported));
}

#[test]
fn copy_object_rejects_public_acl_when_block_public_acls_enabled() {
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
                "src",
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
    put_bucket_public_access_block_test(&coord,
                "bucket",
                "<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>false</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
                test_helpers::requester("owner-a"), None)
            .unwrap();

    let err = coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: PutObjectAcl::PublicRead.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn copy_object_metadata_copy_directive() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let headers = [
        ("Content-Type", "image/png"),
        ("X-Amz-Meta-Author", "alice"),
    ];
    let metadata = MetadataBlob::from_headers(&headers).unwrap();
    let system_metadata = SystemMetadata::from_headers(&headers).unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"data",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "dst",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        obj.system_metadata.content_type().map(|v| v.as_str()),
        Some("image/png")
    );
    assert_eq!(obj.metadata.get("x-amz-meta-author"), Some("alice"));
}

#[test]
fn copy_object_metadata_replace_directive() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let headers = [
        ("Content-Type", "image/png"),
        ("X-Amz-Meta-Author", "alice"),
    ];
    let metadata = MetadataBlob::from_headers(&headers).unwrap();
    let system_metadata = SystemMetadata::from_headers(&headers).unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"data",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let new_headers = [("Content-Type", "text/html"), ("X-Amz-Meta-Version", "2")];
    let new_metadata = MetadataBlob::from_headers(&new_headers).unwrap();
    let new_system_metadata = SystemMetadata::from_headers(&new_headers).unwrap();
    coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Replace {
                metadata: &new_metadata,
                system_metadata: &new_system_metadata,
                checksum_algorithm: None,
            },
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "dst",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"data");
    assert_eq!(
        obj.system_metadata.content_type().map(|v| v.as_str()),
        Some("text/html")
    );
    assert_eq!(obj.metadata.get("x-amz-meta-version"), Some("2"));
    // Old metadata should be gone
    assert_eq!(obj.metadata.get("x-amz-meta-author"), None);
}

#[test]
fn copy_object_same_key_replace_metadata() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let headers = [("Content-Type", "text/plain")];
    let metadata = MetadataBlob::from_headers(&headers).unwrap();
    let system_metadata = SystemMetadata::from_headers(&headers).unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"data",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let new_headers = [("Content-Type", "application/json")];
    let new_metadata = MetadataBlob::from_headers(&new_headers).unwrap();
    let new_system_metadata = SystemMetadata::from_headers(&new_headers).unwrap();
    coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "key", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "key",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Replace {
                metadata: &new_metadata,
                system_metadata: &new_system_metadata,
                checksum_algorithm: None,
            },
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();

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
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"data");
    assert_eq!(
        obj.system_metadata.content_type().map(|v| v.as_str()),
        Some("application/json")
    );

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
            cond: NO_DELETE,
        })
        .unwrap();
}

#[test]
fn copy_object_tagging_copy_preserves_source_tags() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let tags_xml =
        "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(tags_xml),
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();

    let tags = get_object_tags_test(&coord, "bucket", "dst", None, test_requester(), None).unwrap();
    assert_eq!(tags.as_deref(), Some(tags_xml));
}

#[test]
fn copy_object_commits_authorized_acl_and_trusted_copied_tags() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let tags = "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(tags),
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,
            acl: PutObjectAcl::PublicRead.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();

    let object = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "dst",
                None,
                Requester::anonymous(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(read_all_body(object.body).unwrap(), b"data");
    let object_tags =
        get_object_tags_test(&coord, "bucket", "dst", None, test_requester(), None).unwrap();
    assert_eq!(object_tags.as_deref(), Some(tags));
}

#[test]
fn copy_object_tagging_replace_overwrites_source_tags() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let src_tags =
        "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
    let dst_tags =
        "<Tagging><TagSet><Tag><Key>tier</Key><Value>gold</Value></Tag></TagSet></Tagging>";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(src_tags),
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Replace(Some(dst_tags)),

            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();

    let tags = get_object_tags_test(&coord, "bucket", "dst", None, test_requester(), None).unwrap();
    assert_eq!(tags.as_deref(), Some(dst_tags));
}

#[test]
fn copy_object_replace_strips_unverified_inline_checksum_and_applies_default_checksum() {
    // Regression: CopyObject with REPLACE must not persist client-supplied
    // checksum value headers, since there is no body to verify them against.
    // AWS still applies the default CRC64NVME checksum for the copied body.
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
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"hello",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // Metadata blob with only content-type (checksum value headers should
    // be stripped at the HTTP boundary before reaching the coordinator).
    let new_headers = [("Content-Type", "text/plain")];
    let new_metadata = MetadataBlob::from_headers(&new_headers).unwrap();
    let new_system_metadata = SystemMetadata::from_headers(&new_headers).unwrap();
    coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Replace {
                metadata: &new_metadata,
                system_metadata: &new_system_metadata,
                checksum_algorithm: None,
            },
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "dst",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"hello");
    use base64::Engine;
    let expected_crc = checksum::crc64::checksum(b"hello");
    let expected_b64 = base64::engine::general_purpose::STANDARD.encode(expected_crc.to_be_bytes());
    let checksum = obj.system_metadata.checksum().unwrap();
    assert_eq!(checksum.algorithm(), ChecksumAlgorithm::Crc64nvme);
    assert_eq!(checksum.checksum_type(), Some(ChecksumType::FullObject));
    assert_eq!(checksum.value(), expected_b64.as_str());
}

#[test]
fn copy_object_replace_recomputes_checksum_from_algorithm() {
    // When x-amz-checksum-algorithm is specified on CopyObject REPLACE,
    // the checksum should be computed from the copied data.
    use base64::Engine;
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let data = b"hello";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let new_headers = [("Content-Type", "text/plain")];
    let new_metadata = MetadataBlob::from_headers(&new_headers).unwrap();
    let new_system_metadata = SystemMetadata::from_headers(&new_headers).unwrap();
    coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Replace {
                metadata: &new_metadata,
                system_metadata: &new_system_metadata,
                checksum_algorithm: Some(ChecksumAlgorithm::Crc32c),
            },
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "dst",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), data);
    // Checksum should be the real CRC32C of "hello", not missing.
    let expected_crc = checksum::crc32c::checksum(data);
    let expected_b64 = base64::engine::general_purpose::STANDARD.encode(expected_crc.to_be_bytes());
    let checksum = obj.system_metadata.checksum().unwrap();
    assert_eq!(checksum.algorithm(), ChecksumAlgorithm::Crc32c);
    assert_eq!(checksum.value(), expected_b64.as_str());
}

#[test]
fn checksum_algorithm_parse_rejects_bogus() {
    // Invalid checksum algorithm strings are rejected at the parse boundary
    // (HTTP layer), so they can never reach the coordinator as typed values.
    assert!(ChecksumAlgorithm::parse("BOGUS").is_none());
    assert!(ChecksumAlgorithm::parse("").is_none());
    // Valid ones are accepted.
    assert_eq!(
        ChecksumAlgorithm::parse("CRC32C"),
        Some(ChecksumAlgorithm::Crc32c)
    );
}

#[test]
fn copy_object_source_not_found() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let err = coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "no-such-key", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::ObjectNotFound { .. }));
}

#[test]
fn copy_object_dest_bucket_not_found() {
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
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
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
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "no-bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::BucketNotFound { .. }));
}

#[test]
fn copy_object_source_if_match_fails() {
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
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let src_cond = ReadCondition {
        if_match: Some("\"0000000000000000\"".into()),
        ..Default::default()
    };
    let err = coord
        .copy_object(&CopyObjectRequest {
            source: copy_source_with_condition_and_expected_owner(
                "bucket", "src", None, &src_cond, None,
            ),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::PreconditionFailed { .. }));
}

#[test]
fn copy_object_dest_if_none_match_prevents_overwrite() {
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
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"data",
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
            object: object_request_with_expected_owner("bucket", "dst", test_requester(), None),
            data: b"existing",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let dst_cond = WriteCondition::IfNoneMatchStar;
    let err = coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: &dst_cond,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::PreconditionFailed { .. }));
}

#[test]
fn copy_object_dest_if_match_allows_update() {
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
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"new data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let existing = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "dst", test_requester(), None),
            data: b"old data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let dst_cond = WriteCondition::IfMatch(SpecificEtag::new(existing.etag).unwrap());
    let result = coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: &dst_cond,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();
    assert!(!result.etag.is_empty());

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "dst",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"new data");
}

#[test]
fn copy_object_cross_bucket() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "src-bucket", false)
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "dst-bucket", false)
        .unwrap();

    let headers = [("Content-Type", "text/plain")];
    let metadata = MetadataBlob::from_headers(&headers).unwrap();
    let system_metadata = SystemMetadata::from_headers(&headers).unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("src-bucket", "key", test_requester(), None),
            data: b"cross bucket data",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("src-bucket", "key", None),
            destination: object_request_with_expected_owner(
                "dst-bucket",
                "key",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "dst-bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"cross bucket data");
    assert_eq!(
        obj.system_metadata.content_type().map(|v| v.as_str()),
        Some("text/plain")
    );

    // Source should still exist
    let src = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "src-bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(src.body.read_all().unwrap(), b"cross bucket data");
}

// ── Bucket versioning tests ──────────────────────────────────────

#[test]
fn bucket_versioning_default_disabled() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let state = get_bucket_versioning_test(&coord, "bucket", test_requester(), None).unwrap();
    assert_eq!(state, BucketVersioningState::Disabled);
}

#[test]
fn bucket_versioning_enable() {
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
    assert_eq!(
        get_bucket_versioning_test(&coord, "bucket", test_requester(), None).unwrap(),
        BucketVersioningState::Enabled
    );
}

#[test]
fn put_object_acl_updates_only_requested_version() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::new(
        "owner-a",
        CanonicalUserId::from_principal("owner-version-canonical"),
        "Owner A",
    );
    let requester = Requester::authenticated(owner);

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        requester.clone(),
        None,
    )
    .unwrap();

    let v1 = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", requester.clone(), None),
            data: b"v1",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let v2 = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", requester.clone(), None),
            data: b"v2",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    put_object_canned_acl_test(
        &coord,
        "bucket",
        "key",
        Some(v1.version_id),
        PutObjectAcl::PublicRead,
        requester.clone(),
        None,
    )
    .unwrap();

    let v1_acl = get_object_acl_test(
        &coord,
        "bucket",
        "key",
        Some(v1.version_id),
        requester.clone(),
        None,
    )
    .unwrap();
    let v2_acl = get_object_acl_test(
        &coord,
        "bucket",
        "key",
        Some(v2.version_id),
        requester.clone(),
        None,
    )
    .unwrap();
    assert!(grants_contain(
        &v1_acl.acl_grants,
        &AclGrantee::AllUsers,
        AclPermission::Read,
    ));
    assert!(!grants_contain(
        &v2_acl.acl_grants,
        &AclGrantee::AllUsers,
        AclPermission::Read,
    ));
}

#[test]
fn put_object_acl_without_version_updates_current_version_only() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::new(
        "owner-a",
        CanonicalUserId::from_principal("owner-current-version-canonical"),
        "Owner A",
    );
    let requester = Requester::authenticated(owner);

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        requester.clone(),
        None,
    )
    .unwrap();

    let old = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", requester.clone(), None),
            data: b"old",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let current = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", requester.clone(), None),
            data: b"current",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    put_object_canned_acl_test(
        &coord,
        "bucket",
        "key",
        None,
        PutObjectAcl::PublicRead,
        requester.clone(),
        None,
    )
    .unwrap();

    let current_acl = get_object_acl_test(
        &coord,
        "bucket",
        "key",
        Some(current.version_id),
        requester.clone(),
        None,
    )
    .unwrap();
    let old_acl = get_object_acl_test(
        &coord,
        "bucket",
        "key",
        Some(old.version_id),
        requester.clone(),
        None,
    )
    .unwrap();
    assert!(grants_contain(
        &current_acl.acl_grants,
        &AclGrantee::AllUsers,
        AclPermission::Read,
    ));
    assert!(!grants_contain(
        &old_acl.acl_grants,
        &AclGrantee::AllUsers,
        AclPermission::Read,
    ));
}

#[test]
fn bucket_versioning_enable_then_suspend() {
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
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Suspended,
        test_requester(),
        None,
    )
    .unwrap();
    assert_eq!(
        get_bucket_versioning_test(&coord, "bucket", test_requester(), None).unwrap(),
        BucketVersioningState::Suspended
    );
}

#[test]
fn create_bucket_with_object_lock_enables_versioning() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::from_principal("owner-a");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(owner),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
        .unwrap();

    assert_eq!(
        get_bucket_versioning_test(&coord, "bucket", test_helpers::requester("owner-a"), None)
            .unwrap(),
        BucketVersioningState::Enabled
    );
    assert_eq!(
        get_bucket_object_lock_configuration_test(
            &coord,
            "bucket",
            test_helpers::requester("owner-a"),
            None
        )
        .unwrap(),
        BucketObjectLockConfig {
            enabled: true,
            default_retention: None,
        }
    );
}

#[test]
fn bucket_versioning_suspend_then_enable() {
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
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Suspended,
        test_requester(),
        None,
    )
    .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();
    assert_eq!(
        get_bucket_versioning_test(&coord, "bucket", test_requester(), None).unwrap(),
        BucketVersioningState::Enabled
    );
}

#[test]
fn list_object_versions_suspended_null_live_is_latest() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let metadata = MetadataBlob::new();
    let system_metadata = SystemMetadata::EMPTY;
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

    let older = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"older",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Suspended,
        test_requester(),
        None,
    )
    .unwrap();

    let current = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"current",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    assert_eq!(current.version_id, VersionId::Null);

    let resp = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 100,
            requested_max_keys: Some(100),
        })
        .unwrap();
    assert_eq!(resp.versions.len(), 2);
    assert_eq!(resp.versions[0].version_id, VersionId::Null);
    assert!(resp.versions[0].is_latest);
    assert!(!resp.versions[0].is_delete_marker);
    assert_eq!(resp.versions[1].version_id, older.version_id);
    assert!(!resp.versions[1].is_latest);
    assert!(!resp.versions[1].is_delete_marker);
}

#[test]
fn bucket_versioning_cannot_disable_from_enabled() {
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
    let err = put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Disabled,
        test_requester(),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::InvalidRequest { .. }));
}

#[test]
fn bucket_versioning_cannot_suspend_object_lock_bucket() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::from_principal("owner-a");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(owner),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
        .unwrap();

    let err = put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Suspended,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::InvalidBucketState));
}

#[test]
fn bucket_versioning_nonexistent_bucket() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = put_bucket_versioning_test(
        &coord,
        "no-bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::BucketNotFound { .. }));
}

#[test]
fn put_bucket_versioning_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let err = put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_helpers::requester("other-user"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn authorize_put_bucket_versioning_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let err = coord
        .authorize_put_bucket_versioning(&PutBucketVersioningRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("other-user"),
                None,
            ),
            state: BucketVersioningState::Enabled,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn get_bucket_versioning_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
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
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetBucketVersioning","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    let state = get_bucket_versioning_test(
        &coord,
        "bucket",
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert_eq!(state, BucketVersioningState::Enabled);
}

#[test]
fn get_bucket_location_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetBucketLocation","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    coord
        .get_bucket_location(&bucket_request_with_expected_owner(
            "bucket",
            test_helpers::requester("444455556666"),
            None,
        ))
        .unwrap();
}

#[test]
fn put_bucket_versioning_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:PutBucketVersioning","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap();

    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_helpers::requester("444455556666"),
        None,
    )
    .unwrap();
    assert_eq!(
        get_bucket_versioning_test(
            &coord,
            "bucket",
            test_helpers::requester("111122223333"),
            None,
        )
        .unwrap(),
        BucketVersioningState::Enabled
    );
}

#[test]
fn authorize_put_bucket_versioning_rejects_suspend_on_object_lock_bucket() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::from_principal("owner-a");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(owner),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
        .unwrap();

    let err = coord
        .authorize_put_bucket_versioning(&PutBucketVersioningRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("owner-a"),
                None,
            ),
            state: BucketVersioningState::Suspended,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::InvalidBucketState));
}

#[test]
fn put_bucket_object_lock_requires_enabled_versioning() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    let update = BucketObjectLockConfigurationUpdate {
        object_lock_enabled: Some(true),
        default_retention: Some(ObjectLockDefaultRetention {
            mode: s3_types::ObjectLockMode::Governance,
            period: s3_types::RetentionPeriod::days(1).unwrap(),
        }),
    };

    let err = put_bucket_object_lock_configuration_test(
        &coord,
        "bucket",
        update,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::InvalidBucketState));

    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Suspended,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    let err = put_bucket_object_lock_configuration_test(
        &coord,
        "bucket",
        update,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::InvalidBucketState));

    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    put_bucket_object_lock_configuration_test(
        &coord,
        "bucket",
        update,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    assert_eq!(
        get_bucket_object_lock_configuration_test(
            &coord,
            "bucket",
            test_helpers::requester("owner-a"),
            None
        )
        .unwrap(),
        BucketObjectLockConfig {
            enabled: true,
            default_retention: update.default_retention,
        }
    );
}

#[test]
fn get_bucket_object_lock_configuration_missing_reports_not_found() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let err = get_bucket_object_lock_configuration_test(
        &coord,
        "bucket",
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ServerError::ObjectLockConfigurationNotFound { .. }
    ));
}

#[test]
fn get_bucket_object_lock_configuration_bucket_policy_allows_cross_account() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::from_principal("owner-a");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(owner),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
        .unwrap();
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:GetBucketObjectLockConfiguration","Resource":"arn:aws:s3:::bucket"}]}"#,
                test_helpers::requester("owner-a"), None)
            .unwrap();

    let config = get_bucket_object_lock_configuration_test(
        &coord,
        "bucket",
        test_helpers::requester("other-user"),
        None,
    )
    .unwrap();
    assert!(config.enabled);
}

#[test]
fn authorize_get_bucket_object_lock_configuration_bucket_policy_allows_cross_account() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::from_principal("owner-a");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(owner),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
        .unwrap();
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:GetBucketObjectLockConfiguration","Resource":"arn:aws:s3:::bucket"}]}"#,
                test_helpers::requester("owner-a"), None)
            .unwrap();

    let authorized = coord
        .authorize_get_bucket_object_lock_configuration(&bucket_request_with_expected_owner(
            "bucket",
            test_helpers::requester("other-user"),
            None,
        ))
        .unwrap();
    assert!(authorized.config.enabled);
}

#[test]
fn authorize_get_bucket_object_lock_configuration_missing_reports_not_found() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let err = coord
        .authorize_get_bucket_object_lock_configuration(&bucket_request_with_expected_owner(
            "bucket",
            test_helpers::requester("owner-a"),
            None,
        ))
        .unwrap_err();
    assert!(matches!(
        err,
        ServerError::ObjectLockConfigurationNotFound { .. }
    ));
}

#[test]
fn put_bucket_object_lock_configuration_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::from_principal("owner-a");
    let update = BucketObjectLockConfigurationUpdate {
        object_lock_enabled: Some(true),
        default_retention: Some(ObjectLockDefaultRetention {
            mode: s3_types::ObjectLockMode::Governance,
            period: s3_types::RetentionPeriod::days(1).unwrap(),
        }),
    };

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(owner),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
        .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:PutBucketObjectLockConfiguration","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("owner-a"),
            None,
        )
        .unwrap();

    put_bucket_object_lock_configuration_test(
        &coord,
        "bucket",
        update,
        test_helpers::requester("other-user"),
        None,
    )
    .unwrap();
    assert_eq!(
        get_bucket_object_lock_configuration_test(
            &coord,
            "bucket",
            test_helpers::requester("owner-a"),
            None
        )
        .unwrap(),
        BucketObjectLockConfig {
            enabled: true,
            default_retention: update.default_retention,
        }
    );
}

#[test]
fn authorize_put_bucket_object_lock_configuration_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::from_principal("owner-a");
    let update = BucketObjectLockConfigurationUpdate {
        object_lock_enabled: Some(true),
        default_retention: Some(ObjectLockDefaultRetention {
            mode: s3_types::ObjectLockMode::Governance,
            period: s3_types::RetentionPeriod::days(1).unwrap(),
        }),
    };

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(owner),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
        .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:PutBucketObjectLockConfiguration","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("owner-a"),
            None,
        )
        .unwrap();

    let authorized = coord
        .authorize_put_bucket_object_lock_configuration(&PutBucketObjectLockConfigurationRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("other-user"),
                None,
            ),
            config: update,
        })
        .unwrap();
    assert_eq!(
        authorized.config,
        BucketObjectLockConfig {
            enabled: true,
            default_retention: update.default_retention,
        }
    );
}

#[test]
fn put_bucket_object_lock_configuration_bucket_policy_deny_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::from_principal("owner-a");
    let update = BucketObjectLockConfigurationUpdate {
        object_lock_enabled: Some(true),
        default_retention: Some(ObjectLockDefaultRetention {
            mode: s3_types::ObjectLockMode::Governance,
            period: s3_types::RetentionPeriod::days(1).unwrap(),
        }),
    };

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(owner),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
        .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutBucketObjectLockConfiguration","Resource":"arn:aws:s3:::bucket"}]}"#,
            test_helpers::requester("owner-a"),
            None,
        )
        .unwrap();

    let err = put_bucket_object_lock_configuration_test(
        &coord,
        "bucket",
        update,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn get_object_retention_bucket_policy_allows_cross_account() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::from_principal("owner-a");
    let owner_requester = test_helpers::requester("owner-a");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(owner),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
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
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        retention,
        false,
        owner_requester.clone(),
    )
    .unwrap();
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:GetObjectRetention","Resource":"arn:aws:s3:::bucket/*"}]}"#,
                owner_requester.clone(), None)
            .unwrap();

    let fetched = get_object_retention_test(
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        test_helpers::requester("other-user"),
    )
    .unwrap();
    assert_eq!(fetched, Some(retention));
}

#[test]
fn unauthorized_object_lock_calls_do_not_reveal_bucket_lock_configuration() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::from_principal("owner-a");
    let owner_requester = test_helpers::requester("owner-a");
    let other_requester = test_helpers::requester("other-user");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("plain-bucket"),
            requester: Requester::authenticated(owner.clone()),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();
    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("lock-bucket"),
            requester: Requester::authenticated(owner),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
        .unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "plain-bucket",
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
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "lock-bucket",
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

    let plain_retention =
        get_object_retention_test(&coord, "plain-bucket", "key", None, other_requester.clone())
            .unwrap_err();
    let lock_retention =
        get_object_retention_test(&coord, "lock-bucket", "key", None, other_requester).unwrap_err();

    assert!(matches!(plain_retention, ServerError::AccessDenied));
    assert!(matches!(lock_retention, ServerError::AccessDenied));
}

#[test]
fn put_object_retention_bucket_policy_requires_explicit_bypass_allow_cross_account() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::from_principal("owner-a");
    let owner_requester = test_helpers::requester("owner-a");
    let other_requester = test_helpers::requester("other-user");
    let now = Coordinator::current_unix_seconds().unwrap();

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(owner),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
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
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until_unix_seconds: now + 200,
        },
        false,
        owner_requester.clone(),
    )
    .unwrap();
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:PutObjectRetention","Resource":"arn:aws:s3:::bucket/*"}]}"#,
                owner_requester.clone(), None)
            .unwrap();

    let err = put_object_retention_test(
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until_unix_seconds: now + 150,
        },
        true,
        other_requester.clone(),
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":["s3:PutObjectRetention","s3:BypassGovernanceRetention"],"Resource":"arn:aws:s3:::bucket/*"}]}"#,
                owner_requester, None)
            .unwrap();

    put_object_retention_test(
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until_unix_seconds: now + 150,
        },
        true,
        other_requester,
    )
    .unwrap();
}

#[test]
fn put_object_retention_requires_explicit_bypass_allow_cross_account_real_api() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::from_principal("owner-a");
    let owner_requester = test_helpers::requester("owner-a");
    let other_requester = test_helpers::requester("other-user");
    let now = Coordinator::current_unix_seconds().unwrap();

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(owner),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
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
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until_unix_seconds: now + 200,
        },
        false,
        owner_requester.clone(),
    )
    .unwrap();
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:PutObjectRetention","Resource":"arn:aws:s3:::bucket/*"}]}"#,
                owner_requester, None)
            .unwrap();

    let err = coord
        .put_object_retention(&PutObjectRetentionRequest {
            object: object_version_request("bucket", "key", Some(put.version_id), other_requester),
            retention: ObjectRetention {
                mode: ObjectLockMode::Governance,
                retain_until_unix_seconds: now + 150,
            },
            bypass_governance: true,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn put_object_retention_bucket_policy_explicit_deny_blocks_owner_bypass() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::from_principal("owner-a");
    let owner_requester = test_helpers::requester("owner-a");
    let now = Coordinator::current_unix_seconds().unwrap();

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(owner),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
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
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until_unix_seconds: now + 200,
        },
        false,
        owner_requester.clone(),
    )
    .unwrap();
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":"owner-a"},"Action":"s3:BypassGovernanceRetention","Resource":"arn:aws:s3:::bucket/*"}]}"#,
                owner_requester.clone(), None)
            .unwrap();

    let err = put_object_retention_test(
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until_unix_seconds: now + 150,
        },
        true,
        owner_requester,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn put_object_retention_allows_same_account_owner_account() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let same_account_canonical_id = CanonicalUserId::from_principal("111122223333");
    let bucket_owner = AccountIdentity::new(
        "arn:aws:iam::111122223333:root",
        same_account_canonical_id.clone(),
        "Bucket Owner",
    );
    let same_account_account_principal = AccountIdentity::new(
        "111122223333",
        same_account_canonical_id,
        "Same Account Owner Principal",
    );
    let bucket_owner_requester = Requester::authenticated(bucket_owner);
    let same_account_user_requester =
        Requester::authenticated_owner_account_admin(same_account_account_principal);
    let now = Coordinator::current_unix_seconds().unwrap();

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: bucket_owner_requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
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
                bucket_owner_requester.clone(),
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
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until_unix_seconds: now + 200,
        },
        false,
        same_account_user_requester.clone(),
    )
    .unwrap();
    let fetched = get_object_retention_test(
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        same_account_user_requester,
    )
    .unwrap();
    assert_eq!(
        fetched,
        Some(ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until_unix_seconds: now + 200,
        })
    );
}

#[test]
fn put_object_legal_hold_bucket_policy_allows_cross_account_real_api() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::from_principal("owner-a");
    let owner_requester = test_helpers::requester("owner-a");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(owner),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
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
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":["s3:PutObjectLegalHold","s3:GetObjectLegalHold"],"Resource":"arn:aws:s3:::bucket/*"}]}"#,
                owner_requester, None)
            .unwrap();

    put_object_legal_hold_test(
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        LegalHoldStatus::On,
        test_helpers::requester("other-user"),
    )
    .unwrap();
    let fetched = get_object_legal_hold_test(
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        test_helpers::requester("other-user"),
    )
    .unwrap();
    assert_eq!(fetched, Some(LegalHoldStatus::On));
}

#[test]
fn put_object_legal_hold_allows_same_account_owner_account() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let same_account_canonical_id = CanonicalUserId::from_principal("111122223333");
    let bucket_owner = AccountIdentity::new(
        "arn:aws:iam::111122223333:root",
        same_account_canonical_id.clone(),
        "Bucket Owner",
    );
    let same_account_account_principal = AccountIdentity::new(
        "111122223333",
        same_account_canonical_id,
        "Same Account Owner Principal",
    );
    let bucket_owner_requester = Requester::authenticated(bucket_owner);
    let same_account_user_requester =
        Requester::authenticated_owner_account_admin(same_account_account_principal);

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: bucket_owner_requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
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
                bucket_owner_requester.clone(),
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

    put_object_legal_hold_test(
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        LegalHoldStatus::On,
        same_account_user_requester.clone(),
    )
    .unwrap();
    let fetched = get_object_legal_hold_test(
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        same_account_user_requester,
    )
    .unwrap();
    assert_eq!(fetched, Some(LegalHoldStatus::On));
}

#[test]
fn put_object_legal_hold_bucket_policy_allows_cross_account_direct_api() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::from_principal("owner-a");
    let owner_requester = test_helpers::requester("owner-a");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(owner),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
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
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":["s3:PutObjectLegalHold","s3:GetObjectLegalHold"],"Resource":"arn:aws:s3:::bucket/*"}]}"#,
                owner_requester, None)
            .unwrap();

    coord
        .put_object_legal_hold(&PutObjectLegalHoldRequest {
            object: object_version_request(
                "bucket",
                "key",
                Some(put.version_id),
                test_helpers::requester("other-user"),
            ),
            legal_hold: LegalHoldStatus::On,
        })
        .unwrap();
    let fetched = get_object_legal_hold_test(
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        test_helpers::requester("other-user"),
    )
    .unwrap();
    assert_eq!(fetched, Some(LegalHoldStatus::On));
}

#[test]
fn resolve_new_object_lock_state_applies_default_retention_and_preserves_legal_hold() {
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
    put_bucket_object_lock_configuration_test(
        &coord,
        "bucket",
        BucketObjectLockConfigurationUpdate {
            object_lock_enabled: Some(true),
            default_retention: Some(ObjectLockDefaultRetention {
                mode: ObjectLockMode::Governance,
                period: RetentionPeriod::days(1).unwrap(),
            }),
        },
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    let bucket = coord.unchecked_active_bucket_summary("bucket").unwrap();
    let before = Coordinator::current_unix_seconds().unwrap();
    let resolved = Coordinator::resolve_new_object_lock_state(
        &bucket,
        ObjectLockState {
            retention: None,
            legal_hold: StoredLegalHoldStatus::Off,
        },
    )
    .unwrap();
    let after = Coordinator::current_unix_seconds().unwrap();

    assert_eq!(resolved.legal_hold, StoredLegalHoldStatus::Off);
    let retention = resolved.retention.unwrap();
    assert_eq!(retention.mode, ObjectLockMode::Governance);
    assert!(retention.retain_until_unix_seconds >= before + 86_400);
    assert!(retention.retain_until_unix_seconds <= after + 86_405);
}

#[test]
fn resolve_new_object_lock_state_explicit_retention_overrides_bucket_default() {
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
    put_bucket_object_lock_configuration_test(
        &coord,
        "bucket",
        BucketObjectLockConfigurationUpdate {
            object_lock_enabled: Some(true),
            default_retention: Some(ObjectLockDefaultRetention {
                mode: ObjectLockMode::Governance,
                period: RetentionPeriod::days(1).unwrap(),
            }),
        },
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    let bucket = coord.unchecked_active_bucket_summary("bucket").unwrap();
    let explicit = ObjectLockState {
        retention: Some(ObjectRetention {
            mode: ObjectLockMode::Compliance,
            retain_until_unix_seconds: 1_900_000_000,
        }),
        legal_hold: StoredLegalHoldStatus::NotSet,
    };

    let resolved = Coordinator::resolve_new_object_lock_state(&bucket, explicit).unwrap();
    assert_eq!(resolved, explicit);
}

#[test]
fn validate_requested_object_lock_state_rejects_plain_bucket_headers() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    let bucket = coord.unchecked_active_bucket_summary("bucket").unwrap();
    let err = Coordinator::validate_requested_object_lock_state(
        &bucket,
        ObjectLockState {
            retention: Some(ObjectRetention {
                mode: ObjectLockMode::Governance,
                retain_until_unix_seconds: 1_900_000_000,
            }),
            legal_hold: StoredLegalHoldStatus::NotSet,
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::InvalidRequest { .. }));
}

#[test]
fn validate_requested_object_lock_state_rejects_past_retention() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::from_principal("owner-a");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(owner),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
        .unwrap();

    let bucket = coord.unchecked_active_bucket_summary("bucket").unwrap();
    let now = Coordinator::current_unix_seconds().unwrap();
    let err = Coordinator::validate_requested_object_lock_state(
        &bucket,
        ObjectLockState {
            retention: Some(ObjectRetention {
                mode: ObjectLockMode::Governance,
                retain_until_unix_seconds: now.saturating_sub(1),
            }),
            legal_hold: StoredLegalHoldStatus::NotSet,
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::InvalidArgument { .. }));
}

#[test]
fn copy_object_with_object_lock_to_plain_bucket_rejects_before_reading_source() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "src", false)
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "dst", false)
        .unwrap();

    let source_body = vec![b'x'; INTERNAL_SEGMENT_SIZE + 1];
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("src", "source", test_requester(), None),
            data: &source_body,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let first_segment_read = Arc::new(AtomicBool::new(false));
    let first_segment_read_hook = Arc::clone(&first_segment_read);
    let _guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some(("src".to_string(), "source".to_string())),
        after_object_segments_first_segment: Some(Arc::new(move || {
            first_segment_read_hook.store(true, Ordering::SeqCst);
        })),
        ..ReclamationTestHooks::default()
    });

    let err = coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("src", "source", None),
            destination: object_request_with_expected_owner(
                "dst",
                "copied",
                test_requester(),
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
            object_lock: ObjectLockState {
                retention: None,
                legal_hold: StoredLegalHoldStatus::On,
            },
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::InvalidRequest { .. }));
    assert!(
        !first_segment_read.load(Ordering::SeqCst),
        "copy_object should reject invalid Object Lock headers before reading source data"
    );
}

#[test]
fn validate_retention_update_allows_governance_increase_without_bypass() {
    let current = ObjectRetention {
        mode: ObjectLockMode::Governance,
        retain_until_unix_seconds: 100,
    };
    let requested = ObjectRetention {
        mode: ObjectLockMode::Governance,
        retain_until_unix_seconds: 200,
    };
    assert!(Coordinator::validate_retention_update(Some(current), requested, false, false).is_ok());
}

#[test]
fn validate_retention_update_requires_bypass_for_governance_mode_change() {
    let current = ObjectRetention {
        mode: ObjectLockMode::Governance,
        retain_until_unix_seconds: 100,
    };
    let requested = ObjectRetention {
        mode: ObjectLockMode::Compliance,
        retain_until_unix_seconds: 100,
    };
    let err =
        Coordinator::validate_retention_update(Some(current), requested, false, false).unwrap_err();
    assert!(matches!(err, ServerError::ObjectLockProtectedAccessDenied));
    let err =
        Coordinator::validate_retention_update(Some(current), requested, true, false).unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
    assert!(Coordinator::validate_retention_update(Some(current), requested, true, true).is_ok());
}

#[test]
fn validate_retention_update_rejects_compliance_downgrade_or_shorten() {
    let current = ObjectRetention {
        mode: ObjectLockMode::Compliance,
        retain_until_unix_seconds: 200,
    };
    let downgrade = ObjectRetention {
        mode: ObjectLockMode::Governance,
        retain_until_unix_seconds: 200,
    };
    let shorten = ObjectRetention {
        mode: ObjectLockMode::Compliance,
        retain_until_unix_seconds: 100,
    };
    assert!(matches!(
        Coordinator::validate_retention_update(Some(current), downgrade, true, true),
        Err(ServerError::ObjectLockProtectedAccessDenied)
    ));
    assert!(matches!(
        Coordinator::validate_retention_update(Some(current), shorten, true, true),
        Err(ServerError::ObjectLockProtectedAccessDenied)
    ));
}

#[test]
fn validate_delete_against_object_lock_requires_bypass_for_governance_retention() {
    let state = ObjectLockState {
        retention: Some(ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until_unix_seconds: 200,
        }),
        legal_hold: StoredLegalHoldStatus::NotSet,
    };
    let err =
        Coordinator::validate_delete_against_object_lock(state, false, false, 100).unwrap_err();
    assert!(matches!(err, ServerError::ObjectLockProtectedAccessDenied));
    let err =
        Coordinator::validate_delete_against_object_lock(state, true, false, 100).unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
    assert!(Coordinator::validate_delete_against_object_lock(state, true, true, 100).is_ok());
}

#[test]
fn delete_object_bucket_policy_explicit_deny_blocks_owner_bypass() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::from_principal("owner-a");
    let owner_requester = test_helpers::requester("owner-a");
    let now = Coordinator::current_unix_seconds().unwrap();

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(owner),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
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
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until_unix_seconds: now + 200,
        },
        false,
        owner_requester.clone(),
    )
    .unwrap();
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":"owner-a"},"Action":"s3:BypassGovernanceRetention","Resource":"arn:aws:s3:::bucket/*"}]}"#,
                owner_requester.clone(), None)
            .unwrap();

    let err = coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            Some(put.version_id),
            owner_requester.clone(),
            true,
            NO_DELETE,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn delete_object_bucket_policy_allows_cross_account_bypass() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner_requester = test_helpers::requester("owner-a");
    let other_requester = test_helpers::requester("other-user");
    let now = Coordinator::current_unix_seconds().unwrap();

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(AccountIdentity::from_principal("owner-a")),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
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
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until_unix_seconds: now + 200,
        },
        false,
        owner_requester.clone(),
    )
    .unwrap();
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":["s3:DeleteObjectVersion","s3:BypassGovernanceRetention"],"Resource":"arn:aws:s3:::bucket/*"}]}"#,
                owner_requester, None)
            .unwrap();

    coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            Some(put.version_id),
            other_requester,
            true,
            NO_DELETE,
        ))
        .unwrap();
}

#[test]
fn authorize_delete_object_bucket_policy_allows_cross_account_bypass() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner_requester = test_helpers::requester("owner-a");
    let other_requester = test_helpers::requester("other-user");
    let now = Coordinator::current_unix_seconds().unwrap();

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(AccountIdentity::from_principal("owner-a")),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
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
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until_unix_seconds: now + 200,
        },
        false,
        owner_requester.clone(),
    )
    .unwrap();
    put_bucket_policy_test(&coord,
                "bucket",
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":["s3:DeleteObjectVersion","s3:BypassGovernanceRetention"],"Resource":"arn:aws:s3:::bucket/*"}]}"#,
                owner_requester, None)
            .unwrap();

    let authorized = coord
        .authorize_delete_object(&delete_object_request(
            "bucket",
            "key",
            Some(put.version_id),
            other_requester,
            true,
            NO_DELETE,
        ))
        .unwrap();
    assert!(matches!(
        authorized,
        AuthorizedDeleteObject::SpecificVersion { version_id, .. } if version_id == put.version_id
    ));
}

#[test]
fn delete_specific_version_rechecks_object_lock_state_at_execution() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let requester = test_helpers::requester("owner-a");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(AccountIdentity::from_principal("owner-a")),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
        .unwrap();

    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", requester.clone(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let authorized = coord
        .authorize_delete_object(&delete_object_request(
            "bucket",
            "key",
            Some(put.version_id),
            requester.clone(),
            false,
            NO_DELETE,
        ))
        .unwrap();

    put_object_legal_hold_test(
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        LegalHoldStatus::On,
        requester,
    )
    .unwrap();

    let err = coord
        .apply_authorized_delete_object(&coord.storage_node(), authorized, NO_DELETE)
        .unwrap_err();
    assert!(matches!(err, ServerError::ObjectLockProtectedAccessDenied));
}

#[test]
fn delete_unversioned_rechecks_current_object_at_execution() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let requester = test_helpers::requester("owner-a");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(AccountIdentity::from_principal("owner-a")),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", requester.clone(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let cond = crate::conditional::DeleteCondition::IfMatch(put.etag.clone().into());
    let authorized = coord
        .authorize_delete_object(&DeleteObjectRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                requester.clone(),
                None,
            ),
            bypass_governance: false,
            cond: &cond,
        })
        .unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", requester, None),
            data: b"new-data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let err = coord
        .apply_authorized_delete_object(&coord.storage_node(), authorized, &cond)
        .unwrap_err();
    assert!(matches!(err, ServerError::PreconditionFailed { .. }));
}

#[test]
fn delete_current_marker_insert_rechecks_current_object_at_execution() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let requester = test_helpers::requester("owner-a");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(AccountIdentity::from_principal("owner-a")),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        requester.clone(),
        None,
    )
    .unwrap();

    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", requester.clone(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let cond = crate::conditional::DeleteCondition::IfMatch(put.etag.clone().into());
    let authorized = coord
        .authorize_delete_object(&DeleteObjectRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                requester.clone(),
                None,
            ),
            bypass_governance: false,
            cond: &cond,
        })
        .unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", requester, None),
            data: b"new-data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let err = coord
        .apply_authorized_delete_object(&coord.storage_node(), authorized, &cond)
        .unwrap_err();
    assert!(matches!(err, ServerError::PreconditionFailed { .. }));
}

#[test]
fn boe_delete_specific_version_rechecks_object_lock_state_at_execution() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner_requester = test_helpers::requester("owner-a");
    let other_requester = test_helpers::requester("other-user");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(AccountIdentity::from_principal("owner-a")),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: true,
        })
        .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:DeleteObjectVersion","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        owner_requester.clone(),
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

    let authorized = coord
        .authorize_delete_object(&delete_object_request(
            "bucket",
            "key",
            Some(put.version_id),
            other_requester,
            false,
            NO_DELETE,
        ))
        .unwrap();

    put_object_legal_hold_test(
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        LegalHoldStatus::On,
        owner_requester,
    )
    .unwrap();

    let err = coord
        .apply_authorized_delete_object(&coord.storage_node(), authorized, NO_DELETE)
        .unwrap_err();
    assert!(matches!(err, ServerError::ObjectLockProtectedAccessDenied));
}

#[test]
fn boe_delete_unversioned_rechecks_current_object_at_execution() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner_requester = test_helpers::requester("owner-a");
    let other_requester = test_helpers::requester("other-user");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(AccountIdentity::from_principal("owner-a")),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:DeleteObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        owner_requester.clone(),
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

    let cond = crate::conditional::DeleteCondition::IfMatch(put.etag.clone().into());
    let authorized = coord
        .authorize_delete_object(&DeleteObjectRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                other_requester,
                None,
            ),
            bypass_governance: false,
            cond: &cond,
        })
        .unwrap();
    assert!(matches!(
        authorized,
        AuthorizedDeleteObject::UnversionedDelete { .. }
    ));

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", owner_requester, None),
            data: b"new-data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let err = coord
        .apply_authorized_delete_object(&coord.storage_node(), authorized, &cond)
        .unwrap_err();
    assert!(matches!(err, ServerError::PreconditionFailed { .. }));
}

#[test]
fn boe_delete_current_marker_insert_rechecks_current_object_at_execution() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner_requester = test_helpers::requester("owner-a");
    let other_requester = test_helpers::requester("other-user");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(AccountIdentity::from_principal("owner-a")),
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
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:DeleteObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        owner_requester.clone(),
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

    let cond = crate::conditional::DeleteCondition::IfMatch(put.etag.clone().into());
    let authorized = coord
        .authorize_delete_object(&DeleteObjectRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                other_requester,
                None,
            ),
            bypass_governance: false,
            cond: &cond,
        })
        .unwrap();
    assert!(matches!(
        authorized,
        AuthorizedDeleteObject::CurrentDeleteMarkerInsert { .. }
    ));

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", owner_requester, None),
            data: b"new-data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let err = coord
        .apply_authorized_delete_object(&coord.storage_node(), authorized, &cond)
        .unwrap_err();
    assert!(matches!(err, ServerError::PreconditionFailed { .. }));
}

#[test]
fn boe_delete_missing_specific_version_with_bypass_rechecks_execution_permission() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner_requester = test_helpers::requester("owner-a");
    let other_requester = test_helpers::requester("other-user");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(AccountIdentity::from_principal("owner-a")),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: true,
        })
        .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:DeleteObjectVersion","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        owner_requester.clone(),
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

    let authorized = coord
        .authorize_delete_object(&delete_object_request(
            "bucket",
            "key",
            Some(put.version_id),
            other_requester,
            true,
            NO_DELETE,
        ))
        .unwrap();

    coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            Some(put.version_id),
            owner_requester,
            false,
            NO_DELETE,
        ))
        .unwrap();

    let err = coord
        .apply_authorized_delete_object(&coord.storage_node(), authorized, NO_DELETE)
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn delete_object_missing_version_with_bypass_requires_bucket_policy_bypass() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner_requester = test_helpers::requester("owner-a");
    let other_requester = test_helpers::requester("other-user");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(AccountIdentity::from_principal("owner-a")),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
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
    coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            Some(put.version_id),
            owner_requester.clone(),
            false,
            NO_DELETE,
        ))
        .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":"s3:DeleteObjectVersion","Resource":"arn:aws:s3:::bucket/*"}]}"#,
            owner_requester,
            None,
        )
        .unwrap();

    coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            Some(put.version_id),
            other_requester.clone(),
            false,
            NO_DELETE,
        ))
        .unwrap();

    let err = coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            Some(put.version_id),
            other_requester,
            true,
            NO_DELETE,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn delete_object_missing_version_bucket_policy_allows_cross_account_bypass() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner_requester = test_helpers::requester("owner-a");
    let other_requester = test_helpers::requester("other-user");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(AccountIdentity::from_principal("owner-a")),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
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
    coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            Some(put.version_id),
            owner_requester.clone(),
            false,
            NO_DELETE,
        ))
        .unwrap();
    put_bucket_policy_test(
            &coord,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"other-user"},"Action":["s3:DeleteObjectVersion","s3:BypassGovernanceRetention"],"Resource":"arn:aws:s3:::bucket/*"}]}"#,
            owner_requester,
            None,
        )
        .unwrap();

    coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            Some(put.version_id),
            other_requester,
            true,
            NO_DELETE,
        ))
        .unwrap();
}

#[test]
fn delete_object_allows_same_account_owner_account_bypass() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let same_account_canonical_id = CanonicalUserId::from_principal("111122223333");
    let bucket_owner = AccountIdentity::new(
        "arn:aws:iam::111122223333:root",
        same_account_canonical_id.clone(),
        "Bucket Owner",
    );
    let same_account_account_principal = AccountIdentity::new(
        "111122223333",
        same_account_canonical_id,
        "Same Account Owner Principal",
    );
    let bucket_owner_requester = Requester::authenticated(bucket_owner);
    let same_account_user_requester =
        Requester::authenticated_owner_account_admin(same_account_account_principal);
    let now = Coordinator::current_unix_seconds().unwrap();

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: bucket_owner_requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
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
                bucket_owner_requester.clone(),
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
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until_unix_seconds: now + 200,
        },
        false,
        bucket_owner_requester,
    )
    .unwrap();

    coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            Some(put.version_id),
            same_account_user_requester,
            true,
            NO_DELETE,
        ))
        .unwrap();
}

#[test]
fn validate_delete_against_object_lock_rejects_compliance_even_with_bypass() {
    let state = ObjectLockState {
        retention: Some(ObjectRetention {
            mode: ObjectLockMode::Compliance,
            retain_until_unix_seconds: 200,
        }),
        legal_hold: StoredLegalHoldStatus::NotSet,
    };
    let err = Coordinator::validate_delete_against_object_lock(state, true, true, 100).unwrap_err();
    assert!(matches!(err, ServerError::ObjectLockProtectedAccessDenied));
}

#[test]
fn validate_delete_against_object_lock_rejects_legal_hold_even_with_bypass() {
    let state = ObjectLockState {
        retention: Some(ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until_unix_seconds: 50,
        }),
        legal_hold: StoredLegalHoldStatus::On,
    };
    let err = Coordinator::validate_delete_against_object_lock(state, true, true, 100).unwrap_err();
    assert!(matches!(err, ServerError::ObjectLockProtectedAccessDenied));
}

#[test]
fn delete_object_with_retention_still_inserts_delete_marker_without_version_id() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::from_principal("owner-a");
    let requester = test_helpers::requester("owner-a");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(owner),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
        .unwrap();

    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", requester.clone(), None),
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
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until_unix_seconds: Coordinator::current_unix_seconds().unwrap() + 3600,
        },
        false,
        requester.clone(),
    )
    .unwrap();

    let delete_marker = coord
        .delete_object(&DeleteObjectRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                requester.clone(),
                None,
            ),
            bypass_governance: false,
            cond: NO_DELETE,
        })
        .unwrap();
    assert!(delete_marker.delete_marker);
    let delete_marker_version_id = delete_marker
        .version_id
        .expect("versioned delete marker must return a version ID");
    assert!(delete_marker_version_id.is_versioned());

    let err = coord
        .delete_object(&DeleteObjectRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                Some(put.version_id),
                requester.clone(),
                None,
            ),
            bypass_governance: false,
            cond: NO_DELETE,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::ObjectLockProtectedAccessDenied));

    coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            Some(delete_marker_version_id),
            requester.clone(),
            false,
            NO_DELETE,
        ))
        .unwrap();
    coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            Some(put.version_id),
            requester,
            true,
            NO_DELETE,
        ))
        .unwrap();
}

#[test]
fn delete_object_with_legal_hold_rejects_bypass() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::from_principal("owner-a");
    let requester = test_helpers::requester("owner-a");

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(owner),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
        .unwrap();

    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", requester.clone(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    put_object_legal_hold_test(
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        LegalHoldStatus::On,
        requester.clone(),
    )
    .unwrap();

    let err = coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            Some(put.version_id),
            requester.clone(),
            true,
            NO_DELETE,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::ObjectLockProtectedAccessDenied));

    put_object_legal_hold_test(
        &coord,
        "bucket",
        "key",
        Some(put.version_id),
        LegalHoldStatus::Off,
        requester.clone(),
    )
    .unwrap();
    coord
        .delete_object(&DeleteObjectRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                Some(put.version_id),
                requester,
                None,
            ),
            bypass_governance: true,
            cond: NO_DELETE,
        })
        .unwrap();
}

#[test]
fn delete_object_with_governance_bypass_rejects_public_writer() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner_canonical_id = CanonicalUserId::from_principal("owner-a");
    let owner_requester = test_helpers::requester("owner-a");
    let writer_requester = test_helpers::requester("writer-a");

    create_bucket_for_owner_with_flags(
        &coord,
        "owner-a",
        &owner_canonical_id,
        "bucket",
        false,
        true,
        true,
    )
    .unwrap();

    let plain = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "plain",
                owner_requester.clone(),
                None,
            ),
            data: b"plain",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    coord
        .delete_object(&delete_object_request(
            "bucket",
            "plain",
            Some(plain.version_id),
            writer_requester.clone(),
            false,
            NO_DELETE,
        ))
        .unwrap();

    let locked = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "locked",
                owner_requester.clone(),
                None,
            ),
            data: b"locked",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    put_object_retention_test(
        &coord,
        "bucket",
        "locked",
        Some(locked.version_id),
        ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until_unix_seconds: Coordinator::current_unix_seconds().unwrap() + 3600,
        },
        false,
        owner_requester.clone(),
    )
    .unwrap();

    let err = coord
        .delete_object(&delete_object_request(
            "bucket",
            "locked",
            Some(locked.version_id),
            writer_requester.clone(),
            true,
            NO_DELETE,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn bucket_encryption_defaults_to_sse_s3() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    assert_eq!(
        get_bucket_encryption_test(&coord, "bucket", test_requester(), None).unwrap(),
        EffectiveBucketEncryptionConfig::default()
    );
}

#[test]
fn bucket_encryption_block_and_unblock() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    put_bucket_encryption_test(
        &coord,
        "bucket",
        BucketEncryptionConfig {
            default_encryption: Some(ManagedEncryptionAlgorithm::Aes256),
            sse_c_blocked: true,
        },
        test_requester(),
        None,
    )
    .unwrap();
    assert!(
        get_bucket_encryption_test(&coord, "bucket", test_requester(), None)
            .unwrap()
            .sse_c_blocked
    );

    put_bucket_encryption_test(
        &coord,
        "bucket",
        BucketEncryptionConfig {
            default_encryption: None,
            sse_c_blocked: false,
        },
        test_requester(),
        None,
    )
    .unwrap();
    assert_eq!(
        get_bucket_encryption_test(&coord, "bucket", test_requester(), None).unwrap(),
        EffectiveBucketEncryptionConfig::default()
    );
}

#[test]
fn put_bucket_encryption_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let err = put_bucket_encryption_test(
        &coord,
        "bucket",
        BucketEncryptionConfig {
            default_encryption: None,
            sse_c_blocked: true,
        },
        test_helpers::requester("other-user"),
        None,
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn sse_c_put_object_rejected_when_bucket_blocks_sse_c() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator_with_sse_c(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    enable_bucket_sse_c_test(&coord, "bucket", test_requester(), None).unwrap();
    put_bucket_encryption_test(
        &coord,
        "bucket",
        BucketEncryptionConfig {
            default_encryption: None,
            sse_c_blocked: true,
        },
        test_requester(),
        None,
    )
    .unwrap();

    let sse_customer = test_sse_customer_request();
    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::sse_customer(&sse_customer),
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
    .unwrap_err();
    assert!(matches!(err, ServerError::SseCBlockedAccessDenied { .. }));
}

#[test]
fn unencrypted_put_object_allowed_when_bucket_blocks_sse_c() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_encryption_test(
        &coord,
        "bucket",
        BucketEncryptionConfig {
            default_encryption: None,
            sse_c_blocked: true,
        },
        test_requester(),
        None,
    )
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
}

#[test]
fn sse_c_stream_put_rejected_when_bucket_blocks_sse_c() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator_with_sse_c(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_encryption_test(
        &coord,
        "bucket",
        BucketEncryptionConfig {
            default_encryption: None,
            sse_c_blocked: true,
        },
        test_requester(),
        None,
    )
    .unwrap();

    let sse_customer = test_sse_customer_request();

    let err = begin_stream_put_with_authorized_request_test(
        &coord,
        object_request_with_expected_owner("bucket", "key", test_requester(), None),
        NO_PUT_OBJECT_ACL.into(),
        PutObjectPolicyContext::default(),
        WriteEncryptionRequest::sse_customer(&sse_customer),
        ObjectLockState::default(),
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::SseCBlockedAccessDenied { .. }));
}

#[test]
fn sse_c_upload_part_rejected_when_bucket_blocks_sse_c() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator_with_sse_c(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    enable_bucket_sse_c_test(&coord, "bucket", test_requester(), None).unwrap();

    let sse_customer = test_sse_customer_request();
    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::sse_customer(&sse_customer),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    put_bucket_encryption_test(
        &coord,
        "bucket",
        BucketEncryptionConfig {
            default_encryption: None,
            sse_c_blocked: true,
        },
        test_requester(),
        None,
    )
    .unwrap();

    let err = coord
        .begin_stream_part(&BeginStreamPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            policy_context: PutObjectPolicyContext::default(),
            sse_customer: Some(&sse_customer),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn put_object_returns_version_id_zero() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let result = test_helpers::put_object(
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
    assert_eq!(result.version_id, VersionId::Null);
}

#[test]
fn get_object_returns_version_id() {
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
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.version_id, VersionId::Null);
}

#[test]
fn head_object_returns_version_id() {
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
    let head = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(head.version_id, VersionId::Null);
}

#[test]
fn versioned_put_is_safe_across_concurrent_frontends() {
    let tmp = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let make_coord =
        || setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    let admin = make_coord();
    admin
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &admin,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();

    // Repeat to increase the chance of exposing races.
    for i in 0..20 {
        let coord_a = make_coord();
        let coord_b = make_coord();
        let key = format!("key-{i}");
        let key_a = key.clone();
        let key_b = key;

        let barrier = Arc::new(Barrier::new(3));
        let b1 = Arc::clone(&barrier);
        let b2 = Arc::clone(&barrier);

        let t1 = thread::spawn(move || {
            b1.wait();
            put_object_retrying_operation_aborted(
                &coord_a,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: object_request_with_expected_owner(
                        "bucket",
                        &key_a,
                        test_requester(),
                        None,
                    ),
                    data: b"v1",
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,

                    acl: NO_PUT_OBJECT_ACL.into(),
                },
            )
        });
        let t2 = thread::spawn(move || {
            b2.wait();
            put_object_retrying_operation_aborted(
                &coord_b,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: object_request_with_expected_owner(
                        "bucket",
                        &key_b,
                        test_requester(),
                        None,
                    ),
                    data: b"v2",
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,

                    acl: NO_PUT_OBJECT_ACL.into(),
                },
            )
        });

        barrier.wait();

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        assert!(r1.is_ok(), "first concurrent put failed: {r1:?}");
        assert!(r2.is_ok(), "second concurrent put failed: {r2:?}");

        let v1 = r1.unwrap().version_id;
        let v2 = r2.unwrap().version_id;
        assert_ne!(v1, v2, "concurrent puts must not reuse version IDs");
    }
}

fn put_object_retrying_operation_aborted(
    coord: &Coordinator,
    req: &PutObjectRequest<'_>,
) -> Result<PutObjectResult, ServerError> {
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        match test_helpers::put_object(coord, req) {
            Ok(result) => return Ok(result),
            Err(ServerError::OperationAborted) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => return Err(error),
        }
    }
}

fn delete_object_retrying_operation_aborted(
    coord: &Coordinator,
    req: &DeleteObjectRequest<'_>,
) -> Result<DeleteObjectResult, ServerError> {
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        match coord.delete_object(req) {
            Ok(result) => return Ok(result),
            Err(ServerError::OperationAborted) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => return Err(error),
        }
    }
}

#[test]
fn get_object_is_consistent_during_concurrent_overwrite() {
    let tmp = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let make_coord =
        || setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    let admin = make_coord();
    admin
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let object_size = (2 * 1024 * 1024) + 137;
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: &vec![b'A'; object_size],
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let mut current = b'A';
    for _ in 0..20 {
        let next = if current == b'A' { b'B' } else { b'A' };
        let new_payload = vec![next; object_size];

        let reader = make_coord();
        let writer = make_coord();
        let barrier = Arc::new(Barrier::new(3));
        let b1 = Arc::clone(&barrier);
        let b2 = Arc::clone(&barrier);

        let t_write = thread::spawn(move || {
            b1.wait();
            put_object_retrying_operation_aborted(
                &writer,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: object_request_with_expected_owner(
                        "bucket",
                        "key",
                        test_requester(),
                        None,
                    ),
                    data: &new_payload,
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,

                    acl: NO_PUT_OBJECT_ACL.into(),
                },
            )
        });
        let t_read = thread::spawn(move || {
            b2.wait();
            reader.get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "key",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
        });

        barrier.wait();

        let write_res = t_write.join().unwrap();
        assert!(
            write_res.is_ok(),
            "concurrent overwrite failed: {write_res:?}"
        );

        let read_res = t_read.join().unwrap();
        let obj = read_res.expect("get_object must not fail during overwrite");
        let data = obj.body.read_all().unwrap();
        assert_eq!(data.len(), object_size);
        let uniform = data.iter().all(|&b| b == current) || data.iter().all(|&b| b == next);
        assert!(
            uniform,
            "read must return a complete old or new object image"
        );

        current = next;
    }
}

#[test]
fn copy_object_is_consistent_during_concurrent_overwrite() {
    let tmp = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let make_coord =
        || setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    let admin = make_coord();
    admin
        .create_bucket_for_owner("default-owner", "src-bucket", false)
        .unwrap();
    admin
        .create_bucket_for_owner("default-owner", "dst-bucket", false)
        .unwrap();

    let object_size = (2 * 1024 * 1024) + 137;
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("src-bucket", "src", test_requester(), None),
            data: &vec![b'A'; object_size],
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let mut current = b'A';
    for i in 0..12 {
        let next = if current == b'A' { b'B' } else { b'A' };
        let new_payload = vec![next; object_size];
        let dst_key = format!("dst-{i}");
        let dst_key_for_copy = dst_key.clone();

        let writer = make_coord();
        let copier = make_coord();
        let barrier = Arc::new(Barrier::new(3));
        let b1 = Arc::clone(&barrier);
        let b2 = Arc::clone(&barrier);

        let t_write = thread::spawn(move || {
            b1.wait();
            put_object_retrying_operation_aborted(
                &writer,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: object_request_with_expected_owner(
                        "src-bucket",
                        "src",
                        test_requester(),
                        None,
                    ),
                    data: &new_payload,
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,

                    acl: NO_PUT_OBJECT_ACL.into(),
                },
            )
        });
        let t_copy = thread::spawn(move || {
            b2.wait();
            copier.copy_object(&CopyObjectRequest {
                source: copy_source("src-bucket", "src", None),
                destination: object_request_with_expected_owner(
                    "dst-bucket",
                    &dst_key_for_copy,
                    test_requester(),
                    None,
                ),
                dst_condition: NO_WRITE,
                directive: MetadataDirective::Copy,
                website_redirect_location: None,
                tagging: TaggingDirective::Copy,

                acl: NO_PUT_OBJECT_ACL.into(),
                policy_context: PutObjectPolicyContext::default(),
                source_sse_customer: None,
                destination_encryption: WriteEncryptionRequest::none(),
                object_lock: ObjectLockState::default(),
            })
        });

        barrier.wait();

        let write_res = t_write.join().unwrap();
        assert!(
            write_res.is_ok(),
            "concurrent overwrite failed: {write_res:?}"
        );

        let copy_res = t_copy.join().unwrap();
        assert!(
            copy_res.is_ok(),
            "copy_object must not fail during overwrite: {copy_res:?}"
        );

        let copied_obj = admin
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "dst-bucket",
                    &dst_key,
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        let data = copied_obj.body.read_all().unwrap();
        assert_eq!(data.len(), object_size);
        let uniform = data.iter().all(|&b| b == current) || data.iter().all(|&b| b == next);
        assert!(
            uniform,
            "copied object must contain a complete old or new source image"
        );

        current = next;
    }
}

#[test]
fn copy_object_uses_snapshotted_source_during_overwrite() {
    let tmp = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let make_coord =
        || setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    let admin = make_coord();
    admin
        .create_bucket_for_owner("default-owner", "src-bucket", false)
        .unwrap();
    admin
        .create_bucket_for_owner("default-owner", "dst-bucket", false)
        .unwrap();

    let original_body = vec![b'o'; (2 * 1024 * 1024) + 137];
    let original_headers = [
        ("Content-Type", "application/x-original"),
        ("X-Amz-Meta-Source-State", "original"),
    ];
    let original_metadata = MetadataBlob::from_headers(&original_headers).unwrap();
    let original_system_metadata = SystemMetadata::from_headers(&original_headers).unwrap();
    let original = test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("src-bucket", "src", test_requester(), None),
            data: &original_body,
            metadata: &original_metadata,
            system_metadata: &original_system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let original_etag = original.etag;

    let sync = install_object_read_snapshot_race_hook("src-bucket", "src");
    let copier = make_coord();
    let copy = thread::spawn(move || {
        copier.copy_object(&CopyObjectRequest {
            source: copy_source("src-bucket", "src", None),
            destination: object_request_with_expected_owner(
                "dst-bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
    });
    sync.snapshot_reached.wait();

    let replacement_body = vec![b'r'; (2 * 1024 * 1024) + 137];
    let replacement_headers = [
        ("Content-Type", "application/x-replacement"),
        ("X-Amz-Meta-Source-State", "replacement"),
    ];
    let replacement_metadata = MetadataBlob::from_headers(&replacement_headers).unwrap();
    let replacement_system_metadata = SystemMetadata::from_headers(&replacement_headers).unwrap();
    let replacement = test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("src-bucket", "src", test_requester(), None),
            data: &replacement_body,
            metadata: &replacement_metadata,
            system_metadata: &replacement_system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    );
    sync.snapshot_resume.wait();
    let copied = copy.join().unwrap();
    drop(sync);
    let replacement = replacement.expect("source overwrite must complete while copy is paused");
    let copied = copied.expect("copy must complete from its source snapshot");
    assert_ne!(replacement.etag, original_etag);
    assert_eq!(copied.etag, original_etag);

    let destination = admin
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "dst-bucket",
                "dst",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(destination.etag, original_etag);
    assert_eq!(
        destination
            .system_metadata
            .content_type()
            .map(|value| value.as_str()),
        Some("application/x-original")
    );
    assert_eq!(
        destination.metadata.get("x-amz-meta-source-state"),
        Some("original")
    );
    assert_eq!(destination.body.read_all().unwrap(), original_body);

    let source = admin
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "src-bucket",
                "src",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(source.etag, replacement.etag);
    assert_eq!(
        source
            .system_metadata
            .content_type()
            .map(|value| value.as_str()),
        Some("application/x-replacement")
    );
    assert_eq!(
        source.metadata.get("x-amz-meta-source-state"),
        Some("replacement")
    );
    assert_eq!(source.body.read_all().unwrap(), replacement_body);
}

#[test]
fn copy_object_uses_snapshotted_source_during_delete() {
    let tmp = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let make_coord =
        || setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    let admin = make_coord();
    admin
        .create_bucket_for_owner("default-owner", "src-bucket", false)
        .unwrap();
    admin
        .create_bucket_for_owner("default-owner", "dst-bucket", false)
        .unwrap();

    let source_body = vec![b's'; (2 * 1024 * 1024) + 137];
    let source_headers = [
        ("Content-Type", "application/x-before-delete"),
        ("X-Amz-Meta-Source-State", "before-delete"),
    ];
    let source_metadata = MetadataBlob::from_headers(&source_headers).unwrap();
    let source_system_metadata = SystemMetadata::from_headers(&source_headers).unwrap();
    let source = test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("src-bucket", "src", test_requester(), None),
            data: &source_body,
            metadata: &source_metadata,
            system_metadata: &source_system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let source_etag = source.etag;

    let sync = install_object_read_snapshot_race_hook("src-bucket", "src");
    let copier = make_coord();
    let copy = thread::spawn(move || {
        copier.copy_object(&CopyObjectRequest {
            source: copy_source("src-bucket", "src", None),
            destination: object_request_with_expected_owner(
                "dst-bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
    });
    sync.snapshot_reached.wait();

    let deleted = admin.delete_object(&delete_object_request(
        "src-bucket",
        "src",
        None,
        test_requester(),
        false,
        NO_DELETE,
    ));
    sync.snapshot_resume.wait();
    let copied = copy.join().unwrap();
    drop(sync);
    deleted.expect("source delete must complete while copy is paused");
    let copied = copied.expect("copy must complete from its source snapshot");
    assert_eq!(copied.etag, source_etag);

    let destination = admin
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "dst-bucket",
                "dst",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(destination.etag, source_etag);
    assert_eq!(
        destination
            .system_metadata
            .content_type()
            .map(|value| value.as_str()),
        Some("application/x-before-delete")
    );
    assert_eq!(
        destination.metadata.get("x-amz-meta-source-state"),
        Some("before-delete")
    );
    assert_eq!(destination.body.read_all().unwrap(), source_body);

    assert!(matches!(
        admin.get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "src-bucket",
                "src",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        }),
        Err(ServerError::ObjectNotFound { .. })
    ));
}

#[test]
fn upload_part_copy_is_consistent_during_concurrent_overwrite() {
    let tmp = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let make_coord =
        || setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    let admin = make_coord();
    admin
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let object_size = (2 * 1024 * 1024) + 137;
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: &vec![b'A'; object_size],
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let mut current = b'A';
    for i in 0..8 {
        let next = if current == b'A' { b'B' } else { b'A' };
        let new_payload = vec![next; object_size];
        let dst_key = format!("dst-{i}");
        let upload = admin
            .create_multipart_upload(&CreateMultipartUploadRequest {
                object: object_request_with_expected_owner(
                    "bucket",
                    &dst_key,
                    test_requester(),
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
        let dst_key_for_copy = dst_key.clone();
        let upload_id_for_copy = upload.upload_id.clone();

        let writer = make_coord();
        let copier = make_coord();
        let barrier = Arc::new(Barrier::new(3));
        let b1 = Arc::clone(&barrier);
        let b2 = Arc::clone(&barrier);

        let t_write = thread::spawn(move || {
            b1.wait();
            put_object_retrying_operation_aborted(
                &writer,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: object_request_with_expected_owner(
                        "bucket",
                        "src",
                        test_requester(),
                        None,
                    ),
                    data: &new_payload,
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,

                    acl: NO_PUT_OBJECT_ACL.into(),
                },
            )
        });
        let t_copy = thread::spawn(move || {
            b2.wait();
            copier.upload_part_copy(&UploadPartCopyRequest {
                source: copy_source("bucket", "src", None),
                upload: multipart_object_request_with_expected_owner(
                    "bucket",
                    &dst_key_for_copy,
                    &upload_id_for_copy,
                    test_requester(),
                    None,
                ),
                part_number: 1,
                copy_source_range: None,

                policy_context: PutObjectPolicyContext::default(),

                source_sse_customer: None,
                sse_customer: None,
            })
        });

        barrier.wait();

        let write_res = t_write.join().unwrap();
        assert!(
            write_res.is_ok(),
            "concurrent overwrite failed: {write_res:?}"
        );

        let copy_res = t_copy.join().unwrap();
        let copy_res = copy_res.expect("upload_part_copy must not fail during overwrite");

        admin
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                upload: multipart_object_request_with_expected_owner(
                    "bucket",
                    &dst_key,
                    &upload.upload_id,
                    test_requester(),
                    None,
                ),
                parts: &[CompletePart {
                    part_number: 1,
                    etag: copy_res.etag,
                    checksum: None,
                }],
                sse_customer: None,
                claimed_checksum: None,
                expected_object_size: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        let copied_obj = admin
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    &dst_key,
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        let data = copied_obj.body.read_all().unwrap();
        assert_eq!(data.len(), object_size);
        let uniform = data.iter().all(|&b| b == current) || data.iter().all(|&b| b == next);
        assert!(
            uniform,
            "uploaded copied part must contain a complete old or new source image"
        );

        current = next;
    }
}

#[test]
fn delete_object_is_consistent_during_concurrent_overwrite() {
    let tmp = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let make_coord =
        || setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    let admin = make_coord();
    admin
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let object_size = 256 * 1024;
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: &vec![b'A'; object_size],
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    for i in 0..50 {
        let expected_byte = if i % 2 == 0 { b'B' } else { b'C' };
        let payload = vec![expected_byte; object_size];

        let writer = make_coord();
        let deleter = make_coord();
        let barrier = Arc::new(Barrier::new(3));
        let b1 = Arc::clone(&barrier);
        let b2 = Arc::clone(&barrier);

        let t_write = thread::spawn(move || {
            b1.wait();
            put_object_retrying_operation_aborted(
                &writer,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: object_request_with_expected_owner(
                        "bucket",
                        "key",
                        test_requester(),
                        None,
                    ),
                    data: &payload,
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,

                    acl: NO_PUT_OBJECT_ACL.into(),
                },
            )
        });
        let t_delete = thread::spawn(move || {
            b2.wait();
            delete_object_retrying_operation_aborted(
                &deleter,
                &DeleteObjectRequest {
                    object: object_version_request_with_expected_owner(
                        "bucket",
                        "key",
                        None,
                        test_requester(),
                        None,
                    ),
                    bypass_governance: false,
                    cond: NO_DELETE,
                },
            )
        });

        barrier.wait();

        let write_res = t_write.join().unwrap();
        assert!(
            write_res.is_ok(),
            "concurrent overwrite failed: {write_res:?}"
        );

        let delete_res = t_delete.join().unwrap();
        assert!(
            delete_res.is_ok(),
            "concurrent delete failed: {delete_res:?}"
        );

        let check = make_coord().get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        });
        match check {
            Ok(obj) => {
                let data = obj.body.read_all().unwrap();
                assert_eq!(data.len(), object_size);
                assert!(
                    data.iter().all(|&b| b == expected_byte),
                    "if object exists after put/delete race, it must be a full new image"
                );
            }
            Err(ServerError::ObjectNotFound { .. }) => {}
            Err(other) => panic!("unexpected read result after put/delete race: {other:?}"),
        }
    }
}

#[test]
fn multipart_get_object_survives_metadata_delete_mid_read() {
    let tmp = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let make_coord =
        || setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    let admin = make_coord();
    admin
        .create_bucket_for_owner("default-owner", "race-bucket", false)
        .unwrap();
    let key = find_fresh_key_with_object_pg_gt_data_pg(&admin, "race-bucket", "race-key-get");
    let (_, expected) = create_completed_multipart_with_streamed_tail(&admin, "race-bucket", &key);
    assert_object_maps_object_pg_gt_data_pg(&admin, "race-bucket", &key);

    let sync = install_multipart_metadata_race_hooks("race-bucket", &key);
    let reader = make_coord();
    let deleter = make_coord();
    let read_key = key.clone();
    let delete_key = key.clone();

    let t_read = thread::spawn(move || {
        reader
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "race-bucket",
                    &read_key,
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .and_then(|result| result.body.read_all())
    });
    sync.snapshot_reached.wait();

    let t_delete = thread::spawn(move || {
        deleter.delete_object(&DeleteObjectRequest {
            object: object_version_request_with_expected_owner(
                "race-bucket",
                &delete_key,
                None,
                test_requester(),
                None,
            ),
            bypass_governance: false,
            cond: NO_DELETE,
        })
    });
    sync.delete_reached.wait();

    sync.snapshot_resume.wait();
    let read_res = t_read.join().unwrap();
    sync.delete_resume.wait();
    let delete_res = t_delete.join().unwrap();
    assert!(delete_res.is_ok(), "delete failed: {delete_res:?}");
    let read_res = read_res
        .expect("multipart get_object should succeed once source part metadata is snapshotted");
    assert_eq!(read_res, expected);
}

#[test]
fn multipart_get_object_part_survives_metadata_delete_mid_read() {
    let tmp = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let make_coord =
        || setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    let admin = make_coord();
    admin
        .create_bucket_for_owner("default-owner", "race-bucket", false)
        .unwrap();
    let key = find_fresh_key_with_object_pg_gt_data_pg(&admin, "race-bucket", "race-key-part");
    let (_, expected) = create_completed_multipart_with_streamed_tail(&admin, "race-bucket", &key);
    assert_object_maps_object_pg_gt_data_pg(&admin, "race-bucket", &key);
    let expected_tail = b"streamed-tail-data".to_vec();
    assert_eq!(
        &expected[expected.len() - expected_tail.len()..],
        expected_tail.as_slice()
    );

    let sync = install_multipart_metadata_race_hooks("race-bucket", &key);
    let reader = make_coord();
    let deleter = make_coord();
    let read_key = key.clone();
    let delete_key = key.clone();

    let t_read = thread::spawn(move || {
        reader
            .get_object_part(&GetObjectPartRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "race-bucket",
                    &read_key,
                    None,
                    test_requester(),
                    None,
                ),
                part_number: 2,
                cond: NO_READ,
            })
            .and_then(|res| {
                let part_start = res.part_start;
                let body = res.body.read_all()?;
                Ok((part_start, body))
            })
    });
    sync.snapshot_reached.wait();

    let t_delete = thread::spawn(move || {
        deleter.delete_object(&DeleteObjectRequest {
            object: object_version_request_with_expected_owner(
                "race-bucket",
                &delete_key,
                None,
                test_requester(),
                None,
            ),
            bypass_governance: false,
            cond: NO_DELETE,
        })
    });
    sync.delete_reached.wait();

    sync.snapshot_resume.wait();
    let read_res = t_read.join().unwrap();
    sync.delete_resume.wait();
    let delete_res = t_delete.join().unwrap();
    assert!(delete_res.is_ok(), "delete failed: {delete_res:?}");
    let (part_start, body) = read_res.expect(
        "multipart get_object_part should succeed once part segment metadata is snapshotted",
    );
    assert_eq!(body, expected_tail);
    assert_eq!(part_start, MIN_PART as u64);
}

#[test]
fn object_segments_get_object_survives_delete_mid_read() {
    let tmp = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let make_coord =
        || setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    let admin = make_coord();
    admin
        .create_bucket_for_owner("default-owner", "race-bucket", false)
        .unwrap();
    let key = find_fresh_key_with_object_pg_gt_data_pg(&admin, "race-bucket", "race-key-segments");

    let session_id = begin_stream_put_test(&admin, "race-bucket", &key).unwrap();
    admin
        .append_plaintext_stream_segment_for_test(
            "race-bucket",
            &key,
            &session_id,
            0,
            b"segment-zero-",
        )
        .unwrap();
    admin
        .append_plaintext_stream_segment_for_test(
            "race-bucket",
            &key,
            &session_id,
            1,
            b"segment-one",
        )
        .unwrap();
    let expected = b"segment-zero-segment-one".to_vec();
    admin
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("race-bucket", &key, test_requester()),
            session_id: &session_id,
            crc64: checksum::crc64::checksum(&expected),
            total_size: expected.len() as u64,
            metadata_blob: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: ActiveWriteEncryptionRef::None,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap();
    assert_object_maps_object_pg_gt_data_pg(&admin, "race-bucket", &key);

    let sync = install_object_segments_delete_race_hooks("race-bucket", &key);
    let reader = make_coord();
    let deleter = make_coord();
    let read_key = key.clone();
    let delete_key = key.clone();

    let t_read = thread::spawn(move || {
        reader
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "race-bucket",
                    &read_key,
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .and_then(|result| result.body.read_all())
    });
    sync.first_segment_reached.wait();

    let t_delete = thread::spawn(move || {
        deleter.delete_object(&DeleteObjectRequest {
            object: object_version_request_with_expected_owner(
                "race-bucket",
                &delete_key,
                None,
                test_requester(),
                None,
            ),
            bypass_governance: false,
            cond: NO_DELETE,
        })
    });
    sync.delete_reached.wait();

    sync.first_segment_resume.wait();
    let read_res = t_read.join().unwrap();
    sync.delete_resume.wait();
    let delete_res = t_delete.join().unwrap();
    assert!(delete_res.is_ok(), "delete failed: {delete_res:?}");
    let body = read_res.expect("segmented get_object should survive delete mid-read");
    assert_eq!(body, expected);
}

#[test]
fn upload_part_copy_survives_source_metadata_delete_mid_read() {
    let tmp = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let make_coord =
        || setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    let admin = make_coord();
    admin
        .create_bucket_for_owner("default-owner", "race-bucket", false)
        .unwrap();
    let source_key =
        find_fresh_key_with_object_pg_gt_data_pg(&admin, "race-bucket", "race-key-copy");
    create_completed_multipart_with_streamed_tail(&admin, "race-bucket", &source_key);
    assert_object_maps_object_pg_gt_data_pg(&admin, "race-bucket", &source_key);
    let upload = admin
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "race-bucket",
                "dst",
                test_requester(),
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

    let sync = install_multipart_metadata_race_hooks("race-bucket", &source_key);
    let copier = make_coord();
    let deleter = make_coord();
    let copy_key = source_key.clone();
    let delete_key = source_key.clone();

    let t_copy = thread::spawn(move || {
        copier.upload_part_copy(&UploadPartCopyRequest {
            source: copy_source("race-bucket", &copy_key, None),
            upload: multipart_object_request_with_expected_owner(
                "race-bucket",
                "dst",
                &upload.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            copy_source_range: None,

            policy_context: PutObjectPolicyContext::default(),

            source_sse_customer: None,
            sse_customer: None,
        })
    });
    sync.snapshot_reached.wait();

    let t_delete = thread::spawn(move || {
        deleter.delete_object(&delete_object_request(
            "race-bucket",
            &delete_key,
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
    });
    sync.delete_reached.wait();

    sync.snapshot_resume.wait();
    let copy_res = t_copy.join().unwrap();
    sync.delete_resume.wait();
    let delete_res = t_delete.join().unwrap();
    assert!(delete_res.is_ok(), "delete failed: {delete_res:?}");
    assert!(
            copy_res.is_ok(),
            "upload_part_copy should succeed once source multipart metadata is snapshotted: {copy_res:?}"
        );
}

#[test]
fn copy_object_survives_source_metadata_delete_mid_read() {
    let tmp = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let make_coord =
        || setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    let admin = make_coord();
    admin
        .create_bucket_for_owner("default-owner", "src-bucket", false)
        .unwrap();
    admin
        .create_bucket_for_owner("default-owner", "dst-bucket", false)
        .unwrap();
    let (_, expected) =
        create_completed_multipart_with_streamed_tail(&admin, "src-bucket", "race-key-copy");

    let sync = install_multipart_metadata_race_hooks("src-bucket", "race-key-copy");
    let copier = make_coord();
    let deleter = make_coord();

    let t_copy = thread::spawn(move || {
        copier.copy_object(&CopyObjectRequest {
            source: copy_source("src-bucket", "race-key-copy", None),
            destination: object_request_with_expected_owner(
                "dst-bucket",
                "copied",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,

            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
    });
    sync.snapshot_reached.wait();

    let t_delete = thread::spawn(move || {
        deleter.delete_object(&delete_object_request(
            "src-bucket",
            "race-key-copy",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
    });
    sync.delete_reached.wait();

    sync.snapshot_resume.wait();
    let copy_res = t_copy.join().unwrap();
    sync.delete_resume.wait();
    let delete_res = t_delete.join().unwrap();
    assert!(delete_res.is_ok(), "delete failed: {delete_res:?}");
    assert!(
        copy_res.is_ok(),
        "copy_object should succeed once source multipart metadata is snapshotted: {copy_res:?}"
    );

    let dst = admin
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "dst-bucket",
                "copied",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(dst.body.read_all().unwrap(), expected);
}

#[test]
fn delete_object_returns_result() {
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
    let result = coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap();
    assert_eq!(result.version_id, None);
    assert!(!result.delete_marker);
}
