use super::test_helpers::{self, UploadPartRequest};
use super::test_support::*;
use super::test_topology::*;
use super::*;
use crate::conditional::{DeleteCondition, SpecificEtag, WriteCondition};
use crate::coordinator::bucket_handles::BucketHandleRequest;
use crate::sse::SSE_CUSTOMER_ALGORITHM;
use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;
use storage::{
    install_bucket_scoped_test_hooks, BucketScopedTestHooks, MetadataCommandApplyTestKind, PgId,
    ShardScavengerObservationReason, StorageCluster,
};

const TEST_EVENT_TIMEOUT: Duration = Duration::from_secs(2);
const BUCKET_FAST_PATH_WATCH_TEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, PartialEq, Eq)]
enum LockWaitEvent {
    Progress,
    UnexpectedBucketLock,
    UnexpectedStorageLoad,
    CompletedEarly,
}

fn setup_direct_coordinator_with_storage_cluster(
    storage_cluster: Arc<StorageCluster>,
) -> Coordinator {
    Coordinator::new_with_managed_key_provider_for_storage_cluster(
        storage_cluster,
        "us-east-1".to_string(),
        None,
        test_sse_s3_provider(),
    )
    .unwrap()
}

#[test]
fn lock_mutex_unpoisoned_recovers_after_panic() {
    let lock = Mutex::new(vec![1usize]);
    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _guard = lock.lock().unwrap();
        panic!("poison mutex");
    }));

    lock_mutex_unpoisoned(&lock).push(2);
    assert_eq!(*lock_mutex_unpoisoned(&lock), vec![1, 2]);
}

#[test]
fn rwlock_helpers_recover_after_panic() {
    let lock = RwLock::new(HashMap::from([("bucket".to_string(), 1usize)]));
    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let mut guard = lock.write().unwrap();
        guard.insert("poisoned".to_string(), 2);
        panic!("poison rwlock");
    }));

    write_rwlock_unpoisoned(&lock).insert("ok".to_string(), 3);
    let guard = read_rwlock_unpoisoned(&lock);
    assert_eq!(guard.get("bucket"), Some(&1));
    assert_eq!(guard.get("poisoned"), Some(&2));
    assert_eq!(guard.get("ok"), Some(&3));
}

#[test]
fn put_object_effective_policy_context_derives_explicit_sse_s3() {
    let metadata = MetadataBlob::default();
    let system_metadata = SystemMetadata::default();
    let request = PutObjectRequest {
        object: object_request("bucket", "key", test_requester()),
        data: b"body",
        metadata: &metadata,
        system_metadata: &system_metadata,
        tags: None,
        cond: NO_WRITE,
        acl: PutObjectWriteAcl::None,
        policy_context: PutObjectPolicyContext::default(),
        object_lock: ObjectLockState::default(),
        encryption: WriteEncryptionRequest::managed(ManagedEncryptionAlgorithm::Aes256),
    };

    assert_eq!(
        request
            .effective_policy_context()
            .unwrap()
            .managed_encryption,
        Some(ManagedEncryptionAlgorithm::Aes256)
    );
}

#[test]
fn put_object_effective_policy_context_overrides_conflicting_encryption_fields() {
    let metadata = MetadataBlob::default();
    let system_metadata = SystemMetadata::default();
    let sse_customer = test_sse_customer_request();
    let request = PutObjectRequest {
        object: object_request("bucket", "key", test_requester()),
        data: b"body",
        metadata: &metadata,
        system_metadata: &system_metadata,
        tags: None,
        cond: NO_WRITE,
        acl: PutObjectWriteAcl::None,
        policy_context: PutObjectPolicyContext::default()
            .with_managed_encryption(Some(ManagedEncryptionAlgorithm::Aes256)),
        object_lock: ObjectLockState::default(),
        encryption: WriteEncryptionRequest::sse_customer(&sse_customer),
    };

    let policy_context = request.effective_policy_context().unwrap();
    assert_eq!(policy_context.managed_encryption, None);
    assert_eq!(
        policy_context.sse_customer_algorithm,
        Some(SSE_CUSTOMER_ALGORITHM)
    );
}

#[test]
fn create_multipart_effective_policy_context_derives_explicit_sse_s3() {
    let metadata = MetadataBlob::default();
    let system_metadata = SystemMetadata::default();
    let request = CreateMultipartUploadRequest {
        object: object_request("bucket", "key", test_requester()),
        metadata: &metadata,
        system_metadata: &system_metadata,
        tags: None,
        checksum: None,
        acl: PutObjectWriteAcl::None,
        policy_context: PutObjectPolicyContext::default(),
        object_lock: ObjectLockState::default(),
        encryption: WriteEncryptionRequest::managed(ManagedEncryptionAlgorithm::Aes256),
    };

    assert_eq!(
        request
            .effective_policy_context()
            .unwrap()
            .managed_encryption,
        Some(ManagedEncryptionAlgorithm::Aes256)
    );
}

#[test]
fn begin_stream_put_effective_policy_context_uses_request_encryption() {
    let sse_customer = test_sse_customer_request();
    let cleared = WriteEncryptionRequest::none().with_policy_context(
        PutObjectPolicyContext::default()
            .with_managed_encryption(Some(ManagedEncryptionAlgorithm::Aes256))
            .with_sse_customer_algorithm(Some("AES256"))
            .with_default_canned_acl(PutObjectWriteAcl::None.policy_condition_value()),
    );
    assert_eq!(cleared.managed_encryption, None);
    assert_eq!(cleared.sse_customer_algorithm, None);

    let sse_c = WriteEncryptionRequest::sse_customer(&sse_customer).with_policy_context(
        PutObjectPolicyContext::default()
            .with_default_canned_acl(PutObjectWriteAcl::None.policy_condition_value()),
    );
    assert_eq!(sse_c.managed_encryption, None);
    assert_eq!(sse_c.sse_customer_algorithm, Some(SSE_CUSTOMER_ALGORITHM));
}

#[test]
fn put_object_persists_explicit_object_owner_identity() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner_canonical_id = CanonicalUserId::from_principal("custom-object-owner");
    let owner = AccountIdentity::new("owner-a", owner_canonical_id.clone(), "Owner A");
    let requester = Requester::authenticated(owner.clone());

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

    coord
        .put_object(&PutObjectRequest {
            object: object_request_with_expected_owner("bucket", "key", requester.clone(), None),
            data: b"hello",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();

    let live = coord
        .storage_node
        .test_get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
        .unwrap();
    let live = live.into_live().expect("expected live object");
    assert_eq!(live.owner.principal, owner.principal());
    assert_eq!(live.owner.canonical_id, owner_canonical_id);
}

#[test]
fn direct_put_retry_converges_pending_partial_metadata_command() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let object_pg = storage_cluster.test_object_pg_id_for(&bucket, &key);
    let primary_node = storage_cluster
        .local_pg_route(PgId::new(object_pg))
        .unwrap()
        .primary_node_id();
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = storage_cluster.test_install_before_metadata_command_apply_context_hook(
        Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::CommitDirectPutObject
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && context.node_id == primary_node
                && fail_once_hook.swap(false, Ordering::SeqCst)
            {
                return Err(storage::StoreError::Io {
                    context: "injected coordinator direct put metadata command apply failure",
                    source: std::io::Error::other(
                        "injected coordinator direct put metadata command apply failure",
                    ),
                });
            }
            Ok(())
        }),
    );

    let metadata = MetadataBlob::new();
    let first_err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"first-write",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap_err();
    assert!(
        matches!(
            first_err,
            ServerError::Store(storage::StoreError::Io {
                context: "injected coordinator direct put metadata command apply failure",
                ..
            })
        ),
        "expected injected direct PUT command failure, got {first_err:?}"
    );
    assert!(!fail_once.load(Ordering::SeqCst));
    drop(hook_guard);

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"retry-write",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let get = coord
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
    assert_eq!(get.body.read_all().unwrap(), b"retry-write");
}

#[test]
fn delete_marker_persists_explicit_owner_identity() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner_canonical_id = CanonicalUserId::from_principal("custom-delete-owner");
    let owner = AccountIdentity::new("owner-a", owner_canonical_id.clone(), "Owner A");
    let requester = Requester::authenticated(owner.clone());

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
    coord
        .put_object(&PutObjectRequest {
            object: object_request_with_expected_owner("bucket", "key", requester.clone(), None),
            data: b"hello",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();

    coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            requester.clone(),
            false,
            NO_DELETE,
        ))
        .unwrap();

    let marker = coord
        .storage_node
        .test_get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
        .unwrap();
    let marker = match marker {
        StoredObject::DeleteMarker(marker) => marker,
        other => panic!("expected delete marker, got {other:?}"),
    };
    assert_eq!(marker.owner.principal, owner.principal());
    assert_eq!(marker.owner.canonical_id, owner_canonical_id);
}

#[test]
fn multipart_upload_and_complete_persist_explicit_owner_identity() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner_canonical_id = CanonicalUserId::from_principal("custom-mpu-owner");
    let owner = AccountIdentity::new("owner-a", owner_canonical_id.clone(), "Owner A");
    let requester = Requester::authenticated(owner.clone());
    let expected_owner =
        OwnerIdentity::new(owner.principal().to_string(), owner_canonical_id.clone());

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

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", requester.clone(), None),
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

    let upload_record = coord
        .storage_node
        .test_get_multipart_upload(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &upload.upload_id,
        )
        .unwrap();
    assert_eq!(upload_record.initiator, Some(expected_owner.clone()));
    assert_eq!(upload_record.owner, expected_owner);

    test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload.upload_id,
                requester.clone(),
                None,
            ),
            part_number: 1,
            data: b"multipart-data",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap();

    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload.upload_id,
                requester.clone(),
                None,
            ),
            parts: &[CompletePart {
                part_number: 1,
                etag: format_etag(checksum::crc64::checksum(b"multipart-data")),
                checksum: None,
            }],
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap();

    let live = coord
        .storage_node
        .test_get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
        .unwrap();
    let live = live.into_live().expect("expected completed object");
    assert_eq!(live.owner.principal, owner.principal());
    assert_eq!(live.owner.canonical_id, owner_canonical_id);
}

#[test]
fn create_multipart_upload_bucket_owner_preferred_promotes_bucket_owner_with_full_control_acl() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let bucket_owner = AccountIdentity::new(
        "owner-a",
        CanonicalUserId::from_principal("bucket-owner-canonical"),
        "Bucket Owner",
    );
    let writer = AccountIdentity::new(
        "writer-a",
        CanonicalUserId::from_principal("writer-canonical"),
        "Writer",
    );

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(bucket_owner.clone()),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();
    put_bucket_canned_acl_test(
        &coord,
        "bucket",
        BucketAcl::PublicReadWrite,
        Requester::authenticated(bucket_owner.clone()),
        None,
    )
    .unwrap();
    put_bucket_ownership_controls_test(&coord,
            "bucket",
            "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule></OwnershipControls>",
            Requester::authenticated(bucket_owner.clone()), None)
        .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                Requester::authenticated(writer.clone()),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: PutObjectAcl::BucketOwnerFullControl.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let upload_record = coord
        .storage_node
        .test_get_multipart_upload(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &upload.upload_id,
        )
        .unwrap();
    assert_eq!(
        upload_record.initiator,
        Some(OwnerIdentity::new(
            writer.principal().to_string(),
            writer.canonical_user_id().clone(),
        ))
    );
    assert_eq!(
        upload_record.owner,
        OwnerIdentity::new(
            bucket_owner.principal().to_string(),
            bucket_owner.canonical_user_id().clone(),
        )
    );
}

#[test]
fn create_bucket_idempotent_create_does_not_overwrite_ownership_controls() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap();

    let controls = get_bucket_ownership_controls_test(
        &coord,
        "bucket",
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        controls,
        BucketOwnershipControls {
            object_ownership: BucketObjectOwnership::ObjectWriter,
        }
    );
}

#[test]
fn create_bucket_rejects_public_read_with_owner_enforced() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::Canned(BucketAcl::PublicRead),
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap_err();
    assert!(matches!(
        err,
        ServerError::InvalidBucketAclWithObjectOwnership
    ));
}

#[test]
fn create_bucket_rejects_public_read_with_object_writer() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::Canned(BucketAcl::PublicRead),
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap_err();
    assert!(matches!(
        err,
        ServerError::InvalidBucketAclWithBlockPublicAccessError
    ));
}

#[test]
fn create_bucket_allows_default_private_with_owner_enforced() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap();

    let controls = get_bucket_ownership_controls_test(
        &coord,
        "bucket",
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        controls,
        BucketOwnershipControls {
            object_ownership: BucketObjectOwnership::BucketOwnerEnforced,
        }
    );
}

#[test]
fn get_bucket_acl_bucket_owner_enforced_allows_same_account_owner_view() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let bucket_owner = AccountIdentity::new(
        "arn:aws:iam::111122223333:root",
        CanonicalUserId::from_principal("bucket-owner-acl-canonical"),
        "Bucket Owner",
    );
    let same_account_user = AccountIdentity::new(
        "arn:aws:iam::111122223333:user/reader",
        CanonicalUserId::from_principal("bucket-same-account-acl-canonical"),
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
    put_bucket_ownership_controls_test(
        &coord,
        "bucket",
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        owner_requester.clone(),
        None,
    )
    .unwrap();

    let boe_acl = get_bucket_acl_test(&coord, "bucket", same_account_requester, None).unwrap();
    assert_eq!(
        boe_acl.owner_canonical_id,
        bucket_owner.canonical_user_id().clone()
    );
    assert_eq!(boe_acl.acl_grants.iter().count(), 1);
    assert!(boe_acl.acl_grants.iter().any(|grant| {
        grant
            == &AclGrant::new(
                AclGrantee::CanonicalUser(bucket_owner.canonical_user_id().clone()),
                AclPermission::FullControl,
            )
    }));
}

#[test]
fn authorize_get_bucket_acl_bucket_owner_enforced_allows_same_account_owner_view() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let bucket_owner = AccountIdentity::new(
        "arn:aws:iam::111122223333:root",
        CanonicalUserId::from_principal("bucket-owner-acl-canonical"),
        "Bucket Owner",
    );
    let same_account_user = AccountIdentity::new(
        "arn:aws:iam::111122223333:user/reader",
        CanonicalUserId::from_principal("bucket-same-account-acl-canonical"),
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
    put_bucket_ownership_controls_test(
        &coord,
        "bucket",
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        owner_requester,
        None,
    )
    .unwrap();

    let authorized = coord
        .authorize_get_bucket_acl(&bucket_request_with_expected_owner(
            "bucket",
            same_account_requester,
            None,
        ))
        .unwrap();
    assert_eq!(
        authorized.result.owner_canonical_id,
        bucket_owner.canonical_user_id().clone()
    );
    assert_eq!(authorized.result.acl_grants.iter().count(), 1);
    assert!(authorized.result.acl_grants.iter().any(|grant| {
        grant
            == &AclGrant::new(
                AclGrantee::CanonicalUser(bucket_owner.canonical_user_id().clone()),
                AclPermission::FullControl,
            )
    }));
}

#[test]
fn create_bucket_rejects_explicit_private_with_owner_enforced() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::Canned(BucketAcl::Private),
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap_err();
    assert!(matches!(
        err,
        ServerError::InvalidBucketAclWithObjectOwnership
    ));
}

#[test]
fn create_bucket_persists_explicit_grants() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::new(
        "owner-a",
        CanonicalUserId::from_principal("owner-create-grants-canonical"),
        "Owner A",
    );
    let writer = AccountIdentity::new(
        "writer-a",
        CanonicalUserId::from_principal("writer-create-grants-canonical"),
        "Writer A",
    );
    let owner_requester = Requester::authenticated(owner.clone());
    let writer_requester = Requester::authenticated(writer.clone());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: owner_requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::Grants(AclGrants::new(vec![
                AclGrant::new(
                    AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
                    AclPermission::Read,
                ),
                AclGrant::new(
                    AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
                    AclPermission::Write,
                ),
                AclGrant::new(
                    AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
                    AclPermission::ReadAcp,
                ),
                AclGrant::new(
                    AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
                    AclPermission::WriteAcp,
                ),
                AclGrant::new(
                    AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
                    AclPermission::FullControl,
                ),
            ])),
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    let acl = get_bucket_acl_test(&coord, "bucket", owner_requester.clone(), None).unwrap();
    assert!(grants_contain(
        &acl.acl_grants,
        &AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
        AclPermission::Read,
    ));
    assert!(grants_contain(
        &acl.acl_grants,
        &AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
        AclPermission::Write,
    ));
    assert!(grants_contain(
        &acl.acl_grants,
        &AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
        AclPermission::ReadAcp,
    ));
    assert!(grants_contain(
        &acl.acl_grants,
        &AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
        AclPermission::WriteAcp,
    ));
    assert!(grants_contain(
        &acl.acl_grants,
        &AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
        AclPermission::FullControl,
    ));
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", writer_requester, None),
            data: b"granted-write",
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
fn list_buckets_with_sparse_pg_topology() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0, 2, 5]);
    let coord = setup_coordinator_with_storage_cluster(storage_cluster);
    let bucket = "bucket-sparse";
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let names: Vec<String> = coord
        .list_buckets(&ListBucketsRequest {
            requester: test_helpers::requester("default-owner"),
        })
        .unwrap()
        .into_iter()
        .map(|b| b.name.into_string())
        .collect();
    assert_eq!(names, vec![bucket]);
}

#[test]
fn list_objects_with_sparse_pg_topology() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0, 2, 5]);
    let coord = setup_coordinator_with_storage_cluster(storage_cluster);
    let bucket = "bucket-sparse";
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let resp = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();
    assert!(resp.objects.is_empty());
}

#[test]
fn list_object_versions_with_sparse_pg_topology() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0, 2, 5]);
    let coord = setup_coordinator_with_storage_cluster(storage_cluster);
    let bucket = "bucket-sparse";
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let resp = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();
    assert!(resp.versions.is_empty());
}

#[test]
fn list_object_versions_clamps_oversized_max_keys() {
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

    for index in 0..1005 {
        let key = format!("key-{index:04}");
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", &key, test_requester(), None),
                data: b"value",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let resp = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 5000,
            requested_max_keys: Some(5000),
        })
        .unwrap();

    assert_eq!(resp.versions.len(), 1000);
    assert!(resp.is_truncated);
    assert_eq!(resp.next_key_marker.as_deref(), Some("key-0999"));
    assert_eq!(resp.next_version_id_marker, Some(VersionId::from_u64(1)));
}

#[test]
fn list_object_versions_paginates_across_pgs() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0, 2, 5]);
    let coord = setup_coordinator_with_storage_cluster(storage_cluster);
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

    let key_a = find_key_with_object_pg_distinct_from(&coord, "bucket", "a", &[]);
    let pg_a = object_pg_id(&coord, "bucket", &key_a);
    let key_b = find_key_with_object_pg_distinct_from(&coord, "bucket", "b", &[pg_a]);
    let pg_b = object_pg_id(&coord, "bucket", &key_b);
    let key_c = find_key_with_object_pg_distinct_from(&coord, "bucket", "c", &[pg_a, pg_b]);

    let older_a = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", &key_a, test_requester(), None),
            data: b"older-a",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let newer_a = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", &key_a, test_requester(), None),
            data: b"newer-a",
            metadata: &metadata,
            system_metadata: &system_metadata,
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
            object: object_request_with_expected_owner("bucket", &key_b, test_requester(), None),
            data: b"value-b",
            metadata: &metadata,
            system_metadata: &system_metadata,
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
            object: object_request_with_expected_owner("bucket", &key_c, test_requester(), None),
            data: b"value-c",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let first_page = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    assert_eq!(first_page.versions.len(), 2);
    assert_eq!(first_page.versions[0].key, key_a);
    assert_eq!(first_page.versions[0].version_id, newer_a.version_id);
    assert_eq!(first_page.versions[1].key, key_a);
    assert_eq!(first_page.versions[1].version_id, older_a.version_id);
    assert!(first_page.versions[0].is_latest);
    assert!(!first_page.versions[1].is_latest);
    assert!(first_page.is_truncated);
    assert_eq!(first_page.next_key_marker.as_deref(), Some(key_a.as_str()));
    assert_eq!(first_page.next_version_id_marker, Some(older_a.version_id));

    let second_page = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: first_page.next_key_marker.as_deref(),
            version_id_marker: first_page.next_version_id_marker,
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    assert_eq!(second_page.versions.len(), 2);
    assert_eq!(second_page.versions[0].key, key_b);
    assert_eq!(second_page.versions[0].version_id, VersionId::from_u64(1));
    assert!(second_page.versions[0].is_latest);
    assert_eq!(second_page.versions[1].key, key_c);
    assert_eq!(second_page.versions[1].version_id, VersionId::from_u64(1));
    assert!(second_page.versions[1].is_latest);
    assert!(!second_page.is_truncated);
    assert_eq!(second_page.next_key_marker, None);
    assert_eq!(second_page.next_version_id_marker, None);
}

#[test]
fn list_object_versions_with_delimiter_returns_common_prefixes() {
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

    for key in ["dir/a", "dir/b", "z.txt"] {
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", key, test_requester(), None),
                data: b"value",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let result = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            key_marker: None,
            version_id_marker: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();

    assert_eq!(result.common_prefixes, vec!["dir/".to_string()]);
    assert_eq!(result.versions.len(), 1);
    assert_eq!(result.versions[0].key, "z.txt");
    assert!(!result.is_truncated);
    assert_eq!(result.next_key_marker, None);
    assert_eq!(result.next_version_id_marker, None);
}

#[test]
fn list_object_versions_delimiter_paginates_common_prefixes() {
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

    for key in ["dir/a", "dir/b", "z.txt"] {
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", key, test_requester(), None),
                data: b"value",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let first_page = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            key_marker: None,
            version_id_marker: None,
            max_keys: 1,
            requested_max_keys: Some(1),
        })
        .unwrap();
    assert!(first_page.versions.is_empty());
    assert_eq!(first_page.common_prefixes, vec!["dir/".to_string()]);
    assert!(first_page.is_truncated);
    assert_eq!(first_page.next_key_marker.as_deref(), Some("dir/"));
    assert_eq!(first_page.next_version_id_marker, None);

    let second_page = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            key_marker: first_page.next_key_marker.as_deref(),
            version_id_marker: first_page.next_version_id_marker,
            max_keys: 1,
            requested_max_keys: Some(1),
        })
        .unwrap();
    assert!(second_page.common_prefixes.is_empty());
    assert_eq!(second_page.versions.len(), 1);
    assert_eq!(second_page.versions[0].key, "z.txt");
    assert!(!second_page.is_truncated);
    assert_eq!(second_page.next_key_marker, None);
    assert_eq!(second_page.next_version_id_marker, None);
}

#[test]
fn list_object_versions_delimiter_filters_common_prefix_at_or_before_key_marker() {
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

    for key in ["allowed/again", "allowed/versioned", "z.txt"] {
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", key, test_requester(), None),
                data: b"value",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let result = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            key_marker: Some("allowed/again"),
            version_id_marker: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();

    assert!(result.common_prefixes.is_empty());
    assert_eq!(result.versions.len(), 1);
    assert_eq!(result.versions[0].key, "z.txt");
    assert!(!result.is_truncated);
}

#[test]
fn list_multipart_uploads_with_sparse_pg_topology() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0, 2, 5]);
    let coord = setup_coordinator_with_storage_cluster(storage_cluster);
    let bucket = "bucket-sparse";
    let key = "key-sparse";
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(bucket, key, test_requester(), None),
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

    let resp = coord
        .list_multipart_uploads(&ListMultipartUploadsRequest {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 1000,
        })
        .unwrap();
    assert_eq!(resp.uploads.len(), 1);
    assert_eq!(resp.uploads[0].key, key);
}

#[test]
fn delete_nonempty_bucket_fails() {
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

    let err = delete_bucket_test(&coord, "bucket").unwrap_err();
    assert!(matches!(err, ServerError::BucketNotEmpty));
}

#[test]
fn put_object_persists_tags_in_initial_write() {
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
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(tags_xml),
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
    assert_eq!(obj.tags.as_deref(), Some(tags_xml));
}

#[test]
fn put_object_with_tags_allows_same_account_owner_account() {
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

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(bucket_owner.clone()),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    let tags_xml =
        "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                Requester::authenticated_owner_account_admin(same_account_account_principal),
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

    let tags = get_object_tags_test(
        &coord,
        "bucket",
        "key",
        None,
        Requester::authenticated(bucket_owner),
        None,
    )
    .unwrap();
    assert_eq!(tags.as_deref(), Some(tags_xml));
}

#[test]
fn put_object_does_not_wait_for_bucket_lock() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-put-no-lock";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_coordinator_with_storage_cluster_without_lifecycle_sweeper(Arc::clone(
        &storage_cluster,
    ));
    let writer = setup_coordinator_with_storage_cluster_without_lifecycle_sweeper(Arc::clone(
        &storage_cluster,
    ));
    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _write_handle_serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let guard = storage_cluster.test_lock_bucket(&trusted_bucket_name(bucket));
    let (event_tx, event_rx) = mpsc::channel();
    let event_tx_lock = event_tx.clone();
    let _storage_hook_guard = install_bucket_scoped_test_hooks(BucketScopedTestHooks {
        target: Some(trusted_bucket_name(bucket)),
        before_bucket_lock_acquire: Some(Arc::new(move || {
            let _ = event_tx_lock.send(LockWaitEvent::UnexpectedBucketLock);
        })),
        ..BucketScopedTestHooks::default()
    });
    let _write_handle_hook_guard =
        install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
            bucket: Some(bucket.to_string()),
            after_loaded: Some(Arc::new(move || {
                let _ = event_tx.send(LockWaitEvent::Progress);
            })),
            ..BucketWriteHandleTestHooks::default()
        });
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = test_helpers::put_object(
            &writer,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(bucket, "key", test_requester(), None),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        );
        tx.send(res).unwrap();
    });

    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    drop(guard);
    let res = rx.recv().unwrap();
    assert!(
        res.is_ok(),
        "put_object should succeed without waiting on bucket lock: {res:?}"
    );
    handle.join().unwrap();
}

#[test]
fn create_multipart_upload_does_not_wait_for_bucket_lock() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-create-mpu-no-lock";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_coordinator_with_storage_cluster_without_lifecycle_sweeper(Arc::clone(
        &storage_cluster,
    ));
    let creator = setup_coordinator_with_storage_cluster_without_lifecycle_sweeper(Arc::clone(
        &storage_cluster,
    ));
    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _write_handle_serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let guard = storage_cluster.test_lock_bucket(&trusted_bucket_name(bucket));
    let (event_tx, event_rx) = mpsc::channel();
    let event_tx_lock = event_tx.clone();
    let _storage_hook_guard = install_bucket_scoped_test_hooks(BucketScopedTestHooks {
        target: Some(trusted_bucket_name(bucket)),
        before_bucket_lock_acquire: Some(Arc::new(move || {
            let _ = event_tx_lock.send(LockWaitEvent::UnexpectedBucketLock);
        })),
        ..BucketScopedTestHooks::default()
    });
    let _write_handle_hook_guard =
        install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
            bucket: Some(bucket.to_string()),
            after_loaded: Some(Arc::new(move || {
                let _ = event_tx.send(LockWaitEvent::Progress);
            })),
            ..BucketWriteHandleTestHooks::default()
        });
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let metadata = MetadataBlob::new();
        let res = creator.create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(bucket, "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        });
        tx.send(res).unwrap();
    });

    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    drop(guard);
    let res = rx.recv().unwrap();
    assert!(
        res.is_ok(),
        "create_multipart_upload should succeed without waiting on bucket lock: {res:?}"
    );
    handle.join().unwrap();
}

#[test]
fn delete_bucket_waits_for_bucket_write_handle_action() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-delete-waits-handle";
    let coord = Arc::new(setup_coordinator_with_pg_count(tmp.path(), 1));
    let requester = test_requester();

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name(bucket),
            requester: requester.clone(),
            acl: CreateBucketAcl::DefaultPrivate,
            namespace: BucketNamespace::Global,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap();

    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (drain_wait_tx, drain_wait_rx) = mpsc::channel();
    let (delete_tx, delete_rx) = mpsc::channel();
    let _hook_guard = install_bucket_scoped_test_hooks(BucketScopedTestHooks {
        target: Some(trusted_bucket_name(bucket)),
        before_bucket_write_drain_wait: Some(Arc::new(move || {
            let _ = drain_wait_tx.send(());
        })),
        ..BucketScopedTestHooks::default()
    });

    let write_coord = Arc::clone(&coord);
    let write_request = object_request(bucket, "key", requester.clone());
    let write_thread = thread::spawn(move || {
        write_coord.with_bucket_write_handle_for(
            &write_request,
            BucketHandleRequest::new(),
            |_bucket| {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok::<_, ServerError>(())
            },
        )
    });

    started_rx.recv().unwrap();

    let delete_coord = Arc::clone(&coord);
    let delete_request = BucketRequest {
        name: trusted_bucket_name(bucket),
        requester: requester.clone(),
        expected_bucket_owner: None,
    };
    let delete_thread = thread::spawn(move || {
        let result = delete_coord.delete_bucket(&delete_request);
        delete_tx.send(result).unwrap();
    });

    drain_wait_rx.recv().unwrap();
    assert!(matches!(
        delete_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));

    release_tx.send(()).unwrap();

    write_thread.join().unwrap().unwrap();
    delete_thread.join().unwrap();
    delete_rx.recv().unwrap().unwrap();
}

#[test]
fn delete_bucket_does_not_wait_for_bucket_lock() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-delete-no-lock";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_coordinator_with_storage_cluster_without_lifecycle_sweeper(Arc::clone(
        &storage_cluster,
    ));
    let deleter = setup_coordinator_with_storage_cluster_without_lifecycle_sweeper(Arc::clone(
        &storage_cluster,
    ));
    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let guard = storage_cluster.test_lock_bucket(&trusted_bucket_name(bucket));
    let (event_tx, event_rx) = mpsc::channel();
    let event_tx_lock = event_tx.clone();
    let _storage_hook_guard = install_bucket_scoped_test_hooks(BucketScopedTestHooks {
        target: Some(trusted_bucket_name(bucket)),
        before_bucket_lock_acquire: Some(Arc::new(move || {
            let _ = event_tx_lock.send(LockWaitEvent::UnexpectedBucketLock);
        })),
        after_begin_bucket_delete_drain: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        ..BucketScopedTestHooks::default()
    });
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = delete_bucket_test(&deleter, bucket);
        tx.send(res).unwrap();
    });

    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    drop(guard);
    let res = rx.recv().unwrap();
    assert!(
        res.is_ok(),
        "delete_bucket should succeed without waiting on bucket lock: {res:?}"
    );
    handle.join().unwrap();
}

#[test]
fn head_object_lazily_populates_bucket_fast_path_for_boe_bucket() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_pg_count(tmp.path(), METADATA_FANOUT_TEST_PG_COUNT);
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &coord,
        "bucket",
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_requester(),
        None,
    )
    .unwrap();
    let key = find_key_with_object_pg_ne_bucket_pg(&coord, "bucket", "head-fast");
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", &key, test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    coord.remove_bucket_fast_path(&trusted_bucket_name("bucket"));
    assert!(coord
        .get_bucket_fast_path(&trusted_bucket_name("bucket"))
        .is_none());

    let head = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                &key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(head.size, 4);

    let cached = coord
        .get_bucket_fast_path(&trusted_bucket_name("bucket"))
        .expect("head_object should populate BOE bucket fast path");
    assert_eq!(cached.name.as_str(), "bucket");
    assert_eq!(cached.state, BucketState::Active);
}

#[test]
fn head_object_waits_for_bucket_pg_when_non_boe_bucket_fast_path_is_warm() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-head-fast-no-pg";
    let pg_ids: Vec<u32> = (0..METADATA_FANOUT_TEST_PG_COUNT).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let key = find_key_with_object_pg_ne_bucket_pg(&admin, bucket, "head-fast");
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, &key, test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    reader.remove_bucket_fast_path(&trusted_bucket_name(bucket));
    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            panic!("non-BOE head_object should not use fast bucket path");
        })),
    });
    let bucket_pg = storage_cluster
        .test_lock_bucket_pg(&trusted_bucket_name(bucket))
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = reader.head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        });
        tx.send(res).unwrap();
    });

    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    assert!(
        rx.try_recv().is_err(),
        "head_object returned before bucket pg released"
    );
    drop(bucket_pg);
    let head = rx
        .recv()
        .expect("head_object should complete after bucket pg released")
        .unwrap();
    assert_eq!(head.size, 4);
    handle.join().unwrap();
}

#[test]
fn head_object_uses_validated_boe_fast_path_when_warm() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-head-boe-fast-no-pg";
    let pg_ids: Vec<u32> = (0..METADATA_FANOUT_TEST_PG_COUNT).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_requester(),
        None,
    )
    .unwrap();
    let key = find_key_with_object_pg_ne_bucket_pg(&admin, bucket, "head-boe-fast");
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, &key, test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    reader.remove_bucket_fast_path(&trusted_bucket_name(bucket));
    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel();
    let event_tx_load = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx_load.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
    });
    let head = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    assert_eq!(head.size, 4);
}

#[test]
fn head_object_uses_validated_boe_policy_and_abac_tags_fast_path_when_warm() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-head-policy-abac-fast";
    let pg_ids: Vec<u32> = (0..METADATA_FANOUT_TEST_PG_COUNT).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    admin
        .put_bucket_tags(&PutBucketConfigRequest {
            bucket: bucket_request_with_expected_owner(
                bucket,
                test_helpers::requester("111122223333"),
                None,
            ),
            config:
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
        })
        .unwrap();
    admin
        .put_bucket_abac(&PutBucketAbacRequest {
            bucket: bucket_request_with_expected_owner(
                bucket,
                test_helpers::requester("111122223333"),
                None,
            ),
            enabled: true,
        })
        .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-head-policy-abac-fast/*","Condition":{"StringEquals":{"s3:BucketTag/security":"public"}}}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    let key = find_key_with_object_pg_ne_bucket_pg(&admin, bucket, "head-policy-abac-fast");
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket,
                &key,
                test_helpers::requester("111122223333"),
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

    reader.remove_bucket_fast_path(&trusted_bucket_name(bucket));
    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();

    let cached = reader
        .get_bucket_fast_path(&trusted_bucket_name(bucket))
        .expect("head_object should populate policy/tag fast path");
    assert!(matches!(
        cached.policy,
        storage::BucketFastPathPolicy::Loaded(_)
    ));
    assert!(matches!(
        cached.tags,
        storage::BucketFastPathTags::Loaded(_)
    ));

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel();
    let event_tx_load = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx_load.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
    });
    let head = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    assert_eq!(head.size, 4);

    admin
        .put_bucket_tags_for_tag_resource(&PutBucketTagControlRequest {
            control: BucketTagControlRequest {
                bucket: bucket_request_with_expected_owner(
                    bucket,
                    test_helpers::requester("111122223333"),
                    None,
                ),
                account_id: "111122223333",
            },
            config:
                "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>",
            request_tags: &[],
        })
        .unwrap();
    assert!(admin
        .get_bucket_fast_path(&trusted_bucket_name(bucket))
        .is_some());
}

#[test]
fn head_object_fast_path_denies_with_non_matching_boe_abac_bucket_tags() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-head-policy-abac-fast-deny";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    admin
        .put_bucket_tags(&PutBucketConfigRequest {
            bucket: bucket_request_with_expected_owner(
                bucket,
                test_helpers::requester("111122223333"),
                None,
            ),
            config:
                "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>",
        })
        .unwrap();
    admin
        .put_bucket_abac(&PutBucketAbacRequest {
            bucket: bucket_request_with_expected_owner(
                bucket,
                test_helpers::requester("111122223333"),
                None,
            ),
            enabled: true,
        })
        .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-head-policy-abac-fast-deny/*","Condition":{"StringEquals":{"s3:BucketTag/security":"public"}}}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    let key = find_key_with_object_pg_ne_bucket_pg(&admin, bucket, "head-policy-abac-fast-deny");
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket,
                &key,
                test_helpers::requester("111122223333"),
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

    reader.remove_bucket_fast_path(&trusted_bucket_name(bucket));
    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_helpers::requester("111122223333"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();

    let cached = reader
        .get_bucket_fast_path(&trusted_bucket_name(bucket))
        .expect("head_object should populate policy/tag fast path");
    assert!(matches!(
        cached.policy,
        storage::BucketFastPathPolicy::Loaded(_)
    ));
    assert!(matches!(
        cached.tags,
        storage::BucketFastPathTags::Loaded(_)
    ));

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel();
    let event_tx_load = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx_load.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
    });
    let err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn head_object_reloads_after_boe_policy_mutation_rebuilds_fast_path() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-head-policy-cold-fallback";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader_after_reload = setup_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_requester(),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-head-policy-cold-fallback/*"}]}"#,
        test_requester(),
        None,
    )
    .unwrap();

    let key = find_key_with_object_pg_ne_bucket_pg(&admin, bucket, "head-policy-cold");
    let key_after_reload = key.clone();
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, &key, test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    reader.remove_bucket_fast_path(&trusted_bucket_name(bucket));
    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert!(reader
        .get_bucket_fast_path(&trusted_bucket_name(bucket))
        .is_some());

    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-head-policy-cold-fallback/*"}]}"#,
        test_requester(),
        None,
    )
    .unwrap();
    let cached = reader
        .get_bucket_fast_path(&trusted_bucket_name(bucket))
        .expect("policy mutation should leave cached entry in place");
    let raw = storage_cluster
        .test_head_bucket_raw(&trusted_bucket_name(bucket))
        .expect("policy mutation should leave bucket metadata readable");
    assert!(
        raw.bucket_execution_generation > cached.bucket_execution_generation,
        "bucket execution generation should advance on policy mutation"
    );
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&trusted_bucket_name(bucket)),
        Some(false),
        "same-process policy mutation should immediately mark the cached BOE entry stale"
    );

    {
        let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap();
        let saw_storage_load = Arc::new(AtomicBool::new(false));
        let saw_fast_path = Arc::new(AtomicBool::new(false));
        let saw_storage_load_hook = Arc::clone(&saw_storage_load);
        let saw_fast_path_hook = Arc::clone(&saw_fast_path);
        let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
            bucket: Some(bucket.to_string()),
            before_storage_load: Some(Arc::new(move || {
                saw_storage_load_hook.store(true, Ordering::SeqCst);
            })),
            after_policy_fast_path_hit: Some(Arc::new(move || {
                saw_fast_path_hook.store(true, Ordering::SeqCst);
            })),
        });
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let res = reader.head_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    bucket,
                    &key,
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            });
            tx.send(res).unwrap();
        });
        let head = rx
            .recv_timeout(TEST_EVENT_TIMEOUT)
            .expect("head_object should complete after storage reload")
            .unwrap();
        assert!(
            saw_storage_load.load(Ordering::SeqCst),
            "first read after policy mutation should reload from storage"
        );
        assert_eq!(head.size, 4);
        handle.join().unwrap();
    }

    {
        let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap();
        let saw_storage_load = Arc::new(AtomicBool::new(false));
        let saw_fast_path = Arc::new(AtomicBool::new(false));
        let saw_storage_load_hook = Arc::clone(&saw_storage_load);
        let saw_fast_path_hook = Arc::clone(&saw_fast_path);
        let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
            bucket: Some(bucket.to_string()),
            before_storage_load: Some(Arc::new(move || {
                saw_storage_load_hook.store(true, Ordering::SeqCst);
            })),
            after_policy_fast_path_hit: Some(Arc::new(move || {
                saw_fast_path_hook.store(true, Ordering::SeqCst);
            })),
        });
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let res = reader_after_reload.head_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    bucket,
                    &key_after_reload,
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            });
            tx.send(res).unwrap();
        });
        let head = rx
            .recv_timeout(TEST_EVENT_TIMEOUT)
            .expect("head_object should complete from rebuilt fast path")
            .unwrap();
        assert!(
            !saw_storage_load.load(Ordering::SeqCst),
            "rebuilt BOE entry should not reload from storage on the next read"
        );
        assert!(
            saw_fast_path.load(Ordering::SeqCst),
            "rebuilt BOE entry should serve the next read from the fast path"
        );
        assert_eq!(head.size, 4);
        handle.join().unwrap();
    }
}

#[test]
fn production_storage_cluster_constructors_share_bucket_fast_path_cache_across_coordinators() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-prod-shared-cache";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-prod-shared-cache/*"}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket,
                "key",
                test_helpers::requester("111122223333"),
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

    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert!(matches!(
        reader
            .get_bucket_fast_path(&trusted_bucket_name(bucket))
            .expect("reader should warm shared fast path")
            .policy,
        storage::BucketFastPathPolicy::Loaded(_)
    ));

    admin
        .delete_bucket_policy(&bucket_request_with_expected_owner(
            bucket,
            test_helpers::requester("111122223333"),
            None,
        ))
        .unwrap();

    assert!(reader
        .get_bucket_fast_path(&trusted_bucket_name(bucket))
        .is_some());

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });
    let err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn head_object_validates_independent_fast_path_before_stale_policy_allow() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-cross-process-tighten";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let writer =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-fast-path-cross-process-tighten/*"}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket,
                "key",
                test_helpers::requester("111122223333"),
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

    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    let bucket_name = trusted_bucket_name(bucket);
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );

    writer
        .delete_bucket_policy(&bucket_request_with_expected_owner(
            bucket,
            test_helpers::requester("111122223333"),
            None,
        ))
        .unwrap();
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true),
        "writer-side cache hints must not touch an independent reader cache"
    );

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });

    let err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
}

#[test]
fn head_object_validates_independent_fast_path_before_stale_policy_deny() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-cross-process-loosen";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let writer =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket,
                "key",
                test_helpers::requester("111122223333"),
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

    let err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
    let bucket_name = trusted_bucket_name(bucket);
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );

    put_bucket_policy_test(
        &writer,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-fast-path-cross-process-loosen/*"}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true),
        "writer-side cache hints must not touch an independent reader cache"
    );

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });

    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
}

#[test]
fn head_object_validates_independent_fast_path_before_stale_abac_tags() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-cross-process-tags";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let writer =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let owner_account = "111122223333";
    let owner_requester = test_helpers::requester(owner_account);

    admin
        .create_bucket_for_owner(owner_account, bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        owner_requester.clone(),
        None,
    )
    .unwrap();
    admin
        .put_bucket_tags(&PutBucketConfigRequest {
            bucket: bucket_request_with_expected_owner(bucket, owner_requester.clone(), None),
            config:
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
        })
        .unwrap();
    admin
        .put_bucket_abac(&PutBucketAbacRequest {
            bucket: bucket_request_with_expected_owner(bucket, owner_requester.clone(), None),
            enabled: true,
        })
        .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-fast-path-cross-process-tags/*","Condition":{"StringEquals":{"s3:BucketTag/security":"public"}}}]}"#,
        owner_requester.clone(),
        None,
    )
    .unwrap();
    let key = find_key_with_object_pg_ne_bucket_pg(&admin, bucket, "cross-process-tags");
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, &key, owner_requester.clone(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    let bucket_name = trusted_bucket_name(bucket);
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );
    let cached = reader
        .get_bucket_fast_path(&bucket_name)
        .expect("reader should warm shared fast path");
    assert!(matches!(
        cached.policy,
        storage::BucketFastPathPolicy::Loaded(_)
    ));
    assert!(matches!(
        cached.tags,
        storage::BucketFastPathTags::Loaded(_)
    ));

    writer
        .put_bucket_tags_for_tag_resource(&PutBucketTagControlRequest {
            control: BucketTagControlRequest {
                bucket: bucket_request_with_expected_owner(bucket, owner_requester, None),
                account_id: owner_account,
            },
            config:
                "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>",
            request_tags: &[],
        })
        .unwrap();
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true),
        "writer-side cache hints must not touch an independent reader cache"
    );

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });

    let err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
}

#[test]
fn get_object_validates_independent_fast_path_before_stale_ownership_controls() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-cross-process-ownership";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let writer =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let owner_canonical_id = CanonicalUserId::from_principal("owner-a");

    create_bucket_for_owner_with_flags(
        &admin,
        "owner-a",
        &owner_canonical_id,
        bucket,
        false,
        false,
        false,
    )
    .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>ObjectWriter</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"writer-a"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket-fast-path-cross-process-ownership/*"}]}"#,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket,
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
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"owner-a"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-fast-path-cross-process-ownership/*"}]}"#,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    let warm = reader
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("owner-a"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(warm.body.read_all().unwrap(), b"writer-owned");
    let bucket_name = trusted_bucket_name(bucket);
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );

    writer
        .delete_bucket_ownership_controls(&bucket_request_with_expected_owner(
            bucket,
            test_helpers::requester("owner-a"),
            None,
        ))
        .unwrap();
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true),
        "writer-side cache hints must not touch an independent reader cache"
    );

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });

    let err = reader
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("owner-a"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
}

#[test]
fn get_object_validates_independent_fast_path_before_stale_public_access_block() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-cross-process-pab";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let writer =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let owner_requester = test_helpers::requester("111122223333");

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        owner_requester.clone(),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket,
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
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-fast-path-cross-process-pab/*"},{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-fast-path-cross-process-pab/*"}]}"#,
        owner_requester.clone(),
        None,
    )
    .unwrap();

    reader
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    let bucket_name = trusted_bucket_name(bucket);
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );

    put_bucket_public_access_block_test(
        &writer,
        bucket,
        "<PublicAccessBlockConfiguration><BlockPublicAcls>false</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>false</BlockPublicPolicy><RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
        owner_requester,
        None,
    )
    .unwrap();
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true),
        "writer-side cache hints must not touch an independent reader cache"
    );

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });

    let err = reader
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
}

#[test]
fn head_object_bypasses_fast_path_when_identity_validation_fails() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-identity-load-failure";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let coord = setup_process_isolated_cache_coordinator_with_storage_cluster(storage_cluster);
    let owner_requester = test_helpers::requester("111122223333");

    coord
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &coord,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        owner_requester.clone(),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
        &coord,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-fast-path-identity-load-failure/*"}]}"#,
        owner_requester.clone(),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, "key", owner_requester, None),
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
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    let bucket_name = trusted_bucket_name(bucket);
    assert_eq!(
        coord.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _identity_load_error_guard =
        install_bucket_fast_path_identity_load_error_test_hook(bucket.to_string());
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });

    let head = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(head.size, 4);
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    assert!(matches!(
        event_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    assert_eq!(
        coord.bucket_fast_path_is_fresh_for_test(&bucket_name),
        None,
        "failed identity validation should remove the cached fast-path entry"
    );
}

#[test]
fn parsed_policy_cache_bypasses_fast_path_when_identity_validation_fails() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-parsed-policy-identity-load-failure";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let coord = setup_process_isolated_cache_coordinator_with_storage_cluster(storage_cluster);
    let owner_requester = test_helpers::requester("111122223333");

    coord
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &coord,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        owner_requester.clone(),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
        &coord,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-parsed-policy-identity-load-failure/*"}]}"#,
        owner_requester.clone(),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, "key", owner_requester, None),
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
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    let bucket_name = trusted_bucket_name(bucket);
    let bucket_summary = coord.unchecked_active_bucket_summary(bucket).unwrap();
    assert_eq!(
        coord.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );
    assert!(
        coord
            .get_bucket_fast_path(&bucket_name)
            .expect("head_object should populate BOE fast-path policy")
            .bucket_policy_present
    );

    let _identity_load_error_guard =
        install_bucket_fast_path_identity_load_error_test_hook(bucket.to_string());
    let parsed_policy = coord.cached_bucket_policy(&bucket_summary).unwrap();

    assert!(
        parsed_policy.is_some(),
        "loaded bucket policy fallback should still parse after the cached policy proof fails"
    );
    assert_eq!(
        coord.bucket_fast_path_is_fresh_for_test(&bucket_name),
        None,
        "failed parsed-policy identity validation should remove the cached fast-path entry"
    );
}

#[test]
fn head_object_rejects_old_incarnation_fast_path_after_delete_recreate() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-cross-process-recreate";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-fast-path-cross-process-recreate/*"},{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket-fast-path-cross-process-recreate"}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    let warm_err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "missing-key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(
        matches!(warm_err, ServerError::ObjectNotFound { .. }),
        "unexpected warm error: {warm_err:?}"
    );
    let bucket_name = trusted_bucket_name(bucket);
    let cached_identity = reader
        .get_bucket_fast_path(&bucket_name)
        .expect("BOE read should warm cache")
        .identity();
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );

    storage_cluster.begin_bucket_delete(&bucket_name).unwrap();
    storage_cluster
        .test_delete_bucket_metadata(&bucket_name)
        .unwrap();
    let recreated_owner = CanonicalUserId::from_principal("777788889999");
    storage_cluster
        .create_bucket_with_config_and_load_info(&storage::CreateBucketConfig {
            name: bucket,
            owner_principal: "777788889999",
            owner_canonical_id: &recreated_owner,
            acl_grants: &AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
        })
        .unwrap();
    let recreated = storage_cluster
        .load_bucket_fast_path_identity(&bucket_name)
        .unwrap()
        .expect("recreated bucket should have a fast-path identity");
    assert_ne!(
        recreated.bucket_incarnation_generation, cached_identity.bucket_incarnation_generation,
        "delete/recreate must change the bucket incarnation used by cache validation"
    );
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true),
        "isolated reader cache should not receive writer-side invalidation"
    );

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });

    let err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "missing-key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    assert!(matches!(
        event_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    assert!(matches!(err, ServerError::AccessDenied));
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        None,
        "old-incarnation cache entry should be removed after request-time validation"
    );
}

#[test]
fn bucket_fast_path_watcher_survives_first_cluster_handle_drop() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-watch-first-handle-drop";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-fast-path-watch-first-handle-drop/*"}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket,
                "key",
                test_helpers::requester("111122223333"),
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

    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    let bucket_name = trusted_bucket_name(bucket);
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );

    drop(admin);
    storage_cluster
        .delete_bucket_subresource_and_load_info(
            &bucket_name,
            storage::BucketSubresourceKind::Policy,
        )
        .unwrap();

    let start = std::time::Instant::now();
    while reader.bucket_fast_path_is_fresh_for_test(&bucket_name) != Some(false) {
        assert!(
            start.elapsed() < BUCKET_FAST_PATH_WATCH_TEST_TIMEOUT,
            "bucket fast path watcher stopped after first cluster handle was dropped"
        );
        thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[test]
fn bucket_fast_path_watcher_observes_direct_storage_policy_mutation() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-watch-direct-policy";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-fast-path-watch-direct-policy/*"}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket,
                "key",
                test_helpers::requester("111122223333"),
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

    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    let bucket_name = trusted_bucket_name(bucket);
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );

    storage_cluster
        .delete_bucket_subresource_and_load_info(
            &bucket_name,
            storage::BucketSubresourceKind::Policy,
        )
        .unwrap();

    let start = std::time::Instant::now();
    while reader.bucket_fast_path_is_fresh_for_test(&bucket_name) != Some(false) {
        assert!(
            start.elapsed() < BUCKET_FAST_PATH_WATCH_TEST_TIMEOUT,
            "bucket fast path watcher did not observe direct storage policy mutation"
        );
        thread::sleep(std::time::Duration::from_millis(10));
    }

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });
    let err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn bucket_fast_path_watcher_observes_direct_storage_delete_recreate() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-watch-direct-recreate";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    let warm_err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "missing-key",
                None,
                test_helpers::requester("111122223333"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(
        matches!(warm_err, ServerError::ObjectNotFound { .. }),
        "unexpected warm error: {warm_err:?}"
    );
    let bucket_name = trusted_bucket_name(bucket);
    let cached_generation = reader
        .get_bucket_fast_path(&bucket_name)
        .expect("BOE read should warm cache")
        .bucket_execution_generation;
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );

    storage_cluster.begin_bucket_delete(&bucket_name).unwrap();
    storage_cluster
        .test_delete_bucket_metadata(&bucket_name)
        .unwrap();
    let recreated_owner = CanonicalUserId::from_principal("777788889999");
    storage_cluster
        .create_bucket_with_config_and_load_info(&storage::CreateBucketConfig {
            name: bucket,
            owner_principal: "777788889999",
            owner_canonical_id: &recreated_owner,
            acl_grants: &AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
        })
        .unwrap();
    let recreated = storage_cluster.test_head_bucket_raw(&bucket_name).unwrap();
    assert!(
        recreated.bucket_execution_generation > cached_generation,
        "delete/recreate must advance authoritative bucket execution generation"
    );

    let start = std::time::Instant::now();
    while reader.bucket_fast_path_is_fresh_for_test(&bucket_name) == Some(true) {
        assert!(
            start.elapsed() < BUCKET_FAST_PATH_WATCH_TEST_TIMEOUT,
            "bucket fast path watcher did not invalidate after direct storage delete/recreate"
        );
        thread::sleep(std::time::Duration::from_millis(10));
    }

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });
    let err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "missing-key",
                None,
                test_helpers::requester("111122223333"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn bucket_fast_path_watcher_recovers_after_observing_missing_bucket_before_recreate() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-watch-delete-then-recreate";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    let warm_err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "missing-key",
                None,
                test_helpers::requester("111122223333"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(
        matches!(warm_err, ServerError::ObjectNotFound { .. }),
        "unexpected warm error: {warm_err:?}"
    );
    let bucket_name = trusted_bucket_name(bucket);
    assert!(
        reader.get_bucket_fast_path(&bucket_name).is_some(),
        "BOE read should warm cache"
    );

    storage_cluster.begin_bucket_delete(&bucket_name).unwrap();
    storage_cluster
        .test_delete_bucket_metadata(&bucket_name)
        .unwrap();

    let start = std::time::Instant::now();
    while reader.get_bucket_fast_path(&bucket_name).is_some() {
        assert!(
            start.elapsed() < BUCKET_FAST_PATH_WATCH_TEST_TIMEOUT,
            "bucket fast path watcher did not remove cache entry after direct delete"
        );
        thread::sleep(std::time::Duration::from_millis(10));
    }

    let recreated_owner = CanonicalUserId::from_principal("111122223333");
    storage_cluster
        .create_bucket_with_config_and_load_info(&storage::CreateBucketConfig {
            name: bucket,
            owner_principal: "111122223333",
            owner_canonical_id: &recreated_owner,
            acl_grants: &AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
        })
        .unwrap();
    storage_cluster
        .put_bucket_ownership_controls_and_load_info(
            &bucket_name,
            BucketOwnershipControls {
                object_ownership: BucketObjectOwnership::BucketOwnerEnforced,
            },
        )
        .unwrap();

    let reload_err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "missing-key",
                None,
                test_helpers::requester("111122223333"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(
        matches!(reload_err, ServerError::ObjectNotFound { .. }),
        "unexpected reload error: {reload_err:?}"
    );
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true),
        "recreated bucket should repopulate a fresh BOE fast-path entry"
    );
}

#[test]
fn put_bucket_tags_invalidates_warm_fast_path_tags() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-put-tags-invalidates-fast-path";
    let coord = setup_coordinator_with_pg_count(tmp.path(), METADATA_FANOUT_TEST_PG_COUNT);
    let owner_account = "111122223333";
    let owner_requester = test_helpers::requester(owner_account);

    coord
        .create_bucket_for_owner(owner_account, bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &coord,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        owner_requester.clone(),
        None,
    )
    .unwrap();
    coord
        .put_bucket_tags(&PutBucketConfigRequest {
            bucket: bucket_request_with_expected_owner(bucket, owner_requester.clone(), None),
            config:
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
        })
        .unwrap();
    coord
        .put_bucket_abac(&PutBucketAbacRequest {
            bucket: bucket_request_with_expected_owner(bucket, owner_requester.clone(), None),
            enabled: true,
        })
        .unwrap();
    let key = find_key_with_object_pg_ne_bucket_pg(&coord, bucket, "put-tags-invalidates");
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, &key, owner_requester.clone(), None),
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
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                owner_requester.clone(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();

    let cached = coord
        .get_bucket_fast_path(&trusted_bucket_name(bucket))
        .expect("bucket fast path should be populated");
    assert!(matches!(
        cached.tags,
        storage::BucketFastPathTags::Loaded(_)
    ));

    coord
        .put_bucket_tags_for_tag_resource(&PutBucketTagControlRequest {
            control: BucketTagControlRequest {
                bucket: bucket_request_with_expected_owner(bucket, owner_requester, None),
                account_id: owner_account,
            },
            config:
                "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>",
            request_tags: &[],
        })
        .unwrap();

    assert!(coord
        .get_bucket_fast_path(&trusted_bucket_name(bucket))
        .is_some());

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });
    coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_helpers::requester(owner_account),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
}

#[test]
fn delete_object_falls_back_to_storage_load_when_bucket_fast_path_is_acl_free() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-delete-fast-no-pg";
    let pg_ids: Vec<u32> = (0..METADATA_FANOUT_TEST_PG_COUNT).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let deleter = setup_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let key = find_key_with_object_pg_ne_bucket_pg(&admin, bucket, "delete-fast");
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, &key, test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    deleter.remove_bucket_fast_path(&trusted_bucket_name(bucket));
    admin
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            panic!("delete_object should not use ACL-free fast bucket path");
        })),
    });
    let bucket_pg = storage_cluster
        .test_lock_bucket_pg(&trusted_bucket_name(bucket))
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let key_for_delete = key.clone();
    let handle = thread::spawn(move || {
        let res = deleter.delete_object(&delete_object_request(
            bucket,
            &key_for_delete,
            None,
            test_requester(),
            false,
            NO_DELETE,
        ));
        tx.send(res).unwrap();
    });

    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    assert!(
        rx.try_recv().is_err(),
        "delete_object returned before bucket pg released"
    );
    drop(bucket_pg);
    let deleted = rx
        .recv()
        .expect("delete_object should complete after bucket pg released")
        .unwrap();
    assert!(!deleted.delete_marker);
    assert!(matches!(
        admin.get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_requester(),
                None
            ),
            cond: NO_READ,
        }),
        Err(ServerError::ObjectNotFound { .. })
    ));
    handle.join().unwrap();
}

#[test]
fn complete_multipart_upload_does_not_wait_for_bucket_lock() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-complete-no-lock";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_coordinator_with_storage_cluster_without_lifecycle_sweeper(Arc::clone(
        &storage_cluster,
    ));
    let completer = setup_coordinator_with_storage_cluster_without_lifecycle_sweeper(Arc::clone(
        &storage_cluster,
    ));
    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let (upload_id, parts) = create_upload_with_parts(&admin, bucket, "key", &[(1, b"part")]);

    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _write_handle_serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let guard = storage_cluster.test_lock_bucket(&trusted_bucket_name(bucket));
    let (event_tx, event_rx) = mpsc::channel();
    let event_tx_lock = event_tx.clone();
    let _storage_hook_guard = install_bucket_scoped_test_hooks(BucketScopedTestHooks {
        target: Some(trusted_bucket_name(bucket)),
        before_bucket_lock_acquire: Some(Arc::new(move || {
            let _ = event_tx_lock.send(LockWaitEvent::UnexpectedBucketLock);
        })),
        ..BucketScopedTestHooks::default()
    });
    let _write_handle_hook_guard =
        install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
            bucket: Some(bucket.to_string()),
            after_loaded: Some(Arc::new(move || {
                let _ = event_tx.send(LockWaitEvent::Progress);
            })),
            ..BucketWriteHandleTestHooks::default()
        });
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = completer.complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                bucket,
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            parts: &parts,
            claimed_checksum: None,

            expected_object_size: None,

            cond: &WriteCondition::default(),

            sse_customer: None,
        });
        tx.send(res).unwrap();
    });

    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    drop(guard);
    let res = rx.recv().unwrap();
    assert!(
        res.is_ok(),
        "complete_multipart_upload should succeed without waiting on bucket lock: {res:?}"
    );
    handle.join().unwrap();
}

#[test]
fn complete_multipart_upload_waits_for_multipart_completion_lock() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-complete-waits-lock";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_coordinator_with_storage_cluster_without_lifecycle_sweeper(Arc::clone(
        &storage_cluster,
    ));
    let completer = setup_coordinator_with_storage_cluster_without_lifecycle_sweeper(Arc::clone(
        &storage_cluster,
    ));
    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let (upload_id, parts) = create_upload_with_parts(&admin, bucket, "key", &[(1, b"part")]);

    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let reached_before_lock = Arc::new(Barrier::new(2));
    let reached_before_lock_hook = Arc::clone(&reached_before_lock);
    let (acquired_lock_tx, acquired_lock_rx) = mpsc::channel();
    let guard = storage_cluster.test_lock_multipart_completion_bucket(&trusted_bucket_name(bucket));
    let _hook_guard = install_bucket_scoped_test_hooks(BucketScopedTestHooks {
        target: Some(trusted_bucket_name(bucket)),
        before_multipart_completion_lock: Some(Arc::new(move || {
            reached_before_lock_hook.wait();
        })),
        after_multipart_completion_lock: Some(Arc::new(move || {
            let _ = acquired_lock_tx.send(());
        })),
        ..BucketScopedTestHooks::default()
    });
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = completer.complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                bucket,
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            parts: &parts,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        });
        tx.send(res).unwrap();
    });

    reached_before_lock.wait();
    assert!(matches!(
        acquired_lock_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
    drop(guard);
    acquired_lock_rx.recv().unwrap();
    let res = rx.recv().unwrap();
    assert!(
        res.is_ok(),
        "complete_multipart_upload should succeed after multipart completion lock is released: {res:?}"
    );
    handle.join().unwrap();
}

#[test]
fn complete_multipart_upload_does_not_deadlock_when_bucket_policy_shares_pg() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-complete-same-pg";
    let pg_ids: Vec<u32> = (0..METADATA_FANOUT_TEST_PG_COUNT).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_coordinator_with_storage_cluster_without_lifecycle_sweeper(Arc::clone(
        &storage_cluster,
    ));
    let completer = setup_coordinator_with_storage_cluster_without_lifecycle_sweeper(Arc::clone(
        &storage_cluster,
    ));

    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"default-owner"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket-complete-same-pg/*"}]}"#,
        test_requester(),
        None,
    )
    .unwrap();

    let key = find_key_with_object_pg_eq_bucket_pg(&admin, bucket, "same-pg");

    let (upload_id, parts) = create_upload_with_parts(&admin, bucket, &key, &[(1, b"part")]);

    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel();
    let event_tx_hook = event_tx.clone();
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), key.clone())),
        probe_multipart_complete_auth_lookup: true,
        after_multipart_complete_pre_commit: Some(Arc::new(move || {
            let _ = event_tx_hook.send(LockWaitEvent::Progress);
        })),
        ..ReclamationTestHooks::default()
    });
    let (tx, rx) = mpsc::channel();
    let event_tx_complete = event_tx.clone();
    let key_for_complete = key.clone();
    let handle = thread::spawn(move || {
        let res = completer.complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                bucket,
                &key_for_complete,
                &upload_id,
                test_requester(),
                None,
            ),
            parts: &parts,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        });
        let _ = event_tx_complete.send(LockWaitEvent::CompletedEarly);
        tx.send(res).unwrap();
    });

    let event = event_rx.recv().unwrap();
    let res = rx
        .recv()
        .expect("complete_multipart_upload should not deadlock on bucket policy lookup");
    assert_eq!(
        event,
        LockWaitEvent::Progress,
        "complete_multipart_upload returned before the expected progress point: {res:?}"
    );
    assert!(
        res.is_ok(),
        "complete_multipart_upload should succeed when bucket policy shares the metadata PG: {res:?}"
    );
    handle.join().unwrap();
}

#[test]
fn delete_bucket_rejects_active_stream_put_session() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    let err = delete_bucket_test(&coord, "bucket").unwrap_err();
    assert!(matches!(err, ServerError::BucketNotEmpty));

    coord
        .abort_stream_put("bucket", "key", &session_id)
        .unwrap();
    delete_bucket_test(&coord, "bucket").unwrap();
    wait_until_bucket_gone(&coord, "bucket");
}

#[test]
fn put_get_object() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let headers = [("Content-Type", "text/plain")];
    let metadata = MetadataBlob::from_headers(&headers).unwrap();
    let system_metadata = SystemMetadata::from_headers(&headers).unwrap();
    let result = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "hello.txt",
                test_requester(),
                None,
            ),
            data: b"Hello, world!",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    assert!(!result.etag.is_empty());

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "hello.txt",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"Hello, world!");
    assert_eq!(obj.size, 13);
    assert_eq!(
        obj.system_metadata.content_type().map(|v| v.as_str()),
        Some("text/plain")
    );
}

#[test]
fn put_get_with_metadata() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let headers = [
        ("Content-Type", "application/json"),
        ("X-Amz-Meta-Author", "alice"),
        ("X-Amz-Meta-Version", "42"),
    ];
    let metadata = MetadataBlob::from_headers(&headers).unwrap();
    let system_metadata = SystemMetadata::from_headers(&headers).unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "obj", test_requester(), None),
            data: b"{}",
            metadata: &metadata,
            system_metadata: &system_metadata,
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
                "obj",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"{}");
    assert_eq!(
        obj.system_metadata.content_type().map(|v| v.as_str()),
        Some("application/json")
    );
    assert_eq!(obj.metadata.get("x-amz-meta-author"), Some("alice"));
    assert_eq!(obj.metadata.get("x-amz-meta-version"), Some("42"));
}

#[test]
fn head_object() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let metadata = MetadataBlob::from_headers(&[("Content-Type", "text/plain")]).unwrap();
    let system_metadata = SystemMetadata::from_headers(&[("Content-Type", "text/plain")]).unwrap();
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
    assert_eq!(head.size, 4);
    assert_eq!(
        head.system_metadata.content_type().map(|v| v.as_str()),
        Some("text/plain")
    );
}

#[test]
fn overwrite_object() {
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
    assert_eq!(obj.body.read_all().unwrap(), b"v2");
}

#[test]
fn empty_object() {
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
            object: object_request_with_expected_owner("bucket", "empty", test_requester(), None),
            data: b"",
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
                "empty",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"");
    assert_eq!(obj.size, 0);
}

#[test]
fn delete_object_then_get_fails() {
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
    coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap();

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
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::ObjectNotFound { .. }));
}

#[test]
fn delete_object_eventually_reclaims_simple_shards() {
    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_reclaim_sweeper(tmp.path());

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
            data: b"simple-data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let (generation_id, ec, data_pg_id, okh, segment_vid) = {
        match coord
            .storage_node
            .test_get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
            .unwrap()
        {
            StoredObject::Live(record) => {
                let segments = coord
                    .storage_node
                    .test_get_object_segments(
                        &trusted_bucket_name("bucket"),
                        &trusted_object_key("key"),
                        record.version_id,
                    )
                    .unwrap();
                let segment = segments
                    .first()
                    .expect("direct put should store one segment");
                (
                    record.generation_id,
                    record.ec,
                    segment.data_pg_id,
                    segment.segment_okh,
                    segment.segment_vid,
                )
            }
            other @ StoredObject::DeleteMarker(_) => {
                panic!("expected live object, got {other:?}")
            }
        }
    };

    coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap();

    reclaim_object_payload(&coord, "bucket", "key", generation_id);
    assert_shard_set_deleted(&coord, data_pg_id, &okh, segment_vid, ec);
}

#[test]
fn shard_scavenger_worker_records_audit_observations() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_lifecycle_sweeper(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let reservation_id = storage::SessionId::try_from("77".repeat(16)).unwrap();
    let generation_id = coord
        .storage_node
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let written = coord
        .storage_node
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &[0xe7; 16],
            b"background shard scavenger audit candidate",
        )
        .unwrap();

    let start = std::time::Instant::now();
    loop {
        let observations = coord
            .storage_node
            .test_list_shard_scavenger_observations(written.data_pg_id)
            .unwrap();
        if written.written_shards.iter().all(|shard| {
            observations.iter().any(|observation| {
                observation.reason == ShardScavengerObservationReason::FileWithoutShardRow
                    && observation.resolved_at.is_none()
                    && observation.key.data_pg_id == written.data_pg_id
                    && observation.key.shard_key == shard.key
            })
        }) {
            return;
        }
        assert!(
            start.elapsed() < TEST_EVENT_TIMEOUT,
            "shard scavenger worker did not record file-without-row observations; observations={observations:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn reclaim_object_payload_delete_failure_keeps_retryable_reclaim_record() {
    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap();
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_reclaim_sweeper(tmp.path());

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
            data: b"simple-data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let (generation_id, ec, data_pg_id, okh, segment_vid) = {
        match coord
            .storage_node
            .test_get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
            .unwrap()
        {
            StoredObject::Live(record) => {
                let segments = coord
                    .storage_node
                    .test_get_object_segments(
                        &trusted_bucket_name("bucket"),
                        &trusted_object_key("key"),
                        record.version_id,
                    )
                    .unwrap();
                let segment = segments
                    .first()
                    .expect("direct put should store one segment");
                (
                    record.generation_id,
                    record.ec,
                    segment.data_pg_id,
                    segment.segment_okh,
                    segment.segment_vid,
                )
            }
            other @ StoredObject::DeleteMarker(_) => {
                panic!("expected live object, got {other:?}")
            }
        }
    };

    let failing_key = ShardKey::new(&okh, segment_vid.get(), 0);
    let placed_cleanup_guard = coord
        .storage_node
        .test_install_before_placed_payload_shard_delete_hook(Arc::new(move |shard_key| {
            if shard_key == &failing_key {
                return Err(storage::StoreError::Io {
                    context: "injected reclaim placed delete failure",
                    source: std::io::Error::other("injected reclaim placed delete failure"),
                });
            }
            Ok(())
        }));

    coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap();

    let err = coord
        .read_runtime()
        .try_reclaim_object_payload("bucket", "key", generation_id)
        .unwrap_err();
    assert!(
        matches!(
            err,
            ServerError::Store(storage::StoreError::Io {
                context: "injected reclaim placed delete failure",
                ..
            })
        ),
        "expected injected reclaim delete failure, got {err:?}"
    );
    for shard_index in 0..ec.k + ec.m {
        let shard_key = ShardKey::new(&okh, segment_vid.get(), shard_index);
        assert!(
            coord
                .storage_node
                .test_shard_exists(data_pg_id, &shard_key)
                .unwrap(),
            "failed reclaim should keep ack metadata {shard_index} retryable"
        );
        assert!(
            coord
                .storage_node
                .test_payload_shard_file_exists(data_pg_id, ec, &okh, segment_vid, shard_index)
                .unwrap(),
            "failed reclaim should keep placed shard {shard_index} retryable"
        );
    }

    drop(placed_cleanup_guard);
    reclaim_object_payload(&coord, "bucket", "key", generation_id);
    assert_shard_set_deleted(&coord, data_pg_id, &okh, segment_vid, ec);
}

#[test]
fn delete_nonexistent_object_is_ok() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    // Should not error
    coord
        .delete_object(&delete_object_request(
            "bucket",
            "no-such-key",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap();
}

#[test]
fn list_objects() {
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
            object: object_request_with_expected_owner("bucket", "a/1", test_requester(), None),
            data: b"1",
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
            object: object_request_with_expected_owner("bucket", "a/2", test_requester(), None),
            data: b"2",
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
            object: object_request_with_expected_owner("bucket", "b/1", test_requester(), None),
            data: b"3",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let result = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();
    assert_eq!(result.objects.len(), 3);
    // Should be sorted
    assert_eq!(result.objects[0].key, "a/1");
    assert_eq!(result.objects[1].key, "a/2");
    assert_eq!(result.objects[2].key, "b/1");
}

#[test]
fn list_objects_with_prefix() {
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
            object: object_request_with_expected_owner(
                "bucket",
                "photos/cat.jpg",
                test_requester(),
                None,
            ),
            data: b"cat",
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
                "bucket",
                "photos/dog.jpg",
                test_requester(),
                None,
            ),
            data: b"dog",
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
                "bucket",
                "docs/readme.md",
                test_requester(),
                None,
            ),
            data: b"md",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let result = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: Some("photos/"),
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();
    assert_eq!(result.objects.len(), 2);
}

#[test]
fn list_objects_with_delimiter() {
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
            object: object_request_with_expected_owner(
                "bucket",
                "photos/cat.jpg",
                test_requester(),
                None,
            ),
            data: b"cat",
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
                "bucket",
                "photos/dog.jpg",
                test_requester(),
                None,
            ),
            data: b"dog",
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
                "bucket",
                "docs/readme.md",
                test_requester(),
                None,
            ),
            data: b"md",
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
                "bucket",
                "root.txt",
                test_requester(),
                None,
            ),
            data: b"root",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let result = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();
    assert_eq!(result.objects.len(), 1);
    assert_eq!(result.objects[0].key, "root.txt");
    assert!(result.common_prefixes.contains(&"photos/".to_string()));
    assert!(result.common_prefixes.contains(&"docs/".to_string()));
}

#[test]
fn put_get_object_trailing_slash_key() {
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
            object: object_request_with_expected_owner("bucket", "folder/", test_requester(), None),
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
                "folder/",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"data");
    assert_eq!(obj.size, 4);
}

// ── Disk manipulation helpers for EC tests ────────────────────────

/// Compute shard file path on disk for a given object and shard index.
fn shard_file_path(coord: &Coordinator, bucket: &str, key: &str, shard_index: u8) -> PathBuf {
    let (data_pg_id, okh, generation_id, ec) = {
        let bucket_name = trusted_bucket_name(bucket);
        let object_key = trusted_object_key(key);
        let record = coord
            .storage_node
            .test_get_object_meta(&bucket_name, &object_key)
            .unwrap();
        let segments = coord
            .storage_node
            .test_get_object_segments(&bucket_name, &object_key, record.version_id())
            .unwrap();
        if let Some(segment) = segments.first() {
            (
                segment.data_pg_id,
                segment.segment_okh,
                segment.segment_vid,
                EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            )
        } else {
            let live = record.as_live().expect("expected live object");
            (
                object_data_pg_id(
                    coord,
                    bucket_name.as_str(),
                    object_key.as_str(),
                    live.generation_id,
                ),
                object_key_hash(bucket_name.as_str(), object_key.as_str()),
                live.generation_id,
                live.ec,
            )
        }
    };
    coord
        .storage_node
        .test_payload_shard_file_path(data_pg_id, ec, &okh, generation_id, shard_index)
        .unwrap()
}

/// Delete a specific shard file from disk.
fn delete_shard_on_disk(coord: &Coordinator, bucket: &str, key: &str, shard_index: u8) {
    let path = shard_file_path(coord, bucket, key, shard_index);
    std::fs::remove_file(&path).unwrap_or_else(|e| {
        panic!(
            "failed to delete shard {shard_index} at {}: {e}",
            path.display()
        )
    });
}

/// Corrupt a specific shard file on disk (flip first byte).
/// PgStore's read_shard will detect CRC mismatch.
fn corrupt_shard_on_disk(coord: &Coordinator, bucket: &str, key: &str, shard_index: u8) {
    let path = shard_file_path(coord, bucket, key, shard_index);
    let mut data = std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "failed to read shard {shard_index} at {}: {e}",
            path.display()
        )
    });
    assert!(!data.is_empty(), "shard file is empty");
    data[0] ^= 0xFF;
    std::fs::write(&path, &data).unwrap();
}

// ── EC fault injection tests ────────────────────────────────────

#[test]
fn ec_reconstruction_after_shard_loss() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"This data should survive shard loss!";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "resilient",
                test_requester(),
                None,
            ),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // Delete one data shard using the helper
    delete_shard_on_disk(&coord, "bucket", "resilient", 0);

    // Get should still succeed via EC reconstruction
    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "resilient",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), data);
}

#[test]
fn ec_drop_one_data_shard_get() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"EC single shard loss test data";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "obj1", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    delete_shard_on_disk(&coord, "bucket", "obj1", 0);

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj1",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), data);
}

#[test]
fn ec_degraded_read_reuses_reconstruction_scratch() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = vec![5u8; INTERNAL_SEGMENT_SIZE];
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "obj-reconstruct",
                test_requester(),
                None,
            ),
            data: &data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    delete_shard_on_disk(&coord, "bucket", "obj-reconstruct", 0);

    assert_eq!(coord.payload_buffer_pool.allocation_count(), 0);
    let ec = coord.storage_node.default_payload_ec_shape();
    assert_eq!(coord.storage_node.test_ec_scratch_allocation_count(ec), 1);

    let first = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj-reconstruct",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(first.body.read_all().unwrap(), data);
    assert_eq!(coord.payload_buffer_pool.allocation_count(), 1);
    assert_eq!(coord.storage_node.test_ec_scratch_allocation_count(ec), 1);

    let second = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj-reconstruct",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(second.body.read_all().unwrap(), data);
    assert_eq!(coord.payload_buffer_pool.allocation_count(), 1);
    assert_eq!(coord.storage_node.test_ec_scratch_allocation_count(ec), 1);
}

#[test]
fn ec_drop_m_shards_at_limit() {
    if !backend_supports_parity_recovery() {
        return;
    }
    // Config: k=4, m=2. Dropping exactly m=2 shards should still recover.
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"EC m-shard loss limit test data";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "obj2", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // Delete 2 data shards (indices 0 and 1)
    delete_shard_on_disk(&coord, "bucket", "obj2", 0);
    delete_shard_on_disk(&coord, "bucket", "obj2", 1);

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj2",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), data);
}

#[test]
fn ec_drop_m_plus_one_shards_fails() {
    // Config: k=4, m=2. Dropping m+1=3 shards should fail.
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"EC m+1 shard loss test data";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "obj3", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // Delete 3 shards (indices 0, 1, 2)
    delete_shard_on_disk(&coord, "bucket", "obj3", 0);
    delete_shard_on_disk(&coord, "bucket", "obj3", 1);
    delete_shard_on_disk(&coord, "bucket", "obj3", 2);

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj3",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    let err = obj.body.read_all().unwrap_err();
    assert!(matches!(err, ServerError::ObjectNotFound { .. }));
}

#[test]
fn ec_corrupt_one_data_shard_recovery() {
    if !backend_supports_parity_recovery() {
        return;
    }
    // Corrupt shard 0 on disk. PgStore detects CRC mismatch, EC reconstructs.
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"EC corruption recovery test data";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "obj4", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    corrupt_shard_on_disk(&coord, "bucket", "obj4", 0);

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj4",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), data);
}

#[test]
fn ec_range_get_with_missing_shard() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"Hello, World! Range test with EC recovery";
    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "obj5", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("obj5");
    let generation_id = coord
        .storage_node
        .test_get_object_meta(&bucket, &key)
        .unwrap()
        .into_live()
        .expect("put object should create a live object")
        .generation_id;
    let segment = coord
        .storage_node
        .test_get_object_segments(&bucket, &key, put.version_id)
        .unwrap()
        .pop()
        .expect("put object should create one object segment");
    let expected_selected_nodes = coord
        .storage_node
        .segment_payload_shard_locations(
            segment.data_pg_id,
            storage::EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
            &segment.segment_okh,
            segment.segment_vid,
        )
        .unwrap()
        .into_iter()
        .map(|location| location.node_id())
        .collect::<BTreeSet<_>>()
        .len();

    // Delete shard 0 (covers the beginning of the data)
    delete_shard_on_disk(&coord, "bucket", "obj5", 0);

    // Range get should still succeed via EC reconstruction
    let result = coord
        .get_object_range(&GetObjectRangeRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj5",
                None,
                test_requester(),
                None,
            ),
            range: ByteRange::Range { start: 0, end: 4 },
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        coord
            .storage_node
            .object_payload_lease_holder_node_count(&bucket, &key, generation_id),
        expected_selected_nodes,
        "degraded EC range read should hold handles for the selected recovery shard-owner set"
    );
    assert_eq!(result.body.read_all().unwrap(), b"Hello");
    assert_eq!(
        coord
            .storage_node
            .object_payload_lease_holder_node_count(&bucket, &key, generation_id),
        0,
        "degraded EC range read should release shard-owner handles after body consumption"
    );
}

#[test]
fn ec_drop_parity_shard_data_still_works() {
    if !backend_supports_parity_recovery() {
        return;
    }
    // Delete parity shard (index k=4). Only data shards needed for normal read.
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"EC parity shard drop test";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "obj6", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // Delete first parity shard (index 4, since k=4)
    delete_shard_on_disk(&coord, "bucket", "obj6", 4);

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj6",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), data);
}

#[test]
fn ec_healthy_read_skips_corrupt_parity_shards() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"EC healthy read should skip parity shards";
    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "obj7", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let segment = {
        let segments = coord
            .storage_node
            .test_get_object_segments(
                &trusted_bucket_name("bucket"),
                &trusted_object_key("obj7"),
                put.version_id,
            )
            .unwrap();
        assert_eq!(segments.len(), 1);
        segments[0].clone()
    };

    corrupt_shard_on_disk(&coord, "bucket", "obj7", 4);

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj7",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), data);

    let parity_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), 4);
    assert!(
        coord
            .storage_node
            .test_shard_exists(segment.data_pg_id, &parity_key)
            .unwrap(),
        "healthy-path read should not touch parity shard 4"
    );
}

#[test]
fn ec_reconstruction_stops_after_first_needed_parity_shard() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"EC reconstruction should stop after first needed parity";
    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "obj8", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let segment = {
        let segments = coord
            .storage_node
            .test_get_object_segments(
                &trusted_bucket_name("bucket"),
                &trusted_object_key("obj8"),
                put.version_id,
            )
            .unwrap();
        assert_eq!(segments.len(), 1);
        segments[0].clone()
    };

    delete_shard_on_disk(&coord, "bucket", "obj8", 0);
    corrupt_shard_on_disk(&coord, "bucket", "obj8", 5);

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj8",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), data);

    let parity_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), 5);
    assert!(
        coord
            .storage_node
            .test_shard_exists(segment.data_pg_id, &parity_key)
            .unwrap(),
        "reconstruction should stop once enough shards are present"
    );
}

#[test]
fn put_to_nonexistent_bucket_fails() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "no-such-bucket",
                "key",
                test_requester(),
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
    assert!(matches!(err, ServerError::BucketNotFound { .. }));
}

#[test]
fn get_nonexistent_object_fails() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "no-such-key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::ObjectNotFound { .. }));
}

#[test]
fn etag_consistency() {
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
    assert_eq!(result.etag, obj.etag);

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
    assert_eq!(result.etag, head.etag);
}

#[test]
fn list_objects_delimiter_with_continuation() {
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
            object: object_request_with_expected_owner("bucket", "a/1", test_requester(), None),
            data: b"1",
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
            object: object_request_with_expected_owner("bucket", "a/2", test_requester(), None),
            data: b"2",
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
            object: object_request_with_expected_owner("bucket", "b/1", test_requester(), None),
            data: b"3",
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
            object: object_request_with_expected_owner("bucket", "c/1", test_requester(), None),
            data: b"4",
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
                "bucket",
                "root.txt",
                test_requester(),
                None,
            ),
            data: b"5",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // First page: max_keys=2 with delimiter
    let result = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            continuation_token: None,
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    assert_eq!(
        result.objects.len() + result.common_prefixes.len(),
        2,
        "should return exactly 2 entries (objects + prefixes)"
    );
    assert!(result.is_truncated);
    assert!(result.next_continuation_token.is_some());

    // Second page using continuation token
    let token = result.next_continuation_token.unwrap();
    let result2 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            continuation_token: Some(&token),
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    assert!(
        !result2.objects.is_empty() || !result2.common_prefixes.is_empty(),
        "continuation page should have entries"
    );
}

#[test]
fn list_objects_delimiter_continuation_skips_large_common_prefix() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    for i in 0..1500 {
        let key = format!("dir/file-{i:04}.txt");
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", &key, test_requester(), None),
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
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "z.txt", test_requester(), None),
            data: b"z",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let page1 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            continuation_token: None,
            max_keys: 1,
            requested_max_keys: Some(1),
        })
        .unwrap();
    assert!(page1.objects.is_empty());
    assert_eq!(page1.common_prefixes, vec!["dir/".to_string()]);
    assert!(page1.is_truncated);

    let token = page1
        .next_continuation_token
        .as_deref()
        .expect("first page should return a continuation token")
        .to_string();
    let page2 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            continuation_token: Some(&token),
            max_keys: 1,
            requested_max_keys: Some(1),
        })
        .unwrap();
    assert_eq!(page2.common_prefixes, Vec::<String>::new());
    assert_eq!(page2.objects.len(), 1);
    assert_eq!(page2.objects[0].key, "z.txt");
    assert!(!page2.is_truncated);
}

#[test]
fn list_objects_delimiter_with_no_upper_bound_common_prefix_is_final_page() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let delimiter = "\u{10ffff}";

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "a", test_requester(), None),
            data: b"a",
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
                "bucket",
                &format!("{delimiter}child"),
                test_requester(),
                None,
            ),
            data: b"b",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let page1 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some(delimiter),
            continuation_token: None,
            max_keys: 1,
            requested_max_keys: Some(1),
        })
        .unwrap();
    assert_eq!(page1.objects.len(), 1);
    assert_eq!(page1.objects[0].key, "a");
    assert!(page1.common_prefixes.is_empty());
    assert!(page1.is_truncated);

    let token = page1
        .next_continuation_token
        .as_deref()
        .expect("first page should return a continuation token")
        .to_string();
    let page2 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some(delimiter),
            continuation_token: Some(&token),
            max_keys: 1,
            requested_max_keys: Some(1),
        })
        .unwrap();
    assert!(page2.objects.is_empty());
    assert_eq!(page2.common_prefixes, vec![delimiter.to_string()]);
    assert!(!page2.is_truncated);
    assert!(page2.next_continuation_token.is_none());
}

#[test]
fn list_objects_delimiter_continuation_with_boundary_token_does_not_panic() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let delimiter = "\x7f";
    let token = format!("{}{}", "a".repeat(1023), delimiter);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "z", test_requester(), None),
            data: b"z",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let page2 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some(delimiter),
            continuation_token: Some(&token),
            max_keys: 1,
            requested_max_keys: Some(1),
        })
        .unwrap();
    assert_eq!(page2.common_prefixes, Vec::<String>::new());
    assert_eq!(page2.objects.len(), 1);
    assert_eq!(page2.objects[0].key, "z");
    assert!(!page2.is_truncated);
    assert!(page2.next_continuation_token.is_none());
}

#[test]
fn list_objects_delimiter_common_prefix_boundary_falls_back_without_error() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let delimiter = "\x7f";
    let common_prefix = format!("{}{}", "a".repeat(1023), delimiter);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                &common_prefix,
                test_requester(),
                None,
            ),
            data: b"prefix",
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
            object: object_request_with_expected_owner("bucket", "z", test_requester(), None),
            data: b"z",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let page1 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some(delimiter),
            continuation_token: None,
            max_keys: 1,
            requested_max_keys: Some(1),
        })
        .unwrap();
    assert!(page1.objects.is_empty());
    assert_eq!(page1.common_prefixes, vec![common_prefix.clone()]);
    assert!(page1.is_truncated);

    let token = page1
        .next_continuation_token
        .as_deref()
        .expect("first page should return a continuation token")
        .to_string();
    assert_eq!(token, common_prefix);

    let page2 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some(delimiter),
            continuation_token: Some(&token),
            max_keys: 1,
            requested_max_keys: Some(1),
        })
        .unwrap();
    assert!(page2.common_prefixes.is_empty());
    assert_eq!(page2.objects.len(), 1);
    assert_eq!(page2.objects[0].key, "z");
    assert!(!page2.is_truncated);
    assert!(page2.next_continuation_token.is_none());
}

#[test]
fn list_objects_max_keys_counts_prefixes() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    // Create many prefixed objects to ensure common_prefixes count toward max_keys
    for i in 0..10 {
        let key = format!("dir{i}/file.txt");
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", &key, test_requester(), None),
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

    let result = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            continuation_token: None,
            max_keys: 3,
            requested_max_keys: Some(3),
        })
        .unwrap();
    // With delimiter "/", all entries become common prefixes
    assert_eq!(result.common_prefixes.len(), 3);
    assert!(result.is_truncated);
}

#[test]
fn put_object_to_nonexistent_bucket_no_orphaned_shards() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    // Don't create bucket — put should fail at bucket check before writing shards
    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("no-bucket", "key", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::BucketNotFound { .. }));
}

#[test]
fn delete_nonexistent_bucket_fails() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = delete_bucket_test(&coord, "no-such-bucket").unwrap_err();
    assert!(matches!(err, ServerError::BucketNotFound { .. }));
}

#[test]
fn list_objects_no_delimiter_truncated() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    for i in 0..5 {
        let key = format!("key-{i:02}");
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", &key, test_requester(), None),
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

    // Request fewer than available
    let result = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 3,
            requested_max_keys: Some(3),
        })
        .unwrap();
    assert_eq!(result.objects.len(), 3);
    assert!(result.is_truncated);
    assert!(result.next_continuation_token.is_some());
}

#[test]
fn list_objects_no_delimiter_with_continuation() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    for i in 0..5 {
        let key = format!("key-{i:02}");
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", &key, test_requester(), None),
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

    // First page
    let page1 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    assert_eq!(page1.objects.len(), 2);
    assert!(page1.is_truncated);
    let token = page1.next_continuation_token.as_ref().unwrap();

    // Second page using continuation token
    let page2 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: Some(token),
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    assert_eq!(page2.objects.len(), 2);
    assert!(page2.is_truncated);
    let token2 = page2.next_continuation_token.as_ref().unwrap();

    // Third page — should get remainder
    let page3 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: Some(token2),
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    assert_eq!(page3.objects.len(), 1);
    assert!(!page3.is_truncated);
    assert!(page3.next_continuation_token.is_none());
}

#[test]
fn list_objects_prefix_with_delimiter() {
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
            object: object_request_with_expected_owner(
                "bucket",
                "photos/2024/jan.jpg",
                test_requester(),
                None,
            ),
            data: b"j",
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
                "bucket",
                "photos/2024/feb.jpg",
                test_requester(),
                None,
            ),
            data: b"f",
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
                "bucket",
                "photos/2025/mar.jpg",
                test_requester(),
                None,
            ),
            data: b"m",
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
                "bucket",
                "photos/top.jpg",
                test_requester(),
                None,
            ),
            data: b"t",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // List with prefix "photos/" and delimiter "/"
    let result = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: Some("photos/"),
            delimiter: Some("/"),
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();
    // top.jpg is a direct child, 2024/ and 2025/ are common prefixes
    assert_eq!(result.objects.len(), 1);
    assert_eq!(result.objects[0].key, "photos/top.jpg");
    assert_eq!(result.common_prefixes.len(), 2);
    assert!(result.common_prefixes.contains(&"photos/2024/".to_string()));
    assert!(result.common_prefixes.contains(&"photos/2025/".to_string()));
    assert!(!result.is_truncated);
}

#[test]
fn list_objects_not_truncated_no_token() {
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
            object: object_request_with_expected_owner(
                "bucket",
                "only-one",
                test_requester(),
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

    let result = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();
    assert_eq!(result.objects.len(), 1);
    assert!(!result.is_truncated);
    assert!(result.next_continuation_token.is_none());
}

#[test]
fn list_objects_max_keys_zero() {
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
            object: object_request_with_expected_owner("bucket", "key1", test_requester(), None),
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
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 0,
            requested_max_keys: Some(0),
        })
        .unwrap();
    assert!(result.objects.is_empty());
    assert!(result.common_prefixes.is_empty());
    assert!(!result.is_truncated);
    assert!(result.next_continuation_token.is_none());
}

#[test]
fn list_objects_max_keys_zero_with_delimiter() {
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
            object: object_request_with_expected_owner("bucket", "a/1", test_requester(), None),
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
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            continuation_token: None,
            max_keys: 0,
            requested_max_keys: Some(0),
        })
        .unwrap();
    assert!(result.objects.is_empty());
    assert!(result.common_prefixes.is_empty());
    assert!(!result.is_truncated);
}

#[test]
fn list_objects_nonexistent_bucket_fails() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("no-bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::BucketNotFound { .. }));
}

#[test]
fn list_objects_nonexistent_bucket_for_non_owner_still_returns_bucket_not_found() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(
                "no-bucket",
                test_helpers::requester("other-user"),
                None,
            ),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::BucketNotFound { .. }));
}

#[test]
fn delete_objects_batch() {
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

    let entries = vec![
        DeleteEntry {
            key: trusted_object_key("key1"),
            version_id: None,
            cond: DeleteCondition::None,
        },
        DeleteEntry {
            key: trusted_object_key("key2"),
            version_id: None,
            cond: DeleteCondition::None,
        },
        // key3 doesn't exist — should still succeed (idempotent)
        DeleteEntry {
            key: trusted_object_key("key3"),
            version_id: None,
            cond: DeleteCondition::None,
        },
    ];

    let result = coord
        .delete_objects(&DeleteObjectsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            entries: &entries,
            bypass_governance: false,
        })
        .unwrap();
    assert_eq!(result.deleted.len(), 3);
    assert!(result.errors.is_empty());

    // Verify objects are actually gone
    assert!(coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key1",
                None,
                test_requester(),
                None
            ),
            cond: NO_READ,
        })
        .is_err());
    assert!(coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key2",
                None,
                test_requester(),
                None
            ),
            cond: NO_READ,
        })
        .is_err());
}

#[test]
fn delete_objects_nonexistent_bucket() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let entries = vec![DeleteEntry {
        key: trusted_object_key("key1"),
        version_id: None,
        cond: DeleteCondition::None,
    }];

    let err = coord
        .delete_objects(&DeleteObjectsRequest {
            bucket: bucket_request_with_expected_owner("no-bucket", test_requester(), None),
            entries: &entries,
            bypass_governance: false,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::BucketNotFound { .. }));
}

#[test]
fn max_object_size_constant() {
    // Verify the constant matches AWS S3 single PUT limit (5 GiB).
    assert_eq!(MAX_OBJECT_SIZE, 5 * 1024 * 1024 * 1024);
}

#[test]
fn max_parts_constant() {
    assert_eq!(MAX_PARTS, 10_000);
}

#[test]
fn complete_multipart_too_many_parts() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
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

    // Build a part list with MAX_PARTS + 1 entries.
    let parts: Vec<_> = (1..=MAX_PARTS as u32 + 1)
        .map(|n| CompletePart {
            part_number: n,
            etag: "dummy".to_string(),
            checksum: None,
        })
        .collect();

    let err = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            parts: &parts,
            claimed_checksum: None,

            expected_object_size: None,

            cond: &WriteCondition::default(),

            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::InvalidRequest { .. }));
}

// ── shard planning unit tests ──────────────────────────────────────

#[test]
fn compute_shard_size_exact_multiple() {
    // 100 bytes, k=4 → no padding needed → 25 per shard
    assert_eq!(compute_shard_size(100, 4), 25);
}

#[test]
fn compute_shard_size_needs_padding() {
    // 101 bytes, k=4 → pad to 104 → 26 per shard
    assert_eq!(compute_shard_size(101, 4), 26);
}

#[test]
fn compute_shard_size_small() {
    // 1 byte, k=4 → pad to 4 → 1 per shard
    assert_eq!(compute_shard_size(1, 4), 1);
}

#[test]
fn compute_shard_size_zero() {
    // 0 bytes, k=4 → 0 per shard
    assert_eq!(compute_shard_size(0, 4), 0);
}

#[test]
fn shards_for_byte_range_single_shard() {
    // shard_size=25, range [0,24] → shard 0
    assert_eq!(shards_for_byte_range(0, 24, 25, 4), vec![0]);
}

#[test]
fn shards_for_byte_range_spans_two() {
    // shard_size=25, range [20,30] → shards 0,1
    assert_eq!(shards_for_byte_range(20, 30, 25, 4), vec![0, 1]);
}

#[test]
fn shards_for_byte_range_all_shards() {
    // shard_size=25, range [0,99] → shards 0,1,2,3
    assert_eq!(shards_for_byte_range(0, 99, 25, 4), vec![0, 1, 2, 3]);
}

#[test]
fn shards_for_byte_range_last_shard_only() {
    // shard_size=25, range [75,99] → shard 3
    assert_eq!(shards_for_byte_range(75, 99, 25, 4), vec![3]);
}

#[test]
fn shards_for_byte_range_clamped_to_k() {
    // end falls past last shard → clamp to k-1
    assert_eq!(shards_for_byte_range(75, 200, 25, 4), vec![3]);
}

#[test]
fn shards_for_byte_range_zero_shard_size() {
    let empty: Vec<usize> = vec![];
    assert_eq!(shards_for_byte_range(0, 10, 0, 4), empty);
}

// ── range GET tests ────────────────────────────────────────────────

#[test]
fn get_object_range_basic() {
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
            data: b"Hello, World!",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // bytes=0-4 → "Hello"
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
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), b"Hello");
    assert_eq!(result.range_start, 0);
    assert_eq!(result.range_end, 4);
    assert_eq!(result.size, 13);
}

#[test]
fn get_object_range_holds_payload_lease_on_selected_shard_nodes() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster_with_ec_shape(
        tmp.path(),
        &[0, 1, 2, 3],
        storage::EcShape { k: 2, m: 1 },
    );
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let data = b"read handles should only pin selected shard owners";
    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let generation_id = storage_cluster
        .test_get_object_meta(&bucket, &key)
        .unwrap()
        .into_live()
        .expect("put object should create a live object")
        .generation_id;
    let segment = storage_cluster
        .test_get_object_segments(&bucket, &key, put.version_id)
        .unwrap()
        .pop()
        .expect("direct put should create one object segment");
    let expected_selected_nodes = storage_cluster
        .segment_payload_shard_locations(
            segment.data_pg_id,
            storage::EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
            &segment.segment_okh,
            segment.segment_vid,
        )
        .unwrap()
        .into_iter()
        .map(|location| location.node_id())
        .collect::<BTreeSet<_>>()
        .len();

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
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        storage_cluster.object_payload_lease_holder_node_count(&bucket, &key, generation_id),
        expected_selected_nodes,
        "range read should hold payload leases only on selected shard-owner nodes"
    );
    assert_eq!(result.body.read_all().unwrap(), b"read ");
    assert_eq!(
        storage_cluster.object_payload_lease_holder_node_count(&bucket, &key, generation_id),
        0,
        "read handle drop should release selected shard-owner payload leases"
    );
}

#[test]
fn get_object_range_suffix() {
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
            data: b"Hello, World!",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // bytes=-6 → "World!"  (last 6 bytes)
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
            range: ByteRange::Suffix { length: 6 },
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), b"World!");
    assert_eq!(result.range_start, 7);
    assert_eq!(result.range_end, 12);
}

#[test]
fn get_object_range_from_start() {
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
            data: b"Hello, World!",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // bytes=7- → "World!"
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
            range: ByteRange::FromStart { start: 7 },
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), b"World!");
}

#[test]
fn get_object_range_unsatisfiable() {
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
            data: b"Hello",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // bytes=100- → unsatisfiable
    let err = coord
        .get_object_range(&GetObjectRangeRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            range: ByteRange::FromStart { start: 100 },
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::InvalidRange { total_size: 5 }));
}

#[test]
fn get_object_range_clamps_end() {
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
            data: b"Hello",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // bytes=0-99999 on 5-byte object → clamp to 0-4
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
            range: ByteRange::Range {
                start: 0,
                end: 99999,
            },
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), b"Hello");
    assert_eq!(result.range_start, 0);
    assert_eq!(result.range_end, 4);
}

// ── Conditional request integration tests ────────────────────────

#[test]
fn put_if_none_match_star_creates() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let cond = WriteCondition::IfNoneMatchStar;
    let result = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "new-key", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: &cond,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    assert!(!result.etag.is_empty());
}

#[test]
fn put_if_none_match_star_rejects_overwrite() {
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
            data: b"v1",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let cond = WriteCondition::IfNoneMatchStar;
    let err = test_helpers::put_object(
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
            cond: &cond,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::PreconditionFailed));
}

#[test]
fn put_if_match_updates() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let r1 = test_helpers::put_object(
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
    let cond = WriteCondition::IfMatch(SpecificEtag::new(r1.etag.clone()).unwrap());
    let r2 = test_helpers::put_object(
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
            cond: &cond,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    assert_ne!(r1.etag, r2.etag);

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
    assert_eq!(obj.body.read_all().unwrap(), b"v2");
}

#[test]
fn put_if_match_stale_etag_rejected() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let r1 = test_helpers::put_object(
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
    // Overwrite so etag changes
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

    let cond = WriteCondition::IfMatch(SpecificEtag::new(r1.etag).unwrap());
    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"v3",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: &cond,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::PreconditionFailed));
}
