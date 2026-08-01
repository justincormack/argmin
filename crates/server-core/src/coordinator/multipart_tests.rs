use super::test_helpers::{self, UploadPartRequest};
use super::test_support::*;
use super::*;
use crate::conditional::{DeleteCondition, ReadCondition, SpecificEtag, WriteCondition};
use crate::sse::SSE_C_CUSTOMER_KEY_LEN;
use std::sync::Arc;
use storage::{EcShape, TestObjectSegmentsReclaimRecord, TestObjectSegmentsReclaimSegmentRecord};

fn create_bucket_with_explicit_writer_grant(
    coord: &Coordinator,
    name: &str,
    owner_requester: Requester,
    writer: &AccountIdentity,
) -> Result<(), ServerError> {
    coord.create_bucket(&CreateBucketRequest {
        name: trusted_bucket_name(name),
        requester: owner_requester,
        namespace: BucketNamespace::Global,
        acl: CreateBucketAcl::Grants(AclGrants::new(vec![AclGrant::new(
            AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
            AclPermission::Write,
        )])),
        ownership: BucketObjectOwnership::ObjectWriter,
        object_lock_enabled: false,
    })
}

// ── Multipart upload tests ────────────────────────────────────────

fn upload_part_stream_session_count(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    upload_id: &UploadId,
    part_number: u32,
) -> usize {
    coord
        .storage_node()
        .test_list_all_stream_uploads()
        .unwrap()
        .into_iter()
        .filter(|session| {
            session.bucket == bucket
                && session.key == key
                && matches!(
                    &session.target,
                    StreamUploadTarget::UploadPart {
                        upload_id: target_upload_id,
                        part_number: target_part_number,
                    } if target_upload_id == upload_id && *target_part_number == part_number
                )
        })
        .count()
}

fn assert_payload_shard_files_state(
    coord: &Coordinator,
    data_pg_id: u32,
    ec: EcShape,
    okh: &[u8; 16],
    generation_id: GenerationId,
    expected: bool,
    context: &str,
) {
    for shard_index in 0..(ec.k + ec.m) {
        let exists = coord
            .storage_node()
            .test_payload_shard_file_exists(data_pg_id, ec, okh, generation_id, shard_index)
            .unwrap();
        assert_eq!(
            exists, expected,
            "{context}: placed shard file {shard_index} state mismatch"
        );
    }
}

#[test]
fn create_multipart_upload_returns_upload_id() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let result = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    assert_eq!(result.upload_id.as_str().len(), UPLOAD_ID_LEN);
    assert!(result
        .upload_id
        .as_str()
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_'));
}

#[test]
fn create_multipart_upload_unique_ids() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let r1 = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    let r2 = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    assert_ne!(r1.upload_id, r2.upload_id);
}

#[test]
fn create_multipart_upload_requires_bucket() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let metadata = MetadataBlob::new();
    let err = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "no-such-bucket",
                "key",
                test_requester(),
                None,
            ),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::BucketNotFound { .. }));
}

#[test]
fn list_multipart_uploads_empty() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let result = coord
        .list_multipart_uploads(&ListMultipartUploadsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 1000,
        })
        .unwrap();
    assert!(result.uploads.is_empty());
    assert!(!result.is_truncated);
}

#[test]
fn list_multipart_uploads_returns_created() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let r1 = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "alpha", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    let r2 = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "beta", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let result = coord
        .list_multipart_uploads(&ListMultipartUploadsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 1000,
        })
        .unwrap();
    assert_eq!(result.uploads.len(), 2);

    // Should be sorted by key ascending.
    assert_eq!(result.uploads[0].key, "alpha");
    assert_eq!(result.uploads[0].upload_id, r1.upload_id);
    assert_eq!(result.uploads[1].key, "beta");
    assert_eq!(result.uploads[1].upload_id, r2.upload_id);
    assert!(!result.is_truncated);
    assert_eq!(
        result.next_marker,
        Some(ListMultipartUploadsNextMarker::Upload {
            key: "beta".to_string(),
            upload_id: r2.upload_id,
        })
    );
}

#[test]
fn list_multipart_uploads_reports_stored_owner_and_initiator() {
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

    create_bucket_with_explicit_writer_grant(
        &coord,
        "bucket",
        Requester::authenticated(bucket_owner.clone()),
        &writer,
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

    let result = coord
        .list_multipart_uploads(&ListMultipartUploadsRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                Requester::authenticated(bucket_owner.clone()),
                None,
            ),
            prefix: None,
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 1000,
        })
        .unwrap();
    assert_eq!(result.uploads.len(), 1);
    assert_eq!(result.uploads[0].upload_id, upload.upload_id);
    assert_eq!(
        result.uploads[0].owner,
        OwnerIdentity::new(
            bucket_owner.principal().to_string(),
            bucket_owner.canonical_user_id().clone(),
        )
    );
    assert_eq!(
        result.uploads[0].initiator,
        OwnerIdentity::new(
            writer.principal().to_string(),
            writer.canonical_user_id().clone(),
        )
    );
}

#[test]
fn multipart_operations_reject_non_initiator_with_explicit_bucket_write_grant() {
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
    let other = AccountIdentity::new(
        "other-a",
        CanonicalUserId::from_principal("other-canonical"),
        "Other",
    );
    let owner_requester = Requester::authenticated(bucket_owner.clone());
    let writer_requester = Requester::authenticated(writer.clone());
    let other_requester = Requester::authenticated(other);

    create_bucket_with_explicit_writer_grant(&coord, "bucket", owner_requester.clone(), &writer)
        .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                writer_requester.clone(),
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
                other_requester.clone(),
                None,
            ),
            part_number: 1,
            data: b"not-allowed",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let uploaded = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload.upload_id,
                writer_requester.clone(),
                None,
            ),
            part_number: 1,
            data: b"allowed",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap();

    let err = coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload.upload_id,
                other_requester.clone(),
                None,
            ),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));

    let parts = coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload.upload_id,
                writer_requester.clone(),
                None,
            ),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap();
    assert_eq!(parts.parts.len(), 1);
    assert_eq!(parts.parts[0].etag, uploaded.etag);

    let err = coord
        .abort_multipart_upload(&multipart_object_request_with_expected_owner(
            "bucket",
            "key",
            &upload.upload_id,
            other_requester.clone(),
            None,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn create_multipart_upload_rejects_anonymous_on_public_write_bucket() {
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

    let err = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                Requester::anonymous(),
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
fn create_multipart_upload_rejects_cross_account_overwrite_on_public_write_bucket() {
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
    let owner_requester = Requester::authenticated(bucket_owner.clone());
    let writer_requester = Requester::authenticated(writer.clone());

    create_bucket_for_owner_with_flags(
        &coord,
        bucket_owner.principal(),
        bucket_owner.canonical_user_id(),
        "bucket",
        false,
        true,
        false,
    )
    .unwrap();
    put_bucket_ownership_controls_test(
        &coord,
        "bucket",
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule></OwnershipControls>",
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
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                owner_requester.clone(),
                None,
            ),
            data: b"owner-data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let err = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", writer_requester, None),
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
fn complete_multipart_upload_allows_owner_object_created_after_public_write_initiation() {
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
    let owner_requester = Requester::authenticated(bucket_owner.clone());
    let writer_requester = Requester::authenticated(writer.clone());

    create_bucket_for_owner_with_flags(
        &coord,
        bucket_owner.principal(),
        bucket_owner.canonical_user_id(),
        "bucket",
        false,
        true,
        false,
    )
    .unwrap();
    put_bucket_ownership_controls_test(
        &coord,
        "bucket",
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule></OwnershipControls>",
        owner_requester.clone(),
        None,
    )
    .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                writer_requester.clone(),
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
            data: b"owner-data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let uploaded = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload.upload_id,
                writer_requester.clone(),
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
                writer_requester.clone(),
                None,
            ),
            parts: &[CompletePart {
                part_number: 1,
                etag: uploaded.etag.clone(),
                checksum: None,
            }],
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap();

    let object = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                writer_requester.clone(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(object.body.read_all().unwrap(), b"multipart-data");
}

#[test]
fn multipart_initiator_cannot_continue_after_explicit_bucket_write_grant_becomes_private() {
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
    let owner_requester = Requester::authenticated(bucket_owner.clone());
    let writer_requester = Requester::authenticated(writer.clone());

    create_bucket_with_explicit_writer_grant(&coord, "bucket", owner_requester.clone(), &writer)
        .unwrap();
    put_bucket_ownership_controls_test(&coord,
            "bucket",
            "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule></OwnershipControls>",
            owner_requester.clone(), None)
        .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                writer_requester.clone(),
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

    put_bucket_canned_acl_test(
        &coord,
        "bucket",
        BucketAcl::Private,
        owner_requester.clone(),
        None,
    )
    .unwrap();

    let completed = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload.upload_id,
                writer_requester.clone(),
                None,
            ),
            parts: &[CompletePart {
                part_number: 1,
                etag: "\"etag\"".to_string(),
                checksum: None,
            }],
            claimed_checksum: None,

            expected_object_size: None,

            cond: &WriteCondition::default(),

            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(completed, ServerError::AccessDenied));

    let upload_part = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload.upload_id,
                writer_requester.clone(),
                None,
            ),
            part_number: 1,
            data: b"denied-after-acl-change",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap_err();
    assert!(matches!(upload_part, ServerError::AccessDenied));

    let list_parts = coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload.upload_id,
                writer_requester.clone(),
                None,
            ),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap();
    assert!(list_parts.parts.is_empty());

    coord
        .abort_multipart_upload(&multipart_object_request_with_expected_owner(
            "bucket",
            "key",
            &upload.upload_id,
            writer_requester.clone(),
            None,
        ))
        .unwrap();
}

#[test]
fn list_multipart_uploads_sorted_by_key_then_initiated() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    // Create two uploads for the same key.
    let r1 = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    let r2 = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let result = coord
        .list_multipart_uploads(&ListMultipartUploadsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 1000,
        })
        .unwrap();
    assert_eq!(result.uploads.len(), 2);

    // Both same key — sorted by initiation time (ascending).
    assert!(result.uploads[0].initiated <= result.uploads[1].initiated);
    // Both upload IDs present.
    let ids: Vec<&str> = result
        .uploads
        .iter()
        .map(|u| u.upload_id.as_str())
        .collect();
    assert!(ids.contains(&r1.upload_id.as_str()));
    assert!(ids.contains(&r2.upload_id.as_str()));
}

#[test]
fn list_multipart_uploads_pagination() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    // Create 3 uploads for distinct keys so ordering is deterministic.
    coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "a", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "b", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "c", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    // Page 1: max_uploads=2.
    let page1 = coord
        .list_multipart_uploads(&ListMultipartUploadsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 2,
        })
        .unwrap();
    assert_eq!(page1.uploads.len(), 2);
    assert!(page1.is_truncated);
    assert_eq!(page1.uploads[0].key, "a");
    assert_eq!(page1.uploads[1].key, "b");
    let Some(ListMultipartUploadsNextMarker::Upload {
        key: page1_key_marker,
        upload_id: page1_upload_id_marker,
    }) = page1.next_marker.as_ref()
    else {
        panic!("truncated upload page must end at an upload marker");
    };

    // Page 2: use markers from page 1.
    let page2 = coord
        .list_multipart_uploads(&ListMultipartUploadsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: Some(page1_key_marker),
            upload_id_marker: Some(page1_upload_id_marker.clone()),
            max_uploads: 2,
        })
        .unwrap();
    assert_eq!(page2.uploads.len(), 1);
    assert!(!page2.is_truncated);
    assert_eq!(page2.uploads[0].key, "c");
    assert_eq!(
        page2.next_marker,
        Some(ListMultipartUploadsNextMarker::Upload {
            key: "c".to_string(),
            upload_id: page2.uploads[0].upload_id.clone(),
        })
    );
}

#[test]
fn list_multipart_uploads_prefix_filter() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "photos/a.jpg",
                test_requester(),
                None,
            ),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "photos/b.jpg",
                test_requester(),
                None,
            ),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "docs/readme.md",
                test_requester(),
                None,
            ),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let result = coord
        .list_multipart_uploads(&ListMultipartUploadsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: Some("photos/"),
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 1000,
        })
        .unwrap();
    assert_eq!(result.uploads.len(), 2);
    assert!(result.uploads.iter().all(|u| u.key.starts_with("photos/")));
}

#[test]
fn list_multipart_uploads_max_zero() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let result = coord
        .list_multipart_uploads(&ListMultipartUploadsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 0,
        })
        .unwrap();
    assert!(result.uploads.is_empty());
    assert!(!result.is_truncated);
}

#[test]
fn list_multipart_uploads_requires_bucket() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = coord
        .list_multipart_uploads(&ListMultipartUploadsRequest {
            bucket: bucket_request_with_expected_owner("no-such-bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 1000,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::BucketNotFound { .. }));
}

#[test]
fn authorize_list_multipart_uploads_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let err = coord
        .authorize_list_multipart_uploads(&ListMultipartUploadsRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("other-user"),
                None,
            ),
            prefix: None,
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 1000,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn list_multipart_uploads_bucket_policy_allow_applies() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let metadata = MetadataBlob::default();
    let system_metadata = SystemMetadata::default();
    coord
        .create_bucket_for_owner("111122223333", "bucket", false)
        .unwrap();
    coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request("bucket", "key", test_helpers::requester("111122223333")),
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            checksum: None,
            acl: PutObjectWriteAcl::None,
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            encryption: WriteEncryptionRequest::none(),
        })
        .unwrap();
    put_bucket_policy_test(
        &coord,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:ListBucketMultipartUploads","Resource":"arn:aws:s3:::bucket"}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    let result = coord
        .list_multipart_uploads(&ListMultipartUploadsRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_helpers::requester("444455556666"),
                None,
            ),
            prefix: None,
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 1000,
        })
        .unwrap();
    assert_eq!(result.uploads.len(), 1);
    assert_eq!(result.uploads[0].key.as_str(), "key");
}

#[test]
fn create_multipart_upload_preserves_metadata() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let headers = [("Content-Type", "image/png"), ("X-Amz-Meta-Author", "test")];
    let metadata = MetadataBlob::from_headers(&headers).unwrap();
    let system_metadata = SystemMetadata::from_headers(&headers).unwrap();
    let tags_xml =
        "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
    let tags = object_tag_set(tags_xml);

    let result = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "photo.png",
                test_requester(),
                None,
            ),
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: Some(&tags),
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    // Verify we can retrieve the upload and its metadata blob is stored.
    let record = coord
        .storage_node()
        .test_get_multipart_upload(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("photo.png"),
            &result.upload_id,
        )
        .unwrap();
    assert_eq!(record.bucket, "bucket");
    assert_eq!(record.key, "photo.png");
    assert_eq!(record.tags.as_deref(), Some(&tags));

    // Deserialize and verify the metadata blob.
    let blob = MetadataBlob::deserialize(record.metadata_blob.as_slice()).unwrap();
    assert_eq!(blob.get("x-amz-meta-author"), Some("test"));
    let stored_system =
        SystemMetadata::deserialize(record.system_metadata_blob.as_slice()).unwrap();
    assert_eq!(
        stored_system.content_type().map(|v| v.as_str()),
        Some("image/png")
    );
}

#[test]
fn delete_bucket_blocked_by_multipart_uploads() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    // Bucket has no objects but has an in-progress MPU — should fail.
    let err = delete_bucket_test(&coord, "bucket").unwrap_err();
    assert!(matches!(err, ServerError::BucketNotEmpty));
}

#[test]
fn delete_bucket_drains_unqueued_payload_reclaim() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let generation_id = GenerationId::new(1).unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("ghost");
    let data_pg_id = coord
        .storage_node()
        .test_data_pg_id_for(&bucket, &key, generation_id);
    coord
        .storage_node()
        .test_put_object_segments_reclaim(
            &bucket,
            &key,
            &TestObjectSegmentsReclaimRecord {
                bucket: bucket.clone(),
                key: key.clone(),
                generation_id,
                created_at: 1,
                segments: vec![TestObjectSegmentsReclaimSegmentRecord {
                    segment_index: 0,
                    segment_okh: object_key_hash("bucket", "ghost"),
                    segment_vid: generation_id,
                    data_pg_id,
                    ec: EcShape { k: 4, m: 2 },
                }],
            },
        )
        .unwrap();

    delete_bucket_eventually_test(&coord, "bucket").unwrap();
    assert!(matches!(
        coord.unchecked_active_bucket_summary("bucket"),
        Err(ServerError::BucketNotFound { .. })
    ));

    wait_until_bucket_gone(&coord, "bucket");

    assert!(coord
        .storage_node()
        .test_get_object_segments_reclaim(&bucket, &key, generation_id)
        .unwrap()
        .is_none());
}

#[test]
fn delete_bucket_returns_before_payload_lease_and_reclaim_complete() {
    let tmp = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let deleter = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    admin
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"hello world",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let held_read = admin
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

    let generation_id = {
        match admin
            .storage_node()
            .test_get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
            .unwrap()
        {
            StoredObject::Live(record) => record.generation_id,
            other @ StoredObject::DeleteMarker(_) => {
                panic!("expected live object, got {other:?}")
            }
        }
    };

    admin
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap();

    {
        assert!(admin
            .storage_node()
            .test_get_object_segments_reclaim(
                &trusted_bucket_name("bucket"),
                &trusted_object_key("key"),
                generation_id,
            )
            .unwrap()
            .is_some());
    }

    delete_bucket_test(&deleter, "bucket").unwrap();
    assert!(matches!(
        deleter.unchecked_active_bucket_summary("bucket"),
        Err(ServerError::BucketNotFound { .. })
    ));
    assert!(matches!(
        deleter.create_bucket_for_owner("default-owner", "bucket", false),
        Err(ServerError::BucketAlreadyExists)
    ));

    {
        assert!(admin
            .storage_node()
            .test_get_object_segments_reclaim(
                &trusted_bucket_name("bucket"),
                &trusted_object_key("key"),
                generation_id,
            )
            .unwrap()
            .is_some());
    }

    drop(held_read);
    wait_until_bucket_gone(&deleter, "bucket");
}

#[test]
fn create_bucket_reuse_drains_unleased_multipart_reclaim_without_worker() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_reclaim_sweeper(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"first")]);
    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request("bucket", "key", &upload_id, test_requester()),
            parts: &parts,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
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
    delete_bucket_eventually_test(&coord, "bucket").unwrap();

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let err = coord
        .abort_multipart_upload(&multipart_object_request(
            "bucket",
            "key",
            &upload_id,
            test_requester(),
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::NoSuchUpload { .. }));
}

#[test]
fn no_such_upload_from_storage() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    // Directly call get_multipart_upload on a PG with a bogus upload ID.
    let err = coord
        .storage_node()
        .test_get_multipart_upload(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &trusted_upload_id("nonexistent"),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        storage::ObjectPgActionError::Metadata(storage::MetadataError::NoSuchUpload { .. })
    ));
    let err = ServerError::NoSuchUpload {
        upload_id: "nonexistent".to_string(),
    };
    assert_eq!(err.s3_error_code(), "NoSuchUpload");
    assert_eq!(err.http_status(), 404);
}

#[test]
fn list_multipart_uploads_same_key_pagination() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    // Create 3 uploads for the same key.
    let mut upload_ids = Vec::new();
    for _ in 0..3 {
        let r = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                metadata: &metadata,
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                checksum: None,

                acl: NO_PUT_OBJECT_ACL.into(),
                encryption: WriteEncryptionRequest::none(),
                object_lock: ObjectLockState::default(),
                policy_context: PutObjectPolicyContext::default(),
            })
            .unwrap();
        upload_ids.push(r.upload_id);
    }

    // Page 1: max_uploads=2 — should get first 2 by initiation time.
    let page1 = coord
        .list_multipart_uploads(&ListMultipartUploadsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 2,
        })
        .unwrap();
    assert_eq!(page1.uploads.len(), 2);
    assert!(page1.is_truncated);
    assert_eq!(page1.uploads[0].key, "key");
    assert_eq!(page1.uploads[1].key, "key");
    // Initiation time ordering.
    assert!(page1.uploads[0].initiated <= page1.uploads[1].initiated);
    let Some(ListMultipartUploadsNextMarker::Upload {
        key: page1_key_marker,
        upload_id: page1_upload_id_marker,
    }) = page1.next_marker.as_ref()
    else {
        panic!("truncated upload page must end at an upload marker");
    };

    // Page 2: use markers from page 1 — should get remaining upload.
    let page2 = coord
        .list_multipart_uploads(&ListMultipartUploadsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: Some(page1_key_marker),
            upload_id_marker: Some(page1_upload_id_marker.clone()),
            max_uploads: 2,
        })
        .unwrap();
    assert_eq!(page2.uploads.len(), 1);
    assert!(!page2.is_truncated);
    assert_eq!(page2.uploads[0].key, "key");
    assert_eq!(
        page2.next_marker,
        Some(ListMultipartUploadsNextMarker::Upload {
            key: "key".to_string(),
            upload_id: page2.uploads[0].upload_id.clone(),
        })
    );

    // All 3 upload IDs should be covered across both pages.
    let mut seen: Vec<UploadId> = page1
        .uploads
        .iter()
        .chain(page2.uploads.iter())
        .map(|u| u.upload_id.clone())
        .collect();
    seen.sort();
    let mut expected = upload_ids.clone();
    expected.sort();
    assert_eq!(seen, expected);
}

// ── UploadPart tests ──────────────────────────────────────────────
#[test]
fn upload_part_first_upload() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let result = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            data: b"hello world",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap();

    // ETag should be a quoted hex CRC64.
    assert!(result.etag.starts_with('"'));
    assert!(result.etag.ends_with('"'));

    // Verify part metadata was recorded.
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let upload = coord
        .storage_node()
        .test_get_multipart_upload(&bucket, &key, &create.upload_id)
        .unwrap();
    let part = coord
        .storage_node()
        .test_get_multipart_part(&bucket, &key, &create.upload_id, 1)
        .unwrap();
    assert_eq!(part.part_number, 1);
    assert_eq!(part.generation, 0);
    assert_eq!(part.size, 11); // "hello world".len()
    assert_eq!(part.part_vid, GenerationId::MIN);

    let segments = coord
        .storage_node()
        .test_get_all_multipart_part_segments_for_upload(&bucket, &key, &create.upload_id)
        .unwrap();
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].part_number, 1);
    assert_eq!(segments[0].segment_index, 0);
    assert_eq!(segments[0].size, 11);
    let topology = storage::PgTopology::new(coord.storage_node().test_pg_ids()).unwrap();
    assert_eq!(
        segments[0].data_pg_id,
        topology
            .object_generation_multipart_part_segment_data_pg(
                &bucket,
                &key,
                upload.object_generation_id,
                1,
                0,
            )
            .get()
    );
}

#[test]
fn upload_part_reupload_increments_generation() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    // First upload → generation 0.
    test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            data: b"first",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap();

    // Re-upload same part number → generation 1.
    let result = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            data: b"second",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap();

    let part = coord
        .storage_node()
        .test_get_multipart_part(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &create.upload_id,
            1,
        )
        .unwrap();
    assert_eq!(part.generation, 1);
    assert_eq!(part.size, 6); // "second".len()

    // ETag should reflect the new data.
    let expected_crc = checksum::crc64::checksum(b"second");
    assert_eq!(result.etag, format_etag(expected_crc));
}

#[test]
fn upload_part_invalid_part_number_zero() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
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
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number: 0,
            data: b"data",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::InvalidArgument { .. }));
}

#[test]
fn upload_part_invalid_part_number_exceeds_max() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
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
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number: 10_001,
            data: b"data",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::InvalidArgument { .. }));
}

#[test]
fn upload_part_nonexistent_upload() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let err = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                "bogus-upload-id",
                test_requester(),
                None,
            ),
            part_number: 1,
            data: b"data",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::NoSuchUpload { .. }));
}

#[test]
fn upload_part_multiple_parts() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            data: b"part-one",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap();
    test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number: 2,
            data: b"part-two",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap();
    test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number: 3,
            data: b"part-three",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap();

    // Verify all three parts exist.
    let parts_resp = coord
        .storage_node()
        .test_list_multipart_parts(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &storage::ListPartsReq {
                upload_id: UploadId::try_from(create.upload_id.as_str())
                    .expect("generated multipart upload ID is valid"),
                part_number_marker: None,
                max_parts: 100,
            },
        )
        .unwrap();
    assert_eq!(parts_resp.parts.len(), 3);
    assert_eq!(parts_resp.parts[0].part_number, 1);
    assert_eq!(parts_resp.parts[1].part_number, 2);
    assert_eq!(parts_resp.parts[2].part_number, 3);
}

#[test]
fn upload_part_repeated_reupload_generations() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    // Upload same part 4 times — generation should increment each time.
    for i in 0..4u32 {
        let data = format!("version-{i}");
        test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                upload: multipart_object_request_with_expected_owner(
                    "bucket",
                    "key",
                    &create.upload_id,
                    test_requester(),
                    None,
                ),
                part_number: 1,
                data: data.as_bytes(),
                claimed_checksum: None,

                sse_customer: None,
            },
        )
        .unwrap();
    }

    let part = coord
        .storage_node()
        .test_get_multipart_part(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &create.upload_id,
            1,
        )
        .unwrap();
    assert_eq!(part.generation, 3);
    assert_eq!(part.size, "version-3".len() as u64);
}

#[test]
fn upload_part_boundary_part_numbers() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    // Part 1 (min valid).
    test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            data: b"a",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap();
    // Part 10000 (max valid).
    test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number: 10_000,
            data: b"z",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap();

    coord
        .storage_node()
        .test_get_multipart_part(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &create.upload_id,
            1,
        )
        .unwrap();
    coord
        .storage_node()
        .test_get_multipart_part(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &create.upload_id,
            10_000,
        )
        .unwrap();
}

#[test]
fn upload_part_wrong_bucket_key_rejected() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    // Try uploading with wrong key — should be rejected even if upload_id is valid.
    let err = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "wrong-key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            data: b"data",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::NoSuchUpload { .. }));

    // Try uploading with wrong bucket.
    coord
        .create_bucket_for_owner("default-owner", "other-bucket", false)
        .unwrap();
    let err = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "other-bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            data: b"data",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::NoSuchUpload { .. }));
}

#[test]
fn upload_part_same_part_last_writer_wins() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    // Simulate concurrent same-part uploads sequentially.
    // Each successive upload should overwrite, with generation incrementing.
    let etag1 = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            data: b"writer-A",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap()
    .etag;
    let etag2 = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            data: b"writer-B",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap()
    .etag;
    let etag3 = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            data: b"writer-C",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap()
    .etag;

    // Each write has different data → different ETags.
    assert_ne!(etag1, etag2);
    assert_ne!(etag2, etag3);

    // Final state should reflect the last writer.
    let part = coord
        .storage_node()
        .test_get_multipart_part(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &create.upload_id,
            1,
        )
        .unwrap();
    assert_eq!(part.generation, 2); // 0, 1, 2
    assert_eq!(part.size, "writer-C".len() as u64);
    assert_eq!(format_etag(checksum::crc64::checksum(b"writer-C")), etag3);
}

// --- CompleteMultipartUpload tests ---

#[test]
fn complete_multipart_upload_happy_path() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    // Use 5MiB+ parts for non-final parts.
    let big_part = vec![0xABu8; 5 * 1024 * 1024];
    let small_last = b"final-part";

    let (upload_id, parts) =
        create_upload_with_parts(&coord, "bucket", "key", &[(1, &big_part), (2, small_last)]);
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let upload = coord
        .storage_node()
        .test_get_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();

    let result = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap();

    // ETag should be composite format: "hex-2"
    assert!(result.etag.ends_with("-2\""), "etag = {}", result.etag);

    // Object should be visible via get_object metadata.
    let obj = coord
        .storage_node()
        .test_get_object_meta(&bucket, &key)
        .unwrap();
    let live_obj = obj.as_live().expect("expected live object");
    assert!(matches!(
        live_obj.layout,
        ObjectLayout::MultipartManifest { .. }
    ));
    assert_eq!(live_obj.layout.parts_count(), Some(2));
    assert_eq!(
        live_obj.size,
        big_part.len() as u64 + small_last.len() as u64
    );
    assert_eq!(live_obj.generation_id, upload.object_generation_id);

    // object_parts should be committed.
    let committed = coord
        .storage_node()
        .test_get_object_parts(&bucket, &key, result.version_id)
        .unwrap();
    assert_eq!(committed.len(), 2);
    assert_eq!(committed[0].part_number, 1);
    assert_eq!(committed[1].part_number, 2);
    let topology = storage::PgTopology::new(coord.storage_node().test_pg_ids()).unwrap();
    for part in &committed {
        assert_eq!(
            part.data_pg_id,
            topology
                .object_generation_multipart_part_data_pg(
                    &bucket,
                    &key,
                    upload.object_generation_id,
                    part.part_number,
                )
                .get()
        );
    }

    // Upload should be deleted.
    let err = coord
        .storage_node()
        .test_get_multipart_upload(&bucket, &key, &upload_id)
        .unwrap_err();
    assert!(matches!(
        err,
        storage::ObjectPgActionError::Metadata(storage::MetadataError::NoSuchUpload { .. })
    ));
}

#[test]
fn delete_multipart_object_eventually_reclaims_part_shards() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_reclaim_sweeper(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"part")]);
    let segments_to_reclaim = coord
        .storage_node()
        .test_get_all_multipart_part_segments_for_upload(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &upload_id,
        )
        .unwrap();
    assert!(!segments_to_reclaim.is_empty());
    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap();

    let generation_id = match coord
        .storage_node()
        .test_get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
        .unwrap()
    {
        StoredObject::Live(record) => record.generation_id,
        other @ StoredObject::DeleteMarker(_) => {
            panic!("expected live multipart object, got {other:?}")
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
    for segment in segments_to_reclaim {
        assert_shard_set_deleted(
            &coord,
            segment.data_pg_id,
            &segment.segment_okh,
            segment.segment_vid,
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
        );
    }
}

#[test]
fn complete_multipart_upload_missing_part() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, mut parts) =
        create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1"), (3, b"data3")]);

    // Request completion with part 2 which was never uploaded.
    parts.insert(
        1,
        CompletePart {
            part_number: 2,
            etag: "\"0000000000000000\"".to_string(),
            checksum: None,
        },
    );

    let err = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::InvalidPart { part_number: 2 }));
}

#[test]
fn complete_multipart_upload_wrong_etag() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, mut parts) =
        create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1")]);

    // Tamper with the ETag.
    parts[0].etag = "\"ffffffffffffffff\"".to_string();

    let err = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::InvalidPart { part_number: 1 }));
}

#[test]
fn complete_multipart_upload_invalid_order() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, parts) =
        create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1"), (2, b"data2")]);

    // Reverse the order.
    let reversed = vec![parts[1].clone(), parts[0].clone()];
    let err = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            parts: &reversed,
            claimed_checksum: None,

            expected_object_size: None,

            cond: &WriteCondition::default(),

            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::InvalidPartOrder));
}

#[test]
fn complete_multipart_upload_too_small_non_final_part() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    // Part 1 is only 10 bytes (below 5 MiB minimum for non-final).
    let (upload_id, parts) = create_upload_with_parts(
        &coord,
        "bucket",
        "key",
        &[(1, b"small-part"), (2, b"last-part")],
    );

    let err = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap_err();
    assert!(matches!(
        err,
        ServerError::EntityTooSmall { part_number: 1, .. }
    ));
}

#[test]
fn complete_multipart_upload_single_part_any_size() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    // A single part can be any size (it's the "final" part).
    let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"tiny")]);

    let result = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap();
    assert!(result.etag.ends_with("-1\""));
}

#[test]
fn complete_multipart_upload_empty_part_list() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
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
                &create.upload_id,
                test_requester(),
                None,
            ),
            parts: &[],
            claimed_checksum: None,

            expected_object_size: None,

            cond: &WriteCondition::default(),

            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(
        err,
        ServerError::InvalidRequest { reason } if reason == "part list must not be empty"
    ));
}

#[test]
fn complete_multipart_upload_retry_after_validation_failure() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    // Upload two small parts.
    let (upload_id, parts) =
        create_upload_with_parts(&coord, "bucket", "key", &[(1, b"small"), (2, b"last")]);

    // First attempt fails because part 1 is too small.
    let err = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::EntityTooSmall { .. }));

    // Upload remains usable — re-upload part 1 with large data and retry.
    let big_data = vec![0u8; 5 * 1024 * 1024];
    let new_part1 = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            data: &big_data,
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap();

    let retry_parts = vec![
        CompletePart {
            part_number: 1,
            etag: new_part1.etag,
            checksum: None,
        },
        parts[1].clone(),
    ];
    let result = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            parts: &retry_parts,
            claimed_checksum: None,

            expected_object_size: None,

            cond: &WriteCondition::default(),

            sse_customer: None,
        })
        .unwrap();
    assert!(result.etag.ends_with("-2\""));
}

#[test]
fn complete_multipart_upload_duplicate_part_numbers() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1")]);

    // Duplicate part number 1.
    let duped = vec![parts[0].clone(), parts[0].clone()];
    let err = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            parts: &duped,
            claimed_checksum: None,

            expected_object_size: None,

            cond: &WriteCondition::default(),

            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::InvalidPartOrder));
}

#[test]
fn complete_multipart_upload_overwrite_unversioned() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    // First multipart upload to key.
    let (upload_id1, parts1) =
        create_upload_with_parts(&coord, "bucket", "key", &[(1, b"first-upload")]);
    let result1 = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id1,
                test_requester(),
                None,
            ),
            parts: &parts1,
            claimed_checksum: None,

            expected_object_size: None,

            cond: &WriteCondition::default(),

            sse_customer: None,
        })
        .unwrap();
    assert!(result1.etag.ends_with("-1\""));

    // Second multipart upload to the same key (unversioned, version_id=0).
    let big_part = vec![0u8; 5 * 1024 * 1024];
    let (upload_id2, parts2) = create_upload_with_parts(
        &coord,
        "bucket",
        "key",
        &[(1, &big_part), (2, b"second-data-b")],
    );
    let result2 = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id2,
                test_requester(),
                None,
            ),
            parts: &parts2,
            claimed_checksum: None,

            expected_object_size: None,

            cond: &WriteCondition::default(),

            sse_customer: None,
        })
        .unwrap();
    assert!(result2.etag.ends_with("-2\""));
    assert_ne!(result1.etag, result2.etag);

    // Verify the object was overwritten — should have 2 parts now.
    let obj = coord
        .storage_node()
        .test_get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
        .unwrap();
    let live_obj = obj.as_live().expect("expected live object");
    assert_eq!(live_obj.layout.parts_count(), Some(2));

    // Old manifest parts (from first upload) should be replaced.
    let committed = coord
        .storage_node()
        .test_get_object_parts(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            VersionId::Null,
        )
        .unwrap();
    assert_eq!(committed.len(), 2);
}

#[test]
fn complete_multipart_upload_list_shows_composite_etag() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1")]);
    let result = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap();

    // list_objects_v2 should return the composite ETag with -N suffix.
    let list = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 100,
            requested_max_keys: Some(100),
        })
        .unwrap();
    assert_eq!(list.objects.len(), 1);
    assert_eq!(list.objects[0].etag, result.etag);
    assert!(
        list.objects[0].etag.ends_with("-1\""),
        "etag = {}",
        list.objects[0].etag
    );
}

#[test]
fn complete_multipart_upload_list_versions_shows_composite_etag() {
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

    let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1")]);
    let result = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap();

    let versions = coord
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
    assert_eq!(versions.versions.len(), 1);
    assert_eq!(versions.versions[0].etag, result.etag);
    assert!(
        versions.versions[0].etag.ends_with("-1\""),
        "etag = {}",
        versions.versions[0].etag
    );
}

// --- AbortMultipartUpload tests ---

#[test]
fn abort_multipart_upload_success() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, _parts) =
        create_upload_with_parts(&coord, "bucket", "key", &[(1, b"part1"), (2, b"part2")]);

    coord
        .abort_multipart_upload(&multipart_object_request_with_expected_owner(
            "bucket",
            "key",
            &upload_id,
            test_requester(),
            None,
        ))
        .unwrap();

    // Upload should no longer exist.
    let err = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            data: b"nope",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "expected NoSuchUpload, got {err:?}"
    );

    // ListMultipartUploads should be empty.
    let uploads = coord
        .list_multipart_uploads(&ListMultipartUploadsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 100,
        })
        .unwrap();
    assert!(uploads.uploads.is_empty());
}

#[test]
fn complete_multipart_upload_rejects_non_in_progress_upload() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"part1")]);

    coord
        .storage_node()
        .test_set_upload_state(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &upload_id,
            UploadState::Completing,
        )
        .unwrap();

    let err = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "expected NoSuchUpload, got {err:?}"
    );
}

#[test]
fn abort_multipart_upload_reclaims_part_shards() {
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

    test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            data: b"part1",
            claimed_checksum: None,
            sse_customer: None,
        },
    )
    .unwrap();

    let session = begin_stream_part_test(&coord, "bucket", "key", &create.upload_id, 2).unwrap();
    let streamed_part = b"stream-part";
    coord
        .append_plaintext_stream_segment_for_test(
            "bucket",
            "key",
            &session.session_id,
            0,
            streamed_part,
        )
        .unwrap();
    coord
        .finalize_stream_part(FinalizeStreamPartRequest {
            upload: multipart_object_request("bucket", "key", &create.upload_id, test_requester()),
            session_id: &session.session_id,
            part_number: 2,
            crc64: checksum::crc64::checksum(streamed_part),
            total_size: streamed_part.len() as u64,
            claimed_checksum: None,
            computed_checksum: None,
        })
        .unwrap();

    let streamed_segments = coord
        .storage_node()
        .test_get_all_multipart_part_segments_for_upload(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &create.upload_id,
        )
        .unwrap();
    assert!(!streamed_segments.is_empty());
    for segment in &streamed_segments {
        assert_payload_shard_files_state(
            &coord,
            segment.data_pg_id,
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
            &segment.segment_okh,
            segment.segment_vid,
            true,
            "streamed UploadPart before abort",
        );
    }

    coord
        .abort_multipart_upload(&multipart_object_request_with_expected_owner(
            "bucket",
            "key",
            &create.upload_id,
            test_requester(),
            None,
        ))
        .unwrap();

    for segment in streamed_segments {
        assert_shard_set_deleted(
            &coord,
            segment.data_pg_id,
            &segment.segment_okh,
            segment.segment_vid,
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
        );
    }
}

#[test]
fn abort_multipart_upload_nonexistent() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let err = coord
        .abort_multipart_upload(&multipart_object_request_with_expected_owner(
            "bucket",
            "key",
            "no-such-upload",
            test_requester(),
            None,
        ))
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "expected NoSuchUpload, got {err:?}"
    );
}

#[test]
fn list_requests_reject_oversized_user_supplied_keys() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let oversized = "x".repeat(1025);

    let err = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: Some(oversized.as_str()),
            delimiter: None,
            continuation_token: None,
            max_keys: 100,
            requested_max_keys: Some(100),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::InvalidArgument { .. }));

    let err = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            continuation_token: Some(oversized.as_str()),
            max_keys: 100,
            requested_max_keys: Some(100),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::InvalidArgument { .. }));

    let err = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: Some(oversized.as_str()),
            version_id_marker: None,
            max_keys: 100,
            requested_max_keys: Some(100),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::InvalidArgument { .. }));

    let err = coord
        .list_multipart_uploads(&ListMultipartUploadsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: Some(oversized.as_str()),
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 100,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::InvalidArgument { .. }));
}

#[test]
fn abort_multipart_upload_idempotent() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    // First abort succeeds.
    coord
        .abort_multipart_upload(&multipart_object_request_with_expected_owner(
            "bucket",
            "key",
            &create.upload_id,
            test_requester(),
            None,
        ))
        .unwrap();

    // AWS treats AbortMultipartUpload as idempotently successful for an issued ID.
    coord
        .abort_multipart_upload(&multipart_object_request_with_expected_owner(
            "bucket",
            "key",
            &create.upload_id,
            test_requester(),
            None,
        ))
        .unwrap();
}

#[test]
fn abort_multipart_upload_wrong_bucket_key() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "other", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
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
            "other",
            "key",
            &create.upload_id,
            test_requester(),
            None,
        ))
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "expected NoSuchUpload, got {err:?}"
    );
}

#[test]
fn abort_completed_multipart_upload_succeeds_and_does_not_affect_object() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    // Create and complete an upload.
    let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1")]);
    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap();

    // Abort the same completed upload_id succeeds.
    coord
        .abort_multipart_upload(&multipart_object_request_with_expected_owner(
            "bucket",
            "key",
            &upload_id,
            test_requester(),
            None,
        ))
        .unwrap();

    // Object should still exist (visible in listing).
    let list = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 100,
            requested_max_keys: Some(100),
        })
        .unwrap();
    assert_eq!(list.objects.len(), 1);
    assert_eq!(list.objects[0].key, "key");
}

#[test]
fn abort_completed_multipart_upload_after_object_delete_succeeds() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1")]);
    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap();

    coord
        .delete_object(&DeleteObjectRequest {
            object: object_version_request("bucket", "key", None, test_requester()),
            bypass_governance: false,
            cond: &DeleteCondition::default(),
        })
        .unwrap();

    coord
        .abort_multipart_upload(&multipart_object_request_with_expected_owner(
            "bucket",
            "key",
            &upload_id,
            test_requester(),
            None,
        ))
        .unwrap();
}

#[test]
fn abort_completed_multipart_upload_after_overwrite_succeeds() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (first_upload_id, first_parts) =
        create_upload_with_parts(&coord, "bucket", "key", &[(1, b"first")]);
    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &first_upload_id,
                test_requester(),
                None,
            ),
            parts: &first_parts,
            claimed_checksum: None,

            expected_object_size: None,

            cond: &WriteCondition::default(),

            sse_customer: None,
        })
        .unwrap();

    let (second_upload_id, second_parts) =
        create_upload_with_parts(&coord, "bucket", "key", &[(1, b"second")]);
    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &second_upload_id,
                test_requester(),
                None,
            ),
            parts: &second_parts,
            claimed_checksum: None,

            expected_object_size: None,

            cond: &WriteCondition::default(),

            sse_customer: None,
        })
        .unwrap();

    coord
        .abort_multipart_upload(&multipart_object_request_with_expected_owner(
            "bucket",
            "key",
            &first_upload_id,
            test_requester(),
            None,
        ))
        .unwrap();
    coord
        .abort_multipart_upload(&multipart_object_request_with_expected_owner(
            "bucket",
            "key",
            &second_upload_id,
            test_requester(),
            None,
        ))
        .unwrap();

    let object = coord
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
    assert_eq!(object.body.read_all().unwrap(), b"second");
}

#[test]
fn abort_wrong_upload_id_after_complete_still_fails() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1")]);
    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap();

    let err = coord
        .abort_multipart_upload(&multipart_object_request_with_expected_owner(
            "bucket",
            "key",
            "definitely-wrong-upload-id",
            test_requester(),
            None,
        ))
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "expected NoSuchUpload, got {err:?}"
    );
}

#[test]
fn abort_completed_multipart_upload_after_bucket_delete_and_recreate_fails() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1")]);
    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap();

    coord
        .delete_object(&DeleteObjectRequest {
            object: object_version_request("bucket", "key", None, test_requester()),
            bypass_governance: false,
            cond: &DeleteCondition::default(),
        })
        .unwrap();
    delete_bucket_test(&coord, "bucket").unwrap();
    wait_until_bucket_gone(&coord, "bucket");
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let err = coord
        .abort_multipart_upload(&multipart_object_request_with_expected_owner(
            "bucket",
            "key",
            &upload_id,
            test_requester(),
            None,
        ))
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "expected NoSuchUpload, got {err:?}"
    );
}

#[test]
fn upload_part_after_abort_rejected() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            data: b"data",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap();

    coord
        .abort_multipart_upload(&multipart_object_request_with_expected_owner(
            "bucket",
            "key",
            &create.upload_id,
            test_requester(),
            None,
        ))
        .unwrap();

    let err = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number: 2,
            data: b"more",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "expected NoSuchUpload, got {err:?}"
    );
}

#[test]
fn begin_stream_part_after_same_key_abort_stress_regression() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    for iteration in 0..512u32 {
        let create_a = coord
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

        coord
            .abort_multipart_upload(&multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create_a.upload_id,
                test_requester(),
                None,
            ))
            .unwrap();

        let create_b = coord
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

        let session = coord.begin_stream_part(&BeginStreamPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create_b.upload_id,
                test_requester(),
                None,
            ),
            part_number: 2,
            policy_context: PutObjectPolicyContext::default(),
            sse_customer: None,
        });
        assert!(
            session.is_ok(),
            "iteration {iteration}: begin_stream_part after abort/create on same key failed for old upload {} and new upload {}: {session:?}",
            create_a.upload_id,
            create_b.upload_id,
        );

        coord
            .abort_multipart_upload(&multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create_b.upload_id,
                test_requester(),
                None,
            ))
            .unwrap();
    }
}

// --- ListParts tests ---

#[test]
fn list_parts_basic() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, _parts) = create_upload_with_parts(
        &coord,
        "bucket",
        "key",
        &[(1, b"data1"), (3, b"data3"), (5, b"data5")],
    );

    let result = coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap();
    assert_eq!(result.parts.len(), 3);
    assert_eq!(result.parts[0].part_number, 1);
    assert_eq!(result.parts[1].part_number, 3);
    assert_eq!(result.parts[2].part_number, 5);
    assert_eq!(result.parts[0].size, 5); // "data1"
    assert!(!result.is_truncated);
    assert_eq!(result.next_part_number_marker, Some(5));
}

#[test]
fn list_parts_pagination() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, _parts) = create_upload_with_parts(
        &coord,
        "bucket",
        "key",
        &[(1, b"a"), (2, b"b"), (3, b"c"), (4, b"d")],
    );

    // Page 1: max_parts=2
    let page1 = coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            part_number_marker: None,
            max_parts: 2,
        })
        .unwrap();
    assert_eq!(page1.parts.len(), 2);
    assert_eq!(page1.parts[0].part_number, 1);
    assert_eq!(page1.parts[1].part_number, 2);
    assert!(page1.is_truncated);
    assert!(page1.next_part_number_marker.is_some());

    // Page 2: continue from marker
    let page2 = coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            part_number_marker: page1.next_part_number_marker,
            max_parts: 2,
        })
        .unwrap();
    assert_eq!(page2.parts.len(), 2);
    assert_eq!(page2.parts[0].part_number, 3);
    assert_eq!(page2.parts[1].part_number, 4);
    assert!(!page2.is_truncated);
    assert_eq!(page2.next_part_number_marker, Some(4));
}

#[test]
fn list_parts_etag_format() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, complete_parts) =
        create_upload_with_parts(&coord, "bucket", "key", &[(1, b"hello")]);

    let result = coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap();
    assert_eq!(result.parts.len(), 1);
    // ListParts ETag should match the ETag returned by UploadPart.
    assert_eq!(result.parts[0].etag, complete_parts[0].etag);
}

#[test]
fn list_parts_wrong_bucket_key() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "other", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
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
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request_with_expected_owner(
                "other",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "expected NoSuchUpload, got {err:?}"
    );
}

#[test]
fn list_parts_nonexistent_upload() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let err = coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                "no-such-upload",
                test_requester(),
                None,
            ),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "expected NoSuchUpload, got {err:?}"
    );
}

#[test]
fn begin_stream_part_nonexistent_upload() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let err = coord
        .begin_stream_part(&BeginStreamPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                "no-such-upload",
                test_requester(),
                None,
            ),
            part_number: 1,
            policy_context: PutObjectPolicyContext::default(),
            sse_customer: None,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "expected NoSuchUpload, got {err:?}"
    );
    assert_eq!(
        upload_part_stream_session_count(
            &coord,
            "bucket",
            "key",
            &trusted_upload_id("no-such-upload"),
            1,
        ),
        0,
        "nonexistent upload should not create a stream session row",
    );
}

#[test]
fn begin_stream_part_wrong_key_returns_no_such_upload_without_creating_session() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key-a", test_requester(), None),
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
                "key-b",
                &upload.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            policy_context: PutObjectPolicyContext::default(),
            sse_customer: None,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "expected NoSuchUpload, got {err:?}"
    );
    assert_eq!(
        upload_part_stream_session_count(&coord, "bucket", "key-a", &upload.upload_id, 1),
        0,
        "wrong-key request should not create a stream session for the real upload key",
    );
    assert_eq!(
        upload_part_stream_session_count(&coord, "bucket", "key-b", &upload.upload_id, 1),
        0,
        "wrong-key request should not create a stream session for the requested key",
    );
}

#[test]
fn begin_stream_part_wrong_bucket_returns_no_such_upload_without_creating_session() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket-a", false)
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "bucket-b", false)
        .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket-a", "key", test_requester(), None),
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
                "bucket-b",
                "key",
                &upload.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            policy_context: PutObjectPolicyContext::default(),
            sse_customer: None,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "expected NoSuchUpload, got {err:?}"
    );
    assert_eq!(
        upload_part_stream_session_count(&coord, "bucket-a", "key", &upload.upload_id, 1),
        0,
        "wrong-bucket request should not create a stream session for the real upload bucket",
    );
    assert_eq!(
        upload_part_stream_session_count(&coord, "bucket-b", "key", &upload.upload_id, 1),
        0,
        "wrong-bucket request should not create a stream session for the requested bucket",
    );
}

#[test]
fn begin_stream_part_aborting_upload_returns_no_such_upload_without_creating_session() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let upload = coord
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

    coord
        .storage_node()
        .test_set_upload_state(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &upload.upload_id,
            UploadState::Aborting,
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
            sse_customer: None,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "expected NoSuchUpload, got {err:?}"
    );
    assert_eq!(
        upload_part_stream_session_count(&coord, "bucket", "key", &upload.upload_id, 1),
        0,
        "aborting upload should not create a stream session row",
    );
}

#[test]
fn begin_stream_part_completing_upload_returns_no_such_upload_without_creating_session() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let upload = coord
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

    coord
        .storage_node()
        .test_set_upload_state(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &upload.upload_id,
            UploadState::Completing,
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
            sse_customer: None,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "expected NoSuchUpload, got {err:?}"
    );
    assert_eq!(
        upload_part_stream_session_count(&coord, "bucket", "key", &upload.upload_id, 1),
        0,
        "completing upload should not create a stream session row",
    );
}

#[test]
fn authorize_list_parts_rejects_non_owner_requester() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                test_helpers::requester("owner-a"),
                None,
            ),
            metadata: &metadata,
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
        .authorize_list_parts(&ListPartsRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_helpers::requester("other-user"),
                None,
            ),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn list_parts_after_reupload_shows_latest() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    // Upload part 1, then overwrite it.
    test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            data: b"original",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap();
    let reupload = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            data: b"replaced",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap();

    let result = coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap();
    assert_eq!(result.parts.len(), 1);
    assert_eq!(result.parts[0].etag, reupload.etag);
    assert_eq!(result.parts[0].size, "replaced".len() as u64);
}

// --- Multipart-aware read tests (Step 9) ---

/// Make part data: first MIN_PART bytes are `fill`, rest is padding.
/// For the final part, `size` can be less than MIN_PART.
fn make_part(fill: u8, size: usize) -> Vec<u8> {
    vec![fill; size]
}

/// Helper: create a completed multipart object with given (part_number, data) pairs.
fn create_completed_multipart_vec(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    part_data: &[(u32, Vec<u8>)],
) -> CompleteMultipartUploadResult {
    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request(bucket, key, test_requester()),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    let mut complete_parts = Vec::new();
    for (part_number, data) in part_data {
        let result = test_helpers::upload_part(
            coord,
            &UploadPartRequest {
                upload: multipart_object_request(bucket, key, &create.upload_id, test_requester()),
                part_number: *part_number,
                data,
                claimed_checksum: None,
                sse_customer: None,
            },
        )
        .unwrap();
        complete_parts.push(CompletePart {
            part_number: *part_number,
            etag: result.etag,
            checksum: None,
        });
    }
    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request(bucket, key, &create.upload_id, test_requester()),
            parts: &complete_parts,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap()
}

fn corrupt_committed_part_payload_crc64(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    version_id: VersionId,
    part_number: u32,
) {
    let bucket_name = trusted_bucket_name(bucket);
    let object_key = trusted_object_key(key);
    let mut parts = coord
        .storage_node()
        .test_get_object_parts(&bucket_name, &object_key, version_id)
        .unwrap();
    let part = parts
        .iter_mut()
        .find(|part| part.part_number == part_number)
        .expect("committed object part exists");
    part.payload_crc64 ^= 1;
    coord
        .storage_node()
        .test_replace_object_parts(&bucket_name, &object_key, version_id, &parts)
        .unwrap();
}

#[test]
fn get_multipart_object_full() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let part1 = make_part(0xAA, MIN_PART);
    let part2 = make_part(0xBB, 100);
    let expected: Vec<u8> = [part1.as_slice(), part2.as_slice()].concat();

    let result = create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1), (2, part2)]);

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
            cond: &ReadCondition::default(),
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), expected);
    assert_eq!(obj.etag, result.etag);
    assert_eq!(obj.size, expected.len() as u64);
    assert!(obj.etag.ends_with("-2\""), "etag = {}", obj.etag);
}

#[test]
fn get_multipart_object_single_part() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    create_completed_multipart_vec(&coord, "bucket", "key", &[(1, b"only-part".to_vec())]);

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
            cond: &ReadCondition::default(),
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"only-part");
}

#[test]
fn head_multipart_object() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let part1 = make_part(0xAA, MIN_PART);
    let part2 = make_part(0xBB, 200);
    let total_size = part1.len() + part2.len();

    let result = create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1), (2, part2)]);

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
            cond: &ReadCondition::default(),
        })
        .unwrap();
    assert_eq!(head.size, total_size as u64);
    assert_eq!(head.etag, result.etag);
    assert!(head.etag.ends_with("-2\""), "etag = {}", head.etag);
}

#[test]
fn get_multipart_object_range_within_part() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let part1 = make_part(0xAA, MIN_PART);
    let part2 = make_part(0xBB, 100);

    create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1), (2, part2)]);

    // Range within first part: bytes 10-19
    let range = coord
        .get_object_range(&GetObjectRangeRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            range: ByteRange::Range { start: 10, end: 19 },
            cond: &ReadCondition::default(),
        })
        .unwrap();
    assert_eq!(range.body.read_all().unwrap(), vec![0xAA; 10]);
    assert_eq!(range.range_start, 10);
    assert_eq!(range.range_end, 19);
}

#[test]
fn get_multipart_object_range_spanning_parts() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let part1 = make_part(0xAA, MIN_PART);
    let part2 = make_part(0xBB, MIN_PART);
    let part3 = make_part(0xCC, 100);

    create_completed_multipart_vec(
        &coord,
        "bucket",
        "key",
        &[(1, part1), (2, part2), (3, part3)],
    );

    // Range spanning part1/part2 boundary: last 4 bytes of part1 + first 4 of part2
    let boundary = MIN_PART as u64;
    let range = coord
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
                start: boundary - 4,
                end: boundary + 3,
            },
            cond: &ReadCondition::default(),
        })
        .unwrap();
    let mut expected = vec![0xAA; 4];
    expected.extend_from_slice(&[0xBB; 4]);
    assert_eq!(range.body.read_all().unwrap(), expected);
}

#[test]
fn get_multipart_object_range_suffix() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let part1 = make_part(0xAA, MIN_PART);
    let part2 = make_part(0xBB, 100);

    create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1), (2, part2)]);

    // Suffix range: last 50 bytes (all within part2)
    let range = coord
        .get_object_range(&GetObjectRangeRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            range: ByteRange::Suffix { length: 50 },
            cond: &ReadCondition::default(),
        })
        .unwrap();
    assert_eq!(range.body.read_all().unwrap(), vec![0xBB; 50]);
}

#[test]
fn copy_multipart_source() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "src-bucket", false)
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "dst-bucket", false)
        .unwrap();

    let part1 = make_part(0xAA, MIN_PART);
    let part2 = make_part(0xBB, 200);
    let expected: Vec<u8> = [part1.as_slice(), part2.as_slice()].concat();

    create_completed_multipart_vec(&coord, "src-bucket", "src-key", &[(1, part1), (2, part2)]);

    // Copy multipart source to destination (creates inline object).
    coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("src-bucket", "src-key", None),
            destination: object_request_with_expected_owner(
                "dst-bucket",
                "dst-key",
                test_requester(),
                None,
            ),
            dst_condition: &WriteCondition::default(),
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

    // Destination should have the concatenated data as inline object.
    let dst = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "dst-bucket",
                "dst-key",
                None,
                test_requester(),
                None,
            ),
            cond: &ReadCondition::default(),
        })
        .unwrap();
    assert_eq!(dst.body.read_all().unwrap(), expected);
}

#[test]
fn get_multipart_object_zero_byte_single_part() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    create_completed_multipart_vec(&coord, "bucket", "key", &[(1, vec![])]);

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
            cond: &ReadCondition::default(),
        })
        .unwrap();
    assert!(obj.body.read_all().unwrap().is_empty());
    assert_eq!(obj.size, 0);
}

#[test]
fn get_multipart_object_zero_byte_final_part() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let part1 = make_part(0xAA, MIN_PART);
    let expected = part1.clone();

    create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1), (2, vec![])]);

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
            cond: &ReadCondition::default(),
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), expected);
    assert_eq!(obj.size, MIN_PART as u64);
}

#[test]
fn get_object_part_zero_byte_single_part() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    create_completed_multipart_vec(&coord, "bucket", "key", &[(1, vec![])]);

    let result = coord
        .get_object_part(&GetObjectPartRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            part_number: 1,
            cond: &ReadCondition::default(),
        })
        .unwrap();
    assert!(result.body.read_all().unwrap().is_empty());
    assert_eq!(result.part_size, 0);
    assert_eq!(result.size, 0);
    assert_eq!(result.parts_count, Some(1));
    assert_eq!(result.part_start, 0);
    assert_eq!(result.part_end, 0);
}

#[test]
fn get_object_part_zero_byte_final_part() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let part1 = make_part(0xAA, MIN_PART);
    create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1.clone()), (2, vec![])]);

    // Part 1 should return full data
    let result = coord
        .get_object_part(&GetObjectPartRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            part_number: 1,
            cond: &ReadCondition::default(),
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), part1);
    assert_eq!(result.part_size, MIN_PART as u64);
    assert_eq!(result.part_start, 0);
    assert_eq!(result.part_end, MIN_PART as u64 - 1);

    // Part 2 (zero-byte) should return empty data
    let result = coord
        .get_object_part(&GetObjectPartRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            part_number: 2,
            cond: &ReadCondition::default(),
        })
        .unwrap();
    assert!(result.body.read_all().unwrap().is_empty());
    assert_eq!(result.part_size, 0);
    assert_eq!(result.parts_count, Some(2));
    assert_eq!(result.part_start, MIN_PART as u64);
    assert_eq!(result.part_end, MIN_PART as u64);
}

#[test]
fn head_object_part_non_multipart() {
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
            data: b"hello world",
            metadata: &MetadataBlob::from_headers(&[("x-amz-meta-foo", "bar")]).unwrap(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // partNumber=1 on non-multipart object returns the full object.
    let result = coord
        .head_object_part(&GetObjectPartRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            part_number: 1,
            cond: &ReadCondition::default(),
        })
        .unwrap();
    assert_eq!(result.part_size, 11);
    assert_eq!(result.total_size, 11);
    assert_eq!(result.parts_count, None);
    assert_eq!(result.metadata.get("x-amz-meta-foo"), Some("bar"));

    // partNumber=2 on non-multipart object returns the object-read partNumber error.
    let err = coord
        .head_object_part(&GetObjectPartRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            part_number: 2,
            cond: &ReadCondition::default(),
        })
        .unwrap_err();
    assert!(matches!(
        err,
        ServerError::InvalidPartNumber {
            part_number: 2,
            parts_count: 1
        }
    ));
}

#[test]
fn head_object_part_non_multipart_zero_byte() {
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
            data: b"",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let result = coord
        .head_object_part(&GetObjectPartRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            part_number: 1,
            cond: &ReadCondition::default(),
        })
        .unwrap();
    assert_eq!(result.part_size, 0);
    assert_eq!(result.total_size, 0);
    assert_eq!(result.parts_count, None);
}

#[test]
fn head_multipart_object_zero_byte() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    create_completed_multipart_vec(&coord, "bucket", "key", &[(1, vec![])]);

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
            cond: &ReadCondition::default(),
        })
        .unwrap();
    assert_eq!(head.size, 0);
}

#[test]
fn copy_multipart_source_zero_byte() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "src", false)
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "dst", false)
        .unwrap();

    create_completed_multipart_vec(&coord, "src", "key", &[(1, vec![])]);

    coord
        .copy_object(&CopyObjectRequest {
            source: copy_source_with_condition_and_expected_owner(
                "src",
                "key",
                None,
                &ReadCondition::default(),
                None,
            ),
            destination: object_request_with_expected_owner("dst", "key", test_requester(), None),
            dst_condition: &WriteCondition::default(),
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

    let dst = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "dst",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: &ReadCondition::default(),
        })
        .unwrap();
    assert!(dst.body.read_all().unwrap().is_empty());
}

#[test]
fn get_object_rejects_incomplete_manifest_before_body_read() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    // Create a multipart object, then corrupt manifest by deleting a part row.
    let part1 = make_part(0xAA, MIN_PART);
    let part2 = make_part(0xBB, 100);

    let result = create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1), (2, part2)]);

    // Get the real manifest, then replace with only part 2 (gap: part 1 missing).
    let real_parts = coord
        .storage_node()
        .test_get_object_parts(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            result.version_id,
        )
        .unwrap();
    assert_eq!(real_parts.len(), 2);
    let part2_record = real_parts[1].clone(); // real part 2 with valid shards
    coord
        .storage_node()
        .test_replace_object_parts(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            result.version_id,
            &[part2_record],
        )
        .unwrap();

    let err = match coord.get_object(&GetObjectRequest {
        sse_customer: None,
        object: object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        ),
        cond: &ReadCondition::default(),
    }) {
        Ok(_) => panic!("incomplete manifest unexpectedly produced an object body"),
        Err(error) => error,
    };
    assert_eq!(err.s3_error_code(), "InternalError", "{err:?}");
}

#[test]
fn get_object_part_rejects_bad_part_payload_crc64() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let part1 = make_part(0xAA, MIN_PART);
    let part2 = make_part(0xBB, 100);
    let result = create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1), (2, part2)]);

    corrupt_committed_part_payload_crc64(&coord, "bucket", "key", result.version_id, 1);

    let err = coord
        .get_object_part(&GetObjectPartRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            part_number: 1,
            cond: &ReadCondition::default(),
        })
        .unwrap()
        .body
        .read_all()
        .unwrap_err();
    assert!(
        matches!(err, ServerError::IntegrityError { .. }),
        "expected IntegrityError for corrupted part crc64, got {err:?}"
    );
}

#[test]
fn get_multipart_range_uses_segment_crc_without_whole_part_payload_crc64() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let part1 = make_part(0xAA, INTERNAL_SEGMENT_SIZE + 123);
    let part2 = make_part(0xBB, 100);
    let result = create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1), (2, part2)]);

    corrupt_committed_part_payload_crc64(&coord, "bucket", "key", result.version_id, 1);

    let body = coord
        .get_object_range(&GetObjectRangeRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            range: ByteRange::Range { start: 0, end: 9 },
            cond: &ReadCondition::default(),
        })
        .unwrap()
        .body
        .read_all()
        .unwrap();
    assert_eq!(body, vec![0xAA; 10]);
}

// ── CompleteMultipartUpload checksum tests ──────────────────────────

/// Helper: create a multipart upload with a checksum algorithm, upload parts with checksums,
/// and return (upload_id, complete_parts_with_checksums, part_data_list).
fn create_checksum_upload(
    coord: &Coordinator,
    bucket: &str,
    key: &str,
    algo: ChecksumAlgorithm,
    ctype: Option<ChecksumType>,
    part_data: &[&[u8]],
) -> (UploadId, Vec<CompletePart>) {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request(bucket, key, test_requester()),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: Some(MultipartChecksumConfig::new(algo, ctype).unwrap()),

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    let mut complete_parts = Vec::new();
    for (i, data) in part_data.iter().enumerate() {
        let part_number = (i + 1) as u32;
        let checksum_b64 = b64.encode(compute_checksum(algo, data).bytes());
        let claim = ChecksumClaim::from_base64(algo, &checksum_b64).unwrap();
        let result = test_helpers::upload_part(
            coord,
            &UploadPartRequest {
                upload: multipart_object_request(bucket, key, &create.upload_id, test_requester()),
                part_number,
                data,
                claimed_checksum: Some(&claim),
                sse_customer: None,
            },
        )
        .unwrap();
        complete_parts.push(CompletePart {
            part_number,
            etag: result.etag,
            checksum: Some(ChecksumClaim::from_base64(algo, &checksum_b64).unwrap()),
        });
    }
    (create.upload_id, complete_parts)
}

#[test]
fn complete_multipart_sha256_composite_checksum() {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let big = vec![0xABu8; 5 * 1024 * 1024];
    let small = b"final-part";
    let (upload_id, parts) = create_checksum_upload(
        &coord,
        "bucket",
        "key",
        ChecksumAlgorithm::Sha256,
        None, // defaults to COMPOSITE
        &[&big, small],
    );

    let result = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap();

    assert_eq!(result.checksum_algorithm, Some(ChecksumAlgorithm::Sha256));
    assert_eq!(result.checksum_type, Some(ChecksumType::Composite));
    let val = result.checksum_value.unwrap();
    assert!(val.ends_with("-2"), "expected -2 suffix, got {val}");

    // Verify the composite checksum manually:
    // hash(concat(raw_sha256_part1, raw_sha256_part2))
    let raw1 = compute_checksum(ChecksumAlgorithm::Sha256, &big);
    let raw2 = compute_checksum(ChecksumAlgorithm::Sha256, small);
    let mut concat = Vec::new();
    concat.extend_from_slice(raw1.bytes());
    concat.extend_from_slice(raw2.bytes());
    let expected_hash = compute_checksum(ChecksumAlgorithm::Sha256, &concat);
    let expected = format!("{}-2", b64.encode(expected_hash.bytes()));
    assert_eq!(val, expected);
}

#[test]
fn complete_multipart_sha512_composite_checksum() {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let big = vec![0xCDu8; 5 * 1024 * 1024];
    let small = b"final-part";
    let (upload_id, parts) = create_checksum_upload(
        &coord,
        "bucket",
        "key",
        ChecksumAlgorithm::Sha512,
        None,
        &[&big, small],
    );

    let result = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap();

    assert_eq!(result.checksum_algorithm, Some(ChecksumAlgorithm::Sha512));
    assert_eq!(result.checksum_type, Some(ChecksumType::Composite));
    let val = result.checksum_value.unwrap();

    let raw1 = compute_checksum(ChecksumAlgorithm::Sha512, &big);
    let raw2 = compute_checksum(ChecksumAlgorithm::Sha512, small);
    let mut concat = Vec::new();
    concat.extend_from_slice(raw1.bytes());
    concat.extend_from_slice(raw2.bytes());
    let expected_hash = compute_checksum(ChecksumAlgorithm::Sha512, &concat);
    let expected = format!("{}-2", b64.encode(expected_hash.bytes()));
    assert_eq!(val, expected);
}

#[test]
fn complete_multipart_sha512_object_checksum_header_mismatch_rejected() {
    use base64::Engine;

    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let data = b"single sha512 part";
    let (upload_id, parts) = create_checksum_upload(
        &coord,
        "bucket",
        "key",
        ChecksumAlgorithm::Sha512,
        Some(ChecksumType::Composite),
        &[data],
    );
    let wrong_checksum = base64::engine::general_purpose::STANDARD.encode([0u8; 64]);
    let claimed_checksum = EncodedChecksumClaim::new(ChecksumAlgorithm::Sha512, wrong_checksum);

    let err = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            parts: &parts,
            claimed_checksum: Some(&claimed_checksum),
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap_err();

    assert!(
        matches!(
            err,
            ServerError::ChecksumDigestMismatch { ref algorithm }
            if algorithm == "sha512"
        ),
        "expected ChecksumDigestMismatch for wrong SHA512 object checksum header, got {err:?}"
    );
}

#[test]
fn complete_multipart_crc32_full_object_checksum() {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let big = vec![0xABu8; 5 * 1024 * 1024];
    let small = b"final-part";
    let (upload_id, parts) = create_checksum_upload(
        &coord,
        "bucket",
        "key",
        ChecksumAlgorithm::Crc32,
        Some(ChecksumType::FullObject),
        &[&big, small],
    );

    let result = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap();

    assert_eq!(result.checksum_algorithm, Some(ChecksumAlgorithm::Crc32));
    assert_eq!(result.checksum_type, Some(ChecksumType::FullObject));

    // Verify: combine matches computing CRC32 of concatenated data.
    let mut full_data = big.clone();
    full_data.extend_from_slice(small);
    let expected_crc = checksum::crc32::checksum(&full_data);
    let expected = b64.encode(expected_crc.to_be_bytes());
    assert_eq!(result.checksum_value.unwrap(), expected);
}

#[test]
fn complete_multipart_crc32c_full_object_checksum() {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let big = vec![0xCDu8; 5 * 1024 * 1024];
    let small = b"last";
    let (upload_id, parts) = create_checksum_upload(
        &coord,
        "bucket",
        "key",
        ChecksumAlgorithm::Crc32c,
        Some(ChecksumType::FullObject),
        &[&big, small],
    );

    let result = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap();

    assert_eq!(result.checksum_algorithm, Some(ChecksumAlgorithm::Crc32c));
    assert_eq!(result.checksum_type, Some(ChecksumType::FullObject));

    let mut full_data = big.clone();
    full_data.extend_from_slice(small);
    let expected_crc = checksum::crc32c::checksum(&full_data);
    let expected = b64.encode(expected_crc.to_be_bytes());
    assert_eq!(result.checksum_value.unwrap(), expected);
}

#[test]
fn complete_multipart_crc64nvme_full_object_checksum() {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let big = vec![0xEFu8; 5 * 1024 * 1024];
    let small = b"end";
    let (upload_id, parts) = create_checksum_upload(
        &coord,
        "bucket",
        "key",
        ChecksumAlgorithm::Crc64nvme,
        Some(ChecksumType::FullObject),
        &[&big, small],
    );

    let result = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap();

    assert_eq!(
        result.checksum_algorithm,
        Some(ChecksumAlgorithm::Crc64nvme)
    );
    assert_eq!(result.checksum_type, Some(ChecksumType::FullObject));

    let mut full_data = big.clone();
    full_data.extend_from_slice(small);
    let expected_crc = checksum::crc64::checksum(&full_data);
    let expected = b64.encode(expected_crc.to_be_bytes());
    assert_eq!(result.checksum_value.unwrap(), expected);
}

#[test]
fn upload_part_without_checksum_claim_uses_upload_checksum_algorithm() {
    use base64::Engine;

    let b64 = base64::engine::general_purpose::STANDARD;
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request("bucket", "key", test_requester()),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: Some(
                MultipartChecksumConfig::new(
                    ChecksumAlgorithm::Crc64nvme,
                    Some(ChecksumType::FullObject),
                )
                .unwrap(),
            ),
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let data = b"upload-part-without-claim";
    let result = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request("bucket", "key", &create.upload_id, test_requester()),
            part_number: 1,
            data,
            claimed_checksum: None,
            sse_customer: None,
        },
    )
    .unwrap();

    let checksum = result.checksum.expect("expected computed part checksum");
    assert_eq!(checksum.algorithm(), ChecksumAlgorithm::Crc64nvme);
    assert_eq!(
        b64.encode(checksum.bytes()),
        b64.encode(checksum::crc64::checksum(data).to_be_bytes())
    );
}

#[test]
fn complete_multipart_allows_missing_part_checksum_elements() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request("bucket", "key", test_requester()),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: Some(
                MultipartChecksumConfig::new(ChecksumAlgorithm::Crc64nvme, None).unwrap(),
            ),
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    let part = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request("bucket", "key", &create.upload_id, test_requester()),
            part_number: 1,
            data: b"complete-without-part-checksum",
            claimed_checksum: None,
            sse_customer: None,
        },
    )
    .unwrap();

    let result = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            parts: &[CompletePart {
                part_number: 1,
                etag: part.etag,
                checksum: None,
            }],
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap();

    assert_eq!(
        result.checksum_algorithm,
        Some(ChecksumAlgorithm::Crc64nvme)
    );
    assert_eq!(result.checksum_type, Some(ChecksumType::FullObject));
    assert!(result.checksum_value.is_some());
}

#[test]
fn complete_multipart_composite_rejects_missing_part_checksum_elements() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request("bucket", "key", test_requester()),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: Some(MultipartChecksumConfig::new(ChecksumAlgorithm::Sha256, None).unwrap()),
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let data = b"complete-without-part-checksum";
    let checksum = {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD
            .encode(compute_checksum(ChecksumAlgorithm::Sha256, data).bytes())
    };
    let claimed_checksum =
        ChecksumClaim::from_base64(ChecksumAlgorithm::Sha256, &checksum).unwrap();
    let part = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request("bucket", "key", &create.upload_id, test_requester()),
            part_number: 1,
            data,
            claimed_checksum: Some(&claimed_checksum),
            sse_customer: None,
        },
    )
    .unwrap();

    let err = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            parts: &[CompletePart {
                part_number: 1,
                etag: part.etag,
                checksum: None,
            }],
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap_err();

    assert!(matches!(
        err,
        ServerError::CompleteMultipartMissingPartChecksum {
            algorithm,
            part_number: 1,
        } if algorithm == "sha256"
    ));
}

#[test]
fn complete_multipart_checksum_part_number_gap_is_invalid_request() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, parts) = create_checksum_upload(
        &coord,
        "bucket",
        "key",
        ChecksumAlgorithm::Sha256,
        None,
        &[b"omitted-part-one", b"selected-part-two"],
    );

    let err = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            parts: &parts[1..],
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap_err();

    assert!(matches!(
        err,
        ServerError::InvalidRequest { reason }
            if reason
                == "Part numbers must be consecutive and begin with 1 when a checksum is used."
    ));
}

#[test]
fn complete_multipart_crc32_composite_checksum() {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    // CRC32 + COMPOSITE is intentionally allowed (produces hash-of-hashes-N).
    let big = vec![0x11u8; 5 * 1024 * 1024];
    let small = b"tail";
    let (upload_id, parts) = create_checksum_upload(
        &coord,
        "bucket",
        "key",
        ChecksumAlgorithm::Crc32,
        Some(ChecksumType::Composite),
        &[&big, small],
    );

    let result = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap();

    assert_eq!(result.checksum_algorithm, Some(ChecksumAlgorithm::Crc32));
    assert_eq!(result.checksum_type, Some(ChecksumType::Composite));
    let val = result.checksum_value.unwrap();
    assert!(val.ends_with("-2"), "expected -2 suffix, got {val}");

    // Verify: hash of concatenated raw CRC32 bytes.
    let raw1 = compute_checksum(ChecksumAlgorithm::Crc32, &big);
    let raw2 = compute_checksum(ChecksumAlgorithm::Crc32, small);
    let mut concat = Vec::new();
    concat.extend_from_slice(raw1.bytes());
    concat.extend_from_slice(raw2.bytes());
    let hash = compute_checksum(ChecksumAlgorithm::Crc32, &concat);
    let expected = format!("{}-2", b64.encode(hash.bytes()));
    assert_eq!(val, expected);
}

#[test]
fn complete_multipart_bad_part_checksum_rejected() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let big = vec![0xAAu8; 5 * 1024 * 1024];
    let small = b"end";
    let (upload_id, mut parts) = create_checksum_upload(
        &coord,
        "bucket",
        "key",
        ChecksumAlgorithm::Crc32,
        Some(ChecksumType::FullObject),
        &[&big, small],
    );

    // Tamper with part 1's checksum value in the request.
    parts[0].checksum =
        Some(ChecksumClaim::from_base64(ChecksumAlgorithm::Crc32, "AAAAAA==").unwrap());

    let err = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::InvalidPart { part_number: 1 }),
        "expected InvalidPart, got {err:?}"
    );
}

#[test]
fn complete_multipart_no_checksum_defaults_crc64nvme() {
    use base64::Engine;

    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let big = vec![0u8; 5 * 1024 * 1024];
    let small = b"last";
    let mut full_data = big.clone();
    full_data.extend_from_slice(small);
    let (upload_id, parts) =
        create_upload_with_parts(&coord, "bucket", "key", &[(1, &big), (2, small)]);

    let result = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap();

    let expected = base64::engine::general_purpose::STANDARD
        .encode(checksum::crc64::checksum(&full_data).to_be_bytes());
    assert_eq!(
        result.checksum_algorithm,
        Some(ChecksumAlgorithm::Crc64nvme)
    );
    assert_eq!(result.checksum_type, Some(ChecksumType::FullObject));
    assert_eq!(result.checksum_value, Some(expected));
}

#[test]
fn complete_multipart_ignores_legacy_object_checksum_header_without_upload_algorithm() {
    use base64::Engine;

    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, parts) =
        create_upload_with_parts(&coord, "bucket", "key", &[(1, b"only part")]);
    let wrong_checksum = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
    let claimed_checksum = EncodedChecksumClaim::new(ChecksumAlgorithm::Sha256, wrong_checksum);

    let result = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            parts: &parts,
            claimed_checksum: Some(&claimed_checksum),
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap();

    let expected = base64::engine::general_purpose::STANDARD
        .encode(checksum::crc64::checksum(b"only part").to_be_bytes());
    assert_eq!(
        result.checksum_algorithm,
        Some(ChecksumAlgorithm::Crc64nvme)
    );
    assert_eq!(result.checksum_type, Some(ChecksumType::FullObject));
    assert_eq!(result.checksum_value, Some(expected));
}

#[test]
fn complete_multipart_unconfigured_crc64nvme_checksum_is_stored() {
    use base64::Engine;

    let b64 = base64::engine::general_purpose::STANDARD;
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let data = b"only part";
    let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, data)]);
    let expected = b64.encode(checksum::crc64::checksum(data).to_be_bytes());
    let claimed_checksum =
        EncodedChecksumClaim::new(ChecksumAlgorithm::Crc64nvme, expected.clone());

    let result = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            parts: &parts,
            claimed_checksum: Some(&claimed_checksum),
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap();

    assert_eq!(
        result.checksum_algorithm,
        Some(ChecksumAlgorithm::Crc64nvme)
    );
    assert_eq!(result.checksum_type, Some(ChecksumType::FullObject));
    assert_eq!(result.checksum_value, Some(expected));
}

#[test]
fn complete_multipart_unconfigured_crc64nvme_checksum_ignores_claimed_value() {
    use base64::Engine;

    let b64 = base64::engine::general_purpose::STANDARD;
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let data = b"only part";
    let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, data)]);
    let expected = b64.encode(checksum::crc64::checksum(data).to_be_bytes());
    let wrong_checksum = b64.encode([0u8; 8]);
    assert_ne!(wrong_checksum, expected);
    let claimed_checksum = EncodedChecksumClaim::new(ChecksumAlgorithm::Crc64nvme, wrong_checksum);

    let result = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            parts: &parts,
            claimed_checksum: Some(&claimed_checksum),
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap();

    assert_eq!(
        result.checksum_algorithm,
        Some(ChecksumAlgorithm::Crc64nvme)
    );
    assert_eq!(result.checksum_type, Some(ChecksumType::FullObject));
    assert_eq!(result.checksum_value, Some(expected));
}

#[test]
fn complete_multipart_rejects_new_object_checksum_header_without_upload_algorithm() {
    use base64::Engine;

    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, parts) =
        create_upload_with_parts(&coord, "bucket", "key", &[(1, b"only part")]);
    let checksum = base64::engine::general_purpose::STANDARD.encode([0u8; 64]);
    let claimed_checksum = EncodedChecksumClaim::new(ChecksumAlgorithm::Sha512, checksum);

    let err = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            parts: &parts,
            claimed_checksum: Some(&claimed_checksum),
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap_err();

    assert!(
        matches!(err, ServerError::InvalidRequestHostId { .. }),
        "expected InvalidRequestHostId for unconfigured SHA512 complete checksum header, got {err:?}"
    );
}

#[test]
fn complete_multipart_new_part_checksum_without_stored_checksum_is_invalid_part() {
    use base64::Engine;

    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let data = b"only part";
    let (upload_id, mut parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, data)]);
    let checksum = base64::engine::general_purpose::STANDARD
        .encode(compute_checksum(ChecksumAlgorithm::Sha512, data).bytes());
    parts[0].checksum =
        Some(ChecksumClaim::from_base64(ChecksumAlgorithm::Sha512, &checksum).unwrap());

    let err = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap_err();

    assert!(
        matches!(err, ServerError::InvalidPart { part_number: 1 }),
        "expected InvalidPart for SHA512 part checksum element without stored checksum, got {err:?}"
    );
}

#[test]
fn upload_part_accepts_new_checksum_without_upload_algorithm() {
    use base64::Engine;

    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request("bucket", "key", test_requester()),
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
    let data = b"part-with-unconfigured-sha512";
    let checksum = base64::engine::general_purpose::STANDARD
        .encode(compute_checksum(ChecksumAlgorithm::Sha512, data).bytes());
    let claim = ChecksumClaim::from_base64(ChecksumAlgorithm::Sha512, &checksum).unwrap();

    let uploaded = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request("bucket", "key", &create.upload_id, test_requester()),
            part_number: 1,
            data,
            claimed_checksum: Some(&claim),
            sse_customer: None,
        },
    )
    .unwrap();

    let stored = uploaded.checksum.expect("expected echoed part checksum");
    assert_eq!(stored.algorithm(), ChecksumAlgorithm::Sha512);
    assert_eq!(
        stored.bytes(),
        compute_checksum(ChecksumAlgorithm::Sha512, data).bytes()
    );

    let parts = [CompletePart {
        part_number: 1,
        etag: uploaded.etag,
        checksum: Some(claim),
    }];
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
    assert!(
        matches!(err, ServerError::InvalidPart { part_number: 1 }),
        "expected InvalidPart because unconfigured UploadPart checksum is echoed but not stored, got {err:?}"
    );
}

#[test]
fn complete_multipart_wrong_checksum_element_type_rejected() {
    use base64::Engine;

    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let big = vec![0xAAu8; 5 * 1024 * 1024];
    let small = b"end";
    let (upload_id, mut parts) = create_checksum_upload(
        &coord,
        "bucket",
        "key",
        ChecksumAlgorithm::Crc32,
        Some(ChecksumType::FullObject),
        &[&big, small],
    );

    // Replace the CRC32 checksum with a SHA256-tagged element (wrong algorithm).
    // Use a structurally valid SHA256 value so the request reaches the algorithm check.
    let wrong_sha256_value = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
    parts[0].checksum =
        Some(ChecksumClaim::from_base64(ChecksumAlgorithm::Sha256, &wrong_sha256_value).unwrap());

    let err = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::InvalidRequest { .. }),
        "expected InvalidRequest for wrong element type, got {err:?}"
    );
}

#[test]
fn complete_multipart_sse_c_checksum_requires_headers() {
    use base64::Engine;

    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_sse_c(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    enable_bucket_sse_c_test(&coord, "bucket", test_requester(), None).unwrap();

    let sse_customer = test_sse_customer_request();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request("bucket", "key", test_requester()),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: Some(MultipartChecksumConfig::new(ChecksumAlgorithm::Sha256, None).unwrap()),
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::sse_customer(&sse_customer),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let part_data = b"hello multipart checksum";
    let part_checksum = base64::engine::general_purpose::STANDARD
        .encode(compute_checksum(ChecksumAlgorithm::Sha256, part_data).bytes());
    let part_checksum_claim =
        ChecksumClaim::from_base64(ChecksumAlgorithm::Sha256, &part_checksum).unwrap();
    let result = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request("bucket", "key", &create.upload_id, test_requester()),
            part_number: 1,
            data: part_data,
            claimed_checksum: Some(&part_checksum_claim),
            sse_customer: Some(&sse_customer),
        },
    )
    .unwrap();
    let complete_parts = [CompletePart {
        part_number: 1,
        etag: result.etag,
        checksum: Some(part_checksum_claim),
    }];
    let object_checksum_claim =
        EncodedChecksumClaim::new(ChecksumAlgorithm::Sha256, format!("{part_checksum}-1"));

    let err = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            parts: &complete_parts,
            claimed_checksum: Some(&object_checksum_claim),
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            ServerError::ChecksumDigestMismatch { ref algorithm }
            if algorithm == "sha256"
        ),
        "expected ChecksumDigestMismatch when SSE-C checksum finalize omits headers, got {err:?}"
    );
}

#[test]
fn complete_multipart_sse_c_without_checksum_allows_missing_headers() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_sse_c(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    enable_bucket_sse_c_test(&coord, "bucket", test_requester(), None).unwrap();

    let sse_customer = test_sse_customer_request();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request("bucket", "key", test_requester()),
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

    let result = test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request("bucket", "key", &create.upload_id, test_requester()),
            part_number: 1,
            data: b"secret",
            claimed_checksum: None,
            sse_customer: Some(&sse_customer),
        },
    )
    .unwrap();
    let complete_parts = [CompletePart {
        part_number: 1,
        etag: result.etag,
        checksum: None,
    }];

    let complete = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            parts: &complete_parts,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap();

    assert_eq!(complete.checksum_algorithm, None);
    assert_eq!(complete.checksum_type, None);
    assert_eq!(complete.checksum_value, None);
}

// ── Streaming upload session tests ──────────────────────────────────

#[test]
fn stream_put_happy_path() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    // Begin session.
    let session_id = begin_stream_put_test(&coord, "bucket", "mykey").unwrap();
    assert_eq!(session_id.as_str().len(), 32);

    // Append two segments.
    let segment0 = b"hello ";
    let segment1 = b"world";
    coord
        .append_plaintext_stream_segment_for_test("bucket", "mykey", &session_id, 0, segment0)
        .unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "mykey", &session_id, 1, segment1)
        .unwrap();

    // Finalize with caller-computed CRC64 and total_size.
    let mut full_data = Vec::new();
    full_data.extend_from_slice(segment0);
    full_data.extend_from_slice(segment1);
    let crc = checksum::crc64::checksum(&full_data);
    let metadata = MetadataBlob::from_headers(&[("x-amz-meta-foo", "bar")]).unwrap();
    let write_encryption = coord
        .load_stream_put_write_encryption(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("mykey"),
            &session_id,
            None,
        )
        .unwrap();
    let result = coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "mykey", test_requester()),
            session_id: &session_id,
            crc64: crc,
            total_size: full_data.len() as u64,
            metadata_blob: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: write_encryption.as_ref(),
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap();

    assert_eq!(result.etag, format_etag(crc));
    assert_eq!(result.version_id, VersionId::Null);

    // Verify object is visible via head_object.
    let head = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "mykey",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(head.size, full_data.len() as u64);
    assert_eq!(head.etag, format_etag(crc));
    assert_eq!(head.metadata.get("x-amz-meta-foo"), Some("bar"));
    use base64::Engine;
    let expected_checksum = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());
    let checksum = head.system_metadata.checksum().unwrap();
    assert_eq!(checksum.algorithm(), ChecksumAlgorithm::Crc64nvme);
    assert_eq!(checksum.checksum_type(), Some(ChecksumType::FullObject));
    assert_eq!(checksum.value(), expected_checksum.as_str());
}

#[test]
fn sse_c_checksum_metadata_is_not_stored_in_cleartext() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator_with_sse_c(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    enable_bucket_sse_c_test(&coord, "bucket", test_requester(), None).unwrap();

    let sse_customer = test_sse_customer_request();
    let metadata = MetadataBlob::from_headers(&[("x-amz-meta-owner", "alice")]).unwrap();
    let mut system_metadata = SystemMetadata::new();
    system_metadata.set_checksum(
        ChecksumAlgorithm::Sha256,
        Some(ChecksumType::FullObject),
        "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=".to_string(),
    );

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::sse_customer(&sse_customer),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "obj", test_requester(), None),
            data: b"checksum-body",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    {
        let record = coord
            .storage_node()
            .test_get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("obj"))
            .unwrap();
        let live = record.as_live().unwrap();
        let stored_system =
            SystemMetadata::deserialize(live.system_metadata_blob.as_ref().unwrap().as_slice())
                .unwrap();
        assert!(stored_system.checksum().is_none());
        let stored_user =
            Coordinator::deserialize_user_metadata(live.metadata_blob.as_ref()).unwrap();
        assert_eq!(stored_user.get("x-amz-meta-owner"), Some("alice"));
        let ObjectEncryption::SseCustomer(state) = &live.encryption else {
            panic!("expected SSE-C encryption state");
        };
        assert!(!state.encrypted_checksum_metadata().is_empty());
    }

    let head = coord
        .head_object(&GetObjectRequest {
            sse_customer: Some(&sse_customer),
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
    let checksum = head.system_metadata.checksum().unwrap();
    assert_eq!(checksum.algorithm(), ChecksumAlgorithm::Sha256);
    assert_eq!(checksum.checksum_type(), Some(ChecksumType::FullObject));
    assert_eq!(
        checksum.value(),
        "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0="
    );

    let wrong = SseCustomerRequest::new([1u8; SSE_C_CUSTOMER_KEY_LEN], "wrong".to_string());
    let err = coord
        .head_object(&GetObjectRequest {
            sse_customer: Some(&wrong),
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn put_object_applies_default_crc64nvme_checksum() {
    use base64::Engine;

    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let body = b"hello";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "obj", test_requester(), None),
            data: body,
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
                "obj",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    let checksum = head.system_metadata.checksum().unwrap();
    let expected = base64::engine::general_purpose::STANDARD
        .encode(checksum::crc64::checksum(body).to_be_bytes());
    assert_eq!(checksum.algorithm(), ChecksumAlgorithm::Crc64nvme);
    assert_eq!(checksum.checksum_type(), Some(ChecksumType::FullObject));
    assert_eq!(checksum.value(), expected.as_str());
}

#[test]
fn list_object_versions_defaults_explicit_single_part_checksum_type_to_full_object() {
    use base64::Engine;

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

    let body = b"hi";
    let checksum = base64::engine::general_purpose::STANDARD
        .encode(checksum::crc32::checksum(body).to_be_bytes());
    let mut system_metadata = SystemMetadata::new();
    system_metadata.set_checksum(ChecksumAlgorithm::Crc32, None, checksum);

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "obj", test_requester(), None),
            data: body,
            metadata: &MetadataBlob::new(),
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let versions = coord
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

    assert_eq!(versions.versions.len(), 1);
    assert_eq!(
        versions.versions[0].checksum_algorithm,
        Some(ChecksumAlgorithm::Crc32)
    );
    assert_eq!(
        versions.versions[0].checksum_type,
        Some(ChecksumType::FullObject)
    );
}

#[test]
fn put_object_from_authorized_write_commits_authorized_acl_and_tags() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let tags_xml =
        "<Tagging><TagSet><Tag><Key>scope</Key><Value>open</Value></Tag></TagSet></Tagging>";
    let tags = object_tag_set(tags_xml);
    let authorized = coord
        .authorize_put_object_write(&AuthorizePutObjectRequest {
            object: object_request_with_expected_owner("bucket", "obj", test_requester(), None),
            acl: PutObjectAcl::PublicRead.into(),
            policy_context: PutObjectPolicyContext::default().with_request_object_tags(Some(&tags)),
            object_lock: ObjectLockState::default(),
            tags: Some(&tags),
            encryption: WriteEncryptionRequest::none(),
        })
        .unwrap();

    coord
        .put_object_from_authorized_write(
            &AuthorizedPutObjectCommitRequest {
                data: b"hello",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                cond: NO_WRITE,
            },
            &authorized,
        )
        .unwrap();

    let object = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj",
                None,
                Requester::anonymous(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(read_all_body(object.body).unwrap(), b"hello");

    let stored_tags =
        get_object_tags_test(&coord, "bucket", "obj", None, test_requester(), None).unwrap();
    let expected_tags = object_tag_set(tags_xml).to_xml();
    assert_eq!(stored_tags.as_deref(), Some(expected_tags.as_str()));
}

#[test]
fn finalize_stream_put_from_authorized_write_commits_authorized_acl_and_tags() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let tags_xml =
        "<Tagging><TagSet><Tag><Key>scope</Key><Value>stream</Value></Tag></TagSet></Tagging>";
    let tags = object_tag_set(tags_xml);
    let authorized = coord
        .authorize_put_object_write(&AuthorizePutObjectRequest {
            object: object_request_with_expected_owner("bucket", "obj", test_requester(), None),
            acl: PutObjectAcl::PublicRead.into(),
            policy_context: PutObjectPolicyContext::default().with_request_object_tags(Some(&tags)),
            object_lock: ObjectLockState::default(),
            tags: Some(&tags),
            encryption: WriteEncryptionRequest::none(),
        })
        .unwrap();
    let session_id = coord.begin_stream_put_session(&authorized).unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "obj", &session_id, 0, b"hello")
        .unwrap();

    coord
        .finalize_stream_put_from_authorized_write(
            &AuthorizedFinalizeStreamPutRequest {
                session_id: &session_id,
                crc64: checksum::crc64::checksum(b"hello"),
                total_size: 5,
                metadata_blob: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                write_encryption: ActiveWriteEncryptionRef::None,
                cond: NO_WRITE,
            },
            &authorized,
        )
        .unwrap();

    let object = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj",
                None,
                Requester::anonymous(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(read_all_body(object.body).unwrap(), b"hello");

    let stored_tags =
        get_object_tags_test(&coord, "bucket", "obj", None, test_requester(), None).unwrap();
    let expected_tags = object_tag_set(tags_xml).to_xml();
    assert_eq!(stored_tags.as_deref(), Some(expected_tags.as_str()));
}

#[test]
fn finalize_stream_put_rejects_mismatched_sse_c_write_context() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator_with_sse_c(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    enable_bucket_sse_c_test(&coord, "bucket", test_requester(), None).unwrap();

    let sse_customer = test_sse_customer_request();
    let session_id = begin_stream_put_with_authorized_request_test(
        &coord,
        object_request("bucket", "obj", test_requester()),
        NO_PUT_OBJECT_ACL.into(),
        PutObjectPolicyContext::default(),
        WriteEncryptionRequest::sse_customer(&sse_customer),
        ObjectLockState::default(),
    )
    .unwrap();
    coord
        .append_stream_put_data(&AppendStreamPutRequest {
            bucket: &trusted_bucket_name("bucket"),
            key: &trusted_object_key("obj"),
            session_id: &session_id,
            segment_index: 0,
            data: b"hello",
            sse_customer: Some(&sse_customer),
        })
        .unwrap();

    let mut system_metadata = SystemMetadata::new();
    system_metadata.set_checksum(
        ChecksumAlgorithm::Sha256,
        Some(ChecksumType::FullObject),
        "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=".to_string(),
    );
    let wrong_request =
        SseCustomerRequest::new([3u8; SSE_C_CUSTOMER_KEY_LEN], "wrong-md5".to_string());
    let wrong_context = coord
        .prepare_sse_customer_write_context(Some(&wrong_request))
        .unwrap()
        .expect("expected SSE-C write context");
    let err = coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "obj", test_requester()),
            session_id: &session_id,
            crc64: checksum::crc64::checksum(b"hello"),
            total_size: 5,
            metadata_blob: &MetadataBlob::new(),
            system_metadata: &system_metadata,
            write_encryption: ActiveWriteEncryptionRef::SseCustomer(&wrong_context),
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::InvalidRequest { .. }));
}

#[test]
fn finalize_stream_put_rejects_mismatched_sse_s3_write_context() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "obj").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "obj", &session_id, 0, b"hello")
        .unwrap();

    let mut system_metadata = SystemMetadata::new();
    system_metadata.set_checksum(
        ChecksumAlgorithm::Sha256,
        Some(ChecksumType::FullObject),
        "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=".to_string(),
    );
    let wrong_context = coord.prepare_managed_write_context().unwrap();
    let err = coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "obj", test_requester()),
            session_id: &session_id,
            crc64: checksum::crc64::checksum(b"hello"),
            total_size: 5,
            metadata_blob: &MetadataBlob::new(),
            system_metadata: &system_metadata,
            write_encryption: ActiveWriteEncryptionRef::Managed {
                algorithm: ManagedEncryptionAlgorithm::Aes256,
                write: &wrong_context,
            },
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::InvalidRequest { .. }));
}

#[test]
fn finalize_stream_put_does_not_reauthorize_after_stream_start() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("owner-a", "bucket", false)
        .unwrap();
    put_bucket_canned_acl_test(
        &coord,
        "bucket",
        BucketAcl::PublicReadWrite,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    let writer = test_helpers::requester("writer-a");
    let session_id = begin_stream_put_with_authorized_request_test(
        &coord,
        object_request_with_expected_owner("bucket", "obj", writer.clone(), None),
        NO_PUT_OBJECT_ACL.into(),
        PutObjectPolicyContext::default(),
        WriteEncryptionRequest::none(),
        ObjectLockState::default(),
    )
    .unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "obj", &session_id, 0, b"hello")
        .unwrap();

    put_bucket_canned_acl_test(
        &coord,
        "bucket",
        BucketAcl::Private,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    let result = coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request_with_expected_owner("bucket", "obj", writer, None),
            session_id: &session_id,
            crc64: checksum::crc64::checksum(b"hello"),
            total_size: 5,
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
    assert_eq!(
        result.etag,
        format_etag(checksum::crc64::checksum(b"hello"))
    );
}

#[test]
fn sse_c_multipart_parts_with_same_plaintext_use_distinct_nonce_scopes() {
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

    for part_number in [1u32, 2u32] {
        test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                upload: multipart_object_request_with_expected_owner(
                    "bucket",
                    "key",
                    &upload.upload_id,
                    test_requester(),
                    None,
                ),
                part_number,
                data: b"identical-multipart-segment",
                claimed_checksum: None,

                sse_customer: Some(&sse_customer),
            },
        )
        .unwrap();
    }

    let segments = coord
        .storage_node()
        .test_get_all_multipart_part_segments_for_upload(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &upload.upload_id,
        )
        .unwrap();
    let part1: Vec<_> = segments
        .iter()
        .filter(|segment| segment.part_number == 1)
        .collect();
    let part2: Vec<_> = segments
        .iter()
        .filter(|segment| segment.part_number == 2)
        .collect();

    assert_eq!(part1.len(), 1);
    assert_eq!(part2.len(), 1);
    assert_eq!(part1[0].segment_index, 0);
    assert_eq!(part2[0].segment_index, 0);
    assert_ne!(part1[0].segment_crc64, part2[0].segment_crc64);
}

#[test]
fn stream_put_zero_byte_object() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "mykey").unwrap();

    // Finalize with no segments appended — zero-byte object.
    let crc = checksum::crc64::checksum(&[]);
    let metadata = MetadataBlob::new();
    let result = coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "mykey", test_requester()),
            session_id: &session_id,
            crc64: crc,
            total_size: 0,
            metadata_blob: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: ActiveWriteEncryptionRef::None,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap();

    assert_eq!(result.etag, format_etag(crc));

    let head = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "mykey",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(head.size, 0);
}

#[test]
fn finalize_stream_put_persists_tags_in_initial_commit() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "mykey").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "mykey", &session_id, 0, b"hello")
        .unwrap();

    let tags_xml =
        "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
    let tags = object_tag_set(tags_xml);
    let result = coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "mykey", test_requester()),
            session_id: &session_id,
            crc64: checksum::crc64::checksum(b"hello"),
            total_size: 5,
            metadata_blob: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: ActiveWriteEncryptionRef::None,
            tags: Some(&tags),
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap();
    assert_eq!(result.version_id, VersionId::Null);

    let tags =
        get_object_tags_test(&coord, "bucket", "mykey", None, test_requester(), None).unwrap();
    let expected_tags = object_tag_set(tags_xml).to_xml();
    assert_eq!(tags.as_deref(), Some(expected_tags.as_str()));
}

#[test]
fn stream_put_abort() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "mykey").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "mykey", &session_id, 0, b"data")
        .unwrap();

    // Abort the session.
    coord
        .abort_stream_put("bucket", "mykey", &session_id)
        .unwrap();

    // Object should not exist.
    let err = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "mykey",
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
fn stream_put_append_after_finalize_fails() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "mykey").unwrap();
    let crc = checksum::crc64::checksum(&[]);
    let metadata = MetadataBlob::new();
    coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "mykey", test_requester()),
            session_id: &session_id,
            crc64: crc,
            total_size: 0,
            metadata_blob: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: ActiveWriteEncryptionRef::None,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap();

    // Session is deleted after finalize — append should fail.
    let err = coord
        .append_plaintext_stream_segment_for_test("bucket", "mykey", &session_id, 0, b"data")
        .unwrap_err();
    assert!(
        matches!(
            err,
            ServerError::Metadata(storage::MetadataError::StreamSessionNotFound { .. })
        ),
        "expected StreamSessionNotFound, got {err:?}"
    );
}

#[test]
fn stream_put_finalize_after_abort_fails() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "mykey").unwrap();
    coord
        .abort_stream_put("bucket", "mykey", &session_id)
        .unwrap();

    let crc = checksum::crc64::checksum(&[]);
    let metadata = MetadataBlob::new();
    let err = coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "mykey", test_requester()),
            session_id: &session_id,
            crc64: crc,
            total_size: 0,
            metadata_blob: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: ActiveWriteEncryptionRef::None,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            ServerError::Metadata(storage::MetadataError::StreamSessionNotFound { .. })
        ),
        "expected StreamSessionNotFound, got {err:?}"
    );
}

#[test]
fn stream_put_bucket_key_mismatch_append() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key1").unwrap();

    // Attempt append with wrong key.
    let err = coord
        .append_plaintext_stream_segment_for_test("bucket", "key2", &session_id, 0, b"data")
        .unwrap_err();
    // The session lives on key1's metadata PG. If key2 maps to a different PG,
    // the session won't be found. If same PG, the bucket/key check catches it.
    assert!(
        matches!(
            err,
            ServerError::InvalidRequest { .. }
                | ServerError::Metadata(storage::MetadataError::StreamSessionNotFound { .. })
        ),
        "expected mismatch error, got {err:?}"
    );
}

#[test]
fn stream_put_bucket_key_mismatch_finalize() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key1").unwrap();

    let crc = checksum::crc64::checksum(&[]);
    let metadata = MetadataBlob::new();
    let err = coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key2", test_requester()),
            session_id: &session_id,
            crc64: crc,
            total_size: 0,
            metadata_blob: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: ActiveWriteEncryptionRef::None,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            ServerError::InvalidRequest { .. }
                | ServerError::Metadata(storage::MetadataError::StreamSessionNotFound { .. })
        ),
        "expected mismatch error, got {err:?}"
    );
}

#[test]
fn stream_put_nonexistent_bucket() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());

    let err = begin_stream_put_test(&coord, "nonexistent", "key").unwrap_err();
    assert!(matches!(err, ServerError::BucketNotFound { .. }));
}

#[test]
fn stream_put_overwrite_existing_object() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    // Write an existing object via normal put.
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"old-data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: &WriteCondition::default(),

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // Stream-put a new version.
    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    let new_data = b"new-streamed-data";
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, new_data)
        .unwrap();

    let crc = checksum::crc64::checksum(new_data.as_slice());
    let metadata = MetadataBlob::new();
    let result = coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id,
            crc64: crc,
            total_size: new_data.len() as u64,
            metadata_blob: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: ActiveWriteEncryptionRef::None,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap();
    assert_eq!(result.etag, format_etag(crc));

    // Head should show the new object.
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
    assert_eq!(head.size, new_data.len() as u64);
}

#[test]
fn stream_put_with_write_condition() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    // Write initial object.
    let initial = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"initial",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: &WriteCondition::default(),

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // Stream put with if-match on the correct etag succeeds.
    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"updated")
        .unwrap();
    let crc = checksum::crc64::checksum(b"updated");
    let metadata = MetadataBlob::new();
    let cond = WriteCondition::IfMatch(SpecificEtag::new(initial.etag.clone()).unwrap());
    coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id,
            crc64: crc,
            total_size: 7,
            metadata_blob: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: ActiveWriteEncryptionRef::None,
            tags: None,
            cond: &cond,
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap();

    // Stream put with if-match on a wrong etag fails.
    let session_id2 = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id2, 0, b"third")
        .unwrap();
    let bad_cond =
        WriteCondition::IfMatch(SpecificEtag::new("\"0000000000000000\"".to_string()).unwrap());
    let err = coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id2,
            crc64: checksum::crc64::checksum(b"third"),
            total_size: 5,
            metadata_blob: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: ActiveWriteEncryptionRef::None,
            tags: None,
            cond: &bad_cond,
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::PreconditionFailed { .. }),
        "expected PreconditionFailed, got {err:?}"
    );
}

#[test]
fn stream_put_multiple_segments_correct_manifest() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();

    // Append 3 segments.
    let segments: Vec<&[u8]> = vec![b"aaa", b"bbb", b"ccc"];
    for (i, segment) in segments.iter().enumerate() {
        coord
            .append_plaintext_stream_segment_for_test(
                "bucket",
                "key",
                &session_id,
                i as u32,
                segment,
            )
            .unwrap();
    }

    let mut full_data = Vec::new();
    for segment in &segments {
        full_data.extend_from_slice(segment);
    }
    let crc = checksum::crc64::checksum(&full_data);
    let metadata = MetadataBlob::new();
    let result = coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id,
            crc64: crc,
            total_size: full_data.len() as u64,
            metadata_blob: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: ActiveWriteEncryptionRef::None,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap();
    assert_eq!(result.etag, format_etag(crc));

    // Verify the committed object segments exist in the metadata PG.
    let committed = coord
        .storage_node()
        .test_get_object_segments(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            result.version_id,
        )
        .unwrap();
    assert_eq!(committed.len(), 3);
    let live = coord
        .storage_node()
        .test_get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
        .unwrap()
        .as_live()
        .expect("stream put should create a live object")
        .clone();
    let topology = storage::PgTopology::new(coord.storage_node().test_pg_ids()).unwrap();
    for (i, segment) in committed.iter().enumerate() {
        let segment_index = i as u32;
        assert_eq!(segment.segment_index, segment_index);
        assert_eq!(segment.size, 3); // "aaa", "bbb", "ccc" are all 3 bytes
        assert_eq!(
            segment.segment_okh,
            storage::segment_key_hash("bucket", "key", live.generation_id, segment_index)
        );
        assert_eq!(
            segment.data_pg_id,
            topology
                .object_generation_segment_data_pg(
                    &trusted_bucket_name("bucket"),
                    &trusted_object_key("key"),
                    live.generation_id,
                    segment_index,
                )
                .get()
        );
    }
}

#[test]
fn stream_put_abort_cleans_up_shards() {
    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap();
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"data-to-clean")
        .unwrap();

    // Record shard keys before abort for verification.
    let segments = coord
        .storage_node()
        .test_list_stream_segments(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &session_id,
        )
        .unwrap();
    assert_eq!(segments.len(), 1);
    let segment = segments[0].clone();
    let data_pg_id = segment.data_pg_id;
    let ec = EcShape {
        k: segment.ec_k,
        m: segment.ec_m,
    };
    for i in 0..ec.k + ec.m {
        assert!(
            coord
                .storage_node()
                .test_payload_shard_file_exists(
                    data_pg_id,
                    ec,
                    &segment.segment_okh,
                    segment.segment_vid,
                    i,
                )
                .unwrap(),
            "placed shard {i} should exist before abort"
        );
    }

    coord
        .abort_stream_put("bucket", "key", &session_id)
        .unwrap();

    // Verify shards were cleaned up.
    for i in 0..ec.k + ec.m {
        let shard_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i);
        assert!(
            !coord
                .storage_node()
                .test_shard_exists(data_pg_id, &shard_key)
                .unwrap(),
            "shard {i} should have been deleted"
        );
        assert!(
            !coord
                .storage_node()
                .test_payload_shard_file_exists(
                    data_pg_id,
                    ec,
                    &segment.segment_okh,
                    segment.segment_vid,
                    i,
                )
                .unwrap(),
            "placed shard {i} should have been deleted"
        );
    }
}

#[test]
fn stream_put_overwrite_with_normal_put_cleans_segments() {
    // P0 fix: normal PUT after stream-write must clear stale segment rows.
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    // Stream-write an object.
    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"stream-data")
        .unwrap();
    let crc = checksum::crc64::checksum(b"stream-data");
    coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id,
            crc64: crc,
            total_size: 11,
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

    // Verify stream-put is readable.
    let r1 = coord
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
    assert_eq!(r1.body.read_all().unwrap(), b"stream-data");

    // Overwrite with a normal PUT.
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"normal-data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: &WriteCondition::default(),

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // GET should return the new data, not stale segment data.
    let r2 = coord
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
    assert_eq!(r2.body.read_all().unwrap(), b"normal-data");
}

#[test]
fn stream_put_delete_cleans_segments() {
    // P1 fix: delete must clean up object_segments and their shards.
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"delete-me")
        .unwrap();
    let crc = checksum::crc64::checksum(b"delete-me");
    coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id,
            crc64: crc,
            total_size: 9,
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

    // Delete the object.
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
            cond: &crate::conditional::DeleteCondition::default(),
        })
        .unwrap();

    // Object should be gone.
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
fn stream_put_delete_eventually_reclaims_segment_shards() {
    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap();
    let dir = test_util::tempdir();
    let coord = setup_coordinator_without_reclaim_sweeper(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"delete-me")
        .unwrap();
    let crc = checksum::crc64::checksum(b"delete-me");
    let result = coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id,
            crc64: crc,
            total_size: 9,
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

    let (generation_id, segments) = {
        let generation_id = match coord
            .storage_node()
            .test_get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
            .unwrap()
        {
            StoredObject::Live(record) => record.generation_id,
            other @ StoredObject::DeleteMarker(_) => {
                panic!("expected live streamed object, got {other:?}")
            }
        };
        let segments = coord
            .storage_node()
            .test_get_object_segments(
                &trusted_bucket_name("bucket"),
                &trusted_object_key("key"),
                result.version_id,
            )
            .unwrap();
        (generation_id, segments)
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
    for segment in segments {
        assert_shard_set_deleted(
            &coord,
            segment.data_pg_id,
            &segment.segment_okh,
            segment.segment_vid,
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
        );
    }
}

#[test]
fn stream_put_upload_part_copy_from_stream_source() {
    // P2 fix: UploadPartCopy must be able to read stream-written source objects.
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    // Stream-write a source object.
    let session_id = begin_stream_put_test(&coord, "bucket", "src").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "src", &session_id, 0, b"source-data")
        .unwrap();
    let crc = checksum::crc64::checksum(b"source-data");
    coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "src", test_requester()),
            session_id: &session_id,
            crc64: crc,
            total_size: 11,
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

    // Create a multipart upload for the destination.
    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "dst", test_requester(), None),
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

    // UploadPartCopy from the stream-written source.
    let result = coord
        .upload_part_copy(&UploadPartCopyRequest {
            source: copy_source("bucket", "src", None),
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        .unwrap();
    assert!(!result.etag.is_empty());
}

#[test]
fn upload_part_copy_streams_multisegment_source_and_persists_checksum() {
    use base64::Engine;

    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let data: Vec<u8> = (0..((2 * INTERNAL_SEGMENT_SIZE) + 12_345))
        .map(|i| (i % 251) as u8)
        .collect();

    let session_id = begin_stream_put_test(&coord, "bucket", "src").unwrap();
    let mut crc64 = checksum::crc64::Hasher::new();
    for (idx, chunk) in data.chunks(INTERNAL_SEGMENT_SIZE).enumerate() {
        coord
            .append_plaintext_stream_segment_for_test(
                "bucket",
                "src",
                &session_id,
                idx as u32,
                chunk,
            )
            .unwrap();
        crc64.update(chunk);
    }
    coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "src", test_requester()),
            session_id: &session_id,
            crc64: crc64.finalize(),
            total_size: data.len() as u64,
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

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "dst", test_requester(), None),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: Some(MultipartChecksumConfig::new(ChecksumAlgorithm::Crc32c, None).unwrap()),

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let part = coord
        .upload_part_copy(&UploadPartCopyRequest {
            source: copy_source("bucket", "src", None),
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        .unwrap();

    let parts = coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "dst",
                &upload.upload_id,
                test_requester(),
                None,
            ),
            part_number_marker: None,
            max_parts: 1000,
        })
        .unwrap();
    assert_eq!(parts.checksum_algorithm, Some(ChecksumAlgorithm::Crc32c));
    assert_eq!(parts.parts.len(), 1);
    let expected_checksum = base64::engine::general_purpose::STANDARD
        .encode(checksum::crc32c::checksum(&data).to_be_bytes());
    assert_eq!(
        parts.parts[0].checksum.as_deref(),
        Some(expected_checksum.as_str())
    );

    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "dst",
                &upload.upload_id,
                test_requester(),
                None,
            ),
            parts: &[CompletePart {
                part_number: 1,
                etag: part.etag,
                checksum: Some(
                    ChecksumClaim::from_base64(
                        ChecksumAlgorithm::Crc32c,
                        parts.parts[0].checksum.as_deref().unwrap(),
                    )
                    .unwrap(),
                ),
            }],
            sse_customer: None,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
        })
        .unwrap();

    let copied = coord
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
    assert_eq!(copied.body.read_all().unwrap(), data);
}

#[test]
fn upload_part_copy_source_read_failure_aborts_destination_stream_session() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let data: Vec<u8> = (0..(INTERNAL_SEGMENT_SIZE + 1024))
        .map(|i| (i % 251) as u8)
        .collect();
    let session_id = begin_stream_put_test(&coord, "bucket", "src").unwrap();
    let mut crc64 = checksum::crc64::Hasher::new();
    for (idx, chunk) in data.chunks(INTERNAL_SEGMENT_SIZE).enumerate() {
        coord
            .append_plaintext_stream_segment_for_test(
                "bucket",
                "src",
                &session_id,
                idx as u32,
                chunk,
            )
            .unwrap();
        crc64.update(chunk);
    }
    coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "src", test_requester()),
            session_id: &session_id,
            crc64: crc64.finalize(),
            total_size: data.len() as u64,
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

    let source_segments = coord
        .storage_node()
        .test_get_object_segments(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("src"),
            VersionId::Null,
        )
        .unwrap();
    assert_eq!(source_segments.len(), 2);
    let broken_segment = &source_segments[1];
    let broken_ec = EcShape {
        k: broken_segment.ec_k,
        m: broken_segment.ec_m,
    };
    for shard_index in 0..(broken_segment.ec_k + broken_segment.ec_m) {
        let shard_path = coord
            .storage_node()
            .test_payload_shard_file_path(
                broken_segment.data_pg_id,
                broken_ec,
                &broken_segment.segment_okh,
                broken_segment.segment_vid,
                shard_index,
            )
            .unwrap();
        std::fs::remove_file(&shard_path).unwrap_or_else(|error| {
            panic!(
                "failed to delete source shard {shard_index} at {}: {error}",
                shard_path.display()
            )
        });
    }

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "dst", test_requester(), None),
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
        .upload_part_copy(&UploadPartCopyRequest {
            source: copy_source("bucket", "src", None),
            upload: multipart_object_request_with_expected_owner(
                "bucket",
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
        .unwrap_err();
    assert!(
        matches!(
            err,
            ServerError::ObjectNotFound { .. } | ServerError::Store(_)
        ),
        "expected source read failure, got {err:?}"
    );
    assert_eq!(
        upload_part_stream_session_count(&coord, "bucket", "dst", &upload.upload_id, 1),
        0,
        "failed UploadPartCopy must abort the destination UploadPart stream session"
    );
    let parts = coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "dst",
                &upload.upload_id,
                test_requester(),
                None,
            ),
            part_number_marker: None,
            max_parts: 1000,
        })
        .unwrap();
    assert!(parts.parts.is_empty());
}

#[test]
fn upload_part_copy_invalid_part_number_exceeds_max() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
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
            data: b"source-data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "dst", test_requester(), None),
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
        .upload_part_copy(&UploadPartCopyRequest {
            source: copy_source("bucket", "src", None),
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "dst",
                &upload.upload_id,
                test_requester(),
                None,
            ),
            part_number: 10_001,
            copy_source_range: None,
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::InvalidArgument { .. }));
}

#[test]
fn upload_part_copy_rejects_non_owner_requester() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
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
            data: b"source-data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "dst", test_requester(), None),
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
        .upload_part_copy(&UploadPartCopyRequest {
            source: copy_source("bucket", "src", None),
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "dst",
                &upload.upload_id,
                test_helpers::requester("other-user"),
                None,
            ),
            part_number: 1,
            copy_source_range: None,

            policy_context: PutObjectPolicyContext::default(),

            source_sse_customer: None,
            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn authorize_upload_part_copy_rejects_non_owner_requester() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
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
            data: b"source-data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "dst", test_requester(), None),
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
        .authorize_upload_part_copy(&UploadPartCopyRequest {
            source: copy_source("bucket", "src", None),
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "dst",
                &upload.upload_id,
                test_helpers::requester("other-user"),
                None,
            ),
            part_number: 1,
            copy_source_range: None,

            policy_context: PutObjectPolicyContext::default(),

            source_sse_customer: None,
            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn stream_put_duplicate_segment_index_rejected() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"first")
        .unwrap();

    // Appending the same segment_index again should be rejected.
    let err = coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"second")
        .unwrap_err();
    assert!(
        matches!(err, ServerError::InvalidRequest { .. }),
        "expected InvalidRequest for duplicate segment_index, got {err:?}"
    );

    // Original segment should still be intact — verify by finalizing.
    let crc = checksum::crc64::checksum(b"first");
    let metadata = MetadataBlob::new();
    coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id,
            crc64: crc,
            total_size: 5,
            metadata_blob: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: ActiveWriteEncryptionRef::None,
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap();
}

#[test]
fn stream_append_accepts_upload_part_session() {
    // append_stream_segment accepts both PutObject and valid UploadPart sessions.
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
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
    let session = begin_stream_part_test(&coord, "bucket", "key", &create.upload_id, 1).unwrap();

    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session.session_id, 0, b"data")
        .unwrap();
    let segments = coord
        .storage_node()
        .test_list_stream_segments(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &session.session_id,
        )
        .unwrap();
    assert_eq!(segments.len(), 1);
    assert_payload_shard_files_state(
        &coord,
        segments[0].data_pg_id,
        EcShape {
            k: segments[0].ec_k,
            m: segments[0].ec_m,
        },
        &segments[0].segment_okh,
        segments[0].segment_vid,
        true,
        "streamed UploadPart append",
    );
}

#[test]
fn stream_append_reuses_encode_scratch_for_aligned_segments() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    let ec = coord.storage_node().default_payload_ec_shape();
    assert_eq!(coord.storage_node().test_ec_scratch_allocation_count(ec), 0);

    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"data")
        .unwrap();
    assert_eq!(coord.storage_node().test_ec_scratch_allocation_count(ec), 1);

    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 1, b"more")
        .unwrap();
    assert_eq!(coord.storage_node().test_ec_scratch_allocation_count(ec), 1);
}

// ── Phase 3a: Streaming UploadPart tests ─────────────────────────

#[test]
fn stream_part_happy_path() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    // Create a multipart upload first.
    let mpu = coord
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

    // Begin a streaming part session.
    let session = begin_stream_part_test(&coord, "bucket", "key", &mpu.upload_id, 1).unwrap();
    let session_id = session.session_id;

    // Append segments.
    let data = b"hello streaming part";
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, data)
        .unwrap();

    // Finalize.
    let crc = checksum::crc64::checksum(data);
    let result = coord
        .finalize_stream_part(FinalizeStreamPartRequest {
            upload: multipart_object_request("bucket", "key", &mpu.upload_id, test_requester()),
            session_id: &session_id,
            part_number: 1,
            crc64: crc,
            total_size: data.len() as u64,
            claimed_checksum: None,
            computed_checksum: None,
        })
        .unwrap();
    assert!(!result.etag.is_empty());
}

#[test]
fn complete_multipart_upload_omits_streamed_part_cleanup() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let mpu = coord
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

    let session1 = begin_stream_part_test(&coord, "bucket", "key", &mpu.upload_id, 1)
        .unwrap()
        .session_id;
    let session2 = begin_stream_part_test(&coord, "bucket", "key", &mpu.upload_id, 2)
        .unwrap()
        .session_id;
    let data1 = b"selected streamed part";
    let data2 = b"omitted streamed part";
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session1, 0, data1)
        .unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session2, 0, data2)
        .unwrap();

    let part1 = coord
        .finalize_stream_part(FinalizeStreamPartRequest {
            upload: multipart_object_request("bucket", "key", &mpu.upload_id, test_requester()),
            session_id: &session1,
            part_number: 1,
            crc64: checksum::crc64::checksum(data1),
            total_size: data1.len() as u64,
            claimed_checksum: None,
            computed_checksum: None,
        })
        .unwrap();
    coord
        .finalize_stream_part(FinalizeStreamPartRequest {
            upload: multipart_object_request("bucket", "key", &mpu.upload_id, test_requester()),
            session_id: &session2,
            part_number: 2,
            crc64: checksum::crc64::checksum(data2),
            total_size: data2.len() as u64,
            claimed_checksum: None,
            computed_checksum: None,
        })
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let before_segments = coord
        .storage_node()
        .test_get_all_multipart_part_segments_for_upload(&bucket, &key, &mpu.upload_id)
        .unwrap();
    let part2_segments: Vec<_> = before_segments
        .iter()
        .filter(|segment| segment.part_number == 2)
        .cloned()
        .collect();
    assert_eq!(part2_segments.len(), 1);
    let part2_shards: Vec<_> = part2_segments
        .iter()
        .flat_map(|segment| {
            (0..(segment.ec_k + segment.ec_m)).map(|shard_index| {
                (
                    segment.data_pg_id,
                    ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), shard_index),
                )
            })
        })
        .collect();
    for segment in &part2_segments {
        assert_payload_shard_files_state(
            &coord,
            segment.data_pg_id,
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
            &segment.segment_okh,
            segment.segment_vid,
            true,
            "omitted streamed part before complete",
        );
    }
    for (pg_id, shard_key) in &part2_shards {
        assert!(
            coord
                .storage_node()
                .test_shard_exists(*pg_id, shard_key)
                .unwrap(),
            "omitted part shard should exist before complete"
        );
    }

    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &mpu.upload_id,
                test_requester(),
                None,
            ),
            parts: &[CompletePart {
                part_number: 1,
                etag: part1.etag.clone(),
                checksum: None,
            }],
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap();

    let after_segments = coord
        .storage_node()
        .test_get_all_multipart_part_segments_for_upload(&bucket, &key, &mpu.upload_id)
        .unwrap();
    assert!(
        after_segments
            .iter()
            .all(|segment| segment.part_number != 2),
        "omitted part segment rows must be deleted"
    );
    for (pg_id, shard_key) in &part2_shards {
        assert!(
            !coord
                .storage_node()
                .test_shard_exists(*pg_id, shard_key)
                .unwrap(),
            "omitted part shard should be deleted after complete"
        );
    }
    for segment in &part2_segments {
        assert_payload_shard_files_state(
            &coord,
            segment.data_pg_id,
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
            &segment.segment_okh,
            segment.segment_vid,
            false,
            "omitted streamed part after complete",
        );
    }

    let object = coord
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
    assert_eq!(object.body.read_all().unwrap(), data1);
}

#[test]
fn streamed_upload_part_same_part_last_finisher_wins() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let mpu = coord
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

    let session_a = begin_stream_part_test(&coord, "bucket", "key", &mpu.upload_id, 1)
        .unwrap()
        .session_id;
    let session_b = begin_stream_part_test(&coord, "bucket", "key", &mpu.upload_id, 1)
        .unwrap()
        .session_id;

    let data_a = b"request-a-finishes-last";
    let data_b = b"request-b-finishes-first";
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_a, 0, data_a)
        .unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_b, 0, data_b)
        .unwrap();

    let result_b = coord
        .finalize_stream_part(FinalizeStreamPartRequest {
            upload: multipart_object_request("bucket", "key", &mpu.upload_id, test_requester()),
            session_id: &session_b,
            part_number: 1,
            crc64: checksum::crc64::checksum(data_b),
            total_size: data_b.len() as u64,
            claimed_checksum: None,
            computed_checksum: None,
        })
        .unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let first_segments: Vec<_> = coord
        .storage_node()
        .test_get_all_multipart_part_segments_for_upload(&bucket, &key, &mpu.upload_id)
        .unwrap()
        .into_iter()
        .filter(|segment| segment.part_number == 1)
        .collect();
    assert_eq!(first_segments.len(), 1);
    for segment in &first_segments {
        assert_payload_shard_files_state(
            &coord,
            segment.data_pg_id,
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
            &segment.segment_okh,
            segment.segment_vid,
            true,
            "first streamed UploadPart version",
        );
    }
    let result_a = coord
        .finalize_stream_part(FinalizeStreamPartRequest {
            upload: multipart_object_request("bucket", "key", &mpu.upload_id, test_requester()),
            session_id: &session_a,
            part_number: 1,
            crc64: checksum::crc64::checksum(data_a),
            total_size: data_a.len() as u64,
            claimed_checksum: None,
            computed_checksum: None,
        })
        .unwrap();
    assert_ne!(result_a.etag, result_b.etag);
    for segment in &first_segments {
        assert_payload_shard_files_state(
            &coord,
            segment.data_pg_id,
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
            &segment.segment_okh,
            segment.segment_vid,
            false,
            "displaced streamed UploadPart version",
        );
    }
    let final_segments: Vec<_> = coord
        .storage_node()
        .test_get_all_multipart_part_segments_for_upload(&bucket, &key, &mpu.upload_id)
        .unwrap()
        .into_iter()
        .filter(|segment| segment.part_number == 1)
        .collect();
    assert_eq!(final_segments.len(), 1);
    for segment in &final_segments {
        assert_payload_shard_files_state(
            &coord,
            segment.data_pg_id,
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
            &segment.segment_okh,
            segment.segment_vid,
            true,
            "winning streamed UploadPart version",
        );
    }

    let parts = coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &mpu.upload_id,
                test_requester(),
                None,
            ),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap();
    assert_eq!(parts.parts.len(), 1);
    assert_eq!(parts.parts[0].etag, result_a.etag);

    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &mpu.upload_id,
                test_requester(),
                None,
            ),
            parts: &[CompletePart {
                part_number: 1,
                etag: result_a.etag.clone(),
                checksum: None,
            }],
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap();

    let object = coord
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
    assert_eq!(object.body.read_all().unwrap(), data_a);
}

#[test]
fn stream_part_no_upload_rejected() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let err = begin_stream_part_test(&coord, "bucket", "key", "nonexistent", 1).unwrap_err();
    assert!(matches!(err, ServerError::NoSuchUpload { .. }));
}

#[test]
fn stream_part_invalid_part_number_rejected() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let mpu = coord
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

    // Part 0 is invalid.
    let err = begin_stream_part_test(&coord, "bucket", "key", &mpu.upload_id, 0).unwrap_err();
    assert!(matches!(err, ServerError::InvalidArgument { .. }));

    // Part 10001 is invalid.
    let err = begin_stream_part_test(&coord, "bucket", "key", &mpu.upload_id, 10_001).unwrap_err();
    assert!(matches!(err, ServerError::InvalidArgument { .. }));
}

#[test]
fn checksum_claim_invalid_base64_rejected() {
    // P2: Malformed base64 in claimed checksum must return an error,
    // not silently accept a None checksum.
    let err =
        ChecksumClaim::from_base64(ChecksumAlgorithm::Crc32, "not-valid-base64!!!").unwrap_err();
    assert!(
        matches!(err, ServerError::InvalidRequest { .. }),
        "expected InvalidRequest for bad base64, got {err:?}"
    );
}

#[test]
fn checksum_claim_wrong_length_rejected() {
    // A valid base64 string with the wrong byte length for the algorithm.
    use base64::Engine;
    let too_long = base64::engine::general_purpose::STANDARD.encode([0u8; 8]); // CRC32 expects 4
    let err = ChecksumClaim::from_base64(ChecksumAlgorithm::Crc32, &too_long).unwrap_err();
    assert!(
        matches!(err, ServerError::InvalidRequest { .. }),
        "expected InvalidRequest for wrong length, got {err:?}"
    );
}

#[test]
fn finalize_stream_part_wrong_op_kind_rejected() {
    // A PutObject session cannot be finalized as UploadPart.
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"data")
        .unwrap();

    let mpu = coord
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

    let err = coord
        .finalize_stream_part(FinalizeStreamPartRequest {
            upload: multipart_object_request("bucket", "key", &mpu.upload_id, test_requester()),
            session_id: &session_id,
            part_number: 1,
            crc64: checksum::crc64::checksum(b"data"),
            total_size: 4,
            claimed_checksum: None,
            computed_checksum: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::InvalidRequest { .. }));
}

#[test]
fn finalize_stream_part_session_upload_mismatch_beats_missing_requested_upload() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let mpu = coord
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

    let session_id = begin_stream_part_test(&coord, "bucket", "key", &mpu.upload_id, 1)
        .unwrap()
        .session_id;
    let nonexistent_upload_id = trusted_upload_id("nonexistent-upload");

    let err = coord
        .finalize_stream_part(FinalizeStreamPartRequest {
            upload: multipart_object_request(
                "bucket",
                "key",
                &nonexistent_upload_id,
                test_requester(),
            ),
            session_id: &session_id,
            part_number: 1,
            crc64: checksum::crc64::checksum(&[]),
            total_size: 0,
            claimed_checksum: None,
            computed_checksum: None,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::InvalidRequest { ref reason } if reason == "session upload_id/part_number mismatch"),
        "expected session mismatch InvalidRequest, got {err:?}"
    );

    let parts = coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &mpu.upload_id,
                test_requester(),
                None,
            ),
            part_number_marker: None,
            max_parts: 100,
        })
        .unwrap();
    assert!(
        parts.parts.is_empty(),
        "mismatched finalize must not commit a part"
    );
}

#[test]
fn concurrent_streamed_mpu_isolation_on_unversioned_key() {
    // Regression: two streamed MPUs on the same unversioned key must not
    // corrupt each other's segment data. Upload A completes first; upload B
    // completes second (overwriting A). Each must read back its own data.
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();

    // Create two MPUs for the same key.
    let mpu_a = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    let mpu_b = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    // Helper: stream a single part with given data.
    let stream_part = |upload_id: &UploadId, data: &[u8]| -> CompletePart {
        let sess = begin_stream_part_test(&coord, "bucket", "key", upload_id, 1)
            .unwrap()
            .session_id;
        coord
            .append_plaintext_stream_segment_for_test("bucket", "key", &sess, 0, data)
            .unwrap();
        let crc = checksum::crc64::checksum(data);
        let result = coord
            .finalize_stream_part(FinalizeStreamPartRequest {
                upload: multipart_object_request("bucket", "key", upload_id, test_requester()),
                session_id: &sess,
                part_number: 1,
                crc64: crc,
                total_size: data.len() as u64,
                claimed_checksum: None,
                computed_checksum: None,
            })
            .unwrap();
        CompletePart {
            part_number: 1,
            etag: result.etag,
            checksum: None,
        }
    };

    let data_a = b"AAAA-data-for-upload-A";
    let data_b = b"BBBB-data-for-upload-B";

    // Both uploads stage their parts concurrently (interleaved).
    let part_a = stream_part(&mpu_a.upload_id, data_a);
    let part_b = stream_part(&mpu_b.upload_id, data_b);

    // Complete A first.
    let result_a = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &mpu_a.upload_id,
                test_requester(),
                None,
            ),
            parts: &[part_a],
            claimed_checksum: None,

            expected_object_size: None,

            cond: &WriteCondition::default(),

            sse_customer: None,
        })
        .unwrap();

    // Read back A's data — should be A's content.
    let obj_a = coord
        .get_object_part(&GetObjectPartRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            part_number: 1,
            cond: &ReadCondition::default(),
        })
        .unwrap();
    assert_eq!(
        obj_a.body.read_all().unwrap(),
        data_a,
        "after completing A, reading part 1 should return A's data"
    );

    // Complete B — overwrites A on unversioned bucket.
    let result_b = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &mpu_b.upload_id,
                test_requester(),
                None,
            ),
            parts: &[part_b],
            claimed_checksum: None,

            expected_object_size: None,

            cond: &WriteCondition::default(),

            sse_customer: None,
        })
        .unwrap();

    // Read back B's data — should be B's content, not A's.
    let obj_b = coord
        .get_object_part(&GetObjectPartRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            part_number: 1,
            cond: &ReadCondition::default(),
        })
        .unwrap();
    assert_eq!(
        obj_b.body.read_all().unwrap(),
        data_b,
        "after completing B, reading part 1 should return B's data"
    );

    // Sanity: version IDs should both be 0 (unversioned).
    assert_eq!(result_a.version_id, VersionId::Null);
    assert_eq!(result_b.version_id, VersionId::Null);
}

#[test]
fn optional_list_object_key_empty_string_normalizes_to_none() {
    assert_eq!(optional_list_object_key(Some("")).unwrap(), None);
}

#[test]
fn optional_list_object_key_rejects_oversized_value() {
    let oversized = "x".repeat(1025);
    let err = optional_list_object_key(Some(&oversized)).unwrap_err();
    assert!(matches!(err, ServerError::InvalidArgument { .. }));
}

#[test]
fn optional_list_object_key_rejects_nul() {
    let err = optional_list_object_key(Some("bad\0key")).unwrap_err();
    assert!(matches!(err, ServerError::InvalidArgument { .. }));
}

#[test]
fn object_segments_integrity_readback() {
    // Storage-level verification: committed object segments rows match
    // what was written, and shard data is intact.
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "verify").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "verify", &session_id, 0, b"chunk-0-")
        .unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "verify", &session_id, 1, b"chunk-1-")
        .unwrap();
    let full = b"chunk-0-chunk-1-";
    let crc = checksum::crc64::checksum(full);
    coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "verify", test_requester()),
            session_id: &session_id,
            crc64: crc,
            total_size: 16,
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

    // Read back via storage layer directly.
    let record = coord
        .storage_node()
        .test_get_object_meta(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("verify"),
        )
        .unwrap();
    let segments = coord
        .storage_node()
        .test_get_object_segments(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("verify"),
            record.version_id(),
        )
        .unwrap();

    assert_eq!(segments.len(), 2);
    assert_eq!(segments[0].segment_index, 0);
    assert_eq!(segments[0].size, 8);
    assert_ne!(segments[0].segment_crc64, 0);
    assert_eq!(segments[1].segment_index, 1);
    assert_eq!(segments[1].size, 8);
    assert_ne!(segments[1].segment_crc64, 0);

    // Verify full readback via coordinator.
    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "verify",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    let data = result.body.read_all().unwrap();
    assert_eq!(data, full);

    // Verify CRC matches.
    assert_eq!(checksum::crc64::checksum(&data), crc);
}

#[test]
fn get_object_rejects_bad_segment_crc64() {
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "bad-segment-crc",
                test_requester(),
                None,
            ),
            data: b"segment-data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: &WriteCondition::default(),

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    {
        let mut segments = coord
            .storage_node()
            .test_get_object_segments(
                &trusted_bucket_name("bucket"),
                &trusted_object_key("bad-segment-crc"),
                put.version_id,
            )
            .unwrap();
        assert_eq!(segments.len(), 1);
        segments[0].segment_crc64 ^= 1;

        coord
            .storage_node()
            .test_replace_live_object_segments(
                &trusted_bucket_name("bucket"),
                &trusted_object_key("bad-segment-crc"),
                put.version_id,
                &segments,
            )
            .unwrap();
    }

    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "bad-segment-crc",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    let err = result.body.read_all().unwrap_err();
    assert!(matches!(
        err,
        ServerError::Store(storage::StoreError::IntegrityError { .. })
    ));
}

#[test]
fn stream_put_delete_then_reput() {
    // Overwrite cycle: stream-put → delete → normal put → GET succeeds.
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    // 1. Stream-write.
    let session_id = begin_stream_put_test(&coord, "bucket", "cycle").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "cycle", &session_id, 0, b"v1")
        .unwrap();
    let crc = checksum::crc64::checksum(b"v1");
    coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "cycle", test_requester()),
            session_id: &session_id,
            crc64: crc,
            total_size: 2,
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

    // 2. Delete.
    coord
        .delete_object(&delete_object_request(
            "bucket",
            "cycle",
            None,
            test_requester(),
            false,
            &crate::conditional::DeleteCondition::default(),
        ))
        .unwrap();

    // 3. Normal put.
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "cycle", test_requester(), None),
            data: b"v2-normal",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: &WriteCondition::default(),

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // 4. GET should return normal-put data, no object segments interference.
    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "cycle",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), b"v2-normal");
}

#[test]
fn stream_put_overwrite_with_stream_put() {
    // Stream-write → stream-write overwrite: second write's segments replace first.
    let dir = test_util::tempdir();
    let coord = setup_coordinator(dir.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    // First stream-write.
    let s1 = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &s1, 0, b"old-data")
        .unwrap();
    coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &s1,
            crc64: checksum::crc64::checksum(b"old-data"),
            total_size: 8,
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

    // Second stream-write (overwrite).
    let s2 = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &s2, 0, b"new-data")
        .unwrap();
    coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &s2,
            crc64: checksum::crc64::checksum(b"new-data"),
            total_size: 8,
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

    let result = coord
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
    assert_eq!(result.body.read_all().unwrap(), b"new-data");
}
