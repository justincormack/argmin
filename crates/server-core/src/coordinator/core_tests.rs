use super::test_helpers::{self, UploadPartRequest};
use super::test_panic::SuppressExpectedTestPanic;
use super::test_support::*;
use super::test_topology::*;
use super::*;
use crate::conditional::{DeleteCondition, SpecificEtag, WriteCondition};
use crate::coordinator::bucket_handles::{BucketHandleLoader, BucketHandleRequest};
use crate::sse::SSE_CUSTOMER_ALGORITHM;
use std::collections::BTreeSet;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use storage::storage_node_server::{
    StorageNodePgRoute, StorageNodeProcessConfig, StorageNodeProcessConfigParts, StorageNodeServer,
};
use storage::{
    install_bucket_scoped_test_hooks, BucketScopedTestHooks, ClusterEpoch, LocalClusterMap,
    LocalNodeStoreConfig, LocalPgRoute, LocalUnixStorageNodeClientConfig,
    MetadataCommandApplyTestKind, NodeId, PgId, PgState, PlacedSegmentShardBackfillWorkItem,
    RouteMapValidity, SegmentStoredBytesRequest, ShardScavengerObservationReason, StorageCluster,
    StorageClusterRuntimeMapHandle,
};

const TEST_EVENT_TIMEOUT: Duration = Duration::from_secs(2);
const BUCKET_FAST_PATH_WATCH_TEST_TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn malformed_stored_tag_envelopes_fail_closed() {
    for xml in [
        "prefix<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
        "<TaggingX><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></TaggingX>",
        "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet><Unrelated/></Tagging>",
        "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>suffix",
    ] {
        assert!(matches!(
            Coordinator::parse_serialized_tag_set(xml),
            Err(ServerError::InternalError { .. })
        ));
    }
}

fn long_lived_test_route_map_validity() -> RouteMapValidity {
    RouteMapValidity::until_ms(storage::clock::current_time_millis().saturating_add(3_600_000))
        .unwrap()
}

fn make_dynamic_runtime_map_candidate(candidate: Arc<StorageCluster>) -> Arc<StorageCluster> {
    candidate.test_store_route_map_validity(long_lived_test_route_map_validity());
    candidate
}

#[derive(Debug, PartialEq, Eq)]
enum LockWaitEvent {
    Progress,
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

fn setup_coordinator_with_only_shard_repair_worker(
    storage_cluster: Arc<StorageCluster>,
) -> Coordinator {
    Coordinator::new_with_background_sweeper_factories_for_storage_cluster(
        storage_cluster,
        "us-east-1".to_string(),
        None,
        Some(test_sse_s3_provider()),
        (
            false,
            |_, _| Ok(LifecycleSweeper::disabled()),
            |_| Ok(ShardScavengerSweeper::disabled()),
            ShardRepairSweeper::acquire_shared,
            |_| Ok(ShardBackfillSweeper::disabled()),
            |_| Ok(StreamSessionSweeper::disabled()),
        ),
    )
    .unwrap()
}

fn setup_coordinator_with_only_shard_backfill_worker(
    storage_handle: StorageClusterRuntimeMapHandle,
    storage_cluster: Arc<StorageCluster>,
) -> Coordinator {
    Coordinator::new_with_shared_caches_and_background_sweeper_factories(
        storage_handle,
        Arc::clone(&storage_cluster),
        shared_caches_for_storage_cluster(&storage_cluster),
        "us-east-1".to_string(),
        None,
        Some(test_sse_s3_provider()),
        (
            false,
            |_, _| Ok(LifecycleSweeper::disabled()),
            |_| Ok(ShardScavengerSweeper::disabled()),
            |storage_handle| Ok(ShardRepairSweeper::disabled(storage_handle.clone())),
            ShardBackfillSweeper::acquire_shared,
            |_| Ok(StreamSessionSweeper::disabled()),
        ),
    )
    .unwrap()
}

fn setup_coordinator_with_only_reclaim_worker(
    storage_handle: StorageClusterRuntimeMapHandle,
    storage_cluster: Arc<StorageCluster>,
) -> Coordinator {
    Coordinator::new_with_shared_caches_and_background_sweeper_factories(
        storage_handle,
        Arc::clone(&storage_cluster),
        shared_caches_for_storage_cluster(&storage_cluster),
        "us-east-1".to_string(),
        None,
        Some(test_sse_s3_provider()),
        (
            true,
            |_, _| Ok(LifecycleSweeper::disabled()),
            |_| Ok(ShardScavengerSweeper::disabled()),
            |storage_handle| Ok(ShardRepairSweeper::disabled(storage_handle.clone())),
            |_| Ok(ShardBackfillSweeper::disabled()),
            |_| Ok(StreamSessionSweeper::disabled()),
        ),
    )
    .unwrap()
}

fn setup_coordinator_with_only_stream_session_worker(
    storage_handle: StorageClusterRuntimeMapHandle,
    storage_cluster: Arc<StorageCluster>,
) -> Coordinator {
    Coordinator::new_with_shared_caches_and_background_sweeper_factories(
        storage_handle,
        Arc::clone(&storage_cluster),
        shared_caches_for_storage_cluster(&storage_cluster),
        "us-east-1".to_string(),
        None,
        Some(test_sse_s3_provider()),
        (
            false,
            |_, _| Ok(LifecycleSweeper::disabled()),
            |_| Ok(ShardScavengerSweeper::disabled()),
            |storage_handle| Ok(ShardRepairSweeper::disabled(storage_handle.clone())),
            |_| Ok(ShardBackfillSweeper::disabled()),
            StreamSessionSweeper::acquire_shared,
        ),
    )
    .unwrap()
}

fn make_private_socket_dir(path: &std::path::Path) {
    std::fs::create_dir_all(path).unwrap();
    let mut perms = std::fs::metadata(path).unwrap().permissions();
    perms.set_mode(0o700);
    std::fs::set_permissions(path, perms).unwrap();
}

fn spawn_storage_node_server_loop(
    server: Arc<StorageNodeServer>,
    stop: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while !stop.load(Ordering::SeqCst) {
            match server.accept_one() {
                Ok(()) => {}
                Err(error) if stop.load(Ordering::SeqCst) => {
                    let _ = error;
                    break;
                }
                Err(error) => panic!("storage node server failed: {error}"),
            }
        }
    })
}

fn stop_storage_node_server_loops(
    stop: Arc<AtomicBool>,
    socket_paths: &[PathBuf],
    threads: Vec<thread::JoinHandle<()>>,
) {
    stop.store(true, Ordering::SeqCst);
    for socket_path in socket_paths {
        let _ = UnixStream::connect(socket_path);
    }
    for thread in threads {
        thread.join().unwrap();
    }
}

#[test]
fn coordinator_storage_node_tracks_runtime_map_handle_install() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();

    assert!(Arc::ptr_eq(&coord.storage_node(), &initial));

    let next_tmp = test_util::tempdir();
    let candidate =
        make_dynamic_runtime_map_candidate(open_test_storage_cluster(next_tmp.path(), &[0, 1]));
    assert!(!Arc::ptr_eq(&coord.storage_node(), &candidate));
    handle.install(Arc::clone(&candidate)).unwrap();

    assert!(Arc::ptr_eq(&coord.storage_node(), &candidate));
}

#[test]
fn buffered_metadata_operations_recheck_request_admission_deadline_before_storage_access() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0]);
    let initial_coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&initial));
    create_bucket_for_owner_with_flags(
        &initial_coord,
        "default-owner",
        &CanonicalUserId::from_principal("default-owner"),
        "bucket",
        false,
        false,
        true,
    )
    .unwrap();
    test_helpers::put_object(
        &initial_coord,
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

    let (cluster, coord, admission) = storage::clock::with_time_override(1_000, || {
        let cluster = same_store_cluster_with_route_map_validity(
            &initial,
            tmp.path(),
            RouteMapValidity::until_ms(5_000).unwrap(),
        );
        let coord = setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
            Arc::clone(&cluster),
        );
        let admission = coord.admit_storage_route_for_request().unwrap();
        (cluster, coord, admission)
    });

    storage::clock::with_time_override(1_000, || {
        cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    });
    storage::clock::with_time_override(6_000, || {
        // The renewable raw cluster remains live, but authority already handed
        // to this request must not be extended by that renewal.
        cluster
            .head_bucket_info(&trusted_bucket_name("bucket"))
            .unwrap();
        let request = bucket_request_with_expected_owner("bucket", test_requester(), None);
        let object_request = GetObjectRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
            sse_customer: None,
        };
        let object_part_request = GetObjectPartRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            part_number: 1,
            cond: NO_READ,
            sse_customer: None,
        };
        let object_attributes_request = GetObjectAttributesRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
            want_parts: false,
            part_number_marker: None,
            max_parts: 1_000,
            sse_customer: None,
        };
        let object_metadata_request = object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        );
        let put_tags_request = PutObjectTagsRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            tags: "<Tagging><TagSet><Tag><Key>expired</Key><Value>route</Value></Tag></TagSet></Tagging>",
        };
        let put_retention_request = PutObjectRetentionRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            retention: ObjectRetention {
                mode: ObjectLockMode::Governance,
                retain_until_unix_seconds: 3_600,
            },
            bypass_governance: false,
        };
        let put_legal_hold_request = PutObjectLegalHoldRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            legal_hold: LegalHoldStatus::On,
        };
        let put_acl_request = PutObjectAclRequest {
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            acl: PutObjectAclInput::Canned(PutObjectAcl::PublicRead),
            policy_context: PutObjectPolicyContext::default()
                .with_default_canned_acl(PutObjectAcl::PublicRead.policy_condition_value()),
        };
        let put_cors_request = PutBucketConfigRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_requester(),
                None,
            ),
            config: "<CORSConfiguration><CORSRule><AllowedOrigin>https://expired.example</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>",
        };
        let put_bucket_tags_request = PutBucketConfigRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_requester(),
                None,
            ),
            config: "<Tagging><TagSet><Tag><Key>expired</Key><Value>route</Value></Tag></TagSet></Tagging>",
        };
        let put_lifecycle_request = PutBucketConfigRequest {
            bucket: bucket_request_with_expected_owner(
                "bucket",
                test_requester(),
                None,
            ),
            config: "<LifecycleConfiguration><Rule><ID>expired-route</ID><Filter><Prefix/></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>",
        };
        let put_policy_request = PutBucketPolicyRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            config: r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"default-owner"},"Action":"s3:GetBucketPolicy","Resource":"arn:aws:s3:::bucket"}]}"#,
            confirm_remove_self_bucket_access: false,
        };
        let put_versioning_request = PutBucketVersioningRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            state: BucketVersioningState::Enabled,
        };
        let put_object_lock_request = PutBucketObjectLockConfigurationRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            config: BucketObjectLockConfigurationUpdate {
                object_lock_enabled: Some(true),
                default_retention: Some(ObjectLockDefaultRetention {
                    mode: ObjectLockMode::Governance,
                    period: s3_types::RetentionPeriod::days(1).unwrap(),
                }),
            },
        };
        let put_encryption_request = PutBucketEncryptionRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            config: BucketEncryptionConfig {
                default_encryption: Some(ManagedEncryptionAlgorithm::Aes256),
                sse_c_blocked: true,
            },
        };
        let put_abac_request = PutBucketAbacRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            enabled: true,
        };
        let put_public_access_block_request = PutBucketPublicAccessBlockRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            config: PublicAccessBlockConfig {
                block_public_acls: true,
                ignore_public_acls: true,
                block_public_policy: false,
                restrict_public_buckets: false,
            },
        };
        let put_ownership_controls_request = PutBucketOwnershipControlsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            config: BucketOwnershipControls {
                object_ownership: BucketObjectOwnership::BucketOwnerPreferred,
            },
        };
        let put_bucket_acl_request = PutBucketAclRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            acl: PutBucketAclInput::Canned(BucketAcl::PublicRead),
            policy_context: PutObjectPolicyContext::default()
                .with_default_canned_acl(Some("public-read")),
        };
        let create_bucket_request = CreateBucketRequest {
            name: trusted_bucket_name("expired-create"),
            requester: test_requester(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        };
        let control_request = BucketTagControlRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        };
        let control_request_tags = vec![("expired".to_string(), "route".to_string())];
        let put_control_request = PutBucketTagControlRequest {
            control: BucketTagControlRequest {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            },
            config: put_bucket_tags_request.config,
            request_tags: &control_request_tags,
        };
        let put_untag_control_request = PutBucketTagsForUntagResourceRequest {
            control: BucketTagControlRequest {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            },
            config: put_bucket_tags_request.config,
            request_tags: &control_request_tags,
        };
        let delete_untag_control_request = UntagBucketTagControlRequest {
            control: BucketTagControlRequest {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            },
            request_tags: &control_request_tags,
        };
        let fresh_admission = coord.admit_storage_route_for_request().unwrap();
        let baseline_cors = coord
            .get_bucket_cors_on_admitted_route(&fresh_admission, &request)
            .unwrap();
        let baseline_bucket_tags = coord
            .get_bucket_tags_on_admitted_route(&fresh_admission, &request)
            .unwrap();
        let baseline_lifecycle = coord
            .get_bucket_lifecycle_on_admitted_route(&fresh_admission, &request)
            .unwrap();
        let baseline_policy = coord
            .get_bucket_policy_on_admitted_route(&fresh_admission, &request)
            .unwrap();
        let baseline_versioning = coord
            .get_bucket_versioning_on_admitted_route(&fresh_admission, &request)
            .unwrap();
        let baseline_object_lock = coord
            .get_bucket_object_lock_configuration_on_admitted_route(&fresh_admission, &request)
            .unwrap();
        let baseline_encryption = coord
            .get_bucket_encryption_on_admitted_route(&fresh_admission, &request)
            .unwrap();
        let baseline_abac = coord
            .get_bucket_abac_on_admitted_route(&fresh_admission, &request)
            .unwrap();
        let baseline_public_access_block = coord
            .get_bucket_public_access_block_on_admitted_route(&fresh_admission, &request)
            .unwrap();
        let baseline_ownership_controls = coord
            .get_bucket_ownership_controls_on_admitted_route(&fresh_admission, &request)
            .unwrap();
        let baseline_bucket_acl = coord
            .get_bucket_acl_on_admitted_route(&fresh_admission, &request)
            .unwrap();
        let baseline_tags = coord
            .get_object_tags_on_admitted_route(&fresh_admission, &object_metadata_request)
            .unwrap();
        let baseline_retention = coord
            .get_object_retention_on_admitted_route(&fresh_admission, &object_metadata_request)
            .unwrap();
        let baseline_legal_hold = coord
            .get_object_legal_hold_on_admitted_route(&fresh_admission, &object_metadata_request)
            .unwrap();
        let baseline_acl = coord
            .get_object_acl_on_admitted_route(&fresh_admission, &object_metadata_request)
            .unwrap();
        for (operation, result) in [
            (
                "CreateBucket",
                coord.create_bucket_on_admitted_route(&admission, &create_bucket_request),
            ),
            (
                "DeleteBucket",
                coord.delete_bucket_on_admitted_route(&admission, &request),
            ),
            (
                "ListBuckets",
                coord
                    .list_buckets_on_admitted_route(
                        &admission,
                        &ListBucketsRequest {
                            requester: test_requester(),
                        },
                    )
                    .map(|_| ()),
            ),
            (
                "HeadBucket",
                coord
                    .head_bucket_on_admitted_route(&admission, &request)
                    .map(|_| ()),
            ),
            (
                "GetBucketLocation",
                coord.get_bucket_location_on_admitted_route(&admission, &request),
            ),
            (
                "GetBucketVersioning",
                coord
                    .get_bucket_versioning_on_admitted_route(&admission, &request)
                    .map(|_| ()),
            ),
            (
                "GetBucketObjectLockConfiguration",
                coord
                    .get_bucket_object_lock_configuration_on_admitted_route(&admission, &request)
                    .map(|_| ()),
            ),
            (
                "GetBucketEncryption",
                coord
                    .get_bucket_encryption_on_admitted_route(&admission, &request)
                    .map(|_| ()),
            ),
            (
                "GetBucketCors",
                coord
                    .get_bucket_cors_on_admitted_route(&admission, &request)
                    .map(|_| ()),
            ),
            (
                "GetBucketTagging",
                coord
                    .get_bucket_tags_on_admitted_route(&admission, &request)
                    .map(|_| ()),
            ),
            (
                "GetBucketAbac",
                coord
                    .get_bucket_abac_on_admitted_route(&admission, &request)
                    .map(|_| ()),
            ),
            (
                "GetBucketLifecycle",
                coord
                    .get_bucket_lifecycle_on_admitted_route(&admission, &request)
                    .map(|_| ()),
            ),
            (
                "GetBucketPublicAccessBlock",
                coord
                    .get_bucket_public_access_block_on_admitted_route(&admission, &request)
                    .map(|_| ()),
            ),
            (
                "GetBucketOwnershipControls",
                coord
                    .get_bucket_ownership_controls_on_admitted_route(&admission, &request)
                    .map(|_| ()),
            ),
            (
                "GetBucketAcl",
                coord
                    .get_bucket_acl_on_admitted_route(&admission, &request)
                    .map(|_| ()),
            ),
            (
                "GetBucketPolicy",
                coord
                    .get_bucket_policy_on_admitted_route(&admission, &request)
                    .map(|_| ()),
            ),
            (
                "GetBucketPolicyStatus",
                coord
                    .get_bucket_policy_status_on_admitted_route(&admission, &request)
                    .map(|_| ()),
            ),
            (
                "ListObjectsV2",
                coord
                    .list_objects_v2_on_admitted_route(
                        &admission,
                        &ListObjectsV2Request {
                            bucket: bucket_request_with_expected_owner(
                                "bucket",
                                test_requester(),
                                None,
                            ),
                            prefix: None,
                            delimiter: None,
                            continuation_token: None,
                            max_keys: 100,
                            requested_max_keys: Some(100),
                        },
                    )
                    .map(|_| ()),
            ),
            (
                "ListObjectVersions",
                coord
                    .list_object_versions_on_admitted_route(
                        &admission,
                        &ListObjectVersionsRequest {
                            bucket: bucket_request_with_expected_owner(
                                "bucket",
                                test_requester(),
                                None,
                            ),
                            prefix: None,
                            delimiter: None,
                            key_marker: None,
                            version_id_marker: None,
                            max_keys: 100,
                            requested_max_keys: Some(100),
                        },
                    )
                    .map(|_| ()),
            ),
            (
                "ListMultipartUploads",
                coord
                    .list_multipart_uploads_on_admitted_route(
                        &admission,
                        &ListMultipartUploadsRequest {
                            bucket: bucket_request_with_expected_owner(
                                "bucket",
                                test_requester(),
                                None,
                            ),
                            prefix: None,
                            delimiter: None,
                            key_marker: None,
                            upload_id_marker: None,
                            max_uploads: 100,
                        },
                    )
                    .map(|_| ()),
            ),
            (
                "HeadObject",
                coord
                    .head_object_on_admitted_route(&admission, &object_request)
                    .map(|_| ()),
            ),
            (
                "HeadObjectPart",
                coord
                    .head_object_part_on_admitted_route(&admission, &object_part_request)
                    .map(|_| ()),
            ),
            (
                "GetObjectAttributes",
                coord
                    .get_object_attributes_on_admitted_route(&admission, &object_attributes_request)
                    .map(|_| ()),
            ),
            (
                "GetObjectTagging",
                coord
                    .get_object_tags_on_admitted_route(&admission, &object_metadata_request)
                    .map(|_| ()),
            ),
            (
                "GetObjectAcl",
                coord
                    .get_object_acl_on_admitted_route(&admission, &object_metadata_request)
                    .map(|_| ()),
            ),
            (
                "GetObjectRetention",
                coord
                    .get_object_retention_on_admitted_route(&admission, &object_metadata_request)
                    .map(|_| ()),
            ),
            (
                "GetObjectLegalHold",
                coord
                    .get_object_legal_hold_on_admitted_route(&admission, &object_metadata_request)
                    .map(|_| ()),
            ),
            (
                "PutObjectTagging",
                coord.put_object_tags_on_admitted_route(&admission, &put_tags_request),
            ),
            (
                "DeleteObjectTagging",
                coord.delete_object_tags_on_admitted_route(&admission, &object_metadata_request),
            ),
            (
                "PutObjectRetention",
                coord
                    .put_object_retention_on_admitted_route(&admission, &put_retention_request)
                    .map(|_| ()),
            ),
            (
                "PutObjectLegalHold",
                coord
                    .put_object_legal_hold_on_admitted_route(&admission, &put_legal_hold_request)
                    .map(|_| ()),
            ),
            (
                "PutObjectAcl",
                coord
                    .put_object_acl_on_admitted_route(&admission, &put_acl_request)
                    .map(|_| ()),
            ),
            (
                "PutBucketCors",
                coord.put_bucket_cors_on_admitted_route(&admission, &put_cors_request),
            ),
            (
                "DeleteBucketCors",
                coord.delete_bucket_cors_on_admitted_route(&admission, &request),
            ),
            (
                "PutBucketTagging",
                coord.put_bucket_tags_on_admitted_route(&admission, &put_bucket_tags_request),
            ),
            (
                "DeleteBucketTagging",
                coord.delete_bucket_tags_on_admitted_route(&admission, &request),
            ),
            (
                "PutBucketLifecycle",
                coord.put_bucket_lifecycle_on_admitted_route(&admission, &put_lifecycle_request),
            ),
            (
                "DeleteBucketLifecycle",
                coord.delete_bucket_lifecycle_on_admitted_route(&admission, &request),
            ),
            (
                "PutBucketPolicy",
                coord.put_bucket_policy_on_admitted_route(&admission, &put_policy_request),
            ),
            (
                "DeleteBucketPolicy",
                coord.delete_bucket_policy_on_admitted_route(&admission, &request),
            ),
            (
                "PutBucketVersioning",
                coord.put_bucket_versioning_on_admitted_route(&admission, &put_versioning_request),
            ),
            (
                "PutBucketObjectLockConfiguration",
                coord.put_bucket_object_lock_configuration_on_admitted_route(
                    &admission,
                    &put_object_lock_request,
                ),
            ),
            (
                "PutBucketEncryption",
                coord.put_bucket_encryption_on_admitted_route(&admission, &put_encryption_request),
            ),
            (
                "DeleteBucketEncryption",
                coord.delete_bucket_encryption_on_admitted_route(&admission, &request),
            ),
            (
                "PutBucketAbac",
                coord.put_bucket_abac_on_admitted_route(&admission, &put_abac_request),
            ),
            (
                "PutBucketPublicAccessBlock",
                coord.put_bucket_public_access_block_on_admitted_route(
                    &admission,
                    &put_public_access_block_request,
                ),
            ),
            (
                "DeleteBucketPublicAccessBlock",
                coord.delete_bucket_public_access_block_on_admitted_route(&admission, &request),
            ),
            (
                "PutBucketOwnershipControls",
                coord.put_bucket_ownership_controls_on_admitted_route(
                    &admission,
                    &put_ownership_controls_request,
                ),
            ),
            (
                "DeleteBucketOwnershipControls",
                coord.delete_bucket_ownership_controls_on_admitted_route(&admission, &request),
            ),
            (
                "PutBucketAcl",
                coord.put_bucket_acl_on_admitted_route(&admission, &put_bucket_acl_request),
            ),
            (
                "ListTagsForResource",
                coord
                    .get_bucket_tags_for_control_action_on_admitted_route(
                        &admission,
                        &control_request,
                        &[],
                        BucketTagControlAction::ListTagsForResource,
                    )
                    .map(|_| ()),
            ),
            (
                "TagResource",
                coord.put_bucket_tags_for_tag_resource_on_admitted_route(
                    &admission,
                    &put_control_request,
                ),
            ),
            (
                "UntagResourcePut",
                coord.put_bucket_tags_for_untag_resource_on_admitted_route(
                    &admission,
                    &put_untag_control_request,
                ),
            ),
            (
                "UntagResourceDelete",
                coord.delete_bucket_tags_for_untag_resource_on_admitted_route(
                    &admission,
                    &delete_untag_control_request,
                ),
            ),
        ] {
            let error = result.expect_err(operation);
            assert!(
                matches!(error, ServerError::OperationAborted),
                "{operation}: {error:?}"
            );
        }
        assert_eq!(
            coord
                .get_object_tags_on_admitted_route(&fresh_admission, &object_metadata_request)
                .unwrap(),
            baseline_tags
        );
        assert_eq!(
            coord
                .get_object_retention_on_admitted_route(&fresh_admission, &object_metadata_request)
                .unwrap(),
            baseline_retention
        );
        assert_eq!(
            coord
                .get_object_legal_hold_on_admitted_route(
                    &fresh_admission,
                    &object_metadata_request,
                )
                .unwrap(),
            baseline_legal_hold
        );
        assert_eq!(
            coord
                .get_object_acl_on_admitted_route(&fresh_admission, &object_metadata_request)
                .unwrap(),
            baseline_acl
        );
        assert_eq!(
            coord
                .get_bucket_cors_on_admitted_route(&fresh_admission, &request)
                .unwrap(),
            baseline_cors
        );
        assert_eq!(
            coord
                .get_bucket_tags_on_admitted_route(&fresh_admission, &request)
                .unwrap(),
            baseline_bucket_tags
        );
        assert_eq!(
            coord
                .get_bucket_lifecycle_on_admitted_route(&fresh_admission, &request)
                .unwrap(),
            baseline_lifecycle
        );
        assert_eq!(
            coord
                .get_bucket_policy_on_admitted_route(&fresh_admission, &request)
                .unwrap(),
            baseline_policy
        );
        assert_eq!(
            coord
                .get_bucket_versioning_on_admitted_route(&fresh_admission, &request)
                .unwrap(),
            baseline_versioning
        );
        assert_eq!(
            coord
                .get_bucket_object_lock_configuration_on_admitted_route(&fresh_admission, &request,)
                .unwrap(),
            baseline_object_lock
        );
        assert_eq!(
            coord
                .get_bucket_encryption_on_admitted_route(&fresh_admission, &request)
                .unwrap(),
            baseline_encryption
        );
        assert_eq!(
            coord
                .get_bucket_abac_on_admitted_route(&fresh_admission, &request)
                .unwrap(),
            baseline_abac
        );
        assert_eq!(
            coord
                .get_bucket_public_access_block_on_admitted_route(&fresh_admission, &request)
                .unwrap(),
            baseline_public_access_block
        );
        assert_eq!(
            coord
                .get_bucket_ownership_controls_on_admitted_route(&fresh_admission, &request)
                .unwrap(),
            baseline_ownership_controls
        );
        assert_eq!(
            coord
                .get_bucket_acl_on_admitted_route(&fresh_admission, &request)
                .unwrap(),
            baseline_bucket_acl
        );
        assert!(!coord
            .bucket_exists_on_admitted_route(
                &fresh_admission,
                &trusted_bucket_name("expired-create"),
            )
            .unwrap());
    });
}

#[test]
fn object_metadata_mutation_expires_at_pending_install_effect_boundary() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0]);
    let initial_coord =
        setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
            Arc::clone(&initial),
        );
    initial_coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &initial_coord,
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

    let clock = Arc::new(storage::clock::test_time_override_guard(1_000));
    let same_store_inputs = capture_same_store_cluster_inputs(&initial, tmp.path());
    drop(initial_coord);
    drop(initial);
    let cluster = open_same_store_cluster_with_route_map_validity(
        same_store_inputs,
        RouteMapValidity::until_ms(5_000).unwrap(),
    );
    cluster.test_store_route_map_lease(RouteMapValidity::until_ms(5_000).unwrap(), Some(4_000));
    let coord = setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
        Arc::clone(&cluster),
    );
    let admission = coord.admit_storage_route_for_request().unwrap();
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());

    let hook_clock = Arc::clone(&clock);
    let hook =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            hook_clock.set(4_500)
        }));
    let request = PutObjectTagsRequest {
        object: object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        ),
        tags: "<Tagging><TagSet><Tag><Key>late</Key><Value>write</Value></Tag></TagSet></Tagging>",
    };
    let error = coord
        .put_object_tags_on_admitted_route(&admission, &request)
        .unwrap_err();
    assert!(matches!(error, ServerError::OperationAborted), "{error:?}");
    drop(hook);
    drop(admission);

    clock.set(1_000);
    let fresh_admission = coord.admit_storage_route_for_request().unwrap();
    assert_eq!(
        coord
            .get_object_tags_on_admitted_route(&fresh_admission, &request.object)
            .unwrap(),
        None
    );
    coord
        .put_object_tags_on_admitted_route(&fresh_admission, &request)
        .unwrap();
    assert_eq!(
        coord
            .get_object_tags_on_admitted_route(&fresh_admission, &request.object)
            .unwrap()
            .as_deref(),
        Some(request.tags)
    );
}

#[test]
fn bucket_subresource_mutation_expires_at_pending_install_effect_boundary() {
    let tmp = test_util::tempdir();
    let cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let bucket_request = bucket_request_with_expected_owner("bucket", test_requester(), None);
    let put_request = PutBucketConfigRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        config:
            "<Tagging><TagSet><Tag><Key>late</Key><Value>write</Value></Tag></TagSet></Tagging>",
    };
    let clock = Arc::new(storage::clock::test_time_override_guard(1_000));

    cluster.test_store_route_map_lease(RouteMapValidity::until_ms(5_000).unwrap(), Some(4_000));
    let admission = coord.admit_storage_route_for_request().unwrap();
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    let hook_clock = Arc::clone(&clock);
    let hook =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            hook_clock.set(4_500)
        }));
    let error = coord
        .put_bucket_tags_on_admitted_route(&admission, &put_request)
        .unwrap_err();
    assert!(matches!(error, ServerError::OperationAborted), "{error:?}");
    drop(hook);
    drop(admission);

    clock.set(1_000);
    cluster.test_store_route_map_lease(RouteMapValidity::until_ms(5_000).unwrap(), Some(4_000));
    let admission = coord.admit_storage_route_for_request().unwrap();
    assert_eq!(
        coord
            .get_bucket_tags_on_admitted_route(&admission, &bucket_request)
            .unwrap(),
        None
    );
    coord
        .put_bucket_tags_on_admitted_route(&admission, &put_request)
        .unwrap();
    assert_eq!(
        coord
            .get_bucket_tags_on_admitted_route(&admission, &bucket_request)
            .unwrap()
            .as_deref(),
        Some(put_request.config)
    );
    drop(admission);

    clock.set(1_000);
    cluster.test_store_route_map_lease(RouteMapValidity::until_ms(5_000).unwrap(), Some(4_000));
    let admission = coord.admit_storage_route_for_request().unwrap();
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    let hook_clock = Arc::clone(&clock);
    let hook =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            hook_clock.set(4_500)
        }));
    let error = coord
        .delete_bucket_tags_on_admitted_route(&admission, &bucket_request)
        .unwrap_err();
    assert!(matches!(error, ServerError::OperationAborted), "{error:?}");
    drop(hook);
    drop(admission);

    clock.set(1_000);
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    let admission = coord.admit_storage_route_for_request().unwrap();
    assert_eq!(
        coord
            .get_bucket_tags_on_admitted_route(&admission, &bucket_request)
            .unwrap()
            .as_deref(),
        Some(put_request.config)
    );
}

#[test]
fn bucket_property_versioning_and_acl_mutations_expire_at_pending_install_effect_boundary() {
    let tmp = test_util::tempdir();
    let cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
        Arc::clone(&cluster),
    );
    for bucket in ["property", "versioning", "acl"] {
        coord
            .create_bucket_for_owner("default-owner", bucket, false)
            .unwrap();
    }
    let clock = Arc::new(storage::clock::test_time_override_guard(1_000));
    let public_access_block = PublicAccessBlockConfig {
        block_public_acls: true,
        ignore_public_acls: true,
        block_public_policy: false,
        restrict_public_buckets: false,
    };
    let property_request = PutBucketPublicAccessBlockRequest {
        bucket: bucket_request_with_expected_owner("property", test_requester(), None),
        config: public_access_block,
    };

    cluster.test_store_route_map_lease(RouteMapValidity::until_ms(5_000).unwrap(), Some(4_000));
    let admission = coord.admit_storage_route_for_request().unwrap();
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    let hook_clock = Arc::clone(&clock);
    let hook =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            hook_clock.set(4_500)
        }));
    let error = coord
        .put_bucket_public_access_block_on_admitted_route(&admission, &property_request)
        .unwrap_err();
    assert!(matches!(error, ServerError::OperationAborted), "{error:?}");
    drop(hook);
    drop(admission);

    clock.set(1_000);
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    let admission = coord.admit_storage_route_for_request().unwrap();
    assert_eq!(
        coord
            .get_bucket_public_access_block_on_admitted_route(&admission, &property_request.bucket,)
            .unwrap(),
        None
    );
    coord
        .put_bucket_public_access_block_on_admitted_route(&admission, &property_request)
        .unwrap();
    assert_eq!(
        coord
            .get_bucket_public_access_block_on_admitted_route(&admission, &property_request.bucket,)
            .unwrap(),
        Some(public_access_block)
    );
    drop(admission);

    let versioning_request = PutBucketVersioningRequest {
        bucket: bucket_request_with_expected_owner("versioning", test_requester(), None),
        state: BucketVersioningState::Enabled,
    };
    clock.set(1_000);
    cluster.test_store_route_map_lease(RouteMapValidity::until_ms(5_000).unwrap(), Some(4_000));
    let admission = coord.admit_storage_route_for_request().unwrap();
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    let hook_clock = Arc::clone(&clock);
    let hook =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            hook_clock.set(4_500)
        }));
    let error = coord
        .put_bucket_versioning_on_admitted_route(&admission, &versioning_request)
        .unwrap_err();
    assert!(matches!(error, ServerError::OperationAborted), "{error:?}");
    drop(hook);
    drop(admission);

    clock.set(1_000);
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    let admission = coord.admit_storage_route_for_request().unwrap();
    assert_eq!(
        coord
            .get_bucket_versioning_on_admitted_route(&admission, &versioning_request.bucket)
            .unwrap(),
        BucketVersioningState::Disabled
    );
    coord
        .put_bucket_versioning_on_admitted_route(&admission, &versioning_request)
        .unwrap();
    assert_eq!(
        coord
            .get_bucket_versioning_on_admitted_route(&admission, &versioning_request.bucket)
            .unwrap(),
        BucketVersioningState::Enabled
    );
    drop(admission);

    let acl_request = PutBucketAclRequest {
        bucket: bucket_request_with_expected_owner("acl", test_requester(), None),
        acl: PutBucketAclInput::Canned(BucketAcl::PublicRead),
        policy_context: PutObjectPolicyContext::default()
            .with_default_canned_acl(Some("public-read")),
    };
    clock.set(1_000);
    cluster.test_store_route_map_lease(RouteMapValidity::until_ms(5_000).unwrap(), Some(4_000));
    let admission = coord.admit_storage_route_for_request().unwrap();
    let baseline_acl = coord
        .get_bucket_acl_on_admitted_route(&admission, &acl_request.bucket)
        .unwrap();
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    let hook_clock = Arc::clone(&clock);
    let hook =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            hook_clock.set(4_500)
        }));
    let error = coord
        .put_bucket_acl_on_admitted_route(&admission, &acl_request)
        .unwrap_err();
    assert!(matches!(error, ServerError::OperationAborted), "{error:?}");
    drop(hook);
    drop(admission);

    clock.set(1_000);
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    let admission = coord.admit_storage_route_for_request().unwrap();
    assert_eq!(
        coord
            .get_bucket_acl_on_admitted_route(&admission, &acl_request.bucket)
            .unwrap(),
        baseline_acl
    );
    coord
        .put_bucket_acl_on_admitted_route(&admission, &acl_request)
        .unwrap();
    let updated_acl = coord
        .get_bucket_acl_on_admitted_route(&admission, &acl_request.bucket)
        .unwrap();
    assert!(Coordinator::acl_grants_public_read(&updated_acl.acl_grants));
}

#[test]
fn create_bucket_expires_at_pending_install_effect_boundary() {
    let tmp = test_util::tempdir();
    let cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
        Arc::clone(&cluster),
    );
    let request = CreateBucketRequest {
        name: trusted_bucket_name("late-create"),
        requester: test_requester(),
        namespace: BucketNamespace::Global,
        acl: CreateBucketAcl::DefaultPrivate,
        ownership: BucketObjectOwnership::BucketOwnerEnforced,
        object_lock_enabled: false,
    };
    let clock = Arc::new(storage::clock::test_time_override_guard(1_000));

    cluster.test_store_route_map_lease(RouteMapValidity::until_ms(5_000).unwrap(), Some(4_000));
    let admission = coord.admit_storage_route_for_request().unwrap();
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    let hook_clock = Arc::clone(&clock);
    let hook =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            hook_clock.set(4_500)
        }));
    let error = coord
        .create_bucket_on_admitted_route(&admission, &request)
        .unwrap_err();
    assert!(matches!(error, ServerError::OperationAborted), "{error:?}");
    drop(hook);
    drop(admission);

    clock.set(1_000);
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    let admission = coord.admit_storage_route_for_request().unwrap();
    assert!(!coord
        .bucket_exists_on_admitted_route(&admission, &request.name)
        .unwrap());
    coord
        .create_bucket_on_admitted_route(&admission, &request)
        .unwrap();
    assert!(coord
        .bucket_exists_on_admitted_route(&admission, &request.name)
        .unwrap());
}

#[test]
fn delete_bucket_expires_at_drain_and_pending_install_effect_boundaries() {
    let tmp = test_util::tempdir();
    let cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
        Arc::clone(&cluster),
    );
    for bucket in ["delete-drain", "delete-mark"] {
        coord
            .create_bucket_for_owner("default-owner", bucket, false)
            .unwrap();
    }
    let clock = Arc::new(storage::clock::test_time_override_guard(1_000));

    cluster.test_store_route_map_lease(RouteMapValidity::until_ms(5_000).unwrap(), Some(4_000));
    let admission = coord.admit_storage_route_for_request().unwrap();
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    let hook_clock = Arc::clone(&clock);
    let hook = install_bucket_scoped_test_hooks(BucketScopedTestHooks {
        target: Some(trusted_bucket_name("delete-drain")),
        before_begin_bucket_delete_drain: Some(Arc::new(move || hook_clock.set(4_500))),
        ..BucketScopedTestHooks::default()
    });
    let request = bucket_request_with_expected_owner("delete-drain", test_requester(), None);
    let error = coord
        .delete_bucket_on_admitted_route(&admission, &request)
        .unwrap_err();
    assert!(matches!(error, ServerError::OperationAborted), "{error:?}");
    drop(hook);
    drop(admission);
    let snapshot = cluster
        .bucket_delete_debug_snapshot(&trusted_bucket_name("delete-drain"))
        .unwrap();
    assert_eq!(snapshot.bucket_row.unwrap().state, BucketState::Active);
    assert!(snapshot.durable_write_drain.is_none());
    assert!(snapshot.pending_metadata_command.is_none());

    clock.set(1_000);
    cluster.test_store_route_map_lease(RouteMapValidity::until_ms(5_000).unwrap(), Some(4_000));
    let admission = coord.admit_storage_route_for_request().unwrap();
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    let hook_clock = Arc::clone(&clock);
    let hook = install_bucket_scoped_test_hooks(BucketScopedTestHooks {
        target: Some(trusted_bucket_name("delete-mark")),
        after_begin_bucket_delete_drain: Some(Arc::new(move || hook_clock.set(4_500))),
        ..BucketScopedTestHooks::default()
    });
    let request = bucket_request_with_expected_owner("delete-mark", test_requester(), None);
    let error = coord
        .delete_bucket_on_admitted_route(&admission, &request)
        .unwrap_err();
    assert!(matches!(error, ServerError::OperationAborted), "{error:?}");
    drop(hook);
    drop(admission);
    let snapshot = cluster
        .bucket_delete_debug_snapshot(&trusted_bucket_name("delete-mark"))
        .unwrap();
    let bucket_row = snapshot.bucket_row.unwrap();
    assert_eq!(bucket_row.state, BucketState::Active);
    assert!(snapshot.durable_write_drain.is_some());
    assert!(snapshot.pending_metadata_command.is_none());
}

#[test]
fn object_delete_mutations_expire_at_pending_install_effect_boundary() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0]);
    let initial_coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&initial));
    for bucket in ["unversioned", "versioned", "batch"] {
        initial_coord
            .create_bucket_for_owner("default-owner", bucket, false)
            .unwrap();
    }
    put_bucket_versioning_test(
        &initial_coord,
        "versioned",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();
    for (bucket, key) in [
        ("unversioned", "key"),
        ("versioned", "key"),
        ("batch", "first"),
        ("batch", "second"),
    ] {
        test_helpers::put_object(
            &initial_coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(bucket, key, test_requester(), None),
                data: format!("{bucket}/{key}").as_bytes(),
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let clock = Arc::new(storage::clock::test_time_override_guard(1_000));
    let cluster = initial;
    let coord = initial_coord;

    for bucket in ["unversioned", "versioned"] {
        clock.set(1_000);
        cluster.test_store_route_map_lease(RouteMapValidity::until_ms(5_000).unwrap(), Some(4_000));
        let admission = coord.admit_storage_route_for_request().unwrap();
        cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
        let baseline = coord
            .get_object_on_admitted_route(
                &admission,
                &GetObjectRequest {
                    object: object_version_request_with_expected_owner(
                        bucket,
                        "key",
                        None,
                        test_requester(),
                        None,
                    ),
                    cond: NO_READ,
                    sse_customer: None,
                },
            )
            .unwrap();
        let delete_condition = DeleteCondition::IfMatch(baseline.etag.into());
        let hook_clock = Arc::clone(&clock);
        let hook_calls = Arc::new(AtomicUsize::new(0));
        let hook_calls_for_hook = Arc::clone(&hook_calls);
        let hook = cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(
            move || {
                hook_calls_for_hook.fetch_add(1, Ordering::SeqCst);
                hook_clock.set(4_500);
            },
        ));
        let request = DeleteObjectRequest {
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_requester(),
                None,
            ),
            bypass_governance: false,
            cond: &delete_condition,
        };
        let result = coord.delete_object_on_admitted_route(&admission, &request);
        let hook_call_count = hook_calls.load(Ordering::SeqCst);
        assert!(hook_call_count > 0, "{bucket}");
        let admission_error = admission.require_valid_now().unwrap_err();
        let error = result.err().unwrap_or_else(|| {
            panic!(
                "{bucket} unexpectedly succeeded after {hook_call_count} hook calls; admission: {admission_error:?}"
            )
        });
        assert!(matches!(error, ServerError::OperationAborted), "{error:?}");
        drop(hook);
        drop(admission);

        clock.set(1_000);
        let fresh_admission = coord.admit_storage_route_for_request().unwrap();
        let current = coord
            .get_object_on_admitted_route(
                &fresh_admission,
                &GetObjectRequest {
                    object: object_version_request_with_expected_owner(
                        bucket,
                        "key",
                        None,
                        test_requester(),
                        None,
                    ),
                    cond: NO_READ,
                    sse_customer: None,
                },
            )
            .unwrap();
        assert_eq!(
            current.body.read_all().unwrap(),
            format!("{bucket}/key").as_bytes()
        );
    }

    clock.set(1_000);
    cluster.test_store_route_map_lease(RouteMapValidity::until_ms(5_000).unwrap(), Some(4_000));
    let admission = coord.admit_storage_route_for_request().unwrap();
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    let batch_etags = ["first", "second"].map(|key| {
        coord
            .get_object_on_admitted_route(
                &admission,
                &GetObjectRequest {
                    object: object_version_request_with_expected_owner(
                        "batch",
                        key,
                        None,
                        test_requester(),
                        None,
                    ),
                    cond: NO_READ,
                    sse_customer: None,
                },
            )
            .unwrap()
            .etag
    });
    let hook_clock = Arc::clone(&clock);
    let hook =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            hook_clock.set(4_500)
        }));
    let entries = [
        DeleteEntry {
            key: trusted_object_key("first"),
            version_id: None,
            cond: DeleteCondition::IfMatch(batch_etags[0].clone().into()),
        },
        DeleteEntry {
            key: trusted_object_key("second"),
            version_id: None,
            cond: DeleteCondition::IfMatch(batch_etags[1].clone().into()),
        },
    ];
    let result = coord
        .delete_objects_on_admitted_route(
            &admission,
            &DeleteObjectsRequest {
                bucket: bucket_request_with_expected_owner("batch", test_requester(), None),
                entries: &entries,
                bypass_governance: false,
            },
        )
        .unwrap();
    assert!(result.deleted.is_empty());
    assert_eq!(result.errors.len(), 2);
    assert!(result
        .errors
        .iter()
        .all(|error| error.code == "OperationAborted"));
    drop(hook);
    drop(admission);

    clock.set(1_000);
    let fresh_admission = coord.admit_storage_route_for_request().unwrap();
    for key in ["first", "second"] {
        let current = coord
            .get_object_on_admitted_route(
                &fresh_admission,
                &GetObjectRequest {
                    object: object_version_request_with_expected_owner(
                        "batch",
                        key,
                        None,
                        test_requester(),
                        None,
                    ),
                    cond: NO_READ,
                    sse_customer: None,
                },
            )
            .unwrap();
        assert_eq!(
            current.body.read_all().unwrap(),
            format!("batch/{key}").as_bytes()
        );
    }
}

#[test]
fn object_body_reads_recheck_admission_before_retaining_payload_authority() {
    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0]);
    let initial_coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&initial));
    initial_coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &initial_coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"admitted-payload",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let time = Arc::new(storage::clock::test_time_override_guard(1_000));
    let cluster = same_store_cluster_with_route_map_validity(
        &initial,
        tmp.path(),
        RouteMapValidity::until_ms(5_000).unwrap(),
    );
    let coord = setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
        Arc::clone(&cluster),
    );
    let hook_time = Arc::clone(&time);
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some(("bucket".to_string(), "key".to_string())),
        after_object_read_snapshot: Some(Arc::new(move || hook_time.set(6_000))),
        ..ReclamationTestHooks::default()
    });
    let object = || {
        object_version_request_with_expected_owner("bucket", "key", None, test_requester(), None)
    };

    time.set(1_000);
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(5_000).unwrap());
    let admission = coord.admit_storage_route_for_request().unwrap();
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    let error = coord
        .get_object_on_admitted_route(
            &admission,
            &GetObjectRequest {
                object: object(),
                cond: NO_READ,
                sse_customer: None,
            },
        )
        .unwrap_err();
    assert!(matches!(error, ServerError::OperationAborted));
    drop(admission);

    time.set(1_000);
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(5_000).unwrap());
    let admission = coord.admit_storage_route_for_request().unwrap();
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    let error = coord
        .get_object_part_on_admitted_route(
            &admission,
            &GetObjectPartRequest {
                object: object(),
                part_number: 1,
                cond: NO_READ,
                sse_customer: None,
            },
        )
        .unwrap_err();
    assert!(matches!(error, ServerError::OperationAborted));
    drop(admission);

    time.set(1_000);
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(5_000).unwrap());
    let admission = coord.admit_storage_route_for_request().unwrap();
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    let error = coord
        .get_object_range_on_admitted_route(
            &admission,
            &GetObjectRangeRequest {
                object: object(),
                range: ByteRange::Range { start: 0, end: 3 },
                cond: NO_READ,
                sse_customer: None,
            },
        )
        .unwrap_err();
    assert!(matches!(error, ServerError::OperationAborted));
}

#[test]
fn head_bucket_warm_policy_cache_uses_loaded_identity_and_captured_deadline() {
    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let tmp = test_util::tempdir();
    let cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&cluster));
    let bucket = "head-bucket-admitted-policy-cache";
    let requester = test_requester();

    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &coord,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        requester.clone(),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
        &coord,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::head-bucket-admitted-policy-cache"}]}"#,
        requester.clone(),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, "key", requester.clone(), None),
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
                requester.clone(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    let cached = coord
        .get_bucket_fast_path(&bucket_name)
        .expect("HeadObject should warm the parsed bucket-policy cache");
    assert!(matches!(
        cached.policy,
        storage::BucketFastPathPolicy::Loaded(_)
    ));
    assert_eq!(
        coord.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );

    let admission = storage::clock::with_time_override(1_000, || {
        cluster.test_store_route_map_validity(RouteMapValidity::until_ms(5_000).unwrap());
        coord.admit_storage_route_for_request().unwrap()
    });
    storage::clock::with_time_override(1_000, || {
        cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    });

    let _identity_load_error_guard =
        install_bucket_fast_path_identity_load_error_test_hook(bucket.to_string());
    storage::clock::with_time_override(4_000, || {
        coord
            .head_bucket_on_admitted_route(
                &admission,
                &bucket_request_with_expected_owner(bucket, requester.clone(), None),
            )
            .unwrap();
    });
    assert_eq!(
        coord.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true),
        "request-scoped policy-cache validation must not perform the raw identity lookup"
    );

    storage::clock::with_time_override(6_000, || {
        cluster.head_bucket_info(&bucket_name).unwrap();
        let error = coord
            .head_bucket_on_admitted_route(
                &admission,
                &bucket_request_with_expected_owner(bucket, requester, None),
            )
            .unwrap_err();
        assert!(matches!(error, ServerError::OperationAborted), "{error:?}");
    });
}

#[test]
fn object_snapshot_route_rechecks_deadline_after_warm_bucket_fast_path() {
    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let tmp = test_util::tempdir();
    let cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&cluster));
    let bucket = "object-read-admitted-fast-path";
    let requester = test_requester();

    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &coord,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        requester.clone(),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, "key", requester.clone(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let request = GetObjectRequest {
        object: object_version_request_with_expected_owner(bucket, "key", None, requester, None),
        cond: NO_READ,
        sse_customer: None,
    };

    coord.head_object(&request).unwrap();
    assert_eq!(
        coord.bucket_fast_path_is_fresh_for_test(&trusted_bucket_name(bucket)),
        Some(true)
    );

    let clock = Arc::new(storage::clock::test_time_override_guard(1_000));
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(5_000).unwrap());
    let admission = coord.admit_storage_route_for_request().unwrap();
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    coord
        .head_object_on_admitted_route(&admission, &request)
        .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    let key = trusted_object_key("key");
    let route = admission
        .active_object_read_route(
            &bucket_name,
            &key,
            None,
            storage::ObjectReadSnapshotMode::MetadataOnly,
        )
        .unwrap();
    clock.set(6_000);
    let metadata_error = route.load_object_if(|_| Ok::<(), ()>(())).unwrap_err();
    assert!(matches!(
        metadata_error,
        storage::ObjectPgActionError::Store(storage::StoreError::RouteMapExpired { .. })
    ));
    clock.set(1_000);

    let action_clock = Arc::clone(&clock);
    let snapshot_error = route
        .load_object_read_snapshot_if(move |_| {
            action_clock.set(6_000);
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(matches!(
        snapshot_error,
        storage::ObjectPgActionError::Store(storage::StoreError::RouteMapExpired { .. })
    ));
    clock.set(1_000);

    let hook_clock = Arc::clone(&clock);
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: None,
        after_policy_fast_path_hit: Some(Arc::new(move || hook_clock.set(6_000))),
    });
    let error = coord
        .head_object_on_admitted_route(&admission, &request)
        .unwrap_err();
    assert!(matches!(error, ServerError::OperationAborted), "{error:?}");
    cluster
        .head_bucket_info(&trusted_bucket_name(bucket))
        .expect("the renewed raw route should remain valid after admitted authority expires");
}

#[test]
fn bucket_metadata_reads_reject_admission_from_an_unrelated_coordinator() {
    let tmp = test_util::tempdir();
    let cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let local_handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&cluster));
    let foreign_handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&cluster));
    let local = Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
        local_handle,
        "us-east-1".to_string(),
        None,
        test_sse_s3_provider(),
        BackgroundWorkerMode::none(),
    )
    .unwrap();
    let foreign = Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
        foreign_handle,
        "us-east-1".to_string(),
        None,
        test_sse_s3_provider(),
        BackgroundWorkerMode::none(),
    )
    .unwrap();
    assert!(Arc::ptr_eq(&local.storage_node(), &foreign.storage_node()));
    assert!(!local.shares_storage_route_admission_with(&foreign));

    create_bucket_for_owner_with_flags(
        &foreign,
        "default-owner",
        &CanonicalUserId::from_principal("default-owner"),
        "bucket",
        false,
        false,
        true,
    )
    .unwrap();
    put_bucket_policy_test(
        &foreign,
        "bucket",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"default-owner"},"Action":["s3:GetBucketPolicy","s3:GetBucketPolicyStatus"],"Resource":"arn:aws:s3:::bucket"}]}"#,
        test_requester(),
        None,
    )
    .unwrap();
    let foreign_admission = foreign.admit_storage_route_for_request().unwrap();
    let request = bucket_request_with_expected_owner("bucket", test_requester(), None);
    foreign
        .list_buckets_on_admitted_route(
            &foreign_admission,
            &ListBucketsRequest {
                requester: test_requester(),
            },
        )
        .unwrap();
    foreign
        .head_bucket_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    foreign
        .get_bucket_location_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    foreign
        .get_bucket_versioning_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    foreign
        .get_bucket_object_lock_configuration_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    foreign
        .get_bucket_encryption_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    foreign
        .get_bucket_cors_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    foreign
        .get_bucket_tags_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    foreign
        .get_bucket_abac_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    foreign
        .get_bucket_lifecycle_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    foreign
        .get_bucket_public_access_block_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    foreign
        .get_bucket_ownership_controls_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    foreign
        .get_bucket_acl_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    foreign
        .get_bucket_policy_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    foreign
        .get_bucket_policy_status_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    foreign
        .list_objects_v2_on_admitted_route(
            &foreign_admission,
            &ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 100,
                requested_max_keys: Some(100),
            },
        )
        .unwrap();
    foreign
        .list_object_versions_on_admitted_route(
            &foreign_admission,
            &ListObjectVersionsRequest {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: None,
                key_marker: None,
                version_id_marker: None,
                max_keys: 100,
                requested_max_keys: Some(100),
            },
        )
        .unwrap();
    foreign
        .list_multipart_uploads_on_admitted_route(
            &foreign_admission,
            &ListMultipartUploadsRequest {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 100,
            },
        )
        .unwrap();
    let baseline_cors_request = PutBucketConfigRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        config: "<CORSConfiguration><CORSRule><AllowedOrigin>https://baseline.example</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>",
    };
    let baseline_tags_request = PutBucketConfigRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        config: "<Tagging><TagSet><Tag><Key>domain</Key><Value>baseline</Value></Tag></TagSet></Tagging>",
    };
    let baseline_lifecycle_request = PutBucketConfigRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        config: "<LifecycleConfiguration><Rule><ID>domain-baseline</ID><Filter><Prefix/></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>",
    };
    let baseline_versioning_request = PutBucketVersioningRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        state: BucketVersioningState::Enabled,
    };
    let baseline_object_lock_request = PutBucketObjectLockConfigurationRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        config: BucketObjectLockConfigurationUpdate {
            object_lock_enabled: Some(true),
            default_retention: Some(ObjectLockDefaultRetention {
                mode: ObjectLockMode::Governance,
                period: s3_types::RetentionPeriod::days(1).unwrap(),
            }),
        },
    };
    let baseline_encryption_request = PutBucketEncryptionRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        config: BucketEncryptionConfig {
            default_encryption: Some(ManagedEncryptionAlgorithm::Aes256),
            sse_c_blocked: true,
        },
    };
    let baseline_abac_request = PutBucketAbacRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        enabled: true,
    };
    let baseline_public_access_block_request = PutBucketPublicAccessBlockRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        config: PublicAccessBlockConfig::default(),
    };
    let baseline_ownership_controls_request = PutBucketOwnershipControlsRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        config: BucketOwnershipControls {
            object_ownership: BucketObjectOwnership::BucketOwnerPreferred,
        },
    };
    let baseline_bucket_acl_request = PutBucketAclRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        acl: PutBucketAclInput::Canned(BucketAcl::PublicRead),
        policy_context: PutObjectPolicyContext::default()
            .with_default_canned_acl(Some("public-read")),
    };
    foreign
        .put_bucket_cors_on_admitted_route(&foreign_admission, &baseline_cors_request)
        .unwrap();
    foreign
        .put_bucket_tags_on_admitted_route(&foreign_admission, &baseline_tags_request)
        .unwrap();
    foreign
        .put_bucket_lifecycle_on_admitted_route(&foreign_admission, &baseline_lifecycle_request)
        .unwrap();
    foreign
        .put_bucket_versioning_on_admitted_route(&foreign_admission, &baseline_versioning_request)
        .unwrap();
    foreign
        .put_bucket_object_lock_configuration_on_admitted_route(
            &foreign_admission,
            &baseline_object_lock_request,
        )
        .unwrap();
    foreign
        .put_bucket_encryption_on_admitted_route(&foreign_admission, &baseline_encryption_request)
        .unwrap();
    foreign
        .put_bucket_public_access_block_on_admitted_route(
            &foreign_admission,
            &baseline_public_access_block_request,
        )
        .unwrap();
    foreign
        .put_bucket_ownership_controls_on_admitted_route(
            &foreign_admission,
            &baseline_ownership_controls_request,
        )
        .unwrap();
    foreign
        .put_bucket_acl_on_admitted_route(&foreign_admission, &baseline_bucket_acl_request)
        .unwrap();
    foreign
        .put_bucket_abac_on_admitted_route(&foreign_admission, &baseline_abac_request)
        .unwrap();
    let baseline_cors = foreign
        .get_bucket_cors_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    let baseline_tags = foreign
        .get_bucket_tags_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    let baseline_lifecycle = foreign
        .get_bucket_lifecycle_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    let baseline_policy = foreign
        .get_bucket_policy_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    let baseline_versioning = foreign
        .get_bucket_versioning_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    let baseline_object_lock = foreign
        .get_bucket_object_lock_configuration_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    let baseline_encryption = foreign
        .get_bucket_encryption_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    let baseline_abac = foreign
        .get_bucket_abac_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    let baseline_public_access_block = foreign
        .get_bucket_public_access_block_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    let baseline_ownership_controls = foreign
        .get_bucket_ownership_controls_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    let baseline_bucket_acl = foreign
        .get_bucket_acl_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    let rejected_cors_request = PutBucketConfigRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        config: "<CORSConfiguration><CORSRule><AllowedOrigin>https://rejected.example</AllowedOrigin><AllowedMethod>PUT</AllowedMethod></CORSRule></CORSConfiguration>",
    };
    let rejected_tags_request = PutBucketConfigRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        config: "<Tagging><TagSet><Tag><Key>domain</Key><Value>rejected</Value></Tag></TagSet></Tagging>",
    };
    let rejected_lifecycle_request = PutBucketConfigRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        config: "<LifecycleConfiguration><Rule><ID>domain-rejected</ID><Filter><Prefix/></Filter><Status>Enabled</Status><Expiration><Days>2</Days></Expiration></Rule></LifecycleConfiguration>",
    };
    let rejected_policy_request = PutBucketPolicyRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        config: r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"default-owner"},"Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket"}]}"#,
        confirm_remove_self_bucket_access: false,
    };
    let rejected_create_request = CreateBucketRequest {
        name: trusted_bucket_name("foreign-domain-create"),
        requester: test_requester(),
        namespace: BucketNamespace::Global,
        acl: CreateBucketAcl::DefaultPrivate,
        ownership: BucketObjectOwnership::BucketOwnerEnforced,
        object_lock_enabled: false,
    };
    let rejected_object_lock_request = PutBucketObjectLockConfigurationRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        config: BucketObjectLockConfigurationUpdate {
            object_lock_enabled: Some(true),
            default_retention: Some(ObjectLockDefaultRetention {
                mode: ObjectLockMode::Compliance,
                period: s3_types::RetentionPeriod::days(2).unwrap(),
            }),
        },
    };
    let rejected_encryption_request = PutBucketEncryptionRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        config: BucketEncryptionConfig::default(),
    };
    let rejected_abac_request = PutBucketAbacRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        enabled: false,
    };
    let rejected_public_access_block_request = PutBucketPublicAccessBlockRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        config: PublicAccessBlockConfig {
            block_public_acls: true,
            ignore_public_acls: true,
            block_public_policy: true,
            restrict_public_buckets: true,
        },
    };
    let rejected_ownership_controls_request = PutBucketOwnershipControlsRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        config: BucketOwnershipControls {
            object_ownership: BucketObjectOwnership::ObjectWriter,
        },
    };
    let rejected_bucket_acl_request = PutBucketAclRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        acl: PutBucketAclInput::Canned(BucketAcl::Private),
        policy_context: PutObjectPolicyContext::default().with_default_canned_acl(Some("private")),
    };
    let control_request = BucketTagControlRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
    };
    let control_request_tags = vec![("domain".to_string(), "rejected".to_string())];
    let put_control_request = PutBucketTagControlRequest {
        control: BucketTagControlRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        },
        config: rejected_tags_request.config,
        request_tags: &control_request_tags,
    };
    let put_untag_control_request = PutBucketTagsForUntagResourceRequest {
        control: BucketTagControlRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        },
        config: rejected_tags_request.config,
        request_tags: &control_request_tags,
    };
    let delete_untag_control_request = UntagBucketTagControlRequest {
        control: BucketTagControlRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        },
        request_tags: &control_request_tags,
    };

    for (operation, result) in [
        (
            "CreateBucket",
            local.create_bucket_on_admitted_route(&foreign_admission, &rejected_create_request),
        ),
        (
            "DeleteBucket",
            local.delete_bucket_on_admitted_route(&foreign_admission, &request),
        ),
        (
            "ListBuckets",
            local
                .list_buckets_on_admitted_route(
                    &foreign_admission,
                    &ListBucketsRequest {
                        requester: test_requester(),
                    },
                )
                .map(|_| ()),
        ),
        (
            "HeadBucket",
            local
                .head_bucket_on_admitted_route(&foreign_admission, &request)
                .map(|_| ()),
        ),
        (
            "GetBucketLocation",
            local.get_bucket_location_on_admitted_route(&foreign_admission, &request),
        ),
        (
            "GetBucketVersioning",
            local
                .get_bucket_versioning_on_admitted_route(&foreign_admission, &request)
                .map(|_| ()),
        ),
        (
            "GetBucketObjectLockConfiguration",
            local
                .get_bucket_object_lock_configuration_on_admitted_route(
                    &foreign_admission,
                    &request,
                )
                .map(|_| ()),
        ),
        (
            "GetBucketEncryption",
            local
                .get_bucket_encryption_on_admitted_route(&foreign_admission, &request)
                .map(|_| ()),
        ),
        (
            "GetBucketCors",
            local
                .get_bucket_cors_on_admitted_route(&foreign_admission, &request)
                .map(|_| ()),
        ),
        (
            "GetBucketTagging",
            local
                .get_bucket_tags_on_admitted_route(&foreign_admission, &request)
                .map(|_| ()),
        ),
        (
            "GetBucketAbac",
            local
                .get_bucket_abac_on_admitted_route(&foreign_admission, &request)
                .map(|_| ()),
        ),
        (
            "GetBucketLifecycle",
            local
                .get_bucket_lifecycle_on_admitted_route(&foreign_admission, &request)
                .map(|_| ()),
        ),
        (
            "GetBucketPublicAccessBlock",
            local
                .get_bucket_public_access_block_on_admitted_route(&foreign_admission, &request)
                .map(|_| ()),
        ),
        (
            "GetBucketOwnershipControls",
            local
                .get_bucket_ownership_controls_on_admitted_route(&foreign_admission, &request)
                .map(|_| ()),
        ),
        (
            "GetBucketAcl",
            local
                .get_bucket_acl_on_admitted_route(&foreign_admission, &request)
                .map(|_| ()),
        ),
        (
            "GetBucketPolicy",
            local
                .get_bucket_policy_on_admitted_route(&foreign_admission, &request)
                .map(|_| ()),
        ),
        (
            "GetBucketPolicyStatus",
            local
                .get_bucket_policy_status_on_admitted_route(&foreign_admission, &request)
                .map(|_| ()),
        ),
        (
            "ListObjectsV2",
            local
                .list_objects_v2_on_admitted_route(
                    &foreign_admission,
                    &ListObjectsV2Request {
                        bucket: bucket_request_with_expected_owner(
                            "bucket",
                            test_requester(),
                            None,
                        ),
                        prefix: None,
                        delimiter: None,
                        continuation_token: None,
                        max_keys: 100,
                        requested_max_keys: Some(100),
                    },
                )
                .map(|_| ()),
        ),
        (
            "ListObjectVersions",
            local
                .list_object_versions_on_admitted_route(
                    &foreign_admission,
                    &ListObjectVersionsRequest {
                        bucket: bucket_request_with_expected_owner(
                            "bucket",
                            test_requester(),
                            None,
                        ),
                        prefix: None,
                        delimiter: None,
                        key_marker: None,
                        version_id_marker: None,
                        max_keys: 100,
                        requested_max_keys: Some(100),
                    },
                )
                .map(|_| ()),
        ),
        (
            "ListMultipartUploads",
            local
                .list_multipart_uploads_on_admitted_route(
                    &foreign_admission,
                    &ListMultipartUploadsRequest {
                        bucket: bucket_request_with_expected_owner(
                            "bucket",
                            test_requester(),
                            None,
                        ),
                        prefix: None,
                        delimiter: None,
                        key_marker: None,
                        upload_id_marker: None,
                        max_uploads: 100,
                    },
                )
                .map(|_| ()),
        ),
        (
            "PutBucketCors",
            local.put_bucket_cors_on_admitted_route(&foreign_admission, &rejected_cors_request),
        ),
        (
            "DeleteBucketCors",
            local.delete_bucket_cors_on_admitted_route(&foreign_admission, &request),
        ),
        (
            "PutBucketTagging",
            local.put_bucket_tags_on_admitted_route(&foreign_admission, &rejected_tags_request),
        ),
        (
            "DeleteBucketTagging",
            local.delete_bucket_tags_on_admitted_route(&foreign_admission, &request),
        ),
        (
            "PutBucketLifecycle",
            local.put_bucket_lifecycle_on_admitted_route(
                &foreign_admission,
                &rejected_lifecycle_request,
            ),
        ),
        (
            "DeleteBucketLifecycle",
            local.delete_bucket_lifecycle_on_admitted_route(&foreign_admission, &request),
        ),
        (
            "PutBucketPolicy",
            local.put_bucket_policy_on_admitted_route(&foreign_admission, &rejected_policy_request),
        ),
        (
            "DeleteBucketPolicy",
            local.delete_bucket_policy_on_admitted_route(&foreign_admission, &request),
        ),
        (
            "PutBucketVersioning",
            local.put_bucket_versioning_on_admitted_route(
                &foreign_admission,
                &baseline_versioning_request,
            ),
        ),
        (
            "PutBucketObjectLockConfiguration",
            local.put_bucket_object_lock_configuration_on_admitted_route(
                &foreign_admission,
                &rejected_object_lock_request,
            ),
        ),
        (
            "PutBucketEncryption",
            local.put_bucket_encryption_on_admitted_route(
                &foreign_admission,
                &rejected_encryption_request,
            ),
        ),
        (
            "DeleteBucketEncryption",
            local.delete_bucket_encryption_on_admitted_route(&foreign_admission, &request),
        ),
        (
            "PutBucketAbac",
            local.put_bucket_abac_on_admitted_route(&foreign_admission, &rejected_abac_request),
        ),
        (
            "PutBucketPublicAccessBlock",
            local.put_bucket_public_access_block_on_admitted_route(
                &foreign_admission,
                &rejected_public_access_block_request,
            ),
        ),
        (
            "DeleteBucketPublicAccessBlock",
            local.delete_bucket_public_access_block_on_admitted_route(&foreign_admission, &request),
        ),
        (
            "PutBucketOwnershipControls",
            local.put_bucket_ownership_controls_on_admitted_route(
                &foreign_admission,
                &rejected_ownership_controls_request,
            ),
        ),
        (
            "DeleteBucketOwnershipControls",
            local.delete_bucket_ownership_controls_on_admitted_route(&foreign_admission, &request),
        ),
        (
            "PutBucketAcl",
            local
                .put_bucket_acl_on_admitted_route(&foreign_admission, &rejected_bucket_acl_request),
        ),
        (
            "ListTagsForResource",
            local
                .get_bucket_tags_for_control_action_on_admitted_route(
                    &foreign_admission,
                    &control_request,
                    &[],
                    BucketTagControlAction::ListTagsForResource,
                )
                .map(|_| ()),
        ),
        (
            "TagResource",
            local.put_bucket_tags_for_tag_resource_on_admitted_route(
                &foreign_admission,
                &put_control_request,
            ),
        ),
        (
            "UntagResourcePut",
            local.put_bucket_tags_for_untag_resource_on_admitted_route(
                &foreign_admission,
                &put_untag_control_request,
            ),
        ),
        (
            "UntagResourceDelete",
            local.delete_bucket_tags_for_untag_resource_on_admitted_route(
                &foreign_admission,
                &delete_untag_control_request,
            ),
        ),
    ] {
        let error = result.expect_err(operation);
        assert!(
            matches!(error, ServerError::OperationAborted),
            "{operation}: {error:?}"
        );
    }
    assert!(!foreign
        .bucket_exists_on_admitted_route(&foreign_admission, &rejected_create_request.name)
        .unwrap());
    foreign
        .create_bucket_on_admitted_route(&foreign_admission, &rejected_create_request)
        .unwrap();
    assert!(foreign
        .bucket_exists_on_admitted_route(&foreign_admission, &rejected_create_request.name)
        .unwrap());
    assert_eq!(
        foreign
            .get_bucket_cors_on_admitted_route(&foreign_admission, &request)
            .unwrap(),
        baseline_cors
    );
    assert_eq!(
        foreign
            .get_bucket_tags_on_admitted_route(&foreign_admission, &request)
            .unwrap(),
        baseline_tags
    );
    assert_eq!(
        foreign
            .get_bucket_lifecycle_on_admitted_route(&foreign_admission, &request)
            .unwrap(),
        baseline_lifecycle
    );
    assert_eq!(
        foreign
            .get_bucket_policy_on_admitted_route(&foreign_admission, &request)
            .unwrap(),
        baseline_policy
    );
    assert_eq!(
        foreign
            .get_bucket_versioning_on_admitted_route(&foreign_admission, &request)
            .unwrap(),
        baseline_versioning
    );
    assert_eq!(
        foreign
            .get_bucket_object_lock_configuration_on_admitted_route(&foreign_admission, &request,)
            .unwrap(),
        baseline_object_lock
    );
    assert_eq!(
        foreign
            .get_bucket_encryption_on_admitted_route(&foreign_admission, &request)
            .unwrap(),
        baseline_encryption
    );
    assert_eq!(
        foreign
            .get_bucket_abac_on_admitted_route(&foreign_admission, &request)
            .unwrap(),
        baseline_abac
    );
    assert_eq!(
        foreign
            .get_bucket_public_access_block_on_admitted_route(&foreign_admission, &request)
            .unwrap(),
        baseline_public_access_block
    );
    assert_eq!(
        foreign
            .get_bucket_ownership_controls_on_admitted_route(&foreign_admission, &request)
            .unwrap(),
        baseline_ownership_controls
    );
    assert_eq!(
        foreign
            .get_bucket_acl_on_admitted_route(&foreign_admission, &request)
            .unwrap(),
        baseline_bucket_acl
    );
}

#[test]
fn object_metadata_operations_reject_admission_from_an_unrelated_coordinator() {
    let tmp = test_util::tempdir();
    let cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let local_handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&cluster));
    let foreign_handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&cluster));
    let local = Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
        local_handle,
        "us-east-1".to_string(),
        None,
        test_sse_s3_provider(),
        BackgroundWorkerMode::none(),
    )
    .unwrap();
    let foreign = Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
        foreign_handle,
        "us-east-1".to_string(),
        None,
        test_sse_s3_provider(),
        BackgroundWorkerMode::none(),
    )
    .unwrap();
    assert!(Arc::ptr_eq(&local.storage_node(), &foreign.storage_node()));
    assert!(!local.shares_storage_route_admission_with(&foreign));

    create_bucket_for_owner_with_flags(
        &foreign,
        "default-owner",
        &CanonicalUserId::from_principal("default-owner"),
        "bucket",
        false,
        false,
        true,
    )
    .unwrap();
    test_helpers::put_object(
        &foreign,
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

    let request = GetObjectRequest {
        object: object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        ),
        cond: NO_READ,
        sse_customer: None,
    };
    let part_request = GetObjectPartRequest {
        object: object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        ),
        part_number: 1,
        cond: NO_READ,
        sse_customer: None,
    };
    let attributes_request = GetObjectAttributesRequest {
        object: object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        ),
        cond: NO_READ,
        want_parts: false,
        part_number_marker: None,
        max_parts: 1_000,
        sse_customer: None,
    };
    let metadata_request =
        object_version_request_with_expected_owner("bucket", "key", None, test_requester(), None);
    let delete_request = DeleteObjectRequest {
        object: object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        ),
        bypass_governance: false,
        cond: &DeleteCondition::None,
    };
    let delete_entries = [DeleteEntry {
        key: trusted_object_key("key"),
        version_id: None,
        cond: DeleteCondition::None,
    }];
    let delete_objects_request = DeleteObjectsRequest {
        bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
        entries: &delete_entries,
        bypass_governance: false,
    };

    let foreign_admission = foreign.admit_storage_route_for_request().unwrap();
    let canary_tags =
        "<Tagging><TagSet><Tag><Key>domain</Key><Value>canary</Value></Tag></TagSet></Tagging>";
    let put_tags_request = PutObjectTagsRequest {
        object: object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        ),
        tags: canary_tags,
    };
    let baseline_retention = ObjectRetention {
        mode: ObjectLockMode::Governance,
        retain_until_unix_seconds: Coordinator::current_unix_seconds().unwrap() + 3_600,
    };
    let put_retention_request = PutObjectRetentionRequest {
        object: object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        ),
        retention: baseline_retention,
        bypass_governance: false,
    };
    let put_legal_hold_request = PutObjectLegalHoldRequest {
        object: object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        ),
        legal_hold: LegalHoldStatus::On,
    };
    let put_acl_request = PutObjectAclRequest {
        object: object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        ),
        acl: PutObjectAclInput::Canned(PutObjectAcl::Private),
        policy_context: PutObjectPolicyContext::default()
            .with_default_canned_acl(PutObjectAcl::Private.policy_condition_value()),
    };
    foreign
        .put_object_tags_on_admitted_route(&foreign_admission, &put_tags_request)
        .unwrap();
    foreign
        .put_object_retention_on_admitted_route(&foreign_admission, &put_retention_request)
        .unwrap();
    foreign
        .put_object_legal_hold_on_admitted_route(&foreign_admission, &put_legal_hold_request)
        .unwrap();
    foreign
        .put_object_acl_on_admitted_route(&foreign_admission, &put_acl_request)
        .unwrap();
    foreign
        .head_object_on_admitted_route(&foreign_admission, &request)
        .unwrap();
    foreign
        .head_object_part_on_admitted_route(&foreign_admission, &part_request)
        .unwrap();
    foreign
        .get_object_attributes_on_admitted_route(&foreign_admission, &attributes_request)
        .unwrap();
    foreign
        .get_object_tags_on_admitted_route(&foreign_admission, &metadata_request)
        .unwrap();
    let baseline_acl = foreign
        .get_object_acl_on_admitted_route(&foreign_admission, &metadata_request)
        .unwrap();
    foreign
        .get_object_retention_on_admitted_route(&foreign_admission, &metadata_request)
        .unwrap();
    foreign
        .get_object_legal_hold_on_admitted_route(&foreign_admission, &metadata_request)
        .unwrap();

    let rejected_tags = PutObjectTagsRequest {
        object: object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        ),
        tags: "<Tagging><TagSet><Tag><Key>foreign</Key><Value>mutation</Value></Tag></TagSet></Tagging>",
    };
    let rejected_retention = PutObjectRetentionRequest {
        object: object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        ),
        retention: ObjectRetention {
            mode: ObjectLockMode::Governance,
            retain_until_unix_seconds: baseline_retention.retain_until_unix_seconds + 3_600,
        },
        bypass_governance: false,
    };
    let rejected_legal_hold = PutObjectLegalHoldRequest {
        object: object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        ),
        legal_hold: LegalHoldStatus::Off,
    };
    let rejected_acl = PutObjectAclRequest {
        object: object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        ),
        acl: PutObjectAclInput::Canned(PutObjectAcl::PublicRead),
        policy_context: PutObjectPolicyContext::default()
            .with_default_canned_acl(PutObjectAcl::PublicRead.policy_condition_value()),
    };

    for (operation, result) in [
        (
            "HeadObject",
            local
                .head_object_on_admitted_route(&foreign_admission, &request)
                .map(|_| ()),
        ),
        (
            "HeadObjectPart",
            local
                .head_object_part_on_admitted_route(&foreign_admission, &part_request)
                .map(|_| ()),
        ),
        (
            "GetObjectAttributes",
            local
                .get_object_attributes_on_admitted_route(&foreign_admission, &attributes_request)
                .map(|_| ()),
        ),
        (
            "GetObjectTagging",
            local
                .get_object_tags_on_admitted_route(&foreign_admission, &metadata_request)
                .map(|_| ()),
        ),
        (
            "GetObjectAcl",
            local
                .get_object_acl_on_admitted_route(&foreign_admission, &metadata_request)
                .map(|_| ()),
        ),
        (
            "GetObjectRetention",
            local
                .get_object_retention_on_admitted_route(&foreign_admission, &metadata_request)
                .map(|_| ()),
        ),
        (
            "GetObjectLegalHold",
            local
                .get_object_legal_hold_on_admitted_route(&foreign_admission, &metadata_request)
                .map(|_| ()),
        ),
        (
            "PutObjectTagging",
            local.put_object_tags_on_admitted_route(&foreign_admission, &rejected_tags),
        ),
        (
            "DeleteObjectTagging",
            local.delete_object_tags_on_admitted_route(&foreign_admission, &metadata_request),
        ),
        (
            "PutObjectRetention",
            local
                .put_object_retention_on_admitted_route(&foreign_admission, &rejected_retention)
                .map(|_| ()),
        ),
        (
            "PutObjectLegalHold",
            local
                .put_object_legal_hold_on_admitted_route(&foreign_admission, &rejected_legal_hold)
                .map(|_| ()),
        ),
        (
            "PutObjectAcl",
            local
                .put_object_acl_on_admitted_route(&foreign_admission, &rejected_acl)
                .map(|_| ()),
        ),
        (
            "DeleteObject",
            local
                .delete_object_on_admitted_route(&foreign_admission, &delete_request)
                .map(|_| ()),
        ),
        (
            "DeleteObjects",
            local
                .delete_objects_on_admitted_route(&foreign_admission, &delete_objects_request)
                .map(|_| ()),
        ),
    ] {
        let error = result.expect_err(operation);
        assert!(
            matches!(error, ServerError::OperationAborted),
            "{operation}: {error:?}"
        );
    }
    assert_eq!(
        foreign
            .get_object_tags_on_admitted_route(&foreign_admission, &metadata_request)
            .unwrap()
            .as_deref(),
        Some(canary_tags)
    );
    assert_eq!(
        foreign
            .get_object_retention_on_admitted_route(&foreign_admission, &metadata_request)
            .unwrap(),
        Some(baseline_retention)
    );
    assert_eq!(
        foreign
            .get_object_legal_hold_on_admitted_route(&foreign_admission, &metadata_request)
            .unwrap(),
        Some(LegalHoldStatus::On)
    );
    assert_eq!(
        foreign
            .get_object_acl_on_admitted_route(&foreign_admission, &metadata_request)
            .unwrap()
            .acl_grants,
        baseline_acl.acl_grants
    );
}

#[test]
fn retained_stream_cleanup_rejects_admission_from_an_unrelated_coordinator() {
    let tmp = test_util::tempdir();
    let cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let local_handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&cluster));
    let foreign_handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&cluster));
    let local = Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
        local_handle,
        "us-east-1".to_string(),
        None,
        test_sse_s3_provider(),
        BackgroundWorkerMode::none(),
    )
    .unwrap();
    let foreign = Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
        foreign_handle,
        "us-east-1".to_string(),
        None,
        test_sse_s3_provider(),
        BackgroundWorkerMode::none(),
    )
    .unwrap();
    assert!(Arc::ptr_eq(&local.storage_node(), &foreign.storage_node()));
    assert!(!local.shares_storage_route_admission_with(&foreign));

    local
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let session_id = begin_stream_put_test(&local, "bucket", "key").unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");

    let foreign_admission = foreign.admit_storage_route_for_request().unwrap();
    let Err(error) = local.retained_stream_upload_cleanup(&foreign_admission, &bucket, &key) else {
        panic!("foreign admission unexpectedly minted retained cleanup authority");
    };
    assert!(matches!(error, ServerError::OperationAborted), "{error:?}");
    cluster
        .load_stream_upload_session(&bucket, &key, &session_id)
        .expect("foreign admission must not abort the durable stream session");

    let local_admission = local.admit_storage_route_for_request().unwrap();
    let cleanup = local
        .retained_stream_upload_cleanup(&local_admission, &bucket, &key)
        .unwrap();
    local
        .abort_stream_upload_with_retained_cleanup(&cleanup, &session_id)
        .unwrap();
    assert!(matches!(
        cluster.load_stream_upload_session(&bucket, &key, &session_id),
        Err(storage::ObjectPgActionError::Metadata(
            storage::MetadataError::StreamSessionNotFound { .. }
        ))
    ));
}

#[test]
fn bucket_exists_rejects_stale_admission_from_an_unrelated_coordinator() {
    let initial_tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(initial_tmp.path(), &[0]);
    let local_handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let foreign_handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let local = Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
        local_handle.clone(),
        "us-east-1".to_string(),
        None,
        test_sse_s3_provider(),
        BackgroundWorkerMode::none(),
    )
    .unwrap();
    let foreign = Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
        foreign_handle,
        "us-east-1".to_string(),
        None,
        test_sse_s3_provider(),
        BackgroundWorkerMode::none(),
    )
    .unwrap();
    assert!(Arc::ptr_eq(&local.storage_node(), &foreign.storage_node()));
    assert!(!local.shares_storage_route_admission_with(&foreign));

    local
        .create_bucket_for_owner("default-owner", "old-bucket", false)
        .unwrap();
    let bucket = trusted_bucket_name("old-bucket");
    let foreign_admission = foreign.admit_storage_route_for_request().unwrap();

    let replacement_tmp = test_util::tempdir();
    let replacement =
        make_dynamic_runtime_map_candidate(open_test_storage_cluster(replacement_tmp.path(), &[0]));
    local_handle.install(Arc::clone(&replacement)).unwrap();
    assert!(Arc::ptr_eq(&local.storage_node(), &replacement));
    assert!(foreign
        .bucket_exists_on_admitted_route(&foreign_admission, &bucket)
        .unwrap());

    let error = local
        .bucket_exists_on_admitted_route(&foreign_admission, &bucket)
        .unwrap_err();
    assert!(matches!(error, ServerError::OperationAborted), "{error:?}");

    let local_admission = local.admit_storage_route_for_request().unwrap();
    assert!(!local
        .bucket_exists_on_admitted_route(&local_admission, &bucket)
        .unwrap());
}

#[test]
fn bucket_cors_read_rejects_stale_admission_from_an_unrelated_coordinator() {
    let initial_tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(initial_tmp.path(), &[0]);
    let local_handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let foreign_handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let local = Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
        local_handle.clone(),
        "us-east-1".to_string(),
        None,
        test_sse_s3_provider(),
        BackgroundWorkerMode::none(),
    )
    .unwrap();
    let foreign = Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
        foreign_handle,
        "us-east-1".to_string(),
        None,
        test_sse_s3_provider(),
        BackgroundWorkerMode::none(),
    )
    .unwrap();
    assert!(Arc::ptr_eq(&local.storage_node(), &foreign.storage_node()));
    assert!(!local.shares_storage_route_admission_with(&foreign));

    local
        .create_bucket_for_owner("default-owner", "cors-bucket", false)
        .unwrap();
    let old_cors = "<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>https://old.example</AllowedOrigin></CORSRule></CORSConfiguration>";
    local
        .put_bucket_cors(&put_bucket_config_request_with_expected_owner(
            "cors-bucket",
            old_cors,
            test_requester(),
            None,
        ))
        .unwrap();
    let bucket = trusted_bucket_name("cors-bucket");
    let foreign_admission = foreign.admit_storage_route_for_request().unwrap();

    let replacement_tmp = test_util::tempdir();
    let replacement =
        make_dynamic_runtime_map_candidate(open_test_storage_cluster(replacement_tmp.path(), &[0]));
    local_handle.install(Arc::clone(&replacement)).unwrap();
    local
        .create_bucket_for_owner("default-owner", "cors-bucket", false)
        .unwrap();
    assert_eq!(
        foreign
            .load_bucket_cors_config(&foreign_admission, &bucket)
            .unwrap()
            .as_deref(),
        Some(old_cors)
    );

    let error = local
        .load_bucket_cors_config(&foreign_admission, &bucket)
        .unwrap_err();
    assert!(matches!(error, ServerError::OperationAborted), "{error:?}");

    let local_admission = local.admit_storage_route_for_request().unwrap();
    assert_eq!(
        local
            .load_bucket_cors_config(&local_admission, &bucket)
            .unwrap(),
        None
    );
}

#[test]
fn maintenance_worker_operations_sample_epoch_refreshed_runtime_map() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    let stale_lifecycle_runtime = coord.read_runtime();

    install_same_store_next_epoch_runtime_map(&handle, &initial, tmp.path());
    let refreshed = handle.current();
    assert!(refreshed.cluster_epoch() > initial.cluster_epoch());

    let lifecycle_runtime =
        super::runtime::lifecycle_runtime_for_sweep(&handle, &stale_lifecycle_runtime);
    assert!(Arc::ptr_eq(lifecycle_runtime.storage_node(), &refreshed));

    let repair_cluster = super::runtime::shard_repair_cluster_for_work(&handle);
    assert!(Arc::ptr_eq(&repair_cluster, &refreshed));
}

#[test]
fn shard_backfill_worker_uses_refreshed_runtime_map_handle() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let _coord =
        setup_coordinator_with_only_shard_backfill_worker(handle.clone(), Arc::clone(&initial));

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let segment_okh = [0xBF; 16];
    let segment_vid = GenerationId::MIN;
    let payload = b"backfill worker must use refreshed runtime map";
    let written_segment = initial
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            segment_vid,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    initial
        .test_register_payload_shard_acks(
            written_segment.data_pg_id,
            &written_segment.written_shards,
        )
        .unwrap();

    let source_epoch = initial.cluster_epoch();
    let desired_epoch = ClusterEpoch::new(source_epoch.get() + 1).unwrap();
    let source_route = initial
        .local_pg_route(PgId::new(written_segment.data_pg_id))
        .expect("test PG should have a local route");
    let historical_route = storage::control_plane::PgRouteSnapshot::reconstructed(
        source_epoch,
        source_route.pg_id(),
        source_route.primary_node_id(),
        source_route.acting_set().to_vec(),
        source_route.state(),
    );
    let ec_shape = initial.default_payload_ec_shape();
    let node_count = u32::from(ec_shape.k) + u32::from(ec_shape.m);
    let configs = (0..node_count)
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                storage::NodeId::new(node_id),
                tmp.path().join(format!("node-{node_id:04}")),
            )
        })
        .collect::<Vec<_>>();
    let mut refreshed_map = LocalClusterMap::open_frontend_placeholder_with_configs_and_epoch(
        storage::NodeId::new(0),
        configs,
        &[0],
        ec_shape,
        desired_epoch,
    )
    .unwrap();
    refreshed_map.test_install_historical_pg_routes([historical_route]);
    refreshed_map.test_set_route_map_validity(long_lived_test_route_map_validity());
    let refreshed = StorageCluster::from_local_map(Arc::new(refreshed_map)).unwrap();
    handle.install(Arc::clone(&refreshed)).unwrap();

    let work_item = PlacedSegmentShardBackfillWorkItem {
        request: SegmentStoredBytesRequest {
            data_pg_id: written_segment.data_pg_id,
            segment_okh,
            segment_vid,
            stored_size: payload.len(),
            segment_crc64: checksum::crc64::checksum(payload),
            ec: written_segment.ec,
        },
        source_cluster_epoch: source_epoch,
        desired_cluster_epoch: desired_epoch,
    };
    assert!(
        initial
            .backfill_placed_segment_payload_shards_for_work_item(&work_item)
            .is_err(),
        "the stale initial cluster must not be able to reconstruct the desired epoch"
    );
    refreshed
        .record_placed_segment_shard_backfill(&work_item, None)
        .unwrap();

    let start = std::time::Instant::now();
    loop {
        let rows = refreshed
            .list_placed_segment_shard_backfills(written_segment.data_pg_id)
            .unwrap();
        if rows.is_empty() {
            break;
        }
        assert!(
            start.elapsed() < TEST_EVENT_TIMEOUT,
            "shard backfill worker did not complete durable row after runtime-map refresh: {rows:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn shard_backfill_candidate_scanner_retains_cursor_across_runtime_map_replacement() {
    let tmp = test_util::tempdir();
    let ec_shape = EcShape { k: 4, m: 2 };
    let pg_id = PgId::new(0);
    let source_epoch = ClusterEpoch::INITIAL;
    let source_acting_set = (0..6).map(NodeId::new).collect::<Vec<_>>();
    let desired_acting_set = [0, 2, 3, 4, 5, 6]
        .into_iter()
        .map(NodeId::new)
        .collect::<Vec<_>>();
    let configs = (0..7)
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                NodeId::new(node_id),
                tmp.path().join(format!("node-{node_id:04}")),
            )
        })
        .collect::<Vec<_>>();
    let source_route = storage::control_plane::PgRouteSnapshot::reconstructed(
        source_epoch,
        pg_id,
        NodeId::new(0),
        source_acting_set,
        PgState::Active,
    );
    let source_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs.clone(),
        &[pg_id.get()],
        ec_shape,
        source_epoch,
        [LocalPgRoute::from(&source_route)],
    )
    .unwrap();
    let source_cluster = StorageCluster::from_local_map(Arc::new(source_map)).unwrap();
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&source_cluster));
    coord
        .create_bucket_for_owner("default-owner", "backfill-cursor-bucket", false)
        .unwrap();
    let bucket = trusted_bucket_name("backfill-cursor-bucket");

    let put_segment = |key: &str, payload: &[u8]| {
        let put = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "backfill-cursor-bucket",
                    key,
                    test_requester(),
                    None,
                ),
                data: payload,
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        let segment = source_cluster
            .test_get_object_segments(&bucket, &trusted_object_key(key), put.version_id)
            .unwrap()
            .pop()
            .expect("direct PUT should record one segment");
        let encryption = match source_cluster
            .test_get_object_meta(&bucket, &trusted_object_key(key))
            .unwrap()
        {
            StoredObject::Live(object) => object.encryption,
            StoredObject::DeleteMarker(marker) => {
                panic!("direct PUT unexpectedly stored delete marker: {marker:?}")
            }
        };
        SegmentStoredBytesRequest {
            data_pg_id: segment.data_pg_id,
            segment_okh: segment.segment_okh,
            segment_vid: segment.segment_vid,
            stored_size: usize::try_from(segment.size)
                .unwrap()
                .checked_add(encryption.segment_ciphertext_extra_len())
                .unwrap(),
            segment_crc64: segment.segment_crc64,
            ec: EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
        }
    };
    let first_request = put_segment("first", b"first historical payload");
    let second_request = put_segment("second", b"second historical payload");
    let request_order = |request: &SegmentStoredBytesRequest| {
        (
            request.segment_vid.get(),
            request.segment_okh,
            request.stored_size,
            request.segment_crc64,
            request.ec.k,
            request.ec.m,
        )
    };
    let (complete_request, missing_request) =
        if request_order(&first_request) < request_order(&second_request) {
            (first_request, second_request)
        } else {
            (second_request, first_request)
        };

    let desired_epoch = ClusterEpoch::new(source_epoch.get() + 1).unwrap();
    let desired_route = storage::control_plane::PgRouteSnapshot::reconstructed(
        desired_epoch,
        pg_id,
        NodeId::new(0),
        desired_acting_set.clone(),
        PgState::Active,
    );
    let mut desired_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs.clone(),
        &[pg_id.get()],
        ec_shape,
        desired_epoch,
        [LocalPgRoute::from(&desired_route)],
    )
    .unwrap();
    desired_map.test_install_historical_pg_routes([source_route.clone()]);
    desired_map.test_set_route_map_validity(long_lived_test_route_map_validity());
    let desired_cluster = StorageCluster::from_local_map(Arc::new(desired_map)).unwrap();
    let complete_source_health = desired_cluster
        .placed_segment_payload_shard_health_for_pg_route_snapshot(&source_route, complete_request)
        .unwrap();
    assert_eq!(
        complete_source_health.risk,
        storage::PlacedSegmentShardSetRisk::Healthy,
        "source payload should be readable through the replacement map: {complete_source_health:?}"
    );
    desired_cluster
        .backfill_placed_segment_payload_shard_direct_copies(
            &source_route,
            &desired_route,
            complete_request,
        )
        .unwrap();
    assert!(!desired_cluster
        .placed_segment_payload_shard_backfill_plan(&source_route, &desired_route, missing_request,)
        .unwrap()
        .is_complete());

    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&desired_cluster));
    let mut scanner = super::runtime::ShardBackfillCandidateScanner::default();
    let first_scan = scanner.scan_with_limit(&handle, 1).unwrap();
    assert_eq!(first_scan.already_complete, 1);
    assert_eq!(first_scan.enqueued, 0);
    assert!(first_scan.limit_reached);

    let replacement_epoch = ClusterEpoch::new(desired_epoch.get() + 1).unwrap();
    let replacement_route = storage::control_plane::PgRouteSnapshot::reconstructed(
        replacement_epoch,
        pg_id,
        NodeId::new(0),
        desired_acting_set,
        PgState::Active,
    );
    let mut replacement_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs,
        &[pg_id.get()],
        ec_shape,
        replacement_epoch,
        [LocalPgRoute::from(&replacement_route)],
    )
    .unwrap();
    replacement_map.test_install_historical_pg_routes([source_route]);
    replacement_map.test_set_route_map_validity(long_lived_test_route_map_validity());
    let replacement = StorageCluster::from_local_map(Arc::new(replacement_map)).unwrap();
    handle.install(Arc::clone(&replacement)).unwrap();
    assert!(Arc::ptr_eq(&handle.current(), &replacement));

    let second_scan = scanner.scan_with_limit(&handle, 1).unwrap();
    assert_eq!(second_scan.enqueued, 1);
    assert!(replacement
        .placed_segment_shard_backfill_exists(&PlacedSegmentShardBackfillWorkItem {
            request: missing_request,
            source_cluster_epoch: source_epoch,
            desired_cluster_epoch: replacement_epoch,
        })
        .unwrap());
}

#[test]
fn shard_backfill_worker_executes_remote_storage_node_work() {
    let tmp = test_util::tempdir();
    let source_node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let desired_node_ids = [
        NodeId::new(0),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
        NodeId::new(6),
    ];
    let all_node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
        NodeId::new(6),
    ];
    let ec_shape = EcShape { k: 4, m: 2 };
    let pg_id = PgId::new(0);
    let mut authority = storage::control_plane::SingleAuthorityControlPlane::open(
        storage::control_plane::FileControlPlaneStore::new(tmp.path().join("control-plane.state")),
    )
    .unwrap();
    authority
        .bootstrap_initial_cluster_map(
            source_node_ids
                .iter()
                .map(|node_id| {
                    (
                        *node_id,
                        tmp.path()
                            .join("sockets")
                            .join(format!("node-{}.sock", node_id.as_u32()))
                            .to_string_lossy()
                            .into_owned(),
                    )
                })
                .collect(),
            vec![pg_id],
        )
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_node_membership(
            NodeId::new(6),
            storage::control_plane::NodeMembershipState::Active,
        )
        .unwrap();
    authority
        .set_pg_acting_set(pg_id, desired_node_ids.to_vec())
        .unwrap();
    let source_route = authority
        .snapshot()
        .reconstructed_pg_route_at_epoch(pg_id, source_epoch)
        .unwrap();
    let source_route = storage::control_plane::PgRouteSnapshot::reconstructed(
        source_route.cluster_epoch(),
        source_route.pg_id(),
        source_route.primary_node_id(),
        source_route.acting_set().to_vec(),
        PgState::Active,
    );
    let desired_route = authority
        .snapshot()
        .reconstructed_pg_route_at_epoch(pg_id, authority.snapshot().cluster_epoch())
        .unwrap();
    let desired_route = storage::control_plane::PgRouteSnapshot::reconstructed(
        desired_route.cluster_epoch(),
        desired_route.pg_id(),
        desired_route.primary_node_id(),
        desired_route.acting_set().to_vec(),
        PgState::Active,
    );
    assert_eq!(source_route.acting_set(), &source_node_ids);
    assert_eq!(desired_route.acting_set(), &desired_node_ids);

    let configs: Vec<_> = all_node_ids
        .iter()
        .map(|&node_id| {
            LocalNodeStoreConfig::new(
                node_id,
                tmp.path()
                    .join("storage")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect();
    let source_map = Arc::new(
        LocalClusterMap::open_frontend_with_configs_and_pg_routes(
            NodeId::new(0),
            configs.iter().take(source_node_ids.len()).cloned(),
            &[pg_id.get()],
            ec_shape,
            source_route.cluster_epoch(),
            [LocalPgRoute::from(&source_route)],
        )
        .unwrap(),
    );
    let source_cluster = StorageCluster::from_local_map(Arc::clone(&source_map)).unwrap();
    let bucket = trusted_bucket_name("remote-backfill-bucket");
    let key = trusted_object_key("remote-backfill-key");
    let segment_okh = [0xD7; 16];
    let segment_vid = GenerationId::MIN;
    let payload = b"remote storage-node shard backfill worker";
    let written_segment = source_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            segment_vid,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    source_cluster
        .test_register_payload_shard_acks(
            written_segment.data_pg_id,
            &written_segment.written_shards,
        )
        .unwrap();
    let work_item = PlacedSegmentShardBackfillWorkItem {
        request: SegmentStoredBytesRequest {
            data_pg_id: written_segment.data_pg_id,
            segment_okh,
            segment_vid,
            stored_size: payload.len(),
            segment_crc64: checksum::crc64::checksum(payload),
            ec: written_segment.ec,
        },
        source_cluster_epoch: source_route.cluster_epoch(),
        desired_cluster_epoch: desired_route.cluster_epoch(),
    };
    drop(source_cluster);
    drop(source_map);

    let socket_dir = tmp.path().join("sockets-desired");
    make_private_socket_dir(&socket_dir);
    let mut server_threads = Vec::new();
    let mut wake_socket_paths = Vec::new();
    let stop = Arc::new(AtomicBool::new(false));
    let mut client_configs = Vec::new();
    for config in &configs {
        let socket_path = socket_dir.join(format!("node-{}.sock", config.node_id().as_u32()));
        let server_config = StorageNodeProcessConfig::new(StorageNodeProcessConfigParts {
            node_id: config.node_id(),
            cluster_epoch: desired_route.cluster_epoch(),
            route_map_validity: RouteMapValidity::Forever,
            data_dir: config.data_dir().to_path_buf(),
            default_ec_shape: ec_shape,
            pg_ids: vec![pg_id.get()],
            socket_path: socket_path.clone(),
            pg_routes: vec![StorageNodePgRoute::from(&desired_route)],
            historical_pg_routes: vec![StorageNodePgRoute::from(&source_route)],
            pending_metadata_command_recoveries: Vec::new(),
        })
        .unwrap();
        let server = Arc::new(StorageNodeServer::bind(server_config).unwrap());
        for _ in 0..4 {
            server_threads.push(spawn_storage_node_server_loop(
                Arc::clone(&server),
                Arc::clone(&stop),
            ));
            wake_socket_paths.push(socket_path.clone());
        }
        client_configs.push(LocalUnixStorageNodeClientConfig::new(
            config.node_id(),
            socket_path.clone(),
        ));
    }

    let mut desired_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs,
        &[pg_id.get()],
        ec_shape,
        desired_route.cluster_epoch(),
        [LocalPgRoute::from(&desired_route)],
    )
    .unwrap();
    desired_map.test_install_historical_pg_routes([source_route.clone()]);
    desired_map
        .install_unix_storage_node_clients(client_configs)
        .unwrap();
    let desired_cluster = StorageCluster::from_local_map(Arc::new(desired_map)).unwrap();
    let source_health = desired_cluster
        .placed_segment_payload_shard_health_for_pg_route_snapshot(&source_route, work_item.request)
        .unwrap();
    assert_eq!(
        source_health.risk,
        storage::PlacedSegmentShardSetRisk::Healthy
    );
    let before = desired_cluster
        .placed_segment_payload_shard_health_for_pg_route_snapshot(
            &desired_route,
            work_item.request,
        )
        .unwrap();
    assert!(
        matches!(
            before.risk,
            storage::PlacedSegmentShardSetRisk::Unrecoverable
        ),
        "expected unrecoverable desired-route health before backfill, got {before:?}"
    );
    assert!(before
        .shards
        .iter()
        .any(|shard| shard.location.node_id() == NodeId::new(6) && !shard.validation.is_valid()));

    desired_cluster
        .record_placed_segment_shard_backfill(&work_item, None)
        .unwrap();
    super::runtime::run_one_placed_segment_shard_backfill_for_test(
        &desired_cluster,
        "remote-backfill-test",
    );

    let rows = desired_cluster
        .list_placed_segment_shard_backfills(written_segment.data_pg_id)
        .unwrap();
    assert!(rows.is_empty(), "backfill row should complete: {rows:?}");
    let after = desired_cluster
        .placed_segment_payload_shard_health_for_pg_route_snapshot(
            &desired_route,
            work_item.request,
        )
        .unwrap();
    assert_eq!(after.risk, storage::PlacedSegmentShardSetRisk::Healthy);
    assert!(after
        .shards
        .iter()
        .filter(|shard| shard.location.node_id() == NodeId::new(6))
        .all(|shard| shard.validation.is_valid()));

    stop_storage_node_server_loops(stop, &wake_socket_paths, server_threads);
}

#[test]
fn get_object_pins_runtime_map_for_snapshot_and_body() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"pinned-runtime-map",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let candidate_tmp = test_util::tempdir();
    let candidate = make_dynamic_runtime_map_candidate(open_test_storage_cluster(
        candidate_tmp.path(),
        &[0, 1],
    ));
    let publication_thread = Arc::new(Mutex::new(None));
    let hook_publication_thread = Arc::clone(&publication_thread);
    let hook_handle = handle.clone();
    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some(("bucket".to_string(), "key".to_string())),
        after_object_read_snapshot: Some(Arc::new(move || {
            let publishing_handle = hook_handle.clone();
            let publishing_candidate = Arc::clone(&candidate);
            let thread = thread::spawn(move || {
                publishing_handle.install(publishing_candidate).unwrap();
            });
            *hook_publication_thread.lock().unwrap() = Some(thread);
            hook_handle.test_wait_until_route_publication_is_pending();
        })),
        ..ReclamationTestHooks::default()
    });

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

    publication_thread
        .lock()
        .unwrap()
        .take()
        .expect("GetObject hook should start route publication")
        .join()
        .unwrap();

    assert_eq!(result.body.read_all().unwrap(), b"pinned-runtime-map");
}

#[test]
fn copy_object_pins_runtime_map_for_source_and_destination() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"copy-pinned-runtime-map",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let candidate_tmp = test_util::tempdir();
    let candidate = make_dynamic_runtime_map_candidate(open_test_storage_cluster(
        candidate_tmp.path(),
        &[0, 1],
    ));
    let publication_thread = Arc::new(Mutex::new(None));
    let hook_publication_thread = Arc::clone(&publication_thread);
    let handle_for_hook = handle.clone();
    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some(("bucket".to_string(), "src".to_string())),
        after_object_read_snapshot: Some(Arc::new(move || {
            let publishing_handle = handle_for_hook.clone();
            let publishing_candidate = Arc::clone(&candidate);
            let thread = thread::spawn(move || {
                publishing_handle.install(publishing_candidate).unwrap();
            });
            *hook_publication_thread.lock().unwrap() = Some(thread);
            handle_for_hook.test_wait_until_route_publication_is_pending();
        })),
        ..ReclamationTestHooks::default()
    });

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

    publication_thread
        .lock()
        .unwrap()
        .take()
        .expect("CopyObject hook should start route publication")
        .join()
        .unwrap();

    handle
        .install(make_dynamic_runtime_map_candidate(initial))
        .unwrap();
    let result = coord
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

    assert_eq!(result.body.read_all().unwrap(), b"copy-pinned-runtime-map");
}

#[test]
fn object_metadata_pins_runtime_map_after_policy_context_load() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"object-metadata-pinned-runtime-map",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let candidate_tmp = test_util::tempdir();
    let candidate = make_dynamic_runtime_map_candidate(open_test_storage_cluster(
        candidate_tmp.path(),
        &[0, 1],
    ));
    let hook_handle = handle.clone();
    let hook_invocations = Arc::new(AtomicUsize::new(0));
    let hook_invocations_for_hook = Arc::clone(&hook_invocations);
    let publication_threads = Arc::new(Mutex::new(Vec::new()));
    let hook_publication_threads = Arc::clone(&publication_threads);
    let _serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = coord.install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
        bucket: Some("bucket".to_string()),
        after_object_metadata_policy_context: Some(Arc::new(move || {
            hook_invocations_for_hook.fetch_add(1, Ordering::SeqCst);
            let publishing_handle = hook_handle.clone();
            let publishing_candidate = Arc::clone(&candidate);
            let thread = thread::spawn(move || {
                publishing_handle.install(publishing_candidate).unwrap();
            });
            hook_publication_threads.lock().unwrap().push(thread);
            hook_handle.test_wait_until_route_publication_is_pending();
        })),
        ..BucketWriteHandleTestHooks::default()
    });

    put_object_tags_test(
        &coord,
        "bucket",
        "key",
        None,
        "<Tagging><TagSet><Tag><Key>pin</Key><Value>metadata</Value></Tag></TagSet></Tagging>",
        test_requester(),
        None,
    )
    .unwrap();
    publication_threads
        .lock()
        .unwrap()
        .pop()
        .expect("PutObjectTagging hook should start route publication")
        .join()
        .unwrap();

    handle
        .install(make_dynamic_runtime_map_candidate(initial))
        .unwrap();
    let tags = get_object_tags_test(&coord, "bucket", "key", None, test_requester(), None)
        .unwrap()
        .unwrap();
    publication_threads
        .lock()
        .unwrap()
        .pop()
        .expect("GetObjectTagging hook should start route publication")
        .join()
        .unwrap();
    assert_eq!(hook_invocations.load(Ordering::SeqCst), 2);
    assert!(tags.contains("<Key>pin</Key>"));
    assert!(tags.contains("<Value>metadata</Value>"));
}

#[test]
fn delete_object_pins_runtime_map_after_authorization() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"delete-pinned-runtime-map",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let candidate_tmp = test_util::tempdir();
    let candidate = make_dynamic_runtime_map_candidate(open_test_storage_cluster(
        candidate_tmp.path(),
        &[0, 1],
    ));
    let hook_handle = handle.clone();
    let _serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = coord.install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
        bucket: Some("bucket".to_string()),
        after_loaded: Some(Arc::new(move || {
            hook_handle.install(Arc::clone(&candidate)).unwrap();
        })),
        ..BucketWriteHandleTestHooks::default()
    });

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

    handle
        .install(make_dynamic_runtime_map_candidate(initial))
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
fn delete_bucket_pins_runtime_map_after_authorization() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let candidate_tmp = test_util::tempdir();
    let candidate = make_dynamic_runtime_map_candidate(open_test_storage_cluster(
        candidate_tmp.path(),
        &[0, 1],
    ));
    let hook_handle = handle.clone();
    let publication_threads = Arc::new(Mutex::new(Vec::new()));
    let hook_publication_threads = Arc::clone(&publication_threads);
    let _serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = coord.install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
        bucket: Some("bucket".to_string()),
        after_loaded: Some(Arc::new(move || {
            let publishing_handle = hook_handle.clone();
            let publishing_candidate = Arc::clone(&candidate);
            let thread = thread::spawn(move || {
                publishing_handle.install(publishing_candidate).unwrap();
            });
            hook_publication_threads.lock().unwrap().push(thread);
            hook_handle.test_wait_until_route_publication_is_pending();
        })),
        ..BucketWriteHandleTestHooks::default()
    });

    delete_bucket_test(&coord, "bucket").unwrap();
    publication_threads
        .lock()
        .unwrap()
        .pop()
        .expect("DeleteBucket hook should start route publication")
        .join()
        .unwrap();

    handle
        .install(make_dynamic_runtime_map_candidate(initial))
        .unwrap();
    let err = coord
        .head_bucket(&bucket_request_with_expected_owner(
            "bucket",
            test_requester(),
            None,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::BucketNotFound { .. }));
}

#[test]
fn reclaim_worker_follows_runtime_map_refresh_for_bucket_finalize() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = setup_coordinator_with_only_reclaim_worker(handle.clone(), Arc::clone(&initial));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    install_same_store_same_epoch_runtime_map(&handle, &initial, tmp.path());
    thread::sleep(Duration::from_millis(250));

    delete_bucket_test(&coord, "bucket").unwrap();

    let bucket = trusted_bucket_name("bucket");
    let deadline = Instant::now() + TEST_EVENT_TIMEOUT;
    loop {
        match coord.storage_node().test_head_bucket_raw(&bucket) {
            Err(storage::BucketSnapshotLoadError::Metadata(
                storage::MetadataError::BucketNotFound { .. },
            )) => break,
            Ok(info) if Instant::now() < deadline => {
                assert_eq!(info.state, storage::BucketState::Deleting);
                thread::sleep(Duration::from_millis(10));
            }
            Ok(info) => {
                panic!(
                    "reclaim worker did not finalize deleting bucket after runtime-map refresh: {info:?}"
                );
            }
            Err(err) => {
                panic!("unexpected bucket metadata error while waiting for finalize: {err:?}")
            }
        }
    }
}

#[test]
fn reclaim_worker_resamples_runtime_map_after_dequeue() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("reclaim-work-dequeued-before-route-execution");

    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let root = storage::BucketDeleteFinalizeRoot {
        bucket: trusted_bucket_name("reclaim-refresh-after-dequeue"),
        bucket_incarnation_generation: 1,
    };
    initial.enqueue_bucket_delete_finalize(root);

    let gate = DeterministicFaultGate::new(TOKEN);
    let gate_for_hook = Arc::clone(&gate);
    let (observed_cluster_tx, observed_cluster_rx) = mpsc::channel();
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target_reclaim_worker_registry_key: Some(initial.process_local_registry_key()),
        after_reclaim_work_dequeued: Some(Arc::new(move || {
            gate_for_hook.wait_at(TOKEN);
        })),
        before_reclaim_work_execute: Some(Arc::new(move |storage_cluster| {
            let _ = observed_cluster_tx.send(storage_cluster);
        })),
        ..ReclamationTestHooks::default()
    });
    let _worker = setup_coordinator_with_only_reclaim_worker(handle.clone(), Arc::clone(&initial));
    let _gate_release_guard = gate.release_on_drop();
    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);

    install_same_store_same_epoch_runtime_map_with_primary(
        &handle,
        &initial,
        tmp.path(),
        NodeId::new(1),
    );
    let expected_cluster = handle.current();
    gate.release();

    let observed_cluster = observed_cluster_rx
        .recv_timeout(TEST_EVENT_TIMEOUT)
        .unwrap();
    assert!(
        Arc::ptr_eq(&observed_cluster, &expected_cluster),
        "reclaim execution must use the runtime map published after work was dequeued"
    );
}

#[test]
fn deferred_bucket_finalize_clears_its_original_runtime_map_queue_owner() {
    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let tmp = test_util::tempdir();
    let pg_ids = (0..32).collect::<Vec<_>>();
    let initial = open_test_storage_cluster(tmp.path(), &pg_ids);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let bucket = trusted_bucket_name("deferred-finalize-runtime-refresh");
    let direct_coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&initial));
    direct_coord
        .create_bucket_for_owner("default-owner", bucket.as_str(), false)
        .unwrap();
    let bucket_info = initial.test_head_bucket_raw(&bucket).unwrap();
    drop(direct_coord);
    initial
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
    let replacement = open_test_storage_cluster(tmp.path(), &pg_ids);
    replacement.test_store_route_map_validity(long_lived_test_route_map_validity());
    let root = storage::BucketDeleteFinalizeRoot {
        bucket,
        bucket_incarnation_generation: bucket_info.bucket_incarnation_generation,
    };
    initial.enqueue_bucket_delete_finalize(root.clone());

    assert_ne!(
        initial.process_local_registry_key(),
        replacement.process_local_registry_key(),
        "the replacement must own an independent reclaim queue"
    );

    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_hook = Arc::clone(&attempts);
    let handle_for_hook = handle.clone();
    let replacement_for_hook = Arc::clone(&replacement);
    let duplicate_root = root.clone();
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target_reclaim_worker_registry_key: Some(initial.process_local_registry_key()),
        before_reclaim_work_execute: Some(Arc::new(move |_execution_cluster| {
            if attempts_for_hook.fetch_add(1, Ordering::SeqCst) == 0 {
                handle_for_hook
                    .install(Arc::clone(&replacement_for_hook))
                    .unwrap();
                replacement_for_hook.enqueue_bucket_delete_finalize(duplicate_root.clone());
            }
        })),
        ..ReclamationTestHooks::default()
    });
    let _worker = setup_coordinator_with_only_reclaim_worker(handle, Arc::clone(&initial));

    let deadline = Instant::now() + TEST_EVENT_TIMEOUT;
    while initial.test_bucket_delete_finalize_outstanding_depth() != 0 && Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        attempts.load(Ordering::SeqCst) >= 2,
        "the finalizer must retry against the refreshed runtime map"
    );
    assert_eq!(
        initial.test_bucket_delete_finalize_outstanding_depth(),
        0,
        "terminal deferred work must clear the generation that originally owned the queue item"
    );
    assert_eq!(
        replacement.test_bucket_delete_finalize_outstanding_depth(),
        0,
        "duplicate work must acknowledge the replacement generation's outstanding root"
    );
}

#[test]
fn deferred_object_reclaim_clears_original_and_duplicate_runtime_map_queue_owners() {
    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "deferred-object-reclaim-refresh", false)
        .unwrap();
    let bucket = trusted_bucket_name("deferred-object-reclaim-refresh");
    let key = trusted_object_key("key");
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket.as_str(),
                key.as_str(),
                test_requester(),
                None,
            ),
            data: b"first payload",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let generation_id = initial
        .test_get_object_meta(&bucket, &key)
        .unwrap()
        .into_live()
        .expect("first put should create a live object")
        .generation_id;
    let payload_lease = initial
        .acquire_object_payload_lease(&bucket, &key, generation_id)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket.as_str(),
                key.as_str(),
                test_requester(),
                None,
            ),
            data: b"replacement payload",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    drop(coord);

    let replacement = open_test_storage_cluster(tmp.path(), &[0, 1]);
    replacement.test_store_route_map_validity(long_lived_test_route_map_validity());
    assert_ne!(
        initial.process_local_registry_key(),
        replacement.process_local_registry_key(),
        "the replacement must own an independent reclaim queue"
    );
    assert_eq!(initial.test_object_payload_reclaim_outstanding_depth(), 1);

    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_hook = Arc::clone(&attempts);
    let handle_for_hook = handle.clone();
    let replacement_for_hook = Arc::clone(&replacement);
    let duplicate_bucket = bucket.clone();
    let duplicate_key = key.clone();
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target_reclaim_worker_registry_key: Some(initial.process_local_registry_key()),
        before_reclaim_work_execute: Some(Arc::new(move |_execution_cluster| {
            if attempts_for_hook.fetch_add(1, Ordering::SeqCst) == 0 {
                handle_for_hook
                    .install(Arc::clone(&replacement_for_hook))
                    .unwrap();
                replacement_for_hook.enqueue_object_payload_reclaim(
                    &duplicate_bucket,
                    &duplicate_key,
                    generation_id,
                );
            }
        })),
        ..ReclamationTestHooks::default()
    });
    let _worker = setup_coordinator_with_only_reclaim_worker(handle, Arc::clone(&initial));

    let deadline = Instant::now() + TEST_EVENT_TIMEOUT;
    while (initial.test_object_payload_reclaim_outstanding_depth() != 0
        || replacement.test_object_payload_reclaim_outstanding_depth() != 0)
        && Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(10));
    }
    drop(payload_lease);
    assert!(
        attempts.load(Ordering::SeqCst) >= 2,
        "the object reclaim must retry against the refreshed runtime map"
    );
    assert_eq!(
        initial.test_object_payload_reclaim_outstanding_depth(),
        0,
        "terminal deferred reclaim must clear its original generation"
    );
    assert_eq!(
        replacement.test_object_payload_reclaim_outstanding_depth(),
        0,
        "duplicate reclaim must acknowledge the replacement generation"
    );
}

#[test]
fn stream_session_sweeper_follows_runtime_map_refresh_for_durable_cleanup() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0]);
    initial.test_store_route_map_validity(long_lived_test_route_map_validity());
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        setup_coordinator_with_only_stream_session_worker(handle.clone(), Arc::clone(&initial));
    coord
        .create_bucket_for_owner("default-owner", "stream-cleanup-refresh", false)
        .unwrap();

    let bucket = trusted_bucket_name("stream-cleanup-refresh");
    let key = trusted_object_key("key");
    let session_id = storage::SessionId::try_from("81818181818181818181818181818181").unwrap();
    let cleanup_after = storage::clock::current_time_millis().saturating_add(100);
    initial
        .create_put_object_stream_session_record_with_cleanup_deadline(
            &bucket,
            &key,
            &session_id,
            storage::ObjectEncryption::None,
            Some(cleanup_after),
        )
        .unwrap();

    install_same_store_same_epoch_runtime_map(&handle, &initial, tmp.path());
    assert!(!Arc::ptr_eq(&coord.storage_node(), &initial));

    let deadline = Instant::now() + TEST_EVENT_TIMEOUT;
    loop {
        match coord
            .storage_node()
            .load_stream_upload_session(&bucket, &key, &session_id)
        {
            Err(storage::ObjectPgActionError::Metadata(
                storage::MetadataError::StreamSessionNotFound { .. },
            )) => break,
            Ok(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
            Ok(_) => panic!("refreshed-map stream-session sweeper did not finish durable cleanup"),
            Err(error) => panic!("unexpected durable stream cleanup error: {error:?}"),
        }
    }
    assert!(matches!(
        coord
            .storage_node()
            .test_object_generation_reservation_for(&bucket, &key, &session_id),
        Err(storage::ObjectPgActionError::Metadata(
            storage::MetadataError::ObjectGenerationReservationNotFound { .. }
        ))
    ));
}

#[test]
fn reclaim_worker_retries_bucket_delete_begin_after_early_route_map_failure() {
    let tmp = test_util::tempdir();
    let bucket = trusted_bucket_name("bucket-delete-begin-retry-after-route-refresh");
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let direct_coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&initial));
    direct_coord
        .create_bucket_for_owner("default-owner", bucket.as_str(), false)
        .unwrap();
    let bucket_identity = initial.test_head_bucket_raw(&bucket).unwrap();

    let expired = same_store_cluster_with_route_map_validity(
        &initial,
        tmp.path(),
        RouteMapValidity::until_ms(1).unwrap(),
    );
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&expired));
    let _coord = setup_coordinator_with_only_reclaim_worker(handle.clone(), Arc::clone(&expired));

    expired.test_enqueue_bucket_delete_begin(
        &bucket,
        bucket_identity.bucket_execution_generation,
        bucket_identity.bucket_incarnation_generation,
    );
    thread::sleep(Duration::from_millis(350));

    let active_before_refresh = initial.test_head_bucket_raw(&bucket).unwrap();
    assert_eq!(
        active_before_refresh.state,
        storage::BucketState::Active,
        "expired route map should make the first background begin retryable before it can mark deleting"
    );

    install_same_store_next_epoch_runtime_map(&handle, &initial, tmp.path());

    let deadline = Instant::now() + TEST_EVENT_TIMEOUT;
    loop {
        match initial.test_head_bucket_raw(&bucket) {
            Ok(info) if info.state == storage::BucketState::Deleting => break,
            Err(storage::BucketSnapshotLoadError::Metadata(
                storage::MetadataError::BucketNotFound { .. },
            )) => break,
            Ok(info) if Instant::now() < deadline => {
                assert_eq!(
                    info.state,
                    storage::BucketState::Active,
                    "unexpected bucket state while waiting for background begin retry"
                );
                thread::sleep(Duration::from_millis(10));
            }
            Ok(info) => {
                panic!(
                    "reclaim worker did not retry BucketDeleteBegin after route refresh: {info:?}"
                );
            }
            Err(err) => {
                panic!("unexpected bucket metadata error while waiting for begin retry: {err:?}")
            }
        }
    }
}

#[test]
fn bucket_delete_begin_marks_deleting_on_retained_route_after_runtime_map_primary_move() {
    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let tmp = test_util::tempdir();
    let bucket = trusted_bucket_name("bucket-delete-begin-retained-route");
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1, 2]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let direct_coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&initial));
    direct_coord
        .create_bucket_for_owner("default-owner", bucket.as_str(), false)
        .unwrap();
    let bucket_identity = initial.test_head_bucket_raw(&bucket).unwrap();
    let bucket_pg_id = PgId::new(initial.test_bucket_pg_id_for(&bucket));
    assert_eq!(
        initial
            .local_pg_route(bucket_pg_id)
            .expect("initial bucket PG route should exist")
            .primary_node_id(),
        NodeId::new(0),
        "test assumes the pinned route starts on node 0"
    );

    let installed_next_epoch = Arc::new(AtomicBool::new(false));
    let installed_next_epoch_for_hook = Arc::clone(&installed_next_epoch);
    let handle_for_hook = handle.clone();
    let initial_for_hook = Arc::clone(&initial);
    let node_root = tmp.path().to_path_buf();
    let _hook_guard = initial.test_install_after_bucket_delete_final_visibility_proven_hook(
        Arc::new(move || {
            install_same_store_next_epoch_runtime_map_with_primary(
                &handle_for_hook,
                &initial_for_hook,
                &node_root,
                NodeId::new(1),
            );
            installed_next_epoch_for_hook.store(true, Ordering::SeqCst);
            Ok(())
        }),
    );

    initial
        .test_begin_bucket_delete_if_current(&bucket)
        .expect("pinned DeleteBucket begin should commit on retained route after runtime-map move");
    assert!(
        installed_next_epoch.load(Ordering::SeqCst),
        "test hook should install the next-epoch route map before mark-deleting apply"
    );

    let current = handle.current();
    assert_eq!(
        current.cluster_epoch().get(),
        initial.cluster_epoch().get() + 1,
        "runtime map should advance while the pinned operation is still running"
    );
    assert_eq!(
        current
            .local_pg_route(bucket_pg_id)
            .expect("current bucket PG route should exist")
            .primary_node_id(),
        NodeId::new(1),
        "current route should move the bucket PG primary away from the pinned route"
    );
    let current_info = current
        .test_head_bucket_raw(&bucket)
        .expect("current route should observe the retained-route commit");
    assert_eq!(current_info.state, storage::BucketState::Deleting);
    assert_eq!(
        current_info.bucket_execution_generation,
        bucket_identity.bucket_execution_generation + 1,
        "retained-route mark-deleting should apply exactly once to the same bucket generation"
    );
    assert_eq!(
        current_info.bucket_incarnation_generation,
        bucket_identity.bucket_incarnation_generation
    );
}

#[test]
fn reclaim_worker_adopts_bucket_delete_begin_after_partial_frontier() {
    let tmp = test_util::tempdir();
    let pg_ids: Vec<u32> = (0..32).collect();
    let bucket = trusted_bucket_name("bucket-delete-begin-worker-frontier");
    let initial = open_test_storage_cluster(tmp.path(), &pg_ids);
    let direct_coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&initial));
    direct_coord
        .create_bucket_for_owner("default-owner", bucket.as_str(), false)
        .unwrap();

    let pg_count = initial.test_pg_ids().len() as u32;
    assert!(
        pg_count >= 32,
        "test requires several exact-bucket drain chunks"
    );
    let fail_after_first_frontier = Arc::new(AtomicBool::new(true));
    let fail_after_first_frontier_for_hook = Arc::clone(&fail_after_first_frontier);
    let _progress_hook_guard = initial
        .test_install_after_bucket_delete_post_reservation_progress_hook(Arc::new(
            move |next_object_pg_id| {
                if next_object_pg_id < pg_count
                    && fail_after_first_frontier_for_hook.swap(false, Ordering::SeqCst)
                {
                    return Err(storage::StoreError::RouteMapExpired {
                        cluster_epoch: ClusterEpoch::INITIAL,
                        valid_until_ms: 0,
                        now_ms: 1,
                    });
                }
                Ok(())
            },
        ));

    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let _coord = setup_coordinator_with_only_reclaim_worker(handle, Arc::clone(&initial));

    let err = initial
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
        matches!(
            err,
            storage::BucketWriteDrainError::Store(storage::StoreError::RouteMapExpired { .. })
        ),
        "foreground DeleteBucket should preserve the attempt on injected route expiry, got {err:?}"
    );
    assert!(
        !fail_after_first_frontier.load(Ordering::SeqCst),
        "test hook should fail after the first persisted post-reservation frontier"
    );

    let deadline = Instant::now() + TEST_EVENT_TIMEOUT;
    loop {
        match initial.test_head_bucket_raw(&bucket) {
            Ok(info) if info.state == storage::BucketState::Deleting => break,
            Err(storage::BucketSnapshotLoadError::Metadata(
                storage::MetadataError::BucketNotFound { .. },
            )) => break,
            Ok(info) if Instant::now() < deadline => {
                assert_eq!(
                    info.state,
                    storage::BucketState::Active,
                    "unexpected bucket state while waiting for background begin adoption"
                );
                thread::sleep(Duration::from_millis(10));
            }
            Ok(info) => {
                panic!("reclaim worker did not adopt partial BucketDeleteBegin: {info:?}");
            }
            Err(err) => {
                panic!("unexpected bucket metadata error while waiting for begin adoption: {err:?}")
            }
        }
    }
}

#[test]
fn reclaim_worker_adopts_bucket_delete_begin_from_stream_cleanup_phase() {
    let tmp = test_util::tempdir();
    let bucket = trusted_bucket_name("bucket-delete-begin-worker-stream-cleanup");
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1, 2]);
    let direct_coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&initial));
    direct_coord
        .create_bucket_for_owner("default-owner", bucket.as_str(), false)
        .unwrap();
    let bucket_identity = initial.test_head_bucket_raw(&bucket).unwrap();

    initial
        .test_seed_bucket_delete_attempt_outcome(
            &bucket,
            storage::BucketDeleteAttemptOutcomeKind::Retryable,
            storage::BucketDeleteAttemptPhase::StreamCleanup,
            "seeded stream-cleanup retryable attempt".to_string(),
            None,
        )
        .unwrap();

    let post_reservation_scan_ran = Arc::new(AtomicBool::new(false));
    let post_reservation_scan_ran_for_hook = Arc::clone(&post_reservation_scan_ran);
    let initial_scan_ran = Arc::new(AtomicBool::new(false));
    let initial_scan_ran_for_hook = Arc::clone(&initial_scan_ran);
    let _exact_drain_hook_guard = initial.test_install_before_bucket_delete_exact_drain_hook(
        Arc::new(move |has_progress, next_object_pg_id| {
            if !has_progress {
                initial_scan_ran_for_hook.store(true, Ordering::SeqCst);
                return Err(storage::StoreError::Io {
                    context: "unexpected initial exact-bucket drain during stream-cleanup adoption",
                    source: std::io::Error::other(format!("next_object_pg_id={next_object_pg_id}")),
                });
            }
            Ok(())
        }),
    );
    let _progress_hook_guard = initial
        .test_install_after_bucket_delete_post_reservation_progress_hook(Arc::new(
            move |_next_object_pg_id| {
                post_reservation_scan_ran_for_hook.store(true, Ordering::SeqCst);
                Ok(())
            },
        ));

    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let _coord = setup_coordinator_with_only_reclaim_worker(handle, Arc::clone(&initial));
    initial.test_enqueue_bucket_delete_begin(
        &bucket,
        bucket_identity.bucket_execution_generation,
        bucket_identity.bucket_incarnation_generation,
    );

    let deadline = Instant::now() + TEST_EVENT_TIMEOUT;
    loop {
        match initial.test_head_bucket_raw(&bucket) {
            Ok(info) if info.state == storage::BucketState::Deleting => break,
            Err(storage::BucketSnapshotLoadError::Metadata(
                storage::MetadataError::BucketNotFound { .. },
            )) => break,
            Ok(info) if Instant::now() < deadline => {
                assert_eq!(
                    info.state,
                    storage::BucketState::Active,
                    "unexpected bucket state while waiting for stream-cleanup adoption"
                );
                thread::sleep(Duration::from_millis(10));
            }
            Ok(info) => {
                panic!("reclaim worker did not adopt stream-cleanup BucketDeleteBegin: {info:?}");
            }
            Err(err) => {
                panic!(
                    "unexpected bucket metadata error while waiting for stream-cleanup adoption: {err:?}"
                )
            }
        }
    }

    assert!(
        post_reservation_scan_ran.load(Ordering::SeqCst),
        "stream-cleanup worker adoption must still revalidate the post-reservation object-PG drain"
    );
    assert!(
        !initial_scan_ran.load(Ordering::SeqCst),
        "stream-cleanup worker adoption must skip the initial pre-cleanup exact-bucket drain"
    );
}

#[test]
fn reclaim_worker_adopts_bucket_delete_begin_from_reservation_wait_phase() {
    let tmp = test_util::tempdir();
    let bucket = trusted_bucket_name("bucket-delete-begin-worker-reservation-wait");
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1, 2]);
    let direct_coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&initial));
    direct_coord
        .create_bucket_for_owner("default-owner", bucket.as_str(), false)
        .unwrap();
    let bucket_identity = initial.test_head_bucket_raw(&bucket).unwrap();

    initial
        .test_seed_bucket_delete_attempt_outcome(
            &bucket,
            storage::BucketDeleteAttemptOutcomeKind::Retryable,
            storage::BucketDeleteAttemptPhase::ReservationWait,
            "seeded reservation-wait retryable attempt".to_string(),
            None,
        )
        .unwrap();

    let post_reservation_scan_ran = Arc::new(AtomicBool::new(false));
    let post_reservation_scan_ran_for_hook = Arc::clone(&post_reservation_scan_ran);
    let initial_scan_ran = Arc::new(AtomicBool::new(false));
    let initial_scan_ran_for_hook = Arc::clone(&initial_scan_ran);
    let _exact_drain_hook_guard = initial.test_install_before_bucket_delete_exact_drain_hook(
        Arc::new(move |has_progress, next_object_pg_id| {
            if !has_progress {
                initial_scan_ran_for_hook.store(true, Ordering::SeqCst);
                return Err(storage::StoreError::Io {
                    context:
                        "unexpected initial exact-bucket drain during reservation-wait adoption",
                    source: std::io::Error::other(format!("next_object_pg_id={next_object_pg_id}")),
                });
            }
            Ok(())
        }),
    );
    let _progress_hook_guard = initial
        .test_install_after_bucket_delete_post_reservation_progress_hook(Arc::new(
            move |_next_object_pg_id| {
                post_reservation_scan_ran_for_hook.store(true, Ordering::SeqCst);
                Ok(())
            },
        ));

    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let _coord = setup_coordinator_with_only_reclaim_worker(handle, Arc::clone(&initial));
    initial.test_enqueue_bucket_delete_begin(
        &bucket,
        bucket_identity.bucket_execution_generation,
        bucket_identity.bucket_incarnation_generation,
    );

    let deadline = Instant::now() + TEST_EVENT_TIMEOUT;
    loop {
        match initial.test_head_bucket_raw(&bucket) {
            Ok(info) if info.state == storage::BucketState::Deleting => break,
            Err(storage::BucketSnapshotLoadError::Metadata(
                storage::MetadataError::BucketNotFound { .. },
            )) => break,
            Ok(info) if Instant::now() < deadline => {
                assert_eq!(
                    info.state,
                    storage::BucketState::Active,
                    "unexpected bucket state while waiting for reservation-wait adoption"
                );
                thread::sleep(Duration::from_millis(10));
            }
            Ok(info) => {
                panic!("reclaim worker did not adopt reservation-wait BucketDeleteBegin: {info:?}");
            }
            Err(err) => {
                panic!(
                    "unexpected bucket metadata error while waiting for reservation-wait adoption: {err:?}"
                )
            }
        }
    }

    assert!(
        post_reservation_scan_ran.load(Ordering::SeqCst),
        "reservation-wait worker adoption must still revalidate the post-reservation object-PG drain"
    );
    assert!(
        !initial_scan_ran.load(Ordering::SeqCst),
        "reservation-wait worker adoption must skip the initial pre-cleanup exact-bucket drain"
    );
}

#[test]
fn reclaim_worker_adopts_bucket_delete_begin_from_final_visibility_phase() {
    let tmp = test_util::tempdir();
    let bucket = trusted_bucket_name("bucket-delete-begin-worker-final-visibility");
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1, 2]);
    let direct_coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&initial));
    direct_coord
        .create_bucket_for_owner("default-owner", bucket.as_str(), false)
        .unwrap();
    let bucket_identity = initial.test_head_bucket_raw(&bucket).unwrap();

    initial
        .test_seed_bucket_delete_attempt_outcome(
            &bucket,
            storage::BucketDeleteAttemptOutcomeKind::Retryable,
            storage::BucketDeleteAttemptPhase::FinalVisibilityCheck,
            "seeded final-visibility retryable attempt".to_string(),
            Some(0),
        )
        .unwrap();

    let exact_drain_ran = Arc::new(AtomicBool::new(false));
    let exact_drain_ran_for_hook = Arc::clone(&exact_drain_ran);
    let _exact_drain_hook_guard = initial.test_install_before_bucket_delete_exact_drain_hook(
        Arc::new(move |has_progress, next_object_pg_id| {
            exact_drain_ran_for_hook.store(true, Ordering::SeqCst);
            Err(storage::StoreError::Io {
                context: "unexpected exact-bucket drain during final-visibility adoption",
                source: std::io::Error::other(format!(
                    "has_progress={has_progress} next_object_pg_id={next_object_pg_id}"
                )),
            })
        }),
    );

    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let _coord = setup_coordinator_with_only_reclaim_worker(handle, Arc::clone(&initial));
    initial.test_enqueue_bucket_delete_begin(
        &bucket,
        bucket_identity.bucket_execution_generation,
        bucket_identity.bucket_incarnation_generation,
    );

    let deadline = Instant::now() + TEST_EVENT_TIMEOUT;
    loop {
        match initial.test_head_bucket_raw(&bucket) {
            Ok(info) if info.state == storage::BucketState::Deleting => break,
            Err(storage::BucketSnapshotLoadError::Metadata(
                storage::MetadataError::BucketNotFound { .. },
            )) => break,
            Ok(info) if Instant::now() < deadline => {
                assert_eq!(
                    info.state,
                    storage::BucketState::Active,
                    "unexpected bucket state while waiting for final-visibility adoption"
                );
                thread::sleep(Duration::from_millis(10));
            }
            Ok(info) => {
                panic!("reclaim worker did not adopt final-visibility BucketDeleteBegin: {info:?}");
            }
            Err(err) => {
                panic!(
                    "unexpected bucket metadata error while waiting for final-visibility adoption: {err:?}"
                )
            }
        }
    }

    assert!(
        !exact_drain_ran.load(Ordering::SeqCst),
        "final-visibility worker adoption must skip exact-bucket drain phases"
    );
}

#[test]
fn reclaim_worker_adopts_bucket_delete_begin_from_final_visibility_proven_phase() {
    let tmp = test_util::tempdir();
    let bucket = trusted_bucket_name("bucket-delete-begin-worker-final-visibility-proven");
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1, 2]);
    let direct_coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&initial));
    direct_coord
        .create_bucket_for_owner("default-owner", bucket.as_str(), false)
        .unwrap();
    let bucket_identity = initial.test_head_bucket_raw(&bucket).unwrap();

    initial
        .test_seed_bucket_delete_attempt_outcome(
            &bucket,
            storage::BucketDeleteAttemptOutcomeKind::Retryable,
            storage::BucketDeleteAttemptPhase::FinalVisibilityProven,
            "seeded final-visibility-proven retryable attempt".to_string(),
            Some(0),
        )
        .unwrap();

    let visibility_check_ran = Arc::new(AtomicBool::new(false));
    let visibility_check_ran_for_hook = Arc::clone(&visibility_check_ran);
    let _visibility_hook_guard =
        initial.test_install_before_bucket_delete_final_visibility_hook(Arc::new(move || {
            visibility_check_ran_for_hook.store(true, Ordering::SeqCst);
            Err(storage::StoreError::Io {
                context: "unexpected final visibility scan during proven worker adoption",
                source: std::io::Error::other("final visibility should already be proven"),
            })
        }));

    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let _coord = setup_coordinator_with_only_reclaim_worker(handle, Arc::clone(&initial));
    initial.test_enqueue_bucket_delete_begin(
        &bucket,
        bucket_identity.bucket_execution_generation,
        bucket_identity.bucket_incarnation_generation,
    );

    let deadline = Instant::now() + TEST_EVENT_TIMEOUT;
    loop {
        match initial.test_head_bucket_raw(&bucket) {
            Ok(info) if info.state == storage::BucketState::Deleting => break,
            Err(storage::BucketSnapshotLoadError::Metadata(
                storage::MetadataError::BucketNotFound { .. },
            )) => break,
            Ok(info) if Instant::now() < deadline => {
                assert_eq!(
                    info.state,
                    storage::BucketState::Active,
                    "unexpected bucket state while waiting for final-visibility-proven adoption"
                );
                thread::sleep(Duration::from_millis(10));
            }
            Ok(info) => {
                panic!(
                    "reclaim worker did not adopt final-visibility-proven BucketDeleteBegin: {info:?}"
                );
            }
            Err(err) => {
                panic!(
                    "unexpected bucket metadata error while waiting for final-visibility-proven adoption: {err:?}"
                )
            }
        }
    }

    assert!(
        !visibility_check_ran.load(Ordering::SeqCst),
        "final-visibility-proven worker adoption must skip the visibility scan"
    );
}

#[test]
fn reclaim_worker_drops_stale_bucket_delete_begin_after_bucket_recreate() {
    let tmp = test_util::tempdir();
    let bucket = trusted_bucket_name("bucket-delete-begin-stale-recreate");
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let direct_coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&initial));
    direct_coord
        .create_bucket_for_owner("old-owner", bucket.as_str(), false)
        .unwrap();
    let old_identity = initial.test_head_bucket_raw(&bucket).unwrap();

    let expired = same_store_cluster_with_route_map_validity(
        &initial,
        tmp.path(),
        RouteMapValidity::until_ms(1).unwrap(),
    );
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&expired));
    let _coord = setup_coordinator_with_only_reclaim_worker(handle.clone(), Arc::clone(&expired));

    expired.test_enqueue_bucket_delete_begin(
        &bucket,
        old_identity.bucket_execution_generation,
        old_identity.bucket_incarnation_generation,
    );
    thread::sleep(Duration::from_millis(350));

    initial
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
    delete_bucket_metadata_or_accept_reclaim_worker_finalize(&initial, &bucket);
    direct_coord
        .create_bucket_for_owner("new-owner", bucket.as_str(), false)
        .unwrap();
    let recreated = initial.test_head_bucket_raw(&bucket).unwrap();
    assert_ne!(
        recreated.bucket_incarnation_generation,
        old_identity.bucket_incarnation_generation
    );

    install_same_store_next_epoch_runtime_map(&handle, &initial, tmp.path());

    let deadline = Instant::now() + TEST_EVENT_TIMEOUT;
    loop {
        let current = initial.test_head_bucket_raw(&bucket).unwrap();
        assert_eq!(current.owner_principal, "new-owner");
        assert_eq!(
            current.bucket_incarnation_generation,
            recreated.bucket_incarnation_generation
        );
        assert_eq!(
            current.state,
            storage::BucketState::Active,
            "stale BucketDeleteBegin must not delete the recreated bucket"
        );
        if Instant::now() >= deadline {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn bucket_subresource_write_pins_runtime_map_after_authorization() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let candidate_tmp = test_util::tempdir();
    let candidate = make_dynamic_runtime_map_candidate(open_test_storage_cluster(
        candidate_tmp.path(),
        &[0, 1],
    ));
    let publication_thread = Arc::new(Mutex::new(None));
    let hook_publication_thread = Arc::clone(&publication_thread);
    let hook_handle = handle.clone();
    let _serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = coord.install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
        bucket: Some("bucket".to_string()),
        after_loaded: Some(Arc::new(move || {
            let publishing_handle = hook_handle.clone();
            let publishing_candidate = Arc::clone(&candidate);
            let thread = thread::spawn(move || {
                publishing_handle.install(publishing_candidate).unwrap();
            });
            *hook_publication_thread.lock().unwrap() = Some(thread);
            hook_handle.test_wait_until_route_publication_is_pending();
        })),
        ..BucketWriteHandleTestHooks::default()
    });

    let lifecycle = "<LifecycleConfiguration><Rule><ID>pin</ID><Filter><Prefix/></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>";
    put_bucket_lifecycle_test(&coord, "bucket", lifecycle, test_requester(), None).unwrap();
    publication_thread
        .lock()
        .unwrap()
        .take()
        .expect("bucket lifecycle hook should start route publication")
        .join()
        .unwrap();

    handle
        .install(make_dynamic_runtime_map_candidate(initial))
        .unwrap();
    let stored = coord
        .get_bucket_lifecycle(&bucket_request_with_expected_owner(
            "bucket",
            test_requester(),
            None,
        ))
        .unwrap()
        .unwrap();
    assert!(stored.contains("<ID>pin</ID>"));
}

#[test]
fn bucket_subresource_write_pins_runtime_map_before_authorization() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let candidate_tmp = test_util::tempdir();
    let candidate = make_dynamic_runtime_map_candidate(open_test_storage_cluster(
        candidate_tmp.path(),
        &[0, 1],
    ));
    let publication_thread = Arc::new(Mutex::new(None));
    let hook_publication_thread = Arc::clone(&publication_thread);
    let hook_handle = handle.clone();
    let _serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = coord.install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
        bucket: Some("bucket".to_string()),
        after_bucket_mutation_storage_node_capture: Some(Arc::new(move || {
            let publishing_handle = hook_handle.clone();
            let publishing_candidate = Arc::clone(&candidate);
            let thread = thread::spawn(move || {
                publishing_handle.install(publishing_candidate).unwrap();
            });
            *hook_publication_thread.lock().unwrap() = Some(thread);
            hook_handle.test_wait_until_route_publication_is_pending();
        })),
        ..BucketWriteHandleTestHooks::default()
    });

    let lifecycle = "<LifecycleConfiguration><Rule><ID>pin-before-auth</ID><Filter><Prefix/></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>";
    put_bucket_lifecycle_test(&coord, "bucket", lifecycle, test_requester(), None).unwrap();
    publication_thread
        .lock()
        .unwrap()
        .take()
        .expect("bucket lifecycle hook should start route publication")
        .join()
        .unwrap();

    handle
        .install(make_dynamic_runtime_map_candidate(initial))
        .unwrap();
    let stored = coord
        .get_bucket_lifecycle(&bucket_request_with_expected_owner(
            "bucket",
            test_requester(),
            None,
        ))
        .unwrap()
        .unwrap();
    assert!(stored.contains("<ID>pin-before-auth</ID>"));
}

#[test]
fn upload_part_copy_pins_runtime_map_after_stream_session_create() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"upload-part-copy-pinned-runtime-map",
            metadata: &metadata,
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

    let candidate_tmp = test_util::tempdir();
    let candidate = make_dynamic_runtime_map_candidate(open_test_storage_cluster(
        candidate_tmp.path(),
        &[0, 1],
    ));
    let publication_thread = Arc::new(Mutex::new(None));
    let hook_publication_thread = Arc::clone(&publication_thread);
    let hook_handle = handle.clone();
    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some(("bucket".to_string(), "dst".to_string())),
        after_upload_part_copy_stream_session: Some(Arc::new(move || {
            let publishing_handle = hook_handle.clone();
            let publishing_candidate = Arc::clone(&candidate);
            let thread = thread::spawn(move || {
                publishing_handle.install(publishing_candidate).unwrap();
            });
            *hook_publication_thread.lock().unwrap() = Some(thread);
            hook_handle.test_wait_until_route_publication_is_pending();
        })),
        ..ReclamationTestHooks::default()
    });

    coord
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
    publication_thread
        .lock()
        .unwrap()
        .take()
        .expect("UploadPartCopy hook should start route publication")
        .join()
        .unwrap();
}

#[test]
fn put_object_pins_runtime_map_after_bucket_write_reservation() {
    let bucket = "direct-put-pinned-bucket";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let candidate_tmp = test_util::tempdir();
    let candidate = make_dynamic_runtime_map_candidate(open_test_storage_cluster(
        candidate_tmp.path(),
        &[0, 1],
    ));
    let hook_handle = handle.clone();
    let _serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = coord.install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
        bucket: Some(bucket.to_string()),
        after_loaded: Some(Arc::new(move || {
            hook_handle.install(Arc::clone(&candidate)).unwrap();
        })),
        ..BucketWriteHandleTestHooks::default()
    });

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, "key", test_requester(), None),
            data: b"direct-put-pinned-runtime-map",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    handle
        .install(make_dynamic_runtime_map_candidate(initial))
        .unwrap();
    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        result.body.read_all().unwrap(),
        b"direct-put-pinned-runtime-map"
    );
}

fn install_next_epoch_runtime_map_with_historical_routes(
    handle: &StorageClusterRuntimeMapHandle,
    initial: &Arc<StorageCluster>,
    node_root: &std::path::Path,
) {
    let node_count = u32::from(initial.default_payload_ec_shape().k)
        + u32::from(initial.default_payload_ec_shape().m);
    let configs = (0..node_count)
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                NodeId::new(node_id),
                node_root.join(format!("node-{node_id:04}")),
            )
        })
        .collect::<Vec<_>>();
    let next_epoch = ClusterEpoch::new(initial.cluster_epoch().get() + 1).unwrap();
    let acting_set = (0..node_count).map(NodeId::new).collect::<Vec<_>>();
    let routes = initial
        .test_pg_ids()
        .iter()
        .map(|pg_id| {
            let route = storage::control_plane::PgRouteSnapshot::reconstructed(
                next_epoch,
                PgId::new(*pg_id),
                NodeId::new(0),
                acting_set.clone(),
                PgState::Active,
            );
            LocalPgRoute::from(&route)
        })
        .collect::<Vec<_>>();
    let historical_routes = initial
        .local_pg_routes()
        .map(|route| {
            storage::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                route.state(),
            )
        })
        .collect::<Vec<_>>();
    let mut candidate_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs,
        initial.test_pg_ids(),
        initial.default_payload_ec_shape(),
        next_epoch,
        routes,
    )
    .unwrap();
    candidate_map.test_install_historical_pg_routes(historical_routes);
    candidate_map.test_set_route_map_validity(long_lived_test_route_map_validity());
    let candidate = StorageCluster::from_local_map(Arc::new(candidate_map)).unwrap();
    handle.install(candidate).unwrap();
}

fn begin_next_epoch_runtime_map_publication_with_historical_routes(
    handle: &StorageClusterRuntimeMapHandle,
    initial: &Arc<StorageCluster>,
    node_root: &std::path::Path,
) -> thread::JoinHandle<()> {
    let publishing_handle = handle.clone();
    let publishing_initial = Arc::clone(initial);
    let publishing_node_root = node_root.to_path_buf();
    let publication_thread = thread::spawn(move || {
        install_next_epoch_runtime_map_with_historical_routes(
            &publishing_handle,
            &publishing_initial,
            &publishing_node_root,
        );
    });
    handle.test_wait_until_route_publication_is_pending();
    publication_thread
}

fn install_same_store_next_epoch_runtime_map(
    handle: &StorageClusterRuntimeMapHandle,
    initial: &Arc<StorageCluster>,
    node_root: &std::path::Path,
) {
    install_same_store_next_epoch_runtime_map_with_primary(
        handle,
        initial,
        node_root,
        NodeId::new(0),
    );
}

fn install_same_store_same_epoch_runtime_map(
    handle: &StorageClusterRuntimeMapHandle,
    initial: &Arc<StorageCluster>,
    node_root: &std::path::Path,
) {
    install_same_store_same_epoch_runtime_map_with_primary(
        handle,
        initial,
        node_root,
        NodeId::new(0),
    );
}

fn install_same_store_same_epoch_runtime_map_with_primary(
    handle: &StorageClusterRuntimeMapHandle,
    initial: &Arc<StorageCluster>,
    _node_root: &std::path::Path,
    primary_node_id: NodeId,
) {
    let node_count = u32::from(initial.default_payload_ec_shape().k)
        + u32::from(initial.default_payload_ec_shape().m);
    let acting_set = (0..node_count).map(NodeId::new).collect::<Vec<_>>();
    let routes = initial
        .test_pg_ids()
        .iter()
        .map(|pg_id| {
            storage::control_plane::PgRouteSnapshot::reconstructed(
                initial.cluster_epoch(),
                PgId::new(*pg_id),
                primary_node_id,
                acting_set.clone(),
                PgState::Active,
            )
        })
        .collect::<Vec<_>>();
    let historical_routes = initial
        .local_pg_routes()
        .map(|route| {
            storage::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                route.state(),
            )
        })
        .collect::<Vec<_>>();
    let candidate = initial
        .test_clone_with_pg_routes(initial.cluster_epoch(), routes, historical_routes)
        .unwrap();
    candidate.test_store_route_map_validity(long_lived_test_route_map_validity());
    handle.install(candidate).unwrap();
}

fn install_same_store_next_epoch_runtime_map_with_primary(
    handle: &StorageClusterRuntimeMapHandle,
    initial: &Arc<StorageCluster>,
    _node_root: &std::path::Path,
    primary_node_id: NodeId,
) {
    let node_count = u32::from(initial.default_payload_ec_shape().k)
        + u32::from(initial.default_payload_ec_shape().m);
    let next_epoch = ClusterEpoch::new(initial.cluster_epoch().get() + 1).unwrap();
    let acting_set = (0..node_count).map(NodeId::new).collect::<Vec<_>>();
    let routes = initial
        .test_pg_ids()
        .iter()
        .map(|pg_id| {
            storage::control_plane::PgRouteSnapshot::reconstructed(
                next_epoch,
                PgId::new(*pg_id),
                primary_node_id,
                acting_set.clone(),
                PgState::Active,
            )
        })
        .collect::<Vec<_>>();
    let historical_routes = initial
        .local_pg_routes()
        .map(|route| {
            storage::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                route.state(),
            )
        })
        .collect::<Vec<_>>();
    let candidate = initial
        .test_clone_with_pg_routes(next_epoch, routes, historical_routes)
        .unwrap();
    candidate.test_store_route_map_validity(long_lived_test_route_map_validity());
    handle.install(candidate).unwrap();
}

fn same_store_cluster_with_route_map_validity(
    initial: &Arc<StorageCluster>,
    node_root: &std::path::Path,
    route_map_validity: RouteMapValidity,
) -> Arc<StorageCluster> {
    open_same_store_cluster_with_route_map_validity(
        capture_same_store_cluster_inputs(initial, node_root),
        route_map_validity,
    )
}

struct SameStoreClusterInputs {
    configs: Vec<LocalNodeStoreConfig>,
    routes: Vec<LocalPgRoute>,
    pg_ids: Vec<u32>,
    ec_shape: storage::EcShape,
    cluster_epoch: ClusterEpoch,
}

fn capture_same_store_cluster_inputs(
    initial: &StorageCluster,
    node_root: &std::path::Path,
) -> SameStoreClusterInputs {
    let node_count = u32::from(initial.default_payload_ec_shape().k)
        + u32::from(initial.default_payload_ec_shape().m);
    let configs = (0..node_count)
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                NodeId::new(node_id),
                node_root.join(format!("node-{node_id:04}")),
            )
        })
        .collect::<Vec<_>>();
    let routes = initial
        .local_pg_routes()
        .map(|route| {
            let route = storage::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                route.state(),
            );
            LocalPgRoute::from(&route)
        })
        .collect::<Vec<_>>();
    SameStoreClusterInputs {
        configs,
        routes,
        pg_ids: initial.test_pg_ids().to_vec(),
        ec_shape: initial.default_payload_ec_shape(),
        cluster_epoch: initial.cluster_epoch(),
    }
}

fn open_same_store_cluster_with_route_map_validity(
    inputs: SameStoreClusterInputs,
    route_map_validity: RouteMapValidity,
) -> Arc<StorageCluster> {
    let mut local_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        inputs.configs,
        &inputs.pg_ids,
        inputs.ec_shape,
        inputs.cluster_epoch,
        inputs.routes,
    )
    .unwrap();
    local_map.test_set_route_map_validity(route_map_validity);
    StorageCluster::from_local_map(Arc::new(local_map)).unwrap()
}

fn same_epoch_cluster_with_stale_current_pg_routes(
    initial: &Arc<StorageCluster>,
    node_root: &std::path::Path,
) -> Arc<StorageCluster> {
    let node_count = u32::from(initial.default_payload_ec_shape().k)
        + u32::from(initial.default_payload_ec_shape().m);
    let configs = (0..node_count)
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                NodeId::new(node_id),
                node_root.join(format!("node-{node_id:04}")),
            )
        })
        .collect::<Vec<_>>();
    let next_epoch = ClusterEpoch::new(initial.cluster_epoch().get() + 1).unwrap();
    let acting_set = (0..node_count).map(NodeId::new).collect::<Vec<_>>();
    let routes = initial
        .test_pg_ids()
        .iter()
        .map(|pg_id| {
            let route = storage::control_plane::PgRouteSnapshot::reconstructed(
                next_epoch,
                PgId::new(*pg_id),
                NodeId::new(0),
                acting_set.clone(),
                PgState::Active,
            );
            LocalPgRoute::from(&route)
        })
        .collect::<Vec<_>>();
    let historical_routes = initial
        .local_pg_routes()
        .map(|route| {
            storage::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                route.state(),
            )
        })
        .collect::<Vec<_>>();
    let mut local_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs,
        initial.test_pg_ids(),
        initial.default_payload_ec_shape(),
        next_epoch,
        routes,
    )
    .unwrap();
    local_map.test_install_historical_pg_routes(historical_routes);
    let stale_current_routes = initial
        .local_pg_routes()
        .map(|route| {
            storage::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                route.state(),
            )
        })
        .collect::<Vec<_>>();
    local_map.test_install_pg_routes(stale_current_routes);
    StorageCluster::from_local_map(Arc::new(local_map)).unwrap()
}

fn install_same_store_next_epoch_runtime_map_with_peering_pg(
    handle: &StorageClusterRuntimeMapHandle,
    initial: &Arc<StorageCluster>,
    node_root: &std::path::Path,
    peering_pg: u32,
) -> ClusterEpoch {
    let node_count = u32::from(initial.default_payload_ec_shape().k)
        + u32::from(initial.default_payload_ec_shape().m);
    let configs = (0..node_count)
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                NodeId::new(node_id),
                node_root.join(format!("node-{node_id:04}")),
            )
        })
        .collect::<Vec<_>>();
    let next_epoch = ClusterEpoch::new(initial.cluster_epoch().get() + 1).unwrap();
    let acting_set = (0..node_count).map(NodeId::new).collect::<Vec<_>>();
    let routes = initial
        .test_pg_ids()
        .iter()
        .map(|pg_id| {
            let state = if *pg_id == peering_pg {
                PgState::Peering
            } else {
                PgState::Active
            };
            let route = storage::control_plane::PgRouteSnapshot::reconstructed(
                next_epoch,
                PgId::new(*pg_id),
                NodeId::new(0),
                acting_set.clone(),
                state,
            );
            LocalPgRoute::from(&route)
        })
        .collect::<Vec<_>>();
    let historical_routes = initial
        .local_pg_routes()
        .map(|route| {
            storage::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                route.state(),
            )
        })
        .collect::<Vec<_>>();
    let mut candidate_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs,
        initial.test_pg_ids(),
        initial.default_payload_ec_shape(),
        next_epoch,
        routes,
    )
    .unwrap();
    candidate_map.test_install_historical_pg_routes(historical_routes);
    candidate_map.test_set_route_map_validity(long_lived_test_route_map_validity());
    let candidate = StorageCluster::from_local_map(Arc::new(candidate_map)).unwrap();
    handle.install(candidate).unwrap();
    next_epoch
}

fn find_bucket_key_for_metadata_and_data_pg(
    storage_cluster: &StorageCluster,
    metadata_pg_id: u32,
    data_pg_id: u32,
) -> (String, String) {
    for bucket_suffix in 0..1024 {
        let bucket = format!("remote-read-epoch-bucket-{bucket_suffix}");
        let bucket_name = trusted_bucket_name(&bucket);
        if storage_cluster.test_bucket_pg_id_for(&bucket_name) != metadata_pg_id {
            continue;
        }
        for key_suffix in 0..10_000 {
            let key = format!("key-{key_suffix:04}");
            let object_key = trusted_object_key(&key);
            if storage_cluster.test_object_pg_id_for(&bucket_name, &object_key) == metadata_pg_id
                && storage_cluster.test_data_pg_id_for(&bucket_name, &object_key, GenerationId::MIN)
                    == data_pg_id
            {
                return (bucket, key);
            }
        }
    }
    panic!(
        "failed to find bucket/key with bucket and object PG {metadata_pg_id} and data PG {data_pg_id}"
    );
}

fn find_key_for_object_metadata_pg_with_prefix(
    storage_cluster: &StorageCluster,
    bucket: &str,
    metadata_pg_id: u32,
    key_prefix: &str,
) -> String {
    let bucket_name = trusted_bucket_name(bucket);
    for key_suffix in 0..10_000 {
        let key = format!("{key_prefix}{key_suffix:04}");
        let object_key = trusted_object_key(&key);
        if storage_cluster.test_object_pg_id_for(&bucket_name, &object_key) == metadata_pg_id {
            return key;
        }
    }
    panic!("failed to find key with prefix {key_prefix:?} on object metadata PG {metadata_pg_id}");
}

#[test]
fn put_object_epoch_change_before_metadata_apply_commits_once_on_pinned_route() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("put-object-before-metadata-apply");

    let bucket = "direct-put-epoch-change-bucket";
    let key = "key";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = Arc::new(
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap(),
    );
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let bucket_name = trusted_bucket_name(bucket);
    let object_key = trusted_object_key(key);
    let before_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();

    let gate = DeterministicFaultGate::new(TOKEN);
    let hook_bucket = bucket_name.clone();
    let hook_key = object_key.clone();
    let gate_for_hook = Arc::clone(&gate);
    let _hook_guard =
        initial.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::CommitDirectPutObject
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
            {
                gate_for_hook.wait_at(TOKEN);
            }
            Ok(())
        }));

    let put_coord = Arc::clone(&coord);
    let put_thread = thread::spawn(move || {
        let metadata = MetadataBlob::new();
        test_helpers::put_object(
            &put_coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(bucket, key, test_requester(), None),
                data: b"direct-put-crosses-epoch-change",
                metadata: &metadata,
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
    });

    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);
    let paused_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        paused_object_pg_proof.applied_log_index,
        before_object_pg_proof.applied_log_index + 1,
        "direct PUT should have applied only the generation-reservation command before the pre-commit gate"
    );

    install_next_epoch_runtime_map_with_historical_routes(&handle, &initial, tmp.path());

    gate.release();
    let put_result = put_thread.join().unwrap().unwrap();
    assert_eq!(put_result.version_id, VersionId::Null);
    let after_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof.applied_log_index,
        paused_object_pg_proof.applied_log_index + 1,
        "direct PUT crossing an epoch change should append exactly one commit command after the gate"
    );
    assert_ne!(
        after_object_pg_proof.applied_log_hash, before_object_pg_proof.applied_log_hash,
        "direct PUT command should change the object-PG command-log hash"
    );
    assert_ne!(
        after_object_pg_proof.state_digest, before_object_pg_proof.state_digest,
        "direct PUT command should change the object-PG materialized state digest"
    );

    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        result.body.read_all().unwrap(),
        b"direct-put-crosses-epoch-change"
    );
}

#[test]
fn overwrite_object_epoch_change_before_metadata_apply_commits_once_on_pinned_route() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("overwrite-object-before-metadata-apply");

    let bucket = "overwrite-epoch-change-bucket";
    let key = "key";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = Arc::new(
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap(),
    );
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, key, test_requester(), None),
            data: b"old-body",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    let object_key = trusted_object_key(key);
    let before_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();

    let gate = DeterministicFaultGate::new(TOKEN);
    let hook_bucket = bucket_name.clone();
    let hook_key = object_key.clone();
    let gate_for_hook = Arc::clone(&gate);
    let _hook_guard =
        initial.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::CommitDirectPutObject
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
            {
                gate_for_hook.wait_at(TOKEN);
            }
            Ok(())
        }));

    let put_coord = Arc::clone(&coord);
    let put_thread = thread::spawn(move || {
        let metadata = MetadataBlob::new();
        test_helpers::put_object(
            &put_coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(bucket, key, test_requester(), None),
                data: b"new-body",
                metadata: &metadata,
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
    });

    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);
    let paused_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        paused_object_pg_proof.applied_log_index,
        before_object_pg_proof.applied_log_index + 1,
        "overwrite should have applied only the generation-reservation command before the pre-commit gate"
    );

    install_next_epoch_runtime_map_with_historical_routes(&handle, &initial, tmp.path());

    gate.release();
    let put_result = put_thread.join().unwrap().unwrap();
    assert_eq!(put_result.version_id, VersionId::Null);
    let after_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof.applied_log_index,
        paused_object_pg_proof.applied_log_index + 1,
        "overwrite crossing an epoch change should append exactly one commit command after the gate"
    );
    assert_ne!(
        after_object_pg_proof.applied_log_hash, before_object_pg_proof.applied_log_hash,
        "overwrite command should change the object-PG command-log hash"
    );
    assert_ne!(
        after_object_pg_proof.state_digest, before_object_pg_proof.state_digest,
        "overwrite command should change the object-PG materialized state digest"
    );

    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), b"new-body");
}

#[test]
fn copy_object_epoch_change_before_metadata_apply_commits_once_on_pinned_route() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("copy-object-before-metadata-apply");

    let bucket = "copy-object-epoch-change-bucket";
    let src_key = "src";
    let dst_key = "dst";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = Arc::new(
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap(),
    );
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, src_key, test_requester(), None),
            data: b"copy-crosses-epoch-change",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    let dst_object_key = trusted_object_key(dst_key);
    let before_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &dst_object_key)
        .unwrap();

    let gate = DeterministicFaultGate::new(TOKEN);
    let pre_commit_kinds = Arc::new(Mutex::new(Vec::new()));
    let hook_bucket = bucket_name.clone();
    let hook_key = dst_object_key.clone();
    let gate_for_hook = Arc::clone(&gate);
    let pre_commit_kinds_for_hook = Arc::clone(&pre_commit_kinds);
    let _hook_guard =
        initial.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
            {
                if context.kind == MetadataCommandApplyTestKind::CommitDirectPutObject {
                    gate_for_hook.wait_at(TOKEN);
                } else {
                    let mut kinds = pre_commit_kinds_for_hook.lock().unwrap();
                    if kinds.last() != Some(&context.kind) {
                        kinds.push(context.kind);
                    }
                }
            }
            Ok(())
        }));

    let copy_coord = Arc::clone(&coord);
    let copy_thread = thread::spawn(move || {
        copy_coord.copy_object(&CopyObjectRequest {
            source: copy_source(bucket, src_key, None),
            destination: object_request_with_expected_owner(
                bucket,
                dst_key,
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

    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);
    let paused_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &dst_object_key)
        .unwrap();
    assert_eq!(
        paused_object_pg_proof.applied_log_index,
        before_object_pg_proof.applied_log_index + 3,
        "copy should have applied generation reservation, stream-session create, and segment append before the destination commit gate"
    );
    assert_eq!(
        pre_commit_kinds.lock().unwrap().as_slice(),
        &[
            MetadataCommandApplyTestKind::ReserveObjectGeneration,
            MetadataCommandApplyTestKind::CreateStreamUpload,
            MetadataCommandApplyTestKind::AppendStreamSegment,
        ],
        "copy should apply the expected destination command prefix before the gated commit"
    );

    let publishing_handle = handle.clone();
    let publishing_initial = Arc::clone(&initial);
    let publishing_node_root = tmp.path().to_path_buf();
    let publication_thread = thread::spawn(move || {
        install_next_epoch_runtime_map_with_historical_routes(
            &publishing_handle,
            &publishing_initial,
            &publishing_node_root,
        );
    });
    handle.test_wait_until_route_publication_is_pending();

    gate.release();
    let copy_result = copy_thread.join().unwrap().unwrap();
    publication_thread.join().unwrap();
    assert_eq!(copy_result.version_id, VersionId::Null);
    let after_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &dst_object_key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof.applied_log_index,
        paused_object_pg_proof.applied_log_index + 1,
        "copy crossing an epoch change should append exactly one destination commit command after the gate"
    );
    assert_ne!(
        after_object_pg_proof.applied_log_hash, before_object_pg_proof.applied_log_hash,
        "copy destination commit should change the object-PG command-log hash"
    );
    assert_ne!(
        after_object_pg_proof.state_digest, before_object_pg_proof.state_digest,
        "copy destination commit should change the object-PG materialized state digest"
    );

    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                dst_key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        result.body.read_all().unwrap(),
        b"copy-crosses-epoch-change"
    );
}

#[test]
fn delete_object_epoch_change_before_metadata_apply_commits_once_on_pinned_route() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("delete-object-before-metadata-apply");

    let bucket = "delete-epoch-change-bucket";
    let key = "key";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = Arc::new(
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap(),
    );
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, key, test_requester(), None),
            data: b"delete-me",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    let object_key = trusted_object_key(key);
    let before_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();

    let gate = DeterministicFaultGate::new(TOKEN);
    let hook_bucket = bucket_name.clone();
    let hook_key = object_key.clone();
    let gate_for_hook = Arc::clone(&gate);
    let _hook_guard =
        initial.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::DeleteObjectVersion
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
            {
                gate_for_hook.wait_at(TOKEN);
            }
            Ok(())
        }));

    let delete_coord = Arc::clone(&coord);
    let delete_thread = thread::spawn(move || {
        delete_coord.delete_object(&delete_object_request(
            bucket,
            key,
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
    });

    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);
    let paused_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        paused_object_pg_proof.applied_log_index, before_object_pg_proof.applied_log_index,
        "delete should not apply the object-PG command before the pre-apply gate"
    );

    let publication_thread = begin_next_epoch_runtime_map_publication_with_historical_routes(
        &handle,
        &initial,
        tmp.path(),
    );

    gate.release();
    delete_thread.join().unwrap().unwrap();
    publication_thread.join().unwrap();
    let after_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof.applied_log_index,
        before_object_pg_proof.applied_log_index + 1,
        "delete crossing an epoch change should append exactly one delete command after the gate"
    );
    assert_ne!(
        after_object_pg_proof.applied_log_hash, before_object_pg_proof.applied_log_hash,
        "delete command should change the object-PG command-log hash"
    );
    assert_ne!(
        after_object_pg_proof.state_digest, before_object_pg_proof.state_digest,
        "delete command should change the object-PG materialized state digest"
    );

    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                key,
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
fn complete_multipart_epoch_change_before_metadata_apply_commits_once_on_pinned_route() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("complete-multipart-before-metadata-apply");

    let bucket = "complete-multipart-epoch-change-bucket";
    let key = "key";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = Arc::new(
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap(),
    );
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let (upload_id, parts) =
        create_upload_with_parts(&coord, bucket, key, &[(1, b"multipart-crosses-epoch")]);

    let bucket_name = trusted_bucket_name(bucket);
    let object_key = trusted_object_key(key);
    let before_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();

    let gate = DeterministicFaultGate::new(TOKEN);
    let hook_bucket = bucket_name.clone();
    let hook_key = object_key.clone();
    let gate_for_hook = Arc::clone(&gate);
    let _hook_guard =
        initial.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::CommitMultipartObject
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
            {
                gate_for_hook.wait_at(TOKEN);
            }
            Ok(())
        }));

    let complete_coord = Arc::clone(&coord);
    let complete_thread = thread::spawn(move || {
        complete_coord.complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                bucket,
                key,
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
    });

    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);
    let paused_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        paused_object_pg_proof.applied_log_index, before_object_pg_proof.applied_log_index,
        "multipart completion should not apply an object-PG command before the pre-commit gate"
    );

    install_next_epoch_runtime_map_with_historical_routes(&handle, &initial, tmp.path());

    gate.release();
    let complete_result = complete_thread.join().unwrap().unwrap();
    assert_eq!(complete_result.version_id, VersionId::Null);
    let after_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof.applied_log_index,
        paused_object_pg_proof.applied_log_index + 1,
        "multipart completion crossing an epoch change should append exactly one commit command after the gate"
    );
    assert_ne!(
        after_object_pg_proof.applied_log_hash, before_object_pg_proof.applied_log_hash,
        "multipart completion command should change the object-PG command-log hash"
    );
    assert_ne!(
        after_object_pg_proof.state_digest, before_object_pg_proof.state_digest,
        "multipart completion command should change the object-PG materialized state digest"
    );

    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), b"multipart-crosses-epoch");
}

#[test]
fn upload_part_finalize_epoch_change_before_metadata_apply_commits_once_on_pinned_route() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("upload-part-finalize-before-metadata-apply");

    let bucket = "upload-part-finalize-epoch-change-bucket";
    let key = "key";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = Arc::new(
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap(),
    );
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let upload = coord
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
    let session = begin_stream_part_test(&coord, bucket, key, &upload.upload_id, 1).unwrap();
    let data = b"upload-part-finalize-crosses-epoch-change";
    coord
        .append_plaintext_stream_segment_for_test(bucket, key, &session.session_id, 0, data)
        .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    let object_key = trusted_object_key(key);
    let before_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();

    let gate = DeterministicFaultGate::new(TOKEN);
    let hook_bucket = bucket_name.clone();
    let hook_key = object_key.clone();
    let gate_for_hook = Arc::clone(&gate);
    let _hook_guard =
        initial.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::CommitStreamPart
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
            {
                gate_for_hook.wait_at(TOKEN);
            }
            Ok(())
        }));

    let finalize_coord = Arc::clone(&coord);
    let upload_id = upload.upload_id.clone();
    let session_id = session.session_id.clone();
    let finalize_thread = thread::spawn(move || {
        finalize_coord.finalize_stream_part(FinalizeStreamPartRequest {
            upload: multipart_object_request_with_expected_owner(
                bucket,
                key,
                &upload_id,
                test_requester(),
                None,
            ),
            session_id: &session_id,
            part_number: 1,
            crc64: checksum::crc64::checksum(data),
            total_size: data.len() as u64,
            claimed_checksum: None,
            computed_checksum: None,
        })
    });

    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);
    let paused_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        paused_object_pg_proof.applied_log_index, before_object_pg_proof.applied_log_index,
        "UploadPart finalization should not apply an object-PG command before the pre-commit gate"
    );

    install_next_epoch_runtime_map_with_historical_routes(&handle, &initial, tmp.path());

    gate.release();
    let part = finalize_thread.join().unwrap().unwrap();
    let after_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof.applied_log_index,
        paused_object_pg_proof.applied_log_index + 1,
        "UploadPart finalization crossing an epoch change should append exactly one part commit after the gate"
    );
    assert_ne!(
        after_object_pg_proof.applied_log_hash, before_object_pg_proof.applied_log_hash,
        "UploadPart finalization command should change the object-PG command-log hash"
    );
    assert_ne!(
        after_object_pg_proof.state_digest, before_object_pg_proof.state_digest,
        "UploadPart finalization command should change the object-PG materialized state digest"
    );

    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                bucket,
                key,
                &upload.upload_id,
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
    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), data);
}

#[test]
fn upload_part_copy_epoch_change_before_metadata_apply_commits_once_on_pinned_route() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("upload-part-copy-before-metadata-apply");

    let bucket = "upload-part-copy-epoch-change-bucket";
    let src_key = "src";
    let dst_key = "dst";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = Arc::new(
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap(),
    );
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let source_payload = b"upload-part-copy-crosses-epoch-change";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, src_key, test_requester(), None),
            data: source_payload,
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
            object: object_request_with_expected_owner(bucket, dst_key, test_requester(), None),
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

    let bucket_name = trusted_bucket_name(bucket);
    let dst_object_key = trusted_object_key(dst_key);
    let before_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &dst_object_key)
        .unwrap();

    let gate = DeterministicFaultGate::new(TOKEN);
    let pre_commit_kinds = Arc::new(Mutex::new(Vec::new()));
    let hook_bucket = bucket_name.clone();
    let hook_key = dst_object_key.clone();
    let gate_for_hook = Arc::clone(&gate);
    let pre_commit_kinds_for_hook = Arc::clone(&pre_commit_kinds);
    let _hook_guard =
        initial.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
            {
                if context.kind == MetadataCommandApplyTestKind::CommitStreamPart {
                    gate_for_hook.wait_at(TOKEN);
                } else {
                    let mut kinds = pre_commit_kinds_for_hook.lock().unwrap();
                    if kinds.last() != Some(&context.kind) {
                        kinds.push(context.kind);
                    }
                }
            }
            Ok(())
        }));

    let copy_coord = Arc::clone(&coord);
    let upload_id = upload.upload_id.clone();
    let copy_thread = thread::spawn(move || {
        copy_coord.upload_part_copy(&UploadPartCopyRequest {
            source: copy_source(bucket, src_key, None),
            upload: multipart_object_request_with_expected_owner(
                bucket,
                dst_key,
                &upload_id,
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

    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);
    let paused_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &dst_object_key)
        .unwrap();
    assert_eq!(
        paused_object_pg_proof.applied_log_index,
        before_object_pg_proof.applied_log_index + 2,
        "UploadPartCopy should have created a destination stream and appended copied data before the part commit gate"
    );
    assert_eq!(
        pre_commit_kinds.lock().unwrap().as_slice(),
        &[
            MetadataCommandApplyTestKind::CreateStreamUpload,
            MetadataCommandApplyTestKind::AppendStreamSegment,
        ],
        "UploadPartCopy should apply the expected destination command prefix before the gated part commit"
    );

    let publishing_handle = handle.clone();
    let publishing_initial = Arc::clone(&initial);
    let publishing_node_root = tmp.path().to_path_buf();
    let publication_thread = thread::spawn(move || {
        install_next_epoch_runtime_map_with_historical_routes(
            &publishing_handle,
            &publishing_initial,
            &publishing_node_root,
        );
    });
    handle.test_wait_until_route_publication_is_pending();

    gate.release();
    let part = copy_thread.join().unwrap().unwrap();
    publication_thread.join().unwrap();
    let after_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &dst_object_key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof.applied_log_index,
        paused_object_pg_proof.applied_log_index + 1,
        "UploadPartCopy crossing an epoch change should append exactly one part commit after the gate"
    );
    assert_ne!(
        after_object_pg_proof.applied_log_hash, before_object_pg_proof.applied_log_hash,
        "UploadPartCopy part commit should change the object-PG command-log hash"
    );
    assert_ne!(
        after_object_pg_proof.state_digest, before_object_pg_proof.state_digest,
        "UploadPartCopy part commit should change the object-PG materialized state digest"
    );

    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                bucket,
                dst_key,
                &upload.upload_id,
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
    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                dst_key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), source_payload);
}

#[test]
fn put_object_tags_epoch_change_before_metadata_apply_commits_once_on_pinned_route() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("put-object-tags-before-metadata-apply");

    let bucket = "put-tags-epoch-change-bucket";
    let key = "key";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = Arc::new(
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap(),
    );
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, key, test_requester(), None),
            data: b"tagged-across-epoch",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    let object_key = trusted_object_key(key);
    let object_pg = initial.test_object_pg_id_for(&bucket_name, &object_key);
    let primary_node = initial
        .local_pg_route(PgId::new(object_pg))
        .unwrap()
        .primary_node_id();
    let before_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();

    let gate = DeterministicFaultGate::new(TOKEN);
    let hook_bucket = bucket_name.clone();
    let hook_key = object_key.clone();
    let gate_for_hook = Arc::clone(&gate);
    let _hook_guard =
        initial.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::PutObjectMetadata
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && context.node_id == primary_node
            {
                gate_for_hook.wait_at(TOKEN);
            }
            Ok(())
        }));

    let tag_coord = Arc::clone(&coord);
    let tag_thread = thread::spawn(move || {
        put_object_tags_test(
            &tag_coord,
            bucket,
            key,
            None,
            "<Tagging><TagSet><Tag><Key>epoch</Key><Value>changed</Value></Tag></TagSet></Tagging>",
            test_requester(),
            None,
        )
    });

    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);
    let paused_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        paused_object_pg_proof.applied_log_index, before_object_pg_proof.applied_log_index,
        "tag update should not apply the object-PG metadata command before the pre-apply gate"
    );

    let publication_thread = begin_next_epoch_runtime_map_publication_with_historical_routes(
        &handle,
        &initial,
        tmp.path(),
    );

    gate.release();
    tag_thread.join().unwrap().unwrap();
    publication_thread.join().unwrap();
    let after_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof.applied_log_index,
        paused_object_pg_proof.applied_log_index + 1,
        "tag update crossing an epoch change should append exactly one object metadata command after the gate"
    );
    assert_ne!(
        after_object_pg_proof.applied_log_hash, before_object_pg_proof.applied_log_hash,
        "tag update command should change the object-PG command-log hash"
    );
    assert_ne!(
        after_object_pg_proof.state_digest, before_object_pg_proof.state_digest,
        "tag update command should change the object-PG materialized state digest"
    );

    let tags = get_object_tags_test(&coord, bucket, key, None, test_requester(), None).unwrap();
    assert_eq!(
        tags.as_deref(),
        Some(
            "<Tagging><TagSet><Tag><Key>epoch</Key><Value>changed</Value></Tag></TagSet></Tagging>"
        )
    );
}

#[test]
fn put_object_legal_hold_epoch_change_before_metadata_apply_commits_once_on_pinned_route() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("put-object-legal-hold-before-metadata-apply");

    let bucket = "put-legal-hold-epoch-change-bucket";
    let key = "key";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = Arc::new(
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap(),
    );
    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name(bucket),
            requester: test_requester(),
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
            object: object_request_with_expected_owner(bucket, key, test_requester(), None),
            data: b"legal-hold-across-epoch",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    let object_key = trusted_object_key(key);
    let object_pg = initial.test_object_pg_id_for(&bucket_name, &object_key);
    let primary_node = initial
        .local_pg_route(PgId::new(object_pg))
        .unwrap()
        .primary_node_id();
    let before_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();

    let gate = DeterministicFaultGate::new(TOKEN);
    let hook_bucket = bucket_name.clone();
    let hook_key = object_key.clone();
    let gate_for_hook = Arc::clone(&gate);
    let _hook_guard =
        initial.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::PutObjectMetadata
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && context.node_id == primary_node
            {
                gate_for_hook.wait_at(TOKEN);
            }
            Ok(())
        }));

    let legal_hold_coord = Arc::clone(&coord);
    let legal_hold_thread = thread::spawn(move || {
        put_object_legal_hold_test(
            &legal_hold_coord,
            bucket,
            key,
            Some(put.version_id),
            LegalHoldStatus::On,
            test_requester(),
        )
    });

    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);
    let paused_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        paused_object_pg_proof.applied_log_index, before_object_pg_proof.applied_log_index,
        "legal-hold update should not apply the object-PG metadata command before the pre-apply gate"
    );

    let publication_thread = begin_next_epoch_runtime_map_publication_with_historical_routes(
        &handle,
        &initial,
        tmp.path(),
    );

    gate.release();
    legal_hold_thread.join().unwrap().unwrap();
    publication_thread.join().unwrap();
    let after_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof.applied_log_index,
        paused_object_pg_proof.applied_log_index + 1,
        "legal-hold update crossing an epoch change should append exactly one object metadata command after the gate"
    );
    assert_ne!(
        after_object_pg_proof.applied_log_hash, before_object_pg_proof.applied_log_hash,
        "legal-hold update command should change the object-PG command-log hash"
    );
    assert_ne!(
        after_object_pg_proof.state_digest, before_object_pg_proof.state_digest,
        "legal-hold update command should change the object-PG materialized state digest"
    );

    let legal_hold =
        get_object_legal_hold_test(&coord, bucket, key, Some(put.version_id), test_requester())
            .unwrap();
    assert_eq!(legal_hold, Some(LegalHoldStatus::On));
}

#[test]
fn put_object_retention_epoch_change_before_metadata_apply_commits_once_on_pinned_route() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("put-object-retention-before-metadata-apply");

    let bucket = "put-retention-epoch-change-bucket";
    let key = "key";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = Arc::new(
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap(),
    );
    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name(bucket),
            requester: test_requester(),
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
            object: object_request_with_expected_owner(bucket, key, test_requester(), None),
            data: b"retention-across-epoch",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    let object_key = trusted_object_key(key);
    let object_pg = initial.test_object_pg_id_for(&bucket_name, &object_key);
    let primary_node = initial
        .local_pg_route(PgId::new(object_pg))
        .unwrap()
        .primary_node_id();
    let before_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();

    let gate = DeterministicFaultGate::new(TOKEN);
    let hook_bucket = bucket_name.clone();
    let hook_key = object_key.clone();
    let gate_for_hook = Arc::clone(&gate);
    let _hook_guard =
        initial.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::PutObjectMetadata
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && context.node_id == primary_node
            {
                gate_for_hook.wait_at(TOKEN);
            }
            Ok(())
        }));

    let retention = ObjectRetention {
        mode: ObjectLockMode::Governance,
        retain_until_unix_seconds: Coordinator::current_unix_seconds().unwrap() + 3600,
    };
    let retention_coord = Arc::clone(&coord);
    let retention_thread = thread::spawn(move || {
        put_object_retention_test(
            &retention_coord,
            bucket,
            key,
            Some(put.version_id),
            retention,
            false,
            test_requester(),
        )
    });

    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);
    let paused_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        paused_object_pg_proof.applied_log_index, before_object_pg_proof.applied_log_index,
        "retention update should not apply the object-PG metadata command before the pre-apply gate"
    );

    let publication_thread = begin_next_epoch_runtime_map_publication_with_historical_routes(
        &handle,
        &initial,
        tmp.path(),
    );

    gate.release();
    retention_thread.join().unwrap().unwrap();
    publication_thread.join().unwrap();
    let after_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof.applied_log_index,
        paused_object_pg_proof.applied_log_index + 1,
        "retention update crossing an epoch change should append exactly one object metadata command after the gate"
    );
    assert_ne!(
        after_object_pg_proof.applied_log_hash, before_object_pg_proof.applied_log_hash,
        "retention update command should change the object-PG command-log hash"
    );
    assert_ne!(
        after_object_pg_proof.state_digest, before_object_pg_proof.state_digest,
        "retention update command should change the object-PG materialized state digest"
    );

    let fetched =
        get_object_retention_test(&coord, bucket, key, Some(put.version_id), test_requester())
            .unwrap();
    assert_eq!(fetched, Some(retention));
}

#[test]
fn put_object_acl_epoch_change_before_metadata_apply_commits_once_on_pinned_route() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("put-object-acl-before-metadata-apply");

    let bucket = "put-acl-epoch-change-bucket";
    let key = "key";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = Arc::new(
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap(),
    );
    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name(bucket),
            requester: test_requester(),
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
            object: object_request_with_expected_owner(bucket, key, test_requester(), None),
            data: b"acl-across-epoch",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    let object_key = trusted_object_key(key);
    let object_pg = initial.test_object_pg_id_for(&bucket_name, &object_key);
    let primary_node = initial
        .local_pg_route(PgId::new(object_pg))
        .unwrap()
        .primary_node_id();
    let before_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();

    let gate = DeterministicFaultGate::new(TOKEN);
    let hook_bucket = bucket_name.clone();
    let hook_key = object_key.clone();
    let gate_for_hook = Arc::clone(&gate);
    let _hook_guard =
        initial.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::PutObjectMetadata
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && context.node_id == primary_node
            {
                gate_for_hook.wait_at(TOKEN);
            }
            Ok(())
        }));

    let acl_coord = Arc::clone(&coord);
    let acl_thread = thread::spawn(move || {
        put_object_canned_acl_test(
            &acl_coord,
            bucket,
            key,
            Some(put.version_id),
            PutObjectAcl::PublicRead,
            test_requester(),
            None,
        )
    });

    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);
    let paused_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        paused_object_pg_proof.applied_log_index, before_object_pg_proof.applied_log_index,
        "ACL update should not apply the object-PG metadata command before the pre-apply gate"
    );

    let publication_thread = begin_next_epoch_runtime_map_publication_with_historical_routes(
        &handle,
        &initial,
        tmp.path(),
    );

    gate.release();
    acl_thread.join().unwrap().unwrap();
    publication_thread.join().unwrap();
    let after_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof.applied_log_index,
        paused_object_pg_proof.applied_log_index + 1,
        "ACL update crossing an epoch change should append exactly one object metadata command after the gate"
    );
    assert_ne!(
        after_object_pg_proof.applied_log_hash, before_object_pg_proof.applied_log_hash,
        "ACL update command should change the object-PG command-log hash"
    );
    assert_ne!(
        after_object_pg_proof.state_digest, before_object_pg_proof.state_digest,
        "ACL update command should change the object-PG materialized state digest"
    );

    let fetched = get_object_acl_test(
        &coord,
        bucket,
        key,
        Some(put.version_id),
        test_requester(),
        None,
    )
    .unwrap();
    assert_eq!(fetched.version_id, put.version_id);
    assert!(
        fetched.acl_grants.allows_all_users(AclPermission::Read),
        "public-read canned ACL must be visible after the route-crossing update"
    );
}

#[test]
fn get_object_epoch_change_after_read_snapshot_uses_pinned_route() {
    let bucket = "get-epoch-change-bucket";
    let key = "key";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, key, test_requester(), None),
            data: b"get-crosses-epoch-change",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let hook_handle = handle.clone();
    let hook_initial = Arc::clone(&initial);
    let hook_node_root = tmp.path().to_path_buf();
    let publication_thread = Arc::new(Mutex::new(None));
    let hook_publication_thread = Arc::clone(&publication_thread);
    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), key.to_string())),
        after_object_read_snapshot: Some(Arc::new(move || {
            let publishing_handle = hook_handle.clone();
            let publishing_initial = Arc::clone(&hook_initial);
            let publishing_node_root = hook_node_root.clone();
            let thread = thread::spawn(move || {
                install_next_epoch_runtime_map_with_historical_routes(
                    &publishing_handle,
                    &publishing_initial,
                    &publishing_node_root,
                );
            });
            *hook_publication_thread.lock().unwrap() = Some(thread);
            hook_handle.test_wait_until_route_publication_is_pending();
        })),
        ..ReclamationTestHooks::default()
    });

    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    publication_thread
        .lock()
        .unwrap()
        .take()
        .expect("GetObject hook should start route publication")
        .join()
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), b"get-crosses-epoch-change");
}

#[test]
fn head_object_epoch_change_after_read_snapshot_uses_pinned_route() {
    let bucket = "head-epoch-change-bucket";
    let key = "key";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, key, test_requester(), None),
            data: b"head-crosses-epoch-change",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let hook_handle = handle.clone();
    let hook_initial = Arc::clone(&initial);
    let hook_node_root = tmp.path().to_path_buf();
    let publication_thread = Arc::new(Mutex::new(None));
    let hook_publication_thread = Arc::clone(&publication_thread);
    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), key.to_string())),
        after_object_read_snapshot: Some(Arc::new(move || {
            let publishing_handle = hook_handle.clone();
            let publishing_initial = Arc::clone(&hook_initial);
            let publishing_node_root = hook_node_root.clone();
            let thread = thread::spawn(move || {
                install_next_epoch_runtime_map_with_historical_routes(
                    &publishing_handle,
                    &publishing_initial,
                    &publishing_node_root,
                );
            });
            *hook_publication_thread.lock().unwrap() = Some(thread);
            hook_handle.test_wait_until_route_publication_is_pending();
        })),
        ..ReclamationTestHooks::default()
    });

    let result = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    publication_thread
        .lock()
        .unwrap()
        .take()
        .expect("HeadObject hook should start route publication")
        .join()
        .unwrap();
    assert_eq!(result.size, b"head-crosses-epoch-change".len() as u64);
    assert_eq!(result.version_id, VersionId::Null);
}

#[test]
fn get_body_created_before_unix_data_pg_move_uses_retained_route_on_first_read() {
    let tmp = test_util::tempdir();
    let old_acting_set = vec![NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let moved_acting_set = vec![NodeId::new(3), NodeId::new(4), NodeId::new(5)];
    let node_ids = (0..6).map(NodeId::new).collect::<Vec<_>>();
    let pg_ids = vec![0, 1];
    let metadata_pg_id = 0;
    let moved_data_pg_id = 1;
    let ec_shape = storage::EcShape { k: 2, m: 1 };
    let current_epoch = ClusterEpoch::INITIAL;
    let next_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
    let configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join(format!("remote-read-node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let current_routes = pg_ids
        .iter()
        .map(|pg_id| {
            storage::control_plane::PgRouteSnapshot::reconstructed(
                current_epoch,
                PgId::new(*pg_id),
                NodeId::new(0),
                old_acting_set.clone(),
                PgState::Active,
            )
        })
        .collect::<Vec<_>>();
    let current_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs.clone(),
        &pg_ids,
        ec_shape,
        current_epoch,
        current_routes.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    let current_cluster = StorageCluster::from_local_map(Arc::new(current_map)).unwrap();
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&current_cluster));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();

    let (bucket, key) = find_bucket_key_for_metadata_and_data_pg(
        &current_cluster,
        metadata_pg_id,
        moved_data_pg_id,
    );
    coord
        .create_bucket_for_owner("default-owner", &bucket, false)
        .unwrap();
    let payload = b"coordinator remote unix read uses retained placement epoch";
    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(&bucket, &key, test_requester(), None),
            data: payload,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let bucket_name = trusted_bucket_name(&bucket);
    let object_key = trusted_object_key(&key);
    let segment = current_cluster
        .test_get_object_segments(&bucket_name, &object_key, put.version_id)
        .unwrap()
        .pop()
        .expect("direct PUT should record one segment");
    assert_eq!(segment.data_pg_id, moved_data_pg_id);
    assert_eq!(segment.placement_cluster_epoch, current_epoch);

    let next_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let (primary, acting_set) = if *pg_id == moved_data_pg_id {
                (NodeId::new(3), moved_acting_set.clone())
            } else {
                (NodeId::new(0), old_acting_set.clone())
            };
            storage::control_plane::PgRouteSnapshot::reconstructed(
                next_epoch,
                PgId::new(*pg_id),
                primary,
                acting_set,
                PgState::Active,
            )
        })
        .collect::<Vec<_>>();
    let socket_dir = tmp.path().join("remote-read-sockets");
    make_private_socket_dir(&socket_dir);
    let server_route_validity = long_lived_test_route_map_validity();
    let stop = Arc::new(AtomicBool::new(false));
    let mut server_threads = Vec::new();
    let mut wake_socket_paths = Vec::new();
    let mut servers = Vec::new();
    let mut client_configs = Vec::new();
    for config in &configs {
        let socket_path = socket_dir.join(format!("node-{}.sock", config.node_id().as_u32()));
        let server_config = StorageNodeProcessConfig::new(StorageNodeProcessConfigParts {
            node_id: config.node_id(),
            cluster_epoch: current_epoch,
            route_map_validity: server_route_validity,
            data_dir: config.data_dir().to_path_buf(),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.clone(),
            socket_path: socket_path.clone(),
            pg_routes: current_routes
                .iter()
                .map(StorageNodePgRoute::from)
                .collect(),
            historical_pg_routes: Vec::new(),
            pending_metadata_command_recoveries: Vec::new(),
        })
        .unwrap();
        let server = Arc::new(StorageNodeServer::bind(server_config).unwrap());
        for _ in 0..4 {
            server_threads.push(spawn_storage_node_server_loop(
                Arc::clone(&server),
                Arc::clone(&stop),
            ));
            wake_socket_paths.push(socket_path.clone());
        }
        servers.push(Arc::clone(&server));
        client_configs.push(LocalUnixStorageNodeClientConfig::new(
            config.node_id(),
            socket_path,
        ));
    }

    let mut current_unix_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs.clone(),
        &pg_ids,
        ec_shape,
        current_epoch,
        current_routes.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    current_unix_map
        .install_unix_storage_node_clients(client_configs.clone())
        .unwrap();
    current_unix_map.test_set_route_map_validity(long_lived_test_route_map_validity());
    let current_unix_cluster = StorageCluster::from_local_map(Arc::new(current_unix_map)).unwrap();
    handle.install(current_unix_cluster).unwrap();

    let read = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                &bucket,
                &key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();

    // Force the first retained body read to reconstruct from parity. Besides
    // proving the payload RPC uses the exact historical route after the map
    // advance, this pins that repair reporting cannot fall back to an active
    // epoch-N mutation once that frontend generation has been unpublished.
    let corrupt_path = current_cluster
        .test_payload_shard_file_path(
            segment.data_pg_id,
            ec_shape,
            &segment.segment_okh,
            segment.segment_vid,
            0,
        )
        .unwrap();
    let mut corrupt_bytes = std::fs::read(&corrupt_path).unwrap();
    assert!(!corrupt_bytes.is_empty(), "placed shard must not be empty");
    corrupt_bytes[0] ^= 0xff;
    std::fs::write(&corrupt_path, corrupt_bytes).unwrap();

    for (server, config) in servers.iter().zip(&configs) {
        let socket_path = socket_dir.join(format!("node-{}.sock", config.node_id().as_u32()));
        let next_server_config = StorageNodeProcessConfig::new(StorageNodeProcessConfigParts {
            node_id: config.node_id(),
            cluster_epoch: next_epoch,
            route_map_validity: server_route_validity,
            data_dir: config.data_dir().to_path_buf(),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.clone(),
            socket_path,
            pg_routes: next_routes.iter().map(StorageNodePgRoute::from).collect(),
            historical_pg_routes: current_routes
                .iter()
                .map(StorageNodePgRoute::from)
                .collect(),
            pending_metadata_command_recoveries: Vec::new(),
        })
        .unwrap();
        server
            .install_control_plane_runtime_config(next_server_config)
            .unwrap();
    }

    let mut next_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs,
        &pg_ids,
        ec_shape,
        next_epoch,
        next_routes.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    next_map.test_install_historical_pg_routes(current_routes);
    next_map
        .install_unix_storage_node_clients(client_configs)
        .unwrap();
    next_map.test_set_route_map_validity(long_lived_test_route_map_validity());
    let next_cluster = StorageCluster::from_local_map(Arc::new(next_map)).unwrap();
    handle.install(next_cluster).unwrap();

    assert_eq!(read.body.read_all().unwrap(), payload);
    let head = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                &bucket,
                &key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(head.size, payload.len() as u64);
    let listed = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(&bucket, test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();
    let listed_keys = listed
        .objects
        .iter()
        .map(|object| object.key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(listed_keys, [key.as_str()]);
    assert!(!listed.is_truncated);
    let versions = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner(&bucket, test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();
    let version_keys = versions
        .versions
        .iter()
        .map(|version| version.key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(version_keys, [key.as_str()]);
    assert!(!versions.is_truncated);

    stop_storage_node_server_loops(stop, &wake_socket_paths, server_threads);
}

#[test]
fn list_objects_epoch_change_before_storage_list_uses_pinned_route() {
    let bucket = "list-epoch-change-bucket";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let pg_ids = initial.test_pg_ids();
    assert!(
        pg_ids.len() >= 2,
        "test requires at least two object metadata PGs"
    );
    let key_a = find_key_for_object_metadata_pg_with_prefix(&initial, bucket, pg_ids[0], "a/");
    let key_b = find_key_for_object_metadata_pg_with_prefix(&initial, bucket, pg_ids[1], "b/");
    let key_c = find_key_for_object_metadata_pg_with_prefix(&initial, bucket, pg_ids[0], "c/");
    assert_ne!(
        initial.test_object_pg_id_for(&trusted_bucket_name(bucket), &trusted_object_key(&key_a)),
        initial.test_object_pg_id_for(&trusted_bucket_name(bucket), &trusted_object_key(&key_b)),
        "test fixture must span multiple object metadata PGs"
    );

    for key in [&key_a, &key_b, &key_c] {
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(bucket, key, test_requester(), None),
                data: key.as_bytes(),
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let candidate_tmp = test_util::tempdir();
    let hook_handle = handle.clone();
    let hook_initial = Arc::clone(&initial);
    let hook_node_root = candidate_tmp.path().to_path_buf();
    let publication_thread = Arc::new(Mutex::new(None));
    let hook_publication_thread = Arc::clone(&publication_thread);
    let _serial = LIST_OBJECTS_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_list_objects_test_hooks(ListObjectsTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_list: Some(Arc::new(move || {
            let publishing_handle = hook_handle.clone();
            let publishing_initial = Arc::clone(&hook_initial);
            let publishing_node_root = hook_node_root.clone();
            let thread = thread::spawn(move || {
                install_next_epoch_runtime_map_with_historical_routes(
                    &publishing_handle,
                    &publishing_initial,
                    &publishing_node_root,
                );
            });
            *hook_publication_thread.lock().unwrap() = Some(thread);
            hook_handle.test_wait_until_route_publication_is_pending();
        })),
    });

    let result = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();
    publication_thread
        .lock()
        .unwrap()
        .take()
        .expect("listing hook should start route publication")
        .join()
        .unwrap();
    let keys = result
        .objects
        .iter()
        .map(|object| object.key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(keys, [key_a.as_str(), key_b.as_str(), key_c.as_str()]);
    assert!(!result.is_truncated);
}

#[test]
fn list_objects_continuation_survives_epoch_change_between_pages() {
    let bucket = "list-continuation-epoch-change-bucket";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    for key in ["a/1", "a/2", "a/3", "a/4"] {
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(bucket, key, test_requester(), None),
                data: key.as_bytes(),
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let first = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: Some("a/"),
            delimiter: None,
            continuation_token: None,
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    let first_keys = first
        .objects
        .iter()
        .map(|object| object.key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(first_keys, ["a/1", "a/2"]);
    assert!(first.is_truncated);
    assert_eq!(first.next_continuation_token.as_deref(), Some("a/2"));

    install_same_store_next_epoch_runtime_map(&handle, &initial, tmp.path());

    let second = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: Some("a/"),
            delimiter: None,
            continuation_token: first.next_continuation_token.as_deref(),
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    let second_keys = second
        .objects
        .iter()
        .map(|object| object.key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(second_keys, ["a/3", "a/4"]);
    assert!(!second.is_truncated);
    assert_eq!(second.next_continuation_token, None);
}

#[test]
fn list_objects_paginates_global_order_when_smallest_keys_are_on_last_pg() {
    let bucket = "list-global-order-bucket";
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0, 1, 2]);
    let pg_ids = storage_cluster.test_pg_ids().to_vec();
    let coord = setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
        Arc::clone(&storage_cluster),
    );
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let mut expected = Vec::new();
    for (pg_id, lexical_prefix) in [(pg_ids[0], "z/"), (pg_ids[1], "m/"), (pg_ids[2], "a/")] {
        for index in 0..4 {
            let key = find_key_for_object_metadata_pg_with_prefix(
                &storage_cluster,
                bucket,
                pg_id,
                &format!("{lexical_prefix}{index}/"),
            );
            test_helpers::put_object(
                &coord,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: object_request_with_expected_owner(
                        bucket,
                        &key,
                        test_requester(),
                        None,
                    ),
                    data: key.as_bytes(),
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,
                    acl: NO_PUT_OBJECT_ACL.into(),
                },
            )
            .unwrap();
            expected.push(key);
        }
    }
    expected.sort();

    let mut actual = Vec::new();
    let mut continuation_token = None;
    loop {
        let page = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
                prefix: None,
                delimiter: None,
                continuation_token: continuation_token.as_deref(),
                max_keys: 2,
                requested_max_keys: Some(2),
            })
            .unwrap();
        actual.extend(page.objects.into_iter().map(|object| object.key));
        if !page.is_truncated {
            break;
        }
        continuation_token = page.next_continuation_token;
        assert!(continuation_token.is_some());
        assert!(actual.len() <= expected.len());
    }

    assert_eq!(actual, expected);
}

#[test]
fn list_multipart_uploads_paginates_global_order_when_smallest_keys_are_on_last_pg() {
    let bucket = "mpu-list-global-order-bucket";
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0, 1, 2]);
    let pg_ids = storage_cluster.test_pg_ids().to_vec();
    let coord = setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
        Arc::clone(&storage_cluster),
    );
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let mut expected = Vec::new();
    for (pg_id, lexical_prefix) in [(pg_ids[0], "z/"), (pg_ids[1], "m/"), (pg_ids[2], "a/")] {
        for index in 0..4 {
            let key = find_key_for_object_metadata_pg_with_prefix(
                &storage_cluster,
                bucket,
                pg_id,
                &format!("{lexical_prefix}{index}/"),
            );
            let upload = coord
                .create_multipart_upload(&CreateMultipartUploadRequest {
                    object: object_request_with_expected_owner(
                        bucket,
                        &key,
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
            expected.push((key, upload.upload_id));
        }
    }
    expected.sort_by(|left, right| left.0.cmp(&right.0));

    let mut actual = Vec::new();
    let mut key_marker = None;
    let mut upload_id_marker = None;
    loop {
        let page = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest {
                bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
                prefix: None,
                delimiter: None,
                key_marker: key_marker.as_deref(),
                upload_id_marker: upload_id_marker.clone(),
                max_uploads: 2,
            })
            .unwrap();
        actual.extend(
            page.uploads
                .into_iter()
                .map(|upload| (upload.key, upload.upload_id)),
        );
        if !page.is_truncated {
            break;
        }
        let Some(ListMultipartUploadsNextMarker::Upload { key, upload_id }) = page.next_marker
        else {
            panic!("truncated upload page must end at an upload marker");
        };
        key_marker = Some(key);
        upload_id_marker = Some(upload_id);
        assert!(actual.len() <= expected.len());
    }

    assert_eq!(actual, expected);
}

#[test]
fn list_objects_delimiter_continuation_survives_epoch_change_between_pages() {
    let bucket = "list-delimiter-continuation-epoch-change-bucket";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let pg_ids = initial.test_pg_ids();
    assert!(
        pg_ids.len() >= 2,
        "test requires at least two object metadata PGs"
    );
    let key_a = find_key_for_object_metadata_pg_with_prefix(&initial, bucket, pg_ids[0], "a/");
    let key_b = find_key_for_object_metadata_pg_with_prefix(&initial, bucket, pg_ids[1], "b/");
    let key_c = find_key_for_object_metadata_pg_with_prefix(&initial, bucket, pg_ids[0], "c/");
    let root_key =
        find_key_for_object_metadata_pg_with_prefix(&initial, bucket, pg_ids[1], "z-root-");

    for key in [&key_a, &key_b, &key_c, &root_key] {
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(bucket, key, test_requester(), None),
                data: key.as_bytes(),
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let first = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            continuation_token: None,
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    assert!(first.objects.is_empty());
    assert_eq!(first.common_prefixes, ["a/".to_string(), "b/".to_string()]);
    assert!(first.is_truncated);
    assert_eq!(first.next_continuation_token.as_deref(), Some("b/"));

    install_same_store_next_epoch_runtime_map(&handle, &initial, tmp.path());

    let second = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            continuation_token: first.next_continuation_token.as_deref(),
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    let second_keys = second
        .objects
        .iter()
        .map(|object| object.key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(second.common_prefixes, ["c/".to_string()]);
    assert_eq!(second_keys, [root_key.as_str()]);
    assert!(!second.is_truncated);
    assert_eq!(second.next_continuation_token, None);
}

#[test]
fn read_and_list_fail_closed_while_object_metadata_pg_is_peering() {
    let bucket = "object-peering-read-bucket";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let key = find_key_with_object_pg_ne_bucket_pg(&coord, bucket, "peering-key");
    let peering_pg = object_pg_id(&coord, bucket, &key);
    let bucket_pg = bucket_pg_id(&coord, bucket);
    assert_ne!(
        peering_pg, bucket_pg,
        "test must keep the bucket PG active while the object metadata PG peers"
    );

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, &key, test_requester(), None),
            data: b"must-not-be-served-from-peering-pg",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let current_epoch = install_same_store_next_epoch_runtime_map_with_peering_pg(
        &handle,
        &initial,
        tmp.path(),
        peering_pg,
    );
    assert_eq!(
        handle
            .current()
            .local_pg_route(PgId::new(peering_pg))
            .unwrap()
            .state(),
        PgState::Peering
    );

    let assert_pg_not_active = |operation: &str, error: ServerError| {
        assert!(
            matches!(error, ServerError::OperationAborted),
            "{operation} should fail closed with a retryable response on Peering object metadata PG {peering_pg} at epoch {current_epoch:?}, got {error:?}"
        );
    };

    let get_error = coord
        .get_object(&GetObjectRequest {
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
        .unwrap_err();
    assert_pg_not_active("GET", get_error);

    let head_error = coord
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
        .unwrap_err();
    assert_pg_not_active("HEAD", head_error);

    let list_error = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap_err();
    assert_pg_not_active("LIST", list_error);

    let version_list_error = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap_err();
    assert_pg_not_active("LIST versions", version_list_error);
}

#[test]
fn large_put_object_pins_runtime_map_after_stream_session_create() {
    let bucket = "large-put-pinned-bucket";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let candidate_tmp = test_util::tempdir();
    let candidate = make_dynamic_runtime_map_candidate(open_test_storage_cluster(
        candidate_tmp.path(),
        &[0, 1],
    ));
    let hook_handle = handle.clone();
    let _serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = coord.install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
        bucket: Some(bucket.to_string()),
        after_loaded: Some(Arc::new(move || {
            hook_handle.install(Arc::clone(&candidate)).unwrap();
        })),
        ..BucketWriteHandleTestHooks::default()
    });

    let metadata = MetadataBlob::new();
    let data = vec![b'x'; INTERNAL_SEGMENT_SIZE + 1];
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, "large", test_requester(), None),
            data: &data,
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    handle
        .install(make_dynamic_runtime_map_candidate(initial))
        .unwrap();
    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "large",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), data);
}

#[test]
fn streaming_upload_part_pins_runtime_map_after_session_create() {
    let bucket = "stream-part-pinned-bucket";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(bucket, "key", test_requester(), None),
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

    let storage_node = coord.storage_node_for_request();
    let candidate_tmp = test_util::tempdir();
    let candidate = make_dynamic_runtime_map_candidate(open_test_storage_cluster(
        candidate_tmp.path(),
        &[0, 1],
    ));
    let hook_handle = handle.clone();
    let _serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let session = {
        let _hook_guard =
            coord.install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
                bucket: Some(bucket.to_string()),
                after_loaded: Some(Arc::new(move || {
                    hook_handle.install(Arc::clone(&candidate)).unwrap();
                })),
                ..BucketWriteHandleTestHooks::default()
            });

        coord
            .begin_stream_part_with_storage_node(
                &storage_node,
                &BeginStreamPartRequest {
                    upload: multipart_object_request_with_expected_owner(
                        bucket,
                        "key",
                        &upload.upload_id,
                        test_requester(),
                        None,
                    ),
                    part_number: 1,
                    policy_context: PutObjectPolicyContext::default(),
                    sse_customer: None,
                },
            )
            .unwrap()
    };
    let data = b"streaming-upload-part-pinned-runtime-map";
    coord
        .append_stream_part_data_with_storage_node(
            &storage_node,
            &AppendStreamPartRequest {
                bucket: BucketName::new(bucket).unwrap(),
                key: ObjectKey::new("key").unwrap(),
                upload_id: &upload.upload_id,
                session_id: &session.session_id,
                part_number: 1,
                segment_index: 0,
                data,
                sse_customer: None,
            },
        )
        .unwrap();
    let crc64 = checksum::crc64::checksum(data);
    let part = coord
        .finalize_stream_part_with_storage_node(
            &storage_node,
            FinalizeStreamPartRequest {
                upload: multipart_object_request_with_expected_owner(
                    bucket,
                    "key",
                    &upload.upload_id,
                    test_requester(),
                    None,
                ),
                session_id: &session.session_id,
                part_number: 1,
                crc64,
                total_size: data.len() as u64,
                claimed_checksum: None,
                computed_checksum: None,
            },
        )
        .unwrap();

    handle
        .install(make_dynamic_runtime_map_candidate(initial))
        .unwrap();
    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                bucket,
                "key",
                &upload.upload_id,
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
    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), data);
}

#[test]
fn complete_multipart_upload_pins_runtime_map_between_snapshot_and_commit() {
    let bucket = "complete-multipart-pinned-bucket";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let (upload_id, parts) = create_upload_with_parts(
        &coord,
        bucket,
        "key",
        &[(1, b"complete-multipart-pinned-runtime-map")],
    );
    let candidate_tmp = test_util::tempdir();
    let candidate = make_dynamic_runtime_map_candidate(open_test_storage_cluster(
        candidate_tmp.path(),
        &[0, 1],
    ));
    let hook_handle = handle.clone();
    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), "key".to_string())),
        after_multipart_complete_pre_commit: Some(Arc::new(move || {
            hook_handle.install(Arc::clone(&candidate)).unwrap();
        })),
        ..ReclamationTestHooks::default()
    });

    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
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
        })
        .unwrap();

    handle
        .install(make_dynamic_runtime_map_candidate(initial))
        .unwrap();
    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        result.body.read_all().unwrap(),
        b"complete-multipart-pinned-runtime-map"
    );
}

#[test]
fn abort_multipart_upload_pins_runtime_map_after_auth_lookup() {
    let bucket = "abort-multipart-pinned-bucket";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(bucket, "key", test_requester(), None),
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

    let candidate_tmp = test_util::tempdir();
    let candidate = make_dynamic_runtime_map_candidate(open_test_storage_cluster(
        candidate_tmp.path(),
        &[0, 1],
    ));
    let hook_handle = handle.clone();
    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), "key".to_string())),
        after_abort_multipart_auth_lookup: Some(Arc::new(move || {
            hook_handle.install(Arc::clone(&candidate)).unwrap();
        })),
        ..ReclamationTestHooks::default()
    });

    coord
        .abort_multipart_upload(&multipart_object_request_with_expected_owner(
            bucket,
            "key",
            &upload.upload_id,
            test_requester(),
            None,
        ))
        .unwrap();

    handle
        .install(make_dynamic_runtime_map_candidate(initial))
        .unwrap();
    let err = coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request_with_expected_owner(
                bucket,
                "key",
                &upload.upload_id,
                test_requester(),
                None,
            ),
            part_number_marker: None,
            max_parts: 1000,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "expected pinned abort to remove upload from original map, got {err:?}"
    );
}

#[test]
fn abort_multipart_upload_pins_runtime_map_after_bucket_summary() {
    let bucket = "abort-multipart-bucket-summary-pinned";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(bucket, "key", test_requester(), None),
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

    let candidate_tmp = test_util::tempdir();
    let candidate = make_dynamic_runtime_map_candidate(open_test_storage_cluster(
        candidate_tmp.path(),
        &[0, 1],
    ));
    let hook_handle = handle.clone();
    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), "key".to_string())),
        after_abort_multipart_bucket_summary: Some(Arc::new(move || {
            hook_handle.install(Arc::clone(&candidate)).unwrap();
        })),
        ..ReclamationTestHooks::default()
    });

    coord
        .abort_multipart_upload(&multipart_object_request_with_expected_owner(
            bucket,
            "key",
            &upload.upload_id,
            test_requester(),
            None,
        ))
        .unwrap();

    handle
        .install(make_dynamic_runtime_map_candidate(initial))
        .unwrap();
    let err = coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request_with_expected_owner(
                bucket,
                "key",
                &upload.upload_id,
                test_requester(),
                None,
            ),
            part_number_marker: None,
            max_parts: 1000,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "expected pinned abort to remove upload from original map, got {err:?}"
    );
}

fn install_bucket_command_log_conflict_hook(
    storage_cluster: &Arc<StorageCluster>,
    bucket: &BucketName,
    kind: MetadataCommandApplyTestKind,
) -> storage::MetadataCommandApplyContextTestHookGuard {
    let bucket_pg = storage_cluster.test_bucket_pg_id_for(bucket);
    let primary_node = storage_cluster
        .local_pg_route(PgId::new(bucket_pg))
        .unwrap()
        .primary_node_id();
    let hook_bucket = bucket.clone();
    storage_cluster.test_install_before_metadata_command_apply_context_hook(Arc::new(
        move |context| {
            if context.kind == kind
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.is_none()
                && context.node_id == primary_node
            {
                return Err(storage::StoreError::MetadataCommandLogConflict {
                    node_id: primary_node.as_u32(),
                    pg_id: bucket_pg,
                    cluster_epoch: storage::ClusterEpoch::INITIAL,
                    log_index: 1,
                });
            }
            Ok(())
        },
    ))
}

fn install_object_command_log_conflict_hook(
    storage_cluster: &Arc<StorageCluster>,
    bucket: &BucketName,
    key: &ObjectKey,
    kind: MetadataCommandApplyTestKind,
) -> storage::MetadataCommandApplyContextTestHookGuard {
    let object_pg = storage_cluster.test_object_pg_id_for(bucket, key);
    let primary_node = storage_cluster
        .local_pg_route(PgId::new(object_pg))
        .unwrap()
        .primary_node_id();
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    storage_cluster.test_install_before_metadata_command_apply_context_hook(Arc::new(
        move |context| {
            if context.kind == kind
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && context.node_id == primary_node
            {
                return Err(storage::StoreError::MetadataCommandLogConflict {
                    node_id: primary_node.as_u32(),
                    pg_id: object_pg,
                    cluster_epoch: storage::ClusterEpoch::INITIAL,
                    log_index: 1,
                });
            }
            Ok(())
        },
    ))
}

fn delete_bucket_metadata_or_accept_reclaim_worker_finalize(
    storage_cluster: &StorageCluster,
    bucket: &BucketName,
) {
    match storage_cluster.test_delete_bucket_metadata(bucket) {
        Ok(()) => {}
        Err(storage::BucketWriteDrainError::Metadata(storage::MetadataError::BucketNotFound {
            ..
        })) => {}
        Err(err) => panic!("failed to delete test bucket metadata: {err:?}"),
    }
}

#[test]
fn lock_mutex_unpoisoned_recovers_after_panic() {
    let lock = Mutex::new(vec![1usize]);
    let poison_result = {
        let _panic_guard = SuppressExpectedTestPanic::enter();
        std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = lock.lock().unwrap();
            panic!("poison mutex");
        }))
    };
    assert!(poison_result.is_err());

    lock_mutex_unpoisoned(&lock).push(2);
    assert_eq!(*lock_mutex_unpoisoned(&lock), vec![1, 2]);
}

#[test]
fn rwlock_helpers_recover_after_panic() {
    let lock = RwLock::new(HashMap::from([("bucket".to_string(), 1usize)]));
    let poison_result = {
        let _panic_guard = SuppressExpectedTestPanic::enter();
        std::panic::catch_unwind(AssertUnwindSafe(|| {
            let mut guard = lock.write().unwrap();
            guard.insert("poisoned".to_string(), 2);
            panic!("poison rwlock");
        }))
    };
    assert!(poison_result.is_err());

    write_rwlock_unpoisoned(&lock).insert("ok".to_string(), 3);
    let guard = read_rwlock_unpoisoned(&lock);
    assert_eq!(guard.get("bucket"), Some(&1));
    assert_eq!(guard.get("poisoned"), Some(&2));
    assert_eq!(guard.get("ok"), Some(&3));
}

#[test]
fn object_pg_command_contention_maps_to_operation_aborted() {
    let bucket = trusted_bucket_name("contention-bucket");
    let key = trusted_object_key("contention-key");

    fn assert_maps_to_operation_aborted(error: storage::ObjectPgActionError) {
        assert!(matches!(
            Coordinator::map_object_pg_action_error(error),
            ServerError::OperationAborted
        ));
    }

    fn assert_read_snapshot_maps_to_operation_aborted(
        bucket: &BucketName,
        key: &ObjectKey,
        error: storage::ObjectPgActionError,
    ) {
        assert!(matches!(
            Coordinator::map_object_read_snapshot_error(bucket, key, None, true, error),
            ServerError::OperationAborted
        ));
    }

    let epoch = storage::ClusterEpoch::INITIAL;
    assert_maps_to_operation_aborted(storage::ObjectPgActionError::Store(
        storage::StoreError::MetadataCommandLogConflict {
            node_id: 1,
            pg_id: 2,
            cluster_epoch: epoch,
            log_index: 3,
        },
    ));
    assert_read_snapshot_maps_to_operation_aborted(
        &bucket,
        &key,
        storage::ObjectPgActionError::Store(storage::StoreError::MetadataCommandLogConflict {
            node_id: 1,
            pg_id: 2,
            cluster_epoch: epoch,
            log_index: 3,
        }),
    );
    assert_maps_to_operation_aborted(storage::ObjectPgActionError::Store(
        storage::StoreError::MetadataCommandPendingConflict {
            pg_id: 2,
            cluster_epoch: epoch,
            existing_log_index: 3,
            candidate_log_index: 4,
        },
    ));
    assert_read_snapshot_maps_to_operation_aborted(
        &bucket,
        &key,
        storage::ObjectPgActionError::Store(storage::StoreError::MetadataCommandPendingConflict {
            pg_id: 2,
            cluster_epoch: epoch,
            existing_log_index: 3,
            candidate_log_index: 4,
        }),
    );
    assert_maps_to_operation_aborted(storage::ObjectPgActionError::Store(
        storage::StoreError::MetadataCommandContention {
            context: "pending command displaced during cleanup",
        },
    ));
    assert_read_snapshot_maps_to_operation_aborted(
        &bucket,
        &key,
        storage::ObjectPgActionError::Store(storage::StoreError::MetadataCommandContention {
            context: "pending command displaced during cleanup",
        }),
    );
    assert_maps_to_operation_aborted(storage::ObjectPgActionError::Metadata(
        storage::MetadataError::ObjectGenerationReservationConflict {
            reservation_id: "reservation".to_string(),
            generation_id: 5,
        },
    ));
    assert_read_snapshot_maps_to_operation_aborted(
        &bucket,
        &key,
        storage::ObjectPgActionError::Metadata(
            storage::MetadataError::ObjectGenerationReservationConflict {
                reservation_id: "reservation".to_string(),
                generation_id: 5,
            },
        ),
    );
    assert_maps_to_operation_aborted(storage::ObjectPgActionError::Metadata(
        storage::MetadataError::ObjectVersionReservationConflict {
            version_id: storage::VersionId::from_u64(7),
        },
    ));
    assert_read_snapshot_maps_to_operation_aborted(
        &bucket,
        &key,
        storage::ObjectPgActionError::Metadata(
            storage::MetadataError::ObjectVersionReservationConflict {
                version_id: storage::VersionId::from_u64(7),
            },
        ),
    );
    assert_maps_to_operation_aborted(storage::ObjectPgActionError::Metadata(
        storage::MetadataError::StaleObjectWriteCommand {
            bucket: bucket.clone(),
            key: key.clone(),
            write_sequence: 11,
            generation_id: None,
        },
    ));
    assert_read_snapshot_maps_to_operation_aborted(
        &bucket,
        &key,
        storage::ObjectPgActionError::Metadata(storage::MetadataError::StaleObjectWriteCommand {
            bucket: bucket.clone(),
            key: key.clone(),
            write_sequence: 12,
            generation_id: Some(13),
        }),
    );
}

#[test]
fn stale_bucket_metadata_command_maps_to_operation_aborted() {
    let bucket = trusted_bucket_name("stale-bucket-command");
    let error = storage::BucketSnapshotLoadError::Metadata(
        storage::MetadataError::StaleBucketMetadataCommand {
            name: bucket,
            bucket_execution_generation: 7,
        },
    );
    assert!(matches!(
        Coordinator::map_bucket_snapshot_load_error(error),
        ServerError::OperationAborted
    ));

    let bucket = trusted_bucket_name("stale-bucket-handle-command");
    let error = storage::BucketSnapshotLoadError::Metadata(
        storage::MetadataError::StaleBucketMetadataCommand {
            name: bucket,
            bucket_execution_generation: 8,
        },
    );
    assert!(matches!(
        BucketHandleLoader::map_bucket_snapshot_error(error),
        ServerError::OperationAborted
    ));
}

#[test]
fn bucket_snapshot_object_reservation_conflicts_map_to_operation_aborted() {
    let version_conflict = storage::BucketSnapshotLoadError::Metadata(
        storage::MetadataError::ObjectVersionReservationConflict {
            version_id: storage::VersionId::from_u64(9),
        },
    );
    assert!(matches!(
        Coordinator::map_bucket_snapshot_load_error(version_conflict),
        ServerError::OperationAborted
    ));

    let generation_conflict = storage::BucketSnapshotLoadError::Metadata(
        storage::MetadataError::ObjectGenerationReservationConflict {
            reservation_id: "reservation".to_string(),
            generation_id: 17,
        },
    );
    assert!(matches!(
        BucketHandleLoader::map_bucket_snapshot_error(generation_conflict),
        ServerError::OperationAborted
    ));
}

#[test]
fn bucket_write_drain_contention_maps_to_operation_aborted() {
    let bucket = trusted_bucket_name("bucket-write-drain-contention");
    let epoch = storage::ClusterEpoch::INITIAL;
    assert!(matches!(
        Coordinator::map_bucket_write_drain_error(storage::BucketWriteDrainError::Store(
            storage::StoreError::MetadataCommandLogConflict {
                node_id: 1,
                pg_id: 2,
                cluster_epoch: epoch,
                log_index: 3,
            },
        )),
        ServerError::OperationAborted
    ));
    assert!(matches!(
        Coordinator::map_bucket_write_drain_error(storage::BucketWriteDrainError::Store(
            storage::StoreError::MetadataCommandPendingConflict {
                pg_id: 2,
                cluster_epoch: epoch,
                existing_log_index: 3,
                candidate_log_index: 4,
            },
        )),
        ServerError::OperationAborted
    ));
    assert!(matches!(
        Coordinator::map_bucket_write_drain_error(storage::BucketWriteDrainError::Store(
            storage::StoreError::MetadataCommandContention {
                context: "pending bucket command displaced during cleanup",
            },
        )),
        ServerError::OperationAborted
    ));
    assert!(matches!(
        Coordinator::map_bucket_write_drain_error(storage::BucketWriteDrainError::Metadata(
            storage::MetadataError::StaleBucketMetadataCommand {
                name: bucket,
                bucket_execution_generation: 5,
            },
        )),
        ServerError::OperationAborted
    ));
    assert!(matches!(
        Coordinator::map_bucket_write_drain_error(storage::BucketWriteDrainError::Metadata(
            storage::MetadataError::ObjectVersionReservationConflict {
                version_id: storage::VersionId::from_u64(11),
            },
        )),
        ServerError::OperationAborted
    ));
}

#[test]
fn storage_rpc_resource_exhaustion_maps_to_slow_down() {
    fn resource_exhausted() -> storage::StoreError {
        storage::StoreError::StorageRpcResourceExhausted {
            node_id: 1,
            operation: "test operation",
            message: "active read handles exceed limit".to_string(),
        }
    }

    fn nested_resource_exhausted() -> storage::StoreError {
        storage::StoreError::ShardStore {
            node_id: 1,
            pg_id: 2,
            cluster_epoch: storage::ClusterEpoch::INITIAL,
            source: Box::new(resource_exhausted()),
        }
    }

    fn nested_delete_in_progress() -> storage::StoreError {
        storage::StoreError::ShardStore {
            node_id: 1,
            pg_id: 2,
            cluster_epoch: storage::ClusterEpoch::INITIAL,
            source: Box::new(storage::StoreError::StorageRpcShardDeleteInProgress {
                node_id: 1,
                operation: "shard delete",
                message: "shard delete fenced by read handle".to_string(),
            }),
        }
    }

    fn assert_maps_to_operation_aborted(error: storage::StoreError) {
        assert!(
            matches!(super::map_store_error(error), ServerError::OperationAborted),
            "expected stale/retryable storage error to map to OperationAborted"
        );
    }

    assert!(matches!(
        super::map_store_error(nested_resource_exhausted()),
        ServerError::SlowDown
    ));
    assert!(matches!(
        super::map_store_error(nested_delete_in_progress()),
        ServerError::Store(storage::StoreError::ShardStore { source, .. })
            if matches!(*source, storage::StoreError::StorageRpcShardDeleteInProgress { .. })
    ));
    assert!(matches!(
        super::map_store_error(storage::StoreError::MetadataCommandContention {
            context: "pending command displaced during cleanup",
        }),
        ServerError::OperationAborted
    ));
    assert_maps_to_operation_aborted(storage::StoreError::MetadataCommandLogGap {
        node_id: 1,
        pg_id: 2,
        cluster_epoch: storage::ClusterEpoch::INITIAL,
        log_index: 5,
        expected_log_index: 4,
    });
    assert_maps_to_operation_aborted(storage::StoreError::StalePayloadOperation {
        pg_id: 2,
        operation_epoch: storage::ClusterEpoch::INITIAL,
        current_epoch: storage::ClusterEpoch::new(2).unwrap(),
    });
    assert_maps_to_operation_aborted(storage::StoreError::StaleMetadataPrimaryBridge {
        metadata_node_id: 1,
        operation_epoch: storage::ClusterEpoch::INITIAL,
        current_epoch: storage::ClusterEpoch::new(2).unwrap(),
    });
    assert_maps_to_operation_aborted(storage::StoreError::StaleMetadataOperation {
        pg_id: 2,
        operation_epoch: storage::ClusterEpoch::INITIAL,
        current_epoch: storage::ClusterEpoch::new(2).unwrap(),
    });
    assert_maps_to_operation_aborted(storage::StoreError::StaleMetadataRoute {
        pg_id: 2,
        route_epoch: storage::ClusterEpoch::INITIAL,
        current_epoch: storage::ClusterEpoch::new(2).unwrap(),
    });
    assert_maps_to_operation_aborted(storage::StoreError::RouteMapExpired {
        cluster_epoch: storage::ClusterEpoch::INITIAL,
        valid_until_ms: 1_000,
        now_ms: 1_001,
    });
    assert_maps_to_operation_aborted(storage::StoreError::StaleMetadataCommand {
        node_id: 1,
        pg_id: 2,
        command_epoch: storage::ClusterEpoch::INITIAL,
        current_epoch: storage::ClusterEpoch::new(2).unwrap(),
    });
    assert_maps_to_operation_aborted(storage::StoreError::StaleShardOperation {
        node_id: 1,
        pg_id: 2,
        operation_epoch: storage::ClusterEpoch::INITIAL,
        current_epoch: storage::ClusterEpoch::new(2).unwrap(),
    });
    assert_maps_to_operation_aborted(storage::StoreError::StaleShardLocation {
        node_id: 1,
        pg_id: 2,
        location_epoch: storage::ClusterEpoch::INITIAL,
        current_epoch: storage::ClusterEpoch::new(2).unwrap(),
    });
    assert_maps_to_operation_aborted(storage::StoreError::PgNotActive {
        pg_id: 2,
        cluster_epoch: storage::ClusterEpoch::INITIAL,
        state: PgState::Peering,
    });
    assert_maps_to_operation_aborted(storage::StoreError::ShardPgNotActive {
        node_id: 1,
        pg_id: 2,
        cluster_epoch: storage::ClusterEpoch::INITIAL,
        state: PgState::Peering,
    });
    assert_maps_to_operation_aborted(storage::StoreError::StorageRpc {
        node_id: 1,
        operation: "test operation",
        code: storage::StorageRpcErrorCode::InactivePgRoute,
        message: "PG 2 is Peering".to_string(),
    });
    assert_maps_to_operation_aborted(storage::StoreError::StorageRpc {
        node_id: 1,
        operation: "test operation",
        code: storage::StorageRpcErrorCode::MetadataCommandContention,
        message: "metadata command contention during bucket delete begin".to_string(),
    });
    assert_maps_to_operation_aborted(storage::StoreError::StorageRpc {
        node_id: 1,
        operation: "read storage RPC response",
        code: storage::StorageRpcErrorCode::TransportTimeout,
        message: "storage RPC stream I/O error: timed out".to_string(),
    });
    assert_maps_to_operation_aborted(storage::StoreError::StorageRpc {
        node_id: 1,
        operation: "read storage RPC response",
        code: storage::StorageRpcErrorCode::TransportClosed,
        message: "storage RPC stream I/O error: early eof".to_string(),
    });
    assert_maps_to_operation_aborted(storage::StoreError::ShardStore {
        node_id: 1,
        pg_id: 2,
        cluster_epoch: storage::ClusterEpoch::INITIAL,
        source: Box::new(storage::StoreError::MetadataCommandContention {
            context: "pending command displaced during cleanup",
        }),
    });
    assert_maps_to_operation_aborted(storage::StoreError::ShardStore {
        node_id: 1,
        pg_id: 2,
        cluster_epoch: storage::ClusterEpoch::INITIAL,
        source: Box::new(storage::StoreError::RouteMapExpired {
            cluster_epoch: storage::ClusterEpoch::INITIAL,
            valid_until_ms: 1_000,
            now_ms: 1_001,
        }),
    });
    assert_maps_to_operation_aborted(storage::StoreError::ShardStore {
        node_id: 1,
        pg_id: 2,
        cluster_epoch: storage::ClusterEpoch::INITIAL,
        source: Box::new(storage::StoreError::StorageRpc {
            node_id: 1,
            operation: "shard write",
            code: storage::StorageRpcErrorCode::StaleShardLocation,
            message: "request route epoch is stale".to_string(),
        }),
    });
    assert!(matches!(
        Coordinator::map_object_pg_action_error(storage::ObjectPgActionError::Store(
            resource_exhausted(),
        )),
        ServerError::SlowDown
    ));
    assert!(matches!(
        Coordinator::map_object_pg_action_error(storage::ObjectPgActionError::Store(
            nested_resource_exhausted(),
        )),
        ServerError::SlowDown
    ));
    assert!(matches!(
        Coordinator::map_bucket_snapshot_load_error(storage::BucketSnapshotLoadError::Store(
            resource_exhausted(),
        )),
        ServerError::SlowDown
    ));
    assert!(matches!(
        BucketHandleLoader::map_bucket_snapshot_error(storage::BucketSnapshotLoadError::Store(
            resource_exhausted(),
        )),
        ServerError::SlowDown
    ));
    assert!(matches!(
        Coordinator::map_bucket_write_drain_error(storage::BucketWriteDrainError::Store(
            resource_exhausted(),
        )),
        ServerError::SlowDown
    ));
}

fn injected_stale_shard_location() -> storage::StoreError {
    storage::StoreError::StorageRpc {
        node_id: 0,
        operation: "injected payload shard write",
        code: storage::StorageRpcErrorCode::StaleShardLocation,
        message: "injected stale shard location".to_string(),
    }
}

#[test]
fn direct_put_payload_stale_shard_location_maps_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let _hook =
        storage_cluster.test_install_before_placed_payload_shard_write_hook(Arc::new(|_, _| {
            Err(injected_stale_shard_location())
        }));

    let error = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "direct", test_requester(), None),
            data: b"direct payload",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap_err();
    assert!(matches!(error, ServerError::OperationAborted), "{error:?}");
}

#[test]
fn stream_put_payload_stale_shard_location_maps_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let session_id = begin_stream_put_test(&coord, "bucket", "stream").unwrap();
    let hook =
        storage_cluster.test_install_before_placed_payload_shard_write_hook(Arc::new(|_, _| {
            Err(injected_stale_shard_location())
        }));

    let error = coord
        .append_plaintext_stream_segment_for_test(
            "bucket",
            "stream",
            &session_id,
            0,
            b"stream payload",
        )
        .unwrap_err();
    assert!(matches!(error, ServerError::OperationAborted), "{error:?}");

    drop(hook);
    coord
        .abort_stream_put("bucket", "stream", &session_id)
        .unwrap();
}

#[test]
fn get_object_payload_read_resource_exhaustion_maps_to_slow_down() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"payload-read-overload",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let _hook_guard = storage_cluster.test_install_before_placed_payload_shard_read_hook(Arc::new(
        |location, _shard_key| {
            Err(storage::StoreError::StorageRpcResourceExhausted {
                node_id: location.node_id().as_u32(),
                operation: "read payload shard",
                message: "test injected shard-read overload".to_string(),
            })
        },
    ));

    let err = match coord.get_object(&GetObjectRequest {
        sse_customer: None,
        object: object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        ),
        cond: NO_READ,
    }) {
        Ok(result) => result.body.read_all().unwrap_err(),
        Err(err) => err,
    };

    assert!(
        matches!(err, ServerError::SlowDown),
        "expected payload read overload to map to SlowDown, got {err:?}"
    );
}

#[test]
fn get_object_range_payload_read_resource_exhaustion_maps_to_slow_down() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"payload-range-read-overload",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let _hook_guard = storage_cluster.test_install_before_placed_payload_shard_read_hook(Arc::new(
        |location, _shard_key| {
            Err(storage::StoreError::StorageRpcResourceExhausted {
                node_id: location.node_id().as_u32(),
                operation: "read payload shard",
                message: "test injected range shard-read overload".to_string(),
            })
        },
    ));

    let err = match coord.get_object_range(&GetObjectRangeRequest {
        sse_customer: None,
        object: object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        ),
        range: ByteRange::Range { start: 0, end: 6 },
        cond: NO_READ,
    }) {
        Ok(result) => result.body.read_all().unwrap_err(),
        Err(err) => err,
    };

    assert!(
        matches!(err, ServerError::SlowDown),
        "expected range payload read overload to map to SlowDown, got {err:?}"
    );
}

#[test]
fn get_object_part_payload_read_resource_exhaustion_maps_to_slow_down() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let part = vec![0xAB; MIN_PART];
    let (upload_id, complete_parts) =
        create_upload_with_parts(&coord, "bucket", "key", &[(1, part.as_slice())]);
    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            parts: &complete_parts,
            sse_customer: None,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
        })
        .unwrap();

    let _hook_guard = storage_cluster.test_install_before_placed_payload_shard_read_hook(Arc::new(
        |location, _shard_key| {
            Err(storage::StoreError::StorageRpcResourceExhausted {
                node_id: location.node_id().as_u32(),
                operation: "read payload shard",
                message: "test injected multipart-part shard-read overload".to_string(),
            })
        },
    ));

    let err = match coord.get_object_part(&GetObjectPartRequest {
        sse_customer: None,
        object: object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        ),
        part_number: 1,
        cond: NO_READ,
    }) {
        Ok(result) => result.body.read_all().unwrap_err(),
        Err(err) => err,
    };

    assert!(
        matches!(err, ServerError::SlowDown),
        "expected multipart part payload read overload to map to SlowDown, got {err:?}"
    );
}

#[test]
fn copy_object_source_payload_read_resource_exhaustion_maps_to_slow_down() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"copy-source-overload",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let _hook_guard = storage_cluster.test_install_before_placed_payload_shard_read_hook(Arc::new(
        |location, _shard_key| {
            Err(storage::StoreError::StorageRpcResourceExhausted {
                node_id: location.node_id().as_u32(),
                operation: "read payload shard",
                message: "test injected copy source overload".to_string(),
            })
        },
    ));

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
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap_err();

    assert!(
        matches!(err, ServerError::SlowDown),
        "expected copy source read overload to map to SlowDown, got {err:?}"
    );
}

#[test]
fn upload_part_copy_source_payload_read_resource_exhaustion_maps_to_slow_down() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"upload-part-copy-source-overload",
            metadata: &metadata,
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

    let _hook_guard = storage_cluster.test_install_before_placed_payload_shard_read_hook(Arc::new(
        |location, _shard_key| {
            Err(storage::StoreError::StorageRpcResourceExhausted {
                node_id: location.node_id().as_u32(),
                operation: "read payload shard",
                message: "test injected upload-part-copy source overload".to_string(),
            })
        },
    ));

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
        matches!(err, ServerError::SlowDown),
        "expected upload-part-copy source read overload to map to SlowDown, got {err:?}"
    );
}

#[test]
fn upload_part_copy_range_source_payload_read_resource_exhaustion_maps_to_slow_down() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"upload-part-copy-range-source-overload",
            metadata: &metadata,
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

    let _hook_guard = storage_cluster.test_install_before_placed_payload_shard_read_hook(Arc::new(
        |location, _shard_key| {
            Err(storage::StoreError::StorageRpcResourceExhausted {
                node_id: location.node_id().as_u32(),
                operation: "read payload shard",
                message: "test injected upload-part-copy range source overload".to_string(),
            })
        },
    ));

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
            copy_source_range: Some((2, 12)),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            sse_customer: None,
        })
        .unwrap_err();

    assert!(
        matches!(err, ServerError::SlowDown),
        "expected ranged upload-part-copy source read overload to map to SlowDown, got {err:?}"
    );
}

#[test]
fn phase_10_6_remote_frontend_worker_mode_enables_routed_workers() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);

    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_with_background_worker_mode(
            storage_cluster,
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::remote_frontend_phase_10_6(),
        )
        .unwrap();

    assert_eq!(
        coord.background_worker_mode_for_test(),
        BackgroundWorkerMode {
            object_reclaim_and_bucket_finalize: true,
            lifecycle: true,
            shard_scavenger: true,
            shard_repair: true,
            shard_backfill: true,
            stream_session: true,
        }
    );
}

#[test]
fn frontend_coordinators_share_one_reclaim_sweeper_per_storage_handle() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let storage_handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&storage_cluster));
    let first = setup_coordinator_with_only_reclaim_worker(
        storage_handle.clone(),
        Arc::clone(&storage_cluster),
    );
    let second =
        setup_coordinator_with_only_reclaim_worker(storage_handle, Arc::clone(&storage_cluster));

    assert!(
        Arc::ptr_eq(&first._reclaim_sweeper, &second._reclaim_sweeper),
        "coordinators over one process-local storage handle must not multiply durable scans"
    );
    drop(first);
    assert!(
        !second._reclaim_sweeper.stop.load(Ordering::SeqCst),
        "dropping one coordinator must leave the shared reclaim worker serving its peer"
    );
}

#[test]
fn reclaim_worker_rediscovers_capacity_deferred_root_while_idle() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("reclaim-worker-idle-return");

    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let storage_handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&storage_cluster));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            storage_handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    let bucket = "bucket";
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let metadata_pg_id = storage_cluster.test_pg_ids()[0];
    let keys = ["first-", "second-", "capacity-deferred-"].map(|prefix| {
        find_key_for_object_metadata_pg_with_prefix(
            &storage_cluster,
            bucket,
            metadata_pg_id,
            prefix,
        )
    });
    let bucket_name = trusted_bucket_name(bucket);
    let mut reclaim_roots = Vec::new();
    for key in &keys {
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(bucket, key, test_requester(), None),
                data: key.as_bytes(),
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        let object_key = trusted_object_key(key);
        let generation_id = storage_cluster
            .test_get_object_meta(&bucket_name, &object_key)
            .unwrap()
            .into_live()
            .expect("put object should create a live object")
            .generation_id;
        coord
            .delete_object(&delete_object_request(
                bucket,
                key,
                None,
                test_requester(),
                false,
                NO_DELETE,
            ))
            .unwrap();
        reclaim_roots.push((object_key, generation_id));
    }
    assert!(
        reclaim_roots.iter().all(|(key, generation_id)| {
            storage_cluster
                .test_payload_reclaim_exists(&bucket_name, key, *generation_id)
                .unwrap()
        }),
        "all durable roots must exist before the reclaim worker starts"
    );

    let gate = DeterministicFaultGate::new(TOKEN);
    let gate_for_hook = Arc::clone(&gate);
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target_reclaim_worker_registry_key: Some(storage_cluster.process_local_registry_key()),
        reclaim_worker_durable_scan_delay_override: Some(Duration::from_secs(3_600)),
        after_reclaim_worker_idle_return: Some(Arc::new(move || {
            gate_for_hook.wait_at(TOKEN);
        })),
        ..ReclamationTestHooks::default()
    });
    let _worker =
        setup_coordinator_with_only_reclaim_worker(storage_handle, Arc::clone(&storage_cluster));
    let _gate_release_guard = gate.release_on_drop();
    gate.wait_until_arrived(Duration::from_secs(10));
    let root_presence = reclaim_roots
        .iter()
        .map(|(key, generation_id)| {
            storage_cluster.test_payload_reclaim_exists(&bucket_name, key, *generation_id)
        })
        .collect::<Vec<_>>();
    gate.release();
    let root_presence = root_presence
        .into_iter()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    assert_eq!(
        root_presence,
        [false, false, true],
        "the worker must return from an empty queue poll after the queued roots drain and before the capacity-deferred root is rediscovered"
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = reclaim_roots
            .iter()
            .filter(|(key, generation_id)| {
                storage_cluster
                    .test_payload_reclaim_exists(&bucket_name, key, *generation_id)
                    .unwrap()
            })
            .count();
        if remaining == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "idle reclaim worker left {remaining} durable roots stranded"
        );
        thread::sleep(Duration::from_millis(10));
    }
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
fn write_encryption_request_rejects_conflicting_sse_c_and_sse_s3() {
    let sse_customer = test_sse_customer_request();
    let err = WriteEncryptionRequest::from_request_parts(
        Some(&sse_customer),
        Some(ManagedEncryptionAlgorithm::Aes256),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ServerError::InvalidArgument { reason }
            if reason == "x-amz-server-side-encryption may not be used with SSE-C headers"
    ));
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
        .storage_node()
        .test_get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
        .unwrap();
    let live = live.into_live().expect("expected live object");
    assert_eq!(live.owner.principal, owner.principal());
    assert_eq!(live.owner.canonical_id, owner_canonical_id);
}

#[test]
fn direct_put_request_maps_command_log_conflict_to_operation_aborted() {
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
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_guard = storage_cluster.test_install_before_metadata_command_apply_context_hook(
        Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::CommitDirectPutObject
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && context.node_id == primary_node
            {
                return Err(storage::StoreError::MetadataCommandLogConflict {
                    node_id: primary_node.as_u32(),
                    pg_id: object_pg,
                    cluster_epoch: storage::ClusterEpoch::INITIAL,
                    log_index: 1,
                });
            }
            Ok(())
        }),
    );

    let metadata = MetadataBlob::new();
    let err = test_helpers::put_object(
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
        matches!(err, ServerError::OperationAborted),
        "expected direct PUT command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn direct_put_generation_reservation_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::ReserveObjectGeneration,
    );

    let metadata = MetadataBlob::new();
    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"generation-reservation-conflict",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected direct PUT generation reservation conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn direct_put_version_reservation_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    coord
        .put_bucket_versioning(&PutBucketVersioningRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            state: BucketVersioningState::Enabled,
        })
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::ReserveObjectVersion,
    );

    let metadata = MetadataBlob::new();
    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"version-reservation-conflict",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected direct PUT version reservation conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn create_bucket_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    let bucket = trusted_bucket_name("bucket");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_bucket_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        MetadataCommandApplyTestKind::CreateBucket,
    );

    let err = coord
        .create_bucket(&CreateBucketRequest {
            name: bucket,
            requester: test_requester(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected CreateBucket command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn stream_put_begin_request_maps_command_log_conflict_to_operation_aborted() {
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
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_guard = storage_cluster.test_install_before_metadata_command_apply_context_hook(
        Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::CreateStreamUpload
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && context.node_id == primary_node
            {
                return Err(storage::StoreError::MetadataCommandLogConflict {
                    node_id: primary_node.as_u32(),
                    pg_id: object_pg,
                    cluster_epoch: storage::ClusterEpoch::INITIAL,
                    log_index: 1,
                });
            }
            Ok(())
        }),
    );

    let err = begin_stream_put_test(&coord, "bucket", "key").unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected stream PUT begin command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn stream_put_begin_generation_reservation_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::ReserveObjectGeneration,
    );

    let err = begin_stream_put_test(&coord, "bucket", "key").unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected stream PUT begin generation reservation conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn stream_put_finalize_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
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
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_guard = storage_cluster.test_install_before_metadata_command_apply_context_hook(
        Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::CommitDirectPutObject
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && context.node_id == primary_node
            {
                return Err(storage::StoreError::MetadataCommandLogConflict {
                    node_id: primary_node.as_u32(),
                    pg_id: object_pg,
                    cluster_epoch: storage::ClusterEpoch::INITIAL,
                    log_index: 1,
                });
            }
            Ok(())
        }),
    );

    let metadata = MetadataBlob::new();
    let write_encryption = coord
        .load_stream_put_write_encryption(&bucket, &key, &session_id, None)
        .unwrap();
    let err = coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id,
            crc64: checksum::crc64::checksum(b""),
            total_size: 0,
            metadata_blob: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: write_encryption.as_ref(),
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected stream PUT finalize command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn stream_put_finalize_version_reservation_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
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

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::ReserveObjectVersion,
    );

    let metadata = MetadataBlob::new();
    let write_encryption = coord
        .load_stream_put_write_encryption(&bucket, &key, &session_id, None)
        .unwrap();
    let err = coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id,
            crc64: checksum::crc64::checksum(b""),
            total_size: 0,
            metadata_blob: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: write_encryption.as_ref(),
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected stream PUT finalize version reservation conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn stream_put_finalize_stale_snapshot_budget_returns_operation_aborted_and_remains_cleanupable() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let competing_coord =
        setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let stream_body = b"stream body that must not be published";
    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, stream_body)
        .unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let staged_segments = storage_cluster
        .test_list_stream_segments(&bucket, &key, &session_id)
        .unwrap();
    assert_eq!(staged_segments.len(), 1);

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let hook_guard = storage_cluster.test_install_before_stream_put_finalize_command_id_hook(
        Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            test_helpers::put_object(
                &competing_coord,
                &PutObjectRequest {
                    object: object_request("bucket", "key", test_requester()),
                    data: b"competing object",
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,
                    acl: NO_PUT_OBJECT_ACL.into(),
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                },
            )
            .unwrap();
            thread::sleep(Duration::from_millis(1_100));
        }),
    );

    let metadata = MetadataBlob::new();
    let err = coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id,
            crc64: checksum::crc64::checksum(stream_body),
            total_size: stream_body.len() as u64,
            metadata_blob: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: ActiveWriteEncryptionRef::None,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(hook_ran.load(Ordering::SeqCst));
    assert!(
        matches!(err, ServerError::OperationAborted),
        "stale stream finalization exhaustion must be retryable, got {err:?}"
    );
    drop(hook_guard);

    let current = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request("bucket", "key", None, test_requester()),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(current.body.read_all().unwrap(), b"competing object");
    assert_eq!(
        storage_cluster
            .test_list_stream_segments(&bucket, &key, &session_id)
            .unwrap(),
        staged_segments,
        "retry exhaustion must preserve the session for caller-owned cleanup"
    );

    coord
        .abort_stream_put_session(&bucket, &key, &session_id)
        .unwrap();
    assert!(
        storage_cluster
            .test_list_stream_segments(&bucket, &key, &session_id)
            .unwrap()
            .is_empty(),
        "caller cleanup must remove the staged stream segments"
    );
    assert!(
        storage_cluster
            .list_stream_upload_sessions_best_effort()
            .iter()
            .all(|session| session.session_id != session_id),
        "caller cleanup must remove the stale stream session"
    );
}

#[test]
fn copy_object_destination_create_stream_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"copy-source",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("dst");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::CreateStreamUpload,
    );

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
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected CopyObject destination stream create conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
    let leaked_sessions = storage_cluster.list_stream_upload_sessions_best_effort();
    assert!(
        leaked_sessions.is_empty(),
        "failed CopyObject destination stream create must not leave stream uploads: {leaked_sessions:?}"
    );
}

#[test]
fn copy_object_destination_finalize_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"copy-source",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("dst");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::CommitDirectPutObject,
    );

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
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected CopyObject destination finalize conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn copy_object_stale_destination_budget_returns_operation_aborted_and_cleans_stream() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let competing_coord =
        setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"copy source that must not be published",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let hook_guard = storage_cluster.test_install_before_stream_put_finalize_command_id_hook(
        Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            test_helpers::put_object(
                &competing_coord,
                &PutObjectRequest {
                    object: object_request("bucket", "dst", test_requester()),
                    data: b"competing destination",
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,
                    acl: NO_PUT_OBJECT_ACL.into(),
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                },
            )
            .unwrap();
            thread::sleep(Duration::from_millis(1_100));
        }),
    );

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
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(hook_ran.load(Ordering::SeqCst));
    assert!(
        matches!(err, ServerError::OperationAborted),
        "stale CopyObject destination exhaustion must be retryable, got {err:?}"
    );
    drop(hook_guard);

    let destination = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request("bucket", "dst", None, test_requester()),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        destination.body.read_all().unwrap(),
        b"competing destination"
    );
    let source = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request("bucket", "src", None, test_requester()),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        source.body.read_all().unwrap(),
        b"copy source that must not be published"
    );
    let leaked_sessions = storage_cluster.list_stream_upload_sessions_best_effort();
    assert!(
        leaked_sessions.is_empty(),
        "failed CopyObject must clean its destination stream: {leaked_sessions:?}"
    );
}

#[test]
fn copy_object_failure_retries_destination_stream_abort_cleanup() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"copy-source",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("dst");
    let object_pg = storage_cluster.test_object_pg_id_for(&bucket, &key);
    let primary_node = storage_cluster
        .local_pg_route(PgId::new(object_pg))
        .unwrap()
        .primary_node_id();
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let append_failures = Arc::new(AtomicUsize::new(1));
    let abort_failures = Arc::new(AtomicUsize::new(2));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_append_failures = Arc::clone(&append_failures);
    let hook_abort_failures = Arc::clone(&abort_failures);
    let hook_guard = storage_cluster.test_install_before_metadata_command_apply_context_hook(
        Arc::new(move |context| {
            if context.bucket.as_ref() != Some(&hook_bucket)
                || context.key.as_ref() != Some(&hook_key)
                || context.node_id != primary_node
            {
                return Ok(());
            }
            if context.kind == MetadataCommandApplyTestKind::AppendStreamSegment
                && hook_append_failures
                    .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
            {
                return Err(storage::StoreError::MetadataCommandLogConflict {
                    node_id: primary_node.as_u32(),
                    pg_id: object_pg,
                    cluster_epoch: storage::ClusterEpoch::INITIAL,
                    log_index: 1,
                });
            }
            if context.kind == MetadataCommandApplyTestKind::AbortStreamUpload
                && hook_abort_failures
                    .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
            {
                return Err(storage::StoreError::MetadataCommandLogConflict {
                    node_id: primary_node.as_u32(),
                    pg_id: object_pg,
                    cluster_epoch: storage::ClusterEpoch::INITIAL,
                    log_index: 1,
                });
            }
            Ok(())
        }),
    );

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
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected CopyObject append conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
    assert_eq!(
        append_failures.load(Ordering::SeqCst),
        0,
        "test must inject one CopyObject append conflict"
    );
    assert_eq!(
        abort_failures.load(Ordering::SeqCst),
        0,
        "CopyObject cleanup should retry transient abort conflicts"
    );
    let leaked_sessions = storage_cluster.list_stream_upload_sessions_best_effort();
    assert!(
        leaked_sessions.is_empty(),
        "failed CopyObject must not leave stream uploads after retrying abort cleanup: {leaked_sessions:?}"
    );
}

#[test]
fn stream_append_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::AppendStreamSegment,
    );

    let err = coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"chunk")
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected stream append command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn stream_abort_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::AbortStreamUpload,
    );

    let err = coord
        .abort_stream_put("bucket", "key", &session_id)
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected stream abort command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn put_bucket_versioning_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_bucket_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        MetadataCommandApplyTestKind::PutBucketVersioning,
    );

    let err = put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected PutBucketVersioning command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn put_bucket_acl_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_bucket_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        MetadataCommandApplyTestKind::PutBucketAcl,
    );

    let err =
        put_bucket_canned_acl_test(&coord, "bucket", BucketAcl::Private, test_requester(), None)
            .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected PutBucketAcl command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn put_bucket_property_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_bucket_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        MetadataCommandApplyTestKind::PutBucketProperty,
    );

    let err = put_bucket_public_access_block_test(
        &coord,
        "bucket",
        "<PublicAccessBlockConfiguration><BlockPublicPolicy>true</BlockPublicPolicy></PublicAccessBlockConfiguration>",
        test_requester(),
        None,
    )
    .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected PutBucketProperty command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn put_bucket_subresource_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_bucket_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        MetadataCommandApplyTestKind::PutBucketSubresource,
    );

    let cors = "<CORSConfiguration><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>";
    let err = coord
        .put_bucket_cors(&put_bucket_config_request_with_expected_owner(
            "bucket",
            cors,
            test_requester(),
            None,
        ))
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected PutBucketSubresource command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn put_object_metadata_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"tag-me",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::PutObjectMetadata,
    );

    let err = put_object_tags_test(
        &coord,
        "bucket",
        "key",
        None,
        "foo=bar",
        test_requester(),
        None,
    )
    .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected PutObjectMetadata command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn delete_object_version_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"delete-me",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
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
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_guard = storage_cluster.test_install_before_metadata_command_apply_context_hook(
        Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::DeleteObjectVersion
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && context.node_id == primary_node
            {
                return Err(storage::StoreError::MetadataCommandLogConflict {
                    node_id: primary_node.as_u32(),
                    pg_id: object_pg,
                    cluster_epoch: storage::ClusterEpoch::INITIAL,
                    log_index: 1,
                });
            }
            Ok(())
        }),
    );

    let err = coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected delete object command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn delete_objects_entry_maps_command_log_conflict_to_operation_aborted_error() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"delete-me",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::DeleteObjectVersion,
    );

    let entries = [DeleteEntry {
        key,
        version_id: None,
        cond: DeleteCondition::None,
    }];
    let result = coord
        .delete_objects(&DeleteObjectsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            entries: &entries,
            bypass_governance: false,
        })
        .unwrap();
    assert!(
        result.deleted.is_empty(),
        "failed delete entry must not be reported as deleted: {result:?}"
    );
    assert_eq!(result.errors.len(), 1);
    assert_eq!(result.errors[0].key, "key");
    assert_eq!(result.errors[0].code, "OperationAborted");
    drop(hook_guard);
}

#[test]
fn delete_object_marker_insert_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
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
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_guard = storage_cluster.test_install_before_metadata_command_apply_context_hook(
        Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::InsertDeleteMarker
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && context.node_id == primary_node
            {
                return Err(storage::StoreError::MetadataCommandLogConflict {
                    node_id: primary_node.as_u32(),
                    pg_id: object_pg,
                    cluster_epoch: storage::ClusterEpoch::INITIAL,
                    log_index: 1,
                });
            }
            Ok(())
        }),
    );

    let err = coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected delete marker command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn delete_object_marker_version_reservation_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
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

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::ReserveObjectVersion,
    );

    let err = coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected delete marker version reservation conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn conditional_delete_maps_metadata_command_budget_exhaustion_to_request_conflict() {
    let cond = DeleteCondition::IfMatch("\"etag\"".into());
    let error = Coordinator::map_delete_object_pg_action_error(
        storage::ObjectPgActionError::Store(storage::StoreError::MetadataCommandContention {
            context: "object version reservation retry budget exhausted",
        }),
        &cond,
        "key",
    );
    assert!(matches!(
        error,
        ServerError::ConditionalRequestConflict { key, condition }
            if key == "key" && condition == "If-Match"
    ));

    let error = Coordinator::map_delete_object_pg_action_error(
        storage::ObjectPgActionError::Store(storage::StoreError::MetadataCommandContention {
            context: "object version reservation retry budget exhausted",
        }),
        &DeleteCondition::None,
        "key",
    );
    assert!(matches!(error, ServerError::OperationAborted));

    let error = Coordinator::map_delete_object_pg_action_error(
        storage::ObjectPgActionError::Store(storage::StoreError::RouteMapExpired {
            cluster_epoch: storage::ClusterEpoch::INITIAL,
            valid_until_ms: 1,
            now_ms: 2,
        }),
        &cond,
        "key",
    );
    assert!(matches!(error, ServerError::OperationAborted));
}

#[test]
fn conditional_delete_marker_version_reservation_conflict_is_request_conflict() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
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
    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"original",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::ReserveObjectVersion,
    );
    let cond = DeleteCondition::IfMatch(put.etag.into());
    let error = coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            test_requester(),
            false,
            &cond,
        ))
        .unwrap_err();
    assert!(matches!(
        error,
        ServerError::ConditionalRequestConflict { ref key, condition }
            if key == "key" && condition == "If-Match"
    ));
    drop(hook_guard);

    let versions = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: Some("key"),
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 100,
            requested_max_keys: Some(100),
        })
        .unwrap();
    assert_eq!(versions.versions.len(), 1);
    assert!(!versions.versions[0].is_delete_marker);
}

#[test]
fn create_multipart_upload_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::CreateMultipartUpload,
    );

    let metadata = MetadataBlob::new();
    let err = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request("bucket", "key", test_requester()),
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
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected CreateMultipartUpload command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn complete_multipart_upload_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"part1")]);
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::CommitMultipartObject,
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
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected CompleteMultipartUpload command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn complete_multipart_upload_version_reservation_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
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

    let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"part1")]);
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::ReserveObjectVersion,
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
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected CompleteMultipartUpload version reservation conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn upload_part_append_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
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
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    let session = begin_stream_part_test(&coord, "bucket", "key", &create.upload_id, 1).unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::AppendStreamSegment,
    );

    let err = coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session.session_id, 0, b"part")
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected UploadPart append command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn upload_part_finalize_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
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
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    let session = begin_stream_part_test(&coord, "bucket", "key", &create.upload_id, 1).unwrap();
    let data = b"part";
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session.session_id, 0, data)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::CommitStreamPart,
    );

    let err = coord
        .finalize_stream_part(FinalizeStreamPartRequest {
            upload: multipart_object_request("bucket", "key", &create.upload_id, test_requester()),
            session_id: &session.session_id,
            part_number: 1,
            crc64: checksum::crc64::checksum(data),
            total_size: data.len() as u64,
            claimed_checksum: None,
            computed_checksum: None,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected UploadPart finalize command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn upload_part_copy_destination_finalize_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"upload-part-copy-source",
            metadata: &metadata,
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

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("dst");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::CommitStreamPart,
    );

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
        matches!(err, ServerError::OperationAborted),
        "expected UploadPartCopy destination commit conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn abort_multipart_upload_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, _parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"part1")]);
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::AbortMultipartUpload,
    );

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
        matches!(err, ServerError::OperationAborted),
        "expected AbortMultipartUpload command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn delete_bucket_begin_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_bucket_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        MetadataCommandApplyTestKind::MarkBucketDeleting,
    );

    let err = delete_bucket_test(&coord, "bucket").unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected DeleteBucket begin command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn bucket_delete_finalizer_expired_route_map_maps_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
        Arc::clone(&storage_cluster),
    );
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"part1")]);
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
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap();
    delete_bucket_test(&coord, "bucket").unwrap();

    let bucket = trusted_bucket_name("bucket");
    let expired_cluster = same_store_cluster_with_route_map_validity(
        &storage_cluster,
        tmp.path(),
        RouteMapValidity::until_ms(1).unwrap(),
    );
    let expired_coord =
        setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
            expired_cluster,
        );
    let err = expired_coord
        .read_runtime()
        .try_finalize_bucket_delete_for(&bucket)
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected bucket delete finalizer route-map expiry to map to OperationAborted, got {err:?}"
    );
}

#[test]
fn bucket_delete_finalizer_route_map_expiry_after_claim_releases_claim() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-finalizer-mid-expired-route-map";
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
        Arc::clone(&storage_cluster),
    );
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    delete_bucket_test(&coord, bucket).unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    let valid_until = storage::clock::wall_time_millis().saturating_add(250);
    let expiring_cluster = same_store_cluster_with_route_map_validity(
        &storage_cluster,
        tmp.path(),
        RouteMapValidity::until_ms(valid_until).unwrap(),
    );
    let expiring_coord =
        setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
            expiring_cluster,
        );

    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_bucket_scoped_test_hooks(BucketScopedTestHooks {
        target: Some(bucket_name.clone()),
        after_bucket_delete_finalize_claim: Some(Arc::new(move || {
            while storage::clock::wall_time_millis() <= valid_until {
                thread::sleep(Duration::from_millis(5));
            }
        })),
        ..BucketScopedTestHooks::default()
    });

    let err = expiring_coord
        .read_runtime()
        .try_finalize_bucket_delete_for(&bucket_name)
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected bucket delete finalizer mid-claim route-map expiry to map to OperationAborted, got {err:?}"
    );
    drop(_hook_guard);

    assert_eq!(
        storage_cluster
            .try_finalize_bucket_delete(&bucket_name)
            .unwrap(),
        storage::BucketDeleteFinalizeOutcome::Finalized,
        "mid-claim route-map expiry must release the finalizer claim for a fresh retry"
    );
}

#[test]
fn bucket_delete_finalizer_stale_metadata_route_maps_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
        Arc::clone(&storage_cluster),
    );
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    delete_bucket_test(&coord, "bucket").unwrap();

    let bucket = trusted_bucket_name("bucket");
    let stale_cluster =
        same_epoch_cluster_with_stale_current_pg_routes(&storage_cluster, tmp.path());
    let storage_err = stale_cluster
        .try_finalize_bucket_delete(&bucket)
        .unwrap_err();
    assert!(
        matches!(
            storage_err,
            storage::BucketWriteDrainError::Store(storage::StoreError::StaleMetadataRoute { .. })
        ),
        "fixture should exercise StaleMetadataRoute, got {storage_err:?}"
    );

    let stale_coord =
        setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
            stale_cluster,
        );
    let err = stale_coord
        .read_runtime()
        .try_finalize_bucket_delete_for(&bucket)
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected bucket delete finalizer stale metadata route to map to OperationAborted, got {err:?}"
    );
}

#[test]
fn lifecycle_current_expiry_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_same_process_coordinator_with_storage_cluster_without_lifecycle_sweeper(
        Arc::clone(&storage_cluster),
    );
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_lifecycle_test(
        &coord,
        "bucket",
        "<LifecycleConfiguration><Rule><ID>expire</ID><Filter><Prefix/></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>",
        test_requester(),
        None,
    )
    .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"expire-me",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let version_id = coord
        .storage_node()
        .test_get_object_meta(&bucket, &key)
        .unwrap()
        .as_live()
        .unwrap()
        .version_id;
    let bucket_incarnation_generation = coord
        .storage_node()
        .head_bucket_info(&bucket)
        .unwrap()
        .bucket_incarnation_generation;
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::DeleteObjectVersion,
    );

    let err = coord
        .read_runtime()
        .expire_current_object_if_due(
            &bucket,
            &key,
            version_id,
            bucket_incarnation_generation,
            u64::MAX,
        )
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected lifecycle current expiry command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn lifecycle_abort_multipart_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_same_process_coordinator_with_storage_cluster_without_lifecycle_sweeper(
        Arc::clone(&storage_cluster),
    );
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_lifecycle_test(
        &coord,
        "bucket",
        "<LifecycleConfiguration><Rule><ID>abort-mpu</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>",
        test_requester(),
        None,
    )
    .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request("bucket", "logs/app", test_requester()),
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

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("logs/app");
    let bucket_incarnation_generation = coord
        .storage_node()
        .head_bucket_info(&bucket)
        .unwrap()
        .bucket_incarnation_generation;
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::AbortMultipartUpload,
    );

    let err = coord
        .read_runtime()
        .abort_multipart_upload_if_due(
            &bucket,
            &key,
            &create.upload_id,
            bucket_incarnation_generation,
            u64::MAX,
        )
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected lifecycle multipart abort command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
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
        .storage_node()
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
        .storage_node()
        .test_get_multipart_upload(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &upload.upload_id,
        )
        .unwrap();
    assert_eq!(upload_record.initiator, expected_owner.clone());
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
        .storage_node()
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
        .storage_node()
        .test_get_multipart_upload(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &upload.upload_id,
        )
        .unwrap();
    assert_eq!(
        upload_record.initiator,
        OwnerIdentity::new(
            writer.principal().to_string(),
            writer.canonical_user_id().clone(),
        )
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

    let err = coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::BucketAlreadyOwnedByYou));

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
    let coord = setup_same_process_coordinator_with_storage_cluster(storage_cluster);
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
    let coord = setup_same_process_coordinator_with_storage_cluster(storage_cluster);
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
    let coord = setup_same_process_coordinator_with_storage_cluster(storage_cluster);
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
    let coord = setup_same_process_coordinator_with_storage_cluster(storage_cluster);
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
fn list_object_versions_continuation_survives_epoch_change_between_pages() {
    let bucket = "version-continuation-epoch-change-bucket";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    let metadata = MetadataBlob::new();
    let system_metadata = SystemMetadata::EMPTY;

    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        bucket,
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();

    let pg_ids = initial.test_pg_ids();
    assert!(
        pg_ids.len() >= 2,
        "test requires at least two object metadata PGs"
    );
    let key_a = find_key_for_object_metadata_pg_with_prefix(&initial, bucket, pg_ids[0], "a/");
    let key_b = find_key_for_object_metadata_pg_with_prefix(&initial, bucket, pg_ids[1], "b/");

    let older_a = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, &key_a, test_requester(), None),
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
            object: object_request_with_expected_owner(bucket, &key_a, test_requester(), None),
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
            object: object_request_with_expected_owner(bucket, &key_b, test_requester(), None),
            data: b"value-b",
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
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
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
    assert!(first_page.is_truncated);
    assert_eq!(first_page.next_key_marker.as_deref(), Some(key_a.as_str()));
    assert_eq!(first_page.next_version_id_marker, Some(older_a.version_id));

    install_same_store_next_epoch_runtime_map(&handle, &initial, tmp.path());

    let second_page = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: first_page.next_key_marker.as_deref(),
            version_id_marker: first_page.next_version_id_marker,
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    assert_eq!(second_page.versions.len(), 1);
    assert_eq!(second_page.versions[0].key, key_b);
    assert_eq!(second_page.versions[0].version_id, VersionId::from_u64(1));
    assert!(second_page.versions[0].is_latest);
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
fn list_object_versions_delimiter_continuation_survives_epoch_change_between_pages() {
    let bucket = "version-delimiter-continuation-epoch-change-bucket";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    let metadata = MetadataBlob::new();
    let system_metadata = SystemMetadata::EMPTY;

    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        bucket,
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
                object: object_request_with_expected_owner(bucket, key, test_requester(), None),
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
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
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

    install_same_store_next_epoch_runtime_map(&handle, &initial, tmp.path());

    let second_page = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
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
    let coord = setup_same_process_coordinator_with_storage_cluster(storage_cluster);
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
            delimiter: None,
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

    let tags = get_object_tags_test(&coord, "bucket", "key", None, test_requester(), None).unwrap();
    assert_eq!(tags.as_deref(), Some(tags_xml));
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
fn delete_bucket_authorizes_idempotent_retry_while_deleting() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-delete-idempotent-auth";
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
        Arc::clone(&storage_cluster),
    );
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    storage_cluster
        .test_begin_bucket_delete_if_current(&bucket_name)
        .unwrap();

    delete_bucket_test(&coord, bucket)
        .expect("idempotent DeleteBucket retry should authorize while bucket delete drain exists");
}

#[test]
fn delete_bucket_authorization_adopts_active_preserved_attempt_without_drain_wait() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-delete-active-attempt-auth";
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0, 1, 2]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let bucket_name = trusted_bucket_name(bucket);
    storage_cluster
        .test_seed_bucket_delete_attempt_outcome(
            &bucket_name,
            storage::BucketDeleteAttemptOutcomeKind::Retryable,
            storage::BucketDeleteAttemptPhase::ReservationWait,
            "seeded reservation-wait attempt for auth adoption".to_string(),
            None,
        )
        .unwrap();

    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_bucket_scoped_test_hooks(BucketScopedTestHooks {
        target: Some(bucket_name.clone()),
        before_bucket_write_drain_wait: Some(Arc::new(|| {
            panic!("DeleteBucket authorization should not wait behind its own preserved attempt");
        })),
        ..BucketScopedTestHooks::default()
    });

    delete_bucket_test(&coord, bucket)
        .expect("DeleteBucket should authorize and adopt the preserved active attempt");

    match storage_cluster.test_head_bucket_raw(&bucket_name) {
        Ok(info) => assert_eq!(info.state, storage::BucketState::Deleting),
        Err(storage::BucketSnapshotLoadError::Metadata(
            storage::MetadataError::BucketNotFound { .. },
        )) => {}
        Err(err) => panic!("unexpected bucket state after adopted delete: {err:?}"),
    }
}

#[test]
fn delete_bucket_expired_route_map_maps_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-delete-expired-route-map";
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let expired_cluster = same_store_cluster_with_route_map_validity(
        &storage_cluster,
        tmp.path(),
        RouteMapValidity::until_ms(1).unwrap(),
    );
    let expired_coord = setup_direct_coordinator_with_storage_cluster(expired_cluster);
    let err = delete_bucket_test(&expired_coord, bucket).unwrap_err();
    assert!(matches!(err, ServerError::OperationAborted), "{err:?}");
}

#[test]
fn delete_bucket_stale_metadata_route_maps_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-delete-stale-route";
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    let stale_cluster =
        same_epoch_cluster_with_stale_current_pg_routes(&storage_cluster, tmp.path());
    let storage_err = stale_cluster
        .test_begin_bucket_delete_if_current(&bucket_name)
        .unwrap_err();
    assert!(
        matches!(
            storage_err,
            storage::BucketWriteDrainError::Store(storage::StoreError::StaleMetadataRoute { .. })
        ),
        "fixture should exercise StaleMetadataRoute, got {storage_err:?}"
    );

    let stale_coord = setup_direct_coordinator_with_storage_cluster(stale_cluster);
    let err = delete_bucket_test(&stale_coord, bucket).unwrap_err();
    assert!(matches!(err, ServerError::OperationAborted), "{err:?}");
}

#[test]
fn delete_bucket_route_map_expiry_after_drain_maps_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-delete-mid-expired-route-map";
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
        Arc::clone(&storage_cluster),
    );
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let valid_until = storage::clock::wall_time_millis().saturating_add(250);
    let expiring_cluster = same_store_cluster_with_route_map_validity(
        &storage_cluster,
        tmp.path(),
        RouteMapValidity::until_ms(valid_until).unwrap(),
    );
    let expiring_coord =
        setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
            expiring_cluster,
        );

    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_bucket_scoped_test_hooks(BucketScopedTestHooks {
        target: Some(trusted_bucket_name(bucket)),
        after_begin_bucket_delete_drain: Some(Arc::new(move || {
            while storage::clock::wall_time_millis() <= valid_until {
                thread::sleep(Duration::from_millis(5));
            }
        })),
        ..BucketScopedTestHooks::default()
    });

    let err = delete_bucket_test(&expiring_coord, bucket).unwrap_err();
    assert!(matches!(err, ServerError::OperationAborted), "{err:?}");

    let info = storage_cluster
        .head_bucket_info(&trusted_bucket_name(bucket))
        .unwrap();
    assert_eq!(info.state, storage::BucketState::Active);
}

#[test]
fn delete_bucket_stale_raw_authorization_does_not_delete_recreated_bucket() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-delete-stale-auth-recreate";
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    // This test must retain the deleting incarnation until it has captured the
    // idempotent-retry authorization. A background finalizer can otherwise
    // delete it between the explicit begin call and authorization.
    let coord = setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
        Arc::clone(&storage_cluster),
    );
    coord
        .create_bucket_for_owner("attacker-owner", bucket, false)
        .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    storage_cluster
        .test_begin_bucket_delete_if_current(&bucket_name)
        .unwrap();
    let stale_authorized = coord
        .authorize_delete_bucket(&bucket_request_with_expected_owner(
            bucket,
            test_helpers::requester("attacker-owner"),
            None,
        ))
        .expect("idempotent retry should authorize against the deleting bucket incarnation");

    delete_bucket_metadata_or_accept_reclaim_worker_finalize(&storage_cluster, &bucket_name);
    coord
        .create_bucket_for_owner("victim-owner", bucket, false)
        .unwrap();
    let recreated = storage_cluster.head_bucket_info(&bucket_name).unwrap();
    assert_eq!(recreated.owner_principal, "victim-owner");
    assert_eq!(recreated.state, storage::BucketState::Active);
    assert_ne!(
        recreated.bucket_incarnation_generation, stale_authorized.bucket_incarnation_generation,
        "recreated bucket must be a distinct incarnation"
    );

    let err = storage_cluster
        .begin_bucket_delete_if_current(
            &stale_authorized.name,
            storage::cluster::BucketIdentityGenerations {
                bucket_execution_generation: stale_authorized.bucket_execution_generation,
                bucket_incarnation_generation: stale_authorized.bucket_incarnation_generation,
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            storage::BucketWriteDrainError::Store(
                storage::StoreError::MetadataCommandContention { .. }
            )
        ),
        "stale authorization should return retryable contention, got {err:?}"
    );

    let still_active = storage_cluster.head_bucket_info(&bucket_name).unwrap();
    assert_eq!(still_active.owner_principal, "victim-owner");
    assert_eq!(still_active.state, storage::BucketState::Active);
    assert_eq!(
        still_active.bucket_incarnation_generation,
        recreated.bucket_incarnation_generation
    );
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
    let admin = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

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
    let admin = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

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
    let admin = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

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
    let admin = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

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
    let admin = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader_after_reload =
        setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

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
fn head_object_reloads_snapshot_when_fast_path_identity_validation_fails() {
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
        Some(true),
        "the authoritative snapshot reload should replace the rejected fast-path entry"
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

    storage_cluster
        .test_begin_bucket_delete_if_current(&bucket_name)
        .unwrap();
    delete_bucket_metadata_or_accept_reclaim_worker_finalize(&storage_cluster, &bucket_name);
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
            ownership_controls: storage::BucketOwnershipControls {
                object_ownership: storage::BucketObjectOwnership::ObjectWriter,
            },
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

    storage_cluster
        .test_begin_bucket_delete_if_current(&bucket_name)
        .unwrap();
    delete_bucket_metadata_or_accept_reclaim_worker_finalize(&storage_cluster, &bucket_name);
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
            ownership_controls: storage::BucketOwnershipControls {
                object_ownership: storage::BucketObjectOwnership::ObjectWriter,
            },
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

    storage_cluster
        .test_begin_bucket_delete_if_current(&bucket_name)
        .unwrap();
    delete_bucket_metadata_or_accept_reclaim_worker_finalize(&storage_cluster, &bucket_name);

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
            ownership_controls: storage::BucketOwnershipControls {
                object_ownership: storage::BucketObjectOwnership::ObjectWriter,
            },
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
    let admin = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let deleter = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

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
fn complete_multipart_upload_does_not_deadlock_when_bucket_policy_shares_pg() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-complete-same-pg";
    let pg_ids: Vec<u32> = (0..METADATA_FANOUT_TEST_PG_COUNT).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
        Arc::clone(&storage_cluster),
    );
    let completer = setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
        Arc::clone(&storage_cluster),
    );

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
    let put = test_helpers::put_object(
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
    let segments = coord
        .storage_node()
        .test_get_object_segments(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("empty"),
            put.version_id,
        )
        .unwrap();
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].size, 0);

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
            .storage_node()
            .test_get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
            .unwrap()
        {
            StoredObject::Live(record) => {
                let segments = coord
                    .storage_node()
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
fn shard_scavenger_audit_records_file_without_row_observations() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_pg_count_without_background_sweepers(tmp.path(), 1);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let reservation_id = storage::SessionId::try_from("77".repeat(16)).unwrap();
    let generation_id = coord
        .storage_node()
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let written = coord
        .storage_node()
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &[0xe7; 16],
            b"background shard scavenger audit candidate",
        )
        .unwrap();

    coord
        .storage_node()
        .audit_shard_storage_for_scavenger()
        .unwrap();
    let observations = coord
        .storage_node()
        .test_list_shard_scavenger_observations(written.data_pg_id)
        .unwrap();
    assert!(
        written.written_shards.iter().all(|shard| {
            observations.iter().any(|observation| {
                observation.reason == ShardScavengerObservationReason::FileWithoutShardRow
                    && observation.resolved_at.is_none()
                    && observation.key.data_pg_id == written.data_pg_id
                    && observation.key.shard_key == shard.key
            })
        }),
        "shard scavenger audit did not record file-without-row observations; observations={observations:?}"
    );
}

#[test]
fn read_discovered_corrupt_shard_queues_background_repair_without_inline_rewrite() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_pg_count_without_background_sweepers(tmp.path(), 1);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"foreground read should not rewrite corrupt shard inline";
    test_helpers::put_object(
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

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let segment = coord
        .storage_node()
        .test_get_object_segments(&bucket, &key, VersionId::Null)
        .unwrap()
        .pop()
        .expect("put object should create one segment");
    let corrupt_shard_index = 0;
    let corrupt_path = shard_file_path(&coord, "bucket", "key", corrupt_shard_index);
    let original_bytes = std::fs::read(&corrupt_path).unwrap();
    corrupt_shard_on_disk(&coord, "bucket", "key", corrupt_shard_index);
    let corrupt_bytes = std::fs::read(&corrupt_path).unwrap();
    assert_ne!(corrupt_bytes, original_bytes);

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
    assert_eq!(result.body.read_all().unwrap(), data);

    assert_eq!(
        std::fs::read(&corrupt_path).unwrap(),
        corrupt_bytes,
        "foreground read recovery must not rewrite the damaged shard inline"
    );
    let repairs = coord
        .storage_node()
        .list_placed_segment_shard_repairs(segment.data_pg_id)
        .unwrap();
    assert_eq!(repairs.len(), 1);
    let repair = &repairs[0].work_item;
    assert_eq!(repair.request.data_pg_id, segment.data_pg_id);
    assert_eq!(repair.request.segment_okh, segment.segment_okh);
    assert_eq!(repair.request.segment_vid, segment.segment_vid);
    assert_eq!(repair.request.segment_crc64, segment.segment_crc64);
    assert_eq!(repair.shard_index.get(), corrupt_shard_index);
    assert_eq!(
        coord
            .storage_node()
            .try_take_placed_segment_shard_repair_work(),
        Some(*repair),
        "successful read recovery should leave a background repair wake hint"
    );
}

#[test]
fn copy_object_discovered_corrupt_source_shard_queues_background_repair() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_pg_count_without_background_sweepers(tmp.path(), 1);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"copy should recover its source and queue repair";
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

    let bucket = trusted_bucket_name("bucket");
    let source_key = trusted_object_key("src");
    let segment = coord
        .storage_node()
        .test_get_object_segments(&bucket, &source_key, VersionId::Null)
        .unwrap()
        .pop()
        .expect("source object should create one segment");
    let corrupt_shard_index = 0;
    corrupt_shard_on_disk(&coord, "bucket", "src", corrupt_shard_index);

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

    let repairs = coord
        .storage_node()
        .list_placed_segment_shard_repairs(segment.data_pg_id)
        .unwrap();
    assert_eq!(repairs.len(), 1);
    let repair = &repairs[0].work_item;
    assert_eq!(repair.request.data_pg_id, segment.data_pg_id);
    assert_eq!(repair.request.segment_okh, segment.segment_okh);
    assert_eq!(repair.request.segment_vid, segment.segment_vid);
    assert_eq!(repair.request.segment_crc64, segment.segment_crc64);
    assert_eq!(repair.shard_index.get(), corrupt_shard_index);
}

#[test]
fn upload_part_copy_discovered_corrupt_source_shard_queues_background_repair() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_pg_count_without_background_sweepers(tmp.path(), 1);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"upload part copy should recover its source and queue repair";
    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data,
            metadata: &metadata,
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

    let bucket = trusted_bucket_name("bucket");
    let source_key = trusted_object_key("src");
    let segment = coord
        .storage_node()
        .test_get_object_segments(&bucket, &source_key, VersionId::Null)
        .unwrap()
        .pop()
        .expect("source object should create one segment");
    let corrupt_shard_index = 0;
    corrupt_shard_on_disk(&coord, "bucket", "src", corrupt_shard_index);

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
                checksum: None,
            }],
            claimed_checksum: None,
            expected_object_size: None,
            cond: NO_WRITE,
            sse_customer: None,
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

    let repairs = coord
        .storage_node()
        .list_placed_segment_shard_repairs(segment.data_pg_id)
        .unwrap();
    assert_eq!(repairs.len(), 1);
    let repair = &repairs[0].work_item;
    assert_eq!(repair.request.data_pg_id, segment.data_pg_id);
    assert_eq!(repair.request.segment_okh, segment.segment_okh);
    assert_eq!(repair.request.segment_vid, segment.segment_vid);
    assert_eq!(repair.request.segment_crc64, segment.segment_crc64);
    assert_eq!(repair.shard_index.get(), corrupt_shard_index);
}

#[test]
fn retained_read_skips_repair_record_after_admitted_route_expiry_without_publication() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0]);
    let initial_coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&initial));
    initial_coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"expired retained read must not record repair through renewed raw authority";
    test_helpers::put_object(
        &initial_coord,
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

    let time = storage::clock::test_time_override_guard(1_000);
    let cluster = same_store_cluster_with_route_map_validity(
        &initial,
        tmp.path(),
        RouteMapValidity::until_ms(5_000).unwrap(),
    );
    let coord = setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
        Arc::clone(&cluster),
    );
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
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let segment = cluster
        .test_get_object_segments(&bucket, &key, VersionId::Null)
        .unwrap()
        .pop()
        .expect("put object should create one segment");
    corrupt_shard_on_disk(&coord, "bucket", "key", 0);

    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
    time.set(6_000);
    assert_eq!(result.body.read_all().unwrap(), data);
    assert!(
        cluster
            .list_placed_segment_shard_repairs(segment.data_pg_id)
            .unwrap()
            .is_empty(),
        "expired admitted authority must not record repair through the renewed raw route"
    );
}

#[test]
fn shard_repair_worker_retries_after_transient_shard_read_error() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = Coordinator::new_with_background_sweeper_factories_for_storage_cluster(
        Arc::clone(&storage_cluster),
        "us-east-1".to_string(),
        None,
        Some(test_sse_s3_provider()),
        (
            false,
            |_, _| Ok(LifecycleSweeper::disabled()),
            |_| Ok(ShardScavengerSweeper::disabled()),
            |storage_handle| Ok(ShardRepairSweeper::disabled(storage_handle.clone())),
            |_| Ok(ShardBackfillSweeper::disabled()),
            |_| Ok(StreamSessionSweeper::disabled()),
        ),
    )
    .unwrap();

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"background shard repair retries transient read failure";
    test_helpers::put_object(
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

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let segment = coord
        .storage_node()
        .test_get_object_segments(&bucket, &key, VersionId::Null)
        .unwrap()
        .pop()
        .expect("put object should create one segment");
    let corrupt_shard_index = 0;
    let corrupt_path = shard_file_path(&coord, "bucket", "key", corrupt_shard_index);
    corrupt_shard_on_disk(&coord, "bucket", "key", corrupt_shard_index);
    let corrupt_bytes = std::fs::read(&corrupt_path).unwrap();

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
    assert_eq!(result.body.read_all().unwrap(), data);

    let fail_once = Arc::new(AtomicBool::new(true));
    let failure_injected = Arc::new(AtomicBool::new(false));
    let hook_fail_once = Arc::clone(&fail_once);
    let hook_failure_injected = Arc::clone(&failure_injected);
    let _read_hook_guard = storage_cluster.test_install_before_placed_payload_shard_read_hook(
        Arc::new(move |location, _shard_key| {
            if hook_fail_once.swap(false, Ordering::SeqCst) {
                hook_failure_injected.store(true, Ordering::SeqCst);
                return Err(storage::StoreError::StorageRpcResourceExhausted {
                    node_id: location.node_id().as_u32(),
                    operation: "repair read payload shard",
                    message: "test injected transient shard repair read failure".to_string(),
                });
            }
            Ok(())
        }),
    );
    let _worker = setup_coordinator_with_only_shard_repair_worker(Arc::clone(&storage_cluster));

    let start = std::time::Instant::now();
    loop {
        let repairs = coord
            .storage_node()
            .list_placed_segment_shard_repairs(segment.data_pg_id)
            .unwrap();
        if repairs.iter().any(|repair| {
            repair.last_error.as_deref().is_some_and(|error| {
                error.contains("test injected transient shard repair read failure")
            })
        }) {
            break;
        }
        assert!(
            start.elapsed() < TEST_EVENT_TIMEOUT,
            "shard repair worker did not record transient failure; repairs={repairs:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(failure_injected.load(Ordering::SeqCst));

    let start = std::time::Instant::now();
    loop {
        let repairs = coord
            .storage_node()
            .list_placed_segment_shard_repairs(segment.data_pg_id)
            .unwrap();
        if repairs.is_empty() {
            let repaired_bytes = std::fs::read(&corrupt_path).unwrap();
            assert_ne!(
                repaired_bytes, corrupt_bytes,
                "retry should rewrite the corrupt shard after transient failure"
            );
            return;
        }
        assert!(
            start.elapsed() < TEST_EVENT_TIMEOUT + Duration::from_secs(2),
            "shard repair worker did not retry and drain row; repairs={repairs:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn shard_repair_worker_records_unrecoverable_repair_without_partial_write() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = Coordinator::new_with_background_sweeper_factories_for_storage_cluster(
        Arc::clone(&storage_cluster),
        "us-east-1".to_string(),
        None,
        Some(test_sse_s3_provider()),
        (
            false,
            |_, _| Ok(LifecycleSweeper::disabled()),
            |_| Ok(ShardScavengerSweeper::disabled()),
            |storage_handle| Ok(ShardRepairSweeper::disabled(storage_handle.clone())),
            |_| Ok(ShardBackfillSweeper::disabled()),
            |_| Ok(StreamSessionSweeper::disabled()),
        ),
    )
    .unwrap();

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"background shard repair fails closed when too many shards are unavailable";
    test_helpers::put_object(
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

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let segment = coord
        .storage_node()
        .test_get_object_segments(&bucket, &key, VersionId::Null)
        .unwrap()
        .pop()
        .expect("put object should create one segment");
    let corrupt_path = shard_file_path(&coord, "bucket", "key", 0);
    corrupt_shard_on_disk(&coord, "bucket", "key", 0);
    let corrupt_bytes = std::fs::read(&corrupt_path).unwrap();

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
    assert_eq!(result.body.read_all().unwrap(), data);

    let missing_paths = [
        shard_file_path(&coord, "bucket", "key", 1),
        shard_file_path(&coord, "bucket", "key", 2),
    ];
    for path in &missing_paths {
        std::fs::remove_file(path).unwrap();
    }

    let _worker = setup_coordinator_with_only_shard_repair_worker(Arc::clone(&storage_cluster));
    let start = std::time::Instant::now();
    loop {
        let repairs = coord
            .storage_node()
            .list_placed_segment_shard_repairs(segment.data_pg_id)
            .unwrap();
        if repairs
            .iter()
            .any(|repair| repair.last_error.as_deref().is_some())
        {
            assert_eq!(repairs.len(), 1);
            assert_eq!(repairs[0].work_item.shard_index.get(), 0);
            assert_eq!(std::fs::read(&corrupt_path).unwrap(), corrupt_bytes);
            for path in &missing_paths {
                assert!(
                    !path.exists(),
                    "unrecoverable repair should not recreate any shard from an insufficient EC set"
                );
            }
            return;
        }
        assert!(
            start.elapsed() < TEST_EVENT_TIMEOUT,
            "shard repair worker did not record unrecoverable repair failure; repairs={repairs:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn shard_repair_worker_repairs_read_discovered_corrupt_shard() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let _hook_serial = SHARD_REPAIR_WORKER_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let target_registry_key = storage_cluster.process_local_registry_key();
    let (idle_tx, idle_rx) = mpsc::channel();
    let _hook_guard = install_shard_repair_worker_test_hooks(ShardRepairWorkerTestHooks {
        target_registry_key: Some(target_registry_key),
        after_idle_timeout: Some(Arc::new(move || {
            let _ = idle_tx.send(());
        })),
    });
    let coord = Coordinator::new_with_background_sweeper_factories_for_storage_cluster(
        storage_cluster,
        "us-east-1".to_string(),
        None,
        Some(test_sse_s3_provider()),
        (
            false,
            |_, _| Ok(LifecycleSweeper::disabled()),
            |_| Ok(ShardScavengerSweeper::disabled()),
            ShardRepairSweeper::acquire_shared,
            |_| Ok(ShardBackfillSweeper::disabled()),
            |_| Ok(StreamSessionSweeper::disabled()),
        ),
    )
    .unwrap();
    idle_rx
        .recv_timeout(TEST_EVENT_TIMEOUT)
        .expect("shard repair worker did not reach the idle timeout path");

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"background shard repair worker payload";
    test_helpers::put_object(
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

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let segment = coord
        .storage_node()
        .test_get_object_segments(&bucket, &key, VersionId::Null)
        .unwrap()
        .pop()
        .expect("put object should create one segment");
    let corrupt_shard_index = 0;
    let corrupt_path = shard_file_path(&coord, "bucket", "key", corrupt_shard_index);
    corrupt_shard_on_disk(&coord, "bucket", "key", corrupt_shard_index);
    let corrupt_bytes = std::fs::read(&corrupt_path).unwrap();

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
    assert_eq!(result.body.read_all().unwrap(), data);

    let start = std::time::Instant::now();
    loop {
        let repairs = coord
            .storage_node()
            .list_placed_segment_shard_repairs(segment.data_pg_id)
            .unwrap();
        if repairs.is_empty() {
            let repaired = coord
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
            assert_eq!(repaired.body.read_all().unwrap(), data);
            let repaired_bytes = std::fs::read(&corrupt_path).unwrap();
            assert_ne!(
                repaired_bytes, corrupt_bytes,
                "repair should rewrite the corrupt shard file"
            );
            return;
        }
        assert!(
            start.elapsed() < TEST_EVENT_TIMEOUT,
            "shard repair worker did not repair and drain row; repairs={repairs:?}"
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
            .storage_node()
            .test_get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
            .unwrap()
        {
            StoredObject::Live(record) => {
                let segments = coord
                    .storage_node()
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
        .storage_node()
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
                .storage_node()
                .test_shard_exists(data_pg_id, &shard_key)
                .unwrap(),
            "failed reclaim should keep ack metadata {shard_index} retryable"
        );
        assert!(
            coord
                .storage_node()
                .test_payload_shard_file_exists(data_pg_id, ec, &okh, segment_vid, shard_index)
                .unwrap(),
            "failed reclaim should keep placed shard {shard_index} retryable"
        );
    }

    drop(placed_cleanup_guard);
    reclaim_object_payload(&coord, "bucket", "key", generation_id);
    assert_shard_set_deleted(&coord, data_pg_id, &okh, segment_vid, ec);
}

fn setup_deleted_object_reclaim_test(
    payload: &[u8],
) -> (
    test_util::TempDir,
    Coordinator,
    BucketName,
    ObjectKey,
    GenerationId,
) {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_reclaim_sweeper(tmp.path());
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");

    coord
        .create_bucket_for_owner("default-owner", bucket.as_str(), false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket.as_str(),
                key.as_str(),
                test_requester(),
                None,
            ),
            data: payload,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let generation_id = coord
        .storage_node()
        .test_get_object_meta(&bucket, &key)
        .unwrap()
        .into_live()
        .expect("put object should create a live object")
        .generation_id;
    coord
        .delete_object(&delete_object_request(
            bucket.as_str(),
            key.as_str(),
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap();

    (tmp, coord, bucket, key, generation_id)
}

#[test]
fn reclaim_zero_apply_failure_releases_claim_after_pending_slot_cleanup() {
    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap();
    let (_tmp, coord, bucket, key, generation_id) =
        setup_deleted_object_reclaim_test(b"claim-release-after-zero-apply");

    let failed_once = Arc::new(AtomicBool::new(false));
    let hook_failed_once = Arc::clone(&failed_once);
    let apply_guard = coord
        .storage_node()
        .test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::DeleteObjectPayloadReclaim
                && !hook_failed_once.swap(true, Ordering::SeqCst)
            {
                return Err(storage::StoreError::StaleMetadataOperation {
                    pg_id: 0,
                    operation_epoch: ClusterEpoch::INITIAL,
                    current_epoch: ClusterEpoch::new(2).unwrap(),
                });
            }
            Ok(())
        }));

    let error = coord
        .read_runtime()
        .try_reclaim_object_payload("bucket", "key", generation_id)
        .unwrap_err();
    assert!(failed_once.load(Ordering::SeqCst));
    assert!(
        matches!(error, ServerError::OperationAborted),
        "zero-apply route failure should remain retryable, got {error:?}"
    );

    drop(apply_guard);
    assert!(
        coord
            .read_runtime()
            .try_reclaim_object_payload("bucket", "key", generation_id)
            .unwrap(),
        "retry must complete rather than defer behind a leaked reclaim claim"
    );
    assert!(
        !coord
            .storage_node()
            .test_payload_reclaim_exists(&bucket, &key, generation_id,)
            .unwrap(),
        "completed retry must remove the durable reclaim root"
    );
}

#[test]
fn reclaim_route_failure_after_claim_acquisition_releases_claim() {
    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap();
    let (_tmp, coord, bucket, key, generation_id) =
        setup_deleted_object_reclaim_test(b"claim-release-after-route-failure");

    let admission_failed = Arc::new(AtomicBool::new(false));
    let hook_admission_failed = Arc::clone(&admission_failed);
    let claim_guard = coord
        .storage_node()
        .test_install_after_reclaim_claim_acquired_hook(Arc::new(move || {
            hook_admission_failed.store(true, Ordering::SeqCst);
            Err(storage::ObjectPgActionError::Store(
                storage::StoreError::StaleMetadataOperation {
                    pg_id: 0,
                    operation_epoch: ClusterEpoch::INITIAL,
                    current_epoch: ClusterEpoch::new(2).unwrap(),
                },
            ))
        }));

    let error = coord
        .read_runtime()
        .try_reclaim_object_payload("bucket", "key", generation_id)
        .unwrap_err();
    assert!(admission_failed.load(Ordering::SeqCst));
    assert!(
        matches!(error, ServerError::OperationAborted),
        "post-claim route failure should remain retryable, got {error:?}"
    );
    assert!(
        !coord
            .storage_node()
            .test_object_payload_reclaim_is_active(&bucket, &key, generation_id),
        "failure before reclaim admission must not create a process-local active slot"
    );

    drop(claim_guard);
    assert!(
        coord
            .read_runtime()
            .try_reclaim_object_payload("bucket", "key", generation_id)
            .unwrap(),
        "retry must not defer behind the claim acquired by the failed attempt"
    );
    assert!(
        !coord
            .storage_node()
            .test_payload_reclaim_exists(&bucket, &key, generation_id)
            .unwrap(),
        "completed retry must remove the durable reclaim root"
    );
}

#[test]
fn reclaim_ownership_lookup_failure_clears_active_reclaim_slot() {
    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap();
    let (_tmp, coord, bucket, key, generation_id) =
        setup_deleted_object_reclaim_test(b"claim-ownership-lookup-failure");

    let apply_failed = Arc::new(AtomicBool::new(false));
    let hook_apply_failed = Arc::clone(&apply_failed);
    let _apply_guard = coord
        .storage_node()
        .test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::DeleteObjectPayloadReclaim
                && !hook_apply_failed.swap(true, Ordering::SeqCst)
            {
                return Err(storage::StoreError::StaleMetadataOperation {
                    pg_id: 0,
                    operation_epoch: ClusterEpoch::INITIAL,
                    current_epoch: ClusterEpoch::new(2).unwrap(),
                });
            }
            Ok(())
        }));
    let ownership_lookup_failed = Arc::new(AtomicBool::new(false));
    let hook_ownership_lookup_failed = Arc::clone(&ownership_lookup_failed);
    let _lookup_guard = coord
        .storage_node()
        .test_install_before_reclaim_ownership_lookup_hook(Arc::new(move || {
            hook_ownership_lookup_failed.store(true, Ordering::SeqCst);
            Err(storage::ObjectPgActionError::Store(
                storage::StoreError::Io {
                    context: "injected reclaim ownership lookup failure",
                    source: std::io::Error::other("injected reclaim ownership lookup failure"),
                },
            ))
        }));

    let error = coord
        .read_runtime()
        .try_reclaim_object_payload("bucket", "key", generation_id)
        .unwrap_err();
    assert!(apply_failed.load(Ordering::SeqCst));
    assert!(ownership_lookup_failed.load(Ordering::SeqCst));
    assert!(
        matches!(error, ServerError::OperationAborted),
        "the original retryable apply error should be preserved, got {error:?}"
    );
    assert!(
        !coord
            .storage_node()
            .test_object_payload_reclaim_is_active(&bucket, &key, generation_id),
        "ownership uncertainty must retain the fence without stranding the process-local active slot"
    );
    assert!(
        coord
            .storage_node()
            .test_payload_reclaim_exists(&bucket, &key, generation_id)
            .unwrap(),
        "ownership uncertainty must preserve the durable reclaim root"
    );
}

#[test]
fn reclaim_claim_release_failure_clears_active_reclaim_slot() {
    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap();
    let (_tmp, coord, bucket, key, generation_id) =
        setup_deleted_object_reclaim_test(b"claim-release-failure");

    let apply_failed = Arc::new(AtomicBool::new(false));
    let hook_apply_failed = Arc::clone(&apply_failed);
    let _apply_guard = coord
        .storage_node()
        .test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::DeleteObjectPayloadReclaim
                && !hook_apply_failed.swap(true, Ordering::SeqCst)
            {
                return Err(storage::StoreError::StaleMetadataOperation {
                    pg_id: 0,
                    operation_epoch: ClusterEpoch::INITIAL,
                    current_epoch: ClusterEpoch::new(2).unwrap(),
                });
            }
            Ok(())
        }));
    let claim_release_failed = Arc::new(AtomicBool::new(false));
    let hook_claim_release_failed = Arc::clone(&claim_release_failed);
    let _release_guard = coord
        .storage_node()
        .test_install_before_reclaim_claim_release_hook(Arc::new(move || {
            hook_claim_release_failed.store(true, Ordering::SeqCst);
            Err(storage::ObjectPgActionError::Store(
                storage::StoreError::Io {
                    context: "injected reclaim claim release failure",
                    source: std::io::Error::other("injected reclaim claim release failure"),
                },
            ))
        }));

    let error = coord
        .read_runtime()
        .try_reclaim_object_payload("bucket", "key", generation_id)
        .unwrap_err();
    assert!(apply_failed.load(Ordering::SeqCst));
    assert!(claim_release_failed.load(Ordering::SeqCst));
    assert!(
        matches!(
            error,
            ServerError::Store(storage::StoreError::Io {
                context: "injected reclaim claim release failure",
                ..
            })
        ),
        "claim release failure should be returned, got {error:?}"
    );
    assert!(
        !coord
            .storage_node()
            .test_object_payload_reclaim_is_active(&bucket, &key, generation_id),
        "claim release uncertainty must retain the fence without stranding the process-local active slot"
    );
    assert!(
        coord
            .storage_node()
            .test_payload_reclaim_exists(&bucket, &key, generation_id)
            .unwrap(),
        "claim release uncertainty must preserve the durable reclaim root"
    );
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
            .storage_node()
            .test_get_object_meta(&bucket_name, &object_key)
            .unwrap();
        let segments = coord
            .storage_node()
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
        .storage_node()
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

fn setup_ec_fault_injection_coordinator(dir: &std::path::Path) -> Coordinator {
    setup_coordinator_with_pg_count_without_background_sweepers(dir, DEFAULT_TEST_PG_COUNT)
}

#[test]
fn ec_reconstruction_after_shard_loss() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let tmp = test_util::tempdir();
    let coord = setup_ec_fault_injection_coordinator(tmp.path());

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
    let coord = setup_ec_fault_injection_coordinator(tmp.path());

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
    let coord = setup_ec_fault_injection_coordinator(tmp.path());

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
    let ec = coord.storage_node().default_payload_ec_shape();
    assert_eq!(coord.storage_node().test_ec_scratch_allocation_count(ec), 1);

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
    assert_eq!(coord.storage_node().test_ec_scratch_allocation_count(ec), 1);

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
    assert_eq!(coord.storage_node().test_ec_scratch_allocation_count(ec), 1);
}

#[test]
fn ec_drop_m_shards_at_limit() {
    if !backend_supports_parity_recovery() {
        return;
    }
    // Config: k=4, m=2. Dropping exactly m=2 shards should still recover.
    let tmp = test_util::tempdir();
    let coord = setup_ec_fault_injection_coordinator(tmp.path());

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
    let coord = setup_ec_fault_injection_coordinator(tmp.path());

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
    assert!(
        matches!(err, ServerError::Store(storage::StoreError::NotFound)),
        "unrecoverable payload loss must not be reported as NoSuchKey: {err:?}"
    );
    assert_eq!(err.http_status(), 500);
    assert_eq!(err.s3_error_code(), "InternalError");
}

#[test]
fn ec_corrupt_one_data_shard_recovery() {
    if !backend_supports_parity_recovery() {
        return;
    }
    // Corrupt shard 0 on disk. PgStore detects CRC mismatch, EC reconstructs.
    let tmp = test_util::tempdir();
    let coord = setup_ec_fault_injection_coordinator(tmp.path());

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
    let coord = setup_ec_fault_injection_coordinator(tmp.path());

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
        .storage_node()
        .test_get_object_meta(&bucket, &key)
        .unwrap()
        .into_live()
        .expect("put object should create a live object")
        .generation_id;
    let segment = coord
        .storage_node()
        .test_get_object_segments(&bucket, &key, put.version_id)
        .unwrap()
        .pop()
        .expect("put object should create one object segment");
    let expected_selected_nodes = coord
        .storage_node()
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
            .storage_node()
            .object_payload_lease_holder_node_count(&bucket, &key, generation_id),
        expected_selected_nodes,
        "degraded EC range read should hold handles for the selected recovery shard-owner set"
    );
    assert_eq!(result.body.read_all().unwrap(), b"Hello");
    assert_eq!(
        coord
            .storage_node()
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
    let coord = setup_ec_fault_injection_coordinator(tmp.path());

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
    let coord = setup_ec_fault_injection_coordinator(tmp.path());

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
            .storage_node()
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
            .storage_node()
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
    let coord = setup_ec_fault_injection_coordinator(tmp.path());

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
            .storage_node()
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
            .storage_node()
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
    assert_eq!(MAX_MULTIPART_PARTS, 10_000);
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

    // Build a part list with MAX_MULTIPART_PARTS + 1 entries.
    let parts: Vec<_> = (1..=MAX_MULTIPART_PARTS as u32 + 1)
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
    assert!(matches!(err, ServerError::CompleteMultipartTooManyParts));
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
    assert!(matches!(
        err,
        ServerError::InvalidRange { total_size: 5, .. }
    ));
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
    assert!(matches!(err, ServerError::PreconditionFailed { .. }));
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
    assert!(matches!(err, ServerError::PreconditionFailed { .. }));
}
