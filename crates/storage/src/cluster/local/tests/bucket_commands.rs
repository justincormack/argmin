// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{BucketAclSummary, StorageClusterRouteHandle};
use s3_types::{AclGrant, AclGrantee, AclPermission};

#[test]
fn composite_object_listings_fan_out_to_routed_pg_primaries() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let topology = map
        .nodes
        .get(&NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let key_a = key_for_object_pg(topology, &bucket, 1, "dir/a/file-");
    let key_b = key_for_object_pg(topology, &bucket, 2, "dir/b/file-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    write_committed_direct_segment_for_with_okh(&cluster, &bucket, &key_b, [52; 16], b"payload-b");
    write_committed_direct_segment_for_with_okh(&cluster, &bucket, &key_a, [53; 16], b"payload-a");

    let bridge_node = map.node(NodeId::new(0)).unwrap().storage_node();
    let bridge_pg_a = bridge_node.get_pg(1).unwrap();
    crate::PgMetadataStore::delete_object_meta(&*bridge_pg_a, &bucket, &key_a).unwrap();
    drop(bridge_pg_a);
    let bridge_pg_b = bridge_node.get_pg(2).unwrap();
    crate::PgMetadataStore::delete_object_meta(&*bridge_pg_b, &bucket, &key_b).unwrap();
    drop(bridge_pg_b);
    assert!(bridge_node.test_get_object_meta(&bucket, &key_a).is_err());
    assert!(bridge_node.test_get_object_meta(&bucket, &key_b).is_err());

    let all_objects = cluster.list_all_objects_for_bucket(&bucket).unwrap();
    assert_eq!(
        all_objects
            .iter()
            .map(|object| object.key())
            .collect::<Vec<_>>(),
        vec![&key_a, &key_b]
    );

    let listed = cluster
        .list_objects_for_bucket(&bucket, None, None, None, 100)
        .unwrap();
    assert_eq!(
        listed
            .objects
            .iter()
            .map(|object| object.key())
            .collect::<Vec<_>>(),
        vec![&key_a, &key_b]
    );

    let prefix = crate::ObjectKey::try_from("dir/".to_string()).unwrap();
    let delimited = cluster
        .list_objects_for_bucket(&bucket, Some(&prefix), Some("/"), None, 100)
        .unwrap();
    assert!(delimited.objects.is_empty());
    assert_eq!(
        delimited
            .common_prefixes
            .iter()
            .map(crate::ObjectKey::as_str)
            .collect::<Vec<_>>(),
        vec!["dir/a/", "dir/b/"]
    );

    let all_versions = cluster
        .list_all_object_versions_for_bucket(&bucket)
        .unwrap();
    assert_eq!(
        all_versions
            .iter()
            .map(|object| object.key())
            .collect::<Vec<_>>(),
        vec![&key_a, &key_b]
    );

    let listed_versions = cluster
        .list_object_versions_for_bucket(&bucket, None, None, None, None, 100)
        .unwrap();
    assert_eq!(
        listed_versions
            .versions
            .iter()
            .map(|object| object.key())
            .collect::<Vec<_>>(),
        vec![&key_a, &key_b]
    );
}

#[test]
fn object_and_multipart_listing_select_global_first_page_at_production_cap_volume() {
    const PG_COUNT: u32 = 100;
    const RECORDS_PER_PG: usize = 1_001;
    const MAX_KEYS: u32 = 1_000;

    let tmp = test_util::tempdir();
    let node_id = NodeId::new(0);
    let pg_ids = (0..PG_COUNT).collect::<Vec<_>>();
    let map =
        LocalClusterMap::open(tmp.path(), &[node_id], &pg_ids, EcShape { k: 1, m: 0 }).unwrap();
    let bucket = crate::BucketName::try_from("listing-scale-bucket".to_string()).unwrap();
    let node = map.node(node_id).unwrap().storage_node();
    let topology = node.pg_topology();
    let mut expected_smallest_pg_keys = Vec::new();
    let mut expected_smallest_pg_uploads = Vec::new();

    for pg_id in pg_ids.iter().copied() {
        let lexical_group = PG_COUNT - 1 - pg_id;
        let mut keys = Vec::with_capacity(RECORDS_PER_PG);
        for rank in 0..RECORDS_PER_PG {
            keys.push(key_for_object_pg(
                topology,
                &bucket,
                pg_id,
                &format!("{lexical_group:03}/{rank:04}/"),
            ));
        }
        let uploads = keys
            .iter()
            .enumerate()
            .map(|(rank, key)| {
                (
                    key.clone(),
                    upload_id_from_label(&format!("listing{pg_id:03}{rank:04}")),
                )
            })
            .collect::<Vec<_>>();
        if lexical_group == 0 {
            expected_smallest_pg_keys.clone_from(&keys);
            expected_smallest_pg_uploads.clone_from(&uploads);
        }
        let pg = node.get_pg(pg_id).unwrap();
        pg.test_insert_listing_objects(&bucket, &keys).unwrap();
        pg.test_insert_listing_multipart_uploads(&bucket, &uploads)
            .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }

    let cluster = crate::StorageCluster::from_static_local_map(Arc::new(map)).unwrap();
    let listed = cluster
        .list_objects_for_bucket(&bucket, None, None, None, MAX_KEYS)
        .unwrap();
    let listed_keys = listed
        .objects
        .iter()
        .map(|object| object.key())
        .collect::<Vec<_>>();
    let expected = expected_smallest_pg_keys[..MAX_KEYS as usize]
        .iter()
        .collect::<Vec<_>>();

    assert_eq!(listed_keys, expected);
    assert!(listed.is_truncated);
    assert_eq!(
        listed.next_continuation_token.as_ref(),
        expected_smallest_pg_keys.get(MAX_KEYS as usize - 1)
    );

    let listed_uploads = cluster
        .list_multipart_uploads_for_bucket(&bucket, None, None, None, None, MAX_KEYS)
        .unwrap();
    assert!(listed_uploads.is_truncated());
    let expected_last_upload = &expected_smallest_pg_uploads[MAX_KEYS as usize - 1];
    assert_eq!(
        listed_uploads.next_marker(),
        Some(&crate::MultipartUploadListMarker::Upload {
            key: expected_last_upload.0.clone(),
            upload_id: expected_last_upload.1.clone(),
        })
    );
    let listed_debug = format!("{listed_uploads:?}");
    assert!(!listed_debug.contains("object_generation_id"));
    assert!(!listed_debug.contains("metadata_blob"));
    assert!(!listed_debug.contains("system_metadata_blob"));
    assert!(!listed_debug.contains("encryption"));
    let listed_uploads = listed_uploads
        .uploads()
        .iter()
        .map(|upload| (upload.key.clone(), upload.upload_id.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        listed_uploads,
        expected_smallest_pg_uploads[..MAX_KEYS as usize]
    );
}

#[test]
fn multipart_upload_global_merge_uses_pg_listing_position_order() {
    let tmp = test_util::tempdir();
    let node_id = NodeId::new(0);
    let map = LocalClusterMap::open(tmp.path(), &[node_id], &[0], EcShape { k: 1, m: 0 }).unwrap();
    let bucket = crate::BucketName::try_from("multipart-order-bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("same-key".to_string()).unwrap();
    let upload_ids = [
        upload_id_from_label("orderc"),
        upload_id_from_label("ordera"),
        upload_id_from_label("orderb"),
    ];
    let node = map.node(node_id).unwrap().storage_node();
    node.get_pg(0)
        .unwrap()
        .test_insert_listing_multipart_uploads(
            &bucket,
            &upload_ids
                .iter()
                .map(|upload_id| (key.clone(), upload_id.clone()))
                .collect::<Vec<_>>(),
        )
        .unwrap();

    let cluster = crate::StorageCluster::from_static_local_map(Arc::new(map)).unwrap();
    let first = cluster
        .list_multipart_uploads_for_bucket(&bucket, None, None, None, None, 2)
        .unwrap();
    assert_eq!(
        first
            .uploads
            .iter()
            .map(|upload| upload.upload_id.clone())
            .collect::<Vec<_>>(),
        upload_ids[..2]
    );
    assert!(first.is_truncated);

    let second = cluster
        .list_multipart_uploads_for_bucket(&bucket, None, None, Some(&key), Some(&upload_ids[1]), 2)
        .unwrap();
    assert_eq!(second.uploads.len(), 1);
    assert_eq!(second.uploads[0].upload_id, upload_ids[2]);
    assert!(!second.is_truncated);
}

#[test]
fn multipart_upload_delimiter_pagination_merges_common_prefix_across_pgs() {
    let tmp = test_util::tempdir();
    let node_id = NodeId::new(0);
    let map =
        LocalClusterMap::open(tmp.path(), &[node_id], &[0, 1, 2], EcShape { k: 1, m: 0 }).unwrap();
    let bucket = crate::BucketName::try_from("multipart-delimiter-bucket".to_string()).unwrap();
    let node = map.node(node_id).unwrap().storage_node();
    let topology = node.pg_topology();
    let first_key = key_for_object_pg(topology, &bucket, 0, "a-root-");
    let first_upload_id = upload_id_from_label("delimiterfirst");
    let prefix_key_one = key_for_object_pg(topology, &bucket, 1, "dir/one-");
    let prefix_key_two = key_for_object_pg(topology, &bucket, 2, "dir/two-");
    let last_key = key_for_object_pg(topology, &bucket, 0, "z-root-");
    let last_upload_id = upload_id_from_label("delimiterlast");

    node.get_pg(0)
        .unwrap()
        .test_insert_listing_multipart_uploads(
            &bucket,
            &[
                (first_key.clone(), first_upload_id.clone()),
                (last_key.clone(), last_upload_id.clone()),
            ],
        )
        .unwrap();
    node.get_pg(1)
        .unwrap()
        .test_insert_listing_multipart_uploads(
            &bucket,
            &[(prefix_key_one, upload_id_from_label("delimiterprefixone"))],
        )
        .unwrap();
    node.get_pg(2)
        .unwrap()
        .test_insert_listing_multipart_uploads(
            &bucket,
            &[(prefix_key_two, upload_id_from_label("delimiterprefixtwo"))],
        )
        .unwrap();

    let cluster = crate::StorageCluster::from_static_local_map(Arc::new(map)).unwrap();
    let first = cluster
        .list_multipart_uploads_for_bucket(&bucket, None, Some("/"), None, None, 1)
        .unwrap();
    assert_eq!(first.uploads.len(), 1);
    assert_eq!(first.uploads[0].key, first_key);
    assert!(first.common_prefixes.is_empty());
    assert!(first.is_truncated);
    assert_eq!(
        first.next_marker,
        Some(crate::MultipartUploadListMarker::Upload {
            key: first_key.clone(),
            upload_id: first_upload_id.clone(),
        })
    );

    let second = cluster
        .list_multipart_uploads_for_bucket(
            &bucket,
            None,
            Some("/"),
            Some(&first_key),
            Some(&first_upload_id),
            1,
        )
        .unwrap();
    assert!(second.uploads.is_empty());
    assert_eq!(
        second
            .common_prefixes
            .iter()
            .map(crate::ObjectKey::as_str)
            .collect::<Vec<_>>(),
        ["dir/"]
    );
    assert!(second.is_truncated);
    let common_prefix = second.common_prefixes[0].clone();
    assert_eq!(
        second.next_marker,
        Some(crate::MultipartUploadListMarker::CommonPrefix(
            common_prefix.clone()
        ))
    );

    let third = cluster
        .list_multipart_uploads_for_bucket(&bucket, None, Some("/"), Some(&common_prefix), None, 1)
        .unwrap();
    assert_eq!(third.uploads.len(), 1);
    assert_eq!(third.uploads[0].key, last_key);
    assert!(third.common_prefixes.is_empty());
    assert!(!third.is_truncated);
    assert_eq!(
        third.next_marker,
        Some(crate::MultipartUploadListMarker::Upload {
            key: last_key,
            upload_id: last_upload_id,
        })
    );
}

#[test]
fn composite_bucket_listings_fail_closed_while_any_metadata_pg_is_peering() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap());
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    write_committed_direct_segment_for_with_okh(
        &cluster,
        &bucket,
        &crate::ObjectKey::try_from("key".to_string()).unwrap(),
        [54; 16],
        b"payload",
    );

    drop(cluster);
    Arc::get_mut(&mut map)
        .unwrap()
        .pg_routes
        .get_mut(&PgId::new(1))
        .unwrap()
        .state = PgState::Peering;
    let cluster = crate::StorageCluster::from_static_local_map(map).unwrap();

    let err = cluster
        .list_objects_for_bucket(&bucket, None, None, None, 100)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::PgNotActive {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Peering,
        })
    ));

    let err = cluster
        .list_object_versions_for_bucket(&bucket, None, None, None, None, 100)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::PgNotActive {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Peering,
        })
    ));

    let err = cluster
        .list_multipart_uploads_for_bucket(&bucket, None, None, None, None, 100)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::PgNotActive {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Peering,
        })
    ));
}

#[test]
fn composite_bucket_listings_fail_closed_when_route_map_expires_during_pg_scan() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap());
    let cluster = Arc::new(crate::StorageCluster::from_static_local_map(map).unwrap());
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let handle = StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let time = crate::clock::test_time_override_guard(1_000);
    let prepare_scan = || {
        time.set(1_000);
        cluster.test_store_route_map_validity(RouteMapValidity::until_ms(5_000).unwrap());
        let admission = handle.admit_current_route().unwrap();
        // Renew the underlying same-generation route after admission. Every
        // scan must still stop at the immutable deadline captured above.
        cluster.test_store_route_map_validity(RouteMapValidity::until_ms(10_000).unwrap());
        let hook_time = time.control();
        let hook =
            cluster.test_install_after_metadata_listing_pg_complete_hook(Arc::new(move |pg_id| {
                if pg_id == 0 {
                    hook_time.set(6_000);
                }
            }));
        (admission, hook)
    };

    let (bucket_admission, bucket_hook) = prepare_scan();
    let owner = crate::CanonicalUserId::from_principal("owner");
    let bucket_error = bucket_admission
        .active_bucket_metadata_scan(&owner)
        .unwrap()
        .list_buckets_for_owner()
        .unwrap_err();
    assert_eq!(
        bucket_error.kind(),
        crate::BucketListingFailureKind::RetryableConvergence
    );
    assert_eq!(
        bucket_error.diagnostic_cause_label(),
        "store_topology_failure"
    );
    drop(bucket_hook);
    drop(bucket_admission);

    let (object_admission, object_hook) = prepare_scan();
    let object_error = object_admission
        .active_object_metadata_scan(&bucket)
        .unwrap()
        .list_objects(None, None, None, 100)
        .unwrap_err();
    assert_eq!(
        object_error.kind(),
        crate::ObjectMetadataListingFailureKind::RetryableConvergence
    );
    assert_eq!(
        object_error.diagnostic_cause_label(),
        "store_topology_failure"
    );
    drop(object_hook);
    drop(object_admission);

    let (version_admission, version_hook) = prepare_scan();
    let version_error = version_admission
        .active_object_metadata_scan(&bucket)
        .unwrap()
        .list_object_versions(None, None, None, None, 100)
        .unwrap_err();
    assert_eq!(
        version_error.kind(),
        crate::ObjectMetadataListingFailureKind::RetryableConvergence
    );
    assert_eq!(
        version_error.diagnostic_cause_label(),
        "store_topology_failure"
    );
    drop(version_hook);
    drop(version_admission);

    let (multipart_admission, multipart_hook) = prepare_scan();
    let multipart_error = multipart_admission
        .active_object_metadata_scan(&bucket)
        .unwrap()
        .list_multipart_uploads(None, None, None, None, 100)
        .unwrap_err();
    assert_eq!(
        multipart_error.kind(),
        crate::ObjectMetadataListingFailureKind::RetryableConvergence
    );
    assert_eq!(
        multipart_error.diagnostic_cause_label(),
        "store_topology_failure"
    );
    drop(multipart_hook);
    drop(multipart_admission);
}

#[test]
fn composite_bucket_listing_fan_out_to_routed_pg_primaries() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket_a, bucket_b) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, 1, "routed-bucket-a-"),
            bucket_for_pg(topology, 2, "routed-bucket-b-"),
        )
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let owner = crate::CanonicalUserId::from_principal("owner");
    seed_bucket_record(&map, NodeId::new(1), 1, &bucket_a, &owner);
    seed_bucket_record(&map, NodeId::new(2), 2, &bucket_b, &owner);

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let bridge_node = map.node(NodeId::new(0)).unwrap().storage_node();
    assert!(bridge_node.test_head_bucket_raw(&bucket_a).is_err());
    assert!(bridge_node.test_head_bucket_raw(&bucket_b).is_err());

    let buckets = cluster.list_buckets_for_owner(owner.as_str()).unwrap();
    assert_eq!(
        buckets
            .iter()
            .map(|bucket| &bucket.name)
            .collect::<Vec<_>>(),
        vec![&bucket_a, &bucket_b]
    );
}

#[test]
fn create_bucket_command_applies_to_all_acting_pg_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "replicated-create-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let created = cluster
        .create_bucket_with_config_and_load_info_raw(&crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: true,
            public_write: false,
            versioning: crate::BucketVersioningState::Enabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
    let receipt = match created {
        crate::BucketCreateAttemptOutcome::Created(receipt) => receipt,
        crate::BucketCreateAttemptOutcome::Exists(_) => {
            panic!("fresh bucket unexpectedly existed")
        }
    };
    let created = {
        let pg = map
            .node(node_ids[0])
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert_eq!(receipt.name(), &created.name);
    assert_eq!(
        receipt.bucket_execution_generation(),
        created.bucket_execution_generation
    );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.name, created.name);
        assert_eq!(info.owner_principal, created.owner_principal);
        assert_eq!(info.owner_canonical_id, created.owner_canonical_id);
        assert_eq!(info.created_at, created.created_at);
        assert_eq!(info.versioning, created.versioning);
        assert_eq!(info.acl_grants, created.acl_grants);
        assert_eq!(info.public_read, created.public_read);
        assert_eq!(info.public_write, created.public_write);
        assert_eq!(
            info.bucket_execution_generation,
            created.bucket_execution_generation
        );
    }
    assert_bucket_execution_counter_on_acting_nodes(
        &map,
        &node_ids,
        1,
        created.bucket_execution_generation,
    );

    let exists = cluster
        .create_bucket_with_config_and_load_info_raw(&crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: true,
            public_write: false,
            versioning: crate::BucketVersioningState::Enabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
    assert!(matches!(
        exists,
        crate::BucketCreateAttemptOutcome::Exists(info) if info.name == bucket
    ));
}

#[test]
fn create_bucket_command_retry_reuses_pending_partial_replica_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-create-retry-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CreateBucket(create)
                    if create.bucket().name == hook_bucket
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));
    let create_config = || crate::CreateBucketConfig {
        name: bucket.as_str(),
        owner_principal: "owner",
        owner_canonical_id: &owner,
        acl_grants: &acl_grants,
        public_read: true,
        public_write: false,
        versioning: crate::BucketVersioningState::Enabled,
        object_lock: crate::BucketObjectLockConfig::default(),
        ownership_controls: crate::BucketOwnershipControls {
            object_ownership: crate::BucketObjectOwnership::ObjectWriter,
        },
    };

    let published = cluster
        .create_bucket_with_config_and_load_info_raw(&create_config())
        .unwrap();
    assert!(matches!(
        published,
        crate::BucketCreateAttemptOutcome::Created(info) if info.name() == &bucket
    ));
    assert!(!fail_once.load(Ordering::SeqCst));
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(1).unwrap();
        let slot = pg
            .pending_metadata_command_slot(NodeId::new(1).as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .expect("partial create bucket should leave durable primary pending slot");
        assert_eq!(slot.scope_bucket.as_ref(), Some(&bucket));
    }

    let partial_info = {
        let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = applied_replica.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    {
        let node_id = NodeId::new(1);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(
                info.created_at, partial_info.created_at,
                "primary-first apply should create bucket on node {node_id:?} before the replica failure"
            );
    }
    {
        let node_id = NodeId::new(2);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        assert!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).is_err(),
            "node {node_id:?} should not have the partially applied bucket"
        );
    }

    let retried = cluster
        .create_bucket_with_config_and_load_info_raw(&create_config())
        .unwrap();
    assert!(matches!(
        retried,
        crate::BucketCreateAttemptOutcome::Created(receipt)
            if receipt.name() == &bucket
                && receipt.bucket_execution_generation()
                    == partial_info.bucket_execution_generation
    ));

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.name, partial_info.name);
        assert_eq!(info.owner_principal, partial_info.owner_principal);
        assert_eq!(info.owner_canonical_id, partial_info.owner_canonical_id);
        assert_eq!(info.created_at, partial_info.created_at);
        assert_eq!(info.state, partial_info.state);
        assert_eq!(info.versioning, partial_info.versioning);
        assert_eq!(info.object_lock, partial_info.object_lock);
        assert_eq!(info.acl_grants, partial_info.acl_grants);
        assert_eq!(info.public_read, partial_info.public_read);
        assert_eq!(info.public_write, partial_info.public_write);
        assert_eq!(
            info.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );
    }
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(1).unwrap();
        assert!(pg
            .pending_metadata_command_slot(NodeId::new(1).as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .is_none());
    }
}

#[test]
fn newly_installed_create_bucket_does_not_reconstruct_recovered_progress() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let pg_id = PgId::new(1);
    let bucket = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, pg_id.get(), "authoritative-create-")
    };
    set_route_primary(&mut map, pg_id.get(), NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let _serial = lock_metadata_command_apply_hook_test();
    let reconstruction_called = Arc::new(AtomicBool::new(false));
    let reconstruction_called_for_hook = Arc::clone(&reconstruction_called);
    let hook_bucket = bucket.clone();
    let _hook = cluster.test_install_metadata_command_progress_reconstruction_hook(Arc::new(
        move |command| {
            if command.bucket_name() == &hook_bucket {
                reconstruction_called_for_hook.store(true, Ordering::SeqCst);
                return Err(StoreError::StorageRpc {
                    node_id: 2,
                    operation: "injected recovered progress inspection",
                    failure: crate::storage_rpc::StorageRpcErrorCode::PayloadDecode,
                    detail: crate::StorageNodeFailureDetail::new(
                        "newly installed command must not reconstruct recovered progress",
                    ),
                });
            }
            Ok(())
        },
    ));
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let created = cluster
        .create_bucket_with_config_and_load_info_raw(&crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();

    assert!(matches!(
        created,
        crate::BucketCreateAttemptOutcome::Created(info) if info.name() == &bucket
    ));
    assert!(!reconstruction_called.load(Ordering::SeqCst));
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn newly_installed_bucket_command_reconstructs_progress_after_recovery_timeout_and_wait() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let pg_id = PgId::new(1);
    let bucket = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, pg_id.get(), "adopted-create-")
    };
    set_route_primary(&mut map, pg_id.get(), NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let command = create_bucket_metadata_command(pg_id, 1, bucket.clone());
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    let MetadataCommandRecoveryAdmission::Leader(owner) = map
        .runtime_state()
        .join_metadata_command_recovery(pg_id, &command)
    else {
        panic!("injected recovery owner must lead the command flight");
    };

    let _serial = lock_metadata_command_apply_hook_test();
    let reconstruction_called = Arc::new(AtomicBool::new(false));
    let reconstruction_called_for_hook = Arc::clone(&reconstruction_called);
    let _hook =
        cluster.test_install_metadata_command_progress_reconstruction_hook(Arc::new(move |_| {
            reconstruction_called_for_hook.store(true, Ordering::SeqCst);
            Ok(())
        }));
    let timeout_selected = Arc::new(Barrier::new(2));
    let retry_selected = Arc::new(Barrier::new(2));
    cluster.test_install_metadata_command_recovery_wait_hook(
        pg_id,
        &command,
        Arc::clone(&timeout_selected),
        Arc::clone(&retry_selected),
    );
    let waiter_cluster = cluster.clone();
    let waiter_bucket = bucket.clone();
    let waiter_command = command.clone();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let waiter = thread::spawn(move || {
        result_tx
            .send(
                waiter_cluster.finish_pending_metadata_command_to_acting_set(
                    pg_id,
                    &waiter_bucket,
                    &waiter_command,
                    true,
                ),
            )
            .unwrap();
    });
    timeout_selected.wait();
    retry_selected.wait();
    drop(owner);

    assert_eq!(
        result_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("fresh finisher did not adopt the released recovery flight")
            .unwrap(),
        PendingMetadataCommandOutcome::Applied
    );
    waiter.join().unwrap();
    assert!(
        reconstruction_called.load(Ordering::SeqCst),
        "recovery timeout and wait must downgrade authoritative progress provenance"
    );
    assert_eq!(
        cluster.test_take_metadata_command_recovery_wait_hook_observation(),
        (2, 0),
        "fresh finisher must time out once, then wait for the existing recovery owner"
    );
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn create_bucket_retries_transient_abandonment_observation_for_unmarked_pending_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let mut map =
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], EcShape { k: 2, m: 1 }).unwrap();
    let pg_id = PgId::new(1);
    let bucket = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, pg_id.get(), "create-abandonment-observation-")
    };
    set_route_primary(&mut map, pg_id.get(), NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let command = create_bucket_metadata_command(pg_id, 1, bucket.clone());
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let hook_calls = Arc::new(AtomicUsize::new(0));
    let hook_calls_for_hook = Arc::clone(&hook_calls);
    let hook_bucket = bucket.clone();
    let _hook = cluster.test_install_before_metadata_command_abandoned_log_inspection_hook(
        Arc::new(move |command| {
            if command.bucket_name() == &hook_bucket
                && hook_calls_for_hook.fetch_add(1, Ordering::SeqCst) == 0
            {
                return Err(StoreError::StorageRpc {
                    node_id: 2,
                    operation: "injected abandonment observation",
                    failure: crate::storage_rpc::StorageRpcErrorCode::TransportClosed,
                    detail: crate::StorageNodeFailureDetail::new(
                        "transient response loss while inspecting unmarked command",
                    ),
                }
                .into());
            }
            Ok(())
        }),
    );
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let result = cluster
        .create_bucket_with_config_and_load_info_raw(&crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();

    assert!(matches!(
        result,
        crate::BucketCreateAttemptOutcome::Created(receipt) if receipt.name() == &bucket
    ));
    assert!(hook_calls.load(Ordering::SeqCst) >= 2);
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn create_bucket_retries_partial_exact_command_conflict() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-create-exact-conflict-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let _serial = lock_metadata_command_apply_hook_test();
    let applied_by_hook = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let applied_by_hook_guard = Arc::clone(&applied_by_hook);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CreateBucket(create)
                    if create.bucket().name == hook_bucket
                        && node_id == NodeId::new(2)
                        && !applied_by_hook_guard.swap(true, Ordering::SeqCst) =>
                {
                    let node = hook_map.node(NodeId::new(2)).unwrap().storage_node();
                    let pg = node.get_pg(1)?;
                    pg.apply_metadata_command_and_record(NodeId::new(2).as_u32(), command)
                        .map_err(|error| match error {
                            crate::BucketSnapshotLoadError::Store(error) => error,
                            crate::BucketSnapshotLoadError::Metadata(error) => {
                                panic!("manual create bucket command apply failed: {error}")
                            }
                        })?;
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let created = cluster
        .create_bucket_with_config_and_load_info_raw(&crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
    assert!(matches!(
        created,
        crate::BucketCreateAttemptOutcome::Created(info) if info.name() == &bucket
    ));
    assert!(applied_by_hook.load(Ordering::SeqCst));

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.name, bucket);
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn create_bucket_retries_partial_exact_command_conflict_on_first_replica() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-create-first-exact-conflict-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let _serial = lock_metadata_command_apply_hook_test();
    let applied_by_hook = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let applied_by_hook_guard = Arc::clone(&applied_by_hook);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CreateBucket(create)
                    if create.bucket().name == hook_bucket
                        && node_id == NodeId::new(0)
                        && !applied_by_hook_guard.swap(true, Ordering::SeqCst) =>
                {
                    let node = hook_map.node(NodeId::new(0)).unwrap().storage_node();
                    let pg = node.get_pg(1)?;
                    pg.apply_metadata_command_and_record(NodeId::new(0).as_u32(), command)
                        .map_err(|error| match error {
                            crate::BucketSnapshotLoadError::Store(error) => error,
                            crate::BucketSnapshotLoadError::Metadata(error) => {
                                panic!("manual create bucket command apply failed: {error}")
                            }
                        })?;
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let created = cluster
        .create_bucket_with_config_and_load_info_raw(&crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
    assert!(matches!(
        created,
        crate::BucketCreateAttemptOutcome::Created(info) if info.name() == &bucket
    ));
    assert!(applied_by_hook.load(Ordering::SeqCst));

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.name, bucket);
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn create_bucket_drains_different_bucket_pending_command_on_same_pg() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let topology = map
        .nodes
        .get(&NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "partial-create-first-");
    let second_bucket = bucket_for_pg(topology, 1, "partial-create-second-");
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let fail_once_hook = Arc::clone(&fail_once);
    let first_bucket_for_hook = first_bucket.clone();
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CreateBucket(create)
                    if create.bucket().name == first_bucket_for_hook
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected partial create bucket failure",
                        source: std::io::Error::other("injected partial create bucket failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let published = cluster
        .create_bucket_with_config_and_load_info_raw(&crate::CreateBucketConfig {
            name: first_bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
    assert!(matches!(
        published,
        crate::BucketCreateAttemptOutcome::Created(info) if info.name() == &first_bucket
    ));
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &first_bucket).is_some());
    drop(hook_guard);

    let second = cluster
        .create_bucket_with_config_and_load_info_raw(&crate::CreateBucketConfig {
            name: second_bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
    assert!(matches!(
        second,
        crate::BucketCreateAttemptOutcome::Created(info) if info.name() == &second_bucket
    ));
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &second_bucket).is_none());

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let first = crate::PgMetadataStore::head_bucket_raw(&*pg, &first_bucket).unwrap();
        let second = crate::PgMetadataStore::head_bucket_raw(&*pg, &second_bucket).unwrap();
        assert_eq!(first.name, first_bucket);
        assert_eq!(second.name, second_bucket);
    }
}

#[test]
fn put_bucket_versioning_command_applies_to_all_acting_pg_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "replicated-versioning-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let original = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        primary.test_head_bucket_raw(&bucket).unwrap()
    };

    let updated = cluster
        .put_bucket_versioning_and_load_info_raw(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert_eq!(updated.versioning, crate::BucketVersioningState::Enabled);
    assert!(updated.bucket_execution_generation > original.bucket_execution_generation);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.versioning, updated.versioning);
        assert_eq!(
            info.bucket_execution_generation,
            updated.bucket_execution_generation
        );
    }
}

#[test]
fn put_bucket_versioning_retries_abandoned_command_with_slot_cleanup_deferred() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "abandoned-versioning-cleanup-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let _serial = lock_metadata_command_apply_hook_test();
    let captured = Arc::new(Mutex::new(None));
    let hook_bucket = bucket.clone();
    let captured_hook = Arc::clone(&captured);
    let apply_hook = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::PutBucketVersioning(versioning)
                    if versioning.bucket.name == hook_bucket
            ) && node_id == NodeId::new(0)
            {
                *captured_hook.lock().unwrap() = Some(command.clone());
                return Err(StoreError::Io {
                    context: "injected zero-apply bucket versioning failure",
                    source: std::io::Error::other("injected zero-apply bucket versioning failure"),
                });
            }
            Ok(())
        },
    ));

    let err = cluster
        .put_bucket_versioning_and_load_info_raw(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::Io {
            context: "injected zero-apply bucket versioning failure",
            ..
        })
    ));
    drop(apply_hook);
    let abandoned = captured
        .lock()
        .unwrap()
        .take()
        .expect("versioning apply hook should capture the abandoned command");
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none());
    force_insert_pending_metadata_command_for_test(&map, PgId::new(1), &bucket, &abandoned);

    let checksum = abandoned.checksum_crc64();
    let defer_once = Arc::new(AtomicBool::new(true));
    let defer_once_hook = Arc::clone(&defer_once);
    let cleanup_hook = cluster.test_install_global_metadata_command_terminal_slot_removal_hook(
        Arc::new(move |command| {
            command.checksum_crc64() == checksum && defer_once_hook.swap(false, Ordering::SeqCst)
        }),
    );

    let updated = cluster
        .put_bucket_versioning_and_load_info_raw(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert_eq!(updated.versioning, crate::BucketVersioningState::Enabled);
    assert!(!defer_once.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none());
    drop(cleanup_hook);

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .versioning,
            crate::BucketVersioningState::Enabled
        );
    }
}

#[test]
fn bucket_terminal_cleanup_publishes_handoff_before_blocked_slot_removal() {
    struct CleanupRelease(Option<std::sync::mpsc::SyncSender<()>>);

    impl CleanupRelease {
        fn release(&mut self) {
            if let Some(release) = self.0.take() {
                let _ = release.send(());
            }
        }
    }

    impl Drop for CleanupRelease {
        fn drop(&mut self) {
            self.release();
        }
    }

    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let mut map =
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], EcShape { k: 2, m: 1 }).unwrap();
    let pg_id = PgId::new(1);
    let bucket = bucket_for_pg(
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology(),
        pg_id.get(),
        "bucket-cleanup-handoff-",
    );
    set_route_primary(&mut map, pg_id.get(), NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let (cleanup_reached_tx, cleanup_reached_rx) = std::sync::mpsc::sync_channel(1);
    let (cleanup_release_tx, cleanup_release_rx) = std::sync::mpsc::sync_channel(1);
    let cleanup_release_rx = Arc::new(Mutex::new(cleanup_release_rx));
    let cleanup_release_rx_for_hook = Arc::clone(&cleanup_release_rx);
    let hook_bucket = bucket.clone();
    let cleanup_reached = Arc::new(AtomicBool::new(false));
    let cleanup_reached_for_hook = Arc::clone(&cleanup_reached);
    let cleanup_hook = cluster.test_install_global_metadata_command_terminal_slot_removal_hook(
        Arc::new(move |command| {
            if !matches!(
                command.payload(),
                MetadataCommandPayload::PutBucketVersioning(versioning)
                    if versioning.bucket.name == hook_bucket
            ) || cleanup_reached_for_hook.swap(true, Ordering::SeqCst)
            {
                return false;
            }
            cleanup_reached_tx.send(()).unwrap();
            cleanup_release_rx_for_hook
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .recv_timeout(Duration::from_secs(5))
                .expect("bucket terminal cleanup gate was not released");
            true
        }),
    );

    let (owner_result, waiter_error, command) = thread::scope(|scope| {
        let mut cleanup_release = CleanupRelease(Some(cleanup_release_tx));
        let (owner_tx, owner_rx) = std::sync::mpsc::sync_channel(1);
        let owner_cluster = &cluster;
        let owner_bucket = &bucket;
        scope.spawn(move || {
            owner_tx
                .send(owner_cluster.put_bucket_versioning_and_load_info_raw(
                    owner_bucket,
                    crate::BucketVersioningState::Enabled,
                ))
                .unwrap();
        });
        cleanup_reached_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("bucket command did not reach terminal cleanup");
        let command = pending_metadata_command_for_test(&map, pg_id, &bucket)
            .expect("blocked terminal cleanup must retain the bucket command");

        let (waiter_tx, waiter_rx) = std::sync::mpsc::sync_channel(1);
        let waiter_cluster = &cluster;
        let waiter_bucket = &bucket;
        let waiter_command = command.clone();
        scope.spawn(move || {
            waiter_tx
                .send(waiter_cluster.drain_bucket_pg_pending_metadata_command(
                    pg_id,
                    waiter_bucket,
                    &waiter_command,
                    false,
                ))
                .unwrap();
        });
        let waiter_error = waiter_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("unrelated bucket waiter remained blocked by terminal cleanup")
            .unwrap_err();
        cleanup_release.release();
        let owner_result = owner_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("bucket command owner did not finish after cleanup release");
        (owner_result, waiter_error, command)
    });

    assert_eq!(
        owner_result.unwrap().versioning,
        crate::BucketVersioningState::Enabled
    );
    assert!(matches!(
        waiter_error,
        crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandContention { .. })
    ));
    assert!(cleanup_reached.load(Ordering::SeqCst));
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &bucket),
        Some(command.clone())
    );
    assert!(cluster.test_metadata_command_recovery_awaiting_authorized(pg_id, &command));

    drop(cleanup_hook);
    assert_eq!(
        cluster
            .drain_pending_metadata_command_with_authorized_recovery_route(
                pg_id, &command, &cluster,
            )
            .unwrap(),
        PendingMetadataCommandOutcome::Applied
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
}

#[test]
fn put_bucket_versioning_command_retry_reuses_pending_partial_replica_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-versioning-retry-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketVersioning(versioning)
                    if versioning.bucket.name == hook_bucket
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let published = cluster
        .put_bucket_versioning_and_load_info_raw(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert_eq!(published.versioning, crate::BucketVersioningState::Enabled);
    assert!(!fail_once.load(Ordering::SeqCst));
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(1).unwrap();
        let slot = pg
            .pending_metadata_command_slot(NodeId::new(1).as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .expect("partial bucket versioning should leave durable primary pending slot");
        assert_eq!(slot.scope_bucket.as_ref(), Some(&bucket));
    }

    let partial_info = {
        let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = applied_replica.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert_eq!(
        partial_info.versioning,
        crate::BucketVersioningState::Enabled
    );
    {
        let node_id = NodeId::new(1);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .versioning,
            crate::BucketVersioningState::Enabled,
            "primary-first apply should update node {node_id:?} before the replica failure"
        );
    }
    {
        let node_id = NodeId::new(2);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .versioning,
            crate::BucketVersioningState::Disabled,
            "node {node_id:?} should not have the partially applied versioning update"
        );
    }

    let retried = cluster
        .put_bucket_versioning_and_load_info_raw(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert_eq!(retried.versioning, crate::BucketVersioningState::Enabled);
    assert_eq!(
        retried.bucket_execution_generation,
        partial_info.bucket_execution_generation
    );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.versioning, crate::BucketVersioningState::Enabled);
        assert_eq!(
            info.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );
    }
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(1).unwrap();
        assert!(pg
            .pending_metadata_command_slot(NodeId::new(1).as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .is_none());
    }
}

#[test]
fn same_bucket_pending_metadata_command_drains_before_later_acl() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "pending-stream-acl-block-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketVersioning(versioning)
                    if versioning.bucket.name == hook_bucket
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let published = cluster
        .put_bucket_versioning_and_load_info_raw(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert_eq!(published.versioning, crate::BucketVersioningState::Enabled);
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));

    let pending = pending_metadata_command_for_test(&map, PgId::new(1), &bucket)
        .expect("failed versioning command should remain pending");
    assert!(matches!(
        pending.payload(),
        MetadataCommandPayload::PutBucketVersioning(versioning)
            if versioning.bucket.name == bucket
                && versioning.bucket.versioning == crate::BucketVersioningState::Enabled
    ));
    let partial_info = {
        let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = applied_replica.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert_eq!(
        partial_info.versioning,
        crate::BucketVersioningState::Enabled
    );

    let acl_grants = crate::AclGrants::default();
    let acl_updated = cluster
        .put_bucket_acl_and_load_info_raw(
            &bucket,
            &acl_grants,
            BucketAclSummary {
                public_read: true,
                public_write: false,
            },
        )
        .unwrap();
    assert_eq!(
        acl_updated.versioning,
        crate::BucketVersioningState::Enabled
    );
    assert!(acl_updated.public_read);
    assert!(!acl_updated.public_write);
    assert!(
            acl_updated.bucket_execution_generation > partial_info.bucket_execution_generation,
            "later ACL command must reserve a newer execution generation after draining the pending versioning command"
        );
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.versioning, crate::BucketVersioningState::Enabled);
        assert!(info.public_read);
        assert!(!info.public_write);
        assert_eq!(
            info.bucket_execution_generation,
            acl_updated.bucket_execution_generation
        );
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn bucket_update_cleans_terminal_pending_slot_before_new_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "terminal-pending-next-op-");
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket.clone());
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(0), &command)
        .unwrap();
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_some(),
        "test setup should leave a terminal durable pending slot"
    );

    let updated = cluster
        .put_bucket_versioning_and_load_info_raw(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();

    assert_eq!(updated.versioning, crate::BucketVersioningState::Enabled);
    assert!(updated.bucket_execution_generation > 1);
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_none(),
        "later bucket operation should clean the terminal slot before publishing its command"
    );
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.versioning, crate::BucketVersioningState::Enabled);
        assert_eq!(
            info.bucket_execution_generation,
            updated.bucket_execution_generation
        );
    }
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn put_bucket_acl_command_applies_to_all_acting_pg_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "replicated-acl-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let original = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        primary.test_head_bucket_raw(&bucket).unwrap()
    };
    let acl_grants = crate::AclGrants::default();

    let updated = cluster
        .put_bucket_acl_and_load_info_raw(
            &bucket,
            &acl_grants,
            BucketAclSummary {
                public_read: true,
                public_write: false,
            },
        )
        .unwrap();
    assert_eq!(updated.acl_grants, acl_grants);
    assert!(updated.public_read);
    assert!(!updated.public_write);
    assert!(updated.bucket_execution_generation > original.bucket_execution_generation);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.acl_grants, updated.acl_grants);
        assert_eq!(info.public_read, updated.public_read);
        assert_eq!(info.public_write, updated.public_write);
        assert_eq!(
            info.bucket_execution_generation,
            updated.bucket_execution_generation
        );
    }
}

#[test]
fn put_bucket_acl_command_retry_reuses_pending_partial_replica_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-acl-retry-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let acl_grants = crate::AclGrants::default();
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketAcl(acl)
                    if acl.bucket.name == hook_bucket
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let published = cluster
        .put_bucket_acl_and_load_info_raw(
            &bucket,
            &acl_grants,
            BucketAclSummary {
                public_read: true,
                public_write: false,
            },
        )
        .unwrap();
    assert!(published.public_read);
    assert!(!published.public_write);
    assert!(!fail_once.load(Ordering::SeqCst));

    let partial_info = {
        let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = applied_replica.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert!(partial_info.public_read);
    assert!(!partial_info.public_write);
    {
        let node_id = NodeId::new(1);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(
            info.public_read && !info.public_write,
            "primary-first apply should update node {node_id:?} before the replica failure"
        );
    }
    {
        let node_id = NodeId::new(2);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(!info.public_read);
        assert!(
            !info.public_write,
            "node {node_id:?} should not have the partially applied ACL update"
        );
    }

    let retried = cluster
        .put_bucket_acl_and_load_info_raw(
            &bucket,
            &acl_grants,
            BucketAclSummary {
                public_read: true,
                public_write: false,
            },
        )
        .unwrap();
    assert!(retried.public_read);
    assert!(!retried.public_write);
    assert_eq!(
        retried.bucket_execution_generation,
        partial_info.bucket_execution_generation
    );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.acl_grants, acl_grants);
        assert!(info.public_read);
        assert!(!info.public_write);
        assert_eq!(
            info.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );
    }
}

#[test]
fn different_bucket_puts_drain_published_pending_command_before_new_generation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "different-bucket-put-pending-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_versioning_trailing = Arc::new(AtomicBool::new(true));
    let first_acl_grants = crate::AclGrants::new(vec![AclGrant::new(
        AclGrantee::CanonicalUser(crate::CanonicalUserId::from_principal("first-acl-grantee")),
        AclPermission::FullControl,
    )]);
    let replacement_acl_grants = crate::AclGrants::new(vec![AclGrant::new(
        AclGrantee::CanonicalUser(crate::CanonicalUserId::from_principal(
            "replacement-acl-grantee",
        )),
        AclPermission::FullControl,
    )]);
    let fail_acl_trailing = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_first_acl_grants = first_acl_grants.clone();
    let fail_versioning_hook = Arc::clone(&fail_versioning_trailing);
    let fail_acl_hook = Arc::clone(&fail_acl_trailing);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            let should_fail = match command.payload() {
                MetadataCommandPayload::PutBucketVersioning(versioning) => {
                    versioning.bucket.name == hook_bucket
                        && versioning.bucket.versioning == crate::BucketVersioningState::Enabled
                        && node_id == NodeId::new(2)
                        && fail_versioning_hook.load(Ordering::SeqCst)
                }
                MetadataCommandPayload::PutBucketAcl(acl) => {
                    acl.bucket.name == hook_bucket
                        && acl.bucket.acl_grants == hook_first_acl_grants
                        && node_id == NodeId::new(2)
                        && fail_acl_hook.load(Ordering::SeqCst)
                }
                _ => false,
            };
            if should_fail {
                return Err(StoreError::MetadataCommandContention {
                    context: "injected trailing bucket PUT contention",
                });
            }
            Ok(())
        },
    ));

    let enabled = cluster
        .put_bucket_versioning_and_load_info_raw(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert_eq!(enabled.versioning, crate::BucketVersioningState::Enabled);
    assert!(matches!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket)
            .expect("published versioning command should remain pending")
            .payload(),
        MetadataCommandPayload::PutBucketVersioning(versioning)
            if versioning.bucket.versioning == crate::BucketVersioningState::Enabled
    ));

    fail_versioning_trailing.store(false, Ordering::SeqCst);
    let suspended = cluster
        .put_bucket_versioning_and_load_info_raw(&bucket, crate::BucketVersioningState::Suspended)
        .unwrap();
    assert_eq!(
        suspended.versioning,
        crate::BucketVersioningState::Suspended
    );
    assert!(suspended.bucket_execution_generation > enabled.bucket_execution_generation);
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none());

    let first_acl = cluster
        .put_bucket_acl_and_load_info_raw(
            &bucket,
            &first_acl_grants,
            BucketAclSummary {
                public_read: false,
                public_write: false,
            },
        )
        .unwrap();
    assert_eq!(first_acl.acl_grants, first_acl_grants);
    assert!(!first_acl.public_read);
    assert!(!first_acl.public_write);
    assert!(matches!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket)
            .expect("published ACL command should remain pending")
            .payload(),
        MetadataCommandPayload::PutBucketAcl(acl)
            if acl.bucket.acl_grants == first_acl_grants
    ));

    fail_acl_trailing.store(false, Ordering::SeqCst);
    let replacement_acl = cluster
        .put_bucket_acl_and_load_info_raw(
            &bucket,
            &replacement_acl_grants,
            BucketAclSummary {
                public_read: false,
                public_write: false,
            },
        )
        .unwrap();
    assert_eq!(replacement_acl.acl_grants, replacement_acl_grants);
    assert!(!replacement_acl.public_read);
    assert!(!replacement_acl.public_write);
    assert!(replacement_acl.bucket_execution_generation > first_acl.bucket_execution_generation);
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none());

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.versioning, crate::BucketVersioningState::Suspended);
        assert_eq!(info.acl_grants, replacement_acl_grants);
        assert!(!info.public_read);
        assert!(!info.public_write);
        assert_eq!(
            info.bucket_execution_generation,
            replacement_acl.bucket_execution_generation
        );
    }
}

#[test]
fn bucket_acl_drains_pending_multipart_completion_barrier_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "acl-drains-mpu-sequence-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(1);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            crate::ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::AdvanceMultipartCompletionBarrier(
            AdvanceMultipartCompletionBarrierCommand {
                bucket: bucket.clone(),
                barrier_sequence: 7,
            },
        ),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let _serial = lock_metadata_command_apply_hook_test();
    let hook_bucket = bucket.clone();
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::AdvanceMultipartCompletionBarrier(advance)
                    if advance.bucket == hook_bucket && node_id == NodeId::new(2) =>
                {
                    return Err(StoreError::StorageRpc {
                        node_id: node_id.as_u32(),
                        operation: "injected multipart completion barrier sequence apply failure",
                        failure: crate::storage_rpc::StorageRpcErrorCode::TransportTimeout,
                        detail: crate::StorageNodeFailureDetail::new(
                            "injected multipart completion barrier sequence apply failure"
                                .to_owned(),
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .expect("witness and primary publication should hand trailing convergence to recovery");
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_some(),
        "published command must retain its exact recovery slot"
    );
    let trailing = map.node(NodeId::new(2)).unwrap().storage_node();
    assert_eq!(
        crate::traits::PgMetadataStore::head_bucket_record_raw(
            &*trailing.get_pg(pg_id.get()).unwrap(),
            &bucket,
        )
        .unwrap()
        .multipart_completion_barrier_sequence,
        0,
        "the injected trailing failure must leave replica convergence pending"
    );
    drop(hook_guard);

    let acl_grants = crate::AclGrants::default();
    let updated = cluster
        .put_bucket_acl_and_load_info_raw(
            &bucket,
            &acl_grants,
            BucketAclSummary {
                public_read: true,
                public_write: false,
            },
        )
        .unwrap();
    assert!(updated.public_read);
    assert!(!updated.public_write);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::traits::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.acl_grants, updated.acl_grants);
        assert!(info.public_read);
        assert!(!info.public_write);
        assert_eq!(info.multipart_completion_barrier_sequence, 7);
    }
}

#[test]
fn existing_create_bucket_preserves_pending_acl_command_for_retry() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-acl-create-exists-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let acl_grants = crate::AclGrants::default();
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketAcl(acl)
                    if acl.bucket.name == hook_bucket
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let published = cluster
        .put_bucket_acl_and_load_info_raw(
            &bucket,
            &acl_grants,
            BucketAclSummary {
                public_read: true,
                public_write: false,
            },
        )
        .unwrap();
    assert!(published.public_read);
    assert!(!published.public_write);
    assert!(!fail_once.load(Ordering::SeqCst));

    let pending_before = pending_metadata_command_for_test(&map, PgId::new(1), &bucket)
        .expect("failed ACL command should remain pending");
    assert!(matches!(
        pending_before.payload(),
        MetadataCommandPayload::PutBucketAcl(acl)
            if acl.bucket.name == bucket && acl.bucket.public_read && !acl.bucket.public_write
    ));
    let partial_info = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert!(partial_info.public_read);
    assert!(!partial_info.public_write);
    let failed_replica_info = {
        let primary = map.node(NodeId::new(2)).unwrap().storage_node();
        let pg = primary.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert!(!failed_replica_info.public_read);
    assert!(!failed_replica_info.public_write);

    let attacker_owner = crate::CanonicalUserId::from_principal("attacker");
    let exists = cluster
        .create_bucket_with_config_and_load_info_raw(&crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "attacker",
            owner_canonical_id: &attacker_owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
    assert!(matches!(
        exists,
        crate::BucketCreateAttemptOutcome::Exists(info)
            if info.owner_principal == "owner"
                && info.owner_canonical_id
                    == crate::CanonicalUserId::from_principal("owner")
    ));

    assert!(
            pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none(),
            "existing CreateBucket should drain and apply the pending ACL command before returning Exists"
        );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(info.public_read);
        assert!(!info.public_write);
        assert_eq!(
            info.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );
    }
}

#[test]
fn bucket_acl_retry_rejects_same_acl_with_mismatched_post_image() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "acl-post-image-conflict-")
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let acl_grants = crate::AclGrants::default();
    let current = {
        let node = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket).unwrap()
    };
    let mut update = PutBucketAclCommand::from_bucket(
        current.with_execution_generation(77),
        acl_grants.clone(),
        BucketAclSummary {
            public_read: true,
            public_write: false,
        },
    );
    update.bucket.bucket_policy_public = true;
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            PgId::new(1),
            MetadataCommandLogIndex::new(77).unwrap(),
        ),
        MetadataCommandPayload::PutBucketAcl(update),
    );
    insert_pending_metadata_command_for_test(&map, PgId::new(1), &bucket, &command);

    let err = cluster
        .put_bucket_acl_and_load_info_raw(
            &bucket,
            &acl_grants,
            BucketAclSummary {
                public_read: true,
                public_write: false,
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandContention {
                context: "conflicting pending put bucket acl command",
            })
        ),
        "expected conflicting pending ACL command, got {err:?}"
    );
    let info = cluster.head_bucket_info(&bucket).unwrap();
    assert!(!info.public_read);
    assert!(!info.bucket_policy_public);
}

#[test]
fn bucket_property_commands_apply_to_all_acting_pg_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "replicated-property-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let mut previous_generation = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        primary.test_head_bucket_raw(&bucket).unwrap()
    }
    .bucket_execution_generation;
    let updated = cluster
        .put_bucket_versioning_and_load_info_raw(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;

    let object_lock = crate::BucketObjectLockConfig {
        enabled: true,
        default_retention: Some(crate::ObjectLockDefaultRetention {
            mode: crate::ObjectLockMode::Governance,
            period: crate::RetentionPeriod::days(3).unwrap(),
        }),
    };
    let updated = cluster
        .put_bucket_object_lock_and_load_info_raw(&bucket, object_lock)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.object_lock, object_lock);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let encryption = crate::BucketEncryptionConfig {
        default_encryption: Some(crate::ManagedEncryptionAlgorithm::Aes256),
        sse_c_blocked: false,
    };
    let updated = cluster
        .put_bucket_encryption_and_load_info_raw(&bucket, encryption)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.encryption, encryption.effective());
        assert_eq!(
            crate::PgMetadataStore::get_bucket_encryption(&*pg, &bucket).unwrap(),
            encryption
        );
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let public_access_block = crate::PublicAccessBlockConfig {
        block_public_acls: true,
        ignore_public_acls: false,
        block_public_policy: true,
        restrict_public_buckets: false,
    };
    let updated = cluster
        .put_bucket_public_access_block_and_load_info_raw(&bucket, public_access_block)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.public_access_block, Some(public_access_block));
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let updated = cluster
        .delete_bucket_public_access_block_and_load_info_raw(&bucket)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.public_access_block, None);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let ownership_controls = crate::BucketOwnershipControls {
        object_ownership: crate::BucketObjectOwnership::BucketOwnerPreferred,
    };
    let updated = cluster
        .put_bucket_ownership_controls_and_load_info_raw(&bucket, ownership_controls)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.ownership_controls, Some(ownership_controls));
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let updated = cluster
        .delete_bucket_ownership_controls_and_load_info_raw(&bucket)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.ownership_controls, None);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let updated = cluster
        .put_bucket_abac_enabled_and_load_info_raw(&bucket, true)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(info.bucket_abac_enabled);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }
}

#[test]
fn bucket_property_command_retry_reuses_pending_partial_replica_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-property-retry-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let public_access_block = crate::PublicAccessBlockConfig {
        block_public_acls: true,
        ignore_public_acls: true,
        block_public_policy: false,
        restrict_public_buckets: true,
    };
    let expected_mutation = BucketPropertyMutation::PublicAccessBlock(Some(public_access_block));
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketProperty(property)
                    if property.bucket.name == hook_bucket
                        && property.effect == expected_mutation.effect()
                        && property.bucket.public_access_block == Some(public_access_block)
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let published = cluster
        .put_bucket_public_access_block_and_load_info_raw(&bucket, public_access_block)
        .unwrap();
    assert_eq!(published.public_access_block, Some(public_access_block));
    assert!(!fail_once.load(Ordering::SeqCst));

    let partial_info = {
        let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = applied_replica.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert_eq!(partial_info.public_access_block, Some(public_access_block));
    {
        let node_id = NodeId::new(1);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .public_access_block,
            Some(public_access_block),
            "primary-first apply should update node {node_id:?} before the replica failure"
        );
    }
    {
        let node_id = NodeId::new(2);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .public_access_block,
            None,
            "node {node_id:?} should not have the partially applied property update"
        );
    }

    let retried = cluster
        .put_bucket_public_access_block_and_load_info_raw(&bucket, public_access_block)
        .unwrap();
    assert_eq!(retried.public_access_block, Some(public_access_block));
    assert_eq!(
        retried.bucket_execution_generation,
        partial_info.bucket_execution_generation
    );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.public_access_block, Some(public_access_block));
        assert_eq!(
            info.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );
    }
}

#[test]
fn bucket_property_delete_does_not_adopt_pending_put_after_publication() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "property-delete-pending-put-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let public_access_block = crate::PublicAccessBlockConfig {
        block_public_acls: true,
        ignore_public_acls: true,
        block_public_policy: false,
        restrict_public_buckets: true,
    };

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_trailing = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_trailing_hook = Arc::clone(&fail_trailing);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::PutBucketProperty(property)
                    if property.bucket.name == hook_bucket
                        && property.bucket.public_access_block == Some(public_access_block)
                        && node_id == NodeId::new(2)
                        && fail_trailing_hook.load(Ordering::SeqCst)
            ) {
                return Err(StoreError::MetadataCommandContention {
                    context: "injected trailing bucket-property contention",
                });
            }
            Ok(())
        },
    ));

    let published = cluster
        .put_bucket_public_access_block_and_load_info_raw(&bucket, public_access_block)
        .unwrap();
    assert_eq!(published.public_access_block, Some(public_access_block));
    let pending = pending_metadata_command_for_test(&map, PgId::new(1), &bucket)
        .expect("published property command should remain pending for trailing recovery");
    assert!(matches!(
        pending.payload(),
        MetadataCommandPayload::PutBucketProperty(property)
            if property.bucket.public_access_block == Some(public_access_block)
    ));

    fail_trailing.store(false, Ordering::SeqCst);
    drop(hook_guard);
    let deleted = cluster
        .delete_bucket_public_access_block_and_load_info_raw(&bucket)
        .unwrap();
    assert_eq!(deleted.public_access_block, None);
    assert!(
        deleted.bucket_execution_generation > published.bucket_execution_generation,
        "DELETE must publish a new property command rather than return the pending PUT receipt"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .public_access_block,
            None,
            "node {node_id:?} retained the completed PUT property"
        );
    }
}

#[test]
fn invalid_bucket_property_command_does_not_poison_bucket_command_stream() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "invalid-property-no-poison-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let initial_generation = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        primary.test_head_bucket_raw(&bucket).unwrap()
    }
    .bucket_execution_generation;

    let invalid_object_lock = crate::BucketObjectLockConfig {
        enabled: true,
        default_retention: None,
    };
    let err = cluster
        .put_bucket_object_lock_and_load_info_raw(&bucket, invalid_object_lock)
        .unwrap_err();
    match err {
        crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::InvariantViolation {
            context: "put bucket object lock",
            reason,
        }) if reason == "bucket object lock requires enabled versioning" => {}
        other => panic!("expected object-lock storage validation error, got {other:?}"),
    }
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none(),
        "deterministic validation failures must not leave pending commands"
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.object_lock, crate::BucketObjectLockConfig::default());
        assert_eq!(info.bucket_execution_generation, initial_generation);
    }

    let public_access_block = crate::PublicAccessBlockConfig {
        block_public_acls: true,
        ignore_public_acls: false,
        block_public_policy: true,
        restrict_public_buckets: false,
    };
    let updated = cluster
        .put_bucket_public_access_block_and_load_info_raw(&bucket, public_access_block)
        .unwrap();
    assert_eq!(updated.public_access_block, Some(public_access_block));
    assert!(updated.bucket_execution_generation > initial_generation);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.public_access_block, Some(public_access_block));
        assert_eq!(
            info.bucket_execution_generation,
            updated.bucket_execution_generation
        );
    }
}

#[test]
fn bucket_subresource_commands_apply_to_all_acting_pg_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "replicated-subresource-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let mut previous_generation = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        primary.test_head_bucket_raw(&bucket).unwrap()
    }
    .bucket_execution_generation;

    let policy_body = r#"{"Statement":[]}"#;
    let updated = cluster
        .put_bucket_subresource_and_load_info_raw(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Policy,
                body: policy_body,
                aux: crate::BucketSubresourceAux::policy(true),
            },
        )
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        let stored = crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Policy,
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored.body, policy_body);
        assert_eq!(stored.generation, Some(1));
        assert_eq!(stored.aux, crate::BucketSubresourceAux::policy(true));
        assert!(info.bucket_policy_present);
        assert!(info.bucket_policy_public);
        assert_eq!(info.bucket_policy_generation, 1);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let tags_body = "<Tagging><TagSet/></Tagging>";
    let tags = crate::SerializedBucketTagSet::new(tags_body.to_string());
    let updated = cluster
        .put_bucket_subresource_and_load_info_raw(
            &bucket,
            crate::PutBucketSubresource::tagging(&tags),
        )
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        let stored = crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Tagging,
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored.body, tags.as_str());
        assert_eq!(stored.generation, Some(1));
        assert_eq!(stored.aux, crate::BucketSubresourceAux::None);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let updated = cluster
        .delete_bucket_tags_and_load_info_raw(&bucket)
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Tagging,
        )
        .unwrap()
        .is_none());
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let lifecycle_body = "<LifecycleConfiguration/>";
    let updated = cluster
        .put_bucket_subresource_and_load_info_raw(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Lifecycle,
                body: lifecycle_body,
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        let stored = crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Lifecycle,
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored.body, lifecycle_body);
        assert_eq!(stored.generation, Some(1));
        assert!(info.bucket_lifecycle_present);
        assert_eq!(info.bucket_lifecycle_generation, 1);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let cors_body = "<CORSConfiguration/>";
    let updated = cluster
        .put_bucket_subresource_and_load_info_raw(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Cors,
                body: cors_body,
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        let stored = crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Cors,
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored.body, cors_body);
        assert_eq!(stored.generation, Some(1));
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let updated = cluster
        .delete_bucket_subresource_and_load_info_raw(
            &bucket,
            crate::OpaqueBucketSubresourceKind::Cors,
        )
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Cors,
        )
        .unwrap()
        .is_none());
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let updated = cluster
        .delete_bucket_subresource_and_load_info_raw(
            &bucket,
            crate::OpaqueBucketSubresourceKind::Policy,
        )
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Policy,
        )
        .unwrap()
        .is_none());
        assert!(!info.bucket_policy_present);
        assert!(!info.bucket_policy_public);
        assert_eq!(info.bucket_policy_generation, 2);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }

    let updated = cluster
        .delete_bucket_subresource_and_load_info_raw(
            &bucket,
            crate::OpaqueBucketSubresourceKind::Lifecycle,
        )
        .unwrap();
    assert!(updated.bucket_execution_generation > previous_generation);
    previous_generation = updated.bucket_execution_generation;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Lifecycle,
        )
        .unwrap()
        .is_none());
        assert!(!info.bucket_lifecycle_present);
        assert_eq!(info.bucket_lifecycle_generation, 2);
        assert_eq!(info.bucket_execution_generation, previous_generation);
    }
}

#[test]
fn bucket_subresource_command_retry_reuses_pending_partial_replica_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-subresource-retry-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let policy_body = r#"{"Statement":[]}"#;
    let expected_mutation = BucketSubresourceMutation::PutPolicy {
        body: policy_body.to_owned(),
        is_public: false,
    };
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketSubresource(subresource)
                    if subresource.name == hook_bucket
                        && subresource.mutation == expected_mutation
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let published = cluster
        .put_bucket_subresource_and_load_info_raw(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Policy,
                body: policy_body,
                aux: crate::BucketSubresourceAux::policy(false),
            },
        )
        .unwrap();
    assert!(published.bucket_policy_present);
    assert!(!fail_once.load(Ordering::SeqCst));

    let partial_info = {
        let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = applied_replica.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap()
    };
    assert!(partial_info.bucket_policy_present);
    assert!(!partial_info.bucket_policy_public);
    assert_eq!(partial_info.bucket_policy_generation, 1);
    {
        let node_id = NodeId::new(1);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(
            info.bucket_policy_present,
            "primary-first apply should update node {node_id:?} before the replica failure"
        );
    }
    {
        let node_id = NodeId::new(2);
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert!(
            !info.bucket_policy_present,
            "node {node_id:?} should not have the partially applied policy"
        );
    }

    let retried = cluster
        .put_bucket_subresource_and_load_info_raw(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Policy,
                body: policy_body,
                aux: crate::BucketSubresourceAux::policy(false),
            },
        )
        .unwrap();
    assert!(retried.bucket_policy_present);
    assert!(!retried.bucket_policy_public);
    assert_eq!(
        retried.bucket_execution_generation,
        partial_info.bucket_execution_generation
    );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        let stored = crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Policy,
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored.body, policy_body);
        assert_eq!(stored.generation, Some(1));
        assert_eq!(stored.aux, crate::BucketSubresourceAux::policy(false));
        assert!(info.bucket_policy_present);
        assert!(!info.bucket_policy_public);
        assert_eq!(info.bucket_policy_generation, 1);
        assert_eq!(
            info.bucket_execution_generation,
            partial_info.bucket_execution_generation
        );
    }
}

#[test]
fn bucket_subresource_retains_handoff_when_unrelated_pending_command_is_irrevocable() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let mut map =
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], EcShape { k: 2, m: 1 }).unwrap();
    let pg_id = PgId::new(1);
    let (bucket, deleting_bucket) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, pg_id.get(), "subresource-unrelated-drain-"),
            bucket_for_pg(topology, pg_id.get(), "subresource-unrelated-delete-"),
        )
    };
    set_route_primary(&mut map, pg_id.get(), NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    create_test_bucket(&cluster, &deleting_bucket);

    let initial_generation = cluster
        .head_bucket_info(&bucket)
        .unwrap()
        .bucket_execution_generation;
    let command_id = cluster.next_bucket_metadata_command_id(pg_id).unwrap();
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let primary_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let deleting =
        crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &deleting_bucket).unwrap();
    let command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
            deleting.with_execution_generation(
                primary_pg
                    .next_bucket_execution_generation_candidate()
                    .unwrap(),
            ),
        )),
    );
    drop(primary_pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &deleting_bucket, &command);

    let _serial = lock_metadata_command_apply_hook_test();
    let hook_calls = Arc::new(AtomicUsize::new(0));
    let hook_calls_for_hook = Arc::clone(&hook_calls);
    let hook_command = command.clone();
    let _hook =
        cluster.test_install_metadata_command_apply_attempt_hook(Arc::new(move |candidate| {
            if candidate == &hook_command {
                hook_calls_for_hook.fetch_add(1, Ordering::SeqCst);
                let id = candidate.id();
                return Err(StoreError::MetadataCommandIrrevocableConvergencePending {
                    pg_id: id.pg_id().get(),
                    cluster_epoch: id.cluster_epoch(),
                    log_index: id.log_index().get(),
                });
            }
            Ok(())
        }));

    let tags = crate::SerializedBucketTagSet::new(
        "<Tagging><TagSet><Tag><Key>key</Key><Value>value</Value></Tag></TagSet></Tagging>"
            .to_string(),
    );
    let started = std::time::Instant::now();
    let error = cluster
        .put_bucket_subresource_and_load_info(&bucket, crate::PutBucketSubresource::tagging(&tags))
        .expect_err("an unrelated irrevocable command must block with retryable contention");

    assert!(
        started.elapsed() < Duration::from_secs(5),
        "unrelated bucket-command handoff retained the request for {:?}",
        started.elapsed()
    );
    assert_eq!(
        error.kind(),
        &crate::BucketSnapshotLoadFailureKind::MetadataCommandContention
    );
    assert_eq!(hook_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &deleting_bucket),
        Some(command.clone()),
        "the unrelated uncertain command must remain available for recovery"
    );
    assert_eq!(cluster.test_metadata_command_recovery_flight_count(), 1);
    assert!(cluster.test_metadata_command_recovery_awaiting_authorized(pg_id, &command,));

    let retry_started = std::time::Instant::now();
    let retry_error = cluster
        .put_bucket_subresource_and_load_info(&bucket, crate::PutBucketSubresource::tagging(&tags))
        .expect_err("an already-transferred unrelated command must remain retryable contention");
    assert!(
        retry_started.elapsed() < Duration::from_secs(5),
        "existing authorized-recovery handoff retained a later request for {:?}",
        retry_started.elapsed()
    );
    assert_eq!(
        retry_error.kind(),
        &crate::BucketSnapshotLoadFailureKind::MetadataCommandContention
    );
    assert_eq!(
        hook_calls.load(Ordering::SeqCst),
        1,
        "a later unrelated caller must not re-enter command application"
    );
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.bucket_execution_generation, initial_generation);
        assert!(
            crate::PgMetadataStore::get_bucket_subresource(
                &*pg,
                &bucket,
                crate::BucketSubresourceKind::Tagging,
            )
            .unwrap()
            .is_none(),
            "the blocked request must not mutate its target bucket"
        );
    }
}

#[test]
fn bucket_subresource_transfers_published_unrelated_command_to_recovery() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let mut map =
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], EcShape { k: 2, m: 1 }).unwrap();
    let pg_id = PgId::new(1);
    let (bucket, barrier_bucket) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, pg_id.get(), "subresource-published-drain-"),
            bucket_for_pg(topology, pg_id.get(), "subresource-published-barrier-"),
        )
    };
    set_route_primary(&mut map, pg_id.get(), NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    create_test_bucket(&cluster, &barrier_bucket);

    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::AdvanceMultipartCompletionBarrier(
            AdvanceMultipartCompletionBarrierCommand {
                bucket: barrier_bucket.clone(),
                barrier_sequence: 11,
            },
        ),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &barrier_bucket, &command);

    let _serial = lock_metadata_command_apply_hook_test();
    let trailing_failures = Arc::new(AtomicUsize::new(0));
    let trailing_failures_for_hook = Arc::clone(&trailing_failures);
    let hook_command = command.clone();
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, candidate| {
            if node_id == NodeId::new(2) && candidate == &hook_command {
                trailing_failures_for_hook.fetch_add(1, Ordering::SeqCst);
                return Err(StoreError::StorageRpc {
                    node_id: node_id.as_u32(),
                    operation: "injected published bucket-command trailing failure",
                    failure: crate::storage_rpc::StorageRpcErrorCode::TransportTimeout,
                    detail: crate::StorageNodeFailureDetail::new(
                        "injected published bucket-command trailing failure".to_owned(),
                    ),
                });
            }
            Ok(())
        },
    ));
    let tags = crate::SerializedBucketTagSet::new(
        "<Tagging><TagSet><Tag><Key>key</Key><Value>value</Value></Tag></TagSet></Tagging>"
            .to_string(),
    );

    let started = std::time::Instant::now();
    let error = cluster
        .put_bucket_subresource_and_load_info(&bucket, crate::PutBucketSubresource::tagging(&tags))
        .expect_err("published unrelated command must transfer to authorized recovery");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "published unrelated bucket command retained the request for {:?}",
        started.elapsed()
    );
    assert_eq!(
        error.kind(),
        &crate::BucketSnapshotLoadFailureKind::MetadataCommandContention
    );
    assert_eq!(trailing_failures.load(Ordering::SeqCst), 1);
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &barrier_bucket),
        Some(command.clone())
    );
    assert!(cluster.test_metadata_command_recovery_awaiting_authorized(pg_id, &command));
    for node_id in [NodeId::new(0), NodeId::new(1)] {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_record_raw(&*pg, &barrier_bucket)
                .unwrap()
                .multipart_completion_barrier_sequence,
            11
        );
    }
    let trailing_pg = map
        .node(NodeId::new(2))
        .unwrap()
        .storage_node()
        .get_pg(pg_id.get())
        .unwrap();
    assert_eq!(
        crate::PgMetadataStore::head_bucket_record_raw(&*trailing_pg, &barrier_bucket)
            .unwrap()
            .multipart_completion_barrier_sequence,
        0
    );
    drop(trailing_pg);

    let retry_started = std::time::Instant::now();
    let retry_error = cluster
        .put_bucket_subresource_and_load_info(&bucket, crate::PutBucketSubresource::tagging(&tags))
        .expect_err("later unrelated caller must observe the retained recovery handoff");
    assert!(retry_started.elapsed() < Duration::from_secs(5));
    assert_eq!(
        retry_error.kind(),
        &crate::BucketSnapshotLoadFailureKind::MetadataCommandContention
    );
    assert_eq!(trailing_failures.load(Ordering::SeqCst), 1);

    let mut multipart_budget =
        crate::cluster::RequestWorkBudget::new(Duration::from_secs(10), None)
            .for_operation("test_published_bucket_command_multipart_barrier")
            .for_pg(pg_id);
    let multipart_started = std::time::Instant::now();
    let multipart_error = cluster
        .test_finish_pending_command_for_multipart_completion_barrier(
            pg_id,
            &command,
            &mut multipart_budget,
        )
        .expect_err("multipart barrier dispatch must not accept pending recovery as complete");
    assert!(multipart_started.elapsed() < Duration::from_secs(5));
    assert!(matches!(
        multipart_error,
        crate::ObjectPgActionError::Store(StoreError::MetadataCommandContention { .. })
    ));
    assert_eq!(trailing_failures.load(Ordering::SeqCst), 1);

    drop(hook_guard);
    assert_eq!(
        cluster
            .drain_pending_metadata_command_with_authorized_recovery_route(
                pg_id, &command, &cluster,
            )
            .unwrap(),
        PendingMetadataCommandOutcome::Applied
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &barrier_bucket).is_none());
}

#[test]
fn bucket_subresource_waiter_transfers_exact_owner_published_command_to_recovery() {
    #[derive(Default)]
    struct OwnerApplyGate {
        state: Mutex<(bool, bool)>,
        changed: Condvar,
    }

    impl OwnerApplyGate {
        fn block_until_released(&self) {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.0 = true;
            self.changed.notify_all();
            while !state.1 {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(|error| error.into_inner());
            }
        }

        fn wait_until_arrived(&self, timeout: Duration) {
            let deadline = Instant::now() + timeout;
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            while !state.0 {
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .expect("exact owner did not reach the apply gate");
                let (next, wait) = self
                    .changed
                    .wait_timeout(state, remaining)
                    .unwrap_or_else(|error| error.into_inner());
                state = next;
                assert!(
                    !wait.timed_out() || state.0,
                    "exact owner did not reach the apply gate"
                );
            }
        }

        fn release(&self) {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.1 = true;
            self.changed.notify_all();
        }

        fn release_on_drop(self: &Arc<Self>) -> OwnerApplyGateRelease {
            OwnerApplyGateRelease(Arc::clone(self))
        }
    }

    struct OwnerApplyGateRelease(Arc<OwnerApplyGate>);

    impl OwnerApplyGateRelease {
        fn release(&self) {
            self.0.release();
        }
    }

    impl Drop for OwnerApplyGateRelease {
        fn drop(&mut self) {
            self.release();
        }
    }

    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let mut map =
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], EcShape { k: 2, m: 1 }).unwrap();
    let pg_id = PgId::new(1);
    let (bucket, barrier_bucket) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, pg_id.get(), "subresource-published-waiter-"),
            bucket_for_pg(topology, pg_id.get(), "subresource-published-owner-"),
        )
    };
    set_route_primary(&mut map, pg_id.get(), NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    create_test_bucket(&cluster, &barrier_bucket);

    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::AdvanceMultipartCompletionBarrier(
            AdvanceMultipartCompletionBarrierCommand {
                bucket: barrier_bucket.clone(),
                barrier_sequence: 17,
            },
        ),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &barrier_bucket, &command);

    let _serial = lock_metadata_command_apply_hook_test();
    let owner_apply_gate = Arc::new(OwnerApplyGate::default());
    let owner_apply_gate_for_hook = Arc::clone(&owner_apply_gate);
    let owner_blocked = Arc::new(AtomicBool::new(false));
    let owner_blocked_for_hook = Arc::clone(&owner_blocked);
    let trailing_failures = Arc::new(AtomicUsize::new(0));
    let trailing_failures_for_hook = Arc::clone(&trailing_failures);
    let hook_command = command.clone();
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, candidate| {
            if candidate != &hook_command {
                return Ok(());
            }
            if !owner_blocked_for_hook.swap(true, Ordering::SeqCst) {
                owner_apply_gate_for_hook.block_until_released();
            }
            if node_id == NodeId::new(2) {
                trailing_failures_for_hook.fetch_add(1, Ordering::SeqCst);
                return Err(StoreError::StorageRpc {
                    node_id: node_id.as_u32(),
                    operation: "injected exact-owner trailing bucket-command failure",
                    failure: crate::storage_rpc::StorageRpcErrorCode::TransportTimeout,
                    detail: crate::StorageNodeFailureDetail::new(
                        "injected exact-owner trailing bucket-command failure".to_owned(),
                    ),
                });
            }
            Ok(())
        },
    ));
    let tags = crate::SerializedBucketTagSet::new(
        "<Tagging><TagSet><Tag><Key>key</Key><Value>value</Value></Tag></TagSet></Tagging>"
            .to_string(),
    );

    let (owner_result, waiter_result) = thread::scope(|scope| {
        let owner_release = owner_apply_gate.release_on_drop();
        let (owner_result_tx, owner_result_rx) = std::sync::mpsc::sync_channel(1);
        let owner_cluster = &cluster;
        let owner_bucket = &barrier_bucket;
        let owner_command = &command;
        scope.spawn(move || {
            owner_result_tx
                .send(owner_cluster.finish_pending_metadata_command_to_acting_set(
                    pg_id,
                    owner_bucket,
                    owner_command,
                    false,
                ))
                .unwrap();
        });
        owner_apply_gate.wait_until_arrived(Duration::from_secs(5));
        let (waiter_result_tx, waiter_result_rx) = std::sync::mpsc::sync_channel(1);
        let waiter_cluster = &cluster;
        let waiter_bucket = &bucket;
        let waiter_tags = &tags;
        scope.spawn(move || {
            waiter_result_tx
                .send(waiter_cluster.put_bucket_subresource_and_load_info(
                    waiter_bucket,
                    crate::PutBucketSubresource::tagging(waiter_tags),
                ))
                .unwrap();
        });
        let waiter_selection_deadline = Instant::now() + Duration::from_secs(5);
        while !map
            .runtime_state()
            .test_metadata_command_recovery_handoff_requested(pg_id, &command)
        {
            assert!(
                Instant::now() < waiter_selection_deadline,
                "unrelated caller did not select the exact owner's recovery flight"
            );
            thread::sleep(Duration::from_millis(1));
        }
        owner_release.release();
        (
            owner_result_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("exact owner did not finish after gate release"),
            waiter_result_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("unrelated waiter did not finish after publication handoff"),
        )
    });

    assert_eq!(
        owner_result.unwrap(),
        PendingMetadataCommandOutcome::PublishedPendingRecovery,
        "the exact owner must retain its committed-success projection"
    );
    assert_eq!(
        waiter_result
            .expect_err("the waiting unrelated caller must receive retryable contention")
            .kind(),
        &crate::BucketSnapshotLoadFailureKind::MetadataCommandContention
    );
    assert!(owner_blocked.load(Ordering::SeqCst));
    assert_eq!(trailing_failures.load(Ordering::SeqCst), 1);
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &barrier_bucket),
        Some(command.clone())
    );
    assert!(cluster.test_metadata_command_recovery_awaiting_authorized(pg_id, &command));

    drop(hook_guard);
    assert_eq!(
        cluster
            .drain_pending_metadata_command_with_authorized_recovery_route(
                pg_id, &command, &cluster,
            )
            .unwrap(),
        PendingMetadataCommandOutcome::Applied
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &barrier_bucket).is_none());
}

#[test]
fn create_bucket_reports_contention_when_same_bucket_delete_mark_outcome_is_unconfirmed() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let mut map =
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], EcShape { k: 2, m: 1 }).unwrap();
    let pg_id = PgId::new(1);
    let bucket = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, pg_id.get(), "create-same-bucket-delete-drain-")
    };
    set_route_primary(&mut map, pg_id.get(), NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let command_id = cluster.next_bucket_metadata_command_id(pg_id).unwrap();
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let primary_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let current = crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &bucket).unwrap();
    let command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
            current.with_execution_generation(
                primary_pg
                    .next_bucket_execution_generation_candidate()
                    .unwrap(),
            ),
        )),
    );
    drop(primary_pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let _serial = lock_metadata_command_apply_hook_test();
    let hook_calls = Arc::new(AtomicUsize::new(0));
    let hook_calls_for_hook = Arc::clone(&hook_calls);
    let hook_command = command.clone();
    let _hook =
        cluster.test_install_metadata_command_apply_attempt_hook(Arc::new(move |candidate| {
            if candidate == &hook_command {
                hook_calls_for_hook.fetch_add(1, Ordering::SeqCst);
                let id = candidate.id();
                return Err(StoreError::MetadataCommandOutcomeUnconfirmed {
                    pg_id: id.pg_id().get(),
                    cluster_epoch: id.cluster_epoch(),
                    log_index: id.log_index().get(),
                });
            }
            Ok(())
        }));

    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let error = cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .expect_err("same-bucket delete-mark uncertainty must be retryable contention");

    assert_eq!(
        error.kind(),
        &crate::BucketSnapshotLoadFailureKind::MetadataCommandContention
    );
    assert_eq!(hook_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &bucket),
        Some(command.clone()),
        "the uncertain delete mark must remain available for recovery"
    );
    assert_eq!(cluster.test_metadata_command_recovery_flight_count(), 1);
    assert!(cluster.test_metadata_command_recovery_awaiting_authorized(pg_id, &command,));
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .state,
            crate::BucketState::Active,
            "the blocked CreateBucket request must not apply the delete mark"
        );
    }
}

#[test]
fn invalid_bucket_subresource_command_does_not_poison_bucket_command_stream() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "invalid-subresource-no-poison-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let initial_generation = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        primary.test_head_bucket_raw(&bucket).unwrap()
    }
    .bucket_execution_generation;

    let err = cluster
        .put_bucket_subresource_and_load_info_raw(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Tagging,
                body: "<Tagging/>",
                aux: crate::BucketSubresourceAux::policy(true),
            },
        )
        .unwrap_err();
    match err {
        crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::InvariantViolation {
            context: "put bucket subresource",
            reason,
        }) if reason.contains("Tagging does not support aux") => {}
        other => panic!("expected subresource storage validation error, got {other:?}"),
    }
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none(),
        "deterministic validation failures must not leave pending commands"
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.bucket_execution_generation, initial_generation);
        assert!(crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Tagging,
        )
        .unwrap()
        .is_none());
    }

    let tags_body = "<Tagging><TagSet/></Tagging>";
    let tags = crate::SerializedBucketTagSet::new(tags_body.to_string());
    let updated = cluster
        .put_bucket_subresource_and_load_info_raw(
            &bucket,
            crate::PutBucketSubresource::tagging(&tags),
        )
        .unwrap();
    assert!(updated.bucket_execution_generation > initial_generation);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        let stored = crate::PgMetadataStore::get_bucket_subresource(
            &*pg,
            &bucket,
            crate::BucketSubresourceKind::Tagging,
        )
        .unwrap()
        .unwrap();
        assert_eq!(stored.body, tags.as_str());
        assert_eq!(
            info.bucket_execution_generation,
            updated.bucket_execution_generation
        );
    }
}
