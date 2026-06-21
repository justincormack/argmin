use super::*;

#[test]
fn storage_cluster_opens_local_node_map() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let cluster = crate::StorageCluster::open_local_nodes(
        tmp.path(),
        &node_ids,
        &[0, 1],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();

    assert_eq!(cluster.cluster_epoch(), ClusterEpoch::INITIAL);
    assert_eq!(cluster.operation_epoch(), ClusterEpoch::INITIAL);
    assert_eq!(cluster.metadata_node_id(), NodeId::new(0));
    assert_eq!(cluster.local_node_count(), 6);
    assert_eq!(cluster.local_node_ids().collect::<Vec<_>>(), node_ids);
    let routes = cluster.local_pg_routes().collect::<Vec<_>>();
    assert_eq!(routes.len(), 2);
    assert_eq!(
        cluster.local_pg_route(PgId::new(1)).unwrap().acting_set(),
        node_ids
    );
}

#[test]
fn places_payload_shards_deterministically_on_distinct_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
        NodeId::new(6),
        NodeId::new(7),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let cluster =
        crate::StorageCluster::open_local_nodes(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape)
            .unwrap();
    let data_pg_id = DataPgId::new(crate::PgId::new(3));

    let first = cluster
        .place_payload_shards(data_pg_id, ec_shape, b"stable-payload-key")
        .unwrap();
    let second = cluster
        .place_payload_shards(data_pg_id, ec_shape, b"stable-payload-key")
        .unwrap();

    assert_eq!(first, second);
    assert_eq!(first.len(), usize::from(ec_shape.k + ec_shape.m));
    for (expected_index, location) in first.iter().enumerate() {
        assert_eq!(location.cluster_epoch(), ClusterEpoch::INITIAL);
        assert_eq!(location.data_pg_id(), data_pg_id);
        assert_eq!(
            location.shard_index(),
            ShardIndex::new(expected_index as u8)
        );
        assert!(
            cluster
                .local_node_ids()
                .any(|node_id| node_id == location.node_id()),
            "placed shard on unknown node {:?}",
            location.node_id()
        );
    }
    let distinct_nodes: BTreeSet<NodeId> = first.iter().map(ShardLocation::node_id).collect();
    assert_eq!(distinct_nodes.len(), first.len());
}

#[test]
fn places_payload_shards_for_explicit_pg_route_without_current_route_lookup() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
        NodeId::new(6),
        NodeId::new(7),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let cluster =
        crate::StorageCluster::open_local_nodes(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape)
            .unwrap();
    let data_pg_id = DataPgId::new(crate::PgId::new(3));
    let placement_key = b"historical-placement-key";

    let current = cluster
        .place_payload_shards(data_pg_id, ec_shape, placement_key)
        .unwrap();
    let explicit_current = cluster
        .place_payload_shards_for_pg_route(
            ClusterEpoch::INITIAL,
            data_pg_id,
            ec_shape,
            placement_key,
            &node_ids,
        )
        .unwrap();

    assert_eq!(explicit_current, current);

    let historical_epoch = ClusterEpoch::new(7).unwrap();
    let historical_acting_set = &node_ids[..usize::from(ec_shape.k + ec_shape.m)];
    let historical = cluster
        .place_payload_shards_for_pg_route(
            historical_epoch,
            data_pg_id,
            ec_shape,
            placement_key,
            historical_acting_set,
        )
        .unwrap();

    assert_eq!(historical.len(), usize::from(ec_shape.k + ec_shape.m));
    assert!(historical
        .iter()
        .all(|location| location.cluster_epoch() == historical_epoch));
    assert!(historical
        .iter()
        .all(|location| historical_acting_set.contains(&location.node_id())));
    assert_eq!(
        historical
            .iter()
            .map(ShardLocation::node_id)
            .collect::<BTreeSet<_>>()
            .len(),
        historical.len()
    );
}

#[test]
fn payload_shard_node_selects_one_placed_shard() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let data_pg_id = DataPgId::new(crate::PgId::new(1));
    let locations = map
        .place_payload_shards(
            ClusterEpoch::INITIAL,
            data_pg_id,
            ec_shape,
            b"stable-payload-key",
        )
        .unwrap();

    let selected = map
        .payload_shard_node(
            ClusterEpoch::INITIAL,
            data_pg_id,
            ShardIndex::new(2),
            ec_shape,
            b"stable-payload-key",
        )
        .unwrap();

    assert_eq!(selected, locations[2].node_id());
}

#[test]
fn payload_shard_node_rejects_index_outside_ec_shape() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let err = map
        .payload_shard_node(
            ClusterEpoch::INITIAL,
            DataPgId::new(crate::PgId::new(0)),
            ShardIndex::new(ec_shape.k + ec_shape.m),
            ec_shape,
            b"stable-payload-key",
        )
        .unwrap_err();

    assert!(matches!(
        err,
        ClusterBuildError::InvalidShardIndex {
            data_shards: 4,
            parity_shards: 2,
            shard_index: 6,
        }
    ));
}

#[test]
fn place_payload_shards_rejects_unknown_pg() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let map = LocalClusterMap::open(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();

    let err = map
        .place_payload_shards(
            ClusterEpoch::INITIAL,
            DataPgId::new(PgId::new(99)),
            SharedStorageNode::DEFAULT_EC_SHAPE,
            b"stable-payload-key",
        )
        .unwrap_err();

    assert!(matches!(
        err,
        ClusterBuildError::PgNotFound {
            pg_id: 99,
            cluster_epoch: ClusterEpoch::INITIAL,
        }
    ));
}

#[test]
fn place_payload_shards_rejects_stale_operation_epoch() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let map = LocalClusterMap::open(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();

    let err = map
        .place_payload_shards(
            ClusterEpoch::new(2).unwrap(),
            DataPgId::new(PgId::new(0)),
            SharedStorageNode::DEFAULT_EC_SHAPE,
            b"stable-payload-key",
        )
        .unwrap_err();

    assert!(matches!(
        err,
        ClusterBuildError::StalePayloadPlacement {
            pg_id: 0,
            operation_epoch,
            current_epoch,
        } if operation_epoch == ClusterEpoch::new(2).unwrap()
            && current_epoch == ClusterEpoch::INITIAL
    ));
}

#[test]
fn expired_route_maps_reject_payload_placement_and_shard_io() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
    let data_pg_id = DataPgId::new(PgId::new(0));
    map.route_map_valid_until_ms = Some(crate::clock::current_time_millis().saturating_add(60_000));

    let location = map
        .place_payload_shards(ClusterEpoch::INITIAL, data_pg_id, ec_shape, b"payload-key")
        .unwrap()[0];
    let key = ShardKey::new(&[61; 16], 1, location.shard_index().get());
    let ack = map
        .write_payload_shard(ClusterEpoch::INITIAL, location, &key, b"payload")
        .unwrap();
    assert_eq!(
        map.read_payload_shard(ClusterEpoch::INITIAL, location, &key, ack)
            .unwrap(),
        b"payload"
    );

    let expired_at = crate::clock::current_time_millis();
    map.route_map_valid_until_ms = Some(expired_at);
    let err = map
        .place_payload_shards(ClusterEpoch::INITIAL, data_pg_id, ec_shape, b"payload-key")
        .unwrap_err();
    assert!(matches!(
        err,
        ClusterBuildError::RouteMapExpired {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            valid_until_ms,
            now_ms,
        } if valid_until_ms == expired_at && now_ms >= expired_at
    ));

    let err = map
        .write_payload_shard(ClusterEpoch::INITIAL, location, &key, b"blocked")
        .unwrap_err();
    assert!(matches!(
        err,
        ShardIoError::RouteMapExpired {
            node_id,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            valid_until_ms,
            now_ms,
        } if node_id == location.node_id().as_u32()
            && valid_until_ms == expired_at
            && now_ms >= expired_at
    ));

    let err = map
        .read_payload_shard(ClusterEpoch::INITIAL, location, &key, ack)
        .unwrap_err();
    assert!(matches!(
        err,
        ShardIoError::RouteMapExpired {
            node_id,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            valid_until_ms,
            now_ms,
        } if node_id == location.node_id().as_u32()
            && valid_until_ms == expired_at
            && now_ms >= expired_at
    ));

    let err = map
        .delete_payload_shard(ClusterEpoch::INITIAL, location, &key)
        .unwrap_err();
    assert!(matches!(
        err,
        ShardIoError::RouteMapExpired {
            node_id,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            valid_until_ms,
            now_ms,
        } if node_id == location.node_id().as_u32()
            && valid_until_ms == expired_at
            && now_ms >= expired_at
    ));
}

#[test]
fn storage_cluster_dispatches_payload_shard_io_to_placed_local_node() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let data_pg_id = DataPgId::new(crate::PgId::new(1));
    let location = cluster
        .place_payload_shards(data_pg_id, ec_shape, b"stable-payload-key")
        .unwrap()[3];
    let key = ShardKey::new(&[17; 16], 23, location.shard_index().get());

    let ack = cluster
        .write_payload_shard(location, &key, b"placed shard")
        .unwrap();

    assert_eq!(ack.stored_size, b"placed shard".len() as u64);
    assert_eq!(ack.crc64, checksum::crc64::checksum(b"placed shard"));
    assert_eq!(
        cluster.read_payload_shard(location, &key, ack).unwrap(),
        b"placed shard"
    );
    let mut dst = vec![0; b"placed shard".len()];
    cluster
        .read_payload_shard_into(location, &key, ack, &mut dst)
        .unwrap();
    assert_eq!(dst, b"placed shard");
    assert!(matches!(
        cluster.read_payload_shard(
            location,
            &key,
            WriteAck {
                crc64: ack.crc64 ^ 1,
                stored_size: ack.stored_size,
            },
        ),
        Err(ShardIoError::Store {
            source: StoreError::IntegrityError { .. },
            ..
        })
    ));

    let assigned_node = map.node(location.node_id()).unwrap();
    assert_eq!(
        assigned_node
            .storage_node()
            .read_shard_file(data_pg_id.get(), &key)
            .unwrap(),
        b"placed shard"
    );
    for other_node_id in node_ids {
        if other_node_id == location.node_id() {
            continue;
        }
        let other_node = map.node(other_node_id).unwrap();
        assert!(matches!(
            other_node
                .storage_node()
                .read_shard_file(data_pg_id.get(), &key),
            Err(StoreError::NotFound)
        ));
    }

    cluster.delete_payload_shard(location, &key).unwrap();
    assert!(matches!(
        cluster.read_payload_shard(location, &key, ack),
        Err(ShardIoError::Store {
            source: StoreError::NotFound,
            ..
        })
    ));
}

#[test]
fn storage_cluster_payload_write_uses_handle_operation_epoch() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&map),
        ClusterEpoch::new(2).unwrap(),
    )
    .unwrap();
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let segment_okh = [43; 16];
    let generation_id = crate::GenerationId::MIN;

    let err = stale_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            b"stale epoch payload",
        )
        .unwrap_err();

    assert!(matches!(
        err,
        StoreError::StalePayloadOperation {
            pg_id: 0,
            operation_epoch,
            current_epoch,
        } if operation_epoch == ClusterEpoch::new(2).unwrap()
            && current_epoch == ClusterEpoch::INITIAL
    ));
    for shard_index in 0..(ec_shape.k + ec_shape.m) {
        assert!(
            !current_cluster
                .test_payload_shard_file_exists(
                    0,
                    ec_shape,
                    &segment_okh,
                    generation_id,
                    shard_index,
                )
                .unwrap(),
            "stale operation epoch wrote shard {shard_index}"
        );
    }
}

#[test]
fn stale_storage_cluster_payload_read_reports_data_pg() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let mut map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
    Arc::get_mut(&mut map).unwrap().epoch = ClusterEpoch::new(2).unwrap();
    let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&map),
        ClusterEpoch::INITIAL,
    )
    .unwrap();
    let mut dst = Vec::new();

    let err = stale_cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: 0,
                segment_okh: [53; 16],
                segment_vid: crate::GenerationId::MIN,
                stored_size: 0,
                segment_crc64: 0,
                ec: ec_shape,
            },
            &mut dst,
        )
        .unwrap_err();

    assert!(matches!(
        err,
        StoreError::StalePayloadOperation {
            pg_id: 0,
            operation_epoch: ClusterEpoch::INITIAL,
            current_epoch,
        } if current_epoch == ClusterEpoch::new(2).unwrap()
    ));
    assert!(dst.is_empty());
}

#[test]
fn stale_storage_cluster_handle_cannot_use_current_epoch_location() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&map),
        ClusterEpoch::new(2).unwrap(),
    )
    .unwrap();
    let data_pg_id = DataPgId::new(PgId::new(0));
    let location = current_cluster
        .place_payload_shards(data_pg_id, ec_shape, b"current-epoch-location")
        .unwrap()[0];
    let key = ShardKey::new(&[47; 16], 1, location.shard_index().get());

    let err = stale_cluster
        .place_payload_shards(data_pg_id, ec_shape, b"stale-placement")
        .unwrap_err();

    assert!(matches!(
        err,
        ClusterBuildError::StalePayloadPlacement {
            pg_id: 0,
            operation_epoch,
            current_epoch,
        } if operation_epoch == ClusterEpoch::new(2).unwrap()
            && current_epoch == ClusterEpoch::INITIAL
    ));
    let err = stale_cluster
        .write_payload_shard(location, &key, b"must not write")
        .unwrap_err();
    assert!(matches!(
        err,
        ShardIoError::StaleOperationEpoch {
            node_id,
            pg_id: 0,
            operation_epoch,
            current_epoch,
        } if node_id == location.node_id().as_u32()
            && operation_epoch == ClusterEpoch::new(2).unwrap()
            && current_epoch == ClusterEpoch::INITIAL
    ));
    let node = map.node(location.node_id()).unwrap();
    assert!(matches!(
        node.storage_node().read_shard_file(data_pg_id.get(), &key),
        Err(StoreError::NotFound)
    ));
}

#[test]
fn stale_storage_cluster_handle_rejects_bucket_metadata_before_mutation() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&map),
        ClusterEpoch::new(2).unwrap(),
    )
    .unwrap();
    let bucket = crate::BucketName::try_from("stale-bucket".to_string()).unwrap();
    let owner_canonical_id = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let create = crate::CreateBucketConfig {
        name: bucket.as_str(),
        owner_principal: "owner",
        owner_canonical_id: &owner_canonical_id,
        acl_grants: &acl_grants,
        public_read: false,
        public_write: false,
        versioning: crate::BucketVersioningState::Disabled,
        object_lock: crate::BucketObjectLockConfig::default(),
        ownership_controls: crate::BucketOwnershipControls {
            object_ownership: crate::BucketObjectOwnership::ObjectWriter,
        },
    };

    let err = stale_cluster
        .create_bucket_with_config_and_load_info(&create)
        .unwrap_err();

    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::StaleMetadataOperation {
            pg_id: 0,
            operation_epoch,
            current_epoch,
        }) if operation_epoch == ClusterEpoch::new(2).unwrap()
            && current_epoch == ClusterEpoch::INITIAL
    ));
    let err = current_cluster.head_bucket_info(&bucket).unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::BucketNotFound { .. })
    ));
}

#[test]
fn stale_storage_cluster_handle_rejects_object_metadata_before_mutation() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&map),
        ClusterEpoch::new(2).unwrap(),
    )
    .unwrap();
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let reservation_id = crate::SessionId::try_from("02".repeat(16)).unwrap();

    let err = stale_cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap_err();

    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
            pg_id: 0,
            operation_epoch,
            current_epoch,
        }) if operation_epoch == ClusterEpoch::new(2).unwrap()
            && current_epoch == ClusterEpoch::INITIAL
    ));
    let err = current_cluster
        .test_object_generation_reservation_for(&bucket, &key, &reservation_id)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Metadata(
            crate::MetadataError::ObjectGenerationReservationNotFound { .. }
        )
    ));
}

#[test]
fn stale_storage_cluster_handle_rejects_multipart_metadata_before_lookup() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&map),
        ClusterEpoch::new(2).unwrap(),
    )
    .unwrap();
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let upload_id = crate::UploadId::try_from(".".repeat(128)).unwrap();

    let err = stale_cluster
        .load_multipart_upload(&bucket, &key, &upload_id)
        .unwrap_err();

    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::StaleMetadataOperation {
            pg_id: 0,
            operation_epoch,
            current_epoch,
        }) if operation_epoch == ClusterEpoch::new(2).unwrap()
            && current_epoch == ClusterEpoch::INITIAL
    ));
    let err = current_cluster
        .load_multipart_upload(&bucket, &key, &upload_id)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::NoSuchUpload { .. })
    ));
}

#[test]
fn object_payload_lease_token_releases_after_cluster_epoch_transition() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let mut map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let generation_id = crate::GenerationId::MIN;

    let lease = current_cluster
        .acquire_object_payload_lease(&bucket, &key, generation_id)
        .unwrap();
    assert_eq!(
        current_cluster.object_payload_lease_count(&bucket, &key, generation_id),
        1
    );
    drop(current_cluster);

    Arc::get_mut(&mut map).unwrap().epoch = ClusterEpoch::new(2).unwrap();
    let current_epoch_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&map),
        ClusterEpoch::INITIAL,
    )
    .unwrap();

    let err = match stale_cluster.acquire_object_payload_lease(&bucket, &key, generation_id) {
        Ok(_) => panic!("stale cluster handle acquired a payload lease"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        StoreError::StaleMetadataOperation {
            pg_id: 0,
            operation_epoch,
            current_epoch,
        } if operation_epoch == ClusterEpoch::INITIAL
            && current_epoch == ClusterEpoch::new(2).unwrap()
    ));
    stale_cluster.enqueue_object_payload_reclaim(&bucket, &key, generation_id);
    assert!(current_epoch_cluster.try_take_reclaim_work().is_none());

    let released = lease.release();
    assert_eq!(released.remaining(), 0);
    assert_eq!(
        current_epoch_cluster.object_payload_lease_count(&bucket, &key, generation_id),
        0
    );

    released.enqueue_object_payload_reclaim();
    assert!(matches!(
        current_epoch_cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::ObjectPayload((
            queued_bucket,
            queued_key,
            queued_generation_id
        ))) if queued_bucket == bucket
            && queued_key == key
            && queued_generation_id == generation_id
    ));
}

#[test]
fn placed_payload_shard_io_rejects_key_location_shard_index_mismatch() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let data_pg_id = DataPgId::new(crate::PgId::new(1));
    let locations = cluster
        .place_payload_shards(data_pg_id, ec_shape, b"stable-payload-key")
        .unwrap();
    let shard_0_location = locations[0];
    let shard_3_location = locations[3];
    let shard_0_key = ShardKey::new(&[31; 16], 42, shard_0_location.shard_index().get());

    let err = cluster
        .write_payload_shard(shard_3_location, &shard_0_key, b"wrong shard")
        .unwrap_err();

    assert!(matches!(
        err,
        ShardIoError::ShardIndexMismatch {
            location_shard_index: 3,
            key_shard_index: 0,
            ..
        }
    ));
    assert!(matches!(
        map.node(shard_3_location.node_id())
            .unwrap()
            .storage_node()
            .read_shard_file(data_pg_id.get(), &shard_0_key),
        Err(StoreError::NotFound)
    ));
    assert!(matches!(
        map.node(shard_0_location.node_id())
            .unwrap()
            .storage_node()
            .read_shard_file(data_pg_id.get(), &shard_0_key),
        Err(StoreError::NotFound)
    ));

    let ack = cluster
        .write_payload_shard(shard_0_location, &shard_0_key, b"right shard")
        .unwrap();
    assert!(matches!(
        cluster.read_payload_shard(shard_3_location, &shard_0_key, ack),
        Err(ShardIoError::ShardIndexMismatch {
            location_shard_index: 3,
            key_shard_index: 0,
            ..
        })
    ));
    assert!(matches!(
        cluster.delete_payload_shard(shard_3_location, &shard_0_key),
        Err(ShardIoError::ShardIndexMismatch {
            location_shard_index: 3,
            key_shard_index: 0,
            ..
        })
    ));
    assert_eq!(
        cluster
            .read_payload_shard(shard_0_location, &shard_0_key, ack)
            .unwrap(),
        b"right shard"
    );
}

#[test]
fn payload_shard_io_rejects_stale_location_epoch() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let map = LocalClusterMap::open(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let location = ShardLocation::new(
        ClusterEpoch::new(2).unwrap(),
        DataPgId::new(crate::PgId::new(0)),
        ShardIndex::new(0),
        NodeId::new(0),
    );
    let key = ShardKey::new(&[23; 16], 1, 0);

    let err = map
        .write_payload_shard(ClusterEpoch::INITIAL, location, &key, b"stale")
        .unwrap_err();

    assert!(matches!(
        err,
        ShardIoError::StaleLocation {
            location_epoch,
            current_epoch,
            ..
        } if location_epoch == ClusterEpoch::new(2).unwrap()
            && current_epoch == ClusterEpoch::INITIAL
    ));
}

#[test]
fn payload_shard_io_rejects_stale_operation_epoch_before_touching_node_store() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let map = LocalClusterMap::open(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let location = ShardLocation::new(
        ClusterEpoch::INITIAL,
        DataPgId::new(PgId::new(0)),
        ShardIndex::new(0),
        NodeId::new(0),
    );
    let key = ShardKey::new(&[41; 16], 1, 0);

    let err = map
        .write_payload_shard(ClusterEpoch::new(2).unwrap(), location, &key, b"stale op")
        .unwrap_err();

    assert!(matches!(
        err,
        ShardIoError::StaleOperationEpoch {
            node_id: 0,
            pg_id: 0,
            operation_epoch,
            current_epoch,
        } if operation_epoch == ClusterEpoch::new(2).unwrap()
            && current_epoch == ClusterEpoch::INITIAL
    ));
    let node = map.node(NodeId::new(0)).unwrap();
    assert!(matches!(
        node.storage_node().read_shard_file(0, &key),
        Err(StoreError::NotFound)
    ));
}

#[test]
fn payload_shard_io_rejects_node_outside_acting_set() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let map = LocalClusterMap::open(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let location = ShardLocation::new(
        ClusterEpoch::INITIAL,
        DataPgId::new(crate::PgId::new(0)),
        ShardIndex::new(0),
        NodeId::new(99),
    );
    let key = ShardKey::new(&[29; 16], 1, 0);

    let err = map
        .write_payload_shard(ClusterEpoch::INITIAL, location, &key, b"unknown")
        .unwrap_err();

    assert!(matches!(
        err,
        ShardIoError::NodeNotInActingSet {
            node_id: 99,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
        }
    ));
}

#[test]
fn payload_placement_and_shard_io_reject_all_non_active_pg_states() {
    let non_active_states = [
        PgState::Peering,
        PgState::Degraded,
        PgState::Backfilling,
        PgState::Inconsistent,
    ];

    for state in non_active_states {
        let tmp = test_util::tempdir();
        let node_ids = [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
            NodeId::new(5),
        ];
        let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
        map.pg_routes.get_mut(&PgId::new(0)).unwrap().state = state;
        let location = ShardLocation::new(
            ClusterEpoch::INITIAL,
            DataPgId::new(PgId::new(0)),
            ShardIndex::new(0),
            NodeId::new(0),
        );
        let key = ShardKey::new(&[47; 16], 1, 0);

        let err = map
            .place_payload_shards(
                ClusterEpoch::INITIAL,
                DataPgId::new(PgId::new(0)),
                ec_shape,
                b"non-active-placement",
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ClusterBuildError::PgNotActive {
                pg_id: 0,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: err_state,
            } if err_state == state
        ));

        let err = map
            .write_payload_shard(ClusterEpoch::INITIAL, location, &key, b"non-active")
            .unwrap_err();
        assert!(matches!(
            err,
            ShardIoError::PgNotActive {
                node_id: 0,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: err_state,
            } if err_state == state
        ));

        let err = map
            .read_payload_shard(
                ClusterEpoch::INITIAL,
                location,
                &key,
                WriteAck {
                    crc64: 0,
                    stored_size: 1,
                },
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ShardIoError::PgNotActive {
                node_id: 0,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: err_state,
            } if err_state == state
        ));

        let err = map
            .delete_payload_shard(ClusterEpoch::INITIAL, location, &key)
            .unwrap_err();
        assert!(matches!(
            err,
            ShardIoError::PgNotActive {
                node_id: 0,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: err_state,
            } if err_state == state
        ));

        assert!(matches!(
            map.node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .read_shard_file(0, &key),
            Err(StoreError::NotFound)
        ));
    }
}

#[test]
fn direct_put_payload_write_fails_closed_when_required_shard_node_leaves_acting_set() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, _object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };

    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let reservation_id = crate::SessionId::try_from("56".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let segment_okh = [96; 16];
    let placement_key = crate::cluster::segment_payload_placement_key(&segment_okh, generation_id);
    let locations = cluster
        .place_payload_shards(DataPgId::new(PgId::new(data_pg)), ec_shape, &placement_key)
        .unwrap();
    let (removed_shard_index, removed_node) = locations
        .iter()
        .enumerate()
        .rev()
        .map(|(index, location)| (index, location.node_id()))
        .find(|(_, node_id)| *node_id != NodeId::new(0))
        .expect("test placement should use a non-primary shard node");
    assert!(
        removed_shard_index > 0,
        "test must fail after at least one earlier shard write"
    );
    drop(cluster);

    {
        let route = Arc::get_mut(&mut map)
            .unwrap()
            .pg_routes
            .get_mut(&PgId::new(data_pg))
            .unwrap();
        let acting_set: Vec<NodeId> = node_ids
            .into_iter()
            .filter(|node_id| *node_id != removed_node)
            .collect();
        if route.primary_node_id == removed_node {
            route.primary_node_id = acting_set[0];
        }
        route.acting_set = Arc::from(acting_set);
    }
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let err = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            b"strict payload write requires every placed shard",
        )
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::NodeNotInActingSet {
            node_id,
            pg_id,
            cluster_epoch: ClusterEpoch::INITIAL,
        } if node_id == removed_node.as_u32() && pg_id == data_pg
    ));
    for shard_index in 0..ec_shape.k + ec_shape.m {
        assert!(
            !cluster
                .test_payload_shard_file_exists(
                    data_pg,
                    ec_shape,
                    &segment_okh,
                    generation_id,
                    shard_index,
                )
                .unwrap(),
            "failed strict payload write must not leave shard {shard_index}"
        );
    }
}

#[test]
fn placed_segment_recovery_propagates_non_active_pg_route() {
    let non_active_states = [
        PgState::Peering,
        PgState::Degraded,
        PgState::Backfilling,
        PgState::Inconsistent,
    ];

    for state in non_active_states {
        let tmp = test_util::tempdir();
        let node_ids = [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
            NodeId::new(5),
        ];
        let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
        let mut map =
            Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let segment = write_committed_direct_segment(&cluster, b"phase-six-seven-route-read");
        let data_pg_id = DataPgId::new(PgId::new(segment.written.data_pg_id));
        drop(cluster);

        Arc::get_mut(&mut map)
            .unwrap()
            .pg_routes
            .get_mut(&data_pg_id.pg_id())
            .unwrap()
            .state = state;
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let shard_size = segment
            .payload
            .len()
            .div_ceil(usize::from(segment.written.ec.k));
        let mut all_shards = vec![None; usize::from(segment.written.ec.k + segment.written.ec.m)];
        let mut present_count = 0;

        let err = cluster
            .try_load_placed_segment_shard(
                segment.written.data_pg_id,
                &segment.segment_okh,
                segment.generation_id,
                &segment.locations,
                0,
                shard_size,
                &mut all_shards,
                &mut present_count,
                None,
            )
            .unwrap_err();

        assert!(matches!(
            err,
            StoreError::PgNotActive {
                pg_id,
                cluster_epoch,
                state: err_state,
            } if pg_id == data_pg_id.get()
                && cluster_epoch == ClusterEpoch::INITIAL
                && err_state == state
        ));
        assert_eq!(present_count, 0);
        assert!(all_shards.iter().all(Option::is_none));
    }
}

#[test]
fn placed_payload_delete_propagates_missing_pg_route() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let cluster =
        crate::StorageCluster::open_local_nodes(tmp.path(), &node_ids, &[0], ec_shape).unwrap();

    let err = cluster
        .delete_payload_shard_set(99, ec_shape, &[17; 16], crate::GenerationId::MIN)
        .unwrap_err();

    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::ClusterPgNotFound {
            pg_id: 99,
            cluster_epoch: ClusterEpoch::INITIAL,
        })
    ));
}

#[test]
fn placed_segment_recovery_propagates_missing_shard_pg_route() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let cluster =
        crate::StorageCluster::open_local_nodes(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
    let segment = write_committed_direct_segment(&cluster, b"phase-five-missing-pg-route");
    let shard_size = segment
        .payload
        .len()
        .div_ceil(usize::from(segment.written.ec.k));
    let mut locations = segment.locations.clone();
    locations[0] = ShardLocation::new(
        ClusterEpoch::INITIAL,
        DataPgId::new(PgId::new(99)),
        ShardIndex::new(0),
        locations[0].node_id(),
    );
    let mut all_shards = vec![None; usize::from(segment.written.ec.k + segment.written.ec.m)];
    let mut present_count = 0;

    let err = cluster
        .try_load_placed_segment_shard(
            segment.written.data_pg_id,
            &segment.segment_okh,
            segment.generation_id,
            &locations,
            0,
            shard_size,
            &mut all_shards,
            &mut present_count,
            None,
        )
        .unwrap_err();

    assert!(matches!(
        err,
        StoreError::ShardPgNotFound {
            node_id,
            pg_id: 99,
            cluster_epoch: ClusterEpoch::INITIAL,
        } if node_id == locations[0].node_id().as_u32()
    ));
    assert_eq!(present_count, 0);
    assert!(all_shards.iter().all(Option::is_none));
}

#[test]
fn placed_segment_recovery_propagates_node_not_in_acting_set() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let cluster =
        crate::StorageCluster::open_local_nodes(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
    let segment = write_committed_direct_segment(&cluster, b"phase-five-acting-set-route");
    let shard_size = segment
        .payload
        .len()
        .div_ceil(usize::from(segment.written.ec.k));
    let mut locations = segment.locations.clone();
    locations[0] = ShardLocation::new(
        ClusterEpoch::INITIAL,
        DataPgId::new(PgId::new(segment.written.data_pg_id)),
        ShardIndex::new(0),
        NodeId::new(99),
    );
    let mut all_shards = vec![None; usize::from(segment.written.ec.k + segment.written.ec.m)];
    let mut present_count = 0;

    let err = cluster
        .try_load_placed_segment_shard(
            segment.written.data_pg_id,
            &segment.segment_okh,
            segment.generation_id,
            &locations,
            0,
            shard_size,
            &mut all_shards,
            &mut present_count,
            None,
        )
        .unwrap_err();

    assert!(matches!(
        err,
        StoreError::NodeNotInActingSet {
            node_id: 99,
            pg_id,
            cluster_epoch: ClusterEpoch::INITIAL,
        } if pg_id == segment.written.data_pg_id
    ));
    assert_eq!(present_count, 0);
    assert!(all_shards.iter().all(Option::is_none));
}

#[test]
fn placed_segment_recovery_propagates_stale_shard_location() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let cluster =
        crate::StorageCluster::open_local_nodes(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
    let segment = write_committed_direct_segment(&cluster, b"phase-five-stale-location");
    let shard_size = segment
        .payload
        .len()
        .div_ceil(usize::from(segment.written.ec.k));
    let mut locations = segment.locations.clone();
    locations[0] = ShardLocation::new(
        ClusterEpoch::new(2).unwrap(),
        DataPgId::new(PgId::new(segment.written.data_pg_id)),
        ShardIndex::new(0),
        locations[0].node_id(),
    );
    let mut all_shards = vec![None; usize::from(segment.written.ec.k + segment.written.ec.m)];
    let mut present_count = 0;

    let err = cluster
        .try_load_placed_segment_shard(
            segment.written.data_pg_id,
            &segment.segment_okh,
            segment.generation_id,
            &locations,
            0,
            shard_size,
            &mut all_shards,
            &mut present_count,
            None,
        )
        .unwrap_err();

    assert!(matches!(
        err,
        StoreError::StaleShardLocation {
            node_id,
            pg_id,
            location_epoch,
            current_epoch: ClusterEpoch::INITIAL,
        } if node_id == locations[0].node_id().as_u32()
            && pg_id == segment.written.data_pg_id
            && location_epoch == ClusterEpoch::new(2).unwrap()
    ));
    assert_eq!(present_count, 0);
    assert!(all_shards.iter().all(Option::is_none));
}

#[test]
fn placed_segment_recovery_propagates_shard_index_mismatch() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let cluster =
        crate::StorageCluster::open_local_nodes(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
    let segment = write_committed_direct_segment(&cluster, b"phase-five-shard-index-route");
    let shard_size = segment
        .payload
        .len()
        .div_ceil(usize::from(segment.written.ec.k));
    let mut locations = segment.locations.clone();
    locations[0] = ShardLocation::new(
        ClusterEpoch::INITIAL,
        DataPgId::new(PgId::new(segment.written.data_pg_id)),
        ShardIndex::new(1),
        locations[0].node_id(),
    );
    let mut all_shards = vec![None; usize::from(segment.written.ec.k + segment.written.ec.m)];
    let mut present_count = 0;

    let err = cluster
        .try_load_placed_segment_shard(
            segment.written.data_pg_id,
            &segment.segment_okh,
            segment.generation_id,
            &locations,
            0,
            shard_size,
            &mut all_shards,
            &mut present_count,
            None,
        )
        .unwrap_err();

    assert!(matches!(
        err,
        StoreError::ShardIndexMismatch {
            node_id,
            pg_id,
            cluster_epoch: ClusterEpoch::INITIAL,
            location_shard_index: 1,
            key_shard_index: 0,
        } if node_id == locations[0].node_id().as_u32()
            && pg_id == segment.written.data_pg_id
    ));
    assert_eq!(present_count, 0);
    assert!(all_shards.iter().all(Option::is_none));
}

#[test]
fn placed_segment_recovery_wraps_node_store_error_with_shard_route() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let mut map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let segment = write_committed_direct_segment(&cluster, b"phase-five-shard-store-route");
    drop(cluster);

    Arc::get_mut(&mut map).unwrap().pg_routes.insert(
        PgId::new(99),
        LocalPgRoute::active(
            ClusterEpoch::INITIAL,
            PgId::new(99),
            NodeId::new(0),
            Arc::<[NodeId]>::from(node_ids.to_vec()),
        ),
    );
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let shard_size = segment
        .payload
        .len()
        .div_ceil(usize::from(segment.written.ec.k));
    let mut locations = segment.locations.clone();
    locations[0] = ShardLocation::new(
        ClusterEpoch::INITIAL,
        DataPgId::new(PgId::new(99)),
        ShardIndex::new(0),
        locations[0].node_id(),
    );
    let mut all_shards = vec![None; usize::from(segment.written.ec.k + segment.written.ec.m)];
    let mut present_count = 0;

    let err = cluster
        .try_load_placed_segment_shard(
            segment.written.data_pg_id,
            &segment.segment_okh,
            segment.generation_id,
            &locations,
            0,
            shard_size,
            &mut all_shards,
            &mut present_count,
            None,
        )
        .unwrap_err();

    match err {
        StoreError::ShardStore {
            node_id,
            pg_id: 99,
            cluster_epoch: ClusterEpoch::INITIAL,
            source,
        } => {
            assert_eq!(node_id, locations[0].node_id().as_u32());
            assert!(matches!(*source, StoreError::PgNotFound { pg_id: 99 }));
        }
        other => panic!("expected shard store error with route context, got {other:?}"),
    }
    assert_eq!(present_count, 0);
    assert!(all_shards.iter().all(Option::is_none));
}

#[test]
fn placed_segment_recovery_treats_length_corrupt_shard_as_recoverable() {
    for extra_length in [false, true] {
        let tmp = test_util::tempdir();
        let node_ids = [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
            NodeId::new(5),
        ];
        let cluster = crate::StorageCluster::open_local_nodes(
            tmp.path(),
            &node_ids,
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap();
        let segment =
            write_committed_direct_segment(&cluster, b"phase-five-length-corrupt-payload");
        let shard_size = segment
            .payload
            .len()
            .div_ceil(usize::from(segment.written.ec.k));
        let corrupt_len = if extra_length { shard_size + 1 } else { 1 };
        let shard_path = cluster
            .test_payload_shard_file_path(
                segment.written.data_pg_id,
                segment.written.ec,
                &segment.segment_okh,
                segment.generation_id,
                0,
            )
            .unwrap();
        std::fs::write(&shard_path, vec![0xAB; corrupt_len]).unwrap();

        let mut recovered = Vec::new();
        cluster
            .read_segment_payload_stored_bytes_into(
                crate::SegmentStoredBytesRequest {
                    data_pg_id: segment.written.data_pg_id,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.generation_id,
                    stored_size: segment.payload.len(),
                    segment_crc64: checksum::crc64::checksum(&segment.payload),
                    ec: segment.written.ec,
                },
                &mut recovered,
            )
            .unwrap();

        assert_eq!(
            recovered, segment.payload,
            "failed to recover when corrupt shard extra_length={extra_length}"
        );
    }
}

#[test]
fn placed_segment_recovery_treats_checksum_corrupt_shard_as_recoverable() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let cluster = crate::StorageCluster::open_local_nodes(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let segment =
        write_committed_direct_segment(&cluster, b"phase-eleven-checksum-corrupt-payload");
    let shard_size = segment
        .payload
        .len()
        .div_ceil(usize::from(segment.written.ec.k));
    let shard_path = cluster
        .test_payload_shard_file_path(
            segment.written.data_pg_id,
            segment.written.ec,
            &segment.segment_okh,
            segment.generation_id,
            0,
        )
        .unwrap();
    std::fs::write(&shard_path, vec![0xAB; shard_size]).unwrap();

    let mut recovered = Vec::new();
    cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: segment.written.data_pg_id,
                segment_okh: segment.segment_okh,
                segment_vid: segment.generation_id,
                stored_size: segment.payload.len(),
                segment_crc64: checksum::crc64::checksum(&segment.payload),
                ec: segment.written.ec,
            },
            &mut recovered,
        )
        .unwrap();

    assert_eq!(recovered, segment.payload);
}

#[test]
fn placed_segment_recovery_treats_missing_shard_as_recoverable() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let cluster = crate::StorageCluster::open_local_nodes(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let segment = write_committed_direct_segment(&cluster, b"phase-eleven-missing-shard-payload");
    let shard_path = cluster
        .test_payload_shard_file_path(
            segment.written.data_pg_id,
            segment.written.ec,
            &segment.segment_okh,
            segment.generation_id,
            0,
        )
        .unwrap();
    std::fs::remove_file(&shard_path).unwrap();

    let mut recovered = Vec::new();
    cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: segment.written.data_pg_id,
                segment_okh: segment.segment_okh,
                segment_vid: segment.generation_id,
                stored_size: segment.payload.len(),
                segment_crc64: checksum::crc64::checksum(&segment.payload),
                ec: segment.written.ec,
            },
            &mut recovered,
        )
        .unwrap();

    assert_eq!(recovered, segment.payload);
}

#[test]
fn placed_segment_recovery_rejects_more_missing_shards_than_ec_can_tolerate() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let cluster = crate::StorageCluster::open_local_nodes(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let segment = write_committed_direct_segment(&cluster, b"phase-eleven-unrecoverable-shards");
    let missing_shards: Vec<ShardIndex> = (0..=segment.written.ec.m).map(ShardIndex::new).collect();
    for shard_index in &missing_shards {
        let shard_path = cluster
            .test_payload_shard_file_path(
                segment.written.data_pg_id,
                segment.written.ec,
                &segment.segment_okh,
                segment.generation_id,
                shard_index.get(),
            )
            .unwrap();
        std::fs::remove_file(&shard_path).unwrap();
    }

    let req = crate::SegmentStoredBytesRequest {
        data_pg_id: segment.written.data_pg_id,
        segment_okh: segment.segment_okh,
        segment_vid: segment.generation_id,
        stored_size: segment.payload.len(),
        segment_crc64: checksum::crc64::checksum(&segment.payload),
        ec: segment.written.ec,
    };
    let mut recovered = Vec::new();
    let err = cluster
        .read_segment_payload_stored_bytes_into(req, &mut recovered)
        .unwrap_err();

    assert!(matches!(err, StoreError::NotFound));
    assert!(recovered.is_empty());

    let err = cluster
        .repair_placed_segment_payload_shards(req, &[missing_shards[0]])
        .unwrap_err();
    assert!(matches!(err, StoreError::NotFound));

    let health = cluster.placed_segment_payload_shard_health(req).unwrap();
    assert_eq!(
        health.total_shards,
        usize::from(segment.written.ec.k + segment.written.ec.m)
    );
    assert_eq!(health.required_shards, usize::from(segment.written.ec.k));
    assert_eq!(
        health.risk,
        crate::cluster::PlacedSegmentShardSetRisk::Unrecoverable
    );
    assert_eq!(health.repair_targets(), missing_shards);

    let err = cluster
        .placed_segment_payload_shard_repair_targets(req)
        .unwrap_err();
    assert!(matches!(err, StoreError::NotFound));
}

#[test]
fn repair_placed_segment_payload_shards_restores_multiple_missing_physical_shards() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let cluster = crate::StorageCluster::open_local_nodes(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let segment = write_committed_direct_segment(&cluster, b"phase-eleven-repair-shard-payload");
    let shard_indices = [ShardIndex::new(0), ShardIndex::new(1)];
    let shard_paths: Vec<_> = shard_indices
        .iter()
        .map(|shard_index| {
            cluster
                .test_payload_shard_file_path(
                    segment.written.data_pg_id,
                    segment.written.ec,
                    &segment.segment_okh,
                    segment.generation_id,
                    shard_index.get(),
                )
                .unwrap()
        })
        .collect();
    for shard_path in &shard_paths {
        std::fs::remove_file(shard_path).unwrap();
    }

    let repaired = cluster
        .repair_placed_segment_payload_shards(
            crate::SegmentStoredBytesRequest {
                data_pg_id: segment.written.data_pg_id,
                segment_okh: segment.segment_okh,
                segment_vid: segment.generation_id,
                stored_size: segment.payload.len(),
                segment_crc64: checksum::crc64::checksum(&segment.payload),
                ec: segment.written.ec,
            },
            &shard_indices,
        )
        .unwrap();

    assert_eq!(repaired.len(), shard_indices.len());
    for ((repaired, shard_index), shard_path) in repaired
        .iter()
        .zip(shard_indices.iter())
        .zip(shard_paths.iter())
    {
        assert_eq!(repaired.key.shard_index(), *shard_index);
        let repaired_bytes = std::fs::read(shard_path).unwrap();
        assert_eq!(repaired.ack.stored_size, repaired_bytes.len() as u64);
        assert_eq!(
            repaired.ack.crc64,
            checksum::crc64::checksum(&repaired_bytes)
        );
    }

    let mut read_back = Vec::new();
    cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: segment.written.data_pg_id,
                segment_okh: segment.segment_okh,
                segment_vid: segment.generation_id,
                stored_size: segment.payload.len(),
                segment_crc64: checksum::crc64::checksum(&segment.payload),
                ec: segment.written.ec,
            },
            &mut read_back,
        )
        .unwrap();
    assert_eq!(read_back, segment.payload);
}

#[test]
fn placed_segment_payload_shard_repair_targets_identifies_and_clears_missing_and_corrupt_shards() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let cluster = crate::StorageCluster::open_local_nodes(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let segment = write_committed_direct_segment(&cluster, b"phase-eleven-inspect-repair");
    let missing_shard = ShardIndex::new(0);
    let corrupt_shard = ShardIndex::new(1);
    let missing_path = cluster
        .test_payload_shard_file_path(
            segment.written.data_pg_id,
            segment.written.ec,
            &segment.segment_okh,
            segment.generation_id,
            missing_shard.get(),
        )
        .unwrap();
    let corrupt_path = cluster
        .test_payload_shard_file_path(
            segment.written.data_pg_id,
            segment.written.ec,
            &segment.segment_okh,
            segment.generation_id,
            corrupt_shard.get(),
        )
        .unwrap();
    let corrupt_size = segment
        .payload
        .len()
        .div_ceil(usize::from(segment.written.ec.k));
    std::fs::remove_file(&missing_path).unwrap();
    std::fs::write(&corrupt_path, vec![0xAB; corrupt_size]).unwrap();

    let req = crate::SegmentStoredBytesRequest {
        data_pg_id: segment.written.data_pg_id,
        segment_okh: segment.segment_okh,
        segment_vid: segment.generation_id,
        stored_size: segment.payload.len(),
        segment_crc64: checksum::crc64::checksum(&segment.payload),
        ec: segment.written.ec,
    };
    let health = cluster.placed_segment_payload_shard_health(req).unwrap();
    assert_eq!(
        health.total_shards,
        usize::from(segment.written.ec.k + segment.written.ec.m)
    );
    assert_eq!(health.required_shards, usize::from(segment.written.ec.k));
    assert_eq!(
        health.valid_shards,
        usize::from(segment.written.ec.k + segment.written.ec.m) - 2
    );
    assert_eq!(
        health.risk,
        crate::cluster::PlacedSegmentShardSetRisk::Degraded {
            tolerance_remaining: 0,
        }
    );
    assert_eq!(health.repair_targets(), vec![missing_shard, corrupt_shard]);
    assert!(matches!(
        health.shards[usize::from(missing_shard.get())].validation,
        crate::cluster::PlacedSegmentShardValidation::Unreadable { .. }
    ));
    assert!(matches!(
        health.shards[usize::from(corrupt_shard.get())].validation,
        crate::cluster::PlacedSegmentShardValidation::Unreadable { .. }
    ));

    let targets = cluster
        .placed_segment_payload_shard_repair_targets(req)
        .unwrap();
    assert_eq!(targets, vec![missing_shard, corrupt_shard]);

    let repaired = cluster
        .repair_placed_segment_payload_shards(req, &targets)
        .unwrap();
    assert_eq!(repaired.len(), targets.len());
    assert_eq!(
        cluster
            .placed_segment_payload_shard_repair_targets(req)
            .unwrap(),
        Vec::<ShardIndex>::new()
    );
    let health = cluster.placed_segment_payload_shard_health(req).unwrap();
    assert_eq!(
        health.risk,
        crate::cluster::PlacedSegmentShardSetRisk::Healthy
    );
    assert!(health.repair_targets().is_empty());
}

#[test]
fn placed_segment_payload_shard_health_uses_reconstructed_pg_route_snapshot() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let mut authority = crate::control_plane::SingleAuthorityControlPlane::open(
        crate::control_plane::FileControlPlaneStore::new(tmp.path().join("control-plane.state")),
    )
    .unwrap();
    authority
        .bootstrap_initial_cluster_map(
            node_ids
                .iter()
                .map(|node_id| {
                    (
                        *node_id,
                        tmp.path()
                            .join(format!("node-{}.sock", node_id.as_u32()))
                            .to_string_lossy()
                            .into_owned(),
                    )
                })
                .collect(),
            vec![PgId::new(0)],
        )
        .unwrap();
    let route = authority
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(0), authority.snapshot().cluster_epoch())
        .unwrap();
    assert_eq!(route.primary_lease_deadline_ms(), None);

    let configs: Vec<_> = node_ids
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
    let map = Arc::new(
        LocalClusterMap::open_frontend_placeholder_with_configs_and_epoch(
            NodeId::new(0),
            configs.clone(),
            &[0],
            ec_shape,
            route.cluster_epoch(),
        )
        .unwrap(),
    );
    let historical_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let segment = write_committed_direct_segment(&historical_cluster, b"phase-eleven-route-health");
    let req = crate::SegmentStoredBytesRequest {
        data_pg_id: segment.written.data_pg_id,
        segment_okh: segment.segment_okh,
        segment_vid: segment.generation_id,
        stored_size: segment.payload.len(),
        segment_crc64: checksum::crc64::checksum(&segment.payload),
        ec: segment.written.ec,
    };
    let current_epoch = ClusterEpoch::new(route.cluster_epoch().get() + 1).unwrap();
    let current_map = Arc::new(
        LocalClusterMap::open_frontend_placeholder_with_configs_and_epoch(
            NodeId::new(0),
            configs,
            &[0],
            ec_shape,
            current_epoch,
        )
        .unwrap(),
    );
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&current_map)).unwrap();
    assert_ne!(route.cluster_epoch(), cluster.cluster_epoch());

    let health = cluster
        .placed_segment_payload_shard_health_for_pg_route_snapshot(&route, req)
        .unwrap();
    assert_eq!(
        health.risk,
        crate::cluster::PlacedSegmentShardSetRisk::Healthy
    );
    assert_eq!(health.valid_shards, health.total_shards);

    let missing_shard = ShardIndex::new(0);
    let missing_path = cluster
        .test_payload_shard_file_path(
            segment.written.data_pg_id,
            segment.written.ec,
            &segment.segment_okh,
            segment.generation_id,
            missing_shard.get(),
        )
        .unwrap();
    std::fs::remove_file(&missing_path).unwrap();
    let health = cluster
        .placed_segment_payload_shard_health_for_pg_route_snapshot(&route, req)
        .unwrap();
    assert_eq!(
        health.risk,
        crate::cluster::PlacedSegmentShardSetRisk::Degraded {
            tolerance_remaining: usize::from(segment.written.ec.m) - 1,
        }
    );
    assert_eq!(health.repair_targets(), vec![missing_shard]);
}

struct BackfillRouteFixture {
    _tmp: test_util::TempDir,
    source_cluster: Arc<crate::StorageCluster>,
    desired_cluster: Arc<crate::StorageCluster>,
    source_route: crate::control_plane::PgRouteSnapshot,
    desired_route: crate::control_plane::PgRouteSnapshot,
    req: crate::SegmentStoredBytesRequest,
    segment: CommittedDirectSegment,
}

impl BackfillRouteFixture {
    fn remove_source_shard(&self, shard_index: ShardIndex) {
        let shard_path = self
            .source_cluster
            .test_payload_shard_file_path(
                self.segment.written.data_pg_id,
                self.segment.written.ec,
                &self.segment.segment_okh,
                self.segment.generation_id,
                shard_index.get(),
            )
            .unwrap();
        std::fs::remove_file(shard_path).unwrap();
    }
}

fn backfill_route_fixture(payload: &[u8]) -> BackfillRouteFixture {
    let tmp = test_util::tempdir();
    let source_node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
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
    let desired_node_ids = [
        NodeId::new(0),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
        NodeId::new(6),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let mut authority = crate::control_plane::SingleAuthorityControlPlane::open(
        crate::control_plane::FileControlPlaneStore::new(tmp.path().join("control-plane.state")),
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
                            .join(format!("node-{}.sock", node_id.as_u32()))
                            .to_string_lossy()
                            .into_owned(),
                    )
                })
                .collect(),
            vec![PgId::new(0)],
        )
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_node_membership(
            NodeId::new(6),
            crate::control_plane::NodeMembershipState::Active,
        )
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(0), desired_node_ids.to_vec())
        .unwrap();
    let source_route = authority
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(0), source_epoch)
        .unwrap();
    let desired_route = authority
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(0), authority.snapshot().cluster_epoch())
        .unwrap();
    let desired_route = crate::control_plane::PgRouteSnapshot::reconstructed(
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
        LocalClusterMap::open_frontend_placeholder_with_configs_and_epoch(
            NodeId::new(0),
            configs.iter().take(source_node_ids.len()).cloned(),
            &[0],
            ec_shape,
            source_route.cluster_epoch(),
        )
        .unwrap(),
    );
    let source_cluster = crate::StorageCluster::from_local_map(Arc::clone(&source_map)).unwrap();
    let segment = write_committed_direct_segment(&source_cluster, payload);
    let req = crate::SegmentStoredBytesRequest {
        data_pg_id: segment.written.data_pg_id,
        segment_okh: segment.segment_okh,
        segment_vid: segment.generation_id,
        stored_size: segment.payload.len(),
        segment_crc64: checksum::crc64::checksum(&segment.payload),
        ec: segment.written.ec,
    };
    let mut desired_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs,
        &[0],
        ec_shape,
        desired_route.cluster_epoch(),
        [LocalPgRoute::from(&desired_route)],
    )
    .unwrap();
    desired_map.test_install_historical_pg_routes([source_route.clone()]);
    let desired_map = Arc::new(desired_map);
    let desired_cluster = crate::StorageCluster::from_local_map(Arc::clone(&desired_map)).unwrap();

    BackfillRouteFixture {
        _tmp: tmp,
        source_cluster,
        desired_cluster,
        source_route,
        desired_route,
        req,
        segment,
    }
}

#[test]
fn placed_segment_payload_shard_backfill_plan_identifies_direct_copy_targets() {
    let fixture = backfill_route_fixture(b"phase-eleven-backfill-plan");

    let plan = fixture
        .desired_cluster
        .placed_segment_payload_shard_backfill_plan(
            &fixture.source_route,
            &fixture.desired_route,
            fixture.req,
        )
        .unwrap();
    assert_eq!(
        plan.source_health.risk,
        crate::cluster::PlacedSegmentShardSetRisk::Healthy
    );
    assert!(!plan.is_complete());
    assert!(!plan.copy_targets.is_empty());
    assert!(plan.reconstruction_targets.is_empty());
    assert!(plan.unrecoverable_targets.is_empty());
    assert_eq!(
        plan.already_present.len() + plan.copy_targets.len(),
        plan.desired_health.total_shards
    );
    for target in &plan.copy_targets {
        assert_ne!(target.source.node_id(), target.destination.node_id());
        assert_eq!(target.source.data_pg_id(), target.destination.data_pg_id());
        assert_eq!(
            target.source.shard_index(),
            target.destination.shard_index()
        );
        assert_eq!(target.shard_index, target.source.shard_index());
    }

    let copied = fixture
        .desired_cluster
        .backfill_placed_segment_payload_shard_direct_copies(
            &fixture.source_route,
            &fixture.desired_route,
            fixture.req,
        )
        .unwrap();
    assert_eq!(copied.len(), plan.copy_targets.len());
    let plan = fixture
        .desired_cluster
        .placed_segment_payload_shard_backfill_plan(
            &fixture.source_route,
            &fixture.desired_route,
            fixture.req,
        )
        .unwrap();
    assert!(plan.is_complete());
    assert_eq!(plan.already_present.len(), plan.desired_health.total_shards);
}

#[test]
fn placed_segment_payload_direct_copy_backfill_rejects_reconstruction_targets() {
    let fixture = backfill_route_fixture(b"phase-eleven-backfill-reconstruction");
    let plan = fixture
        .desired_cluster
        .placed_segment_payload_shard_backfill_plan(
            &fixture.source_route,
            &fixture.desired_route,
            fixture.req,
        )
        .unwrap();
    let target = plan.copy_targets[0].shard_index;
    fixture.remove_source_shard(target);

    let plan = fixture
        .desired_cluster
        .placed_segment_payload_shard_backfill_plan(
            &fixture.source_route,
            &fixture.desired_route,
            fixture.req,
        )
        .unwrap();
    assert!(plan
        .copy_targets
        .iter()
        .all(|copy| copy.shard_index != target));
    assert!(plan.reconstruction_targets.contains(&target));
    assert!(plan.unrecoverable_targets.is_empty());

    let err = fixture
        .desired_cluster
        .backfill_placed_segment_payload_shard_direct_copies(
            &fixture.source_route,
            &fixture.desired_route,
            fixture.req,
        )
        .unwrap_err();
    assert!(
        matches!(err, StoreError::PayloadShardSetMismatch { ref reason } if reason.contains("EC reconstruction targets")),
        "expected reconstruction rejection, got {err:?}"
    );
}

#[test]
fn placed_segment_payload_backfill_reconstructs_missing_source_targets() {
    let fixture = backfill_route_fixture(b"phase-eleven-backfill-reconstructs-target");
    let plan = fixture
        .desired_cluster
        .placed_segment_payload_shard_backfill_plan(
            &fixture.source_route,
            &fixture.desired_route,
            fixture.req,
        )
        .unwrap();
    let target = plan.copy_targets[0].shard_index;
    fixture.remove_source_shard(target);

    let plan = fixture
        .desired_cluster
        .placed_segment_payload_shard_backfill_plan(
            &fixture.source_route,
            &fixture.desired_route,
            fixture.req,
        )
        .unwrap();
    assert!(plan.reconstruction_targets.contains(&target));
    assert!(plan.unrecoverable_targets.is_empty());

    let backfilled = fixture
        .desired_cluster
        .backfill_placed_segment_payload_shards(
            &fixture.source_route,
            &fixture.desired_route,
            fixture.req,
        )
        .unwrap();
    assert!(backfilled
        .iter()
        .any(|written| written.key.shard_index() == target));
    let plan = fixture
        .desired_cluster
        .placed_segment_payload_shard_backfill_plan(
            &fixture.source_route,
            &fixture.desired_route,
            fixture.req,
        )
        .unwrap();
    assert!(plan.is_complete());
    assert_eq!(plan.already_present.len(), plan.desired_health.total_shards);
}

#[test]
fn placed_segment_payload_direct_copy_backfill_rejects_unrecoverable_targets() {
    let fixture = backfill_route_fixture(b"phase-eleven-backfill-unrecoverable");
    let plan = fixture
        .desired_cluster
        .placed_segment_payload_shard_backfill_plan(
            &fixture.source_route,
            &fixture.desired_route,
            fixture.req,
        )
        .unwrap();
    let target = plan.copy_targets[0].shard_index;
    let mut missing = vec![target];
    for shard_index in 0..(fixture.req.ec.k + fixture.req.ec.m) {
        let shard_index = ShardIndex::new(shard_index);
        if !missing.contains(&shard_index) {
            missing.push(shard_index);
        }
        if missing.len() > usize::from(fixture.req.ec.m) {
            break;
        }
    }
    for shard_index in &missing {
        fixture.remove_source_shard(*shard_index);
    }

    let plan = fixture
        .desired_cluster
        .placed_segment_payload_shard_backfill_plan(
            &fixture.source_route,
            &fixture.desired_route,
            fixture.req,
        )
        .unwrap();
    assert_eq!(
        plan.source_health.risk,
        crate::cluster::PlacedSegmentShardSetRisk::Unrecoverable
    );
    assert!(plan.unrecoverable_targets.contains(&target));

    let err = fixture
        .desired_cluster
        .backfill_placed_segment_payload_shard_direct_copies(
            &fixture.source_route,
            &fixture.desired_route,
            fixture.req,
        )
        .unwrap_err();
    assert!(
        matches!(err, StoreError::PayloadShardSetMismatch { ref reason } if reason.contains("unrecoverable targets")),
        "expected unrecoverable rejection, got {err:?}"
    );
}

#[test]
fn placed_segment_payload_backfill_plan_record_derives_durable_priority() {
    let fixture = backfill_route_fixture(b"phase-eleven-backfill-record-priority");
    let initial_plan = fixture
        .desired_cluster
        .placed_segment_payload_shard_backfill_plan(
            &fixture.source_route,
            &fixture.desired_route,
            fixture.req,
        )
        .unwrap();
    let degraded_source_target = initial_plan.copy_targets[0].shard_index;
    fixture.remove_source_shard(degraded_source_target);

    let plan = fixture
        .desired_cluster
        .record_placed_segment_shard_backfill_for_plan(
            &fixture.source_route,
            &fixture.desired_route,
            fixture.req,
            None,
        )
        .unwrap();
    assert!(!plan.is_complete());
    assert_eq!(
        plan.source_health.risk,
        crate::cluster::PlacedSegmentShardSetRisk::Degraded {
            tolerance_remaining: usize::from(fixture.req.ec.m - 1),
        }
    );
    assert!(plan
        .reconstruction_targets
        .contains(&degraded_source_target));

    let rows = fixture
        .desired_cluster
        .list_placed_segment_shard_backfills(fixture.req.data_pg_id)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].work_item.request, fixture.req);
    assert_eq!(
        rows[0].work_item.source_cluster_epoch,
        fixture.source_route.cluster_epoch()
    );
    assert_eq!(
        rows[0].work_item.desired_cluster_epoch,
        fixture.desired_route.cluster_epoch()
    );
    assert_eq!(rows[0].remaining_tolerance, fixture.req.ec.m - 1);

    let mut missing = vec![degraded_source_target];
    for shard_index in 0..(fixture.req.ec.k + fixture.req.ec.m) {
        let shard_index = ShardIndex::new(shard_index);
        if !missing.contains(&shard_index) {
            missing.push(shard_index);
        }
        if missing.len() > usize::from(fixture.req.ec.m) {
            break;
        }
    }
    for shard_index in missing.iter().skip(1) {
        fixture.remove_source_shard(*shard_index);
    }

    let err = fixture
        .desired_cluster
        .record_placed_segment_shard_backfill_for_plan(
            &fixture.source_route,
            &fixture.desired_route,
            fixture.req,
            None,
        )
        .unwrap_err();
    assert!(
        matches!(err, StoreError::PayloadShardSetMismatch { ref reason } if reason.contains("unrecoverable targets")),
        "expected unrecoverable plan rejection, got {err:?}"
    );
    assert_eq!(
        fixture
            .desired_cluster
            .list_placed_segment_shard_backfills(fixture.req.data_pg_id)
            .unwrap()
            .len(),
        1,
        "unrecoverable plan must not enqueue another durable backfill row"
    );
}

#[test]
fn shard_scavenger_backfill_candidate_scan_records_historical_payloads() {
    let fixture = backfill_route_fixture(b"phase-eleven-scanner-backfill-candidate");
    let bucket = crate::BucketName::try_from("backfill-scan-bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("backfill-scan-key".to_string()).unwrap();
    let primary = fixture
        .desired_cluster
        .local_map
        .metadata_pg_primary_node(
            fixture.desired_cluster.operation_epoch(),
            PgId::new(fixture.req.data_pg_id),
        )
        .unwrap();
    assert_eq!(primary.node_id(), NodeId::new(0));
    {
        let pg = primary
            .storage_node()
            .get_pg(fixture.req.data_pg_id)
            .unwrap();
        crate::PgMetadataStore::put_object_with_segments(
            &*pg,
            &crate::PutLiveObjectReq {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: crate::VersionId::Null,
                owner: crate::OwnerIdentity::from_principal("owner"),
                acl_grants: crate::AclGrants::default(),
                public_read: false,
                generation_id: fixture.req.segment_vid,
                size: fixture.req.stored_size as u64,
                etag: crate::ObjectEtag::single_part(fixture.req.segment_crc64),
                ec: fixture.req.ec,
                layout: crate::ObjectLayout::Standard,
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: crate::ObjectLockState::default(),
                encryption: crate::ObjectEncryption::None,
            },
            &[crate::ObjectSegmentRecord {
                bucket,
                key,
                version_id: crate::VersionId::Null,
                segment_index: 0,
                size: fixture.req.stored_size as u64,
                segment_crc64: fixture.req.segment_crc64,
                segment_okh: fixture.req.segment_okh,
                segment_vid: fixture.req.segment_vid,
                data_pg_id: fixture.req.data_pg_id,
                placement_cluster_epoch: fixture.source_route.cluster_epoch(),
                ec_k: fixture.req.ec.k,
                ec_m: fixture.req.ec.m,
            }],
        )
        .unwrap();
    }
    let summary = fixture
        .desired_cluster
        .enqueue_placed_segment_shard_backfills_from_scavenger_references()
        .unwrap();
    assert_eq!(
        summary,
        crate::cluster::PlacedSegmentShardBackfillCandidateEnqueueSummary {
            scanned: 1,
            current_epoch: 0,
            already_complete: 0,
            enqueued: 1,
            unrecoverable: 0,
            failed: 0,
        }
    );

    let rows = fixture
        .desired_cluster
        .list_placed_segment_shard_backfills(fixture.req.data_pg_id)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].work_item.request, fixture.req);
    assert_eq!(
        rows[0].work_item.source_cluster_epoch,
        fixture.source_route.cluster_epoch()
    );
    assert_eq!(
        rows[0].work_item.desired_cluster_epoch,
        fixture.desired_route.cluster_epoch()
    );
}

#[test]
fn placed_segment_payload_shard_repair_targets_rejects_invalid_ec_shape() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let cluster = crate::StorageCluster::open_local_nodes(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();

    let err = cluster
        .placed_segment_payload_shard_repair_targets(crate::SegmentStoredBytesRequest {
            data_pg_id: 0,
            segment_okh: [7; 16],
            segment_vid: crate::GenerationId::MIN,
            stored_size: 1,
            segment_crc64: 0,
            ec: EcShape { k: 0, m: 1 },
        })
        .unwrap_err();

    assert!(matches!(err, StoreError::ErasureCoding { .. }));
}

#[test]
fn repair_placed_segment_payload_shards_if_needed_noops_for_clean_segment() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let cluster = crate::StorageCluster::open_local_nodes(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let segment = write_committed_direct_segment(&cluster, b"phase-eleven-clean-repair-noop");
    let req = crate::SegmentStoredBytesRequest {
        data_pg_id: segment.written.data_pg_id,
        segment_okh: segment.segment_okh,
        segment_vid: segment.generation_id,
        stored_size: segment.payload.len(),
        segment_crc64: checksum::crc64::checksum(&segment.payload),
        ec: segment.written.ec,
    };
    let health = cluster.placed_segment_payload_shard_health(req).unwrap();
    assert_eq!(
        health.risk,
        crate::cluster::PlacedSegmentShardSetRisk::Healthy
    );
    assert_eq!(
        health.valid_shards,
        usize::from(segment.written.ec.k + segment.written.ec.m)
    );
    assert!(health.repair_targets().is_empty());

    let repaired = cluster
        .repair_placed_segment_payload_shards_if_needed(req)
        .unwrap();

    assert!(repaired.is_empty());
}

#[test]
fn placed_segment_payload_shard_health_treats_zero_byte_segment_as_full_shard_set() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let cluster = crate::StorageCluster::open_local_nodes(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let segment = write_committed_direct_segment(&cluster, b"");
    let req = crate::SegmentStoredBytesRequest {
        data_pg_id: segment.written.data_pg_id,
        segment_okh: segment.segment_okh,
        segment_vid: segment.generation_id,
        stored_size: 0,
        segment_crc64: checksum::crc64::checksum(&[]),
        ec: segment.written.ec,
    };

    let health = cluster.placed_segment_payload_shard_health(req).unwrap();

    assert_eq!(
        health.total_shards,
        usize::from(segment.written.ec.k + segment.written.ec.m)
    );
    assert_eq!(health.required_shards, usize::from(segment.written.ec.k));
    assert_eq!(health.valid_shards, health.total_shards);
    assert_eq!(
        health.risk,
        crate::cluster::PlacedSegmentShardSetRisk::Healthy
    );
    assert!(health.repair_targets().is_empty());
    assert!(health.shards.iter().all(|shard| {
        matches!(
            shard.validation,
            crate::cluster::PlacedSegmentShardValidation::Valid
        )
    }));
}

#[test]
fn repair_placed_segment_payload_shards_if_needed_repairs_identified_targets() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let cluster = crate::StorageCluster::open_local_nodes(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let segment = write_committed_direct_segment(&cluster, b"phase-eleven-orchestrated-repair");
    let missing_shard = ShardIndex::new(0);
    let corrupt_shard = ShardIndex::new(1);
    let missing_path = cluster
        .test_payload_shard_file_path(
            segment.written.data_pg_id,
            segment.written.ec,
            &segment.segment_okh,
            segment.generation_id,
            missing_shard.get(),
        )
        .unwrap();
    let corrupt_path = cluster
        .test_payload_shard_file_path(
            segment.written.data_pg_id,
            segment.written.ec,
            &segment.segment_okh,
            segment.generation_id,
            corrupt_shard.get(),
        )
        .unwrap();
    let corrupt_size = segment
        .payload
        .len()
        .div_ceil(usize::from(segment.written.ec.k));
    std::fs::remove_file(&missing_path).unwrap();
    std::fs::write(&corrupt_path, vec![0xAB; corrupt_size]).unwrap();

    let req = crate::SegmentStoredBytesRequest {
        data_pg_id: segment.written.data_pg_id,
        segment_okh: segment.segment_okh,
        segment_vid: segment.generation_id,
        stored_size: segment.payload.len(),
        segment_crc64: checksum::crc64::checksum(&segment.payload),
        ec: segment.written.ec,
    };
    let repaired = cluster
        .repair_placed_segment_payload_shards_if_needed(req)
        .unwrap();

    assert_eq!(repaired.len(), 2);
    assert_eq!(
        repaired
            .iter()
            .map(|written| written.key.shard_index())
            .collect::<Vec<_>>(),
        vec![missing_shard, corrupt_shard]
    );
    assert_eq!(
        cluster
            .placed_segment_payload_shard_repair_targets(req)
            .unwrap(),
        Vec::<ShardIndex>::new()
    );
}

#[test]
fn read_recovery_queues_observed_corrupt_placed_shard_repair_once() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let cluster = crate::StorageCluster::open_local_nodes(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let segment = write_committed_direct_segment(&cluster, b"phase-eleven-read-repair-schedule");
    let data_shard_index = ShardIndex::new(0);
    let shard_size = segment
        .payload
        .len()
        .div_ceil(usize::from(segment.written.ec.k));
    let shard_path = cluster
        .test_payload_shard_file_path(
            segment.written.data_pg_id,
            segment.written.ec,
            &segment.segment_okh,
            segment.generation_id,
            data_shard_index.get(),
        )
        .unwrap();
    std::fs::write(&shard_path, vec![0xAB; shard_size]).unwrap();
    let req = crate::SegmentStoredBytesRequest {
        data_pg_id: segment.written.data_pg_id,
        segment_okh: segment.segment_okh,
        segment_vid: segment.generation_id,
        stored_size: segment.payload.len(),
        segment_crc64: checksum::crc64::checksum(&segment.payload),
        ec: segment.written.ec,
    };

    assert!(cluster
        .try_take_placed_segment_shard_repair_work()
        .is_none());
    for _ in 0..2 {
        let mut recovered = Vec::new();
        cluster
            .read_segment_payload_stored_bytes_into(req, &mut recovered)
            .unwrap();
        assert_eq!(recovered, segment.payload);
    }

    let work = cluster
        .try_take_placed_segment_shard_repair_work()
        .expect("successful read recovery should enqueue observed corrupt shard repair");
    assert_eq!(work.request, req);
    assert_eq!(work.shard_index, data_shard_index);
    assert!(cluster
        .try_take_placed_segment_shard_repair_work()
        .is_none());
    let durable_repairs = cluster
        .list_placed_segment_shard_repairs(req.data_pg_id)
        .unwrap();
    assert_eq!(durable_repairs.len(), 1);
    assert_eq!(durable_repairs[0].work_item, work);
    assert_eq!(durable_repairs[0].observation_count, 2);

    let first_scan = cluster
        .enqueue_durable_placed_segment_shard_repair_work()
        .unwrap();
    assert_eq!(first_scan.scanned, 1);
    assert_eq!(first_scan.enqueued, 1);
    let second_scan = cluster
        .enqueue_durable_placed_segment_shard_repair_work()
        .unwrap();
    assert_eq!(second_scan.scanned, 1);
    assert_eq!(second_scan.enqueued, 0);
    assert_eq!(
        cluster.try_take_placed_segment_shard_repair_work(),
        Some(work)
    );
}

#[test]
fn repair_placed_segment_payload_shard_queues_other_failed_shards_after_verification() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let cluster = crate::StorageCluster::open_local_nodes(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let segment =
        write_committed_direct_segment(&cluster, b"phase-eleven-repair-verifies-full-set");
    let repaired_shard_index = ShardIndex::new(0);
    let other_shard_index = ShardIndex::new(segment.written.ec.k + segment.written.ec.m - 1);
    let shard_size = segment
        .payload
        .len()
        .div_ceil(usize::from(segment.written.ec.k));
    for shard_index in [repaired_shard_index, other_shard_index] {
        let shard_path = cluster
            .test_payload_shard_file_path(
                segment.written.data_pg_id,
                segment.written.ec,
                &segment.segment_okh,
                segment.generation_id,
                shard_index.get(),
            )
            .unwrap();
        std::fs::write(&shard_path, vec![0xAB; shard_size]).unwrap();
    }
    let req = crate::SegmentStoredBytesRequest {
        data_pg_id: segment.written.data_pg_id,
        segment_okh: segment.segment_okh,
        segment_vid: segment.generation_id,
        stored_size: segment.payload.len(),
        segment_crc64: checksum::crc64::checksum(&segment.payload),
        ec: segment.written.ec,
    };

    let repaired = cluster
        .repair_placed_segment_payload_shard(req, repaired_shard_index)
        .unwrap();

    assert_eq!(repaired.key.shard_index(), repaired_shard_index);
    let work = cluster
        .try_take_placed_segment_shard_repair_work()
        .expect("full-set repair verification should enqueue other bad shard");
    assert_eq!(work.request, req);
    assert_eq!(work.shard_index, other_shard_index);
    assert!(cluster
        .try_take_placed_segment_shard_repair_work()
        .is_none());

    let repaired_path = cluster
        .test_payload_shard_file_path(
            segment.written.data_pg_id,
            segment.written.ec,
            &segment.segment_okh,
            segment.generation_id,
            repaired_shard_index.get(),
        )
        .unwrap();
    assert_eq!(
        repaired.ack.crc64,
        checksum::crc64::checksum(&std::fs::read(repaired_path).unwrap())
    );
}

#[test]
fn repair_placed_segment_payload_shards_restores_checksum_corrupt_physical_shard() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let cluster = crate::StorageCluster::open_local_nodes(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let segment = write_committed_direct_segment(&cluster, b"phase-eleven-repair-corrupt-shard");
    let shard_index = ShardIndex::new(0);
    let shard_size = segment
        .payload
        .len()
        .div_ceil(usize::from(segment.written.ec.k));
    let shard_path = cluster
        .test_payload_shard_file_path(
            segment.written.data_pg_id,
            segment.written.ec,
            &segment.segment_okh,
            segment.generation_id,
            shard_index.get(),
        )
        .unwrap();
    let corrupt_bytes = vec![0xAB; shard_size];
    std::fs::write(&shard_path, &corrupt_bytes).unwrap();

    let repaired = cluster
        .repair_placed_segment_payload_shards(
            crate::SegmentStoredBytesRequest {
                data_pg_id: segment.written.data_pg_id,
                segment_okh: segment.segment_okh,
                segment_vid: segment.generation_id,
                stored_size: segment.payload.len(),
                segment_crc64: checksum::crc64::checksum(&segment.payload),
                ec: segment.written.ec,
            },
            &[shard_index],
        )
        .unwrap();

    assert!(cluster
        .try_take_placed_segment_shard_repair_work()
        .is_none());
    assert_eq!(repaired.len(), 1);
    assert_eq!(repaired[0].key.shard_index(), shard_index);
    let repaired_bytes = std::fs::read(&shard_path).unwrap();
    assert_ne!(repaired_bytes, corrupt_bytes);
    assert_eq!(repaired[0].ack.stored_size, repaired_bytes.len() as u64);
    assert_eq!(
        repaired[0].ack.crc64,
        checksum::crc64::checksum(&repaired_bytes)
    );

    let mut read_back = Vec::new();
    cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: segment.written.data_pg_id,
                segment_okh: segment.segment_okh,
                segment_vid: segment.generation_id,
                stored_size: segment.payload.len(),
                segment_crc64: checksum::crc64::checksum(&segment.payload),
                ec: segment.written.ec,
            },
            &mut read_back,
        )
        .unwrap();
    assert_eq!(read_back, segment.payload);
}

#[test]
fn repair_placed_segment_payload_shards_rejects_non_active_pg_route_without_writing() {
    let non_active_states = [
        PgState::Peering,
        PgState::Degraded,
        PgState::Backfilling,
        PgState::Inconsistent,
    ];

    for state in non_active_states {
        let tmp = test_util::tempdir();
        let node_ids = [
            NodeId::new(0),
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
            NodeId::new(5),
        ];
        let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
        let mut map =
            Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        let segment = write_committed_direct_segment(&cluster, b"phase-eleven-repair-route");
        let shard_index = ShardIndex::new(0);
        let shard_path = cluster
            .test_payload_shard_file_path(
                segment.written.data_pg_id,
                segment.written.ec,
                &segment.segment_okh,
                segment.generation_id,
                shard_index.get(),
            )
            .unwrap();
        std::fs::remove_file(&shard_path).unwrap();
        drop(cluster);

        Arc::get_mut(&mut map)
            .unwrap()
            .pg_routes
            .get_mut(&PgId::new(segment.written.data_pg_id))
            .unwrap()
            .state = state;
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

        let err = cluster
            .repair_placed_segment_payload_shards(
                crate::SegmentStoredBytesRequest {
                    data_pg_id: segment.written.data_pg_id,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.generation_id,
                    stored_size: segment.payload.len(),
                    segment_crc64: checksum::crc64::checksum(&segment.payload),
                    ec: segment.written.ec,
                },
                &[shard_index],
            )
            .unwrap_err();

        assert!(matches!(
            err,
            StoreError::PgNotActive {
                pg_id,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: err_state,
            } if pg_id == segment.written.data_pg_id && err_state == state
        ));
        assert!(
            !shard_path.exists(),
            "repair wrote shard while PG route state was {state}"
        );
    }
}

#[test]
fn repair_placed_segment_payload_shards_rejects_stale_operation_epoch_without_writing() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let segment = write_committed_direct_segment(&current_cluster, b"phase-eleven-stale-repair");
    let shard_index = ShardIndex::new(0);
    let shard_path = current_cluster
        .test_payload_shard_file_path(
            segment.written.data_pg_id,
            segment.written.ec,
            &segment.segment_okh,
            segment.generation_id,
            shard_index.get(),
        )
        .unwrap();
    std::fs::remove_file(&shard_path).unwrap();
    let stale_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&map),
        ClusterEpoch::new(2).unwrap(),
    )
    .unwrap();

    let err = stale_cluster
        .repair_placed_segment_payload_shards(
            crate::SegmentStoredBytesRequest {
                data_pg_id: segment.written.data_pg_id,
                segment_okh: segment.segment_okh,
                segment_vid: segment.generation_id,
                stored_size: segment.payload.len(),
                segment_crc64: checksum::crc64::checksum(&segment.payload),
                ec: segment.written.ec,
            },
            &[shard_index],
        )
        .unwrap_err();

    assert!(matches!(
        err,
        StoreError::StalePayloadOperation {
            pg_id,
            operation_epoch,
            current_epoch,
        } if pg_id == segment.written.data_pg_id
            && operation_epoch == ClusterEpoch::new(2).unwrap()
            && current_epoch == ClusterEpoch::INITIAL
    ));
    assert!(!shard_path.exists(), "stale repair wrote target shard");
}

#[test]
fn repair_placed_segment_payload_shards_rejects_invalid_target_sets() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let cluster = crate::StorageCluster::open_local_nodes(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let segment = write_committed_direct_segment(&cluster, b"phase-eleven-repair-targets");
    let req = crate::SegmentStoredBytesRequest {
        data_pg_id: segment.written.data_pg_id,
        segment_okh: segment.segment_okh,
        segment_vid: segment.generation_id,
        stored_size: segment.payload.len(),
        segment_crc64: checksum::crc64::checksum(&segment.payload),
        ec: segment.written.ec,
    };

    let err = cluster
        .repair_placed_segment_payload_shards(req, &[ShardIndex::new(0), ShardIndex::new(0)])
        .unwrap_err();
    assert!(matches!(err, StoreError::PayloadShardSetMismatch { .. }));

    let too_many = [ShardIndex::new(0), ShardIndex::new(1), ShardIndex::new(2)];
    let err = cluster
        .repair_placed_segment_payload_shards(req, &too_many)
        .unwrap_err();
    assert!(matches!(err, StoreError::PayloadShardSetMismatch { .. }));
}

#[test]
fn payload_shard_io_rejects_unknown_pg_before_touching_node_store() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let map = LocalClusterMap::open(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let location = ShardLocation::new(
        ClusterEpoch::INITIAL,
        DataPgId::new(PgId::new(99)),
        ShardIndex::new(0),
        NodeId::new(0),
    );
    let key = ShardKey::new(&[37; 16], 1, 0);

    let err = map
        .write_payload_shard(ClusterEpoch::INITIAL, location, &key, b"unknown pg")
        .unwrap_err();

    assert!(matches!(
        err,
        ShardIoError::PgNotFound {
            node_id: 0,
            pg_id: 99,
            cluster_epoch: ClusterEpoch::INITIAL,
        }
    ));
    assert!(!tmp.path().join("node-0000").join("pg-0099").exists());
}

#[test]
fn rejects_too_few_local_nodes_for_default_ec_shape_before_preparing_dirs() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
    ];
    let err = LocalClusterMap::open(
        tmp.path(),
        &node_ids,
        &[0, 1],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ClusterBuildError::UnplaceableEcShape {
            data_shards: 4,
            parity_shards: 2,
            required_nodes: 6,
            node_count: 5,
        }
    ));
    for node_id in node_ids {
        assert!(
            !tmp.path()
                .join(format!("node-{:04}", node_id.as_u32()))
                .exists(),
            "placement validation must run before preparing local node directories"
        );
    }
}

#[test]
fn rejects_invalid_ec_shape_before_preparing_dirs() {
    let tmp = test_util::tempdir();
    let node_dir = tmp.path().join("node-0000");
    let err = LocalClusterMap::open_with_configs(
        NodeId::new(0),
        [LocalNodeStoreConfig::new(NodeId::new(0), &node_dir)],
        &[0],
        EcShape { k: 0, m: 2 },
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ClusterBuildError::InvalidEcShape {
            data_shards: 0,
            parity_shards: 2,
            ..
        }
    ));
    assert!(
        !node_dir.exists(),
        "EC shape validation must run before preparing local node directories"
    );
}

#[test]
fn rejects_duplicate_local_node_ids() {
    let tmp = test_util::tempdir();
    let err = LocalClusterMap::open_with_configs(
        NodeId::new(0),
        [
            LocalNodeStoreConfig::new(NodeId::new(0), tmp.path().join("a")),
            LocalNodeStoreConfig::new(NodeId::new(0), tmp.path().join("b")),
        ],
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap_err();

    assert!(matches!(err, ClusterBuildError::DuplicateNodeId { id: 0 }));
}

#[test]
fn rejects_empty_pg_set_before_preparing_dirs() {
    let tmp = test_util::tempdir();
    let node_dir = tmp.path().join("node-0000");
    let err = LocalClusterMap::open_with_configs(
        NodeId::new(0),
        [LocalNodeStoreConfig::new(NodeId::new(0), &node_dir)],
        &[],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap_err();

    assert!(matches!(err, ClusterBuildError::EmptyPgSet));
    assert!(
        !node_dir.exists(),
        "PG validation must run before preparing local node directories"
    );
}

#[test]
fn rejects_duplicate_pg_ids_before_preparing_dirs() {
    let tmp = test_util::tempdir();
    let node_dir = tmp.path().join("node-0000");
    let err = LocalClusterMap::open_with_configs(
        NodeId::new(0),
        [LocalNodeStoreConfig::new(NodeId::new(0), &node_dir)],
        &[0, 1, 1],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap_err();

    assert!(matches!(err, ClusterBuildError::DuplicatePgId { pg_id: 1 }));
    assert!(
        !node_dir.exists(),
        "PG validation must run before preparing local node directories"
    );
}

#[test]
fn rejects_duplicate_local_node_data_dirs() {
    let tmp = test_util::tempdir();
    let shared = tmp.path().join("shared");
    let err = LocalClusterMap::open_with_configs(
        NodeId::new(0),
        [
            LocalNodeStoreConfig::new(NodeId::new(0), &shared),
            LocalNodeStoreConfig::new(NodeId::new(1), &shared),
        ],
        &[0],
        EcShape { k: 1, m: 1 },
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ClusterBuildError::DuplicateDataDir {
            first_node_id: 0,
            duplicate_node_id: 1,
            ..
        }
    ));
    assert!(
        !shared.join("pg-0000").exists(),
        "duplicate directory validation must run before opening PG stores"
    );
}

#[test]
fn rejects_missing_metadata_primary() {
    let tmp = test_util::tempdir();
    let node_dir = tmp.path().join("a");
    let err = LocalClusterMap::open_with_configs(
        NodeId::new(0),
        [LocalNodeStoreConfig::new(NodeId::new(1), &node_dir)],
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ClusterBuildError::MetadataPrimaryNotFound { id: 0 }
    ));
    assert!(
        !node_dir.exists(),
        "metadata primary validation must run before preparing node directories"
    );
}
