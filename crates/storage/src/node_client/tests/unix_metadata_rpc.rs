use super::*;
use crate::node_runtime::clients::unix_sessions::UnixStorageNodeMetadataCommandSession;

#[test]
fn unix_storage_node_client_writes_deletes_and_validates_ack_rows() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || {
        for _ in 0..5 {
            server.accept_one().unwrap();
        }
    });
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let key = ShardKey::new(&[0x55; 16], 11, 0);
    let data_pg_id = DataPgId::new_for_test(PgId::new(0));
    let location = crate::cluster::ShardLocation::new(
        config.cluster_epoch,
        data_pg_id,
        key.shard_index(),
        config.node_id,
    );
    let route = client.open_placed_shard_route(location, &key).unwrap();

    assert_eq!(client.node_id(), NodeId::new(7));
    let ack = route.write_placed_shard(b"remote payload").unwrap();
    let read_back = route.read_placed_shard(ack).unwrap();
    assert_eq!(read_back, b"remote payload");
    client
        .register_written_shard_acks(data_pg_id, &[(&key, ack)])
        .unwrap();
    client
        .validate_written_shard_acks(data_pg_id, &[(&key, ack)])
        .unwrap();
    route.delete_placed_shard().unwrap();
    server_thread.join().unwrap();

    let reopened = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    assert!(matches!(
        reopened.read_shard_file(0, &key),
        Err(StoreError::NotFound)
    ));
    let pg = reopened.get_pg(0).unwrap();
    pg.validate_written_shard_ack(&key, ack).unwrap();
}

#[test]
fn unix_storage_node_client_times_out_waiting_for_response() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let listener = UnixListener::bind(&config.socket_path).unwrap();
    let server_thread = thread::spawn(move || {
        let (_stream, _) = listener.accept().unwrap();
        thread::sleep(STORAGE_RPC_CLIENT_RESPONSE_TIMEOUT + Duration::from_millis(250));
    });
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let key = ShardKey::new(&[0x56; 16], 12, 0);
    let data_pg_id = DataPgId::new_for_test(PgId::new(0));
    let location = crate::cluster::ShardLocation::new(
        config.cluster_epoch,
        data_pg_id,
        key.shard_index(),
        config.node_id,
    );
    let route = client.open_placed_shard_route(location, &key).unwrap();
    let started = Instant::now();
    let err = route
        .read_placed_shard(WriteAck {
            stored_size: 1,
            crc64: 2,
        })
        .unwrap_err();
    assert!(
        started.elapsed() < STORAGE_RPC_CLIENT_RESPONSE_TIMEOUT + Duration::from_secs(2),
        "storage RPC read should time out promptly"
    );
    assert!(matches!(
        err,
        StoreError::StorageRpc {
            operation: "read storage RPC response",
            failure: StorageRpcErrorCode::TransportTimeout,
            ..
        }
    ));
    server_thread.join().unwrap();
}

#[test]
fn storage_rpc_stream_closed_maps_to_transport_closed() {
    let error = storage_rpc_stream_error(
        NodeId::new(7),
        "read storage RPC response",
        StorageRpcStreamError::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "storage-node closed the connection",
        )),
    );
    assert!(matches!(
        error,
        StoreError::StorageRpc {
            node_id: 7,
            operation: "read storage RPC response",
            failure: StorageRpcErrorCode::TransportClosed,
            ..
        }
    ));

    let error = storage_rpc_stream_error(
        NodeId::new(7),
        "read storage RPC response",
        StorageRpcStreamError::Frame(crate::storage_rpc::StorageRpcFrameError::UnknownMagic),
    );
    assert!(matches!(
        error,
        StoreError::StorageRpc {
            node_id: 7,
            operation: "read storage RPC response",
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        }
    ));
}

#[test]
fn unix_storage_node_metadata_session_times_out_waiting_for_response() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let listener = UnixListener::bind(&config.socket_path).unwrap();
    let server_thread = thread::spawn(move || {
        let (_stream, _) = listener.accept().unwrap();
        thread::sleep(STORAGE_RPC_CLIENT_RESPONSE_TIMEOUT + Duration::from_millis(250));
    });
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let started = Instant::now();
    let err = match client.open_metadata_command_critical_section(PgId::new(0)) {
        Ok(_) => panic!("metadata-command session unexpectedly opened without a response"),
        Err(error) => error,
    };
    assert!(
        started.elapsed() < STORAGE_RPC_CLIENT_RESPONSE_TIMEOUT + Duration::from_secs(2),
        "metadata-command RPC read should time out promptly"
    );
    assert!(matches!(
        err,
        StoreError::StorageRpc {
            operation: "read metadata command session RPC response",
            failure: StorageRpcErrorCode::TransportTimeout,
            ..
        }
    ));
    server_thread.join().unwrap();
}

#[test]
fn metadata_command_session_drop_closes_without_release_round_trip() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let listener = UnixListener::bind(&config.socket_path).unwrap();
    let server_thread = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let acquire = read_storage_rpc_frame_from(&mut stream).unwrap();
        assert_eq!(
            acquire.kind,
            StorageRpcMessageKind::MetadataCommandPgLockAcquire
        );
        let acquire_response = StorageRpcFrame {
            request_id: acquire.request_id,
            kind: acquire.kind,
            payload: encode_storage_rpc_success_response(&[]),
        };
        write_storage_rpc_frame_to(&mut stream, &acquire_response).unwrap();
        let started = Instant::now();
        let err = read_storage_rpc_frame_from(&mut stream).unwrap_err();
        assert!(
            started.elapsed() < STORAGE_RPC_CLIENT_RESPONSE_TIMEOUT,
            "drop should close the stream instead of waiting for release response"
        );
        assert!(matches!(err, StorageRpcStreamError::Io(_)));
    });
    {
        let client = UnixStorageNodeClient::new(
            config.node_id,
            config.cluster_epoch,
            config.socket_path.clone(),
        );
        let _session = client
            .open_metadata_command_critical_section(PgId::new(0))
            .unwrap();
    }
    server_thread.join().unwrap();
}

#[test]
fn unix_storage_node_client_reads_cluster_map_history_reference_summary() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        pg.test_insert_object_segment(&crate::ObjectSegmentRecord {
            bucket: crate::BucketName::try_from("unix-history-floor-bucket").unwrap(),
            key: crate::ObjectKey::try_from("segment-object").unwrap(),
            version_id: VersionId::from_u64(1),
            segment_index: 0,
            size: 1024,
            segment_crc64: 0x1234,
            segment_okh: [0x11; 16],
            segment_vid: GenerationId::new(10).unwrap(),
            data_pg_id: 0,
            placement_cluster_epoch: ClusterEpoch::new(6).unwrap(),
            ec_k: 4,
            ec_m: 2,
        })
        .unwrap();
        let backfill = crate::PlacedSegmentShardBackfillWorkItem {
            request: crate::SegmentStoredBytesRequest {
                data_pg_id: 0,
                segment_okh: [0x44; 16],
                segment_vid: GenerationId::new(12).unwrap(),
                stored_size: 4096,
                segment_crc64: 0x9abc,
                ec: EcShape { k: 4, m: 2 },
            },
            source_cluster_epoch: ClusterEpoch::new(3).unwrap(),
            desired_cluster_epoch: ClusterEpoch::new(9).unwrap(),
        };
        pg.record_placed_segment_shard_backfill(&backfill, backfill.request.ec.m, None)
            .unwrap();
        let reclaim_bucket = crate::tests::bucket_name("unix-history-reclaim-claim");
        let reclaim_key = crate::tests::object_key("reclaim-object");
        let reclaim_generation = GenerationId::new(13).unwrap();
        PgMetadataStore::put_object_segments_reclaim(
            &*pg,
            &ObjectSegmentsReclaimRecord {
                bucket: reclaim_bucket.clone(),
                key: reclaim_key.clone(),
                generation_id: reclaim_generation,
                created_at: 2,
                segments: Vec::new(),
            },
        )
        .unwrap();
        PgMetadataStore::acquire_object_payload_reclaim_claim(
            &*pg,
            &reclaim_bucket,
            1,
            &reclaim_key,
            reclaim_generation,
            ObjectPayloadReclaimKind::ObjectSegments,
            "unix-history-reclaim-claim",
            "unix-history-reclaim-owner",
            ClusterEpoch::new(2).unwrap(),
            AdmittedRouteEffectFence::unbounded(ClusterEpoch::new(2).unwrap()),
            2,
            None,
            2,
        )
        .unwrap()
        .expect("durable reclaim root must be claimable");
        pg.refresh_metadata_command_state_digest().unwrap();
    }
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );

    let references = client.cluster_map_history_route_references().unwrap();
    let summary = references.summary();
    assert_eq!(references.len(), 4);
    assert_eq!(
        summary.oldest_live_placement_epoch,
        Some(ClusterEpoch::new(6).unwrap())
    );
    assert_eq!(
        summary.oldest_durable_backfill_epoch,
        Some(ClusterEpoch::new(3).unwrap())
    );
    assert_eq!(
        summary.oldest_object_payload_reclaim_claim_epoch,
        Some(ClusterEpoch::new(2).unwrap())
    );
    assert_eq!(
        summary.oldest_required_epoch(),
        Some(ClusterEpoch::new(2).unwrap())
    );
    server_thread.join().unwrap();
}

#[test]
fn unix_storage_node_client_reads_metadata_command_state_and_acceptance() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let first = test_metadata_command(0, 1);
    let applied = test_metadata_command(0, 2);
    let pending = test_metadata_command(0, 3);
    let bucket = crate::tests::bucket_name("metadata-rpc-bucket");
    let applied_hashes;
    {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        pg.record_metadata_command_abandoned(7, &first).unwrap();
        pg.apply_metadata_command_and_record(7, &applied).unwrap();
        applied_hashes = pg
            .applied_metadata_command_log_entry_hashes(7, &applied)
            .unwrap()
            .unwrap();
        pg.try_insert_pending_metadata_command_slot(7, &pending, Some(&bucket))
            .unwrap();
    }
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || {
        for _ in 0..10 {
            server.accept_one().unwrap();
        }
    });
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );

    let state =
        MetadataCommandInspectionNodeClient::metadata_command_replica_state(&client, PgId::new(0))
            .unwrap();
    let max_log_index = MetadataCommandInspectionNodeClient::max_metadata_command_log_index(
        &client,
        PgId::new(0),
        ClusterEpoch::new(1).unwrap(),
    )
    .unwrap();
    let pending_read = MetadataCommandInspectionNodeClient::pending_metadata_command_envelope(
        &client,
        PgId::new(0),
        ClusterEpoch::new(1).unwrap(),
    )
    .unwrap()
    .unwrap();
    let peering = MetadataCommandPeeringNodeClient::open_metadata_command_peering_route(
        &client,
        PgId::new(0),
        ClusterEpoch::new(1).unwrap(),
    )
    .unwrap();
    let replay_state = peering
        .validate_metadata_command_replay_state_preserving_pending_slot()
        .unwrap();
    let remote_hashes =
        MetadataCommandInspectionNodeClient::applied_metadata_command_log_entry_hashes(
            &client,
            PgId::new(0),
            &applied,
        )
        .unwrap();
    let matching =
        MetadataCommandInspectionNodeClient::has_matching_applied_metadata_command_log_entry(
            &client,
            PgId::new(0),
            &applied,
            applied_hashes.0,
        )
        .unwrap();
    let abandoned = MetadataCommandInspectionNodeClient::metadata_command_abandoned(
        &client,
        PgId::new(0),
        &first,
    )
    .unwrap();
    let can_initialize =
        MetadataCommandInspectionNodeClient::metadata_command_replica_state_can_initialize(
            &client,
            PgId::new(0),
            ClusterEpoch::new(1).unwrap(),
        )
        .unwrap();
    let next_conflict = MetadataCommandNodeClient::next_metadata_command_id_at_least(
        &client,
        PgId::new(0),
        ClusterEpoch::new(1).unwrap(),
        MetadataCommandLogIndex::new(5).unwrap(),
    )
    .unwrap_err();
    let acceptance = MetadataCommandInspectionNodeClient::metadata_command_acceptance(
        &client,
        PgId::new(0),
        &applied,
    )
    .unwrap();

    assert_eq!(state.cluster_epoch, ClusterEpoch::INITIAL);
    assert_eq!(state.applied_log_index, 2);
    assert_eq!(max_log_index, 2);
    assert_eq!(pending_read.command_bytes(), pending.command_bytes());
    assert_eq!(replay_state.applied_log_index, 2);
    assert_eq!(remote_hashes, Some(applied_hashes));
    assert!(matching);
    assert!(abandoned);
    assert!(!can_initialize);
    assert!(matches!(
        next_conflict,
        StoreError::MetadataCommandLogConflict {
            pg_id: 0,
            log_index: 3,
            ..
        }
    ));
    assert_eq!(acceptance, MetadataCommandAcceptance::AlreadyApplied);
    server_thread.join().unwrap();
}

#[test]
fn unix_storage_node_client_adopts_metadata_transfer_state() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let command = test_metadata_command(0, 1);
    let expected_state_digest;
    {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        pg.apply_metadata_command_and_record(7, &command).unwrap();
        expected_state_digest = pg.metadata_command_replica_state().unwrap().state_digest;
    }

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    config.cluster_epoch = destination_epoch;
    config.pg_routes[0].cluster_epoch = destination_epoch;
    config.pg_routes[0].state = crate::PgState::Peering;
    config.pg_routes[0].metadata_transfer_destination_epoch = Some(destination_epoch);
    let rebased = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            destination_epoch,
            PgId::new(0),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        command.payload().clone(),
    );
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || {
        server.accept_one().unwrap();
    });
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );

    let peering = MetadataCommandPeeringNodeClient::open_metadata_command_peering_route(
        &client,
        PgId::new(0),
        destination_epoch,
    )
    .unwrap();
    let state = peering
        .adopt_metadata_transfer_state_from_rebased_commands(
            &[MetadataTransferCommand {
                command: rebased,
                pre_state_digest: 0,
                post_state_digest: expected_state_digest,
            }],
            expected_state_digest,
        )
        .unwrap();
    server_thread.join().unwrap();

    assert_eq!(state.cluster_epoch, destination_epoch);
    assert_eq!(state.applied_log_index, 1);
    assert_eq!(state.state_digest, expected_state_digest);
    let reopened = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let persisted = reopened
        .get_pg(0)
        .unwrap()
        .metadata_command_replica_state()
        .unwrap();
    assert_eq!(persisted, state);
}

fn test_metadata_checkpoint_with_bucket(
    bucket_name: &str,
) -> (
    BucketName,
    crate::node_runtime::pg_store::MetadataCommandCheckpoint,
) {
    let source_tmp = test_util::tempdir();
    let source_node = SharedStorageNode::open(source_tmp.path(), &[0]).unwrap();
    let bucket = crate::tests::bucket_name(bucket_name);
    let checkpoint = {
        let source_pg = source_node.get_pg(0).unwrap();
        let owner = OwnerIdentity::from_principal("owner");
        source_pg
            .create_bucket_with_config(&CreateBucketConfig {
                name: bucket.as_str(),
                owner_principal: &owner.principal,
                owner_canonical_id: &owner.canonical_id,
                acl_grants: &s3_types::AclGrants::default(),
                public_read: false,
                public_write: false,
                versioning: s3_types::BucketVersioningState::Disabled,
                object_lock: s3_types::BucketObjectLockConfig::default(),
                ownership_controls: crate::BucketOwnershipControls {
                    object_ownership: crate::BucketObjectOwnership::ObjectWriter,
                },
            })
            .unwrap();
        source_pg
            .put_bucket_subresource(
                &bucket,
                crate::PutBucketSubresource {
                    kind: BucketSubresourceKind::Lifecycle,
                    body: "<LifecycleConfiguration/>",
                    aux: crate::BucketSubresourceAux::None,
                },
            )
            .unwrap();
        source_pg.refresh_metadata_command_state_digest().unwrap();
        source_pg
            .metadata_command_checkpoint(7, ClusterEpoch::INITIAL)
            .unwrap()
    };
    (bucket, checkpoint)
}

#[test]
fn unix_storage_node_client_exports_metadata_command_checkpoint() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    config.pg_routes[0].state = crate::PgState::Peering;
    let bucket = crate::tests::bucket_name("unix-metadata-checkpoint-export");
    let expected_checkpoint = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        let owner = OwnerIdentity::from_principal("owner");
        pg.create_bucket_with_config(&CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &s3_types::AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: s3_types::BucketVersioningState::Disabled,
            object_lock: s3_types::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        pg.metadata_command_checkpoint(7, ClusterEpoch::INITIAL)
            .unwrap()
    };
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || {
        server.accept_one().unwrap();
    });
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );

    let checkpoint = MetadataCommandInspectionNodeClient::metadata_command_checkpoint(
        &client,
        PgId::new(0),
        ClusterEpoch::INITIAL,
    )
    .unwrap();
    server_thread.join().unwrap();

    assert_eq!(checkpoint, expected_checkpoint);
}

#[test]
fn unix_storage_node_client_records_current_metadata_command_checkpoint() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let bucket = crate::tests::bucket_name("unix-metadata-checkpoint-record-current");
    let expected_state = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        let owner = OwnerIdentity::from_principal("owner");
        pg.create_bucket_with_config(&CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &s3_types::AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: s3_types::BucketVersioningState::Disabled,
            object_lock: s3_types::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        pg.metadata_command_replica_state().unwrap()
    };
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || {
        server.accept_one().unwrap();
    });
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );

    let recorded_state = MetadataCommandNodeClient::record_current_metadata_command_checkpoint(
        &client,
        PgId::new(0),
        ClusterEpoch::INITIAL,
    )
    .unwrap();
    server_thread.join().unwrap();

    assert_eq!(recorded_state, expected_state);

    let reopened = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let candidates = reopened
        .get_pg(0)
        .unwrap()
        .metadata_command_checkpoint_candidates(ClusterEpoch::INITIAL, u64::MAX, 1)
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates[0].applied_log_index,
        expected_state.applied_log_index
    );
    assert_eq!(
        candidates[0].applied_log_hash,
        expected_state.applied_log_hash
    );
    assert_eq!(candidates[0].state_digest, expected_state.state_digest);
}

#[test]
fn unix_storage_node_client_lists_metadata_command_checkpoint_candidates() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let bucket = crate::tests::bucket_name("unix-metadata-checkpoint-candidates");
    let (first, second) = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        let owner = OwnerIdentity::from_principal("owner");
        pg.create_bucket_with_config(&CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &s3_types::AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: s3_types::BucketVersioningState::Disabled,
            object_lock: s3_types::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        let first = pg
            .record_current_metadata_command_checkpoint(7, ClusterEpoch::INITIAL)
            .unwrap();
        pg.put_bucket_subresource(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Lifecycle,
                body: "<LifecycleConfiguration/>",
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        let second = pg
            .record_current_metadata_command_checkpoint(7, ClusterEpoch::INITIAL)
            .unwrap();
        (first, second)
    };
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || {
        server.accept_one().unwrap();
    });
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );

    let candidates = MetadataCommandInspectionNodeClient::metadata_command_checkpoint_candidates(
        &client,
        PgId::new(0),
        ClusterEpoch::INITIAL,
        u64::MAX,
        2,
    )
    .unwrap();
    server_thread.join().unwrap();

    assert_eq!(candidates.len(), 2);
    assert!(candidates.contains(&first));
    assert!(candidates.contains(&second));
}

#[test]
fn unix_storage_node_client_exports_active_metadata_command_checkpoint() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || {
        server.accept_one().unwrap();
    });
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );

    let checkpoint = MetadataCommandInspectionNodeClient::metadata_command_checkpoint(
        &client,
        PgId::new(0),
        ClusterEpoch::INITIAL,
    )
    .unwrap();
    server_thread.join().unwrap();

    assert_eq!(checkpoint.cluster_epoch, ClusterEpoch::INITIAL);
    assert_eq!(checkpoint.pg_id, PgId::new(0));
    assert_eq!(checkpoint.applied_log_index, 0);
    assert_eq!(checkpoint.applied_log_hash, 0);
    checkpoint.verify().unwrap();
}

#[test]
fn unix_storage_node_client_installs_metadata_transfer_checkpoint_base() {
    let (bucket, checkpoint) = test_metadata_checkpoint_with_bucket("unix-metadata-checkpoint");

    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let destination_epoch = ClusterEpoch::new(2).unwrap();
    config.cluster_epoch = destination_epoch;
    config.pg_routes[0].cluster_epoch = destination_epoch;
    config.pg_routes[0].state = crate::PgState::Peering;
    config.pg_routes[0].metadata_transfer_destination_epoch = Some(destination_epoch);
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || {
        server.accept_one().unwrap();
    });
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );

    let peering = MetadataCommandPeeringNodeClient::open_metadata_command_peering_route(
        &client,
        PgId::new(0),
        destination_epoch,
    )
    .unwrap();
    let state = peering
        .install_metadata_transfer_checkpoint_base(&checkpoint)
        .unwrap();
    server_thread.join().unwrap();

    assert_eq!(state.cluster_epoch, destination_epoch);
    assert_eq!(state.applied_log_index, 0);
    assert_eq!(state.applied_log_hash, 0);
    assert_eq!(state.state_digest, checkpoint.state_digest);

    let reopened = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let pg = reopened.get_pg(0).unwrap();
    assert_eq!(pg.head_bucket(&bucket).unwrap().name, bucket);
    let lifecycle = pg
        .get_bucket_subresource(&bucket, BucketSubresourceKind::Lifecycle)
        .unwrap()
        .unwrap();
    assert_eq!(lifecycle.body, "<LifecycleConfiguration/>");
}

#[test]
fn unix_storage_node_client_rejects_active_metadata_transfer_checkpoint_base_install() {
    let (_bucket, checkpoint) =
        test_metadata_checkpoint_with_bucket("unix-metadata-checkpoint-active");
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || {
        server.accept_one().unwrap();
    });
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );

    let peering = MetadataCommandPeeringNodeClient::open_metadata_command_peering_route(
        &client,
        PgId::new(0),
        config.cluster_epoch,
    )
    .unwrap();
    let err = peering
        .install_metadata_transfer_checkpoint_base(&checkpoint)
        .unwrap_err();
    server_thread.join().unwrap();

    assert!(matches!(
        err,
        StoreError::StorageRpc {
            operation: "metadata command transfer checkpoint base install",
            detail,
            ..
        } if detail.as_str().contains("route is active")
    ));
}

#[test]
fn unix_storage_node_client_rejects_empty_metadata_transfer_adoption() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let command = test_metadata_command(0, 1);
    let before;
    {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        pg.apply_metadata_command_and_record(7, &command).unwrap();
        before = pg.metadata_command_replica_state().unwrap();
    }

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    config.cluster_epoch = destination_epoch;
    config.pg_routes[0].cluster_epoch = destination_epoch;
    config.pg_routes[0].state = crate::PgState::Peering;
    config.pg_routes[0].metadata_transfer_destination_epoch = Some(destination_epoch);
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || {
        server.accept_one().unwrap();
    });
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );

    let peering = MetadataCommandPeeringNodeClient::open_metadata_command_peering_route(
        &client,
        PgId::new(0),
        destination_epoch,
    )
    .unwrap();
    let err = peering
        .adopt_metadata_transfer_state_from_rebased_commands(&[], before.state_digest)
        .unwrap_err();
    server_thread.join().unwrap();

    assert!(matches!(
        err,
        StoreError::StorageRpc {
            operation: "metadata command transfer state adopt",
            detail,
            ..
        } if detail.as_str().contains("requires at least one retained command")
    ));
    let reopened = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let after = reopened
        .get_pg(0)
        .unwrap()
        .metadata_command_replica_state()
        .unwrap();
    assert_eq!(after, before);
}

#[test]
fn unix_storage_node_client_applies_metadata_command_idempotently() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || {
        for _ in 0..2 {
            server.accept_one().unwrap();
        }
    });
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let command = test_metadata_command(0, 1);

    let applied = MetadataCommandNodeClient::apply_metadata_command_and_record(
        &client,
        PgId::new(0),
        &command,
    )
    .unwrap();
    let retried = MetadataCommandNodeClient::apply_metadata_command_and_record(
        &client,
        PgId::new(0),
        &command,
    )
    .unwrap();
    server_thread.join().unwrap();

    assert_eq!(applied.applied_log_index, 1);
    assert_eq!(retried.applied_log_index, 1);
    let reopened = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let state = reopened
        .get_pg(0)
        .unwrap()
        .metadata_command_replica_state()
        .unwrap();
    assert_eq!(state.applied_log_index, 1);
}

#[test]
fn unix_storage_node_client_inserts_pending_metadata_command_slot_idempotently() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || {
        for _ in 0..4 {
            server.accept_one().unwrap();
        }
    });
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let command = test_metadata_command(0, 1);
    let replacement = test_metadata_command(0, 2);
    let bucket = crate::tests::bucket_name("metadata-rpc-bucket");

    MetadataCommandNodeClient::try_insert_pending_metadata_command_slot(
        &client,
        PgId::new(0),
        &command,
        Some(&bucket),
    )
    .unwrap();
    MetadataCommandNodeClient::try_insert_pending_metadata_command_slot(
        &client,
        PgId::new(0),
        &command,
        Some(&bucket),
    )
    .unwrap();
    let recovery_session =
        MetadataCommandRecoveryNodeClient::open_metadata_command_recovery_critical_section(
            &client,
            PgId::new(0),
            config.cluster_epoch,
        )
        .unwrap();
    assert!(
        MetadataCommandRecoveryCriticalSection::replace_pending_metadata_command_slot_for_reissue(
            recovery_session.as_ref(),
            &command,
            &replacement,
            Some(&bucket),
        )
        .unwrap()
    );
    assert!(
        MetadataCommandRecoveryCriticalSection::replace_pending_metadata_command_slot_for_reissue(
            recovery_session.as_ref(),
            &command,
            &replacement,
            Some(&bucket),
        )
        .unwrap(),
        "replacing after a lost response should be idempotent"
    );
    drop(recovery_session);
    let conflict = MetadataCommandNodeClient::try_insert_pending_metadata_command_slot(
        &client,
        PgId::new(0),
        &test_metadata_command(0, 3),
        Some(&bucket),
    )
    .unwrap_err();
    assert!(matches!(
        conflict,
        StoreError::MetadataCommandPendingConflict {
            pg_id: 0,
            existing_log_index: 2,
            candidate_log_index: 3,
            ..
        }
    ));
    server_thread.join().unwrap();

    let reopened = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let pending = reopened
        .get_pg(0)
        .unwrap()
        .pending_metadata_command_envelope(7, ClusterEpoch::new(1).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(pending.command_bytes(), replacement.command_bytes());
}

#[test]
fn unix_recovery_critical_section_rejects_command_for_another_pg_without_mutation() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let recovery =
        MetadataCommandRecoveryNodeClient::open_metadata_command_recovery_critical_section(
            &client,
            PgId::new(0),
            config.cluster_epoch,
        )
        .unwrap();

    let error = recovery
        .record_metadata_command_abandoned(&test_metadata_command(1, 1))
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        }
    ));
    drop(recovery);
    server_thread.join().unwrap();

    let reopened = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    assert_eq!(
        reopened
            .get_pg(0)
            .unwrap()
            .max_metadata_command_log_index(config.cluster_epoch)
            .unwrap(),
        0
    );
}

#[test]
fn unix_active_critical_section_rejects_command_for_another_pg_without_mutation() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let active = MetadataCommandNodeClient::open_metadata_command_critical_section(
        &client,
        PgId::new(0),
        config.cluster_epoch,
    )
    .unwrap();

    let error = active
        .apply_metadata_command_and_record(&test_metadata_command(1, 1))
        .unwrap_err();
    match error {
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            detail,
            ..
        }) => assert!(
            detail
                .as_str()
                .contains("command PG does not match RPC route"),
            "unexpected route-mismatch diagnostic: {}",
            detail.as_str()
        ),
        other => panic!("expected route payload rejection, got {other:?}"),
    }
    drop(active);
    server_thread.join().unwrap();

    let reopened = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    assert_eq!(
        reopened
            .get_pg(0)
            .unwrap()
            .max_metadata_command_log_index(config.cluster_epoch)
            .unwrap(),
        0
    );
}

#[test]
fn unix_peering_route_rejects_redirected_commands_before_rpc() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.pg_routes[0].state = crate::PgState::Peering;
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let peering = MetadataCommandPeeringNodeClient::open_metadata_command_peering_route(
        &client,
        PgId::new(0),
        config.cluster_epoch,
    )
    .unwrap();

    let wrong_pg_command = test_metadata_command(1, 1);
    let error = peering
        .replay_metadata_command_for_peering(&wrong_pg_command)
        .unwrap_err();
    assert!(matches!(
        error,
        BucketSnapshotLoadError::Store(StoreError::MetadataCommandWrongPg {
            command_pg_id: 1,
            target_pg_id: 0,
            ..
        })
    ));

    let command = test_metadata_command(0, 1);
    let future_epoch = ClusterEpoch::new(config.cluster_epoch.get() + 1).unwrap();
    let future_command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(future_epoch, PgId::new(0), command.id().log_index()),
        command.payload().clone(),
    );
    let error = peering
        .adopt_metadata_transfer_state_from_rebased_commands(
            &[MetadataTransferCommand {
                command: future_command,
                pre_state_digest: 0,
                post_state_digest: 1,
            }],
            1,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::StaleMetadataOperation {
            pg_id: 0,
            operation_epoch,
            current_epoch,
        } if operation_epoch == future_epoch && current_epoch == config.cluster_epoch
    ));

    let (_, checkpoint) = test_metadata_checkpoint_with_bucket("unix-peering-route-checkpoint");
    let mut wrong_pg_checkpoint = checkpoint.clone();
    wrong_pg_checkpoint.pg_id = PgId::new(1);
    let error = peering
        .install_metadata_transfer_checkpoint_base(&wrong_pg_checkpoint)
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::MetadataCheckpointInvalid {
            pg_id: 0,
            cluster_epoch,
            ..
        } if cluster_epoch == config.cluster_epoch
    ));
}

#[test]
fn unix_shard_scavenger_observation_route_rejects_foreign_subject_before_rpc() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let route = client
        .open_shard_scavenger_observation_route(DataPgId::new_for_test(PgId::new(0)))
        .unwrap();
    let observation = test_shard_scavenger_observation(1, 0x43);

    for error in [
        route
            .record_shard_scavenger_observation(&observation)
            .unwrap_err(),
        route
            .resolve_shard_scavenger_observation(&observation.key)
            .unwrap_err(),
    ] {
        assert!(matches!(
            error,
            StoreError::ShardScavengerObservationWrongPg {
                store_pg_id: 0,
                observation_pg_id: 1,
            }
        ));
    }
}

#[test]
fn unix_shard_scavenger_observation_route_rejects_foreign_list_response() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let listener = UnixListener::bind(&config.socket_path).unwrap();
    let observation = test_shard_scavenger_observation(1, 0x44);
    let response_observation = ShardScavengerObservation {
        key: observation.key,
        first_seen_at: 10,
        last_seen_at: 11,
        observation_count: 1,
        data_size: observation.data_size,
        crc64: observation.crc64,
        file_exists: observation.file_exists,
        shard_row_exists: observation.shard_row_exists,
        reason: observation.reason,
        last_error: observation.last_error,
        resolved_at: None,
    };
    let server_thread = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let request = read_storage_rpc_frame_from(&mut stream).unwrap();
        assert_eq!(
            request.kind,
            StorageRpcMessageKind::ShardScavengerObservations
        );
        let payload = encode_scavenger_observations_response(&[response_observation]);
        write_storage_rpc_frame_to(
            &mut stream,
            &StorageRpcFrame {
                request_id: request.request_id,
                kind: request.kind,
                payload: encode_storage_rpc_success_response(&payload),
            },
        )
        .unwrap();
    });
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let route = client
        .open_shard_scavenger_observation_route(DataPgId::new_for_test(PgId::new(0)))
        .unwrap();

    let error = route.list_shard_scavenger_observations().unwrap_err();
    server_thread.join().unwrap();

    assert!(matches!(
        error,
        StoreError::StorageRpc {
            operation: "validate shard scavenger observations response",
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        }
    ));
}

#[test]
fn unix_shard_ack_route_rejects_foreign_repair_and_backfill_list_responses() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let listener = UnixListener::bind(&config.socket_path).unwrap();
    let repair = PlacedSegmentShardRepairRecord {
        work_item: test_shard_repair_work_item(1, 31),
        first_seen_at: 10,
        last_seen_at: 11,
        observation_count: 1,
        last_error: None,
    };
    let backfill = PlacedSegmentShardBackfillRecord {
        work_item: test_shard_backfill_work_item(1, 32),
        remaining_tolerance: 2,
        first_seen_at: 12,
        last_seen_at: 13,
        observation_count: 1,
        last_error: None,
    };
    let repair_payload =
        crate::storage_rpc::encode_placed_segment_shard_repairs_response(&[repair]).unwrap();
    let backfill_payload =
        crate::storage_rpc::encode_placed_segment_shard_backfills_response(&[backfill]).unwrap();
    let server_thread = thread::spawn(move || {
        for expected_kind in [
            StorageRpcMessageKind::PlacedSegmentShardRepairs,
            StorageRpcMessageKind::PlacedSegmentShardBackfills,
        ] {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_storage_rpc_frame_from(&mut stream).unwrap();
            assert_eq!(request.kind, expected_kind);
            let payload = match expected_kind {
                StorageRpcMessageKind::PlacedSegmentShardRepairs => repair_payload.clone(),
                StorageRpcMessageKind::PlacedSegmentShardBackfills => backfill_payload.clone(),
                _ => unreachable!(),
            };
            write_storage_rpc_frame_to(
                &mut stream,
                &StorageRpcFrame {
                    request_id: request.request_id,
                    kind: request.kind,
                    payload: encode_storage_rpc_success_response(&payload),
                },
            )
            .unwrap();
        }
    });
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let route = client
        .open_shard_ack_route(config.cluster_epoch, DataPgId::new_for_test(PgId::new(0)))
        .unwrap();

    for (error, operation) in [
        (
            route.list_placed_segment_shard_repairs().unwrap_err(),
            "validate placed segment shard repairs response",
        ),
        (
            route.list_placed_segment_shard_backfills().unwrap_err(),
            "validate placed segment shard backfills response",
        ),
    ] {
        assert!(matches!(
            error,
            StoreError::StorageRpc {
                operation: observed_operation,
                failure: StorageRpcErrorCode::PayloadDecode,
                ..
            } if observed_operation == operation
        ));
    }
    server_thread.join().unwrap();
}

#[test]
fn unix_shard_ack_route_binds_acquired_claim_responses_to_requests() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let listener = UnixListener::bind(&config.socket_path).unwrap();
    let repair_acquire = PlacedSegmentShardRepairClaimAcquire {
        claim_id: "repair-claim".to_string(),
        owner_token: "repair-owner".to_string(),
        cluster_epoch: config.cluster_epoch,
        claimed_at: 30,
        lease_deadline: 40,
        now: 30,
    };
    let repair_claim = PlacedSegmentShardRepairClaimRecord {
        work_item: test_shard_repair_work_item(0, 41),
        claim_id: repair_acquire.claim_id.clone(),
        owner_token: repair_acquire.owner_token.clone(),
        cluster_epoch: repair_acquire.cluster_epoch,
        claimed_at: repair_acquire.claimed_at,
        lease_deadline: Some(repair_acquire.lease_deadline),
        attempt_count: 1,
        last_error: None,
    };
    let backfill_acquire = PlacedSegmentShardBackfillClaimAcquire {
        claim_id: "backfill-claim".to_string(),
        owner_token: "backfill-owner".to_string(),
        cluster_epoch: config.cluster_epoch,
        claimed_at: 50,
        lease_deadline: 60,
        now: 50,
    };
    let backfill_claim = PlacedSegmentShardBackfillClaimRecord {
        work_item: test_shard_backfill_work_item(0, 42),
        remaining_tolerance: 2,
        claim_id: backfill_acquire.claim_id.clone(),
        owner_token: backfill_acquire.owner_token.clone(),
        cluster_epoch: backfill_acquire.cluster_epoch,
        claimed_at: backfill_acquire.claimed_at,
        lease_deadline: Some(backfill_acquire.lease_deadline),
        attempt_count: 1,
        last_error: None,
    };

    let mut repair_claims = Vec::new();
    let mut mismatched = repair_claim.clone();
    mismatched.claim_id = "other-repair-claim".to_string();
    repair_claims.push(mismatched);
    let mut mismatched = repair_claim.clone();
    mismatched.owner_token = "other-repair-owner".to_string();
    repair_claims.push(mismatched);
    let mut mismatched = repair_claim.clone();
    mismatched.claimed_at += 1;
    repair_claims.push(mismatched);
    let mut mismatched = repair_claim;
    mismatched.lease_deadline = Some(repair_acquire.lease_deadline + 1);
    repair_claims.push(mismatched);

    let mut backfill_claims = Vec::new();
    let mut mismatched = backfill_claim.clone();
    mismatched.claim_id = "other-backfill-claim".to_string();
    backfill_claims.push(mismatched);
    let mut mismatched = backfill_claim.clone();
    mismatched.owner_token = "other-backfill-owner".to_string();
    backfill_claims.push(mismatched);
    let mut mismatched = backfill_claim.clone();
    mismatched.claimed_at += 1;
    backfill_claims.push(mismatched);
    let mut mismatched = backfill_claim;
    mismatched.lease_deadline = Some(backfill_acquire.lease_deadline + 1);
    backfill_claims.push(mismatched);

    let mut responses = Vec::new();
    for claim in repair_claims {
        responses.push((
            StorageRpcMessageKind::PlacedSegmentShardRepairClaimAcquire,
            encode_placed_segment_shard_repair_claim_optional_record_response(
                &StorageRpcPlacedSegmentShardRepairClaimOptionalRecordResponse {
                    record: Some(claim),
                },
            )
            .unwrap(),
        ));
    }
    for claim in backfill_claims {
        responses.push((
            StorageRpcMessageKind::PlacedSegmentShardBackfillClaimAcquire,
            encode_placed_segment_shard_backfill_claim_optional_record_response(
                &StorageRpcPlacedSegmentShardBackfillClaimOptionalRecordResponse {
                    record: Some(claim),
                },
            )
            .unwrap(),
        ));
    }
    let server_thread = thread::spawn(move || {
        for (expected_kind, payload) in responses {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_storage_rpc_frame_from(&mut stream).unwrap();
            assert_eq!(request.kind, expected_kind);
            write_storage_rpc_frame_to(
                &mut stream,
                &StorageRpcFrame {
                    request_id: request.request_id,
                    kind: request.kind,
                    payload: encode_storage_rpc_success_response(&payload),
                },
            )
            .unwrap();
        }
    });
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let route = client
        .open_shard_ack_route(config.cluster_epoch, DataPgId::new_for_test(PgId::new(0)))
        .unwrap();

    for _ in 0..4 {
        assert!(matches!(
            route
                .acquire_placed_segment_shard_repair_claim(&repair_acquire)
                .unwrap_err(),
            StoreError::StorageRpc {
                operation: "validate placed segment shard repair claim response",
                failure: StorageRpcErrorCode::PayloadDecode,
                ..
            }
        ));
    }
    for _ in 0..4 {
        assert!(matches!(
            route
                .acquire_placed_segment_shard_backfill_claim(&backfill_acquire)
                .unwrap_err(),
            StoreError::StorageRpc {
                operation: "validate placed segment shard backfill claim response",
                failure: StorageRpcErrorCode::PayloadDecode,
                ..
            }
        ));
    }
    server_thread.join().unwrap();
}

#[test]
fn unix_storage_node_client_inserts_bucket_control_pending_slot_idempotently() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || {
        for _ in 0..2 {
            server.accept_one().unwrap();
        }
    });
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let command = test_metadata_command(0, 1);
    let bucket = crate::tests::bucket_name("metadata-rpc-bucket");

    assert!(
        MetadataCommandNodeClient::try_insert_bucket_control_pending_metadata_command_slot(
            &client,
            PgId::new(0),
            &command,
            &bucket,
        )
        .unwrap()
    );
    assert!(
        MetadataCommandNodeClient::try_insert_bucket_control_pending_metadata_command_slot(
            &client,
            PgId::new(0),
            &command,
            &bucket,
        )
        .unwrap(),
        "retrying after a lost response should observe the existing exact slot"
    );
    server_thread.join().unwrap();

    let reopened = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let pending = reopened
        .get_pg(0)
        .unwrap()
        .pending_metadata_command_slot(7, ClusterEpoch::new(1).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(pending.command_bytes, command.command_bytes());
    assert_eq!(pending.scope_bucket.as_ref(), Some(&bucket));
}

#[test]
fn unix_storage_node_client_removes_pending_metadata_command_slot_idempotently() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let command = test_metadata_command(0, 1);
    let bucket = crate::tests::bucket_name("metadata-rpc-bucket");
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || {
        for _ in 0..4 {
            server.accept_one().unwrap();
        }
    });
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );

    MetadataCommandNodeClient::try_insert_pending_metadata_command_slot(
        &client,
        PgId::new(0),
        &command,
        Some(&bucket),
    )
    .unwrap();
    MetadataCommandRecoveryNodeClient::open_metadata_command_recovery_critical_section(
        &client,
        PgId::new(0),
        config.cluster_epoch,
    )
    .unwrap()
    .record_metadata_command_abandoned(&command)
    .unwrap();
    assert!(
        MetadataCommandNodeClient::remove_pending_metadata_command_slot(
            &client,
            PgId::new(0),
            &command
        )
        .unwrap()
    );
    assert!(
        !MetadataCommandNodeClient::remove_pending_metadata_command_slot(
            &client,
            PgId::new(0),
            &command
        )
        .unwrap()
    );
    server_thread.join().unwrap();

    let reopened = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    assert!(reopened
        .get_pg(0)
        .unwrap()
        .pending_metadata_command_envelope(7, ClusterEpoch::new(1).unwrap())
        .unwrap()
        .is_none());
}

#[test]
fn unix_recovery_replica_route_rejects_crossed_authority_before_rpc() {
    let client = test_unix_storage_node_client();
    let source = test_metadata_command(0, 1);
    let abandoned = test_metadata_command(0, 2);
    let command = test_metadata_command(0, 3);
    let wrong_pg = test_metadata_command(1, 4);
    let future_epoch = ClusterEpoch::new(client.cluster_epoch.get() + 1).unwrap();
    let future_command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(future_epoch, PgId::new(0), command.id().log_index()),
        command.payload().clone(),
    );
    let (certificate_source, reissued, cleanup, unrelated) =
        test_metadata_command_recovery_chain(0);
    let cleanup_before_abandoned =
        MetadataCommandEnvelope::new(reissued.id(), cleanup.payload().clone());

    assert!(matches!(
        client
            .open_metadata_command_recovery_replica_apply_route(
                PgId::new(0),
                future_epoch,
                &source,
                Some(&abandoned),
                &command,
            )
            .err()
            .expect("foreign recovery epoch must be rejected before RPC"),
        StoreError::StalePayloadOperation {
            operation_epoch,
            current_epoch,
            ..
        } if operation_epoch == future_epoch && current_epoch == client.cluster_epoch
    ));

    for (authorized_source, abandoned_source, command) in [
        (&wrong_pg, Some(&abandoned), &command),
        (&source, Some(&wrong_pg), &command),
        (&source, Some(&abandoned), &wrong_pg),
    ] {
        assert!(matches!(
            client
                .open_metadata_command_recovery_replica_apply_route(
                    PgId::new(0),
                    client.cluster_epoch,
                    authorized_source,
                    abandoned_source,
                    command,
                )
                .err()
                .expect("crossed recovery command subject must be rejected before RPC"),
            StoreError::MetadataCommandWrongPg {
                command_pg_id: 1,
                target_pg_id: 0,
                ..
            }
        ));
    }
    assert!(matches!(
        client
            .open_metadata_command_recovery_replica_apply_route(
                PgId::new(0),
                client.cluster_epoch,
                &source,
                Some(&abandoned),
                &future_command,
            )
            .err()
            .expect("crossed recovery command epoch must be rejected before RPC"),
        StoreError::StaleMetadataOperation {
            operation_epoch,
            current_epoch,
            ..
        } if operation_epoch == future_epoch && current_epoch == client.cluster_epoch
    ));
    assert!(matches!(
        client
            .open_metadata_command_recovery_replica_abandon_route(
                PgId::new(0),
                client.cluster_epoch,
                &wrong_pg,
                Some(&abandoned),
                &command,
            )
            .err()
            .expect("crossed tombstone recovery subject must be rejected before RPC"),
        StoreError::MetadataCommandWrongPg {
            command_pg_id: 1,
            target_pg_id: 0,
            ..
        }
    ));

    for (authorized_source, abandoned_source, command) in [
        (&certificate_source, None, &unrelated),
        (&certificate_source, Some(&reissued), &reissued),
        (&certificate_source, None, &cleanup),
        (&certificate_source, Some(&unrelated), &cleanup),
        (
            &certificate_source,
            Some(&reissued),
            &cleanup_before_abandoned,
        ),
    ] {
        assert!(matches!(
            client
                .open_metadata_command_recovery_replica_apply_route(
                    PgId::new(0),
                    client.cluster_epoch,
                    authorized_source,
                    abandoned_source,
                    command,
                )
                .err()
                .expect("invalid recovery certificate must reject Unix replica apply before RPC"),
            StoreError::RouteCapabilitySubjectMismatch {
                operation: "open metadata command recovery replica route"
            }
        ));
        assert!(matches!(
            client
                .open_metadata_command_recovery_replica_abandon_route(
                    PgId::new(0),
                    client.cluster_epoch,
                    authorized_source,
                    abandoned_source,
                    command,
                )
                .err()
                .expect(
                    "invalid recovery certificate must reject Unix replica abandonment before RPC"
                ),
            StoreError::RouteCapabilitySubjectMismatch {
                operation: "open metadata command recovery replica route"
            }
        ));
    }
}

#[test]
fn unix_storage_node_client_records_abandoned_metadata_command_idempotently() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let command = test_metadata_command(0, 1);

    let recovery =
        MetadataCommandRecoveryNodeClient::open_metadata_command_recovery_critical_section(
            &client,
            PgId::new(0),
            config.cluster_epoch,
        )
        .unwrap();
    let first = recovery
        .record_metadata_command_abandoned(&command)
        .unwrap();
    let second = recovery
        .record_metadata_command_abandoned(&command)
        .unwrap();
    drop(recovery);
    server_thread.join().unwrap();

    assert_eq!(first, second);
    let reopened = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    assert!(reopened
        .get_pg(0)
        .unwrap()
        .metadata_command_abandoned(7, &command)
        .unwrap());
}

#[test]
fn unix_storage_node_client_rejects_mismatched_next_id_conflict_response() {
    fn next_id_error_from_fake_response(
        outcome: StorageRpcMetadataCommandNextIdOutcome,
    ) -> StoreError {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let listener = UnixListener::bind(&socket_path).unwrap();
        let join = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_storage_rpc_frame_from(&mut stream).unwrap();
            let payload = encode_metadata_command_next_id_response(
                &StorageRpcMetadataCommandNextIdResponse { outcome },
            );
            let response = StorageRpcFrame {
                request_id: request.request_id,
                kind: request.kind,
                payload: encode_storage_rpc_success_response(&payload),
            };
            write_storage_rpc_frame_to(&mut stream, &response).unwrap();
        });
        let client =
            UnixStorageNodeClient::new(NodeId::new(7), ClusterEpoch::new(1).unwrap(), socket_path);

        let err = MetadataCommandNodeClient::next_metadata_command_id_at_least(
            &client,
            PgId::new(0),
            ClusterEpoch::new(1).unwrap(),
            MetadataCommandLogIndex::new(1).unwrap(),
        )
        .unwrap_err();

        join.join().unwrap();
        err
    }

    let wrong_route =
        next_id_error_from_fake_response(StorageRpcMetadataCommandNextIdOutcome::LogConflict {
            node_id: 7,
            pg_id: 1,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            log_index: 1,
        });
    assert!(matches!(
        wrong_route,
        StoreError::StorageRpc {
            operation: "decode metadata command next id response",
            ..
        }
    ));

    let zero_index =
        next_id_error_from_fake_response(StorageRpcMetadataCommandNextIdOutcome::LogConflict {
            node_id: 7,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            log_index: 0,
        });
    assert!(matches!(
        zero_index,
        StoreError::StorageRpc {
            operation: "decode metadata command next id response",
            ..
        }
    ));
}

#[test]
fn unix_storage_node_client_preserves_pending_slot_log_conflict() {
    fn pending_insert_error_from_fake_response(
        outcome: StorageRpcMetadataCommandPendingSlotInsertOutcome,
    ) -> StoreError {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let listener = UnixListener::bind(&socket_path).unwrap();
        let join = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_storage_rpc_frame_from(&mut stream).unwrap();
            let payload = crate::storage_rpc::encode_metadata_command_pending_slot_insert_response(
                &crate::storage_rpc::StorageRpcMetadataCommandPendingSlotInsertResponse { outcome },
            );
            let response = StorageRpcFrame {
                request_id: request.request_id,
                kind: request.kind,
                payload: encode_storage_rpc_success_response(&payload),
            };
            write_storage_rpc_frame_to(&mut stream, &response).unwrap();
        });
        let client =
            UnixStorageNodeClient::new(NodeId::new(7), ClusterEpoch::new(1).unwrap(), socket_path);

        let err = MetadataCommandNodeClient::try_insert_pending_metadata_command_slot(
            &client,
            PgId::new(0),
            &test_metadata_command(0, 1),
            Some(&crate::tests::bucket_name("metadata-rpc-bucket")),
        )
        .unwrap_err();

        join.join().unwrap();
        err
    }

    let conflict = pending_insert_error_from_fake_response(
        StorageRpcMetadataCommandPendingSlotInsertOutcome::LogConflict {
            node_id: 7,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            log_index: 1,
        },
    );
    assert!(matches!(
        conflict,
        StoreError::MetadataCommandLogConflict {
            pg_id: 0,
            log_index: 1,
            ..
        }
    ));

    let wrong_route = pending_insert_error_from_fake_response(
        StorageRpcMetadataCommandPendingSlotInsertOutcome::LogConflict {
            node_id: 7,
            pg_id: 1,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            log_index: 1,
        },
    );
    assert!(matches!(
        wrong_route,
        StoreError::StorageRpc {
            operation: "decode metadata command pending slot insert response",
            ..
        }
    ));

    let zero_index = pending_insert_error_from_fake_response(
        StorageRpcMetadataCommandPendingSlotInsertOutcome::LogConflict {
            node_id: 7,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            log_index: 0,
        },
    );
    assert!(matches!(
        zero_index,
        StoreError::StorageRpc {
            operation: "decode metadata command pending slot insert response",
            ..
        }
    ));
}

fn metadata_command_session_result_from_fake_response<R>(
    target_payload: Vec<u8>,
    call: impl FnOnce(UnixStorageNodeMetadataCommandSession) -> R,
) -> R {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("sock").join("storage.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let listener = UnixListener::bind(&socket_path).unwrap();
    let join = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let acquire = read_storage_rpc_frame_from(&mut stream).unwrap();
        assert_eq!(
            acquire.kind,
            StorageRpcMessageKind::MetadataCommandPgLockAcquire
        );
        let acquire_response = StorageRpcFrame {
            request_id: acquire.request_id,
            kind: acquire.kind,
            payload: encode_storage_rpc_success_response(&[]),
        };
        write_storage_rpc_frame_to(&mut stream, &acquire_response).unwrap();

        let request = read_storage_rpc_frame_from(&mut stream).unwrap();
        let response = StorageRpcFrame {
            request_id: request.request_id,
            kind: request.kind,
            payload: encode_storage_rpc_success_response(&target_payload),
        };
        write_storage_rpc_frame_to(&mut stream, &response).unwrap();

        assert!(matches!(
            read_storage_rpc_frame_from(&mut stream),
            Err(StorageRpcStreamError::Io(_))
        ));
    });
    let client =
        UnixStorageNodeClient::new(NodeId::new(7), ClusterEpoch::new(1).unwrap(), socket_path);
    let session = client
        .open_metadata_command_critical_section(PgId::new(0))
        .unwrap();

    let result = call(session);
    join.join().unwrap();
    result
}

fn metadata_command_recovery_session_result_from_fake_response<R>(
    target_payload: Vec<u8>,
    call: impl FnOnce(Box<dyn MetadataCommandRecoveryCriticalSection>) -> R,
) -> R {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("sock").join("storage.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let listener = UnixListener::bind(&socket_path).unwrap();
    let join = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let acquire = read_storage_rpc_frame_from(&mut stream).unwrap();
        assert_eq!(
            acquire.kind,
            StorageRpcMessageKind::MetadataCommandPgLockAcquire
        );
        let acquire_response = StorageRpcFrame {
            request_id: acquire.request_id,
            kind: acquire.kind,
            payload: encode_storage_rpc_success_response(&[]),
        };
        write_storage_rpc_frame_to(&mut stream, &acquire_response).unwrap();

        let request = read_storage_rpc_frame_from(&mut stream).unwrap();
        let response = StorageRpcFrame {
            request_id: request.request_id,
            kind: request.kind,
            payload: encode_storage_rpc_success_response(&target_payload),
        };
        write_storage_rpc_frame_to(&mut stream, &response).unwrap();

        assert!(matches!(
            read_storage_rpc_frame_from(&mut stream),
            Err(StorageRpcStreamError::Io(_))
        ));
    });
    let client =
        UnixStorageNodeClient::new(NodeId::new(7), ClusterEpoch::new(1).unwrap(), socket_path);
    let session =
        MetadataCommandRecoveryNodeClient::open_metadata_command_recovery_critical_section(
            &client,
            PgId::new(0),
            ClusterEpoch::new(1).unwrap(),
        )
        .unwrap();

    let result = call(session);
    join.join().unwrap();
    result
}

#[test]
fn unix_storage_node_session_rejects_malformed_log_conflicts() {
    let command = test_metadata_command(0, 1);

    let pending_payload = encode_metadata_command_pending_slot_insert_response(
        &StorageRpcMetadataCommandPendingSlotInsertResponse {
            outcome: StorageRpcMetadataCommandPendingSlotInsertOutcome::LogConflict {
                node_id: 7,
                pg_id: 1,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 1,
            },
        },
    );
    let pending_error =
        metadata_command_session_result_from_fake_response(pending_payload, |session| {
            session
                .try_insert_pending_metadata_command_slot(
                    PgId::new(0),
                    &command,
                    Some(&crate::tests::bucket_name("metadata-rpc-bucket")),
                )
                .unwrap_err()
        });
    assert!(matches!(
        pending_error,
        StoreError::StorageRpc {
            operation: "decode metadata command pending slot insert response",
            ..
        }
    ));

    let bucket_control_payload = encode_metadata_command_bool_outcome_response(
        &StorageRpcMetadataCommandBoolOutcomeResponse {
            outcome: StorageRpcMetadataCommandBoolOutcome::LogConflict {
                node_id: 7,
                pg_id: 1,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 1,
            },
        },
    );
    let bucket_control_error =
        metadata_command_session_result_from_fake_response(bucket_control_payload, |session| {
            session
                .try_insert_bucket_control_pending_metadata_command_slot(
                    PgId::new(0),
                    &command,
                    &crate::tests::bucket_name("metadata-rpc-bucket"),
                )
                .unwrap_err()
        });
    assert!(matches!(
        bucket_control_error,
        StoreError::StorageRpc {
            operation: "decode metadata command bucket-control pending slot insert response",
            ..
        }
    ));

    let acceptance_payload =
        encode_metadata_command_acceptance_response(&StorageRpcMetadataCommandAcceptanceResponse {
            outcome: StorageRpcMetadataCommandAcceptanceOutcome::LogConflict {
                node_id: 7,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 0,
            },
        });
    let acceptance_error =
        metadata_command_session_result_from_fake_response(acceptance_payload, |session| {
            MetadataCommandNodeClient::metadata_command_acceptance(&session, PgId::new(0), &command)
                .unwrap_err()
        });
    assert!(matches!(
        acceptance_error,
        StoreError::StorageRpc {
            operation: "decode metadata command acceptance response",
            ..
        }
    ));

    let hashes_payload = encode_metadata_command_applied_hashes_response(
        &StorageRpcMetadataCommandAppliedHashesResponse {
            outcome: StorageRpcMetadataCommandAppliedHashesOutcome::LogConflict {
                node_id: 7,
                pg_id: 1,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 1,
            },
        },
    );
    let hashes_error =
        metadata_command_session_result_from_fake_response(hashes_payload, |session| {
            session
                .applied_metadata_command_log_entry_hashes(PgId::new(0), &command)
                .unwrap_err()
        });
    assert!(matches!(
        hashes_error,
        StoreError::StorageRpc {
            operation: "decode metadata command applied hashes response",
            ..
        }
    ));

    let apply_payload = encode_metadata_command_state_outcome_response(
        &StorageRpcMetadataCommandStateOutcomeResponse {
            outcome: StorageRpcMetadataCommandStateOutcome::LogConflict {
                node_id: 7,
                pg_id: 1,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 1,
            },
        },
    );
    let apply_error =
        metadata_command_session_result_from_fake_response(apply_payload, |session| {
            MetadataCommandNodeClient::apply_metadata_command_and_record(
                &session,
                PgId::new(0),
                &command,
            )
            .unwrap_err()
        });
    assert!(matches!(
        apply_error,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "decode metadata command apply and record response",
            ..
        })
    ));

    let abandoned_payload = encode_metadata_command_state_outcome_response(
        &StorageRpcMetadataCommandStateOutcomeResponse {
            outcome: StorageRpcMetadataCommandStateOutcome::LogConflict {
                node_id: 7,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 0,
            },
        },
    );
    let abandoned_error =
        metadata_command_recovery_session_result_from_fake_response(abandoned_payload, |session| {
            session
                .record_metadata_command_abandoned(&command)
                .unwrap_err()
        });
    assert!(matches!(
        abandoned_error,
        StoreError::StorageRpc {
            operation: "decode metadata command record abandoned response",
            ..
        }
    ));
}

#[test]
fn unix_storage_node_client_preserves_applied_hash_log_conflict() {
    fn applied_hashes_error_from_fake_response(
        outcome: StorageRpcMetadataCommandAppliedHashesOutcome,
    ) -> StoreError {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let listener = UnixListener::bind(&socket_path).unwrap();
        let join = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_storage_rpc_frame_from(&mut stream).unwrap();
            let payload = encode_metadata_command_applied_hashes_response(
                &StorageRpcMetadataCommandAppliedHashesResponse { outcome },
            );
            let response = StorageRpcFrame {
                request_id: request.request_id,
                kind: request.kind,
                payload: encode_storage_rpc_success_response(&payload),
            };
            write_storage_rpc_frame_to(&mut stream, &response).unwrap();
        });
        let client =
            UnixStorageNodeClient::new(NodeId::new(7), ClusterEpoch::new(1).unwrap(), socket_path);

        let err = MetadataCommandInspectionNodeClient::applied_metadata_command_log_entry_hashes(
            &client,
            PgId::new(0),
            &test_metadata_command(0, 1),
        )
        .unwrap_err();

        join.join().unwrap();
        err
    }

    let conflict = applied_hashes_error_from_fake_response(
        StorageRpcMetadataCommandAppliedHashesOutcome::LogConflict {
            node_id: 7,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            log_index: 1,
        },
    );
    assert!(matches!(
        conflict,
        StoreError::MetadataCommandLogConflict {
            pg_id: 0,
            log_index: 1,
            ..
        }
    ));

    let wrong_route = applied_hashes_error_from_fake_response(
        StorageRpcMetadataCommandAppliedHashesOutcome::LogConflict {
            node_id: 7,
            pg_id: 1,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            log_index: 1,
        },
    );
    assert!(matches!(
        wrong_route,
        StoreError::StorageRpc {
            operation: "decode metadata command applied hashes response",
            ..
        }
    ));

    let zero_index = applied_hashes_error_from_fake_response(
        StorageRpcMetadataCommandAppliedHashesOutcome::LogConflict {
            node_id: 7,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            log_index: 0,
        },
    );
    assert!(matches!(
        zero_index,
        StoreError::StorageRpc {
            operation: "decode metadata command applied hashes response",
            ..
        }
    ));
}

#[test]
fn unix_storage_node_client_preserves_record_abandoned_log_conflict() {
    fn record_abandoned_error_from_fake_response(
        outcome: StorageRpcMetadataCommandStateOutcome,
    ) -> StoreError {
        let payload = encode_metadata_command_state_outcome_response(
            &StorageRpcMetadataCommandStateOutcomeResponse { outcome },
        );
        metadata_command_recovery_session_result_from_fake_response(payload, |session| {
            session
                .record_metadata_command_abandoned(&test_metadata_command(0, 1))
                .unwrap_err()
        })
    }

    let conflict = record_abandoned_error_from_fake_response(
        StorageRpcMetadataCommandStateOutcome::LogConflict {
            node_id: 7,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            log_index: 1,
        },
    );
    assert!(matches!(
        conflict,
        StoreError::MetadataCommandLogConflict {
            pg_id: 0,
            log_index: 1,
            ..
        }
    ));

    let wrong_route = record_abandoned_error_from_fake_response(
        StorageRpcMetadataCommandStateOutcome::LogConflict {
            node_id: 7,
            pg_id: 1,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            log_index: 1,
        },
    );
    assert!(matches!(
        wrong_route,
        StoreError::StorageRpc {
            operation: "decode metadata command record abandoned response",
            ..
        }
    ));

    let zero_index = record_abandoned_error_from_fake_response(
        StorageRpcMetadataCommandStateOutcome::LogConflict {
            node_id: 7,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            log_index: 0,
        },
    );
    assert!(matches!(
        zero_index,
        StoreError::StorageRpc {
            operation: "decode metadata command record abandoned response",
            ..
        }
    ));
}

#[test]
fn unix_storage_node_client_preserves_apply_metadata_command_log_conflict() {
    fn apply_error_from_fake_response(
        outcome: StorageRpcMetadataCommandStateOutcome,
    ) -> BucketSnapshotLoadError {
        apply_error_from_fake_response_for_command(outcome, test_metadata_command(0, 1))
    }

    fn apply_error_from_fake_response_for_command(
        outcome: StorageRpcMetadataCommandStateOutcome,
        command: MetadataCommandEnvelope,
    ) -> BucketSnapshotLoadError {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let listener = UnixListener::bind(&socket_path).unwrap();
        let join = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_storage_rpc_frame_from(&mut stream).unwrap();
            let payload = encode_metadata_command_state_outcome_response(
                &StorageRpcMetadataCommandStateOutcomeResponse { outcome },
            );
            let response = StorageRpcFrame {
                request_id: request.request_id,
                kind: request.kind,
                payload: encode_storage_rpc_success_response(&payload),
            };
            write_storage_rpc_frame_to(&mut stream, &response).unwrap();
        });
        let client =
            UnixStorageNodeClient::new(NodeId::new(7), ClusterEpoch::new(1).unwrap(), socket_path);

        let err = MetadataCommandNodeClient::apply_metadata_command_and_record(
            &client,
            PgId::new(0),
            &command,
        )
        .unwrap_err();

        join.join().unwrap();
        err
    }

    let conflict =
        apply_error_from_fake_response(StorageRpcMetadataCommandStateOutcome::LogConflict {
            node_id: 7,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            log_index: 1,
        });
    assert!(matches!(
        conflict,
        BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
            pg_id: 0,
            log_index: 1,
            ..
        })
    ));

    let wrong_route =
        apply_error_from_fake_response(StorageRpcMetadataCommandStateOutcome::LogConflict {
            node_id: 7,
            pg_id: 1,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            log_index: 1,
        });
    assert!(matches!(
        wrong_route,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "decode metadata command apply and record response",
            ..
        })
    ));

    let zero_index =
        apply_error_from_fake_response(StorageRpcMetadataCommandStateOutcome::LogConflict {
            node_id: 7,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            log_index: 0,
        });
    assert!(matches!(
        zero_index,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "decode metadata command apply and record response",
            ..
        })
    ));

    let stale_version = apply_error_from_fake_response(
        StorageRpcMetadataCommandStateOutcome::ObjectVersionReservationConflict {
            version_id: VersionId::from_u64(7),
        },
    );
    assert!(matches!(
        stale_version,
        BucketSnapshotLoadError::Metadata(MetadataError::ObjectVersionReservationConflict {
            version_id
        }) if version_id == VersionId::from_u64(7)
    ));

    let bucket = crate::tests::bucket_name("stale-bucket-rpc");
    let stale_command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(1).unwrap(),
            PgId::new(0),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand::new(
            bucket.clone(),
            BucketSubresourceMutation::Delete {
                kind: BucketSubresourceKind::Cors,
            },
            11,
        )),
    );
    let stale_bucket = apply_error_from_fake_response_for_command(
        StorageRpcMetadataCommandStateOutcome::StaleBucketMetadataCommand {
            name: bucket.clone(),
            bucket_execution_generation: 11,
        },
        stale_command.clone(),
    );
    assert!(matches!(
        stale_bucket,
        BucketSnapshotLoadError::Metadata(MetadataError::StaleBucketMetadataCommand {
            ref name,
            bucket_execution_generation: 11,
        }) if name == &bucket
    ));

    let stale_bucket_mismatch = apply_error_from_fake_response_for_command(
        StorageRpcMetadataCommandStateOutcome::StaleBucketMetadataCommand {
            name: crate::tests::bucket_name("wrong-stale-bucket-rpc"),
            bucket_execution_generation: 11,
        },
        stale_command,
    );
    assert!(matches!(
        stale_bucket_mismatch,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "decode metadata command apply and record response",
            ..
        })
    ));

    let object_bucket = crate::tests::bucket_name("stale-object-rpc");
    let object_key = crate::tests::object_key("object");
    let stale_object_command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(1).unwrap(),
            PgId::new(0),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
            bucket: object_bucket.clone(),
            key: object_key.clone(),
            version_id: VersionId::from_u64(7),
            owner: crate::OwnerIdentity::from_principal("owner"),
            write_sequence: 3,
            last_modified_millis: 123,
            stale_payload: None,
            bucket_write_reservation: test_bucket_write_reservation_proof(
                object_bucket.clone(),
                &object_key,
            ),
        }),
    );
    let stale_object = apply_error_from_fake_response_for_command(
        StorageRpcMetadataCommandStateOutcome::StaleObjectWriteCommand {
            bucket: object_bucket.clone(),
            key: object_key.clone(),
            write_sequence: 3,
            generation_id: None,
        },
        stale_object_command.clone(),
    );
    assert!(matches!(
        stale_object,
        BucketSnapshotLoadError::Metadata(MetadataError::StaleObjectWriteCommand {
            ref bucket,
            ref key,
            write_sequence: 3,
            generation_id: None,
        }) if bucket == &object_bucket && key == &object_key
    ));

    let stale_delete_marker_command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(1).unwrap(),
            PgId::new(0),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
            bucket: object_bucket.clone(),
            key: object_key.clone(),
            version_id: VersionId::Null,
            mode: crate::metadata_command::DeleteObjectVersionMode::Current,
            target: DeleteObjectVersionTarget::DeleteMarker { write_sequence: 5 },
            bucket_write_reservation: test_bucket_write_reservation_proof(
                object_bucket.clone(),
                &object_key,
            ),
        })),
    );
    let stale_delete_marker = apply_error_from_fake_response_for_command(
        StorageRpcMetadataCommandStateOutcome::StaleObjectWriteCommand {
            bucket: object_bucket.clone(),
            key: object_key.clone(),
            write_sequence: 5,
            generation_id: None,
        },
        stale_delete_marker_command.clone(),
    );
    assert!(matches!(
        stale_delete_marker,
        BucketSnapshotLoadError::Metadata(MetadataError::StaleObjectWriteCommand {
            ref bucket,
            ref key,
            write_sequence: 5,
            generation_id: None,
        }) if bucket == &object_bucket && key == &object_key
    ));

    let stale_delete_marker_generation_mismatch = apply_error_from_fake_response_for_command(
        StorageRpcMetadataCommandStateOutcome::StaleObjectWriteCommand {
            bucket: object_bucket.clone(),
            key: object_key.clone(),
            write_sequence: 5,
            generation_id: Some(GenerationId::MIN),
        },
        stale_delete_marker_command,
    );
    assert!(matches!(
        stale_delete_marker_generation_mismatch,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "decode metadata command apply and record response",
            ..
        })
    ));

    let stale_object_mismatch = apply_error_from_fake_response_for_command(
        StorageRpcMetadataCommandStateOutcome::StaleObjectWriteCommand {
            bucket: object_bucket,
            key: object_key,
            write_sequence: 4,
            generation_id: None,
        },
        stale_object_command,
    );
    assert!(matches!(
        stale_object_mismatch,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "decode metadata command apply and record response",
            ..
        })
    ));

    let impossible_stale_bucket = apply_error_from_fake_response(
        StorageRpcMetadataCommandStateOutcome::StaleBucketMetadataCommand {
            name: bucket,
            bucket_execution_generation: 11,
        },
    );
    assert!(matches!(
        impossible_stale_bucket,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "decode metadata command apply and record response",
            ..
        })
    ));
}

#[test]
fn unix_storage_node_client_preserves_bucket_control_pending_slot_log_conflict() {
    fn bucket_control_error_from_fake_response(
        outcome: StorageRpcMetadataCommandBoolOutcome,
    ) -> StoreError {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let listener = UnixListener::bind(&socket_path).unwrap();
        let join = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_storage_rpc_frame_from(&mut stream).unwrap();
            let payload = encode_metadata_command_bool_outcome_response(
                &StorageRpcMetadataCommandBoolOutcomeResponse { outcome },
            );
            let response = StorageRpcFrame {
                request_id: request.request_id,
                kind: request.kind,
                payload: encode_storage_rpc_success_response(&payload),
            };
            write_storage_rpc_frame_to(&mut stream, &response).unwrap();
        });
        let client =
            UnixStorageNodeClient::new(NodeId::new(7), ClusterEpoch::new(1).unwrap(), socket_path);

        let err =
            MetadataCommandNodeClient::try_insert_bucket_control_pending_metadata_command_slot(
                &client,
                PgId::new(0),
                &test_metadata_command(0, 1),
                &crate::tests::bucket_name("metadata-rpc-bucket"),
            )
            .unwrap_err();

        join.join().unwrap();
        err
    }

    let conflict = bucket_control_error_from_fake_response(
        StorageRpcMetadataCommandBoolOutcome::LogConflict {
            node_id: 7,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            log_index: 1,
        },
    );
    assert!(matches!(
        conflict,
        StoreError::MetadataCommandLogConflict {
            pg_id: 0,
            log_index: 1,
            ..
        }
    ));

    let wrong_route = bucket_control_error_from_fake_response(
        StorageRpcMetadataCommandBoolOutcome::LogConflict {
            node_id: 7,
            pg_id: 1,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            log_index: 1,
        },
    );
    assert!(matches!(
        wrong_route,
        StoreError::StorageRpc {
            operation: "decode metadata command bucket-control pending slot insert response",
            ..
        }
    ));

    let zero_index = bucket_control_error_from_fake_response(
        StorageRpcMetadataCommandBoolOutcome::LogConflict {
            node_id: 7,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            log_index: 0,
        },
    );
    assert!(matches!(
        zero_index,
        StoreError::StorageRpc {
            operation: "decode metadata command bucket-control pending slot insert response",
            ..
        }
    ));
}

#[test]
fn unix_storage_node_client_preserves_bool_metadata_command_log_conflicts() {
    fn bool_metadata_error_from_fake_response(
        kind: StorageRpcMessageKind,
        outcome: StorageRpcMetadataCommandBoolOutcome,
    ) -> StoreError {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("sock").join("storage.sock");
        private_socket_dir(socket_path.parent().unwrap());
        let listener = UnixListener::bind(&socket_path).unwrap();
        let join = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_storage_rpc_frame_from(&mut stream).unwrap();
            let payload = encode_metadata_command_bool_outcome_response(
                &StorageRpcMetadataCommandBoolOutcomeResponse { outcome },
            );
            let response = StorageRpcFrame {
                request_id: request.request_id,
                kind: request.kind,
                payload: encode_storage_rpc_success_response(&payload),
            };
            write_storage_rpc_frame_to(&mut stream, &response).unwrap();
        });
        let client =
            UnixStorageNodeClient::new(NodeId::new(7), ClusterEpoch::new(1).unwrap(), socket_path);
        let command = test_metadata_command(0, 1);

        let err = match kind {
            StorageRpcMessageKind::MetadataCommandMatchingAppliedLog => {
                MetadataCommandInspectionNodeClient::has_matching_applied_metadata_command_log_entry(
                    &client,
                    PgId::new(0),
                    &command,
                    0,
                )
                .unwrap_err()
            }
            StorageRpcMessageKind::MetadataCommandAbandoned => {
                MetadataCommandInspectionNodeClient::metadata_command_abandoned(
                    &client,
                    PgId::new(0),
                    &command,
                )
                .unwrap_err()
            }
            _ => panic!("unsupported bool metadata command test kind"),
        };

        join.join().unwrap();
        err
    }

    for kind in [
        StorageRpcMessageKind::MetadataCommandMatchingAppliedLog,
        StorageRpcMessageKind::MetadataCommandAbandoned,
    ] {
        let conflict = bool_metadata_error_from_fake_response(
            kind,
            StorageRpcMetadataCommandBoolOutcome::LogConflict {
                node_id: 7,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 1,
            },
        );
        assert!(matches!(
            conflict,
            StoreError::MetadataCommandLogConflict {
                pg_id: 0,
                log_index: 1,
                ..
            }
        ));

        let wrong_route = bool_metadata_error_from_fake_response(
            kind,
            StorageRpcMetadataCommandBoolOutcome::LogConflict {
                node_id: 7,
                pg_id: 1,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 1,
            },
        );
        assert!(matches!(
            wrong_route,
            StoreError::StorageRpc {
                operation: "decode metadata command matching applied response"
                    | "decode metadata command abandoned response",
                ..
            }
        ));

        let zero_index = bool_metadata_error_from_fake_response(
            kind,
            StorageRpcMetadataCommandBoolOutcome::LogConflict {
                node_id: 7,
                pg_id: 0,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                log_index: 0,
            },
        );
        assert!(matches!(
            zero_index,
            StoreError::StorageRpc {
                operation: "decode metadata command matching applied response"
                    | "decode metadata command abandoned response",
                ..
            }
        ));
    }
}

#[test]
fn unix_storage_node_client_read_into_requires_full_shard_buffer() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || {
        for _ in 0..2 {
            server.accept_one().unwrap();
        }
    });
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let key = ShardKey::new(&[0x56; 16], 12, 0);
    let data_pg_id = DataPgId::new_for_test(PgId::new(0));
    let location = crate::cluster::ShardLocation::new(
        config.cluster_epoch,
        data_pg_id,
        key.shard_index(),
        config.node_id,
    );
    let route = client.open_placed_shard_route(location, &key).unwrap();
    let ack = route.write_placed_shard(b"remote payload").unwrap();

    let mut short = vec![0; ack.stored_size as usize - 1];
    let err = route.read_placed_shard_into(ack, &mut short).unwrap_err();
    assert!(matches!(
        err,
        StoreError::StorageRpc {
            operation: "shard read range",
            ref detail,
            ..
        } if detail.as_str().contains("expected")
    ));
    assert_eq!(route.read_placed_shard(ack).unwrap(), b"remote payload");
    drop(route);
    drop(client);
    server_thread.join().unwrap();
}

#[test]
fn unix_storage_node_read_handle_session_is_idempotent_and_disconnect_releases() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_for_thread = Arc::clone(&server);
    let server_thread = thread::spawn(move || server_for_thread.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let location = crate::cluster::ShardLocation::new(
        config.cluster_epoch,
        DataPgId::new_for_test(PgId::new(0)),
        crate::ShardIndex::new(0),
        config.node_id,
    );
    let key = ShardKey::new(&[0x55; 16], 55, 0);
    let mut session = client.open_read_handle_session_for_test().unwrap();

    assert_eq!(
        session
            .acquire_read_handles("read-op", vec![(location, key.clone())])
            .unwrap(),
        vec![location]
    );
    assert_eq!(
        session
            .acquire_read_handles("read-op", vec![(location, key.clone())])
            .unwrap(),
        vec![location]
    );
    assert_eq!(server.read_handle_count(location), 1);
    session.release_read_handles("read-op").unwrap();
    session.release_read_handles("read-op").unwrap();
    assert_eq!(server.read_handle_count(location), 0);
    session
        .acquire_read_handles("read-op-disconnect", vec![(location, key)])
        .unwrap();
    assert_eq!(server.read_handle_count(location), 1);
    drop(session);
    server_thread.join().unwrap();
    assert_eq!(server.read_handle_count(location), 0);
}

#[test]
fn unix_storage_node_delete_fails_while_read_handle_active() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..4)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let key = ShardKey::new(&[0x66; 16], 12, 0);
    let data_pg_id = DataPgId::new_for_test(PgId::new(0));
    let location = crate::cluster::ShardLocation::new(
        config.cluster_epoch,
        data_pg_id,
        key.shard_index(),
        config.node_id,
    );
    let mut session = client.open_read_handle_session_for_test().unwrap();
    let route = client.open_placed_shard_route(location, &key).unwrap();

    route.write_placed_shard(b"protected payload").unwrap();
    session
        .acquire_read_handles("protected-read", vec![(location, key.clone())])
        .unwrap();
    assert_eq!(server.read_handle_count(location), 1);

    let err = route.delete_placed_shard().unwrap_err();
    assert!(matches!(
        err,
        StoreError::StorageRpcResourceExhausted {
            operation: "shard delete",
            ref detail,
            ..
        } if detail.as_str().contains("active read handles")
    ));

    session.release_read_handles("protected-read").unwrap();
    assert_eq!(server.read_handle_count(location), 0);
    route.delete_placed_shard().unwrap();
    drop(route);
    drop(session);
    for join in server_threads {
        join.join().unwrap();
    }

    let reopened = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    assert!(matches!(
        reopened.read_shard_file(0, &key),
        Err(StoreError::NotFound)
    ));
}

#[test]
fn unix_storage_node_rpc_admission_exhaustion_is_typed_before_connect() {
    let client =
        test_unix_storage_node_client_with_rpc_admission_timeout(1, Duration::from_millis(10));
    let _held = client
        .acquire_rpc_admission(StorageRpcMessageKind::ShardRead)
        .unwrap();
    let before = observability::metrics_snapshot();

    let err = client
        .rpc_request_result(StorageRpcMessageKind::ShardWrite, Vec::new())
        .unwrap_err();
    let after = observability::metrics_snapshot();
    assert!(matches!(
        err,
        StoreError::StorageRpcResourceExhausted {
            node_id: 7,
            operation: "shard write",
            ref detail,
        } if detail.as_str().contains("admission limit 1")
    ));
    assert!(after.storage_rpc_admission_total > before.storage_rpc_admission_total);
    assert!(after.storage_rpc_admission_timeout_total > before.storage_rpc_admission_timeout_total);
}

#[test]
fn unix_storage_node_rpc_admission_wait_metric_records_released_capacity() {
    let client = Arc::new(test_unix_storage_node_client_with_rpc_admission_timeout(
        1,
        Duration::from_secs(1),
    ));
    let held = client
        .acquire_rpc_admission(StorageRpcMessageKind::ShardRead)
        .unwrap();
    let before = observability::metrics_snapshot();
    let (attempt_tx, attempt_rx) = std::sync::mpsc::channel();
    let client_for_thread = Arc::clone(&client);
    let join = thread::spawn(move || {
        attempt_tx.send(()).unwrap();
        client_for_thread.acquire_rpc_admission(StorageRpcMessageKind::ShardWrite)
    });

    attempt_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    thread::sleep(Duration::from_millis(50));
    drop(held);
    let permit = join.join().unwrap().unwrap();
    drop(permit);
    let after = observability::metrics_snapshot();

    assert!(after.storage_rpc_admission_total > before.storage_rpc_admission_total);
    assert!(after.storage_rpc_admission_wait_total > before.storage_rpc_admission_wait_total);
    assert!(after.storage_rpc_admission_wait_us_total > before.storage_rpc_admission_wait_us_total);
}

#[test]
fn unix_storage_node_rpc_admission_is_shared_by_node_and_socket() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("missing.sock");
    let rpc_admission = shared_unix_storage_node_rpc_admission_with_settings(
        NodeId::new(7),
        &socket_path,
        crate::node_client::UnixStorageNodeRpcAdmissionSettings {
            limit: 1,
            wait_timeout: Duration::from_millis(10),
            control_wait_timeout: Duration::from_millis(10),
        },
    );
    let client_a = UnixStorageNodeClient::with_rpc_admission(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        socket_path.clone(),
        Arc::clone(&rpc_admission),
        None,
    );
    let client_b =
        UnixStorageNodeClient::new(NodeId::new(7), ClusterEpoch::new(1).unwrap(), socket_path);
    let _held = client_a
        .acquire_rpc_admission(StorageRpcMessageKind::ShardRead)
        .unwrap();

    let err = client_b
        .rpc_request_result(StorageRpcMessageKind::ShardWrite, Vec::new())
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::StorageRpcResourceExhausted {
            node_id: 7,
            operation: "shard write",
            ref detail,
        } if detail.as_str().contains("admission limit")
    ));
}

#[test]
fn unix_storage_node_shard_write_admission_exhausts_before_socket_write() {
    let client =
        test_unix_storage_node_client_with_rpc_admission_timeout(1, Duration::from_millis(10));
    let _held = client
        .acquire_rpc_admission(StorageRpcMessageKind::ShardRead)
        .unwrap();
    let key = ShardKey::new(&[0x55; 16], 55, 0);
    let location = crate::cluster::ShardLocation::new(
        client.cluster_epoch,
        DataPgId::new_for_test(PgId::new(0)),
        key.shard_index(),
        client.node_id,
    );
    let route = client.open_placed_shard_route(location, &key).unwrap();
    let err = route.write_placed_shard(&[0x5a; 4096]).unwrap_err();

    assert!(matches!(
        err,
        StoreError::StorageRpcResourceExhausted {
            node_id: 7,
            operation: "shard write",
            ref detail,
        } if detail.as_str().contains("admission limit 1")
    ));
}

#[test]
fn unix_storage_node_read_handle_session_admission_exhausts_before_connect() {
    let client =
        test_unix_storage_node_client_with_rpc_admission_timeout(1, Duration::from_millis(10));
    let _held = client
        .acquire_rpc_admission(StorageRpcMessageKind::ShardRead)
        .unwrap();
    let err = match client.open_read_handle_session_for_test() {
        Ok(_) => panic!("read-handle session admission unexpectedly succeeded"),
        Err(err) => err,
    };

    assert!(matches!(
        err,
        StoreError::StorageRpcResourceExhausted {
            node_id: 7,
            operation: "read handles acquire",
            ref detail,
        } if detail.as_str().contains("admission limit 1")
    ));
}

#[test]
fn unix_storage_node_metadata_session_admission_exhausts_before_connect() {
    let client =
        test_unix_storage_node_client_with_rpc_admission_timeout(1, Duration::from_millis(10));
    let _held = client
        .acquire_rpc_admission(StorageRpcMessageKind::ShardRead)
        .unwrap();
    let err = match client.open_metadata_command_critical_section(PgId::new(0)) {
        Ok(_) => panic!("metadata-command session admission unexpectedly succeeded"),
        Err(err) => err,
    };

    assert!(matches!(
        err,
        StoreError::StorageRpcResourceExhausted {
            node_id: 7,
            operation: "metadata command PG lock acquire",
            ref detail,
        } if detail.as_str().contains("admission limit 1")
    ));
}

#[test]
fn unix_storage_node_metadata_session_lock_contention_returns_typed_error() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let owner_server = Arc::clone(&server);
    let owner_thread = thread::spawn(move || owner_server.accept_one().unwrap());
    let owner = client
        .open_metadata_command_critical_section(PgId::new(0))
        .unwrap();

    let waiter_server = Arc::clone(&server);
    let waiter_thread = thread::spawn(move || waiter_server.accept_one().unwrap());
    let started = Instant::now();
    let err = match client.open_metadata_command_critical_section(PgId::new(0)) {
        Ok(_) => panic!("contended metadata-command session unexpectedly acquired the PG lock"),
        Err(error) => error,
    };

    assert!(
        started.elapsed() < STORAGE_RPC_CLIENT_RESPONSE_TIMEOUT + Duration::from_secs(1),
        "metadata-command lock contention should return before the Unix client response timeout"
    );
    assert!(matches!(
        err,
        StoreError::StorageRpc {
            node_id: 7,
            operation: "metadata command PG lock acquire",
            failure: StorageRpcErrorCode::MetadataCommandContention,
            ..
        }
    ));
    waiter_thread.join().unwrap();
    drop(owner);
    owner_thread.join().unwrap();
}

#[test]
fn unix_storage_node_read_handle_route_rejects_mismatched_acquire_response() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("sock").join("storage.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let listener = UnixListener::bind(&socket_path).unwrap();
    let join = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let request = read_storage_rpc_frame_from(&mut stream).unwrap();
        let mismatched_location = crate::cluster::ShardLocation::new(
            ClusterEpoch::new(1).unwrap(),
            DataPgId::new_for_test(PgId::new(0)),
            crate::ShardIndex::new(1),
            NodeId::new(7),
        );
        let payload = encode_read_handle_acquire_response(&StorageRpcReadHandleAcquireResponse {
            locations: vec![mismatched_location.into()],
        })
        .unwrap();
        let response = StorageRpcFrame {
            request_id: request.request_id,
            kind: request.kind,
            payload: encode_storage_rpc_success_response(&payload),
        };
        write_storage_rpc_frame_to(&mut stream, &response).unwrap();
    });
    let client =
        UnixStorageNodeClient::new(NodeId::new(7), ClusterEpoch::new(1).unwrap(), socket_path);
    let requested_location = crate::cluster::ShardLocation::new(
        ClusterEpoch::new(1).unwrap(),
        DataPgId::new_for_test(PgId::new(0)),
        crate::ShardIndex::new(0),
        NodeId::new(7),
    );
    let err = client
        .open_shard_read_handle_route(
            ClusterEpoch::new(1).unwrap(),
            "read-op",
            vec![(
                requested_location,
                ShardKey::new(&[0x77; 16], 77, requested_location.shard_index().get()),
            )],
        )
        .and_then(|route| route.acquire())
        .err()
        .expect("mismatched read-handle response must be rejected");

    assert!(matches!(
        err,
        StoreError::StorageRpc {
            operation: "validate read handle acquire response",
            ..
        }
    ));
    join.join().unwrap();
}
