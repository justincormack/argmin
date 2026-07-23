use super::*;
use crate::node_client::ObjectPayloadLeaseKind;
use crate::storage_rpc::StorageRpcErrorCode;
use crate::{
    ObjectPayloadReclaimKind, PlacedSegmentShardBackfillClaimAcquire,
    PlacedSegmentShardBackfillClaimRecord, PlacedSegmentShardBackfillRecord,
    PlacedSegmentShardBackfillWorkItem, PlacedSegmentShardRepairClaimAcquire,
    PlacedSegmentShardRepairClaimRecord, PlacedSegmentShardRepairRecord,
};

struct RecordingPlacedShardClient {
    node_id: NodeId,
    writes: Mutex<Vec<(DataPgId, ShardKey, Vec<u8>)>>,
}

impl RecordingPlacedShardClient {
    fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            writes: Mutex::new(Vec::new()),
        }
    }
}

impl PlacedShardNodeClient for RecordingPlacedShardClient {
    fn node_id(&self) -> NodeId {
        self.node_id
    }

    fn write_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError> {
        self.writes.lock().unwrap_or_else(|e| e.into_inner()).push((
            data_pg_id,
            key.clone(),
            data.to_vec(),
        ));
        Ok(WriteAck {
            crc64: checksum::crc64::checksum(data),
            stored_size: data.len() as u64,
        })
    }

    fn repair_placed_shard(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError> {
        self.write_placed_shard(data_pg_id, key, data)
    }

    fn read_placed_shard(
        &self,
        _data_pg_id: DataPgId,
        _key: &ShardKey,
        _expected_ack: WriteAck,
    ) -> Result<Vec<u8>, StoreError> {
        Err(StoreError::Io {
            context: "recording shard client read",
            source: std::io::Error::from(std::io::ErrorKind::Unsupported),
        })
    }

    fn read_placed_shard_for_historical_inspection(
        &self,
        _location: ShardLocation,
        _key: &ShardKey,
        _expected_ack: WriteAck,
    ) -> Result<Vec<u8>, StoreError> {
        Err(StoreError::Io {
            context: "recording shard client historical read",
            source: std::io::Error::from(std::io::ErrorKind::Unsupported),
        })
    }

    fn read_placed_shard_into(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        expected_ack: WriteAck,
        dst: &mut [u8],
    ) -> Result<(), StoreError> {
        let data = self.read_placed_shard(data_pg_id, key, expected_ack)?;
        dst.copy_from_slice(&data);
        Ok(())
    }

    fn delete_placed_shard(
        &self,
        _data_pg_id: DataPgId,
        _key: &ShardKey,
    ) -> Result<(), StoreError> {
        Ok(())
    }
}

struct StorageNodeServerGuard {
    stop: Arc<std::sync::atomic::AtomicBool>,
    socket_path: std::path::PathBuf,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for StorageNodeServerGuard {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        let _ = std::os::unix::net::UnixStream::connect(&self.socket_path);
        if let Some(thread) = self.thread.take() {
            if let Err(panic) = thread.join() {
                if std::thread::panicking() {
                    return;
                }
                std::panic::resume_unwind(panic);
            }
        }
    }
}

fn spawn_storage_node_server(server: StorageNodeServer) -> StorageNodeServerGuard {
    let socket_path = server.socket_path_for_test();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let thread = thread::spawn(move || {
        server.serve_until_stop_for_test(&thread_stop).unwrap();
    });
    StorageNodeServerGuard {
        stop,
        socket_path,
        thread: Some(thread),
    }
}

fn spawn_shared_storage_node_server(server: Arc<StorageNodeServer>) -> StorageNodeServerGuard {
    let socket_path = server.socket_path_for_test();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let thread = thread::spawn(move || {
        server.serve_until_stop_for_test(&thread_stop).unwrap();
    });
    StorageNodeServerGuard {
        stop,
        socket_path,
        thread: Some(thread),
    }
}

static UNIX_CLIENT_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn unix_client_tempdir() -> (std::sync::MutexGuard<'static, ()>, test_util::TempDir) {
    let guard = UNIX_CLIENT_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    (guard, test_util::tempdir())
}

#[test]
fn unix_broad_payload_lease_survives_frontend_runtime_map_refresh() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(0);
    let node_ids = [node_id];
    let pg_id = PgId::new(0);
    let ec_shape = EcShape { k: 1, m: 0 };
    let epoch = ClusterEpoch::INITIAL;
    let socket_path = tmp.path().join("sockets").join("lease-refresh.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let route = StorageNodePgRoute {
        pg_id: pg_id.get(),
        cluster_epoch: epoch,
        state: PgState::Active,
        primary_node_id: node_id,
        acting_set: node_ids.to_vec(),
    };
    let _server = spawn_storage_node_server(
        StorageNodeServer::bind(StorageNodeProcessConfig {
            node_id,
            cluster_epoch: epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: tmp.path().join("lease-refresh-node"),
            default_ec_shape: ec_shape,
            pg_ids: vec![pg_id.get()],
            socket_path: socket_path.clone(),
            pg_routes: vec![route],
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: Vec::new(),
        })
        .unwrap(),
    );

    let open_frontend_map = || {
        let mut map = LocalClusterMap::open_frontend_topology_only_with_epoch(
            node_id,
            node_ids,
            &[pg_id.get()],
            ec_shape,
            epoch,
        )
        .unwrap();
        map.install_unix_storage_node_clients([LocalUnixStorageNodeClientConfig::new(
            node_id,
            socket_path.clone(),
        )])
        .unwrap();
        map
    };
    let map_a = Arc::new(open_frontend_map());
    let cluster_a = StorageCluster::from_local_map(Arc::clone(&map_a)).unwrap();
    let bucket = BucketName::new("lease-refresh-bucket").unwrap();
    let key = ObjectKey::new("source").unwrap();
    let original = write_committed_direct_segment_for(&cluster_a, &bucket, &key, b"original");
    let broad_lease = cluster_a
        .acquire_object_payload_lease(&bucket, &key, original.generation_id)
        .unwrap();
    write_committed_direct_segment_for(&cluster_a, &bucket, &key, b"replacement");
    assert!(cluster_a
        .payload_reclaim_exists(&bucket, &key, original.generation_id)
        .unwrap());

    let mut map_b = open_frontend_map();
    map_b.inherit_process_local_state_from(&map_a);
    let cluster_b = StorageCluster::from_local_map(Arc::new(map_b)).unwrap();
    assert_eq!(
        cluster_b
            .reclaim_object_payload_if_unleased_with_outcome(&bucket, &key, original.generation_id,)
            .unwrap(),
        crate::cluster::ObjectPayloadReclaimAttempt::Deferred,
        "refreshed frontend must observe the lease held through the original Unix client map"
    );

    drop(broad_lease);
    assert_eq!(
        cluster_b
            .reclaim_object_payload_if_unleased_with_outcome(&bucket, &key, original.generation_id,)
            .unwrap(),
        crate::cluster::ObjectPayloadReclaimAttempt::Completed,
        "reclaim should proceed through the refreshed map after lease release"
    );
}

#[test]
fn unix_broad_payload_lease_saturation_preserves_read_handle_handoff_capacity() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(0);
    let pg_id = PgId::new(0);
    let epoch = ClusterEpoch::INITIAL;
    let ec_shape = EcShape { k: 1, m: 0 };
    let socket_path = tmp.path().join("sockets").join("lease-admission.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let _server = spawn_storage_node_server(
        StorageNodeServer::bind(StorageNodeProcessConfig {
            node_id,
            cluster_epoch: epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: tmp.path().join("lease-admission-node"),
            default_ec_shape: ec_shape,
            pg_ids: vec![pg_id.get()],
            socket_path: socket_path.clone(),
            pg_routes: vec![StorageNodePgRoute {
                pg_id: pg_id.get(),
                cluster_epoch: epoch,
                state: PgState::Active,
                primary_node_id: node_id,
                acting_set: vec![node_id],
            }],
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: Vec::new(),
        })
        .unwrap(),
    );
    let admission_limit = LocalUnixStorageNodeClientConfig::MIN_RPC_ADMISSION_LIMIT;
    let reserved_rpc_capacity = (admission_limit / 4).clamp(1, 4);
    let shared_non_control_limit = admission_limit - reserved_rpc_capacity;
    let lease_limit = shared_non_control_limit.saturating_sub(1).max(1);
    let broad_lease_limit = lease_limit.saturating_sub(1).max(1);
    let client = UnixStorageNodeClient::with_rpc_admission_settings(
        node_id,
        epoch,
        socket_path,
        LocalUnixStorageNodeClientAdmissionSettings {
            rpc_admission_limit: admission_limit,
            rpc_admission_wait_timeout: Duration::from_millis(20),
            rpc_control_admission_wait_timeout: Duration::from_millis(20),
        },
    );
    let bucket = BucketName::new("lease-admission-bucket").unwrap();
    let object_key = ObjectKey::new("source").unwrap();
    let generation_id = GenerationId::new(1).unwrap();
    let mut broad_leases = Vec::new();
    for _ in 0..broad_lease_limit {
        broad_leases.push(
            client
                .acquire_object_payload_lease(
                    epoch,
                    &bucket,
                    &object_key,
                    generation_id,
                    ObjectPayloadLeaseKind::BroadSnapshot,
                )
                .unwrap()
                .expect("broad lease admission should reach its reserved child limit"),
        );
        assert!(
            client.active_admitted_session_count_for_test() <= admission_limit,
            "broad leases must remain within the aggregate RPC admission limit"
        );
    }
    assert!(matches!(
        client.acquire_object_payload_lease(
            epoch,
            &bucket,
            &object_key,
            generation_id,
            ObjectPayloadLeaseKind::BroadSnapshot,
        ),
        Err(StoreError::StorageRpcResourceExhausted {
            operation: "broad object payload lease acquire",
            ..
        })
    ));

    let data_pg_id = DataPgId::new_for_test(pg_id);
    let shard_key = ShardKey::new(&[0x5A; 16], generation_id.get(), 0);
    let location = ShardLocation::new(epoch, data_pg_id, shard_key.shard_index(), node_id);
    let mut narrow_leases = Vec::new();
    while let Some(broad_lease) = broad_leases.pop() {
        let narrow_lease = client
            .acquire_object_payload_lease(
                epoch,
                &bucket,
                &object_key,
                generation_id,
                ObjectPayloadLeaseKind::ShardLocations,
            )
            .unwrap()
            .expect("a saturated broad lease must retain one shard-lease handoff slot");
        assert!(
            client.active_admitted_session_count_for_test() <= admission_limit,
            "broad-to-narrow overlap must remain within the aggregate admission limit"
        );
        drop(broad_lease);
        narrow_leases.push(narrow_lease);
        assert!(matches!(
            client.acquire_object_payload_lease(
                epoch,
                &bucket,
                &object_key,
                generation_id,
                ObjectPayloadLeaseKind::BroadSnapshot,
            ),
            Err(StoreError::StorageRpcResourceExhausted {
                operation: "broad object payload lease acquire",
                ..
            })
        ));
        assert!(
            client.active_admitted_session_count_for_test() <= admission_limit,
            "new broad leases must not consume the slot reserved for the next handoff"
        );
    }

    narrow_leases.push(
        client
            .acquire_object_payload_lease(
                epoch,
                &bucket,
                &object_key,
                generation_id,
                ObjectPayloadLeaseKind::ShardLocations,
            )
            .unwrap()
            .expect("lease pool should admit a final direct narrow lease"),
    );
    assert!(matches!(
        client.acquire_object_payload_lease(
            epoch,
            &bucket,
            &object_key,
            generation_id,
            ObjectPayloadLeaseKind::ShardLocations,
        ),
        Err(StoreError::StorageRpcResourceExhausted {
            operation: "shard object payload lease acquire",
            ..
        })
    ));

    let read_handle = client
        .acquire_read_handles(
            "narrow-to-read-handle-handoff",
            vec![(location, shard_key.clone())],
        )
        .expect("narrow-lease saturation must retain one read-handle handoff slot");
    assert!(
        client.active_admitted_session_count_for_test() <= admission_limit,
        "narrow-to-read-handle overlap must remain within the aggregate admission limit"
    );
    assert_eq!(
        client
            .object_payload_lease_count(epoch, &bucket, &object_key, generation_id)
            .expect("short lease-control RPC must retain reserved admission"),
        lease_limit
    );
    assert!(client.active_admitted_session_count_for_test() <= admission_limit);
    assert!(
        !client
            .try_begin_object_payload_reclaim(
                epoch,
                &bucket,
                &object_key,
                generation_id,
                &ObjectPayloadReclaimClaimProof {
                    bucket_incarnation_generation: 1,
                    reclaim_kind: ObjectPayloadReclaimKind::ObjectSegments,
                    claim_id: "lease-admission-claim".to_string(),
                    owner_token: "lease-admission-owner".to_string(),
                    cluster_epoch: epoch,
                },
            )
            .expect("reclaim control must retain reserved admission"),
        "storage node must still observe every narrow lease"
    );
    assert!(client.active_admitted_session_count_for_test() <= admission_limit);

    drop(read_handle);
    drop(narrow_leases);
    assert_eq!(client.active_admitted_session_count_for_test(), 0);
    assert_eq!(
        client
            .object_payload_lease_count(epoch, &bucket, &object_key, generation_id)
            .unwrap(),
        0
    );
}

#[test]
fn unix_object_payload_reclaim_fence_rejects_crossed_claim_authority() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(0);
    let pg_id = PgId::new(0);
    let epoch = ClusterEpoch::INITIAL;
    let ec_shape = EcShape { k: 1, m: 0 };
    let socket_path = tmp.path().join("sockets").join("reclaim-authority.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let _server = spawn_storage_node_server(
        StorageNodeServer::bind(StorageNodeProcessConfig {
            node_id,
            cluster_epoch: epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: tmp.path().join("reclaim-authority-node"),
            default_ec_shape: ec_shape,
            pg_ids: vec![pg_id.get()],
            socket_path: socket_path.clone(),
            pg_routes: vec![StorageNodePgRoute {
                pg_id: pg_id.get(),
                cluster_epoch: epoch,
                state: PgState::Active,
                primary_node_id: node_id,
                acting_set: vec![node_id],
            }],
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: Vec::new(),
        })
        .unwrap(),
    );
    let client = UnixStorageNodeClient::new(node_id, epoch, socket_path);
    let bucket = BucketName::new("reclaim-authority-bucket").unwrap();
    let key = ObjectKey::new("source").unwrap();
    let generation_id = GenerationId::new(1).unwrap();
    let first = ObjectPayloadReclaimClaimProof {
        bucket_incarnation_generation: 1,
        reclaim_kind: ObjectPayloadReclaimKind::ObjectSegments,
        claim_id: "claim-a".to_string(),
        owner_token: "owner-a".to_string(),
        cluster_epoch: epoch,
    };
    let second = ObjectPayloadReclaimClaimProof {
        claim_id: "claim-b".to_string(),
        owner_token: "owner-b".to_string(),
        ..first.clone()
    };

    assert!(client
        .try_begin_object_payload_reclaim(epoch, &bucket, &key, generation_id, &first)
        .unwrap());
    let error = client
        .finish_object_payload_reclaim(epoch, &bucket, &key, generation_id, &second, false)
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::StorageRpc {
            code: StorageRpcErrorCode::PayloadDecode,
            ..
        }
    ));
    assert!(client
        .acquire_object_payload_lease(
            epoch,
            &bucket,
            &key,
            generation_id,
            ObjectPayloadLeaseKind::ShardLocations,
        )
        .unwrap()
        .is_none());

    client
        .finish_object_payload_reclaim(epoch, &bucket, &key, generation_id, &first, true)
        .unwrap();
    assert!(client
        .try_begin_object_payload_reclaim(epoch, &bucket, &key, generation_id, &second)
        .unwrap());
    let error = client
        .clear_object_payload_reclaim_fence(epoch, &bucket, &key, generation_id, &first)
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::StorageRpc {
            code: StorageRpcErrorCode::PayloadDecode,
            ..
        }
    ));
    client
        .finish_object_payload_reclaim(epoch, &bucket, &key, generation_id, &second, false)
        .unwrap();
    let mut lease = client
        .acquire_object_payload_lease(
            epoch,
            &bucket,
            &key,
            generation_id,
            ObjectPayloadLeaseKind::ShardLocations,
        )
        .unwrap()
        .expect("matching reclaim finish must remove the fence");
    assert_eq!(lease.release().unwrap(), 0);
}

fn assert_historical_pending_command_recovery_over_unix(
    pre_applied_node_count: usize,
    current_epoch_delta: u64,
    bucket_pg_command: bool,
) {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1)];
    let pg_id = PgId::new(0);
    let ec_shape = EcShape { k: 1, m: 0 };
    let command_epoch = ClusterEpoch::INITIAL;
    assert!(pre_applied_node_count <= node_ids.len());
    let current_epoch = ClusterEpoch::new(command_epoch.get() + current_epoch_delta).unwrap();
    let bucket = BucketName::new(format!(
        "unix-pending-recovery-{pre_applied_node_count}-{current_epoch_delta}"
    ))
    .unwrap();
    let key = ObjectKey::new("key").unwrap();
    let command = if bucket_pg_command {
        create_bucket_metadata_command_with_epoch(pg_id, 1, bucket.clone(), command_epoch)
    } else {
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                command_epoch,
                pg_id,
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                bucket.clone(),
                key.clone(),
                crate::VersionId::from_u64(1),
            )),
        )
    };
    let pending = crate::control_plane::PendingMetadataCommandObservation::new(
        command_epoch,
        std::num::NonZeroU64::MIN,
        command.checksum_crc64(),
    );
    let recovery =
        crate::control_plane::PendingMetadataCommandRecovery::new(NodeId::new(0), pending);
    let historical_route = StorageNodePgRoute {
        pg_id: pg_id.get(),
        cluster_epoch: command_epoch,
        state: PgState::Active,
        primary_node_id: NodeId::new(0),
        acting_set: node_ids.to_vec(),
    };
    let current_route = StorageNodePgRoute {
        pg_id: pg_id.get(),
        cluster_epoch: current_epoch,
        state: PgState::Peering,
        primary_node_id: NodeId::new(0),
        acting_set: node_ids.to_vec(),
    };

    let mut client_configs = Vec::new();
    let mut server_configs = Vec::new();
    for node_id in node_ids {
        let data_dir = tmp
            .path()
            .join(format!("historical-recovery-node-{}", node_id.as_u32()));
        let socket_path = tmp.path().join("sockets").join(format!(
            "historical-recovery-node-{}.sock",
            node_id.as_u32()
        ));
        private_socket_dir(socket_path.parent().unwrap());
        {
            let node =
                SharedStorageNode::open_with_default_ec_shape(&data_dir, &[pg_id.get()], ec_shape)
                    .unwrap();
            let pg = node.get_pg(pg_id.get()).unwrap();
            if node_id == NodeId::new(0) {
                pg.try_insert_pending_metadata_command_slot(
                    NodeId::new(0).as_u32(),
                    &command,
                    Some(&bucket),
                )
                .unwrap();
            }
            if (node_id.as_u32() as usize) < pre_applied_node_count {
                pg.apply_metadata_command_and_record(NodeId::new(0).as_u32(), &command)
                    .unwrap();
            }
        }
        client_configs.push(LocalUnixStorageNodeClientConfig::new(
            node_id,
            socket_path.clone(),
        ));
        server_configs.push(StorageNodeProcessConfig {
            node_id,
            cluster_epoch: current_epoch,
            route_map_validity: RouteMapValidity::until_ms_saturating(
                crate::clock::current_time_millis().saturating_add(60_000),
            ),
            data_dir,
            default_ec_shape: ec_shape,
            pg_ids: vec![pg_id.get()],
            socket_path,
            pg_routes: vec![current_route.clone()],
            historical_pg_routes: vec![historical_route.clone()],
            pending_metadata_command_recoveries: vec![(pg_id, recovery)],
        });
    }

    let mut server_guards = Vec::new();
    for config in server_configs.iter().cloned() {
        server_guards.push(spawn_storage_node_server(
            StorageNodeServer::bind(config).unwrap(),
        ));
    }
    let mut historical_map = LocalClusterMap::open_frontend_topology_only_with_pg_routes(
        NodeId::new(0),
        node_ids,
        &[pg_id.get()],
        ec_shape,
        command_epoch,
        [LocalPgRoute::from(&PgRouteSnapshot::reconstructed(
            command_epoch,
            pg_id,
            NodeId::new(0),
            node_ids.to_vec(),
            PgState::Active,
        ))],
    )
    .unwrap();
    historical_map
        .install_unix_storage_node_clients(client_configs)
        .unwrap();
    let cluster = StorageCluster::from_local_map(Arc::new(historical_map)).unwrap();

    assert_eq!(
        cluster
            .drain_pending_metadata_command_with_recovery_gate(pg_id, &command)
            .unwrap(),
        PendingMetadataCommandOutcome::Applied
    );
    drop(cluster);
    drop(server_guards);

    let mut expected_replica_state = None;
    for config in server_configs {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &[pg_id.get()],
            ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(pg_id.get()).unwrap();
        let replica_state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(
            replica_state.applied_log_index,
            1,
            "node {} did not converge the historical command",
            config.node_id.as_u32()
        );
        if let Some(expected) = expected_replica_state {
            assert_eq!(
                replica_state,
                expected,
                "node {} converged to a different metadata command replica state",
                config.node_id.as_u32()
            );
        } else {
            expected_replica_state = Some(replica_state);
        }
        if bucket_pg_command {
            let bucket_info = crate::PgMetadataStore::head_bucket(&*pg, &bucket).unwrap();
            assert_eq!(bucket_info.name, bucket);
        } else {
            assert_eq!(
                crate::PgMetadataStore::next_version_id(&*pg, &bucket, &key).unwrap(),
                crate::VersionId::from_u64(2),
                "node {} did not apply the object-version reservation",
                config.node_id.as_u32()
            );
        }
        if config.node_id == NodeId::new(0) {
            assert!(
                pg.pending_metadata_command_envelope(NodeId::new(0).as_u32(), command_epoch)
                    .unwrap()
                    .is_none(),
                "historical primary retained the terminal pending slot"
            );
        }
    }
}

#[test]
fn unix_historical_recovery_clears_fully_applied_pending_command() {
    assert_historical_pending_command_recovery_over_unix(2, 1, true);
}

#[test]
fn unix_historical_recovery_converges_partial_pending_command() {
    assert_historical_pending_command_recovery_over_unix(1, 1, false);
}

#[test]
fn unix_historical_recovery_reissues_then_cleans_stale_stream_generation() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(0);
    let node_ids = [node_id];
    let pg_ids = [0, 1];
    let ec_shape = EcShape { k: 1, m: 0 };
    let command_epoch = ClusterEpoch::INITIAL;
    let current_epoch = ClusterEpoch::new(command_epoch.get() + 1).unwrap();
    let placement_map = LocalClusterMap::open_frontend_topology_only_with_epoch(
        node_id,
        node_ids,
        &pg_ids,
        ec_shape,
        command_epoch,
    )
    .unwrap();
    let (bucket, key, pg_id, bucket_pg_id) = (0..1_000)
        .find_map(|index| {
            let bucket = BucketName::new(format!("unix-reissued-stream-cleanup-{index}")).unwrap();
            let key = ObjectKey::new("object").unwrap();
            let bucket_pg_id = PgId::new(placement_map.bucket_pg_for(&bucket));
            let pg_id = PgId::new(placement_map.object_pg_for(&bucket, &key));
            (bucket_pg_id != pg_id).then_some((bucket, key, pg_id, bucket_pg_id))
        })
        .expect("two-PG topology must place one test object separately from its bucket metadata");
    drop(placement_map);
    let session_id =
        crate::SessionId::try_from("78787878787878787878787878787878".to_string()).unwrap();
    let now_ms = crate::clock::current_time_millis();
    let source = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            command_epoch,
            pg_id,
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::CreateStreamUpload(Box::new(
            crate::metadata_command::CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                crate::CreateStreamUploadReq {
                    session_id: session_id.clone(),
                    bucket: bucket.clone(),
                    key: key.clone(),
                    target: crate::StreamUploadTarget::PutObject,
                    encryption: crate::ObjectEncryption::None,
                },
                now_ms,
                crate::BucketWriteReservationProof {
                    bucket: bucket.clone(),
                    reservation_id: "missing-recovery-reservation".to_string(),
                    owner_token: "recovery-owner".to_string(),
                    cluster_epoch: command_epoch,
                    bucket_execution_generation: 1,
                    bucket_incarnation_generation: 1,
                    operation_kind: "historical-recovery".to_string(),
                    created_at: now_ms,
                    lease_deadline: now_ms.saturating_add(60_000),
                    target_context: Some(key.as_str().to_string()),
                },
            ),
        )),
    );
    let reissued = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            command_epoch,
            pg_id,
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
        source.payload().clone(),
    );
    let cleanup = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            command_epoch,
            pg_id,
            MetadataCommandLogIndex::new(3).unwrap(),
        ),
        source
            .payload()
            .abandoned_recovery_follow_up()
            .expect("PutObject stream creation requires generation cleanup"),
    );
    let pending = crate::control_plane::PendingMetadataCommandObservation::new(
        command_epoch,
        std::num::NonZeroU64::MIN,
        source.checksum_crc64(),
    );
    let recovery = crate::control_plane::PendingMetadataCommandRecovery::new(node_id, pending);
    let historical_routes = pg_ids.map(|raw_pg_id| StorageNodePgRoute {
        pg_id: raw_pg_id,
        cluster_epoch: command_epoch,
        state: PgState::Active,
        primary_node_id: node_id,
        acting_set: node_ids.to_vec(),
    });
    let current_routes = pg_ids.map(|raw_pg_id| StorageNodePgRoute {
        pg_id: raw_pg_id,
        cluster_epoch: current_epoch,
        state: if raw_pg_id == pg_id.get() {
            PgState::Peering
        } else {
            PgState::Active
        },
        primary_node_id: node_id,
        acting_set: node_ids.to_vec(),
    });
    let data_dir = tmp.path().join("reissued-stream-cleanup-node");
    let socket_path = tmp
        .path()
        .join("sockets")
        .join("reissued-stream-cleanup.sock");
    private_socket_dir(socket_path.parent().unwrap());
    {
        let node =
            SharedStorageNode::open_with_default_ec_shape(&data_dir, &pg_ids, ec_shape).unwrap();
        let owner = crate::OwnerIdentity::from_principal("owner");
        let acl_grants = crate::AclGrants::default();
        let create_bucket = crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        };
        for raw_pg_id in pg_ids {
            let pg = node.get_pg(raw_pg_id).unwrap();
            pg.create_bucket_with_config(&create_bucket).unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
        }
        let pg = node.get_pg(pg_id.get()).unwrap();
        crate::PgMetadataStore::reserve_object_generation(&*pg, &bucket, &key, &session_id)
            .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        pg.try_insert_pending_metadata_command_slot(node_id.as_u32(), &source, Some(&bucket))
            .unwrap();
    }
    let route_map_validity = RouteMapValidity::until_ms_saturating(now_ms.saturating_add(60_000));
    let initial_server_config = StorageNodeProcessConfig {
        node_id,
        cluster_epoch: command_epoch,
        route_map_validity,
        data_dir: data_dir.clone(),
        default_ec_shape: ec_shape,
        pg_ids: pg_ids.to_vec(),
        socket_path: socket_path.clone(),
        pg_routes: historical_routes.to_vec(),
        historical_pg_routes: Vec::new(),
        pending_metadata_command_recoveries: Vec::new(),
    };
    let server = Arc::new(StorageNodeServer::bind(initial_server_config).unwrap());
    let server_guard = spawn_shared_storage_node_server(Arc::clone(&server));
    let client_config = LocalUnixStorageNodeClientConfig::new(node_id, socket_path);

    let historical_snapshots: Vec<_> = pg_ids
        .map(|raw_pg_id| {
            PgRouteSnapshot::reconstructed(
                command_epoch,
                PgId::new(raw_pg_id),
                node_id,
                node_ids.to_vec(),
                PgState::Active,
            )
        })
        .into();
    let mut historical_map = LocalClusterMap::open_frontend_topology_only_with_pg_routes(
        node_id,
        node_ids,
        &pg_ids,
        ec_shape,
        command_epoch,
        historical_snapshots.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    historical_map
        .install_unix_storage_node_clients([client_config.clone()])
        .unwrap();
    let historical_cluster = StorageCluster::from_local_map(Arc::new(historical_map)).unwrap();
    let conflict = MetadataCommandEnvelope::new(
        source.id(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            ObjectKey::new("conflicting-object").unwrap(),
            crate::SessionId::try_from("79797979797979797979797979797979".to_string()).unwrap(),
            GenerationId::MIN,
            now_ms,
        )),
    );
    historical_cluster
        .test_apply_metadata_command_to_acting_set_from_origin(node_id, &conflict)
        .unwrap();

    server
        .install_control_plane_runtime_config(StorageNodeProcessConfig {
            node_id,
            cluster_epoch: current_epoch,
            route_map_validity,
            data_dir: data_dir.clone(),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.to_vec(),
            socket_path: server_guard.socket_path.clone(),
            pg_routes: current_routes.to_vec(),
            historical_pg_routes: historical_routes.to_vec(),
            pending_metadata_command_recoveries: vec![(pg_id, recovery)],
        })
        .unwrap();

    let current_snapshots: Vec<_> = pg_ids
        .map(|raw_pg_id| {
            PgRouteSnapshot::reconstructed(
                current_epoch,
                PgId::new(raw_pg_id),
                node_id,
                node_ids.to_vec(),
                if raw_pg_id == pg_id.get() {
                    PgState::Peering
                } else {
                    PgState::Active
                },
            )
        })
        .into();
    let mut current_map = LocalClusterMap::open_frontend_topology_only_with_pg_routes(
        node_id,
        node_ids,
        &pg_ids,
        ec_shape,
        current_epoch,
        current_snapshots.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    current_map.test_install_historical_pg_routes(historical_snapshots);
    current_map
        .install_unix_storage_node_clients([client_config])
        .unwrap();
    let current_cluster = StorageCluster::from_local_map(Arc::new(current_map)).unwrap();

    assert_eq!(
        historical_cluster
            .drain_pending_metadata_command_with_authorized_recovery_route(
                pg_id,
                &source,
                &current_cluster,
            )
            .unwrap(),
        PendingMetadataCommandOutcome::Abandoned
    );
    drop(historical_cluster);
    drop(current_cluster);
    drop(server_guard);

    let node = SharedStorageNode::open_with_default_ec_shape(&data_dir, &pg_ids, ec_shape).unwrap();
    let pg = node.get_pg(pg_id.get()).unwrap();
    assert!(pg
        .metadata_command_abandoned(node_id.as_u32(), &reissued)
        .unwrap());
    assert_eq!(
        pg.metadata_command_replica_state()
            .unwrap()
            .applied_log_index,
        cleanup.id().log_index().get()
    );
    assert_eq!(
        pg.pending_metadata_command_envelope(node_id.as_u32(), command_epoch)
            .unwrap(),
        None
    );
    assert!(matches!(
        crate::PgMetadataStore::get_object_generation_reservation(&*pg, &bucket, &key, &session_id,),
        Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
    ));
    assert_ne!(pg_id, bucket_pg_id);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]

    #[test]
    fn prop_unix_historical_recovery_converges_every_applied_prefix_across_epoch_gaps(
        pre_applied_node_count in 0_usize..=2,
        current_epoch_delta in 1_u64..32,
        bucket_pg_command in any::<bool>(),
    ) {
        assert_historical_pending_command_recovery_over_unix(
            pre_applied_node_count,
            current_epoch_delta,
            bucket_pg_command,
        );
    }
}

#[test]
fn payload_shard_writes_route_through_pluggable_shard_client() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
    let recording_client = Arc::new(RecordingPlacedShardClient::new(NodeId::new(1)));
    let recording_client_for_assert = Arc::clone(&recording_client);
    map.replace_shard_client_for_tests(NodeId::new(1), recording_client);

    let data_pg_id = DataPgId::new_for_test(PgId::new(0));
    let key = ShardKey::new(&[0x41; 16], 77, 0);
    let location = ShardLocation::new(
        ClusterEpoch::INITIAL,
        data_pg_id,
        key.shard_index(),
        NodeId::new(1),
    );
    let payload = b"remote-shard-client-plumbing";
    let ack = map
        .write_payload_shard(ClusterEpoch::INITIAL, location, &key, payload)
        .unwrap();

    assert_eq!(ack.stored_size, payload.len() as u64);
    assert_eq!(ack.crc64, checksum::crc64::checksum(payload));
    let writes = recording_client_for_assert
        .writes
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0], (data_pg_id, key, payload.to_vec()));
}

struct RecordingShardAckClient {
    records: Mutex<Vec<(DataPgId, ShardKey, WriteAck)>>,
    validates: Mutex<Vec<(DataPgId, ShardKey, WriteAck)>>,
}

impl RecordingShardAckClient {
    fn new() -> Self {
        Self {
            records: Mutex::new(Vec::new()),
            validates: Mutex::new(Vec::new()),
        }
    }
}

impl ShardAckNodeClient for RecordingShardAckClient {
    fn register_written_shard_acks(
        &self,
        data_pg_id: DataPgId,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), StoreError> {
        let mut records = self.records.lock().unwrap_or_else(|e| e.into_inner());
        for (key, ack) in shard_batch {
            records.push((data_pg_id, (*key).clone(), *ack));
        }
        Ok(())
    }

    fn validate_written_shard_ack(
        &self,
        data_pg_id: DataPgId,
        key: &ShardKey,
        ack: WriteAck,
    ) -> Result<(), StoreError> {
        self.validates
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((data_pg_id, key.clone(), ack));
        Ok(())
    }

    fn load_written_shard_ack(
        &self,
        _data_pg_id: DataPgId,
        _key: &ShardKey,
    ) -> Result<WriteAck, StoreError> {
        Err(StoreError::NotFound)
    }

    fn load_written_shard_ack_for_historical_inspection(
        &self,
        _route_cluster_epoch: ClusterEpoch,
        data_pg_id: DataPgId,
        key: &ShardKey,
    ) -> Result<WriteAck, StoreError> {
        self.load_written_shard_ack(data_pg_id, key)
    }

    fn delete_written_shard_ack(
        &self,
        _data_pg_id: DataPgId,
        _key: &ShardKey,
    ) -> Result<(), StoreError> {
        Err(StoreError::NotFound)
    }

    fn record_placed_segment_shard_repair(
        &self,
        _data_pg_id: DataPgId,
        _work_item: &PlacedSegmentShardRepairWorkItem,
        _last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        Ok(())
    }

    fn list_placed_segment_shard_repairs(
        &self,
        _data_pg_id: DataPgId,
    ) -> Result<Vec<PlacedSegmentShardRepairRecord>, StoreError> {
        Ok(Vec::new())
    }

    fn acquire_placed_segment_shard_repair_claim(
        &self,
        _data_pg_id: DataPgId,
        _request: &PlacedSegmentShardRepairClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardRepairClaimRecord>, StoreError> {
        Ok(None)
    }

    fn complete_placed_segment_shard_repair_claim(
        &self,
        _data_pg_id: DataPgId,
        _cluster_epoch: ClusterEpoch,
        _claim: &PlacedSegmentShardRepairClaimRecord,
    ) -> Result<bool, StoreError> {
        Ok(false)
    }

    fn record_placed_segment_shard_repair_claim_error(
        &self,
        _data_pg_id: DataPgId,
        _cluster_epoch: ClusterEpoch,
        _claim: &PlacedSegmentShardRepairClaimRecord,
        _last_error: &str,
        _next_attempt_after: u64,
    ) -> Result<bool, StoreError> {
        Ok(false)
    }

    fn resolve_placed_segment_shard_repair(
        &self,
        _data_pg_id: DataPgId,
        _work_item: &PlacedSegmentShardRepairWorkItem,
    ) -> Result<(), StoreError> {
        Ok(())
    }

    fn record_placed_segment_shard_backfill(
        &self,
        _data_pg_id: DataPgId,
        _work_item: &PlacedSegmentShardBackfillWorkItem,
        _remaining_tolerance: u8,
        _last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        Ok(())
    }

    fn list_placed_segment_shard_backfills(
        &self,
        _data_pg_id: DataPgId,
    ) -> Result<Vec<PlacedSegmentShardBackfillRecord>, StoreError> {
        Ok(Vec::new())
    }

    fn count_placed_segment_shard_backfills(
        &self,
        _data_pg_id: DataPgId,
    ) -> Result<usize, StoreError> {
        Ok(0)
    }

    fn placed_segment_shard_backfill_exists(
        &self,
        _data_pg_id: DataPgId,
        _work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<bool, StoreError> {
        Ok(false)
    }

    fn acquire_placed_segment_shard_backfill_claim(
        &self,
        _data_pg_id: DataPgId,
        _request: &PlacedSegmentShardBackfillClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardBackfillClaimRecord>, StoreError> {
        Ok(None)
    }

    fn complete_placed_segment_shard_backfill_claim(
        &self,
        _data_pg_id: DataPgId,
        _cluster_epoch: ClusterEpoch,
        _claim: &PlacedSegmentShardBackfillClaimRecord,
    ) -> Result<bool, StoreError> {
        Ok(false)
    }

    fn record_placed_segment_shard_backfill_claim_error(
        &self,
        _data_pg_id: DataPgId,
        _cluster_epoch: ClusterEpoch,
        _claim: &PlacedSegmentShardBackfillClaimRecord,
        _last_error: &str,
        _next_attempt_after: u64,
    ) -> Result<bool, StoreError> {
        Ok(false)
    }

    fn resolve_placed_segment_shard_backfill(
        &self,
        _data_pg_id: DataPgId,
        _work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<(), StoreError> {
        Ok(())
    }
}

#[test]
fn metadata_pg_primary_exposes_pluggable_shard_ack_client() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let recording_client = Arc::new(RecordingShardAckClient::new());
    let recording_client_for_assert = Arc::clone(&recording_client);
    map.replace_shard_ack_client_for_tests(NodeId::new(2), recording_client);
    set_route_primary(&mut map, 1, NodeId::new(2));

    let pg_id = PgId::new(1);
    let data_pg_id = DataPgId::new_for_test(pg_id);
    let key = ShardKey::new(&[0x51; 16], 88, 0);
    let ack = WriteAck {
        crc64: 1234,
        stored_size: 5678,
    };
    let node = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    node.shard_ack_client()
        .register_written_shard_acks(data_pg_id, &[(&key, ack)])
        .unwrap();
    node.shard_ack_client()
        .validate_written_shard_ack(data_pg_id, &key, ack)
        .unwrap();

    assert_eq!(
        *recording_client_for_assert
            .records
            .lock()
            .unwrap_or_else(|e| e.into_inner()),
        vec![(data_pg_id, key.clone(), ack)]
    );
    assert_eq!(
        *recording_client_for_assert
            .validates
            .lock()
            .unwrap_or_else(|e| e.into_inner()),
        vec![(data_pg_id, key, ack)]
    );
}

#[derive(Default)]
struct ReadHandleEvents {
    acquires: Mutex<Vec<Vec<ShardLocation>>>,
    releases: Mutex<Vec<Vec<ShardLocation>>>,
}

struct RecordingReadHandleClient {
    fail_acquire: bool,
    events: Arc<ReadHandleEvents>,
}

impl RecordingReadHandleClient {
    fn new(fail_acquire: bool) -> Self {
        Self {
            fail_acquire,
            events: Arc::new(ReadHandleEvents::default()),
        }
    }
}

struct RecordingReadHandleLease {
    locations: Vec<ShardLocation>,
    events: Arc<ReadHandleEvents>,
    released: bool,
}

impl ShardReadHandleNodeClient for RecordingReadHandleClient {
    fn acquire_read_handles(
        &self,
        _read_operation_id: &str,
        entries: Vec<(ShardLocation, ShardKey)>,
    ) -> Result<Box<dyn crate::node_client::ShardReadHandleLease>, StoreError> {
        let locations: Vec<ShardLocation> = entries.iter().map(|(location, _)| *location).collect();
        self.events
            .acquires
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(locations.clone());
        if self.fail_acquire {
            return Err(StoreError::Io {
                context: "recording read handle acquire",
                source: std::io::Error::from(std::io::ErrorKind::WouldBlock),
            });
        }
        Ok(Box::new(RecordingReadHandleLease {
            locations,
            events: Arc::clone(&self.events),
            released: false,
        }))
    }
}

impl crate::node_client::ShardReadHandleLease for RecordingReadHandleLease {
    fn release(&mut self) -> Result<(), StoreError> {
        if !self.released {
            self.events
                .releases
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(self.locations.clone());
            self.released = true;
        }
        Ok(())
    }
}

#[test]
fn partial_multi_node_read_handle_acquire_failure_releases_prior_handles() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
    let ok_client = Arc::new(RecordingReadHandleClient::new(false));
    let ok_events = Arc::clone(&ok_client.events);
    let failing_client = Arc::new(RecordingReadHandleClient::new(true));
    let failing_events = Arc::clone(&failing_client.events);
    map.replace_shard_read_handle_client_for_tests(NodeId::new(1), ok_client);
    map.replace_shard_read_handle_client_for_tests(NodeId::new(2), failing_client);

    let data_pg_id = DataPgId::new_for_test(PgId::new(0));
    let key_0 = ShardKey::new(&[0x57; 16], 91, 0);
    let key_1 = ShardKey::new(&[0x58; 16], 91, 1);
    let location_0 = ShardLocation::new(
        ClusterEpoch::INITIAL,
        data_pg_id,
        key_0.shard_index(),
        NodeId::new(1),
    );
    let location_1 = ShardLocation::new(
        ClusterEpoch::INITIAL,
        data_pg_id,
        key_1.shard_index(),
        NodeId::new(2),
    );

    let err = match map.acquire_payload_shard_read_handles(
        ClusterEpoch::INITIAL,
        &[(location_0, key_0), (location_1, key_1)],
    ) {
        Ok(_) => panic!("expected later-node read handle acquire to fail"),
        Err(error) => error,
    };

    assert!(matches!(
        err,
        ShardIoError::Store {
            node_id: 2,
            pg_id: 0,
            ..
        }
    ));
    assert_eq!(
        *ok_events.acquires.lock().unwrap_or_else(|e| e.into_inner()),
        vec![vec![location_0]]
    );
    assert_eq!(
        *ok_events.releases.lock().unwrap_or_else(|e| e.into_inner()),
        vec![vec![location_0]]
    );
    assert_eq!(
        *failing_events
            .acquires
            .lock()
            .unwrap_or_else(|e| e.into_inner()),
        vec![vec![location_1]]
    );
    assert!(failing_events
        .releases
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .is_empty());
}

fn private_socket_dir(path: &Path) {
    std::fs::create_dir_all(path).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
}

#[test]
fn unix_shard_clients_route_payload_io_and_ack_rows_to_storage_node() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let frontend_dir = tmp.path().join("frontend");
    let mut map = LocalClusterMap::open(&frontend_dir, &node_ids, &[0], ec_shape).unwrap();
    set_route_primary(&mut map, 0, NodeId::new(1));

    let socket_path = tmp.path().join("sockets").join("node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let server_config = StorageNodeProcessConfig {
        node_id: NodeId::new(1),
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: tmp.path().join("remote-node-1"),
        default_ec_shape: ec_shape,
        pg_ids: vec![0],
        socket_path: socket_path.clone(),
        pg_routes: vec![StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Active,
            primary_node_id: NodeId::new(1),
            acting_set: node_ids.to_vec(),
        }],

        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    };
    let server = Arc::new(StorageNodeServer::bind(server_config.clone()).unwrap());
    let server_threads: Vec<_> = (0..15)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    map.install_unix_shard_clients([LocalUnixShardNodeClientConfig::new(
        NodeId::new(1),
        socket_path,
    )])
    .unwrap();

    let data_pg_id = DataPgId::new_for_test(PgId::new(0));
    let key = ShardKey::new(&[0x61; 16], 99, 0);
    let location = ShardLocation::new(
        ClusterEpoch::INITIAL,
        data_pg_id,
        key.shard_index(),
        NodeId::new(1),
    );
    let payload = b"payload routed over unix socket";
    let ack = map
        .write_payload_shard(ClusterEpoch::INITIAL, location, &key, payload)
        .unwrap();
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(0))
        .unwrap();
    primary
        .shard_ack_client()
        .register_written_shard_acks(data_pg_id, &[(&key, ack)])
        .unwrap();
    primary
        .shard_ack_client()
        .validate_written_shard_ack(data_pg_id, &key, ack)
        .unwrap();
    assert_eq!(
        map.read_payload_shard(ClusterEpoch::INITIAL, location, &key, ack)
            .unwrap(),
        payload
    );
    assert_eq!(server.read_handle_count(location), 0);
    let mut read_into = vec![0; payload.len()];
    map.read_payload_shard_into(ClusterEpoch::INITIAL, location, &key, ack, &mut read_into)
        .unwrap();
    assert_eq!(read_into, payload);
    assert_eq!(server.read_handle_count(location), 0);
    let scan = map
        .node(NodeId::new(1))
        .unwrap()
        .shard_scavenger_client()
        .list_scavenger_shard_files(data_pg_id)
        .unwrap();
    assert_eq!(scan.files.len(), 1);
    assert_eq!(scan.files[0].key, key);
    assert_eq!(scan.files[0].size, payload.len() as u64);
    assert!(scan.errors.is_empty());
    assert!(map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .list_scavenger_shard_files(0)
        .unwrap()
        .files
        .is_empty());
    let remote_rows = map
        .node(NodeId::new(1))
        .unwrap()
        .shard_scavenger_client()
        .list_scavenger_shard_rows(data_pg_id)
        .unwrap();
    assert_eq!(remote_rows.len(), 1);
    assert_eq!(remote_rows[0].key, key);
    assert_eq!(remote_rows[0].ack, ack);
    assert!(map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(0)
        .unwrap()
        .list_scavenger_shard_rows()
        .unwrap()
        .is_empty());
    let observation_key = crate::ShardScavengerObservationKey {
        node_id: 1,
        data_pg_id: 0,
        shard_index: key.shard_index(),
        shard_key: key.clone(),
    };
    map.node(NodeId::new(1))
        .unwrap()
        .shard_scavenger_client()
        .record_shard_scavenger_observation(
            data_pg_id,
            &crate::ShardScavengerObservationRecord {
                key: observation_key.clone(),
                data_size: Some(payload.len() as u64),
                crc64: Some(ack.crc64),
                file_exists: true,
                shard_row_exists: true,
                reason: crate::ShardScavengerObservationReason::UnreferencedShardRowAndFile,
                last_error: None,
            },
        )
        .unwrap();
    let observations = map
        .node(NodeId::new(1))
        .unwrap()
        .shard_scavenger_client()
        .list_shard_scavenger_observations(data_pg_id)
        .unwrap();
    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].key, observation_key);
    assert!(map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(0)
        .unwrap()
        .list_shard_scavenger_observations()
        .unwrap()
        .is_empty());
    map.node(NodeId::new(1))
        .unwrap()
        .shard_scavenger_client()
        .resolve_shard_scavenger_observation(data_pg_id, &observation_key)
        .unwrap();
    let missing_key = ShardKey::new(&[0x62; 16], 100, 0);
    let err = map
        .read_payload_shard(ClusterEpoch::INITIAL, location, &missing_key, ack)
        .unwrap_err();
    assert!(matches!(
        err,
        ShardIoError::Store {
            source: StoreError::NotFound,
            ..
        }
    ));
    assert_eq!(server.read_handle_count(location), 0);
    map.delete_payload_shard(ClusterEpoch::INITIAL, location, &key)
        .unwrap();
    for thread in server_threads {
        thread.join().unwrap();
    }

    let remote = SharedStorageNode::open_with_default_ec_shape(
        &server_config.data_dir,
        &server_config.pg_ids,
        server_config.default_ec_shape,
    )
    .unwrap();
    let remote_pg = remote.get_pg(0).unwrap();
    remote_pg.validate_written_shard_ack(&key, ack).unwrap();
    assert!(matches!(
        map.node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .read_shard_file(0, &key),
        Err(StoreError::NotFound)
    ));
    assert!(matches!(
        remote.read_shard_file(0, &key),
        Err(StoreError::NotFound)
    ));
}

#[test]
fn frontend_unix_shard_mode_uses_storage_node_owned_data_dir() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let remote_data_dir = tmp.path().join("remote-node-1-owned");
    let socket_path = tmp.path().join("sockets").join("owned-node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let server_config = StorageNodeProcessConfig {
        node_id: NodeId::new(1),
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: remote_data_dir.clone(),
        default_ec_shape: ec_shape,
        pg_ids: vec![0],
        socket_path: socket_path.clone(),
        pg_routes: vec![StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Active,
            primary_node_id: NodeId::new(1),
            acting_set: node_ids.to_vec(),
        }],

        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    };
    let server = StorageNodeServer::bind(server_config.clone()).unwrap();
    assert!(remote_data_dir.join(".argmin-storage-node.lock").is_file());
    let server_thread = thread::spawn(move || server.accept_one().unwrap());

    let frontend_dir = tmp.path().join("frontend-only-shard-routing");
    let mut map = LocalClusterMap::open(&frontend_dir, &node_ids, &[0], ec_shape).unwrap();
    map.install_unix_shard_clients([LocalUnixShardNodeClientConfig::new(
        NodeId::new(1),
        socket_path,
    )])
    .unwrap();

    let data_pg_id = DataPgId::new_for_test(PgId::new(0));
    let key = ShardKey::new(&[0x64; 16], 102, 0);
    let location = ShardLocation::new(
        ClusterEpoch::INITIAL,
        data_pg_id,
        key.shard_index(),
        NodeId::new(1),
    );
    let payload = b"frontend writes to storage-node-owned shard dir";
    let ack = map
        .write_payload_shard(ClusterEpoch::INITIAL, location, &key, payload)
        .unwrap();
    server_thread.join().unwrap();

    assert!(matches!(
        map.node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .read_shard_file(0, &key),
        Err(StoreError::NotFound)
    ));
    let remote = SharedStorageNode::open_with_default_ec_shape(
        &server_config.data_dir,
        &server_config.pg_ids,
        server_config.default_ec_shape,
    )
    .unwrap();
    assert_eq!(remote.read_shard_file(0, &key).unwrap(), payload);
    assert_eq!(ack.stored_size, payload.len() as u64);
}

#[test]
fn frontend_unix_metadata_command_mode_uses_storage_node_owned_data_dir() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let remote_data_dir = tmp.path().join("remote-metadata-node-1-owned");
    let socket_path = tmp.path().join("sockets").join("metadata-node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let server_config = StorageNodeProcessConfig {
        node_id,
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: remote_data_dir.clone(),
        default_ec_shape: ec_shape,
        pg_ids: vec![0],
        socket_path: socket_path.clone(),
        pg_routes: vec![StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Active,
            primary_node_id: node_id,
            acting_set: vec![node_id],
        }],

        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    };
    let server = Arc::new(StorageNodeServer::bind(server_config.clone()).unwrap());
    assert!(remote_data_dir.join(".argmin-storage-node.lock").is_file());
    let server_thread = {
        let server = Arc::clone(&server);
        thread::spawn(move || server.accept_one().unwrap())
    };

    let frontend_data_dir = tmp.path().join("frontend-only-metadata-routing");
    let mut map = LocalClusterMap::open_with_configs(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            frontend_data_dir.join("node-0001"),
        )],
        &[0],
        ec_shape,
    )
    .unwrap();
    map.install_unix_metadata_command_clients([LocalUnixMetadataCommandNodeClientConfig::new(
        node_id,
        socket_path,
    )])
    .unwrap();
    let map = Arc::new(map);
    let cluster = StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let bucket = crate::tests::bucket_name("remote-metadata-command-bucket");
    let command = create_bucket_metadata_command(PgId::new(0), 1, bucket.clone());

    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(node_id, &command)
        .unwrap();
    server_thread.join().unwrap();

    let frontend_pg = map.node(node_id).unwrap().storage_node().get_pg(0).unwrap();
    assert!(crate::PgMetadataStore::head_bucket_raw(&*frontend_pg, &bucket).is_err());
    let remote = SharedStorageNode::open_with_default_ec_shape(
        &server_config.data_dir,
        &server_config.pg_ids,
        server_config.default_ec_shape,
    )
    .unwrap();
    let remote_pg = remote.get_pg(0).unwrap();
    crate::PgMetadataStore::head_bucket_raw(&*remote_pg, &bucket).unwrap();
}

#[test]
fn peering_replay_catches_up_replicas_through_unix_storage_clients() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let frontend_dir = tmp.path().join("frontend-peering-replay");
    let mut map = LocalClusterMap::open(&frontend_dir, &node_ids, &[1], ec_shape).unwrap();
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "unix-peering-replay-first-");
    let second_bucket = bucket_for_pg(topology, 1, "unix-peering-replay-second-");
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);

    let mut server_configs = Vec::new();
    let mut client_configs = Vec::new();
    for node_id in node_ids {
        let socket_path = tmp
            .path()
            .join("sockets")
            .join(format!("peering-node-{}.sock", node_id.as_u32()));
        private_socket_dir(socket_path.parent().unwrap());
        let data_dir = tmp
            .path()
            .join(format!("remote-peering-node-{}", node_id.as_u32()));
        let remote =
            SharedStorageNode::open_with_default_ec_shape(&data_dir, &[1], ec_shape).unwrap();
        let pg = remote.get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
        if node_id == NodeId::new(0) {
            pg.apply_metadata_command_and_record(node_id.as_u32(), &second)
                .unwrap();
        }
        drop(pg);
        drop(remote);

        server_configs.push(StorageNodeProcessConfig {
            node_id,
            cluster_epoch: ClusterEpoch::INITIAL,
            route_map_validity: RouteMapValidity::Forever,
            data_dir,
            default_ec_shape: ec_shape,
            pg_ids: vec![1],
            socket_path: socket_path.clone(),
            pg_routes: vec![StorageNodePgRoute {
                pg_id: 1,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: PgState::Peering,
                primary_node_id: NodeId::new(0),
                acting_set: node_ids.to_vec(),
            }],

            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: Vec::new(),
        });
        client_configs.push(LocalUnixStorageNodeClientConfig::new(node_id, socket_path));
    }

    let mut _server_guards = Vec::new();
    for config in server_configs {
        let server = StorageNodeServer::bind(config).unwrap();
        _server_guards.push(spawn_storage_node_server(server));
    }
    map.install_unix_storage_node_clients(client_configs)
        .unwrap();
    let map = Arc::new(map);
    let cluster = StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let decision = cluster
        .replay_pg_peering_catchup_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap();

    let primary_state = map
        .node(NodeId::new(0))
        .unwrap()
        .metadata_command_client()
        .metadata_command_replica_state(pg_id)
        .unwrap();
    let proof = crate::control_plane::PgMetadataProof {
        applied_log_index: primary_state.applied_log_index,
        applied_log_hash: primary_state.applied_log_hash,
        state_digest: primary_state.state_digest,
    };
    assert_eq!(
        decision,
        crate::peering::PgPeeringReconstructionDecision::AlreadyConverged { proof }
    );
    for node_id in node_ids {
        let state = map
            .node(node_id)
            .unwrap()
            .metadata_command_client()
            .metadata_command_replica_state(pg_id)
            .unwrap();
        assert_eq!(state, primary_state);
    }
}

#[test]
fn frontend_unix_bucket_metadata_mode_creates_bucket_on_storage_node() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let remote_data_dir = tmp.path().join("remote-bucket-metadata-node-1-owned");
    let socket_path = tmp
        .path()
        .join("sockets")
        .join("bucket-metadata-node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let server_config = StorageNodeProcessConfig {
        node_id,
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: remote_data_dir.clone(),
        default_ec_shape: ec_shape,
        pg_ids: vec![0],
        socket_path: socket_path.clone(),
        pg_routes: vec![StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Active,
            primary_node_id: node_id,
            acting_set: vec![node_id],
        }],

        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    };
    let server = StorageNodeServer::bind(server_config.clone()).unwrap();
    assert!(remote_data_dir.join(".argmin-storage-node.lock").is_file());
    let _server_guard = spawn_storage_node_server(server);

    let frontend_data_dir = tmp.path().join("frontend-only-bucket-metadata-routing");
    let mut map = LocalClusterMap::open_with_configs(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            frontend_data_dir.join("node-0001"),
        )],
        &[0],
        ec_shape,
    )
    .unwrap();
    map.install_unix_metadata_command_clients([LocalUnixMetadataCommandNodeClientConfig::new(
        node_id,
        socket_path.clone(),
    )])
    .unwrap();
    map.install_unix_bucket_metadata_clients([LocalUnixBucketMetadataNodeClientConfig::new(
        node_id,
        socket_path,
    )])
    .unwrap();
    let map = Arc::new(map);
    let cluster = StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let bucket = crate::tests::bucket_name("remote-bucket-metadata-create");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let config = crate::CreateBucketConfig {
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
    };

    let outcome = cluster
        .create_bucket_with_config_and_load_info(&config)
        .unwrap();
    assert!(matches!(
        outcome,
        crate::BucketCreateAttemptOutcome::Created(_)
    ));
    let destination_bucket = crate::tests::bucket_name("remote-bucket-metadata-destination");
    let destination_config = crate::CreateBucketConfig {
        name: destination_bucket.as_str(),
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
    };
    assert!(matches!(
        cluster
            .create_bucket_with_config_and_load_info(&destination_config)
            .unwrap(),
        crate::BucketCreateAttemptOutcome::Created(_)
    ));

    let runtime_map = crate::StorageClusterRuntimeMapHandle::new(Arc::clone(&cluster));
    let admission = runtime_map.admit_current_route().unwrap();
    let active_bucket_route = admission.active_bucket_route(&bucket).unwrap();
    assert_eq!(active_bucket_route.head_bucket_info().unwrap().name, bucket);
    assert_eq!(
        active_bucket_route
            .get_bucket_subresource(crate::BucketSubresourceKind::Cors)
            .unwrap(),
        None
    );
    assert_eq!(
        active_bucket_route
            .load_bucket_snapshot(crate::BucketSnapshotRequest::default())
            .unwrap()
            .bucket
            .name,
        bucket
    );
    let pair = admission
        .active_bucket_route_pair(&bucket, &destination_bucket)
        .unwrap()
        .load_bucket_snapshot_pair(
            crate::BucketSnapshotRequest::default(),
            crate::BucketSnapshotRequest::default(),
        )
        .unwrap();
    match pair {
        crate::BucketSnapshotPair::Distinct {
            source,
            destination,
        } => {
            assert_eq!(source.bucket.name, bucket);
            assert_eq!(destination.bucket.name, destination_bucket);
        }
        crate::BucketSnapshotPair::Same { .. } => {
            panic!("distinct bucket routes returned a same-bucket snapshot")
        }
    }
    assert!(matches!(
        admission
            .active_bucket_route_pair(&bucket, &bucket)
            .unwrap()
            .load_bucket_snapshot_pair(
                crate::BucketSnapshotRequest::default(),
                crate::BucketSnapshotRequest::default(),
            )
            .unwrap(),
        crate::BucketSnapshotPair::Same { .. }
    ));

    let frontend_pg = map.node(node_id).unwrap().storage_node().get_pg(0).unwrap();
    assert!(crate::PgMetadataStore::head_bucket_raw(&*frontend_pg, &bucket).is_err());
    let remote = SharedStorageNode::open_with_default_ec_shape(
        &server_config.data_dir,
        &server_config.pg_ids,
        server_config.default_ec_shape,
    )
    .unwrap();
    let remote_pg = remote.get_pg(0).unwrap();
    let remote_info = crate::PgMetadataStore::head_bucket_raw(&*remote_pg, &bucket).unwrap();
    assert_eq!(remote_info.name, bucket);
}

#[test]
fn frontend_unix_reclaim_and_bucket_finalize_resume_from_storage_node_owned_rows() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let remote_data_dir = tmp.path().join("remote-reclaim-finalize-node-1");
    let socket_path = tmp
        .path()
        .join("sockets")
        .join("reclaim-finalize-node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let server_config = StorageNodeProcessConfig {
        node_id,
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: remote_data_dir.clone(),
        default_ec_shape: ec_shape,
        pg_ids: vec![0],
        socket_path: socket_path.clone(),
        pg_routes: vec![StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Active,
            primary_node_id: node_id,
            acting_set: vec![node_id],
        }],

        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    };
    let server = StorageNodeServer::bind(server_config).unwrap();
    assert!(remote_data_dir.join(".argmin-storage-node.lock").is_file());
    let _server_guard = spawn_storage_node_server(server);

    let open_frontend = |name: &str| {
        let mut map = LocalClusterMap::open_frontend_placeholder_with_configs_and_epoch(
            node_id,
            [LocalNodeStoreConfig::new(
                node_id,
                tmp.path().join(name).join("node-0001"),
            )],
            &[0],
            ec_shape,
            ClusterEpoch::INITIAL,
        )
        .unwrap();
        map.install_unix_storage_node_clients([LocalUnixStorageNodeClientConfig::new(
            node_id,
            socket_path.clone(),
        )])
        .unwrap();
        let map = Arc::new(map);
        let cluster = StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        (map, cluster)
    };

    let (first_map, first_cluster) = open_frontend("frontend-reclaim-first");
    let bucket = crate::tests::bucket_name("remote-reclaim-finalize");
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let committed =
        write_committed_direct_segment_for(&first_cluster, &bucket, &key, b"remote reclaim");
    first_cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
    assert!(first_cluster
        .payload_reclaim_exists(&bucket, &key, committed.generation_id)
        .unwrap());
    first_cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
    drop(first_cluster);
    drop(first_map);

    let (reopened_map, reopened_cluster) = open_frontend("frontend-reclaim-reopened");
    let reclaim_scan =
        reopened_cluster.enqueue_durable_object_payload_reclaim_roots_excluding(&HashSet::new());
    assert_eq!(reclaim_scan.errors, 0);
    assert_eq!(reclaim_scan.queued, 1);
    assert!(matches!(
        reopened_cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::ObjectPayload((
            queued_bucket,
            queued_key,
            queued_generation_id,
        ))) if queued_bucket == bucket
            && queued_key == key
            && queued_generation_id == committed.generation_id
    ));
    assert!(reopened_cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
        .unwrap());
    assert!(!reopened_cluster
        .payload_reclaim_exists(&bucket, &key, committed.generation_id)
        .unwrap());
    let shard = committed
        .written
        .written_shards
        .first()
        .expect("single-shard test payload should write one shard");
    let location = committed.locations[usize::from(shard.key.shard_index().get())];
    let read_after_reclaim =
        reopened_map.read_payload_shard(ClusterEpoch::INITIAL, location, &shard.key, shard.ack);
    assert!(
        matches!(read_after_reclaim, Err(ShardIoError::Store { .. })),
        "remote shard read should fail after reclaim deletes the storage-node-owned file"
    );

    let finalize_scan = reopened_cluster.enqueue_durable_bucket_delete_finalize_roots();
    assert_eq!(finalize_scan.errors, 0);
    assert_eq!(finalize_scan.queued, 1);
    assert!(matches!(
        reopened_cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDelete(queued_root))
            if queued_root.bucket == bucket
    ));
    assert_eq!(
        reopened_cluster
            .try_finalize_bucket_delete(&bucket)
            .unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
    assert!(matches!(
        reopened_map
            .node(node_id)
            .unwrap()
            .bucket_metadata_client()
            .head_bucket_raw(crate::BucketPgId::new_for_test(PgId::new(0)), &bucket),
        Err(crate::BucketSnapshotLoadError::Metadata(
            crate::MetadataError::BucketNotFound { .. }
        ))
    ));
    assert!(crate::PgMetadataStore::head_bucket_raw(
        &*reopened_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(0)
            .unwrap(),
        &bucket,
    )
    .is_err());
}

#[test]
fn frontend_unix_durable_reclaim_scan_stops_after_first_stale_route() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 1, m: 0 };
    let frontend_epoch = ClusterEpoch::INITIAL;
    let storage_epoch = ClusterEpoch::new(2).unwrap();
    let socket_path = tmp.path().join("sockets").join("stale-reclaim.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let storage_routes = pg_ids
        .iter()
        .copied()
        .map(|pg_id| StorageNodePgRoute {
            pg_id,
            cluster_epoch: storage_epoch,
            state: PgState::Active,
            primary_node_id: node_id,
            acting_set: vec![node_id],
        })
        .collect();
    let server = StorageNodeServer::bind(StorageNodeProcessConfig {
        node_id,
        cluster_epoch: storage_epoch,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: tmp.path().join("stale-reclaim-node"),
        default_ec_shape: ec_shape,
        pg_ids: pg_ids.to_vec(),
        socket_path: socket_path.clone(),
        pg_routes: storage_routes,
        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    })
    .unwrap();
    let _server_guard = spawn_storage_node_server(server);

    let mut map = LocalClusterMap::open_frontend_topology_only_with_epoch(
        node_id,
        [node_id],
        &pg_ids,
        ec_shape,
        frontend_epoch,
    )
    .unwrap();
    map.install_unix_storage_node_clients([LocalUnixStorageNodeClientConfig::new(
        node_id,
        socket_path,
    )])
    .unwrap();
    let cluster = StorageCluster::from_local_map(Arc::new(map)).unwrap();

    let scan = cluster.enqueue_durable_object_payload_reclaim_roots_excluding(&HashSet::new());
    assert_eq!(scan.errors, 1, "one stale response must end the PG scan");
    assert!(scan.route_refresh_required);
    let batch = cluster.enqueue_durable_reclaim_work_batch_excluding(
        None,
        1,
        &HashSet::new(),
        &HashSet::new(),
        &HashSet::new(),
    );
    assert_eq!(
        batch.outcome,
        crate::DurableReclaimScanOutcome::RouteRefreshRequired,
        "the batch scan must skip subsequent bucket scans after the stale response"
    );
    assert_eq!(batch.next_pg_id, Some(pg_ids[0]));
    assert_eq!(batch.scanned_pgs, 0);
}

#[test]
fn frontend_unix_delete_bucket_reaps_expired_reservation_from_older_epoch() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let acquire_epoch = ClusterEpoch::INITIAL;
    let route_epoch = ClusterEpoch::new(acquire_epoch.get() + 1).unwrap();
    let remote_data_dir = tmp.path().join("remote-delete-expired-reservation-node-1");
    let socket_path = tmp
        .path()
        .join("sockets")
        .join("delete-expired-reservation-node-1.sock");
    let bucket = crate::tests::bucket_name("remote-delete-expired-reservation");
    let owner = crate::CanonicalUserId::from_principal("owner");
    {
        let node = SharedStorageNode::open_with_default_ec_shape(&remote_data_dir, &[0], ec_shape)
            .unwrap();
        let pg = node.get_pg(0).unwrap();
        crate::PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        crate::PgMetadataStore::acquire_durable_bucket_write_reservation(
            &*pg,
            crate::traits::DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id: "expired-reservation-before-route-change",
                owner_token: "expired-owner-before-route-change",
                cluster_epoch: acquire_epoch,
                operation_kind: "put-object",
                created_at: 1,
                lease_deadline: 2,
                target_context: Some("key=expired"),
            },
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }

    private_socket_dir(socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(StorageNodeProcessConfig {
        node_id,
        cluster_epoch: route_epoch,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: remote_data_dir,
        default_ec_shape: ec_shape,
        pg_ids: vec![0],
        socket_path: socket_path.clone(),
        pg_routes: vec![StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: route_epoch,
            state: PgState::Active,
            primary_node_id: node_id,
            acting_set: vec![node_id],
        }],
        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    })
    .unwrap();
    let _server_guard = spawn_storage_node_server(server);

    let mut map = LocalClusterMap::open_frontend_placeholder_with_configs_and_epoch(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            tmp.path()
                .join("frontend-delete-expired-reservation-node-1"),
        )],
        &[0],
        ec_shape,
        route_epoch,
    )
    .unwrap();
    map.install_unix_storage_node_clients([LocalUnixStorageNodeClientConfig::new(
        node_id,
        socket_path,
    )])
    .unwrap();
    let map = Arc::new(map);
    let cluster = StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .expect("DeleteBucket should reap the old-epoch expired reservation over Unix RPC");
    assert!(map
        .node(node_id)
        .unwrap()
        .bucket_write_reservation_client()
        .durable_bucket_write_reservations(crate::BucketPgId::new_for_test(PgId::new(0)), &bucket,)
        .unwrap()
        .is_empty());
    assert_eq!(
        map.node(node_id)
            .unwrap()
            .bucket_metadata_client()
            .head_bucket_raw(crate::BucketPgId::new_for_test(PgId::new(0)), &bucket)
            .unwrap()
            .state,
        crate::BucketState::Deleting
    );
}

#[test]
fn frontend_unix_delete_bucket_adopts_live_drain_from_older_epoch() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let drain_epoch = ClusterEpoch::INITIAL;
    let route_epoch = ClusterEpoch::new(drain_epoch.get() + 1).unwrap();
    let remote_data_dir = tmp.path().join("remote-delete-live-old-drain-node-1");
    let socket_path = tmp
        .path()
        .join("sockets")
        .join("delete-live-old-drain-node-1.sock");
    let bucket = crate::tests::bucket_name("remote-delete-live-old-drain");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let original_deadline = crate::clock::current_time_millis().saturating_add(30_000);
    {
        let node = SharedStorageNode::open_with_default_ec_shape(&remote_data_dir, &[0], ec_shape)
            .unwrap();
        let pg = node.get_pg(0).unwrap();
        crate::PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        crate::PgMetadataStore::begin_durable_bucket_write_drain(
            &*pg,
            &bucket,
            "live-drain-before-route-change",
            "delete-owner-before-route-change",
            drain_epoch,
            crate::clock::current_time_millis(),
            original_deadline,
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }

    private_socket_dir(socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(StorageNodeProcessConfig {
        node_id,
        cluster_epoch: route_epoch,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: remote_data_dir,
        default_ec_shape: ec_shape,
        pg_ids: vec![0],
        socket_path: socket_path.clone(),
        pg_routes: vec![StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: route_epoch,
            state: PgState::Active,
            primary_node_id: node_id,
            acting_set: vec![node_id],
        }],
        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    })
    .unwrap();
    let _server_guard = spawn_storage_node_server(server);

    let mut map = LocalClusterMap::open_frontend_placeholder_with_configs_and_epoch(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            tmp.path().join("frontend-delete-live-old-drain-node-1"),
        )],
        &[0],
        ec_shape,
        route_epoch,
    )
    .unwrap();
    map.install_unix_storage_node_clients([LocalUnixStorageNodeClientConfig::new(
        node_id,
        socket_path,
    )])
    .unwrap();
    let map = Arc::new(map);
    let cluster = StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .expect("DeleteBucket should adopt an old-epoch live drain over the current Unix route");
    let drain = map
        .node(node_id)
        .unwrap()
        .bucket_write_reservation_client()
        .durable_bucket_write_drain(crate::BucketPgId::new_for_test(PgId::new(0)), &bucket)
        .unwrap()
        .expect("successful delete begin should retain its terminal drain");
    assert_eq!(drain.cluster_epoch, drain_epoch);
    assert_eq!(drain.drain_id, "live-drain-before-route-change");
    assert!(drain.lease_deadline > crate::clock::current_time_millis());
    assert_eq!(
        map.node(node_id)
            .unwrap()
            .bucket_metadata_client()
            .head_bucket_raw(crate::BucketPgId::new_for_test(PgId::new(0)), &bucket)
            .unwrap()
            .state,
        crate::BucketState::Deleting
    );
}

#[test]
fn frontend_unix_lifecycle_claims_resume_from_storage_node_owned_rows() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let remote_data_dir = tmp.path().join("remote-lifecycle-node-1");
    let socket_path = tmp.path().join("sockets").join("lifecycle-node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let server_config = StorageNodeProcessConfig {
        node_id,
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: remote_data_dir.clone(),
        default_ec_shape: ec_shape,
        pg_ids: vec![0],
        socket_path: socket_path.clone(),
        pg_routes: vec![StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Active,
            primary_node_id: node_id,
            acting_set: vec![node_id],
        }],

        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    };
    let server = StorageNodeServer::bind(server_config).unwrap();
    assert!(remote_data_dir.join(".argmin-storage-node.lock").is_file());
    let _server_guard = spawn_storage_node_server(server);

    let open_frontend = |name: &str| {
        let mut map = LocalClusterMap::open_frontend_placeholder_with_configs_and_epoch(
            node_id,
            [LocalNodeStoreConfig::new(
                node_id,
                tmp.path().join(name).join("node-0001"),
            )],
            &[0],
            ec_shape,
            ClusterEpoch::INITIAL,
        )
        .unwrap();
        map.install_unix_storage_node_clients([LocalUnixStorageNodeClientConfig::new(
            node_id,
            socket_path.clone(),
        )])
        .unwrap();
        let map = Arc::new(map);
        let cluster = StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        (map, cluster)
    };

    let (first_map, first_cluster) = open_frontend("frontend-lifecycle-first");
    let bucket = crate::tests::bucket_name("remote-lifecycle-claims");
    create_test_bucket(&first_cluster, &bucket);
    put_test_lifecycle(&first_cluster, &bucket);
    let bucket_incarnation_generation = first_cluster
        .head_bucket_info(&bucket)
        .unwrap()
        .bucket_incarnation_generation;
    assert!(crate::PgMetadataStore::head_bucket_raw(
        &*first_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(0)
            .unwrap(),
        &bucket,
    )
    .is_err());
    drop(first_cluster);
    drop(first_map);

    let (reopened_map, reopened_cluster) = open_frontend("frontend-lifecycle-reopened");
    let roots = reopened_cluster.list_lifecycle_sweep_roots(10).unwrap();
    assert!(roots.iter().any(|root| {
        root.bucket == bucket
            && root.bucket_incarnation_generation == bucket_incarnation_generation
            && root.source == crate::LifecycleSweepRootSource::LifecycleConfig
    }));

    let claim = reopened_cluster
        .acquire_lifecycle_sweep_claim(&bucket, bucket_incarnation_generation, 20)
        .unwrap()
        .expect("remote lifecycle claim should acquire");
    assert_eq!(claim.bucket, bucket);
    assert_eq!(
        claim.bucket_incarnation_generation,
        bucket_incarnation_generation
    );
    assert_eq!(claim.pg_id, 0);
    assert_eq!(claim.attempt_count, 1);

    let heartbeat = reopened_cluster
        .heartbeat_lifecycle_sweep_claim(&claim, 30)
        .unwrap();
    assert_eq!(heartbeat.heartbeat_at, 30);
    assert_eq!(heartbeat.lease_deadline, Some(60_030));
    reopened_cluster
        .release_lifecycle_sweep_claim(&heartbeat)
        .unwrap();

    let stale_claim = reopened_cluster
        .acquire_lifecycle_sweep_claim(&bucket, bucket_incarnation_generation, 100)
        .unwrap()
        .expect("remote lifecycle claim should reacquire after release");
    assert_eq!(stale_claim.attempt_count, 1);
    drop(reopened_cluster);
    drop(reopened_map);

    let (_final_map, final_cluster) = open_frontend("frontend-lifecycle-final");
    let expired_roots = final_cluster.list_lifecycle_sweep_roots(60_101).unwrap();
    assert!(
        expired_roots.iter().any(|root| {
            root.bucket == bucket
                && root.bucket_incarnation_generation == bucket_incarnation_generation
                && root.source == crate::LifecycleSweepRootSource::ExpiredClaim
        }),
        "expired durable lifecycle claim should be rediscovered through the Unix client"
    );
    let recovered = final_cluster
        .acquire_lifecycle_sweep_claim(&bucket, bucket_incarnation_generation, 60_101)
        .unwrap()
        .expect("expired remote lifecycle claim should be recoverable");
    assert_eq!(recovered.bucket, bucket);
    assert_eq!(recovered.attempt_count, 2);
    final_cluster
        .release_lifecycle_sweep_claim(&recovered)
        .unwrap();
}

#[test]
fn frontend_unix_stream_session_scavenger_lists_storage_node_owned_rows() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let remote_data_dir = tmp.path().join("remote-stream-scavenge-node-1");
    let socket_path = tmp
        .path()
        .join("sockets")
        .join("stream-scavenge-node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let server_config = StorageNodeProcessConfig {
        node_id,
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: remote_data_dir.clone(),
        default_ec_shape: ec_shape,
        pg_ids: vec![0],
        socket_path: socket_path.clone(),
        pg_routes: vec![StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Active,
            primary_node_id: node_id,
            acting_set: vec![node_id],
        }],

        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    };
    let bucket = crate::tests::bucket_name("remote-stream-scavenge");
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let session_id = crate::SessionId::try_from("ab".repeat(16)).unwrap();
    {
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &server_config.data_dir,
            &server_config.pg_ids,
            server_config.default_ec_shape,
        )
        .unwrap();
        let pg = remote.get_pg(0).unwrap();
        crate::PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &crate::CanonicalUserId::from_principal("owner"),
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        crate::PgMetadataStore::create_stream_upload(
            &*pg,
            &crate::CreateStreamUploadReq {
                session_id: session_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: crate::StreamUploadTarget::PutObject,
                encryption: crate::ObjectEncryption::None,
            },
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }
    let server = StorageNodeServer::bind(server_config).unwrap();
    assert!(remote_data_dir.join(".argmin-storage-node.lock").is_file());
    let _server_guard = spawn_storage_node_server(server);

    let mut map = LocalClusterMap::open_frontend_placeholder_with_configs_and_epoch(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            tmp.path()
                .join("frontend-stream-scavenge")
                .join("node-0001"),
        )],
        &[0],
        ec_shape,
        ClusterEpoch::INITIAL,
    )
    .unwrap();
    map.install_unix_storage_node_clients([LocalUnixStorageNodeClientConfig::new(
        node_id,
        socket_path,
    )])
    .unwrap();
    let map = Arc::new(map);
    let cluster = StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let frontend_pg = map.node(node_id).unwrap().storage_node().get_pg(0).unwrap();
    assert!(
        crate::PgMetadataStore::list_all_stream_uploads(&*frontend_pg)
            .unwrap()
            .is_empty(),
        "frontend placeholder PG must not be the stream-session scan authority"
    );

    let sessions = cluster.list_stream_upload_sessions_best_effort();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].session_id, session_id);
    assert_eq!(sessions[0].bucket, bucket);
    assert_eq!(sessions[0].key, key);

    cluster
        .abort_stream_upload_session(&bucket, &key, &session_id)
        .unwrap();
    assert!(cluster.list_stream_upload_sessions_best_effort().is_empty());
    assert!(
        crate::PgMetadataStore::list_all_stream_uploads(&*frontend_pg)
            .unwrap()
            .is_empty(),
        "remote abort must not create placeholder stream-session state"
    );
}

#[test]
fn frontend_unix_cluster_map_history_reference_summary_reads_storage_node_owned_rows() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let remote_data_dir = tmp.path().join("remote-history-summary-node-1");
    let socket_path = tmp
        .path()
        .join("sockets")
        .join("history-summary-node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let server_config = StorageNodeProcessConfig {
        node_id,
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: remote_data_dir.clone(),
        default_ec_shape: ec_shape,
        pg_ids: vec![0],
        socket_path: socket_path.clone(),
        pg_routes: vec![StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Active,
            primary_node_id: node_id,
            acting_set: vec![node_id],
        }],

        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    };
    {
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &server_config.data_dir,
            &server_config.pg_ids,
            server_config.default_ec_shape,
        )
        .unwrap();
        let pg = remote.get_pg(0).unwrap();
        pg.connection()
            .execute(
                "INSERT INTO object_segments \
                 (bucket, key, version_id, segment_index, size, segment_crc64, segment_okh, \
                  segment_vid, data_pg_id, placement_cluster_epoch, ec_k, ec_m) \
                 VALUES (?1, ?2, 1, 0, 1024, ?3, ?4, 10, 0, ?5, 1, 0)",
                rusqlite::params![
                    "remote-history-summary-bucket",
                    "object",
                    0x1234_i64,
                    [0x11_u8; 16].as_slice(),
                    8_i64,
                ],
            )
            .unwrap();
        let backfill = crate::PlacedSegmentShardBackfillWorkItem {
            request: crate::SegmentStoredBytesRequest {
                data_pg_id: 0,
                segment_okh: [0x44; 16],
                segment_vid: GenerationId::new(12).unwrap(),
                stored_size: 4096,
                segment_crc64: 0x9abc,
                ec: ec_shape,
            },
            source_cluster_epoch: ClusterEpoch::new(4).unwrap(),
            desired_cluster_epoch: ClusterEpoch::new(11).unwrap(),
        };
        pg.record_placed_segment_shard_backfill(&backfill, backfill.request.ec.m, None)
            .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }
    let server = StorageNodeServer::bind(server_config).unwrap();
    assert!(remote_data_dir.join(".argmin-storage-node.lock").is_file());
    let server_thread = thread::spawn(move || server.accept_one().unwrap());

    let mut map = LocalClusterMap::open_frontend_placeholder_with_configs_and_epoch(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            tmp.path()
                .join("frontend-history-summary")
                .join("node-0001"),
        )],
        &[0],
        ec_shape,
        ClusterEpoch::INITIAL,
    )
    .unwrap();
    map.install_unix_storage_node_clients([LocalUnixStorageNodeClientConfig::new(
        node_id,
        socket_path,
    )])
    .unwrap();

    let frontend_summary = map
        .node(node_id)
        .unwrap()
        .storage_node()
        .cluster_map_history_reference_summary()
        .unwrap();
    assert_eq!(frontend_summary.oldest_required_epoch(), None);

    let summary = map.cluster_map_history_reference_summary().unwrap();
    assert_eq!(
        summary.oldest_live_placement_epoch,
        Some(ClusterEpoch::new(8).unwrap())
    );
    assert_eq!(
        summary.oldest_durable_backfill_epoch,
        Some(ClusterEpoch::new(4).unwrap())
    );
    assert_eq!(
        summary.oldest_required_epoch(),
        Some(ClusterEpoch::new(4).unwrap())
    );
    server_thread.join().unwrap();
}

#[test]
fn frontend_unix_stream_session_scavenger_rejects_wrong_pg_rows() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let remote_data_dir = tmp.path().join("remote-stream-wrong-pg-node-1");
    let socket_path = tmp
        .path()
        .join("sockets")
        .join("stream-wrong-pg-node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let server_config = StorageNodeProcessConfig {
        node_id,
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: remote_data_dir.clone(),
        default_ec_shape: ec_shape,
        pg_ids: vec![0, 1],
        socket_path: socket_path.clone(),
        pg_routes: vec![
            StorageNodePgRoute {
                pg_id: 0,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: PgState::Active,
                primary_node_id: node_id,
                acting_set: vec![node_id],
            },
            StorageNodePgRoute {
                pg_id: 1,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: PgState::Active,
                primary_node_id: node_id,
                acting_set: vec![node_id],
            },
        ],

        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    };
    let topology = crate::PgTopology::new(&server_config.pg_ids).unwrap();
    let bucket = crate::tests::bucket_name("remote-stream-wrong-pg");
    let key = key_for_object_pg(&topology, &bucket, 1, "key-");
    let session_id = crate::SessionId::try_from("cd".repeat(16)).unwrap();
    {
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &server_config.data_dir,
            &server_config.pg_ids,
            server_config.default_ec_shape,
        )
        .unwrap();
        let wrong_pg = remote.get_pg(0).unwrap();
        crate::PgMetadataStore::create_stream_upload(
            &*wrong_pg,
            &crate::CreateStreamUploadReq {
                session_id: session_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: crate::StreamUploadTarget::PutObject,
                encryption: crate::ObjectEncryption::None,
            },
        )
        .unwrap();
        wrong_pg.refresh_metadata_command_state_digest().unwrap();
    }
    let server = StorageNodeServer::bind(server_config).unwrap();
    let _server_guard = spawn_storage_node_server(server);

    let mut map = LocalClusterMap::open_frontend_placeholder_with_configs_and_epoch(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            tmp.path()
                .join("frontend-stream-wrong-pg")
                .join("node-0001"),
        )],
        &[0, 1],
        ec_shape,
        ClusterEpoch::INITIAL,
    )
    .unwrap();
    map.install_unix_storage_node_clients([LocalUnixStorageNodeClientConfig::new(
        node_id,
        socket_path,
    )])
    .unwrap();
    let cluster = StorageCluster::from_local_map(Arc::new(map)).unwrap();

    assert_eq!(cluster.object_metadata_pg_id(&bucket, &key), 1);
    assert!(
        cluster.list_stream_upload_sessions_best_effort().is_empty(),
        "PG-wide stream-session scan must reject rows that belong to another object PG"
    );
}

#[test]
fn frontend_unix_bucket_metadata_mode_reads_bucket_batches_from_storage_node() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let remote_data_dir = tmp.path().join("remote-bucket-metadata-read-node-1");
    let socket_path = tmp
        .path()
        .join("sockets")
        .join("bucket-metadata-read-node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let server_config = StorageNodeProcessConfig {
        node_id,
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: remote_data_dir.clone(),
        default_ec_shape: ec_shape,
        pg_ids: vec![0],
        socket_path: socket_path.clone(),
        pg_routes: vec![StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Active,
            primary_node_id: node_id,
            acting_set: vec![node_id],
        }],

        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    };
    let bucket = crate::tests::bucket_name("remote-bucket-metadata-read");
    let filtered_bucket = crate::tests::bucket_name("remote-bucket-metadata-other-owner");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let other_owner = crate::CanonicalUserId::from_principal("other-owner");
    let (expected_generation, expected_identity) = {
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &server_config.data_dir,
            &server_config.pg_ids,
            server_config.default_ec_shape,
        )
        .unwrap();
        let remote_pg = remote.get_pg(0).unwrap();
        crate::PgMetadataStore::create_bucket(
            &*remote_pg,
            &bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        crate::PgMetadataStore::create_bucket(
            &*remote_pg,
            &filtered_bucket,
            "other-owner",
            &other_owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        let generation = remote_pg
            .load_bucket_execution_generations(std::slice::from_ref(&bucket))
            .unwrap()
            .remove(&bucket)
            .unwrap();
        let identity = remote_pg
            .load_bucket_fast_path_identities(std::slice::from_ref(&bucket))
            .unwrap()
            .remove(&bucket)
            .unwrap();
        remote_pg.refresh_metadata_command_state_digest().unwrap();
        (generation, identity)
    };
    let server = StorageNodeServer::bind(server_config).unwrap();
    let _server_guard = spawn_storage_node_server(server);

    let frontend_data_dir = tmp.path().join("frontend-bucket-metadata-read-routing");
    let mut map = LocalClusterMap::open_with_configs(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            frontend_data_dir.join("node-0001"),
        )],
        &[0],
        ec_shape,
    )
    .unwrap();
    map.install_unix_bucket_metadata_clients([LocalUnixBucketMetadataNodeClientConfig::new(
        node_id,
        socket_path,
    )])
    .unwrap();
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let frontend_pg = map.node(node_id).unwrap().storage_node().get_pg(0).unwrap();
    assert!(crate::PgMetadataStore::head_bucket_raw(&*frontend_pg, &bucket).is_err());

    let buckets = cluster.list_buckets_for_owner(owner.as_str()).unwrap();
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0].name, bucket);

    let generation_batches =
        cluster.load_available_bucket_execution_generation_batches(std::slice::from_ref(&bucket));
    assert_eq!(generation_batches.len(), 1);
    assert_eq!(
        generation_batches[0].1.get(&bucket),
        Some(&expected_generation)
    );

    let identity = cluster.load_bucket_fast_path_identity(&bucket).unwrap();
    assert_eq!(identity, Some(expected_identity));
}

#[test]
fn bucket_list_page_validation_rejects_wrong_pg_bucket() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let map = LocalClusterMap::open_with_configs(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            tmp.path().join("node-0001"),
        )],
        &[0, 1],
        ec_shape,
    )
    .unwrap();
    let correct_bucket = (0..100)
        .map(|index| crate::tests::bucket_name(format!("list-page-correct-pg-{index}")))
        .find(|bucket| map.bucket_pg_for(bucket) == 0)
        .expect("two-PG topology must place a test bucket on PG 0");
    let wrong_bucket = (0..100)
        .map(|index| crate::tests::bucket_name(format!("list-page-wrong-pg-{index}")))
        .find(|bucket| map.bucket_pg_for(bucket) == 1)
        .expect("two-PG topology must place a test bucket on PG 1");
    let cluster = crate::StorageCluster::from_local_map(Arc::new(map)).unwrap();
    let owner = crate::CanonicalUserId::from_principal("owner");
    let correct_info = test_bucket_list_info(correct_bucket, &owner);
    let wrong_info = test_bucket_list_info(wrong_bucket, &owner);

    cluster
        .validate_bucket_list_page_for_pg(PgId::new(0), node_id, &[correct_info])
        .unwrap();

    let error = cluster
        .validate_bucket_list_page_for_pg(PgId::new(0), node_id, &[wrong_info])
        .unwrap_err();
    assert!(matches!(
        error,
        crate::ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate bucket list response",
            ..
        })
    ));
}

fn test_bucket_list_info(
    bucket: BucketName,
    owner: &crate::CanonicalUserId,
) -> crate::types::BucketInfo {
    crate::types::BucketInfo {
        name: bucket,
        owner_principal: "owner".to_string(),
        owner_canonical_id: owner.clone(),
        created_at: 123,
        region: 0,
        state: crate::types::BucketState::Active,
        versioning: crate::BucketVersioningState::Disabled,
        object_lock: crate::BucketObjectLockConfig::default(),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        public_write: false,
        public_access_block: None,
        ownership_controls: None,
        bucket_policy_present: false,
        bucket_policy_public: false,
        bucket_policy_generation: 0,
        bucket_lifecycle_present: false,
        bucket_lifecycle_generation: 0,
        bucket_execution_generation: 1,
        bucket_incarnation_generation: 1,
        multipart_upload_id_key: crate::MultipartUploadIdKey::from_bytes([1; 32]),
        bucket_abac_enabled: false,
        encryption: crate::types::EffectiveBucketEncryptionConfig::default(),
    }
}

#[test]
fn frontend_unix_object_generation_mode_reserves_on_storage_node() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let remote_data_dir = tmp
        .path()
        .join("remote-object-generation-metadata-node-1-owned");
    let socket_path = tmp
        .path()
        .join("sockets")
        .join("object-generation-metadata-node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let server_config = StorageNodeProcessConfig {
        node_id,
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: remote_data_dir.clone(),
        default_ec_shape: ec_shape,
        pg_ids: vec![0],
        socket_path: socket_path.clone(),
        pg_routes: vec![StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Active,
            primary_node_id: node_id,
            acting_set: vec![node_id],
        }],

        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    };
    let server = StorageNodeServer::bind(server_config.clone()).unwrap();
    assert!(remote_data_dir.join(".argmin-storage-node.lock").is_file());
    let _server_guard = spawn_storage_node_server(server);

    let frontend_data_dir = tmp
        .path()
        .join("frontend-only-object-generation-metadata-routing");
    let mut map = LocalClusterMap::open_with_configs(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            frontend_data_dir.join("node-0001"),
        )],
        &[0],
        ec_shape,
    )
    .unwrap();
    map.install_unix_metadata_command_clients([LocalUnixMetadataCommandNodeClientConfig::new(
        node_id,
        socket_path.clone(),
    )])
    .unwrap();
    map.install_unix_object_generation_metadata_clients([
        LocalUnixObjectGenerationMetadataNodeClientConfig::new(node_id, socket_path),
    ])
    .unwrap();
    let map = Arc::new(map);
    let cluster = StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let bucket = crate::tests::bucket_name("remote-object-generation-reserve");
    let key = crate::tests::object_key("key");
    let reservation_id = crate::tests::stream_session_id("remote-obj-gen");

    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();

    let frontend_pg = map.node(node_id).unwrap().storage_node().get_pg(0).unwrap();
    assert!(matches!(
        crate::PgMetadataStore::get_object_generation_reservation(
            &*frontend_pg,
            &bucket,
            &key,
            &reservation_id
        ),
        Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
    ));
    let remote = SharedStorageNode::open_with_default_ec_shape(
        &server_config.data_dir,
        &server_config.pg_ids,
        server_config.default_ec_shape,
    )
    .unwrap();
    let remote_pg = remote.get_pg(0).unwrap();
    assert_eq!(
        crate::PgMetadataStore::get_object_generation_reservation(
            &*remote_pg,
            &bucket,
            &key,
            &reservation_id
        )
        .unwrap(),
        generation_id
    );
}

#[test]
fn frontend_unix_object_version_mode_reserves_on_storage_node() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let remote_data_dir = tmp
        .path()
        .join("remote-object-version-metadata-node-1-owned");
    let socket_path = tmp
        .path()
        .join("sockets")
        .join("object-version-metadata-node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let server_config = StorageNodeProcessConfig {
        node_id,
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: remote_data_dir.clone(),
        default_ec_shape: ec_shape,
        pg_ids: vec![0],
        socket_path: socket_path.clone(),
        pg_routes: vec![StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Active,
            primary_node_id: node_id,
            acting_set: vec![node_id],
        }],

        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    };
    let server = StorageNodeServer::bind(server_config.clone()).unwrap();
    assert!(remote_data_dir.join(".argmin-storage-node.lock").is_file());
    let _server_guard = spawn_storage_node_server(server);

    let frontend_data_dir = tmp
        .path()
        .join("frontend-only-object-version-metadata-routing");
    let mut map = LocalClusterMap::open_with_configs(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            frontend_data_dir.join("node-0001"),
        )],
        &[0],
        ec_shape,
    )
    .unwrap();
    map.install_unix_metadata_command_clients([LocalUnixMetadataCommandNodeClientConfig::new(
        node_id,
        socket_path.clone(),
    )])
    .unwrap();
    map.install_unix_object_version_metadata_clients([
        LocalUnixObjectVersionMetadataNodeClientConfig::new(node_id, socket_path),
    ])
    .unwrap();
    let map = Arc::new(map);
    let cluster = StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let bucket = crate::tests::bucket_name("remote-object-version-reserve");
    let key = crate::tests::object_key("key");

    let version_id = cluster
        .reserve_next_object_version(PgId::new(0), &bucket, &key)
        .unwrap();

    assert_eq!(version_id, crate::VersionId::from_u64(1));
    let frontend_pg = map.node(node_id).unwrap().storage_node().get_pg(0).unwrap();
    assert_eq!(
        crate::PgMetadataStore::next_version_id(&*frontend_pg, &bucket, &key).unwrap(),
        crate::VersionId::from_u64(1)
    );
    let remote = SharedStorageNode::open_with_default_ec_shape(
        &server_config.data_dir,
        &server_config.pg_ids,
        server_config.default_ec_shape,
    )
    .unwrap();
    let remote_pg = remote.get_pg(0).unwrap();
    assert_eq!(
        crate::PgMetadataStore::next_version_id(&*remote_pg, &bucket, &key).unwrap(),
        crate::VersionId::from_u64(2)
    );
}

#[test]
fn frontend_unix_bucket_write_reservation_mode_uses_storage_node() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let remote_data_dir = tmp
        .path()
        .join("remote-bucket-write-reservation-node-1-owned");
    let socket_path = tmp
        .path()
        .join("sockets")
        .join("bucket-write-reservation-node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let server_config = StorageNodeProcessConfig {
        node_id,
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: remote_data_dir.clone(),
        default_ec_shape: ec_shape,
        pg_ids: vec![0],
        socket_path: socket_path.clone(),
        pg_routes: vec![StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Active,
            primary_node_id: node_id,
            acting_set: vec![node_id],
        }],

        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    };
    let bucket = crate::tests::bucket_name("remote-bucket-write-reservation");
    let owner = crate::CanonicalUserId::from_principal("owner");
    {
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &server_config.data_dir,
            &server_config.pg_ids,
            server_config.default_ec_shape,
        )
        .unwrap();
        let remote_pg = remote.get_pg(0).unwrap();
        crate::PgMetadataStore::create_bucket(
            &*remote_pg,
            &bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        crate::PgMetadataStore::put_bucket_subresource(
            &*remote_pg,
            &bucket,
            crate::types::PutBucketSubresource {
                kind: crate::types::BucketSubresourceKind::Policy,
                body: "{\"Version\":\"2012-10-17\",\"Statement\":[]}",
                aux: crate::types::BucketSubresourceAux::policy(false),
            },
        )
        .unwrap();
        remote_pg.refresh_metadata_command_state_digest().unwrap();
    }
    let server = StorageNodeServer::bind(server_config.clone()).unwrap();
    assert!(remote_data_dir.join(".argmin-storage-node.lock").is_file());
    let _server_guard = spawn_storage_node_server(server);

    let frontend_data_dir = tmp
        .path()
        .join("frontend-only-bucket-write-reservation-routing");
    let mut map = LocalClusterMap::open_with_configs(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            frontend_data_dir.join("node-0001"),
        )],
        &[0],
        ec_shape,
    )
    .unwrap();
    map.install_unix_bucket_metadata_clients([LocalUnixBucketMetadataNodeClientConfig::new(
        node_id,
        socket_path.clone(),
    )])
    .unwrap();
    map.install_unix_bucket_write_reservation_clients([
        LocalUnixBucketWriteReservationNodeClientConfig::new(node_id, socket_path),
    ])
    .unwrap();
    let map = Arc::new(map);
    let cluster = StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let reservation = cluster
        .acquire_durable_bucket_write_reservation(&bucket, "bucket-write-snapshot", None)
        .unwrap();
    assert_eq!(reservation.record.bucket, bucket);
    assert_eq!(reservation.record.cluster_epoch, ClusterEpoch::INITIAL);
    let frontend_pg = map.node(node_id).unwrap().storage_node().get_pg(0).unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*frontend_pg, &bucket)
            .unwrap()
            .is_empty()
    );
    assert!(crate::PgMetadataStore::head_bucket_raw(&*frontend_pg, &bucket).is_err());
    let remote = SharedStorageNode::open_with_default_ec_shape(
        &server_config.data_dir,
        &server_config.pg_ids,
        server_config.default_ec_shape,
    )
    .unwrap();
    let remote_pg = remote.get_pg(0).unwrap();
    assert_eq!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*remote_pg, &bucket)
            .unwrap()
            .len(),
        1
    );

    cluster
        .release_durable_bucket_write_reservation(reservation)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*remote_pg, &bucket)
            .unwrap()
            .is_empty()
    );

    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        super::super::super::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        super::super::super::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("active bucket should acquire a delete drain")
        }
    };
    let mut stale_drain = drain.record.clone();
    stale_drain.owner_token = "stale-delete-owner".to_string();
    let err = map
        .node(node_id)
        .unwrap()
        .bucket_write_reservation_client()
        .heartbeat_durable_bucket_write_drain(
            crate::BucketPgId::new_for_test(PgId::new(drain.pg_id)),
            &stale_drain,
            200,
        )
        .unwrap_err();
    assert!(
            matches!(
                err,
                crate::BucketSnapshotLoadError::Metadata(
                    crate::MetadataError::BucketWriteDrainConflict { .. }
                )
            ),
            "Unix heartbeat should preserve stale drain identity as typed metadata contention, got {err:?}"
        );
    cluster.clear_durable_bucket_delete_drain(&drain).unwrap();

    let snapshot_result: Result<(), MetadataError> = cluster
        .with_bucket_write_snapshot_for_command(
            &bucket,
            crate::BucketSnapshotRequest {
                policy: true,
                tags: crate::BucketSnapshotTagsRequest::NotRequested,
                lifecycle: false,
                cors: false,
            },
            |snapshot, proof| {
                assert_eq!(proof.bucket, bucket);
                assert_eq!(
                    snapshot.policy,
                    crate::types::LoadedBucketSubresource::Loaded(
                        "{\"Version\":\"2012-10-17\",\"Statement\":[]}".to_string()
                    )
                );
                Ok(crate::BucketWriteSnapshotAction::release(Ok(())))
            },
        )
        .unwrap();
    snapshot_result.unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*remote_pg, &bucket)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn frontend_unix_bucket_snapshot_pair_mode_uses_storage_node() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let remote_data_dir = tmp.path().join("remote-bucket-snapshot-pair-node-1");
    let socket_path = tmp
        .path()
        .join("sockets")
        .join("bucket-snapshot-pair-node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let server_config = StorageNodeProcessConfig {
        node_id,
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: remote_data_dir.clone(),
        default_ec_shape: ec_shape,
        pg_ids: vec![0],
        socket_path: socket_path.clone(),
        pg_routes: vec![StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Active,
            primary_node_id: node_id,
            acting_set: vec![node_id],
        }],

        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    };
    let source_bucket = crate::tests::bucket_name("remote-pair-source");
    let destination_bucket = crate::tests::bucket_name("remote-pair-destination");
    let owner = crate::CanonicalUserId::from_principal("owner");
    {
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &server_config.data_dir,
            &server_config.pg_ids,
            server_config.default_ec_shape,
        )
        .unwrap();
        let remote_pg = remote.get_pg(0).unwrap();
        for bucket in [&source_bucket, &destination_bucket] {
            crate::PgMetadataStore::create_bucket(
                &*remote_pg,
                bucket,
                "owner",
                &owner,
                &crate::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
        }
        crate::PgMetadataStore::put_bucket_subresource(
            &*remote_pg,
            &source_bucket,
            crate::types::PutBucketSubresource {
                kind: crate::types::BucketSubresourceKind::Tagging,
                body: "<Tagging/>",
                aux: crate::types::BucketSubresourceAux::None,
            },
        )
        .unwrap();
        crate::PgMetadataStore::put_bucket_subresource(
            &*remote_pg,
            &destination_bucket,
            crate::types::PutBucketSubresource {
                kind: crate::types::BucketSubresourceKind::Cors,
                body: "<CORSConfiguration/>",
                aux: crate::types::BucketSubresourceAux::None,
            },
        )
        .unwrap();
        remote_pg.refresh_metadata_command_state_digest().unwrap();
    }
    let server = StorageNodeServer::bind(server_config).unwrap();
    let _server_guard = spawn_storage_node_server(server);

    let frontend_data_dir = tmp.path().join("frontend-bucket-snapshot-pair-routing");
    let mut map = LocalClusterMap::open_with_configs(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            frontend_data_dir.join("node-0001"),
        )],
        &[0],
        ec_shape,
    )
    .unwrap();
    map.install_unix_bucket_metadata_clients([LocalUnixBucketMetadataNodeClientConfig::new(
        node_id,
        socket_path,
    )])
    .unwrap();
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let pair = cluster
        .load_bucket_snapshot_pair(
            (
                &source_bucket,
                crate::BucketSnapshotRequest {
                    tags: crate::BucketSnapshotTagsRequest::Always,
                    ..Default::default()
                },
            ),
            (
                &destination_bucket,
                crate::BucketSnapshotRequest {
                    cors: true,
                    ..Default::default()
                },
            ),
        )
        .unwrap();
    assert_eq!(
        pair.source().tags,
        crate::LoadedBucketSubresource::Loaded("<Tagging/>".to_string())
    );
    assert_eq!(
        pair.destination().cors,
        crate::LoadedBucketSubresource::Loaded("<CORSConfiguration/>".to_string())
    );
    let frontend_pg = map.node(node_id).unwrap().storage_node().get_pg(0).unwrap();
    assert!(crate::PgMetadataStore::head_bucket_raw(&*frontend_pg, &source_bucket).is_err());
    assert!(crate::PgMetadataStore::head_bucket_raw(&*frontend_pg, &destination_bucket).is_err());
}

#[test]
fn frontend_unix_object_mutation_stream_append_reads_route_to_storage_node() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let remote_data_dir = tmp.path().join("remote-stream-append-node-1");
    let socket_path = tmp
        .path()
        .join("sockets")
        .join("remote-stream-append-node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let bucket = crate::tests::bucket_name("remote-stream-append-rpc");
    let key = crate::tests::object_key("key");
    let session_id = crate::tests::stream_session_id("rsappend");
    {
        let remote =
            SharedStorageNode::open_with_default_ec_shape(&remote_data_dir, &[0], ec_shape)
                .unwrap();
        let remote_pg = remote.get_pg(0).unwrap();
        crate::PgMetadataStore::create_stream_upload(
            &*remote_pg,
            &crate::CreateStreamUploadReq {
                session_id: session_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: crate::StreamUploadTarget::PutObject,
                encryption: crate::ObjectEncryption::None,
            },
        )
        .unwrap();
        assert_eq!(
            crate::PgMetadataStore::reserve_object_generation(
                &*remote_pg,
                &bucket,
                &key,
                &session_id,
            )
            .unwrap(),
            GenerationId::new(1).unwrap()
        );
        remote_pg.refresh_metadata_command_state_digest().unwrap();
    }
    let server_config = StorageNodeProcessConfig {
        node_id,
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: remote_data_dir,
        default_ec_shape: ec_shape,
        pg_ids: vec![0],
        socket_path: socket_path.clone(),
        pg_routes: vec![StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Active,
            primary_node_id: node_id,
            acting_set: vec![node_id],
        }],

        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    };
    let server = StorageNodeServer::bind(server_config).unwrap();
    let _server_guard = spawn_storage_node_server(server);

    let mut map = LocalClusterMap::open_with_configs(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            tmp.path().join("frontend-stream-append").join("node-0001"),
        )],
        &[0],
        ec_shape,
    )
    .unwrap();
    map.install_unix_object_mutation_metadata_clients([
        LocalUnixObjectMutationMetadataNodeClientConfig::new(node_id, socket_path),
    ])
    .unwrap();
    let frontend_pg = map.node(node_id).unwrap().storage_node().get_pg(0).unwrap();
    assert!(matches!(
        crate::PgMetadataStore::get_stream_upload(&*frontend_pg, &session_id),
        Err(crate::MetadataError::StreamSessionNotFound { .. })
    ));
    let mutation_client = Arc::clone(map.node(node_id).unwrap().object_mutation_metadata_client());

    let loaded = mutation_client
        .load_stream_upload_session(
            ObjectMetadataPgId::new_for_test(PgId::new(0)),
            &bucket,
            &key,
            &session_id,
        )
        .unwrap();
    assert_eq!(loaded.session_id, session_id);
    let segments = mutation_client
        .load_stream_upload_segments(
            ObjectMetadataPgId::new_for_test(PgId::new(0)),
            &bucket,
            &key,
            &loaded.session_id,
        )
        .unwrap();
    assert!(segments.is_empty());
    let (target, segment) = mutation_client
        .prepare_stream_segment_append(
            ObjectMetadataPgId::new_for_test(PgId::new(0)),
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: loaded.session_id.clone(),
                segment_index: 0,
                size: 11,
                segment_crc64: 123,
                payload_crc64: 123,
                segment_okh: [7; 16],
            },
        )
        .unwrap();
    assert_eq!(target, crate::StreamUploadTarget::PutObject);
    assert_eq!(segment.session_id, loaded.session_id);
    assert_eq!(segment.segment_index, 0);
    assert_eq!(segment.size, 11);
    assert_eq!(segment.segment_crc64, 123);
    assert_eq!(
        segment.segment_okh,
        crate::segment_key_hash(
            bucket.as_str(),
            key.as_str(),
            GenerationId::new(1).unwrap(),
            0
        )
    );
}

#[test]
fn frontend_unix_object_generation_loser_retries_stale_generation() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let remote_data_dir = tmp
        .path()
        .join("remote-object-generation-stale-loser-node-1");
    let socket_path = tmp
        .path()
        .join("sockets")
        .join("object-generation-stale-loser-node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let server_config = StorageNodeProcessConfig {
        node_id,
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: remote_data_dir.clone(),
        default_ec_shape: ec_shape,
        pg_ids: vec![0],
        socket_path: socket_path.clone(),
        pg_routes: vec![StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Active,
            primary_node_id: node_id,
            acting_set: vec![node_id],
        }],

        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    };
    let server = StorageNodeServer::bind(server_config.clone()).unwrap();
    let _server_guard = spawn_storage_node_server(server);

    let build_frontend = |name: &str| {
        let mut map = LocalClusterMap::open_with_configs(
            node_id,
            [LocalNodeStoreConfig::new(
                node_id,
                tmp.path().join(name).join("node-0001"),
            )],
            &[0],
            ec_shape,
        )
        .unwrap();
        map.install_unix_metadata_command_clients([LocalUnixMetadataCommandNodeClientConfig::new(
            node_id,
            socket_path.clone(),
        )])
        .unwrap();
        map.install_unix_object_generation_metadata_clients([
            LocalUnixObjectGenerationMetadataNodeClientConfig::new(node_id, socket_path.clone()),
        ])
        .unwrap();
        let map = Arc::new(map);
        let cluster = StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        (map, cluster)
    };
    let (loser_map, loser) = build_frontend("frontend-loser");
    let (_winner_map, winner) = build_frontend("frontend-winner");
    let winner = Arc::new(winner);
    let bucket = crate::tests::bucket_name("remote-object-generation-stale-loser");
    let key = crate::tests::object_key("key");
    let winner_reservation = crate::tests::stream_session_id("winner-gen");
    let loser_reservation = crate::tests::stream_session_id("loser-gen");
    let hook_ran = Arc::new(AtomicBool::new(false));
    let _hook_guard = loser.test_install_before_object_generation_command_id_hook({
        let winner = Arc::clone(&winner);
        let bucket = bucket.clone();
        let key = key.clone();
        let winner_reservation = winner_reservation.clone();
        let hook_ran = Arc::clone(&hook_ran);
        Arc::new(move || {
            if !hook_ran.swap(true, Ordering::SeqCst) {
                let generation = winner
                    .reserve_put_object_generation(&bucket, &key, &winner_reservation)
                    .unwrap();
                assert_eq!(generation, GenerationId::new(1).unwrap());
            }
        })
    });

    let loser_generation = loser
        .reserve_put_object_generation(&bucket, &key, &loser_reservation)
        .unwrap();

    assert!(hook_ran.load(Ordering::SeqCst));
    assert_eq!(loser_generation, GenerationId::new(2).unwrap());
    let frontend_pg = loser_map
        .node(node_id)
        .unwrap()
        .storage_node()
        .get_pg(0)
        .unwrap();
    assert!(matches!(
        crate::PgMetadataStore::get_object_generation_reservation(
            &*frontend_pg,
            &bucket,
            &key,
            &loser_reservation
        ),
        Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
    ));
    let remote = SharedStorageNode::open_with_default_ec_shape(
        &server_config.data_dir,
        &server_config.pg_ids,
        server_config.default_ec_shape,
    )
    .unwrap();
    let remote_pg = remote.get_pg(0).unwrap();
    assert_eq!(
        crate::PgMetadataStore::get_object_generation_reservation(
            &*remote_pg,
            &bucket,
            &key,
            &winner_reservation
        )
        .unwrap(),
        GenerationId::new(1).unwrap()
    );
    assert_eq!(
        crate::PgMetadataStore::get_object_generation_reservation(
            &*remote_pg,
            &bucket,
            &key,
            &loser_reservation
        )
        .unwrap(),
        GenerationId::new(2).unwrap()
    );
}

#[test]
fn frontend_unix_object_generation_loser_retries_rpc_reservation_conflict() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let remote_data_dir = tmp
        .path()
        .join("remote-object-generation-conflict-loser-node-1");
    let socket_path = tmp
        .path()
        .join("sockets")
        .join("object-generation-conflict-loser-node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let server_config = StorageNodeProcessConfig {
        node_id,
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: remote_data_dir.clone(),
        default_ec_shape: ec_shape,
        pg_ids: vec![0],
        socket_path: socket_path.clone(),
        pg_routes: vec![StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Active,
            primary_node_id: node_id,
            acting_set: vec![node_id],
        }],

        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    };
    let server = StorageNodeServer::bind(server_config.clone()).unwrap();
    let _server_guard = spawn_storage_node_server(server);

    let build_frontend = |name: &str| {
        let mut map = LocalClusterMap::open_with_configs(
            node_id,
            [LocalNodeStoreConfig::new(
                node_id,
                tmp.path().join(name).join("node-0001"),
            )],
            &[0],
            ec_shape,
        )
        .unwrap();
        map.install_unix_metadata_command_clients([LocalUnixMetadataCommandNodeClientConfig::new(
            node_id,
            socket_path.clone(),
        )])
        .unwrap();
        map.install_unix_object_generation_metadata_clients([
            LocalUnixObjectGenerationMetadataNodeClientConfig::new(node_id, socket_path.clone()),
        ])
        .unwrap();
        let map = Arc::new(map);
        let cluster = StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        (map, cluster)
    };
    let (map, loser) = build_frontend("frontend-loser");
    let (_winner_map, winner) = build_frontend("frontend-winner");
    let winner = Arc::new(winner);
    let bucket = crate::tests::bucket_name("remote-object-generation-rpc-conflict");
    let key = crate::tests::object_key("key");
    let winner_reservation = crate::tests::stream_session_id("winner-gen");
    let loser_reservation = crate::tests::stream_session_id("loser-gen");
    let hook_ran = Arc::new(AtomicBool::new(false));
    let _hook_guard = loser.test_install_before_metadata_command_pending_install_hook({
        let winner = Arc::clone(&winner);
        let bucket = bucket.clone();
        let key = key.clone();
        let winner_reservation = winner_reservation.clone();
        let hook_ran = Arc::clone(&hook_ran);
        Arc::new(move || {
            if hook_ran.swap(true, Ordering::SeqCst) {
                return;
            }
            let generation = winner
                .reserve_put_object_generation(&bucket, &key, &winner_reservation)
                .unwrap();
            assert_eq!(generation, GenerationId::new(1).unwrap());
        })
    });

    let loser_generation = loser
        .reserve_put_object_generation(&bucket, &key, &loser_reservation)
        .unwrap();

    assert!(hook_ran.load(Ordering::SeqCst));
    assert_eq!(loser_generation, GenerationId::new(2).unwrap());
    let frontend_pg = map.node(node_id).unwrap().storage_node().get_pg(0).unwrap();
    assert!(matches!(
        crate::PgMetadataStore::get_object_generation_reservation(
            &*frontend_pg,
            &bucket,
            &key,
            &loser_reservation
        ),
        Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
    ));
    let remote = SharedStorageNode::open_with_default_ec_shape(
        &server_config.data_dir,
        &server_config.pg_ids,
        server_config.default_ec_shape,
    )
    .unwrap();
    let remote_pg = remote.get_pg(0).unwrap();
    assert_eq!(
        crate::PgMetadataStore::get_object_generation_reservation(
            &*remote_pg,
            &bucket,
            &key,
            &winner_reservation
        )
        .unwrap(),
        GenerationId::new(1).unwrap()
    );
    assert_eq!(
        crate::PgMetadataStore::get_object_generation_reservation(
            &*remote_pg,
            &bucket,
            &key,
            &loser_reservation
        )
        .unwrap(),
        GenerationId::new(2).unwrap()
    );
}

#[test]
fn unix_metadata_command_client_install_rejects_relative_socket_path() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let mut map = LocalClusterMap::open_with_configs(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            tmp.path().join("node-0001"),
        )],
        &[0],
        ec_shape,
    )
    .unwrap();

    let err = map
        .install_unix_metadata_command_clients([LocalUnixMetadataCommandNodeClientConfig::new(
            node_id,
            PathBuf::from("relative-metadata-node-1.sock"),
        )])
        .unwrap_err();
    assert!(matches!(
        err,
        ClusterBuildError::RemoteMetadataCommandClientSocketPathNotAbsolute { path }
            if path == Path::new("relative-metadata-node-1.sock")
    ));
}

#[test]
fn unix_bucket_metadata_client_install_rejects_relative_socket_path() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let mut map = LocalClusterMap::open_with_configs(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            tmp.path().join("node-0001"),
        )],
        &[0],
        ec_shape,
    )
    .unwrap();

    let err = map
        .install_unix_bucket_metadata_clients([LocalUnixBucketMetadataNodeClientConfig::new(
            node_id,
            PathBuf::from("relative-bucket-metadata-node-1.sock"),
        )])
        .unwrap_err();
    assert!(matches!(
        err,
        ClusterBuildError::RemoteBucketMetadataClientSocketPathNotAbsolute { path }
            if path == Path::new("relative-bucket-metadata-node-1.sock")
    ));
}

#[test]
fn unix_bucket_write_reservation_client_install_rejects_relative_socket_path() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let mut map = LocalClusterMap::open_with_configs(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            tmp.path().join("node-0001"),
        )],
        &[0],
        ec_shape,
    )
    .unwrap();

    let err = map
        .install_unix_bucket_write_reservation_clients([
            LocalUnixBucketWriteReservationNodeClientConfig::new(
                node_id,
                PathBuf::from("relative-bucket-write-node-1.sock"),
            ),
        ])
        .unwrap_err();
    assert!(matches!(
        err,
        ClusterBuildError::RemoteBucketWriteReservationClientSocketPathNotAbsolute { path }
            if path == Path::new("relative-bucket-write-node-1.sock")
    ));
}

#[test]
fn unix_bucket_write_reservation_client_install_requires_matching_bucket_metadata_client() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let mut map = LocalClusterMap::open_with_configs(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            tmp.path().join("node-0001"),
        )],
        &[0],
        ec_shape,
    )
    .unwrap();
    let bucket_write_socket_path = tmp.path().join("bucket-write-node-1.sock");

    let err = map
        .install_unix_bucket_write_reservation_clients([
            LocalUnixBucketWriteReservationNodeClientConfig::new(
                node_id,
                bucket_write_socket_path.clone(),
            ),
        ])
        .unwrap_err();
    assert!(matches!(
        err,
        ClusterBuildError::RemoteBucketWriteReservationClientMissingBucketMetadataClient { id }
            if id == node_id.as_u32()
    ));

    let bucket_metadata_socket_path = tmp.path().join("bucket-metadata-node-1.sock");
    map.install_unix_bucket_metadata_clients([LocalUnixBucketMetadataNodeClientConfig::new(
        node_id,
        bucket_metadata_socket_path.clone(),
    )])
    .unwrap();
    let err = map
        .install_unix_bucket_write_reservation_clients([
            LocalUnixBucketWriteReservationNodeClientConfig::new(
                node_id,
                bucket_write_socket_path.clone(),
            ),
        ])
        .unwrap_err();
    assert!(matches!(
        err,
        ClusterBuildError::RemoteBucketWriteReservationClientMismatchedBucketMetadataClient {
            id,
            bucket_metadata_socket_path: actual_bucket_metadata_socket_path,
            bucket_write_reservation_socket_path: actual_bucket_write_socket_path,
        } if id == node_id.as_u32()
            && actual_bucket_metadata_socket_path == bucket_metadata_socket_path
            && actual_bucket_write_socket_path == bucket_write_socket_path
    ));

    map.install_unix_bucket_write_reservation_clients([
        LocalUnixBucketWriteReservationNodeClientConfig::new(
            node_id,
            bucket_metadata_socket_path.clone(),
        ),
    ])
    .unwrap();

    let replacement_bucket_metadata_socket_path =
        tmp.path().join("replacement-bucket-metadata-node-1.sock");
    let err = map
        .install_unix_bucket_metadata_clients([LocalUnixBucketMetadataNodeClientConfig::new(
            node_id,
            replacement_bucket_metadata_socket_path.clone(),
        )])
        .unwrap_err();
    assert!(matches!(
        err,
        ClusterBuildError::RemoteBucketMetadataClientMismatchedBucketWriteReservationClient {
            id,
            bucket_metadata_socket_path: actual_bucket_metadata_socket_path,
            bucket_write_reservation_socket_path: actual_bucket_write_socket_path,
        } if id == node_id.as_u32()
            && actual_bucket_metadata_socket_path == replacement_bucket_metadata_socket_path
            && actual_bucket_write_socket_path == bucket_metadata_socket_path
    ));

    map.install_unix_bucket_metadata_clients([LocalUnixBucketMetadataNodeClientConfig::new(
        node_id,
        bucket_metadata_socket_path,
    )])
    .unwrap();
}

#[test]
fn unix_object_generation_metadata_client_install_rejects_relative_socket_path() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let mut map = LocalClusterMap::open_with_configs(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            tmp.path().join("node-0001"),
        )],
        &[0],
        ec_shape,
    )
    .unwrap();

    let err = map
        .install_unix_object_generation_metadata_clients([
            LocalUnixObjectGenerationMetadataNodeClientConfig::new(
                node_id,
                PathBuf::from("relative-object-generation-node-1.sock"),
            ),
        ])
        .unwrap_err();
    assert!(matches!(
        err,
        ClusterBuildError::RemoteObjectGenerationMetadataClientSocketPathNotAbsolute { path }
            if path == Path::new("relative-object-generation-node-1.sock")
    ));
}

#[test]
fn unix_object_version_metadata_client_install_rejects_relative_socket_path() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let mut map = LocalClusterMap::open_with_configs(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            tmp.path().join("node-0001"),
        )],
        &[0],
        ec_shape,
    )
    .unwrap();

    let err = map
        .install_unix_object_version_metadata_clients([
            LocalUnixObjectVersionMetadataNodeClientConfig::new(
                node_id,
                PathBuf::from("relative-object-version-node-1.sock"),
            ),
        ])
        .unwrap_err();
    assert!(matches!(
        err,
        ClusterBuildError::RemoteObjectVersionMetadataClientSocketPathNotAbsolute { path }
            if path == Path::new("relative-object-version-node-1.sock")
    ));
}

#[test]
fn unix_direct_put_metadata_client_install_rejects_relative_socket_path() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let mut map = LocalClusterMap::open_with_configs(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            tmp.path().join("node-0001"),
        )],
        &[0],
        ec_shape,
    )
    .unwrap();

    let err = map
        .install_unix_direct_put_metadata_clients([
            LocalUnixDirectPutMetadataNodeClientConfig::new(
                node_id,
                PathBuf::from("relative-direct-put-node-1.sock"),
            ),
        ])
        .unwrap_err();
    assert!(matches!(
        err,
        ClusterBuildError::RemoteDirectPutMetadataClientSocketPathNotAbsolute { path }
            if path == Path::new("relative-direct-put-node-1.sock")
    ));
}

#[test]
fn unix_object_mutation_metadata_client_install_rejects_relative_socket_path() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let mut map = LocalClusterMap::open_with_configs(
        node_id,
        [LocalNodeStoreConfig::new(
            node_id,
            tmp.path().join("node-0001"),
        )],
        &[0],
        ec_shape,
    )
    .unwrap();

    let err = map
        .install_unix_object_mutation_metadata_clients([
            LocalUnixObjectMutationMetadataNodeClientConfig::new(
                node_id,
                PathBuf::from("relative-object-mutation-node-1.sock"),
            ),
        ])
        .unwrap_err();
    assert!(matches!(
        err,
        ClusterBuildError::RemoteObjectMutationMetadataClientSocketPathNotAbsolute { path }
            if path == Path::new("relative-object-mutation-node-1.sock")
    ));
}

#[test]
fn unix_object_mutation_client_repeats_suspended_null_delete_marker() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_id = NodeId::new(0);
    let pg_id = PgId::new(0);
    let ec_shape = EcShape { k: 1, m: 0 };
    let remote_data_dir = tmp.path().join("remote-repeat-suspended-delete");
    let socket_path = tmp
        .path()
        .join("sockets")
        .join("repeat-suspended-delete.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let bucket = crate::tests::bucket_name("unix-repeat-suspended-delete");
    let key = crate::tests::object_key("key");
    let owner = crate::OwnerIdentity::from_principal("owner");

    {
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &remote_data_dir,
            &[pg_id.get()],
            ec_shape,
        )
        .unwrap();
        let pg = remote.get_pg(pg_id.get()).unwrap();
        crate::PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &owner.canonical_id,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        crate::PgMetadataStore::put_bucket_versioning(
            &*pg,
            &bucket,
            crate::BucketVersioningState::Suspended,
        )
        .unwrap();
        crate::PgMetadataStore::put_object_meta(
            &*pg,
            &crate::PutObjectReq::DeleteMarker(crate::PutDeleteMarkerReq {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: crate::VersionId::Null,
                owner: owner.clone(),
            }),
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }

    let server = StorageNodeServer::bind(StorageNodeProcessConfig {
        node_id,
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: remote_data_dir.clone(),
        default_ec_shape: ec_shape,
        pg_ids: vec![pg_id.get()],
        socket_path: socket_path.clone(),
        pg_routes: vec![StorageNodePgRoute {
            pg_id: pg_id.get(),
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Active,
            primary_node_id: node_id,
            acting_set: vec![node_id],
        }],
        pending_metadata_command_recoveries: Vec::new(),
        historical_pg_routes: Vec::new(),
    })
    .unwrap();
    let _server_guard = spawn_storage_node_server(server);

    let mut map = LocalClusterMap::open_frontend_topology_only_with_epoch(
        node_id,
        [node_id],
        &[pg_id.get()],
        ec_shape,
        ClusterEpoch::INITIAL,
    )
    .unwrap();
    map.install_unix_storage_node_clients([LocalUnixStorageNodeClientConfig::new(
        node_id,
        socket_path,
    )])
    .unwrap();
    let cluster = StorageCluster::from_local_map(Arc::new(map)).unwrap();

    let repeated = cluster
        .insert_current_delete_marker_if(
            &bucket,
            &key,
            crate::BucketVersioningState::Suspended,
            owner,
            |stored| {
                assert!(matches!(stored, Some(crate::StoredObject::DeleteMarker(_))));
                Ok::<_, ()>(())
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(repeated.version_id, crate::VersionId::Null);
    let remote =
        SharedStorageNode::open_with_default_ec_shape(&remote_data_dir, &[pg_id.get()], ec_shape)
            .unwrap();
    let remote_pg = remote.get_pg(pg_id.get()).unwrap();
    assert!(matches!(
        crate::PgMetadataStore::get_object_meta(&*remote_pg, &bucket, &key).unwrap(),
        crate::StoredObject::DeleteMarker(marker) if marker.version_id == crate::VersionId::Null
    ));
}

#[test]
fn unix_shard_client_install_rejects_relative_socket_paths_before_mutation() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
    let err = map
        .install_unix_shard_clients([
            LocalUnixShardNodeClientConfig::new(
                NodeId::new(0),
                tmp.path().join("sockets").join("node-0.sock"),
            ),
            LocalUnixShardNodeClientConfig::new(NodeId::new(1), "relative-node-1.sock"),
        ])
        .unwrap_err();
    assert!(matches!(
        err,
        ClusterBuildError::RemoteShardClientSocketPathNotAbsolute { path }
            if path == Path::new("relative-node-1.sock")
    ));

    let data_pg_id = DataPgId::new_for_test(PgId::new(0));
    let key = ShardKey::new(&[0x62; 16], 100, 0);
    let location = ShardLocation::new(
        ClusterEpoch::INITIAL,
        data_pg_id,
        key.shard_index(),
        NodeId::new(0),
    );
    let payload = b"still-local-after-failed-install";
    let ack = map
        .write_payload_shard(ClusterEpoch::INITIAL, location, &key, payload)
        .unwrap();
    assert_eq!(ack.stored_size, payload.len() as u64);
    assert_eq!(
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .read_shard_file(0, &key)
            .unwrap(),
        payload
    );
}

#[test]
fn unix_shard_client_fails_closed_when_storage_node_is_unavailable() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
    let socket_path = tmp.path().join("sockets").join("dead-node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    map.install_unix_shard_clients([LocalUnixShardNodeClientConfig::new(
        NodeId::new(1),
        socket_path,
    )])
    .unwrap();

    let data_pg_id = DataPgId::new_for_test(PgId::new(0));
    let key = ShardKey::new(&[0x63; 16], 101, 0);
    let location = ShardLocation::new(
        ClusterEpoch::INITIAL,
        data_pg_id,
        key.shard_index(),
        NodeId::new(1),
    );
    let write_err = map
        .write_payload_shard(ClusterEpoch::INITIAL, location, &key, b"dead owner")
        .unwrap_err();
    assert!(matches!(
        write_err,
        ShardIoError::Store {
            source: StoreError::Io {
                context: "connect storage-node RPC endpoint",
                ..
            },
            ..
        }
    ));

    let read_err = map
        .read_payload_shard(
            ClusterEpoch::INITIAL,
            location,
            &key,
            WriteAck {
                stored_size: 10,
                crc64: 0x1234,
            },
        )
        .unwrap_err();
    assert!(matches!(
        read_err,
        ShardIoError::Store {
            source: StoreError::Io {
                context: "connect storage-node read-handle RPC endpoint",
                ..
            },
            ..
        }
    ));
    assert!(matches!(
        map.node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .read_shard_file(0, &key),
        Err(StoreError::NotFound)
    ));
}

#[test]
fn direct_put_publishes_after_remote_shard_io_and_ack_validation() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let frontend_dir = tmp.path().join("frontend");
    let mut map = LocalClusterMap::open(&frontend_dir, &node_ids, &[0], ec_shape).unwrap();
    let mut server_configs = Vec::new();
    let mut shard_client_configs = Vec::new();
    for node_id in node_ids {
        let socket_path = tmp
            .path()
            .join("sockets")
            .join(format!("node-{}.sock", node_id.as_u32()));
        private_socket_dir(socket_path.parent().unwrap());
        server_configs.push(StorageNodeProcessConfig {
            node_id,
            cluster_epoch: ClusterEpoch::INITIAL,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: tmp.path().join(format!("remote-node-{}", node_id.as_u32())),
            default_ec_shape: ec_shape,
            pg_ids: vec![0],
            socket_path: socket_path.clone(),
            pg_routes: vec![StorageNodePgRoute {
                pg_id: 0,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: PgState::Active,
                primary_node_id: NodeId::new(0),
                acting_set: node_ids.to_vec(),
            }],

            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: Vec::new(),
        });
        shard_client_configs.push(LocalUnixShardNodeClientConfig::new(node_id, socket_path));
    }
    let mut server_threads = Vec::new();
    for config in server_configs.iter().cloned() {
        let expected_connections = if config.node_id == NodeId::new(0) {
            7
        } else {
            3
        };
        let server = Arc::new(StorageNodeServer::bind(config).unwrap());
        for _ in 0..expected_connections {
            let server = Arc::clone(&server);
            server_threads.push(thread::spawn(move || server.accept_one().unwrap()));
        }
    }
    map.install_unix_shard_clients(shard_client_configs)
        .unwrap();
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let committed = write_committed_direct_segment(&cluster, b"direct put remote payload");

    assert_eq!(committed.payload, b"direct put remote payload");
    assert_eq!(committed.written.written_shards.len(), 3);
    for thread in server_threads {
        thread.join().unwrap();
    }
    for written in &committed.written.written_shards {
        let location = committed
            .locations
            .iter()
            .copied()
            .find(|location| location.shard_index() == written.key.shard_index())
            .unwrap();
        assert!(matches!(
            map.node(location.node_id())
                .unwrap()
                .storage_node()
                .read_shard_file(committed.written.data_pg_id, &written.key),
            Err(StoreError::NotFound)
        ));
        let remote_config = server_configs
            .iter()
            .find(|config| config.node_id == location.node_id())
            .unwrap();
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &remote_config.data_dir,
            &remote_config.pg_ids,
            remote_config.default_ec_shape,
        )
        .unwrap();
        assert_eq!(
            remote
                .read_shard_file(committed.written.data_pg_id, &written.key)
                .unwrap()
                .len() as u64,
            written.ack.stored_size
        );
    }
    let data_pg_primary = server_configs
        .iter()
        .find(|config| config.node_id == NodeId::new(0))
        .unwrap();
    let remote_primary = SharedStorageNode::open_with_default_ec_shape(
        &data_pg_primary.data_dir,
        &data_pg_primary.pg_ids,
        data_pg_primary.default_ec_shape,
    )
    .unwrap();
    let remote_pg = remote_primary.get_pg(committed.written.data_pg_id).unwrap();
    for written in &committed.written.written_shards {
        remote_pg
            .validate_written_shard_ack(&written.key, written.ack)
            .unwrap();
    }
}

#[test]
fn non_current_epoch_unix_direct_put_commit_fails_closed_and_cleans_remote_state() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let current_epoch = ClusterEpoch::INITIAL;
    let stale_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
    let mut map =
        LocalClusterMap::open(&tmp.path().join("frontend"), &node_ids, &pg_ids, ec_shape).unwrap();
    for pg_id in pg_ids {
        set_route_primary(&mut map, pg_id, NodeId::new(0));
    }
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };

    let mut server_configs = Vec::new();
    let mut client_configs = Vec::new();
    for node_id in node_ids {
        let socket_path = tmp
            .path()
            .join("sockets")
            .join(format!("stale-direct-put-node-{}.sock", node_id.as_u32()));
        private_socket_dir(socket_path.parent().unwrap());
        let pg_routes = pg_ids
            .iter()
            .map(|pg_id| StorageNodePgRoute {
                pg_id: *pg_id,
                cluster_epoch: current_epoch,
                state: PgState::Active,
                primary_node_id: NodeId::new(0),
                acting_set: node_ids.to_vec(),
            })
            .collect();
        server_configs.push(StorageNodeProcessConfig {
            node_id,
            cluster_epoch: current_epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: tmp
                .path()
                .join(format!("remote-stale-direct-put-{}", node_id.as_u32())),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.to_vec(),
            socket_path: socket_path.clone(),
            pg_routes,
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: Vec::new(),
        });
        client_configs.push(LocalUnixStorageNodeClientConfig::new(node_id, socket_path));
    }
    let mut _server_guards = Vec::new();
    for config in server_configs.iter().cloned() {
        let server = StorageNodeServer::bind(config).unwrap();
        _server_guards.push(spawn_storage_node_server(server));
    }
    map.install_unix_storage_node_clients(client_configs)
        .unwrap();
    let map = Arc::new(map);
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&current_cluster, &bucket);

    let reservation_id =
        crate::SessionId::try_from("56565656565656565656565656565656".to_string()).unwrap();
    let generation_id = current_cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let before_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    let payload = b"stale epoch remote direct put";
    let segment_okh = [0xc4; 16];
    let written = current_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let commit_req = direct_put_commit_req(
        &current_cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );
    let stale_cluster =
        crate::StorageCluster::test_from_local_map_with_epoch(Arc::clone(&map), stale_epoch)
            .unwrap();

    let err = stale_cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    // The stale operation epoch is rejected by local route validation before
    // the direct-put command-build RPC. The remote assertions below prove that
    // cleanup still uses the proof/current placement epoch through Unix clients.
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == stale_epoch
                && observed_current_epoch == current_epoch
        ),
        "stale Unix direct PUT commit should fail closed before command build, got {err:?}"
    );

    let after_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof, before_object_pg_proof,
        "stale Unix direct PUT commit must not append an object-PG command"
    );
    let remote_primary_config = server_configs
        .iter()
        .find(|config| config.node_id == NodeId::new(0))
        .unwrap();
    let remote_primary = SharedStorageNode::open_with_default_ec_shape(
        &remote_primary_config.data_dir,
        &remote_primary_config.pg_ids,
        remote_primary_config.default_ec_shape,
    )
    .unwrap();
    let remote_object_pg = remote_primary.get_pg(object_pg).unwrap();
    assert!(
        matches!(
            crate::PgMetadataStore::get_object_meta(&*remote_object_pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ),
        "stale Unix direct PUT must not publish object metadata"
    );
    let remote_bucket_pg = remote_primary
        .get_pg(current_cluster.test_bucket_pg_id_for(&bucket))
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*remote_bucket_pg, &bucket)
            .unwrap()
            .is_empty(),
        "stale Unix direct PUT cleanup must release caller-owned bucket write proof"
    );

    for written_shard in &written.written_shards {
        for config in &server_configs {
            let remote = SharedStorageNode::open_with_default_ec_shape(
                &config.data_dir,
                &config.pg_ids,
                config.default_ec_shape,
            )
            .unwrap();
            assert!(
                matches!(
                    remote.read_shard_file(written.data_pg_id, &written_shard.key),
                    Err(StoreError::NotFound)
                ),
                "stale Unix direct PUT cleanup must delete shard {} from node {}",
                written_shard.key,
                config.node_id.as_u32()
            );
        }
    }
    let remote_data_pg = remote_primary.get_pg(written.data_pg_id).unwrap();
    for written_shard in &written.written_shards {
        assert!(
            matches!(
                remote_data_pg.validate_written_shard_ack(&written_shard.key, written_shard.ack),
                Err(StoreError::NotFound)
            ),
            "stale Unix direct PUT cleanup must delete remote ack for shard {}",
            written_shard.key
        );
    }
}

#[test]
fn control_plane_peering_unix_direct_put_old_primary_fails_closed_and_cleans_remote_state() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
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
                            .join("control-plane-sockets")
                            .join(format!("node-{}.sock", node_id.as_u32()))
                            .to_string_lossy()
                            .into_owned(),
                    )
                })
                .collect(),
            pg_ids.iter().copied().map(PgId::new).collect(),
        )
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    let source_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), source_epoch)
                .unwrap();
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                PgState::Active,
            )
        })
        .collect::<Vec<_>>();

    let source_storage_configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("remote-peering-stale-direct-put")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();

    let source_map = Arc::new(
        LocalClusterMap::open_frontend_with_configs_and_pg_routes(
            NodeId::new(0),
            source_storage_configs.clone(),
            &pg_ids,
            ec_shape,
            source_epoch,
            source_routes.iter().map(LocalPgRoute::from),
        )
        .unwrap(),
    );
    let (bucket, key, object_pg, _data_pg) = {
        let topology = source_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let source_cluster = crate::StorageCluster::from_local_map(Arc::clone(&source_map)).unwrap();
    create_test_bucket(&source_cluster, &bucket);

    let reservation_id =
        crate::SessionId::try_from("68686868686868686868686868686868".to_string()).unwrap();
    let generation_id = source_cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let before_object_pg_proof = source_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    let payload = b"control-plane peering stale unix direct put";
    let segment_okh = [0xc8; 16];
    let written = source_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    source_cluster
        .test_register_payload_shard_acks(written.data_pg_id, &written.written_shards)
        .unwrap();
    let source_data_pg_primary = source_routes
        .iter()
        .find(|route| route.pg_id() == PgId::new(written.data_pg_id))
        .unwrap()
        .primary_node_id();
    {
        let primary_config = source_storage_configs
            .iter()
            .find(|config| config.node_id == source_data_pg_primary)
            .unwrap();
        let remote_primary = SharedStorageNode::open_with_default_ec_shape(
            &primary_config.data_dir,
            &pg_ids,
            ec_shape,
        )
        .unwrap();
        let remote_data_pg = remote_primary.get_pg(written.data_pg_id).unwrap();
        for written_shard in &written.written_shards {
            remote_data_pg
                .validate_written_shard_ack(&written_shard.key, written_shard.ack)
                .unwrap();
        }
    }
    let bucket_write_reservation = source_cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "control-plane-peering-stale-unix-direct-put",
            Some(key.as_str()),
        )
        .unwrap();
    let commit_req = direct_put_commit_req_with_bucket_write_proof(
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
        crate::metadata_command::BucketWriteReservationProof::from(
            &bucket_write_reservation.record,
        ),
    );
    drop(source_cluster);
    drop(source_map);

    authority
        .set_pg_acting_set(PgId::new(object_pg), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    let current_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), current_epoch)
                .unwrap();
            let state = if *pg_id == object_pg {
                PgState::Peering
            } else {
                PgState::Active
            };
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                state,
            )
        })
        .collect::<Vec<_>>();
    let historical_pg_routes = source_routes
        .iter()
        .map(|route| StorageNodePgRoute {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            acting_set: route.acting_set().to_vec(),
        })
        .collect::<Vec<_>>();
    let current_pg_routes = current_routes
        .iter()
        .map(|route| StorageNodePgRoute {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            acting_set: route.acting_set().to_vec(),
        })
        .collect::<Vec<_>>();
    let mut server_configs = Vec::new();
    let mut client_configs = Vec::new();
    for node_config in &source_storage_configs {
        let socket_path = tmp.path().join("sockets").join(format!(
            "peering-stale-direct-put-node-{}.sock",
            node_config.node_id.as_u32()
        ));
        private_socket_dir(socket_path.parent().unwrap());
        server_configs.push(StorageNodeProcessConfig {
            node_id: node_config.node_id,
            cluster_epoch: current_epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: node_config.data_dir.clone(),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.to_vec(),
            socket_path: socket_path.clone(),
            pg_routes: current_pg_routes.clone(),
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: historical_pg_routes.clone(),
        });
        client_configs.push(LocalUnixStorageNodeClientConfig::new(
            node_config.node_id,
            socket_path,
        ));
    }
    let mut _server_guards = Vec::new();
    for config in server_configs.iter().cloned() {
        let server = StorageNodeServer::bind(config).unwrap();
        _server_guards.push(spawn_storage_node_server(server));
    }

    let frontend_configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("frontend-peering-stale-direct-put")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let mut current_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        frontend_configs,
        &pg_ids,
        ec_shape,
        current_epoch,
        current_routes.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    current_map.test_install_historical_pg_routes(source_routes.clone());
    current_map
        .install_unix_storage_node_clients(client_configs)
        .unwrap();
    let current_map = Arc::new(current_map);
    assert_eq!(
        current_map.pg_route(PgId::new(object_pg)).unwrap().state(),
        PgState::Peering,
        "control-plane acting-set change should put the object PG into Peering"
    );
    assert!(
        !current_map
            .pg_route(PgId::new(object_pg))
            .unwrap()
            .acting_set()
            .contains(&NodeId::new(0)),
        "the old source primary should no longer be in the current acting set"
    );
    let old_primary_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&current_map),
        source_epoch,
    )
    .unwrap();

    let err = old_primary_cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == source_epoch
                && observed_current_epoch == current_epoch
        ),
        "old-primary Unix direct PUT commit should fail closed after control-plane Peering transition, got {err:?}"
    );

    let bucket_pg = current_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .bucket_pg_for(&bucket);
    for config in &server_configs {
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let object_pg_store = remote.get_pg(object_pg).unwrap();
        let state = object_pg_store.metadata_command_replica_state().unwrap();
        let proof = crate::control_plane::PgMetadataProof {
            applied_log_index: state.applied_log_index,
            applied_log_hash: state.applied_log_hash,
            state_digest: state.state_digest,
        };
        assert_eq!(
            proof, before_object_pg_proof,
            "old-primary Unix direct PUT must not append an object-PG command on node {:?}",
            config.node_id
        );
        assert!(
            matches!(
                crate::PgMetadataStore::get_object_meta(&*object_pg_store, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ),
            "old-primary Unix direct PUT must not publish object metadata on node {:?}",
            config.node_id
        );
        assert!(
            object_pg_store
                .pending_metadata_command_envelope(config.node_id.as_u32(), source_epoch)
                .unwrap()
                .is_none(),
            "old-primary Unix direct PUT must not leave a source-epoch pending command on node {:?}",
            config.node_id
        );
        assert!(
            object_pg_store
                .pending_metadata_command_envelope(config.node_id.as_u32(), current_epoch)
                .unwrap()
                .is_none(),
            "old-primary Unix direct PUT must not leave a current-epoch pending command on node {:?}",
            config.node_id
        );
        let bucket_pg_store = remote.get_pg(bucket_pg).unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg_store, &bucket,)
                .unwrap()
                .is_empty(),
            "old-primary Unix direct PUT must release bucket write reservations on node {:?}",
            config.node_id
        );
        for written_shard in &written.written_shards {
            assert!(
                matches!(
                    remote.read_shard_file(written.data_pg_id, &written_shard.key),
                    Err(StoreError::NotFound)
                ),
                "old-primary Unix direct PUT must delete shard {} from node {:?}",
                written_shard.key,
                config.node_id
            );
        }
    }
    let source_primary_config = server_configs
        .iter()
        .find(|config| config.node_id == source_data_pg_primary)
        .unwrap();
    let remote_primary = SharedStorageNode::open_with_default_ec_shape(
        &source_primary_config.data_dir,
        &source_primary_config.pg_ids,
        source_primary_config.default_ec_shape,
    )
    .unwrap();
    let remote_data_pg = remote_primary.get_pg(written.data_pg_id).unwrap();
    for written_shard in &written.written_shards {
        assert!(
            matches!(
                remote_data_pg.validate_written_shard_ack(&written_shard.key, written_shard.ack),
                Err(StoreError::NotFound)
            ),
            "old-primary Unix direct PUT must delete retained data-PG ack row for shard {}",
            written_shard.key
        );
    }
}

#[test]
fn control_plane_peering_unix_copy_object_destination_old_primary_cleans_remote_staging() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
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
                            .join("control-plane-sockets")
                            .join(format!("copy-node-{}.sock", node_id.as_u32()))
                            .to_string_lossy()
                            .into_owned(),
                    )
                })
                .collect(),
            pg_ids.iter().copied().map(PgId::new).collect(),
        )
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    let source_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), source_epoch)
                .unwrap();
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                PgState::Active,
            )
        })
        .collect::<Vec<_>>();

    let source_storage_configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("remote-peering-stale-copy-object")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();

    let source_map = Arc::new(
        LocalClusterMap::open_frontend_with_configs_and_pg_routes(
            NodeId::new(0),
            source_storage_configs.clone(),
            &pg_ids,
            ec_shape,
            source_epoch,
            source_routes.iter().map(LocalPgRoute::from),
        )
        .unwrap(),
    );
    let (bucket, dst_key, dst_object_pg, _dst_data_pg) = {
        let topology = source_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let source_key = {
        let topology = source_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let source_pg = pg_ids
            .iter()
            .copied()
            .find(|pg_id| *pg_id != dst_object_pg)
            .unwrap();
        key_for_object_pg(topology, &bucket, source_pg, "copy-source-")
    };
    let source_cluster = crate::StorageCluster::from_local_map(Arc::clone(&source_map)).unwrap();
    create_test_bucket(&source_cluster, &bucket);
    let source_payload = b"control-plane peering stale unix copy source payload";
    let source_segment = write_committed_direct_segment_for_with_okh(
        &source_cluster,
        &bucket,
        &source_key,
        [0xcb; 16],
        source_payload,
    );

    let reservation_id =
        crate::SessionId::try_from("70707070707070707070707070707070".to_string()).unwrap();
    let generation_id = source_cluster
        .reserve_put_object_generation(&bucket, &dst_key, &reservation_id)
        .unwrap();
    let before_dst_object_pg_proof = source_cluster
        .test_object_pg_metadata_proof(&bucket, &dst_key)
        .unwrap();
    let dst_segment_okh = [0xcc; 16];
    let written = source_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &dst_key,
            generation_id,
            0,
            &dst_segment_okh,
            &source_segment.payload,
        )
        .unwrap();
    source_cluster
        .test_register_payload_shard_acks(written.data_pg_id, &written.written_shards)
        .unwrap();
    let source_data_pg_primary = source_routes
        .iter()
        .find(|route| route.pg_id() == PgId::new(written.data_pg_id))
        .unwrap()
        .primary_node_id();
    let bucket_write_reservation = source_cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "control-plane-peering-stale-unix-copy-object",
            Some(dst_key.as_str()),
        )
        .unwrap();
    let commit_req = direct_put_commit_req_with_bucket_write_proof(
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &dst_key,
            reservation_id,
            generation_id,
            payload: &source_segment.payload,
            segment_okh: dst_segment_okh,
            written: &written,
        },
        crate::metadata_command::BucketWriteReservationProof::from(
            &bucket_write_reservation.record,
        ),
    );
    drop(source_cluster);
    drop(source_map);

    authority
        .set_pg_acting_set(
            PgId::new(dst_object_pg),
            vec![NodeId::new(1), NodeId::new(2)],
        )
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    let current_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), current_epoch)
                .unwrap();
            let state = if *pg_id == dst_object_pg {
                PgState::Peering
            } else {
                PgState::Active
            };
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                state,
            )
        })
        .collect::<Vec<_>>();
    let historical_pg_routes = source_routes
        .iter()
        .map(|route| StorageNodePgRoute {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            acting_set: route.acting_set().to_vec(),
        })
        .collect::<Vec<_>>();
    let current_pg_routes = current_routes
        .iter()
        .map(|route| StorageNodePgRoute {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            acting_set: route.acting_set().to_vec(),
        })
        .collect::<Vec<_>>();
    let mut server_configs = Vec::new();
    let mut client_configs = Vec::new();
    for node_config in &source_storage_configs {
        let socket_path = tmp.path().join("sockets").join(format!(
            "peering-stale-copy-object-node-{}.sock",
            node_config.node_id.as_u32()
        ));
        private_socket_dir(socket_path.parent().unwrap());
        server_configs.push(StorageNodeProcessConfig {
            node_id: node_config.node_id,
            cluster_epoch: current_epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: node_config.data_dir.clone(),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.to_vec(),
            socket_path: socket_path.clone(),
            pg_routes: current_pg_routes.clone(),
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: historical_pg_routes.clone(),
        });
        client_configs.push(LocalUnixStorageNodeClientConfig::new(
            node_config.node_id,
            socket_path,
        ));
    }
    let mut _server_guards = Vec::new();
    for config in server_configs.iter().cloned() {
        let server = StorageNodeServer::bind(config).unwrap();
        _server_guards.push(spawn_storage_node_server(server));
    }

    let frontend_configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("frontend-peering-stale-copy-object")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let mut current_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        frontend_configs,
        &pg_ids,
        ec_shape,
        current_epoch,
        current_routes.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    current_map.test_install_historical_pg_routes(source_routes.clone());
    current_map
        .install_unix_storage_node_clients(client_configs)
        .unwrap();
    let current_map = Arc::new(current_map);
    assert_eq!(
        current_map
            .pg_route(PgId::new(dst_object_pg))
            .unwrap()
            .state(),
        PgState::Peering,
        "control-plane acting-set change should put the destination object PG into Peering"
    );
    assert!(
        !current_map
            .pg_route(PgId::new(dst_object_pg))
            .unwrap()
            .acting_set()
            .contains(&NodeId::new(0)),
        "the old source primary should no longer be in the destination acting set"
    );
    let old_primary_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&current_map),
        source_epoch,
    )
    .unwrap();

    let err = old_primary_cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == dst_object_pg
                && operation_epoch == source_epoch
                && observed_current_epoch == current_epoch
        ),
        "old-primary Unix CopyObject destination commit should fail closed after control-plane Peering transition, got {err:?}"
    );

    let bucket_pg = current_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .bucket_pg_for(&bucket);
    let source_object_pg = current_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .object_pg_for(&bucket, &source_key);
    for config in &server_configs {
        let remote_dst = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &[dst_object_pg],
            config.default_ec_shape,
        )
        .unwrap();
        let dst_pg = remote_dst.get_pg(dst_object_pg).unwrap();
        let state = dst_pg.metadata_command_replica_state().unwrap();
        let proof = crate::control_plane::PgMetadataProof {
            applied_log_index: state.applied_log_index,
            applied_log_hash: state.applied_log_hash,
            state_digest: state.state_digest,
        };
        assert_eq!(
            proof, before_dst_object_pg_proof,
            "old-primary Unix CopyObject destination must not append an object-PG command on node {:?}",
            config.node_id
        );
        assert!(
            matches!(
                crate::PgMetadataStore::get_object_meta(&*dst_pg, &bucket, &dst_key),
                Err(crate::MetadataError::ObjectNotFound)
            ),
            "old-primary Unix CopyObject destination must not publish destination metadata on node {:?}",
            config.node_id
        );
        assert!(
            dst_pg
                .pending_metadata_command_envelope(config.node_id.as_u32(), source_epoch)
                .unwrap()
                .is_none(),
            "old-primary Unix CopyObject destination must not leave a source-epoch pending command on node {:?}",
            config.node_id
        );
        assert!(
            dst_pg
                .pending_metadata_command_envelope(config.node_id.as_u32(), current_epoch)
                .unwrap()
                .is_none(),
            "old-primary Unix CopyObject destination must not leave a current-epoch pending command on node {:?}",
            config.node_id
        );
        let remote_source = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &[source_object_pg],
            config.default_ec_shape,
        )
        .unwrap();
        let source_pg = remote_source.get_pg(source_object_pg).unwrap();
        let stored =
            crate::PgMetadataStore::get_object_meta(&*source_pg, &bucket, &source_key).unwrap();
        let live = stored.as_live().unwrap();
        assert_eq!(
            live.generation_id, source_segment.generation_id,
            "failed Unix CopyObject destination commit must preserve source object on node {:?}",
            config.node_id
        );
        let remote_bucket = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &[bucket_pg],
            config.default_ec_shape,
        )
        .unwrap();
        let bucket_pg_store = remote_bucket.get_pg(bucket_pg).unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_reservations(
                &*bucket_pg_store,
                &bucket,
            )
            .unwrap()
            .is_empty(),
            "old-primary Unix CopyObject destination must release bucket write reservations on node {:?}",
            config.node_id
        );
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &[written.data_pg_id],
            config.default_ec_shape,
        )
        .unwrap();
        for written_shard in &written.written_shards {
            assert!(
                matches!(
                    remote.read_shard_file(written.data_pg_id, &written_shard.key),
                    Err(StoreError::NotFound)
                ),
                "old-primary Unix CopyObject destination must delete copied shard {} from node {:?}",
                written_shard.key,
                config.node_id
            );
        }
    }
    let source_primary_config = server_configs
        .iter()
        .find(|config| config.node_id == source_data_pg_primary)
        .unwrap();
    let remote_primary = SharedStorageNode::open_with_default_ec_shape(
        &source_primary_config.data_dir,
        &[written.data_pg_id],
        source_primary_config.default_ec_shape,
    )
    .unwrap();
    let remote_data_pg = remote_primary.get_pg(written.data_pg_id).unwrap();
    for written_shard in &written.written_shards {
        assert!(
            matches!(
                remote_data_pg.validate_written_shard_ack(&written_shard.key, written_shard.ack),
                Err(StoreError::NotFound)
            ),
            "old-primary Unix CopyObject destination must delete retained data-PG ack row for shard {}",
            written_shard.key
        );
    }
}

#[test]
fn control_plane_peering_unix_object_delete_old_primary_fails_closed_without_remote_mutation() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
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
                            .join("control-plane-sockets")
                            .join(format!("delete-node-{}.sock", node_id.as_u32()))
                            .to_string_lossy()
                            .into_owned(),
                    )
                })
                .collect(),
            pg_ids.iter().copied().map(PgId::new).collect(),
        )
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    let source_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), source_epoch)
                .unwrap();
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                PgState::Active,
            )
        })
        .collect::<Vec<_>>();

    let source_storage_configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("remote-peering-stale-delete")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let source_map = Arc::new(
        LocalClusterMap::open_frontend_with_configs_and_pg_routes(
            NodeId::new(0),
            source_storage_configs.clone(),
            &pg_ids,
            ec_shape,
            source_epoch,
            source_routes.iter().map(LocalPgRoute::from),
        )
        .unwrap(),
    );
    let (bucket, key, object_pg, _data_pg) = {
        let topology = source_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let source_cluster = crate::StorageCluster::from_local_map(Arc::clone(&source_map)).unwrap();
    let committed = write_committed_direct_segment_for(
        &source_cluster,
        &bucket,
        &key,
        b"control-plane peering stale unix delete",
    );
    let before_object_pg_proof = source_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    drop(source_cluster);
    drop(source_map);

    authority
        .set_pg_acting_set(PgId::new(object_pg), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    let current_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), current_epoch)
                .unwrap();
            let state = if *pg_id == object_pg {
                PgState::Peering
            } else {
                PgState::Active
            };
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                state,
            )
        })
        .collect::<Vec<_>>();
    let historical_pg_routes = source_routes
        .iter()
        .map(|route| StorageNodePgRoute {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            acting_set: route.acting_set().to_vec(),
        })
        .collect::<Vec<_>>();
    let current_pg_routes = current_routes
        .iter()
        .map(|route| StorageNodePgRoute {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            acting_set: route.acting_set().to_vec(),
        })
        .collect::<Vec<_>>();
    let mut server_configs = Vec::new();
    let mut client_configs = Vec::new();
    for node_config in &source_storage_configs {
        let socket_path = tmp.path().join("sockets").join(format!(
            "peering-stale-delete-node-{}.sock",
            node_config.node_id.as_u32()
        ));
        private_socket_dir(socket_path.parent().unwrap());
        server_configs.push(StorageNodeProcessConfig {
            node_id: node_config.node_id,
            cluster_epoch: current_epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: node_config.data_dir.clone(),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.to_vec(),
            socket_path: socket_path.clone(),
            pg_routes: current_pg_routes.clone(),
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: historical_pg_routes.clone(),
        });
        client_configs.push(LocalUnixStorageNodeClientConfig::new(
            node_config.node_id,
            socket_path,
        ));
    }
    let mut _server_guards = Vec::new();
    for config in server_configs.iter().cloned() {
        let server = StorageNodeServer::bind(config).unwrap();
        _server_guards.push(spawn_storage_node_server(server));
    }

    let frontend_configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("frontend-peering-stale-delete")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let mut current_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        frontend_configs,
        &pg_ids,
        ec_shape,
        current_epoch,
        current_routes.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    current_map.test_install_historical_pg_routes(source_routes);
    current_map
        .install_unix_storage_node_clients(client_configs)
        .unwrap();
    let current_map = Arc::new(current_map);
    assert_eq!(
        current_map.pg_route(PgId::new(object_pg)).unwrap().state(),
        PgState::Peering,
        "control-plane acting-set change should put the object PG into Peering"
    );
    assert!(
        !current_map
            .pg_route(PgId::new(object_pg))
            .unwrap()
            .acting_set()
            .contains(&NodeId::new(0)),
        "the old source primary should no longer be in the current acting set"
    );

    let old_primary_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&current_map),
        source_epoch,
    )
    .unwrap();
    let err = old_primary_cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == source_epoch
                && observed_current_epoch == current_epoch
        ),
        "old-primary Unix object delete should fail closed after control-plane Peering transition, got {err:?}"
    );

    let bucket_pg = current_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .bucket_pg_for(&bucket);
    for config in &server_configs {
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let object_pg_store = remote.get_pg(object_pg).unwrap();
        let state = object_pg_store.metadata_command_replica_state().unwrap();
        let proof = crate::control_plane::PgMetadataProof {
            applied_log_index: state.applied_log_index,
            applied_log_hash: state.applied_log_hash,
            state_digest: state.state_digest,
        };
        assert_eq!(
            proof, before_object_pg_proof,
            "old-primary Unix object delete must not append an object-PG command on node {:?}",
            config.node_id
        );
        let stored = crate::PgMetadataStore::get_object_meta(&*object_pg_store, &bucket, &key)
            .unwrap()
            .into_live()
            .unwrap();
        assert_eq!(
            stored.generation_id, committed.generation_id,
            "old-primary Unix object delete must preserve the live object on node {:?}",
            config.node_id
        );
        assert!(
            !crate::PgMetadataStore::payload_reclaim_exists(
                &*object_pg_store,
                &bucket,
                &key,
                committed.generation_id
            )
            .unwrap(),
            "old-primary Unix object delete must not publish reclaim metadata on node {:?}",
            config.node_id
        );
        assert!(
            object_pg_store
                .pending_metadata_command_envelope(config.node_id.as_u32(), source_epoch)
                .unwrap()
                .is_none(),
            "old-primary Unix object delete must not leave a source-epoch pending command on node {:?}",
            config.node_id
        );
        assert!(
            object_pg_store
                .pending_metadata_command_envelope(config.node_id.as_u32(), current_epoch)
                .unwrap()
                .is_none(),
            "old-primary Unix object delete must not leave a current-epoch pending command on node {:?}",
            config.node_id
        );
        let bucket_pg_store = remote.get_pg(bucket_pg).unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg_store, &bucket,)
                .unwrap()
                .is_empty(),
            "old-primary Unix object delete must leave no bucket-write reservation on node {:?}",
            config.node_id
        );
    }
}

#[test]
fn control_plane_peering_unix_object_metadata_old_primary_fails_closed_without_remote_mutation() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
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
                            .join("control-plane-sockets")
                            .join(format!("metadata-node-{}.sock", node_id.as_u32()))
                            .to_string_lossy()
                            .into_owned(),
                    )
                })
                .collect(),
            pg_ids.iter().copied().map(PgId::new).collect(),
        )
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    let source_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), source_epoch)
                .unwrap();
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                PgState::Active,
            )
        })
        .collect::<Vec<_>>();

    let source_storage_configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("remote-peering-stale-metadata")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let source_map = Arc::new(
        LocalClusterMap::open_frontend_with_configs_and_pg_routes(
            NodeId::new(0),
            source_storage_configs.clone(),
            &pg_ids,
            ec_shape,
            source_epoch,
            source_routes.iter().map(LocalPgRoute::from),
        )
        .unwrap(),
    );
    let (bucket, key, object_pg, _data_pg) = {
        let topology = source_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let source_cluster = crate::StorageCluster::from_local_map(Arc::clone(&source_map)).unwrap();
    let committed = write_committed_direct_segment_for(
        &source_cluster,
        &bucket,
        &key,
        b"control-plane peering stale unix metadata",
    );
    let before_object_pg_proof = source_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    drop(source_cluster);
    drop(source_map);

    authority
        .set_pg_acting_set(PgId::new(object_pg), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    let current_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), current_epoch)
                .unwrap();
            let state = if *pg_id == object_pg {
                PgState::Peering
            } else {
                PgState::Active
            };
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                state,
            )
        })
        .collect::<Vec<_>>();
    let historical_pg_routes = source_routes
        .iter()
        .map(|route| StorageNodePgRoute {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            acting_set: route.acting_set().to_vec(),
        })
        .collect::<Vec<_>>();
    let current_pg_routes = current_routes
        .iter()
        .map(|route| StorageNodePgRoute {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            acting_set: route.acting_set().to_vec(),
        })
        .collect::<Vec<_>>();
    let mut server_configs = Vec::new();
    let mut client_configs = Vec::new();
    for node_config in &source_storage_configs {
        let socket_path = tmp.path().join("sockets").join(format!(
            "peering-stale-metadata-node-{}.sock",
            node_config.node_id.as_u32()
        ));
        private_socket_dir(socket_path.parent().unwrap());
        server_configs.push(StorageNodeProcessConfig {
            node_id: node_config.node_id,
            cluster_epoch: current_epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: node_config.data_dir.clone(),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.to_vec(),
            socket_path: socket_path.clone(),
            pg_routes: current_pg_routes.clone(),
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: historical_pg_routes.clone(),
        });
        client_configs.push(LocalUnixStorageNodeClientConfig::new(
            node_config.node_id,
            socket_path,
        ));
    }
    let mut _server_guards = Vec::new();
    for config in server_configs.iter().cloned() {
        let server = StorageNodeServer::bind(config).unwrap();
        _server_guards.push(spawn_storage_node_server(server));
    }

    let frontend_configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("frontend-peering-stale-metadata")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let mut current_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        frontend_configs,
        &pg_ids,
        ec_shape,
        current_epoch,
        current_routes.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    current_map.test_install_historical_pg_routes(source_routes);
    current_map
        .install_unix_storage_node_clients(client_configs)
        .unwrap();
    let current_map = Arc::new(current_map);
    assert_eq!(
        current_map.pg_route(PgId::new(object_pg)).unwrap().state(),
        PgState::Peering,
        "control-plane acting-set change should put the object PG into Peering"
    );
    assert!(
        !current_map
            .pg_route(PgId::new(object_pg))
            .unwrap()
            .acting_set()
            .contains(&NodeId::new(0)),
        "the old source primary should no longer be in the current acting set"
    );

    let old_primary_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&current_map),
        source_epoch,
    )
    .unwrap();
    let tags =
        "<Tagging><TagSet><Tag><Key>stale</Key><Value>ignored</Value></Tag></TagSet></Tagging>";
    let err = old_primary_cluster
        .put_object_tags_if(&bucket, &key, None, tags, |stored| {
            Ok::<_, ()>(stored.version_id())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == source_epoch
                && observed_current_epoch == current_epoch
        ),
        "old-primary Unix object metadata update should fail closed after control-plane Peering transition, got {err:?}"
    );

    let bucket_pg = current_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .bucket_pg_for(&bucket);
    for config in &server_configs {
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let object_pg_store = remote.get_pg(object_pg).unwrap();
        let state = object_pg_store.metadata_command_replica_state().unwrap();
        let proof = crate::control_plane::PgMetadataProof {
            applied_log_index: state.applied_log_index,
            applied_log_hash: state.applied_log_hash,
            state_digest: state.state_digest,
        };
        assert_eq!(
            proof, before_object_pg_proof,
            "old-primary Unix object metadata update must not append an object-PG command on node {:?}",
            config.node_id
        );
        let stored = crate::PgMetadataStore::get_object_meta(&*object_pg_store, &bucket, &key)
            .unwrap()
            .into_live()
            .unwrap();
        assert_eq!(
            stored.generation_id, committed.generation_id,
            "old-primary Unix object metadata update must preserve the live object on node {:?}",
            config.node_id
        );
        assert_eq!(
            crate::PgMetadataStore::get_object_tags(
                &*object_pg_store,
                &bucket,
                &key,
                crate::VersionId::Null,
            )
            .unwrap(),
            None,
            "old-primary Unix object metadata update must not publish tags on node {:?}",
            config.node_id
        );
        assert!(
            object_pg_store
                .pending_metadata_command_envelope(config.node_id.as_u32(), source_epoch)
                .unwrap()
                .is_none(),
            "old-primary Unix object metadata update must not leave a source-epoch pending command on node {:?}",
            config.node_id
        );
        assert!(
            object_pg_store
                .pending_metadata_command_envelope(config.node_id.as_u32(), current_epoch)
                .unwrap()
                .is_none(),
            "old-primary Unix object metadata update must not leave a current-epoch pending command on node {:?}",
            config.node_id
        );
        let bucket_pg_store = remote.get_pg(bucket_pg).unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg_store, &bucket,)
                .unwrap()
                .is_empty(),
            "old-primary Unix object metadata update must leave no bucket-write reservation on node {:?}",
            config.node_id
        );
    }
}

#[test]
fn control_plane_peering_unix_multipart_completion_old_primary_fails_closed_without_remote_mutation(
) {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
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
                            .join("control-plane-sockets")
                            .join(format!("multipart-node-{}.sock", node_id.as_u32()))
                            .to_string_lossy()
                            .into_owned(),
                    )
                })
                .collect(),
            pg_ids.iter().copied().map(PgId::new).collect(),
        )
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    let source_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), source_epoch)
                .unwrap();
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                PgState::Active,
            )
        })
        .collect::<Vec<_>>();

    let source_storage_configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("remote-peering-stale-multipart-completion")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let source_map = Arc::new(
        LocalClusterMap::open_frontend_with_configs_and_pg_routes(
            NodeId::new(0),
            source_storage_configs.clone(),
            &pg_ids,
            ec_shape,
            source_epoch,
            source_routes.iter().map(LocalPgRoute::from),
        )
        .unwrap(),
    );
    let (bucket, key, object_pg, _data_pg) = {
        let topology = source_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let source_cluster = crate::StorageCluster::from_local_map(Arc::clone(&source_map)).unwrap();
    create_test_bucket(&source_cluster, &bucket);
    let (req, _) =
        seed_streamed_multipart_completion(&source_cluster, &bucket, &key, "unixpeeringcomplete");
    let before_object_pg_proof = source_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    drop(source_cluster);
    drop(source_map);

    authority
        .set_pg_acting_set(PgId::new(object_pg), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    let current_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), current_epoch)
                .unwrap();
            let state = if *pg_id == object_pg {
                PgState::Peering
            } else {
                PgState::Active
            };
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                state,
            )
        })
        .collect::<Vec<_>>();
    let historical_pg_routes = source_routes
        .iter()
        .map(|route| StorageNodePgRoute {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            acting_set: route.acting_set().to_vec(),
        })
        .collect::<Vec<_>>();
    let current_pg_routes = current_routes
        .iter()
        .map(|route| StorageNodePgRoute {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            acting_set: route.acting_set().to_vec(),
        })
        .collect::<Vec<_>>();
    let mut server_configs = Vec::new();
    let mut client_configs = Vec::new();
    for node_config in &source_storage_configs {
        let socket_path = tmp.path().join("sockets").join(format!(
            "peering-stale-multipart-completion-node-{}.sock",
            node_config.node_id.as_u32()
        ));
        private_socket_dir(socket_path.parent().unwrap());
        server_configs.push(StorageNodeProcessConfig {
            node_id: node_config.node_id,
            cluster_epoch: current_epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: node_config.data_dir.clone(),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.to_vec(),
            socket_path: socket_path.clone(),
            pg_routes: current_pg_routes.clone(),
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: historical_pg_routes.clone(),
        });
        client_configs.push(LocalUnixStorageNodeClientConfig::new(
            node_config.node_id,
            socket_path,
        ));
    }
    let mut _server_guards = Vec::new();
    for config in server_configs.iter().cloned() {
        let server = StorageNodeServer::bind(config).unwrap();
        _server_guards.push(spawn_storage_node_server(server));
    }

    let frontend_configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("frontend-peering-stale-multipart-completion")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let mut current_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        frontend_configs,
        &pg_ids,
        ec_shape,
        current_epoch,
        current_routes.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    current_map.test_install_historical_pg_routes(source_routes);
    current_map
        .install_unix_storage_node_clients(client_configs)
        .unwrap();
    let current_map = Arc::new(current_map);
    assert_eq!(
        current_map.pg_route(PgId::new(object_pg)).unwrap().state(),
        PgState::Peering,
        "control-plane acting-set change should put the object PG into Peering"
    );
    assert!(
        !current_map
            .pg_route(PgId::new(object_pg))
            .unwrap()
            .acting_set()
            .contains(&NodeId::new(0)),
        "the old source primary should no longer be in the current acting set"
    );

    let old_primary_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&current_map),
        source_epoch,
    )
    .unwrap();
    let err = old_primary_cluster
        .complete_multipart_upload_commit_serialized(req.clone())
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == source_epoch
                && observed_current_epoch == current_epoch
        ),
        "old-primary Unix multipart completion should fail closed after control-plane Peering transition, got {err:?}"
    );

    let bucket_pg = current_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .bucket_pg_for(&bucket);
    for config in &server_configs {
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let object_pg_store = remote.get_pg(object_pg).unwrap();
        let state = object_pg_store.metadata_command_replica_state().unwrap();
        let proof = crate::control_plane::PgMetadataProof {
            applied_log_index: state.applied_log_index,
            applied_log_hash: state.applied_log_hash,
            state_digest: state.state_digest,
        };
        assert_eq!(
            proof, before_object_pg_proof,
            "old-primary Unix multipart completion must not append an object-PG command on node {:?}",
            config.node_id
        );
        assert!(
            matches!(
                crate::PgMetadataStore::get_object_meta(&*object_pg_store, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ),
            "old-primary Unix multipart completion must not publish object metadata on node {:?}",
            config.node_id
        );
        assert!(
            crate::PgMetadataStore::get_multipart_upload(&*object_pg_store, &req.upload_id).is_ok(),
            "old-primary Unix multipart completion must leave upload in progress on node {:?}",
            config.node_id
        );
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part(
                &*object_pg_store,
                &req.upload_id,
                req.part_records[0].part_number,
            )
            .unwrap(),
            req.part_records[0],
            "old-primary Unix multipart completion must preserve selected part row on node {:?}",
            config.node_id
        );
        assert_eq!(
            crate::PgMetadataStore::get_all_multipart_part_segments_for_upload(
                &*object_pg_store,
                &req.upload_id,
            )
            .unwrap(),
            req.selected_streaming_segments,
            "old-primary Unix multipart completion must preserve staged segment rows on node {:?}",
            config.node_id
        );
        assert!(
            object_pg_store
                .pending_metadata_command_envelope(config.node_id.as_u32(), source_epoch)
                .unwrap()
                .is_none(),
            "old-primary Unix multipart completion must not leave a source-epoch pending command on node {:?}",
            config.node_id
        );
        assert!(
            object_pg_store
                .pending_metadata_command_envelope(config.node_id.as_u32(), current_epoch)
                .unwrap()
                .is_none(),
            "old-primary Unix multipart completion must not leave a current-epoch pending command on node {:?}",
            config.node_id
        );

        let bucket_pg_store = remote.get_pg(bucket_pg).unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_reservations(
                &*bucket_pg_store,
                &bucket,
            )
            .unwrap()
            .is_empty(),
            "old-primary Unix multipart completion must leave no bucket-write reservation on node {:?}",
            config.node_id
        );
    }
}

#[test]
fn control_plane_peering_unix_multipart_abort_old_primary_fails_closed_without_remote_mutation() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
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
                            .join("control-plane-sockets")
                            .join(format!("multipart-abort-node-{}.sock", node_id.as_u32()))
                            .to_string_lossy()
                            .into_owned(),
                    )
                })
                .collect(),
            pg_ids.iter().copied().map(PgId::new).collect(),
        )
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    let source_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), source_epoch)
                .unwrap();
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                PgState::Active,
            )
        })
        .collect::<Vec<_>>();

    let source_storage_configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("remote-peering-stale-multipart-abort")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let source_map = Arc::new(
        LocalClusterMap::open_frontend_with_configs_and_pg_routes(
            NodeId::new(0),
            source_storage_configs.clone(),
            &pg_ids,
            ec_shape,
            source_epoch,
            source_routes.iter().map(LocalPgRoute::from),
        )
        .unwrap(),
    );
    let (bucket, key, object_pg) = {
        let topology = source_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "unix-multipart-abort-peering-");
        let object_pg = 2;
        let key = key_for_object_pg(topology, &bucket, object_pg, "object-");
        (bucket, key, object_pg)
    };
    let source_cluster = crate::StorageCluster::from_local_map(Arc::clone(&source_map)).unwrap();
    create_test_bucket(&source_cluster, &bucket);
    let (req, _) =
        seed_streamed_multipart_completion(&source_cluster, &bucket, &key, "unixpeeringabort");
    let before_object_pg_proof = source_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    drop(source_cluster);
    drop(source_map);

    authority
        .set_pg_acting_set(PgId::new(object_pg), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    let current_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), current_epoch)
                .unwrap();
            let state = if *pg_id == object_pg {
                PgState::Peering
            } else {
                PgState::Active
            };
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                state,
            )
        })
        .collect::<Vec<_>>();
    let historical_pg_routes = source_routes
        .iter()
        .map(|route| StorageNodePgRoute {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            acting_set: route.acting_set().to_vec(),
        })
        .collect::<Vec<_>>();
    let current_pg_routes = current_routes
        .iter()
        .map(|route| StorageNodePgRoute {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            acting_set: route.acting_set().to_vec(),
        })
        .collect::<Vec<_>>();
    let mut server_configs = Vec::new();
    let mut client_configs = Vec::new();
    for node_config in &source_storage_configs {
        let socket_path = tmp.path().join("sockets").join(format!(
            "peering-stale-multipart-abort-node-{}.sock",
            node_config.node_id.as_u32()
        ));
        private_socket_dir(socket_path.parent().unwrap());
        server_configs.push(StorageNodeProcessConfig {
            node_id: node_config.node_id,
            cluster_epoch: current_epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: node_config.data_dir.clone(),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.to_vec(),
            socket_path: socket_path.clone(),
            pg_routes: current_pg_routes.clone(),
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: historical_pg_routes.clone(),
        });
        client_configs.push(LocalUnixStorageNodeClientConfig::new(
            node_config.node_id,
            socket_path,
        ));
    }
    let mut _server_guards = Vec::new();
    for config in server_configs.iter().cloned() {
        let server = StorageNodeServer::bind(config).unwrap();
        _server_guards.push(spawn_storage_node_server(server));
    }

    let frontend_configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("frontend-peering-stale-multipart-abort")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let mut current_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        frontend_configs,
        &pg_ids,
        ec_shape,
        current_epoch,
        current_routes.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    current_map.test_install_historical_pg_routes(source_routes);
    current_map
        .install_unix_storage_node_clients(client_configs)
        .unwrap();
    let current_map = Arc::new(current_map);
    assert_eq!(
        current_map.pg_route(PgId::new(object_pg)).unwrap().state(),
        PgState::Peering,
        "control-plane acting-set change should put the object PG into Peering"
    );
    assert!(
        !current_map
            .pg_route(PgId::new(object_pg))
            .unwrap()
            .acting_set()
            .contains(&NodeId::new(0)),
        "the old source primary should no longer be in the current acting set"
    );

    let old_primary_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&current_map),
        source_epoch,
    )
    .unwrap();
    let err = old_primary_cluster
        .abort_multipart_upload(&bucket, &key, &req.upload_id)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == source_epoch
                && observed_current_epoch == current_epoch
        ),
        "old-primary Unix multipart abort should fail closed after control-plane Peering transition, got {err:?}"
    );

    let bucket_pg = current_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .bucket_pg_for(&bucket);
    for config in &server_configs {
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let object_pg_store = remote.get_pg(object_pg).unwrap();
        let state = object_pg_store.metadata_command_replica_state().unwrap();
        let proof = crate::control_plane::PgMetadataProof {
            applied_log_index: state.applied_log_index,
            applied_log_hash: state.applied_log_hash,
            state_digest: state.state_digest,
        };
        assert_eq!(
            proof, before_object_pg_proof,
            "old-primary Unix multipart abort must not append an object-PG command on node {:?}",
            config.node_id
        );
        assert!(
            crate::PgMetadataStore::get_multipart_upload(&*object_pg_store, &req.upload_id).is_ok(),
            "old-primary Unix multipart abort must leave upload in progress on node {:?}",
            config.node_id
        );
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part(
                &*object_pg_store,
                &req.upload_id,
                req.part_records[0].part_number,
            )
            .unwrap(),
            req.part_records[0],
            "old-primary Unix multipart abort must preserve selected part row on node {:?}",
            config.node_id
        );
        assert_eq!(
            crate::PgMetadataStore::get_all_multipart_part_segments_for_upload(
                &*object_pg_store,
                &req.upload_id,
            )
            .unwrap(),
            req.selected_streaming_segments,
            "old-primary Unix multipart abort must preserve staged segment rows on node {:?}",
            config.node_id
        );
        assert!(
            object_pg_store
                .pending_metadata_command_envelope(config.node_id.as_u32(), source_epoch)
                .unwrap()
                .is_none(),
            "old-primary Unix multipart abort must not leave a source-epoch pending command on node {:?}",
            config.node_id
        );
        assert!(
            object_pg_store
                .pending_metadata_command_envelope(config.node_id.as_u32(), current_epoch)
                .unwrap()
                .is_none(),
            "old-primary Unix multipart abort must not leave a current-epoch pending command on node {:?}",
            config.node_id
        );

        let bucket_pg_store = remote.get_pg(bucket_pg).unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg_store, &bucket,)
                .unwrap()
                .is_empty(),
            "old-primary Unix multipart abort must leave no bucket-write reservation on node {:?}",
            config.node_id
        );
    }
}

#[test]
fn control_plane_peering_unix_upload_part_session_old_primary_fails_closed_without_remote_mutation()
{
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
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
                            .join("control-plane-sockets")
                            .join(format!("upload-part-node-{}.sock", node_id.as_u32()))
                            .to_string_lossy()
                            .into_owned(),
                    )
                })
                .collect(),
            pg_ids.iter().copied().map(PgId::new).collect(),
        )
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    let source_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), source_epoch)
                .unwrap();
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                PgState::Active,
            )
        })
        .collect::<Vec<_>>();

    let source_storage_configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("remote-peering-stale-upload-part-session")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let source_map = Arc::new(
        LocalClusterMap::open_frontend_with_configs_and_pg_routes(
            NodeId::new(0),
            source_storage_configs.clone(),
            &pg_ids,
            ec_shape,
            source_epoch,
            source_routes.iter().map(LocalPgRoute::from),
        )
        .unwrap(),
    );
    let (bucket, key, object_pg, _data_pg) = {
        let topology = source_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let source_cluster = crate::StorageCluster::from_local_map(Arc::clone(&source_map)).unwrap();
    create_test_bucket(&source_cluster, &bucket);
    let upload_id = upload_id_from_label("unixpeeringuppart");
    let create = crate::CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: crate::OwnerIdentity::from_principal("initiator"),
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        object_lock: crate::ObjectLockState::default(),
        checksum: None,
        encryption: crate::ObjectEncryption::None,
    };
    source_cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    let upload = source_cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    let before_object_pg_proof = source_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    drop(source_cluster);
    drop(source_map);

    authority
        .set_pg_acting_set(PgId::new(object_pg), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    let current_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), current_epoch)
                .unwrap();
            let state = if *pg_id == object_pg {
                PgState::Peering
            } else {
                PgState::Active
            };
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                state,
            )
        })
        .collect::<Vec<_>>();
    let historical_pg_routes = source_routes
        .iter()
        .map(|route| StorageNodePgRoute {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            acting_set: route.acting_set().to_vec(),
        })
        .collect::<Vec<_>>();
    let current_pg_routes = current_routes
        .iter()
        .map(|route| StorageNodePgRoute {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            acting_set: route.acting_set().to_vec(),
        })
        .collect::<Vec<_>>();
    let mut server_configs = Vec::new();
    let mut client_configs = Vec::new();
    for node_config in &source_storage_configs {
        let socket_path = tmp.path().join("sockets").join(format!(
            "peering-stale-upload-part-session-node-{}.sock",
            node_config.node_id.as_u32()
        ));
        private_socket_dir(socket_path.parent().unwrap());
        server_configs.push(StorageNodeProcessConfig {
            node_id: node_config.node_id,
            cluster_epoch: current_epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: node_config.data_dir.clone(),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.to_vec(),
            socket_path: socket_path.clone(),
            pg_routes: current_pg_routes.clone(),
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: historical_pg_routes.clone(),
        });
        client_configs.push(LocalUnixStorageNodeClientConfig::new(
            node_config.node_id,
            socket_path,
        ));
    }
    let mut _server_guards = Vec::new();
    for config in server_configs.iter().cloned() {
        let server = StorageNodeServer::bind(config).unwrap();
        _server_guards.push(spawn_storage_node_server(server));
    }

    let frontend_configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("frontend-peering-stale-upload-part-session")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let mut current_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        frontend_configs,
        &pg_ids,
        ec_shape,
        current_epoch,
        current_routes.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    current_map.test_install_historical_pg_routes(source_routes);
    current_map
        .install_unix_storage_node_clients(client_configs)
        .unwrap();
    let current_map = Arc::new(current_map);
    assert_eq!(
        current_map.pg_route(PgId::new(object_pg)).unwrap().state(),
        PgState::Peering,
        "control-plane acting-set change should put the object PG into Peering"
    );
    assert!(
        !current_map
            .pg_route(PgId::new(object_pg))
            .unwrap()
            .acting_set()
            .contains(&NodeId::new(0)),
        "the old source primary should no longer be in the current acting set"
    );

    let old_primary_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&current_map),
        source_epoch,
    )
    .unwrap();
    let session_id = crate::tests::stream_session_id("unixpeeruppart");
    let err = old_primary_cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            1,
            &session_id,
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == source_epoch
                && observed_current_epoch == current_epoch
        ),
        "old-primary Unix UploadPart session create should fail closed after control-plane Peering transition, got {err:?}"
    );

    let bucket_pg = current_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .bucket_pg_for(&bucket);
    for config in &server_configs {
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let object_pg_store = remote.get_pg(object_pg).unwrap();
        let state = object_pg_store.metadata_command_replica_state().unwrap();
        let proof = crate::control_plane::PgMetadataProof {
            applied_log_index: state.applied_log_index,
            applied_log_hash: state.applied_log_hash,
            state_digest: state.state_digest,
        };
        assert_eq!(
            proof, before_object_pg_proof,
            "old-primary Unix UploadPart session create must not append an object-PG command on node {:?}",
            config.node_id
        );
        assert!(
            crate::PgMetadataStore::get_multipart_upload(&*object_pg_store, &upload_id).is_ok(),
            "old-primary Unix UploadPart session create must preserve the in-progress upload on node {:?}",
            config.node_id
        );
        assert!(
            matches!(
                crate::PgMetadataStore::get_stream_upload(&*object_pg_store, &session_id),
                Err(crate::MetadataError::StreamSessionNotFound { .. })
            ),
            "old-primary Unix UploadPart session create must not publish a stream session on node {:?}",
            config.node_id
        );
        assert!(
            crate::PgMetadataStore::list_stream_segments(&*object_pg_store, &session_id)
                .unwrap()
                .is_empty(),
            "old-primary Unix UploadPart session create must not publish segment rows on node {:?}",
            config.node_id
        );
        assert!(
            object_pg_store
                .pending_metadata_command_envelope(config.node_id.as_u32(), source_epoch)
                .unwrap()
                .is_none(),
            "old-primary Unix UploadPart session create must not leave a source-epoch pending command on node {:?}",
            config.node_id
        );
        assert!(
            object_pg_store
                .pending_metadata_command_envelope(config.node_id.as_u32(), current_epoch)
                .unwrap()
                .is_none(),
            "old-primary Unix UploadPart session create must not leave a current-epoch pending command on node {:?}",
            config.node_id
        );

        let bucket_pg_store = remote.get_pg(bucket_pg).unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_reservations(
                &*bucket_pg_store,
                &bucket,
            )
            .unwrap()
            .is_empty(),
            "old-primary Unix UploadPart session create must leave no bucket-write reservation on node {:?}",
            config.node_id
        );
    }
}

#[test]
fn control_plane_peering_unix_upload_part_finalize_old_primary_preserves_remote_staging() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
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
                            .join("control-plane-sockets")
                            .join(format!(
                                "upload-part-finalize-node-{}.sock",
                                node_id.as_u32()
                            ))
                            .to_string_lossy()
                            .into_owned(),
                    )
                })
                .collect(),
            pg_ids.iter().copied().map(PgId::new).collect(),
        )
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    let source_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), source_epoch)
                .unwrap();
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                PgState::Active,
            )
        })
        .collect::<Vec<_>>();

    let source_storage_configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("remote-peering-stale-upload-part-finalize")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let source_map = Arc::new(
        LocalClusterMap::open_frontend_with_configs_and_pg_routes(
            NodeId::new(0),
            source_storage_configs.clone(),
            &pg_ids,
            ec_shape,
            source_epoch,
            source_routes.iter().map(LocalPgRoute::from),
        )
        .unwrap(),
    );
    let (bucket, key, object_pg, _data_pg) = {
        let topology = source_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let source_cluster = crate::StorageCluster::from_local_map(Arc::clone(&source_map)).unwrap();
    create_test_bucket(&source_cluster, &bucket);
    let upload_id = upload_id_from_label("unixpeerfin");
    let create = crate::CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: crate::OwnerIdentity::from_principal("initiator"),
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        object_lock: crate::ObjectLockState::default(),
        checksum: None,
        encryption: crate::ObjectEncryption::None,
    };
    source_cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    let upload = source_cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    let session_id = crate::tests::stream_session_id("unixpeerfin");
    source_cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            1,
            &session_id,
        )
        .unwrap();
    let payload = b"peering unix streamed upload part finalize";
    let payload_crc64 = checksum::crc64::checksum(payload);
    let (_target, segment) = source_cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: payload_crc64,
                payload_crc64,
                segment_okh: [0xf5; 16],
            },
        )
        .unwrap();
    let written = source_cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let placement_key = super::super::super::segment_payload_placement_key(
        &segment.segment_okh,
        segment.segment_vid,
    );
    let written_locations = source_cluster
        .place_payload_shards(
            DataPgId::new_for_test(PgId::new(segment.data_pg_id)),
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
            &placement_key,
        )
        .unwrap();
    let shard_batch = written
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    source_cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();
    let source_data_pg_primary = source_routes
        .iter()
        .find(|route| route.pg_id() == PgId::new(segment.data_pg_id))
        .unwrap()
        .primary_node_id();
    {
        let primary_config = source_storage_configs
            .iter()
            .find(|config| config.node_id == source_data_pg_primary)
            .unwrap();
        let remote_primary = SharedStorageNode::open_with_default_ec_shape(
            &primary_config.data_dir,
            &pg_ids,
            ec_shape,
        )
        .unwrap();
        let remote_data_pg = remote_primary.get_pg(segment.data_pg_id).unwrap();
        for written_shard in &written {
            remote_data_pg
                .validate_written_shard_ack(&written_shard.key, written_shard.ack)
                .unwrap();
        }
    }
    let before_object_pg_proof = source_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    drop(source_cluster);
    drop(source_map);

    authority
        .set_pg_acting_set(PgId::new(object_pg), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    let current_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), current_epoch)
                .unwrap();
            let state = if *pg_id == object_pg {
                PgState::Peering
            } else {
                PgState::Active
            };
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                state,
            )
        })
        .collect::<Vec<_>>();
    let historical_pg_routes = source_routes
        .iter()
        .map(|route| StorageNodePgRoute {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            acting_set: route.acting_set().to_vec(),
        })
        .collect::<Vec<_>>();
    let current_pg_routes = current_routes
        .iter()
        .map(|route| StorageNodePgRoute {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            acting_set: route.acting_set().to_vec(),
        })
        .collect::<Vec<_>>();
    let mut server_configs = Vec::new();
    let mut client_configs = Vec::new();
    for node_config in &source_storage_configs {
        let socket_path = tmp.path().join("sockets").join(format!(
            "peering-stale-upload-part-finalize-node-{}.sock",
            node_config.node_id.as_u32()
        ));
        private_socket_dir(socket_path.parent().unwrap());
        server_configs.push(StorageNodeProcessConfig {
            node_id: node_config.node_id,
            cluster_epoch: current_epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: node_config.data_dir.clone(),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.to_vec(),
            socket_path: socket_path.clone(),
            pg_routes: current_pg_routes.clone(),
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: historical_pg_routes.clone(),
        });
        client_configs.push(LocalUnixStorageNodeClientConfig::new(
            node_config.node_id,
            socket_path,
        ));
    }
    let mut _server_guards = Vec::new();
    for config in server_configs.iter().cloned() {
        let server = StorageNodeServer::bind(config).unwrap();
        _server_guards.push(spawn_storage_node_server(server));
    }

    let frontend_configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("frontend-peering-stale-upload-part-finalize")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let mut current_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        frontend_configs,
        &pg_ids,
        ec_shape,
        current_epoch,
        current_routes.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    current_map.test_install_historical_pg_routes(source_routes.clone());
    current_map
        .install_unix_storage_node_clients(client_configs)
        .unwrap();
    let current_map = Arc::new(current_map);
    assert_eq!(
        current_map.pg_route(PgId::new(object_pg)).unwrap().state(),
        PgState::Peering,
        "control-plane acting-set change should put the object PG into Peering"
    );
    assert!(
        !current_map
            .pg_route(PgId::new(object_pg))
            .unwrap()
            .acting_set()
            .contains(&NodeId::new(0)),
        "the old source primary should no longer be in the current acting set"
    );

    let old_primary_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&current_map),
        source_epoch,
    )
    .unwrap();
    let err = old_primary_cluster
        .finalize_upload_part_stream(
            &bucket,
            &key,
            &upload_id,
            &session_id,
            1,
            |_| -> Result<crate::PreparedStreamPartCommit<()>, ()> {
                panic!(
                    "old-primary UploadPart finalization should fail before preparing commit metadata"
                )
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == source_epoch
                && observed_current_epoch == current_epoch
        ),
        "old-primary Unix UploadPart finalization should fail closed after control-plane Peering transition, got {err:?}"
    );

    let bucket_pg = current_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .bucket_pg_for(&bucket);
    for config in &server_configs {
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let object_pg_store = remote.get_pg(object_pg).unwrap();
        let state = object_pg_store.metadata_command_replica_state().unwrap();
        let proof = crate::control_plane::PgMetadataProof {
            applied_log_index: state.applied_log_index,
            applied_log_hash: state.applied_log_hash,
            state_digest: state.state_digest,
        };
        assert_eq!(
            proof, before_object_pg_proof,
            "old-primary Unix UploadPart finalization must not append an object-PG command on node {:?}",
            config.node_id
        );
        assert!(
            crate::PgMetadataStore::get_multipart_upload(&*object_pg_store, &upload_id).is_ok(),
            "old-primary Unix UploadPart finalization must preserve the in-progress upload on node {:?}",
            config.node_id
        );
        assert!(
            crate::PgMetadataStore::get_stream_upload(&*object_pg_store, &session_id).is_ok(),
            "old-primary Unix UploadPart finalization must preserve the active stream session on node {:?}",
            config.node_id
        );
        assert_eq!(
            crate::PgMetadataStore::list_stream_segments(&*object_pg_store, &session_id)
                .unwrap()
                .len(),
            1,
            "old-primary Unix UploadPart finalization must preserve staged segment metadata on node {:?}",
            config.node_id
        );
        assert!(
            matches!(
                crate::PgMetadataStore::get_multipart_part(&*object_pg_store, &upload_id, 1),
                Err(crate::MetadataError::PartNotFound { .. })
            ),
            "old-primary Unix UploadPart finalization must not publish multipart part metadata on node {:?}",
            config.node_id
        );
        assert!(
            object_pg_store
                .pending_metadata_command_envelope(config.node_id.as_u32(), source_epoch)
                .unwrap()
                .is_none(),
            "old-primary Unix UploadPart finalization must not leave a source-epoch pending command on node {:?}",
            config.node_id
        );
        assert!(
            object_pg_store
                .pending_metadata_command_envelope(config.node_id.as_u32(), current_epoch)
                .unwrap()
                .is_none(),
            "old-primary Unix UploadPart finalization must not leave a current-epoch pending command on node {:?}",
            config.node_id
        );

        let bucket_pg_store = remote.get_pg(bucket_pg).unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_reservations(
                &*bucket_pg_store,
                &bucket,
            )
            .unwrap()
            .is_empty(),
            "old-primary Unix UploadPart finalization must leave no bucket-write reservation on node {:?}",
            config.node_id
        );
    }

    for (written_shard, location) in written.iter().zip(written_locations) {
        let config = server_configs
            .iter()
            .find(|config| config.node_id == location.node_id())
            .unwrap();
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert_eq!(
            remote
                .read_shard_file(segment.data_pg_id, &written_shard.key)
                .unwrap()
                .len() as u64,
            written_shard.ack.stored_size,
            "old-primary Unix UploadPart finalization must preserve staged shard file {} on node {}",
            written_shard.key,
            config.node_id.as_u32()
        );
    }
    let data_pg_primary_config = server_configs
        .iter()
        .find(|config| config.node_id == source_data_pg_primary)
        .unwrap();
    let data_pg_primary = SharedStorageNode::open_with_default_ec_shape(
        &data_pg_primary_config.data_dir,
        &data_pg_primary_config.pg_ids,
        data_pg_primary_config.default_ec_shape,
    )
    .unwrap();
    let data_pg_store = data_pg_primary.get_pg(segment.data_pg_id).unwrap();
    for written_shard in &written {
        data_pg_store
            .validate_written_shard_ack(&written_shard.key, written_shard.ack)
            .unwrap();
    }
}

#[test]
fn control_plane_peering_unix_stream_put_finalize_old_primary_preserves_remote_staging() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
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
                            .join("control-plane-sockets")
                            .join(format!(
                                "stream-put-finalize-node-{}.sock",
                                node_id.as_u32()
                            ))
                            .to_string_lossy()
                            .into_owned(),
                    )
                })
                .collect(),
            pg_ids.iter().copied().map(PgId::new).collect(),
        )
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    let source_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), source_epoch)
                .unwrap();
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                PgState::Active,
            )
        })
        .collect::<Vec<_>>();

    let source_storage_configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("remote-peering-stale-stream-put-finalize")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let source_map = Arc::new(
        LocalClusterMap::open_frontend_with_configs_and_pg_routes(
            NodeId::new(0),
            source_storage_configs.clone(),
            &pg_ids,
            ec_shape,
            source_epoch,
            source_routes.iter().map(LocalPgRoute::from),
        )
        .unwrap(),
    );
    let (bucket, key, object_pg, _data_pg) = {
        let topology = source_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let source_cluster = crate::StorageCluster::from_local_map(Arc::clone(&source_map)).unwrap();
    create_test_bucket(&source_cluster, &bucket);
    let session_id = crate::tests::stream_session_id("unixpeerputfin");
    source_cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"peering unix streamed put object finalize";
    let payload_crc64 = checksum::crc64::checksum(payload);
    let (_target, segment) = source_cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: payload_crc64,
                payload_crc64,
                segment_okh: [0xf7; 16],
            },
        )
        .unwrap();
    let written = source_cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let placement_key = super::super::super::segment_payload_placement_key(
        &segment.segment_okh,
        segment.segment_vid,
    );
    let written_locations = source_cluster
        .place_payload_shards(
            DataPgId::new_for_test(PgId::new(segment.data_pg_id)),
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
            &placement_key,
        )
        .unwrap();
    let shard_batch = written
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    source_cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();
    let source_data_pg_primary = source_routes
        .iter()
        .find(|route| route.pg_id() == PgId::new(segment.data_pg_id))
        .unwrap()
        .primary_node_id();
    {
        let primary_config = source_storage_configs
            .iter()
            .find(|config| config.node_id == source_data_pg_primary)
            .unwrap();
        let remote_primary = SharedStorageNode::open_with_default_ec_shape(
            &primary_config.data_dir,
            &pg_ids,
            ec_shape,
        )
        .unwrap();
        let remote_data_pg = remote_primary.get_pg(segment.data_pg_id).unwrap();
        for written_shard in &written {
            remote_data_pg
                .validate_written_shard_ack(&written_shard.key, written_shard.ack)
                .unwrap();
        }
    }
    let before_object_pg_proof = source_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    let stream_reservation_id = source_cluster
        .load_stream_upload_session(&bucket, &key, &session_id)
        .unwrap()
        .bucket_write_reservation
        .unwrap()
        .reservation_id;
    drop(source_cluster);
    drop(source_map);

    authority
        .set_pg_acting_set(PgId::new(object_pg), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    let current_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), current_epoch)
                .unwrap();
            let state = if *pg_id == object_pg {
                PgState::Peering
            } else {
                PgState::Active
            };
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                state,
            )
        })
        .collect::<Vec<_>>();
    let historical_pg_routes = source_routes
        .iter()
        .map(|route| StorageNodePgRoute {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            acting_set: route.acting_set().to_vec(),
        })
        .collect::<Vec<_>>();
    let current_pg_routes = current_routes
        .iter()
        .map(|route| StorageNodePgRoute {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            acting_set: route.acting_set().to_vec(),
        })
        .collect::<Vec<_>>();
    let mut server_configs = Vec::new();
    let mut client_configs = Vec::new();
    for node_config in &source_storage_configs {
        let socket_path = tmp.path().join("sockets").join(format!(
            "peering-stale-stream-put-finalize-node-{}.sock",
            node_config.node_id.as_u32()
        ));
        private_socket_dir(socket_path.parent().unwrap());
        server_configs.push(StorageNodeProcessConfig {
            node_id: node_config.node_id,
            cluster_epoch: current_epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: node_config.data_dir.clone(),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.to_vec(),
            socket_path: socket_path.clone(),
            pg_routes: current_pg_routes.clone(),
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: historical_pg_routes.clone(),
        });
        client_configs.push(LocalUnixStorageNodeClientConfig::new(
            node_config.node_id,
            socket_path,
        ));
    }
    let mut _server_guards = Vec::new();
    for config in server_configs.iter().cloned() {
        let server = StorageNodeServer::bind(config).unwrap();
        _server_guards.push(spawn_storage_node_server(server));
    }

    let frontend_configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("frontend-peering-stale-stream-put-finalize")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let mut current_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        frontend_configs,
        &pg_ids,
        ec_shape,
        current_epoch,
        current_routes.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    current_map.test_install_historical_pg_routes(source_routes);
    current_map
        .install_unix_storage_node_clients(client_configs)
        .unwrap();
    let current_map = Arc::new(current_map);
    assert_eq!(
        current_map.pg_route(PgId::new(object_pg)).unwrap().state(),
        PgState::Peering,
        "control-plane acting-set change should put the object PG into Peering"
    );
    assert!(
        !current_map
            .pg_route(PgId::new(object_pg))
            .unwrap()
            .acting_set()
            .contains(&NodeId::new(0)),
        "the old source primary should no longer be in the current acting set"
    );

    let old_primary_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&current_map),
        source_epoch,
    )
    .unwrap();
    let err = old_primary_cluster
        .finalize_put_object_stream(
            &bucket,
            &key,
            &session_id,
            payload.len() as u64,
            |_| -> Result<crate::PreparedStreamPutCommit<()>, ()> {
                panic!("old-primary stream PUT finalization should fail before preparing commit metadata")
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == source_epoch
                && observed_current_epoch == current_epoch
        ),
        "old-primary Unix stream PUT finalization should fail closed after control-plane Peering transition, got {err:?}"
    );

    let bucket_pg = current_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .bucket_pg_for(&bucket);
    let bucket_primary_node_id = current_map
        .metadata_pg_primary_node(current_epoch, PgId::new(bucket_pg))
        .unwrap()
        .node_id();
    for config in &server_configs {
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let object_pg_store = remote.get_pg(object_pg).unwrap();
        let state = object_pg_store.metadata_command_replica_state().unwrap();
        let proof = crate::control_plane::PgMetadataProof {
            applied_log_index: state.applied_log_index,
            applied_log_hash: state.applied_log_hash,
            state_digest: state.state_digest,
        };
        assert_eq!(
            proof, before_object_pg_proof,
            "old-primary Unix stream PUT finalization must not append an object-PG command on node {:?}",
            config.node_id
        );
        assert!(
            matches!(
                crate::PgMetadataStore::get_object_meta(&*object_pg_store, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ),
            "old-primary Unix stream PUT finalization must not publish object metadata on node {:?}",
            config.node_id
        );
        assert!(
            crate::PgMetadataStore::get_stream_upload(&*object_pg_store, &session_id).is_ok(),
            "old-primary Unix stream PUT finalization must preserve the active stream session on node {:?}",
            config.node_id
        );
        assert_eq!(
            crate::PgMetadataStore::list_stream_segments(&*object_pg_store, &session_id)
                .unwrap(),
            vec![segment.clone()],
            "old-primary Unix stream PUT finalization must preserve staged segment metadata on node {:?}",
            config.node_id
        );
        assert!(
            object_pg_store
                .pending_metadata_command_envelope(config.node_id.as_u32(), source_epoch)
                .unwrap()
                .is_none(),
            "old-primary Unix stream PUT finalization must not leave a source-epoch pending command on node {:?}",
            config.node_id
        );
        assert!(
            object_pg_store
                .pending_metadata_command_envelope(config.node_id.as_u32(), current_epoch)
                .unwrap()
                .is_none(),
            "old-primary Unix stream PUT finalization must not leave a current-epoch pending command on node {:?}",
            config.node_id
        );

        if config.node_id == bucket_primary_node_id {
            let bucket_pg_store = remote.get_pg(bucket_pg).unwrap();
            assert!(
                crate::PgMetadataStore::durable_bucket_write_reservations(
                    &*bucket_pg_store,
                    &bucket,
                )
                .unwrap()
                .iter()
                .any(|reservation| reservation.reservation_id == stream_reservation_id),
                "old-primary Unix stream PUT finalization must preserve its stream bucket write reservation on the bucket primary"
            );
        }
    }

    for (written_shard, location) in written.iter().zip(written_locations) {
        let config = server_configs
            .iter()
            .find(|config| config.node_id == location.node_id())
            .unwrap();
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert_eq!(
            remote
                .read_shard_file(segment.data_pg_id, &written_shard.key)
                .unwrap()
                .len() as u64,
            written_shard.ack.stored_size,
            "old-primary Unix stream PUT finalization must preserve staged shard file {} on node {}",
            written_shard.key,
            config.node_id.as_u32()
        );
    }
    let data_pg_primary_config = server_configs
        .iter()
        .find(|config| config.node_id == source_data_pg_primary)
        .unwrap();
    let data_pg_primary = SharedStorageNode::open_with_default_ec_shape(
        &data_pg_primary_config.data_dir,
        &data_pg_primary_config.pg_ids,
        data_pg_primary_config.default_ec_shape,
    )
    .unwrap();
    let data_pg_store = data_pg_primary.get_pg(segment.data_pg_id).unwrap();
    for written_shard in &written {
        data_pg_store
            .validate_written_shard_ack(&written_shard.key, written_shard.ack)
            .unwrap();
    }
}

#[test]
fn control_plane_peering_unix_upload_part_copy_finalize_old_primary_preserves_remote_staging() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
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
                            .join("control-plane-sockets")
                            .join(format!(
                                "upload-part-copy-finalize-node-{}.sock",
                                node_id.as_u32()
                            ))
                            .to_string_lossy()
                            .into_owned(),
                    )
                })
                .collect(),
            pg_ids.iter().copied().map(PgId::new).collect(),
        )
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    let source_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), source_epoch)
                .unwrap();
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                PgState::Active,
            )
        })
        .collect::<Vec<_>>();

    let source_storage_configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("remote-peering-stale-upload-part-copy-finalize")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let source_map = Arc::new(
        LocalClusterMap::open_frontend_with_configs_and_pg_routes(
            NodeId::new(0),
            source_storage_configs.clone(),
            &pg_ids,
            ec_shape,
            source_epoch,
            source_routes.iter().map(LocalPgRoute::from),
        )
        .unwrap(),
    );
    let (bucket, key, object_pg, _data_pg) = {
        let topology = source_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let source_cluster = crate::StorageCluster::from_local_map(Arc::clone(&source_map)).unwrap();
    create_test_bucket(&source_cluster, &bucket);
    let upload_id = upload_id_from_label("unixpeercopyfin");
    let create = crate::CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: crate::OwnerIdentity::from_principal("initiator"),
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        object_lock: crate::ObjectLockState::default(),
        checksum: None,
        encryption: crate::ObjectEncryption::None,
    };
    source_cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    let upload = source_cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    let session_id = crate::tests::stream_session_id("unixpeercpyfin");
    source_cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            2,
            &session_id,
        )
        .unwrap();

    let mut staged = Vec::new();
    for (segment_index, (payload, segment_okh)) in [
        (
            b"peering unix copied source segment one".as_slice(),
            [0xc1; 16],
        ),
        (
            b"peering unix copied source segment two".as_slice(),
            [0xc2; 16],
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let payload_crc64 = checksum::crc64::checksum(payload);
        let (_target, segment) = source_cluster
            .prepare_stream_segment_append(
                &bucket,
                &key,
                &crate::PrepareStreamUploadSegmentAppendReq {
                    session_id: session_id.clone(),
                    segment_index: segment_index as u32,
                    size: payload.len() as u64,
                    segment_crc64: payload_crc64,
                    payload_crc64,
                    segment_okh,
                },
            )
            .unwrap();
        let written = source_cluster
            .write_stream_segment_payload_shards(&segment, payload)
            .unwrap();
        let placement_key = super::super::super::segment_payload_placement_key(
            &segment.segment_okh,
            segment.segment_vid,
        );
        let written_locations = source_cluster
            .place_payload_shards(
                DataPgId::new_for_test(PgId::new(segment.data_pg_id)),
                EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
                &placement_key,
            )
            .unwrap();
        let shard_batch = written
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect::<Vec<_>>();
        source_cluster
            .commit_stream_segment_append(
                &bucket,
                &key,
                &session_id,
                segment.segment_index,
                &segment,
                &shard_batch,
            )
            .unwrap();
        let source_data_pg_primary = source_routes
            .iter()
            .find(|route| route.pg_id() == PgId::new(segment.data_pg_id))
            .unwrap()
            .primary_node_id();
        {
            let primary_config = source_storage_configs
                .iter()
                .find(|config| config.node_id == source_data_pg_primary)
                .unwrap();
            let remote_primary = SharedStorageNode::open_with_default_ec_shape(
                &primary_config.data_dir,
                &pg_ids,
                ec_shape,
            )
            .unwrap();
            let remote_data_pg = remote_primary.get_pg(segment.data_pg_id).unwrap();
            for written_shard in &written {
                remote_data_pg
                    .validate_written_shard_ack(&written_shard.key, written_shard.ack)
                    .unwrap();
            }
        }
        staged.push((segment, written, written_locations, source_data_pg_primary));
    }
    let before_object_pg_proof = source_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    drop(source_cluster);
    drop(source_map);

    authority
        .set_pg_acting_set(PgId::new(object_pg), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    let current_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), current_epoch)
                .unwrap();
            let state = if *pg_id == object_pg {
                PgState::Peering
            } else {
                PgState::Active
            };
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                state,
            )
        })
        .collect::<Vec<_>>();
    let historical_pg_routes = source_routes
        .iter()
        .map(|route| StorageNodePgRoute {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            acting_set: route.acting_set().to_vec(),
        })
        .collect::<Vec<_>>();
    let current_pg_routes = current_routes
        .iter()
        .map(|route| StorageNodePgRoute {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            acting_set: route.acting_set().to_vec(),
        })
        .collect::<Vec<_>>();
    let mut server_configs = Vec::new();
    let mut client_configs = Vec::new();
    for node_config in &source_storage_configs {
        let socket_path = tmp.path().join("sockets").join(format!(
            "peering-stale-upload-part-copy-finalize-node-{}.sock",
            node_config.node_id.as_u32()
        ));
        private_socket_dir(socket_path.parent().unwrap());
        server_configs.push(StorageNodeProcessConfig {
            node_id: node_config.node_id,
            cluster_epoch: current_epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: node_config.data_dir.clone(),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.to_vec(),
            socket_path: socket_path.clone(),
            pg_routes: current_pg_routes.clone(),
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: historical_pg_routes.clone(),
        });
        client_configs.push(LocalUnixStorageNodeClientConfig::new(
            node_config.node_id,
            socket_path,
        ));
    }
    let mut _server_guards = Vec::new();
    for config in server_configs.iter().cloned() {
        let server = StorageNodeServer::bind(config).unwrap();
        _server_guards.push(spawn_storage_node_server(server));
    }

    let frontend_configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("frontend-peering-stale-upload-part-copy-finalize")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let mut current_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        frontend_configs,
        &pg_ids,
        ec_shape,
        current_epoch,
        current_routes.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    current_map.test_install_historical_pg_routes(source_routes);
    current_map
        .install_unix_storage_node_clients(client_configs)
        .unwrap();
    let current_map = Arc::new(current_map);
    assert_eq!(
        current_map.pg_route(PgId::new(object_pg)).unwrap().state(),
        PgState::Peering,
        "control-plane acting-set change should put the object PG into Peering"
    );
    assert!(
        !current_map
            .pg_route(PgId::new(object_pg))
            .unwrap()
            .acting_set()
            .contains(&NodeId::new(0)),
        "the old source primary should no longer be in the current acting set"
    );

    let old_primary_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&current_map),
        source_epoch,
    )
    .unwrap();
    let err = old_primary_cluster
        .finalize_upload_part_stream(
            &bucket,
            &key,
            &upload_id,
            &session_id,
            2,
            |_| -> Result<crate::PreparedStreamPartCommit<()>, ()> {
                panic!(
                    "old-primary UploadPartCopy finalization should fail before preparing commit metadata"
                )
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == source_epoch
                && observed_current_epoch == current_epoch
        ),
        "old-primary Unix UploadPartCopy finalization should fail closed after control-plane Peering transition, got {err:?}"
    );

    let bucket_pg = current_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .bucket_pg_for(&bucket);
    for config in &server_configs {
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let object_pg_store = remote.get_pg(object_pg).unwrap();
        let state = object_pg_store.metadata_command_replica_state().unwrap();
        let proof = crate::control_plane::PgMetadataProof {
            applied_log_index: state.applied_log_index,
            applied_log_hash: state.applied_log_hash,
            state_digest: state.state_digest,
        };
        assert_eq!(
            proof, before_object_pg_proof,
            "old-primary Unix UploadPartCopy finalization must not append an object-PG command on node {:?}",
            config.node_id
        );
        assert!(
            crate::PgMetadataStore::get_multipart_upload(&*object_pg_store, &upload_id).is_ok(),
            "old-primary Unix UploadPartCopy finalization must preserve the in-progress upload on node {:?}",
            config.node_id
        );
        assert!(
            crate::PgMetadataStore::get_stream_upload(&*object_pg_store, &session_id).is_ok(),
            "old-primary Unix UploadPartCopy finalization must preserve the active stream session on node {:?}",
            config.node_id
        );
        assert_eq!(
            crate::PgMetadataStore::list_stream_segments(&*object_pg_store, &session_id)
                .unwrap()
                .len(),
            staged.len(),
            "old-primary Unix UploadPartCopy finalization must preserve copied staged segment metadata on node {:?}",
            config.node_id
        );
        assert!(
            matches!(
                crate::PgMetadataStore::get_multipart_part(&*object_pg_store, &upload_id, 2),
                Err(crate::MetadataError::PartNotFound { .. })
            ),
            "old-primary Unix UploadPartCopy finalization must not publish multipart part metadata on node {:?}",
            config.node_id
        );
        assert!(
            object_pg_store
                .pending_metadata_command_envelope(config.node_id.as_u32(), source_epoch)
                .unwrap()
                .is_none(),
            "old-primary Unix UploadPartCopy finalization must not leave a source-epoch pending command on node {:?}",
            config.node_id
        );
        assert!(
            object_pg_store
                .pending_metadata_command_envelope(config.node_id.as_u32(), current_epoch)
                .unwrap()
                .is_none(),
            "old-primary Unix UploadPartCopy finalization must not leave a current-epoch pending command on node {:?}",
            config.node_id
        );

        let bucket_pg_store = remote.get_pg(bucket_pg).unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_reservations(
                &*bucket_pg_store,
                &bucket,
            )
            .unwrap()
            .is_empty(),
            "old-primary Unix UploadPartCopy finalization must leave no bucket-write reservation on node {:?}",
            config.node_id
        );
    }

    for (segment, written, locations, source_data_pg_primary) in &staged {
        for (written_shard, location) in written.iter().zip(locations) {
            let config = server_configs
                .iter()
                .find(|config| config.node_id == location.node_id())
                .unwrap();
            let remote = SharedStorageNode::open_with_default_ec_shape(
                &config.data_dir,
                &config.pg_ids,
                config.default_ec_shape,
            )
            .unwrap();
            assert_eq!(
                remote
                    .read_shard_file(segment.data_pg_id, &written_shard.key)
                    .unwrap()
                    .len() as u64,
                written_shard.ack.stored_size,
                "old-primary Unix UploadPartCopy finalization must preserve copied shard file {} on node {}",
                written_shard.key,
                config.node_id.as_u32()
            );
        }
        let data_pg_primary_config = server_configs
            .iter()
            .find(|config| config.node_id == *source_data_pg_primary)
            .unwrap();
        let data_pg_primary = SharedStorageNode::open_with_default_ec_shape(
            &data_pg_primary_config.data_dir,
            &data_pg_primary_config.pg_ids,
            data_pg_primary_config.default_ec_shape,
        )
        .unwrap();
        let data_pg_store = data_pg_primary.get_pg(segment.data_pg_id).unwrap();
        for written_shard in written {
            data_pg_store
                .validate_written_shard_ack(&written_shard.key, written_shard.ack)
                .unwrap();
        }
    }
}

#[test]
fn non_current_epoch_unix_stream_append_commit_fails_closed_and_cleans_remote_state() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let current_epoch = ClusterEpoch::INITIAL;
    let stale_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
    let mut map = LocalClusterMap::open(
        &tmp.path().join("frontend-stream-append-stale"),
        &node_ids,
        &pg_ids,
        ec_shape,
    )
    .unwrap();
    for pg_id in pg_ids {
        set_route_primary(&mut map, pg_id, NodeId::new(0));
    }
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };

    let mut server_configs = Vec::new();
    let mut client_configs = Vec::new();
    for node_id in node_ids {
        let socket_path = tmp.path().join("sockets").join(format!(
            "stale-stream-append-node-{}.sock",
            node_id.as_u32()
        ));
        private_socket_dir(socket_path.parent().unwrap());
        let pg_routes = pg_ids
            .iter()
            .map(|pg_id| StorageNodePgRoute {
                pg_id: *pg_id,
                cluster_epoch: current_epoch,
                state: PgState::Active,
                primary_node_id: NodeId::new(0),
                acting_set: node_ids.to_vec(),
            })
            .collect();
        server_configs.push(StorageNodeProcessConfig {
            node_id,
            cluster_epoch: current_epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: tmp
                .path()
                .join(format!("remote-stale-stream-append-{}", node_id.as_u32())),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.to_vec(),
            socket_path: socket_path.clone(),
            pg_routes,
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: Vec::new(),
        });
        client_configs.push(LocalUnixStorageNodeClientConfig::new(node_id, socket_path));
    }
    let mut _server_guards = Vec::new();
    for config in server_configs.iter().cloned() {
        let server = StorageNodeServer::bind(config).unwrap();
        _server_guards.push(spawn_storage_node_server(server));
    }
    map.install_unix_storage_node_clients(client_configs)
        .unwrap();
    let map = Arc::new(map);
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&current_cluster, &bucket);
    let session_id = crate::tests::stream_session_id("unixstaleappend");
    current_cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let before_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    let payload = b"stale unix stream append";
    let payload_crc64 = checksum::crc64::checksum(payload);
    let (_target, segment) = current_cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: payload_crc64,
                payload_crc64,
                segment_okh: [0xc7; 16],
            },
        )
        .unwrap();
    let written = current_cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    let stale_cluster =
        crate::StorageCluster::test_from_local_map_with_epoch(Arc::clone(&map), stale_epoch)
            .unwrap();

    let err = stale_cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == stale_epoch
                && observed_current_epoch == current_epoch
        ),
        "stale Unix stream append commit should fail closed before command build, got {err:?}"
    );

    let after_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof, before_object_pg_proof,
        "stale Unix stream append commit must not append an object-PG command"
    );

    let remote_primary_config = server_configs
        .iter()
        .find(|config| config.node_id == NodeId::new(0))
        .unwrap();
    let remote_primary = SharedStorageNode::open_with_default_ec_shape(
        &remote_primary_config.data_dir,
        &remote_primary_config.pg_ids,
        remote_primary_config.default_ec_shape,
    )
    .unwrap();
    let remote_object_pg = remote_primary.get_pg(object_pg).unwrap();
    assert!(
        remote_object_pg
            .pending_metadata_command_slot(NodeId::new(0).as_u32(), current_epoch)
            .unwrap()
            .is_none(),
        "stale Unix stream append commit must not leave a remote pending command"
    );
    assert!(
        crate::PgMetadataStore::get_stream_upload(&*remote_object_pg, &session_id).is_ok(),
        "stale Unix stream append commit must preserve the in-progress session"
    );
    assert!(
        crate::PgMetadataStore::list_stream_segments(&*remote_object_pg, &session_id)
            .unwrap()
            .is_empty(),
        "stale Unix stream append commit must not publish staged segment metadata"
    );

    for written_shard in &written {
        for config in &server_configs {
            let remote = SharedStorageNode::open_with_default_ec_shape(
                &config.data_dir,
                &config.pg_ids,
                config.default_ec_shape,
            )
            .unwrap();
            assert!(
                matches!(
                    remote.read_shard_file(segment.data_pg_id, &written_shard.key),
                    Err(StoreError::NotFound)
                ),
                "stale Unix stream append cleanup must delete shard {} from node {}",
                written_shard.key,
                config.node_id.as_u32()
            );
        }
    }
    let remote_data_pg = remote_primary.get_pg(segment.data_pg_id).unwrap();
    for written_shard in &written {
        assert!(
            matches!(
                remote_data_pg.validate_written_shard_ack(&written_shard.key, written_shard.ack),
                Err(StoreError::NotFound)
            ),
            "stale Unix stream append cleanup must delete remote ack for shard {}",
            written_shard.key
        );
    }
}

#[test]
fn non_current_epoch_unix_upload_part_stream_session_create_fails_closed_without_remote_mutation() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let current_epoch = ClusterEpoch::INITIAL;
    let stale_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
    let mut map = LocalClusterMap::open(
        &tmp.path().join("frontend-upload-part-session-stale"),
        &node_ids,
        &pg_ids,
        ec_shape,
    )
    .unwrap();
    for pg_id in pg_ids {
        set_route_primary(&mut map, pg_id, NodeId::new(0));
    }
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };

    let mut server_configs = Vec::new();
    let mut client_configs = Vec::new();
    for node_id in node_ids {
        let socket_path = tmp.path().join("sockets").join(format!(
            "stale-upload-part-session-node-{}.sock",
            node_id.as_u32()
        ));
        private_socket_dir(socket_path.parent().unwrap());
        let pg_routes = pg_ids
            .iter()
            .map(|pg_id| StorageNodePgRoute {
                pg_id: *pg_id,
                cluster_epoch: current_epoch,
                state: PgState::Active,
                primary_node_id: NodeId::new(0),
                acting_set: node_ids.to_vec(),
            })
            .collect();
        server_configs.push(StorageNodeProcessConfig {
            node_id,
            cluster_epoch: current_epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: tmp.path().join(format!(
                "remote-stale-upload-part-session-{}",
                node_id.as_u32()
            )),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.to_vec(),
            socket_path: socket_path.clone(),
            pg_routes,
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: Vec::new(),
        });
        client_configs.push(LocalUnixStorageNodeClientConfig::new(node_id, socket_path));
    }
    let mut _server_guards = Vec::new();
    for config in server_configs.iter().cloned() {
        let server = StorageNodeServer::bind(config).unwrap();
        _server_guards.push(spawn_storage_node_server(server));
    }
    map.install_unix_storage_node_clients(client_configs)
        .unwrap();
    let map = Arc::new(map);
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&current_cluster, &bucket);
    let upload_id = upload_id_from_label("unixstaleuppart");
    let create = crate::CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: crate::OwnerIdentity::from_principal("initiator"),
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        object_lock: crate::ObjectLockState::default(),
        checksum: None,
        encryption: crate::ObjectEncryption::None,
    };
    current_cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    let upload = current_cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    let before_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    let stale_cluster =
        crate::StorageCluster::test_from_local_map_with_epoch(Arc::clone(&map), stale_epoch)
            .unwrap();
    let session_id = crate::tests::stream_session_id("unixstaleuppart");

    let err = stale_cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            1,
            &session_id,
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == stale_epoch
                && observed_current_epoch == current_epoch
        ),
        "stale Unix UploadPart session create should fail closed before command build, got {err:?}"
    );

    let after_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof, before_object_pg_proof,
        "stale Unix UploadPart session create must not append an object-PG command"
    );
    current_cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();

    let remote_primary_config = server_configs
        .iter()
        .find(|config| config.node_id == NodeId::new(0))
        .unwrap();
    let remote_primary = SharedStorageNode::open_with_default_ec_shape(
        &remote_primary_config.data_dir,
        &remote_primary_config.pg_ids,
        remote_primary_config.default_ec_shape,
    )
    .unwrap();
    let remote_object_pg = remote_primary.get_pg(object_pg).unwrap();
    assert!(
        remote_object_pg
            .pending_metadata_command_slot(NodeId::new(0).as_u32(), current_epoch)
            .unwrap()
            .is_none(),
        "stale Unix UploadPart session create must not leave a remote pending command"
    );
    assert!(
        crate::PgMetadataStore::get_multipart_upload(&*remote_object_pg, &upload_id).is_ok(),
        "stale Unix UploadPart session create must preserve the in-progress upload"
    );
    assert!(
        matches!(
            crate::PgMetadataStore::get_stream_upload(&*remote_object_pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ),
        "stale Unix UploadPart session create must not publish a stream session"
    );
    assert!(
        crate::PgMetadataStore::list_stream_segments(&*remote_object_pg, &session_id)
            .unwrap()
            .is_empty(),
        "stale Unix UploadPart session create must not publish segment rows"
    );
    let remote_bucket_pg = remote_primary
        .get_pg(current_cluster.test_bucket_pg_id_for(&bucket))
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*remote_bucket_pg, &bucket)
            .unwrap()
            .is_empty(),
        "stale Unix UploadPart session create must not leave bucket write reservations"
    );
}

#[test]
fn non_current_epoch_unix_upload_part_stream_finalize_fails_closed_without_remote_mutation() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let current_epoch = ClusterEpoch::INITIAL;
    let stale_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
    let mut map = LocalClusterMap::open(
        &tmp.path().join("frontend-upload-part-finalize-stale"),
        &node_ids,
        &pg_ids,
        ec_shape,
    )
    .unwrap();
    for pg_id in pg_ids {
        set_route_primary(&mut map, pg_id, NodeId::new(0));
    }
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };

    let mut server_configs = Vec::new();
    let mut client_configs = Vec::new();
    for node_id in node_ids {
        let socket_path = tmp.path().join("sockets").join(format!(
            "stale-upload-part-finalize-node-{}.sock",
            node_id.as_u32()
        ));
        private_socket_dir(socket_path.parent().unwrap());
        let pg_routes = pg_ids
            .iter()
            .map(|pg_id| StorageNodePgRoute {
                pg_id: *pg_id,
                cluster_epoch: current_epoch,
                state: PgState::Active,
                primary_node_id: NodeId::new(0),
                acting_set: node_ids.to_vec(),
            })
            .collect();
        server_configs.push(StorageNodeProcessConfig {
            node_id,
            cluster_epoch: current_epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: tmp.path().join(format!(
                "remote-stale-upload-part-finalize-{}",
                node_id.as_u32()
            )),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.to_vec(),
            socket_path: socket_path.clone(),
            pg_routes,
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: Vec::new(),
        });
        client_configs.push(LocalUnixStorageNodeClientConfig::new(node_id, socket_path));
    }
    let mut _server_guards = Vec::new();
    for config in server_configs.iter().cloned() {
        let server = StorageNodeServer::bind(config).unwrap();
        _server_guards.push(spawn_storage_node_server(server));
    }
    map.install_unix_storage_node_clients(client_configs)
        .unwrap();
    let map = Arc::new(map);
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&current_cluster, &bucket);
    let upload_id = upload_id_from_label("unixstalefinal");
    let create = crate::CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: crate::OwnerIdentity::from_principal("initiator"),
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        object_lock: crate::ObjectLockState::default(),
        checksum: None,
        encryption: crate::ObjectEncryption::None,
    };
    current_cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    let upload = current_cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    let session_id = crate::tests::stream_session_id("unixstalefinal");
    current_cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            1,
            &session_id,
        )
        .unwrap();
    let payload = b"stale unix streamed upload part finalize";
    let payload_crc64 = checksum::crc64::checksum(payload);
    let (_target, segment) = current_cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: payload_crc64,
                payload_crc64,
                segment_okh: [0xd7; 16],
            },
        )
        .unwrap();
    let written = current_cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    current_cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();
    let before_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    let stale_cluster =
        crate::StorageCluster::test_from_local_map_with_epoch(Arc::clone(&map), stale_epoch)
            .unwrap();

    let err = stale_cluster
        .finalize_upload_part_stream(
            &bucket,
            &key,
            &upload_id,
            &session_id,
            1,
            |_| -> Result<crate::PreparedStreamPartCommit<()>, ()> {
                panic!("stale UploadPart finalization should fail before preparing commit metadata")
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == stale_epoch
                && observed_current_epoch == current_epoch
        ),
        "stale Unix UploadPart finalization should fail closed before command build, got {err:?}"
    );

    let after_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof, before_object_pg_proof,
        "stale Unix UploadPart finalization must not append an object-PG command"
    );
    let remote_primary_config = server_configs
        .iter()
        .find(|config| config.node_id == NodeId::new(0))
        .unwrap();
    let remote_primary = SharedStorageNode::open_with_default_ec_shape(
        &remote_primary_config.data_dir,
        &remote_primary_config.pg_ids,
        remote_primary_config.default_ec_shape,
    )
    .unwrap();
    let remote_object_pg = remote_primary.get_pg(object_pg).unwrap();
    assert!(
        remote_object_pg
            .pending_metadata_command_slot(NodeId::new(0).as_u32(), current_epoch)
            .unwrap()
            .is_none(),
        "stale Unix UploadPart finalization must not leave a remote pending command"
    );
    assert!(
        crate::PgMetadataStore::get_stream_upload(&*remote_object_pg, &session_id).is_ok(),
        "stale Unix UploadPart finalization must preserve the active stream session"
    );
    assert_eq!(
        crate::PgMetadataStore::list_stream_segments(&*remote_object_pg, &session_id)
            .unwrap()
            .len(),
        1,
        "stale Unix UploadPart finalization must preserve staged segment metadata"
    );
    assert!(
        matches!(
            crate::PgMetadataStore::get_multipart_part(&*remote_object_pg, &upload_id, 1),
            Err(crate::MetadataError::PartNotFound { .. })
        ),
        "stale Unix UploadPart finalization must not publish multipart part metadata"
    );
    let remote_bucket_pg = remote_primary
        .get_pg(current_cluster.test_bucket_pg_id_for(&bucket))
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*remote_bucket_pg, &bucket)
            .unwrap()
            .is_empty(),
        "stale Unix UploadPart finalization must not leave bucket write reservations"
    );
    let placement_key = super::super::super::segment_payload_placement_key(
        &segment.segment_okh,
        segment.segment_vid,
    );
    let locations = current_cluster
        .place_payload_shards(
            DataPgId::new_for_test(PgId::new(segment.data_pg_id)),
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
            &placement_key,
        )
        .unwrap();
    for (written_shard, location) in written.iter().zip(locations) {
        let config = server_configs
            .iter()
            .find(|config| config.node_id == location.node_id())
            .unwrap();
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert_eq!(
            remote
                .read_shard_file(segment.data_pg_id, &written_shard.key)
                .unwrap()
                .len() as u64,
            written_shard.ack.stored_size,
            "stale Unix UploadPart finalization must preserve staged shard file {} on node {}",
            written_shard.key,
            config.node_id.as_u32()
        );
        remote_primary
            .get_pg(segment.data_pg_id)
            .unwrap()
            .validate_written_shard_ack(&written_shard.key, written_shard.ack)
            .unwrap();
    }
}

#[test]
fn non_current_epoch_unix_upload_part_copy_finalize_preserves_copied_staging() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let current_epoch = ClusterEpoch::INITIAL;
    let stale_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
    let mut map = LocalClusterMap::open(
        &tmp.path().join("frontend-upload-part-copy-finalize-stale"),
        &node_ids,
        &pg_ids,
        ec_shape,
    )
    .unwrap();
    for pg_id in pg_ids {
        set_route_primary(&mut map, pg_id, NodeId::new(0));
    }
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };

    let mut server_configs = Vec::new();
    let mut client_configs = Vec::new();
    for node_id in node_ids {
        let socket_path = tmp.path().join("sockets").join(format!(
            "stale-upload-part-copy-finalize-node-{}.sock",
            node_id.as_u32()
        ));
        private_socket_dir(socket_path.parent().unwrap());
        let pg_routes = pg_ids
            .iter()
            .map(|pg_id| StorageNodePgRoute {
                pg_id: *pg_id,
                cluster_epoch: current_epoch,
                state: PgState::Active,
                primary_node_id: NodeId::new(0),
                acting_set: node_ids.to_vec(),
            })
            .collect();
        server_configs.push(StorageNodeProcessConfig {
            node_id,
            cluster_epoch: current_epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: tmp.path().join(format!(
                "remote-stale-upload-part-copy-finalize-{}",
                node_id.as_u32()
            )),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.to_vec(),
            socket_path: socket_path.clone(),
            pg_routes,
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: Vec::new(),
        });
        client_configs.push(LocalUnixStorageNodeClientConfig::new(node_id, socket_path));
    }
    let mut _server_guards = Vec::new();
    for config in server_configs.iter().cloned() {
        let server = StorageNodeServer::bind(config).unwrap();
        _server_guards.push(spawn_storage_node_server(server));
    }
    map.install_unix_storage_node_clients(client_configs)
        .unwrap();
    let map = Arc::new(map);
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&current_cluster, &bucket);
    let upload_id = upload_id_from_label("unixstalecopyfin");
    let create = crate::CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: crate::OwnerIdentity::from_principal("initiator"),
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        object_lock: crate::ObjectLockState::default(),
        checksum: None,
        encryption: crate::ObjectEncryption::None,
    };
    current_cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    let upload = current_cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    let session_id = crate::tests::stream_session_id("unixstalecpyfin");
    current_cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            2,
            &session_id,
        )
        .unwrap();

    let mut staged = Vec::new();
    for (segment_index, (payload, segment_okh)) in [
        (
            b"stale unix copied source segment one".as_slice(),
            [0xe1; 16],
        ),
        (
            b"stale unix copied source segment two".as_slice(),
            [0xe2; 16],
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let payload_crc64 = checksum::crc64::checksum(payload);
        let (_target, segment) = current_cluster
            .prepare_stream_segment_append(
                &bucket,
                &key,
                &crate::PrepareStreamUploadSegmentAppendReq {
                    session_id: session_id.clone(),
                    segment_index: segment_index as u32,
                    size: payload.len() as u64,
                    segment_crc64: payload_crc64,
                    payload_crc64,
                    segment_okh,
                },
            )
            .unwrap();
        let written = current_cluster
            .write_stream_segment_payload_shards(&segment, payload)
            .unwrap();
        let shard_batch = written
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect::<Vec<_>>();
        current_cluster
            .commit_stream_segment_append(
                &bucket,
                &key,
                &session_id,
                segment.segment_index,
                &segment,
                &shard_batch,
            )
            .unwrap();
        staged.push((segment, written));
    }
    let before_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    let stale_cluster =
        crate::StorageCluster::test_from_local_map_with_epoch(Arc::clone(&map), stale_epoch)
            .unwrap();

    let err = stale_cluster
        .finalize_upload_part_stream(
            &bucket,
            &key,
            &upload_id,
            &session_id,
            2,
            |_| -> Result<crate::PreparedStreamPartCommit<()>, ()> {
                panic!("stale UploadPartCopy finalization should fail before preparing commit metadata")
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == stale_epoch
                && observed_current_epoch == current_epoch
        ),
        "stale Unix UploadPartCopy finalization should fail closed before command build, got {err:?}"
    );

    let after_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof, before_object_pg_proof,
        "stale Unix UploadPartCopy finalization must not append an object-PG command"
    );
    let remote_primary_config = server_configs
        .iter()
        .find(|config| config.node_id == NodeId::new(0))
        .unwrap();
    let remote_primary = SharedStorageNode::open_with_default_ec_shape(
        &remote_primary_config.data_dir,
        &remote_primary_config.pg_ids,
        remote_primary_config.default_ec_shape,
    )
    .unwrap();
    let remote_object_pg = remote_primary.get_pg(object_pg).unwrap();
    assert!(
        remote_object_pg
            .pending_metadata_command_slot(NodeId::new(0).as_u32(), current_epoch)
            .unwrap()
            .is_none(),
        "stale Unix UploadPartCopy finalization must not leave a remote pending command"
    );
    assert!(
        crate::PgMetadataStore::get_stream_upload(&*remote_object_pg, &session_id).is_ok(),
        "stale Unix UploadPartCopy finalization must preserve the active stream session"
    );
    assert_eq!(
        crate::PgMetadataStore::list_stream_segments(&*remote_object_pg, &session_id)
            .unwrap()
            .len(),
        staged.len(),
        "stale Unix UploadPartCopy finalization must preserve all copied staged segment metadata"
    );
    assert!(
        matches!(
            crate::PgMetadataStore::get_multipart_part(&*remote_object_pg, &upload_id, 2),
            Err(crate::MetadataError::PartNotFound { .. })
        ),
        "stale Unix UploadPartCopy finalization must not publish multipart part metadata"
    );
    let remote_bucket_pg = remote_primary
        .get_pg(current_cluster.test_bucket_pg_id_for(&bucket))
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*remote_bucket_pg, &bucket)
            .unwrap()
            .is_empty(),
        "stale Unix UploadPartCopy finalization must not leave bucket write reservations"
    );
    for (segment, written) in staged {
        let placement_key = super::super::super::segment_payload_placement_key(
            &segment.segment_okh,
            segment.segment_vid,
        );
        let locations = current_cluster
            .place_payload_shards(
                DataPgId::new_for_test(PgId::new(segment.data_pg_id)),
                EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
                &placement_key,
            )
            .unwrap();
        for (written_shard, location) in written.iter().zip(locations) {
            let config = server_configs
                .iter()
                .find(|config| config.node_id == location.node_id())
                .unwrap();
            let remote = SharedStorageNode::open_with_default_ec_shape(
                &config.data_dir,
                &config.pg_ids,
                config.default_ec_shape,
            )
            .unwrap();
            assert_eq!(
                remote
                    .read_shard_file(segment.data_pg_id, &written_shard.key)
                    .unwrap()
                    .len() as u64,
                written_shard.ack.stored_size,
                "stale Unix UploadPartCopy finalization must preserve copied shard file {} on node {}",
                written_shard.key,
                config.node_id.as_u32()
            );
            remote_primary
                .get_pg(segment.data_pg_id)
                .unwrap()
                .validate_written_shard_ack(&written_shard.key, written_shard.ack)
                .unwrap();
        }
    }
}

#[test]
fn non_current_epoch_unix_multipart_completion_fails_closed_without_remote_mutation() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let current_epoch = ClusterEpoch::INITIAL;
    let stale_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
    let mut map = LocalClusterMap::open(
        &tmp.path().join("frontend-multipart-completion"),
        &node_ids,
        &pg_ids,
        ec_shape,
    )
    .unwrap();
    for pg_id in pg_ids {
        set_route_primary(&mut map, pg_id, NodeId::new(0));
    }
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };

    let mut server_configs = Vec::new();
    let mut client_configs = Vec::new();
    for node_id in node_ids {
        let socket_path = tmp.path().join("sockets").join(format!(
            "stale-multipart-completion-node-{}.sock",
            node_id.as_u32()
        ));
        private_socket_dir(socket_path.parent().unwrap());
        let pg_routes = pg_ids
            .iter()
            .map(|pg_id| StorageNodePgRoute {
                pg_id: *pg_id,
                cluster_epoch: current_epoch,
                state: PgState::Active,
                primary_node_id: NodeId::new(0),
                acting_set: node_ids.to_vec(),
            })
            .collect();
        server_configs.push(StorageNodeProcessConfig {
            node_id,
            cluster_epoch: current_epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: tmp.path().join(format!(
                "remote-stale-multipart-completion-{}",
                node_id.as_u32()
            )),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.to_vec(),
            socket_path: socket_path.clone(),
            pg_routes,
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: Vec::new(),
        });
        client_configs.push(LocalUnixStorageNodeClientConfig::new(node_id, socket_path));
    }
    let mut _server_guards = Vec::new();
    for config in server_configs.iter().cloned() {
        let server = StorageNodeServer::bind(config).unwrap();
        _server_guards.push(spawn_storage_node_server(server));
    }
    map.install_unix_storage_node_clients(client_configs)
        .unwrap();
    let map = Arc::new(map);
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&current_cluster, &bucket);
    let (req, _) =
        seed_streamed_multipart_completion(&current_cluster, &bucket, &key, "unixstalecomplete");
    let before_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    let stale_cluster =
        crate::StorageCluster::test_from_local_map_with_epoch(Arc::clone(&map), stale_epoch)
            .unwrap();

    let err = stale_cluster
        .complete_multipart_upload_commit_serialized(req.clone())
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == stale_epoch
                && observed_current_epoch == current_epoch
        ),
        "stale Unix multipart completion should fail closed before command build, got {err:?}"
    );

    let after_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof, before_object_pg_proof,
        "stale Unix multipart completion must not append an object-PG command"
    );
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none(),
        "stale Unix multipart completion must not leave a pending command"
    );

    let remote_primary_config = server_configs
        .iter()
        .find(|config| config.node_id == NodeId::new(0))
        .unwrap();
    let remote_primary = SharedStorageNode::open_with_default_ec_shape(
        &remote_primary_config.data_dir,
        &remote_primary_config.pg_ids,
        remote_primary_config.default_ec_shape,
    )
    .unwrap();
    let remote_object_pg = remote_primary.get_pg(object_pg).unwrap();
    assert!(
        matches!(
            crate::PgMetadataStore::get_object_meta(&*remote_object_pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ),
        "stale Unix multipart completion must not publish object metadata"
    );
    assert!(
        crate::PgMetadataStore::get_multipart_upload(&*remote_object_pg, &req.upload_id).is_ok(),
        "stale Unix multipart completion must leave upload in progress"
    );
    assert_eq!(
        crate::PgMetadataStore::get_multipart_part(
            &*remote_object_pg,
            &req.upload_id,
            req.part_records[0].part_number
        )
        .unwrap(),
        req.part_records[0],
        "stale Unix multipart completion must preserve selected part row"
    );
    assert_eq!(
        crate::PgMetadataStore::get_all_multipart_part_segments_for_upload(
            &*remote_object_pg,
            &req.upload_id
        )
        .unwrap(),
        req.selected_streaming_segments,
        "stale Unix multipart completion must preserve staged segment rows"
    );
    let remote_bucket_pg = remote_primary
        .get_pg(current_cluster.test_bucket_pg_id_for(&bucket))
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*remote_bucket_pg, &bucket)
            .unwrap()
            .is_empty(),
        "stale Unix multipart completion must not leave bucket write reservations"
    );
}

#[test]
fn non_current_epoch_unix_object_metadata_update_fails_closed_without_remote_mutation() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let current_epoch = ClusterEpoch::INITIAL;
    let stale_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
    let mut map = LocalClusterMap::open(
        &tmp.path().join("frontend-object-metadata"),
        &node_ids,
        &pg_ids,
        ec_shape,
    )
    .unwrap();
    for pg_id in pg_ids {
        set_route_primary(&mut map, pg_id, NodeId::new(0));
    }
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };

    let mut server_configs = Vec::new();
    let mut client_configs = Vec::new();
    for node_id in node_ids {
        let socket_path = tmp.path().join("sockets").join(format!(
            "stale-object-metadata-node-{}.sock",
            node_id.as_u32()
        ));
        private_socket_dir(socket_path.parent().unwrap());
        let pg_routes = pg_ids
            .iter()
            .map(|pg_id| StorageNodePgRoute {
                pg_id: *pg_id,
                cluster_epoch: current_epoch,
                state: PgState::Active,
                primary_node_id: NodeId::new(0),
                acting_set: node_ids.to_vec(),
            })
            .collect();
        server_configs.push(StorageNodeProcessConfig {
            node_id,
            cluster_epoch: current_epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: tmp
                .path()
                .join(format!("remote-stale-object-metadata-{}", node_id.as_u32())),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.to_vec(),
            socket_path: socket_path.clone(),
            pg_routes,
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: Vec::new(),
        });
        client_configs.push(LocalUnixStorageNodeClientConfig::new(node_id, socket_path));
    }
    let mut _server_guards = Vec::new();
    for config in server_configs.iter().cloned() {
        let server = StorageNodeServer::bind(config).unwrap();
        _server_guards.push(spawn_storage_node_server(server));
    }
    map.install_unix_storage_node_clients(client_configs)
        .unwrap();
    let map = Arc::new(map);
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let committed = write_committed_direct_segment_for(
        &current_cluster,
        &bucket,
        &key,
        b"stale unix object metadata",
    );
    let before_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    let stale_cluster =
        crate::StorageCluster::test_from_local_map_with_epoch(Arc::clone(&map), stale_epoch)
            .unwrap();
    let tags =
        "<Tagging><TagSet><Tag><Key>stale</Key><Value>ignored</Value></Tag></TagSet></Tagging>";

    let err = stale_cluster
        .put_object_tags_if(&bucket, &key, None, tags, |stored| {
            Ok::<_, ()>(stored.version_id())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == stale_epoch
                && observed_current_epoch == current_epoch
        ),
        "stale Unix object metadata update should fail closed before command build, got {err:?}"
    );

    let after_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof, before_object_pg_proof,
        "stale Unix object metadata update must not append an object-PG command"
    );

    let remote_primary_config = server_configs
        .iter()
        .find(|config| config.node_id == NodeId::new(0))
        .unwrap();
    let remote_primary = SharedStorageNode::open_with_default_ec_shape(
        &remote_primary_config.data_dir,
        &remote_primary_config.pg_ids,
        remote_primary_config.default_ec_shape,
    )
    .unwrap();
    let remote_object_pg = remote_primary.get_pg(object_pg).unwrap();
    assert!(
        remote_object_pg
            .pending_metadata_command_slot(NodeId::new(0).as_u32(), current_epoch)
            .unwrap()
            .is_none(),
        "stale Unix object metadata update must not leave a remote pending command"
    );
    let stored = crate::PgMetadataStore::get_object_meta(&*remote_object_pg, &bucket, &key)
        .unwrap()
        .into_live()
        .unwrap();
    assert_eq!(stored.generation_id, committed.generation_id);
    assert_eq!(
        crate::PgMetadataStore::get_object_tags(
            &*remote_object_pg,
            &bucket,
            &key,
            crate::VersionId::Null,
        )
        .unwrap(),
        None,
        "stale Unix object metadata update must not publish tags"
    );
    let remote_bucket_pg = remote_primary
        .get_pg(current_cluster.test_bucket_pg_id_for(&bucket))
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*remote_bucket_pg, &bucket)
            .unwrap()
            .is_empty(),
        "stale Unix object metadata update must not leave bucket write reservations"
    );
}

#[test]
fn non_current_epoch_unix_object_delete_fails_closed_without_remote_mutation() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let current_epoch = ClusterEpoch::INITIAL;
    let stale_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
    let mut map = LocalClusterMap::open(
        &tmp.path().join("frontend-object-delete"),
        &node_ids,
        &pg_ids,
        ec_shape,
    )
    .unwrap();
    for pg_id in pg_ids {
        set_route_primary(&mut map, pg_id, NodeId::new(0));
    }
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };

    let mut server_configs = Vec::new();
    let mut client_configs = Vec::new();
    for node_id in node_ids {
        let socket_path = tmp.path().join("sockets").join(format!(
            "stale-object-delete-node-{}.sock",
            node_id.as_u32()
        ));
        private_socket_dir(socket_path.parent().unwrap());
        let pg_routes = pg_ids
            .iter()
            .map(|pg_id| StorageNodePgRoute {
                pg_id: *pg_id,
                cluster_epoch: current_epoch,
                state: PgState::Active,
                primary_node_id: NodeId::new(0),
                acting_set: node_ids.to_vec(),
            })
            .collect();
        server_configs.push(StorageNodeProcessConfig {
            node_id,
            cluster_epoch: current_epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: tmp
                .path()
                .join(format!("remote-stale-object-delete-{}", node_id.as_u32())),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.to_vec(),
            socket_path: socket_path.clone(),
            pg_routes,
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: Vec::new(),
        });
        client_configs.push(LocalUnixStorageNodeClientConfig::new(node_id, socket_path));
    }
    let mut _server_guards = Vec::new();
    for config in server_configs.iter().cloned() {
        let server = StorageNodeServer::bind(config).unwrap();
        _server_guards.push(spawn_storage_node_server(server));
    }
    map.install_unix_storage_node_clients(client_configs)
        .unwrap();
    let map = Arc::new(map);
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let committed = write_committed_direct_segment_for(
        &current_cluster,
        &bucket,
        &key,
        b"stale unix object delete",
    );
    let before_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    let stale_cluster =
        crate::StorageCluster::test_from_local_map_with_epoch(Arc::clone(&map), stale_epoch)
            .unwrap();

    let err = stale_cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == stale_epoch
                && observed_current_epoch == current_epoch
        ),
        "stale Unix object delete should fail closed before command build, got {err:?}"
    );

    let after_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof, before_object_pg_proof,
        "stale Unix object delete must not append an object-PG command"
    );

    let remote_primary_config = server_configs
        .iter()
        .find(|config| config.node_id == NodeId::new(0))
        .unwrap();
    let remote_primary = SharedStorageNode::open_with_default_ec_shape(
        &remote_primary_config.data_dir,
        &remote_primary_config.pg_ids,
        remote_primary_config.default_ec_shape,
    )
    .unwrap();
    let remote_object_pg = remote_primary.get_pg(object_pg).unwrap();
    assert!(
        remote_object_pg
            .pending_metadata_command_slot(NodeId::new(0).as_u32(), current_epoch)
            .unwrap()
            .is_none(),
        "stale Unix object delete must not leave a remote pending command"
    );
    let stored = crate::PgMetadataStore::get_object_meta(&*remote_object_pg, &bucket, &key)
        .unwrap()
        .into_live()
        .unwrap();
    assert_eq!(stored.generation_id, committed.generation_id);
    assert!(
        !crate::PgMetadataStore::payload_reclaim_exists(
            &*remote_object_pg,
            &bucket,
            &key,
            committed.generation_id
        )
        .unwrap(),
        "stale Unix object delete must not publish reclaim metadata"
    );
    let remote_bucket_pg = remote_primary
        .get_pg(current_cluster.test_bucket_pg_id_for(&bucket))
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*remote_bucket_pg, &bucket)
            .unwrap()
            .is_empty(),
        "stale Unix object delete must not leave bucket write reservations"
    );
}

#[test]
fn historical_payload_shard_inspection_can_route_to_unix_storage_node_client() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let target_node = NodeId::new(1);
    let ec_shape = EcShape { k: 2, m: 1 };
    let current_epoch = ClusterEpoch::new(8).unwrap();
    let mut map = LocalClusterMap::open_frontend_topology_only_with_epoch(
        NodeId::new(0),
        node_ids,
        &[0],
        ec_shape,
        current_epoch,
    )
    .unwrap();
    let socket_path = tmp.path().join("sockets").join("historical-node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let remote_data_dir = tmp.path().join("historical-remote-node-1");
    let data_pg_id = DataPgId::new_for_test(PgId::new(0));
    let historical_epoch = ClusterEpoch::new(7).unwrap();
    let historical_route = crate::control_plane::PgRouteSnapshot::reconstructed(
        historical_epoch,
        data_pg_id.pg_id(),
        NodeId::new(0),
        node_ids.to_vec(),
        PgState::Active,
    );
    map.test_install_historical_pg_routes([historical_route.clone()]);
    let location = ShardLocation::new(
        historical_epoch,
        data_pg_id,
        ShardIndex::new(0),
        target_node,
    );
    let shard_key = ShardKey::new(&[0x7b; 16], 3, location.shard_index().get());
    let payload = b"historical shard inspection over unix";
    let remote = SharedStorageNode::open_with_default_ec_shape(
        &remote_data_dir,
        &[data_pg_id.get()],
        ec_shape,
    )
    .unwrap();
    let ack = remote
        .write_shard_file(data_pg_id.get(), &shard_key, payload)
        .unwrap();

    let server = Arc::new(
        StorageNodeServer::bind(StorageNodeProcessConfig {
            node_id: target_node,
            cluster_epoch: current_epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: remote_data_dir,
            default_ec_shape: ec_shape,
            pg_ids: vec![data_pg_id.get()],
            socket_path: socket_path.clone(),
            pg_routes: vec![StorageNodePgRoute {
                pg_id: data_pg_id.get(),
                cluster_epoch: current_epoch,
                state: PgState::Active,
                primary_node_id: NodeId::new(0),
                acting_set: node_ids.to_vec(),
            }],

            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: vec![StorageNodePgRoute {
                pg_id: historical_route.pg_id().get(),
                cluster_epoch: historical_route.cluster_epoch(),
                state: historical_route.state(),
                primary_node_id: historical_route.primary_node_id(),
                acting_set: historical_route.acting_set().to_vec(),
            }],
        })
        .unwrap(),
    );
    let server_thread = {
        let server = Arc::clone(&server);
        thread::spawn(move || server.accept_one().unwrap())
    };
    map.install_unix_shard_clients([LocalUnixShardNodeClientConfig::new(
        target_node,
        socket_path,
    )])
    .unwrap();

    let observed = map
        .read_payload_shard_for_historical_inspection(location, &shard_key, ack)
        .unwrap();

    assert_eq!(observed, payload);
    let local_read = map
        .node(target_node)
        .unwrap()
        .storage_node()
        .read_shard_file(data_pg_id.get(), &shard_key);
    assert!(
        matches!(
            local_read,
            Err(StoreError::NotFound | StoreError::PgNotFound { .. })
        ),
        "frontend-local shard should remain absent: {local_read:?}"
    );
    server_thread.join().unwrap();
}

#[test]
fn cross_epoch_segment_read_uses_retained_route_over_unix_storage_nodes() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let old_acting_set = vec![NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let new_acting_set = vec![NodeId::new(3), NodeId::new(4), NodeId::new(5)];
    let pg_ids = [0];
    let ec_shape = EcShape { k: 2, m: 1 };
    let current_epoch = ClusterEpoch::INITIAL;
    let next_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
    let old_route = PgRouteSnapshot::reconstructed(
        current_epoch,
        PgId::new(0),
        NodeId::new(0),
        old_acting_set,
        PgState::Active,
    );
    let new_route = PgRouteSnapshot::reconstructed(
        next_epoch,
        PgId::new(0),
        NodeId::new(3),
        new_acting_set,
        PgState::Active,
    );
    let mut current_map = LocalClusterMap::open(
        &tmp.path().join("frontend-current-read"),
        &node_ids,
        &pg_ids,
        ec_shape,
    )
    .unwrap();
    current_map.test_install_pg_routes([old_route.clone()]);

    let mut server_configs = Vec::new();
    let mut shard_client_configs = Vec::new();
    for node_id in node_ids {
        let socket_path = tmp
            .path()
            .join("sockets")
            .join(format!("cross-epoch-read-node-{}.sock", node_id.as_u32()));
        private_socket_dir(socket_path.parent().unwrap());
        server_configs.push(StorageNodeProcessConfig {
            node_id,
            cluster_epoch: current_epoch,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: tmp
                .path()
                .join(format!("cross-epoch-read-remote-{}", node_id.as_u32())),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.to_vec(),
            socket_path: socket_path.clone(),
            pg_routes: vec![StorageNodePgRoute::from(&old_route)],
            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: Vec::new(),
        });
        shard_client_configs.push(LocalUnixShardNodeClientConfig::new(node_id, socket_path));
    }
    let mut _server_guards = Vec::new();
    for config in server_configs.iter().cloned() {
        let server = StorageNodeServer::bind(config).unwrap();
        _server_guards.push(spawn_storage_node_server(server));
    }
    current_map
        .install_unix_shard_clients(shard_client_configs.clone())
        .unwrap();
    let current_cluster = crate::StorageCluster::from_local_map(Arc::new(current_map)).unwrap();
    let committed =
        write_committed_direct_segment(&current_cluster, b"cross epoch historical unix read");
    let request = crate::SegmentStoredBytesRequest {
        data_pg_id: committed.written.data_pg_id,
        segment_okh: committed.segment_okh,
        segment_vid: committed.generation_id,
        stored_size: committed.payload.len(),
        segment_crc64: checksum::crc64::checksum(&committed.payload),
        ec: committed.written.ec,
    };

    let mut next_map = LocalClusterMap::open_frontend_topology_only_with_pg_routes(
        NodeId::new(3),
        node_ids,
        &pg_ids,
        ec_shape,
        next_epoch,
        [LocalPgRoute::from(&new_route)],
    )
    .unwrap();
    next_map.test_install_historical_pg_routes([old_route]);
    next_map
        .install_unix_shard_clients(shard_client_configs)
        .unwrap();
    let next_cluster = crate::StorageCluster::from_local_map(Arc::new(next_map)).unwrap();

    let mut current_route_read = Vec::new();
    let current_route_error = next_cluster
        .read_segment_payload_stored_bytes_into(request, &mut current_route_read)
        .unwrap_err();
    assert!(
        matches!(
            current_route_error,
            StoreError::NotFound | StoreError::StorageRpc { .. }
        ),
        "current next-epoch route should fail before reading old placed bytes: {current_route_error:?}"
    );

    let mut historical_read = Vec::new();
    next_cluster
        .read_segment_payload_stored_bytes_at_placement_epoch_into(
            current_epoch,
            request,
            &mut historical_read,
        )
        .unwrap();
    assert_eq!(historical_read, committed.payload);

    for written in &committed.written.written_shards {
        let location = committed
            .locations
            .iter()
            .copied()
            .find(|location| location.shard_index() == written.key.shard_index())
            .unwrap();
        let config = server_configs
            .iter()
            .find(|config| config.node_id == location.node_id())
            .unwrap();
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        assert_eq!(
            remote
                .read_shard_file(committed.written.data_pg_id, &written.key)
                .unwrap()
                .len() as u64,
            written.ack.stored_size
        );
    }
}

#[test]
fn remote_shard_files_without_ack_rows_are_not_publishable() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let frontend_dir = tmp.path().join("frontend");
    let mut map = LocalClusterMap::open(&frontend_dir, &node_ids, &[0], ec_shape).unwrap();
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let generation_id = crate::GenerationId::new(1).unwrap();
    let segment_okh = [0x91; 16];
    let data_pg = map.object_generation_segment_data_pg(&bucket, &key, generation_id, 0);
    let placement_key =
        super::super::super::segment_payload_placement_key(&segment_okh, generation_id);
    let locations = map
        .place_payload_shards(ClusterEpoch::INITIAL, data_pg, ec_shape, &placement_key)
        .unwrap();
    let mut expected_connections_by_node = BTreeMap::<NodeId, usize>::new();
    for location in &locations {
        *expected_connections_by_node
            .entry(location.node_id())
            .or_default() += 1;
    }
    *expected_connections_by_node
        .entry(NodeId::new(0))
        .or_default() += 1;

    let mut server_configs = Vec::new();
    let mut shard_client_configs = Vec::new();
    for node_id in node_ids {
        let socket_path = tmp
            .path()
            .join("sockets")
            .join(format!("node-{}.sock", node_id.as_u32()));
        private_socket_dir(socket_path.parent().unwrap());
        server_configs.push(StorageNodeProcessConfig {
            node_id,
            cluster_epoch: ClusterEpoch::INITIAL,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: tmp
                .path()
                .join(format!("remote-node-missing-ack-{}", node_id.as_u32())),
            default_ec_shape: ec_shape,
            pg_ids: vec![0],
            socket_path: socket_path.clone(),
            pg_routes: vec![StorageNodePgRoute {
                pg_id: 0,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: PgState::Active,
                primary_node_id: NodeId::new(0),
                acting_set: node_ids.to_vec(),
            }],

            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: Vec::new(),
        });
        shard_client_configs.push(LocalUnixShardNodeClientConfig::new(node_id, socket_path));
    }
    let mut server_threads = Vec::new();
    for config in server_configs.iter().cloned() {
        let expected_connections = *expected_connections_by_node
            .get(&config.node_id)
            .unwrap_or(&0);
        let server = StorageNodeServer::bind(config).unwrap();
        server_threads.push(thread::spawn(move || {
            for _ in 0..expected_connections {
                server.accept_one().unwrap();
            }
        }));
    }
    map.install_unix_shard_clients(shard_client_configs)
        .unwrap();
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let direct_written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            b"remote shard file without ack row",
        )
        .unwrap();
    let shard_batch: Vec<(&ShardKey, WriteAck)> = direct_written
        .written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect();

    let err = cluster
        .validate_payload_shard_acks(
            direct_written.data_pg_id,
            direct_written.ec,
            &segment_okh,
            generation_id,
            &shard_batch,
        )
        .unwrap_err();
    assert!(
        matches!(err, crate::ObjectPgActionError::Store(StoreError::NotFound)),
        "missing remote ack row should fail publish validation, got {err:?}"
    );

    for thread in server_threads {
        thread.join().unwrap();
    }
    for written in &direct_written.written_shards {
        let location = locations
            .iter()
            .copied()
            .find(|location| location.shard_index() == written.key.shard_index())
            .unwrap();
        let remote_config = server_configs
            .iter()
            .find(|config| config.node_id == location.node_id())
            .unwrap();
        let remote = SharedStorageNode::open_with_default_ec_shape(
            &remote_config.data_dir,
            &remote_config.pg_ids,
            remote_config.default_ec_shape,
        )
        .unwrap();
        assert_eq!(
            remote
                .read_shard_file(direct_written.data_pg_id, &written.key)
                .unwrap()
                .len() as u64,
            written.ack.stored_size
        );
    }
    let remote_primary = SharedStorageNode::open_with_default_ec_shape(
        &server_configs[0].data_dir,
        &server_configs[0].pg_ids,
        server_configs[0].default_ec_shape,
    )
    .unwrap();
    let remote_pg = remote_primary.get_pg(direct_written.data_pg_id).unwrap();
    for written in &direct_written.written_shards {
        assert!(matches!(
            remote_pg.validate_written_shard_ack(&written.key, written.ack),
            Err(StoreError::NotFound)
        ));
    }
}

#[test]
fn remote_shard_ack_rows_on_wrong_node_are_not_publishable() {
    let (_unix_client_test_guard, tmp) = unix_client_tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let wrong_ack_node = NodeId::new(1);
    let ec_shape = EcShape { k: 2, m: 1 };
    let frontend_dir = tmp.path().join("frontend");
    let mut map = LocalClusterMap::open(&frontend_dir, &node_ids, &[0], ec_shape).unwrap();
    set_route_primary(&mut map, 0, NodeId::new(0));
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let generation_id = crate::GenerationId::new(1).unwrap();
    let segment_okh = [0x92; 16];
    let data_pg = map.object_generation_segment_data_pg(&bucket, &key, generation_id, 0);
    let placement_key =
        super::super::super::segment_payload_placement_key(&segment_okh, generation_id);
    let locations = map
        .place_payload_shards(ClusterEpoch::INITIAL, data_pg, ec_shape, &placement_key)
        .unwrap();
    let mut expected_connections_by_node = BTreeMap::<NodeId, usize>::new();
    for location in &locations {
        *expected_connections_by_node
            .entry(location.node_id())
            .or_default() += 1;
    }
    *expected_connections_by_node
        .entry(NodeId::new(0))
        .or_default() += 1;

    let mut server_configs = Vec::new();
    let mut shard_client_configs = Vec::new();
    for node_id in node_ids {
        let socket_path = tmp
            .path()
            .join("sockets")
            .join(format!("wrong-ack-node-{}.sock", node_id.as_u32()));
        private_socket_dir(socket_path.parent().unwrap());
        server_configs.push(StorageNodeProcessConfig {
            node_id,
            cluster_epoch: ClusterEpoch::INITIAL,
            route_map_validity: RouteMapValidity::Forever,
            data_dir: tmp
                .path()
                .join(format!("remote-node-wrong-ack-{}", node_id.as_u32())),
            default_ec_shape: ec_shape,
            pg_ids: vec![0],
            socket_path: socket_path.clone(),
            pg_routes: vec![StorageNodePgRoute {
                pg_id: 0,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: PgState::Active,
                primary_node_id: NodeId::new(0),
                acting_set: node_ids.to_vec(),
            }],

            pending_metadata_command_recoveries: Vec::new(),
            historical_pg_routes: Vec::new(),
        });
        shard_client_configs.push(LocalUnixShardNodeClientConfig::new(node_id, socket_path));
    }
    let mut server_threads = Vec::new();
    for config in server_configs.iter().cloned() {
        let expected_connections = *expected_connections_by_node
            .get(&config.node_id)
            .unwrap_or(&0);
        let server = StorageNodeServer::bind(config).unwrap();
        server_threads.push(thread::spawn(move || {
            for _ in 0..expected_connections {
                server.accept_one().unwrap();
            }
        }));
    }
    map.install_unix_shard_clients(shard_client_configs)
        .unwrap();
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let direct_written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            b"remote shard ack row on wrong node",
        )
        .unwrap();
    let shard_batch: Vec<(&ShardKey, WriteAck)> = direct_written
        .written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect();
    let wrong_config = server_configs
        .iter()
        .find(|config| config.node_id == wrong_ack_node)
        .unwrap();
    {
        let wrong_node = SharedStorageNode::open_with_default_ec_shape(
            &wrong_config.data_dir,
            &wrong_config.pg_ids,
            wrong_config.default_ec_shape,
        )
        .unwrap();
        let wrong_pg = wrong_node.get_pg(direct_written.data_pg_id).unwrap();
        wrong_pg
            .register_written_shards_batch_exact(&shard_batch)
            .unwrap();
    }

    let err = cluster
        .validate_payload_shard_acks(
            direct_written.data_pg_id,
            direct_written.ec,
            &segment_okh,
            generation_id,
            &shard_batch,
        )
        .unwrap_err();
    assert!(
        matches!(err, crate::ObjectPgActionError::Store(StoreError::NotFound)),
        "wrong-node remote ack rows should fail publish validation, got {err:?}"
    );

    for thread in server_threads {
        thread.join().unwrap();
    }
    let wrong_node = SharedStorageNode::open_with_default_ec_shape(
        &wrong_config.data_dir,
        &wrong_config.pg_ids,
        wrong_config.default_ec_shape,
    )
    .unwrap();
    let wrong_pg = wrong_node.get_pg(direct_written.data_pg_id).unwrap();
    for written in &direct_written.written_shards {
        wrong_pg
            .validate_written_shard_ack(&written.key, written.ack)
            .unwrap();
    }
    let primary_config = server_configs
        .iter()
        .find(|config| config.node_id == NodeId::new(0))
        .unwrap();
    let primary = SharedStorageNode::open_with_default_ec_shape(
        &primary_config.data_dir,
        &primary_config.pg_ids,
        primary_config.default_ec_shape,
    )
    .unwrap();
    let primary_pg = primary.get_pg(direct_written.data_pg_id).unwrap();
    for written in &direct_written.written_shards {
        assert!(matches!(
            primary_pg.validate_written_shard_ack(&written.key, written.ack),
            Err(StoreError::NotFound)
        ));
    }
}
