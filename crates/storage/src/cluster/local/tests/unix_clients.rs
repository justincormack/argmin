use super::*;
use crate::{
    PlacedSegmentShardBackfillClaimAcquire, PlacedSegmentShardBackfillClaimRecord,
    PlacedSegmentShardBackfillRecord, PlacedSegmentShardBackfillWorkItem,
    PlacedSegmentShardRepairClaimAcquire, PlacedSegmentShardRepairClaimRecord,
    PlacedSegmentShardRepairRecord,
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

#[test]
fn payload_shard_writes_route_through_pluggable_shard_client() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
    let recording_client = Arc::new(RecordingPlacedShardClient::new(NodeId::new(1)));
    let recording_client_for_assert = Arc::clone(&recording_client);
    map.replace_shard_client_for_tests(NodeId::new(1), recording_client);

    let data_pg_id = DataPgId::new(PgId::new(0));
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
    records: Mutex<Vec<(PgId, ShardKey, WriteAck)>>,
    validates: Mutex<Vec<(PgId, ShardKey, WriteAck)>>,
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
        pg_id: PgId,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), StoreError> {
        let mut records = self.records.lock().unwrap_or_else(|e| e.into_inner());
        for (key, ack) in shard_batch {
            records.push((pg_id, (*key).clone(), *ack));
        }
        Ok(())
    }

    fn validate_written_shard_ack(
        &self,
        pg_id: PgId,
        key: &ShardKey,
        ack: WriteAck,
    ) -> Result<(), StoreError> {
        self.validates
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((pg_id, key.clone(), ack));
        Ok(())
    }

    fn load_written_shard_ack(
        &self,
        _pg_id: PgId,
        _key: &ShardKey,
    ) -> Result<WriteAck, StoreError> {
        Err(StoreError::NotFound)
    }

    fn delete_written_shard_ack(&self, _pg_id: PgId, _key: &ShardKey) -> Result<(), StoreError> {
        Err(StoreError::NotFound)
    }

    fn record_placed_segment_shard_repair(
        &self,
        _pg_id: PgId,
        _work_item: &PlacedSegmentShardRepairWorkItem,
        _last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        Ok(())
    }

    fn list_placed_segment_shard_repairs(
        &self,
        _pg_id: PgId,
    ) -> Result<Vec<PlacedSegmentShardRepairRecord>, StoreError> {
        Ok(Vec::new())
    }

    fn acquire_placed_segment_shard_repair_claim(
        &self,
        _pg_id: PgId,
        _request: &PlacedSegmentShardRepairClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardRepairClaimRecord>, StoreError> {
        Ok(None)
    }

    fn complete_placed_segment_shard_repair_claim(
        &self,
        _pg_id: PgId,
        _cluster_epoch: ClusterEpoch,
        _claim: &PlacedSegmentShardRepairClaimRecord,
    ) -> Result<bool, StoreError> {
        Ok(false)
    }

    fn record_placed_segment_shard_repair_claim_error(
        &self,
        _pg_id: PgId,
        _cluster_epoch: ClusterEpoch,
        _claim: &PlacedSegmentShardRepairClaimRecord,
        _last_error: &str,
        _next_attempt_after: u64,
    ) -> Result<bool, StoreError> {
        Ok(false)
    }

    fn resolve_placed_segment_shard_repair(
        &self,
        _pg_id: PgId,
        _work_item: &PlacedSegmentShardRepairWorkItem,
    ) -> Result<(), StoreError> {
        Ok(())
    }

    fn record_placed_segment_shard_backfill(
        &self,
        _pg_id: PgId,
        _work_item: &PlacedSegmentShardBackfillWorkItem,
        _remaining_tolerance: u8,
        _last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        Ok(())
    }

    fn list_placed_segment_shard_backfills(
        &self,
        _pg_id: PgId,
    ) -> Result<Vec<PlacedSegmentShardBackfillRecord>, StoreError> {
        Ok(Vec::new())
    }

    fn count_placed_segment_shard_backfills(&self, _pg_id: PgId) -> Result<usize, StoreError> {
        Ok(0)
    }

    fn acquire_placed_segment_shard_backfill_claim(
        &self,
        _pg_id: PgId,
        _request: &PlacedSegmentShardBackfillClaimAcquire,
    ) -> Result<Option<PlacedSegmentShardBackfillClaimRecord>, StoreError> {
        Ok(None)
    }

    fn complete_placed_segment_shard_backfill_claim(
        &self,
        _pg_id: PgId,
        _cluster_epoch: ClusterEpoch,
        _claim: &PlacedSegmentShardBackfillClaimRecord,
    ) -> Result<bool, StoreError> {
        Ok(false)
    }

    fn record_placed_segment_shard_backfill_claim_error(
        &self,
        _pg_id: PgId,
        _cluster_epoch: ClusterEpoch,
        _claim: &PlacedSegmentShardBackfillClaimRecord,
        _last_error: &str,
        _next_attempt_after: u64,
    ) -> Result<bool, StoreError> {
        Ok(false)
    }

    fn resolve_placed_segment_shard_backfill(
        &self,
        _pg_id: PgId,
        _work_item: &PlacedSegmentShardBackfillWorkItem,
    ) -> Result<(), StoreError> {
        Ok(())
    }
}

#[test]
fn metadata_pg_primary_exposes_pluggable_shard_ack_client() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let recording_client = Arc::new(RecordingShardAckClient::new());
    let recording_client_for_assert = Arc::clone(&recording_client);
    map.replace_shard_ack_client_for_tests(NodeId::new(2), recording_client);
    set_route_primary(&mut map, 1, NodeId::new(2));

    let pg_id = PgId::new(1);
    let key = ShardKey::new(&[0x51; 16], 88, 0);
    let ack = WriteAck {
        crc64: 1234,
        stored_size: 5678,
    };
    let node = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    node.shard_ack_client()
        .register_written_shard_acks(pg_id, &[(&key, ack)])
        .unwrap();
    node.shard_ack_client()
        .validate_written_shard_ack(pg_id, &key, ack)
        .unwrap();

    assert_eq!(
        *recording_client_for_assert
            .records
            .lock()
            .unwrap_or_else(|e| e.into_inner()),
        vec![(pg_id, key.clone(), ack)]
    );
    assert_eq!(
        *recording_client_for_assert
            .validates
            .lock()
            .unwrap_or_else(|e| e.into_inner()),
        vec![(pg_id, key, ack)]
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
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
    let ok_client = Arc::new(RecordingReadHandleClient::new(false));
    let ok_events = Arc::clone(&ok_client.events);
    let failing_client = Arc::new(RecordingReadHandleClient::new(true));
    let failing_events = Arc::clone(&failing_client.events);
    map.replace_shard_read_handle_client_for_tests(NodeId::new(1), ok_client);
    map.replace_shard_read_handle_client_for_tests(NodeId::new(2), failing_client);

    let data_pg_id = DataPgId::new(PgId::new(0));
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
    let tmp = test_util::tempdir();
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
        route_map_valid_until_ms: None,
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

    let data_pg_id = DataPgId::new(PgId::new(0));
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
        .register_written_shard_acks(PgId::new(0), &[(&key, ack)])
        .unwrap();
    primary
        .shard_ack_client()
        .validate_written_shard_ack(PgId::new(0), &key, ack)
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
        .list_scavenger_shard_rows(PgId::new(0))
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
            PgId::new(0),
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
        .list_shard_scavenger_observations(PgId::new(0))
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
        .resolve_shard_scavenger_observation(PgId::new(0), &observation_key)
        .unwrap();
    let missing_key = ShardKey::new(&[0x62; 16], 100, 0);
    let err = map
        .read_payload_shard(ClusterEpoch::INITIAL, location, &missing_key, ack)
        .unwrap_err();
    assert!(matches!(
        err,
        ShardIoError::Store {
            source: StoreError::StorageRpc {
                operation: "shard read",
                ..
            },
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
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let remote_data_dir = tmp.path().join("remote-node-1-owned");
    let socket_path = tmp.path().join("sockets").join("owned-node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let server_config = StorageNodeProcessConfig {
        node_id: NodeId::new(1),
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_valid_until_ms: None,
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

    let data_pg_id = DataPgId::new(PgId::new(0));
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
    let tmp = test_util::tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let remote_data_dir = tmp.path().join("remote-metadata-node-1-owned");
    let socket_path = tmp.path().join("sockets").join("metadata-node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let server_config = StorageNodeProcessConfig {
        node_id,
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_valid_until_ms: None,
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
    let tmp = test_util::tempdir();
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
            route_map_valid_until_ms: None,
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
        });
        client_configs.push(LocalUnixStorageNodeClientConfig::new(node_id, socket_path));
    }

    for config in server_configs {
        let server = StorageNodeServer::bind(config).unwrap();
        let _server_thread = thread::spawn(move || server.serve_forever().unwrap());
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
    let proof = crate::control_plane::PgMetadataProof::new(
        primary_state.applied_log_index,
        primary_state.applied_log_hash,
        primary_state.state_digest,
    );
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
    let tmp = test_util::tempdir();
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
        route_map_valid_until_ms: None,
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
    };
    let server = StorageNodeServer::bind(server_config.clone()).unwrap();
    assert!(remote_data_dir.join(".argmin-storage-node.lock").is_file());
    let _server_thread = thread::spawn(move || server.serve_forever().unwrap());

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
    let tmp = test_util::tempdir();
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
        route_map_valid_until_ms: None,
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
    };
    let server = StorageNodeServer::bind(server_config).unwrap();
    assert!(remote_data_dir.join(".argmin-storage-node.lock").is_file());
    let _server_thread = thread::spawn(move || server.serve_forever().unwrap());

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
    first_cluster.begin_bucket_delete(&bucket).unwrap();
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
        Some(crate::ReclaimWorkItem::BucketDelete(queued_bucket))
            if queued_bucket == bucket
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
            .head_bucket_raw(PgId::new(0), &bucket),
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
fn frontend_unix_lifecycle_claims_resume_from_storage_node_owned_rows() {
    let tmp = test_util::tempdir();
    let node_id = NodeId::new(1);
    let ec_shape = EcShape { k: 1, m: 0 };
    let remote_data_dir = tmp.path().join("remote-lifecycle-node-1");
    let socket_path = tmp.path().join("sockets").join("lifecycle-node-1.sock");
    private_socket_dir(socket_path.parent().unwrap());
    let server_config = StorageNodeProcessConfig {
        node_id,
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_valid_until_ms: None,
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
    };
    let server = StorageNodeServer::bind(server_config).unwrap();
    assert!(remote_data_dir.join(".argmin-storage-node.lock").is_file());
    let _server_thread = thread::spawn(move || server.serve_forever().unwrap());

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
    let tmp = test_util::tempdir();
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
        route_map_valid_until_ms: None,
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
    let _server_thread = thread::spawn(move || server.serve_forever().unwrap());

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
fn frontend_unix_stream_session_scavenger_rejects_wrong_pg_rows() {
    let tmp = test_util::tempdir();
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
        route_map_valid_until_ms: None,
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
    let _server_thread = thread::spawn(move || server.serve_forever().unwrap());

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
    let tmp = test_util::tempdir();
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
        route_map_valid_until_ms: None,
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
        (generation, identity)
    };
    let server = StorageNodeServer::bind(server_config).unwrap();
    let _server_thread = thread::spawn(move || server.serve_forever().unwrap());

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
    let tmp = test_util::tempdir();
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
        bucket_abac_enabled: false,
        encryption: crate::types::EffectiveBucketEncryptionConfig::default(),
    }
}

#[test]
fn frontend_unix_object_generation_mode_reserves_on_storage_node() {
    let tmp = test_util::tempdir();
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
        route_map_valid_until_ms: None,
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
    };
    let server = StorageNodeServer::bind(server_config.clone()).unwrap();
    assert!(remote_data_dir.join(".argmin-storage-node.lock").is_file());
    let _server_thread = thread::spawn(move || server.serve_forever().unwrap());

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
    let tmp = test_util::tempdir();
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
        route_map_valid_until_ms: None,
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
    };
    let server = StorageNodeServer::bind(server_config.clone()).unwrap();
    assert!(remote_data_dir.join(".argmin-storage-node.lock").is_file());
    let _server_thread = thread::spawn(move || server.serve_forever().unwrap());

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
    let tmp = test_util::tempdir();
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
        route_map_valid_until_ms: None,
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
    }
    let server = StorageNodeServer::bind(server_config.clone()).unwrap();
    assert!(remote_data_dir.join(".argmin-storage-node.lock").is_file());
    let _server_thread = thread::spawn(move || server.serve_forever().unwrap());

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
        .heartbeat_durable_bucket_write_drain(PgId::new(drain.pg_id), &stale_drain, 200)
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
    let tmp = test_util::tempdir();
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
        route_map_valid_until_ms: None,
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
    }
    let server = StorageNodeServer::bind(server_config).unwrap();
    let _server_thread = thread::spawn(move || server.serve_forever().unwrap());

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
    let tmp = test_util::tempdir();
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
    }
    let server_config = StorageNodeProcessConfig {
        node_id,
        cluster_epoch: ClusterEpoch::INITIAL,
        route_map_valid_until_ms: None,
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
    };
    let server = StorageNodeServer::bind(server_config).unwrap();
    let _server_thread = thread::spawn(move || server.serve_forever().unwrap());

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
        .load_stream_upload_session(PgId::new(0), &bucket, &key, &session_id)
        .unwrap();
    assert_eq!(loaded.session_id, session_id);
    let segments = mutation_client
        .load_stream_upload_segments(PgId::new(0), &bucket, &key, &loaded.session_id)
        .unwrap();
    assert!(segments.is_empty());
    let (target, segment) = mutation_client
        .prepare_stream_segment_append(
            PgId::new(0),
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
    let tmp = test_util::tempdir();
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
        route_map_valid_until_ms: None,
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
    };
    let server = StorageNodeServer::bind(server_config.clone()).unwrap();
    let _server_thread = thread::spawn(move || server.serve_forever().unwrap());

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
    let tmp = test_util::tempdir();
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
        route_map_valid_until_ms: None,
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
    };
    let server = StorageNodeServer::bind(server_config.clone()).unwrap();
    let _server_thread = thread::spawn(move || server.serve_forever().unwrap());

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
    let tmp = test_util::tempdir();
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
    let tmp = test_util::tempdir();
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
    let tmp = test_util::tempdir();
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
    let tmp = test_util::tempdir();
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
    let tmp = test_util::tempdir();
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
    let tmp = test_util::tempdir();
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
    let tmp = test_util::tempdir();
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
    let tmp = test_util::tempdir();
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
fn unix_shard_client_install_rejects_relative_socket_paths_before_mutation() {
    let tmp = test_util::tempdir();
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

    let data_pg_id = DataPgId::new(PgId::new(0));
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
    let tmp = test_util::tempdir();
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

    let data_pg_id = DataPgId::new(PgId::new(0));
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
                context: "connect storage-node RPC socket",
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
                context: "connect storage-node read-handle RPC socket",
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
    let tmp = test_util::tempdir();
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
            route_map_valid_until_ms: None,
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
fn remote_shard_files_without_ack_rows_are_not_publishable() {
    let tmp = test_util::tempdir();
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
            route_map_valid_until_ms: None,
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
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "shard ack validate",
                ..
            })
        ),
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
    let tmp = test_util::tempdir();
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
            route_map_valid_until_ms: None,
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
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "shard ack validate",
                ..
            })
        ),
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
