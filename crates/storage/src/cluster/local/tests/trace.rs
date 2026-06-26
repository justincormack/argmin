use super::*;

#[derive(Debug, Clone)]
enum LocalClusterTraceOp {
    AdvanceEpoch,
    SetPgState(PgState),
    WriteCurrent(u8),
    WriteStale(u8),
    ReadCurrent,
    ReadStale,
    DeleteCurrent,
    DeleteStale,
    MetadataOperationStale(u8),
    ZeroSizeStalePayloadRead,
    QueueCurrent(u8),
    QueueStale(u8),
    LeaseReleaseAcrossEpoch(u8),
    RecoverAfterPhysicalShardLoss(u8),
    DurableRepairQueueAfterShardCorruption(u8),
    RetainedRouteHistoricalRead(u8),
    DurableBackfillClaimAfterRouteChange(u8),
    StaleDirectPutCleanupAfterRouteChange(u8),
    RoutineMetadataCheckpointTick,
    DrainPendingCreateBucketFromSecondHandle(u8),
    RestartAndValidate,
    ReissueDuplicateCreateBucketIndex(u8),
}

#[derive(Debug, Clone)]
struct TraceWrittenShard {
    placement_key: Vec<u8>,
    location: ShardLocation,
    key: ShardKey,
    ack: WriteAck,
    data: Vec<u8>,
}

struct TracePlacedSegment {
    generation_id: crate::GenerationId,
    segment_okh: [u8; 16],
    payload: Vec<u8>,
    written: crate::DirectPutWrittenSegment,
}

fn local_cluster_trace_strategy() -> impl Strategy<Value = Vec<LocalClusterTraceOp>> {
    prop::collection::vec(
        prop_oneof![
            2 => Just(LocalClusterTraceOp::AdvanceEpoch),
            3 => pg_state_strategy().prop_map(LocalClusterTraceOp::SetPgState),
            5 => any::<u8>().prop_map(LocalClusterTraceOp::WriteCurrent),
            3 => any::<u8>().prop_map(LocalClusterTraceOp::WriteStale),
            4 => Just(LocalClusterTraceOp::ReadCurrent),
            3 => Just(LocalClusterTraceOp::ReadStale),
            3 => Just(LocalClusterTraceOp::DeleteCurrent),
            2 => Just(LocalClusterTraceOp::DeleteStale),
            3 => any::<u8>().prop_map(LocalClusterTraceOp::MetadataOperationStale),
            3 => Just(LocalClusterTraceOp::ZeroSizeStalePayloadRead),
            2 => any::<u8>().prop_map(LocalClusterTraceOp::QueueCurrent),
            2 => any::<u8>().prop_map(LocalClusterTraceOp::QueueStale),
            2 => any::<u8>().prop_map(LocalClusterTraceOp::LeaseReleaseAcrossEpoch),
            1 => any::<u8>().prop_map(LocalClusterTraceOp::RecoverAfterPhysicalShardLoss),
            1 => any::<u8>().prop_map(LocalClusterTraceOp::DurableRepairQueueAfterShardCorruption),
            1 => any::<u8>().prop_map(LocalClusterTraceOp::RetainedRouteHistoricalRead),
            1 => any::<u8>().prop_map(LocalClusterTraceOp::DurableBackfillClaimAfterRouteChange),
            1 => any::<u8>().prop_map(LocalClusterTraceOp::StaleDirectPutCleanupAfterRouteChange),
            1 => Just(LocalClusterTraceOp::RoutineMetadataCheckpointTick),
            1 => any::<u8>().prop_map(LocalClusterTraceOp::DrainPendingCreateBucketFromSecondHandle),
            1 => Just(LocalClusterTraceOp::RestartAndValidate),
            1 => any::<u8>().prop_map(LocalClusterTraceOp::ReissueDuplicateCreateBucketIndex),
        ],
        1..=40,
    )
}

fn pg_state_strategy() -> impl Strategy<Value = PgState> {
    prop_oneof![
        Just(PgState::Active),
        Just(PgState::Peering),
        Just(PgState::Degraded),
        Just(PgState::Backfilling),
        Just(PgState::Inconsistent),
    ]
}

pub(super) fn trace_node_ids() -> [NodeId; 6] {
    [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ]
}

fn trace_node_ids_with_spare() -> [NodeId; 7] {
    [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
        NodeId::new(6),
    ]
}

pub(super) fn current_cluster(map: &Arc<LocalClusterMap>) -> Arc<crate::StorageCluster> {
    crate::StorageCluster::from_local_map(Arc::clone(map)).unwrap()
}

fn stale_cluster(
    map: &Arc<LocalClusterMap>,
    current_epoch: ClusterEpoch,
) -> Arc<crate::StorageCluster> {
    crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(map),
        stale_epoch_for(current_epoch),
    )
    .unwrap()
}

fn stale_epoch_for(current_epoch: ClusterEpoch) -> ClusterEpoch {
    if current_epoch == ClusterEpoch::INITIAL {
        ClusterEpoch::new(2).unwrap()
    } else {
        ClusterEpoch::INITIAL
    }
}

fn set_trace_epoch(map: &mut Arc<LocalClusterMap>, epoch: ClusterEpoch) {
    let map = Arc::get_mut(map).expect("trace must not retain StorageCluster handles");
    map.epoch = epoch;
    for route in map.pg_routes.values_mut() {
        route.cluster_epoch = epoch;
    }
}

fn set_trace_pg_state(map: &mut Arc<LocalClusterMap>, state: PgState) {
    let map = Arc::get_mut(map).expect("trace must not retain StorageCluster handles");
    map.pg_routes.get_mut(&PgId::new(0)).unwrap().state = state;
}

fn trace_historical_source_acting_set() -> Vec<NodeId> {
    trace_node_ids().to_vec()
}

fn trace_historical_current_acting_set(seed: u8) -> Vec<NodeId> {
    let mut acting_set = trace_node_ids_with_spare().to_vec();
    let removed_index = usize::from(seed) % trace_node_ids().len();
    acting_set.remove(removed_index);
    acting_set
}

fn trace_bucket(seed: u8) -> crate::BucketName {
    crate::BucketName::try_from(format!("trace-bucket-{seed}")).unwrap()
}

fn trace_key(seed: u8) -> crate::ObjectKey {
    crate::ObjectKey::try_from(format!("trace-key-{seed}")).unwrap()
}

pub(super) fn trace_session(seed: u8) -> crate::SessionId {
    crate::SessionId::try_from(format!("{:032x}", u128::from(seed) + 1)).unwrap()
}

fn trace_session_for_step(step: usize, seed: u8) -> crate::SessionId {
    crate::SessionId::try_from(format!(
        "{:032x}",
        ((step as u128) << 8) | (u128::from(seed) + 1)
    ))
    .unwrap()
}

fn trace_generation(seed: u8) -> crate::GenerationId {
    crate::GenerationId::new(u64::from(seed) + 1).unwrap()
}

fn trace_segment_generation(step: usize, seed: u8) -> crate::GenerationId {
    crate::GenerationId::new(10_000 + step as u64 * 257 + u64::from(seed)).unwrap()
}

fn trace_shard_key(step: usize, seed: u8) -> ShardKey {
    ShardKey::new(&[seed.wrapping_add(1); 16], 10_000 + step as u64, 0)
}

fn trace_bucket_for_pg(
    map: &LocalClusterMap,
    target_pg_id: u32,
    prefix: &str,
) -> crate::BucketName {
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    bucket_for_pg(topology, target_pg_id, prefix)
}

fn trace_placement_key(step: usize, seed: u8) -> Vec<u8> {
    format!("trace-placement-{step}-{seed}").into_bytes()
}

fn trace_segment_payload_placement_key(
    segment_okh: &[u8; 16],
    segment_vid: crate::GenerationId,
) -> [u8; 24] {
    let mut key = [0u8; 24];
    key[..16].copy_from_slice(segment_okh);
    key[16..].copy_from_slice(&segment_vid.get().to_be_bytes());
    key
}

fn write_trace_placed_segment(
    cluster: &crate::StorageCluster,
    bucket: &crate::BucketName,
    key: &crate::ObjectKey,
    generation_id: crate::GenerationId,
    segment_okh: [u8; 16],
    payload: &[u8],
) -> Result<TracePlacedSegment, TestCaseError> {
    let written = cluster
        .write_direct_put_segment_payload_shards(
            bucket,
            key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    cluster
        .test_register_payload_shard_acks(written.data_pg_id, &written.written_shards)
        .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    Ok(TracePlacedSegment {
        generation_id,
        segment_okh,
        payload: payload.to_vec(),
        written,
    })
}

fn trace_segment_stored_bytes_request(
    segment: &TracePlacedSegment,
) -> crate::SegmentStoredBytesRequest {
    crate::SegmentStoredBytesRequest {
        data_pg_id: segment.written.data_pg_id,
        segment_okh: segment.segment_okh,
        segment_vid: segment.generation_id,
        stored_size: segment.payload.len(),
        segment_crc64: checksum::crc64::checksum(&segment.payload),
        ec: segment.written.ec,
    }
}

fn current_trace_location(
    cluster: &crate::StorageCluster,
    written: &TraceWrittenShard,
    ec_shape: EcShape,
) -> ShardLocation {
    cluster
        .place_payload_shards(
            DataPgId::new(PgId::new(0)),
            ec_shape,
            &written.placement_key,
        )
        .unwrap()[usize::from(written.key.shard_index().get())]
}

fn written_trace_location_for_epoch(
    written: &TraceWrittenShard,
    current_epoch: ClusterEpoch,
) -> ShardLocation {
    ShardLocation::new(
        current_epoch,
        written.location.data_pg_id(),
        written.location.shard_index(),
        written.location.node_id(),
    )
}

fn arbitrary_current_location(current_epoch: ClusterEpoch) -> ShardLocation {
    ShardLocation::new(
        current_epoch,
        DataPgId::new(PgId::new(0)),
        ShardIndex::new(0),
        NodeId::new(0),
    )
}

fn shard_file_present_on_any_trace_node(map: &LocalClusterMap, key: &ShardKey) -> bool {
    trace_node_ids_with_spare().into_iter().any(|node_id| {
        map.node(node_id)
            .unwrap()
            .storage_node()
            .read_shard_file(0, key)
            .is_ok()
    })
}

fn drain_trace_reclaim_work(cluster: &crate::StorageCluster) {
    while let Some(work) = cluster.try_take_reclaim_work() {
        if let crate::ReclaimWorkItem::ObjectPayload((bucket, key, generation_id)) = work {
            cluster.finish_object_payload_reclaim_work(&bucket, &key, generation_id);
        }
    }
}

fn assert_stale_metadata_operation_error(
    err: crate::ObjectPgActionError,
    current_epoch: ClusterEpoch,
) -> TestCaseResult {
    let expected = matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
            pg_id: 0,
            operation_epoch,
            current_epoch: err_current_epoch,
        }) if operation_epoch == stale_epoch_for(current_epoch)
            && err_current_epoch == current_epoch
    );
    prop_assert!(expected, "unexpected metadata operation error: {err:?}");
    Ok(())
}

fn run_local_cluster_trace(ops: &[LocalClusterTraceOp]) -> TestCaseResult {
    let tmp = test_util::tempdir();
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let node_ids = trace_node_ids_with_spare();
    let mut map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap());
    let mut current_epoch = ClusterEpoch::INITIAL;
    let mut visited_epochs = BTreeSet::from([current_epoch]);
    let mut pg_state = PgState::Active;
    let mut written = None::<TraceWrittenShard>;

    for (step, op) in ops.iter().enumerate() {
        match op {
            LocalClusterTraceOp::AdvanceEpoch => {
                current_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
                visited_epochs.insert(current_epoch);
                set_trace_epoch(&mut map, current_epoch);
            }
            LocalClusterTraceOp::SetPgState(state) => {
                pg_state = *state;
                set_trace_pg_state(&mut map, pg_state);
            }
            LocalClusterTraceOp::WriteCurrent(seed) => {
                let cluster = current_cluster(&map);
                let placement_key = trace_placement_key(step, *seed);
                let key = trace_shard_key(step, *seed);
                let data = format!("trace-payload-{step}-{seed}").into_bytes();
                match cluster.place_payload_shards(
                    DataPgId::new(PgId::new(0)),
                    ec_shape,
                    &placement_key,
                ) {
                    Ok(locations) => {
                        prop_assert_eq!(pg_state, PgState::Active);
                        let location = locations[usize::from(key.shard_index().get())];
                        let ack = cluster.write_payload_shard(location, &key, &data).unwrap();
                        let read = cluster.read_payload_shard(location, &key, ack).unwrap();
                        prop_assert_eq!(read.as_slice(), data.as_slice());
                        written = Some(TraceWrittenShard {
                            placement_key,
                            location,
                            key,
                            ack,
                            data,
                        });
                    }
                    Err(ClusterBuildError::PgNotActive { state, .. }) => {
                        prop_assert_eq!(state, pg_state);
                        prop_assert!(!shard_file_present_on_any_trace_node(&map, &key));
                    }
                    Err(err) => return Err(TestCaseError::fail(format!("{err:?}"))),
                }
            }
            LocalClusterTraceOp::WriteStale(seed) => {
                let cluster = stale_cluster(&map, current_epoch);
                let key = trace_shard_key(step, *seed);
                let err = cluster
                    .write_payload_shard(
                        arbitrary_current_location(current_epoch),
                        &key,
                        b"stale write",
                    )
                    .unwrap_err();
                let expected = matches!(
                    err,
                    ShardIoError::StaleOperationEpoch {
                        operation_epoch,
                        current_epoch: err_current_epoch,
                        ..
                    } if operation_epoch == stale_epoch_for(current_epoch)
                        && err_current_epoch == current_epoch
                );
                prop_assert!(expected, "unexpected stale write error: {err:?}");
                prop_assert!(!shard_file_present_on_any_trace_node(&map, &key));
            }
            LocalClusterTraceOp::ReadCurrent => {
                let Some(written) = written.as_ref() else {
                    continue;
                };
                let cluster = current_cluster(&map);
                if pg_state == PgState::Active {
                    let location = current_trace_location(&cluster, written, ec_shape);
                    let read = cluster
                        .read_payload_shard(location, &written.key, written.ack)
                        .unwrap();
                    prop_assert_eq!(read.as_slice(), written.data.as_slice());
                } else {
                    let location = written_trace_location_for_epoch(written, current_epoch);
                    let err = cluster
                        .read_payload_shard(location, &written.key, written.ack)
                        .unwrap_err();
                    let expected = matches!(
                        err,
                        ShardIoError::PgNotActive { state, .. } if state == pg_state
                    );
                    prop_assert!(expected, "unexpected inactive read error: {err:?}");
                    prop_assert!(shard_file_present_on_any_trace_node(&map, &written.key));
                }
            }
            LocalClusterTraceOp::ReadStale => {
                let key = written
                    .as_ref()
                    .map(|written| written.key.clone())
                    .unwrap_or_else(|| trace_shard_key(step, 0));
                let ack = written
                    .as_ref()
                    .map(|written| written.ack)
                    .unwrap_or(WriteAck {
                        crc64: 0,
                        stored_size: 1,
                    });
                let cluster = stale_cluster(&map, current_epoch);
                let err = cluster
                    .read_payload_shard(arbitrary_current_location(current_epoch), &key, ack)
                    .unwrap_err();
                let expected = matches!(
                    err,
                    ShardIoError::StaleOperationEpoch {
                        operation_epoch,
                        current_epoch: err_current_epoch,
                        ..
                    } if operation_epoch == stale_epoch_for(current_epoch)
                        && err_current_epoch == current_epoch
                );
                prop_assert!(expected, "unexpected stale read error: {err:?}");
            }
            LocalClusterTraceOp::DeleteCurrent => {
                let Some(previous) = written.clone() else {
                    continue;
                };
                let cluster = current_cluster(&map);
                if pg_state == PgState::Active {
                    let location = current_trace_location(&cluster, &previous, ec_shape);
                    cluster
                        .delete_payload_shard(location, &previous.key)
                        .unwrap();
                    prop_assert!(!shard_file_present_on_any_trace_node(&map, &previous.key));
                    written = None;
                } else {
                    let location = written_trace_location_for_epoch(&previous, current_epoch);
                    let err = cluster
                        .delete_payload_shard(location, &previous.key)
                        .unwrap_err();
                    let expected = matches!(
                        err,
                        ShardIoError::PgNotActive { state, .. } if state == pg_state
                    );
                    prop_assert!(expected, "unexpected inactive delete error: {err:?}");
                    prop_assert!(shard_file_present_on_any_trace_node(&map, &previous.key));
                }
            }
            LocalClusterTraceOp::DeleteStale => {
                let Some(previous) = written.as_ref() else {
                    continue;
                };
                let cluster = stale_cluster(&map, current_epoch);
                let err = cluster
                    .delete_payload_shard(arbitrary_current_location(current_epoch), &previous.key)
                    .unwrap_err();
                let expected = matches!(
                    err,
                    ShardIoError::StaleOperationEpoch {
                        operation_epoch,
                        current_epoch: err_current_epoch,
                        ..
                    } if operation_epoch == stale_epoch_for(current_epoch)
                        && err_current_epoch == current_epoch
                );
                prop_assert!(expected, "unexpected stale delete error: {err:?}");
                prop_assert!(shard_file_present_on_any_trace_node(&map, &previous.key));
            }
            LocalClusterTraceOp::MetadataOperationStale(seed) => {
                let cluster = stale_cluster(&map, current_epoch);
                let bucket = trace_bucket(*seed);
                let key = trace_key(*seed);
                let reservation_id = trace_session(*seed);
                let err = cluster
                    .reserve_put_object_generation(&bucket, &key, &reservation_id)
                    .unwrap_err();
                assert_stale_metadata_operation_error(err, current_epoch)?;
                let cluster = current_cluster(&map);
                let err = cluster
                    .test_object_generation_reservation_for(&bucket, &key, &reservation_id)
                    .unwrap_err();
                let expected = matches!(
                    err,
                    crate::ObjectPgActionError::Metadata(
                        crate::MetadataError::ObjectGenerationReservationNotFound { .. }
                    )
                );
                prop_assert!(expected, "unexpected reservation lookup error: {err:?}");
            }
            LocalClusterTraceOp::ZeroSizeStalePayloadRead => {
                let cluster = stale_cluster(&map, current_epoch);
                let mut dst = vec![0xAA];
                let err = cluster
                    .read_segment_payload_stored_bytes_into(
                        crate::SegmentStoredBytesRequest {
                            data_pg_id: 0,
                            segment_okh: [step as u8; 16],
                            segment_vid: crate::GenerationId::MIN,
                            stored_size: 0,
                            segment_crc64: 0,
                            ec: ec_shape,
                        },
                        &mut dst,
                    )
                    .unwrap_err();
                let expected = matches!(
                    err,
                    StoreError::StalePayloadOperation {
                        pg_id: 0,
                        operation_epoch,
                        current_epoch: err_current_epoch,
                    } if operation_epoch == stale_epoch_for(current_epoch)
                        && err_current_epoch == current_epoch
                );
                prop_assert!(expected, "unexpected stale zero-size read error: {err:?}");
                prop_assert_eq!(dst, vec![0xAA]);
            }
            LocalClusterTraceOp::QueueCurrent(seed) => {
                let cluster = current_cluster(&map);
                drain_trace_reclaim_work(&cluster);
                let bucket = trace_bucket(*seed);
                let key = trace_key(*seed);
                let generation_id = trace_generation(*seed);
                cluster.enqueue_object_payload_reclaim(&bucket, &key, generation_id);
                let work = cluster.try_take_reclaim_work();
                let expected = matches!(
                    &work,
                    Some(crate::ReclaimWorkItem::ObjectPayload((
                        queued_bucket,
                        queued_key,
                        queued_generation_id
                    ))) if queued_bucket == &bucket
                        && queued_key == &key
                        && *queued_generation_id == generation_id
                );
                prop_assert!(expected, "unexpected reclaim work item");
                if let Some(crate::ReclaimWorkItem::ObjectPayload((
                    queued_bucket,
                    queued_key,
                    queued_generation_id,
                ))) = work
                {
                    cluster.finish_object_payload_reclaim_work(
                        &queued_bucket,
                        &queued_key,
                        queued_generation_id,
                    );
                }
                drain_trace_reclaim_work(&cluster);
            }
            LocalClusterTraceOp::QueueStale(seed) => {
                let cluster = current_cluster(&map);
                drain_trace_reclaim_work(&cluster);
                drop(cluster);
                let cluster = stale_cluster(&map, current_epoch);
                cluster.enqueue_object_payload_reclaim(
                    &trace_bucket(*seed),
                    &trace_key(*seed),
                    trace_generation(*seed),
                );
                drop(cluster);
                let cluster = current_cluster(&map);
                prop_assert!(cluster.try_take_reclaim_work().is_none());
            }
            LocalClusterTraceOp::LeaseReleaseAcrossEpoch(seed) => {
                let bucket = trace_bucket(*seed);
                let key = trace_key(*seed);
                let generation_id = trace_generation(*seed);
                let cluster = current_cluster(&map);
                if pg_state != PgState::Active {
                    let err =
                        match cluster.acquire_object_payload_lease(&bucket, &key, generation_id) {
                            Ok(_) => {
                                return Err(TestCaseError::fail(
                                    "inactive PG acquired a payload lease",
                                ));
                            }
                            Err(err) => err,
                        };
                    let expected = matches!(
                        err,
                        StoreError::PgNotActive {
                            pg_id: 0,
                            cluster_epoch,
                            state,
                        } if cluster_epoch == current_epoch && state == pg_state
                    );
                    prop_assert!(expected, "unexpected inactive lease acquire error: {err:?}");
                    continue;
                }
                let lease = cluster
                    .acquire_object_payload_lease(&bucket, &key, generation_id)
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                prop_assert_eq!(
                    cluster.object_payload_lease_count(&bucket, &key, generation_id),
                    1
                );
                drop(cluster);

                current_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
                visited_epochs.insert(current_epoch);
                set_trace_epoch(&mut map, current_epoch);
                let cluster = current_cluster(&map);
                prop_assert_eq!(
                    cluster.object_payload_lease_count(&bucket, &key, generation_id),
                    1
                );
                let released = lease.release();
                prop_assert_eq!(released.remaining(), 0);
                prop_assert_eq!(
                    cluster.object_payload_lease_count(&bucket, &key, generation_id),
                    0
                );
            }
            LocalClusterTraceOp::RecoverAfterPhysicalShardLoss(seed) => {
                if pg_state != PgState::Active {
                    continue;
                }
                let cluster = current_cluster(&map);
                let payload = format!("recoverable-physical-shard-loss-{step}-{seed}");
                let bucket = trace_bucket(seed.wrapping_add(step as u8));
                let key = trace_key(seed.wrapping_add(step as u8));
                let segment = write_trace_placed_segment(
                    &cluster,
                    &bucket,
                    &key,
                    trace_segment_generation(step, *seed),
                    [seed.wrapping_add(step as u8); 16],
                    payload.as_bytes(),
                )?;
                let shard_path = cluster
                    .test_payload_shard_file_path(
                        segment.written.data_pg_id,
                        segment.written.ec,
                        &segment.segment_okh,
                        segment.generation_id,
                        0,
                    )
                    .unwrap();
                std::fs::remove_file(shard_path).unwrap();

                let mut recovered = Vec::new();
                let req = trace_segment_stored_bytes_request(&segment);
                cluster
                    .read_segment_payload_stored_bytes_into(req, &mut recovered)
                    .unwrap();
                prop_assert_eq!(recovered, segment.payload);
            }
            LocalClusterTraceOp::DurableRepairQueueAfterShardCorruption(seed) => {
                if pg_state != PgState::Active {
                    continue;
                }
                let cluster = current_cluster(&map);
                while cluster
                    .try_take_placed_segment_shard_repair_work()
                    .is_some()
                {}
                let payload = format!("trace-durable-repair-queue-{step}-{seed}");
                let bucket = trace_bucket(seed.wrapping_add(step as u8));
                let key = trace_key(seed.wrapping_add(step as u8));
                let segment = write_trace_placed_segment(
                    &cluster,
                    &bucket,
                    &key,
                    trace_segment_generation(step, *seed),
                    [seed.wrapping_add(step as u8).wrapping_add(23); 16],
                    payload.as_bytes(),
                )?;
                let shard_index = ShardIndex::new(0);
                let shard_size = segment
                    .payload
                    .len()
                    .div_ceil(usize::from(segment.written.ec.k));
                let shard_path = cluster.test_payload_shard_file_path(
                    segment.written.data_pg_id,
                    segment.written.ec,
                    &segment.segment_okh,
                    segment.generation_id,
                    shard_index.get(),
                )?;
                std::fs::write(shard_path, vec![0xAB; shard_size])
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                let req = trace_segment_stored_bytes_request(&segment);

                for _ in 0..2 {
                    let mut recovered = Vec::new();
                    cluster
                        .read_segment_payload_stored_bytes_into(req, &mut recovered)
                        .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                    prop_assert_eq!(&recovered, &segment.payload);
                }

                let work = cluster
                    .try_take_placed_segment_shard_repair_work()
                    .ok_or_else(|| {
                        TestCaseError::fail(
                            "successful read recovery should enqueue corrupt shard repair",
                        )
                    })?;
                prop_assert_eq!(work.request, req);
                prop_assert_eq!(work.shard_index, shard_index);
                prop_assert!(cluster
                    .try_take_placed_segment_shard_repair_work()
                    .is_none());

                let durable_repairs = cluster
                    .list_placed_segment_shard_repairs(req.data_pg_id)
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                let matching_repair = durable_repairs
                    .iter()
                    .find(|repair| repair.work_item == work)
                    .ok_or_else(|| TestCaseError::fail("durable repair row missing"))?;
                prop_assert!(matching_repair.observation_count >= 2);

                let first_scan = cluster
                    .enqueue_durable_placed_segment_shard_repair_work()
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                prop_assert!(first_scan.scanned >= 1);
                prop_assert!(first_scan.enqueued >= 1);
                let second_scan = cluster
                    .enqueue_durable_placed_segment_shard_repair_work()
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                prop_assert!(second_scan.scanned >= 1);
                prop_assert_eq!(second_scan.enqueued, 0);
                let mut queued_repair_work = Vec::new();
                while let Some(queued) = cluster.try_take_placed_segment_shard_repair_work() {
                    queued_repair_work.push(queued);
                }
                prop_assert!(
                    queued_repair_work.iter().any(|queued| queued == &work),
                    "durable repair scan did not enqueue observed repair row: {queued_repair_work:?}"
                );
            }
            LocalClusterTraceOp::RetainedRouteHistoricalRead(seed) => {
                if pg_state != PgState::Active {
                    continue;
                }
                let source_acting_set = trace_historical_source_acting_set();
                let source_route = PgRouteSnapshot::reconstructed(
                    current_epoch,
                    PgId::new(0),
                    source_acting_set[0],
                    source_acting_set,
                    PgState::Active,
                );
                {
                    let map = Arc::get_mut(&mut map)
                        .expect("trace must not retain StorageCluster handles");
                    map.test_install_pg_routes([source_route.clone()]);
                    map.epoch = current_epoch;
                }
                let (segment, req) = {
                    let cluster = current_cluster(&map);
                    let payload = format!("trace-retained-route-{step}-{seed}");
                    let bucket = trace_bucket(seed.wrapping_add(step as u8));
                    let key = trace_key(seed.wrapping_add(step as u8));
                    let segment = write_trace_placed_segment(
                        &cluster,
                        &bucket,
                        &key,
                        trace_segment_generation(step, *seed),
                        [seed.wrapping_add(step as u8).wrapping_add(41); 16],
                        payload.as_bytes(),
                    )?;
                    let req = trace_segment_stored_bytes_request(&segment);
                    (segment, req)
                };

                current_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
                visited_epochs.insert(current_epoch);
                let acting_set = trace_historical_current_acting_set(*seed);
                let next_route = PgRouteSnapshot::reconstructed(
                    current_epoch,
                    PgId::new(0),
                    acting_set[0],
                    acting_set,
                    PgState::Active,
                );
                {
                    let map = Arc::get_mut(&mut map)
                        .expect("trace must not retain StorageCluster handles");
                    map.test_install_pg_routes([next_route]);
                    map.test_install_historical_pg_routes([source_route.clone()]);
                    map.epoch = current_epoch;
                }
                pg_state = PgState::Active;
                written = None;

                let cluster = current_cluster(&map);
                let placement_key = trace_segment_payload_placement_key(
                    &segment.segment_okh,
                    segment.generation_id,
                );
                let historical_locations = cluster
                    .place_payload_shards_for_pg_route_snapshot(
                        &source_route,
                        DataPgId::new(PgId::new(req.data_pg_id)),
                        req.ec,
                        &placement_key,
                    )
                    .unwrap();
                let current_locations = cluster
                    .place_payload_shards(
                        DataPgId::new(PgId::new(req.data_pg_id)),
                        req.ec,
                        &placement_key,
                    )
                    .unwrap();
                prop_assert!(
                    historical_locations
                        .iter()
                        .zip(&current_locations)
                        .any(|(historical, current)| historical.node_id() != current.node_id()),
                    "retained-route trace must move at least one shard to a different node"
                );

                let mut recovered = Vec::new();
                cluster
                    .read_segment_payload_stored_bytes_at_placement_epoch_into(
                        source_route.cluster_epoch(),
                        req,
                        &mut recovered,
                    )
                    .unwrap();
                prop_assert_eq!(recovered, segment.payload);
                prop_assert_ne!(cluster.cluster_epoch(), source_route.cluster_epoch());
            }
            LocalClusterTraceOp::DurableBackfillClaimAfterRouteChange(seed) => {
                if pg_state != PgState::Active {
                    continue;
                }
                let source_acting_set = trace_historical_source_acting_set();
                let source_route = PgRouteSnapshot::reconstructed(
                    current_epoch,
                    PgId::new(0),
                    source_acting_set[0],
                    source_acting_set,
                    PgState::Active,
                );
                {
                    let map = Arc::get_mut(&mut map)
                        .expect("trace must not retain StorageCluster handles");
                    map.test_install_pg_routes([source_route.clone()]);
                    map.epoch = current_epoch;
                }
                let (segment, req) = {
                    let cluster = current_cluster(&map);
                    let payload = format!("trace-durable-backfill-claim-{step}-{seed}");
                    let bucket = trace_bucket(seed.wrapping_add(step as u8));
                    let key = trace_key(seed.wrapping_add(step as u8));
                    let segment = write_trace_placed_segment(
                        &cluster,
                        &bucket,
                        &key,
                        trace_segment_generation(step, *seed),
                        [seed.wrapping_add(step as u8).wrapping_add(59); 16],
                        payload.as_bytes(),
                    )?;
                    let req = trace_segment_stored_bytes_request(&segment);
                    (segment, req)
                };

                current_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
                visited_epochs.insert(current_epoch);
                let acting_set = trace_historical_current_acting_set(*seed);
                let desired_route = PgRouteSnapshot::reconstructed(
                    current_epoch,
                    PgId::new(0),
                    acting_set[0],
                    acting_set,
                    PgState::Active,
                );
                {
                    let map = Arc::get_mut(&mut map)
                        .expect("trace must not retain StorageCluster handles");
                    map.test_install_pg_routes([desired_route.clone()]);
                    map.test_install_historical_pg_routes([source_route.clone()]);
                    map.epoch = current_epoch;
                }
                pg_state = PgState::Active;
                written = None;

                let cluster = current_cluster(&map);
                let placement_key = trace_segment_payload_placement_key(
                    &segment.segment_okh,
                    segment.generation_id,
                );
                let historical_locations = cluster
                    .place_payload_shards_for_pg_route_snapshot(
                        &source_route,
                        DataPgId::new(PgId::new(req.data_pg_id)),
                        req.ec,
                        &placement_key,
                    )
                    .unwrap();
                let desired_locations = cluster
                    .place_payload_shards_for_pg_route_snapshot(
                        &desired_route,
                        DataPgId::new(PgId::new(req.data_pg_id)),
                        req.ec,
                        &placement_key,
                    )
                    .unwrap();
                prop_assert!(
                    historical_locations
                        .iter()
                        .zip(&desired_locations)
                        .any(|(historical, desired)| historical.node_id() != desired.node_id()),
                    "backfill trace must move at least one shard to a different node"
                );

                for row in cluster
                    .list_placed_segment_shard_backfills(req.data_pg_id)
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?
                {
                    cluster
                        .resolve_placed_segment_shard_backfill(&row.work_item)
                        .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                }

                let plan = cluster
                    .record_placed_segment_shard_backfill_for_plan(
                        &source_route,
                        &desired_route,
                        req,
                        None,
                    )
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                prop_assert!(!plan.is_complete());
                prop_assert!(
                    !plan.copy_targets.is_empty() || !plan.reconstruction_targets.is_empty()
                );

                let work_item = crate::PlacedSegmentShardBackfillWorkItem {
                    request: req,
                    source_cluster_epoch: source_route.cluster_epoch(),
                    desired_cluster_epoch: desired_route.cluster_epoch(),
                };
                let rows = cluster
                    .list_placed_segment_shard_backfills(req.data_pg_id)
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                prop_assert_eq!(
                    rows.iter().filter(|row| row.work_item == work_item).count(),
                    1
                );

                let now = 100_000 + step as u64;
                let claim = cluster
                    .acquire_next_placed_segment_shard_backfill_claim(
                        &format!("trace-backfill-claim-{step}"),
                        &format!("trace-backfill-owner-{step}"),
                        now,
                        now + 60,
                        now,
                    )
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?
                    .ok_or_else(|| {
                        TestCaseError::fail("new durable backfill row should be claimable")
                    })?;
                prop_assert_eq!(claim.work_item, work_item);
                prop_assert_eq!(claim.remaining_tolerance, plan.source_remaining_tolerance());
                prop_assert_eq!(claim.cluster_epoch, current_epoch);
                prop_assert_eq!(claim.attempt_count, 1);

                let busy = cluster
                    .acquire_next_placed_segment_shard_backfill_claim(
                        &format!("trace-backfill-claim-busy-{step}"),
                        &format!("trace-backfill-owner-busy-{step}"),
                        now + 1,
                        now + 61,
                        now + 1,
                    )
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                prop_assert!(busy.is_none());

                let completed = cluster
                    .complete_placed_segment_shard_backfill_claim(&claim)
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                prop_assert!(completed);
                let remaining_rows = cluster
                    .list_placed_segment_shard_backfills(req.data_pg_id)
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                prop_assert!(remaining_rows.iter().all(|row| row.work_item != work_item));
            }
            LocalClusterTraceOp::StaleDirectPutCleanupAfterRouteChange(seed) => {
                if pg_state != PgState::Active || current_epoch != ClusterEpoch::INITIAL {
                    continue;
                }
                let source_epoch = current_epoch;
                let source_route = PgRouteSnapshot::reconstructed(
                    source_epoch,
                    PgId::new(0),
                    NodeId::new(0),
                    trace_historical_source_acting_set(),
                    PgState::Active,
                );
                {
                    let map = Arc::get_mut(&mut map)
                        .expect("trace must not retain StorageCluster handles");
                    map.test_install_pg_routes([source_route.clone()]);
                    map.epoch = source_epoch;
                }
                let source_cluster = current_cluster(&map);
                let bucket =
                    crate::BucketName::try_from(format!("trace-stale-cleanup-{step}-{seed}"))
                        .unwrap();
                let key =
                    crate::ObjectKey::try_from(format!("trace-stale-cleanup-key-{step}-{seed}"))
                        .unwrap();
                ensure_test_bucket(&source_cluster, &bucket);

                let reservation_id = trace_session_for_step(step, *seed);
                let generation_id = source_cluster
                    .reserve_put_object_generation(&bucket, &key, &reservation_id)
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                let payload = format!("trace-stale-direct-put-cleanup-{step}-{seed}");
                let segment_okh = [seed.wrapping_add(step as u8).wrapping_add(79); 16];
                let staged_written = source_cluster
                    .write_direct_put_segment_payload_shards(
                        &bucket,
                        &key,
                        generation_id,
                        0,
                        &segment_okh,
                        payload.as_bytes(),
                    )
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                source_cluster
                    .test_register_payload_shard_acks(
                        staged_written.data_pg_id,
                        &staged_written.written_shards,
                    )
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;

                let source_data_pg_primary = map
                    .node(source_route.primary_node_id())
                    .unwrap()
                    .storage_node()
                    .get_pg(staged_written.data_pg_id)
                    .unwrap();
                for written_shard in &staged_written.written_shards {
                    source_data_pg_primary
                        .validate_written_shard_ack(&written_shard.key, written_shard.ack)
                        .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                    prop_assert!(shard_file_present_on_any_trace_node(
                        &map,
                        &written_shard.key
                    ));
                }
                drop(source_data_pg_primary);

                let commit_req = direct_put_commit_req(
                    &source_cluster,
                    DirectPutCommitReqFixture {
                        bucket: &bucket,
                        key: &key,
                        reservation_id,
                        generation_id,
                        payload: payload.as_bytes(),
                        segment_okh,
                        written: &staged_written,
                    },
                );
                drop(source_cluster);

                current_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
                visited_epochs.insert(current_epoch);
                let acting_set = trace_historical_current_acting_set(*seed);
                let current_route = PgRouteSnapshot::reconstructed(
                    current_epoch,
                    PgId::new(0),
                    acting_set[0],
                    acting_set,
                    PgState::Active,
                );
                {
                    let map = Arc::get_mut(&mut map)
                        .expect("trace must not retain StorageCluster handles");
                    map.test_install_pg_routes([current_route]);
                    map.test_install_historical_pg_routes([source_route.clone()]);
                    map.epoch = current_epoch;
                }
                pg_state = PgState::Active;
                written = None;

                let stale_cluster = stale_cluster(&map, current_epoch);
                let err = stale_cluster
                    .commit_direct_put_object_from_payload_shards(
                        &commit_req,
                        &staged_written.written_shards,
                        |_| Ok::<(), ()>(()),
                    )
                    .unwrap_err();
                assert_stale_metadata_operation_error(err, current_epoch)?;
                drop(stale_cluster);

                let source_data_pg_primary = map
                    .node(source_route.primary_node_id())
                    .unwrap()
                    .storage_node()
                    .get_pg(commit_req.data_pg_id)
                    .unwrap();
                for written_shard in &staged_written.written_shards {
                    prop_assert!(
                        !shard_file_present_on_any_trace_node(&map, &written_shard.key),
                        "stale direct PUT cleanup left shard file {}",
                        written_shard.key
                    );
                    let ack_result = source_data_pg_primary
                        .validate_written_shard_ack(&written_shard.key, written_shard.ack);
                    prop_assert!(
                        matches!(ack_result, Err(StoreError::NotFound)),
                        "stale direct PUT cleanup left ack row for {}: {ack_result:?}",
                        written_shard.key
                    );
                }
            }
            LocalClusterTraceOp::RoutineMetadataCheckpointTick => {
                let cluster = current_cluster(&map);
                let summary = cluster
                    .record_routine_metadata_command_checkpoints()
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                prop_assert_eq!(summary.failed, 0);
                prop_assert_eq!(summary.compaction_failed, 0);
                assert_no_pending_metadata_command_slots_at_epoch(
                    &map,
                    &[PgId::new(0)],
                    current_epoch,
                );
            }
            LocalClusterTraceOp::DrainPendingCreateBucketFromSecondHandle(seed) => {
                if pg_state != PgState::Active || current_epoch != ClusterEpoch::INITIAL {
                    continue;
                }
                let pg_id = PgId::new(0);
                let pending_bucket = trace_bucket_for_pg(
                    &map,
                    pg_id.get(),
                    &format!("trace-pending-create-{step}-{seed}-"),
                );
                let requested_bucket = trace_bucket_for_pg(
                    &map,
                    pg_id.get(),
                    &format!("trace-request-create-{step}-{seed}-"),
                );
                let primary = map
                    .metadata_pg_primary_node(current_epoch, pg_id)
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                let primary_pg = primary
                    .storage_node()
                    .get_pg(pg_id.get())
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                if primary_pg
                    .pending_metadata_command_slot(primary.node_id().as_u32(), current_epoch)
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?
                    .is_some()
                {
                    continue;
                }
                let log_index = primary_pg
                    .max_metadata_command_log_index(current_epoch)
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?
                    + 1;
                let pending_command =
                    create_bucket_metadata_command(pg_id, log_index, pending_bucket.clone());
                primary_pg
                    .try_insert_pending_metadata_command_slot(
                        primary.node_id().as_u32(),
                        &pending_command,
                        Some(&pending_bucket),
                    )
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                drop(primary_pg);

                let second_handle = current_cluster(&map);
                create_test_bucket(&second_handle, &requested_bucket);
                for node_id in trace_node_ids() {
                    let pg = map
                        .node(node_id)
                        .unwrap()
                        .storage_node()
                        .get_pg(pg_id.get())
                        .unwrap();
                    crate::PgMetadataStore::head_bucket(&*pg, &pending_bucket).unwrap();
                    crate::PgMetadataStore::head_bucket(&*pg, &requested_bucket).unwrap();
                }
            }
            LocalClusterTraceOp::RestartAndValidate => {
                if pg_state != PgState::Active || current_epoch != ClusterEpoch::INITIAL {
                    continue;
                }
                map = Arc::new(
                    LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape)
                        .map_err(|err| TestCaseError::fail(format!("{err:?}")))?,
                );
                assert_clean_metadata_command_stream(&map, &[0]);
            }
            LocalClusterTraceOp::ReissueDuplicateCreateBucketIndex(seed) => {
                if pg_state != PgState::Active || current_epoch != ClusterEpoch::INITIAL {
                    continue;
                }
                let pg_id = PgId::new(0);
                let first_bucket = trace_bucket_for_pg(
                    &map,
                    pg_id.get(),
                    &format!("trace-reissue-first-{step}-{seed}-"),
                );
                let second_bucket = trace_bucket_for_pg(
                    &map,
                    pg_id.get(),
                    &format!("trace-reissue-second-{step}-{seed}-"),
                );
                let cluster = current_cluster(&map);
                let primary = map
                    .metadata_pg_primary_node(current_epoch, pg_id)
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                let primary_pg = primary
                    .storage_node()
                    .get_pg(pg_id.get())
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                if primary_pg
                    .pending_metadata_command_slot(primary.node_id().as_u32(), current_epoch)
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?
                    .is_some()
                {
                    continue;
                }
                let duplicate_index = primary_pg
                    .max_metadata_command_log_index(current_epoch)
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?
                    + 1;
                drop(primary_pg);

                let applied =
                    create_bucket_metadata_command(pg_id, duplicate_index, first_bucket.clone());
                cluster
                    .test_apply_metadata_command_to_acting_set_from_origin(
                        primary.node_id(),
                        &applied,
                    )
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                let stale_duplicate =
                    create_bucket_metadata_command(pg_id, duplicate_index, second_bucket.clone());
                let reissued = MetadataCommandEnvelope::new(
                    MetadataCommandId::new(
                        current_epoch,
                        pg_id,
                        MetadataCommandLogIndex::new(duplicate_index + 1)
                            .expect("trace duplicate index should not overflow"),
                    ),
                    stale_duplicate.payload().clone(),
                );
                force_insert_pending_metadata_command_for_test(
                    &map,
                    pg_id,
                    &second_bucket,
                    &stale_duplicate,
                );

                create_test_bucket(&cluster, &second_bucket);
                for node_id in trace_node_ids() {
                    let pg = map
                        .node(node_id)
                        .unwrap()
                        .storage_node()
                        .get_pg(pg_id.get())
                        .unwrap();
                    crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
                    crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
                    let (command_checksum, command_bytes, abandoned): (i64, Vec<u8>, i64) = pg
                        .connection()
                        .query_row(
                            "SELECT command_checksum, command_bytes, abandoned \
                                 FROM metadata_command_log \
                                 WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3",
                            rusqlite::params![
                                current_epoch.get() as i64,
                                pg_id.get() as i64,
                                duplicate_index as i64 + 1,
                            ],
                            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                        )
                        .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                    prop_assert_eq!(abandoned, 0);
                    prop_assert_eq!(command_checksum as u64, reissued.checksum_crc64());
                    prop_assert_eq!(command_bytes, reissued.command_bytes());
                }
                assert_clean_metadata_command_stream(&map, &[0]);
            }
        }
    }

    let trace_pg_ids = [PgId::new(0)];
    for epoch in visited_epochs.iter().copied() {
        assert_no_pending_metadata_command_slots_at_epoch(&map, &trace_pg_ids, epoch);
    }
    if visited_epochs.len() == 1 {
        assert_clean_metadata_command_stream(&map, &[0]);
    }
    Ok(())
}

#[test]
fn local_cluster_trace_retained_route_after_epoch_advances_uses_placed_segment_rows() {
    let mut ops = vec![LocalClusterTraceOp::RetainedRouteHistoricalRead(151)];
    ops.extend(std::iter::repeat(LocalClusterTraceOp::AdvanceEpoch).take(22));
    ops.extend([
        LocalClusterTraceOp::WriteCurrent(0),
        LocalClusterTraceOp::WriteCurrent(0),
        LocalClusterTraceOp::DurableRepairQueueAfterShardCorruption(126),
    ]);

    run_local_cluster_trace(&ops).unwrap();
}

#[test]
fn local_cluster_trace_stale_direct_put_cleanup_uses_retained_route() {
    run_local_cluster_trace(
        &[LocalClusterTraceOp::StaleDirectPutCleanupAfterRouteChange(
            37,
        )],
    )
    .unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 32,
        .. ProptestConfig::default()
    })]

    #[test]
    fn prop_local_cluster_trace_preserves_epoch_route_and_cleanup_invariants(
        ops in local_cluster_trace_strategy()
    ) {
        run_local_cluster_trace(&ops)?;
    }
}
