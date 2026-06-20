use super::trace::trace_session;
use super::*;

#[derive(Debug, Clone)]
enum MultipartTraceOp {
    Create { slot_seed: u8, key_seed: u8 },
    UploadStreamPart { slot_seed: u8, payload_seed: u8 },
    UploadCopiedPart { slot_seed: u8, payload_seed: u8 },
    Complete { slot_seed: u8 },
    Abort { slot_seed: u8 },
    DeleteRecreateBucket,
    StaleCreate { slot_seed: u8, key_seed: u8 },
    StaleUploadPartCreate { slot_seed: u8, payload_seed: u8 },
    StaleAbort { slot_seed: u8 },
    Reopen,
}

#[derive(Debug)]
struct MultipartTraceUpload {
    upload_id: crate::UploadId,
    key: crate::ObjectKey,
    object_pg: u32,
    next_part_number: u32,
    parts: Vec<crate::MultipartPartRecord>,
}

fn multipart_trace_strategy() -> impl Strategy<Value = Vec<MultipartTraceOp>> {
    prop::collection::vec(
        prop_oneof![
            3 => (any::<u8>(), any::<u8>()).prop_map(|(slot_seed, key_seed)| {
                MultipartTraceOp::Create { slot_seed, key_seed }
            }),
            3 => (any::<u8>(), any::<u8>()).prop_map(|(slot_seed, payload_seed)| {
                MultipartTraceOp::UploadStreamPart { slot_seed, payload_seed }
            }),
            2 => (any::<u8>(), any::<u8>()).prop_map(|(slot_seed, payload_seed)| {
                MultipartTraceOp::UploadCopiedPart { slot_seed, payload_seed }
            }),
            2 => any::<u8>().prop_map(|slot_seed| MultipartTraceOp::Complete { slot_seed }),
            2 => any::<u8>().prop_map(|slot_seed| MultipartTraceOp::Abort { slot_seed }),
            1 => Just(MultipartTraceOp::DeleteRecreateBucket),
            1 => (any::<u8>(), any::<u8>()).prop_map(|(slot_seed, key_seed)| {
                MultipartTraceOp::StaleCreate { slot_seed, key_seed }
            }),
            1 => (any::<u8>(), any::<u8>()).prop_map(|(slot_seed, payload_seed)| {
                MultipartTraceOp::StaleUploadPartCreate { slot_seed, payload_seed }
            }),
            1 => any::<u8>().prop_map(|slot_seed| MultipartTraceOp::StaleAbort { slot_seed }),
            1 => Just(MultipartTraceOp::Reopen),
        ],
        1..=16,
    )
}

fn multipart_trace_upload_id(step: usize, seed: u8) -> crate::UploadId {
    upload_id_from_label(&format!("trmpu{step:02x}{seed:02x}"))
}

fn multipart_trace_slot(seed: u8) -> usize {
    usize::from(seed % 3)
}

fn create_multipart_trace_upload(
    cluster: &crate::StorageCluster,
    bucket: &crate::BucketName,
    key: &crate::ObjectKey,
    upload_id: crate::UploadId,
) -> TestCaseResult {
    let create = crate::CreateMultipartUploadReq {
        upload_id,
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: Some(crate::OwnerIdentity::from_principal("initiator")),
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        object_lock: crate::ObjectLockState::default(),
        checksum: None,
        encryption: crate::ObjectEncryption::None,
    };
    cluster
        .create_multipart_upload(
            bucket,
            key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, _existing_object| Ok::<_, ()>(((), create.clone())),
        )
        .map_err(|err| TestCaseError::fail(format!("{err:?}")))?
        .map_err(|_| TestCaseError::fail("multipart trace create action failed"))?;
    Ok(())
}

fn stale_multipart_trace_cluster(map: &Arc<LocalClusterMap>) -> Arc<crate::StorageCluster> {
    crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(map),
        ClusterEpoch::new(2).unwrap(),
    )
    .unwrap()
}

fn assert_multipart_trace_stale_snapshot_error(
    err: crate::BucketSnapshotLoadError,
) -> TestCaseResult {
    prop_assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::StaleMetadataOperation {
                operation_epoch,
                current_epoch,
                ..
            }) if operation_epoch == ClusterEpoch::new(2).unwrap()
                && current_epoch == ClusterEpoch::INITIAL
        ),
        "unexpected stale multipart snapshot error: {err:?}"
    );
    Ok(())
}

fn assert_multipart_trace_stale_object_pg_error(err: crate::ObjectPgActionError) -> TestCaseResult {
    prop_assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                operation_epoch,
                current_epoch,
                ..
            }) if operation_epoch == ClusterEpoch::new(2).unwrap()
                && current_epoch == ClusterEpoch::INITIAL
        ),
        "unexpected stale multipart object-PG error: {err:?}"
    );
    Ok(())
}

fn stale_create_multipart_trace_upload(
    cluster: &crate::StorageCluster,
    stale_cluster: &crate::StorageCluster,
    bucket: &crate::BucketName,
    key: &crate::ObjectKey,
    upload_id: crate::UploadId,
) -> TestCaseResult {
    let create = crate::CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: Some(crate::OwnerIdentity::from_principal("initiator")),
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        object_lock: crate::ObjectLockState::default(),
        checksum: None,
        encryption: crate::ObjectEncryption::None,
    };
    let err = stale_cluster
        .create_multipart_upload(
            bucket,
            key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, _existing_object| Ok::<_, ()>(((), create.clone())),
        )
        .unwrap_err();
    assert_multipart_trace_stale_snapshot_error(err)?;
    let err = cluster
        .load_multipart_upload(bucket, key, &upload_id)
        .unwrap_err();
    prop_assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::NoSuchUpload { .. })
        ),
        "stale create mutated upload state: {err:?}"
    );
    Ok(())
}

fn stale_create_upload_part_stream_session(
    cluster: &crate::StorageCluster,
    stale_cluster: &crate::StorageCluster,
    map: &LocalClusterMap,
    bucket: &crate::BucketName,
    upload: &MultipartTraceUpload,
    session_id: crate::SessionId,
) -> TestCaseResult {
    let upload_row = cluster
        .load_in_progress_multipart_upload(bucket, &upload.key, &upload.upload_id)
        .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    let err = stale_cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload_row),
            upload.next_part_number,
            &session_id,
        )
        .unwrap_err();
    assert_multipart_trace_stale_object_pg_error(err)?;
    cluster
        .load_in_progress_multipart_upload(bucket, &upload.key, &upload.upload_id)
        .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    for node_id in map.node_ids() {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(upload.object_pg)
            .unwrap();
        prop_assert!(
            matches!(
                crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
                Err(crate::MetadataError::StreamSessionNotFound { .. })
            ),
            "stale UploadPart session create inserted stream_uploads row on node {node_id:?}"
        );
        let segments = crate::PgMetadataStore::list_stream_segments(&*pg, &session_id)
            .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
        prop_assert!(
                segments.is_empty(),
                "stale UploadPart session create inserted stream segments on node {node_id:?}: {segments:?}"
            );
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct MultipartTraceUploadSnapshot {
    upload_exists: bool,
    parts: Vec<crate::MultipartPartRecord>,
    part_segments: Vec<crate::MultipartPartSegmentRecord>,
    stream_sessions: Vec<(
        crate::StreamUploadRecord,
        Vec<crate::StreamUploadSegmentRecord>,
    )>,
}

fn multipart_trace_upload_snapshot(
    map: &LocalClusterMap,
    node_id: NodeId,
    upload: &MultipartTraceUpload,
) -> MultipartTraceUploadSnapshot {
    let pg = map
        .node(node_id)
        .unwrap()
        .storage_node()
        .get_pg(upload.object_pg)
        .unwrap();
    let upload_exists =
        crate::PgMetadataStore::get_multipart_upload(&*pg, &upload.upload_id).is_ok();
    let parts = if upload_exists {
        crate::PgMetadataStore::list_multipart_parts(
            &*pg,
            &crate::ListPartsReq {
                upload_id: upload.upload_id.clone(),
                part_number_marker: None,
                max_parts: 1000,
            },
        )
        .unwrap()
        .parts
    } else {
        Vec::new()
    };
    let part_segments =
        crate::PgMetadataStore::get_all_multipart_part_segments_for_upload(&*pg, &upload.upload_id)
            .unwrap();
    let mut stream_sessions = crate::PgMetadataStore::list_all_stream_uploads(&*pg)
        .unwrap()
        .into_iter()
        .filter(|session| {
            matches!(
                &session.target,
                crate::StreamUploadTarget::UploadPart {
                    upload_id: session_upload_id,
                    ..
                } if session_upload_id == &upload.upload_id
            )
        })
        .map(|session| {
            let segments =
                crate::PgMetadataStore::list_stream_segments(&*pg, &session.session_id).unwrap();
            (session, segments)
        })
        .collect::<Vec<_>>();
    stream_sessions
        .sort_by(|(left, _), (right, _)| left.session_id.as_str().cmp(right.session_id.as_str()));
    MultipartTraceUploadSnapshot {
        upload_exists,
        parts,
        part_segments,
        stream_sessions,
    }
}

fn multipart_trace_upload_snapshots(
    map: &LocalClusterMap,
    upload: &MultipartTraceUpload,
) -> BTreeMap<u32, MultipartTraceUploadSnapshot> {
    map.node_ids()
        .map(|node_id| {
            (
                node_id.as_u32(),
                multipart_trace_upload_snapshot(map, node_id, upload),
            )
        })
        .collect()
}

fn stale_abort_multipart_trace_upload(
    cluster: &crate::StorageCluster,
    stale_cluster: &crate::StorageCluster,
    map: &LocalClusterMap,
    bucket: &crate::BucketName,
    upload: &MultipartTraceUpload,
) -> TestCaseResult {
    let before = multipart_trace_upload_snapshots(map, upload);
    let err = stale_cluster
        .abort_multipart_upload(bucket, &upload.key, &upload.upload_id)
        .unwrap_err();
    assert_multipart_trace_stale_object_pg_error(err)?;
    cluster
        .load_in_progress_multipart_upload(bucket, &upload.key, &upload.upload_id)
        .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    let after = multipart_trace_upload_snapshots(map, upload);
    prop_assert_eq!(
        after,
        before,
        "stale abort changed multipart upload parts or stream-session state"
    );
    Ok(())
}

fn upload_copied_test_multipart_part(
    cluster: &crate::StorageCluster,
    bucket: &crate::BucketName,
    key: &crate::ObjectKey,
    upload_id: &crate::UploadId,
    part_number: u32,
    payload_seed: u8,
    payloads: [&[u8]; 2],
) -> crate::MultipartPartRecord {
    let session_seed = payload_seed.max(1);
    let session_nonce = STREAMED_MULTIPART_PART_SESSION_NONCE.fetch_add(1, Ordering::SeqCst);
    let session_id = crate::SessionId::try_from(format!(
        "{session_nonce:016x}{:014x}{session_seed:02x}",
        u64::from(part_number),
    ))
    .unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(bucket, key, upload_id)
        .unwrap();
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            part_number,
            &session_id,
        )
        .unwrap();

    let mut staged_segments = Vec::new();
    for (segment_index, payload) in payloads.into_iter().enumerate() {
        let mut segment_okh = [payload_seed
            .wrapping_add(u8::try_from(segment_index).unwrap())
            .max(1); 16];
        let segment_nonce = session_nonce + u64::try_from(segment_index).unwrap();
        segment_okh[..8].copy_from_slice(&segment_nonce.to_be_bytes());
        let (_target, segment) = cluster
            .prepare_stream_segment_append(
                bucket,
                key,
                &crate::PrepareStreamUploadSegmentAppendReq {
                    session_id: session_id.clone(),
                    segment_index: u32::try_from(segment_index).unwrap(),
                    size: payload.len() as u64,
                    segment_crc64: checksum::crc64::checksum(payload),
                    payload_crc64: checksum::crc64::checksum(payload),
                    segment_okh,
                },
            )
            .unwrap();
        let written_shards = cluster
            .write_stream_segment_payload_shards(&segment, payload)
            .unwrap();
        let shard_batch = written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect::<Vec<_>>();
        cluster
            .commit_stream_segment_append(
                bucket,
                key,
                &session_id,
                segment.segment_index,
                &segment,
                &shard_batch,
            )
            .unwrap();
        staged_segments.push(segment);
    }

    let size = payloads.iter().map(|payload| payload.len() as u64).sum();
    let part = cluster
        .finalize_upload_part_stream(
            bucket,
            key,
            upload_id,
            &session_id,
            part_number,
            |snapshot| {
                let generation = snapshot
                    .existing_part_generation
                    .map_or(0, |generation| generation + 1);
                let ec = snapshot
                    .staging_segments
                    .first()
                    .map(|segment| EcShape {
                        k: segment.ec_k,
                        m: segment.ec_m,
                    })
                    .unwrap_or(EcShape { k: 0, m: 0 });
                let payload_crc64 = snapshot.staging_segments.iter().fold(
                    checksum::crc64::checksum(&[]),
                    |crc64, segment| {
                        checksum::crc64::combine(crc64, segment.payload_crc64, segment.size)
                    },
                );
                let part = crate::MultipartPartRecord {
                    upload_id: upload_id.clone(),
                    part_number,
                    generation,
                    size,
                    payload_crc64,
                    etag: vec![payload_seed; 8],
                    etag_kind: crate::EtagKind::Crc64,
                    part_okh: [0u8; 16],
                    part_vid: crate::GenerationId::new(u64::from(generation) + 1).unwrap(),
                    ec_k: ec.k,
                    ec_m: ec.m,
                    last_modified: 123,
                    checksum: None,
                };
                let segments = snapshot
                    .staging_segments
                    .iter()
                    .map(|staged| crate::MultipartPartSegmentRecord {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        upload_id: upload_id.clone(),
                        version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
                        part_number,
                        segment_index: staged.segment_index,
                        size: staged.size,
                        segment_crc64: staged.segment_crc64,
                        segment_okh: staged.segment_okh,
                        segment_vid: staged.segment_vid,
                        data_pg_id: staged.data_pg_id,
                        placement_cluster_epoch: staged.placement_cluster_epoch,
                        ec_k: staged.ec_k,
                        ec_m: staged.ec_m,
                    })
                    .collect::<Vec<_>>();
                Ok::<_, ()>(crate::PreparedStreamPartCommit {
                    value: part.clone(),
                    part,
                    segments,
                })
            },
        )
        .unwrap()
        .unwrap()
        .value;
    assert_eq!(staged_segments.len(), 2);
    part
}

fn complete_multipart_trace_upload(
    cluster: &crate::StorageCluster,
    map: &LocalClusterMap,
    node_ids: &[NodeId],
    bucket: &crate::BucketName,
    upload: &MultipartTraceUpload,
) -> TestCaseResult {
    if upload.parts.is_empty() {
        return Ok(());
    }
    let upload_row = cluster
        .load_in_progress_multipart_upload(bucket, &upload.key, &upload.upload_id)
        .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    let requested_parts = upload
        .parts
        .iter()
        .map(|part| part.part_number)
        .collect::<Vec<_>>();
    let completion_snapshot = cluster
        .load_multipart_completion_snapshot(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload_row.clone()),
            &requested_parts,
        )
        .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    let size = upload.parts.iter().map(|part| part.size).sum();
    cluster
        .complete_multipart_upload_commit_serialized(
            crate::CompleteMultipartCommitRequest {
                bucket: bucket.clone(),
                key: upload.key.clone(),
                upload_id: upload.upload_id.clone(),
                versioning: crate::BucketVersioningState::Disabled,
                owner: upload_row.owner,
                acl_grants: upload_row.acl_grants,
                public_read: upload_row.public_read,
                generation_id: upload_row.object_generation_id,
                size,
                etag_crc64: [0x51; 8],
                tags: upload_row.tags,
                metadata_blob: Some(upload_row.metadata_blob),
                system_metadata_blob: Some(upload_row.system_metadata_blob),
                object_lock: upload_row.object_lock,
                encryption: upload_row.encryption,
                expected_stale_payload_source: completion_snapshot.stale_payload_source,
                part_records: upload.parts.clone(),
                selected_streaming_segments: completion_snapshot.selected_streaming_segments,
                expected_cleanup: completion_snapshot.cleanup,
            },
            16,
        )
        .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    assert_terminal_multipart_upload_invariants(
        map,
        node_ids,
        upload.object_pg,
        bucket,
        &upload.key,
        &upload.upload_id,
        TerminalMultipartOutcome::Completed,
    );
    assert_clean_metadata_command_stream(map, &[1, upload.object_pg]);
    Ok(())
}

fn abort_multipart_trace_upload(
    cluster: &crate::StorageCluster,
    map: &LocalClusterMap,
    node_ids: &[NodeId],
    bucket: &crate::BucketName,
    upload: &MultipartTraceUpload,
) -> TestCaseResult {
    cluster
        .abort_multipart_upload(bucket, &upload.key, &upload.upload_id)
        .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    assert_terminal_multipart_upload_invariants(
        map,
        node_ids,
        upload.object_pg,
        bucket,
        &upload.key,
        &upload.upload_id,
        TerminalMultipartOutcome::Aborted,
    );
    assert_clean_metadata_command_stream(map, &[upload.object_pg]);
    Ok(())
}

fn delete_recreate_multipart_trace_bucket(
    cluster: &crate::StorageCluster,
    map: &LocalClusterMap,
    bucket: &crate::BucketName,
) -> TestCaseResult {
    cluster
        .begin_bucket_delete(bucket)
        .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    let outcome = cluster
        .try_finalize_bucket_delete(bucket)
        .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    prop_assert_eq!(outcome, crate::BucketDeleteFinalizeOutcome::Finalized);
    assert_clean_metadata_command_stream(map, &[1]);
    create_test_bucket(cluster, bucket);
    assert_clean_metadata_command_stream(map, &[1]);
    Ok(())
}

fn run_multipart_trace(ops: &[MultipartTraceOp]) -> TestCaseResult {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut initial_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape)
        .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    let topology = initial_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "mpu-trace-");
    let keys = [
        (key_for_object_pg(topology, &bucket, 2, "object-a-"), 2_u32),
        (key_for_object_pg(topology, &bucket, 3, "object-b-"), 3_u32),
    ];
    set_route_primary(&mut initial_map, 1, NodeId::new(1));
    set_route_primary(&mut initial_map, 2, NodeId::new(1));
    set_route_primary(&mut initial_map, 3, NodeId::new(2));

    let mut map = Arc::new(initial_map);
    let mut cluster = crate::StorageCluster::from_local_map(Arc::clone(&map))
        .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    create_test_bucket(&cluster, &bucket);

    let mut active_uploads: [Option<MultipartTraceUpload>; 3] = [None, None, None];
    let mut completed_object_exists = false;
    for (step, op) in ops.iter().enumerate() {
        match op {
            MultipartTraceOp::Create {
                slot_seed,
                key_seed,
            } => {
                let slot = multipart_trace_slot(*slot_seed);
                if active_uploads[slot].is_some() {
                    continue;
                }
                let (key, object_pg) = &keys[usize::from(*key_seed % 2)];
                let upload_id = multipart_trace_upload_id(step, *slot_seed);
                create_multipart_trace_upload(&cluster, &bucket, key, upload_id.clone())?;
                active_uploads[slot] = Some(MultipartTraceUpload {
                    upload_id,
                    key: key.clone(),
                    object_pg: *object_pg,
                    next_part_number: 1,
                    parts: Vec::new(),
                });
            }
            MultipartTraceOp::UploadStreamPart {
                slot_seed,
                payload_seed,
            } => {
                let Some(upload) = active_uploads[multipart_trace_slot(*slot_seed)].as_mut() else {
                    continue;
                };
                if upload.next_part_number > 3 {
                    continue;
                }
                let segment_seed = payload_seed.wrapping_add(step as u8).max(1);
                let payload = format!("multipart trace payload {step} {payload_seed}");
                let (_shard_keys, part, _segment) = upload_streamed_test_multipart_part(
                    &cluster,
                    &bucket,
                    &upload.key,
                    &upload.upload_id,
                    upload.next_part_number,
                    [segment_seed; 16],
                    payload.as_bytes(),
                );
                upload.parts.push(part);
                upload.next_part_number += 1;
            }
            MultipartTraceOp::UploadCopiedPart {
                slot_seed,
                payload_seed,
            } => {
                let Some(upload) = active_uploads[multipart_trace_slot(*slot_seed)].as_mut() else {
                    continue;
                };
                if upload.next_part_number > 3 {
                    continue;
                }
                let first_payload = format!("copied trace payload {step} {payload_seed} a");
                let second_payload = format!("copied trace payload {step} {payload_seed} b");
                let part = upload_copied_test_multipart_part(
                    &cluster,
                    &bucket,
                    &upload.key,
                    &upload.upload_id,
                    upload.next_part_number,
                    payload_seed.wrapping_add(step as u8).max(1),
                    [first_payload.as_bytes(), second_payload.as_bytes()],
                );
                upload.parts.push(part);
                upload.next_part_number += 1;
            }
            MultipartTraceOp::Complete { slot_seed } => {
                let slot = multipart_trace_slot(*slot_seed);
                let Some(upload) = active_uploads[slot].take() else {
                    continue;
                };
                if upload.parts.is_empty() {
                    active_uploads[slot] = Some(upload);
                    continue;
                }
                complete_multipart_trace_upload(&cluster, &map, &node_ids, &bucket, &upload)?;
                completed_object_exists = true;
            }
            MultipartTraceOp::Abort { slot_seed } => {
                let slot = multipart_trace_slot(*slot_seed);
                let Some(upload) = active_uploads[slot].take() else {
                    continue;
                };
                abort_multipart_trace_upload(&cluster, &map, &node_ids, &bucket, &upload)?;
            }
            MultipartTraceOp::DeleteRecreateBucket => {
                if completed_object_exists || active_uploads.iter().any(Option::is_some) {
                    continue;
                }
                delete_recreate_multipart_trace_bucket(&cluster, &map, &bucket)?;
            }
            MultipartTraceOp::StaleCreate {
                slot_seed,
                key_seed,
            } => {
                let stale_cluster = stale_multipart_trace_cluster(&map);
                let (key, _) = &keys[usize::from(*key_seed % 2)];
                let upload_id = multipart_trace_upload_id(step, *slot_seed);
                stale_create_multipart_trace_upload(
                    &cluster,
                    &stale_cluster,
                    &bucket,
                    key,
                    upload_id,
                )?;
            }
            MultipartTraceOp::StaleUploadPartCreate {
                slot_seed,
                payload_seed,
            } => {
                let Some(upload) = active_uploads[multipart_trace_slot(*slot_seed)].as_ref() else {
                    continue;
                };
                let stale_cluster = stale_multipart_trace_cluster(&map);
                let session_id = trace_session(payload_seed.wrapping_add(step as u8));
                stale_create_upload_part_stream_session(
                    &cluster,
                    &stale_cluster,
                    &map,
                    &bucket,
                    upload,
                    session_id,
                )?;
            }
            MultipartTraceOp::StaleAbort { slot_seed } => {
                let Some(upload) = active_uploads[multipart_trace_slot(*slot_seed)].as_ref() else {
                    continue;
                };
                let stale_cluster = stale_multipart_trace_cluster(&map);
                stale_abort_multipart_trace_upload(
                    &cluster,
                    &stale_cluster,
                    &map,
                    &bucket,
                    upload,
                )?;
            }
            MultipartTraceOp::Reopen => {
                drop(cluster);
                drop(map);
                let mut reopened = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape)
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                set_route_primary(&mut reopened, 1, NodeId::new(1));
                set_route_primary(&mut reopened, 2, NodeId::new(1));
                set_route_primary(&mut reopened, 3, NodeId::new(2));
                map = Arc::new(reopened);
                cluster = crate::StorageCluster::from_local_map(Arc::clone(&map))
                    .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
                assert_clean_metadata_command_stream(&map, &[1, 2, 3]);
            }
        }
    }

    for upload in active_uploads.into_iter().flatten() {
        abort_multipart_trace_upload(&cluster, &map, &node_ids, &bucket, &upload)?;
    }
    assert_clean_metadata_command_stream(&map, &[1, 2, 3]);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 24,
        .. ProptestConfig::default()
    })]

    #[test]
    fn prop_multipart_trace_preserves_terminal_lifecycle_invariants(
        ops in multipart_trace_strategy()
    ) {
        run_multipart_trace(&ops)?;
    }
}

#[test]
fn multipart_trace_exercises_bucket_delete_recreate() {
    run_multipart_trace(&[
        MultipartTraceOp::DeleteRecreateBucket,
        MultipartTraceOp::Create {
            slot_seed: 0,
            key_seed: 0,
        },
        MultipartTraceOp::Abort { slot_seed: 0 },
        MultipartTraceOp::DeleteRecreateBucket,
        MultipartTraceOp::StaleCreate {
            slot_seed: 2,
            key_seed: 1,
        },
        MultipartTraceOp::Reopen,
        MultipartTraceOp::Create {
            slot_seed: 1,
            key_seed: 1,
        },
        MultipartTraceOp::UploadCopiedPart {
            slot_seed: 1,
            payload_seed: 9,
        },
        MultipartTraceOp::StaleUploadPartCreate {
            slot_seed: 1,
            payload_seed: 7,
        },
        MultipartTraceOp::StaleAbort { slot_seed: 1 },
        MultipartTraceOp::Complete { slot_seed: 1 },
    ])
    .unwrap();
}
