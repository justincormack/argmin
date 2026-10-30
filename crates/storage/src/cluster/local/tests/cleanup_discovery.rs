// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Evidence for the cleanup design, not a failover/cleanup implementation test.
//! Run actual preparation, file publication, ack registration and append phases,
//! then reopen only a replica's stores. No old-primary or frontend state is used
//! to discover filenames; captured expected records are comparison canaries only.

use super::*;
use crate::node::SharedStorageNode;
use crate::PgMetadataStore;

#[derive(Clone, Copy, Debug)]
enum Target {
    PutObject,
    UploadPart,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StopAfter {
    Preparation,
    Files,
    Acknowledgements,
    Append,
}

fn prepare(
    cluster: &StorageCluster,
    bucket: &crate::BucketName,
    key: &crate::ObjectKey,
    session_id: &crate::SessionId,
    encrypted: &crate::tests::SseS3SegmentTestFixture,
    index: u32,
    admission: &crate::StorageClusterRouteAdmission,
) -> Result<crate::StreamUploadSegmentRecord, crate::ObjectPgActionError> {
    let expected = {
        let primary = cluster.local_map.node(NodeId::new(0)).unwrap();
        let pg = primary.storage_node().get_pg(0).unwrap();
        let session = pg.get_stream_upload(session_id).unwrap();
        let generation_id = pg
            .get_object_generation_reservation(bucket, key, session_id)
            .unwrap();
        match &session.target {
            crate::StreamUploadTarget::PutObject => crate::ExpectedStreamAppendState::PutObject {
                session,
                generation_id,
            },
            crate::StreamUploadTarget::UploadPart { upload_id, .. } => {
                crate::ExpectedStreamAppendState::UploadPart {
                    upload: Box::new(pg.get_multipart_upload(upload_id).unwrap()),
                    session,
                    part_generation_id: generation_id,
                }
            }
        }
    };
    cluster
        .prepare_stream_segment_append_with_route_validation(
            crate::cluster::PutObjectMutationEffectRoute {
                bucket_pg_id: cluster.bucket_metadata_pg(bucket),
                object_pg_id: cluster.object_metadata_pg(bucket, key),
                bucket,
                key,
                effect_fence: admission.effect_fence(),
            },
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: index,
                size: encrypted.logical_size,
                segment_crc64: encrypted.storage_crc64,
                payload_crc64: encrypted.payload_crc64,
            },
            Some(&expected),
            || admission.require_valid_now_raw(),
        )
        .map(|(_, segment)| segment)
}

fn inspect_surviving_replica(target: Target, stop: StopAfter) {
    let tmp = test_util::tempdir();
    let nodes = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &nodes, &pg_ids, ec).unwrap();
    let topology = map.node(nodes[0]).unwrap().storage_node().pg_topology();
    let bucket = crate::BucketName::try_from("discovery-bucket").unwrap();
    // Object primary 0 will be unavailable. Both segment ack PGs have primary 1,
    // so their rows can independently survive loss of the object primary.
    let key = (0..10_000)
        .map(|i| crate::ObjectKey::try_from(format!("key-{i}")).unwrap())
        .find(|key| {
            topology.object_pg_for(&bucket, key) == 0
                && (0..2).all(|index| {
                    topology
                        .object_generation_segment_data_pg(
                            &bucket,
                            key,
                            crate::GenerationId::MIN,
                            index,
                        )
                        .get()
                        != 0
                        && topology
                            .object_generation_multipart_part_segment_data_pg(
                                &bucket,
                                key,
                                crate::GenerationId::MIN,
                                2,
                                index,
                            )
                            .get()
                            != 0
                })
        })
        .expect("fixture needs separate object and acknowledgement PGs");
    for pg_id in [1, 2, 3] {
        map.pg_routes
            .get_mut(&PgId::new(pg_id))
            .unwrap()
            .primary_node_id = nodes[1];
    }
    let survivor_dir = map.node(nodes[1]).unwrap().data_dir().to_path_buf();
    let cluster = StorageCluster::from_static_local_map(Arc::new(map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("b7".repeat(16)).unwrap();
    // Production streams flush a full internal segment before their final tail.
    let first_plaintext = vec![0x3a; 8 * 1024 * 1024];
    let tail = b"second segment discovery canary";
    let encrypted = match target {
        Target::PutObject => [
            crate::tests::sse_s3_segment_test_fixture(&first_plaintext, 0),
            crate::tests::sse_s3_segment_test_fixture(tail, 1),
        ],
        Target::UploadPart => [
            crate::tests::sse_s3_multipart_segment_test_fixture(&first_plaintext, 2, 0),
            crate::tests::sse_s3_multipart_segment_test_fixture(tail, 2, 1),
        ],
    };
    match target {
        Target::PutObject => cluster
            .create_put_object_stream_session_record(
                &bucket,
                &key,
                &session_id,
                encrypted[0].encryption.clone(),
            )
            .unwrap(),
        Target::UploadPart => {
            let (completion, _) =
                seed_streamed_multipart_completion(&cluster, &bucket, &key, "discovery");
            let upload = cluster
                .load_in_progress_multipart_upload(&bucket, &key, &completion.upload_id)
                .unwrap();
            assert_eq!(upload.object_generation_id, crate::GenerationId::MIN);
            assert_eq!(upload.encryption, encrypted[0].encryption);
            cluster
                .create_upload_part_stream_session(
                    &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
                    2,
                    &session_id,
                )
                .unwrap();
        }
    }
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let write = |segment: &crate::StreamUploadSegmentRecord, bytes: &[u8]| {
        cluster
            .write_stream_segment_payload_shards_with_route_validation::<StoreError>(
                segment,
                bytes,
                admission.effect_fence(),
                || admission.require_valid_now_raw(),
                &mut || Ok(()),
            )
            .unwrap()
    };
    let first = prepare(
        &cluster,
        &bucket,
        &key,
        &session_id,
        &encrypted[0],
        0,
        &admission,
    )
    .unwrap();
    assert_ne!(first.data_pg_id, 0);
    let first_written = write(&first, &encrypted[0].storage_bytes);
    let batch = first_written
        .iter()
        .map(|shard| (&shard.key, shard.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(&bucket, &key, &session_id, 0, &first, &batch)
        .unwrap();
    assert_encrypted_test_stream_segment_stored(&cluster, &first, &encrypted[0]);
    let (before, first_manifest, before_log) = {
        let node = cluster.local_map.node(nodes[1]).unwrap();
        let pg = node.storage_node().get_pg(0).unwrap();
        (
            pg.get_stream_upload(&session_id).unwrap(),
            pg.list_stream_segments(&session_id).unwrap(),
            pg.metadata_command_replica_state().unwrap(),
        )
    };
    assert_eq!(first_manifest.len(), 1);
    assert_eq!(first_manifest[0].segment_vid, first.segment_vid);

    // One preparation consumes the current local slot. Losing that response
    // does not authorize another candidate; the replica still cannot establish
    // issuer continuity, placement or a finite physical-write cutoff from this.
    let second = prepare(
        &cluster,
        &bucket,
        &key,
        &session_id,
        &encrypted[1],
        1,
        &admission,
    )
    .unwrap();
    let primary_before_retry = cluster
        .local_map
        .node(nodes[0])
        .unwrap()
        .storage_node()
        .get_pg(0)
        .unwrap()
        .get_stream_upload(&session_id)
        .unwrap();
    let retry = prepare(
        &cluster,
        &bucket,
        &key,
        &session_id,
        &encrypted[1],
        1,
        &admission,
    )
    .unwrap_err();
    assert!(
        matches!(retry, crate::ObjectPgActionError::InvalidRequest { reason }
        if reason.contains("unconsumed local slot"))
    );
    assert_eq!(
        cluster
            .local_map
            .node(nodes[0])
            .unwrap()
            .storage_node()
            .get_pg(0)
            .unwrap()
            .get_stream_upload(&session_id)
            .unwrap(),
        primary_before_retry
    );
    assert_ne!(second.data_pg_id, 0);
    assert_eq!(second.segment_vid, before.next_segment_vid);
    let primary_floor = cluster
        .local_map
        .node(nodes[0])
        .unwrap()
        .storage_node()
        .get_pg(0)
        .unwrap()
        .get_stream_upload(&session_id)
        .unwrap()
        .next_segment_vid;
    assert!(primary_floor.get() > before.next_segment_vid.get());
    let mut expected_second = second.clone();
    if stop != StopAfter::Preparation {
        let written = write(&second, &encrypted[1].storage_bytes);
        let batch = written
            .iter()
            .map(|shard| (&shard.key, shard.ack))
            .collect::<Vec<_>>();
        if matches!(stop, StopAfter::Acknowledgements | StopAfter::Append) {
            let witness = StorageCluster::build_payload_shard_integrity_witness(
                second.data_pg_id,
                ec,
                &second.segment_okh,
                second.segment_vid,
                encrypted[1].storage_bytes.len() as u64,
                second.segment_crc64,
                &batch,
            )
            .unwrap();
            expected_second.shard_integrity = Some(witness.durable_record());
            cluster
                .register_validated_payload_shard_acks(&witness)
                .unwrap();
        }
        if stop == StopAfter::Append {
            cluster
                .commit_stream_segment_append(&bucket, &key, &session_id, 1, &second, &batch)
                .unwrap();
            assert_encrypted_test_stream_segment_stored(&cluster, &second, &encrypted[1]);
        }
    }

    // No RAII payload handle repairs the missing manifest. Drop all runtimes
    // before reopening: this is a persisted-evidence test, not a power-loss or
    // control-plane election test. The original primary is never reopened.
    drop(admission);
    drop(handle);
    drop(cluster);
    let survivor =
        SharedStorageNode::open_with_default_ec_shape(&survivor_dir, &pg_ids, ec).unwrap();
    let pg = survivor.get_pg(0).unwrap();
    let after = pg.get_stream_upload(&session_id).unwrap();
    let manifest = pg.list_stream_segments(&session_id).unwrap();
    assert_eq!(manifest[0], first_manifest[0]);
    if stop == StopAfter::Append {
        assert_eq!(after.next_segment_vid, primary_floor);
        assert_eq!(manifest.len(), 2);
        assert_eq!(manifest[1], expected_second);
        assert!(
            pg.metadata_command_replica_state()
                .unwrap()
                .applied_log_index
                > before_log.applied_log_index
        );
    } else {
        assert_eq!(
            after, before,
            "{target:?} at {stop:?}: preparation is primary-local"
        );
        assert_eq!(manifest, first_manifest);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), before_log);
    }
    drop(pg);

    // Scan actual files, not paths supplied by the original preparation.
    // Filtering uses the known next index as an experimental candidate, NOT
    // a production-authorized frontier (none exists in the current schema).
    let hash = crate::stream_segment_key_hash(&session_id, 1);
    let mut files = Vec::new();
    for pg_id in pg_ids {
        let scan = survivor.list_scavenger_shard_files(pg_id).unwrap();
        assert!(scan.errors.is_empty());
        files.extend(
            scan.files
                .into_iter()
                .filter(|file| file.key.as_bytes()[..16] == hash)
                .map(|file| (pg_id, file)),
        );
    }
    assert_eq!(files.len(), usize::from(stop != StopAfter::Preparation));
    if let Some((pg_id, file)) = files.first() {
        assert_eq!(*pg_id, second.data_pg_id);
        assert_eq!(file.key, ShardKey::new(&hash, second.segment_vid.get(), 1));
        assert_eq!(
            file.size,
            (encrypted[1].storage_bytes.len() as u64).div_ceil(u64::from(ec.k))
        );
    }
    let rows = survivor
        .get_pg(second.data_pg_id)
        .unwrap()
        .list_scavenger_shard_rows()
        .unwrap();
    let matching = rows
        .iter()
        .filter(|row| row.key.as_bytes()[..16] == hash)
        .collect::<Vec<_>>();
    let has_acks = matches!(stop, StopAfter::Acknowledgements | StopAfter::Append);
    assert_eq!(
        matching.len(),
        if has_acks {
            usize::from(ec.k + ec.m)
        } else {
            0
        }
    );
    for row in matching {
        assert_eq!(
            row.key,
            ShardKey::new(&hash, second.segment_vid.get(), row.key.shard_index().get())
        );
    }
}

#[test]
fn streamed_put_replica_discovery_evidence_at_each_durable_boundary() {
    for stop in [
        StopAfter::Preparation,
        StopAfter::Files,
        StopAfter::Acknowledgements,
        StopAfter::Append,
    ] {
        inspect_surviving_replica(Target::PutObject, stop);
    }
}

#[test]
fn upload_part_replica_discovery_evidence_at_each_durable_boundary() {
    for stop in [
        StopAfter::Preparation,
        StopAfter::Files,
        StopAfter::Acknowledgements,
        StopAfter::Append,
    ] {
        inspect_surviving_replica(Target::UploadPart, stop);
    }
}

// Production workflow evidence for the proposed one-slot policy. These tests
// do not claim that today's allocator enforces a replicated window. They force
// a real command-ID collision after preparation and check that the append owner
// retries publication with the same physical identity instead of preparing again.
fn append_publication_retry_consumes_one_vid(target: Target) {
    let tmp = test_util::tempdir();
    let nodes = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec = EcShape { k: 2, m: 1 };
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &nodes, &[0, 1], ec).unwrap());
    let cluster = StorageCluster::from_static_local_map(map.clone()).unwrap();
    let bucket = crate::BucketName::try_from("one-slot-bucket").unwrap();
    let key = crate::ObjectKey::try_from("one-slot-key").unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("cd".repeat(16)).unwrap();
    let plaintext = b"append retries retain the prepared encrypted payload";
    let encrypted = match target {
        Target::PutObject => {
            let encrypted = crate::tests::sse_s3_segment_test_fixture(plaintext, 0);
            cluster
                .create_put_object_stream_session_record(
                    &bucket,
                    &key,
                    &session_id,
                    encrypted.encryption.clone(),
                )
                .unwrap();
            encrypted
        }
        Target::UploadPart => {
            let (completion, _) =
                seed_streamed_multipart_completion(&cluster, &bucket, &key, "oneslot");
            let upload = cluster
                .load_in_progress_multipart_upload(&bucket, &key, &completion.upload_id)
                .unwrap();
            let encrypted = crate::tests::sse_s3_multipart_segment_test_fixture(plaintext, 2, 0);
            assert_eq!(upload.encryption, encrypted.encryption);
            cluster
                .create_upload_part_stream_session(
                    &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
                    2,
                    &session_id,
                )
                .unwrap();
            encrypted
        }
    };
    let pg_id = cluster.object_metadata_pg(&bucket, &key).pg_id();
    let initial_cursor = map
        .node(nodes[0])
        .unwrap()
        .storage_node()
        .get_pg(pg_id.get())
        .unwrap()
        .get_stream_upload(&session_id)
        .unwrap()
        .next_segment_vid;
    let command_builds = Arc::new(AtomicUsize::new(0));
    let hook_builds = command_builds.clone();
    let hook_map = map.clone();
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let _hook =
        cluster.test_install_after_stream_append_command_id_allocated_hook(Arc::new(move |id| {
            if hook_builds.fetch_add(1, Ordering::SeqCst) != 0 {
                return;
            }
            let collision = MetadataCommandEnvelope::new(
                id,
                MetadataCommandPayload::ReleaseObjectGeneration(
                    crate::metadata_command::ReleaseObjectGenerationCommand::new(
                        hook_bucket.clone(),
                        hook_key.clone(),
                        crate::SessionId::try_from("ef".repeat(16)).unwrap(),
                    ),
                ),
            );
            for node in hook_map
                .metadata_pg_acting_nodes(id.cluster_epoch(), id.pg_id())
                .unwrap()
            {
                node.metadata_command_client()
                    .apply_metadata_command_and_record(id.pg_id(), &collision)
                    .unwrap();
            }
        }));
    let preparations = AtomicUsize::new(0);
    cluster
        .test_append_stream_segment_with_after_prepare(
            &bucket,
            &key,
            crate::StreamSegmentAppendInput {
                session_id: &session_id,
                segment_index: 0,
                payload_crc64: encrypted.payload_crc64,
                storage_bytes: &encrypted.storage_bytes,
            },
            || {
                preparations.fetch_add(1, Ordering::SeqCst);
            },
        )
        .expect("publication must retry the collision and finish the append");
    assert!(
        command_builds.load(Ordering::SeqCst) >= 2,
        "fixture must force a publication retry"
    );
    assert_eq!(preparations.load(Ordering::SeqCst), 1);
    let mut committed = None;
    for node_id in nodes {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        let session = pg.get_stream_upload(&session_id).unwrap();
        assert_eq!(session.next_segment_vid.get(), initial_cursor.get() + 1);
        let segments = pg.list_stream_segments(&session_id).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].segment_vid, initial_cursor);
        if let Some(expected) = &committed {
            assert_eq!(&segments[0], expected);
        } else {
            committed = Some(segments[0].clone());
        }
    }
    let segment = committed.unwrap();
    let mut bytes = Vec::new();
    cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: segment.data_pg_id,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                stored_size: encrypted.storage_bytes.len(),
                segment_crc64: encrypted.storage_crc64,
                ec,
            },
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, encrypted.storage_bytes);
}

#[test]
fn put_stream_publication_retry_consumes_one_vid() {
    append_publication_retry_consumes_one_vid(Target::PutObject);
}

#[test]
fn upload_part_publication_retry_consumes_one_vid() {
    append_publication_retry_consumes_one_vid(Target::UploadPart);
}
