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
) -> crate::StreamUploadSegmentRecord {
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
        .unwrap()
        .1
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
    );
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

    // A lost preparation reply can consume another VID without an append.
    // Do not let the experiment imply replica_floor + 1 bounds all issued VIDs.
    let discarded = prepare(
        &cluster,
        &bucket,
        &key,
        &session_id,
        &encrypted[1],
        1,
        &admission,
    );
    let second = prepare(
        &cluster,
        &bucket,
        &key,
        &session_id,
        &encrypted[1],
        1,
        &admission,
    );
    assert_ne!(second.data_pg_id, 0);
    assert_eq!(discarded.segment_okh, second.segment_okh);
    assert!(second.segment_vid.get() > discarded.segment_vid.get());
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

    // Scan actual files, not expected paths from the discarded preparation.
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
