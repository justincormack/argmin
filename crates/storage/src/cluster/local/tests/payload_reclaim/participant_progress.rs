// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::ObjectPgActionError;

fn publish_segments(
    cluster: &StorageCluster,
    bucket: &BucketName,
    key: &ObjectKey,
    multipart: bool,
) -> (GenerationId, Vec<crate::SegmentStoredBytesRequest>) {
    if multipart {
        let upload_id = upload_id_from_label("reclaimprogress");
        let request = crate::CreateMultipartUploadReq {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            tags: None,
            metadata_blob: crate::SerializedMetadataBlob::default(),
            system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
            initiator: crate::OwnerIdentity::from_principal("owner"),
            owner: crate::OwnerIdentity::from_principal("owner"),
            acl_grants: crate::AclGrants::default(),
            public_read: false,
            object_lock: crate::ObjectLockState::default(),
            checksum: None,
            encryption: sse_s3_multipart_test_encryption(),
        };
        cluster
            .create_multipart_upload(
                bucket,
                key,
                crate::BucketSnapshotRequest::default(),
                |_, _| Ok::<_, ()>(((), request.clone())),
            )
            .unwrap()
            .unwrap();
        let upload = cluster
            .load_in_progress_multipart_upload(bucket, key, &upload_id)
            .unwrap();
        let mut parts = Vec::new();
        let mut segments = Vec::new();
        let mut reads = Vec::new();
        let mut encryption = None;
        // Non-final parts meet the S3 minimum; both parts contain real ciphertext.
        for (index, payload) in [vec![0x41; 5 * 1024 * 1024], b"last part".to_vec()]
            .iter()
            .enumerate()
        {
            let (_, part, segment, encrypted) = upload_streamed_test_multipart_part(
                cluster,
                bucket,
                key,
                &upload_id,
                u32::try_from(index + 1).unwrap(),
                [0xB1; 16],
                payload,
            );
            let read = encrypted_test_multipart_part_segment_read_request(&segment, &encrypted);
            let mut bytes = Vec::new();
            cluster
                .read_segment_payload_stored_bytes_into(read, &mut bytes)
                .unwrap();
            assert_eq!(bytes, encrypted.storage_bytes);
            reads.push(read);
            encryption = Some(committed_test_object_encryption_for_incarnation(
                &encrypted,
                cluster
                    .load_bucket_snapshot(bucket, crate::BucketSnapshotRequest::default())
                    .unwrap()
                    .bucket_incarnation_read_subject()
                    .bucket_incarnation_generation(),
                bucket,
                key,
                upload.object_generation_id,
            ));
            parts.push(part);
            segments.push(segment);
        }
        cluster
            .complete_multipart_upload_commit_serialized(crate::CompleteMultipartCommitRequest {
                bucket: bucket.clone(),
                key: key.clone(),
                upload_id,
                completion_fingerprint: crate::MultipartCompletionFingerprint::from_bytes(
                    [0x31; 32],
                ),
                versioning: crate::BucketVersioningState::Disabled,
                owner: upload.owner,
                acl_grants: upload.acl_grants,
                public_read: upload.public_read,
                generation_id: upload.object_generation_id,
                size: parts.iter().map(|part| part.size).sum(),
                etag_crc64: [0x44; 8],
                tags: upload.tags,
                metadata_blob: Some(upload.metadata_blob),
                system_metadata_blob: Some(upload.system_metadata_blob),
                object_lock: upload.object_lock,
                encryption: encryption.unwrap(),
                expected_stale_payload_source: None,
                expected_current_object_identity: None,
                conditional_completion: false,
                part_records: parts,
                selected_streaming_segments: segments,
                expected_cleanup: crate::CompleteMultipartCommitCleanup::default(),
            })
            .unwrap();
        (upload.object_generation_id, reads)
    } else {
        let session = crate::tests::stream_session_id("reclaimprogress");
        let payloads = [vec![0x42; 8 * 1024 * 1024], b"last segment".to_vec()];
        let encrypted = payloads
            .iter()
            .enumerate()
            .map(|(index, bytes)| {
                crate::tests::sse_s3_segment_test_fixture(bytes, u32::try_from(index).unwrap())
            })
            .collect::<Vec<_>>();
        cluster
            .create_put_object_stream_session_record(
                bucket,
                key,
                &session,
                encrypted[0].encryption.clone(),
            )
            .unwrap();
        let generation = cluster
            .test_object_generation_reservation_for(bucket, key, &session)
            .unwrap();
        let reads = encrypted
            .iter()
            .enumerate()
            .map(|(index, encrypted)| {
                let segment = append_encrypted_test_multipart_stream_segment(
                    cluster,
                    bucket,
                    key,
                    &session,
                    u32::try_from(index).unwrap(),
                    encrypted,
                );
                assert_encrypted_test_stream_segment_stored(cluster, &segment, encrypted);
                encrypted_test_stream_segment_read_request(&segment, encrypted)
            })
            .collect::<Vec<_>>();
        let encryption =
            committed_stream_test_encryption(cluster, bucket, key, &session, &encrypted[0]);
        cluster
            .finalize_put_object_stream(
                bucket,
                key,
                &session,
                payloads.iter().map(|p| p.len() as u64).sum(),
                |_| {
                    Ok::<_, ()>(crate::PreparedStreamPutCommit {
                        value: (),
                        versioning: crate::BucketVersioningState::Disabled,
                        owner: crate::OwnerIdentity::from_principal("owner"),
                        acl_grants: crate::AclGrants::default(),
                        public_read: false,
                        etag_crc64: checksum::crc64::checksum(&payloads.concat()),
                        tags: None,
                        metadata_blob: crate::SerializedMetadataBlob::default(),
                        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                        object_lock: crate::ObjectLockState::default(),
                        encryption: encryption.clone(),
                    })
                },
            )
            .unwrap()
            .unwrap();
        (generation, reads)
    }
}

fn check_reclaim_progress(multipart: bool, fail_file: bool, fail_ack: bool) {
    let tmp = test_util::tempdir();
    let ec = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &trace_node_ids(), &[0], ec).unwrap();
    set_route_primary(&mut map, 0, NodeId::new(2));
    let map = Arc::new(map);
    let cluster = current_cluster(&map);
    let bucket = crate::tests::bucket_name("reclaim-progress");
    let key = crate::tests::object_key("target");
    create_test_bucket(&cluster, &bucket);
    let (generation, reads) = publish_segments(&cluster, &bucket, &key, multipart);
    let canary_key = crate::tests::object_key("canary");
    let canary = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &canary_key,
        crate::BucketVersioningState::Disabled,
        [0xC1; 16],
        [0xC2; 16],
        b"unrelated live payload",
    );
    cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<_, ()>(()))
        .unwrap()
        .unwrap();
    let root = || {
        let pg = map
            .node(NodeId::new(2))
            .unwrap()
            .storage_node()
            .get_pg(0)
            .unwrap();
        (
            PgMetadataStore::get_object_segments_reclaim(&*pg, &bucket, &key, generation).unwrap(),
            PgMetadataStore::get_multipart_reclaim(&*pg, &bucket, &key, generation).unwrap(),
        )
    };
    let original_root = root();
    assert_eq!(original_root.0.is_some(), !multipart);
    assert_eq!(original_root.1.is_some(), multipart);
    let expected_keys = reads
        .iter()
        .flat_map(|read| {
            (0..ec.k + ec.m)
                .map(move |index| ShardKey::new(&read.segment_okh, read.segment_vid.get(), index))
        })
        .collect::<Vec<_>>();
    assert_eq!(expected_keys.len(), 6);
    let ack_exists = |data_pg_id: u32, shard_key: &ShardKey| {
        map.metadata_pg_primary_node(cluster.operation_epoch(), PgId::new(data_pg_id))
            .unwrap()
            .storage_node()
            .test_shard_exists(data_pg_id, shard_key)
            .unwrap()
    };
    for read in &reads {
        for index in 0..ec.k + ec.m {
            let shard_key = ShardKey::new(&read.segment_okh, read.segment_vid.get(), index);
            assert!(ack_exists(read.data_pg_id, &shard_key));
            assert!(cluster
                .test_payload_shard_file_exists(
                    read.data_pg_id,
                    ec,
                    &read.segment_okh,
                    read.segment_vid,
                    index,
                )
                .unwrap());
        }
    }
    let failed_key = expected_keys[0].clone();
    let files = Arc::new(Mutex::new(Vec::new()));
    let acks = Arc::new(Mutex::new(Vec::new()));
    let file_calls = Arc::clone(&files);
    let failed_file = failed_key.clone();
    let file_hook =
        cluster.test_install_before_placed_payload_shard_delete_hook(Arc::new(move |key| {
            file_calls.lock().unwrap().push(key.clone());
            if fail_file && *key == failed_file {
                Err(StoreError::Io {
                    context: "injected reclaim progress file failure",
                    source: std::io::Error::other("injected"),
                })
            } else {
                Ok(())
            }
        }));
    let ack_calls = Arc::clone(&acks);
    let failed_ack = failed_key.clone();
    let ack_hook = cluster.test_install_before_metadata_primary_payload_ack_delete_hook(Arc::new(
        move |key| {
            ack_calls.lock().unwrap().push(key.clone());
            if fail_ack && *key == failed_ack {
                Err(StoreError::Io {
                    context: "injected reclaim progress ack failure",
                    source: std::io::Error::other("injected"),
                })
            } else {
                Ok(())
            }
        },
    ));
    let initial_index = map.test_next_metadata_command_log_index(PgId::new(0));
    let error = cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, generation)
        .unwrap_err();
    assert!(
        matches!(error, ObjectPgActionError::Store(StoreError::Io { context, .. })
        if context == if fail_file { "injected reclaim progress file failure" } else { "injected reclaim progress ack failure" })
    );
    assert_eq!(
        *files.lock().unwrap(),
        expected_keys,
        "attempt every file across both segments/parts"
    );
    assert_eq!(
        *acks.lock().unwrap(),
        expected_keys,
        "attempt acknowledgements independently of file failures"
    );
    assert_eq!(
        root(),
        original_root,
        "partial cleanup must retain the exact durable root"
    );
    assert_eq!(
        map.test_next_metadata_command_log_index(PgId::new(0)),
        initial_index
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(0), &bucket).is_none());
    assert!(matches!(
        cluster.acquire_object_payload_lease(&bucket, &key, generation),
        Err(StoreError::NotFound)
    ));
    let assert_effects = |retry_complete: bool| {
        for read in &reads {
            for index in 0..ec.k + ec.m {
                let shard_key = ShardKey::new(&read.segment_okh, read.segment_vid.get(), index);
                assert_eq!(
                    cluster
                        .test_payload_shard_file_exists(
                            read.data_pg_id,
                            ec,
                            &read.segment_okh,
                            read.segment_vid,
                            index
                        )
                        .unwrap(),
                    !retry_complete && fail_file && shard_key == failed_key
                );
                assert_eq!(
                    ack_exists(read.data_pg_id, &shard_key),
                    !retry_complete && fail_ack && shard_key == failed_key
                );
            }
        }
        for index in 0..ec.k + ec.m {
            let shard_key = ShardKey::new(&canary.segment_okh, canary.generation_id.get(), index);
            assert!(cluster
                .test_payload_shard_file_exists(
                    canary.written.data_pg_id,
                    ec,
                    &canary.segment_okh,
                    canary.generation_id,
                    index
                )
                .unwrap());
            assert!(ack_exists(canary.written.data_pg_id, &shard_key));
        }
    };
    assert_effects(false);
    drop(file_hook);
    drop(ack_hook);
    assert!(cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, generation)
        .unwrap());
    assert_eq!(root(), (None, None));
    assert_effects(true);
    let completed_index = map.test_next_metadata_command_log_index(PgId::new(0));
    assert!(!cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, generation)
        .unwrap());
    assert_eq!(
        map.test_next_metadata_command_log_index(PgId::new(0)),
        completed_index
    );
}

#[test]
fn segmented_reclaim_attempts_all_participants_and_retains_failed_obligations() {
    let _serial = lock_payload_cleanup_hook_test();
    for (file, ack) in [(true, false), (false, true), (true, true)] {
        check_reclaim_progress(false, file, ack);
    }
}

#[test]
fn multipart_reclaim_attempts_all_participants_and_retains_failed_obligations() {
    let _serial = lock_payload_cleanup_hook_test();
    for (file, ack) in [(true, false), (false, true), (true, true)] {
        check_reclaim_progress(true, file, ack);
    }
}

#[test]
fn reclaim_invalid_placement_rejects_before_either_cleanup_participant() {
    let _serial = lock_payload_cleanup_hook_test();
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &trace_node_ids(), &[0], EcShape { k: 2, m: 1 }).unwrap(),
    );
    let cluster = current_cluster(&map);
    let committed = write_committed_direct_segment(&cluster, b"invalid placement canary");
    let assert_present = || {
        for index in 0..3 {
            let key = ShardKey::new(&committed.segment_okh, committed.generation_id.get(), index);
            assert!(cluster
                .test_shard_exists(committed.written.data_pg_id, &key)
                .unwrap());
            assert!(cluster
                .test_payload_shard_file_exists(
                    committed.written.data_pg_id,
                    committed.written.ec,
                    &committed.segment_okh,
                    committed.generation_id,
                    index,
                )
                .unwrap());
        }
    };
    assert_present();
    let _files = cluster.test_install_before_placed_payload_shard_delete_hook(Arc::new(|_| {
        panic!("invalid placement reached file deletion");
    }));
    let _acks =
        cluster.test_install_before_metadata_primary_payload_ack_delete_hook(Arc::new(|_| {
            panic!("invalid placement reached acknowledgement deletion");
        }));
    // The subject has real files/acks, but zero data shards is invalid. A
    // route-construction failure is not an independent participant effect error.
    assert!(cluster
        .delete_payload_shard_set(
            committed.written.data_pg_id,
            EcShape { k: 0, m: 1 },
            &committed.segment_okh,
            committed.generation_id,
        )
        .is_err());
    assert_present();
}
