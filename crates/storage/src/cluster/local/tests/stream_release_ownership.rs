// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_client::BuildStreamPutCommitCommandReq;
use crate::PgMetadataStore as _;
use crate::{BucketSnapshotLoadError, ObjectPgActionError};

fn empty_stream_terminal_command(
    cluster: &StorageCluster,
    bucket: &BucketName,
    key: &ObjectKey,
    session_id: &crate::SessionId,
) -> MetadataCommandEnvelope {
    let encrypted = crate::tests::sse_s3_segment_test_fixture(&[], 0);
    cluster
        .create_put_object_stream_session_record(
            bucket,
            key,
            session_id,
            encrypted.encryption.clone(),
        )
        .unwrap();
    let encryption = committed_stream_test_encryption(cluster, bucket, key, session_id, &encrypted);
    let client = cluster
        .object_mutation_metadata_primary_client(bucket, key)
        .unwrap();
    let route = client
        .open_stream_put_finalization_metadata_route(
            cluster.operation_epoch(),
            cluster.object_metadata_pg(bucket, key),
            bucket,
            key,
            session_id,
        )
        .unwrap();
    let snapshot = route.load_snapshot().unwrap();
    let commit = crate::StreamPutCommitInput {
        versioning: crate::BucketVersioningState::Disabled,
        version_id: crate::VersionId::Null,
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        etag_crc64: checksum::crc64::checksum(&[]),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        object_lock: crate::ObjectLockState::default(),
        encryption,
    };
    route
        .build_commit_command(
            BuildStreamPutCommitCommandReq {
                total_size: 0,
                expected_snapshot: &snapshot,
                commit: &commit,
                bucket_write_reservation: snapshot
                    .session
                    .bucket_write_reservation
                    .as_ref()
                    .unwrap(),
            },
            crate::types::AdmittedRouteEffectFence::unbounded(cluster.operation_epoch()),
        )
        .unwrap()
}

#[test]
fn abandoned_terminal_cleanup_preserves_stream_owned_generation_until_abort() {
    use crate::metadata_command::{
        decode_metadata_command_envelope, PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
        PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
        PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
    };

    for recovery in [false, true] {
        for operation in [
            PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
            PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
        ] {
            let stream = operation == PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND;
            let standalone = operation == PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND;
            let dir = test_util::tempdir();
            let nodes = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
            let map = Arc::new(
                LocalClusterMap::open(dir.path(), &nodes, &[0, 1, 2], EcShape { k: 2, m: 1 })
                    .unwrap(),
            );
            let cluster = StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
            let bucket = crate::tests::bucket_name("generation-owner");
            let key = crate::tests::object_key("object");
            let session_id = crate::tests::stream_session_id("abandoned-owner");
            create_test_bucket(&cluster, &bucket);
            let template = if stream {
                empty_stream_terminal_command(&cluster, &bucket, &key, &session_id)
            } else {
                staged_direct_put_recovery_command(&cluster, &bucket, &key, &session_id)
            };
            let mut payload = template.payload().clone();
            let MetadataCommandPayload::CommitDirectPutObject(commit) = &mut payload else {
                unreachable!();
            };
            commit.bucket_write_reservation.operation_kind = operation.into();
            let encoded = MetadataCommandEnvelope::new(template.id(), payload);
            let command = decode_metadata_command_envelope(
                &encoded.command_bytes(),
                &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
            )
            .unwrap();
            assert_eq!(command, encoded);
            let pg_id = command.id().pg_id();
            let generation = cluster
                .test_object_generation_reservation_for(&bucket, &key, &session_id)
                .unwrap();
            let session = stream.then(|| {
                cluster
                    .load_stream_upload_session(&bucket, &key, &session_id)
                    .unwrap()
            });
            let derivative = MetadataCommandPayload::ReleaseObjectGeneration(
                crate::metadata_command::ReleaseObjectGenerationCommand::new(
                    bucket.clone(),
                    key.clone(),
                    session_id.clone(),
                ),
            );
            assert_eq!(
                derivative.is_authorized_recovery_derivative_of(command.payload()),
                standalone
            );
            if !stream && !standalone {
                assert!(command.payload().abandoned_recovery_follow_up().is_err());
                assert_invalid_abandonment_preserves_staging(&cluster, &map, &command, recovery);
                continue;
            }
            insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
            assert_eq!(
                command.payload().abandoned_recovery_follow_up().unwrap(),
                standalone.then(|| derivative.clone())
            );
            // Authoritative unstarted abandonment; no apply/ambiguous dispatch
            // is invented to justify discarding a started publication.
            cluster
                .test_record_abandoned_metadata_command_to_acting_set_until(
                    &command,
                    Instant::now() + Duration::from_secs(5),
                )
                .unwrap();
            if recovery {
                let outcome = cluster
                    .drain_pending_metadata_command_with_authorized_recovery_source(
                        pg_id, &command, &command, &cluster,
                    )
                    .unwrap();
                assert!(matches!(
                    outcome,
                    PendingMetadataCommandOutcome::Abandoned
                        | PendingMetadataCommandOutcome::Applied
                ));
            } else {
                cluster
                    .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
                    .unwrap();
            }
            assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
            for node in map
                .metadata_pg_acting_nodes(cluster.operation_epoch(), pg_id)
                .unwrap()
            {
                let pg = node.storage_node().get_pg(pg_id.get()).unwrap();
                assert!(pg
                    .metadata_command_abandoned(node.node_id().as_u32(), &command)
                    .unwrap());
                let reserved = pg.get_object_generation_reservation(&bucket, &key, &session_id);
                if let Some(session) = &session {
                    assert_eq!(reserved.unwrap(), generation);
                    assert_eq!(&pg.get_stream_upload(&session_id).unwrap(), session);
                    assert_eq!(
                        pg.max_metadata_command_log_index(cluster.operation_epoch())
                            .unwrap(),
                        command.id().log_index().get(),
                        "stream abandonment published a standalone release"
                    );
                } else {
                    assert!(matches!(
                        reserved,
                        Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
                    ));
                    assert!(
                        pg.max_metadata_command_log_index(cluster.operation_epoch())
                            .unwrap()
                            > command.id().log_index().get(),
                        "Direct PUT must still publish generation cleanup"
                    );
                }
            }
            if stream {
                // Retained session ownership is cleanup responsibility, not a
                // promise to resume a failed client attempt. Its ordinary abort
                // must still atomically remove session and generation.
                cluster
                    .abort_stream_upload_session(&bucket, &key, &session_id)
                    .unwrap();
                for node in map
                    .metadata_pg_acting_nodes(cluster.operation_epoch(), pg_id)
                    .unwrap()
                {
                    let pg = node.storage_node().get_pg(pg_id.get()).unwrap();
                    assert!(matches!(
                        pg.get_stream_upload(&session_id),
                        Err(crate::MetadataError::StreamSessionNotFound { .. })
                    ));
                    assert!(matches!(
                        pg.get_object_generation_reservation(&bucket, &key, &session_id),
                        Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
                    ));
                }
            }
        }
    }
}

fn assert_invalid_abandonment_preserves_staging(
    cluster: &StorageCluster,
    map: &LocalClusterMap,
    command: &MetadataCommandEnvelope,
    recovery: bool,
) {
    let MetadataCommandPayload::CommitDirectPutObject(commit) = command.payload() else {
        unreachable!();
    };
    assert!(!commit.segments.is_empty());
    // The staging helper stops before acknowledgement registration. Complete
    // that normal pre-install step using the actual encrypted shard bytes.
    let mut written = Vec::new();
    for segment in &commit.segments {
        let subject = map
            .placed_segment_shard_subject(
                segment.placement_cluster_epoch,
                DataPgId::new_for_test(PgId::new(segment.data_pg_id)),
                EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
                &segment.segment_okh,
                segment.segment_vid,
            )
            .unwrap();
        for index in 0..subject.locations().len() {
            let (location, key) = subject.location_and_key(index).unwrap();
            let node = map.node(location.node_id()).unwrap().storage_node();
            let bytes = node.read_shard_file(segment.data_pg_id, &key).unwrap();
            assert!(!bytes.is_empty());
            let ack = WriteAck {
                stored_size: bytes.len() as u64,
                crc64: checksum::crc64::checksum(&bytes),
            };
            node.get_pg(segment.data_pg_id)
                .unwrap()
                .register_written_shard(&key, ack)
                .unwrap();
            written.push((location.node_id(), segment.data_pg_id, key, ack));
        }
    }
    insert_pending_metadata_command_for_test(
        map,
        command.id().pg_id(),
        &commit.object.bucket,
        command,
    );
    // Capture every replica/PG, including bucket reservations and data-PG
    // acknowledgements. Require every physical shard, not just k reconstructable
    // shards, to remain byte-for-byte intact.
    let capture = || {
        let mut states = Vec::new();
        for node_id in [NodeId::new(0), NodeId::new(1), NodeId::new(2)] {
            for raw_pg_id in 0..3 {
                let pg = map
                    .node(node_id)
                    .unwrap()
                    .storage_node()
                    .get_pg(raw_pg_id)
                    .unwrap();
                let generation = match pg.get_object_generation_reservation(
                    &commit.object.bucket,
                    &commit.object.key,
                    &commit.generation_reservation_id,
                ) {
                    Ok(generation) => Some(generation),
                    Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. }) => None,
                    Err(error) => panic!("unexpected generation lookup failure: {error:?}"),
                };
                if raw_pg_id == command.id().pg_id().get() {
                    assert_eq!(generation, Some(commit.object.generation_id));
                    assert!(!pg
                        .metadata_command_abandoned(node_id.as_u32(), command)
                        .unwrap());
                }
                states.push((
                    pg.metadata_command_replica_state().unwrap(),
                    pg.max_metadata_command_log_index(cluster.operation_epoch())
                        .unwrap(),
                    pg.durable_bucket_write_reservations(&commit.object.bucket)
                        .unwrap(),
                    generation,
                ));
            }
        }
        let mut shards = Vec::new();
        for (node_id, pg_id, key, ack) in &written {
            let node = map.node(*node_id).unwrap().storage_node();
            node.get_pg(*pg_id)
                .unwrap()
                .validate_written_shard_ack(key, *ack)
                .unwrap();
            shards.push(node.read_shard_file(*pg_id, key).unwrap());
        }
        (
            states,
            shards,
            pending_metadata_command_for_test(map, command.id().pg_id(), &commit.object.bucket),
        )
    };
    let before = capture();
    assert!(before.2.is_some());
    let failure = cluster
        .record_abandoned_metadata_command_to_acting_set_until(
            command,
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap_err();
    assert_eq!(failure.applied_nodes, 0);
    assert!(!failure.may_have_applied);
    assert!(matches!(
        failure.source,
        BucketSnapshotLoadError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "put-object-terminal-generation-owner"
        })
    ));
    assert_eq!(capture(), before);
    let error = if recovery {
        cluster
            .drain_pending_metadata_command_with_authorized_recovery_source(
                command.id().pg_id(),
                command,
                command,
                cluster,
            )
            .unwrap_err()
    } else {
        cluster
            .drain_pending_object_metadata_commands_for_bucket(
                command.id().pg_id(),
                &commit.object.bucket,
            )
            .unwrap_err()
    };
    assert!(
        matches!(
            error,
            ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch { .. })
        ),
        "{error:?}"
    );
    assert_eq!(capture(), before);
    assert!(cluster
        .after_object_metadata_command_abandoned(command, cluster)
        .is_err());
    assert_eq!(capture(), before);
    assert!(cluster
        .after_object_metadata_command_abandoned_payload_cleanup(command)
        .is_err());
    assert_eq!(capture(), before);
}
