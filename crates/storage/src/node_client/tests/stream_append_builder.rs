// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::storage_rpc::{
    encode_object_metadata_command_build_response, StorageRpcObjectMetadataCommandBuildResponse,
};
use crate::ObjectEncryption;

fn segment(session_id: &SessionId, encryption: &ObjectEncryption) -> StreamUploadSegmentRecord {
    // Metadata-only fixture: coherent geometry/witness, never exposed as payload.
    let (crc, witness) = crate::types::SegmentShardIntegrityRecord::for_zero_filled_segment(
        EcShape { k: 1, m: 0 },
        encryption.segment_stored_size(1).unwrap(),
    );
    StreamUploadSegmentRecord {
        session_id: session_id.clone(),
        segment_index: 0,
        size: 1,
        segment_crc64: crc,
        payload_crc64: 0,
        segment_okh: crate::stream_segment_key_hash(session_id, 0),
        segment_vid: GenerationId::MIN,
        data_pg_id: 0,
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 1,
        ec_m: 0,
        shard_integrity: Some(witness),
    }
}

#[test]
fn stream_append_primary_builder_is_read_only_and_checks_durable_subject() {
    for remote in [false, true] {
        for part in [false, true] {
            let tmp = test_util::tempdir();
            let mut config = test_config(&tmp);
            config.default_ec_shape = EcShape { k: 1, m: 0 };
            config.pg_ids = (0..8).collect();
            config.pg_routes = config
                .pg_ids
                .iter()
                .map(|&pg_id| StorageNodePgRoute {
                    pg_id,
                    ..config.pg_routes[0].clone()
                })
                .collect();
            let topology = Arc::new(PgTopology::new(&config.pg_ids).unwrap());
            let bucket = crate::tests::bucket_name("append-builder");
            let payload_generation = GenerationId::new(2).unwrap();
            let key = (0..10_000)
                .map(|n| crate::tests::object_key(format!("key-{n}")))
                .find(|key| {
                    topology.object_pg_for(&bucket, key) == 0
                        && topology.object_generation_multipart_part_segment_data_pg(
                            &bucket,
                            key,
                            GenerationId::MIN,
                            1,
                            0,
                        ) != topology.object_generation_multipart_part_segment_data_pg(
                            &bucket,
                            key,
                            payload_generation,
                            1,
                            0,
                        )
                })
                .unwrap();
            private_socket_dir(config.socket_path.parent().unwrap());
            let server = StorageNodeServer::bind(config.clone()).unwrap();
            let node = server.test_storage_node();
            let session_id = crate::tests::stream_session_id("session");
            let encryption = metadata_only_test_encryption();
            let upload_request = CreateMultipartUploadReq {
                upload_id: crate::tests::multipart_upload_id("upload"),
                bucket: bucket.clone(),
                key: key.clone(),
                tags: None,
                metadata_blob: SerializedMetadataBlob::default(),
                system_metadata_blob: SerializedSystemMetadataBlob::default(),
                initiator: OwnerIdentity::from_principal("owner"),
                owner: OwnerIdentity::from_principal("owner"),
                acl_grants: AclGrants::default(),
                public_read: false,
                object_lock: ObjectLockState::default(),
                checksum: None,
                encryption: encryption.clone(),
            };
            let upload = {
                let pg = node.get_pg(0).unwrap();
                PgMetadataStore::create_multipart_upload(&*pg, &upload_request).unwrap();
                pg.refresh_metadata_command_state_digest().unwrap();
                pg.get_multipart_upload(&upload_request.upload_id).unwrap()
            };
            let target = if part {
                StreamUploadTarget::UploadPart {
                    upload_id: upload.upload_id.clone(),
                    part_number: 1,
                }
            } else {
                StreamUploadTarget::PutObject
            };
            let create = CreateStreamUploadReq {
                session_id: session_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: target.clone(),
                encryption: encryption.clone(),
            };
            let mut proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
            proof.operation_kind = if part {
                crate::metadata_command::UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
            } else {
                PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
            }
            .into();
            let _serving = spawn_test_storage_node_server(server);
            let local = LocalStorageNodeClient::new(config.node_id, Arc::clone(&node));
            let unix = UnixStorageNodeClient::new(
                config.node_id,
                config.cluster_epoch,
                config.socket_path.clone(),
            )
            .with_pg_topology(Arc::clone(&topology));
            let client: &dyn ObjectMutationMetadataNodeClient = if remote { &unix } else { &local };
            let pg_id = ObjectMetadataPgId::new_for_test(PgId::new(0));
            let fence = AdmittedRouteEffectFence::unbounded(config.cluster_epoch);
            let creation_route = client
                .open_stream_upload_creation_metadata_route(
                    config.cluster_epoch,
                    pg_id,
                    &bucket,
                    &key,
                )
                .unwrap();
            let command = creation_route
                .build_create_stream_upload_command(BuildCreateStreamUploadCommandReq {
                    request: &create,
                    cleanup_after: Some(10_000),
                    bucket_write_reservation: &proof,
                    effect_fence: fence,
                    placement: PayloadPlacementInput::from_server_config(&config),
                    precondition: if part {
                        CreateStreamUploadPrecondition::UploadPart {
                            expected_upload: &upload,
                        }
                    } else {
                        CreateStreamUploadPrecondition::PutObjectNoCurrentCheck {}
                    },
                })
                .unwrap();
            node.get_pg(0)
                .unwrap()
                .apply_metadata_command_and_record(config.node_id.as_u32(), &command)
                .unwrap();
            let route = client
                .open_stream_upload_session_metadata_route(
                    config.cluster_epoch,
                    pg_id,
                    &bucket,
                    &key,
                    &session_id,
                )
                .unwrap();
            let observe = || {
                let pg = node.get_pg(0).unwrap();
                (
                    pg.metadata_command_checkpoint(config.node_id.as_u32(), config.cluster_epoch)
                        .unwrap(),
                    pg.pending_metadata_command_envelope(
                        config.node_id.as_u32(),
                        config.cluster_epoch,
                    )
                    .unwrap(),
                    pg.max_metadata_command_log_index(config.cluster_epoch)
                        .unwrap(),
                    pg.get_stream_upload(&session_id).unwrap(),
                    pg.list_stream_segments(&session_id).unwrap(),
                )
            };
            let before = observe();
            let reserved = PgMetadataStore::get_object_generation_reservation(
                &*node.get_pg(0).unwrap(),
                &bucket,
                &key,
                &session_id,
            )
            .unwrap();
            assert_eq!(reserved, payload_generation);
            assert_eq!(upload.object_generation_id, GenerationId::MIN);
            let mut segment = segment(&session_id, &encryption);
            segment.data_pg_id = if part {
                topology
                    .object_generation_multipart_part_segment_data_pg(
                        &bucket,
                        &key,
                        upload.object_generation_id,
                        1,
                        0,
                    )
                    .get()
            } else {
                topology
                    .object_generation_segment_data_pg(&bucket, &key, reserved, 0)
                    .get()
            };
            let error = route
                .build_append_command(
                    &target,
                    &segment,
                    fence,
                    PayloadPlacementInput::from_server_config(&config),
                )
                .unwrap_err();
            assert_unprepared_append_error(&error, remote);
            assert_eq!(
                observe(),
                before,
                "an unprepared append must not allocate a command"
            );
            let expected = if part {
                crate::ExpectedStreamAppendState::UploadPart {
                    session: route.load_session().unwrap(),
                    upload: Box::new(upload.clone()),
                    part_generation_id: reserved,
                }
            } else {
                crate::ExpectedStreamAppendState::PutObject {
                    session: route.load_session().unwrap(),
                    generation_id: reserved,
                }
            };
            let (_, mut prepared) = route
                .prepare_segment_append_with_expected_append_state(
                    &PrepareStreamUploadSegmentAppendReq {
                        session_id: session_id.clone(),
                        segment_index: segment.segment_index,
                        size: segment.size,
                        segment_crc64: segment.segment_crc64,
                        payload_crc64: segment.payload_crc64,
                    },
                    Some(&expected),
                    fence,
                    PayloadPlacementInput::from_server_config(&config),
                )
                .unwrap();
            prepared.shard_integrity = segment.shard_integrity.clone();
            assert_eq!(prepared, segment);
            let before = observe();
            for case in 0..2 {
                let mut crossed = segment.clone();
                if case == 0 {
                    crossed.segment_vid = GenerationId::new(2).unwrap();
                } else {
                    crossed.segment_index = 1;
                    crossed.segment_okh = crate::stream_segment_key_hash(&session_id, 1);
                }
                let error = route
                    .build_append_command(
                        &target,
                        &crossed,
                        fence,
                        PayloadPlacementInput::from_server_config(&config),
                    )
                    .unwrap_err();
                assert_unprepared_append_error(&error, remote);
                assert_eq!(observe(), before, "crossed preparation changed state");
            }
            let built = route
                .build_append_command(
                    &target,
                    &segment,
                    fence,
                    PayloadPlacementInput::from_server_config(&config),
                )
                .unwrap();
            assert_eq!(built.id().log_index().get(), before.2 + 1);
            assert!(
                matches!(built.payload(), MetadataCommandPayload::AppendStreamSegment(append)
                if append.bucket == bucket && append.key == key && append.target == target && append.segment == segment)
            );
            assert_eq!(observe(), before, "healthy construction must be read-only");
            // These mutations retain coherent witness geometry. In particular,
            // the part case crosses only MPU-vs-payload generation placement.
            for case in 0..3 {
                let mut crossed = segment.clone();
                match case {
                    0 => {
                        crossed.data_pg_id = if part {
                            topology
                                .object_generation_multipart_part_segment_data_pg(
                                    &bucket, &key, reserved, 1, 0,
                                )
                                .get()
                        } else {
                            (segment.data_pg_id + 1) % 8
                        }
                    }
                    1 => crossed.placement_cluster_epoch = ClusterEpoch::new(2).unwrap(),
                    2 => {
                        crossed.ec_m = 1;
                        let (crc, witness) =
                            crate::types::SegmentShardIntegrityRecord::for_zero_filled_segment(
                                EcShape { k: 1, m: 1 },
                                encryption.segment_stored_size(crossed.size).unwrap(),
                            );
                        crossed.segment_crc64 = crc;
                        crossed.shard_integrity = Some(witness);
                    }
                    _ => unreachable!(),
                }
                let error = route
                    .build_append_command(
                        &target,
                        &crossed,
                        fence,
                        PayloadPlacementInput::from_server_config(&config),
                    )
                    .unwrap_err();
                match &error {
                    ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
                        operation,
                    }) => {
                        assert_eq!(
                            *operation,
                            if remote && case == 2 {
                                "stream preparation captured placement"
                            } else {
                                "stream append placement subject"
                            }
                        );
                    }
                    ObjectPgActionError::Store(StoreError::StorageRpc {
                        failure: StorageRpcErrorCode::PayloadDecode,
                        detail,
                        ..
                    }) if remote && case == 0 => {
                        assert!(
                            detail.as_str().contains("stream append placement subject"),
                            "{detail}"
                        );
                    }
                    _ => panic!("case={case} remote={remote}: {error:?}"),
                }
                assert_eq!(observe(), before);
                if remote {
                    // Bypass sender preflight so epoch/EC failures must also be
                    // rejected independently by the installed primary builder.
                    let error = unix
                        .rpc_request(
                            StorageRpcMessageKind::ObjectStreamAppendCommandBuild,
                            crate::storage_rpc::encode_stream_append_command_build_request(
                                &crate::storage_rpc::StorageRpcStreamAppendCommandBuildRequest {
                                    object: unix.object_request(pg_id.pg_id(), &bucket, &key),
                                    target: target.clone(),
                                    segment: crossed,
                                    effect_deadline: None,
                                },
                            ),
                        )
                        .unwrap_err();
                    let StoreError::StorageRpc {
                        failure: StorageRpcErrorCode::PayloadDecode,
                        detail,
                        ..
                    } = error
                    else {
                        panic!("{error:?}");
                    };
                    assert!(
                        detail.as_str().contains("stream append placement subject"),
                        "{detail}"
                    );
                    assert_eq!(observe(), before);
                }
            }
            for case in 0..5 {
                let mut crossed = segment.clone();
                match case {
                    0 => crossed.session_id = crate::tests::stream_session_id("foreign"),
                    1 => crossed.segment_okh[0] ^= 1,
                    2 => crossed.size = u64::MAX,
                    3 => crossed.segment_crc64 ^= 1,
                    4 => crossed.shard_integrity = None,
                    _ => unreachable!(),
                }
                assert!(
                    route
                        .build_append_command(
                            &target,
                            &crossed,
                            fence,
                            PayloadPlacementInput::from_server_config(&config)
                        )
                        .is_err(),
                    "case={case} remote={remote}"
                );
                assert_eq!(observe(), before);
            }
            let wrong_target = if part {
                StreamUploadTarget::PutObject
            } else {
                StreamUploadTarget::UploadPart {
                    upload_id: upload.upload_id.clone(),
                    part_number: 1,
                }
            };
            assert!(route
                .build_append_command(
                    &wrong_target,
                    &segment,
                    fence,
                    PayloadPlacementInput::from_server_config(&config)
                )
                .is_err());
            let wrong_key = client
                .open_stream_upload_session_metadata_route(
                    config.cluster_epoch,
                    pg_id,
                    &bucket,
                    &(0..10_000)
                        .map(|n| crate::tests::object_key(format!("foreign-{n}")))
                        .find(|key| topology.object_pg_for(&bucket, key) == 0)
                        .unwrap(),
                    &session_id,
                )
                .unwrap();
            assert!(wrong_key
                .build_append_command(
                    &target,
                    &segment,
                    fence,
                    PayloadPlacementInput::from_server_config(&config)
                )
                .is_err());
            assert!(route
                .build_append_command(
                    &target,
                    &segment,
                    AdmittedRouteEffectFence::bounded(config.cluster_epoch, 2_000, 1),
                    PayloadPlacementInput::from_server_config(&config)
                )
                .is_err());
            assert_eq!(observe(), before);
            drop(
                node.get_pg(0)
                    .unwrap()
                    .begin_allocation_lifetime_replacement()
                    .unwrap(),
            );
            let error = route
                .build_append_command(
                    &target,
                    &segment,
                    fence,
                    PayloadPlacementInput::from_server_config(&config),
                )
                .unwrap_err();
            assert!(
                if remote {
                    matches!(
                        error,
                        ObjectPgActionError::Store(StoreError::StorageRpc {
                            failure: StorageRpcErrorCode::InactivePgRoute,
                            ..
                        })
                    )
                } else {
                    matches!(
                        error,
                        ObjectPgActionError::Store(
                            StoreError::PgAllocationLifetimeUnavailable { .. }
                        )
                    )
                },
                "{error:?}"
            );
            assert_eq!(observe(), before);
            node.get_pg(0)
                .unwrap()
                .begin_allocation_lifetime_replacement()
                .unwrap()
                .complete()
                .unwrap();
            let error = route
                .build_append_command(
                    &target,
                    &segment,
                    fence,
                    PayloadPlacementInput::from_server_config(&config),
                )
                .unwrap_err();
            assert!(
                if remote {
                    matches!(
                        error,
                        ObjectPgActionError::Store(StoreError::StorageRpc {
                            failure: StorageRpcErrorCode::InactivePgRoute,
                            ..
                        })
                    )
                } else {
                    matches!(
                        error,
                        ObjectPgActionError::Store(
                            StoreError::PgAllocationLifetimeUnavailable { .. }
                        )
                    )
                },
                "replacement adopted an old stream issuer: {error:?}"
            );
            assert_eq!(observe(), before);
            // Already-built commands still converge after issuer replacement.
            node.get_pg(0)
                .unwrap()
                .apply_metadata_command_and_record(config.node_id.as_u32(), &built)
                .unwrap();
            node.get_pg(0)
                .unwrap()
                .apply_metadata_command_and_record(config.node_id.as_u32(), &built)
                .unwrap();
            assert_eq!(route.load_segments().unwrap(), vec![segment]);
        }
    }
}

fn assert_unprepared_append_error(error: &ObjectPgActionError, remote: bool) {
    match error {
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch { operation })
            if !remote =>
        {
            assert_eq!(*operation, "stream append primary preparation");
        }
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            detail,
            ..
        }) if remote => {
            assert!(
                detail
                    .as_str()
                    .contains("stream append primary preparation"),
                "{}",
                detail.as_str()
            );
        }
        _ => panic!("unexpected unprepared-append error: {error:?}"),
    }
}

#[test]
fn unix_stream_append_builder_rejects_crossed_peer_responses() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.default_ec_shape = EcShape { k: 1, m: 0 };
    private_socket_dir(config.socket_path.parent().unwrap());
    let listener = UnixListener::bind(&config.socket_path).unwrap();
    let bucket = crate::tests::bucket_name("append-peer");
    let key = crate::tests::object_key("key");
    let session_id = crate::tests::stream_session_id("session");
    let segment = segment(&session_id, &metadata_only_test_encryption());
    let target = StreamUploadTarget::UploadPart {
        upload_id: crate::tests::multipart_upload_id("upload"),
        part_number: 1,
    };
    let exact_id = MetadataCommandId::new(
        config.cluster_epoch,
        PgId::new(0),
        MetadataCommandLogIndex::new(1).unwrap(),
    );
    let exact = crate::metadata_command::AppendStreamSegmentCommand {
        bucket: bucket.clone(),
        key: key.clone(),
        target: target.clone(),
        segment: segment.clone(),
    };
    let mut commands = Vec::new();
    for case in 0..11 {
        let mut id = exact_id;
        let mut append = exact.clone();
        match case {
            0 => {
                id = MetadataCommandId::new(
                    ClusterEpoch::new(2).unwrap(),
                    PgId::new(0),
                    id.log_index(),
                )
            }
            1 => id = MetadataCommandId::new(config.cluster_epoch, PgId::new(1), id.log_index()),
            2 => append.bucket = crate::tests::bucket_name("foreign"),
            3 => append.key = crate::tests::object_key("foreign"),
            4 => append.target = StreamUploadTarget::PutObject,
            5 => append.segment.session_id = crate::tests::stream_session_id("foreign"),
            6 => append.segment.segment_vid = GenerationId::new(2).unwrap(),
            7 => append.segment.data_pg_id = 1,
            8 => append.segment.payload_crc64 ^= 1,
            9 => append.segment.shard_integrity = None,
            10 => {} // Positive exact response, after all crossed responses.
            _ => unreachable!(),
        }
        commands.push(MetadataCommandEnvelope::new(
            id,
            MetadataCommandPayload::AppendStreamSegment(Box::new(append)),
        ));
    }
    let serving = thread::spawn(move || {
        for command in commands {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_storage_rpc_frame_from(&mut stream).unwrap();
            assert_eq!(
                request.kind,
                StorageRpcMessageKind::ObjectStreamAppendCommandBuild
            );
            let payload = encode_object_metadata_command_build_response(
                &StorageRpcObjectMetadataCommandBuildResponse {
                    outcome: StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(
                        command,
                    )),
                },
            );
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
        .open_stream_upload_session_metadata_route(
            config.cluster_epoch,
            ObjectMetadataPgId::new_for_test(PgId::new(0)),
            &bucket,
            &key,
            &session_id,
        )
        .unwrap();
    for _ in 0..10 {
        let error = route
            .build_append_command(
                &target,
                &segment,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                PayloadPlacementInput::from_server_config(&config),
            )
            .unwrap_err();
        assert!(
            matches!(
                error,
                ObjectPgActionError::Store(StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::PayloadDecode,
                    ..
                })
            ),
            "{error:?}"
        );
    }
    assert_eq!(
        route
            .build_append_command(
                &target,
                &segment,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                PayloadPlacementInput::from_server_config(&config),
            )
            .unwrap()
            .id(),
        exact_id
    );
    serving.join().unwrap();
}
