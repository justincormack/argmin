// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND;
use crate::storage_rpc::{
    encode_object_generation_reserve_command_build_request,
    encode_object_metadata_command_build_response,
    StorageRpcObjectGenerationReserveCommandBuildRequest,
    StorageRpcObjectMetadataCommandBuildResponse,
};

fn direct_proof(bucket: &BucketName, key: &ObjectKey) -> BucketWriteReservationProof {
    let mut proof = test_bucket_write_reservation_proof(bucket.clone(), key);
    proof.operation_kind = PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND.into();
    proof
}

#[test]
fn generation_reservation_builder_rejects_crossed_proofs_before_storage_or_transport() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.default_ec_shape = EcShape { k: 1, m: 0 };
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let node = server.test_storage_node();
    let bucket = crate::tests::bucket_name("generation-proof");
    let key = crate::tests::object_key("key");
    let pg_id = ObjectMetadataPgId::new_for_test(PgId::new(0));
    let reservation_id = crate::tests::stream_session_id("reserve");
    let local = LocalStorageNodeClient::new(config.node_id, node.clone());
    // No listener at this endpoint: PayloadDecode must precede transport.
    let unavailable = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        tmp.path().join("absent.sock"),
    );
    let unix = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let _server = spawn_test_storage_node_server(server);
    let observe = || {
        let pg = node.get_pg(0).unwrap();
        (
            pg.metadata_command_checkpoint(config.node_id.as_u32(), config.cluster_epoch)
                .unwrap(),
            pg.pending_metadata_command_envelope(config.node_id.as_u32(), config.cluster_epoch)
                .unwrap(),
            pg.max_metadata_command_log_index(config.cluster_epoch)
                .unwrap(),
            PgMetadataStore::next_generation_id(&*pg, &bucket, &key).unwrap(),
        )
    };
    let before = observe();
    for crossed in 0..5 {
        let mut proof = direct_proof(&bucket, &key);
        match crossed {
            0 => proof.bucket = crate::tests::bucket_name("other-bucket"),
            1 => proof.target_context = Some("other-key".into()),
            2 => proof.operation_kind = PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND.into(),
            3 => proof.cluster_epoch = ClusterEpoch::new(2).unwrap(),
            4 => proof.target_context = None,
            _ => unreachable!(),
        }
        for client in [
            &local as &dyn ObjectGenerationMetadataNodeClient,
            &unavailable,
        ] {
            let route = client
                .open_object_generation_metadata_route(config.cluster_epoch, pg_id, &bucket, &key)
                .unwrap();
            let error = route
                .build_reserve_put_object_generation_command(
                    &reservation_id,
                    GenerationId::MIN,
                    &proof,
                    PayloadPlacementInput::from_server_config(&config),
                    AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                )
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
                        operation: "build direct PUT generation reservation",
                    }) | ObjectPgActionError::Store(StoreError::StorageRpc {
                        failure: StorageRpcErrorCode::PayloadDecode,
                        operation: "build direct PUT generation reservation",
                        ..
                    })
                ),
                "crossed={crossed}: {error:?}"
            );
            assert_eq!(observe(), before);
        }
        // Bypass the sender preflight to exercise the installed server's own
        // route capability. All subjects map to this PG; placement cannot mask it.
        let payload = encode_object_generation_reserve_command_build_request(
            &StorageRpcObjectGenerationReserveCommandBuildRequest {
                object: unix.object_request(pg_id.pg_id(), &bucket, &key),
                reservation_id: reservation_id.clone(),
                expected_generation: GenerationId::MIN,
                bucket_write_reservation: proof,
                effect_deadline: None,
            },
        );
        let error = unix
            .rpc_request(
                StorageRpcMessageKind::ObjectGenerationReserveCommandBuild,
                payload,
            )
            .unwrap_err();
        assert!(
            matches!(&error, StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode, detail, ..
        } if detail.as_str().contains("bucket write reservation proof does not match")),
            "{error:?}"
        );
        assert_eq!(observe(), before);
    }
    let route = unix
        .open_object_generation_metadata_route(config.cluster_epoch, pg_id, &bucket, &key)
        .unwrap();
    route
        .build_reserve_put_object_generation_command(
            &reservation_id,
            GenerationId::MIN,
            &direct_proof(&bucket, &key),
            PayloadPlacementInput::from_server_config(&config),
            AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
        )
        .unwrap();
    assert_eq!(observe(), before, "positive read-only builder canary");
}

#[test]
fn embedded_generation_reservation_builder_checks_primary_state_and_placement() {
    builder_matrix(false);
}

#[test]
fn unix_generation_reservation_builder_checks_primary_state_and_placement() {
    builder_matrix(true);
}

fn builder_matrix(remote: bool) {
    for unavailable in [false, true] {
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
        let bucket = crate::tests::bucket_name("generation-builder");
        let key = (0..1000)
            .map(|i| crate::tests::object_key(format!("key-{i}")))
            .find(|key| {
                topology.object_pg_for(&bucket, key)
                    != topology
                        .object_generation_segment_data_pg(&bucket, key, GenerationId::MIN, 0)
                        .get()
            })
            .unwrap();
        let pg_id =
            ObjectMetadataPgId::new_for_test(PgId::new(topology.object_pg_for(&bucket, &key)));
        let data_pg = topology
            .object_generation_segment_data_pg(&bucket, &key, GenerationId::MIN, 0)
            .get();
        assert_ne!(pg_id.get(), data_pg);
        // Unrelated unavailable routes must not prevent construction.
        let unrelated = (0..8)
            .find(|&pg| pg != pg_id.get() && pg != data_pg)
            .unwrap();
        config.pg_routes[unrelated as usize].state = crate::PgState::Peering;
        if unavailable {
            config.pg_routes[data_pg as usize].state = crate::PgState::Peering;
        }
        let mut frontend = config.clone();
        frontend.pg_routes[data_pg as usize].state = crate::PgState::Active;
        private_socket_dir(config.socket_path.parent().unwrap());
        let server = StorageNodeServer::bind(config.clone()).unwrap();
        let node = server.test_storage_node();
        let _server = spawn_test_storage_node_server(server);
        let unix = UnixStorageNodeClient::new(
            config.node_id,
            config.cluster_epoch,
            config.socket_path.clone(),
        )
        .with_pg_topology(topology);
        let local = LocalStorageNodeClient::new(config.node_id, node.clone());
        let client: &dyn ObjectGenerationMetadataNodeClient = if remote { &unix } else { &local };
        let route = client
            .open_object_generation_metadata_route(config.cluster_epoch, pg_id, &bucket, &key)
            .unwrap();
        let reservation_id = crate::tests::stream_session_id("reserve");
        let proof = direct_proof(&bucket, &key);
        let observe = || {
            let pg = node.get_pg(pg_id.get()).unwrap();
            (
                pg.metadata_command_replica_state().unwrap(),
                pg.max_metadata_command_log_index(config.cluster_epoch)
                    .unwrap(),
                pg.pending_metadata_command_envelope(config.node_id.as_u32(), config.cluster_epoch)
                    .unwrap(),
                PgMetadataStore::next_generation_id(&*pg, &bucket, &key).unwrap(),
            )
        };
        let before = observe();
        let placement =
            PayloadPlacementInput::from_server_config(if remote { &frontend } else { &config });
        let fence = AdmittedRouteEffectFence::bounded(
            config.cluster_epoch,
            crate::clock::current_time_millis() + 60_000,
            crate::clock::monotonic_time_millis() + 59_000,
        );
        let result = route.build_reserve_put_object_generation_command(
            &reservation_id,
            GenerationId::MIN,
            &proof,
            placement,
            fence,
        );
        assert_eq!(
            observe(),
            before,
            "building must not install/apply a command"
        );
        assert!(matches!(
            route.object_generation_reservation(&reservation_id),
            Err(ObjectPgActionError::Metadata(
                MetadataError::ObjectGenerationReservationNotFound { .. }
            ))
        ));
        if unavailable {
            let error = result.unwrap_err();
            assert!(
                if remote {
                    matches!(
                        error,
                        ObjectPgActionError::Store(StoreError::StorageRpc {
                            failure: StorageRpcErrorCode::Internal,
                            ..
                        })
                    )
                } else {
                    matches!(error, ObjectPgActionError::Store(StoreError::PgNotActive { pg_id, .. }) if pg_id == data_pg)
                },
                "{error:?}"
            );
            continue;
        }
        let command = result.unwrap();
        assert_eq!(command.id().pg_id(), pg_id.pg_id());
        assert_eq!(command.id().cluster_epoch(), config.cluster_epoch);
        assert_eq!(command.id().log_index().get(), before.1 + 1);
        assert!(
            matches!(command.payload(), MetadataCommandPayload::ReserveObjectGeneration(reservation)
            if reservation.matches_request(&bucket, &key, &reservation_id) && reservation.generation_id == GenerationId::MIN)
        );
        assert!(matches!(
            route.build_reserve_put_object_generation_command(
                &reservation_id,
                GenerationId::new(2).unwrap(),
                &proof,
                placement,
                fence,
            ),
            Err(ObjectPgActionError::StaleObjectReadSubject)
        ));
        assert_eq!(observe(), before);
        if remote {
            // Exercise receiver rebinding, not the sender's preflight: an
            // expired portable request still reaches the installed handler.
            let payload = encode_object_generation_reserve_command_build_request(
                &StorageRpcObjectGenerationReserveCommandBuildRequest {
                    object: unix.object_request(pg_id.pg_id(), &bucket, &key),
                    reservation_id: reservation_id.clone(),
                    expected_generation: GenerationId::MIN,
                    bucket_write_reservation: proof.clone(),
                    effect_deadline: Some(StorageRpcAdmittedRouteEffectDeadline {
                        authority_valid_until_ms: 10_000,
                        portable_wall_valid_until_ms: 9_000,
                    }),
                },
            );
            let error = unix
                .rpc_request(
                    StorageRpcMessageKind::ObjectGenerationReserveCommandBuild,
                    payload,
                )
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    StoreError::StorageRpc {
                        failure: StorageRpcErrorCode::StaleShardLocation,
                        ..
                    }
                ),
                "{error:?}"
            );
            assert_eq!(observe(), before);
        }
        node.get_pg(pg_id.get())
            .unwrap()
            .apply_metadata_command_and_record(config.node_id.as_u32(), &command)
            .unwrap();
        let applied = observe();
        assert_eq!(
            route
                .object_generation_reservation(&reservation_id)
                .unwrap(),
            GenerationId::MIN
        );
        assert!(
            matches!(
                route.build_reserve_put_object_generation_command(
                    &reservation_id,
                    applied.3,
                    &proof,
                    placement,
                    fence,
                ),
                Err(ObjectPgActionError::StaleObjectReadSubject)
            ),
            "existing reservations must be reinspected, not adopted by fresh construction"
        );
        assert_eq!(observe(), applied);
    }
}

#[test]
fn unix_generation_reservation_builder_rejects_crossed_peer_commands() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.default_ec_shape = EcShape { k: 1, m: 0 };
    private_socket_dir(config.socket_path.parent().unwrap());
    let listener = UnixListener::bind(&config.socket_path).unwrap();
    let bucket = crate::tests::bucket_name("generation-peer");
    let key = crate::tests::object_key("key");
    let reservation_id = crate::tests::stream_session_id("exact");
    let exact_id = MetadataCommandId::new(
        config.cluster_epoch,
        PgId::new(0),
        MetadataCommandLogIndex::new(1).unwrap(),
    );
    let exact_payload = crate::metadata_command::ReserveObjectGenerationCommand::new(
        bucket.clone(),
        key.clone(),
        reservation_id.clone(),
        GenerationId::MIN,
        1,
    );
    let mut commands = Vec::new();
    for index in 0..6 {
        let mut id = exact_id;
        let mut payload = exact_payload.clone();
        match index {
            0 => {
                id =
                    MetadataCommandId::new(config.cluster_epoch, PgId::new(1), exact_id.log_index())
            }
            1 => {
                id = MetadataCommandId::new(
                    ClusterEpoch::new(2).unwrap(),
                    PgId::new(0),
                    exact_id.log_index(),
                )
            }
            2 => payload.bucket = crate::tests::bucket_name("crossed"),
            3 => payload.key = crate::tests::object_key("crossed"),
            4 => payload.reservation_id = crate::tests::stream_session_id("crossed"),
            5 => payload.generation_id = GenerationId::new(2).unwrap(),
            _ => unreachable!(),
        }
        commands.push(MetadataCommandEnvelope::new(
            id,
            MetadataCommandPayload::ReserveObjectGeneration(payload),
        ));
    }
    let expected_proof = direct_proof(&bucket, &key);
    let server = thread::spawn(move || {
        for command in commands {
            let (mut connection, _) = listener.accept().unwrap();
            let request = read_storage_rpc_frame_from(&mut connection).unwrap();
            assert_eq!(
                request.kind,
                StorageRpcMessageKind::ObjectGenerationReserveCommandBuild
            );
            assert_eq!(
                crate::storage_rpc::decode_object_generation_reserve_command_build_request(
                    &request.payload
                )
                .unwrap()
                .bucket_write_reservation,
                expected_proof,
                "transport must preserve the complete proof, not just its route subject",
            );
            let payload = encode_object_metadata_command_build_response(
                &StorageRpcObjectMetadataCommandBuildResponse {
                    outcome: StorageRpcObjectMetadataCommandBuildOutcome::Command(Box::new(
                        command,
                    )),
                },
            );
            write_storage_rpc_frame_to(
                &mut connection,
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
        .open_object_generation_metadata_route(
            config.cluster_epoch,
            ObjectMetadataPgId::new_for_test(PgId::new(0)),
            &bucket,
            &key,
        )
        .unwrap();
    for _ in 0..6 {
        let error = route
            .build_reserve_put_object_generation_command(
                &reservation_id,
                GenerationId::MIN,
                &direct_proof(&bucket, &key),
                PayloadPlacementInput::from_server_config(&config),
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            )
            .unwrap_err();
        assert!(
            matches!(
                error,
                ObjectPgActionError::Store(StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::PayloadDecode,
                    operation: "validate object generation reserve command build response",
                    ..
                })
            ),
            "{error:?}"
        );
    }
    server.join().unwrap();
}
