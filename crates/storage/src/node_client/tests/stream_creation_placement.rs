// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::metadata_command::UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND;

#[test]
fn embedded_stream_creation_validates_placement_before_command_allocation() {
    creation_placement_matrix(false);
}

#[test]
fn installed_unix_stream_creation_validates_placement_before_command_allocation() {
    creation_placement_matrix(true);
}

fn creation_placement_matrix(remote: bool) {
    for part in [false, true] {
        for unavailable in [true, false] {
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
            let bucket = crate::tests::bucket_name("creation-placement");
            let upload_generation = GenerationId::MIN;
            let payload_generation = GenerationId::new(2).unwrap();
            // Keep the selected data route distinct from the metadata route:
            // an installed rejection must reach placement, not fail admission.
            // Also distinguish MPU generation from the part's own generation.
            let key = (0..10_000)
                .map(|n| crate::tests::object_key(format!("key-{n}")))
                .find(|key| {
                    let put_pg = topology
                        .object_generation_segment_data_pg(&bucket, key, payload_generation, 0)
                        .get();
                    let part_pg = topology
                        .object_generation_multipart_part_segment_data_pg(
                            &bucket,
                            key,
                            upload_generation,
                            1,
                            0,
                        )
                        .get();
                    let crossed_part_pg = topology
                        .object_generation_multipart_part_segment_data_pg(
                            &bucket,
                            key,
                            payload_generation,
                            1,
                            0,
                        )
                        .get();
                    put_pg != topology.object_pg_for(&bucket, key)
                        && part_pg != topology.object_pg_for(&bucket, key)
                        && part_pg != crossed_part_pg
                })
                .unwrap();
            let object_pg = topology.object_pg_for(&bucket, &key);
            let selected_pg = if part {
                topology
                    .object_generation_multipart_part_segment_data_pg(
                        &bucket,
                        &key,
                        upload_generation,
                        1,
                        0,
                    )
                    .get()
            } else {
                topology
                    .object_generation_segment_data_pg(&bucket, &key, payload_generation, 0)
                    .get()
            };
            assert_ne!(selected_pg, object_pg);
            if part {
                assert_ne!(
                    selected_pg,
                    topology
                        .object_generation_multipart_part_segment_data_pg(
                            &bucket,
                            &key,
                            payload_generation,
                            1,
                            0
                        )
                        .get()
                );
            }
            if unavailable {
                config.pg_routes[selected_pg as usize].state = crate::PgState::Peering;
            }
            let unrelated_pg = (0..8)
                .find(|&pg| pg != selected_pg && pg != object_pg)
                .unwrap();
            config.pg_routes[unrelated_pg as usize].state = crate::PgState::Peering;
            private_socket_dir(config.socket_path.parent().unwrap());
            let server = StorageNodeServer::bind(config.clone()).unwrap();
            let node = server.test_storage_node();
            let upload_id = crate::tests::multipart_upload_id("creation");
            let upload = {
                let pg = node.get_pg(object_pg).unwrap();
                // Metadata-only fixture; no encrypted payload or shard I/O.
                PgMetadataStore::create_multipart_upload(
                    &*pg,
                    &CreateMultipartUploadReq {
                        upload_id: upload_id.clone(),
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
                        encryption: metadata_only_test_encryption(),
                    },
                )
                .unwrap();
                pg.refresh_metadata_command_state_digest().unwrap();
                let upload = pg.get_multipart_upload(&upload_id).unwrap();
                assert_eq!(upload.object_generation_id, upload_generation);
                assert_eq!(
                    PgMetadataStore::next_generation_id(&*pg, &bucket, &key).unwrap(),
                    payload_generation
                );
                upload
            };
            let request = CreateStreamUploadReq {
                session_id: crate::tests::stream_session_id("placement"),
                bucket: bucket.clone(),
                key: key.clone(),
                target: if part {
                    StreamUploadTarget::UploadPart {
                        upload_id,
                        part_number: 1,
                    }
                } else {
                    StreamUploadTarget::PutObject
                },
                encryption: upload.encryption.clone(),
            };
            let mut proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
            proof.operation_kind = if part {
                UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
            } else {
                PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
            }
            .to_string();
            let _server = spawn_test_storage_node_server(server);
            let unix = UnixStorageNodeClient::new(
                config.node_id,
                config.cluster_epoch,
                config.socket_path.clone(),
            )
            .with_pg_topology(topology);
            let local = LocalStorageNodeClient::new(config.node_id, node.clone());
            let client: &dyn ObjectMutationMetadataNodeClient = if remote { &unix } else { &local };
            let route = client
                .open_stream_upload_creation_metadata_route(
                    config.cluster_epoch,
                    ObjectMetadataPgId::new_for_test(PgId::new(object_pg)),
                    &bucket,
                    &key,
                )
                .unwrap();
            let observe = || {
                let pg = node.get_pg(object_pg).unwrap();
                assert!(matches!(
                    PgMetadataStore::get_object_generation_reservation(
                        &*pg,
                        &bucket,
                        &key,
                        &request.session_id
                    ),
                    Err(MetadataError::ObjectGenerationReservationNotFound { .. })
                ));
                (
                    (
                        pg.list_all_stream_uploads().unwrap(),
                        pg.list_stream_segments(&request.session_id).unwrap(),
                        pg.get_multipart_upload(&upload.upload_id).unwrap(),
                    ),
                    pg.metadata_command_replica_state().unwrap(),
                    pg.max_metadata_command_log_index(config.cluster_epoch)
                        .unwrap(),
                    PgMetadataStore::next_generation_id(&*pg, &bucket, &key).unwrap(),
                )
            };
            let before = observe();
            // The frontend deliberately has a healthy catalogue even in the
            // failing case: installed-server validation must reject it itself.
            let mut frontend_config = config.clone();
            frontend_config.pg_routes[selected_pg as usize].state = crate::PgState::Active;
            let build = BuildCreateStreamUploadCommandReq {
                placement: PayloadPlacementInput::from_server_config(if remote {
                    &frontend_config
                } else {
                    &config
                }),
                request: &request,
                cleanup_after: Some(10_000),
                precondition: if part {
                    CreateStreamUploadPrecondition::UploadPart {
                        expected_upload: &upload,
                    }
                } else {
                    CreateStreamUploadPrecondition::PutObjectNoCurrentCheck {}
                },
                bucket_write_reservation: &proof,
            };
            let result = route.build_create_stream_upload_command(build);
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
                        matches!(error, ObjectPgActionError::Store(StoreError::PgNotActive { pg_id, state: crate::PgState::Peering, .. }) if pg_id == selected_pg)
                    },
                    "remote={remote} part={part}: {error:?}"
                );
                assert_eq!(observe(), before, "failed placement advanced durable state");
            } else {
                let command = result.unwrap();
                let MetadataCommandPayload::CreateStreamUpload(create) = command.payload() else {
                    panic!("unexpected command")
                };
                assert_eq!(create.payload_generation, payload_generation);
                assert_eq!(command.id().log_index().get(), before.2 + 1);
                let mut crossed_catalogue = frontend_config.clone();
                crossed_catalogue.pg_routes[selected_pg as usize].state = crate::PgState::Peering;
                let crossed = BuildCreateStreamUploadCommandReq {
                    placement: PayloadPlacementInput::from_server_config(&crossed_catalogue),
                    request: &request,
                    cleanup_after: Some(10_000),
                    precondition: if part {
                        CreateStreamUploadPrecondition::UploadPart {
                            expected_upload: &upload,
                        }
                    } else {
                        CreateStreamUploadPrecondition::PutObjectNoCurrentCheck {}
                    },
                    bucket_write_reservation: &proof,
                };
                assert!(
                    matches!(unix.validate_create_stream_upload_command_response(
                    &command, ObjectMetadataPgId::new_for_test(PgId::new(object_pg)),
                    config.cluster_epoch, &crossed),
                    Err(ObjectPgActionError::Store(StoreError::PgNotActive { pg_id, .. })) if pg_id == selected_pg)
                );
                let pg = node.get_pg(object_pg).unwrap();
                assert!(matches!(
                    pg.get_stream_upload(&request.session_id),
                    Err(MetadataError::StreamSessionNotFound { .. })
                ));
                pg.apply_metadata_command_and_record(config.node_id.as_u32(), &command)
                    .unwrap();
                assert_eq!(
                    pg.get_stream_upload(&request.session_id).unwrap().target,
                    request.target
                );
                assert_eq!(
                    PgMetadataStore::get_object_generation_reservation(
                        &*pg,
                        &bucket,
                        &key,
                        &request.session_id
                    )
                    .unwrap(),
                    payload_generation
                );
            }
        }
    }
}
