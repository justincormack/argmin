// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn unix_put_stream_issuer_survives_primary_move_and_return_without_adoption() {
    primary_move_and_return(false);
}

#[test]
fn unix_part_stream_issuer_survives_primary_move_and_return_without_adoption() {
    primary_move_and_return(true);
}

fn primary_move_and_return(part: bool) {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(7), NodeId::new(8), NodeId::new(9)];
    let mut configs = node_ids
        .iter()
        .map(|&node_id| {
            let mut config = test_config(&tmp);
            config.node_id = node_id;
            config.data_dir = tmp.path().join(format!("node-{}", node_id.as_u32()));
            config.socket_path = tmp
                .path()
                .join("sock")
                .join(format!("{}.sock", node_id.as_u32()));
            config.default_ec_shape = EcShape { k: 2, m: 1 };
            config.route_map_validity =
                RouteMapValidity::until_ms(crate::clock::current_time_millis() + 300_000).unwrap();
            config.pg_routes[0].acting_set = node_ids.to_vec();
            config
        })
        .collect::<Vec<_>>();
    private_socket_dir(configs[0].socket_path.parent().unwrap());
    let servers = configs
        .iter()
        .map(|config| Arc::new(StorageNodeServer::bind(config.clone()).unwrap()))
        .collect::<Vec<_>>();
    let serving = servers
        .iter()
        .map(|server| spawn_shared_test_storage_node_server(Arc::clone(server)))
        .collect::<Vec<_>>();
    let clients = configs
        .iter()
        .map(|config| {
            UnixStorageNodeClient::new(
                config.node_id,
                config.cluster_epoch,
                config.socket_path.clone(),
            )
        })
        .collect::<Vec<_>>();
    let pg_id = ObjectMetadataPgId::new_for_test(PgId::new(0));
    let bucket = crate::tests::bucket_name("published-issuer");
    let key = crate::tests::object_key("key");
    let upload_id = crate::tests::multipart_upload_id("issuer-mpu");
    let encryption = metadata_only_test_encryption();

    // Metadata-only fixture: exercise command construction, replica application,
    // and allocation, not ciphertext publication. Seed identical MPU state on
    // the three independent runtimes; all stream commands below use real RPCs.
    let uploads = servers
        .iter()
        .map(|server| {
            let node = server.test_storage_node();
            let pg = node.get_pg(0).unwrap();
            crate::clock::with_time_override(1_000, || {
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
                        encryption: encryption.clone(),
                    },
                )
                .unwrap();
            });
            pg.refresh_metadata_command_state_digest().unwrap();
            pg.get_multipart_upload(&upload_id).unwrap()
        })
        .collect::<Vec<_>>();
    assert!(uploads.iter().all(|upload| upload == &uploads[0]));
    let upload = &uploads[0];
    let target = if part {
        StreamUploadTarget::UploadPart {
            upload_id,
            part_number: 1,
        }
    } else {
        StreamUploadTarget::PutObject
    };
    let old_request = CreateStreamUploadReq {
        session_id: crate::tests::stream_session_id("old-issuer"),
        bucket: bucket.clone(),
        key: key.clone(),
        target: target.clone(),
        encryption,
    };
    let mut proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
    proof.operation_kind = if part {
        UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
    } else {
        PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
    }
    .into();
    let observe = || {
        servers
            .iter()
            .zip(node_ids)
            .map(|(server, node_id)| {
                let node = server.test_storage_node();
                let pg = node.get_pg(0).unwrap();
                (
                    pg.metadata_command_checkpoint(node_id.as_u32(), ClusterEpoch::INITIAL)
                        .unwrap(),
                    pg.pending_metadata_command_envelope(node_id.as_u32(), ClusterEpoch::INITIAL)
                        .unwrap(),
                    pg.list_all_stream_uploads().unwrap(),
                    pg.list_stream_segments(&old_request.session_id).unwrap(),
                )
            })
            .collect::<Vec<_>>()
    };
    let create = |primary: usize,
                  config: &StorageNodeProcessConfig,
                  request: &CreateStreamUploadReq| {
        let mut proof = proof.clone();
        proof.reservation_id = format!("proof-{}", request.session_id.as_str());
        let route = clients[primary]
            .open_stream_upload_creation_metadata_route(config.cluster_epoch, pg_id, &bucket, &key)
            .unwrap();
        let command = route
            .build_create_stream_upload_command(BuildCreateStreamUploadCommandReq {
                effect_fence: AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                placement: PayloadPlacementInput::from_server_config(config),
                request,
                cleanup_after: Some(crate::clock::current_time_millis() + 300_000),
                precondition: if part {
                    CreateStreamUploadPrecondition::UploadPart {
                        expected_upload: upload,
                    }
                } else {
                    CreateStreamUploadPrecondition::PutObjectNoCurrentCheck {}
                },
                bucket_write_reservation: &proof,
            })
            .unwrap();
        // Apply the witness before the primary. The stored primary issuer must
        // never be compared to a replica's independent process lifetime.
        let mut states = Vec::new();
        for offset in [1, 2, 0] {
            let index = (primary + offset) % clients.len();
            states.push(
                MetadataCommandNodeClient::apply_metadata_command_and_record(
                    &clients[index],
                    pg_id.pg_id(),
                    &command,
                )
                .unwrap(),
            );
        }
        assert!(states.iter().all(|state| state == &states[0]));
        command
    };
    let old_command = create(0, &configs[0], &old_request);
    let MetadataCommandPayload::CreateStreamUpload(old_create) = old_command.payload() else {
        panic!("expected stream creation");
    };
    assert_eq!(old_create.allocation_issuer.node_id, node_ids[0]);
    let original_lifetime = old_create.allocation_issuer.lifetime;

    // Keep the epoch fixed to isolate issuer binding: rejection must not come
    // from a stale request epoch, a stale session snapshot or an exhausted slot.
    // Stage 0 is a live-primary positive control; stages 1 and 2 actually publish
    // a different primary and return to the original still-open process.
    for (stage, primary) in [0, 1, 0].into_iter().enumerate() {
        if stage != 0 {
            let before_publication = observe();
            for (server, config) in servers.iter().zip(&mut configs) {
                config.pg_routes[0].primary_node_id = node_ids[primary];
                server
                    .install_control_plane_runtime_config(config.clone())
                    .unwrap();
            }
            assert_eq!(
                observe(),
                before_publication,
                "route publication changed durable state"
            );
            let current_lifetime = servers[primary]
                .test_storage_node()
                .get_pg(0)
                .unwrap()
                .test_allocation_lifetime_identity()
                .unwrap();
            assert_ne!(current_lifetime, original_lifetime);
            if stage == 2 {
                assert_eq!(node_ids[primary], old_create.allocation_issuer.node_id);
            }
            let client = &clients[primary];
            let route = client
                .open_stream_upload_session_metadata_route(
                    configs[primary].cluster_epoch,
                    pg_id,
                    &bucket,
                    &key,
                    &old_request.session_id,
                )
                .unwrap();
            let session = route.load_session().unwrap();
            assert_eq!(session.state, StreamUploadState::InProgress);
            assert_eq!(session.next_segment_vid, GenerationId::MIN);
            assert!(route.load_segments().unwrap().is_empty());
            let expected = if part {
                crate::ExpectedStreamAppendState::UploadPart {
                    session,
                    upload: Box::new(upload.clone()),
                    part_generation_id: old_create.payload_generation,
                }
            } else {
                crate::ExpectedStreamAppendState::PutObject {
                    session,
                    generation_id: old_create.payload_generation,
                }
            };
            let before = observe();
            let rpc_count = rpc_requests_started_for_test(client);
            let error = route
                .prepare_segment_append_with_expected_append_state(
                    &PrepareStreamUploadSegmentAppendReq {
                        session_id: old_request.session_id.clone(),
                        segment_index: 0,
                        size: 1,
                        segment_crc64: 0,
                        payload_crc64: 0,
                    },
                    Some(&expected),
                    AdmittedRouteEffectFence::unbounded(ClusterEpoch::INITIAL),
                    PayloadPlacementInput::from_server_config(&configs[primary]),
                )
                .expect_err(&format!(
                    "stage {stage}: old issuer must not allocate on primary {primary}"
                ));
            assert_lifetime_unavailable(error, true);
            assert!(rpc_requests_started_for_test(client) > rpc_count);
            assert_eq!(observe(), before, "rejected old issuance mutated a replica");

            // Convergence/matching is not fresh allocation. Exact replay is
            // still accepted after the route and live issuer have changed.
            for client in &clients {
                MetadataCommandNodeClient::apply_metadata_command_and_record(
                    client,
                    pg_id.pg_id(),
                    &old_command,
                )
                .unwrap();
            }
            let creation_route = client
                .open_stream_upload_creation_metadata_route(
                    ClusterEpoch::INITIAL,
                    pg_id,
                    &bucket,
                    &key,
                )
                .unwrap();
            assert!(creation_route
                .matching_stream_upload_exists(&old_request, Some(old_create),)
                .unwrap());
            assert_eq!(
                observe(),
                before,
                "exact replay/matching changed existing state"
            );
        }

        let mut fresh = old_request.clone();
        fresh.session_id = crate::tests::stream_session_id(format!("control-{stage}"));
        let command = create(primary, &configs[primary], &fresh);
        let MetadataCommandPayload::CreateStreamUpload(created) = command.payload() else {
            panic!("expected fresh stream creation");
        };
        assert_eq!(created.allocation_issuer.node_id, node_ids[primary]);
        let current_lifetime = servers[primary]
            .test_storage_node()
            .get_pg(0)
            .unwrap()
            .test_allocation_lifetime_identity()
            .unwrap();
        assert_eq!(created.allocation_issuer.lifetime, current_lifetime);
        let route = clients[primary]
            .open_stream_upload_session_metadata_route(
                ClusterEpoch::INITIAL,
                pg_id,
                &bucket,
                &key,
                &fresh.session_id,
            )
            .unwrap();
        let session = route.load_session().unwrap();
        let expected = if part {
            crate::ExpectedStreamAppendState::UploadPart {
                session,
                upload: Box::new(upload.clone()),
                part_generation_id: created.payload_generation,
            }
        } else {
            crate::ExpectedStreamAppendState::PutObject {
                session,
                generation_id: created.payload_generation,
            }
        };
        let (actual_target, segment) = route
            .prepare_segment_append_with_expected_append_state(
                &PrepareStreamUploadSegmentAppendReq {
                    session_id: fresh.session_id.clone(),
                    segment_index: 0,
                    size: 1,
                    segment_crc64: 0,
                    payload_crc64: 0,
                },
                Some(&expected),
                AdmittedRouteEffectFence::unbounded(ClusterEpoch::INITIAL),
                PayloadPlacementInput::from_server_config(&configs[primary]),
            )
            .unwrap();
        assert_eq!(actual_target, target);
        assert_eq!(segment.segment_vid, GenerationId::MIN);
        assert_eq!(route.load_session().unwrap().next_segment_vid.get(), 2);
        for (index, server) in servers.iter().enumerate() {
            let node = server.test_storage_node();
            let pg = node.get_pg(0).unwrap();
            assert_eq!(
                pg.get_stream_upload(&old_request.session_id)
                    .unwrap()
                    .next_segment_vid,
                GenerationId::MIN
            );
            assert_eq!(
                pg.test_stream_allocation_issuer(&old_request.session_id),
                old_create.allocation_issuer
            );
            assert_eq!(
                pg.get_stream_upload(&fresh.session_id)
                    .unwrap()
                    .next_segment_vid
                    .get(),
                if index == primary { 2 } else { 1 }
            );
        }
    }
    drop(serving);
}
