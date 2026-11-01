// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn unix_put_stream_checkpoint_transfer_preserves_issuer_without_allocation_authority() {
    checkpoint_transfer(false);
}

#[test]
fn unix_part_stream_checkpoint_transfer_preserves_issuer_without_allocation_authority() {
    checkpoint_transfer(true);
}

fn expected_append(
    session: StreamUploadRecord,
    upload: &MultipartUploadRecord,
    generation: GenerationId,
) -> crate::ExpectedStreamAppendState {
    match session.target {
        StreamUploadTarget::PutObject => crate::ExpectedStreamAppendState::PutObject {
            session,
            generation_id: generation,
        },
        StreamUploadTarget::UploadPart { .. } => crate::ExpectedStreamAppendState::UploadPart {
            session,
            upload: Box::new(upload.clone()),
            part_generation_id: generation,
        },
    }
}

fn checkpoint_transfer(part: bool) {
    let tmp = test_util::tempdir();
    let mut source_config = test_config(&tmp);
    source_config.default_ec_shape = EcShape { k: 1, m: 0 };
    source_config.route_map_validity =
        RouteMapValidity::until_ms(crate::clock::current_time_millis() + 300_000).unwrap();
    private_socket_dir(source_config.socket_path.parent().unwrap());
    let source = Arc::new(StorageNodeServer::bind(source_config.clone()).unwrap());
    let source_serving = spawn_shared_test_storage_node_server(Arc::clone(&source));
    let source_client = UnixStorageNodeClient::new(
        source_config.node_id,
        source_config.cluster_epoch,
        source_config.socket_path.clone(),
    );
    let pg_id = ObjectMetadataPgId::new_for_test(PgId::new(0));
    let bucket = crate::tests::bucket_name("transferred-issuer");
    let key = crate::tests::object_key("key");
    // Metadata-only transfer fixture: no physical payload is written or read.
    let upload = {
        let node = source.test_storage_node();
        let pg = node.get_pg(0).unwrap();
        let request = CreateMultipartUploadReq {
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
            encryption: metadata_only_test_encryption(),
        };
        PgMetadataStore::create_multipart_upload(&*pg, &request).unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        pg.get_multipart_upload(&request.upload_id).unwrap()
    };
    let request = CreateStreamUploadReq {
        session_id: crate::tests::stream_session_id("transferred"),
        bucket: bucket.clone(),
        key: key.clone(),
        target: if part {
            StreamUploadTarget::UploadPart {
                upload_id: upload.upload_id.clone(),
                part_number: 1,
            }
        } else {
            StreamUploadTarget::PutObject
        },
        encryption: upload.encryption.clone(),
    };
    let create = |client: &UnixStorageNodeClient,
                  config: &StorageNodeProcessConfig,
                  request: &CreateStreamUploadReq| {
        let mut proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
        proof.cluster_epoch = config.cluster_epoch;
        proof.reservation_id = format!("proof-{}", request.session_id.as_str());
        proof.operation_kind = if part {
            UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
        } else {
            PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
        }
        .into();
        let route = client
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
                        expected_upload: &upload,
                    }
                } else {
                    CreateStreamUploadPrecondition::PutObjectNoCurrentCheck {}
                },
                bucket_write_reservation: &proof,
            })
            .unwrap();
        MetadataCommandNodeClient::apply_metadata_command_and_record(
            client,
            pg_id.pg_id(),
            &command,
        )
        .unwrap();
        command
    };
    let creation = create(&source_client, &source_config, &request);
    let MetadataCommandPayload::CreateStreamUpload(created) = creation.payload() else {
        panic!("expected stream creation");
    };
    let issuer = created.allocation_issuer.clone();
    let generation = created.payload_generation;
    let checkpoint = MetadataCommandInspectionNodeClient::metadata_command_checkpoint(
        &source_client,
        pg_id.pg_id(),
        source_config.cluster_epoch,
    )
    .unwrap();
    // Export the base before local preparation. The successor therefore has an
    // unused cursor, not a floor whose exhaustion could mask missing fencing.
    let (segment, append, appended_state) = {
        let route = source_client
            .open_stream_upload_session_metadata_route(
                source_config.cluster_epoch,
                pg_id,
                &bucket,
                &key,
                &request.session_id,
            )
            .unwrap();
        let (crc, witness) = crate::types::SegmentShardIntegrityRecord::for_zero_filled_segment(
            source_config.default_ec_shape,
            request.encryption.segment_stored_size(1).unwrap(),
        );
        let expected = expected_append(route.load_session().unwrap(), &upload, generation);
        let (_, mut segment) = route
            .prepare_segment_append_with_expected_append_state(
                &PrepareStreamUploadSegmentAppendReq {
                    session_id: request.session_id.clone(),
                    segment_index: 0,
                    size: 1,
                    segment_crc64: crc,
                    payload_crc64: 0,
                },
                Some(&expected),
                AdmittedRouteEffectFence::unbounded(source_config.cluster_epoch),
                PayloadPlacementInput::from_server_config(&source_config),
            )
            .unwrap();
        segment.shard_integrity = Some(witness);
        let append = route
            .build_append_command(
                &request.target,
                &segment,
                AdmittedRouteEffectFence::unbounded(source_config.cluster_epoch),
                PayloadPlacementInput::from_server_config(&source_config),
            )
            .unwrap();
        let state = MetadataCommandNodeClient::apply_metadata_command_and_record(
            &source_client,
            pg_id.pg_id(),
            &append,
        )
        .unwrap();
        (segment, append, state)
    };
    drop(source_serving);
    drop(source);

    let mut config = source_config.clone();
    config.data_dir = tmp.path().join("destination");
    config.socket_path = tmp.path().join("sock").join("destination.sock");
    config.cluster_epoch = ClusterEpoch::new(2).unwrap();
    config.pg_routes[0].cluster_epoch = config.cluster_epoch;
    config.pg_routes[0].state = crate::PgState::Peering;
    config.pg_routes[0].metadata_transfer_destination_epoch = Some(config.cluster_epoch);
    // Reuse the node ID deliberately: only a fresh process lifetime distinguishes
    // the successor from the old issuer. No two runtimes open the same stores.
    let mut server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let mut serving = spawn_shared_test_storage_node_server(Arc::clone(&server));
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    {
        let peering = client
            .open_metadata_command_peering_route(pg_id.pg_id(), config.cluster_epoch)
            .unwrap();
        let imported = peering
            .install_metadata_transfer_checkpoint_base(&checkpoint)
            .unwrap();
        assert_eq!(imported.applied_log_index, 0);
        assert_eq!(imported.state_digest, checkpoint.state_digest);
        let transferred = MetadataCommandInspectionNodeClient::metadata_command_checkpoint(
            &client,
            pg_id.pg_id(),
            config.cluster_epoch,
        )
        .unwrap();
        assert_eq!(transferred.table_blocks, checkpoint.table_blocks);
        {
            let node = server.test_storage_node();
            let pg = node.get_pg(0).unwrap();
            assert_eq!(
                pg.load_stream_allocation_issuer(&request.session_id)
                    .unwrap(),
                issuer
            );
            assert_eq!(
                pg.get_stream_upload(&request.session_id)
                    .unwrap()
                    .next_segment_vid,
                GenerationId::MIN
            );
            assert_ne!(
                pg.stream_allocation_issuer(config.node_id)
                    .unwrap()
                    .lifetime,
                issuer.lifetime
            );
        }
        // Replay the already-issued suffix at the destination prefix. This tests
        // the installed peering apply boundary, not control-plane certification.
        let rebased = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                config.cluster_epoch,
                pg_id.pg_id(),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            append.payload().clone(),
        );
        let applied = peering
            .replay_metadata_command_for_peering(&rebased)
            .unwrap();
        assert_eq!(applied.state_digest, appended_state.state_digest);
        assert_eq!(applied.applied_log_index, 1);
        assert_eq!(
            peering
                .replay_metadata_command_for_peering(&rebased)
                .unwrap(),
            applied
        );
    }
    config.pg_routes[0].state = crate::PgState::Active;
    config.pg_routes[0].metadata_transfer_destination_epoch = None;
    server
        .install_control_plane_runtime_config(config.clone())
        .unwrap();

    let mut last_live_lifetime = issuer.lifetime;
    for stage in 0..2 {
        let node = server.test_storage_node();
        let current_issuer = node
            .get_pg(0)
            .unwrap()
            .stream_allocation_issuer(config.node_id)
            .unwrap();
        assert_eq!(current_issuer.node_id, issuer.node_id);
        assert_ne!(current_issuer.lifetime, last_live_lifetime);
        last_live_lifetime = current_issuer.lifetime;
        let route = client
            .open_stream_upload_session_metadata_route(
                config.cluster_epoch,
                pg_id,
                &bucket,
                &key,
                &request.session_id,
            )
            .unwrap();
        let observe = || {
            let pg = node.get_pg(0).unwrap();
            (
                pg.metadata_command_checkpoint(config.node_id.as_u32(), config.cluster_epoch)
                    .unwrap(),
                pg.pending_metadata_command_envelope(config.node_id.as_u32(), config.cluster_epoch)
                    .unwrap(),
                pg.get_stream_upload(&request.session_id).unwrap(),
                pg.list_stream_segments(&request.session_id).unwrap(),
                pg.get_object_generation_reservation(&bucket, &key, &request.session_id)
                    .unwrap(),
                pg.load_stream_allocation_issuer(&request.session_id)
                    .unwrap(),
            )
        };
        let before = observe();
        assert_eq!(before.2.state, StreamUploadState::InProgress);
        assert_eq!(before.2.next_segment_vid.get(), 2);
        assert_eq!(before.3, vec![segment.clone()]);
        assert_eq!(before.4, generation);
        assert_eq!(before.5, issuer);
        let expected = expected_append(route.load_session().unwrap(), &upload, generation);
        let rpc_count = rpc_requests_started_for_test(&client);
        let error = route
            .prepare_segment_append_with_expected_append_state(
                &PrepareStreamUploadSegmentAppendReq {
                    session_id: request.session_id.clone(),
                    segment_index: 1,
                    size: 1,
                    segment_crc64: 0,
                    payload_crc64: 0,
                },
                Some(&expected),
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                PayloadPlacementInput::from_server_config(&config),
            )
            .expect_err("transferred session must not inherit the destination's live issuer");
        assert_lifetime_unavailable(error, true);
        assert!(rpc_requests_started_for_test(&client) > rpc_count);
        assert_eq!(
            observe(),
            before,
            "stage {stage}: rejection mutated transferred state"
        );

        let mut fresh = request.clone();
        fresh.session_id = crate::tests::stream_session_id(format!("fresh-{stage}"));
        let fresh_command = create(&client, &config, &fresh);
        let MetadataCommandPayload::CreateStreamUpload(fresh_create) = fresh_command.payload()
        else {
            panic!("expected fresh stream creation");
        };
        assert_eq!(fresh_create.allocation_issuer, current_issuer);
        let fresh_route = client
            .open_stream_upload_session_metadata_route(
                config.cluster_epoch,
                pg_id,
                &bucket,
                &key,
                &fresh.session_id,
            )
            .unwrap();
        let expected = expected_append(
            fresh_route.load_session().unwrap(),
            &upload,
            fresh_create.payload_generation,
        );
        let (_, prepared) = fresh_route
            .prepare_segment_append_with_expected_append_state(
                &PrepareStreamUploadSegmentAppendReq {
                    session_id: fresh.session_id.clone(),
                    segment_index: 0,
                    size: 1,
                    segment_crc64: 0,
                    payload_crc64: 0,
                },
                Some(&expected),
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                PayloadPlacementInput::from_server_config(&config),
            )
            .unwrap();
        assert_eq!(prepared.segment_vid, GenerationId::MIN);
        assert_eq!(
            fresh_route.load_session().unwrap().next_segment_vid.get(),
            2
        );
        drop(fresh_route);
        drop(route);
        drop(node);
        if stage == 0 {
            let before_reopen = MetadataCommandInspectionNodeClient::metadata_command_checkpoint(
                &client,
                pg_id.pg_id(),
                config.cluster_epoch,
            )
            .unwrap();
            drop(serving);
            drop(server);
            server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
            serving = spawn_shared_test_storage_node_server(Arc::clone(&server));
            let after_reopen = MetadataCommandInspectionNodeClient::metadata_command_checkpoint(
                &client,
                pg_id.pg_id(),
                config.cluster_epoch,
            )
            .unwrap();
            assert_eq!(
                after_reopen, before_reopen,
                "reopening must not change durable state"
            );
        }
    }
    drop(serving);
}
