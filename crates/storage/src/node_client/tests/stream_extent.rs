// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn invalid_extents() -> [(u32, u64); 4] {
    [
        (0, crate::INTERNAL_SEGMENT_SIZE as u64 + 1),
        (0, u64::MAX),
        (crate::payload_limits::MAX_STREAM_SEGMENTS, 0),
        (u32::MAX, 1),
    ]
}

#[test]
fn unix_stream_extent_rejection_precedes_transport() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let client = test_unix_storage_node_client();
    let bucket = crate::tests::bucket_name("extent-bucket");
    let key = crate::tests::object_key("key");
    let session_id = crate::tests::stream_session_id("extent");
    let route = client
        .open_stream_upload_session_metadata_route(
            client.cluster_epoch,
            ObjectMetadataPgId::new_for_test(PgId::new(0)),
            &bucket,
            &key,
            &session_id,
        )
        .unwrap();
    let before = rpc_requests_started_for_test(&client);
    for (segment_index, size) in invalid_extents() {
        let error = route
            .prepare_segment_append_with_expected_append_state(
                &PrepareStreamUploadSegmentAppendReq {
                    session_id: session_id.clone(),
                    segment_index,
                    size,
                    segment_crc64: 0,
                    payload_crc64: 0,
                },
                None,
                AdmittedRouteEffectFence::unbounded(client.cluster_epoch),
                PayloadPlacementInput::from_server_config(&config),
            )
            .unwrap_err();
        assert!(
            matches!(error, ObjectPgActionError::InvalidRequest { reason }
            if reason.contains("single-write index or logical-size limit"))
        );
        assert_eq!(rpc_requests_started_for_test(&client), before);
    }
}

#[derive(Clone, Copy)]
enum PreparationChecks {
    Extents,
    Placement,
}

fn preparation_rejects_before_allocation(remote: bool, checks: PreparationChecks) {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.default_ec_shape = match checks {
        PreparationChecks::Extents => EcShape { k: 1, m: 0 },
        PreparationChecks::Placement => EcShape { k: 2, m: 1 },
    };
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let node = server.test_storage_node();
    let bucket = crate::tests::bucket_name("extent-bucket");
    let key = crate::tests::object_key("key");
    let upload_id = crate::tests::multipart_upload_id("extent");
    let targets = [
        StreamUploadTarget::PutObject,
        StreamUploadTarget::UploadPart {
            upload_id: upload_id.clone(),
            part_number: 1,
        },
    ];
    let mut sessions = Vec::new();
    {
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &crate::CanonicalUserId::from_principal("owner"),
            &AclGrants::default(),
            false,
            false,
        )
        .unwrap();
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
        for (index, target) in targets.iter().enumerate() {
            let session_id = crate::tests::stream_session_id(format!("extent-{index}"));
            let generation = pg
                .reserve_object_generation(&bucket, &key, &session_id)
                .unwrap();
            // Metadata-only preparation fixture: no ciphertext is supplied or
            // persisted. The positive control tests allocation, not payload IO.
            pg.create_stream_upload(&CreateStreamUploadReq {
                session_id: session_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: target.clone(),
                encryption: metadata_only_test_encryption(),
            })
            .unwrap();
            let session = pg.get_stream_upload(&session_id).unwrap();
            let expected = match target {
                StreamUploadTarget::PutObject => crate::ExpectedStreamAppendState::PutObject {
                    session,
                    generation_id: generation,
                },
                StreamUploadTarget::UploadPart { .. } => {
                    crate::ExpectedStreamAppendState::UploadPart {
                        session,
                        upload: Box::new(pg.get_multipart_upload(&upload_id).unwrap()),
                        part_generation_id: generation,
                    }
                }
            };
            sessions.push((session_id, expected));
        }
    }
    let _server = spawn_test_storage_node_server(server);
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let placement = PayloadPlacementInput::from_server_config(&config);
    let local = LocalStorageNodeClient::new(config.node_id, node.clone());
    let pg_id = ObjectMetadataPgId::new_for_test(PgId::new(0));
    let fence = AdmittedRouteEffectFence::unbounded(config.cluster_epoch);
    for (session_id, expected) in &sessions {
        let route = local
            .open_stream_upload_session_metadata_route(
                config.cluster_epoch,
                pg_id,
                &bucket,
                &key,
                session_id,
            )
            .unwrap();
        let observe = || {
            let pg = node.get_pg(0).unwrap();
            let rows = sessions
                .iter()
                .map(|(id, _)| {
                    (
                        pg.get_stream_upload(id).unwrap(),
                        pg.list_stream_segments(id).unwrap(),
                        pg.get_object_generation_reservation(&bucket, &key, id)
                            .unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            (
                rows,
                pg.metadata_command_replica_state().unwrap(),
                pg.max_metadata_command_log_index(config.cluster_epoch)
                    .unwrap(),
            )
        };
        let before = observe();
        if matches!(checks, PreparationChecks::Placement) {
            let request = PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: 1,
                segment_crc64: 0,
                payload_crc64: 0,
            };
            let mut usable = config.clone();
            usable.pg_routes[0].acting_set = [7, 8, 9].map(NodeId::new).to_vec();
            // Each malformed catalogue has an independent discriminator; all
            // attempts retain the same valid captured session/generation.
            for case in 0..7 {
                let mut invalid = usable.clone();
                match case {
                    0 => invalid.pg_routes[0].acting_set = vec![config.node_id],
                    1 => invalid.pg_routes.clear(),
                    2 => invalid.pg_routes[0].state = crate::PgState::Peering,
                    3 => invalid.pg_routes[0].cluster_epoch = ClusterEpoch::new(2).unwrap(),
                    4 => {
                        invalid.cluster_epoch = ClusterEpoch::new(2).unwrap();
                        invalid.pg_routes[0].cluster_epoch = invalid.cluster_epoch;
                    }
                    5 => invalid.default_ec_shape = EcShape { k: 1, m: 0 },
                    6 => invalid.pg_routes[0].primary_node_id = NodeId::new(99),
                    _ => unreachable!(),
                }
                let error = route
                    .prepare_segment_append_with_expected_append_state(
                        &request,
                        Some(expected),
                        fence,
                        PayloadPlacementInput::from_server_config(&invalid),
                    )
                    .unwrap_err();
                assert!(
                    match (&error, case) {
                        (
                            ObjectPgActionError::Store(StoreError::PayloadShardSetMismatch {
                                reason,
                            }),
                            0,
                        ) => reason.starts_with("captured payload placement:"),
                        (
                            ObjectPgActionError::Store(StoreError::ClusterPgNotFound {
                                pg_id: 0,
                                ..
                            }),
                            1,
                        )
                        | (
                            ObjectPgActionError::Store(StoreError::PgNotActive {
                                pg_id: 0,
                                state: crate::PgState::Peering,
                                ..
                            }),
                            2,
                        )
                        | (
                            ObjectPgActionError::Store(StoreError::StaleMetadataRoute {
                                pg_id: 0,
                                ..
                            }),
                            3,
                        )
                        | (
                            ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                                pg_id: 0,
                                ..
                            }),
                            4,
                        )
                        | (
                            ObjectPgActionError::Store(
                                StoreError::RouteCapabilitySubjectMismatch {
                                    operation: "stream preparation captured placement",
                                },
                            ),
                            5 | 6,
                        ) => true,
                        _ => false,
                    },
                    "case {case}: {error:?}"
                );
                assert_eq!(
                    observe(),
                    before,
                    "case {case} consumed the sole preparation slot"
                );
            }
            if remote {
                // Bypass frontend placement checks to exercise the installed
                // handler's own catalogue (2+1 with only one acting node).
                let rpc = StorageRpcStreamSegmentAppendPrepareRequest {
                    object: client.object_request(pg_id.pg_id(), &bucket, &key),
                    request: request.clone(),
                    expected_append_state: Some(expected.clone()),
                    effect_deadline: None,
                };
                let error = client
                    .rpc_request(
                        StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare,
                        encode_stream_segment_append_prepare_request(&rpc),
                    )
                    .unwrap_err();
                let StoreError::StorageRpc {
                    failure, detail, ..
                } = error
                else {
                    panic!("unexpected preparation error: {error:?}");
                };
                assert_eq!(
                    failure.wire_code(),
                    StorageRpcErrorCode::Internal.wire_code(),
                    "{}",
                    detail.as_str()
                );
                // Object action diagnostics are deliberately redacted on wire.
                // The embedded cases above pin the precise placement cause.
                assert_eq!(detail.as_str(), "object placement-group action failed");
                assert_eq!(observe(), before);
            }
            // The same authority can still consume exactly its original slot
            // when supplied a placeable catalogue; rejection is not rollback
            // followed by an advanced allocator, nor a blanket failure.
            let (_, segment) = route
                .prepare_segment_append_with_expected_append_state(
                    &request,
                    Some(expected),
                    fence,
                    PayloadPlacementInput::from_server_config(&usable),
                )
                .unwrap();
            assert_eq!(segment.segment_vid, GenerationId::MIN);
            assert_eq!(segment.placement_cluster_epoch, config.cluster_epoch);
            assert_eq!(route.load_session().unwrap().next_segment_vid.get(), 2);
            continue;
        }
        for (segment_index, size) in invalid_extents() {
            let request = PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index,
                size,
                segment_crc64: 0,
                payload_crc64: 0,
            };
            if remote {
                // Bypass the sender's validation to pin installed-server
                // rejection rather than just the no-transport guard above.
                let request = StorageRpcStreamSegmentAppendPrepareRequest {
                    object: client.object_request(pg_id.pg_id(), &bucket, &key),
                    request,
                    expected_append_state: Some(expected.clone()),
                    effect_deadline: None,
                };
                let error = client
                    .rpc_request(
                        StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare,
                        encode_stream_segment_append_prepare_request(&request),
                    )
                    .unwrap_err();
                assert!(
                    matches!(
                        error,
                        StoreError::StorageRpc {
                            failure: StorageRpcErrorCode::PayloadDecode,
                            ..
                        }
                    ),
                    "{error:?}"
                );
            } else {
                let error = route
                    .prepare_segment_append_with_expected_append_state(
                        &request,
                        Some(expected),
                        fence,
                        placement,
                    )
                    .unwrap_err();
                assert!(
                    matches!(error, ObjectPgActionError::InvalidRequest { reason }
                    if reason.contains("single-write index or logical-size limit"))
                );
            }
            assert_eq!(
                observe(),
                before,
                "rejected {segment_index}/{size} must preserve both sessions and the log"
            );
        }
        let request = PrepareStreamUploadSegmentAppendReq {
            session_id: session_id.clone(),
            segment_index: 0,
            size: crate::INTERNAL_SEGMENT_SIZE as u64,
            segment_crc64: 0,
            payload_crc64: 0,
        };
        let remote_route = client
            .open_stream_upload_session_metadata_route(
                config.cluster_epoch,
                pg_id,
                &bucket,
                &key,
                session_id,
            )
            .unwrap();
        let positive = if remote {
            remote_route.as_ref()
        } else {
            route.as_ref()
        };
        let (_, segment) = positive
            .prepare_segment_append_with_expected_append_state(
                &request,
                Some(expected),
                fence,
                placement,
            )
            .unwrap();
        assert_eq!(segment.size, crate::INTERNAL_SEGMENT_SIZE as u64);
        assert_eq!(segment.segment_index, request.segment_index);
        assert_eq!(
            node.get_pg(0)
                .unwrap()
                .get_stream_upload(session_id)
                .unwrap()
                .next_segment_vid
                .get(),
            segment.segment_vid.get() + 1
        );
        // A successful preparation with its reply ignored has consumed the
        // local slot. Neither repeating it nor skipping ahead may allocate.
        let prepared = observe();
        for segment_index in [0, 1] {
            let retry = PrepareStreamUploadSegmentAppendReq {
                segment_index,
                ..request.clone()
            };
            let error = positive
                .prepare_segment_append_with_expected_append_state(
                    &retry,
                    Some(expected),
                    fence,
                    placement,
                )
                .unwrap_err();
            if remote {
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
            } else {
                assert!(
                    matches!(error, ObjectPgActionError::InvalidRequest { reason }
                    if reason.contains("unconsumed local slot"))
                );
            }
            assert_eq!(observe(), prepared);
        }
    }
}

#[test]
fn embedded_stream_extent_rejection_preserves_allocator_and_sessions() {
    preparation_rejects_before_allocation(false, PreparationChecks::Extents);
}

#[test]
fn installed_unix_stream_extent_rejection_preserves_allocator_and_sessions() {
    preparation_rejects_before_allocation(true, PreparationChecks::Extents);
}

#[test]
fn embedded_stream_placement_rejection_preserves_allocator_and_sessions() {
    preparation_rejects_before_allocation(false, PreparationChecks::Placement);
}

#[test]
fn installed_unix_stream_placement_rejection_preserves_allocator_and_sessions() {
    preparation_rejects_before_allocation(true, PreparationChecks::Placement);
}

#[test]
fn unix_stream_preparation_binds_returned_placement_to_captured_inputs() {
    use crate::storage_rpc::*;

    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.default_ec_shape = EcShape { k: 1, m: 0 };
    config.pg_ids = vec![0, 1];
    let mut second_route = config.pg_routes[0].clone();
    second_route.pg_id = 1;
    config.pg_routes.push(second_route);
    let topology = PgTopology::new(&config.pg_ids).unwrap();
    let bucket = crate::tests::bucket_name("prepare-response-placement");
    let key = crate::tests::object_key("key");
    let generation = GenerationId::new(7).unwrap();
    let session = StreamUploadRecord {
        session_id: crate::tests::stream_session_id("place-response"),
        bucket: bucket.clone(),
        key: key.clone(),
        target: StreamUploadTarget::PutObject,
        state: StreamUploadState::InProgress,
        created_at: 1,
        bucket_incarnation_generation: 1,
        cleanup_after: Some(100),
        encryption: metadata_only_test_encryption(),
        next_segment_vid: GenerationId::MIN,
        bucket_write_reservation: None,
    };
    let request = PrepareStreamUploadSegmentAppendReq {
        session_id: session.session_id.clone(),
        segment_index: 0,
        size: 1,
        segment_crc64: 17,
        payload_crc64: 19,
    };
    let correct = StreamUploadSegmentRecord {
        session_id: session.session_id.clone(),
        segment_index: 0,
        size: request.size,
        segment_crc64: request.segment_crc64,
        payload_crc64: request.payload_crc64,
        segment_okh: crate::stream_segment_key_hash(&session.session_id, 0),
        segment_vid: GenerationId::MIN,
        data_pg_id: topology
            .object_generation_segment_data_pg(&bucket, &key, generation, 0)
            .get(),
        placement_cluster_epoch: config.cluster_epoch,
        ec_k: 1,
        ec_m: 0,
        shard_integrity: None,
    };
    let variants = (0..4)
        .map(|case| {
            let mut segment = correct.clone();
            match case {
                0 => {}
                1 => segment.data_pg_id = 1 - segment.data_pg_id,
                2 => segment.placement_cluster_epoch = ClusterEpoch::new(2).unwrap(),
                3 => {
                    segment.ec_k = 2;
                    segment.ec_m = 1;
                }
                _ => unreachable!(),
            }
            segment
        })
        .collect::<Vec<_>>();
    private_socket_dir(config.socket_path.parent().unwrap());
    let listener = UnixListener::bind(&config.socket_path).unwrap();
    listener.set_nonblocking(true).unwrap();
    let returned_session = session.clone();
    let server = thread::spawn(move || {
        for segment in variants {
            for (kind, payload) in [
                (
                    StorageRpcMessageKind::ObjectStreamUploadSessionLoad,
                    encode_stream_upload_session_response(&StorageRpcStreamUploadSessionResponse {
                        outcome: StorageRpcStreamUploadSessionOutcome::Loaded(Box::new(
                            returned_session.clone(),
                        )),
                    }),
                ),
                (
                    StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare,
                    encode_stream_segment_append_prepare_response(
                        &StorageRpcStreamSegmentAppendPrepareResponse {
                            outcome: StorageRpcStreamSegmentAppendPrepareOutcome::Prepared {
                                target: StreamUploadTarget::PutObject,
                                segment: Box::new(segment.clone()),
                            },
                        },
                    ),
                ),
            ] {
                let deadline = Instant::now() + Duration::from_secs(5);
                let mut connection = loop {
                    match listener.accept() {
                        Ok((connection, _)) => break connection,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            assert!(
                                Instant::now() < deadline,
                                "missing preparation RPC {kind:?}"
                            );
                            thread::sleep(Duration::from_millis(1));
                        }
                        Err(error) => panic!("accept preparation RPC: {error}"),
                    }
                };
                connection
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                connection
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let request = read_storage_rpc_frame_from(&mut connection).unwrap();
                assert_eq!(request.kind, kind);
                write_storage_rpc_frame_to(
                    &mut connection,
                    &StorageRpcFrame {
                        request_id: request.request_id,
                        kind,
                        payload: encode_storage_rpc_success_response(&payload),
                    },
                )
                .unwrap();
            }
        }
    });
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    )
    .with_pg_topology(Arc::new(topology.clone()));
    let route = client
        .open_stream_upload_session_metadata_route(
            config.cluster_epoch,
            ObjectMetadataPgId::new_for_test(PgId::new(topology.object_pg_for(&bucket, &key))),
            &bucket,
            &key,
            &session.session_id,
        )
        .unwrap();
    let expected = crate::ExpectedStreamAppendState::PutObject {
        session,
        generation_id: generation,
    };
    for case in 0..4 {
        let result = route.prepare_segment_append_with_expected_append_state(
            &request,
            Some(&expected),
            AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            PayloadPlacementInput::from_server_config(&config),
        );
        if case == 0 {
            assert_eq!(result.unwrap().1, correct);
        } else {
            let error = result.unwrap_err();
            assert!(
                matches!(&error, ObjectPgActionError::Store(StoreError::StorageRpc {
                failure: StorageRpcErrorCode::PayloadDecode, detail, ..
            }) if detail.as_str().contains("placement does not match captured route and expected generation")),
                "case {case}: {error:?}"
            );
        }
    }
    server.join().unwrap();
}
