// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn embedded_stream_preparation_rechecks_captured_deadline_inside_sql_write_transaction() {
    preparation_deadline_matrix(false);
}

#[test]
fn unix_stream_preparation_rechecks_captured_deadline_inside_sql_write_transaction() {
    preparation_deadline_matrix(true);
}

fn preparation_deadline_matrix(run_remote: bool) {
    for (remote, server_first, absent_client) in [
        (false, false, false),
        (true, false, false),
        (true, true, false),
        (true, true, true),
    ]
    .into_iter()
    .filter(|(remote, _, _)| *remote == run_remote)
    {
        for part in [false, true] {
            let time = crate::clock::test_time_override_guard(1_000);
            let tmp = test_util::tempdir();
            let mut config = test_config(&tmp);
            config.default_ec_shape = EcShape { k: 1, m: 0 };
            config.route_map_validity =
                RouteMapValidity::until_ms(if server_first { 5_000 } else { 10_000 }).unwrap();
            private_socket_dir(config.socket_path.parent().unwrap());
            let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
            let node = server.test_storage_node();
            let fixture = CreationFixture::new(&node, part);
            fixture.seed_session(&node, &config);
            let expected = {
                let pg = node.get_pg(0).unwrap();
                let session = pg.get_stream_upload(&fixture.request.session_id).unwrap();
                let generation = pg
                    .get_object_generation_reservation(
                        &fixture.request.bucket,
                        &fixture.request.key,
                        &fixture.request.session_id,
                    )
                    .unwrap();
                if part {
                    crate::ExpectedStreamAppendState::UploadPart {
                        session,
                        upload: Box::new(fixture.upload.clone()),
                        part_generation_id: generation,
                    }
                } else {
                    crate::ExpectedStreamAppendState::PutObject {
                        session,
                        generation_id: generation,
                    }
                }
            };
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
                    pg.list_all_stream_uploads().unwrap(),
                    pg.list_stream_segments(&fixture.request.session_id)
                        .unwrap(),
                )
            };
            let before = observe();
            let fence = if absent_client {
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch)
            } else {
                AdmittedRouteEffectFence::bounded(
                    config.cluster_epoch,
                    if server_first { 10_000 } else { 5_000 },
                    if server_first { 9_000 } else { 4_000 },
                )
            };
            let mut renewed = config.clone();
            renewed.route_map_validity = RouteMapValidity::until_ms(20_000).unwrap();
            let hook_ran = Arc::new(AtomicBool::new(false));
            let serving = if remote {
                let server = Arc::clone(&server);
                let hook_server = Arc::clone(&server);
                let renewed = renewed.clone();
                let ran = Arc::clone(&hook_ran);
                Some(thread::spawn(move || {
                    let time = crate::clock::test_time_override_guard(1_000);
                    // The scoped Unix client loads the target session before
                    // sending each preparation request. Do not spend the
                    // one-shot allocation hook on that preliminary frame.
                    server.accept_one().unwrap();
                    let control = time.control();
                    PgStore::test_with_before_stream_segment_allocation_effect_hook(
                        move |in_write_transaction| {
                            assert!(
                                in_write_transaction,
                                "SQLite write admission must precede the final check"
                            );
                            // Validity-only renewal does not acquire the PG lock or
                            // extend the request/receiving frame's captured fence.
                            hook_server
                                .install_control_plane_runtime_config(renewed)
                                .unwrap();
                            control.set(4_500);
                            ran.store(true, Ordering::SeqCst);
                        },
                        || server.accept_one().unwrap(),
                    );
                    server.accept_one().unwrap();
                    server.accept_one().unwrap();
                }))
            } else {
                None
            };
            let unix = UnixStorageNodeClient::new(
                config.node_id,
                config.cluster_epoch,
                config.socket_path.clone(),
            );
            let local = LocalStorageNodeClient::new(config.node_id, Arc::clone(&node));
            let client: &dyn ObjectMutationMetadataNodeClient = if remote { &unix } else { &local };
            let route = client
                .open_stream_upload_session_metadata_route(
                    config.cluster_epoch,
                    ObjectMetadataPgId::new_for_test(PgId::new(0)),
                    &fixture.request.bucket,
                    &fixture.request.key,
                    &fixture.request.session_id,
                )
                .unwrap();
            let prepare = |fence| {
                route.prepare_segment_append_with_expected_append_state(
                    &PrepareStreamUploadSegmentAppendReq {
                        session_id: fixture.request.session_id.clone(),
                        segment_index: 0,
                        size: 1,
                        segment_crc64: 0,
                        payload_crc64: 0,
                    },
                    Some(&expected),
                    fence,
                    PayloadPlacementInput::from_server_config(&config),
                )
            };
            let error = if remote {
                prepare(fence).expect_err(&format!(
                    "remote part={part} server_first={server_first} absent_client={absent_client}"
                ))
            } else {
                let control = time.control();
                let ran = Arc::clone(&hook_ran);
                let hook_server = Arc::clone(&server);
                PgStore::test_with_before_stream_segment_allocation_effect_hook(
                    move |in_write_transaction| {
                        assert!(
                            in_write_transaction,
                            "SQLite write admission must precede the final check"
                        );
                        hook_server
                            .install_control_plane_runtime_config(renewed)
                            .unwrap();
                        control.set(4_500);
                        ran.store(true, Ordering::SeqCst);
                    },
                    || prepare(fence),
                )
                .unwrap_err()
            };
            assert!(
                hook_ran.load(Ordering::SeqCst),
                "preparation must reach the transactional effect check"
            );
            assert!(
                if remote {
                    matches!(&error, ObjectPgActionError::Store(StoreError::StorageRpc {
                        failure: StorageRpcErrorCode::StaleShardLocation, ..
                    }))
                } else {
                    matches!(&error, ObjectPgActionError::Store(StoreError::RouteMapExpired {
                        valid_until_ms: 5_000, now_ms: 4_500, ..
                    }))
                },
                "remote={remote} part={part} server_first={server_first} absent_client={absent_client}: {error:?}"
            );
            assert_eq!(
                observe(),
                before,
                "expired preparation changed durable state"
            );
            // Same request and unchanged slot, with genuinely fresh admission.
            let (_, segment) = prepare(AdmittedRouteEffectFence::bounded(
                config.cluster_epoch,
                20_000,
                19_000,
            ))
            .unwrap();
            assert_eq!(segment.segment_vid, GenerationId::MIN);
            if let Some(serving) = serving {
                serving.join().unwrap();
            }
            assert_eq!(
                node.get_pg(0)
                    .unwrap()
                    .get_stream_upload(&fixture.request.session_id)
                    .unwrap()
                    .next_segment_vid
                    .get(),
                2
            );
        }
    }
}
