// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Actual bucket reservation custody around an installed Unix/TLS introduction.
use super::*;
use crate::node_runtime::server::StorageNodeServer;
use crate::StoredObject;

const INSTALL: Kind = Kind::StagingDirectIntroductionInstall;

fn serve(primary: &fanout_mixed::Remote) -> application_sender::RunningReplica {
    // Bucket RPCs use the ordinary connection pool, independently of the held
    // primary session. Serve both concurrently as production does; accept_one
    // can strand the release behind an idle pooled TLS connection.
    application_sender::RunningReplica::start_concurrent(Arc::clone(&primary.server), primary.tls)
}

#[test]
fn staging_direct_request_encryption_unix_tls_uses_original_introduction_custody_and_dispatch() {
    encrypted_request(false);
}

#[test]
fn staging_encrypted_request_unix_tls_publishes_readable_payload_after_uncertain_installation() {
    encrypted_request(true);
}

fn encrypted_request(complete: bool) {
    for tls in [false, true] {
        let (
            Setup {
                _dir,
                map,
                handle,
                context,
            },
            owner,
            proof,
        ) = inputs();
        let f =
            fanout_mixed::Mixed::from_settled_fixture(map, handle, context, [(0, tls), (4, tls)]);
        prepare_bucket(&f, &proof);
        let before = reservations(&f, &proof.bucket);
        let primary = f.remote(NodeId::new(4)).unwrap();
        let _running: Vec<_> = f.remote.iter().map(serve).collect();
        for peer in &f.remote {
            peer.server.test_install_window_receiver_peers(
                f.remote
                    .iter()
                    .map(|p| (p.node, p.client.test_execution_endpoint().clone()))
                    .collect(),
                super::super::super::super::super::complete_log_rpc::window_receiver_auth_for(
                    f.context.id().cluster_epoch(),
                    peer.node.as_u32(),
                ),
            );
        }
        let admission = f.handle.admit_current_route().unwrap();
        let put = admission
            .active_put_object_route(&proof.bucket, &owner.subject.key)
            .unwrap();
        let result = put.with_direct_put_scope(Default::default(), |_, scope| {
            let route = admission
                .complete_fanout_route(f.context.id().pg_id(), f.context.id().cluster_epoch())
                .unwrap();
            let session = primary
                .client
                .open_complete_fanout_primary(&route, deadline())
                .unwrap();
            let mut introduction = route
                .prepare_request_direct_introduction_remote(
                    scope,
                    &primary.client,
                    &session,
                    &owner.subject.id.attempt,
                    deadline(),
                )
                .unwrap();
            introduction.install().unwrap();
            let mut publication = introduction
                .into_publication()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            publication.publish().unwrap();
            publication
                .apply_remote(&f.remote(NodeId::new(0)).unwrap().client)
                .unwrap();
            publication.apply_remote(&primary.client).unwrap();
            for id in [2, 3, 1] {
                publication.apply_embedded_replica(NodeId::new(id)).unwrap();
            }
            let mut terminal = publication
                .into_terminal()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            terminal.retire().unwrap();
            let window = terminal
                .into_confirmed_window()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let plaintext = b"request encrypted under original remote introduction";
            let mut fixture = crate::tests::sse_s3_segment_test_fixture(plaintext, 0);
            let writer = window
                .into_file_dispatch(&fixture.encryption)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            primary
                .server
                .test_require_window_primary_released(f.context.id().pg_id());
            let before_binding = f.observe();
            let prepared = writer
                .prepare_encryption(plaintext.len() as u64)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            fixture.encrypt_for_subject(prepared.encryption_subject(), plaintext);
            let wrap_context = prepared.object_encryption_subject().dek_wrap_subject().aead_context().to_vec();
            let committed =
                fixture.committed_encryption_evidence(prepared.object_encryption_subject());
            let input = crate::StreamPutCommitInput {
                versioning: crate::BucketVersioningState::Disabled,
                version_id: crate::VersionId::Null,
                owner: crate::OwnerIdentity::from_principal("encrypted-request-owner"),
                acl_grants: Default::default(),
                public_read: false,
                etag_crc64: fixture.payload_crc64,
                tags: None,
                metadata_blob: Default::default(),
                system_metadata_blob: Default::default(),
                object_lock: Default::default(),
                encryption: committed.encryption().clone(),
            };
            let mut encrypted = prepared
                .bind_ciphertext(fixture.storage_bytes.clone(), committed)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!(encrypted.test_observe().0, f.context.id());
            assert_eq!(encrypted.test_observe().3, fixture.storage_crc64);
            assert_eq!(f.observe(), before_binding);
            let file_peer = f.remote(NodeId::new(0)).unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let counter = Arc::clone(&calls);
            file_peer
                .server
                .set_response_frame_test_hook(Arc::new(move |kind, _| {
                    if kind == Kind::StagingShardWrite
                        && counter.fetch_add(1, Ordering::SeqCst) == 0
                    {
                        StorageNodeServer::test_lose_staging_file_reply();
                    }
                }));
            let node = f.node(NodeId::new(4));
            let owner = {
                let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                let snapshot = pg.command_build_snapshot().unwrap();
                let owner = append::load(&pg, &fresh_capture::template(StagingTarget::DirectPut));
                snapshot.finish().unwrap();
                owner
            };
            let window = &owner.windows[0];
            assert_eq!(window.participants.files[0], file_peer.node);
            let dispatch: Vec<_> = window
                .participants
                .files
                .iter()
                .map(|node| f.remote(*node).map(|peer| peer.client.as_ref()))
                .collect();
            let mut crossed = dispatch.clone();
            assert_ne!(window.participants.files[1], file_peer.node);
            crossed[1] = Some(file_peer.client.as_ref());
            assert!(encrypted.write_files(&crossed).is_err());
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            let lost = encrypted.write_files(&dispatch).unwrap_err();
            assert_eq!(
                calls.load(Ordering::SeqCst),
                1,
                "expected lost file reply, got {lost}"
            );
            encrypted.write_files(&dispatch).unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 2);
            encrypted.write_files(&dispatch).unwrap();
            assert_eq!(
                calls.load(Ordering::SeqCst),
                2,
                "confirmed file retry sends nothing"
            );
            let mut systematic = Vec::new();
            for (index, node_id) in window
                .participants
                .files
                .iter()
                .enumerate()
                .take(usize::from(window.geometry.ec.k))
            {
                let key =
                    crate::ShardKey::new(&window.segment_hash, window.start.get(), index as u8);
                systematic.extend(
                    f.node(*node_id)
                        .read_shard_file(window.participants.data_pg.get(), &key)
                        .unwrap(),
                );
            }
            assert_eq!(
                &systematic[..fixture.storage_bytes.len()],
                fixture.storage_bytes
            );
            encrypted
                .persist_acknowledgements(Some(&primary.client))
                .unwrap();
            let before_drop = f.observe();
            let mut ready = encrypted
                .into_acknowledged()
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let bounds = ready.test_terminal_bounds();
            assert!(bounds.1 < owner.maximum_cutoff_ms);
            let held = ready.open_primary_session(&primary.client).unwrap();
            let mut inspected = ready
                .inspect_remote(&primary.client, &held)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            assert!(inspected.auth_snapshot().existing_etag.is_none());
            let evaluations = std::cell::Cell::new(0);
            let failure = crate::clock::with_time_override(bounds.1, || {
                inspected.prepare_terminal_remote_if(&primary.client, &held, &input, |_| {
                    evaluations.set(evaluations.get() + 1);
                    Ok::<(), &'static str>(())
                })
            })
            .err()
            .expect("expired request decision");
            assert!(failure.storage_error().is_some());
            assert_eq!(evaluations.get(), 0);
            inspected = failure.into_retry();
            let failure = inspected
                .prepare_terminal_remote_if(&primary.client, &held, &input, |snapshot| {
                    evaluations.set(evaluations.get() + 1);
                    assert!(snapshot.existing_etag.is_none());
                    Err("original conditional error")
                })
                .err()
                .expect("condition rejected");
            assert_eq!(failure.request_error(), Some(&"original conditional error"));
            assert!(failure.storage_error().is_none());
            assert_eq!(f.observe(), before_drop);
            inspected = failure.into_retry();
            let mut crossed = input.clone();
            crossed.encryption = fixture.encryption.clone();
            let failure = inspected
                .prepare_terminal_remote(&primary.client, &held, &crossed)
                .err()
                .expect("crossed terminal encryption");
            assert!(failure
                .error()
                .to_string()
                .contains("committed encryption subject"));
            inspected = failure.into_writer();
            inspected.test_cross_snapshot();
            let failure = inspected
                .prepare_terminal_remote_if(&primary.client, &held, &input, |snapshot| {
                    evaluations.set(evaluations.get() + 1);
                    assert_eq!(snapshot.existing_etag.as_deref(), Some("crossed snapshot"));
                    Ok::<(), &'static str>(())
                })
                .err()
                .expect("crossed terminal snapshot");
            assert!(failure.storage_error().is_some());
            assert!(failure.request_error().is_none());
            let inspected = failure
                .into_retry()
                .reinspect_remote(&primary.client, &held)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            assert_eq!(inspected.test_terminal_bounds(), bounds);
            assert!(inspected.auth_snapshot().existing_etag.is_none());
            let terminal = inspected
                .prepare_terminal_remote_if(&primary.client, &held, &input, |snapshot| {
                    evaluations.set(evaluations.get() + 1);
                    assert!(snapshot.existing_etag.is_none());
                    Ok::<(), &'static str>(())
                })
                .unwrap_or_else(|e| panic!("{:?}", e.error()));
            assert_eq!(evaluations.get(), 3);
            assert_eq!(terminal.test_binding_state().0, bounds.0);
            let command = terminal.test_command();
            let crate::metadata_command::MetadataCommandPayload::CommitDirectPutObject(commit) =
                command.payload()
            else {
                panic!("direct terminal")
            };
            assert_eq!(commit.object.encryption, input.encryption);
            assert_eq!(commit.object.size, plaintext.len() as u64);
            assert_eq!(
                commit.object.generation_id,
                owner.subject.payload_generation
            );
            assert_eq!(commit.segments[0].segment_crc64, fixture.storage_crc64);
            assert!(commit.segments[0].shard_integrity.is_some());
            if complete {
                let mut submission = terminal.into_submission();
                let attempts = submission.test_attempts();
                let before_attempt = f.observe();
                crate::clock::with_time_override(bounds.1, || submission.install_remote(&primary.client, &held))
                    .expect_err("expired preflight cannot install");
                assert_eq!(submission.installation(), crate::node_runtime::pg_store::DirectTerminalInstallationObservation::NoAcceptanceObserved);
                assert_eq!(f.observe(), before_attempt);
                assert_eq!(submission.test_attempts(), attempts, "expired preflight does not start an attempt");
                let ready = submission.into_reinspection().unwrap_or_else(|e| panic!("{}", e.error()));
                assert_eq!(ready.test_attempts(), attempts);
                assert_eq!(ready.test_terminal_bounds(), bounds);
                // Model the cold prefix left by a definitive SQLite rollback on
                // the receiver, not just a sender-side preflight rejection.
                let writes = {
                    let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                    pg.conn.execute_batch("BEGIN IMMEDIATE; UPDATE metadata_command_replica_state SET applied_log_index = applied_log_index; ROLLBACK").unwrap();
                    pg.conn.total_changes()
                };
                let before_read = f.observe();
                assert_eq!(before_read.iter().map(|(state, _)| state).collect::<Vec<_>>(), before_attempt.iter().map(|(state, _)| state).collect::<Vec<_>>());
                let inspected = ready.inspect_remote(&primary.client, &held).unwrap_or_else(|e| panic!("{}", e.error()));
                assert_eq!(inspected.test_attempts(), attempts + 1);
                assert_eq!(node.get_pg(f.context.id().pg_id().get()).unwrap().conn.total_changes(), writes, "remote prefix validation wrote SQL");
                assert_eq!(f.observe(), before_read);
                let terminal = inspected.prepare_terminal_remote_if(&primary.client, &held, &input, |snapshot| {
                    evaluations.set(evaluations.get() + 1);
                    assert!(snapshot.existing_etag.is_none());
                    Ok::<(), &'static str>(())
                }).unwrap_or_else(|e| panic!("{:?}", e.error()));
                assert_eq!(evaluations.get(), 4);
                let command = terminal.test_command();
                let mut submission = terminal.into_submission();
                let installs = Arc::new(AtomicUsize::new(0));
                let counter = Arc::clone(&installs);
                let markers = Arc::new(AtomicUsize::new(0));
                let marker_counter = Arc::clone(&markers);
                primary.server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
                    if (kind == Kind::StagingDirectInstall && counter.fetch_add(1, Ordering::SeqCst) == 0)
                        || (kind == Kind::MetadataCommandPublicationStart && marker_counter.fetch_add(1, Ordering::SeqCst) == 0) {
                        // The slot committed, but the authenticated reply reports failure.
                        // Keep this exact connection usable for the owning retry.
                        frame.payload = rpc::encode_storage_rpc_error_response(&rpc::StorageRpcErrorResponse {
                            code: rpc::StorageRpcErrorCode::ResourceExhausted,
                            message: "injected after encrypted terminal installation".into(),
                        }).unwrap();
                        rpc::set_storage_rpc_response_connection_reusable(&mut frame.payload, true).unwrap();
                    }
                }));
                assert!(submission.install_remote(&primary.client, &held).is_err());
                assert_eq!(submission.test_attempts(), attempts + 2);
                assert_eq!(submission.installation(), crate::node_runtime::pg_store::DirectTerminalInstallationObservation::Unconfirmed);
                let installed = f.observe();
                crate::clock::with_time_override(bounds.1, || submission.install_remote(&primary.client, &held))
                    .expect_err("later preflight rejection cannot erase earlier uncertainty");
                let failure = submission.into_reinspection().err().expect("uncertain installation cannot discard its command");
                let submission = failure.into_submission();
                assert_eq!(submission.test_attempts(), attempts + 2, "uncertainty retains the shared accounting");
                assert_eq!(submission.id(), command.id());
                assert_eq!(f.observe(), installed);
                assert_eq!(submission.installation(), crate::node_runtime::pg_store::DirectTerminalInstallationObservation::Unconfirmed);
                let failure = submission.into_publication().err().expect("uncertain installation cannot publish");
                let mut submission = failure.into_submission();
                submission.install_remote(&primary.client, &held).unwrap();
                assert_eq!(submission.test_attempts(), attempts + 3);
                assert_eq!(installs.load(Ordering::SeqCst), 2);
                assert_eq!(f.observe(), installed, "exact retry must not allocate or apply another command");
                let failure = submission.into_reinspection().err().expect("installed command cannot be discarded");
                let submission = failure.into_submission();
                let mut publication = submission.into_publication().unwrap_or_else(|e| panic!("{}", e.error()));
                assert_eq!(publication.test_attempts(), attempts + 3);
                assert_eq!(publication.test_cutoff().0, bounds.0);
                assert!(publication.test_cutoff().1 <= bounds.1);
                publication.advance_remote(&primary.client, &held, Some(&file_peer.client)).expect_err("marker step must not accept a replica target");
                assert_eq!(publication.test_attempts(), attempts + 3);
                assert_eq!(markers.load(Ordering::SeqCst), 0);
                publication.advance_remote(&primary.client, &held, None).expect_err("marker applied but reply is uncertain");
                assert_eq!(publication.publication(), crate::node_runtime::pg_store::PublicationObservation::Unconfirmed);
                assert_eq!(publication.test_attempts(), attempts + 4);
                let marked = f.observe();
                crate::clock::with_time_override(bounds.1, || publication.advance_remote(&primary.client, &held, None))
                    .expect_err("expired work must preserve uncertain marker history");
                assert_eq!(markers.load(Ordering::SeqCst), 1);
                assert_eq!(publication.test_attempts(), attempts + 4);
                assert_eq!(publication.publication(), crate::node_runtime::pg_store::PublicationObservation::Unconfirmed);
                assert_eq!(f.observe(), marked);
                let progress = publication.advance_remote(&primary.client, &held, None).unwrap();
                assert!(!progress.is_ready());
                assert_eq!(progress.applied_node(), None);
                assert_eq!(markers.load(Ordering::SeqCst), 2);
                assert_eq!(publication.test_attempts(), attempts + 5);
                let before_wrong = f.observe();
                publication.advance_remote(&primary.client, &held, Some(&primary.client)).expect_err("primary cannot replace the next witness");
                assert_eq!(publication.test_attempts(), attempts + 5);
                assert_eq!(f.observe(), before_wrong);
                let applications = Arc::new(AtomicUsize::new(0));
                let apply_counter = Arc::clone(&applications);
                file_peer.server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
                    if kind == Kind::MetadataCommandApplyAndRecord && apply_counter.fetch_add(1, Ordering::SeqCst) == 0 {
                        frame.payload = rpc::encode_storage_rpc_error_response(&rpc::StorageRpcErrorResponse {
                            code: rpc::StorageRpcErrorCode::ResourceExhausted,
                            message: "injected after progress witness application".into(),
                        }).unwrap();
                        rpc::set_storage_rpc_response_connection_reusable(&mut frame.payload, true).unwrap();
                    }
                }));
                publication.advance_remote(&primary.client, &held, Some(&file_peer.client)).expect_err("witness applied but reply is uncertain");
                assert_eq!(publication.application(NodeId::new(0)), crate::node_runtime::pg_store::ApplicationObservation::Unconfirmed);
                assert_eq!(publication.test_attempts(), attempts + 6);
                let uncertain = f.observe();
                publication.advance_remote(&primary.client, &held, Some(&primary.client)).expect_err("uncertain witness cannot be skipped");
                assert_eq!(publication.test_attempts(), attempts + 6);
                assert_eq!(f.observe(), uncertain);
                let progress = publication.advance_remote(&primary.client, &held, Some(&file_peer.client)).unwrap();
                assert_eq!(progress.applied_node(), Some(NodeId::new(0)));
                assert_eq!(applications.load(Ordering::SeqCst), 2);
                assert_eq!(f.observe(), uncertain, "exact witness retry must not mutate again");
                let failure = publication.into_remote_terminal().err().expect("partial apply cannot release custody");
                let mut publication = failure.into_publication();
                assert_eq!(publication.test_attempts(), attempts + 7);
                assert_eq!(publication.advance_remote(&primary.client, &held, Some(&primary.client)).unwrap().applied_node(), Some(NodeId::new(4)));
                for id in [1, 2, 3] {
                    let before = publication.test_attempts();
                    assert_eq!(publication.advance_remote(&primary.client, &held, None).unwrap().applied_node(), Some(NodeId::new(id)));
                    assert_eq!(publication.test_attempts(), before + 1);
                }
                let applied = f.observe();
                for _ in 0..2 { assert!(publication.advance_remote(&primary.client, &held, None).unwrap().is_ready()); }
                assert_eq!(publication.test_attempts(), attempts + 11);
                assert_eq!(f.observe(), applied);
                crate::clock::with_time_override(bounds.1, || publication.advance_remote(&primary.client, &held, None)).expect_err("ready is still deadline-bound");
                let mut terminal = publication.into_remote_terminal().unwrap_or_else(|e| panic!("{}", e.error()));
                assert_eq!(terminal.test_attempts(), attempts + 11);
                assert_eq!(terminal.test_cutoff().0, bounds.0);
                assert!(terminal.test_cutoff().1 <= bounds.1);
                terminal.release_reservation(&primary.client, &held).unwrap();
                assert_eq!(terminal.test_attempts(), attempts + 12);
                terminal.retire(&primary.client, &held).unwrap();
                assert_eq!(terminal.test_attempts(), attempts + 13);
                assert_eq!(terminal.retirement(), crate::node_runtime::pg_store::TerminalSlotRetirementObservation::Retired);
                // Read physical bytes again after all terminal effects, not just
                // the pre-publication buffer retained above.
                systematic.clear();
                for (index, node_id) in window.participants.files.iter().enumerate().take(usize::from(window.geometry.ec.k)) {
                    let key = crate::ShardKey::new(&window.segment_hash, window.start.get(), index as u8);
                    systematic.extend(f.node(*node_id).read_shard_file(window.participants.data_pg.get(), &key).unwrap());
                }
                systematic.truncate(fixture.storage_bytes.len());
                assert_eq!(systematic, fixture.storage_bytes);
                for id in 0..5 {
                    let node = f.node(NodeId::new(id));
                    let pg = node.get_pg(owner.subject.object_pg.get()).unwrap();
                    object::assert_object(&pg, &command);
                    let StoredObject::Live(row) = pg.get_object_version(&owner.subject.bucket, &owner.subject.key, crate::VersionId::Null).unwrap() else { panic!("live object") };
                    assert_eq!(fixture.open_committed_payload(&row.encryption, &wrap_context, &systematic), plaintext);
                    let snapshot = pg.command_build_snapshot().unwrap();
                    assert_eq!(append::load(&pg, &owner), object::expected(&owner, &command));
                    snapshot.finish().unwrap();
                    if id == 4 { assert!(complete_pending::observe(&pg).iter().all(Vec::is_empty)); }
                }
                let retired = f.observe();
                drop(terminal);
                assert_eq!(f.observe(), retired);
            } else {
                let mut submission = terminal.into_submission();
                let attempts = submission.test_attempts();
                submission.test_limit_attempts(attempts);
                let installs = Arc::new(AtomicUsize::new(0));
                let counter = Arc::clone(&installs);
                primary.server.set_response_frame_test_hook(Arc::new(move |kind, _| {
                    if kind == Kind::StagingDirectInstall {
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                }));
                let error = submission.install_remote(&primary.client, &held).unwrap_err();
                assert!(error.to_string().contains("encrypted direct terminal attempt budget"), "{error}");
                assert_eq!(submission.test_attempts(), attempts);
                assert_eq!(submission.installation(), crate::node_runtime::pg_store::DirectTerminalInstallationObservation::NoAcceptanceObserved);
                assert_eq!(installs.load(Ordering::SeqCst), 0, "exhaustion must reject before transport");
                drop(submission);
                assert_eq!(f.observe(), before_drop);
            }
            if complete { Ok(()) } else { Err("request ended after terminal preparation") }
        })
        .unwrap();
        assert_eq!(result.is_ok(), complete);
        let retained = reservations(&f, &proof.bucket);
        assert_eq!(retained.len(), before.len() + usize::from(!complete));
        for record in before {
            assert!(retained.contains(&record));
        }
    }
}

fn prepare_bucket(f: &fanout_mixed::Mixed, proof: &BucketWriteReservationProof) {
    for id in 0..5 {
        let node = f.node(NodeId::new(id));
        let pg = node
            .get_pg(topology().bucket_pg_for(&proof.bucket))
            .unwrap();
        terminal_release::seed_pg_reservations(&pg, proof, None);
    }
}

fn reservations(
    f: &fanout_mixed::Mixed,
    bucket: &BucketName,
) -> Vec<crate::BucketWriteReservationRecord> {
    f.node(NodeId::new(4))
        .get_pg(topology().bucket_pg_for(bucket))
        .unwrap()
        .durable_bucket_write_reservations(bucket)
        .unwrap()
}

#[test]
fn staging_direct_introduction_request_remote_rejection_and_preparation_unwind_release() {
    for tls in [false, true] {
        for case in [
            "capture",
            "not-sent",
            "expired",
            "unwind",
            "bad-preparation",
        ] {
            let (f, owner, proof, _dir) = fixture(tls);
            prepare_bucket(&f, &proof);
            let before = reservations(&f, &proof.bucket);
            let primary = f.remote(NodeId::new(4)).unwrap();
            let _running = serve(primary);
            let admission = f.handle.admit_current_route().unwrap();
            let put = admission
                .active_put_object_route(&proof.bucket, &owner.subject.key)
                .unwrap();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                put.with_direct_put_scope(Default::default(), |_, scope| {
                    let route = admission
                        .complete_fanout_route(
                            f.context.id().pg_id(),
                            f.context.id().cluster_epoch(),
                        )
                        .unwrap();
                    let session = primary
                        .client
                        .open_complete_fanout_primary(&route, deadline())
                        .unwrap();
                    if case == "bad-preparation" {
                        primary
                            .server
                            .set_response_frame_test_hook(Arc::new(|kind, frame| {
                                if kind == KIND {
                                    frame.payload = rpc::encode_storage_rpc_success_response(&[]);
                                    rpc::set_storage_rpc_response_connection_reusable(
                                        &mut frame.payload,
                                        true,
                                    )
                                    .unwrap();
                                }
                            }));
                        assert!(route
                            .prepare_request_direct_introduction_remote(
                                scope,
                                &primary.client,
                                &session,
                                &owner.subject.id.attempt,
                                deadline()
                            )
                            .is_err());
                        let retry = route
                            .prepare_request_direct_introduction_remote(
                                scope,
                                &primary.client,
                                &session,
                                &owner.subject.id.attempt,
                                deadline(),
                            )
                            .err()
                            .expect("malformed preparation closes original session");
                        assert!(
                            retry.to_string().contains("exact open primary session"),
                            "{retry}"
                        );
                        return Err::<(), _>("request stopped");
                    }
                    let mut introduction = route
                        .prepare_request_direct_introduction_remote(
                            scope,
                            &primary.client,
                            &session,
                            &owner.subject.id.attempt,
                            deadline(),
                        )
                        .unwrap();
                    assert_eq!(introduction.id(), f.context.id());
                    assert!(scope.reserve_generation().is_err());
                    assert!(route
                        .prepare_request_direct_introduction_remote(
                            scope,
                            &primary.client,
                            &session,
                            &owner.subject.id.attempt,
                            deadline()
                        )
                        .is_err());
                    match case {
                        "not-sent" => {
                            session.test_introduction_install_before_dispatch(false);
                            assert!(introduction.install().is_err());
                        }
                        "expired" => crate::clock::with_time_override(
                            crate::clock::current_time_millis() + 1_000_000,
                            || {
                                assert!(introduction.install().is_err());
                            },
                        ),
                        "unwind" => panic!("frontend stopped before installation"),
                        "capture" => (),
                        _ => unreachable!(),
                    }
                    assert_eq!(introduction.installation(), Installed::NoAcceptanceObserved);
                    let failure = introduction
                        .into_publication()
                        .err()
                        .expect("no installation");
                    assert!(failure
                        .error()
                        .to_string()
                        .contains("confirmed installation"));
                    drop(failure.into_introduction());
                    Err::<(), _>("request stopped")
                })
                .unwrap()
            }));
            assert_eq!(result.is_err(), case == "unwind");
            assert_eq!(reservations(&f, &proof.bucket), before, "{case}, tls={tls}");
            assert!(f
                .node(NodeId::new(4))
                .get_pg(f.context.id().pg_id().get())
                .unwrap()
                .pending_staging_protection(&topology())
                .unwrap()
                .is_none());
        }
    }
}

#[test]
fn staging_direct_introduction_request_remote_uncertainty_retains_proof_across_return_and_retry() {
    for tls in [false, true] {
        for case in ["lost", "crossed", "retry", "unwind", "installation-unwind"] {
            let (f, owner, proof, _dir) = fixture(tls);
            prepare_bucket(&f, &proof);
            let before = reservations(&f, &proof.bucket);
            let primary = f.remote(NodeId::new(4)).unwrap();
            let _running = serve(primary);
            let admission = f.handle.admit_current_route().unwrap();
            let put = admission
                .active_put_object_route(&proof.bucket, &owner.subject.key)
                .unwrap();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                put.with_direct_put_scope(Default::default(), |_, scope| {
                    let route = admission
                        .complete_fanout_route(
                            f.context.id().pg_id(),
                            f.context.id().cluster_epoch(),
                        )
                        .unwrap();
                    let session = primary
                        .client
                        .open_complete_fanout_primary(&route, deadline())
                        .unwrap();
                    let mut introduction = route
                        .prepare_request_direct_introduction_remote(
                            scope,
                            &primary.client,
                            &session,
                            &owner.subject.id.attempt,
                            deadline(),
                        )
                        .unwrap();
                    let bounds = introduction.test_bounds();
                    if case == "installation-unwind" {
                        session.test_introduction_install_before_dispatch(true);
                        let _ = introduction.install();
                        unreachable!("installation hook must unwind through request scope");
                    }
                    primary
                        .server
                        .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                            if kind == INSTALL {
                                if case == "lost" {
                                    StorageNodeServer::test_lose_direct_introduction_reply();
                                } else {
                                    let mut body =
                                        rpc::decode_storage_rpc_response_payload(&frame.payload)
                                            .unwrap()
                                            .unwrap();
                                    body[6] ^= 1;
                                    frame.payload = rpc::encode_storage_rpc_success_response(&body);
                                    rpc::set_storage_rpc_response_connection_reusable(
                                        &mut frame.payload,
                                        true,
                                    )
                                    .unwrap();
                                }
                            }
                        }));
                    assert!(introduction.install().is_err());
                    assert_eq!(introduction.installation(), Installed::Unconfirmed);
                    let failure = introduction
                        .into_publication()
                        .err()
                        .expect("uncertain owner cannot escape");
                    let mut introduction = failure.into_introduction();
                    let installed = f.observe();
                    if case == "unwind" {
                        panic!("frontend stopped after uncertain installation");
                    }
                    if case == "lost" {
                        session.test_close_primary_section();
                        let _replacement = primary
                            .client
                            .open_complete_fanout_primary(&route, deadline())
                            .unwrap();
                        assert!(introduction.install().is_err());
                        assert_eq!(introduction.installation(), Installed::Unconfirmed);
                    } else {
                        session.test_introduction_install_before_dispatch(false);
                        assert!(introduction.install().is_err());
                        assert_eq!(introduction.installation(), Installed::Unconfirmed);
                        if case == "retry" {
                            primary
                                .server
                                .set_response_frame_test_hook(Arc::new(|_, _| {}));
                            introduction.install().unwrap();
                            assert_eq!(introduction.installation(), Installed::Installed);
                            assert_eq!(introduction.test_bounds(), bounds);
                            assert_eq!(f.observe(), installed, "exact retry is inert");
                            let publication = introduction
                                .into_publication()
                                .unwrap_or_else(|e| panic!("{}", e.error()));
                            assert_eq!(publication.test_bounds(), bounds);
                        }
                    }
                    Err::<(), _>("request stopped")
                })
                .unwrap()
            }));
            assert_eq!(
                result.is_err(),
                matches!(case, "unwind" | "installation-unwind")
            );
            let retained = reservations(&f, &proof.bucket);
            assert_eq!(retained.len(), before.len() + 1, "{case}, tls={tls}");
            for record in before {
                assert!(retained.contains(&record));
            }
            assert_eq!(
                f.node(NodeId::new(4))
                    .get_pg(f.context.id().pg_id().get())
                    .unwrap()
                    .pending_staging_protection(&topology())
                    .unwrap()
                    .is_some(),
                case != "installation-unwind"
            );
        }
    }
}

#[test]
fn staging_direct_introduction_request_remote_partial_fanout_retains_proof_on_unwind() {
    for tls in [false, true] {
        for unwind in [false, true] {
            let (f, owner, proof, _dir) = fixture(tls);
            prepare_bucket(&f, &proof);
            let before = reservations(&f, &proof.bucket);
            let primary = f.remote(NodeId::new(4)).unwrap();
            let _running = serve(primary);
            let admission = f.handle.admit_current_route().unwrap();
            let put = admission
                .active_put_object_route(&proof.bucket, &owner.subject.key)
                .unwrap();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                put.with_direct_put_scope(Default::default(), |_, scope| {
                    let route = admission
                        .complete_fanout_route(
                            f.context.id().pg_id(),
                            f.context.id().cluster_epoch(),
                        )
                        .unwrap();
                    let session = primary
                        .client
                        .open_complete_fanout_primary(&route, deadline())
                        .unwrap();
                    let mut introduction = route
                        .prepare_request_direct_introduction_remote(
                            scope,
                            &primary.client,
                            &session,
                            &owner.subject.id.attempt,
                            deadline(),
                        )
                        .unwrap();
                    introduction.install().unwrap();
                    crate::clock::with_time_override(
                        crate::clock::current_time_millis() + 1_000_000,
                        || assert!(introduction.install().is_err()),
                    );
                    assert_eq!(introduction.installation(), Installed::Installed);
                    let mut publication = introduction
                        .into_publication()
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    publication.publish().unwrap();
                    publication.apply_embedded_replica(NodeId::new(0)).unwrap();
                    if unwind {
                        panic!("frontend interrupted after witness application");
                    }
                    Err::<(), _>("request stopped")
                })
                .unwrap()
            }));
            assert_eq!(result.is_err(), unwind);
            let retained = reservations(&f, &proof.bucket);
            assert_eq!(retained.len(), before.len() + 1);
            for record in before {
                assert!(retained.contains(&record));
            }
        }
    }
}

#[test]
fn staging_direct_introduction_request_remote_crossed_admission_rejects_before_transport() {
    for tls in [false, true] {
        let (f, owner, proof, _dir) = fixture(tls);
        prepare_bucket(&f, &proof);
        let primary = f.remote(NodeId::new(4)).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&calls);
        primary
            .server
            .set_response_frame_test_hook(Arc::new(move |kind, _| {
                if kind == KIND {
                    count.fetch_add(1, Ordering::SeqCst);
                }
            }));
        let _running = serve(primary);
        let before = reservations(&f, &proof.bucket);
        let admission = f.handle.admit_current_route().unwrap();
        let other = f.handle.admit_current_route().unwrap();
        let put = admission
            .active_put_object_route(&proof.bucket, &owner.subject.key)
            .unwrap();
        put.with_direct_put_scope(Default::default(), |_, scope| {
            let crossed = other
                .complete_fanout_route(f.context.id().pg_id(), f.context.id().cluster_epoch())
                .unwrap();
            let session = primary
                .client
                .open_complete_fanout_primary(&crossed, deadline())
                .unwrap();
            let error = crossed
                .prepare_request_direct_introduction_remote(
                    scope,
                    &primary.client,
                    &session,
                    &owner.subject.id.attempt,
                    deadline(),
                )
                .err()
                .unwrap();
            assert!(
                error
                    .to_string()
                    .contains("original admitted primary route"),
                "{error}"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            let wrong_pg = PgId::new((f.context.id().pg_id().get() + 1) % 8);
            let wrong = admission
                .complete_fanout_route(wrong_pg, f.context.id().cluster_epoch())
                .unwrap();
            let error = wrong
                .prepare_request_direct_introduction_remote(
                    scope,
                    &primary.client,
                    &session,
                    &owner.subject.id.attempt,
                    deadline(),
                )
                .err()
                .unwrap();
            assert!(
                error
                    .to_string()
                    .contains("original admitted primary route"),
                "{error}"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            let route = admission
                .complete_fanout_route(f.context.id().pg_id(), f.context.id().cluster_epoch())
                .unwrap();
            let introduction = route
                .prepare_request_direct_introduction_remote(
                    scope,
                    &primary.client,
                    &session,
                    &owner.subject.id.attempt,
                    deadline(),
                )
                .unwrap();
            assert_eq!(introduction.installation(), Installed::NoAcceptanceObserved);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            Ok::<_, ()>(())
        })
        .unwrap()
        .unwrap();
        assert_eq!(reservations(&f, &proof.bucket), before);
    }
}
