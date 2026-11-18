// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Actual ciphertext and durable acknowledgements, not fabricated receipt claims.
use super::*;

fn cross_reply(frame: &mut rpc::StorageRpcFrame, field: &str) {
    let reusable =
        rpc::decode_storage_rpc_response_payload_with_connection_disposition(&frame.payload)
            .unwrap()
            .connection_reusable;
    let bytes = rpc::decode_storage_rpc_response_payload(&frame.payload)
        .unwrap()
        .unwrap();
    // Keep request correlation intact, but supply a canonical, correctly
    // checksummed complete context with independently crossed semantics.
    let body = rpc::complete_applied::decode(&bytes[38..]).unwrap();
    let context =
        ContextualCommand::decode_command(body.command, body.checksum, &topology()).unwrap();
    let crossed_context = context.test_cross_remote_append_reply(field);
    let crossed = crossed_context.encode_command().unwrap();
    let mut response = bytes[..38].to_vec();
    response.extend_from_slice(
        &rpc::complete_applied::encode(rpc::complete_applied::BorrowedRequest {
            node_id: body.node_id,
            epoch: body.epoch,
            pg_id: body.pg_id,
            checksum: crossed.checksum_crc64(),
            command: crossed.bytes(),
        })
        .unwrap(),
    );
    frame.payload = rpc::encode_storage_rpc_success_response(&response);
    rpc::set_storage_rpc_response_connection_reusable(&mut frame.payload, reusable).unwrap();
}

#[test]
fn staging_remote_append_sender_rejects_canonical_crossed_replies_and_preserves_retry_custody() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            with_writer(
                tls,
                target,
                false,
                "uncertain",
                |f, mut writer, owner, shards| {
                    write_all(f, &mut writer, owner, shards);
                    writer
                        .persist(
                            &f.remote(owner.windows[0].participants.acknowledgement_primary)
                                .unwrap()
                                .client,
                        )
                        .unwrap();
                    let Writer::Stream(writer) = writer else {
                        unreachable!()
                    };
                    let mut ready = writer
                        .into_acknowledged()
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    let primary = f.remote(NodeId::new(4)).unwrap();
                    let session = ready.open_append_primary_session(&primary.client).unwrap();
                    let before = f.observe();
                    let history = ready.test_binding_state();
                    let calls = Arc::new(AtomicUsize::new(0));
                    let count = Arc::clone(&calls);
                    primary
                        .server
                        .set_response_frame_test_hook(Arc::new(move |kind, _| {
                            if kind == Kind::StagingStreamAppendPrepare {
                                count.fetch_add(1, Ordering::SeqCst);
                            }
                        }));
                    let failure = ready
                        .prepare_append_remote(&f.remote(NodeId::new(0)).unwrap().client, &session)
                        .err()
                        .expect("crossed endpoint");
                    ready = failure.into_acknowledged();
                    assert_eq!(calls.load(Ordering::SeqCst), 0);
                    let correlation = Arc::new(std::sync::Mutex::new(None::<Vec<u8>>));
                    for field in [
                        "payload",
                        "witness",
                        "index",
                        "cutoff",
                        "participants",
                        "issuer",
                    ] {
                        let correlation = Arc::clone(&correlation);
                        primary.server.set_response_frame_test_hook(Arc::new(
                            move |kind, frame| {
                                if kind == Kind::StagingStreamAppendPrepare {
                                    let bytes =
                                        rpc::decode_storage_rpc_response_payload(&frame.payload)
                                            .unwrap()
                                            .unwrap();
                                    let mut previous = correlation.lock().unwrap();
                                    if let Some(expected) = previous.as_ref() {
                                        assert_eq!(
                                            &bytes[6..38],
                                            expected,
                                            "retry must retain exact request and cutoff"
                                        );
                                    } else {
                                        *previous = Some(bytes[6..38].to_vec());
                                    }
                                    cross_reply(frame, field);
                                }
                            },
                        ));
                        let failure = ready
                            .prepare_append_remote(&primary.client, &session)
                            .err()
                            .expect("crossed reply must fail");
                        assert!(
                            failure.error().to_string().contains("remote append"),
                            "{field}: {}",
                            failure.error()
                        );
                        ready = failure.into_acknowledged();
                        assert_eq!(ready.test_binding_state(), history);
                        assert_eq!(f.observe(), before, "{field}");
                        require_payload(f, owner, shards, true);
                    }
                    primary
                        .server
                        .set_response_frame_test_hook(Arc::new(|_, _| {}));
                    let prepared = ready
                        .prepare_append_remote(&primary.client, &session)
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    assert_eq!(prepared.test_binding_state(), history);
                    drop(prepared);
                    assert_eq!(f.observe(), before);
                    require_payload(f, owner, shards, true);
                },
            );
        }
    }
}

#[test]
fn staging_remote_append_sender_lost_or_closed_reply_keeps_custody_and_original_cutoff() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            for lost in [false, true] {
                with_writer(
                    tls,
                    target.clone(),
                    false,
                    "installed",
                    |f, mut writer, owner, shards| {
                        write_all(f, &mut writer, owner, shards);
                        writer
                            .persist(
                                &f.remote(owner.windows[0].participants.acknowledgement_primary)
                                    .unwrap()
                                    .client,
                            )
                            .unwrap();
                        let Writer::Stream(writer) = writer else {
                            unreachable!()
                        };
                        let ready = writer
                            .into_acknowledged()
                            .unwrap_or_else(|e| panic!("{}", e.error()));
                        let history = ready.test_binding_state();
                        let primary = f.remote(NodeId::new(4)).unwrap();
                        let session = ready.open_append_primary_session(&primary.client).unwrap();
                        primary.server.set_response_frame_test_hook(Arc::new(
                            move |kind, frame| {
                                if kind == Kind::StagingStreamAppendPrepare {
                                    if lost {
                                        StorageNodeServer::test_lose_stream_append_reply();
                                    } else {
                                        rpc::set_storage_rpc_response_connection_reusable(
                                            &mut frame.payload,
                                            false,
                                        )
                                        .unwrap();
                                    }
                                }
                            },
                        ));
                        let before = f.observe();
                        let failure = ready
                            .prepare_append_remote(&primary.client, &session)
                            .err()
                            .expect("lost successful reply");
                        let ready = failure.into_acknowledged();
                        assert_eq!(ready.test_binding_state(), history);
                        assert_eq!(f.observe(), before);
                        require_payload(f, owner, shards, true);
                        drop(session);
                        primary
                            .server
                            .set_response_frame_test_hook(Arc::new(|_, _| {}));
                        // Preparation was read-only: an explicit new attempt may use a
                        // new section, but retains the old physical cutoff and custody.
                        let session = ready.open_append_primary_session(&primary.client).unwrap();
                        let now = crate::clock::current_time_millis();
                        let failure = crate::clock::with_time_override(now + 1_000_000, || {
                            ready
                                .prepare_append_remote(&primary.client, &session)
                                .err()
                                .expect("expired original authority")
                        });
                        let ready = failure.into_acknowledged();
                        assert_eq!(ready.test_binding_state(), history);
                        let prepared = ready
                            .prepare_append_remote(&primary.client, &session)
                            .unwrap_or_else(|e| panic!("{}", e.error()));
                        assert_eq!(prepared.test_binding_state(), history);
                        drop(prepared);
                        assert_eq!(f.observe(), before);
                        require_payload(f, owner, shards, true);
                    },
                );
            }
        }
    }
}

#[test]
fn staging_remote_append_sender_prepares_exact_acknowledged_payload_without_effects() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            for appended in [false, true] {
                with_writer(
                    tls,
                    target.clone(),
                    appended,
                    "installed",
                    |f, mut writer, owner, shards| {
                        write_all(f, &mut writer, owner, shards);
                        writer
                            .persist(
                                &f.remote(
                                    owner
                                        .windows
                                        .last()
                                        .unwrap()
                                        .participants
                                        .acknowledgement_primary,
                                )
                                .unwrap()
                                .client,
                            )
                            .unwrap();
                        let Writer::Stream(writer) = writer else {
                            unreachable!()
                        };
                        let ready = writer
                            .into_acknowledged()
                            .unwrap_or_else(|e| panic!("{}", e.error()));
                        let history = ready.test_binding_state();
                        let client = &f.remote(NodeId::new(4)).unwrap().client;
                        let session = ready.open_append_primary_session(client).unwrap();
                        let before = f.observe();
                        let prepared = ready
                            .prepare_append_remote(client, &session)
                            .unwrap_or_else(|e| panic!("{}", e.error()));
                        assert_eq!(prepared.test_binding_state(), history);
                        assert_eq!(
                            prepared.id().log_index().get(),
                            f.context.id().log_index().get() + 1
                        );
                        let command = prepared.test_command();
                        let crate::metadata_command::MetadataCommandPayload::AppendStreamSegment(
                            append,
                        ) = command.payload()
                        else {
                            unreachable!()
                        };
                        assert_eq!(append.segment.segment_index, u32::from(appended));
                        assert!(append.segment.shard_integrity.is_some());
                        assert_eq!(f.observe(), before);
                        let submission = prepared.into_submission();
                        assert_eq!(submission.installation(), crate::node_runtime::pg_store::StreamAppendInstallationObservation::NoAcceptanceObserved);
                        drop(submission);
                        assert_eq!(f.observe(), before);
                        require_payload(f, owner, shards, true);
                    },
                );
            }
        }
    }
}
