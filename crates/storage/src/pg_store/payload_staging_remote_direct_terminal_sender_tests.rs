// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0
use super::*;

macro_rules! witnessed {
    ($f:expr, $writer:ident, $owner:expr, $shards:expr, $session:ident) => {{
        let mut ready = ready!($f, $writer, $owner, $shards);
        let client = &$f.remote(NodeId::new(4)).unwrap().client;
        $session = ready.open_direct_primary_session(client).unwrap();
        ready
            .prepare_direct_remote(client, &$session)
            .unwrap_or_else(|e| panic!("{}", e.error()))
            .with_integrity(crate::tests::sse_s3_segment_test_fixture(b"data", 0).storage_crc64)
            .unwrap_or_else(|e| panic!("{}", e.error()))
    }};
}

#[path = "payload_staging_remote_direct_install_sender_tests.rs"]
mod installation;

fn input(evidence: &crate::types::CommittedObjectEncryption) -> crate::StreamPutCommitInput {
    crate::StreamPutCommitInput {
        versioning: crate::BucketVersioningState::Suspended,
        version_id: crate::VersionId::Null,
        owner: crate::OwnerIdentity::from_principal("remote-direct-owner"),
        acl_grants: Default::default(),
        public_read: true,
        etag_crc64: 1234,
        tags: None,
        metadata_blob: Default::default(),
        system_metadata_blob: Default::default(),
        object_lock: Default::default(),
        encryption: evidence.encryption().clone(),
    }
}

#[test]
fn staging_remote_direct_terminal_sender_preserves_custody_and_exact_command() {
    for tls in [false, true] {
        for origin in ["installed", "inspected", "uncertain"] {
            with_writer(
                tls,
                StagingTarget::DirectPut,
                false,
                origin,
                |f, mut writer, owner, shards| {
                    let session;
                    let witnessed = witnessed!(f, writer, owner, shards, session);
                    let history = witnessed.test_binding_state();
                    let evidence = crate::tests::sse_s3_segment_test_fixture(b"data", 0)
                        .committed_encryption_evidence(witnessed.encryption_subject());
                    let input = input(&evidence);
                    let before = f.observe();
                    let prepared = witnessed
                        .prepare_terminal_remote(
                            &f.remote(NodeId::new(4)).unwrap().client,
                            &session,
                            &input,
                            &evidence,
                        )
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    let command = prepared.test_command();
                    assert_eq!(
                        command.id().log_index().get(),
                        f.context.id().log_index().get() + 1
                    );
                    assert_eq!(prepared.test_binding_state(), history);
                    let crate::metadata_command::MetadataCommandPayload::CommitDirectPutObject(
                        commit,
                    ) = command.payload()
                    else {
                        panic!("terminal")
                    };
                    assert_eq!(commit.object.encryption, input.encryption);
                    assert_eq!(commit.object.owner, input.owner);
                    assert_eq!(
                        commit.object.generation_id,
                        owner.subject.payload_generation
                    );
                    assert_eq!(commit.segments[0].segment_vid, owner.windows[0].start);
                    assert_eq!(prepared.into_submission().installation(), crate::node_runtime::pg_store::DirectTerminalInstallationObservation::NoAcceptanceObserved);
                    assert_eq!(f.observe(), before);
                    require_payload(f, owner, shards, true);
                },
            );
        }
    }
}

fn cross_reply(frame: &mut rpc::StorageRpcFrame, field: &str) {
    let reusable =
        rpc::decode_storage_rpc_response_payload_with_connection_disposition(&frame.payload)
            .unwrap()
            .connection_reusable;
    let bytes = rpc::decode_storage_rpc_response_payload(&frame.payload)
        .unwrap()
        .unwrap();
    let mut response = bytes[..38].to_vec();
    if field == "correlation" {
        response[6] ^= 1;
        response.extend_from_slice(&bytes[38..]);
    } else {
        let body = rpc::complete_applied::decode(&bytes[38..]).unwrap();
        let context =
            ContextualCommand::decode_command(body.command, body.checksum, &topology()).unwrap();
        let context = context.test_cross_direct_terminal_reply(field);
        let encoded = context.encode_command().unwrap();
        response.extend_from_slice(
            &rpc::complete_applied::encode(rpc::complete_applied::BorrowedRequest {
                node_id: body.node_id,
                epoch: body.epoch,
                pg_id: body.pg_id,
                checksum: encoded.checksum_crc64(),
                command: encoded.bytes(),
            })
            .unwrap(),
        );
    }
    frame.payload = rpc::encode_storage_rpc_success_response(&response);
    rpc::set_storage_rpc_response_connection_reusable(&mut frame.payload, reusable).unwrap();
}

#[test]
fn staging_remote_direct_terminal_sender_rejects_canonical_crossed_replies_and_retries() {
    for tls in [false, true] {
        with_writer(
            tls,
            StagingTarget::DirectPut,
            false,
            "uncertain",
            |f, mut writer, owner, shards| {
                let session;
                let mut witnessed = witnessed!(f, writer, owner, shards, session);
                let history = witnessed.test_binding_state();
                let evidence = crate::tests::sse_s3_segment_test_fixture(b"data", 0)
                    .committed_encryption_evidence(witnessed.encryption_subject());
                let input = input(&evidence);
                let before = f.observe();
                let primary = f.remote(NodeId::new(4)).unwrap();
                let correlation = Arc::new(std::sync::Mutex::new(None::<Vec<u8>>));
                for field in [
                    "correlation",
                    "index",
                    "generation",
                    "key",
                    "owner",
                    "etag",
                    "encryption",
                    "segment",
                    "witness",
                    "reservation",
                    "sequence",
                ] {
                    let correlation = Arc::clone(&correlation);
                    primary
                        .server
                        .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                            if kind == Kind::StagingDirectPrepare {
                                let bytes =
                                    rpc::decode_storage_rpc_response_payload(&frame.payload)
                                        .unwrap()
                                        .unwrap();
                                let mut saved = correlation.lock().unwrap();
                                if let Some(previous) = saved.as_ref() {
                                    assert_eq!(&bytes[6..38], previous);
                                } else {
                                    *saved = Some(bytes[6..38].to_vec());
                                }
                                cross_reply(frame, field);
                            }
                        }));
                    let failure = witnessed
                        .prepare_terminal_remote(&primary.client, &session, &input, &evidence)
                        .err()
                        .expect(field);
                    assert!(
                        failure.error().to_string().contains("direct terminal"),
                        "{field}: {}",
                        failure.error()
                    );
                    witnessed = failure.into_witnessed();
                    assert_eq!(witnessed.test_binding_state(), history);
                    assert_eq!(f.observe(), before);
                    require_payload(f, owner, shards, true);
                }
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(|_, _| {}));
                let prepared = witnessed
                    .prepare_terminal_remote(&primary.client, &session, &input, &evidence)
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                assert_eq!(prepared.test_binding_state(), history);
                drop(prepared);
                assert_eq!(f.observe(), before);
                require_payload(f, owner, shards, true);
            },
        );
    }
}

#[test]
fn staging_remote_direct_terminal_sender_rejects_crossed_authority_before_transport() {
    for tls in [false, true] {
        with_writer(
            tls,
            StagingTarget::DirectPut,
            false,
            "installed",
            |f, mut writer, owner, shards| {
                let session;
                let mut witnessed = witnessed!(f, writer, owner, shards, session);
                let history = witnessed.test_binding_state();
                let fixture = crate::tests::sse_s3_segment_test_fixture(b"data", 0);
                let evidence =
                    fixture.committed_encryption_evidence(witnessed.encryption_subject());
                let input = input(&evidence);
                let primary = f.remote(NodeId::new(4)).unwrap();
                let calls = Arc::new(AtomicUsize::new(0));
                let count = Arc::clone(&calls);
                primary
                    .server
                    .set_response_frame_test_hook(Arc::new(move |kind, _| {
                        if kind == Kind::StagingDirectPrepare {
                            count.fetch_add(1, Ordering::SeqCst);
                        }
                    }));
                let before = f.observe();
                let crossed = crate::ObjectEncryptionSubject::standard_object(
                    owner.subject.bucket_incarnation,
                    &owner.subject.bucket,
                    &crate::tests::object_key("crossed"),
                    owner.subject.payload_generation,
                    &fixture.encryption,
                );
                let wrong = fixture.committed_encryption_evidence(&crossed);
                let mut crossed_input = input.clone();
                crossed_input.encryption = wrong.encryption().clone();
                let failure = witnessed
                    .prepare_terminal_remote(&primary.client, &session, &crossed_input, &wrong)
                    .err()
                    .unwrap();
                assert!(failure
                    .error()
                    .to_string()
                    .contains("committed encryption subject"));
                witnessed = failure.into_witnessed();
                witnessed = witnessed
                    .prepare_terminal_remote(
                        &f.remote(NodeId::new(0)).unwrap().client,
                        &session,
                        &input,
                        &evidence,
                    )
                    .err()
                    .unwrap()
                    .into_witnessed();
                witnessed = crate::clock::with_time_override(
                    crate::clock::current_time_millis() + 1_000_000,
                    || {
                        witnessed
                            .prepare_terminal_remote(&primary.client, &session, &input, &evidence)
                            .err()
                            .unwrap()
                            .into_witnessed()
                    },
                );
                drop(session);
                let replacement = primary
                    .client
                    .open_metadata_command_critical_section_until(
                        f.context.id().pg_id(),
                        deadline(),
                    )
                    .unwrap();
                witnessed = witnessed
                    .prepare_terminal_remote(&primary.client, &replacement, &input, &evidence)
                    .err()
                    .expect("reconnected session")
                    .into_witnessed();
                assert_eq!(calls.load(Ordering::SeqCst), 0);
                assert_eq!(witnessed.test_binding_state(), history);
                drop(witnessed);
                assert_eq!(f.observe(), before);
                require_payload(f, owner, shards, true);
            },
        );
    }
}

#[test]
fn staging_remote_direct_terminal_sender_lost_or_closed_response_retains_payload() {
    for tls in [false, true] {
        for lost in [false, true] {
            with_writer(
                tls,
                StagingTarget::DirectPut,
                false,
                "installed",
                |f, mut writer, owner, shards| {
                    let session;
                    let witnessed = witnessed!(f, writer, owner, shards, session);
                    let history = witnessed.test_binding_state();
                    let evidence = crate::tests::sse_s3_segment_test_fixture(b"data", 0)
                        .committed_encryption_evidence(witnessed.encryption_subject());
                    let input = input(&evidence);
                    let primary = f.remote(NodeId::new(4)).unwrap();
                    primary
                        .server
                        .set_response_frame_test_hook(Arc::new(move |kind, frame| {
                            if kind == Kind::StagingDirectPrepare {
                                if lost {
                                    StorageNodeServer::test_lose_direct_snapshot_reply();
                                } else {
                                    rpc::set_storage_rpc_response_connection_reusable(
                                        &mut frame.payload,
                                        false,
                                    )
                                    .unwrap();
                                }
                            }
                        }));
                    let before = f.observe();
                    let witnessed = witnessed
                        .prepare_terminal_remote(&primary.client, &session, &input, &evidence)
                        .err()
                        .expect("unconfirmed read")
                        .into_witnessed();
                    assert_eq!(witnessed.test_binding_state(), history);
                    drop(witnessed);
                    assert_eq!(f.observe(), before);
                    require_payload(f, owner, shards, true);
                },
            );
        }
    }
}
