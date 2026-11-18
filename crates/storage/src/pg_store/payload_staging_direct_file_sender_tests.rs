// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Real owning handoff and installed receiver, including self-witness IO.
use super::*;
use crate::node_runtime::pg_store::RemoteDirectStagingFileWriter;
use crate::storage_rpc::{self as rpc, StorageRpcMessageKind as Kind};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn with_writer(
    tls: bool,
    history: &str,
    action: impl FnOnce(&Mixed, RemoteDirectStagingFileWriter<'_>, &StagingOwnerRecord, &[u8]),
) {
    let (_dir, f, proof) = allocation_remote::setup(!tls, StagingTarget::DirectPut, false);
    let receiver = f.remote(NodeId::new(0)).unwrap();
    assert_eq!(receiver.tls, tls);
    receiver.server.test_install_window_receiver_peers(
        f.remote
            .iter()
            .map(|peer| (peer.node, peer.client.test_execution_endpoint().clone()))
            .collect(),
        super::super::super::super::complete_log_rpc::window_receiver_auth_for(
            f.context.id().cluster_epoch(),
            0,
        ),
    );
    run_terminal(&f, proof.as_ref(), history, |f, primary, mut terminal| {
        primary.retire(&mut terminal);
        let confirmed = primary
            .confirm(terminal, deadline(), |_| {})
            .unwrap_or_else(|e| panic!("{}", e.error()));
        let original = confirmed.installation_observation();
        let Primary::Remote(client, session) = primary else {
            unreachable!()
        };
        let writer = confirmed
            .into_direct_file_writer_remote(
                client,
                session,
                &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
            )
            .unwrap_or_else(|e| panic!("{}", e.error()));
        assert_eq!(writer.test_binding_state().1, original);
        let node = f.node(NodeId::new(4));
        let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
        let snapshot = pg.command_build_snapshot().unwrap();
        let owner = append::load(&pg, &fresh_capture::template(StagingTarget::DirectPut));
        snapshot.finish().unwrap();
        let shards = super::super::files::shards(&owner, b"data");
        assert_eq!(owner.windows[0].participants.files[0], receiver.node);
        drop(pg);
        action(f, writer, &owner, &shards[0]);
    });
}

fn read(f: &Mixed, owner: &StagingOwnerRecord) -> Result<Vec<u8>, StoreError> {
    let window = &owner.windows[0];
    f.node(window.participants.files[0]).read_shard_file(
        window.participants.data_pg.get(),
        &crate::ShardKey::new(&window.segment_hash, window.start.get(), 0),
    )
}

pub(super) fn equal_crc_substitution(bytes: &[u8]) -> Vec<u8> {
    // Equal-length CRC64 collision delta; applying it to any prefix preserves
    // the CRC of the complete bytes, including their unchanged suffix.
    let collision = [28, 250, 229, 61, 115, 85, 169, 81, 98, 105, 109, 101, 110];
    let mut crossed = bytes.to_vec();
    for (index, (&left, right)) in b"wire specimen".iter().zip(collision).enumerate() {
        crossed[index] ^= left ^ right;
    }
    assert_ne!(crossed, bytes);
    assert_eq!(
        checksum::crc64::checksum(&crossed),
        checksum::crc64::checksum(bytes)
    );
    crossed
}

#[test]
fn staging_direct_file_sender_unix_tls_success_and_confirmed_retry_preserve_custody() {
    for tls in [false, true] {
        for history in ["installed", "inspected", "uncertain"] {
            with_writer(tls, history, |f, mut writer, owner, bytes| {
                let receiver = f.remote(NodeId::new(0)).unwrap();
                let calls = Arc::new(AtomicUsize::new(0));
                let count = Arc::clone(&calls);
                receiver
                    .server
                    .set_response_frame_test_hook(Arc::new(move |kind, _| {
                        if kind == Kind::StagingShardWrite {
                            count.fetch_add(1, Ordering::SeqCst);
                        }
                    }));
                let before = f.observe();
                let history = writer.test_binding_state();
                assert!(writer.test_file_attempt(0).is_none());
                let ack = writer.write_remote(&receiver.client, 0, 4, bytes).unwrap();
                assert_eq!(read(f, owner).unwrap(), bytes);
                assert_eq!(ack.stored_size, bytes.len() as u64);
                assert_eq!(ack.crc64, checksum::crc64::checksum(bytes));
                let confirmed = writer.test_file_attempt(0).unwrap();
                assert_eq!(confirmed.1, Some(ack));
                assert_eq!(
                    writer.write_remote(&receiver.client, 0, 4, bytes).unwrap(),
                    ack
                );
                assert_eq!(
                    calls.load(Ordering::SeqCst),
                    1,
                    "confirmed retry sends nothing"
                );
                let crossed = equal_crc_substitution(bytes);
                assert!(writer
                    .write_remote(&receiver.client, 0, 4, &crossed)
                    .unwrap_err()
                    .to_string()
                    .contains("changed request"));
                assert_eq!(writer.test_file_attempt(0), Some(confirmed));
                crate::clock::with_time_override(confirmed.2, || {
                    assert!(writer.write_remote(&receiver.client, 0, 4, bytes).is_err());
                });
                assert_eq!(writer.test_file_attempt(0), Some(confirmed));
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                assert_eq!(writer.test_binding_state(), history);
                assert_eq!(f.observe(), before, "file receipt is not a durable ack row");
                drop(writer);
                assert_eq!(f.observe(), before);
                assert_eq!(read(f, owner).unwrap(), bytes, "Drop is not cleanup");
            });
        }
    }
}

#[test]
fn staging_direct_file_sender_lost_or_crossed_reply_keeps_exact_attempt_until_retry() {
    for tls in [false, true] {
        for lost in [false, true] {
            with_writer(tls, "uncertain", |f, mut writer, owner, bytes| {
                let receiver = f.remote(NodeId::new(0)).unwrap();
                let calls = Arc::new(AtomicUsize::new(0));
                let count = Arc::clone(&calls);
                receiver.server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
                    if kind != Kind::StagingShardWrite {
                        return;
                    }
                    match count.fetch_add(1, Ordering::SeqCst) {
                        0 if lost => crate::storage_node_server::StorageNodeServer::test_lose_staging_file_reply(),
                        0 => {
                            let mut payload = rpc::decode_storage_rpc_response_payload(&frame.payload).unwrap().unwrap().to_vec();
                            payload[6] ^= 1;
                            frame.payload = rpc::encode_storage_rpc_success_response(&payload);
                        }
                        1 => {
                            frame.payload = rpc::encode_storage_rpc_error_response(&rpc::StorageRpcErrorResponse {
                                code: rpc::StorageRpcErrorCode::PayloadDecode,
                                message: "later rejection cannot erase the lost file success".into(),
                            }).unwrap();
                        }
                        _ => {}
                    }
                }));
                let before = f.observe();
                let history = writer.test_binding_state();
                assert!(writer.write_remote(&receiver.client, 0, 4, bytes).is_err());
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                assert_eq!(read(f, owner).unwrap(), bytes);
                let uncertain = writer.test_file_attempt(0).unwrap();
                assert!(uncertain.1.is_none());
                let crossed = equal_crc_substitution(bytes);
                assert!(writer
                    .write_remote(&receiver.client, 0, 4, &crossed)
                    .unwrap_err()
                    .to_string()
                    .contains("changed request"));
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                assert_eq!(writer.test_file_attempt(0), Some(uncertain));
                assert!(writer.write_remote(&receiver.client, 0, 4, bytes).is_err());
                assert_eq!(calls.load(Ordering::SeqCst), 2);
                assert_eq!(writer.test_file_attempt(0), Some(uncertain));
                assert_eq!(read(f, owner).unwrap(), bytes);
                let ack = crate::clock::with_time_and_monotonic_override(
                    crate::clock::current_time_millis() + 100,
                    crate::clock::monotonic_time_millis(),
                    || writer.write_remote(&receiver.client, 0, 4, bytes),
                )
                .unwrap();
                assert_eq!(
                    writer.test_file_attempt(0),
                    Some((uncertain.0, Some(ack), uncertain.2))
                );
                assert_eq!(calls.load(Ordering::SeqCst), 3);
                assert_eq!(writer.test_binding_state(), history);
                assert_eq!(f.observe(), before);
            });
        }
    }
}

#[test]
fn staging_direct_file_sender_rejects_crossed_client_geometry_and_expired_retry_without_send() {
    for tls in [false, true] {
        let (_dir, foreign, _) = allocation_remote::setup(!tls, StagingTarget::DirectPut, false);
        with_writer(tls, "installed", |f, mut writer, owner, bytes| {
            let receiver = f.remote(NodeId::new(0)).unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let count = Arc::clone(&calls);
            receiver
                .server
                .set_response_frame_test_hook(Arc::new(move |kind, _| {
                    if kind == Kind::StagingShardWrite {
                        count.fetch_add(1, Ordering::SeqCst);
                        crate::storage_node_server::StorageNodeServer::test_lose_staging_file_reply(
                        );
                    }
                }));
            assert!(writer
                .write_remote(&f.remote(NodeId::new(4)).unwrap().client, 0, 4, bytes)
                .is_err());
            let foreign_client = &foreign.remote(NodeId::new(0)).unwrap().client;
            assert_eq!(
                foreign.context.id().cluster_epoch(),
                f.context.id().cluster_epoch()
            );
            assert!(matches!(
                writer.write_remote(foreign_client, 0, 4, bytes),
                Err(
                    crate::node_runtime::pg_store::StagingWindowConfirmationError::Store(
                        StoreError::RouteCapabilitySubjectMismatch {
                            operation:
                                "staging file requires the exact admitted remote participant"
                        }
                    )
                )
            ));
            assert!(writer
                .write_remote(&receiver.client, 255, 4, bytes)
                .is_err());
            assert!(writer
                .write_remote(&receiver.client, 0, u64::MAX, bytes)
                .is_err());
            assert!(writer.test_file_attempt(0).is_none());
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert!(matches!(read(f, owner), Err(StoreError::NotFound)));
            let before = f.observe();
            // This case injects a lost receiver reply, not failure on an idle
            // pooled socket left by the preceding confirmation RPCs.
            receiver
                .client
                .test_execution_endpoint()
                .close_idle_requests_for_test();
            let error = writer
                .write_remote(&receiver.client, 0, 4, bytes)
                .unwrap_err();
            let detail = match &error {
                crate::node_runtime::pg_store::StagingWindowConfirmationError::Store(
                    StoreError::StorageRpc { detail, .. },
                ) => detail.as_str().to_owned(),
                error => error.to_string(),
            };
            assert_eq!(
                calls.load(Ordering::SeqCst),
                1,
                "first file attempt: {detail}"
            );
            let attempt = writer.test_file_attempt(0).unwrap();
            assert!(attempt.1.is_none());
            crate::clock::with_time_override(attempt.2, || {
                assert!(writer.write_remote(&receiver.client, 0, 4, bytes).is_err());
            });
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(writer.test_file_attempt(0), Some(attempt));
            drop(writer);
            assert_eq!(read(f, owner).unwrap(), bytes);
            assert_eq!(f.observe(), before);
        });
    }
}
