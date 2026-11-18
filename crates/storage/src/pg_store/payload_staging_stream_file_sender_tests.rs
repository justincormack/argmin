// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Successful allocation custody -> acknowledged section release -> real files.
use super::*;
use crate::node_runtime::pg_store::RemoteStreamStagingFileWriter;
use crate::storage_rpc::{self as rpc, StorageRpcMessageKind as Kind};
use std::sync::atomic::{AtomicUsize, Ordering};

fn with_writer(
    tls: bool,
    target: StagingTarget,
    appended: bool,
    history: &str,
    action: impl FnOnce(&Mixed, RemoteStreamStagingFileWriter<'_>, &StagingOwnerRecord, &[u8]),
) {
    let (_dir, f, proof) = allocation_remote::setup(!tls, target.clone(), appended);
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
        let (owner, mut request, expected) = allocation_remote::inputs(f, target);
        let shards = super::super::files::shards(&owner, b"data");
        let window = owner.windows.last().unwrap();
        assert_eq!(window.participants.files[0], receiver.node);
        let mut stored = shards[..usize::from(window.geometry.ec.k)].concat();
        stored.truncate(window.geometry.codec.stored_size(4).unwrap() as usize);
        request.segment_crc64 = checksum::crc64::checksum(&stored);
        request.payload_crc64 = checksum::crc64::checksum(b"data");
        let Primary::Remote(client, session) = primary else {
            unreachable!()
        };
        let allocated = confirmed
            .allocate_stream_remote(client, session, &request, &expected)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        assert_eq!(allocated.segment().segment_vid, window.start);
        assert_eq!(allocated.segment().segment_index, u32::from(appended));
        let writer = allocated
            .into_stream_file_writer_remote(client, session)
            .unwrap_or_else(|e| panic!("{}", e.error()));
        assert_eq!(writer.test_binding_state().1, original);
        f.remote(NodeId::new(4))
            .unwrap()
            .server
            .test_require_window_primary_released(f.context.id().pg_id());
        // These canaries inject receiver outcomes, not idle pooled-socket errors.
        receiver
            .client
            .test_execution_endpoint()
            .close_idle_requests_for_test();
        action(f, writer, &owner, &shards[0]);
    });
}

fn read(f: &Mixed, owner: &StagingOwnerRecord) -> Result<Vec<u8>, StoreError> {
    let window = owner.windows.last().unwrap();
    f.node(window.participants.files[0]).read_shard_file(
        window.participants.data_pg.get(),
        &crate::ShardKey::new(&window.segment_hash, window.start.get(), 0),
    )
}

fn success(tls: bool) {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for appended in [false, true] {
            for history in ["installed", "inspected", "uncertain"] {
                with_writer(
                    tls,
                    target.clone(),
                    appended,
                    history,
                    |f, mut writer, owner, bytes| {
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
                        let custody = writer.test_binding_state();
                        let ack = writer.write_remote(&receiver.client, 0, bytes).unwrap();
                        assert_eq!(read(f, owner).unwrap(), bytes);
                        assert_eq!(ack.stored_size, bytes.len() as u64);
                        assert_eq!(ack.crc64, checksum::crc64::checksum(bytes));
                        let confirmed = writer.test_file_attempt(0).unwrap();
                        assert_eq!(confirmed.1, Some(ack));
                        assert_eq!(
                            writer.write_remote(&receiver.client, 0, bytes).unwrap(),
                            ack
                        );
                        assert_eq!(calls.load(Ordering::SeqCst), 1);
                        let crossed = direct_file_sender::equal_crc_substitution(bytes);
                        assert!(writer
                            .write_remote(&receiver.client, 0, &crossed)
                            .unwrap_err()
                            .to_string()
                            .contains("changed request"));
                        crate::clock::with_time_override(confirmed.2, || {
                            assert!(writer.write_remote(&receiver.client, 0, bytes).is_err());
                        });
                        assert_eq!(calls.load(Ordering::SeqCst), 1);
                        assert_eq!(writer.test_file_attempt(0), Some(confirmed));
                        assert_eq!(writer.test_binding_state(), custody);
                        assert_eq!(
                            f.observe(),
                            before,
                            "no cursor change or acknowledgement SQL"
                        );
                        drop(writer);
                        assert_eq!(read(f, owner).unwrap(), bytes);
                        assert_eq!(f.observe(), before, "Drop cannot release custody");
                    },
                );
            }
        }
    }
}

#[test]
fn staging_stream_file_sender_unix_creation_append_and_histories() {
    success(false);
}
#[test]
fn staging_stream_file_sender_tls_creation_append_and_histories() {
    success(true);
}

fn failures(tls: bool) {
    // Construct the foreign domain before original admission, not while its
    // session/transport deadlines are already running.
    let (_dir, foreign, _) = allocation_remote::setup(!tls, StagingTarget::StreamPut, false);
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for lost in [false, true] {
            with_writer(
                tls,
                target.clone(),
                true,
                "uncertain",
                |f, mut writer, owner, bytes| {
                    let receiver = f.remote(NodeId::new(0)).unwrap();
                    let calls = Arc::new(AtomicUsize::new(0));
                    let count = Arc::clone(&calls);
                    receiver.server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
                    if kind != Kind::StagingShardWrite { return; }
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
                                message: "later rejection cannot erase the lost stream file success".into(),
                            }).unwrap();
                        }
                        _ => {}
                    }
                }));
                    assert_eq!(
                        foreign.context.id().cluster_epoch(),
                        f.context.id().cluster_epoch()
                    );
                    for client in [
                        &foreign.remote(NodeId::new(0)).unwrap().client,
                        &f.remote(NodeId::new(4)).unwrap().client,
                    ] {
                        assert!(matches!(writer.write_remote(client, 0, bytes), Err(
                        crate::node_runtime::pg_store::StagingWindowConfirmationError::Store(
                            StoreError::RouteCapabilitySubjectMismatch { operation: "staging file requires the exact admitted remote participant" }
                        )
                    )));
                    }
                    assert!(writer.write_remote(&receiver.client, 255, bytes).is_err());
                    assert!(writer
                        .write_remote(&receiver.client, 0, &bytes[..bytes.len() - 1])
                        .is_err());
                    assert!(writer.test_file_attempt(0).is_none());
                    assert_eq!(calls.load(Ordering::SeqCst), 0);
                    assert!(matches!(read(f, owner), Err(StoreError::NotFound)));
                    let before = f.observe();
                    let custody = writer.test_binding_state();
                    assert!(writer.write_remote(&receiver.client, 0, bytes).is_err());
                    assert_eq!(calls.load(Ordering::SeqCst), 1);
                    assert_eq!(read(f, owner).unwrap(), bytes);
                    let uncertain = writer.test_file_attempt(0).unwrap();
                    assert!(uncertain.1.is_none());
                    let crossed = direct_file_sender::equal_crc_substitution(bytes);
                    assert!(writer
                        .write_remote(&receiver.client, 0, &crossed)
                        .unwrap_err()
                        .to_string()
                        .contains("changed request"));
                    crate::clock::with_time_override(uncertain.2, || {
                        assert!(writer.write_remote(&receiver.client, 0, bytes).is_err());
                    });
                    assert_eq!(calls.load(Ordering::SeqCst), 1);
                    assert_eq!(writer.test_file_attempt(0), Some(uncertain));
                    assert!(writer.write_remote(&receiver.client, 0, bytes).is_err());
                    assert_eq!(calls.load(Ordering::SeqCst), 2);
                    assert_eq!(writer.test_file_attempt(0), Some(uncertain));
                    let ack = crate::clock::with_time_and_monotonic_override(
                        crate::clock::current_time_millis() + 100,
                        crate::clock::monotonic_time_millis(),
                        || writer.write_remote(&receiver.client, 0, bytes),
                    )
                    .unwrap();
                    assert_eq!(calls.load(Ordering::SeqCst), 3);
                    assert_eq!(
                        writer.test_file_attempt(0),
                        Some((uncertain.0, Some(ack), uncertain.2))
                    );
                    assert_eq!(writer.test_binding_state(), custody);
                    assert_eq!(f.observe(), before);
                    drop(writer);
                    assert_eq!(read(f, owner).unwrap(), bytes);
                    assert_eq!(f.observe(), before);
                },
            );
        }
    }
}

#[test]
fn staging_stream_file_sender_unix_failures_preserve_exact_allocation_and_retry() {
    failures(false);
}
#[test]
fn staging_stream_file_sender_tls_failures_preserve_exact_allocation_and_retry() {
    failures(true);
}
