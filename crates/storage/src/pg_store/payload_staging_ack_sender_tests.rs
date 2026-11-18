// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_runtime::pg_store::{
    RemoteDirectStagingFileWriter, RemoteStreamStagingFileWriter,
    StagingWindowConfirmationError as Error,
};
use crate::storage_node_server::StorageNodeServer;
use crate::storage_rpc::{self as rpc, StorageRpcMessageKind as Kind};
use std::sync::atomic::{AtomicUsize, Ordering};

#[path = "payload_staging_remote_append_install_sender_tests.rs"]
mod append_install;
#[path = "payload_staging_remote_append_sender_tests.rs"]
mod append_prepare;
#[path = "payload_staging_remote_direct_snapshot_sender_tests.rs"]
mod direct_prepare;

enum Writer<'a> {
    Direct(Box<RemoteDirectStagingFileWriter<'a>>),
    Stream(Box<RemoteStreamStagingFileWriter<'a>>),
}

impl Writer<'_> {
    fn reject_handoff(self) -> Self {
        match self {
            Self::Direct(writer) => {
                let before = writer.test_binding_state();
                let failure = match writer.into_acknowledged() {
                    Ok(_) => panic!("unconfirmed files handed off"),
                    Err(failure) => failure,
                };
                assert!(matches!(failure.error(), Error::Window(message)
                    if message == "staging acknowledgement persistence is not confirmed"));
                let writer = failure.into_writer();
                assert_eq!(writer.test_binding_state(), before);
                Self::Direct(Box::new(writer))
            }
            Self::Stream(writer) => {
                let before = writer.test_binding_state();
                let failure = match writer.into_acknowledged() {
                    Ok(_) => panic!("unconfirmed files handed off"),
                    Err(failure) => failure,
                };
                assert!(matches!(failure.error(), Error::Window(message)
                    if message == "staging acknowledgement persistence is not confirmed"));
                let writer = failure.into_writer();
                assert_eq!(writer.test_binding_state(), before);
                Self::Stream(Box::new(writer))
            }
        }
    }

    fn finish(self) {
        match self {
            Self::Direct(writer) => {
                let before = writer.test_binding_state();
                let id = writer.id();
                let ready = writer
                    .into_acknowledged()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                assert_eq!(ready.test_binding_state(), before);
                assert_eq!(ready.id(), id);
                drop(ready);
            }
            Self::Stream(writer) => {
                let before = writer.test_binding_state();
                let id = writer.id();
                let ready = writer
                    .into_acknowledged()
                    .unwrap_or_else(|e| panic!("{}", e.error()));
                assert_eq!(ready.test_binding_state(), before);
                assert_eq!(ready.id(), id);
                drop(ready);
            }
        }
    }

    fn write(
        &mut self,
        client: &crate::node_client::UnixStorageNodeClient,
        index: u8,
        bytes: &[u8],
    ) -> Result<crate::WriteAck, Error> {
        match self {
            Self::Direct(w) => w.write_remote(client, index, 4, bytes),
            Self::Stream(w) => w.write_remote(client, index, bytes),
        }
    }
    fn persist(&mut self, client: &crate::node_client::UnixStorageNodeClient) -> Result<(), Error> {
        match self {
            Self::Direct(w) => w.persist_acknowledgements_remote(client),
            Self::Stream(w) => w.persist_acknowledgements_remote(client),
        }
    }
    fn observation(&self) -> Option<(Vec<u8>, bool)> {
        match self {
            Self::Direct(w) => w.test_ack_attempt(),
            Self::Stream(w) => w.test_ack_attempt(),
        }
    }
    fn history(&self) -> (Instant, Option<ContextSubmissionObservation>) {
        match self {
            Self::Direct(w) => w.test_binding_state(),
            Self::Stream(w) => w.test_binding_state(),
        }
    }
}

fn with_writer(
    tls: bool,
    target: StagingTarget,
    appended: bool,
    history: &str,
    action: impl FnOnce(&Mixed, Writer<'_>, &StagingOwnerRecord, &[Vec<u8>]),
) {
    with_writer_endpoints(
        target,
        appended,
        history,
        std::array::from_fn::<_, 5, _>(|i| (i as u32, tls)),
        action,
    );
}

fn with_writer_endpoints<const N: usize>(
    target: StagingTarget,
    appended: bool,
    history: &str,
    endpoints: [(u32, bool); N],
    action: impl FnOnce(&Mixed, Writer<'_>, &StagingOwnerRecord, &[Vec<u8>]),
) {
    let (_dir, f, proof) =
        allocation_remote::setup_with_endpoints(target.clone(), appended, endpoints);
    for receiver in &f.remote {
        receiver.server.test_install_window_receiver_peers(
            f.remote
                .iter()
                .map(|p| (p.node, p.client.test_execution_endpoint().clone()))
                .collect(),
            super::super::super::super::complete_log_rpc::window_receiver_auth_for(
                f.context.id().cluster_epoch(),
                receiver.node.as_u32(),
            ),
        );
    }
    run_terminal(&f, proof.as_ref(), history, |f, primary, mut terminal| {
        primary.retire(&mut terminal);
        let confirmed = primary
            .confirm(terminal, deadline(), |_| {})
            .unwrap_or_else(|e| panic!("{}", e.error()));
        let node = f.node(NodeId::new(4));
        let owner = {
            let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
            let snapshot = pg.command_build_snapshot().unwrap();
            let owner = append::load(&pg, &fresh_capture::template(target.clone()));
            snapshot.finish().unwrap();
            owner
        };
        let shards = super::super::files::shards(&owner, b"data");
        let Primary::Remote(client, session) = primary else {
            unreachable!()
        };
        let writer = if target == StagingTarget::DirectPut {
            Writer::Direct(Box::new(
                confirmed
                    .into_direct_file_writer_remote(
                        client,
                        session,
                        &crate::tests::sse_s3_segment_test_fixture(b"data", 0).encryption,
                    )
                    .unwrap_or_else(|e| panic!("{}", e.error())),
            ))
        } else {
            let (_, mut request, expected) = allocation_remote::inputs(f, target);
            let window = owner.windows.last().unwrap();
            let mut stored = shards[..usize::from(window.geometry.ec.k)].concat();
            stored.truncate(window.geometry.codec.stored_size(4).unwrap() as usize);
            request.segment_crc64 = checksum::crc64::checksum(&stored);
            request.payload_crc64 = checksum::crc64::checksum(b"data");
            let allocated = confirmed
                .allocate_stream_remote(client, session, &request, &expected)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            Writer::Stream(Box::new(
                allocated
                    .into_stream_file_writer_remote(client, session)
                    .unwrap_or_else(|e| panic!("{}", e.error())),
            ))
        };
        action(f, writer, &owner, &shards);
    });
}

fn write_all(f: &Mixed, writer: &mut Writer<'_>, owner: &StagingOwnerRecord, shards: &[Vec<u8>]) {
    let window = owner.windows.last().unwrap();
    for (i, bytes) in shards.iter().enumerate() {
        let client = &f.remote(window.participants.files[i]).unwrap().client;
        writer.write(client, i as u8, bytes).unwrap();
    }
}

fn require_payload(f: &Mixed, owner: &StagingOwnerRecord, shards: &[Vec<u8>], registered: bool) {
    let window = owner.windows.last().unwrap();
    let node = f.node(window.participants.acknowledgement_primary);
    let pg = node.get_pg(window.participants.data_pg.get()).unwrap();
    for (i, bytes) in shards.iter().enumerate() {
        let key = crate::ShardKey::new(&window.segment_hash, window.start.get(), i as u8);
        let result = pg.validate_written_shard_ack(
            &key,
            crate::WriteAck {
                stored_size: bytes.len() as u64,
                crc64: checksum::crc64::checksum(bytes),
            },
        );
        if registered {
            result.unwrap();
        } else {
            assert!(matches!(result, Err(StoreError::NotFound)));
            let count: i64 = pg
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM shards WHERE shard_key = ?1",
                    [key.as_bytes().as_slice()],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(
                count, 0,
                "no acknowledgement row of any status before persistence"
            );
        }
        assert_eq!(
            &f.node(window.participants.files[i])
                .read_shard_file(window.participants.data_pg.get(), &key)
                .unwrap(),
            bytes
        );
    }
}

fn success_matrix(tls: bool) {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        for appended in [false, true] {
            if appended && target == StagingTarget::DirectPut {
                continue;
            }
            for history in ["installed", "inspected", "uncertain"] {
                with_writer(
                    tls,
                    target.clone(),
                    appended,
                    history,
                    |f, mut writer, owner, shards| {
                        let window = owner.windows.last().unwrap();
                        let primary = f
                            .remote(window.participants.acknowledgement_primary)
                            .unwrap();
                        let calls = Arc::new(AtomicUsize::new(0));
                        let count = Arc::clone(&calls);
                        primary
                            .server
                            .set_response_frame_test_hook(Arc::new(move |kind, _| {
                                if kind == Kind::StagingShardAckRecord {
                                    count.fetch_add(1, Ordering::SeqCst);
                                }
                            }));
                        let before = f.observe();
                        let history = writer.history();
                        for (i, bytes) in shards.iter().enumerate() {
                            writer = writer.reject_handoff();
                            assert!(writer.persist(&primary.client).is_err());
                            assert!(writer.observation().is_none());
                            writer
                                .write(
                                    &f.remote(window.participants.files[i]).unwrap().client,
                                    i as u8,
                                    bytes,
                                )
                                .unwrap();
                        }
                        assert_eq!(calls.load(Ordering::SeqCst), 0);
                        require_payload(f, owner, shards, false);
                        writer = writer.reject_handoff();
                        writer.persist(&primary.client).unwrap();
                        require_payload(f, owner, shards, true);
                        let confirmed = writer.observation().unwrap();
                        assert!(confirmed.1);
                        write_all(f, &mut writer, owner, shards);
                        assert_eq!(writer.observation(), Some(confirmed.clone()));
                        let node = f.node(window.participants.acknowledgement_primary);
                        let changes = node
                            .get_pg(window.participants.data_pg.get())
                            .unwrap()
                            .conn
                            .total_changes();
                        writer.persist(&primary.client).unwrap();
                        assert_eq!(calls.load(Ordering::SeqCst), 1);
                        assert_eq!(
                            node.get_pg(window.participants.data_pg.get())
                                .unwrap()
                                .conn
                                .total_changes(),
                            changes
                        );
                        let cutoff = rpc::complete_staging_ack::decode_request(&confirmed.0)
                            .unwrap()
                            .receiver
                            .deadline
                            .portable_wall_valid_until_ms;
                        crate::clock::with_time_override(cutoff, || {
                            assert!(writer.persist(&primary.client).is_err())
                        });
                        assert_eq!(writer.observation(), Some(confirmed));
                        assert_eq!(writer.history(), history);
                        assert_eq!(f.observe(), before);
                        // Evidence can be handed off after expiry without renewing
                        // mutation authority. The ready holder has no write API.
                        crate::clock::with_time_override(cutoff, || writer.finish());
                        require_payload(f, owner, shards, true);
                        assert_eq!(f.observe(), before, "Drop never releases custody");
                    },
                );
            }
        }
    }
}

#[test]
fn staging_ack_sender_unix_complete_receipts_exact_retry_and_custody() {
    success_matrix(false);
}

#[test]
fn staging_ack_sender_tls_complete_receipts_exact_retry_and_custody() {
    success_matrix(true);
}

#[test]
fn staging_ack_sender_lost_or_crossed_reply_retains_uncertainty_and_frozen_request() {
    for tls in [false, true] {
        for target in [
            StagingTarget::DirectPut,
            StagingTarget::StreamPut,
            multipart_target(),
        ] {
            for lost in [false, true] {
                with_writer(
                    tls,
                    target.clone(),
                    false,
                    "uncertain",
                    |f, mut writer, owner, shards| {
                        write_all(f, &mut writer, owner, shards);
                        let primary = f
                            .remote(owner.windows[0].participants.acknowledgement_primary)
                            .unwrap();
                        let calls = Arc::new(AtomicUsize::new(0));
                        let count = Arc::clone(&calls);
                        primary.server.set_response_frame_test_hook(Arc::new(
                            move |kind, frame| {
                                if kind != Kind::StagingShardAckRecord {
                                    return;
                                }
                                match count.fetch_add(1, Ordering::SeqCst) {
                                    0 if lost => StorageNodeServer::test_lose_staging_ack_reply(),
                                    0 => {
                                        let mut bytes = rpc::decode_storage_rpc_response_payload(
                                            &frame.payload,
                                        )
                                        .unwrap()
                                        .unwrap()
                                        .to_vec();
                                        bytes[6] ^= 1;
                                        frame.payload =
                                            rpc::encode_storage_rpc_success_response(&bytes);
                                    }
                                    1 => {
                                        frame.payload = rpc::encode_storage_rpc_error_response(
                                            &rpc::StorageRpcErrorResponse {
                                                code: rpc::StorageRpcErrorCode::PayloadDecode,
                                                message: "later rejection is not absence evidence"
                                                    .into(),
                                            },
                                        )
                                        .unwrap();
                                    }
                                    _ => {}
                                }
                            },
                        ));
                        let before = f.observe();
                        let history = writer.history();
                        assert!(writer.persist(&primary.client).is_err());
                        assert_eq!(calls.load(Ordering::SeqCst), 1, "no automatic retry");
                        require_payload(f, owner, shards, true);
                        let uncertain = writer.observation().unwrap();
                        assert!(!uncertain.1);
                        writer = writer.reject_handoff();
                        assert_eq!(writer.observation(), Some(uncertain.clone()));
                        assert!(writer.persist(&primary.client).is_err());
                        assert_eq!(writer.observation(), Some(uncertain.clone()));
                        writer = writer.reject_handoff();
                        assert_eq!(writer.observation(), Some(uncertain.clone()));
                        crate::clock::with_time_and_monotonic_override(
                            crate::clock::current_time_millis() + 100,
                            crate::clock::monotonic_time_millis(),
                            || writer.persist(&primary.client),
                        )
                        .unwrap();
                        assert_eq!(writer.observation(), Some((uncertain.0, true)));
                        assert_eq!(calls.load(Ordering::SeqCst), 3);
                        assert_eq!(writer.history(), history);
                        assert_eq!(f.observe(), before);
                        writer.finish();
                        require_payload(f, owner, shards, true);
                    },
                );
            }
        }
    }
}

#[test]
fn staging_ack_sender_requires_confirmed_files_and_exact_primary_client() {
    for tls in [false, true] {
        // Construct unrelated clients before taking any owning route admission.
        let (_foreign_dir, foreign, _) = allocation_remote::setup_with_endpoints(
            StagingTarget::DirectPut,
            false,
            std::array::from_fn::<_, 5, _>(|i| (i as u32, tls)),
        );
        for target in [
            StagingTarget::DirectPut,
            StagingTarget::StreamPut,
            multipart_target(),
        ] {
            with_writer(
                tls,
                target,
                false,
                "installed",
                |f, mut writer, owner, shards| {
                    let window = owner.windows.last().unwrap();
                    let primary = f
                        .remote(window.participants.acknowledgement_primary)
                        .unwrap();
                    let last_index = shards.len() - 1;
                    let last = f.remote(window.participants.files[last_index]).unwrap();
                    last.server
                        .set_response_frame_test_hook(Arc::new(|kind, _| {
                            if kind == Kind::StagingShardWrite {
                                StorageNodeServer::test_lose_staging_file_reply();
                            }
                        }));
                    for (index, bytes) in shards.iter().enumerate() {
                        let result = writer.write(
                            &f.remote(window.participants.files[index]).unwrap().client,
                            index as u8,
                            bytes,
                        );
                        if index == last_index {
                            assert!(result.is_err());
                        } else {
                            result.unwrap();
                        }
                    }
                    require_payload(f, owner, shards, false);
                    assert!(writer
                        .persist(&primary.client)
                        .unwrap_err()
                        .to_string()
                        .contains("incomplete staging acknowledgement set"));
                    assert!(
                        writer.observation().is_none(),
                        "uncertain file is not a receipt"
                    );
                    last.server
                        .set_response_frame_test_hook(Arc::new(|_, _| {}));
                    writer
                        .write(&last.client, last_index as u8, &shards[last_index])
                        .unwrap();
                    let calls = Arc::new(AtomicUsize::new(0));
                    let count = Arc::clone(&calls);
                    primary
                        .server
                        .set_response_frame_test_hook(Arc::new(move |kind, _| {
                            if kind == Kind::StagingShardAckRecord {
                                count.fetch_add(1, Ordering::SeqCst);
                                StorageNodeServer::test_lose_staging_ack_reply();
                            }
                        }));
                    for client in [
                        &foreign.remote(primary.node).unwrap().client,
                        &f.remote(window.participants.files[0]).unwrap().client,
                    ] {
                        assert!(matches!(
                            writer.persist(client),
                            Err(Error::Store(StoreError::RouteCapabilitySubjectMismatch {
                                operation: "staging acknowledgement captured remote primary"
                            }))
                        ));
                        assert!(writer.observation().is_none());
                    }
                    assert_eq!(calls.load(Ordering::SeqCst), 0);
                    let before = f.observe();
                    assert!(writer.persist(&primary.client).is_err());
                    let uncertain = writer.observation().unwrap();
                    assert!(!uncertain.1);
                    assert!(writer
                        .persist(&foreign.remote(primary.node).unwrap().client)
                        .is_err());
                    let cutoff = rpc::complete_staging_ack::decode_request(&uncertain.0)
                        .unwrap()
                        .receiver
                        .deadline
                        .portable_wall_valid_until_ms;
                    crate::clock::with_time_override(cutoff, || {
                        assert!(writer.persist(&primary.client).is_err())
                    });
                    assert_eq!(writer.observation(), Some(uncertain));
                    assert_eq!(calls.load(Ordering::SeqCst), 1);
                    drop(writer);
                    require_payload(f, owner, shards, true);
                    assert_eq!(
                        f.observe(),
                        before,
                        "expired uncertain Drop must preserve the owner"
                    );
                },
            );
        }
    }
}
