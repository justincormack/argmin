// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Read-only installed receiver, not remote file-write activation.
use super::*;
use crate::storage_rpc::{
    complete_stream_allocation as allocation, complete_stream_file_primary as wire,
};

fn diagnostic(error: &StoreError) -> String {
    match error {
        StoreError::StorageRpc { detail, .. } => detail.as_str().to_owned(),
        _ => error.to_string(),
    }
}

fn matrix(tls: bool) {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for appended in [false, true] {
            for history in ["installed", "inspected", "uncertain"] {
                let (_dir, f, proof) = allocation_remote::setup(tls, target.clone(), appended);
                run_terminal(&f, proof.as_ref(), history, |f, primary, mut terminal| {
                    primary.retire(&mut terminal);
                    let confirmed = primary
                        .confirm(terminal, deadline(), |_| {})
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    let (owner, request, expected) = allocation_remote::inputs(f, target.clone());
                    let Primary::Remote(client, session) = primary else {
                        unreachable!()
                    };
                    let allocation_bytes =
                        confirmed.test_stream_allocation_bytes(&request, &expected);
                    let allocated = confirmed
                        .allocate_stream_remote(client, session, &request, &expected)
                        .unwrap_or_else(|e| panic!("{}", e.error()));
                    let bytes =
                        wire::encode_request(&allocation_bytes, allocated.segment()).unwrap();
                    let before = f.observe();
                    let shadows = f.shadows();
                    let inspect = |payload: Vec<u8>| {
                        session.test_stream_allocation_payload(payload, deadline())
                    };
                    let reply = inspect(bytes.clone()).unwrap();
                    wire::decode_response(&reply, &bytes).unwrap();
                    assert!(
                        allocation::decode_response(&reply, allocation::digest(&bytes)).is_err()
                    );
                    // Reinspection is read-only and idempotent; it cannot consume
                    // a second VID or renew the original window.
                    assert_eq!(inspect(bytes.clone()).unwrap(), reply);
                    f.remote(NodeId::new(4))
                        .unwrap()
                        .server
                        .test_stream_allocation_admission(&bytes);
                    assert_eq!(f.observe(), before);
                    assert_eq!(f.shadows(), shadows);

                    for crossed in [
                        "session",
                        "index",
                        "vid",
                        "hash",
                        "data-pg",
                        "epoch",
                        "ec",
                        "size",
                        "stored-crc",
                        "payload-crc",
                    ] {
                        let mut segment = allocated.segment().clone();
                        match crossed {
                            "session" => {
                                segment.session_id = crate::tests::stream_session_id("crossed")
                            }
                            "index" => segment.segment_index += 1,
                            "vid" => {
                                segment.segment_vid =
                                    GenerationId::new(segment.segment_vid.get() + 1).unwrap()
                            }
                            "hash" => segment.segment_okh[0] ^= 1,
                            "data-pg" => segment.data_pg_id += 1,
                            "epoch" => {
                                segment.placement_cluster_epoch =
                                    ClusterEpoch::new(segment.placement_cluster_epoch.get() + 1)
                                        .unwrap()
                            }
                            "ec" => segment.ec_m += 1,
                            "size" => segment.size += 1,
                            "stored-crc" => segment.segment_crc64 ^= 1,
                            "payload-crc" => segment.payload_crc64 ^= 1,
                            _ => unreachable!(),
                        }
                        let error =
                            inspect(wire::encode_request(&allocation_bytes, &segment).unwrap())
                                .unwrap_err();
                        assert!(
                            diagnostic(&error).contains("allocation response segment"),
                            "{crossed}: {}",
                            diagnostic(&error)
                        );
                        assert_eq!(f.observe(), before);
                    }
                    for crossed in [
                        "expected-session",
                        "generation",
                        "prefix",
                        "topology",
                        "cutoff",
                    ] {
                        let mut bad = allocation::decode_request(&allocation_bytes).unwrap();
                        match crossed {
                            "expected-session" => {
                                match bad.preparation.expected_append_state.as_mut().unwrap() {
                                    crate::types::ExpectedStreamAppendState::PutObject {
                                        session,
                                        ..
                                    }
                                    | crate::types::ExpectedStreamAppendState::UploadPart {
                                        session,
                                        ..
                                    } => session.created_at += 1,
                                }
                            }
                            "generation" => {
                                match bad.preparation.expected_append_state.as_mut().unwrap() {
                                    crate::types::ExpectedStreamAppendState::PutObject {
                                        generation_id,
                                        ..
                                    } => {
                                        *generation_id =
                                            GenerationId::new(generation_id.get() + 1).unwrap()
                                    }
                                    crate::types::ExpectedStreamAppendState::UploadPart {
                                        part_generation_id,
                                        ..
                                    } => {
                                        *part_generation_id =
                                            GenerationId::new(part_generation_id.get() + 1).unwrap()
                                    }
                                }
                            }
                            "prefix" => bad.prefix.applied_log_index += 1,
                            "topology" => bad.window.route.topology[0] ^= 1,
                            "cutoff" => bad.window.route.deadline.portable_wall_valid_until_ms = 1,
                            _ => unreachable!(),
                        }
                        let bad = allocation::encode_request(&bad).unwrap();
                        assert!(
                            inspect(wire::encode_request(&bad, allocated.segment()).unwrap())
                                .is_err(),
                            "{crossed}"
                        );
                        assert_eq!(f.observe(), before);
                    }
                    let node = f.node(NodeId::new(4));
                    let window = owner.windows.last().unwrap();
                    for cursor in [window.start.get(), window.end.get() + 1] {
                        node.get_pg(f.context.id().pg_id().get()).unwrap().conn.execute(
                            "UPDATE stream_uploads SET next_segment_vid = ?1 WHERE session_id = ?2",
                            rusqlite::params![cursor, request.session_id],
                        ).unwrap();
                        // The local cursor is excluded from canonical state,
                        // but raw SQL invalidates the inspection cache. Recheck
                        // that state so this canary isolates the cursor binding.
                        node.get_pg(f.context.id().pg_id().get())
                            .unwrap()
                            .recover_complete_command_state(
                                PgStoreRecoveryContext::for_node(NodeId::new(4)),
                                &topology(),
                            )
                            .unwrap();
                        let changed = f.observe();
                        let error = inspect(bytes.clone()).unwrap_err();
                        assert!(
                            diagnostic(&error).contains("stream file consumed cursor"),
                            "{}",
                            diagnostic(&error)
                        );
                        assert_eq!(f.observe(), changed);
                    }
                    {
                        let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                        pg.conn.execute("UPDATE stream_uploads SET next_segment_vid = ?1 WHERE session_id = ?2", rusqlite::params![window.end.get(), request.session_id]).unwrap();
                        pg.begin_allocation_lifetime_replacement()
                            .unwrap()
                            .complete()
                            .unwrap();
                        pg.recover_complete_command_state(
                            PgStoreRecoveryContext::for_node(NodeId::new(4)),
                            &topology(),
                        )
                        .unwrap();
                    }
                    let changed = f.observe();
                    let error = inspect(bytes.clone()).unwrap_err();
                    assert!(
                        diagnostic(&error).contains("stream file issuer"),
                        "{}",
                        diagnostic(&error)
                    );
                    assert_eq!(f.observe(), changed);
                    drop(allocated);
                    assert_eq!(
                        f.observe(),
                        changed,
                        "inspection/drop cannot release custody"
                    );
                    assert_eq!(f.shadows(), shadows);
                    if !appended
                        && history == "installed"
                        && matches!(target, StagingTarget::StreamPut)
                    {
                        // This helper acquires its own server-side section to
                        // stall PG admission. Release ours first, deliberately.
                        session.test_close_primary_section();
                        f.remote(NodeId::new(4))
                            .unwrap()
                            .server
                            .test_stream_allocation_route_expiry(&bytes);
                        assert_eq!(f.observe(), changed);
                    }
                });
            }
        }
    }
}

#[test]
fn staging_stream_file_primary_unix_exact_allocation_and_read_only_rejections() {
    matrix(false);
}

#[test]
fn staging_stream_file_primary_tls_exact_allocation_and_read_only_rejections() {
    matrix(true);
}
