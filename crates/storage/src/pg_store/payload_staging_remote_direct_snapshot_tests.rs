// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Installed authenticated Unix/TLS receiver tests. Receipt claims describe real
//! fixture ciphertext; this read-only boundary does not assert physical effects.
use super::*;
#[path = "payload_staging_remote_direct_preparation_tests.rs"]
mod terminal;
use crate::node_runtime::traits::PgMetadataStore;
use crate::storage_node_server::StorageNodeServer;
use crate::storage_rpc::{
    self as rpc, complete_direct_snapshot as wire, complete_staging_ack as ack,
    StorageRpcErrorCode as Code, StorageRpcMessageKind as Kind,
};

fn with_request(
    tls: bool,
    action: impl FnOnce(&Mixed, &UnixStorageNodeMetadataCommandSession, &[u8]),
) {
    let (_dir, f, proof) = allocation_remote::setup(tls, StagingTarget::DirectPut, false);
    f.remote(NodeId::new(4))
        .unwrap()
        .server
        .test_receiver_route_validity(crate::clock::current_time_millis() + 300_000);
    run_terminal(
        &f,
        proof.as_ref(),
        "installed",
        |f, primary, mut terminal| {
            primary.retire(&mut terminal);
            let confirmed = primary
                .confirm(terminal, deadline(), |_| {})
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let Primary::Remote(_, session) = primary else {
                unreachable!()
            };
            let owner = {
                let node = f.node(NodeId::new(4));
                let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                let read = pg.command_build_snapshot().unwrap();
                let owner = append::load(&pg, &fresh_capture::template(StagingTarget::DirectPut));
                read.finish().unwrap();
                owner
            };
            let field = f.context.encode_field().unwrap();
            let shards = super::super::files::shards(&owner, b"data");
            let request = wire::encode_request(&ack::Request {
                receiver: rpc::complete_settled_prefix::Request {
                    node: NodeId::new(4),
                    pg: f.context.id().pg_id(),
                    epoch: f.context.id().cluster_epoch(),
                    topology: rpc::complete_settled_prefix::topology_digest(&topology()),
                    deadline: rpc::StorageRpcOperationDeadline::from_instant(deadline()),
                },
                provenance: ack::Provenance::Direct {
                    descriptor: &field[7..],
                    logical_size: 4,
                },
                receipts: shards
                    .iter()
                    .enumerate()
                    .map(|(i, bytes)| ack::Receipt {
                        shard_index: i as u8,
                        ack: crate::WriteAck {
                            stored_size: bytes.len() as u64,
                            crc64: checksum::crc64::checksum(bytes),
                        },
                    })
                    .collect(),
            })
            .unwrap();
            action(f, session, request.bytes());
            drop(confirmed);
        },
    );
}

#[test]
fn staging_remote_direct_snapshot_captures_exact_empty_and_existing_subject_unix_tls() {
    for tls in [false, true] {
        with_request(tls, |f, session, bytes| {
            let request = wire::encode_request(&wire::decode_request(bytes).unwrap()).unwrap();
            for existing in [false, true] {
                let node = f.node(NodeId::new(4));
                let id = f.context.id();
                let owner = fresh_capture::template(StagingTarget::DirectPut);
                if existing {
                    let key = (0..1000)
                        .map(|i| crate::tests::object_key(format!("snapshot-crossed-{i}")))
                        .find(|key| {
                            key != &owner.subject.key
                                && topology().object_pg_for(&owner.subject.bucket, key)
                                    == id.pg_id().get()
                        })
                        .unwrap();
                    let pg = node.get_pg(id.pg_id().get()).unwrap();
                    for (key, etag) in [(&owner.subject.key, 42), (&key, 99)] {
                        // Metadata-only predecessor, not a payload/decryption fixture.
                        pg.put_object_with_segments(
                            &crate::PutLiveObjectReq {
                                bucket: owner.subject.bucket.clone(),
                                key: key.clone(),
                                version_id: crate::VersionId::Null,
                                owner: crate::OwnerIdentity::from_principal("old-owner"),
                                acl_grants: crate::AclGrants::default(),
                                public_read: false,
                                generation_id: crate::GenerationId::MIN,
                                size: 0,
                                etag: crate::ObjectEtag::single_part(etag),
                                ec: owner.windows[0].geometry.ec,
                                layout: crate::ObjectLayout::Standard,
                                tags: None,
                                metadata_blob: None,
                                system_metadata_blob: None,
                                object_lock: crate::ObjectLockState::default(),
                                encryption: crate::tests::metadata_only_test_encryption(),
                            },
                            &[],
                        )
                        .unwrap();
                    }
                    complete_application::anchor(&pg, id);
                    pg.recover_complete_command_state(
                        PgStoreRecoveryContext::for_node(NodeId::new(4)),
                        &topology(),
                    )
                    .unwrap();
                }
                let before = f.observe();
                let shadows = f.shadows();
                for _ in 0..2 {
                    let reply = session
                        .test_direct_snapshot_request(bytes.to_vec())
                        .unwrap()
                        .unwrap();
                    let snapshot = rpc::decode_direct_put_commit_snapshot_response(
                        request.decode_response(&reply).unwrap(),
                    )
                    .unwrap()
                    .snapshot;
                    assert_eq!(
                        snapshot.auth_snapshot.existing_etag,
                        existing.then(|| crate::ObjectEtag::single_part(42).format())
                    );
                    if existing {
                        assert_eq!(snapshot.current.as_ref().unwrap().key(), &owner.subject.key);
                        assert_eq!(snapshot.current, snapshot.stale_payload_source);
                    } else {
                        assert!(snapshot.current.is_none());
                        assert!(snapshot.stale_payload.is_none());
                    }
                    assert_eq!(f.observe(), before);
                    assert_eq!(f.shadows(), shadows);
                    assert!(node.get_pg(id.pg_id().get()).unwrap().conn.is_autocommit());
                }
            }
        });
    }
}

#[test]
fn staging_remote_direct_snapshot_rejects_crossed_route_descriptor_and_receipts() {
    for tls in [false, true] {
        with_request(tls, |f, session, bytes| {
            let before = f.observe();
            f.remote(NodeId::new(4))
                .unwrap()
                .server
                .test_direct_snapshot_admission(bytes);
            for case in [
                "node",
                "epoch",
                "pg",
                "topology",
                "descriptor",
                "count",
                "length",
                "size",
                "expiry",
            ] {
                let mut request = wire::decode_request(bytes).unwrap();
                let ack::Provenance::Direct {
                    descriptor,
                    logical_size,
                } = request.provenance
                else {
                    unreachable!()
                };
                let mut crossed = descriptor.to_vec();
                match case {
                    "node" => request.receiver.node = NodeId::new(0),
                    "epoch" => request.receiver.epoch = ClusterEpoch::new(99).unwrap(),
                    "pg" => request.receiver.pg = PgId::new(0),
                    "topology" => request.receiver.topology[0] ^= 1,
                    "descriptor" => {
                        crossed[10] = 99;
                        request.provenance = ack::Provenance::Direct {
                            descriptor: &crossed,
                            logical_size,
                        };
                    }
                    "count" => {
                        request.receipts.pop();
                    }
                    "length" => request
                        .receipts
                        .iter_mut()
                        .for_each(|r| r.ack.stored_size += 1),
                    "size" => {
                        request.provenance = ack::Provenance::Direct {
                            descriptor,
                            logical_size: u64::MAX,
                        }
                    }
                    "expiry" => request.receiver.deadline.portable_wall_valid_until_ms = 1,
                    _ => unreachable!(),
                }
                let request = wire::encode_request(&request).unwrap();
                let error = session
                    .test_direct_snapshot_request(request.bytes().to_vec())
                    .unwrap()
                    .unwrap_err();
                let expected = match case {
                    "node" => Code::UnknownNode,
                    "epoch" => Code::StaleShardLocation,
                    "pg" | "topology" | "descriptor" => Code::PayloadDecode,
                    "count" | "length" | "size" => Code::MetadataCommandContention,
                    "expiry" => Code::TransportTimeout,
                    _ => unreachable!(),
                };
                assert_eq!(error.code, expected, "{case}: {error:?}");
                assert_eq!(f.observe(), before, "{case}");
            }
            session
                .test_direct_snapshot_request(bytes.to_vec())
                .unwrap()
                .unwrap();
        });
    }
}

#[test]
fn staging_remote_direct_snapshot_rechecks_durable_reservation_window_and_issuer() {
    for tls in [false, true] {
        for case in ["reservation", "window", "issuer"] {
            with_request(tls, |f, session, bytes| {
                session
                    .test_direct_snapshot_request(bytes.to_vec())
                    .unwrap()
                    .unwrap();
                let node = f.node(NodeId::new(4));
                {
                    let pg = node.get_pg(f.context.id().pg_id().get()).unwrap();
                    match case {
                        "reservation" => {
                            pg.conn
                                .execute_batch("DELETE FROM object_generation_reservations")
                                .unwrap();
                        }
                        "window" => {
                            pg.conn.execute_batch("UPDATE payload_staging_windows SET logical_bound = logical_bound - 1").unwrap();
                        }
                        "issuer" => {
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
                        _ => unreachable!(),
                    }
                }
                let before = f.observe();
                let error = session
                    .test_direct_snapshot_request(bytes.to_vec())
                    .unwrap()
                    .unwrap_err();
                assert_eq!(
                    error.code,
                    Code::MetadataCommandContention,
                    "{case}: {error:?}"
                );
                if case == "issuer" {
                    assert!(error.message.contains("issuer"), "{error:?}");
                }
                assert_eq!(f.observe(), before);
            });
        }
    }
}

#[test]
fn staging_remote_direct_snapshot_rechecks_both_cutoffs_after_read_teardown() {
    for tls in [false, true] {
        for server_route in [false, true] {
            with_request(tls, |f, session, bytes| {
                let server = &f.remote(NodeId::new(4)).unwrap().server;
                server.set_response_frame_test_hook(Arc::new(move |kind, _| {
                    if kind == Kind::StagingDirectSnapshot {
                        StorageNodeServer::test_expire_direct_snapshot_after_capture(server_route);
                    }
                }));
                let before = f.observe();
                session
                    .test_direct_snapshot_request(bytes.to_vec())
                    .unwrap()
                    .unwrap();
                let error = session
                    .test_direct_snapshot_request(bytes.to_vec())
                    .unwrap()
                    .unwrap_err();
                assert_eq!(
                    error.code,
                    if server_route {
                        Code::StaleShardLocation
                    } else {
                        Code::TransportTimeout
                    },
                    "{error:?}"
                );
                assert_eq!(f.observe(), before);
                assert!(f
                    .node(NodeId::new(4))
                    .get_pg(f.context.id().pg_id().get())
                    .unwrap()
                    .conn
                    .is_autocommit());
            });
        }
    }
}
