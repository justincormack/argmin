// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Installed authenticated Unix/TLS read-only preparation. Receipt claims here
//! describe fixture ciphertext, not file/ack effects; owning sender integration
//! must separately retain successful physical acknowledgement custody.
use super::*;
#[path = "payload_staging_remote_append_install_receiver_tests.rs"]
mod installation;
use crate::metadata_command::{MetadataCommandEnvelope, MetadataCommandPayload};
use crate::node_runtime::server::StorageNodeServer;
use crate::storage_rpc::{
    complete_staging_ack as ack, complete_stream_append_prepare as prepare,
    complete_stream_file_primary as files,
};

fn with_request(
    tls: bool,
    target: StagingTarget,
    appended: bool,
    action: impl FnOnce(
        &Mixed,
        &UnixStorageNodeMetadataCommandSession,
        &[u8],
        &crate::StreamUploadSegmentRecord,
    ),
) {
    let (_dir, f, proof) = setup(tls, target.clone(), appended);
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
            let (owner, mut request, expected) = inputs(f, target);
            let shards = super::super::super::files::shards(&owner, b"data");
            let window = owner.windows.last().unwrap();
            let mut stored = shards[..usize::from(window.geometry.ec.k)].concat();
            stored.truncate(window.geometry.codec.stored_size(4).unwrap() as usize);
            request.segment_crc64 = checksum::crc64::checksum(&stored);
            request.payload_crc64 = checksum::crc64::checksum(b"data");
            let allocation = confirmed.test_stream_allocation_bytes(&request, &expected);
            let Primary::Remote(client, session) = primary else {
                unreachable!()
            };
            let allocated = confirmed
                .allocate_stream_remote(client, session, &request, &expected)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            let inspection = files::encode_request(&allocation, allocated.segment()).unwrap();
            let request = ack::Request {
                receiver: wire::decode_request(&allocation).unwrap().window.route,
                provenance: ack::Provenance::Stream {
                    inspection: &inspection,
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
            };
            let bytes = prepare::encode_request(&request).unwrap();
            action(f, session, bytes.bytes(), allocated.segment());
        },
    );
}

#[test]
fn staging_remote_append_receiver_captures_exact_command_without_mutation_unix_tls() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            for appended in [false, true] {
                with_request(
                    tls,
                    target.clone(),
                    appended,
                    |f, session, bytes, segment| {
                        let before = f.observe();
                        let shadows = f.shadows();
                        let request =
                            prepare::encode_request(&prepare::decode_request(bytes).unwrap())
                                .unwrap();
                        for _ in 0..2 {
                            let reply = session
                                .test_stream_append_prepare_request(bytes.to_vec())
                                .unwrap()
                                .unwrap();
                            let returned = request.decode_response(&reply).unwrap();
                            let context = ContextualCommand::decode_command(
                                returned.command,
                                returned.checksum,
                                &topology(),
                            )
                            .unwrap();
                            assert_eq!(context.id().pg_id(), f.context.id().pg_id());
                            assert_eq!(
                                context.id().cluster_epoch(),
                                f.context.id().cluster_epoch()
                            );
                            assert_eq!(
                                context.id().log_index().get(),
                                f.context.id().log_index().get() + 1
                            );
                            let claims = prepare::decode_request(bytes).unwrap();
                            let ack::Provenance::Stream { inspection } = claims.provenance else {
                                unreachable!()
                            };
                            let file = files::decode_request(inspection).unwrap();
                            let allocation = wire::decode_request(file.allocation).unwrap();
                            let Some(expected) = allocation.preparation.expected_append_state
                            else {
                                unreachable!()
                            };
                            let record = match expected {
                                ExpectedStreamAppendState::PutObject { session, .. }
                                | ExpectedStreamAppendState::UploadPart { session, .. } => session,
                            };
                            let keys: Vec<_> = claims
                                .receipts
                                .iter()
                                .map(|r| {
                                    (
                                        crate::ShardKey::new(
                                            &segment.segment_okh,
                                            segment.segment_vid.get(),
                                            r.shard_index,
                                        ),
                                        r.ack,
                                    )
                                })
                                .collect();
                            let mut expected = segment.clone();
                            expected.shard_integrity = Some(crate::types::SegmentShardIntegrityWitness::from_ordered_shard_acks(
                            segment.data_pg_id, EcShape { k: segment.ec_k, m: segment.ec_m }, &segment.segment_okh, segment.segment_vid,
                            record.encryption.segment_stored_size(segment.size).unwrap(), segment.segment_crc64,
                            &keys.iter().map(|(key, ack)| (key, *ack)).collect::<Vec<_>>(),
                        ).unwrap().durable_record());
                            context.assert_command_for_test(&MetadataCommandEnvelope::new(
                                context.id(),
                                MetadataCommandPayload::AppendStreamSegment(Box::new(
                                    crate::metadata_command::AppendStreamSegmentCommand {
                                        bucket: record.bucket,
                                        key: record.key,
                                        target: record.target,
                                        segment: expected,
                                    },
                                )),
                            ));
                            assert_eq!(f.observe(), before);
                            assert_eq!(f.shadows(), shadows);
                        }
                    },
                );
            }
        }
    }
}

#[test]
fn staging_remote_append_receiver_rejects_crossed_prefix_session_and_placement_without_effects() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            with_request(tls, target, false, |f, session, bytes, _| {
                let before = f.observe();
                for fault in [
                    "prefix",
                    "session",
                    "generation",
                    "segment-hash",
                    "segment-vid",
                    "data-pg",
                ] {
                    let mut request = prepare::decode_request(bytes).unwrap();
                    let ack::Provenance::Stream { inspection } = request.provenance else {
                        unreachable!()
                    };
                    let mut file = files::decode_request(inspection).unwrap();
                    let mut allocation = wire::decode_request(file.allocation).unwrap();
                    match fault {
                        "prefix" => allocation.prefix.applied_log_index += 1,
                        "session" => match allocation
                            .preparation
                            .expected_append_state
                            .as_mut()
                            .unwrap()
                        {
                            ExpectedStreamAppendState::PutObject { session, .. }
                            | ExpectedStreamAppendState::UploadPart { session, .. } => {
                                session.created_at += 1
                            }
                        },
                        "generation" => match allocation
                            .preparation
                            .expected_append_state
                            .as_mut()
                            .unwrap()
                        {
                            ExpectedStreamAppendState::PutObject { generation_id, .. } => {
                                *generation_id = GenerationId::new(generation_id.get() + 1).unwrap()
                            }
                            ExpectedStreamAppendState::UploadPart {
                                part_generation_id, ..
                            } => {
                                *part_generation_id =
                                    GenerationId::new(part_generation_id.get() + 1).unwrap()
                            }
                        },
                        "segment-hash" => file.segment.segment_okh[0] ^= 1,
                        "segment-vid" => {
                            file.segment.segment_vid =
                                GenerationId::new(file.segment.segment_vid.get() + 1).unwrap()
                        }
                        "data-pg" => {
                            assert_ne!(file.segment.data_pg_id, 0);
                            file.segment.data_pg_id = 0;
                        }
                        _ => unreachable!(),
                    }
                    let allocation = wire::encode_request(&allocation).unwrap();
                    let inspection = files::encode_request(&allocation, &file.segment).unwrap();
                    request.provenance = ack::Provenance::Stream {
                        inspection: &inspection,
                    };
                    let crossed = prepare::encode_request(&request).unwrap();
                    let error = session
                        .test_stream_append_prepare_request(crossed.bytes().to_vec())
                        .unwrap()
                        .unwrap_err();
                    assert_eq!(
                        error.code,
                        crate::storage_rpc::StorageRpcErrorCode::MetadataCommandContention,
                        "{fault}: {error:?}"
                    );
                    assert_eq!(f.observe(), before, "{fault}");
                }
            });
        }
    }
}

#[test]
fn staging_remote_append_receiver_requires_admission_and_rechecks_expiry_after_capture() {
    for tls in [false, true] {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            for server_route in [false, true] {
                with_request(tls, target.clone(), false, |f, session, bytes, _| {
                    let before = f.observe();
                    let server = &f.remote(NodeId::new(4)).unwrap().server;
                    server.test_stream_append_admission(bytes);
                    assert_eq!(f.observe(), before);
                    let mut request = prepare::decode_request(bytes).unwrap();
                    request.receiver.deadline.portable_wall_valid_until_ms =
                        crate::clock::current_time_millis() + 10_000;
                    let request = prepare::encode_request(&request).unwrap();
                    let bytes = request.bytes();
                    server.set_response_frame_test_hook(Arc::new(move |kind, _| {
                        if kind == Kind::StagingStreamAppendPrepare {
                            StorageNodeServer::test_expire_stream_append_after_capture(
                                server_route,
                            );
                        }
                    }));
                    // A successful capture arms expiry on this exact connection's
                    // next request, after its read snapshot has completed.
                    session
                        .test_stream_append_prepare_request(bytes.to_vec())
                        .unwrap()
                        .unwrap();
                    let error = session
                        .test_stream_append_prepare_request(bytes.to_vec())
                        .unwrap()
                        .unwrap_err();
                    assert_eq!(
                        error.code,
                        if server_route {
                            crate::storage_rpc::StorageRpcErrorCode::StaleShardLocation
                        } else {
                            crate::storage_rpc::StorageRpcErrorCode::TransportTimeout
                        },
                        "{error:?}"
                    );
                    assert!(
                        error.message.contains("cutoff") || error.message.contains("expired"),
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
}
