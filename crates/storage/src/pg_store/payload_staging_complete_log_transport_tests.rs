// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate framed transport, not installed RPC/recovery admission. Real
//! complete application supplies the rows; decoded contexts must remain intact.

use super::super::abandonment::source::PartFinalizationSource;
use super::complete_log_range::{apply, index, ordinary};
use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::{abandoned_command_log_bytes, metadata_command_log_hash};
use crate::node_runtime::pg_store::command_log::{
    CompleteCommandLogEntryKind, CompleteLogRangeRequest,
};
use crate::storage_rpc::{self as rpc, complete_log_range as wire};

fn encode_untrusted(entries: Vec<wire::WireEntry<'_>>) -> Vec<u8> {
    entries
        .into_iter()
        .try_fold(wire::Encoder::new(), |encoder, entry| encoder.push(entry))
        .unwrap()
        .finish()
}

#[test]
fn staging_complete_log_transport_preserves_all_contexts_and_exact_replay() {
    let _clock = test_time_override_guard(2_000);
    let topology = topology();
    for case in 0..18 {
        let (_dir, pg, context) = complete_application::setup(case);
        apply(&pg, &context);
        let id = context.id();
        let request = CompleteLogRangeRequest::new(
            id.pg_id(),
            id.cluster_epoch(),
            id.log_index(),
            id.log_index(),
            &topology,
        )
        .unwrap();
        let entries = pg
            .retained_complete_metadata_command_log_entries(
                0,
                id.cluster_epoch(),
                id.log_index(),
                id.log_index(),
                &topology,
            )
            .unwrap();
        let payload = request.encode_response(&entries).unwrap();
        let frame = rpc::encode_storage_rpc_frame(
            17,
            rpc::StorageRpcMessageKind::MetadataCommandRetainedLogEntries,
            &rpc::encode_storage_rpc_success_response(&payload),
        )
        .unwrap();
        let frame = rpc::decode_storage_rpc_frame(&frame).unwrap();
        let body = rpc::decode_storage_rpc_response_payload(&frame.payload)
            .unwrap()
            .unwrap();
        let mut decoded = request.decode_response(&body).unwrap();
        assert_eq!(
            request.encode_response(&decoded).unwrap(),
            payload,
            "case {case}"
        );
        let CompleteCommandLogEntryKind::Applied(decoded) = decoded.pop().unwrap().into_kind()
        else {
            panic!("case {case}")
        };
        assert_eq!(
            decoded.encode_command().unwrap().bytes(),
            context.encode_command().unwrap().bytes()
        );
        let writes = pg.conn.total_changes();
        apply(&pg, &decoded);
        assert_eq!(
            pg.conn.total_changes(),
            writes,
            "replay mutated case {case}"
        );
        assert!(
            rpc::decode_metadata_command_log_entry_range_response(
                &body,
                &crate::node_runtime::MetadataCommandDecodeAuthority::new()
            )
            .is_err(),
            "live V13 decoder accepted complete V14"
        );
    }
}

#[test]
fn staging_complete_log_transport_rejects_resealed_crossed_commands_and_old_contexts() {
    let _clock = test_time_override_guard(2_000);
    let topology = topology();
    let (_dir, pg, context) = complete_application::setup(5);
    apply(&pg, &context);
    let id = context.id();
    let request = CompleteLogRangeRequest::new(
        id.pg_id(),
        id.cluster_epoch(),
        id.log_index(),
        id.log_index(),
        &topology,
    )
    .unwrap();
    let entries = pg
        .retained_complete_metadata_command_log_entries(
            0,
            id.cluster_epoch(),
            id.log_index(),
            id.log_index(),
            &topology,
        )
        .unwrap();
    let good = request.encode_response(&entries).unwrap();
    let writes = pg.conn.total_changes();
    for change in [
        "pg", "epoch", "index", "v13", "context", "crc", "hash", "pre", "post",
    ] {
        let mut bytes = context.encode_command().unwrap().bytes().to_vec();
        if change == "v13" {
            bytes = ordinary(id).command_bytes();
        } else if change == "context" {
            *bytes.last_mut().unwrap() = 42;
        } else if matches!(change, "pg" | "epoch" | "index") {
            let crossed_id = MetadataCommandId::new(
                if change == "epoch" {
                    ClusterEpoch::new(id.cluster_epoch().get() + 1).unwrap()
                } else {
                    id.cluster_epoch()
                },
                if change == "pg" {
                    PgId::new(id.pg_id().get() ^ 1)
                } else {
                    id.pg_id()
                },
                if change == "index" {
                    index(id.log_index().get() + 1)
                } else {
                    id.log_index()
                },
            );
            bytes = ContextualCommand::decode_field(ordinary(crossed_id), &[1, 0, 0], &topology)
                .unwrap()
                .encode_command()
                .unwrap()
                .bytes()
                .to_vec();
        }
        let mut row = wire::decode(&good).unwrap().pop().unwrap();
        let checksum = checksum::crc64::checksum(&bytes);
        row.log_hash = metadata_command_log_hash(
            id.cluster_epoch(),
            id.pg_id(),
            id.log_index(),
            row.previous_log_hash.value(),
            checksum,
        );
        row.kind = wire::WireKind::Applied {
            checksum: if change == "crc" {
                checksum ^ 1
            } else {
                checksum
            },
            bytes: &bytes,
        };
        if change == "hash" {
            row.log_hash = row.previous_log_hash;
        }
        if change == "pre" {
            row.pre_state_digest = None;
        }
        if change == "post" {
            row.post_state_digest = None;
        }
        assert!(
            request
                .decode_response(&encode_untrusted(vec![row]))
                .is_err(),
            "accepted {change}"
        );
        assert_eq!(pg.conn.total_changes(), writes);
    }
}

#[test]
fn staging_complete_log_transport_checks_order_range_tombstones_and_adjacent_links() {
    let _clock = test_time_override_guard(2_000);
    let topology = topology();
    let (_dir, pg, context) = complete_application::setup(5);
    apply(&pg, &context);
    let id = context.id();
    let first = index(id.log_index().get() - 2);
    let request = CompleteLogRangeRequest::new(
        id.pg_id(),
        id.cluster_epoch(),
        first,
        id.log_index(),
        &topology,
    )
    .unwrap();
    let entries = pg
        .retained_complete_metadata_command_log_entries(
            0,
            id.cluster_epoch(),
            first,
            id.log_index(),
            &topology,
        )
        .unwrap();
    let good = request.encode_response(&entries).unwrap();
    assert_eq!(request.decode_response(&good).unwrap().len(), 3);
    for change in [
        "order",
        "duplicate",
        "range",
        "tombstone_crc",
        "tombstone_sha",
        "link",
    ] {
        let mut rows = wire::decode(&good).unwrap();
        match change {
            "order" => rows.swap(0, 1),
            "duplicate" => rows[1] = wire::decode(&good).unwrap().remove(0),
            "range" => rows[0].log_index -= 1,
            "tombstone_crc" | "tombstone_sha" => {
                let wire::WireKind::Abandoned { checksum, sha256 } = &mut rows[0].kind else {
                    panic!()
                };
                if change == "tombstone_crc" {
                    *checksum ^= 1;
                } else {
                    sha256[0] ^= 1;
                }
            }
            "link" => {
                let wire::WireKind::Abandoned { checksum, sha256 } = rows[1].kind else {
                    panic!()
                };
                let row_id = MetadataCommandId::new(
                    id.cluster_epoch(),
                    id.pg_id(),
                    index(rows[1].log_index),
                );
                let checksum = checksum::crc64::checksum(&abandoned_command_log_bytes(
                    row_id, checksum, sha256,
                ));
                rows[1].previous_log_hash = rows[0].previous_log_hash;
                rows[1].log_hash = metadata_command_log_hash(
                    id.cluster_epoch(),
                    id.pg_id(),
                    row_id.log_index(),
                    rows[1].previous_log_hash.value(),
                    checksum,
                );
                let isolated = CompleteLogRangeRequest::new(
                    id.pg_id(),
                    id.cluster_epoch(),
                    row_id.log_index(),
                    row_id.log_index(),
                    &topology,
                )
                .unwrap();
                let body = encode_untrusted(vec![rows.remove(1)]);
                assert_eq!(isolated.decode_response(&body).unwrap().len(), 1);
                rows.insert(1, wire::decode(&body).unwrap().remove(0));
                // Validate while `body` still owns the borrowed record.
                assert!(request.decode_response(&encode_untrusted(rows)).is_err());
                continue;
            }
            _ => unreachable!(),
        }
        assert!(
            request.decode_response(&encode_untrusted(rows)).is_err(),
            "accepted {change}"
        );
    }
    let mut sparse = wire::decode(&good).unwrap();
    sparse.remove(1);
    assert_eq!(
        request
            .decode_response(&encode_untrusted(sparse))
            .unwrap()
            .len(),
        2
    );
    assert!(request
        .decode_response(&wire::Encoder::new().finish())
        .unwrap()
        .is_empty());
}

#[test]
fn staging_complete_log_transport_validates_scope_topology_and_truncated_framing() {
    let _clock = test_time_override_guard(2_000);
    let topology = topology();
    let (_dir, pg, context) = complete_application::setup(0);
    apply(&pg, &context);
    let id = context.id();
    let entries = pg
        .retained_complete_metadata_command_log_entries(
            0,
            id.cluster_epoch(),
            id.log_index(),
            id.log_index(),
            &topology,
        )
        .unwrap();
    let request = CompleteLogRangeRequest::new(
        id.pg_id(),
        id.cluster_epoch(),
        id.log_index(),
        id.log_index(),
        &topology,
    )
    .unwrap();
    let bytes = request.encode_response(&entries).unwrap();
    for end in 0..bytes.len() {
        assert!(
            request.decode_response(&bytes[..end]).is_err(),
            "accepted truncation {end}"
        );
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(request.decode_response(&trailing).is_err());
    let foreign = PgTopology::new(&(0..16).collect::<Vec<_>>()).unwrap();
    let crossed = CompleteLogRangeRequest::new(
        id.pg_id(),
        id.cluster_epoch(),
        id.log_index(),
        id.log_index(),
        &foreign,
    )
    .unwrap();
    assert!(crossed.decode_response(&bytes).is_err());
    assert!(crossed.encode_response(&entries).is_err());
    for (first, last) in [(2, 1), (1, 32), (u64::MAX, u64::MAX)] {
        assert!(CompleteLogRangeRequest::new(
            id.pg_id(),
            id.cluster_epoch(),
            index(first),
            index(last),
            &topology
        )
        .is_err());
    }
    assert!(CompleteLogRangeRequest::new(
        PgId::new(99),
        id.cluster_epoch(),
        index(1),
        index(1),
        &topology
    )
    .is_err());
}

#[test]
fn staging_complete_log_transport_v37_v14_context_v1_composite() {
    let _clock = test_time_override_guard(2_000);
    let topology = topology();
    let (_dir, pg, context) = complete_application::setup(5);
    apply(&pg, &context);
    let id = context.id();
    let first = index(id.log_index().get() - 1);
    let entries = pg
        .retained_complete_metadata_command_log_entries(
            0,
            id.cluster_epoch(),
            first,
            id.log_index(),
            &topology,
        )
        .unwrap();
    let request = CompleteLogRangeRequest::new(
        id.pg_id(),
        id.cluster_epoch(),
        first,
        id.log_index(),
        &topology,
    )
    .unwrap();
    let payload = request.encode_response(&entries).unwrap();
    let frame = rpc::encode_storage_rpc_frame(
        17,
        rpc::StorageRpcMessageKind::MetadataCommandRetainedLogEntries,
        &rpc::encode_storage_rpc_success_response(&payload),
    )
    .unwrap();
    assert_eq!(
        frame,
        crate::tests::frozen_hex_fixture(include_str!(
            "testdata/complete_log_range_rpc_v37_command_v14_context_v1.hex"
        ))
    );
    assert_eq!(
        request
            .encode_response(&request.decode_response(&payload).unwrap())
            .unwrap(),
        payload
    );
}

#[test]
fn staging_complete_log_transport_retains_maximal_source_above_current_item_cap() {
    use crate::metadata_command::{
        MetadataCommandEnvelope, MetadataCommandPayload, MAX_METADATA_COMMAND_BYTES,
    };
    use crate::node_runtime::pg_store::PgStore;
    let topology = topology();
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = part::initialize(&pg, 0);
    let source = part::command(&pg, &owner);
    let cleanup = abandonment::cleanup(&pg, &owner, &source);
    let MetadataCommandPayload::CommitStreamPart(mut payload) = source.payload().clone() else {
        panic!()
    };
    // Capacity-only canonical specimen, not an S3 publication/apply fixture.
    payload.part.etag.resize(
        payload.part.etag.len() + MAX_METADATA_COMMAND_BYTES - source.command_bytes().len(),
        0x41,
    );
    let source = MetadataCommandEnvelope::new(
        source.id(),
        MetadataCommandPayload::CommitStreamPart(payload),
    );
    let context = ContextualCommand::abandoned_part(
        PartFinalizationSource::capture_for_test(&source)
            .unwrap()
            .bind_cleanup(cleanup, &topology)
            .unwrap(),
        &topology,
    )
    .unwrap();
    let encoded = context.encode_command().unwrap();
    assert!(encoded.bytes().len() > MAX_METADATA_COMMAND_BYTES);
    let id = context.id();
    let row = wire::WireEntry {
        log_index: id.log_index().get(),
        previous_log_hash: crate::control_plane::MetadataCommandLogHash::for_test(0),
        log_hash: metadata_command_log_hash(
            id.cluster_epoch(),
            id.pg_id(),
            id.log_index(),
            0,
            encoded.checksum_crc64(),
        ),
        pre_state_digest: Some(crate::control_plane::CanonicalStateDigest::for_test(1)),
        post_state_digest: Some(crate::control_plane::CanonicalStateDigest::for_test(2)),
        kind: wire::WireKind::Applied {
            checksum: encoded.checksum_crc64(),
            bytes: encoded.bytes(),
        },
    };
    let body = encode_untrusted(vec![row]);
    let frame = rpc::encode_storage_rpc_frame(
        17,
        rpc::StorageRpcMessageKind::MetadataCommandRetainedLogEntries,
        &rpc::encode_storage_rpc_success_response(&body),
    )
    .unwrap();
    let frame = rpc::decode_storage_rpc_frame(&frame).unwrap();
    assert_eq!(
        rpc::decode_storage_rpc_response_payload(&frame.payload)
            .unwrap()
            .unwrap(),
        body
    );
    let request = CompleteLogRangeRequest::new(
        id.pg_id(),
        id.cluster_epoch(),
        id.log_index(),
        id.log_index(),
        &topology,
    )
    .unwrap();
    let decoded = request.decode_response(&body).unwrap();
    assert_eq!(request.encode_response(&decoded).unwrap(), body);
    let CompleteCommandLogEntryKind::Applied(received) =
        decoded.into_iter().next().unwrap().into_kind()
    else {
        panic!()
    };
    assert_eq!(received.encode_command().unwrap().bytes(), encoded.bytes());
    assert!(rpc::decode_metadata_command_log_entry_range_response(
        &body,
        &crate::node_runtime::MetadataCommandDecodeAuthority::new()
    )
    .is_err());
    pg.conn.execute_batch("ROLLBACK").unwrap();
}
