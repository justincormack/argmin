// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Installed, authenticated scalar reads with explicit complete-grammar selection.
//! Hash observations never authorize application or prove a complete prefix.

use super::complete_log_range::{index, ordinary};
use super::complete_log_rpc::{budget_hint, installed, installed_at_epoch, seed_context, Running};
use super::*;
use crate::metadata_command::metadata_command_log_hash;
use crate::node_runtime::clients::UnixStorageNodeClient;
use crate::node_runtime::pg_store::{CompleteLogHashPage, CompleteLogHashRangeRequest};
use crate::storage_rpc::{self as rpc, StorageRpcErrorCode, StorageRpcMessageKind};
use crate::{PgState, StoreError};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

const KIND: StorageRpcMessageKind = StorageRpcMessageKind::MetadataCommandRetainedLogHashes;

fn read(
    client: &UnixStorageNodeClient,
    pg: PgId,
    first: u64,
    last: u64,
) -> Result<CompleteLogHashPage, StoreError> {
    client.complete_metadata_log_hash_page_until(
        pg,
        ClusterEpoch::new(5).unwrap(),
        index(first),
        index(last),
        Instant::now() + Duration::from_secs(30),
    )
}

#[test]
fn staging_complete_hash_rpc_validates_all_contexts_on_active_peering_and_historical_routes() {
    for (state, historical) in [
        (PgState::Active, false),
        (PgState::Peering, false),
        (PgState::Peering, true),
    ] {
        for case in 0..3 {
            let dir = test_util::tempdir();
            let epoch = if case == 2 { 6 } else { 5 };
            let (server, client, config) = installed_at_epoch(
                &dir,
                state,
                ClusterEpoch::new(epoch + u64::from(historical)).unwrap(),
                true,
            );
            let node = server.test_storage_node();
            let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
            let (id, expected, writes) = {
                let pg = node.get_pg(pg_id.get()).unwrap();
                let context = seed_context(&pg, case);
                let id = context.id();
                let expected = pg
                    .retained_complete_metadata_command_log_hashes(
                        4,
                        id.cluster_epoch(),
                        index(1),
                        id.log_index(),
                        &topology(),
                    )
                    .unwrap();
                (id, expected, pg.conn.total_changes())
            };
            let _running = Running::start(server, config.socket_path);
            let page = client
                .complete_metadata_log_hash_page_until(
                    pg_id,
                    id.cluster_epoch(),
                    index(1),
                    id.log_index(),
                    Instant::now() + Duration::from_secs(30),
                )
                .unwrap();
            assert_eq!(page.covered_range(), (index(1), id.log_index()));
            assert_eq!(page.len() as u64, id.log_index().get());
            assert_eq!(page.entries(), expected);
            if case == 0 && !historical {
                // A scalar page has its own 4096-position cap, independent of
                // the 31-entry body cap. Missing suffixes are not fabricated.
                let page = read(&client, pg_id, 1, 4096).unwrap();
                assert_eq!(page.covered_range(), (index(1), index(4096)));
                assert_eq!(page.entries(), expected);
                let first_missing = id.log_index().get() + 1;
                let last_missing = first_missing + 4095;
                let empty = read(&client, pg_id, first_missing, last_missing).unwrap();
                assert_eq!(
                    empty.covered_range(),
                    (index(first_missing), index(last_missing))
                );
                assert_eq!(empty.len(), 0);
            }
            assert_eq!(
                node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
                writes
            );
        }
    }
}

#[test]
fn staging_complete_hash_rpc_byte_budget_reduces_the_interval_without_skipping_rows() {
    let dir = test_util::tempdir();
    let (server, client, config) = installed(&dir, PgState::Active, false, true);
    let node = server.test_storage_node();
    let bucket = fixture(StagingTarget::DirectPut).subject.bucket;
    let pg_id = PgId::new(topology().bucket_pg_for(&bucket));
    let (writes, payload_reads) = {
        let mut pg = node.get_pg(pg_id.get()).unwrap();
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let mut previous = 0;
        for position in 1..=17 {
            previous = complete_hash_range::seed_log_row(
                &pg,
                MetadataCommandId::new(config.cluster_epoch, pg_id, index(position)),
                1024 * 1024,
                previous,
            );
        }
        pg.conn.execute_batch("COMMIT").unwrap();
        (
            pg.conn.total_changes(),
            complete_hash_range::count_payload_reads(&mut pg),
        )
    };
    let responses = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&responses);
    server.set_response_frame_test_hook(Arc::new(move |kind, _| {
        if kind == KIND {
            count.fetch_add(1, Ordering::SeqCst);
        }
    }));
    let _running = Running::start(server, config.socket_path);
    let first = read(&client, pg_id, 1, 4096).unwrap();
    assert_eq!(responses.load(Ordering::SeqCst), 2);
    // Count actual command loads: exactly 16 MiB in the initial inspection,
    // then 16 MiB once more in its known-safe prefix. Not 144 MiB by halving.
    assert_eq!(payload_reads.load(Ordering::SeqCst), 32);
    assert_eq!(first.covered_range(), (index(1), index(16)));
    let second = read(&client, pg_id, 17, 4096).unwrap();
    assert_eq!(second.covered_range(), (index(17), index(4096)));
    assert_eq!(responses.load(Ordering::SeqCst), 3);
    assert_eq!(payload_reads.load(Ordering::SeqCst), 33);
    let rows: Vec<_> = first.entries().iter().chain(second.entries()).collect();
    assert_eq!(
        rows.iter().map(|r| r.log_index).collect::<Vec<_>>(),
        (1..=17).collect::<Vec<_>>()
    );
    for pair in rows.windows(2) {
        assert_eq!(pair[0].log_hash, pair[1].previous_log_hash);
    }
    assert_eq!(
        node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
        writes
    );
}

#[test]
fn staging_complete_hash_rpc_retries_only_a_structured_budget_hint() {
    for (code, mixed, expected) in [
        (
            StorageRpcErrorCode::RetainedLogValidationBudgetExceeded,
            false,
            1,
        ),
        (StorageRpcErrorCode::ResourceExhausted, false, 1),
        (StorageRpcErrorCode::Internal, false, 1),
        (StorageRpcErrorCode::PayloadDecode, false, 1),
        (StorageRpcErrorCode::ResourceExhausted, true, 2),
    ] {
        let dir = test_util::tempdir();
        let (server, client, config) = installed(&dir, PgState::Active, false, true);
        let responses = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&responses);
        server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
            if kind == KIND {
                let attempt = count.fetch_add(1, Ordering::SeqCst);
                if mixed && attempt == 0 {
                    frame.payload = budget_hint(kind, PgId::new(0), 1, 4096, 16);
                    return;
                }
                frame.payload =
                    rpc::encode_storage_rpc_error_response(&rpc::StorageRpcErrorResponse {
                        code,
                        message: "budget exhausted".into(),
                    })
                    .unwrap();
            }
        }));
        let _running = Running::start(server, config.socket_path);
        let error = read(&client, PgId::new(0), 1, 4096).unwrap_err();
        if code == StorageRpcErrorCode::ResourceExhausted {
            assert!(
                matches!(error, StoreError::StorageRpcResourceExhausted { .. }),
                "{error:?}"
            );
        } else {
            assert!(
                matches!(error, StoreError::StorageRpc { failure, .. } if failure == code),
                "{error:?}"
            );
        }
        assert_eq!(responses.load(Ordering::SeqCst), expected);
    }
}

#[test]
fn staging_complete_hash_rpc_rejects_signed_malformed_pages_and_broken_authentication() {
    for fault in [
        "range",
        "reduced",
        "count",
        "order",
        "duplicate",
        "link",
        "genesis",
        "truncated",
        "trailing",
        "auth",
    ] {
        let dir = test_util::tempdir();
        let (server, client, config) = installed(&dir, PgState::Active, false, true);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        {
            let pg = node.get_pg(pg_id.get()).unwrap();
            for position in 1..=3 {
                let command = ordinary(MetadataCommandId::new(
                    config.cluster_epoch,
                    pg_id,
                    index(position),
                ));
                pg.record_metadata_command_abandoned(4, &command).unwrap();
            }
        }
        let responses = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&responses);
        server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
            if kind != KIND {
                return;
            }
            let attempt = count.fetch_add(1, Ordering::SeqCst);
            if fault == "reduced" && attempt == 0 {
                frame.payload = budget_hint(kind, pg_id, 1, 3, 2);
                return;
            }
            let body = rpc::decode_storage_rpc_response_payload(&frame.payload)
                .unwrap()
                .unwrap();
            let mut response = rpc::decode_metadata_command_log_hash_range_response(&body).unwrap();
            match fault {
                "range" => response.entries[2].log_index = 4,
                // The retry covers 1..=2, not the original 1..=3. This row
                // would otherwise pass count, ordering and nonadjacent links.
                "reduced" => response.entries.last_mut().unwrap().log_index = 3,
                "count" => response.entries.push(response.entries[2]),
                "order" => response.entries.swap(1, 2),
                "duplicate" => response.entries[1] = response.entries[0],
                "link" => {
                    response.entries[1].previous_log_hash = response.entries[0].previous_log_hash
                }
                "genesis" => response.entries[0].previous_log_hash = response.entries[0].log_hash,
                "truncated" | "trailing" | "auth" => {}
                _ => unreachable!(),
            }
            let mut body = rpc::encode_metadata_command_log_hash_range_response(&response).unwrap();
            if fault == "truncated" {
                body.pop();
            }
            if fault == "trailing" {
                body.push(0);
            }
            frame.payload = rpc::encode_storage_rpc_success_response(&body);
        }));
        if fault == "auth" {
            server.set_response_envelope_test_hook(Arc::new(|kind, bytes| {
                if kind == KIND {
                    *bytes.last_mut().unwrap() ^= 1;
                }
            }));
        }
        let _running = Running::start(server, config.socket_path);
        let error = read(&client, pg_id, 1, 3).unwrap_err();
        let reason = match fault {
            "range" | "reduced" => Some("entry range"),
            "count" => Some("count exceeds request"),
            "order" | "duplicate" | "link" => Some("ordering/hash link"),
            "genesis" => Some("genesis"),
            _ => None,
        };
        if fault == "auth" {
            assert!(
                matches!(&error, StoreError::StorageRpc { detail, .. } if detail.as_str().contains("authentication")),
                "{error:?}"
            );
        } else {
            assert!(
                matches!(&error, StoreError::StorageRpc { failure, .. } if *failure == StorageRpcErrorCode::PayloadDecode),
                "{fault}: {error:?}"
            );
            if let Some(reason) = reason {
                assert!(
                    matches!(&error, StoreError::StorageRpc { detail, .. } if detail.as_str().contains(reason)),
                    "{fault}: {error:?}"
                );
            }
        }
        assert_eq!(
            responses.load(Ordering::SeqCst),
            if fault == "reduced" { 2 } else { 1 },
            "{fault}"
        );
    }
}

#[test]
fn staging_complete_hash_rpc_keeps_interior_gaps_as_sparse_observations() {
    let dir = test_util::tempdir();
    let (server, client, config) = installed(&dir, PgState::Active, false, true);
    let node = server.test_storage_node();
    let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
    let writes = {
        let pg = node.get_pg(pg_id.get()).unwrap();
        for position in 1..=3 {
            let command = ordinary(MetadataCommandId::new(
                config.cluster_epoch,
                pg_id,
                index(position),
            ));
            pg.record_metadata_command_abandoned(4, &command).unwrap();
        }
        pg.conn
            .execute("DELETE FROM metadata_command_log WHERE log_index = 2", [])
            .unwrap();
        pg.conn.total_changes()
    };
    let _running = Running::start(server, config.socket_path);
    let page = read(&client, pg_id, 1, 3).unwrap();
    assert_eq!(page.covered_range(), (index(1), index(3)));
    assert_eq!(
        page.entries()
            .iter()
            .map(|row| row.log_index)
            .collect::<Vec<_>>(),
        [1, 3]
    );
    assert_ne!(
        page.entries()[0].log_hash,
        page.entries()[1].previous_log_hash
    );
    assert_eq!(
        node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
        writes
    );
}

#[test]
fn staging_complete_hash_rpc_keeps_original_deadline_and_rejects_invalid_scope_before_transport() {
    let dir = test_util::tempdir();
    let (server, client, config) = installed(&dir, PgState::Active, false, true);
    let responses = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&responses);
    let deadline = Arc::new(Mutex::new(None::<Instant>));
    let cutoff = Arc::clone(&deadline);
    server.set_response_frame_test_hook(Arc::new(move |kind, frame| {
        if kind != KIND {
            return;
        }
        let attempt = count.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            frame.payload = budget_hint(kind, PgId::new(0), 1, 4096, 16);
            return;
        }
        if attempt == 1 {
            let until = cutoff.lock().unwrap().unwrap();
            std::thread::sleep(
                until.saturating_duration_since(Instant::now()) + Duration::from_millis(20),
            );
        }
        frame.payload = rpc::encode_storage_rpc_error_response(&rpc::StorageRpcErrorResponse {
            code: StorageRpcErrorCode::RetainedLogValidationBudgetExceeded,
            message: "budget exhausted".into(),
        })
        .unwrap();
    }));
    let _running = Running::start(server, config.socket_path);
    for (pg, epoch, first, last, until) in [
        (
            PgId::new(99),
            config.cluster_epoch,
            1,
            1,
            Instant::now() + Duration::from_secs(30),
        ),
        (
            PgId::new(0),
            config.cluster_epoch,
            2,
            1,
            Instant::now() + Duration::from_secs(30),
        ),
        (
            PgId::new(0),
            config.cluster_epoch,
            1,
            4097,
            Instant::now() + Duration::from_secs(30),
        ),
        (
            PgId::new(0),
            ClusterEpoch::new(6).unwrap(),
            1,
            1,
            Instant::now() + Duration::from_secs(30),
        ),
        (PgId::new(0), config.cluster_epoch, 1, 1, Instant::now()),
    ] {
        assert!(client
            .complete_metadata_log_hash_page_until(pg, epoch, index(first), index(last), until)
            .is_err());
    }
    assert_eq!(responses.load(Ordering::SeqCst), 0);
    let until = Instant::now() + Duration::from_secs(2);
    *deadline.lock().unwrap() = Some(until);
    assert!(client
        .complete_metadata_log_hash_page_until(
            PgId::new(0),
            config.cluster_epoch,
            index(1),
            index(4096),
            until,
        )
        .is_err());
    assert_eq!(responses.load(Ordering::SeqCst), 2);
}

#[test]
fn staging_complete_hash_rpc_rejects_crossed_routes_and_does_not_select_complete_grammar_by_default(
) {
    for (select_complete, historical) in [(false, false), (true, true)] {
        let dir = test_util::tempdir();
        let (server, client, config) =
            installed(&dir, PgState::Active, historical, select_complete);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let id = seed_context(&node.get_pg(pg_id.get()).unwrap(), 0).id();
        let _running = Running::start(server, config.socket_path);
        let error = read(&client, pg_id, id.log_index().get(), id.log_index().get()).unwrap_err();
        if historical {
            assert!(
                matches!(error, StoreError::StorageRpc { failure, .. } if failure == StorageRpcErrorCode::StaleShardLocation),
                "{error:?}"
            );
        }
    }
}

#[test]
fn staging_complete_hash_rpc_rejects_retired_and_checksummed_malformed_context_rows() {
    for malformed in [false, true] {
        let dir = test_util::tempdir();
        let (server, client, config) = installed(&dir, PgState::Active, false, true);
        let node = server.test_storage_node();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg;
        let writes = {
            let pg = node.get_pg(pg_id.get()).unwrap();
            if malformed {
                let context = seed_context(&pg, 0);
                let id = context.id();
                let mut bytes = context.encode_command().unwrap().bytes().to_vec();
                *bytes.last_mut().unwrap() = 42;
                let crc = checksum::crc64::checksum(&bytes);
                let previous: i64 = pg
                    .conn
                    .query_row(
                        "SELECT previous_log_hash FROM metadata_command_log WHERE log_index = ?1",
                        [id.log_index().get() as i64],
                        |row| row.get(0),
                    )
                    .unwrap();
                let hash = metadata_command_log_hash(
                    id.cluster_epoch(),
                    pg_id,
                    id.log_index(),
                    previous as u64,
                    crc,
                );
                pg.conn.execute("UPDATE metadata_command_log SET command_bytes = ?1, command_checksum = ?2, log_hash = ?3 WHERE log_index = ?4", rusqlite::params![bytes, crc as i64, hash.value() as i64, id.log_index().get() as i64]).unwrap();
            } else {
                let command = ordinary(MetadataCommandId::new(
                    config.cluster_epoch,
                    pg_id,
                    index(1),
                ));
                pg.apply_metadata_command_and_record(4, &command).unwrap();
            }
            pg.conn.total_changes()
        };
        let _running = Running::start(server, config.socket_path);
        let error = read(&client, pg_id, 1, 31).unwrap_err();
        assert!(matches!(error, StoreError::StorageRpc { .. }), "{error:?}");
        assert_eq!(
            node.get_pg(pg_id.get()).unwrap().conn.total_changes(),
            writes
        );
    }
}

#[test]
fn staging_complete_hash_response_rejects_a_different_store_pg_before_loading_rows() {
    let dir = test_util::tempdir();
    let pg = crate::node_runtime::pg_store::PgStore::open(dir.path(), 1).unwrap();
    let topology = topology();
    let scope = CompleteLogHashRangeRequest::new(
        PgId::new(0),
        ClusterEpoch::INITIAL,
        index(1),
        index(1),
        &topology,
    )
    .unwrap();
    assert!(scope.read_response(&pg, 4).is_err());
}
