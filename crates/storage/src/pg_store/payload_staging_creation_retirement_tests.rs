// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Exercised by the Unix/TLS creation matrix for inspected, installed and
//! uncertain submissions, after genuine witness-first all-member application.

use super::*;
use crate::node_runtime::pg_store::TerminalSlotRetirementObservation as Retirement;

#[test]
fn staging_terminal_creation_retirement_custody_survives_append_and_restart() {
    let _clock = crate::clock::test_time_override_guard(2_000);
    let dir = test_util::tempdir();
    let mut template = fresh_capture::template(multipart_target());
    let pg_id = template.subject.object_pg.get();
    let pg = PgStore::open(dir.path(), pg_id).unwrap();
    let command = fresh_capture::candidate(&pg, &mut template);
    let creation =
        ContextualCommand::from_described(candidate::capture(&pg, &template, command, false));
    complete_application::anchor(&pg, creation.id());
    complete_log_range::apply(&pg, &creation);
    let snapshot = pg.command_build_snapshot().unwrap();
    let owner = append::load(&pg, &template);
    snapshot.finish().unwrap();
    fresh_append_capture::prepare(&pg, &owner);
    let appended = ContextualCommand::from_described(candidate::capture(
        &pg,
        &owner,
        append::command(&owner),
        true,
    ));
    complete_log_range::apply(&pg, &appended);
    let expected = introduction::state(&pg);
    drop(pg);
    let pg = PgStore::open(dir.path(), pg_id).unwrap();
    let changes = pg.conn.total_changes();
    let snapshot = pg.command_build_snapshot().unwrap();
    let progressed = append::load(&pg, &template);
    assert_eq!(progressed.windows.len(), 2);
    assert_eq!(progressed.references.len(), 1);
    creation.require_slot_retirement_custody(&pg).unwrap();
    snapshot.finish().unwrap();
    assert_eq!(introduction::state(&pg), expected);
    assert_eq!(pg.conn.total_changes(), changes);
}

pub(super) fn require_release_gate(terminal: &mut CompleteTerminalHandoff<'_>) {
    assert!(terminal
        .test_retirement_attempt(deadline(), |_| panic!(
            "unconfirmed release reached transport"
        ))
        .is_err());
    assert_eq!(
        terminal.slot_retirement_observation(),
        Retirement::NoAttemptObserved
    );
}

pub(super) fn retire(
    primary: &StorageNodeServer,
    client: &crate::node_client::UnixStorageNodeClient,
    context: &ContextualCommand,
    terminal: &mut CompleteTerminalHandoff<'_>,
    faults: bool,
) {
    let node = primary.test_storage_node();
    let pg_id = context.id().pg_id().get();
    let before = {
        let pg = node.get_pg(pg_id).unwrap();
        let generation: u64 = pg
            .conn
            .query_row(
                "SELECT payload_generation FROM payload_staging_owners",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            generation > 1,
            "the lowered positive counter must cross the retained fence"
        );
        introduction::state(&pg)
    };
    let mut session = client
        .open_metadata_command_critical_section_until(context.id().pg_id(), deadline())
        .unwrap();
    if faults {
        for damage in [
            "DELETE FROM payload_staging_owners",
            "UPDATE payload_staging_owners SET incarnation = incarnation + 1",
            "DELETE FROM payload_staging_windows",
            "UPDATE payload_staging_windows SET cutoff_ms = cutoff_ms + 1",
            "DELETE FROM payload_staging_participants WHERE shard_index = 0",
            "DELETE FROM payload_staging_resources WHERE role = 0",
            "UPDATE payload_staging_resources SET owner_token = owner_token || '-crossed' WHERE role = 0",
            "UPDATE payload_staging_resources SET release_pg = release_pg + 1 WHERE role = 0",
            "UPDATE payload_staging_resources SET state = 1 WHERE role = 0",
            "DELETE FROM object_write_counters",
            "UPDATE object_write_counters SET max_committed_generation = 1",
        ] {
            let damaged = {
                let pg = node.get_pg(pg_id).unwrap();
                backup(&pg);
                pg.conn.execute_batch(damage).unwrap();
                (snapshot(&pg), pg.conn.total_changes())
            };
            let error = terminal.retire_remote(client, &session, deadline()).unwrap_err();
            assert_eq!(terminal.slot_retirement_observation(), Retirement::Unconfirmed);
            let pg = node.get_pg(pg_id).unwrap();
            assert_eq!((snapshot(&pg), pg.conn.total_changes()), damaged, "{damage}: {error}");
            assert!(pg.conn.is_autocommit());
            restore(&pg);
            assert_eq!(introduction::state(&pg), before);
        }
        let replies = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&replies);
        primary.set_response_envelope_test_hook(Arc::new(move |kind, bytes| {
            if kind == Kind::MetadataCommandPendingSlotRemove
                && count.fetch_add(1, Ordering::SeqCst) == 0
            {
                *bytes.last_mut().unwrap() ^= 1;
            }
        }));
        assert!(terminal
            .retire_remote(client, &session, deadline())
            .is_err());
        assert_eq!(
            terminal.slot_retirement_observation(),
            Retirement::Unconfirmed
        );
        assert_eq!(replies.load(Ordering::SeqCst), 1, "no hidden retry");
        {
            let pg = node.get_pg(pg_id).unwrap();
            assert!(complete_pending::observe(&pg).iter().all(Vec::is_empty));
            assert_eq!(introduction::state(&pg), before);
        }
        drop(session);
        session = client
            .open_metadata_command_critical_section_until(context.id().pg_id(), deadline())
            .unwrap();
        // An absent slot alone is insufficient evidence after a lost reply.
        {
            let pg = node.get_pg(pg_id).unwrap();
            backup(&pg);
            pg.conn
                .execute_batch("DELETE FROM payload_staging_resources WHERE role = 0")
                .unwrap();
        }
        assert!(terminal
            .retire_remote(client, &session, deadline())
            .is_err());
        assert_eq!(
            terminal.slot_retirement_observation(),
            Retirement::Unconfirmed
        );
        restore(&node.get_pg(pg_id).unwrap());
    }
    terminal
        .retire_remote(client, &session, deadline())
        .unwrap();
    assert_eq!(terminal.slot_retirement_observation(), Retirement::Retired);
    assert_eq!(terminal.reservation_observation(), Observation::Released);
    let settled = {
        let pg = node.get_pg(pg_id).unwrap();
        assert!(complete_pending::observe(&pg).iter().all(Vec::is_empty));
        assert_eq!(
            introduction::state(&pg),
            before,
            "retirement must preserve all materialized custody"
        );
        (snapshot(&pg), pg.conn.total_changes())
    };
    terminal
        .retire_remote(client, &session, deadline())
        .unwrap();
    let pg = node.get_pg(pg_id).unwrap();
    assert_eq!(
        (snapshot(&pg), pg.conn.total_changes()),
        settled,
        "confirmed retry cannot mutate"
    );
}

const TABLES: [&str; 5] = [
    "payload_staging_owners",
    "payload_staging_windows",
    "payload_staging_participants",
    "payload_staging_resources",
    "object_write_counters",
];

fn backup(pg: &PgStore) {
    for table in TABLES {
        pg.conn
            .execute_batch(&format!(
                "CREATE TEMP TABLE saved_{table} AS SELECT * FROM {table}"
            ))
            .unwrap();
    }
}

fn restore(pg: &PgStore) {
    // Test-only restoration, after proving the admitted operation made no
    // change. This initial creation has no references or selected payload.
    pg.conn
        .execute_batch("DELETE FROM payload_staging_owners; DELETE FROM object_write_counters")
        .unwrap();
    for table in TABLES {
        pg.conn
            .execute_batch(&format!(
                "INSERT INTO {table} SELECT * FROM saved_{table}; DROP TABLE saved_{table}"
            ))
            .unwrap();
    }
}
