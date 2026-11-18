// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::{Duration, Instant};

fn assert_rejected_unchanged(pg: &PgStore, epoch: ClusterEpoch, expected: &str) {
    let writes = pg.conn.total_changes();
    let before = introduction::state(pg);
    let error = validate_reinspection(pg, epoch).unwrap_err();
    assert!(error.to_string().contains(expected), "{error}");
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(pg), before);
    assert!(pg.conn.is_autocommit());
    cold(pg, epoch);
}

#[test]
fn staging_direct_reinspection_prefix_cumulative_entry_and_byte_budgets_leave_cache_cold() {
    let _clock = test_time_override_guard(2_000);
    for byte_limit in [false, true] {
        let (_dir, pg, context) = setup(3);
        let epoch = context.id().cluster_epoch();
        let (count, first_bytes): (usize, usize) = pg.conn.query_row(
            "SELECT count(*), (SELECT length(command_bytes) FROM metadata_command_log ORDER BY log_index LIMIT 1) FROM metadata_command_log", [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).unwrap();
        assert!(count > 1, "must validate a nonempty prefix before failing");
        let limits = if byte_limit {
            (4096, 100_000, count * 4 + first_bytes)
        } else {
            (1, 100_000, 16 * 1024 * 1024)
        };
        let reads = pg.test_metadata_command_log_replay_validation_entries();
        PgStore::test_with_prefix_validation_bounds(
            limits,
            || {},
            || {
                assert_rejected_unchanged(
                    &pg,
                    epoch,
                    if byte_limit {
                        "byte budget exhausted"
                    } else {
                        "entry budget exhausted"
                    },
                );
            },
        );
        assert_eq!(
            pg.test_metadata_command_log_replay_validation_entries() - reads,
            1
        );
        validate_reinspection(&pg, epoch).unwrap();
        observe(&pg, epoch).unwrap();
    }
}

#[test]
fn staging_direct_reinspection_prefix_deadline_expires_between_entries_without_seeding() {
    let _clock = crate::clock::test_time_and_monotonic_override_guard(2_000, 10_000);
    let (_dir, pg, context) = setup(3);
    let epoch = context.id().cluster_epoch();
    let fence = crate::types::AdmittedRouteEffectFence::bounded(epoch, 50_000, 10_100);
    let time = Rc::new(RefCell::new(None));
    let advance = Rc::clone(&time);
    let entries = Rc::new(Cell::new(0));
    let seen = Rc::clone(&entries);
    let reads = pg.test_metadata_command_log_replay_validation_entries();
    let writes = pg.conn.total_changes();
    PgStore::test_with_prefix_validation_bounds(
        (4096, 100_000, 16 * 1024 * 1024),
        move || {
            seen.set(seen.get() + 1);
            if seen.get() == 2 {
                advance.replace(Some(crate::clock::test_time_and_monotonic_override_guard(
                    2_000, 10_100,
                )));
            }
        },
        || {
            let error = pg
                .validate_complete_direct_reinspection_prefix(
                    NodeId::new(0),
                    epoch,
                    &topology(),
                    Instant::now() + Duration::from_secs(60),
                    fence,
                )
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    CompleteSettledPrefixError::Store(StoreError::RouteMapExpired { .. })
                ),
                "{error}"
            );
        },
    );
    assert_eq!(entries.get(), 2);
    assert_eq!(
        pg.test_metadata_command_log_replay_validation_entries() - reads,
        1
    );
    assert_eq!(pg.conn.total_changes(), writes);
    drop(time.take());
    cold(&pg, epoch);
    validate_reinspection(&pg, epoch).unwrap();
}

#[test]
fn staging_direct_reinspection_prefix_work_cutoff_stops_mid_suffix_with_live_route() {
    let _clock = test_time_override_guard(2_000);
    let (_dir, pg, context) = setup(3);
    let epoch = context.id().cluster_epoch();
    let fence = crate::types::AdmittedRouteEffectFence::bind_portable(epoch, 50_000, 50_000);
    let entries = Rc::new(Cell::new(0));
    let seen = Rc::clone(&entries);
    let reads = pg.test_metadata_command_log_replay_validation_entries();
    let writes = pg.conn.total_changes();
    PgStore::test_with_prefix_validation_bounds(
        (4096, 100_000, 16 * 1024 * 1024),
        move || {
            seen.set(seen.get() + 1);
            if seen.get() == 2 {
                PgStore::test_expire_prefix_validation_deadline();
            }
        },
        || {
            let error = pg
                .validate_complete_direct_reinspection_prefix(
                    NodeId::new(0),
                    epoch,
                    &topology(),
                    Instant::now() + Duration::from_secs(60),
                    fence,
                )
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    CompleteSettledPrefixError::Store(StoreError::OperationDeadlineExceeded {
                        context: "complete prefix reinspection validation"
                    })
                ),
                "{error}"
            );
            fence
                .require_valid_for(epoch)
                .expect("only the work cutoff must expire");
        },
    );
    assert_eq!(entries.get(), 2);
    assert_eq!(
        pg.test_metadata_command_log_replay_validation_entries() - reads,
        1
    );
    assert_eq!(pg.conn.total_changes(), writes);
    cold(&pg, epoch);
    validate_reinspection(&pg, epoch).unwrap();
}

#[test]
fn staging_direct_reinspection_prefix_materialized_row_budget_is_shared_and_nonmutating() {
    let _clock = test_time_override_guard(2_000);
    let (_dir, pg, context) = setup(3);
    let epoch = context.id().cluster_epoch();
    let count: usize = pg
        .conn
        .query_row("SELECT count(*) FROM metadata_command_log", [], |row| {
            row.get(0)
        })
        .unwrap();
    PgStore::test_with_prefix_validation_bounds(
        (4096, count * 2, 16 * 1024 * 1024),
        || {},
        || {
            let reads = pg.test_metadata_command_log_replay_validation_entries();
            assert_rejected_unchanged(&pg, epoch, "row budget exhausted");
            assert_eq!(
                pg.test_metadata_command_log_replay_validation_entries() - reads,
                count as u64
            );
        },
    );
}

#[test]
fn staging_direct_reinspection_prefix_checkpoint_is_charged_before_loading_or_decoding() {
    let _clock = test_time_override_guard(2_000);
    let (_dir, pg, context) = setup(3);
    pg.record_complete_metadata_command_checkpoint(0, &topology())
        .unwrap();
    // The ordinary candidate reader skips this invalid specimen and falls back
    // to the log. Foreground validation must reject its size before allocating
    // or decoding it, rather than silently spending unbounded candidate work.
    pg.conn
        .execute(
            "UPDATE metadata_command_checkpoints SET checkpoint_bytes = zeroblob(?1)",
            [16 * 1024 * 1024 + 1],
        )
        .unwrap();
    let reads = pg.test_metadata_command_log_replay_validation_entries();
    assert_rejected_unchanged(&pg, context.id().cluster_epoch(), "byte budget exhausted");
    assert_eq!(
        pg.test_metadata_command_log_replay_validation_entries(),
        reads
    );
}
