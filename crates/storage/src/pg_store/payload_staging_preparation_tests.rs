// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only allocator tests. No shard is written and no remote physical
//! capability is minted. Introduction/append use the reviewed apply components.

use super::super::preparation::{
    prepare_in_open_transaction, with_before_effect, PreparationError,
};
use super::*;
use crate::clock::test_time_override_guard;
use crate::node_runtime::payload_placement::PayloadPlacementInput;
use crate::node_runtime::traits::PgMetadataStore;
use crate::pg_store::PgStore;
use crate::types::{AdmittedRouteEffectFence, ExpectedStreamAppendState};
use crate::{PrepareStreamUploadSegmentAppendReq, StoreError, StreamUploadSegmentRecord};

fn targets() -> [StagingTarget; 2] {
    [StagingTarget::StreamPut, multipart_target()]
}

fn expected(pg: &PgStore, owner: &StagingOwnerRecord) -> ExpectedStreamAppendState {
    let session = pg.get_stream_upload(&owner.subject.id.attempt).unwrap();
    match &owner.subject.target {
        StagingTarget::StreamPut => ExpectedStreamAppendState::PutObject {
            session,
            generation_id: owner.subject.payload_generation,
        },
        StagingTarget::UploadPart { upload_id, .. } => ExpectedStreamAppendState::UploadPart {
            session,
            upload: Box::new(pg.get_multipart_upload(upload_id).unwrap()),
            part_generation_id: owner.subject.payload_generation,
        },
        StagingTarget::DirectPut => unreachable!(),
    }
}

fn request(owner: &StagingOwnerRecord) -> PrepareStreamUploadSegmentAppendReq {
    PrepareStreamUploadSegmentAppendReq {
        session_id: owner.subject.id.attempt.clone(),
        segment_index: owner.windows.last().unwrap().index,
        size: 4,
        segment_crc64: 0,
        payload_crc64: 0,
    }
}

fn prepare(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    expected: &ExpectedStreamAppendState,
    fence: AdmittedRouteEffectFence,
) -> Result<StreamUploadSegmentRecord, PreparationError> {
    let config = capture::config(owner);
    prepare_in_open_transaction(
        pg,
        &topology(),
        config.node_id,
        PayloadPlacementInput::from_server_config(&config),
        fence,
        &request(owner),
        expected,
    )
}

fn fence(owner: &StagingOwnerRecord) -> AdmittedRouteEffectFence {
    AdmittedRouteEffectFence::unbounded(owner.subject.originating_epoch)
}

fn existing_allocator_changes(pg: &PgStore, owner: &StagingOwnerRecord) -> u64 {
    // SQLite total_changes includes the existing digest/cache trigger writes.
    // Compare with the production allocator, then roll back the measurement;
    // the raw path is a cost baseline, not retained-window authority.
    pg.conn.execute_batch("SAVEPOINT allocation_cost").unwrap();
    let before = pg.conn.total_changes();
    assert_eq!(
        pg.allocate_stream_segment_vid_for_admitted_append(
            &owner.subject.id.attempt,
            0,
            owner.subject.issuer.node_id,
            fence(owner),
        )
        .unwrap(),
        Some(owner.windows[0].start)
    );
    let changes = pg.conn.total_changes() - before;
    assert_eq!(pg.conn.changes(), 1, "one outer cursor UPDATE");
    pg.conn
        .execute_batch("ROLLBACK TO allocation_cost; RELEASE allocation_cost")
        .unwrap();
    changes
}

#[test]
fn staging_preparation_consumes_once_and_append_opens_only_the_next_retained_window() {
    let _clock = test_time_override_guard(2_000);
    for target in targets() {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let owner = append::initialize(&pg, target);
        let captured = expected(&pg, &owner);
        let allocator_changes = existing_allocator_changes(&pg, &owner);
        let changes = pg.conn.total_changes();
        let first = prepare(&pg, &owner, &captured, fence(&owner)).unwrap();
        assert_eq!(
            pg.conn.total_changes(),
            changes + allocator_changes,
            "same allocator UPDATE and digest trigger cost"
        );
        assert_eq!(first.segment_vid, owner.windows[0].start);
        assert_eq!(first.segment_okh, owner.windows[0].segment_hash);
        assert_eq!(
            first.data_pg_id,
            owner.windows[0].participants.data_pg.get()
        );
        assert_eq!(
            first.placement_cluster_epoch,
            owner.windows[0].participants.placement_epoch
        );
        assert_eq!(
            append::load(&pg, &owner),
            owner,
            "no owner/window/horizon rewrite"
        );
        assert!(pg
            .list_stream_segments(&owner.subject.id.attempt)
            .unwrap()
            .is_empty());
        pg.conn.execute_batch("COMMIT").unwrap();
        // Pretend the successful reply was lost. Reacquiring and replaying the
        // same request is not idempotent physical allocation.
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let state = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(matches!(
            prepare(&pg, &owner, &captured, fence(&owner)),
            Err(PreparationError::Window)
        ));
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), state);
        let next = append::next(&pg, &owner);
        super::super::descriptor::apply_append_for_test(
            &pg,
            &topology(),
            &append::command(&owner),
            &next,
        )
        .unwrap();
        let after = append::load(&pg, &owner);
        assert_eq!(after.windows, vec![owner.windows[0].clone(), next.clone()]);
        let changes = pg.conn.total_changes();
        let second = prepare(&pg, &after, &captured, fence(&owner)).unwrap();
        assert_eq!(pg.conn.total_changes(), changes + allocator_changes);
        assert_eq!(second.segment_index, 1);
        assert_eq!(second.segment_vid, next.start);
        assert_eq!(
            pg.get_stream_upload(&owner.subject.id.attempt)
                .unwrap()
                .next_segment_vid,
            next.end
        );
        assert_eq!(append::load(&pg, &owner), after);
    }
}

#[test]
fn staging_preparation_requires_writer_admission_not_merely_an_open_transaction() {
    let _clock = test_time_override_guard(2_000);
    for target in targets() {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let owner = append::initialize(&pg, target);
        let captured = expected(&pg, &owner);
        pg.conn.execute_batch("COMMIT").unwrap();
        for read_snapshot in [false, true] {
            if read_snapshot {
                pg.conn.execute_batch("BEGIN").unwrap();
                pg.get_stream_upload(&owner.subject.id.attempt).unwrap();
                assert_eq!(
                    pg.conn.transaction_state(Some(rusqlite::MAIN_DB)).unwrap(),
                    rusqlite::TransactionState::Read
                );
            }
            let changes = pg.conn.total_changes();
            assert!(matches!(
                prepare(&pg, &owner, &captured, fence(&owner)),
                Err(PreparationError::TransactionRequired)
            ));
            assert_eq!(pg.conn.total_changes(), changes);
            if read_snapshot {
                pg.conn.execute_batch("ROLLBACK").unwrap();
            }
        }
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        prepare(&pg, &owner, &captured, fence(&owner)).unwrap();
    }
}

#[test]
fn staging_preparation_rechecks_both_admission_and_window_cutoffs_at_the_update() {
    for target in targets() {
        for window_expires_first in [false, true] {
            let clock = test_time_override_guard(2_000);
            let dir = test_util::tempdir();
            let pg =
                PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
            let owner = append::initialize(&pg, target.clone());
            let captured = expected(&pg, &owner);
            let state = introduction::state(&pg);
            let session = pg.get_stream_upload(&owner.subject.id.attempt).unwrap();
            let changes = pg.conn.total_changes();
            let authority = if window_expires_first {
                fence(&owner) // renewed/static raw admission cannot renew the window
            } else {
                AdmittedRouteEffectFence::bounded(owner.subject.originating_epoch, 5_000, 4_000)
            };
            let control = clock.control();
            let after = if window_expires_first {
                owner.windows[0].cutoff_ms
            } else {
                4_500
            };
            let error = with_before_effect(
                move || control.set(after),
                || prepare(&pg, &owner, &captured, authority),
            )
            .unwrap_err();
            assert!(
                matches!(
                    error,
                    PreparationError::Store(StoreError::RouteMapExpired { .. })
                ),
                "{error:?}"
            );
            assert_eq!(
                pg.conn.total_changes(),
                changes,
                "reject before UPDATE, not after rollback"
            );
            assert_eq!(
                pg.get_stream_upload(&owner.subject.id.attempt).unwrap(),
                session
            );
            assert_eq!(introduction::state(&pg), state);
            // An expired request can fail without consuming the slot; a fresh
            // admission succeeds only while the immutable window remains live.
            if !window_expires_first {
                prepare(&pg, &owner, &captured, fence(&owner)).unwrap();
            } else {
                assert!(matches!(
                    prepare(&pg, &owner, &captured, fence(&owner)),
                    Err(PreparationError::Store(StoreError::RouteMapExpired { .. }))
                ));
            }
        }
    }
}

#[test]
fn staging_preparation_restart_never_reuses_the_original_issuer_even_before_first_consumption() {
    let _clock = test_time_override_guard(2_000);
    for target in targets() {
        for consumed in [false, true] {
            let dir = test_util::tempdir();
            let pg_id = fixture(target.clone()).subject.object_pg.get();
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            let owner = append::initialize(&pg, target.clone());
            let captured = expected(&pg, &owner);
            if consumed {
                prepare(&pg, &owner, &captured, fence(&owner)).unwrap();
            }
            pg.conn.execute_batch("COMMIT").unwrap();
            drop(pg);
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            assert_ne!(
                pg.stream_allocation_issuer(owner.subject.issuer.node_id)
                    .unwrap(),
                owner.subject.issuer
            );
            let state = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            let error = prepare(&pg, &owner, &captured, fence(&owner)).unwrap_err();
            if consumed {
                assert!(matches!(error, PreparationError::Window), "{error:?}");
            } else {
                assert!(
                    matches!(
                        error,
                        PreparationError::Store(StoreError::PgAllocationLifetimeUnavailable { .. })
                    ),
                    "{error:?}"
                );
            }
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), state);
            assert_eq!(append::load(&pg, &owner), owner);
        }
    }
}

#[test]
fn staging_preparation_replication_and_lifetime_replacement_do_not_delegate_allocation() {
    let _clock = test_time_override_guard(2_000);
    for target in targets() {
        let primary_dir = test_util::tempdir();
        let replica_dir = test_util::tempdir();
        let pg_id = fixture(target.clone()).subject.object_pg.get();
        let primary = PgStore::open(primary_dir.path(), pg_id).unwrap();
        let replica = PgStore::open(replica_dir.path(), pg_id).unwrap();
        let (owner, create) = introduction::setup(&primary, target);
        let mut replica_input = owner.clone();
        crate::clock::with_time_and_monotonic_override(1_000, 70_000, || {
            introduction::seed_upload(&replica, &mut replica_input);
        });
        assert_eq!(replica_input, owner);
        // Witness first, with exactly the same replicated issuer/window.
        for pg in [&replica, &primary] {
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            super::super::descriptor::apply_introduction_for_test(pg, &topology(), &create, &owner)
                .unwrap();
        }
        let captured = expected(&primary, &owner);
        assert_eq!(expected(&replica, &owner), captured);
        let changes = replica.conn.total_changes();
        let error = prepare(&replica, &owner, &captured, fence(&owner)).unwrap_err();
        assert!(
            matches!(
                error,
                PreparationError::Store(StoreError::PgAllocationLifetimeUnavailable { .. })
            ),
            "{error:?}"
        );
        assert_eq!(replica.conn.total_changes(), changes);
        assert_eq!(append::load(&replica, &owner), owner);
        // The actual original lifetime remains usable.
        prepare(&primary, &owner, &captured, fence(&owner)).unwrap();
        let next = append::next(&primary, &owner);
        super::super::descriptor::apply_append_for_test(
            &primary,
            &topology(),
            &append::command(&owner),
            &next,
        )
        .unwrap();
        let advanced = append::load(&primary, &owner);
        // Replacement without reopening also permanently withdraws allocation;
        // an unused lookahead does not become successor authority.
        drop(primary.begin_allocation_lifetime_replacement().unwrap());
        let changes = primary.conn.total_changes();
        let error = prepare(&primary, &advanced, &captured, fence(&owner)).unwrap_err();
        assert!(
            matches!(
                error,
                PreparationError::Store(StoreError::PgAllocationLifetimeUnavailable { .. })
            ),
            "{error:?}"
        );
        assert_eq!(primary.conn.total_changes(), changes);
        assert_eq!(append::load(&primary, &owner), advanced);
    }
}

#[test]
fn staging_preparation_reloads_session_authority_and_generation_inside_the_transaction() {
    let _clock = test_time_override_guard(2_000);
    for target in targets() {
        for changed in ["session", "generation", "high_water", "prefix", "owner"] {
            let dir = test_util::tempdir();
            let pg =
                PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
            let owner = append::initialize(&pg, target.clone());
            let captured = expected(&pg, &owner);
            match changed {
                "session" => {
                    pg.conn
                        .execute_batch(
                            "UPDATE stream_uploads SET cleanup_after = cleanup_after + 1",
                        )
                        .unwrap();
                }
                "generation" => {
                    pg.conn.execute_batch("UPDATE object_generation_reservations SET generation_id = generation_id + 1").unwrap();
                }
                "high_water" => {
                    pg.conn
                        .execute_batch("DELETE FROM object_write_counters")
                        .unwrap();
                }
                "prefix" => {
                    // An ordinary append without the owner transition must not
                    // permit another preparation under the old retained window.
                    let crate::metadata_command::MetadataCommandPayload::AppendStreamSegment(
                        command,
                    ) = append::command(&owner).payload().clone()
                    else {
                        unreachable!()
                    };
                    pg.apply_append_stream_segment_command(&command).unwrap();
                }
                "owner" => {
                    pg.conn
                        .execute_batch("DELETE FROM payload_staging_owners")
                        .unwrap();
                }
                _ => unreachable!(),
            }
            let state = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            let error = prepare(&pg, &owner, &captured, fence(&owner)).unwrap_err();
            match changed {
                "session" => assert!(matches!(error, PreparationError::Authority), "{error:?}"),
                "high_water" => assert!(
                    matches!(
                        error,
                        PreparationError::Invariant(StagingInvariant::GenerationFence)
                    ),
                    "{error:?}"
                ),
                _ => assert!(
                    matches!(error, PreparationError::Responsibility),
                    "{changed}: {error:?}"
                ),
            }
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), state);
        }
    }
}

#[test]
fn staging_preparation_rejects_crossed_inputs_without_consuming_the_valid_slot() {
    let _clock = test_time_override_guard(2_000);
    for target in targets() {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let owner = append::initialize(&pg, target);
        let captured = expected(&pg, &owner);
        for changed in [
            "index", "size", "key", "epoch", "primary", "ack", "files", "ec", "topology",
        ] {
            let mut config = capture::config(&owner);
            let mut req = request(&owner);
            let mut expected = captured.clone();
            let mut authority = fence(&owner);
            match changed {
                "index" => req.segment_index += 1,
                "size" => req.size = owner.windows[0].geometry.logical_bound + 1,
                "key" => {
                    let session = match &mut expected {
                        ExpectedStreamAppendState::PutObject { session, .. }
                        | ExpectedStreamAppendState::UploadPart { session, .. } => session,
                    };
                    session.key = crate::tests::object_key("crossed");
                }
                "epoch" => {
                    authority = AdmittedRouteEffectFence::unbounded(
                        ClusterEpoch::new(config.cluster_epoch.get() + 1).unwrap(),
                    )
                }
                "primary" => config.node_id = NodeId::new(2),
                "ack" => {
                    let data_pg = owner.windows[0].participants.data_pg.get();
                    config
                        .pg_routes
                        .iter_mut()
                        .find(|route| route.pg_id == data_pg)
                        .unwrap()
                        .primary_node_id = NodeId::new(3);
                }
                "files" => {
                    for route in &mut config.pg_routes {
                        route.acting_set.retain(|node| *node != NodeId::new(0));
                    }
                }
                "ec" => config.default_ec_shape = EcShape { k: 1, m: 1 },
                "topology" => config.pg_ids.push(8),
                _ => unreachable!(),
            }
            let state = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            let error = prepare_in_open_transaction(
                &pg,
                &topology(),
                config.node_id,
                PayloadPlacementInput::from_server_config(&config),
                authority,
                &req,
                &expected,
            )
            .unwrap_err();
            assert_eq!(pg.conn.total_changes(), changes, "{changed}: {error:?}");
            assert_eq!(introduction::state(&pg), state, "{changed}");
        }
        prepare(&pg, &owner, &captured, fence(&owner)).unwrap();
    }
}

#[test]
fn staging_preparation_update_failure_and_transaction_rollback_do_not_consume_the_slot() {
    let _clock = test_time_override_guard(2_000);
    for target in targets() {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let owner = append::initialize(&pg, target);
        let captured = expected(&pg, &owner);
        pg.conn
            .execute_batch(
                "COMMIT; BEGIN IMMEDIATE;
            CREATE TEMP TRIGGER fail_prepare BEFORE UPDATE OF next_segment_vid ON stream_uploads
            BEGIN SELECT RAISE(ABORT, 'injected retained preparation failure'); END;",
            )
            .unwrap();
        let changes = pg.conn.total_changes();
        let error = prepare(&pg, &owner, &captured, fence(&owner)).unwrap_err();
        assert!(matches!(error, PreparationError::Database(_)), "{error:?}");
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(
            pg.get_stream_upload(&owner.subject.id.attempt)
                .unwrap()
                .next_segment_vid,
            owner.windows[0].start
        );
        pg.conn.execute_batch("DROP TRIGGER fail_prepare").unwrap();
        prepare(&pg, &owner, &captured, fence(&owner)).unwrap();
        pg.conn.execute_batch("ROLLBACK; BEGIN IMMEDIATE").unwrap();
        assert_eq!(
            pg.get_stream_upload(&owner.subject.id.attempt)
                .unwrap()
                .next_segment_vid,
            owner.windows[0].start
        );
        prepare(&pg, &owner, &captured, fence(&owner)).unwrap();
        pg.conn.execute_batch("COMMIT; BEGIN IMMEDIATE").unwrap();
        assert!(matches!(
            prepare(&pg, &owner, &captured, fence(&owner)),
            Err(PreparationError::Window)
        ));
    }
}
