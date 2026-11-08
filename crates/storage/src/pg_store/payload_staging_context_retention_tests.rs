// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Read-only context projection. These metadata-only fixtures do not install
//! unauthenticated v13 context sidecars or mint physical/custody authority.

use super::super::abandonment::{
    source::PartFinalizationSource, AbandonmentError, AbandonmentOutcome,
};
use super::super::candidate::CandidateBindingError;
use super::super::command_context::{ApplicationError, Applied, ContextError, ContextualCommand};
use super::*;
use crate::clock::{test_time_override_guard, with_time_and_monotonic_override};
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandPayload, ReserveObjectVersionCommand,
};
use crate::node_runtime::pg_store::command_log::{
    PendingMetadataCommandRouteDependency as Route, PendingRouteTarget,
};
use crate::pg_store::PgStore;
use crate::types::{ShardScavengerProtectedShardSetReference, ShardScavengerStagingProtection};
use crate::{StoreError, VersionId};

fn exact(epoch: ClusterEpoch, pg: PgId) -> Route {
    Route {
        cluster_epoch: epoch,
        target: PendingRouteTarget::ExactPg(pg),
    }
}

fn release(proof: &BucketWriteReservationProof) -> Route {
    Route {
        cluster_epoch: proof.cluster_epoch,
        target: PendingRouteTarget::BucketHash(PgTopology::bucket_route_hash(
            proof.bucket.as_str(),
        )),
    }
}

fn sorted(mut routes: Vec<Route>) -> Vec<Route> {
    routes.sort_unstable();
    routes.dedup();
    routes
}

fn successor(id: MetadataCommandId) -> MetadataCommandId {
    MetadataCommandId::new(
        ClusterEpoch::new(id.cluster_epoch().get() + 1).unwrap(),
        id.pg_id(),
        MetadataCommandLogIndex::new(1).unwrap(),
    )
}

fn expected_window(window: &StagingWindow) -> ShardScavengerStagingProtection {
    ShardScavengerStagingProtection {
        identity: ShardScavengerProtectedShardSetReference {
            data_pg_id: window.participants.data_pg.get(),
            okh: window.segment_hash,
            generation_id: window.start,
            ec: window.geometry.ec,
        },
        placement_epoch: window.participants.placement_epoch,
        file_nodes: window.participants.files.clone(),
        acknowledgement_primary: window.participants.acknowledgement_primary,
    }
}

#[test]
fn staging_context_retention_keeps_all_introduction_and_lookahead_dependencies() {
    let _clock = test_time_override_guard(2_000);
    let mut reached_route_bound = false;
    for (target, is_append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target, is_append);
        let captured = candidate::capture(&pg, &owner, command.clone(), is_append);
        let topology = topology();
        let (subject, window) = {
            let bound = captured.bind(&topology).unwrap();
            let protection = bound.pending_protection();
            let (_, _, subject, window) = protection.index_inputs();
            (subject.clone(), window.clone())
        };
        if is_append {
            let MetadataCommandPayload::AppendStreamSegment(append) = command.payload() else {
                unreachable!()
            };
            assert_eq!(window.index, append.segment.segment_index + 1);
            assert_ne!(window.segment_hash, append.segment.segment_okh);
            assert_ne!(window.start, append.segment.segment_vid);
        }
        let context = ContextualCommand::described(captured, &topology).unwrap();
        let field = context.encode_field().unwrap();
        let mut context =
            ContextualCommand::decode_field(command.clone(), &field, &topology).unwrap();
        let before = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let writes = pg.conn.total_changes();
        for step in 0..3 {
            if step > 0 {
                let id = successor(context.id());
                context = context.into_reissue(id).unwrap();
                assert_eq!(context.id(), id);
            }
            let actual = with_time_and_monotonic_override(9_000_000, 90_000_000, || {
                context.pending_retention(&topology)
            })
            .unwrap();
            let mut routes = vec![
                exact(context.id().cluster_epoch(), subject.object_pg),
                exact(subject.originating_epoch, subject.object_pg),
                exact(
                    window.participants.placement_epoch,
                    window.participants.data_pg,
                ),
            ];
            if !is_append {
                routes.push(exact(
                    subject.originating_epoch,
                    PgId::new(topology.bucket_pg_for(&subject.bucket)),
                ));
            }
            match command.payload() {
                MetadataCommandPayload::CreateStreamUpload(create) => {
                    routes.push(release(&create.bucket_write_reservation))
                }
                MetadataCommandPayload::ReserveObjectGeneration(_)
                | MetadataCommandPayload::AppendStreamSegment(_) => {}
                _ => unreachable!(),
            }
            let expected = sorted(routes);
            reached_route_bound |= expected.len() == 5;
            assert_eq!(actual.command_id(), context.id());
            assert_eq!(actual.routes(), expected);
            assert!(actual.routes().windows(2).all(|pair| pair[0] < pair[1]));
            assert_eq!(actual.future_window(), Some(&expected_window(&window)));
            assert_eq!(context.encode_field().unwrap(), field);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            assert_eq!(introduction::state(&pg), before);
        }
    }
    assert!(
        reached_route_bound,
        "exercise the exact five-route bound after epoch reset"
    );
}

fn source_context(
    source: &MetadataCommandEnvelope,
    cleanup: &MetadataCommandEnvelope,
) -> ContextualCommand {
    let topology = topology();
    let candidate = PartFinalizationSource::capture(source)
        .unwrap()
        .bind_cleanup(cleanup.clone(), &topology)
        .unwrap();
    let context = ContextualCommand::abandoned_part(candidate, &topology).unwrap();
    ContextualCommand::decode_field(cleanup.clone(), &context.encode_field().unwrap(), &topology)
        .unwrap()
}

#[test]
fn staging_context_retention_distinguishes_ordinary_abort_from_original_source_after_reopen() {
    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg_id = fixture(multipart_target()).subject.object_pg.get();
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let owner = part::initialize(&pg, count);
        let source = part::command(&pg, &owner);
        let cleanup = abandonment::cleanup(&pg, &owner, &source);
        let topology = topology();
        let bucket_pg = PgId::new(topology.bucket_pg_for(&owner.subject.bucket));
        assert_ne!(bucket_pg, owner.subject.object_pg);
        let mut context = source_context(&source, &cleanup);
        let mut ordinary = application::ordinary_context(&cleanup, &topology).unwrap();
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        let routes = context.pending_retention(&topology).unwrap();
        assert_eq!(
            routes.routes(),
            sorted(vec![
                exact(source.id().cluster_epoch(), source.id().pg_id()),
                exact(source.id().cluster_epoch(), bucket_pg)
            ])
        );
        assert!(routes.future_window().is_none());
        assert!(ordinary
            .pending_retention(&topology)
            .unwrap()
            .routes()
            .is_empty());
        assert!(matches!(
            context
                .application(&topology)
                .unwrap()
                .apply_in_open_transaction(&pg, 0),
            Err(ApplicationError::Abandonment(
                AbandonmentError::NotAbandoned
            ))
        ));
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);

        abandonment::terminal(&pg, &source, true);
        pg.conn.execute_batch("COMMIT").unwrap();
        pg.record_current_metadata_command_checkpoint(0, source.id().cluster_epoch())
            .unwrap();
        pg.compact_metadata_command_log(source.id().cluster_epoch())
            .unwrap();
        assert_eq!(
            pg.metadata_command_log_stats(source.id().cluster_epoch())
                .unwrap()
                .retained_entries,
            0
        );
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let before = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let writes = pg.conn.total_changes();
        for _ in 0..2 {
            let id = successor(context.id());
            context = context.into_reissue(id).unwrap();
            ordinary = ordinary.into_reissue(id).unwrap();
            assert_eq!(context.id(), id);
            let projected = with_time_and_monotonic_override(9_000_000, 90_000_000, || {
                context.pending_retention(&topology)
            })
            .unwrap();
            assert_eq!(projected.command_id(), id);
            assert_eq!(
                projected.routes(),
                sorted(vec![
                    exact(id.cluster_epoch(), id.pg_id()),
                    exact(source.id().cluster_epoch(), source.id().pg_id()),
                    exact(source.id().cluster_epoch(), bucket_pg)
                ])
            );
            assert!(projected.future_window().is_none());
            let ordinary = ordinary.pending_retention(&topology).unwrap();
            assert_eq!(ordinary.command_id(), id);
            assert!(ordinary.routes().is_empty());
            assert!(ordinary.future_window().is_none());
        }
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        assert_eq!(introduction::state(&pg), before);
        // Projection grants no authentication; only the real terminal evidence
        // recorded above permits this exact custody transition.
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert_eq!(
            context
                .application(&topology)
                .unwrap()
                .apply_in_open_transaction(&pg, 0)
                .unwrap(),
            Applied::Abandonment(AbandonmentOutcome::ClosedAndRetained)
        );
        abandonment::assert_closed(&pg, &owner, &source);
        pg.conn.execute_batch("ROLLBACK").unwrap();
        assert_eq!(introduction::state(&pg), before);
    }
}

fn reserve_version(pg: PgId) -> MetadataCommandEnvelope {
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(5).unwrap(),
            pg,
            MetadataCommandLogIndex::new(20).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            BucketName::new("bucket").unwrap(),
            ObjectKey::new("key").unwrap(),
            VersionId::from_u64(7),
        )),
    )
}

#[test]
fn staging_context_retention_ordinary_proofs_keep_their_original_release_epoch() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = part::initialize(&pg, 2);
    let finalizer = part::command(&pg, &owner);
    let MetadataCommandPayload::CommitStreamPart(part) = finalizer.payload() else {
        unreachable!()
    };
    let expected = vec![release(&part.bucket_write_reservation)];
    let before = introduction::state(&pg);
    let writes = pg.conn.total_changes();
    for (command, expected) in [
        (finalizer.clone(), expected),
        (reserve_version(finalizer.id().pg_id()), Vec::new()),
    ] {
        let mut context = application::ordinary_context(&command, &topology()).unwrap();
        for _ in 0..3 {
            let projected = context.pending_retention(&topology()).unwrap();
            assert_eq!(projected.command_id(), context.id());
            assert_eq!(projected.routes(), expected);
            assert!(projected.future_window().is_none());
            let id = successor(context.id());
            context = context.into_reissue(id).unwrap();
            assert_eq!(context.id(), id);
        }
    }
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(&pg), before);
    pg.conn.execute_batch("ROLLBACK").unwrap();
}

fn reject_foreign(pg: &PgStore, context: &ContextualCommand, owner: &StagingOwnerRecord) {
    let topology = topology();
    let foreign = (1..256_u32)
        .find_map(|mask| {
            let ids = (0..8)
                .filter(|id| mask & (1 << id) != 0)
                .collect::<Vec<_>>();
            let candidate = PgTopology::new(&ids).unwrap();
            (candidate.object_pg_for(&owner.subject.bucket, &owner.subject.key)
                == owner.subject.object_pg.get()
                && candidate.bucket_pg_for(&owner.subject.bucket)
                    != topology.bucket_pg_for(&owner.subject.bucket))
            .then_some(candidate)
        })
        .expect("same object PG, different bucket release PG");
    assert_eq!(
        foreign.object_pg_for(&owner.subject.bucket, &owner.subject.key),
        owner.subject.object_pg.get()
    );
    assert_ne!(
        foreign.bucket_pg_for(&owner.subject.bucket),
        topology.bucket_pg_for(&owner.subject.bucket)
    );
    let before = introduction::state(pg);
    let writes = pg.conn.total_changes();
    let expected = context.pending_retention(&topology).unwrap();
    assert!(matches!(
        context.pending_retention(&foreign),
        Err(ContextError::Candidate(CandidateBindingError::Topology))
    ));
    assert_eq!(context.pending_retention(&topology).unwrap(), expected);
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(pg), before);
}

#[test]
fn staging_context_retention_rejects_crossed_topology_for_every_variant() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target, is_append);
        let candidate = candidate::capture(&pg, &owner, command, is_append);
        let context = ContextualCommand::described(candidate, &topology()).unwrap();
        reject_foreign(&pg, &context, &owner);
    }
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = part::initialize(&pg, 2);
    let source = part::command(&pg, &owner);
    let cleanup = abandonment::cleanup(&pg, &owner, &source);
    reject_foreign(&pg, &source_context(&source, &cleanup), &owner);
    reject_foreign(
        &pg,
        &application::ordinary_context(&source, &topology()).unwrap(),
        &owner,
    );
    reject_foreign(
        &pg,
        &application::ordinary_context(&cleanup, &topology()).unwrap(),
        &owner,
    );
    pg.conn.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn staging_context_retention_rejects_an_ordinary_command_outside_configured_pgs() {
    let context =
        application::ordinary_context(&reserve_version(PgId::new(8)), &topology()).unwrap();
    assert!(matches!(
        context.pending_retention(&topology()),
        Err(ContextError::Retention(
            StoreError::RouteCapabilitySubjectMismatch {
                operation: "pending-context-command-pg"
            }
        ))
    ));
    let valid = application::ordinary_context(&reserve_version(PgId::new(0)), &topology()).unwrap();
    assert!(valid
        .pending_retention(&topology())
        .unwrap()
        .routes()
        .is_empty());
}
