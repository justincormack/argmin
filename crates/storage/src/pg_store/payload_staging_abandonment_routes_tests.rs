// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Source-bound route projection, not installed pending-carrier activation.

use super::*;
use crate::node_runtime::pg_store::command_log::{
    PendingMetadataCommandRouteDependency, PendingRouteTarget,
};

fn expected_routes(
    source: &MetadataCommandEnvelope,
    follow_up: &MetadataCommandEnvelope,
    bucket_pg: PgId,
) -> Vec<PendingMetadataCommandRouteDependency> {
    let mut routes = vec![
        PendingMetadataCommandRouteDependency {
            cluster_epoch: source.id().cluster_epoch(),
            target: PendingRouteTarget::ExactPg(source.id().pg_id()),
        },
        PendingMetadataCommandRouteDependency {
            cluster_epoch: source.id().cluster_epoch(),
            target: PendingRouteTarget::ExactPg(bucket_pg),
        },
        PendingMetadataCommandRouteDependency {
            cluster_epoch: follow_up.id().cluster_epoch(),
            target: PendingRouteTarget::ExactPg(follow_up.id().pg_id()),
        },
    ];
    routes.sort_unstable();
    routes.dedup();
    routes
}

#[test]
fn staging_abandonment_routes_include_the_independent_original_bucket_release_pg() {
    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owner = part::initialize(&pg, count);
        let source = part::command(&pg, &owner);
        let follow_up = cleanup(&pg, &owner, &source);
        let topology = topology();
        let bucket_pg = PgId::new(topology.bucket_pg_for(&owner.subject.bucket));
        assert_ne!(bucket_pg, owner.subject.object_pg);
        assert_eq!(
            follow_up
                .payload()
                .bucket_write_reservation_route_dependencies()
                .count(),
            0,
            "bare UploadPart abort cannot retain its finalizer's release route"
        );
        let candidate = PartFinalizationSource::capture_for_test(&source)
            .unwrap()
            .bind_cleanup(follow_up.clone(), &topology)
            .unwrap();
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        let routes = candidate
            .bind(&topology)
            .unwrap()
            .pending_route_dependencies();
        assert_eq!(
            routes.len(),
            2,
            "deduplicate the same-epoch object route only"
        );
        assert_eq!(routes, expected_routes(&source, &follow_up, bucket_pg));
        assert!(routes
            .iter()
            .all(|route| route.target.resolve(&topology).is_ok()));
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.conn.total_changes(), writes);
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

#[test]
fn staging_abandonment_routes_keep_original_source_and_release_epochs_after_reissue_and_reopen() {
    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg_id = fixture(multipart_target()).subject.object_pg.get();
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let owner = part::initialize(&pg, count);
        let source = part::command(&pg, &owner);
        let follow_up = cleanup(&pg, &owner, &source);
        let original = PartFinalizationSource::capture_for_test(&source)
            .unwrap()
            .encode();
        let topology = topology();
        let bucket_pg = PgId::new(topology.bucket_pg_for(&owner.subject.bucket));
        assert_ne!(bucket_pg, owner.subject.object_pg);
        let mut candidate = PartFinalizationSource::capture_for_test(&source)
            .unwrap()
            .bind_cleanup(follow_up.clone(), &topology)
            .unwrap();
        terminal(&pg, &source, true);
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
        for epoch in [
            source.id().cluster_epoch().get() + 1,
            source.id().cluster_epoch().get() + 2,
        ] {
            candidate = candidate
                .reissued_at(MetadataCommandId::new(
                    ClusterEpoch::new(epoch).unwrap(),
                    source.id().pg_id(),
                    MetadataCommandLogIndex::new(1).unwrap(),
                ))
                .unwrap();
            // Simulate the receiver's required-source decode after restart.
            candidate = super::super::super::abandonment::source::AbandonedPartCleanupCommand::decode_source_field(
                candidate.command().clone(),
                &candidate.source_bytes(),
                &topology,
            )
            .unwrap();
            assert_eq!(candidate.source_bytes(), original);
            let routes =
                crate::clock::with_time_and_monotonic_override(9_000_000, 90_000_000, || {
                    candidate
                        .bind(&topology)
                        .unwrap()
                        .pending_route_dependencies()
                });
            assert_eq!(routes.len(), 3);
            assert_eq!(
                routes,
                expected_routes(&source, candidate.command(), bucket_pg)
            );
            assert!(
                !routes.contains(&PendingMetadataCommandRouteDependency {
                    cluster_epoch: candidate.command().id().cluster_epoch(),
                    target: PendingRouteTarget::ExactPg(bucket_pg),
                }),
                "the original release route was replaced by the new epoch"
            );
            let same_epoch = candidate
                .reissued_at(MetadataCommandId::new(
                    candidate.command().id().cluster_epoch(),
                    source.id().pg_id(),
                    MetadataCommandLogIndex::new(2).unwrap(),
                ))
                .unwrap();
            assert_eq!(
                same_epoch
                    .bind(&topology)
                    .unwrap()
                    .pending_route_dependencies(),
                routes
            );
            candidate = same_epoch;
        }
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        assert_eq!(pg.conn.total_changes(), writes);
    }
}

#[test]
fn staging_abandonment_routes_reject_a_same_object_pg_topology_with_a_different_release_pg() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = part::initialize(&pg, 2);
    let source = part::command(&pg, &owner);
    let follow_up = cleanup(&pg, &owner, &source);
    let topology = topology();
    let candidate = PartFinalizationSource::capture_for_test(&source)
        .unwrap()
        .bind_cleanup(follow_up.clone(), &topology)
        .unwrap();
    let foreign = (1..256_u32)
        .find_map(|mask| {
            let ids = (0..8)
                .filter(|id| mask & (1 << id) != 0)
                .collect::<Vec<_>>();
            let other = PgTopology::new(&ids).unwrap();
            (other.object_pg_for(&owner.subject.bucket, &owner.subject.key) == pg.pg_id
                && other.bucket_pg_for(&owner.subject.bucket)
                    != topology.bucket_pg_for(&owner.subject.bucket))
            .then_some(other)
        })
        .expect("same object PG, independently different bucket release PG");
    assert_eq!(
        foreign.object_pg_for(&owner.subject.bucket, &owner.subject.key),
        pg.pg_id
    );
    assert_ne!(
        foreign.bucket_pg_for(&owner.subject.bucket),
        topology.bucket_pg_for(&owner.subject.bucket)
    );
    let before = introduction::state(&pg);
    let writes = pg.conn.total_changes();
    assert!(matches!(
        candidate.bind(&foreign),
        Err(AbandonmentError::Topology)
    ));
    assert_eq!(
        candidate
            .bind(&topology)
            .unwrap()
            .pending_route_dependencies(),
        expected_routes(
            &source,
            &follow_up,
            PgId::new(topology.bucket_pg_for(&owner.subject.bucket))
        ),
    );
    assert_eq!(introduction::state(&pg), before);
    assert_eq!(pg.conn.total_changes(), writes);
    pg.conn.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn staging_abandonment_route_projection_does_not_authenticate_terminal_source() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = part::initialize(&pg, 2);
    let source = part::command(&pg, &owner);
    let follow_up = cleanup(&pg, &owner, &source);
    let topology = topology();
    let candidate = PartFinalizationSource::capture_for_test(&source)
        .unwrap()
        .bind_cleanup(follow_up, &topology)
        .unwrap();
    let before = introduction::state(&pg);
    let writes = pg.conn.total_changes();
    let routes = candidate
        .bind(&topology)
        .unwrap()
        .pending_route_dependencies();
    assert!(!routes.is_empty());
    assert!(matches!(
        candidate
            .bind(&topology)
            .unwrap()
            .apply_in_open_transaction(&pg, 0),
        Err(AbandonmentError::NotAbandoned)
    ));
    assert_eq!(introduction::state(&pg), before);
    assert_eq!(pg.conn.total_changes(), writes);
    // Positive authentication canary: projection did not close or otherwise
    // mutate the owner; real tombstone evidence still enables the transition.
    terminal(&pg, &source, true);
    assert_eq!(
        candidate
            .bind(&topology)
            .unwrap()
            .pending_route_dependencies(),
        routes
    );
    assert_eq!(
        candidate
            .bind(&topology)
            .unwrap()
            .apply_in_open_transaction(&pg, 0)
            .unwrap(),
        AbandonmentOutcome::ClosedAndRetained
    );
    assert_closed(&pg, &owner, &source);
    pg.conn.execute_batch("ROLLBACK").unwrap();
}
