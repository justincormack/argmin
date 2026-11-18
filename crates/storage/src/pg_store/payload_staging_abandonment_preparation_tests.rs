// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Admitted read-only preparation from real all-member tombstones. Metadata
//! fixtures only; no physical-cleanup or live publication activation claim.

use super::*;
use crate::cluster::EmbeddedPublicationSection;
use crate::metadata_command::MetadataCommandEnvelope;
use crate::node_runtime::pg_store::payload_staging::abandonment::source::PartFinalizationSource;
use crate::node_runtime::pg_store::CompleteAbandonmentHandoff;
use crate::{LocalClusterMap, StorageClusterRouteAdmission, StorageClusterRouteHandle};
use std::sync::Arc;

pub(super) fn setup(
    dir: &test_util::TempDir,
    count: usize,
) -> (
    Arc<LocalClusterMap>,
    StorageClusterRouteHandle,
    ContextualCommand,
    ContextualCommand,
) {
    let (map, handle, source, cleanup, ()) = setup_with_seed(dir, count, |_, _| ());
    (map, handle, source, cleanup)
}

pub(super) fn setup_with_seed<T>(
    dir: &test_util::TempDir,
    count: usize,
    capture: impl FnOnce(&PgStore, &StagingOwnerRecord) -> T,
) -> (
    Arc<LocalClusterMap>,
    StorageClusterRouteHandle,
    ContextualCommand,
    ContextualCommand,
    T,
) {
    let seed_dir = test_util::tempdir();
    let seed = PgStore::open(
        seed_dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = part::initialize(&seed, count);
    let captured = capture(&seed, &owner);
    let raw = part::command(&seed, &owner);
    let source = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            raw.id().cluster_epoch(),
            raw.id().pg_id(),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        raw.payload().clone(),
    );
    let cleanup = ContextualCommand::abandoned_part(
        PartFinalizationSource::capture_for_test(&source)
            .unwrap()
            .bind_cleanup(abandonment::cleanup(&seed, &owner, &source), &topology())
            .unwrap(),
        &topology(),
    )
    .unwrap();
    let context = abandonment::source_context(&source);
    seed.refresh_metadata_command_state_digest().unwrap();
    seed.conn.execute_batch("COMMIT").unwrap();
    let (map, handle, context) =
        application_embedded::from_seed(dir, &seed, context, 2, RouteMapValidity::Forever);
    (map, handle, context, cleanup, captured)
}

pub(super) fn handoff<'a>(
    map: &LocalClusterMap,
    admission: &'a StorageClusterRouteAdmission,
    section: &EmbeddedPublicationSection<'_>,
    context: &ContextualCommand,
    origin: &str,
    cutoff: Instant,
) -> CompleteAbandonmentHandoff<'a> {
    let mut owner = unstarted_abandonment::sender(map, context, origin, cutoff)
        .into_fanout(
            admission
                .complete_fanout_route(context.id().pg_id(), context.id().cluster_epoch())
                .unwrap(),
        )
        .unwrap_or_else(|(_, e)| panic!("{e}"))
        .into_unstarted_abandonment()
        .unwrap_or_else(|(_, e)| panic!("{e}"));
    owner.record_primary_embedded(section, deadline()).unwrap();
    for node in [2, 0, 3, 1] {
        owner
            .record_replica_embedded(section, NodeId::new(node), deadline())
            .unwrap();
    }
    owner
        .into_abandoned()
        .unwrap_or_else(|e| panic!("{}", e.error()))
}

#[test]
fn staging_abandonment_preparation_captures_exact_finalizer_and_keeps_all_origins_without_writes() {
    for count in [0, 2] {
        for origin in ["inspected", "installed", "uncertain"] {
            let dir = test_util::tempdir();
            let (map, handle, context, cleanup) = setup(&dir, count);
            let admission = handle.admit_current_route().unwrap();
            let route = admission
                .embedded_publication_route(
                    NodeId::new(4),
                    context.id().pg_id(),
                    context.id().cluster_epoch(),
                )
                .unwrap();
            let section = route.lock_until(deadline()).unwrap();
            let owner = handoff(&map, &admission, &section, &context, origin, deadline());
            let history = owner.installation_observation();
            let node = map.node(NodeId::new(4)).unwrap().test_node();
            let observe = || {
                let pg = node.get_pg(context.id().pg_id().get()).unwrap();
                (snapshot(&pg), pg.conn.total_changes())
            };
            let before = observe();
            let prepared = owner
                .prepare_part_cleanup(&section)
                .unwrap_or_else(|e| panic!("{}", e.error()));
            prepared.assert_contexts_for_test(&context, &cleanup, &topology());
            assert_eq!(prepared.source_id(), context.id());
            assert_eq!(prepared.cleanup_id(), cleanup.id());
            assert_eq!(
                prepared.prefix().applied_log_index,
                context.id().log_index().get()
            );
            assert_eq!(prepared.installation_observation(), history);
            prepared.require_valid().unwrap();
            assert_eq!(observe(), before);
            drop(prepared);
            assert_eq!(
                observe(),
                before,
                "Drop must not clean up or release source custody"
            );
        }
    }
}

#[test]
fn staging_abandonment_preparation_wrong_kind_returns_original_ambiguous_holder() {
    for case in 0..4 {
        let dir = test_util::tempdir();
        let (map, handle, context) =
            application_embedded::setup(&dir, case, RouteMapValidity::Forever);
        let admission = handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(
                NodeId::new(4),
                context.id().pg_id(),
                context.id().cluster_epoch(),
            )
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let owner = handoff(
            &map,
            &admission,
            &section,
            &context,
            "uncertain",
            deadline(),
        );
        let history = owner.installation_observation();
        let node = map.node(NodeId::new(4)).unwrap().test_node();
        let before = snapshot(&node.get_pg(context.id().pg_id().get()).unwrap());
        let failure = owner
            .prepare_part_cleanup(&section)
            .err()
            .expect("wrong kind accepted");
        assert!(
            failure.error().to_string().contains("kind does not match"),
            "{}",
            failure.error()
        );
        let owner = failure.into_handoff();
        owner.assert_context_for_test(&context, &topology());
        assert_eq!(owner.installation_observation(), history);
        assert_eq!(
            snapshot(&node.get_pg(context.id().pg_id().get()).unwrap()),
            before
        );
    }
}

#[test]
fn staging_abandonment_preparation_rechecks_durable_source_and_indexes() {
    for damage in ["scope", "slot", "routes", "tombstone", "started"] {
        let dir = test_util::tempdir();
        let (map, handle, context, _) = setup(&dir, 2);
        let admission = handle.admit_current_route().unwrap();
        let route = admission
            .embedded_publication_route(
                NodeId::new(4),
                context.id().pg_id(),
                context.id().cluster_epoch(),
            )
            .unwrap();
        let section = route.lock_until(deadline()).unwrap();
        let owner = handoff(
            &map,
            &admission,
            &section,
            &context,
            "uncertain",
            deadline(),
        );
        let history = owner.installation_observation();
        let node = map.node(NodeId::new(4)).unwrap().test_node();
        let pg = node.get_pg(context.id().pg_id().get()).unwrap();
        let sql = match damage {
            "scope" => "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
            "slot" => "DELETE FROM metadata_command_pending_slot",
            "routes" => "DELETE FROM metadata_command_pending_route_dependencies",
            "tombstone" => "DELETE FROM metadata_command_log",
            "started" => "UPDATE metadata_command_pending_slot SET publication_started = 1",
            _ => unreachable!(),
        };
        pg.conn.execute_batch(sql).unwrap();
        let before = (snapshot(&pg), pg.conn.total_changes());
        drop(pg);
        let failure = owner.prepare_part_cleanup(&section).err().expect(damage);
        let owner = failure.into_handoff();
        assert_eq!(owner.installation_observation(), history);
        owner.assert_context_for_test(&context, &topology());
        let pg = node.get_pg(context.id().pg_id().get()).unwrap();
        assert_eq!((snapshot(&pg), pg.conn.total_changes()), before, "{damage}");
    }
}

#[test]
fn staging_abandonment_preparation_rejects_foreign_admission_and_expiry_after_capture() {
    let dir = test_util::tempdir();
    let (map, handle, context, _) = setup(&dir, 2);
    let other_dir = test_util::tempdir();
    let (_, other_handle, _, _) = setup(&other_dir, 2);
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .embedded_publication_route(
            NodeId::new(4),
            context.id().pg_id(),
            context.id().cluster_epoch(),
        )
        .unwrap();
    let section = route.lock_until(deadline()).unwrap();
    let cutoff = Instant::now() + Duration::from_secs(2);
    let owner = handoff(&map, &admission, &section, &context, "uncertain", cutoff);
    let history = owner.installation_observation();
    let other_admission = other_handle.admit_current_route().unwrap();
    let other_route = other_admission
        .embedded_publication_route(
            NodeId::new(4),
            context.id().pg_id(),
            context.id().cluster_epoch(),
        )
        .unwrap();
    let other_section = other_route.lock_until(deadline()).unwrap();
    let failure = owner
        .prepare_part_cleanup(&other_section)
        .err()
        .expect("foreign section accepted");
    assert!(
        failure.error().to_string().contains("subject"),
        "{}",
        failure.error()
    );
    let owner = failure.into_handoff();
    let node = map.node(NodeId::new(4)).unwrap().test_node();
    let before = snapshot(&node.get_pg(context.id().pg_id().get()).unwrap());
    let mut captured = false;
    let failure = owner
        .test_prepare_part_cleanup(&section, |stage| {
            assert_eq!(stage, "after-capture");
            captured = true;
            std::thread::sleep(
                cutoff.saturating_duration_since(Instant::now()) + Duration::from_millis(1),
            );
            Ok(())
        })
        .err()
        .expect("expired preparation accepted");
    assert!(captured);
    assert!(
        failure.error().to_string().contains("deadline"),
        "{}",
        failure.error()
    );
    let owner = failure.into_handoff();
    assert_eq!(owner.installation_observation(), history);
    owner.assert_context_for_test(&context, &topology());
    let pg = node.get_pg(context.id().pg_id().get()).unwrap();
    assert!(pg.conn.is_autocommit());
    assert_eq!(snapshot(&pg), before);
}
