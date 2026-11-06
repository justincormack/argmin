// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Materialized-owner retention tests. Private SQL seeds represent retained
//! responsibility after the original session/reservation/log has disappeared.
//! They do not claim that introduction commands or physical cleanup are wired.

use super::*;
use crate::pg_store::{
    PgClusterMapHistoryReferenceSummary, PgClusterMapHistoryRouteReference,
    PgClusterMapHistoryRouteReferenceKind, PgClusterMapHistoryRouteReferences,
};

fn distinct_route_owner(target: StagingTarget) -> StagingOwnerRecord {
    for index in 0..1_000 {
        let mut owner = fixture(target.clone());
        owner.subject.key = crate::tests::object_key(format!("history-{index}"));
        owner.subject.object_pg =
            PgId::new(topology().object_pg_for(&owner.subject.bucket, &owner.subject.key));
        owner.subject.id.introduction = MetadataCommandId::new(
            owner.subject.originating_epoch,
            owner.subject.object_pg,
            owner.subject.id.introduction.log_index(),
        );
        owner.windows[0] = window(&owner.subject, 0, owner.subject.first_vid());
        let creation = match &mut owner.resources.creation {
            ResourceResponsibility::Live(resource)
            | ResourceResponsibility::ReleaseDue(resource) => resource,
            ResourceResponsibility::NoResource => unreachable!(),
        };
        creation.identity.target_context = Some(owner.subject.key.as_str().into());
        let object = owner.subject.object_pg;
        let data = owner.windows[0].participants.data_pg;
        let bucket = creation.release_pg;
        if object != data && object != bucket && data != bucket {
            owner.validate(&topology()).unwrap();
            return owner;
        }
    }
    panic!("no independent object/data/bucket PGs found");
}

fn reference_at(pg: PgId, epoch: ClusterEpoch) -> PgClusterMapHistoryRouteReference {
    PgClusterMapHistoryRouteReference::new(
        PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource,
        epoch,
        pg,
    )
}

fn expected_routes(owner: &StagingOwnerRecord) -> PgClusterMapHistoryRouteReferences {
    let mut expected = vec![reference_at(
        owner.subject.object_pg,
        owner.subject.originating_epoch,
    )];
    for window in &owner.windows {
        expected.push(reference_at(
            window.participants.data_pg,
            window.participants.placement_epoch,
        ));
    }
    for resource in [&owner.resources.creation, &owner.resources.finalization] {
        if let ResourceResponsibility::Live(resource)
        | ResourceResponsibility::ReleaseDue(resource) = resource
        {
            expected.push(reference_at(
                resource.release_pg,
                resource.identity.cluster_epoch,
            ));
        }
    }
    for resource in &owner.resources.abandoned_finalizations {
        expected.push(reference_at(
            resource.release_pg,
            resource.identity.cluster_epoch,
        ));
    }
    PgClusterMapHistoryRouteReferences::try_from_iter(expected).unwrap()
}

fn assert_no_other_history_sources(store: &PgStore) {
    for table in [
        "object_segments",
        "multipart_part_segments",
        "stream_upload_segments",
        "stream_uploads",
        "object_generation_reservations",
        "bucket_write_reservations",
        "metadata_command_pending_slot",
        "metadata_command_log",
        "placed_segment_shard_backfills",
        "object_payload_reclaim_claims",
        "shards",
    ] {
        let count: u64 = store
            .conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "{table} would mask missing owner retention");
    }
}

fn assert_routes(store: &PgStore, owner: &StagingOwnerRecord) {
    assert_no_other_history_sources(store);
    let expected = expected_routes(owner);
    let references = store
        .cluster_map_history_route_references(&topology())
        .unwrap();
    assert_eq!(references, expected);
    let summary = store.cluster_map_history_reference_summary().unwrap();
    assert_eq!(summary, expected.summary());
    assert_eq!(
        summary.oldest_required_epoch(),
        Some(owner.subject.originating_epoch)
    );
}

#[test]
fn staging_history_empty_windows_retain_three_distinct_routes_after_expiry() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let owner = distinct_route_owner(target);
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
        assert_eq!(
            store.cluster_map_history_reference_summary().unwrap(),
            Default::default()
        );
        assert!(store
            .cluster_map_history_route_references(&topology())
            .unwrap()
            .is_empty());
        seed(&store, &owner);
        assert!(owner.references.is_empty());
        assert_eq!(expected_routes(&owner).len(), 3);
        assert!(!owner.windows[0]
            .participants
            .files
            .contains(&owner.windows[0].participants.acknowledgement_primary,));
        // Neither a passed physical cutoff nor a passed proof lease confirms
        // deletion/release. Retention must not sample the clock at all.
        crate::clock::with_time_override(owner.maximum_cutoff_ms + 1_000_000, || {
            assert_routes(&store, &owner);
        });
    }
}

#[test]
fn staging_obligations_history_pins_each_additional_historical_release_route() {
    let owner = owner_with_abandoned_obligations();
    let tmp = test_util::tempdir();
    let store = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
    seed(&store, &owner);
    assert_routes(&store, &owner);
    let routes = store
        .cluster_map_history_route_references(&topology())
        .unwrap();
    for proof in &owner.resources.abandoned_finalizations {
        assert!(routes
            .iter()
            .any(|route| route == reference_at(proof.release_pg, proof.identity.cluster_epoch)));
    }
    drop(store);
    let store = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
    crate::clock::with_time_override(9_000_000, || assert_routes(&store, &owner));
}

fn multi_epoch_owner(target: StagingTarget, lifecycle: StagingLifecycle) -> StagingOwnerRecord {
    let mut owner = distinct_route_owner(target);
    if owner.subject.target != StagingTarget::DirectPut {
        append(&mut owner);
        append(&mut owner);
    }
    // Give every window, including unused lookahead, an independently visible
    // epoch. Do not let a selected window's route mask omission of lookahead.
    for (index, window) in owner.windows.iter_mut().enumerate() {
        window.participants.placement_epoch = ClusterEpoch::new(6 + index as u64).unwrap();
    }
    if lifecycle != StagingLifecycle::Active
        && lifecycle != StagingLifecycle::Retiring(StagingRetirement::Unpublished)
    {
        publish(&mut owner);
    }
    owner.lifecycle = lifecycle;
    if let StagingLifecycle::Retiring(retirement) = lifecycle {
        release_due(&mut owner.resources.generation);
        release_due(&mut owner.resources.creation);
        if retirement != StagingRetirement::PublishedRetained {
            owner.references.clear();
        }
    }
    if let ResourceResponsibility::ReleaseDue(resource) = &mut owner.resources.finalization {
        resource.identity.cluster_epoch = ClusterEpoch::new(11).unwrap();
    }
    owner.validate(&topology()).unwrap();
    owner
}

#[test]
fn staging_history_keeps_all_windows_and_release_roles_in_every_lifecycle() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        for lifecycle in [
            StagingLifecycle::Active,
            StagingLifecycle::Published,
            StagingLifecycle::Retiring(StagingRetirement::Unpublished),
            StagingLifecycle::Retiring(StagingRetirement::PublishedRetained),
            StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced),
        ] {
            let owner = multi_epoch_owner(target.clone(), lifecycle);
            let tmp = test_util::tempdir();
            let store = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
            seed(&store, &owner);
            assert_routes(&store, &owner);
        }
    }
}

#[test]
fn staging_history_checkpoint_import_and_reopen_preserve_original_routes() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let owner = multi_epoch_owner(
            target,
            StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced),
        );
        let checkpoint = checkpoint(&owner);
        let tmp = test_util::tempdir();
        let destination_epoch = ClusterEpoch::new(19).unwrap();
        {
            let destination = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
            destination
                .install_metadata_transfer_checkpoint_base(
                    0,
                    destination_epoch,
                    &checkpoint,
                    &topology(),
                )
                .unwrap();
            assert_routes(&destination, &owner);
        }
        let reopened = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
        assert_routes(&reopened, &owner);
        assert!(!reopened
            .cluster_map_history_route_references(&topology())
            .unwrap()
            .iter()
            .any(|reference| reference.cluster_epoch() == destination_epoch));
    }
}

#[test]
fn staging_history_node_discovery_and_wire_response_preserve_exact_routes() {
    use crate::node_runtime::engine::SharedStorageNode;
    use crate::storage_rpc::{
        decode_cluster_map_history_reference_summary_response,
        encode_cluster_map_history_reference_summary_response,
        StorageRpcClusterMapHistoryReferenceSummaryResponse,
    };

    let owner = multi_epoch_owner(multipart_target(), StagingLifecycle::Published);
    let tmp = test_util::tempdir();
    let node = SharedStorageNode::open_with_default_ec_shape(
        tmp.path(),
        &[0, 1, 2, 3, 4, 5, 6, 7],
        EcShape { k: 2, m: 1 },
    )
    .unwrap();
    seed(&node.get_pg(owner.subject.object_pg.get()).unwrap(), &owner);
    let references = node.cluster_map_history_route_references().unwrap();
    assert_eq!(references, expected_routes(&owner));
    assert_eq!(
        node.cluster_map_history_reference_summary().unwrap(),
        references.summary()
    );
    let response = StorageRpcClusterMapHistoryReferenceSummaryResponse { references };
    let encoded = encode_cluster_map_history_reference_summary_response(&response);
    assert_eq!(
        decode_cluster_map_history_reference_summary_response(&encoded).unwrap(),
        response
    );
}

#[test]
fn staging_history_owner_floor_survives_two_runtime_map_refreshes() {
    use crate::control_plane::{
        FileControlPlaneStore, NodeMembershipState, SingleAuthorityControlPlane,
    };
    use crate::storage_node_server::StorageNodeProcessConfig;

    let tmp = test_util::tempdir();
    let node_id = NodeId::new(4);
    let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
        tmp.path().join("control-plane.state"),
    ))
    .unwrap();
    authority
        .bootstrap_initial_cluster_map(
            (0..5)
                .map(|node| {
                    (
                        NodeId::new(node),
                        format!("/tmp/staging-history-node-{node}.sock"),
                    )
                })
                .collect(),
            (0..8).map(PgId::new).collect(),
        )
        .unwrap();
    let initial_map = authority.snapshot().runtime_map(1_000).unwrap();
    let original_epoch = initial_map.cluster_epoch();
    let mut owner = distinct_route_owner(multipart_target());
    owner.subject.originating_epoch = original_epoch;
    owner.subject.id.introduction = MetadataCommandId::new(
        original_epoch,
        owner.subject.object_pg,
        owner.subject.id.introduction.log_index(),
    );
    owner.windows[0] = window(&owner.subject, 0, owner.subject.first_vid());
    let ResourceResponsibility::ReleaseDue(creation) = &mut owner.resources.creation else {
        unreachable!();
    };
    creation.identity.cluster_epoch = original_epoch;
    owner.validate(&topology()).unwrap();
    let store = PgStore::open(&tmp.path().join("owner"), owner.subject.object_pg.get()).unwrap();
    seed(&store, &owner);
    let mut current = StorageNodeProcessConfig::from_runtime_map(
        node_id,
        tmp.path().join("node"),
        EcShape { k: 2, m: 1 },
        &initial_map,
    )
    .unwrap();
    let original_routes = current.pg_routes().to_vec();
    let mut unreferenced = current.clone();
    for step in 0..2 {
        authority
            .set_node_membership(NodeId::new(100 + step), NodeMembershipState::Active)
            .unwrap();
        let delta = authority
            .snapshot()
            .runtime_map_for_storage_node_refresh(
                2_000 + u64::from(step),
                node_id,
                current.cluster_epoch(),
            )
            .unwrap();
        current = StorageNodeProcessConfig::from_runtime_map_refresh(
            &current,
            &delta,
            store.cluster_map_history_reference_summary().unwrap(),
        )
        .unwrap();
        unreferenced = StorageNodeProcessConfig::from_runtime_map_refresh(
            &unreferenced,
            &delta,
            PgClusterMapHistoryReferenceSummary::default(),
        )
        .unwrap();
        for reference in expected_routes(&owner).iter() {
            let original = original_routes
                .iter()
                .find(|route| route.pg_id == reference.pg_id().get())
                .unwrap();
            assert!(current.historical_pg_routes().contains(original));
        }
    }
    assert!(
        !unreferenced
            .historical_pg_routes()
            .iter()
            .any(|route| route.cluster_epoch == original_epoch),
        "without owner evidence the second refresh must prune the canary epoch"
    );
    assert_routes(&store, &owner);
}
