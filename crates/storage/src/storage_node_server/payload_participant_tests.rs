// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Computational placement evidence, not a persisted owner/window or RPC grant.

use super::*;
use crate::control_plane::PgRouteSnapshot;
use crate::node_runtime::payload_placement::PayloadPlacementInput;

fn mirror_routes(map: &mut LocalClusterMap, config: &StorageNodeProcessConfig) {
    map.test_install_pg_routes(config.pg_routes.iter().map(|route| {
        PgRouteSnapshot::reconstructed(
            route.cluster_epoch,
            PgId::new(route.pg_id),
            route.primary_node_id,
            route.acting_set.clone(),
            route.state,
        )
    }));
}

#[test]
fn captured_payload_participants_bind_ack_owner_separately_from_file_locations() {
    let tmp = test_util::tempdir();
    let ec = EcShape { k: 2, m: 1 };
    let nodes = [0, 2, 3, 4].map(NodeId::new);
    let mut map = LocalClusterMap::open(tmp.path(), &nodes, &[0, 1], ec).unwrap();
    let mut config = test_config(&tmp);
    config.node_id = nodes[1];
    config.default_ec_shape = ec;
    config.pg_ids = vec![0, 1];
    config.pg_routes = vec![test_route(0), test_route(1)];
    for route in &mut config.pg_routes {
        route.acting_set = vec![nodes[2], nodes[0], nodes[3], nodes[1]];
        route.primary_node_id = nodes[3];
    }
    let epoch = config.cluster_epoch;
    let vid = GenerationId::new(7).unwrap();
    let okh = [3; 16];
    mirror_routes(&mut map, &config);
    let resolve = |input: PayloadPlacementInput<'_>, pg| {
        input.capture_segment_participants(epoch, pg, ec, &okh, vid).unwrap()
    };
    let captured = resolve(PayloadPlacementInput::from_local_map(&map), 0);
    assert_eq!(
        captured.1,
        map.place_payload_shards(
            epoch,
            crate::DataPgId::new_for_test(PgId::new(0)),
            ec,
            &crate::node_runtime::payload_placement::segment_payload_placement_key(&okh, vid),
        )
        .unwrap(),
        "captured file participants must match the existing placed-write path"
    );
    assert_eq!(captured, resolve(PayloadPlacementInput::from_server_config(&config), 0));
    assert_eq!(captured.0, nodes[3]);
    assert_eq!(captured.1.len(), 3);
    for (index, location) in captured.1.iter().enumerate() {
        assert_eq!(location.cluster_epoch(), epoch);
        assert_eq!(location.data_pg_id().get(), 0);
        assert_eq!(usize::from(location.shard_index().get()), index);
    }
    let file_nodes: std::collections::BTreeSet<_> = captured.1.iter().map(|location| location.node_id()).collect();
    assert_eq!(file_nodes.len(), 3);
    let spare = nodes.into_iter().find(|node| !file_nodes.contains(node)).unwrap();
    config.pg_routes[0].primary_node_id = spare;
    mirror_routes(&mut map, &config);
    let with_spare_primary = resolve(PayloadPlacementInput::from_local_map(&map), 0);
    assert_eq!(with_spare_primary, resolve(PayloadPlacementInput::from_server_config(&config), 0));
    assert_eq!(with_spare_primary.0, spare);
    assert_eq!(with_spare_primary.1, captured.1);
    assert!(!with_spare_primary.1.iter().any(|location| location.node_id() == spare));

    // Reordering the same acting set must not reorder shard identities. Move
    // only the ack owner and prove that file placement does not follow it.
    config.pg_routes[0].acting_set.reverse();
    config.pg_routes[0].primary_node_id = with_spare_primary.1[1].node_id();
    mirror_routes(&mut map, &config);
    let changed = resolve(PayloadPlacementInput::from_local_map(&map), 0);
    assert_eq!(changed, resolve(PayloadPlacementInput::from_server_config(&config), 0));
    assert_ne!(changed.0, with_spare_primary.0);
    assert_eq!(changed.1, with_spare_primary.1);
    // Previously captured participants are owned data, not a view of the new
    // catalogue. An unrelated PG keeps its own primary and subject as well.
    assert_eq!(with_spare_primary.0, spare);
    let other = resolve(PayloadPlacementInput::from_server_config(&config), 1);
    assert_eq!(other.0, nodes[3]);
    assert!(other.1.iter().all(|location| location.data_pg_id().get() == 1));
    assert_eq!(other, resolve(PayloadPlacementInput::from_local_map(&map), 1));
}

#[test]
fn captured_payload_participants_reject_invalid_captured_routes() {
    let tmp = test_util::tempdir();
    let ec = EcShape { k: 2, m: 1 };
    let nodes = [0, 2, 3].map(NodeId::new);
    let mut map = LocalClusterMap::open(tmp.path(), &nodes, &[0], ec).unwrap();
    let mut config = test_config(&tmp);
    config.node_id = nodes[1];
    config.default_ec_shape = ec;
    config.pg_routes[0].acting_set = vec![nodes[2], nodes[0], nodes[1]];
    config.pg_routes[0].primary_node_id = nodes[1];
    let epoch = config.cluster_epoch;
    let next_epoch = ClusterEpoch::new(epoch.get() + 1).unwrap();
    let okh = [4; 16];
    let vid = GenerationId::new(8).unwrap();
    for case in 0..7 {
        let mut invalid = config.clone();
        let mut operation_epoch = epoch;
        let mut shape = ec;
        match case {
            0 => operation_epoch = next_epoch,
            1 => invalid.pg_routes[0].cluster_epoch = next_epoch,
            2 => invalid.pg_routes[0].state = PgState::Peering,
            3 => invalid.pg_routes[0].primary_node_id = NodeId::new(99),
            4 => shape = EcShape { k: 1, m: 1 },
            5 => invalid.pg_routes[0].acting_set.push(nodes[0]),
            6 => invalid.pg_routes.clear(),
            _ => unreachable!(),
        }
        mirror_routes(&mut map, &invalid);
        for input in [PayloadPlacementInput::from_local_map(&map), PayloadPlacementInput::from_server_config(&invalid)] {
            let error = input.capture_segment_participants(operation_epoch, 0, shape, &okh, vid).unwrap_err();
            let expected = match case {
                0 => matches!(error, StoreError::StaleMetadataOperation { .. }),
                1 => matches!(error, StoreError::StaleMetadataRoute { .. }),
                2 => matches!(error, StoreError::PgNotActive { .. }),
                3 | 4 => matches!(error, StoreError::RouteCapabilitySubjectMismatch { .. }),
                5 => matches!(error, StoreError::PayloadShardSetMismatch { .. }),
                6 => matches!(error, StoreError::ClusterPgNotFound { .. }),
                _ => unreachable!(),
            };
            assert!(expected, "case {case}: {error:?}");
        }
    }
    mirror_routes(&mut map, &config);
    assert!(PayloadPlacementInput::from_local_map(&map).capture_segment_participants(epoch, 0, ec, &okh, vid).is_ok());
    assert!(PayloadPlacementInput::from_server_config(&config).capture_segment_participants(epoch, 0, ec, &okh, vid).is_ok());
}
