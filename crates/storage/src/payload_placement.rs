// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Shared, deterministic payload placement computation.
//!
//! This is NOT route or write authority. Callers supply an already captured
//! route and retain responsibility for admission, active/retained route checks,
//! deadlines and physical exclusion. No runtime-map lookup or I/O occurs here.
//! In particular, file locations do not select the acknowledgement owner: that
//! is the captured data-PG metadata primary, independently of shard index.

use placement::{NodeId, PlacementConstraint, PlacementError, TopologyKey};

use crate::cluster::ShardLocation;
use crate::error::ClusterBuildError;
use crate::{ClusterEpoch, DataPgId, EcShape, ShardIndex};

const PAYLOAD_SHARD_PLACEMENT_KEY_DOMAIN: &[u8] = b"argmin/payload-shard-placement/v1";

pub(crate) fn place_payload_shards_for_pg_route(
    cluster_epoch: ClusterEpoch,
    data_pg_id: DataPgId,
    ec_shape: EcShape,
    stable_placement_key: &[u8],
    acting_set: &[NodeId],
) -> Result<Vec<ShardLocation>, ClusterBuildError> {
    let ec_config = ec_config_for_shape(ec_shape)?;
    let total_shards = ec_config.total_shards();
    let placement_map = build_local_placement_map(acting_set.iter().copied())?;
    let placer = local_payload_placer(&placement_map, ec_shape)?;
    let placement_key = payload_shard_placement_key(data_pg_id, stable_placement_key);
    let mut node_ids = vec![NodeId::new(0); total_shards];
    placer
        .place(&placement_key, &mut node_ids)
        .map_err(|error| placement_error_for_shape(ec_shape, error))?;

    Ok(node_ids
        .into_iter()
        .enumerate()
        .map(|(shard_index, node_id)| {
            ShardLocation::new(
                cluster_epoch,
                data_pg_id,
                ShardIndex::new(shard_index as u8),
                node_id,
            )
        })
        .collect())
}

pub(crate) fn validate_local_payload_placement(
    placement_map: &placement::ClusterMap,
    default_ec_shape: EcShape,
) -> Result<(), ClusterBuildError> {
    local_payload_placer(placement_map, default_ec_shape)?;
    Ok(())
}

fn local_payload_placer(
    placement_map: &placement::ClusterMap,
    ec_shape: EcShape,
) -> Result<placement::Placer, ClusterBuildError> {
    let ec_config = ec_config_for_shape(ec_shape)?;
    let placement_config = placement::PlacementConfig::new(ec_config.total_shards())
        .map_err(|error| invalid_ec_shape_error(ec_shape, error))?;
    placement::Placer::new(placement_config, placement_map, PlacementConstraint::none())
        .map_err(|error| placement_error_for_shape(ec_shape, error))
}

fn ec_config_for_shape(ec_shape: EcShape) -> Result<ec::EcConfig, ClusterBuildError> {
    ec::EcConfig::new(ec_shape.k, ec_shape.m)
        .map_err(|error| invalid_ec_shape_error(ec_shape, error))
}

fn invalid_ec_shape_error(ec_shape: EcShape, error: impl std::fmt::Display) -> ClusterBuildError {
    ClusterBuildError::InvalidEcShape {
        data_shards: ec_shape.k,
        parity_shards: ec_shape.m,
        reason: error.to_string(),
    }
}

fn placement_error_for_shape(ec_shape: EcShape, error: PlacementError) -> ClusterBuildError {
    match error {
        PlacementError::TooFewNodes { shards, nodes } => ClusterBuildError::UnplaceableEcShape {
            data_shards: ec_shape.k,
            parity_shards: ec_shape.m,
            required_nodes: shards,
            node_count: nodes,
        },
        other => ClusterBuildError::InvalidLocalPlacement {
            reason: other.to_string(),
        },
    }
}

pub(crate) fn build_local_placement_map(
    node_ids: impl IntoIterator<Item = NodeId>,
) -> Result<placement::ClusterMap, ClusterBuildError> {
    let placement_nodes: Vec<placement::NodeInfo> = node_ids
        .into_iter()
        .map(|node_id| placement::NodeInfo {
            id: node_id,
            location: TopologyKey::rack_machine(node_id.as_u32(), node_id.as_u32()),
            weight: 1.0,
        })
        .collect();
    placement::ClusterMap::new(&placement_nodes).map_err(|error| {
        ClusterBuildError::InvalidLocalPlacement {
            reason: error.to_string(),
        }
    })
}

fn payload_shard_placement_key(data_pg_id: DataPgId, stable_placement_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(
        PAYLOAD_SHARD_PLACEMENT_KEY_DOMAIN.len()
            + std::mem::size_of::<u32>()
            + stable_placement_key.len(),
    );
    key.extend_from_slice(PAYLOAD_SHARD_PLACEMENT_KEY_DOMAIN);
    key.extend_from_slice(&data_pg_id.get().to_be_bytes());
    key.extend_from_slice(stable_placement_key);
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PgId;

    #[test]
    fn payload_placement_key_preserves_frozen_domain_and_big_endian_pg() {
        assert_eq!(
            payload_shard_placement_key(
                DataPgId::new_for_test(PgId::new(0x0102_0304)),
                b"\x00\xffsegment"
            ),
            b"argmin/payload-shard-placement/v1\x01\x02\x03\x04\x00\xffsegment"
        );
    }

    #[test]
    fn payload_placement_uses_canonical_candidates_not_acting_set_positions() {
        let pg = DataPgId::new_for_test(PgId::new(7));
        let epoch = ClusterEpoch::new(9).unwrap();
        for acting in [
            [4, 2, 3],
            [3, 4, 2],
            [2, 3, 4],
            [4, 3, 2],
            [3, 2, 4],
            [2, 4, 3],
        ] {
            let locations = place_payload_shards_for_pg_route(
                epoch,
                pg,
                EcShape { k: 2, m: 1 },
                b"segment",
                &acting.map(NodeId::new),
            )
            .unwrap();
            assert_eq!(locations.len(), 3);
            for (index, node) in [2, 3, 4].into_iter().enumerate() {
                assert_eq!(locations[index].node_id(), NodeId::new(node));
                assert_eq!(locations[index].shard_index().get(), index as u8);
                assert_eq!(locations[index].data_pg_id(), pg);
                assert_eq!(locations[index].cluster_epoch(), epoch);
            }
        }
    }

    #[test]
    fn payload_placement_overfull_acting_set_preserves_keyed_placement() {
        let nodes = [8, 3, 19, 2, 14, 5, 11, 7].map(NodeId::new);
        // Independent reference uses literal framed bytes, not the production
        // key helper, so dropping the domain or PG prefix is observable even
        // though both calls use the existing placement engine.
        let reference_nodes = nodes.map(|id| placement::NodeInfo {
            id,
            location: TopologyKey::rack_machine(id.as_u32(), id.as_u32()),
            weight: 1.0,
        });
        let reference_map = placement::ClusterMap::new(&reference_nodes).unwrap();
        for shape in [
            EcShape { k: 1, m: 0 },
            EcShape { k: 2, m: 1 },
            EcShape { k: 4, m: 2 },
        ] {
            let count = usize::from(shape.k) + usize::from(shape.m);
            let reference = placement::Placer::new(
                placement::PlacementConfig::new(count).unwrap(),
                &reference_map,
                PlacementConstraint::none(),
            )
            .unwrap();
            for pg_id in [0_u32, 7, 255] {
                for key in [b"".as_slice(), b"segment", b"another-segment"] {
                    let mut reference_key = b"argmin/payload-shard-placement/v1".to_vec();
                    reference_key.extend_from_slice(&pg_id.to_be_bytes());
                    reference_key.extend_from_slice(key);
                    let mut expected = vec![NodeId::new(0); count];
                    reference.place(&reference_key, &mut expected).unwrap();
                    let locations = place_payload_shards_for_pg_route(
                        ClusterEpoch::INITIAL,
                        DataPgId::new_for_test(PgId::new(pg_id)),
                        shape,
                        key,
                        &nodes,
                    )
                    .unwrap();
                    assert_eq!(
                        locations
                            .iter()
                            .map(ShardLocation::node_id)
                            .collect::<Vec<_>>(),
                        expected
                    );
                }
            }
        }
    }

    #[test]
    fn payload_placement_rejects_invalid_or_unplaceable_inputs() {
        let place = |shape, nodes: &[NodeId]| {
            place_payload_shards_for_pg_route(
                ClusterEpoch::INITIAL,
                DataPgId::new_for_test(PgId::new(0)),
                shape,
                b"segment",
                nodes,
            )
        };
        assert!(matches!(
            place(EcShape { k: 0, m: 1 }, &[NodeId::new(2)]),
            Err(ClusterBuildError::InvalidEcShape { .. })
        ));
        assert!(matches!(
            place(EcShape { k: 2, m: 1 }, &[NodeId::new(2), NodeId::new(3)]),
            Err(ClusterBuildError::UnplaceableEcShape {
                required_nodes: 3,
                node_count: 2,
                ..
            })
        ));
        assert!(place(EcShape { k: 1, m: 0 }, &[]).is_err());
        assert!(matches!(
            place(EcShape { k: 1, m: 0 }, &[NodeId::new(2), NodeId::new(2)]),
            Err(ClusterBuildError::InvalidLocalPlacement { .. })
        ));
    }
}
