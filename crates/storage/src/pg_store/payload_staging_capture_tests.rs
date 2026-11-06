// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Builder-component tests, not installed-command/physical-write acceptance.

use super::super::capture::{StagingCapture, StagingCaptureError, StagingIntroduction};
use super::*;
use crate::clock::with_time_and_monotonic_override;
use crate::node_runtime::payload_placement::PayloadPlacementInput;
use crate::node_runtime::server::{StorageNodePgRoute, StorageNodeProcessConfig};
use crate::pg_store::PgStore;
use crate::types::AdmittedRouteEffectFence;
use crate::{PgState, RouteMapValidity, StoreError};

pub(super) fn config(owner: &StagingOwnerRecord) -> StorageNodeProcessConfig {
    StorageNodeProcessConfig {
        node_id: NodeId::new(4),
        cluster_epoch: owner.subject.originating_epoch,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: "/unused-staging-capture".into(),
        default_ec_shape: EcShape { k: 2, m: 1 },
        pg_ids: (0..8).collect(),
        socket_path: "/unused-staging-capture.sock".into(),
        pg_routes: (0..8)
            .map(|pg_id| StorageNodePgRoute {
                pg_id,
                cluster_epoch: owner.subject.originating_epoch,
                state: PgState::Active,
                primary_node_id: NodeId::new(4),
                metadata_transfer_destination_epoch: None,
                metadata_read_route: None,
                acting_set: [4, 2, 0, 3, 1].map(NodeId::new).to_vec(),
            })
            .collect(),
        historical_pg_routes: vec![],
        pending_metadata_command_recoveries: vec![],
    }
}

fn input(owner: &StagingOwnerRecord) -> StagingIntroduction {
    StagingIntroduction {
        id: owner.subject.id.clone(),
        bucket: owner.subject.bucket.clone(),
        key: owner.subject.key.clone(),
        payload_generation: owner.subject.payload_generation,
        target: owner.subject.target.clone(),
    }
}

fn distinct_data_pg_owner(target: StagingTarget) -> StagingOwnerRecord {
    for index in 0..1_000 {
        let mut owner = fixture(target.clone());
        owner.subject.key = crate::tests::object_key(format!("capture-{index}"));
        owner.subject.object_pg =
            PgId::new(topology().object_pg_for(&owner.subject.bucket, &owner.subject.key));
        owner.subject.id.introduction = MetadataCommandId::new(
            owner.subject.originating_epoch,
            owner.subject.object_pg,
            owner.subject.id.introduction.log_index(),
        );
        let expected = match owner.subject.target {
            StagingTarget::UploadPart {
                upload_generation,
                part_number,
                ..
            } => {
                let expected = topology()
                    .object_generation_multipart_part_segment_data_pg(
                        &owner.subject.bucket,
                        &owner.subject.key,
                        upload_generation,
                        part_number,
                        0,
                    )
                    .get();
                let crossed = topology()
                    .object_generation_multipart_part_segment_data_pg(
                        &owner.subject.bucket,
                        &owner.subject.key,
                        owner.subject.payload_generation,
                        part_number,
                        0,
                    )
                    .get();
                if expected == crossed {
                    continue;
                }
                expected
            }
            _ => topology()
                .object_generation_segment_data_pg(
                    &owner.subject.bucket,
                    &owner.subject.key,
                    owner.subject.payload_generation,
                    0,
                )
                .get(),
        };
        if expected != owner.subject.object_pg.get() {
            // Only the logical introduction inputs are consumed, not the old
            // fixture window/resource rows after changing its subject.
            return owner;
        }
    }
    panic!("no distinct object and data PGs found");
}

fn capture<'a>(
    config: &'a StorageNodeProcessConfig,
    topology: &'a PgTopology,
    fence: AdmittedRouteEffectFence,
) -> StagingCapture<'a> {
    StagingCapture {
        topology,
        placement: PayloadPlacementInput::from_server_config(config),
        effect_fence: fence,
    }
}

pub(super) fn introduce(
    pg: &PgStore,
    config: &StorageNodeProcessConfig,
    owner: &StagingOwnerRecord,
    fence: AdmittedRouteEffectFence,
) -> Result<StagingOwnerRecord, StagingCaptureError> {
    capture(config, &topology(), fence).introduction(
        pg,
        config.node_id,
        input(owner),
        &proof(&owner.subject, owner.subject.creation_operation()),
    )
}

#[test]
fn staging_capture_introductions_derive_geometry_issuer_and_resource_roles() {
    with_time_and_monotonic_override(1_000, 70_000, || {
        for target in [
            StagingTarget::DirectPut,
            StagingTarget::StreamPut,
            multipart_target(),
        ] {
            let original = fixture(target.clone());
            let tmp = test_util::tempdir();
            let pg = PgStore::open(tmp.path(), original.subject.object_pg.get()).unwrap();
            let config = config(&original);
            let before = pg
                .metadata_command_checkpoint(4, ClusterEpoch::INITIAL)
                .unwrap();
            let owner = introduce(
                &pg,
                &config,
                &original,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            )
            .unwrap();
            owner.validate(&topology()).unwrap();
            assert_eq!(
                owner.subject.issuer,
                pg.stream_allocation_issuer(config.node_id).unwrap()
            );
            let mut expected_subject = original.subject.clone();
            expected_subject.issuer = pg.stream_allocation_issuer(config.node_id).unwrap();
            assert_eq!(owner.subject, expected_subject);
            assert_ne!(
                owner.subject.issuer.lifetime,
                original.subject.issuer.lifetime
            );
            let window = &owner.windows[0];
            assert_eq!(
                window.start,
                match target {
                    StagingTarget::DirectPut => original.subject.payload_generation,
                    _ => GenerationId::MIN,
                }
            );
            assert_eq!(window.end.get(), window.start.get() + 1);
            assert_eq!(owner.reserved_end, window.end);
            assert_eq!(window.issued_at_ms, 1_000);
            assert_eq!(
                window.cutoff_ms,
                match target {
                    StagingTarget::DirectPut => 11_000,
                    _ => 301_000,
                }
            );
            assert_eq!(owner.maximum_cutoff_ms, window.cutoff_ms);
            assert_eq!(window.geometry.logical_bound, 8 * 1024 * 1024);
            assert_eq!(
                window.geometry.codec,
                SegmentPayloadCodec::Cobblestone256SegmentV1
            );
            assert_eq!(
                window.geometry.stored_bound,
                window.geometry.codec.stored_size(8 * 1024 * 1024).unwrap()
            );
            assert_eq!(
                window.geometry.shard_bound,
                window.geometry.stored_bound.div_ceil(2)
            );
            assert!(owner.references.is_empty());
            assert!(matches!(
                owner.resources.generation,
                ResourceResponsibility::Live(_)
            ));
            assert_eq!(
                matches!(
                    owner.resources.creation,
                    ResourceResponsibility::ReleaseDue(_)
                ),
                matches!(target, StagingTarget::UploadPart { .. })
            );
            assert!(matches!(
                owner.resources.finalization,
                ResourceResponsibility::NoResource
            ));
            assert_eq!(
                pg.metadata_command_checkpoint(4, ClusterEpoch::INITIAL)
                    .unwrap(),
                before,
                "capture must not allocate or persist"
            );
        }
    });
}

#[test]
fn staging_capture_uses_exact_data_route_and_independent_ack_primary() {
    with_time_and_monotonic_override(1_000, 70_000, || {
        for target in [
            StagingTarget::DirectPut,
            StagingTarget::StreamPut,
            multipart_target(),
        ] {
            let owner = distinct_data_pg_owner(target);
            let tmp = test_util::tempdir();
            let pg = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
            let mut config = config(&owner);
            let expected_data_pg = match owner.subject.target {
                StagingTarget::UploadPart {
                    upload_generation,
                    part_number,
                    ..
                } => topology().object_generation_multipart_part_segment_data_pg(
                    &owner.subject.bucket,
                    &owner.subject.key,
                    upload_generation,
                    part_number,
                    0,
                ),
                _ => topology().object_generation_segment_data_pg(
                    &owner.subject.bucket,
                    &owner.subject.key,
                    owner.subject.payload_generation,
                    0,
                ),
            };
            assert_ne!(expected_data_pg.get(), owner.subject.object_pg.get());
            if let StagingTarget::UploadPart { part_number, .. } = owner.subject.target {
                assert_ne!(
                    expected_data_pg.get(),
                    topology()
                        .object_generation_multipart_part_segment_data_pg(
                            &owner.subject.bucket,
                            &owner.subject.key,
                            owner.subject.payload_generation,
                            part_number,
                            0,
                        )
                        .get(),
                    "the MPU-generation canary must not be masked by matching placement"
                );
            }
            let (_, expected_files) = PayloadPlacementInput::from_server_config(&config)
                .capture_segment_participants(
                    config.cluster_epoch,
                    expected_data_pg.get(),
                    config.default_ec_shape,
                    &owner.subject.segment_hash(0),
                    owner.subject.first_vid(),
                )
                .unwrap();
            let ack = (0..5)
                .map(NodeId::new)
                .find(|id| {
                    expected_files
                        .iter()
                        .all(|location| location.node_id() != *id)
                })
                .unwrap();
            config
                .pg_routes
                .iter_mut()
                .find(|route| route.pg_id == expected_data_pg.get())
                .unwrap()
                .primary_node_id = ack;
            let captured = introduce(
                &pg,
                &config,
                &owner,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            )
            .unwrap();
            let participants = &captured.windows[0].participants;
            assert_eq!(participants.data_pg.get(), expected_data_pg.get());
            assert_eq!(
                participants.files,
                expected_files
                    .iter()
                    .map(|location| location.node_id())
                    .collect::<Vec<_>>()
            );
            assert_eq!(participants.acknowledgement_primary, ack);
            assert!(!participants.files.contains(&ack));
        }
    });
}

#[test]
fn staging_capture_caps_portable_effective_deadline_not_raw_authority() {
    let owner = fixture(StagingTarget::DirectPut);
    let tmp = test_util::tempdir();
    let pg = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
    let config = config(&owner);
    // The same wall cutoff with unrelated local monotonic origins.
    for origin in [5_000, 900_000_000] {
        with_time_and_monotonic_override(1_000, origin, || {
            let fence =
                AdmittedRouteEffectFence::bounded(config.cluster_epoch, 6_000, origin + 2_500);
            let captured = introduce(&pg, &config, &owner, fence).unwrap();
            assert_eq!(captured.windows[0].cutoff_ms, 3_500);
        });
    }
    with_time_and_monotonic_override(3_500, 7_500, || {
        // Expired effective authority while the raw authority remains valid.
        let error = introduce(
            &pg,
            &config,
            &owner,
            AdmittedRouteEffectFence::bounded(config.cluster_epoch, 6_000, 7_500),
        )
        .unwrap_err();
        assert!(
            matches!(
                error,
                StagingCaptureError::Store(StoreError::RouteMapExpired { .. })
            ),
            "{error:?}"
        );
    });
}

#[test]
fn staging_capture_rejects_crossed_proofs_before_any_mutation() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let owner = fixture(target);
        let tmp = test_util::tempdir();
        let pg = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
        let config = config(&owner);
        let before = pg
            .metadata_command_checkpoint(4, ClusterEpoch::INITIAL)
            .unwrap();
        let topology = topology();
        let capture = capture(
            &config,
            &topology,
            AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
        );
        for case in 0..4 {
            let mut crossed = proof(&owner.subject, owner.subject.creation_operation());
            match case {
                0 => crossed.bucket = crate::tests::bucket_name("foreign-bucket"),
                1 => crossed.target_context = Some("foreign-key".into()),
                2 => crossed.operation_kind = "foreign-operation".into(),
                3 => crossed.cluster_epoch = ClusterEpoch::INITIAL,
                _ => unreachable!(),
            }
            assert!(matches!(
                capture.introduction(&pg, config.node_id, input(&owner), &crossed),
                Err(StagingCaptureError::Invariant(StagingInvariant::Resource))
            ));
        }
        assert_eq!(
            pg.metadata_command_checkpoint(4, ClusterEpoch::INITIAL)
                .unwrap(),
            before
        );
    }
}

#[test]
fn staging_capture_rejects_crossed_topology_primary_and_data_routes() {
    with_time_and_monotonic_override(1_000, 70_000, || {
        let owner = distinct_data_pg_owner(StagingTarget::StreamPut);
        let tmp = test_util::tempdir();
        let pg = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
        let config = config(&owner);
        let before = pg
            .metadata_command_checkpoint(4, ClusterEpoch::INITIAL)
            .unwrap();
        for case in 0..5 {
            let mut crossed = config.clone();
            match case {
                0 => crossed.pg_ids.push(8),
                1 => {
                    crossed
                        .pg_routes
                        .iter_mut()
                        .find(|route| route.pg_id == owner.subject.object_pg.get())
                        .unwrap()
                        .primary_node_id = NodeId::new(2)
                }
                2 => crossed.cluster_epoch = ClusterEpoch::INITIAL,
                3 => {
                    crossed
                        .pg_routes
                        .iter_mut()
                        .find(|route| route.pg_id == owner.subject.data_pg(&topology(), 0))
                        .unwrap()
                        .state = PgState::Peering
                }
                4 => {
                    crossed
                        .pg_routes
                        .iter_mut()
                        .find(|route| route.pg_id == owner.subject.data_pg(&topology(), 0))
                        .unwrap()
                        .acting_set = vec![NodeId::new(4)]
                }
                _ => unreachable!(),
            }
            let error = introduce(
                &pg,
                &crossed,
                &owner,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            )
            .unwrap_err();
            match case {
                0 => assert!(
                    matches!(
                        error,
                        StagingCaptureError::Store(StoreError::RouteCapabilitySubjectMismatch {
                            operation: "staging capture topology"
                        })
                    ),
                    "{error:?}"
                ),
                1 | 2 => assert!(
                    matches!(
                        error,
                        StagingCaptureError::Store(StoreError::RouteCapabilitySubjectMismatch {
                            operation: "staging capture primary"
                        })
                    ),
                    "{error:?}"
                ),
                3 => assert!(
                    matches!(
                        error,
                        StagingCaptureError::Store(StoreError::PgNotActive { .. })
                    ),
                    "{error:?}"
                ),
                4 => assert!(
                    matches!(
                        error,
                        StagingCaptureError::Store(StoreError::PayloadShardSetMismatch { .. })
                    ),
                    "{error:?}"
                ),
                _ => unreachable!(),
            }
        }
        introduce(
            &pg,
            &config,
            &owner,
            AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
        )
        .unwrap();
        assert_eq!(
            pg.metadata_command_checkpoint(4, ClusterEpoch::INITIAL)
                .unwrap(),
            before
        );
    });
}

#[test]
fn staging_capture_next_window_retains_issuer_prefix_and_old_cutoff() {
    with_time_and_monotonic_override(1_000, 70_000, || {
        for target in [StagingTarget::StreamPut, multipart_target()] {
            let original = fixture(target);
            let tmp = test_util::tempdir();
            let pg = PgStore::open(tmp.path(), original.subject.object_pg.get()).unwrap();
            let config = config(&original);
            let owner = introduce(
                &pg,
                &config,
                &original,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
            )
            .unwrap();
            let before = owner.clone();
            with_time_and_monotonic_override(2_000, 71_000, || {
                let topology = topology();
                let next = capture(
                    &config,
                    &topology,
                    AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
                )
                .next_window(&pg, config.node_id, &owner)
                .unwrap();
                assert_eq!(next.owner, owner.subject.id);
                assert_eq!(next.issuer, owner.subject.issuer);
                assert_eq!(next.index, 1);
                assert_eq!(next.start, owner.reserved_end);
                assert_eq!(next.end.get(), owner.reserved_end.get() + 1);
                assert_eq!(next.cutoff_ms, 302_000);
                assert_eq!(
                    next.segment_hash,
                    crate::stream_segment_key_hash(&owner.subject.id.attempt, 1)
                );
                let mut applied = owner.clone();
                applied.references.push(reference(&owner, 0));
                applied.reserved_end = next.end;
                applied.maximum_cutoff_ms = next.cutoff_ms;
                applied.windows.push(next);
                applied.validate(&topology).unwrap();
                assert_eq!(applied.windows[0], before.windows[0]);
            });
            assert_eq!(
                owner, before,
                "construction must not advance allocation or alter prior evidence"
            );
        }
    });
}

#[test]
fn staging_capture_next_window_rejects_new_lifetime_and_terminal_owner() {
    with_time_and_monotonic_override(1_000, 70_000, || {
        let original = fixture(StagingTarget::StreamPut);
        let tmp = test_util::tempdir();
        let pg = PgStore::open(tmp.path(), original.subject.object_pg.get()).unwrap();
        let config = config(&original);
        let owner = introduce(
            &pg,
            &config,
            &original,
            AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
        )
        .unwrap();
        let topology = topology();
        let capture = capture(
            &config,
            &topology,
            AdmittedRouteEffectFence::unbounded(config.cluster_epoch),
        );
        let mut terminal = owner.clone();
        publish(&mut terminal);
        assert!(matches!(
            capture.next_window(&pg, config.node_id, &terminal),
            Err(StagingCaptureError::Invariant(StagingInvariant::Lifecycle))
        ));
        drop(pg);
        let reopened = PgStore::open(tmp.path(), original.subject.object_pg.get()).unwrap();
        assert!(matches!(
            capture.next_window(&reopened, config.node_id, &owner),
            Err(StagingCaptureError::Invariant(
                StagingInvariant::WindowOwner
            ))
        ));
        drop(reopened.begin_allocation_lifetime_replacement().unwrap());
        assert!(matches!(
            capture.next_window(&reopened, config.node_id, &owner),
            Err(StagingCaptureError::Store(
                StoreError::PgAllocationLifetimeUnavailable { .. }
            ))
        ));
    });
}

#[test]
fn staging_capture_rejects_nonrepresentable_window_and_cutoff() {
    let mut owner = fixture(StagingTarget::DirectPut);
    owner.subject.payload_generation = GenerationId::new(i64::MAX as u64).unwrap();
    let tmp = test_util::tempdir();
    let pg = PgStore::open(tmp.path(), owner.subject.object_pg.get()).unwrap();
    let config = config(&owner);
    with_time_and_monotonic_override(1_000, 70_000, || {
        assert!(matches!(
            introduce(
                &pg,
                &config,
                &owner,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch)
            ),
            Err(StagingCaptureError::Invariant(
                StagingInvariant::IntegerDomain
            ))
        ));
    });
    owner.subject.payload_generation = GenerationId::new(12).unwrap();
    with_time_and_monotonic_override(u64::MAX - 1, 70_000, || {
        assert!(matches!(
            introduce(
                &pg,
                &config,
                &owner,
                AdmittedRouteEffectFence::unbounded(config.cluster_epoch)
            ),
            Err(StagingCaptureError::Invariant(StagingInvariant::Cutoff))
        ));
    });
}
