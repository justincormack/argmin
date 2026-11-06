// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Command-bound pending dependencies, not installed command-field evidence.
//! No payload is written: these fixtures exercise metadata correspondence and
//! conservative future-window protection before materialization/publication.

use super::super::descriptor::{
    apply_append_for_test, apply_introduction_for_test, AppendDescriptor, BoundAppendDescriptor,
    BoundIntroductionDescriptor, IntroductionDescriptor,
};
use super::*;
use crate::metadata_command::{MetadataCommandEnvelope, MetadataCommandPayload};
use crate::pg_store::{
    PgClusterMapHistoryRouteReference, PgClusterMapHistoryRouteReferenceKind, PgStore,
};
use crate::types::{
    ShardScavengerPayloadReference, ShardScavengerProtectedShardSetReference,
    ShardScavengerStagingProtection,
};
use crate::{StreamUploadState, StreamUploadTarget};

fn route(epoch: ClusterEpoch, pg: PgId) -> PgClusterMapHistoryRouteReference {
    PgClusterMapHistoryRouteReference::new(
        PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource,
        epoch,
        pg,
    )
}

fn reissued(command: &MetadataCommandEnvelope) -> MetadataCommandEnvelope {
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(command.id().cluster_epoch().get() + 1).unwrap(),
            command.id().pg_id(),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        command.payload().clone(),
    )
}

fn expected_protection(window: &StagingWindow) -> ShardScavengerPayloadReference {
    // Deliberately non-sorted file order and a spare ack owner: do not derive
    // either from an acting set, from the command epoch or from selected rows.
    assert_eq!(window.participants.files, [2, 0, 3].map(NodeId::new));
    assert_eq!(window.participants.acknowledgement_primary, NodeId::new(1));
    ShardScavengerPayloadReference::StagingProtection(ShardScavengerStagingProtection {
        identity: ShardScavengerProtectedShardSetReference {
            data_pg_id: window.participants.data_pg.get(),
            okh: window.segment_hash,
            generation_id: window.start,
            ec: EcShape { k: 2, m: 1 },
        },
        placement_epoch: window.participants.placement_epoch,
        file_nodes: [2, 0, 3].map(NodeId::new).to_vec(),
        acknowledgement_primary: NodeId::new(1),
    })
}

#[test]
fn staging_bound_introduction_protects_unused_scope_and_exact_historical_routes() {
    let topology = topology();
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let owner = fixture(target);
        let command = introduction::command(&owner);
        let descriptor =
            IntroductionDescriptor::capture(&owner.validate(&topology).unwrap()).unwrap();
        let descriptor = IntroductionDescriptor::decode(&descriptor.encode().unwrap()).unwrap();
        let original_bytes = descriptor.encode().unwrap();
        // No reservation, session, manifest or materialized owner is needed to
        // discover the exact future scope carried by a pending introduction.
        for command in [&command, &reissued(&command)] {
            crate::clock::with_time_and_monotonic_override(9_000_000, 1, || {
                let bound = descriptor.bind(&topology, command).unwrap();
                let pending = bound.pending_protection();
                assert_eq!(pending.reference(), expected_protection(&owner.windows[0]));
                assert_eq!(
                    pending.historical_routes(),
                    BTreeSet::from([
                        route(command.id().cluster_epoch(), owner.subject.object_pg),
                        route(owner.subject.originating_epoch, owner.subject.object_pg),
                        route(
                            owner.windows[0].participants.placement_epoch,
                            owner.windows[0].participants.data_pg
                        ),
                        route(
                            owner.subject.originating_epoch,
                            PgId::new(topology.bucket_pg_for(&owner.subject.bucket))
                        ),
                    ])
                );
                assert_eq!(descriptor.encode().unwrap(), original_bytes);
            });
        }
    }
}

#[test]
fn staging_bound_append_protects_lookahead_without_turning_it_into_readable_payload() {
    let topology = topology();
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let owner = fixture(target);
        let mut next = window(&owner.subject, 1, owner.reserved_end);
        next.participants.placement_epoch = ClusterEpoch::new(6).unwrap();
        let descriptor =
            AppendDescriptor::capture(&owner.validate(&topology).unwrap(), next.clone()).unwrap();
        let descriptor = AppendDescriptor::decode(&descriptor.encode().unwrap()).unwrap();
        let command = reissued(&reissued(&append::command(&owner)));
        assert_eq!(command.id().cluster_epoch().get(), 7);
        assert_ne!(next.segment_hash, owner.windows[0].segment_hash);
        crate::clock::with_time_and_monotonic_override(9_000_000, 1, || {
            let bound = descriptor.bind(&topology, &command).unwrap();
            let pending = bound.pending_protection();
            assert_eq!(pending.reference(), expected_protection(&next));
            assert_eq!(
                pending.historical_routes(),
                BTreeSet::from([
                    route(command.id().cluster_epoch(), owner.subject.object_pg),
                    route(owner.subject.originating_epoch, owner.subject.object_pg),
                    route(next.participants.placement_epoch, next.participants.data_pg),
                ])
            );
            // Consumed/current payload references are separately derived from
            // the append command. Do not substitute that readable segment here.
            assert_ne!(pending.reference(), expected_protection(&owner.windows[0]));
        });
    }
}

#[test]
fn staging_bound_introduction_rejects_crossed_command_fields_before_projection() {
    let topology = topology();
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let owner = fixture(target);
        let original = introduction::command(&owner);
        let descriptor =
            IntroductionDescriptor::capture(&owner.validate(&topology).unwrap()).unwrap();
        for field in [
            "bucket",
            "key",
            "attempt",
            "generation",
            "target",
            "upload",
            "part",
            "issuer",
            "proof",
            "state",
            "incarnation",
            "cursor",
        ] {
            let mut payload = original.payload().clone();
            match &mut payload {
                MetadataCommandPayload::ReserveObjectGeneration(reserve) => match field {
                    "bucket" => reserve.bucket = crate::tests::bucket_name("crossed-bucket"),
                    "key" => reserve.key = ObjectKey::try_from("crossed-key").unwrap(),
                    "attempt" => {
                        reserve.reservation_id = crate::tests::stream_session_id("crossed-attempt")
                    }
                    "generation" => reserve.generation_id = GenerationId::new(13).unwrap(),
                    _ => continue, // These fields exist only on stream creation.
                },
                MetadataCommandPayload::CreateStreamUpload(create) => match field {
                    "bucket" => create.session.bucket = crate::tests::bucket_name("crossed-bucket"),
                    "key" => create.session.key = ObjectKey::try_from("crossed-key").unwrap(),
                    "attempt" => {
                        create.session.session_id =
                            crate::tests::stream_session_id("crossed-attempt")
                    }
                    "generation" => create.payload_generation = GenerationId::new(13).unwrap(),
                    "target" => {
                        create.session.target = match create.session.target {
                            StreamUploadTarget::PutObject => StreamUploadTarget::UploadPart {
                                upload_id: crate::tests::multipart_upload_id("crossed"),
                                part_number: 8,
                            },
                            StreamUploadTarget::UploadPart { .. } => StreamUploadTarget::PutObject,
                        }
                    }
                    "upload" => {
                        let StreamUploadTarget::UploadPart { upload_id, .. } =
                            &mut create.session.target
                        else {
                            continue;
                        };
                        *upload_id = crate::tests::multipart_upload_id("crossed");
                    }
                    "part" => {
                        let StreamUploadTarget::UploadPart { part_number, .. } =
                            &mut create.session.target
                        else {
                            continue;
                        };
                        *part_number += 1;
                    }
                    "issuer" => create.allocation_issuer.lifetime[0] ^= 1,
                    "proof" => create.bucket_write_reservation.owner_token.push('x'),
                    "state" => create.session.state = StreamUploadState::Completing,
                    "incarnation" => create.session.bucket_incarnation_generation += 1,
                    "cursor" => create.initial_next_segment_vid = GenerationId::new(2).unwrap(),
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            }
            let crossed = MetadataCommandEnvelope::new(original.id(), payload);
            assert!(crossed.verify_checksum());
            assert!(
                descriptor.bind(&topology, &crossed).is_err(),
                "{field}: {:?}",
                owner.subject.target
            );
        }
        let wrong_pg = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                original.id().cluster_epoch(),
                PgId::new(99),
                original.id().log_index(),
            ),
            original.payload().clone(),
        );
        assert!(descriptor.bind(&topology, &wrong_pg).is_err());
        let earlier = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                original.id().cluster_epoch(),
                original.id().pg_id(),
                MetadataCommandLogIndex::new(10).unwrap(),
            ),
            original.payload().clone(),
        );
        assert!(descriptor.bind(&topology, &earlier).is_err());
    }
}

#[test]
fn staging_bound_append_rejects_crossed_subject_and_nonadjacent_command() {
    let topology = topology();
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let owner = fixture(target);
        let descriptor = AppendDescriptor::capture(
            &owner.validate(&topology).unwrap(),
            window(&owner.subject, 1, owner.reserved_end),
        )
        .unwrap();
        let original = append::command(&owner);
        for field in [
            "bucket",
            "key",
            "attempt",
            "index",
            "vid",
            "hash",
            "data_pg",
            "old_epoch",
            "future_epoch",
            "target",
            "upload",
            "part",
        ] {
            let mut payload = original.payload().clone();
            let MetadataCommandPayload::AppendStreamSegment(append) = &mut payload else {
                unreachable!()
            };
            match field {
                "bucket" => append.bucket = crate::tests::bucket_name("crossed-bucket"),
                "key" => append.key = ObjectKey::try_from("crossed-key").unwrap(),
                "attempt" => {
                    append.segment.session_id = crate::tests::stream_session_id("crossed-attempt")
                }
                "index" => append.segment.segment_index += 1,
                "vid" => append.segment.segment_vid = GenerationId::new(3).unwrap(),
                "hash" => append.segment.segment_okh[0] ^= 1,
                "data_pg" => append.segment.data_pg_id = (append.segment.data_pg_id + 1) % 8,
                "old_epoch" => {
                    append.segment.placement_cluster_epoch = ClusterEpoch::new(4).unwrap()
                }
                "future_epoch" => {
                    append.segment.placement_cluster_epoch = ClusterEpoch::new(6).unwrap()
                }
                "target" => {
                    append.target = match append.target {
                        StreamUploadTarget::PutObject => StreamUploadTarget::UploadPart {
                            upload_id: crate::tests::multipart_upload_id("crossed"),
                            part_number: 8,
                        },
                        StreamUploadTarget::UploadPart { .. } => StreamUploadTarget::PutObject,
                    }
                }
                "upload" => {
                    let StreamUploadTarget::UploadPart { upload_id, .. } = &mut append.target
                    else {
                        continue;
                    };
                    *upload_id = crate::tests::multipart_upload_id("crossed");
                }
                "part" => {
                    let StreamUploadTarget::UploadPart { part_number, .. } = &mut append.target
                    else {
                        continue;
                    };
                    *part_number += 1;
                }
                _ => unreachable!(),
            }
            let crossed = MetadataCommandEnvelope::new(original.id(), payload);
            assert!(crossed.verify_checksum());
            assert!(
                descriptor.bind(&topology, &crossed).is_err(),
                "{field}: {:?}",
                owner.subject.target
            );
        }
        assert!(descriptor
            .bind(&topology, &introduction::command(&owner))
            .is_err());
        let wrong_pg = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                original.id().cluster_epoch(),
                PgId::new(99),
                original.id().log_index(),
            ),
            original.payload().clone(),
        );
        assert!(descriptor.bind(&topology, &wrong_pg).is_err());
        let not_after_introduction =
            MetadataCommandEnvelope::new(owner.subject.id.introduction, original.payload().clone());
        assert!(descriptor.bind(&topology, &not_after_introduction).is_err());
    }
}

#[test]
fn staging_bound_descriptors_reject_same_object_pg_foreign_topology() {
    let actual = topology();
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let owner = fixture(target);
        let subject = &owner.subject;
        let foreign = (1_u32..256)
            .filter_map(|mask| {
                let pgs = (0..8)
                    .filter(|pg| mask & (1 << pg) != 0)
                    .collect::<Vec<_>>();
                let topology = PgTopology::new(&pgs).unwrap();
                (topology.object_pg_for(&subject.bucket, &subject.key) == subject.object_pg.get()
                    && topology.bucket_pg_for(&subject.bucket)
                        != actual.bucket_pg_for(&subject.bucket)
                    && subject.data_pg(&topology, 0) != subject.data_pg(&actual, 0)
                    && subject.data_pg(&topology, 1) != subject.data_pg(&actual, 1))
                .then_some(topology)
            })
            .next()
            .expect("same object PG with crossed release and window PGs");
        assert_eq!(
            foreign.object_pg_for(&subject.bucket, &subject.key),
            subject.object_pg.get()
        );
        let introduction =
            IntroductionDescriptor::capture(&owner.validate(&actual).unwrap()).unwrap();
        assert!(introduction
            .bind(&foreign, &introduction::command(&owner))
            .is_err());
        if !matches!(subject.target, StagingTarget::DirectPut) {
            let append = AppendDescriptor::capture(
                &owner.validate(&actual).unwrap(),
                window(subject, 1, owner.reserved_end),
            )
            .unwrap();
            assert!(append.bind(&foreign, &append::command(&owner)).is_err());
        }
    }
}

#[test]
fn staging_bound_descriptors_reject_future_placement_before_projection() {
    let topology = topology();
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let mut owner = fixture(target);
        owner.windows[0].participants.placement_epoch = ClusterEpoch::new(6).unwrap();
        let command = introduction::command(&owner);
        assert_eq!(command.id().cluster_epoch().get(), 5);
        let descriptor =
            IntroductionDescriptor::capture(&owner.validate(&topology).unwrap()).unwrap();
        // The descriptor is independently valid, but cannot be carried by a
        // command predating its placement. This is not a codec-rejection test.
        let descriptor = IntroductionDescriptor::decode(&descriptor.encode().unwrap()).unwrap();
        assert!(matches!(
            descriptor.bind(&topology, &command),
            Err(super::super::introduction::IntroductionError::CommandBinding)
        ));
        assert!(descriptor.bind(&topology, &reissued(&command)).is_ok());

        if !matches!(owner.subject.target, StagingTarget::DirectPut) {
            owner.windows[0].participants.placement_epoch = ClusterEpoch::new(5).unwrap();
            let mut next = window(&owner.subject, 1, owner.reserved_end);
            next.participants.placement_epoch = ClusterEpoch::new(6).unwrap();
            let descriptor =
                AppendDescriptor::capture(&owner.validate(&topology).unwrap(), next).unwrap();
            let descriptor = AppendDescriptor::decode(&descriptor.encode().unwrap()).unwrap();
            let command = append::command(&owner);
            assert!(matches!(
                descriptor.bind(&topology, &command),
                Err(super::super::append::AppendError::CommandBinding)
            ));
            assert!(descriptor.bind(&topology, &reissued(&command)).is_ok());
        }
    }
}

#[test]
fn staging_bound_append_rechecks_live_responsibility_after_binding_before_mutation() {
    let topology = topology();
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let owner = append::initialize(&pg, target);
        let descriptor = AppendDescriptor::capture(
            &owner.validate(&topology).unwrap(),
            append::next(&pg, &owner),
        )
        .unwrap();
        let command = append::command(&owner);
        let bound = descriptor.bind(&topology, &command).unwrap();
        // Binding is not a cached live-state certificate. A later authoritative
        // reinspection must see the changed allocator even though the immutable
        // descriptor and command still have matching subjects.
        pg.conn
            .execute(
                "UPDATE stream_uploads SET next_segment_vid = 3 WHERE session_id = ?1",
                [&owner.subject.id.attempt],
            )
            .unwrap();
        let changes = pg.conn.total_changes();
        assert!(matches!(
            super::super::append::apply_in_open_transaction(&pg, bound),
            Err(super::super::append::AppendError::LiveResponsibility)
        ));
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(append::load(&pg, &owner), owner);
    }
}

#[test]
fn staging_application_entry_points_require_bound_descriptor_inputs() {
    // These signatures are themselves compiler-checked regression evidence:
    // reverting either deepest entry point to independently supplied inputs
    // breaks the test build, even if a bound convenience wrapper remains.
    let introduce: fn(
        &PgStore,
        BoundIntroductionDescriptor<'_>,
    ) -> Result<
        super::super::introduction::IntroductionApplied,
        super::super::introduction::IntroductionError,
    > = super::super::introduction::apply_in_open_transaction;
    let append: fn(
        &PgStore,
        BoundAppendDescriptor<'_>,
    ) -> Result<
        super::super::append::AppendApplied,
        super::super::append::AppendError,
    > = super::super::append::apply_in_open_transaction;
    let topology = topology();
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let (owner, command) = introduction::setup(&pg, target);
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let descriptor =
            IntroductionDescriptor::capture(&owner.validate(&topology).unwrap()).unwrap();
        assert_eq!(
            introduce(&pg, descriptor.bind(&topology, &command).unwrap()).unwrap(),
            super::super::introduction::IntroductionApplied::Created,
        );
        assert_eq!(append::load(&pg, &owner), owner);
        if owner.subject.target != StagingTarget::DirectPut {
            let next = append::next(&pg, &owner);
            let descriptor =
                AppendDescriptor::capture(&owner.validate(&topology).unwrap(), next.clone())
                    .unwrap();
            let command = append::command(&owner);
            assert_eq!(
                append(&pg, descriptor.bind(&topology, &command).unwrap()).unwrap(),
                super::super::append::AppendApplied::Appended,
            );
            assert_eq!(append::load(&pg, &owner).windows.last(), Some(&next));
        }
    }
}

#[test]
fn staging_test_description_adapters_do_not_bypass_binding() {
    let topology = topology();
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let (owner, command) = introduction::setup(&pg, target);
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let mut future = owner.clone();
        future.windows[0].participants.placement_epoch =
            ClusterEpoch::new(command.id().cluster_epoch().get() + 1).unwrap();
        assert!(future.validate(&topology).is_ok());
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(matches!(
            apply_introduction_for_test(&pg, &topology, &command, &future),
            Err(super::super::introduction::IntroductionError::CommandBinding),
        ));
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
        apply_introduction_for_test(&pg, &topology, &command, &owner).unwrap();
        if owner.subject.target != StagingTarget::DirectPut {
            let command = append::command(&owner);
            let mut next = append::next(&pg, &owner);
            next.participants.placement_epoch =
                ClusterEpoch::new(command.id().cluster_epoch().get() + 1).unwrap();
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(matches!(
                apply_append_for_test(&pg, &topology, &command, &next),
                Err(super::super::append::AppendError::CommandBinding),
            ));
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
        }
    }
}
