// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::metadata_command::MetadataCommandLogIndex;

#[path = "payload_staging_sql_tests.rs"]
mod persistence;

#[path = "payload_staging_capture_tests.rs"]
mod capture;

#[path = "payload_staging_descriptor_tests.rs"]
mod descriptor;

#[path = "payload_staging_descriptor_binding_tests.rs"]
mod descriptor_binding;

#[path = "payload_staging_introduction_tests.rs"]
mod introduction;

#[path = "payload_staging_append_tests.rs"]
mod append;

#[path = "payload_staging_preparation_tests.rs"]
mod preparation;

#[path = "payload_staging_close_tests.rs"]
mod close;

#[path = "payload_staging_part_tests.rs"]
mod part;

#[path = "payload_staging_object_tests.rs"]
mod object;

#[path = "payload_staging_object_overwrite_tests.rs"]
mod object_overwrite;

#[path = "payload_staging_completion_tests.rs"]
mod completion;

#[path = "payload_staging_completion_overwrite_tests.rs"]
mod completion_overwrite;

#[path = "payload_staging_displacement_tests.rs"]
mod displacement;

#[path = "payload_staging_delete_tests.rs"]
mod delete;

#[path = "payload_staging_marker_tests.rs"]
mod marker;

#[path = "payload_staging_reclaim_tests.rs"]
mod reclaim;

#[path = "payload_staging_abandonment_tests.rs"]
mod abandonment;

#[path = "payload_staging_epoch_order_tests.rs"]
mod epoch_order;

#[path = "payload_staging_abort_tests.rs"]
mod abort;

fn topology() -> PgTopology {
    PgTopology::new(&[0, 1, 2, 3, 4, 5, 6, 7]).unwrap()
}

fn multipart_target() -> StagingTarget {
    StagingTarget::UploadPart {
        upload_id: crate::tests::multipart_upload_id("staging-record"),
        upload_generation: GenerationId::new(7).unwrap(),
        part_number: 3,
    }
}

fn proof(owner: &StagingSubject, operation: &str) -> BucketWriteReservationProof {
    BucketWriteReservationProof {
        bucket: owner.bucket.clone(),
        reservation_id: format!("{operation}-reservation"),
        owner_token: format!("{operation}-owner"),
        cluster_epoch: owner.originating_epoch,
        bucket_execution_generation: 21,
        bucket_incarnation_generation: owner.bucket_incarnation,
        operation_kind: operation.into(),
        created_at: 1_000,
        lease_deadline: 10_000,
        target_context: Some(owner.key.as_str().into()),
    }
}

fn window(owner: &StagingSubject, index: u32, start: GenerationId) -> StagingWindow {
    StagingWindow {
        owner: owner.id.clone(),
        issuer: owner.issuer.clone(),
        index,
        segment_hash: owner.segment_hash(index),
        start,
        end: GenerationId::new(start.get() + 1).unwrap(),
        issued_at_ms: 1_000,
        cutoff_ms: 1_000 + owner.cap_ms(),
        geometry: StagingGeometry::new(
            SegmentPayloadCodec::Cobblestone256SegmentV1,
            EcShape { k: 2, m: 1 },
            INTERNAL_SEGMENT_SIZE as u64,
        )
        .unwrap(),
        participants: StagingParticipants {
            data_pg: PgId::new(owner.data_pg(&topology(), index)),
            placement_epoch: owner.originating_epoch,
            files: [2, 0, 3].map(NodeId::new).to_vec(),
            // A spare acting-set node need not hold any shard file.
            acknowledgement_primary: NodeId::new(1),
        },
    }
}

fn fixture(target: StagingTarget) -> StagingOwnerRecord {
    let bucket = BucketName::try_from("staging-bucket").unwrap();
    let key = ObjectKey::try_from("payload").unwrap();
    let object_pg = PgId::new(topology().object_pg_for(&bucket, &key));
    let subject = StagingSubject {
        id: StagingOwnerId {
            attempt: crate::tests::stream_session_id("staging-owner"),
            introduction: MetadataCommandId::new(
                ClusterEpoch::new(5).unwrap(),
                object_pg,
                MetadataCommandLogIndex::new(11).unwrap(),
            ),
        },
        bucket,
        bucket_incarnation: 17,
        key,
        object_pg,
        originating_epoch: ClusterEpoch::new(5).unwrap(),
        payload_generation: GenerationId::new(12).unwrap(),
        target,
        issuer: StreamAllocationIssuer {
            node_id: NodeId::new(4),
            lifetime: [0x71; 16],
        },
    };
    let first = window(&subject, 0, subject.first_vid());
    let creation = StagingBucketResource::capture(
        &proof(&subject, subject.creation_operation()),
        PgId::new(topology().bucket_pg_for(&subject.bucket)),
    );
    let resources = StagingResources {
        generation: ResourceResponsibility::Live(StagingGenerationResource {
            reservation_id: subject.id.attempt.clone(),
            generation: subject.payload_generation,
        }),
        creation: if matches!(subject.target, StagingTarget::UploadPart { .. }) {
            ResourceResponsibility::ReleaseDue(creation)
        } else {
            ResourceResponsibility::Live(creation)
        },
        finalization: ResourceResponsibility::NoResource,
        abandoned_finalizations: vec![],
    };
    StagingOwnerRecord {
        subject,
        lifecycle: StagingLifecycle::Active,
        reserved_end: first.end,
        maximum_cutoff_ms: first.cutoff_ms,
        windows: vec![first],
        references: vec![],
        resources,
    }
}

fn reference(owner: &StagingOwnerRecord, index: usize) -> StagingReference {
    let window = &owner.windows[index];
    StagingReference {
        window_index: window.index,
        segment_hash: window.segment_hash,
        physical_vid: window.start,
        logical_size: 4_096,
        stored_size: window.geometry.codec.stored_size(4_096).unwrap(),
        destination: StagingDestination::Stream,
    }
}

fn owner_with_abandoned_obligations() -> StagingOwnerRecord {
    let mut owner = fixture(multipart_target());
    append(&mut owner);
    publish(&mut owner);
    for (suffix, epoch) in [("-z", 7), ("-a", 8)] {
        let mut proof = proof(
            &owner.subject,
            UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
        );
        proof.reservation_id.push_str(suffix);
        proof.owner_token.push_str(suffix);
        proof.cluster_epoch = ClusterEpoch::new(epoch).unwrap();
        let resource = StagingBucketResource::capture(
            &proof,
            PgId::new(topology().bucket_pg_for(&owner.subject.bucket)),
        );
        owner = owner.with_abandoned_part_finalization(&resource).unwrap();
    }
    owner.validate(&topology()).unwrap();
    owner
}

#[test]
fn staging_obligations_validate_lifecycle_subject_and_independent_stable_identities() {
    let owner = owner_with_abandoned_obligations();
    for lifecycle in [
        StagingLifecycle::Published,
        StagingLifecycle::Retiring(StagingRetirement::PublishedRetained),
        StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced),
        StagingLifecycle::Retiring(StagingRetirement::Unpublished),
    ] {
        let mut terminal = owner.clone();
        terminal.lifecycle = lifecycle;
        if matches!(
            lifecycle,
            StagingLifecycle::Retiring(
                StagingRetirement::PublishedUnreferenced | StagingRetirement::Unpublished
            )
        ) {
            terminal.references.clear();
        }
        terminal.validate(&topology()).unwrap();
        invalid(
            &terminal,
            |x| {
                x.resources.abandoned_finalizations[1].identity =
                    x.resources.abandoned_finalizations[0].identity.clone()
            },
            StagingInvariant::Resource,
        );
        invalid(
            &terminal,
            |x| {
                let ResourceResponsibility::ReleaseDue(proof) = &x.resources.finalization else {
                    unreachable!()
                };
                x.resources.abandoned_finalizations[0].identity = proof.identity.clone();
                x.resources.abandoned_finalizations[0].observed_lease_deadline += 1_000;
            },
            StagingInvariant::Resource,
        );
        invalid(
            &terminal,
            |x| x.resources.finalization = ResourceResponsibility::NoResource,
            StagingInvariant::Resource,
        );
        invalid(
            &terminal,
            |x| {
                x.resources.abandoned_finalizations[0]
                    .identity
                    .target_context = Some("crossed-key".into())
            },
            StagingInvariant::Resource,
        );
        invalid(
            &terminal,
            |x| {
                x.resources.abandoned_finalizations[0]
                    .identity
                    .operation_kind = "crossed-operation".into()
            },
            StagingInvariant::Resource,
        );
        invalid(
            &terminal,
            |x| x.resources.abandoned_finalizations[0].release_pg = PgId::new(99),
            StagingInvariant::Resource,
        );
    }
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let mut active = fixture(target);
        active.resources.abandoned_finalizations = owner.resources.abandoned_finalizations.clone();
        assert_eq!(
            active.validate(&topology()).unwrap_err(),
            StagingInvariant::Resource
        );
    }
}

fn append(owner: &mut StagingOwnerRecord) {
    let index = owner.windows.len() - 1;
    owner.references.push(reference(owner, index));
    let mut next = window(&owner.subject, index as u32 + 1, owner.reserved_end);
    next.issued_at_ms += 1_000;
    next.cutoff_ms += 1_000;
    owner.reserved_end = next.end;
    owner.maximum_cutoff_ms = owner.maximum_cutoff_ms.max(next.cutoff_ms);
    owner.windows.push(next);
}

fn release_due<T: Clone>(resource: &mut ResourceResponsibility<T>) {
    if let ResourceResponsibility::Live(value) = resource {
        *resource = ResourceResponsibility::ReleaseDue(value.clone());
    }
}

fn finalization_resource(owner: &StagingOwnerRecord) -> StagingBucketResource {
    StagingBucketResource::capture(
        &proof(
            &owner.subject,
            UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
        ),
        PgId::new(topology().bucket_pg_for(&owner.subject.bucket)),
    )
}

fn publish(owner: &mut StagingOwnerRecord) {
    if owner.subject.target == StagingTarget::DirectPut {
        owner.references.push(reference(owner, 0));
    }
    owner.lifecycle = StagingLifecycle::Published;
    release_due(&mut owner.resources.generation);
    release_due(&mut owner.resources.creation);
    if matches!(owner.subject.target, StagingTarget::UploadPart { .. }) {
        owner.resources.finalization =
            ResourceResponsibility::ReleaseDue(finalization_resource(owner));
    }
    for reference in &mut owner.references {
        reference.destination = match &owner.subject.target {
            StagingTarget::DirectPut | StagingTarget::StreamPut => StagingDestination::Object {
                generation: owner.subject.payload_generation,
                part_number: None,
            },
            StagingTarget::UploadPart {
                upload_id,
                part_number,
                ..
            } => StagingDestination::Part {
                upload_id: upload_id.clone(),
                part_number: *part_number,
                payload_generation: owner.subject.payload_generation,
            },
        };
    }
}

fn invalid(
    original: &StagingOwnerRecord,
    mutate: impl FnOnce(&mut StagingOwnerRecord),
    expected: StagingInvariant,
) {
    original.validate(&topology()).unwrap();
    let mut crossed = original.clone();
    mutate(&mut crossed);
    let before = crossed.clone();
    assert_eq!(crossed.validate(&topology()).unwrap_err(), expected);
    assert_eq!(crossed, before, "validation must be read-only");
}

#[test]
fn staging_records_bind_distinct_direct_stream_and_part_identities() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let owner = fixture(target);
        let validated_topology = topology();
        let checked = owner.validate(&validated_topology).unwrap();
        assert!(std::ptr::eq(
            checked.record_for_topology(&validated_topology).unwrap(),
            &owner
        ));
        assert_eq!(owner.windows[0].start, owner.subject.first_vid());
        assert_eq!(owner.windows[0].issuer, owner.subject.issuer);
        assert_ne!(
            owner.windows[0].participants.acknowledgement_primary,
            owner.windows[0].participants.files[0]
        );
    }
    let part = fixture(multipart_target());
    assert_ne!(
        part.subject.payload_generation,
        part.subject.published_generation()
    );
    assert_eq!(part.windows[0].start, GenerationId::MIN);
    assert_eq!(fixture(StagingTarget::DirectPut).windows[0].start.get(), 12);
}

#[test]
fn staging_records_reject_crossed_subject_revision_issuer_and_hash() {
    let owner = fixture(StagingTarget::StreamPut);
    invalid(
        &owner,
        |x| x.subject.object_pg = PgId::new(99),
        StagingInvariant::Subject,
    );
    invalid(
        &owner,
        |x| {
            x.subject.id.introduction = MetadataCommandId::new(
                ClusterEpoch::new(6).unwrap(),
                x.subject.object_pg,
                MetadataCommandLogIndex::new(11).unwrap(),
            )
        },
        StagingInvariant::Subject,
    );
    invalid(
        &owner,
        |x| x.subject.bucket_incarnation = 0,
        StagingInvariant::IntegerDomain,
    );
    invalid(
        &owner,
        |x| x.subject.payload_generation = GenerationId::new(u64::MAX).unwrap(),
        StagingInvariant::IntegerDomain,
    );
    invalid(
        &owner,
        |x| x.windows[0].owner.attempt = crate::tests::stream_session_id("other"),
        StagingInvariant::WindowOwner,
    );
    invalid(
        &owner,
        |x| {
            x.windows[0].owner.introduction = MetadataCommandId::new(
                x.subject.originating_epoch,
                x.subject.object_pg,
                MetadataCommandLogIndex::new(12).unwrap(),
            )
        },
        StagingInvariant::WindowOwner,
    );
    invalid(
        &owner,
        |x| x.windows[0].issuer.node_id = NodeId::new(9),
        StagingInvariant::WindowOwner,
    );
    invalid(
        &owner,
        |x| x.windows[0].issuer.lifetime[0] ^= 1,
        StagingInvariant::WindowOwner,
    );
    invalid(
        &owner,
        |x| x.windows[0].segment_hash[0] ^= 1,
        StagingInvariant::WindowOwner,
    );
}

#[test]
fn staging_records_bound_every_interval_cutoff_and_geometry_without_clocks() {
    let owner = fixture(StagingTarget::DirectPut);
    invalid(
        &owner,
        |x| x.windows[0].end = x.windows[0].start,
        StagingInvariant::Interval,
    );
    invalid(
        &owner,
        |x| x.windows[0].end = GenerationId::new(14).unwrap(),
        StagingInvariant::Interval,
    );
    invalid(
        &owner,
        |x| x.windows[0].end = GenerationId::new(u64::MAX).unwrap(),
        StagingInvariant::IntegerDomain,
    );
    invalid(
        &owner,
        |x| x.windows[0].cutoff_ms = x.windows[0].issued_at_ms,
        StagingInvariant::Cutoff,
    );
    invalid(
        &owner,
        |x| x.windows[0].cutoff_ms += 1,
        StagingInvariant::Cutoff,
    );
    invalid(
        &owner,
        |x| x.windows[0].cutoff_ms = u64::MAX,
        StagingInvariant::IntegerDomain,
    );
    invalid(
        &owner,
        |x| x.windows[0].geometry.stored_bound += 1,
        StagingInvariant::Geometry,
    );
    invalid(
        &owner,
        |x| x.windows[0].geometry.shard_bound += 1,
        StagingInvariant::Geometry,
    );
    invalid(
        &owner,
        |x| x.windows[0].geometry.ec.k = 0,
        StagingInvariant::Geometry,
    );
    invalid(
        &owner,
        |x| x.windows[0].geometry.logical_bound = u64::MAX,
        StagingInvariant::Geometry,
    );
    for size in [0, 1, INTERNAL_SEGMENT_SIZE as u64] {
        StagingGeometry::new(
            SegmentPayloadCodec::Cobblestone256SegmentV1,
            EcShape { k: 1, m: 0 },
            size,
        )
        .unwrap()
        .validate()
        .unwrap();
    }
    // Expired durable state must remain importable/replayable; it is not an
    // authority grant. No override or local clock-health state is consulted.
    assert!(owner.maximum_cutoff_ms < crate::clock::current_time_millis());
    owner.validate(&topology()).unwrap();
}

#[test]
fn staging_records_reject_missing_reordered_and_overlapping_windows() {
    let mut owner = fixture(StagingTarget::StreamPut);
    append(&mut owner);
    append(&mut owner);
    invalid(&owner, |x| x.windows.clear(), StagingInvariant::Windows);
    invalid(
        &owner,
        |x| {
            x.windows.remove(1);
        },
        StagingInvariant::Windows,
    );
    invalid(&owner, |x| x.windows.swap(0, 1), StagingInvariant::Windows);
    invalid(
        &owner,
        |x| x.windows[1].start = x.windows[0].start,
        StagingInvariant::Windows,
    );
    invalid(
        &owner,
        |x| x.reserved_end = GenerationId::new(19).unwrap(),
        StagingInvariant::Windows,
    );
    invalid(
        &owner,
        |x| x.maximum_cutoff_ms -= 1,
        StagingInvariant::Windows,
    );
    invalid(
        &owner,
        |x| x.maximum_cutoff_ms += 1,
        StagingInvariant::Windows,
    );
    invalid(
        &owner,
        |x| {
            x.references.remove(0);
        },
        StagingInvariant::Reference,
    );
}

#[test]
fn staging_records_validate_placement_and_ack_primary_independently() {
    let owner = fixture(multipart_target());
    invalid(
        &owner,
        |x| x.windows[0].participants.data_pg = PgId::new(99),
        StagingInvariant::Placement,
    );
    invalid(
        &owner,
        |x| x.windows[0].participants.placement_epoch = ClusterEpoch::INITIAL,
        StagingInvariant::Placement,
    );
    invalid(
        &owner,
        |x| {
            x.windows[0].participants.files.pop();
        },
        StagingInvariant::Placement,
    );
    invalid(
        &owner,
        |x| x.windows[0].participants.files[1] = x.windows[0].participants.files[0],
        StagingInvariant::Placement,
    );
    let original = owner.clone();
    let mut moved = owner;
    moved.windows[0].participants.acknowledgement_primary = NodeId::new(99);
    // An immutable descriptor records a primary independently; checking that
    // this was the captured authoritative route is the builder's responsibility.
    // No local topology/route resampling is permitted during replica validation.
    moved.validate(&topology()).unwrap();
    assert_ne!(moved, original);
}

#[test]
fn staging_records_keep_upload_part_payload_and_completed_object_generations_distinct() {
    let mut owner = fixture(multipart_target());
    append(&mut owner);
    publish(&mut owner);
    owner.validate(&topology()).unwrap();
    invalid(
        &owner,
        |x| {
            if let StagingDestination::Part {
                payload_generation, ..
            } = &mut x.references[0].destination
            {
                *payload_generation = GenerationId::new(7).unwrap();
            }
        },
        StagingInvariant::Reference,
    );
    invalid(
        &owner,
        |x| {
            if let StagingDestination::Part { part_number, .. } = &mut x.references[0].destination {
                *part_number += 1;
            }
        },
        StagingInvariant::Reference,
    );
    invalid(
        &owner,
        |x| {
            if let StagingDestination::Part { upload_id, .. } = &mut x.references[0].destination {
                *upload_id = crate::tests::multipart_upload_id("crossed-upload");
            }
        },
        StagingInvariant::Reference,
    );
    owner.references[0].destination = StagingDestination::Object {
        generation: GenerationId::new(7).unwrap(),
        part_number: Some(3),
    };
    owner.validate(&topology()).unwrap();
    invalid(
        &owner,
        |x| {
            if let StagingDestination::Object { generation, .. } = &mut x.references[0].destination
            {
                *generation = x.subject.payload_generation;
            }
        },
        StagingInvariant::Reference,
    );
    invalid(
        &owner,
        |x| {
            if let StagingDestination::Object { part_number, .. } = &mut x.references[0].destination
            {
                *part_number = None;
            }
        },
        StagingInvariant::Reference,
    );
    invalid(
        &owner,
        |x| x.references[0].physical_vid = x.subject.payload_generation,
        StagingInvariant::Reference,
    );
    invalid(
        &owner,
        |x| x.references[0].segment_hash[0] ^= 1,
        StagingInvariant::Reference,
    );
    invalid(
        &owner,
        |x| x.references[0].stored_size += 1,
        StagingInvariant::Reference,
    );
}

#[test]
fn staging_records_preserve_unused_windows_and_release_evidence_after_publication_and_closure() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let mut owner = fixture(target);
        if owner.subject.target != StagingTarget::DirectPut {
            append(&mut owner);
        }
        let windows = owner.windows.clone();
        publish(&mut owner);
        owner.validate(&topology()).unwrap();
        assert_eq!(owner.windows, windows);
        owner.lifecycle = StagingLifecycle::Retiring(StagingRetirement::PublishedRetained);
        // Closing allocation does not erase or invalidate a live object/part
        // binding; its entire exact reference set remains protected.
        owner.validate(&topology()).unwrap();
        for reference in &mut owner.references {
            reference.destination = StagingDestination::Reclaim {
                generation: owner.subject.published_generation(),
                part_number: match owner.subject.target {
                    StagingTarget::UploadPart { part_number, .. } => Some(part_number),
                    StagingTarget::DirectPut | StagingTarget::StreamPut => None,
                },
            };
        }
        owner.validate(&topology()).unwrap();
        invalid(
            &owner,
            |x| x.references.clear(),
            StagingInvariant::Reference,
        );
        owner.lifecycle = StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced);
        owner.references.clear();
        owner.validate(&topology()).unwrap();
        assert_eq!(owner.windows, windows);
        assert!(matches!(
            owner.resources.generation,
            ResourceResponsibility::ReleaseDue(_)
        ));
        assert!(matches!(
            owner.resources.creation,
            ResourceResponsibility::ReleaseDue(_)
        ));
    }
}

#[test]
fn staging_records_reject_resource_subject_and_release_route_substitution() {
    let owner = fixture(StagingTarget::DirectPut);
    let alter = |mutate: fn(&mut StagingBucketResource)| {
        invalid(
            &owner,
            |x| {
                let ResourceResponsibility::Live(resource) = &mut x.resources.creation else {
                    unreachable!()
                };
                mutate(resource);
            },
            StagingInvariant::Resource,
        );
    };
    alter(|x| x.identity.bucket = BucketName::try_from("another-bucket").unwrap());
    alter(|x| x.identity.incarnation += 1);
    alter(|x| x.identity.execution_generation = x.identity.incarnation - 1);
    alter(|x| x.identity.cluster_epoch = ClusterEpoch::new(6).unwrap());
    alter(|x| x.identity.target_context = Some("other-key".into()));
    alter(|x| {
        x.identity.operation_kind = UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND.into()
    });
    alter(|x| x.release_pg = PgId::new(99));
    alter(|x| x.observed_lease_deadline = x.identity.created_at);
    invalid(
        &owner,
        |x| x.resources.creation = ResourceResponsibility::NoResource,
        StagingInvariant::Resource,
    );
    invalid(
        &owner,
        |x| release_due(&mut x.resources.creation),
        StagingInvariant::Resource,
    );
    invalid(
        &owner,
        |x| x.resources.generation = ResourceResponsibility::NoResource,
        StagingInvariant::Resource,
    );
    invalid(
        &owner,
        |x| {
            if let ResourceResponsibility::Live(resource) = &mut x.resources.generation {
                resource.generation = GenerationId::new(13).unwrap();
            }
        },
        StagingInvariant::Resource,
    );
    invalid(
        &owner,
        |x| {
            if let ResourceResponsibility::Live(resource) = &mut x.resources.generation {
                resource.reservation_id = crate::tests::stream_session_id("other");
            }
        },
        StagingInvariant::Resource,
    );
}

#[test]
fn staging_records_bound_every_bucket_resource_identity_in_bytes() {
    fn identity(owner: &mut StagingOwnerRecord, finalization: bool, token: bool) -> &mut String {
        let resource = if finalization {
            &mut owner.resources.finalization
        } else {
            &mut owner.resources.creation
        };
        let (ResourceResponsibility::Live(proof) | ResourceResponsibility::ReleaseDue(proof)) =
            resource
        else {
            unreachable!()
        };
        if token {
            &mut proof.identity.owner_token
        } else {
            &mut proof.identity.reservation_id
        }
    }
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        for terminal in [false, true] {
            let mut owner = fixture(target.clone());
            if terminal {
                publish(&mut owner);
            }
            for finalization in [false, true] {
                if finalization && !(terminal && matches!(target, StagingTarget::UploadPart { .. }))
                {
                    continue;
                }
                for token in [false, true] {
                    for value in ["x".repeat(256), "é".repeat(128)] {
                        let mut at_limit = owner.clone();
                        *identity(&mut at_limit, finalization, token) = value;
                        at_limit.validate(&topology()).unwrap();
                    }
                    for value in [
                        String::new(),
                        "\0identity".into(),
                        "x".repeat(257),
                        "é".repeat(129),
                        "x".repeat(16 * 1024),
                    ] {
                        invalid(
                            &owner,
                            |record| *identity(record, finalization, token) = value,
                            StagingInvariant::Resource,
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn staging_records_retain_two_independent_upload_part_proofs_and_stable_identity() {
    let mut owner = fixture(multipart_target());
    append(&mut owner);
    publish(&mut owner);
    let mut finalization = proof(
        &owner.subject,
        UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
    );
    // A terminal request may use a successor route while creation responsibility
    // still names the original route. Validation must not rewrite either epoch.
    finalization.cluster_epoch = ClusterEpoch::new(6).unwrap();
    let captured = StagingBucketResource::capture(
        &finalization,
        PgId::new(topology().bucket_pg_for(&owner.subject.bucket)),
    );
    finalization.lease_deadline += 50;
    let renewed = StagingBucketResource::capture(&finalization, captured.release_pg);
    assert_eq!(captured.identity, renewed.identity);
    assert_ne!(
        captured.observed_lease_deadline,
        renewed.observed_lease_deadline
    );
    owner.resources.finalization = ResourceResponsibility::ReleaseDue(renewed.clone());
    owner.validate(&topology()).unwrap();
    let ResourceResponsibility::ReleaseDue(creation) = &owner.resources.creation else {
        unreachable!()
    };
    assert_ne!(creation.identity, renewed.identity);
    assert_ne!(
        creation.identity.cluster_epoch,
        renewed.identity.cluster_epoch
    );
    invalid(
        &owner,
        |x| x.resources.creation = x.resources.finalization.clone(),
        StagingInvariant::Resource,
    );
    invalid(
        &owner,
        |x| x.resources.finalization = x.resources.creation.clone(),
        StagingInvariant::Resource,
    );
    invalid(
        &owner,
        |x| {
            let ResourceResponsibility::ReleaseDue(resource) = &mut x.resources.finalization else {
                unreachable!()
            };
            resource.identity.cluster_epoch = ClusterEpoch::INITIAL;
        },
        StagingInvariant::Resource,
    );
}

#[test]
fn staging_records_enforce_lifecycle_and_last_representable_interval() {
    let mut direct = fixture(StagingTarget::DirectPut);
    let generation = GenerationId::new(i64::MAX as u64 - 1).unwrap();
    direct.subject.payload_generation = generation;
    direct.windows[0].start = generation;
    direct.windows[0].end = GenerationId::new(i64::MAX as u64).unwrap();
    direct.windows[0].participants.data_pg = PgId::new(direct.subject.data_pg(&topology(), 0));
    direct.reserved_end = direct.windows[0].end;
    let ResourceResponsibility::Live(resource) = &mut direct.resources.generation else {
        unreachable!()
    };
    resource.generation = generation;
    direct.validate(&topology()).unwrap();
    publish(&mut direct);
    direct.validate(&topology()).unwrap();
    direct.lifecycle = StagingLifecycle::Retiring(StagingRetirement::PublishedRetained);
    direct.validate(&topology()).unwrap();
    invalid(
        &direct,
        |x| x.references[0].destination = StagingDestination::Stream,
        StagingInvariant::Lifecycle,
    );
    invalid(
        &direct,
        |x| {
            let ResourceResponsibility::ReleaseDue(resource) = &x.resources.generation else {
                unreachable!()
            };
            x.resources.generation = ResourceResponsibility::Live(resource.clone());
        },
        StagingInvariant::Resource,
    );
    let mut stream = fixture(StagingTarget::StreamPut);
    append(&mut stream);
    invalid(
        &stream,
        |x| x.lifecycle = StagingLifecycle::Published,
        StagingInvariant::Lifecycle,
    );
    invalid(
        &stream,
        |x| {
            x.references[0].destination = StagingDestination::Object {
                generation: x.subject.payload_generation,
                part_number: None,
            }
        },
        StagingInvariant::Reference,
    );
}

#[test]
fn staging_records_terminal_lookahead_is_never_a_selected_payload() {
    let mut owner = fixture(StagingTarget::StreamPut);
    for _ in 0..MAX_STREAM_SEGMENTS {
        append(&mut owner);
    }
    owner.validate(&topology()).unwrap();
    assert_eq!(owner.windows.len(), MAX_STREAM_SEGMENTS as usize + 1);
    publish(&mut owner);
    owner.validate(&topology()).unwrap();
    invalid(
        &owner,
        |x| {
            let mut extra = reference(x, MAX_STREAM_SEGMENTS as usize);
            extra.destination = StagingDestination::Object {
                generation: x.subject.payload_generation,
                part_number: None,
            };
            x.references.push(extra);
        },
        StagingInvariant::Reference,
    );
}

fn completed_object_destination(owner: &StagingOwnerRecord) -> StagingDestination {
    StagingDestination::Object {
        generation: owner.subject.published_generation(),
        part_number: match owner.subject.target {
            StagingTarget::DirectPut | StagingTarget::StreamPut => None,
            StagingTarget::UploadPart { part_number, .. } => Some(part_number),
        },
    }
}

fn reclaim_destination(owner: &StagingOwnerRecord) -> StagingDestination {
    let StagingDestination::Object {
        generation,
        part_number,
    } = completed_object_destination(owner)
    else {
        unreachable!()
    };
    StagingDestination::Reclaim {
        generation,
        part_number,
    }
}

#[test]
fn staging_records_terminal_selection_requires_the_complete_ordered_prefix() {
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let mut owner = fixture(target);
        if owner.subject.target != StagingTarget::DirectPut {
            for _ in 0..3 {
                append(&mut owner);
            }
        }
        publish(&mut owner);
        for lifecycle in [
            StagingLifecycle::Published,
            StagingLifecycle::Retiring(StagingRetirement::PublishedRetained),
        ] {
            owner.lifecycle = lifecycle;
            invalid(
                &owner,
                |x| x.references.clear(),
                StagingInvariant::Reference,
            );
            invalid(
                &owner,
                |x| {
                    x.references.pop();
                },
                StagingInvariant::Reference,
            );
            if owner.subject.target != StagingTarget::DirectPut {
                invalid(
                    &owner,
                    |x| x.references.swap(0, 1),
                    StagingInvariant::Reference,
                );
                invalid(
                    &owner,
                    |x| x.references[1] = x.references[0].clone(),
                    StagingInvariant::Reference,
                );
                invalid(
                    &owner,
                    |x| {
                        let destination = x.references[0].destination.clone();
                        x.references.remove(0);
                        let mut lookahead = reference(x, x.windows.len() - 1);
                        lookahead.destination = destination;
                        x.references.push(lookahead);
                    },
                    StagingInvariant::Reference,
                );
            }
        }
    }
}

#[test]
fn staging_records_terminal_selection_cannot_split_valid_destinations() {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let mut owner = fixture(target);
        append(&mut owner);
        append(&mut owner);
        publish(&mut owner);
        let mut destinations = vec![
            completed_object_destination(&owner),
            reclaim_destination(&owner),
        ];
        if matches!(owner.subject.target, StagingTarget::UploadPart { .. }) {
            destinations.push(owner.references[0].destination.clone());
        }
        for lifecycle in [
            StagingLifecycle::Published,
            StagingLifecycle::Retiring(StagingRetirement::PublishedRetained),
        ] {
            owner.lifecycle = lifecycle;
            for destination in &destinations {
                for reference in &mut owner.references {
                    reference.destination = destination.clone();
                }
                // Prove each entire destination is legal on its own: rejection
                // below must depend on the split, not a bad subject/lifecycle.
                owner.validate(&topology()).unwrap();
                for crossed in &destinations {
                    if crossed != destination {
                        invalid(
                            &owner,
                            |x| x.references[1].destination = crossed.clone(),
                            StagingInvariant::Reference,
                        );
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponsibilityKind {
    Absent,
    Live,
    Due,
}

impl ResponsibilityKind {
    fn with<T>(self, value: T) -> ResourceResponsibility<T> {
        match self {
            Self::Absent => ResourceResponsibility::NoResource,
            Self::Live => ResourceResponsibility::Live(value),
            Self::Due => ResourceResponsibility::ReleaseDue(value),
        }
    }
}

#[test]
fn staging_records_resource_matrix_binds_every_role_to_target_and_lifecycle() {
    use ResponsibilityKind::{Absent, Due, Live};
    use StagingLifecycle::{Active, Published, Retiring};
    use StagingRetirement::{PublishedRetained, PublishedUnreferenced, Unpublished};

    // Expected states are explicit, independent of the production validator.
    // All 27 role combinations are checked for every target/lifecycle row.
    let standard = [
        (Active, Live, Live, &[Absent][..]),
        (Published, Due, Due, &[Absent][..]),
        (Retiring(Unpublished), Due, Due, &[Absent][..]),
        (Retiring(PublishedRetained), Due, Due, &[Absent][..]),
        (Retiring(PublishedUnreferenced), Due, Due, &[Absent][..]),
    ];
    let multipart = [
        (Active, Live, Due, &[Absent][..]),
        (Published, Due, Due, &[Due][..]),
        // An abandoned finalization may transfer its release responsibility
        // while closing the attempt, but not while leaving it Active.
        (Retiring(Unpublished), Due, Due, &[Absent, Due][..]),
        (Retiring(PublishedRetained), Due, Due, &[Due][..]),
        (Retiring(PublishedUnreferenced), Due, Due, &[Due][..]),
    ];
    for target in [
        StagingTarget::DirectPut,
        StagingTarget::StreamPut,
        multipart_target(),
    ] {
        let matrix = if matches!(target, StagingTarget::UploadPart { .. }) {
            &multipart
        } else {
            &standard
        };
        for (lifecycle, expected_generation, expected_creation, expected_finalization) in matrix {
            let mut owner = fixture(target.clone());
            if target != StagingTarget::DirectPut {
                append(&mut owner);
            }
            match lifecycle {
                Active | Retiring(Unpublished) => {}
                Published | Retiring(PublishedRetained | PublishedUnreferenced) => {
                    publish(&mut owner)
                }
            }
            owner.lifecycle = *lifecycle;
            if matches!(lifecycle, Retiring(Unpublished | PublishedUnreferenced)) {
                owner.references.clear();
            }
            let generation = StagingGenerationResource {
                reservation_id: owner.subject.id.attempt.clone(),
                generation: owner.subject.payload_generation,
            };
            let creation = StagingBucketResource::capture(
                &proof(&owner.subject, owner.subject.creation_operation()),
                PgId::new(topology().bucket_pg_for(&owner.subject.bucket)),
            );
            let finalization = finalization_resource(&owner);
            for generation_kind in [Absent, Live, Due] {
                for creation_kind in [Absent, Live, Due] {
                    for finalization_kind in [Absent, Live, Due] {
                        owner.resources = StagingResources {
                            generation: generation_kind.with(generation.clone()),
                            creation: creation_kind.with(creation.clone()),
                            finalization: finalization_kind.with(finalization.clone()),
                            abandoned_finalizations: vec![],
                        };
                        let expected = generation_kind == *expected_generation
                            && creation_kind == *expected_creation
                            && expected_finalization.contains(&finalization_kind);
                        let validated_topology = topology();
                        let result = owner.validate(&validated_topology);
                        assert_eq!(result.is_ok(), expected,
                            "target={target:?}, lifecycle={lifecycle:?}, generation={generation_kind:?}, creation={creation_kind:?}, finalization={finalization_kind:?}: {result:?}");
                        if !expected {
                            assert_eq!(result.unwrap_err(), StagingInvariant::Resource);
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn staging_records_empty_publication_still_retains_terminal_proof() {
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let mut owner = fixture(target);
        publish(&mut owner);
        assert!(owner.references.is_empty());
        for lifecycle in [
            StagingLifecycle::Published,
            StagingLifecycle::Retiring(StagingRetirement::PublishedRetained),
            StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced),
        ] {
            owner.lifecycle = lifecycle;
            owner.validate(&topology()).unwrap();
            invalid(
                &owner,
                |x| x.resources.generation = ResourceResponsibility::NoResource,
                StagingInvariant::Resource,
            );
            invalid(
                &owner,
                |x| x.resources.creation = ResourceResponsibility::NoResource,
                StagingInvariant::Resource,
            );
            if matches!(owner.subject.target, StagingTarget::UploadPart { .. }) {
                invalid(
                    &owner,
                    |x| x.resources.finalization = ResourceResponsibility::NoResource,
                    StagingInvariant::Resource,
                );
            }
        }
    }
}

#[test]
fn staging_records_unreferenced_retirement_cannot_keep_selected_payload() {
    let mut owner = fixture(multipart_target());
    append(&mut owner);
    publish(&mut owner);
    for retirement in [
        StagingRetirement::Unpublished,
        StagingRetirement::PublishedUnreferenced,
    ] {
        invalid(
            &owner,
            |x| x.lifecycle = StagingLifecycle::Retiring(retirement),
            StagingInvariant::Reference,
        );
    }
    owner.lifecycle = StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced);
    owner.references.clear();
    owner.validate(&topology()).unwrap();
    invalid(
        &owner,
        |x| x.resources.finalization = ResourceResponsibility::NoResource,
        StagingInvariant::Resource,
    );
}
