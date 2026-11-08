// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Mandatory-field interpretation, not an authenticated carrier/RPC test. The
//! v13 envelope and V1 descriptor are deliberately decoded separately here;
//! production activation must authenticate both in the coordinated format.

use super::super::candidate::{CandidateBindingError, DescribedStagingCommand};
use super::super::command_context::{ApplicationError, ContextError, ContextualCommand};
use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::{
    decode_metadata_command_envelope, MetadataCommandEnvelope, MetadataCommandPayload,
    ReserveObjectVersionCommand,
};
use crate::node_runtime::MetadataCommandDecodeAuthority;
use crate::pg_store::PgStore;

fn receive(
    command: &MetadataCommandEnvelope,
    descriptor: &[u8],
    topology: &PgTopology,
) -> Result<DescribedStagingCommand, CandidateBindingError> {
    let command = decode_metadata_command_envelope(
        &command.command_bytes(),
        &MetadataCommandDecodeAuthority::new_for_test(),
    )
    .unwrap();
    DescribedStagingCommand::decode_descriptor_field(command, descriptor, topology)
}

#[test]
fn staging_received_candidate_applies_witness_first_after_expiry_without_recapture() {
    let clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        clock.set(2_000);
        let (_dir, primary, owner, command) = candidate::setup(target, is_append);
        let captured = candidate::capture(&primary, &owner, command, is_append);
        let command = captured.command().clone();
        let descriptor = captured.descriptor_bytes();
        let topology = topology();
        let bound = captured.bind(&topology).unwrap();
        let protection = bound.pending_protection().reference();
        let routes = bound.pending_protection().historical_routes();

        // Replicate the pre-command state, not the primary's live allocation
        // authority. The witness has its own PG lifetime and does no capture.
        primary.refresh_metadata_command_state_digest().unwrap();
        let checkpoint_epoch = primary
            .metadata_command_replica_state()
            .unwrap()
            .cluster_epoch;
        let checkpoint = primary
            .metadata_command_checkpoint(owner.subject.issuer.node_id.as_u32(), checkpoint_epoch)
            .unwrap();
        let witness_dir = test_util::tempdir();
        let witness = PgStore::open(witness_dir.path(), owner.subject.object_pg.get()).unwrap();
        witness
            .install_metadata_transfer_checkpoint_base(
                7,
                command.id().cluster_epoch(),
                &checkpoint,
                &topology,
            )
            .unwrap();
        assert_ne!(
            witness.stream_allocation_issuer(NodeId::new(7)).unwrap(),
            owner.subject.issuer
        );
        drop(captured);
        clock.set(9_000_000);

        let mut expected_owner = None;
        for (pg, node_id) in [
            (&witness, 7),
            (&primary, owner.subject.issuer.node_id.as_u32()),
        ] {
            let before = introduction::state(pg);
            let writes = pg.conn.total_changes();
            let received = receive(&command, &descriptor, &topology).unwrap();
            assert_eq!(received.command(), &command);
            assert_eq!(received.descriptor_bytes(), descriptor);
            let bound = received.bind(&topology).unwrap();
            assert_eq!(bound.pending_protection().reference(), protection);
            assert_eq!(bound.pending_protection().historical_routes(), routes);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(pg), before);
            assert!(matches!(
                application::ordinary_context(received.command(), &topology),
                Err(ContextError::CommandKind)
            ));
            let context = ContextualCommand::described(received, &topology).unwrap();
            assert!(matches!(
                context
                    .application(&topology)
                    .unwrap()
                    .apply_in_open_transaction(pg, node_id),
                Err(ApplicationError::TransactionRequired)
            ));

            let prefix = pg
                .max_metadata_command_log_index(command.id().cluster_epoch())
                .unwrap();
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            context
                .application(&topology)
                .unwrap()
                .apply_in_open_transaction(pg, node_id)
                .unwrap();
            let retained = append::load(pg, &owner);
            assert_eq!(retained.subject, owner.subject);
            assert_eq!(retained.windows.len(), 1 + usize::from(is_append));
            assert_eq!(retained.references.len(), usize::from(is_append));
            if let Some(expected) = &expected_owner {
                assert_eq!(&retained, expected);
            } else {
                expected_owner = Some(retained.clone());
            }
            let writes = pg.conn.total_changes();
            context
                .application(&topology)
                .unwrap()
                .apply_in_open_transaction(pg, node_id)
                .unwrap();
            assert_eq!(
                pg.conn.total_changes(),
                writes,
                "exact replay rewrote state"
            );
            // No recording of an unauthenticated descriptor as applied v13.
            assert_eq!(
                pg.max_metadata_command_log_index(command.id().cluster_epoch())
                    .unwrap(),
                prefix
            );
            pg.conn.execute_batch("ROLLBACK").unwrap();
            assert_eq!(introduction::state(pg), before);
        }
    }
}

#[test]
fn staging_received_candidate_rejects_absent_malformed_and_wrong_kind_fields() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target, is_append);
        let captured = candidate::capture(&pg, &owner, command, is_append);
        let bytes = captured.descriptor_bytes();
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        for end in 0..bytes.len() {
            assert!(
                matches!(
                    receive(captured.command(), &bytes[..end], &topology()),
                    Err(CandidateBindingError::Descriptor(_))
                ),
                "accepted prefix {end}"
            );
        }
        let mut wrong_kind = bytes.clone();
        wrong_kind[10] = u8::from(!is_append);
        let mut trailing = bytes.clone();
        trailing.push(0);
        let mut wrong_version = bytes.clone();
        wrong_version[8..10].copy_from_slice(&2_u16.to_le_bytes());
        for malformed in [wrong_kind, trailing, wrong_version, vec![0; 16 * 1024 + 1]] {
            assert!(matches!(
                receive(captured.command(), &malformed, &topology()),
                Err(CandidateBindingError::Descriptor(_))
            ));
        }
        let ordinary = MetadataCommandEnvelope::new(
            captured.command().id(),
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand {
                bucket: owner.subject.bucket.clone(),
                key: owner.subject.key.clone(),
                version_id: crate::VersionId::from_u64(1),
            }),
        );
        assert!(matches!(
            receive(&ordinary, &bytes, &topology()),
            Err(CandidateBindingError::CommandKind)
        ));
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_received_candidate_rejects_independently_crossed_command_subjects() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target, is_append);
        let captured = candidate::capture(&pg, &owner, command, is_append);
        let bytes = captured.descriptor_bytes();
        let command = captured.command();
        let crossed_key = (0..1_000)
            .map(|i| ObjectKey::try_from(format!("other-{i}")).unwrap())
            .find(|key| {
                topology().object_pg_for(&owner.subject.bucket, key)
                    == owner.subject.object_pg.get()
            })
            .unwrap();
        assert_ne!(crossed_key, owner.subject.key);
        assert_eq!(
            topology().object_pg_for(&owner.subject.bucket, &crossed_key),
            owner.subject.object_pg.get()
        );
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        for field in 0..4 {
            let mut id = command.id();
            let mut payload = command.payload().clone();
            if field == 0 {
                id = MetadataCommandId::new(
                    id.cluster_epoch(),
                    PgId::new((id.pg_id().get() + 1) % 8),
                    id.log_index(),
                );
            } else {
                match &mut payload {
                    MetadataCommandPayload::ReserveObjectGeneration(reserve) => match field {
                        1 => reserve.key = crossed_key.clone(),
                        2 => reserve.reservation_id = crate::tests::stream_session_id("crossed"),
                        3 => {
                            reserve.generation_id =
                                GenerationId::new(reserve.generation_id.get() + 1).unwrap()
                        }
                        _ => unreachable!(),
                    },
                    MetadataCommandPayload::CreateStreamUpload(create) => match field {
                        1 => create.session.key = crossed_key.clone(),
                        2 => create.session.session_id = crate::tests::stream_session_id("crossed"),
                        3 => {
                            create.payload_generation =
                                GenerationId::new(create.payload_generation.get() + 1).unwrap()
                        }
                        _ => unreachable!(),
                    },
                    MetadataCommandPayload::AppendStreamSegment(append) => match field {
                        1 => append.key = crossed_key.clone(),
                        2 => append.segment.session_id = crate::tests::stream_session_id("crossed"),
                        3 => {
                            append.segment.segment_vid =
                                GenerationId::new(append.segment.segment_vid.get() + 1).unwrap()
                        }
                        _ => unreachable!(),
                    },
                    _ => unreachable!(),
                }
            }
            // Canonical, correctly checksummed commands: reject correspondence,
            // not an unrelated malformed envelope/CRC failure.
            let crossed = MetadataCommandEnvelope::new(id, payload);
            let result = receive(&crossed, &bytes, &topology());
            assert!(
                if is_append {
                    matches!(
                        result,
                        Err(CandidateBindingError::Append(
                            super::super::append::AppendError::CommandBinding
                        ))
                    )
                } else {
                    matches!(
                        result,
                        Err(CandidateBindingError::Introduction(
                            super::super::introduction::IntroductionError::CommandBinding
                        ))
                    )
                },
                "crossed field {field} must fail command binding"
            );
        }
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_received_candidate_rejects_crossed_data_topology_at_decode_and_rebind() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target, is_append);
        let captured = candidate::capture(&pg, &owner, command, is_append);
        let bytes = captured.descriptor_bytes();
        let topology = topology();
        let bound = captured.bind(&topology).unwrap();
        let protection = bound.pending_protection();
        let (_, _, subject, window) = protection.index_inputs();
        let foreign = (1_u32..256)
            .find_map(|mask| {
                let ids: Vec<_> = (0..8).filter(|pg| mask & (1 << pg) != 0).collect();
                let other = PgTopology::new(&ids).unwrap();
                (other.object_pg_for(&subject.bucket, &subject.key) == subject.object_pg.get()
                    && subject.data_pg(&other, window.index) != window.participants.data_pg.get())
                .then_some(other)
            })
            .expect("same object PG with different data placement");
        assert_eq!(
            foreign.object_pg_for(&subject.bucket, &subject.key),
            subject.object_pg.get()
        );
        assert_ne!(
            subject.data_pg(&foreign, window.index),
            window.participants.data_pg.get()
        );
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        let result = receive(captured.command(), &bytes, &foreign);
        assert!(if is_append {
            matches!(
                result,
                Err(CandidateBindingError::Append(
                    super::super::append::AppendError::Invariant(StagingInvariant::Placement)
                ))
            )
        } else {
            matches!(
                result,
                Err(CandidateBindingError::Introduction(
                    super::super::introduction::IntroductionError::Invariant(
                        StagingInvariant::Placement
                    )
                ))
            )
        });
        let received = receive(captured.command(), &bytes, &topology).unwrap();
        assert!(matches!(
            received.bind(&foreign),
            Err(CandidateBindingError::Topology)
        ));
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_received_candidate_survives_restart_and_reissue_without_renewing_description() {
    let clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        clock.set(2_000);
        let (dir, pg, owner, command) = candidate::setup(target, is_append);
        let captured = candidate::capture(&pg, &owner, command, is_append);
        let bytes = captured.descriptor_bytes();
        let command = captured.command().clone();
        drop(captured);
        drop(pg);
        clock.set(9_000_000);
        let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        assert_ne!(
            pg.stream_allocation_issuer(owner.subject.issuer.node_id)
                .unwrap(),
            owner.subject.issuer
        );
        let topology = topology();
        let received = receive(&command, &bytes, &topology).unwrap();
        let before = introduction::state(&pg);
        for id in [
            MetadataCommandId::new(
                command.id().cluster_epoch(),
                command.id().pg_id(),
                MetadataCommandLogIndex::new(command.id().log_index().get() + 1).unwrap(),
            ),
            MetadataCommandId::new(
                ClusterEpoch::new(command.id().cluster_epoch().get() + 1).unwrap(),
                command.id().pg_id(),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
        ] {
            let reissued = received.reissued_at(id).unwrap();
            assert_eq!(reissued.descriptor_bytes(), bytes);
            let decoded = receive(reissued.command(), &bytes, &topology).unwrap();
            assert_eq!(decoded.descriptor_bytes(), bytes);
            let original = received.bind(&topology).unwrap();
            let replacement = decoded.bind(&topology).unwrap();
            assert_eq!(
                replacement.pending_protection().reference(),
                original.pending_protection().reference()
            );
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            ContextualCommand::described(decoded, &topology)
                .unwrap()
                .application(&topology)
                .unwrap()
                .apply_in_open_transaction(&pg, 0)
                .unwrap();
            let retained = append::load(&pg, &owner);
            assert_eq!(retained.subject, owner.subject);
            assert!(retained.maximum_cutoff_ms < 9_000_000);
            pg.conn.execute_batch("ROLLBACK").unwrap();
            assert_eq!(introduction::state(&pg), before);
        }
    }
}
