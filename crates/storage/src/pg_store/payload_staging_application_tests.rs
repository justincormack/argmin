// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only routing tests. The current v13 command is never recorded as
//! authentication of a separate descriptor/source, nor is production activated.

use super::super::abandonment::source::PartFinalizationSource;
use super::super::command_context::{ApplicationError, Applied, ContextError, ContextualCommand};
use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::{
    DeleteObjectVersionMode, MetadataCommandEnvelope, MetadataCommandPayload,
    ReserveObjectVersionCommand,
};
use crate::pg_store::PgStore;
use crate::VersionId;

// Test ingress with an explicit ordinary field, not an application constructor
// or a way to extract/reinterpret an already decoded context.
pub(super) fn ordinary_context(
    command: &MetadataCommandEnvelope,
    topology: &PgTopology,
) -> Result<ContextualCommand, ContextError> {
    ContextualCommand::decode_field(command.clone(), &[1, 0, 0], topology)
}

#[test]
fn staging_application_requires_descriptor_for_every_introduction_and_append() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target, is_append);
        let captured = candidate::capture(&pg, &owner, command, is_append);
        let topology = topology();
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        assert!(matches!(
            ordinary_context(captured.command(), &topology),
            Err(ContextError::CommandKind)
        ));
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        let context = ContextualCommand::described(captured, &topology).unwrap();
        assert!(matches!(
            context
                .application(&topology)
                .unwrap()
                .apply_in_open_transaction(&pg, 0),
            Err(ApplicationError::TransactionRequired)
        ));
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let before_index = pg
            .max_metadata_command_log_index(context.id().cluster_epoch())
            .unwrap();
        let result = context
            .application(&topology)
            .unwrap()
            .apply_in_open_transaction(&pg, 0)
            .unwrap();
        assert_eq!(
            result,
            Applied::Described(if is_append {
                super::super::candidate::CandidateApplied::Append(
                    super::super::append::AppendApplied::Appended,
                )
            } else {
                super::super::candidate::CandidateApplied::Introduction(
                    super::super::introduction::IntroductionApplied::Created,
                )
            })
        );
        let retained = append::load(&pg, &owner);
        assert_eq!(retained.windows.len(), 1 + usize::from(is_append));
        assert_eq!(retained.references.len(), usize::from(is_append));
        assert_eq!(
            pg.max_metadata_command_log_index(context.id().cluster_epoch())
                .unwrap(),
            before_index
        );
        pg.conn.execute_batch("ROLLBACK").unwrap();
        assert_eq!(introduction::state(&pg), before);
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Terminal {
    CloseDirect,
    CloseStream,
    ClosePart,
    ObjectDirect,
    ObjectStream,
    Part,
    Complete,
    Abort,
    Delete,
    Marker,
    Reclaim,
}

pub(super) const TERMINALS: [Terminal; 11] = [
    Terminal::CloseDirect,
    Terminal::CloseStream,
    Terminal::ClosePart,
    Terminal::ObjectDirect,
    Terminal::ObjectStream,
    Terminal::Part,
    Terminal::Complete,
    Terminal::Abort,
    Terminal::Delete,
    Terminal::Marker,
    Terminal::Reclaim,
];

pub(super) fn setup_terminal(pg: &PgStore, case: Terminal) -> MetadataCommandEnvelope {
    match case {
        Terminal::CloseDirect | Terminal::CloseStream | Terminal::ClosePart => {
            let owner = match case {
                Terminal::CloseDirect => object::initialize(pg, StagingTarget::DirectPut, 0),
                Terminal::CloseStream => object::initialize(pg, StagingTarget::StreamPut, 2),
                Terminal::ClosePart => part::initialize(pg, 2),
                _ => unreachable!(),
            };
            close::command(pg, &owner)
        }
        Terminal::ObjectDirect | Terminal::ObjectStream => {
            let target = if matches!(case, Terminal::ObjectDirect) {
                StagingTarget::DirectPut
            } else {
                StagingTarget::StreamPut
            };
            let owner = object::initialize(pg, target, 2);
            object::command(pg, &owner, VersionId::Null)
        }
        Terminal::Part => {
            let owner = part::initialize(pg, 2);
            part::command(pg, &owner)
        }
        Terminal::Complete | Terminal::Abort => {
            let owners = completion::setup(pg, 2);
            if matches!(case, Terminal::Complete) {
                completion::command(pg, &owners[0], VersionId::Null)
            } else {
                abort::command(pg, &owners[0])
            }
        }
        Terminal::Delete | Terminal::Marker => {
            let owners = delete::source(pg, 0, VersionId::Null);
            if matches!(case, Terminal::Delete) {
                delete::command(
                    pg,
                    &owners[0],
                    VersionId::Null,
                    DeleteObjectVersionMode::Specific,
                )
            } else {
                marker::command(pg, &owners[0], VersionId::Null)
            }
        }
        Terminal::Reclaim => reclaim::setup(pg, 0, VersionId::Null).1,
    }
}

fn expected_application(pg: &PgStore, case: Terminal, command: &MetadataCommandEnvelope) {
    // Independent explicit routing oracle; compare the full metadata state,
    // not just Ok or a record count. Setup never writes physical payload.
    let topology = topology();
    match case {
        Terminal::CloseDirect | Terminal::CloseStream | Terminal::ClosePart => {
            super::super::close::apply_in_open_transaction(pg, &topology, command).unwrap()
        }
        Terminal::ObjectDirect | Terminal::ObjectStream => {
            super::super::object::apply_in_open_transaction(pg, &topology, command).unwrap()
        }
        Terminal::Part => {
            super::super::part::apply_in_open_transaction(pg, &topology, command).unwrap()
        }
        Terminal::Complete | Terminal::Abort => {
            super::super::multipart::apply_in_open_transaction(pg, &topology, command).unwrap()
        }
        Terminal::Delete => {
            super::super::delete::apply_in_open_transaction(pg, &topology, command).unwrap()
        }
        Terminal::Marker => {
            super::super::marker::apply_in_open_transaction(pg, &topology, command).unwrap()
        }
        Terminal::Reclaim => {
            super::super::reclaim::apply_in_open_transaction(pg, &topology, command).unwrap()
        }
    }
}

#[test]
fn staging_application_routes_every_terminal_family_through_owner_transfer() {
    for case in TERMINALS {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::DirectPut).subject.object_pg.get(),
        )
        .unwrap();
        let command = setup_terminal(&pg, case);
        let before = introduction::state(&pg);
        pg.conn
            .execute_batch("SAVEPOINT expected_application")
            .unwrap();
        expected_application(&pg, case, &command);
        let expected = introduction::state(&pg);
        assert_ne!(
            expected, before,
            "{case:?} must actually transfer ownership"
        );
        pg.conn
            .execute_batch("ROLLBACK TO expected_application; RELEASE expected_application")
            .unwrap();
        assert_eq!(introduction::state(&pg), before);
        let topology = topology();
        let index = pg
            .max_metadata_command_log_index(command.id().cluster_epoch())
            .unwrap();
        assert_eq!(
            ordinary_context(&command, &topology)
                .unwrap()
                .application(&topology)
                .unwrap()
                .apply_in_open_transaction(&pg, 0)
                .unwrap(),
            Applied::Existing
        );
        assert_eq!(introduction::state(&pg), expected, "{case:?}");
        assert_eq!(
            pg.max_metadata_command_log_index(command.id().cluster_epoch())
                .unwrap(),
            index
        );
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

#[test]
fn staging_application_does_not_fall_back_when_terminal_owner_is_missing() {
    for case in TERMINALS {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::DirectPut).subject.object_pg.get(),
        )
        .unwrap();
        let command = setup_terminal(&pg, case);
        let topology = topology();
        // Bind first, then remove owners: binding is not cached authority.
        let context = ordinary_context(&command, &topology).unwrap();
        let application = context.application(&topology).unwrap();
        pg.conn
            .execute("DELETE FROM payload_staging_owners", [])
            .unwrap();
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        let error = application.apply_in_open_transaction(&pg, 0).unwrap_err();
        assert!(
            match case {
                Terminal::CloseDirect | Terminal::CloseStream | Terminal::ClosePart =>
                    matches!(error, ApplicationError::Close(_)),
                Terminal::ObjectDirect | Terminal::ObjectStream =>
                    matches!(error, ApplicationError::Object(_)),
                Terminal::Part => matches!(error, ApplicationError::Part(_)),
                Terminal::Complete | Terminal::Abort =>
                    matches!(error, ApplicationError::Multipart(_)),
                Terminal::Delete => matches!(error, ApplicationError::Delete(_)),
                Terminal::Marker => matches!(error, ApplicationError::Marker(_)),
                Terminal::Reclaim => matches!(error, ApplicationError::Reclaim(_)),
            },
            "{case:?}: {error:?}"
        );
        assert_eq!(pg.conn.total_changes(), writes, "{case:?}");
        assert_eq!(introduction::state(&pg), before, "{case:?}");
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

#[test]
fn staging_application_abandoned_part_requires_live_durable_source_evidence() {
    for has_tombstone in [false, true] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owner = part::initialize(&pg, 2);
        let source = part::command(&pg, &owner);
        let cleanup = abandonment::cleanup(&pg, &owner, &source);
        let carrier = PartFinalizationSource::capture(&source).unwrap();
        let topology = topology();
        let candidate = carrier.clone().bind_cleanup(cleanup, &topology).unwrap();
        let context = ContextualCommand::abandoned_part(candidate, &topology).unwrap();
        let application = context.application(&topology).unwrap();
        if has_tombstone {
            abandonment::terminal(&pg, &source, true);
        }
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        let result = application.apply_in_open_transaction(&pg, 0);
        if has_tombstone {
            assert_eq!(
                result.unwrap(),
                Applied::Abandonment(
                    super::super::abandonment::AbandonmentOutcome::ClosedAndRetained
                )
            );
            abandonment::assert_closed(&pg, &owner, &source);
        } else {
            assert!(
                matches!(
                    result,
                    Err(ApplicationError::Abandonment(
                        super::super::abandonment::AbandonmentError::NotAbandoned
                    ))
                ),
                "{result:?}"
            );
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
        }
        assert!(matches!(
            carrier.bind_cleanup(source.clone(), &topology),
            Err(super::super::abandonment::AbandonmentError::Subject)
        ));
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

fn reserve_version(pg_id: PgId) -> MetadataCommandEnvelope {
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            BucketName::new("bucket").unwrap(),
            ObjectKey::new("key").unwrap(),
            VersionId::from_u64(7),
        )),
    )
}

#[test]
fn staging_application_unchanged_commands_still_mutate_only_in_caller_transaction() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), 0).unwrap();
    let command = reserve_version(PgId::new(0));
    let topology = topology();
    let before = introduction::state(&pg);
    assert!(matches!(
        ordinary_context(&command, &topology)
            .unwrap()
            .application(&topology)
            .unwrap()
            .apply_in_open_transaction(&pg, 0),
        Err(ApplicationError::TransactionRequired)
    ));
    assert_eq!(introduction::state(&pg), before);
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert_eq!(
        ordinary_context(&command, &topology)
            .unwrap()
            .application(&topology)
            .unwrap()
            .apply_in_open_transaction(&pg, 0)
            .unwrap(),
        Applied::Existing
    );
    assert_ne!(introduction::state(&pg), before);
    assert_eq!(
        pg.max_metadata_command_log_index(ClusterEpoch::INITIAL)
            .unwrap(),
        0
    );
    pg.conn.execute_batch("ROLLBACK").unwrap();
    assert_eq!(introduction::state(&pg), before);
}

#[test]
fn staging_application_rejects_crossed_pg_and_topology_before_mutation() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), 0).unwrap();
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    let before = introduction::state(&pg);
    let writes = pg.conn.total_changes();
    for (command, topology) in [
        (reserve_version(PgId::new(1)), topology()),
        (
            reserve_version(PgId::new(0)),
            PgTopology::new(&[1, 2]).unwrap(),
        ),
    ] {
        assert!(matches!(
            ordinary_context(&command, &topology)
                .unwrap()
                .application(&topology)
                .unwrap()
                .apply_in_open_transaction(&pg, 0),
            Err(ApplicationError::Command)
        ));
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
    }
    pg.conn.execute_batch("ROLLBACK").unwrap();
}
