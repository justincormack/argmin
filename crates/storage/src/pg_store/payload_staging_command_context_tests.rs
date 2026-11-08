// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Required-field evidence, not an installed authenticated-command/RPC test.
//! All fixtures are metadata-only; no physical payload is written here.

use super::super::abandonment::{source::PartFinalizationSource, AbandonmentError};
use super::super::command_context::{ApplicationError, Applied};
use super::super::command_context::{ContextError, ContextualCommand};
use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandPayload, ReserveObjectVersionCommand,
};
use crate::pg_store::PgStore;
use crate::VersionId;

fn ordinary(owner: &StagingOwnerRecord) -> MetadataCommandEnvelope {
    MetadataCommandEnvelope::new(
        owner.subject.id.introduction,
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            owner.subject.bucket.clone(),
            owner.subject.key.clone(),
            VersionId::from_u64(17),
        )),
    )
}

// Independent framing literals. These never use production version/tag/size
// constants; the nested encodings have their own frozen complete specimens.
fn field(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut bytes = vec![1, 0, tag];
    if tag != 0 {
        bytes.extend_from_slice(&u32::try_from(body.len()).unwrap().to_le_bytes());
        bytes.extend_from_slice(body);
    }
    bytes
}

#[test]
fn staging_command_context_v1_pins_ordinary_and_descriptor_composites() {
    let owner = fixture(StagingTarget::DirectPut);
    assert_eq!(
        ContextualCommand::decode_field(ordinary(&owner), &[1, 0, 0], &topology())
            .unwrap()
            .encode_field()
            .unwrap(),
        [1, 0, 0]
    );

    for (target, append, header, literal) in [
        (
            StagingTarget::DirectPut,
            false,
            [1, 0, 1, 161, 1, 0, 0],
            include_str!("testdata/staging_descriptor_v1_intro_direct_put.hex"),
        ),
        (
            StagingTarget::StreamPut,
            false,
            [1, 0, 1, 161, 1, 0, 0],
            include_str!("testdata/staging_descriptor_v1_intro_stream_put.hex"),
        ),
        (
            multipart_target(),
            false,
            [1, 0, 1, 52, 2, 0, 0],
            include_str!("testdata/staging_descriptor_v1_intro_upload_part.hex"),
        ),
        (
            StagingTarget::StreamPut,
            true,
            [1, 0, 2, 241, 0, 0, 0],
            include_str!("testdata/staging_descriptor_v1_append_stream_put.hex"),
        ),
        (
            multipart_target(),
            true,
            [1, 0, 2, 129, 1, 0, 0],
            include_str!("testdata/staging_descriptor_v1_append_upload_part.hex"),
        ),
    ] {
        let owner = fixture(target);
        let command = if append {
            super::append::command(&owner)
        } else {
            introduction::command(&owner)
        };
        let body = crate::tests::frozen_hex_fixture(literal);
        let expected = [header.as_slice(), &body].concat();
        let decoded = ContextualCommand::decode_field(command, &expected, &topology()).unwrap();
        assert_eq!(decoded.encode_field().unwrap(), expected);
    }
}

#[test]
fn staging_command_context_round_trip_selects_required_application_after_expiry() {
    let clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        clock.set(2_000);
        let (_dir, pg, owner, command) = candidate::setup(target, is_append);
        let captured = candidate::capture(&pg, &owner, command.clone(), is_append);
        let descriptor = captured.descriptor_bytes();
        let input = ContextualCommand::described(captured, &topology()).unwrap();
        let bytes = input.encode_field().unwrap();
        assert_eq!(bytes, field(if is_append { 2 } else { 1 }, &descriptor));
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        clock.set(9_000_000);
        let decoded = ContextualCommand::decode_field(command, &bytes, &topology()).unwrap();
        assert_eq!(decoded.id(), input.id());
        assert_eq!(decoded.encode_field().unwrap(), bytes);
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        assert!(matches!(
            decoded
                .application(&topology())
                .unwrap()
                .apply_in_open_transaction(&pg, 0),
            Err(ApplicationError::TransactionRequired)
        ));
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert!(matches!(
            decoded
                .application(&topology())
                .unwrap()
                .apply_in_open_transaction(&pg, 0)
                .unwrap(),
            Applied::Described(_)
        ));
        let retained = append::load(&pg, &owner);
        assert_eq!(retained.subject, owner.subject);
        assert_eq!(retained.windows.len(), 1 + usize::from(is_append));
        assert_eq!(retained.references.len(), usize::from(is_append));
        pg.conn.execute_batch("ROLLBACK").unwrap();
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_command_context_rejects_missing_crossed_and_malformed_fields_before_effects() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target, is_append);
        let captured = candidate::capture(&pg, &owner, command.clone(), is_append);
        let input = ContextualCommand::described(captured, &topology()).unwrap();
        let valid = input.encode_field().unwrap();
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        for end in 0..valid.len() {
            assert!(
                ContextualCommand::decode_field(command.clone(), &valid[..end], &topology())
                    .is_err(),
                "prefix {end}"
            );
        }
        for tag in [0, if is_append { 1 } else { 2 }, 3] {
            let wrong = field(tag, &valid[7..]);
            assert!(matches!(
                ContextualCommand::decode_field(command.clone(), &wrong, &topology()),
                Err(ContextError::CommandKind)
            ));
        }
        assert!(matches!(
            ContextualCommand::decode_field(command.clone(), &[1, 0, 0], &topology()),
            Err(ContextError::CommandKind)
        ));
        for version in [0_u16, 2, u16::MAX] {
            let mut wrong = valid.clone();
            wrong[..2].copy_from_slice(&version.to_le_bytes());
            assert!(matches!(
                ContextualCommand::decode_field(command.clone(), &wrong, &topology()),
                Err(ContextError::Encoding("unsupported context version"))
            ));
        }
        for length in [16_385, u32::MAX] {
            let mut wrong = valid.clone();
            wrong[3..7].copy_from_slice(&length.to_le_bytes());
            assert!(matches!(
                ContextualCommand::decode_field(command.clone(), &wrong, &topology()),
                Err(ContextError::Encoding("context exceeds kind bound"))
            ));
        }
        let mut wrong = valid.clone();
        wrong.push(0);
        assert!(matches!(
            ContextualCommand::decode_field(command.clone(), &wrong, &topology()),
            Err(ContextError::Encoding("context length/trailing bytes"))
        ));
        wrong = valid.clone();
        wrong[2] = 255;
        assert!(matches!(
            ContextualCommand::decode_field(command.clone(), &wrong, &topology()),
            Err(ContextError::Encoding("unknown context kind"))
        ));
        wrong = valid.clone();
        wrong[7 + 8] = 2; // nested descriptor version, with valid outer framing
        assert!(matches!(
            ContextualCommand::decode_field(command.clone(), &wrong, &topology()),
            Err(ContextError::Candidate(_))
        ));
        assert!(matches!(
            ContextualCommand::decode_field(ordinary(&owner), &valid, &topology()),
            Err(ContextError::CommandKind)
        ));
        assert!(
            ContextualCommand::decode_field(ordinary(&owner), &[1, 0, 0, 0], &topology()).is_err()
        );
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_command_context_rejects_crossed_subjects_and_configured_topologies() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target, is_append);
        let captured = candidate::capture(&pg, &owner, command.clone(), is_append);
        let input = ContextualCommand::described(captured, &topology()).unwrap();
        let bytes = input.encode_field().unwrap();
        let mut payload = command.payload().clone();
        let crossed_key = (0..1_000)
            .map(|i| ObjectKey::try_from(format!("crossed-context-{i}")).unwrap())
            .find(|key| topology().object_pg_for(&owner.subject.bucket, key) == pg.pg_id)
            .unwrap();
        assert_ne!(crossed_key, owner.subject.key);
        assert_eq!(
            topology().object_pg_for(&owner.subject.bucket, &crossed_key),
            pg.pg_id
        );
        match &mut payload {
            MetadataCommandPayload::ReserveObjectGeneration(command) => command.key = crossed_key,
            MetadataCommandPayload::CreateStreamUpload(command) => {
                command.session.key = crossed_key
            }
            MetadataCommandPayload::AppendStreamSegment(command) => command.key = crossed_key,
            _ => unreachable!(),
        }
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        let crossed = MetadataCommandEnvelope::new(command.id(), payload);
        assert!(matches!(
            ContextualCommand::decode_field(crossed, &bytes, &topology()),
            Err(ContextError::Candidate(_))
        ));
        let foreign = PgTopology::new(&[pg.pg_id]).unwrap();
        assert_ne!(foreign, topology());
        assert!(matches!(
            input.application(&foreign),
            Err(ApplicationError::Candidate(_))
        ));
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_command_context_preserves_source_and_requires_durable_abandonment() {
    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owner = part::initialize(&pg, count);
        let source = part::command(&pg, &owner);
        let cleanup = abandonment::cleanup(&pg, &owner, &source);
        let carrier = PartFinalizationSource::capture(&source).unwrap();
        let original_source = carrier.encode();
        let candidate = carrier.bind_cleanup(cleanup.clone(), &topology()).unwrap();
        let input = ContextualCommand::abandoned_part(candidate, &topology()).unwrap();
        let bytes = input.encode_field().unwrap();
        assert_eq!(bytes, field(3, &original_source));
        let received =
            ContextualCommand::decode_field(cleanup.clone(), &bytes, &topology()).unwrap();
        assert_eq!(received.encode_field().unwrap(), bytes);
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        assert!(matches!(
            received
                .application(&topology())
                .unwrap()
                .apply_in_open_transaction(&pg, 0),
            Err(ApplicationError::Abandonment(
                AbandonmentError::NotAbandoned
            ))
        ));
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        // Ordinary abort and abandonment follow-up are explicitly different
        // field values. Only the future enclosing hash can authenticate which
        // was sent; the v13 envelope alone is intentionally identical here.
        let ordinary = ContextualCommand::decode_field(cleanup, &[1, 0, 0], &topology()).unwrap();
        assert_eq!(ordinary.id(), received.id());
        assert_eq!(ordinary.encode_field().unwrap(), [1, 0, 0]);
        assert_ne!(ordinary.encode_field().unwrap(), bytes);
        abandonment::terminal(&pg, &source, true);
        assert!(matches!(
            received
                .application(&topology())
                .unwrap()
                .apply_in_open_transaction(&pg, 0)
                .unwrap(),
            Applied::Abandonment(_)
        ));
        abandonment::assert_closed(&pg, &owner, &source);
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

#[test]
fn staging_command_context_source_bounds_and_wrong_command_fail_before_storage() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = part::initialize(&pg, 2);
    let source = part::command(&pg, &owner);
    let cleanup = abandonment::cleanup(&pg, &owner, &source);
    let body = PartFinalizationSource::capture(&source).unwrap().encode();
    let valid = field(3, &body);
    let before = introduction::state(&pg);
    let writes = pg.conn.total_changes();
    assert!(matches!(
        ContextualCommand::decode_field(source.clone(), &valid, &topology()),
        Err(ContextError::CommandKind)
    ));
    for length in [2 * 1024 * 1024 + 23, u32::MAX] {
        let mut wrong = valid.clone();
        wrong[3..7].copy_from_slice(&length.to_le_bytes());
        assert!(matches!(
            ContextualCommand::decode_field(cleanup.clone(), &wrong, &topology()),
            Err(ContextError::Encoding("context exceeds kind bound"))
        ));
    }
    for end in 0..valid.len() {
        assert!(
            ContextualCommand::decode_field(cleanup.clone(), &valid[..end], &topology()).is_err(),
            "source prefix {end}"
        );
    }
    let mut wrong = valid.clone();
    wrong[7 + 8] = 2;
    assert!(matches!(
        ContextualCommand::decode_field(cleanup.clone(), &wrong, &topology()),
        Err(ContextError::Abandonment(_))
    ));
    wrong = valid;
    wrong[7 + 10] ^= 1; // nested source checksum
    assert!(matches!(
        ContextualCommand::decode_field(cleanup, &wrong, &topology()),
        Err(ContextError::Abandonment(_))
    ));
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(&pg), before);
    pg.conn.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn staging_command_context_cannot_be_downgraded_to_ordinary_abort() {
    use super::super::command_context::CommandApplication;

    // Same-crate negative API canaries, compiled by ordinary Nextest builds.
    // These fallback traits exist only in this test. Reintroducing any inherent
    // raw accessor/constructor shadows its fallback and fails the independent
    // RemovedApi type assertion below. Unlike an external compile-fail doctest,
    // this does not pass merely because the entire module is crate-private.
    struct RemovedApi;
    trait NoRawEnvelope {
        fn command(&self) -> RemovedApi {
            RemovedApi
        }
    }
    impl NoRawEnvelope for ContextualCommand {}
    trait NoOrdinaryReconstruction {
        fn ordinary(_: MetadataCommandEnvelope, _: &PgTopology) -> RemovedApi {
            RemovedApi
        }
    }
    impl NoOrdinaryReconstruction for ContextualCommand {}
    trait NoRawApplication {
        fn ordinary(_: &MetadataCommandEnvelope, _: &PgTopology) -> RemovedApi {
            RemovedApi
        }
    }
    impl NoRawApplication for CommandApplication<'_> {}

    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owner = part::initialize(&pg, count);
        let source = part::command(&pg, &owner);
        let cleanup = abandonment::cleanup(&pg, &owner, &source);
        let bytes = field(
            3,
            &PartFinalizationSource::capture(&source).unwrap().encode(),
        );
        let topology = topology();
        let received = ContextualCommand::decode_field(cleanup.clone(), &bytes, &topology).unwrap();

        // Pin every leg of the reported downgrade independently, so one closed
        // leg cannot mask reopening another. There is no raw envelope to clone
        // from received, no context ordinary constructor, and no parallel raw
        // ordinary-application constructor.
        let _: RemovedApi = received.command();
        let _: RemovedApi = ContextualCommand::ordinary(cleanup.clone(), &topology);
        let _: RemovedApi = CommandApplication::ordinary(&cleanup, &topology);

        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        assert!(matches!(
            received
                .application(&topology)
                .unwrap()
                .apply_in_open_transaction(&pg, 0),
            Err(ApplicationError::Abandonment(
                AbandonmentError::NotAbandoned
            ))
        ));
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);

        // Positive exploit canary: an independently supplied *ordinary* input
        // really would close this same owner without finalization-proof custody.
        // Build it from the original fixture, not a projection of received.
        let control = ContextualCommand::decode_field(cleanup, &[1, 0, 0], &topology).unwrap();
        pg.conn.execute_batch("SAVEPOINT ordinary_control").unwrap();
        assert_eq!(
            control
                .application(&topology)
                .unwrap()
                .apply_in_open_transaction(&pg, 0)
                .unwrap(),
            Applied::Existing
        );
        let closed = append::load(&pg, &owner);
        assert!(matches!(
            closed.resources.finalization,
            ResourceResponsibility::NoResource
        ));
        assert_ne!(introduction::state(&pg), before);
        pg.conn
            .execute_batch("ROLLBACK TO ordinary_control; RELEASE ordinary_control")
            .unwrap();
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(received.encode_field().unwrap(), bytes);

        // The only available application of received still takes the source
        // branch. A real tombstone enables it and retains the exact proof.
        abandonment::terminal(&pg, &source, true);
        assert!(matches!(
            received
                .application(&topology)
                .unwrap()
                .apply_in_open_transaction(&pg, 0)
                .unwrap(),
            Applied::Abandonment(_)
        ));
        abandonment::assert_closed(&pg, &owner, &source);
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}
