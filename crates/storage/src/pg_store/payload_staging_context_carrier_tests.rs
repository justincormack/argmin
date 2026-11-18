// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Complete candidate-carrier integration, not live RPC activation. These
//! metadata-only tests decode into the retained application input; they do not
//! simulate physical payloads or claim custody/recovery authorization.

use super::super::abandonment::{source::PartFinalizationSource, AbandonmentError};
use super::super::command_context::{
    ApplicationError, Applied, CarrierError, ContextError, ContextualCommand,
};
use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::{
    context_carrier, decode_metadata_command_envelope, MetadataCommandEnvelope,
    MetadataCommandFormatError, MetadataCommandPayload, MetadataCommandRecordFormat,
    ReserveObjectVersionCommand, MAX_METADATA_COMMAND_BYTES,
};
use crate::node_runtime::{
    pg_store::PgStore, CommandContextCodecAuthority, MetadataCommandDecodeAuthority,
};
use crate::VersionId;

fn ordinary() -> (MetadataCommandEnvelope, ContextualCommand) {
    let owner = fixture(StagingTarget::DirectPut);
    let command = MetadataCommandEnvelope::new(
        owner.subject.id.introduction,
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            owner.subject.bucket,
            owner.subject.key,
            VersionId::from_u64(17),
        )),
    );
    let context =
        ContextualCommand::decode_field(command.clone(), &[1, 0, 0], &topology()).unwrap();
    (command, context)
}

fn decode(bytes: &[u8]) -> Result<ContextualCommand, CarrierError> {
    ContextualCommand::decode_command(bytes, checksum::crc64::checksum(bytes), &topology())
}

#[test]
fn staging_complete_carrier_v14_has_exact_context_v1_composites() {
    let (_, ordinary) = ordinary();
    let mut cases = vec![("ordinary", ordinary)];
    for (label, target, is_append, literal) in [
        (
            "direct",
            StagingTarget::DirectPut,
            false,
            include_str!("testdata/staging_descriptor_v1_intro_direct_put.hex"),
        ),
        (
            "stream",
            StagingTarget::StreamPut,
            false,
            include_str!("testdata/staging_descriptor_v1_intro_stream_put.hex"),
        ),
        (
            "part",
            multipart_target(),
            false,
            include_str!("testdata/staging_descriptor_v1_intro_upload_part.hex"),
        ),
        (
            "append-stream",
            StagingTarget::StreamPut,
            true,
            include_str!("testdata/staging_descriptor_v1_append_stream_put.hex"),
        ),
        (
            "append-part",
            multipart_target(),
            true,
            include_str!("testdata/staging_descriptor_v1_append_upload_part.hex"),
        ),
    ] {
        let owner = fixture(target);
        let command = if is_append {
            append::command(&owner)
        } else {
            introduction::command(&owner)
        };
        let descriptor = crate::tests::frozen_hex_fixture(literal);
        let mut field = vec![1, 0, if is_append { 2 } else { 1 }];
        field.extend_from_slice(&u32::try_from(descriptor.len()).unwrap().to_le_bytes());
        field.extend_from_slice(&descriptor);
        cases.push((
            label,
            ContextualCommand::decode_field(command, &field, &topology()).unwrap(),
        ));
    }
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = part::initialize(&pg, 2);
    let source = part::command(&pg, &owner);
    cases.push((
        "abandoned-part",
        ContextualCommand::abandoned_part(
            PartFinalizationSource::capture_for_test(&source)
                .unwrap()
                .bind_cleanup(abandonment::cleanup(&pg, &owner, &source), &topology())
                .unwrap(),
            &topology(),
        )
        .unwrap(),
    ));
    let mut actual = String::new();
    for (label, context) in cases {
        use std::fmt::Write;
        let encoded = context.encode_command().unwrap();
        let bytes = encoded
            .bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let digest = encoded
            .command_sha256()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        writeln!(
            actual,
            "{label} {:016x} {digest} {bytes}",
            encoded.checksum_crc64()
        )
        .unwrap();
        assert_eq!(
            decode(encoded.bytes())
                .unwrap()
                .encode_command()
                .unwrap()
                .bytes(),
            encoded.bytes()
        );
    }
    let predecessor = include_str!("testdata/staging_context_carrier_v14_context_v1.hex")
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
        .collect::<Vec<_>>();
    assert_eq!(predecessor.len(), 7);
    // Six unchanged compositions still use their original exact evidence. Only
    // source V2's independently versioned composite is appended, never replaced.
    let new_source = include_str!("testdata/staging_context_carrier_v14_context_v1_source_v2.hex")
        .lines()
        .find(|line| !line.starts_with('#') && !line.is_empty())
        .unwrap();
    let expected = predecessor[..6]
        .iter()
        .copied()
        .chain([new_source])
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(actual.trim_end(), expected);
    let retired =
        crate::tests::frozen_hex_fixture(predecessor[6].split_whitespace().nth(3).unwrap());
    assert!(matches!(
        decode(&retired),
        Err(CarrierError::Context(ContextError::Abandonment(
            AbandonmentError::Source(super::super::abandonment::source::SourceError::Encoding(
                "source magic/version"
            ))
        )))
    ));
    pg.conn.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn staging_complete_carrier_round_trips_required_descriptors_into_application() {
    let clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        clock.set(2_000);
        let (_dir, pg, owner, command) = candidate::setup(target, is_append);
        let draft_checksum = command.checksum_crc64();
        let captured = candidate::capture(&pg, &owner, command, is_append);
        let context = ContextualCommand::described(captured, &topology()).unwrap();
        let encoded = context.encode_command().unwrap();
        assert_eq!(encoded.id(), context.id());
        assert_eq!(
            encoded.checksum_crc64(),
            checksum::crc64::checksum(encoded.bytes())
        );
        assert_eq!(
            encoded.command_sha256(),
            checksum::sha256::digest(encoded.bytes())
        );
        assert_ne!(encoded.checksum_crc64(), draft_checksum);
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        clock.set(9_000_000);
        let received = decode(encoded.bytes()).unwrap();
        assert_eq!(received.encode_command().unwrap().bytes(), encoded.bytes());
        assert_eq!(
            received.encode_field().unwrap(),
            context.encode_field().unwrap()
        );
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert!(matches!(
            received
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
fn staging_complete_carrier_binds_ordinary_and_source_bearing_abort_differently() {
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
        let ordinary =
            ContextualCommand::decode_field(cleanup.clone(), &[1, 0, 0], &topology()).unwrap();
        let context = ContextualCommand::abandoned_part(
            PartFinalizationSource::capture_for_test(&source)
                .unwrap()
                .bind_cleanup(cleanup.clone(), &topology())
                .unwrap(),
            &topology(),
        )
        .unwrap();
        let encoded = context.encode_command().unwrap();
        let ordinary_encoded = ordinary.encode_command().unwrap();
        assert_eq!(encoded.id(), ordinary_encoded.id());
        assert_ne!(encoded.bytes(), ordinary_encoded.bytes());
        assert_ne!(encoded.checksum_crc64(), ordinary_encoded.checksum_crc64());
        assert_ne!(encoded.command_sha256(), ordinary_encoded.command_sha256());
        // Dropping or substituting the required source is no longer invisible
        // to command integrity, even though both drafts are the same abort.
        assert!(ContextualCommand::decode_command(
            ordinary_encoded.bytes(),
            encoded.checksum_crc64(),
            &topology()
        )
        .is_err());
        let received = decode(encoded.bytes()).unwrap();
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(matches!(
            received
                .application(&topology())
                .unwrap()
                .apply_in_open_transaction(&pg, 0),
            Err(ApplicationError::Abandonment(
                AbandonmentError::NotAbandoned
            ))
        ));
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
        // Decoding is descriptive. Only durable original-source authentication
        // in the apply transaction authorizes the proof-custody handoff.
        abandonment::terminal(&pg, &source, true);
        let crossed_source =
            abandonment::source_carrier::crossed_proof_with_equal_crc(&source, "owner");
        assert_eq!(crossed_source.checksum_crc64(), source.checksum_crc64());
        let crossed = ContextualCommand::abandoned_part(
            PartFinalizationSource::capture_for_test(&crossed_source)
                .unwrap()
                .bind_cleanup(cleanup, &topology())
                .unwrap(),
            &topology(),
        )
        .unwrap();
        let crossed_encoded = crossed.encode_command().unwrap();
        assert_ne!(crossed_encoded.command_sha256(), encoded.command_sha256());
        let crossed_received = decode(crossed_encoded.bytes()).unwrap();
        let before_crossed = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        let result = crossed_received
            .application(&topology())
            .unwrap()
            .apply_in_open_transaction(&pg, 0);
        // Unlike the absent-tombstone case above, durable evidence exists but
        // binds another original SHA-256. Pin that exact conflict, not a broad
        // error or a missing-abandonment diagnostic.
        assert!(
            matches!(
                &result,
                Err(ApplicationError::Abandonment(AbandonmentError::Store(
                    crate::StoreError::MetadataCommandLogConflict { node_id: 0, pg_id, cluster_epoch, log_index }
                ))) if *pg_id == source.id().pg_id().get()
                    && *cluster_epoch == source.id().cluster_epoch()
                    && *log_index == source.id().log_index().get()
            ),
            "crossed original source was not rejected at its terminal identity: {result:?}"
        );
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before_crossed);
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
fn staging_complete_carrier_rejects_corrupt_and_resealed_invalid_commands() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target, is_append);
        let draft_len = command.command_bytes().len();
        let context = ContextualCommand::described(
            candidate::capture(&pg, &owner, command.clone(), is_append),
            &topology(),
        )
        .unwrap();
        let encoded = context.encode_command().unwrap();
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        // Every truncated prefix is rechecksummed: these exercise parsing, not
        // merely a checksum mismatch at the outer boundary.
        for end in 0..encoded.bytes().len() {
            assert!(decode(&encoded.bytes()[..end]).is_err(), "prefix {end}");
        }
        for offset in [draft_len - 1, draft_len + 2, encoded.bytes().len() - 1] {
            let mut corrupt = encoded.bytes().to_vec();
            corrupt[offset] ^= 0x80;
            assert!(matches!(
                ContextualCommand::decode_command(&corrupt, encoded.checksum_crc64(), &topology()),
                Err(CarrierError::Format(MetadataCommandFormatError::InvalidPayload(reason)))
                    if reason == "complete metadata command checksum mismatch"
            ));
        }
        let mut wrong = encoded.bytes()[..draft_len].to_vec();
        wrong.extend_from_slice(&[1, 0, 0]);
        assert!(matches!(
            decode(&wrong),
            Err(CarrierError::Context(ContextError::CommandKind))
        ));
        let mut wrong = encoded.bytes().to_vec();
        wrong.push(0);
        assert!(matches!(
            decode(&wrong),
            Err(CarrierError::Context(ContextError::Encoding(_)))
        ));
        let mut wrong = encoded.bytes().to_vec();
        wrong[draft_len] = 2;
        assert!(matches!(
            decode(&wrong),
            Err(CarrierError::Context(ContextError::Encoding(
                "unsupported context version"
            )))
        ));
        let mut wrong = encoded.bytes().to_vec();
        wrong[draft_len + 2] = 255;
        assert!(matches!(
            decode(&wrong),
            Err(CarrierError::Context(ContextError::Encoding(
                "unknown context kind"
            )))
        ));
        let mut payload = command.payload().clone();
        let crossed_key = (0..1000)
            .map(|index| ObjectKey::new(format!("crossed-carrier-key-{index}")).unwrap())
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
        let crossed = context_carrier::encode(
            &MetadataCommandEnvelope::new(command.id(), payload),
            &context.encode_field().unwrap(),
            &CommandContextCodecAuthority::new_for_test(),
        )
        .unwrap();
        assert!(matches!(
            decode(&crossed),
            Err(CarrierError::Context(ContextError::Candidate(_)))
        ));
        let foreign = PgTopology::new(&[pg.pg_id]).unwrap();
        assert!(ContextualCommand::decode_command(
            encoded.bytes(),
            encoded.checksum_crc64(),
            &foreign
        )
        .is_err());
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_complete_carrier_is_not_accepted_by_live_v13_and_has_no_v13_fallback() {
    let (draft, context) = ordinary();
    let encoded = context.encode_command().unwrap();
    assert!(matches!(
        decode_metadata_command_envelope(encoded.bytes(), &MetadataCommandDecodeAuthority::new()),
        Err(MetadataCommandFormatError::UnsupportedVersion {
            format: MetadataCommandRecordFormat::Applied,
            version: 14
        })
    ));
    assert!(matches!(
        decode(&draft.command_bytes()),
        Err(CarrierError::Format(
            MetadataCommandFormatError::UnsupportedVersion {
                format: MetadataCommandRecordFormat::Applied,
                version: 13
            }
        ))
    ));
    // The marker is a four-byte length plus 23 literal bytes, then u16 version.
    // Locate it independently rather than importing the production constant.
    assert_eq!(&encoded.bytes()[4..27], b"argmin-metadata-command");
    for version in [0_u16, 13, 15, u16::MAX] {
        let mut wrong = encoded.bytes().to_vec();
        wrong[27..29].copy_from_slice(&version.to_le_bytes());
        assert!(
            matches!(decode(&wrong), Err(CarrierError::Format(MetadataCommandFormatError::UnsupportedVersion { version: actual, .. })) if actual == version)
        );
    }
}

#[test]
fn staging_complete_carrier_counts_context_overhead_and_bounds_before_payload_decode() {
    let (draft, _) = ordinary();
    // Pure framing-capacity fixture, not a semantically valid context. Exercise
    // both field bounds independently before the owner checks the context kind.
    let max_context = vec![0; 2 * 1024 * 1024 + 3 + 22 + 7];
    let bytes = context_carrier::encode(
        &draft,
        &max_context,
        &CommandContextCodecAuthority::new_for_test(),
    )
    .unwrap();
    assert_eq!(bytes.len(), draft.command_bytes().len() + max_context.len());
    let (_, field) = context_carrier::decode(
        &bytes,
        checksum::crc64::checksum(&bytes),
        &CommandContextCodecAuthority::new_for_test(),
    )
    .unwrap();
    assert_eq!(field.len(), max_context.len());
    assert!(decode(&bytes).is_err());
    let oversized = vec![0; 2 * MAX_METADATA_COMMAND_BYTES + 3 + 22 + 7 + 1];
    assert!(
        matches!(decode(&oversized), Err(CarrierError::Format(MetadataCommandFormatError::InvalidPayload(reason))) if reason == "complete metadata command exceeds size limit")
    );
    let mut oversized_field = max_context;
    oversized_field.push(0);
    assert!(context_carrier::encode(
        &draft,
        &oversized_field,
        &CommandContextCodecAuthority::new_for_test()
    )
    .is_err());
    assert!(
        context_carrier::encode(&draft, &[], &CommandContextCodecAuthority::new_for_test())
            .is_err()
    );
}

#[test]
fn staging_complete_carrier_retains_maximum_source_without_truncating_it() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = part::initialize(&pg, 0);
    let source = part::command(&pg, &owner);
    let cleanup = abandonment::cleanup(&pg, &owner, &source);
    let MetadataCommandPayload::CommitStreamPart(mut payload) = source.payload().clone() else {
        unreachable!()
    };
    // Codec capacity only; this oversized ETag is not an S3 publication fixture.
    payload.part.etag.resize(
        payload.part.etag.len() + MAX_METADATA_COMMAND_BYTES - source.command_bytes().len(),
        0x41,
    );
    let source = MetadataCommandEnvelope::new(
        source.id(),
        MetadataCommandPayload::CommitStreamPart(payload),
    );
    assert_eq!(source.command_bytes().len(), MAX_METADATA_COMMAND_BYTES);
    // Exact complete framing cap, separately from semantic context admission.
    let max_field = vec![0; 2 * 1024 * 1024 + 32];
    let at_cap = context_carrier::encode(
        &source,
        &max_field,
        &CommandContextCodecAuthority::new_for_test(),
    )
    .unwrap();
    assert_eq!(at_cap.len(), 4 * 1024 * 1024 + 32);
    assert_eq!(
        context_carrier::decode(
            &at_cap,
            checksum::crc64::checksum(&at_cap),
            &CommandContextCodecAuthority::new_for_test()
        )
        .unwrap()
        .1,
        max_field
    );
    let MetadataCommandPayload::CommitStreamPart(mut too_large) = source.payload().clone() else {
        unreachable!()
    };
    too_large.part.etag.push(0x41);
    let too_large = MetadataCommandEnvelope::new(
        source.id(),
        MetadataCommandPayload::CommitStreamPart(too_large),
    );
    assert!(context_carrier::encode(
        &too_large,
        &[1, 0, 0],
        &CommandContextCodecAuthority::new_for_test()
    )
    .is_err());
    let context = ContextualCommand::abandoned_part(
        PartFinalizationSource::capture_for_test(&source)
            .unwrap()
            .bind_cleanup(cleanup, &topology())
            .unwrap(),
        &topology(),
    )
    .unwrap();
    let encoded = context.encode_command().unwrap();
    assert!(encoded.bytes().len() > MAX_METADATA_COMMAND_BYTES);
    let decoded = decode(encoded.bytes()).unwrap();
    assert_eq!(
        decoded.encode_field().unwrap(),
        context.encode_field().unwrap()
    );
    assert_eq!(decoded.encode_command().unwrap().bytes(), encoded.bytes());
    pg.conn.execute_batch("ROLLBACK").unwrap();
}
