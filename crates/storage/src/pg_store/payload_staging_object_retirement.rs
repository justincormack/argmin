// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Read-only custody check inside the exact slot-removal transaction. This is
//! not proof of application, bucket release, physical deletion or owner retirement.
//! The enclosing boundary authenticates Applied evidence and the complete slot.
use super::*;

#[path = "payload_staging_displaced_retirement.rs"]
mod displaced;

pub(in super::super) fn require_direct_retirement(
    pg: &PgStore,
    topology: &PgTopology,
    envelope: &MetadataCommandEnvelope,
) -> Result<(), ObjectError> {
    if pg.conn.is_autocommit() {
        return Err(ObjectError::TransactionRequired);
    }
    let MetadataCommandPayload::CommitDirectPutObject(command) = envelope.payload() else {
        return Err(ObjectError::CommandBinding);
    };
    if command.bucket_write_reservation.operation_kind
        != PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND
        || !envelope.verify_checksum()
        || envelope.id().pg_id().get() != pg.pg_id
    {
        return Err(ObjectError::CommandBinding);
    }
    let owner = sql::load(
        &pg.conn,
        PgId::new(pg.pg_id),
        &command.generation_reservation_id,
    )?
    .ok_or(ObjectError::LiveResponsibility)?;
    owner.validate(topology)?;
    let subject = &owner.subject;
    let ResourceResponsibility::ReleaseDue(creation) = &owner.resources.creation else {
        return Err(ObjectError::LiveResponsibility);
    };
    if subject.target != StagingTarget::DirectPut
        || subject.bucket != command.object.bucket
        || subject.key != command.object.key
        || subject.payload_generation != command.object.generation_id
        || subject.object_pg != envelope.id().pg_id()
        || !command_follows(subject.id.introduction, envelope.id())
        || command.object.layout != ObjectLayout::Standard
        || owner.windows.len() != 1
        || command.segments.len() != 1
        || owner.windows[0].geometry.ec != command.object.ec
        || command.segments[0].size != command.object.size
        || StagingBucketResource::capture(
            &command.bucket_write_reservation,
            PgId::new(topology.bucket_pg_for(&subject.bucket)),
        )
        .identity
            != creation.identity
    {
        return Err(ObjectError::CommandBinding);
    }
    let high_water = pg.conn.query_row(
        "SELECT max_committed_generation FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
        rusqlite::params![subject.bucket, subject.key], |row| row.get::<_, Option<u64>>(0),
    ).optional()?.flatten();
    subject.require_generation_high_water(high_water)?;
    if let Some(reclaim) = &command.stale_payload {
        displaced::require_displacement(pg, topology, envelope, command, reclaim)?;
    }
    let mut expected = owner.selected_object_reference(command, 0, &command.segments[0])?;
    // An absence retry can follow another command's Object -> Reclaim ->
    // unreferenced transition. Never mistake that for loss of this owner: the
    // immutable window and exact release responsibilities must still survive.
    match owner.lifecycle {
        StagingLifecycle::Published => {}
        StagingLifecycle::Retiring(StagingRetirement::PublishedRetained) => {
            // Retirement can start while the selected Object reference is
            // still live; later displacement changes only its destination.
            if matches!(
                owner.references[0].destination,
                StagingDestination::Reclaim { .. }
            ) {
                expected.destination = StagingDestination::Reclaim {
                    generation: subject.payload_generation,
                    part_number: None,
                };
            }
        }
        StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced) => {
            // Structural validation requires no references in this state.
            return Ok(());
        }
        _ => return Err(ObjectError::LiveResponsibility),
    }
    if owner.references.as_slice() != [expected] {
        return Err(ObjectError::SelectedPrefix);
    }
    Ok(())
}
