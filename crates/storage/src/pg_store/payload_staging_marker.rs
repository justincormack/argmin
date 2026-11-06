// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Fresh delete-marker publication component, not production dispatch. A null
//! marker transfers the exact displaced live payload to reclaim; a new numbered
//! marker retains every existing payload and owner. Accepted replay belongs to
//! the enclosing command boundary, not a matching marker's visible fields.

use super::*;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandPayload,
    INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
};
use crate::node_runtime::pg_store::PgStore;
use crate::node_runtime::traits::PgMetadataStore;
use crate::{MetadataError, StoredObject};

#[derive(Debug, thiserror::Error)]
pub(in super::super) enum MarkerError {
    #[error("staging delete marker requires the enclosing command transaction")]
    TransactionRequired,
    #[error("staging delete marker does not bind the exact command and authority")]
    CommandBinding,
    #[error("staging delete marker does not match its durable target")]
    Target,
    #[error(transparent)]
    Reclaim(#[from] object::ObjectError),
    #[error(transparent)]
    Invariant(#[from] StagingInvariant),
    #[error(transparent)]
    Persistence(#[from] sql::PersistenceError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error(transparent)]
    Database(#[from] rusqlite::Error),
}

pub(in super::super) fn apply_in_open_transaction(
    pg: &PgStore,
    topology: &PgTopology,
    envelope: &MetadataCommandEnvelope,
) -> Result<(), MarkerError> {
    if pg.conn.is_autocommit() {
        return Err(MarkerError::TransactionRequired);
    }
    let MetadataCommandPayload::InsertDeleteMarker(command) = envelope.payload() else {
        return Err(MarkerError::CommandBinding);
    };
    let proof = &command.bucket_write_reservation;
    if !envelope.verify_checksum()
        || envelope.id().pg_id().get() != pg.pg_id
        || topology.object_pg_for(&command.bucket, &command.key) != pg.pg_id
        || proof.bucket_incarnation_generation == 0
        || !proof.matches_exact_mutation_subject(
            envelope.id().cluster_epoch(),
            &command.bucket,
            INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
            Some(command.key.as_str()),
        )
    {
        return Err(MarkerError::CommandBinding);
    }
    // The builder captures (but does not reserve) this sequence. Fresh apply
    // cannot reuse an existing marker as an inferred replay certificate or
    // publish a stale ordering behind a later object. No clock is consulted.
    if command.write_sequence
        != pg.next_object_write_sequence(command.bucket.as_str(), command.key.as_str())?
    {
        return Err(MarkerError::Target);
    }
    let transfers = match pg.get_object_version(&command.bucket, &command.key, command.version_id) {
        Ok(StoredObject::Live(record)) if command.version_id.is_null() => {
            object::retained_for_reclaim(
                pg,
                topology,
                &record,
                proof.bucket_incarnation_generation,
                command.stale_payload.as_ref().ok_or(MarkerError::Target)?,
            )?
        }
        Ok(StoredObject::DeleteMarker(_))
            if command.version_id.is_null() && command.stale_payload.is_none() =>
        {
            Vec::new()
        }
        Err(MetadataError::ObjectNotFound) if command.stale_payload.is_none() => Vec::new(),
        Ok(_) | Err(MetadataError::ObjectNotFound) => return Err(MarkerError::Target),
        Err(error) => return Err(error.into()),
    };
    let checked = transfers
        .iter()
        .map(|(before, after)| {
            if !command_follows(before.subject.id.introduction, envelope.id()) {
                return Err(MarkerError::CommandBinding);
            }
            Ok((before.validate(topology)?, after.validate(topology)?))
        })
        .collect::<Result<Vec<_>, MarkerError>>()?;
    let savepoint = sql::CommandApplySavepoint::begin(&pg.conn)?;
    pg.apply_insert_delete_marker_command(command)?;
    for (before, after) in &checked {
        sql::retain_object_reclaim(&pg.conn, PgId::new(pg.pg_id), topology, before, after)?;
    }
    savepoint.commit()?;
    Ok(())
}
