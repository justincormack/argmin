// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Fresh object-version deletion component, not production dispatch. Transfer
//! original ownership to reclaim before deleting the live metadata in the same
//! object-PG transaction. Missing objects or surviving reclaim roots are not
//! replay certificates: exact accepted replay belongs to the enclosing boundary.

use super::*;
use crate::metadata_command::{
    DeleteObjectVersionMode, DeleteObjectVersionTarget, MetadataCommandEnvelope,
    MetadataCommandPayload, DELETE_CURRENT_OBJECT_BUCKET_WRITE_OPERATION_KIND,
    DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
};
use crate::node_runtime::pg_store::PgStore;
use crate::node_runtime::traits::PgMetadataStore;
use crate::{MetadataError, StoredObject};

#[derive(Debug, thiserror::Error)]
pub(in super::super) enum DeleteError {
    #[error("staging object deletion requires the enclosing command transaction")]
    TransactionRequired,
    #[error("staging object deletion does not bind the exact command and authority")]
    CommandBinding,
    #[error("staging object deletion does not match its durable target")]
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
) -> Result<(), DeleteError> {
    if pg.conn.is_autocommit() {
        return Err(DeleteError::TransactionRequired);
    }
    let MetadataCommandPayload::DeleteObjectVersion(command) = envelope.payload() else {
        return Err(DeleteError::CommandBinding);
    };
    let proof = &command.bucket_write_reservation;
    let operation = match command.mode {
        DeleteObjectVersionMode::Current => DELETE_CURRENT_OBJECT_BUCKET_WRITE_OPERATION_KIND,
        DeleteObjectVersionMode::Specific => DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
    };
    if !envelope.verify_checksum()
        || envelope.id().pg_id().get() != pg.pg_id
        || topology.object_pg_for(&command.bucket, &command.key) != pg.pg_id
        || proof.bucket_incarnation_generation == 0
        || !proof.matches_exact_mutation_subject(
            envelope.id().cluster_epoch(),
            &command.bucket,
            operation,
            Some(command.key.as_str()),
        )
    {
        return Err(DeleteError::CommandBinding);
    }
    let stored = pg.get_object_version(&command.bucket, &command.key, command.version_id)?;
    if command.mode == DeleteObjectVersionMode::Current
        && (!matches!(stored, StoredObject::Live(_))
            || pg.get_object_meta(&command.bucket, &command.key)? != stored)
    {
        return Err(DeleteError::Target);
    }
    let transfers = match (&command.target, &stored) {
        (
            DeleteObjectVersionTarget::Live {
                generation_id,
                layout,
                payload,
            },
            StoredObject::Live(record),
        ) if *generation_id == record.generation_id && *layout == record.layout => {
            object::retained_for_reclaim(
                pg,
                topology,
                record,
                proof.bucket_incarnation_generation,
                payload,
            )?
        }
        (
            DeleteObjectVersionTarget::DeleteMarker { write_sequence },
            StoredObject::DeleteMarker(_),
        ) if command.mode == DeleteObjectVersionMode::Specific
            && pg.object_write_sequence(
                command.bucket.as_str(),
                command.key.as_str(),
                command.version_id,
            )? == Some(*write_sequence) =>
        {
            Vec::new()
        }
        _ => return Err(DeleteError::Target),
    };
    let checked = transfers
        .iter()
        .map(|(before, after)| {
            if !command_follows(before.subject.id.introduction, envelope.id()) {
                return Err(DeleteError::CommandBinding);
            }
            Ok((before.validate(topology)?, after.validate(topology)?))
        })
        .collect::<Result<Vec<_>, DeleteError>>()?;
    let savepoint = sql::CommandApplySavepoint::begin(&pg.conn)?;
    pg.apply_delete_object_version_command(command)?;
    for (before, after) in &checked {
        sql::retain_object_reclaim(&pg.conn, PgId::new(pg.pg_id), topology, before, after)?;
    }
    savepoint.commit()?;
    Ok(())
}
