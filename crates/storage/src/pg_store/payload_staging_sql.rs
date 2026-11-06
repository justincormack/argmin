// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Storage-owned relational representation included in the PG schema and
//! canonical/checkpoint inventory. Command integration remains incomplete;
//! these records alone do not transfer frontend custody or authorize cleanup.

use super::*;
use crate::metadata_command::MetadataCommandLogIndex;
use rusqlite::{params, types::FromSql, types::ValueRef, Connection, OptionalExtension, Row};

use super::super::command_log::{MetadataCheckpointTableBlock, MetadataCheckpointValue};

/// Both exact-route enumeration and its summary use this projection. Retained
/// ownership survives sessions, manifests and reservations; neither lifecycle,
/// selected-reference presence nor an expired cutoff permits pruning its routes.
/// File and acknowledgement participants share the exact data-PG route, even
/// when the acknowledgement primary holds no file. Release PG/epoch come from
/// the retained proof, not the owner PG or a recomputed current placement.
pub(in super::super) const ROUTE_REFERENCES: &str = "\
    SELECT object_pg AS pg_id, originating_epoch AS cluster_epoch FROM payload_staging_owners \
    UNION SELECT data_pg, placement_epoch FROM payload_staging_windows \
    UNION SELECT release_pg, cluster_epoch FROM payload_staging_resources";

#[derive(Debug, thiserror::Error)]
pub(super) enum PersistenceError {
    #[error("staging persistence requires the enclosing metadata transaction")]
    TransactionRequired,
    #[error("staging owner belongs to another object PG")]
    WrongPg,
    #[error("staging owner identity already exists")]
    OwnerExists,
    #[error("staging append does not extend the exact retained prefix")]
    AppendConflict,
    #[error("staging closure does not preserve the exact unpublished owner")]
    CloseConflict,
    #[error("staging part publication does not preserve the exact owner transition")]
    PartConflict,
    #[error("committed part must have exactly one retained staging owner")]
    PartOwner,
    #[error("staging object publication does not preserve the exact owner transition")]
    ObjectConflict,
    #[error("standard object must have exactly one retained staging owner")]
    ObjectOwner,
    #[error("staging multipart transfer does not preserve the complete owner transitions")]
    MultipartTransferConflict,
    #[error("malformed staging row: {0}")]
    Row(&'static str),
    #[error(transparent)]
    Invariant(#[from] StagingInvariant),
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
}

// No FK points at a session, upload, object, reclaim or reservation. Deleting
// any of those must not implicitly destroy retained cleanup responsibility.
// Only owner-owned children cascade. The generation obligation has the owner's
// exact attempt/generation by construction, so only its state needs a column.
pub(in super::super) const SCHEMA: &str = r"
CREATE TABLE payload_staging_owners (
    attempt TEXT PRIMARY KEY NOT NULL,
    introduction_epoch INTEGER NOT NULL CHECK (introduction_epoch > 0),
    introduction_pg INTEGER NOT NULL CHECK (introduction_pg BETWEEN 0 AND 4294967295),
    introduction_index INTEGER NOT NULL CHECK (introduction_index > 0),
    bucket TEXT NOT NULL,
    incarnation INTEGER NOT NULL CHECK (incarnation > 0),
    key TEXT NOT NULL,
    object_pg INTEGER NOT NULL CHECK (object_pg BETWEEN 0 AND 4294967295),
    originating_epoch INTEGER NOT NULL CHECK (originating_epoch > 0),
    payload_generation INTEGER NOT NULL CHECK (payload_generation > 0),
    target INTEGER NOT NULL CHECK (target IN (0, 1, 2)),
    upload_id TEXT,
    upload_generation INTEGER,
    part_number INTEGER,
    issuer_node INTEGER NOT NULL CHECK (issuer_node BETWEEN 0 AND 4294967295),
    issuer_lifetime BLOB NOT NULL CHECK (length(issuer_lifetime) = 16),
    lifecycle INTEGER NOT NULL CHECK (lifecycle BETWEEN 0 AND 4),
    reserved_end INTEGER NOT NULL CHECK (reserved_end > 0),
    maximum_cutoff_ms INTEGER NOT NULL CHECK (maximum_cutoff_ms > 0),
    generation_state INTEGER NOT NULL CHECK (generation_state IN (1, 2)),
    CHECK ((target IN (0, 1) AND upload_id IS NULL AND upload_generation IS NULL AND part_number IS NULL)
        OR (target = 2 AND upload_id IS NOT NULL AND upload_generation IS NOT NULL
            AND upload_generation > 0 AND part_number IS NOT NULL AND part_number BETWEEN 1 AND 10000))
) STRICT;
CREATE TABLE payload_staging_windows (
    attempt TEXT NOT NULL REFERENCES payload_staging_owners(attempt) ON DELETE CASCADE,
    window_index INTEGER NOT NULL CHECK (window_index BETWEEN 0 AND 640),
    segment_hash BLOB NOT NULL CHECK (length(segment_hash) = 16),
    start_vid INTEGER NOT NULL CHECK (start_vid > 0),
    end_vid INTEGER NOT NULL CHECK (end_vid > start_vid),
    issued_at_ms INTEGER NOT NULL CHECK (issued_at_ms >= 0),
    cutoff_ms INTEGER NOT NULL CHECK (cutoff_ms > issued_at_ms),
    codec INTEGER NOT NULL,
    ec_k INTEGER NOT NULL CHECK (ec_k BETWEEN 1 AND 32),
    ec_m INTEGER NOT NULL CHECK (ec_m BETWEEN 0 AND 31),
    logical_bound INTEGER NOT NULL CHECK (logical_bound >= 0),
    stored_bound INTEGER NOT NULL CHECK (stored_bound > 0),
    shard_bound INTEGER NOT NULL CHECK (shard_bound > 0),
    data_pg INTEGER NOT NULL CHECK (data_pg BETWEEN 0 AND 4294967295),
    placement_epoch INTEGER NOT NULL CHECK (placement_epoch > 0),
    acknowledgement_primary INTEGER NOT NULL CHECK (acknowledgement_primary BETWEEN 0 AND 4294967295),
    PRIMARY KEY (attempt, window_index)
) STRICT;
CREATE TABLE payload_staging_participants (
    attempt TEXT NOT NULL,
    window_index INTEGER NOT NULL,
    shard_index INTEGER NOT NULL CHECK (shard_index BETWEEN 0 AND 31),
    node_id INTEGER NOT NULL CHECK (node_id BETWEEN 0 AND 4294967295),
    PRIMARY KEY (attempt, window_index, shard_index),
    UNIQUE (attempt, window_index, node_id),
    FOREIGN KEY (attempt, window_index) REFERENCES payload_staging_windows(attempt, window_index) ON DELETE CASCADE
) STRICT;
CREATE TABLE payload_staging_references (
    attempt TEXT NOT NULL,
    window_index INTEGER NOT NULL,
    segment_hash BLOB NOT NULL CHECK (length(segment_hash) = 16),
    physical_vid INTEGER NOT NULL CHECK (physical_vid > 0),
    logical_size INTEGER NOT NULL CHECK (logical_size >= 0),
    stored_size INTEGER NOT NULL CHECK (stored_size > 0),
    destination INTEGER NOT NULL CHECK (destination BETWEEN 0 AND 3),
    upload_id TEXT,
    generation INTEGER,
    part_number INTEGER,
    PRIMARY KEY (attempt, window_index),
    FOREIGN KEY (attempt, window_index) REFERENCES payload_staging_windows(attempt, window_index) ON DELETE CASCADE,
    CHECK ((destination = 0 AND upload_id IS NULL AND generation IS NULL AND part_number IS NULL)
        OR (destination = 1 AND upload_id IS NOT NULL AND generation IS NOT NULL AND generation > 0
            AND part_number IS NOT NULL AND part_number BETWEEN 1 AND 10000)
        OR (destination IN (2, 3) AND upload_id IS NULL AND generation IS NOT NULL AND generation > 0
            AND (part_number IS NULL OR part_number BETWEEN 1 AND 10000)))
) STRICT;
CREATE TABLE payload_staging_resources (
    attempt TEXT NOT NULL REFERENCES payload_staging_owners(attempt) ON DELETE CASCADE,
    role INTEGER NOT NULL CHECK (role BETWEEN 0 AND 4294967295),
    state INTEGER NOT NULL CHECK (state IN (1, 2) AND (role < 2 OR state = 2)),
    bucket TEXT NOT NULL,
    reservation_id TEXT NOT NULL CHECK (length(reservation_id) > 0),
    owner_token TEXT NOT NULL CHECK (length(owner_token) > 0),
    cluster_epoch INTEGER NOT NULL CHECK (cluster_epoch > 0),
    execution_generation INTEGER NOT NULL CHECK (execution_generation > 0),
    incarnation INTEGER NOT NULL CHECK (incarnation > 0),
    operation_kind TEXT NOT NULL,
    created_at INTEGER NOT NULL CHECK (created_at >= 0),
    target_context TEXT,
    observed_lease_deadline INTEGER NOT NULL CHECK (observed_lease_deadline > created_at),
    release_pg INTEGER NOT NULL CHECK (release_pg BETWEEN 0 AND 4294967295),
    PRIMARY KEY (attempt, role)
) STRICT;
";

// One column inventory feeds SQL projection/insertion and the live
// canonical/checkpoint table registry.
pub(in super::super) const TABLES: &[super::super::command_log::MetadataDigestTable] = &[
    super::super::command_log::MetadataDigestTable {
        name: "payload_staging_owners",
        columns: &[
            "attempt",
            "introduction_epoch",
            "introduction_pg",
            "introduction_index",
            "bucket",
            "incarnation",
            "key",
            "object_pg",
            "originating_epoch",
            "payload_generation",
            "target",
            "upload_id",
            "upload_generation",
            "part_number",
            "issuer_node",
            "issuer_lifetime",
            "lifecycle",
            "reserved_end",
            "maximum_cutoff_ms",
            "generation_state",
        ],
        order_columns: &["attempt"],
        filter: super::super::command_log::MetadataDigestFilter::AllRows,
    },
    super::super::command_log::MetadataDigestTable {
        name: "payload_staging_windows",
        columns: &[
            "attempt",
            "window_index",
            "segment_hash",
            "start_vid",
            "end_vid",
            "issued_at_ms",
            "cutoff_ms",
            "codec",
            "ec_k",
            "ec_m",
            "logical_bound",
            "stored_bound",
            "shard_bound",
            "data_pg",
            "placement_epoch",
            "acknowledgement_primary",
        ],
        order_columns: &["attempt", "window_index"],
        filter: super::super::command_log::MetadataDigestFilter::AllRows,
    },
    super::super::command_log::MetadataDigestTable {
        name: "payload_staging_participants",
        columns: &["attempt", "window_index", "shard_index", "node_id"],
        order_columns: &["attempt", "window_index", "shard_index"],
        filter: super::super::command_log::MetadataDigestFilter::AllRows,
    },
    super::super::command_log::MetadataDigestTable {
        name: "payload_staging_references",
        columns: &[
            "attempt",
            "window_index",
            "segment_hash",
            "physical_vid",
            "logical_size",
            "stored_size",
            "destination",
            "upload_id",
            "generation",
            "part_number",
        ],
        order_columns: &["attempt", "window_index"],
        filter: super::super::command_log::MetadataDigestFilter::AllRows,
    },
    super::super::command_log::MetadataDigestTable {
        name: "payload_staging_resources",
        columns: &[
            "attempt",
            "role",
            "state",
            "bucket",
            "reservation_id",
            "owner_token",
            "cluster_epoch",
            "execution_generation",
            "incarnation",
            "operation_kind",
            "created_at",
            "target_context",
            "observed_lease_deadline",
            "release_pg",
        ],
        order_columns: &["attempt", "role"],
        filter: super::super::command_log::MetadataDigestFilter::AllRows,
    },
];

fn insert_sql(table: usize) -> String {
    let table = &TABLES[table];
    let placeholders = (1..=table.columns.len())
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {} ({}) VALUES ({placeholders})",
        table.name,
        table.columns.join(", ")
    )
}

fn select_sql(table: usize) -> String {
    let table = &TABLES[table];
    format!(
        "SELECT {} FROM {} WHERE attempt = ?1 ORDER BY {}",
        table.columns.join(", "),
        table.name,
        table.order_columns.join(", ")
    )
}

fn lifecycle_tag(lifecycle: StagingLifecycle) -> u8 {
    match lifecycle {
        StagingLifecycle::Active => 0,
        StagingLifecycle::Published => 1,
        StagingLifecycle::Retiring(StagingRetirement::Unpublished) => 2,
        StagingLifecycle::Retiring(StagingRetirement::PublishedRetained) => 3,
        StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced) => 4,
    }
}

fn responsibility_tag<T>(resource: &ResourceResponsibility<T>) -> u8 {
    match resource {
        ResourceResponsibility::NoResource => 0,
        ResourceResponsibility::Live(_) => 1,
        ResourceResponsibility::ReleaseDue(_) => 2,
    }
}

/// Only an already validated record can enter SQL. The savepoint is nested in
/// the existing metadata transaction: RELEASE does not commit/fsync that outer
/// transaction. A failed child write rolls back this insertion, not unrelated
/// work in the caller's transaction. Exact-session/command replay is a separate
/// enclosing apply decision; an existing attempt never authorizes adoption.
/// `configured_topology` must come from the owning node configuration, not from
/// the record. Compare its complete canonical PG set before any SQL access.
pub(super) fn insert(
    conn: &Connection,
    pg_id: PgId,
    configured_topology: &PgTopology,
    owner: &CheckedStagingOwner<'_>,
) -> Result<(), PersistenceError> {
    if conn.is_autocommit() {
        return Err(PersistenceError::TransactionRequired);
    }
    let record = owner.record_for_topology(configured_topology)?;
    if record.subject.object_pg != pg_id {
        return Err(PersistenceError::WrongPg);
    }
    let exists = conn
        .query_row(
            "SELECT 1 FROM payload_staging_owners WHERE attempt = ?1",
            [&record.subject.id.attempt],
            |_| Ok(()),
        )
        .optional()?;
    if exists.is_some() {
        return Err(PersistenceError::OwnerExists);
    }
    let savepoint = InsertionSavepoint::begin(conn)?;
    insert_rows(conn, record)?;
    savepoint.commit()?;
    Ok(())
}

struct InsertionSavepoint<'a> {
    conn: &'a Connection,
    active: bool,
}

/// The live metadata mutation and its ownership update must roll back together,
/// even if the caller catches the error and commits unrelated outer work.
pub(super) struct CommandApplySavepoint<'a>(&'a Connection, bool);

impl<'a> CommandApplySavepoint<'a> {
    pub(super) fn begin(conn: &'a Connection) -> rusqlite::Result<Self> {
        conn.execute_batch("SAVEPOINT apply_payload_staging_command")?;
        Ok(Self(conn, true))
    }

    pub(super) fn commit(mut self) -> rusqlite::Result<()> {
        self.0
            .execute_batch("RELEASE apply_payload_staging_command")?;
        self.1 = false;
        Ok(())
    }
}

impl Drop for CommandApplySavepoint<'_> {
    fn drop(&mut self) {
        if self.1 {
            let _ = self.0.execute_batch(
                "ROLLBACK TO apply_payload_staging_command; RELEASE apply_payload_staging_command",
            );
        }
    }
}

/// Incremental ownership half of append, inside the command's transaction.
/// Earlier descriptors/references and all resource obligations are immutable.
pub(super) fn append_selection(
    conn: &Connection,
    pg_id: PgId,
    configured_topology: &PgTopology,
    before: &CheckedStagingOwner<'_>,
    after: &CheckedStagingOwner<'_>,
) -> Result<(), PersistenceError> {
    if conn.is_autocommit() {
        return Err(PersistenceError::TransactionRequired);
    }
    let before = before.record_for_topology(configured_topology)?;
    let after = after.record_for_topology(configured_topology)?;
    if before.subject.object_pg != pg_id || after.subject.object_pg != pg_id {
        return Err(PersistenceError::WrongPg);
    }
    if before.lifecycle != StagingLifecycle::Active
        || after.lifecycle != StagingLifecycle::Active
        || matches!(before.subject.target, StagingTarget::DirectPut)
        || before.subject != after.subject
        || before.resources != after.resources
        || after.windows.len() != before.windows.len() + 1
        || after.references.len() != before.references.len() + 1
        || !after.windows.starts_with(&before.windows)
        || !after.references.starts_with(&before.references)
        || load(conn, pg_id, &before.subject.id.attempt)?.as_ref() != Some(before)
    {
        return Err(PersistenceError::AppendConflict);
    }
    let savepoint = InsertionSavepoint::begin(conn)?;
    insert_window(conn, &after.windows[before.windows.len()])?;
    insert_reference(
        conn,
        &after.subject.id.attempt,
        &after.references[before.references.len()],
    )?;
    conn.execute(
        "UPDATE payload_staging_owners SET reserved_end = ?1, maximum_cutoff_ms = ?2 WHERE attempt = ?3",
        params![after.reserved_end.get(), after.maximum_cutoff_ms, after.subject.id.attempt],
    )?;
    savepoint.commit()?;
    Ok(())
}

/// The ownership half of ordinary release/abort. Only selected stream bindings
/// disappear; every issued window, participant and exact resource survives.
pub(super) fn close_unpublished(
    conn: &Connection,
    pg_id: PgId,
    configured_topology: &PgTopology,
    before: &CheckedStagingOwner<'_>,
    after: &CheckedStagingOwner<'_>,
) -> Result<(), PersistenceError> {
    if conn.is_autocommit() {
        return Err(PersistenceError::TransactionRequired);
    }
    let before = before.record_for_topology(configured_topology)?;
    let after = after.record_for_topology(configured_topology)?;
    if before.subject.object_pg != pg_id || after.subject.object_pg != pg_id {
        return Err(PersistenceError::WrongPg);
    }
    if before.closed_unpublished()? != *after
        || load(conn, pg_id, &before.subject.id.attempt)?.as_ref() != Some(before)
    {
        return Err(PersistenceError::CloseConflict);
    }
    let savepoint = InsertionSavepoint::begin(conn)?;
    conn.execute(
        "DELETE FROM payload_staging_references WHERE attempt = ?1",
        [&before.subject.id.attempt],
    )?;
    conn.execute(
        "UPDATE payload_staging_resources SET state = 2 WHERE attempt = ?1 AND state = 1",
        [&before.subject.id.attempt],
    )?;
    conn.execute(
        "UPDATE payload_staging_owners SET lifecycle = ?1, generation_state = 2 WHERE attempt = ?2",
        params![lifecycle_tag(after.lifecycle), before.subject.id.attempt],
    )?;
    savepoint.commit()?;
    Ok(())
}

/// The abandonment follow-up's outer savepoint also encloses ordinary session
/// closure. The tombstone itself never mutates materialized ownership.
pub(super) fn retain_abandoned_part_finalization(
    conn: &Connection,
    pg_id: PgId,
    topology: &PgTopology,
    closed: &CheckedStagingOwner<'_>,
    retained: &CheckedStagingOwner<'_>,
    finalization: &StagingBucketResource,
) -> Result<(), PersistenceError> {
    if conn.is_autocommit() {
        return Err(PersistenceError::TransactionRequired);
    }
    let closed = closed.record_for_topology(topology)?;
    let retained = retained.record_for_topology(topology)?;
    if closed.subject.object_pg != pg_id || retained.subject.object_pg != pg_id {
        return Err(PersistenceError::WrongPg);
    }
    if closed.with_abandoned_part_finalization(finalization)? != *retained
        || load(conn, pg_id, &closed.subject.id.attempt)?.as_ref() != Some(closed)
    {
        return Err(PersistenceError::CloseConflict);
    }
    // Slots 0/1 retain the creation/original terminal roles. Additional
    // abandoned obligations use contiguous slots 2.. in certified follow-up
    // order. No existing proof or renewable evidence is rewritten.
    let role = if closed.resources.finalization == ResourceResponsibility::NoResource {
        1
    } else {
        closed.resources.abandoned_finalizations.len() + 2
    };
    let savepoint = CommandApplySavepoint::begin(conn)?;
    insert_resource(
        conn,
        &closed.subject.id.attempt,
        role,
        &ResourceResponsibility::ReleaseDue(finalization.clone()),
    )?;
    savepoint.commit()?;
    Ok(())
}

impl<'a> InsertionSavepoint<'a> {
    fn begin(conn: &'a Connection) -> Result<Self, rusqlite::Error> {
        conn.execute_batch("SAVEPOINT insert_payload_staging_owner")?;
        Ok(Self { conn, active: true })
    }

    fn commit(mut self) -> Result<(), rusqlite::Error> {
        self.conn
            .execute_batch("RELEASE insert_payload_staging_owner")?;
        self.active = false;
        Ok(())
    }
}

impl Drop for InsertionSavepoint<'_> {
    fn drop(&mut self) {
        if self.active {
            let _ = self.conn.execute_batch(
                "ROLLBACK TO insert_payload_staging_owner; RELEASE insert_payload_staging_owner",
            );
        }
    }
}

fn insert_rows(conn: &Connection, owner: &StagingOwnerRecord) -> Result<(), PersistenceError> {
    let subject = &owner.subject;
    let attempt = &subject.id.attempt;
    let (target, upload_id, upload_generation, part_number) = match &subject.target {
        StagingTarget::DirectPut => (0, None, None, None),
        StagingTarget::StreamPut => (1, None, None, None),
        StagingTarget::UploadPart {
            upload_id,
            upload_generation,
            part_number,
        } => (
            2,
            Some(upload_id),
            Some(upload_generation.get()),
            Some(*part_number),
        ),
    };
    conn.execute(
        &insert_sql(0),
        params![
            attempt,
            subject.id.introduction.cluster_epoch().get(),
            subject.id.introduction.pg_id().get(),
            subject.id.introduction.log_index().get(),
            subject.bucket,
            subject.bucket_incarnation,
            subject.key,
            subject.object_pg.get(),
            subject.originating_epoch.get(),
            subject.payload_generation.get(),
            target,
            upload_id,
            upload_generation,
            part_number,
            subject.issuer.node_id.as_u32(),
            subject.issuer.lifetime.as_slice(),
            lifecycle_tag(owner.lifecycle),
            owner.reserved_end.get(),
            owner.maximum_cutoff_ms,
            responsibility_tag(&owner.resources.generation),
        ],
    )?;
    for window in &owner.windows {
        insert_window(conn, window)?;
    }
    for reference in &owner.references {
        insert_reference(conn, attempt, reference)?;
    }
    for (role, resource) in [&owner.resources.creation, &owner.resources.finalization]
        .into_iter()
        .enumerate()
    {
        insert_resource(conn, attempt, role, resource)?;
    }
    for (index, proof) in owner.resources.abandoned_finalizations.iter().enumerate() {
        insert_resource(
            conn,
            attempt,
            index + 2,
            &ResourceResponsibility::ReleaseDue(proof.clone()),
        )?;
    }
    Ok(())
}

fn insert_resource(
    conn: &Connection,
    attempt: &SessionId,
    role: usize,
    resource: &ResourceResponsibility<StagingBucketResource>,
) -> Result<(), PersistenceError> {
    if let ResourceResponsibility::Live(proof) | ResourceResponsibility::ReleaseDue(proof) =
        resource
    {
        let identity = &proof.identity;
        conn.prepare_cached(&insert_sql(4))?.execute(params![
            attempt,
            role,
            responsibility_tag(resource),
            identity.bucket,
            identity.reservation_id,
            identity.owner_token,
            identity.cluster_epoch.get(),
            identity.execution_generation,
            identity.incarnation,
            identity.operation_kind,
            identity.created_at,
            identity.target_context,
            proof.observed_lease_deadline,
            proof.release_pg.get(),
        ])?;
    }
    Ok(())
}

/// Resolve original ownership by the complete logical part generation, never by
/// interpreting a shard hash. Zero-segment parts require this same lookup.
pub(super) fn owner_for_part(
    conn: &Connection,
    pg_id: PgId,
    bucket: &BucketName,
    key: &ObjectKey,
    part: &crate::types::MultipartPartRecord,
) -> Result<StagingOwnerRecord, PersistenceError> {
    if conn.is_autocommit() {
        return Err(PersistenceError::TransactionRequired);
    }
    let mut statement = conn.prepare_cached(
        "SELECT attempt FROM payload_staging_owners WHERE bucket = ?1 AND key = ?2 \
         AND target = 2 AND upload_id = ?3 AND part_number = ?4 AND payload_generation = ?5 LIMIT 2",
    )?;
    let attempts = statement
        .query_map(
            params![
                bucket,
                key,
                part.upload_id,
                part.part_number,
                part.part_vid.get()
            ],
            |row| row.get::<_, SessionId>(0),
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let [attempt] = attempts.as_slice() else {
        return Err(PersistenceError::PartOwner);
    };
    load(conn, pg_id, attempt)?.ok_or(PersistenceError::PartOwner)
}

/// Both the new publication and the displaced part's retained responsibility
/// are one SQL transition. The caller's outer savepoint also encloses part and
/// session mutations. No window, participant or creation proof is rewritten.
pub(super) fn publish_part(
    conn: &Connection,
    pg_id: PgId,
    configured_topology: &PgTopology,
    before: &CheckedStagingOwner<'_>,
    after: &CheckedStagingOwner<'_>,
    displaced: Option<(&CheckedStagingOwner<'_>, &CheckedStagingOwner<'_>)>,
) -> Result<(), PersistenceError> {
    if conn.is_autocommit() {
        return Err(PersistenceError::TransactionRequired);
    }
    let before = before.record_for_topology(configured_topology)?;
    let after = after.record_for_topology(configured_topology)?;
    let ResourceResponsibility::ReleaseDue(finalization) = &after.resources.finalization else {
        return Err(PersistenceError::PartConflict);
    };
    if before.subject.object_pg != pg_id || after.subject.object_pg != pg_id {
        return Err(PersistenceError::WrongPg);
    }
    if before.published_part(finalization)? != *after
        || load(conn, pg_id, &before.subject.id.attempt)?.as_ref() != Some(before)
    {
        return Err(PersistenceError::PartConflict);
    }
    let displaced = displaced
        .map(|(old, retired)| -> Result<_, PersistenceError> {
            let old = old.record_for_topology(configured_topology)?;
            let retired = retired.record_for_topology(configured_topology)?;
            if old.subject.object_pg != pg_id || retired.subject.object_pg != pg_id {
                return Err(PersistenceError::WrongPg);
            }
            if old.subject.id == before.subject.id
                || old.subject.payload_generation == before.subject.payload_generation
                || old.subject.bucket != before.subject.bucket
                || old.subject.key != before.subject.key
                || old.subject.bucket_incarnation != before.subject.bucket_incarnation
                || old.subject.target != before.subject.target
                || old.unreferenced_part()? != *retired
                || load(conn, pg_id, &old.subject.id.attempt)?.as_ref() != Some(old)
            {
                return Err(PersistenceError::PartConflict);
            }
            Ok((old, retired))
        })
        .transpose()?;
    let StagingTarget::UploadPart {
        upload_id,
        part_number,
        ..
    } = &after.subject.target
    else {
        return Err(PersistenceError::PartConflict);
    };
    let savepoint = InsertionSavepoint::begin(conn)?;
    insert_resource(
        conn,
        &after.subject.id.attempt,
        1,
        &after.resources.finalization,
    )?;
    conn.execute(
        "UPDATE payload_staging_references SET destination = 1, upload_id = ?1, generation = ?2, part_number = ?3 WHERE attempt = ?4",
        params![upload_id, after.subject.payload_generation.get(), part_number, after.subject.id.attempt],
    )?;
    conn.execute(
        "UPDATE payload_staging_owners SET lifecycle = ?1, generation_state = 2 WHERE attempt = ?2",
        params![lifecycle_tag(after.lifecycle), after.subject.id.attempt],
    )?;
    if let Some((old, retired)) = displaced {
        conn.execute(
            "DELETE FROM payload_staging_references WHERE attempt = ?1",
            [&old.subject.id.attempt],
        )?;
        conn.execute(
            "UPDATE payload_staging_owners SET lifecycle = ?1 WHERE attempt = ?2",
            params![lifecycle_tag(retired.lifecycle), old.subject.id.attempt],
        )?;
    }
    savepoint.commit()?;
    Ok(())
}

/// Resolve the complete logical standard-object generation, even for an empty
/// stream. Never infer ownership from a shard hash or silently choose one owner.
pub(super) fn owner_for_standard(
    conn: &Connection,
    pg_id: PgId,
    bucket: &BucketName,
    key: &ObjectKey,
    generation: GenerationId,
) -> Result<StagingOwnerRecord, PersistenceError> {
    if conn.is_autocommit() {
        return Err(PersistenceError::TransactionRequired);
    }
    let mut statement = conn.prepare_cached(
        "SELECT attempt FROM payload_staging_owners WHERE bucket = ?1 AND key = ?2 \
         AND target IN (0, 1) AND payload_generation = ?3 LIMIT 2",
    )?;
    let attempts = statement
        .query_map(params![bucket, key, generation.get()], |row| {
            row.get::<_, SessionId>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let [attempt] = attempts.as_slice() else {
        return Err(PersistenceError::ObjectOwner);
    };
    load(conn, pg_id, attempt)?.ok_or(PersistenceError::ObjectOwner)
}

/// Publish the new standard owner inside the caller's manifest transaction.
/// Displaced ownership uses the shared retained-reclaim transition below.
pub(super) fn publish_standard(
    conn: &Connection,
    pg_id: PgId,
    configured_topology: &PgTopology,
    before: &CheckedStagingOwner<'_>,
    after: &CheckedStagingOwner<'_>,
) -> Result<(), PersistenceError> {
    if conn.is_autocommit() {
        return Err(PersistenceError::TransactionRequired);
    }
    let before = before.record_for_topology(configured_topology)?;
    let after = after.record_for_topology(configured_topology)?;
    if before.subject.object_pg != pg_id || after.subject.object_pg != pg_id {
        return Err(PersistenceError::WrongPg);
    }
    if before.published_standard(&after.references)? != *after
        || load(conn, pg_id, &before.subject.id.attempt)?.as_ref() != Some(before)
    {
        return Err(PersistenceError::ObjectConflict);
    }
    let savepoint = InsertionSavepoint::begin(conn)?;
    let attempt = &after.subject.id.attempt;
    if before.subject.target == StagingTarget::DirectPut {
        insert_reference(conn, attempt, &after.references[0])?;
    } else {
        conn.execute(
            "UPDATE payload_staging_references SET destination = 2, generation = ?1 WHERE attempt = ?2",
            params![after.subject.payload_generation.get(), attempt],
        )?;
    }
    conn.execute(
        "UPDATE payload_staging_resources SET state = 2 WHERE attempt = ?1 AND role = 0",
        [attempt],
    )?;
    conn.execute(
        "UPDATE payload_staging_owners SET lifecycle = 1, generation_state = 2 WHERE attempt = ?1",
        [attempt],
    )?;
    savepoint.commit()?;
    Ok(())
}

/// Object-to-Reclaim handoff shared by PUT, MPU completion and both delete commands. The
/// caller encloses live replacement, reclaim insertion and this transition in
/// one rollback savepoint. No resource proof or physical window is released.
pub(super) fn retain_object_reclaim(
    conn: &Connection,
    pg_id: PgId,
    topology: &PgTopology,
    old: &CheckedStagingOwner<'_>,
    retired: &CheckedStagingOwner<'_>,
) -> Result<(), PersistenceError> {
    if conn.is_autocommit() {
        return Err(PersistenceError::TransactionRequired);
    }
    let old = old.record_for_topology(topology)?;
    let retired = retired.record_for_topology(topology)?;
    if old.subject.object_pg != pg_id || retired.subject.object_pg != pg_id {
        return Err(PersistenceError::WrongPg);
    }
    if old.reclaimed_object()? != *retired
        || load(conn, pg_id, &old.subject.id.attempt)?.as_ref() != Some(old)
    {
        return Err(PersistenceError::ObjectConflict);
    }
    let savepoint = InsertionSavepoint::begin(conn)?;
    conn.execute(
        "UPDATE payload_staging_references SET destination = 3 WHERE attempt = ?1",
        [&old.subject.id.attempt],
    )?;
    conn.execute(
        "UPDATE payload_staging_owners SET lifecycle = ?1 WHERE attempt = ?2",
        params![lifecycle_tag(retired.lifecycle), old.subject.id.attempt],
    )?;
    savepoint.commit()?;
    Ok(())
}

/// Include closed attempts as well: their evidence survives parent MPU removal.
/// Include empty owners and omitted multipart attempts, not just rows with a
/// selected reference. Incarnation and layout are checked by the caller.
pub(super) fn owners_for_object_generation(
    conn: &Connection,
    pg_id: PgId,
    bucket: &BucketName,
    key: &ObjectKey,
    generation: GenerationId,
) -> Result<Vec<StagingOwnerRecord>, PersistenceError> {
    if conn.is_autocommit() {
        return Err(PersistenceError::TransactionRequired);
    }
    let mut statement = conn.prepare_cached(
        "SELECT attempt FROM payload_staging_owners WHERE bucket = ?1 AND key = ?2 \
         AND ((target IN (0, 1) AND payload_generation = ?3) OR (target = 2 AND upload_generation = ?3)) ORDER BY attempt",
    )?;
    let attempts = statement
        .query_map(params![bucket, key, generation.get()], |row| {
            row.get::<_, SessionId>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    attempts
        .iter()
        .map(|attempt| load(conn, pg_id, attempt)?.ok_or(PersistenceError::ObjectOwner))
        .collect()
}

/// Drop only the exact reclaim bindings after the root-removal preflight. The
/// enclosing savepoint also protects the root and claim. This is not retirement
/// or evidence of confirmed physical deletion/release.
pub(super) fn complete_reclaim(
    conn: &Connection,
    pg_id: PgId,
    topology: &PgTopology,
    before: &CheckedStagingOwner<'_>,
    after: &CheckedStagingOwner<'_>,
) -> Result<(), PersistenceError> {
    if conn.is_autocommit() {
        return Err(PersistenceError::TransactionRequired);
    }
    let before = before.record_for_topology(topology)?;
    let after = after.record_for_topology(topology)?;
    if before.subject.object_pg != pg_id || after.subject.object_pg != pg_id {
        return Err(PersistenceError::WrongPg);
    }
    if before.completed_reclaim()? != *after
        || load(conn, pg_id, &before.subject.id.attempt)?.as_ref() != Some(before)
    {
        return Err(PersistenceError::ObjectConflict);
    }
    let savepoint = InsertionSavepoint::begin(conn)?;
    conn.execute(
        "DELETE FROM payload_staging_references WHERE attempt = ?1",
        [&before.subject.id.attempt],
    )?;
    conn.execute(
        "UPDATE payload_staging_owners SET lifecycle = 4 WHERE attempt = ?1",
        [&before.subject.id.attempt],
    )?;
    savepoint.commit()?;
    Ok(())
}

/// Include closed attempts as well: their evidence survives parent MPU removal.
/// Query by upload identity so crossed bucket/key rows cannot silently disappear.
pub(super) fn owners_for_upload(
    conn: &Connection,
    pg_id: PgId,
    upload: &UploadId,
) -> Result<Vec<StagingOwnerRecord>, PersistenceError> {
    if conn.is_autocommit() {
        return Err(PersistenceError::TransactionRequired);
    }
    let mut statement = conn.prepare_cached("SELECT attempt FROM payload_staging_owners WHERE target = 2 AND upload_id = ?1 ORDER BY attempt")?;
    let attempts = statement
        .query_map([upload], |row| row.get::<_, SessionId>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    attempts
        .iter()
        .map(|attempt| {
            load(conn, pg_id, attempt)?.ok_or(PersistenceError::MultipartTransferConflict)
        })
        .collect()
}

/// The outer savepoint encloses upload/manifest/session removal and every
/// transition here. No windows, participants, proof identities or cutoffs change.
pub(super) fn transfer_multipart_ownership(
    conn: &Connection,
    pg_id: PgId,
    topology: &PgTopology,
    transfers: &[(
        super::multipart::Disposition,
        CheckedStagingOwner<'_>,
        CheckedStagingOwner<'_>,
    )],
) -> Result<(), PersistenceError> {
    use super::multipart::Disposition;
    if conn.is_autocommit() {
        return Err(PersistenceError::TransactionRequired);
    }
    let mut attempts = BTreeSet::new();
    for (disposition, before, after) in transfers {
        let before = before.record_for_topology(topology)?;
        let after = after.record_for_topology(topology)?;
        if before.subject.object_pg != pg_id || after.subject.object_pg != pg_id {
            return Err(PersistenceError::WrongPg);
        }
        if !matches!(before.subject.target, StagingTarget::UploadPart { .. })
            || !attempts.insert(&before.subject.id.attempt)
            || disposition.transition(before)? != *after
            || load(conn, pg_id, &before.subject.id.attempt)?.as_ref() != Some(before)
        {
            return Err(PersistenceError::MultipartTransferConflict);
        }
    }
    let savepoint = InsertionSavepoint::begin(conn)?;
    for (disposition, before, after) in transfers {
        let before = before.record_for_topology(topology)?;
        let after = after.record_for_topology(topology)?;
        let attempt = &before.subject.id.attempt;
        match disposition {
            Disposition::Selected => {
                let StagingTarget::UploadPart {
                    upload_generation, ..
                } = before.subject.target
                else {
                    return Err(PersistenceError::MultipartTransferConflict);
                };
                conn.execute("UPDATE payload_staging_references SET destination = 2, upload_id = NULL, generation = ?1 WHERE attempt = ?2",
                    params![upload_generation.get(), attempt])?;
            }
            Disposition::Omitted | Disposition::Active => {
                conn.execute(
                    "DELETE FROM payload_staging_references WHERE attempt = ?1",
                    [attempt],
                )?;
                conn.execute("UPDATE payload_staging_owners SET lifecycle = ?1, generation_state = 2 WHERE attempt = ?2",
                    params![lifecycle_tag(after.lifecycle), attempt])?;
            }
        }
    }
    savepoint.commit()?;
    Ok(())
}

fn insert_window(conn: &Connection, window: &StagingWindow) -> Result<(), PersistenceError> {
    let attempt = &window.owner.attempt;
    let geometry = &window.geometry;
    let placement = &window.participants;
    conn.prepare_cached(&insert_sql(1))?.execute(params![
        attempt,
        window.index,
        window.segment_hash.as_slice(),
        window.start.get(),
        window.end.get(),
        window.issued_at_ms,
        window.cutoff_ms,
        geometry.codec.tag(),
        geometry.ec.k,
        geometry.ec.m,
        geometry.logical_bound,
        geometry.stored_bound,
        geometry.shard_bound,
        placement.data_pg.get(),
        placement.placement_epoch.get(),
        placement.acknowledgement_primary.as_u32(),
    ])?;
    let mut participants = conn.prepare_cached(&insert_sql(2))?;
    for (index, node) in placement.files.iter().enumerate() {
        participants.execute(params![attempt, window.index, index, node.as_u32()])?;
    }
    Ok(())
}

fn insert_reference(
    conn: &Connection,
    attempt: &SessionId,
    reference: &StagingReference,
) -> Result<(), PersistenceError> {
    let (destination, upload, generation, part) = match &reference.destination {
        StagingDestination::Stream => (0, None, None, None),
        StagingDestination::Part {
            upload_id,
            part_number,
            payload_generation,
        } => (
            1,
            Some(upload_id),
            Some(payload_generation.get()),
            Some(*part_number),
        ),
        StagingDestination::Object {
            generation,
            part_number,
        } => (2, None, Some(generation.get()), *part_number),
        StagingDestination::Reclaim {
            generation,
            part_number,
        } => (3, None, Some(generation.get()), *part_number),
    };
    conn.prepare_cached(&insert_sql(3))?.execute(params![
        attempt,
        reference.window_index,
        reference.segment_hash.as_slice(),
        reference.physical_vid.get(),
        reference.logical_size,
        reference.stored_size,
        destination,
        upload,
        generation,
        part,
    ])?;
    Ok(())
}

trait StagingRow {
    fn get<I: AsRef<str>, T: FromSql>(&self, column: I) -> Result<T, PersistenceError>;
}

impl StagingRow for Row<'_> {
    fn get<I: AsRef<str>, T: FromSql>(&self, column: I) -> Result<T, PersistenceError> {
        Row::get(self, column.as_ref()).map_err(Into::into)
    }
}

struct CheckpointRow<'a> {
    columns: &'a [&'a str],
    values: &'a [MetadataCheckpointValue],
}

impl StagingRow for CheckpointRow<'_> {
    fn get<I: AsRef<str>, T: FromSql>(&self, column: I) -> Result<T, PersistenceError> {
        let index = self
            .columns
            .iter()
            .position(|name| *name == column.as_ref())
            .ok_or(PersistenceError::Row("checkpoint column"))?;
        let value = match self
            .values
            .get(index)
            .ok_or(PersistenceError::Row("checkpoint row width"))?
        {
            MetadataCheckpointValue::Null => ValueRef::Null,
            MetadataCheckpointValue::Integer(value) => ValueRef::Integer(*value),
            MetadataCheckpointValue::Text(value) => ValueRef::Text(value),
            MetadataCheckpointValue::Blob(value) => ValueRef::Blob(value),
            MetadataCheckpointValue::RealBits(_) => {
                return Err(PersistenceError::Row("checkpoint real value"))
            }
        };
        T::column_result(value)
            .map_err(|_| PersistenceError::Row("checkpoint value type or domain"))
    }
}

fn bytes16(row: &impl StagingRow, column: &str) -> Result<[u8; 16], PersistenceError> {
    row.get::<_, Vec<u8>>(column)?
        .try_into()
        .map_err(|_| PersistenceError::Row("expected 16-byte identity"))
}

fn generation(row: &impl StagingRow, column: &str) -> Result<GenerationId, PersistenceError> {
    GenerationId::new(row.get(column)?).ok_or(PersistenceError::Row("generation"))
}

fn epoch(row: &impl StagingRow, column: &str) -> Result<ClusterEpoch, PersistenceError> {
    ClusterEpoch::new(row.get(column)?).ok_or(PersistenceError::Row("epoch"))
}

fn resource<T>(state: u8, value: T) -> Result<ResourceResponsibility<T>, PersistenceError> {
    match state {
        1 => Ok(ResourceResponsibility::Live(value)),
        2 => Ok(ResourceResponsibility::ReleaseDue(value)),
        _ => Err(PersistenceError::Row("resource state")),
    }
}

fn read_owner(row: &impl StagingRow) -> Result<StagingOwnerRecord, PersistenceError> {
    let upload_id: Option<UploadId> = row.get("upload_id")?;
    let upload_generation: Option<u64> = row.get("upload_generation")?;
    let part_number: Option<u32> = row.get("part_number")?;
    let target = match (
        row.get::<_, u8>("target")?,
        upload_id,
        upload_generation,
        part_number,
    ) {
        (0, None, None, None) => StagingTarget::DirectPut,
        (1, None, None, None) => StagingTarget::StreamPut,
        (2, Some(upload_id), Some(upload_generation), Some(part_number)) => {
            StagingTarget::UploadPart {
                upload_id,
                upload_generation: GenerationId::new(upload_generation)
                    .ok_or(PersistenceError::Row("upload generation"))?,
                part_number,
            }
        }
        _ => return Err(PersistenceError::Row("target")),
    };
    let lifecycle = match row.get::<_, u8>("lifecycle")? {
        0 => StagingLifecycle::Active,
        1 => StagingLifecycle::Published,
        2 => StagingLifecycle::Retiring(StagingRetirement::Unpublished),
        3 => StagingLifecycle::Retiring(StagingRetirement::PublishedRetained),
        4 => StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced),
        _ => return Err(PersistenceError::Row("lifecycle")),
    };
    let subject = StagingSubject {
        id: StagingOwnerId {
            attempt: row.get("attempt")?,
            introduction: MetadataCommandId::new(
                epoch(row, "introduction_epoch")?,
                PgId::new(row.get("introduction_pg")?),
                MetadataCommandLogIndex::new(row.get("introduction_index")?)
                    .ok_or(PersistenceError::Row("introduction index"))?,
            ),
        },
        bucket: row.get("bucket")?,
        bucket_incarnation: row.get("incarnation")?,
        key: row.get("key")?,
        object_pg: PgId::new(row.get("object_pg")?),
        originating_epoch: epoch(row, "originating_epoch")?,
        payload_generation: generation(row, "payload_generation")?,
        target,
        issuer: StreamAllocationIssuer {
            node_id: NodeId::new(row.get("issuer_node")?),
            lifetime: bytes16(row, "issuer_lifetime")?,
        },
    };
    let generation_resource = resource(
        row.get("generation_state")?,
        StagingGenerationResource {
            reservation_id: subject.id.attempt.clone(),
            generation: subject.payload_generation,
        },
    )?;
    Ok(StagingOwnerRecord {
        subject,
        lifecycle,
        reserved_end: generation(row, "reserved_end")?,
        maximum_cutoff_ms: row.get("maximum_cutoff_ms")?,
        windows: vec![],
        references: vec![],
        resources: StagingResources {
            generation: generation_resource,
            creation: ResourceResponsibility::NoResource,
            finalization: ResourceResponsibility::NoResource,
            abandoned_finalizations: vec![],
        },
    })
}

fn read_window(
    row: &impl StagingRow,
    subject: &StagingSubject,
) -> Result<StagingWindow, PersistenceError> {
    Ok(StagingWindow {
        owner: subject.id.clone(),
        issuer: subject.issuer.clone(),
        index: row.get("window_index")?,
        segment_hash: bytes16(row, "segment_hash")?,
        start: generation(row, "start_vid")?,
        end: generation(row, "end_vid")?,
        issued_at_ms: row.get("issued_at_ms")?,
        cutoff_ms: row.get("cutoff_ms")?,
        geometry: StagingGeometry {
            codec: SegmentPayloadCodec::from_u8(row.get("codec")?)
                .ok_or(PersistenceError::Row("codec"))?,
            ec: EcShape {
                k: row.get("ec_k")?,
                m: row.get("ec_m")?,
            },
            logical_bound: row.get("logical_bound")?,
            stored_bound: row.get("stored_bound")?,
            shard_bound: row.get("shard_bound")?,
        },
        participants: StagingParticipants {
            data_pg: PgId::new(row.get("data_pg")?),
            placement_epoch: epoch(row, "placement_epoch")?,
            files: vec![],
            acknowledgement_primary: NodeId::new(row.get("acknowledgement_primary")?),
        },
    })
}

fn read_reference(row: &impl StagingRow) -> Result<StagingReference, PersistenceError> {
    let upload: Option<UploadId> = row.get("upload_id")?;
    let raw_generation: Option<u64> = row.get("generation")?;
    let destination_generation = raw_generation
        .map(|value| GenerationId::new(value).ok_or(PersistenceError::Row("reference generation")))
        .transpose()?;
    let part: Option<u32> = row.get("part_number")?;
    let destination = match (
        row.get::<_, u8>("destination")?,
        upload,
        destination_generation,
        part,
    ) {
        (0, None, None, None) => StagingDestination::Stream,
        (1, Some(upload_id), Some(payload_generation), Some(part_number)) => {
            StagingDestination::Part {
                upload_id,
                part_number,
                payload_generation,
            }
        }
        (2, None, Some(generation), part_number) => StagingDestination::Object {
            generation,
            part_number,
        },
        (3, None, Some(generation), part_number) => StagingDestination::Reclaim {
            generation,
            part_number,
        },
        _ => return Err(PersistenceError::Row("reference destination")),
    };
    Ok(StagingReference {
        window_index: row.get("window_index")?,
        segment_hash: bytes16(row, "segment_hash")?,
        physical_vid: generation(row, "physical_vid")?,
        logical_size: row.get("logical_size")?,
        stored_size: row.get("stored_size")?,
        destination,
    })
}

fn read_resource(
    row: &impl StagingRow,
) -> Result<ResourceResponsibility<StagingBucketResource>, PersistenceError> {
    resource(
        row.get("state")?,
        StagingBucketResource {
            identity: StableBucketReservation {
                bucket: row.get("bucket")?,
                reservation_id: row.get("reservation_id")?,
                owner_token: row.get("owner_token")?,
                cluster_epoch: epoch(row, "cluster_epoch")?,
                execution_generation: row.get("execution_generation")?,
                incarnation: row.get("incarnation")?,
                operation_kind: row.get("operation_kind")?,
                created_at: row.get("created_at")?,
                target_context: row.get("target_context")?,
            },
            observed_lease_deadline: row.get("observed_lease_deadline")?,
            release_pg: PgId::new(row.get("release_pg")?),
        },
    )
}

fn append_resource(
    owner: &mut StagingOwnerRecord,
    row: &impl StagingRow,
) -> Result<(), PersistenceError> {
    let role: u32 = row.get("role")?;
    let resource = read_resource(row)?;
    match role {
        0 | 1 => {
            let slot = if role == 0 {
                &mut owner.resources.creation
            } else {
                &mut owner.resources.finalization
            };
            if *slot != ResourceResponsibility::NoResource {
                return Err(PersistenceError::Row("duplicate resource role"));
            }
            *slot = resource;
        }
        index => {
            if (index - 2) as usize != owner.resources.abandoned_finalizations.len() {
                return Err(PersistenceError::Row(
                    "noncontiguous abandoned finalization slots",
                ));
            }
            let ResourceResponsibility::ReleaseDue(proof) = resource else {
                return Err(PersistenceError::Row(
                    "abandoned finalization must be release-due",
                ));
            };
            owner.resources.abandoned_finalizations.push(proof);
        }
    }
    Ok(())
}

/// The caller holds the object-PG lock and a transaction covering all these
/// reads, including any live-manifest correspondence checks. A row projection
/// alone is never a detached cleanup/write capability.
pub(super) fn load(
    conn: &Connection,
    pg_id: PgId,
    attempt: &SessionId,
) -> Result<Option<StagingOwnerRecord>, PersistenceError> {
    if conn.is_autocommit() {
        return Err(PersistenceError::TransactionRequired);
    }
    let mut stmt = conn.prepare_cached(&select_sql(0))?;
    let mut rows = stmt.query([attempt])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let mut owner = read_owner(row)?;
    if owner.subject.object_pg != pg_id {
        return Err(PersistenceError::WrongPg);
    }
    let mut stmt = conn.prepare_cached(&select_sql(1))?;
    let mut rows = stmt.query([attempt])?;
    while let Some(row) = rows.next()? {
        if owner.windows.len() > MAX_STREAM_SEGMENTS as usize {
            return Err(StagingInvariant::Windows.into());
        }
        owner.windows.push(read_window(row, &owner.subject)?);
    }
    let mut stmt = conn.prepare_cached(&select_sql(2))?;
    let mut rows = stmt.query([attempt])?;
    while let Some(row) = rows.next()? {
        let index: u32 = row.get("window_index")?;
        let window = owner
            .windows
            .get_mut(index as usize)
            .filter(|window| window.index == index)
            .ok_or(PersistenceError::Row("participant window"))?;
        let shard: u32 = row.get("shard_index")?;
        if shard as usize != window.participants.files.len() || shard >= 32 {
            return Err(StagingInvariant::Placement.into());
        }
        window
            .participants
            .files
            .push(NodeId::new(row.get("node_id")?));
    }
    let mut stmt = conn.prepare_cached(&select_sql(3))?;
    let mut rows = stmt.query([attempt])?;
    while let Some(row) = rows.next()? {
        if owner.references.len() >= MAX_STREAM_SEGMENTS as usize {
            return Err(StagingInvariant::Reference.into());
        }
        owner.references.push(read_reference(row)?);
    }
    let mut stmt = conn.prepare_cached(&select_sql(4))?;
    let mut rows = stmt.query([attempt])?;
    while let Some(row) = rows.next()? {
        append_resource(&mut owner, row)?;
    }
    owner.validate_structure()?;
    Ok(Some(owner))
}

/// Validate the allocator fence in the complete checkpoint, not just the owner
/// tables. All lifecycles retain physical identities; releasing live resources
/// must never make those identities available to a subsequent attempt again.
pub(super) fn validate_checkpoint_generation_fences(
    owners: &[StagingOwnerRecord],
    blocks: &[MetadataCheckpointTableBlock],
) -> Result<(), PersistenceError> {
    if owners.is_empty() {
        return Ok(());
    }
    let counters = blocks
        .iter()
        .find(|block| block.table_name == "object_write_counters")
        .ok_or(PersistenceError::Row(
            "checkpoint object generation counters",
        ))?;
    let columns = counters
        .columns
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let mut high_waters = std::collections::BTreeMap::new();
    for row in &counters.rows {
        let row = CheckpointRow {
            columns: &columns,
            values: &row.values,
        };
        let key = (
            row.get::<_, BucketName>("bucket")?,
            row.get::<_, ObjectKey>("key")?,
        );
        let high_water = row.get::<_, Option<u64>>("max_committed_generation")?;
        if high_waters.insert(key, high_water).is_some() {
            return Err(PersistenceError::Row(
                "checkpoint duplicate object generation counter",
            ));
        }
    }
    for owner in owners {
        owner.subject.require_generation_high_water(
            high_waters
                .get(&(owner.subject.bucket.clone(), owner.subject.key.clone()))
                .copied()
                .flatten(),
        )?;
    }
    Ok(())
}

/// Validate the complete owner-controlled subset before checkpoint replacement.
/// The enclosing verifier checks table/row digests; this checks typed values,
/// ownership joins and the same lifecycle/reference/resource matrix as SQL.
/// It does not grant authority or establish correspondence with live manifests.
pub(super) fn decode_checkpoint_blocks(
    pg_id: PgId,
    blocks: &[MetadataCheckpointTableBlock],
) -> Result<Vec<StagingOwnerRecord>, PersistenceError> {
    use std::collections::BTreeMap;

    if blocks.len() != TABLES.len() {
        return Err(PersistenceError::Row("checkpoint staging table count"));
    }
    let mut owners = BTreeMap::<SessionId, StagingOwnerRecord>::new();
    for (table_index, (table, block)) in TABLES.iter().zip(blocks).enumerate() {
        if block.table_name != table.name
            || block
                .columns
                .iter()
                .map(String::as_str)
                .ne(table.columns.iter().copied())
            || block
                .order_columns
                .iter()
                .map(String::as_str)
                .ne(table.order_columns.iter().copied())
            || block.filter != "all-rows"
        {
            return Err(PersistenceError::Row("checkpoint staging table identity"));
        }
        // Sorting is part of the canonical representation. Reject duplicates,
        // reordering and orphan children rather than normalizing them silently.
        let mut previous = None;
        for row in &block.rows {
            if row.values.len() != table.columns.len() {
                return Err(PersistenceError::Row("checkpoint row width"));
            }
            let row = CheckpointRow {
                columns: table.columns,
                values: &row.values,
            };
            let attempt: SessionId = row.get("attempt")?;
            let index = match table_index {
                0 => 0,
                1..=3 => row.get::<_, u32>("window_index")?,
                4 => row.get::<_, u32>("role")?,
                _ => unreachable!(),
            };
            let shard = if table_index == 2 {
                row.get::<_, u32>("shard_index")?
            } else {
                0
            };
            let key = (attempt.clone(), index, shard);
            if previous.as_ref().is_some_and(|previous| *previous >= key) {
                return Err(PersistenceError::Row(
                    "checkpoint duplicate or unordered key",
                ));
            }
            previous = Some(key);
            if table_index == 0 {
                let owner = read_owner(&row)?;
                if owner.subject.object_pg != pg_id {
                    return Err(PersistenceError::WrongPg);
                }
                owners.insert(attempt, owner);
                continue;
            }
            let owner = owners
                .get_mut(&attempt)
                .ok_or(PersistenceError::Row("checkpoint orphan staging child"))?;
            match table_index {
                1 => {
                    if owner.windows.len() > MAX_STREAM_SEGMENTS as usize {
                        return Err(StagingInvariant::Windows.into());
                    }
                    owner.windows.push(read_window(&row, &owner.subject)?);
                }
                2 => {
                    let window = owner
                        .windows
                        .get_mut(index as usize)
                        .filter(|window| window.index == index)
                        .ok_or(PersistenceError::Row("checkpoint orphan participant"))?;
                    if shard as usize != window.participants.files.len() || shard >= 32 {
                        return Err(StagingInvariant::Placement.into());
                    }
                    window
                        .participants
                        .files
                        .push(NodeId::new(row.get("node_id")?));
                }
                3 => {
                    if owner.references.len() >= MAX_STREAM_SEGMENTS as usize {
                        return Err(StagingInvariant::Reference.into());
                    }
                    owner.references.push(read_reference(&row)?);
                }
                4 => {
                    append_resource(owner, &row)?;
                }
                _ => unreachable!(),
            }
        }
    }
    for owner in owners.values() {
        owner.validate_structure()?;
    }
    Ok(owners.into_values().collect())
}
