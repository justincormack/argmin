// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use placement::NodeId;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};

use crate::control_plane::UnavailablePgTransitionMutationBinding;
use crate::data_dir::prepare_private_data_dir;
use crate::{ClusterEpoch, PgId};

const STAGING_STORE_DIR: &str = "metadata-transfer-staging";
const ESTABLISHMENT_MARKER_FILE: &str = "metadata-transfer-staging.established";
const ESTABLISHMENT_MARKER_TEMP_FILE: &str = ".metadata-transfer-staging.established.tmp";
const ARTIFACTS_DIR: &str = "artifacts";
const QUARANTINE_DIR: &str = "quarantine";
const MANIFEST_FILE: &str = "manifest";
const INITIALIZATION_MARKER_FILE: &str = "initialized";
const INITIALIZATION_MARKER_TEMP_FILE: &str = ".initialized.tmp";
const CATALOGUE_FILE: &str = "catalogue.db";
const STAGING_STORE_MAGIC: &[u8; 8] = b"ARGMSTG\0";
const STAGING_INITIALIZATION_MAGIC: &[u8; 8] = b"ARGMSTGI";
const STAGING_ESTABLISHMENT_MAGIC: &[u8; 8] = b"ARGMSTGE";
const STAGING_STORE_FORMAT_VERSION: u16 = 1;
const STAGING_STORE_SCHEMA_V1: &str =
    include_str!("schema_manifests/metadata_transfer_staging_v1.sql");
const DIGEST_LEN: usize = 32;
const MANIFEST_BODY_LEN: usize = STAGING_STORE_MAGIC.len() + 2 + DIGEST_LEN;
const MANIFEST_LEN: usize = MANIFEST_BODY_LEN + 8;
const INITIALIZATION_MARKER_BODY_LEN: usize = STAGING_INITIALIZATION_MAGIC.len() + 2 + DIGEST_LEN;
const INITIALIZATION_MARKER_LEN: usize = INITIALIZATION_MARKER_BODY_LEN + 8;
const ESTABLISHMENT_MARKER_BODY_LEN: usize = STAGING_ESTABLISHMENT_MAGIC.len() + 2 + DIGEST_LEN;
const ESTABLISHMENT_MARKER_LEN: usize = ESTABLISHMENT_MARKER_BODY_LEN + 8;
const MAX_ENDPOINT_BYTES: usize = 2_048;
const MAX_STAGING_EVIDENCE_BYTES: usize = 4_096;
const MAX_STAGING_EVIDENCE_PAGE_BYTES: usize = 120 * 1_024;
pub(crate) const METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION: u16 = 1;

#[derive(Debug, thiserror::Error)]
pub(crate) enum MetadataTransferStagingError {
    #[error("{context}: {source}")]
    Io {
        context: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("{context}: {source}")]
    Sql {
        context: &'static str,
        #[source]
        source: rusqlite::Error,
    },
    #[error("unsupported metadata-transfer staging-store format version {0}")]
    UnsupportedFormatVersion(u16),
    #[error("unsupported metadata-transfer staging catalogue version {0}")]
    UnsupportedCatalogueVersion(u32),
    #[error("metadata-transfer staging-store invariant failed: {0}")]
    Invariant(String),
    #[error("metadata-transfer staging intent conflicts with durable state: {0}")]
    IntentConflict(String),
    #[error("metadata-transfer staging capacity exhausted: {0}")]
    Capacity(String),
    #[error("metadata-transfer staging artifact digest or length mismatch")]
    ArtifactMismatch,
    #[error("metadata-transfer staging generation has been tombstoned or finalized")]
    GenerationRetired,
}

impl MetadataTransferStagingError {
    fn io(context: &'static str, source: std::io::Error) -> Self {
        Self::Io { context, source }
    }

    fn sql(context: &'static str, source: rusqlite::Error) -> Self {
        Self::Sql { context, source }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MetadataTransferStagingLimits {
    pub(crate) max_entries: usize,
    pub(crate) max_artifact_bytes: u64,
    pub(crate) max_total_bytes: u64,
}

impl MetadataTransferStagingLimits {
    pub(crate) fn new(
        max_entries: usize,
        max_artifact_bytes: u64,
        max_total_bytes: u64,
    ) -> Result<Self, MetadataTransferStagingError> {
        if max_entries == 0 || max_artifact_bytes == 0 || max_total_bytes < max_artifact_bytes {
            return Err(MetadataTransferStagingError::Invariant(
                "staging limits require entries > 0 and total bytes >= artifact bytes > 0"
                    .to_owned(),
            ));
        }
        Ok(Self {
            max_entries,
            max_artifact_bytes,
            max_total_bytes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataTransferStagingNodeIdentity {
    node_id: NodeId,
    node_incarnation: u64,
    endpoint: String,
}

impl MetadataTransferStagingNodeIdentity {
    pub(crate) fn new(
        node_id: NodeId,
        node_incarnation: u64,
        endpoint: String,
    ) -> Result<Self, MetadataTransferStagingError> {
        if node_incarnation == 0 || endpoint.is_empty() || endpoint.len() > MAX_ENDPOINT_BYTES {
            return Err(MetadataTransferStagingError::Invariant(
                "staging node identity requires a nonzero incarnation and bounded endpoint"
                    .to_owned(),
            ));
        }
        Ok(Self {
            node_id,
            node_incarnation,
            endpoint,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataTransferStagingIntent {
    pg_id: PgId,
    transition_epoch: ClusterEpoch,
    source_epoch: ClusterEpoch,
    source_acting_set: Vec<NodeId>,
    destination_acting_set: Vec<NodeId>,
    staging_generation: u64,
    artifact_digest: [u8; DIGEST_LEN],
    artifact_length: u64,
    artifact_format_version: u16,
}

impl MetadataTransferStagingIntent {
    pub(crate) fn for_unavailable_transition(
        binding: &UnavailablePgTransitionMutationBinding,
        artifact_digest: [u8; DIGEST_LEN],
        artifact_length: u64,
        artifact_format_version: u16,
    ) -> Result<Self, MetadataTransferStagingError> {
        let intent = Self {
            pg_id: binding.pg_id(),
            transition_epoch: binding.transition_epoch(),
            source_epoch: binding.source_epoch(),
            source_acting_set: binding.source_acting_set().to_vec(),
            destination_acting_set: binding.destination_acting_set().to_vec(),
            staging_generation: binding.transition_epoch().get(),
            artifact_digest,
            artifact_length,
            artifact_format_version,
        };
        validate_staging_intent_shape(&intent)?;
        Ok(intent)
    }

    fn artifact_file_name(&self) -> String {
        format!(
            "pg-{:08x}-transition-{:016x}-generation-{:016x}-{}.artifact",
            self.pg_id.get(),
            self.transition_epoch.get(),
            self.staging_generation,
            hex(&self.artifact_digest)
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetadataTransferStagingIntentOutcome {
    Created,
    ExactReplay,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataTransferStagingReceipt {
    bytes: Vec<u8>,
}

impl MetadataTransferStagingReceipt {
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StagingState {
    Intent = 0,
    Published = 1,
    Imported = 2,
    Tombstoned = 3,
}

impl StagingState {
    fn decode(value: i64) -> Result<Self, MetadataTransferStagingError> {
        match value {
            0 => Ok(Self::Intent),
            1 => Ok(Self::Published),
            2 => Ok(Self::Imported),
            3 => Ok(Self::Tombstoned),
            _ => Err(MetadataTransferStagingError::Invariant(format!(
                "unknown staging state {value}"
            ))),
        }
    }
}

#[derive(Clone)]
struct StagingRow {
    intent: MetadataTransferStagingIntent,
    state: StagingState,
    publication_receipt: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StagingEvidenceDelta {
    actor: MetadataTransferStagingNodeIdentity,
    bytes: Vec<u8>,
}

type StagingDurabilityObserver = Arc<dyn Fn() + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StagingParentDirectorySyncPoint {
    RootCreation,
    EstablishmentPublication,
}

type StagingParentDirectorySync = Arc<
    dyn Fn(
            &Path,
            &'static str,
            StagingParentDirectorySyncPoint,
        ) -> Result<(), MetadataTransferStagingError>
        + Send
        + Sync,
>;

struct MetadataTransferStagingState {
    connection: Connection,
    quarantined_bytes: u64,
}

pub(crate) struct MetadataTransferStagingStore {
    artifacts_dir: PathBuf,
    quarantine_dir: PathBuf,
    identity: MetadataTransferStagingNodeIdentity,
    limits: MetadataTransferStagingLimits,
    state: Mutex<MetadataTransferStagingState>,
    temp_sequence: AtomicU64,
    publication_durability_observer: Option<StagingDurabilityObserver>,
}

impl MetadataTransferStagingStore {
    pub(crate) fn open(
        data_dir: &Path,
        identity: MetadataTransferStagingNodeIdentity,
        limits: MetadataTransferStagingLimits,
    ) -> Result<Self, MetadataTransferStagingError> {
        Self::open_inner(data_dir, identity, limits, None, None)
    }

    #[cfg(test)]
    fn open_with_publication_durability_observer(
        data_dir: &Path,
        identity: MetadataTransferStagingNodeIdentity,
        limits: MetadataTransferStagingLimits,
        observer: StagingDurabilityObserver,
    ) -> Result<Self, MetadataTransferStagingError> {
        Self::open_inner(data_dir, identity, limits, None, Some(observer))
    }

    #[cfg(test)]
    fn open_with_parent_directory_sync(
        data_dir: &Path,
        identity: MetadataTransferStagingNodeIdentity,
        limits: MetadataTransferStagingLimits,
        parent_directory_sync: StagingParentDirectorySync,
    ) -> Result<Self, MetadataTransferStagingError> {
        Self::open_inner(
            data_dir,
            identity,
            limits,
            Some(parent_directory_sync),
            None,
        )
    }

    fn open_inner(
        data_dir: &Path,
        identity: MetadataTransferStagingNodeIdentity,
        limits: MetadataTransferStagingLimits,
        parent_directory_sync: Option<StagingParentDirectorySync>,
        publication_durability_observer: Option<StagingDurabilityObserver>,
    ) -> Result<Self, MetadataTransferStagingError> {
        prepare_secure_directory(data_dir, "prepare staging-store data directory")?;
        let root = data_dir.join(STAGING_STORE_DIR);
        let artifacts_dir = root.join(ARTIFACTS_DIR);
        let quarantine_dir = root.join(QUARANTINE_DIR);
        let established = establishment_marker_exists(data_dir)?;
        if established {
            sync_staging_parent_directory(
                data_dir,
                "confirm staging establishment marker publication",
                StagingParentDirectorySyncPoint::EstablishmentPublication,
                parent_directory_sync.as_ref(),
            )?;
            reject_path_if_present(
                &data_dir.join(ESTABLISHMENT_MARKER_TEMP_FILE),
                "inspect established staging-store temporary marker",
            )?;
            require_secure_directory(&root, "open established staging-store directory")?;
        } else {
            prepare_secure_directory(&root, "prepare staging-store directory")?;
            sync_staging_parent_directory(
                data_dir,
                "sync staging-store root creation",
                StagingParentDirectorySyncPoint::RootCreation,
                parent_directory_sync.as_ref(),
            )?;
            remove_interrupted_establishment_marker(data_dir)?;
        }
        let initialized = initialization_marker_exists(&root)?;
        if established && !initialized {
            return Err(MetadataTransferStagingError::Invariant(
                "established staging store is missing its initialization marker".to_owned(),
            ));
        }
        if !initialized {
            remove_interrupted_initialization_marker(&root)?;
        }
        prepare_secure_directory(&artifacts_dir, "prepare staging artifact directory")?;
        prepare_secure_directory(&quarantine_dir, "prepare staging quarantine directory")?;
        ensure_manifest(&root)?;

        let catalogue_path = root.join(CATALOGUE_FILE);
        if initialized {
            require_catalogue_file(&catalogue_path)?;
        }
        validate_catalogue_files_if_present(&catalogue_path)?;
        let mut flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW;
        if !initialized {
            flags |= OpenFlags::SQLITE_OPEN_CREATE;
        }
        let connection = Connection::open_with_flags(&catalogue_path, flags).map_err(|source| {
            MetadataTransferStagingError::sql("open staging catalogue", source)
        })?;
        validate_catalogue_files_if_present(&catalogue_path)?;
        if initialized {
            validate_initialized_catalogue(&connection)?;
            configure_catalogue(&connection)?;
        } else {
            configure_catalogue(&connection)?;
            initialize_or_validate_unmarked_catalogue(&connection)?;
            sync_directory(&root, "sync initialized staging catalogue")?;
            publish_initialization_marker(&root)?;
        }
        if !established {
            publish_establishment_marker(data_dir, parent_directory_sync.as_ref())?;
        }

        let mut store = Self {
            artifacts_dir,
            quarantine_dir,
            identity,
            limits,
            state: Mutex::new(MetadataTransferStagingState {
                connection,
                quarantined_bytes: 0,
            }),
            temp_sequence: AtomicU64::new(0),
            publication_durability_observer,
        };
        store.reconcile_startup_inventory()?;
        Ok(store)
    }

    pub(crate) fn create_intent(
        &self,
        intent: &MetadataTransferStagingIntent,
    ) -> Result<MetadataTransferStagingIntentOutcome, MetadataTransferStagingError> {
        self.validate_intent_limits(intent)?;
        let state = self.lock_state()?;
        if finalized_floor(&state.connection, intent.pg_id)? >= intent.staging_generation {
            return Err(MetadataTransferStagingError::GenerationRetired);
        }
        if let Some(existing) =
            load_staging_row(&state.connection, intent.pg_id, intent.staging_generation)?
        {
            require_exact_intent(&existing.intent, intent)?;
            if existing.state == StagingState::Tombstoned {
                return Err(MetadataTransferStagingError::GenerationRetired);
            }
            return Ok(MetadataTransferStagingIntentOutcome::ExactReplay);
        }
        let (entry_count, reserved_bytes) = staging_capacity(&state.connection)?;
        if entry_count >= self.limits.max_entries {
            return Err(MetadataTransferStagingError::Capacity(format!(
                "entry limit {} reached",
                self.limits.max_entries
            )));
        }
        let total = reserved_bytes
            .checked_add(state.quarantined_bytes)
            .and_then(|value| value.checked_add(intent.artifact_length))
            .ok_or_else(|| {
                MetadataTransferStagingError::Capacity("byte accounting overflowed".to_owned())
            })?;
        if total > self.limits.max_total_bytes {
            return Err(MetadataTransferStagingError::Capacity(format!(
                "reserved and quarantined bytes {total} exceed limit {}",
                self.limits.max_total_bytes
            )));
        }
        insert_intent(&state.connection, intent, StagingState::Intent, None)?;
        Ok(MetadataTransferStagingIntentOutcome::Created)
    }

    pub(crate) fn publish_artifact(
        &self,
        intent: &MetadataTransferStagingIntent,
        artifact: &[u8],
    ) -> Result<MetadataTransferStagingReceipt, MetadataTransferStagingError> {
        self.validate_intent_limits(intent)?;
        if u64::try_from(artifact.len()).ok() != Some(intent.artifact_length)
            || checksum::sha256::digest(artifact) != intent.artifact_digest
        {
            return Err(MetadataTransferStagingError::ArtifactMismatch);
        }
        let mut state = self.lock_state()?;
        let existing =
            load_staging_row(&state.connection, intent.pg_id, intent.staging_generation)?
                .ok_or_else(|| {
                    MetadataTransferStagingError::IntentConflict(
                        "artifact publication has no durable intent".to_owned(),
                    )
                })?;
        require_exact_intent(&existing.intent, intent)?;
        match existing.state {
            StagingState::Tombstoned => {
                return Err(MetadataTransferStagingError::GenerationRetired)
            }
            StagingState::Published | StagingState::Imported => {
                validate_artifact_file(&self.artifact_path(intent), intent)?;
                let receipt = existing.publication_receipt.ok_or_else(|| {
                    MetadataTransferStagingError::Invariant(
                        "published staging row has no receipt".to_owned(),
                    )
                })?;
                let delta = load_evidence_delta(
                    &state.connection,
                    intent.pg_id,
                    intent.staging_generation,
                    0,
                )?
                .ok_or_else(|| {
                    MetadataTransferStagingError::Invariant(
                        "published staging row has no durable evidence delta".to_owned(),
                    )
                })?;
                validate_staging_evidence(&receipt, intent, 0, &delta.actor)?;
                if delta.bytes != receipt {
                    return Err(MetadataTransferStagingError::Invariant(
                        "staging publication receipt and evidence delta differ".to_owned(),
                    ));
                }
                return Ok(MetadataTransferStagingReceipt { bytes: receipt });
            }
            StagingState::Intent => {}
        }

        self.publish_artifact_file(intent, artifact)?;
        let receipt = encode_staging_evidence(&self.identity, intent, 0);
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| {
                MetadataTransferStagingError::sql("begin staging publication", source)
            })?;
        let changed = transaction
            .execute(
                "UPDATE staging_intents SET state = 1, publication_receipt = ?1 \
                 WHERE pg_id = ?2 AND staging_generation = ?3 AND state = 0",
                params![
                    &receipt,
                    i64::from(intent.pg_id.get()),
                    to_sql_u64(intent.staging_generation)?
                ],
            )
            .map_err(|source| {
                MetadataTransferStagingError::sql("publish staged artifact", source)
            })?;
        if changed != 1 {
            return Err(MetadataTransferStagingError::Invariant(
                "staging publication lost its exact intent".to_owned(),
            ));
        }
        insert_evidence_delta(&transaction, &self.identity, intent, 0, &receipt)?;
        transaction.commit().map_err(|source| {
            MetadataTransferStagingError::sql("commit staging publication", source)
        })?;
        Ok(MetadataTransferStagingReceipt { bytes: receipt })
    }

    pub(crate) fn mark_imported(
        &self,
        intent: &MetadataTransferStagingIntent,
    ) -> Result<(), MetadataTransferStagingError> {
        let state = self.lock_state()?;
        let existing =
            load_staging_row(&state.connection, intent.pg_id, intent.staging_generation)?
                .ok_or_else(|| {
                    MetadataTransferStagingError::IntentConflict(
                        "import completion has no durable staged artifact".to_owned(),
                    )
                })?;
        require_exact_intent(&existing.intent, intent)?;
        match existing.state {
            StagingState::Imported => Ok(()),
            StagingState::Published => {
                validate_artifact_file(&self.artifact_path(intent), intent)?;
                let changed = state
                    .connection
                    .execute(
                        "UPDATE staging_intents SET state = 2 \
                         WHERE pg_id = ?1 AND staging_generation = ?2 AND state = 1",
                        params![
                            i64::from(intent.pg_id.get()),
                            to_sql_u64(intent.staging_generation)?
                        ],
                    )
                    .map_err(|source| {
                        MetadataTransferStagingError::sql("record staged artifact import", source)
                    })?;
                if changed != 1 {
                    return Err(MetadataTransferStagingError::Invariant(
                        "staging import lost its published artifact".to_owned(),
                    ));
                }
                Ok(())
            }
            StagingState::Intent => Err(MetadataTransferStagingError::IntentConflict(
                "artifact has not been durably published".to_owned(),
            )),
            StagingState::Tombstoned => Err(MetadataTransferStagingError::GenerationRetired),
        }
    }

    pub(crate) fn read_artifact(
        &self,
        intent: &MetadataTransferStagingIntent,
    ) -> Result<Vec<u8>, MetadataTransferStagingError> {
        let state = self.lock_state()?;
        let existing =
            load_staging_row(&state.connection, intent.pg_id, intent.staging_generation)?
                .ok_or_else(|| {
                    MetadataTransferStagingError::IntentConflict(
                        "artifact read has no durable staging row".to_owned(),
                    )
                })?;
        require_exact_intent(&existing.intent, intent)?;
        if !matches!(
            existing.state,
            StagingState::Published | StagingState::Imported
        ) {
            return Err(if existing.state == StagingState::Tombstoned {
                MetadataTransferStagingError::GenerationRetired
            } else {
                MetadataTransferStagingError::IntentConflict(
                    "artifact has not been durably published".to_owned(),
                )
            });
        }
        read_artifact_file(&self.artifact_path(intent), intent)
    }

    pub(crate) fn tombstone(
        &self,
        intent: &MetadataTransferStagingIntent,
    ) -> Result<MetadataTransferStagingReceipt, MetadataTransferStagingError> {
        self.validate_intent_limits(intent)?;
        let mut state = self.lock_state()?;
        if finalized_floor(&state.connection, intent.pg_id)? >= intent.staging_generation {
            return Err(MetadataTransferStagingError::GenerationRetired);
        }
        if let Some(existing) =
            load_staging_row(&state.connection, intent.pg_id, intent.staging_generation)?
        {
            require_exact_intent(&existing.intent, intent)?;
            if existing.state == StagingState::Tombstoned {
                let receipt = load_evidence_delta(
                    &state.connection,
                    intent.pg_id,
                    intent.staging_generation,
                    1,
                )?
                .ok_or_else(|| {
                    MetadataTransferStagingError::Invariant(
                        "tombstoned staging row has no durable evidence".to_owned(),
                    )
                })?;
                validate_staging_evidence(&receipt.bytes, intent, 1, &receipt.actor)?;
                remove_artifact_if_present(&self.artifact_path(intent))?;
                sync_directory(&self.artifacts_dir, "sync replayed staging tombstone")?;
                return Ok(MetadataTransferStagingReceipt {
                    bytes: receipt.bytes,
                });
            }
        } else {
            let (entry_count, _) = staging_capacity(&state.connection)?;
            if entry_count >= self.limits.max_entries {
                return Err(MetadataTransferStagingError::Capacity(format!(
                    "entry limit {} reached",
                    self.limits.max_entries
                )));
            }
        }
        let receipt = encode_staging_evidence(&self.identity, intent, 1);
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| {
                MetadataTransferStagingError::sql("begin staging tombstone", source)
            })?;
        transaction
            .execute(
                "INSERT INTO staging_intents (\
                    pg_id, transition_epoch, source_epoch, source_acting_set, destination_acting_set,\
                    staging_generation, artifact_digest, artifact_length, artifact_format_version,\
                    state, publication_receipt\
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 3, NULL)\
                 ON CONFLICT(pg_id, staging_generation) DO UPDATE SET state = 3",
                params![
                    i64::from(intent.pg_id.get()),
                    to_sql_u64(intent.transition_epoch.get())?,
                    to_sql_u64(intent.source_epoch.get())?,
                    encode_acting_set(&intent.source_acting_set),
                    encode_acting_set(&intent.destination_acting_set),
                    to_sql_u64(intent.staging_generation)?,
                    &intent.artifact_digest[..],
                    to_sql_u64(intent.artifact_length)?,
                    i64::from(intent.artifact_format_version),
                ],
            )
            .map_err(|source| {
                MetadataTransferStagingError::sql("record staging tombstone", source)
            })?;
        insert_evidence_delta(&transaction, &self.identity, intent, 1, &receipt)?;
        transaction.commit().map_err(|source| {
            MetadataTransferStagingError::sql("commit staging tombstone", source)
        })?;
        remove_artifact_if_present(&self.artifact_path(intent))?;
        sync_directory(&self.artifacts_dir, "sync staged artifact removal")?;
        Ok(MetadataTransferStagingReceipt { bytes: receipt })
    }

    pub(crate) fn advance_finalized_floor(
        &self,
        pg_id: PgId,
        staging_generation: u64,
    ) -> Result<(), MetadataTransferStagingError> {
        if staging_generation == 0 {
            return Err(MetadataTransferStagingError::Invariant(
                "staging finalized floor must be nonzero".to_owned(),
            ));
        }
        let state = self.lock_state()?;
        let current = finalized_floor(&state.connection, pg_id)?;
        if staging_generation < current {
            return Err(MetadataTransferStagingError::Invariant(format!(
                "staging finalized floor regressed from {current} to {staging_generation}"
            )));
        }
        let live: i64 = state
            .connection
            .query_row(
                "SELECT COUNT(*) FROM staging_intents \
                 WHERE pg_id = ?1 AND staging_generation <= ?2 AND state != 3",
                params![i64::from(pg_id.get()), to_sql_u64(staging_generation)?],
                |row| row.get(0),
            )
            .map_err(|source| {
                MetadataTransferStagingError::sql("check staging finalized floor", source)
            })?;
        if live != 0 {
            return Err(MetadataTransferStagingError::IntentConflict(
                "finalized floor crosses a non-tombstoned staging generation".to_owned(),
            ));
        }
        state
            .connection
            .execute(
                "INSERT INTO staging_finalized_floors (pg_id, staging_generation) VALUES (?1, ?2)\
                 ON CONFLICT(pg_id) DO UPDATE SET staging_generation = excluded.staging_generation",
                params![i64::from(pg_id.get()), to_sql_u64(staging_generation)?],
            )
            .map_err(|source| {
                MetadataTransferStagingError::sql("advance staging finalized floor", source)
            })?;
        Ok(())
    }

    #[cfg(test)]
    fn unacknowledged_evidence_count(&self) -> usize {
        let state = self.lock_state().unwrap();
        state
            .connection
            .query_row(
                "SELECT COUNT(*) FROM staging_evidence_deltas WHERE acknowledged = 0",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|value| usize::try_from(value).unwrap())
            .unwrap()
    }

    fn lock_state(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, MetadataTransferStagingState>, MetadataTransferStagingError>
    {
        self.state.lock().map_err(|_| {
            MetadataTransferStagingError::Invariant("staging-store lock poisoned".to_owned())
        })
    }

    fn validate_intent_limits(
        &self,
        intent: &MetadataTransferStagingIntent,
    ) -> Result<(), MetadataTransferStagingError> {
        validate_staging_intent_shape(intent)?;
        if intent.artifact_length > self.limits.max_artifact_bytes {
            return Err(MetadataTransferStagingError::Capacity(format!(
                "artifact length {} exceeds limit {}",
                intent.artifact_length, self.limits.max_artifact_bytes
            )));
        }
        let _ = to_sql_u64(intent.transition_epoch.get())?;
        let _ = to_sql_u64(intent.source_epoch.get())?;
        let _ = to_sql_u64(intent.staging_generation)?;
        Ok(())
    }

    fn artifact_path(&self, intent: &MetadataTransferStagingIntent) -> PathBuf {
        self.artifacts_dir.join(intent.artifact_file_name())
    }

    fn publish_artifact_file(
        &self,
        intent: &MetadataTransferStagingIntent,
        artifact: &[u8],
    ) -> Result<(), MetadataTransferStagingError> {
        let final_path = self.artifact_path(intent);
        if final_path
            .try_exists()
            .map_err(|source| MetadataTransferStagingError::io("inspect staged artifact", source))?
        {
            validate_artifact_file(&final_path, intent)?;
            sync_artifact_file(&final_path)?;
            return self.sync_artifacts_before_publication_commit(
                "sync recovered staged artifact publication",
            );
        }
        let sequence = self.temp_sequence.fetch_add(1, Ordering::Relaxed);
        let temp_path = self.artifacts_dir.join(format!(
            ".{}.tmp-{}-{sequence}",
            intent.artifact_file_name(),
            std::process::id()
        ));
        let result = (|| {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&temp_path)
                .map_err(|source| {
                    MetadataTransferStagingError::io(
                        "create staged artifact temporary file",
                        source,
                    )
                })?;
            file.write_all(artifact).map_err(|source| {
                MetadataTransferStagingError::io("write staged artifact", source)
            })?;
            file.sync_all().map_err(|source| {
                MetadataTransferStagingError::io("sync staged artifact", source)
            })?;
            validate_open_artifact(&mut file, intent)?;
            fs::rename(&temp_path, &final_path).map_err(|source| {
                MetadataTransferStagingError::io("publish staged artifact", source)
            })?;
            self.sync_artifacts_before_publication_commit("sync staged artifact publication")
        })();
        if result.is_err() {
            let _ = fs::remove_file(temp_path);
        }
        result
    }

    fn sync_artifacts_before_publication_commit(
        &self,
        context: &'static str,
    ) -> Result<(), MetadataTransferStagingError> {
        sync_directory(&self.artifacts_dir, context)?;
        if let Some(observer) = &self.publication_durability_observer {
            observer();
        }
        Ok(())
    }

    fn reconcile_startup_inventory(&mut self) -> Result<(), MetadataTransferStagingError> {
        let mut state = self.lock_state()?;
        let rows = load_all_staging_rows(&state.connection, self.limits.max_entries + 1)?;
        if rows.len() > self.limits.max_entries {
            return Err(MetadataTransferStagingError::Capacity(format!(
                "catalogue has {} entries, limit is {}",
                rows.len(),
                self.limits.max_entries
            )));
        }
        let mut known = BTreeMap::new();
        let mut reserved_bytes = 0u64;
        for row in &rows {
            self.validate_intent_limits(&row.intent)?;
            if row.state != StagingState::Tombstoned {
                reserved_bytes = reserved_bytes
                    .checked_add(row.intent.artifact_length)
                    .ok_or_else(|| {
                        MetadataTransferStagingError::Capacity(
                            "catalogue byte accounting overflowed".to_owned(),
                        )
                    })?;
            }
            known.insert(row.intent.artifact_file_name(), row);
        }
        validate_auxiliary_catalogue(&state.connection, &rows, self.limits.max_entries)?;

        let mut interrupted = Vec::new();
        let mut unexplained = Vec::new();
        let mut recovered_publications = Vec::new();
        let mut tombstoned_artifacts = Vec::new();
        let mut found = BTreeSet::new();
        let mut unexplained_bytes = 0u64;
        let artifact_inventory_limit = self.limits.max_entries.saturating_mul(2).saturating_add(1);
        for entry in read_bounded_directory(&self.artifacts_dir, artifact_inventory_limit)? {
            let name = entry.file_name().into_string().map_err(|_| {
                MetadataTransferStagingError::Invariant(
                    "staging artifact filename is not UTF-8".to_owned(),
                )
            })?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|source| {
                MetadataTransferStagingError::io("inspect staging artifact inventory", source)
            })?;
            if !file_type.is_file() {
                return Err(MetadataTransferStagingError::Invariant(format!(
                    "staging artifact inventory entry {name} is not a regular file"
                )));
            }
            let metadata = entry.metadata().map_err(|source| {
                MetadataTransferStagingError::io("inspect staging artifact inventory", source)
            })?;
            if name.starts_with('.') && name.contains(".tmp-") {
                interrupted.push(path);
                continue;
            }
            let Some(row) = known.get(&name) else {
                unexplained_bytes =
                    unexplained_bytes
                        .checked_add(metadata.len())
                        .ok_or_else(|| {
                            MetadataTransferStagingError::Capacity(
                                "staging unexplained byte accounting overflowed".to_owned(),
                            )
                        })?;
                unexplained.push((path, name));
                continue;
            };
            found.insert(name);
            match row.state {
                StagingState::Intent => {
                    validate_artifact_file(&path, &row.intent)?;
                    recovered_publications.push((*row).clone());
                }
                StagingState::Published | StagingState::Imported => {
                    validate_artifact_file(&path, &row.intent)?;
                }
                StagingState::Tombstoned => {
                    tombstoned_artifacts.push(path);
                }
            }
        }
        for row in &rows {
            match row.state {
                StagingState::Published | StagingState::Imported => {
                    if !found.contains(&row.intent.artifact_file_name()) {
                        return Err(MetadataTransferStagingError::Invariant(format!(
                            "published staged artifact {} is missing",
                            row.intent.artifact_file_name()
                        )));
                    }
                }
                StagingState::Tombstoned | StagingState::Intent => {}
            }
        }
        let existing_quarantined_bytes = quarantine_inventory_bytes(
            &self.quarantine_dir,
            self.limits.max_entries.saturating_add(1),
        )?;
        let quarantined_bytes = existing_quarantined_bytes
            .checked_add(unexplained_bytes)
            .ok_or_else(|| {
                MetadataTransferStagingError::Capacity(
                    "staging quarantine byte accounting overflowed".to_owned(),
                )
            })?;
        let total = reserved_bytes
            .checked_add(quarantined_bytes)
            .ok_or_else(|| {
                MetadataTransferStagingError::Capacity(
                    "startup staging byte accounting overflowed".to_owned(),
                )
            })?;
        if total > self.limits.max_total_bytes {
            return Err(MetadataTransferStagingError::Capacity(format!(
                "startup staged and quarantined bytes {total} exceed limit {}",
                self.limits.max_total_bytes
            )));
        }

        let inventory_changed =
            !interrupted.is_empty() || !unexplained.is_empty() || !tombstoned_artifacts.is_empty();
        for path in interrupted {
            fs::remove_file(path).map_err(|source| {
                MetadataTransferStagingError::io("remove interrupted staged artifact", source)
            })?;
        }
        for (path, name) in unexplained {
            let quarantine_path = self.quarantine_dir.join(name);
            reject_path_if_present(&quarantine_path, "inspect staging quarantine target")?;
            fs::rename(path, quarantine_path).map_err(|source| {
                MetadataTransferStagingError::io("quarantine unexplained staged artifact", source)
            })?;
        }
        for row in &recovered_publications {
            let path = self.artifact_path(&row.intent);
            sync_artifact_file(&path)?;
        }
        if !recovered_publications.is_empty() {
            self.sync_artifacts_before_publication_commit(
                "sync recovered startup staging publications",
            )?;
        }
        for row in recovered_publications {
            let receipt = encode_staging_evidence(&self.identity, &row.intent, 0);
            let transaction = state
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|source| {
                    MetadataTransferStagingError::sql("begin recovered staging publication", source)
                })?;
            let changed = transaction
                .execute(
                    "UPDATE staging_intents SET state = 1, publication_receipt = ?1 \
                     WHERE pg_id = ?2 AND staging_generation = ?3 AND state = 0",
                    params![
                        &receipt,
                        i64::from(row.intent.pg_id.get()),
                        to_sql_u64(row.intent.staging_generation)?
                    ],
                )
                .map_err(|source| {
                    MetadataTransferStagingError::sql("recover staged artifact publication", source)
                })?;
            if changed != 1 {
                return Err(MetadataTransferStagingError::Invariant(
                    "recovered staging publication lost its exact intent".to_owned(),
                ));
            }
            insert_evidence_delta(&transaction, &self.identity, &row.intent, 0, &receipt)?;
            transaction.commit().map_err(|source| {
                MetadataTransferStagingError::sql("commit recovered staging publication", source)
            })?;
        }
        for path in tombstoned_artifacts {
            fs::remove_file(path).map_err(|source| {
                MetadataTransferStagingError::io(
                    "finish tombstoned staged artifact removal",
                    source,
                )
            })?;
        }
        if inventory_changed {
            sync_directory(&self.artifacts_dir, "sync reconciled staging artifacts")?;
            sync_directory(&self.quarantine_dir, "sync staging quarantine")?;
        }
        state.quarantined_bytes = quarantined_bytes;
        Ok(())
    }
}

fn configure_catalogue(connection: &Connection) -> Result<(), MetadataTransferStagingError> {
    connection
        .execute_batch(
            "PRAGMA trusted_schema = OFF;\
             PRAGMA foreign_keys = ON;\
             PRAGMA journal_mode = WAL;\
             PRAGMA synchronous = FULL;",
        )
        .map_err(|source| MetadataTransferStagingError::sql("configure staging catalogue", source))
}

fn initialize_or_validate_unmarked_catalogue(
    connection: &Connection,
) -> Result<(), MetadataTransferStagingError> {
    let version: u32 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|source| {
            MetadataTransferStagingError::sql("read staging catalogue version", source)
        })?;
    match version {
        0 => {
            let table_count: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
                    [],
                    |row| row.get(0),
                )
                .map_err(|source| {
                    MetadataTransferStagingError::sql("inspect empty staging catalogue", source)
                })?;
            if table_count != 0 {
                return Err(MetadataTransferStagingError::Invariant(
                    "unversioned staging catalogue is not empty".to_owned(),
                ));
            }
            connection
                .execute_batch(STAGING_STORE_SCHEMA_V1)
                .map_err(|source| {
                    MetadataTransferStagingError::sql("initialize staging catalogue", source)
                })?;
        }
        value if value == u32::from(STAGING_STORE_FORMAT_VERSION) => {}
        other => {
            return Err(MetadataTransferStagingError::UnsupportedCatalogueVersion(
                other,
            ))
        }
    }
    validate_schema_catalogue(connection)
}

fn validate_initialized_catalogue(
    connection: &Connection,
) -> Result<(), MetadataTransferStagingError> {
    let version: u32 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|source| {
            MetadataTransferStagingError::sql("read initialized staging catalogue version", source)
        })?;
    match version {
        value if value == u32::from(STAGING_STORE_FORMAT_VERSION) => {
            validate_schema_catalogue(connection)
        }
        0 => Err(MetadataTransferStagingError::Invariant(
            "initialized staging store has an unversioned catalogue".to_owned(),
        )),
        other => Err(MetadataTransferStagingError::UnsupportedCatalogueVersion(
            other,
        )),
    }
}

fn validate_schema_catalogue(connection: &Connection) -> Result<(), MetadataTransferStagingError> {
    let expected = Connection::open_in_memory().map_err(|source| {
        MetadataTransferStagingError::sql("open expected staging catalogue", source)
    })?;
    expected
        .execute_batch(STAGING_STORE_SCHEMA_V1)
        .map_err(|source| {
            MetadataTransferStagingError::sql("build expected staging catalogue", source)
        })?;
    if schema_catalogue(connection)? != schema_catalogue(&expected)? {
        return Err(MetadataTransferStagingError::Invariant(
            "staging catalogue schema does not match format v1".to_owned(),
        ));
    }
    Ok(())
}

fn schema_catalogue(
    connection: &Connection,
) -> Result<Vec<(String, String, String)>, MetadataTransferStagingError> {
    let mut statement = connection
        .prepare(
            "SELECT type, name, COALESCE(sql, '') FROM sqlite_schema \
             WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name",
        )
        .map_err(|source| {
            MetadataTransferStagingError::sql("prepare staging schema inspection", source)
        })?;
    let rows = statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .map_err(|source| MetadataTransferStagingError::sql("query staging schema", source))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| MetadataTransferStagingError::sql("read staging schema", source))
}

fn insert_intent(
    connection: &Connection,
    intent: &MetadataTransferStagingIntent,
    state: StagingState,
    receipt: Option<&[u8]>,
) -> Result<(), MetadataTransferStagingError> {
    connection
        .execute(
            "INSERT INTO staging_intents (\
                pg_id, transition_epoch, source_epoch, source_acting_set, destination_acting_set,\
                staging_generation, artifact_digest, artifact_length, artifact_format_version,\
                state, publication_receipt\
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                i64::from(intent.pg_id.get()),
                to_sql_u64(intent.transition_epoch.get())?,
                to_sql_u64(intent.source_epoch.get())?,
                encode_acting_set(&intent.source_acting_set),
                encode_acting_set(&intent.destination_acting_set),
                to_sql_u64(intent.staging_generation)?,
                &intent.artifact_digest[..],
                to_sql_u64(intent.artifact_length)?,
                i64::from(intent.artifact_format_version),
                state as i64,
                receipt,
            ],
        )
        .map_err(|source| MetadataTransferStagingError::sql("insert staging intent", source))?;
    Ok(())
}

fn load_staging_row(
    connection: &Connection,
    pg_id: PgId,
    staging_generation: u64,
) -> Result<Option<StagingRow>, MetadataTransferStagingError> {
    connection
        .query_row(
            "SELECT transition_epoch, source_epoch, source_acting_set, destination_acting_set,\
                    artifact_digest, artifact_length, artifact_format_version, state, publication_receipt \
             FROM staging_intents WHERE pg_id = ?1 AND staging_generation = ?2",
            params![i64::from(pg_id.get()), to_sql_u64(staging_generation)?],
            |row| decode_staging_row(pg_id, staging_generation, row),
        )
        .optional()
        .map_err(|source| MetadataTransferStagingError::sql("load staging intent", source))?
        .transpose()
}

fn load_all_staging_rows(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<StagingRow>, MetadataTransferStagingError> {
    let mut statement = connection
        .prepare(
            "SELECT pg_id, staging_generation, transition_epoch, source_epoch, source_acting_set,\
                    destination_acting_set, artifact_digest, artifact_length, artifact_format_version,\
                    state, publication_receipt \
             FROM staging_intents ORDER BY pg_id, staging_generation LIMIT ?1",
        )
        .map_err(|source| {
            MetadataTransferStagingError::sql("prepare staging inventory", source)
        })?;
    let rows = statement
        .query_map([i64::try_from(limit).unwrap_or(i64::MAX)], |row| {
            let pg_id = PgId::new(row.get::<_, u32>(0)?);
            let generation = row.get::<_, u64>(1)?;
            decode_staging_row_offset(pg_id, generation, row, 2)
        })
        .map_err(|source| MetadataTransferStagingError::sql("query staging inventory", source))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| MetadataTransferStagingError::sql("read staging inventory", source))?
        .into_iter()
        .collect()
}

fn decode_staging_row(
    pg_id: PgId,
    staging_generation: u64,
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<Result<StagingRow, MetadataTransferStagingError>> {
    decode_staging_row_offset(pg_id, staging_generation, row, 0)
}

fn decode_staging_row_offset(
    pg_id: PgId,
    staging_generation: u64,
    row: &rusqlite::Row<'_>,
    offset: usize,
) -> rusqlite::Result<Result<StagingRow, MetadataTransferStagingError>> {
    let transition_epoch = row.get::<_, u64>(offset)?;
    let source_epoch = row.get::<_, u64>(offset + 1)?;
    let source_acting_set = row.get::<_, Vec<u8>>(offset + 2)?;
    let destination_acting_set = row.get::<_, Vec<u8>>(offset + 3)?;
    let artifact_digest = row.get::<_, Vec<u8>>(offset + 4)?;
    let artifact_length = row.get::<_, u64>(offset + 5)?;
    let artifact_format_version = row.get::<_, u16>(offset + 6)?;
    let state = row.get::<_, i64>(offset + 7)?;
    let publication_receipt = row.get::<_, Option<Vec<u8>>>(offset + 8)?;
    Ok((|| {
        let artifact_digest: [u8; DIGEST_LEN] = artifact_digest.try_into().map_err(|_| {
            MetadataTransferStagingError::Invariant(
                "staging artifact digest has invalid length".to_owned(),
            )
        })?;
        let intent = MetadataTransferStagingIntent {
            pg_id,
            transition_epoch: ClusterEpoch::new(transition_epoch).ok_or_else(|| {
                MetadataTransferStagingError::Invariant(
                    "staging transition epoch is zero".to_owned(),
                )
            })?,
            source_epoch: ClusterEpoch::new(source_epoch).ok_or_else(|| {
                MetadataTransferStagingError::Invariant("staging source epoch is zero".to_owned())
            })?,
            source_acting_set: decode_acting_set(&source_acting_set)?,
            destination_acting_set: decode_acting_set(&destination_acting_set)?,
            staging_generation,
            artifact_digest,
            artifact_length,
            artifact_format_version,
        };
        validate_staging_intent_shape(&intent)?;
        Ok(StagingRow {
            intent,
            state: StagingState::decode(state)?,
            publication_receipt,
        })
    })())
}

fn require_exact_intent(
    actual: &MetadataTransferStagingIntent,
    expected: &MetadataTransferStagingIntent,
) -> Result<(), MetadataTransferStagingError> {
    if actual != expected {
        return Err(MetadataTransferStagingError::IntentConflict(
            "same PG/generation has a different transition or artifact tuple".to_owned(),
        ));
    }
    Ok(())
}

fn validate_staging_intent_shape(
    intent: &MetadataTransferStagingIntent,
) -> Result<(), MetadataTransferStagingError> {
    if intent.artifact_format_version != METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION {
        return Err(MetadataTransferStagingError::Invariant(format!(
            "unsupported staged artifact format version {}",
            intent.artifact_format_version
        )));
    }
    if intent.source_epoch >= intent.transition_epoch
        || intent.staging_generation != intent.transition_epoch.get()
        || intent.source_acting_set.is_empty()
        || intent.source_acting_set.len() != intent.destination_acting_set.len()
    {
        return Err(MetadataTransferStagingError::Invariant(
            "staging intent has invalid transition epochs or acting-set shape".to_owned(),
        ));
    }
    let source = intent
        .source_acting_set
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let destination = intent
        .destination_acting_set
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let changed = intent
        .source_acting_set
        .iter()
        .zip(&intent.destination_acting_set)
        .filter(|(source, destination)| source != destination)
        .count();
    if source.len() != intent.source_acting_set.len()
        || destination.len() != intent.destination_acting_set.len()
        || changed != 1
    {
        return Err(MetadataTransferStagingError::Invariant(
            "staging intent is not an exact one-node acting-set substitution".to_owned(),
        ));
    }
    Ok(())
}

fn insert_evidence_delta(
    connection: &Connection,
    actor: &MetadataTransferStagingNodeIdentity,
    intent: &MetadataTransferStagingIntent,
    kind: i64,
    bytes: &[u8],
) -> Result<(), MetadataTransferStagingError> {
    let changed = connection
        .execute(
            "INSERT INTO staging_evidence_deltas (\
                pg_id, staging_generation, evidence_kind, actor_node_id,\
                actor_node_incarnation, actor_endpoint, evidence_bytes\
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
             ON CONFLICT(pg_id, staging_generation, evidence_kind) DO UPDATE SET \
                evidence_bytes = excluded.evidence_bytes \
             WHERE staging_evidence_deltas.actor_node_id = excluded.actor_node_id \
               AND staging_evidence_deltas.actor_node_incarnation = excluded.actor_node_incarnation \
               AND staging_evidence_deltas.actor_endpoint = excluded.actor_endpoint \
               AND staging_evidence_deltas.evidence_bytes = excluded.evidence_bytes",
            params![
                i64::from(intent.pg_id.get()),
                to_sql_u64(intent.staging_generation)?,
                kind,
                i64::from(actor.node_id.as_u32()),
                to_sql_u64(actor.node_incarnation)?,
                &actor.endpoint,
                bytes,
            ],
        )
        .map_err(|source| {
            MetadataTransferStagingError::sql("record staging evidence delta", source)
        })?;
    if changed != 1 {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence replay differs from durable evidence".to_owned(),
        ));
    }
    Ok(())
}

fn load_evidence_delta(
    connection: &Connection,
    pg_id: PgId,
    staging_generation: u64,
    kind: i64,
) -> Result<Option<StagingEvidenceDelta>, MetadataTransferStagingError> {
    connection
        .query_row(
            "SELECT actor_node_id, actor_node_incarnation, actor_endpoint, evidence_bytes \
             FROM staging_evidence_deltas \
             WHERE pg_id = ?1 AND staging_generation = ?2 AND evidence_kind = ?3",
            params![
                i64::from(pg_id.get()),
                to_sql_u64(staging_generation)?,
                kind
            ],
            |row| {
                Ok((
                    row.get::<_, u32>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                ))
            },
        )
        .optional()
        .map_err(|source| MetadataTransferStagingError::sql("load staging evidence delta", source))?
        .map(|(node_id, node_incarnation, endpoint, bytes)| {
            Ok(StagingEvidenceDelta {
                actor: MetadataTransferStagingNodeIdentity::new(
                    NodeId::new(node_id),
                    node_incarnation,
                    endpoint,
                )?,
                bytes,
            })
        })
        .transpose()
}

fn validate_auxiliary_catalogue(
    connection: &Connection,
    rows: &[StagingRow],
    max_entries: usize,
) -> Result<(), MetadataTransferStagingError> {
    let integrity: String = connection
        .query_row("PRAGMA quick_check(1)", [], |row| row.get(0))
        .map_err(|source| {
            MetadataTransferStagingError::sql("check staging catalogue integrity", source)
        })?;
    if integrity != "ok" {
        return Err(MetadataTransferStagingError::Invariant(format!(
            "staging catalogue integrity check failed: {integrity}"
        )));
    }

    let evidence_limit = max_entries.saturating_mul(2);
    let mut statement = connection
        .prepare(
            "SELECT pg_id, staging_generation, evidence_kind, actor_node_id, \
                    actor_node_incarnation, actor_endpoint, evidence_bytes, acknowledged \
             FROM staging_evidence_deltas ORDER BY sequence LIMIT ?1",
        )
        .map_err(|source| {
            MetadataTransferStagingError::sql("prepare staging evidence inventory", source)
        })?;
    let evidence = statement
        .query_map(
            [i64::try_from(evidence_limit.saturating_add(1)).unwrap_or(i64::MAX)],
            |row| {
                Ok((
                    row.get::<_, u32>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, u32>(3)?,
                    row.get::<_, u64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Vec<u8>>(6)?,
                    row.get::<_, i64>(7)?,
                ))
            },
        )
        .map_err(|source| {
            MetadataTransferStagingError::sql("query staging evidence inventory", source)
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| {
            MetadataTransferStagingError::sql("read staging evidence inventory", source)
        })?;
    if evidence.len() > evidence_limit {
        return Err(MetadataTransferStagingError::Capacity(format!(
            "staging evidence inventory exceeds bounded limit {evidence_limit}"
        )));
    }
    for (
        pg_id,
        generation,
        kind,
        actor_node_id,
        actor_node_incarnation,
        actor_endpoint,
        bytes,
        acknowledged,
    ) in evidence
    {
        if bytes.is_empty()
            || bytes.len() > MAX_STAGING_EVIDENCE_BYTES
            || !matches!(acknowledged, 0 | 1)
        {
            return Err(MetadataTransferStagingError::Invariant(
                "staging evidence has invalid length or acknowledgement state".to_owned(),
            ));
        }
        let row = rows
            .iter()
            .find(|row| {
                row.intent.pg_id == PgId::new(pg_id) && row.intent.staging_generation == generation
            })
            .ok_or_else(|| {
                MetadataTransferStagingError::Invariant(
                    "staging evidence has no exact intent".to_owned(),
                )
            })?;
        let expected_kind = u8::try_from(kind).map_err(|_| {
            MetadataTransferStagingError::Invariant("staging evidence has invalid kind".to_owned())
        })?;
        let actor = MetadataTransferStagingNodeIdentity::new(
            NodeId::new(actor_node_id),
            actor_node_incarnation,
            actor_endpoint,
        )?;
        validate_staging_evidence(&bytes, &row.intent, expected_kind, &actor)?;
        match kind {
            0 if matches!(
                row.state,
                StagingState::Published | StagingState::Imported | StagingState::Tombstoned
            ) && row.publication_receipt.as_deref() == Some(bytes.as_slice()) => {}
            1 if row.state == StagingState::Tombstoned => {}
            _ => {
                return Err(MetadataTransferStagingError::Invariant(
                    "staging evidence contradicts its intent state".to_owned(),
                ))
            }
        }
    }
    for row in rows {
        if let Some(publication_receipt) = row.publication_receipt.as_deref() {
            let delta = load_evidence_delta(
                connection,
                row.intent.pg_id,
                row.intent.staging_generation,
                0,
            )?
            .ok_or_else(|| {
                MetadataTransferStagingError::Invariant(
                    "staging publication receipt has no durable evidence".to_owned(),
                )
            })?;
            validate_staging_evidence(publication_receipt, &row.intent, 0, &delta.actor)?;
            if delta.bytes != publication_receipt {
                return Err(MetadataTransferStagingError::Invariant(
                    "staging publication receipt and durable evidence differ".to_owned(),
                ));
            }
        }
        let required_kind = match row.state {
            StagingState::Intent => continue,
            StagingState::Published | StagingState::Imported => 0,
            StagingState::Tombstoned => 1,
        };
        let receipt = load_evidence_delta(
            connection,
            row.intent.pg_id,
            row.intent.staging_generation,
            required_kind,
        )?
        .ok_or_else(|| {
            MetadataTransferStagingError::Invariant(
                "staging intent is missing required durable evidence".to_owned(),
            )
        })?;
        validate_staging_evidence(
            &receipt.bytes,
            &row.intent,
            u8::try_from(required_kind).unwrap(),
            &receipt.actor,
        )?;
    }

    let live_below_floor: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM staging_intents AS intent \
             JOIN staging_finalized_floors AS floor USING (pg_id) \
             WHERE intent.staging_generation <= floor.staging_generation AND intent.state != 3",
            [],
            |row| row.get(0),
        )
        .map_err(|source| {
            MetadataTransferStagingError::sql("validate staging finalized floors", source)
        })?;
    if live_below_floor != 0 {
        return Err(MetadataTransferStagingError::Invariant(
            "staging finalized floor crosses live intent state".to_owned(),
        ));
    }

    let inflight = connection
        .query_row(
            "SELECT previous_generation, previous_apply_receipt_digest, generation, \
                    operation_payload, page_digest, apply_receipt \
             FROM staging_evidence_inflight_page WHERE singleton = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, u64>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, Option<Vec<u8>>>(5)?,
                ))
            },
        )
        .optional()
        .map_err(|source| {
            MetadataTransferStagingError::sql("read staging in-flight evidence page", source)
        })?;
    if let Some((previous, previous_digest, generation, payload, digest, apply_receipt)) = inflight
    {
        let previous_digest: [u8; DIGEST_LEN] = previous_digest.try_into().map_err(|_| {
            MetadataTransferStagingError::Invariant(
                "staging in-flight predecessor digest has invalid length".to_owned(),
            )
        })?;
        let digest: [u8; DIGEST_LEN] = digest.try_into().map_err(|_| {
            MetadataTransferStagingError::Invariant(
                "staging in-flight page digest has invalid length".to_owned(),
            )
        })?;
        if generation != previous.checked_add(1).unwrap_or(0)
            || payload.is_empty()
            || payload.len() > MAX_STAGING_EVIDENCE_PAGE_BYTES
            || checksum::sha256::digest(&payload) != digest
            || (previous == 0) != (previous_digest == [0; DIGEST_LEN])
            || apply_receipt.as_ref().is_some_and(|receipt| {
                receipt.is_empty() || receipt.len() > MAX_STAGING_EVIDENCE_BYTES
            })
        {
            return Err(MetadataTransferStagingError::Invariant(
                "staging in-flight evidence page is not canonical".to_owned(),
            ));
        }
    }
    Ok(())
}

fn finalized_floor(
    connection: &Connection,
    pg_id: PgId,
) -> Result<u64, MetadataTransferStagingError> {
    connection
        .query_row(
            "SELECT staging_generation FROM staging_finalized_floors WHERE pg_id = ?1",
            [i64::from(pg_id.get())],
            |row| row.get(0),
        )
        .optional()
        .map(|value| value.unwrap_or(0))
        .map_err(|source| MetadataTransferStagingError::sql("read staging finalized floor", source))
}

fn staging_capacity(connection: &Connection) -> Result<(usize, u64), MetadataTransferStagingError> {
    let (count, bytes): (i64, i64) = connection
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(CASE WHEN state != 3 THEN artifact_length ELSE 0 END), 0) \
             FROM staging_intents",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|source| {
            MetadataTransferStagingError::sql("measure staging capacity", source)
        })?;
    Ok((
        usize::try_from(count).map_err(|_| {
            MetadataTransferStagingError::Invariant("negative staging entry count".to_owned())
        })?,
        u64::try_from(bytes).map_err(|_| {
            MetadataTransferStagingError::Invariant("negative staging byte count".to_owned())
        })?,
    ))
}

fn encode_staging_evidence(
    identity: &MetadataTransferStagingNodeIdentity,
    intent: &MetadataTransferStagingIntent,
    kind: u8,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"ARGMIN-STAGING-EVIDENCE-V1\0");
    out.push(kind);
    out.extend_from_slice(&identity.node_id.as_u32().to_be_bytes());
    out.extend_from_slice(&identity.node_incarnation.to_be_bytes());
    put_bytes(&mut out, identity.endpoint.as_bytes());
    encode_staging_intent_evidence(&mut out, intent);
    out.push(0b0000_0111); // artifact, catalogue, and parent-directory fsync scope
    out
}

fn encode_staging_intent_evidence(out: &mut Vec<u8>, intent: &MetadataTransferStagingIntent) {
    out.extend_from_slice(&intent.pg_id.get().to_be_bytes());
    out.extend_from_slice(&intent.transition_epoch.get().to_be_bytes());
    out.extend_from_slice(&intent.source_epoch.get().to_be_bytes());
    put_bytes(out, &encode_acting_set(&intent.source_acting_set));
    put_bytes(out, &encode_acting_set(&intent.destination_acting_set));
    out.extend_from_slice(&intent.staging_generation.to_be_bytes());
    out.extend_from_slice(&intent.artifact_digest);
    out.extend_from_slice(&intent.artifact_length.to_be_bytes());
    out.extend_from_slice(&intent.artifact_format_version.to_be_bytes());
}

fn validate_staging_evidence(
    bytes: &[u8],
    intent: &MetadataTransferStagingIntent,
    expected_kind: u8,
    expected_actor: &MetadataTransferStagingNodeIdentity,
) -> Result<(), MetadataTransferStagingError> {
    const MAGIC: &[u8] = b"ARGMIN-STAGING-EVIDENCE-V1\0";
    let mut offset = MAGIC.len();
    if bytes.get(..offset) != Some(MAGIC) || bytes.get(offset) != Some(&expected_kind) {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence has invalid magic or kind".to_owned(),
        ));
    }
    offset += 1;
    let node_id = u32::from_be_bytes(take(bytes, &mut offset, 4)?.try_into().unwrap());
    let incarnation = u64::from_be_bytes(take(bytes, &mut offset, 8)?.try_into().unwrap());
    let endpoint_len = usize::try_from(u32::from_be_bytes(
        take(bytes, &mut offset, 4)?.try_into().unwrap(),
    ))
    .unwrap();
    let endpoint = std::str::from_utf8(take(bytes, &mut offset, endpoint_len)?).map_err(|_| {
        MetadataTransferStagingError::Invariant(
            "staging evidence has invalid node identity".to_owned(),
        )
    })?;
    if node_id != expected_actor.node_id.as_u32()
        || incarnation != expected_actor.node_incarnation
        || endpoint != expected_actor.endpoint
    {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence does not match its durable actor identity".to_owned(),
        ));
    }
    let mut expected = Vec::new();
    encode_staging_intent_evidence(&mut expected, intent);
    expected.push(0b0000_0111);
    if bytes.get(offset..) != Some(expected.as_slice()) {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence does not bind the exact intent and fsync scope".to_owned(),
        ));
    }
    Ok(())
}

fn take<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    length: usize,
) -> Result<&'a [u8], MetadataTransferStagingError> {
    let end = offset.checked_add(length).ok_or_else(|| {
        MetadataTransferStagingError::Invariant("staging evidence length overflowed".to_owned())
    })?;
    let value = bytes.get(*offset..end).ok_or_else(|| {
        MetadataTransferStagingError::Invariant("staging evidence is truncated".to_owned())
    })?;
    *offset = end;
    Ok(value)
}

fn encode_acting_set(acting_set: &[NodeId]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + acting_set.len() * 4);
    out.extend_from_slice(&u32::try_from(acting_set.len()).unwrap().to_be_bytes());
    for node_id in acting_set {
        out.extend_from_slice(&node_id.as_u32().to_be_bytes());
    }
    out
}

fn decode_acting_set(bytes: &[u8]) -> Result<Vec<NodeId>, MetadataTransferStagingError> {
    let count_bytes = bytes.get(..4).ok_or_else(|| {
        MetadataTransferStagingError::Invariant("staging acting set is truncated".to_owned())
    })?;
    let count = usize::try_from(u32::from_be_bytes(count_bytes.try_into().unwrap())).unwrap();
    let expected = 4usize
        .checked_add(count.checked_mul(4).ok_or_else(|| {
            MetadataTransferStagingError::Invariant("staging acting set overflows".to_owned())
        })?)
        .ok_or_else(|| {
            MetadataTransferStagingError::Invariant("staging acting set overflows".to_owned())
        })?;
    if count == 0 || bytes.len() != expected {
        return Err(MetadataTransferStagingError::Invariant(
            "staging acting set has invalid length".to_owned(),
        ));
    }
    let mut seen = BTreeSet::new();
    let mut acting_set = Vec::with_capacity(count);
    for chunk in bytes[4..].chunks_exact(4) {
        let node_id = NodeId::new(u32::from_be_bytes(chunk.try_into().unwrap()));
        if !seen.insert(node_id) {
            return Err(MetadataTransferStagingError::Invariant(
                "staging acting set contains a duplicate node".to_owned(),
            ));
        }
        acting_set.push(node_id);
    }
    Ok(acting_set)
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&u32::try_from(bytes.len()).unwrap().to_be_bytes());
    out.extend_from_slice(bytes);
}

fn validate_artifact_file(
    path: &Path,
    intent: &MetadataTransferStagingIntent,
) -> Result<(), MetadataTransferStagingError> {
    let mut file = open_regular_nofollow(path, "open staged artifact")?;
    validate_open_artifact(&mut file, intent)
}

fn validate_open_artifact(
    file: &mut File,
    intent: &MetadataTransferStagingIntent,
) -> Result<(), MetadataTransferStagingError> {
    let metadata = file
        .metadata()
        .map_err(|source| MetadataTransferStagingError::io("inspect staged artifact", source))?;
    if metadata.len() != intent.artifact_length {
        return Err(MetadataTransferStagingError::ArtifactMismatch);
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|source| MetadataTransferStagingError::io("rewind staged artifact", source))?;
    let mut bytes = Vec::with_capacity(usize::try_from(intent.artifact_length).unwrap_or(0));
    file.read_to_end(&mut bytes)
        .map_err(|source| MetadataTransferStagingError::io("read staged artifact", source))?;
    if checksum::sha256::digest(&bytes) != intent.artifact_digest {
        return Err(MetadataTransferStagingError::ArtifactMismatch);
    }
    Ok(())
}

fn read_artifact_file(
    path: &Path,
    intent: &MetadataTransferStagingIntent,
) -> Result<Vec<u8>, MetadataTransferStagingError> {
    let mut file = open_regular_nofollow(path, "open staged artifact for read")?;
    let mut bytes = Vec::with_capacity(usize::try_from(intent.artifact_length).unwrap_or(0));
    file.read_to_end(&mut bytes)
        .map_err(|source| MetadataTransferStagingError::io("read staged artifact", source))?;
    if u64::try_from(bytes.len()).ok() != Some(intent.artifact_length)
        || checksum::sha256::digest(&bytes) != intent.artifact_digest
    {
        return Err(MetadataTransferStagingError::ArtifactMismatch);
    }
    Ok(bytes)
}

fn sync_artifact_file(path: &Path) -> Result<(), MetadataTransferStagingError> {
    open_regular_nofollow(path, "open staged artifact for sync")?
        .sync_all()
        .map_err(|source| MetadataTransferStagingError::io("sync staged artifact", source))
}

fn remove_artifact_if_present(path: &Path) -> Result<(), MetadataTransferStagingError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(MetadataTransferStagingError::io(
            "remove staged artifact",
            source,
        )),
    }
}

fn read_bounded_directory(
    path: &Path,
    limit: usize,
) -> Result<Vec<fs::DirEntry>, MetadataTransferStagingError> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(path)
        .map_err(|source| MetadataTransferStagingError::io("read staging directory", source))?
    {
        entries.push(entry.map_err(|source| {
            MetadataTransferStagingError::io("read staging directory entry", source)
        })?);
        if entries.len() > limit {
            return Err(MetadataTransferStagingError::Capacity(format!(
                "staging directory exceeds bounded inventory limit {limit}"
            )));
        }
    }
    Ok(entries)
}

fn quarantine_inventory_bytes(
    path: &Path,
    limit: usize,
) -> Result<u64, MetadataTransferStagingError> {
    let mut bytes = 0u64;
    for entry in read_bounded_directory(path, limit)? {
        let file_type = entry.file_type().map_err(|source| {
            MetadataTransferStagingError::io("inspect staging quarantine", source)
        })?;
        if !file_type.is_file() {
            return Err(MetadataTransferStagingError::Invariant(
                "staging quarantine contains a non-regular entry".to_owned(),
            ));
        }
        let metadata = entry.metadata().map_err(|source| {
            MetadataTransferStagingError::io("inspect staging quarantine", source)
        })?;
        bytes = bytes.checked_add(metadata.len()).ok_or_else(|| {
            MetadataTransferStagingError::Capacity(
                "staging quarantine byte accounting overflowed".to_owned(),
            )
        })?;
    }
    Ok(bytes)
}

fn prepare_secure_directory(
    path: &Path,
    context: &'static str,
) -> Result<(), MetadataTransferStagingError> {
    reject_symlink_if_present(path, context)?;
    prepare_private_data_dir(path)
        .map_err(|source| MetadataTransferStagingError::io(context, source))?;
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| MetadataTransferStagingError::io(context, source))?;
    if !metadata.file_type().is_dir() || metadata.permissions().mode() & 0o777 != 0o700 {
        return Err(MetadataTransferStagingError::Invariant(format!(
            "{} is not a private real directory",
            path.display()
        )));
    }
    Ok(())
}

fn require_secure_directory(
    path: &Path,
    context: &'static str,
) -> Result<(), MetadataTransferStagingError> {
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|source| {
            if source.kind() == std::io::ErrorKind::NotFound {
                MetadataTransferStagingError::Invariant(format!(
                    "established staging store is missing {}",
                    path.display()
                ))
            } else {
                MetadataTransferStagingError::io(context, source)
            }
        })?;
    let metadata = directory
        .metadata()
        .map_err(|source| MetadataTransferStagingError::io(context, source))?;
    if !metadata.file_type().is_dir() || metadata.permissions().mode() & 0o777 != 0o700 {
        return Err(MetadataTransferStagingError::Invariant(format!(
            "{} is not a private real directory",
            path.display()
        )));
    }
    Ok(())
}

fn reject_symlink_if_present(
    path: &Path,
    context: &'static str,
) -> Result<(), MetadataTransferStagingError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(MetadataTransferStagingError::Invariant(format!(
                "{} must not be a symlink",
                path.display()
            )))
        }
        Ok(_) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(MetadataTransferStagingError::io(context, source)),
    }
}

fn reject_path_if_present(
    path: &Path,
    context: &'static str,
) -> Result<(), MetadataTransferStagingError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(MetadataTransferStagingError::Invariant(format!(
            "{} already exists",
            path.display()
        ))),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(MetadataTransferStagingError::io(context, source)),
    }
}

fn validate_catalogue_file_if_present(path: &Path) -> Result<(), MetadataTransferStagingError> {
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
    {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(MetadataTransferStagingError::io(
                "open existing staging catalogue without following links",
                source,
            ))
        }
    };
    let metadata = file.metadata().map_err(|source| {
        MetadataTransferStagingError::io("inspect existing staging catalogue", source)
    })?;
    if !metadata.file_type().is_file() {
        return Err(MetadataTransferStagingError::Invariant(format!(
            "{} is not a regular catalogue file",
            path.display()
        )));
    }
    Ok(())
}

fn require_catalogue_file(path: &Path) -> Result<(), MetadataTransferStagingError> {
    match open_regular_nofollow(path, "open initialized staging catalogue") {
        Ok(file) => {
            drop(file);
            Ok(())
        }
        Err(MetadataTransferStagingError::Io { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            Err(MetadataTransferStagingError::Invariant(
                "initialized staging store is missing its catalogue".to_owned(),
            ))
        }
        Err(error) => Err(error),
    }
}

fn validate_catalogue_files_if_present(path: &Path) -> Result<(), MetadataTransferStagingError> {
    validate_catalogue_file_if_present(path)?;
    for suffix in ["-journal", "-wal", "-shm"] {
        let mut sidecar = path.as_os_str().to_os_string();
        sidecar.push(suffix);
        validate_catalogue_file_if_present(Path::new(&sidecar))?;
    }
    Ok(())
}

fn open_regular_nofollow(
    path: &Path,
    context: &'static str,
) -> Result<File, MetadataTransferStagingError> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
        .map_err(|source| MetadataTransferStagingError::io(context, source))?;
    let metadata = file
        .metadata()
        .map_err(|source| MetadataTransferStagingError::io(context, source))?;
    if !metadata.file_type().is_file() {
        return Err(MetadataTransferStagingError::Invariant(format!(
            "{} is not a regular file",
            path.display()
        )));
    }
    Ok(file)
}

fn sync_directory(path: &Path, context: &'static str) -> Result<(), MetadataTransferStagingError> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| MetadataTransferStagingError::io(context, source))
}

fn sync_staging_parent_directory(
    data_dir: &Path,
    context: &'static str,
    point: StagingParentDirectorySyncPoint,
    injected_sync: Option<&StagingParentDirectorySync>,
) -> Result<(), MetadataTransferStagingError> {
    if let Some(injected_sync) = injected_sync {
        return injected_sync(data_dir, context, point);
    }
    sync_directory(data_dir, context)
}

fn establishment_marker_exists(data_dir: &Path) -> Result<bool, MetadataTransferStagingError> {
    let path = data_dir.join(ESTABLISHMENT_MARKER_FILE);
    match open_regular_nofollow(&path, "open staging establishment marker") {
        Ok(mut file) => {
            let mut bytes = Vec::with_capacity(ESTABLISHMENT_MARKER_LEN + 1);
            std::io::Read::by_ref(&mut file)
                .take(u64::try_from(ESTABLISHMENT_MARKER_LEN + 1).unwrap())
                .read_to_end(&mut bytes)
                .map_err(|source| {
                    MetadataTransferStagingError::io("read staging establishment marker", source)
                })?;
            validate_establishment_marker(&bytes)?;
            Ok(true)
        }
        Err(MetadataTransferStagingError::Io { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

fn remove_interrupted_establishment_marker(
    data_dir: &Path,
) -> Result<(), MetadataTransferStagingError> {
    let path = data_dir.join(ESTABLISHMENT_MARKER_TEMP_FILE);
    match open_regular_nofollow(&path, "open interrupted staging establishment marker") {
        Ok(file) => {
            drop(file);
            fs::remove_file(path).map_err(|source| {
                MetadataTransferStagingError::io(
                    "remove interrupted staging establishment marker",
                    source,
                )
            })?;
            sync_directory(data_dir, "sync interrupted staging establishment removal")
        }
        Err(MetadataTransferStagingError::Io { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn publish_establishment_marker(
    data_dir: &Path,
    parent_directory_sync: Option<&StagingParentDirectorySync>,
) -> Result<(), MetadataTransferStagingError> {
    let path = data_dir.join(ESTABLISHMENT_MARKER_FILE);
    reject_path_if_present(&path, "inspect staging establishment marker target")?;
    let temp = data_dir.join(ESTABLISHMENT_MARKER_TEMP_FILE);
    reject_path_if_present(&temp, "inspect staging establishment marker temporary file")?;
    let bytes = current_establishment_marker_bytes();
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&temp)
        .map_err(|source| {
            MetadataTransferStagingError::io("create staging establishment marker", source)
        })?;
    file.write_all(&bytes).map_err(|source| {
        MetadataTransferStagingError::io("write staging establishment marker", source)
    })?;
    file.sync_all().map_err(|source| {
        MetadataTransferStagingError::io("sync staging establishment marker", source)
    })?;
    fs::rename(temp, path).map_err(|source| {
        MetadataTransferStagingError::io("publish staging establishment marker", source)
    })?;
    sync_staging_parent_directory(
        data_dir,
        "sync staging establishment marker publication",
        StagingParentDirectorySyncPoint::EstablishmentPublication,
        parent_directory_sync,
    )
}

fn current_establishment_marker_bytes() -> Vec<u8> {
    let mut bytes = Vec::with_capacity(ESTABLISHMENT_MARKER_LEN);
    bytes.extend_from_slice(STAGING_ESTABLISHMENT_MAGIC);
    bytes.extend_from_slice(&STAGING_STORE_FORMAT_VERSION.to_be_bytes());
    bytes.extend_from_slice(&checksum::sha256::digest(
        &current_initialization_marker_bytes(),
    ));
    let checksum = checksum::crc64::checksum(&bytes);
    bytes.extend_from_slice(&checksum.to_be_bytes());
    bytes
}

fn validate_establishment_marker(bytes: &[u8]) -> Result<(), MetadataTransferStagingError> {
    if bytes.len() != ESTABLISHMENT_MARKER_LEN
        || bytes.get(..STAGING_ESTABLISHMENT_MAGIC.len()) != Some(STAGING_ESTABLISHMENT_MAGIC)
    {
        return Err(MetadataTransferStagingError::Invariant(
            "staging establishment marker has invalid framing".to_owned(),
        ));
    }
    let version_offset = STAGING_ESTABLISHMENT_MAGIC.len();
    let version = u16::from_be_bytes(
        bytes[version_offset..version_offset + 2]
            .try_into()
            .unwrap(),
    );
    if version != STAGING_STORE_FORMAT_VERSION {
        return Err(MetadataTransferStagingError::UnsupportedFormatVersion(
            version,
        ));
    }
    let expected_checksum =
        u64::from_be_bytes(bytes[ESTABLISHMENT_MARKER_BODY_LEN..].try_into().unwrap());
    let actual_checksum = checksum::crc64::checksum(&bytes[..ESTABLISHMENT_MARKER_BODY_LEN]);
    if expected_checksum != actual_checksum || bytes != current_establishment_marker_bytes() {
        return Err(MetadataTransferStagingError::Invariant(
            "staging establishment marker does not match the initialized store".to_owned(),
        ));
    }
    Ok(())
}

fn initialization_marker_exists(root: &Path) -> Result<bool, MetadataTransferStagingError> {
    let path = root.join(INITIALIZATION_MARKER_FILE);
    match open_regular_nofollow(&path, "open staging initialization marker") {
        Ok(mut file) => {
            let mut bytes = Vec::with_capacity(INITIALIZATION_MARKER_LEN + 1);
            std::io::Read::by_ref(&mut file)
                .take(u64::try_from(INITIALIZATION_MARKER_LEN + 1).unwrap())
                .read_to_end(&mut bytes)
                .map_err(|source| {
                    MetadataTransferStagingError::io("read staging initialization marker", source)
                })?;
            validate_initialization_marker(&bytes)?;
            Ok(true)
        }
        Err(MetadataTransferStagingError::Io { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

fn remove_interrupted_initialization_marker(
    root: &Path,
) -> Result<(), MetadataTransferStagingError> {
    let path = root.join(INITIALIZATION_MARKER_TEMP_FILE);
    match open_regular_nofollow(&path, "open interrupted staging initialization marker") {
        Ok(file) => {
            drop(file);
            fs::remove_file(path).map_err(|source| {
                MetadataTransferStagingError::io(
                    "remove interrupted staging initialization marker",
                    source,
                )
            })?;
            sync_directory(root, "sync interrupted staging marker removal")
        }
        Err(MetadataTransferStagingError::Io { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn publish_initialization_marker(root: &Path) -> Result<(), MetadataTransferStagingError> {
    let path = root.join(INITIALIZATION_MARKER_FILE);
    reject_path_if_present(&path, "inspect staging initialization marker target")?;
    let temp = root.join(INITIALIZATION_MARKER_TEMP_FILE);
    reject_path_if_present(
        &temp,
        "inspect staging initialization marker temporary file",
    )?;
    let bytes = current_initialization_marker_bytes();
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&temp)
        .map_err(|source| {
            MetadataTransferStagingError::io("create staging initialization marker", source)
        })?;
    file.write_all(&bytes).map_err(|source| {
        MetadataTransferStagingError::io("write staging initialization marker", source)
    })?;
    file.sync_all().map_err(|source| {
        MetadataTransferStagingError::io("sync staging initialization marker", source)
    })?;
    fs::rename(temp, path).map_err(|source| {
        MetadataTransferStagingError::io("publish staging initialization marker", source)
    })?;
    sync_directory(root, "sync staging initialization marker publication")
}

fn current_initialization_marker_bytes() -> Vec<u8> {
    let mut bytes = Vec::with_capacity(INITIALIZATION_MARKER_LEN);
    bytes.extend_from_slice(STAGING_INITIALIZATION_MAGIC);
    bytes.extend_from_slice(&STAGING_STORE_FORMAT_VERSION.to_be_bytes());
    bytes.extend_from_slice(&checksum::sha256::digest(
        STAGING_STORE_SCHEMA_V1.as_bytes(),
    ));
    let checksum = checksum::crc64::checksum(&bytes);
    bytes.extend_from_slice(&checksum.to_be_bytes());
    bytes
}

fn validate_initialization_marker(bytes: &[u8]) -> Result<(), MetadataTransferStagingError> {
    if bytes.len() != INITIALIZATION_MARKER_LEN
        || bytes.get(..STAGING_INITIALIZATION_MAGIC.len()) != Some(STAGING_INITIALIZATION_MAGIC)
    {
        return Err(MetadataTransferStagingError::Invariant(
            "staging initialization marker has invalid framing".to_owned(),
        ));
    }
    let version_offset = STAGING_INITIALIZATION_MAGIC.len();
    let version = u16::from_be_bytes(
        bytes[version_offset..version_offset + 2]
            .try_into()
            .unwrap(),
    );
    if version != STAGING_STORE_FORMAT_VERSION {
        return Err(MetadataTransferStagingError::UnsupportedFormatVersion(
            version,
        ));
    }
    let expected_checksum =
        u64::from_be_bytes(bytes[INITIALIZATION_MARKER_BODY_LEN..].try_into().unwrap());
    let actual_checksum = checksum::crc64::checksum(&bytes[..INITIALIZATION_MARKER_BODY_LEN]);
    if expected_checksum != actual_checksum || bytes != current_initialization_marker_bytes() {
        return Err(MetadataTransferStagingError::Invariant(
            "staging initialization marker does not match the current catalogue".to_owned(),
        ));
    }
    Ok(())
}

fn ensure_manifest(root: &Path) -> Result<(), MetadataTransferStagingError> {
    let path = root.join(MANIFEST_FILE);
    let expected = current_manifest_bytes();
    match open_regular_nofollow(&path, "open staging-store manifest") {
        Ok(mut file) => {
            let mut bytes = Vec::with_capacity(MANIFEST_LEN + 1);
            std::io::Read::by_ref(&mut file)
                .take(u64::try_from(MANIFEST_LEN + 1).unwrap())
                .read_to_end(&mut bytes)
                .map_err(|source| {
                    MetadataTransferStagingError::io("read staging-store manifest", source)
                })?;
            validate_manifest(&bytes)?;
            if bytes != expected {
                return Err(MetadataTransferStagingError::Invariant(
                    "staging-store manifest does not match current schema".to_owned(),
                ));
            }
            Ok(())
        }
        Err(MetadataTransferStagingError::Io { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            let temp = root.join(format!(".{MANIFEST_FILE}.tmp-{}", std::process::id()));
            reject_path_if_present(&temp, "inspect staging manifest temporary file")?;
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&temp)
                .map_err(|source| {
                    MetadataTransferStagingError::io("create staging-store manifest", source)
                })?;
            file.write_all(&expected).map_err(|source| {
                MetadataTransferStagingError::io("write staging-store manifest", source)
            })?;
            file.sync_all().map_err(|source| {
                MetadataTransferStagingError::io("sync staging-store manifest", source)
            })?;
            fs::rename(temp, path).map_err(|source| {
                MetadataTransferStagingError::io("publish staging-store manifest", source)
            })?;
            sync_directory(root, "sync staging-store manifest publication")
        }
        Err(error) => Err(error),
    }
}

fn current_manifest_bytes() -> Vec<u8> {
    let mut bytes = Vec::with_capacity(MANIFEST_LEN);
    bytes.extend_from_slice(STAGING_STORE_MAGIC);
    bytes.extend_from_slice(&STAGING_STORE_FORMAT_VERSION.to_be_bytes());
    bytes.extend_from_slice(&checksum::sha256::digest(
        STAGING_STORE_SCHEMA_V1.as_bytes(),
    ));
    let checksum = checksum::crc64::checksum(&bytes);
    bytes.extend_from_slice(&checksum.to_be_bytes());
    bytes
}

fn validate_manifest(bytes: &[u8]) -> Result<(), MetadataTransferStagingError> {
    if bytes.len() != MANIFEST_LEN {
        return Err(MetadataTransferStagingError::Invariant(
            "staging-store manifest has invalid length".to_owned(),
        ));
    }
    if &bytes[..STAGING_STORE_MAGIC.len()] != STAGING_STORE_MAGIC {
        return Err(MetadataTransferStagingError::Invariant(
            "staging-store manifest has unknown magic".to_owned(),
        ));
    }
    let version_offset = STAGING_STORE_MAGIC.len();
    let version = u16::from_be_bytes(
        bytes[version_offset..version_offset + 2]
            .try_into()
            .unwrap(),
    );
    if version != STAGING_STORE_FORMAT_VERSION {
        return Err(MetadataTransferStagingError::UnsupportedFormatVersion(
            version,
        ));
    }
    let expected_checksum = u64::from_be_bytes(bytes[MANIFEST_BODY_LEN..].try_into().unwrap());
    let actual_checksum = checksum::crc64::checksum(&bytes[..MANIFEST_BODY_LEN]);
    if actual_checksum != expected_checksum {
        return Err(MetadataTransferStagingError::Invariant(
            "staging-store manifest checksum mismatch".to_owned(),
        ));
    }
    Ok(())
}

fn to_sql_u64(value: u64) -> Result<i64, MetadataTransferStagingError> {
    i64::try_from(value).map_err(|_| {
        MetadataTransferStagingError::Invariant(format!(
            "staging value {value} exceeds SQLite integer range"
        ))
    })
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    out
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{symlink, FileTypeExt};
    use std::sync::atomic::AtomicUsize;

    use super::*;

    fn identity() -> MetadataTransferStagingNodeIdentity {
        MetadataTransferStagingNodeIdentity::new(
            NodeId::new(4),
            7,
            "unix:///run/argmin/storage-4.sock".to_owned(),
        )
        .unwrap()
    }

    fn limits() -> MetadataTransferStagingLimits {
        MetadataTransferStagingLimits::new(8, 1024 * 1024, 4 * 1024 * 1024).unwrap()
    }

    fn binding() -> UnavailablePgTransitionMutationBinding {
        UnavailablePgTransitionMutationBinding::new(
            PgId::new(19),
            ClusterEpoch::new(12).unwrap(),
            ClusterEpoch::new(11).unwrap(),
            vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
            vec![NodeId::new(4), NodeId::new(2), NodeId::new(3)],
        )
    }

    fn intent(bytes: &[u8]) -> MetadataTransferStagingIntent {
        MetadataTransferStagingIntent::for_unavailable_transition(
            &binding(),
            checksum::sha256::digest(bytes),
            u64::try_from(bytes.len()).unwrap(),
            METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
        )
        .unwrap()
    }

    fn staging_root(root: &Path) -> PathBuf {
        root.join(STAGING_STORE_DIR)
    }

    fn catalogue_path(root: &Path) -> PathBuf {
        staging_root(root).join(CATALOGUE_FILE)
    }

    fn initialization_marker_path(root: &Path) -> PathBuf {
        staging_root(root).join(INITIALIZATION_MARKER_FILE)
    }

    fn establishment_marker_path(root: &Path) -> PathBuf {
        root.join(ESTABLISHMENT_MARKER_FILE)
    }

    fn open(root: &Path) -> MetadataTransferStagingStore {
        MetadataTransferStagingStore::open(root, identity(), limits()).unwrap()
    }

    #[test]
    fn current_metadata_transfer_staging_store_matches_frozen_v1_manifest_and_requires_version_bump(
    ) {
        assert_eq!(
            hex(&current_manifest_bytes()),
            "4152474d535447000001493cbd07edce4ea79738076b97b6858db1cc6103f29b358c9775f48a0259f116ef1ccc40564fe60a"
        );
        let tmp = test_util::tempdir();
        let store = open(tmp.path());
        drop(store);
        let manifest = fs::read(tmp.path().join(STAGING_STORE_DIR).join(MANIFEST_FILE)).unwrap();
        assert_eq!(manifest, current_manifest_bytes());
        let marker = fs::read(initialization_marker_path(tmp.path())).unwrap();
        assert_eq!(marker, current_initialization_marker_bytes());
        let establishment = fs::read(establishment_marker_path(tmp.path())).unwrap();
        assert_eq!(establishment, current_establishment_marker_bytes());
    }

    #[test]
    fn initialization_marker_v1_encoding_is_fixed() {
        assert_eq!(
            hex(&current_initialization_marker_bytes()),
            "4152474d535447490001493cbd07edce4ea79738076b97b6858db1cc6103f29b358c9775f48a0259f116f8eae3a04c26a75c"
        );
    }

    #[test]
    fn establishment_marker_v1_encoding_is_fixed() {
        assert_eq!(
            hex(&current_establishment_marker_bytes()),
            "4152474d535447450001f3c0bb1bae15166121d7680bfa15de6744e1ff314428f24ee88440bf018a5785f7312e6cf3c0586f"
        );
    }

    #[test]
    fn staging_root_is_parent_synced_before_initialization_and_receipts() {
        let tmp = test_util::tempdir();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observer_state = Arc::clone(&observed);
        let data_dir = tmp.path().to_path_buf();
        let parent_directory_sync: StagingParentDirectorySync =
            Arc::new(move |path, context, point| {
                assert_eq!(path, data_dir);
                sync_directory(path, context)?;
                match point {
                    StagingParentDirectorySyncPoint::RootCreation => {
                        assert!(staging_root(&data_dir).is_dir());
                        assert!(!initialization_marker_path(&data_dir).exists());
                        assert!(!establishment_marker_path(&data_dir).exists());
                    }
                    StagingParentDirectorySyncPoint::EstablishmentPublication => {
                        assert!(initialization_marker_path(&data_dir).exists());
                        assert!(establishment_marker_path(&data_dir).exists());
                    }
                }
                observer_state.lock().unwrap().push(point);
                Ok(())
            });

        let store = MetadataTransferStagingStore::open_with_parent_directory_sync(
            tmp.path(),
            identity(),
            limits(),
            parent_directory_sync,
        )
        .unwrap();

        assert_eq!(
            *observed.lock().unwrap(),
            vec![
                StagingParentDirectorySyncPoint::RootCreation,
                StagingParentDirectorySyncPoint::EstablishmentPublication,
            ]
        );
        assert!(initialization_marker_path(tmp.path()).exists());
        assert!(establishment_marker_path(tmp.path()).exists());
        let artifact = b"receipt after durable staging-root establishment";
        let intent = intent(artifact);
        store.create_intent(&intent).unwrap();
        assert!(!store
            .publish_artifact(&intent, artifact)
            .unwrap()
            .as_bytes()
            .is_empty());
    }

    #[test]
    fn parent_directory_sync_failures_prevent_store_admission_and_receipts() {
        for failed_point in [
            StagingParentDirectorySyncPoint::RootCreation,
            StagingParentDirectorySyncPoint::EstablishmentPublication,
        ] {
            let tmp = test_util::tempdir();
            let parent_directory_sync: StagingParentDirectorySync =
                Arc::new(move |path, context, point| {
                    if point == failed_point {
                        return Err(MetadataTransferStagingError::Invariant(format!(
                            "injected {point:?} parent-directory sync failure"
                        )));
                    }
                    sync_directory(path, context)
                });

            let error = MetadataTransferStagingStore::open_with_parent_directory_sync(
                tmp.path(),
                identity(),
                limits(),
                parent_directory_sync,
            )
            .err()
            .expect("a failed parent-directory durability barrier must prevent open");

            assert!(matches!(error, MetadataTransferStagingError::Invariant(_)));
            if failed_point == StagingParentDirectorySyncPoint::RootCreation {
                assert!(!initialization_marker_path(tmp.path()).exists());
                assert!(!establishment_marker_path(tmp.path()).exists());
            } else {
                assert!(initialization_marker_path(tmp.path()).exists());
                assert!(establishment_marker_path(tmp.path()).exists());
            }

            let retry_points = Arc::new(Mutex::new(Vec::new()));
            let recorded_retry_points = Arc::clone(&retry_points);
            let retry_sync: StagingParentDirectorySync = Arc::new(move |path, context, point| {
                sync_directory(path, context)?;
                recorded_retry_points.lock().unwrap().push(point);
                Ok(())
            });
            let store = MetadataTransferStagingStore::open_with_parent_directory_sync(
                tmp.path(),
                identity(),
                limits(),
                retry_sync,
            )
            .unwrap();
            let expected_retry_points =
                if failed_point == StagingParentDirectorySyncPoint::RootCreation {
                    vec![
                        StagingParentDirectorySyncPoint::RootCreation,
                        StagingParentDirectorySyncPoint::EstablishmentPublication,
                    ]
                } else {
                    vec![StagingParentDirectorySyncPoint::EstablishmentPublication]
                };
            assert_eq!(*retry_points.lock().unwrap(), expected_retry_points);

            let artifact = b"receipt only after retrying the failed parent sync";
            let intent = intent(artifact);
            store.create_intent(&intent).unwrap();
            assert!(!store
                .publish_artifact(&intent, artifact)
                .unwrap()
                .as_bytes()
                .is_empty());
        }
    }

    #[test]
    fn established_store_rejects_changed_initialization_marker_without_catalogue_mutation() {
        for corrupt_version in [false, true] {
            let tmp = test_util::tempdir();
            drop(open(tmp.path()));
            let marker_path = initialization_marker_path(tmp.path());
            let mut marker = current_initialization_marker_bytes();
            if corrupt_version {
                let offset = STAGING_INITIALIZATION_MAGIC.len();
                marker[offset..offset + 2].copy_from_slice(&2u16.to_be_bytes());
                let checksum = checksum::crc64::checksum(&marker[..INITIALIZATION_MARKER_BODY_LEN]);
                marker[INITIALIZATION_MARKER_BODY_LEN..].copy_from_slice(&checksum.to_be_bytes());
            } else {
                marker.truncate(marker.len() - 1);
            }
            fs::write(&marker_path, &marker).unwrap();
            let catalogue_before = fs::read(catalogue_path(tmp.path())).unwrap();

            let error = MetadataTransferStagingStore::open(tmp.path(), identity(), limits())
                .err()
                .expect("invalid initialization marker must fail before catalogue admission");

            if corrupt_version {
                assert!(matches!(
                    error,
                    MetadataTransferStagingError::UnsupportedFormatVersion(2)
                ));
            } else {
                assert!(matches!(error, MetadataTransferStagingError::Invariant(_)));
            }
            assert_eq!(
                fs::read(catalogue_path(tmp.path())).unwrap(),
                catalogue_before
            );
        }
    }

    #[test]
    fn established_store_rejects_changed_establishment_marker_without_root_mutation() {
        for corrupt_version in [false, true] {
            let tmp = test_util::tempdir();
            drop(open(tmp.path()));
            let marker_path = establishment_marker_path(tmp.path());
            let mut marker = current_establishment_marker_bytes();
            if corrupt_version {
                let offset = STAGING_ESTABLISHMENT_MAGIC.len();
                marker[offset..offset + 2].copy_from_slice(&2u16.to_be_bytes());
                let checksum = checksum::crc64::checksum(&marker[..ESTABLISHMENT_MARKER_BODY_LEN]);
                marker[ESTABLISHMENT_MARKER_BODY_LEN..].copy_from_slice(&checksum.to_be_bytes());
            } else {
                marker.truncate(marker.len() - 1);
            }
            fs::write(&marker_path, &marker).unwrap();
            let catalogue_before = fs::read(catalogue_path(tmp.path())).unwrap();
            let initialization_before = fs::read(initialization_marker_path(tmp.path())).unwrap();

            let error = MetadataTransferStagingStore::open(tmp.path(), identity(), limits())
                .err()
                .expect("invalid establishment marker must fail before staging-root admission");

            if corrupt_version {
                assert!(matches!(
                    error,
                    MetadataTransferStagingError::UnsupportedFormatVersion(2)
                ));
            } else {
                assert!(matches!(error, MetadataTransferStagingError::Invariant(_)));
            }
            assert_eq!(
                fs::read(catalogue_path(tmp.path())).unwrap(),
                catalogue_before
            );
            assert_eq!(
                fs::read(initialization_marker_path(tmp.path())).unwrap(),
                initialization_before
            );
        }
    }

    #[test]
    fn first_initialization_recovers_before_and_after_catalogue_creation() {
        for create_empty_catalogue in [false, true] {
            let tmp = test_util::tempdir();
            let root = staging_root(tmp.path());
            let artifacts = root.join(ARTIFACTS_DIR);
            let quarantine = root.join(QUARANTINE_DIR);
            prepare_secure_directory(&root, "test prepare staging root").unwrap();
            prepare_secure_directory(&artifacts, "test prepare staging artifacts").unwrap();
            prepare_secure_directory(&quarantine, "test prepare staging quarantine").unwrap();
            ensure_manifest(&root).unwrap();
            if create_empty_catalogue {
                drop(Connection::open(catalogue_path(tmp.path())).unwrap());
            }
            fs::write(root.join(INITIALIZATION_MARKER_TEMP_FILE), b"interrupted").unwrap();
            fs::write(
                tmp.path().join(ESTABLISHMENT_MARKER_TEMP_FILE),
                b"interrupted",
            )
            .unwrap();

            let store = open(tmp.path());

            assert!(initialization_marker_path(tmp.path()).exists());
            assert!(establishment_marker_path(tmp.path()).exists());
            let version: u32 = store
                .lock_state()
                .unwrap()
                .connection
                .query_row("PRAGMA user_version", [], |row| row.get(0))
                .unwrap();
            assert_eq!(version, u32::from(STAGING_STORE_FORMAT_VERSION));
            assert!(!root.join(INITIALIZATION_MARKER_TEMP_FILE).exists());
            assert!(!tmp.path().join(ESTABLISHMENT_MARKER_TEMP_FILE).exists());
        }
    }

    #[test]
    fn first_initialization_recovers_complete_unmarked_catalogue_without_losing_retirement() {
        let tmp = test_util::tempdir();
        let artifact = b"retired before initialization acknowledgement";
        let intent = intent(artifact);
        let store = open(tmp.path());
        store.tombstone(&intent).unwrap();
        store
            .advance_finalized_floor(intent.pg_id, intent.staging_generation)
            .unwrap();
        drop(store);
        fs::remove_file(initialization_marker_path(tmp.path())).unwrap();
        fs::remove_file(establishment_marker_path(tmp.path())).unwrap();
        sync_directory(
            &staging_root(tmp.path()),
            "test remove initialization marker",
        )
        .unwrap();
        sync_directory(tmp.path(), "test remove staging establishment marker").unwrap();

        let reopened = open(tmp.path());

        assert!(matches!(
            reopened.create_intent(&intent),
            Err(MetadataTransferStagingError::GenerationRetired)
        ));
        assert_eq!(
            finalized_floor(&reopened.lock_state().unwrap().connection, intent.pg_id).unwrap(),
            intent.staging_generation
        );
        assert!(initialization_marker_path(tmp.path()).exists());
        assert!(establishment_marker_path(tmp.path()).exists());
    }

    #[test]
    fn established_store_rejects_deleted_root_without_recreating_it() {
        let tmp = test_util::tempdir();
        let artifact = b"retired generation outside deleted staging root";
        let intent = intent(artifact);
        let store = open(tmp.path());
        store.tombstone(&intent).unwrap();
        store
            .advance_finalized_floor(intent.pg_id, intent.staging_generation)
            .unwrap();
        drop(store);
        let establishment_before = fs::read(establishment_marker_path(tmp.path())).unwrap();
        fs::remove_dir_all(staging_root(tmp.path())).unwrap();

        let error = MetadataTransferStagingStore::open(tmp.path(), identity(), limits())
            .err()
            .expect("established staging-root loss must fail closed");

        assert!(matches!(error, MetadataTransferStagingError::Invariant(_)));
        assert!(!staging_root(tmp.path()).exists());
        assert_eq!(
            fs::read(establishment_marker_path(tmp.path())).unwrap(),
            establishment_before
        );
    }

    #[test]
    fn established_store_rejects_deleted_or_truncated_catalogue_without_reinitializing() {
        for truncate in [false, true] {
            let tmp = test_util::tempdir();
            let artifact = b"durably retired staging generation";
            let intent = intent(artifact);
            let store = open(tmp.path());
            store.tombstone(&intent).unwrap();
            store
                .advance_finalized_floor(intent.pg_id, intent.staging_generation)
                .unwrap();
            drop(store);
            let catalogue = catalogue_path(tmp.path());
            let marker_before = fs::read(initialization_marker_path(tmp.path())).unwrap();
            if truncate {
                OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .open(&catalogue)
                    .unwrap();
            } else {
                fs::remove_file(&catalogue).unwrap();
            }

            let error = MetadataTransferStagingStore::open(tmp.path(), identity(), limits())
                .err()
                .expect("established catalogue loss must fail closed");

            assert!(matches!(error, MetadataTransferStagingError::Invariant(_)));
            assert_eq!(
                fs::read(initialization_marker_path(tmp.path())).unwrap(),
                marker_before
            );
            if truncate {
                assert_eq!(fs::metadata(catalogue).unwrap().len(), 0);
            } else {
                assert!(!catalogue.exists());
            }
        }
    }

    #[test]
    fn staging_store_rejects_adjacent_versions_without_mutation() {
        for version in [0u16, 2] {
            let tmp = test_util::tempdir();
            let store = open(tmp.path());
            drop(store);
            let root = tmp.path().join(STAGING_STORE_DIR);
            let manifest_path = root.join(MANIFEST_FILE);
            let mut manifest = current_manifest_bytes();
            manifest[STAGING_STORE_MAGIC.len()..STAGING_STORE_MAGIC.len() + 2]
                .copy_from_slice(&version.to_be_bytes());
            let checksum = checksum::crc64::checksum(&manifest[..MANIFEST_BODY_LEN]);
            manifest[MANIFEST_BODY_LEN..].copy_from_slice(&checksum.to_be_bytes());
            fs::write(&manifest_path, &manifest).unwrap();
            let before = fs::read(&manifest_path).unwrap();

            let error = MetadataTransferStagingStore::open(tmp.path(), identity(), limits())
                .err()
                .expect("unsupported staging format must fail");

            assert!(matches!(
                error,
                MetadataTransferStagingError::UnsupportedFormatVersion(actual) if actual == version
            ));
            assert_eq!(fs::read(manifest_path).unwrap(), before);
        }
    }

    #[test]
    fn staged_artifact_format_accepts_only_exact_v1() {
        for version in [0, 2] {
            let error = MetadataTransferStagingIntent::for_unavailable_transition(
                &binding(),
                [7; DIGEST_LEN],
                17,
                version,
            )
            .unwrap_err();
            assert!(matches!(error, MetadataTransferStagingError::Invariant(_)));
        }
    }

    #[test]
    fn staging_receipt_v1_encoding_is_fixed() {
        let artifact = b"one retained metadata command";
        let intent = intent(artifact);
        let receipt = encode_staging_evidence(&identity(), &intent, 0);
        assert_eq!(
            hex(&receipt),
            "4152474d494e2d53544147494e472d45564944454e43452d5631000000000004000000000000000700000021756e69783a2f2f2f72756e2f6172676d696e2f73746f726167652d342e736f636b00000013000000000000000c000000000000000b00000010000000030000000100000002000000030000001000000003000000040000000200000003000000000000000c8e23ea24e8bba02e69f180fc02083e99416e1f8614ce65c9d3a96c7f3d24d572000000000000001d000107"
        );
        validate_staging_evidence(&receipt, &intent, 0, &identity()).unwrap();
    }

    #[test]
    fn artifact_publication_is_durable_idempotent_and_restart_readable() {
        let tmp = test_util::tempdir();
        let artifact = b"one retained metadata command";
        let intent = intent(artifact);
        let store = open(tmp.path());
        assert_eq!(
            store.create_intent(&intent).unwrap(),
            MetadataTransferStagingIntentOutcome::Created
        );
        let receipt = store.publish_artifact(&intent, artifact).unwrap();
        assert!(!receipt.as_bytes().is_empty());
        assert_eq!(store.unacknowledged_evidence_count(), 1);
        assert_eq!(store.read_artifact(&intent).unwrap(), artifact);
        assert_eq!(
            store.create_intent(&intent).unwrap(),
            MetadataTransferStagingIntentOutcome::ExactReplay
        );
        assert_eq!(store.publish_artifact(&intent, artifact).unwrap(), receipt);
        drop(store);

        let replacement_identity = MetadataTransferStagingNodeIdentity::new(
            NodeId::new(4),
            8,
            "unix:///run/argmin/storage-4-restarted.sock".to_owned(),
        )
        .unwrap();
        let reopened =
            MetadataTransferStagingStore::open(tmp.path(), replacement_identity, limits()).unwrap();
        assert_eq!(reopened.read_artifact(&intent).unwrap(), artifact);
        assert_eq!(
            reopened.publish_artifact(&intent, artifact).unwrap(),
            receipt
        );
        reopened.mark_imported(&intent).unwrap();
        reopened.mark_imported(&intent).unwrap();
    }

    #[test]
    fn existing_artifact_retry_syncs_directory_before_publication_receipt() {
        let tmp = test_util::tempdir();
        let artifact = b"renamed before publication retry";
        let intent = intent(artifact);
        let observed = Arc::new(AtomicUsize::new(0));
        let observed_for_hook = Arc::clone(&observed);
        let catalogue = catalogue_path(tmp.path());
        let observer = Arc::new(move || {
            let connection = Connection::open(&catalogue).unwrap();
            let state: i64 = connection
                .query_row(
                    "SELECT state FROM staging_intents WHERE pg_id = 19",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(state, StagingState::Intent as i64);
            observed_for_hook.fetch_add(1, Ordering::SeqCst);
        });
        let store = MetadataTransferStagingStore::open_with_publication_durability_observer(
            tmp.path(),
            identity(),
            limits(),
            observer,
        )
        .unwrap();
        store.create_intent(&intent).unwrap();
        let path = store.artifact_path(&intent);
        fs::write(&path, artifact).unwrap();
        File::open(path).unwrap().sync_all().unwrap();

        store.publish_artifact(&intent, artifact).unwrap();

        assert_eq!(observed.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn startup_syncs_recovered_artifact_directory_before_publication_receipt() {
        let tmp = test_util::tempdir();
        let artifact = b"renamed before startup recovery";
        let intent = intent(artifact);
        let store = open(tmp.path());
        store.create_intent(&intent).unwrap();
        let path = store.artifact_path(&intent);
        fs::write(&path, artifact).unwrap();
        File::open(path).unwrap().sync_all().unwrap();
        drop(store);

        let observed = Arc::new(AtomicUsize::new(0));
        let observed_for_hook = Arc::clone(&observed);
        let catalogue = catalogue_path(tmp.path());
        let observer = Arc::new(move || {
            let connection = Connection::open(&catalogue).unwrap();
            let state: i64 = connection
                .query_row(
                    "SELECT state FROM staging_intents WHERE pg_id = 19",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(state, StagingState::Intent as i64);
            observed_for_hook.fetch_add(1, Ordering::SeqCst);
        });
        let reopened = MetadataTransferStagingStore::open_with_publication_durability_observer(
            tmp.path(),
            identity(),
            limits(),
            observer,
        )
        .unwrap();

        assert_eq!(observed.load(Ordering::SeqCst), 1);
        assert_eq!(reopened.read_artifact(&intent).unwrap(), artifact);
    }

    #[test]
    fn mismatched_artifact_and_intent_fail_without_publication() {
        let tmp = test_util::tempdir();
        let artifact = b"expected artifact";
        let intent = intent(artifact);
        let store = open(tmp.path());
        store.create_intent(&intent).unwrap();

        assert!(matches!(
            store.publish_artifact(&intent, b"different artifact"),
            Err(MetadataTransferStagingError::ArtifactMismatch)
        ));
        assert!(matches!(
            store.read_artifact(&intent),
            Err(MetadataTransferStagingError::IntentConflict(_))
        ));
        assert_eq!(store.unacknowledged_evidence_count(), 0);
    }

    #[test]
    fn tombstone_precedes_removal_and_rejects_delayed_publication() {
        let tmp = test_util::tempdir();
        let artifact = b"staged then cancelled";
        let intent = intent(artifact);
        let store = open(tmp.path());
        store.create_intent(&intent).unwrap();
        store.publish_artifact(&intent, artifact).unwrap();
        let artifact_path = store.artifact_path(&intent);
        assert!(artifact_path.exists());

        let tombstone = store.tombstone(&intent).unwrap();
        assert!(!tombstone.as_bytes().is_empty());
        assert!(!artifact_path.exists());
        assert!(matches!(
            store.publish_artifact(&intent, artifact),
            Err(MetadataTransferStagingError::GenerationRetired)
        ));
        store
            .advance_finalized_floor(intent.pg_id, intent.staging_generation)
            .unwrap();
        assert!(matches!(
            store.create_intent(&intent),
            Err(MetadataTransferStagingError::GenerationRetired)
        ));
    }

    #[test]
    fn tombstone_before_intent_rejects_delayed_stage_and_replays_original_receipt() {
        let tmp = test_util::tempdir();
        let artifact = b"cancelled before destination stage";
        let intent = intent(artifact);
        let store = open(tmp.path());
        let receipt = store.tombstone(&intent).unwrap();
        let delayed_path = store.artifact_path(&intent);
        fs::write(&delayed_path, artifact).unwrap();
        assert!(matches!(
            store.create_intent(&intent),
            Err(MetadataTransferStagingError::GenerationRetired)
        ));
        drop(store);

        let replacement_identity = MetadataTransferStagingNodeIdentity::new(
            NodeId::new(4),
            8,
            "unix:///run/argmin/storage-4-restarted.sock".to_owned(),
        )
        .unwrap();
        let reopened =
            MetadataTransferStagingStore::open(tmp.path(), replacement_identity, limits()).unwrap();
        assert!(!delayed_path.exists());
        fs::write(&delayed_path, artifact).unwrap();
        assert_eq!(reopened.tombstone(&intent).unwrap(), receipt);
        assert!(!delayed_path.exists());
        assert!(matches!(
            reopened.publish_artifact(&intent, artifact),
            Err(MetadataTransferStagingError::GenerationRetired)
        ));
    }

    #[test]
    fn restart_recovers_exact_renamed_artifact_and_quarantines_unknown_file() {
        let tmp = test_util::tempdir();
        let artifact = b"renamed before catalogue commit";
        let intent = intent(artifact);
        let store = open(tmp.path());
        store.create_intent(&intent).unwrap();
        let artifact_path = store.artifact_path(&intent);
        fs::write(&artifact_path, artifact).unwrap();
        File::open(&artifact_path).unwrap().sync_all().unwrap();
        sync_directory(&store.artifacts_dir, "test sync").unwrap();
        let unknown = store.artifacts_dir.join("unknown.artifact");
        fs::write(&unknown, b"unknown").unwrap();
        drop(store);

        let reopened = open(tmp.path());
        assert_eq!(reopened.read_artifact(&intent).unwrap(), artifact);
        assert_eq!(reopened.unacknowledged_evidence_count(), 1);
        assert!(!unknown.exists());
        assert!(reopened.quarantine_dir.join("unknown.artifact").exists());
    }

    #[test]
    fn restart_removes_interrupted_temporary_artifact() {
        let tmp = test_util::tempdir();
        let store = open(tmp.path());
        let temp = store.artifacts_dir.join(".interrupted.artifact.tmp-99-1");
        fs::write(&temp, b"partial").unwrap();
        drop(store);

        let _reopened = open(tmp.path());
        assert!(!temp.exists());
    }

    #[test]
    fn startup_rejects_symlinked_artifact_without_following_it() {
        let tmp = test_util::tempdir();
        let outside = tmp.path().join("outside");
        fs::write(&outside, b"must remain untouched").unwrap();
        let store = open(tmp.path());
        let link = store.artifacts_dir.join("unknown.artifact");
        symlink(&outside, &link).unwrap();
        drop(store);

        let error = MetadataTransferStagingStore::open(tmp.path(), identity(), limits())
            .err()
            .expect("symlinked staging artifact must fail startup");
        assert!(matches!(error, MetadataTransferStagingError::Invariant(_)));
        assert_eq!(fs::read(outside).unwrap(), b"must remain untouched");
        assert!(link.symlink_metadata().unwrap().file_type().is_symlink());
    }

    #[test]
    fn startup_rejects_fifo_catalogue_files_without_blocking_in_sqlite() {
        for suffix in ["", "-wal"] {
            let tmp = test_util::tempdir();
            drop(open(tmp.path()));
            let catalogue = catalogue_path(tmp.path());
            let mut fifo = catalogue.as_os_str().to_os_string();
            fifo.push(suffix);
            let fifo = PathBuf::from(fifo);
            if suffix.is_empty() {
                fs::remove_file(&fifo).unwrap();
            }
            let c_path = CString::new(fifo.as_os_str().as_bytes()).unwrap();
            // SAFETY: `c_path` is a live, NUL-terminated pathname and the mode is valid.
            assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);

            let error = MetadataTransferStagingStore::open(tmp.path(), identity(), limits())
                .err()
                .expect("FIFO staging catalogue file must fail before SQLite opens it");

            assert!(matches!(error, MetadataTransferStagingError::Invariant(_)));
            assert!(fifo.symlink_metadata().unwrap().file_type().is_fifo());
        }
    }

    #[test]
    fn catalogue_file_validation_rejects_devices() {
        let error = validate_catalogue_file_if_present(Path::new("/dev/null"))
            .expect_err("a character device must not be accepted as a SQLite catalogue");
        assert!(matches!(error, MetadataTransferStagingError::Invariant(_)));
    }

    #[test]
    fn restart_fails_closed_for_missing_or_corrupt_published_artifact() {
        for corrupt in [false, true] {
            let tmp = test_util::tempdir();
            let artifact = b"durable artifact";
            let intent = intent(artifact);
            let store = open(tmp.path());
            store.create_intent(&intent).unwrap();
            store.publish_artifact(&intent, artifact).unwrap();
            let path = store.artifact_path(&intent);
            drop(store);
            if corrupt {
                fs::write(path, b"corrupt artifact").unwrap();
            } else {
                fs::remove_file(path).unwrap();
            }

            let error = MetadataTransferStagingStore::open(tmp.path(), identity(), limits())
                .err()
                .expect("invalid published artifact must fail startup");
            assert!(matches!(
                error,
                MetadataTransferStagingError::ArtifactMismatch
                    | MetadataTransferStagingError::Invariant(_)
            ));
        }
    }

    #[test]
    fn catalogue_rejects_adjacent_versions_and_schema_changes_without_repair() {
        for version in [0u32, 2] {
            let tmp = test_util::tempdir();
            drop(open(tmp.path()));
            let connection = Connection::open(catalogue_path(tmp.path())).unwrap();
            connection
                .pragma_update(None, "user_version", version)
                .unwrap();
            drop(connection);

            let error = MetadataTransferStagingStore::open(tmp.path(), identity(), limits())
                .err()
                .expect("noncurrent catalogue must fail startup");
            if version == 0 {
                assert!(matches!(error, MetadataTransferStagingError::Invariant(_)));
            } else {
                assert!(matches!(
                    error,
                    MetadataTransferStagingError::UnsupportedCatalogueVersion(actual)
                        if actual == version
                ));
            }
            let connection = Connection::open(catalogue_path(tmp.path())).unwrap();
            let actual: u32 = connection
                .query_row("PRAGMA user_version", [], |row| row.get(0))
                .unwrap();
            assert_eq!(actual, version);
        }

        let tmp = test_util::tempdir();
        drop(open(tmp.path()));
        let connection = Connection::open(catalogue_path(tmp.path())).unwrap();
        connection
            .execute("ALTER TABLE staging_intents ADD COLUMN forged INTEGER", [])
            .unwrap();
        drop(connection);
        let error = MetadataTransferStagingStore::open(tmp.path(), identity(), limits())
            .err()
            .expect("changed v1 catalogue must fail startup");
        assert!(matches!(error, MetadataTransferStagingError::Invariant(_)));
        let connection = Connection::open(catalogue_path(tmp.path())).unwrap();
        let forged_columns: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('staging_intents') WHERE name = 'forged'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(forged_columns, 1);
    }

    #[test]
    fn startup_rejects_coordinated_receipt_corruption() {
        let tmp = test_util::tempdir();
        let artifact = b"receipt-bound artifact";
        let intent = intent(artifact);
        let store = open(tmp.path());
        store.create_intent(&intent).unwrap();
        store.publish_artifact(&intent, artifact).unwrap();
        drop(store);

        let connection = Connection::open(catalogue_path(tmp.path())).unwrap();
        connection
            .execute("UPDATE staging_intents SET publication_receipt = X'00'", [])
            .unwrap();
        connection
            .execute(
                "UPDATE staging_evidence_deltas SET evidence_bytes = X'00'",
                [],
            )
            .unwrap();
        drop(connection);
        let error = MetadataTransferStagingStore::open(tmp.path(), identity(), limits())
            .err()
            .expect("coordinated receipt corruption must fail startup");
        assert!(matches!(error, MetadataTransferStagingError::Invariant(_)));
    }

    #[test]
    fn startup_binds_receipt_to_independently_persisted_actor_identity() {
        let forged_identities = [
            MetadataTransferStagingNodeIdentity::new(
                NodeId::new(5),
                7,
                "unix:///run/argmin/storage-4.sock".to_owned(),
            )
            .unwrap(),
            MetadataTransferStagingNodeIdentity::new(
                NodeId::new(4),
                8,
                "unix:///run/argmin/storage-4.sock".to_owned(),
            )
            .unwrap(),
            MetadataTransferStagingNodeIdentity::new(
                NodeId::new(4),
                7,
                "unix:///run/argmin/storage-5.sock".to_owned(),
            )
            .unwrap(),
        ];
        for forged_identity in forged_identities {
            let tmp = test_util::tempdir();
            let artifact = b"actor-bound staging receipt";
            let intent = intent(artifact);
            let store = open(tmp.path());
            store.create_intent(&intent).unwrap();
            store.publish_artifact(&intent, artifact).unwrap();
            drop(store);
            let forged_receipt = encode_staging_evidence(&forged_identity, &intent, 0);
            let connection = Connection::open(catalogue_path(tmp.path())).unwrap();
            connection
                .execute(
                    "UPDATE staging_intents SET publication_receipt = ?1",
                    params![&forged_receipt],
                )
                .unwrap();
            connection
                .execute(
                    "UPDATE staging_evidence_deltas SET evidence_bytes = ?1",
                    params![&forged_receipt],
                )
                .unwrap();
            drop(connection);

            let error = MetadataTransferStagingStore::open(tmp.path(), identity(), limits())
                .err()
                .expect("receipt actor mutation must fail against durable actor identity");

            assert!(matches!(error, MetadataTransferStagingError::Invariant(_)));
        }
    }

    #[test]
    fn same_generation_rejects_a_different_artifact_tuple() {
        let tmp = test_util::tempdir();
        let store = open(tmp.path());
        let first = intent(b"first artifact");
        store.create_intent(&first).unwrap();
        let mut conflicting = first.clone();
        conflicting.artifact_digest = checksum::sha256::digest(b"second artifact");
        conflicting.artifact_length = u64::try_from(b"second artifact".len()).unwrap();
        assert!(matches!(
            store.create_intent(&conflicting),
            Err(MetadataTransferStagingError::IntentConflict(_))
        ));
    }

    #[test]
    fn capacity_accounts_for_intents_and_quarantined_files() {
        let tmp = test_util::tempdir();
        let small_limits = MetadataTransferStagingLimits::new(1, 16, 16).unwrap();
        let store =
            MetadataTransferStagingStore::open(tmp.path(), identity(), small_limits).unwrap();
        let first = intent(b"1234567890abcdef");
        store.create_intent(&first).unwrap();
        let mut second = intent(b"x");
        second.pg_id = PgId::new(20);
        assert!(matches!(
            store.create_intent(&second),
            Err(MetadataTransferStagingError::Capacity(_))
        ));
    }

    #[test]
    fn tombstones_consume_entry_capacity_until_finalized_evidence_can_be_pruned() {
        let tmp = test_util::tempdir();
        let small_limits = MetadataTransferStagingLimits::new(1, 64, 64).unwrap();
        let store =
            MetadataTransferStagingStore::open(tmp.path(), identity(), small_limits).unwrap();
        let first = intent(b"first cancelled artifact");
        store.tombstone(&first).unwrap();
        let mut second = intent(b"second cancelled artifact");
        second.pg_id = PgId::new(20);
        assert!(matches!(
            store.tombstone(&second),
            Err(MetadataTransferStagingError::Capacity(_))
        ));
    }

    #[test]
    fn startup_validates_complete_catalogue_before_reconciling_files() {
        let tmp = test_util::tempdir();
        let recover_artifact = b"renamed but not committed";
        let recover_intent = intent(recover_artifact);
        let published_artifact = b"published with later corruption";
        let mut published_intent = intent(published_artifact);
        published_intent.pg_id = PgId::new(20);
        let store = open(tmp.path());
        store.create_intent(&recover_intent).unwrap();
        fs::write(store.artifact_path(&recover_intent), recover_artifact).unwrap();
        store.create_intent(&published_intent).unwrap();
        store
            .publish_artifact(&published_intent, published_artifact)
            .unwrap();
        drop(store);

        let connection = Connection::open(catalogue_path(tmp.path())).unwrap();
        connection
            .execute(
                "UPDATE staging_intents SET publication_receipt = X'00' WHERE pg_id = 20",
                [],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE staging_evidence_deltas SET evidence_bytes = X'00' WHERE pg_id = 20",
                [],
            )
            .unwrap();
        drop(connection);

        assert!(MetadataTransferStagingStore::open(tmp.path(), identity(), limits()).is_err());
        let connection = Connection::open(catalogue_path(tmp.path())).unwrap();
        let state: i64 = connection
            .query_row(
                "SELECT state FROM staging_intents WHERE pg_id = 19",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, StagingState::Intent as i64);
    }

    #[test]
    fn startup_rejects_noncanonical_inflight_evidence_page() {
        let tmp = test_util::tempdir();
        drop(open(tmp.path()));
        let connection = Connection::open(catalogue_path(tmp.path())).unwrap();
        connection
            .execute(
                "INSERT INTO staging_evidence_inflight_page (\
                    singleton, previous_generation, previous_apply_receipt_digest, generation, \
                    operation_payload, page_digest, apply_receipt\
                 ) VALUES (1, 0, zeroblob(32), 1, X'01', zeroblob(32), NULL)",
                [],
            )
            .unwrap();
        drop(connection);

        let error = MetadataTransferStagingStore::open(tmp.path(), identity(), limits())
            .err()
            .expect("noncanonical in-flight page must fail startup");
        assert!(matches!(error, MetadataTransferStagingError::Invariant(_)));
    }
}
