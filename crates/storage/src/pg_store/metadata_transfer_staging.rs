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

use crate::control_plane::{
    PgMetadataProof, PgMetadataTransferProof, UnavailablePgTransitionMutationBinding,
};
use crate::control_plane_command::CommittedUnavailablePgStagingAuthorization;
use crate::data_dir::prepare_private_data_dir;
use crate::metadata_command::MetadataCommandLogRangeEntry;
use crate::node_runtime::MetadataCommandDecodeAuthority;
use crate::peering::{PgMetadataTransferArtifact, PgMetadataTransferBaseKind};
use crate::storage_rpc::{
    decode_metadata_command_checkpoint_payload, decode_metadata_command_log_entry_range_response,
    encode_metadata_command_checkpoint_payload, encode_metadata_command_log_entry_range_response,
    StorageRpcMetadataCommandLogEntryRangeResponse,
    STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES,
};
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
const STAGING_STORE_FORMAT_VERSION: u16 = 4;
const STAGING_STORE_SCHEMA_V4: &str =
    include_str!("schema_manifests/metadata_transfer_staging_v4.sql");
const DIGEST_LEN: usize = 32;
const MANIFEST_BODY_LEN: usize = STAGING_STORE_MAGIC.len() + 2 + DIGEST_LEN;
const MANIFEST_LEN: usize = MANIFEST_BODY_LEN + 8;
const INITIALIZATION_MARKER_BODY_LEN: usize = STAGING_INITIALIZATION_MAGIC.len() + 2 + DIGEST_LEN;
const INITIALIZATION_MARKER_LEN: usize = INITIALIZATION_MARKER_BODY_LEN + 8;
const ESTABLISHMENT_MARKER_BODY_LEN: usize = STAGING_ESTABLISHMENT_MAGIC.len() + 2 + DIGEST_LEN;
const ESTABLISHMENT_MARKER_LEN: usize = ESTABLISHMENT_MARKER_BODY_LEN + 8;
const MAX_ENDPOINT_BYTES: usize = 2_048;
pub(crate) const MAX_STAGING_INTENT_BYTES: usize = 16 * 1_024;
pub(crate) const MAX_STAGING_EVIDENCE_BYTES: usize = 4_096;
pub(crate) const MAX_STAGING_EVIDENCE_PAGE_ENTRIES: usize = 64;
pub(crate) const MAX_STAGING_EVIDENCE_PAGE_BYTES: usize = 120 * 1_024;
pub(crate) const MAX_STAGING_EPOCH_PROOFS_PER_INTENT: usize = 64;
const STAGING_EVIDENCE_MAGIC: &[u8] = b"ARGMIN-STAGING-EVIDENCE-V4\0";
const STAGING_EVIDENCE_PAGE_MAGIC: &[u8] = b"ARGMIN-STAGING-EVIDENCE-PAGE-V4\0";
const STAGING_EVIDENCE_APPLY_RECEIPT_MAGIC: &[u8] = b"ARGMIN-STAGING-EVIDENCE-APPLY-V4\0";
const STAGED_ARTIFACT_MAGIC: &[u8] = b"ARGMIN-METADATA-TRANSFER-ARTIFACT-V3\0";
pub(crate) const METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION: u16 = 3;
pub(crate) const METADATA_TRANSFER_STAGED_ARTIFACT_MAX_BYTES: u64 = 63 * 1_024 * 1_024;

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
    #[error("metadata-transfer staging artifact semantic validation failed: {0}")]
    ArtifactSemanticMismatch(String),
    #[error("metadata-transfer staging artifact length {length} exceeds protocol limit {limit}")]
    ArtifactTooLarge { length: u64, limit: u64 },
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
        if max_entries == 0
            || max_artifact_bytes == 0
            || max_artifact_bytes > METADATA_TRANSFER_STAGED_ARTIFACT_MAX_BYTES
            || max_total_bytes < max_artifact_bytes
        {
            return Err(MetadataTransferStagingError::Invariant(
                "staging limits require entries > 0 and total bytes >= artifact bytes > 0 within the protocol maximum"
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
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

    pub(crate) fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub(crate) fn node_incarnation(&self) -> u64 {
        self.node_incarnation
    }

    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
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

    pub(crate) fn pg_id(&self) -> PgId {
        self.pg_id
    }

    pub(crate) fn transition_epoch(&self) -> ClusterEpoch {
        self.transition_epoch
    }

    pub(crate) fn source_epoch(&self) -> ClusterEpoch {
        self.source_epoch
    }

    pub(crate) fn source_acting_set(&self) -> &[NodeId] {
        &self.source_acting_set
    }

    pub(crate) fn destination_acting_set(&self) -> &[NodeId] {
        &self.destination_acting_set
    }

    pub(crate) fn staging_generation(&self) -> u64 {
        self.staging_generation
    }

    pub(crate) fn artifact_digest(&self) -> [u8; DIGEST_LEN] {
        self.artifact_digest
    }

    pub(crate) fn artifact_length(&self) -> u64 {
        self.artifact_length
    }

    pub(crate) fn artifact_format_version(&self) -> u16 {
        self.artifact_format_version
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

    pub(crate) fn from_publication_bytes(
        bytes: &[u8],
        expected_intent: &MetadataTransferStagingIntent,
        expected_node_id: NodeId,
    ) -> Result<Self, MetadataTransferStagingError> {
        let evidence = decode_staging_evidence(bytes)?;
        if evidence.kind() != MetadataTransferStagingEvidenceKind::Publication {
            return Err(MetadataTransferStagingError::Invariant(
                "staging publication response contains non-publication evidence".to_owned(),
            ));
        }
        if evidence.intent() != expected_intent || evidence.actor().node_id != expected_node_id {
            return Err(MetadataTransferStagingError::Invariant(
                "staging publication response does not match its request subject".to_owned(),
            ));
        }
        Ok(Self {
            bytes: evidence.as_bytes().to_vec(),
        })
    }

    pub(crate) fn from_epoch_bound_publication_bytes(
        bytes: &[u8],
        expected_intent: &MetadataTransferStagingIntent,
        expected_node_id: NodeId,
        expected_target_epoch: ClusterEpoch,
    ) -> Result<Self, MetadataTransferStagingError> {
        let receipt = Self::from_publication_bytes(bytes, expected_intent, expected_node_id)?;
        let evidence = decode_staging_evidence(receipt.as_bytes())?;
        if evidence.target_epoch() != Some(expected_target_epoch) {
            return Err(MetadataTransferStagingError::Invariant(
                "staging publication response does not match its requested target epoch".to_owned(),
            ));
        }
        Ok(receipt)
    }

    pub(crate) fn from_tombstone_bytes(
        bytes: &[u8],
        expected_intent: &MetadataTransferStagingIntent,
        expected_node_id: NodeId,
    ) -> Result<Self, MetadataTransferStagingError> {
        let evidence = decode_staging_evidence(bytes)?;
        if evidence.kind() != MetadataTransferStagingEvidenceKind::Tombstone
            || evidence.target_epoch().is_some()
            || evidence.transfer().is_some()
        {
            return Err(MetadataTransferStagingError::Invariant(
                "staging tombstone response contains non-tombstone evidence".to_owned(),
            ));
        }
        if evidence.intent() != expected_intent || evidence.actor().node_id != expected_node_id {
            return Err(MetadataTransferStagingError::Invariant(
                "staging tombstone response does not match its request subject".to_owned(),
            ));
        }
        Ok(Self {
            bytes: evidence.as_bytes().to_vec(),
        })
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataTransferStagingActorClosureCandidate {
    first_actor: MetadataTransferStagingNodeIdentity,
    first_accepted_generation: u64,
    first_accepted_apply_receipt_digest: [u8; DIGEST_LEN],
    first_ambiguous_page: Option<MetadataTransferStagingActorClosureAmbiguousPage>,
    through_actor: MetadataTransferStagingNodeIdentity,
    rebound_entry_count: u64,
    rebound_max_sequence: u64,
    rebound_evidence_digest: [u8; DIGEST_LEN],
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetadataTransferStagingActorClosureAmbiguousPage {
    generation: u64,
    page_digest: [u8; DIGEST_LEN],
    apply_receipt_digest: [u8; DIGEST_LEN],
}

impl MetadataTransferStagingActorClosureCandidate {
    pub(crate) fn first_actor(&self) -> &MetadataTransferStagingNodeIdentity {
        &self.first_actor
    }

    pub(crate) fn first_accepted_generation(&self) -> u64 {
        self.first_accepted_generation
    }

    pub(crate) fn first_accepted_apply_receipt_digest(&self) -> [u8; DIGEST_LEN] {
        self.first_accepted_apply_receipt_digest
    }

    pub(crate) fn accepts_first_tip(
        &self,
        actor: &MetadataTransferStagingNodeIdentity,
        generation: u64,
        page_digest: [u8; DIGEST_LEN],
        apply_receipt_digest: [u8; DIGEST_LEN],
    ) -> bool {
        actor == &self.first_actor
            && ((generation == self.first_accepted_generation
                && apply_receipt_digest == self.first_accepted_apply_receipt_digest)
                || self.first_ambiguous_page.as_ref().is_some_and(|ambiguous| {
                    generation == ambiguous.generation
                        && page_digest == ambiguous.page_digest
                        && apply_receipt_digest == ambiguous.apply_receipt_digest
                }))
    }

    pub(crate) fn through_actor(&self) -> &MetadataTransferStagingNodeIdentity {
        &self.through_actor
    }

    pub(crate) fn rebound_entry_count(&self) -> u64 {
        self.rebound_entry_count
    }

    pub(crate) fn rebound_max_sequence(&self) -> u64 {
        self.rebound_max_sequence
    }

    pub(crate) fn rebound_evidence_digest(&self) -> [u8; DIGEST_LEN] {
        self.rebound_evidence_digest
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataTransferStagingEvidencePageEntry {
    sequence: u64,
    evidence: Vec<u8>,
}

impl MetadataTransferStagingEvidencePageEntry {
    pub(crate) fn sequence(&self) -> u64 {
        self.sequence
    }

    pub(crate) fn evidence(&self) -> &[u8] {
        &self.evidence
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum MetadataTransferStagingEvidenceKind {
    Publication = 0,
    Tombstone = 1,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataTransferStagingEvidence {
    actor: MetadataTransferStagingNodeIdentity,
    intent: MetadataTransferStagingIntent,
    kind: MetadataTransferStagingEvidenceKind,
    target_epoch: Option<ClusterEpoch>,
    transfer: Option<PgMetadataTransferProof>,
    fsync_scope: u8,
    bytes: Vec<u8>,
}

impl MetadataTransferStagingEvidence {
    pub(crate) fn actor(&self) -> &MetadataTransferStagingNodeIdentity {
        &self.actor
    }

    pub(crate) fn intent(&self) -> &MetadataTransferStagingIntent {
        &self.intent
    }

    pub(crate) fn kind(&self) -> MetadataTransferStagingEvidenceKind {
        self.kind
    }

    pub(crate) fn fsync_scope(&self) -> u8 {
        self.fsync_scope
    }

    pub(crate) fn transfer(&self) -> Option<PgMetadataTransferProof> {
        self.transfer
    }

    pub(crate) fn target_epoch(&self) -> Option<ClusterEpoch> {
        self.target_epoch
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn rebound_for_actor(&self, actor: &MetadataTransferStagingNodeIdentity) -> Vec<u8> {
        encode_staging_evidence(
            actor,
            &self.intent,
            self.kind as u8,
            self.target_epoch,
            self.transfer,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ValidatedStagedMetadataTransferArtifact {
    artifact: PgMetadataTransferArtifact,
    destination_epoch: ClusterEpoch,
    transfer: PgMetadataTransferProof,
}

pub(crate) fn encode_staged_metadata_transfer_artifact(
    artifact: &PgMetadataTransferArtifact,
    destination_epoch: ClusterEpoch,
) -> Result<Vec<u8>, MetadataTransferStagingError> {
    let imported_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        artifact,
        destination_epoch,
    )
    .map_err(|error| MetadataTransferStagingError::ArtifactSemanticMismatch(error.to_string()))?;
    let mut out = Vec::new();
    out.extend_from_slice(STAGED_ARTIFACT_MAGIC);
    out.extend_from_slice(&METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION.to_be_bytes());
    out.extend_from_slice(&artifact.pg_id.get().to_be_bytes());
    out.extend_from_slice(&artifact.source_node_id.as_u32().to_be_bytes());
    out.extend_from_slice(&artifact.cluster_epoch.get().to_be_bytes());
    out.push(match artifact.base_kind {
        PgMetadataTransferBaseKind::Empty => 0,
        PgMetadataTransferBaseKind::RetainedLogPrefix => 1,
        PgMetadataTransferBaseKind::Checkpoint => 2,
    });
    encode_metadata_proof(&mut out, artifact.base_proof);
    encode_metadata_proof(&mut out, artifact.proof);
    out.extend_from_slice(&destination_epoch.get().to_be_bytes());
    encode_metadata_proof(&mut out, imported_proof);
    match artifact.checkpoint_base.as_ref() {
        None => out.push(0),
        Some(checkpoint) => {
            out.push(1);
            let checkpoint =
                encode_metadata_command_checkpoint_payload(checkpoint).map_err(|error| {
                    MetadataTransferStagingError::ArtifactSemanticMismatch(error.to_string())
                })?;
            put_bytes(&mut out, &checkpoint);
        }
    }
    let chunks = artifact
        .retained_log_entries
        .chunks(STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES as usize)
        .collect::<Vec<_>>();
    out.extend_from_slice(
        &u32::try_from(chunks.len())
            .map_err(|_| {
                MetadataTransferStagingError::ArtifactSemanticMismatch(
                    "staged artifact has too many retained-log chunks".to_owned(),
                )
            })?
            .to_be_bytes(),
    );
    for chunk in chunks {
        let encoded = encode_metadata_command_log_entry_range_response(
            &StorageRpcMetadataCommandLogEntryRangeResponse {
                entries: chunk.to_vec(),
            },
        )
        .map_err(|error| {
            MetadataTransferStagingError::ArtifactSemanticMismatch(error.to_string())
        })?;
        put_bytes(&mut out, &encoded);
    }
    if out.len() as u64 > METADATA_TRANSFER_STAGED_ARTIFACT_MAX_BYTES {
        return Err(MetadataTransferStagingError::ArtifactTooLarge {
            length: out.len() as u64,
            limit: METADATA_TRANSFER_STAGED_ARTIFACT_MAX_BYTES,
        });
    }
    Ok(out)
}

fn validate_staged_metadata_transfer_artifact(
    bytes: &[u8],
    intent: &MetadataTransferStagingIntent,
    authority: &MetadataCommandDecodeAuthority,
) -> Result<ValidatedStagedMetadataTransferArtifact, MetadataTransferStagingError> {
    let mut offset = 0;
    if take(bytes, &mut offset, STAGED_ARTIFACT_MAGIC.len())? != STAGED_ARTIFACT_MAGIC {
        return Err(MetadataTransferStagingError::ArtifactSemanticMismatch(
            "staged artifact has invalid magic".to_owned(),
        ));
    }
    let version = u16::from_be_bytes(take(bytes, &mut offset, 2)?.try_into().unwrap());
    if version != METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION {
        return Err(MetadataTransferStagingError::ArtifactSemanticMismatch(
            format!("staged artifact has unsupported format version {version}"),
        ));
    }
    let pg_id = PgId::new(u32::from_be_bytes(
        take(bytes, &mut offset, 4)?.try_into().unwrap(),
    ));
    let source_node_id = NodeId::new(u32::from_be_bytes(
        take(bytes, &mut offset, 4)?.try_into().unwrap(),
    ));
    let cluster_epoch = ClusterEpoch::new(u64::from_be_bytes(
        take(bytes, &mut offset, 8)?.try_into().unwrap(),
    ))
    .ok_or_else(|| {
        MetadataTransferStagingError::ArtifactSemanticMismatch(
            "staged artifact source epoch is zero".to_owned(),
        )
    })?;
    let base_kind = match take(bytes, &mut offset, 1)?[0] {
        0 => PgMetadataTransferBaseKind::Empty,
        1 => PgMetadataTransferBaseKind::RetainedLogPrefix,
        2 => PgMetadataTransferBaseKind::Checkpoint,
        _ => {
            return Err(MetadataTransferStagingError::ArtifactSemanticMismatch(
                "staged artifact has invalid base kind".to_owned(),
            ))
        }
    };
    let base_proof = decode_metadata_proof(bytes, &mut offset)?;
    let proof = decode_metadata_proof(bytes, &mut offset)?;
    let destination_epoch = ClusterEpoch::new(u64::from_be_bytes(
        take(bytes, &mut offset, 8)?.try_into().unwrap(),
    ))
    .ok_or_else(|| {
        MetadataTransferStagingError::ArtifactSemanticMismatch(
            "staged artifact destination epoch is zero".to_owned(),
        )
    })?;
    let claimed_imported_proof = decode_metadata_proof(bytes, &mut offset)?;
    let checkpoint_base = match take(bytes, &mut offset, 1)?[0] {
        0 => None,
        1 => {
            let checkpoint_bytes = take_length_prefixed(bytes, &mut offset)?;
            Some(
                decode_metadata_command_checkpoint_payload(checkpoint_bytes).map_err(|error| {
                    MetadataTransferStagingError::ArtifactSemanticMismatch(error.to_string())
                })?,
            )
        }
        _ => {
            return Err(MetadataTransferStagingError::ArtifactSemanticMismatch(
                "staged artifact has invalid checkpoint tag".to_owned(),
            ))
        }
    };
    let chunk_count = usize::try_from(u32::from_be_bytes(
        take(bytes, &mut offset, 4)?.try_into().unwrap(),
    ))
    .unwrap();
    let mut retained_log_entries = Vec::<MetadataCommandLogRangeEntry>::new();
    for chunk_index in 0..chunk_count {
        let chunk = decode_metadata_command_log_entry_range_response(
            take_length_prefixed(bytes, &mut offset)?,
            authority,
        )
        .map_err(|error| MetadataTransferStagingError::ArtifactSemanticMismatch(error.to_string()))?
        .entries;
        if chunk.is_empty()
            || (chunk_index + 1 < chunk_count
                && chunk.len() != STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES as usize)
        {
            return Err(MetadataTransferStagingError::ArtifactSemanticMismatch(
                "staged artifact retained-log chunks are not canonical".to_owned(),
            ));
        }
        retained_log_entries.extend(chunk);
    }
    if offset != bytes.len()
        || pg_id != intent.pg_id
        || cluster_epoch > intent.source_epoch
        || destination_epoch <= intent.transition_epoch
        || !intent.source_acting_set.contains(&source_node_id)
    {
        return Err(MetadataTransferStagingError::ArtifactSemanticMismatch(
            "staged artifact does not match its authorized transition".to_owned(),
        ));
    }
    let artifact = PgMetadataTransferArtifact {
        pg_id,
        source_node_id,
        cluster_epoch,
        base_kind,
        base_proof,
        checkpoint_base,
        proof,
        retained_log_entries,
    };
    let imported_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &artifact,
        destination_epoch,
    )
    .map_err(|error| MetadataTransferStagingError::ArtifactSemanticMismatch(error.to_string()))?;
    if imported_proof != claimed_imported_proof
        || encode_staged_metadata_transfer_artifact(&artifact, destination_epoch)? != bytes
    {
        return Err(MetadataTransferStagingError::ArtifactSemanticMismatch(
            "staged artifact imported proof or canonical encoding is invalid".to_owned(),
        ));
    }
    Ok(ValidatedStagedMetadataTransferArtifact {
        artifact,
        destination_epoch,
        transfer: PgMetadataTransferProof::new_with_imported_metadata_proof(
            cluster_epoch,
            proof,
            imported_proof,
        ),
    })
}

fn validate_staged_artifact_for_publication(
    bytes: &[u8],
    intent: &MetadataTransferStagingIntent,
) -> Result<ValidatedStagedMetadataTransferArtifact, MetadataTransferStagingError> {
    validate_staged_metadata_transfer_artifact(
        bytes,
        intent,
        &MetadataCommandDecodeAuthority::new(),
    )
}

fn encode_metadata_proof(out: &mut Vec<u8>, proof: PgMetadataProof) {
    out.extend_from_slice(&proof.applied_log_index().to_be_bytes());
    out.push(proof.applied_log_hash().encoding_version());
    out.extend_from_slice(&proof.applied_log_hash().value().to_be_bytes());
    out.push(proof.state_digest().encoding_version());
    out.extend_from_slice(&proof.state_digest().value().to_be_bytes());
}

fn decode_metadata_proof(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<PgMetadataProof, MetadataTransferStagingError> {
    let applied_log_index = u64::from_be_bytes(take(bytes, offset, 8)?.try_into().unwrap());
    let applied_log_hash_encoding_version = take(bytes, offset, 1)?[0];
    let applied_log_hash = u64::from_be_bytes(take(bytes, offset, 8)?.try_into().unwrap());
    let state_digest_encoding_version = take(bytes, offset, 1)?[0];
    let state_digest = u64::from_be_bytes(take(bytes, offset, 8)?.try_into().unwrap());
    PgMetadataProof::from_encoded_parts(
        applied_log_index,
        applied_log_hash_encoding_version,
        applied_log_hash,
        state_digest_encoding_version,
        state_digest,
    )
    .map_err(|error| MetadataTransferStagingError::ArtifactSemanticMismatch(error.to_string()))
}

fn take_length_prefixed<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
) -> Result<&'a [u8], MetadataTransferStagingError> {
    let length = usize::try_from(u32::from_be_bytes(
        take(bytes, offset, 4)?.try_into().unwrap(),
    ))
    .unwrap();
    take(bytes, offset, length)
}

struct StagingInflightEvidencePage {
    page: MetadataTransferStagingEvidencePage,
    apply_receipt: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataTransferStagingEvidencePage {
    actor: MetadataTransferStagingNodeIdentity,
    actor_closure_candidate: Option<MetadataTransferStagingActorClosureCandidate>,
    previous_generation: u64,
    previous_apply_receipt_digest: [u8; DIGEST_LEN],
    generation: u64,
    entries: Vec<MetadataTransferStagingEvidencePageEntry>,
    operation_payload: Vec<u8>,
    page_digest: [u8; DIGEST_LEN],
}

impl MetadataTransferStagingEvidencePage {
    pub(crate) fn operation_payload(&self) -> &[u8] {
        &self.operation_payload
    }

    pub(crate) fn page_digest(&self) -> [u8; DIGEST_LEN] {
        self.page_digest
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn actor(&self) -> &MetadataTransferStagingNodeIdentity {
        &self.actor
    }

    pub(crate) fn actor_closure_candidate(
        &self,
    ) -> Option<&MetadataTransferStagingActorClosureCandidate> {
        self.actor_closure_candidate.as_ref()
    }

    pub(crate) fn previous_generation(&self) -> u64 {
        self.previous_generation
    }

    pub(crate) fn previous_apply_receipt_digest(&self) -> [u8; DIGEST_LEN] {
        self.previous_apply_receipt_digest
    }

    pub(crate) fn entries(&self) -> &[MetadataTransferStagingEvidencePageEntry] {
        &self.entries
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataTransferStagingEvidenceApplyReceipt {
    actor: MetadataTransferStagingNodeIdentity,
    previous_generation: u64,
    previous_apply_receipt_digest: [u8; DIGEST_LEN],
    generation: u64,
    page_digest: [u8; DIGEST_LEN],
    accepted_generation: u64,
    bytes: Vec<u8>,
}

impl MetadataTransferStagingEvidenceApplyReceipt {
    pub(crate) fn for_page(page: &MetadataTransferStagingEvidencePage) -> Self {
        let mut receipt = Self {
            actor: page.actor.clone(),
            previous_generation: page.previous_generation,
            previous_apply_receipt_digest: page.previous_apply_receipt_digest,
            generation: page.generation,
            page_digest: page.page_digest,
            accepted_generation: page.generation,
            bytes: Vec::new(),
        };
        receipt.bytes = encode_staging_evidence_apply_receipt(&receipt);
        receipt
    }

    pub(crate) fn for_checkpoint_link(
        actor: MetadataTransferStagingNodeIdentity,
        previous_generation: u64,
        previous_apply_receipt_digest: [u8; DIGEST_LEN],
        generation: u64,
        page_digest: [u8; DIGEST_LEN],
    ) -> Self {
        let mut receipt = Self {
            actor,
            previous_generation,
            previous_apply_receipt_digest,
            generation,
            page_digest,
            accepted_generation: generation,
            bytes: Vec::new(),
        };
        receipt.bytes = encode_staging_evidence_apply_receipt(&receipt);
        receipt
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn actor(&self) -> &MetadataTransferStagingNodeIdentity {
        &self.actor
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn previous_generation(&self) -> u64 {
        self.previous_generation
    }

    pub(crate) fn previous_apply_receipt_digest(&self) -> [u8; DIGEST_LEN] {
        self.previous_apply_receipt_digest
    }

    pub(crate) fn accepted_generation(&self) -> u64 {
        self.accepted_generation
    }

    pub(crate) fn page_digest(&self) -> [u8; DIGEST_LEN] {
        self.page_digest
    }

    pub(crate) fn is_for_page(&self, page: &MetadataTransferStagingEvidencePage) -> bool {
        self.actor == page.actor
            && self.previous_generation == page.previous_generation
            && self.previous_apply_receipt_digest == page.previous_apply_receipt_digest
            && self.generation == page.generation
            && self.page_digest == page.page_digest
            && self.accepted_generation == page.generation
    }
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
    evidence_page_assignment_observer: Option<StagingDurabilityObserver>,
}

impl MetadataTransferStagingStore {
    #[cfg(test)]
    pub(crate) fn has_intent_for_test(&self, pg_id: PgId, staging_generation: u64) -> bool {
        let state = self.lock_state().unwrap();
        load_staging_row(&state.connection, pg_id, staging_generation)
            .unwrap()
            .is_some()
    }

    pub(crate) fn open(
        data_dir: &Path,
        identity: MetadataTransferStagingNodeIdentity,
        limits: MetadataTransferStagingLimits,
    ) -> Result<Self, MetadataTransferStagingError> {
        Self::open_inner(data_dir, identity, limits, None, None, None)
    }

    #[cfg(test)]
    fn open_with_publication_durability_observer(
        data_dir: &Path,
        identity: MetadataTransferStagingNodeIdentity,
        limits: MetadataTransferStagingLimits,
        observer: StagingDurabilityObserver,
    ) -> Result<Self, MetadataTransferStagingError> {
        Self::open_inner(data_dir, identity, limits, None, Some(observer), None)
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
            None,
        )
    }

    #[cfg(test)]
    fn open_with_evidence_page_assignment_observer(
        data_dir: &Path,
        identity: MetadataTransferStagingNodeIdentity,
        limits: MetadataTransferStagingLimits,
        observer: StagingDurabilityObserver,
    ) -> Result<Self, MetadataTransferStagingError> {
        Self::open_inner(data_dir, identity, limits, None, None, Some(observer))
    }

    fn open_inner(
        data_dir: &Path,
        identity: MetadataTransferStagingNodeIdentity,
        limits: MetadataTransferStagingLimits,
        parent_directory_sync: Option<StagingParentDirectorySync>,
        publication_durability_observer: Option<StagingDurabilityObserver>,
        evidence_page_assignment_observer: Option<StagingDurabilityObserver>,
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
            if load_staging_evidence_actor(&connection)?.is_none() {
                insert_staging_evidence_actor(&connection, &identity)?;
            }
            sync_directory(&root, "sync initialized staging catalogue")?;
            publish_initialization_marker(&root)?;
        }
        let durable_identity = load_staging_evidence_actor(&connection)?.ok_or_else(|| {
            MetadataTransferStagingError::Invariant(
                "initialized staging store has no durable evidence actor".to_owned(),
            )
        })?;
        validate_staging_evidence_actor_open(&durable_identity, &identity)?;
        if !established {
            publish_establishment_marker(data_dir, parent_directory_sync.as_ref())?;
        }

        let mut store = Self {
            artifacts_dir,
            quarantine_dir,
            identity: durable_identity,
            limits,
            state: Mutex::new(MetadataTransferStagingState {
                connection,
                quarantined_bytes: 0,
            }),
            temp_sequence: AtomicU64::new(0),
            publication_durability_observer,
            evidence_page_assignment_observer,
        };
        store.reconcile_startup_inventory()?;
        store.rebind_staging_evidence_actor(identity)?;
        Ok(store)
    }

    fn rebind_staging_evidence_actor(
        &mut self,
        identity: MetadataTransferStagingNodeIdentity,
    ) -> Result<(), MetadataTransferStagingError> {
        if self.identity == identity {
            return Ok(());
        }
        validate_staging_evidence_actor_open(&self.identity, &identity)?;

        let mut state = self.lock_state()?;
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| {
                MetadataTransferStagingError::sql("begin staging evidence actor rollover", source)
            })?;
        let durable_identity = load_staging_evidence_actor(&transaction)?.ok_or_else(|| {
            MetadataTransferStagingError::Invariant(
                "staging evidence actor disappeared during rollover".to_owned(),
            )
        })?;
        if durable_identity != self.identity {
            return Err(MetadataTransferStagingError::Invariant(
                "staging evidence actor changed through another store handle".to_owned(),
            ));
        }
        let existing_closure_candidate =
            load_staging_evidence_actor_closure_candidate(&transaction)?;
        let old_inflight = load_inflight_evidence_page(&transaction)?;

        let rows = load_all_staging_rows(&transaction, self.limits.max_entries + 1)?;
        if rows.len() > self.limits.max_entries {
            return Err(MetadataTransferStagingError::Capacity(format!(
                "staging inventory exceeds configured entry limit {}",
                self.limits.max_entries
            )));
        }
        let mut statement = transaction
            .prepare(
                "SELECT sequence, pg_id, staging_generation, evidence_kind, evidence_bytes \
                 FROM staging_evidence_deltas ORDER BY sequence",
            )
            .map_err(|source| {
                MetadataTransferStagingError::sql("prepare staging evidence actor rollover", source)
            })?;
        let deltas = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, u32>(1)?,
                    row.get::<_, u64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                ))
            })
            .map_err(|source| {
                MetadataTransferStagingError::sql("query staging evidence actor rollover", source)
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| {
                MetadataTransferStagingError::sql("read staging evidence actor rollover", source)
            })?;
        drop(statement);

        for (sequence, pg_id, generation, kind, old_bytes) in deltas {
            let row = rows
                .iter()
                .find(|row| {
                    row.intent.pg_id == PgId::new(pg_id)
                        && row.intent.staging_generation == generation
                })
                .ok_or_else(|| {
                    MetadataTransferStagingError::Invariant(
                        "staging evidence rollover found no exact intent".to_owned(),
                    )
                })?;
            let kind = u8::try_from(kind).map_err(|_| {
                MetadataTransferStagingError::Invariant(
                    "staging evidence rollover found an invalid kind".to_owned(),
                )
            })?;
            validate_staging_evidence(&old_bytes, &row.intent, kind, &self.identity)?;
            let old_evidence = decode_staging_evidence(&old_bytes)?;
            let rebound = encode_staging_evidence(
                &identity,
                &row.intent,
                kind,
                old_evidence.target_epoch,
                old_evidence.transfer,
            );
            let changed = transaction
                .execute(
                    "UPDATE staging_evidence_deltas SET actor_node_id = ?1, \
                        actor_node_incarnation = ?2, actor_endpoint = ?3, evidence_bytes = ?4, \
                        acknowledged = 0 WHERE sequence = ?5 AND evidence_bytes = ?6",
                    params![
                        i64::from(identity.node_id.as_u32()),
                        to_sql_u64(identity.node_incarnation)?,
                        &identity.endpoint,
                        &rebound,
                        to_sql_u64(sequence)?,
                        &old_bytes,
                    ],
                )
                .map_err(|source| {
                    MetadataTransferStagingError::sql("rebind staging evidence delta actor", source)
                })?;
            if changed != 1 {
                return Err(MetadataTransferStagingError::Invariant(
                    "staging evidence changed during actor rollover".to_owned(),
                ));
            }
            if kind == 0 && row.publication_receipt.as_deref() == Some(old_bytes.as_slice()) {
                let changed = transaction
                    .execute(
                        "UPDATE staging_intents SET publication_receipt = ?1 \
                         WHERE pg_id = ?2 AND staging_generation = ?3 \
                           AND publication_receipt = ?4",
                        params![
                            &rebound,
                            i64::from(pg_id),
                            to_sql_u64(generation)?,
                            &old_bytes,
                        ],
                    )
                    .map_err(|source| {
                        MetadataTransferStagingError::sql(
                            "rebind staging publication receipt actor",
                            source,
                        )
                    })?;
                if changed != 1 {
                    return Err(MetadataTransferStagingError::Invariant(
                        "staging publication receipt changed during actor rollover".to_owned(),
                    ));
                }
            }
        }
        let closure_candidate = if let Some(existing) = existing_closure_candidate {
            let (entry_count, max_sequence, evidence_digest) =
                rebound_staging_evidence_digest(&transaction, None)?;
            Some(MetadataTransferStagingActorClosureCandidate {
                first_actor: existing.first_actor,
                first_accepted_generation: existing.first_accepted_generation,
                first_accepted_apply_receipt_digest: existing.first_accepted_apply_receipt_digest,
                first_ambiguous_page: existing.first_ambiguous_page,
                through_actor: self.identity.clone(),
                rebound_entry_count: entry_count,
                rebound_max_sequence: max_sequence,
                rebound_evidence_digest: evidence_digest,
            })
        } else if let Some(old_inflight) = old_inflight {
            let expected_receipt =
                MetadataTransferStagingEvidenceApplyReceipt::for_page(&old_inflight.page);
            let (
                first_accepted_generation,
                first_accepted_apply_receipt_digest,
                first_ambiguous_page,
            ) = if let Some(receipt) = old_inflight.apply_receipt.as_deref() {
                let receipt = decode_staging_evidence_apply_receipt(receipt)?;
                require_apply_receipt_for_page(&receipt, &old_inflight.page)?;
                (
                    old_inflight.page.generation(),
                    checksum::sha256::digest(receipt.as_bytes()),
                    None,
                )
            } else {
                (
                    old_inflight.page.previous_generation(),
                    old_inflight.page.previous_apply_receipt_digest(),
                    Some(MetadataTransferStagingActorClosureAmbiguousPage {
                        generation: old_inflight.page.generation(),
                        page_digest: old_inflight.page.page_digest(),
                        apply_receipt_digest: checksum::sha256::digest(expected_receipt.as_bytes()),
                    }),
                )
            };
            let (entry_count, max_sequence, evidence_digest) =
                rebound_staging_evidence_digest(&transaction, None)?;
            Some(MetadataTransferStagingActorClosureCandidate {
                first_actor: self.identity.clone(),
                first_accepted_generation,
                first_accepted_apply_receipt_digest,
                first_ambiguous_page,
                through_actor: self.identity.clone(),
                rebound_entry_count: entry_count,
                rebound_max_sequence: max_sequence,
                rebound_evidence_digest: evidence_digest,
            })
        } else {
            None
        };
        persist_staging_evidence_actor_closure_candidate(&transaction, closure_candidate.as_ref())?;
        transaction
            .execute("DELETE FROM staging_evidence_inflight_page", [])
            .map_err(|source| {
                MetadataTransferStagingError::sql(
                    "reset staging evidence page chain for actor rollover",
                    source,
                )
            })?;
        update_staging_evidence_actor(&transaction, &self.identity, &identity)?;
        transaction.commit().map_err(|source| {
            MetadataTransferStagingError::sql("commit staging evidence actor rollover", source)
        })?;
        drop(state);
        self.identity = identity;
        Ok(())
    }

    fn require_current_evidence_actor(
        &self,
        connection: &Connection,
    ) -> Result<(), MetadataTransferStagingError> {
        if load_staging_evidence_actor(connection)?.as_ref() != Some(&self.identity) {
            return Err(MetadataTransferStagingError::Invariant(
                "staging-store handle has a stale evidence actor".to_owned(),
            ));
        }
        Ok(())
    }

    fn create_intent_inner(
        &self,
        intent: &MetadataTransferStagingIntent,
    ) -> Result<MetadataTransferStagingIntentOutcome, MetadataTransferStagingError> {
        self.validate_intent_limits(intent)?;
        let mut state = self.lock_state()?;
        let quarantined_bytes = state.quarantined_bytes;
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| {
                MetadataTransferStagingError::sql("begin staging intent creation", source)
            })?;
        self.require_current_evidence_actor(&transaction)?;
        if finalized_floor(&transaction, intent.pg_id)? >= intent.staging_generation {
            return Err(MetadataTransferStagingError::GenerationRetired);
        }
        if let Some(existing) =
            load_staging_row(&transaction, intent.pg_id, intent.staging_generation)?
        {
            require_exact_intent(&existing.intent, intent)?;
            if existing.state == StagingState::Tombstoned {
                return Err(MetadataTransferStagingError::GenerationRetired);
            }
            return Ok(MetadataTransferStagingIntentOutcome::ExactReplay);
        }
        let (entry_count, reserved_bytes) = staging_capacity(&transaction)?;
        if entry_count >= self.limits.max_entries {
            return Err(MetadataTransferStagingError::Capacity(format!(
                "entry limit {} reached",
                self.limits.max_entries
            )));
        }
        let total = reserved_bytes
            .checked_add(quarantined_bytes)
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
        insert_intent(&transaction, intent, StagingState::Intent, None)?;
        transaction.commit().map_err(|source| {
            MetadataTransferStagingError::sql("commit staging intent creation", source)
        })?;
        Ok(MetadataTransferStagingIntentOutcome::Created)
    }

    pub(crate) fn create_intent_authorized(
        &self,
        authorization: &CommittedUnavailablePgStagingAuthorization,
        intent: &MetadataTransferStagingIntent,
    ) -> Result<MetadataTransferStagingIntentOutcome, MetadataTransferStagingError> {
        validate_committed_staging_authorization(&self.identity, authorization, intent)?;
        self.create_intent_inner(intent)
    }

    #[cfg(test)]
    pub(crate) fn create_intent(
        &self,
        intent: &MetadataTransferStagingIntent,
    ) -> Result<MetadataTransferStagingIntentOutcome, MetadataTransferStagingError> {
        self.create_intent_inner(intent)
    }

    fn publish_artifact_inner(
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
        let validated_artifact = validate_staged_artifact_for_publication(artifact, intent)?;
        let mut state = self.lock_state()?;
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| {
                MetadataTransferStagingError::sql("begin staging publication", source)
            })?;
        self.require_current_evidence_actor(&transaction)?;
        let existing = load_staging_row(&transaction, intent.pg_id, intent.staging_generation)?
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
                let delta = load_evidence_delta(
                    &transaction,
                    intent.pg_id,
                    intent.staging_generation,
                    0,
                    Some(validated_artifact.destination_epoch),
                )?
                .ok_or_else(|| {
                    MetadataTransferStagingError::Invariant(
                        "published staging row has no exact epoch-bound evidence delta".to_owned(),
                    )
                })?;
                let receipt = delta.bytes.clone();
                validate_staging_evidence(&receipt, intent, 0, &delta.actor)?;
                let evidence = decode_staging_evidence(&receipt)?;
                if evidence.target_epoch != Some(validated_artifact.destination_epoch)
                    || evidence.transfer != Some(validated_artifact.transfer)
                {
                    return Err(MetadataTransferStagingError::Invariant(
                        "staging publication receipt has the wrong semantic transfer proof"
                            .to_owned(),
                    ));
                }
                return Ok(MetadataTransferStagingReceipt { bytes: receipt });
            }
            StagingState::Intent => {}
        }

        self.publish_artifact_file(intent, artifact)?;
        let receipt = encode_staging_evidence(
            &self.identity,
            intent,
            0,
            Some(validated_artifact.destination_epoch),
            Some(validated_artifact.transfer),
        );
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
        insert_evidence_delta(
            &transaction,
            &self.identity,
            intent,
            0,
            Some(validated_artifact.destination_epoch),
            &receipt,
        )?;
        transaction.commit().map_err(|source| {
            MetadataTransferStagingError::sql("commit staging publication", source)
        })?;
        Ok(MetadataTransferStagingReceipt { bytes: receipt })
    }

    pub(crate) fn publish_artifact_authorized(
        &self,
        authorization: &CommittedUnavailablePgStagingAuthorization,
        intent: &MetadataTransferStagingIntent,
        artifact: &[u8],
    ) -> Result<MetadataTransferStagingReceipt, MetadataTransferStagingError> {
        validate_committed_staging_authorization(&self.identity, authorization, intent)?;
        self.publish_artifact_inner(intent, artifact)
    }

    #[cfg(test)]
    pub(crate) fn publish_artifact(
        &self,
        intent: &MetadataTransferStagingIntent,
        artifact: &[u8],
    ) -> Result<MetadataTransferStagingReceipt, MetadataTransferStagingError> {
        self.publish_artifact_inner(intent, artifact)
    }

    fn publish_proof_for_epoch_inner(
        &self,
        intent: &MetadataTransferStagingIntent,
        target_epoch: ClusterEpoch,
    ) -> Result<MetadataTransferStagingReceipt, MetadataTransferStagingError> {
        self.validate_intent_limits(intent)?;
        if target_epoch <= intent.transition_epoch {
            return Err(MetadataTransferStagingError::Invariant(
                "staging proof target epoch must follow the transition epoch".to_owned(),
            ));
        }
        let mut state = self.lock_state()?;
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| {
                MetadataTransferStagingError::sql("begin staging proof publication", source)
            })?;
        self.require_current_evidence_actor(&transaction)?;
        let existing = load_staging_row(&transaction, intent.pg_id, intent.staging_generation)?
            .ok_or_else(|| {
                MetadataTransferStagingError::IntentConflict(
                    "proof publication has no durable staged artifact".to_owned(),
                )
            })?;
        require_exact_intent(&existing.intent, intent)?;
        match existing.state {
            StagingState::Intent => {
                return Err(MetadataTransferStagingError::IntentConflict(
                    "artifact has not been durably published".to_owned(),
                ))
            }
            StagingState::Tombstoned => {
                return Err(MetadataTransferStagingError::GenerationRetired)
            }
            StagingState::Published | StagingState::Imported => {}
        }
        if let Some(delta) = load_evidence_delta(
            &transaction,
            intent.pg_id,
            intent.staging_generation,
            0,
            Some(target_epoch),
        )? {
            validate_staging_evidence(&delta.bytes, intent, 0, &delta.actor)?;
            let evidence = decode_staging_evidence(&delta.bytes)?;
            if evidence.target_epoch != Some(target_epoch) {
                return Err(MetadataTransferStagingError::Invariant(
                    "staging proof replay has the wrong target epoch".to_owned(),
                ));
            }
            return Ok(MetadataTransferStagingReceipt { bytes: delta.bytes });
        }
        if existing.state == StagingState::Imported {
            return Err(MetadataTransferStagingError::IntentConflict(
                "imported staging artifact cannot publish a proof for a new epoch".to_owned(),
            ));
        }
        let proof_count: usize = transaction
            .query_row(
                "SELECT COUNT(*) FROM staging_evidence_deltas \
                 WHERE pg_id = ?1 AND staging_generation = ?2 AND evidence_kind = 0",
                params![
                    i64::from(intent.pg_id.get()),
                    to_sql_u64(intent.staging_generation)?,
                ],
                |row| row.get(0),
            )
            .map_err(|source| {
                MetadataTransferStagingError::sql("count staged epoch proofs", source)
            })?;
        if proof_count >= MAX_STAGING_EPOCH_PROOFS_PER_INTENT {
            return Err(MetadataTransferStagingError::Capacity(format!(
                "epoch-proof limit {MAX_STAGING_EPOCH_PROOFS_PER_INTENT} reached for the intent"
            )));
        }
        let artifact = read_artifact_file(&self.artifact_path(intent), intent)?;
        let validated = validate_staged_artifact_for_publication(&artifact, intent)?;
        let imported_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
            &validated.artifact,
            target_epoch,
        )
        .map_err(|error| {
            MetadataTransferStagingError::ArtifactSemanticMismatch(error.to_string())
        })?;
        let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
            validated.artifact.cluster_epoch,
            validated.artifact.proof,
            imported_proof,
        );
        let receipt = encode_staging_evidence(
            &self.identity,
            intent,
            0,
            Some(target_epoch),
            Some(transfer),
        );
        let changed = transaction
            .execute(
                "UPDATE staging_intents SET publication_receipt = ?1 \
                 WHERE pg_id = ?2 AND staging_generation = ?3 AND state = 1",
                params![
                    &receipt,
                    i64::from(intent.pg_id.get()),
                    to_sql_u64(intent.staging_generation)?,
                ],
            )
            .map_err(|source| MetadataTransferStagingError::sql("publish staging proof", source))?;
        if changed != 1 {
            return Err(MetadataTransferStagingError::Invariant(
                "staging proof publication lost its exact published intent".to_owned(),
            ));
        }
        insert_evidence_delta(
            &transaction,
            &self.identity,
            intent,
            0,
            Some(target_epoch),
            &receipt,
        )?;
        transaction.commit().map_err(|source| {
            MetadataTransferStagingError::sql("commit staging proof publication", source)
        })?;
        Ok(MetadataTransferStagingReceipt { bytes: receipt })
    }

    pub(crate) fn publish_proof_for_epoch_authorized(
        &self,
        authorization: &CommittedUnavailablePgStagingAuthorization,
        intent: &MetadataTransferStagingIntent,
        target_epoch: ClusterEpoch,
    ) -> Result<MetadataTransferStagingReceipt, MetadataTransferStagingError> {
        validate_committed_staging_authorization(&self.identity, authorization, intent)?;
        self.publish_proof_for_epoch_inner(intent, target_epoch)
    }

    #[cfg(test)]
    pub(crate) fn publish_proof_for_epoch(
        &self,
        intent: &MetadataTransferStagingIntent,
        target_epoch: ClusterEpoch,
    ) -> Result<MetadataTransferStagingReceipt, MetadataTransferStagingError> {
        self.publish_proof_for_epoch_inner(intent, target_epoch)
    }

    pub(crate) fn mark_imported(
        &self,
        intent: &MetadataTransferStagingIntent,
    ) -> Result<(), MetadataTransferStagingError> {
        let mut state = self.lock_state()?;
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| {
                MetadataTransferStagingError::sql("begin staged artifact import", source)
            })?;
        self.require_current_evidence_actor(&transaction)?;
        let existing = load_staging_row(&transaction, intent.pg_id, intent.staging_generation)?
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
                let changed = transaction
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
                transaction.commit().map_err(|source| {
                    MetadataTransferStagingError::sql("commit staged artifact import", source)
                })?;
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

    fn tombstone_inner(
        &self,
        intent: &MetadataTransferStagingIntent,
    ) -> Result<MetadataTransferStagingReceipt, MetadataTransferStagingError> {
        self.validate_intent_limits(intent)?;
        let mut state = self.lock_state()?;
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| {
                MetadataTransferStagingError::sql("begin staging tombstone", source)
            })?;
        self.require_current_evidence_actor(&transaction)?;
        if finalized_floor(&transaction, intent.pg_id)? >= intent.staging_generation {
            return Err(MetadataTransferStagingError::GenerationRetired);
        }
        if let Some(existing) =
            load_staging_row(&transaction, intent.pg_id, intent.staging_generation)?
        {
            require_exact_intent(&existing.intent, intent)?;
            if existing.state == StagingState::Tombstoned {
                let receipt = load_evidence_delta(
                    &transaction,
                    intent.pg_id,
                    intent.staging_generation,
                    1,
                    None,
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
            let (entry_count, _) = staging_capacity(&transaction)?;
            if entry_count >= self.limits.max_entries {
                return Err(MetadataTransferStagingError::Capacity(format!(
                    "entry limit {} reached",
                    self.limits.max_entries
                )));
            }
        }
        let receipt = encode_staging_evidence(&self.identity, intent, 1, None, None);
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
        insert_evidence_delta(&transaction, &self.identity, intent, 1, None, &receipt)?;
        transaction.commit().map_err(|source| {
            MetadataTransferStagingError::sql("commit staging tombstone", source)
        })?;
        remove_artifact_if_present(&self.artifact_path(intent))?;
        sync_directory(&self.artifacts_dir, "sync staged artifact removal")?;
        Ok(MetadataTransferStagingReceipt { bytes: receipt })
    }

    #[cfg(test)]
    pub(crate) fn tombstone(
        &self,
        intent: &MetadataTransferStagingIntent,
    ) -> Result<MetadataTransferStagingReceipt, MetadataTransferStagingError> {
        self.tombstone_inner(intent)
    }

    pub(crate) fn tombstone_authorized(
        &self,
        authorization: &CommittedUnavailablePgStagingAuthorization,
        intent: &MetadataTransferStagingIntent,
    ) -> Result<MetadataTransferStagingReceipt, MetadataTransferStagingError> {
        validate_committed_staging_authorization(&self.identity, authorization, intent)?;
        self.tombstone_inner(intent)
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
        let mut state = self.lock_state()?;
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| {
                MetadataTransferStagingError::sql("begin staging finalized-floor advance", source)
            })?;
        self.require_current_evidence_actor(&transaction)?;
        let current = finalized_floor(&transaction, pg_id)?;
        if staging_generation < current {
            return Err(MetadataTransferStagingError::Invariant(format!(
                "staging finalized floor regressed from {current} to {staging_generation}"
            )));
        }
        let live: i64 = transaction
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
        transaction
            .execute(
                "INSERT INTO staging_finalized_floors (pg_id, staging_generation) VALUES (?1, ?2)\
                 ON CONFLICT(pg_id) DO UPDATE SET staging_generation = excluded.staging_generation",
                params![i64::from(pg_id.get()), to_sql_u64(staging_generation)?],
            )
            .map_err(|source| {
                MetadataTransferStagingError::sql("advance staging finalized floor", source)
            })?;
        transaction.commit().map_err(|source| {
            MetadataTransferStagingError::sql("commit staging finalized-floor advance", source)
        })?;
        Ok(())
    }

    pub(crate) fn next_evidence_page(
        &self,
    ) -> Result<Option<MetadataTransferStagingEvidencePage>, MetadataTransferStagingError> {
        let mut state = self.lock_state()?;
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| {
                MetadataTransferStagingError::sql("begin staging evidence page assignment", source)
            })?;
        self.require_current_evidence_actor(&transaction)?;
        let result = if let Some(inflight) = load_inflight_evidence_page(&transaction)? {
            let page = inflight.page;
            let apply_receipt = inflight.apply_receipt;
            if apply_receipt.is_none() {
                Some(page)
            } else if unacknowledged_evidence_exists(&transaction)? {
                let receipt =
                    decode_staging_evidence_apply_receipt(apply_receipt.as_deref().unwrap())?;
                require_apply_receipt_for_page(&receipt, &page)?;
                let predecessor_digest = checksum::sha256::digest(receipt.as_bytes());
                let successor = build_staging_evidence_page(
                    &transaction,
                    &self.identity,
                    None,
                    page.generation,
                    predecessor_digest,
                )?
                .ok_or_else(|| {
                    MetadataTransferStagingError::Invariant(
                        "staging evidence disappeared while assigning its successor page"
                            .to_owned(),
                    )
                })?;
                if let Some(observer) = &self.evidence_page_assignment_observer {
                    observer();
                }
                persist_inflight_evidence_page(&transaction, &successor)?;
                Some(successor)
            } else {
                None
            }
        } else {
            let closure_candidate = load_staging_evidence_actor_closure_candidate(&transaction)?;
            let page = build_staging_evidence_page(
                &transaction,
                &self.identity,
                closure_candidate.as_ref(),
                0,
                [0; DIGEST_LEN],
            )?;
            if let Some(page) = page {
                if let Some(observer) = &self.evidence_page_assignment_observer {
                    observer();
                }
                persist_inflight_evidence_page(&transaction, &page)?;
                Some(page)
            } else {
                None
            }
        };
        transaction.commit().map_err(|source| {
            MetadataTransferStagingError::sql("commit staging evidence page assignment", source)
        })?;
        Ok(result)
    }

    pub(crate) fn record_evidence_apply_receipt(
        &self,
        page: &MetadataTransferStagingEvidencePage,
        receipt: &MetadataTransferStagingEvidenceApplyReceipt,
    ) -> Result<(), MetadataTransferStagingError> {
        require_apply_receipt_for_page(receipt, page)?;
        let mut state = self.lock_state()?;
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| {
                MetadataTransferStagingError::sql("begin staging evidence acknowledgement", source)
            })?;
        self.require_current_evidence_actor(&transaction)?;
        let current = load_inflight_evidence_page(&transaction)?.ok_or_else(|| {
            MetadataTransferStagingError::IntentConflict(
                "staging evidence acknowledgement has no in-flight page".to_owned(),
            )
        })?;
        require_exact_evidence_page(&current.page, page)?;

        if let Some(current_receipt) = current.apply_receipt {
            if current_receipt != receipt.as_bytes() {
                return Err(MetadataTransferStagingError::IntentConflict(
                    "staging evidence acknowledgement differs from the durable receipt".to_owned(),
                ));
            }
            for entry in &page.entries {
                let member: Option<(Vec<u8>, i64)> = transaction
                    .query_row(
                        "SELECT evidence_bytes, acknowledged FROM staging_evidence_deltas \
                         WHERE sequence = ?1",
                        [to_sql_u64(entry.sequence)?],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()
                    .map_err(|source| {
                        MetadataTransferStagingError::sql(
                            "validate replayed staging evidence acknowledgement",
                            source,
                        )
                    })?;
                if member.as_ref().is_none_or(|(evidence, acknowledged)| {
                    evidence != &entry.evidence || *acknowledged != 1
                }) {
                    return Err(MetadataTransferStagingError::Invariant(
                        "replayed staging evidence acknowledgement lost a durable member"
                            .to_owned(),
                    ));
                }
            }
        } else {
            for entry in &page.entries {
                let (evidence, acknowledged): (Vec<u8>, i64) = transaction
                    .query_row(
                        "SELECT evidence_bytes, acknowledged FROM staging_evidence_deltas \
                         WHERE sequence = ?1",
                        [to_sql_u64(entry.sequence)?],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(|source| {
                        MetadataTransferStagingError::sql(
                            "load staging evidence acknowledgement member",
                            source,
                        )
                    })?;
                if evidence != entry.evidence || acknowledged != 0 {
                    return Err(MetadataTransferStagingError::Invariant(
                        "staging evidence acknowledgement does not match its durable member"
                            .to_owned(),
                    ));
                }
            }
            let changed = transaction
                .execute(
                    "UPDATE staging_evidence_inflight_page SET apply_receipt = ?1 \
                     WHERE singleton = 1 AND apply_receipt IS NULL",
                    params![receipt.as_bytes()],
                )
                .map_err(|source| {
                    MetadataTransferStagingError::sql(
                        "record staging evidence apply receipt",
                        source,
                    )
                })?;
            if changed != 1 {
                return Err(MetadataTransferStagingError::Invariant(
                    "staging evidence acknowledgement lost its in-flight page".to_owned(),
                ));
            }
            for entry in &page.entries {
                let changed = transaction
                    .execute(
                        "UPDATE staging_evidence_deltas SET acknowledged = 1 \
                         WHERE sequence = ?1 AND evidence_bytes = ?2 AND acknowledged = 0",
                        params![to_sql_u64(entry.sequence)?, &entry.evidence],
                    )
                    .map_err(|source| {
                        MetadataTransferStagingError::sql(
                            "acknowledge staging evidence member",
                            source,
                        )
                    })?;
                if changed != 1 {
                    return Err(MetadataTransferStagingError::Invariant(
                        "staging evidence acknowledgement lost a page member".to_owned(),
                    ));
                }
            }
        }
        transaction.commit().map_err(|source| {
            MetadataTransferStagingError::sql("commit staging evidence acknowledgement", source)
        })
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
        if !intent
            .destination_acting_set
            .contains(&self.identity.node_id)
        {
            return Err(MetadataTransferStagingError::Invariant(
                "staging-store actor is not a destination for the intent".to_owned(),
            ));
        }
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
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| {
                MetadataTransferStagingError::sql("begin staging startup reconciliation", source)
            })?;
        self.require_current_evidence_actor(&transaction)?;
        let rows = load_all_staging_rows(&transaction, self.limits.max_entries + 1)?;
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
        validate_auxiliary_catalogue(&transaction, &rows, self.limits.max_entries)?;

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
            let artifact = read_artifact_file(&self.artifact_path(&row.intent), &row.intent)?;
            let validated_artifact =
                validate_staged_artifact_for_publication(&artifact, &row.intent)?;
            let receipt = encode_staging_evidence(
                &self.identity,
                &row.intent,
                0,
                Some(validated_artifact.destination_epoch),
                Some(validated_artifact.transfer),
            );
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
            insert_evidence_delta(
                &transaction,
                &self.identity,
                &row.intent,
                0,
                Some(validated_artifact.destination_epoch),
                &receipt,
            )?;
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
        transaction.commit().map_err(|source| {
            MetadataTransferStagingError::sql("commit staging startup reconciliation", source)
        })?;
        state.quarantined_bytes = quarantined_bytes;
        Ok(())
    }
}

fn load_staging_evidence_actor(
    connection: &Connection,
) -> Result<Option<MetadataTransferStagingNodeIdentity>, MetadataTransferStagingError> {
    connection
        .query_row(
            "SELECT node_id, node_incarnation, endpoint FROM staging_evidence_actor \
             WHERE singleton = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, u32>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(|source| MetadataTransferStagingError::sql("load staging evidence actor", source))?
        .map(|(node_id, incarnation, endpoint)| {
            MetadataTransferStagingNodeIdentity::new(NodeId::new(node_id), incarnation, endpoint)
        })
        .transpose()
}

fn load_staging_evidence_actor_closure_candidate(
    connection: &Connection,
) -> Result<Option<MetadataTransferStagingActorClosureCandidate>, MetadataTransferStagingError> {
    connection
        .query_row(
            "SELECT first_node_id, first_node_incarnation, first_endpoint, \
                    first_accepted_generation, first_accepted_apply_receipt_digest, \
                    first_ambiguous_generation, first_ambiguous_page_digest, \
                    first_ambiguous_apply_receipt_digest, through_node_id, \
                    through_node_incarnation, through_endpoint, rebound_entry_count, \
                    rebound_max_sequence, rebound_evidence_digest \
             FROM staging_evidence_actor_closure_candidate WHERE singleton = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, u32>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, u64>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, Option<u64>>(5)?,
                    row.get::<_, Option<Vec<u8>>>(6)?,
                    row.get::<_, Option<Vec<u8>>>(7)?,
                    row.get::<_, u32>(8)?,
                    row.get::<_, u64>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, u64>(11)?,
                    row.get::<_, u64>(12)?,
                    row.get::<_, Vec<u8>>(13)?,
                ))
            },
        )
        .optional()
        .map_err(|source| {
            MetadataTransferStagingError::sql("load staging evidence actor closure", source)
        })?
        .map(
            |(
                first_node_id,
                first_node_incarnation,
                first_endpoint,
                first_accepted_generation,
                first_accepted_apply_receipt_digest,
                first_ambiguous_generation,
                first_ambiguous_page_digest,
                first_ambiguous_apply_receipt_digest,
                through_node_id,
                through_node_incarnation,
                through_endpoint,
                rebound_entry_count,
                rebound_max_sequence,
                rebound_evidence_digest,
            )| {
                let candidate = MetadataTransferStagingActorClosureCandidate {
                    first_actor: MetadataTransferStagingNodeIdentity::new(
                        NodeId::new(first_node_id),
                        first_node_incarnation,
                        first_endpoint,
                    )?,
                    first_accepted_generation,
                    first_accepted_apply_receipt_digest: first_accepted_apply_receipt_digest
                        .try_into()
                        .map_err(|_| {
                            MetadataTransferStagingError::Invariant(
                                "staging actor closure accepted receipt digest has invalid length"
                                    .to_owned(),
                            )
                        })?,
                    first_ambiguous_page: match (
                        first_ambiguous_generation,
                        first_ambiguous_page_digest,
                        first_ambiguous_apply_receipt_digest,
                    ) {
                        (None, None, None) => None,
                        (Some(generation), Some(page_digest), Some(apply_receipt_digest)) => {
                            Some(MetadataTransferStagingActorClosureAmbiguousPage {
                                generation,
                                page_digest: page_digest.try_into().map_err(|_| {
                                    MetadataTransferStagingError::Invariant(
                                        "staging actor closure ambiguous page digest has invalid length"
                                            .to_owned(),
                                    )
                                })?,
                                apply_receipt_digest: apply_receipt_digest.try_into().map_err(
                                    |_| {
                                        MetadataTransferStagingError::Invariant(
                                            "staging actor closure ambiguous receipt digest has invalid length"
                                                .to_owned(),
                                        )
                                    },
                                )?,
                            })
                        }
                        _ => {
                            return Err(MetadataTransferStagingError::Invariant(
                                "staging actor closure has incomplete ambiguous page evidence"
                                    .to_owned(),
                            ));
                        }
                    },
                    through_actor: MetadataTransferStagingNodeIdentity::new(
                        NodeId::new(through_node_id),
                        through_node_incarnation,
                        through_endpoint,
                    )?,
                    rebound_entry_count,
                    rebound_max_sequence,
                    rebound_evidence_digest: rebound_evidence_digest.try_into().map_err(|_| {
                        MetadataTransferStagingError::Invariant(
                            "staging actor closure evidence digest has invalid length".to_owned(),
                        )
                    })?,
                };
                validate_staging_evidence_actor_closure_candidate(&candidate)?;
                Ok(candidate)
            },
        )
        .transpose()
}

fn persist_staging_evidence_actor_closure_candidate(
    connection: &Connection,
    candidate: Option<&MetadataTransferStagingActorClosureCandidate>,
) -> Result<(), MetadataTransferStagingError> {
    if let Some(candidate) = candidate {
        validate_staging_evidence_actor_closure_candidate(candidate)?;
        connection
            .execute(
                "INSERT INTO staging_evidence_actor_closure_candidate (\
                    singleton, first_node_id, first_node_incarnation, first_endpoint, \
                    first_accepted_generation, first_accepted_apply_receipt_digest, \
                    first_ambiguous_generation, first_ambiguous_page_digest, \
                    first_ambiguous_apply_receipt_digest, through_node_id, \
                    through_node_incarnation, through_endpoint, rebound_entry_count, \
                    rebound_max_sequence, rebound_evidence_digest\
                 ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14) \
                 ON CONFLICT(singleton) DO UPDATE SET \
                    first_node_id = excluded.first_node_id, \
                    first_node_incarnation = excluded.first_node_incarnation, \
                    first_endpoint = excluded.first_endpoint, \
                    first_accepted_generation = excluded.first_accepted_generation, \
                    first_accepted_apply_receipt_digest = excluded.first_accepted_apply_receipt_digest, \
                    first_ambiguous_generation = excluded.first_ambiguous_generation, \
                    first_ambiguous_page_digest = excluded.first_ambiguous_page_digest, \
                    first_ambiguous_apply_receipt_digest = excluded.first_ambiguous_apply_receipt_digest, \
                    through_node_id = excluded.through_node_id, \
                    through_node_incarnation = excluded.through_node_incarnation, \
                    through_endpoint = excluded.through_endpoint, \
                    rebound_entry_count = excluded.rebound_entry_count, \
                    rebound_max_sequence = excluded.rebound_max_sequence, \
                    rebound_evidence_digest = excluded.rebound_evidence_digest",
                params![
                    i64::from(candidate.first_actor.node_id().as_u32()),
                    to_sql_u64(candidate.first_actor.node_incarnation())?,
                    candidate.first_actor.endpoint(),
                    to_sql_u64(candidate.first_accepted_generation)?,
                    &candidate.first_accepted_apply_receipt_digest[..],
                    candidate
                        .first_ambiguous_page
                        .as_ref()
                        .map(|page| to_sql_u64(page.generation))
                        .transpose()?,
                    candidate
                        .first_ambiguous_page
                        .as_ref()
                        .map(|page| page.page_digest.as_slice()),
                    candidate
                        .first_ambiguous_page
                        .as_ref()
                        .map(|page| page.apply_receipt_digest.as_slice()),
                    i64::from(candidate.through_actor.node_id().as_u32()),
                    to_sql_u64(candidate.through_actor.node_incarnation())?,
                    candidate.through_actor.endpoint(),
                    to_sql_u64(candidate.rebound_entry_count)?,
                    to_sql_u64(candidate.rebound_max_sequence)?,
                    &candidate.rebound_evidence_digest[..],
                ],
            )
            .map_err(|source| {
                MetadataTransferStagingError::sql("persist staging evidence actor closure", source)
            })?;
    } else {
        connection
            .execute("DELETE FROM staging_evidence_actor_closure_candidate", [])
            .map_err(|source| {
                MetadataTransferStagingError::sql("clear staging evidence actor closure", source)
            })?;
    }
    Ok(())
}

fn validate_staging_evidence_actor_closure_candidate(
    candidate: &MetadataTransferStagingActorClosureCandidate,
) -> Result<(), MetadataTransferStagingError> {
    let accepted_is_genesis = candidate.first_accepted_generation == 0;
    let accepted_digest_is_genesis =
        candidate.first_accepted_apply_receipt_digest == [0; DIGEST_LEN];
    if candidate.first_actor.node_id() != candidate.through_actor.node_id()
        || candidate.first_actor.node_incarnation() > candidate.through_actor.node_incarnation()
        || (candidate.first_actor.node_incarnation() == candidate.through_actor.node_incarnation()
            && candidate.first_actor != candidate.through_actor)
        || accepted_is_genesis != accepted_digest_is_genesis
        || (accepted_is_genesis && candidate.first_ambiguous_page.is_none())
        || candidate.first_ambiguous_page.as_ref().is_some_and(|page| {
            page.generation
                != candidate
                    .first_accepted_generation
                    .checked_add(1)
                    .unwrap_or(0)
        })
        || candidate.rebound_entry_count == 0
        || candidate.rebound_max_sequence == 0
        || candidate.rebound_entry_count > candidate.rebound_max_sequence
    {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence actor closure candidate is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn encode_staging_evidence_actor_closure_candidate(
    out: &mut Vec<u8>,
    candidate: &MetadataTransferStagingActorClosureCandidate,
) {
    encode_staging_evidence_actor(out, &candidate.first_actor);
    out.extend_from_slice(&candidate.first_accepted_generation.to_be_bytes());
    out.extend_from_slice(&candidate.first_accepted_apply_receipt_digest);
    if let Some(page) = &candidate.first_ambiguous_page {
        out.push(1);
        out.extend_from_slice(&page.generation.to_be_bytes());
        out.extend_from_slice(&page.page_digest);
        out.extend_from_slice(&page.apply_receipt_digest);
    } else {
        out.push(0);
    }
    encode_staging_evidence_actor(out, &candidate.through_actor);
    out.extend_from_slice(&candidate.rebound_entry_count.to_be_bytes());
    out.extend_from_slice(&candidate.rebound_max_sequence.to_be_bytes());
    out.extend_from_slice(&candidate.rebound_evidence_digest);
}

fn decode_staging_evidence_actor_closure_candidate(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<MetadataTransferStagingActorClosureCandidate, MetadataTransferStagingError> {
    let first_actor = decode_staging_evidence_actor(bytes, offset)?;
    let first_accepted_generation = u64::from_be_bytes(take(bytes, offset, 8)?.try_into().unwrap());
    let first_accepted_apply_receipt_digest = take(bytes, offset, DIGEST_LEN)?.try_into().unwrap();
    let first_ambiguous_page = match take(bytes, offset, 1)?[0] {
        0 => None,
        1 => Some(MetadataTransferStagingActorClosureAmbiguousPage {
            generation: u64::from_be_bytes(take(bytes, offset, 8)?.try_into().unwrap()),
            page_digest: take(bytes, offset, DIGEST_LEN)?.try_into().unwrap(),
            apply_receipt_digest: take(bytes, offset, DIGEST_LEN)?.try_into().unwrap(),
        }),
        _ => {
            return Err(MetadataTransferStagingError::Invariant(
                "staging actor closure has an invalid ambiguous page tag".to_owned(),
            ));
        }
    };
    let through_actor = decode_staging_evidence_actor(bytes, offset)?;
    let rebound_entry_count = u64::from_be_bytes(take(bytes, offset, 8)?.try_into().unwrap());
    let rebound_max_sequence = u64::from_be_bytes(take(bytes, offset, 8)?.try_into().unwrap());
    let rebound_evidence_digest = take(bytes, offset, DIGEST_LEN)?.try_into().unwrap();
    let candidate = MetadataTransferStagingActorClosureCandidate {
        first_actor,
        first_accepted_generation,
        first_accepted_apply_receipt_digest,
        first_ambiguous_page,
        through_actor,
        rebound_entry_count,
        rebound_max_sequence,
        rebound_evidence_digest,
    };
    validate_staging_evidence_actor_closure_candidate(&candidate)?;
    Ok(candidate)
}

fn rebound_staging_evidence_digest(
    connection: &Connection,
    through_sequence: Option<u64>,
) -> Result<(u64, u64, [u8; DIGEST_LEN]), MetadataTransferStagingError> {
    let mut statement = connection
        .prepare(
            "SELECT sequence, evidence_bytes FROM staging_evidence_deltas \
             WHERE sequence <= ?1 ORDER BY sequence",
        )
        .map_err(|source| {
            MetadataTransferStagingError::sql("prepare rebound staging evidence digest", source)
        })?;
    let rows = statement
        .query_map(
            [through_sequence.map_or(Ok(i64::MAX), to_sql_u64)?],
            |row| Ok((row.get::<_, u64>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .map_err(|source| {
            MetadataTransferStagingError::sql("query rebound staging evidence digest", source)
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| {
            MetadataTransferStagingError::sql("read rebound staging evidence digest", source)
        })?;
    metadata_transfer_staging_rebound_evidence_digest(
        rows.iter()
            .map(|(sequence, evidence)| (*sequence, evidence.as_slice())),
    )
}

pub(crate) fn metadata_transfer_staging_rebound_evidence_digest<'a>(
    entries: impl IntoIterator<Item = (u64, &'a [u8])>,
) -> Result<(u64, u64, [u8; DIGEST_LEN]), MetadataTransferStagingError> {
    let rows = entries.into_iter().collect::<Vec<_>>();
    let entry_count = u64::try_from(rows.len()).map_err(|_| {
        MetadataTransferStagingError::Invariant(
            "staging evidence entry count does not fit u64".to_owned(),
        )
    })?;
    let max_sequence = rows.last().map_or(0, |(sequence, _)| *sequence);
    if entry_count == 0 || max_sequence == 0 {
        return Err(MetadataTransferStagingError::Invariant(
            "staging actor closure has no rebound evidence".to_owned(),
        ));
    }
    let mut context = checksum::sha256::Sha256::new();
    context.update(b"argmin-staging-evidence-rebound-v1\0");
    context.update(&entry_count.to_be_bytes());
    context.update(&max_sequence.to_be_bytes());
    let mut previous_sequence = 0;
    for (sequence, evidence) in rows {
        if sequence <= previous_sequence {
            return Err(MetadataTransferStagingError::Invariant(
                "staging evidence rebound entries are not strictly ordered".to_owned(),
            ));
        }
        previous_sequence = sequence;
        context.update(&sequence.to_be_bytes());
        context.update(&u64::try_from(evidence.len()).unwrap().to_be_bytes());
        context.update(evidence);
    }
    Ok((entry_count, max_sequence, context.finalize()))
}

fn validate_staging_evidence_actor_open(
    durable: &MetadataTransferStagingNodeIdentity,
    requested: &MetadataTransferStagingNodeIdentity,
) -> Result<(), MetadataTransferStagingError> {
    if durable == requested {
        return Ok(());
    }
    if durable.node_id != requested.node_id {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence actor cannot move between node identities".to_owned(),
        ));
    }
    if requested.node_incarnation <= durable.node_incarnation {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence actor incarnation did not advance".to_owned(),
        ));
    }
    Ok(())
}

fn insert_staging_evidence_actor(
    connection: &Connection,
    identity: &MetadataTransferStagingNodeIdentity,
) -> Result<(), MetadataTransferStagingError> {
    connection
        .execute(
            "INSERT INTO staging_evidence_actor (singleton, node_id, node_incarnation, endpoint) \
             VALUES (1, ?1, ?2, ?3)",
            params![
                i64::from(identity.node_id.as_u32()),
                to_sql_u64(identity.node_incarnation)?,
                &identity.endpoint,
            ],
        )
        .map_err(|source| {
            MetadataTransferStagingError::sql("initialize staging evidence actor", source)
        })?;
    Ok(())
}

fn update_staging_evidence_actor(
    connection: &Connection,
    previous: &MetadataTransferStagingNodeIdentity,
    next: &MetadataTransferStagingNodeIdentity,
) -> Result<(), MetadataTransferStagingError> {
    let changed = connection
        .execute(
            "UPDATE staging_evidence_actor SET node_id = ?1, node_incarnation = ?2, endpoint = ?3 \
             WHERE singleton = 1 AND node_id = ?4 AND node_incarnation = ?5 AND endpoint = ?6",
            params![
                i64::from(next.node_id.as_u32()),
                to_sql_u64(next.node_incarnation)?,
                &next.endpoint,
                i64::from(previous.node_id.as_u32()),
                to_sql_u64(previous.node_incarnation)?,
                &previous.endpoint,
            ],
        )
        .map_err(|source| {
            MetadataTransferStagingError::sql("update staging evidence actor", source)
        })?;
    if changed != 1 {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence actor changed during rollover".to_owned(),
        ));
    }
    Ok(())
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
                .execute_batch(STAGING_STORE_SCHEMA_V4)
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
        .execute_batch(STAGING_STORE_SCHEMA_V4)
        .map_err(|source| {
            MetadataTransferStagingError::sql("build expected staging catalogue", source)
        })?;
    if schema_catalogue(connection)? != schema_catalogue(&expected)? {
        return Err(MetadataTransferStagingError::Invariant(
            "staging catalogue schema does not match format v4".to_owned(),
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

fn validate_committed_staging_authorization(
    actor: &MetadataTransferStagingNodeIdentity,
    authorization: &CommittedUnavailablePgStagingAuthorization,
    intent: &MetadataTransferStagingIntent,
) -> Result<(), MetadataTransferStagingError> {
    if authorization.destination_node_id() != actor.node_id()
        || authorization.pg_id() != intent.pg_id
    {
        return Err(MetadataTransferStagingError::IntentConflict(
            "committed authorization is not bound to this staging actor and PG".to_owned(),
        ));
    }
    let Some(committed) = authorization.authorization_for_pg(intent.pg_id) else {
        return Err(MetadataTransferStagingError::IntentConflict(
            "committed authorization does not contain the staging intent PG".to_owned(),
        ));
    };
    let binding = UnavailablePgTransitionMutationBinding::new(
        intent.pg_id,
        intent.transition_epoch,
        intent.source_epoch,
        intent.source_acting_set.clone(),
        intent.destination_acting_set.clone(),
    );
    if committed.unavailable_transition != binding
        || committed.staging_generation != intent.staging_generation
        || committed.artifact_digest != intent.artifact_digest
        || committed.artifact_length != intent.artifact_length
        || committed.artifact_format_version != intent.artifact_format_version
        || authorization.committed_epoch() < intent.transition_epoch
    {
        return Err(MetadataTransferStagingError::IntentConflict(
            "committed authorization does not match the exact staging intent".to_owned(),
        ));
    }
    Ok(())
}

fn validate_staging_intent_shape(
    intent: &MetadataTransferStagingIntent,
) -> Result<(), MetadataTransferStagingError> {
    if intent.artifact_length == 0 {
        return Err(MetadataTransferStagingError::Invariant(
            "staging intent artifact length must be nonzero".to_owned(),
        ));
    }
    if intent.artifact_length > METADATA_TRANSFER_STAGED_ARTIFACT_MAX_BYTES {
        return Err(MetadataTransferStagingError::ArtifactTooLarge {
            length: intent.artifact_length,
            limit: METADATA_TRANSFER_STAGED_ARTIFACT_MAX_BYTES,
        });
    }
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
    target_epoch: Option<ClusterEpoch>,
    bytes: &[u8],
) -> Result<(), MetadataTransferStagingError> {
    let target_epoch = target_epoch.map(ClusterEpoch::get).unwrap_or_default();
    let changed = connection
        .execute(
            "INSERT INTO staging_evidence_deltas (\
                pg_id, staging_generation, evidence_kind, target_epoch, actor_node_id,\
                actor_node_incarnation, actor_endpoint, evidence_bytes\
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
             ON CONFLICT(pg_id, staging_generation, evidence_kind, target_epoch) DO UPDATE SET \
                evidence_bytes = excluded.evidence_bytes \
             WHERE staging_evidence_deltas.actor_node_id = excluded.actor_node_id \
               AND staging_evidence_deltas.actor_node_incarnation = excluded.actor_node_incarnation \
               AND staging_evidence_deltas.actor_endpoint = excluded.actor_endpoint \
               AND staging_evidence_deltas.evidence_bytes = excluded.evidence_bytes",
            params![
                i64::from(intent.pg_id.get()),
                to_sql_u64(intent.staging_generation)?,
                kind,
                to_sql_u64(target_epoch)?,
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
    target_epoch: Option<ClusterEpoch>,
) -> Result<Option<StagingEvidenceDelta>, MetadataTransferStagingError> {
    let target_epoch = target_epoch.map(ClusterEpoch::get).unwrap_or_default();
    connection
        .query_row(
            "SELECT actor_node_id, actor_node_incarnation, actor_endpoint, evidence_bytes \
             FROM staging_evidence_deltas \
             WHERE pg_id = ?1 AND staging_generation = ?2 AND evidence_kind = ?3 \
               AND target_epoch = ?4",
            params![
                i64::from(pg_id.get()),
                to_sql_u64(staging_generation)?,
                kind,
                to_sql_u64(target_epoch)?,
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

fn unacknowledged_evidence_exists(
    connection: &Connection,
) -> Result<bool, MetadataTransferStagingError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM staging_evidence_deltas WHERE acknowledged = 0)",
            [],
            |row| row.get(0),
        )
        .map_err(|source| {
            MetadataTransferStagingError::sql("check unacknowledged staging evidence", source)
        })
}

fn build_staging_evidence_page(
    connection: &Connection,
    actor: &MetadataTransferStagingNodeIdentity,
    actor_closure_candidate: Option<&MetadataTransferStagingActorClosureCandidate>,
    previous_generation: u64,
    previous_apply_receipt_digest: [u8; DIGEST_LEN],
) -> Result<Option<MetadataTransferStagingEvidencePage>, MetadataTransferStagingError> {
    if (previous_generation == 0) != (previous_apply_receipt_digest == [0; DIGEST_LEN]) {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence page has an invalid predecessor".to_owned(),
        ));
    }
    if actor_closure_candidate.is_some() && previous_generation != 0 {
        return Err(MetadataTransferStagingError::Invariant(
            "staging actor closure candidate must appear only on a genesis page".to_owned(),
        ));
    }
    if let Some(candidate) = actor_closure_candidate {
        validate_staging_evidence_actor_closure_candidate(candidate)?;
        if candidate.first_actor.node_id() != actor.node_id()
            || candidate.through_actor.node_id() != actor.node_id()
            || candidate.through_actor.node_incarnation() >= actor.node_incarnation()
        {
            return Err(MetadataTransferStagingError::Invariant(
                "staging actor closure candidate does not precede its genesis actor".to_owned(),
            ));
        }
    }
    let generation = previous_generation.checked_add(1).ok_or_else(|| {
        MetadataTransferStagingError::Invariant(
            "staging evidence page generation overflowed".to_owned(),
        )
    })?;
    let mut statement = connection
        .prepare(
            "SELECT sequence, evidence_bytes FROM staging_evidence_deltas \
             WHERE acknowledged = 0 ORDER BY sequence \
             LIMIT ?1",
        )
        .map_err(|source| {
            MetadataTransferStagingError::sql("prepare staging evidence page", source)
        })?;
    let candidates = statement
        .query_map(
            [i64::try_from(MAX_STAGING_EVIDENCE_PAGE_ENTRIES).unwrap()],
            |row| Ok((row.get::<_, u64>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .map_err(|source| MetadataTransferStagingError::sql("query staging evidence page", source))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| {
            MetadataTransferStagingError::sql("read staging evidence page", source)
        })?;
    if candidates.is_empty() {
        return Ok(None);
    }

    let mut entries = Vec::with_capacity(candidates.len());
    for (sequence, evidence) in candidates {
        if evidence.is_empty() || evidence.len() > MAX_STAGING_EVIDENCE_BYTES {
            return Err(MetadataTransferStagingError::Invariant(
                "staging evidence page member has an invalid length".to_owned(),
            ));
        }
        let candidate = MetadataTransferStagingEvidencePageEntry { sequence, evidence };
        entries.push(candidate);
        let payload = encode_staging_evidence_page_payload(
            actor,
            actor_closure_candidate,
            previous_generation,
            previous_apply_receipt_digest,
            generation,
            &entries,
        );
        if payload.len() > MAX_STAGING_EVIDENCE_PAGE_BYTES {
            entries.pop();
            break;
        }
    }
    if entries.is_empty() {
        return Err(MetadataTransferStagingError::Capacity(
            "one staging evidence member does not fit the page byte limit".to_owned(),
        ));
    }
    let operation_payload = encode_staging_evidence_page_payload(
        actor,
        actor_closure_candidate,
        previous_generation,
        previous_apply_receipt_digest,
        generation,
        &entries,
    );
    let page_digest = checksum::sha256::digest(&operation_payload);
    Ok(Some(MetadataTransferStagingEvidencePage {
        actor: actor.clone(),
        actor_closure_candidate: actor_closure_candidate.cloned(),
        previous_generation,
        previous_apply_receipt_digest,
        generation,
        entries,
        operation_payload,
        page_digest,
    }))
}

fn persist_inflight_evidence_page(
    connection: &Connection,
    page: &MetadataTransferStagingEvidencePage,
) -> Result<(), MetadataTransferStagingError> {
    connection
        .execute(
            "INSERT INTO staging_evidence_inflight_page (\
                singleton, previous_generation, previous_apply_receipt_digest, generation, \
                operation_payload, page_digest, apply_receipt\
             ) VALUES (1, ?1, ?2, ?3, ?4, ?5, NULL) \
             ON CONFLICT(singleton) DO UPDATE SET \
                previous_generation = excluded.previous_generation, \
                previous_apply_receipt_digest = excluded.previous_apply_receipt_digest, \
                generation = excluded.generation, \
                operation_payload = excluded.operation_payload, \
                page_digest = excluded.page_digest, \
                apply_receipt = NULL",
            params![
                to_sql_u64(page.previous_generation)?,
                &page.previous_apply_receipt_digest[..],
                to_sql_u64(page.generation)?,
                &page.operation_payload,
                &page.page_digest[..],
            ],
        )
        .map_err(|source| {
            MetadataTransferStagingError::sql("persist staging evidence page", source)
        })?;
    Ok(())
}

fn load_inflight_evidence_page(
    connection: &Connection,
) -> Result<Option<StagingInflightEvidencePage>, MetadataTransferStagingError> {
    let row = connection
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
            MetadataTransferStagingError::sql("load staging in-flight evidence page", source)
        })?;
    row.map(
        |(previous_generation, previous_digest, generation, payload, digest, receipt)| {
            let previous_apply_receipt_digest: [u8; DIGEST_LEN] =
                previous_digest.try_into().map_err(|_| {
                    MetadataTransferStagingError::Invariant(
                        "staging in-flight predecessor digest has invalid length".to_owned(),
                    )
                })?;
            let page_digest: [u8; DIGEST_LEN] = digest.try_into().map_err(|_| {
                MetadataTransferStagingError::Invariant(
                    "staging in-flight page digest has invalid length".to_owned(),
                )
            })?;
            let page = decode_staging_evidence_page_payload(&payload, page_digest)?;
            if page.previous_generation != previous_generation
                || page.previous_apply_receipt_digest != previous_apply_receipt_digest
                || page.generation != generation
            {
                return Err(MetadataTransferStagingError::Invariant(
                    "staging in-flight row differs from its canonical operation payload".to_owned(),
                ));
            }
            Ok(StagingInflightEvidencePage {
                page,
                apply_receipt: receipt,
            })
        },
    )
    .transpose()
}

fn require_exact_evidence_page(
    current: &MetadataTransferStagingEvidencePage,
    supplied: &MetadataTransferStagingEvidencePage,
) -> Result<(), MetadataTransferStagingError> {
    if current != supplied {
        return Err(MetadataTransferStagingError::IntentConflict(
            "staging evidence operation does not match the durable in-flight page".to_owned(),
        ));
    }
    Ok(())
}

fn require_apply_receipt_for_page(
    receipt: &MetadataTransferStagingEvidenceApplyReceipt,
    page: &MetadataTransferStagingEvidencePage,
) -> Result<(), MetadataTransferStagingError> {
    if receipt.actor != page.actor
        || receipt.previous_generation != page.previous_generation
        || receipt.previous_apply_receipt_digest != page.previous_apply_receipt_digest
        || receipt.generation != page.generation
        || receipt.page_digest != page.page_digest
        || receipt.accepted_generation != page.generation
        || receipt.bytes != encode_staging_evidence_apply_receipt(receipt)
    {
        return Err(MetadataTransferStagingError::IntentConflict(
            "staging evidence apply receipt does not bind the exact in-flight page".to_owned(),
        ));
    }
    Ok(())
}

fn load_acknowledgement_edge(
    connection: &Connection,
    acknowledged: bool,
    count: usize,
) -> Result<Vec<MetadataTransferStagingEvidencePageEntry>, MetadataTransferStagingError> {
    let order = if acknowledged { "DESC" } else { "ASC" };
    let mut statement = connection
        .prepare(&format!(
            "SELECT sequence, evidence_bytes FROM staging_evidence_deltas \
             WHERE acknowledged = ?1 ORDER BY sequence {order} LIMIT ?2"
        ))
        .map_err(|source| {
            MetadataTransferStagingError::sql(
                "prepare staging evidence acknowledgement edge",
                source,
            )
        })?;
    let mut entries = statement
        .query_map(
            params![i64::from(acknowledged), i64::try_from(count).unwrap()],
            |row| {
                Ok(MetadataTransferStagingEvidencePageEntry {
                    sequence: row.get(0)?,
                    evidence: row.get(1)?,
                })
            },
        )
        .map_err(|source| {
            MetadataTransferStagingError::sql("query staging evidence acknowledgement edge", source)
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| {
            MetadataTransferStagingError::sql("read staging evidence acknowledgement edge", source)
        })?;
    if acknowledged {
        entries.reverse();
    }
    Ok(entries)
}

fn validate_auxiliary_catalogue(
    connection: &Connection,
    rows: &[StagingRow],
    max_entries: usize,
) -> Result<(), MetadataTransferStagingError> {
    let durable_actor = load_staging_evidence_actor(connection)?.ok_or_else(|| {
        MetadataTransferStagingError::Invariant(
            "staging catalogue has no durable evidence actor".to_owned(),
        )
    })?;
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

    let evidence_limit = max_entries.saturating_mul(MAX_STAGING_EPOCH_PROOFS_PER_INTENT + 1);
    let mut statement = connection
        .prepare(
            "SELECT pg_id, staging_generation, evidence_kind, target_epoch, actor_node_id, \
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
                    row.get::<_, u64>(3)?,
                    row.get::<_, u32>(4)?,
                    row.get::<_, u64>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Vec<u8>>(7)?,
                    row.get::<_, i64>(8)?,
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
        target_epoch,
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
        if actor != durable_actor {
            return Err(MetadataTransferStagingError::Invariant(
                "staging evidence delta does not match the durable actor".to_owned(),
            ));
        }
        validate_staging_evidence(&bytes, &row.intent, expected_kind, &actor)?;
        let decoded = decode_staging_evidence(&bytes)?;
        if decoded
            .target_epoch()
            .map(ClusterEpoch::get)
            .unwrap_or_default()
            != target_epoch
        {
            return Err(MetadataTransferStagingError::Invariant(
                "staging evidence target epoch does not match its catalogue identity".to_owned(),
            ));
        }
        match kind {
            0 if matches!(
                row.state,
                StagingState::Published | StagingState::Imported | StagingState::Tombstoned
            ) => {}
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
            let decoded = decode_staging_evidence(publication_receipt)?;
            let target_epoch = decoded.target_epoch().ok_or_else(|| {
                MetadataTransferStagingError::Invariant(
                    "staging publication receipt has no target epoch".to_owned(),
                )
            })?;
            let delta = load_evidence_delta(
                connection,
                row.intent.pg_id,
                row.intent.staging_generation,
                0,
                Some(target_epoch),
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
        match row.state {
            StagingState::Intent => {}
            StagingState::Published | StagingState::Imported => {
                if row.publication_receipt.is_none() {
                    return Err(MetadataTransferStagingError::Invariant(
                        "published staging intent is missing required durable evidence".to_owned(),
                    ));
                }
            }
            StagingState::Tombstoned => {
                let receipt = load_evidence_delta(
                    connection,
                    row.intent.pg_id,
                    row.intent.staging_generation,
                    1,
                    None,
                )?
                .ok_or_else(|| {
                    MetadataTransferStagingError::Invariant(
                        "tombstoned staging intent is missing required durable evidence".to_owned(),
                    )
                })?;
                validate_staging_evidence(&receipt.bytes, &row.intent, 1, &receipt.actor)?;
            }
        }
    }

    if let Some(candidate) = load_staging_evidence_actor_closure_candidate(connection)? {
        if candidate.first_actor.node_id() != durable_actor.node_id()
            || candidate.through_actor.node_id() != durable_actor.node_id()
            || candidate.through_actor.node_incarnation() >= durable_actor.node_incarnation()
        {
            return Err(MetadataTransferStagingError::Invariant(
                "staging actor closure does not precede the durable actor".to_owned(),
            ));
        }
        let (entry_count, max_sequence, evidence_digest) =
            rebound_staging_evidence_digest(connection, Some(candidate.rebound_max_sequence))?;
        if entry_count != candidate.rebound_entry_count
            || max_sequence != candidate.rebound_max_sequence
            || evidence_digest != candidate.rebound_evidence_digest
        {
            return Err(MetadataTransferStagingError::Invariant(
                "staging actor closure does not bind the exact rebound evidence prefix".to_owned(),
            ));
        }
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

    let (acknowledged_count, maximum_acknowledged, minimum_unacknowledged): (
        i64,
        Option<u64>,
        Option<u64>,
    ) = connection
        .query_row(
            "SELECT COALESCE(SUM(acknowledged), 0), \
                    MAX(CASE WHEN acknowledged = 1 THEN sequence END), \
                    MIN(CASE WHEN acknowledged = 0 THEN sequence END) \
             FROM staging_evidence_deltas",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(|source| {
            MetadataTransferStagingError::sql(
                "validate staging evidence acknowledgement prefix",
                source,
            )
        })?;
    if maximum_acknowledged
        .zip(minimum_unacknowledged)
        .is_some_and(|(acknowledged, unacknowledged)| acknowledged >= unacknowledged)
    {
        return Err(MetadataTransferStagingError::Invariant(
            "acknowledged staging evidence is not an ordered catalogue prefix".to_owned(),
        ));
    }

    if let Some(inflight) = load_inflight_evidence_page(connection)? {
        let page = inflight.page;
        if page.actor != durable_actor {
            return Err(MetadataTransferStagingError::Invariant(
                "staging in-flight page does not match the durable actor".to_owned(),
            ));
        }
        let apply_receipt = inflight.apply_receipt;
        let has_apply_receipt = apply_receipt.is_some();
        let expected_acknowledged = i64::from(has_apply_receipt);
        if let Some(apply_receipt) = apply_receipt.as_deref() {
            let receipt = decode_staging_evidence_apply_receipt(apply_receipt)?;
            require_apply_receipt_for_page(&receipt, &page).map_err(|_| {
                MetadataTransferStagingError::Invariant(
                    "staging in-flight apply receipt does not bind its durable page".to_owned(),
                )
            })?;
        }
        let current_page_acknowledged_count = if has_apply_receipt {
            i64::try_from(page.entries.len()).unwrap()
        } else {
            0
        };
        let predecessor_acknowledged_count = acknowledged_count
            .checked_sub(current_page_acknowledged_count)
            .filter(|count| *count >= 0)
            .ok_or_else(|| {
                MetadataTransferStagingError::Invariant(
                    "staging evidence acknowledgement count is smaller than its retained page"
                        .to_owned(),
                )
            })?;
        if (page.previous_generation == 0) != (predecessor_acknowledged_count == 0) {
            return Err(MetadataTransferStagingError::Invariant(
                "staging evidence predecessor does not match acknowledged history".to_owned(),
            ));
        }
        let edge = load_acknowledgement_edge(connection, has_apply_receipt, page.entries.len())?;
        if edge != page.entries {
            return Err(MetadataTransferStagingError::Invariant(
                "staging in-flight page is not the exact acknowledgement frontier".to_owned(),
            ));
        }
        for entry in &page.entries {
            let member = connection
                .query_row(
                    "SELECT evidence_bytes, acknowledged FROM staging_evidence_deltas \
                     WHERE sequence = ?1",
                    [to_sql_u64(entry.sequence)?],
                    |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?)),
                )
                .optional()
                .map_err(|source| {
                    MetadataTransferStagingError::sql(
                        "validate staging in-flight evidence member",
                        source,
                    )
                })?;
            if member.as_ref().is_none_or(|(evidence, acknowledged)| {
                evidence != &entry.evidence || *acknowledged != expected_acknowledged
            }) {
                return Err(MetadataTransferStagingError::Invariant(
                    "staging in-flight page does not match its durable evidence members".to_owned(),
                ));
            }
        }
    } else if acknowledged_count != 0 {
        return Err(MetadataTransferStagingError::Invariant(
            "acknowledged staging evidence has no durable page and apply receipt".to_owned(),
        ));
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
    target_epoch: Option<ClusterEpoch>,
    transfer: Option<PgMetadataTransferProof>,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(STAGING_EVIDENCE_MAGIC);
    out.push(kind);
    out.extend_from_slice(&identity.node_id.as_u32().to_be_bytes());
    out.extend_from_slice(&identity.node_incarnation.to_be_bytes());
    put_bytes(&mut out, identity.endpoint.as_bytes());
    encode_staging_intent_evidence(&mut out, intent);
    out.extend_from_slice(
        &target_epoch
            .map(ClusterEpoch::get)
            .unwrap_or_default()
            .to_be_bytes(),
    );
    match transfer {
        None => out.push(0),
        Some(transfer) => {
            out.push(1);
            out.extend_from_slice(&transfer.source_epoch().get().to_be_bytes());
            encode_metadata_proof(&mut out, transfer.source_metadata_proof());
            encode_metadata_proof(&mut out, transfer.metadata_proof());
        }
    }
    out.push(0b0000_0111); // artifact, catalogue, and parent-directory fsync scope
    out
}

pub(crate) fn canonical_metadata_transfer_staging_evidence(
    actor: &MetadataTransferStagingNodeIdentity,
    intent: &MetadataTransferStagingIntent,
    kind: MetadataTransferStagingEvidenceKind,
    target_epoch: Option<ClusterEpoch>,
    transfer: Option<PgMetadataTransferProof>,
) -> Result<Vec<u8>, MetadataTransferStagingError> {
    let bytes = encode_staging_evidence(actor, intent, kind as u8, target_epoch, transfer);
    decode_staging_evidence(&bytes)?;
    Ok(bytes)
}

pub(crate) fn metadata_transfer_staging_checkpoint_page_digest(
    actor: &MetadataTransferStagingNodeIdentity,
    previous_generation: u64,
    previous_apply_receipt_digest: [u8; DIGEST_LEN],
    generation: u64,
    entries: &[(u64, Vec<u8>)],
) -> Result<[u8; DIGEST_LEN], MetadataTransferStagingError> {
    let entries = entries
        .iter()
        .map(
            |(sequence, evidence)| MetadataTransferStagingEvidencePageEntry {
                sequence: *sequence,
                evidence: evidence.clone(),
            },
        )
        .collect::<Vec<_>>();
    let payload = encode_staging_evidence_page_payload(
        actor,
        None,
        previous_generation,
        previous_apply_receipt_digest,
        generation,
        &entries,
    );
    let digest = checksum::sha256::digest(&payload);
    decode_staging_evidence_page_payload(&payload, digest)?;
    Ok(digest)
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

pub(crate) fn encode_staging_intent(
    intent: &MetadataTransferStagingIntent,
) -> Result<Vec<u8>, MetadataTransferStagingError> {
    validate_staging_intent_shape(intent)?;
    let mut out = Vec::new();
    encode_staging_intent_evidence(&mut out, intent);
    if out.len() > MAX_STAGING_INTENT_BYTES {
        return Err(MetadataTransferStagingError::Invariant(
            "staging intent exceeds its encoded byte limit".to_owned(),
        ));
    }
    Ok(out)
}

pub(crate) fn decode_staging_intent(
    bytes: &[u8],
) -> Result<MetadataTransferStagingIntent, MetadataTransferStagingError> {
    if bytes.is_empty() || bytes.len() > MAX_STAGING_INTENT_BYTES {
        return Err(MetadataTransferStagingError::Invariant(
            "staging intent has invalid encoded length".to_owned(),
        ));
    }
    let mut offset = 0;
    let intent = decode_staging_intent_fields(bytes, &mut offset)?;
    if offset != bytes.len() || encode_staging_intent(&intent)? != bytes {
        return Err(MetadataTransferStagingError::Invariant(
            "staging intent is not canonically encoded".to_owned(),
        ));
    }
    Ok(intent)
}

fn decode_staging_intent_fields(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<MetadataTransferStagingIntent, MetadataTransferStagingError> {
    let pg_id = PgId::new(u32::from_be_bytes(
        take(bytes, offset, 4)?.try_into().unwrap(),
    ));
    let transition_epoch = ClusterEpoch::new(u64::from_be_bytes(
        take(bytes, offset, 8)?.try_into().unwrap(),
    ))
    .ok_or_else(|| {
        MetadataTransferStagingError::Invariant(
            "staging intent transition epoch must be nonzero".to_owned(),
        )
    })?;
    let source_epoch = ClusterEpoch::new(u64::from_be_bytes(
        take(bytes, offset, 8)?.try_into().unwrap(),
    ))
    .ok_or_else(|| {
        MetadataTransferStagingError::Invariant(
            "staging intent source epoch must be nonzero".to_owned(),
        )
    })?;
    let source_acting_set_len = usize::try_from(u32::from_be_bytes(
        take(bytes, offset, 4)?.try_into().unwrap(),
    ))
    .unwrap();
    let source_acting_set = decode_acting_set(take(bytes, offset, source_acting_set_len)?)?;
    let destination_acting_set_len = usize::try_from(u32::from_be_bytes(
        take(bytes, offset, 4)?.try_into().unwrap(),
    ))
    .unwrap();
    let destination_acting_set =
        decode_acting_set(take(bytes, offset, destination_acting_set_len)?)?;
    let staging_generation = u64::from_be_bytes(take(bytes, offset, 8)?.try_into().unwrap());
    let artifact_digest = take(bytes, offset, DIGEST_LEN)?.try_into().unwrap();
    let artifact_length = u64::from_be_bytes(take(bytes, offset, 8)?.try_into().unwrap());
    let artifact_format_version = u16::from_be_bytes(take(bytes, offset, 2)?.try_into().unwrap());
    let intent = MetadataTransferStagingIntent {
        pg_id,
        transition_epoch,
        source_epoch,
        source_acting_set,
        destination_acting_set,
        staging_generation,
        artifact_digest,
        artifact_length,
        artifact_format_version,
    };
    validate_staging_intent_shape(&intent)?;
    Ok(intent)
}

fn validate_staging_evidence(
    bytes: &[u8],
    intent: &MetadataTransferStagingIntent,
    expected_kind: u8,
    expected_actor: &MetadataTransferStagingNodeIdentity,
) -> Result<(), MetadataTransferStagingError> {
    let evidence = decode_staging_evidence(bytes)?;
    let expected_kind = match expected_kind {
        0 => MetadataTransferStagingEvidenceKind::Publication,
        1 => MetadataTransferStagingEvidenceKind::Tombstone,
        _ => {
            return Err(MetadataTransferStagingError::Invariant(
                "staging evidence has invalid expected kind".to_owned(),
            ))
        }
    };
    if evidence.kind != expected_kind {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence does not match its expected kind".to_owned(),
        ));
    }
    if &evidence.actor != expected_actor {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence does not match its durable actor identity".to_owned(),
        ));
    }
    if &evidence.intent != intent {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence does not bind the exact intent and fsync scope".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn decode_staging_evidence(
    bytes: &[u8],
) -> Result<MetadataTransferStagingEvidence, MetadataTransferStagingError> {
    if bytes.is_empty() || bytes.len() > MAX_STAGING_EVIDENCE_BYTES {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence has invalid length".to_owned(),
        ));
    }
    let mut offset = STAGING_EVIDENCE_MAGIC.len();
    if bytes.get(..offset) != Some(STAGING_EVIDENCE_MAGIC) {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence has invalid magic".to_owned(),
        ));
    }
    let kind = match *take(bytes, &mut offset, 1)?.first().unwrap() {
        0 => MetadataTransferStagingEvidenceKind::Publication,
        1 => MetadataTransferStagingEvidenceKind::Tombstone,
        _ => {
            return Err(MetadataTransferStagingError::Invariant(
                "staging evidence has invalid kind".to_owned(),
            ))
        }
    };
    let actor = decode_staging_evidence_actor(bytes, &mut offset)?;
    let intent = decode_staging_intent_fields(bytes, &mut offset)?;
    let target_epoch_raw = u64::from_be_bytes(take(bytes, &mut offset, 8)?.try_into().unwrap());
    let target_epoch = ClusterEpoch::new(target_epoch_raw);
    let transfer = match take(bytes, &mut offset, 1)?[0] {
        0 => None,
        1 => {
            let source_epoch = ClusterEpoch::new(u64::from_be_bytes(
                take(bytes, &mut offset, 8)?.try_into().unwrap(),
            ))
            .ok_or_else(|| {
                MetadataTransferStagingError::Invariant(
                    "staging evidence transfer source epoch is zero".to_owned(),
                )
            })?;
            Some(PgMetadataTransferProof::new_with_imported_metadata_proof(
                source_epoch,
                decode_metadata_proof(bytes, &mut offset)?,
                decode_metadata_proof(bytes, &mut offset)?,
            ))
        }
        _ => {
            return Err(MetadataTransferStagingError::Invariant(
                "staging evidence has invalid transfer-proof tag".to_owned(),
            ))
        }
    };
    let fsync_scope = *take(bytes, &mut offset, 1)?.first().unwrap();
    let evidence = MetadataTransferStagingEvidence {
        actor,
        intent,
        kind,
        target_epoch,
        transfer,
        fsync_scope,
        bytes: bytes.to_vec(),
    };
    if offset != bytes.len()
        || fsync_scope != 0b0000_0111
        || (kind == MetadataTransferStagingEvidenceKind::Publication) != transfer.is_some()
        || (kind == MetadataTransferStagingEvidenceKind::Publication) != target_epoch.is_some()
        || encode_staging_evidence(
            &evidence.actor,
            &evidence.intent,
            evidence.kind as u8,
            evidence.target_epoch,
            evidence.transfer,
        ) != bytes
    {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence is not canonical or fsync-complete".to_owned(),
        ));
    }
    Ok(evidence)
}

fn encode_staging_evidence_page_payload(
    actor: &MetadataTransferStagingNodeIdentity,
    actor_closure_candidate: Option<&MetadataTransferStagingActorClosureCandidate>,
    previous_generation: u64,
    previous_apply_receipt_digest: [u8; DIGEST_LEN],
    generation: u64,
    entries: &[MetadataTransferStagingEvidencePageEntry],
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(STAGING_EVIDENCE_PAGE_MAGIC);
    encode_staging_evidence_actor(&mut out, actor);
    if let Some(candidate) = actor_closure_candidate {
        out.push(1);
        encode_staging_evidence_actor_closure_candidate(&mut out, candidate);
    } else {
        out.push(0);
    }
    out.extend_from_slice(&previous_generation.to_be_bytes());
    out.extend_from_slice(&previous_apply_receipt_digest);
    out.extend_from_slice(&generation.to_be_bytes());
    out.extend_from_slice(&u32::try_from(entries.len()).unwrap().to_be_bytes());
    for entry in entries {
        out.extend_from_slice(&entry.sequence.to_be_bytes());
        put_bytes(&mut out, &entry.evidence);
    }
    out
}

pub(crate) fn decode_staging_evidence_page_payload(
    bytes: &[u8],
    expected_digest: [u8; DIGEST_LEN],
) -> Result<MetadataTransferStagingEvidencePage, MetadataTransferStagingError> {
    if bytes.is_empty()
        || bytes.len() > MAX_STAGING_EVIDENCE_PAGE_BYTES
        || checksum::sha256::digest(bytes) != expected_digest
    {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence page has invalid length or digest".to_owned(),
        ));
    }
    let mut offset = STAGING_EVIDENCE_PAGE_MAGIC.len();
    if bytes.get(..offset) != Some(STAGING_EVIDENCE_PAGE_MAGIC) {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence page has unknown magic".to_owned(),
        ));
    }
    let actor = decode_staging_evidence_actor(bytes, &mut offset)?;
    let actor_closure_candidate = match take(bytes, &mut offset, 1)?[0] {
        0 => None,
        1 => Some(decode_staging_evidence_actor_closure_candidate(
            bytes,
            &mut offset,
        )?),
        _ => {
            return Err(MetadataTransferStagingError::Invariant(
                "staging evidence page has an invalid actor closure tag".to_owned(),
            ))
        }
    };
    let previous_generation = u64::from_be_bytes(take(bytes, &mut offset, 8)?.try_into().unwrap());
    let previous_apply_receipt_digest = take(bytes, &mut offset, DIGEST_LEN)?.try_into().unwrap();
    let generation = u64::from_be_bytes(take(bytes, &mut offset, 8)?.try_into().unwrap());
    let count = usize::try_from(u32::from_be_bytes(
        take(bytes, &mut offset, 4)?.try_into().unwrap(),
    ))
    .unwrap();
    if count == 0
        || count > MAX_STAGING_EVIDENCE_PAGE_ENTRIES
        || generation != previous_generation.checked_add(1).unwrap_or(0)
        || (previous_generation == 0) != (previous_apply_receipt_digest == [0; DIGEST_LEN])
        || (actor_closure_candidate.is_some() && previous_generation != 0)
        || actor_closure_candidate.as_ref().is_some_and(|candidate| {
            candidate.first_actor.node_id() != actor.node_id()
                || candidate.through_actor.node_id() != actor.node_id()
                || candidate.through_actor.node_incarnation() >= actor.node_incarnation()
        })
    {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence page has invalid count, generation, or predecessor".to_owned(),
        ));
    }
    let mut entries = Vec::with_capacity(count);
    let mut last_sequence = None;
    for _ in 0..count {
        let sequence = u64::from_be_bytes(take(bytes, &mut offset, 8)?.try_into().unwrap());
        let evidence_len = usize::try_from(u32::from_be_bytes(
            take(bytes, &mut offset, 4)?.try_into().unwrap(),
        ))
        .unwrap();
        if sequence == 0
            || last_sequence.is_some_and(|last| sequence <= last)
            || evidence_len == 0
            || evidence_len > MAX_STAGING_EVIDENCE_BYTES
        {
            return Err(MetadataTransferStagingError::Invariant(
                "staging evidence page has a noncanonical member".to_owned(),
            ));
        }
        let evidence = take(bytes, &mut offset, evidence_len)?.to_vec();
        entries.push(MetadataTransferStagingEvidencePageEntry { sequence, evidence });
        last_sequence = Some(sequence);
    }
    if offset != bytes.len() {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence page has trailing bytes".to_owned(),
        ));
    }
    let canonical = encode_staging_evidence_page_payload(
        &actor,
        actor_closure_candidate.as_ref(),
        previous_generation,
        previous_apply_receipt_digest,
        generation,
        &entries,
    );
    if canonical != bytes {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence page is not canonical".to_owned(),
        ));
    }
    Ok(MetadataTransferStagingEvidencePage {
        actor,
        actor_closure_candidate,
        previous_generation,
        previous_apply_receipt_digest,
        generation,
        entries,
        operation_payload: bytes.to_vec(),
        page_digest: expected_digest,
    })
}

fn encode_staging_evidence_apply_receipt(
    receipt: &MetadataTransferStagingEvidenceApplyReceipt,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(STAGING_EVIDENCE_APPLY_RECEIPT_MAGIC);
    encode_staging_evidence_actor(&mut out, &receipt.actor);
    out.extend_from_slice(&receipt.previous_generation.to_be_bytes());
    out.extend_from_slice(&receipt.previous_apply_receipt_digest);
    out.extend_from_slice(&receipt.generation.to_be_bytes());
    out.extend_from_slice(&receipt.page_digest);
    out.extend_from_slice(&receipt.accepted_generation.to_be_bytes());
    out
}

pub(crate) fn decode_staging_evidence_apply_receipt(
    bytes: &[u8],
) -> Result<MetadataTransferStagingEvidenceApplyReceipt, MetadataTransferStagingError> {
    if bytes.is_empty() || bytes.len() > MAX_STAGING_EVIDENCE_BYTES {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence apply receipt has invalid length".to_owned(),
        ));
    }
    let mut offset = STAGING_EVIDENCE_APPLY_RECEIPT_MAGIC.len();
    if bytes.get(..offset) != Some(STAGING_EVIDENCE_APPLY_RECEIPT_MAGIC) {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence apply receipt has unknown magic".to_owned(),
        ));
    }
    let actor = decode_staging_evidence_actor(bytes, &mut offset)?;
    let previous_generation = u64::from_be_bytes(take(bytes, &mut offset, 8)?.try_into().unwrap());
    let previous_apply_receipt_digest = take(bytes, &mut offset, DIGEST_LEN)?.try_into().unwrap();
    let generation = u64::from_be_bytes(take(bytes, &mut offset, 8)?.try_into().unwrap());
    let page_digest = take(bytes, &mut offset, DIGEST_LEN)?.try_into().unwrap();
    let accepted_generation = u64::from_be_bytes(take(bytes, &mut offset, 8)?.try_into().unwrap());
    let receipt = MetadataTransferStagingEvidenceApplyReceipt {
        actor,
        previous_generation,
        previous_apply_receipt_digest,
        generation,
        page_digest,
        accepted_generation,
        bytes: bytes.to_vec(),
    };
    if offset != bytes.len()
        || generation == 0
        || generation != previous_generation.checked_add(1).unwrap_or(0)
        || accepted_generation != generation
        || (previous_generation == 0) != (previous_apply_receipt_digest == [0; DIGEST_LEN])
        || encode_staging_evidence_apply_receipt(&receipt) != bytes
    {
        return Err(MetadataTransferStagingError::Invariant(
            "staging evidence apply receipt is not canonical".to_owned(),
        ));
    }
    Ok(receipt)
}

#[cfg(test)]
pub(crate) fn metadata_transfer_staging_evidence_page_for_test(
    actor: MetadataTransferStagingNodeIdentity,
    binding: &UnavailablePgTransitionMutationBinding,
    artifact_digest: [u8; DIGEST_LEN],
    artifact_length: u64,
    artifact_format_version: u16,
    kind: MetadataTransferStagingEvidenceKind,
    previous_receipt: Option<&MetadataTransferStagingEvidenceApplyReceipt>,
) -> MetadataTransferStagingEvidencePage {
    let intent = MetadataTransferStagingIntent::for_unavailable_transition(
        binding,
        artifact_digest,
        artifact_length,
        artifact_format_version,
    )
    .unwrap();
    metadata_transfer_staging_evidence_page_with_member_actor_for_test(
        actor.clone(),
        actor,
        &intent,
        kind,
        previous_receipt,
    )
}

#[cfg(test)]
pub(crate) fn metadata_transfer_staging_closure_evidence_page_for_test(
    first_actor: MetadataTransferStagingNodeIdentity,
    destination_actor: MetadataTransferStagingNodeIdentity,
    binding: &UnavailablePgTransitionMutationBinding,
    artifact_digest: [u8; DIGEST_LEN],
    artifact_length: u64,
    artifact_format_version: u16,
    kind: MetadataTransferStagingEvidenceKind,
) -> MetadataTransferStagingEvidencePage {
    let old_page = metadata_transfer_staging_evidence_page_for_test(
        first_actor.clone(),
        binding,
        artifact_digest,
        artifact_length,
        artifact_format_version,
        kind,
        None,
    );
    let rebound_entries = old_page
        .entries()
        .iter()
        .map(|entry| MetadataTransferStagingEvidencePageEntry {
            sequence: entry.sequence(),
            evidence: decode_staging_evidence(entry.evidence())
                .unwrap()
                .rebound_for_actor(&destination_actor),
        })
        .collect::<Vec<_>>();
    let (rebound_entry_count, rebound_max_sequence, rebound_evidence_digest) =
        metadata_transfer_staging_rebound_evidence_digest(
            rebound_entries
                .iter()
                .map(|entry| (entry.sequence(), entry.evidence())),
        )
        .unwrap();
    let candidate = MetadataTransferStagingActorClosureCandidate {
        first_actor: first_actor.clone(),
        first_accepted_generation: 0,
        first_accepted_apply_receipt_digest: [0; DIGEST_LEN],
        first_ambiguous_page: Some(MetadataTransferStagingActorClosureAmbiguousPage {
            generation: old_page.generation(),
            page_digest: old_page.page_digest(),
            apply_receipt_digest: checksum::sha256::digest(
                MetadataTransferStagingEvidenceApplyReceipt::for_page(&old_page).as_bytes(),
            ),
        }),
        through_actor: first_actor,
        rebound_entry_count,
        rebound_max_sequence,
        rebound_evidence_digest,
    };
    let operation_payload = encode_staging_evidence_page_payload(
        &destination_actor,
        Some(&candidate),
        0,
        [0; DIGEST_LEN],
        1,
        &rebound_entries,
    );
    let page_digest = checksum::sha256::digest(&operation_payload);
    decode_staging_evidence_page_payload(&operation_payload, page_digest).unwrap()
}

#[cfg(test)]
pub(crate) fn metadata_transfer_staging_incomplete_closure_evidence_page_for_test(
    first_tip: &MetadataTransferStagingEvidencePage,
    through_actor: MetadataTransferStagingNodeIdentity,
    destination_actor: MetadataTransferStagingNodeIdentity,
    through_entry: &MetadataTransferStagingEvidencePageEntry,
) -> MetadataTransferStagingEvidencePage {
    let through_evidence = decode_staging_evidence(through_entry.evidence()).unwrap();
    assert_eq!(through_evidence.actor(), &through_actor);
    let entries = vec![MetadataTransferStagingEvidencePageEntry {
        sequence: through_entry.sequence(),
        evidence: through_evidence.rebound_for_actor(&destination_actor),
    }];
    let candidate = MetadataTransferStagingActorClosureCandidate {
        first_actor: first_tip.actor().clone(),
        first_accepted_generation: first_tip.generation(),
        first_accepted_apply_receipt_digest: checksum::sha256::digest(
            MetadataTransferStagingEvidenceApplyReceipt::for_page(first_tip).as_bytes(),
        ),
        first_ambiguous_page: None,
        through_actor,
        rebound_entry_count: 2,
        rebound_max_sequence: through_entry.sequence().checked_add(1).unwrap(),
        rebound_evidence_digest: [0x5a; DIGEST_LEN],
    };
    let operation_payload = encode_staging_evidence_page_payload(
        &destination_actor,
        Some(&candidate),
        0,
        [0; DIGEST_LEN],
        1,
        &entries,
    );
    let page_digest = checksum::sha256::digest(&operation_payload);
    decode_staging_evidence_page_payload(&operation_payload, page_digest).unwrap()
}

#[cfg(test)]
pub(crate) fn rebind_metadata_transfer_staging_evidence_actor_for_test(
    bytes: &[u8],
    actor: &MetadataTransferStagingNodeIdentity,
) -> Vec<u8> {
    let evidence = decode_staging_evidence(bytes).unwrap();
    encode_staging_evidence(
        actor,
        evidence.intent(),
        evidence.kind() as u8,
        evidence.target_epoch(),
        evidence.transfer(),
    )
}

#[cfg(test)]
pub(crate) fn metadata_transfer_staging_evidence_page_with_member_actor_for_test(
    page_actor: MetadataTransferStagingNodeIdentity,
    evidence_actor: MetadataTransferStagingNodeIdentity,
    intent: &MetadataTransferStagingIntent,
    kind: MetadataTransferStagingEvidenceKind,
    previous_receipt: Option<&MetadataTransferStagingEvidenceApplyReceipt>,
) -> MetadataTransferStagingEvidencePage {
    metadata_transfer_staging_evidence_page_with_transfer_for_test(
        page_actor,
        evidence_actor,
        intent,
        kind,
        test_staging_evidence_transfer(intent, kind),
        previous_receipt,
    )
}

#[cfg(test)]
pub(crate) fn metadata_transfer_staging_publication_evidence_page_for_test(
    actor: MetadataTransferStagingNodeIdentity,
    intent: &MetadataTransferStagingIntent,
    transfer: PgMetadataTransferProof,
    previous_receipt: Option<&MetadataTransferStagingEvidenceApplyReceipt>,
) -> MetadataTransferStagingEvidencePage {
    metadata_transfer_staging_evidence_page_with_transfer_for_test(
        actor.clone(),
        actor,
        intent,
        MetadataTransferStagingEvidenceKind::Publication,
        Some(transfer),
        previous_receipt,
    )
}

#[cfg(test)]
pub(crate) fn metadata_transfer_staging_publication_evidence_page_at_epoch_for_test(
    actor: MetadataTransferStagingNodeIdentity,
    intent: &MetadataTransferStagingIntent,
    target_epoch: ClusterEpoch,
    transfer: PgMetadataTransferProof,
    previous_receipt: Option<&MetadataTransferStagingEvidenceApplyReceipt>,
) -> MetadataTransferStagingEvidencePage {
    metadata_transfer_staging_evidence_page_with_target_for_test(
        actor.clone(),
        actor,
        intent,
        MetadataTransferStagingEvidenceKind::Publication,
        Some(target_epoch),
        Some(transfer),
        previous_receipt,
    )
}

#[cfg(test)]
fn metadata_transfer_staging_evidence_page_with_transfer_for_test(
    page_actor: MetadataTransferStagingNodeIdentity,
    evidence_actor: MetadataTransferStagingNodeIdentity,
    intent: &MetadataTransferStagingIntent,
    kind: MetadataTransferStagingEvidenceKind,
    transfer: Option<PgMetadataTransferProof>,
    previous_receipt: Option<&MetadataTransferStagingEvidenceApplyReceipt>,
) -> MetadataTransferStagingEvidencePage {
    let target_epoch = transfer.map(|_| test_staging_evidence_target_epoch(intent));
    metadata_transfer_staging_evidence_page_with_target_for_test(
        page_actor,
        evidence_actor,
        intent,
        kind,
        target_epoch,
        transfer,
        previous_receipt,
    )
}

#[cfg(test)]
fn metadata_transfer_staging_evidence_page_with_target_for_test(
    page_actor: MetadataTransferStagingNodeIdentity,
    evidence_actor: MetadataTransferStagingNodeIdentity,
    intent: &MetadataTransferStagingIntent,
    kind: MetadataTransferStagingEvidenceKind,
    target_epoch: Option<ClusterEpoch>,
    transfer: Option<PgMetadataTransferProof>,
    previous_receipt: Option<&MetadataTransferStagingEvidenceApplyReceipt>,
) -> MetadataTransferStagingEvidencePage {
    let evidence =
        encode_staging_evidence(&evidence_actor, intent, kind as u8, target_epoch, transfer);
    let (previous_generation, previous_apply_receipt_digest) =
        previous_receipt.map_or((0, [0; DIGEST_LEN]), |receipt| {
            (
                receipt.generation(),
                checksum::sha256::digest(receipt.as_bytes()),
            )
        });
    let generation = previous_generation.checked_add(1).unwrap();
    let entries = vec![MetadataTransferStagingEvidencePageEntry {
        sequence: generation,
        evidence,
    }];
    let operation_payload = encode_staging_evidence_page_payload(
        &page_actor,
        None,
        previous_generation,
        previous_apply_receipt_digest,
        generation,
        &entries,
    );
    let page_digest = checksum::sha256::digest(&operation_payload);
    decode_staging_evidence_page_payload(&operation_payload, page_digest).unwrap()
}

#[cfg(test)]
fn test_staging_evidence_transfer(
    intent: &MetadataTransferStagingIntent,
    kind: MetadataTransferStagingEvidenceKind,
) -> Option<PgMetadataTransferProof> {
    (kind == MetadataTransferStagingEvidenceKind::Publication).then(|| {
        PgMetadataTransferProof::new_with_imported_metadata_proof(
            intent.source_epoch,
            PgMetadataProof::empty(),
            PgMetadataProof::empty(),
        )
    })
}

#[cfg(test)]
fn test_staging_evidence_target_epoch(intent: &MetadataTransferStagingIntent) -> ClusterEpoch {
    ClusterEpoch::new(intent.transition_epoch.get().checked_add(1).unwrap()).unwrap()
}

#[cfg(test)]
pub(crate) fn canonical_nonempty_staged_metadata_transfer_artifact_for_test(
    binding: &UnavailablePgTransitionMutationBinding,
    destination_epoch: ClusterEpoch,
) -> Vec<u8> {
    use crate::control_plane::{CanonicalStateDigest, MetadataCommandLogHash};
    use crate::metadata_command::{
        metadata_command_log_hash, CreateBucketCommand, MetadataCommandEnvelope, MetadataCommandId,
        MetadataCommandLogIndex, MetadataCommandLogRangeEntryKind, MetadataCommandPayload,
        MetadataCommandReplicaState,
    };

    let owner = crate::types::OwnerIdentity::from_principal("staging-artifact-owner");
    let bucket = crate::types::BucketName::try_from(format!(
        "staged-pg-{}-epoch-{}",
        binding.pg_id().get(),
        binding.transition_epoch().get()
    ))
    .unwrap();
    let config = crate::types::CreateBucketConfig {
        name: bucket.as_str(),
        owner_principal: &owner.principal,
        owner_canonical_id: &owner.canonical_id,
        acl_grants: &s3_types::AclGrants::default(),
        public_read: false,
        public_write: false,
        versioning: s3_types::BucketVersioningState::Disabled,
        object_lock: s3_types::BucketObjectLockConfig::default(),
        ownership_controls: crate::types::BucketOwnershipControls {
            object_ownership: crate::types::BucketObjectOwnership::ObjectWriter,
        },
    };
    let log_index = MetadataCommandLogIndex::new(1).unwrap();
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(binding.source_epoch(), binding.pg_id(), log_index),
        MetadataCommandPayload::CreateBucket(
            CreateBucketCommand::from_config_with_fixed_upload_id_key_for_test(&config, 1, 1)
                .unwrap(),
        ),
    );
    let previous_log_hash = MetadataCommandLogHash::genesis();
    let log_hash = metadata_command_log_hash(
        binding.source_epoch(),
        binding.pg_id(),
        log_index,
        previous_log_hash.value(),
        command.checksum_crc64(),
    );
    let pre_state_digest = CanonicalStateDigest::genesis();
    let post_state_digest = CanonicalStateDigest::for_test(0x5354_4147_4544);
    let entry = MetadataCommandLogRangeEntry {
        log_index: log_index.get(),
        previous_log_hash,
        log_hash,
        pre_state_digest: Some(pre_state_digest),
        post_state_digest: Some(post_state_digest),
        kind: MetadataCommandLogRangeEntryKind::Applied(Box::new(command)),
    };
    let artifact = crate::peering::build_pg_metadata_transfer_artifact_from_retained_log_entries(
        binding.source_epoch(),
        binding.pg_id(),
        binding.source_acting_set()[0],
        MetadataCommandReplicaState {
            cluster_epoch: binding.source_epoch(),
            applied_log_index: 1,
            applied_log_hash: log_hash,
            state_digest: post_state_digest,
        },
        false,
        vec![entry],
    )
    .unwrap();
    encode_staged_metadata_transfer_artifact(&artifact, destination_epoch).unwrap()
}

#[cfg(test)]
pub(crate) fn metadata_transfer_staging_evidence_page_with_duplicate_member_for_test(
    actor: MetadataTransferStagingNodeIdentity,
    intent: &MetadataTransferStagingIntent,
    kind: MetadataTransferStagingEvidenceKind,
    previous_receipt: Option<&MetadataTransferStagingEvidenceApplyReceipt>,
) -> MetadataTransferStagingEvidencePage {
    let evidence = encode_staging_evidence(
        &actor,
        intent,
        kind as u8,
        test_staging_evidence_transfer(intent, kind)
            .map(|_| test_staging_evidence_target_epoch(intent)),
        test_staging_evidence_transfer(intent, kind),
    );
    let (previous_generation, previous_apply_receipt_digest) =
        previous_receipt.map_or((0, [0; DIGEST_LEN]), |receipt| {
            (
                receipt.generation(),
                checksum::sha256::digest(receipt.as_bytes()),
            )
        });
    let generation = previous_generation.checked_add(1).unwrap();
    let entries = vec![
        MetadataTransferStagingEvidencePageEntry {
            sequence: generation,
            evidence: evidence.clone(),
        },
        MetadataTransferStagingEvidencePageEntry {
            sequence: generation.checked_add(1).unwrap(),
            evidence,
        },
    ];
    let operation_payload = encode_staging_evidence_page_payload(
        &actor,
        None,
        previous_generation,
        previous_apply_receipt_digest,
        generation,
        &entries,
    );
    let page_digest = checksum::sha256::digest(&operation_payload);
    decode_staging_evidence_page_payload(&operation_payload, page_digest).unwrap()
}

fn encode_staging_evidence_actor(out: &mut Vec<u8>, actor: &MetadataTransferStagingNodeIdentity) {
    out.extend_from_slice(&actor.node_id.as_u32().to_be_bytes());
    out.extend_from_slice(&actor.node_incarnation.to_be_bytes());
    put_bytes(out, actor.endpoint.as_bytes());
}

fn decode_staging_evidence_actor(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<MetadataTransferStagingNodeIdentity, MetadataTransferStagingError> {
    let node_id = NodeId::new(u32::from_be_bytes(
        take(bytes, offset, 4)?.try_into().unwrap(),
    ));
    let incarnation = u64::from_be_bytes(take(bytes, offset, 8)?.try_into().unwrap());
    let endpoint_len = usize::try_from(u32::from_be_bytes(
        take(bytes, offset, 4)?.try_into().unwrap(),
    ))
    .unwrap();
    let endpoint = std::str::from_utf8(take(bytes, offset, endpoint_len)?)
        .map_err(|_| {
            MetadataTransferStagingError::Invariant(
                "staging evidence actor endpoint is not UTF-8".to_owned(),
            )
        })?
        .to_owned();
    MetadataTransferStagingNodeIdentity::new(node_id, incarnation, endpoint)
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
    for chunk in bytes[4..].as_chunks::<4>().0 {
        let node_id = NodeId::new(u32::from_be_bytes(*chunk));
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
        STAGING_STORE_SCHEMA_V4.as_bytes(),
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
        STAGING_STORE_SCHEMA_V4.as_bytes(),
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
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;

    fn decode_hex(value: &str) -> Vec<u8> {
        assert_eq!(value.len() % 2, 0);
        value
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|digits| {
                let high = char::from(digits[0]).to_digit(16).unwrap();
                let low = char::from(digits[1]).to_digit(16).unwrap();
                u8::try_from((high << 4) | low).unwrap()
            })
            .collect()
    }

    struct AssignmentGateRelease(Option<mpsc::SyncSender<()>>);

    impl AssignmentGateRelease {
        fn release(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    impl Drop for AssignmentGateRelease {
        fn drop(&mut self) {
            self.release();
        }
    }

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
        binding_with_destination(NodeId::new(4))
    }

    fn binding_with_destination(destination: NodeId) -> UnavailablePgTransitionMutationBinding {
        binding_for_pg(PgId::new(19), destination)
    }

    fn binding_for_pg(pg_id: PgId, destination: NodeId) -> UnavailablePgTransitionMutationBinding {
        UnavailablePgTransitionMutationBinding::new(
            pg_id,
            ClusterEpoch::new(12).unwrap(),
            ClusterEpoch::new(11).unwrap(),
            vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
            vec![destination, NodeId::new(2), NodeId::new(3)],
        )
    }

    fn intent(bytes: &[u8]) -> MetadataTransferStagingIntent {
        intent_for_binding(bytes, &binding())
    }

    fn intent_for_binding(
        bytes: &[u8],
        binding: &UnavailablePgTransitionMutationBinding,
    ) -> MetadataTransferStagingIntent {
        MetadataTransferStagingIntent::for_unavailable_transition(
            binding,
            checksum::sha256::digest(bytes),
            u64::try_from(bytes.len()).unwrap(),
            METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
        )
        .unwrap()
    }

    fn canonical_artifact(label: &[u8]) -> &'static [u8] {
        canonical_artifact_for_binding(label, &binding())
    }

    fn canonical_artifact_for_binding(
        label: &[u8],
        binding: &UnavailablePgTransitionMutationBinding,
    ) -> &'static [u8] {
        let source_route_epoch = binding.source_epoch().get();
        let source_epoch = ClusterEpoch::new(
            source_route_epoch - checksum::crc64::checksum(label) % source_route_epoch,
        )
        .unwrap();
        let destination_epoch =
            ClusterEpoch::new(binding.transition_epoch().get().checked_add(1).unwrap()).unwrap();
        let artifact = PgMetadataTransferArtifact {
            pg_id: binding.pg_id(),
            source_node_id: binding.source_acting_set()[0],
            cluster_epoch: source_epoch,
            base_kind: PgMetadataTransferBaseKind::Empty,
            base_proof: PgMetadataProof::empty(),
            checkpoint_base: None,
            proof: PgMetadataProof::empty(),
            retained_log_entries: Vec::new(),
        };
        Box::leak(
            encode_staged_metadata_transfer_artifact(&artifact, destination_epoch)
                .unwrap()
                .into_boxed_slice(),
        )
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
    fn staging_intent_codec_is_canonical_and_bounded() {
        let expected = intent(b"canonical staged artifact");
        let encoded = encode_staging_intent(&expected).unwrap();

        assert_eq!(decode_staging_intent(&encoded).unwrap(), expected);

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(matches!(
            decode_staging_intent(&trailing),
            Err(MetadataTransferStagingError::Invariant(_))
        ));
        assert!(matches!(
            decode_staging_intent(&encoded[..encoded.len() - 1]),
            Err(MetadataTransferStagingError::Invariant(_))
        ));
        assert!(matches!(
            decode_staging_intent(&vec![0; MAX_STAGING_INTENT_BYTES + 1]),
            Err(MetadataTransferStagingError::Invariant(_))
        ));
    }

    #[test]
    fn current_metadata_transfer_staging_store_matches_frozen_v4_manifest_and_requires_version_bump(
    ) {
        assert_eq!(
            hex(&current_manifest_bytes()),
            "4152474d53544700000423102023e7f72fa5d48c93286276a9beab0186ee83033fda684f4c73b6445fec97dbab018fc0b574"
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
    fn initialization_marker_v4_encoding_is_fixed() {
        assert_eq!(
            hex(&current_initialization_marker_bytes()),
            "4152474d53544749000423102023e7f72fa5d48c93286276a9beab0186ee83033fda684f4c73b6445fec802d84e195a9f422"
        );
    }

    #[test]
    fn establishment_marker_v4_encoding_is_fixed() {
        assert_eq!(
            hex(&current_establishment_marker_bytes()),
            "4152474d53544745000489a556301f7a0cfa523303a2fe8cf748d3886648a69b685a196d96fbcb7e0a3e413ecc4a40ff5b0e"
        );
    }

    #[test]
    fn historical_staging_store_v1_v2_v3_markers_remain_exact_rejection_evidence() {
        for (version, manifest, initialization, establishment) in [
            (
                1,
                "4152474d5354470000013f41a30ad6b597e31dbdbd63329cd7ccd944c18a5c27b93f492d3f684774c2ed7b6fa66472b5374b",
                "4152474d5354474900013f41a30ad6b597e31dbdbd63329cd7ccd944c18a5c27b93f492d3f684774c2ed6c99898468dc761d",
                "4152474d535447450001a7221d66ea51b3a38deafc1868ce6a6b0bb884463dd56626720658a445cc29bcb4fd5b0300c7b553",
            ),
            (
                2,
                "4152474d5354470000026e722f4492ef06abc27ea34e6bc367916cd81c0ef362fb9c4f1e41b1e5cae494f8f99a4accd44275",
                "4152474d5354474900026e722f4492ef06abc27ea34e6bc367916cd81c0ef362fb9c4f1e41b1e5cae494ef0fb5aad6bd0323",
                "4152474d5354474500027c9533697c7a588542438ed5f246c781c987d4a750c7c2b52d8ed329cd145a1453a427f53714c545",
            ),
            (
                3,
                "4152474d535447000003643f43e864097c6001733c10fefe1649bd18411ef8c7e4392efc5e27762c1f6f2f034ddb97f20b7b",
                "4152474d535447490003643f43e864097c6001733c10fefe1649bd18411ef8c7e4392efc5e27762c1f6f38f5623b8d9b4a2d",
                "4152474d535447450003a1d53a4cc83b031831e2c5cc0274d4d0acbde55d560a014e2b59c132195f052b0af5e90a20720eaa",
            ),
        ] {
            for result in [
                validate_manifest(&decode_hex(manifest)),
                validate_initialization_marker(&decode_hex(initialization)),
                validate_establishment_marker(&decode_hex(establishment)),
            ] {
                assert!(matches!(
                    result,
                    Err(MetadataTransferStagingError::UnsupportedFormatVersion(actual))
                        if actual == version
                ));
            }
        }
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
        let artifact = canonical_artifact(b"receipt after durable staging-root establishment");
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

            let artifact =
                canonical_artifact(b"receipt only after retrying the failed parent sync");
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
                marker[offset..offset + 2].copy_from_slice(&5u16.to_be_bytes());
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
                    MetadataTransferStagingError::UnsupportedFormatVersion(5)
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
                marker[offset..offset + 2].copy_from_slice(&5u16.to_be_bytes());
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
                    MetadataTransferStagingError::UnsupportedFormatVersion(5)
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
        for version in [1u16, 2, 3, 5] {
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
    fn staged_artifact_format_accepts_only_exact_v3() {
        for version in [1, 2, 4] {
            let error = MetadataTransferStagingIntent::for_unavailable_transition(
                &binding(),
                [7; DIGEST_LEN],
                17,
                version,
            )
            .unwrap_err();
            assert!(matches!(error, MetadataTransferStagingError::Invariant(_)));
        }
        assert!(matches!(
            MetadataTransferStagingIntent::for_unavailable_transition(
                &binding(),
                [7; DIGEST_LEN],
                METADATA_TRANSFER_STAGED_ARTIFACT_MAX_BYTES + 1,
                METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
            ),
            Err(MetadataTransferStagingError::ArtifactTooLarge { .. })
        ));
        assert!(matches!(
            MetadataTransferStagingLimits::new(
                1,
                METADATA_TRANSFER_STAGED_ARTIFACT_MAX_BYTES + 1,
                METADATA_TRANSFER_STAGED_ARTIFACT_MAX_BYTES + 1,
            ),
            Err(MetadataTransferStagingError::Invariant(_))
        ));
    }

    #[test]
    fn staged_artifact_publication_derives_and_binds_the_exact_transfer_proof() {
        let tmp = test_util::tempdir();
        let artifact = PgMetadataTransferArtifact {
            pg_id: PgId::new(19),
            source_node_id: NodeId::new(1),
            cluster_epoch: ClusterEpoch::new(11).unwrap(),
            base_kind: PgMetadataTransferBaseKind::Empty,
            base_proof: PgMetadataProof::empty(),
            checkpoint_base: None,
            proof: PgMetadataProof::empty(),
            retained_log_entries: Vec::new(),
        };
        let destination_epoch = ClusterEpoch::new(13).unwrap();
        let bytes = encode_staged_metadata_transfer_artifact(&artifact, destination_epoch).unwrap();
        let staged_intent = intent(&bytes);
        let store = open(tmp.path());
        store.create_intent(&staged_intent).unwrap();

        let receipt = store.publish_artifact(&staged_intent, &bytes).unwrap();
        let evidence = decode_staging_evidence(receipt.as_bytes()).unwrap();
        assert_eq!(
            evidence.transfer(),
            Some(PgMetadataTransferProof::new_with_imported_metadata_proof(
                artifact.cluster_epoch,
                artifact.proof,
                PgMetadataProof::empty(),
            ))
        );

        let mut forged = bytes;
        let imported_hash_value_offset =
            STAGED_ARTIFACT_MAGIC.len() + 2 + 4 + 4 + 8 + 1 + 26 + 26 + 8 + 8 + 1;
        forged[imported_hash_value_offset] ^= 1;
        let forged_intent = intent(&forged);
        let forged_tmp = test_util::tempdir();
        let forged_store = open(forged_tmp.path());
        forged_store.create_intent(&forged_intent).unwrap();
        assert!(matches!(
            forged_store.publish_artifact(&forged_intent, &forged),
            Err(MetadataTransferStagingError::ArtifactSemanticMismatch(_))
        ));

        let future_artifact = PgMetadataTransferArtifact {
            cluster_epoch: ClusterEpoch::new(12).unwrap(),
            ..artifact
        };
        let future_bytes =
            encode_staged_metadata_transfer_artifact(&future_artifact, destination_epoch).unwrap();
        let future_intent = intent(&future_bytes);
        let future_tmp = test_util::tempdir();
        let future_store = open(future_tmp.path());
        future_store.create_intent(&future_intent).unwrap();
        assert!(matches!(
            future_store.publish_artifact(&future_intent, &future_bytes),
            Err(MetadataTransferStagingError::ArtifactSemanticMismatch(_))
        ));
    }

    #[test]
    fn nonempty_staged_artifact_publishes_replaceable_epoch_bound_proofs() {
        let tmp = test_util::tempdir();
        let initial_epoch = ClusterEpoch::new(13).unwrap();
        let rebased_epoch = ClusterEpoch::new(14).unwrap();
        let artifact = canonical_nonempty_staged_metadata_transfer_artifact_for_test(
            &binding(),
            initial_epoch,
        );
        let intent = intent(&artifact);
        let store = open(tmp.path());
        store.create_intent(&intent).unwrap();

        let initial = store.publish_artifact(&intent, &artifact).unwrap();
        let initial_evidence = decode_staging_evidence(initial.as_bytes()).unwrap();
        assert_eq!(initial_evidence.target_epoch(), Some(initial_epoch));
        let rebased = store
            .publish_proof_for_epoch(&intent, rebased_epoch)
            .unwrap();
        let rebased_evidence = decode_staging_evidence(rebased.as_bytes()).unwrap();
        assert_eq!(rebased_evidence.target_epoch(), Some(rebased_epoch));
        assert_ne!(rebased_evidence.transfer(), initial_evidence.transfer());
        assert_eq!(
            store
                .publish_proof_for_epoch(&intent, rebased_epoch)
                .unwrap(),
            rebased
        );
        assert_eq!(store.publish_artifact(&intent, &artifact).unwrap(), initial);
        drop(store);

        let reopened = open(tmp.path());
        assert_eq!(
            reopened
                .publish_proof_for_epoch(&intent, rebased_epoch)
                .unwrap(),
            rebased
        );
        assert_eq!(
            reopened.publish_artifact(&intent, &artifact).unwrap(),
            initial
        );
        assert!(matches!(
            reopened.publish_proof_for_epoch(&intent, binding().transition_epoch()),
            Err(MetadataTransferStagingError::Invariant(_))
        ));
    }

    #[test]
    fn epoch_rebound_publication_receipts_survive_actor_rollover_and_reopen() {
        let tmp = test_util::tempdir();
        let initial_epoch = ClusterEpoch::new(13).unwrap();
        let rebound_epoch = ClusterEpoch::new(14).unwrap();
        let artifact = canonical_nonempty_staged_metadata_transfer_artifact_for_test(
            &binding(),
            initial_epoch,
        );
        let intent = intent(&artifact);
        let old = open(tmp.path());
        old.create_intent(&intent).unwrap();
        let initial = old.publish_artifact(&intent, &artifact).unwrap();
        let rebound = old.publish_proof_for_epoch(&intent, rebound_epoch).unwrap();
        drop(old);

        let restarted_identity = MetadataTransferStagingNodeIdentity::new(
            NodeId::new(4),
            8,
            "tcp://storage-4.example:9000".to_owned(),
        )
        .unwrap();
        let restarted =
            MetadataTransferStagingStore::open(tmp.path(), restarted_identity.clone(), limits())
                .unwrap();
        let rebound_initial = restarted.publish_artifact(&intent, &artifact).unwrap();
        let rebound_current = restarted
            .publish_proof_for_epoch(&intent, rebound_epoch)
            .unwrap();
        assert_ne!(rebound_initial, initial);
        assert_ne!(rebound_current, rebound);
        for (receipt, target_epoch) in [
            (&rebound_initial, initial_epoch),
            (&rebound_current, rebound_epoch),
        ] {
            let evidence = decode_staging_evidence(receipt.as_bytes()).unwrap();
            assert_eq!(evidence.actor(), &restarted_identity);
            assert_eq!(evidence.target_epoch(), Some(target_epoch));
        }
        let page = restarted.next_evidence_page().unwrap().unwrap();
        assert_eq!(page.entries().len(), 2);
        assert!(page.entries().iter().all(|entry| {
            decode_staging_evidence(entry.evidence()).unwrap().actor() == &restarted_identity
        }));
        drop(restarted);

        let reopened =
            MetadataTransferStagingStore::open(tmp.path(), restarted_identity, limits()).unwrap();
        assert_eq!(
            reopened.publish_artifact(&intent, &artifact).unwrap(),
            rebound_initial
        );
        assert_eq!(
            reopened
                .publish_proof_for_epoch(&intent, rebound_epoch)
                .unwrap(),
            rebound_current
        );
    }

    #[test]
    fn staging_store_rejects_intents_for_another_destination_before_mutation() {
        let tmp = test_util::tempdir();
        let store = MetadataTransferStagingStore::open(
            tmp.path(),
            identity(),
            MetadataTransferStagingLimits::new(1, 1_024, 1_024).unwrap(),
        )
        .unwrap();
        let artifact = canonical_artifact(b"misrouted artifact");
        let misrouted = MetadataTransferStagingIntent::for_unavailable_transition(
            &binding_with_destination(NodeId::new(5)),
            checksum::sha256::digest(artifact),
            u64::try_from(artifact.len()).unwrap(),
            METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
        )
        .unwrap();

        assert!(matches!(
            store.create_intent(&misrouted),
            Err(MetadataTransferStagingError::Invariant(message))
                if message.contains("not a destination")
        ));
        assert!(matches!(
            store.publish_artifact(&misrouted, artifact),
            Err(MetadataTransferStagingError::Invariant(message))
                if message.contains("not a destination")
        ));

        let cross_member =
            crate::control_plane::tests::transitions::authenticated_staging_authorization_fixture();
        assert!(matches!(
            store.create_intent_authorized(
                &cross_member.cross_member_authorization,
                &cross_member.cross_member_intent,
            ),
            Err(MetadataTransferStagingError::IntentConflict(message))
                if message.contains("not bound to this staging actor and PG")
        ));
        assert!(!store.has_intent_for_test(
            cross_member.cross_member_intent.pg_id(),
            cross_member.cross_member_intent.staging_generation(),
        ));
        assert!(matches!(
            store.tombstone_authorized(
                &cross_member.cross_member_authorization,
                &cross_member.cross_member_intent,
            ),
            Err(MetadataTransferStagingError::IntentConflict(message))
                if message.contains("not bound to this staging actor and PG")
        ));
        assert!(!store.has_intent_for_test(
            cross_member.cross_member_intent.pg_id(),
            cross_member.cross_member_intent.staging_generation(),
        ));

        let valid_artifact = canonical_artifact(b"valid artifact");
        let valid = intent(valid_artifact);
        assert_eq!(
            store.create_intent(&valid).unwrap(),
            MetadataTransferStagingIntentOutcome::Created
        );
        assert!(!store
            .publish_artifact(&valid, valid_artifact)
            .unwrap()
            .as_bytes()
            .is_empty());
    }

    #[test]
    fn committed_authorization_tombstones_exact_intent_and_removes_artifact() {
        let tmp = test_util::tempdir();
        let store = open(tmp.path());
        let artifact = canonical_artifact(b"authorized tombstone");
        let intent = intent(artifact);
        let authorization =
            crate::pg_store::committed_staging_authorization_for_intent_for_test(&intent);
        store
            .create_intent_authorized(&authorization, &intent)
            .unwrap();
        let publication = store
            .publish_artifact_authorized(&authorization, &intent, artifact)
            .unwrap();
        assert!(store.artifact_path(&intent).exists());

        let first = store.tombstone_authorized(&authorization, &intent).unwrap();
        let replay = store.tombstone_authorized(&authorization, &intent).unwrap();

        assert_eq!(replay, first);
        assert!(!store.artifact_path(&intent).exists());
        let evidence = decode_staging_evidence(first.as_bytes()).unwrap();
        assert_eq!(
            evidence.kind(),
            MetadataTransferStagingEvidenceKind::Tombstone
        );
        assert_eq!(evidence.intent(), &intent);
        assert_eq!(evidence.actor(), &identity());
        assert_eq!(evidence.target_epoch(), None);
        assert_eq!(evidence.transfer(), None);
        assert!(MetadataTransferStagingReceipt::from_tombstone_bytes(
            publication.as_bytes(),
            &intent,
            NodeId::new(4),
        )
        .is_err());
        assert!(MetadataTransferStagingReceipt::from_publication_bytes(
            first.as_bytes(),
            &intent,
            NodeId::new(4),
        )
        .is_err());
    }

    #[test]
    fn staging_receipt_v4_encoding_is_fixed_and_v2_v3_remain_rejected() {
        let historical_v2 = decode_hex(
            "4152474d494e2d53544147494e472d45564944454e43452d5632000000000004000000000000000700000021756e69783a2f2f2f72756e2f6172676d696e2f73746f726167652d342e736f636b00000013000000000000000c000000000000000b00000010000000030000000100000002000000030000001000000003000000040000000200000003000000000000000c0a2188fce572c606858dffa56cc1590344a166e9ff8858bab341658e0c2e596000000000000000930002000000000000000d01000000000000000b0000000000000000010000000000000000050000000000000000000000000000000001000000000000000005000000000000000007",
        );
        assert!(decode_staging_evidence(&historical_v2).is_err());
        let historical_v3 = decode_hex(
            "4152474d494e2d53544147494e472d45564944454e43452d5633000000000004000000000000000700000021756e69783a2f2f2f72756e2f6172676d696e2f73746f726167652d342e736f636b00000013000000000000000c000000000000000b00000010000000030000000100000002000000030000001000000003000000040000000200000003000000000000000c0efadd220adcc57506f8a395ce807f64911785127f9af6724bde5399ea513ba500000000000000930003000000000000000d01000000000000000b0000000000000000010000000000000000050000000000000000000000000000000001000000000000000005000000000000000007",
        );
        assert!(decode_staging_evidence(&historical_v3).is_err());
        let artifact = canonical_artifact(b"one retained metadata command receipt");
        let expected_intent = intent(artifact);
        let receipt = encode_staging_evidence(
            &identity(),
            &expected_intent,
            0,
            Some(test_staging_evidence_target_epoch(&expected_intent)),
            test_staging_evidence_transfer(
                &expected_intent,
                MetadataTransferStagingEvidenceKind::Publication,
            ),
        );
        assert_eq!(
            hex(&receipt),
            "4152474d494e2d53544147494e472d45564944454e43452d5634000000000004000000000000000700000021756e69783a2f2f2f72756e2f6172676d696e2f73746f726167652d342e736f636b00000013000000000000000c000000000000000b00000010000000030000000100000002000000030000001000000003000000040000000200000003000000000000000c0efadd220adcc57506f8a395ce807f64911785127f9af6724bde5399ea513ba500000000000000930003000000000000000d01000000000000000b0000000000000000010000000000000000050000000000000000000000000000000001000000000000000005000000000000000007"
        );
        validate_staging_evidence(&receipt, &expected_intent, 0, &identity()).unwrap();
        assert!(MetadataTransferStagingReceipt::from_publication_bytes(
            &receipt,
            &expected_intent,
            NodeId::new(4),
        )
        .is_ok());
        assert!(MetadataTransferStagingReceipt::from_publication_bytes(
            &receipt,
            &intent(b"another artifact"),
            NodeId::new(4),
        )
        .is_err());
        assert!(MetadataTransferStagingReceipt::from_publication_bytes(
            &receipt,
            &expected_intent,
            NodeId::new(5),
        )
        .is_err());
        let tombstone = encode_staging_evidence(&identity(), &expected_intent, 1, None, None);
        assert!(MetadataTransferStagingReceipt::from_publication_bytes(
            &tombstone,
            &expected_intent,
            NodeId::new(4),
        )
        .is_err());
    }

    #[test]
    fn artifact_publication_is_durable_idempotent_and_restart_readable() {
        let tmp = test_util::tempdir();
        let artifact = canonical_artifact(b"one retained metadata command publication");
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
            MetadataTransferStagingStore::open(tmp.path(), replacement_identity.clone(), limits())
                .unwrap();
        assert_eq!(reopened.read_artifact(&intent).unwrap(), artifact);
        let rebound_receipt = reopened.publish_artifact(&intent, artifact).unwrap();
        assert_ne!(rebound_receipt, receipt);
        assert_eq!(
            decode_staging_evidence(rebound_receipt.as_bytes())
                .unwrap()
                .actor(),
            &replacement_identity
        );
        assert_eq!(
            reopened.publish_artifact(&intent, artifact).unwrap(),
            rebound_receipt
        );
        reopened.mark_imported(&intent).unwrap();
        reopened.mark_imported(&intent).unwrap();
    }

    #[test]
    fn existing_artifact_retry_syncs_directory_before_publication_receipt() {
        let tmp = test_util::tempdir();
        let artifact = canonical_artifact(b"renamed before publication retry");
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
        let artifact = canonical_artifact(b"renamed before startup recovery");
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
    fn invalid_open_identity_is_rejected_before_startup_reconciliation_mutates_state() {
        let invalid_identities = [
            MetadataTransferStagingNodeIdentity::new(
                NodeId::new(5),
                8,
                "tcp://storage-5.example:9000".to_owned(),
            )
            .unwrap(),
            MetadataTransferStagingNodeIdentity::new(
                NodeId::new(4),
                6,
                "unix:///run/argmin/storage-4.sock".to_owned(),
            )
            .unwrap(),
            MetadataTransferStagingNodeIdentity::new(
                NodeId::new(4),
                7,
                "tcp://storage-4.example:9001".to_owned(),
            )
            .unwrap(),
        ];
        for invalid_identity in invalid_identities {
            let tmp = test_util::tempdir();
            let artifact = canonical_artifact(b"unreconciled publication");
            let intent = intent(artifact);
            let store = open(tmp.path());
            store.create_intent(&intent).unwrap();
            let artifact_path = store.artifact_path(&intent);
            fs::write(&artifact_path, artifact).unwrap();
            File::open(&artifact_path).unwrap().sync_all().unwrap();
            let unknown_path = store.artifacts_dir.join("unknown.artifact");
            fs::write(&unknown_path, b"unexplained").unwrap();
            drop(store);

            assert!(
                MetadataTransferStagingStore::open(tmp.path(), invalid_identity, limits(),)
                    .is_err()
            );
            assert!(artifact_path.exists());
            assert!(unknown_path.exists());
            assert!(!tmp
                .path()
                .join(STAGING_STORE_DIR)
                .join(QUARANTINE_DIR)
                .join("unknown.artifact")
                .exists());
            let connection = Connection::open(catalogue_path(tmp.path())).unwrap();
            assert_eq!(
                load_staging_row(&connection, intent.pg_id, intent.staging_generation)
                    .unwrap()
                    .unwrap()
                    .state,
                StagingState::Intent
            );
        }
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
        let artifact = canonical_artifact(b"staged then cancelled");
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
    fn tombstone_before_intent_rejects_delayed_stage_and_rebinds_receipt_on_restart() {
        let tmp = test_util::tempdir();
        let artifact = canonical_artifact(b"cancelled before destination stage");
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
            MetadataTransferStagingStore::open(tmp.path(), replacement_identity.clone(), limits())
                .unwrap();
        assert!(!delayed_path.exists());
        fs::write(&delayed_path, artifact).unwrap();
        let rebound_receipt = reopened.tombstone(&intent).unwrap();
        assert_ne!(rebound_receipt, receipt);
        assert_eq!(
            decode_staging_evidence(rebound_receipt.as_bytes())
                .unwrap()
                .actor(),
            &replacement_identity
        );
        assert!(!delayed_path.exists());
        assert!(matches!(
            reopened.publish_artifact(&intent, artifact),
            Err(MetadataTransferStagingError::GenerationRetired)
        ));
    }

    #[test]
    fn restart_recovers_exact_renamed_artifact_and_quarantines_unknown_file() {
        let tmp = test_util::tempdir();
        let artifact = canonical_artifact(b"renamed before catalogue commit");
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
            let artifact = canonical_artifact(b"durable artifact");
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
        for version in [0u32, 1, 2, 3, 5] {
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
        let artifact = canonical_artifact(b"receipt-bound artifact");
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
            let artifact = canonical_artifact(b"actor-bound staging receipt");
            let intent = intent(artifact);
            let store = open(tmp.path());
            store.create_intent(&intent).unwrap();
            store.publish_artifact(&intent, artifact).unwrap();
            drop(store);
            let forged_receipt = encode_staging_evidence(
                &forged_identity,
                &intent,
                0,
                Some(test_staging_evidence_target_epoch(&intent)),
                test_staging_evidence_transfer(
                    &intent,
                    MetadataTransferStagingEvidenceKind::Publication,
                ),
            );
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
        let recover_artifact = canonical_artifact(b"renamed but not committed");
        let recover_intent = intent(recover_artifact);
        let published_binding = binding_for_pg(PgId::new(20), NodeId::new(4));
        let published_artifact =
            canonical_artifact_for_binding(b"published with later corruption", &published_binding);
        let published_intent = intent_for_binding(published_artifact, &published_binding);
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

    #[test]
    fn evidence_page_replays_within_an_incarnation_and_rebinds_on_restart() {
        let tmp = test_util::tempdir();
        let first_artifact = canonical_artifact(b"first evidence page artifact");
        let first = intent(first_artifact);
        let store = open(tmp.path());
        store.create_intent(&first).unwrap();
        store.publish_artifact(&first, first_artifact).unwrap();

        let first_page = store.next_evidence_page().unwrap().unwrap();
        assert_eq!(first_page.generation(), 1);
        assert_eq!(first_page.entries.len(), 1);
        let first_payload = first_page.operation_payload().to_vec();
        let first_digest = first_page.page_digest();

        let mut second = intent(b"second evidence page artifact");
        second.pg_id = PgId::new(20);
        store.tombstone(&second).unwrap();
        let replay = store.next_evidence_page().unwrap().unwrap();
        assert_eq!(replay.operation_payload(), first_payload);
        assert_eq!(replay.page_digest(), first_digest);
        assert_eq!(replay.entries, first_page.entries);
        drop(store);

        let restarted_identity = MetadataTransferStagingNodeIdentity::new(
            NodeId::new(4),
            8,
            "tcp://storage-4.example:9000".to_owned(),
        )
        .unwrap();
        let restarted =
            MetadataTransferStagingStore::open(tmp.path(), restarted_identity.clone(), limits())
                .unwrap();
        let replay = restarted.next_evidence_page().unwrap().unwrap();
        assert_ne!(replay.operation_payload(), first_payload);
        assert_ne!(replay.page_digest(), first_digest);
        assert_eq!(replay.actor(), &restarted_identity);
        assert_eq!(replay.previous_generation(), 0);
        assert_eq!(replay.generation(), 1);
        assert_eq!(replay.entries.len(), 2);
        let closure = replay
            .actor_closure_candidate()
            .expect("rollover genesis must carry durable actor closure evidence");
        assert_eq!(closure.first_actor(), &identity());
        assert_eq!(closure.first_accepted_generation(), 0);
        assert_eq!(closure.first_accepted_apply_receipt_digest(), [0; 32]);
        assert!(closure.accepts_first_tip(
            &identity(),
            first_page.generation(),
            first_page.page_digest(),
            checksum::sha256::digest(
                MetadataTransferStagingEvidenceApplyReceipt::for_page(&first_page).as_bytes()
            ),
        ));
        assert_eq!(closure.through_actor(), &identity());
        assert_eq!(closure.rebound_entry_count(), 2);
        assert_eq!(closure.rebound_max_sequence(), 2);
        assert_eq!(
            closure.rebound_evidence_digest(),
            metadata_transfer_staging_rebound_evidence_digest(
                replay
                    .entries()
                    .iter()
                    .map(|entry| (entry.sequence(), entry.evidence()))
            )
            .unwrap()
            .2
        );
        for entry in replay.entries() {
            assert_eq!(
                decode_staging_evidence(entry.evidence()).unwrap().actor(),
                &restarted_identity
            );
        }
        assert_eq!(restarted.unacknowledged_evidence_count(), 2);
    }

    #[test]
    fn repeated_actor_rollover_preserves_the_first_tip_and_advances_the_rebind_boundary() {
        let tmp = test_util::tempdir();
        let first = open(tmp.path());
        first
            .tombstone(&intent(b"first rollover evidence"))
            .unwrap();
        let first_page = first.next_evidence_page().unwrap().unwrap();
        drop(first);

        let second_identity = MetadataTransferStagingNodeIdentity::new(
            NodeId::new(4),
            8,
            "tcp://storage-4.example:9000".to_owned(),
        )
        .unwrap();
        let second =
            MetadataTransferStagingStore::open(tmp.path(), second_identity.clone(), limits())
                .unwrap();
        let second_page = second.next_evidence_page().unwrap().unwrap();
        drop(second);

        let third_identity = MetadataTransferStagingNodeIdentity::new(
            NodeId::new(4),
            9,
            "tcp://storage-4.example:9001".to_owned(),
        )
        .unwrap();
        let third =
            MetadataTransferStagingStore::open(tmp.path(), third_identity.clone(), limits())
                .unwrap();
        let third_page = third.next_evidence_page().unwrap().unwrap();
        let closure = third_page.actor_closure_candidate().unwrap();

        assert_eq!(closure.first_actor(), &identity());
        assert_eq!(closure.first_accepted_generation(), 0);
        assert!(closure.accepts_first_tip(
            &identity(),
            first_page.generation(),
            first_page.page_digest(),
            checksum::sha256::digest(
                MetadataTransferStagingEvidenceApplyReceipt::for_page(&first_page).as_bytes()
            ),
        ));
        assert_eq!(closure.through_actor(), &second_identity);
        assert_eq!(third_page.actor(), &third_identity);
        assert_ne!(third_page.page_digest(), second_page.page_digest());
        assert_eq!(closure.rebound_entry_count(), 1);
        assert_eq!(closure.rebound_max_sequence(), 1);
    }

    #[test]
    fn rollover_preserves_acknowledged_floor_and_ambiguous_assigned_successor() {
        let tmp = test_util::tempdir();
        let first = open(tmp.path());
        first
            .tombstone(&intent(b"acknowledged rollover evidence"))
            .unwrap();
        let acknowledged_page = first.next_evidence_page().unwrap().unwrap();
        let acknowledged_receipt =
            MetadataTransferStagingEvidenceApplyReceipt::for_page(&acknowledged_page);
        first
            .record_evidence_apply_receipt(&acknowledged_page, &acknowledged_receipt)
            .unwrap();

        let mut second_intent = intent(b"ambiguous rollover evidence");
        second_intent.pg_id = PgId::new(20);
        first.tombstone(&second_intent).unwrap();
        let ambiguous_page = first.next_evidence_page().unwrap().unwrap();
        assert_eq!(ambiguous_page.generation(), 2);
        drop(first);

        let next_identity = MetadataTransferStagingNodeIdentity::new(
            NodeId::new(4),
            8,
            "tcp://storage-4.example:9000".to_owned(),
        )
        .unwrap();
        let restarted =
            MetadataTransferStagingStore::open(tmp.path(), next_identity, limits()).unwrap();
        let rebound_page = restarted.next_evidence_page().unwrap().unwrap();
        let closure = rebound_page.actor_closure_candidate().unwrap();

        assert_eq!(
            closure.first_accepted_generation(),
            acknowledged_page.generation()
        );
        assert_eq!(
            closure.first_accepted_apply_receipt_digest(),
            checksum::sha256::digest(acknowledged_receipt.as_bytes())
        );
        assert!(closure.accepts_first_tip(
            acknowledged_page.actor(),
            acknowledged_page.generation(),
            acknowledged_page.page_digest(),
            checksum::sha256::digest(acknowledged_receipt.as_bytes()),
        ));
        assert!(closure.accepts_first_tip(
            ambiguous_page.actor(),
            ambiguous_page.generation(),
            ambiguous_page.page_digest(),
            checksum::sha256::digest(
                MetadataTransferStagingEvidenceApplyReceipt::for_page(&ambiguous_page).as_bytes()
            ),
        ));
    }

    #[test]
    fn actor_closure_candidate_binds_the_complete_paged_rebound_prefix() {
        let tmp = test_util::tempdir();
        let evidence_count = MAX_STAGING_EVIDENCE_PAGE_ENTRIES + 1;
        let page_limits = MetadataTransferStagingLimits::new(
            evidence_count,
            1024,
            u64::try_from(evidence_count).unwrap() * 1024,
        )
        .unwrap();
        let first =
            MetadataTransferStagingStore::open(tmp.path(), identity(), page_limits).unwrap();
        for pg in 1..=u32::try_from(evidence_count).unwrap() {
            let mut cancelled = intent(b"paged actor closure evidence");
            cancelled.pg_id = PgId::new(pg);
            first.tombstone(&cancelled).unwrap();
        }
        let old_page = first.next_evidence_page().unwrap().unwrap();
        drop(first);

        let next_identity = MetadataTransferStagingNodeIdentity::new(
            NodeId::new(4),
            8,
            "tcp://storage-4.example:9000".to_owned(),
        )
        .unwrap();
        let restarted =
            MetadataTransferStagingStore::open(tmp.path(), next_identity.clone(), page_limits)
                .unwrap();
        let first_rebound_page = restarted.next_evidence_page().unwrap().unwrap();
        let closure = first_rebound_page
            .actor_closure_candidate()
            .cloned()
            .expect("rebound genesis must carry the closure candidate");
        assert!(closure.accepts_first_tip(
            &identity(),
            old_page.generation(),
            old_page.page_digest(),
            checksum::sha256::digest(
                MetadataTransferStagingEvidenceApplyReceipt::for_page(&old_page).as_bytes()
            ),
        ));
        assert_eq!(
            closure.rebound_entry_count(),
            u64::try_from(evidence_count).unwrap()
        );
        assert!(first_rebound_page.entries().len() < evidence_count);

        let mut impossible_closure = closure.clone();
        impossible_closure.rebound_entry_count = impossible_closure.rebound_max_sequence + 1;
        assert!(validate_staging_evidence_actor_closure_candidate(&impossible_closure).is_err());
        let impossible_payload = encode_staging_evidence_page_payload(
            first_rebound_page.actor(),
            Some(&impossible_closure),
            first_rebound_page.previous_generation(),
            first_rebound_page.previous_apply_receipt_digest(),
            first_rebound_page.generation(),
            first_rebound_page.entries(),
        );
        assert!(decode_staging_evidence_page_payload(
            &impossible_payload,
            checksum::sha256::digest(&impossible_payload),
        )
        .is_err());

        let mut rebound_entries = Vec::new();
        let mut page = first_rebound_page;
        loop {
            assert_eq!(page.actor(), &next_identity);
            if page.generation() > 1 {
                assert!(page.actor_closure_candidate().is_none());
            }
            rebound_entries.extend(
                page.entries()
                    .iter()
                    .map(|entry| (entry.sequence(), entry.evidence().to_vec())),
            );
            let receipt = MetadataTransferStagingEvidenceApplyReceipt::for_page(&page);
            restarted
                .record_evidence_apply_receipt(&page, &receipt)
                .unwrap();
            let Some(next) = restarted.next_evidence_page().unwrap() else {
                break;
            };
            page = next;
        }

        assert_eq!(rebound_entries.len(), evidence_count);
        let (entry_count, max_sequence, evidence_digest) =
            metadata_transfer_staging_rebound_evidence_digest(
                rebound_entries
                    .iter()
                    .map(|(sequence, evidence)| (*sequence, evidence.as_slice())),
            )
            .unwrap();
        assert_eq!(entry_count, closure.rebound_entry_count());
        assert_eq!(max_sequence, closure.rebound_max_sequence());
        assert_eq!(evidence_digest, closure.rebound_evidence_digest());
        drop(restarted);

        let connection = Connection::open(catalogue_path(tmp.path())).unwrap();
        assert!(connection
            .execute(
                "UPDATE staging_evidence_actor_closure_candidate \
                 SET rebound_entry_count = rebound_max_sequence + 1",
                [],
            )
            .is_err());
    }

    #[test]
    fn evidence_actor_rollover_fences_stale_handles_and_same_incarnation_endpoint_changes() {
        let tmp = test_util::tempdir();
        let old = open(tmp.path());
        old.tombstone(&intent(b"actor rollover fence")).unwrap();
        let next_identity = MetadataTransferStagingNodeIdentity::new(
            NodeId::new(4),
            8,
            "tcp://storage-4.example:9000".to_owned(),
        )
        .unwrap();
        let next = MetadataTransferStagingStore::open(tmp.path(), next_identity.clone(), limits())
            .unwrap();
        assert!(old
            .next_evidence_page()
            .unwrap_err()
            .to_string()
            .contains("stale evidence actor"));
        assert_eq!(
            next.next_evidence_page().unwrap().unwrap().actor(),
            &next_identity
        );

        let changed_endpoint = MetadataTransferStagingNodeIdentity::new(
            NodeId::new(4),
            8,
            "tcp://storage-4.example:9001".to_owned(),
        )
        .unwrap();
        assert!(
            MetadataTransferStagingStore::open(tmp.path(), changed_endpoint, limits())
                .err()
                .expect("same-incarnation endpoint change must fail")
                .to_string()
                .contains("incarnation did not advance")
        );
    }

    #[test]
    fn evidence_actor_rollover_fences_every_stale_filesystem_and_catalogue_mutation() {
        let tmp = test_util::tempdir();
        let old = open(tmp.path());

        let unpublished_bytes = canonical_artifact(b"stale publication");
        let unpublished = intent(unpublished_bytes);
        old.create_intent(&unpublished).unwrap();

        let published_binding = binding_for_pg(PgId::new(20), NodeId::new(4));
        let published_artifact =
            canonical_artifact_for_binding(b"stale import", &published_binding);
        let published = intent_for_binding(published_artifact, &published_binding);
        old.create_intent(&published).unwrap();
        old.publish_artifact(&published, published_artifact)
            .unwrap();

        let retained_binding = binding_for_pg(PgId::new(21), NodeId::new(4));
        let retained_artifact_bytes =
            canonical_artifact_for_binding(b"stale tombstone", &retained_binding);
        let retained_artifact = intent_for_binding(retained_artifact_bytes, &retained_binding);
        old.create_intent(&retained_artifact).unwrap();
        old.publish_artifact(&retained_artifact, retained_artifact_bytes)
            .unwrap();
        let retained_artifact_path = old.artifact_path(&retained_artifact);

        let mut retired = intent(b"stale finalized floor");
        retired.pg_id = PgId::new(22);
        old.tombstone(&retired).unwrap();

        let next_identity = MetadataTransferStagingNodeIdentity::new(
            NodeId::new(4),
            8,
            "tcp://storage-4.example:9000".to_owned(),
        )
        .unwrap();
        let next = MetadataTransferStagingStore::open(tmp.path(), next_identity, limits()).unwrap();

        let stale = |error: MetadataTransferStagingError| {
            assert!(error.to_string().contains("stale evidence actor"));
        };
        stale(
            old.publish_artifact(&unpublished, unpublished_bytes)
                .unwrap_err(),
        );
        assert!(!old.artifact_path(&unpublished).exists());
        stale(old.mark_imported(&published).unwrap_err());
        stale(old.tombstone(&retained_artifact).unwrap_err());
        assert!(retained_artifact_path.exists());
        stale(
            old.advance_finalized_floor(retired.pg_id, retired.staging_generation)
                .unwrap_err(),
        );
        let mut stale_intent = intent(b"stale intent creation");
        stale_intent.pg_id = PgId::new(23);
        stale(old.create_intent(&stale_intent).unwrap_err());

        let state = next.state.lock().unwrap();
        assert_eq!(
            load_staging_row(
                &state.connection,
                published.pg_id,
                published.staging_generation,
            )
            .unwrap()
            .unwrap()
            .state,
            StagingState::Published
        );
        assert_eq!(
            finalized_floor(&state.connection, retired.pg_id).unwrap(),
            0
        );
        assert!(load_staging_row(
            &state.connection,
            stale_intent.pg_id,
            stale_intent.staging_generation,
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn evidence_page_assignment_is_atomic_across_independent_store_handles() {
        let tmp = test_util::tempdir();
        let artifact = canonical_artifact(b"two-handle evidence assignment");
        let intent = intent(artifact);
        let setup = open(tmp.path());
        setup.create_intent(&intent).unwrap();
        setup.publish_artifact(&intent, artifact).unwrap();
        drop(setup);

        let (selected_tx, selected_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let release_rx = Arc::new(Mutex::new(release_rx));
        let observer_release = Arc::clone(&release_rx);
        let first = MetadataTransferStagingStore::open_with_evidence_page_assignment_observer(
            tmp.path(),
            identity(),
            limits(),
            Arc::new(move || {
                selected_tx.send(()).unwrap();
                observer_release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            }),
        )
        .unwrap();
        let second = MetadataTransferStagingStore::open(tmp.path(), identity(), limits()).unwrap();
        second
            .state
            .lock()
            .unwrap()
            .connection
            .busy_timeout(Duration::ZERO)
            .unwrap();
        let first_thread = std::thread::spawn(move || first.next_evidence_page());
        selected_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let mut release = AssignmentGateRelease(Some(release_tx));

        let (second_result_tx, second_result_rx) = mpsc::sync_channel(0);
        let second_thread = std::thread::spawn(move || {
            let result = second.next_evidence_page();
            second_result_tx.send((second, result)).unwrap();
        });
        let (second, competing_result) = second_result_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("competing evidence assignment must fail without waiting for the first handle");
        let competing_error = competing_result.unwrap_err();
        assert!(matches!(
            competing_error,
            MetadataTransferStagingError::Sql { source, .. }
                if source.sqlite_error_code()
                    == Some(rusqlite::ffi::ErrorCode::DatabaseBusy)
        ));
        second_thread.join().unwrap();

        release.release();
        let assigned = first_thread.join().unwrap().unwrap().unwrap();
        let replay = second.next_evidence_page().unwrap().unwrap();
        assert_eq!(replay, assigned);
    }

    #[test]
    fn staging_evidence_page_and_apply_receipt_v4_encodings_are_fixed() {
        let tmp = test_util::tempdir();
        let artifact = canonical_artifact(b"fixed evidence page artifact");
        let intent = intent(artifact);
        let store = open(tmp.path());
        store.create_intent(&intent).unwrap();
        store.publish_artifact(&intent, artifact).unwrap();
        let page = store.next_evidence_page().unwrap().unwrap();
        let receipt = MetadataTransferStagingEvidenceApplyReceipt::for_page(&page);

        assert_eq!(
            hex(page.operation_payload()),
            "4152474d494e2d53544147494e472d45564944454e43452d504147452d56340000000004000000000000000700000021756e69783a2f2f2f72756e2f6172676d696e2f73746f726167652d342e736f636b00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001000000010000000000000001000001014152474d494e2d53544147494e472d45564944454e43452d5634000000000004000000000000000700000021756e69783a2f2f2f72756e2f6172676d696e2f73746f726167652d342e736f636b00000013000000000000000c000000000000000b00000010000000030000000100000002000000030000001000000003000000040000000200000003000000000000000c0efadd220adcc57506f8a395ce807f64911785127f9af6724bde5399ea513ba500000000000000930003000000000000000d01000000000000000a0000000000000000010000000000000000050000000000000000000000000000000001000000000000000005000000000000000007"
        );
        assert_eq!(
            hex(receipt.as_bytes()),
            "4152474d494e2d53544147494e472d45564944454e43452d4150504c592d56340000000004000000000000000700000021756e69783a2f2f2f72756e2f6172676d696e2f73746f726167652d342e736f636b000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001b8f437d29cbd3a83732c4708bfebf568026c69dd725c33aebbfcf4c6b9dccd050000000000000001"
        );
    }

    #[test]
    fn historical_staging_evidence_v1_encodings_remain_rejected() {
        let evidence = decode_hex(
            "4152474d494e2d53544147494e472d45564944454e43452d5631000000000004000000000000000700000021756e69783a2f2f2f72756e2f6172676d696e2f73746f726167652d342e736f636b00000013000000000000000c000000000000000b00000010000000030000000100000002000000030000001000000003000000040000000200000003000000000000000c8e23ea24e8bba02e69f180fc02083e99416e1f8614ce65c9d3a96c7f3d24d572000000000000001d000107",
        );
        let page = decode_hex(
            "4152474d494e2d53544147494e472d45564944454e43452d504147452d56310000000004000000000000000700000021756e69783a2f2f2f72756e2f6172676d696e2f73746f726167652d342e736f636b000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001000000010000000000000001000000bc4152474d494e2d53544147494e472d45564944454e43452d5631000000000004000000000000000700000021756e69783a2f2f2f72756e2f6172676d696e2f73746f726167652d342e736f636b00000013000000000000000c000000000000000b00000010000000030000000100000002000000030000001000000003000000040000000200000003000000000000000c0bf51475e8b2d3cf459b84790992ceb689fd3261ab949c76545228fd3cf39381000000000000001c000107",
        );
        let apply_receipt = decode_hex(
            "4152474d494e2d53544147494e472d45564944454e43452d4150504c592d56310000000004000000000000000700000021756e69783a2f2f2f72756e2f6172676d696e2f73746f726167652d342e736f636b000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001b97e8570c0ac86e556b72d83542c0aafea95e3908882344ac802537b5e55a57e0000000000000001",
        );

        assert!(decode_staging_evidence(&evidence).is_err());
        assert!(
            decode_staging_evidence_page_payload(&page, checksum::sha256::digest(&page)).is_err()
        );
        assert!(decode_staging_evidence_apply_receipt(&apply_receipt).is_err());
    }

    #[test]
    fn historical_staging_evidence_v2_page_and_apply_receipt_remain_rejected() {
        let page = decode_hex(
            "4152474d494e2d53544147494e472d45564944454e43452d504147452d56320000000004000000000000000700000021756e69783a2f2f2f72756e2f6172676d696e2f73746f726167652d342e736f636b000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001000000010000000000000001000001014152474d494e2d53544147494e472d45564944454e43452d5632000000000004000000000000000700000021756e69783a2f2f2f72756e2f6172676d696e2f73746f726167652d342e736f636b00000013000000000000000c000000000000000b00000010000000030000000100000002000000030000001000000003000000040000000200000003000000000000000cdf119ce6d30f03ee41c522f38a714f40c51f6203f5686863315034ea17d5815c0000000000000093000200000000000003340100000000000003330000000000000000010000000000000000050000000000000000000000000000000001000000000000000005000000000000000007",
        );
        let apply_receipt = decode_hex(
            "4152474d494e2d53544147494e472d45564944454e43452d4150504c592d56320000000004000000000000000700000021756e69783a2f2f2f72756e2f6172676d696e2f73746f726167652d342e736f636b000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001d21dec7bc9da9454bf830dd0c5b7d398f5de9db47b004d49fa396e8eb8ef76ea0000000000000001",
        );

        assert!(
            decode_staging_evidence_page_payload(&page, checksum::sha256::digest(&page)).is_err()
        );
        assert!(decode_staging_evidence_apply_receipt(&apply_receipt).is_err());
    }

    #[test]
    fn historical_staging_evidence_v3_page_and_apply_receipt_remain_rejected() {
        let page = decode_hex(
            "4152474d494e2d53544147494e472d45564944454e43452d504147452d56330000000004000000000000000700000021756e69783a2f2f2f72756e2f6172676d696e2f73746f726167652d342e736f636b000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001000000010000000000000001000001014152474d494e2d53544147494e472d45564944454e43452d5633000000000004000000000000000700000021756e69783a2f2f2f72756e2f6172676d696e2f73746f726167652d342e736f636b00000013000000000000000c000000000000000b00000010000000030000000100000002000000030000001000000003000000040000000200000003000000000000000c0efadd220adcc57506f8a395ce807f64911785127f9af6724bde5399ea513ba500000000000000930003000000000000000d01000000000000000a0000000000000000010000000000000000050000000000000000000000000000000001000000000000000005000000000000000007",
        );
        let apply_receipt = decode_hex(
            "4152474d494e2d53544147494e472d45564944454e43452d4150504c592d56330000000004000000000000000700000021756e69783a2f2f2f72756e2f6172676d696e2f73746f726167652d342e736f636b0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000015318b54c537c0c6ffa829573687a23ffcc84968b7f56fa41d947adf146c89b570000000000000001",
        );

        assert!(
            decode_staging_evidence_page_payload(&page, checksum::sha256::digest(&page)).is_err()
        );
        assert!(decode_staging_evidence_apply_receipt(&apply_receipt).is_err());
    }

    #[test]
    fn evidence_acknowledgement_is_atomic_and_chains_the_successor_page() {
        let tmp = test_util::tempdir();
        let first_artifact = canonical_artifact(b"acknowledged evidence page artifact");
        let first = intent(first_artifact);
        let store = open(tmp.path());
        store.create_intent(&first).unwrap();
        store.publish_artifact(&first, first_artifact).unwrap();
        let first_page = store.next_evidence_page().unwrap().unwrap();

        let mut second = intent(b"successor evidence page artifact");
        second.pg_id = PgId::new(20);
        store.tombstone(&second).unwrap();
        let receipt = MetadataTransferStagingEvidenceApplyReceipt::for_page(&first_page);
        store
            .record_evidence_apply_receipt(&first_page, &receipt)
            .unwrap();
        store
            .record_evidence_apply_receipt(&first_page, &receipt)
            .unwrap();
        assert_eq!(store.unacknowledged_evidence_count(), 1);

        let successor = store.next_evidence_page().unwrap().unwrap();
        assert_eq!(successor.generation(), 2);
        assert_eq!(successor.previous_generation, 1);
        assert_eq!(
            successor.previous_apply_receipt_digest,
            checksum::sha256::digest(receipt.as_bytes())
        );
        assert_eq!(successor.entries.len(), 1);
        assert_ne!(successor.entries, first_page.entries);
        let successor_payload = successor.operation_payload().to_vec();
        drop(store);

        let restarted = open(tmp.path());
        let replay = restarted.next_evidence_page().unwrap().unwrap();
        assert_eq!(replay.operation_payload(), successor_payload);
        assert_eq!(replay, successor);
        assert_eq!(restarted.unacknowledged_evidence_count(), 1);
    }

    #[test]
    fn invalid_apply_receipt_cannot_acknowledge_or_replace_the_inflight_page() {
        let tmp = test_util::tempdir();
        let artifact = canonical_artifact(b"receipt rejection artifact");
        let intent = intent(artifact);
        let store = open(tmp.path());
        store.create_intent(&intent).unwrap();
        store.publish_artifact(&intent, artifact).unwrap();
        let page = store.next_evidence_page().unwrap().unwrap();
        let mut receipt = MetadataTransferStagingEvidenceApplyReceipt::for_page(&page);
        receipt.page_digest[0] ^= 0x80;
        receipt.bytes = encode_staging_evidence_apply_receipt(&receipt);

        assert!(matches!(
            store.record_evidence_apply_receipt(&page, &receipt),
            Err(MetadataTransferStagingError::IntentConflict(_))
        ));
        assert_eq!(store.unacknowledged_evidence_count(), 1);
        assert_eq!(store.next_evidence_page().unwrap().unwrap(), page);
    }

    #[test]
    fn evidence_pages_are_bounded_and_ordered() {
        let tmp = test_util::tempdir();
        let page_limits = MetadataTransferStagingLimits::new(
            MAX_STAGING_EVIDENCE_PAGE_ENTRIES + 1,
            1024,
            u64::try_from(MAX_STAGING_EVIDENCE_PAGE_ENTRIES + 1).unwrap() * 1024,
        )
        .unwrap();
        let store =
            MetadataTransferStagingStore::open(tmp.path(), identity(), page_limits).unwrap();
        for pg in 1..=u32::try_from(MAX_STAGING_EVIDENCE_PAGE_ENTRIES + 1).unwrap() {
            let mut cancelled = intent(b"bounded evidence page");
            cancelled.pg_id = PgId::new(pg);
            store.tombstone(&cancelled).unwrap();
        }

        let page = store.next_evidence_page().unwrap().unwrap();
        assert_eq!(page.entries.len(), MAX_STAGING_EVIDENCE_PAGE_ENTRIES);
        assert!(page.operation_payload().len() <= MAX_STAGING_EVIDENCE_PAGE_BYTES);
        assert!(page
            .entries
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence));
    }

    #[test]
    fn evidence_page_builder_enforces_the_encoded_byte_ceiling() {
        let tmp = test_util::tempdir();
        let page_limits = MetadataTransferStagingLimits::new(
            MAX_STAGING_EVIDENCE_PAGE_ENTRIES,
            1024,
            u64::try_from(MAX_STAGING_EVIDENCE_PAGE_ENTRIES).unwrap() * 1024,
        )
        .unwrap();
        let long_identity = MetadataTransferStagingNodeIdentity::new(
            NodeId::new(4),
            7,
            "x".repeat(MAX_ENDPOINT_BYTES),
        )
        .unwrap();
        let store =
            MetadataTransferStagingStore::open(tmp.path(), long_identity, page_limits).unwrap();
        for pg in 1..=u32::try_from(MAX_STAGING_EVIDENCE_PAGE_ENTRIES).unwrap() {
            let mut cancelled = intent(b"byte-bounded evidence page");
            cancelled.pg_id = PgId::new(pg);
            store.tombstone(&cancelled).unwrap();
        }

        let page = store.next_evidence_page().unwrap().unwrap();
        assert!(page.entries.len() < MAX_STAGING_EVIDENCE_PAGE_ENTRIES);
        assert!(page.operation_payload().len() <= MAX_STAGING_EVIDENCE_PAGE_BYTES);
        let mut oversized = page.entries.clone();
        let next_sequence = oversized.last().unwrap().sequence + 1;
        oversized.push(MetadataTransferStagingEvidencePageEntry {
            sequence: next_sequence,
            evidence: encode_staging_evidence(
                &store.identity,
                &intent(b"next evidence"),
                1,
                None,
                None,
            ),
        });
        assert!(
            encode_staging_evidence_page_payload(
                &page.actor,
                page.actor_closure_candidate.as_ref(),
                page.previous_generation,
                page.previous_apply_receipt_digest,
                page.generation,
                &oversized,
            )
            .len()
                > MAX_STAGING_EVIDENCE_PAGE_BYTES
        );
    }

    #[test]
    fn startup_rejects_partial_evidence_acknowledgement_transactions() {
        for receipt_without_members in [false, true] {
            let tmp = test_util::tempdir();
            let artifact = canonical_artifact(b"partial evidence acknowledgement");
            let intent = intent(artifact);
            let store = open(tmp.path());
            store.create_intent(&intent).unwrap();
            store.publish_artifact(&intent, artifact).unwrap();
            let page = store.next_evidence_page().unwrap().unwrap();
            let receipt = MetadataTransferStagingEvidenceApplyReceipt::for_page(&page);
            drop(store);

            let connection = Connection::open(catalogue_path(tmp.path())).unwrap();
            if receipt_without_members {
                connection
                    .execute(
                        "UPDATE staging_evidence_inflight_page SET apply_receipt = ?1",
                        params![receipt.as_bytes()],
                    )
                    .unwrap();
            } else {
                connection
                    .execute("UPDATE staging_evidence_deltas SET acknowledged = 1", [])
                    .unwrap();
            }
            drop(connection);

            let error = MetadataTransferStagingStore::open(tmp.path(), identity(), limits())
                .err()
                .expect("partial evidence acknowledgement must fail startup");
            assert!(matches!(error, MetadataTransferStagingError::Invariant(_)));
        }
    }

    #[test]
    fn startup_rejects_acknowledged_evidence_without_its_retained_page() {
        let tmp = test_util::tempdir();
        let artifact = canonical_artifact(b"missing acknowledged evidence page");
        let intent = intent(artifact);
        let store = open(tmp.path());
        store.create_intent(&intent).unwrap();
        store.publish_artifact(&intent, artifact).unwrap();
        let page = store.next_evidence_page().unwrap().unwrap();
        let receipt = MetadataTransferStagingEvidenceApplyReceipt::for_page(&page);
        store
            .record_evidence_apply_receipt(&page, &receipt)
            .unwrap();
        drop(store);
        drop(open(tmp.path()));

        let connection = Connection::open(catalogue_path(tmp.path())).unwrap();
        connection
            .execute("DELETE FROM staging_evidence_inflight_page", [])
            .unwrap();
        drop(connection);

        let error = MetadataTransferStagingStore::open(tmp.path(), identity(), limits())
            .err()
            .expect("acknowledged evidence without its page must fail startup");
        assert!(matches!(error, MetadataTransferStagingError::Invariant(_)));
    }

    #[test]
    fn startup_rejects_acknowledged_evidence_outside_the_retained_page_chain() {
        let tmp = test_util::tempdir();
        let first_artifact = canonical_artifact(b"retained acknowledged evidence page");
        let first = intent(first_artifact);
        let store = open(tmp.path());
        store.create_intent(&first).unwrap();
        store.publish_artifact(&first, first_artifact).unwrap();
        let page = store.next_evidence_page().unwrap().unwrap();
        let receipt = MetadataTransferStagingEvidenceApplyReceipt::for_page(&page);
        store
            .record_evidence_apply_receipt(&page, &receipt)
            .unwrap();
        let mut unrelated = intent(b"unassigned evidence");
        unrelated.pg_id = PgId::new(20);
        store.tombstone(&unrelated).unwrap();
        drop(store);
        drop(open(tmp.path()));

        let connection = Connection::open(catalogue_path(tmp.path())).unwrap();
        connection
            .execute(
                "UPDATE staging_evidence_deltas SET acknowledged = 1 \
                 WHERE pg_id = ?1 AND staging_generation = ?2",
                params![
                    i64::from(unrelated.pg_id.get()),
                    to_sql_u64(unrelated.staging_generation).unwrap()
                ],
            )
            .unwrap();
        drop(connection);

        let error = MetadataTransferStagingStore::open(tmp.path(), identity(), limits())
            .err()
            .expect("acknowledged nonmember evidence must fail startup");
        assert!(matches!(error, MetadataTransferStagingError::Invariant(_)));
    }

    #[test]
    fn acknowledged_genesis_page_reopens_before_successor_assignment() {
        for queue_successor_delta in [false, true] {
            let tmp = test_util::tempdir();
            let artifact = canonical_artifact(b"acknowledged genesis restart");
            let first = intent(artifact);
            let store = open(tmp.path());
            store.create_intent(&first).unwrap();
            store.publish_artifact(&first, artifact).unwrap();
            let first_page = store.next_evidence_page().unwrap().unwrap();
            let receipt = MetadataTransferStagingEvidenceApplyReceipt::for_page(&first_page);
            store
                .record_evidence_apply_receipt(&first_page, &receipt)
                .unwrap();

            if queue_successor_delta {
                let mut second = intent(b"queued after genesis acknowledgement");
                second.pg_id = PgId::new(20);
                store.tombstone(&second).unwrap();
            }
            drop(store);

            let restarted = open(tmp.path());
            if queue_successor_delta {
                let successor = restarted.next_evidence_page().unwrap().unwrap();
                assert_eq!(successor.previous_generation, first_page.generation());
                assert_eq!(
                    successor.previous_apply_receipt_digest,
                    checksum::sha256::digest(receipt.as_bytes())
                );
                assert_ne!(successor.entries, first_page.entries);
            } else {
                assert!(restarted.next_evidence_page().unwrap().is_none());
                restarted
                    .record_evidence_apply_receipt(&first_page, &receipt)
                    .unwrap();
            }
        }
    }

    #[test]
    fn startup_rejects_apply_receipt_not_bound_to_the_durable_page() {
        let tmp = test_util::tempdir();
        let artifact = canonical_artifact(b"startup apply receipt validation");
        let intent = intent(artifact);
        let store = open(tmp.path());
        store.create_intent(&intent).unwrap();
        store.publish_artifact(&intent, artifact).unwrap();
        let page = store.next_evidence_page().unwrap().unwrap();
        let receipt = MetadataTransferStagingEvidenceApplyReceipt::for_page(&page);
        store
            .record_evidence_apply_receipt(&page, &receipt)
            .unwrap();
        drop(store);

        let connection = Connection::open(catalogue_path(tmp.path())).unwrap();
        let mut forged = receipt.clone();
        forged.page_digest[0] ^= 1;
        forged.bytes = encode_staging_evidence_apply_receipt(&forged);
        connection
            .execute(
                "UPDATE staging_evidence_inflight_page SET apply_receipt = ?1",
                params![forged.as_bytes()],
            )
            .unwrap();
        drop(connection);

        let error = MetadataTransferStagingStore::open(tmp.path(), identity(), limits())
            .err()
            .expect("misbound staging evidence receipt must fail startup");
        assert!(matches!(error, MetadataTransferStagingError::Invariant(_)));
    }
}
